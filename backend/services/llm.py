from __future__ import annotations

import ast
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import requests

try:
    from backend.config import (
        DEFAULT_LLM_MODEL,
        DEFAULT_OPENROUTER_API_BASE,
        FUZZY_MATCH_THRESHOLD,
        LLM_PARSE_MAX_RETRIES,
        LLM_REQUEST_TIMEOUT_SECONDS,
        LLM_RETRY_PREVIEW_CHARS,
        LLM_TEXT_CHAR_LIMIT,
        LOGGER,
        MIN_ENTITY_CONFIDENCE,
        TABLE_PARSER_ENABLED,
        US_STATE_ABBREVIATIONS,
        US_STATE_NAMES,
        _clean_env_value,
        _get_openrouter_api_key,
        _normalize_openrouter_api_base,
    )
    from backend.models import BoundingBox, Detection, TableRegion, WordSpan
    from backend.services.fuzzy import find_fuzzy_spans
    from backend.text_mapping import get_bboxes_for_offsets
except ImportError:
    from config import (
        DEFAULT_LLM_MODEL,
        DEFAULT_OPENROUTER_API_BASE,
        FUZZY_MATCH_THRESHOLD,
        LLM_PARSE_MAX_RETRIES,
        LLM_REQUEST_TIMEOUT_SECONDS,
        LLM_RETRY_PREVIEW_CHARS,
        LLM_TEXT_CHAR_LIMIT,
        LOGGER,
        MIN_ENTITY_CONFIDENCE,
        TABLE_PARSER_ENABLED,
        US_STATE_ABBREVIATIONS,
        US_STATE_NAMES,
        _clean_env_value,
        _get_openrouter_api_key,
        _normalize_openrouter_api_base,
    )
    from models import BoundingBox, Detection, TableRegion, WordSpan
    from text_mapping import get_bboxes_for_offsets

    from services.fuzzy import find_fuzzy_spans


@dataclass
class LLMQuoteCandidate:
    quote: str
    category: str
    confidence: float


_LLM_POLICY_LABEL_TOKENS: set[str] = {
    "account",
    "authorized",
    "category",
    "cloud",
    "compliance",
    "customer",
    "data",
    "disposal",
    "duration",
    "files",
    "information",
    "intern",
    "location",
    "log",
    "management",
    "method",
    "party",
    "personnel",
    "policy",
    "requirements",
    "responsible",
    "retention",
    "secure",
    "storage",
    "support",
    "tickets",
    "user",
}

_LLM_NON_SENSITIVE_ENTITY_TYPES: set[str] = {
    "BUSINESS_CATEGORY",
    "CATEGORY",
    "COMPLIANCE_REQUIREMENT",
    "DATA_CATEGORY",
    "DISPOSAL_METHOD",
    "DOCUMENT_TITLE",
    "RETENTION_DURATION",
    "RETENTION_POLICY",
    "ROLE",
    "STORAGE_LOCATION",
}

_LLM_LOW_SIGNAL_TOKENS: set[str] = {
    "a",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "been",
    "being",
    "but",
    "by",
    "dear",
    "for",
    "from",
    "here",
    "i",
    "if",
    "in",
    "is",
    "it",
    "its",
    "me",
    "my",
    "not",
    "of",
    "on",
    "or",
    "our",
    "ours",
    "please",
    "sincerely",
    "thank",
    "thanks",
    "that",
    "the",
    "their",
    "them",
    "there",
    "these",
    "they",
    "this",
    "those",
    "to",
    "us",
    "was",
    "we",
    "were",
    "with",
    "you",
    "your",
    "yours",
}

LLM_MAX_LOCALIZED_ENTITY_TOKENS = max(4, int(os.getenv("LLM_MAX_LOCALIZED_ENTITY_TOKENS", "14")))
LLM_MAX_LOCALIZED_ENTITY_CHARS = max(24, int(os.getenv("LLM_MAX_LOCALIZED_ENTITY_CHARS", "140")))


def classify_llm_quote_type(quote: str) -> str:
    if re.search(r"\$\s?\d[\d,]*(?:\.\d+)?", quote):
        return "FINANCIAL_PENALTY_AMOUNT"
    normalized = re.sub(r"[\.,]", "", quote).strip().lower()
    if normalized in US_STATE_NAMES:
        return "JURISDICTION_STATE"
    upper = quote.strip().upper()
    if upper in US_STATE_ABBREVIATIONS:
        return "JURISDICTION_STATE"
    return "LEGAL_PARTY_NAME"


def _is_low_signal_llm_quote(quote: str, entity_type: str) -> bool:
    raw_quote = str(quote or "").strip()
    if not raw_quote:
        return True
    tokens = re.findall(r"[A-Za-z0-9]+", raw_quote.lower())
    if not tokens:
        return True
    has_digit = any(any(char.isdigit() for char in token) for token in tokens)
    candidate_type = str(entity_type or "").strip().upper()

    if candidate_type in _LLM_NON_SENSITIVE_ENTITY_TYPES:
        return True
    if not has_digit and all(token in _LLM_POLICY_LABEL_TOKENS for token in tokens):
        return True
    if not has_digit and all(token in _LLM_LOW_SIGNAL_TOKENS for token in tokens):
        return True
    if len(tokens) == 1 and not has_digit:
        token = tokens[0]
        if token in _LLM_LOW_SIGNAL_TOKENS:
            return True
        if candidate_type in {"LEGAL_PARTY_NAME", "PERSON", "ORGANIZATION"}:
            compact = re.sub(r"[^A-Za-z0-9]+", "", raw_quote)
            looks_like_acronym = compact.isupper() and len(compact) >= 2
            if len(token) <= 3 and not looks_like_acronym:
                return True
    if not has_digit and candidate_type in {"LEGAL_PARTY_NAME", "PERSON"}:
        alpha_tokens = [token for token in tokens if token.isalpha()]
        if len(alpha_tokens) <= 2:
            lower_count = sum(1 for token in re.findall(r"[A-Za-z]+", raw_quote) if token.islower())
            if lower_count == len(alpha_tokens):
                return True
    return False


def _is_oversized_llm_localized_span(localized_text: str) -> bool:
    compact = str(localized_text or "").strip()
    if not compact:
        return False
    token_count = len(re.findall(r"[A-Za-z0-9]+", compact))
    if token_count > LLM_MAX_LOCALIZED_ENTITY_TOKENS:
        return True
    if len(compact) > LLM_MAX_LOCALIZED_ENTITY_CHARS:
        return True
    sentence_ending_count = len(re.findall(r"[\.!?;]", compact))
    if sentence_ending_count >= 2 and token_count >= 8:
        return True
    return False


def _build_llm_messages(
    document_text: str,
    retry_feedback: str = "",
    previous_response: str = "",
    has_table_context: bool = False,
) -> List[Dict[str, str]]:
    system_prompt = (
        "You extract personally identifiable and sensitive information from documents.\n"
        "Output contract:\n"
        "1) Return a JSON array as the top-level value, no wrapper object.\n"
        "2) Each array item must be an object with exactly: quote, category, confidence.\n"
        "3) quote must be verbatim text from input.\n"
        "4) quote must be a minimal atomic value. Never include field labels, keys, or contextual headers in the quote (e.g., extract 'MARK-3456' not 'employee id MARK-3456').\n"
        "5) quote must be complete, not a fragment.\n"
        "6) For URL, email, IP, account, ID, phone: the quote must be the full token only without surrounding text.\n"
        "7) category is open-ended; do not restrict category to a fixed list only. Use UPPER_SNAKE_CASE.\n"
        "8) confidence must be numeric in range 0 to 1.\n"
        "9) No prose, no markdown, no code fences. Return the JSON array directly.\n"
        "10) Do not include any reasoning or <think> blocks. Output only the JSON array.\n"
        "11) If uncertain or nothing is found, return [].\n"
        "12) Deduplicate exact quote+category pairs.\n"
        "13) Prefer exact value-only spans, not full sentences that contain the value."
    )
    if has_table_context:
        system_prompt += (
            "\n13) Input may include [TABLE] blocks where each row uses ' | ' as column separators."
            "\n14) Treat each cell value as independently detectable sensitive text."
            "\n15) Preserve exact quote text from cells, including wrapped values."
        )

    user_prompt = f"""
Detect any personally identifiable or sensitive information in the document.
Include names, addresses, phone/fax numbers, emails, identifiers, account numbers,
legal references, locations, organization names, and other sensitive data when present.

Return only the JSON array.

Document:
{document_text}
""".strip()
    if retry_feedback:
        user_prompt += f"\n\nRetry reason: {retry_feedback}"
    if previous_response:
        compact_prev = re.sub(r"\s+", " ", previous_response).strip()[:1200]
        user_prompt += (
            f"\n\nPrevious invalid output (for correction):\n{compact_prev}\nRe-emit as valid JSON array only."
        )

    return [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}]


def _read_completion_content(response: object) -> str:
    if isinstance(response, dict):
        choices = response.get("choices", [])
        if not choices:
            return ""
        message = choices[0].get("message", {})
        return str(message.get("content", "") or "")
    choices = getattr(response, "choices", None)
    if not choices:
        return ""
    message = getattr(choices[0], "message", None)
    if message is None:
        return ""
    content = getattr(message, "content", "")
    if isinstance(content, list):
        chunks = []
        for item in content:
            if isinstance(item, dict) and "text" in item:
                chunks.append(str(item["text"]))
            else:
                chunks.append(str(item))
        return "".join(chunks)
    return str(content or "")


def _call_openrouter_chat_completion(
    api_base: str,
    api_key: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float,
    max_tokens: int,
) -> Dict[str, Any]:
    normalized_base = _normalize_openrouter_api_base(api_base)
    endpoint = f"{normalized_base.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    referer = _clean_env_value(os.getenv("OPENROUTER_SITE_URL") or os.getenv("OPENROUTER_HTTP_REFERER") or "")
    if referer:
        headers["HTTP-Referer"] = referer

    x_title = _clean_env_value(
        os.getenv("OPENROUTER_SITE_NAME")
        or os.getenv("OPENROUTER_X_OPENROUTER_TITLE")
        or os.getenv("OPENROUTER_X_TITLE")
        or "snv-PII-redact"
    )
    if x_title:
        headers["X-OpenRouter-Title"] = x_title
        headers["X-Title"] = x_title

    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    response = requests.post(
        endpoint,
        headers=headers,
        json=payload,
        timeout=LLM_REQUEST_TIMEOUT_SECONDS,
        allow_redirects=False,
    )

    if response.status_code in {301, 302, 307, 308}:
        redirect_target = response.headers.get("Location", "").strip()
        if redirect_target:
            redirected_endpoint = redirect_target
            if redirect_target.startswith("/"):
                redirected_endpoint = urljoin(endpoint, redirect_target)
            response = requests.post(
                redirected_endpoint,
                headers=headers,
                json=payload,
                timeout=LLM_REQUEST_TIMEOUT_SECONDS,
                allow_redirects=False,
            )

    if response.status_code >= 400:
        message = response.text.strip()
        if len(message) > 400:
            message = f"{message[:400]}..."
        if response.status_code == 401 and "Missing Authentication header" in message:
            raise RuntimeError(
                "OpenRouter API error 401: Missing Authentication header. "
                "Verify OPENROUTER_API_BASE is https://openrouter.ai/api/v1 and "
                "OPENROUTER_API_KEY is the raw token (without Bearer)."
            )
        raise RuntimeError(f"OpenRouter API error {response.status_code}: {message}")

    try:
        parsed = response.json()
    except ValueError as exc:
        raise RuntimeError("OpenRouter API returned a non-JSON response.") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError("OpenRouter API response shape is invalid.")

    if "error" in parsed:
        err_msg = parsed["error"].get("message", str(parsed["error"]))
        raise RuntimeError(f"OpenRouter upstream error: {err_msg}")

    return parsed


def _strip_think_tags(raw_content: str) -> str:
    """Strip DeepSeek R1 think blocks that contain chain-of-thought reasoning."""
    open_tag = f"{chr(60)}think{chr(62)}"
    close_tag = f"{chr(60)}/think{chr(62)}"
    stripped = str(raw_content or "")

    while True:
        start = stripped.find(open_tag)
        if start == -1:
            break

        end = stripped.find(close_tag, start + len(open_tag))
        if end == -1:
            # Unclosed think tag — remove from start to end.
            stripped = stripped[:start]
            break

        stripped = stripped[:start] + stripped[end + len(close_tag) :]

    return stripped.strip()


def _strip_markdown_code_fence(raw_content: str) -> str:
    stripped = raw_content.strip()
    if not stripped.startswith("```"):
        return stripped
    first_newline = stripped.find("\n")
    if first_newline != -1:
        stripped = stripped[first_newline + 1 :]
    if stripped.endswith("```"):
        stripped = stripped[:-3]
    return stripped.strip()


def _normalize_json_like_text(raw_content: str) -> str:
    normalized = str(raw_content or "")
    normalized = normalized.replace("\u201c", '"').replace("\u201d", '"')
    normalized = normalized.replace("\u2018", "'").replace("\u2019", "'")
    normalized = re.sub(r",(\s*[}\]])", r"\1", normalized)
    return normalized


def _loads_json_maybe_nested(raw_content: str) -> Optional[Any]:
    candidate = _normalize_json_like_text(raw_content).strip()[:200_000]
    if not candidate:
        return None
    for _ in range(2):
        parsed = None
        try:
            parsed = json.loads(candidate)
        except (json.JSONDecodeError, ValueError):
            pass
        if parsed is None and len(candidate) < 10_000:
            try:
                parsed = ast.literal_eval(candidate)
            except (ValueError, SyntaxError):
                return None
        if parsed is None:
            return None
        if isinstance(parsed, str):
            candidate = parsed.strip()
            continue
        return parsed
    return None


def _extract_balanced_json_segments(raw_content: str, open_char: str, close_char: str) -> List[str]:
    segments: List[str] = []
    if not raw_content:
        return segments
    in_string = False
    quote_char = ""
    escape_next = False
    depth = 0
    start_index: Optional[int] = None

    for index, char in enumerate(raw_content):
        if escape_next:
            escape_next = False
            continue
        if in_string and char == "\\":
            escape_next = True
            continue
        if char in {'"', "'"}:
            if not in_string:
                in_string = True
                quote_char = char
            elif char == quote_char:
                in_string = False
                quote_char = ""
            continue
        if in_string:
            continue
        if char == open_char:
            if depth == 0:
                start_index = index
            depth += 1
            continue
        if char == close_char and depth > 0:
            depth -= 1
            if depth == 0 and start_index is not None:
                segments.append(raw_content[start_index : index + 1])
                start_index = None
    return segments


def _extract_items_from_llm_payload(payload: Any) -> Optional[List[Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if any(key in payload for key in ("quote", "text", "value", "entity")):
            return [payload]
        for key in ("quotes", "results", "entities", "items", "data", "output", "candidates"):
            nested_payload = payload.get(key)
            nested_items = _extract_items_from_llm_payload(nested_payload)
            if nested_items is not None:
                return nested_items
        for nested_payload in payload.values():
            nested_items = _extract_items_from_llm_payload(nested_payload)
            if nested_items is not None:
                return nested_items
    return None


def _extract_llm_objects_from_text(raw_content: str) -> List[Any]:
    objects: List[Any] = []
    for object_blob in _extract_balanced_json_segments(raw_content, "{", "}"):
        parsed = _loads_json_maybe_nested(object_blob)
        if isinstance(parsed, dict) and any(key in parsed for key in ("quote", "text", "value", "entity")):
            objects.append(parsed)
    return objects


def _parse_llm_markdown_table_row(line: str) -> Optional[Dict[str, Any]]:
    if "|" not in line:
        return None
    cells = [cell.strip() for cell in line.strip("|").split("|")]
    if len(cells) < 3:
        return None
    lower_cells = [cell.lower() for cell in cells[:3]]
    if lower_cells[0] in {"quote", "text", "entity"}:
        return None
    if re.fullmatch(r"[-: ]+", cells[0]):
        return None
    quote = cells[0].strip("`\"'")
    category = cells[1].strip("`\"'")
    confidence = cells[2].strip("`\"'")
    if not quote:
        return None
    return {"quote": quote, "category": category, "confidence": confidence or 0.85}


def _parse_llm_keyed_line(line: str) -> Optional[Dict[str, Any]]:
    keyed_token_pattern = re.compile(r"(?i)\b(quote|text|entity|category|type|label|confidence|score)\b\s*[:=]")
    matches = list(keyed_token_pattern.finditer(line))
    if len(matches) < 2:
        return None
    values: Dict[str, str] = {}
    for index, match in enumerate(matches):
        key = match.group(1).lower()
        value_start = match.end()
        value_end = matches[index + 1].start() if index + 1 < len(matches) else len(line)
        value = line[value_start:value_end].strip(" \t,;|-")
        if value:
            values[key] = value.strip("`\"'")
    quote = values.get("quote") or values.get("text") or values.get("entity")
    if not quote:
        return None
    category = values.get("category") or values.get("type") or values.get("label") or ""
    confidence = values.get("confidence") or values.get("score") or 0.85
    return {"quote": quote, "category": category, "confidence": confidence}


def _parse_llm_quoted_line(line: str) -> Optional[Dict[str, Any]]:
    match = re.search(r"[\"'](?P<quote>[^\"']+)[\"']\s*(?P<tail>.*)$", line)
    if not match:
        return None
    quote = (match.group("quote") or "").strip()
    tail = (match.group("tail") or "").strip()
    confidence_match = re.search(r"(\d+(?:\.\d+)?%?)", tail)
    confidence = confidence_match.group(1) if confidence_match else 0.85
    category = tail
    if confidence_match:
        category = tail[: confidence_match.start()]
    category = re.sub(r"(?i)^category\s*[:=]\s*", "", category)
    category = category.strip(" \t-_|(),:")
    if not quote:
        return None
    return {"quote": quote, "category": category, "confidence": confidence}


def _extract_llm_plaintext_items(raw_content: str) -> List[Any]:
    if not raw_content:
        return []
    text = _normalize_json_like_text(_strip_markdown_code_fence(raw_content))
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    items: List[Any] = []
    for line in lines:
        compact_line = re.sub(r"^\s*(?:[-*\u2022]+|\d+[\.)])\s*", "", line).strip()
        if not compact_line:
            continue
        table_item = _parse_llm_markdown_table_row(compact_line)
        if table_item is not None:
            items.append(table_item)
            continue
        keyed_item = _parse_llm_keyed_line(compact_line)
        if keyed_item is not None:
            items.append(keyed_item)
            continue
        quoted_item = _parse_llm_quoted_line(compact_line)
        if quoted_item is not None:
            items.append(quoted_item)
    return items


def _parse_confidence_value(confidence_raw: Any, default: float = 0.85) -> float:
    if isinstance(confidence_raw, (int, float)):
        value = float(confidence_raw)
    else:
        raw_text = str(confidence_raw or "").strip()
        if not raw_text:
            value = default
        else:
            percent_match = re.search(r"(-?\d+(?:\.\d+)?)\s*%", raw_text)
            if percent_match:
                value = float(percent_match.group(1)) / 100.0
            else:
                numeric_match = re.search(r"-?\d+(?:\.\d+)?", raw_text)
                value = float(numeric_match.group(0)) if numeric_match else default
    if value > 1:
        value = value / 100.0
    return max(0.0, min(1.0, value))


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _build_llm_quote_candidates_from_items(items: Sequence[Any]) -> List[LLMQuoteCandidate]:
    deduped: Dict[Tuple[str, str], LLMQuoteCandidate] = {}
    for item in items:
        quote = ""
        category = ""
        confidence_raw: Any = 0.85
        if isinstance(item, str):
            quote = item.strip()
        elif isinstance(item, dict):
            quote = str(item.get("quote") or item.get("text") or "").strip()
            category = str(
                item.get("category") or item.get("entity_type") or item.get("type") or item.get("label") or ""
            ).strip()
            confidence_raw = item.get("confidence", item.get("score", 0.85))
        else:
            continue
        if not quote:
            continue
        confidence_value = _parse_confidence_value(confidence_raw, default=0.85)
        normalized_quote = re.sub(r"\s+", " ", quote).strip().lower()
        normalized_category = re.sub(r"\s+", " ", category).strip().lower()
        key = (normalized_quote, normalized_category)
        existing = deduped.get(key)
        if existing is None or confidence_value > existing.confidence:
            deduped[key] = LLMQuoteCandidate(quote=quote, category=category, confidence=confidence_value)
    return list(deduped.values())


def _sub_timer(timer: Optional[Dict[str, float]], key: str, start: float) -> None:
    if timer is not None:
        timer.setdefault(key, 0.0)
        timer[key] += time.perf_counter() - start


def parse_llm_quote_candidates(
    raw_content: str,
    timer: Optional[Dict[str, float]] = None,
) -> Tuple[List[LLMQuoteCandidate], bool]:
    if not raw_content or not raw_content.strip():
        return [], True

    # Truncate first to prevent O(n) character iteration on huge LLM responses
    raw_content = raw_content[:300_000]

    # Strip DeepSeek R1 think tags before any parsing
    _t_sub = time.perf_counter()
    cleaned = _strip_think_tags(raw_content)
    primary_text = cleaned.strip()
    fenced_text = _strip_markdown_code_fence(primary_text)
    _sub_timer(timer, "llm_parse_strip", _t_sub)

    payload: Optional[Any] = None
    candidate_texts: List[str] = [primary_text, fenced_text]

    _t_sub = time.perf_counter()
    candidate_texts.extend(_extract_balanced_json_segments(primary_text, "[", "]"))
    candidate_texts.extend(_extract_balanced_json_segments(primary_text, "{", "}"))
    _sub_timer(timer, "llm_parse_extract", _t_sub)

    _t_sub = time.perf_counter()
    for candidate_text in candidate_texts:
        if not candidate_text:
            continue
        payload = _loads_json_maybe_nested(candidate_text)
        if payload is None:
            continue
        items = _extract_items_from_llm_payload(payload)
        if items is None:
            continue
        candidates = _build_llm_quote_candidates_from_items(items)
        _sub_timer(timer, "llm_parse_json", _t_sub)
        return candidates, True
    _sub_timer(timer, "llm_parse_json", _t_sub)

    _t_sub = time.perf_counter()
    fallback_items = _extract_llm_objects_from_text(primary_text)
    if fallback_items:
        candidates = _build_llm_quote_candidates_from_items(fallback_items)
        _sub_timer(timer, "llm_parse_fallback", _t_sub)
        return candidates, True

    plaintext_items = _extract_llm_plaintext_items(primary_text)
    if plaintext_items:
        candidates = _build_llm_quote_candidates_from_items(plaintext_items)
        _sub_timer(timer, "llm_parse_fallback", _t_sub)
        return candidates, True
    _sub_timer(timer, "llm_parse_fallback", _t_sub)

    lower_text = primary_text.lower()
    if len(lower_text) < 150 and any(
        kw in lower_text
        for kw in (
            "none",
            "no pii",
            "not found",
            "n/a",
            "no sensitive",
            "[]",
            "{}",
            "nothing",
            "no personally identifiable",
        )
    ):
        return [], True

    return [], False


def normalize_llm_category(raw_category: str, quote: str) -> str:
    cleaned = str(raw_category or "").strip()
    if not cleaned:
        return classify_llm_quote_type(quote)
    cleaned = cleaned.replace("-", "_")
    cleaned = re.sub(r"[^A-Za-z0-9_\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", "_", cleaned).strip("_").upper()
    if not cleaned:
        return classify_llm_quote_type(quote)
    alias_map = {
        "NAME": "PERSON",
        "PERSON_NAME": "PERSON",
        "FULL_NAME": "PERSON",
        "COMPANY": "ORGANIZATION",
        "ORG": "ORGANIZATION",
        "PHONE": "PHONE_NUMBER",
        "MOBILE": "PHONE_NUMBER",
        "TELEPHONE": "PHONE_NUMBER",
        "FAX": "FAX_NUMBER",
        "ADDRESS": "STREET_ADDRESS",
        "STATE": "JURISDICTION_STATE",
        "AMOUNT": "FINANCIAL_AMOUNT",
    }
    return alias_map.get(cleaned, cleaned)


def run_llm_triage(
    canonical_text: str,
    char_map: Sequence[Tuple[int, int, BoundingBox]],
    word_spans: Sequence[WordSpan],
    table_regions: Optional[Sequence[TableRegion]] = None,
    timer: Optional[Dict[str, float]] = None,
) -> Tuple[List[Detection], List[str]]:
    warnings: List[str] = []

    api_key = _get_openrouter_api_key()
    if not api_key:
        warnings.append(
            "LLM step skipped: OPENROUTER_API_KEY is not configured. "
            "(OPENAI_API_KEY is also accepted as fallback.) "
            "Pipeline continued with Presidio-only detections."
        )
        return [], warnings

    model = os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL)
    api_base = os.getenv("OPENROUTER_API_BASE", DEFAULT_OPENROUTER_API_BASE)
    llm_max_output_tokens = max(300, min(4000, int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "1200"))))
    llm_calls_per_page = max(1, int(os.getenv("LLM_CALLS_PER_PAGE", "2")))
    text_slice = canonical_text[:LLM_TEXT_CHAR_LIMIT]
    has_table_context = TABLE_PARSER_ENABLED and bool(table_regions)

    merged_candidates: Dict[Tuple[str, str], LLMQuoteCandidate] = {}
    successful_passes = 0

    for pass_index in range(1, llm_calls_per_page + 1):
        raw_content = ""
        pass_candidates: List[LLMQuoteCandidate] = []
        parse_succeeded = False
        terminal_error: Optional[str] = None

        for attempt in range(1, LLM_PARSE_MAX_RETRIES + 1):
            retry_feedback = ""
            if attempt > 1:
                retry_feedback = (
                    "Previous response was invalid. Return ONLY a top-level JSON array of "
                    "objects with quote, category, confidence."
                )

            try:
                _t_api = time.perf_counter()
                response_json = _call_openrouter_chat_completion(
                    api_base=api_base,
                    api_key=api_key,
                    model=model,
                    messages=_build_llm_messages(
                        text_slice,
                        retry_feedback=retry_feedback,
                        previous_response=raw_content,
                        has_table_context=has_table_context,
                    ),
                    temperature=0.0,
                    max_tokens=llm_max_output_tokens,
                )
                if timer is not None:
                    timer.setdefault("llm_api", 0.0)
                    timer["llm_api"] += time.perf_counter() - _t_api
                raw_content = _read_completion_content(response_json)
            except Exception as exc:
                if attempt >= LLM_PARSE_MAX_RETRIES:
                    terminal_error = str(exc)
                    break
                continue

            _t_parse = time.perf_counter()
            pass_candidates, parse_succeeded = parse_llm_quote_candidates(raw_content, timer=timer)
            if timer is not None:
                timer.setdefault("llm_parse", 0.0)
                timer["llm_parse"] += time.perf_counter() - _t_parse
            if parse_succeeded:
                break

        if terminal_error is not None:
            warnings.append(
                f"LLM pass {pass_index}/{llm_calls_per_page} failed after retries and was skipped: {terminal_error}"
            )
            continue

        if not parse_succeeded:
            preview = re.sub(r"\s+", " ", raw_content).strip()
            if LLM_RETRY_PREVIEW_CHARS > 0:
                preview = preview[:LLM_RETRY_PREVIEW_CHARS]
            LOGGER.info(
                "[DEBUG] LLM_PARSE_FAILURE pass=%s/%s preview=%s",
                pass_index,
                llm_calls_per_page,
                preview or "<empty>",
            )
            warnings.append(
                f"LLM pass {pass_index}/{llm_calls_per_page} returned non-JSON output after retries "
                f"and was skipped (Got: '{preview}')."
            )
            continue

        successful_passes += 1
        for candidate in pass_candidates:
            normalized_quote = re.sub(r"\s+", " ", candidate.quote).strip().lower()
            normalized_category = re.sub(r"\s+", " ", candidate.category).strip().lower()
            key = (normalized_quote, normalized_category)
            existing = merged_candidates.get(key)
            if existing is None or candidate.confidence > existing.confidence:
                merged_candidates[key] = candidate

    candidates = list(merged_candidates.values())
    LOGGER.info(
        "[DEBUG] LLM_MULTI_PASS_SUMMARY passes=%s/%s merged=%s",
        successful_passes,
        llm_calls_per_page,
        len(candidates),
    )

    if not candidates:
        return [], warnings

    detections: List[Detection] = []
    for candidate in candidates:
        inferred_type = normalize_llm_category(candidate.category, candidate.quote)
        if _is_low_signal_llm_quote(candidate.quote, inferred_type):
            continue

        quote_tokens = len(re.findall(r"[A-Za-z0-9]+", candidate.quote))
        LOGGER.info(
            "[DEBUG] LLM_CANDIDATE quote=%s type=%s conf=%.4f tokens=%s chars=%s",
            candidate.quote,
            inferred_type,
            candidate.confidence,
            quote_tokens,
            len(candidate.quote),
        )

        _t_fuzzy = time.perf_counter()
        matches = find_fuzzy_spans(candidate.quote, word_spans, threshold=FUZZY_MATCH_THRESHOLD)
        if timer is not None:
            timer.setdefault("llm_fuzzy", 0.0)
            timer["llm_fuzzy"] += time.perf_counter() - _t_fuzzy
        if not matches:
            LOGGER.info(
                "[DEBUG] LLM_NO_FUZZY_MATCH quote=%s type=%s threshold=%s",
                candidate.quote,
                inferred_type,
                FUZZY_MATCH_THRESHOLD,
            )
            continue

        for start_char, end_char, similarity_score in matches:
            fuzzy_conf = max(0.0, min(1.0, similarity_score / 100.0))
            combined_conf = (candidate.confidence + fuzzy_conf) / 2.0
            if combined_conf < MIN_ENTITY_CONFIDENCE:
                continue

            _t_bbox = time.perf_counter()
            boxes = get_bboxes_for_offsets(start_char, end_char, char_map)
            if timer is not None:
                timer.setdefault("llm_bbox", 0.0)
                timer["llm_bbox"] += time.perf_counter() - _t_bbox
            if not boxes:
                continue

            localized_text = canonical_text[start_char:end_char].strip() or candidate.quote
            if _is_low_signal_llm_quote(localized_text, inferred_type):
                continue

            localized_tokens = len(re.findall(r"[A-Za-z0-9]+", localized_text))
            inflation_ratio = localized_tokens / max(1, quote_tokens)

            if _is_oversized_llm_localized_span(localized_text):
                LOGGER.info(
                    "[DEBUG] LLM_SKIP_OVERSIZED quote=%s localized=%s tokens=%d/%d",
                    candidate.quote,
                    localized_text,
                    localized_tokens,
                    LLM_MAX_LOCALIZED_ENTITY_TOKENS,
                )
                continue

            LOGGER.info(
                "[DEBUG] LLM_LOCALIZED quote=%s localized=%s sim=%.2f conf=%.4f inflation=%.2f",
                candidate.quote,
                localized_text,
                similarity_score,
                combined_conf,
                inflation_ratio,
            )

            detections.append(
                Detection(
                    entity_text=localized_text,
                    entity_type=inferred_type,
                    confidence_score=combined_conf,
                    source="LLM",
                    boxes=boxes,
                    supporting_sources=["LLM"],
                    decision_reason="single_source_llm",
                )
            )

    if not detections:
        LOGGER.info(
            "[DEBUG] LLM_LOCALIZATION_MISS candidates=%s threshold=%s",
            len(candidates),
            FUZZY_MATCH_THRESHOLD,
        )

    return detections, warnings
