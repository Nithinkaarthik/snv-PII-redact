from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider

try:
    from backend.config import (
        BUSINESS_KEYWORD_PATTERN,
        DEFAULT_SPACY_MODEL,
        IGNORE_JSON_KEYS,
        LOGGER,
        MIN_ENTITY_CONFIDENCE,
        TARGET_PII_ENTITIES,
    )
    from backend.models import BoundingBox, Detection, EngineSource
    from backend.text_mapping import get_bboxes_for_offsets
except ImportError:
    from config import (
        BUSINESS_KEYWORD_PATTERN,
        DEFAULT_SPACY_MODEL,
        IGNORE_JSON_KEYS,
        LOGGER,
        MIN_ENTITY_CONFIDENCE,
        TARGET_PII_ENTITIES,
    )
    from models import BoundingBox, Detection, EngineSource
    from text_mapping import get_bboxes_for_offsets

_ANALYZER: Optional[AnalyzerEngine] = None

_CONTEXTUAL_IDENTIFIER_PATTERN = re.compile(
    r"\b(?:customer|client|member|user|policy|account|receipt|transaction|order)\s*"
    r"(?:id|identifier|number|no\.?|#)\b",
    flags=re.IGNORECASE,
)
_CONTEXTUAL_SHORT_CODE_PATTERN = re.compile(
    r"\b(?:pin|otp|one[-\s]*time\s*(?:password|pin)|passcode|security\s*code|verification\s*code)\b",
    flags=re.IGNORECASE,
)
_CONTEXTUAL_CUSTOMER_IDENTIFIER_RULE = re.compile(
    r"\b(?:customer|client|member|user|policy|account|receipt|transaction|order)\s*"
    r"(?:id|identifier|number|no\.?|#)\s*(?:is|:|=)?\s*([A-Z0-9][A-Z0-9\-]{3,23})\b",
    flags=re.IGNORECASE,
)
_CONTEXTUAL_SECURITY_CODE_RULE = re.compile(
    r"\b(?:pin|otp|one[-\s]*time\s*(?:password|pin)|passcode|security\s*code|verification\s*code)"
    r"\s*(?:is|:|=)?\s*(\d{4,8})\b",
    flags=re.IGNORECASE,
)


def get_analyzer() -> AnalyzerEngine:
    global _ANALYZER
    if _ANALYZER is not None:
        return _ANALYZER

    nlp_configuration = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": DEFAULT_SPACY_MODEL}],
    }

    try:
        provider = NlpEngineProvider(nlp_configuration=nlp_configuration)
        nlp_engine = provider.create_engine()
        _ANALYZER = AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])
    except Exception as primary_exc:
        LOGGER.warning("Transformer Presidio initialization failed: %s", str(primary_exc))
        try:
            fallback_provider = NlpEngineProvider(
                nlp_configuration={
                    "nlp_engine_name": "spacy",
                    "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
                }
            )
            fallback_engine = fallback_provider.create_engine()
            _ANALYZER = AnalyzerEngine(nlp_engine=fallback_engine, supported_languages=["en"])
        except Exception as fallback_exc:
            raise RuntimeError(
                "Presidio AnalyzerEngine could not initialize. "
                "Install spaCy transformer model with: python -m spacy download en_core_web_trf"
            ) from fallback_exc

    return _ANALYZER


def resolve_target_pii_entities(analyzer: AnalyzerEngine) -> List[str]:
    supported_entities = sorted(set(analyzer.get_supported_entities(language="en")))
    if not supported_entities:
        raise RuntimeError("Presidio has no supported entities configured for language 'en'.")

    env_override = os.getenv("TARGET_PII_ENTITIES", "").strip()
    if env_override:
        if env_override.upper() == "ALL":
            return supported_entities
        requested_entities = [item.strip() for item in env_override.split(",") if item.strip()]
        selected_entities = [item for item in requested_entities if item in supported_entities]
        unknown_entities = sorted(set(requested_entities) - set(selected_entities))
        if unknown_entities:
            LOGGER.warning("Ignoring unsupported TARGET_PII_ENTITIES values: %s", ", ".join(unknown_entities))
        if selected_entities:
            return selected_entities
        LOGGER.warning("TARGET_PII_ENTITIES override did not match any supported entities. Falling back to all entities.")

    if TARGET_PII_ENTITIES:
        selected_static_entities = [item for item in TARGET_PII_ENTITIES if item in supported_entities]
        if selected_static_entities:
            return selected_static_entities
        LOGGER.warning("Static TARGET_PII_ENTITIES did not match supported entities. Falling back to all entities.")

    return supported_entities


def get_text_chunks(text: str, chunk_size: int = 2000, overlap: int = 200) -> List[Dict[str, Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")
    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >= 0 and smaller than chunk_size.")
    normalized_text = text or ""
    if not normalized_text:
        return []
    chunks: List[Dict[str, Any]] = []
    cursor = 0
    text_length = len(normalized_text)
    while cursor < text_length:
        end_index = min(text_length, cursor + chunk_size)
        chunks.append({"chunk_text": normalized_text[cursor:end_index], "global_offset": cursor})
        if end_index >= text_length:
            break
        cursor = max(cursor + 1, end_index - overlap)
    return chunks


def _is_ignored_key(key_path: str) -> bool:
    lower_key = key_path.lower()
    if lower_key in IGNORE_JSON_KEYS:
        return True
    return lower_key.split(".")[-1] in {"id", "filename"}


def _ranges_overlap(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    return start_a < end_b and end_a > start_b


def _json_scalar_to_text(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _flatten_json_values(value: Any, parent_key: str = "") -> List[Tuple[str, Any]]:
    flattened: List[Tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, nested_value in value.items():
            key_part = str(key).strip()
            key_path = f"{parent_key}.{key_part}" if parent_key else key_part
            if _is_ignored_key(key_path):
                continue
            if isinstance(nested_value, (dict, list)):
                flattened.extend(_flatten_json_values(nested_value, key_path))
            else:
                flattened.append((key_path, nested_value))
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, (dict, list)):
                flattened.extend(_flatten_json_values(item, parent_key))
            else:
                flattened.append((parent_key, item))
    return flattened


def _locate_value_segments_in_json(
    canonical_text: str, segment_start: int, segment_end: int, parsed_obj: Any,
) -> List[Tuple[int, int]]:
    flat_values = _flatten_json_values(parsed_obj)
    if not flat_values:
        return []
    scope_text = canonical_text[segment_start:segment_end]
    scoped_segments: List[Tuple[int, int]] = []
    for key_path, raw_value in flat_values:
        if _is_ignored_key(key_path):
            continue
        value_text = _json_scalar_to_text(raw_value).strip()
        if not value_text:
            continue
        for match in re.finditer(re.escape(value_text), scope_text):
            abs_start = segment_start + match.start()
            abs_end = segment_start + match.end()
            if any(_ranges_overlap(abs_start, abs_end, start, end) for start, end in scoped_segments):
                continue
            scoped_segments.append((abs_start, abs_end))
            break
    return scoped_segments


def _extract_json_value_segments_regex(canonical_text: str) -> List[Tuple[int, int]]:
    pattern = re.compile(r'(?P<key>"?[A-Za-z0-9_.-]+"?)\s*:\s*(?P<value>"(?:\\.|[^"])*"|[^,\}\]\n]+)')
    segments: List[Tuple[int, int]] = []
    for match in pattern.finditer(canonical_text):
        raw_key = match.group("key").strip().strip('"')
        if _is_ignored_key(raw_key):
            continue
        value_start = match.start("value")
        value_end = match.end("value")
        while value_start < value_end and canonical_text[value_start].isspace():
            value_start += 1
        while value_end > value_start and canonical_text[value_end - 1].isspace():
            value_end -= 1
        if value_end - value_start >= 2 and canonical_text[value_start] == '"' and canonical_text[value_end - 1] == '"':
            value_start += 1
            value_end -= 1
        if value_start < value_end:
            segments.append((value_start, value_end))
    return segments


def _extract_json_value_segments(canonical_text: str) -> List[Tuple[int, int]]:
    decoder = json.JSONDecoder()
    collected_segments: List[Tuple[int, int]] = []
    cursor = 0

    while cursor < len(canonical_text):
        opening = re.search(r"[\{\[]", canonical_text[cursor:])
        if not opening:
            break
        start_index = cursor + opening.start()
        try:
            parsed_obj, consumed = decoder.raw_decode(canonical_text[start_index:])
        except json.JSONDecodeError:
            cursor = start_index + 1
            continue
        end_index = start_index + consumed
        collected_segments.extend(_locate_value_segments_in_json(canonical_text, start_index, end_index, parsed_obj))
        cursor = end_index

    if collected_segments:
        return _normalize_segments(collected_segments)
    regex_segments = _extract_json_value_segments_regex(canonical_text)
    return _normalize_segments(regex_segments)


def _normalize_segments(segments: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
    filtered = sorted((start, end) for start, end in segments if end > start)
    if not filtered:
        return []
    normalized: List[Tuple[int, int]] = []
    for start, end in filtered:
        if not normalized:
            normalized.append((start, end))
            continue
        previous_start, previous_end = normalized[-1]
        if start <= previous_end:
            normalized[-1] = (previous_start, max(previous_end, end))
        else:
            normalized.append((start, end))
    return normalized


def _project_text_from_segments(canonical_text: str, segments: Sequence[Tuple[int, int]]) -> Tuple[str, List[int]]:
    chunks: List[str] = []
    offset_map: List[int] = []
    for index, (start_char, end_char) in enumerate(segments):
        if start_char >= end_char:
            continue
        if index > 0:
            chunks.append("\n")
            anchor = max(0, start_char - 1)
            offset_map.append(anchor)
        chunks.append(canonical_text[start_char:end_char])
        offset_map.extend(range(start_char, end_char))
    return "".join(chunks), offset_map


def _remap_offsets_to_canonical(
    start_char: int, end_char: int, offset_map: Sequence[int], canonical_length: int,
) -> Optional[Tuple[int, int]]:
    if canonical_length <= 0 or end_char <= start_char or not offset_map:
        return None
    bounded_start = max(0, min(start_char, len(offset_map) - 1))
    bounded_end = max(0, min(end_char - 1, len(offset_map) - 1))
    canonical_start = offset_map[bounded_start]
    canonical_end = offset_map[bounded_end] + 1
    if canonical_end <= canonical_start:
        return None
    return canonical_start, canonical_end


def _prepare_text_for_presidio(canonical_text: str) -> Tuple[str, List[int]]:
    value_segments = _extract_json_value_segments(canonical_text)
    if not value_segments:
        return canonical_text, list(range(len(canonical_text)))
    projected_text, projected_offset_map = _project_text_from_segments(canonical_text, value_segments)
    if not projected_text.strip() or not projected_offset_map:
        return "", []
    return projected_text, projected_offset_map


def _reclassify_entity_type(entity_text: str, original_type: str) -> str:
    if original_type != "PERSON":
        return original_type
    if BUSINESS_KEYWORD_PATTERN.search(entity_text):
        return "ORGANIZATION"
    return original_type


def _maybe_promote_contextual_identifier(
    *,
    entity_text: str,
    entity_type: str,
    confidence: float,
    chunk_text: str,
    start_char: int,
    end_char: int,
) -> Optional[Tuple[str, float]]:
    if confidence >= MIN_ENTITY_CONFIDENCE:
        return None

    if entity_type in {"URL", "EMAIL_ADDRESS"}:
        return entity_type, max(confidence, 0.85)

    digits_only = re.sub(r"\D", "", entity_text)
    context_start = max(0, start_char - 48)
    context_end = min(len(chunk_text), end_char + 24)
    local_context = chunk_text[context_start:context_end]

    if 4 <= len(digits_only) <= 7 and _CONTEXTUAL_SHORT_CODE_PATTERN.search(local_context):
        return "SECURITY_CODE", max(confidence, 0.89)

    if entity_type not in {"PHONE_NUMBER", "US_BANK_NUMBER", "US_DRIVER_LICENSE"}:
        return None
    if len(digits_only) < 8 or len(digits_only) > 18:
        return None
    if not _CONTEXTUAL_IDENTIFIER_PATTERN.search(local_context):
        return None
    return "CUSTOMER_IDENTIFIER", max(confidence, 0.86)


def run_presidio_triage(
    canonical_text: str,
    char_map: Sequence[Tuple[int, int, BoundingBox]],
    *,
    chunk_size: int = 2000,
    overlap: int = 200,
    base_global_offset: int = 0,
) -> List[Detection]:
    if not canonical_text.strip():
        return []

    analyzer = get_analyzer()
    target_entities = resolve_target_pii_entities(analyzer)
    text_chunks = get_text_chunks(canonical_text, chunk_size=chunk_size, overlap=overlap)
    detections: List[Detection] = []

    for chunk in text_chunks:
        chunk_text = str(chunk.get("chunk_text") or "")
        if not chunk_text.strip():
            continue
        chunk_offset_raw = chunk.get("global_offset", 0)
        try:
            chunk_offset = int(chunk_offset_raw)
        except (TypeError, ValueError):
            chunk_offset = 0
        chunk_global_offset = base_global_offset + chunk_offset

        analyzable_text, offset_map = _prepare_text_for_presidio(chunk_text)
        if not analyzable_text.strip():
            continue

        results = analyzer.analyze(text=analyzable_text, entities=target_entities, language="en")

        for result in sorted(results, key=lambda item: (item.start, item.end)):
            if result.end <= result.start:
                continue
            remapped_offsets = _remap_offsets_to_canonical(
                result.start, result.end, offset_map, len(chunk_text),
            )
            if remapped_offsets is None:
                continue
            chunk_start, chunk_end = remapped_offsets
            entity_text = chunk_text[chunk_start:chunk_end].strip()
            if not entity_text:
                continue

            confidence = float(result.score or 0.0)
            entity_type = _reclassify_entity_type(entity_text, result.entity_type)
            promoted = _maybe_promote_contextual_identifier(
                entity_text=entity_text, entity_type=entity_type, confidence=confidence,
                chunk_text=chunk_text, start_char=chunk_start, end_char=chunk_end,
            )
            if promoted is not None:
                entity_type, confidence = promoted
            if confidence < MIN_ENTITY_CONFIDENCE:
                continue

            absolute_start = chunk_global_offset + chunk_start
            absolute_end = chunk_global_offset + chunk_end
            boxes = get_bboxes_for_offsets(absolute_start, absolute_end, char_map)
            if not boxes:
                continue

            detections.append(
                Detection(
                    entity_text=entity_text,
                    entity_type=entity_type,
                    confidence_score=confidence,
                    source="Presidio",
                    boxes=boxes,
                    supporting_sources=["Presidio"],
                    decision_reason="single_source_presidio",
                )
            )

    return detections


def run_contextual_numeric_triage(
    canonical_text: str,
    char_map: Sequence[Tuple[int, int, BoundingBox]],
) -> List[Detection]:
    if not canonical_text.strip() or not char_map:
        return []

    detections: List[Detection] = []

    for match in _CONTEXTUAL_CUSTOMER_IDENTIFIER_RULE.finditer(canonical_text):
        start_char, end_char = match.span(1)
        entity_text = canonical_text[start_char:end_char].strip()
        if not entity_text:
            continue
        digits_only = re.sub(r"\D", "", entity_text)
        if len(digits_only) < 6 and len(entity_text) < 6:
            continue
        boxes = get_bboxes_for_offsets(start_char, end_char, char_map)
        if not boxes:
            continue
        detections.append(
            Detection(
                entity_text=entity_text,
                entity_type="CUSTOMER_IDENTIFIER",
                confidence_score=max(MIN_ENTITY_CONFIDENCE, 0.9),
                source="Presidio",
                boxes=boxes,
                supporting_sources=["Presidio"],
                decision_reason="contextual_numeric_rule",
            )
        )

    for match in _CONTEXTUAL_SECURITY_CODE_RULE.finditer(canonical_text):
        start_char, end_char = match.span(1)
        entity_text = canonical_text[start_char:end_char].strip()
        if not entity_text:
            continue
        boxes = get_bboxes_for_offsets(start_char, end_char, char_map)
        if not boxes:
            continue
        detections.append(
            Detection(
                entity_text=entity_text,
                entity_type="SECURITY_CODE",
                confidence_score=max(MIN_ENTITY_CONFIDENCE, 0.92),
                source="Presidio",
                boxes=boxes,
                supporting_sources=["Presidio"],
                decision_reason="contextual_short_code_rule",
            )
        )

    return detections
