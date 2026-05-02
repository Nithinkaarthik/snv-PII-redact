from __future__ import annotations

import os
import re
from typing import List, Sequence, Tuple

from rapidfuzz import fuzz

try:
    from backend.config import FUZZY_MATCH_THRESHOLD, LOGGER
    from backend.models import WordSpan
    from backend.text_mapping import get_bboxes_for_offsets
except ImportError:
    from config import FUZZY_MATCH_THRESHOLD, LOGGER
    from models import WordSpan
    from text_mapping import get_bboxes_for_offsets

_DEBUG_FALSE_VALUES = {"0", "false", "no", "off"}
_DEBUG_ENABLED = os.getenv("BACKEND_DEBUG_BLOCKS", "0").strip().lower() not in _DEBUG_FALSE_VALUES

FUZZY_MAX_TOKEN_PADDING = max(1, int(os.getenv("FUZZY_MAX_TOKEN_PADDING", "2")))
FUZZY_MIN_TOKEN_PADDING = max(0, int(os.getenv("FUZZY_MIN_TOKEN_PADDING", "1")))
FUZZY_LENGTH_PENALTY_PER_EXTRA_TOKEN = max(
    0.0,
    min(8.0, float(os.getenv("FUZZY_LENGTH_PENALTY_PER_EXTRA_TOKEN", "2.0"))),
)
FUZZY_LENGTH_PENALTY_CAP = max(
    FUZZY_LENGTH_PENALTY_PER_EXTRA_TOKEN,
    min(25.0, float(os.getenv("FUZZY_LENGTH_PENALTY_CAP", "14.0"))),
)


def _debug(message: str, *args: object) -> None:
    if not _DEBUG_ENABLED:
        return
    LOGGER.info("[DEBUG] " + message, *args)


def normalize_for_fuzzy(text: str) -> str:
    normalized = str(text or "").upper()
    normalized = re.sub(r"(?<=\d)[OQ](?=\d|$)", "0", normalized)
    normalized = re.sub(r"(?<=\d)[IL](?=\d|$)", "1", normalized)
    normalized = re.sub(r"[^A-Z0-9$]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _adaptive_fuzzy_threshold(normalized_quote: str, base_threshold: int) -> int:
    token_count = max(1, len(normalized_quote.split()))
    char_count = len(normalized_quote)

    if token_count >= 6 or char_count >= 45:
        return max(80, base_threshold - 10)
    if token_count >= 4 or char_count >= 28:
        return max(84, base_threshold - 8)
    return max(88, base_threshold - 4)


def find_fuzzy_spans(
    quote: str,
    word_spans: Sequence[WordSpan],
    threshold: int = FUZZY_MATCH_THRESHOLD,
) -> List[Tuple[int, int, float]]:
    normalized_quote = normalize_for_fuzzy(quote)
    if not normalized_quote or not word_spans:
        return []

    raw_token_count = max(1, len(quote.split()))
    norm_token_count = max(1, len(normalized_quote.split()))
    quote_token_count = min(raw_token_count, norm_token_count)
    min_window = max(1, quote_token_count - FUZZY_MIN_TOKEN_PADDING)
    max_window = max(min_window, quote_token_count + FUZZY_MAX_TOKEN_PADDING)
    window_sizes = list(range(min_window, max_window + 1))

    _debug(
        "FUZZY_WINDOW_CONFIG quote=%s raw_tokens=%s norm_tokens=%s min_window=%s max_window=%s",
        quote, raw_token_count, norm_token_count, min_window, max_window,
    )

    candidates: List[Tuple[float, int, int]] = []
    total_words = len(word_spans)

    for start_index in range(total_words):
        for window_size in window_sizes:
            end_index = start_index + window_size
            if end_index > total_words:
                continue

            left = word_spans[start_index]
            right = word_spans[end_index - 1]
            if left.bbox.page_number != right.bbox.page_number:
                continue

            candidate_text = " ".join(item.text for item in word_spans[start_index:end_index])
            normalized_candidate = normalize_for_fuzzy(candidate_text)
            if not normalized_candidate:
                continue

            ratio_score = float(fuzz.ratio(normalized_quote, normalized_candidate))
            token_sort_score = float(fuzz.token_sort_ratio(normalized_quote, normalized_candidate))
            token_set_score = float(fuzz.token_set_ratio(normalized_quote, normalized_candidate))
            partial_score = float(fuzz.partial_ratio(normalized_quote, normalized_candidate))

            similarity = max(ratio_score, token_sort_score, token_set_score, partial_score)

            quote_tokens_len = max(1, len(normalized_quote.split()))
            candidate_tokens_len = max(1, len(normalized_candidate.split()))
            extra_tokens = max(0, candidate_tokens_len - quote_tokens_len)
            if extra_tokens > 0:
                similarity -= min(
                    FUZZY_LENGTH_PENALTY_CAP,
                    float(extra_tokens) * FUZZY_LENGTH_PENALTY_PER_EXTRA_TOKEN,
                )

            if (normalized_quote in normalized_candidate or normalized_candidate in normalized_quote) and extra_tokens <= 2:
                similarity = max(similarity, 96.0)

            if similarity >= max(float(threshold) - 3.0, 80.0):
                _debug(
                    "FUZZY_NEAR_THRESHOLD quote=%s candidate=%s extra_tokens=%s sim=%.2f",
                    quote, candidate_text, extra_tokens, similarity,
                )

            candidates.append((similarity, left.start_char, right.end_char))

    def _select_non_overlapping(min_similarity: float) -> List[Tuple[float, int, int]]:
        selected: List[Tuple[float, int, int]] = []
        for similarity, start_char, end_char in sorted(candidates, key=lambda item: (-item[0], item[1], item[2])):
            if similarity < min_similarity:
                continue
            overlaps = any(
                not (end_char <= chosen_start or start_char >= chosen_end)
                for _similarity, chosen_start, chosen_end in selected
            )
            if overlaps:
                continue
            selected.append((similarity, start_char, end_char))
        return selected

    selected = _select_non_overlapping(float(threshold))
    if not selected:
        relaxed_threshold = _adaptive_fuzzy_threshold(normalized_quote, threshold)
        if relaxed_threshold < threshold:
            selected = _select_non_overlapping(float(relaxed_threshold))

    return [(start_char, end_char, similarity) for similarity, start_char, end_char in selected]
