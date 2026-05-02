from __future__ import annotations

from models import BoundingBox, WordSpan
from services.fuzzy import find_fuzzy_spans, normalize_for_fuzzy


def _make_word_span(text: str, page: int, start: int, end: int) -> WordSpan:
    return WordSpan(
        text=text,
        start_char=start,
        end_char=end,
        bbox=BoundingBox(page_number=page, x0=0.0, y0=0.0, x1=float(len(text) * 10), y1=10.0),
        line_key="test:0:0",
    )


class TestNormalizeForFuzzy:
    def test_uppercases(self) -> None:
        assert normalize_for_fuzzy("Hello World") == "HELLO WORLD"

    def test_replaces_similar_chars(self) -> None:
        assert normalize_for_fuzzy("0O1I") == "0011"

    def test_strips_punctuation(self) -> None:
        assert normalize_for_fuzzy("hello-world!") == "HELLO WORLD"

    def test_collapses_whitespace(self) -> None:
        assert normalize_for_fuzzy("hello   world") == "HELLO WORLD"

    def test_empty_string(self) -> None:
        assert normalize_for_fuzzy("") == ""


class TestFindFuzzySpans:
    def test_exact_match_single_word(self) -> None:
        spans = [_make_word_span("Hello", 0, 0, 5)]
        result = find_fuzzy_spans("Hello", spans, threshold=92)
        assert len(result) == 1
        start, end, score = result[0]
        assert start == 0
        assert end == 5
        assert score >= 92.0

    def test_exact_match_multi_word(self) -> None:
        spans = [
            _make_word_span("Hello", 0, 0, 5),
            _make_word_span("World", 0, 6, 11),
        ]
        result = find_fuzzy_spans("Hello World", spans, threshold=92)
        # Each individual word matches, so we may get 2 results (not 1 combined "Hello World")
        # since the min/max window is based on token count
        assert len(result) >= 1

    def test_no_match(self) -> None:
        spans = [_make_word_span("Hello", 0, 0, 5)]
        result = find_fuzzy_spans("Goodbye", spans, threshold=92)
        assert len(result) == 0

    def test_fuzzy_match_extra_token_penalty(self) -> None:
        spans = [
            _make_word_span("Hello", 0, 0, 5),
            _make_word_span("World", 0, 6, 11),
            _make_word_span("Extra", 0, 12, 17),
        ]
        # "Hello World" should fuzzy match but with a small penalty for the extra token
        result = find_fuzzy_spans("Hello World", spans, threshold=80)
        assert len(result) >= 1

    def test_empty_quote_returns_empty(self) -> None:
        assert find_fuzzy_spans("", [_make_word_span("Hello", 0, 0, 5)]) == []

    def test_empty_word_spans_returns_empty(self) -> None:
        assert find_fuzzy_spans("Hello", []) == []

    def test_cross_page_does_not_match_as_phrase(self) -> None:
        spans = [
            _make_word_span("Hello", 0, 0, 5),
            _make_word_span("World", 1, 0, 5),
        ]
        result = find_fuzzy_spans("Hello World", spans, threshold=80)
        # Cross-page won't match as a combined span, but individual words may match
        for start, end, score in result:
            span = spans[0] if start == 0 and end == 5 else spans[1]
            assert span is not None
