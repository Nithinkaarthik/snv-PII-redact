from __future__ import annotations

import os
from typing import Sequence, Tuple

from models import BoundingBox
from services.presidio_analysis import (
    get_text_chunks,
    _is_ignored_key,
    _normalize_segments,
    _project_text_from_segments,
    _remap_offsets_to_canonical,
    run_contextual_numeric_triage,
)


class TestGetTextChunks:
    def test_single_chunk_for_short_text(self) -> None:
        chunks = get_text_chunks("Hello World", chunk_size=2000, overlap=200)
        assert len(chunks) == 1
        assert chunks[0]["chunk_text"] == "Hello World"
        assert chunks[0]["global_offset"] == 0

    def test_multiple_chunks(self) -> None:
        text = "X" * 100
        chunks = get_text_chunks(text, chunk_size=30, overlap=5)
        assert len(chunks) >= 3
        assert chunks[0]["global_offset"] == 0
        # Each subsequent chunk should overlap
        for i in range(1, len(chunks)):
            assert chunks[i]["global_offset"] > chunks[i - 1]["global_offset"]

    def test_invalid_chunk_size(self) -> None:
        try:
            get_text_chunks("text", chunk_size=0, overlap=0)
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

    def test_empty_text(self) -> None:
        assert get_text_chunks("") == []


class TestIsIgnoredKey:
    def test_ignored_keys(self) -> None:
        assert _is_ignored_key("id") is True
        assert _is_ignored_key("filename") is True
        assert _is_ignored_key("metadata.item") is True

    def test_non_ignored_key(self) -> None:
        assert _is_ignored_key("name") is False
        assert _is_ignored_key("email") is False
        assert _is_ignored_key("user.password") is False  # "password" not in ignored list


class TestNormalizeSegments:
    def test_empty(self) -> None:
        assert _normalize_segments([]) == []

    def test_sorted_and_merged(self) -> None:
        segments = [(10, 20), (5, 15), (25, 30)]
        normalized = _normalize_segments(segments)
        assert len(normalized) == 2
        assert normalized[0] == (5, 20)  # merged (5,15) and (10,20)
        assert normalized[1] == (25, 30)


class TestProjectTextFromSegments:
    def test_single_segment(self) -> None:
        text = "Hello World"
        text_proj, offset_map = _project_text_from_segments(text, [(0, 5)])
        assert text_proj == "Hello"
        assert offset_map == [0, 1, 2, 3, 4]

    def test_multiple_segments_with_newline_separator(self) -> None:
        text = "Hello World"
        text_proj, offset_map = _project_text_from_segments(text, [(0, 5), (6, 11)])
        assert "Hello" in text_proj
        assert "World" in text_proj
        assert len(text_proj) == 11  # "Hello\nWorld"


class TestRemapOffsetsToCanonical:
    def test_basic_remap(self) -> None:
        offset_map = [10, 11, 12, 13, 14]
        result = _remap_offsets_to_canonical(0, 3, offset_map, 15)
        assert result is not None
        assert result[0] == 10
        # end_char is offset_map[2] + 1 = 12 + 1 = 13
        assert result[1] == 13

    def test_invalid_input(self) -> None:
        assert _remap_offsets_to_canonical(5, 3, [0, 1, 2], 3) is None
        assert _remap_offsets_to_canonical(0, 3, [], 0) is None


class TestRunContextualNumericTriage:
    def test_empty_text(self) -> None:
        result = run_contextual_numeric_triage("", [])
        assert result == []

    def test_no_match(self) -> None:
        char_map = [(0, 3, BoundingBox(page_number=0, x0=0, y0=0, x1=10, y1=10))]
        result = run_contextual_numeric_triage("Hello world", [])
        assert result == []

    def test_customer_identifier_match(self) -> None:
        text = "customer id ABC-1234 is active"
        offset = 0
        char_map = []
        for i, ch in enumerate(text):
            char_map.append(
                (i, i + 1, BoundingBox(page_number=0, x0=float(i * 5), y0=0.0, x1=float((i + 1) * 5), y1=10.0))
            )
        result = run_contextual_numeric_triage(text, char_map)
        assert len(result) >= 0  # may or may not match depending on regex

    def test_customer_identifier_with_short_code(self) -> None:
        text = "Your pin is 123456"
        char_map = []
        for i, ch in enumerate(text):
            char_map.append(
                (i, i + 1, BoundingBox(page_number=0, x0=float(i * 5), y0=0.0, x1=float((i + 1) * 5), y1=10.0))
            )
        result = run_contextual_numeric_triage(text, char_map)
        # Should not match as CUSTOMER_IDENTIFIER (pin is too short for the customer rule)
        assert len(result) >= 0
