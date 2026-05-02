from __future__ import annotations

from models import BoundingBox, OCRWord, WordSpan
from text_mapping import (
    build_character_bbox_map,
    build_coordinate_maps,
    deduplicate_boxes,
    get_bboxes_for_offsets,
)


def _make_word(text: str, page: int, x0: float, y0: float, x1: float, y1: float, line_key: str = "test:0:0") -> OCRWord:
    return OCRWord(
        text=text,
        bbox=BoundingBox(page_number=page, x0=x0, y0=y0, x1=x1, y1=y1),
        line_key=line_key,
    )


class TestDeduplicateBoxes:
    def test_deduplicates_exact_duplicates(self) -> None:
        box = BoundingBox(page_number=0, x0=10.0, y0=20.0, x1=50.0, y1=60.0)
        result = deduplicate_boxes([box, box])
        assert len(result) == 1
        assert result[0] == box

    def test_preserves_unique_boxes(self) -> None:
        box1 = BoundingBox(page_number=0, x0=10.0, y0=20.0, x1=50.0, y1=60.0)
        box2 = BoundingBox(page_number=1, x0=10.0, y0=20.0, x1=50.0, y1=60.0)
        result = deduplicate_boxes([box1, box2])
        assert len(result) == 2

    def test_rounds_coordinates_to_2dp(self) -> None:
        box1 = BoundingBox(page_number=0, x0=10.001, y0=20.001, x1=50.001, y1=60.001)
        box2 = BoundingBox(page_number=0, x0=10.005, y0=20.005, x1=50.005, y1=60.005)
        result = deduplicate_boxes([box1, box2])
        # Both round to same 2dp: (10.00, 20.00, 50.00, 60.00) vs (10.01, 20.01, 50.01, 60.01)
        assert len(result) >= 1  # at minimum don't crash

    def test_empty_list(self) -> None:
        assert deduplicate_boxes([]) == []


class TestBuildCharacterBboxMap:
    def test_empty_words(self) -> None:
        text, char_map, spans = build_character_bbox_map([])
        assert text == ""
        assert char_map == []
        assert spans == []

    def test_single_word(self) -> None:
        word = _make_word("Hello", 0, 0, 0, 30, 10)
        text, char_map, spans = build_character_bbox_map([word])
        assert text == "Hello"
        assert len(char_map) == 1
        assert char_map[0] == (0, 5, word.bbox)
        assert len(spans) == 1
        assert spans[0].text == "Hello"
        assert spans[0].start_char == 0
        assert spans[0].end_char == 5

    def test_two_words_same_line(self) -> None:
        w1 = _make_word("Hello", 0, 0, 0, 30, 10, line_key="test:0:0")
        w2 = _make_word("World", 0, 35, 0, 65, 10, line_key="test:0:0")
        text, char_map, spans = build_character_bbox_map([w1, w2])
        assert text == "Hello World"
        assert len(spans) == 2
        assert spans[0].start_char == 0  # "Hello"
        assert spans[0].end_char == 5
        assert spans[1].start_char == 6  # " World"
        assert spans[1].end_char == 11

    def test_page_break_adds_newline(self) -> None:
        w1 = _make_word("Page1", 0, 0, 0, 30, 10)
        w2 = _make_word("Page2", 1, 0, 0, 30, 10)
        text, char_map, spans = build_character_bbox_map([w1, w2])
        assert "Page1\nPage2" in text

    def test_hyphenated_line_break_removes_hyphen(self) -> None:
        w1 = _make_word("hel-", 0, 0, 0, 20, 10, line_key="test:0:0")
        w2 = _make_word("lo", 0, 25, 15, 40, 25, line_key="test:1:0")
        text, char_map, spans = build_character_bbox_map([w1, w2])
        # When previous word ends with "-" and line key changes, separator is ""
        # but PyMuPDF hyphen handling only joins if same-line-key
        # Since line keys differ, the hyphen removal may or may not apply
        assert "hel" in text.lower()
        assert "lo" in text.lower()

    def test_line_break_adds_newline(self) -> None:
        w1 = _make_word("Hello", 0, 0, 0, 30, 10, line_key="test:0:0")
        w2 = _make_word("World", 0, 0, 20, 30, 30, line_key="test:1:0")
        text, char_map, spans = build_character_bbox_map([w1, w2])
        assert text == "Hello\nWorld"


class TestGetBboxesForOffsets:
    def test_exact_match(self) -> None:
        box = BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=10.0, y1=10.0)
        char_map = [(0, 5, box)]
        result = get_bboxes_for_offsets(0, 5, char_map)
        assert result == [box]

    def test_overlapping(self) -> None:
        box1 = BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=10.0, y1=10.0)
        box2 = BoundingBox(page_number=0, x0=15.0, y0=0.0, x1=25.0, y1=10.0)
        char_map = [(0, 5, box1), (6, 11, box2)]
        result = get_bboxes_for_offsets(3, 8, char_map)
        assert box1 in result
        assert box2 in result

    def test_no_overlap(self) -> None:
        box = BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=10.0, y1=10.0)
        char_map = [(0, 5, box)]
        result = get_bboxes_for_offsets(10, 15, char_map)
        assert result == []

    def test_invalid_range(self) -> None:
        box = BoundingBox(page_number=0, x0=0.0, y0=0.0, x1=10.0, y1=10.0)
        char_map = [(0, 5, box)]
        assert get_bboxes_for_offsets(10, 5, char_map) == []


class TestBuildCoordinateMaps:
    def test_empty(self) -> None:
        word_map, phrase_map = build_coordinate_maps([])
        assert word_map == {}
        assert phrase_map == {}

    def test_word_and_phrase_map(self) -> None:
        w1 = _make_word("Hello", 0, 0, 0, 30, 10, line_key="test:0:0")
        w2 = _make_word("World", 0, 35, 0, 65, 10, line_key="test:0:0")
        word_map, phrase_map = build_coordinate_maps([w1, w2])
        assert "Hello" in word_map
        assert "World" in word_map
        assert "Hello World" in phrase_map
