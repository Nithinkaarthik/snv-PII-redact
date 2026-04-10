from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

try:
    from backend.models import BoundingBox, OCRWord, WordSpan
except ImportError:
    from models import BoundingBox, OCRWord, WordSpan


def deduplicate_boxes(boxes: Sequence[BoundingBox]) -> List[BoundingBox]:
    unique: List[BoundingBox] = []
    seen = set()

    for box in boxes:
        key = (
            box.page_number,
            round(box.x0, 2),
            round(box.y0, 2),
            round(box.x1, 2),
            round(box.y1, 2),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(box)

    return unique


def _merge_boxes(boxes: Sequence[BoundingBox]) -> BoundingBox:
    first = boxes[0]
    return BoundingBox(
        page_number=first.page_number,
        x0=min(box.x0 for box in boxes),
        y0=min(box.y0 for box in boxes),
        x1=max(box.x1 for box in boxes),
        y1=max(box.y1 for box in boxes),
    )


def build_coordinate_maps(
    words: Sequence[OCRWord],
) -> Tuple[Dict[str, List[BoundingBox]], Dict[str, List[BoundingBox]]]:
    word_map: Dict[str, List[BoundingBox]] = {}
    line_groups: Dict[Tuple[int, str], List[OCRWord]] = {}

    for word in words:
        word_map.setdefault(word.text, []).append(word.bbox)
        line_groups.setdefault((word.bbox.page_number, word.line_key), []).append(word)

    phrase_map: Dict[str, List[BoundingBox]] = {}
    for (_page_number, _line_key), line_words in line_groups.items():
        sorted_line_words = sorted(line_words, key=lambda item: item.bbox.x0)
        phrase = " ".join(item.text for item in sorted_line_words).strip()
        if not phrase:
            continue
        phrase_box = _merge_boxes([item.bbox for item in sorted_line_words])
        phrase_map.setdefault(phrase, []).append(phrase_box)

    return word_map, phrase_map


def build_character_bbox_map(
    words: Sequence[OCRWord],
) -> Tuple[str, List[Tuple[int, int, BoundingBox]], List[WordSpan]]:
    if not words:
        return "", [], []

    text_parts: List[str] = []
    char_map: List[Tuple[int, int, BoundingBox]] = []
    word_spans: List[WordSpan] = []

    cursor = 0
    previous_page = words[0].bbox.page_number
    previous_line_key = words[0].line_key
    previous_text = ""

    for index, word in enumerate(words):
        if index > 0:
            if word.bbox.page_number != previous_page:
                separator = "\n"
            elif previous_text.endswith("-") and previous_line_key != word.line_key:
                separator = ""
            else:
                separator = " "

            text_parts.append(separator)
            cursor += len(separator)

        start_char = cursor
        text_parts.append(word.text)
        cursor += len(word.text)
        end_char = cursor

        char_map.append((start_char, end_char, word.bbox))
        word_spans.append(
            WordSpan(
                text=word.text,
                start_char=start_char,
                end_char=end_char,
                bbox=word.bbox,
                line_key=word.line_key,
            )
        )

        previous_page = word.bbox.page_number
        previous_line_key = word.line_key
        previous_text = word.text

    return "".join(text_parts), char_map, word_spans


def get_bboxes_for_offsets(
    start_char: int,
    end_char: int,
    char_map: Sequence[Tuple[int, int, BoundingBox]],
) -> List[BoundingBox]:
    if end_char <= start_char:
        return []

    overlapping_boxes = [
        bbox
        for span_start, span_end, bbox in char_map
        if span_start < end_char and span_end > start_char
    ]
    return deduplicate_boxes(sorted(overlapping_boxes, key=lambda box: (box.page_number, box.y0, box.x0)))
