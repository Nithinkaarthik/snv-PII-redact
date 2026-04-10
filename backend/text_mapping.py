from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Set, Tuple

try:
    from backend.models import BoundingBox, OCRWord, TableRegion, WordSpan
except ImportError:
    from models import BoundingBox, OCRWord, TableRegion, WordSpan


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
    table_regions: Optional[Sequence[TableRegion]] = None,
) -> Tuple[str, List[Tuple[int, int, BoundingBox]], List[WordSpan]]:
    if not words:
        return "", [], []

    if table_regions:
        return _build_character_bbox_map_with_tables(words, table_regions)

    return _build_character_bbox_map_linear(words)


def _build_character_bbox_map_linear(
    words: Sequence[OCRWord],
) -> Tuple[str, List[Tuple[int, int, BoundingBox]], List[WordSpan]]:
    text_parts: List[str] = []
    char_map: List[Tuple[int, int, BoundingBox]] = []
    word_spans: List[WordSpan] = []

    cursor = 0
    previous_page: Optional[int] = None
    previous_line_key: Optional[str] = None
    previous_text = ""

    for word in words:
        separator = _separator_for_word(
            word,
            previous_page=previous_page,
            previous_line_key=previous_line_key,
            previous_text=previous_text,
        )
        cursor = _append_word_token(
            word,
            separator,
            text_parts,
            char_map,
            word_spans,
            cursor,
        )
        previous_page = word.bbox.page_number
        previous_line_key = word.line_key
        previous_text = word.text

    return "".join(text_parts), char_map, word_spans


def _build_character_bbox_map_with_tables(
    words: Sequence[OCRWord],
    table_regions: Sequence[TableRegion],
) -> Tuple[str, List[Tuple[int, int, BoundingBox]], List[WordSpan]]:
    text_parts: List[str] = []
    char_map: List[Tuple[int, int, BoundingBox]] = []
    word_spans: List[WordSpan] = []

    ordered_regions = sorted(table_regions, key=lambda region: (region.bbox.page_number, region.bbox.y0, region.bbox.x0))
    word_to_region: Dict[int, int] = {}
    for region_index, region in enumerate(ordered_regions):
        for cell in region.cells:
            for word_index in cell.word_indexes:
                if 0 <= word_index < len(words) and word_index not in word_to_region:
                    word_to_region[word_index] = region_index

    emitted_regions: Set[int] = set()
    emitted_words: Set[int] = set()
    cursor = 0
    previous_page: Optional[int] = None
    previous_line_key: Optional[str] = None
    previous_text = ""

    for word_index, word in enumerate(words):
        if word_index in emitted_words:
            continue

        region_index = word_to_region.get(word_index)
        if region_index is not None and region_index not in emitted_regions:
            cursor, previous_page, previous_line_key, previous_text = _append_table_region_block(
                ordered_regions[region_index],
                words,
                text_parts,
                char_map,
                word_spans,
                emitted_words,
                cursor,
            )
            emitted_regions.add(region_index)
            continue

        separator = _separator_for_word(
            word,
            previous_page=previous_page,
            previous_line_key=previous_line_key,
            previous_text=previous_text,
        )
        cursor = _append_word_token(
            word,
            separator,
            text_parts,
            char_map,
            word_spans,
            cursor,
        )
        emitted_words.add(word_index)
        previous_page = word.bbox.page_number
        previous_line_key = word.line_key
        previous_text = word.text

    return "".join(text_parts), char_map, word_spans


def _append_table_region_block(
    table_region: TableRegion,
    words: Sequence[OCRWord],
    text_parts: List[str],
    char_map: List[Tuple[int, int, BoundingBox]],
    word_spans: List[WordSpan],
    emitted_words: Set[int],
    cursor: int,
) -> Tuple[int, Optional[int], Optional[str], str]:
    if text_parts and not text_parts[-1].endswith("\n"):
        text_parts.append("\n")
        cursor += 1

    text_parts.append("[TABLE]\n")
    cursor += len("[TABLE]\n")

    cells_by_row: Dict[int, Dict[int, object]] = {}
    for cell in table_region.cells:
        row = cells_by_row.setdefault(cell.row_index, {})
        row[cell.col_index] = cell

    last_page: Optional[int] = None
    last_line_key: Optional[str] = None
    last_text = ""

    for row_index in range(table_region.row_count):
        if row_index > 0:
            text_parts.append("\n")
            cursor += 1

        row_cells = cells_by_row.get(row_index, {})
        for col_index in range(table_region.col_count):
            if col_index > 0:
                text_parts.append(" | ")
                cursor += 3

            cell = row_cells.get(col_index)
            if cell is None:
                continue

            word_indexes = sorted(
                {
                    index
                    for index in cell.word_indexes
                    if 0 <= index < len(words) and index not in emitted_words
                },
                key=lambda index: (words[index].bbox.y0, words[index].bbox.x0),
            )
            for index_in_cell, word_index in enumerate(word_indexes):
                if index_in_cell > 0:
                    text_parts.append(" ")
                    cursor += 1

                word = words[word_index]
                start_char = cursor
                text_parts.append(word.text)
                cursor += len(word.text)
                end_char = cursor

                emitted_words.add(word_index)
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

                last_page = word.bbox.page_number
                last_line_key = word.line_key
                last_text = word.text

    text_parts.append("\n[/TABLE]")
    cursor += len("\n[/TABLE]")

    if last_page is None:
        return cursor, table_region.page_number, "__table_block__", "[/TABLE]"

    return cursor, last_page, last_line_key or "__table_block__", last_text or "[/TABLE]"


def _separator_for_word(
    word: OCRWord,
    *,
    previous_page: Optional[int],
    previous_line_key: Optional[str],
    previous_text: str,
) -> str:
    if previous_page is None or previous_line_key is None:
        return ""

    if word.bbox.page_number != previous_page:
        return "\n"

    if previous_text.endswith("-") and previous_line_key != word.line_key:
        return ""

    if previous_line_key != word.line_key:
        return "\n"

    return " "


def _append_word_token(
    word: OCRWord,
    separator: str,
    text_parts: List[str],
    char_map: List[Tuple[int, int, BoundingBox]],
    word_spans: List[WordSpan],
    cursor: int,
) -> int:
    if separator:
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

    return cursor


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
