from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Dict, List, Optional, Sequence, Tuple

try:
    from backend.config import (
        TABLE_COLUMN_GAP_MIN_PT,
        TABLE_CONTINUATION_MAX_Y_GAP_MULT,
        TABLE_MAX_COLUMN_DRIFT_PT,
        TABLE_MIN_COLS,
        TABLE_MIN_CONFIDENCE,
        TABLE_MIN_ROWS,
        TABLE_PARSER_ENABLED,
        TABLE_ROW_Y_TOLERANCE_PT,
    )
    from backend.models import BoundingBox, OCRWord, TableCell, TableRegion
except ImportError:
    from config import (
        TABLE_COLUMN_GAP_MIN_PT,
        TABLE_CONTINUATION_MAX_Y_GAP_MULT,
        TABLE_MAX_COLUMN_DRIFT_PT,
        TABLE_MIN_COLS,
        TABLE_MIN_CONFIDENCE,
        TABLE_MIN_ROWS,
        TABLE_PARSER_ENABLED,
        TABLE_ROW_Y_TOLERANCE_PT,
    )
    from models import BoundingBox, OCRWord, TableCell, TableRegion


@dataclass
class _CellDraft:
    word_indexes: List[int]
    text: str
    x0: float
    y0: float
    x1: float
    y1: float


@dataclass
class _RowDraft:
    cells: List[_CellDraft]
    y0: float
    y1: float

    @property
    def height(self) -> float:
        return max(0.1, self.y1 - self.y0)


def detect_table_regions(words: Sequence[OCRWord]) -> List[TableRegion]:
    if not TABLE_PARSER_ENABLED:
        return []

    indexed_words = [(index, word) for index, word in enumerate(words) if word.text.strip()]
    if len(indexed_words) < max(4, TABLE_MIN_ROWS * TABLE_MIN_COLS):
        return []

    row_drafts = _build_row_drafts(indexed_words)
    if len(row_drafts) < TABLE_MIN_ROWS:
        return []

    groups = _group_candidate_rows(row_drafts)
    if not groups:
        return []

    regions: List[TableRegion] = []
    for group in groups:
        region = _build_table_region(group, words)
        if region is None:
            continue
        if region.confidence < TABLE_MIN_CONFIDENCE:
            continue
        regions.append(region)

    return sorted(regions, key=lambda item: (item.page_number, item.bbox.y0, item.bbox.x0))


def _build_row_drafts(indexed_words: Sequence[Tuple[int, OCRWord]]) -> List[_RowDraft]:
    sorted_words = sorted(
        indexed_words,
        key=lambda item: ((item[1].bbox.y0 + item[1].bbox.y1) / 2.0, item[1].bbox.x0),
    )
    if not sorted_words:
        return []

    heights = [max(0.1, word.bbox.y1 - word.bbox.y0) for _index, word in sorted_words]
    base_height = median(heights)
    y_tolerance = max(TABLE_ROW_Y_TOLERANCE_PT, base_height * 0.55)

    clustered_rows: List[List[Tuple[int, OCRWord]]] = []
    row_centers: List[float] = []

    for index, word in sorted_words:
        center = (word.bbox.y0 + word.bbox.y1) / 2.0
        if not clustered_rows:
            clustered_rows.append([(index, word)])
            row_centers.append(center)
            continue

        last_center = row_centers[-1]
        if abs(center - last_center) <= y_tolerance:
            row = clustered_rows[-1]
            current_size = len(row)
            row.append((index, word))
            row_centers[-1] = ((last_center * current_size) + center) / float(current_size + 1)
        else:
            clustered_rows.append([(index, word)])
            row_centers.append(center)

    row_drafts: List[_RowDraft] = []
    for row_words in clustered_rows:
        row_words = sorted(row_words, key=lambda item: item[1].bbox.x0)
        cells = _split_row_into_cells(row_words)
        if not cells:
            continue

        y0 = min(cell.y0 for cell in cells)
        y1 = max(cell.y1 for cell in cells)
        row_drafts.append(_RowDraft(cells=cells, y0=y0, y1=y1))

    return row_drafts


def _split_row_into_cells(row_words: Sequence[Tuple[int, OCRWord]]) -> List[_CellDraft]:
    if not row_words:
        return []

    heights = [max(0.1, word.bbox.y1 - word.bbox.y0) for _index, word in row_words]
    min_cell_gap = max(TABLE_COLUMN_GAP_MIN_PT, median(heights) * 0.9)

    groups: List[List[Tuple[int, OCRWord]]] = []
    current_group: List[Tuple[int, OCRWord]] = []
    previous_word: Optional[OCRWord] = None

    for index, word in row_words:
        if previous_word is None:
            current_group = [(index, word)]
            previous_word = word
            continue

        horizontal_gap = word.bbox.x0 - previous_word.bbox.x1
        if horizontal_gap >= min_cell_gap and current_group:
            groups.append(current_group)
            current_group = [(index, word)]
        else:
            current_group.append((index, word))

        previous_word = word

    if current_group:
        groups.append(current_group)

    cells: List[_CellDraft] = []
    for group in groups:
        sorted_group = sorted(group, key=lambda item: item[1].bbox.x0)
        word_indexes = [index for index, _word in sorted_group]
        text = " ".join(word.text for _index, word in sorted_group).strip()
        if not text:
            continue

        x0 = min(word.bbox.x0 for _index, word in sorted_group)
        y0 = min(word.bbox.y0 for _index, word in sorted_group)
        x1 = max(word.bbox.x1 for _index, word in sorted_group)
        y1 = max(word.bbox.y1 for _index, word in sorted_group)
        cells.append(
            _CellDraft(
                word_indexes=word_indexes,
                text=text,
                x0=x0,
                y0=y0,
                x1=x1,
                y1=y1,
            )
        )

    return cells


def _group_candidate_rows(rows: Sequence[_RowDraft]) -> List[List[_RowDraft]]:
    if not rows:
        return []

    sorted_rows = sorted(rows, key=lambda row: row.y0)
    median_height = median(row.height for row in sorted_rows)
    max_row_gap = max(
        TABLE_ROW_Y_TOLERANCE_PT * 2.5,
        median_height * TABLE_CONTINUATION_MAX_Y_GAP_MULT,
    )

    groups: List[List[_RowDraft]] = []
    current: List[_RowDraft] = []

    for row in sorted_rows:
        has_structured_shape = len(row.cells) >= TABLE_MIN_COLS
        could_be_continuation = len(row.cells) == 1

        if not current:
            if has_structured_shape:
                current = [row]
            continue

        previous = current[-1]
        gap = max(0.0, row.y0 - previous.y1)
        if gap <= max_row_gap and (has_structured_shape or could_be_continuation):
            current.append(row)
            continue

        if _count_structured_rows(current) >= TABLE_MIN_ROWS:
            groups.append(current)

        current = [row] if has_structured_shape else []

    if current and _count_structured_rows(current) >= TABLE_MIN_ROWS:
        groups.append(current)

    return groups


def _count_structured_rows(rows: Sequence[_RowDraft]) -> int:
    return sum(1 for row in rows if len(row.cells) >= TABLE_MIN_COLS)


def _build_table_region(rows: Sequence[_RowDraft], words: Sequence[OCRWord]) -> Optional[TableRegion]:
    structured_rows = [row for row in rows if len(row.cells) >= TABLE_MIN_COLS]
    if len(structured_rows) < TABLE_MIN_ROWS:
        return None

    anchors = _cluster_column_anchors(structured_rows)
    if len(anchors) < TABLE_MIN_COLS:
        return None

    mapped_rows: List[Dict[int, _CellDraft]] = []
    alignment_errors: List[float] = []

    for row in rows:
        mapped: Dict[int, _CellDraft] = {}
        for cell in row.cells:
            col_index, delta = _closest_anchor(cell.x0, anchors)
            if col_index is None:
                continue
            if delta > TABLE_MAX_COLUMN_DRIFT_PT * 1.75:
                continue

            existing = mapped.get(col_index)
            if existing is None:
                mapped[col_index] = cell
            else:
                mapped[col_index] = _merge_cell_drafts(existing, cell)

            alignment_errors.append(delta)

        if mapped:
            mapped_rows.append(mapped)

    if not mapped_rows:
        return None

    merged_rows = _merge_continuation_rows(mapped_rows)
    structured_row_count = sum(1 for row in merged_rows if len(row) >= TABLE_MIN_COLS)
    if structured_row_count < TABLE_MIN_ROWS:
        return None

    density_values = [len(row) / float(len(anchors)) for row in merged_rows]
    density_score = sum(density_values) / len(density_values) if density_values else 0.0
    row_score = min(1.0, structured_row_count / float(TABLE_MIN_ROWS))
    col_score = min(1.0, len(anchors) / float(TABLE_MIN_COLS))

    mean_error = sum(alignment_errors) / len(alignment_errors) if alignment_errors else TABLE_MAX_COLUMN_DRIFT_PT
    alignment_score = max(0.0, 1.0 - (mean_error / max(TABLE_MAX_COLUMN_DRIFT_PT, 1.0)))

    confidence = (
        (0.40 * alignment_score)
        + (0.25 * density_score)
        + (0.20 * row_score)
        + (0.15 * col_score)
    )

    table_cells: List[TableCell] = []
    for row_index, row in enumerate(merged_rows):
        for col_index in sorted(row.keys()):
            cell = row[col_index]
            if not cell.word_indexes:
                continue
            unique_indexes = tuple(sorted(set(cell.word_indexes)))
            cell_bbox = _build_bbox_for_indexes(unique_indexes, words)
            table_cells.append(
                TableCell(
                    row_index=row_index,
                    col_index=col_index,
                    text=cell.text,
                    bbox=cell_bbox,
                    word_indexes=unique_indexes,
                )
            )

    if not table_cells:
        return None

    region_bbox = _merge_bboxes([cell.bbox for cell in table_cells])
    page_number = table_cells[0].bbox.page_number
    return TableRegion(
        page_number=page_number,
        bbox=region_bbox,
        row_count=max(cell.row_index for cell in table_cells) + 1,
        col_count=len(anchors),
        confidence=round(max(0.0, min(1.0, confidence)), 4),
        cells=tuple(table_cells),
    )


def _cluster_column_anchors(rows: Sequence[_RowDraft]) -> List[float]:
    positions: List[float] = []
    for row in rows:
        for cell in row.cells:
            positions.append(cell.x0)

    if not positions:
        return []

    positions.sort()
    clusters: List[List[float]] = []
    for value in positions:
        if not clusters:
            clusters.append([value])
            continue

        cluster = clusters[-1]
        anchor = sum(cluster) / len(cluster)
        if abs(value - anchor) <= TABLE_MAX_COLUMN_DRIFT_PT:
            cluster.append(value)
        else:
            clusters.append([value])

    anchors = [sum(cluster) / len(cluster) for cluster in clusters]
    return sorted(anchors)


def _closest_anchor(x_position: float, anchors: Sequence[float]) -> Tuple[Optional[int], float]:
    best_index: Optional[int] = None
    best_delta = float("inf")

    for index, anchor in enumerate(anchors):
        delta = abs(x_position - anchor)
        if delta < best_delta:
            best_index = index
            best_delta = delta

    return best_index, best_delta


def _merge_continuation_rows(rows: Sequence[Dict[int, _CellDraft]]) -> List[Dict[int, _CellDraft]]:
    merged_rows: List[Dict[int, _CellDraft]] = []

    for row in rows:
        if not merged_rows:
            merged_rows.append(dict(row))
            continue

        columns = sorted(row.keys())
        if len(columns) == 1:
            continuation_col = columns[0]
            previous_row = merged_rows[-1]
            previous_cell = previous_row.get(continuation_col)
            current_cell = row[continuation_col]
            if previous_cell is not None and continuation_col > 0:
                previous_row[continuation_col] = _merge_cell_drafts(previous_cell, current_cell)
                continue

        merged_rows.append(dict(row))

    return merged_rows


def _merge_cell_drafts(left: _CellDraft, right: _CellDraft) -> _CellDraft:
    merged_text = f"{left.text} {right.text}".strip()
    return _CellDraft(
        word_indexes=list(left.word_indexes) + list(right.word_indexes),
        text=merged_text,
        x0=min(left.x0, right.x0),
        y0=min(left.y0, right.y0),
        x1=max(left.x1, right.x1),
        y1=max(left.y1, right.y1),
    )


def _build_bbox_for_indexes(indexes: Sequence[int], words: Sequence[OCRWord]) -> BoundingBox:
    boxes = [words[index].bbox for index in indexes]
    return _merge_bboxes(boxes)


def _merge_bboxes(boxes: Sequence[BoundingBox]) -> BoundingBox:
    first = boxes[0]
    return BoundingBox(
        page_number=first.page_number,
        x0=min(box.x0 for box in boxes),
        y0=min(box.y0 for box in boxes),
        x1=max(box.x1 for box in boxes),
        y1=max(box.y1 for box in boxes),
    )
