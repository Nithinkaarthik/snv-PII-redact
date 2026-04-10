from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Sequence, Tuple


@dataclass(frozen=True)
class BoundingBox:
    page_number: int  # zero-indexed internally
    x0: float
    y0: float
    x1: float
    y1: float


@dataclass(frozen=True)
class OCRWord:
    text: str
    bbox: BoundingBox
    line_key: str


@dataclass(frozen=True)
class WordSpan:
    text: str
    start_char: int
    end_char: int
    bbox: BoundingBox
    line_key: str


@dataclass(frozen=True)
class TableCell:
    row_index: int
    col_index: int
    text: str
    bbox: BoundingBox
    word_indexes: Tuple[int, ...]


@dataclass(frozen=True)
class TableRegion:
    page_number: int
    bbox: BoundingBox
    row_count: int
    col_count: int
    confidence: float
    cells: Tuple[TableCell, ...]


@dataclass(frozen=True)
class LineMetadata:
    page_number: int
    line_key: str
    y0: float
    y1: float
    bbox: BoundingBox


@dataclass
class LineHeightCache:
    lines_by_page: Dict[int, Tuple[LineMetadata, ...]] = field(default_factory=dict)

    @classmethod
    def from_words(cls, words: Sequence[OCRWord]) -> "LineHeightCache":
        grouped: Dict[Tuple[int, str], List[BoundingBox]] = {}
        for word in words:
            key = (word.bbox.page_number, word.line_key)
            grouped.setdefault(key, []).append(word.bbox)

        lines_by_page: Dict[int, List[LineMetadata]] = {}
        for (page_number, line_key), boxes in grouped.items():
            x0 = min(box.x0 for box in boxes)
            y0 = min(box.y0 for box in boxes)
            x1 = max(box.x1 for box in boxes)
            y1 = max(box.y1 for box in boxes)
            metadata = LineMetadata(
                page_number=page_number,
                line_key=line_key,
                y0=y0,
                y1=y1,
                bbox=BoundingBox(
                    page_number=page_number,
                    x0=x0,
                    y0=y0,
                    x1=x1,
                    y1=y1,
                ),
            )
            lines_by_page.setdefault(page_number, []).append(metadata)

        frozen_mapping: Dict[int, Tuple[LineMetadata, ...]] = {}
        for page_number, items in lines_by_page.items():
            frozen_mapping[page_number] = tuple(sorted(items, key=lambda line: (line.y0, line.y1)))

        return cls(lines_by_page=frozen_mapping)

    def compute_safe_vertical_inset(
        self,
        box: BoundingBox,
        *,
        safety_margin_pt: float,
    ) -> float:
        page_lines = self.lines_by_page.get(box.page_number)
        if not page_lines:
            return float("inf")

        gap_above = float("inf")
        gap_below = float("inf")

        for line in page_lines:
            if line.y1 <= box.y0:
                gap_above = min(gap_above, box.y0 - line.y1)
                continue

            if line.y0 >= box.y1:
                gap_below = min(gap_below, line.y0 - box.y1)

        finite_gaps = [gap for gap in (gap_above, gap_below) if gap != float("inf")]
        if not finite_gaps:
            return float("inf")

        nearest_gap = min(finite_gaps)
        return max(0.0, (nearest_gap / 2.0) - max(0.0, safety_margin_pt))
