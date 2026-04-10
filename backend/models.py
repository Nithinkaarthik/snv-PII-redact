from __future__ import annotations

from dataclasses import dataclass


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
