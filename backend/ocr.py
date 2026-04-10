from __future__ import annotations

import io
from typing import Dict, List, Sequence, Tuple

import fitz
import pytesseract
from PIL import Image

try:
    from backend.config import LOGGER, NATIVE_TEXT_MIN_ALNUM
    from backend.models import BoundingBox, OCRWord
    from backend.text_mapping import build_coordinate_maps
except ImportError:
    from config import LOGGER, NATIVE_TEXT_MIN_ALNUM
    from models import BoundingBox, OCRWord
    from text_mapping import build_coordinate_maps


def extract_words_with_coordinates(
    pdf_bytes: bytes,
) -> Tuple[List[OCRWord], Dict[str, List[BoundingBox]], Dict[str, List[BoundingBox]]]:
    document = fitz.open(stream=pdf_bytes, filetype="pdf")
    words: List[OCRWord] = []

    try:
        for page_number in range(document.page_count):
            page = document[page_number]
            native_words = _extract_page_words_pymupdf(page, page_number)

            if _is_page_text_meaningful(native_words):
                page_words = native_words
            else:
                ocr_words = _extract_page_words_tesseract(page, page_number)
                if ocr_words:
                    LOGGER.info("Fell back to OCR on page %s due to weak native text layer.", page_number + 1)
                page_words = ocr_words if ocr_words else native_words

            words.extend(page_words)
    finally:
        document.close()

    word_coordinate_map, phrase_coordinate_map = build_coordinate_maps(words)
    return words, word_coordinate_map, phrase_coordinate_map


def _is_page_text_meaningful(page_words: Sequence[OCRWord]) -> bool:
    if not page_words:
        return False
    flattened = "".join(word.text for word in page_words)
    alnum_count = sum(1 for char in flattened if char.isalnum())
    return alnum_count >= NATIVE_TEXT_MIN_ALNUM


def _extract_page_words_pymupdf(page: fitz.Page, page_number: int) -> List[OCRWord]:
    raw_words = page.get_text("words")
    if not raw_words:
        return []

    sorted_words = sorted(raw_words, key=lambda item: (item[5], item[6], item[7]))
    extracted: List[OCRWord] = []

    for x0, y0, x1, y1, text, block_no, line_no, _word_no in sorted_words:
        clean_text = str(text).strip()
        if not clean_text:
            continue
        extracted.append(
            OCRWord(
                text=clean_text,
                bbox=BoundingBox(
                    page_number=page_number,
                    x0=float(x0),
                    y0=float(y0),
                    x1=float(x1),
                    y1=float(y1),
                ),
                line_key=f"pymupdf:{block_no}:{line_no}",
            )
        )
    return extracted


def _extract_page_words_tesseract(page: fitz.Page, page_number: int) -> List[OCRWord]:
    matrix = fitz.Matrix(2.0, 2.0)
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    image = Image.open(io.BytesIO(pixmap.tobytes("png")))

    try:
        ocr_data = pytesseract.image_to_data(
            image,
            output_type=pytesseract.Output.DICT,
            config="--oem 3 --psm 6",
        )
    except pytesseract.TesseractNotFoundError:
        LOGGER.warning("Tesseract executable was not found on PATH.")
        return []

    extracted: List[OCRWord] = []
    texts = ocr_data.get("text", [])
    confs = ocr_data.get("conf", [])
    lefts = ocr_data.get("left", [])
    tops = ocr_data.get("top", [])
    widths = ocr_data.get("width", [])
    heights = ocr_data.get("height", [])
    blocks = ocr_data.get("block_num", [])
    paragraphs = ocr_data.get("par_num", [])
    lines = ocr_data.get("line_num", [])

    zoom_x = matrix.a
    zoom_y = matrix.d

    for index, raw_text in enumerate(texts):
        clean_text = str(raw_text).strip()
        if not clean_text:
            continue

        confidence = _safe_float(confs[index] if index < len(confs) else "-1", default=-1.0)
        if confidence < 0:
            continue

        left = _safe_float(lefts[index] if index < len(lefts) else 0.0) / zoom_x
        top = _safe_float(tops[index] if index < len(tops) else 0.0) / zoom_y
        width = _safe_float(widths[index] if index < len(widths) else 0.0) / zoom_x
        height = _safe_float(heights[index] if index < len(heights) else 0.0) / zoom_y
        right = left + max(width, 0.0)
        bottom = top + max(height, 0.0)

        block_no = blocks[index] if index < len(blocks) else 0
        paragraph_no = paragraphs[index] if index < len(paragraphs) else 0
        line_no = lines[index] if index < len(lines) else 0

        extracted.append(
            OCRWord(
                text=clean_text,
                bbox=BoundingBox(
                    page_number=page_number,
                    x0=left,
                    y0=top,
                    x1=right,
                    y1=bottom,
                ),
                line_key=f"tesseract:{block_no}:{paragraph_no}:{line_no}",
            )
        )

    return extracted


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
