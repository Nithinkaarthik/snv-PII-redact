from __future__ import annotations

import io
import os
from typing import Dict, List, Sequence, Tuple

import fitz
import pytesseract
from PIL import Image

try:
    from backend.config import LOGGER, NATIVE_TEXT_MIN_ALNUM, TABLE_PARSER_ENABLED
    from backend.models import BoundingBox, LineHeightCache, OCRWord, TableRegion
    from backend.table_detection import detect_table_regions
    from backend.text_mapping import build_coordinate_maps
except ImportError:
    from config import LOGGER, NATIVE_TEXT_MIN_ALNUM, TABLE_PARSER_ENABLED
    from models import BoundingBox, LineHeightCache, OCRWord, TableRegion
    from table_detection import detect_table_regions
    from text_mapping import build_coordinate_maps


_DEBUG_FALSE_VALUES = {"0", "false", "no", "off"}
_DEBUG_ENABLED = os.getenv("BACKEND_DEBUG_BLOCKS", "0").strip().lower() not in _DEBUG_FALSE_VALUES


def _debug(message: str, *args: object) -> None:
    if not _DEBUG_ENABLED:
        return
    LOGGER.info("[DEBUG] " + message, *args)


def extract_words_with_coordinates(
    pdf_bytes: bytes,
) -> Tuple[List[OCRWord], Dict[str, List[BoundingBox]], Dict[str, List[BoundingBox]]]:
    document = fitz.open(stream=pdf_bytes, filetype="pdf")
    words: List[OCRWord] = []
    _debug("OCR_PIPELINE_START pages=%s", document.page_count)

    try:
        for page_number in range(document.page_count):
            page = document[page_number]
            page_words = extract_page_words(page, page_number)
            words.extend(page_words)
            _debug("OCR_PAGE_DONE page=%s words=%s", page_number + 1, len(page_words))
    finally:
        document.close()

    word_coordinate_map, phrase_coordinate_map = build_coordinate_maps(words)
    _debug(
        "OCR_PIPELINE_FINISH total_words=%s unique_words=%s phrase_lines=%s",
        len(words),
        len(word_coordinate_map),
        len(phrase_coordinate_map),
    )
    return words, word_coordinate_map, phrase_coordinate_map


def extract_page_words(page: fitz.Page, page_number: int) -> List[OCRWord]:
    native_words = _extract_page_words_pymupdf(page, page_number)
    is_meaningful = _is_page_text_meaningful(native_words)
    _debug(
        "PAGE_TEXT_CHECK page=%s native_words=%s meaningful=%s min_alnum=%s",
        page_number + 1,
        len(native_words),
        is_meaningful,
        NATIVE_TEXT_MIN_ALNUM,
    )

    if is_meaningful:
        _debug("PAGE_SOURCE_SELECTED page=%s source=native", page_number + 1)
        return native_words

    _debug("PAGE_SOURCE_SELECTED page=%s source=ocr_fallback", page_number + 1)
    ocr_words = _extract_page_words_tesseract(page, page_number)
    if ocr_words:
        LOGGER.info("Fell back to OCR on page %s due to weak native text layer.", page_number + 1)
        _debug("OCR_FALLBACK_RESULT page=%s ocr_words=%s", page_number + 1, len(ocr_words))
        return ocr_words

    _debug("OCR_FALLBACK_EMPTY page=%s returning_native_words=%s", page_number + 1, len(native_words))
    return native_words


def extract_page_words_with_tables(
    page: fitz.Page,
    page_number: int,
) -> Tuple[List[OCRWord], List[TableRegion], LineHeightCache]:
    page_words = extract_page_words(page, page_number)
    line_height_cache = LineHeightCache.from_words(page_words)
    if not TABLE_PARSER_ENABLED or not page_words:
        return page_words, [], line_height_cache

    table_regions = detect_table_regions(page_words)
    _debug(
        "PAGE_TABLE_PARSE page=%s tables=%s cells=%s lines=%s",
        page_number + 1,
        len(table_regions),
        sum(len(region.cells) for region in table_regions),
        len(line_height_cache.lines_by_page.get(page_number, ())),
    )
    return page_words, table_regions, line_height_cache


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
    ocr_render_scale = _safe_float(os.getenv("OCR_RENDER_SCALE", "1.5"), default=1.5)
    if ocr_render_scale <= 0:
        ocr_render_scale = 1.5

    matrix = fitz.Matrix(ocr_render_scale, ocr_render_scale)
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    image = Image.open(io.BytesIO(pixmap.tobytes("png")))
    tesseract_config = os.getenv("TESSERACT_CONFIG", "--oem 3 --psm 6").strip() or "--oem 3 --psm 6"
    _debug(
        "OCR_RENDER page=%s scale=%.2f config=%s",
        page_number + 1,
        ocr_render_scale,
        tesseract_config,
    )

    try:
        ocr_data = pytesseract.image_to_data(
            image,
            output_type=pytesseract.Output.DICT,
            config=tesseract_config,
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
    _debug("OCR_RAW_OUTPUT page=%s candidates=%s", page_number + 1, len(texts))

    zoom_x = matrix.a
    zoom_y = matrix.d
    skipped_empty = 0
    skipped_conf = 0

    for index, raw_text in enumerate(texts):
        clean_text = str(raw_text).strip()
        if not clean_text:
            skipped_empty += 1
            continue

        confidence = _safe_float(confs[index] if index < len(confs) else "-1", default=-1.0)
        if confidence < 0:
            skipped_conf += 1
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

    _debug(
        "OCR_PARSE_RESULT page=%s accepted=%s skipped_empty=%s skipped_conf=%s",
        page_number + 1,
        len(extracted),
        skipped_empty,
        skipped_conf,
    )
    return extracted


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
