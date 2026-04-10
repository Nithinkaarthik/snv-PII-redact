from __future__ import annotations

import io
import os
import re
from statistics import mean
from typing import Any, Dict, List, Sequence, Tuple

import fitz
import pytesseract
from PIL import Image, ImageEnhance, ImageFilter, ImageOps

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
        native_score = _word_quality_score(native_words)
        ocr_score = _word_quality_score(ocr_words)
        _debug(
            "OCR_SELECTION_SCORE page=%s native_score=%.2f ocr_score=%.2f",
            page_number + 1,
            native_score,
            ocr_score,
        )

        if native_words and native_score > ocr_score * 1.12:
            _debug("OCR_SELECTION_RESULT page=%s selected=native_low_conf_ocr", page_number + 1)
            return native_words

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
    base_scale = _safe_float(os.getenv("OCR_RENDER_SCALE", "1.5"), default=1.5)
    if base_scale <= 0:
        base_scale = 1.5

    adaptive_scale = _resolve_adaptive_ocr_scale(page, base_scale)
    matrix = fitz.Matrix(adaptive_scale, adaptive_scale)
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    base_image = Image.open(io.BytesIO(pixmap.tobytes("png")))

    base_config = os.getenv("TESSERACT_CONFIG", "--oem 3 --psm 6").strip() or "--oem 3 --psm 6"
    min_confidence = _safe_float(os.getenv("OCR_MIN_CONFIDENCE", "20"), default=20.0)
    strong_word_threshold = int(_safe_float(os.getenv("OCR_STRONG_WORD_THRESHOLD", "16"), default=16.0))
    strong_avg_conf = _safe_float(os.getenv("OCR_STRONG_AVG_CONF", "46"), default=46.0)

    passes = [
        {
            "name": "base",
            "image": base_image,
            "config": base_config,
            "min_conf": min_confidence,
        },
        {
            "name": "enhanced",
            "image": _prepare_ocr_variant(base_image, "enhanced"),
            "config": base_config,
            "min_conf": max(8.0, min_confidence - 6.0),
        },
        {
            "name": "threshold_sparse",
            "image": _prepare_ocr_variant(base_image, "threshold"),
            "config": _replace_or_append_psm(base_config, "11"),
            "min_conf": max(6.0, min_confidence - 8.0),
        },
    ]

    _debug(
        "OCR_RENDER page=%s scale=%.2f base_config=%s min_conf=%.1f",
        page_number + 1,
        adaptive_scale,
        base_config,
        min_confidence,
    )

    best_words: List[OCRWord] = []
    best_metrics: Dict[str, float] = {"quality_score": -1.0, "accepted": 0.0, "avg_conf": 0.0, "alnum": 0.0}

    for current_pass in passes:
        pass_name = str(current_pass["name"])
        pass_image = current_pass["image"]
        pass_config = str(current_pass["config"])
        pass_min_conf = float(current_pass["min_conf"])

        try:
            ocr_data = pytesseract.image_to_data(
                pass_image,
                output_type=pytesseract.Output.DICT,
                config=pass_config,
            )
        except pytesseract.TesseractNotFoundError:
            LOGGER.warning("Tesseract executable was not found on PATH.")
            return []

        words, metrics = _extract_words_from_ocr_data(
            ocr_data=ocr_data,
            page_number=page_number,
            zoom_x=matrix.a,
            zoom_y=matrix.d,
            min_confidence=pass_min_conf,
            line_key_prefix=f"tesseract:{pass_name}",
        )

        _debug(
            "OCR_PASS_RESULT page=%s pass=%s accepted=%s avg_conf=%.2f alnum=%s score=%.2f",
            page_number + 1,
            pass_name,
            int(metrics.get("accepted", 0.0)),
            metrics.get("avg_conf", 0.0),
            int(metrics.get("alnum", 0.0)),
            metrics.get("quality_score", 0.0),
        )

        if metrics.get("quality_score", 0.0) > best_metrics.get("quality_score", -1.0):
            best_words = words
            best_metrics = metrics

        if _is_ocr_pass_strong(metrics, strong_word_threshold, strong_avg_conf):
            _debug("OCR_EARLY_EXIT page=%s pass=%s", page_number + 1, pass_name)
            return words

    _debug(
        "OCR_FINAL_SELECTION page=%s accepted=%s avg_conf=%.2f score=%.2f",
        page_number + 1,
        int(best_metrics.get("accepted", 0.0)),
        best_metrics.get("avg_conf", 0.0),
        best_metrics.get("quality_score", 0.0),
    )
    return best_words


def _extract_words_from_ocr_data(
    ocr_data: Dict[str, Any],
    page_number: int,
    zoom_x: float,
    zoom_y: float,
    min_confidence: float,
    line_key_prefix: str,
) -> Tuple[List[OCRWord], Dict[str, float]]:
    extracted: List[OCRWord] = []
    confidences: List[float] = []
    alnum_count = 0

    texts = ocr_data.get("text", [])
    confs = ocr_data.get("conf", [])
    lefts = ocr_data.get("left", [])
    tops = ocr_data.get("top", [])
    widths = ocr_data.get("width", [])
    heights = ocr_data.get("height", [])
    blocks = ocr_data.get("block_num", [])
    paragraphs = ocr_data.get("par_num", [])
    lines = ocr_data.get("line_num", [])

    full_matrix = page.rotation_matrix * matrix
    inverse_matrix = ~full_matrix

    skipped_empty = 0
    skipped_conf = 0

    for index, raw_text in enumerate(texts):
        clean_text = str(raw_text).strip()
        if not clean_text:
            skipped_empty += 1
            continue

        confidence = _safe_float(confs[index] if index < len(confs) else "-1", default=-1.0)
        if confidence < min_confidence:
            skipped_conf += 1
            continue

        raw_left = _safe_float(lefts[index] if index < len(lefts) else 0.0)
        raw_top = _safe_float(tops[index] if index < len(tops) else 0.0)
        raw_width = _safe_float(widths[index] if index < len(widths) else 0.0)
        raw_height = _safe_float(heights[index] if index < len(heights) else 0.0)
        
        rect = fitz.Rect(raw_left, raw_top, raw_left + raw_width, raw_top + raw_height)
        rect *= inverse_matrix

        left, top, right, bottom = rect.x0, rect.y0, rect.x1, rect.y1

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
                line_key=f"{line_key_prefix}:{block_no}:{paragraph_no}:{line_no}",
            )
        )

        confidences.append(confidence)
        alnum_count += sum(1 for char in clean_text if char.isalnum())

    avg_conf = float(mean(confidences)) if confidences else 0.0
    accepted = len(extracted)
    quality_score = (accepted * 2.0) + (avg_conf * 0.45) + (alnum_count * 0.03)

    metrics = {
        "accepted": float(accepted),
        "avg_conf": avg_conf,
        "alnum": float(alnum_count),
        "quality_score": quality_score,
        "skipped_empty": float(skipped_empty),
        "skipped_conf": float(skipped_conf),
    }
    return extracted, metrics


def _prepare_ocr_variant(image: Image.Image, variant: str) -> Image.Image:
    gray = ImageOps.grayscale(image)

    if variant == "enhanced":
        enhanced = ImageOps.autocontrast(gray, cutoff=1)
        enhanced = ImageEnhance.Contrast(enhanced).enhance(1.7)
        enhanced = ImageEnhance.Sharpness(enhanced).enhance(1.25)
        return enhanced.filter(ImageFilter.MedianFilter(size=3))

    if variant == "threshold":
        enhanced = ImageOps.autocontrast(gray, cutoff=2)
        threshold_value = int(_safe_float(os.getenv("OCR_THRESHOLD_VALUE", "164"), default=164.0))
        threshold_value = max(90, min(threshold_value, 220))
        binary = enhanced.point(lambda value: 255 if value > threshold_value else 0, mode="1")
        return binary.convert("L")

    return gray


def _replace_or_append_psm(config: str, psm_value: str) -> str:
    candidate = str(config or "").strip()
    if not candidate:
        return f"--oem 3 --psm {psm_value}"

    if re.search(r"--psm\s+\d+", candidate):
        return re.sub(r"--psm\s+\d+", f"--psm {psm_value}", candidate)

    return f"{candidate} --psm {psm_value}".strip()


def _resolve_adaptive_ocr_scale(page: fitz.Page, base_scale: float) -> float:
    width = float(page.rect.width or 0.0)
    height = float(page.rect.height or 0.0)
    area = width * height

    scale = float(base_scale)
    if area > 900000:
        scale *= 0.9
    elif area < 300000:
        scale *= 1.12

    return max(1.2, min(scale, 2.4))


def _is_ocr_pass_strong(metrics: Dict[str, float], strong_word_threshold: int, strong_avg_conf: float) -> bool:
    accepted = metrics.get("accepted", 0.0)
    avg_conf = metrics.get("avg_conf", 0.0)
    alnum = metrics.get("alnum", 0.0)

    if accepted >= float(strong_word_threshold) and avg_conf >= float(strong_avg_conf):
        return True

    if accepted >= 8 and avg_conf >= 34.0 and alnum >= float(NATIVE_TEXT_MIN_ALNUM * 2):
        return True

    return False


def _word_quality_score(words: Sequence[OCRWord]) -> float:
    if not words:
        return 0.0

    joined = " ".join(word.text for word in words)
    alnum = sum(1 for char in joined if char.isalnum())
    long_words = sum(1 for word in words if len(word.text) >= 3)
    unique_words = len({word.text.strip().lower() for word in words if word.text.strip()})

    return (alnum * 0.9) + (long_words * 2.0) + (unique_words * 0.6)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
