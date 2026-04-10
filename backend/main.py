from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Sequence, Tuple

import fitz
import pytesseract
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from litellm import completion
from PIL import Image
from pydantic import BaseModel, Field
from presidio_analyzer import AnalyzerEngine

LOGGER = logging.getLogger("sanitize_pipeline")
logging.basicConfig(level=logging.INFO)

TARGET_PII_ENTITIES: Optional[List[str]] = None
MAX_FILE_SIZE_BYTES = int(os.getenv("MAX_UPLOAD_MB", "30")) * 1024 * 1024
LLM_TEXT_CHAR_LIMIT = int(os.getenv("LLM_TEXT_CHAR_LIMIT", "20000"))
DEFAULT_LLM_MODEL = os.getenv("LLM_MODEL", "openrouter/openai/gpt-4o-mini")
DEFAULT_OPENROUTER_API_BASE = os.getenv("OPENROUTER_API_BASE", "https://openrouter.ai/api/v1")

US_STATE_NAMES = {
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
    "district of columbia",
}

US_STATE_ABBREVIATIONS = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}

_ANALYZER: Optional[AnalyzerEngine] = None


class BBoxModel(BaseModel):
    page_number: int = Field(..., ge=1)
    x0: float
    y0: float
    x1: float
    y1: float


class DetectedEntity(BaseModel):
    entity_text: str
    entity_type: str
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    source: Literal["Presidio", "LLM"]
    boxes: List[BBoxModel] = Field(default_factory=list)


class SanitizeResponse(BaseModel):
    detected_entities: List[DetectedEntity]
    redacted_pdf_base64: str
    warnings: List[str] = Field(default_factory=list)


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
class IndexedWord:
    word: OCRWord
    start_char: int
    end_char: int


@dataclass
class Detection:
    entity_text: str
    entity_type: str
    confidence_score: float
    source: Literal["Presidio", "LLM"]
    boxes: List[BoundingBox]


app = FastAPI(title="Document Sanitization MVP1", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)


@app.get("/health")
def health_check() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/sanitize", response_model=SanitizeResponse)
async def sanitize_document(file: UploadFile = File(...)) -> SanitizeResponse:
    payload = await file.read()
    _validate_pdf_upload(file, payload)

    words, word_coordinate_map, phrase_coordinate_map = extract_words_with_coordinates(payload)
    if not words:
        raise HTTPException(status_code=422, detail="No text could be extracted from the PDF.")

    LOGGER.info(
        "OCR extraction complete: %s words, %s unique words, %s phrase lines",
        len(words),
        len(word_coordinate_map),
        len(phrase_coordinate_map),
    )

    canonical_text, indexed_words = build_canonical_text_index(words)
    if not canonical_text.strip():
        raise HTTPException(status_code=422, detail="Extracted text is empty after OCR processing.")

    try:
        presidio_detections = run_presidio_triage(canonical_text, indexed_words)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    llm_detections, llm_warnings = run_llm_context_triage(canonical_text, indexed_words)
    all_detections = presidio_detections + llm_detections

    redacted_pdf_bytes = apply_secure_redactions(payload, all_detections)
    redacted_pdf_b64 = base64.b64encode(redacted_pdf_bytes).decode("ascii")

    return SanitizeResponse(
        detected_entities=serialize_detections(all_detections),
        redacted_pdf_base64=redacted_pdf_b64,
        warnings=llm_warnings,
    )


def _validate_pdf_upload(file: UploadFile, payload: bytes) -> None:
    filename = file.filename or "uploaded.pdf"
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only .pdf uploads are supported.")
    if len(payload) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    if len(payload) > MAX_FILE_SIZE_BYTES:
        max_mb = MAX_FILE_SIZE_BYTES // (1024 * 1024)
        raise HTTPException(status_code=413, detail=f"PDF exceeds {max_mb} MB upload limit.")
    if not payload.startswith(b"%PDF"):
        raise HTTPException(status_code=400, detail="Uploaded file is not a valid PDF stream.")


def extract_words_with_coordinates(
    pdf_bytes: bytes,
) -> Tuple[List[OCRWord], Dict[str, List[BoundingBox]], Dict[str, List[BoundingBox]]]:
    document = fitz.open(stream=pdf_bytes, filetype="pdf")
    words: List[OCRWord] = []

    try:
        for page_number in range(document.page_count):
            page = document[page_number]
            page_words = _extract_page_words_pymupdf(page, page_number)
            if not page_words:
                page_words = _extract_page_words_tesseract(page, page_number)
            words.extend(page_words)
    finally:
        document.close()

    word_coordinate_map, phrase_coordinate_map = build_coordinate_maps(words)
    return words, word_coordinate_map, phrase_coordinate_map


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


def build_canonical_text_index(words: Sequence[OCRWord]) -> Tuple[str, List[IndexedWord]]:
    if not words:
        return "", []

    text_parts: List[str] = []
    indexed_words: List[IndexedWord] = []
    cursor = 0
    previous_page = words[0].bbox.page_number

    for index, word in enumerate(words):
        if index > 0:
            separator = "\n" if word.bbox.page_number != previous_page else " "
            text_parts.append(separator)
            cursor += len(separator)

        start_char = cursor
        text_parts.append(word.text)
        cursor += len(word.text)
        end_char = cursor

        indexed_words.append(
            IndexedWord(
                word=word,
                start_char=start_char,
                end_char=end_char,
            )
        )
        previous_page = word.bbox.page_number

    return "".join(text_parts), indexed_words


def span_to_bboxes(start_char: int, end_char: int, indexed_words: Sequence[IndexedWord]) -> List[BoundingBox]:
    if end_char <= start_char:
        return []

    overlapping_words = [
        token.word
        for token in indexed_words
        if token.start_char < end_char and token.end_char > start_char
    ]
    if not overlapping_words:
        return []

    grouped: Dict[Tuple[int, str], List[BoundingBox]] = {}
    for word in overlapping_words:
        grouped.setdefault((word.bbox.page_number, word.line_key), []).append(word.bbox)

    merged_boxes = [_merge_boxes(boxes) for boxes in grouped.values()]
    return sorted(merged_boxes, key=lambda box: (box.page_number, box.y0, box.x0))


def run_presidio_triage(canonical_text: str, indexed_words: Sequence[IndexedWord]) -> List[Detection]:
    if not canonical_text.strip():
        return []

    analyzer = _get_analyzer()
    target_entities = _resolve_target_pii_entities(analyzer)
    results = analyzer.analyze(
        text=canonical_text,
        entities=target_entities,
        language="en",
    )

    detections: List[Detection] = []
    for result in sorted(results, key=lambda item: (item.start, item.end)):
        if result.end <= result.start:
            continue

        entity_text = canonical_text[result.start : result.end]
        boxes = span_to_bboxes(result.start, result.end, indexed_words)
        if not boxes:
            continue

        detections.append(
            Detection(
                entity_text=entity_text,
                entity_type=result.entity_type,
                confidence_score=float(result.score or 0.0),
                source="Presidio",
                boxes=boxes,
            )
        )

    return detections


def run_llm_context_triage(
    canonical_text: str,
    indexed_words: Sequence[IndexedWord],
) -> Tuple[List[Detection], List[str]]:
    warnings: List[str] = []

    api_key = os.getenv("OPENROUTER_API_KEY") or os.getenv("LITELLM_API_KEY")
    if not api_key:
        warnings.append(
            "LLM step skipped: OPENROUTER_API_KEY or LITELLM_API_KEY is not configured. "
            "Pipeline continued with Presidio-only detections."
        )
        return [], warnings

    model = os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL)
    api_base = os.getenv("OPENROUTER_API_BASE", DEFAULT_OPENROUTER_API_BASE)
    text_slice = canonical_text[:LLM_TEXT_CHAR_LIMIT]

    try:
        response = completion(
            model=model,
            api_key=api_key,
            api_base=api_base,
            messages=_build_llm_messages(text_slice),
            temperature=0,
            max_tokens=350,
        )
        raw_content = _read_completion_content(response)
    except Exception as exc:  # broad except to guarantee fallback behavior
        warnings.append(
            "LLM step failed and pipeline continued with Presidio-only detections: "
            f"{str(exc)}"
        )
        return [], warnings

    quotes = _parse_llm_json_array(raw_content)
    if not quotes:
        warnings.append("LLM response did not contain a strict JSON array of string quotes.")
        return [], warnings

    detections: List[Detection] = []
    for quote in quotes:
        spans = find_exact_spans(canonical_text, quote)
        if not spans:
            continue

        inferred_type = classify_llm_quote_type(quote)
        for start_char, end_char in spans:
            boxes = span_to_bboxes(start_char, end_char, indexed_words)
            if not boxes:
                continue
            detections.append(
                Detection(
                    entity_text=quote,
                    entity_type=inferred_type,
                    confidence_score=0.85,
                    source="LLM",
                    boxes=boxes,
                )
            )

    if not detections:
        warnings.append("LLM returned quotes, but none matched OCR text exactly for localization.")

    return detections, warnings


def classify_llm_quote_type(quote: str) -> str:
    if re.search(r"\$\s?\d[\d,]*(?:\.\d+)?", quote):
        return "FINANCIAL_PENALTY_AMOUNT"

    normalized = re.sub(r"[\.,]", "", quote).strip().lower()
    if normalized in US_STATE_NAMES:
        return "JURISDICTION_STATE"

    upper = quote.strip().upper()
    if upper in US_STATE_ABBREVIATIONS:
        return "JURISDICTION_STATE"

    return "LEGAL_PARTY_NAME"


def find_exact_spans(text: str, quote: str) -> List[Tuple[int, int]]:
    if not text or not quote:
        return []
    return [(match.start(), match.end()) for match in re.finditer(re.escape(quote), text)]


def apply_secure_redactions(pdf_bytes: bytes, detections: Sequence[Detection]) -> bytes:
    if not detections:
        return pdf_bytes

    document = fitz.open(stream=pdf_bytes, filetype="pdf")
    boxes_by_page: Dict[int, List[BoundingBox]] = {}

    for detection in detections:
        for box in detection.boxes:
            boxes_by_page.setdefault(box.page_number, []).append(box)

    try:
        for page_number, page_boxes in boxes_by_page.items():
            if page_number < 0 or page_number >= document.page_count:
                continue

            page = document[page_number]
            for box in deduplicate_boxes(page_boxes):
                rect = fitz.Rect(box.x0, box.y0, box.x1, box.y1)
                if rect.is_empty or rect.is_infinite:
                    continue
                page.add_redact_annot(rect, fill=(0, 0, 0))

            page.apply_redactions()

        return document.tobytes(garbage=4, deflate=True, clean=True)
    finally:
        document.close()


def serialize_detections(detections: Sequence[Detection]) -> List[DetectedEntity]:
    serialized: List[DetectedEntity] = []
    for detection in detections:
        score = min(max(float(detection.confidence_score), 0.0), 1.0)
        serialized.append(
            DetectedEntity(
                entity_text=detection.entity_text,
                entity_type=detection.entity_type,
                confidence_score=score,
                source=detection.source,
                boxes=[
                    BBoxModel(
                        page_number=box.page_number + 1,
                        x0=box.x0,
                        y0=box.y0,
                        x1=box.x1,
                        y1=box.y1,
                    )
                    for box in deduplicate_boxes(detection.boxes)
                ],
            )
        )
    return serialized


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


def _build_llm_messages(document_text: str) -> List[Dict[str, str]]:
    system_prompt = (
        "You extract confidentiality entities from legal and corporate documents. "
        "Return ONLY a strict JSON array of exact strings that appear verbatim in the input."
    )

    user_prompt = f"""
Targets:
1) Legal party names
2) Financial penalty amounts
3) Jurisdiction states

Example 1
Input:
This Service Agreement is between Acme Holdings LLC and Beta Logistics Inc. A breach triggers liquidated damages of $250,000. Governing law is Texas.
Output:
["Acme Holdings LLC", "Beta Logistics Inc.", "$250,000", "Texas"]

Example 2
Input:
The parties Northwind Energy Ltd. and Sunrise Manufacturing Co. agree that late delivery incurs a penalty of $1,500,000 under California law.
Output:
["Northwind Energy Ltd.", "Sunrise Manufacturing Co.", "$1,500,000", "California"]

Example 3
Input:
This contract binds Apex Insurance Group with Riverview Clinics. Non-compliance results in damages of $75,000 and disputes are resolved in New York.
Output:
["Apex Insurance Group", "Riverview Clinics", "$75,000", "New York"]

Now process the following text and return only a strict JSON array of strings:
{document_text}
""".strip()

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def _read_completion_content(response: object) -> str:
    if isinstance(response, dict):
        choices = response.get("choices", [])
        if not choices:
            return ""
        message = choices[0].get("message", {})
        return str(message.get("content", "") or "")

    choices = getattr(response, "choices", None)
    if not choices:
        return ""

    message = getattr(choices[0], "message", None)
    if message is None:
        return ""

    content = getattr(message, "content", "")
    if isinstance(content, list):
        chunks = []
        for item in content:
            if isinstance(item, dict) and "text" in item:
                chunks.append(str(item["text"]))
            else:
                chunks.append(str(item))
        return "".join(chunks)

    return str(content or "")


def _parse_llm_json_array(raw_content: str) -> List[str]:
    if not raw_content:
        return []

    def coerce_quotes(data: object) -> List[str]:
        if not isinstance(data, list):
            return []

        deduped: List[str] = []
        seen = set()
        for item in data:
            if not isinstance(item, str):
                continue
            candidate = item.strip()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        return deduped

    try:
        return coerce_quotes(json.loads(raw_content.strip()))
    except json.JSONDecodeError:
        match = re.search(r"\[[\s\S]*\]", raw_content)
        if not match:
            return []
        try:
            return coerce_quotes(json.loads(match.group(0)))
        except json.JSONDecodeError:
            return []


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_target_pii_entities(analyzer: AnalyzerEngine) -> List[str]:
    supported_entities = sorted(set(analyzer.get_supported_entities(language="en")))

    if not supported_entities:
        raise RuntimeError("Presidio has no supported entities configured for language 'en'.")

    env_override = os.getenv("TARGET_PII_ENTITIES", "").strip()
    if env_override:
        if env_override.upper() == "ALL":
            return supported_entities

        requested_entities = [item.strip() for item in env_override.split(",") if item.strip()]
        selected_entities = [item for item in requested_entities if item in supported_entities]
        unknown_entities = sorted(set(requested_entities) - set(selected_entities))

        if unknown_entities:
            LOGGER.warning(
                "Ignoring unsupported TARGET_PII_ENTITIES values: %s",
                ", ".join(unknown_entities),
            )

        if selected_entities:
            return selected_entities

        LOGGER.warning(
            "TARGET_PII_ENTITIES override did not match any supported entities. Falling back to all entities."
        )

    if TARGET_PII_ENTITIES:
        selected_static_entities = [item for item in TARGET_PII_ENTITIES if item in supported_entities]
        if selected_static_entities:
            return selected_static_entities
        LOGGER.warning(
            "Static TARGET_PII_ENTITIES did not match supported entities. Falling back to all entities."
        )

    return supported_entities


def _get_analyzer() -> AnalyzerEngine:
    global _ANALYZER
    if _ANALYZER is not None:
        return _ANALYZER

    try:
        _ANALYZER = AnalyzerEngine()
    except Exception as primary_exc:
        LOGGER.warning("Default Presidio initialization failed: %s", str(primary_exc))
        try:
            _ANALYZER = AnalyzerEngine(nlp_engine=None, supported_languages=["en"])
        except Exception as fallback_exc:
            raise RuntimeError(
                "Presidio AnalyzerEngine could not initialize. "
                "Install spaCy model with: python -m spacy download en_core_web_sm"
            ) from fallback_exc

    return _ANALYZER
