from __future__ import annotations

import io
import json
import logging
import os
import re
import tempfile
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Full, Queue
from threading import Lock, Thread
from typing import Any, Dict, List, Literal, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin
from uuid import uuid4

import fitz
import pytesseract
import requests
from fastapi import FastAPI, File, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from PIL import Image
from pydantic import BaseModel, Field
from rapidfuzz import fuzz
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider


def _strip_env_inline_comment(value: str) -> str:
    in_single_quote = False
    in_double_quote = False
    escape_next = False

    for index, char in enumerate(value):
        if escape_next:
            escape_next = False
            continue

        if char == "\\":
            escape_next = True
            continue

        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            continue

        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            continue

        if char == "#" and not in_single_quote and not in_double_quote:
            return value[:index].rstrip()

    return value.rstrip()


def _clean_env_value(raw_value: str) -> str:
    value = _strip_env_inline_comment((raw_value or "").strip())
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    return value.strip()


def _normalize_openrouter_api_key(raw_key: str) -> str:
    key = _clean_env_value(raw_key)
    if key.lower().startswith("bearer "):
        key = key[7:].strip()

    # OpenRouter keys are case-sensitive in practice and should begin with sk-or-.
    if key.lower().startswith("sk-or-"):
        key = f"sk-or-{key[6:]}"

    return key


def _load_local_env_files() -> None:
    backend_dir = Path(__file__).resolve().parent
    candidate_paths = [
        backend_dir / ".env",
        backend_dir.parent / ".env",
    ]

    for env_path in candidate_paths:
        if not env_path.exists():
            continue

        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.lstrip("\ufeff").strip()
                if not line or line.startswith("#"):
                    continue

                if line.lower().startswith("export "):
                    line = line[7:].strip()

                if "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = _clean_env_value(value)
                if not key:
                    continue

                os.environ.setdefault(key, value)
        except OSError as exc:
            logging.getLogger("sanitize_pipeline").warning(
                "Failed to load environment file %s: %s",
                str(env_path),
                str(exc),
            )


def _get_openrouter_api_key() -> str:
    return _normalize_openrouter_api_key(
        os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY") or ""
    )


def _normalize_openrouter_api_base(api_base: str) -> str:
    cleaned = _clean_env_value(api_base)
    if not cleaned:
        return DEFAULT_OPENROUTER_API_BASE

    lowered = cleaned.lower().rstrip("/")
    if lowered.endswith("/chat/completions"):
        cleaned = cleaned[: -len("/chat/completions")]

    root = cleaned.rstrip("/")
    if root in {"https://openrouter.ai", "http://openrouter.ai"}:
        cleaned = f"{root}/api/v1"

    return cleaned


_load_local_env_files()

LOGGER = logging.getLogger("sanitize_pipeline")
logging.basicConfig(level=logging.INFO)
logging.getLogger("presidio-analyzer").setLevel(logging.ERROR)
logging.getLogger("transformers").setLevel(logging.ERROR)
warnings.filterwarnings(
    "ignore",
    message=r".*torch.utils\._pytree\._register_pytree_node.*",
    category=FutureWarning,
)

TARGET_PII_ENTITIES: Optional[List[str]] = None
MAX_FILE_SIZE_BYTES = int(os.getenv("MAX_UPLOAD_MB", "30")) * 1024 * 1024
MAX_JOB_QUEUE_SIZE = int(os.getenv("MAX_JOB_QUEUE_SIZE", "32"))
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", "1800"))
NATIVE_TEXT_MIN_ALNUM = int(os.getenv("NATIVE_TEXT_MIN_ALNUM", "20"))

LLM_TEXT_CHAR_LIMIT = int(os.getenv("LLM_TEXT_CHAR_LIMIT", "20000"))
DEFAULT_LLM_MODEL = os.getenv("LLM_MODEL", "openai/gpt-oss-safeguard-20b")
DEFAULT_OPENROUTER_API_BASE = os.getenv("OPENROUTER_API_BASE", "https://openrouter.ai/api/v1")
LLM_REQUEST_TIMEOUT_SECONDS = int(os.getenv("LLM_REQUEST_TIMEOUT_SECONDS", "60"))
LLM_PARSE_MAX_RETRIES = max(1, int(os.getenv("LLM_PARSE_MAX_RETRIES", "3")))
LLM_RETRY_PREVIEW_CHARS = int(os.getenv("LLM_RETRY_PREVIEW_CHARS", "220"))

DEFAULT_SPACY_MODEL = os.getenv("PRESIDIO_SPACY_MODEL", "en_core_web_trf")
MIN_ENTITY_CONFIDENCE = float(os.getenv("MIN_ENTITY_CONFIDENCE", "0.7"))
FUZZY_MATCH_THRESHOLD = int(os.getenv("FUZZY_MATCH_THRESHOLD", "92"))

IGNORE_JSON_KEYS: Set[str] = {"id", "filename", "metadata.item", "input.ke"}
BUSINESS_KEYWORD_PATTERN = re.compile(
    r"\b(?:inc|inc\.|llc|corp|corp\.|corporation|co|co\.|company|ltd|ltd\.|plc|bioventures|ventures)\b",
    flags=re.IGNORECASE,
)

JOB_STORAGE_DIR = Path(
    os.getenv("SANITIZE_JOB_STORAGE_DIR", str(Path(tempfile.gettempdir()) / "snv-pii-redact-jobs"))
)

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

JobStatus = Literal["queued", "processing", "completed", "failed"]

_ANALYZER: Optional[AnalyzerEngine] = None
WORKER_THREAD: Optional[Thread] = None
JOB_QUEUE: Queue[str] = Queue(maxsize=MAX_JOB_QUEUE_SIZE)
JOB_STORE: Dict[str, "JobRecord"] = {}
JOB_LOCK = Lock()


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


class SanitizeJobCreateResponse(BaseModel):
    job_id: str
    status: JobStatus
    status_url: str
    download_url: str


class SanitizeJobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    detected_entities: List[DetectedEntity] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    error: Optional[str] = None
    download_url: Optional[str] = None


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
class LLMQuoteCandidate:
    quote: str
    category: str
    confidence: float


@dataclass
class Detection:
    entity_text: str
    entity_type: str
    confidence_score: float
    source: Literal["Presidio", "LLM"]
    boxes: List[BoundingBox]


@dataclass
class JobRecord:
    job_id: str
    filename: str
    status: JobStatus
    created_at: datetime
    updated_at: datetime
    input_pdf_bytes: Optional[bytes] = None
    output_pdf_path: Optional[str] = None
    detected_entities: List[DetectedEntity] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None


app = FastAPI(title="Document Sanitization Pipeline", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)


@app.on_event("startup")
def startup_event() -> None:
    JOB_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    _start_worker_if_needed()


@app.get("/health")
def health_check() -> Dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/api/v1/sanitize",
    response_model=SanitizeJobCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def sanitize_document(file: UploadFile = File(...)) -> SanitizeJobCreateResponse:
    _cleanup_expired_jobs()
    _start_worker_if_needed()

    payload = await file.read()
    _validate_pdf_upload(file, payload)

    job_id = uuid4().hex
    created_at = _utc_now()
    filename = file.filename or "uploaded.pdf"

    record = JobRecord(
        job_id=job_id,
        filename=filename,
        status="queued",
        created_at=created_at,
        updated_at=created_at,
        input_pdf_bytes=payload,
    )

    with JOB_LOCK:
        JOB_STORE[job_id] = record

    try:
        JOB_QUEUE.put_nowait(job_id)
    except Full:
        with JOB_LOCK:
            JOB_STORE.pop(job_id, None)
        raise HTTPException(
            status_code=503,
            detail="Sanitization queue is full. Please retry shortly.",
        )

    return SanitizeJobCreateResponse(
        job_id=job_id,
        status="queued",
        status_url=f"/api/v1/jobs/{job_id}",
        download_url=f"/api/v1/download/{job_id}",
    )


@app.get("/api/v1/jobs/{job_id}", response_model=SanitizeJobStatusResponse)
def get_job_status(job_id: str) -> SanitizeJobStatusResponse:
    _cleanup_expired_jobs()
    record = _get_job_or_404(job_id)

    download_url = f"/api/v1/download/{job_id}" if record.status == "completed" else None

    return SanitizeJobStatusResponse(
        job_id=record.job_id,
        status=record.status,
        detected_entities=record.detected_entities,
        warnings=record.warnings,
        error=record.error,
        download_url=download_url,
    )


@app.get("/api/v1/download/{job_id}")
def download_redacted_pdf(job_id: str) -> FileResponse:
    _cleanup_expired_jobs()
    record = _get_job_or_404(job_id)

    if record.status != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"Job {job_id} is not completed yet (current status: {record.status}).",
        )

    if not record.output_pdf_path:
        raise HTTPException(status_code=500, detail="Redacted file path was not recorded.")

    output_path = Path(record.output_pdf_path)
    if not output_path.exists():
        raise HTTPException(status_code=410, detail="Redacted file is no longer available.")

    filename_stem = Path(record.filename).stem
    download_name = f"sanitized_{filename_stem}.pdf"
    return FileResponse(path=str(output_path), media_type="application/pdf", filename=download_name)


def _start_worker_if_needed() -> None:
    global WORKER_THREAD

    if WORKER_THREAD is not None and WORKER_THREAD.is_alive():
        return

    WORKER_THREAD = Thread(target=_job_worker_loop, name="sanitize-job-worker", daemon=True)
    WORKER_THREAD.start()


def _job_worker_loop() -> None:
    while True:
        job_id = JOB_QUEUE.get()
        try:
            _process_job(job_id)
        except Exception as exc:  # broad except to avoid worker thread death
            LOGGER.exception("Unhandled exception in job worker for job %s: %s", job_id, str(exc))
            _mark_job_failed(job_id, str(exc))
        finally:
            JOB_QUEUE.task_done()


def _process_job(job_id: str) -> None:
    with JOB_LOCK:
        record = JOB_STORE.get(job_id)
        if record is None:
            return
        payload = record.input_pdf_bytes
        record.status = "processing"
        record.updated_at = _utc_now()

    if not payload:
        _mark_job_failed(job_id, "No input payload available for processing.")
        return

    try:
        detections, warnings, redacted_pdf_bytes = run_sanitization_pipeline(payload)
        output_path = JOB_STORAGE_DIR / f"{job_id}.pdf"
        output_path.write_bytes(redacted_pdf_bytes)

        serialized_entities = serialize_detections(detections)
        _mark_job_completed(job_id, serialized_entities, warnings, str(output_path))
    except Exception as exc:
        _mark_job_failed(job_id, str(exc))


def _mark_job_completed(
    job_id: str,
    detected_entities: List[DetectedEntity],
    warnings: List[str],
    output_pdf_path: str,
) -> None:
    with JOB_LOCK:
        record = JOB_STORE.get(job_id)
        if record is None:
            return
        record.status = "completed"
        record.updated_at = _utc_now()
        record.detected_entities = detected_entities
        record.warnings = warnings
        record.error = None
        record.output_pdf_path = output_pdf_path
        record.input_pdf_bytes = None


def _mark_job_failed(job_id: str, error: str) -> None:
    with JOB_LOCK:
        record = JOB_STORE.get(job_id)
        if record is None:
            return
        record.status = "failed"
        record.updated_at = _utc_now()
        record.error = error
        record.input_pdf_bytes = None


def _get_job_or_404(job_id: str) -> JobRecord:
    with JOB_LOCK:
        record = JOB_STORE.get(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} was not found.")
        return record


def _cleanup_expired_jobs() -> None:
    expiry_cutoff = _utc_now() - timedelta(seconds=JOB_TTL_SECONDS)
    stale_job_ids: List[str] = []

    with JOB_LOCK:
        for job_id, record in JOB_STORE.items():
            if record.updated_at < expiry_cutoff:
                stale_job_ids.append(job_id)

        for job_id in stale_job_ids:
            stale = JOB_STORE.pop(job_id)
            if stale.output_pdf_path:
                stale_path = Path(stale.output_pdf_path)
                if stale_path.exists():
                    try:
                        stale_path.unlink()
                    except OSError:
                        LOGGER.warning("Failed to delete stale output file: %s", stale.output_pdf_path)


def run_sanitization_pipeline(pdf_bytes: bytes) -> Tuple[List[Detection], List[str], bytes]:
    words, word_coordinate_map, phrase_coordinate_map = extract_words_with_coordinates(pdf_bytes)
    if not words:
        raise RuntimeError("No text could be extracted from the PDF.")

    LOGGER.info(
        "OCR extraction complete: %s words, %s unique words, %s phrase lines",
        len(words),
        len(word_coordinate_map),
        len(phrase_coordinate_map),
    )

    canonical_text, char_map, word_spans = build_character_bbox_map(words)
    if not canonical_text.strip():
        raise RuntimeError("Extracted text is empty after OCR processing.")

    presidio_detections = run_presidio_triage(canonical_text, char_map)
    llm_detections, llm_warnings = run_llm_context_triage(canonical_text, char_map, word_spans)
    all_detections = deduplicate_detections(presidio_detections + llm_detections)

    redacted_pdf_bytes = apply_secure_redactions(pdf_bytes, all_detections)
    return all_detections, llm_warnings, redacted_pdf_bytes


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


def run_presidio_triage(
    canonical_text: str,
    char_map: Sequence[Tuple[int, int, BoundingBox]],
) -> List[Detection]:
    if not canonical_text.strip():
        return []

    analyzer = _get_analyzer()
    target_entities = _resolve_target_pii_entities(analyzer)
    analyzable_text, offset_map = _prepare_text_for_presidio(canonical_text)
    if not analyzable_text.strip():
        return []

    results = analyzer.analyze(
        text=analyzable_text,
        entities=target_entities,
        language="en",
    )

    detections: List[Detection] = []
    for result in sorted(results, key=lambda item: (item.start, item.end)):
        if result.end <= result.start:
            continue

        confidence = float(result.score or 0.0)
        if confidence < MIN_ENTITY_CONFIDENCE:
            continue

        remapped_offsets = _remap_offsets_to_canonical(
            result.start,
            result.end,
            offset_map,
            len(canonical_text),
        )
        if remapped_offsets is None:
            continue

        canonical_start, canonical_end = remapped_offsets
        entity_text = canonical_text[canonical_start:canonical_end].strip()
        if not entity_text:
            continue

        boxes = get_bboxes_for_offsets(canonical_start, canonical_end, char_map)
        if not boxes:
            continue

        entity_type = _reclassify_entity_type(entity_text, result.entity_type)
        detections.append(
            Detection(
                entity_text=entity_text,
                entity_type=entity_type,
                confidence_score=confidence,
                source="Presidio",
                boxes=boxes,
            )
        )

    return detections


def run_llm_context_triage(
    canonical_text: str,
    char_map: Sequence[Tuple[int, int, BoundingBox]],
    word_spans: Sequence[WordSpan],
) -> Tuple[List[Detection], List[str]]:
    warnings: List[str] = []

    api_key = _get_openrouter_api_key()
    if not api_key:
        warnings.append(
            "LLM step skipped: OPENROUTER_API_KEY is not configured. "
            "(OPENAI_API_KEY is also accepted as fallback.) "
            "Pipeline continued with Presidio-only detections."
        )
        return [], warnings

    model = os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL)
    api_base = os.getenv("OPENROUTER_API_BASE", DEFAULT_OPENROUTER_API_BASE)
    text_slice = canonical_text[:LLM_TEXT_CHAR_LIMIT]

    raw_content = ""
    candidates: List[LLMQuoteCandidate] = []
    parse_succeeded = False

    for attempt in range(1, LLM_PARSE_MAX_RETRIES + 1):
        retry_feedback = ""
        if attempt > 1:
            retry_feedback = (
                "Previous response was invalid. Return ONLY a top-level JSON array of "
                "objects with quote, category, confidence."
            )

        try:
            response_json = _call_openrouter_chat_completion(
                api_base=api_base,
                api_key=api_key,
                model=model,
                messages=_build_llm_messages(
                    text_slice,
                    retry_feedback=retry_feedback,
                    previous_response=raw_content,
                ),
                temperature=0.0,
                max_tokens=450,
            )
            raw_content = _read_completion_content(response_json)
        except Exception as exc:  # broad except to guarantee fallback behavior
            if attempt >= LLM_PARSE_MAX_RETRIES:
                warnings.append(
                    "LLM step failed after retries and pipeline continued with Presidio-only detections: "
                    f"{str(exc)}"
                )
                return [], warnings
            continue

        candidates, parse_succeeded = _parse_llm_quote_candidates(raw_content)
        if parse_succeeded:
            break

    if not candidates:
        if parse_succeeded:
            # A valid empty array means the model found no LLM entities, not a parse failure.
            return [], warnings

        preview = re.sub(r"\s+", " ", raw_content).strip()
        if LLM_RETRY_PREVIEW_CHARS > 0:
            preview = preview[:LLM_RETRY_PREVIEW_CHARS]

        if preview:
            warnings.append(
                "LLM response did not contain a strict JSON array of quote-category-confidence objects "
                f"after {LLM_PARSE_MAX_RETRIES} attempts. Last response preview: {preview}"
            )
        else:
            warnings.append(
                "LLM response did not contain a strict JSON array of quote-category-confidence objects "
                f"after {LLM_PARSE_MAX_RETRIES} attempts."
            )
        return [], warnings

    detections: List[Detection] = []
    for candidate in candidates:
        matches = find_fuzzy_spans(candidate.quote, word_spans, threshold=FUZZY_MATCH_THRESHOLD)
        if not matches:
            continue

        inferred_type = _normalize_llm_category(candidate.category, candidate.quote)
        for start_char, end_char, similarity_score in matches:
            fuzzy_conf = max(0.0, min(1.0, similarity_score / 100.0))
            combined_conf = (candidate.confidence + fuzzy_conf) / 2.0
            if combined_conf < MIN_ENTITY_CONFIDENCE:
                continue

            boxes = get_bboxes_for_offsets(start_char, end_char, char_map)
            if not boxes:
                continue

            localized_text = canonical_text[start_char:end_char].strip() or candidate.quote
            detections.append(
                Detection(
                    entity_text=localized_text,
                    entity_type=inferred_type,
                    confidence_score=combined_conf,
                    source="LLM",
                    boxes=boxes,
                )
            )

    if not detections:
        warnings.append(
            "LLM returned quotes, but fuzzy matching did not localize any quote at >= 92% similarity."
        )

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


def find_fuzzy_spans(
    quote: str,
    word_spans: Sequence[WordSpan],
    threshold: int = FUZZY_MATCH_THRESHOLD,
) -> List[Tuple[int, int, float]]:
    normalized_quote = _normalize_for_fuzzy(quote)
    if not normalized_quote or not word_spans:
        return []

    token_count = max(1, len(quote.split()))
    window_sizes = sorted(
        {
            max(1, token_count - 2),
            max(1, token_count - 1),
            token_count,
            token_count + 1,
            token_count + 2,
        }
    )

    candidates: List[Tuple[float, int, int]] = []
    total_words = len(word_spans)

    for start_index in range(total_words):
        for window_size in window_sizes:
            end_index = start_index + window_size
            if end_index > total_words:
                continue

            left = word_spans[start_index]
            right = word_spans[end_index - 1]
            if left.bbox.page_number != right.bbox.page_number:
                continue

            candidate_text = " ".join(item.text for item in word_spans[start_index:end_index])
            normalized_candidate = _normalize_for_fuzzy(candidate_text)
            if not normalized_candidate:
                continue

            similarity = max(
                float(fuzz.ratio(normalized_quote, normalized_candidate)),
                float(fuzz.token_sort_ratio(normalized_quote, normalized_candidate)),
            )
            if similarity < threshold:
                continue

            candidates.append((similarity, left.start_char, right.end_char))

    selected: List[Tuple[float, int, int]] = []
    for similarity, start_char, end_char in sorted(candidates, key=lambda item: (-item[0], item[1], item[2])):
        overlaps = any(
            not (end_char <= chosen_start or start_char >= chosen_end)
            for _similarity, chosen_start, chosen_end in selected
        )
        if overlaps:
            continue
        selected.append((similarity, start_char, end_char))

    return [(start_char, end_char, similarity) for similarity, start_char, end_char in selected]


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
                page.add_redact_annot(quad=rect, fill=(0, 0, 0))

            page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)

        return document.tobytes(garbage=4, deflate=True, clean=True)
    finally:
        document.close()


def deduplicate_detections(detections: Sequence[Detection]) -> List[Detection]:
    deduplicated: Dict[Tuple[str, str], Detection] = {}

    for detection in detections:
        confidence = float(detection.confidence_score)
        if confidence < MIN_ENTITY_CONFIDENCE:
            continue

        normalized_text = re.sub(r"\s+", " ", detection.entity_text).strip().lower()
        if not normalized_text:
            continue

        key = (normalized_text, detection.entity_type)
        if key not in deduplicated:
            deduplicated[key] = Detection(
                entity_text=detection.entity_text.strip(),
                entity_type=detection.entity_type,
                confidence_score=confidence,
                source=detection.source,
                boxes=deduplicate_boxes(detection.boxes),
            )
            continue

        existing = deduplicated[key]
        existing.boxes = deduplicate_boxes(existing.boxes + detection.boxes)
        if confidence > existing.confidence_score or (
            confidence == existing.confidence_score and detection.source == "Presidio"
        ):
            existing.confidence_score = confidence
            existing.source = detection.source

    return sorted(
        deduplicated.values(),
        key=lambda item: (item.entity_type, item.entity_text.lower()),
    )


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


def _build_llm_messages(
    document_text: str,
    retry_feedback: str = "",
    previous_response: str = "",
) -> List[Dict[str, str]]:
    system_prompt = (
        "You extract personally identifiable and sensitive information from documents.\n"
        "Output contract:\n"
        "1) Return a JSON array as the top-level value.\n"
        "2) Each array item must be an object with exactly: quote, category, confidence.\n"
        "3) quote must be verbatim text from input.\n"
        "4) category must be a concise label (prefer UPPER_SNAKE_CASE) chosen by you.\n"
        "5) category is open-ended; do not limit yourself to any fixed list.\n"
        "6) confidence must be numeric in range 0 to 1.\n"
        "7) Do not return markdown, prose, code fences, or wrapper objects.\n"
        "8) If nothing is found, return []."
    )

    user_prompt = f"""
Detect any personally identifiable or sensitive information in the document.
Include names, addresses, phone/fax numbers, emails, identifiers, account numbers,
legal references, locations, organization names, and other sensitive data when present.

Return only the JSON array.

Document:
{document_text}
""".strip()

    if retry_feedback:
        user_prompt += f"\n\nRetry reason: {retry_feedback}"

    if previous_response:
        compact_prev = re.sub(r"\s+", " ", previous_response).strip()[:1200]
        user_prompt += (
            "\n\nPrevious invalid output (for correction):\n"
            f"{compact_prev}\n"
            "Re-emit as valid JSON array only."
        )

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


def _call_openrouter_chat_completion(
    api_base: str,
    api_key: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float,
    max_tokens: int,
) -> Dict[str, Any]:
    normalized_base = _normalize_openrouter_api_base(api_base)
    endpoint = f"{normalized_base.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    referer = _clean_env_value(
        os.getenv("OPENROUTER_SITE_URL")
        or os.getenv("OPENROUTER_HTTP_REFERER")
        or ""
    )
    if referer:
        headers["HTTP-Referer"] = referer

    x_title = _clean_env_value(
        os.getenv("OPENROUTER_SITE_NAME")
        or os.getenv("OPENROUTER_X_OPENROUTER_TITLE")
        or os.getenv("OPENROUTER_X_TITLE")
        or "snv-PII-redact"
    )
    if x_title:
        headers["X-OpenRouter-Title"] = x_title
        headers["X-Title"] = x_title

    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    response = requests.post(
        endpoint,
        headers=headers,
        json=payload,
        timeout=LLM_REQUEST_TIMEOUT_SECONDS,
        allow_redirects=False,
    )

    if response.status_code in {301, 302, 307, 308}:
        redirect_target = response.headers.get("Location", "").strip()
        if redirect_target:
            redirected_endpoint = redirect_target
            if redirect_target.startswith("/"):
                redirected_endpoint = urljoin(endpoint, redirect_target)

            response = requests.post(
                redirected_endpoint,
                headers=headers,
                json=payload,
                timeout=LLM_REQUEST_TIMEOUT_SECONDS,
                allow_redirects=False,
            )

    if response.status_code >= 400:
        message = response.text.strip()
        if len(message) > 400:
            message = f"{message[:400]}..."
        if response.status_code == 401 and "Missing Authentication header" in message:
            raise RuntimeError(
                "OpenRouter API error 401: Missing Authentication header. "
                "Sent Authorization header, but upstream did not receive it. "
                "Verify OPENROUTER_API_BASE is https://openrouter.ai/api/v1 and OPENROUTER_API_KEY is the raw token (without Bearer)."
            )
        raise RuntimeError(f"OpenRouter API error {response.status_code}: {message}")

    try:
        parsed = response.json()
    except ValueError as exc:
        raise RuntimeError("OpenRouter API returned a non-JSON response.") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError("OpenRouter API response shape is invalid.")

    return parsed


def _strip_markdown_code_fence(raw_content: str) -> str:
    stripped = raw_content.strip()
    if not stripped.startswith("```"):
        return stripped

    first_newline = stripped.find("\n")
    if first_newline != -1:
        stripped = stripped[first_newline + 1 :]

    if stripped.endswith("```"):
        stripped = stripped[:-3]

    return stripped.strip()


def _loads_json_maybe_nested(raw_content: str) -> Optional[Any]:
    candidate = raw_content.strip()
    if not candidate:
        return None

    for _ in range(2):
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            return None

        if isinstance(parsed, str):
            candidate = parsed.strip()
            continue

        return parsed

    return None


def _extract_items_from_llm_payload(payload: Any) -> Optional[List[Any]]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if any(key in payload for key in ("quote", "text", "value", "entity")):
            return [payload]

        for key in ("quotes", "results", "entities", "items", "data", "output", "candidates"):
            nested_payload = payload.get(key)
            nested_items = _extract_items_from_llm_payload(nested_payload)
            if nested_items is not None:
                return nested_items

        for nested_payload in payload.values():
            nested_items = _extract_items_from_llm_payload(nested_payload)
            if nested_items is not None:
                return nested_items

    return None


def _parse_llm_quote_candidates(raw_content: str) -> Tuple[List[LLMQuoteCandidate], bool]:
    if not raw_content:
        return [], False

    payload: Optional[Any] = None
    primary_text = raw_content.strip()
    fenced_text = _strip_markdown_code_fence(primary_text)

    for candidate_text in (primary_text, fenced_text):
        if not candidate_text:
            continue
        payload = _loads_json_maybe_nested(candidate_text)
        if payload is not None:
            break

    if payload is None:
        array_match = re.search(r"\[[\s\S]*\]", raw_content)
        if array_match:
            payload = _loads_json_maybe_nested(array_match.group(0))

    if payload is None:
        object_match = re.search(r"\{[\s\S]*\}", raw_content)
        if object_match:
            payload = _loads_json_maybe_nested(object_match.group(0))

    items = _extract_items_from_llm_payload(payload)
    if items is None:
        return [], False

    deduped: Dict[Tuple[str, str], LLMQuoteCandidate] = {}
    for item in items:
        quote = ""
        category = ""
        confidence_raw: Any = 0.85

        if isinstance(item, str):
            quote = item.strip()
        elif isinstance(item, dict):
            quote = str(item.get("quote") or item.get("text") or "").strip()
            category = str(
                item.get("category")
                or item.get("entity_type")
                or item.get("type")
                or item.get("label")
                or ""
            ).strip()
            confidence_raw = item.get("confidence", item.get("score", 0.85))
        else:
            continue

        if not quote:
            continue

        confidence_value = _safe_float(confidence_raw, default=0.85)
        if confidence_value > 1:
            confidence_value = confidence_value / 100.0
        confidence_value = max(0.0, min(1.0, confidence_value))

        normalized_quote = re.sub(r"\s+", " ", quote).strip().lower()
        normalized_category = re.sub(r"\s+", " ", category).strip().lower()
        key = (normalized_quote, normalized_category)
        existing = deduped.get(key)
        if existing is None or confidence_value > existing.confidence:
            deduped[key] = LLMQuoteCandidate(
                quote=quote,
                category=category,
                confidence=confidence_value,
            )

    return list(deduped.values()), True


def _normalize_llm_category(raw_category: str, quote: str) -> str:
    cleaned = str(raw_category or "").strip()
    if not cleaned:
        return classify_llm_quote_type(quote)

    cleaned = cleaned.replace("-", "_")
    cleaned = re.sub(r"[^A-Za-z0-9_\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", "_", cleaned).strip("_").upper()
    if not cleaned:
        return classify_llm_quote_type(quote)

    alias_map = {
        "NAME": "PERSON",
        "PERSON_NAME": "PERSON",
        "FULL_NAME": "PERSON",
        "COMPANY": "ORGANIZATION",
        "ORG": "ORGANIZATION",
        "PHONE": "PHONE_NUMBER",
        "MOBILE": "PHONE_NUMBER",
        "TELEPHONE": "PHONE_NUMBER",
        "FAX": "FAX_NUMBER",
        "ADDRESS": "STREET_ADDRESS",
        "STATE": "JURISDICTION_STATE",
        "AMOUNT": "FINANCIAL_AMOUNT",
    }
    return alias_map.get(cleaned, cleaned)


def _prepare_text_for_presidio(canonical_text: str) -> Tuple[str, List[int]]:
    value_segments = _extract_json_value_segments(canonical_text)
    if not value_segments:
        return canonical_text, list(range(len(canonical_text)))

    projected_text, projected_offset_map = _project_text_from_segments(canonical_text, value_segments)
    if not projected_text.strip() or not projected_offset_map:
        return "", []

    return projected_text, projected_offset_map


def _extract_json_value_segments(canonical_text: str) -> List[Tuple[int, int]]:
    decoder = json.JSONDecoder()
    collected_segments: List[Tuple[int, int]] = []
    cursor = 0

    while cursor < len(canonical_text):
        opening = re.search(r"[\{\[]", canonical_text[cursor:])
        if not opening:
            break

        start_index = cursor + opening.start()
        try:
            parsed_obj, consumed = decoder.raw_decode(canonical_text[start_index:])
        except json.JSONDecodeError:
            cursor = start_index + 1
            continue

        end_index = start_index + consumed
        collected_segments.extend(_locate_value_segments_in_json(canonical_text, start_index, end_index, parsed_obj))
        cursor = end_index

    if collected_segments:
        return _normalize_segments(collected_segments)

    regex_segments = _extract_json_value_segments_regex(canonical_text)
    return _normalize_segments(regex_segments)


def _extract_json_value_segments_regex(canonical_text: str) -> List[Tuple[int, int]]:
    pattern = re.compile(
        r'(?P<key>"?[A-Za-z0-9_.-]+"?)\s*:\s*(?P<value>"(?:\\.|[^"])*"|[^,\}\]\n]+)'
    )
    segments: List[Tuple[int, int]] = []

    for match in pattern.finditer(canonical_text):
        raw_key = match.group("key").strip().strip('"')
        if _is_ignored_key(raw_key):
            continue

        value_start = match.start("value")
        value_end = match.end("value")

        while value_start < value_end and canonical_text[value_start].isspace():
            value_start += 1
        while value_end > value_start and canonical_text[value_end - 1].isspace():
            value_end -= 1

        if value_end - value_start >= 2 and canonical_text[value_start] == '"' and canonical_text[value_end - 1] == '"':
            value_start += 1
            value_end -= 1

        if value_start < value_end:
            segments.append((value_start, value_end))

    return segments


def _locate_value_segments_in_json(
    canonical_text: str,
    segment_start: int,
    segment_end: int,
    parsed_obj: Any,
) -> List[Tuple[int, int]]:
    flat_values = _flatten_json_values(parsed_obj)
    if not flat_values:
        return []

    scope_text = canonical_text[segment_start:segment_end]
    scoped_segments: List[Tuple[int, int]] = []

    for key_path, raw_value in flat_values:
        if _is_ignored_key(key_path):
            continue

        value_text = _json_scalar_to_text(raw_value).strip()
        if not value_text:
            continue

        for match in re.finditer(re.escape(value_text), scope_text):
            abs_start = segment_start + match.start()
            abs_end = segment_start + match.end()
            if any(_ranges_overlap(abs_start, abs_end, start, end) for start, end in scoped_segments):
                continue
            scoped_segments.append((abs_start, abs_end))
            break

    return scoped_segments


def _flatten_json_values(value: Any, parent_key: str = "") -> List[Tuple[str, Any]]:
    flattened: List[Tuple[str, Any]] = []

    if isinstance(value, dict):
        for key, nested_value in value.items():
            key_part = str(key).strip()
            key_path = f"{parent_key}.{key_part}" if parent_key else key_part
            if _is_ignored_key(key_path):
                continue

            if isinstance(nested_value, (dict, list)):
                flattened.extend(_flatten_json_values(nested_value, key_path))
            else:
                flattened.append((key_path, nested_value))
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, (dict, list)):
                flattened.extend(_flatten_json_values(item, parent_key))
            else:
                flattened.append((parent_key, item))

    return flattened


def _project_text_from_segments(canonical_text: str, segments: Sequence[Tuple[int, int]]) -> Tuple[str, List[int]]:
    chunks: List[str] = []
    offset_map: List[int] = []

    for index, (start_char, end_char) in enumerate(segments):
        if start_char >= end_char:
            continue

        if index > 0:
            chunks.append("\n")
            anchor = max(0, start_char - 1)
            offset_map.append(anchor)

        chunks.append(canonical_text[start_char:end_char])
        offset_map.extend(range(start_char, end_char))

    return "".join(chunks), offset_map


def _normalize_segments(segments: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
    filtered = sorted((start, end) for start, end in segments if end > start)
    if not filtered:
        return []

    normalized: List[Tuple[int, int]] = []
    for start, end in filtered:
        if not normalized:
            normalized.append((start, end))
            continue

        previous_start, previous_end = normalized[-1]
        if start <= previous_end:
            normalized[-1] = (previous_start, max(previous_end, end))
        else:
            normalized.append((start, end))

    return normalized


def _json_scalar_to_text(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _is_ignored_key(key_path: str) -> bool:
    lower_key = key_path.lower()
    if lower_key in IGNORE_JSON_KEYS:
        return True
    return lower_key.split(".")[-1] in {"id", "filename"}


def _ranges_overlap(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    return start_a < end_b and end_a > start_b


def _remap_offsets_to_canonical(
    start_char: int,
    end_char: int,
    offset_map: Sequence[int],
    canonical_length: int,
) -> Optional[Tuple[int, int]]:
    if canonical_length <= 0 or end_char <= start_char or not offset_map:
        return None

    bounded_start = max(0, min(start_char, len(offset_map) - 1))
    bounded_end = max(0, min(end_char - 1, len(offset_map) - 1))

    canonical_start = offset_map[bounded_start]
    canonical_end = offset_map[bounded_end] + 1

    if canonical_end <= canonical_start:
        return None

    return canonical_start, canonical_end


def _reclassify_entity_type(entity_text: str, original_type: str) -> str:
    if original_type != "PERSON":
        return original_type
    if BUSINESS_KEYWORD_PATTERN.search(entity_text):
        return "ORGANIZATION"
    return original_type


def _normalize_for_fuzzy(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text.upper()).strip()
    normalized = re.sub(r"(?<=\d)[OQ](?=\d|$)", "0", normalized)
    return normalized


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

    nlp_configuration = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": DEFAULT_SPACY_MODEL}],
    }

    try:
        provider = NlpEngineProvider(nlp_configuration=nlp_configuration)
        nlp_engine = provider.create_engine()
        _ANALYZER = AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=["en"])
    except Exception as primary_exc:
        LOGGER.warning("Transformer Presidio initialization failed: %s", str(primary_exc))
        try:
            fallback_provider = NlpEngineProvider(
                nlp_configuration={
                    "nlp_engine_name": "spacy",
                    "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
                }
            )
            fallback_engine = fallback_provider.create_engine()
            _ANALYZER = AnalyzerEngine(nlp_engine=fallback_engine, supported_languages=["en"])
        except Exception as fallback_exc:
            raise RuntimeError(
                "Presidio AnalyzerEngine could not initialize. "
                "Install spaCy transformer model with: python -m spacy download en_core_web_trf"
            ) from fallback_exc

    return _ANALYZER


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
