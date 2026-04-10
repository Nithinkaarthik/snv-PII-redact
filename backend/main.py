from __future__ import annotations

import ast
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Full, Queue
from threading import Lock, Thread
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple
from urllib.parse import urljoin
from uuid import uuid4

import fitz
import requests
from fastapi import FastAPI, File, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from rapidfuzz import fuzz
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider

try:
    from backend.config import (
        BUSINESS_KEYWORD_PATTERN,
        DEFAULT_LLM_MODEL,
        DEFAULT_OPENROUTER_API_BASE,
        DEFAULT_SPACY_MODEL,
        FUZZY_MATCH_THRESHOLD,
        IGNORE_JSON_KEYS,
        JOB_STORAGE_DIR,
        JOB_TTL_SECONDS,
        LLM_PARSE_MAX_RETRIES,
        LLM_REQUEST_TIMEOUT_SECONDS,
        LLM_RETRY_PREVIEW_CHARS,
        LLM_TEXT_CHAR_LIMIT,
        LOGGER,
        MAX_FILE_SIZE_BYTES,
        MAX_JOB_QUEUE_SIZE,
        MIN_ENTITY_CONFIDENCE,
        NATIVE_TEXT_MIN_ALNUM,
        TARGET_PII_ENTITIES,
        US_STATE_ABBREVIATIONS,
        US_STATE_NAMES,
        _clean_env_value,
        _get_openrouter_api_key,
        _normalize_openrouter_api_base,
    )
except ImportError:
    from config import (
        BUSINESS_KEYWORD_PATTERN,
        DEFAULT_LLM_MODEL,
        DEFAULT_OPENROUTER_API_BASE,
        DEFAULT_SPACY_MODEL,
        FUZZY_MATCH_THRESHOLD,
        IGNORE_JSON_KEYS,
        JOB_STORAGE_DIR,
        JOB_TTL_SECONDS,
        LLM_PARSE_MAX_RETRIES,
        LLM_REQUEST_TIMEOUT_SECONDS,
        LLM_RETRY_PREVIEW_CHARS,
        LLM_TEXT_CHAR_LIMIT,
        LOGGER,
        MAX_FILE_SIZE_BYTES,
        MAX_JOB_QUEUE_SIZE,
        MIN_ENTITY_CONFIDENCE,
        NATIVE_TEXT_MIN_ALNUM,
        TARGET_PII_ENTITIES,
        US_STATE_ABBREVIATIONS,
        US_STATE_NAMES,
        _clean_env_value,
        _get_openrouter_api_key,
        _normalize_openrouter_api_base,
    )

try:
    from backend.models import BoundingBox, OCRWord, WordSpan
    from backend.ocr import extract_page_words
    from backend.text_mapping import build_character_bbox_map, deduplicate_boxes, get_bboxes_for_offsets
except ImportError:
    from models import BoundingBox, OCRWord, WordSpan
    from ocr import extract_page_words
    from text_mapping import build_character_bbox_map, deduplicate_boxes, get_bboxes_for_offsets

JobStatus = Literal["queued", "processing", "completed", "failed"]

_ANALYZER: Optional[AnalyzerEngine] = None
WORKER_THREAD: Optional[Thread] = None
JOB_QUEUE: Queue[str] = Queue(maxsize=MAX_JOB_QUEUE_SIZE)
JOB_STORE: Dict[str, "JobRecord"] = {}
JOB_LOCK = Lock()
JOB_STATUS: Dict[str, Dict[str, Any]] = {}
JOB_STATUS_LOCK = Lock()
_DEBUG_FALSE_VALUES = {"0", "false", "no", "off"}
DEBUG_BLOCKS_ENABLED = os.getenv("BACKEND_DEBUG_BLOCKS", "0").strip().lower() not in _DEBUG_FALSE_VALUES
BACKEND_DIR = Path(__file__).resolve().parent
REPO_ROOT = BACKEND_DIR.parent
FRONTEND_DIR = REPO_ROOT / "frontend"
FRONTEND_ENTRYPOINT = FRONTEND_DIR / "index.html"


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
    progress: float = Field(default=0.0, ge=0.0, le=1.0)
    status_message: Optional[str] = None
    detected_entities: List[DetectedEntity] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    error: Optional[str] = None
    download_url: Optional[str] = None


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
    input_pdf_path: Optional[str] = None
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

if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend-assets")


@app.on_event("startup")
def startup_event() -> None:
    JOB_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    _start_worker_if_needed()


@app.get("/", include_in_schema=False)
def serve_frontend() -> FileResponse:
    if not FRONTEND_ENTRYPOINT.exists():
        raise HTTPException(status_code=404, detail="Frontend entrypoint was not found.")
    return FileResponse(path=str(FRONTEND_ENTRYPOINT))


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
    input_path = JOB_STORAGE_DIR / f"{job_id}.input.pdf"

    try:
        input_path.write_bytes(payload)
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to persist uploaded PDF for processing: {str(exc)}",
        ) from exc

    record = JobRecord(
        job_id=job_id,
        filename=filename,
        status="queued",
        created_at=created_at,
        updated_at=created_at,
        input_pdf_path=str(input_path),
        input_pdf_bytes=None,
    )

    with JOB_LOCK:
        JOB_STORE[job_id] = record

    _update_job_status(
        job_id,
        progress=0.0,
        status_message="Queued",
        state="queued",
        error=None,
    )
    _log_debug_block(
        "JOB_QUEUED",
        job_id=job_id,
        filename=filename,
        upload_bytes=len(payload),
        queue_size=JOB_QUEUE.qsize(),
    )

    try:
        JOB_QUEUE.put_nowait(job_id)
    except Full:
        with JOB_LOCK:
            JOB_STORE.pop(job_id, None)
        _delete_file_quietly(str(input_path))
        _update_job_status(
            job_id,
            progress=0.0,
            status_message="FAILED",
            state="failed",
            error="Sanitization queue is full. Please retry shortly.",
        )
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
    progress_state = _read_job_status(job_id)

    download_url = f"/api/v1/download/{job_id}" if record.status == "completed" else None
    progress = float(progress_state.get("progress", 1.0 if record.status == "completed" else 0.0))
    status_message = str(progress_state.get("status") or record.status)

    return SanitizeJobStatusResponse(
        job_id=record.job_id,
        status=record.status,
        progress=max(0.0, min(progress, 1.0)),
        status_message=status_message,
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
            _update_job_status(
                job_id,
                status_message="FAILED",
                state="failed",
                error=str(exc),
            )
            _log_debug_block(
                "WORKER_UNHANDLED_EXCEPTION",
                job_id=job_id,
                error=str(exc),
            )
        finally:
            JOB_QUEUE.task_done()


def _process_job(job_id: str) -> None:
    processing_error: Optional[str] = None

    with JOB_LOCK:
        record = JOB_STORE.get(job_id)
        if record is None:
            return
        input_pdf_path = record.input_pdf_path
        payload = record.input_pdf_bytes
        record.status = "processing"
        record.updated_at = _utc_now()

    _log_debug_block(
        "JOB_PROCESSING_STARTED",
        job_id=job_id,
        input_pdf_path=input_pdf_path,
        has_inline_payload=bool(payload),
    )

    _update_job_status(
        job_id,
        progress=0.0,
        status_message="Processing page 0 of 0",
        state="processing",
        error=None,
    )

    try:
        if not input_pdf_path and not payload:
            raise RuntimeError("No input payload available for processing.")

        def _page_progress(current_page: int, total_pages: int) -> None:
            _update_job_status(
                job_id,
                progress=(current_page / total_pages) if total_pages > 0 else 0.0,
                status_message=f"Processing page {current_page} of {total_pages}",
                state="processing",
            )
            _log_debug_block(
                "PAGE_PROGRESS",
                job_id=job_id,
                page=current_page,
                total_pages=total_pages,
                progress=f"{current_page}/{total_pages}",
            )

        detections, warnings, redacted_pdf_bytes = run_sanitization_pipeline(
            pdf_bytes=payload,
            pdf_input_path=input_pdf_path,
            progress_callback=_page_progress,
        )
        _log_debug_block(
            "PIPELINE_RESULT",
            job_id=job_id,
            detection_count=len(detections),
            warning_count=len(warnings),
            output_bytes=len(redacted_pdf_bytes),
        )
        output_path = JOB_STORAGE_DIR / f"{job_id}.pdf"
        output_path.write_bytes(redacted_pdf_bytes)

        serialized_entities = serialize_detections(detections)
        _mark_job_completed(job_id, serialized_entities, warnings, str(output_path))
        _update_job_status(
            job_id,
            progress=1.0,
            status_message="Completed",
            state="completed",
            error=None,
        )
        _log_debug_block(
            "JOB_COMPLETED",
            job_id=job_id,
            output_pdf_path=str(output_path),
        )
    except Exception as exc:
        processing_error = str(exc)
        _mark_job_failed(job_id, processing_error)
    finally:
        if processing_error is not None:
            _update_job_status(
                job_id,
                status_message="FAILED",
                state="failed",
                error=processing_error,
            )
            _log_debug_block(
                "JOB_FAILED",
                job_id=job_id,
                error=processing_error,
            )


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
        _delete_file_quietly(record.input_pdf_path)
        record.input_pdf_path = None
        record.input_pdf_bytes = None


def _mark_job_failed(job_id: str, error: str) -> None:
    with JOB_LOCK:
        record = JOB_STORE.get(job_id)
        if record is None:
            return
        record.status = "failed"
        record.updated_at = _utc_now()
        record.error = error
        _delete_file_quietly(record.input_pdf_path)
        record.input_pdf_path = None
        record.input_pdf_bytes = None


def _get_job_or_404(job_id: str) -> JobRecord:
    with JOB_LOCK:
        record = JOB_STORE.get(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} was not found.")
        return record


def _debug_safe_text(value: Any, max_chars: int = 240) -> str:
    compact = re.sub(r"\s+", " ", str(value)).strip()
    if len(compact) <= max_chars:
        return compact
    return f"{compact[:max_chars]}..."


def _log_debug_block(title: str, **details: Any) -> None:
    if not DEBUG_BLOCKS_ENABLED:
        return

    lines = [f"[DEBUG] ===== {title} ====="]
    for key, value in details.items():
        lines.append(f"[DEBUG] {key}: {_debug_safe_text(value)}")
    lines.append("[DEBUG] =====================")
    LOGGER.info("\n%s", "\n".join(lines))


def _update_job_status(
    job_id: str,
    *,
    progress: Optional[float] = None,
    status_message: Optional[str] = None,
    state: Optional[JobStatus] = None,
    error: Optional[str] = None,
) -> None:
    with JOB_STATUS_LOCK:
        status_payload = JOB_STATUS.get(job_id, {})

        if progress is not None:
            status_payload["progress"] = max(0.0, min(float(progress), 1.0))

        if status_message is not None:
            status_payload["status"] = status_message

        if state is not None:
            status_payload["state"] = state

        status_payload["error"] = error
        status_payload["updated_at"] = _utc_now().isoformat()
        JOB_STATUS[job_id] = status_payload


def _read_job_status(job_id: str) -> Dict[str, Any]:
    with JOB_STATUS_LOCK:
        return dict(JOB_STATUS.get(job_id, {}))


def _delete_file_quietly(file_path: Optional[str]) -> None:
    if not file_path:
        return

    candidate = Path(file_path)
    if not candidate.exists():
        return

    try:
        candidate.unlink()
    except OSError:
        LOGGER.warning("Failed to delete file: %s", file_path)


def _cleanup_expired_jobs() -> None:
    expiry_cutoff = _utc_now() - timedelta(seconds=JOB_TTL_SECONDS)
    stale_job_ids: List[str] = []

    with JOB_LOCK:
        for job_id, record in JOB_STORE.items():
            if record.updated_at < expiry_cutoff:
                stale_job_ids.append(job_id)

        for job_id in stale_job_ids:
            stale = JOB_STORE.pop(job_id)
            _delete_file_quietly(stale.input_pdf_path)
            if stale.output_pdf_path:
                stale_path = Path(stale.output_pdf_path)
                if stale_path.exists():
                    try:
                        stale_path.unlink()
                    except OSError:
                        LOGGER.warning("Failed to delete stale output file: %s", stale.output_pdf_path)

            with JOB_STATUS_LOCK:
                JOB_STATUS.pop(job_id, None)


def get_text_chunks(
    text: str,
    chunk_size: int = 2000,
    overlap: int = 200,
) -> List[Dict[str, Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")

    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >= 0 and smaller than chunk_size.")

    normalized_text = text or ""
    if not normalized_text:
        return []

    chunks: List[Dict[str, Any]] = []
    cursor = 0
    text_length = len(normalized_text)

    while cursor < text_length:
        end_index = min(text_length, cursor + chunk_size)
        chunks.append(
            {
                "chunk_text": normalized_text[cursor:end_index],
                "global_offset": cursor,
            }
        )

        if end_index >= text_length:
            break

        cursor = max(cursor + 1, end_index - overlap)

    return chunks


def _open_pdf_document(
    pdf_bytes: Optional[bytes],
    pdf_input_path: Optional[str],
) -> fitz.Document:
    if pdf_input_path:
        return fitz.open(pdf_input_path)

    if pdf_bytes:
        return fitz.open(stream=pdf_bytes, filetype="pdf")

    raise RuntimeError("No PDF payload available for sanitization.")


def _shift_char_map_offsets(
    char_map: Sequence[Tuple[int, int, BoundingBox]],
    global_offset: int,
) -> List[Tuple[int, int, BoundingBox]]:
    return [
        (start_char + global_offset, end_char + global_offset, bbox)
        for start_char, end_char, bbox in char_map
    ]


def _should_run_llm_for_page(
    page_text: str,
    presidio_detections: Sequence[Detection],
    llm_pages_processed: int,
    llm_max_pages_per_job: int,
    llm_min_page_chars: int,
    llm_skip_when_presidio_count: int,
) -> bool:
    if llm_max_pages_per_job > 0 and llm_pages_processed >= llm_max_pages_per_job:
        return False

    if len(page_text.strip()) < llm_min_page_chars:
        return False

    if len(presidio_detections) >= llm_skip_when_presidio_count:
        return False

    return True


def run_sanitization_pipeline(
    pdf_bytes: Optional[bytes] = None,
    *,
    pdf_input_path: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Tuple[List[Detection], List[str], bytes]:
    all_detections: List[Detection] = []
    warnings: List[str] = []
    document_char_offset = 0
    llm_pages_processed = 0
    llm_max_pages_per_job = max(0, int(os.getenv("LLM_MAX_PAGES_PER_JOB", "10")))
    llm_min_page_chars = max(0, int(os.getenv("LLM_MIN_PAGE_CHARS", "120")))
    llm_skip_when_presidio_count = max(1, int(os.getenv("LLM_SKIP_WHEN_PRESIDIO_COUNT", "2")))

    with _open_pdf_document(pdf_bytes=pdf_bytes, pdf_input_path=pdf_input_path) as document:
        total_pages = document.page_count
        if total_pages <= 0:
            raise RuntimeError("Uploaded PDF has no pages.")

        _log_debug_block(
            "PIPELINE_STARTED",
            total_pages=total_pages,
            source="file" if pdf_input_path else "memory",
        )

        for page_number in range(total_pages):
            page = document[page_number]
            page_words = extract_page_words(page, page_number)

            page_detections: List[Detection] = []
            page_text = ""
            if page_words:
                page_text, page_char_map_local, page_word_spans_local = build_character_bbox_map(page_words)
                if page_text.strip():
                    page_char_map_absolute = _shift_char_map_offsets(page_char_map_local, document_char_offset)
                    presidio_detections = run_presidio_triage(
                        page_text,
                        page_char_map_absolute,
                        chunk_size=2000,
                        overlap=200,
                        base_global_offset=document_char_offset,
                    )
                    if _should_run_llm_for_page(
                        page_text,
                        presidio_detections,
                        llm_pages_processed,
                        llm_max_pages_per_job,
                        llm_min_page_chars,
                        llm_skip_when_presidio_count,
                    ):
                        llm_detections, llm_warnings = run_llm_context_triage(
                            page_text,
                            page_char_map_local,
                            page_word_spans_local,
                        )
                        llm_pages_processed += 1
                    else:
                        llm_detections, llm_warnings = [], []
                    warnings.extend(llm_warnings)
                    page_detections = deduplicate_entities(presidio_detections + llm_detections)

            page_boxes = [
                box
                for detection in page_detections
                for box in detection.boxes
                if box.page_number == page_number
            ]

            for box in deduplicate_boxes(page_boxes):
                rect = fitz.Rect(box.x0, box.y0, box.x1, box.y1)
                if rect.is_empty or rect.is_infinite:
                    continue
                page.add_redact_annot(quad=rect, fill=(0, 0, 0))

            # Apply exactly once per page to keep incremental memory usage bounded.
            page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)

            _log_debug_block(
                "PAGE_ANALYZED",
                page=page_number + 1,
                total_pages=total_pages,
                words=len(page_words),
                detections=len(page_detections),
                cumulative_detections=len(all_detections) + len(page_detections),
            )

            all_detections.extend(page_detections)
            document_char_offset += len(page_text) + 1

            if progress_callback is not None:
                progress_callback(page_number + 1, total_pages)

        redacted_pdf_bytes = document.tobytes(garbage=4, deflate=True, clean=True)

    deduplicated = deduplicate_entities(all_detections)
    unique_warnings = list(dict.fromkeys(warnings))
    _log_debug_block(
        "PIPELINE_FINISHED",
        raw_detection_count=len(all_detections),
        deduplicated_detection_count=len(deduplicated),
        warning_count=len(unique_warnings),
    )
    return deduplicated, unique_warnings, redacted_pdf_bytes


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


def run_presidio_triage(
    canonical_text: str,
    char_map: Sequence[Tuple[int, int, BoundingBox]],
    *,
    chunk_size: int = 2000,
    overlap: int = 200,
    base_global_offset: int = 0,
) -> List[Detection]:
    if not canonical_text.strip():
        return []

    analyzer = _get_analyzer()
    target_entities = _resolve_target_pii_entities(analyzer)
    text_chunks = get_text_chunks(canonical_text, chunk_size=chunk_size, overlap=overlap)
    detections: List[Detection] = []
    for chunk in text_chunks:
        chunk_text = str(chunk.get("chunk_text") or "")
        if not chunk_text.strip():
            continue

        chunk_offset_raw = chunk.get("global_offset", 0)
        try:
            chunk_offset = int(chunk_offset_raw)
        except (TypeError, ValueError):
            chunk_offset = 0

        chunk_global_offset = base_global_offset + chunk_offset
        analyzable_text, offset_map = _prepare_text_for_presidio(chunk_text)
        if not analyzable_text.strip():
            continue

        results = analyzer.analyze(
            text=analyzable_text,
            entities=target_entities,
            language="en",
        )

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
                len(chunk_text),
            )
            if remapped_offsets is None:
                continue

            chunk_start, chunk_end = remapped_offsets
            entity_text = chunk_text[chunk_start:chunk_end].strip()
            if not entity_text:
                continue

            absolute_start = chunk_global_offset + chunk_start
            absolute_end = chunk_global_offset + chunk_end
            boxes = get_bboxes_for_offsets(absolute_start, absolute_end, char_map)
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
    llm_max_output_tokens = max(300, min(1800, int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "700"))))
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
                max_tokens=llm_max_output_tokens,
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

        _log_debug_block(
            "LLM_PARSE_FAILURE",
            attempts=LLM_PARSE_MAX_RETRIES,
            response_preview=preview or "<empty>",
        )
        warnings.append(
            "LLM returned non-JSON output after retries for at least one chunk. "
            "That chunk was skipped and processing continued."
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
        _log_debug_block(
            "LLM_LOCALIZATION_MISS",
            quote_candidates=len(candidates),
            threshold=FUZZY_MATCH_THRESHOLD,
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

    token_count = max(1, len(normalized_quote.split()))
    window_sizes = sorted(
        {
            max(1, token_count - 3),
            max(1, token_count - 1),
            token_count,
            token_count + 1,
            token_count + 2,
            token_count + 3,
            token_count + 4,
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
                float(fuzz.token_set_ratio(normalized_quote, normalized_candidate)),
                float(fuzz.partial_ratio(normalized_quote, normalized_candidate)),
            )
            # Strongly reward exact containment after normalization (common with extra context words).
            if normalized_quote in normalized_candidate or normalized_candidate in normalized_quote:
                similarity = max(similarity, 98.0)

            candidates.append((similarity, left.start_char, right.end_char))

    def _select_non_overlapping(min_similarity: float) -> List[Tuple[float, int, int]]:
        selected: List[Tuple[float, int, int]] = []
        for similarity, start_char, end_char in sorted(candidates, key=lambda item: (-item[0], item[1], item[2])):
            if similarity < min_similarity:
                continue

            overlaps = any(
                not (end_char <= chosen_start or start_char >= chosen_end)
                for _similarity, chosen_start, chosen_end in selected
            )
            if overlaps:
                continue

            selected.append((similarity, start_char, end_char))

        return selected

    selected = _select_non_overlapping(float(threshold))
    if not selected:
        relaxed_threshold = _adaptive_fuzzy_threshold(normalized_quote, threshold)
        if relaxed_threshold < threshold:
            selected = _select_non_overlapping(float(relaxed_threshold))

    return [(start_char, end_char, similarity) for similarity, start_char, end_char in selected]


def _adaptive_fuzzy_threshold(normalized_quote: str, base_threshold: int) -> int:
    token_count = max(1, len(normalized_quote.split()))
    char_count = len(normalized_quote)

    if token_count >= 6 or char_count >= 45:
        return max(80, base_threshold - 10)
    if token_count >= 4 or char_count >= 28:
        return max(84, base_threshold - 8)
    return max(88, base_threshold - 4)


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


def deduplicate_entities(detected_entities: Sequence[Detection]) -> List[Detection]:
    winners_by_box: Dict[Tuple[int, float, float, float, float], Tuple[Detection, BoundingBox]] = {}

    for detection in detected_entities:
        confidence = float(detection.confidence_score)
        if confidence < MIN_ENTITY_CONFIDENCE:
            continue

        for box in deduplicate_boxes(detection.boxes):
            key = (
                box.page_number,
                round(box.x0, 2),
                round(box.y0, 2),
                round(box.x1, 2),
                round(box.y1, 2),
            )
            existing = winners_by_box.get(key)
            if existing is None:
                winners_by_box[key] = (detection, box)
                continue

            existing_detection, _existing_box = existing
            existing_confidence = float(existing_detection.confidence_score)
            should_replace = confidence > existing_confidence
            if confidence == existing_confidence and detection.source == "Presidio" and existing_detection.source != "Presidio":
                should_replace = True

            if should_replace:
                winners_by_box[key] = (detection, box)

    deduplicated: Dict[Tuple[str, str, str, float], Detection] = {}
    for detection, box in winners_by_box.values():
        normalized_text = re.sub(r"\s+", " ", detection.entity_text).strip()
        if not normalized_text:
            continue

        aggregate_key = (
            normalized_text.lower(),
            detection.entity_type,
            detection.source,
            round(float(detection.confidence_score), 6),
        )
        aggregate = deduplicated.get(aggregate_key)
        if aggregate is None:
            aggregate = Detection(
                entity_text=normalized_text,
                entity_type=detection.entity_type,
                confidence_score=float(detection.confidence_score),
                source=detection.source,
                boxes=[],
            )
            deduplicated[aggregate_key] = aggregate

        aggregate.boxes.append(box)

    for aggregate in deduplicated.values():
        aggregate.boxes = deduplicate_boxes(aggregate.boxes)

    return sorted(
        deduplicated.values(),
        key=lambda item: (item.entity_type, item.entity_text.lower()),
    )


def deduplicate_detections(detections: Sequence[Detection]) -> List[Detection]:
    return deduplicate_entities(detections)


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


def _build_llm_messages(
    document_text: str,
    retry_feedback: str = "",
    previous_response: str = "",
) -> List[Dict[str, str]]:
    system_prompt = (
        "You extract personally identifiable and sensitive information from documents.\n"
        "Output contract:\n"
        "1) Return a JSON array as the top-level value, no wrapper object.\n"
        "2) Each array item must be an object with exactly: quote, category, confidence.\n"
        "3) quote must be verbatim text from input.\n"
        "4) category must be a concise label (prefer UPPER_SNAKE_CASE) chosen by you.\n"
        "5) category is open-ended; do not limit yourself to any fixed list.\n"
        "6) confidence must be numeric in range 0 to 1.\n"
        "7) Do not return markdown, prose, code fences, or wrapper objects.\n"
        "8) First output character must be '[' and last character must be ']'.\n"
        "9) If nothing is found, return [].\n"
        "10) Never include analysis, explanation, or preface text."
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


def _normalize_json_like_text(raw_content: str) -> str:
    normalized = str(raw_content or "")
    normalized = normalized.replace("\u201c", '"').replace("\u201d", '"')
    normalized = normalized.replace("\u2018", "'").replace("\u2019", "'")
    normalized = re.sub(r",(\s*[}\]])", r"\1", normalized)
    return normalized


def _loads_json_maybe_nested(raw_content: str) -> Optional[Any]:
    candidate = _normalize_json_like_text(raw_content).strip()
    if not candidate:
        return None

    for _ in range(2):
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            try:
                parsed = ast.literal_eval(candidate)
            except (ValueError, SyntaxError):
                return None

        if isinstance(parsed, str):
            candidate = parsed.strip()
            continue

        return parsed

    return None


def _extract_balanced_json_segments(raw_content: str, open_char: str, close_char: str) -> List[str]:
    segments: List[str] = []
    if not raw_content:
        return segments

    in_string = False
    quote_char = ""
    escape_next = False
    depth = 0
    start_index: Optional[int] = None

    for index, char in enumerate(raw_content):
        if escape_next:
            escape_next = False
            continue

        if in_string and char == "\\":
            escape_next = True
            continue

        if char in {'"', "'"}:
            if not in_string:
                in_string = True
                quote_char = char
            elif char == quote_char:
                in_string = False
                quote_char = ""
            continue

        if in_string:
            continue

        if char == open_char:
            if depth == 0:
                start_index = index
            depth += 1
            continue

        if char == close_char and depth > 0:
            depth -= 1
            if depth == 0 and start_index is not None:
                segments.append(raw_content[start_index : index + 1])
                start_index = None

    return segments


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


def _extract_llm_objects_from_text(raw_content: str) -> List[Any]:
    objects: List[Any] = []
    for object_blob in _extract_balanced_json_segments(raw_content, "{", "}"):
        parsed = _loads_json_maybe_nested(object_blob)
        if isinstance(parsed, dict) and any(key in parsed for key in ("quote", "text", "value", "entity")):
            objects.append(parsed)
    return objects


def _extract_llm_plaintext_items(raw_content: str) -> List[Any]:
    if not raw_content:
        return []

    text = _normalize_json_like_text(_strip_markdown_code_fence(raw_content))
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    items: List[Any] = []

    for line in lines:
        compact_line = re.sub(r"^\s*(?:[-*\u2022]+|\d+[\.)])\s*", "", line).strip()
        if not compact_line:
            continue

        table_item = _parse_llm_markdown_table_row(compact_line)
        if table_item is not None:
            items.append(table_item)
            continue

        keyed_item = _parse_llm_keyed_line(compact_line)
        if keyed_item is not None:
            items.append(keyed_item)
            continue

        quoted_item = _parse_llm_quoted_line(compact_line)
        if quoted_item is not None:
            items.append(quoted_item)

    return items


def _parse_llm_markdown_table_row(line: str) -> Optional[Dict[str, Any]]:
    if "|" not in line:
        return None

    cells = [cell.strip() for cell in line.strip("|").split("|")]
    if len(cells) < 3:
        return None

    lower_cells = [cell.lower() for cell in cells[:3]]
    if lower_cells[0] in {"quote", "text", "entity"}:
        return None
    if re.fullmatch(r"[-: ]+", cells[0]):
        return None

    quote = cells[0].strip("`\"'")
    category = cells[1].strip("`\"'")
    confidence = cells[2].strip("`\"'")
    if not quote:
        return None

    return {
        "quote": quote,
        "category": category,
        "confidence": confidence or 0.85,
    }


def _parse_llm_keyed_line(line: str) -> Optional[Dict[str, Any]]:
    keyed_token_pattern = re.compile(
        r"(?i)\b(quote|text|entity|category|type|label|confidence|score)\b\s*[:=]"
    )
    matches = list(keyed_token_pattern.finditer(line))
    if len(matches) < 2:
        return None

    values: Dict[str, str] = {}
    for index, match in enumerate(matches):
        key = match.group(1).lower()
        value_start = match.end()
        value_end = matches[index + 1].start() if index + 1 < len(matches) else len(line)
        value = line[value_start:value_end].strip(" \t,;|-")
        if value:
            values[key] = value.strip("`\"'")

    quote = values.get("quote") or values.get("text") or values.get("entity")
    if not quote:
        return None

    category = values.get("category") or values.get("type") or values.get("label") or ""
    confidence = values.get("confidence") or values.get("score") or 0.85
    return {
        "quote": quote,
        "category": category,
        "confidence": confidence,
    }


def _parse_llm_quoted_line(line: str) -> Optional[Dict[str, Any]]:
    match = re.search(r"[\"'](?P<quote>[^\"']+)[\"']\s*(?P<tail>.*)$", line)
    if not match:
        return None

    quote = (match.group("quote") or "").strip()
    tail = (match.group("tail") or "").strip()

    confidence_match = re.search(r"(\d+(?:\.\d+)?%?)", tail)
    confidence = confidence_match.group(1) if confidence_match else 0.85

    category = tail
    if confidence_match:
        category = tail[: confidence_match.start()]

    category = re.sub(r"(?i)^category\s*[:=]\s*", "", category)
    category = category.strip(" \t-_|(),:")
    if not quote:
        return None

    return {
        "quote": quote,
        "category": category,
        "confidence": confidence,
    }


def _parse_confidence_value(confidence_raw: Any, default: float = 0.85) -> float:
    if isinstance(confidence_raw, (int, float)):
        value = float(confidence_raw)
    else:
        raw_text = str(confidence_raw or "").strip()
        if not raw_text:
            value = default
        else:
            percent_match = re.search(r"(-?\d+(?:\.\d+)?)\s*%", raw_text)
            if percent_match:
                value = _safe_float(percent_match.group(1), default=default) / 100.0
            else:
                numeric_match = re.search(r"-?\d+(?:\.\d+)?", raw_text)
                if numeric_match:
                    value = _safe_float(numeric_match.group(0), default=default)
                else:
                    value = default

    if value > 1:
        value = value / 100.0

    return max(0.0, min(1.0, value))


def _build_llm_quote_candidates_from_items(items: Sequence[Any]) -> List[LLMQuoteCandidate]:
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

        confidence_value = _parse_confidence_value(confidence_raw, default=0.85)

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

    return list(deduped.values())


def _parse_llm_quote_candidates(raw_content: str) -> Tuple[List[LLMQuoteCandidate], bool]:
    if not raw_content:
        return [], False

    payload: Optional[Any] = None
    primary_text = raw_content.strip()
    fenced_text = _strip_markdown_code_fence(primary_text)

    candidate_texts: List[str] = [primary_text, fenced_text]
    candidate_texts.extend(_extract_balanced_json_segments(primary_text, "[", "]"))
    candidate_texts.extend(_extract_balanced_json_segments(primary_text, "{", "}"))

    for candidate_text in candidate_texts:
        if not candidate_text:
            continue
        payload = _loads_json_maybe_nested(candidate_text)
        if payload is None:
            continue

        items = _extract_items_from_llm_payload(payload)
        if items is None:
            continue

        candidates = _build_llm_quote_candidates_from_items(items)
        return candidates, True

    fallback_items = _extract_llm_objects_from_text(primary_text)
    if fallback_items:
        candidates = _build_llm_quote_candidates_from_items(fallback_items)
        return candidates, True

    plaintext_items = _extract_llm_plaintext_items(primary_text)
    if plaintext_items:
        candidates = _build_llm_quote_candidates_from_items(plaintext_items)
        return candidates, True

    return [], False


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
    normalized = str(text or "").upper()
    normalized = re.sub(r"(?<=\d)[OQ](?=\d|$)", "0", normalized)
    normalized = re.sub(r"(?<=\d)[IL](?=\d|$)", "1", normalized)
    normalized = re.sub(r"[^A-Z0-9$]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
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
