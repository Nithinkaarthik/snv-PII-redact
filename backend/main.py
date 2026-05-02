from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Full, Queue
from threading import Lock, Thread
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple
from uuid import uuid4

import fitz
from fastapi import FastAPI, File, Form, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

try:
    from backend.config import (
        CORS_ALLOW_CREDENTIALS,
        CORS_ALLOW_HEADERS,
        CORS_ALLOW_METHODS,
        CORS_ALLOW_ORIGINS,
        JOB_STORAGE_DIR,
        JOB_TTL_SECONDS,
        LOGGER,
        MAX_FILE_SIZE_BYTES,
        MAX_JOB_QUEUE_SIZE,
    )
    from backend.face_detection import detect_faces_on_page
    from backend.models import BoundingBox, Detection, DetectionSource, EngineSource
    from backend.ocr import extract_page_words_with_tables
    from backend.services.dedup import deduplicate_entities, extract_supporting_sources
    from backend.services.llm import run_llm_triage
    from backend.services.presidio_analysis import (
        run_contextual_numeric_triage,
        run_presidio_triage,
    )
    from backend.services.redaction import sanitize_font_names, tighten_detections_for_page
    from backend.text_mapping import build_character_bbox_map, deduplicate_boxes
except ImportError:
    from config import (
        CORS_ALLOW_CREDENTIALS,
        CORS_ALLOW_HEADERS,
        CORS_ALLOW_METHODS,
        CORS_ALLOW_ORIGINS,
        JOB_STORAGE_DIR,
        JOB_TTL_SECONDS,
        LOGGER,
        MAX_FILE_SIZE_BYTES,
        MAX_JOB_QUEUE_SIZE,
    )
    from face_detection import detect_faces_on_page
    from models import BoundingBox, Detection, DetectionSource, EngineSource
    from ocr import extract_page_words_with_tables
    from services.dedup import deduplicate_entities, extract_supporting_sources
    from services.llm import run_llm_triage
    from services.presidio_analysis import run_contextual_numeric_triage, run_presidio_triage
    from services.redaction import sanitize_font_names, tighten_detections_for_page
    from text_mapping import build_character_bbox_map, deduplicate_boxes

JobStatus = Literal["queued", "processing", "completed", "failed"]

_DEBUG_FALSE_VALUES = {"0", "false", "no", "off"}
DEBUG_BLOCKS_ENABLED = os.getenv("BACKEND_DEBUG_BLOCKS", "0").strip().lower() not in _DEBUG_FALSE_VALUES

BACKEND_DIR = Path(__file__).resolve().parent
REPO_ROOT = BACKEND_DIR.parent
FRONTEND_DIR = REPO_ROOT / "frontend"
FRONTEND_ENTRYPOINT = FRONTEND_DIR / "index.html"

WORKER_THREAD: Optional[Thread] = None
JOB_QUEUE: Queue[str] = Queue(maxsize=MAX_JOB_QUEUE_SIZE)
JOB_STORE: Dict[str, "JobRecord"] = {}
JOB_LOCK = Lock()
JOB_STATUS: Dict[str, Dict[str, Any]] = {}
JOB_STATUS_LOCK = Lock()


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
    source: DetectionSource
    supporting_sources: List[EngineSource] = Field(default_factory=list)
    decision_reason: Optional[str] = None
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
    timings: Dict[str, float] = Field(default_factory=dict)


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
    enable_face_detection: bool = True


app = FastAPI(title="Document Sanitization Pipeline", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS.split(",") if CORS_ALLOW_ORIGINS != "*" else ["*"],
    allow_methods=CORS_ALLOW_METHODS.split(",") if CORS_ALLOW_METHODS != "*" else ["*"],
    allow_headers=CORS_ALLOW_HEADERS.split(",") if CORS_ALLOW_HEADERS != "*" else ["*"],
    allow_credentials=CORS_ALLOW_CREDENTIALS,
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
async def sanitize_document(
    file: UploadFile = File(...),
    enable_face_detection: bool = Form(True),
) -> SanitizeJobCreateResponse:
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
            status_code=500, detail=f"Unable to persist uploaded PDF for processing: {str(exc)}"
        ) from exc

    record = JobRecord(
        job_id=job_id,
        filename=filename,
        status="queued",
        created_at=created_at,
        updated_at=created_at,
        input_pdf_path=str(input_path),
        input_pdf_bytes=None,
        enable_face_detection=enable_face_detection,
    )

    with JOB_LOCK:
        JOB_STORE[job_id] = record

    _update_job_status(job_id, progress=0.0, status_message="Queued", state="queued", error=None)
    _log_debug_block(
        "JOB_QUEUED", job_id=job_id, filename=filename, upload_bytes=len(payload), queue_size=JOB_QUEUE.qsize()
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
        raise HTTPException(status_code=503, detail="Sanitization queue is full. Please retry shortly.")

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
    timings = dict(progress_state.get("timings", {}))

    return SanitizeJobStatusResponse(
        job_id=record.job_id,
        status=record.status,
        progress=max(0.0, min(progress, 1.0)),
        status_message=status_message,
        detected_entities=record.detected_entities,
        warnings=record.warnings,
        error=record.error,
        download_url=download_url,
        timings=timings,
    )


@app.get("/api/v1/download/{job_id}")
def download_redacted_pdf(job_id: str) -> FileResponse:
    _cleanup_expired_jobs()
    record = _get_job_or_404(job_id)

    if record.status != "completed":
        raise HTTPException(
            status_code=409, detail=f"Job {job_id} is not completed yet (current status: {record.status})."
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
        except Exception as exc:
            LOGGER.exception("Unhandled exception in job worker for job %s: %s", job_id, str(exc))
            _mark_job_failed(job_id, str(exc))
            _update_job_status(job_id, status_message="FAILED", state="failed", error=str(exc))
            _log_debug_block("WORKER_UNHANDLED_EXCEPTION", job_id=job_id, error=str(exc))
        finally:
            JOB_QUEUE.task_done()


def _process_job(job_id: str) -> None:
    with JOB_LOCK:
        record = JOB_STORE.get(job_id)
        if record is None:
            return
        input_pdf_path = record.input_pdf_path
        payload = record.input_pdf_bytes
        record.status = "processing"
        record.updated_at = _utc_now()

    _log_debug_block(
        "JOB_PROCESSING_STARTED", job_id=job_id, input_pdf_path=input_pdf_path, has_inline_payload=bool(payload)
    )
    _update_job_status(job_id, progress=0.0, status_message="Processing page 0 of 0", state="processing", error=None)

    try:
        if not input_pdf_path and not payload:
            raise RuntimeError("No input payload available for processing.")

        def _page_progress(
            current_page: int, total_pages: int, page_timings: Optional[Dict[str, float]] = None, extra_status: str = ""
        ) -> None:
            msg = f"Processing page {current_page} of {total_pages}"
            if extra_status:
                msg += f" — {extra_status}"
            _update_job_status(
                job_id,
                progress=(current_page / total_pages) if total_pages > 0 else 0.0,
                status_message=msg,
                state="processing",
                timings=page_timings,
            )

        detections, warnings, redacted_pdf_bytes, pipeline_timings = run_sanitization_pipeline(
            pdf_bytes=payload,
            pdf_input_path=input_pdf_path,
            progress_callback=_page_progress,
            enable_face_detection=record.enable_face_detection,
        )

        output_path = JOB_STORAGE_DIR / f"{job_id}.pdf"
        output_path.write_bytes(redacted_pdf_bytes)

        serialized_entities = serialize_detections(detections)
        _mark_job_completed(job_id, serialized_entities, warnings, str(output_path))
        _update_job_status(
            job_id, progress=1.0, status_message="Completed", state="completed", error=None, timings=pipeline_timings
        )
    except Exception as exc:
        _mark_job_failed(job_id, str(exc))
        _update_job_status(job_id, status_message="FAILED", state="failed", error=str(exc))
        _log_debug_block("JOB_FAILED", job_id=job_id, error=str(exc))


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
    timings: Optional[Dict[str, float]] = None,
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
        if timings is not None:
            status_payload["timings"] = timings
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


def _open_pdf_document(pdf_bytes: Optional[bytes], pdf_input_path: Optional[str]) -> fitz.Document:
    if pdf_input_path:
        return fitz.open(pdf_input_path)
    if pdf_bytes:
        return fitz.open(stream=pdf_bytes, filetype="pdf")
    raise RuntimeError("No PDF payload available for sanitization.")


def _shift_char_map_offsets(
    char_map: Sequence[Tuple[int, int, BoundingBox]],
    global_offset: int,
) -> List[Tuple[int, int, BoundingBox]]:
    return [(start_char + global_offset, end_char + global_offset, bbox) for start_char, end_char, bbox in char_map]


def _should_run_llm_for_page(
    page_text: str,
    presidio_detections: Sequence[Detection],
    llm_pages_processed: int,
    llm_max_pages_per_job: int,
    llm_min_page_chars: int,
    llm_skip_when_presidio_count: int,
) -> bool:
    if not page_text.strip():
        return False
    if llm_max_pages_per_job > 0 and llm_pages_processed >= llm_max_pages_per_job:
        return False
    if llm_min_page_chars > 0 and len(page_text.strip()) < llm_min_page_chars:
        return False
    if llm_skip_when_presidio_count > 0 and len(presidio_detections) >= llm_skip_when_presidio_count:
        return False
    return True


def run_sanitization_pipeline(
    pdf_bytes: Optional[bytes] = None,
    *,
    pdf_input_path: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int, Optional[Dict[str, float]], str], None]] = None,
    enable_face_detection: bool = True,
) -> Tuple[List[Detection], List[str], bytes, Dict[str, float]]:
    all_detections: List[Detection] = []
    warnings: List[str] = []
    document_char_offset = 0
    llm_pages_processed = 0
    llm_max_pages_per_job = max(0, int(os.getenv("LLM_MAX_PAGES_PER_JOB", "0")))
    llm_min_page_chars = max(0, int(os.getenv("LLM_MIN_PAGE_CHARS", "0")))
    llm_skip_when_presidio_count = max(0, int(os.getenv("LLM_SKIP_WHEN_PRESIDIO_COUNT", "0")))
    timer: Dict[str, float] = {
        "ocr": 0.0,
        "char_map": 0.0,
        "presidio": 0.0,
        "llm": 0.0,
        "llm_api": 0.0,
        "llm_parse": 0.0,
        "llm_parse_strip": 0.0,
        "llm_parse_extract": 0.0,
        "llm_parse_json": 0.0,
        "llm_parse_fallback": 0.0,
        "llm_fuzzy": 0.0,
        "llm_bbox": 0.0,
        "contextual_rules": 0.0,
        "dedup": 0.0,
        "face_detection": 0.0,
        "redaction": 0.0,
        "font_subset": 0.0,
        "dedup_final": 0.0,
    }

    with _open_pdf_document(pdf_bytes=pdf_bytes, pdf_input_path=pdf_input_path) as document:
        total_pages = document.page_count
        if total_pages <= 0:
            raise RuntimeError("Uploaded PDF has no pages.")

        for page_number in range(total_pages):
            # Report progress at the start of each page (current / total = 0 for page 0)
            if progress_callback is not None:
                progress_callback(page_number, total_pages, timer)

            page = document[page_number]

            _t0 = time.perf_counter()
            page_words, page_tables, page_line_cache = extract_page_words_with_tables(page, page_number)
            timer["ocr"] += time.perf_counter() - _t0
            if progress_callback is not None:
                progress_callback(page_number, total_pages, timer, "OCR done")

            page_detections: List[Detection] = []
            page_text = ""
            if page_words:
                _t0 = time.perf_counter()
                page_text, page_char_map_local, page_word_spans_local = build_character_bbox_map(
                    page_words,
                    table_regions=page_tables,
                )
                timer["char_map"] += time.perf_counter() - _t0
                if progress_callback is not None:
                    progress_callback(page_number, total_pages, timer, "Char map done")
                if page_text.strip():
                    page_char_map_absolute = _shift_char_map_offsets(page_char_map_local, document_char_offset)

                    _t0 = time.perf_counter()
                    presidio_detections = run_presidio_triage(
                        page_text,
                        page_char_map_absolute,
                        chunk_size=2000,
                        overlap=200,
                        base_global_offset=document_char_offset,
                    )
                    timer["presidio"] += time.perf_counter() - _t0
                    if progress_callback is not None:
                        progress_callback(page_number, total_pages, timer, "Presidio done")

                    if _should_run_llm_for_page(
                        page_text,
                        presidio_detections,
                        llm_pages_processed,
                        llm_max_pages_per_job,
                        llm_min_page_chars,
                        llm_skip_when_presidio_count,
                    ):
                        _t0 = time.perf_counter()
                        llm_detections, llm_warnings = run_llm_triage(
                            page_text,
                            page_char_map_local,
                            page_word_spans_local,
                            table_regions=page_tables,
                            timer=timer,
                        )
                        timer["llm"] += time.perf_counter() - _t0
                        if progress_callback is not None:
                            progress_callback(page_number, total_pages, timer, "LLM done")
                        llm_pages_processed += 1
                    else:
                        llm_detections, llm_warnings = [], []

                    _t0 = time.perf_counter()
                    contextual_rule_detections = run_contextual_numeric_triage(page_text, page_char_map_local)
                    timer["contextual_rules"] += time.perf_counter() - _t0
                    if progress_callback is not None:
                        progress_callback(page_number, total_pages, timer, "Contextual done")

                    warnings.extend(llm_warnings)
                    _t0 = time.perf_counter()
                    page_detections = deduplicate_entities(
                        presidio_detections + llm_detections + contextual_rule_detections
                    )
                    timer["dedup"] += time.perf_counter() - _t0
                    if progress_callback is not None:
                        progress_callback(page_number, total_pages, timer, "Dedup done")

            # Face detection — run on every page regardless of whether text was extracted
            if enable_face_detection:
                try:
                    _t0 = time.perf_counter()
                    face_boxes = detect_faces_on_page(page, page_number)
                    timer["face_detection"] += time.perf_counter() - _t0
                    if progress_callback is not None:
                        progress_callback(page_number, total_pages, timer, "Face detection done")
                    if face_boxes:
                        face_detection = Detection(
                            entity_text="[FACE - REDACTED]",
                            entity_type="FACE",
                            confidence_score=0.99,
                            source="Vision",
                            boxes=face_boxes,
                            supporting_sources=["Vision"],
                            decision_reason="cv2_multi_cascade",
                        )
                        page_detections.append(face_detection)
                except Exception as e:
                    _log_debug_block("FACE_DETECTION_ERROR", error=str(e))

            # Tighten boxes for both text and face detections
            if page_detections:
                _t0 = time.perf_counter()
                page_detections = tighten_detections_for_page(page_detections, line_cache=page_line_cache)
                timer["redaction"] += time.perf_counter() - _t0
                if progress_callback is not None:
                    progress_callback(page_number, total_pages, timer, "Tighten done")

            _t0 = time.perf_counter()
            page_boxes = [
                box for detection in page_detections for box in detection.boxes if box.page_number == page_number
            ]
            for box in deduplicate_boxes(page_boxes):
                rect = fitz.Rect(box.x0, box.y0, box.x1, box.y1)
                rect = rect & page.rect
                if rect.is_empty or rect.is_infinite:
                    continue
                page.add_redact_annot(rect, fill=(0, 0, 0))
            page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)
            timer["redaction"] += time.perf_counter() - _t0
            if progress_callback is not None:
                progress_callback(page_number, total_pages, timer, "Redactions applied")

            all_detections.extend(page_detections)
            document_char_offset += len(page_text) + 1

        _t0 = time.perf_counter()
        try:
            document.subset_fonts()
        except Exception as e:
            _log_debug_block("FONT_SUBSET_ERROR", error=str(e))

        sanitize_font_names(document)
        redacted_pdf_bytes = document.tobytes(garbage=4, deflate=True, clean=True)
        timer["font_subset"] += time.perf_counter() - _t0

    _t0 = time.perf_counter()
    deduplicated = deduplicate_entities(all_detections)
    timer["dedup_final"] += time.perf_counter() - _t0
    unique_warnings = list(dict.fromkeys(warnings))
    return deduplicated, unique_warnings, redacted_pdf_bytes, timer


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


def serialize_detections(detections: Sequence[Detection]) -> List[DetectedEntity]:
    serialized: List[DetectedEntity] = []
    for detection in detections:
        score = min(max(float(detection.confidence_score), 0.0), 1.0)
        supporting_sources = extract_supporting_sources(detection)
        serialized.append(
            DetectedEntity(
                entity_text=detection.entity_text,
                entity_type=detection.entity_type,
                confidence_score=score,
                source=detection.source,
                supporting_sources=supporting_sources,
                decision_reason=detection.decision_reason,
                boxes=[
                    BBoxModel(page_number=box.page_number + 1, x0=box.x0, y0=box.y0, x1=box.x1, y1=box.y1)
                    for box in deduplicate_boxes(detection.boxes)
                ],
            )
        )
    return serialized


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
