# SNV PII Redact: Architecture and Showcase Demo

## 1) Idea in One Line

SNV PII Redact is a document sanitization platform that detects sensitive legal and business information in PDFs and applies forensic-grade redaction so hidden text is actually removed, not just visually covered.

## 2) What This Project Is Built On

### Core Runtime

- Python 3.11+
- FastAPI backend API and async job orchestration
- Streamlit frontend for interactive demonstrations

### Detection and Redaction Stack

- PyMuPDF for PDF parsing, coordinates, and redaction
- Tesseract OCR (via pytesseract) for scanned pages
- Microsoft Presidio for PII/entity detection
- spaCy transformer model (en_core_web_trf) for high-quality NER
- OpenRouter LLM call for context-aware legal entity extraction
- RapidFuzz for quote localization and fuzzy span matching

### Supporting Libraries

- Requests for backend/frontend HTTP calls
- Pandas for entity table visualization

Reference: [requirements.txt](requirements.txt)

## 3) Architecture (Current Prototype)

This is the architecture currently implemented in code.

```mermaid
flowchart LR
    U[User] --> FE[Streamlit Frontend]
    FE -->|POST PDF| API[FastAPI /api/v1/sanitize]
    API --> Q[In-memory Queue]
    Q --> W[Background Worker Thread]

    W --> EX[Hybrid Text Extraction\nPyMuPDF then OCR fallback]
    EX --> MAP[Character Offset to Bounding Box Map]
    MAP --> P[Presidio Analysis]
    MAP --> L[LLM Quote Detection via OpenRouter]
    P --> FUSE[Confidence + Dedup Fusion]
    L --> FUSE
    FUSE --> REDACT[Secure PDF Redaction\nadd_redact_annot + apply_redactions]
    REDACT --> STORE[Temp File Storage]

    FE -->|poll| JOB[GET /api/v1/jobs/{job_id}]
    FE -->|download| DL[GET /api/v1/download/{job_id}]
    JOB --> API
    DL --> API
```

Implementation anchors:

- API and worker orchestration: [backend/main.py](backend/main.py#L349), [backend/main.py](backend/main.py#L395), [backend/main.py](backend/main.py#L412), [backend/main.py](backend/main.py#L435), [backend/main.py](backend/main.py#L445)
- Pipeline entry point: [backend/main.py](backend/main.py#L539)
- Hybrid extraction (native text + OCR fallback): [backend/main.py](backend/main.py#L576), [backend/main.py](backend/main.py#L611), [backend/main.py](backend/main.py#L639)
- Character to box mapping: [backend/main.py](backend/main.py#L727), [backend/main.py](backend/main.py#L777)
- Presidio triage and JSON value focus: [backend/main.py](backend/main.py#L793), [backend/main.py](backend/main.py#L1341), [backend/main.py](backend/main.py#L1353)
- LLM triage with fuzzy localization: [backend/main.py](backend/main.py#L853), [backend/main.py](backend/main.py#L949), [backend/main.py](backend/main.py#L1215)
- Forensic redaction: [backend/main.py](backend/main.py#L1007), [backend/main.py](backend/main.py#L1028), [backend/main.py](backend/main.py#L1030)
- Demo frontend zones and actions: [frontend/app.py](frontend/app.py#L106), [frontend/app.py](frontend/app.py#L120), [frontend/app.py](frontend/app.py#L166), [frontend/app.py](frontend/app.py#L187)


Why this architecture:

- Horizontal scale for heavy OCR workloads
- Durable queue and retries for fault tolerance
- Persistent metadata for auditing and compliance
- Object storage for large file lifecycle management
- Better observability and SLA tracking

## 5) Complete Demo Plan (10 to 15 Minutes)

### Demo Goal

Show that the system can ingest a mixed-quality PDF, detect high-risk entities, and produce a verifiably sanitized output.

### Demo Assets

- A text PDF containing names, dollar amounts, and jurisdiction states
- A scanned PDF page image to prove OCR fallback
- A configured .env with OpenRouter key for LLM augmentation

### Script

1. Explain the problem in 30 seconds.
2. Open the UI and point out Zone 1, Zone 2, Zone 3.
3. Upload a sample PDF and click Queue Sanitization.
4. Show queued to processing to completed state.
5. In Zone 2, show detected entities with:
   - entity type
   - confidence
   - source (Presidio or LLM)
   - page references
6. Open Zone 3 and preview the sanitized PDF.
7. Download the output and confirm sensitive content is redacted.
8. Mention secure redaction semantics (content removal, not overlay masking).
9. Show fallback behavior by removing LLM key and rerunning:
   - pipeline still works with Presidio-only mode
10. Close with production architecture evolution and ROI.

## 6) Setup Commands for Live Demo

From repository root:

```powershell
pip install -r requirements.txt
python -m spacy download en_core_web_trf
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
streamlit run frontend/app.py
```

Reference setup sections: [README.md](README.md#L37), [README.md](README.md#L88), [README.md](README.md#L102)

## 7) API Demo Commands (Optional)

Queue:

```powershell
curl -X POST "http://localhost:8000/api/v1/sanitize" -F "file=@sample.pdf"
```

Poll:

```powershell
curl "http://localhost:8000/api/v1/jobs/<job_id>"
```

Download:

```powershell
curl -L "http://localhost:8000/api/v1/download/<job_id>" --output sanitized_sample.pdf
```

API contract reference: [README.md](README.md#L109)

## 8) Showcase Talking Points

- Privacy and compliance by design
- Handles both digital text and scanned documents
- Combines deterministic NLP and context-aware LLM extraction
- Keeps geometry-accurate redaction mapping from text offsets to PDF coordinates
- Clear path to enterprise-scale deployment

## 9) Known Prototype Constraints

- Queue and job store are in-memory (single-instance scope)
- Output files are in temporary local storage
- No authentication or RBAC yet
- No persistent audit database yet

## 10) Next Phase Roadmap

1. Replace in-memory queue with Redis/RabbitMQ/SQS.
2. Persist job state in PostgreSQL.
3. Move PDFs to object storage with lifecycle policies.
4. Add auth, tenant isolation, and audit logs.
5. Add load tests and benchmark dashboards.
