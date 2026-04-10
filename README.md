# snv-PII-redact

Robust document sanitization pipeline with a decoupled architecture:

- FastAPI backend for OCR, PII/context extraction, and forensic PDF redaction.
- Tailwind + vanilla JavaScript frontend for upload, queue monitoring, entity inspection, and sanitized download.

## Highlights

- Hybrid extraction per page:
	- Uses native PDF text (`PyMuPDF`) when meaningful text exists.
	- Falls back to OCR (`pytesseract`) for scanned/low-text pages.
- Character offset bridge:
	- Builds a running character-to-bounding-box map during extraction.
	- Maps Presidio offsets back to exact PDF geometry.
- Structural noise reduction:
	- For JSON-like text, Presidio focuses on dictionary values.
	- Ignores noisy keys like `id`, `filename`, `metadata.item`, `input.ke`.
- NER accuracy and post-processing:
	- Presidio uses spaCy transformer model (`en_core_web_trf`).
	- Reclassifies likely business names from `PERSON` to `ORGANIZATION`.
- LLM robustness:
	- OpenRouter-only integration (no LiteLLM).
	- LLM returns quote+confidence objects; localization uses fuzzy matching (`rapidfuzz`) with threshold >= 92.
- Security-grade redaction:
	- Uses `add_redact_annot(...)` + `apply_redactions(images=PDF_REDACT_IMAGE_NONE)`.
	- Removes underlying content instead of visual-only overlay.
- Async processing:
	- Queue-based job flow with polling and file download endpoint.

## Project Layout

- backend/main.py: FastAPI API, queue worker, sanitization pipeline.
- frontend/index.html: Dark technical dashboard shell and component structure.
- frontend/app.js: Frontend state machine, API integration, polling, and rendering.
- docs/presidio-char-index-mapping.md: mapping math notes.

## Setup

1. Create and activate a Python environment.
2. Install dependencies.

```bash
pip install -r requirements.txt
```

3. Install external OCR/model requirements.

```bash
python -m spacy download en_core_web_trf
```

- Install Tesseract OCR and ensure it is on system PATH.

## Environment Variables

The backend auto-loads `.env` from either location:

- `backend/.env`
- repository root `.env`

Use `.env.example` as the template, then copy values into your local `.env`.

Example `.env`:

```env
OPENROUTER_API_KEY=your_openrouter_key_without_bearer
OPENROUTER_API_BASE=https://openrouter.ai/api/v1
LLM_MODEL=openai/gpt-oss-safeguard-20b

# optional OpenRouter metadata headers
OPENROUTER_SITE_URL=https://your-site.example
OPENROUTER_SITE_NAME=snv-PII-redact
```

Important:

- Use the raw key value (do not prefix with `Bearer`).
- `OPENAI_API_KEY` is accepted as a fallback if `OPENROUTER_API_KEY` is not set.
- Legacy metadata names `OPENROUTER_HTTP_REFERER`, `OPENROUTER_X_OPENROUTER_TITLE`, and `OPENROUTER_X_TITLE` are still accepted.

Useful tuning knobs:

- `MAX_UPLOAD_MB` (default `30`)
- `MAX_JOB_QUEUE_SIZE` (default `32`)
- `JOB_TTL_SECONDS` (default `1800`)
- `MIN_ENTITY_CONFIDENCE` (default `0.7`)
- `FUZZY_MATCH_THRESHOLD` (default `92`)
- `LLM_REQUEST_TIMEOUT_SECONDS` (default `60`)

## Run Backend

From repository root:

```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

If you run inside `backend/`, use:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Run Frontend

The frontend is served directly by FastAPI. Start the backend, then open the app in your browser:

```bash
http://localhost:8000
```

## API Contract

### POST `/api/v1/sanitize`

- Request: `multipart/form-data` with `file` field (`.pdf`).
- Response: queued job metadata.

```json
{
	"job_id": "<job_id>",
	"status": "queued",
	"status_url": "/api/v1/jobs/<job_id>",
	"download_url": "/api/v1/download/<job_id>"
}


### GET `/api/v1/jobs/{job_id}`

- Returns one of: `queued`, `processing`, `completed`, `failed`.
- On completion, includes `detected_entities`, `warnings`, and `download_url`.

### GET `/api/v1/download/{job_id}`

- Returns the redacted PDF file when job status is `completed`.