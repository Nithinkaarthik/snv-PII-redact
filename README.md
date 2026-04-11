# snv-PII-redact

Robust document sanitization pipeline with a decoupled architecture:

- FastAPI backend for OCR, PII/context extraction, and forensic PDF redaction.
- Tailwind + vanilla JavaScript frontend for upload, queue monitoring, entity inspection, and sanitized download.

## Highlights

- Hybrid extraction per page:
	- Uses native PDF text (`PyMuPDF`) when meaningful text exists.
	- Falls back to OCR (`pytesseract`) for scanned/low-text pages.
	- Detects table-like layouts from OCR geometry and preserves row/column structure.
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
- `OCR_RENDER_SCALE` (default `1.5`)
- `OCR_MIN_CONFIDENCE` (default `20`)
- `OCR_THRESHOLD_VALUE` (default `164`)
- `OCR_STRONG_WORD_THRESHOLD` (default `16`)
- `OCR_STRONG_AVG_CONF` (default `46`)
- `OCR_HIGH_ACCURACY_MODE` (default `0`)
- `OCR_HIGH_ACCURACY_SCALE_MULT` (default `1.35`)
- `OCR_HIGH_ACCURACY_MIN_CONFIDENCE` (default `10`)
- `OCR_HIGH_ACCURACY_STRONG_WORD_THRESHOLD` (default `26`)
- `OCR_HIGH_ACCURACY_STRONG_AVG_CONF` (default `58`)
- `TABLE_PARSER_ENABLED` (default `1`)
- `TABLE_MIN_ROWS` (default `2`)
- `TABLE_MIN_COLS` (default `2`)
- `TABLE_MIN_CONFIDENCE` (default `0.58`)
- `TABLE_ROW_Y_TOLERANCE_PT` (default `4.0`)
- `TABLE_COLUMN_GAP_MIN_PT` (default `14.0`)
- `TABLE_MAX_COLUMN_DRIFT_PT` (default `14.0`)
- `TABLE_CONTINUATION_MAX_Y_GAP_MULT` (default `1.9`)
- `REDACTION_BOX_TIGHTEN_ENABLED` (default `1`)
- `REDACTION_VERTICAL_INSET_RATIO` (default `0.18`)
- `REDACTION_VERTICAL_INSET_MAX_PT` (default `2.2`)
- `REDACTION_HORIZONTAL_INSET_RATIO` (default `0.0`)
- `REDACTION_HORIZONTAL_INSET_MAX_PT` (default `0.0`)
- `REDACTION_DYNAMIC_INSET_ENABLED` (default `1`)
- `REDACTION_MIN_SAFE_GAP_PT` (default `0.3`)

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

## Browser Extension MVP

The repository now includes a browser extension under `extension/` that:

- Intercepts file input changes containing PDF uploads.
- Sends PDFs to this backend (`/api/v1/sanitize` -> `/api/v1/jobs/{job_id}` -> `/api/v1/download/{job_id}`).
- Replaces the selected upload file with the sanitized PDF.
- Does not auto-submit forms.
- Runs only on domains in an explicit allowlist.
- Provides a popup dashboard to inspect extension errors and backend health.

Default extension backend URL: `http://127.0.0.1:8000`

Load in Chrome:

1. Open `chrome://extensions`.
2. Enable Developer mode.
3. Click **Load unpacked** and select the `extension/` folder.

Load in Firefox:

1. Open `about:debugging#/runtime/this-firefox`.
2. Click **Load Temporary Add-on**.
3. Select `extension/manifest.json`.

Then open Extension Options and add your allowlisted domains (one per line).

To inspect extension errors:

1. Click the extension icon in your browser toolbar.
2. Use the popup dashboard to view recent errors.
3. Use **Test Backend**, **Refresh**, **Copy Latest**, and **Clear Errors** as needed.

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
```


### GET `/api/v1/jobs/{job_id}`

- Returns one of: `queued`, `processing`, `completed`, `failed`.
- On completion, includes `detected_entities`, `warnings`, and `download_url`.

### GET `/api/v1/download/{job_id}`

- Returns the redacted PDF file when job status is `completed`.

## Benchmarking (30 PDFs + 30 Dataset Rows)

The repository now includes benchmark harness scripts under `benchmark/`:

- `benchmark/performance_benchmark.py`:
	- Runs performance metrics on local PDFs (for example, the 30 files under `pdfs/`).
	- Captures queue wait, processing latency, total latency, p50/p90/p95, throughput, and failure rate.
- `benchmark/accuracy_benchmark.py`:
	- Loads `ai4privacy/pii-masking-300k`, samples English rows deterministically, and computes accuracy metrics.
	- Reports both:
		- all-label metrics (strictly includes all dataset labels), and
		- capability-slice metrics (diagnostic view for labels the current detector targets).
- `benchmark/run_benchmark.py`:
	- Runs both tracks in one command and writes a consolidated summary.
	- Automatically generates `benchmark_report.html` inside each combined run directory.
- `benchmark/html_report.py`:
	- Generates an HTML report from existing benchmark artifacts.
	- Can be used standalone when you already have `combined_summary.json`.

Install dependencies first:

```bash
pip install -r requirements.txt
```

### Performance Benchmark (Local 30 PDFs)

```bash
python -m benchmark.performance_benchmark \
	--api-base http://127.0.0.1:8000 \
	--pdf-dir pdfs \
	--max-pdfs 30 \
	--poll-interval 2.0 \
	--output-dir benchmark_outputs/performance
```

### Accuracy Benchmark (30 AI4Privacy Rows)

```bash
python -m benchmark.accuracy_benchmark \
	--dataset ai4privacy/pii-masking-300k \
	--split train \
	--sample-size 30 \
	--seed 42 \
	--iou-threshold 0.5 \
	--output-dir benchmark_outputs/accuracy
```

### Combined Benchmark Run

```bash
python -m benchmark.run_benchmark \
	--api-base http://127.0.0.1:8000 \
	--pdf-dir pdfs \
	--max-pdfs 30 \
	--dataset ai4privacy/pii-masking-300k \
	--dataset-split train \
	--sample-size 30 \
	--seed 42 \
	--iou-threshold 0.5 \
	--output-root benchmark_outputs
```

Artifacts are written under `benchmark_outputs/` with JSON and CSV summaries for audit and regression tracking.

### Standalone HTML Report Generation

Use this when a combined run already exists and you want to regenerate the HTML report:

```bash
python -m benchmark.html_report \
	--run-dir benchmark_outputs/<RUN_TIMESTAMP> \
	--output-name benchmark_report.html
```