# snv-PII-redact

MVP1 document sanitization pipeline with:

- FastAPI backend for OCR, PII detection, LLM contextual extraction, and secure PDF redaction
- Streamlit frontend for upload, entity inspection, and redacted PDF preview/download

## Project Layout

- backend/main.py: FastAPI sanitize endpoint and full pipeline
- frontend/app.py: Streamlit X-Ray dashboard
- docs/presidio-char-index-mapping.md: span-to-bbox mapping math

## Setup

1. Create and activate a Python environment.
2. Install dependencies:

	pip install -r requirements.txt

3. Install external OCR/model requirements:

	- Install Tesseract OCR and ensure the binary is on PATH.
	- Install spaCy English model:

	  python -m spacy download en_core_web_sm

4. Optional LLM configuration (OpenRouter via LiteLLM):

	- OPENROUTER_API_KEY
	- LLM_MODEL (default: openrouter/openai/gpt-4o-mini)
	- OPENROUTER_API_BASE (default: https://openrouter.ai/api/v1)

## Run Backend

uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload

## Run Frontend

streamlit run frontend/app.py

## API

POST /api/v1/sanitize

- multipart/form-data with file field named file
- returns detected_entities and redacted_pdf_base64