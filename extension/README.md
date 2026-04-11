# SNV DLP Tripwire (MVP2)

Manifest V3 extension that intercepts PDF uploads, pauses host-page handling, prompts user intent, sanitizes through backend, and replays the file input with a sanitized PDF.

## Core Files

- manifest.json
- content.js
- styles.css
- background.js
- DATATRANSFER_EXPLANATION.md

## Backend Contract Used

- POST http://localhost:8000/api/v1/sanitize
- Request: multipart/form-data with `file`
- Response: supported in either of these formats

Inline (synchronous):

```json
{
  "entities": [],
  "redacted_pdf_base64": "..."
}
```

Queued (asynchronous):

```json
{
  "job_id": "<job_id>",
  "status": "queued",
  "status_url": "/api/v1/jobs/<job_id>",
  "download_url": "/api/v1/download/<job_id>"
}
```

## Runtime Flow

1. Capture `change` events on file inputs.
2. If any selected file is a PDF, pause propagation.
3. Render shadow-DOM modal prompt.
4. On **Upload Original**, replay original FileList.
5. On **Sanitize Document**, call backend.
6. If response is queued, poll status endpoint and download the sanitized PDF on completion.
7. Replace only the PDF file via DataTransfer and dispatch synthetic `change`.

## Load in Chrome

1. Open chrome://extensions.
2. Enable Developer mode.
3. Click Load unpacked.
4. Select the extension folder.
