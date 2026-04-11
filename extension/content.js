(() => {
  const SANITIZE_ENDPOINT = "http://localhost:8000/api/v1/sanitize";
  const JOB_POLL_INTERVAL_MS = 1000;
  const JOB_POLL_TIMEOUT_MS = 180000;
  const INTERNAL_REDISPATCH = new WeakSet();

  const modalState = {
    input: null,
    files: [],
    pdfIndex: -1,
    host: null,
    shadowRoot: null,
    statusEl: null,
    primaryButton: null,
    secondaryButton: null,
  };

  document.addEventListener("change", onFileInputChanged, true);

  function onFileInputChanged(event) {
    const input = event.target;
    if (!(input instanceof HTMLInputElement) || input.type !== "file") {
      return;
    }

    if (INTERNAL_REDISPATCH.has(input)) {
      INTERNAL_REDISPATCH.delete(input);
      return;
    }

    const selectedFiles = Array.from(input.files || []);
    if (selectedFiles.length === 0) {
      return;
    }

    const pdfIndex = selectedFiles.findIndex((file) => isPdfFile(file));
    if (pdfIndex === -1) {
      return;
    }

    event.preventDefault();
    event.stopPropagation();
    if (typeof event.stopImmediatePropagation === "function") {
      event.stopImmediatePropagation();
    }

    openSanitizationModal({
      input,
      files: selectedFiles,
      pdfIndex,
    });
  }

  function openSanitizationModal(payload) {
    closeSanitizationModal();

    modalState.input = payload.input;
    modalState.files = payload.files;
    modalState.pdfIndex = payload.pdfIndex;

    const host = document.createElement("div");
    host.setAttribute("data-snv-dlp-host", "true");
    host.style.position = "fixed";
    host.style.inset = "0";
    host.style.zIndex = "2147483647";
    host.style.pointerEvents = "none";

    const shadowRoot = host.attachShadow({ mode: "open" });
    const stylesheetLink = document.createElement("link");
    stylesheetLink.rel = "stylesheet";
    stylesheetLink.href = resolveStylesheetUrl();

    const container = document.createElement("div");
    container.className = "snv-dlp-overlay";
    container.innerHTML = `
      <section class="snv-dlp-modal" role="dialog" aria-modal="true" aria-label="Document sanitization prompt">
        <h2 class="snv-dlp-title">Wait! This document may contain sensitive data.</h2>
        <p class="snv-dlp-subtitle">Would you like to sanitize it before uploading?</p>
        <div class="snv-dlp-actions">
          <button type="button" class="snv-dlp-button snv-dlp-button-secondary" data-action="original">Upload Original</button>
          <button type="button" class="snv-dlp-button snv-dlp-button-primary" data-action="sanitize">Sanitize Document</button>
        </div>
        <p class="snv-dlp-status" data-role="status" aria-live="polite"></p>
      </section>
    `;

    shadowRoot.appendChild(stylesheetLink);
    shadowRoot.appendChild(container);

    modalState.host = host;
    modalState.shadowRoot = shadowRoot;
    modalState.statusEl = container.querySelector('[data-role="status"]');
    modalState.secondaryButton = container.querySelector('[data-action="original"]');
    modalState.primaryButton = container.querySelector('[data-action="sanitize"]');

    modalState.secondaryButton.addEventListener("click", () => {
      replayOriginalSelection();
    });

    modalState.primaryButton.addEventListener("click", () => {
      void sanitizeAndReplaceSelection();
    });

    const mountPoint = document.documentElement || document.body;
    if (!mountPoint) {
      throw new Error("Unable to mount sanitization modal because no document root was found.");
    }

    mountPoint.appendChild(host);
    host.style.pointerEvents = "auto";
  }

  function closeSanitizationModal() {
    if (modalState.host && modalState.host.parentNode) {
      modalState.host.parentNode.removeChild(modalState.host);
    }

    modalState.input = null;
    modalState.files = [];
    modalState.pdfIndex = -1;
    modalState.host = null;
    modalState.shadowRoot = null;
    modalState.statusEl = null;
    modalState.primaryButton = null;
    modalState.secondaryButton = null;
  }

  function replayOriginalSelection() {
    if (!modalState.input || modalState.files.length === 0) {
      closeSanitizationModal();
      return;
    }

    applyFilesToInput(modalState.input, modalState.files);
    closeSanitizationModal();
  }

  async function sanitizeAndReplaceSelection() {
    if (!modalState.input || modalState.pdfIndex < 0) {
      return;
    }

    const targetInput = modalState.input;
    const originalFiles = [...modalState.files];
    const originalPdf = originalFiles[modalState.pdfIndex];
    if (!originalPdf) {
      setStatus("Unable to find the selected PDF.", true);
      return;
    }

    setBusyState(true);
    setStatus("Sanitizing document...", false);

    try {
      const responsePayload = await sanitizePdf(originalPdf, (progressMessage) => {
        setStatus(progressMessage, false);
      });
      const sanitizedFile = buildSanitizedFile(originalPdf.name, responsePayload.redacted_pdf_base64);

      const swappedFiles = [...originalFiles];
      swappedFiles[modalState.pdfIndex] = sanitizedFile;

      applyFilesToInput(targetInput, swappedFiles);

      const entityCount = Array.isArray(responsePayload.entities)
        ? responsePayload.entities.length
        : 0;
      setStatus(
        entityCount > 0
          ? `Sanitized successfully (${entityCount} entities redacted).`
          : "Sanitized successfully.",
        false,
      );

      window.setTimeout(() => {
        closeSanitizationModal();
      }, 260);
    } catch (error) {
      setBusyState(false);
      setStatus(readErrorMessage(error), true);
    }
  }

  async function sanitizePdf(file, onProgress) {
    const formData = new FormData();
    formData.append("file", file, file.name);

    const response = await fetch(SANITIZE_ENDPOINT, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      throw new Error(await readApiError(response));
    }

    const payload = await readJsonSafe(response);
    const inlineBase64 = extractRedactedPdfBase64(payload);
    if (inlineBase64) {
      return {
        redacted_pdf_base64: inlineBase64,
        entities: extractEntities(payload),
      };
    }

    if (isQueuedJobPayload(payload)) {
      return await resolveQueuedSanitization(payload, onProgress);
    }

    throw new Error(
      "Sanitization response did not include redacted_pdf_base64 or queued job metadata.",
    );
  }

  function extractRedactedPdfBase64(payload) {
    if (!payload || typeof payload !== "object") {
      return "";
    }

    const candidates = [
      payload.redacted_pdf_base64,
      payload.redactedPdfBase64,
      payload.sanitized_pdf_base64,
      payload.pdf_base64,
    ];

    const match = candidates.find((candidate) => typeof candidate === "string" && candidate.trim());
    return typeof match === "string" ? match : "";
  }

  function extractEntities(payload) {
    if (!payload || typeof payload !== "object") {
      return [];
    }

    if (Array.isArray(payload.entities)) {
      return payload.entities;
    }
    if (Array.isArray(payload.detected_entities)) {
      return payload.detected_entities;
    }
    return [];
  }

  function isQueuedJobPayload(payload) {
    if (!payload || typeof payload !== "object") {
      return false;
    }
    return (
      typeof payload.job_id === "string" ||
      typeof payload.status_url === "string" ||
      typeof payload.download_url === "string"
    );
  }

  async function resolveQueuedSanitization(createPayload, onProgress) {
    const startedAt = Date.now();
    const statusUrl = resolveApiUrl(
      createPayload.status_url ||
        (createPayload.job_id ? `/api/v1/jobs/${createPayload.job_id}` : ""),
    );

    if (!statusUrl) {
      throw new Error("Queued sanitization response did not include a usable status URL.");
    }

    if (typeof onProgress === "function") {
      onProgress("Sanitization job queued...");
    }

    while (Date.now() - startedAt < JOB_POLL_TIMEOUT_MS) {
      const statusResponse = await fetch(statusUrl, { method: "GET" });
      if (!statusResponse.ok) {
        throw new Error(await readApiError(statusResponse));
      }

      const statusPayload = await readJsonSafe(statusResponse);
      const state = String(statusPayload && statusPayload.status ? statusPayload.status : "")
        .trim()
        .toLowerCase();

      if (state === "failed") {
        const failureMessage =
          (statusPayload && statusPayload.error) ||
          (statusPayload && statusPayload.status_message) ||
          "Sanitization job failed.";
        throw new Error(String(failureMessage));
      }

      if (state === "completed") {
        const downloadUrl = resolveApiUrl(
          (statusPayload && statusPayload.download_url) ||
            createPayload.download_url ||
            (createPayload.job_id ? `/api/v1/download/${createPayload.job_id}` : ""),
        );

        if (!downloadUrl) {
          throw new Error("Sanitization job completed but no download URL was provided.");
        }

        if (typeof onProgress === "function") {
          onProgress("Downloading sanitized document...");
        }

        const redactedPdfBase64 = await downloadPdfAsBase64(downloadUrl);
        return {
          redacted_pdf_base64: redactedPdfBase64,
          entities: extractEntities(statusPayload),
        };
      }

      if (typeof onProgress === "function") {
        const progress =
          statusPayload && typeof statusPayload.progress === "number"
            ? Math.max(0, Math.min(100, Math.round(statusPayload.progress * 100)))
            : null;
        const statusMessage =
          statusPayload && typeof statusPayload.status_message === "string"
            ? statusPayload.status_message.trim()
            : "";

        if (typeof progress === "number") {
          onProgress(`Sanitizing document... ${progress}%`);
        } else if (statusMessage) {
          onProgress(`Sanitizing document... ${statusMessage}`);
        } else {
          onProgress("Sanitizing document...");
        }
      }

      await wait(JOB_POLL_INTERVAL_MS);
    }

    throw new Error("Sanitization job timed out while waiting for completion.");
  }

  async function downloadPdfAsBase64(downloadUrl) {
    const response = await fetch(downloadUrl, { method: "GET" });
    if (!response.ok) {
      throw new Error(await readApiError(response));
    }

    const blob = await response.blob();
    return await blobToBase64(blob);
  }

  function blobToBase64(blob) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onloadend = () => {
        const result = String(reader.result || "");
        const marker = "base64,";
        const markerIndex = result.indexOf(marker);
        if (markerIndex === -1) {
          reject(new Error("Unable to convert sanitized PDF to base64."));
          return;
        }
        resolve(result.slice(markerIndex + marker.length));
      };
      reader.onerror = () => {
        reject(new Error("Unable to read sanitized PDF data."));
      };
      reader.readAsDataURL(blob);
    });
  }

  function resolveApiUrl(pathOrUrl) {
    const value = String(pathOrUrl || "").trim();
    if (!value) {
      return "";
    }

    try {
      return new URL(value, SANITIZE_ENDPOINT).toString();
    } catch (_) {
      return "";
    }
  }

  function wait(milliseconds) {
    return new Promise((resolve) => {
      window.setTimeout(resolve, milliseconds);
    });
  }

  async function readJsonSafe(response) {
    try {
      return await response.json();
    } catch (_) {
      return null;
    }
  }

  async function readApiError(response) {
    const fallback = `Sanitization API failed with HTTP ${response.status}.`;

    try {
      const payload = await response.json();
      if (payload && typeof payload.detail === "string" && payload.detail.trim()) {
        return payload.detail.trim();
      }
      if (payload && typeof payload.error === "string" && payload.error.trim()) {
        return payload.error.trim();
      }
      return fallback;
    } catch (_) {
      return fallback;
    }
  }

  function buildSanitizedFile(originalName, rawBase64) {
    const normalizedBase64 = normalizeBase64Payload(rawBase64);
    const binary = atob(normalizedBase64);
    const bytes = new Uint8Array(binary.length);

    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }

    const blob = new Blob([bytes], { type: "application/pdf" });
    const sanitizedName = createSanitizedFileName(originalName);
    return new File([blob], sanitizedName, {
      type: "application/pdf",
      lastModified: Date.now(),
    });
  }

  function normalizeBase64Payload(rawBase64) {
    const text = String(rawBase64 || "").trim();
    if (!text) {
      throw new Error("Redacted PDF payload was empty.");
    }

    const marker = "base64,";
    const markerIndex = text.indexOf(marker);
    if (markerIndex !== -1) {
      return text.slice(markerIndex + marker.length);
    }
    return text;
  }

  function createSanitizedFileName(originalName) {
    const fallback = "document_sanitized.pdf";
    const cleanName = String(originalName || "").trim();
    if (!cleanName) {
      return fallback;
    }

    const lowerName = cleanName.toLowerCase();
    if (lowerName.endsWith(".pdf")) {
      return `${cleanName.slice(0, -4)}_sanitized.pdf`;
    }
    return `${cleanName}_sanitized.pdf`;
  }

  function applyFilesToInput(input, files) {
    const transfer = new DataTransfer();
    files.forEach((file) => transfer.items.add(file));

    INTERNAL_REDISPATCH.add(input);
    input.files = transfer.files;
    input.dispatchEvent(new Event("input", { bubbles: true, composed: true }));
    input.dispatchEvent(new Event("change", { bubbles: true, composed: true }));
  }

  function isPdfFile(file) {
    const mime = String(file.type || "").toLowerCase();
    const fileName = String(file.name || "").toLowerCase();
    return mime === "application/pdf" || fileName.endsWith(".pdf");
  }

  function setBusyState(isBusy) {
    if (!modalState.primaryButton || !modalState.secondaryButton) {
      return;
    }

    modalState.primaryButton.disabled = isBusy;
    modalState.secondaryButton.disabled = isBusy;
    modalState.primaryButton.textContent = isBusy ? "Sanitizing..." : "Sanitize Document";
    modalState.secondaryButton.textContent = "Upload Original";
  }

  function setStatus(message, isError) {
    if (!modalState.statusEl) {
      return;
    }

    modalState.statusEl.textContent = String(message || "");
    modalState.statusEl.classList.toggle("snv-dlp-status-error", Boolean(isError));
  }

  function resolveStylesheetUrl() {
    if (typeof chrome !== "undefined" && chrome.runtime && chrome.runtime.getURL) {
      return chrome.runtime.getURL("styles.css");
    }
    if (typeof browser !== "undefined" && browser.runtime && browser.runtime.getURL) {
      return browser.runtime.getURL("styles.css");
    }
    return "";
  }

  function readErrorMessage(error) {
    if (!error) {
      return "Unknown error occurred while sanitizing the document.";
    }
    if (typeof error === "string") {
      return error;
    }
    if (error instanceof Error && error.message) {
      return error.message;
    }
    return "Unknown error occurred while sanitizing the document.";
  }
})();
