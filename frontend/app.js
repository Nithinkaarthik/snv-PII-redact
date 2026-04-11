const POLL_INTERVAL_MS = 2000;
const SANITIZE_SUFFIX = "/api/v1/sanitize";
const FALLBACK_API_URL = `${window.location.origin}${SANITIZE_SUFFIX}`;

const STATUS_PROGRESS = {
  idle: 0,
  queued: 24,
  processing: 68,
  completed: 100,
  failed: 100,
};

const HIGH_RISK_ENTITY_TYPES = new Set([
  "CREDIT_CARD",
  "CRYPTO",
  "IBAN_CODE",
  "IP_ADDRESS",
  "NRP",
  "PASSWORD",
  "US_BANK_NUMBER",
  "US_DRIVER_LICENSE",
  "US_ITIN",
  "US_PASSPORT",
  "US_SSN",
  "ACCOUNT_NUMBER",
  "BANK_ACCOUNT_NUMBER",
  "FINANCIAL_ACCOUNT",
  "CUSTOMER_IDENTIFIER",
  "SECURITY_CODE",
]);

const state = {
  apiUrl: FALLBACK_API_URL,
  selectedFile: null,
  jobInfo: null,
  jobStatus: null,
  selectedEntityIndex: null,
  pollHandle: null,
  redactedBlob: null,
  redactedUrl: "",
  redactedFileName: "",
  lastError: "",
  lastUpdatedIso: "",
  notifications: [],
  notificationCounter: 0,
  activeTopPanel: null,
};

const ui = {
  uploadZone: document.getElementById("uploadZone"),
  uploadIcon: document.getElementById("uploadIcon"),
  uploadPromptText: document.getElementById("uploadPromptText"),
  uploadHintText: document.getElementById("uploadHintText"),
  pdfInput: document.getElementById("pdfInput"),
  browseBtn: document.getElementById("browseBtn"),
  securityBtn: document.getElementById("securityBtn"),
  notificationsBtn: document.getElementById("notificationsBtn"),
  notificationsBadge: document.getElementById("notificationsBadge"),
  securityPanel: document.getElementById("securityPanel"),
  notificationsPanel: document.getElementById("notificationsPanel"),
  notificationsMarkRead: document.getElementById("notificationsMarkRead"),
  notificationsList: document.getElementById("notificationsList"),
  securityStatusValue: document.getElementById("securityStatusValue"),
  securityJobValue: document.getElementById("securityJobValue"),
  securityEntityValue: document.getElementById("securityEntityValue"),
  securityWarningValue: document.getElementById("securityWarningValue"),
  securityUpdatedValue: document.getElementById("securityUpdatedValue"),
  topPdfProgressFill: document.getElementById("topPdfProgressFill"),
  topPdfProgressLabel: document.getElementById("topPdfProgressLabel"),
  refreshBtn: document.getElementById("refreshBtn"),
  manualRefreshBtn: document.getElementById("manualRefreshBtn"),
  downloadBtn: document.getElementById("downloadBtn"),
  resetBtn: document.getElementById("resetBtn"),

  stepUpload: document.getElementById("stepUpload"),
  stepQueue: document.getElementById("stepQueue"),
  stepReview: document.getElementById("stepReview"),
  nextActionHint: document.getElementById("nextActionHint"),

  fileNameValue: document.getElementById("fileNameValue"),
  fileMetaValue: document.getElementById("fileMetaValue"),
  queueStatusTitle: document.getElementById("queueStatusTitle"),
  queueStatusDetail: document.getElementById("queueStatusDetail"),

  metricEntities: document.getElementById("metricEntities"),
  metricWarnings: document.getElementById("metricWarnings"),
  metricProgress: document.getElementById("metricProgress"),
  primaryProgressFill: document.getElementById("primaryProgressFill"),
  secondaryProgressFill: document.getElementById("secondaryProgressFill"),
  statusProgressFill: document.getElementById("statusProgressFill"),
  liveValue: document.getElementById("liveValue"),

  pipelineBadgeTitle: document.getElementById("pipelineBadgeTitle"),
  pipelineBadgeDetail: document.getElementById("pipelineBadgeDetail"),
  alertPanel: document.getElementById("alertPanel"),
  alertTitle: document.getElementById("alertTitle"),
  alertDetail: document.getElementById("alertDetail"),

  jobIdBadge: document.getElementById("jobIdBadge"),
  jobIdTableBadge: document.getElementById("jobIdTableBadge"),
  jobIdOutputBadge: document.getElementById("jobIdOutputBadge"),

  entitiesTableBody: document.getElementById("entitiesTableBody"),
  warningsList: document.getElementById("warningsList"),
  selectedEntityCard: document.getElementById("selectedEntityCard"),
  selectedEntityText: document.getElementById("selectedEntityText"),
  selectedEntityType: document.getElementById("selectedEntityType"),
  selectedEntitySource: document.getElementById("selectedEntitySource"),
  selectedEntityConfidence: document.getElementById("selectedEntityConfidence"),
  selectedEntityPages: document.getElementById("selectedEntityPages"),

  statusValue: document.getElementById("statusValue"),
  statusBadgePill: document.getElementById("statusBadgePill"),

  previewEmptyState: document.getElementById("previewEmptyState"),
  pdfPreview: document.getElementById("pdfPreview"),
  outputFileValue: document.getElementById("outputFileValue"),
  outputSizeValue: document.getElementById("outputSizeValue"),
  outputStateValue: document.getElementById("outputStateValue"),
};

function init() {
  ui.securityBtn?.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleTopPanel("security");
  });
  ui.notificationsBtn?.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleTopPanel("notifications");
  });
  ui.notificationsMarkRead?.addEventListener("click", (event) => {
    event.stopPropagation();
    markAllNotificationsRead();
  });
  ui.securityPanel?.addEventListener("click", (event) => event.stopPropagation());
  ui.notificationsPanel?.addEventListener("click", (event) => event.stopPropagation());
  document.addEventListener("click", () => closeTopPanels());

  ui.uploadZone.addEventListener("click", () => ui.pdfInput.click());
  ui.browseBtn.addEventListener("click", (event) => {
    event.stopPropagation();
    ui.pdfInput.click();
  });

  ui.pdfInput.addEventListener("change", async (event) => {
    const [file] = event.target.files;
    await setSelectedFile(file || null);
  });

  ui.uploadZone.addEventListener("dragover", (event) => {
    event.preventDefault();
    ui.uploadZone.classList.add("border-primary/40", "bg-surface-container-high");
  });

  ui.uploadZone.addEventListener("dragleave", () => {
    ui.uploadZone.classList.remove("border-primary/40", "bg-surface-container-high");
  });

  ui.uploadZone.addEventListener("drop", async (event) => {
    event.preventDefault();
    ui.uploadZone.classList.remove("border-primary/40", "bg-surface-container-high");
    const [file] = event.dataTransfer.files;
    await setSelectedFile(file || null);
  });

  ui.refreshBtn.addEventListener("click", () => refreshStatus(false));
  ui.manualRefreshBtn.addEventListener("click", () => refreshStatus(false));
  ui.resetBtn.addEventListener("click", resetWorkspace);

  ui.downloadBtn.addEventListener("click", downloadPdf);

  window.addEventListener("beforeunload", () => {
    stopPolling();
    if (state.redactedUrl) {
      URL.revokeObjectURL(state.redactedUrl);
    }
  });

  pushNotification("info", "Console ready", "Upload a PDF to start sanitization.");

  render();
}

async function setSelectedFile(file) {
  if (!file) {
    state.selectedFile = null;
    render();
    return;
  }

  const fileName = file.name || "document.pdf";
  if (!fileName.toLowerCase().endsWith(".pdf")) {
    setAlert("Upload blocked", "Only .pdf files are accepted.");
    state.selectedFile = null;
    render();
    return;
  }

  state.selectedFile = file;
  stopPolling();
  state.jobInfo = null;
  state.jobStatus = null;
  state.selectedEntityIndex = null;
  state.lastUpdatedIso = new Date().toISOString();
  clearRedactedBlob();
  clearAlert();
  pushNotification("info", "Document selected", `${file.name} • ${formatBytes(file.size)}`);
  render();

  await enqueueJob();
}

async function enqueueJob() {
  if (!state.selectedFile) {
    setAlert("Missing file", "Select a PDF to start sanitization.");
    return;
  }

  clearAlert();

  try {
    const payload = new FormData();
    payload.append("file", state.selectedFile, state.selectedFile.name);
    pushNotification("info", "Sanitization started", "Uploading document and creating job.");

    const response = await fetch(state.apiUrl, {
      method: "POST",
      body: payload,
    });
    const data = await readJsonOrThrow(response);

    state.jobInfo = {
      ...data,
      status_url: resolveApiPath(state.apiUrl, data.status_url),
      download_url: resolveApiPath(state.apiUrl, data.download_url),
    };
    state.jobStatus = null;
    state.selectedEntityIndex = null;
    state.lastUpdatedIso = new Date().toISOString();

    clearRedactedBlob();
    clearAlert();
    pushNotification("success", "Job queued", `Tracking ID ${shortJobId(state.jobInfo.job_id)}.`);

    await refreshStatus(true);
    const currentStatus = getCurrentStatus();
    if (currentStatus === "queued" || currentStatus === "processing") {
      schedulePolling();
    } else {
      stopPolling();
    }
  } catch (error) {
    pushNotification("error", "Queue request failed", readErrorMessage(error));
    setAlert("Queue request failed", readErrorMessage(error));
  } finally {
    render();
  }
}

async function refreshStatus(isAutoRefresh) {
  if (!state.jobInfo || !state.jobInfo.status_url) {
    if (!isAutoRefresh) {
      setAlert("No active job", "Upload a document first to poll status.");
    }
    render();
    return;
  }

  try {
    const previousStatus = getCurrentStatus();
    const response = await fetch(state.jobInfo.status_url, { method: "GET" });
    const data = await readJsonOrThrow(response);

    state.jobStatus = {
      ...data,
      download_url: resolveApiPath(
        state.apiUrl,
        data.download_url || (state.jobInfo ? state.jobInfo.download_url : null)
      ),
    };
    state.lastUpdatedIso = new Date().toISOString();

    if (state.jobStatus.status !== previousStatus) {
      notifyStatusTransition(state.jobStatus.status);
    }

    if (state.jobStatus.status === "completed") {
      stopPolling();
      await fetchRedactedPdf();
      const warningCount = Array.isArray(state.jobStatus.warnings) ? state.jobStatus.warnings.length : 0;
      if (warningCount > 0) {
        pushNotification("warning", "Completed with warnings", `${warningCount} warning(s) reported by backend.`);
      }
    } else if (state.jobStatus.status === "failed") {
      stopPolling();
      clearRedactedBlob();
      if (state.jobStatus.error) {
        setAlert("Pipeline failure", state.jobStatus.error);
      }
    } else if (state.jobStatus.status === "queued" || state.jobStatus.status === "processing") {
      schedulePolling();
    }

    clearAlertIfRecovered();
  } catch (error) {
    if (!isAutoRefresh) {
      setAlert("Status sync failed", readErrorMessage(error));
    }
  } finally {
    render();
  }
}

async function fetchRedactedPdf() {
  const candidateUrl =
    (state.jobStatus && state.jobStatus.download_url) ||
    (state.jobInfo && state.jobInfo.download_url);

  if (!candidateUrl) {
    return;
  }

  const response = await fetch(candidateUrl, { method: "GET" });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const json = await response.json();
      detail = json.detail || detail;
    } catch (error) {
      detail = await response.text();
    }
    throw new Error(`Download failed (${response.status}): ${detail}`);
  }

  const blob = await response.blob();
  setRedactedBlob(blob, inferOutputFilename());
}

function setRedactedBlob(blob, filename) {
  clearRedactedBlob();
  state.redactedBlob = blob;
  state.redactedUrl = URL.createObjectURL(blob);
  state.redactedFileName = filename;
}

function clearRedactedBlob() {
  if (state.redactedUrl) {
    URL.revokeObjectURL(state.redactedUrl);
  }
  state.redactedBlob = null;
  state.redactedUrl = "";
  state.redactedFileName = "";
}

function resetWorkspace() {
  stopPolling();
  clearRedactedBlob();
  clearAlert();
  closeTopPanels(false);

  state.selectedFile = null;
  state.jobInfo = null;
  state.jobStatus = null;
  state.selectedEntityIndex = null;
  state.lastUpdatedIso = "";

  if (ui.pdfInput) {
    ui.pdfInput.value = "";
  }

  pushNotification("info", "Workspace reset", "Cleared active document and job state.");

  render();
}

function inferOutputFilename() {
  const fallback = "sanitized_document.pdf";
  if (!state.selectedFile || !state.selectedFile.name) {
    return fallback;
  }
  const stem = state.selectedFile.name.replace(/\.pdf$/i, "") || "document";
  return `sanitized_${stem}.pdf`;
}

function schedulePolling() {
  stopPolling();
  state.pollHandle = window.setInterval(() => {
    refreshStatus(true);
  }, POLL_INTERVAL_MS);
}

function stopPolling() {
  if (state.pollHandle) {
    window.clearInterval(state.pollHandle);
    state.pollHandle = null;
  }
}

function setAlert(title, message) {
  state.lastError = message || "Unknown error.";
  ui.alertTitle.textContent = title;
  ui.alertDetail.textContent = state.lastError;
  ui.alertPanel.classList.remove("hidden");
}

function clearAlert() {
  state.lastError = "";
  ui.alertPanel.classList.add("hidden");
}

function clearAlertIfRecovered() {
  if (state.jobStatus && state.jobStatus.status !== "failed") {
    clearAlert();
  }
}

function resolveApiPath(baseSanitizeUrl, endpoint) {
  if (!endpoint) {
    return null;
  }

  if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
    return endpoint;
  }

  let base = (baseSanitizeUrl || FALLBACK_API_URL).trim();
  if (base.endsWith(SANITIZE_SUFFIX)) {
    base = base.slice(0, -SANITIZE_SUFFIX.length);
  }

  try {
    const normalized = `${base.replace(/\/$/, "")}/`;
    return new URL(endpoint.replace(/^\//, ""), normalized).toString();
  } catch (error) {
    return endpoint;
  }
}

async function readJsonOrThrow(response) {
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const payload = await response.json();
      detail = payload.detail || JSON.stringify(payload);
    } catch (error) {
      detail = await response.text();
    }
    throw new Error(`Backend error ${response.status}: ${detail}`);
  }

  const data = await response.json();
  if (!data || typeof data !== "object") {
    throw new Error("Backend returned an invalid response shape.");
  }
  return data;
}

function readErrorMessage(error) {
  if (!error) {
    return "Unexpected error.";
  }
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function getCurrentStatus() {
  if (state.jobStatus && state.jobStatus.status) {
    return state.jobStatus.status;
  }
  if (state.jobInfo && state.jobInfo.status) {
    return state.jobInfo.status;
  }
  return "idle";
}

function getEntities() {
  if (!state.jobStatus || !Array.isArray(state.jobStatus.detected_entities)) {
    return [];
  }
  return state.jobStatus.detected_entities;
}

function getWarnings() {
  if (!state.jobStatus || !Array.isArray(state.jobStatus.warnings)) {
    return [];
  }
  return state.jobStatus.warnings;
}

function render() {
  const status = getCurrentStatus();
  const entities = getEntities();
  const warnings = getWarnings();
  const hasActiveJob = Boolean(state.jobInfo && state.jobInfo.job_id);
  const hasSelectedFile = Boolean(state.selectedFile);
  const hasPreview = Boolean(state.redactedUrl && state.redactedBlob);

  ui.uploadPromptText.textContent = hasSelectedFile
    ? "Document Uploaded"
    : "Drop a PDF or click to browse";
  ui.uploadHintText.textContent = hasSelectedFile
    ? "Click this box or drop another PDF to replace current file."
    : "Formats: PDF only • Max size enforced by backend";
  ui.uploadIcon.textContent = hasSelectedFile ? "task_alt" : "file_upload";
  ui.uploadIcon.style.fontVariationSettings = hasSelectedFile
    ? "'FILL' 1, 'wght' 500, 'GRAD' 0, 'opsz' 24"
    : "'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 24";
  ui.browseBtn.textContent = hasSelectedFile ? "Change PDF" : "Select File";
  ui.uploadZone.classList.toggle("upload-zone-selected", hasSelectedFile);

  if (hasSelectedFile) {
    ui.uploadZone.classList.add("border-primary/40", "bg-surface-container-high");
  } else {
    ui.uploadZone.classList.remove("border-primary/40", "bg-surface-container-high");
  }

  ui.fileNameValue.textContent = state.selectedFile ? state.selectedFile.name : "none";
  ui.fileMetaValue.textContent = state.selectedFile
    ? `${formatBytes(state.selectedFile.size)} • PDF document`
    : "No file loaded.";

  ui.refreshBtn.disabled = !hasActiveJob;
  ui.manualRefreshBtn.disabled = !hasActiveJob;

  const jobId = hasActiveJob ? state.jobInfo.job_id : "-";
  ui.jobIdBadge.textContent = jobId;
  ui.jobIdTableBadge.textContent = jobId;
  ui.jobIdOutputBadge.textContent = jobId;

  ui.metricEntities.textContent = String(entities.length);
  ui.metricWarnings.textContent = String(warnings.length);

  const progress = getProgressPercent(status);
  ui.metricProgress.textContent = String(progress);
  ui.statusProgressFill.style.width = `${progress}%`;

  ui.topPdfProgressFill.style.width = `${progress}%`;
  ui.topPdfProgressFill.classList.remove("loading", "completed");
  if (status === "queued" || status === "processing") {
    ui.topPdfProgressFill.classList.add("loading");
  } else if (status === "completed") {
    ui.topPdfProgressFill.classList.add("completed");
  }
  ui.topPdfProgressLabel.textContent = hasActiveJob ? `${progress}%` : "idle";

  const entityProgress = Math.min(100, entities.length * 10);
  ui.primaryProgressFill.style.width = `${entityProgress}%`;

  const warningProgress = Math.min(100, warnings.length * 25);
  ui.secondaryProgressFill.style.width = `${warningProgress}%`;

  ui.statusValue.textContent = status;
  ui.queueStatusTitle.textContent = statusLabel(status);
  ui.queueStatusDetail.textContent =
    (state.jobStatus && String(state.jobStatus.status_message || "").trim()) || statusDetail(status, jobId);
  ui.pipelineBadgeTitle.textContent = "Pipeline State";
  ui.pipelineBadgeDetail.textContent = `${status.toUpperCase()} • ${jobId}`;
  ui.liveValue.textContent = liveStateLabel(status, hasActiveJob);

  renderWorkflow(status, hasSelectedFile, hasActiveJob, hasPreview);

  renderStatusPill(status);
  renderWarnings(warnings);
  renderEntities(entities);
  renderSelectedEntity(entities);
  renderOutput(status);
  renderTopPanels(status, jobId, entities, warnings);
}

function notifyStatusTransition(status) {
  if (status === "queued") {
    pushNotification("info", "Job queued", "Waiting for worker assignment.");
    return;
  }

  if (status === "processing") {
    pushNotification("info", "Processing", "OCR and entity detection are in progress.");
    return;
  }

  if (status === "completed") {
    pushNotification("success", "Sanitization complete", "Redacted output is ready for review and download.");
    return;
  }

  if (status === "failed") {
    pushNotification("error", "Pipeline failed", "Review the alert panel for error details.");
  }
}

function pushNotification(level, title, detail) {
  const entry = {
    id: ++state.notificationCounter,
    level,
    title: String(title || "Update"),
    detail: String(detail || ""),
    timeIso: new Date().toISOString(),
    read: state.activeTopPanel === "notifications",
  };

  state.notifications = [entry, ...state.notifications].slice(0, 30);
}

function markAllNotificationsRead() {
  state.notifications = state.notifications.map((item) => ({ ...item, read: true }));
  renderTopPanels(getCurrentStatus(), state.jobInfo?.job_id || "-", getEntities(), getWarnings());
}

function toggleTopPanel(panelName) {
  state.activeTopPanel = state.activeTopPanel === panelName ? null : panelName;
  if (state.activeTopPanel === "notifications") {
    state.notifications = state.notifications.map((item) => ({ ...item, read: true }));
  }
  render();
}

function closeTopPanels(shouldRender = true) {
  if (!state.activeTopPanel) {
    return;
  }
  state.activeTopPanel = null;
  if (shouldRender) {
    render();
  }
}

function renderTopPanels(status, jobId, entities, warnings) {
  if (ui.securityPanel) {
    ui.securityPanel.classList.toggle("hidden", state.activeTopPanel !== "security");
  }
  if (ui.notificationsPanel) {
    ui.notificationsPanel.classList.toggle("hidden", state.activeTopPanel !== "notifications");
  }
  if (ui.securityBtn) {
    ui.securityBtn.classList.toggle("active", state.activeTopPanel === "security");
  }
  if (ui.notificationsBtn) {
    ui.notificationsBtn.classList.toggle("active", state.activeTopPanel === "notifications");
  }

  const unreadCount = state.notifications.filter((item) => !item.read).length;
  if (ui.notificationsBadge) {
    ui.notificationsBadge.textContent = unreadCount > 9 ? "9+" : String(unreadCount);
    ui.notificationsBadge.classList.toggle("hidden", unreadCount === 0);
  }

  if (ui.notificationsList) {
    ui.notificationsList.replaceChildren();
    if (!state.notifications.length) {
      const empty = document.createElement("li");
      empty.className = "text-[10px] text-on-surface-variant";
      empty.textContent = "No notifications yet.";
      ui.notificationsList.appendChild(empty);
    } else {
      state.notifications.forEach((item) => {
        const row = document.createElement("li");
        row.className = `notification-item ${escapeHtml(item.level || "info")}`;
        row.innerHTML = [
          `<p class="text-[10px] font-semibold tracking-wide uppercase ${item.level === "error" ? "text-error" : "text-primary"}">${escapeHtml(item.title)}</p>`,
          `<p class="mt-1 text-[10px] text-on-surface-variant leading-relaxed">${escapeHtml(item.detail)}</p>`,
          `<p class="mt-1 text-[9px] font-mono text-on-surface-variant/80">${escapeHtml(formatTimeLabel(item.timeIso))}</p>`,
        ].join("");
        ui.notificationsList.appendChild(row);
      });
    }
  }

  if (ui.securityStatusValue) {
    ui.securityStatusValue.textContent = String(status || "idle");
  }
  if (ui.securityJobValue) {
    ui.securityJobValue.textContent = shortJobId(jobId);
  }
  if (ui.securityEntityValue) {
    ui.securityEntityValue.textContent = String(Array.isArray(entities) ? entities.length : 0);
  }
  if (ui.securityWarningValue) {
    ui.securityWarningValue.textContent = String(Array.isArray(warnings) ? warnings.length : 0);
  }
  if (ui.securityUpdatedValue) {
    ui.securityUpdatedValue.textContent = formatTimeLabel(state.lastUpdatedIso);
  }
}

function shortJobId(jobId) {
  const id = String(jobId || "").trim();
  if (!id || id === "-") {
    return "-";
  }
  return id.length <= 10 ? id : `${id.slice(0, 8)}...`;
}

function formatTimeLabel(isoString) {
  if (!isoString) {
    return "-";
  }

  const parsed = new Date(isoString);
  if (Number.isNaN(parsed.getTime())) {
    return "-";
  }

  return parsed.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function renderStatusPill(status) {
  ui.statusBadgePill.className =
    "inline-flex items-center px-2 py-0.5 rounded text-[9px] uppercase font-bold tracking-tighter border";

  if (status === "completed") {
    ui.statusBadgePill.classList.add("bg-primary/10", "text-primary", "border-primary/30");
    ui.statusBadgePill.textContent = "Completed";
    return;
  }

  if (status === "failed") {
    ui.statusBadgePill.classList.add("bg-error/10", "text-error", "border-error/30");
    ui.statusBadgePill.textContent = "Failed";
    return;
  }

  if (status === "processing" || status === "queued") {
    ui.statusBadgePill.classList.add("bg-primary/10", "text-primary", "border-primary/30");
    ui.statusBadgePill.textContent = status === "queued" ? "Queued" : "Processing";
    return;
  }

  ui.statusBadgePill.classList.add("bg-surface-container-high", "text-on-surface-variant", "border-outline-variant/20");
  ui.statusBadgePill.textContent = "No active job";
}

function renderWarnings(warnings) {
  ui.warningsList.replaceChildren();

  if (!warnings.length) {
    const empty = document.createElement("li");
    empty.className = "text-[10px] text-on-surface-variant leading-relaxed";
    empty.textContent = "No warnings yet.";
    ui.warningsList.appendChild(empty);
    return;
  }

  warnings.forEach((warning) => {
    const item = document.createElement("li");
    item.className = "warning-reveal p-3 bg-error/10 border border-error/30 rounded text-[10px] leading-relaxed text-on-surface";
    item.style.setProperty("--warning-index", String(Math.min(6, ui.warningsList.childElementCount)));
    item.textContent = warning;
    ui.warningsList.appendChild(item);
  });
}

function renderEntities(entities) {
  ui.entitiesTableBody.replaceChildren();

  if (!entities.length) {
    const row = document.createElement("tr");
    row.innerHTML =
      '<td colspan="4" class="px-6 py-8 text-on-surface-variant text-xs">No entities detected yet.</td>';
    ui.entitiesTableBody.appendChild(row);
    return;
  }

  if (
    state.selectedEntityIndex === null ||
    state.selectedEntityIndex < 0 ||
    state.selectedEntityIndex >= entities.length
  ) {
    state.selectedEntityIndex = 0;
  }

  entities.forEach((entity, index) => {
    const row = document.createElement("tr");
    row.className = "table-row-reveal hover:bg-surface-container-highest/30 transition-colors cursor-pointer";
    row.style.setProperty("--row-index", String(Math.min(index, 9)));

    if (index === state.selectedEntityIndex) {
      row.classList.add("bg-surface-container-highest/30");
    }

    row.addEventListener("click", () => {
      state.selectedEntityIndex = index;
      renderSelectedEntity(entities);
      renderEntities(entities);
    });

    const pages = extractPages(entity).join(", ") || "-";
    const confidence = formatPercent(entity.confidence_score);
    const badgeClass = isHighRisk(entity)
      ? "bg-error/10 text-error border-error/30"
      : "bg-primary/10 text-primary border-primary/30";

    row.innerHTML = [
      `<td class="px-6 py-4 text-on-surface font-medium">${escapeHtml(entity.entity_text || "-")}</td>`,
      `<td class="px-4 py-4"><span class="px-2 py-0.5 ${badgeClass} border rounded text-[9px] uppercase font-bold tracking-tighter">${escapeHtml(entity.entity_type || "UNKNOWN")}</span></td>`,
      `<td class="px-6 py-4 text-on-surface-variant font-mono text-[10px] tracking-widest">${escapeHtml(pages)}</td>`,
      `<td class="px-6 py-4 font-mono text-primary font-bold">${escapeHtml(confidence)}</td>`,
    ].join("");

    ui.entitiesTableBody.appendChild(row);
  });
}

function renderSelectedEntity(entities) {
  if (!entities.length || state.selectedEntityIndex === null) {
    ui.selectedEntityCard.classList.add("hidden");
    return;
  }

  const entity = entities[state.selectedEntityIndex];
  if (!entity) {
    ui.selectedEntityCard.classList.add("hidden");
    return;
  }

  ui.selectedEntityCard.classList.remove("hidden");
  ui.selectedEntityText.textContent = entity.entity_text || "-";
  ui.selectedEntityType.textContent = entity.entity_type || "-";
  ui.selectedEntitySource.textContent = formatEntitySource(entity);
  ui.selectedEntityConfidence.textContent = formatPercent(entity.confidence_score);
  ui.selectedEntityPages.textContent = extractPages(entity).join(", ") || "-";
}

function renderOutput(status) {
  const hasPreview = Boolean(state.redactedUrl && state.redactedBlob);
  ui.downloadBtn.disabled = !hasPreview;

  if (hasPreview) {
    ui.previewEmptyState.classList.add("hidden");
    ui.pdfPreview.classList.remove("hidden");
    ui.pdfPreview.classList.add("ready");
    ui.pdfPreview.src = state.redactedUrl;
    ui.outputFileValue.textContent = state.redactedFileName || "sanitized_document.pdf";
    ui.outputSizeValue.textContent = formatBytes(state.redactedBlob.size);
    ui.outputStateValue.textContent = "completed";
    return;
  }

  ui.pdfPreview.classList.remove("ready");
  ui.pdfPreview.classList.add("hidden");
  ui.pdfPreview.removeAttribute("src");
  ui.previewEmptyState.classList.remove("hidden");

  if (status === "failed") {
    ui.outputFileValue.textContent = "pipeline failed";
    ui.outputSizeValue.textContent = "-";
    ui.outputStateValue.textContent = "failed";
    return;
  }

  if (status === "processing" || status === "queued") {
    ui.outputFileValue.textContent = "pending completion";
    ui.outputSizeValue.textContent = "-";
    ui.outputStateValue.textContent = status;
    return;
  }

  ui.outputFileValue.textContent = "-";
  ui.outputSizeValue.textContent = "-";
  ui.outputStateValue.textContent = "idle";
}

function liveStateLabel(status, hasActiveJob) {
  if (!hasActiveJob) {
    return "idle";
  }
  if (status === "queued") {
    return "queued";
  }
  if (status === "processing") {
    return "processing";
  }
  if (status === "completed") {
    return "secured";
  }
  if (status === "failed") {
    return "attention";
  }
  return "active";
}

function getProgressPercent(status) {
  const backendProgress = Number(state.jobStatus && state.jobStatus.progress);
  if (Number.isFinite(backendProgress)) {
    return Math.round(Math.max(0, Math.min(1, backendProgress)) * 100);
  }

  return STATUS_PROGRESS[status] ?? 0;
}

function setWorkflowStepState(node, mode) {
  if (!node) {
    return;
  }

  node.classList.remove("active", "done");
  if (mode === "active") {
    node.classList.add("active");
  } else if (mode === "done") {
    node.classList.add("done");
  }
}

function renderWorkflow(status, hasSelectedFile, hasActiveJob, hasPreview) {
  if (!ui.stepUpload || !ui.stepQueue || !ui.stepReview || !ui.nextActionHint) {
    return;
  }

  setWorkflowStepState(ui.stepUpload, hasSelectedFile ? "done" : "active");
  setWorkflowStepState(ui.stepQueue, "");
  setWorkflowStepState(ui.stepReview, "");

  if (hasSelectedFile && !hasActiveJob) {
    setWorkflowStepState(ui.stepQueue, "active");
    ui.nextActionHint.textContent = "Uploading document and starting sanitization...";
    return;
  }

  if (status === "queued" || status === "processing") {
    setWorkflowStepState(ui.stepQueue, "active");
    ui.nextActionHint.textContent = "Processing in progress. You can keep this page open while status auto-refreshes.";
    return;
  }

  if (status === "completed" && hasPreview) {
    setWorkflowStepState(ui.stepQueue, "done");
    setWorkflowStepState(ui.stepReview, "done");
    ui.nextActionHint.textContent = "Review detected entities, preview output, then download sanitized PDF.";
    return;
  }

  if (status === "failed") {
    setWorkflowStepState(ui.stepQueue, "done");
    setWorkflowStepState(ui.stepReview, "active");
    ui.nextActionHint.textContent = "Check warnings/error details, then upload the file again.";
    return;
  }

  ui.nextActionHint.textContent = "Select a PDF and processing will start automatically.";
}

function downloadPdf() {
  if (!state.redactedBlob || !state.redactedUrl) {
    return;
  }

  const anchor = document.createElement("a");
  anchor.href = state.redactedUrl;
  anchor.download = state.redactedFileName || "sanitized_document.pdf";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
}

function extractPages(entity) {
  if (!entity || !Array.isArray(entity.boxes)) {
    return [];
  }

  const pages = new Set();
  entity.boxes.forEach((box) => {
    if (box && Number.isInteger(box.page_number)) {
      pages.add(box.page_number);
    }
  });

  return Array.from(pages).sort((a, b) => a - b).map((page) => String(page));
}

function isHighRisk(entity) {
  const type = String(entity.entity_type || "").toUpperCase();
  return HIGH_RISK_ENTITY_TYPES.has(type);
}

function formatEntitySource(entity) {
  const source = String((entity && entity.source) || "").trim();
  if (!source) {
    return "-";
  }

  if (source !== "Hybrid") {
    return source;
  }

  const rawSupporting = Array.isArray(entity.supporting_sources)
    ? entity.supporting_sources
    : [];
  const normalized = [];
  rawSupporting.forEach((item) => {
    const label = String(item || "").trim();
    if (!label || normalized.includes(label)) {
      return;
    }
    normalized.push(label);
  });

  if (!normalized.length) {
    return "Hybrid";
  }

  return `Hybrid (${normalized.join(" + ")})`;
}

function statusLabel(status) {
  if (status === "queued") {
    return "Job Queued";
  }
  if (status === "processing") {
    return "Sanitization Running";
  }
  if (status === "completed") {
    return "Output Ready";
  }
  if (status === "failed") {
    return "Execution Failed";
  }
  return "Queue Idle";
}

function statusDetail(status, jobId) {
  if (status === "queued") {
    return `Awaiting worker dispatch • ${jobId}`;
  }
  if (status === "processing") {
    return `OCR + entity triage in flight • ${jobId}`;
  }
  if (status === "completed") {
    return `Redacted payload secured • ${jobId}`;
  }
  if (status === "failed") {
    return `Review warnings and retry • ${jobId}`;
  }
  return "Awaiting job submission";
}

function formatBytes(byteCount) {
  if (!Number.isFinite(byteCount) || byteCount <= 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB"];
  let index = 0;
  let value = byteCount;

  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }

  const fixed = value >= 100 ? value.toFixed(0) : value.toFixed(2);
  return `${fixed} ${units[index]}`;
}

function formatPercent(value) {
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) {
    return "0.0%";
  }
  const percent = Math.max(0, Math.min(1, normalized)) * 100;
  return `${percent.toFixed(1)}%`;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

init();
