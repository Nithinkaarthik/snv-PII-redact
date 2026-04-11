const extensionApi = typeof browser !== "undefined" ? browser : chrome;

const state = {
  settings: null,
  errorLogs: [],
};

const ui = {
  globalStatus: document.getElementById("globalStatus"),
  apiBaseValue: document.getElementById("apiBaseValue"),
  allowlistCountValue: document.getElementById("allowlistCountValue"),
  backendHealthValue: document.getElementById("backendHealthValue"),
  errorCountValue: document.getElementById("errorCountValue"),
  errorList: document.getElementById("errorList"),
  emptyState: document.getElementById("emptyState"),
  refreshBtn: document.getElementById("refreshBtn"),
  clearErrorsBtn: document.getElementById("clearErrorsBtn"),
  copyLatestBtn: document.getElementById("copyLatestBtn"),
  testBackendBtn: document.getElementById("testBackendBtn"),
  openOptionsBtn: document.getElementById("openOptionsBtn"),
};

void initialize();

async function initialize() {
  ui.refreshBtn.addEventListener("click", () => {
    void refreshDashboard();
  });

  ui.clearErrorsBtn.addEventListener("click", () => {
    void clearErrors();
  });

  ui.copyLatestBtn.addEventListener("click", () => {
    void copyLatestError();
  });

  ui.testBackendBtn.addEventListener("click", () => {
    void testBackendHealth();
  });

  ui.openOptionsBtn.addEventListener("click", () => {
    void openSettingsPage();
  });

  await refreshDashboard();
}

async function refreshDashboard() {
  setStatus("Loading dashboard...", "muted");

  try {
    const [settings, errorLogs] = await Promise.all([
      SNVConfig.getSettings(),
      SNVConfig.getErrorLogs(60),
    ]);

    state.settings = settings;
    state.errorLogs = Array.isArray(errorLogs) ? [...errorLogs].reverse() : [];

    renderSettings();
    renderErrorLogs();

    setStatus("Dashboard ready.", "ok");
  } catch (error) {
    const message = readErrorMessage(error);
    setStatus(`Failed to load dashboard: ${message}`, "error");
    await appendPopupError("DASHBOARD_LOAD_FAILED", message);
  }
}

function renderSettings() {
  const settings = state.settings || {
    apiBaseUrl: SNVConfig.DEFAULT_API_BASE_URL,
    allowedDomains: [],
  };

  ui.apiBaseValue.textContent = settings.apiBaseUrl;
  ui.allowlistCountValue.textContent = String(
    Array.isArray(settings.allowedDomains) ? settings.allowedDomains.length : 0,
  );
}

function renderErrorLogs() {
  const logs = Array.isArray(state.errorLogs) ? state.errorLogs : [];
  ui.errorCountValue.textContent = String(logs.length);

  if (logs.length === 0) {
    ui.errorList.hidden = true;
    ui.emptyState.hidden = false;
    ui.errorList.textContent = "";
    return;
  }

  ui.errorList.hidden = false;
  ui.emptyState.hidden = true;
  ui.errorList.textContent = "";

  for (const log of logs) {
    const item = document.createElement("li");
    item.className = "error-item";

    const meta = document.createElement("div");
    meta.className = "error-meta";

    const timestamp = formatTimestamp(log.timestamp);
    const source = String(log.source || "extension");
    const code = log.code ? ` [${log.code}]` : "";
    meta.textContent = `${timestamp} - ${source}${code}`;

    const text = document.createElement("div");
    text.className = "error-text";
    text.textContent = String(log.message || "Unknown error.");

    item.appendChild(meta);
    item.appendChild(text);

    if (log.pageUrl || log.fileName || log.jobId) {
      const detail = document.createElement("div");
      detail.className = "error-meta";
      const parts = [];
      if (log.fileName) {
        parts.push(`File: ${log.fileName}`);
      }
      if (log.jobId) {
        parts.push(`Job: ${log.jobId}`);
      }
      if (log.pageUrl) {
        parts.push(`Page: ${compactUrl(log.pageUrl)}`);
      }
      detail.textContent = parts.join(" | ");
      item.appendChild(detail);
    }

    ui.errorList.appendChild(item);
  }
}

async function clearErrors() {
  try {
    await SNVConfig.clearErrorLogs();
    state.errorLogs = [];
    renderErrorLogs();
    setStatus("Error log cleared.", "ok");
  } catch (error) {
    const message = readErrorMessage(error);
    setStatus(`Unable to clear errors: ${message}`, "error");
  }
}

async function copyLatestError() {
  if (!state.errorLogs || state.errorLogs.length === 0) {
    setStatus("No errors to copy.", "warn");
    return;
  }

  const latest = state.errorLogs[0];
  const text = JSON.stringify(latest, null, 2);

  try {
    await navigator.clipboard.writeText(text);
    setStatus("Latest error copied to clipboard.", "ok");
  } catch (error) {
    const message = readErrorMessage(error);
    setStatus(`Clipboard copy failed: ${message}`, "error");
  }
}

async function testBackendHealth() {
  const settings = state.settings || (await SNVConfig.getSettings());
  const healthUrl = SNVConfig.resolveApiUrl(settings.apiBaseUrl, "/health");

  ui.backendHealthValue.textContent = "Checking...";
  ui.backendHealthValue.className = "value muted";

  const controller = new AbortController();
  const timeoutId = setTimeout(() => {
    controller.abort();
  }, 6000);

  try {
    const response = await fetch(healthUrl, {
      method: "GET",
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new Error(`Health check returned HTTP ${response.status}.`);
    }

    const payload = await readJsonSafe(response);
    if (!payload || String(payload.status || "").toLowerCase() !== "ok") {
      throw new Error("Health endpoint responded without status=ok.");
    }

    ui.backendHealthValue.textContent = "Healthy";
    ui.backendHealthValue.className = "value ok";
    setStatus("Backend health check passed.", "ok");
  } catch (error) {
    clearTimeout(timeoutId);
    const message = readErrorMessage(error);

    ui.backendHealthValue.textContent = "Unhealthy";
    ui.backendHealthValue.className = "value error";
    setStatus(`Backend check failed: ${message}`, "error");

    await appendPopupError("BACKEND_HEALTH_CHECK_FAILED", message, {
      detail: healthUrl,
    });
    await refreshDashboard();
  }
}

async function openSettingsPage() {
  try {
    if (extensionApi.runtime && extensionApi.runtime.openOptionsPage) {
      const maybePromise = extensionApi.runtime.openOptionsPage();
      if (maybePromise && typeof maybePromise.catch === "function") {
        await maybePromise;
      }
      return;
    }

    throw new Error("Options page API is unavailable.");
  } catch (error) {
    const message = readErrorMessage(error);
    setStatus(`Unable to open settings: ${message}`, "error");
    await appendPopupError("OPEN_SETTINGS_FAILED", message);
  }
}

async function appendPopupError(code, message, extra) {
  try {
    await SNVConfig.appendErrorLog({
      source: "popup",
      code,
      message,
      detail: extra && extra.detail ? String(extra.detail) : null,
    });
  } catch (_) {
    // Ignore secondary logging failures.
  }
}

function setStatus(text, level) {
  ui.globalStatus.textContent = text;
  ui.globalStatus.className = `status-line ${level || "muted"}`;
}

function formatTimestamp(rawValue) {
  if (!rawValue) {
    return "Unknown time";
  }

  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) {
    return String(rawValue);
  }

  return parsed.toLocaleString();
}

function compactUrl(rawValue) {
  try {
    const url = new URL(String(rawValue));
    return `${url.hostname}${url.pathname}`;
  } catch (_) {
    return String(rawValue);
  }
}

async function readJsonSafe(response) {
  try {
    return await response.json();
  } catch (_) {
    return null;
  }
}

function readErrorMessage(error) {
  if (!error) {
    return "Unknown error.";
  }

  if (typeof error === "string") {
    return error;
  }

  if (error instanceof Error && error.message) {
    return error.message;
  }

  return "Unknown error.";
}
