(function (globalScope) {
  const STORAGE_KEY = "snvRedactorSettings";
  const ERROR_LOG_KEY = "snvRedactorErrorLog";
  const ERROR_LOG_LIMIT = 120;
  const DEFAULT_API_BASE_URL = "http://127.0.0.1:8000";

  const DEFAULT_SETTINGS = Object.freeze({
    apiBaseUrl: DEFAULT_API_BASE_URL,
    allowedDomains: [],
  });

  function getExtensionApi() {
    if (typeof browser !== "undefined") {
      return browser;
    }
    if (typeof chrome !== "undefined") {
      return chrome;
    }
    throw new Error("Browser extension API is unavailable.");
  }

  function getStorageArea(areaName = "sync") {
    const extApi = getExtensionApi();
    if (!extApi.storage) {
      throw new Error("Browser storage API is unavailable.");
    }

    if (areaName === "local") {
      if (!extApi.storage.local) {
        throw new Error("Browser storage.local API is unavailable.");
      }
      return extApi.storage.local;
    }

    if (!extApi.storage.sync) {
      throw new Error("Browser storage.sync API is unavailable.");
    }
    return extApi.storage.sync;
  }

  function storageGet(keys, areaName = "sync") {
    const storage = getStorageArea(areaName);
    try {
      const maybePromise = storage.get(keys);
      if (maybePromise && typeof maybePromise.then === "function") {
        return maybePromise.then((result) => result || {});
      }
    } catch (_) {
      // Fallback to callback style for engines that do not expose Promise-based storage.
    }

    return new Promise((resolve, reject) => {
      try {
        storage.get(keys, (result) => {
          const runtimeError =
            typeof chrome !== "undefined" && chrome.runtime ? chrome.runtime.lastError : null;
          if (runtimeError) {
            reject(new Error(runtimeError.message));
            return;
          }
          resolve(result || {});
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  function storageSet(value, areaName = "sync") {
    const storage = getStorageArea(areaName);
    try {
      const maybePromise = storage.set(value);
      if (maybePromise && typeof maybePromise.then === "function") {
        return maybePromise;
      }
    } catch (_) {
      // Fallback to callback style for engines that do not expose Promise-based storage.
    }

    return new Promise((resolve, reject) => {
      try {
        storage.set(value, () => {
          const runtimeError =
            typeof chrome !== "undefined" && chrome.runtime ? chrome.runtime.lastError : null;
          if (runtimeError) {
            reject(new Error(runtimeError.message));
            return;
          }
          resolve();
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  function normalizeApiBaseUrl(rawValue) {
    const fallback = DEFAULT_SETTINGS.apiBaseUrl;
    const candidate = String(rawValue || "").trim() || fallback;

    try {
      const parsed = new URL(candidate);
      if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
        return fallback;
      }
      parsed.hash = "";
      parsed.search = "";
      return parsed.toString().replace(/\/+$/, "");
    } catch (_) {
      return fallback;
    }
  }

  function isIpv4Address(hostname) {
    if (!/^\d{1,3}(?:\.\d{1,3}){3}$/.test(hostname)) {
      return false;
    }
    return hostname.split(".").every((part) => {
      const numeric = Number(part);
      return Number.isInteger(numeric) && numeric >= 0 && numeric <= 255;
    });
  }

  function isValidHostname(hostname) {
    if (!hostname) {
      return false;
    }

    const normalized = hostname.toLowerCase();
    if (normalized === "localhost") {
      return true;
    }
    if (isIpv4Address(normalized)) {
      return true;
    }
    return /^[a-z0-9-]+(?:\.[a-z0-9-]+)+$/.test(normalized);
  }

  function normalizeDomainEntry(value) {
    if (typeof value !== "string") {
      return null;
    }

    let candidate = value.trim().toLowerCase();
    if (!candidate) {
      return null;
    }

    if (candidate.includes("://")) {
      try {
        candidate = new URL(candidate).hostname.toLowerCase();
      } catch (_) {
        return null;
      }
    }

    if (candidate.includes(":") && !candidate.startsWith("*.")) {
      const parts = candidate.split(":");
      if (parts.length === 2 && /^\d{1,5}$/.test(parts[1])) {
        candidate = parts[0];
      }
    }

    candidate = candidate.replace(/^\.+/, "").replace(/\.+$/, "");
    if (!candidate) {
      return null;
    }

    if (candidate.startsWith("*.")) {
      const baseDomain = candidate.slice(2);
      if (!isValidHostname(baseDomain)) {
        return null;
      }
      return `*.${baseDomain}`;
    }

    if (!isValidHostname(candidate)) {
      return null;
    }

    return candidate;
  }

  function parseAllowedDomains(rawValue) {
    const tokens = Array.isArray(rawValue)
      ? rawValue
      : String(rawValue || "")
          .split(/[\n,]/)
          .map((token) => token.trim())
          .filter(Boolean);

    const deduped = new Set();
    for (const token of tokens) {
      const normalized = normalizeDomainEntry(token);
      if (normalized) {
        deduped.add(normalized);
      }
    }

    return Array.from(deduped);
  }

  function formatAllowedDomains(domains) {
    return parseAllowedDomains(domains).join("\n");
  }

  function domainMatches(hostname, domainRule) {
    const host = String(hostname || "").toLowerCase();
    const rule = String(domainRule || "").toLowerCase();
    if (!host || !rule) {
      return false;
    }

    if (rule.startsWith("*.")) {
      const base = rule.slice(2);
      return host === base || host.endsWith(`.${base}`);
    }

    return host === rule;
  }

  function isUrlAllowed(urlValue, allowedDomains) {
    const rules = parseAllowedDomains(allowedDomains);
    if (rules.length === 0) {
      return false;
    }

    try {
      const hostname = new URL(urlValue).hostname.toLowerCase();
      return rules.some((rule) => domainMatches(hostname, rule));
    } catch (_) {
      return false;
    }
  }

  function resolveApiUrl(apiBaseUrl, pathOrUrl) {
    const base = normalizeApiBaseUrl(apiBaseUrl);
    try {
      return new URL(String(pathOrUrl || ""), `${base}/`).toString();
    } catch (_) {
      return `${base}/api/v1/sanitize`;
    }
  }

  function normalizeSettings(rawSettings) {
    const source = rawSettings && typeof rawSettings === "object" ? rawSettings : {};
    return {
      apiBaseUrl: normalizeApiBaseUrl(source.apiBaseUrl),
      allowedDomains: parseAllowedDomains(source.allowedDomains),
    };
  }

  async function getSettings() {
    const result = await storageGet([STORAGE_KEY]);
    return normalizeSettings(result[STORAGE_KEY] || DEFAULT_SETTINGS);
  }

  async function saveSettings(partialSettings) {
    const existing = await getSettings();
    const merged = normalizeSettings({
      ...existing,
      ...(partialSettings || {}),
    });
    await storageSet({ [STORAGE_KEY]: merged });
    return merged;
  }

  function normalizeErrorMessage(value) {
    const text = String(value || "").trim();
    return text || "Unknown error.";
  }

  function normalizeErrorLogEntry(entry) {
    const source = String(entry && entry.source ? entry.source : "extension").trim() || "extension";
    const codeValue = entry && entry.code ? String(entry.code).trim() : "";
    const pageUrl = entry && entry.pageUrl ? String(entry.pageUrl).trim() : "";
    const jobId = entry && entry.jobId ? String(entry.jobId).trim() : "";
    const fileName = entry && entry.fileName ? String(entry.fileName).trim() : "";
    const detail = entry && entry.detail ? String(entry.detail).trim() : "";

    return {
      timestamp: new Date().toISOString(),
      source,
      code: codeValue || null,
      message: normalizeErrorMessage(entry && entry.message),
      pageUrl: pageUrl || null,
      jobId: jobId || null,
      fileName: fileName || null,
      detail: detail || null,
    };
  }

  function normalizeErrorLogList(rawValue) {
    if (!Array.isArray(rawValue)) {
      return [];
    }

    const normalized = [];
    for (const item of rawValue) {
      if (!item || typeof item !== "object") {
        continue;
      }

      const timestamp = String(item.timestamp || "").trim();
      normalized.push({
        timestamp: timestamp || new Date().toISOString(),
        source: String(item.source || "extension").trim() || "extension",
        code: item.code ? String(item.code).trim() : null,
        message: normalizeErrorMessage(item.message),
        pageUrl: item.pageUrl ? String(item.pageUrl).trim() : null,
        jobId: item.jobId ? String(item.jobId).trim() : null,
        fileName: item.fileName ? String(item.fileName).trim() : null,
        detail: item.detail ? String(item.detail).trim() : null,
      });
    }

    return normalized.slice(-ERROR_LOG_LIMIT);
  }

  async function getErrorLogs(limit) {
    const result = await storageGet([ERROR_LOG_KEY], "local");
    const logs = normalizeErrorLogList(result[ERROR_LOG_KEY]);
    if (typeof limit === "number" && Number.isFinite(limit) && limit > 0) {
      return logs.slice(-Math.floor(limit));
    }
    return logs;
  }

  async function appendErrorLog(entry) {
    const existing = await getErrorLogs();
    const normalized = normalizeErrorLogEntry(entry || {});
    const updated = [...existing, normalized].slice(-ERROR_LOG_LIMIT);
    await storageSet({ [ERROR_LOG_KEY]: updated }, "local");
    return normalized;
  }

  async function clearErrorLogs() {
    await storageSet({ [ERROR_LOG_KEY]: [] }, "local");
  }

  const exported = {
    ERROR_LOG_KEY,
    STORAGE_KEY,
    DEFAULT_API_BASE_URL,
    DEFAULT_SETTINGS,
    appendErrorLog,
    clearErrorLogs,
    formatAllowedDomains,
    getErrorLogs,
    getSettings,
    isUrlAllowed,
    normalizeApiBaseUrl,
    parseAllowedDomains,
    resolveApiUrl,
    saveSettings,
  };

  globalScope.SNVConfig = exported;
})(typeof self !== "undefined" ? self : window);
