const apiBaseUrlInput = document.getElementById("apiBaseUrl");
const allowedDomainsInput = document.getElementById("allowedDomains");
const saveBtn = document.getElementById("saveBtn");
const restoreDefaultsBtn = document.getElementById("restoreDefaultsBtn");
const statusText = document.getElementById("statusText");

void initialize();

saveBtn.addEventListener("click", async () => {
  await saveSettings();
});

restoreDefaultsBtn.addEventListener("click", async () => {
  await restoreDefaults();
});

async function initialize() {
  try {
    const settings = await SNVConfig.getSettings();
    applySettingsToForm(settings);
    setStatus("Settings loaded.", "ok");
  } catch (error) {
    setStatus(readErrorMessage(error), "error");
  }
}

async function saveSettings() {
  saveBtn.disabled = true;
  restoreDefaultsBtn.disabled = true;

  try {
    const updated = await SNVConfig.saveSettings({
      apiBaseUrl: apiBaseUrlInput.value,
      allowedDomains: SNVConfig.parseAllowedDomains(allowedDomainsInput.value),
    });

    applySettingsToForm(updated);
    setStatus("Saved. Upload interception is active for allowlisted domains.", "ok");
  } catch (error) {
    setStatus(readErrorMessage(error), "error");
  } finally {
    saveBtn.disabled = false;
    restoreDefaultsBtn.disabled = false;
  }
}

async function restoreDefaults() {
  saveBtn.disabled = true;
  restoreDefaultsBtn.disabled = true;

  try {
    const defaults = await SNVConfig.saveSettings({
      apiBaseUrl: SNVConfig.DEFAULT_API_BASE_URL,
      allowedDomains: [],
    });

    applySettingsToForm(defaults);
    setStatus("Defaults restored. Add domains before interception can run.", "ok");
  } catch (error) {
    setStatus(readErrorMessage(error), "error");
  } finally {
    saveBtn.disabled = false;
    restoreDefaultsBtn.disabled = false;
  }
}

function applySettingsToForm(settings) {
  apiBaseUrlInput.value = settings.apiBaseUrl;
  allowedDomainsInput.value = SNVConfig.formatAllowedDomains(settings.allowedDomains);
}

function setStatus(message, mode) {
  statusText.textContent = message || "";
  statusText.className = `status ${mode || ""}`.trim();
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
