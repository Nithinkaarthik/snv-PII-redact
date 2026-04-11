const extensionApi = typeof browser !== "undefined" ? browser : chrome;

extensionApi.runtime.onInstalled.addListener(() => {
  console.info("[SNV DLP Tripwire] Background service worker initialized.");
});

extensionApi.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (!message || typeof message !== "object") {
    return undefined;
  }

  if (message.type === "SNV_PING") {
    sendResponse({ ok: true, service: "background" });
    return true;
  }

  return undefined;
});
