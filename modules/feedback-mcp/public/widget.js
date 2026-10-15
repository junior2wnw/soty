(() => {
  "use strict";
  const script = document.currentScript,
    project = script?.dataset.feedbackProject;
  if (!/^fp_[a-f0-9]{32}$/.test(project ?? "")) return;
  const origin = new URL(script.src).origin,
    key = "feedback-widget-" + project;
  if (document.getElementById(key)) return;
  const host = document.createElement("div");
  host.id = key;
  const button = document.createElement("button");
  button.type = "button";
  button.textContent = script.dataset.feedbackLabel || "Обратная связь";
  Object.assign(button.style, {
    position: "fixed",
    bottom: "20px",
    right: "20px",
    zIndex: "2147483000",
    minHeight: "44px",
    padding: "12px 18px",
    borderRadius: "12px",
    border: "1px solid #175e4c",
    background: "#175e4c",
    color: "#fff",
    font: "600 14px system-ui",
    cursor: "pointer",
  });
  const dialog = document.createElement("dialog");
  dialog.setAttribute("aria-label", "Обратная связь");
  Object.assign(dialog.style, {
    padding: "0",
    border: "1px solid #dce4dd",
    borderRadius: "16px",
    width: "min(720px,calc(100vw - 24px))",
    height: "min(800px,calc(100dvh - 24px))",
    maxWidth: "calc(100vw - 24px)",
    maxHeight: "calc(100dvh - 24px)",
  });
  const close = document.createElement("button");
  close.type = "button";
  close.textContent = "Закрыть";
  Object.assign(close.style, {
    minHeight: "44px",
    width: "100%",
    background: "#f4f6f4",
    border: "0",
    font: "600 14px system-ui",
    cursor: "pointer",
  });
  let frame;
  close.addEventListener("click", () => dialog.close());
  dialog.addEventListener("close", () => button.focus());
  button.addEventListener("click", () => {
    if (typeof dialog.showModal !== "function") {
      window.open(origin + "/p/" + project, "_blank", "noopener,noreferrer");
      return;
    }
    if (!frame) {
      frame = document.createElement("iframe");
      frame.src = origin + "/p/" + project;
      frame.title = "Обратная связь по проекту";
      frame.referrerPolicy = "no-referrer";
      frame.setAttribute(
        "sandbox",
        "allow-scripts allow-forms allow-same-origin allow-popups",
      );
      frame.allow = "microphone; display-capture";
      Object.assign(frame.style, {
        width: "100%",
        height: "calc(100% - 44px)",
        border: "0",
        display: "block",
      });
      dialog.append(frame);
    }
    dialog.showModal();
  });
  dialog.append(close);
  host.append(button, dialog);
  (document.body || document.documentElement).append(host);
})();
