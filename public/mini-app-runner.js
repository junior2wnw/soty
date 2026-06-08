const runnerProtocol = "soty.mini-app.runner.v1";
const appFrame = document.getElementById("mini-app-frame");

let pendingMessages = [];
let childReady = false;

function postToChild(message) {
  if (!appFrame?.contentWindow) {
    return;
  }
  appFrame.contentWindow.postMessage(message, "*");
}

function flushPendingMessages() {
  const messages = pendingMessages;
  pendingMessages = [];
  messages.forEach(postToChild);
}

function loadInlineApp(message) {
  if (!appFrame) {
    return;
  }
  childReady = false;
  pendingMessages = [];
  appFrame.removeAttribute("src");
  appFrame.srcdoc = typeof message.html === "string" ? message.html : "";
}

window.addEventListener("message", (event) => {
  if (event.source === appFrame?.contentWindow) {
    window.parent.postMessage(event.data, "*");
    return;
  }
  const message = event.data;
  if (message && typeof message === "object" && message.schema === runnerProtocol && message.type === "load") {
    loadInlineApp(message);
    return;
  }
  if (!appFrame?.contentWindow) {
    return;
  }
  if (!childReady) {
    pendingMessages.push(message);
    return;
  }
  postToChild(message);
});

appFrame?.addEventListener("load", () => {
  childReady = true;
  flushPendingMessages();
});

window.parent.postMessage({
  schema: runnerProtocol,
  type: "ready"
}, "*");
