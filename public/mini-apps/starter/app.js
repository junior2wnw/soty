const params = new URLSearchParams(location.search);
const appId = params.get("sotyMiniApp") || "starter";
const nonce = params.get("sotyNonce") || "";
const protocol = "soty.mini-app.v1";
const contextProtocol = "soty.mini-app.context.v1";
let selected = null;

function send(type, body = {}) {
  parent.postMessage({ schema: protocol, nonce, type, ...body }, location.origin);
}

function setLog(text) {
  document.getElementById("log").textContent = text;
}

window.addEventListener("message", (event) => {
  const message = event.data || {};
  if (event.origin !== location.origin || message.schema !== contextProtocol || message.nonce !== nonce) {
    return;
  }
  selected = message.selected || null;
  document.getElementById("title").textContent = `${appId} / ${selected?.label || "no chat"}`;
  document.getElementById("context").textContent = selected
    ? `${selected.tunnelId} / ${selected.syncState}`
    : "select a chat";
  document.getElementById("state").textContent = selected?.remoteController ? "REMOTE" : "CHAT";
});

document.getElementById("form").addEventListener("submit", (event) => {
  event.preventDefault();
  const input = document.getElementById("input");
  const text = input.value.trim();
  if (!text) {
    return;
  }
  send("chat.append", { text });
  setLog(`Sent to chat: ${text}`);
  input.value = "";
});

document.getElementById("agent").addEventListener("click", () => {
  const input = document.getElementById("input");
  const text = input.value.trim();
  if (!text) {
    return;
  }
  send("agent.invoke", {
    visibleText: `Mini app: ${text}`,
    text: `агент, ${text}`
  });
  setLog(`Agent invoked: ${text}`);
  input.value = "";
});

send("ready");
