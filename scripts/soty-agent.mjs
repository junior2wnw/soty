#!/usr/bin/env node
import { spawn } from "node:child_process";
import { WebSocketServer, WebSocket } from "ws";

const port = Number.parseInt(arg("--port") || process.env.SOTY_AGENT_PORT || "49424", 10);
const defaultTimeoutMs = Number.parseInt(arg("--timeout") || process.env.SOTY_AGENT_TIMEOUT_MS || "600000", 10);
const requestedShell = arg("--shell") || process.env.SOTY_AGENT_SHELL || "";
const maxChunkBytes = 12_000;
const active = new Map();
const allowedOrigins = new Set([
  "https://\u0441\u043e\u0442\u044b.online",
  "https://xn--n1afe0b.online",
  "http://localhost:5173",
  "http://127.0.0.1:5173",
  "http://localhost:5178",
  "http://127.0.0.1:5178"
]);

const server = new WebSocketServer({
  host: "127.0.0.1",
  port,
  maxPayload: 64_000
});

server.on("connection", (ws, request) => {
  const origin = request.headers.origin || "";
  if (origin && !allowedOrigins.has(origin) && !origin.startsWith("http://localhost:") && !origin.startsWith("http://127.0.0.1:")) {
    ws.close(1008, "origin");
    return;
  }
  ws.on("message", (raw) => handleMessage(ws, raw));
});

server.on("listening", () => {
  process.stdout.write(`soty-agent:${port}\n`);
});

function handleMessage(ws, raw) {
  let message;
  try {
    message = JSON.parse(raw.toString("utf8"));
  } catch {
    return;
  }

  if (message?.type === "hello") {
    send(ws, message.id || "hello", "", undefined, "ready", {
      cwd: process.cwd(),
      platform: process.platform,
      shell: shellName()
    });
    return;
  }

  if (message?.type === "stop" && isSafeText(message.id, 160)) {
    active.get(message.id)?.kill();
    return;
  }

  if (message?.type !== "run" || !isSafeText(message.id, 160) || !isSafeText(message.command, 8000)) {
    return;
  }

  runCommand(
    ws,
    message.id,
    message.command,
    Number.isSafeInteger(message.timeoutMs) ? message.timeoutMs : defaultTimeoutMs
  );
}

function runCommand(ws, id, command, timeoutMs) {
  const shell = shellSpec(command);
  const child = spawn(shell.file, shell.args, {
    cwd: process.cwd(),
    env: process.env,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"]
  });
  active.set(id, child);
  let timedOut = false;
  send(ws, id, "", undefined, "start", {
    pid: child.pid || 0,
    cwd: process.cwd()
  });

  const timer = setTimeout(() => {
    if (active.get(id) !== child) {
      return;
    }
    timedOut = true;
    child.kill();
    send(ws, id, "!\n", 124, "exit");
  }, Math.max(1000, timeoutMs));

  child.stdout.on("data", (chunk) => sendChunks(ws, id, chunk.toString("utf8")));
  child.stderr.on("data", (chunk) => sendChunks(ws, id, chunk.toString("utf8")));
  child.on("error", (error) => {
    clearTimeout(timer);
    active.delete(id);
    send(ws, id, `${error.message}\n`, 127, "error");
    ws.close(1011);
  });
  child.on("close", (code) => {
    clearTimeout(timer);
    active.delete(id);
    if (!timedOut) {
      send(ws, id, "", Number.isSafeInteger(code) ? code : 0, "exit");
    }
    ws.close(1000);
  });
}

function sendChunks(ws, id, text) {
  for (let index = 0; index < text.length; index += maxChunkBytes) {
    send(ws, id, text.slice(index, index + maxChunkBytes));
  }
}

function send(ws, id, text, exitCode, type = "data", extra = {}) {
  if (ws.readyState !== WebSocket.OPEN) {
    return;
  }
  ws.send(JSON.stringify({
    type,
    id,
    text,
    ...extra,
    ...(typeof exitCode === "number" ? { exitCode } : {})
  }));
}

function isSafeText(value, max) {
  return typeof value === "string" && value.length > 0 && value.length <= max;
}

function arg(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : "";
}

function shellSpec(command) {
  if (process.platform !== "win32") {
    return { file: requestedShell || process.env.SHELL || "/bin/sh", args: ["-lc", command] };
  }
  if (requestedShell.toLowerCase().includes("cmd")) {
    return { file: process.env.ComSpec || "cmd.exe", args: ["/d", "/s", "/c", command] };
  }
  const file = requestedShell || "powershell.exe";
  const wrapped = `${command}; if ($global:LASTEXITCODE -ne $null) { exit $global:LASTEXITCODE }`;
  return {
    file,
    args: ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", wrapped]
  };
}

function shellName() {
  if (process.platform !== "win32") {
    return requestedShell || process.env.SHELL || "/bin/sh";
  }
  return requestedShell || "powershell.exe";
}
