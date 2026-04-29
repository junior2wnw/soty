#!/usr/bin/env node
import { spawn } from "node:child_process";
import { WebSocketServer, WebSocket } from "ws";

const port = Number.parseInt(arg("--port") || process.env.SOTY_AGENT_PORT || "49424", 10);
const allowedOrigins = new Set([
  "https://соты.online",
  "https://xn--n1afe0b.online",
  "http://localhost:5173",
  "http://127.0.0.1:5173"
]);

const server = new WebSocketServer({
  host: "127.0.0.1",
  port,
  maxPayload: 16_384
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
  if (message?.type !== "run" || !isSafeText(message.id, 160) || !isSafeText(message.command, 8000)) {
    return;
  }
  runCommand(ws, message.id, message.command);
}

function runCommand(ws, id, command) {
  const shell = process.platform === "win32"
    ? { file: process.env.ComSpec || "cmd.exe", args: ["/d", "/s", "/c", command] }
    : { file: process.env.SHELL || "/bin/sh", args: ["-lc", command] };
  const child = spawn(shell.file, shell.args, {
    cwd: process.cwd(),
    env: process.env,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"]
  });

  child.stdout.on("data", (chunk) => send(ws, id, chunk.toString("utf8")));
  child.stderr.on("data", (chunk) => send(ws, id, chunk.toString("utf8")));
  child.on("error", (error) => {
    send(ws, id, `${error.message}\n`, 127);
    ws.close(1011);
  });
  child.on("close", (code) => {
    send(ws, id, "", Number.isSafeInteger(code) ? code : 0);
    ws.close(1000);
  });
}

function send(ws, id, text, exitCode) {
  if (ws.readyState !== WebSocket.OPEN) {
    return;
  }
  ws.send(JSON.stringify({
    type: "data",
    id,
    text,
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
