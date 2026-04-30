#!/usr/bin/env node
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { copyFile, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const agentVersion = "0.3.7";
const port = Number.parseInt(arg("--port") || process.env.SOTY_AGENT_PORT || "49424", 10);
const defaultTimeoutMs = Number.parseInt(arg("--timeout") || process.env.SOTY_AGENT_TIMEOUT_MS || "600000", 10);
const requestedShell = arg("--shell") || process.env.SOTY_AGENT_SHELL || "";
const updateManifestUrl = arg("--update-url") || process.env.SOTY_AGENT_UPDATE_URL || "https://xn--n1afe0b.online/agent/manifest.json";
const managed = process.argv.includes("--managed") || process.env.SOTY_AGENT_MANAGED === "1";
const agentScope = safeScope(process.env.SOTY_AGENT_SCOPE || (managed ? "CurrentUser" : "Dev"));
const maxCommandChars = 8_000;
const maxScriptChars = 1_000_000;
const maxChatChars = 12_000;
const maxChunkBytes = 12_000;
const maxFrameBytes = 2_500_000;
const active = new Map();
const operatorRuns = new Map();
let operatorBridge = null;
let operatorTargets = [];
const allowedOrigins = new Set([
  "https://xn--n1afe0b.online",
]);

if (process.argv[2] === "ctl") {
  runControlCli(process.argv.slice(3)).catch((error) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });
} else {
  startServer();
}

function startServer() {
  const server = createServer((request, response) => {
    void handleHttpRequest(request, response);
  });

  server.on("upgrade", (request, socket) => {
    const origin = String(request.headers.origin || "");
    if (!originAllowed(origin)) {
      socket.write("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    const key = String(request.headers["sec-websocket-key"] || "");
    if (!/^[+/0-9A-Za-z]{20,}={0,2}$/u.test(key)) {
      socket.write("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    const accept = createHash("sha1")
      .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
      .digest("base64");
    socket.write([
      "HTTP/1.1 101 Switching Protocols",
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Accept: ${accept}`,
      "\r\n"
    ].join("\r\n"));
    const ws = new LocalWebSocket(socket);
    ws.onMessage = (raw) => handleMessage(ws, raw);
    ws.onClose = () => cleanupOperatorSocket(ws);
  });

  server.listen(port, "127.0.0.1", () => {
    process.stdout.write(`soty-agent:${port}\n`);
    scheduleUpdate();
  });
}

async function handleHttpRequest(request, response) {
  const origin = String(request.headers.origin || "");
  if (!originAllowed(origin)) {
    response.writeHead(403, { "Cache-Control": "no-store" });
    response.end();
    return;
  }
  const headers = {
    "Access-Control-Allow-Origin": origin || "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Private-Network": "true",
    "Cache-Control": "no-store"
  };
  if (request.method === "OPTIONS") {
    response.writeHead(204, headers);
    response.end();
    return;
  }
  const url = new URL(request.url || "/", "http://127.0.0.1");
  if (url.pathname === "/health") {
    sendJson(response, 200, headers, {
      ok: true,
      ...runtimeHealth()
    });
    return;
  }
  if (url.pathname === "/operator/targets" && request.method === "GET") {
    sendJson(response, 200, headers, {
      ok: true,
      attached: Boolean(operatorBridge?.open),
      targets: operatorTargets
    });
    return;
  }
  if (url.pathname === "/operator/run" && request.method === "POST") {
    await handleOperatorHttpRun(request, response, headers);
    return;
  }
  if (url.pathname === "/operator/script" && request.method === "POST") {
    await handleOperatorHttpScript(request, response, headers);
    return;
  }
  if (url.pathname === "/operator/chat" && request.method === "POST") {
    await handleOperatorHttpChat(request, response, headers);
    return;
  }
  if (url.pathname === "/operator/export" && (request.method === "GET" || request.method === "POST")) {
    await handleOperatorHttpExport(response, headers);
    return;
  }
  response.writeHead(204, headers);
  response.end();
}

function handleMessage(ws, raw) {
  let message;
  try {
    message = JSON.parse(raw);
  } catch {
    return;
  }

  if (typeof message?.type === "string" && message.type.startsWith("operator.")) {
    handleOperatorMessage(ws, message);
    return;
  }

  if (message?.type === "hello") {
    send(ws, message.id || "hello", "", undefined, "ready", {
      cwd: process.cwd(),
      ...runtimeHealth()
    });
    return;
  }

  if (message?.type === "stop" && isSafeText(message.id, 160)) {
    active.get(message.id)?.kill();
    return;
  }

  if (message?.type === "script" && isSafeText(message.id, 160) && isSafeText(message.script, maxScriptChars)) {
    void runScript(
      ws,
      message.id,
      {
        name: typeof message.name === "string" ? message.name : "script",
        shell: typeof message.shell === "string" ? message.shell : "",
        script: message.script
      },
      Number.isSafeInteger(message.timeoutMs) ? message.timeoutMs : defaultTimeoutMs
    );
    return;
  }

  if (message?.type !== "run" || !isSafeText(message.id, 160) || !isSafeText(message.command, maxCommandChars)) {
    return;
  }

  runCommand(
    ws,
    message.id,
    message.command,
    Number.isSafeInteger(message.timeoutMs) ? message.timeoutMs : defaultTimeoutMs
  );
}

function handleOperatorMessage(ws, message) {
  if (message.type === "operator.attach") {
    operatorBridge = ws;
    sendRaw(ws, { type: "operator.ready" });
    return;
  }
  if (message.type === "operator.targets" && ws === operatorBridge) {
    operatorTargets = sanitizeTargets(message.targets);
    return;
  }
  if (message.type === "operator.output" && ws === operatorBridge && isSafeText(message.id, 160)) {
    handleOperatorOutput(message);
  }
}

function cleanupOperatorSocket(ws) {
  if (operatorBridge === ws) {
    operatorBridge = null;
    operatorTargets = [];
    for (const run of operatorRuns.values()) {
      run.finish(127, "! bridge");
    }
    operatorRuns.clear();
  }
}

async function handleOperatorHttpRun(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, 16_000);
  } catch {
    sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
    return;
  }
  const target = typeof payload.target === "string" ? payload.target.slice(0, 160) : "";
  const command = typeof payload.command === "string" ? payload.command.slice(0, maxCommandChars) : "";
  const timeoutMs = Number.isSafeInteger(payload.timeoutMs) ? Math.max(1000, Math.min(payload.timeoutMs, defaultTimeoutMs)) : defaultTimeoutMs;
  if (!operatorBridge?.open || !target || !command.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  const id = registerOperatorRun(response, headers, timeoutMs);
  sendRaw(operatorBridge, {
    type: "operator.run",
    id,
    target,
    command
  });
}

async function handleOperatorHttpScript(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, 2_200_000);
  } catch {
    sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
    return;
  }
  const target = typeof payload.target === "string" ? payload.target.slice(0, 160) : "";
  const script = typeof payload.script === "string" ? payload.script.slice(0, maxScriptChars) : "";
  const name = typeof payload.name === "string" ? payload.name.slice(0, 120) : "script";
  const shell = typeof payload.shell === "string" ? payload.shell.slice(0, 40) : "";
  const timeoutMs = Number.isSafeInteger(payload.timeoutMs) ? Math.max(1000, Math.min(payload.timeoutMs, defaultTimeoutMs)) : defaultTimeoutMs;
  if (!operatorBridge?.open || !target || !script.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  const id = registerOperatorRun(response, headers, timeoutMs);
  sendRaw(operatorBridge, {
    type: "operator.script",
    id,
    target,
    name,
    shell,
    script
  });
}

async function handleOperatorHttpChat(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, 80_000);
  } catch {
    sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
    return;
  }
  const target = typeof payload.target === "string" ? payload.target.slice(0, 160) : "";
  const text = typeof payload.text === "string" ? payload.text.slice(0, maxChatChars) : "";
  const speed = typeof payload.speed === "string" ? payload.speed.slice(0, 20) : "";
  const persona = typeof payload.persona === "string" ? payload.persona.slice(0, 80) : "";
  const timeoutMs = Number.isSafeInteger(payload.timeoutMs) ? Math.max(1000, Math.min(payload.timeoutMs, defaultTimeoutMs)) : defaultTimeoutMs;
  if (!operatorBridge?.open || !target || !text.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  const id = registerOperatorRun(response, headers, timeoutMs);
  sendRaw(operatorBridge, {
    type: "operator.chat",
    id,
    target,
    text,
    speed,
    persona
  });
}

async function handleOperatorHttpExport(response, headers) {
  if (!operatorBridge?.open) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  const id = registerOperatorRun(response, headers, 60_000);
  sendRaw(operatorBridge, {
    type: "operator.export",
    id
  });
}

function registerOperatorRun(response, headers, timeoutMs) {
  const id = `operator_${randomUUID()}`;
  let body = "";
  let done = false;
  const finish = (exitCode, extraText = "") => {
    if (done) {
      return;
    }
    done = true;
    clearTimeout(timer);
    operatorRuns.delete(id);
    if (extraText) {
      body += `${body ? "\n" : ""}${extraText}`;
    }
    sendJson(response, 200, headers, {
      ok: exitCode === 0,
      text: body,
      exitCode
    });
  };
  const timer = setTimeout(() => finish(124, "! timeout"), timeoutMs);
  operatorRuns.set(id, {
    append: (text) => {
      body = `${body}${text}`.slice(-1_000_000);
    },
    finish
  });
  return id;
}

function handleOperatorOutput(message) {
  const run = operatorRuns.get(message.id);
  if (!run) {
    return;
  }
  if (typeof message.text === "string") {
    run.append(message.text);
  }
  if (typeof message.exitCode === "number") {
    run.finish(message.exitCode);
  }
}

function sanitizeTargets(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => ({
      id: typeof item?.id === "string" ? item.id.slice(0, 160) : "",
      label: typeof item?.label === "string" ? item.label.slice(0, 160) : ""
    }))
    .filter((item) => item.id && item.label)
    .slice(0, 128);
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
    cwd: process.cwd(),
    pid: child.pid || 0
  });

  const timer = setTimeout(() => {
    if (active.get(id) !== child) {
      return;
    }
    timedOut = true;
    child.kill();
    send(ws, id, "!\n", 124, "exit");
  }, Math.max(1000, timeoutMs));

  const decodeStdout = createOutputDecoder();
  const decodeStderr = createOutputDecoder();
  child.stdout.on("data", (chunk) => sendChunks(ws, id, decodeStdout(chunk)));
  child.stderr.on("data", (chunk) => sendChunks(ws, id, decodeStderr(chunk)));
  child.stdout.on("end", () => sendChunks(ws, id, decodeStdout(Buffer.alloc(0), true)));
  child.stderr.on("end", () => sendChunks(ws, id, decodeStderr(Buffer.alloc(0), true)));
  child.on("error", (error) => {
    clearTimeout(timer);
    active.delete(id);
    send(ws, id, `${error.message}\n`, 127, "error");
    ws.close(1011, "error");
  });
  child.on("close", (code) => {
    clearTimeout(timer);
    active.delete(id);
    if (!timedOut) {
      send(ws, id, "", Number.isSafeInteger(code) ? code : 0, "exit");
    }
    ws.close(1000, "done");
  });
}

async function runScript(ws, id, payload, timeoutMs) {
  const jobDir = join(tmpdir(), "soty-agent", safeFileName(id));
  await mkdir(jobDir, { recursive: true });
  const script = scriptSpec(payload, jobDir);
  try {
    await writeFile(script.path, script.content, { encoding: "utf8", mode: 0o700 });
  } catch (error) {
    send(ws, id, `${error instanceof Error ? error.message : String(error)}\n`, 127, "error");
    ws.close(1011, "error");
    await rm(jobDir, { recursive: true, force: true }).catch(() => undefined);
    return;
  }

  const child = spawn(script.file, script.args, {
    cwd: process.cwd(),
    env: process.env,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"]
  });
  active.set(id, child);
  let timedOut = false;
  send(ws, id, "", undefined, "start", {
    cwd: process.cwd(),
    pid: child.pid || 0,
    name: script.name
  });

  const finish = async () => {
    active.delete(id);
    await rm(jobDir, { recursive: true, force: true }).catch(() => undefined);
  };

  const timer = setTimeout(() => {
    if (active.get(id) !== child) {
      return;
    }
    timedOut = true;
    child.kill();
    send(ws, id, "!\n", 124, "exit");
  }, Math.max(1000, timeoutMs));

  const decodeStdout = createOutputDecoder();
  const decodeStderr = createOutputDecoder();
  child.stdout.on("data", (chunk) => sendChunks(ws, id, decodeStdout(chunk)));
  child.stderr.on("data", (chunk) => sendChunks(ws, id, decodeStderr(chunk)));
  child.stdout.on("end", () => sendChunks(ws, id, decodeStdout(Buffer.alloc(0), true)));
  child.stderr.on("end", () => sendChunks(ws, id, decodeStderr(Buffer.alloc(0), true)));
  child.on("error", (error) => {
    clearTimeout(timer);
    void finish();
    send(ws, id, `${error.message}\n`, 127, "error");
    ws.close(1011, "error");
  });
  child.on("close", (code) => {
    clearTimeout(timer);
    void finish();
    if (!timedOut) {
      send(ws, id, "", Number.isSafeInteger(code) ? code : 0, "exit");
    }
    ws.close(1000, "done");
  });
}

function sendChunks(ws, id, text) {
  for (let index = 0; index < text.length; index += maxChunkBytes) {
    send(ws, id, text.slice(index, index + maxChunkBytes));
  }
}

function send(ws, id, text, exitCode, type = "data", extra = {}) {
  if (!ws.open) {
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

function sendRaw(ws, payload) {
  if (ws?.open) {
    ws.send(JSON.stringify(payload));
  }
}

function sendJson(response, status, headers, payload) {
  response.writeHead(status, {
    ...headers,
    "Content-Type": "application/json; charset=utf-8"
  });
  response.end(JSON.stringify(payload));
}

function readJsonBody(request, maxBytes) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    request.on("data", (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        reject(new Error("large"));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on("end", () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      } catch (error) {
        reject(error);
      }
    });
    request.on("error", reject);
  });
}

async function runControlCli(args) {
  const command = args[0] || "list";
  if (command === "health") {
    const response = await fetch(`http://127.0.0.1:${port}/health`, { cache: "no-store" });
    const payload = await response.json();
    process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
    process.exit(response.ok ? 0 : 1);
  }
  if (command === "list") {
    const response = await fetch(`http://127.0.0.1:${port}/operator/targets`, { cache: "no-store" });
    const payload = await response.json();
    if (!payload.attached) {
      process.stderr.write("sotyctl: pwa bridge is not attached\n");
      process.exit(2);
    }
    for (const target of payload.targets || []) {
      process.stdout.write(`${target.label}\t${target.id}\n`);
    }
    return;
  }
  if (command === "run") {
    const target = args[1] || "";
    const remoteCommand = args.slice(2).join(" ");
    if (!target || !remoteCommand) {
      process.stderr.write("sotyctl run <target> <command>\n");
      process.exit(2);
    }
    const response = await fetch(`http://127.0.0.1:${port}/operator/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ target, command: remoteCommand })
    });
    const payload = await response.json();
    if (payload.text) {
      process.stdout.write(payload.text);
      if (!payload.text.endsWith("\n")) {
        process.stdout.write("\n");
      }
    }
    process.exit(typeof payload.exitCode === "number" ? payload.exitCode : (response.ok ? 0 : 1));
  }
  if (command === "install-machine" || command === "elevate-machine") {
    const target = args[1] || "";
    if (!target) {
      process.stderr.write("sotyctl install-machine <target>\n");
      process.exit(2);
    }
    const response = await fetch(`http://127.0.0.1:${port}/operator/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ target, command: machineInstallCommand(), timeoutMs: 60_000 })
    });
    const payload = await response.json();
    if (payload.text) {
      process.stdout.write(payload.text);
      if (!payload.text.endsWith("\n")) {
        process.stdout.write("\n");
      }
    }
    process.exit(typeof payload.exitCode === "number" ? payload.exitCode : (response.ok ? 0 : 1));
  }
  if (command === "machine-status" || command === "maintenance-status") {
    const target = args[1] || "";
    if (!target) {
      process.stderr.write("sotyctl machine-status <target>\n");
      process.exit(2);
    }
    const response = await fetch(`http://127.0.0.1:${port}/operator/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ target, command: machineStatusCommand(), timeoutMs: 20_000 })
    });
    const payload = await response.json();
    if (payload.text) {
      process.stdout.write(payload.text);
      if (!payload.text.endsWith("\n")) {
        process.stdout.write("\n");
      }
    }
    process.exit(typeof payload.exitCode === "number" ? payload.exitCode : (response.ok ? 0 : 1));
  }
  if (command === "script") {
    const target = args[1] || "";
    const filePath = args[2] || "";
    const shell = args[3] || "";
    if (!target || !filePath) {
      process.stderr.write("sotyctl script <target> <file> [shell]\n");
      process.exit(2);
    }
    const script = await readFile(filePath, "utf8");
    const response = await fetch(`http://127.0.0.1:${port}/operator/script`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ target, name: basename(filePath), shell, script })
    });
    const payload = await response.json();
    if (payload.text) {
      process.stdout.write(payload.text);
      if (!payload.text.endsWith("\n")) {
        process.stdout.write("\n");
      }
    }
    process.exit(typeof payload.exitCode === "number" ? payload.exitCode : (response.ok ? 0 : 1));
  }
  if (command === "say" || command === "chat") {
    const sayArgs = args.slice(1);
    let speed = "";
    if (sayArgs[0] === "--fast" || sayArgs[0] === "--slow") {
      speed = sayArgs.shift().slice(2);
    } else if (sayArgs[0]?.startsWith("--speed=")) {
      speed = sayArgs.shift().slice("--speed=".length);
    }
    const target = sayArgs[0] || "";
    const text = sayArgs.slice(1).join(" ");
    if (!target || !text) {
      process.stderr.write("sotyctl say [--fast|--slow|--speed=fast] <target> <text>\n");
      process.exit(2);
    }
    const response = await fetch(`http://127.0.0.1:${port}/operator/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ target, text, speed, persona: "sysadmin" })
    });
    const payload = await response.json();
    if (payload.text) {
      process.stdout.write(payload.text);
      if (!payload.text.endsWith("\n")) {
        process.stdout.write("\n");
      }
    }
    process.exit(typeof payload.exitCode === "number" ? payload.exitCode : (response.ok ? 0 : 1));
  }
  if (command === "export") {
    const filePath = args[1] || "";
    const response = await fetch(`http://127.0.0.1:${port}/operator/export`, { cache: "no-store" });
    const payload = await response.json();
    if (!payload.ok) {
      if (payload.text) {
        process.stderr.write(`${payload.text}\n`);
      }
      process.exit(typeof payload.exitCode === "number" ? payload.exitCode : 1);
    }
    const text = payload.text || "";
    if (filePath) {
      await writeFile(filePath, text, "utf8");
    } else {
      process.stdout.write(text);
      if (!text.endsWith("\n")) {
        process.stdout.write("\n");
      }
    }
    process.exit(0);
  }
  process.stderr.write("sotyctl health | list | run <target> <command> | script <target> <file> [shell] | install-machine <target> | machine-status <target> | say [--fast|--slow] <target> <text> | export [file]\n");
  process.exit(2);
}

function machineInstallCommand() {
  const installerUrl = "https://xn--n1afe0b.online/agent/install-windows.ps1";
  return [
    "$ErrorActionPreference='Stop'",
    "[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12",
    "$dir=Join-Path $env:TEMP 'soty-agent-machine'",
    "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
    "$script=Join-Path $dir 'install-windows.ps1'",
    `Invoke-WebRequest -Uri ${psQuote(installerUrl)} -UseBasicParsing -OutFile $script`,
    "$args='-NoLogo -NoProfile -ExecutionPolicy Bypass -File \"' + $script + '\" -Scope Machine -LaunchAppAtLogon'",
    "Start-Process -FilePath 'powershell.exe' -Verb RunAs -ArgumentList $args",
    "Write-Output 'soty-agent-machine:uac-started'"
  ].join("; ");
}

function machineStatusCommand() {
  return [
    "$ErrorActionPreference='Stop'",
    "try {",
    "$h=Invoke-RestMethod -Uri 'http://127.0.0.1:49424/health' -Headers @{ Origin='https://xn--n1afe0b.online' } -TimeoutSec 2",
    "$h | ConvertTo-Json -Compress",
    "} catch {",
    "$m=$_.Exception.Message.Replace('\"','')",
    "Write-Output ('{\"ok\":false,\"error\":\"' + $m + '\"}')",
    "exit 1",
    "}"
  ].join("; ");
}

function psQuote(value) {
  return `'${String(value).replace(/'/gu, "''")}'`;
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
    return { file: process.env.ComSpec || "cmd.exe", args: ["/d", "/s", "/c", `chcp 65001>nul & ${command}`] };
  }
  const file = requestedShell || "powershell.exe";
  const wrapped = `${powerShellUtf8Prelude()}; ${command}; if ($global:LASTEXITCODE -ne $null) { exit $global:LASTEXITCODE }`;
  return {
    file,
    args: ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", wrapped]
  };
}

function scriptSpec(payload, jobDir) {
  const shell = String(payload.shell || "").toLowerCase();
  const name = safeFileName(payload.name || "script");
  const base = name.replace(/\.[A-Za-z0-9]{1,8}$/u, "") || "script";
  if (shell.includes("node")) {
    const path = join(jobDir, `${base}.mjs`);
    return { name: basename(path), path, content: payload.script, file: process.execPath, args: [path] };
  }
  if (shell.includes("python")) {
    const path = join(jobDir, `${base}.py`);
    return { name: basename(path), path, content: payload.script, file: process.platform === "win32" ? "python.exe" : "python3", args: [path] };
  }
  if (process.platform === "win32") {
    if (shell.includes("cmd")) {
      const path = join(jobDir, `${base}.cmd`);
      return {
        name: basename(path),
        path,
        content: `@echo off\r\nchcp 65001>nul\r\n${payload.script}`,
        file: process.env.ComSpec || "cmd.exe",
        args: ["/d", "/s", "/c", path]
      };
    }
    const path = join(jobDir, `${base}.ps1`);
    const file = shell.includes("pwsh") ? "pwsh.exe" : (requestedShell || "powershell.exe");
    return {
      name: basename(path),
      path,
      content: `\uFEFF${powerShellUtf8Prelude()}\r\n${payload.script}`,
      file,
      args: ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", path]
    };
  }
  const path = join(jobDir, `${base}.sh`);
  const file = shell.includes("bash") ? "bash" : (requestedShell || process.env.SHELL || "/bin/sh");
  return { name: basename(path), path, content: payload.script, file, args: [path] };
}

function powerShellUtf8Prelude() {
  return "$__sotyUtf8 = New-Object System.Text.UTF8Encoding $false; [Console]::InputEncoding = $__sotyUtf8; [Console]::OutputEncoding = $__sotyUtf8; $OutputEncoding = $__sotyUtf8; chcp.com 65001 | Out-Null";
}

function shellName() {
  if (process.platform !== "win32") {
    return requestedShell || process.env.SHELL || "/bin/sh";
  }
  return requestedShell || "powershell.exe";
}

function runtimeHealth() {
  return {
    managed,
    scope: agentScope,
    platform: process.platform,
    shell: shellName(),
    version: agentVersion,
    ...(process.platform === "win32" ? {
      windowsUser: windowsUserName(),
      system: isWindowsSystem(),
      maintenance: agentScope === "Machine" && isWindowsSystem()
    } : {})
  };
}

function windowsUserName() {
  const domain = process.env.USERDOMAIN || "";
  const user = process.env.USERNAME || "";
  return domain && user ? `${domain}\\${user}` : user;
}

function isWindowsSystem() {
  return (process.env.USERNAME || "").toLowerCase() === "system";
}

function safeScope(value) {
  const text = String(value || "").trim();
  if (text === "Machine" || text === "CurrentUser" || text === "Dev") {
    return text;
  }
  return "CurrentUser";
}

function safeFileName(value) {
  return String(value || "script")
    .replace(/[^\-.0-9A-Z_a-z]/gu, "_")
    .replace(/^\.+/u, "")
    .slice(0, 80)
    || "script";
}

function originAllowed(origin) {
  return !origin
    || allowedOrigins.has(origin)
    || ((!managed || process.env.SOTY_AGENT_DEV === "1") && localDevOrigin(origin));
}

function localDevOrigin(origin) {
  return origin.startsWith("http://localhost:")
    || origin.startsWith("http://127.0.0.1:");
}

function createOutputDecoder() {
  const utf8 = new TextDecoder("utf-8");
  const oem = process.platform === "win32" ? new TextDecoder("ibm866") : null;
  let selected = "utf8";
  return (chunk, flush = false) => {
    if (selected === "oem" && oem) {
      return oem.decode(chunk, { stream: !flush });
    }
    const text = utf8.decode(chunk, { stream: !flush });
    if (oem && text.includes("\uFFFD")) {
      selected = "oem";
      return oem.decode(chunk, { stream: !flush });
    }
    return text;
  };
}

function scheduleUpdate() {
  if (!managed || !updateManifestUrl) {
    return;
  }
  const firstDelay = 8000 + Math.floor(Math.random() * 5000);
  setTimeout(() => {
    void checkForUpdate();
    setInterval(() => void checkForUpdate(), 10 * 60 * 1000);
  }, firstDelay);
}

async function checkForUpdate() {
  try {
    const response = await fetch(updateManifestUrl, { cache: "no-store" });
    if (!response.ok) {
      return;
    }
    const manifest = await response.json();
    if (!isSafeManifest(manifest)) {
      return;
    }
    if (compareVersion(manifest.version, agentVersion) <= 0) {
      return;
    }
    const scriptPath = fileURLToPath(import.meta.url);
    const currentHash = sha256(await readFile(scriptPath));
    if (manifest.sha256 === currentHash) {
      return;
    }
    const nextUrl = new URL(manifest.agentUrl, updateManifestUrl);
    const nextResponse = await fetch(nextUrl, { cache: "no-store" });
    if (!nextResponse.ok) {
      return;
    }
    const bytes = Buffer.from(await nextResponse.arrayBuffer());
    if (sha256(bytes) !== manifest.sha256) {
      return;
    }
    await mkdir(dirname(scriptPath), { recursive: true });
    const tempPath = join(dirname(scriptPath), "soty-agent.next.mjs");
    await writeFile(tempPath, bytes, { mode: 0o755 });
    await copyFile(tempPath, scriptPath);
    await rm(tempPath, { force: true });
    process.exit(75);
  } catch {
    // Updates are best-effort; the running agent must keep the tunnel useful.
  }
}

function isSafeManifest(value) {
  return value
    && typeof value === "object"
    && typeof value.version === "string"
    && value.version.length <= 40
    && typeof value.agentUrl === "string"
    && value.agentUrl.length <= 300
    && /^[a-f0-9]{64}$/u.test(value.sha256);
}

function compareVersion(left, right) {
  const leftParts = parseVersion(left);
  const rightParts = parseVersion(right);
  if (!leftParts || !rightParts) {
    return 0;
  }
  for (let index = 0; index < Math.max(leftParts.length, rightParts.length); index += 1) {
    const delta = (leftParts[index] || 0) - (rightParts[index] || 0);
    if (delta !== 0) {
      return delta > 0 ? 1 : -1;
    }
  }
  return 0;
}

function parseVersion(value) {
  const text = String(value || "").trim();
  if (!/^\d+(?:\.\d+){0,3}$/u.test(text)) {
    return null;
  }
  return text.split(".").map((part) => Number.parseInt(part, 10));
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

class LocalWebSocket {
  constructor(socket) {
    this.socket = socket;
    this.open = true;
    this.buffer = Buffer.alloc(0);
    this.onMessage = () => undefined;
    this.onClose = () => undefined;
    socket.on("data", (chunk) => this.receive(chunk));
    socket.on("close", () => {
      this.open = false;
      this.onClose();
    });
    socket.on("error", () => {
      this.open = false;
      this.onClose();
    });
  }

  send(text) {
    if (!this.open) {
      return;
    }
    this.socket.write(frameText(Buffer.from(text, "utf8")));
  }

  close(code = 1000, reason = "") {
    if (!this.open) {
      return;
    }
    const reasonBytes = Buffer.from(reason, "utf8").subarray(0, 120);
    const payload = Buffer.alloc(2 + reasonBytes.length);
    payload.writeUInt16BE(code, 0);
    reasonBytes.copy(payload, 2);
    this.socket.write(framePayload(8, payload), () => this.socket.end());
    this.open = false;
  }

  receive(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 2) {
      const first = this.buffer[0];
      const second = this.buffer[1];
      const opcode = first & 0x0f;
      const masked = (second & 0x80) !== 0;
      let length = second & 0x7f;
      let offset = 2;
      if (length === 126) {
        if (this.buffer.length < 4) {
          return;
        }
        length = this.buffer.readUInt16BE(2);
        offset = 4;
      } else if (length === 127) {
        if (this.buffer.length < 10) {
          return;
        }
        const bigLength = this.buffer.readBigUInt64BE(2);
        if (bigLength > BigInt(maxFrameBytes)) {
          this.close(1009, "large");
          return;
        }
        length = Number(bigLength);
        offset = 10;
      }
      if (length > maxFrameBytes) {
        this.close(1009, "large");
        return;
      }
      const maskOffset = offset;
      if (masked) {
        offset += 4;
      }
      if (this.buffer.length < offset + length) {
        return;
      }
      let payload = this.buffer.subarray(offset, offset + length);
      if (masked) {
        const mask = this.buffer.subarray(maskOffset, maskOffset + 4);
        payload = Buffer.from(payload.map((byte, index) => byte ^ mask[index % 4]));
      }
      this.buffer = this.buffer.subarray(offset + length);
      if (opcode === 8) {
        this.close(1000, "bye");
        return;
      }
      if (opcode === 9) {
        this.socket.write(framePayload(10, payload));
        continue;
      }
      if (opcode === 1) {
        this.onMessage(payload.toString("utf8"));
      }
    }
  }
}

function frameText(payload) {
  return framePayload(1, payload);
}

function framePayload(opcode, payload) {
  const length = payload.length;
  if (length < 126) {
    return Buffer.concat([Buffer.from([0x80 | opcode, length]), payload]);
  }
  if (length <= 0xffff) {
    const header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(length, 2);
    return Buffer.concat([header, payload]);
  }
  const header = Buffer.alloc(10);
  header[0] = 0x80 | opcode;
  header[1] = 127;
  header.writeBigUInt64BE(BigInt(length), 2);
  return Buffer.concat([header, payload]);
}
