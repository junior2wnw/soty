#!/usr/bin/env node
import { execFileSync, spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { copyFile, cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const agentVersion = "0.3.42";
const scriptPath = fileURLToPath(import.meta.url);
const agentDir = dirname(scriptPath);
const agentConfigPath = join(agentDir, "agent-config.json");
const persistedAgentConfig = loadAgentConfig();
const port = Number.parseInt(arg("--port") || process.env.SOTY_AGENT_PORT || "49424", 10);
const defaultTimeoutMs = Number.parseInt(arg("--timeout") || process.env.SOTY_AGENT_TIMEOUT_MS || "600000", 10);
const requestedShell = arg("--shell") || process.env.SOTY_AGENT_SHELL || "";
const codexSandbox = process.env.SOTY_CODEX_SANDBOX || "danger-full-access";
const codexReplyReasoningEffort = (process.env.SOTY_CODEX_REASONING_EFFORT || "low").trim();
const codexReplyModel = (process.env.SOTY_CODEX_MODEL || "").trim();
const codexReplyEphemeral = process.env.SOTY_CODEX_EPHEMERAL !== "0";
const codexDisableGithubPlugin = process.env.SOTY_CODEX_DISABLE_GITHUB_PLUGIN !== "0";
const updateManifestUrl = arg("--update-url") || process.env.SOTY_AGENT_UPDATE_URL || "https://xn--n1afe0b.online/agent/manifest.json";
let agentRelayId = safeRelayId(arg("--relay-id") || process.env.SOTY_AGENT_RELAY_ID || persistedAgentConfig.relayId || "");
let agentRelayBaseUrl = safeHttpBaseUrl(process.env.SOTY_AGENT_RELAY_URL || persistedAgentConfig.relayBaseUrl || originFromUrl(updateManifestUrl) || "https://xn--n1afe0b.online");
const managed = process.argv.includes("--managed") || process.env.SOTY_AGENT_MANAGED === "1";
const agentScope = safeScope(process.env.SOTY_AGENT_SCOPE || (managed ? "CurrentUser" : "Dev"));
const maxCommandChars = 8_000;
const maxScriptChars = 1_000_000;
const maxChatChars = 12_000;
const maxImportChars = 2_000_000;
const maxChunkBytes = 12_000;
const maxFrameBytes = 2_500_000;
const maxSourceChars = 180;
const agentReplyTimeoutMs = Number.parseInt(process.env.SOTY_CODEX_REPLY_TIMEOUT_MS || "300000", 10);
const skillSyncRepoUrl = process.env.SOTY_CODEX_SKILL_SYNC_REPO || "https://github.com/junior2wnw/universal-install-ops-skill.git";
const skillSyncRef = process.env.SOTY_CODEX_SKILL_SYNC_REF || "main";
const skillSyncName = process.env.SOTY_CODEX_SKILL_SYNC_NAME || "universal-install-ops";
const skillSyncIntervalMs = Number.parseInt(process.env.SOTY_CODEX_SKILL_SYNC_INTERVAL_MS || "0", 10);
const active = new Map();
const operatorRuns = new Map();
const operatorMessages = [];
const operatorMessageWaiters = new Set();
let operatorBridge = null;
let operatorTargets = [];
let operatorDeviceId = "";
let operatorDeviceNick = "";
let cachedWindowsWhoami = "";
let cachedCodexProbeAt = 0;
let cachedCodexAvailable = false;
let skillSyncInFlight = null;
let lastSkillSyncAt = 0;
let lastSkillSyncStatus = { ok: false, detail: "not-run", revision: "", at: 0 };
let agentRelayStarted = false;
const allowedOrigins = new Set([
  "https://xn--n1afe0b.online",
]);

function loadAgentConfig() {
  try {
    const parsed = JSON.parse(readFileSync(agentConfigPath, "utf8"));
    return {
      relayId: typeof parsed?.relayId === "string" ? parsed.relayId : "",
      relayBaseUrl: typeof parsed?.relayBaseUrl === "string" ? parsed.relayBaseUrl : ""
    };
  } catch {
    return { relayId: "", relayBaseUrl: "" };
  }
}

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
    startAgentRelay();
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
  if (url.pathname === "/operator/agent-message" && request.method === "POST") {
    await handleOperatorHttpAgentMessage(request, response, headers);
    return;
  }
  if (url.pathname === "/operator/messages" && request.method === "GET") {
    handleOperatorHttpMessages(url, response, headers);
    return;
  }
  if (url.pathname === "/agent/reply" && request.method === "POST") {
    await handleAgentReply(request, response, headers);
    return;
  }
  if (url.pathname === "/agent/relay" && request.method === "POST") {
    await handleAgentRelayBind(request, response, headers);
    return;
  }
  if (url.pathname === "/operator/access" && request.method === "POST") {
    await handleOperatorHttpAccess(request, response, headers);
    return;
  }
  if (url.pathname === "/operator/export" && (request.method === "GET" || request.method === "POST")) {
    await handleOperatorHttpExport(response, headers);
    return;
  }
  if (url.pathname === "/operator/import" && request.method === "POST") {
    await handleOperatorHttpImport(request, response, headers);
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
    killProcessTree(active.get(message.id));
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
    operatorDeviceId = safeSourceText(message.deviceId);
    operatorDeviceNick = safeSourceText(message.deviceNick);
    return;
  }
  if (message.type === "operator.message" && ws === operatorBridge) {
    handleOperatorIncomingMessage(message);
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
    operatorDeviceId = "";
    operatorDeviceNick = "";
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
  let target = typeof payload.target === "string" ? payload.target.slice(0, 160) : "";
  let sourceDeviceId = typeof payload.sourceDeviceId === "string" ? payload.sourceDeviceId.slice(0, maxSourceChars) : "";
  const command = typeof payload.command === "string" ? payload.command.slice(0, maxCommandChars) : "";
  const timeoutMs = Number.isSafeInteger(payload.timeoutMs) ? Math.max(1000, Math.min(payload.timeoutMs, defaultTimeoutMs)) : defaultTimeoutMs;
  if (isAgentSourceTarget(target)) {
    const deviceId = agentSourceDeviceId(target);
    if (sourceDeviceId && sourceDeviceId !== deviceId) {
      sendJson(response, 403, headers, { ok: false, text: "! source-target", exitCode: 403 });
      return;
    }
    const resolved = resolveOperatorTargetForSourceDevice(deviceId);
    if (resolved && operatorBridge?.open) {
      target = resolved.id;
      sourceDeviceId = deviceId;
    } else {
      await handleAgentSourceHttpRun(target, sourceDeviceId, command, timeoutMs, response, headers);
      return;
    }
  }
  if (!operatorBridge?.open || !target || !command.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  const id = registerOperatorRun(response, headers, timeoutMs);
  sendRaw(operatorBridge, {
    type: "operator.run",
    id,
    target,
    sourceDeviceId,
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
  let target = typeof payload.target === "string" ? payload.target.slice(0, 160) : "";
  let sourceDeviceId = typeof payload.sourceDeviceId === "string" ? payload.sourceDeviceId.slice(0, maxSourceChars) : "";
  const script = typeof payload.script === "string" ? payload.script.slice(0, maxScriptChars) : "";
  const name = typeof payload.name === "string" ? payload.name.slice(0, 120) : "script";
  const shell = typeof payload.shell === "string" ? payload.shell.slice(0, 40) : "";
  const timeoutMs = Number.isSafeInteger(payload.timeoutMs) ? Math.max(1000, Math.min(payload.timeoutMs, defaultTimeoutMs)) : defaultTimeoutMs;
  if (isAgentSourceTarget(target)) {
    const deviceId = agentSourceDeviceId(target);
    if (sourceDeviceId && sourceDeviceId !== deviceId) {
      sendJson(response, 403, headers, { ok: false, text: "! source-target", exitCode: 403 });
      return;
    }
    const resolved = resolveOperatorTargetForSourceDevice(deviceId);
    if (resolved && operatorBridge?.open) {
      target = resolved.id;
      sourceDeviceId = deviceId;
    } else {
      await handleAgentSourceHttpScript(target, sourceDeviceId, { script, name, shell }, timeoutMs, response, headers);
      return;
    }
  }
  if (!operatorBridge?.open || !target || !script.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  const id = registerOperatorRun(response, headers, timeoutMs);
  sendRaw(operatorBridge, {
    type: "operator.script",
    id,
    target,
    sourceDeviceId,
    name,
    shell,
    script
  });
}

async function handleAgentSourceHttpRun(target, sourceDeviceId, command, timeoutMs, response, headers) {
  const deviceId = agentSourceDeviceId(target);
  if (!deviceId || !command.trim()) {
    sendJson(response, 400, headers, { ok: false, text: "! request", exitCode: 400 });
    return;
  }
  if (sourceDeviceId && sourceDeviceId !== deviceId) {
    sendJson(response, 403, headers, { ok: false, text: "! source-target", exitCode: 403 });
    return;
  }
  const result = await postAgentSourceJob("/api/agent/source/run", {
    deviceId,
    command,
    timeoutMs
  });
  rememberAgentSourceOutcome({ kind: "run", command, result });
  sendJson(response, 200, headers, result);
}

async function handleAgentSourceHttpScript(target, sourceDeviceId, payload, timeoutMs, response, headers) {
  const deviceId = agentSourceDeviceId(target);
  if (!deviceId || !String(payload.script || "").trim()) {
    sendJson(response, 400, headers, { ok: false, text: "! request", exitCode: 400 });
    return;
  }
  if (sourceDeviceId && sourceDeviceId !== deviceId) {
    sendJson(response, 403, headers, { ok: false, text: "! source-target", exitCode: 403 });
    return;
  }
  const result = await postAgentSourceJob("/api/agent/source/script", {
    deviceId,
    ...payload,
    timeoutMs
  });
  rememberAgentSourceOutcome({ kind: "script", command: payload.script, result });
  sendJson(response, 200, headers, result);
}

async function postAgentSourceJob(path, body) {
  const relayBaseUrl = agentRelayBaseUrl || originFromUrl(updateManifestUrl);
  if (!relayBaseUrl || !agentRelayId) {
    return { ok: false, text: "! relay", exitCode: 409 };
  }
  try {
    const response = await fetch(new URL(path, relayBaseUrl), {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        relayId: agentRelayId,
        ...body
      })
    });
    const payload = await response.json();
    return {
      ok: Boolean(response.ok && payload?.ok),
      text: String(payload?.text || "").slice(0, maxChatChars),
      exitCode: Number.isSafeInteger(payload?.exitCode) ? payload.exitCode : (response.ok ? 0 : response.status)
    };
  } catch {
    return { ok: false, text: "! agent-source", exitCode: 127 };
  }
}

function rememberAgentSourceOutcome({ kind, command, result }) {
  if (process.env.SOTY_AGENT_REMEMBER_OUTCOMES === "0") {
    return;
  }
  const family = classifySourceCommand(command);
  const exitCode = Number.isSafeInteger(result?.exitCode) ? result.exitCode : (result?.ok ? 0 : 1);
  const ok = Boolean(result?.ok && exitCode === 0);
  if (ok && family === "generic") {
    return;
  }
  if (ok && family === "identity-probe") {
    return;
  }
  const opsPath = findOpsScriptPath();
  if (!opsPath) {
    return;
  }
  const remember = ok
    ? `Soty Agent ${family} ${kind} succeeded through source-scoped ctl; future low-risk reversible ${family} tasks should use one agent-source action+readback command and answer plainly.`
    : `Soty Agent source-scoped ${family} ${kind} failed; keep retries source-scoped, do not switch by label to another device, and report the exact Soty route blocker.`;
  const proof = ok
    ? `exitCode=0; family=${family}; output=${sourceOutputShape(result?.text)}`
    : `exitCode=${exitCode}; proof=${sourceFailureProof(result?.text)}`;
  const env = `soty-agent ${agentVersion}; target=agent-source; sourceDevice=present; kind=${kind}`;
  spawnDetached(process.env.SOTY_PYTHON || process.env.PYTHON || "python", [
    opsPath,
    "Soty Agent source-scoped command outcome",
    "--remember",
    remember,
    "--proof",
    proof,
    "--env",
    env
  ], dirname(opsPath));
}

function classifySourceCommand(command) {
  const lower = String(command || "").toLowerCase();
  if (/\b(whoami|hostname)\b|computername|username/u.test(lower)) {
    return "identity-probe";
  }
  if (/volume|mute|audio|sound|endpointvolume|nircmd|sndvol|speaker|mic|микрофон|звук|громк/u.test(lower)) {
    return /mute|muted|выключ/u.test(lower) ? "audio-mute" : "audio-volume";
  }
  if (/driver|pnputil|devmgmt|device manager|драйвер/u.test(lower)) {
    return "driver-check";
  }
  if (/reinstall|reset this pc|windows reset|winre|recovery|bcd|boot\.wim|setupcomplete|переустанов|сброс|восстановлен|вернуть компьютер|удалить всё|удалить все/u.test(lower)) {
    return "windows-reinstall";
  }
  if (/battery|powercfg|sleep|lid|заряд|питан/u.test(lower)) {
    return "power-check";
  }
  if (/winget|choco|scoop|msiexec|install|установ/u.test(lower)) {
    return "package-install";
  }
  if (/get-service|systemctl|service|служб/u.test(lower)) {
    return "service-check";
  }
  return "generic";
}

function sourceOutputShape(text) {
  const value = String(text || "");
  const volume = value.match(/\b(volume|vol|громкость)\s*[:=]\s*([0-9]{1,3})\b/iu);
  const muted = value.match(/\b(muted|mute)\s*[:=]\s*(true|false|0|1)\b/iu);
  const parts = [
    volume ? `volume=${volume[2]}` : "",
    muted ? `muted=${muted[2]}` : "",
    value.trim() ? "nonempty" : "empty"
  ].filter(Boolean);
  return parts.join("; ");
}

function sourceFailureProof(text) {
  const value = String(text || "");
  const known = value.match(/!\s*(target|bridge|source-target|access|tunnel|timeout|agent-source|relay|request)\b/iu);
  if (known) {
    return `! ${known[1].toLowerCase()}`;
  }
  return value.trim() ? "nonzero-output" : "empty-output";
}

function findOpsScriptPath() {
  const explicit = process.env.SOTY_OPS_SCRIPT || "";
  const codexHome = chooseCodexHome();
  const candidates = [
    explicit,
    codexHome ? join(codexHome, "skills", skillSyncName, "scripts", "ops.py") : "",
    join(agentDir, "skill-sources", "universal-install-ops-skill", "scripts", "ops.py")
  ].filter(Boolean);
  return candidates.find((candidate) => existsSync(candidate)) || "";
}

function spawnDetached(file, args, cwd) {
  try {
    const child = spawn(file, args, {
      cwd,
      detached: true,
      windowsHide: true,
      stdio: "ignore"
    });
    child.unref();
  } catch {
    // Learning must never block the user's command route.
  }
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
  if (!operatorBridge?.open || !target || !text.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  if (!hasKnownOperatorTarget(target)) {
    sendJson(response, 404, headers, { ok: false, text: "! target", exitCode: 404 });
    return;
  }
  const id = `operator_${randomUUID()}`;
  sendRaw(operatorBridge, {
    type: "operator.chat",
    id,
    target,
    text,
    speed,
    persona
  });
  sendJson(response, 200, headers, { ok: true, text: "queued\n", exitCode: 0, id });
}

async function handleOperatorHttpAgentMessage(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, 80_000);
  } catch {
    sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
    return;
  }
  const target = typeof payload.target === "string" ? payload.target.slice(0, 160) : "";
  const text = typeof payload.text === "string" ? payload.text.slice(0, maxChatChars) : "";
  const timeoutMs = Number.isSafeInteger(payload.timeoutMs) ? Math.max(1000, Math.min(payload.timeoutMs, defaultTimeoutMs)) : defaultTimeoutMs;
  if (!operatorBridge?.open || !text.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  const id = registerOperatorRun(response, headers, timeoutMs);
  sendRaw(operatorBridge, {
    type: "operator.agent-message",
    id,
    target,
    text
  });
}

function handleOperatorHttpMessages(url, response, headers) {
  const target = url.searchParams.get("target") || "";
  const after = url.searchParams.get("after") || "";
  const wait = url.searchParams.get("wait") === "1";
  const messages = filterOperatorMessages(target, after);
  if (messages.length > 0 || !wait) {
    sendJson(response, 200, headers, { ok: true, messages });
    return;
  }
  const waiter = {
    target,
    after,
    response,
    headers,
    timer: setTimeout(() => {
      operatorMessageWaiters.delete(waiter);
      sendJson(response, 200, headers, { ok: true, messages: [] });
    }, 30000)
  };
  response.on("close", () => {
    clearTimeout(waiter.timer);
    operatorMessageWaiters.delete(waiter);
  });
  operatorMessageWaiters.add(waiter);
}

async function handleOperatorHttpAccess(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, 16_000);
  } catch {
    sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
    return;
  }
  const target = typeof payload.target === "string" ? payload.target.slice(0, 160) : "";
  if (!operatorBridge?.open || !target) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  if (!hasKnownOperatorTarget(target)) {
    sendJson(response, 404, headers, { ok: false, text: "! target", exitCode: 404 });
    return;
  }
  const id = registerOperatorRun(response, headers, 20_000);
  sendRaw(operatorBridge, {
    type: "operator.access",
    id,
    target
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

async function handleOperatorHttpImport(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, maxImportChars + 1000);
  } catch {
    sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
    return;
  }
  const text = typeof payload.text === "string" ? payload.text.slice(0, maxImportChars) : "";
  if (!operatorBridge?.open || !text.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  const id = registerOperatorRun(response, headers, 60_000);
  sendRaw(operatorBridge, {
    type: "operator.import",
    id,
    text
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

function handleOperatorIncomingMessage(message) {
  const text = typeof message.text === "string" ? message.text.slice(0, maxChatChars) : "";
  const target = typeof message.target === "string" ? message.target.slice(0, 160) : "";
  if (!target || !text.trim()) {
    return;
  }
  const item = {
    id: typeof message.id === "string" && message.id.length > 0 && message.id.length <= 160 ? message.id : `operator_message_${randomUUID()}`,
    target,
    label: typeof message.label === "string" ? message.label.slice(0, 160) : "",
    text,
    createdAt: typeof message.createdAt === "string" && message.createdAt.length <= 80 ? message.createdAt : new Date().toISOString()
  };
  operatorMessages.push(item);
  while (operatorMessages.length > 500) {
    operatorMessages.shift();
  }
  flushOperatorMessageWaiters();
}

function filterOperatorMessages(target, after) {
  const targetNeedle = String(target || "").trim().toLowerCase();
  let messages = operatorMessages;
  if (after) {
    const index = messages.findIndex((item) => item.id === after);
    messages = index >= 0 ? messages.slice(index + 1) : messages;
  }
  if (!targetNeedle) {
    return messages;
  }
  return messages.filter((item) => item.target === target
    || item.target.toLowerCase() === targetNeedle
    || item.label.toLowerCase() === targetNeedle
    || item.label.toLowerCase().includes(targetNeedle));
}

function flushOperatorMessageWaiters() {
  for (const waiter of [...operatorMessageWaiters]) {
    const messages = filterOperatorMessages(waiter.target, waiter.after);
    if (messages.length === 0) {
      continue;
    }
    clearTimeout(waiter.timer);
    operatorMessageWaiters.delete(waiter);
    sendJson(waiter.response, 200, waiter.headers, { ok: true, messages });
  }
}

async function handleAgentReply(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, 120_000);
  } catch {
    sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
    return;
  }
  const text = typeof payload.text === "string" ? payload.text.slice(0, maxChatChars) : "";
  const context = typeof payload.context === "string" ? payload.context.slice(-16_000) : "";
  const source = sanitizeAgentSource(payload.source);
  if (!text.trim()) {
    sendJson(response, 400, headers, { ok: false, text: "! text", exitCode: 400 });
    return;
  }
  const result = await askCodexForAgentReply(text, context, source);
  sendJson(response, result.ok ? 200 : 502, headers, result);
}

async function handleAgentRelayBind(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, 16_000);
  } catch {
    sendJson(response, 400, headers, { ok: false });
    return;
  }
  const relayId = safeRelayId(payload?.relayId || "");
  const relayBaseUrl = safeHttpBaseUrl(payload?.relayBaseUrl || originFromUrl(updateManifestUrl) || "https://xn--n1afe0b.online");
  if (!relayId || !relayBaseUrl) {
    sendJson(response, 400, headers, { ok: false });
    return;
  }
  agentRelayId = relayId;
  agentRelayBaseUrl = relayBaseUrl;
  await writeFile(agentConfigPath, JSON.stringify({ relayId, relayBaseUrl }, null, 2), "utf8")
    .catch(() => undefined);
  startAgentRelay();
  sendJson(response, 200, headers, {
    ok: true,
    ...runtimeHealth()
  });
}

function startAgentRelay() {
  if (!agentRelayId || !agentRelayBaseUrl) {
    return;
  }
  if (agentRelayStarted) {
    return;
  }
  agentRelayStarted = true;
  void runAgentRelayLoop();
}

async function runAgentRelayLoop() {
  let retryMs = 1000;
  while (true) {
    try {
      const jobs = await pollAgentRelay();
      retryMs = 1000;
      for (const job of jobs) {
        await handleAgentRelayJob(job);
      }
    } catch {
      await sleep(retryMs);
      retryMs = Math.min(30_000, Math.round(retryMs * 1.6));
    }
  }
}

async function pollAgentRelay() {
  const url = new URL("/api/agent/relay/poll", agentRelayBaseUrl);
  url.searchParams.set("relayId", agentRelayId);
  url.searchParams.set("version", agentVersion);
  url.searchParams.set("codex", hasCodexBinary() ? "1" : "0");
  if (operatorDeviceId) {
    url.searchParams.set("deviceId", operatorDeviceId);
  }
  if (operatorDeviceNick) {
    url.searchParams.set("deviceNick", operatorDeviceNick);
  }
  url.searchParams.set("wait", "1");
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`relay poll ${response.status}`);
  }
  const payload = await response.json();
  return Array.isArray(payload.jobs)
    ? payload.jobs.filter((job) => isSafeText(job?.id, 160) && isSafeText(job?.text, maxChatChars))
    : [];
}

async function handleAgentRelayJob(job) {
  const result = await askCodexForAgentReply(
    String(job.text || "").slice(0, maxChatChars),
    String(job.context || "").slice(-16_000),
    sanitizeAgentSource(job.source)
  );
  await postAgentRelayReply(job.id, result);
}

async function postAgentRelayReply(id, result) {
  const response = await fetch(new URL("/api/agent/relay/reply", agentRelayBaseUrl), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      relayId: agentRelayId,
      id,
      ok: Boolean(result.ok),
      text: String(result.text || "").slice(0, maxChatChars),
      ...(typeof result.exitCode === "number" ? { exitCode: result.exitCode } : {})
    })
  });
  if (!response.ok) {
    throw new Error(`relay reply ${response.status}`);
  }
}

async function askCodexForAgentReply(text, context, source = {}) {
  const codexBin = findCodexBinary();
  if (!codexBin) {
    const relay = await askCodexRelayFallback(text, context, source);
    if (relay) {
      return relay;
    }
    return {
      ok: false,
      text: "Я вижу сообщение, но серверный мост сейчас не нашел подключенный Codex-исполнитель. На удаленном устройстве Codex не нужен: проверь, что Soty и Soty Agent запущены на компьютере-операторе, где открыт рабочий Codex.",
      exitCode: 126
    };
  }

  const jobDir = join(tmpdir(), "soty-agent-codex", randomUUID());
  const outPath = join(jobDir, "reply.txt");
  await mkdir(jobDir, { recursive: true });
  const codexHome = chooseCodexHome();
  await maybeSyncCodexSkills(codexHome);
  const childEnv = {
    ...process.env,
    ...(codexHome ? { CODEX_HOME: codexHome } : {})
  };
  const args = [
    "exec",
    "--skip-git-repo-check",
    "--sandbox",
    codexSandbox,
    "--cd",
    process.env.SOTY_CODEX_CWD || process.cwd(),
    "-o",
    outPath
  ];
  if (codexReplyEphemeral) {
    args.push("--ephemeral");
  }
  if (codexReplyModel) {
    args.push("--model", codexReplyModel);
  }
  if (codexReplyReasoningEffort && codexReplyReasoningEffort !== "inherit") {
    args.push("-c", `model_reasoning_effort=${JSON.stringify(codexReplyReasoningEffort)}`);
  }
  if (codexDisableGithubPlugin) {
    args.push("-c", 'plugins."github@openai-curated".enabled=false');
  }
  args.push("-");

  try {
    const prompt = await buildAgentPrompt(text, context, source);
    const result = await runChildForText(codexBin, args, childEnv, agentReplyTimeoutMs, prompt);
    const reply = existsSync(outPath) ? (await readFile(outPath, "utf8")).trim() : "";
    if (result.exitCode === 0 && reply) {
      return { ok: true, text: cleanAgentChatReply(reply).slice(0, maxChatChars), exitCode: 0 };
    }
    return {
      ok: false,
      text: agentFailureText(`${result.stderr}\n${result.stdout}`),
      exitCode: result.exitCode || 1
    };
  } catch (error) {
    return {
      ok: false,
      text: agentFailureText(error instanceof Error ? error.message : String(error)),
      exitCode: 1
    };
  } finally {
    await rm(jobDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

function cleanAgentChatReply(value) {
  return String(value || "")
    .replace(/\r\n?/gu, "\n")
    .split("\n")
    .filter((line) => !isInternalAgentReceiptLine(line))
    .join("\n")
    .replace(/\n{3,}/gu, "\n\n")
    .trim();
}

function isInternalAgentReceiptLine(line) {
  const text = String(line || "").trim();
  return /^`?(learning_delta|proof|final_line|finish_skill_edit)\s*=/iu.test(text)
    || /^`?ops-memory\s*:/iu.test(text)
    || /^ops:\s*`?(learning_delta|proof|final_line)\s*=/iu.test(text);
}

async function askCodexRelayFallback(text, context, source = {}) {
  const relayBaseUrl = agentRelayBaseUrl || originFromUrl(updateManifestUrl);
  if (!relayBaseUrl) {
    return null;
  }
  const requestRelayId = agentRelayId || await currentCodexRelayId(relayBaseUrl);
  if (!requestRelayId) {
    return null;
  }
  try {
    const request = await fetch(new URL("/api/agent/relay/request", relayBaseUrl), {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        relayId: requestRelayId,
        text: String(text || "").slice(0, maxChatChars),
        context: String(context || "").slice(-16_000),
        source: sanitizeAgentSource(source)
      })
    });
    const created = await request.json();
    if (!request.ok || !created?.ok || !isSafeText(created.id, 160)) {
      return null;
    }
    const replyRelayId = safeRelayId(created.relayId || requestRelayId);
    return await waitForCodexRelayFallbackReply(relayBaseUrl, replyRelayId, created.id, agentReplyTimeoutMs);
  } catch {
    return null;
  }
}

async function currentCodexRelayId(relayBaseUrl) {
  try {
    const response = await fetch(new URL("/api/agent/relay/current", relayBaseUrl), { cache: "no-store" });
    const payload = await response.json();
    if (!response.ok || !payload?.connected || payload.codex === false) {
      return "";
    }
    return safeRelayId(payload.relayId || "");
  } catch {
    return "";
  }
}

async function waitForCodexRelayFallbackReply(relayBaseUrl, relayId, id, timeoutMs) {
  if (!relayId || !id) {
    return null;
  }
  const deadline = Date.now() + Math.max(5000, timeoutMs || 120000);
  while (Date.now() < deadline) {
    const url = new URL("/api/agent/relay/reply", relayBaseUrl);
    url.searchParams.set("relayId", relayId);
    url.searchParams.set("id", id);
    url.searchParams.set("wait", "1");
    try {
      const response = await fetch(url, { cache: "no-store" });
      if (!response.ok) {
        return null;
      }
      const payload = await response.json();
      if (payload?.reply) {
        return {
          ok: Boolean(payload.reply.ok),
          text: cleanAgentChatReply(payload.reply.text || "").slice(0, maxChatChars),
          ...(Number.isSafeInteger(payload.reply.exitCode) ? { exitCode: payload.reply.exitCode } : {})
        };
      }
    } catch {
      // Keep waiting until the outer timeout; transient network switches are common on remote devices.
    }
  }
  return null;
}

async function buildAgentPrompt(text, context, source = {}) {
  const trimmedContext = String(context || "").trim();
  const sourceContext = formatAgentSource(source);
  const operatorContext = formatOperatorTargets(source);
  const lifecycleGuidance = lifecycleOpsGuidance(text, trimmedContext);
  const deterministicFacts = await collectDeterministicAgentFacts(text, trimmedContext, source);
  return [
    "You are the local Codex agent inside the Soty app.",
    "Answer in Russian. Keep it simple, warm, and clear. Usually 2-6 sentences.",
    "Treat simple greetings and small talk as valid conversation: answer warmly in Russian, then gently ask what the user wants to do next only if useful.",
    "This is a chat mode through a local bridge: do not run commands or edit files unless the user explicitly asks.",
    "Use the request source below to understand which Soty device contacted you. Do not assume the current Codex host, the last selected Soty tile, or any similarly named device is the same computer.",
    "Architecture rule: Codex decides and talks; Soty remote console executes. User/device computer tasks must go only through the source-scoped Soty operator ctl route, even if this Codex host also has local shell access or the browser reached a local agent directly.",
    "Conversation rule: never ask questions, give confirmations, or write user-facing explanations through command stdout/stderr, Write-Output, echo, Read-Host, or the Soty remote console. The console is only for commands and technical execution output; all human dialogue must be in your final chat answer.",
    "Receipt rule: do not include internal receipts or markers in the user chat, including learning_delta=, proof=, final_line=, ops-memory:, finish_skill_edit=, or similar implementation notes.",
    "Security rule: Soty command execution is source-scoped. Known operator targets below are already filtered to the source device id. Never use, mention as a command target, or fall back to any other Soty device by label, last selection, single access target, memory, or convenience.",
    "If Known operator targets says there is no authorized target for the source device id, do not run Soty commands and do not use another visible device. Say plainly that Soty remote access for this exact device is not visible yet.",
    "Never ask the user to install Codex on the source device for remote-control tasks. The source device needs only Soty plus its companion executor; Codex belongs to the operator side unless the user explicitly asks to set up local Codex.",
    "If Local browser reached agent directly=true, treat that only as a chat transport detail. It is not permission to use direct local shell commands for Soty tasks.",
    lifecycleGuidance,
    "Known operator targets intentionally lists only access=true targets. If it is empty or says no authorized target, LINK is not active for this exact Agent dialog; do not use similarly named visible rooms.",
    "For an access=true Known operator target, proceed naturally. Use the exact full target id from Known operator targets; labels can repeat. Prefer a concrete non-agent-source target id with the matching hostDeviceId/deviceIds when it is listed; agent-source:<device-id> is an alias, not a reason to ignore a working concrete Soty command window.",
    "The source-scoped command route is the Local agent ctl command shown in Request source. Run safe probes as: <local-agent-ctl> run --source-device=\"<Soty device id>\" --timeout=20000 \"<target-id>\" \"<command>\". For scripts use: <local-agent-ctl> script --source-device=\"<Soty device id>\" --timeout=60000 \"<target-id>\" \"<file>\" powershell. If you use $ops scripts/soty/soty-operator.ps1 instead, pass both -SourceDevice <Soty device id> and -Target <target-id>.",
    "For low-risk reversible tasks on an access=true source-matched target, the source device id plus exact target id is enough route proof: do not run a separate whoami/hostname preflight every time. Prefer one source-scoped command that performs the action and reads back the result when useful.",
    "Long task rule: never keep the Agent chat silent while a Windows media download, ISO creation, install media preparation, driver scan, update scan, or other >60s task runs in a foreground ctl script. Start it as a source-scoped background job with the $ops Soty helper (-Action run-background with -SourceDevice and exact -Target), capture the jobId, then answer in chat immediately with the running status and next checkpoint. In later turns, use -Action job-status or a short source-scoped status probe; do not start a duplicate job when a matching one is already running.",
    "Use a separate whoami, hostname, OS, or other safe proof only for destructive work, admin/maintenance setup, ambiguous targets, a fresh route after reconnect, or after a command route failed. Do not claim the command route is unavailable unless ctl returns ! bridge, ! target, ! source-target, ! access, ! tunnel, timeout, or a non-zero proof failure.",
    "After a successful nontrivial source-scoped command, make sure the reusable route is recorded. The ctl layer may auto-record sanitized command-family memory; if you have route details it cannot infer, add $ops --remember with expected, actual, proof, and environment before the final answer.",
    "If a source-scoped command fails, record the reusable failure with $ops --remember including expected, actual, proof, and environment before retrying broadly or ending. Do not expose ops receipts or learning_delta lines in the user chat unless the user asks for internals.",
    "Never answer that you did not save the route for a command you just ran successfully. If memory recording was automatic, say that the reusable route was captured without exposing the receipt.",
    "For destructive work like Windows reset or reinstall, always use safe proof commands first, then ask for one final confirmation before any destructive step. Do not fall back to manual Windows settings while an access=true ctl route is still untested.",
    "If the user asks for IDE work on the Codex host, briefly acknowledge the task and explain that real code changes need the full Codex session in the IDE or a working backend for codex exec.",
    sourceContext ? `Request source:\n${sourceContext}` : "",
    operatorContext ? `Known operator targets:\n${operatorContext}` : "",
    deterministicFacts ? `Deterministic source facts:\n${deterministicFacts}` : "",
    trimmedContext ? `Recent chat context as a JSON string with Unicode escapes. Decode it before using it:\n${asciiJsonString(trimmedContext)}` : "",
    `User message as a JSON string with Unicode escapes. Decode it before answering:\n${asciiJsonString(String(text || "").trim())}`,
    "Answer in Russian:"
  ].filter(Boolean).join("\n\n");
}

async function collectDeterministicAgentFacts(text, context, source = {}) {
  if (!isWindowsReinstallRequest(text, context)) {
    return "";
  }
  const safe = sanitizeAgentSource(source);
  if (!safe.deviceId) {
    return "windowsReinstallPreflight=not-run; reason=no-source-device-id";
  }
  const target = sourceMatchedOperatorTargets(source)[0] || {
    id: `agent-source:${safe.deviceId}`,
    label: "source device",
    deviceIds: [safe.deviceId],
    hostDeviceId: safe.deviceId,
    access: true
  };
  const result = await postLocalOperatorRun(target.id, safe.deviceId, windowsReinstallPreflightCommand(), 45_000);
  return formatWindowsReinstallPreflightFacts(result);
}

async function postLocalOperatorRun(target, sourceDeviceId, command, timeoutMs) {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/operator/run`, {
      method: "POST",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://xn--n1afe0b.online"
      },
      body: JSON.stringify({
        target,
        sourceDeviceId,
        command,
        timeoutMs
      })
    });
    const payload = await response.json();
    return {
      ok: Boolean(response.ok && payload?.ok),
      text: String(payload?.text || ""),
      ...(Number.isSafeInteger(payload?.exitCode) ? { exitCode: payload.exitCode } : { exitCode: response.status || 1 })
    };
  } catch (error) {
    return {
      ok: false,
      text: error instanceof Error ? error.message : String(error),
      exitCode: 1
    };
  }
}

function windowsReinstallPreflightCommand() {
  return [
    "$ErrorActionPreference='Continue'",
    "$ProgressPreference='SilentlyContinue'",
    "$health=$null",
    "try { $health=Invoke-RestMethod -Uri 'http://127.0.0.1:49424/health' -Headers @{ Origin='https://xn--n1afe0b.online' } -TimeoutSec 2 } catch {}",
    "$os=$null",
    "try { $os=Get-CimInstance Win32_OperatingSystem | Select-Object Caption,Version,BuildNumber,CSName } catch {}",
    "$computer=$null",
    "try { $computer=Get-CimInstance Win32_ComputerSystem | Select-Object Name,Manufacturer,Model,PCSystemType } catch {}",
    "$battery=@()",
    "try { $battery=@(Get-CimInstance Win32_Battery | Select-Object Name,BatteryStatus,EstimatedChargeRemaining) } catch {}",
    "$volumes=@()",
    "try { $volumes=@(Get-CimInstance Win32_LogicalDisk | Select-Object DeviceID,VolumeName,DriveType,Size,FreeSpace) } catch {}",
    "[pscustomobject]@{schema='soty.agent.windows-reinstall-preflight.v1';health=$health;os=$os;computer=$computer;battery=$battery;volumes=$volumes;collectedAt=(Get-Date).ToString('o')} | ConvertTo-Json -Compress -Depth 8"
  ].join("; ");
}

function formatWindowsReinstallPreflightFacts(result) {
  const exitCode = Number.isSafeInteger(result?.exitCode) ? result.exitCode : (result?.ok ? 0 : 1);
  const text = String(result?.text || "").trim();
  const parsed = extractJsonObject(text);
  const health = parsed && typeof parsed === "object" ? parsed.health : null;
  const machineReady = Boolean(health
    && health.scope === "Machine"
    && health.system === true
    && health.maintenance === true);
  const parts = [
    "windowsReinstallPreflight=read-only",
    `ok=${Boolean(result?.ok && exitCode === 0)}`,
    `exitCode=${exitCode}`,
    `machineWorkerReady=${machineReady}`,
    parsed ? `json=${JSON.stringify(parsed).slice(0, 6000)}` : `raw=${text.slice(0, 1000)}`
  ];
  return parts.join("\n");
}

function extractJsonObject(value) {
  const text = String(value || "").trim();
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start < 0 || end <= start) {
    return null;
  }
  try {
    return JSON.parse(text.slice(start, end + 1));
  } catch {
    return null;
  }
}

function lifecycleOpsGuidance(text, context) {
  if (!isWindowsReinstallRequest(text, context)) {
    return "";
  }
  const opsPath = findOpsScriptPath();
  return [
    "Windows reinstall/reset mode is active.",
    opsPath ? `Use $ops first: run ${quoteForPrompt(process.env.SOTY_PYTHON || process.env.PYTHON || "python")} ${quoteForPrompt(opsPath)} "<decoded user task>" before planning or acting.` : "Use $ops first if available; if it is unavailable, say that the reinstall workflow route is blocked.",
    "Do not offer Settings -> System -> Recovery -> Reset this PC as the primary path from Soty Agent. That is only a last-resort manual fallback if the user explicitly asks for manual steps.",
    "If no access=true source-matched target is visible, the right answer is: LINK for this exact Agent dialog is not active yet; press LINK and keep Soty open. Do not switch to another visible device or tell the user to start Windows reset manually.",
    "If an access=true source-matched target is visible, start with safe preflight only: source target identity, power/awake/lid, current OS, Soty machine-worker status, backup/return path. Do not wipe, reboot into recovery, reset, format USB, edit BCD, or launch setup until one exact final destructive confirmation is accepted.",
    "If that preflight already proves machine-status scope=Machine, system=true, and maintenance=true, treat the Soty machine worker as ready and do not ask the user to enable maintenance again.",
    "For privileged reinstall staging, prefer Soty machine worker proof: install-machine/elevate only after explaining one UAC approval, then require machine-status scope=Machine system=true maintenance=true before destructive prep."
  ].join("\n");
}

function isWindowsReinstallRequest(text, context = "") {
  const body = `${String(text || "")}\n${String(context || "").slice(-4000)}`.toLowerCase();
  return /reinstall|reset this pc|windows reset|winre|recovery|boot\.wim|setupcomplete|переустанов|сброс|восстановлен|вернуть компьютер|удалить всё|удалить все|установить\s+винд|винд\w*\s+заново|снести\s+винд/u.test(body);
}

function sanitizeAgentSource(value) {
  if (!value || typeof value !== "object") {
    return {};
  }
  const clean = (field) => String(field || "").trim().slice(0, maxSourceChars);
  return {
    tunnelId: clean(value.tunnelId),
    tunnelLabel: clean(value.tunnelLabel),
    deviceId: clean(value.deviceId),
    deviceNick: clean(value.deviceNick),
    appOrigin: clean(value.appOrigin),
    preferredTargetId: clean(value.preferredTargetId),
    preferredTargetLabel: clean(value.preferredTargetLabel),
    localAgentDirect: value.localAgentDirect === true,
    operatorTargets: sanitizeTargets(value.operatorTargets)
  };
}

function formatAgentSource(source) {
  const safe = sanitizeAgentSource(source);
  const lines = [
    ["Soty device nick", safe.deviceNick],
    ["Soty device id", safe.deviceId],
    ["Soty tunnel label", safe.tunnelLabel],
    ["Soty tunnel id", safe.tunnelId],
    ["App origin", safe.appOrigin],
    ["Local browser reached agent directly", safe.localAgentDirect ? "true" : ""],
    ["Preferred operator target", safe.preferredTargetId ? `${safe.preferredTargetLabel || "target"} (${safe.preferredTargetId})` : ""],
    ["Local agent ctl", `${quoteForPrompt(process.execPath)} ${quoteForPrompt(scriptPath)} ctl`],
    ["Local agent relay id", agentRelayId ? `${agentRelayId.slice(0, 10)}...` : ""],
    ["Local agent scope", agentScope],
    ["Local agent platform", process.platform]
  ]
    .filter(([, value]) => value)
    .map(([name, value]) => `${name}: ${value}`);
  return lines.join("\n");
}

function formatOperatorTargets(source) {
  const safe = sanitizeAgentSource(source);
  const sourceDeviceId = safe.deviceId;
  const targets = sourceMatchedOperatorTargets(source).slice(0, 16);
  if (targets.length === 0) {
    return sourceDeviceId
      ? `No authorized Soty operator target is currently attached for source device id ${sourceDeviceId}. Other Soty devices are intentionally hidden for safety.`
      : "No source-scoped Soty operator target is available because the request source has no device id.";
  }
  return targets
    .map((target) => {
      const flags = [
        `access=${formatTargetFlag(target.access)}`,
        `host=${formatTargetFlag(target.host)}`,
        target.hostDeviceId ? `hostDeviceId=${target.hostDeviceId}` : "",
        target.deviceIds.length > 0 ? `deviceIds=${target.deviceIds.join(",")}` : "",
        target.selected === true ? "selected=true" : "",
        Number.isSafeInteger(target.rank) ? `rank=${target.rank}` : "",
        target.lastActionAt ? `lastActionAt=${target.lastActionAt}` : ""
      ].filter(Boolean).join(" ");
      return `- ${target.label} (${target.id}) ${flags}`;
    })
    .join("\n");
}

function sourceMatchedOperatorTargets(source) {
  const safe = sanitizeAgentSource(source);
  const sourceDeviceId = safe.deviceId;
  const merged = new Map();
  for (const target of sanitizeTargets(source?.operatorTargets)) {
    merged.set(target.id, target);
  }
  for (const target of operatorTargets) {
    merged.set(target.id, target);
  }
  return [...merged.values()]
    .filter((target) => targetMatchesSourceDevice(target, sourceDeviceId))
    .filter((target) => target.access === true)
    .sort((left, right) => operatorSourceTargetScore(right, sourceDeviceId) - operatorSourceTargetScore(left, sourceDeviceId));
}

function targetMatchesSourceDevice(target, sourceDeviceId) {
  const sourceId = String(sourceDeviceId || "").trim();
  if (!sourceId) {
    return false;
  }
  return target.hostDeviceId === sourceId || target.deviceIds.includes(sourceId);
}

function formatTargetFlag(value) {
  return typeof value === "boolean" ? (value ? "true" : "false") : "unknown";
}

function asciiJsonString(value) {
  return JSON.stringify(String(value || "")).replace(/[^\x00-\x7f]/gu, (char) => {
    const code = char.codePointAt(0) || 0;
    if (code <= 0xffff) {
      return `\\u${code.toString(16).padStart(4, "0")}`;
    }
    const value = code - 0x10000;
    const high = 0xd800 + (value >> 10);
    const low = 0xdc00 + (value & 0x3ff);
    return `\\u${high.toString(16)}\\u${low.toString(16)}`;
  });
}

function quoteForPrompt(value) {
  return `"${String(value || "").replace(/"/gu, "\\\"")}"`;
}

function agentFailureText(details) {
  const value = String(details || "");
  let reason = "локальный Codex не смог собрать текстовый ответ.";
  let next = "Команды через Soty могли уже уйти в окно удаленных команд; это отдельный канал, и удаленному устройству Codex не нужен.";
  if (value.includes("Missing environment variable")) {
    reason = "на компьютере-операторе фоновому запуску Codex не хватает авторизации или API-ключа.";
    next = "Проверять нужно только компьютер-оператор, где открыт рабочий Codex, а не удаленное устройство.";
  } else if (value.includes("403 Forbidden") || value.includes("Unable to load site")) {
    reason = "фоновые запросы Codex или его плагинов получили 403 с компьютера-оператора.";
    next = "Это не значит, что удаленное устройство потеряло доступ; основной маршрут команд Soty остается отдельным.";
  } else if (value.includes("timeout")) {
    reason = "Codex не уложился в лимит ожидания текстового ответа.";
  }
  return [
    "Сообщение дошло до локального агента.",
    `Проблема не в удаленном компьютере и не в окне команд: ${reason}`,
    next
  ].join(" ");
}

function findCodexBinary() {
  const explicit = process.env.SOTY_CODEX_BIN || "";
  if (explicit && existsSync(explicit)) {
    return explicit;
  }
  if (process.platform === "win32") {
    const pathHit = [
      ...cursorCodexCandidates(),
      ...nodeCodexCandidates(),
      process.env.LOCALAPPDATA ? join(process.env.LOCALAPPDATA, "Microsoft", "WindowsApps", "codex.exe") : "",
      whichOnPath("codex.exe"),
      whichOnPath("codex.cmd"),
      whichOnPath("codex")
    ].find((candidate) => candidate && existsSync(candidate));
    if (pathHit) {
      return pathHit;
    }
  }
  return whichOnPath("codex");
}

function hasCodexBinary() {
  const now = Date.now();
  if (now - cachedCodexProbeAt < 30_000) {
    return cachedCodexAvailable;
  }
  cachedCodexProbeAt = now;
  cachedCodexAvailable = Boolean(findCodexBinary());
  return cachedCodexAvailable;
}

function cursorCodexCandidates() {
  const roots = userProfileRoots()
    .map((profile) => join(profile, ".cursor", "extensions"));
  const candidates = [];
  for (const root of roots) {
    try {
      candidates.push(...readdirSync(root, { withFileTypes: true })
        .filter((entry) => entry.isDirectory() && entry.name.startsWith("openai.chatgpt-"))
        .map((entry) => join(root, entry.name, "bin", "windows-x86_64", "codex.exe"))
        .filter((candidate) => existsSync(candidate))
        .sort()
        .reverse());
    } catch {
      // Ignore inaccessible user profiles.
    }
  }
  return candidates;
}

function nodeCodexCandidates() {
  const candidates = [];
  const exeDir = dirname(process.execPath || "");
  for (const dir of [
    exeDir,
    process.env.APPDATA ? join(process.env.APPDATA, "npm") : "",
    process.env.LOCALAPPDATA ? join(process.env.LOCALAPPDATA, "Microsoft", "WinGet", "Packages") : ""
  ].filter(Boolean)) {
    candidates.push(join(dir, "codex.cmd"), join(dir, "codex.exe"), join(dir, "codex"));
  }
  const wingetRoot = process.env.LOCALAPPDATA ? join(process.env.LOCALAPPDATA, "Microsoft", "WinGet", "Packages") : "";
  try {
    if (!wingetRoot) {
      return candidates;
    }
    for (const packageDir of readdirSync(wingetRoot, { withFileTypes: true })) {
      if (!packageDir.isDirectory() || !packageDir.name.startsWith("OpenJS.NodeJS.")) {
        continue;
      }
      const packageRoot = join(wingetRoot, packageDir.name);
      for (const nodeDir of readdirSync(packageRoot, { withFileTypes: true })) {
        if (nodeDir.isDirectory() && nodeDir.name.startsWith("node-")) {
          candidates.push(
            join(packageRoot, nodeDir.name, "codex.cmd"),
            join(packageRoot, nodeDir.name, "codex.exe"),
            join(packageRoot, nodeDir.name, "codex")
          );
        }
      }
    }
  } catch {
    // PATH lookup below still covers normal installs.
  }
  return candidates;
}

function userProfileRoots() {
  const roots = new Set([homedir(), process.env.USERPROFILE || ""]);
  const systemDrive = process.env.SystemDrive || "C:";
  try {
    for (const entry of readdirSync(join(systemDrive, "Users"), { withFileTypes: true })) {
      if (entry.isDirectory()) {
        roots.add(join(systemDrive, "Users", entry.name));
      }
    }
  } catch {
    // Single-user systems may not expose C:\Users to the agent.
  }
  return [...roots].filter(Boolean);
}

function whichOnPath(name) {
  const pathEnv = process.env.PATH || "";
  const parts = pathEnv.split(process.platform === "win32" ? ";" : ":").filter(Boolean);
  for (const part of parts) {
    const candidate = join(part, name);
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return "";
}

function chooseCodexHome() {
  const explicit = process.env.SOTY_CODEX_HOME || process.env.CODEX_HOME || "";
  if (explicit && existsSync(explicit)) {
    return explicit;
  }
  const localAppData = process.env.LOCALAPPDATA || join(homedir(), "AppData", "Local");
  const proxyApiHome = join(localAppData, "OpenAI", "CodexProxyAPI");
  if (process.env.PROXYAPI_KEY && existsSync(proxyApiHome)) {
    return proxyApiHome;
  }
  const gonkaHome = join(localAppData, "OpenAI", "CodexGonka");
  if (process.env.GONKA_API_KEY && existsSync(gonkaHome)) {
    return gonkaHome;
  }
  const home = join(homedir(), ".codex");
  return existsSync(home) ? home : "";
}

async function maybeSyncCodexSkills(codexHome) {
  if (!codexHome || process.env.SOTY_CODEX_SKILL_SYNC_DISABLED === "1") {
    return;
  }
  if (!skillSyncRepoUrl || !skillSyncName) {
    return;
  }
  const now = Date.now();
  if (skillSyncIntervalMs > 0 && now - lastSkillSyncAt < skillSyncIntervalMs) {
    return;
  }
  if (!skillSyncInFlight) {
    skillSyncInFlight = syncCodexSkills(codexHome).finally(() => {
      skillSyncInFlight = null;
    });
  }
  await skillSyncInFlight;
}

async function syncCodexSkills(codexHome) {
  lastSkillSyncAt = Date.now();
  const sourceDir = resolve(process.env.SOTY_CODEX_SKILL_SYNC_SOURCE || join(agentDir, "skill-sources", "universal-install-ops-skill"));
  const sourceParent = resolve(dirname(sourceDir));
  const skillsRoot = resolve(codexHome, "skills");
  const destDir = resolve(skillsRoot, skillSyncName);
  try {
    if (!isPathInside(sourceDir, resolve(agentDir)) && !process.env.SOTY_CODEX_SKILL_SYNC_SOURCE) {
      throw new Error("skill source path is outside agent dir");
    }
    if (!isPathInside(destDir, skillsRoot)) {
      throw new Error("skill destination path is outside CODEX_HOME skills");
    }
    if (sourceDir === destDir) {
      throw new Error("skill source and destination are the same path");
    }
    await mkdir(sourceParent, { recursive: true });
    if (existsSync(join(sourceDir, ".git"))) {
      runGit(["-C", sourceDir, "fetch", "--depth", "1", "origin", skillSyncRef], 20_000);
      runGit(["-C", sourceDir, "reset", "--hard", `origin/${skillSyncRef}`], 20_000);
    } else if (!existsSync(sourceDir)) {
      runGit(["clone", "--depth", "1", "--branch", skillSyncRef, skillSyncRepoUrl, sourceDir], 45_000);
    } else {
      throw new Error("skill source exists but is not a git checkout");
    }
    if (!existsSync(join(sourceDir, "SKILL.md")) || !existsSync(join(sourceDir, "scripts", "ops.py"))) {
      throw new Error("skill source missing SKILL.md or scripts/ops.py");
    }
    await installSkillCopy(sourceDir, destDir);
    const revision = runGit(["-C", sourceDir, "rev-parse", "--short", "HEAD"], 5000).trim();
    lastSkillSyncStatus = { ok: true, detail: "synced", revision, at: Date.now() };
  } catch (error) {
    lastSkillSyncStatus = {
      ok: false,
      detail: error instanceof Error ? error.message.slice(0, 240) : String(error).slice(0, 240),
      revision: "",
      at: Date.now()
    };
  }
}

function runGit(args, timeoutMs) {
  return execFileSync("git", args, {
    encoding: "utf8",
    timeout: timeoutMs,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"]
  });
}

async function installSkillCopy(sourceDir, destDir) {
  await mkdir(destDir, { recursive: true });
  for (const entry of readdirSync(destDir, { withFileTypes: true })) {
    if (entry.name === ".skill-memory") {
      continue;
    }
    await rm(join(destDir, entry.name), { recursive: true, force: true });
  }
  for (const entry of readdirSync(sourceDir, { withFileTypes: true })) {
    if (entry.name === ".git" || entry.name === ".skill-memory") {
      continue;
    }
    await cp(join(sourceDir, entry.name), join(destDir, entry.name), {
      recursive: entry.isDirectory(),
      force: true,
      dereference: false
    });
  }
}

function isPathInside(child, parent) {
  const delta = relative(parent, child);
  return delta === "" || (!delta.startsWith("..") && !isAbsolute(delta));
}

function runChildForText(file, args, env, timeoutMs, input = "") {
  return new Promise((resolve, reject) => {
    const child = spawn(file, args, {
      cwd: process.env.SOTY_CODEX_CWD || process.cwd(),
      env,
      windowsHide: true,
      stdio: [input ? "pipe" : "ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    let done = false;
    const finish = (exitCode) => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      resolve({
        exitCode: Number.isSafeInteger(exitCode) ? exitCode : 0,
        stdout: stdout.slice(-12_000),
        stderr: stderr.slice(-12_000)
      });
    };
    const timer = setTimeout(() => {
      if (done) {
        return;
      }
      killProcessTree(child);
      reject(new Error("timeout"));
    }, Math.max(5000, timeoutMs || 120000));
    child.stdout.on("data", (chunk) => {
      stdout = `${stdout}${chunk.toString("utf8")}`.slice(-24_000);
    });
    child.stderr.on("data", (chunk) => {
      stderr = `${stderr}${chunk.toString("utf8")}`.slice(-24_000);
    });
    if (input && child.stdin) {
      child.stdin.end(input, "utf8");
    }
    child.on("error", (error) => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", finish);
  });
}

function sanitizeTargets(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => ({
      id: typeof item?.id === "string" ? item.id.slice(0, 160) : "",
      label: typeof item?.label === "string" ? item.label.slice(0, 160) : "",
      deviceIds: Array.isArray(item?.deviceIds)
        ? [...new Set(item.deviceIds
          .filter((value) => typeof value === "string")
          .map((value) => value.slice(0, maxSourceChars))
          .filter(Boolean))]
          .slice(0, 16)
        : [],
      hostDeviceId: typeof item?.hostDeviceId === "string" ? item.hostDeviceId.slice(0, maxSourceChars) : "",
      access: typeof item?.access === "boolean" ? item.access : undefined,
      host: typeof item?.host === "boolean" ? item.host : undefined,
      selected: typeof item?.selected === "boolean" ? item.selected : undefined,
      rank: Number.isSafeInteger(item?.rank) ? Math.max(1, Math.min(item.rank, 999)) : undefined,
      lastActionAt: typeof item?.lastActionAt === "string" ? item.lastActionAt.slice(0, 80) : ""
    }))
    .filter((item) => item.id && item.label)
    .slice(0, 128);
}

function hasKnownOperatorTarget(target) {
  const needle = String(target || "").trim().toLowerCase();
  if (!needle) {
    return false;
  }
  return operatorTargets.some((item) => item.id === target
    || item.id.toLowerCase() === needle
    || item.label.toLowerCase() === needle
    || item.label.toLowerCase().includes(needle));
}

function resolveOperatorTargetForSourceDevice(sourceDeviceId) {
  const deviceId = String(sourceDeviceId || "").trim();
  if (!deviceId) {
    return null;
  }
  return operatorTargets
    .filter((target) => target.access === true && targetMatchesSourceDevice(target, deviceId))
    .sort((left, right) => operatorSourceTargetScore(right, deviceId) - operatorSourceTargetScore(left, deviceId))[0] || null;
}

function operatorSourceTargetScore(target, sourceDeviceId) {
  let score = 0;
  if (target.selected === true) {
    score += 1000;
  }
  if (target.hostDeviceId === sourceDeviceId) {
    score += 200;
  }
  if (target.deviceIds.includes(sourceDeviceId)) {
    score += 100;
  }
  if (target.lastActionAt) {
    const time = Date.parse(target.lastActionAt);
    if (Number.isFinite(time)) {
      score += Math.max(0, Math.min(99, Math.floor((time - Date.now() + 24 * 60 * 60 * 1000) / (15 * 60 * 1000))));
    }
  }
  if (Number.isSafeInteger(target.rank)) {
    score += Math.max(0, 50 - target.rank);
  }
  return score;
}

function isAgentSourceTarget(target) {
  return agentSourceDeviceId(target) !== "";
}

function agentSourceDeviceId(target) {
  const text = String(target || "").trim();
  if (!text.startsWith("agent-source:")) {
    return "";
  }
  return text.slice("agent-source:".length, "agent-source:".length + maxSourceChars);
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
    killProcessTree(child);
    send(ws, id, "!\n", 124, "exit");
  }, Math.max(1000, timeoutMs));
  addCloseHandler(ws, () => {
    if (active.get(id) === child) {
      clearTimeout(timer);
      active.delete(id);
      killProcessTree(child);
    }
  });

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
    killProcessTree(child);
    send(ws, id, "!\n", 124, "exit");
  }, Math.max(1000, timeoutMs));
  addCloseHandler(ws, () => {
    if (active.get(id) === child) {
      clearTimeout(timer);
      void finish();
      killProcessTree(child);
    }
  });

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

function addCloseHandler(ws, handler) {
  const previous = ws.onClose;
  ws.onClose = () => {
    try {
      previous();
    } finally {
      handler();
    }
  };
}

function killProcessTree(child) {
  if (!child || !child.pid) {
    return;
  }
  if (process.platform === "win32") {
    try {
      spawn("taskkill.exe", ["/PID", String(child.pid), "/T", "/F"], {
        windowsHide: true,
        stdio: "ignore"
      });
      return;
    } catch {
      // Fall back to child.kill below.
    }
  }
  try {
    child.kill();
  } catch {
    // Best-effort cleanup; process may already be gone.
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
  if (args[0] === "--") {
    args = args.slice(1);
  }
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
      const status = target.access === true ? "access" : target.host === true ? "host" : target.access === false ? "visible" : "unknown";
      process.stdout.write(`${target.label}\t${target.id}\t${status}\n`);
    }
    return;
  }
  if (command === "run") {
    const parsed = parseCtlOptions(args.slice(1));
    const target = parsed.args[0] || "";
    const remoteCommand = parsed.args.slice(1).join(" ");
    if (!target || !remoteCommand) {
      process.stderr.write("sotyctl run [--source-device=id] [--timeout=ms] <target> <command>\n");
      process.exit(2);
    }
    const response = await fetch(`http://127.0.0.1:${port}/operator/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target,
        command: remoteCommand,
        ...(parsed.sourceDeviceId ? { sourceDeviceId: parsed.sourceDeviceId } : {}),
        ...(parsed.timeoutMs ? { timeoutMs: parsed.timeoutMs } : {})
      })
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
    const parsed = parseCtlOptions(args.slice(1));
    const target = parsed.args[0] || "";
    const filePath = parsed.args[1] || "";
    const shell = parsed.args[2] || "";
    if (!target || !filePath) {
      process.stderr.write("sotyctl script [--source-device=id] [--timeout=ms] <target> <file> [shell]\n");
      process.exit(2);
    }
    const script = await readFile(filePath, "utf8");
    const response = await fetch(`http://127.0.0.1:${port}/operator/script`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target,
        name: basename(filePath),
        shell,
        script,
        ...(parsed.sourceDeviceId ? { sourceDeviceId: parsed.sourceDeviceId } : {}),
        ...(parsed.timeoutMs ? { timeoutMs: parsed.timeoutMs } : {})
      })
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
  if (command === "access" || command === "request-access") {
    const target = args[1] || "";
    if (!target) {
      process.stderr.write("sotyctl access <target>\n");
      process.exit(2);
    }
    const response = await fetch(`http://127.0.0.1:${port}/operator/access`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ target })
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
  if (command === "agent-message" || command === "agent-chat") {
    const parsed = parseCtlOptions(args.slice(1));
    const target = parsed.args.length > 1 ? parsed.args[0] || "" : "";
    const text = (target ? parsed.args.slice(1) : parsed.args).join(" ");
    if (!text) {
      process.stderr.write("sotyctl agent-message [--timeout=ms] [agent-tunnel-id] <text>\n");
      process.exit(2);
    }
    const response = await fetch(`http://127.0.0.1:${port}/operator/agent-message`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target,
        text,
        ...(parsed.timeoutMs ? { timeoutMs: parsed.timeoutMs } : {})
      })
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
  if (command === "read" || command === "inbox" || command === "messages") {
    const target = args[1] || "";
    const url = new URL(`http://127.0.0.1:${port}/operator/messages`);
    if (target) {
      url.searchParams.set("target", target);
    }
    const response = await fetch(url, { cache: "no-store" });
    const payload = await response.json();
    for (const message of payload.messages || []) {
      process.stdout.write(`${message.createdAt}\t${message.label || message.target}\t${message.text.replace(/\n/gu, "\\n")}\t${message.id}\n`);
    }
    process.exit(response.ok ? 0 : 1);
  }
  if (command === "listen") {
    const target = args[1] || "";
    let after = args[2] || "";
    for (;;) {
      const url = new URL(`http://127.0.0.1:${port}/operator/messages`);
      url.searchParams.set("wait", "1");
      if (target) {
        url.searchParams.set("target", target);
      }
      if (after) {
        url.searchParams.set("after", after);
      }
      const response = await fetch(url, { cache: "no-store" });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        process.exit(response.ok ? 1 : response.status);
      }
      for (const message of payload.messages || []) {
        process.stdout.write(`${JSON.stringify(message)}\n`);
        after = message.id || after;
      }
    }
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
  if (command === "import") {
    const filePath = args[1] || "";
    if (!filePath) {
      process.stderr.write("import needs a backup JSON file\n");
      process.exit(2);
    }
    const text = await readFile(filePath, "utf8");
    const response = await fetch(`http://127.0.0.1:${port}/operator/import`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text })
    });
    const payload = await response.json();
    if (!payload.ok) {
      if (payload.text) {
        process.stderr.write(`${payload.text}\n`);
      }
      process.exit(typeof payload.exitCode === "number" ? payload.exitCode : 1);
    }
    process.stdout.write(payload.text || "restored\n");
    if (!String(payload.text || "").endsWith("\n")) {
      process.stdout.write("\n");
    }
    process.exit(0);
  }
  process.stderr.write("sotyctl health | list | run [--source-device=id] [--timeout=ms] <target> <command> | script [--source-device=id] [--timeout=ms] <target> <file> [shell] | install-machine <target> | machine-status <target> | access <target> | say [--fast|--slow] <target> <text> | agent-message [--timeout=ms] [agent-tunnel-id] <text> | read [target] | listen [target] | export [file] | import <file>\n");
  process.exit(2);
}

function parseCtlOptions(args) {
  const rest = [...args];
  let timeoutMs = 0;
  let sourceDeviceId = "";
  while (rest.length > 0) {
    const head = rest[0] || "";
    if (head.startsWith("--timeout=")) {
      timeoutMs = safeCtlTimeout(head.slice("--timeout=".length));
      rest.shift();
      continue;
    }
    if (head === "--timeout" && rest.length > 1) {
      timeoutMs = safeCtlTimeout(rest[1]);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--source-device=")) {
      sourceDeviceId = String(head.slice("--source-device=".length) || "").slice(0, maxSourceChars);
      rest.shift();
      continue;
    }
    if (head === "--source-device" && rest.length > 1) {
      sourceDeviceId = String(rest[1] || "").slice(0, maxSourceChars);
      rest.splice(0, 2);
      continue;
    }
    break;
  }
  return { timeoutMs, sourceDeviceId, args: rest };
}

function parseCtlTimeout(args) {
  return parseCtlOptions(args);
}

function safeCtlTimeout(value) {
  const timeoutMs = Number.parseInt(String(value || ""), 10);
  return Number.isSafeInteger(timeoutMs) ? Math.max(1000, Math.min(timeoutMs, defaultTimeoutMs)) : 0;
}

function machineInstallCommand() {
  const encoded = psEncoded(machineInstallLauncherScript());
  return [
    "$ErrorActionPreference='Stop'",
    `Start-Process -FilePath 'powershell.exe' -WindowStyle Hidden -ArgumentList ${psQuote(`-NoLogo -NoProfile -ExecutionPolicy Bypass -EncodedCommand ${encoded}`)}`,
    "Write-Output 'soty-agent-machine:uac-launcher-started'"
  ].join("; ");
}

function machineInstallLauncherScript() {
  return [
    "$ErrorActionPreference='Stop'",
    "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12",
    "$dir = Join-Path $env:TEMP 'soty-agent-machine'",
    "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
    "$script = Join-Path $dir 'install-windows.ps1'",
    "Invoke-WebRequest -Uri 'https://xn--n1afe0b.online/agent/install-windows.ps1' -UseBasicParsing -OutFile $script",
    "$args = '-NoLogo -NoProfile -ExecutionPolicy Bypass -File \"' + $script + '\" -Scope Machine -LaunchAppAtLogon'",
    "Start-Process -FilePath 'powershell.exe' -Verb RunAs -ArgumentList $args"
  ].join("\r\n");
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

function psEncoded(value) {
  return Buffer.from(String(value), "utf16le").toString("base64");
}

function isSafeText(value, max) {
  return typeof value === "string" && value.length > 0 && value.length <= max;
}

function safeSourceText(value) {
  return typeof value === "string" ? value.trim().slice(0, maxSourceChars) : "";
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
    const startsWithParam = /^\uFEFF?\s*param\s*\(/iu.test(payload.script);
    return {
      name: basename(path),
      path,
      content: startsWithParam ? `\uFEFF${payload.script}` : `\uFEFF${powerShellUtf8Prelude()}\r\n${payload.script}`,
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
    relay: Boolean(agentRelayId),
    codex: hasCodexBinary(),
    skillSync: lastSkillSyncStatus,
    ...(process.platform === "win32" ? {
      windowsUser: windowsUserName(),
      system: isWindowsSystem(),
      maintenance: agentScope === "Machine" && isWindowsSystem()
    } : {})
  };
}

function windowsUserName() {
  const actual = windowsWhoami();
  if (actual) {
    return actual;
  }
  const domain = process.env.USERDOMAIN || "";
  const user = process.env.USERNAME || "";
  return domain && user ? `${domain}\\${user}` : user;
}

function isWindowsSystem() {
  const actual = windowsWhoami().toLowerCase();
  return actual === "nt authority\\system"
    || actual === "nt authority\\система"
    || (agentScope === "Machine" && (process.env.USERNAME || "").endsWith("$"));
}

function windowsWhoami() {
  if (process.platform !== "win32") {
    return "";
  }
  if (cachedWindowsWhoami) {
    return cachedWindowsWhoami;
  }
  try {
    cachedWindowsWhoami = execFileSync("whoami.exe", {
      encoding: "utf8",
      timeout: 1000,
      windowsHide: true
    }).trim();
  } catch {
    cachedWindowsWhoami = "";
  }
  return cachedWindowsWhoami;
}

function safeScope(value) {
  const text = String(value || "").trim();
  if (text === "Machine" || text === "CurrentUser" || text === "Dev") {
    return text;
  }
  return "CurrentUser";
}

function safeRelayId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

function safeHttpBaseUrl(value) {
  try {
    const url = new URL(String(value || ""));
    if (url.protocol !== "https:" && url.protocol !== "http:") {
      return "";
    }
    return url.origin;
  } catch {
    return "";
  }
}

function originFromUrl(value) {
  try {
    return new URL(String(value || "")).origin;
  } catch {
    return "";
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
    let fastChecks = 0;
    const fastInterval = setInterval(() => {
      fastChecks += 1;
      void checkForUpdate();
      if (fastChecks >= 10) {
        clearInterval(fastInterval);
      }
    }, 60 * 1000);
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
