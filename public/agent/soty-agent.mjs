#!/usr/bin/env node
import { execFileSync, spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { appendFile, chmod, copyFile, mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const agentVersion = "0.4.0";
const scriptPath = fileURLToPath(import.meta.url);
const agentDir = dirname(scriptPath);
const agentConfigPath = join(agentDir, "agent-config.json");
const codexSessionsPath = join(agentDir, "agent-codex-sessions.json");
const codexWorkspacesDir = join(agentDir, "codex-workspaces");
const agentTracesDir = resolve(process.env.SOTY_AGENT_TRACE_DIR || join(agentDir, "agent-traces"));
const learningOutboxPath = join(agentDir, "learning-outbox.jsonl");
const learningSentPath = join(agentDir, "learning-sent.jsonl");
const actionJobsDir = resolve(process.env.SOTY_AGENT_ACTION_JOBS_DIR || join(agentDir, "action-jobs"));
const persistedAgentConfig = loadAgentConfig();
const persistedCodexSessions = loadCodexSessions();
const port = Number.parseInt(arg("--port") || process.env.SOTY_AGENT_PORT || "49424", 10);
const maxLongTaskTimeoutMs = 24 * 60 * 60_000;
const defaultTimeoutMs = safeDurationMs(arg("--timeout") || process.env.SOTY_AGENT_TIMEOUT_MS, 30 * 60_000, maxLongTaskTimeoutMs);
const mcpInlineToolBudgetMs = 95_000;
const turnkeyGuardTimeoutMs = safeDurationMs(process.env.SOTY_TURNKEY_GUARD_TIMEOUT_MS, maxLongTaskTimeoutMs, maxLongTaskTimeoutMs);
const turnkeyGuardProgressMs = safeDurationMs(process.env.SOTY_TURNKEY_GUARD_PROGRESS_MS, 20 * 60_000, 60 * 60_000);
const requestedShell = arg("--shell") || process.env.SOTY_AGENT_SHELL || "";
const updateManifestUrl = arg("--update-url") || process.env.SOTY_AGENT_UPDATE_URL || "https://xn--n1afe0b.online/agent/manifest.json";
let agentRelayId = safeRelayId(arg("--relay-id") || process.env.SOTY_AGENT_RELAY_ID || persistedAgentConfig.relayId || "");
let agentRelayBaseUrl = safeHttpBaseUrl(process.env.SOTY_AGENT_RELAY_URL || persistedAgentConfig.relayBaseUrl || originFromUrl(updateManifestUrl) || "https://xn--n1afe0b.online");
const agentInstallId = safeInstallId(persistedAgentConfig.installId) || randomUUID();
const managed = process.argv.includes("--managed") || process.env.SOTY_AGENT_MANAGED === "1";
const agentAutoUpdate = process.env.SOTY_AGENT_AUTO_UPDATE === "1"
  || (process.env.SOTY_AGENT_AUTO_UPDATE !== "0" && process.env.SOTY_AGENT_SUPERVISED === "1");
const agentScope = safeScope(process.env.SOTY_AGENT_SCOPE || (managed ? "CurrentUser" : "Dev"));
const maxCommandChars = 8_000;
const maxScriptChars = 8_000_000;
const maxChatChars = 12_000;
const maxAgentContextChars = 16_000;
const maxAgentRuntimePromptChars = 48_000;
const maxLearningMarkersPerTurn = 8;
const maxImportChars = 2_000_000;
const maxChunkBytes = 12_000;
const maxFrameBytes = 2_500_000;
const maxSourceChars = 180;
const updateFetchTimeoutMs = 20_000;
const sourceJobPickupBaseMs = 90_000;
const configuredAgentDeviceId = safeSourceText(process.env.SOTY_AGENT_DEVICE_ID || "");
const configuredAgentDeviceNick = safeSourceText(process.env.SOTY_AGENT_DEVICE_NICK || "");
const maxCodexDialogMessages = 64;
const audioToolTimeoutMs = 120_000;
const audioWarmupTimeoutMs = 45_000;
const agentReplyTimeoutMs = Math.max(
  maxLongTaskTimeoutMs,
  safeDurationMs(process.env.SOTY_CODEX_REPLY_TIMEOUT_MS, maxLongTaskTimeoutMs, maxLongTaskTimeoutMs)
);
const codexStartupTimeoutMs = safeDurationMs(process.env.SOTY_CODEX_STARTUP_TIMEOUT_MS, 25_000, 120_000);
const maxConcurrentCodexJobs = Math.max(1, Math.min(Number.parseInt(process.env.SOTY_CODEX_CONCURRENCY || "4", 10) || 4, 16));
const codexFullLocalTools = process.env.SOTY_CODEX_FULL_LOCAL_TOOLS !== "0";
const codexProxyUrl = safeProxyUrl(process.env.SOTY_CODEX_PROXY_URL || process.env.SOTY_AGENT_PROXY_URL || "");
const codexDefaultReasoningEffort = safeCodexReasoningEffort(process.env.SOTY_CODEX_REASONING_EFFORT || "");
const codexRelayFallback = process.env.SOTY_CODEX_RELAY_FALLBACK !== "0";
const codexDisabled = process.env.SOTY_CODEX_DISABLED === "1";
const enableFastDirectAnswers = process.env.SOTY_AGENT_ENABLE_FAST_DIRECT === "1";
const agentTraceEnabled = process.env.SOTY_AGENT_TRACE !== "0";
const agentTraceFullPrompt = process.env.SOTY_AGENT_TRACE_FULL_PROMPT !== "0";
const agentTraceRetain = Math.max(10, Math.min(Number.parseInt(process.env.SOTY_AGENT_TRACE_RETAIN || "200", 10) || 200, 5000));
const agentTraceMaxJsonEvents = Math.max(20, Math.min(Number.parseInt(process.env.SOTY_AGENT_TRACE_MAX_EVENTS || "360", 10) || 360, 5000));
const codexSessionMode = "soty-clean-codex-memory-plane-v1";
const agentResponseStyleProfiles = Object.freeze([
  {
    id: "lord-sysadmin",
    displayName: "Лорд",
    base: "agent",
    tone: "brief-sysadmin",
    maxUserFacingLines: 0,
    phraseBank: [],
    promptRules: []
  }
]);
const defaultAgentResponseStyleId = "lord-sysadmin";
const agentResponseStyleId = safeAgentResponseStyleId(
  process.env.SOTY_AGENT_RESPONSE_STYLE || persistedAgentConfig.responseStyle || defaultAgentResponseStyleId
);
const activeAgentResponseStyle = agentResponseStyleProfile(agentResponseStyleId);
const active = new Map();
const operatorRuns = new Map();
const actionJobs = new Map();
const actionControllers = new Map();
const operatorMessages = [];
const operatorMessageWaiters = new Set();
const agentOperatorReplyQueues = new Map();
const recentAgentOperatorMessageKeys = new Map();
const recentCodexTurnKeys = new Map();
const activeRelayJobs = new Set();
let learningSyncTimer = null;
let operatorBridge = null;
let operatorBridgeVisible = false;
let operatorBridgeProtocol = "";
let operatorBridgeCapabilities = [];
let operatorTargets = [];
let operatorDeviceId = "";
let operatorDeviceNick = "";
let cachedWindowsWhoami = "";
let cachedCodexProbeAt = 0;
let cachedCodexAvailable = false;
let cachedCodexLearningMemoryAt = 0;
let cachedCodexLearningMemoryText = "";
let agentRelayStarted = false;
let audioWarmupStarted = false;
let updateCheckRunning = false;
let deferredUpdateTimer = null;
const allowedOrigins = new Set([
  "https://xn--n1afe0b.online",
]);

function loadAgentConfig() {
  try {
    const parsed = JSON.parse(readFileSync(agentConfigPath, "utf8"));
    return {
      relayId: typeof parsed?.relayId === "string" ? parsed.relayId : "",
      relayBaseUrl: typeof parsed?.relayBaseUrl === "string" ? parsed.relayBaseUrl : "",
      installId: typeof parsed?.installId === "string" ? parsed.installId : ""
    };
  } catch {
    return { relayId: "", relayBaseUrl: "", installId: "" };
  }
}

function loadCodexSessions() {
  try {
    const parsed = JSON.parse(readFileSync(codexSessionsPath, "utf8"));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

async function saveCodexSessions() {
  await writeFile(codexSessionsPath, JSON.stringify(persistedCodexSessions, null, 2), "utf8")
    .catch(() => undefined);
}

async function saveAgentConfig() {
  await writeFile(agentConfigPath, JSON.stringify({
    relayId: agentRelayId,
    relayBaseUrl: agentRelayBaseUrl,
    installId: agentInstallId
  }, null, 2), "utf8").catch(() => undefined);
}

if (process.argv[2] === "mcp") {
  runMcpServer();
} else if (process.argv[2] === "ctl") {
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
    void ensureCtlLauncher();
    void preparePersistentStockCodexHome();
    scheduleWindowsAudioWarmup();
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
      bridgeProtocol: operatorBridgeProtocol,
      bridgeCapabilities: operatorBridgeCapabilities,
      targets: operatorTargets
    });
    return;
  }
  if (url.pathname === "/operator/source-status" && request.method === "GET") {
    await handleOperatorHttpSourceStatus(url, response, headers);
    return;
  }
  if (url.pathname === "/operator/toolkits" && request.method === "GET") {
    sendJson(response, 200, headers, {
      ok: true,
      version: agentVersion,
      manifestUrl: updateManifestUrl,
      ...automationToolkitStatus()
    });
    return;
  }
  if (url.pathname === "/operator/actions" && request.method === "GET") {
    await handleOperatorHttpActions(response, headers);
    return;
  }
  if (url.pathname === "/operator/action" && request.method === "POST") {
    await handleOperatorHttpAction(request, response, headers);
    return;
  }
  const actionMatch = url.pathname.match(/^\/operator\/action\/([A-Za-z0-9_-]{8,96})$/u);
  if (actionMatch && request.method === "GET") {
    await handleOperatorHttpActionStatus(actionMatch[1], response, headers);
    return;
  }
  const actionStopMatch = url.pathname.match(/^\/operator\/action\/([A-Za-z0-9_-]{8,96})\/stop$/u);
  if (actionStopMatch && request.method === "POST") {
    await handleOperatorHttpActionStop(actionStopMatch[1], response, headers);
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
  if (url.pathname === "/operator/agent-new" && request.method === "POST") {
    await handleOperatorHttpAgentNew(request, response, headers);
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
  if (url.pathname === "/agent/traces" && request.method === "GET") {
    await handleAgentTraceList(url, response, headers);
    return;
  }
  const traceMatch = url.pathname.match(/^\/agent\/trace\/([A-Za-z0-9_.-]{12,120})$/u);
  if (traceMatch && request.method === "GET") {
    await handleAgentTraceRead(traceMatch[1], response, headers);
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
    await handleOperatorHttpExport(request, url, response, headers);
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
      safeRunTimeoutMs(message.timeoutMs)
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
    safeRunTimeoutMs(message.timeoutMs)
  );
}

function handleOperatorMessage(ws, message) {
  if (message.type === "operator.attach") {
    const visible = message.visible === true;
    if (operatorBridge?.open && operatorBridge !== ws && operatorBridgeVisible && !visible) {
      sendRaw(ws, { type: "operator.ready", accepted: false });
      return;
    }
    operatorBridge = ws;
    operatorBridgeVisible = visible;
    operatorBridgeProtocol = typeof message.protocol === "string" ? message.protocol.slice(0, 80) : "";
    operatorBridgeCapabilities = Array.isArray(message.capabilities)
      ? message.capabilities.filter((item) => typeof item === "string").slice(0, 24).map((item) => item.slice(0, 80))
      : [];
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
    operatorBridgeVisible = false;
    operatorBridgeProtocol = "";
    operatorBridgeCapabilities = [];
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
  const sourceRelayId = safeRelayId(payload.sourceRelayId || "");
  const command = typeof payload.command === "string" ? payload.command.slice(0, maxCommandChars) : "";
  const timeoutMs = safeRunTimeoutMs(payload.timeoutMs);
  const blocked = blockedManualWindowsRecoveryHandoff(command);
  if (blocked) {
    recordBlockedWindowsReinstallHandoff({ kind: "run", command });
    sendJson(response, 422, headers, { ok: false, text: blocked, exitCode: 422 });
    return;
  }
  ({ target, sourceDeviceId } = await normalizeOperatorHttpTarget(target, sourceDeviceId, sourceRelayId));
  if (isAgentSourceTarget(target)) {
    const deviceId = agentSourceDeviceId(target);
    if (sourceDeviceId && sourceDeviceId !== deviceId) {
      sendJson(response, 403, headers, { ok: false, text: "! source-target", exitCode: 403 });
      return;
    }
    await handleAgentSourceHttpRun(target, sourceDeviceId || deviceId, command, timeoutMs, response, headers, sourceRelayId);
    return;
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
    command,
    timeoutMs
  });
}

async function handleOperatorHttpScript(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, 12_500_000);
  } catch {
    sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
    return;
  }
  let target = typeof payload.target === "string" ? payload.target.slice(0, 160) : "";
  let sourceDeviceId = typeof payload.sourceDeviceId === "string" ? payload.sourceDeviceId.slice(0, maxSourceChars) : "";
  const sourceRelayId = safeRelayId(payload.sourceRelayId || "");
  const script = typeof payload.script === "string" ? payload.script.slice(0, maxScriptChars) : "";
  const name = typeof payload.name === "string" ? payload.name.slice(0, 120) : "script";
  const shell = typeof payload.shell === "string" ? payload.shell.slice(0, 40) : "";
  const timeoutMs = safeRunTimeoutMs(payload.timeoutMs);
  const blocked = blockedManualWindowsRecoveryHandoff(script);
  if (blocked) {
    recordBlockedWindowsReinstallHandoff({ kind: "script", command: script });
    sendJson(response, 422, headers, { ok: false, text: blocked, exitCode: 422 });
    return;
  }
  ({ target, sourceDeviceId } = await normalizeOperatorHttpTarget(target, sourceDeviceId, sourceRelayId));
  if (isAgentSourceTarget(target)) {
    const deviceId = agentSourceDeviceId(target);
    if (sourceDeviceId && sourceDeviceId !== deviceId) {
      sendJson(response, 403, headers, { ok: false, text: "! source-target", exitCode: 403 });
      return;
    }
    await handleAgentSourceHttpScript(target, sourceDeviceId || deviceId, { script, name, shell }, timeoutMs, response, headers, sourceRelayId);
    return;
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
    script,
    timeoutMs
  });
}

async function handleOperatorHttpSourceStatus(url, response, headers) {
  const target = String(url.searchParams.get("target") || "").slice(0, 160);
  const sourceRelayId = safeRelayId(url.searchParams.get("sourceRelayId") || "");
  const sourceDeviceId = safeSourceText(url.searchParams.get("sourceDeviceId") || "") || agentSourceDeviceId(target);
  const status = await operatorSourceStatus({ target, sourceRelayId, sourceDeviceId });
  sendJson(response, status.ok ? 200 : 409, headers, status);
}

async function operatorSourceStatus({ target = "", sourceRelayId = "", sourceDeviceId = "" } = {}) {
  const relayBaseUrl = agentRelayBaseUrl || originFromUrl(updateManifestUrl);
  const relayId = safeRelayId(sourceRelayId) || agentRelayId;
  const requestedTarget = String(target || "").slice(0, 160);
  const deviceId = safeSourceText(sourceDeviceId) || agentSourceDeviceId(requestedTarget);
  const result = {
    ok: Boolean(relayBaseUrl && relayId),
    relayConfigured: Boolean(relayBaseUrl && relayId),
    relayId,
    target: requestedTarget,
    sourceDeviceId: deviceId,
    localAgent: {
      version: agentVersion,
      relay: Boolean(agentRelayId),
      relayBaseUrl: relayBaseUrl || "",
      operatorDeviceId,
      operatorDeviceNick
    },
    operatorBridge: {
      attached: Boolean(operatorBridge?.open),
      targets: operatorTargets.length
    },
    relay: null
  };
  if (!relayBaseUrl || !relayId) {
    return { ...result, ok: false, text: "! relay", exitCode: 409 };
  }
  try {
    const url = new URL("/api/agent/source/status", relayBaseUrl);
    url.searchParams.set("relayId", relayId);
    if (deviceId) {
      url.searchParams.set("deviceId", deviceId);
    }
    const response = await fetch(url, { cache: "no-store" });
    const payload = await response.json().catch(() => ({}));
    return {
      ...result,
      ok: Boolean(response.ok && payload?.ok !== false),
      relay: payload,
      sourceTargets: Array.isArray(payload?.candidates) ? payload.candidates : [],
      text: payload?.reason ? `source ${payload.reason}` : "",
      exitCode: response.ok ? 0 : response.status
    };
  } catch (error) {
    return {
      ...result,
      ok: false,
      text: `! relay-fetch: ${error instanceof Error ? error.message : String(error)}`,
      exitCode: 127
    };
  }
}

async function handleOperatorHttpActions(response, headers) {
  const jobs = await listActionJobs();
  sendJson(response, 200, headers, { ok: true, jobs });
}

async function handleOperatorHttpActionStatus(jobId, response, headers) {
  const job = await readActionJob(jobId);
  if (!job) {
    sendJson(response, 404, headers, { ok: false, text: "! action-job", exitCode: 404 });
    return;
  }
  sendJson(response, 200, headers, { ok: true, ...job });
}

async function handleOperatorHttpActionStop(jobId, response, headers) {
  const entry = await readActionJob(jobId);
  if (!entry?.job) {
    sendJson(response, 404, headers, { ok: false, text: "! action-job", exitCode: 404 });
    return;
  }
  const controller = actionControllers.get(jobId);
  if (!controller) {
    const payload = actionJobResponsePayload(entry);
    sendJson(response, payload.status === "running" ? 409 : 200, headers, payload);
    return;
  }
  controller.cancel();
  const stopped = await waitForActionJobSettle(jobId, 2500);
  const payload = actionJobResponsePayload(stopped || entry);
  sendJson(response, payload.status === "running" ? 202 : 200, headers, payload);
}

async function handleOperatorHttpAction(request, response, headers) {
  let payload;
  try {
    payload = await readJsonBody(request, 2_250_000);
  } catch {
    sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
    return;
  }
  const action = normalizeOperatorActionPayload(payload);
  if (!action.ok) {
    sendJson(response, 400, headers, { ok: false, text: action.text, exitCode: 400 });
    return;
  }
  const actionBody = action.mode === "script" ? action.script : action.command;
  const blocked = blockedManualWindowsRecoveryHandoff(actionBody);
  if (blocked) {
    recordBlockedWindowsReinstallHandoff({ kind: action.mode, command: actionBody });
    sendJson(response, 422, headers, {
      ok: false,
      status: "blocked",
      family: action.family,
      risk: action.risk,
      text: blocked,
      exitCode: 422
    });
    return;
  }
  if (action.idempotencyKey) {
    const previous = await findActionJobByIdempotencyKey(action);
    if (previous?.conflict) {
      sendJson(response, 409, headers, {
        ok: false,
        text: "! idempotency-key",
        exitCode: 409,
        jobId: previous.job?.id || "",
        statusPath: previous.job?.id ? `/operator/action/${previous.job.id}` : ""
      });
      return;
    }
    if (previous?.entry) {
      const payload = actionJobResponsePayload(previous.entry);
      sendJson(response, payload.status === "running" ? 202 : 200, headers, payload);
      return;
    }
  }
  const job = await createActionJob(action);
  const promise = runActionJob(job, action);
  if (action.detached) {
    promise.catch(() => undefined);
    sendJson(response, 202, headers, {
      ok: true,
      jobId: job.id,
      idempotencyKey: job.idempotencyKey,
      status: "running",
      family: job.family,
      risk: job.risk,
      statusPath: `/operator/action/${job.id}`,
      resultPath: job.artifacts.resultPath
    });
    return;
  }
  const result = await promise;
  sendJson(response, result.httpStatus, headers, result.payload);
}

function normalizeOperatorActionPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return { ok: false, text: "! request" };
  }
  const mode = payload.mode === "script" || typeof payload.script === "string" ? "script" : "run";
  const target = cleanActionText(payload.target, 160);
  const sourceDeviceId = safeSourceText(payload.sourceDeviceId || "");
  const sourceRelayId = safeRelayId(payload.sourceRelayId || "");
  const timeoutMs = safeRunTimeoutMs(payload.timeoutMs);
  const command = mode === "run" ? String(payload.command || "").slice(0, maxCommandChars) : "";
  const script = mode === "script" ? String(payload.script || "").slice(0, maxScriptChars) : "";
  if (!target) {
    return { ok: false, text: "! target" };
  }
  if (mode === "run" && !command.trim()) {
    return { ok: false, text: "! command" };
  }
  if (mode === "script" && !script.trim()) {
    return { ok: false, text: "! script" };
  }
  const body = mode === "script" ? script : command;
  const family = cleanActionToken(payload.family || classifySourceCommand(body), "generic");
  const actionType = cleanActionToken(payload.kind || payload.actionType || mode, mode);
  const phase = cleanActionToken(payload.phase || actionType, actionType);
  const toolkit = normalizeToolkitName(payload.toolkit || toolkitForFamily(family));
  const intent = cleanActionText(payload.intent || payload.name || family, 180);
  const commandSig = commandSignature(body, family);
  const inferredRisk = cleanActionRisk(inferActionRisk(body, family));
  const explicitRisk = cleanActionRiskOrEmpty(payload.risk);
  const risk = explicitRisk ? maxActionRisk(explicitRisk, inferredRisk) : inferredRisk;
  const reuseKey = cleanActionText(payload.reuseKey || payload.routeKey || payload.scriptKey || "", 120);
  const pivotFrom = cleanActionText(payload.pivotFrom || payload.pivotOf || payload.previousVector || "", 160);
  const successCriteria = cleanActionText(payload.successCriteria || payload.qualityTarget || payload.doneWhen || "", 220);
  const scriptUse = cleanActionText(payload.scriptUse || payload.knowledgeUse || payload.reuseUse || "", 180);
  const contextFingerprint = cleanActionText(payload.contextFingerprint || payload.environmentKey || "", 120);
  return {
    ok: true,
    mode,
    actionType,
    phase,
    toolkit,
    family,
    intent,
    target,
    sourceDeviceId,
    sourceRelayId,
    timeoutMs,
    command,
    script,
    name: cleanActionText(payload.name || (mode === "script" ? "action-script" : "action-run"), 120),
    shell: cleanActionText(payload.shell, 40),
    risk,
    detached: payload.detached === true || payload.wait === false || shouldForceDetachedAction({ family, actionType, risk }),
    createdBy: cleanActionText(payload.createdBy || "soty-agent", 80),
    idempotencyKey: cleanActionId(payload.idempotencyKey || payload.clientRequestId || payload.requestId || ""),
    commandSig,
    taskSig: taskSignature(`${toolkit} ${phase} ${family} ${intent} ${target} ${reuseKey}`),
    improvement: cleanActionText(payload.improvement || payload.improvementNote || "", 240),
    reuseKey,
    pivotFrom,
    successCriteria,
    scriptUse,
    contextFingerprint
  };
}

async function createActionJob(action) {
  const id = `act_${randomUUID().replace(/-/gu, "").slice(0, 24)}`;
  const root = join(actionJobsDir, id);
  const createdAt = new Date().toISOString();
  const job = {
    schema: "soty.action.job.v1",
    id,
    status: "created",
    toolkit: action.toolkit,
    phase: action.phase,
    mode: action.mode,
    kind: action.actionType,
    family: action.family,
    intent: action.intent,
    risk: action.risk,
    target: action.target,
    sourceDeviceId: action.sourceDeviceId,
    createdBy: action.createdBy,
    idempotencyKey: action.idempotencyKey,
    createdAt,
    startedAt: "",
    finishedAt: "",
    durationMs: 0,
    route: "",
    improvement: action.improvement,
    reuseKey: action.reuseKey,
    pivotFrom: action.pivotFrom,
    successCriteria: action.successCriteria,
    scriptUse: action.scriptUse,
    contextFingerprint: action.contextFingerprint,
    commandSig: action.commandSig,
    taskSig: action.taskSig,
    artifacts: {
      root,
      jobPath: join(root, "job.json"),
      resultPath: join(root, "result.json"),
      stdoutPath: join(root, "stdout.txt")
    }
  };
  await mkdir(root, { recursive: true });
  await writeJsonAtomic(join(root, "input.json"), {
    schema: "soty.action.input.v1",
    mode: action.mode,
    toolkit: action.toolkit,
    phase: action.phase,
    kind: action.actionType,
    family: action.family,
    intent: action.intent,
    risk: action.risk,
    target: action.target,
    sourceDeviceId: action.sourceDeviceId,
    sourceRelayId: action.sourceRelayId ? "<set>" : "",
    timeoutMs: action.timeoutMs,
    idempotencyKey: action.idempotencyKey,
    commandSig: job.commandSig,
    taskSig: job.taskSig,
    improvement: action.improvement ? "<set>" : "",
    reuseKey: action.reuseKey,
    pivotFrom: action.pivotFrom ? "<set>" : "",
    successCriteria: action.successCriteria ? "<set>" : "",
    scriptUse: action.scriptUse ? "<set>" : "",
    contextFingerprint: action.contextFingerprint,
    createdAt
  });
  await writeActionJob(job);
  actionJobs.set(id, job);
  return job;
}

async function runActionJob(job, action) {
  const started = Date.now();
  const abortController = new AbortController();
  let current = {
    ...job,
    status: "running",
    startedAt: new Date(started).toISOString()
  };
  actionJobs.set(job.id, current);
  actionControllers.set(job.id, {
    cancel: () => abortController.abort()
  });
  await writeActionJob(current);
  let execution;
  try {
    execution = await executeOperatorAction({ ...action, jobId: job.id }, abortController.signal);
  } catch (error) {
    execution = isAbortError(error)
      ? {
        ok: false,
        text: "! cancelled",
        exitCode: 130,
        route: "action-cancelled",
        target: action.target,
        sourceDeviceId: action.sourceDeviceId
      }
      : {
        ok: false,
        text: error instanceof Error ? `! action ${error.message}` : "! action",
        exitCode: 127,
        route: "action-kernel",
        target: action.target,
        sourceDeviceId: action.sourceDeviceId
      };
  } finally {
    actionControllers.delete(job.id);
  }
  const finished = Date.now();
  const exitCode = Number.isSafeInteger(execution.exitCode) ? execution.exitCode : (execution.ok ? 0 : 1);
  const status = execution.ok && exitCode === 0
    ? "ok"
    : exitCode === 130
      ? "cancelled"
      : exitCode === 124
        ? "timeout"
        : exitCode === 422
          ? "blocked"
          : "failed";
  const durationMs = Math.max(0, finished - started);
  const text = String(execution.text || "").slice(-1_000_000);
  const route = cleanActionText(execution.route || `operator-action.${action.mode}`, 120);
  const proof = appendActionMetaProof(action, enrichActionProof(action, text, buildActionProof({ action, execution: { ...execution, exitCode, route, text }, status })));
  const resultDoc = {
    schema: "soty.action.result.v1",
    jobId: job.id,
    ok: status === "ok",
    status,
    toolkit: action.toolkit,
    phase: action.phase,
    family: action.family,
    mode: action.mode,
    kind: action.actionType,
    risk: action.risk,
    idempotencyKey: action.idempotencyKey,
    target: cleanActionText(execution.target || action.target, 160),
    sourceDeviceId: cleanActionText(execution.sourceDeviceId || action.sourceDeviceId, maxSourceChars),
    route,
    exitCode,
    durationMs,
    proof,
    improvement: action.improvement,
    reuseKey: action.reuseKey,
    pivotFrom: action.pivotFrom,
    successCriteria: action.successCriteria,
    scriptUse: action.scriptUse,
    contextFingerprint: action.contextFingerprint,
    output: {
      chars: text.length,
      shape: sourceOutputShape(text),
      tail: text.slice(-12_000)
    },
    ...(execution.diagnostic && typeof execution.diagnostic === "object" ? { diagnostic: execution.diagnostic } : {}),
    startedAt: current.startedAt,
    finishedAt: new Date(finished).toISOString()
  };
  await writeFile(job.artifacts.stdoutPath, text, "utf8").catch(() => undefined);
  await writeJsonAtomic(job.artifacts.resultPath, resultDoc).catch(() => undefined);
  current = {
    ...current,
    status,
    route,
    target: resultDoc.target || current.target,
    sourceDeviceId: resultDoc.sourceDeviceId || current.sourceDeviceId,
    finishedAt: resultDoc.finishedAt,
    durationMs,
    exitCode,
    proof
  };
  actionJobs.set(job.id, current);
  await writeActionJob(current);
  recordLearningReceipt({
    kind: "action-job",
    toolkit: action.toolkit,
    phase: action.phase,
    family: action.family,
    result: status,
    route,
    commandSig: job.commandSig,
    taskSig: job.taskSig,
    proof: action.improvement ? `${proof}; improvement=${action.improvement}` : proof,
    exitCode,
    durationMs,
    ...learningContextForAction(action)
  });
  return {
    httpStatus: status === "blocked" ? 422 : 200,
    payload: {
      ok: status === "ok",
      jobId: job.id,
      idempotencyKey: job.idempotencyKey,
      status,
      toolkit: action.toolkit,
      phase: action.phase,
      family: action.family,
      risk: action.risk,
      route,
      proof,
      text: text.slice(-maxChatChars),
      ...(execution.diagnostic && typeof execution.diagnostic === "object" ? { diagnostic: execution.diagnostic } : {}),
      exitCode,
      durationMs,
      statusPath: `/operator/action/${job.id}`,
      resultPath: job.artifacts.resultPath
    }
  };
}

async function executeOperatorAction(action, signal = null) {
  if (signal?.aborted) {
    return actionCancelledResult(action);
  }
  const body = action.mode === "script" ? action.script : action.command;
  const blocked = blockedManualWindowsRecoveryHandoff(body);
  if (blocked) {
    recordBlockedWindowsReinstallHandoff({ kind: action.mode, command: body });
    return {
      ok: false,
      text: blocked,
      exitCode: 422,
      route: `action-gate.${action.family}`,
      target: action.target,
      sourceDeviceId: action.sourceDeviceId
    };
  }
  let target = action.target;
  let sourceDeviceId = action.sourceDeviceId;
  ({ target, sourceDeviceId } = await normalizeOperatorHttpTarget(target, sourceDeviceId, action.sourceRelayId));
  if (isAgentSourceTarget(target)) {
    const deviceId = agentSourceDeviceId(target);
    if (sourceDeviceId && sourceDeviceId !== deviceId) {
      return { ok: false, text: "! source-target", exitCode: 403, route: `agent-source.${action.mode}`, target, sourceDeviceId };
    }
    const sourceJobId = cleanActionId(action.jobId || "") || `act_${randomUUID().replace(/-/gu, "").slice(0, 24)}`;
    const cancelSource = () => {
      void cancelAgentSourceJob(action.sourceRelayId, deviceId, sourceJobId).catch(() => undefined);
    };
    signal?.addEventListener("abort", cancelSource, { once: true });
    const result = action.mode === "script"
      ? await postAgentSourceJob("/api/agent/source/script", {
        deviceId,
        clientJobId: sourceJobId,
        script: action.script,
        name: action.name,
        shell: action.shell,
        timeoutMs: action.timeoutMs
      }, action.sourceRelayId, 1_000_000, signal)
      : await postAgentSourceJob("/api/agent/source/run", {
        deviceId,
        clientJobId: sourceJobId,
        command: action.command,
        timeoutMs: action.timeoutMs
      }, action.sourceRelayId, 1_000_000, signal);
    signal?.removeEventListener("abort", cancelSource);
    return {
      ...result,
      route: `agent-source.${action.mode}`,
      target,
      sourceDeviceId: sourceDeviceId || deviceId
    };
  }
  if (!operatorBridge?.open || !target) {
    return { ok: false, text: "! bridge", exitCode: 409, route: `operator-bridge.${action.mode}`, target, sourceDeviceId };
  }
  const { id, promise, cancel } = registerOperatorPromiseRun(action.timeoutMs);
  const cancelBridge = () => {
    sendRaw(operatorBridge, { type: "operator.cancel", id });
    cancel(130, "! cancelled");
  };
  signal?.addEventListener("abort", cancelBridge, { once: true });
  sendRaw(operatorBridge, action.mode === "script" ? {
    type: "operator.script",
    id,
    target,
    sourceDeviceId,
    name: action.name,
    shell: action.shell,
    script: action.script
  } : {
    type: "operator.run",
    id,
    target,
    sourceDeviceId,
    command: action.command
  });
  const result = await promise;
  signal?.removeEventListener("abort", cancelBridge);
  return {
    ...result,
    route: `operator-bridge.${action.mode}`,
    target,
    sourceDeviceId
  };
}

function registerOperatorPromiseRun(timeoutMs) {
  const id = `operator_${randomUUID()}`;
  let body = "";
  let done = false;
  let timer;
  let cancelRun = () => undefined;
  const promise = new Promise((resolvePromise) => {
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
      resolvePromise({
        ok: exitCode === 0,
        text: body,
        exitCode
      });
    };
    cancelRun = finish;
    timer = setTimeout(() => finish(124, "! timeout"), timeoutMs);
    operatorRuns.set(id, {
      append: (text) => {
        body = `${body}${text}`.slice(-1_000_000);
      },
      finish
    });
  });
  return { id, promise, cancel: cancelRun };
}

function actionCancelledResult(action) {
  return {
    ok: false,
    text: "! cancelled",
    exitCode: 130,
    route: `action-cancelled.${action.mode || "run"}`,
    target: action.target,
    sourceDeviceId: action.sourceDeviceId
  };
}

function isAbortError(error) {
  return error?.name === "AbortError" || /aborted|abort/iu.test(String(error?.message || error || ""));
}

function buildActionProof({ action, execution, status }) {
  const exitCode = Number.isSafeInteger(execution.exitCode) ? execution.exitCode : (execution.ok ? 0 : 1);
  const toolkitProof = action.toolkit ? `toolkit=${action.toolkit}; phase=${action.phase || action.actionType}; ` : "";
  if (status === "ok") {
    return `${toolkitProof}exitCode=0; family=${action.family}; route=${execution.route}; output=${sourceOutputShape(execution.text)}`;
  }
  const diagnostic = sourceDiagnosticProof(execution.diagnostic);
  return `${toolkitProof}exitCode=${exitCode}; family=${action.family}; route=${execution.route}; proof=${sourceFailureProof(execution.text)}${diagnostic ? `; ${diagnostic}` : ""}`;
}

function enrichActionProof(action, text, proof) {
  if (action?.family !== "windows-reinstall") {
    return proof;
  }
  const phase = String(action.phase || action.actionType || "").toLowerCase();
  if (phase !== "arm") {
    return proof;
  }
  const parsed = parseJsonObject(text);
  const result = parsed?.result && typeof parsed.result === "object" ? parsed.result : null;
  if (result?.rebooting === true) {
    const backupOk = result.backupProof?.ok === true ? "; backupProof=ok" : "";
    return `${proof}; rebooting=true${backupOk}`;
  }
  return proof;
}

function appendActionMetaProof(action, proof) {
  const parts = [];
  if (action?.reuseKey) {
    parts.push(`reuseKey=${cleanProofToken(action.reuseKey)}`);
  }
  if (action?.pivotFrom) {
    parts.push(`pivotFrom=${cleanProofToken(action.pivotFrom)}`);
  }
  if (action?.successCriteria) {
    parts.push("successCriteria=set");
  }
  if (action?.scriptUse) {
    parts.push(`scriptUse=${cleanProofToken(action.scriptUse)}`);
  }
  if (action?.contextFingerprint) {
    parts.push(`context=${cleanProofToken(action.contextFingerprint)}`);
  }
  return parts.length > 0 ? `${proof}; ${parts.join("; ")}` : proof;
}

function cleanProofToken(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9_.:-]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 80) || "set";
}

async function writeActionJob(job) {
  await mkdir(dirname(job.artifacts.jobPath), { recursive: true });
  await writeJsonAtomic(job.artifacts.jobPath, job);
}

async function writeJsonAtomic(filePath, value) {
  const dir = dirname(filePath);
  await mkdir(dir, { recursive: true });
  const tempPath = join(dir, `.${basename(filePath)}.${process.pid}.${randomUUID()}.tmp`);
  await writeFile(tempPath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await rename(tempPath, filePath);
}

async function beginAgentTrace({ entrypoint, text, context = "", source = {} }) {
  if (!agentTraceEnabled) {
    return null;
  }
  try {
    const startedAt = new Date();
    const sig = taskSignature(text).replace(/[^a-z0-9_-]+/giu, "-");
    const traceId = `${startedAt.toISOString().replace(/[-:.TZ]/gu, "").slice(0, 14)}-${sig}-${randomUUID().slice(0, 8)}`;
    const dir = join(agentTracesDir, traceId);
    const safeSource = sanitizeAgentSource(source);
    const trace = {
      id: traceId,
      dir,
      jsonPath: join(dir, "trace.json"),
      writeQueue: Promise.resolve(),
      doc: {
        schema: "soty.agent.trace.v1",
        traceId,
        status: "running",
        version: agentVersion,
        pid: process.pid,
        platform: process.platform,
        startedAt: startedAt.toISOString(),
        endedAt: "",
        entrypoint: String(entrypoint || "agent").slice(0, 80),
        taskSig: taskSignature(text),
        textHash: hashText(String(text || "")),
        config: {
          traceFullPrompt: agentTraceFullPrompt,
          fastDirectEnabled: enableFastDirectAnswers,
          codexDisabled,
          codexFullLocalTools,
          codexRelayFallback,
          codexSessionMode,
          responseStyle: activeAgentResponseStyle.id,
          maxPromptChars: maxAgentRuntimePromptChars
        },
        input: {
          textChars: String(text || "").length,
          textPreview: redactTraceString(text, 300),
          contextChars: String(context || "").length,
          source: traceValue(safeSource, 1600, 4)
        },
        routing: {},
        codex: {
          spawned: false,
          jobDir: "",
          args: [],
          eventCount: 0,
          eventsDropped: 0,
          lastEvents: [],
          usage: emptyCodexUsage()
        },
        files: [
          "trace.json",
          "raw-user.txt",
          "visible-context.txt"
        ],
        steps: [],
        result: {}
      }
    };
    await mkdir(dir, { recursive: true });
    await Promise.all([
      writeFile(join(dir, "raw-user.txt"), `${redactTraceString(text, maxChatChars)}\n`, "utf8"),
      writeFile(join(dir, "visible-context.txt"), `${redactTraceString(context, maxAgentContextChars)}\n`, "utf8")
    ]);
    await writeJsonAtomic(trace.jsonPath, trace.doc);
    void pruneAgentTraces();
    return trace;
  } catch {
    return null;
  }
}

function traceStep(trace, name, details = {}) {
  if (!trace?.doc) {
    return;
  }
  trace.doc.steps.push({
    at: new Date().toISOString(),
    name: String(name || "step").slice(0, 100),
    details: traceValue(details, 6000, 5)
  });
  while (trace.doc.steps.length > 160) {
    trace.doc.steps.shift();
  }
  queueAgentTraceWrite(trace);
}

function traceRouting(trace, details = {}) {
  if (!trace?.doc) {
    return;
  }
  trace.doc.routing = {
    ...trace.doc.routing,
    ...traceValue(details, 4000, 5)
  };
  queueAgentTraceWrite(trace);
}

function traceCodexEvent(trace, line, event) {
  if (!trace?.doc) {
    return;
  }
  const codex = trace.doc.codex;
  codex.eventCount += 1;
  const record = {
    at: new Date().toISOString(),
    type: codexEventType(event),
    rawChars: String(line || "").length,
    event: traceValue(event, 2400, 5)
  };
  codex.lastEvents.push(record);
  if (codex.lastEvents.length > agentTraceMaxJsonEvents) {
    codex.eventsDropped += codex.lastEvents.length - agentTraceMaxJsonEvents;
    codex.lastEvents = codex.lastEvents.slice(-agentTraceMaxJsonEvents);
  }
  if (codex.eventCount === 1 || codex.eventCount % 25 === 0) {
    queueAgentTraceWrite(trace);
  }
}

function codexEventType(event) {
  if (!event || typeof event !== "object") {
    return "unknown";
  }
  return String(event.type || event.event || event.kind || event.name || event.msg?.type || event.item?.type || "unknown")
    .replace(/\s+/gu, "-")
    .slice(0, 120);
}

async function traceWriteText(trace, name, text, max = 120_000) {
  if (!trace?.doc || !name) {
    return;
  }
  const safeName = String(name).replace(/[^A-Za-z0-9_.-]+/gu, "-").slice(0, 120);
  if (!safeName) {
    return;
  }
  const body = redactTraceString(text, max);
  const filePath = join(trace.dir, safeName);
  await writeFile(filePath, body.endsWith("\n") ? body : `${body}\n`, "utf8").catch(() => undefined);
  if (!trace.doc.files.includes(safeName)) {
    trace.doc.files.push(safeName);
    queueAgentTraceWrite(trace);
  }
}

async function traceWriteJson(trace, name, value) {
  if (!trace?.doc || !name) {
    return;
  }
  const safeName = String(name).replace(/[^A-Za-z0-9_.-]+/gu, "-").slice(0, 120);
  if (!safeName) {
    return;
  }
  await writeJsonAtomic(join(trace.dir, safeName), traceValue(value, 80_000, 8)).catch(() => undefined);
  if (!trace.doc.files.includes(safeName)) {
    trace.doc.files.push(safeName);
    queueAgentTraceWrite(trace);
  }
}

function queueAgentTraceWrite(trace) {
  if (!trace?.doc) {
    return;
  }
  trace.writeQueue = Promise.resolve(trace.writeQueue)
    .catch(() => undefined)
    .then(() => writeJsonAtomic(trace.jsonPath, trace.doc))
    .catch(() => undefined);
}

async function finishAgentTrace(trace, result = {}, status = "") {
  if (!trace?.doc) {
    return;
  }
  trace.doc.status = status || (result?.ok ? "ok" : "failed");
  trace.doc.endedAt = new Date().toISOString();
  trace.doc.durationMs = Date.parse(trace.doc.endedAt) - Date.parse(trace.doc.startedAt);
  trace.doc.result = traceValue({
    ok: Boolean(result?.ok),
    exitCode: Number.isSafeInteger(result?.exitCode) ? result.exitCode : undefined,
    textChars: String(result?.text || "").length,
    textPreview: redactTraceString(result?.text || "", 600),
    messages: Array.isArray(result?.messages) ? result.messages.length : 0,
    terminal: Array.isArray(result?.terminal) ? result.terminal.length : 0
  }, 3000, 4);
  if (result?.text) {
    await traceWriteText(trace, "final.txt", result.text, maxChatChars);
  }
  queueAgentTraceWrite(trace);
  await trace.writeQueue.catch(() => undefined);
}

function withTraceId(result, trace) {
  return trace?.id ? { ...result, traceId: trace.id } : result;
}

async function pruneAgentTraces() {
  if (!agentTraceEnabled) {
    return;
  }
  const entries = await readdir(agentTracesDir, { withFileTypes: true }).catch(() => []);
  const dirs = entries
    .filter((entry) => entry.isDirectory() && /^[0-9]{14}-/u.test(entry.name))
    .map((entry) => entry.name)
    .sort();
  const remove = dirs.slice(0, Math.max(0, dirs.length - agentTraceRetain));
  for (const name of remove) {
    await rm(join(agentTracesDir, name), { recursive: true, force: true }).catch(() => undefined);
  }
}

function agentTraceStatus() {
  return {
    schema: "soty.agent.trace.v1",
    enabled: agentTraceEnabled,
    fullPrompt: agentTraceFullPrompt,
    retain: agentTraceRetain,
    maxJsonEvents: agentTraceMaxJsonEvents,
    dir: agentTracesDir
  };
}

async function handleAgentTraceList(url, response, headers) {
  const limit = Math.max(1, Math.min(Number.parseInt(url.searchParams.get("limit") || "20", 10) || 20, 200));
  const entries = await readdir(agentTracesDir, { withFileTypes: true }).catch(() => []);
  const names = entries
    .filter((entry) => entry.isDirectory() && /^[0-9]{14}-/u.test(entry.name))
    .map((entry) => entry.name)
    .sort()
    .reverse()
    .slice(0, limit);
  const traces = [];
  for (const name of names) {
    const trace = await readJsonFile(join(agentTracesDir, name, "trace.json"));
    if (trace) {
      traces.push({
        traceId: trace.traceId || name,
        status: trace.status || "unknown",
        startedAt: trace.startedAt || "",
        durationMs: Number.isSafeInteger(trace.durationMs) ? trace.durationMs : undefined,
        taskSig: trace.taskSig || "",
        textPreview: trace.input?.textPreview || "",
        family: trace.routing?.taskFamily || trace.routing?.family || "",
        route: trace.routing?.route || trace.routing?.finalRoute || "",
        ok: typeof trace.result?.ok === "boolean" ? trace.result.ok : undefined,
        exitCode: Number.isSafeInteger(trace.result?.exitCode) ? trace.result.exitCode : undefined,
        path: join(agentTracesDir, name)
      });
    }
  }
  sendJson(response, 200, headers, {
    ok: true,
    ...agentTraceStatus(),
    traces
  });
}

async function handleAgentTraceRead(traceId, response, headers) {
  const id = safeAgentTraceId(traceId);
  if (!id) {
    sendJson(response, 400, headers, { ok: false, text: "! trace-id" });
    return;
  }
  const trace = await readJsonFile(join(agentTracesDir, id, "trace.json"));
  if (!trace) {
    sendJson(response, 404, headers, { ok: false, text: "! trace-not-found" });
    return;
  }
  sendJson(response, 200, headers, {
    ok: true,
    trace,
    path: join(agentTracesDir, id)
  });
}

function safeAgentTraceId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_.-]{12,120}$/u.test(text) && !text.includes("..") ? text : "";
}

function traceValue(value, maxString = 4000, depth = 0) {
  if (value == null) {
    return value;
  }
  if (typeof value === "string") {
    return redactTraceString(value, maxString);
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  if (Array.isArray(value)) {
    if (depth <= 0) {
      return `[array:${value.length}]`;
    }
    return value.slice(0, 40).map((item) => traceValue(item, maxString, depth - 1));
  }
  if (typeof value === "object") {
    if (depth <= 0) {
      return "[object]";
    }
    const out = {};
    for (const [key, item] of Object.entries(value).slice(0, 80)) {
      out[String(key).slice(0, 120)] = traceValue(item, maxString, depth - 1);
    }
    return out;
  }
  return String(value).slice(0, 200);
}

function redactTraceString(value, max = 4000) {
  return String(value || "")
    .replace(/\r\n?/gu, "\n")
    .replace(/(api[_-]?key|authorization|bearer|token|secret|password|passwd|cap_sid)\s*[:=]\s*['"]?[^'"\s]+/giu, "$1=<redacted>")
    .replace(/\b(?:sk|sess|cap|pat|ghp|github_pat)_[A-Za-z0-9_-]{16,}\b/gu, "<redacted-token>")
    .replace(/[A-Za-z0-9+/]{80,}={0,2}/gu, "<redacted-long-token>")
    .slice(0, Math.max(0, max));
}

async function readActionJob(jobId) {
  if (!/^[A-Za-z0-9_-]{8,96}$/u.test(String(jobId || ""))) {
    return null;
  }
  const live = actionJobs.get(jobId);
  const root = live?.artifacts?.root || join(actionJobsDir, jobId);
  const jobPath = live?.artifacts?.jobPath || join(root, "job.json");
  const resultPath = live?.artifacts?.resultPath || join(root, "result.json");
  const job = live || await readJsonFile(jobPath);
  if (!job) {
    return null;
  }
  const result = await readJsonFile(resultPath);
  const hydratedJob = hydrateActionJob(job, { live: Boolean(live), result });
  return {
    job: hydratedJob,
    ...(result ? { result } : {})
  };
}

async function listActionJobs() {
  const entries = await readdir(actionJobsDir, { withFileTypes: true }).catch(() => []);
  const jobs = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const item = await readActionJob(entry.name);
    if (item?.job) {
      jobs.push(summarizeActionJob(item.job));
    }
  }
  for (const live of actionJobs.values()) {
    if (!jobs.some((item) => item.id === live.id)) {
      jobs.push(summarizeActionJob(live));
    }
  }
  return jobs
    .sort((left, right) => String(right.startedAt || right.createdAt).localeCompare(String(left.startedAt || left.createdAt)))
    .slice(0, 40);
}

async function waitForActionJobSettle(jobId, timeoutMs) {
  const deadline = Date.now() + Math.max(100, timeoutMs || 1000);
  let last = await readActionJob(jobId);
  while (Date.now() < deadline) {
    const status = String(last?.result?.status || last?.job?.status || "");
    if (status && status !== "created" && status !== "running") {
      return last;
    }
    await sleep(80);
    last = await readActionJob(jobId);
  }
  return last;
}

async function findActionJobByIdempotencyKey(action) {
  const key = cleanActionId(action.idempotencyKey);
  if (!key) {
    return null;
  }
  for (const job of actionJobs.values()) {
    if (job.idempotencyKey === key) {
      const entry = await readActionJob(job.id) || { job };
      return job.commandSig === action.commandSig
        ? { entry }
        : { conflict: true, job };
    }
  }
  const entries = await readdir(actionJobsDir, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const item = await readActionJob(entry.name);
    const job = item?.job;
    if (job?.idempotencyKey !== key) {
      continue;
    }
    return job.commandSig === action.commandSig
      ? { entry: item }
      : { conflict: true, job };
  }
  return null;
}

function actionJobResponsePayload(entry) {
  const job = entry?.job || {};
  const result = entry?.result || null;
  const status = cleanActionText(result?.status || job.status || "unknown", 24);
  const exitCode = Number.isSafeInteger(result?.exitCode)
    ? result.exitCode
    : Number.isSafeInteger(job.exitCode)
      ? job.exitCode
      : status === "ok"
        ? 0
        : status === "running"
          ? undefined
          : 1;
  return {
    ok: status === "ok",
    jobId: cleanActionText(job.id, 96),
    idempotencyKey: cleanActionText(job.idempotencyKey, 120),
    status,
    toolkit: cleanActionText(result?.toolkit || job.toolkit, 80),
    phase: cleanActionText(result?.phase || job.phase, 80),
    family: cleanActionText(result?.family || job.family, 80),
    risk: cleanActionText(result?.risk || job.risk, 20),
    route: cleanActionText(result?.route || job.route, 120),
    proof: cleanActionText(result?.proof || job.proof, 900),
    text: String(result?.output?.tail || "").slice(-maxChatChars),
    ...(result?.diagnostic && typeof result.diagnostic === "object" ? { diagnostic: result.diagnostic } : {}),
    ...(exitCode === undefined ? {} : { exitCode }),
    ...(Number.isSafeInteger(result?.durationMs) || Number.isSafeInteger(job.durationMs)
      ? { durationMs: Number.isSafeInteger(result?.durationMs) ? result.durationMs : job.durationMs }
      : {}),
    statusPath: job.id ? `/operator/action/${job.id}` : "",
    resultPath: result ? cleanActionText(job.artifacts?.resultPath, 260) : ""
  };
}

function hydrateActionJob(job, { live = false, result = null } = {}) {
  if (!job || typeof job !== "object") {
    return job;
  }
  if (result && typeof result === "object") {
    return {
      ...job,
      status: cleanActionText(result.status || job.status, 24),
      toolkit: cleanActionText(result.toolkit || job.toolkit, 80),
      phase: cleanActionText(result.phase || job.phase, 80),
      route: cleanActionText(result.route || job.route, 120),
      finishedAt: cleanActionText(result.finishedAt || job.finishedAt, 80),
      durationMs: Number.isSafeInteger(result.durationMs) ? result.durationMs : job.durationMs,
      exitCode: Number.isSafeInteger(result.exitCode) ? result.exitCode : job.exitCode,
      proof: cleanActionText(result.proof || job.proof, 900)
    };
  }
  if (!live && (job.status === "created" || job.status === "running")) {
    return {
      ...job,
      status: "interrupted",
      exitCode: 127,
      proof: "local action supervisor exited before result artifact"
    };
  }
  return job;
}

function summarizeActionJob(job) {
  return {
    id: cleanActionText(job.id, 96),
    idempotencyKey: cleanActionText(job.idempotencyKey, 120),
    status: cleanActionText(job.status, 24),
    toolkit: cleanActionText(job.toolkit, 80),
    phase: cleanActionText(job.phase, 80),
    family: cleanActionText(job.family, 80),
    mode: cleanActionText(job.mode, 20),
    kind: cleanActionText(job.kind, 80),
    risk: cleanActionText(job.risk, 20),
    target: cleanActionText(job.target, 160),
    route: cleanActionText(job.route, 120),
    exitCode: Number.isSafeInteger(job.exitCode) ? job.exitCode : undefined,
    durationMs: Number.isSafeInteger(job.durationMs) ? job.durationMs : undefined,
    createdAt: cleanActionText(job.createdAt, 80),
    startedAt: cleanActionText(job.startedAt, 80),
    finishedAt: cleanActionText(job.finishedAt, 80),
    proof: cleanActionText(job.proof, 240),
    statusPath: `/operator/action/${cleanActionText(job.id, 96)}`
  };
}

function cleanActionRisk(value) {
  const text = String(value || "").trim().toLowerCase();
  return ["low", "medium", "high", "destructive"].includes(text) ? text : "medium";
}

function cleanActionRiskOrEmpty(value) {
  const text = String(value || "").trim().toLowerCase();
  return ["low", "medium", "high", "destructive"].includes(text) ? text : "";
}

function maxActionRisk(left, right) {
  const ranks = { low: 0, medium: 1, high: 2, destructive: 3 };
  return (ranks[cleanActionRisk(left)] >= ranks[cleanActionRisk(right)])
    ? cleanActionRisk(left)
    : cleanActionRisk(right);
}

function shouldForceDetachedAction({ family, actionType, risk }) {
  if (family === "windows-reinstall") {
    return true;
  }
  if (risk === "high" || risk === "destructive") {
    return true;
  }
  return actionType === "prepare" && risk !== "low";
}

function toolkitForFamily(family) {
  const token = cleanActionToken(family, "generic");
  if (token === "windows-reinstall") {
    return "windows-reinstall";
  }
  return "durable-action";
}

function normalizeToolkitName(value) {
  const token = cleanActionToken(value, "durable-action");
  if (["windows-reinstall", "durable-action", "console", "software", "generic"].includes(token)) {
    return token === "generic" ? "durable-action" : token;
  }
  return token;
}

function cleanActionId(value) {
  return String(value || "")
    .trim()
    .replace(/[^A-Za-z0-9_.:-]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 120);
}

function inferActionRisk(text, family) {
  const lower = String(text || "").toLowerCase();
  if (family === "windows-reinstall" || /\b(format-volume|clear-disk|diskpart|systemreset|reagentc|bcdedit|remove-item\s+-recurse|rm\s+-rf)\b/u.test(lower)) {
    return "high";
  }
  if (/\b(install|upgrade|update|set-|new-|remove-|delete|restart|stop-service|start-service|winget|msiexec)\b/u.test(lower)) {
    return "medium";
  }
  return "low";
}

function cleanActionToken(value, fallback = "generic") {
  const text = String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9_.:-]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 80);
  return text || fallback;
}

function cleanActionText(value, max) {
  return String(value || "")
    .replace(/[\u0000-\u001F\u007F]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

async function handleAgentSourceHttpRun(target, sourceDeviceId, command, timeoutMs, response, headers, sourceRelayId = "") {
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
  }, sourceRelayId);
  rememberAgentSourceOutcome({ kind: "run", command, result });
  sendJson(response, 200, headers, result);
}

async function normalizeOperatorHttpTarget(target, sourceDeviceId, sourceRelayId = "") {
  if (isAgentSourceTarget(target)) {
    return { target, sourceDeviceId };
  }
  const sourceTarget = operatorHttpAgentSourceTarget(target, sourceDeviceId, await activeAgentSourceTargets(sourceRelayId));
  if (!sourceTarget) {
    return { target, sourceDeviceId };
  }
  const deviceId = agentSourceDeviceId(sourceTarget.id);
  return {
    target: sourceTarget.id,
    sourceDeviceId: deviceId || sourceDeviceId || ""
  };
}

function operatorHttpAgentSourceTarget(target, sourceDeviceId, sourceTargets) {
  const sources = sanitizeTargets(sourceTargets);
  if (sources.length === 0) {
    return null;
  }
  const targetText = String(target || "").trim();
  const needle = cleanTargetNeedle(targetText);
  const requestedDeviceId = String(sourceDeviceId || "").trim();
  const operatorTarget = operatorTargetByText(targetText);
  const operatorDeviceId = requestedDeviceId
    || operatorTarget?.hostDeviceId
    || (operatorTarget?.deviceIds?.length === 1 ? operatorTarget.deviceIds[0] : "");
  if (operatorDeviceId) {
    const byDevice = sources.find((item) => item.hostDeviceId === operatorDeviceId || item.deviceIds.includes(operatorDeviceId));
    if (byDevice) {
      return byDevice;
    }
  }
  if (!needle) {
    return null;
  }
  return sources.find((item) => cleanTargetNeedle(item.label) === needle)
    || sources.find((item) => item.id.toLowerCase() === needle)
    || sources.find((item) => cleanTargetNeedle(item.label).includes(needle))
    || null;
}

function operatorTargetByText(target) {
  const needle = cleanTargetNeedle(target);
  if (!needle) {
    return null;
  }
  return operatorTargets.find((item) => item.id === target || item.id.toLowerCase() === needle)
    || operatorTargets.find((item) => cleanTargetNeedle(item.label) === needle)
    || operatorTargets.find((item) => cleanTargetNeedle(item.label).includes(needle))
    || null;
}

async function handleAgentSourceHttpScript(target, sourceDeviceId, payload, timeoutMs, response, headers, sourceRelayId = "") {
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
  }, sourceRelayId);
  rememberAgentSourceOutcome({ kind: "script", command: payload.script, result });
  sendJson(response, 200, headers, result);
}

async function postAgentSourceJob(path, body, relayId = "", maxTextLength = maxChatChars, signal = null) {
  const relayBaseUrl = agentRelayBaseUrl || originFromUrl(updateManifestUrl);
  const jobRelayId = safeRelayId(relayId) || agentRelayId;
  if (!relayBaseUrl || !jobRelayId) {
    return { ok: false, text: "! relay", exitCode: 409 };
  }
  const asyncResult = await postAgentSourceJobAsync(path, body, jobRelayId, relayBaseUrl, maxTextLength, signal);
  if (asyncResult?.supported !== false) {
    return asyncResult;
  }
  return await postAgentSourceJobSync(path, body, jobRelayId, relayBaseUrl, maxTextLength, signal);
}

async function postAgentSourceJobAsync(path, body, jobRelayId, relayBaseUrl, maxTextLength = maxChatChars, signal = null) {
  const type = path.endsWith("/script") ? "script" : "run";
  const start = await fetchAgentSourceJson(new URL("/api/agent/source/start", relayBaseUrl), {
    relayId: jobRelayId,
    type,
    ...body
  }, signal);
  if (start.unsupported) {
    return { supported: false };
  }
  if (!start.payload?.ok || !start.payload?.id) {
    return agentSourcePayloadResult(start.payload, start.httpStatus, maxTextLength);
  }
  const deviceId = safeSourceText(body?.deviceId || "");
  const jobId = cleanActionId(start.payload.id);
  const timeoutMs = safeDurationMs(body?.timeoutMs, defaultTimeoutMs, maxLongTaskTimeoutMs);
  const pickupTimeoutMs = sourceJobPickupTimeoutMs(timeoutMs);
  const started = Date.now();
  let leasedAt = 0;
  let lastPayload = start.payload;
  while (true) {
    if (signal?.aborted) {
      await cancelAgentSourceJob(jobRelayId, deviceId, jobId).catch(() => undefined);
      return { ok: false, text: "! cancelled", exitCode: 130 };
    }
    await sleep(sourceJobPollDelayMs(Date.now() - started));
    const statusUrl = new URL("/api/agent/source/job", relayBaseUrl);
    statusUrl.searchParams.set("relayId", jobRelayId);
    statusUrl.searchParams.set("deviceId", deviceId);
    statusUrl.searchParams.set("id", jobId);
    const status = await fetchAgentSourceJson(statusUrl, null, signal);
    lastPayload = status.payload || lastPayload;
    if (!status.payload || !status.payload.ok || sourceJobTerminal(status.payload)) {
      return agentSourcePayloadResult(status.payload, status.httpStatus, maxTextLength);
    }
    const job = status.payload?.diagnostic?.job && typeof status.payload.diagnostic.job === "object"
      ? status.payload.diagnostic.job
      : {};
    if (!leasedAt && job.leased === true) {
      const parsedLeasedAt = Date.parse(String(job.leasedAt || ""));
      leasedAt = Number.isFinite(parsedLeasedAt) ? parsedLeasedAt : Date.now();
    }
    if (!leasedAt) {
      if (Date.now() - started <= pickupTimeoutMs) {
        continue;
      }
      return {
        ok: false,
        text: String(lastPayload?.text || "! pickup timeout").slice(0, Math.max(1, Math.min(maxTextLength, 1_000_000))),
        exitCode: 124,
        diagnostic: {
          kind: "source-job",
          reason: "pickup-timeout",
          jobId,
          status: String(lastPayload?.status || "queued").slice(0, 80),
          last: lastPayload?.diagnostic && typeof lastPayload.diagnostic === "object" ? lastPayload.diagnostic : undefined
        }
      };
    }
    if (Date.now() - leasedAt <= timeoutMs + 3000) {
      continue;
    }
    await cancelAgentSourceJob(jobRelayId, deviceId, jobId).catch(() => undefined);
    break;
  }
  return {
    ok: false,
    text: String(lastPayload?.text || "! timeout").slice(0, Math.max(1, Math.min(maxTextLength, 1_000_000))),
    exitCode: 124,
    diagnostic: {
      kind: "source-job",
      reason: "poll-timeout",
      jobId,
      status: String(lastPayload?.status || "running").slice(0, 80),
      last: lastPayload?.diagnostic && typeof lastPayload.diagnostic === "object" ? lastPayload.diagnostic : undefined
    }
  };
}

async function postAgentSourceJobSync(path, body, jobRelayId, relayBaseUrl, maxTextLength = maxChatChars, signal = null) {
  try {
    const response = await fetch(new URL(path, relayBaseUrl), {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        relayId: jobRelayId,
        ...body
      }),
      ...(signal ? { signal } : {})
    });
    const responseText = await response.text();
    const payload = parseAgentSourceJson(responseText);
    if (!payload) {
      return {
        ok: false,
        text: "! relay-json: invalid response from Soty relay",
        exitCode: response.ok ? 502 : (response.status || 502),
        diagnostic: {
          kind: "relay-json",
          httpStatus: response.status,
          bodyPreview: responseText.slice(0, 240)
        }
      };
    }
    return {
      ok: Boolean(response.ok && payload?.ok),
      text: String(payload?.text || "").slice(0, Math.max(1, Math.min(maxTextLength, 1_000_000))),
      exitCode: Number.isSafeInteger(payload?.exitCode) ? payload.exitCode : (response.ok ? 0 : response.status),
      httpStatus: response.status,
      ...(payload?.diagnostic && typeof payload.diagnostic === "object" ? { diagnostic: payload.diagnostic } : {}),
      ...(typeof payload?.reason === "string" ? { reason: payload.reason.slice(0, 120) } : {})
    };
  } catch (error) {
    if (isAbortError(error) || signal?.aborted) {
      return { ok: false, text: "! cancelled", exitCode: 130 };
    }
    const message = error instanceof Error ? error.message : String(error);
    return {
      ok: false,
      text: `! relay-fetch: ${message}`.slice(0, maxChatChars),
      exitCode: 127,
      diagnostic: {
        kind: "relay-fetch",
        message: message.slice(0, 500)
      }
    };
  }
}

async function fetchAgentSourceJson(url, body = null, signal = null) {
  try {
    const response = await fetch(url, {
      method: body ? "POST" : "GET",
      cache: "no-store",
      headers: body ? { "Content-Type": "application/json" } : {},
      ...(body ? { body: JSON.stringify(body) } : {}),
      ...(signal ? { signal } : {})
    });
    const responseText = await response.text();
    const payload = parseAgentSourceJson(responseText);
    if (!payload) {
      return {
        payload: {
          ok: false,
          text: "! relay-json: invalid response from Soty relay",
          exitCode: response.ok ? 502 : (response.status || 502),
          diagnostic: {
            kind: "relay-json",
            httpStatus: response.status,
            bodyPreview: responseText.slice(0, 240)
          }
        },
        httpStatus: response.status
      };
    }
    const unsupported = response.status === 404
      && payload?.ok === false
      && !payload.text
      && !payload.diagnostic
      && !payload.id;
    return { payload, httpStatus: response.status, unsupported };
  } catch (error) {
    if (isAbortError(error) || signal?.aborted) {
      return { payload: { ok: false, text: "! cancelled", exitCode: 130 }, httpStatus: 499 };
    }
    const message = error instanceof Error ? error.message : String(error);
    return {
      payload: {
        ok: false,
        text: `! relay-fetch: ${message}`.slice(0, maxChatChars),
        exitCode: 127,
        diagnostic: {
          kind: "relay-fetch",
          message: message.slice(0, 500)
        }
      },
      httpStatus: 0
    };
  }
}

function agentSourcePayloadResult(payload, httpStatus = 0, maxTextLength = maxChatChars) {
  const safePayload = payload && typeof payload === "object" ? payload : {};
  const exitCode = Number.isSafeInteger(safePayload.exitCode)
    ? safePayload.exitCode
    : (safePayload.ok === true ? 0 : (httpStatus || 1));
  return {
    ok: Boolean(safePayload.ok && exitCode === 0),
    text: String(safePayload.text || "").slice(0, Math.max(1, Math.min(maxTextLength, 1_000_000))),
    exitCode,
    httpStatus,
    ...(safePayload.diagnostic && typeof safePayload.diagnostic === "object" ? { diagnostic: safePayload.diagnostic } : {}),
    ...(typeof safePayload.reason === "string" ? { reason: safePayload.reason.slice(0, 120) } : {}),
    ...(typeof safePayload.status === "string" ? { status: safePayload.status.slice(0, 80) } : {}),
    ...(typeof safePayload.id === "string" ? { sourceJobId: safePayload.id.slice(0, 120) } : {})
  };
}

function sourceJobTerminal(payload) {
  const status = String(payload?.status || "").toLowerCase();
  return Number.isSafeInteger(payload?.exitCode) || ["ok", "failed", "timeout", "cancelled", "missing"].includes(status);
}

function sourceJobPollDelayMs(elapsedMs) {
  if (elapsedMs < 5000) {
    return 500;
  }
  if (elapsedMs < 60_000) {
    return 1500;
  }
  return 5000;
}

function sourceJobPickupTimeoutMs(timeoutMs) {
  const safe = safeDurationMs(timeoutMs, defaultTimeoutMs, maxLongTaskTimeoutMs);
  return Math.max(sourceJobPickupBaseMs, Math.min(10 * 60_000, safe + sourceJobPickupBaseMs));
}

function parseAgentSourceJson(value) {
  try {
    const parsed = JSON.parse(String(value || ""));
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

async function cancelAgentSourceJob(relayId, deviceId, jobId) {
  const relayBaseUrl = agentRelayBaseUrl || originFromUrl(updateManifestUrl);
  const jobRelayId = safeRelayId(relayId) || agentRelayId;
  if (!relayBaseUrl || !jobRelayId || !deviceId || !jobId) {
    return { ok: false };
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 3000);
  try {
    const response = await fetch(new URL("/api/agent/source/cancel", relayBaseUrl), {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        relayId: jobRelayId,
        deviceId,
        id: jobId
      }),
      signal: controller.signal
    });
    const payload = await response.json().catch(() => ({}));
    return { ok: Boolean(response.ok && payload?.ok) };
  } finally {
    clearTimeout(timer);
  }
}

function rememberAgentSourceOutcome({ kind, command, result }) {
  const family = classifySourceCommand(command);
  const exitCode = Number.isSafeInteger(result?.exitCode) ? result.exitCode : (result?.ok ? 0 : 1);
  const ok = Boolean(result?.ok && exitCode === 0);
  const diagnostic = sourceDiagnosticProof(result?.diagnostic);
  recordLearningReceipt({
    kind: "source-command",
    family,
    result: ok ? "ok" : exitCode === 124 ? "timeout" : "failed",
    route: `agent-source.${kind}`,
    commandSig: commandSignature(command, family),
    proof: ok
      ? `exitCode=0; output=${sourceOutputShape(result?.text)}`
      : `exitCode=${exitCode}; proof=${sourceFailureProof(result?.text)}${diagnostic ? `; ${diagnostic}` : ""}`,
    exitCode
  });
  if (process.env.SOTY_AGENT_REMEMBER_OUTCOMES !== "1") {
    return;
  }
  if (ok && family === "generic") {
    return;
  }
  if (ok && family === "identity-probe") {
    return;
  }
}

function classifyRoutineSourceTask(lower) {
  const text = String(lower || "");
  if (/driver|pnputil|devmgmt|device manager|problem device|устройств|драйвер|диспетчер/u.test(text)) {
    return "driver-check";
  }
  if (/battery|powercfg|sleep|lid|power plan|заряд|батаре|питани|сон|крышк/u.test(text)) {
    return "power-check";
  }
  if (/(?:\bport\b|listener|listen|tcp|udp|netstat|порт|слуша|соединен)/u.test(text)) {
    return "system-check";
  }
  if (hasExplicitEventLogIntent(text)) {
    return "system-check";
  }
  if (/notepad|calc|calculator|paint|process|pid|start-process|stop-process/u.test(text)) {
    return "program-control";
  }
  if (/script|powershell-скрипт|\.ps1|скрипт/u.test(text)) {
    return "script-task";
  }
  if (/internet|web|browser|curl|invoke-webrequest|официальн|сайт|ссылк|релиз|lts|github|node\.js|powershell/u.test(text)
    && /(official|официальн|релиз|release|lts|stable|стабиль|ссылк|link|github)/u.test(text)) {
    return "web-lookup";
  }
  if (/(?:winget|where\.exe|where\s+|which\s+|installed|version|версии?|установлен[аоы]?|наличи|программ|приложени|git|node|npm|python|pwsh|powershell)\b/u.test(text)
    && !/\b(?:install|upgrade|uninstall|remove)\b|установи|обнови|удали/u.test(text)) {
    return "software-check";
  }
  if (/internet|web|browser|curl|invoke-webrequest|официальн|сайт|ссылк|релиз|lts|github|node\.js|powershell/u.test(text)) {
    return "web-lookup";
  }
  if (/uptime|ram|memory|disk|cpu|bits|windows update|ipv4|ip address|gateway|dns|defender|firewall|памят|диск|шлюз|сеть|сетев|защит|брандмауэр/u.test(text)) {
    return "system-check";
  }
  if (/temp|file|folder|directory|report\.txt|hash|checksum|zip|archive|compress|файл|папк|архив|отчет|отчёт|создай папку|удали скрипт/u.test(text)) {
    return "file-work";
  }
  if (/notepad|calc|calculator|paint|process|pid|start-process|stop-process|блокнот|калькулятор|процесс|запусти|закрой/u.test(text)) {
    return "program-control";
  }
  return "";
}

function hasExplicitEventLogIntent(text) {
  const value = String(text || "").toLowerCase();
  if (/(?:event\s*log|eventlog|winlog|eventvwr|журнал\s+событи|событи[яй]?\s+windows|windows\s+events|системн\w*\s+журнал)/iu.test(value)) {
    return true;
  }
  const hasErrorWord = /\b(?:errors?|critical|criticals?)\b|ошиб|критич/iu.test(value);
  if (!hasErrorWord) {
    return false;
  }
  const hasSystemAnchor = /\b(?:windows|system|win)\b|винд|систем|журнал|событ|event|за\s+\d{1,3}\s*(?:h|ч|час)|24\s*(?:h|ч|час)|последн|last\s+\d/iu.test(value);
  const hasProbeVerb = /\b(?:check|show|list|find|diagnos|inspect)\b|проверь|проверить|посмотри|покажи|найди|выведи|диагност|последн/iu.test(value);
  return hasSystemAnchor && hasProbeVerb;
}

function isContextualOrCorrectionMessage(text) {
  const value = String(text || "").toLowerCase();
  return /(?:\b(?:same|again|previous|instead|fix|redo|wrong)\b|то\s+же|т[оа]же\s+сам|так\s+же|предыдущ|прошл|снова|ещ[её]|только|исправь|переделай|не\s+то|не\s+туда|ошиб)/iu.test(value);
}

function isCreativeOrGenerativeMessage(text) {
  const value = String(text || "").toLowerCase();
  return /(?:\b(?:generate|draw|image|photo|wallpaper|realistic|ultra|style|prompt)\b|сгенер|генерир|нарис|картинк|изображ|фото|обо[и]|ультрареал|реалист|стил|промпт)/iu.test(value);
}

function hasExplicitRoutineIntent(text) {
  const value = String(text || "").toLowerCase();
  return /(?:\b(?:check|show|list|find|diagnos|inspect|open|start|close|create|archive|count|set|turn|run|launch)\b|проверь|проверить|посмотри|покажи|найди|узнай|выведи|диагност|открой|запусти|закрой|создай|заархивируй|посчитай|сколько|какой|какая|поставь|включи|выключи)/iu.test(value);
}

function hasRoutineTechnicalAnchor(text, family, kind = "") {
  const value = String(text || "").toLowerCase();
  const normalizedFamily = cleanActionToken(family, "");
  const normalizedKind = cleanActionToken(kind, "");
  if (normalizedKind === "system-eventlog-critical") {
    return hasExplicitEventLogIntent(value);
  }
  if (normalizedFamily === "driver-check") {
    return /driver|pnputil|devmgmt|device manager|problem device|драйвер|диспетчер|устройств|pnp/iu.test(value);
  }
  if (normalizedFamily === "power-check") {
    return /battery|powercfg|sleep|lid|power plan|заряд|батаре|питани|сон|крышк/iu.test(value);
  }
  if (normalizedFamily === "software-check") {
    return /winget|where\.exe|where\s+|which\s+|installed|version|верси|установлен|наличи|программ|приложени|git|node|npm|python|pwsh|powershell/iu.test(value);
  }
  if (normalizedFamily === "program-control") {
    return /notepad|calc|calculator|paint|process|pid|start-process|stop-process|блокнот|калькулятор|процесс/iu.test(value);
  }
  if (normalizedFamily === "file-work") {
    return /temp|tmp|file|folder|directory|report\.txt|hash|checksum|zip|archive|compress|файл|папк|архив|отчет|отчёт/iu.test(value);
  }
  if (normalizedFamily === "web-lookup") {
    return /internet|web|browser|curl|invoke-webrequest|официальн|сайт|ссылк|релиз|lts|github|node\.js|powershell|stable/iu.test(value);
  }
  if (normalizedFamily === "script-task") {
    return /script|powershell|\.ps1|скрипт/iu.test(value);
  }
  if (["system-check", "service-check", "identity-probe"].includes(normalizedFamily)) {
    return /(?:\bport\b|listener|listen|tcp|udp|netstat|uptime|ram|memory|disk|cpu|bits|windows update|ipv4|ip address|gateway|dns|defender|firewall|service|служб|порт|памят|диск|шлюз|сеть|защит|брандмауэр)/iu.test(value)
      || hasExplicitEventLogIntent(value);
  }
  return false;
}

function shouldRunDeterministicFastRoutine(text, spec) {
  if (!spec) {
    return false;
  }
  if (cleanActionToken(spec.kind, "") === "system-eventlog-critical") {
    return hasExplicitEventLogIntent(text);
  }
  if (isCreativeOrGenerativeMessage(text)) {
    return false;
  }
  const explicit = hasExplicitRoutineIntent(text);
  const anchored = hasRoutineTechnicalAnchor(text, spec.family, spec.kind);
  if (isContextualOrCorrectionMessage(text) && !explicit && !anchored) {
    return false;
  }
  return explicit || anchored;
}

function isRoutineAgentTaskFamily(family) {
  return ["program-control", "file-work", "system-check", "service-check", "identity-probe", "script-task", "web-lookup", "power-check", "driver-check", "software-check", "audio-volume", "audio-mute"].includes(cleanActionToken(family, ""));
}

function classifySourceCommand(command) {
  const lower = String(command || "").toLowerCase();
  const routineFamily = classifyRoutineSourceTask(lower);
  if (routineFamily) {
    return routineFamily;
  }
  if (/utf-?8|unicode|codepage|chcp|outputencoding|inputencoding|windowsidentity|text\.encoding|[Рр]РєСЂР°Рє|кракозябр|кодиров/u.test(lower)) {
    return "encoding-identity";
  }
  if (/\b(whoami|hostname)\b|computername|username/u.test(lower)) {
    return "identity-probe";
  }
  if (/volume|mute|audio|sound|endpointvolume|nircmd|sndvol|speaker|mic|микрофон|звук|громк/u.test(lower)) {
    return /mute|muted|выключ/u.test(lower) ? "audio-mute" : "audio-volume";
  }
  if (/driver|pnputil|devmgmt|device manager|драйвер/u.test(lower)) {
    return "driver-check";
  }
  if (/systemreset|reagentc\s+\/boottore/u.test(lower)) {
    return "windows-reinstall";
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

function tryFastDirectAgentReply({ text, source, startedAt }) {
  if (!enableFastDirectAnswers) {
    return null;
  }
  const spec = fastDirectAnswerSpec(text);
  if (!spec) {
    return null;
  }
  const finalText = spec.text;
  recordLearningReceipt({
    kind: "agent-runtime",
    family: "direct-answer",
    result: "ok",
    route: "agent-runtime.fast-direct-answer",
    taskSig: taskSignature(text),
    proof: `exitCode=0; direct=${spec.kind}; final=nonempty; tokens=actual; input=0; output=0; total=0; cached=0`,
    exitCode: 0,
    durationMs: Date.now() - startedAt,
    ...learningContextForTurn(source, null)
  });
  return {
    ok: true,
    text: finalText.slice(0, maxChatChars),
    exitCode: 0
  };
}

function fastDirectAnswerSpec(text) {
  const body = String(text || "");
  const lower = body.toLowerCase();
  if (!isPlainNonDeviceTask(body)) {
    return null;
  }
  const steps = fastRequestedStepCount(body, 5);
  if (/omelet|омлет|яичниц/iu.test(lower)) {
    const lines = [
      "1. Взбей яйца с солью и ложкой молока или воды.",
      "2. Разогрей сковороду и растопи немного масла.",
      "3. Влей смесь, убавь огонь до среднего.",
      "4. Жарь до схватывания краев, середину слегка подтягивай лопаткой.",
      "5. Накрой на минуту, сними с огня и сразу подавай."
    ];
    return { kind: "recipe-omelet", text: lines.slice(0, steps).join("\n") };
  }
  if (/разминк|тренировк|зарядк|workout|warm-?up|exercise/iu.test(lower)) {
    const lines = [
      "1. Круги плечами и шеей - 1 минута.",
      "2. Приседания в спокойном темпе - 1 минута.",
      "3. Наклоны и мягкая растяжка спины - 1 минута.",
      "4. Выпады назад без рывков - 1 минута.",
      "5. Легкая планка или шаг на месте - 1 минута."
    ];
    return { kind: "simple-workout", text: lines.slice(0, steps).join("\n") };
  }
  return null;
}

function isPlainNonDeviceTask(text) {
  const lower = String(text || "").toLowerCase();
  return /без компьютера|не используй компьютер|не трогай компьютер|no computer|without computer/iu.test(lower)
    || (/(омлет|рецепт|готовк|сковород|яичниц|разминк|тренировк|зарядк|workout|warm-?up|exercise)/iu.test(lower) && !/(файл|папк|windows|powershell|cmd|браузер|интернет|сайт|программ|служб|процесс|pid|диск|сеть)/iu.test(lower));
}

function fastRequestedStepCount(text, fallback) {
  const match = /(\d{1,2})\s*(?:шаг|step)/iu.exec(String(text || ""));
  if (!match) {
    return fallback;
  }
  return Math.max(1, Math.min(12, Number.parseInt(match[1], 10) || fallback));
}

async function tryFastRoutineAgentReply({ text, source, target, taskFamily, startedAt, learningContext }) {
  const spec = fastRoutineSpecForTask(text, taskFamily);
  if (!spec || !target?.id) {
    return null;
  }
  if (!shouldRunDeterministicFastRoutine(text, spec)) {
    return null;
  }
  const deviceId = agentSourceDeviceId(target.id) || bridgeSourceDeviceId(target, source);
  if (!deviceId) {
    return null;
  }
  const result = isAgentSourceTarget(target.id)
    ? await postAgentSourceJob("/api/agent/source/script", {
      deviceId,
      script: spec.script,
      shell: "powershell",
      name: spec.name,
      timeoutMs: spec.timeoutMs
    }, safeRelayId(source?.sourceRelayId || ""))
    : target.access === true
      ? await postLocalOperatorScript(target.id, deviceId, {
        script: spec.script,
        shell: "powershell",
        name: spec.name
      }, spec.timeoutMs)
      : null;
  if (!result) {
    return null;
  }
  const parsed = parseFastRoutineJson(result?.text || "");
  const quality = parsed ? fastRoutineQuality(spec, parsed, text) : { ok: false, score: 0, missing: ["json"] };
  const ok = Boolean(result?.ok && parsed && quality.ok);
  const finalText = ok
    ? spec.format(parsed)
    : agentFailureText(result?.text || "fast routine did not return proof");
  recordLearningReceipt({
    kind: "agent-runtime",
    family: spec.family,
    result: ok ? "ok" : parsed ? "partial" : "failed",
    route: "agent-runtime.fast-source-script",
    commandSig: commandSignature(spec.name, spec.family),
    taskSig: taskSignature(text),
    proof: `exitCode=${ok ? 0 : result?.exitCode || 1}; fastRoutine=${spec.kind}; final=${finalText ? "nonempty" : "empty"}; quality=${quality.ok ? "pass" : "fail"}; qualityScore=${quality.score}; missing=${quality.missing.join(",").slice(0, 160)}; tokens=actual; input=0; output=0; total=0; cached=0`,
    exitCode: ok ? 0 : result?.exitCode || 1,
    durationMs: Date.now() - startedAt,
    ...learningContext
  });
  if (parsed && !quality.ok) {
    return null;
  }
  return {
    ok,
    text: finalText.slice(0, maxChatChars),
    exitCode: ok ? 0 : result?.exitCode || 1
  };
}

async function tryFastWindowsReinstallGateReply({ text, source, target, taskFamily, startedAt, learningContext }) {
  if (taskFamily !== "windows-reinstall" && classifySourceCommand(text) !== "windows-reinstall") {
    return null;
  }
  if (!target?.id) {
    return null;
  }
  const deviceId = agentSourceDeviceId(target.id) || bridgeSourceDeviceId(target, source);
  if (!deviceId) {
    return null;
  }
  const request = managedReinstallGuardRequest("preflight");
  const script = sourceManagedWindowsReinstallScript(request);
  const result = isAgentSourceTarget(target.id)
    ? await postAgentSourceJob("/api/agent/source/script", {
      deviceId,
      script,
      shell: "powershell",
      name: "fast-windows-reinstall-preflight",
      timeoutMs: 120_000
    }, safeRelayId(source?.sourceRelayId || ""))
    : await postLocalOperatorScript(target.id, deviceId, {
      script,
      shell: "powershell",
      name: "fast-windows-reinstall-preflight"
    }, 120_000);
  const parsed = parseFastRoutineJson(result?.text || "") || parseJsonObjectLoose(result?.text || "");
  if (!parsed || String(parsed.action || "") !== "preflight") {
    return null;
  }
  const blockers = reinstallPreflightBlockers(parsed);
  const hardBlockers = reinstallHardPreflightBlockers(parsed, blockers);
  if (hardBlockers.length === 0) {
    recordLearningReceipt({
      kind: "agent-runtime",
      family: "windows-reinstall",
      result: "partial",
      route: "agent-runtime.fast-reinstall-preflight",
      commandSig: commandSignature("fast-windows-reinstall-preflight", "windows-reinstall"),
      taskSig: taskSignature(text),
      proof: `exitCode=${Number.isSafeInteger(result?.exitCode) ? result.exitCode : 0}; action=preflight; destructive=false; handoff=codex-agent; recoverable=${blockers.join(",").slice(0, 180)}; tokens=actual; input=0; output=0; total=0; cached=0`,
      exitCode: 0,
      durationMs: Date.now() - startedAt,
      ...learningContext
    });
    return null;
  }
  const finalText = formatFastWindowsReinstallPreflight(parsed, target, hardBlockers);
  recordLearningReceipt({
    kind: "agent-runtime",
    family: "windows-reinstall",
    result: blockers.length > 0 ? "blocked" : "partial",
    route: "agent-runtime.fast-reinstall-preflight",
    commandSig: commandSignature("fast-windows-reinstall-preflight", "windows-reinstall"),
    taskSig: taskSignature(text),
    proof: `exitCode=${Number.isSafeInteger(result?.exitCode) ? result.exitCode : 0}; action=preflight; destructive=false; final=nonempty; quality=pass; qualityScore=92; blockers=${hardBlockers.join(",").slice(0, 180)}; tokens=actual; input=0; output=0; total=0; cached=0`,
    exitCode: 0,
    durationMs: Date.now() - startedAt,
    ...learningContext
  });
  return {
    ok: true,
    text: finalText.slice(0, maxChatChars),
    exitCode: 0
  };
}

function fastRoutineSpecForTask(text, taskFamily) {
  const body = String(text || "");
  const lower = body.toLowerCase();
  if (taskFamily === "program-control") {
    return fastRoutineProgramSpec(body);
  }
  if (taskFamily === "driver-check") {
    return fastRoutineDriverProblemSpec(lower);
  }
  if (taskFamily === "power-check") {
    return fastRoutinePowerSpec(lower);
  }
  if (taskFamily === "software-check" || (taskFamily === "package-install" && /(version|верс|installed|установлен|наличи|where|which|проверь)/iu.test(lower))) {
    return fastRoutineSoftwareSpec(lower);
  }
  if (taskFamily === "system-check" && /(?:\bport\b|listener|listen|tcp|udp|netstat|порт|слуша|соединен)/iu.test(lower)) {
    return fastRoutinePortSpec(lower);
  }
  if (taskFamily === "system-check" && hasExplicitEventLogIntent(lower)) {
    return fastRoutineEventLogSpec();
  }
  if (taskFamily === "file-work" && /(zip|archive|compress|архив|сожм|упак)/iu.test(lower) && /temp|tmp|врем/iu.test(lower)) {
    return fastRoutineZipReportSpec(fastRoutineFileCount(body) || 4);
  }
  if (taskFamily === "file-work" && /report\.txt|отчет|отчёт/iu.test(lower) && /temp|tmp|врем/iu.test(lower)) {
    return fastRoutineFileReportSpec(body);
  }
  if (["system-check", "service-check", "identity-probe"].includes(taskFamily) && /(uptime|ram|памят|диск|место|служб|bits|windows update|cpu|процесс|ipv4|ip address|dns|шлюз|сеть|defender|firewall|защит|брандмауэр)/iu.test(lower)) {
    return fastRoutineSystemSpec(lower);
  }
  if (taskFamily === "script-task" && /(powershell|ps1|скрипт)/iu.test(lower)) {
    return fastRoutineScriptReportSpec(lower);
  }
  if (taskFamily === "web-lookup" && /(node\.?js|powershell)/iu.test(lower) && /(official|официальн|релиз|lts|stable|стабиль)/iu.test(lower)) {
    return fastRoutineWebFactSpec(lower);
  }
  return null;
}

function fastRoutineProgramSpec(text) {
  const target = fastRoutineProgramTarget(text);
  if (!target) {
    return null;
  }
  const shouldClose = /close|stop|kill|закрой|закрыть|заверши|останови/iu.test(String(text || ""));
  const args = target.tempFile
    ? [
      "$dir = Join-Path $env:TEMP 'soty-program-check'",
      "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
      "$argFile = Join-Path $dir ('program-' + [guid]::NewGuid().ToString('N') + '.txt')",
      "Set-Content -LiteralPath $argFile -Value ('Soty program check ' + (Get-Date).ToString('o')) -Encoding UTF8",
      "$argumentList = @($argFile)"
    ].join("\n")
    : "$argFile = ''\n$argumentList = @()";
  const processNames = target.processNames.map((name) => `'${name.replace(/'/gu, "''")}'`).join(",");
  return {
    kind: `program-${target.id}`,
    family: "program-control",
    name: `fast-program-${target.id}`,
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      `$processNames = @(${processNames})`,
      "$before = @(Get-Process -Name $processNames -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)",
      args,
      `$startedProcess = if ($argumentList.Count -gt 0) { Start-Process -FilePath '${target.exe}' -ArgumentList $argumentList -PassThru } else { Start-Process -FilePath '${target.exe}' -PassThru }`,
      "Start-Sleep -Milliseconds 1200",
      "$after = @(Get-Process -Name $processNames -ErrorAction SilentlyContinue | Where-Object { $before -notcontains $_.Id } | Sort-Object Id)",
      "$chosen = if ($after.Count -gt 0) { $after[0] } elseif ($startedProcess -and (Get-Process -Id $startedProcess.Id -ErrorAction SilentlyContinue)) { Get-Process -Id $startedProcess.Id } else { $null }",
      "$pidValue = if ($chosen) { $chosen.Id } elseif ($startedProcess) { $startedProcess.Id } else { 0 }",
      "$aliveBeforeClose = if ($pidValue) { [bool](Get-Process -Id $pidValue -ErrorAction SilentlyContinue) } else { $false }",
      `$closeRequested = ${shouldClose ? "$true" : "$false"}`,
      "if ($closeRequested -and $aliveBeforeClose) { Stop-Process -Id $pidValue -Force -ErrorAction SilentlyContinue; Start-Sleep -Milliseconds 300 }",
      "$aliveAfterClose = if ($pidValue) { [bool](Get-Process -Id $pidValue -ErrorAction SilentlyContinue) } else { $false }",
      `[pscustomobject]@{ app = '${target.label}'; pid = $pidValue; file = $argFile; started = [bool]$pidValue; aliveBeforeClose = $aliveBeforeClose; closeRequested = $closeRequested; closed = (-not $aliveAfterClose) } | ConvertTo-Json -Compress`
    ].join("\n"),
    format: (data) => [
      `${data.app || target.label}: PID ${data.pid || "не найден"}.`,
      data.file ? `Файл: \`${data.file}\`.` : `Запуск: ${data.started === false ? "не подтвержден" : "подтвержден"}.`,
      data.closeRequested === false
        ? "Не закрывал: закрытие не просили."
        : data.closed === false
          ? "Закрытие не подтверждено."
          : data.aliveBeforeClose === false
            ? "Процесс завершился сам; сейчас не живой."
            : "Закрыл только этот процесс."
    ].join("\n")
  };
}

function fastRoutineProgramTarget(text) {
  const lower = String(text || "").toLowerCase();
  if (/notepad|блокнот/iu.test(lower)) {
    return { id: "notepad", label: "Блокнот", exe: "notepad.exe", processNames: ["notepad"], tempFile: true };
  }
  if (/\bcalc(?:ulator)?\b|калькулятор/iu.test(lower)) {
    return { id: "calculator", label: "Калькулятор", exe: "calc.exe", processNames: ["CalculatorApp", "Calculator"], tempFile: false };
  }
  if (/mspaint|paint|рисовалк|paintbrush/iu.test(lower)) {
    return { id: "paint", label: "Paint", exe: "mspaint.exe", processNames: ["mspaint"], tempFile: false };
  }
  return null;
}

function fastRoutineProgramNotepadSpec() {
  return {
    kind: "program-notepad",
    family: "program-control",
    name: "fast-program-notepad",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      "$dir = Join-Path $env:TEMP 'soty-notepad-check'",
      "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
      "$file = Join-Path $dir ('notepad-' + [guid]::NewGuid().ToString('N') + '.txt')",
      "Set-Content -LiteralPath $file -Value ('Soty Notepad check ' + (Get-Date).ToString('o')) -Encoding UTF8",
      "$process = Start-Process -FilePath 'notepad.exe' -ArgumentList @($file) -PassThru",
      "Start-Sleep -Milliseconds 800",
      "$pidValue = $process.Id",
      "$started = [bool](Get-Process -Id $pidValue -ErrorAction SilentlyContinue)",
      "Stop-Process -Id $pidValue -Force -ErrorAction SilentlyContinue",
      "Start-Sleep -Milliseconds 250",
      "$closed = -not [bool](Get-Process -Id $pidValue -ErrorAction SilentlyContinue)",
      "[pscustomobject]@{ file = $file; pid = $pidValue; started = $started; closed = $closed } | ConvertTo-Json -Compress"
    ].join("\n"),
    format: (data) => [
      "Готово.",
      `Файл: \`${data.file || ""}\``,
      `PID Блокнота: \`${data.pid || ""}\``,
      data.closed === false ? "Процесс запускался, но закрытие не подтверждено." : "Закрыл только этот процесс."
    ].join("\n")
  };
}

function fastRoutineFileReportSpec(text) {
  const safeCount = Math.max(2, Math.min(9, fastRoutineFileCount(text) || 4));
  const wantsHash = /sha-?256|checksum|hash|хеш|контрольн/iu.test(String(text || ""));
  const extension = /txt|текст/iu.test(String(text || "")) ? "txt" : "bin";
  return wantsHash
    ? fastRoutineHashFileReportSpec(safeCount, extension)
    : fastRoutineLargestFileReportSpec(safeCount);
}

function fastRoutineHashFileReportSpec(count, extension) {
  const safeExtension = extension === "txt" ? "txt" : "bin";
  return {
    kind: "temp-file-hash-report",
    family: "file-work",
    name: "fast-file-hash-report",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      `$count = ${count}`,
      `$extension = '${safeExtension}'`,
      "$dir = Join-Path $env:TEMP ('soty-file-hash-' + [guid]::NewGuid().ToString('N'))",
      "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
      "$items = @()",
      "for ($i = 0; $i -lt $count; $i++) {",
      "  $path = Join-Path $dir ('item-' + ($i + 1) + '.' + $extension)",
      "  if ($extension -eq 'txt') {",
      "    Set-Content -LiteralPath $path -Value ('soty file ' + ($i + 1) + ' ' + [guid]::NewGuid().ToString('N')) -Encoding UTF8",
      "  } else {",
      "    $size = [int](256 * ($i + 1))",
      "    $bytes = New-Object byte[] $size",
      "    [System.IO.File]::WriteAllBytes($path, $bytes)",
      "  }",
      "  $hash = Get-FileHash -LiteralPath $path -Algorithm SHA256",
      "  $items += [pscustomobject]@{ name = (Split-Path -Leaf $path); sha256 = $hash.Hash }",
      "}",
      "$report = Join-Path $dir 'report.txt'",
      "$body = $items | ForEach-Object { $_.name + '=' + $_.sha256 }",
      "Set-Content -LiteralPath $report -Value $body -Encoding UTF8",
      "Get-ChildItem -LiteralPath $dir -File | Where-Object { $_.Name -ne 'report.txt' } | Remove-Item -Force",
      "$remaining = @(Get-ChildItem -LiteralPath $dir -File).Count",
      "[pscustomobject]@{ report = $report; count = $count; hashes = $items; remaining = $remaining } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => [
      "Готово.",
      `Отчет SHA256: \`${data.report || ""}\``,
      `Файлов обработано: ${data.count || 0}.`,
      `В папке оставлен только отчет: ${data.remaining === 1 ? "да" : "проверь вручную"}.`
    ].join("\n")
  };
}

function fastRoutineLargestFileReportSpec(count) {
  return {
    kind: "temp-file-report",
    family: "file-work",
    name: "fast-file-report",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      `$count = ${count}`,
      "$dir = Join-Path $env:TEMP ('soty-file-check-' + [guid]::NewGuid().ToString('N'))",
      "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
      "$sizes = @(128, 512, 2048, 4096, 8192, 12288, 16384, 24576, 32768)",
      "for ($i = 0; $i -lt $count; $i++) {",
      "  $path = Join-Path $dir ('item-' + ($i + 1) + '.bin')",
      "  $size = [int]$sizes[$i]",
      "  $bytes = New-Object byte[] $size",
      "  [System.IO.File]::WriteAllBytes($path, $bytes)",
      "}",
      "$largest = Get-ChildItem -LiteralPath $dir -File | Sort-Object Length -Descending | Select-Object -First 1",
      "$report = Join-Path $dir 'report.txt'",
      "$body = @('largest=' + $largest.Name, 'bytes=' + $largest.Length, 'created=' + $count)",
      "Set-Content -LiteralPath $report -Value $body -Encoding UTF8",
      "Get-ChildItem -LiteralPath $dir -File | Where-Object { $_.Name -ne 'report.txt' } | Remove-Item -Force",
      "$remaining = @(Get-ChildItem -LiteralPath $dir -File).Count",
      "[pscustomobject]@{ report = $report; largest = $largest.Name; bytes = $largest.Length; remaining = $remaining } | ConvertTo-Json -Compress"
    ].join("\n"),
    format: (data) => [
      "Готово.",
      `Отчет: \`${data.report || ""}\``,
      `Самый большой был: \`${data.largest || ""}\`, ${data.bytes || 0} байт.`,
      `В папке оставлен только отчет: ${data.remaining === 1 ? "да" : "проверь вручную"}.`
    ].join("\n")
  };
}

function fastRoutineZipReportSpec(count) {
  const safeCount = Math.max(2, Math.min(9, Number.parseInt(String(count || 4), 10) || 4));
  return {
    kind: "temp-zip-report",
    family: "file-work",
    name: "fast-file-zip-report",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      `$count = ${safeCount}`,
      "$dir = Join-Path $env:TEMP ('soty-zip-check-' + [guid]::NewGuid().ToString('N'))",
      "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
      "$sourceDir = Join-Path $dir 'source'",
      "New-Item -ItemType Directory -Force -Path $sourceDir | Out-Null",
      "for ($i = 0; $i -lt $count; $i++) {",
      "  Set-Content -LiteralPath (Join-Path $sourceDir ('item-' + ($i + 1) + '.txt')) -Value ('soty archive item ' + ($i + 1) + ' ' + [guid]::NewGuid().ToString('N')) -Encoding UTF8",
      "}",
      "$zip = Join-Path $dir 'archive.zip'",
      "$sourceItems = @(Get-ChildItem -LiteralPath $sourceDir -File)",
      "Compress-Archive -LiteralPath $sourceItems.FullName -DestinationPath $zip -Force",
      "$hash = Get-FileHash -LiteralPath $zip -Algorithm SHA256",
      "$report = Join-Path $dir 'report.txt'",
      "Set-Content -LiteralPath $report -Value @('archive=' + $zip, 'sha256=' + $hash.Hash, 'items=' + $count) -Encoding UTF8",
      "[pscustomobject]@{ archive = $zip; report = $report; count = $count; sha256 = $hash.Hash; bytes = (Get-Item -LiteralPath $zip).Length } | ConvertTo-Json -Compress"
    ].join("\n"),
    format: (data) => [
      "Готово.",
      `Архив: \`${data.archive || ""}\` (${data.bytes || 0} байт).`,
      `Отчет: \`${data.report || ""}\``,
      `SHA256: \`${data.sha256 || ""}\`; файлов внутри: ${data.count || 0}.`
    ].join("\n")
  };
}

function fastRoutineSystemSpec(lower) {
  if (/ipv4|ip address|dns|gateway|шлюз|сеть|сетев/iu.test(lower)) {
    return fastRoutineSystemNetworkSpec();
  }
  if (/defender|firewall|защит|брандмауэр|служб|service|bits|windows update|wuauserv/iu.test(lower)) {
    return fastRoutineSystemServicesSpec(lower);
  }
  const diskMode = /диск|место|bits|windows update|служб|памят/iu.test(lower) && !/uptime/iu.test(lower);
  return diskMode ? fastRoutineSystemDiskSpec() : fastRoutineSystemUptimeSpec();
}

function fastRoutineSystemNetworkSpec() {
  return {
    kind: "system-network",
    family: "system-check",
    name: "fast-system-network",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      "$cfg = Get-CimInstance Win32_NetworkAdapterConfiguration -Filter \"IPEnabled=True\" | Select-Object -First 1",
      "$dns = @($cfg.DNSServerSearchOrder)",
      "$ipv4 = @($cfg.IPAddress | Where-Object { $_ -match '^\\d+\\.\\d+\\.\\d+\\.\\d+$' } | Select-Object -First 1)",
      "$gateway = @($cfg.DefaultIPGateway | Select-Object -First 1)",
      "[pscustomobject]@{ computer = $env:COMPUTERNAME; ipv4 = $ipv4; dns = $dns; gateway = $gateway } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => [
      `Имя: ${data.computer || "нет данных"}`,
      `IPv4: ${data.ipv4 || "нет данных"}`,
      `DNS: ${Array.isArray(data.dns) ? data.dns.join(", ") : data.dns || "нет данных"}`,
      `Шлюз: ${data.gateway || "нет данных"}`
    ].join("\n")
  };
}

function fastRoutineSystemServicesSpec(lower) {
  const services = fastRoutineServiceNames(lower);
  const serviceNames = services.map((name) => `'${name}'`).join(",");
  const includeDisk = /c:|диск|место|free|свобод/iu.test(lower);
  const includeFirewall = /firewall|брандмауэр|фаервол/iu.test(lower);
  return {
    kind: "system-services",
    family: "system-check",
    name: "fast-system-services",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      `$serviceNames = @(${serviceNames})`,
      "$services = @(Get-Service -Name $serviceNames -ErrorAction SilentlyContinue | ForEach-Object { [pscustomobject]@{ name = $_.Name; status = $_.Status.ToString() } })",
      includeDisk
        ? "$disk = Get-CimInstance Win32_LogicalDisk -Filter \"DeviceID='C:'\"; $freeGB = [math]::Round($disk.FreeSpace / 1GB, 2)"
        : "$freeGB = $null",
      includeFirewall
        ? "$fw = @(Get-NetFirewallProfile | ForEach-Object { [pscustomobject]@{ name = $_.Name; enabled = [bool]$_.Enabled } })"
        : "$fw = @()",
      "[pscustomobject]@{ freeGB = $freeGB; services = $services; firewall = $fw } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => {
      const servicesOut = Array.isArray(data.services) ? data.services : [];
      const firewallOut = Array.isArray(data.firewall) ? data.firewall : [];
      const lines = [];
      if (data.freeGB !== null && data.freeGB !== undefined) {
        lines.push(`C: свободно: ${data.freeGB} ГБ.`);
      }
      lines.push(`Службы: ${servicesOut.map((item) => `${item.name}=${item.status}`).join(", ") || "нет данных"}.`);
      if (firewallOut.length > 0) {
        lines.push(`Firewall: ${firewallOut.map((item) => `${item.name}=${item.enabled ? "On" : "Off"}`).join(", ")}.`);
      }
      lines.push("Ничего не менял.");
      return lines.join("\n");
    }
  };
}

function fastRoutineSystemDiskSpec() {
  return {
    kind: "system-disk-services-memory",
    family: "system-check",
    name: "fast-system-disk-services-memory",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      "$disk = Get-CimInstance Win32_LogicalDisk -Filter \"DeviceID='C:'\"",
      "$services = Get-Service -Name wuauserv,BITS -ErrorAction SilentlyContinue | ForEach-Object { $_.Name + '=' + $_.Status }",
      "$top = Get-Process | Sort-Object WorkingSet64 -Descending | Select-Object -First 3 Name,Id,@{Name='MB';Expression={[math]::Round($_.WorkingSet64 / 1MB, 1)}}",
      "[pscustomobject]@{ freeGB = [math]::Round($disk.FreeSpace / 1GB, 2); services = $services; topMemory = $top } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => {
      const top = Array.isArray(data.topMemory) ? data.topMemory : [];
      return [
        `C: свободно: ${data.freeGB ?? "?"} ГБ.`,
        `Службы: ${Array.isArray(data.services) ? data.services.join(", ") : data.services || "нет данных"}.`,
        `Топ памяти: ${top.map((item) => `${item.Name}(${item.MB} МБ)`).join(", ") || "нет данных"}.`,
        "Ничего не менял."
      ].join("\n");
    }
  };
}

function fastRoutineSystemUptimeSpec() {
  return {
    kind: "system-uptime-ram-cpu",
    family: "system-check",
    name: "fast-system-uptime-ram-cpu",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      "$os = Get-CimInstance Win32_OperatingSystem",
      "$uptime = (Get-Date) - $os.LastBootUpTime",
      "$top = Get-Process | Where-Object { $_.CPU -ne $null } | Sort-Object CPU -Descending | Select-Object -First 3 Name,Id,@{Name='CPU';Expression={[math]::Round($_.CPU, 1)}}",
      "[pscustomobject]@{ uptime = ('{0}д {1}ч {2}м' -f [int]$uptime.TotalDays, $uptime.Hours, $uptime.Minutes); freeRamGB = [math]::Round($os.FreePhysicalMemory / 1MB, 2); topCpu = $top } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => {
      const top = Array.isArray(data.topCpu) ? data.topCpu : [];
      return [
        `Uptime: ${data.uptime || "нет данных"}.`,
        `Свободная RAM: ${data.freeRamGB ?? "?"} ГБ.`,
        `Топ CPU: ${top.map((item) => `${item.Name}(${item.CPU})`).join(", ") || "нет данных"}.`
      ].join("\n");
    }
  };
}

function fastRoutinePortSpec(lower) {
  const port = fastRoutineRequestedPort(lower);
  return {
    kind: "system-port-listener",
    family: "system-check",
    name: "fast-system-port-listener",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      `$port = ${port}`,
      "$items = @(Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue | Select-Object -First 12 LocalAddress,LocalPort,State,OwningProcess)",
      "$rows = @($items | ForEach-Object {",
      "  $proc = Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue",
      "  [pscustomobject]@{ address = $_.LocalAddress; port = $_.LocalPort; state = $_.State.ToString(); pid = $_.OwningProcess; process = if ($proc) { $proc.ProcessName } else { '' } }",
      "})",
      "[pscustomobject]@{ port = $port; count = $rows.Count; listeners = $rows; changed = $false } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => {
      const listeners = Array.isArray(data.listeners) ? data.listeners : [];
      const rows = listeners.map((item) => `${item.process || "pid"}:${item.pid || 0} ${item.address || "*"}:${item.port || data.port} ${item.state || ""}`);
      return [
        `Порт ${data.port}: ${Number(data.count || 0) > 0 ? "есть соединения/слушатели" : "не найден активным"}.`,
        rows.length > 0 ? rows.join("\n") : "Активных записей TCP нет.",
        "Ничего не менял."
      ].join("\n");
    }
  };
}

function fastRoutineEventLogSpec() {
  return {
    kind: "system-eventlog-critical",
    family: "system-check",
    name: "fast-system-eventlog-critical",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      "$start = (Get-Date).AddHours(-24)",
      "$events = @(Get-WinEvent -FilterHashtable @{ LogName = 'System'; Level = 1,2; StartTime = $start } -MaxEvents 5 -ErrorAction SilentlyContinue | ForEach-Object {",
      "  [pscustomobject]@{ time = $_.TimeCreated.ToString('s'); id = $_.Id; provider = $_.ProviderName; message = ([string]$_.Message).Replace(\"`r\", ' ').Replace(\"`n\", ' ').Substring(0, [Math]::Min(180, ([string]$_.Message).Length)) }",
      "})",
      "[pscustomobject]@{ hours = 24; count = $events.Count; events = $events; changed = $false } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => {
      const events = Array.isArray(data.events) ? data.events : [];
      return [
        `System Event Log за ${data.hours || 24}ч: критических/ошибок ${data.count || 0}.`,
        events.length > 0 ? events.map((item) => `${item.time || ""} ${item.provider || ""} #${item.id || ""}`).join("\n") : "Критических/ошибок не нашел.",
        "Ничего не менял."
      ].join("\n");
    }
  };
}

function fastRoutineDriverProblemSpec() {
  return {
    kind: "driver-problem-devices",
    family: "driver-check",
    name: "fast-driver-problem-devices",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      "$items = @(Get-CimInstance Win32_PnPEntity -ErrorAction SilentlyContinue | Where-Object { $null -ne $_.ConfigManagerErrorCode -and [int]$_.ConfigManagerErrorCode -ne 0 } | Select-Object -First 12 Name,DeviceID,ConfigManagerErrorCode)",
      "$rows = @($items | ForEach-Object { [pscustomobject]@{ name = [string]$_.Name; code = [int]$_.ConfigManagerErrorCode; id = ([string]$_.DeviceID).Substring(0, [Math]::Min(120, ([string]$_.DeviceID).Length)) } })",
      "[pscustomobject]@{ problemCount = $rows.Count; devices = $rows; changed = $false } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => {
      const devices = Array.isArray(data.devices) ? data.devices : [];
      return [
        `Проблемных устройств: ${data.problemCount || 0}.`,
        devices.length > 0 ? devices.map((item) => `${item.name || "device"}: code ${item.code}`).join("\n") : "PnP-ошибок не нашел.",
        "Ничего не менял."
      ].join("\n");
    }
  };
}

function fastRoutinePowerSpec() {
  return {
    kind: "system-power-battery",
    family: "power-check",
    name: "fast-system-power-battery",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      "$battery = @(Get-CimInstance Win32_Battery -ErrorAction SilentlyContinue | Select-Object -First 2 Name,EstimatedChargeRemaining,BatteryStatus)",
      "$scheme = ''",
      "try { $scheme = (powercfg /getactivescheme 2>$null | Out-String).Trim() } catch {}",
      "$sleep = ''",
      "try { $sleep = (powercfg /a 2>$null | Out-String).Trim() } catch {}",
      "[pscustomobject]@{ battery = $battery; activeScheme = $scheme; sleepSummary = $sleep.Substring(0, [Math]::Min(900, $sleep.Length)); changed = $false } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => {
      const battery = Array.isArray(data.battery) ? data.battery : data.battery ? [data.battery] : [];
      const batteryLine = battery.length > 0
        ? battery.map((item) => `${item.Name || "Battery"} ${item.EstimatedChargeRemaining ?? "?"}% status=${item.BatteryStatus ?? "?"}`).join(", ")
        : "батарея не найдена";
      return [
        `Батарея: ${batteryLine}.`,
        `План питания: ${String(data.activeScheme || "нет данных").slice(0, 220)}.`,
        "Ничего не менял."
      ].join("\n");
    }
  };
}

function fastRoutineSoftwareSpec(lower) {
  const tools = fastRoutineRequestedTools(lower);
  const toolRows = tools.map((tool) => `@{ Name = '${tool.name}'; Command = '${tool.command}'; Args = @(${tool.args.map((arg) => `'${arg}'`).join(",")}) }`).join(",\n");
  return {
    kind: "software-version-check",
    family: "software-check",
    name: "fast-software-version-check",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      `$tools = @(${toolRows})`,
      "$rows = @()",
      "foreach ($tool in $tools) {",
      "  $cmd = Get-Command $tool.Command -ErrorAction SilentlyContinue | Select-Object -First 1",
      "  if (-not $cmd) { $rows += [pscustomobject]@{ name = $tool.Name; found = $false; path = ''; version = '' }; continue }",
      "  $exe = if ($cmd.Path) { [string]$cmd.Path } elseif ($cmd.Source) { [string]$cmd.Source } else { [string]$cmd.Name }",
      "  $version = ''",
      "  try { $version = (& $exe @($tool.Args) 2>$null | Out-String).Trim() } catch { $version = $_.Exception.Message }",
      "  $rows += [pscustomobject]@{ name = $tool.Name; found = $true; path = $exe; version = $version.Substring(0, [Math]::Min(220, $version.Length)) }",
      "}",
      "[pscustomobject]@{ tools = $rows; changed = $false } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => {
      const toolsOut = Array.isArray(data.tools) ? data.tools : [];
      return [
        "Проверил программы:",
        toolsOut.map((item) => `${item.name}: ${item.found ? (item.version || item.path || "найдено") : "не найдено"}`).join("\n") || "нет данных",
        "Ничего не менял."
      ].join("\n");
    }
  };
}

function fastRoutineScriptReportSpec(lower) {
  const services = fastRoutineServiceNames(lower);
  const serviceNames = services.map((name) => `'${name}'`).join(",");
  const includeIdentity = /whoami|hostname|имя|host|компьютер/iu.test(String(lower || ""));
  return {
    kind: "temp-powershell-report",
    family: "script-task",
    name: "fast-script-report",
    timeoutMs: 45_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      `$serviceNames = @(${serviceNames})`,
      "$dir = Join-Path $env:TEMP ('soty-script-check-' + [guid]::NewGuid().ToString('N'))",
      "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
      "$scriptPath = Join-Path $dir 'probe.ps1'",
      "$report = Join-Path $dir 'report.json'",
      "$serviceLiteral = ($serviceNames | ForEach-Object { \"'\" + $_.Replace(\"'\", \"''\") + \"'\" }) -join ','",
      "$scriptBody = @'",
      "$ErrorActionPreference = 'Stop'",
      "$names = @(__SERVICES__)",
      "$services = @(Get-Service -Name $names -ErrorAction SilentlyContinue | ForEach-Object { [pscustomobject]@{ name = $_.Name; status = $_.Status.ToString() } })",
      `[pscustomobject]@{ whoami = ${includeIdentity ? "(whoami)" : "''"}; hostname = ${includeIdentity ? "$env:COMPUTERNAME" : "''"}; services = $services } | ConvertTo-Json -Compress -Depth 4`,
      "'@.Replace('__SERVICES__', $serviceLiteral)",
      "Set-Content -LiteralPath $scriptPath -Value $scriptBody -Encoding UTF8",
      "$json = powershell.exe -NoProfile -ExecutionPolicy Bypass -File $scriptPath",
      "Remove-Item -LiteralPath $scriptPath -Force",
      "Set-Content -LiteralPath $report -Value $json -Encoding UTF8",
      "$obj = $json | ConvertFrom-Json",
      "[pscustomobject]@{ report = $report; whoami = $obj.whoami; hostname = $obj.hostname; services = $obj.services; scriptDeleted = (-not (Test-Path -LiteralPath $scriptPath)) } | ConvertTo-Json -Compress -Depth 4"
    ].join("\n"),
    format: (data) => {
      const serviceData = Array.isArray(data.services)
        ? data.services
        : data.services
          ? [data.services]
          : [];
      const lines = [
        "Готово.",
        `JSON-отчет: \`${data.report || ""}\``
      ];
      if (data.whoami || data.hostname) {
        lines.push(`whoami: \`${data.whoami || ""}\`, host: \`${data.hostname || ""}\`.`);
      }
      lines.push(`Службы: ${serviceData.map((item) => `${item.name}=${item.status}`).join(", ") || "нет данных"}; скрипт удален: ${data.scriptDeleted === false ? "нет" : "да"}.`);
      return lines.join("\n");
    }
  };
}

function fastRoutineWebFactSpec(lower) {
  const nodeMode = /node\.?js|lts/iu.test(lower) && !/powershell/iu.test(lower);
  return nodeMode ? fastRoutineNodeLtsSpec() : fastRoutinePowerShellReleaseSpec();
}

function fastRoutineNodeLtsSpec() {
  return {
    kind: "web-node-lts",
    family: "web-lookup",
    name: "fast-web-node-lts",
    timeoutMs: 60_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      "$ProgressPreference = 'SilentlyContinue'",
      "$source = 'https://nodejs.org/dist/index.json'",
      "$items = Invoke-RestMethod -Uri $source -UseBasicParsing -TimeoutSec 25",
      "$lts = @($items | Where-Object { $_.lts -ne $false })[0]",
      "$dir = Join-Path $env:TEMP 'soty-web-facts'",
      "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
      "$file = Join-Path $dir ('node-lts-' + [guid]::NewGuid().ToString('N') + '.txt')",
      "$version = [string]$lts.version",
      "Set-Content -LiteralPath $file -Value @('Node.js LTS=' + $version, 'source=' + $source) -Encoding UTF8",
      "[pscustomobject]@{ version = $version; source = $source; file = $file } | ConvertTo-Json -Compress"
    ].join("\n"),
    format: (data) => [
      `Node.js LTS: \`${data.version || ""}\`.`,
      `Файл: \`${data.file || ""}\``,
      `Источник: ${data.source || "https://nodejs.org/"}`
    ].join("\n")
  };
}

function fastRoutinePowerShellReleaseSpec() {
  return {
    kind: "web-powershell-release",
    family: "web-lookup",
    name: "fast-web-powershell-release",
    timeoutMs: 60_000,
    script: [
      "$ErrorActionPreference = 'Stop'",
      "$ProgressPreference = 'SilentlyContinue'",
      "$api = 'https://api.github.com/repos/PowerShell/PowerShell/releases/latest'",
      "$release = Invoke-RestMethod -Uri $api -Headers @{ 'User-Agent' = 'SotyAgent' } -UseBasicParsing -TimeoutSec 25",
      "$dir = Join-Path $env:TEMP 'soty-web-facts'",
      "New-Item -ItemType Directory -Force -Path $dir | Out-Null",
      "$file = Join-Path $dir ('powershell-release-' + [guid]::NewGuid().ToString('N') + '.txt')",
      "$version = [string]$release.tag_name",
      "$source = [string]$release.html_url",
      "Set-Content -LiteralPath $file -Value @('PowerShell stable=' + $version, 'source=' + $source) -Encoding UTF8",
      "[pscustomobject]@{ version = $version; source = $source; file = $file } | ConvertTo-Json -Compress"
    ].join("\n"),
    format: (data) => [
      `PowerShell stable: \`${data.version || ""}\`.`,
      `Файл: \`${data.file || ""}\``,
      `Источник: ${data.source || "https://github.com/PowerShell/PowerShell/releases"}`
    ].join("\n")
  };
}

function fastRoutineFileCount(text) {
  const match = /([2-9])(?:\s+[a-zа-я0-9_-]+){0,3}\s*(?:files?|файл(?:а|ов)?)/iu.exec(String(text || ""));
  return match ? Number.parseInt(match[1], 10) : 4;
}

function fastRoutineRequestedPort(text) {
  const value = String(text || "");
  const explicit = /(?:port|порт)\s*[:#]?\s*(\d{2,5})/iu.exec(value);
  const fallback = /\b(22|25|53|80|110|143|443|445|993|995|1433|1521|3000|3306|3389|5000|5173|5432|6379|8000|8080|8443|9000)\b/u.exec(value);
  const port = Number.parseInt((explicit || fallback || [])[1] || "80", 10);
  return Math.max(1, Math.min(65535, Number.isFinite(port) ? port : 80));
}

function fastRoutineRequestedTools(lower) {
  const text = String(lower || "");
  const specs = [
    { id: "winget", name: "winget", command: "winget", args: ["--version"], test: /winget|пакет|программ|приложени/u },
    { id: "git", name: "git", command: "git", args: ["--version"], test: /\bgit\b/u },
    { id: "node", name: "node", command: "node", args: ["--version"], test: /\bnode(?:\.js)?\b/u },
    { id: "npm", name: "npm", command: "npm", args: ["--version"], test: /\bnpm\b/u },
    { id: "python", name: "python", command: "python", args: ["--version"], test: /\bpython|питон/u },
    { id: "pwsh", name: "PowerShell", command: "pwsh", args: ["--version"], test: /\bpwsh\b|powershell|power shell/u }
  ];
  const selected = specs.filter((item) => item.test.test(text));
  if (selected.length > 0) {
    return selected.slice(0, 6);
  }
  return specs.filter((item) => ["winget", "git", "node", "python"].includes(item.id));
}

function fastRoutineServiceNames(lower) {
  const text = String(lower || "");
  const names = [];
  const add = (name) => {
    if (!names.includes(name)) {
      names.push(name);
    }
  };
  if (/bits/iu.test(text)) add("BITS");
  if (/wuauserv|windows update|обновлен|обновлён/iu.test(text)) add("wuauserv");
  if (/windefend|defender|защит/iu.test(text)) add("WinDefend");
  if (/firewall|mpssvc|брандмауэр|фаервол/iu.test(text)) add("MpsSvc");
  if (/spooler|печати|принтер/iu.test(text)) add("Spooler");
  if (names.length === 0) {
    add("BITS");
    add("wuauserv");
  }
  return names.slice(0, 8);
}

function fastRoutineQuality(spec, data, text) {
  const missing = [];
  const need = (condition, key) => {
    if (!condition) {
      missing.push(key);
    }
  };
  const lower = String(text || "").toLowerCase();
  const kind = String(spec?.kind || "");
  if (kind.startsWith("program-")) {
    need(data.started !== false && Number(data.pid || 0) >= 0, "program-start-proof");
    if (/close|stop|kill|закрой|закрыть|заверши|останови/iu.test(lower)) {
      need(data.closed !== false || data.aliveBeforeClose === false, "close-proof");
    }
  } else if (kind === "temp-file-hash-report") {
    const count = Number(data.count || 0);
    need(Boolean(data.report), "report");
    need(count > 0, "count");
    need(Array.isArray(data.hashes) ? data.hashes.length === count : count === 1 && Boolean(data.hashes), "hashes");
    need(data.remaining === 1, "cleanup");
  } else if (kind === "temp-file-report") {
    need(Boolean(data.report), "report");
    need(Boolean(data.largest), "largest");
    need(data.remaining === 1, "cleanup");
  } else if (kind === "temp-zip-report") {
    need(Boolean(data.archive), "archive");
    need(Boolean(data.report), "report");
    need(Boolean(data.sha256), "sha256");
    need(Number(data.count || 0) > 0, "count");
  } else if (kind === "system-network") {
    need(Boolean(data.computer), "computer");
    need(Boolean(data.ipv4) || Boolean(data.dns) || Boolean(data.gateway), "network-facts");
  } else if (kind === "system-services") {
    const services = Array.isArray(data.services) ? data.services : [];
    need(services.length > 0, "services");
    if (/firewall|брандмауэр|фаервол/iu.test(lower)) {
      need(Array.isArray(data.firewall) && data.firewall.length > 0, "firewall");
    }
    if (/c:|диск|место|free|свобод/iu.test(lower)) {
      need(data.freeGB !== null && data.freeGB !== undefined, "disk");
    }
  } else if (kind === "system-port-listener") {
    need(Number(data.port || 0) > 0, "port");
    need(data.changed === false, "read-only");
  } else if (kind === "system-eventlog-critical") {
    need(Number.isFinite(Number(data.count || 0)), "event-count");
    need(data.changed === false, "read-only");
  } else if (kind === "driver-problem-devices") {
    need(Number.isFinite(Number(data.problemCount || 0)), "driver-count");
    need(data.changed === false, "read-only");
  } else if (kind === "system-power-battery") {
    need(Boolean(data.activeScheme) || Boolean(data.sleepSummary) || data.battery !== undefined, "power-facts");
    need(data.changed === false, "read-only");
  } else if (kind === "software-version-check") {
    const tools = Array.isArray(data.tools) ? data.tools : [];
    need(tools.length > 0, "tools");
    need(tools.some((item) => item.found === true || item.found === false), "found-state");
    need(data.changed === false, "read-only");
  } else if (kind === "temp-powershell-report") {
    need(Boolean(data.report), "report");
    need(data.scriptDeleted !== false, "script-deleted");
  } else if (kind.startsWith("web-")) {
    need(Boolean(data.version), "version");
    need(Boolean(data.source), "source");
    need(Boolean(data.file), "file");
  }
  const score = missing.length === 0 ? 100 : Math.max(0, 100 - missing.length * 22);
  return { ok: score >= 80, score, missing };
}

function reinstallPreflightBlockers(data) {
  const blockers = [];
  if (Array.isArray(data?.blockers)) {
    blockers.push(...data.blockers.map((item) => String(item || "").trim()).filter(Boolean));
  }
  if (data?.isAdmin === false && !blockers.includes("not-elevated")) {
    blockers.push("not-elevated");
  }
  if (data?.status?.backupProofOk !== true && !blockers.includes("backup-not-ready")) {
    blockers.push("backup-not-ready");
  }
  if (!data?.status?.installImage && !blockers.includes("install-media-not-ready")) {
    blockers.push("install-media-not-ready");
  }
  return Array.from(new Set(blockers)).slice(0, 8);
}

function reinstallHardPreflightBlockers(data, blockers) {
  const hard = [];
  const values = new Set((Array.isArray(blockers) ? blockers : []).map((item) => String(item || "").trim()).filter(Boolean));
  for (const blocker of ["not-elevated", "usb-not-found", "usb-not-removable", "usb-free-space-low"]) {
    if (values.has(blocker)) {
      hard.push(blocker);
    }
  }
  if (hard.length === 0 && data?.error) {
    hard.push("preflight-error");
  }
  if (hard.length === 0 && data?.ok === false) {
    for (const blocker of values) {
      if (blocker !== "backup-not-ready" && blocker !== "install-media-not-ready") {
        hard.push(blocker);
      }
    }
  }
  return Array.from(new Set(hard)).slice(0, 8);
}

function formatFastWindowsReinstallPreflight(data, target, blockers) {
  const device = target?.label || data?.computerName || "ноут";
  const os = [data?.osCaption, data?.osVersion].filter(Boolean).join(" ").trim();
  if (blockers.length > 0) {
    return [
      `Переустановку на ${device} не начал.`,
      `Проверил безопасно: ${os || data?.computerName || "система отвечает"}.`,
      `Блокер: ${blockers.join(", ")}.`,
      "Диск Windows не трогал; перед продолжением нужны бэкап, носитель/возвратный путь и явное подтверждение стирания."
    ].join("\n");
  }
  return [
    `Переустановку на ${device} пока не запускал.`,
    `Предпроверка прошла: ${os || data?.computerName || "система отвечает"}.`,
    "Следующий безопасный шаг: подготовка бэкапа и установочного носителя; стирание только после точной фразы подтверждения."
  ].join("\n");
}

function parseFastRoutineJson(text) {
  const value = String(text || "").trim();
  if (!value) {
    return null;
  }
  const lines = value.split(/\r?\n/u).map((line) => line.trim()).filter(Boolean);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index];
    if (!line.startsWith("{") || !line.endsWith("}")) {
      continue;
    }
    try {
      return JSON.parse(line);
    } catch {}
  }
  const start = value.lastIndexOf("{");
  const end = value.lastIndexOf("}");
  if (start >= 0 && end > start) {
    try {
      return JSON.parse(value.slice(start, end + 1));
    } catch {}
  }
  return null;
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
  const known = value.match(/!\s*(target|bridge|source-target|access|tunnel|timeout|cancelled|agent-source|relay|request)\b/iu);
  if (known) {
    return `! ${known[1].toLowerCase()}`;
  }
  return value.trim() ? "nonzero-output" : "empty-output";
}

function sourceDiagnosticProof(diagnostic) {
  if (!diagnostic || typeof diagnostic !== "object") {
    return "";
  }
  const relay = diagnostic.relay && typeof diagnostic.relay === "object" ? diagnostic.relay : null;
  const source = diagnostic.source && typeof diagnostic.source === "object"
    ? diagnostic.source
    : relay?.source && typeof relay.source === "object"
      ? relay.source
      : null;
  const job = diagnostic.job && typeof diagnostic.job === "object" ? diagnostic.job : null;
  const reason = cleanActionText(diagnostic.reason || diagnostic.kind || relay?.reason || "", 80);
  const parts = [
    reason ? `diagnostic=${reason}` : "",
    Number.isSafeInteger(source?.lastSeenAgeMs) ? `sourceLastSeenAgeMs=${source.lastSeenAgeMs}` : "",
    source?.connected === false ? "sourceConnected=false" : "",
    source?.access === false ? "sourceAccess=false" : "",
    Number.isSafeInteger(job?.ageMs) ? `jobAgeMs=${job.ageMs}` : "",
    job?.leased === true ? "jobLeased=true" : ""
  ].filter(Boolean);
  return parts.join("; ").slice(0, 360);
}

function commandSignature(command, family = "") {
  const normalized = String(command || "")
    .replace(/\r\n?/gu, "\n")
    .replace(/[A-Za-z]:\\[^\s'"]+/gu, "<path>")
    .replace(/\/(?:Users|home)\/[^\s'"]+/giu, "<path>")
    .replace(/[A-Za-z0-9_-]{32,}/gu, "<id>")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, 2000);
  return `${family || "generic"}:${hashText(normalized).slice(0, 16)}`;
}

function taskSignature(text) {
  const normalized = redactLearningText(text).toLowerCase().slice(0, 2000);
  return `task:${hashText(normalized).slice(0, 16)}`;
}

function learningContextForTurn(source, target) {
  const safe = sanitizeAgentSource(source);
  return cleanLearningContext({
    dialogHash: hashLearningRef(safe.tunnelId),
    sourceDeviceHash: hashLearningRef(safe.deviceId),
    sourceDeviceNick: safe.deviceNick,
    targetHash: hashLearningRef(target?.id || ""),
    targetLabel: target?.label || ""
  });
}

function learningContextForAction(action) {
  return cleanLearningContext({
    sourceDeviceHash: hashLearningRef(action?.sourceDeviceId || ""),
    targetHash: hashLearningRef(action?.target || "")
  });
}

function cleanLearningContext(value) {
  const context = {};
  const targetLabel = cleanLearningText(value?.targetLabel, 80);
  const sourceDeviceNick = cleanLearningText(value?.sourceDeviceNick, 80);
  const targetHash = cleanLearningHash(value?.targetHash);
  const sourceDeviceHash = cleanLearningHash(value?.sourceDeviceHash);
  const dialogHash = cleanLearningHash(value?.dialogHash);
  if (targetLabel) context.targetLabel = targetLabel;
  if (sourceDeviceNick) context.sourceDeviceNick = sourceDeviceNick;
  if (targetHash) context.targetHash = targetHash;
  if (sourceDeviceHash) context.sourceDeviceHash = sourceDeviceHash;
  if (dialogHash) context.dialogHash = dialogHash;
  return context;
}

function hashLearningRef(value) {
  const text = String(value || "").trim();
  return text ? hashText(text).slice(0, 16) : "";
}

function cleanLearningHash(value) {
  const text = String(value || "").trim().toLowerCase();
  return /^[a-f0-9]{8,32}$/u.test(text) ? text.slice(0, 32) : "";
}

function recordLearningReceipt(receipt) {
  const clean = cleanLearningReceipt(receipt);
  if (!clean) {
    return;
  }
  void appendLearningReceipt(clean);
}

function recordBlockedWindowsReinstallHandoff({ kind, command }) {
  recordLearningReceipt({
    kind: "source-command",
    family: "windows-reinstall",
    result: "blocked",
    route: `operator-http.${kind}`,
    commandSig: commandSignature(command, "windows-reinstall"),
    proof: "blocked-manual-windows-reinstall-handoff; missing managed reinstall gates",
    exitCode: 422
  });
}

async function appendLearningReceipt(receipt) {
  try {
    await mkdir(agentDir, { recursive: true });
    await appendFile(learningOutboxPath, `${JSON.stringify(receipt)}\n`, "utf8");
    scheduleLearningSync();
  } catch {
    // Learning receipts are best-effort; never break the user's active command.
  }
}

function cleanLearningReceipt(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const exitCode = Number.isSafeInteger(value.exitCode) ? Math.max(-32768, Math.min(32767, value.exitCode)) : undefined;
  return {
    kind: cleanLearningEnum(value.kind, ["codex-turn", "source-command", "agent-runtime", "action-job"], "agent-runtime"),
    result: cleanLearningEnum(value.result, ["ok", "failed", "partial", "blocked", "timeout", "cancelled"], "failed"),
    toolkit: cleanLearningText(value.toolkit, 80),
    phase: cleanLearningText(value.phase, 80),
    family: cleanLearningText(value.family, 80),
    platform: process.platform,
    codexMode: codexFullLocalTools ? "stock-cli-full-local-tools" : "stock-cli-bridge",
    route: cleanLearningText(value.route, 120),
    commandSig: cleanLearningText(value.commandSig, 120),
    taskSig: cleanLearningText(value.taskSig, 120),
    proof: redactLearningText(value.proof).slice(0, 900),
    ...cleanLearningContext(value),
    ...(exitCode === undefined ? {} : { exitCode }),
    ...(Number.isSafeInteger(value.durationMs) ? { durationMs: Math.max(0, Math.min(86_400_000, value.durationMs)) } : {}),
    memorySchema: "soty.memory.receipt.v1",
    createdAt: new Date().toISOString()
  };
}

function cleanLearningEnum(value, allowed, fallback) {
  const text = String(value || "").trim();
  return allowed.includes(text) ? text : fallback;
}

function cleanLearningText(value, max) {
  return String(value || "")
    .replace(/[\u0000-\u001F\u007F]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function redactLearningText(value) {
  return String(value || "")
    .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/giu, "<email>")
    .replace(/\b(?:\d{1,3}\.){3}\d{1,3}\b/gu, "<ip>")
    .replace(/\b[0-9A-F]{2}(?::[0-9A-F]{2}){5}\b/giu, "<mac>")
    .replace(/[A-Za-z]:\\[^\s'"]+/gu, "<path>")
    .replace(/\/(?:Users|home)\/[^\s'"]+/giu, "<path>")
    .replace(/\b[A-Za-z0-9_-]{48,}\b/gu, "<id>")
    .replace(/\b(?:sk|sess|key|token(?!s\b)|secret|password|pwd)[-_A-Za-z0-9]*\b\s*[:=]\s*['"]?[^'"\s]+/giu, "<secret>")
    .replace(/\s+/gu, " ")
    .trim();
}

function hashText(value) {
  return createHash("sha256").update(String(value || "")).digest("hex");
}

function scheduleLearningSync(delayMs = 15_000) {
  if (!agentRelayBaseUrl || learningSyncTimer) {
    return;
  }
  learningSyncTimer = setTimeout(() => {
    learningSyncTimer = null;
    void syncLearningOutbox().catch(() => undefined);
  }, Math.max(1000, delayMs));
}

async function syncLearningOutbox() {
  if (!agentRelayBaseUrl || !existsSync(learningOutboxPath)) {
    return { ok: true, sent: 0, pending: 0 };
  }
  const raw = await readFile(learningOutboxPath, "utf8").catch(() => "");
  const lines = raw.split(/\r?\n/u).map((line) => line.trim()).filter(Boolean);
  if (lines.length === 0) {
    return { ok: true, sent: 0, pending: 0 };
  }
  const batchLines = lines.slice(0, 80);
  const receipts = batchLines
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
  if (receipts.length === 0) {
    await writeFile(learningOutboxPath, "", "utf8").catch(() => undefined);
    return { ok: false, sent: 0, pending: 0 };
  }
  const response = await fetch(new URL("/api/agent/memory/receipts", agentRelayBaseUrl), {
    method: "POST",
    cache: "no-store",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      installId: agentInstallId,
      relayId: agentRelayId,
      agentVersion,
      receipts
    })
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload?.ok !== true) {
    return { ok: false, sent: 0, pending: lines.length };
  }
  await appendFile(learningSentPath, `${batchLines.join("\n")}\n`, "utf8").catch(() => undefined);
  const rest = lines.slice(batchLines.length);
  await writeFile(learningOutboxPath, rest.length > 0 ? `${rest.join("\n")}\n` : "", "utf8").catch(() => undefined);
  return { ok: true, sent: receipts.length, pending: rest.length };
}

async function fetchLearningTeacherReport(limit = 800) {
  if (!agentRelayBaseUrl) {
    return { ok: false, status: 0, error: "memory relay url is not configured" };
  }
  const url = new URL("/api/agent/memory/query", agentRelayBaseUrl);
  url.searchParams.set("limit", String(Math.max(1, Math.min(2000, Number.parseInt(String(limit || 800), 10) || 800))));
  const response = await fetch(url, { cache: "no-store" });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload?.ok !== true) {
    return {
      ok: false,
      status: response.status,
      error: cleanLearningText(payload?.error || response.statusText || "memory request failed", 160)
    };
  }
  return payload;
}

function formatLearningTeacherReport(sync, report) {
  if (!report?.ok) {
    return [
      `soty-memory-doctor: ok=false sent=${sync?.sent || 0} pending=${sync?.pending || 0}`,
      `memory: failed status=${report?.status || 0} error=${report?.error || "unknown"}`
    ].join("\n");
  }
  const lines = [
    `soty-memory-doctor: ok=true receipts=${report.receipts || 0} sent=${sync?.sent || 0} pending=${sync?.pending || 0}`,
    `memory: ${report.schema || "soty.memory.query"} generated=${report.generatedAt || ""}`,
    `scope: ${formatLearningScope(report)}`,
    `publish: ${formatLearningPublishModel(report)}`
  ];
  const recommendations = Array.isArray(report.recommendations)
    ? report.recommendations.slice(0, 5)
    : Array.isArray(report.items)
      ? report.items.slice(0, 5)
      : [];
  if (recommendations.length > 0) {
    lines.push("recommendations:");
    for (const item of recommendations) {
      const prefix = item.priority ? `[${item.priority}] ` : "";
      lines.push(`- ${prefix}${item.family || "generic"}: ${item.title || "review route"}`);
      if (item.action || item.guidance) {
        lines.push(`  action: ${item.action || item.guidance}`);
      }
    }
  }
  const candidates = Array.isArray(report.candidates) ? report.candidates.slice(0, 5) : [];
  if (candidates.length > 0) {
    lines.push("promotion candidates:");
    for (const item of candidates) {
      lines.push(`- ${item.scope || "candidate"} ${item.family || "generic"}: ${item.marker || ""}`);
    }
  }
  if (report.oneCommand) {
    lines.push(`one command: ${report.oneCommand}`);
  }
  if (report.reviewMergeCommand) {
    lines.push(`review command: ${report.reviewMergeCommand}`);
  }
  return lines.join("\n");
}

function formatLearningScope(report) {
  const scope = report?.scope || {};
  const platforms = formatLearningCountList(scope.platformCounts);
  const versions = formatLearningCountList(scope.agentVersions);
  const deviceCount = Number(scope.deviceCount || 0);
  const kind = cleanLearningText(scope.kind || report?.source || "global-sanitized-route-memory", 80) || "global-sanitized-route-memory";
  return `${kind} devices=${deviceCount} platforms=${platforms || "unknown"} agentVersions=${versions || "unknown"}`;
}

function formatLearningPublishModel(report) {
  return cleanLearningText(report?.publishModel || "reviewed-memory-route-then-release", 120)
    || "reviewed-memory-route-then-release";
}

function formatLearningCountList(entries, limit = 3) {
  if (!Array.isArray(entries)) {
    return "";
  }
  return entries
    .slice(0, limit)
    .map((item) => `${cleanLearningText(item?.key || "unknown", 40)}:${Number(item?.count || 0)}`)
    .join(",");
}

async function runLearningReviewMerge(rest = []) {
  const options = parseLearningReviewMergeOptions(rest);
  const sync = await syncLearningOutbox().catch(() => ({ ok: false, sent: 0, pending: 0 }));
  const memory = await fetchLearningTeacherReport(options.limit).catch((error) => ({
    ok: false,
    status: 0,
    error: error instanceof Error ? error.message : String(error)
  }));
  const items = Array.isArray(memory.items) ? memory.items : [];
  const recommendations = Array.isArray(memory.recommendations) ? memory.recommendations : [];
  const candidates = Array.isArray(memory.candidates) ? memory.candidates : [];
  const report = {
    ok: Boolean(sync.ok && memory.ok),
    mode: "review",
    sync,
    memory,
    accepted: items.length + recommendations.length,
    candidates: candidates.length,
    blockedByReview: false,
    error: ""
  };
  if (!memory.ok) {
    report.error = memory.error || "memory query failed";
  }
  if (options.jsonPath && report.ok) {
    await mkdir(dirname(options.jsonPath), { recursive: true });
    await writeFile(options.jsonPath, JSON.stringify(report, null, 2), "utf8");
  }
  return report;
}

async function finishControlCli(exitCode = 0) {
  process.exitCode = Number.isSafeInteger(exitCode) ? exitCode : 1;
  await new Promise((resolveReady) => setImmediate(resolveReady));
}

function parseLearningReviewMergeOptions(rest) {
  const options = {
    dryRun: false,
    json: false,
    strict: false,
    limit: 800,
    jsonPath: "",
    scopes: []
  };
  for (let index = 0; index < rest.length; index += 1) {
    const item = rest[index] || "";
    if (item === "--dry-run" || item === "--no-write") {
      options.dryRun = true;
      continue;
    }
    if (item === "--write") {
      options.dryRun = false;
      continue;
    }
    if (item === "--json") {
      options.json = true;
      continue;
    }
    if (item === "--strict") {
      options.strict = true;
      continue;
    }
    if (item.startsWith("--limit=")) {
      options.limit = Number.parseInt(item.slice("--limit=".length), 10) || options.limit;
      continue;
    }
    if (item === "--limit" && rest[index + 1]) {
      index += 1;
      options.limit = Number.parseInt(rest[index], 10) || options.limit;
      continue;
    }
    if (item.startsWith("--out=")) {
      options.jsonPath = item.slice("--out=".length);
      continue;
    }
    if (item === "--out" && rest[index + 1]) {
      index += 1;
      options.jsonPath = rest[index] || "";
      continue;
    }
    if (item.startsWith("--scope=")) {
      options.scopes.push(item.slice("--scope=".length));
      continue;
    }
    if (item === "--scope" && rest[index + 1]) {
      index += 1;
      options.scopes.push(rest[index] || "");
    }
  }
  options.limit = Math.max(1, Math.min(2000, options.limit));
  if (options.jsonPath) {
    options.jsonPath = resolve(options.jsonPath);
  }
  options.scopes = options.scopes.map((scope) => scope.trim()).filter(Boolean);
  return options;
}

function formatLearningReviewMergeReport(report) {
  if (!report?.ok) {
    return [
      "soty-memory-review: ok=false",
      `error: ${report?.error || "unknown"}`
    ].join("\n");
  }
  const lines = [
    `soty-memory-review: ok=true mode=${report.mode} scope=server-global`,
    `memory: schema=${report.memory?.schema || "soty.memory.query.v1"} receipts=${report.memory?.receipts || 0} devices=${Number(report.memory?.scope?.deviceCount || 0)} sent=${report.sync?.sent || 0} pending=${report.sync?.pending || 0}`,
    `hints: accepted=${report.accepted || 0} candidates=${report.candidates || 0}`,
    `publish: ${formatLearningPublishModel(report.memory)}`
  ];
  return lines.join("\n");
}

function memoryPlaneStatus() {
  let pending = 0;
  try {
    const text = readFileSync(learningOutboxPath, "utf8");
    pending = text.split(/\r?\n/u).filter((line) => line.trim()).length;
  } catch {
    pending = 0;
  }
  return {
    schema: "soty.memory-plane.v1",
    outbox: pending,
    syncUrl: agentRelayBaseUrl ? "/api/agent/memory/receipts" : "",
    queryUrl: agentRelayBaseUrl ? "/api/agent/memory/query" : "",
    reportUrl: agentRelayBaseUrl ? "/api/agent/memory/report" : "",
    installHash: hashText(agentInstallId).slice(0, 12)
  };
}

function blockedManualWindowsRecoveryHandoff(command) {
  if (process.env.SOTY_AGENT_ALLOW_MANUAL_WINRE_HANDOFF === "1") {
    return "";
  }
  const text = String(command || "").toLowerCase();
  const compact = text.replace(/\s+/gu, " ");
  const isManualRecovery = /\bsystemreset(?:\.exe)?\b/u.test(compact)
    || /\breagentc(?:\.exe)?\s+\/boottore\b/u.test(compact)
    || /\bshutdown(?:\.exe)?\b(?=[^;&|]*\s\/r\b)(?=[^;&|]*\s\/o\b)/u.test(compact)
    || /\bms-settings:recovery\b/u.test(compact)
    || /\bstart-process\b[^;&|]*\bsystemreset(?:\.exe)?\b/u.test(compact)
    || /\b(?:start-process|https?:\/\/)[^;&|]*microsoft\.com\/software-download\/windows(?:10|11)\b/u.test(compact);
  if (!isManualRecovery) {
    return "";
  }
  return [
    "! reinstall-route-blocked",
    "Internal tool note: do not open manual Windows Reset/Recovery/Media Creation Tool screens unless the user explicitly asks for a manual fallback.",
    "Continue with managed Soty preparation after the target channel is healthy. If the target channel is not healthy, tell the user plainly: I do not see the computer through Soty right now; restart or open Soty on that PC.",
    "Do not quote this tool note or mention router/preflight/gates/bridge/source-scoped internals in chat."
  ].join("\n");
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
  const sourceDeviceId = typeof payload.sourceDeviceId === "string" ? payload.sourceDeviceId.slice(0, maxSourceChars) : "";
  const sourceDeviceNick = typeof payload.sourceDeviceNick === "string" ? payload.sourceDeviceNick.slice(0, maxSourceChars) : "";
  const text = typeof payload.text === "string" ? payload.text.slice(0, maxChatChars) : "";
  const speed = typeof payload.speed === "string" ? payload.speed.slice(0, 20) : "";
  const persona = typeof payload.persona === "string" ? payload.persona.slice(0, 80) : "";
  if (!operatorBridge?.open || !target || !text.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  // Let the PWA validate the final visible target. Agent dialogs are not remote
  // command targets, but they are valid chat targets for operator status notes.
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
  const sourceDeviceId = typeof payload.sourceDeviceId === "string" ? payload.sourceDeviceId.slice(0, maxSourceChars) : "";
  const sourceDeviceNick = typeof payload.sourceDeviceNick === "string" ? payload.sourceDeviceNick.slice(0, maxSourceChars) : "";
  const text = typeof payload.text === "string" ? payload.text.slice(0, maxChatChars) : "";
  const timeoutMs = safeRunTimeoutMs(payload.timeoutMs);
  if (!operatorBridge?.open || !text.trim()) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  const id = registerOperatorRun(response, headers, timeoutMs);
  sendRaw(operatorBridge, {
    type: "operator.agent-message",
    id,
    target,
    sourceDeviceId,
    sourceDeviceNick,
    text
  });
}

async function handleOperatorHttpAgentNew(request, response, headers) {
  if (!operatorBridge?.open) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  let payload = {};
  try {
    payload = await readJsonBody(request, 4096);
  } catch {
    payload = {};
  }
  const timeoutMs = safeRunTimeoutMs(payload.timeoutMs || 120_000);
  const id = registerOperatorRun(response, headers, timeoutMs);
  sendRaw(operatorBridge, {
    type: "operator.agent-new",
    id
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

async function handleOperatorHttpExport(request, url, response, headers) {
  if (!operatorBridge?.open) {
    sendJson(response, 409, headers, { ok: false, text: "! bridge", exitCode: 409 });
    return;
  }
  let payload = {};
  if (request.method === "POST") {
    try {
      payload = await readJsonBody(request, 4096);
    } catch {
      sendJson(response, 400, headers, { ok: false, text: "! json", exitCode: 400 });
      return;
    }
  }
  const target = typeof payload.target === "string"
    ? payload.target.slice(0, 160)
    : (url.searchParams.get("target") || "").slice(0, 160);
  const rawTailChars = Number(payload.tailChars ?? url.searchParams.get("tailChars") ?? 0);
  const tailChars = Number.isSafeInteger(rawTailChars)
    ? Math.max(0, Math.min(200_000, rawTailChars))
    : 0;
  const id = registerOperatorRun(response, headers, 60_000);
  sendRaw(operatorBridge, {
    type: "operator.export",
    id,
    target,
    tailChars
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
  const cancelBridgeRun = () => {
    if (operatorBridge?.open) {
      sendRaw(operatorBridge, { type: "operator.cancel", id });
    }
  };
  const finish = (exitCode, extraText = "") => {
    if (done) {
      return;
    }
    done = true;
    clearTimeout(timer);
    response.off?.("close", cancelOnClientClose);
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
  const cancelOnClientClose = () => {
    if (done) {
      return;
    }
    cancelBridgeRun();
    finish(130, "! cancelled");
  };
  response.on?.("close", cancelOnClientClose);
  const timer = setTimeout(() => {
    cancelBridgeRun();
    finish(124, "! timeout");
  }, timeoutMs);
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
    sourceDeviceId: safeSourceText(message.sourceDeviceId || message.deviceId),
    sourceDeviceNick: safeSourceText(message.sourceDeviceNick || message.deviceNick),
    agent: message.agent === true,
    text,
    context: typeof message.context === "string" ? message.context.slice(-maxAgentContextChars) : "",
    createdAt: typeof message.createdAt === "string" && message.createdAt.length <= 80 ? message.createdAt : new Date().toISOString()
  };
  operatorMessages.push(item);
  while (operatorMessages.length > 500) {
    operatorMessages.shift();
  }
  flushOperatorMessageWaiters();
  if (isDuplicateAgentOperatorMessage(item)) {
    return;
  }
  maybeStartAgentOperatorReply(item);
}

function maybeStartAgentOperatorReply(item) {
  if (!shouldAutoReplyOperatorMessage(item)) {
    return;
  }
  const previous = agentOperatorReplyQueues.get(item.target) || Promise.resolve();
  const next = previous
    .catch(() => undefined)
    .then(() => replyToAgentOperatorMessage(item));
  agentOperatorReplyQueues.set(item.target, next);
  void next.finally(() => {
    if (agentOperatorReplyQueues.get(item.target) === next) {
      agentOperatorReplyQueues.delete(item.target);
    }
  });
}

function isAgentOperatorMessage(item) {
  const label = String(item?.label || "").trim().toLowerCase();
  return item?.agent === true || label === "агент" || label === "codex";
}

function shouldAutoReplyOperatorMessage(item) {
  return isAgentOperatorMessage(item) || isActionableTargetOperatorMessage(item);
}

function isActionableTargetOperatorMessage(item) {
  if (!item || isAgentOperatorMessage(item)) {
    return false;
  }
  const text = String(item.text || "").trim();
  if (!text) {
    return false;
  }
  const directAgentMention = /(?:^|[\s,.:;!?])(?:агент|соты|лорд\s+роя|codex)(?:$|[\s,.:;!?])/iu.test(text);
  if (!directAgentMention && isLikelyAgentStatusQuote(text)) {
    return false;
  }
  const highConfidenceDeviceTask = /(?:переустанов\p{L}*|перестанов\p{L}*|винд\p{L}*|windows|win11|установочн\p{L}*\s+флеш\p{L}*|флеш\p{L}*.*винд\p{L}*|стираем\s+вс[её]|стереть\s+вс[её]|формат\p{L}*|бэкап\p{L}*|резервн\p{L}*\s+коп\p{L}*|\b(?:backup|reinstall|windows\s+reinstall|factory\s+reset|format)\b)/iu.test(text);
  return directAgentMention || highConfidenceDeviceTask;
}

function isLikelyAgentStatusQuote(text) {
  const value = String(text || "").trim();
  if (!value) {
    return false;
  }
  const statusStart = /^(?:reinstall|reset)\b.{0,120}\b(?:not started|did not start)\b/iu.test(value);
  const commandStart = !statusStart && /^(?:переустанови|переустановить|снеси|сотри|стереть|форматируй|подготовь|запусти|продолжай|готово\b|reinstall|reset|format|prepare|start|continue)\b/iu.test(value);
  if (commandStart) {
    return false;
  }
  const lines = value
    .split(/\r?\n/u)
    .map((line) => line.replace(/^>\s?/u, "").trim())
    .filter(Boolean)
    .slice(0, 8);
  if (lines.length === 0) {
    return false;
  }
  const joined = lines.join("\n");
  const markers = [
    /^(?:переустановку|переустановка|windows reinstall|reinstall)(?:$|[\s,.:;!?]).{0,120}(?:не начал|не начата|не запускал|not started|did not start)\b/iu,
    /^проверил безопасно\b/iu,
    /^блокер\s*:/iu,
    /\b(?:install-media-not-ready|backup-not-ready|not-elevated|usb-not-found|usb-not-removable|usb-free-space-low)\b/iu,
    /\bдиск\s+windows\s+не\s+трогал\b/iu,
    /^blocker\s*:/iu,
    /\bwindows\s+disk\b.{0,80}\b(?:not touched|untouched)\b/iu
  ];
  const hits = markers.reduce((count, pattern) => count + (pattern.test(joined) ? 1 : 0), 0);
  return hits >= 2 || (markers[0].test(lines[0] || "") && hits >= 1);
}

function isDuplicateAgentOperatorMessage(item) {
  if (!shouldAutoReplyOperatorMessage(item)) {
    return false;
  }
  const now = Date.now();
  for (const [key, seenAt] of recentAgentOperatorMessageKeys) {
    if (now - seenAt > 15000) {
      recentAgentOperatorMessageKeys.delete(key);
    }
  }
  const key = `${item.target}\n${item.text}`;
  const previous = recentAgentOperatorMessageKeys.get(key) || 0;
  recentAgentOperatorMessageKeys.set(key, now);
  return previous > 0 && now - previous < 8000;
}

async function replyToAgentOperatorMessage(item) {
  const agentDialog = isAgentOperatorMessage(item);
  const source = {
    tunnelId: item.target,
    tunnelLabel: item.label || "Агент",
    deviceId: item.sourceDeviceId || operatorDeviceId || "",
    deviceNick: item.sourceDeviceNick || operatorDeviceNick || "",
    appOrigin: agentRelayBaseUrl || originFromUrl(updateManifestUrl) || "https://xn--n1afe0b.online",
    preferredTargetId: agentDialog ? "" : item.target,
    preferredTargetLabel: agentDialog ? "" : item.label,
    operatorTargets
  };
  const streamedMessages = [];
  const result = await askCodexForAgentReply(item.text, item.context || "", source, (message) => {
    const clean = cleanAgentChatReply(message);
    if (!clean || streamedMessages[streamedMessages.length - 1] === clean) {
      return;
    }
    streamedMessages.push(clean);
    sendAgentOperatorChat(item.target, clean);
  }, (message) => {
    sendAgentOperatorTerminal(item.target, message);
  });
  const delivered = new Set(streamedMessages);
  const messages = Array.isArray(result.messages)
    ? result.messages.map((message) => cleanAgentChatReply(message)).filter((message) => message && !delivered.has(message))
    : [];
  if (messages.length > 0) {
    sendAgentOperatorChat(item.target, messages.join("\n\n"));
    return;
  }
  const finalText = cleanAgentChatReply(result.text || "");
  const streamedText = cleanAgentChatReply(streamedMessages.join("\n\n"));
  if (streamedText && finalText === streamedText) {
    return;
  }
  if (finalText && !delivered.has(finalText)) {
    sendAgentOperatorChat(item.target, finalText);
  }
}

function sendAgentOperatorChat(target, text) {
  const body = cleanAgentChatReply(text);
  if (!operatorBridge?.open || !target || !body) {
    return false;
  }
  sendRaw(operatorBridge, {
    type: "operator.chat",
    id: `agent_chat_${randomUUID()}`,
    target,
    text: body,
    speed: "instant",
    persona: "sysadmin"
  });
  return true;
}

function sendAgentOperatorTerminal(target, text) {
  const body = cleanTerminalTranscript(text);
  if (!operatorBridge?.open || !target || !body) {
    return false;
  }
  sendRaw(operatorBridge, {
    type: "operator.terminal",
    id: `agent_terminal_${randomUUID()}`,
    target,
    text: body
  });
  return true;
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
  const context = typeof payload.context === "string" ? payload.context.slice(-maxAgentContextChars) : "";
  const source = sanitizeAgentSource(payload.source);
  if (!text.trim()) {
    sendJson(response, 400, headers, { ok: false, text: "! text", exitCode: 400 });
    return;
  }
  const terminal = [];
  const result = await askCodexForAgentReply(text, context, source, null, (message) => {
    const clean = cleanTerminalTranscript(message);
    if (clean && terminal[terminal.length - 1] !== clean) {
      terminal.push(clean);
    }
  });
  sendJson(response, result.ok ? 200 : 502, headers, {
    ...result,
    ...(terminal.length > 0 ? { terminal } : {})
  });
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
  await saveAgentConfig();
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
  scheduleLearningSync();
  void runAgentRelayLoop();
}

async function runAgentRelayLoop() {
  let retryMs = 1000;
  while (true) {
    try {
      if (!hasCodexBinary()) {
        await sleep(30_000);
        continue;
      }
      if (activeRelayJobs.size >= maxConcurrentCodexJobs) {
        await sleep(500);
        continue;
      }
      const jobs = await pollAgentRelay();
      retryMs = 1000;
      for (const job of jobs) {
        scheduleAgentRelayJob(job);
      }
    } catch {
      await sleep(retryMs);
      retryMs = Math.min(30_000, Math.round(retryMs * 1.6));
    }
  }
}

function scheduleAgentRelayJob(job) {
  const task = handleAgentRelayJob(job)
    .catch(async (error) => {
      await postAgentRelayReply(job.id, {
        ok: false,
        text: agentFailureText(error instanceof Error ? error.message : String(error)),
        exitCode: 1
      }).catch(() => undefined);
    })
    .finally(() => {
      activeRelayJobs.delete(task);
    });
  activeRelayJobs.add(task);
}

async function pollAgentRelay() {
  if (!hasCodexBinary()) {
    return [];
  }
  const url = new URL("/api/agent/relay/poll", agentRelayBaseUrl);
  url.searchParams.set("relayId", agentRelayId);
  url.searchParams.set("version", agentVersion);
  url.searchParams.set("codex", "1");
  if (operatorDeviceId) {
    url.searchParams.set("deviceId", operatorDeviceId);
  } else if (configuredAgentDeviceId) {
    url.searchParams.set("deviceId", configuredAgentDeviceId);
  }
  if (operatorDeviceNick) {
    url.searchParams.set("deviceNick", operatorDeviceNick);
  } else if (configuredAgentDeviceNick) {
    url.searchParams.set("deviceNick", configuredAgentDeviceNick);
  }
  url.searchParams.set("scope", agentScope);
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
    String(job.context || "").slice(-maxAgentContextChars),
    sanitizeAgentSource(job.source),
    (message) => postAgentRelayEvent(job.id, message),
    (message) => postAgentRelayEvent(job.id, message, "agent_terminal")
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
      ...(Array.isArray(result.messages) && result.messages.length > 0
        ? { messages: result.messages.map((item) => String(item || "").slice(0, maxChatChars)).filter(Boolean).slice(-maxCodexDialogMessages) }
        : {}),
      ...(Array.isArray(result.terminal) && result.terminal.length > 0
        ? { terminal: result.terminal.map((item) => String(item || "").slice(0, maxChatChars)).filter(Boolean).slice(-maxCodexDialogMessages) }
        : {}),
      ...(result.traceId ? { traceId: String(result.traceId).slice(0, 120) } : {}),
      ...(typeof result.exitCode === "number" ? { exitCode: result.exitCode } : {})
    })
  });
  if (!response.ok) {
    throw new Error(`relay reply ${response.status}`);
  }
}

async function postAgentRelayEvent(id, message, type = "agent_message") {
  const text = type === "agent_terminal" ? cleanTerminalTranscript(message) : cleanAgentChatReply(message);
  if (!text) {
    return;
  }
  await fetch(new URL("/api/agent/relay/event", agentRelayBaseUrl), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      relayId: agentRelayId,
      id,
      type,
      text: text.slice(0, maxChatChars)
    })
  }).catch(() => undefined);
}

async function askCodexForAgentReply(text, context, source = {}, onMessage = null, onTerminal = null) {
  const trace = await beginAgentTrace({ entrypoint: "agent.reply", text, context, source });
  try {
    traceStep(trace, "agent.start", {
      codexDisabled,
      codexProbe: hasCodexBinary(),
      relayFallback: codexRelayFallback
    });
    const fast = await tryAgentRuntimeFastReply({ text, source, trace });
    if (fast) {
      traceRouting(trace, { finalRoute: "agent-runtime.fast" });
      await finishAgentTrace(trace, fast);
      return withTraceId(fast, trace);
    }
    const codexBin = hasCodexBinary() ? findCodexBinary() : "";
    if (!codexBin) {
      traceStep(trace, "codex.missing", { codexDisabled, relayFallback: codexRelayFallback });
      const relay = codexRelayFallback
        ? await askCodexRelayFallback(text, context, source, onMessage, onTerminal)
        : null;
      if (relay) {
        traceRouting(trace, { finalRoute: "codex.relay-fallback" });
        await finishAgentTrace(trace, relay);
        return withTraceId(relay, trace);
      }
      const missing = {
        ok: false,
        text: "! codex-cli: not found on this computer",
        exitCode: 126
      };
      await finishAgentTrace(trace, missing);
      return withTraceId(missing, trace);
    }

    const codexHome = await preparePersistentStockCodexHome();
    const childEnv = withAgentToolPath({
      ...process.env,
      ...codexNetworkProxyEnv(),
      CODEX_HOME: codexHome
    });
    traceStep(trace, "codex.local.ready", {
      codexBinary: basename(codexBin),
      codexHome,
      proxy: Boolean(codexProxyUrl)
    });
    const local = await runCodexSotySessionTurn({
      codexBin,
      childEnv,
      text,
      context,
      source,
      onMessage,
      onTerminal,
      trace
    });
    if (shouldUseCodexRelayFallback(local)) {
      const relay = await askCodexRelayFallback(text, context, source, onMessage, onTerminal, { preferServer: true });
      if (relay) {
        traceRouting(trace, { finalRoute: "codex.relay-fallback-after-local" });
        await finishAgentTrace(trace, relay);
        return withTraceId(relay, trace);
      }
    }
    await finishAgentTrace(trace, local);
    return withTraceId(local, trace);
  } catch (error) {
    const local = {
      ok: false,
      text: agentFailureText(error instanceof Error ? error.message : String(error)),
      exitCode: 1
    };
    traceStep(trace, "agent.error", { message: error instanceof Error ? error.message : String(error) });
    if (shouldUseCodexRelayFallback(local)) {
      const relay = await askCodexRelayFallback(text, context, source, onMessage, onTerminal, { preferServer: true });
      if (relay) {
        traceRouting(trace, { finalRoute: "codex.relay-fallback-after-error" });
        await finishAgentTrace(trace, relay);
        return withTraceId(relay, trace);
      }
    }
    await finishAgentTrace(trace, local);
    return withTraceId(local, trace);
  }
}

async function tryAgentRuntimeFastReply({ text, source = {}, trace = null }) {
  const startedAt = Date.now();
  const safeSource = sanitizeAgentSource(source);
  const directFast = tryFastDirectAgentReply({
    text,
    source: safeSource,
    startedAt
  });
  if (directFast) {
    traceStep(trace, "fast.direct.hit", { enabled: enableFastDirectAnswers });
    return directFast;
  }
  const sourceTargets = await activeAgentSourceTargets(safeSource.sourceRelayId);
  const target = resolveAgentBridgeTarget(safeSource, text, sourceTargets);
  const learningContext = learningContextForTurn(safeSource, target);
  const taskFamily = classifyTaskFamily(text, target);
  traceRouting(trace, {
    taskFamily,
    targetId: target?.id || "",
    targetLabel: target?.label || "",
    activeTargets: sourceTargets.length,
    route: "agent-runtime.fast-check"
  });
  const reply = await tryFastRoutineAgentReply({
    text,
    source: safeSource,
    target,
    taskFamily,
    startedAt,
    learningContext
  });
  traceStep(trace, "fast.routine", {
    taskFamily,
    hit: Boolean(reply),
    exitCode: Number.isSafeInteger(reply?.exitCode) ? reply.exitCode : undefined
  });
  return reply;
}

async function runCodexSotySessionTurn({ codexBin, childEnv, text, context = "", source, onMessage, onTerminal, trace = null }) {
  const startedAt = Date.now();
  const safeSource = sanitizeAgentSource(source);
  const directFast = tryFastDirectAgentReply({
    text,
    source: safeSource,
    startedAt
  });
  if (directFast) {
    return directFast;
  }
  const sourceTargets = await activeAgentSourceTargets(safeSource.sourceRelayId);
  const target = resolveAgentBridgeTarget(safeSource, text, sourceTargets);
  const learningContext = learningContextForTurn(safeSource, target);
  const taskFamily = classifyTaskFamily(text, target);
  const sessionKey = codexSessionKey(safeSource, target, taskFamily);
  const turnKey = codexTurnDedupeKey(sessionKey, text);
  traceRouting(trace, {
    route: "codex.local",
    taskFamily,
    targetId: target?.id || "",
    targetLabel: target?.label || "",
    activeTargets: sourceTargets.length,
    sessionKey: hashText(sessionKey).slice(0, 16)
  });
  if (isDuplicateCodexTurn(turnKey)) {
    const duplicate = { ok: true, text: "", messages: [], exitCode: 0 };
    traceStep(trace, "codex.duplicate-turn", { sessionKey: hashText(sessionKey).slice(0, 16) });
    return duplicate;
  }
  const reinstallGate = await tryFastWindowsReinstallGateReply({
    text,
    source: safeSource,
    target,
    taskFamily,
    startedAt,
    learningContext
  });
  if (reinstallGate) {
    traceRouting(trace, { finalRoute: "agent-runtime.fast-reinstall-preflight" });
    traceStep(trace, "fast.reinstall-gate.hit", { taskFamily, exitCode: reinstallGate.exitCode });
    return reinstallGate;
  }
  const fastRoutine = await tryFastRoutineAgentReply({
    text,
    source: safeSource,
    target,
    taskFamily,
    startedAt,
    learningContext
  });
  if (fastRoutine) {
    traceRouting(trace, { finalRoute: "agent-runtime.fast-source-script" });
    traceStep(trace, "fast.routine-after-codex-check.hit", { taskFamily, exitCode: fastRoutine.exitCode });
    return fastRoutine;
  }
  const sessionRecord = target?.id ? null : usableCodexSessionRecord(persistedCodexSessions[sessionKey]);
  const jobDir = await prepareCodexWorkspace(sessionKey, sessionRecord);
  const runtimeContext = await buildAgentRuntimeContext({
    text,
    context,
    source: safeSource,
    target,
    sourceTargets,
    sessionRecord,
    jobDir
  });
  await writeCodexRuntimeFiles(jobDir, runtimeContext);
  const prompt = buildAgentPrompt(text, context, runtimeContext);
  const outPath = join(jobDir, `last-message-${randomUUID()}.txt`);
  if (trace?.doc) {
    trace.doc.codex.jobDir = jobDir;
  }
  traceStep(trace, "codex.workspace", {
    jobDir,
    resumed: Boolean(sessionRecord?.threadId),
    promptChars: prompt.length,
    memoryChars: String(runtimeContext.memory || "").length
  });
  await traceWriteJson(trace, "runtime-context.json", runtimeContext);
  if (agentTraceFullPrompt) {
    await traceWriteText(trace, "prompt.txt", prompt, maxAgentRuntimePromptChars + 2000);
  }
  const guardTurnkeyMessages = shouldGuardTurnkeyTask(taskFamily, text);
  const bufferedCodexMessages = [];
  const codexOnMessage = guardTurnkeyMessages
    ? (message) => {
      const clean = cleanAgentChatReply(message);
      if (clean) {
        bufferedCodexMessages.push(clean);
      }
    }
    : onMessage;
  const args = codexSotySessionArgs({
    jobDir,
    target,
    source: safeSource,
    outPath,
    threadId: sessionRecord?.threadId || "",
    taskFamily
  });
  if (trace?.doc) {
    trace.doc.codex.spawned = true;
    trace.doc.codex.args = traceValue(args, 8000, 3);
  }
  await traceWriteJson(trace, "codex-args.json", {
    file: basename(codexBin),
    args,
    outPath,
    reasoningEffort: codexReasoningEffortForTask(taskFamily, target),
    mcpAttached: args.some((item) => String(item).includes("mcp_servers.soty"))
  });
  const state = {
    threadId: "",
    lastMessage: "",
    messages: [],
    terminal: [],
    terminalKeys: new Set(),
    learningMarkers: [],
    usage: emptyCodexUsage(),
    trace
  };
  let result = await runCodexForSotyChat(codexBin, args, childEnv, agentReplyTimeoutMs, prompt, state, jobDir, codexOnMessage, onTerminal);
  if (sessionRecord?.threadId && shouldRetryCodexWithoutResume(result, state)) {
    const freshState = {
      threadId: "",
      lastMessage: "",
      messages: [],
      terminal: [],
      terminalKeys: new Set(),
      learningMarkers: [],
      usage: emptyCodexUsage(),
      trace
    };
    const freshArgs = codexSotySessionArgs({
      jobDir,
      target,
      source: safeSource,
      outPath,
      threadId: "",
      taskFamily
    });
    delete persistedCodexSessions[sessionKey];
    await saveCodexSessions();
    result = await runCodexForSotyChat(codexBin, freshArgs, childEnv, agentReplyTimeoutMs, prompt, freshState, jobDir, codexOnMessage, onTerminal);
    state.threadId = freshState.threadId;
    state.lastMessage = freshState.lastMessage;
    state.messages = freshState.messages;
    state.terminal = freshState.terminal;
    state.terminalKeys = freshState.terminalKeys;
    state.learningMarkers = freshState.learningMarkers;
    state.usage = freshState.usage;
  }
  const lastFileRaw = existsSync(outPath) ? await readFile(outPath, "utf8") : "";
  await traceWriteText(trace, "last-message.txt", lastFileRaw, maxChatChars + 2000);
  pushLearningMarkers(state, extractInternalLearningMarkers(lastFileRaw));
  const lastFromFile = cleanAgentChatReply(lastFileRaw);
  let messages = compactCodexMessages(state.messages.length > 0 ? state.messages : [lastFromFile]);
  let finalText = cleanAgentChatReply(messages.join("\n\n") || state.lastMessage || lastFromFile);
  if (state.threadId) {
    persistedCodexSessions[sessionKey] = {
      threadId: state.threadId,
      mode: codexSessionMode,
      workspaceDir: jobDir,
      sourceDeviceId: safeSource.deviceId || "",
      tunnelId: safeSource.tunnelId || "",
      updatedAt: new Date().toISOString()
    };
    await saveCodexSessions();
  }
  if (result.exitCode === 0) {
    if (!finalText) {
      traceStep(trace, "codex.no-final-message", {
        messages: messages.length,
        stdout: Boolean(result.stdout),
        stderr: Boolean(result.stderr),
        usage: state.usage
      });
      recordLearningReceipt({
        kind: "codex-turn",
        family: taskFamily === "generic" ? "no-final-assistant-message" : taskFamily,
        result: "failed",
        route: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume",
        taskSig: taskSignature(text),
        proof: `exitCode=0; messages=${messages.length}; stdout=${result.stdout ? "nonempty" : "empty"}; stderr=${result.stderr ? "nonempty" : "empty"}; ${codexUsageProof(state.usage, prompt, finalText)}`,
        exitCode: 125,
        durationMs: Date.now() - startedAt,
        ...learningContext
      });
      recordAgentLearningMarkers(state.learningMarkers, {
        route: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume",
        taskSig: taskSignature(text),
        durationMs: Date.now() - startedAt,
        ...learningContext
      });
      return {
        ok: false,
        text: agentFailureText("Codex CLI exited successfully but did not produce a final assistant message."),
        ...(state.terminal.length > 0 ? { terminal: state.terminal } : {}),
        exitCode: 125
      };
    }
    const turnkeyGuard = await waitForTurnkeyTargetAfterCodex({
      text,
      taskFamily,
      target,
      source: safeSource,
      startedAt,
      finalText,
      onMessage
    });
    if (turnkeyGuard?.text) {
      finalText = cleanAgentChatReply(turnkeyGuard.text);
      messages = compactCodexMessages([...messages, finalText]);
    }
    recordLearningReceipt({
      kind: "codex-turn",
      family: taskFamily,
      result: "ok",
      route: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume",
      taskSig: taskSignature(text),
      proof: `exitCode=0; messages=${messages.length}; final=nonempty; ${codexUsageProof(state.usage, prompt, finalText)}`,
      exitCode: 0,
      durationMs: Date.now() - startedAt,
      ...learningContext
    });
    recordAgentLearningMarkers(state.learningMarkers, {
      route: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume",
      taskSig: taskSignature(text),
      durationMs: Date.now() - startedAt,
      ...learningContext
    });
    if (trace?.doc) {
      trace.doc.codex.usage = state.usage;
    }
    traceRouting(trace, { finalRoute: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume" });
    traceStep(trace, "codex.ok", {
      messages: messages.length,
      terminal: state.terminal.length,
      usage: state.usage
    });
    return {
      ok: true,
      text: finalText.slice(0, maxChatChars),
      ...(messages.length > 0 ? { messages } : {}),
      ...(state.terminal.length > 0 ? { terminal: state.terminal } : {}),
      exitCode: 0
    };
  }
  if (trace?.doc) {
    trace.doc.codex.usage = state.usage;
  }
  traceRouting(trace, { finalRoute: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume" });
  traceStep(trace, "codex.nonzero", {
    exitCode: result.exitCode || 1,
    stdout: Boolean(result.stdout),
    stderr: Boolean(result.stderr),
    finalText: Boolean(finalText),
    usage: state.usage
  });
  recordLearningReceipt({
    kind: "codex-turn",
    family: taskFamily === "generic" ? "codex-cli-nonzero" : taskFamily,
    result: result.exitCode === 124 ? "timeout" : "failed",
    route: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume",
    taskSig: taskSignature(text),
    proof: `exitCode=${result.exitCode || 1}; stderr=${result.stderr ? "nonempty" : "empty"}; stdout=${result.stdout ? "nonempty" : "empty"}; final=${finalText ? "nonempty" : "empty"}; ${codexUsageProof(state.usage, prompt, finalText)}`,
    exitCode: result.exitCode || 1,
    durationMs: Date.now() - startedAt,
    ...learningContext
  });
  recordAgentLearningMarkers(state.learningMarkers, {
    route: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume",
    taskSig: taskSignature(text),
    durationMs: Date.now() - startedAt,
    ...learningContext
  });
  return {
    ok: false,
    text: agentFailureText(`${result.stderr}\n${result.stdout}\n${finalText}`),
    ...(state.terminal.length > 0 ? { terminal: state.terminal } : {}),
    exitCode: result.exitCode || 1
  };
}

async function waitForTurnkeyTargetAfterCodex({ text, taskFamily, target, source, startedAt, finalText, onMessage }) {
  if (!target?.id || !shouldGuardTurnkeyTask(taskFamily, text)) {
    return null;
  }
  if (target.access !== true && !isAgentSourceTarget(target.id)) {
    return null;
  }
  const sourceDeviceId = bridgeSourceDeviceId(target, source);
  if (!sourceDeviceId) {
    return null;
  }
  return await waitForManagedReinstallAfterCodex({
    target,
    sourceDeviceId,
    startedAt,
    finalText,
    onMessage
  }).catch(() => null);
}

function shouldGuardTurnkeyTask(taskFamily, text) {
  return taskFamily === "windows-reinstall" || classifySourceCommand(text) === "windows-reinstall";
}

async function waitForManagedReinstallAfterCodex({ target, sourceDeviceId, startedAt, finalText, onMessage }) {
  const started = Date.now();
  const deadline = started + turnkeyGuardTimeoutMs;
  let sawManagedStatus = false;
  let consecutiveMisses = 0;
  let lastStatus = null;
  let progressAt = 0;
  const recentJobs = await recentTurnkeyActionJobs(target.id, sourceDeviceId, startedAt);
  while (Date.now() < deadline) {
    const probe = await readManagedReinstallStatusForTarget(target.id, sourceDeviceId);
    const status = parseManagedReinstallStatusProbe(probe);
    if (status) {
      sawManagedStatus = true;
      consecutiveMisses = 0;
      lastStatus = status;
      const terminal = managedReinstallGuardTerminal(status);
      if (terminal) {
        return terminal;
      }
      if (isManagedReinstallGuardActive(status)) {
        if (!progressAt || Date.now() - progressAt > turnkeyGuardProgressMs) {
          progressAt = Date.now();
          await postTurnkeyGuardProgress(onMessage, managedReinstallGuardProgress(status));
        }
        await sleep(managedReinstallGuardPollMs(status));
        continue;
      }
      return recentJobs.length > 0 ? null : null;
    }
    consecutiveMisses += 1;
    if (!sawManagedStatus && consecutiveMisses >= 2) {
      return null;
    }
    if (sawManagedStatus && consecutiveMisses >= 2) {
      return {
        text: "не могу дочитать статус на этом устройстве\nоткрой Soty Agent там и напиши «готово»"
      };
    }
    await sleep(15_000);
  }
  if (lastStatus && isManagedReinstallGuardActive(lastStatus)) {
    return {
      text: "подготовка ещё идёт\nдиск Windows не трогаю\nоткрой Soty и напиши «продолжай»"
    };
  }
  return finalText ? null : {
    text: "задача не дошла до понятного конца\nпроверь Soty на этом устройстве"
  };
}

async function recentTurnkeyActionJobs(targetId, sourceDeviceId, startedAt) {
  const cutoff = Math.max(0, Number(startedAt || 0) - 2 * 60_000);
  const jobs = await listActionJobs().catch(() => []);
  return jobs.filter((job) => {
    const family = String(job.family || "").toLowerCase();
    const risk = String(job.risk || "").toLowerCase();
    if (family !== "windows-reinstall" && risk !== "high" && risk !== "destructive") {
      return false;
    }
    if (targetId && job.target && job.target !== targetId) {
      return false;
    }
    if (sourceDeviceId && job.sourceDeviceId && job.sourceDeviceId !== sourceDeviceId) {
      return false;
    }
    const time = Date.parse(job.startedAt || job.createdAt || "");
    return Number.isFinite(time) ? time >= cutoff : true;
  });
}

async function readManagedReinstallStatusForTarget(targetId, sourceDeviceId) {
  return await postLocalOperatorScript(targetId, sourceDeviceId, {
    script: sourceManagedWindowsReinstallScript(managedReinstallGuardRequest("status")),
    shell: "powershell",
    name: "soty-reinstall-status"
  }, 45_000);
}

function managedReinstallGuardRequest(action = "status") {
  return {
    action,
    usbDriveLetter: "D",
    confirmationPhrase: "",
    useExistingUsbInstallImage: false,
    manifestUrl: updateManifestUrl,
    panelSiteUrl: originFromUrl(updateManifestUrl) || agentRelayBaseUrl || "https://xn--n1afe0b.online",
    workspaceRoot: "C:\\ProgramData\\Soty\\WindowsReinstall"
  };
}

function parseManagedReinstallStatusProbe(result) {
  const parsed = parseJsonObjectLoose(result?.text || result?.payload?.text || "");
  return parsed?.action === "status" ? parsed : null;
}

function managedReinstallGuardTerminal(status) {
  const blockers = managedReinstallGuardReadyBlockers(status);
  if (status?.ready === true && blockers.length === 0) {
    const phrase = String(status.confirmationPhrase || "ERASE INTERNAL DISK").trim();
    return {
      text: `готово\nбэкап и установочная флешка проверены\nнапиши точно: ${phrase}`
    };
  }
  if (status?.ready === true && blockers.length > 0) {
    return {
      text: `стоп\nподготовка неполная: ${blockers.join(", ")}`
    };
  }
  if (isManagedReinstallGuardActive(status)) {
    return null;
  }
  const latest = status?.latestPrepare && typeof status.latestPrepare === "object" ? status.latestPrepare : null;
  const latestStatus = String(latest?.status || "").toLowerCase();
  if (latest && latestStatus && !["running-or-started", "running", "created"].includes(latestStatus)) {
    return {
      text: "стоп\nподготовка остановилась до готовности\nдиск Windows не трогаю"
    };
  }
  return null;
}

function isManagedReinstallGuardActive(status) {
  const media = status?.media && typeof status.media === "object" ? status.media : null;
  if (media?.downloading === true && (
    media?.active === true
    || (Number.isFinite(Number(media?.updatedAgeSeconds)) && Number(media.updatedAgeSeconds) < 900)
  )) {
    return true;
  }
  const latest = status?.latestPrepare && typeof status.latestPrepare === "object" ? status.latestPrepare : null;
  const latestStatus = String(latest?.status || "").toLowerCase();
  return latestStatus === "running-or-started" || latestStatus === "running" || latestStatus === "created";
}

function managedReinstallGuardReadyBlockers(status) {
  const blockers = [];
  if (String(status?.managedUserName || "") !== "Соты") {
    blockers.push("local-account");
  }
  if (String(status?.managedUserPasswordMode || "") !== "blank-no-password") {
    blockers.push("passwordless-account");
  }
  if (status?.backupProofOk !== true) {
    blockers.push("backup");
  }
  if (!String(status?.installImage || "")) {
    blockers.push("install-media");
  }
  if (status?.rootAutounattend !== true) {
    blockers.push("autounattend");
  }
  if (status?.oemSetupComplete !== true) {
    blockers.push("setupcomplete");
  }
  return blockers;
}

function managedReinstallGuardProgress(status) {
  const media = status?.media && typeof status.media === "object" ? status.media : null;
  if (media?.downloading === true) {
    const gb = Number.isFinite(Number(media.gb)) ? `, скачано примерно ${media.gb} ГБ` : "";
    return `подготовка идёт: образ Windows${gb}\nдиск Windows не трогаю`;
  }
  return "подготовка идёт\nдиск Windows не трогаю";
}

function managedReinstallGuardPollMs(status) {
  return status?.media?.downloading === true ? 120_000 : 60_000;
}

async function postTurnkeyGuardProgress(onMessage, text) {
  const clean = cleanAgentChatReply(text);
  if (!clean || typeof onMessage !== "function") {
    return;
  }
  await Promise.resolve(onMessage(clean)).catch(() => undefined);
}

function parseJsonObjectLoose(value) {
  const text = String(value || "").trim();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {}
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start >= 0 && end > start) {
    try {
      return JSON.parse(text.slice(start, end + 1));
    } catch {}
  }
  return null;
}

function codexTurnDedupeKey(sessionKey, text) {
  const body = String(text || "").replace(/\r\n?/gu, "\n").trim();
  if (!body) {
    return "";
  }
  return `${sessionKey}\n${body}`;
}

function isDuplicateCodexTurn(key) {
  if (!key) {
    return false;
  }
  const now = Date.now();
  for (const [entry, seenAt] of recentCodexTurnKeys) {
    if (now - seenAt > 15000) {
      recentCodexTurnKeys.delete(entry);
    }
  }
  const previous = recentCodexTurnKeys.get(key) || 0;
  recentCodexTurnKeys.set(key, now);
  return previous > 0 && now - previous < 8000;
}

function codexSotySessionArgs({ jobDir, target, source, outPath, threadId = "", taskFamily = "generic" }) {
  const resumeThreadId = safeCodexThreadId(threadId);
  const args = resumeThreadId
    ? ["exec", "resume", "--skip-git-repo-check", "--json"]
    : ["exec", "--skip-git-repo-check", "--cd", jobDir, "--json"];
  const reasoningEffort = codexReasoningEffortForTask(taskFamily, target);
  if (reasoningEffort) {
    args.push("-c", `model_reasoning_effort=${JSON.stringify(reasoningEffort)}`);
  }
  if (codexFullLocalTools) {
    args.push("--dangerously-bypass-approvals-and-sandbox");
  }
  const targetId = target?.id || "";
  const safeSource = sanitizeAgentSource(source);
  const sourceDeviceId = bridgeSourceDeviceId(target, safeSource);
  const sourceRelayId = safeRelayId(safeSource.sourceRelayId) || agentRelayId;
  const family = cleanActionToken(taskFamily, "generic");
  const attachSotyMcp = !(family === "plain-dialog" && !targetId);
  const mcpArgs = [
    scriptPath,
    "mcp",
    "--port",
    String(port)
  ];
  if (sourceRelayId) {
    mcpArgs.push("--source-relay", sourceRelayId);
  }
  if (targetId && sourceDeviceId) {
    mcpArgs.push("--target", targetId, "--source-device", sourceDeviceId);
  }
  if (attachSotyMcp) {
    args.push("-c", `mcp_servers.soty.command=${JSON.stringify(process.execPath)}`);
    args.push("-c", `mcp_servers.soty.args=${JSON.stringify(mcpArgs)}`);
    for (const tool of ["soty_toolkit", "soty_toolkits", "soty_reinstall", "soty_action", "soty_action_status", "soty_action_stop", "soty_action_list", "soty_link_status", "soty_run", "soty_script", "soty_file", "soty_browser", "soty_desktop", "soty_open_url", "soty_audio", "soty_image"]) {
      args.push("-c", `mcp_servers.soty.tools.${tool}.approval_mode="approve"`);
    }
  }
  if (outPath) {
    args.push("-o", outPath);
  }
  if (resumeThreadId) {
    args.push(resumeThreadId);
  }
  args.push("-");
  return args;
}

function codexReasoningEffortForTask(taskFamily, target = null) {
  if (codexDefaultReasoningEffort) {
    return codexDefaultReasoningEffort;
  }
  const family = cleanActionToken(taskFamily, "generic");
  if (family === "windows-reinstall") {
    return "xhigh";
  }
  if (["package-install", "driver-check"].includes(family)) {
    return "high";
  }
  if (family === "web-lookup") {
    return "medium";
  }
  if (["program-control", "file-work", "system-check", "service-check", "identity-probe", "script-task", "power-check", "driver-check", "software-check", "audio-volume", "audio-mute"].includes(family)) {
    return "low";
  }
  return "medium";
}

function safeCodexReasoningEffort(value) {
  const effort = String(value || "").trim().toLowerCase();
  return ["low", "medium", "high", "xhigh"].includes(effort) ? effort : "";
}

function safeAgentResponseStyleId(value) {
  const id = String(value || "").trim().toLowerCase().replace(/[^a-z0-9_-]+/gu, "-").replace(/^-+|-+$/gu, "");
  return agentResponseStyleProfiles.some((profile) => profile.id === id) ? id : defaultAgentResponseStyleId;
}

function agentResponseStyleProfile(id = agentResponseStyleId) {
  return agentResponseStyleProfiles.find((profile) => profile.id === id) || agentResponseStyleProfiles[0];
}

function agentResponseStylePromptLines(profile = activeAgentResponseStyle) {
  const rules = Array.isArray(profile?.promptRules) ? profile.promptRules : [];
  const maxLines = Number.isSafeInteger(profile?.maxUserFacingLines) && profile.maxUserFacingLines > 0
    ? `; max_user_facing_lines=${profile.maxUserFacingLines}`
    : "";
  return [
    `- response_style: ${profile.id}; base=${profile.base}; tone=${profile.tone}${maxLines}`,
    `- response_identity: visible chat handle is ${profile.displayName}. Use this name when asked who you are.`,
    ...rules.map((rule, index) => `- response_style_rule_${index + 1}: ${rule}`)
  ];
}

function agentResponseStyleStatus(profile = activeAgentResponseStyle) {
  return {
    schema: "soty.response-style.v1",
    id: profile.id,
    displayName: profile.displayName,
    base: profile.base,
    tone: profile.tone,
    maxUserFacingLines: profile.maxUserFacingLines,
    phraseBank: profile.phraseBank
  };
}

function shouldRetryCodexWithoutResume(result, state) {
  if (!result || result.exitCode === 0) {
    return false;
  }
  if (state?.messages?.length || state?.terminal?.length) {
    return false;
  }
  const details = `${result.stderr || ""}\n${result.stdout || ""}`.toLowerCase();
  return /resume|session|thread|conversation|not found|missing|invalid|no such/u.test(details);
}

function codexSessionKey(source, target = null, taskFamily = "generic") {
  const safe = sanitizeAgentSource(source);
  const targetId = String(target?.id || safe.preferredTargetId || "").trim();
  const key = [
    safe.tunnelId || safe.deviceId || "default",
    targetId,
    `family:${codexSessionFamilyBucket(taskFamily)}`
  ].filter(Boolean).join("@");
  return key.replace(/[^A-Za-z0-9_.:-]/gu, "_").slice(0, 180);
}

function codexSessionFamilyBucket(taskFamily) {
  const family = String(taskFamily || "generic").trim().toLowerCase();
  if (!family || family === "generic") {
    return "dialog";
  }
  if (family === "plain-dialog" || family === "source-scoped-dialog") {
    return family;
  }
  if (family.includes("windows-reinstall")) {
    return "windows-reinstall";
  }
  if (family.includes("audio") || family.includes("volume") || family.includes("mute")) {
    return "audio";
  }
  if (family.includes("browser") || family.includes("pwa")) {
    return "browser";
  }
  if (family.includes("install") || family.includes("repair") || family.includes("lifecycle")) {
    return "lifecycle";
  }
  return family.replace(/[^a-z0-9_.:-]/gu, "_").slice(0, 60) || "dialog";
}

function classifyTaskFamily(text, target = null) {
  const family = classifySourceCommand(text);
  if (family !== "generic") {
    return family;
  }
  return target?.id ? "source-scoped-dialog" : "plain-dialog";
}

function usableCodexSessionRecord(value) {
  if (!value || typeof value !== "object" || value.mode !== codexSessionMode) {
    return null;
  }
  const threadId = safeCodexThreadId(value.threadId);
  if (!threadId) {
    return null;
  }
  const workspaceDir = safeCodexWorkspacePath(value.workspaceDir);
  return {
    threadId,
    workspaceDir: workspaceDir || ""
  };
}

async function prepareCodexWorkspace(sessionKey, sessionRecord = null) {
  const existing = safeCodexWorkspacePath(sessionRecord?.workspaceDir);
  if (existing) {
    await mkdir(existing, { recursive: true });
    return existing;
  }
  const name = codexWorkspaceName(sessionKey);
  const workspace = join(codexWorkspacesDir, name);
  await mkdir(workspace, { recursive: true });
  return workspace;
}

function codexWorkspaceName(sessionKey) {
  const value = String(sessionKey || "default");
  const slug = value.replace(/[^A-Za-z0-9_.-]/gu, "_").replace(/^_+/u, "").slice(0, 72) || "default";
  const digest = createHash("sha256").update(value).digest("hex").slice(0, 12);
  return `${slug}-${digest}`;
}

function safeCodexWorkspacePath(value) {
  const text = String(value || "");
  if (!text) {
    return "";
  }
  const resolved = resolve(text);
  const root = resolve(codexWorkspacesDir);
  return resolved === root || resolved.startsWith(`${root}\\`) || resolved.startsWith(`${root}/`) ? resolved : "";
}

function safeCodexThreadId(value) {
  const text = String(value || "").trim();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(text) ? text : "";
}

function spawnCommand(file, args, options) {
  const needsWindowsShell = process.platform === "win32" && /\.(?:cmd|bat)$/iu.test(String(file || ""));
  if (!needsWindowsShell) {
    return spawn(file, args, options);
  }
  const command = `"${[file, ...args].map(quoteWindowsCommandArg).join(" ")}"`;
  return spawn(process.env.ComSpec || "cmd.exe", ["/d", "/s", "/c", command], {
    ...options,
    windowsVerbatimArguments: true
  });
}

function quoteWindowsCommandArg(value) {
  const text = String(value ?? "");
  return `"${text.replace(/(\\*)"/gu, "$1$1\\\"").replace(/(\\+)$/u, "$1$1")}"`;
}

function runCodexForSotyChat(file, args, env, timeoutMs, input, state, jobDir, onMessage = null, onTerminal = null) {
  return new Promise((resolve, reject) => {
    traceStep(state?.trace, "codex.spawn", {
      file: basename(file || ""),
      cwd: jobDir || process.cwd(),
      timeoutMs,
      inputChars: String(input || "").length
    });
    const child = spawnCommand(file, args, {
      cwd: jobDir || process.cwd(),
      env,
      windowsHide: true,
      stdio: [input ? "pipe" : "ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    let jsonBuffer = "";
    let done = false;
    let sawStartupActivity = false;
    const markStartupActivity = () => {
      if (sawStartupActivity) {
        return;
      }
      sawStartupActivity = true;
      clearTimeout(startupTimer);
    };
    const finish = (exitCode) => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      clearTimeout(startupTimer);
      void traceWriteText(state?.trace, "stdout-tail.txt", stdout, 24_000);
      void traceWriteText(state?.trace, "stderr-tail.txt", stderr, 24_000);
      traceStep(state?.trace, "codex.exit", {
        exitCode: Number.isSafeInteger(exitCode) ? exitCode : 0,
        stdoutChars: stdout.length,
        stderrChars: stderr.length,
        events: state?.trace?.doc?.codex?.eventCount || 0
      });
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
      void traceWriteText(state?.trace, "stdout-tail.txt", stdout, 24_000);
      void traceWriteText(state?.trace, "stderr-tail.txt", stderr, 24_000);
      traceStep(state?.trace, "codex.timeout", {
        timeoutMs,
        stdoutChars: stdout.length,
        stderrChars: stderr.length
      });
      reject(new Error("timeout"));
    }, Math.max(5000, timeoutMs || 120000));
    const startupTimer = setTimeout(() => {
      if (done || sawStartupActivity) {
        return;
      }
      done = true;
      clearTimeout(timer);
      killProcessTree(child);
      traceStep(state?.trace, "codex.startup-timeout", {
        timeoutMs: codexStartupTimeoutMs,
        stdoutChars: stdout.length,
        stderrChars: stderr.length
      });
      reject(new Error("codex cold start timeout"));
    }, Math.max(5000, Math.min(codexStartupTimeoutMs, timeoutMs || codexStartupTimeoutMs)));
    child.stdout.on("data", (chunk) => {
      markStartupActivity();
      const text = chunk.toString("utf8");
      stdout = `${stdout}${text}`.slice(-24_000);
      jsonBuffer = `${jsonBuffer}${text}`;
      const lines = jsonBuffer.split(/\r?\n/u);
      jsonBuffer = lines.pop() || "";
      for (const line of lines) {
        handleCodexJsonLineForSoty(line, state, onMessage, onTerminal);
      }
    });
    child.stderr.on("data", (chunk) => {
      markStartupActivity();
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
      clearTimeout(startupTimer);
      traceStep(state?.trace, "codex.spawn-error", {
        message: error instanceof Error ? error.message : String(error)
      });
      reject(error);
    });
    child.on("close", finish);
  });
}

function handleCodexJsonLineForSoty(line, state, onMessage = null, onTerminal = null) {
  const text = String(line || "").trim();
  if (!text) {
    return;
  }
  let event;
  try {
    event = JSON.parse(text);
  } catch {
    return;
  }
  traceCodexEvent(state?.trace, text, event);
  mergeCodexUsage(state, extractCodexUsage(event));
  const threadId = codexEventThreadId(event);
  if (threadId) {
    state.threadId = threadId;
    return;
  }
  for (const terminalText of extractCodexTerminalTexts(event, state)) {
    const message = cleanTerminalTranscript(terminalText);
    if (!message) {
      continue;
    }
    state.terminal.push(message);
    while (state.terminal.length > maxCodexDialogMessages) {
      state.terminal.shift();
    }
    if (typeof onTerminal === "function") {
      Promise.resolve(onTerminal(message)).catch(() => undefined);
    }
  }
  for (const rawMessage of extractCodexAssistantTexts(event)) {
    pushLearningMarkers(state, extractInternalLearningMarkers(rawMessage));
    const message = cleanAgentChatReply(rawMessage);
    if (!message) {
      continue;
    }
    state.lastMessage = message;
    if (state.messages[state.messages.length - 1] !== message) {
      state.messages.push(message);
      while (state.messages.length > maxCodexDialogMessages) {
        state.messages.shift();
      }
      if (typeof onMessage === "function") {
        Promise.resolve(onMessage(message)).catch(() => undefined);
      }
    }
  }
}

function emptyCodexUsage() {
  return {
    inputTokens: 0,
    outputTokens: 0,
    totalTokens: 0,
    cachedInputTokens: 0,
    actual: false
  };
}

function mergeCodexUsage(state, usage) {
  if (!state || !usage || usage.totalTokens <= 0) {
    return;
  }
  const current = state.usage || emptyCodexUsage();
  state.usage = {
    inputTokens: Math.max(current.inputTokens || 0, usage.inputTokens || 0),
    outputTokens: Math.max(current.outputTokens || 0, usage.outputTokens || 0),
    totalTokens: Math.max(current.totalTokens || 0, usage.totalTokens || 0),
    cachedInputTokens: Math.max(current.cachedInputTokens || 0, usage.cachedInputTokens || 0),
    actual: true
  };
}

function extractCodexUsage(value, depth = 0) {
  if (!value || typeof value !== "object" || depth > 4) {
    return emptyCodexUsage();
  }
  const usage = usageFromRecord(value);
  if (usage.totalTokens > 0) {
    return usage;
  }
  const keys = ["usage", "token_usage", "usage_metadata", "response", "payload", "event", "data"];
  for (const key of keys) {
    const nested = value[key];
    if (nested && typeof nested === "object") {
      const found = extractCodexUsage(nested, depth + 1);
      if (found.totalTokens > 0) {
        return found;
      }
    }
  }
  if (Array.isArray(value)) {
    for (const item of value.slice(0, 8)) {
      const found = extractCodexUsage(item, depth + 1);
      if (found.totalTokens > 0) {
        return found;
      }
    }
  }
  return emptyCodexUsage();
}

function usageFromRecord(record) {
  const inputTokens = firstSafeInteger(record, [
    "input_tokens",
    "prompt_tokens",
    "inputTokens",
    "promptTokens"
  ]);
  const outputTokens = firstSafeInteger(record, [
    "output_tokens",
    "completion_tokens",
    "outputTokens",
    "completionTokens"
  ]);
  const cachedInputTokens = firstSafeInteger(record, [
    "cached_input_tokens",
    "cached_tokens",
    "cachedInputTokens",
    "cachedTokens"
  ]);
  const totalFromRecord = firstSafeInteger(record, [
    "total_tokens",
    "totalTokens"
  ]);
  const totalTokens = totalFromRecord || inputTokens + outputTokens;
  return {
    inputTokens,
    outputTokens,
    totalTokens,
    cachedInputTokens,
    actual: totalTokens > 0
  };
}

function firstSafeInteger(record, keys) {
  for (const key of keys) {
    const value = Number(record?.[key]);
    if (Number.isSafeInteger(value) && value > 0) {
      return Math.min(10_000_000, value);
    }
  }
  return 0;
}

function codexUsageProof(usage, prompt, finalText) {
  if (usage?.actual && usage.totalTokens > 0) {
    return [
      "tokens=actual",
      `input=${usage.inputTokens || 0}`,
      `output=${usage.outputTokens || 0}`,
      `total=${usage.totalTokens || 0}`,
      `cached=${usage.cachedInputTokens || 0}`
    ].join("; ");
  }
  const input = estimateTokenCount(prompt);
  const output = estimateTokenCount(finalText);
  return `tokens=estimated; input=${input}; output=${output}; total=${input + output}; cached=0`;
}

function estimateTokenCount(text) {
  const normalized = String(text || "").trim();
  if (!normalized) {
    return 0;
  }
  return Math.max(1, Math.ceil(normalized.length / 4));
}

function pushLearningMarkers(state, markers) {
  if (!state || !Array.isArray(markers) || markers.length === 0) {
    return;
  }
  const target = Array.isArray(state.learningMarkers) ? state.learningMarkers : [];
  for (const marker of markers) {
    const clean = cleanInternalLearningMarker(marker);
    if (clean && !target.includes(clean)) {
      target.push(clean);
    }
  }
  state.learningMarkers = target.slice(-maxLearningMarkersPerTurn);
}

function extractInternalLearningMarkers(value) {
  return String(value || "")
    .replace(/\r\n?/gu, "\n")
    .split("\n")
    .map((line) => cleanInternalLearningMarker(line))
    .filter(Boolean);
}

function cleanInternalLearningMarker(value) {
  const text = String(value || "").trim().replace(/^`|`$/gu, "");
  if (/^ops-memory\s*:/iu.test(text)) {
    return `soty-memory:${text.replace(/^ops-memory\s*:/iu, "")}`.slice(0, 900);
  }
  if (/^soty-memory\s*:/iu.test(text)) {
    return text.slice(0, 900);
  }
  return "";
}

function recordAgentLearningMarkers(markers, context = {}) {
  const unique = [...new Set((markers || []).map(cleanInternalLearningMarker).filter(Boolean))]
    .slice(-maxLearningMarkersPerTurn);
  for (const marker of unique) {
    recordLearningReceipt({
      kind: "agent-runtime",
      family: "dialog-memory",
      result: "ok",
      route: context.route || "codex.exec.resume",
      taskSig: context.taskSig || "",
      proof: marker,
      exitCode: 0,
      ...(Number.isSafeInteger(context.durationMs) ? { durationMs: context.durationMs } : {}),
      ...cleanLearningContext(context)
    });
  }
}

function extractCodexTerminalTexts(event, state) {
  const type = String(event?.type || "");
  const payload = event?.payload && typeof event.payload === "object" ? event.payload : null;
  if ((type === "event_msg" || type === "response_item") && payload) {
    return extractCodexTerminalTexts(payload, state);
  }
  const item = event?.item && typeof event.item === "object" ? event.item : null;
  if (!item || item.type !== "command_execution") {
    return [];
  }
  const id = String(item.id || "");
  const command = cleanTerminalTranscript(item.command || "");
  const output = cleanTerminalTranscript(item.aggregated_output || "");
  const keyBase = id || createHash("sha256").update(`${command}\n${output}`).digest("hex").slice(0, 16);
  const terminalKeys = state.terminalKeys instanceof Set ? state.terminalKeys : new Set();
  state.terminalKeys = terminalKeys;
  const lines = [];
  if (type === "item.started" && command) {
    const key = `${keyBase}:started`;
    if (!terminalKeys.has(key)) {
      terminalKeys.add(key);
      lines.push(`$ ${command}`);
    }
  }
  if (type === "item.completed") {
    const startKey = `${keyBase}:started`;
    if (command && !terminalKeys.has(startKey)) {
      terminalKeys.add(startKey);
      lines.push(`$ ${command}`);
    }
    const outputKey = `${keyBase}:output`;
    if (output && !terminalKeys.has(outputKey)) {
      terminalKeys.add(outputKey);
      lines.push(output);
    }
    const exitCode = Number.isSafeInteger(item.exit_code) ? item.exit_code : 0;
    const exitKey = `${keyBase}:exit`;
    if (exitCode !== 0 && !terminalKeys.has(exitKey)) {
      terminalKeys.add(exitKey);
      lines.push(`! ${exitCode}`);
    }
  }
  return lines;
}

function extractCodexAssistantTexts(event) {
  const type = String(event?.type || "");
  const payload = event?.payload && typeof event.payload === "object" ? event.payload : null;
  if ((type === "event_msg" || type === "response_item") && payload) {
    return extractCodexAssistantTexts(payload);
  }
  const item = event?.item && typeof event.item === "object" ? event.item : null;
  const messages = [];
  if (type === "item.completed" && item?.type === "agent_message") {
    messages.push(extractCodexText(item.text ?? item.content));
  }
  if (type === "item.completed" && item?.type === "message" && item.role === "assistant") {
    messages.push(extractCodexText(item.content ?? item.text));
  }
  if ((type === "agent_message" || type === "assistant_message") && (event.text || event.message)) {
    messages.push(extractCodexText(event.text ?? event.message));
  }
  const message = event?.message && typeof event.message === "object" ? event.message : null;
  if (message?.role === "assistant") {
    messages.push(extractCodexText(message.content ?? message.text));
  }
  if ((type === "response.completed" || type === "turn.completed") && event.last_message) {
    messages.push(extractCodexText(event.last_message));
  }
  if (type === "task_complete" && event.last_agent_message) {
    messages.push(extractCodexText(event.last_agent_message));
  }
  return messages.filter(Boolean);
}

function codexEventThreadId(event) {
  const type = String(event?.type || "");
  const direct = typeof event?.thread_id === "string" ? event.thread_id : "";
  if (direct) {
    return direct;
  }
  const payload = event?.payload && typeof event.payload === "object" ? event.payload : null;
  if (typeof payload?.thread_id === "string") {
    return payload.thread_id;
  }
  if (type === "session_meta" && typeof payload?.id === "string") {
    return payload.id;
  }
  return "";
}

function extractCodexText(value) {
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => extractCodexText(item)).filter(Boolean).join("\n");
  }
  if (value && typeof value === "object") {
    if (typeof value.text === "string") {
      return value.text;
    }
    if (typeof value.output_text === "string") {
      return value.output_text;
    }
    if (typeof value.content === "string" || Array.isArray(value.content)) {
      return extractCodexText(value.content);
    }
  }
  return "";
}

function compactCodexMessages(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => cleanAgentChatReply(item))
    .filter(Boolean)
    .slice(-maxCodexDialogMessages);
}

function compactTerminalMessages(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => cleanTerminalTranscript(item))
    .filter(Boolean)
    .slice(-maxCodexDialogMessages);
}

function resolveAgentBridgeTarget(source, text = "", sourceTargets = []) {
  const safe = sanitizeAgentSource(source);
  if (isPlainNonDeviceTask(text)) {
    return null;
  }
  const preferred = preferredOperatorTarget(safe);
  const mentionedTarget = targetMentionedAtStart(text, runtimeActiveTargets(safe, preferred, sourceTargets));
  if (mentionedTarget) {
    return mentionedTarget;
  }
  if (preferred) {
    return preferred;
  }
  const implicitTarget = implicitOperatorTargetForRequest(safe, text, sourceTargets);
  if (implicitTarget) {
    return implicitTarget;
  }
  const [linked] = sourceAgentLinkTargets(safe, sourceTargets);
  if (linked) {
    return linked;
  }
  if (safe.deviceId) {
    return sourceDeviceFallbackTarget(safe);
  }
  return null;
}

function implicitOperatorTargetForRequest(source, text = "", sourceTargets = []) {
  const safe = sanitizeAgentSource(source);
  if (classifySourceCommand(text) !== "windows-reinstall") {
    return null;
  }
  const candidates = runtimeActiveTargets(safe, null, sourceTargets)
    .filter((target) => target.access === true && !isAgentSourceTarget(target.id));
  const selected = candidates.filter((target) => target.selected === true);
  if (selected.length === 1) {
    return selected[0];
  }
  return candidates.length === 1 ? candidates[0] : null;
}

function sourceDeviceFallbackTarget(source) {
  const safe = sanitizeAgentSource(source);
  if (!safe.deviceId) {
    return null;
  }
  return {
    id: `agent-source:${safe.deviceId}`,
    label: safe.deviceNick || "source device",
    deviceIds: [safe.deviceId],
    hostDeviceId: safe.deviceId,
    access: true,
    host: true
  };
}

async function activeAgentSourceTargets(relayId = "") {
  const relayBaseUrl = agentRelayBaseUrl || originFromUrl(updateManifestUrl);
  const sourceRelayId = safeRelayId(relayId) || agentRelayId;
  if (!relayBaseUrl || !sourceRelayId) {
    return [];
  }
  try {
    const url = new URL("/api/agent/source/targets", relayBaseUrl);
    url.searchParams.set("relayId", sourceRelayId);
    const response = await fetch(url, { cache: "no-store" });
    const payload = await response.json();
    if (!response.ok || !payload?.ok) {
      return [];
    }
    return sanitizeTargets(payload.targets)
      .sort((left, right) => targetLastActionMs(right) - targetLastActionMs(left));
  } catch {
    return [];
  }
}

function targetLastActionMs(target) {
  const time = Date.parse(target?.lastActionAt || "");
  return Number.isFinite(time) ? time : 0;
}

function preferredOperatorTarget(source) {
  const safe = sanitizeAgentSource(source);
  const allTargets = sanitizeTargets(safe.operatorTargets);
  const accessTargets = allTargets.filter((target) => target.access === true);
  const preferredId = String(safe.preferredTargetId || "").trim().toLowerCase();
  if (preferredId) {
    const byId = allTargets.find((target) => target.id === safe.preferredTargetId || target.id.toLowerCase() === preferredId);
    if (byId) {
      return byId;
    }
  }
  const preferredLabel = String(safe.preferredTargetLabel || "").trim().toLowerCase();
  if (preferredLabel) {
    const byLabel = allTargets.find((target) => target.label.toLowerCase() === preferredLabel);
    if (byLabel) {
      return byLabel;
    }
  }
  return accessTargets.length === 1 ? accessTargets[0] : null;
}

function targetMentionedAtStart(text, targets) {
  const match = /^([^:\n]{1,80})\s*:/u.exec(String(text || "").trim());
  if (!match) {
    return null;
  }
  const needle = cleanTargetNeedle(match[1]);
  if (!needle) {
    return null;
  }
  const sorted = sanitizeTargets(targets);
  return sorted.find((target) => cleanTargetNeedle(target.label) === needle)
    || sorted.find((target) => target.id.toLowerCase() === needle)
    || sorted.find((target) => cleanTargetNeedle(target.label).includes(needle))
    || null;
}

function cleanTargetNeedle(value) {
  return String(value || "").replace(/\s+/gu, " ").trim().toLowerCase();
}

function bridgeSourceDeviceId(target, source) {
  if (!target) {
    return "";
  }
  if (isAgentSourceTarget(target.id)) {
    return agentSourceDeviceId(target.id) || sanitizeAgentSource(source).deviceId || "";
  }
  if (target.hostDeviceId) {
    return target.hostDeviceId;
  }
  if (Array.isArray(target.deviceIds) && target.deviceIds.length === 1) {
    return target.deviceIds[0] || "";
  }
  return sanitizeAgentSource(source).deviceId || "";
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

function cleanTerminalTranscript(value) {
  return String(value || "")
    .replace(/\r\n?/gu, "\n")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu, "")
    .replace(/\n{5,}/gu, "\n\n\n\n")
    .trim()
    .slice(0, maxChatChars);
}

function isInternalAgentReceiptLine(line) {
  const text = internalAgentReceiptText(line);
  return /^`?(learning_delta|proof|final_line|finish_skill_edit)\s*=/iu.test(text)
    || /^`?ops-memory\s*:/iu.test(text)
    || /^`?soty-memory\s*:/iu.test(text)
    || /^ops:\s*`?(learning_delta|proof|final_line)\s*=/iu.test(text);
}

function internalAgentReceiptText(line) {
  return String(line || "")
    .trim()
    .replace(/^(?:>\s*)+/u, "")
    .replace(/^(?:[-*]\s*)+/u, "")
    .replace(/^`{1,3}\s*/u, "")
    .replace(/\s*`{1,3}$/u, "")
    .trim();
}

async function askCodexRelayFallback(text, context, source = {}, onMessage = null, onTerminal = null, options = {}) {
  const relayBaseUrl = agentRelayBaseUrl || originFromUrl(updateManifestUrl);
  if (!relayBaseUrl) {
    return null;
  }
  const requestRelayId = agentRelayId;
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
        context: String(context || "").slice(-maxAgentContextChars),
        source: sanitizeAgentSource(source),
        ...(options?.preferServer === true ? { preferServer: true } : {})
      })
    });
    const created = await request.json();
    if (!request.ok || !created?.ok || !isSafeText(created.id, 160)) {
      return null;
    }
    const replyRelayId = safeRelayId(created.relayId || requestRelayId);
    let stopEvents = false;
    const eventStream = typeof onMessage === "function" || typeof onTerminal === "function"
      ? watchCodexRelayFallbackEvents(relayBaseUrl, replyRelayId, created.id, agentReplyTimeoutMs, onMessage, onTerminal, () => stopEvents)
      : Promise.resolve();
    const reply = await waitForCodexRelayFallbackReply(relayBaseUrl, replyRelayId, created.id, agentReplyTimeoutMs);
    stopEvents = true;
    void eventStream.catch(() => undefined);
    return reply;
  } catch {
    return null;
  }
}

async function watchCodexRelayFallbackEvents(relayBaseUrl, relayId, id, timeoutMs, onMessage, onTerminal, stopped) {
  const deadline = Date.now() + Math.max(5000, timeoutMs || 120000);
  let after = 0;
  while (!stopped() && Date.now() < deadline) {
    const url = new URL("/api/agent/relay/events", relayBaseUrl);
    url.searchParams.set("relayId", relayId);
    url.searchParams.set("id", id);
    url.searchParams.set("after", String(after));
    url.searchParams.set("wait", "1");
    try {
      const response = await fetch(url, { cache: "no-store" });
      if (!response.ok) {
        return;
      }
      if (stopped()) {
        return;
      }
      const payload = await response.json();
      for (const event of Array.isArray(payload?.events) ? payload.events : []) {
        const seq = Number.isSafeInteger(event?.seq) ? event.seq : 0;
        if (seq <= after) {
          continue;
        }
        after = seq;
        const type = String(event?.type || "agent_message");
        if (type === "agent_terminal") {
          const text = cleanTerminalTranscript(event?.text || "");
          if (text && typeof onTerminal === "function") {
            await Promise.resolve(onTerminal(text)).catch(() => undefined);
          }
          continue;
        }
        const text = cleanAgentChatReply(event?.text || "");
        if (text && typeof onMessage === "function") {
          await Promise.resolve(onMessage(text)).catch(() => undefined);
        }
      }
      if (payload?.done) {
        return;
      }
    } catch {
      await sleep(1000);
    }
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
        const messages = compactCodexMessages(payload.reply.messages);
        return {
          ok: Boolean(payload.reply.ok),
          text: cleanAgentChatReply(payload.reply.text || "").slice(0, maxChatChars),
          ...(messages.length > 0 ? { messages } : {}),
          ...(Array.isArray(payload.reply.terminal) ? { terminal: compactTerminalMessages(payload.reply.terminal) } : {}),
          ...(Number.isSafeInteger(payload.reply.exitCode) ? { exitCode: payload.reply.exitCode } : {})
        };
      }
    } catch {
      // Keep waiting until the outer timeout; transient network switches are common on remote devices.
    }
  }
  return null;
}

async function buildAgentRuntimeContext({ text, context = "", source = {}, target = null, sourceTargets = [], sessionRecord = null, jobDir = "" }) {
  const safeSource = sanitizeAgentSource(source);
  const taskFamily = classifyTaskFamily(text, target);
  const routineTask = isRoutineAgentTaskFamily(taskFamily);
  const sourceDeviceId = promptInline(bridgeSourceDeviceId(target, safeSource) || safeSource.deviceId || "");
  const targetLabel = promptInline(target?.label || safeSource.preferredTargetLabel || "");
  const targetId = promptInline(target?.id || safeSource.preferredTargetId || "");
  const activeTargets = runtimeActiveTargets(safeSource, target, sourceTargets)
    .slice(0, 8)
    .map((item) => `${promptInline(item.label)} (${promptInline(item.id)})${item.access ? " access=true" : ""}`)
    .join("\n");
  return {
    taskFamily,
    userText: String(text || "").trim().slice(0, maxChatChars),
    visibleContext: cleanPromptBlock(context, routineTask ? 3000 : maxAgentContextChars),
    source: {
      tunnelId: promptInline(safeSource.tunnelId),
      tunnelLabel: promptInline(safeSource.tunnelLabel),
      deviceId: promptInline(safeSource.deviceId),
      deviceNick: promptInline(safeSource.deviceNick),
      appOrigin: promptInline(safeSource.appOrigin)
    },
    target: {
      id: targetId,
      label: targetLabel,
      sourceDeviceId
    },
    activeTargets,
    session: {
      resumed: Boolean(sessionRecord?.threadId),
      threadId: safeCodexThreadId(sessionRecord?.threadId || ""),
      mode: codexSessionMode,
      workspaceDir: promptInline(jobDir)
    },
    memory: (await codexLearningMemoryPrompt()).slice(0, routineTask ? 1400 : 4000)
  };
}

function runtimeActiveTargets(source, target = null, sourceTargets = []) {
  const safe = sanitizeAgentSource(source);
  const preferredId = String(target?.id || safe.preferredTargetId || "");
  const merged = new Map();
  const add = (item) => {
    if (item?.id) {
      merged.set(item.id, item);
    }
  };
  for (const item of sanitizeTargets(sourceTargets)) {
    add(item);
  }
  for (const item of sanitizeTargets(safe.operatorTargets)) {
    add(item);
  }
  for (const item of sanitizeTargets(target ? [target] : [])) {
    add(item);
  }
  return [...merged.values()]
    .sort((left, right) => runtimeTargetScore(right, preferredId) - runtimeTargetScore(left, preferredId));
}

function runtimeTargetScore(target, preferredId) {
  let score = targetLastActionMs(target);
  if (target.id === preferredId) {
    score += 1_000_000_000_000_000;
  }
  if (target.access === true) {
    score += 1_000_000_000_000;
  }
  if (target.selected === true) {
    score += 1_000_000_000;
  }
  return score;
}

function sotyRuntimeHints() {
  return [
    "- Identity: Лорд.",
    "- Use memory as short reusable hints, not as rules.",
    "- Use capability tools for the user's computer; verify important actions with proof.",
    "- Keep answers brief unless the task needs detail. Hidden memory line: `soty-memory:`."
  ];
}

async function writeCodexRuntimeFiles(jobDir, runtimeContext) {
  if (!jobDir) {
    return;
  }
  await mkdir(jobDir, { recursive: true });
  const agents = [
    "# Soty Runtime",
    "",
    "Generated Soty workspace. It is not automatically the user's project checkout.",
    "",
    "Operating model:",
    ...sotyRuntimeHints(),
    ...agentResponseStylePromptLines(activeAgentResponseStyle),
    "",
    "Useful local files:",
    "- SOTY_CONTEXT.md contains the last runtime packet and sanitized shared-text context for this turn."
  ].join("\n");
  const context = [
    "# Soty Runtime Packet",
    "",
    `session_mode: ${runtimeContext.session.mode}`,
    `session_resumed: ${runtimeContext.session.resumed ? "true" : "false"}`,
    `source_device: ${runtimeContext.source.deviceNick || "unknown"} (${runtimeContext.source.deviceId || "no-id"})`,
    `target: ${runtimeContext.target.label || "none"} (${runtimeContext.target.id || "none"})`,
    `target_source_device_id: ${runtimeContext.target.sourceDeviceId || "none"}`,
    `response_style: ${activeAgentResponseStyle.id} (${activeAgentResponseStyle.displayName})`,
    "",
    "## Memory Plane Hints",
    runtimeContext.memory || "unavailable",
    "",
    "## Active Soty Targets",
    runtimeContext.activeTargets || "none",
    "",
    "## Visible Soty Shared Text Context",
    runtimeContext.visibleContext || "none",
    "",
    "## Current User Request",
    runtimeContext.userText || "none"
  ].join("\n").slice(0, maxAgentRuntimePromptChars);
  await writeFile(join(jobDir, "AGENTS.md"), `${agents}\n`, "utf8");
  await writeFile(join(jobDir, "SOTY_CONTEXT.md"), `${context}\n`, "utf8");
}

function buildAgentPrompt(text, context = "", runtimeContext = null) {
  const body = String(text || "").trim();
  const runtime = runtimeContext || {
    source: {},
    target: {},
    session: { resumed: false, mode: codexSessionMode },
    activeTargets: "",
    visibleContext: cleanPromptBlock(context, maxAgentContextChars),
    memory: "",
    taskFamily: classifyTaskFamily(body, null)
  };
  const lines = [
    "Current user request (authoritative):",
    body || "(empty)",
    "",
    "Soty runtime packet:",
    `- session_mode: ${runtime.session?.mode || codexSessionMode}`,
    `- session_resumed: ${runtime.session?.resumed ? "true" : "false"}`,
    `- source_device: ${runtime.source?.deviceNick || "unknown"} (${runtime.source?.deviceId || "no-id"})`,
    `- target: ${runtime.target?.label || "none"} (${runtime.target?.id || "none"})`,
    `- target_source_device_id: ${runtime.target?.sourceDeviceId || "none"}`,
    ...sotyRuntimeHints(),
    ...agentResponseStylePromptLines(activeAgentResponseStyle),
    "",
    "Memory plane hints:",
    runtime.memory || "unavailable",
    "",
    "Active Soty targets:",
    runtime.activeTargets || "none",
    "",
    "Visible Soty shared-text context:",
    runtime.visibleContext || cleanPromptBlock(context, maxAgentContextChars) || "none",
    "",
    "User message to satisfy now:",
    body || "(empty)",
    "",
    "Use the user message above as the task. Treat service context and memory hints as supporting material only."
  ];
  return lines.join("\n").slice(0, maxAgentRuntimePromptChars);
}

async function codexLearningMemoryPrompt() {
  const now = Date.now();
  if (cachedCodexLearningMemoryText && now - cachedCodexLearningMemoryAt < 5 * 60_000) {
    return cachedCodexLearningMemoryText;
  }
  if (!agentRelayBaseUrl) {
    cachedCodexLearningMemoryAt = now;
    cachedCodexLearningMemoryText = "memory plane unavailable: relay is not configured";
    return cachedCodexLearningMemoryText;
  }
  const report = await Promise.race([
    fetchLearningTeacherReport(500).catch((error) => ({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    })),
    sleep(2500).then(() => ({ ok: false, error: "memory timeout" }))
  ]);
  cachedCodexLearningMemoryAt = now;
  cachedCodexLearningMemoryText = formatCodexLearningMemory(report).slice(0, 4000);
  return cachedCodexLearningMemoryText;
}

function formatCodexLearningMemory(report) {
  if (!report?.ok) {
    return `memory plane unavailable: ${cleanLearningText(report?.error || "unknown", 160)}`;
  }
  const lines = [
    `memory=${report.schema || "soty.memory.query.v1"} receipts=${Number(report.receipts || 0)} source=${cleanLearningText(report.source || "", 80)}`,
    `scope=${formatLearningScope(report)}`,
    `publish=${formatLearningPublishModel(report)}`
  ];
  const recommendations = Array.isArray(report.recommendations)
    ? report.recommendations.slice(0, 4)
    : Array.isArray(report.items)
      ? report.items.slice(0, 4)
      : [];
  if (recommendations.length > 0) {
    lines.push("recommendations:");
    for (const item of recommendations) {
      lines.push(`- ${cleanLearningText(item.priority || "normal", 20)} ${cleanLearningText(item.family || "generic", 80)}: ${cleanLearningText(item.title || item.action || "review route", 220)}`);
    }
  }
  const candidates = Array.isArray(report.candidates) ? report.candidates.slice(0, 4) : [];
  if (candidates.length > 0) {
    lines.push("candidate memory:");
    for (const item of candidates) {
      lines.push(`- ${cleanLearningText(item.scope || "candidate", 40)} ${cleanLearningText(item.family || "generic", 80)}: ${cleanLearningText(item.marker || "", 260)}`);
    }
  }
  return lines.join("\n");
}

function cleanPromptBlock(value, max = maxAgentContextChars) {
  return String(value || "")
    .replace(/\r\n?/gu, "\n")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu, "")
    .replace(/\n{6,}/gu, "\n\n\n")
    .trim()
    .slice(-Math.max(0, max));
}

function promptInline(value) {
  return String(value || "").replace(/\s+/gu, " ").trim().slice(0, maxSourceChars);
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

async function postLocalOperatorScript(target, sourceDeviceId, payload, timeoutMs) {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/operator/script`, {
      method: "POST",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://xn--n1afe0b.online"
      },
      body: JSON.stringify({
        target,
        sourceDeviceId,
        script: String(payload?.script || ""),
        name: String(payload?.name || "script"),
        shell: String(payload?.shell || ""),
        timeoutMs
      })
    });
    const data = await response.json();
    return {
      ok: Boolean(response.ok && data?.ok),
      text: String(data?.text || ""),
      ...(Number.isSafeInteger(data?.exitCode) ? { exitCode: data.exitCode } : { exitCode: response.status || 1 })
    };
  } catch (error) {
    return {
      ok: false,
      text: error instanceof Error ? error.message : String(error),
      exitCode: 1
    };
  }
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
    sourceRelayId: safeRelayId(value.sourceRelayId),
    preferredTargetId: clean(value.preferredTargetId),
    preferredTargetLabel: clean(value.preferredTargetLabel),
    localAgentDirect: value.localAgentDirect === true,
    operatorTargets: sanitizeTargets(value.operatorTargets)
  };
}

function sourceMatchedOperatorTargets(source, extraTargets = []) {
  const safe = sanitizeAgentSource(source);
  const sourceDeviceId = safe.deviceId;
  const merged = new Map();
  for (const target of sanitizeTargets(source?.operatorTargets)) {
    merged.set(target.id, target);
  }
  for (const target of operatorTargets) {
    merged.set(target.id, target);
  }
  for (const target of sanitizeTargets(extraTargets)) {
    merged.set(target.id, target);
  }
  return [...merged.values()]
    .filter((target) => targetMatchesSourceDevice(target, sourceDeviceId))
    .filter((target) => target.access === true)
    .sort((left, right) => operatorSourceTargetScore(right, sourceDeviceId) - operatorSourceTargetScore(left, sourceDeviceId));
}

function sourceAgentLinkTargets(source, extraTargets = []) {
  const matches = sourceMatchedOperatorTargets(source, extraTargets);
  const synthetic = matches.filter((target) => isAgentSourceTarget(target.id));
  if (synthetic.length > 0) {
    return synthetic;
  }
  return matches.length === 1 ? matches : [];
}

function targetMatchesSourceDevice(target, sourceDeviceId) {
  const sourceId = String(sourceDeviceId || "").trim();
  if (!sourceId) {
    return false;
  }
  return target.hostDeviceId === sourceId || target.deviceIds.includes(sourceId);
}

function shouldUseCodexRelayFallback(reply) {
  if (!codexRelayFallback || !reply || reply.ok) {
    return false;
  }
  return /! codex-cli:\s*(?:not found|missing auth|OpenAI\/ChatGPT transport rejected|local Codex did not start in time)/iu.test(String(reply.text || ""));
}

function agentFailureText(details) {
  const value = String(details || "");
  let reason = "no final assistant message";
  if (value.includes("Missing environment variable")) {
    reason = "missing auth or API key";
  } else if (value.includes("403 Forbidden") || value.includes("Unable to load site")) {
    reason = "OpenAI/ChatGPT transport rejected the Codex CLI request";
  } else if (value.includes("cold start")) {
    reason = "local Codex did not start in time";
  } else if (value.includes("timeout")) {
    reason = "timeout waiting for Codex CLI";
  }
  return `! codex-cli: ${reason}`;
}

async function preparePersistentStockCodexHome() {
  const target = join(agentDir, "codex-stock-home");
  await mkdir(target, { recursive: true });
  const authHome = chooseCodexAuthHome();
  for (const file of ["auth.json", "cap_sid", "installation_id", "version.json"]) {
    const source = authHome ? join(authHome, file) : "";
    if (source && existsSync(source)) {
      await copyFile(source, join(target, file)).catch(() => undefined);
    }
  }
  return target;
}

function chooseCodexAuthHome() {
  const explicit = process.env.CODEX_HOME || "";
  if (explicit && existsSync(explicit)) {
    return explicit;
  }
  const home = join(homedir(), ".codex");
  return existsSync(home) ? home : "";
}

function codexNetworkProxyEnv() {
  if (!codexProxyUrl) {
    return {};
  }
  const noProxy = mergedNoProxy(process.env.NO_PROXY || process.env.no_proxy || "");
  return {
    HTTPS_PROXY: codexProxyUrl,
    HTTP_PROXY: codexProxyUrl,
    ALL_PROXY: codexProxyUrl,
    https_proxy: codexProxyUrl,
    http_proxy: codexProxyUrl,
    all_proxy: codexProxyUrl,
    NO_PROXY: noProxy,
    no_proxy: noProxy
  };
}

function safeProxyUrl(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  try {
    const parsed = new URL(text);
    return ["http:", "https:", "socks5:", "socks5h:"].includes(parsed.protocol) ? text : "";
  } catch {
    return "";
  }
}

function proxyScheme(value) {
  if (!value) {
    return "";
  }
  try {
    return new URL(value).protocol.replace(/:$/u, "");
  } catch {
    return "";
  }
}

function mergedNoProxy(value) {
  const parts = new Set(String(value || "")
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean));
  for (const host of ["127.0.0.1", "localhost", "::1"]) {
    parts.add(host);
  }
  return Array.from(parts).join(",");
}

function findCodexBinary() {
  if (codexDisabled) {
    return "";
  }
  return stockCodexPathCandidates().find((candidate) => candidate && existsSync(candidate)) || "";
}

function hasCodexBinary() {
  if (codexDisabled) {
    cachedCodexProbeAt = Date.now();
    cachedCodexAvailable = false;
    return false;
  }
  const now = Date.now();
  if (now - cachedCodexProbeAt < 30_000) {
    return cachedCodexAvailable;
  }
  cachedCodexProbeAt = now;
  cachedCodexAvailable = Boolean(findCodexBinary() && hasCodexAuth());
  return cachedCodexAvailable;
}

function hasCodexAuth() {
  if (process.env.OPENAI_API_KEY || process.env.CODEX_API_KEY) {
    return true;
  }
  const home = chooseCodexAuthHome();
  return Boolean(home && (
    existsSync(join(home, "auth.json"))
    || existsSync(join(home, "cap_sid"))
  ));
}

function stockCodexPathCandidates() {
  const names = process.platform === "win32"
    ? ["codex.cmd", "codex.exe", "codex.bat", "codex"]
    : ["codex"];
  const dirs = new Set((process.env.PATH || "")
    .split(process.platform === "win32" ? ";" : ":")
    .filter(Boolean));
  dirs.add(dirname(process.execPath || ""));
  if (process.platform === "win32" && process.env.APPDATA) {
    dirs.add(join(process.env.APPDATA, "npm"));
  }
  const candidates = [];
  for (const dir of dirs) {
    for (const name of names) {
      candidates.push(join(dir, name));
    }
  }
  return candidates;
}

function withAgentToolPath(env) {
  const next = { ...env };
  const key = pathEnvKey(next);
  next[key] = prependPathEntries(String(next[key] || ""), agentToolPathEntries());
  return next;
}

function pathEnvKey(env) {
  return Object.keys(env).find((key) => key.toLowerCase() === "path") || "PATH";
}

function prependPathEntries(currentPath, entries) {
  const delimiter = process.platform === "win32" ? ";" : ":";
  const existing = String(currentPath || "")
    .split(delimiter)
    .filter(Boolean);
  const seen = new Set(existing.map((entry) => entry.toLowerCase()));
  const prefix = entries.filter((entry) => entry && !seen.has(entry.toLowerCase()));
  return [...prefix, ...existing].join(delimiter);
}

function agentToolPathEntries() {
  return process.platform === "win32"
    ? [agentDir]
    : [agentDir, "/usr/local/bin"];
}

async function ensureCtlLauncher() {
  try {
    if (process.platform === "win32") {
      const ctlPath = join(agentDir, "sotyctl.cmd");
      const ctlPs1Path = join(agentDir, "sotyctl.ps1");
      await writeFile(ctlPath, `@echo off\r\nchcp 65001 >nul\r\n"${process.execPath}" "${scriptPath}" ctl %*\r\n`, "utf8");
      await writeFile(
        ctlPs1Path,
        `$OutputEncoding = [System.Text.UTF8Encoding]::new($false)\r\n& "${process.execPath}" "${scriptPath}" ctl @args\r\nexit $LASTEXITCODE\r\n`,
        "utf8"
      );
      return;
    }
    const launcher = `#!/bin/sh\nexec ${quoteSh(process.execPath)} ${quoteSh(scriptPath)} ctl "$@"\n`;
    const localPath = join(agentDir, "sotyctl");
    await writeFile(localPath, launcher, { encoding: "utf8", mode: 0o755 });
    await chmod(localPath, 0o755).catch(() => undefined);
    await writeFile("/usr/local/bin/sotyctl", launcher, { encoding: "utf8", mode: 0o755 }).catch(() => undefined);
    await chmod("/usr/local/bin/sotyctl", 0o755).catch(() => undefined);
  } catch {
    // PATH launchers are convenience only; direct node /agent/soty-agent.mjs ctl must still work.
  }
}

function quoteSh(value) {
  return `'${String(value).replace(/'/gu, "'\"'\"'")}'`;
}

function runMcpServer() {
  const mcpTarget = arg("--target") || process.env.SOTY_MCP_TARGET || "";
  const mcpSourceDeviceId = arg("--source-device") || process.env.SOTY_MCP_SOURCE_DEVICE || "";
  const mcpSourceRelayId = safeRelayId(arg("--source-relay") || process.env.SOTY_MCP_SOURCE_RELAY || "");
  let mcpPostArmReboot = null;
  let mcpBuffer = Buffer.alloc(0);
  process.stdin.on("data", (chunk) => {
    mcpBuffer = Buffer.concat([mcpBuffer, chunk]);
    drainMcpMessages();
  });

  function drainMcpMessages() {
    while (true) {
      if (mcpBuffer[0] === 123) {
        const newline = mcpBuffer.indexOf("\n");
        if (newline < 0) {
          return;
        }
        const rawLine = mcpBuffer.slice(0, newline).toString("utf8").trim();
        mcpBuffer = mcpBuffer.slice(newline + 1);
        if (rawLine) {
          void handleMcpRawMessage(rawLine);
        }
        continue;
      }
      const headerEnd = mcpBuffer.indexOf("\r\n\r\n");
      if (headerEnd < 0) {
        return;
      }
      const header = mcpBuffer.slice(0, headerEnd).toString("utf8");
      const match = header.match(/content-length:\s*(\d+)/iu);
      if (!match) {
        mcpBuffer = mcpBuffer.slice(headerEnd + 4);
        continue;
      }
      const length = Number.parseInt(match[1] || "0", 10);
      const start = headerEnd + 4;
      const end = start + length;
      if (mcpBuffer.length < end) {
        return;
      }
      const raw = mcpBuffer.slice(start, end).toString("utf8");
      mcpBuffer = mcpBuffer.slice(end);
      void handleMcpRawMessage(raw);
    }
  }

  async function handleMcpRawMessage(raw) {
    let message;
    try {
      message = JSON.parse(raw);
    } catch {
      return;
    }
    if (!message || typeof message !== "object" || !("id" in message)) {
      return;
    }
    try {
      const result = await handleMcpRequest(message);
      sendMcp({ jsonrpc: "2.0", id: message.id, result });
    } catch (error) {
      sendMcp({
        jsonrpc: "2.0",
        id: message.id,
        error: {
          code: -32000,
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }
  }

  async function handleMcpRequest(message) {
    if (message.method === "initialize") {
      return {
        protocolVersion: "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: "soty-source-console", version: agentVersion }
      };
    }
    if (message.method === "tools/list") {
      return { tools: sotyMcpToolList() };
    }
    if (message.method === "tools/call") {
      return await callSotyMcpTool(message.params || {});
    }
    return {};
  }

  function sotyMcpToolList() {
    return [
      {
        name: "soty_toolkit",
        description: "Universal Soty automation toolkit entrypoint for any software or console work on the current LINK source device. Use this first for repeated, long, state-changing, install/repair/diagnostic, or scriptable tasks. It routes to first-class toolkits such as windows-reinstall or to the durable-action kernel, records proof, and keeps old run/script paths as low-level fallback.",
        inputSchema: {
          type: "object",
          properties: {
            operation: { type: "string", description: "describe, start, status, stop, list, or reinstall. Defaults to start when command/script is present." },
            toolkit: { type: "string", description: "Toolkit name, for example windows-reinstall, durable-action, console, software, or auto." },
            phase: { type: "string", description: "Toolkit phase, for example probe, prepare, install, repair, verify, backup, status, arm." },
            mode: { type: "string", description: "run or script for durable-action start. Defaults to script when script is provided." },
            command: { type: "string", description: "Command for mode=run." },
            script: { type: "string", description: "Script body for mode=script." },
            shell: { type: "string", description: "Optional shell hint, usually powershell on Windows." },
            name: { type: "string", description: "Short operator label." },
            family: { type: "string", description: "Task family, for example package-install, service-check, browser-restore, driver-check, generic." },
            intent: { type: "string", description: "Short intent for reusable learning." },
            risk: { type: "string", description: "low, medium, high, or destructive." },
            idempotencyKey: { type: "string", description: "Stable key to avoid duplicate execution on retries." },
            detached: { type: "boolean", description: "When true, return immediately with a running jobId and poll status." },
            waitForCompletion: { type: "boolean", description: "When true, wait for a terminal state unless the user explicitly asked for background mode." },
            waitTimeoutMs: { type: "integer", description: "Maximum turnkey wait in milliseconds, 1000-86400000." },
            timeoutMs: { type: "integer", description: "Per-action timeout in milliseconds, 1000-86400000." },
            jobId: { type: "string", description: "Job id for status/stop." },
            action: { type: "string", description: "Windows reinstall action when toolkit=windows-reinstall: preflight, prepare, status, or arm." },
            usbDriveLetter: { type: "string", description: "Windows reinstall USB drive letter." },
            confirmationPhrase: { type: "string", description: "Exact destructive confirmation phrase for arm." },
            useExistingUsbInstallImage: { type: "boolean", description: "Windows reinstall prepare: require existing valid USB install image." },
            improvement: { type: "string", description: "Optional sanitized reusable improvement note when this run proves a safe toolkit improvement." },
            reuseKey: { type: "string", description: "Stable reusable route/script key when this action should help unrelated future tasks reuse the same method." },
            pivotFrom: { type: "string", description: "Optional previous task vector when the user changed direction and this action continues with existing proof/artifacts." },
            successCriteria: { type: "string", description: "Short done condition used to keep quality high while optimizing speed." },
            scriptUse: { type: "string", description: "How the script/knowledge is being reused, for example probe, backup, repair, prepare, verify." },
            contextFingerprint: { type: "string", description: "Tiny environment boundary for reusable learning, without secrets or private ids." }
          },
          additionalProperties: false
        }
      },
      {
        name: "soty_run",
        description: "Run a shell command on the current Soty Agent LINK source device.",
        inputSchema: {
          type: "object",
          properties: {
            command: { type: "string", description: "Command to run on the source device." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-86400000." }
          },
          required: ["command"],
          additionalProperties: false
        }
      },
      {
        name: "soty_script",
        description: "Run a multiline script on the current Soty Agent LINK source device.",
        inputSchema: {
          type: "object",
          properties: {
            script: { type: "string", description: "Script body to run on the source device." },
            shell: { type: "string", description: "Optional shell hint, usually powershell on Windows." },
            name: { type: "string", description: "Short technical label shown in the LINK console." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-86400000." }
          },
          required: ["script"],
          additionalProperties: false
        }
      },
      {
        name: "soty_action",
        description: "Start a supervised durable job on the current Soty Agent LINK source device.",
        inputSchema: {
          type: "object",
          properties: {
            mode: { type: "string", description: "run or script. Defaults to script when script is provided, otherwise run." },
            command: { type: "string", description: "Command for mode=run." },
            script: { type: "string", description: "Script body for mode=script." },
            shell: { type: "string", description: "Optional shell hint, usually powershell on Windows." },
            name: { type: "string", description: "Short label shown in the LINK console." },
            toolkit: { type: "string", description: "Toolkit name, defaults from family." },
            phase: { type: "string", description: "Toolkit phase, defaults from kind." },
            family: { type: "string", description: "Task family, for example windows-reinstall, package-install, service-check, driver-check, generic." },
            kind: { type: "string", description: "Action kind, for example prepare, verify, install, repair, backup, probe." },
            intent: { type: "string", description: "Short operator intent for future learning." },
            risk: { type: "string", description: "low, medium, high, or destructive." },
            idempotencyKey: { type: "string", description: "Stable key to avoid duplicate execution on retries." },
            detached: { type: "boolean", description: "When true, return immediately with a running jobId and poll with soty_action_status." },
            waitForCompletion: { type: "boolean", description: "When true, keep the tool call open until the action reaches a terminal state. Use this for turnkey user-facing tasks unless the user explicitly asked for background mode." },
            waitTimeoutMs: { type: "integer", description: "Maximum turnkey wait in milliseconds, 1000-86400000." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-86400000." },
            improvement: { type: "string", description: "Optional sanitized reusable improvement note when this job proves a safe toolkit improvement." },
            reuseKey: { type: "string", description: "Stable reusable route/script key when this action should help unrelated future tasks reuse the same method." },
            pivotFrom: { type: "string", description: "Optional previous task vector when the user changed direction and this action continues with existing proof/artifacts." },
            successCriteria: { type: "string", description: "Short done condition used to keep quality high while optimizing speed." },
            scriptUse: { type: "string", description: "How the script/knowledge is being reused, for example probe, backup, repair, prepare, verify." },
            contextFingerprint: { type: "string", description: "Tiny environment boundary for reusable learning, without secrets or private ids." }
          },
          additionalProperties: false
        }
      },
      {
        name: "soty_action_status",
        description: "Read status/result for a supervised Soty action job by jobId.",
        inputSchema: {
          type: "object",
          properties: {
            jobId: { type: "string", description: "Action job id returned by soty_action." }
          },
          required: ["jobId"],
          additionalProperties: false
        }
      },
      {
        name: "soty_action_stop",
        description: "Stop a running supervised Soty action job. This sends a cancel to the target device when possible and marks the job cancelled.",
        inputSchema: {
          type: "object",
          properties: {
            jobId: { type: "string", description: "Action job id returned by soty_action." }
          },
          required: ["jobId"],
          additionalProperties: false
        }
      },
      {
        name: "soty_action_list",
        description: "List recent supervised Soty action jobs with status, proof, and statusPath.",
        inputSchema: {
          type: "object",
          properties: {},
          additionalProperties: false
        }
      },
      {
        name: "soty_link_status",
        description: "Inspect Soty relay/source health for the current LINK source device.",
        inputSchema: {
          type: "object",
          properties: {},
          additionalProperties: false
        }
      },
      {
        name: "soty_toolkits",
        description: "Show current Soty automation toolkit entrypoints, phases, proof fields, and terminal states.",
        inputSchema: {
          type: "object",
          properties: {},
          additionalProperties: false
        }
      },
      {
        name: "soty_reinstall",
        description: "Managed Soty Windows reinstall toolkit for preflight, prepare, status, and arm.",
        inputSchema: {
          type: "object",
          properties: {
            action: { type: "string", description: "One of: preflight, prepare, status, arm." },
            usbDriveLetter: { type: "string", description: "Removable install USB drive letter, for example D. Defaults to D." },
            confirmationPhrase: { type: "string", description: "Exact destructive confirmation phrase. Required only for arm." },
            useExistingUsbInstallImage: { type: "boolean", description: "When true, prepare refuses to download Windows and requires a valid existing USB install image." },
            waitForCompletion: { type: "boolean", description: "Default true for prepare. Keep true unless the user explicitly asked to run in background." },
            waitTimeoutMs: { type: "integer", description: "Maximum turnkey wait in milliseconds, default up to 86400000 for prepare." },
            waitMs: { type: "integer", description: "For status only: wait inside the toolkit before reading status again. Use instead of local shell sleep." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds. Use short timeouts for preflight/status; prepare and arm are durable actions." }
          },
          required: ["action"],
          additionalProperties: false
        }
      },
      {
        name: "soty_open_url",
        description: "Open a URL in the default browser on the current Soty Agent LINK source device.",
        inputSchema: {
          type: "object",
          properties: {
            url: { type: "string", description: "URL to open on the source device." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-86400000." }
          },
          required: ["url"],
          additionalProperties: false
        }
      },
      {
        name: "soty_file",
        description: "Seamless Desktop-Commander-style file access on the current Soty Agent LINK source device. Use for listing, reading, writing, searching, moving, copying, deleting, and creating project files. The operation is shown in the user's LINK console and is scoped to the current LINK source device.",
        inputSchema: {
          type: "object",
          properties: {
            action: { type: "string", description: "One of: stat, list, read, write, append, mkdir, search, move, copy, delete." },
            path: { type: "string", description: "File or directory path on the source device." },
            toPath: { type: "string", description: "Destination path for move/copy." },
            content: { type: "string", description: "Content for write/append." },
            pattern: { type: "string", description: "Search text or regular expression." },
            glob: { type: "string", description: "Optional filename wildcard for search, for example *.ts." },
            regex: { type: "boolean", description: "When true, treat pattern as regex. Default false uses plain text." },
            recursive: { type: "boolean", description: "Recurse for list/search/delete directory. Default false except search." },
            maxResults: { type: "integer", description: "Maximum list/search results, 1-500." },
            maxChars: { type: "integer", description: "Maximum characters returned for read/search, 1000-12000." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-86400000." }
          },
          required: ["action", "path"],
          additionalProperties: false
        }
      },
      {
        name: "soty_browser",
        description: "Seamless browser automation on the current Soty Agent LINK source device. Uses installed Edge/Chrome through a local DevTools session when possible; no separate user confirmation is shown beyond the active LINK. Use for opening pages, reading title/text, JavaScript eval, click-by-text, typing into selectors, and saving screenshots.",
        inputSchema: {
          type: "object",
          properties: {
            action: { type: "string", description: "One of: open, goto, title, text, eval, click_text, type, screenshot." },
            url: { type: "string", description: "URL for open/goto." },
            script: { type: "string", description: "JavaScript expression/function body for eval." },
            text: { type: "string", description: "Visible text to click, or text to type." },
            selector: { type: "string", description: "CSS selector for type/eval helper actions." },
            headless: { type: "boolean", description: "Launch browser headless. Default false so the user can see it." },
            maxChars: { type: "integer", description: "Maximum returned text, 1000-12000." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-86400000." }
          },
          required: ["action"],
          additionalProperties: false
        }
      },
      {
        name: "soty_desktop",
        description: "Seamless Windows desktop control on the current Soty Agent LINK source device. Use for screenshots, window listing/focus, clicks, typing, and hotkeys when command/API routes are not enough. Actions are shown in the user's LINK console.",
        inputSchema: {
          type: "object",
          properties: {
            action: { type: "string", description: "One of: screenshot, windows, focus, click, type, key." },
            title: { type: "string", description: "Window title substring for focus." },
            x: { type: "integer", description: "Screen X coordinate for click." },
            y: { type: "integer", description: "Screen Y coordinate for click." },
            button: { type: "string", description: "left or right. Default left." },
            text: { type: "string", description: "Text for type action." },
            keys: { type: "string", description: "SendKeys pattern for key action, for example ^l or %{F4}." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-86400000." }
          },
          required: ["action"],
          additionalProperties: false
        }
      },
      {
        name: "soty_audio",
        description: "Read or change the default Windows output volume/mute on the current Soty Agent LINK source device. Use this for Russian requests like 'звук на 30', 'громкость 30', 'выключи звук', 'включи звук'. 'звук на 30' means volumePercent=30 and muted=false, not waiting 30 seconds. The PowerShell command and result are shown in the user's LINK console.",
        inputSchema: {
          type: "object",
          properties: {
            volumePercent: { type: "integer", description: "Optional output volume percent, 0-100." },
            muted: { type: "boolean", description: "Optional mute state. true mutes output; false unmutes output." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-86400000. Default is 120000 to survive cold Windows audio startup." }
          },
          additionalProperties: false
        }
      },
      {
        name: "soty_image",
        description: "Generate a raster image with the OpenAI Images API and save it as a local PNG/JPEG/WebP file in the Soty Codex runtime. Use for photos, wallpapers, avatars, illustrations, product shots, and other generated bitmap assets.",
        inputSchema: {
          type: "object",
          properties: {
            prompt: { type: "string", description: "Image prompt." },
            path: { type: "string", description: "Optional output path. If omitted, saves to the Desktop when available." },
            model: { type: "string", description: "Optional GPT Image model, default from SOTY_IMAGE_MODEL or gpt-image-1.5." },
            size: { type: "string", description: "auto, 1024x1024, 1536x1024, or 1024x1536. Default auto." },
            quality: { type: "string", description: "auto, low, medium, or high. Default auto." },
            format: { type: "string", description: "png, jpeg, or webp. Default png." },
            background: { type: "string", description: "auto, opaque, or transparent when supported by the selected model." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, default 300000." }
          },
          required: ["prompt"],
          additionalProperties: false
        }
      },
    ];
  }

  function isPowerShellWorkflowCommand(command) {
    const value = String(command || "");
    if (!/\b(?:powershell|pwsh)(?:\.exe)?\b/iu.test(value)) {
      return false;
    }
    return /[$;|`]|[\r\n]|\b(?:Get|Set|New|Remove|Start|Stop|Invoke|Convert|Where|ForEach)-[A-Za-z]/u.test(value);
  }

  async function callSotyMcpTool(params) {
    const name = String(params.name || "");
    const args = params.arguments && typeof params.arguments === "object" ? params.arguments : {};
    if (name === "soty_action_list") {
      const result = await mcpRequestOperator("GET", "/operator/actions");
      return mcpToolJson(result.payload || result, !result.ok, result.exitCode);
    }
    if (name === "soty_link_status") {
      const query = new URLSearchParams();
      if (mcpTarget) {
        query.set("target", mcpTarget);
      }
      if (mcpSourceDeviceId) {
        query.set("sourceDeviceId", mcpSourceDeviceId);
      }
      if (mcpSourceRelayId) {
        query.set("sourceRelayId", mcpSourceRelayId);
      }
      const suffix = query.toString() ? `?${query.toString()}` : "";
      const result = await mcpRequestOperator("GET", `/operator/source-status${suffix}`);
      return mcpToolJson(result.payload || result, !result.ok, result.exitCode);
    }
    if (name === "soty_toolkits") {
      return mcpToolJson({
        ok: true,
        version: agentVersion,
        manifestUrl: updateManifestUrl,
        ...automationToolkitStatus()
      });
    }
    if (name === "soty_toolkit") {
      return await callSotyToolkitTool(args);
    }
    if (name === "soty_action_status") {
      const jobId = String(args.jobId || "").trim();
      if (!/^[A-Za-z0-9_-]{8,96}$/u.test(jobId)) {
        return mcpToolText("! action-job", true, 2);
      }
      const result = await mcpRequestOperator("GET", `/operator/action/${encodeURIComponent(jobId)}`);
      const payload = result.payload || result;
      if (isManagedReinstallActionPayload(payload)) {
        return await mcpToolManagedReinstallActionStatus(payload, result);
      }
      return mcpToolJson(payload, !result.ok, result.exitCode);
    }
    if (name === "soty_action_stop") {
      const jobId = String(args.jobId || "").trim();
      if (!/^[A-Za-z0-9_-]{8,96}$/u.test(jobId)) {
        return mcpToolText("! action-job", true, 2);
      }
      const result = await mcpRequestOperator("POST", `/operator/action/${encodeURIComponent(jobId)}/stop`, {});
      return mcpToolJson(result.payload || result, !result.ok, result.exitCode);
    }
    if (!mcpTarget || !mcpSourceDeviceId) {
      return mcpToolText("! agent-source: current Soty Agent LINK source is not attached", true);
    }
    if (name === "soty_reinstall") {
      return await callSotyReinstallTool(args);
    }
    if (name === "soty_action") {
      return await callSotyActionKernelTool(args);
    }
    if (name === "soty_run") {
      const command = String(args.command || "").trim();
      if (!command) {
        return mcpToolText("! command", true);
      }
      const managedRedirect = await maybeRedirectManagedReinstallProbe("soty_run", command);
      if (managedRedirect) {
        return managedRedirect;
      }
      if (isPowerShellWorkflowCommand(command)) {
        return mcpToolText("! soty-run-powershell-workflow: use soty_script with shell=\"powershell\" for PowerShell variables, pipelines, semicolons, or multi-step checks.", true, 64);
      }
      const result = await mcpPostOperator("/operator/run", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        command,
        timeoutMs: mcpSafeTimeout(args.timeoutMs, defaultTimeoutMs)
      });
      return mcpToolOperatorResult(result);
    }
    if (name === "soty_script") {
      const script = String(args.script || "").trim();
      if (!script) {
        return mcpToolText("! script", true);
      }
      const managedRedirect = await maybeRedirectManagedReinstallProbe("soty_script", `${args.name || ""}\n${script}`);
      if (managedRedirect) {
        return managedRedirect;
      }
      const result = await mcpPostOperator("/operator/script", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script,
        shell: String(args.shell || ""),
        name: String(args.name || "script"),
        timeoutMs: mcpSafeTimeout(args.timeoutMs, defaultTimeoutMs)
      });
      return mcpToolOperatorResult(result);
    }
    if (name === "soty_file") {
      const action = String(args.action || "").trim().toLowerCase();
      const path = String(args.path || "").trim();
      if (!action || !path) {
        return mcpToolText("! file", true);
      }
      const managedRedirect = await maybeRedirectManagedReinstallProbe("soty_file", path);
      if (managedRedirect) {
        return managedRedirect;
      }
      const result = await mcpPostOperator("/operator/script", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script: sourceFileScript(args),
        shell: "node",
        name: `soty-file-${action}`.slice(0, 120),
        timeoutMs: mcpSafeTimeout(args.timeoutMs, defaultTimeoutMs)
      });
      return mcpToolOperatorResult(result);
    }
    if (name === "soty_browser") {
      const action = String(args.action || "").trim().toLowerCase();
      if (!action) {
        return mcpToolText("! browser", true);
      }
      const result = await mcpPostOperator("/operator/script", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script: sourceBrowserScript(args),
        shell: "node",
        name: `soty-browser-${action}`.slice(0, 120),
        timeoutMs: mcpSafeTimeout(args.timeoutMs, 10 * 60_000)
      });
      return mcpToolOperatorResult(result);
    }
    if (name === "soty_desktop") {
      const action = String(args.action || "").trim().toLowerCase();
      if (!action) {
        return mcpToolText("! desktop", true);
      }
      const result = await mcpPostOperator("/operator/script", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script: sourceDesktopScript(args),
        shell: "powershell",
        name: `soty-desktop-${action}`.slice(0, 120),
        timeoutMs: mcpSafeTimeout(args.timeoutMs, 60_000)
      });
      return mcpToolOperatorResult(result);
    }
    if (name === "soty_open_url") {
      const url = String(args.url || "").trim();
      if (!/^https?:\/\//iu.test(url)) {
        return mcpToolText("! url", true);
      }
      const result = await mcpPostOperator("/operator/script", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script: sourceOpenUrlScript(url),
        shell: "node",
        name: "soty-open-url",
        timeoutMs: mcpSafeTimeout(args.timeoutMs, 60_000)
      });
      return mcpToolOperatorResult(result, "opened");
    }
    if (name === "soty_audio") {
      const rawVolume = Number(args.volumePercent);
      const volumePercent = Number.isFinite(rawVolume) ? Math.max(0, Math.min(100, Math.round(rawVolume))) : -1;
      const muteMode = typeof args.muted === "boolean" ? (args.muted ? 1 : 0) : -1;
      const timeoutMs = mcpSafeTimeout(args.timeoutMs, audioToolTimeoutMs);
      const audioPayload = {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script: windowsAudioScript(volumePercent, muteMode),
        shell: "powershell",
        name: "soty-audio",
        timeoutMs
      };
      let result = await mcpPostOperator("/operator/script", audioPayload);
      if (isAudioTimeoutResult(result)) {
        await sleep(800);
        result = await mcpPostOperator("/operator/script", {
          ...audioPayload,
          timeoutMs: Math.max(timeoutMs, audioToolTimeoutMs),
          name: "soty-audio-retry"
        });
      }
      return mcpToolOperatorResult(result);
    }
    if (name === "soty_image") {
      const result = await callSotyImageTool(args);
      return result.ok
        ? mcpToolJson(result, false, 0)
        : mcpToolJson(result, true, result.exitCode || 1);
    }
    return mcpToolText(`! unknown tool ${name}`, true);
  }

  function mcpSourceUnavailableResult() {
    return mcpToolText("! agent-source: current Soty Agent LINK source is not attached", true);
  }

  async function callSotyImageTool(args) {
    const image = await generateOpenAiImageData(args);
    if (!image.ok) {
      return image;
    }
    if (mcpTarget && mcpSourceDeviceId) {
      const save = await mcpPostOperator("/operator/script", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script: sourceSaveGeneratedImageScript({
          imageBase64: image.imageBase64,
          path: args.path || "",
          format: image.format
        }),
        shell: "node",
        name: "soty-image-save",
        timeoutMs: mcpSafeTimeout(args.timeoutMs, 300_000)
      });
      if (save.ok) {
        const saved = parseJsonObject(save.text) || {};
        return imageResultPublic({
          ...image,
          path: saved.path || "",
          bytes: Number.isSafeInteger(saved.bytes) ? saved.bytes : image.bytes,
          savedBy: "source-device"
        });
      }
      return {
        ...imageResultPublic(image),
        ok: false,
        error: "image generated but source-device save failed",
        save: save.payload || { text: save.text, exitCode: save.exitCode },
        exitCode: save.exitCode || 1
      };
    }
    const saved = await saveGeneratedImageLocal(image, args);
    return imageResultPublic({
      ...image,
      ...saved,
      savedBy: "codex-runtime"
    });
  }

  async function callSotyToolkitTool(args) {
    const rawOperation = String(args.operation || "").trim().toLowerCase();
    const operation = cleanActionToken(rawOperation || (args.jobId ? "status" : (args.action ? "reinstall" : (args.command || args.script ? "start" : "describe"))), "describe");
    const toolkit = normalizeToolkitName(args.toolkit || (args.action ? "windows-reinstall" : ""));
    const phase = cleanActionToken(args.phase || args.action || operation, operation);
    if (operation === "describe" || operation === "toolkits") {
      return mcpToolJson({
        ok: true,
        version: agentVersion,
        manifestUrl: updateManifestUrl,
        ...automationToolkitStatus()
      });
    }
    if (operation === "list") {
      const result = await mcpRequestOperator("GET", "/operator/actions");
      return mcpToolJson({
        ...(result.payload || result),
        toolkitContract: automationToolkitStatus()
      }, !result.ok, result.exitCode);
    }
    if (operation === "status" && args.jobId) {
      const jobId = String(args.jobId || "").trim();
      if (!/^[A-Za-z0-9_-]{8,96}$/u.test(jobId)) {
        return mcpToolText("! action-job", true, 2);
      }
      const result = await mcpRequestOperator("GET", `/operator/action/${encodeURIComponent(jobId)}`);
      return mcpToolJson(result.payload || result, !result.ok, result.exitCode);
    }
    if (operation === "stop") {
      const jobId = String(args.jobId || "").trim();
      if (!/^[A-Za-z0-9_-]{8,96}$/u.test(jobId)) {
        return mcpToolText("! action-job", true, 2);
      }
      const result = await mcpRequestOperator("POST", `/operator/action/${encodeURIComponent(jobId)}/stop`, {});
      return mcpToolJson(result.payload || result, !result.ok, result.exitCode);
    }
    const reinstallAction = cleanActionToken(args.action || (toolkit === "windows-reinstall" && ["preflight", "prepare", "status", "arm"].includes(phase) ? phase : ""), "");
    if (operation === "reinstall" || toolkit === "windows-reinstall" || reinstallAction) {
      if (!mcpTarget || !mcpSourceDeviceId) {
        return mcpSourceUnavailableResult();
      }
      return await callSotyReinstallTool({
        ...args,
        action: reinstallAction || phase || "status"
      });
    }
    if (!["start", "run", "script", "execute", "probe", "prepare", "install", "repair", "verify", "backup"].includes(operation)) {
      return mcpToolText("! toolkit-operation", true, 2);
    }
    return await callSotyActionKernelTool({
      ...args,
      toolkit,
      phase,
      kind: args.kind || phase,
      waitForCompletion: args.waitForCompletion === true
    });
  }

  async function callSotyActionKernelTool(args) {
    if (!mcpTarget || !mcpSourceDeviceId) {
      return mcpSourceUnavailableResult();
    }
    const mode = args.mode === "script" || typeof args.script === "string" ? "script" : "run";
    const command = String(args.command || "").trim();
    const script = String(args.script || "").trim();
    if (mode === "run" && !command) {
      return mcpToolText("! command", true);
    }
    if (mode === "script" && !script) {
      return mcpToolText("! script", true);
    }
    const family = String(args.family || "");
    const toolkit = normalizeToolkitName(args.toolkit || toolkitForFamily(family || classifySourceCommand(mode === "script" ? script : command)));
    const phase = cleanActionToken(args.phase || args.kind || "execute", "execute");
    const result = await mcpRequestOperator("POST", "/operator/action", {
      mode,
      target: mcpTarget,
      sourceDeviceId: mcpSourceDeviceId,
      ...(mode === "run" ? { command } : { script }),
      shell: String(args.shell || ""),
      name: String(args.name || `${toolkit}-${phase}`),
      toolkit,
      phase,
      family,
      kind: String(args.kind || phase),
      intent: String(args.intent || ""),
      risk: String(args.risk || ""),
      idempotencyKey: String(args.idempotencyKey || ""),
      improvement: String(args.improvement || ""),
      reuseKey: String(args.reuseKey || ""),
      pivotFrom: String(args.pivotFrom || ""),
      successCriteria: String(args.successCriteria || ""),
      scriptUse: String(args.scriptUse || ""),
      contextFingerprint: String(args.contextFingerprint || ""),
      detached: args.detached === true,
      wait: args.waitForCompletion === true,
      timeoutMs: mcpSafeTimeout(args.timeoutMs, defaultTimeoutMs)
    });
    if (args.waitForCompletion === true) {
      const waited = await waitForMcpActionTerminal(result.payload || result, {
        waitTimeoutMs: mcpSafeTimeout(args.waitTimeoutMs, maxLongTaskTimeoutMs),
        progressKind: String(args.phase || args.kind || args.family || args.toolkit || "toolkit")
      });
      return mcpToolJson(waited, waited.ok === false, waited.exitCode);
    }
    return mcpToolJson(result.payload || result, !result.ok, result.exitCode);
  }

  async function mcpCurrentSourceStatus() {
    const query = new URLSearchParams();
    if (mcpTarget) {
      query.set("target", mcpTarget);
    }
    if (mcpSourceRelayId) {
      query.set("sourceRelayId", mcpSourceRelayId);
    }
    if (mcpSourceDeviceId) {
      query.set("sourceDeviceId", mcpSourceDeviceId);
    }
    const suffix = query.toString() ? `?${query.toString()}` : "";
    const result = await mcpRequestOperator("GET", `/operator/source-status${suffix}`);
    return result.payload || {
      ok: false,
      text: "source status unavailable",
      exitCode: result.exitCode || 1
    };
  }

  function sourceStatusSummary(status) {
    const relay = status?.relay && typeof status.relay === "object" ? status.relay : {};
    const source = relay.source && typeof relay.source === "object" ? relay.source : null;
    const candidates = Array.isArray(relay.candidates)
      ? relay.candidates
      : Array.isArray(status?.sourceTargets)
        ? status.sourceTargets
        : [];
    const best = source || candidates[0] || null;
    const lastSeenAgeMs = Number(best?.lastSeenAgeMs);
    const sourceConnectedMs = Number(best?.sourceConnectedMs);
    const recentlySeen = Number.isFinite(lastSeenAgeMs)
      && Number.isFinite(sourceConnectedMs)
      && lastSeenAgeMs < sourceConnectedMs;
    const runnable = relay.runnable === true || best?.connected === true || (best?.access === true && recentlySeen);
    return {
      ok: status?.ok === true,
      runnable,
      reason: String(relay.reason || status?.text || "").slice(0, 120),
      agentVersion: String(status?.localAgent?.version || "").slice(0, 40),
      relayConfigured: status?.relayConfigured === true,
      target: String(status?.target || mcpTarget || "").slice(0, 180),
      sourceDeviceId: String(status?.sourceDeviceId || mcpSourceDeviceId || "").slice(0, 180),
      lastSeenAgeMs: Number.isFinite(lastSeenAgeMs) ? lastSeenAgeMs : null,
      pendingJobs: Number.isFinite(Number(best?.pendingJobs)) ? Number(best.pendingJobs) : null,
      leasedJobs: Number.isFinite(Number(best?.leasedJobs)) ? Number(best.leasedJobs) : null
    };
  }

  function mcpOperatorPayload(result) {
    const payload = result?.payload && typeof result.payload === "object" ? { ...result.payload } : {};
    if (!Object.prototype.hasOwnProperty.call(payload, "ok")) {
      payload.ok = Boolean(result?.ok);
    }
    if (!payload.text && result?.text) {
      payload.text = String(result.text);
    }
    if (!Number.isSafeInteger(payload.exitCode)) {
      payload.exitCode = Number.isSafeInteger(result?.exitCode) ? result.exitCode : (payload.ok ? 0 : 1);
    }
    return payload;
  }

  function isSourceRouteFailure(result) {
    const payload = result?.payload && typeof result.payload === "object" ? result.payload : {};
    const diagnostic = payload.diagnostic && typeof payload.diagnostic === "object" ? payload.diagnostic : {};
    const exitCode = Number.isSafeInteger(payload.exitCode)
      ? payload.exitCode
      : Number.isSafeInteger(result?.exitCode)
        ? result.exitCode
        : 0;
    const text = [
      result?.text,
      payload.text,
      diagnostic.kind,
      diagnostic.reason,
      diagnostic.bodyPreview
    ].map((item) => String(item || "").toLowerCase()).join(" ");
    return [124, 127, 502, 504].includes(exitCode)
      || text.includes("timeout")
      || text.includes("relay-json")
      || text.includes("relay-fetch")
      || text.includes("agent-source");
  }

  async function mcpToolJsonTextWithSourceStatus(result, context = {}) {
    if (result?.ok || !isSourceRouteFailure(result)) {
      return mcpToolJsonText(result);
    }
    let sourceStatus = null;
    try {
      sourceStatus = await mcpCurrentSourceStatus();
    } catch {}
    const link = sourceStatus ? sourceStatusSummary(sourceStatus) : null;
    const payload = {
      ...mcpOperatorPayload(result),
      toolkit: String(context.toolkit || "").slice(0, 80),
      action: String(context.action || "").slice(0, 40),
      route: String(context.route || "").slice(0, 120),
      sourceLink: link,
      blocker: link?.runnable ? "source-command-route-timeout" : "source-link-unavailable",
      agentGuidance: link?.runnable
        ? "LINK/source status is healthy enough; do not tell the user the PC is not visible. Report the failed command route/job, then continue through durable status/action or give one concrete blocker."
        : "Only say the target channel is unavailable if this sourceLink summary proves it is not runnable."
    };
    return mcpToolJson(payload, true, payload.exitCode);
  }

  async function callSotyReinstallTool(args) {
    const action = String(args.action || "").trim().toLowerCase();
    if (!["preflight", "prepare", "status", "arm"].includes(action)) {
      return mcpToolText("! reinstall-action", true, 2);
    }
    const usbDriveLetter = normalizeUsbDriveLetter(args.usbDriveLetter || "D");
    const request = {
      action,
      usbDriveLetter,
      confirmationPhrase: String(args.confirmationPhrase || "").trim().slice(0, 300),
      useExistingUsbInstallImage: args.useExistingUsbInstallImage === true,
      manifestUrl: updateManifestUrl,
      panelSiteUrl: originFromUrl(updateManifestUrl) || agentRelayBaseUrl || "https://xn--n1afe0b.online",
      workspaceRoot: "C:\\ProgramData\\Soty\\WindowsReinstall"
    };
    if (action === "arm" && !request.confirmationPhrase) {
      return mcpToolText("! confirmation-phrase", true, 2);
    }
    if (action === "preflight" || action === "status") {
      if (action === "status" && mcpPostArmReboot && Date.now() - mcpPostArmReboot.createdAt < 90 * 60_000) {
        return mcpToolJson({
          ok: true,
          action: "status",
          status: "rebooting",
          terminalReason: "post-arm-rebooting",
          text: "Windows reinstall has been armed and the PC is rebooting. Do not poll the source device until the designed return path is due.",
          exitCode: 0,
          postArm: mcpPostArmReboot,
          agentGuidance: "Stop source/LINK status probes after arm rebooting=true. Tell the user connection may drop during reinstall and wait for the return path."
        });
      }
      const minimumTimeoutMs = action === "preflight" ? 90_000 : 45_000;
      const operatorTimeoutMs = Math.max(mcpSafeTimeout(args.timeoutMs, minimumTimeoutMs), minimumTimeoutMs);
      if (action === "status") {
        const requestedWaitMs = Number.parseInt(String(args.waitMs ?? args.waitForChangeMs ?? "0"), 10);
        const statusWaitMs = Number.isSafeInteger(requestedWaitMs)
          ? Math.max(0, Math.min(requestedWaitMs, Math.max(0, mcpInlineToolBudgetMs - operatorTimeoutMs - 5000)))
          : 0;
        if (statusWaitMs > 0) {
          await sleep(statusWaitMs);
        }
      }
      const result = await mcpPostOperator("/operator/script", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script: sourceManagedWindowsReinstallScript(request),
        shell: "powershell",
        name: `soty-reinstall-${action}`,
        timeoutMs: operatorTimeoutMs
      });
      return await mcpToolJsonTextWithSourceStatus(result, {
        toolkit: "windows-reinstall",
        action,
        route: `soty_reinstall.${action}`
      });
    }
    const shouldWait = action === "prepare" && args.waitForCompletion !== false;
    if (action === "prepare") {
      const existingStatusResult = await readSotyReinstallStatus(managedReinstallStatusRequest(usbDriveLetter));
      const existingStatus = parseReinstallStatusResult(existingStatusResult);
      if (existingStatus) {
        const existingInitial = {
          ok: true,
          action: "prepare",
          status: "running",
          reusedExistingPrepare: true,
          reason: "managed-prepare-already-active-or-ready"
        };
        const terminal = evaluateReinstallPrepareTerminal(existingStatus, existingInitial, 0);
        if (terminal) {
          return mcpToolJson(terminal, terminal.ok === false, terminal.exitCode);
        }
        if (isReinstallPrepareActive(existingStatus)) {
          if (shouldWait) {
            const requestedWaitTimeoutMs = mcpSafeTimeout(args.waitTimeoutMs, maxLongTaskTimeoutMs);
            const waited = await waitForSotyReinstallPrepare({
              request,
              initial: existingInitial,
              waitTimeoutMs: Math.min(requestedWaitTimeoutMs, mcpInlineToolBudgetMs),
              requestedWaitTimeoutMs
            });
            return mcpToolJson(waited, waited.ok === false, waited.exitCode);
          }
          return mcpToolJson({
            ...existingInitial,
            statusSnapshot: existingStatus,
            nextTool: {
              name: "soty_reinstall",
              args: {
                action: "status",
                waitMs: 45_000,
                timeoutMs: 45_000
              }
            },
            agentGuidance: "An existing managed prepare is already active. Continue with soty_reinstall status; do not start another prepare."
          });
        }
      }
    }
    const keyDate = new Date().toISOString().slice(0, 10).replace(/-/gu, "");
    const keyMinute = Math.floor(Date.now() / 60_000);
    const sourceToken = cleanActionId(String(mcpSourceDeviceId || "").slice(0, 24)) || "source";
    const phraseHash = action === "arm"
      ? createHash("sha256").update(request.confirmationPhrase).digest("hex").slice(0, 12)
      : "";
    const result = await mcpRequestOperator("POST", "/operator/action", {
      mode: "script",
      target: mcpTarget,
      sourceDeviceId: mcpSourceDeviceId,
      script: sourceManagedWindowsReinstallScript(request),
      shell: "powershell",
      name: `soty-reinstall-${action}`,
      family: "windows-reinstall",
      kind: action,
      intent: action === "prepare"
        ? "managed Windows reinstall prepare: backup, media, unattended account, postinstall"
        : "managed Windows reinstall arm after exact destructive confirmation",
      risk: action === "arm" ? "destructive" : "high",
      idempotencyKey: action === "prepare"
        ? `windows-reinstall-prepare-${usbDriveLetter}-${sourceToken}-m${keyMinute}-v${agentVersion}`
        : `windows-reinstall-arm-${usbDriveLetter}-${sourceToken}-${keyDate}-${phraseHash}-v${agentVersion}`,
      detached: true,
      timeoutMs: mcpSafeTimeout(args.timeoutMs, action === "prepare" ? 120_000 : 90_000)
    });
    const payload = result.payload || result;
    if (action === "arm" && args.waitForCompletion !== false) {
      const requestedWaitTimeoutMs = mcpSafeTimeout(args.waitTimeoutMs, mcpInlineToolBudgetMs);
      const terminal = await waitForMcpActionTerminal(payload, {
        waitTimeoutMs: Math.min(requestedWaitTimeoutMs, mcpInlineToolBudgetMs),
        progressKind: "windows-reinstall-arm",
        pollDelayMs: 1000
      });
      const postArm = rememberPostArmReboot(terminal);
      if (postArm) {
        return mcpToolJson({
          ...terminal,
          ok: true,
          action: "arm",
          status: "rebooting",
          terminalReason: "post-arm-rebooting",
          text: "Windows reinstall has been armed and the PC is rebooting. Connection may drop during reinstall.",
          exitCode: 0,
          postArm,
          agentGuidance: "Do not call soty_reinstall status, soty_link_status, hostname, or health probes against this source after rebooting=true. Give the user the post-arm handoff and wait for the designed return path."
        });
      }
      return mcpToolJson(terminal, terminal.ok === false, terminal.exitCode);
    }
    if (shouldWait) {
      const requestedWaitTimeoutMs = mcpSafeTimeout(args.waitTimeoutMs, maxLongTaskTimeoutMs);
      const waited = await waitForSotyReinstallPrepare({
        request,
        initial: payload,
        waitTimeoutMs: Math.min(requestedWaitTimeoutMs, mcpInlineToolBudgetMs),
        requestedWaitTimeoutMs
      });
      return mcpToolJson(waited, waited.ok === false, waited.exitCode);
    }
    return mcpToolJson(payload, !result.ok, result.exitCode);
  }

  async function waitForMcpActionTerminal(initial, { waitTimeoutMs = maxLongTaskTimeoutMs, progressKind = "action", pollDelayMs = 15_000 } = {}) {
    const jobId = String(initial?.jobId || initial?.id || "").trim();
    if (!jobId) {
      return initial;
    }
    const started = Date.now();
    let lastPayload = initial;
    let lastProgressAt = Date.now();
    while (Date.now() - started < waitTimeoutMs) {
      const status = String(lastPayload?.status || "").toLowerCase();
      if (status && status !== "created" && status !== "running") {
        return lastPayload;
      }
      if (Date.now() - lastProgressAt > 15 * 60_000) {
        lastProgressAt = Date.now();
        await postMcpAgentProgress("Работа продолжается. Остановлюсь на результате, ошибке или нужном действии от вас.");
      }
      await sleep(Math.max(250, Math.min(15_000, pollDelayMs)));
      const result = await mcpRequestOperator("GET", `/operator/action/${encodeURIComponent(jobId)}`);
      lastPayload = result.payload || {
        ok: false,
        status: "blocked",
        text: `Cannot read ${progressKind} status`,
        exitCode: result.exitCode || 1
      };
    }
    return {
      ...lastPayload,
      ok: false,
      status: "blocked",
      text: "Live wait limit reached before the action reached a terminal state. Ask the user to keep the PC and Soty Agent open and write `продолжай` to resume monitoring.",
      blocker: "turnkey-wait-timeout",
      exitCode: 124
    };
  }

  async function waitForSotyReinstallPrepare({ request, initial, waitTimeoutMs, requestedWaitTimeoutMs = waitTimeoutMs }) {
    const started = Date.now();
    let lastStatus = null;
    let lastPayload = initial;
    let lastProgressAt = Date.now();
    let consecutiveStatusFailures = 0;
    while (Date.now() - started < waitTimeoutMs) {
      const statusResult = await readSotyReinstallStatus(request);
      const status = parseReinstallStatusResult(statusResult);
      if (status) {
        consecutiveStatusFailures = 0;
        lastStatus = status;
        const terminal = evaluateReinstallPrepareTerminal(status, initial, Date.now() - started);
        if (terminal) {
          return terminal;
        }
        if (Date.now() - lastProgressAt > reinstallProgressIntervalMs(status)) {
          lastProgressAt = Date.now();
          await postMcpAgentProgress(formatReinstallPrepareProgress(status));
        }
      } else {
        consecutiveStatusFailures += 1;
        lastPayload = statusResult.payload || statusResult;
        if (consecutiveStatusFailures >= 2) {
          return {
            ok: false,
            action: "prepare",
            status: "blocked",
            blocker: "source-status-unavailable",
            text: "I cannot continue monitoring the PC through Soty. Ask the user to open or restart Soty Agent on that PC, then retry status.",
            exitCode: statusResult.exitCode || 127,
            initial,
            lastStatus,
            lastProbe: lastPayload
          };
        }
      }
      const elapsedMs = Date.now() - started;
      const remainingMs = waitTimeoutMs - elapsedMs;
      const delayMs = reinstallPollDelayMs(lastStatus);
      if (remainingMs <= Math.min(10_000, delayMs)) {
        break;
      }
      await sleep(Math.min(delayMs, Math.max(1000, remainingMs - 5000)));
    }
    if (lastStatus) {
      return {
        ok: true,
        action: "prepare",
        status: "running",
        terminalReason: "still-running",
        text: "Preparation is still running. Continue monitoring with soty_reinstall status; do not start another prepare and do not use generic script/file polling.",
        exitCode: 0,
        elapsedMs: Date.now() - started,
        waitCapped: requestedWaitTimeoutMs > waitTimeoutMs,
        nextPollMs: Math.min(reinstallPollDelayMs(lastStatus), 45_000),
        nextTool: {
          name: "soty_reinstall",
          args: {
            action: "status",
            waitMs: Math.min(reinstallPollDelayMs(lastStatus), 45_000),
            timeoutMs: 45_000
          }
        },
        agentGuidance: "This is a non-terminal progress result. Keep the chat alive by calling nextTool. Do not use local shell sleep or generic script/file probes.",
        initial,
        lastStatus,
        lastProbe: lastPayload
      };
    }
    return {
      ok: false,
      action: "prepare",
      status: "blocked",
      blocker: "turnkey-wait-timeout",
      text: "Preparation did not reach ready or failed state before the live wait limit. Ask the user to keep the PC and Soty Agent open and write `продолжай` to resume monitoring.",
      exitCode: 124,
      initial,
      lastStatus,
      lastProbe: lastPayload
    };
  }

  async function readSotyReinstallStatus(request) {
    return await mcpPostOperator("/operator/script", {
      target: mcpTarget,
      sourceDeviceId: mcpSourceDeviceId,
      script: sourceManagedWindowsReinstallScript({ ...request, action: "status" }),
      shell: "powershell",
      name: "soty-reinstall-status",
      timeoutMs: 45_000
    });
  }

  function managedReinstallStatusRequest(usbDriveLetter = "D") {
    return {
      action: "status",
      usbDriveLetter: normalizeUsbDriveLetter(usbDriveLetter || "D"),
      confirmationPhrase: "",
      useExistingUsbInstallImage: false,
      manifestUrl: updateManifestUrl,
      panelSiteUrl: originFromUrl(updateManifestUrl) || agentRelayBaseUrl || "https://xn--n1afe0b.online",
      workspaceRoot: "C:\\ProgramData\\Soty\\WindowsReinstall"
    };
  }

  function isManagedReinstallActionPayload(payload) {
    const job = payload?.job && typeof payload.job === "object" ? payload.job : payload;
    const text = [
      job?.family,
      job?.toolkit,
      job?.intent,
      job?.idempotencyKey,
      job?.name
    ].map((item) => String(item || "").toLowerCase()).join(" ");
    return String(job?.family || "").toLowerCase() === "windows-reinstall"
      || String(job?.toolkit || "").toLowerCase() === "windows-reinstall"
      || text.includes("windows-reinstall")
      || text.includes("windows reinstall");
  }

  async function mcpToolManagedReinstallActionStatus(payload, result) {
    const postArm = rememberPostArmReboot(payload);
    if (postArm) {
      return mcpToolJson({
        ...payload,
        ok: true,
        status: "rebooting",
        terminalReason: "post-arm-rebooting",
        postArm,
        agentGuidance: "The managed arm already reached rebooting=true. Do not read live source status now; the source is expected to disconnect during reinstall."
      }, false, 0);
    }
    const statusResult = await readSotyReinstallStatus(managedReinstallStatusRequest());
    const liveStatus = parseReinstallStatusResult(statusResult);
    const body = {
      ...payload,
      liveStatus: liveStatus || mcpOperatorPayload(statusResult),
      liveStatusOk: Boolean(liveStatus),
      agentGuidance: "For managed Windows reinstall progress use liveStatus or soty_reinstall status. Do not crawl C:\\ProgramData\\Soty\\WindowsReinstall with soty_script, soty_run, or soty_file."
    };
    return mcpToolJson(body, !result.ok || !liveStatus, result.exitCode || statusResult.exitCode);
  }

  function rememberPostArmReboot(payload) {
    const postArm = managedReinstallPostArm(payload);
    if (postArm?.rebooting === true) {
      mcpPostArmReboot = {
        ...postArm,
        createdAt: Date.now()
      };
      return mcpPostArmReboot;
    }
    return null;
  }

  function managedReinstallPostArm(payload) {
    const job = payload?.job && typeof payload.job === "object" ? payload.job : payload;
    const phase = String(payload?.phase || payload?.result?.phase || job?.phase || job?.kind || "").toLowerCase();
    const family = String(payload?.family || payload?.result?.family || job?.family || "").toLowerCase();
    if (family !== "windows-reinstall" && phase !== "arm") {
      return null;
    }
    const parsed = parseJsonObject(payload?.result?.output?.tail || payload?.text || payload?.output?.tail || "");
    const armResult = parsed?.action === "arm" && parsed?.result && typeof parsed.result === "object"
      ? parsed.result
      : parsed?.rebooting === true
        ? parsed
        : null;
    if (armResult?.rebooting !== true) {
      return null;
    }
    const backupProof = armResult.backupProof && typeof armResult.backupProof === "object" ? armResult.backupProof : {};
    return {
      rebooting: true,
      caseId: cleanActionText(armResult.caseId || "", 80),
      backupProofOk: backupProof.ok === true,
      backupRootExists: backupProof.backupRootExists === true,
      wifiProfileCount: Number.isSafeInteger(backupProof.wifiProfileCount) ? backupProof.wifiProfileCount : undefined,
      driverInfCount: Number.isSafeInteger(backupProof.driverInfCount) ? backupProof.driverInfCount : undefined,
      rootAutounattend: backupProof.rootAutounattend === true,
      oemSetupComplete: backupProof.oemSetupComplete === true
    };
  }

  async function maybeRedirectManagedReinstallProbe(toolName, text) {
    if (!looksLikeManagedReinstallProbe(text)) {
      return null;
    }
    const statusResult = await readSotyReinstallStatus(managedReinstallStatusRequest());
    const liveStatus = parseReinstallStatusResult(statusResult);
    const body = {
      ok: Boolean(liveStatus),
      redirected: true,
      blockedTool: toolName,
      reason: "managed-reinstall-toolkit-required",
      text: liveStatus
        ? "Managed reinstall status returned by soty_reinstall; generic filesystem/script polling was skipped."
        : "Generic reinstall probing was skipped, but soty_reinstall status did not return structured status.",
      liveStatus: liveStatus || mcpOperatorPayload(statusResult),
      nextTool: {
        name: "soty_reinstall",
        args: {
          action: "status",
          timeoutMs: 45_000
        }
      },
      agentGuidance: "Continue only with soty_reinstall status/prepare/arm for this reinstall flow; do not retry generic script/file/run probes for WindowsReinstall artifacts."
    };
    return mcpToolJson(body, !liveStatus, statusResult.exitCode);
  }

  function looksLikeManagedReinstallProbe(text) {
    const lower = String(text || "").toLowerCase();
    return lower.includes("programdata\\soty\\windowsreinstall")
      || lower.includes("programdata/soty/windowsreinstall")
      || lower.includes("soty\\windowsreinstall")
      || lower.includes("soty/windowsreinstall")
      || lower.includes("soty-managed-windows-reinstall")
      || lower.includes("backup-proof.json")
      || lower.includes("ready.json")
      || lower.includes("autounattend.xml")
      || lower.includes("setupcomplete.cmd")
      || lower.includes(".esd.download")
      || lower.includes("windows11_25h2_clientconsumer");
  }

  function parseReinstallStatusResult(result) {
    const parsed = parseJsonObject(result?.text || result?.payload?.text || "");
    return parsed && parsed.action === "status" ? parsed : null;
  }

  function evaluateReinstallPrepareTerminal(status, initial, elapsedMs) {
    const readyBlockers = reinstallReadyBlockers(status);
    if (status?.ready === true && readyBlockers.length === 0) {
      return {
        ok: true,
        action: "prepare",
        status: "needs-confirmation",
        terminalReason: "user-confirmation-required",
        text: "Preparation is complete. Ask the user for the exact confirmation phrase before wiping the Windows disk.",
        exitCode: 0,
        elapsedMs,
        confirmationPhrase: String(status.confirmationPhrase || ""),
        initial,
        statusSnapshot: status
      };
    }
    if (status?.ready === true && readyBlockers.length > 0) {
      return {
        ok: false,
        action: "prepare",
        status: "blocked",
        blocker: "ready-proof-incomplete",
        blockers: readyBlockers,
        text: "Preparation reported ready, but required proof is incomplete.",
        exitCode: 1,
        elapsedMs,
        initial,
        statusSnapshot: status
      };
    }
    if (isReinstallPrepareActive(status)) {
      return null;
    }
    const latest = status?.latestPrepare && typeof status.latestPrepare === "object" ? status.latestPrepare : null;
    const latestStatus = String(latest?.status || "").toLowerCase();
    if (latest && latestStatus && latestStatus !== "running-or-started" && latestStatus !== "running" && latestStatus !== "created") {
      return {
        ok: false,
        action: "prepare",
        status: "blocked",
        blocker: "prepare-job-finished-without-ready",
        text: "Preparation stopped before producing ready proof.",
        exitCode: Number.isSafeInteger(latest.exitCode) ? latest.exitCode : 1,
        elapsedMs,
        latestPrepare: latest,
        initial,
        statusSnapshot: status
      };
    }
    return null;
  }

  function isReinstallPrepareActive(status) {
    const media = status?.media && typeof status.media === "object" ? status.media : null;
    const mediaActive = media?.downloading === true && (
      media?.active === true
      || (Number.isFinite(Number(media?.updatedAgeSeconds)) && Number(media.updatedAgeSeconds) < 900)
    );
    if (mediaActive) {
      return true;
    }
    const latest = status?.latestPrepare && typeof status.latestPrepare === "object" ? status.latestPrepare : null;
    const latestStatus = String(latest?.status || "").toLowerCase();
    return latestStatus === "running-or-started" || latestStatus === "running" || latestStatus === "created";
  }

  function reinstallReadyBlockers(status) {
    const blockers = [];
    if (String(status?.managedUserName || "") !== "Соты") {
      blockers.push("managed-user-name");
    }
    if (String(status?.managedUserPasswordMode || "") !== "blank-no-password") {
      blockers.push("managed-user-password-mode");
    }
    if (status?.backupProofOk !== true) {
      blockers.push("backup-proof");
    }
    if (!String(status?.installImage || "")) {
      blockers.push("install-image");
    }
    if (status?.rootAutounattend !== true) {
      blockers.push("autounattend");
    }
    if (status?.oemSetupComplete !== true) {
      blockers.push("setupcomplete");
    }
    return blockers;
  }

  function reinstallProgressIntervalMs(status) {
    return status?.media?.downloading === true ? 30 * 60_000 : 20 * 60_000;
  }

  function reinstallPollDelayMs(status) {
    return status?.media?.downloading === true ? 120_000 : 60_000;
  }

  function formatReinstallPrepareProgress(status) {
    const media = status?.media && typeof status.media === "object" ? status.media : null;
    if (media?.downloading === true) {
      const gb = Number.isFinite(Number(media.gb)) ? `, скачано примерно ${media.gb} ГБ` : "";
      const active = media.active === true ? "процесс скачивания жив" : "докачка сохранена и будет продолжена";
      return `Подготовка идёт: образ Windows${gb}, ${active}. Диск Windows не трогаю.`;
    }
    const latest = status?.latestPrepare && typeof status.latestPrepare === "object" ? status.latestPrepare : null;
    if (latest?.stdoutTail && /backup|driver|robocopy|export/iu.test(String(latest.stdoutTail))) {
      return "Подготовка идёт: резервная копия и установочные файлы. Диск Windows не трогаю.";
    }
    return "Подготовка идёт. Диск Windows не трогаю.";
  }

  async function postMcpAgentProgress(text) {
    const clean = String(text || "").trim().slice(0, 1000);
    if (!clean) {
      return;
    }
    await mcpRequestOperator("POST", "/operator/agent-message", {
      target: mcpTarget,
      sourceDeviceId: mcpSourceDeviceId,
      text: clean,
      timeoutMs: 20_000
    }).catch(() => undefined);
  }

  async function mcpPostOperator(path, body) {
    const result = await mcpRequestOperator("POST", path, body);
    const payload = result.payload || {};
    return {
      ok: Boolean(result.ok),
      text: String(payload.text || ""),
      exitCode: Number.isSafeInteger(payload.exitCode) ? payload.exitCode : result.exitCode,
      payload
    };
  }

  function mcpToolOperatorResult(result, fallbackText = "") {
    if (result.ok) {
      return mcpToolText(result.text || fallbackText, false, result.exitCode);
    }
    return mcpToolJson(result.payload || result, true, result.exitCode);
  }

  async function mcpRequestOperator(method, path, body = undefined) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        method,
        headers: {
          "Content-Type": "application/json",
          Origin: "https://xn--n1afe0b.online"
        },
        ...(body === undefined ? {} : {
          body: JSON.stringify({
            ...body,
            ...(mcpSourceRelayId ? { sourceRelayId: mcpSourceRelayId } : {})
          })
        })
      });
      const payload = await response.json().catch(() => ({}));
      return {
        ok: Boolean(response.ok && payload?.ok),
        payload,
        exitCode: Number.isSafeInteger(payload?.exitCode) ? payload.exitCode : (response.ok ? 0 : response.status)
      };
    } catch (error) {
      return {
        ok: false,
        payload: { ok: false, text: error instanceof Error ? error.message : String(error) },
        exitCode: 1
      };
    }
  }

  function mcpToolText(text, isError = false, exitCode = 0) {
    const body = String(text || "").trim() || (isError ? "!" : "ok");
    return {
      content: [
        {
          type: "text",
          text: `${body}${Number.isSafeInteger(exitCode) ? `\nexitCode=${exitCode}` : ""}`
        }
      ],
      isError: Boolean(isError)
    };
  }

  function mcpToolJson(value, isError = false, exitCode = 0) {
    const payload = value && typeof value === "object" ? value : { text: String(value || ""), exitCode };
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(payload, null, 2)
        }
      ],
      isError: Boolean(isError)
    };
  }

  function mcpToolJsonText(result) {
    if (!result?.ok && result?.payload && typeof result.payload === "object") {
      return mcpToolJson(result.payload, true, result.exitCode);
    }
    const text = String(result?.text || "").trim();
    const parsed = parseJsonObject(text);
    if (parsed) {
      return mcpToolJson(parsed, !result.ok || parsed.ok === false, result.exitCode);
    }
    return mcpToolText(text, !result.ok, result.exitCode);
  }

  function parseJsonObject(value) {
    const text = String(value || "").trim();
    if (!text) {
      return null;
    }
    try {
      return JSON.parse(text);
    } catch {}
    const start = text.indexOf("{");
    const end = text.lastIndexOf("}");
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(text.slice(start, end + 1));
      } catch {}
    }
    return null;
  }

  function mcpSafeTimeout(value, fallback) {
    return Number.isSafeInteger(value) ? Math.max(1000, Math.min(value, maxLongTaskTimeoutMs)) : fallback;
  }

  function normalizeUsbDriveLetter(value) {
    const letter = String(value || "D").trim().replace(/[:\\/\s]+/gu, "").toUpperCase();
    return /^[A-Z]$/u.test(letter) ? letter : "D";
  }

function quotePowerShell(value) {
  return `'${String(value).replace(/'/gu, "''")}'`;
}

function sourceOpenUrlScript(url) {
  const payload = Buffer.from(JSON.stringify({ url: String(url || "").slice(0, 4000) }), "utf8").toString("base64");
  return `
const { spawn } = require("node:child_process");
const req = JSON.parse(Buffer.from("${payload}", "base64").toString("utf8"));
const url = String(req.url || "");
if (!/^https?:\\/\\//i.test(url)) {
  console.error("invalid url");
  process.exit(2);
}
const command = process.platform === "win32"
  ? { file: "cmd.exe", args: ["/d", "/s", "/c", "start", "", url] }
  : process.platform === "darwin"
    ? { file: "open", args: [url] }
    : { file: "xdg-open", args: [url] };
const child = spawn(command.file, command.args, { detached: true, stdio: "ignore", windowsHide: false });
child.unref();
console.log(JSON.stringify({ ok: true, action: "open", url, platform: process.platform }));
`.trim();
}

function sendMcp(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}
}

async function generateOpenAiImageData(args) {
  const prompt = String(args?.prompt || "").trim();
  if (!prompt) {
    return { ok: false, error: "prompt is required", exitCode: 2 };
  }
  const apiKey = String(process.env.SOTY_OPENAI_API_KEY || process.env.OPENAI_API_KEY || process.env.CODEX_API_KEY || "").trim();
  if (!apiKey) {
    return {
      ok: false,
      error: "OPENAI_API_KEY is not available to the Soty Agent process",
      exitCode: 78
    };
  }
  const model = safeImageModel(args?.model || process.env.SOTY_IMAGE_MODEL || "gpt-image-1.5");
  const size = safeImageSize(args?.size || "auto");
  const quality = safeImageQuality(args?.quality || "auto");
  const format = safeImageFormat(args?.format || "png");
  const background = safeImageBackground(args?.background || "");
  const body = {
    model,
    prompt: prompt.slice(0, 16000),
    size,
    quality,
    output_format: format
  };
  if (background) {
    body.background = background;
  }
  const controller = new AbortController();
  const timeoutMs = mcpSafeImageTimeout(args?.timeoutMs);
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let response;
  let payload;
  try {
    response = await fetch("https://api.openai.com/v1/images/generations", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body),
      signal: controller.signal
    });
    payload = await response.json().catch(() => ({}));
  } catch (error) {
    clearTimeout(timer);
    return {
      ok: false,
      error: error?.name === "AbortError" ? "image generation timed out" : cleanImageError(error),
      model,
      size,
      quality,
      format,
      exitCode: error?.name === "AbortError" ? 124 : 1
    };
  } finally {
    clearTimeout(timer);
  }
  if (!response.ok) {
    return {
      ok: false,
      error: cleanImageError(payload?.error?.message || payload?.error || `OpenAI image request failed with HTTP ${response.status}`),
      status: response.status,
      model,
      size,
      quality,
      format,
      exitCode: response.status
    };
  }
  const imageBase64 = String(payload?.data?.[0]?.b64_json || "");
  if (!imageBase64) {
    return {
      ok: false,
      error: "OpenAI image response did not include b64_json",
      model,
      size,
      quality,
      format,
      exitCode: 1
    };
  }
  const bytes = Buffer.from(imageBase64, "base64");
  return {
    ok: true,
    action: "image",
    imageBase64,
    bytes: bytes.length,
    model,
    size,
    quality,
    format,
    revisedPrompt: String(payload?.data?.[0]?.revised_prompt || "").slice(0, 2000)
  };
}

async function saveGeneratedImageLocal(image, args) {
  const outputPath = resolveImageOutputPath(args?.path || "", image.format || "png");
  const bytes = Buffer.from(String(image?.imageBase64 || ""), "base64");
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, bytes);
  return {
    path: outputPath,
    bytes: bytes.length
  };
}

function imageResultPublic(image) {
  return {
    ok: Boolean(image?.ok),
    action: "image",
    path: String(image?.path || ""),
    bytes: Number.isSafeInteger(image?.bytes) ? image.bytes : 0,
    model: String(image?.model || ""),
    size: String(image?.size || ""),
    quality: String(image?.quality || ""),
    format: String(image?.format || ""),
    savedBy: String(image?.savedBy || ""),
    revisedPrompt: String(image?.revisedPrompt || "").slice(0, 2000)
  };
}

function sourceSaveGeneratedImageScript(args) {
  const payload = Buffer.from(JSON.stringify({
    imageBase64: String(args?.imageBase64 || ""),
    path: String(args?.path || "").slice(0, 2000),
    format: safeImageFormat(args?.format || "png")
  }), "utf8").toString("base64");
  return `
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const req = JSON.parse(Buffer.from("${payload}", "base64").toString("utf8"));
function desktopDir() {
  const candidates = [
    process.env.OneDrive ? path.join(process.env.OneDrive, "Desktop") : "",
    process.env.USERPROFILE ? path.join(process.env.USERPROFILE, "OneDrive", "Desktop") : "",
    path.join(os.homedir(), "OneDrive", "Desktop"),
    path.join(os.homedir(), "Desktop")
  ].filter(Boolean);
  return candidates.find((item) => fs.existsSync(item)) || path.join(os.homedir(), "Desktop");
}
function outputPath() {
  const raw = String(req.path || "").trim();
  const ext = ["png", "jpeg", "webp"].includes(String(req.format || "").toLowerCase()) ? String(req.format).toLowerCase() : "png";
  if (raw) {
    const expanded = raw.replace(/^~(?=$|[\\\\/])/, os.homedir());
    const resolved = path.resolve(expanded);
    return /\\.[A-Za-z0-9]{2,5}$/.test(resolved) ? resolved : resolved + "." + ext;
  }
  const name = "soty-image-" + new Date().toISOString().replace(/[:.]/g, "-") + "." + ext;
  return path.join(desktopDir(), name);
}
const out = outputPath();
const bytes = Buffer.from(String(req.imageBase64 || ""), "base64");
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, bytes);
console.log(JSON.stringify({ ok: true, path: out, bytes: bytes.length }));
`.trim();
}

function mcpSafeImageTimeout(value) {
  return Number.isSafeInteger(value) ? Math.max(30_000, Math.min(value, maxLongTaskTimeoutMs)) : 300_000;
}

function safeImageModel(value) {
  const text = String(value || "").trim();
  return /^gpt-image-[A-Za-z0-9_.-]+$/u.test(text) ? text : "gpt-image-1.5";
}

function safeImageSize(value) {
  const text = String(value || "").trim().toLowerCase();
  return ["auto", "1024x1024", "1536x1024", "1024x1536"].includes(text) ? text : "auto";
}

function safeImageQuality(value) {
  const text = String(value || "").trim().toLowerCase();
  return ["auto", "low", "medium", "high"].includes(text) ? text : "auto";
}

function safeImageFormat(value) {
  const text = String(value || "").trim().toLowerCase();
  return ["png", "jpeg", "webp"].includes(text) ? text : "png";
}

function safeImageBackground(value) {
  const text = String(value || "").trim().toLowerCase();
  return ["auto", "opaque", "transparent"].includes(text) ? text : "";
}

function resolveImageOutputPath(value, format) {
  const raw = String(value || "").trim();
  if (raw) {
    const resolved = resolve(raw.replace(/^~(?=$|[\\/])/u, homedir()));
    const ext = safeImageFormat(format);
    return /\.[A-Za-z0-9]{2,5}$/u.test(resolved) ? resolved : `${resolved}.${ext}`;
  }
  const desktop = defaultDesktopDir();
  const name = `soty-image-${new Date().toISOString().replace(/[:.]/gu, "-")}.${safeImageFormat(format)}`;
  return join(desktop, name);
}

function defaultDesktopDir() {
  const candidates = [
    process.env.OneDrive ? join(process.env.OneDrive, "Desktop") : "",
    process.env.USERPROFILE ? join(process.env.USERPROFILE, "OneDrive", "Desktop") : "",
    join(homedir(), "OneDrive", "Desktop"),
    join(homedir(), "Desktop")
  ].filter(Boolean);
  return candidates.find((candidate) => existsSync(candidate)) || join(homedir(), "Desktop");
}

function cleanImageError(value) {
  return String(value?.message || value || "image generation failed")
    .replace(/sk-[A-Za-z0-9_-]+/gu, "sk-***")
    .slice(0, 500);
}

function windowsAudioScript(volumePercent, muteMode) {
  const safeVolume = Number.isFinite(volumePercent) ? Math.max(-1, Math.min(100, Math.round(volumePercent))) : -1;
  const safeMute = muteMode === 1 ? 1 : muteMode === 0 ? 0 : safeVolume >= 0 ? 0 : -1;
  return `
$ErrorActionPreference = 'Stop'
$code = @"
using System;
using System.Runtime.InteropServices;
namespace SotyAudio {
  [ComImport, Guid("BCDE0395-E52F-467C-8E3D-C4579291692E")] class MMDeviceEnumerator {}
  enum EDataFlow { eRender = 0, eCapture = 1, eAll = 2 }
  enum ERole { eConsole = 0, eMultimedia = 1, eCommunications = 2 }
  [ComImport, Guid("A95664D2-9614-4F35-A746-DE8DB63617E6"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
  interface IMMDeviceEnumerator { int NotImpl1(); [PreserveSig] int GetDefaultAudioEndpoint(EDataFlow dataFlow, ERole role, out IMMDevice ppDevice); }
  [ComImport, Guid("D666063F-1587-4E43-81F1-B948E807363F"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
  interface IMMDevice { [PreserveSig] int Activate(ref Guid iid, int dwClsCtx, IntPtr pActivationParams, [MarshalAs(UnmanagedType.Interface)] out IAudioEndpointVolume ppInterface); }
  [ComImport, Guid("5CDF2C82-841E-4546-9722-0CF74078229A"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
  interface IAudioEndpointVolume {
    int RegisterControlChangeNotify(IntPtr pNotify); int UnregisterControlChangeNotify(IntPtr pNotify); int GetChannelCount(out uint pnChannelCount);
    int SetMasterVolumeLevel(float fLevelDB, ref Guid pguidEventContext); int SetMasterVolumeLevelScalar(float fLevel, ref Guid pguidEventContext);
    int GetMasterVolumeLevel(out float pfLevelDB); int GetMasterVolumeLevelScalar(out float pfLevel);
    int SetChannelVolumeLevel(uint nChannel, float fLevelDB, ref Guid pguidEventContext); int SetChannelVolumeLevelScalar(uint nChannel, float fLevel, ref Guid pguidEventContext);
    int GetChannelVolumeLevel(uint nChannel, out float pfLevelDB); int GetChannelVolumeLevelScalar(uint nChannel, out float pfLevel);
    int SetMute([MarshalAs(UnmanagedType.Bool)] bool bMute, ref Guid pguidEventContext); int GetMute(out bool pbMute);
    int GetVolumeStepInfo(out uint pnStep, out uint pnStepCount); int VolumeStepUp(ref Guid pguidEventContext); int VolumeStepDown(ref Guid pguidEventContext);
    int QueryHardwareSupport(out uint pdwHardwareSupportMask); int GetVolumeRange(out float pflVolumeMindB, out float pflVolumeMaxdB, out float pflVolumeIncrementdB);
  }
  public static class Endpoint {
    static IAudioEndpointVolume DefaultRender() {
      var enumerator = (IMMDeviceEnumerator)(new MMDeviceEnumerator());
      IMMDevice device; int hr = enumerator.GetDefaultAudioEndpoint(EDataFlow.eRender, ERole.eMultimedia, out device);
      if (hr != 0) Marshal.ThrowExceptionForHR(hr);
      Guid iid = typeof(IAudioEndpointVolume).GUID; IAudioEndpointVolume endpoint;
      hr = device.Activate(ref iid, 23, IntPtr.Zero, out endpoint);
      if (hr != 0) Marshal.ThrowExceptionForHR(hr);
      return endpoint;
    }
    public static string Apply(int volume, int muteMode) {
      var ep = DefaultRender();
      Guid ctx = Guid.Empty;
      if (volume >= 0) ep.SetMasterVolumeLevelScalar(Math.Max(0, Math.Min(100, volume)) / 100.0f, ref ctx);
      if (muteMode == 1) ep.SetMute(true, ref ctx);
      if (muteMode == 0) ep.SetMute(false, ref ctx);
      float level; bool muted; ep.GetMasterVolumeLevelScalar(out level); ep.GetMute(out muted);
      return String.Format("volume={0}; muted={1}", (int)Math.Round(level * 100), muted.ToString().ToLowerInvariant());
    }
  }
}
"@
Add-Type -TypeDefinition $code -Language CSharp
[SotyAudio.Endpoint]::Apply(${safeVolume}, ${safeMute})
`.trim();
}

function isAudioTimeoutResult(result) {
  return result?.exitCode === 124 || /(^|\n)!\s*timeout\b/iu.test(String(result?.text || ""));
}

function scheduleWindowsAudioWarmup() {
  if (process.platform !== "win32" || audioWarmupStarted) {
    return;
  }
  audioWarmupStarted = true;
  setTimeout(() => {
    runWindowsAudioWarmup();
  }, 1500);
}

function runWindowsAudioWarmup() {
  const child = spawn("powershell.exe", [
    "-NoLogo",
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-Command",
    windowsAudioScript(-1, -1)
  ], {
    cwd: process.cwd(),
    env: process.env,
    windowsHide: true,
    stdio: "ignore"
  });
  const timer = setTimeout(() => {
    killProcessTree(child);
  }, audioWarmupTimeoutMs);
  child.on("error", () => clearTimeout(timer));
  child.on("close", () => clearTimeout(timer));
}

function sourceManagedWindowsReinstallBootstrap(args) {
  const payload = Buffer.from(JSON.stringify({
    action: String(args.action || "status").slice(0, 40),
    usbDriveLetter: String(args.usbDriveLetter || "D").slice(0, 8),
    confirmationPhrase: String(args.confirmationPhrase || "").slice(0, 300),
    useExistingUsbInstallImage: args.useExistingUsbInstallImage === true,
    manifestUrl: String(args.manifestUrl || updateManifestUrl).slice(0, 4000),
    panelSiteUrl: String(args.panelSiteUrl || originFromUrl(updateManifestUrl) || "https://xn--n1afe0b.online").slice(0, 4000),
    workspaceRoot: String(args.workspaceRoot || "C:\\ProgramData\\Soty\\WindowsReinstall").slice(0, 1000)
  }), "utf8").toString("base64");
  return `
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
try {
  [Console]::InputEncoding = [System.Text.Encoding]::UTF8
  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
  $OutputEncoding = [System.Text.Encoding]::UTF8
  chcp.com 65001 > $null
} catch {}
$req = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('${payload}')) | ConvertFrom-Json
function Emit($Value, [int]$Code = 0) {
  $Value | ConvertTo-Json -Depth 16 -Compress
  exit $Code
}
function New-Dir([string]$Path) {
  if (-not [string]::IsNullOrWhiteSpace($Path)) { New-Item -ItemType Directory -Force -Path $Path | Out-Null }
}
function Get-ManagedScript([string]$WorkspaceRoot) {
  $manifestUrl = [string]$req.manifestUrl
  if ([string]::IsNullOrWhiteSpace($manifestUrl)) { throw 'manifestUrl is empty' }
  $manifest = Invoke-RestMethod -Uri $manifestUrl -UseBasicParsing -TimeoutSec 30 -ErrorAction Stop
  $scriptSpec = @($manifest.windowsReinstall.scripts | Where-Object { [string]$_.name -eq 'managed' } | Select-Object -First 1)
  if (-not $scriptSpec) { throw 'manifest missing windowsReinstall managed script' }
  if ([string]::IsNullOrWhiteSpace([string]$scriptSpec.url) -or [string]::IsNullOrWhiteSpace([string]$scriptSpec.sha256)) {
    throw 'manifest managed script is incomplete'
  }
  $downloadRoot = Join-Path $WorkspaceRoot 'downloads\\manifest-scripts'
  New-Dir $downloadRoot
  $baseUri = New-Object System.Uri -ArgumentList $manifestUrl
  $scriptUri = New-Object System.Uri -ArgumentList $baseUri, ([string]$scriptSpec.url)
  $path = Join-Path $downloadRoot (Split-Path -Leaf ([string]$scriptSpec.url))
  $expected = ([string]$scriptSpec.sha256).ToLowerInvariant()
  if (Test-Path -LiteralPath $path) {
    $cached = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant()
    if ($cached -eq $expected) {
      return [pscustomobject]@{ path = $path; url = $scriptUri.AbsoluteUri; sha256 = $cached; bytes = (Get-Item -LiteralPath $path).Length; cached = $true }
    }
  }
  Invoke-WebRequest -Uri $scriptUri.AbsoluteUri -UseBasicParsing -OutFile $path -TimeoutSec 120 -ErrorAction Stop
  $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant()
  if ($actual -ne $expected) { throw ('SHA256 mismatch for managed script: expected=' + $expected + ' actual=' + $actual) }
  return [pscustomobject]@{ path = $path; url = $scriptUri.AbsoluteUri; sha256 = $actual; bytes = (Get-Item -LiteralPath $path).Length; cached = $false }
}
try {
  $workspaceRoot = [string]$req.workspaceRoot
  if ([string]::IsNullOrWhiteSpace($workspaceRoot)) { $workspaceRoot = 'C:\\ProgramData\\Soty\\WindowsReinstall' }
  New-Dir $workspaceRoot
  $managed = Get-ManagedScript $workspaceRoot
  $psArgs = @(
    '-NoLogo', '-NoProfile', '-ExecutionPolicy', 'Bypass',
    '-File', $managed.path,
    '-Action', ([string]$req.action),
    '-WorkspaceRoot', $workspaceRoot,
    '-UsbDriveLetter', ([string]$req.usbDriveLetter),
    '-ManifestUrl', ([string]$req.manifestUrl),
    '-PanelSiteUrl', ([string]$req.panelSiteUrl)
  )
  if ([bool]$req.useExistingUsbInstallImage) { $psArgs += '-UseExistingUsbInstallImage' }
  if (-not [string]::IsNullOrWhiteSpace([string]$req.confirmationPhrase)) { $psArgs += @('-ConfirmationPhrase', [string]$req.confirmationPhrase) }
  $output = & powershell.exe @psArgs 2>&1
  $code = if ($null -ne $global:LASTEXITCODE) { [int]$global:LASTEXITCODE } else { 0 }
  $text = ($output | Out-String).Trim()
  $parsed = $null
  try { if ($text) { $parsed = $text | ConvertFrom-Json -ErrorAction Stop } } catch {}
  if ($parsed) {
    $parsed | Add-Member -NotePropertyName managedScript -NotePropertyValue $managed -Force
    Emit $parsed $code
  }
  Emit ([pscustomobject]@{ ok = ($code -eq 0); action = [string]$req.action; managedScript = $managed; text = $text }) $code
} catch {
  Emit ([pscustomobject]@{ ok = $false; action = [string]$req.action; error = $_.Exception.Message }) 1
}
`.trim();
}

function sourceManagedWindowsReinstallScript(args) {
  return sourceManagedWindowsReinstallBootstrap(args);
}

function sourceFileScript(args) {
  const payload = Buffer.from(JSON.stringify({
    action: String(args.action || "").slice(0, 40),
    path: String(args.path || "").slice(0, 2000),
    toPath: String(args.toPath || "").slice(0, 2000),
    content: String(args.content || "").slice(0, 300_000),
    pattern: String(args.pattern || "").slice(0, 2000),
    glob: String(args.glob || "").slice(0, 200),
    regex: args.regex === true,
    recursive: args.recursive === true,
    maxResults: Number.isSafeInteger(args.maxResults) ? Math.max(1, Math.min(args.maxResults, 500)) : 80,
    maxChars: Number.isSafeInteger(args.maxChars) ? Math.max(1000, Math.min(args.maxChars, 12000)) : 9000
  }), "utf8").toString("base64");
  return `
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const req = JSON.parse(Buffer.from("${payload}", "base64").toString("utf8"));
const emit = (value) => console.log(JSON.stringify(value));
function expandPath(value) {
  let text = String(value || "").trim();
  if (!text) throw new Error("empty path");
  if (text === "~" || text.startsWith("~/") || text.startsWith("~\\\\")) {
    text = path.join(os.homedir(), text.slice(2));
  }
  text = text
    .replace(/%([^%]+)%/g, (_, name) => process.env[name] || "")
    .replace(/\\$\\{([^}]+)\\}|\\$([A-Za-z_][A-Za-z0-9_]*)/g, (_, braced, plain) => process.env[braced || plain] || "");
  return path.resolve(text);
}
function itemInfo(fullPath, name = path.basename(fullPath)) {
  const stat = fs.statSync(fullPath);
  return {
    name,
    path: fullPath,
    type: stat.isDirectory() ? "directory" : "file",
    length: stat.isDirectory() ? 0 : stat.size,
    updated: stat.mtime.toISOString()
  };
}
function wildcardToRegExp(glob) {
  const escaped = String(glob || "*").replace(/[.+^$(){}|[\\]\\\\]/g, "\\\\$&").replace(/\\*/g, ".*").replace(/\\?/g, ".");
  return new RegExp("^" + escaped + "$", "i");
}
function listFiles(root, recursive, limit, out = []) {
  if (out.length >= limit) return out;
  const stat = fs.statSync(root);
  if (!stat.isDirectory()) {
    out.push(root);
    return out;
  }
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    if (out.length >= limit) break;
    const full = path.join(root, entry.name);
    if (entry.isDirectory()) {
      if (recursive) listFiles(full, true, limit, out);
    } else if (entry.isFile()) {
      out.push(full);
    }
  }
  return out;
}
let action = "";
let fullPath = "";
try {
  action = String(req.action || "").trim().toLowerCase();
  fullPath = expandPath(req.path);
  const maxResults = Math.max(1, Math.min(500, Number(req.maxResults) || 80));
  const maxChars = Math.max(1000, Math.min(12000, Number(req.maxChars) || 9000));
  if (action === "stat") {
    emit({ ok: true, action, ...itemInfo(fullPath) });
  } else if (action === "list") {
    const stat = fs.statSync(fullPath);
    const entries = stat.isDirectory()
      ? (req.recursive ? listFiles(fullPath, true, maxResults) : fs.readdirSync(fullPath).slice(0, maxResults).map((name) => path.join(fullPath, name)))
      : [fullPath];
    emit({ ok: true, action, path: fullPath, items: entries.map((entry) => itemInfo(entry)) });
  } else if (action === "read") {
    const text = fs.readFileSync(fullPath, "utf8").slice(0, maxChars);
    emit({ ok: true, action, path: fullPath, text });
  } else if (action === "write" || action === "append") {
    fs.mkdirSync(path.dirname(fullPath), { recursive: true });
    if (action === "write") fs.writeFileSync(fullPath, String(req.content || ""), "utf8");
    else fs.appendFileSync(fullPath, String(req.content || ""), "utf8");
    emit({ ok: true, action, path: fullPath, bytes: Buffer.byteLength(String(req.content || ""), "utf8") });
  } else if (action === "mkdir") {
    fs.mkdirSync(fullPath, { recursive: true });
    emit({ ok: true, action, path: fullPath });
  } else if (action === "move" || action === "copy") {
    const toPath = expandPath(req.toPath);
    fs.mkdirSync(path.dirname(toPath), { recursive: true });
    if (action === "move") fs.renameSync(fullPath, toPath);
    else fs.cpSync(fullPath, toPath, { recursive: req.recursive === true, force: true });
    emit({ ok: true, action, path: fullPath, toPath });
  } else if (action === "delete") {
    fs.rmSync(fullPath, { recursive: req.recursive === true, force: true });
    emit({ ok: true, action, path: fullPath });
  } else if (action === "search") {
    const pattern = String(req.pattern || "");
    if (!pattern.trim()) throw new Error("empty pattern");
    const glob = wildcardToRegExp(req.glob || "*");
    const matcher = req.regex ? new RegExp(pattern, "iu") : null;
    const files = listFiles(fullPath, true, maxResults * 20).filter((file) => glob.test(path.basename(file)));
    const matches = [];
    for (const file of files) {
      if (matches.length >= maxResults) break;
      let text = "";
      try { text = fs.readFileSync(file, "utf8"); } catch { continue; }
      const lines = text.split(/\\r?\\n/u);
      for (let index = 0; index < lines.length && matches.length < maxResults; index += 1) {
        const line = lines[index];
        if (matcher ? matcher.test(line) : line.includes(pattern)) {
          matches.push({ path: file, line: index + 1, text: line.trim().slice(0, 1000) });
        }
      }
    }
    emit({ ok: true, action, path: fullPath, pattern, matches });
  } else {
    throw new Error("unsupported file action: " + action);
  }
} catch (error) {
  emit({ ok: false, action, path: fullPath, error: error && error.message ? error.message : String(error) });
  process.exit(1);
}
`.trim();
}

function sourceBrowserScript(args) {
  const request = Buffer.from(JSON.stringify({
    action: String(args.action || "").slice(0, 40),
    url: String(args.url || "").slice(0, 4000),
    script: String(args.script || "").slice(0, 20000),
    text: String(args.text || "").slice(0, 4000),
    selector: String(args.selector || "").slice(0, 1000),
    headless: args.headless === true,
    maxChars: Number.isSafeInteger(args.maxChars) ? Math.max(1000, Math.min(args.maxChars, 12000)) : 9000
  }), "utf8").toString("base64");
  const driver = Buffer.from(`
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn, spawnSync } = require("node:child_process");
const req = JSON.parse(Buffer.from("${request}", "base64").toString("utf8"));
const port = 9222;
const base = "http://127.0.0.1:" + port;
let nextId = 1;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function fetchJson(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error("http " + res.status + " " + url);
  return await res.json();
}
function browserCandidates() {
  if (process.platform === "win32") {
    const roots = [process.env["ProgramFiles"], process.env["ProgramFiles(x86)"], process.env.LOCALAPPDATA].filter(Boolean);
    const suffixes = [
      "Microsoft/Edge/Application/msedge.exe",
      "Google/Chrome/Application/chrome.exe"
    ];
    return roots.flatMap((root) => suffixes.map((suffix) => path.join(root, suffix)));
  }
  if (process.platform === "darwin") {
    return ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"];
  }
  return ["google-chrome", "microsoft-edge", "chromium", "chromium-browser"];
}
function executableExists(candidate) {
  if (process.platform === "win32" || candidate.includes("/")) {
    return fs.existsSync(candidate);
  }
  return spawnSync("sh", ["-lc", "command -v " + candidate], { stdio: "ignore" }).status === 0;
}
function openDefaultBrowser(url) {
  if (!/^https?:\\/\\//i.test(String(url || ""))) return false;
  const command = process.platform === "win32"
    ? { file: "cmd.exe", args: ["/d", "/s", "/c", "start", "", url] }
    : process.platform === "darwin"
      ? { file: "open", args: [url] }
      : { file: "xdg-open", args: [url] };
  try {
    const child = spawn(command.file, command.args, { detached: true, stdio: "ignore", windowsHide: false });
    child.unref();
    return true;
  } catch {
    return false;
  }
}
async function ensureBrowser() {
  try {
    await fetchJson(base + "/json/version");
    return;
  } catch {}
  const exe = browserCandidates().find(executableExists) || browserCandidates()[0];
  const profile = path.join(os.tmpdir(), "soty-browser-profile");
  fs.mkdirSync(profile, { recursive: true });
  const args = ["--remote-debugging-port=" + port, "--user-data-dir=" + profile, "--no-first-run", "--no-default-browser-check"];
  if (req.headless) args.push("--headless=new");
  args.push(req.url || "about:blank");
  const child = spawn(exe, args, { detached: true, stdio: "ignore", windowsHide: false });
  child.unref();
  for (let i = 0; i < 50; i += 1) {
    await sleep(200);
    try {
      await fetchJson(base + "/json/version");
      return;
    } catch {}
  }
  throw new Error("browser devtools did not start");
}
async function pages() {
  return (await fetchJson(base + "/json")).filter((item) => item.type === "page" && item.webSocketDebuggerUrl);
}
async function page(preferNew = false) {
  await ensureBrowser();
  if (preferNew || req.url) {
    try {
      const created = await fetchJson(base + "/json/new?" + encodeURIComponent(req.url || "about:blank"), { method: "PUT" });
      if (created.webSocketDebuggerUrl) return created;
    } catch {
      try {
        const created = await fetchJson(base + "/json/new?" + encodeURIComponent(req.url || "about:blank"));
        if (created.webSocketDebuggerUrl) return created;
      } catch {}
    }
  }
  const list = await pages();
  if (list[0]) return list[0];
  return await fetchJson(base + "/json/new?about:blank");
}
function connect(wsUrl) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl);
    const pending = new Map();
    ws.onopen = () => resolve({
      send(method, params = {}) {
        const id = nextId++;
        ws.send(JSON.stringify({ id, method, params }));
        return new Promise((ok, bad) => pending.set(id, { ok, bad }));
      },
      close() { try { ws.close(); } catch {} }
    });
    ws.onerror = () => reject(new Error("websocket failed"));
    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (!msg.id || !pending.has(msg.id)) return;
      const item = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) item.bad(new Error(msg.error.message || "cdp error"));
      else item.ok(msg.result || {});
    };
  });
}
async function withClient(fn) {
  const p = await page(req.action === "open" || req.action === "goto");
  const client = await connect(p.webSocketDebuggerUrl);
  try {
    await client.send("Runtime.enable").catch(() => {});
    await client.send("Page.enable").catch(() => {});
    return await fn(client, p);
  } finally {
    client.close();
  }
}
async function evalText(client, expression) {
  const result = await client.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
  const value = result.result ? result.result.value : null;
  return typeof value === "string" ? value : JSON.stringify(value);
}
(async () => {
  const action = String(req.action || "").toLowerCase();
  const maxChars = Math.max(1000, Math.min(12000, Number(req.maxChars) || 9000));
  if (action === "open" || action === "goto") {
    await withClient(async (client) => {
      if (req.url) await client.send("Page.navigate", { url: req.url }).catch(() => {});
      await sleep(500);
      const text = await evalText(client, "JSON.stringify({ title: document.title, url: location.href })");
      console.log(text);
    });
    return;
  }
  if (action === "title") {
    await withClient(async (client) => console.log(await evalText(client, "JSON.stringify({ title: document.title, url: location.href })")));
    return;
  }
  if (action === "text") {
    await withClient(async (client) => console.log((await evalText(client, "document.body ? document.body.innerText : ''")).slice(0, maxChars)));
    return;
  }
  if (action === "eval") {
    await withClient(async (client) => console.log((await evalText(client, req.script || "location.href")).slice(0, maxChars)));
    return;
  }
  if (action === "click_text") {
    const needle = JSON.stringify(req.text || "");
    const expression = "(() => { const needle = " + needle + ".toLowerCase(); const all = [...document.querySelectorAll('button,a,input,textarea,select,[role=button],label,div,span')]; const el = all.find(e => (e.innerText || e.value || e.ariaLabel || '').toLowerCase().includes(needle)); if (!el) return { clicked:false }; el.scrollIntoView({block:'center', inline:'center'}); el.click(); return { clicked:true, text:(el.innerText || el.value || el.ariaLabel || '').slice(0,200) }; })()";
    await withClient(async (client) => console.log(await evalText(client, expression)));
    return;
  }
  if (action === "type") {
    const selector = JSON.stringify(req.selector || "input,textarea,[contenteditable=true]");
    const text = JSON.stringify(req.text || "");
    const expression = "(() => { const el = document.querySelector(" + selector + "); if (!el) return { typed:false }; el.focus(); if ('value' in el) { el.value = " + text + "; el.dispatchEvent(new Event('input', {bubbles:true})); el.dispatchEvent(new Event('change', {bubbles:true})); } else { el.textContent = " + text + "; el.dispatchEvent(new InputEvent('input', {bubbles:true, inputType:'insertText', data:" + text + "})); } return { typed:true }; })()";
    await withClient(async (client) => console.log(await evalText(client, expression)));
    return;
  }
  if (action === "screenshot") {
    await withClient(async (client) => {
      const shot = await client.send("Page.captureScreenshot", { format: "jpeg", quality: 60, captureBeyondViewport: false });
      const dir = path.join(os.tmpdir(), "soty-browser");
      fs.mkdirSync(dir, { recursive: true });
      const out = path.join(dir, "screenshot-" + Date.now() + ".jpg");
      fs.writeFileSync(out, Buffer.from(shot.data || "", "base64"));
      console.log(JSON.stringify({ ok: true, action, path: out, bytes: fs.statSync(out).size }));
    });
    return;
  }
  throw new Error("unsupported browser action: " + action);
})().catch((error) => {
  const action = String(req.action || "").toLowerCase();
  if ((action === "open" || action === "goto") && openDefaultBrowser(req.url)) {
    console.log(JSON.stringify({ ok: true, action, url: req.url, mode: "default-browser-fallback" }));
    return;
  }
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
`, "utf8").toString("base64");
  return Buffer.from(driver, "base64").toString("utf8");
}

function sourceDesktopScript(args) {
  const payload = Buffer.from(JSON.stringify({
    action: String(args.action || "").slice(0, 40),
    title: String(args.title || "").slice(0, 300),
    x: Number.isSafeInteger(args.x) ? args.x : 0,
    y: Number.isSafeInteger(args.y) ? args.y : 0,
    button: String(args.button || "left").slice(0, 20),
    text: String(args.text || "").slice(0, 4000),
    keys: String(args.keys || "").slice(0, 200)
  }), "utf8").toString("base64");
  return `
$ErrorActionPreference = 'Stop'
$req = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('${payload}')) | ConvertFrom-Json
$action = ([string]$req.action).Trim().ToLowerInvariant()
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
function Emit($Value) { $Value | ConvertTo-Json -Depth 6 -Compress }
switch ($action) {
  'windows' {
    $items = Get-Process | Where-Object { $_.MainWindowTitle } | Sort-Object ProcessName | Select-Object -First 80 ProcessName, Id, MainWindowTitle
    Emit ([pscustomobject]@{ ok=$true; action=$action; windows=@($items) })
  }
  'focus' {
    $title = [string]$req.title
    if ([string]::IsNullOrWhiteSpace($title)) { throw 'empty title' }
    $shell = New-Object -ComObject WScript.Shell
    $ok = $shell.AppActivate($title)
    Emit ([pscustomobject]@{ ok=[bool]$ok; action=$action; title=$title })
  }
  'screenshot' {
    $bounds = [System.Windows.Forms.SystemInformation]::VirtualScreen
    $bmp = New-Object System.Drawing.Bitmap $bounds.Width, $bounds.Height
    $graphics = [System.Drawing.Graphics]::FromImage($bmp)
    $graphics.CopyFromScreen($bounds.Left, $bounds.Top, 0, 0, $bounds.Size)
    $dir = Join-Path $env:TEMP 'soty-desktop'
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $path = Join-Path $dir ("screenshot-{0}.png" -f ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()))
    $bmp.Save($path, [System.Drawing.Imaging.ImageFormat]::Png)
    $graphics.Dispose()
    $bmp.Dispose()
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$path; width=$bounds.Width; height=$bounds.Height; bytes=(Get-Item -LiteralPath $path).Length })
  }
  'click' {
    if (-not ('SotyMouse' -as [type])) {
      Add-Type @"
using System;
using System.Runtime.InteropServices;
public class SotyMouse {
  [DllImport("user32.dll")] public static extern bool SetCursorPos(int X, int Y);
  [DllImport("user32.dll")] public static extern void mouse_event(uint flags, uint dx, uint dy, uint data, UIntPtr extra);
}
"@
    }
    $x = [int]$req.x
    $y = [int]$req.y
    [SotyMouse]::SetCursorPos($x, $y) | Out-Null
    Start-Sleep -Milliseconds 80
    if (([string]$req.button).ToLowerInvariant() -eq 'right') {
      [SotyMouse]::mouse_event(0x0008, 0, 0, 0, [UIntPtr]::Zero)
      [SotyMouse]::mouse_event(0x0010, 0, 0, 0, [UIntPtr]::Zero)
    } else {
      [SotyMouse]::mouse_event(0x0002, 0, 0, 0, [UIntPtr]::Zero)
      [SotyMouse]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero)
    }
    Emit ([pscustomobject]@{ ok=$true; action=$action; x=$x; y=$y; button=([string]$req.button) })
  }
  'type' {
    [System.Windows.Forms.SendKeys]::SendWait([string]$req.text)
    Emit ([pscustomobject]@{ ok=$true; action=$action; chars=([string]$req.text).Length })
  }
  'key' {
    [System.Windows.Forms.SendKeys]::SendWait([string]$req.keys)
    Emit ([pscustomobject]@{ ok=$true; action=$action; keys=([string]$req.keys) })
  }
  default { throw "unsupported desktop action: $action" }
}
`.trim();
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

function operatorSourceTargetScore(target, sourceDeviceId) {
  let score = 0;
  if (isAgentSourceTarget(target.id)) {
    score += 10_000;
  }
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
    const [operatorResult, sourceTargets] = await Promise.all([
      fetch(`http://127.0.0.1:${port}/operator/targets`, { cache: "no-store" })
        .then(async (response) => ({ ok: response.ok, payload: await response.json() }))
        .catch(() => ({ ok: false, payload: {} })),
      activeAgentSourceTargets()
    ]);
    const payload = operatorResult.payload || {};
    if (!payload.attached && sourceTargets.length === 0) {
      process.stderr.write("sotyctl: pwa bridge is not attached\n");
      process.exit(2);
    }
    const printed = new Set();
    for (const target of sourceTargets) {
      printed.add(target.id);
      process.stdout.write(`${target.label}\t${target.id}\tsource\n`);
    }
    for (const target of payload.targets || []) {
      if (printed.has(target.id)) {
        continue;
      }
      const status = target.access === true ? "access" : target.host === true ? "host" : target.access === false ? "visible" : "unknown";
      process.stdout.write(`${target.label}\t${target.id}\t${status}\n`);
    }
    return;
  }
  if (command === "toolkit" || command === "toolkits") {
    const subcommand = args[1] || "describe";
    if (subcommand === "describe" || subcommand === "contract" || subcommand === "info") {
      const response = await fetch(`http://127.0.0.1:${port}/operator/toolkits`, { cache: "no-store" });
      const payload = await response.json();
      process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
      process.exit(response.ok && payload.ok ? 0 : 1);
    }
    if (subcommand === "list" || subcommand === "ls" || subcommand === "status" || subcommand === "show" || subcommand === "stop" || subcommand === "cancel") {
      return await runControlCli(["action", ...args.slice(1)]);
    }
    if (subcommand === "run" || subcommand === "script") {
      const rest = args.slice(2);
      const hasToolkit = rest.some((item) => item === "--toolkit" || String(item || "").startsWith("--toolkit="));
      return await runControlCli(["action", subcommand, ...(hasToolkit ? rest : ["--toolkit", "durable-action", ...rest])]);
    }
    process.stderr.write("sotyctl toolkit describe | toolkit list | toolkit status <job-id> | toolkit stop <job-id> | toolkit run [--toolkit=name] <target> <command> | toolkit script [--toolkit=name] <target> <file> [shell]\n");
    process.exit(2);
  }
  if (command === "action" || command === "actions") {
    const subcommand = args[1] || "list";
    if (subcommand === "list" || subcommand === "ls") {
      const response = await fetch(`http://127.0.0.1:${port}/operator/actions`, { cache: "no-store" });
      const payload = await response.json();
      for (const job of payload.jobs || []) {
        process.stdout.write(formatActionJobLine(job));
      }
      process.exit(response.ok && payload.ok ? 0 : 1);
    }
    if (subcommand === "status" || subcommand === "show") {
      const jobId = args[2] || "";
      if (!jobId) {
        process.stderr.write("sotyctl action status <job-id>\n");
        process.exit(2);
      }
      const response = await fetch(`http://127.0.0.1:${port}/operator/action/${encodeURIComponent(jobId)}`, { cache: "no-store" });
      const payload = await response.json();
      process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
      process.exit(response.ok && payload.ok ? 0 : 1);
    }
    if (subcommand === "stop" || subcommand === "cancel") {
      const jobId = args[2] || "";
      if (!jobId) {
        process.stderr.write("sotyctl action stop <job-id>\n");
        process.exit(2);
      }
      const response = await fetch(`http://127.0.0.1:${port}/operator/action/${encodeURIComponent(jobId)}/stop`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}"
      });
      const payload = await response.json();
      printActionCliResult(payload);
      process.exit(typeof payload.exitCode === "number" ? payload.exitCode : (response.ok && payload.ok ? 0 : 1));
    }
    if (subcommand === "run") {
      const parsed = parseActionCtlOptions(args.slice(2));
      const target = parsed.args[0] || "";
      const remoteCommand = parsed.args.slice(1).join(" ");
      if (!target || !remoteCommand) {
        process.stderr.write("sotyctl action run [--toolkit=name] [--phase=name] [--family=name] [--kind=name] [--risk=low|medium|high|destructive] [--idempotency-key=key] [--detached] [--source-device=id] [--timeout=ms] <target> <command>\n");
        process.exit(2);
      }
      const response = await fetch(`http://127.0.0.1:${port}/operator/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          mode: "run",
          target,
          command: remoteCommand,
          ...actionCtlRequestOptions(parsed)
        })
      });
      const payload = await response.json();
      printActionCliResult(payload);
      process.exit(typeof payload.exitCode === "number" ? payload.exitCode : (response.ok && payload.ok ? 0 : 1));
    }
    if (subcommand === "script") {
      const parsed = parseActionCtlOptions(args.slice(2));
      const target = parsed.args[0] || "";
      const filePath = parsed.args[1] || "";
      const shell = parsed.shell || parsed.args[2] || "";
      if (!target || !filePath) {
        process.stderr.write("sotyctl action script [--toolkit=name] [--phase=name] [--family=name] [--kind=name] [--risk=low|medium|high|destructive] [--idempotency-key=key] [--detached] [--source-device=id] [--timeout=ms] <target> <file> [shell]\n");
        process.exit(2);
      }
      const script = await readFile(filePath, "utf8");
      const response = await fetch(`http://127.0.0.1:${port}/operator/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          mode: "script",
          target,
          name: basename(filePath),
          shell,
          script,
          ...actionCtlRequestOptions(parsed)
        })
      });
      const payload = await response.json();
      printActionCliResult(payload);
      process.exit(typeof payload.exitCode === "number" ? payload.exitCode : (response.ok && payload.ok ? 0 : 1));
    }
    process.stderr.write("sotyctl action list | action status <job-id> | action stop <job-id> | action run <target> <command> | action script <target> <file> [shell]\n");
    process.exit(2);
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
  if (command === "agent-new" || command === "new-agent-chat") {
    const parsed = parseCtlOptions(args.slice(1));
    const response = await fetch(`http://127.0.0.1:${port}/operator/agent-new`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
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
  if (command === "learn-sync" || command === "learning-sync" || command === "memory-sync" || (command === "learn" && args[1] === "sync") || (command === "memory" && args[1] === "sync")) {
    const result = await syncLearningOutbox().catch(() => ({ ok: false, sent: 0, pending: 0 }));
    process.stdout.write(`soty-memory: ok=${result.ok ? "true" : "false"} sent=${result.sent || 0} pending=${result.pending || 0}\n`);
    return await finishControlCli(result.ok ? 0 : 1);
  }
  if (command === "learn-doctor" || command === "learn-teacher" || command === "learning-doctor" || command === "learning-teacher" || command === "memory-doctor" || command === "memory-query" || (command === "learn" && (args[1] === "doctor" || args[1] === "teacher")) || (command === "memory" && (args[1] === "doctor" || args[1] === "query"))) {
    const rest = command === "learn" || command === "memory" ? args.slice(2) : args.slice(1);
    const json = rest.includes("--json");
    const limitArg = rest.find((item) => item.startsWith("--limit="));
    const limit = limitArg ? Number.parseInt(limitArg.slice("--limit=".length), 10) : 800;
    const sync = await syncLearningOutbox().catch(() => ({ ok: false, sent: 0, pending: 0 }));
    const report = await fetchLearningTeacherReport(limit).catch((error) => ({
      ok: false,
      status: 0,
      error: error instanceof Error ? error.message : String(error)
    }));
    if (json) {
      process.stdout.write(`${JSON.stringify({ ok: sync.ok && report.ok, sync, memory: report }, null, 2)}\n`);
    } else {
      process.stdout.write(`${formatLearningTeacherReport(sync, report)}\n`);
    }
    return await finishControlCli(sync.ok && report.ok ? 0 : 1);
  }
  if (command === "learn-review-merge" || command === "learning-review-merge" || command === "learn-global-review" || command === "learning-global-review" || command === "memory-review" || (command === "learn" && (args[1] === "review-merge" || args[1] === "merge" || args[1] === "global-review" || args[1] === "global-review-merge")) || (command === "memory" && (args[1] === "review" || args[1] === "global-review"))) {
    const rest = command === "learn" || command === "memory" ? args.slice(2) : args.slice(1);
    const options = parseLearningReviewMergeOptions(rest);
    const report = await runLearningReviewMerge(rest);
    if (options.json) {
      process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    } else {
      process.stdout.write(`${formatLearningReviewMergeReport(report)}\n`);
    }
    return await finishControlCli(report.ok && (!options.strict || !report.blockedByReview) ? 0 : 1);
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
  process.stderr.write("sotyctl health | list | toolkit describe|list|status|run|script | action list|status|run|script | run [--source-device=id] [--timeout=ms] <target> <command> | script [--source-device=id] [--timeout=ms] <target> <file> [shell] | install-machine <target> | machine-status <target> | access <target> | say [--fast|--slow] <target> <text> | agent-new | agent-message [--timeout=ms] [agent-tunnel-id] <text> | read [target] | listen [target] | export [file] | memory sync|doctor|query|review [--json] [--limit=n] | import <file>\n");
  process.exit(2);
}

function parseActionCtlOptions(args) {
  const rest = [...args];
  let timeoutMs = 0;
  let sourceDeviceId = "";
  let sourceRelayId = "";
  let toolkit = "";
  let phase = "";
  let family = "";
  let kind = "";
  let risk = "";
  let shell = "";
  let idempotencyKey = "";
  let improvement = "";
  let reuseKey = "";
  let pivotFrom = "";
  let successCriteria = "";
  let scriptUse = "";
  let contextFingerprint = "";
  let detached = false;
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
    if (head.startsWith("--source-relay=")) {
      sourceRelayId = safeRelayId(head.slice("--source-relay=".length));
      rest.shift();
      continue;
    }
    if (head === "--source-relay" && rest.length > 1) {
      sourceRelayId = safeRelayId(rest[1]);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--family=")) {
      family = cleanActionToken(head.slice("--family=".length), "");
      rest.shift();
      continue;
    }
    if (head.startsWith("--toolkit=")) {
      toolkit = normalizeToolkitName(head.slice("--toolkit=".length));
      rest.shift();
      continue;
    }
    if (head === "--toolkit" && rest.length > 1) {
      toolkit = normalizeToolkitName(rest[1]);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--phase=")) {
      phase = cleanActionToken(head.slice("--phase=".length), "");
      rest.shift();
      continue;
    }
    if (head === "--phase" && rest.length > 1) {
      phase = cleanActionToken(rest[1], "");
      rest.splice(0, 2);
      continue;
    }
    if (head === "--family" && rest.length > 1) {
      family = cleanActionToken(rest[1], "");
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--kind=")) {
      kind = cleanActionToken(head.slice("--kind=".length), "");
      rest.shift();
      continue;
    }
    if (head === "--kind" && rest.length > 1) {
      kind = cleanActionToken(rest[1], "");
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--risk=")) {
      risk = cleanActionRisk(head.slice("--risk=".length));
      rest.shift();
      continue;
    }
    if (head === "--risk" && rest.length > 1) {
      risk = cleanActionRisk(rest[1]);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--shell=")) {
      shell = cleanActionText(head.slice("--shell=".length), 40);
      rest.shift();
      continue;
    }
    if (head === "--shell" && rest.length > 1) {
      shell = cleanActionText(rest[1], 40);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--idempotency-key=")) {
      idempotencyKey = cleanActionId(head.slice("--idempotency-key=".length));
      rest.shift();
      continue;
    }
    if ((head === "--idempotency-key" || head === "--request-id") && rest.length > 1) {
      idempotencyKey = cleanActionId(rest[1]);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--improvement=")) {
      improvement = cleanActionText(head.slice("--improvement=".length), 240);
      rest.shift();
      continue;
    }
    if (head === "--improvement" && rest.length > 1) {
      improvement = cleanActionText(rest[1], 240);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--reuse-key=")) {
      reuseKey = cleanActionText(head.slice("--reuse-key=".length), 120);
      rest.shift();
      continue;
    }
    if (head === "--reuse-key" && rest.length > 1) {
      reuseKey = cleanActionText(rest[1], 120);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--pivot-from=")) {
      pivotFrom = cleanActionText(head.slice("--pivot-from=".length), 160);
      rest.shift();
      continue;
    }
    if (head === "--pivot-from" && rest.length > 1) {
      pivotFrom = cleanActionText(rest[1], 160);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--success-criteria=")) {
      successCriteria = cleanActionText(head.slice("--success-criteria=".length), 220);
      rest.shift();
      continue;
    }
    if (head === "--success-criteria" && rest.length > 1) {
      successCriteria = cleanActionText(rest[1], 220);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--script-use=")) {
      scriptUse = cleanActionText(head.slice("--script-use=".length), 180);
      rest.shift();
      continue;
    }
    if (head === "--script-use" && rest.length > 1) {
      scriptUse = cleanActionText(rest[1], 180);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--context=")) {
      contextFingerprint = cleanActionText(head.slice("--context=".length), 120);
      rest.shift();
      continue;
    }
    if (head === "--context" && rest.length > 1) {
      contextFingerprint = cleanActionText(rest[1], 120);
      rest.splice(0, 2);
      continue;
    }
    if (head.startsWith("--request-id=")) {
      idempotencyKey = cleanActionId(head.slice("--request-id=".length));
      rest.shift();
      continue;
    }
    if (head === "--detached" || head === "--detach" || head === "--no-wait") {
      detached = true;
      rest.shift();
      continue;
    }
    if (head === "--wait=false") {
      detached = true;
      rest.shift();
      continue;
    }
    break;
  }
  return { timeoutMs, sourceDeviceId, sourceRelayId, toolkit, phase, family, kind, risk, shell, idempotencyKey, improvement, reuseKey, pivotFrom, successCriteria, scriptUse, contextFingerprint, detached, args: rest };
}

function actionCtlRequestOptions(parsed) {
  return {
    ...(parsed.sourceDeviceId ? { sourceDeviceId: parsed.sourceDeviceId } : {}),
    ...(parsed.sourceRelayId ? { sourceRelayId: parsed.sourceRelayId } : {}),
    ...(parsed.timeoutMs ? { timeoutMs: parsed.timeoutMs } : {}),
    ...(parsed.toolkit ? { toolkit: parsed.toolkit } : {}),
    ...(parsed.phase ? { phase: parsed.phase } : {}),
    ...(parsed.family ? { family: parsed.family } : {}),
    ...(parsed.kind ? { kind: parsed.kind } : {}),
    ...(parsed.risk ? { risk: parsed.risk } : {}),
    ...(parsed.idempotencyKey ? { idempotencyKey: parsed.idempotencyKey } : {}),
    ...(parsed.improvement ? { improvement: parsed.improvement } : {}),
    ...(parsed.reuseKey ? { reuseKey: parsed.reuseKey } : {}),
    ...(parsed.pivotFrom ? { pivotFrom: parsed.pivotFrom } : {}),
    ...(parsed.successCriteria ? { successCriteria: parsed.successCriteria } : {}),
    ...(parsed.scriptUse ? { scriptUse: parsed.scriptUse } : {}),
    ...(parsed.contextFingerprint ? { contextFingerprint: parsed.contextFingerprint } : {}),
    ...(parsed.detached ? { detached: true } : {})
  };
}

function printActionCliResult(payload) {
  if (payload.text) {
    process.stdout.write(payload.text);
    if (!payload.text.endsWith("\n")) {
      process.stdout.write("\n");
    }
  }
  const status = cleanActionText(payload.status || (payload.ok ? "ok" : "failed"), 24);
  const jobId = cleanActionText(payload.jobId, 96);
  const proof = cleanActionText(payload.proof, 240);
  process.stderr.write(`soty-action: ${status}${jobId ? ` ${jobId}` : ""}${proof ? ` ${proof}` : ""}\n`);
}

function formatActionJobLine(job) {
  return [
    job.createdAt || "",
    job.status || "",
    job.toolkit || "",
    job.phase || "",
    job.family || "",
    job.mode || "",
    job.risk || "",
    job.target || "",
    job.id || ""
  ].join("\t") + "\n";
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

function safeDurationMs(value, fallback, max = maxLongTaskTimeoutMs) {
  const timeoutMs = Number.parseInt(String(value || ""), 10);
  return Number.isSafeInteger(timeoutMs) ? Math.max(1000, Math.min(timeoutMs, max)) : fallback;
}

function safeRunTimeoutMs(value) {
  return safeDurationMs(value, defaultTimeoutMs, maxLongTaskTimeoutMs);
}

function safeCtlTimeout(value) {
  return safeDurationMs(value, 0, maxLongTaskTimeoutMs);
}

function machineInstallCommand() {
  if (process.platform !== "win32") {
    return unixMachineInstallCommand();
  }
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
  if (process.platform !== "win32") {
    return unixMachineStatusCommand();
  }
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

function unixMachineInstallCommand() {
  const base = "https://xn--n1afe0b.online/agent";
  const lines = [
    "set -eu",
    "tmp=\"${TMPDIR:-/tmp}/soty-agent-machine\"",
    "mkdir -p \"$tmp\"",
    "script=\"$tmp/install-macos-linux.sh\"",
    `base=${shQuote(base)}`,
    "if command -v curl >/dev/null 2>&1; then curl -fsSL \"$base/install-macos-linux.sh\" -o \"$script\"; elif command -v wget >/dev/null 2>&1; then wget -qO \"$script\" \"$base/install-macos-linux.sh\"; else echo 'soty-agent-machine:missing-downloader'; exit 1; fi",
    "chmod 755 \"$script\"",
    "log=\"$tmp/install.log\"",
    "(",
    "  if [ \"$(id -u)\" = \"0\" ]; then",
    "    sh \"$script\" --scope machine --base \"$base\"",
    "  elif [ \"$(uname -s)\" = \"Darwin\" ] && command -v osascript >/dev/null 2>&1; then",
    "    cmd=\"sh $(printf %s \"$script\" | sed \"s/'/'\\\\''/g; s/^/'/; s/$/'/\") --scope machine --base $(printf %s \"$base\" | sed \"s/'/'\\\\''/g; s/^/'/; s/$/'/\")\"",
    "    esc=$(printf %s \"$cmd\" | sed 's/\\\\/\\\\\\\\/g; s/\"/\\\\\"/g')",
    "    osascript -e \"do shell script \\\"$esc\\\" with administrator privileges\"",
    "  elif command -v pkexec >/dev/null 2>&1; then",
    "    pkexec sh \"$script\" --scope machine --base \"$base\"",
    "  elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then",
    "    sudo -n sh \"$script\" --scope machine --base \"$base\"",
    "  else",
    "    echo 'soty-agent-machine:sudo-required'",
    "    exit 1",
    "  fi",
    ") >\"$log\" 2>&1 &",
    "echo \"soty-agent-machine:launcher-started log=$log\""
  ];
  return lines.join("\n");
}

function unixMachineStatusCommand() {
  return [
    "set -eu",
    "url='http://127.0.0.1:49424/health'",
    "if command -v curl >/dev/null 2>&1; then",
    "  curl -fsS --max-time 3 \"$url\"",
    "elif command -v wget >/dev/null 2>&1; then",
    "  wget -qO- --timeout=3 \"$url\"",
    "else",
    "  printf '%s\\n' '{\"ok\":false,\"error\":\"curl-or-wget-required\"}'",
    "  exit 1",
    "fi"
  ].join("\n");
}

function shQuote(value) {
  return `'${String(value).replace(/'/gu, "'\\''")}'`;
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
    autoUpdate: agentAutoUpdate,
    platform: process.platform,
    shell: shellName(),
    version: agentVersion,
    relay: Boolean(agentRelayId),
    codex: hasCodexBinary(),
    codexBinary: Boolean(findCodexBinary()),
    codexAuth: hasCodexAuth(),
    codexMode: codexFullLocalTools ? "stock-cli-full-local-tools" : "stock-cli-bridge",
    codexSessionMode,
    codexRuntimeContext: "clean-codex+memory-plane+capability-gateway",
    codexProxy: Boolean(codexProxyUrl),
    codexProxyScheme: proxyScheme(codexProxyUrl),
    responseStyle: agentResponseStyleStatus(),
    trace: agentTraceStatus(),
    memory: memoryPlaneStatus(),
    automationToolkits: automationToolkitStatus(),
    ...(process.platform === "win32" ? {
      windowsUser: windowsUserName(),
      system: isWindowsSystem(),
      maintenance: agentScope === "Machine" && isWindowsSystem()
    } : {
      user: unixUserName(),
      uid: unixUid(),
      gid: unixGid(),
      system: isUnixRoot(),
      maintenance: agentScope === "Machine" && isUnixRoot()
    })
  };
}

function automationToolkitStatus() {
  return {
    schema: "soty.automation-toolkits.v2",
    policy: "capability-api-with-memory-hints",
    chat: activeAgentResponseStyle.id,
    responseStyle: agentResponseStyleStatus(),
    frontDoor: "soty_toolkit",
    defaultKernel: "soty_action",
    terminalStates: ["completed", "failed", "blocked-needs-user", "waiting-confirmation"],
    available: ["capability-gateway", "durable-action", "windows-reinstall"],
    toolkits: [
      {
        name: "capability-gateway",
        entryTool: "soty_toolkit",
        phases: ["describe", "start", "status", "stop", "list", "reinstall"],
        proof: ["toolkit", "phase", "jobId", "statusPath", "resultPath", "proof"]
      },
      {
        name: "durable-action",
        entryTool: "soty_action",
        phases: ["start", "status", "stop"],
        proof: ["jobId", "statusPath", "resultPath", "proof"]
      },
      {
        name: "windows-reinstall",
        entryTool: "soty_reinstall",
        phases: ["preflight", "prepare", "status", "arm"],
        proof: ["backupProof", "installMedia", "unattend", "postinstall", "rebooting"]
      }
    ]
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

function unixUserName() {
  if (process.platform === "win32") {
    return "";
  }
  return process.env.USER || process.env.LOGNAME || process.env.SUDO_USER || "";
}

function unixUid() {
  return typeof process.getuid === "function" ? process.getuid() : undefined;
}

function unixGid() {
  return typeof process.getgid === "function" ? process.getgid() : undefined;
}

function isUnixRoot() {
  return process.platform !== "win32" && unixUid() === 0;
}

function windowsWhoami() {
  if (process.platform !== "win32") {
    return "";
  }
  if (cachedWindowsWhoami) {
    return cachedWindowsWhoami;
  }
  try {
    cachedWindowsWhoami = execFileSync("powershell.exe", [
      "-NoLogo",
      "-NoProfile",
      "-ExecutionPolicy",
      "Bypass",
      "-Command",
      `${powerShellUtf8Prelude()}; [System.Security.Principal.WindowsIdentity]::GetCurrent().Name`
    ], {
      encoding: "utf8",
      timeout: 1000,
      windowsHide: true
    }).trim();
  } catch {
    try {
      cachedWindowsWhoami = execFileSync("whoami.exe", {
        encoding: "utf8",
        timeout: 1000,
        windowsHide: true
      }).trim();
    } catch {
      cachedWindowsWhoami = "";
    }
  }
  return cachedWindowsWhoami;
}

function safeScope(value) {
  const text = String(value || "").trim();
  if (text === "Machine" || text === "CurrentUser" || text === "Dev" || text === "Server") {
    return text;
  }
  return "CurrentUser";
}

function safeRelayId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

function safeInstallId(value) {
  const text = String(value || "").trim();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(text)
    ? text.toLowerCase()
    : "";
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
  if (!managed || !updateManifestUrl || !agentAutoUpdate) {
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
  if (!agentAutoUpdate) {
    return;
  }
  if (updateCheckRunning) {
    return;
  }
  updateCheckRunning = true;
  try {
    const { response, json: manifest } = await fetchJsonWithTimeout(updateManifestUrl, { cache: "no-store" }, updateFetchTimeoutMs);
    if (!response.ok) {
      return;
    }
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
    if (shouldDeferAgentUpdate()) {
      scheduleDeferredUpdateCheck();
      return;
    }
    const nextUrl = new URL(manifest.agentUrl, updateManifestUrl);
    const { response: nextResponse, bytes } = await fetchBytesWithTimeout(nextUrl, { cache: "no-store" }, updateFetchTimeoutMs);
    if (!nextResponse.ok) {
      return;
    }
    if (sha256(bytes) !== manifest.sha256) {
      return;
    }
    await mkdir(dirname(scriptPath), { recursive: true });
    const tempPath = join(dirname(scriptPath), "soty-agent.next.mjs");
    await writeFile(tempPath, bytes, { mode: 0o755 });
    await copyFile(tempPath, scriptPath);
    await rm(tempPath, { force: true });
    notifyOperatorUpdating(manifest.version);
    await sleep(250);
    process.exit(75);
  } catch {
    // Updates are best-effort; the running agent must keep the tunnel useful.
  } finally {
    updateCheckRunning = false;
  }
}

function shouldDeferAgentUpdate() {
  return active.size > 0
    || operatorRuns.size > 0
    || actionControllers.size > 0
    || activeRelayJobs.size > 0;
}

function scheduleDeferredUpdateCheck() {
  if (deferredUpdateTimer) {
    return;
  }
  deferredUpdateTimer = setTimeout(() => {
    deferredUpdateTimer = null;
    void checkForUpdate();
  }, 60_000);
  deferredUpdateTimer.unref?.();
}

function notifyOperatorUpdating(version) {
  sendRaw(operatorBridge, {
    type: "operator.updating",
    version: typeof version === "string" ? version.slice(0, 40) : ""
  });
}

async function fetchJsonWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    return { response, json: await response.json() };
  } finally {
    clearTimeout(timer);
  }
}

async function fetchBytesWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    return { response, bytes: Buffer.from(await response.arrayBuffer()) };
  } finally {
    clearTimeout(timer);
  }
}

async function readJsonFile(path) {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch {
    return null;
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
