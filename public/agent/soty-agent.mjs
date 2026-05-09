#!/usr/bin/env node
import { execFileSync, spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { appendFile, chmod, copyFile, cp, mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const agentVersion = "0.3.112";
const scriptPath = fileURLToPath(import.meta.url);
const agentDir = dirname(scriptPath);
const agentConfigPath = join(agentDir, "agent-config.json");
const codexSessionsPath = join(agentDir, "agent-codex-sessions.json");
const codexWorkspacesDir = join(agentDir, "codex-workspaces");
const learningOutboxPath = join(agentDir, "learning-outbox.jsonl");
const learningSentPath = join(agentDir, "learning-sent.jsonl");
const actionJobsDir = resolve(process.env.SOTY_AGENT_ACTION_JOBS_DIR || join(agentDir, "action-jobs"));
const persistedAgentConfig = loadAgentConfig();
const persistedCodexSessions = loadCodexSessions();
const port = Number.parseInt(arg("--port") || process.env.SOTY_AGENT_PORT || "49424", 10);
const maxLongTaskTimeoutMs = 2 * 60 * 60_000;
const defaultTimeoutMs = safeDurationMs(arg("--timeout") || process.env.SOTY_AGENT_TIMEOUT_MS, 30 * 60_000, maxLongTaskTimeoutMs);
const requestedShell = arg("--shell") || process.env.SOTY_AGENT_SHELL || "";
const updateManifestUrl = arg("--update-url") || process.env.SOTY_AGENT_UPDATE_URL || "https://xn--n1afe0b.online/agent/manifest.json";
let agentRelayId = safeRelayId(arg("--relay-id") || process.env.SOTY_AGENT_RELAY_ID || persistedAgentConfig.relayId || "");
let agentRelayBaseUrl = safeHttpBaseUrl(process.env.SOTY_AGENT_RELAY_URL || persistedAgentConfig.relayBaseUrl || originFromUrl(updateManifestUrl) || "https://xn--n1afe0b.online");
const agentInstallId = safeInstallId(persistedAgentConfig.installId) || randomUUID();
const managed = process.argv.includes("--managed") || process.env.SOTY_AGENT_MANAGED === "1";
const agentScope = safeScope(process.env.SOTY_AGENT_SCOPE || (managed ? "CurrentUser" : "Dev"));
const maxCommandChars = 8_000;
const maxScriptChars = 1_000_000;
const maxChatChars = 12_000;
const maxAgentContextChars = 16_000;
const maxAgentRuntimePromptChars = 48_000;
const maxImportChars = 2_000_000;
const maxChunkBytes = 12_000;
const maxFrameBytes = 2_500_000;
const maxSourceChars = 180;
const configuredAgentDeviceId = safeSourceText(process.env.SOTY_AGENT_DEVICE_ID || "");
const configuredAgentDeviceNick = safeSourceText(process.env.SOTY_AGENT_DEVICE_NICK || "");
const maxCodexDialogMessages = 64;
const audioToolTimeoutMs = 120_000;
const audioWarmupTimeoutMs = 45_000;
const agentReplyTimeoutMs = Math.max(
  maxLongTaskTimeoutMs,
  safeDurationMs(process.env.SOTY_CODEX_REPLY_TIMEOUT_MS, maxLongTaskTimeoutMs, maxLongTaskTimeoutMs)
);
const maxConcurrentCodexJobs = Math.max(1, Math.min(Number.parseInt(process.env.SOTY_CODEX_CONCURRENCY || "4", 10) || 4, 16));
const codexFullLocalTools = process.env.SOTY_CODEX_FULL_LOCAL_TOOLS !== "0";
const codexProxyUrl = safeProxyUrl(process.env.SOTY_CODEX_PROXY_URL || process.env.SOTY_AGENT_PROXY_URL || "");
const codexDefaultReasoningEffort = safeCodexReasoningEffort(process.env.SOTY_CODEX_REASONING_EFFORT || "");
const codexRelayFallback = process.env.SOTY_CODEX_RELAY_FALLBACK === "1";
const codexSessionMode = "stock-openai-codex-cli-full-local-tools-v4-runtime-context";
const active = new Map();
const operatorRuns = new Map();
const actionJobs = new Map();
const operatorMessages = [];
const operatorMessageWaiters = new Set();
const agentOperatorReplyQueues = new Map();
const recentAgentOperatorMessageKeys = new Map();
const recentCodexTurnKeys = new Map();
const activeRelayJobs = new Set();
let learningSyncTimer = null;
let operatorBridge = null;
let operatorBridgeVisible = false;
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
      targets: operatorTargets
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
    script
  });
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
  const intent = cleanActionText(payload.intent || payload.name || family, 180);
  const commandSig = commandSignature(body, family);
  return {
    ok: true,
    mode,
    actionType: cleanActionToken(payload.kind || payload.actionType || mode, mode),
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
    risk: cleanActionRisk(payload.risk || inferActionRisk(body, family)),
    detached: payload.detached === true || payload.wait === false,
    createdBy: cleanActionText(payload.createdBy || "soty-agent", 80),
    idempotencyKey: cleanActionId(payload.idempotencyKey || payload.clientRequestId || payload.requestId || ""),
    commandSig,
    taskSig: taskSignature(`${family} ${intent} ${target}`)
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
    createdAt
  });
  await writeActionJob(job);
  actionJobs.set(id, job);
  return job;
}

async function runActionJob(job, action) {
  const started = Date.now();
  let current = {
    ...job,
    status: "running",
    startedAt: new Date(started).toISOString()
  };
  actionJobs.set(job.id, current);
  await writeActionJob(current);
  let execution;
  try {
    execution = await executeOperatorAction(action);
  } catch (error) {
    execution = {
      ok: false,
      text: error instanceof Error ? `! action ${error.message}` : "! action",
      exitCode: 127,
      route: "action-kernel",
      target: action.target,
      sourceDeviceId: action.sourceDeviceId
    };
  }
  const finished = Date.now();
  const exitCode = Number.isSafeInteger(execution.exitCode) ? execution.exitCode : (execution.ok ? 0 : 1);
  const status = execution.ok && exitCode === 0
    ? "ok"
    : exitCode === 124
      ? "timeout"
      : exitCode === 422
        ? "blocked"
        : "failed";
  const durationMs = Math.max(0, finished - started);
  const text = String(execution.text || "").slice(-1_000_000);
  const route = cleanActionText(execution.route || `operator-action.${action.mode}`, 120);
  const proof = buildActionProof({ action, execution: { ...execution, exitCode, route, text }, status });
  const resultDoc = {
    schema: "soty.action.result.v1",
    jobId: job.id,
    ok: status === "ok",
    status,
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
    output: {
      chars: text.length,
      shape: sourceOutputShape(text),
      tail: text.slice(-12_000)
    },
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
    family: action.family,
    result: status,
    route,
    commandSig: job.commandSig,
    taskSig: job.taskSig,
    proof,
    exitCode,
    durationMs
  });
  return {
    httpStatus: status === "blocked" ? 422 : 200,
    payload: {
      ok: status === "ok",
      jobId: job.id,
      idempotencyKey: job.idempotencyKey,
      status,
      family: action.family,
      risk: action.risk,
      route,
      proof,
      text: text.slice(-maxChatChars),
      exitCode,
      durationMs,
      statusPath: `/operator/action/${job.id}`,
      resultPath: job.artifacts.resultPath
    }
  };
}

async function executeOperatorAction(action) {
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
    const result = action.mode === "script"
      ? await postAgentSourceJob("/api/agent/source/script", {
        deviceId,
        script: action.script,
        name: action.name,
        shell: action.shell,
        timeoutMs: action.timeoutMs
      }, action.sourceRelayId, 1_000_000)
      : await postAgentSourceJob("/api/agent/source/run", {
        deviceId,
        command: action.command,
        timeoutMs: action.timeoutMs
      }, action.sourceRelayId, 1_000_000);
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
  const { id, promise } = registerOperatorPromiseRun(action.timeoutMs);
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
    timer = setTimeout(() => finish(124, "! timeout"), timeoutMs);
    operatorRuns.set(id, {
      append: (text) => {
        body = `${body}${text}`.slice(-1_000_000);
      },
      finish
    });
  });
  return { id, promise };
}

function buildActionProof({ action, execution, status }) {
  const exitCode = Number.isSafeInteger(execution.exitCode) ? execution.exitCode : (execution.ok ? 0 : 1);
  if (status === "ok") {
    return `exitCode=0; family=${action.family}; route=${execution.route}; output=${sourceOutputShape(execution.text)}`;
  }
  return `exitCode=${exitCode}; family=${action.family}; route=${execution.route}; proof=${sourceFailureProof(execution.text)}`;
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
    family: cleanActionText(result?.family || job.family, 80),
    risk: cleanActionText(result?.risk || job.risk, 20),
    route: cleanActionText(result?.route || job.route, 120),
    proof: cleanActionText(result?.proof || job.proof, 900),
    text: String(result?.output?.tail || "").slice(-maxChatChars),
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
    status: cleanActionText(job.status, 24),
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

async function postAgentSourceJob(path, body, relayId = "", maxTextLength = maxChatChars) {
  const relayBaseUrl = agentRelayBaseUrl || originFromUrl(updateManifestUrl);
  const jobRelayId = safeRelayId(relayId) || agentRelayId;
  if (!relayBaseUrl || !jobRelayId) {
    return { ok: false, text: "! relay", exitCode: 409 };
  }
  try {
    const response = await fetch(new URL(path, relayBaseUrl), {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        relayId: jobRelayId,
        ...body
      })
    });
    const payload = await response.json();
    return {
      ok: Boolean(response.ok && payload?.ok),
      text: String(payload?.text || "").slice(0, Math.max(1, Math.min(maxTextLength, 1_000_000))),
      exitCode: Number.isSafeInteger(payload?.exitCode) ? payload.exitCode : (response.ok ? 0 : response.status)
    };
  } catch {
    return { ok: false, text: "! agent-source", exitCode: 127 };
  }
}

function rememberAgentSourceOutcome({ kind, command, result }) {
  const family = classifySourceCommand(command);
  const exitCode = Number.isSafeInteger(result?.exitCode) ? result.exitCode : (result?.ok ? 0 : 1);
  const ok = Boolean(result?.ok && exitCode === 0);
  recordLearningReceipt({
    kind: "source-command",
    family,
    result: ok ? "ok" : exitCode === 124 ? "timeout" : "failed",
    route: `agent-source.${kind}`,
    commandSig: commandSignature(command, family),
    proof: ok
      ? `exitCode=0; output=${sourceOutputShape(result?.text)}`
      : `exitCode=${exitCode}; proof=${sourceFailureProof(result?.text)}`,
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
    result: cleanLearningEnum(value.result, ["ok", "failed", "partial", "blocked", "timeout"], "failed"),
    family: cleanLearningText(value.family, 80),
    platform: process.platform,
    codexMode: codexFullLocalTools ? "stock-cli-full-local-tools" : "stock-cli-bridge",
    route: cleanLearningText(value.route, 120),
    commandSig: cleanLearningText(value.commandSig, 120),
    taskSig: cleanLearningText(value.taskSig, 120),
    proof: redactLearningText(value.proof).slice(0, 900),
    ...(exitCode === undefined ? {} : { exitCode }),
    ...(Number.isSafeInteger(value.durationMs) ? { durationMs: Math.max(0, Math.min(86_400_000, value.durationMs)) } : {}),
    skillSha: cleanLearningText(opsSkillStatus().tarSha256 || "", 80),
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
    .replace(/\b(?:sk|sess|key|token|secret|password|pwd)[-_A-Za-z0-9]*\b\s*[:=]\s*['"]?[^'"\s]+/giu, "<secret>")
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
  const response = await fetch(new URL("/api/agent/learning/receipts", agentRelayBaseUrl), {
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
    return { ok: false, status: 0, error: "learning relay url is not configured" };
  }
  const url = new URL("/api/agent/learning/teacher", agentRelayBaseUrl);
  url.searchParams.set("limit", String(Math.max(1, Math.min(2000, Number.parseInt(String(limit || 800), 10) || 800))));
  const response = await fetch(url, { cache: "no-store" });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload?.ok !== true) {
    return {
      ok: false,
      status: response.status,
      error: cleanLearningText(payload?.error || response.statusText || "teacher request failed", 160)
    };
  }
  return payload;
}

function formatLearningTeacherReport(sync, report) {
  if (!report?.ok) {
    return [
      `soty-learning-doctor: ok=false sent=${sync?.sent || 0} pending=${sync?.pending || 0}`,
      `teacher: failed status=${report?.status || 0} error=${report?.error || "unknown"}`
    ].join("\n");
  }
  const lines = [
    `soty-learning-doctor: ok=true receipts=${report.receipts || 0} sent=${sync?.sent || 0} pending=${sync?.pending || 0}`,
    `teacher: ${report.schema || "soty.learning.teacher"} generated=${report.generatedAt || ""}`,
    `scope: ${formatLearningScope(report)}`,
    `publish: ${formatLearningPublishModel(report)}`
  ];
  const recommendations = Array.isArray(report.recommendations) ? report.recommendations.slice(0, 5) : [];
  if (recommendations.length > 0) {
    lines.push("recommendations:");
    for (const item of recommendations) {
      const prefix = item.priority ? `[${item.priority}] ` : "";
      lines.push(`- ${prefix}${item.family || "generic"}: ${item.title || "review route"}`);
      if (item.action) {
        lines.push(`  action: ${item.action}`);
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
  const kind = cleanLearningText(scope.kind || report?.source || "server-global-sanitized-receipts", 80) || "server-global-sanitized-receipts";
  return `${kind} devices=${deviceCount} platforms=${platforms || "unknown"} agentVersions=${versions || "unknown"}`;
}

function formatLearningPublishModel(report) {
  return cleanLearningText(report?.publishModel || "reviewed-ops-patch-then-build-release-deploy", 120)
    || "reviewed-ops-patch-then-build-release-deploy";
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
  const teacher = await fetchLearningTeacherReport(options.limit).catch((error) => ({
    ok: false,
    status: 0,
    error: error instanceof Error ? error.message : String(error)
  }));
  const report = {
    ok: Boolean(sync.ok && teacher.ok),
    mode: options.dryRun ? "dry-run" : "write",
    sync,
    teacher,
    ingest: null,
    compile: null,
    reportPath: "",
    blockedByReview: false,
    error: ""
  };
  if (!teacher.ok) {
    report.error = teacher.error || "teacher request failed";
    return report;
  }

  const skillDir = opsSkillSourceDir();
  if (!skillDir) {
    return { ...report, ok: false, error: "ops skill source is not installed" };
  }
  const ingestScript = join(skillDir, "scripts", "learning", "ingest_teacher_report.py");
  const compileScript = join(skillDir, "scripts", "learning", "compile_memory_queue.py");
  if (!existsSync(ingestScript) || !existsSync(compileScript)) {
    return { ...report, ok: false, error: "ops learning review helpers are missing" };
  }
  const python = findPythonCommand();
  if (!python) {
    return { ...report, ok: false, error: "python is required for ops learning helpers" };
  }

  const tempRoot = join(tmpdir(), "soty-learning-review", randomUUID());
  await mkdir(tempRoot, { recursive: true });
  const reportPath = join(tempRoot, "teacher-report.json");
  report.reportPath = reportPath;
  await writeFile(reportPath, JSON.stringify({ ok: report.ok, sync, teacher }, null, 2), "utf8");

  const commonArgs = [];
  if (options.memoryDir) {
    commonArgs.push("--memory-dir", options.memoryDir);
  }
  if (options.queue) {
    commonArgs.push("--queue", options.queue);
  }
  for (const scope of options.scopes) {
    commonArgs.push("--scope", scope);
  }
  const ingestArgs = [
    ingestScript,
    reportPath,
    ...commonArgs,
    ...(options.dryRun ? [] : ["--write"]),
    "--json"
  ];
  const compileArgs = [
    compileScript,
    ...(options.memoryDir ? ["--memory-dir", options.memoryDir] : []),
    "--json"
  ];

  try {
    report.ingest = runPythonJson(python, ingestArgs, skillDir);
    report.compile = runPythonJson(python, compileArgs, skillDir);
    report.blockedByReview = Number(report.compile?.decision_counts?.review || 0) > 0;
    report.ok = Boolean(report.sync.ok && report.teacher.ok && report.ingest?.ok !== false && report.compile);
  } catch (error) {
    report.ok = false;
    report.error = error instanceof Error ? error.message : String(error);
  }
  return report;
}

function parseLearningReviewMergeOptions(rest) {
  const options = {
    dryRun: false,
    json: false,
    strict: false,
    limit: 800,
    memoryDir: "",
    queue: "",
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
    if (item.startsWith("--memory-dir=")) {
      options.memoryDir = item.slice("--memory-dir=".length);
      continue;
    }
    if (item === "--memory-dir" && rest[index + 1]) {
      index += 1;
      options.memoryDir = rest[index] || "";
      continue;
    }
    if (item.startsWith("--queue=")) {
      options.queue = item.slice("--queue=".length);
      continue;
    }
    if (item === "--queue" && rest[index + 1]) {
      index += 1;
      options.queue = rest[index] || "";
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
  if (options.memoryDir) {
    options.memoryDir = resolve(options.memoryDir);
  }
  if (options.queue) {
    options.queue = resolve(options.queue);
    if (!options.memoryDir) {
      options.memoryDir = dirname(options.queue);
    }
  }
  options.scopes = options.scopes.map((scope) => scope.trim()).filter(Boolean);
  return options;
}

function findPythonCommand() {
  const candidates = process.platform === "win32"
    ? [{ command: "python", args: [] }, { command: "py", args: ["-3"] }, { command: "python3", args: [] }]
    : [{ command: "python3", args: [] }, { command: "python", args: [] }];
  for (const candidate of candidates) {
    try {
      execFileSync(candidate.command, [...candidate.args, "--version"], { encoding: "utf8", timeout: 5000 });
      return candidate;
    } catch {
      // Try the next well-known Python launcher.
    }
  }
  return null;
}

function runPythonJson(python, args, cwd) {
  const output = execFileSync(python.command, [...python.args, ...args], {
    cwd,
    encoding: "utf8",
    timeout: 120_000,
    env: { ...process.env, PYTHONIOENCODING: "utf-8" }
  });
  return JSON.parse(output.replace(/^\uFEFF/u, ""));
}

function formatLearningReviewMergeReport(report) {
  if (!report?.ok) {
    return [
      "soty-learning-review-merge: ok=false",
      `error: ${report?.error || "unknown"}`
    ].join("\n");
  }
  const decisions = report.compile?.decision_counts || {};
  const lines = [
    `soty-learning-review-merge: ok=true mode=${report.mode} scope=server-global`,
    `teacher: receipts=${report.teacher?.receipts || 0} devices=${Number(report.teacher?.scope?.deviceCount || 0)} sent=${report.sync?.sent || 0} pending=${report.sync?.pending || 0}`,
    `ingest: accepted=${report.ingest?.accepted_count || 0} skipped=${report.ingest?.skipped_count || 0}`,
    `compile: promote=${decisions.promote || 0} keep=${decisions.keep || 0} review=${decisions.review || 0} covered=${decisions.covered || 0}`,
    `queue: ${report.ingest?.queue || ""}`,
    `publish: ${formatLearningPublishModel(report.teacher)}`
  ];
  if (report.blockedByReview) {
    lines.push("next: review rows block automatic promotion; patch only reviewed shelf/hot/helper/profile/eval changes.");
  } else if (Number(decisions.promote || 0) > 0) {
    lines.push("next: promote rows are ready for a reviewed ops patch, then finish_skill_edit/build/deploy.");
  } else {
    lines.push("next: no blocking review rows; keep candidates in queue or patch a reviewed sink when useful.");
  }
  return lines.join("\n");
}

function learningStatus() {
  let pending = 0;
  try {
    const text = readFileSync(learningOutboxPath, "utf8");
    pending = text.split(/\r?\n/u).filter((line) => line.trim()).length;
  } catch {
    pending = 0;
  }
  return {
    outbox: pending,
    syncUrl: agentRelayBaseUrl ? "/api/agent/learning/receipts" : "",
    teacherUrl: agentRelayBaseUrl ? "/api/agent/learning/teacher" : "",
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
    "Do not quote this tool note or mention ops/router/preflight/gates/bridge/source-scoped details in chat."
  ].join("\n");
}

function findOpsScriptPath() {
  const explicit = process.env.SOTY_OPS_SCRIPT || "";
  const candidates = [
    explicit,
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
  if (!isAgentOperatorMessage(item)) {
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

function isDuplicateAgentOperatorMessage(item) {
  if (!isAgentOperatorMessage(item)) {
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
  const source = {
    tunnelId: item.target,
    tunnelLabel: item.label || "Агент",
    deviceId: item.sourceDeviceId || operatorDeviceId || "",
    deviceNick: item.sourceDeviceNick || operatorDeviceNick || "",
    appOrigin: agentRelayBaseUrl || originFromUrl(updateManifestUrl) || "https://xn--n1afe0b.online",
    preferredTargetId: "",
    preferredTargetLabel: "",
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
  const codexBin = hasCodexBinary() ? findCodexBinary() : "";
  if (!codexBin) {
    const relay = codexRelayFallback
      ? await askCodexRelayFallback(text, context, source, onMessage, onTerminal)
      : null;
    if (relay) {
      return relay;
    }
    return {
      ok: false,
      text: "! codex-cli: not found on this computer",
      exitCode: 126
    };
  }

  const codexHome = await preparePersistentStockCodexHome();
  const childEnv = withAgentToolPath({
    ...process.env,
    ...codexNetworkProxyEnv(),
    CODEX_HOME: codexHome
  });

  try {
    return await runCodexSotySessionTurn({
      codexBin,
      childEnv,
      text,
      context,
      source,
      onMessage,
      onTerminal
    });
  } catch (error) {
    return {
      ok: false,
      text: agentFailureText(error instanceof Error ? error.message : String(error)),
      exitCode: 1
    };
  }
}

async function runCodexSotySessionTurn({ codexBin, childEnv, text, context = "", source, onMessage, onTerminal }) {
  const startedAt = Date.now();
  const safeSource = sanitizeAgentSource(source);
  const sourceTargets = await activeAgentSourceTargets();
  const target = resolveAgentBridgeTarget(safeSource, text, sourceTargets);
  const sessionKey = codexSessionKey(safeSource, target);
  const turnKey = codexTurnDedupeKey(sessionKey, text);
  if (isDuplicateCodexTurn(turnKey)) {
    return { ok: true, text: "", messages: [], exitCode: 0 };
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
  const taskFamily = classifyTaskFamily(text, target);
  const args = codexSotySessionArgs({
    jobDir,
    target,
    source: safeSource,
    outPath,
    threadId: sessionRecord?.threadId || "",
    taskFamily
  });
  const state = {
    threadId: "",
    lastMessage: "",
    messages: [],
    terminal: [],
    terminalKeys: new Set()
  };
  let result = await runCodexForSotyChat(codexBin, args, childEnv, agentReplyTimeoutMs, prompt, state, jobDir, onMessage, onTerminal);
  if (sessionRecord?.threadId && shouldRetryCodexWithoutResume(result, state)) {
    const freshState = {
      threadId: "",
      lastMessage: "",
      messages: [],
      terminal: [],
      terminalKeys: new Set()
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
    result = await runCodexForSotyChat(codexBin, freshArgs, childEnv, agentReplyTimeoutMs, prompt, freshState, jobDir, onMessage, onTerminal);
    state.threadId = freshState.threadId;
    state.lastMessage = freshState.lastMessage;
    state.messages = freshState.messages;
    state.terminal = freshState.terminal;
    state.terminalKeys = freshState.terminalKeys;
  }
  const lastFromFile = existsSync(outPath) ? cleanAgentChatReply(await readFile(outPath, "utf8")) : "";
  const messages = compactCodexMessages(state.messages.length > 0 ? state.messages : [lastFromFile]);
  const finalText = cleanAgentChatReply(messages.join("\n\n") || state.lastMessage || lastFromFile);
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
      recordLearningReceipt({
        kind: "codex-turn",
        family: taskFamily === "generic" ? "no-final-assistant-message" : taskFamily,
        result: "failed",
        route: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume",
        taskSig: taskSignature(text),
        proof: `exitCode=0; messages=${messages.length}; stdout=${result.stdout ? "nonempty" : "empty"}; stderr=${result.stderr ? "nonempty" : "empty"}`,
        exitCode: 125,
        durationMs: Date.now() - startedAt
      });
      return {
        ok: false,
      text: agentFailureText("Codex CLI exited successfully but did not produce a final assistant message."),
        ...(state.terminal.length > 0 ? { terminal: state.terminal } : {}),
        exitCode: 125
      };
    }
    recordLearningReceipt({
      kind: "codex-turn",
      family: taskFamily,
      result: "ok",
      route: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume",
      taskSig: taskSignature(text),
      proof: `exitCode=0; messages=${messages.length}; final=nonempty`,
      exitCode: 0,
      durationMs: Date.now() - startedAt
    });
    return {
      ok: true,
      text: finalText.slice(0, maxChatChars),
      ...(messages.length > 0 ? { messages } : {}),
      ...(state.terminal.length > 0 ? { terminal: state.terminal } : {}),
      exitCode: 0
    };
  }
  recordLearningReceipt({
    kind: "codex-turn",
    family: taskFamily === "generic" ? "codex-cli-nonzero" : taskFamily,
    result: result.exitCode === 124 ? "timeout" : "failed",
    route: target?.id ? "codex.exec.resume+soty-mcp" : "codex.exec.resume",
    taskSig: taskSignature(text),
    proof: `exitCode=${result.exitCode || 1}; stderr=${result.stderr ? "nonempty" : "empty"}; stdout=${result.stdout ? "nonempty" : "empty"}; final=${finalText ? "nonempty" : "empty"}`,
    exitCode: result.exitCode || 1,
    durationMs: Date.now() - startedAt
  });
  return {
    ok: false,
    text: agentFailureText(`${result.stderr}\n${result.stdout}\n${finalText}`),
    ...(state.terminal.length > 0 ? { terminal: state.terminal } : {}),
    exitCode: result.exitCode || 1
  };
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
  args.push("-c", `mcp_servers.soty.command=${JSON.stringify(process.execPath)}`);
  args.push("-c", `mcp_servers.soty.args=${JSON.stringify(mcpArgs)}`);
  for (const tool of ["soty_run", "soty_script", "soty_file", "soty_browser", "soty_desktop", "soty_open_url", "soty_audio", "soty_skill_read"]) {
    args.push("-c", `mcp_servers.soty.tools.${tool}.approval_mode="approve"`);
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
  return "xhigh";
}

function safeCodexReasoningEffort(value) {
  const effort = String(value || "").trim().toLowerCase();
  return ["low", "medium", "high", "xhigh"].includes(effort) ? effort : "";
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

function codexSessionKey(source, target = null) {
  const safe = sanitizeAgentSource(source);
  const targetId = String(target?.id || safe.preferredTargetId || "").trim();
  const key = [safe.tunnelId || safe.deviceId || "default", targetId].filter(Boolean).join("@");
  return key.replace(/[^A-Za-z0-9_.:-]/gu, "_").slice(0, 180);
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
  const preferred = preferredOperatorTarget(safe);
  const mentionedTarget = targetMentionedAtStart(text, runtimeActiveTargets(safe, preferred, sourceTargets));
  if (mentionedTarget) {
    return mentionedTarget;
  }
  if (preferred) {
    return preferred;
  }
  if (safe.deviceId) {
    return {
      id: `agent-source:${safe.deviceId}`,
      label: safe.deviceNick || "source device",
      deviceIds: [safe.deviceId],
      hostDeviceId: safe.deviceId,
      access: true,
      host: true
    };
  }
  const [linked] = sourceAgentLinkTargets(safe);
  if (linked) {
    return linked;
  }
  return null;
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
  const targets = sanitizeTargets(safe.operatorTargets).filter((target) => target.access === true);
  if (targets.length === 0) {
    return null;
  }
  const preferredId = String(safe.preferredTargetId || "").trim().toLowerCase();
  if (preferredId) {
    const byId = targets.find((target) => target.id === safe.preferredTargetId || target.id.toLowerCase() === preferredId);
    if (byId) {
      return byId;
    }
  }
  const preferredLabel = String(safe.preferredTargetLabel || "").trim().toLowerCase();
  if (preferredLabel) {
    const byLabel = targets.find((target) => target.label.toLowerCase() === preferredLabel);
    if (byLabel) {
      return byLabel;
    }
  }
  return null;
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
  const text = String(line || "").trim();
  return /^`?(learning_delta|proof|final_line|finish_skill_edit)\s*=/iu.test(text)
    || /^`?ops-memory\s*:/iu.test(text)
    || /^ops:\s*`?(learning_delta|proof|final_line)\s*=/iu.test(text);
}

async function askCodexRelayFallback(text, context, source = {}, onMessage = null, onTerminal = null) {
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
        source: sanitizeAgentSource(source)
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
  const sourceDeviceId = promptInline(bridgeSourceDeviceId(target, safeSource) || safeSource.deviceId || "");
  const targetLabel = promptInline(target?.label || safeSource.preferredTargetLabel || "");
  const targetId = promptInline(target?.id || safeSource.preferredTargetId || "");
  const activeTargets = runtimeActiveTargets(safeSource, target, sourceTargets)
    .slice(0, 8)
    .map((item) => `${promptInline(item.label)} (${promptInline(item.id)})${item.access ? " access=true" : ""}`)
    .join("\n");
  return {
    userText: String(text || "").trim().slice(0, maxChatChars),
    visibleContext: cleanPromptBlock(context, maxAgentContextChars),
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
    memory: await codexLearningMemoryPrompt()
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

async function writeCodexRuntimeFiles(jobDir, runtimeContext) {
  if (!jobDir) {
    return;
  }
  await mkdir(jobDir, { recursive: true });
  const agents = [
    "# Soty Codex Runtime",
    "",
    "This is a generated Soty server-Codex workspace. It is not automatically the user's project checkout.",
    "",
    "Operating contract:",
    "- Read the current prompt's Soty runtime packet before acting, but treat the current user request as the authoritative task.",
    "- Answer in the user's language unless they ask otherwise.",
    "- For install, reinstall, repair, reset, recovery, boot-media, flashing, OS, package, service, shell, SSH, device, MCP, Codex skill, skill sync, or operational memory tasks, apply the ops operating contract internally: prove target, act narrowly, verify, learn, and close.",
    "- $ops is one canonical skill at skills/universal-install-ops; `ops` is a name/alias, not a second package copy.",
    "- If the $ops body is needed, use the Soty MCP tool `soty_skill_read` with skill `ops` and path `SKILL.md`.",
    "- Keep $ops, skill routing, route selection, helper names, preflight/gate language, MCP names, and bridge/source-scoped mechanics internal unless the user explicitly asks for technical details.",
    "- In Soty chat, do not announce skills, $ops, routes, helpers, MCP tool names, or source-scoped mechanics; show the user only the action, result, or one plain blocker.",
    "- Write like a practical IDE Codex: concise, warm, decisive, and action-oriented. The user should see what you did or need next, not how the routing machinery works.",
    "- Do not show raw tool errors or codes such as agent-source 404, exitCode, timeoutMs, stack traces, helper names, or JSON snippets in user-facing chat; translate them into one plain sentence.",
    "- Before tools, either act silently or send one short plain-language progress line. Do not narrate every probe, retry, helper, or memory lookup.",
    "- The local shell belongs to the server agent runtime. Use it only for server/runtime/repo work that the prompt clearly targets.",
    "- For work on a paired user device, use Soty MCP tools: soty_run, soty_script, soty_file, soty_browser, soty_desktop, soty_open_url, soty_audio.",
    "- Use `soty_script` with `shell: \"powershell\"` for any PowerShell variables, pipelines, semicolons, or multi-step checks. Reserve `soty_run` for trivial one-line commands.",
    "- If a Soty target tool returns missing target, timeout, malformed command output, or nonzero exit, repair or prove that exact target channel once, then name one plain blocker instead of continuing through the local server shell.",
    "- For quick identity, health, and readiness probes, pass a short timeoutMs such as 15000-45000. Reserve timeoutMs up to 7200000 for real long-running installs, downloads, repairs, scans, or staged scripts after the target is proven.",
    "- For destructive or long device work such as format, diskpart, robocopy, dism, installers, downloads, reset, or reboot, issue exactly one target tool call at a time; wait for that call to return or name a blocker before starting any second write, format, reset, or reboot command.",
    "- Do not use the local shell for target-device actions. If the target device is missing or a Soty tool fails, repair or prove the narrow channel once, then name one plain blocker and one next action.",
    "- For Soty-managed clean Windows reinstall, the happy path is mandatory: prove machine worker, stage/run the managed prepare script from the current /agent/manifest.json windowsReinstall URLs after verifying the listed SHA-256, wait for ready.json, read backupProof, then ask the exact confirmation phrase and run the managed arm script. Do not use Media Creation Tool, Windows Settings reset, generic installer GUI, or Shift+F10/localonly as the planned path.",
    "- Before any reinstall arm/reboot, backupProof must show backupRoot, Wi-Fi export result, exported drivers, Soty restore/postinstall artifacts, root Autounattend.xml, and OEM SetupComplete fallback. If those artifacts are missing, repair preparation instead of asking the user to click OOBE screens.",
    "- For Windows reinstall/reset, do not ask for destructive confirmations until target identity, control channel, backup/data intent, USB scope if needed, BitLocker/recovery safety, and return path are proven. Ask at most one plain question at a time.",
    "- If a Windows reinstall/reset is blocked only because the target Soty channel is unavailable, answer in no more than three short sentences: `Переустановку не начал. Я пока не вижу <device> через Soty. Открой/перезапусти Soty Agent на этом ПК и напиши «готово».`",
    "- For project work, detect the real project/root before editing. If the project is on the source device, operate through Soty MCP; if it is the server checkout, state that boundary.",
    "- Preserve multi-turn continuity: use the resumed session, the visible Soty shared-text context, and the learning memory snapshot.",
    "- Verify changes with the smallest useful proof, record reusable learning when behavior changes, and keep user-facing explanations simple.",
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
    "",
    "## Learning Memory",
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
    memory: ""
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
    "- local_shell_scope: server agent runtime only; use Soty MCP for paired device work.",
    "- ops_rule: apply the ops operating contract internally for system/device/install/repair/package/service/skill/memory work; read it with soty_skill_read skill=ops path=SKILL.md only when detailed route context is needed.",
    "- communication_rule: keep $ops/skills/router/helper/preflight/gate/MCP/bridge/source-scoped details internal; answer in short plain language like IDE Codex.",
    "- error_translation_rule: do not show raw tool errors/codes such as agent-source 404, exitCode, timeoutMs, stack traces, helper names, or JSON snippets; translate to a human sentence.",
    "- progress_rule: do not narrate every probe or retry; send at most one short progress line before a long wait, then the result or one blocker.",
    "- powershell_tool_rule: use soty_script with shell=\"powershell\" for PowerShell variables, semicolons, pipelines, or multi-step checks. Use soty_run only for trivial one-line commands.",
    "- target_failure_rule: if a Soty target tool returns missing target, timeout, malformed command output, or nonzero exit, repair or prove that exact target channel once, then give one plain blocker; do not continue through the local server shell unless the server/runtime is explicitly the target.",
    "- timeout_rule: use timeoutMs 15000-45000 for quick identity/health/readiness probes; use timeoutMs up to 7200000 only for real long-running jobs after the target is proven.",
    "- serial_long_job_rule: for destructive or long target work, call only one soty_* write/format/reset/reboot job at a time; wait for its result or name a blocker before launching another.",
    "- managed_reinstall_rule: clean Windows reinstall through Soty must use the managed prepare/ready/backupProof/arm flow from the current /agent/manifest.json windowsReinstall URLs with SHA-256 verification; manual OOBE, MCT GUI, Settings reset, or Shift+F10 local account steps are recovery fallbacks, not the normal answer.",
    "- backup_proof_rule: before asking for the destructive reinstall phrase, prove backupProof with backup root, Wi-Fi profile export result, driver export result, Soty restore/postinstall assets, Autounattend.xml, and OEM SetupComplete fallback.",
    "- reinstall_rule: for Windows reinstall/reset, ask at most one plain question at a time and do not ask for destructive confirmation until control, backup/data intent, USB scope, BitLocker/recovery safety, and return path are proven.",
    "- missing_channel_reinstall_rule: if reinstall/reset is blocked only by an unavailable target Soty channel, answer in <=3 short sentences: not started; cannot see <device> through Soty; open/restart Soty Agent there and reply ready.",
    "- project_rule: detect the real project/root before editing; do not assume this generated workspace is the user's project.",
    "- continuity_rule: use the visible shared-text context and resumed session; do not answer from only the latest sentence when context is present.",
    "",
    "Learning memory snapshot:",
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
    "Use the user message above as the task. Treat service context, learning memory, and any skill text as supporting material only."
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
    cachedCodexLearningMemoryText = "server learning unavailable: relay is not configured";
    return cachedCodexLearningMemoryText;
  }
  const report = await Promise.race([
    fetchLearningTeacherReport(500).catch((error) => ({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    })),
    sleep(2500).then(() => ({ ok: false, error: "teacher timeout" }))
  ]);
  cachedCodexLearningMemoryAt = now;
  cachedCodexLearningMemoryText = formatCodexLearningMemory(report).slice(0, 4000);
  return cachedCodexLearningMemoryText;
}

function formatCodexLearningMemory(report) {
  if (!report?.ok) {
    return `server learning unavailable: ${cleanLearningText(report?.error || "unknown", 160)}`;
  }
  const lines = [
    `teacher=${report.schema || "soty.learning.teacher"} receipts=${Number(report.receipts || 0)} source=${cleanLearningText(report.source || "", 80)}`,
    `scope=${formatLearningScope(report)}`,
    `publish=${formatLearningPublishModel(report)}`
  ];
  const recommendations = Array.isArray(report.recommendations) ? report.recommendations.slice(0, 4) : [];
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

function sourceAgentLinkTargets(source) {
  const matches = sourceMatchedOperatorTargets(source);
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

function agentFailureText(details) {
  const value = String(details || "");
  let reason = "no final assistant message";
  if (value.includes("Missing environment variable")) {
    reason = "missing auth or API key";
  } else if (value.includes("403 Forbidden") || value.includes("Unable to load site")) {
    reason = "OpenAI/ChatGPT transport rejected the Codex CLI request";
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
  await installOpsSkillForCodexHome(target);
  return target;
}

async function installOpsSkillForCodexHome(codexHome) {
  const source = opsSkillSourceDir();
  if (!source) {
    return false;
  }
  const skillsDir = join(codexHome, "skills");
  await mkdir(skillsDir, { recursive: true });
  const target = join(skillsDir, "universal-install-ops");
  if (!sameFsPath(source, target)) {
    await rm(target, { recursive: true, force: true }).catch(() => undefined);
    await cp(source, target, {
      recursive: true,
      force: true,
      filter: (path) => {
        const normalized = String(path || "").replace(/\\/gu, "/");
        return !/(^|\/)(\.git|\.skill-memory|__pycache__)(\/|$)/u.test(normalized)
          && !/\.pyc$/iu.test(normalized);
      }
    });
  }
  await rm(join(skillsDir, "ops"), { recursive: true, force: true }).catch(() => undefined);
  return true;
}

function sameFsPath(left, right) {
  const leftPath = resolve(left).replace(/\\/gu, "/");
  const rightPath = resolve(right).replace(/\\/gu, "/");
  return process.platform === "win32"
    ? leftPath.toLowerCase() === rightPath.toLowerCase()
    : leftPath === rightPath;
}

function opsSkillSourceDir() {
  const candidates = [
    join(agentDir, "skill-sources", "universal-install-ops-skill"),
    join(chooseCodexAuthHome(), "skills", "universal-install-ops"),
    join(homedir(), ".codex", "skills", "universal-install-ops")
  ];
  return candidates.find((candidate) => candidate && existsSync(join(candidate, "SKILL.md"))) || "";
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
  return stockCodexPathCandidates().find((candidate) => candidate && existsSync(candidate)) || "";
}

function hasCodexBinary() {
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
      await writeFile(ctlPath, `@echo off\r\n"${process.execPath}" "${scriptPath}" ctl %*\r\n`, "utf8");
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
        name: "soty_run",
        description: "Run one trivial shell command on the current Soty Agent LINK source device. The command and output are shown in the user's LINK console. Do not use for PowerShell variables, semicolons, pipelines, or multi-step checks; use soty_script with shell=\"powershell\" instead. Do not use this to read Codex skill files from CODEX_HOME; those are in the operator runtime, not on the source device. Use soty_skill_read for skill files.",
        inputSchema: {
          type: "object",
          properties: {
            command: { type: "string", description: "Exact trivial command to run on the source device. Do not include Soty target wrappers. For PowerShell workflows with $, ;, |, or multiple statements, use soty_script." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-7200000. Use 15000-45000 for quick probes; use a long timeout only for install, download, repair, reset, or reinstall jobs that are expected to keep running." }
          },
          required: ["command"],
          additionalProperties: false
        }
      },
      {
        name: "soty_script",
        description: "Run a multiline script on the current Soty Agent LINK source device. Use for PowerShell workflows, checks, and browser automation launched on that device.",
        inputSchema: {
          type: "object",
          properties: {
            script: { type: "string", description: "Script body to run on the source device." },
            shell: { type: "string", description: "Optional shell hint, usually powershell on Windows." },
            name: { type: "string", description: "Short technical label shown in the LINK console." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-7200000. Use 15000-45000 for quick probes; use a long timeout only for install, download, repair, reset, or reinstall jobs that are expected to keep running." }
          },
          required: ["script"],
          additionalProperties: false
        }
      },
      {
        name: "soty_open_url",
        description: "Open a URL in the default browser on the current Soty Agent LINK source device. Do not use this as a Windows reinstall handoff; for reinstall/reset, continue with source-scoped preflight, media staging, and exact blocker proof.",
        inputSchema: {
          type: "object",
          properties: {
            url: { type: "string", description: "URL to open on the source device." },
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-7200000." }
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
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-7200000." }
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
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-7200000." }
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
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-7200000." }
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
            timeoutMs: { type: "integer", description: "Timeout in milliseconds, 1000-7200000. Default is 120000 to survive cold Windows audio startup." }
          },
          additionalProperties: false
        }
      },
      {
        name: "soty_skill_read",
        description: "Read a file from the Codex skill directory loaded for this session. Use this before applying a skill such as $ops when SKILL.md or a referenced skill file is needed. This is read-only and does not run commands on the source device.",
        inputSchema: {
          type: "object",
          properties: {
            skill: { type: "string", description: "Skill name, for example ops or universal-install-ops. Defaults to ops." },
            path: { type: "string", description: "Relative file path inside the skill directory. Defaults to SKILL.md." },
            maxChars: { type: "integer", description: "Maximum characters to return, 1000-60000." }
          },
          additionalProperties: false
        }
      }
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
    if (name === "soty_skill_read") {
      const result = await readCodexSkillFile(args);
      return mcpToolText(result.text, !result.ok, result.exitCode);
    }
    if (!mcpTarget || !mcpSourceDeviceId) {
      return mcpToolText("! agent-source: current Soty Agent LINK source is not attached", true);
    }
    if (name === "soty_run") {
      const command = String(args.command || "").trim();
      if (!command) {
        return mcpToolText("! command", true);
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
      return mcpToolText(result.text || "", !result.ok, result.exitCode);
    }
    if (name === "soty_script") {
      const script = String(args.script || "").trim();
      if (!script) {
        return mcpToolText("! script", true);
      }
      const result = await mcpPostOperator("/operator/script", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script,
        shell: String(args.shell || ""),
        name: String(args.name || "script"),
        timeoutMs: mcpSafeTimeout(args.timeoutMs, defaultTimeoutMs)
      });
      return mcpToolText(result.text || "", !result.ok, result.exitCode);
    }
    if (name === "soty_file") {
      const action = String(args.action || "").trim().toLowerCase();
      const path = String(args.path || "").trim();
      if (!action || !path) {
        return mcpToolText("! file", true);
      }
      const result = await mcpPostOperator("/operator/script", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        script: sourceFileScript(args),
        shell: "powershell",
        name: `soty-file-${action}`.slice(0, 120),
        timeoutMs: mcpSafeTimeout(args.timeoutMs, defaultTimeoutMs)
      });
      return mcpToolText(result.text || "", !result.ok, result.exitCode);
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
        shell: "powershell",
        name: `soty-browser-${action}`.slice(0, 120),
        timeoutMs: mcpSafeTimeout(args.timeoutMs, 10 * 60_000)
      });
      return mcpToolText(result.text || "", !result.ok, result.exitCode);
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
      return mcpToolText(result.text || "", !result.ok, result.exitCode);
    }
    if (name === "soty_open_url") {
      const url = String(args.url || "").trim();
      if (!/^https?:\/\//iu.test(url)) {
        return mcpToolText("! url", true);
      }
      const result = await mcpPostOperator("/operator/run", {
        target: mcpTarget,
        sourceDeviceId: mcpSourceDeviceId,
        command: `Start-Process ${quotePowerShell(url)}`,
        timeoutMs: mcpSafeTimeout(args.timeoutMs, 60_000)
      });
      return mcpToolText(result.text || "opened", !result.ok, result.exitCode);
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
      return mcpToolText(result.text || "", !result.ok, result.exitCode);
    }
    return mcpToolText(`! unknown tool ${name}`, true);
  }

  async function readCodexSkillFile(args) {
    const skillName = normalizeMcpSkillName(String(args.skill || "ops"));
    const relativePath = String(args.path || "SKILL.md").trim() || "SKILL.md";
    const maxChars = Number.isSafeInteger(args.maxChars) ? Math.max(1000, Math.min(args.maxChars, 60000)) : 24000;
    if (!skillName || !/^[A-Za-z0-9_.-]+$/u.test(skillName)) {
      return { ok: false, text: "! skill", exitCode: 2 };
    }
    if (relativePath.includes("\0") || /(^|[\\/])\.\.([\\/]|$)/u.test(relativePath)) {
      return { ok: false, text: "! skill-path", exitCode: 2 };
    }
    const skillRoot = mcpSkillRoot(skillName);
    if (!skillRoot) {
      return { ok: false, text: "! skill-read: skill is not installed", exitCode: 1 };
    }
    const fullPath = resolve(skillRoot, relativePath);
    const rel = relative(skillRoot, fullPath);
    if (!rel || rel.startsWith("..") || isAbsolute(rel)) {
      return { ok: false, text: "! skill-path", exitCode: 2 };
    }
    try {
      const text = await readFile(fullPath, "utf8");
      return {
        ok: true,
        text: text.slice(0, maxChars),
        exitCode: 0
      };
    } catch (error) {
      return {
        ok: false,
        text: `! skill-read: ${error instanceof Error ? error.message : String(error)}`,
        exitCode: 1
      };
    }
  }

  function normalizeMcpSkillName(value) {
    const text = String(value || "").trim().toLowerCase();
    if (!text || text === "ops" || text === "universal-install-ops" || text === "universal-install-ops-skill") {
      return "universal-install-ops";
    }
    return text;
  }

  function mcpSkillRoot(skillName) {
    if (skillName === "universal-install-ops") {
      return opsSkillSourceDir() || "";
    }
    const skillsRoot = join(chooseCodexAuthHome(), "skills");
    const root = resolve(skillsRoot, skillName);
    return existsSync(join(root, "SKILL.md")) ? root : "";
  }

  async function mcpPostOperator(path, body) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Origin: "https://xn--n1afe0b.online"
        },
        body: JSON.stringify({
          ...body,
          ...(mcpSourceRelayId ? { sourceRelayId: mcpSourceRelayId } : {})
        })
      });
      const payload = await response.json();
      return {
        ok: Boolean(response.ok && payload?.ok),
        text: String(payload?.text || ""),
        exitCode: Number.isSafeInteger(payload?.exitCode) ? payload.exitCode : (response.ok ? 0 : response.status)
      };
    } catch (error) {
      return {
        ok: false,
        text: error instanceof Error ? error.message : String(error),
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

  function mcpSafeTimeout(value, fallback) {
    return Number.isSafeInteger(value) ? Math.max(1000, Math.min(value, maxLongTaskTimeoutMs)) : fallback;
  }

  function quotePowerShell(value) {
    return `'${String(value).replace(/'/gu, "''")}'`;
  }

  function sendMcp(message) {
    process.stdout.write(`${JSON.stringify(message)}\n`);
  }
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
$ErrorActionPreference = 'Stop'
$req = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('${payload}')) | ConvertFrom-Json
function Full-SotyPath([string]$Path) {
  if ([string]::IsNullOrWhiteSpace($Path)) { throw 'empty path' }
  $expanded = [Environment]::ExpandEnvironmentVariables($Path)
  if (-not [IO.Path]::IsPathRooted($expanded)) { $expanded = Join-Path (Get-Location).Path $expanded }
  [IO.Path]::GetFullPath($expanded)
}
function Emit($Value) { $Value | ConvertTo-Json -Depth 8 -Compress }
$action = ([string]$req.action).Trim().ToLowerInvariant()
$path = Full-SotyPath ([string]$req.path)
$maxResults = [Math]::Max(1, [Math]::Min(500, [int]$req.maxResults))
$maxChars = [Math]::Max(1000, [Math]::Min(12000, [int]$req.maxChars))
switch ($action) {
  'stat' {
    $item = Get-Item -LiteralPath $path -Force
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$item.FullName; type=$(if ($item.PSIsContainer) { 'directory' } else { 'file' }); length=$item.Length; updated=$item.LastWriteTimeUtc.ToString('o') })
  }
  'list' {
    $items = if ($req.recursive -eq $true) { Get-ChildItem -LiteralPath $path -Force -Recurse -ErrorAction SilentlyContinue | Select-Object -First $maxResults } else { Get-ChildItem -LiteralPath $path -Force -ErrorAction SilentlyContinue | Select-Object -First $maxResults }
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$path; items=@($items | ForEach-Object { [pscustomobject]@{ name=$_.Name; path=$_.FullName; type=$(if ($_.PSIsContainer) { 'directory' } else { 'file' }); length=$_.Length; updated=$_.LastWriteTimeUtc.ToString('o') } }) })
  }
  'read' {
    $text = Get-Content -LiteralPath $path -Raw -ErrorAction Stop
    if ($text.Length -gt $maxChars) { $text = $text.Substring(0, $maxChars) }
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$path; text=$text })
  }
  'write' {
    $dir = Split-Path -Parent $path
    if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
    Set-Content -LiteralPath $path -Value ([string]$req.content) -Encoding UTF8 -NoNewline
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$path; bytes=([Text.Encoding]::UTF8.GetByteCount([string]$req.content)) })
  }
  'append' {
    $dir = Split-Path -Parent $path
    if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
    Add-Content -LiteralPath $path -Value ([string]$req.content) -Encoding UTF8
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$path; bytes=([Text.Encoding]::UTF8.GetByteCount([string]$req.content)) })
  }
  'mkdir' {
    $item = New-Item -ItemType Directory -Force -Path $path
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$item.FullName })
  }
  'move' {
    $to = Full-SotyPath ([string]$req.toPath)
    $dir = Split-Path -Parent $to
    if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
    Move-Item -LiteralPath $path -Destination $to -Force
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$path; toPath=$to })
  }
  'copy' {
    $to = Full-SotyPath ([string]$req.toPath)
    $dir = Split-Path -Parent $to
    if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
    Copy-Item -LiteralPath $path -Destination $to -Force -Recurse:($req.recursive -eq $true)
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$path; toPath=$to })
  }
  'delete' {
    Remove-Item -LiteralPath $path -Force -Recurse:($req.recursive -eq $true)
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$path })
  }
  'search' {
    $pattern = [string]$req.pattern
    if ([string]::IsNullOrWhiteSpace($pattern)) { throw 'empty pattern' }
    $root = Get-Item -LiteralPath $path -Force
    $glob = if ([string]::IsNullOrWhiteSpace([string]$req.glob)) { '*' } else { [string]$req.glob }
    $files = if ($root.PSIsContainer) {
      Get-ChildItem -LiteralPath $root.FullName -Recurse -File -ErrorAction SilentlyContinue | Where-Object { $_.Name -like $glob }
    } else { @($root) }
    $matches = New-Object System.Collections.Generic.List[object]
    foreach ($file in $files) {
      if ($matches.Count -ge $maxResults) { break }
      try {
        $found = if ($req.regex -eq $true) { Select-String -LiteralPath $file.FullName -Pattern $pattern -ErrorAction Stop } else { Select-String -LiteralPath $file.FullName -Pattern $pattern -SimpleMatch -ErrorAction Stop }
        foreach ($m in $found) {
          $matches.Add([pscustomobject]@{ path=$m.Path; line=$m.LineNumber; text=([string]$m.Line).Trim() })
          if ($matches.Count -ge $maxResults) { break }
        }
      } catch {}
    }
    Emit ([pscustomobject]@{ ok=$true; action=$action; path=$path; pattern=$pattern; matches=@($matches) })
  }
  default { throw "unsupported file action: $action" }
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
const { spawn } = require("node:child_process");
const req = JSON.parse(Buffer.from(process.argv[2] || "", "base64").toString("utf8"));
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
async function ensureBrowser() {
  try {
    await fetchJson(base + "/json/version");
    return;
  } catch {}
  const exe = browserCandidates().find((candidate) => process.platform !== "win32" || fs.existsSync(candidate)) || browserCandidates()[0];
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
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
`, "utf8").toString("base64");
  return `
$ErrorActionPreference = 'Stop'
$dir = Join-Path $env:TEMP 'soty-browser'
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$scriptPath = Join-Path $dir 'soty-browser-driver.cjs'
$driver = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('${driver}'))
Set-Content -LiteralPath $scriptPath -Value $driver -Encoding UTF8
& node $scriptPath '${request}'
$code = $LASTEXITCODE
if ($code -ne 0) {
  $req = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('${request}')) | ConvertFrom-Json
  $action = ([string]$req.action).Trim().ToLowerInvariant()
  if (($action -eq 'open' -or $action -eq 'goto') -and -not [string]::IsNullOrWhiteSpace([string]$req.url)) {
    Start-Process ([string]$req.url)
    @{ ok=$true; action=$action; url=([string]$req.url); mode='start-process-fallback' } | ConvertTo-Json -Compress
    exit 0
  }
}
exit $code
`.trim();
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
    if (subcommand === "run") {
      const parsed = parseActionCtlOptions(args.slice(2));
      const target = parsed.args[0] || "";
      const remoteCommand = parsed.args.slice(1).join(" ");
      if (!target || !remoteCommand) {
        process.stderr.write("sotyctl action run [--family=name] [--kind=name] [--risk=low|medium|high|destructive] [--idempotency-key=key] [--detached] [--source-device=id] [--timeout=ms] <target> <command>\n");
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
        process.stderr.write("sotyctl action script [--family=name] [--kind=name] [--risk=low|medium|high|destructive] [--idempotency-key=key] [--detached] [--source-device=id] [--timeout=ms] <target> <file> [shell]\n");
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
    process.stderr.write("sotyctl action list | action status <job-id> | action run <target> <command> | action script <target> <file> [shell]\n");
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
  if (command === "learn-sync" || command === "learning-sync" || (command === "learn" && args[1] === "sync")) {
    const result = await syncLearningOutbox().catch(() => ({ ok: false, sent: 0, pending: 0 }));
    process.stdout.write(`soty-learning: ok=${result.ok ? "true" : "false"} sent=${result.sent || 0} pending=${result.pending || 0}\n`);
    process.exit(result.ok ? 0 : 1);
  }
  if (command === "learn-doctor" || command === "learn-teacher" || command === "learning-doctor" || command === "learning-teacher" || (command === "learn" && (args[1] === "doctor" || args[1] === "teacher"))) {
    const rest = command === "learn" ? args.slice(2) : args.slice(1);
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
      process.stdout.write(`${JSON.stringify({ ok: sync.ok && report.ok, sync, teacher: report }, null, 2)}\n`);
    } else {
      process.stdout.write(`${formatLearningTeacherReport(sync, report)}\n`);
    }
    process.exit(sync.ok && report.ok ? 0 : 1);
  }
  if (command === "learn-review-merge" || command === "learning-review-merge" || command === "learn-global-review" || command === "learning-global-review" || (command === "learn" && (args[1] === "review-merge" || args[1] === "merge" || args[1] === "global-review" || args[1] === "global-review-merge"))) {
    const rest = command === "learn" ? args.slice(2) : args.slice(1);
    const options = parseLearningReviewMergeOptions(rest);
    const report = await runLearningReviewMerge(rest);
    if (options.json) {
      process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    } else {
      process.stdout.write(`${formatLearningReviewMergeReport(report)}\n`);
    }
    process.exit(report.ok && (!options.strict || !report.blockedByReview) ? 0 : 1);
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
  process.stderr.write("sotyctl health | list | action list|status|run|script | run [--source-device=id] [--timeout=ms] <target> <command> | script [--source-device=id] [--timeout=ms] <target> <file> [shell] | install-machine <target> | machine-status <target> | access <target> | say [--fast|--slow] <target> <text> | agent-message [--timeout=ms] [agent-tunnel-id] <text> | read [target] | listen [target] | export [file] | learn-sync | learn doctor [--json] [--limit=n] | learn review-merge|global-review [--dry-run] [--strict] | import <file>\n");
  process.exit(2);
}

function parseActionCtlOptions(args) {
  const rest = [...args];
  let timeoutMs = 0;
  let sourceDeviceId = "";
  let sourceRelayId = "";
  let family = "";
  let kind = "";
  let risk = "";
  let shell = "";
  let idempotencyKey = "";
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
  return { timeoutMs, sourceDeviceId, sourceRelayId, family, kind, risk, shell, idempotencyKey, detached, args: rest };
}

function actionCtlRequestOptions(parsed) {
  return {
    ...(parsed.sourceDeviceId ? { sourceDeviceId: parsed.sourceDeviceId } : {}),
    ...(parsed.sourceRelayId ? { sourceRelayId: parsed.sourceRelayId } : {}),
    ...(parsed.timeoutMs ? { timeoutMs: parsed.timeoutMs } : {}),
    ...(parsed.family ? { family: parsed.family } : {}),
    ...(parsed.kind ? { kind: parsed.kind } : {}),
    ...(parsed.risk ? { risk: parsed.risk } : {}),
    ...(parsed.idempotencyKey ? { idempotencyKey: parsed.idempotencyKey } : {}),
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
    codexBinary: Boolean(findCodexBinary()),
    codexAuth: hasCodexAuth(),
    codexMode: codexFullLocalTools ? "stock-cli-full-local-tools" : "stock-cli-bridge",
    codexSessionMode,
    codexRuntimeContext: "prompt+AGENTS+SOTY_CONTEXT+learning-memory+always-on-soty-mcp",
    codexProxy: Boolean(codexProxyUrl),
    codexProxyScheme: proxyScheme(codexProxyUrl),
    learning: learningStatus(),
    opsSkill: opsSkillStatus(),
    ...(process.platform === "win32" ? {
      windowsUser: windowsUserName(),
      system: isWindowsSystem(),
      maintenance: agentScope === "Machine" && isWindowsSystem()
    } : {})
  };
}

function opsSkillStatus() {
  const source = opsSkillSourceDir();
  if (!source) {
    return { installed: false };
  }
  let marker = null;
  try {
    marker = JSON.parse(readFileSync(join(source, ".soty-skill-bundle.json"), "utf8"));
  } catch {
    marker = null;
  }
  return {
    installed: true,
    bundled: source.startsWith(join(agentDir, "skill-sources")),
    ...(typeof marker?.tarSha256 === "string" ? { tarSha256: marker.tarSha256 } : {}),
    ...(typeof marker?.revision === "string" && marker.revision ? { revision: marker.revision } : {})
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
    await maybeUpdateOpsSkillFromManifest(manifest, updateManifestUrl).catch(() => undefined);
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

async function maybeUpdateOpsSkillFromManifest(manifest, manifestUrl) {
  const skill = manifest?.opsSkill;
  if (!isSafeOpsSkillManifest(skill)) {
    return false;
  }
  const target = join(agentDir, "skill-sources", "universal-install-ops-skill");
  if (existsSync(join(target, "SKILL.md"))) {
    const marker = await readJsonFile(join(target, ".soty-skill-bundle.json"));
    if (marker?.tarSha256 === skill.tarSha256) {
      return false;
    }
  }

  const archiveUrl = new URL(skill.tarUrl, manifestUrl);
  if (!/^https?:$/iu.test(archiveUrl.protocol)) {
    return false;
  }
  const archiveResponse = await fetch(archiveUrl, { cache: "no-store" });
  if (!archiveResponse.ok) {
    return false;
  }
  const archiveBytes = Buffer.from(await archiveResponse.arrayBuffer());
  if (sha256(archiveBytes) !== skill.tarSha256) {
    return false;
  }

  const tempRoot = join(tmpdir(), "soty-agent-skill", randomUUID());
  const skillRoot = join(agentDir, "skill-sources");
  const next = join(skillRoot, `universal-install-ops-skill.next-${process.pid}`);
  const backup = join(skillRoot, `universal-install-ops-skill.backup-${process.pid}`);
  try {
    await mkdir(tempRoot, { recursive: true });
    const archivePath = join(tempRoot, "ops-skill.tar.gz");
    await writeFile(archivePath, archiveBytes);
    await extractTarGz(archivePath, tempRoot);
    const inner = join(tempRoot, skill.root || "universal-install-ops-skill");
    if (!existsSync(join(inner, "SKILL.md"))) {
      return false;
    }

    await mkdir(skillRoot, { recursive: true });
    await rm(next, { recursive: true, force: true }).catch(() => undefined);
    await rm(backup, { recursive: true, force: true }).catch(() => undefined);
    await cp(inner, next, {
      recursive: true,
      force: true,
      filter: (path) => {
        const normalized = String(path || "").replace(/\\/gu, "/");
        return !/(^|\/)(\.git|\.skill-memory|__pycache__)(\/|$)/u.test(normalized)
          && !/\.pyc$/iu.test(normalized);
      }
    });
    await writeFile(join(next, ".soty-skill-bundle.json"), JSON.stringify({
      tarSha256: skill.tarSha256,
      revision: typeof skill.revision === "string" ? skill.revision : "",
      installedAt: new Date().toISOString()
    }, null, 2), "utf8");
    if (existsSync(target)) {
      await rename(target, backup);
    }
    try {
      await rename(next, target);
      await rm(backup, { recursive: true, force: true }).catch(() => undefined);
    } catch (error) {
      if (!existsSync(target) && existsSync(backup)) {
        await rename(backup, target).catch(() => undefined);
      }
      throw error;
    }
    return true;
  } finally {
    await rm(tempRoot, { recursive: true, force: true }).catch(() => undefined);
    await rm(next, { recursive: true, force: true }).catch(() => undefined);
    await rm(backup, { recursive: true, force: true }).catch(() => undefined);
  }
}

function extractTarGz(archivePath, destinationDir) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawnCommand("tar", ["-xzf", archivePath, "-C", destinationDir], {
      windowsHide: true,
      stdio: ["ignore", "ignore", "pipe"]
    });
    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr = `${stderr}${chunk.toString("utf8")}`.slice(-4000);
    });
    child.on("error", rejectPromise);
    child.on("close", (exitCode) => {
      if (exitCode === 0) {
        resolvePromise();
      } else {
        rejectPromise(new Error(`tar failed: ${stderr || exitCode}`));
      }
    });
  });
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

function isSafeOpsSkillManifest(value) {
  return value
    && typeof value === "object"
    && value.name === "universal-install-ops"
    && typeof value.root === "string"
    && /^[A-Za-z0-9_.-]{1,80}$/u.test(value.root)
    && typeof value.tarUrl === "string"
    && value.tarUrl.length <= 300
    && /^[a-f0-9]{64}$/u.test(value.tarSha256);
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
