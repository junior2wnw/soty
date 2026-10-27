#!/usr/bin/env node
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { defaultGonkaProxyModel, glmProxyModel } from "../server/gonka-proxy.js";

const root = await mkdtemp(path.join(tmpdir(), "soty-connector-integration-"));
const serverPort = await freePort();
const connectorPort = await freePort();
const connectorDataDir = path.join(root, "connector-data");
const baseUrl = `http://127.0.0.1:${serverPort}`;
const linkId = "i".repeat(43);
const controllerLinkId = "j".repeat(43);
const deviceId = "integration-device";
const controllerDeviceId = "integration-controller";
const processes = [];
const openCodePath = String(process.env.SOTY_OPENCODE_E2E_PATH || "").trim();
const connectorRuntime = String(process.env.SOTY_CONNECTOR_E2E_RUNTIME || "scripts/soty-connector.mjs").trim();
let gonkaServer;
let gonkaRequests = [];
let gonkaPort = 0;
let connector, connectorEnv;
const modelCases = [];
const childClosures = new WeakMap();
const evidenceDir = String(process.env.SOTY_CONNECTOR_E2E_EVIDENCE_DIR || "").trim();

try {
  gonkaPort = openCodePath ? await freePort() : 0;
  if (openCodePath) {
    ({ server: gonkaServer, requests: gonkaRequests } = await startFakeGonka(gonkaPort));
  }
  const server = start(process.execPath, ["server/index.js"], {
    HOST: "127.0.0.1",
    PORT: String(serverPort),
    DATA_DIR: path.join(root, "server-data"),
    ...(openCodePath ? {
      SOTY_GONKA_API_KEY: "integration-server-gonka-key",
      SOTY_GONKA_BASE_URL: `http://127.0.0.1:${gonkaPort}/v1`,
      // /ready also checks the application proxy; this is an own synthetic
      // localhost credential, not an inherited provider/application secret.
      SOTY_GONKA_APPLICATION_TOKENS: JSON.stringify({ "integration-app": "a".repeat(43) })
    } : {})
  });
  processes.push(server);
  await waitJson(`${baseUrl}/health`, (value) => value.ok === true);

  await mkdir(connectorDataDir, { recursive: true });
  await writeFile(path.join(connectorDataDir, "connector-config.json"), `${JSON.stringify({
    workspaceRoot: path.join(root, "removed-workspace"),
    allowedRoots: [path.join(root, "removed-workspace")],
    ...(openCodePath ? { gonkaModel: "MiniMaxAI/MiniMax-M2.7" } : {})
  })}\n`, "utf8");

  connectorEnv = {
    SOTY_CONNECTOR_PORT: String(connectorPort),
    SOTY_CONNECTOR_DATA_DIR: connectorDataDir,
    SOTY_CONNECTOR_LINK_ID: linkId,
    SOTY_CONNECTOR_SERVER_URL: baseUrl,
    SOTY_CONNECTOR_DEVICE_ID: deviceId,
    SOTY_CONNECTOR_DEVICE_NICK: "Integration",
    SOTY_CONNECTOR_SCOPE: "Dev",
    SOTY_CONNECTOR_AUTO_UPDATE: "0",
    ...(process.platform === "win32" ? {
      PATH: path.dirname(process.execPath),
      Path: path.dirname(process.execPath)
    } : {}),
    ...(openCodePath ? {
      SOTY_OPENCODE_PATH: openCodePath,
      SOTY_CONNECTOR_MODEL: defaultGonkaProxyModel
    } : {})
  };
  connector = start(process.execPath, [connectorRuntime, ...(openCodePath ? ["--model", glmProxyModel] : [])], connectorEnv);
  processes.push(connector);
  const connectorHealth = await waitJson(`http://127.0.0.1:${connectorPort}/health`, (value) => value.ok === true && value.connector === true);
  assert.equal(connectorHealth.package?.wrapper?.id, "soty-connector");
  assert.equal(connectorHealth.package?.agent?.id, "opencode");
  assert.equal(connectorHealth.update?.enabled, false);
  assert.equal(connectorHealth.update?.lastResult, "disabled");
  const retiredStatus = await fetch(`http://127.0.0.1:${connectorPort}/integrations/spreadex/v1/status`);
  assert.equal(retiredStatus.status, 404);
  assert.equal(Object.hasOwn(connectorHealth, "integrations"), false);
  await waitJson(`${baseUrl}/api/connectors/status?linkId=${linkId}`, (value) => value.connected === true);

  if (process.platform === "win32") {
    const pathIndependentShell = await createJob({
      kind: "command",
      input: {
        text: "Write-Output path-independent-shell-ok",
        runAs: "user",
        cwd: root,
        timeoutMs: 10_000
      }
    });
    const shellState = await waitJob(pathIndependentShell.id, 20_000);
    assert.equal(shellState.status, "succeeded");
    assert.match(shellState.result.text, /path-independent-shell-ok/u);
  }

  if (openCodePath) {
    await waitJson(`${baseUrl}/ready`, (value) => value.ok === true && value.agentModelProxy?.ready === true);
    await waitJson(`http://127.0.0.1:${connectorPort}/agent/status`, (value) => value.agent?.available === true, 20_000);
    await runAgentModelCase("cli_over_env_and_saved", glmProxyModel);
    const saved = JSON.parse(await readFile(path.join(connectorDataDir, "connector-config.json"), "utf8"));
    assert.equal(saved.gonkaModel, glmProxyModel);
    await restartConnector("");
    await runAgentModelCase("saved_model_after_restart", glmProxyModel);
    await restartConnector(defaultGonkaProxyModel);
    await runAgentModelCase("env_over_saved", defaultGonkaProxyModel);
    const defaultConfig = JSON.parse(await readFile(path.join(connectorDataDir, "connector-config.json"), "utf8"));
    delete defaultConfig.gonkaModel;
    await stopOwnedChild(connector);
    await writeFile(path.join(connectorDataDir, "connector-config.json"), JSON.stringify(defaultConfig), { mode: 0o600 });
    await restartConnector("");
    await runAgentModelCase("unchanged_empty_default", defaultGonkaProxyModel);
    if (process.platform === "win32") await verifyCompanionModelTransfer();
  }

  const completed = await createJob({
    kind: "script",
    input: {
      kind: "script",
      name: "integration.mjs",
      shell: "node",
      script: "process.stdout.write('connector-ok')",
      runAs: "user",
      cwd: root,
      timeoutMs: 10_000
    }
  });
  const completedState = await waitJob(completed.id, 20_000);
  assert.equal(completedState.status, "succeeded");
  assert.match(completedState.result.text, /connector-ok/u);
  assert.ok(completedState.events.some((event) => event.type === "stdout" && event.text.includes("connector-ok")));

  const access = await json(`${baseUrl}/api/connectors/access-grants`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Soty-Link-Id": linkId },
    body: JSON.stringify({ deviceId, controllerDeviceId, capabilities: ["status", "agent", "command", "script", "events", "cancel"], expiresInMs: 60_000 })
  });
  assert.equal(access.ok, true);
  assert.match(access.token, /^[A-Za-z0-9_-]{40,160}$/u);
  const accessHeaders = {
    "X-Soty-Link-Id": controllerLinkId,
    "X-Soty-Access-Grant-Id": access.grant.id,
    "X-Soty-Controller-Device-Id": controllerDeviceId,
    Authorization: `Bearer ${access.token}`
  };
  const delegatedStatus = await json(`${baseUrl}/api/connectors/status`, { headers: accessHeaders });
  assert.deepEqual(delegatedStatus.devices.map((item) => item.deviceId), [deviceId]);
  const delegatedCreated = await json(`${baseUrl}/api/connectors/jobs`, {
    method: "POST",
    headers: { ...accessHeaders, "Content-Type": "application/json" },
    body: JSON.stringify({
      linkId: controllerLinkId,
      deviceId,
      kind: "script",
      input: {
        kind: "script",
        name: "delegated.mjs",
        shell: "node",
        script: "process.stdout.write('delegated-ok')",
        runAs: "user",
        cwd: root,
        timeoutMs: 10_000
      }
    })
  });
  assert.equal(delegatedCreated.ok, true);
  const delegatedState = await waitDelegatedJob(delegatedCreated.job.id, accessHeaders, 20_000);
  assert.equal(delegatedState.job.status, "succeeded");
  assert.match(delegatedState.job.result.text, /delegated-ok/u);
  assert.ok(delegatedState.events.some((event) => event.type === "stdout" && event.text.includes("delegated-ok")));
  const revokedAccess = await json(`${baseUrl}/api/connectors/access-grants/${access.grant.id}/revoke`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Soty-Link-Id": linkId },
    body: "{}"
  });
  assert.equal(revokedAccess.ok, true);
  assert.equal((await json(`${baseUrl}/api/connectors/status`, { headers: accessHeaders })).error, "connector-access-revoked");

  const timed = await createJob({
    kind: "script",
    input: {
      kind: "script",
      name: "timeout.mjs",
      shell: "node",
      script: "setTimeout(() => process.stdout.write('too-late'), 30000)",
      runAs: "user",
      cwd: root,
      timeoutMs: 1_200
    }
  });
  const timedState = await waitJob(timed.id, 10_000);
  assert.equal(timedState.status, "failed");
  assert.equal(timedState.result.exitCode, 124);

  if (process.platform === "win32") {
    const childPidPath = path.join(root, "timeout-tree-child.pid");
    const treeTimed = await createJob({
      kind: "script",
      input: {
        kind: "script",
        name: "timeout-tree.ps1",
        shell: "powershell",
        script: `$child=Start-Process -FilePath powershell.exe -ArgumentList '-NoLogo','-NoProfile','-Command','while($true){Start-Sleep -Seconds 1}' -NoNewWindow -PassThru
[IO.File]::WriteAllText('${childPidPath.replace(/'/gu, "''")}',[string]$child.Id)
while($true){Start-Sleep -Seconds 1}`,
        runAs: "user",
        cwd: root,
        // Include cold PowerShell startup before asserting descendant cleanup.
        // The short generic timeout case above still checks the 1.2s deadline.
        timeoutMs: 8_000
      }
    });
    const treeTimedState = await waitJob(treeTimed.id, 25_000);
    assert.equal(treeTimedState.status, "failed");
    assert.equal(treeTimedState.result.exitCode, 124);
    const childPid = Number((await readFile(childPidPath, "utf8")).trim());
    assert.ok(childPid > 0, "timeout tree child pid is captured");
    await waitProcessExit(childPid, 5_000);
  }

  const cancellable = await createJob({
    kind: "script",
    input: {
      kind: "script",
      name: "cancel.mjs",
      shell: "node",
      script: "setTimeout(() => process.stdout.write('too-late'), 30000)",
      runAs: "user",
      cwd: root,
      timeoutMs: 60_000
    }
  });
  await waitJobStatus(cancellable.id, ["leased", "running"], 10_000);
  const cancelled = await json(`${baseUrl}/api/connectors/jobs/${cancellable.id}/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Soty-Link-Id": linkId },
    body: JSON.stringify({ linkId })
  });
  assert.equal(cancelled.ok, true);
  assert.equal((await waitJob(cancellable.id, 10_000)).status, "cancelled");

  process.stdout.write("connector:integration:selftest:ok\n");
} finally {
  const closed = await Promise.allSettled(processes.reverse().map(stopOwnedChild));
  if (gonkaServer) {
    gonkaServer.closeAllConnections();
    await new Promise((resolveClose, reject) => gonkaServer.close(error => error ? reject(error) : resolveClose()));
  }
  const allChildrenClosed = closed.every(value => value.status === "fulfilled")
    && processes.every(child => child.exitCode !== null || child.signalCode !== null);
  const portsFree = await Promise.all([serverPort, connectorPort, ...(gonkaPort ? [gonkaPort] : [])].map(portIsFree));
  if (evidenceDir) {
    assert.ok(path.isAbsolute(evidenceDir)); await mkdir(evidenceDir, { recursive: true });
    await writeFile(path.join(evidenceDir, "MODEL-REQUESTS-AND-CLOSURE.json"), JSON.stringify({
      realOpenCode: Boolean(openCodePath), controlledLocalUpstream: Boolean(openCodePath), externalProvider: false,
      modelCases, requests: gonkaRequests.map(request => ({
        route: request.url === "/v1/chat/completions" ? "chat_completions" : "other",
        model: [defaultGonkaProxyModel, glmProxyModel, "MiniMaxAI/MiniMax-M2.7"].includes(request.body?.model) ? request.body.model : "unknown",
        maxTokens: Number.isSafeInteger(request.body?.max_tokens) ? request.body.max_tokens : null,
        maxCompletionTokens: Number.isSafeInteger(request.body?.max_completion_tokens) ? request.body.max_completion_tokens : null,
        syntheticAuthorizationMatches: request.authorization === "Bearer integration-server-gonka-key"
      })), allChildrenClosed, ownedChildCount: processes.length, allOwnedPortsFree: portsFree.every(Boolean),
      privateValuesPrinted: false, machineCompanionInstalled: false
    }, null, 2) + "\n", { flag: "wx" });
  }
  assert.equal(allChildrenClosed, true); assert.equal(portsFree.every(Boolean), true);
  assert.ok(path.dirname(root) === path.resolve(tmpdir()) && path.basename(root).startsWith("soty-connector-integration-"));
  await rm(root, { recursive: true, force: true });
}

async function restartConnector(modelEnv) {
  await stopOwnedChild(connector);
  connector = start(process.execPath, [connectorRuntime], { ...connectorEnv, SOTY_CONNECTOR_MODEL: modelEnv });
  processes.push(connector);
  await waitJson(`http://127.0.0.1:${connectorPort}/agent/status`, value => value.agent?.available === true, 20_000);
  await waitJson(`${baseUrl}/api/connectors/status?linkId=${linkId}`, value => value.connected === true);
}

async function runAgentModelCase(name, expectedModel) {
  const before = gonkaRequests.length;
  const job = await createJob({ kind: "agent", input: {
    text: "Reply with exactly: gonka-opencode-ok", cwd: root, timeoutMs: 60_000,
    // An inbound job DTO must not select the host's model.
    model: "MiniMaxAI/MiniMax-M2.7"
  } });
  const state = await waitJob(job.id, 60_000);
  assert.equal(state.status, "succeeded"); assert.equal(state.result.text.trim(), "gonka-opencode-ok");
  assert.match(state.result.sessionId, /^ses_/u);
  assert.ok(state.events.some(event => event.type === "message" && event.text.includes("gonka-opencode-ok")));
  const requests = gonkaRequests.slice(before).filter(request => request.url === "/v1/chat/completions");
  modelCases.push({ name, expectedModel, actualModels: [...new Set(requests.map(request => request.body?.model))],
    requestCount: requests.length, jobSucceeded: true,
    jobModelIgnored: requests.length > 0 && requests.every(request => request.body?.model !== "MiniMaxAI/MiniMax-M2.7") });
  assert.ok(requests.length > 0, "real OpenCode must reach the controlled upstream");
  assert.ok(requests.every(request => request.authorization === "Bearer integration-server-gonka-key"));
  assert.deepEqual([...new Set(requests.map(request => request.body?.model))], [expectedModel]);
  for (const request of requests) for (const field of ["max_tokens", "max_completion_tokens"])
    if (request.body[field] !== undefined) assert.ok(Number.isSafeInteger(request.body[field]) && request.body[field] > 0 && request.body[field] <= 8192);
}

async function verifyCompanionModelTransfer() {
  const source = await readFile(connectorRuntime, "utf8");
  const block = source.match(/"    foreach \(\$name in @\([^\n]+\)\) \{",[\s\S]*?"    \}",/u)?.[0];
  assert.ok(block);
  // Execute ONLY the literal existing config-copy loop, against memory-only
  // synthetic rows. No registry/task/SYSTEM/user installation is executed.
  const script = block.split(/\r?\n/u).map(line => JSON.parse(line.trim().replace(/,$/u, ""))).join("\n");
  const prefix = `$sourceData=[pscustomobject]@{schema='synthetic';gonkaModel='${glmProxyModel}';privateExtra='never-copy'};`;
  const body = prefix + "$userData=[pscustomobject]@{gonkaModel='old';userOnly='retained'};" + script
    + ";[pscustomobject]@{model=$userData.gonkaModel;kept=$userData.userOnly -eq 'retained';extraCopied=$null -ne $userData.PSObject.Properties['privateExtra']}|ConvertTo-Json -Compress";
  const executable = path.join(process.env.SystemRoot || "C:\\Windows", "System32", "WindowsPowerShell", "v1.0", "powershell.exe");
  const result = JSON.parse(execFileSync(executable, ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", Buffer.from(body, "utf16le").toString("base64")],
    { encoding: "utf8", timeout: 5000, windowsHide: true }));
  assert.equal(result.model, glmProxyModel); assert.equal(result.kept, true); assert.equal(result.extraCopied, false);
  modelCases.push({ name: "actual_companion_copy_loop", modelCopied: true, userFieldsPreserved: true, nonWhitelistRefused: true, memoryOnly: true });
}

async function startFakeGonka(port) {
  const requests = [];
  const server = createServer(async (request, response) => {
    const body = await readRequestBody(request);
    requests.push({
      url: request.url,
      authorization: String(request.headers.authorization || ""),
      body
    });
    if (request.method !== "POST" || request.url !== "/v1/chat/completions") {
      response.writeHead(404, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ error: { message: "not found" } }));
      return;
    }
    const id = "chatcmpl-soty-integration";
    const model = typeof body?.model === "string" ? body.model : defaultGonkaProxyModel;
    if (body?.stream === false) {
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ id, object: "chat.completion", created: 1, model, choices: [{ index: 0, message: { role: "assistant", content: "gonka-opencode-ok" }, finish_reason: "stop" }] }));
      return;
    }
    response.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
    response.write(`data: ${JSON.stringify({ id, object: "chat.completion.chunk", created: 1, model, choices: [{ index: 0, delta: { role: "assistant", content: "gonka-opencode-ok" }, finish_reason: null }] })}\n\n`);
    response.write(`data: ${JSON.stringify({ id, object: "chat.completion.chunk", created: 1, model, choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`);
    response.end("data: [DONE]\n\n");
  });
  await new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolveListen);
  });
  return { server, requests };
}

function readRequestBody(request) {
  return new Promise((resolveBody, reject) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      try { resolveBody(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}")); }
      catch (error) { reject(error); }
    });
    request.on("error", reject);
  });
}

async function createJob(body) {
  const result = await json(`${baseUrl}/api/connectors/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Soty-Link-Id": linkId },
    body: JSON.stringify({ linkId, deviceId, ...body })
  });
  assert.equal(result.ok, true);
  return result.job;
}

async function waitJob(id, timeoutMs) {
  return await waitJobStatus(id, ["succeeded", "failed", "cancelled"], timeoutMs);
}

async function waitJobStatus(id, statuses, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await json(`${baseUrl}/api/connectors/jobs/${id}`, { headers: { "X-Soty-Link-Id": linkId } });
    if (result.ok && statuses.includes(result.job.status)) return result.job;
    await new Promise((resolveWait) => setTimeout(resolveWait, 150));
  }
  throw new Error(`Job ${id} did not reach ${statuses.join(",")}`);
}

async function waitDelegatedJob(id, headers, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let after = 0;
  const events = [];
  while (Date.now() < deadline) {
    const result = await json(`${baseUrl}/api/connectors/jobs/${id}/events?after=${after}`, { headers });
    assert.equal(result.ok, true);
    events.push(...result.events);
    after = result.cursor;
    if (result.done) return { job: result.job, events };
    await new Promise((resolveWait) => setTimeout(resolveWait, 150));
  }
  throw new Error(`Delegated job ${id} did not finish`);
}

function start(command, args, extraEnv) {
  const child = spawn(command, args, {
    cwd: process.cwd(),
    env: { ...process.env, ...extraEnv, NODE_OPTIONS: "" },
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"]
  });
  let stderr = "";
  childClosures.set(child, new Promise(resolveClose => child.once("close", resolveClose)));
  child.stderr.on("data", (chunk) => { stderr = `${stderr}${chunk}`.slice(-8_000); });
  child.on("exit", (code) => {
    if (code && !process.exitCode) process.stderr.write(`child:${code}:${stderr}\n`);
  });
  return child;
}

async function stopOwnedChild(child) {
  const closed = childClosures.get(child); assert.ok(closed);
  if (child.exitCode === null && child.signalCode === null) child.kill();
  let timer;
  try { await Promise.race([closed, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("owned_child_close_timeout")), 5000); })]); }
  finally { clearTimeout(timer); }
}

function portIsFree(port) {
  return new Promise(resolveFree => {
    const server = createServer(); server.once("error", () => resolveFree(false));
    server.listen(port, "127.0.0.1", () => server.close(error => resolveFree(!error)));
  });
}

async function waitJson(url, predicate, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const value = await json(url);
      if (predicate(value)) return value;
    } catch { /* Service is still starting. */ }
    await new Promise((resolveWait) => setTimeout(resolveWait, 100));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

async function waitProcessExit(pid, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try { process.kill(pid, 0); } catch { return; }
    await new Promise((resolveWait) => setTimeout(resolveWait, 100));
  }
  throw new Error(`Process tree child remained alive: ${pid}`);
}

async function json(url, options) {
  const response = await fetch(url, { cache: "no-store", ...options });
  return await response.json();
}

function freePort() {
  return new Promise((resolvePort, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close((error) => error ? reject(error) : resolvePort(port));
    });
  });
}
