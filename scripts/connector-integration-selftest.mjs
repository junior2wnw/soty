#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { defaultGonkaProxyModel } from "../server/gonka-proxy.js";

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

try {
  const gonkaPort = openCodePath ? await freePort() : 0;
  if (openCodePath) {
    ({ server: gonkaServer, requests: gonkaRequests } = await startFakeGonka(gonkaPort));
  }
  const server = start(process.execPath, ["server/index.js"], {
    PORT: String(serverPort),
    DATA_DIR: path.join(root, "server-data"),
    ...(openCodePath ? {
      SOTY_GONKA_API_KEY: "integration-server-gonka-key",
      SOTY_GONKA_BASE_URL: `http://127.0.0.1:${gonkaPort}/v1`
    } : {})
  });
  processes.push(server);
  await waitJson(`${baseUrl}/health`, (value) => value.ok === true);

  await mkdir(connectorDataDir, { recursive: true });
  await writeFile(path.join(connectorDataDir, "connector-config.json"), `${JSON.stringify({
    workspaceRoot: path.join(root, "removed-workspace"),
    allowedRoots: [path.join(root, "removed-workspace")]
  })}\n`, "utf8");

  const connector = start(process.execPath, [connectorRuntime], {
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
      SOTY_OPENCODE_PATH: openCodePath
    } : {})
  });
  processes.push(connector);
  const connectorHealth = await waitJson(`http://127.0.0.1:${connectorPort}/health`, (value) => value.ok === true && value.connector === true);
  assert.equal(connectorHealth.package?.wrapper?.id, "soty-connector");
  assert.equal(connectorHealth.package?.agent?.id, "opencode");
  assert.equal(connectorHealth.update?.enabled, false);
  assert.equal(connectorHealth.update?.lastResult, "disabled");
  const spreadExStatusUrl = `http://127.0.0.1:${connectorPort}/integrations/spreadex/v1/status`;
  const directSpreadExStatus = await fetch(spreadExStatusUrl);
  assert.equal(directSpreadExStatus.status, 200);
  assert.equal(directSpreadExStatus.headers.get("access-control-allow-origin"), null);
  assert.equal((await directSpreadExStatus.json()).component.available, false);
  const browserSpreadExStatus = await fetch(spreadExStatusUrl, { headers: { Origin: "https://miniapp.spreadex.me" } });
  assert.equal(browserSpreadExStatus.status, 200);
  assert.equal(browserSpreadExStatus.headers.get("access-control-allow-origin"), "https://miniapp.spreadex.me");
  const rejectedSpreadExOrigin = await fetch(spreadExStatusUrl, { headers: { Origin: "https://miniapp.spreadex.me.evil.test" } });
  assert.equal(rejectedSpreadExOrigin.status, 403);
  assert.equal(rejectedSpreadExOrigin.headers.get("access-control-allow-origin"), null);
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
    const agentJob = await createJob({
      kind: "agent",
      input: {
        text: "Reply with exactly: gonka-opencode-ok",
        cwd: root,
        timeoutMs: 60_000
      }
    });
    const agentState = await waitJob(agentJob.id, 60_000);
    assert.equal(agentState.status, "succeeded");
    assert.equal(agentState.result.text.trim(), "gonka-opencode-ok");
    assert.match(agentState.result.sessionId, /^ses_/u);
    assert.ok(agentState.events.some((event) => event.type === "message" && event.text.includes("gonka-opencode-ok")));
    assert.ok(gonkaRequests.some((request) => request.url === "/v1/chat/completions"
      && request.authorization === "Bearer integration-server-gonka-key"
      && request.body?.model === defaultGonkaProxyModel));
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
  for (const child of processes.reverse()) child.kill();
  if (gonkaServer) await new Promise((resolveClose) => gonkaServer.close(resolveClose));
  await new Promise((resolveWait) => setTimeout(resolveWait, 300));
  await rm(root, { recursive: true, force: true });
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
  child.stderr.on("data", (chunk) => { stderr = `${stderr}${chunk}`.slice(-8_000); });
  child.on("exit", (code) => {
    if (code && !process.exitCode) process.stderr.write(`child:${code}:${stderr}\n`);
  });
  return child;
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
