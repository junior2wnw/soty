#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { defaultGonkaProxyModel } from "../server/gonka-proxy.js";

const root = await mkdtemp(join(tmpdir(), "soty-gonka-proxy-"));
const appPort = await freePort();
const upstreamPort = await freePort();
const appBase = `http://127.0.0.1:${appPort}`;
const token = "t".repeat(48);
const applicationToken = "a".repeat(48);
const upstreamKey = "server-only-gonka-key-for-selftest";
const requests = [];
const upstream = createServer(async (request, response) => {
  const body = await readJson(request);
  requests.push({ url: request.url, authorization: request.headers.authorization, body });
  response.writeHead(200, { "Content-Type": "text/event-stream" });
  response.write(`data: ${JSON.stringify({ id: "chatcmpl-proxy", object: "chat.completion.chunk", created: 1, model: defaultGonkaProxyModel, choices: [{ index: 0, delta: { content: "proxy-ok" }, finish_reason: null }] })}\n\n`);
  response.end("data: [DONE]\n\n");
});
await listen(upstream, upstreamPort);

const app = spawn(process.execPath, ["server/index.js"], {
  cwd: process.cwd(),
  windowsHide: true,
  stdio: ["ignore", "pipe", "pipe"],
  env: {
    ...process.env,
    NODE_OPTIONS: "",
    PORT: String(appPort),
    DATA_DIR: join(root, "data"),
    SOTY_GONKA_API_KEY: upstreamKey,
    SOTY_GONKA_BASE_URL: `http://127.0.0.1:${upstreamPort}/v1`,
    SOTY_GONKA_APPLICATION_TOKENS: JSON.stringify({ peremetrika: applicationToken })
  }
});
let stderr = "";
app.stderr.on("data", (chunk) => { stderr = `${stderr}${chunk}`.slice(-8_000); });

try {
  const readiness = await waitJson(`${appBase}/ready`, (value) => value.ok === true);
  assert.equal(readiness.agentModelProxy.model, defaultGonkaProxyModel);
  assert.equal(readiness.agentModelProxy.transport, "authenticated-server-proxy");
  assert.equal(readiness.applicationModelProxy.ready, true);
  assert.equal(readiness.applicationModelProxy.path, "/api/inference/v1/chat/completions");

  const registration = await requestJson(`${appBase}/api/connectors/register`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      linkId: "l".repeat(43),
      deviceId: "proxy-device",
      connectorId: "proxy-install:currentuser",
      deviceNick: "Proxy self-test",
      version: "1.2.1",
      platform: "test-x64",
      scope: "CurrentUser",
      capabilities: ["agent"],
      agent: { id: "opencode", provider: "gonka", model: defaultGonkaProxyModel, available: true }
    })
  });
  assert.equal(registration.response.status, 200);
  assert.equal(registration.body.ok, true);

  const proxied = await fetch(`${appBase}/api/connectors/gonka/v1/chat/completions`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ model: defaultGonkaProxyModel, stream: true, messages: [{ role: "user", content: "ping" }] })
  });
  assert.equal(proxied.status, 200);
  assert.equal(proxied.headers.get("x-soty-model-proxy"), "gonka");
  assert.match(await proxied.text(), /proxy-ok/u);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "/v1/chat/completions");
  assert.equal(requests[0].authorization, `Bearer ${upstreamKey}`);
  assert.notEqual(requests[0].authorization, `Bearer ${token}`);
  assert.equal(requests[0].body.model, defaultGonkaProxyModel);

  const applicationProxied = await fetch(`${appBase}/api/inference/v1/chat/completions`, {
    method: "POST",
    headers: { Authorization: `Bearer ${applicationToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ model: defaultGonkaProxyModel, stream: true, messages: [{ role: "user", content: "app-ping" }] })
  });
  assert.equal(applicationProxied.status, 200);
  assert.match(await applicationProxied.text(), /proxy-ok/u);
  assert.equal(requests.length, 2);
  assert.equal(requests[1].authorization, `Bearer ${upstreamKey}`);
  assert.equal(requests[1].body.messages[0].content, "app-ping");

  const rejectedConnectorOnApplicationApi = await requestJson(`${appBase}/api/inference/v1/chat/completions`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ model: defaultGonkaProxyModel, messages: [{ role: "user", content: "ping" }] })
  });
  assert.equal(rejectedConnectorOnApplicationApi.response.status, 401);

  const rejectedAuth = await requestJson(`${appBase}/api/connectors/gonka/v1/chat/completions`, {
    method: "POST",
    headers: { Authorization: `Bearer ${"x".repeat(48)}`, "Content-Type": "application/json" },
    body: JSON.stringify({ model: defaultGonkaProxyModel, messages: [{ role: "user", content: "ping" }] })
  });
  assert.equal(rejectedAuth.response.status, 401);

  const rejectedModel = await requestJson(`${appBase}/api/connectors/gonka/v1/chat/completions`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ model: "moonshotai/Kimi-K2.6", messages: [{ role: "user", content: "ping" }] })
  });
  assert.equal(rejectedModel.response.status, 400);
  assert.equal(requests.length, 2, "rejected requests must never reach Gonka");

  process.stdout.write("gonka-proxy:selftest:ok\n");
} finally {
  app.kill();
  await new Promise((resolveClose) => upstream.close(resolveClose));
  await new Promise((resolveWait) => setTimeout(resolveWait, 200));
  await rm(root, { recursive: true, force: true });
  if (app.exitCode && app.exitCode !== 0) process.stderr.write(stderr);
}

async function requestJson(url, options) {
  const response = await fetch(url, options);
  return { response, body: await response.json() };
}

async function waitJson(url, predicate, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url, { cache: "no-store" });
      const value = await response.json();
      if (predicate(value)) return value;
    } catch { /* Server is still starting. */ }
    await new Promise((resolveWait) => setTimeout(resolveWait, 100));
  }
  throw new Error(`Timed out waiting for ${url}: ${stderr}`);
}

function readJson(request) {
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

function listen(server, port) {
  return new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolveListen);
  });
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
