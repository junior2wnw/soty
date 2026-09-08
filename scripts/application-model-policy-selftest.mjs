#!/usr/bin/env node
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import express from "express";
import { attachConnectorApi } from "../server/connector-api.js";
import { createApplicationModelPolicy, applicationModelPolicySchema, candidateApplicationModel } from "../server/application-model-policy.js";
import { createApplicationTokenAuthenticator, defaultGonkaProxyModel } from "../server/gonka-proxy.js";

const fixture = await mkdtemp(path.join(tmpdir(), "soty-app-policy-"));
const candidate = candidateApplicationModel;
const deepseek = defaultGonkaProxyModel;
const tokens = { kvartalufa: "a".repeat(48), otherapp: "b".repeat(48) };
const connectorToken = "c".repeat(48);
const servers = [];
const requests = [];
const held = [];
const checks = [];
const policyPath = path.join(fixture, "application-model-policy.json");
const policy = { schema: applicationModelPolicySchema, applications: [{ id: "kvartalufa", allowedModels: [deepseek, candidate] }] };
await writeFile(policyPath, JSON.stringify(policy));
const tokenPath = path.join(fixture, "tokens.json");
const tokenBytes = JSON.stringify({ applications: Object.entries(tokens).map(([id, token]) => ({ id, token })) });
await writeFile(tokenPath, tokenBytes);
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function listen(server) {
  servers.push(server);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return `http://127.0.0.1:${server.address().port}`;
}
function reply(response, body) {
  response.writeHead(200, { "content-type": "text/event-stream" });
  response.end(`data: ${JSON.stringify({ model: body.model, choices: [{ delta: { content: "synthetic-ok", ...(body.tools ? { tool_calls: [{ id: "synthetic-tool", type: "function", function: { name: "get_status", arguments: "{}" } }] } : {}) }, finish_reason: "stop" }] })}\n\ndata: [DONE]\n\n`);
}
const upstream = await listen(createServer(async (request, response) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const body = JSON.parse(Buffer.concat(chunks));
  requests.push({ body, upstreamAuthorizationCorrect: request.headers.authorization === "Bearer synthetic-upstream-key-0001" });
  if (body.messages[0].content === "hold") held.push({ response, body });
  else reply(response, body);
}));
async function app(filePath) {
  const server = express();
  const attached = attachConnectorApi(server, {
    dataDir: path.join(fixture, `store-${servers.length}`),
    gonka: { baseUrl: upstream + "/v1", apiKey: "synthetic-upstream-key-0001", applicationTokens: "", applicationTokensFile: tokenPath, applicationModelPolicyFile: filePath }
  });
  const origin = await listen(createServer(server));
  const registration = await fetch(origin + "/api/connectors/register", {
    method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${connectorToken}` },
    body: JSON.stringify({ linkId: "l".repeat(43), deviceId: "policy-device", connectorId: "policy-install:currentuser", scope: "CurrentUser", platform: "synthetic", version: "1.2.12", capabilities: ["agent"], agent: { id: "opencode", provider: "gonka", model: deepseek, available: true } })
  });
  assert.equal(registration.status, 200);
  return { origin, attached };
}
async function call(application, model, { token = tokens.kvartalufa, connector = false, extra = {}, prompt = "ping" } = {}) {
  const response = await fetch(application.origin + (connector ? "/api/connectors/gonka/v1/chat/completions" : "/api/inference/v1/chat/completions"), {
    method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify({ model, stream: true, messages: [{ role: "user", content: prompt }], ...extra }), signal: AbortSignal.timeout(15000)
  });
  return { status: response.status, text: await response.text() };
}
try {
  const plain = await app("");
  assert.equal((await call(plain, deepseek)).status, 200);
  assert.equal((await call(plain, candidate)).status, 400);
  assert.equal((await call(plain, candidate, { connector: true, token: connectorToken })).status, 400);
  checks.push("unconfigured application and connector retain DeepSeek-only default");

  const configured = await app(policyPath);
  assert.equal(configured.attached.applicationModelProxy.ready, true);
  const mini = await call(configured, candidate, { extra: { tools: [{ type: "function", function: { name: "get_status", parameters: { type: "object", properties: {} } } }], tool_choice: "auto" } });
  assert.equal(mini.status, 200);
  assert.match(mini.text, /MiniMaxAI\/MiniMax-M2\.7/);
  assert.match(mini.text, /get_status/);
  assert.match(mini.text, /"finish_reason":"stop"/);
  assert.match(mini.text, /\[DONE\]/);
  assert.equal((await call(configured, deepseek)).status, 200);
  assert.equal((await call(configured, candidate, { token: tokens.otherapp })).status, 400);
  assert.equal((await call(configured, deepseek, { token: tokens.otherapp })).status, 200);
  assert.equal((await call(configured, candidate, { token: "w".repeat(48) })).status, 401);
  assert.equal((await call(configured, candidate, { token: connectorToken })).status, 401);
  assert.equal((await call(configured, candidate, { connector: true, token: tokens.kvartalufa })).status, 401);
  assert.equal((await call(configured, candidate, { connector: true, token: connectorToken })).status, 400);
  assert.equal((await call(configured, deepseek, { connector: true, token: connectorToken })).status, 200);
  assert.equal((await call(configured, "unapproved/model")).status, 400);
  assert.ok(requests.every((entry) => entry.upstreamAuthorizationCorrect));
  checks.push("exact authenticated application gets both native models; wrong app/token/route/model cannot inherit rule; SSE tools/terminal intact");

  const beforeInvalid = requests.length;
  for (const extra of [{ max_tokens: 8193 }, { max_completion_tokens: 8193 }, { n: 2 }, { stream: "true" }]) {
    assert.equal((await call(configured, candidate, { extra })).status, 400);
  }
  assert.equal(requests.length, beforeInvalid);
  checks.push("existing token/output-count/stream request limits reject before upstream");

  const one = call(configured, candidate, { prompt: "hold" });
  const two = call(configured, deepseek, { prompt: "hold" });
  for (let i = 0; held.length < 2 && i < 100; i++) await pause(10);
  assert.equal(held.length, 2);
  assert.equal((await call(configured, candidate)).status, 429);
  assert.equal((await call(configured, deepseek, { token: tokens.otherapp })).status, 200);
  for (const entry of held.splice(0)) reply(entry.response, entry.body);
  assert.equal((await one).status, 200);
  assert.equal((await two).status, 200);
  assert.equal((await call(configured, candidate)).status, 200);
  checks.push("two-request concurrency limit shared across models of same app, isolated from other app, released after completion");

  const invalidFiles = [
    ["malformed", "{"],
    ["unknown-key", JSON.stringify({ ...policy, token: "forbidden" })],
    ["unknown-model", JSON.stringify({ ...policy, applications: [{ id: "kvartalufa", allowedModels: ["arbitrary/model"] }] })],
    ["duplicate-id", JSON.stringify({ ...policy, applications: [policy.applications[0], policy.applications[0]] })],
    ["duplicate-model", JSON.stringify({ ...policy, applications: [{ id: "kvartalufa", allowedModels: [candidate, candidate] }] })],
    ["nonstring-id", JSON.stringify({ ...policy, applications: [{ id: ["kvartalufa"], allowedModels: [candidate] }] })],
    ["empty-rule", JSON.stringify({ ...policy, applications: [{ id: "kvartalufa", allowedModels: [] }] })],
    ["oversized", " ".repeat(65537)]
  ];
  for (const [name, bytes] of invalidFiles) {
    const file = path.join(fixture, name + ".json");
    await writeFile(file, bytes);
    assert.equal(createApplicationModelPolicy({ filePath: file, defaultModel: deepseek }).ready, false, name);
  }
  const missingPath = path.join(fixture, "missing.json");
  const directoryPath = path.join(fixture, "unreadable-as-file");
  await mkdir(directoryPath);
  for (const file of [missingPath, directoryPath, path.join(fixture, "malformed.json")]) {
    const broken = await app(file);
    assert.equal(broken.attached.applicationModelProxy.ready, false);
    assert.equal(broken.attached.modelProxy.ready, true);
    assert.equal((await call(broken, deepseek)).status, 503);
    assert.equal((await call(broken, candidate)).status, 503);
    assert.equal((await call(broken, deepseek, { connector: true, token: connectorToken })).status, 200);
  }
  checks.push("invalid schema/rules/oversize and missing/unreadable files fail closed for applications; connector DeepSeek remains available");

  assert.equal(await readFile(tokenPath, "utf8"), tokenBytes);
  const unchangedAuthenticator = createApplicationTokenAuthenticator("", { filePath: tokenPath });
  assert.equal(await unchangedAuthenticator(tokens.kvartalufa), "kvartalufa");
  assert.equal((await call(plain, deepseek)).status, 200);
  assert.equal((await call(plain, candidate)).status, 400);
  checks.push("token-file bytes/schema unchanged; removing optional policy restores original model contract");
  console.log(JSON.stringify({ ok: true, fixture, checks, upstream: "synthetic loopback only", nativeReadinessProven: false }, null, 2));
} finally {
  for (const entry of held) entry.response.destroy();
  for (const server of servers) { server.closeAllConnections(); await new Promise((resolve) => server.close(resolve)); }
}
