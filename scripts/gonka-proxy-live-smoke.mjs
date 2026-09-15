#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { defaultGonkaProxyModel } from "../server/gonka-proxy.js";

if (!String(process.env.SOTY_GONKA_API_KEY || "").trim()) throw new Error("SOTY_GONKA_API_KEY is required");

const root = await mkdtemp(join(tmpdir(), "soty-gonka-live-"));
const port = await freePort();
const baseUrl = `http://127.0.0.1:${port}`;
const token = "s".repeat(48);
const app = spawn(process.execPath, ["server/index.js"], {
  cwd: process.cwd(),
  windowsHide: true,
  stdio: ["ignore", "pipe", "pipe"],
  env: { ...process.env, NODE_OPTIONS: "", PORT: String(port), DATA_DIR: join(root, "data") }
});
let stderr = "";
app.stderr.on("data", (chunk) => { stderr = `${stderr}${chunk}`.slice(-8_000); });

try {
  await waitReady(`${baseUrl}/ready`);
  const registration = await fetch(`${baseUrl}/api/connectors/register`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      linkId: "g".repeat(43),
      deviceId: "live-smoke-device",
      connectorId: "live-smoke:currentuser",
      version: "1.2.1",
      platform: `${process.platform}-${process.arch}`,
      scope: "CurrentUser",
      capabilities: ["agent"],
      agent: { id: "opencode", provider: "gonka", model: defaultGonkaProxyModel, available: true }
    })
  });
  assert.equal(registration.status, 200);

  const startedAt = Date.now();
  const response = await fetch(`${baseUrl}/api/connectors/gonka/v1/chat/completions`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      model: defaultGonkaProxyModel,
      stream: false,
      max_tokens: 32,
      messages: [{ role: "user", content: "Reply with exactly PONG" }]
    }),
    signal: AbortSignal.timeout(180_000)
  });
  const body = await response.json();
  const content = String(body?.choices?.[0]?.message?.content || "").trim();
  assert.equal(response.status, 200, JSON.stringify(body).slice(0, 1_000));
  assert.match(content, /PONG/iu);
  process.stdout.write(`gonka-proxy:live:ok model=${defaultGonkaProxyModel} latencyMs=${Date.now() - startedAt} reply=${JSON.stringify(content.slice(0, 80))}\n`);
} finally {
  app.kill();
  await new Promise((resolveWait) => setTimeout(resolveWait, 200));
  await rm(root, { recursive: true, force: true });
  if (app.exitCode && app.exitCode !== 0) process.stderr.write(stderr);
}

async function waitReady(url) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url, { cache: "no-store" });
      if (response.ok && (await response.json()).ok === true) return;
    } catch { /* Server is still starting. */ }
    await new Promise((resolveWait) => setTimeout(resolveWait, 100));
  }
  throw new Error(`Timed out waiting for proxy readiness: ${stderr}`);
}

function freePort() {
  return new Promise((resolvePort, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const value = typeof address === "object" && address ? address.port : 0;
      server.close((error) => error ? reject(error) : resolvePort(value));
    });
  });
}
