#!/usr/bin/env node
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const releaseRuntime = await readFile(join(root, "public", "agent", "soty-connector.mjs"));
const releaseManifest = JSON.parse(await readFile(join(root, "public", "agent", "manifest.json"), "utf8"));
const releaseText = releaseRuntime.toString("utf8");
const previousVersion = previousPatch(releaseManifest.version);
const versionLine = `const connectorVersion = "${releaseManifest.version}";`;
assert.equal(releaseText.split(versionLine).length - 1, 1);

const temp = await mkdtemp(join(tmpdir(), "soty-update-selftest-"));
const candidatePath = join(temp, "soty-connector.mjs");
const previousBytes = Buffer.from(releaseText.replace(versionLine, `const connectorVersion = "${previousVersion}";`));
await writeFile(candidatePath, previousBytes, { mode: 0o755 });

const port = await freePort();
const connectorPort = await freePort();
const baseUrl = `http://127.0.0.1:${port}`;
const manifest = {
  ...releaseManifest,
  connectorUrl: "/soty-connector.mjs",
  agentUrl: "/soty-connector.mjs"
};
const server = createServer((request, response) => {
  if (request.url === "/manifest.json") {
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify(manifest));
    return;
  }
  if (request.url === "/soty-connector.mjs") {
    response.writeHead(200, { "Content-Type": "text/javascript" });
    response.end(releaseRuntime);
    return;
  }
  response.writeHead(404).end();
});
await new Promise((resolveListen, reject) => {
  server.once("error", reject);
  server.listen(port, "127.0.0.1", resolveListen);
});

let connector;
try {
  const update = await runProcess(process.execPath, [candidatePath, "ctl", "update"], {
    cwd: temp,
    timeoutMs: 30_000,
    env: updateEnv()
  });
  const updateExit = update.code;
  const updateOutput = `${update.stdout}${update.stderr}`;
  assert.equal(updateExit, 75, `a verified update must request supervisor restart: ${updateOutput.trim()}`);
  assert.equal(sha256(await readFile(candidatePath)), releaseManifest.sha256);
  assert.equal(sha256(await readFile(`${candidatePath}.previous`)), sha256(previousBytes));
  const pending = JSON.parse(await readFile(`${candidatePath}.update-pending.json`, "utf8"));
  assert.equal(pending.fromVersion, previousVersion);
  assert.equal(pending.toVersion, releaseManifest.version);
  assert.equal(pending.sha256, releaseManifest.sha256);

  connector = spawn(process.execPath, [candidatePath], {
    cwd: temp,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      ...updateEnv(),
      SOTY_CONNECTOR_MANAGED: "1",
      SOTY_CONNECTOR_AUTO_UPDATE: "0",
      SOTY_CONNECTOR_PORT: String(connectorPort),
      SOTY_CONNECTOR_UPDATE_CONFIRM_MS: "1000"
    }
  });
  await waitJson(`http://127.0.0.1:${connectorPort}/health`, (value) => value.version === releaseManifest.version);
  await waitFor(async () => {
    try {
      const receipt = JSON.parse(await readFile(join(temp, "runtime-release.json"), "utf8"));
      return receipt.version === releaseManifest.version && receipt.sha256 === releaseManifest.sha256;
    } catch {
      return false;
    }
  }, 10_000);
  await assert.rejects(readFile(`${candidatePath}.previous`));
  await assert.rejects(readFile(`${candidatePath}.update-pending.json`));

  const runnerName = process.platform === "win32" ? "start-agent.ps1" : "start-agent.sh";
  const runner = await readFile(join(temp, runnerName), "utf8");
  assert.match(runner, /update-pending\.json/u);
  assert.match(runner, /previous/u);
  if (process.platform === "win32") {
    execFileSync("powershell.exe", ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", `[void][ScriptBlock]::Create((Get-Content -LiteralPath '${join(temp, runnerName).replace(/'/gu, "''")}' -Raw))`], { timeout: 10_000, windowsHide: true });
  }

  process.stdout.write(`agent-update-selftest:ok:${previousVersion}->${releaseManifest.version}\n`);
} finally {
  connector?.kill();
  await new Promise((resolveClose) => server.close(resolveClose));
  await new Promise((resolveWait) => setTimeout(resolveWait, 200));
  await rm(temp, { recursive: true, force: true });
}

function updateEnv() {
  return {
    ...process.env,
    NODE_OPTIONS: "",
    SOTY_CONNECTOR_DATA_DIR: temp,
    SOTY_CONNECTOR_AUTO_UPDATE: "0",
    SOTY_AGENT_AUTO_UPDATE: "0",
    SOTY_CONNECTOR_UPDATE_URL: `${baseUrl}/manifest.json`
  };
}

function previousPatch(version) {
  const parts = version.split(".").map(Number);
  assert.equal(parts.length, 3);
  assert.ok(parts[2] > 0, "release update self-test requires a non-zero patch version");
  return `${parts[0]}.${parts[1]}.${parts[2] - 1}`;
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function freePort() {
  return new Promise((resolvePort, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      const value = typeof address === "object" && address ? address.port : 0;
      probe.close((error) => error ? reject(error) : resolvePort(value));
    });
  });
}

function runProcess(command, args, { cwd, env, timeoutMs }) {
  return new Promise((resolveRun, reject) => {
    const child = spawn(command, args, {
      cwd,
      env,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    const timer = setTimeout(() => child.kill(), timeoutMs);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      resolveRun({ code: Number(code), stdout, stderr });
    });
  });
}

async function waitJson(url, predicate, timeoutMs = 15_000) {
  await waitFor(async () => {
    try {
      const response = await fetch(url, { cache: "no-store" });
      return response.ok && predicate(await response.json());
    } catch {
      return false;
    }
  }, timeoutMs);
}

async function waitFor(predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolveWait) => setTimeout(resolveWait, 100));
  }
  throw new Error("Timed out waiting for update state");
}
