#!/usr/bin/env node
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const sourceAgentPath = join(root, "scripts", "soty-agent.mjs");
const tempRoot = await mkdtemp(join(tmpdir(), "soty-action-selftest-"));
const agentPath = join(tempRoot, "soty-agent.mjs");
const actionJobsDir = join(tempRoot, "action-jobs");
const mock = createMockRelay();
let agent = null;
let port = 0;
let scenariosRun = 0;

try {
  await copyFile(sourceAgentPath, agentPath);
  await listen(mock.server, "127.0.0.1", 0);
  const relayUrl = `http://127.0.0.1:${mock.server.address().port}`;
  port = await freePort();
  agent = await startAgent({ port, relayUrl });
  await runScenarios({ relayUrl });
  process.stdout.write(`action-kernel-selftest: ok scenarios=${scenariosRun}\n`);
} finally {
  await stopAgent();
  await closeServer(mock.server);
  if (process.env.SOTY_KEEP_SELFTEST !== "1") {
    await rm(tempRoot, { recursive: true, force: true }).catch(() => undefined);
  } else {
    process.stdout.write(`action-kernel-selftest: kept ${tempRoot}\n`);
  }
}

async function runScenarios() {
  const cases = [
    ["health reports new version", async () => {
      const health = await get("/health");
      assertEqual(health.status, 200);
      assertEqual(health.body.version, "0.3.112");
    }],
    ["actions list starts empty", async () => {
      const list = await get("/operator/actions");
      assertEqual(list.status, 200);
      assert(Array.isArray(list.body.jobs));
      assertEqual(list.body.jobs.length, 0);
    }],
    ["invalid json is rejected", async () => {
      const response = await raw("POST", "/operator/action", "{");
      assertEqual(response.status, 400);
    }],
    ["missing target is rejected", async () => {
      const response = await action({ command: "whoami" });
      assertEqual(response.status, 400);
      assertEqual(response.body.text, "! target");
    }],
    ["missing command is rejected", async () => {
      const response = await action({ target: "agent-source:dev1" });
      assertEqual(response.status, 400);
      assertEqual(response.body.text, "! command");
    }],
    ["missing script is rejected", async () => {
      const response = await action({ mode: "script", target: "agent-source:dev1", script: "" });
      assertEqual(response.status, 400);
      assertEqual(response.body.text, "! script");
    }],
    ["source run succeeds", async () => expectStatus(await action(sourceRun("SELFTEST_OK run")), "ok")],
    ["source script succeeds", async () => expectStatus(await action(sourceScript("SELFTEST_OK script")), "ok")],
    ["source run failure is captured", async () => {
      const response = await action(sourceRun("SELFTEST_FAIL"));
      expectStatus(response, "failed");
      assertEqual(response.body.exitCode, 7);
    }],
    ["source timeout is captured", async () => expectStatus(await action(sourceRun("SELFTEST_TIMEOUT")), "timeout")],
    ["bad relay json is captured", async () => expectStatus(await action(sourceRun("SELFTEST_BAD_JSON")), "failed")],
    ["relay http error is captured", async () => {
      const response = await action(sourceRun("SELFTEST_HTTP_500"));
      expectStatus(response, "failed");
      assertEqual(response.body.exitCode, 500);
    }],
    ["ordinary target without bridge fails narrowly", async () => {
      const response = await action({ target: "room-a", command: "whoami SELFTEST_OK" });
      expectStatus(response, "failed");
      assertEqual(response.body.exitCode, 409);
    }],
    ["source device mismatch is not rerouted", async () => {
      const response = await action({ target: "agent-source:dev1", sourceDeviceId: "dev2", command: "SELFTEST_OK" });
      expectStatus(response, "failed");
      assertEqual(response.body.exitCode, 403);
    }],
    ["manual windows reset handoff is blocked", async () => {
      const response = await action(sourceRun("systemreset -factoryreset SELFTEST_OK"));
      assertEqual(response.status, 422);
      expectStatus(response, "blocked");
    }],
    ["identity command is classified", async () => expectFamily(await action(sourceRun("whoami SELFTEST_OK")), "identity-probe")],
    ["audio volume command is classified", async () => expectFamily(await action(sourceRun("set volume SELFTEST_OK")), "audio-volume")],
    ["audio mute command is classified", async () => expectFamily(await action(sourceRun("mute audio SELFTEST_OK")), "audio-mute")],
    ["driver command is classified", async () => expectFamily(await action(sourceRun("pnputil /enum-drivers SELFTEST_OK")), "driver-check")],
    ["power command is classified", async () => expectFamily(await action(sourceRun("powercfg /batteryreport SELFTEST_OK")), "power-check")],
    ["package command is classified", async () => expectFamily(await action(sourceRun("winget install app SELFTEST_OK")), "package-install")],
    ["service command is classified", async () => expectFamily(await action(sourceRun("Get-Service SELFTEST_OK")), "service-check")],
    ["generic command stays generic", async () => expectFamily(await action(sourceRun("echo SELFTEST_OK")), "generic")],
    ["low risk is inferred", async () => expectRisk(await action(sourceRun("whoami SELFTEST_OK")), "low")],
    ["medium risk is inferred", async () => expectRisk(await action(sourceRun("winget install app SELFTEST_OK")), "medium")],
    ["high disk risk is inferred", async () => expectRisk(await action(sourceRun("diskpart SELFTEST_OK")), "high")],
    ["windows reinstall risk is high", async () => expectRisk(await action({ ...sourceRun("reinstall windows SELFTEST_OK"), family: "windows-reinstall" }), "high")],
    ["explicit family wins", async () => expectFamily(await action({ ...sourceRun("echo SELFTEST_OK"), family: "custom-family" }), "custom-family")],
    ["explicit kind is stored", async () => {
      const response = await action({ ...sourceRun("echo SELFTEST_OK"), kind: "browser.automation" });
      const status = await get(response.body.statusPath);
      assertEqual(status.body.job.kind, "browser.automation");
    }],
    ["intent and name are stored", async () => {
      const response = await action({ ...sourceScript("echo SELFTEST_OK"), name: "repair-step", intent: "repair proof" });
      const status = await get(response.body.statusPath);
      assertEqual(status.body.job.intent, "repair proof");
      assertEqual(status.body.result.kind, "script");
    }],
    ["source relay is redacted in input artifact", async () => {
      const response = await action({ ...sourceRun("SELFTEST_OK relay"), sourceRelayId: "relay_secret_00000000000000000001" });
      const status = await get(response.body.statusPath);
      const input = await readFile(join(dirname(status.body.job.artifacts.jobPath), "input.json"), "utf8");
      assert(input.includes("\"sourceRelayId\": \"<set>\""));
      assert(!input.includes("relay_secret_00000000000000000001"));
    }],
    ["idempotency returns same job", async () => {
      const first = await action({ ...sourceRun("SELFTEST_OK idem"), idempotencyKey: "idem-one" });
      const second = await action({ ...sourceRun("SELFTEST_OK idem"), idempotencyKey: "idem-one" });
      assertEqual(first.body.jobId, second.body.jobId);
      assertEqual(mock.count("SELFTEST_OK idem"), 1);
    }],
    ["idempotency conflict is rejected", async () => {
      await action({ ...sourceRun("SELFTEST_OK idem-conflict-a"), idempotencyKey: "idem-conflict" });
      const second = await action({ ...sourceRun("SELFTEST_OK idem-conflict-b"), idempotencyKey: "idem-conflict" });
      assertEqual(second.status, 409);
    }],
    ["detached action returns durable id", async () => {
      const response = await action({ ...sourceRun("SELFTEST_DELAY SELFTEST_OK detached"), detached: true, idempotencyKey: "detached-one" });
      assertEqual(response.status, 202);
      assert(response.body.jobId);
      await waitForStatus(response.body.statusPath, "ok");
    }],
    ["actions list includes completed jobs", async () => {
      const list = await get("/operator/actions");
      assert(list.body.jobs.some((job) => job.status === "ok"));
    }],
    ["unknown action status returns 404", async () => {
      const response = await get("/operator/action/act_missing1234567890");
      assertEqual(response.status, 404);
    }],
    ["result artifact has stable schema", async () => {
      const response = await action(sourceRun("SELFTEST_OK artifact"));
      const result = JSON.parse(await readFile(response.body.resultPath, "utf8"));
      assertEqual(result.schema, "soty.action.result.v1");
      assertEqual(result.status, "ok");
    }],
    ["input artifact does not store raw command", async () => {
      const response = await action(sourceRun("SELFTEST_OK secret-token-123"));
      const status = await get(response.body.statusPath);
      const input = await readFile(join(dirname(status.body.job.artifacts.jobPath), "input.json"), "utf8");
      assert(!input.includes("secret-token-123"));
      assert(input.includes("commandSig"));
    }],
    ["stdout artifact stores raw output locally", async () => {
      const response = await action(sourceRun("SELFTEST_LARGE"));
      const status = await get(response.body.statusPath);
      const stdout = await readFile(status.body.job.artifacts.stdoutPath, "utf8");
      assert(stdout.length > 12_000);
    }],
    ["learning outbox records action-job", async () => {
      await action(sourceRun("SELFTEST_OK learning"));
      const outbox = await readFile(join(tempRoot, "learning-outbox.jsonl"), "utf8");
      assert(outbox.includes("\"kind\":\"action-job\"") || outbox.includes("\"kind\": \"action-job\""));
    }],
    ["cli action list works", async () => {
      const cli = await runCli(["action", "list"]);
      assertEqual(cli.code, 0);
      assert(cli.stdout.includes("agent-source:dev1") || cli.stdout.includes("room-a"));
    }],
    ["cli action status works", async () => {
      const response = await action(sourceRun("SELFTEST_OK cli-status"));
      const cli = await runCli(["action", "status", response.body.jobId]);
      assertEqual(cli.code, 0);
      assert(cli.stdout.includes(response.body.jobId));
    }],
    ["cli action run works", async () => {
      const cli = await runCli(["action", "run", "--idempotency-key", "cli-run-one", "agent-source:dev1", "SELFTEST_OK", "cli-run"]);
      assertEqual(cli.code, 0);
      assert(cli.stderr.includes("soty-action: ok"));
    }],
    ["cli action script works", async () => {
      const scriptPath = join(tempRoot, "selftest-script.ps1");
      await writeFile(scriptPath, "SELFTEST_OK cli-script", "utf8");
      const cli = await runCli(["action", "script", "--idempotency-key", "cli-script-one", "agent-source:dev1", scriptPath, "powershell"]);
      assertEqual(cli.code, 0);
      assert(cli.stderr.includes("soty-action: ok"));
    }],
    ["long command is bounded before transport", async () => {
      const marker = "SELFTEST_OK long-command";
      await action(sourceRun(`${marker} ${"x".repeat(20_000)}`));
      const seen = mock.lastCommandWith(marker);
      assert(seen.length <= 8_000);
    }],
    ["status endpoint includes job and result", async () => {
      const response = await action(sourceRun("SELFTEST_OK status-shape"));
      const status = await get(response.body.statusPath);
      assert(status.body.job);
      assert(status.body.result);
    }],
    ["running job becomes interrupted after supervisor restart", async () => {
      const response = await action({ ...sourceRun("SELFTEST_HANG restart"), detached: true, idempotencyKey: "restart-interrupted" });
      await stopAgent();
      agent = await startAgent({ port, relayUrl: `http://127.0.0.1:${mock.server.address().port}` });
      const status = await get(response.body.statusPath);
      assertEqual(status.body.job.status, "interrupted");
    }],
    ["idempotent retry after restart does not duplicate interrupted job", async () => {
      const response = await action({ ...sourceRun("SELFTEST_HANG restart"), detached: true, idempotencyKey: "restart-interrupted" });
      assertEqual(response.body.status, "interrupted");
      assert(mock.count("SELFTEST_HANG restart") <= 1);
    }],
    ["release builder reuses committed skill bundle", async () => {
      const cli = await runNode([join(root, "scripts", "build-agent-release.mjs")], {
        ...process.env,
        SOTY_OPS_SKILL_SOURCE: join(tempRoot, "missing-skill")
      }, root);
      assertEqual(cli.code, 0);
      assert(cli.stdout.includes("ops-skill:"));
    }],
    ["public manifest still validates after fallback build", async () => {
      const manifest = JSON.parse(await readFile(join(root, "public", "agent", "manifest.json"), "utf8"));
      assertEqual(manifest.version, "0.3.112");
      assertEqual(manifest.windowsReinstall.scripts.length, 3);
    }]
  ];
  assertEqual(cases.length, 50);
  for (const [name, fn] of cases) {
    await fn();
    scenariosRun += 1;
    process.stdout.write(`ok ${scenariosRun} - ${name}\n`);
  }
}

function sourceRun(command) {
  return { target: "agent-source:dev1", command };
}

function sourceScript(script) {
  return { mode: "script", target: "agent-source:dev1", script, shell: "powershell" };
}

async function action(body) {
  return await post("/operator/action", body);
}

async function waitForStatus(path, status) {
  for (let index = 0; index < 40; index += 1) {
    const response = await get(path);
    if (response.body?.result?.status === status || response.body?.job?.status === status) {
      return response;
    }
    await sleep(100);
  }
  throw new Error(`status ${status} did not appear at ${path}`);
}

function expectStatus(response, status) {
  assertEqual(response.body.status, status);
  return response;
}

function expectFamily(response, family) {
  expectStatus(response, "ok");
  assertEqual(response.body.family, family);
}

function expectRisk(response, risk) {
  expectStatus(response, "ok");
  assertEqual(response.body.risk, risk);
}

function createMockRelay() {
  const calls = [];
  const server = createServer(async (request, response) => {
    const url = new URL(request.url || "/", "http://127.0.0.1");
    if (url.pathname === "/agent/manifest.json") {
      json(response, 200, { version: "0.0.0" });
      return;
    }
    if (url.pathname === "/api/agent/source/run" || url.pathname === "/api/agent/source/script") {
      const body = await readBody(request);
      const payload = JSON.parse(body || "{}");
      const text = String(payload.command || payload.script || "");
      calls.push(text);
      if (text.includes("SELFTEST_HANG")) {
        await sleep(30_000);
      }
      if (text.includes("SELFTEST_DELAY")) {
        await sleep(250);
      }
      if (text.includes("SELFTEST_BAD_JSON")) {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end("{");
        return;
      }
      if (text.includes("SELFTEST_HTTP_500")) {
        json(response, 500, { ok: false, text: "! mock-http", exitCode: 500 });
        return;
      }
      if (text.includes("SELFTEST_TIMEOUT")) {
        json(response, 200, { ok: false, text: "! timeout", exitCode: 124 });
        return;
      }
      if (text.includes("SELFTEST_FAIL")) {
        json(response, 200, { ok: false, text: "! mock-fail", exitCode: 7 });
        return;
      }
      const output = text.includes("SELFTEST_LARGE")
        ? `SELFTEST_LARGE ${"x".repeat(20_000)} volume=22 muted=false`
        : `SELFTEST_OK output for ${text} volume=22 muted=false`;
      json(response, 200, { ok: true, text: output, exitCode: 0 });
      return;
    }
    if (url.pathname === "/api/agent/learning/receipts") {
      json(response, 200, { ok: true, accepted: 1 });
      return;
    }
    json(response, 404, { ok: false });
  });
  return {
    server,
    count: (needle) => calls.filter((item) => item.includes(needle)).length,
    lastCommandWith: (needle) => [...calls].reverse().find((item) => item.includes(needle)) || ""
  };
}

async function startAgent({ port: requestedPort, relayUrl }) {
  const child = spawn(process.execPath, [
    agentPath,
    "--port",
    String(requestedPort),
    "--relay-id",
    "selftest_relay_00000000000000000001",
    "--update-url",
    `${relayUrl}/agent/manifest.json`
  ], {
    cwd: tempRoot,
    env: {
      ...process.env,
      SOTY_AGENT_ACTION_JOBS_DIR: actionJobsDir,
      SOTY_AGENT_RELAY_URL: relayUrl,
      SOTY_AGENT_REMEMBER_OUTCOMES: "0",
      SOTY_AGENT_DEV: "1",
      SOTY_AGENT_MANAGED: "0"
    },
    stdio: ["ignore", "pipe", "pipe"]
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString("utf8");
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString("utf8");
  });
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (stdout.includes(`soty-agent:${requestedPort}`)) {
      return child;
    }
    if (child.exitCode !== null) {
      throw new Error(`agent exited: ${stderr || stdout || child.exitCode}`);
    }
    await sleep(50);
  }
  throw new Error(`agent did not start: ${stderr || stdout}`);
}

async function stopAgent() {
  if (!agent) {
    return;
  }
  const child = agent;
  agent = null;
  if (child.exitCode === null) {
    child.kill();
    await Promise.race([
      onceExit(child),
      sleep(2000).then(() => {
        if (child.exitCode === null) {
          child.kill("SIGKILL");
        }
      })
    ]);
  }
}

async function post(path, body) {
  return await request("POST", path, JSON.stringify(body), { "Content-Type": "application/json" });
}

async function get(path) {
  return await request("GET", path);
}

async function raw(method, path, body) {
  return await request(method, path, body, { "Content-Type": "application/json" });
}

async function request(method, path, body = undefined, headers = {}) {
  const response = await fetch(`http://127.0.0.1:${port}${path}`, {
    method,
    headers,
    ...(body === undefined ? {} : { body })
  });
  const text = await response.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = { text };
  }
  return { status: response.status, body: parsed };
}

async function runCli(args) {
  return await runNode([agentPath, "ctl", ...args], {
    ...process.env,
    SOTY_AGENT_PORT: String(port)
  }, tempRoot);
}

function runNode(args, env, cwd) {
  return new Promise((resolvePromise) => {
    const child = spawn(process.execPath, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });
    child.on("close", (code) => {
      resolvePromise({ code, stdout, stderr });
    });
  });
}

function readBody(request) {
  return new Promise((resolvePromise, rejectPromise) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => resolvePromise(Buffer.concat(chunks).toString("utf8")));
    request.on("error", rejectPromise);
  });
}

function json(response, status, body) {
  response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify(body));
}

async function freePort() {
  const server = createServer();
  await listen(server, "127.0.0.1", 0);
  const value = server.address().port;
  await closeServer(server);
  return value;
}

function listen(server, host, requestedPort) {
  return new Promise((resolvePromise, rejectPromise) => {
    server.once("error", rejectPromise);
    server.listen(requestedPort, host, () => {
      server.off("error", rejectPromise);
      resolvePromise();
    });
  });
}

function closeServer(server) {
  return new Promise((resolvePromise) => server.close(() => resolvePromise()));
}

function onceExit(child) {
  return new Promise((resolvePromise) => child.once("exit", () => resolvePromise()));
}

function sleep(ms) {
  return new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
}

function assert(value, message = "assertion failed") {
  if (!value) {
    throw new Error(message);
  }
}

function assertEqual(actual, expected) {
  if (actual !== expected) {
    throw new Error(`expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}
