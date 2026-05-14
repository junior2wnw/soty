#!/usr/bin/env node
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import express from "express";
import { buildMemoryControl, buildMemoryQuery, buildTeacherReport } from "../server/agent-learning.js";
import { attachAgentRelay } from "../server/agent-relay.js";

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

async function runScenarios({ relayUrl } = {}) {
  const cases = [
    ["health reports new version", async () => {
      const health = await get("/health");
      assertEqual(health.status, 200);
      assertEqual(health.body.version, "0.4.14");
      assertEqual(health.body.autoUpdate, false);
      assertEqual(health.body.trace.schema, "soty.agent.trace.v1");
      assertEqual(health.body.trace.enabled, true);
      assertEqual(health.body.responseStyle.id, "lord-sysadmin");
      assertEqual(health.body.responseStyle.displayName, "Лорд");
      assertEqual(health.body.memory.schema, "soty.memory-plane.v1");
      assertEqual(health.body.memory.controller, "soty.memctl.v1");
      assertEqual(health.body.automationToolkits.schema, "soty.automation-toolkits.v2");
      assertEqual(health.body.automationToolkits.frontDoor, "soty_toolkit");
      assert(health.body.automationToolkits.available.includes("capability-gateway"));
      assert(health.body.automationToolkits.available.includes("durable-action"));
      assert(health.body.automationToolkits.available.includes("windows-reinstall"));
      assertEqual(health.body.automationToolkits.defaultKernel, "soty_action");
      assertEqual(health.body.automationToolkits.responseStyle.id, "lord-sysadmin");
    }],
    ["trace endpoint lists diagnostic turns", async () => {
      const traces = await get("/agent/traces?limit=5");
      assertEqual(traces.status, 200);
      assertEqual(traces.body.schema, "soty.agent.trace.v1");
      assert(Array.isArray(traces.body.traces));
    }],
    ["toolkit endpoint exposes universal contract", async () => {
      const toolkits = await get("/operator/toolkits");
      assertEqual(toolkits.status, 200);
      assertEqual(toolkits.body.schema, "soty.automation-toolkits.v2");
      assertEqual(toolkits.body.responseStyle.displayName, "Лорд");
      assert(toolkits.body.toolkits.some((toolkit) => toolkit.name === "capability-gateway"));
      assert(toolkits.body.toolkits.some((toolkit) => toolkit.name === "durable-action"));
      assert(toolkits.body.toolkits.some((toolkit) => toolkit.name === "windows-reinstall"));
    }],
    ["actions list starts empty", async () => {
      const list = await get("/operator/actions");
      assertEqual(list.status, 200);
      assert(Array.isArray(list.body.jobs));
      assertEqual(list.body.jobs.length, 0);
    }],
    ["installed agent leases source jobs directly", async () => {
      mock.resetDirectSource();
      const bind = await post("/agent/relay", {
        relayId: "selftest_relay_00000000000000000001",
        relayBaseUrl: relayUrl,
        deviceId: "dev-direct",
        deviceNick: "selftest-direct"
      });
      assertEqual(bind.status, 200);
      assertEqual(bind.body.ok, true);
      const output = await waitForDirectSourceOutput("direct_selftest_1");
      assert(output.text.includes("DIRECT_SOURCE_OK"));
      assertEqual(output.exitCode, 0);
      assert(mock.directPolls().some((item) => String(item.clientCapabilities || "").includes("direct-device-worker")));
      const health = await get("/health");
      assertEqual(health.body.sourceWorker, true);
      assertEqual(health.body.deviceId, "dev-direct");
    }],
    ["real relay routes user jobs to user companion and system jobs to machine worker", async () => {
      const app = express();
      attachAgentRelay(app);
      const relayServer = createServer(app);
      await listen(relayServer, "127.0.0.1", 0);
      const base = `http://127.0.0.1:${relayServer.address().port}`;
      const relayId = "relay_plane_selftest_0000000000000001";
      const deviceId = "plane-device";
      const systemQuery = sourceWorkerQuery(relayId, deviceId, {
        scope: "Machine",
        companion: false,
        system: true,
        executionPlane: "system-controller+user-session-companion-required"
      });
      const userQuery = sourceWorkerQuery(relayId, deviceId, {
        scope: "CurrentUser",
        companion: true,
        system: false,
        executionPlane: "user-session-companion"
      });
      try {
        await relayRequest(base, "POST", "/api/agent/source/grant", { relayId, deviceId, enabled: true });
        await relayRequest(base, "GET", `/api/agent/source/poll?${systemQuery}`);
        const blockedUser = await relayRequest(base, "POST", "/api/agent/source/start", {
          relayId,
          deviceId,
          type: "run",
          command: "echo user",
          runAs: "user",
          timeoutMs: 5000
        });
        assertEqual(blockedUser.status, 409);
        assert(String(blockedUser.body.text || "").includes("user-session Soty Agent companion"));

        await relayRequest(base, "GET", `/api/agent/source/poll?${userQuery}`);
        const waitingSystemPoll = relayRequest(base, "GET", `/api/agent/source/poll?${sourceWorkerQuery(relayId, deviceId, {
          scope: "Machine",
          companion: false,
          system: true,
          executionPlane: "system-controller+user-session-companion-required",
          wait: "1"
        })}`);
        await sleep(40);
        const userStarted = await relayRequest(base, "POST", "/api/agent/source/start", {
          relayId,
          deviceId,
          type: "run",
          command: "echo user",
          runAs: "user",
          timeoutMs: 5000
        });
        assertEqual(userStarted.status, 200);
        const waitingSystemResult = await waitingSystemPoll;
        assertEqual(waitingSystemResult.body.jobs.length, 0);
        const systemPollForUserJob = await relayRequest(base, "GET", `/api/agent/source/poll?${systemQuery}`);
        assertEqual(systemPollForUserJob.body.jobs.length, 0);
        const userPollForUserJob = await relayRequest(base, "GET", `/api/agent/source/poll?${userQuery}`);
        assertEqual(userPollForUserJob.body.jobs.length, 1);
        assertEqual(userPollForUserJob.body.jobs[0].runAs, "user");

        const systemStarted = await relayRequest(base, "POST", "/api/agent/source/start", {
          relayId,
          deviceId,
          type: "run",
          command: "whoami",
          runAs: "system",
          timeoutMs: 5000
        });
        assertEqual(systemStarted.status, 200);
        const userPollForSystemJob = await relayRequest(base, "GET", `/api/agent/source/poll?${userQuery}`);
        assertEqual(userPollForSystemJob.body.jobs.length, 0);
        const systemPollForSystemJob = await relayRequest(base, "GET", `/api/agent/source/poll?${systemQuery}`);
        assertEqual(systemPollForSystemJob.body.jobs.length, 1);
        assertEqual(systemPollForSystemJob.body.jobs[0].runAs, "system");
      } finally {
        await closeServer(relayServer);
      }
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
    ["source route failure carries diagnostic", async () => {
      const response = await action(sourceRun("SELFTEST_AGENT_SOURCE_MISSING"));
      expectStatus(response, "failed");
      assertEqual(response.body.exitCode, 404);
      assertEqual(response.body.diagnostic.reason, "not-found");
    }],
    ["source link status exposes relay diagnostic", async () => {
      const response = await get("/operator/source-status?sourceRelayId=selftest_relay_00000000000000000001&sourceDeviceId=dev1");
      assertEqual(response.status, 200);
      assertEqual(response.body.relay.reason, "ok");
      assertEqual(response.body.relay.source.connected, true);
    }],
    ["ordinary target without bridge fails narrowly", async () => {
      const response = await action({ target: "room-a", command: "whoami SELFTEST_OK" });
      expectStatus(response, "failed");
      assertEqual(response.body.exitCode, 409);
    }],
    ["ordinary target with source device uses active source link", async () => {
      const response = await action({
        target: "room-a",
        sourceDeviceId: "dev1",
        command: "SELFTEST_OK ordinary-source-link"
      });
      expectStatus(response, "ok");
      assertEqual(mock.count("SELFTEST_OK ordinary-source-link"), 1);
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
    ["driver prompt goes through Codex dialog instead of prewritten fast route", async () => {
      const before = mock.count("Win32_PnPEntity");
      const response = await agentReply("check drivers");
      assert(!String(response.body.text || "").includes("Problem devices"));
      assertEqual(mock.count("Win32_PnPEntity"), before);
    }],
    ["software prompt goes through Codex dialog instead of prewritten fast route", async () => {
      const before = mock.count("Get-Command $tool.Command");
      const response = await agentReply("check python pip git node npm");
      assert(!String(response.body.text || "").includes("git:"));
      assertEqual(mock.count("Get-Command $tool.Command"), before);
    }],
    ["windows reinstall prompt goes through Codex dialog instead of prewritten preflight", async () => {
      const beforePreflight = mock.count("windows-reinstall-preflight");
      const response = await agentReply("reinstall windows");
      assert(!String(response.body.text || "").includes("backup"));
      assert(!String(response.body.text || "").includes("готово"));
      assertEqual(mock.count("windows-reinstall-preflight"), beforePreflight);
    }],
    ["repeated prompt is not swallowed by duplicate shortcut", async () => {
      await agentReply("repeat smoke no duplicate shortcut");
      const response = await agentReply("repeat smoke no duplicate shortcut");
      assert(String(response.body.text || "").trim().length > 0);
    }],
    ["hosts path does not trigger driver fast route", async () => {
      const before = mock.count("Win32_PnPEntity");
      const response = await agentReply("проверь путь C:\\Windows\\System32\\drivers\\etc\\hosts");
      assert(!String(response.body.text || "").includes("Проблемных устройств"));
      assertEqual(mock.count("Win32_PnPEntity"), before);
    }],
    ["composite memory test skips deterministic fast route", async () => {
      const before = mock.count("Win32_PnPEntity");
      const response = await agentReply([
        "Живой недетерминированный тест памяти и ускорения.",
        "Метка: Оля-QA.",
        "1) статус связи с текущим ПК;",
        "2) python и pip;",
        "3) путь C:\\Windows\\System32\\drivers\\etc\\hosts;",
        "4) запомни имя и проверь память."
      ].join("\n"));
      assert(!String(response.body.text || "").includes("Проблемных устройств"));
      assertEqual(mock.count("Win32_PnPEntity"), before);
    }],
    ["inline composite memory test skips system performance fast route", async () => {
      const before = mock.count("wuauserv,BITS");
      const response = await agentReply("Live nondeterministic memory speed test. 1) link status; 2) python and pip; 3) disk memory CPU check; 4) path C:\\Windows\\System32\\drivers\\etc\\hosts; 5) remember name.");
      assert(!String(response.body.text || "").includes("C:"));
      assertEqual(mock.count("wuauserv,BITS"), before);
    }],
    ["memory recall follow-up skips software fast route", async () => {
      const before = mock.count("Get-Command $tool.Command");
      const response = await agentReply("Memory check B. What was my name in the previous test? Repeat only python/pip, git, node/npm, hosts path.");
      assert(!String(response.body.text || "").includes("git:"));
      assertEqual(mock.count("Get-Command $tool.Command"), before);
    }],
    ["power command is classified", async () => expectFamily(await action(sourceRun("powercfg /batteryreport SELFTEST_OK")), "power-check")],
    ["package command is classified", async () => expectFamily(await action(sourceRun("winget install app SELFTEST_OK")), "package-install")],
    ["service command is classified", async () => expectFamily(await action(sourceRun("Get-Service SELFTEST_OK")), "service-check")],
    ["generic command stays generic", async () => expectFamily(await action(sourceRun("echo SELFTEST_OK")), "generic")],
    ["low risk is inferred", async () => expectRisk(await action(sourceRun("whoami SELFTEST_OK")), "low")],
    ["medium risk is inferred", async () => expectRisk(await action(sourceRun("winget install app SELFTEST_OK")), "medium")],
    ["high disk risk is inferred and detached", async () => expectDetachedRisk(await action(sourceRun("diskpart SELFTEST_OK high-detached")), "high")],
    ["windows reinstall risk is high and detached", async () => expectDetachedRisk(await action({ ...sourceRun("reinstall windows SELFTEST_OK win-detached"), family: "windows-reinstall" }), "high")],
    ["windows reinstall risk cannot be downgraded", async () => expectDetachedRisk(await action({ ...sourceRun("reinstall windows SELFTEST_OK win-risk-floor"), family: "windows-reinstall", risk: "medium" }), "high")],
    ["prepare action is detached by default", async () => {
      const response = await action({
        ...sourceRun("SELFTEST_DELAY SELFTEST_OK prepare-detached"),
        kind: "prepare",
        risk: "medium",
        idempotencyKey: "prepare-detached-one"
      });
      assertEqual(response.status, 202);
      assertEqual(response.body.status, "running");
      await waitForStatus(response.body.statusPath, "ok");
    }],
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
    ["reuse and pivot metadata are stored as portable action context", async () => {
      const response = await action({
        ...sourceScript("echo SELFTEST_OK reuse-pivot"),
        reuseKey: "powershell-structured-proof",
        pivotFrom: "driver probe",
        successCriteria: "json output plus exitCode 0",
        scriptUse: "verify",
        contextFingerprint: "windows-link"
      });
      const status = await get(response.body.statusPath);
      assertEqual(status.body.job.reuseKey, "powershell-structured-proof");
      assertEqual(status.body.result.scriptUse, "verify");
      assert(status.body.result.proof.includes("reuseKey=powershell-structured-proof"));
      assert(status.body.result.proof.includes("pivotFrom=driver-probe"));
      assert(status.body.result.proof.includes("successCriteria=set"));
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
    ["detached action can be stopped", async () => {
      const response = await action({ ...sourceRun("SELFTEST_HANG cancel"), detached: true, idempotencyKey: "detached-cancel-one" });
      assertEqual(response.status, 202);
      await waitForCall("SELFTEST_HANG cancel");
      const stop = await post(`/operator/action/${response.body.jobId}/stop`, {});
      assert(stop.status === 200 || stop.status === 202);
      const status = await waitForStatus(response.body.statusPath, "cancelled");
      assertEqual(status.body.job.status, "cancelled");
      await waitForCancel(response.body.jobId);
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
      assert(outbox.includes("\"toolkit\":\"durable-action\"") || outbox.includes("\"toolkit\": \"durable-action\""));
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
    ["cli toolkit describe works", async () => {
      const cli = await runCli(["toolkit", "describe"]);
      assertEqual(cli.code, 0);
      assert(cli.stdout.includes("soty.automation-toolkits.v2"));
      assert(cli.stdout.includes("soty_toolkit"));
    }],
    ["cli toolkit run uses durable-action metadata", async () => {
      const cli = await runCli(["toolkit", "run", "--phase", "probe", "--idempotency-key", "cli-toolkit-run-one", "agent-source:dev1", "SELFTEST_OK", "cli-toolkit"]);
      assertEqual(cli.code, 0);
      assert(cli.stderr.includes("soty-action: ok"));
      const list = await get("/operator/actions");
      const job = list.body.jobs.find((item) => item.idempotencyKey === "cli-toolkit-run-one");
      assert(job);
      assertEqual(job.toolkit, "durable-action");
      assertEqual(job.phase, "probe");
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
    ["release builder publishes memory plane manifest without skill bundle", async () => {
      const cli = await runNode([join(root, "scripts", "build-agent-release.mjs")], {
        ...process.env,
        SOTY_OPS_SKILL_SOURCE: join(tempRoot, "missing-skill")
      }, root);
      assertEqual(cli.code, 0);
      assert(cli.stdout.includes("memory-plane:soty.memory-plane.v1"));
      assert(!cli.stdout.includes("ops-skill:"));
    }],
    ["release builder keeps memory manifest stable", async () => {
      const manifestPath = join(root, "public", "agent", "manifest.json");
      const first = await runNode([join(root, "scripts", "build-agent-release.mjs")], process.env, root);
      assertEqual(first.code, 0);
      const firstManifest = JSON.parse(await readFile(manifestPath, "utf8"));
      const second = await runNode([join(root, "scripts", "build-agent-release.mjs")], process.env, root);
      assertEqual(second.code, 0);
      const secondManifest = JSON.parse(await readFile(manifestPath, "utf8"));
      assertEqual(secondManifest.schema, "soty.agent.release.v2");
      assertEqual(secondManifest.memoryPlane.schema, firstManifest.memoryPlane.schema);
      assertEqual(secondManifest.memoryPlane.controller, "soty.memctl.v1");
      assertEqual(secondManifest.memoryPlane.queryUrl, "/api/agent/memory/query");
      assertEqual(secondManifest.opsSkill, undefined);
    }],
    ["windows reinstall scripts default to managed Cyrillic passwordless account", async () => {
      const agent = await readFile(join(root, "scripts", "soty-agent.mjs"), "utf8");
      const relay = await readFile(join(root, "server", "agent-relay.js"), "utf8");
      const main = await readFile(join(root, "src", "main.ts"), "utf8");
      const prepare = await readFile(join(root, "scripts", "windows", "soty-prepare-windows-reinstall.ps1"), "utf8");
      const arm = await readFile(join(root, "scripts", "windows", "soty-arm-windows-reinstall.ps1"), "utf8");
      const fastUsb = await readFile(join(root, "scripts", "windows", "soty-make-fast-usb.ps1"), "utf8");
      const managed = await readFile(join(root, "scripts", "windows", "soty-managed-windows-reinstall.ps1"), "utf8");
      const windowsInstall = await readFile(join(root, "public", "agent", "install-windows.ps1"), "utf8");
      assert(agent.includes("sotyRuntimeHints"));
      assert(agent.includes("runAgentSourceWorkerLoop"));
      assert(agent.includes("direct-device-worker"));
      assert(agent.includes("sourceWorker: canRunAgentSourceWorker()"));
      assert(!agent.includes("configuredAgentDeviceId"));
      assert(!agent.includes("configuredAgentDeviceNick"));
      assert(!agent.includes('$p.PSObject.Properties["${prefix}.cx"]'));
      assert(!agent.includes('$p.PSObject.Properties["${prefix}.cy"]'));
      assert(agent.includes("$p.PSObject.Properties[($prefix + '.cx')]"));
      assert(agent.includes("$p.PSObject.Properties[($prefix + '.cy')]"));
      assert(agent.includes("clean-codex+memory-plane+capability-gateway"));
      assert(agent.includes("soty_image"));
      assert(agent.includes("source device does not need an OpenAI API key"));
      assert(agent.includes("soty_artifact"));
      assert(agent.includes("Source-device canonical"));
      assert(agent.includes("Device execution plane"));
      assert(agent.includes("windowsInteractiveTaskSpec"));
      assert(agent.includes("LogonType Interactive"));
      assert(agent.includes("system-controller+user-session-companion-required"));
      assert(agent.includes("SOTY_AGENT_COMPANION"));
      assert(agent.includes("ensureWindowsUserCompanion"));
      assert(agent.includes("soty-agent-user"));
      assert(agent.includes("user-session-agent-unavailable"));
      assert(agent.includes("allowWindowsInteractiveTaskBridge"));
      assert(agent.includes("New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited"));
      assert(!agent.includes("-RunLevel LeastPrivilege"));
      assert(relay.includes("sourceWorkerRoute"));
      assert(relay.includes("workers: {}"));
      assert(relay.includes("user-session-agent-unavailable"));
      assert(relay.includes("waiter.buildPayload"));
      assert(!windowsInstall.includes("Disable-CurrentUserAgentAutostart"));
      assert(!windowsInstall.includes("Stop-ExistingSotyAgentsForMachine"));
      assert(agent.includes("no active interactive Windows user session"));
      assert(agent.includes("sourceArtifactChunkScript"));
      assert(agent.includes('savedBy: "source-device"'));
      assert(agent.includes("Server workspace is allowed"));
      assert(agent.includes("display before wallpaper"));
      assert(agent.includes('runAs: "user"'));
      assert(agent.includes('runAs: "system"'));
      assert(agent.includes("soty_toolkit"));
      assert(agent.includes("automationToolkitStatus"));
      assert(agent.includes("soty_toolkits"));
      assert(agent.includes("waitForSotyReinstallPrepare"));
      assert(agent.includes("waitForCompletion"));
      assert(agent.includes("sourceManagedWindowsReinstallBootstrap"));
      assert(agent.includes("86400000"));
      assert(agent.includes("mcpInlineToolBudgetMs"));
      assert(agent.includes("maybeRedirectManagedReinstallProbe"));
      assert(agent.includes("managed-reinstall-toolkit-required"));
      assert(agent.includes("waitMs"));
      assert(agent.includes("reusedExistingPrepare"));
      assert(agent.includes("isReinstallPrepareActive"));
      assert(agent.includes("post-arm-rebooting"));
      assert(agent.includes("rememberPostArmReboot"));
      assert(agent.includes("agentResponseStyleProfiles"));
      assert(agent.includes("lord-sysadmin"));
      assert(agent.includes("Лорд"));
      assert(agent.includes("response_style_rule_${index + 1}"));
      assert(agent.includes("shouldAutoReplyOperatorMessage"));
      assert(!agent.includes("isActionableTargetOperatorMessage"));
      assert(agent.includes('preferredTargetId: agentDialog ? "" : item.target'));
      assert(agent.includes("Capability contract:"));
      assert(agent.includes("soty_browser for browser work"));
      assert(agent.includes("soty_desktop for screen/mouse/keyboard"));
      assert(agent.includes("soty_action or soty_reinstall for long jobs"));
      assert(agent.includes("Use memory as short reusable hints"));
      assert(agent.includes("const allTargets = sanitizeTargets(safe.operatorTargets)"));
      assert(!agent.includes("waitForTurnkeyTargetAfterCodex"));
      assert(!agent.includes("turnkeyGuardTimeoutMs"));
      assert(!agent.includes("guardTurnkeyMessages"));
      assert(!agent.includes("managedReinstallGuardTerminal"));
      assert(!agent.includes("reinstallHardPreflightBlockers"));
      assert(!agent.includes("handoff=codex-agent"));
      assert(!agent.includes("tryFastWindowsReinstallGateReply"));
      assert(!agent.includes("codex.duplicate-turn"));
      assert(!agent.includes("isLikelyAgentStatusQuote"));
      assert(!main.includes("sendOperatorUserMessage"));
      assert(!main.includes('type: "operator.message"'));
      assert(main.includes("containsLordAgentInvocation"));
      assert(main.includes("(?:лорд|lord)"));
      assert(main.includes("explicitMention: true"));
      assert(agent.includes("learningContextForTurn"));
      assert(agent.includes("targetHash"));
      assert(agent.includes("sourceDeviceHash"));
      assert(agent.includes("soty.agent.trace.v1"));
      assert(agent.includes("beginAgentTrace"));
      assert(agent.includes("/agent/traces"));
      assert(!agent.includes("enableFastDirectAnswers"));
      assert(agent.includes("hasExplicitEventLogIntent"));
      assert(!agent.includes("shouldRunDeterministicFastRoutine"));
      assert(!agent.includes("isCreativeOrGenerativeMessage"));
      assert(relay.includes("pollRequesterCanLeaseSourceJobs"));
      assert(relay.includes("direct-device-worker-required"));
      assert(relay.includes("agentSourceDirectWorkerFresh"));
      assert(relay.includes("directWorkerSeenAt"));
      assert(relay.includes("isDirectWorkerHeartbeat || !agentSourceDirectWorkerFresh(source)"));
      assert(relay.includes("installed Soty Agent must refresh before direct device control is available"));
      assert(!main.includes("localAgent.sourceWorker !== true"));
      assert(!main.includes("function runAgentSourceJob"));
      assert(!main.includes("pollAgentSourceCommands"));
      assert(!main.includes("sendAgentSourceOutput"));
      assert(!main.includes("Ничего не менял"));
      assert(!main.includes("Не смог сейчас ответить"));
      assert(managed.includes("Get-MediaStatus"));
      assert(managed.includes("updatedAgeSeconds"));
      assert(managed.includes('*.download.parts'));
      assert(managed.includes("partBytes"));
      assert(managed.includes("Get-ManagedScript"));
      assert(managed.includes("managed account must be passwordless"));
      assert(managed.includes("[System.IO.File]::Open"));
      const tailTextStart = managed.indexOf("function Tail-Text");
      const tailTextEnd = managed.indexOf("function Normalize-UsbLetter");
      const tailTextBody = managed.slice(tailTextStart, tailTextEnd);
      assert(tailTextStart >= 0 && tailTextEnd > tailTextStart);
      assert(!tailTextBody.includes("Get-Content -LiteralPath $Path -Raw"));
      assert(tailTextBody.includes("Get-Content -LiteralPath $Path -Tail 80"));
      assert(prepare.includes("Invoke-ResumableDownload"));
      assert(prepare.includes("Invoke-ParallelRangeDownloadAttempt"));
      assert(prepare.includes('$partDir = $TempPath + ".parts"'));
      assert(prepare.includes('"--range"'));
      assert(prepare.includes('"65536"'));
      assert(prepare.includes('"-C", "-"'));
      assert(prepare.includes("Windows image download did not complete within the retry window"));
      const sotyUserCodepoints = "0x0421, 0x043E, 0x0442, 0x044B";
      assert(prepare.includes(sotyUserCodepoints));
      assert(fastUsb.includes(sotyUserCodepoints));
      assert(arm.includes(sotyUserCodepoints));
      assert(managed.includes("Get-SotyUserName"));
      for (const script of [prepare, arm, fastUsb, managed]) {
        assert(!script.includes('"Соты"'));
      }
      assert(prepare.includes("UTF8Encoding($true)"));
      assert(fastUsb.includes("UTF8Encoding($true)"));
      assert(prepare.includes("$AllowTemporaryManagedPassword -and -not $NoTemporaryManagedPassword"));
      assert(fastUsb.includes("$AllowTemporaryManagedPassword -and -not $NoTemporaryManagedPassword"));
      assert(!prepare.includes("$AllowTemporaryManagedPassword -or -not $NoTemporaryManagedPassword"));
      assert(!fastUsb.includes("$AllowTemporaryManagedPassword -or -not $NoTemporaryManagedPassword"));
      assert(prepare.includes("personal-folders-Desktop-Documents-Downloads-Pictures-Videos-Music"));
      assert(prepare.includes("personalFolderCounts"));
      assert(prepare.includes("Preferred Windows edition from current OS"));
      assert(prepare.includes("Test-WindowsEditionMatch"));
      assert(arm.includes("Managed user must be passwordless before arming"));
      assert(arm.includes("Personal-folder backup proof is missing"));
    }],
    ["public manifest still validates after fallback build", async () => {
      const manifest = JSON.parse(await readFile(join(root, "public", "agent", "manifest.json"), "utf8"));
      assertEqual(manifest.version, "0.4.14");
      assertEqual(manifest.schema, "soty.agent.release.v2");
      assertEqual(manifest.memoryPlane.schema, "soty.memory-plane.v1");
      assertEqual(manifest.memoryPlane.controller, "soty.memctl.v1");
      assertEqual(manifest.memoryPlane.querySchema, "soty.memory.query.v2");
      assertEqual(manifest.opsSkill, undefined);
      assertEqual(manifest.windowsReinstall.scripts.length, 4);
      assert(manifest.windowsReinstall.scripts.some((script) => script.name === "managed"));
      assertEqual(manifest.automationToolkits.schema, "soty.automation-toolkits.v2");
      assertEqual(manifest.automationToolkits.policy.entrypoint, "soty_toolkit");
      assertEqual(manifest.automationToolkits.policy.fallbackKernel, "soty_action");
      assertEqual(manifest.automationToolkits.policy.chat, "lord-sysadmin");
      assertEqual(manifest.automationToolkits.policy.diagnostics.trace, "soty.agent.trace.v1");
      assertEqual(manifest.automationToolkits.policy.diagnostics.eval, "soty-agent-eval");
      assertEqual(manifest.automationToolkits.policy.responseStyle.displayName, "Лорд");
      assertEqual(manifest.automationToolkits.policy.responseStyle.phraseBank.length, 0);
      const capabilityGateway = manifest.automationToolkits.toolkits.find((toolkit) => toolkit.name === "capability-gateway");
      assert(capabilityGateway);
      assertEqual(capabilityGateway.entryTool, "soty_toolkit");
      const reinstallToolkit = manifest.automationToolkits.toolkits.find((toolkit) => toolkit.name === "windows-reinstall");
      assert(reinstallToolkit);
      assertEqual(reinstallToolkit.entryTool, "soty_reinstall");
      assert(reinstallToolkit.scripts.some((script) => script.name === "managed"));
      const durableKernel = manifest.automationToolkits.toolkits.find((toolkit) => toolkit.name === "durable-action");
      assert(durableKernel);
      assertEqual(durableKernel.entryTool, "soty_action");
    }],
    ["agent installers stay lightweight by default", async () => {
      const windowsInstall = await readFile(join(root, "public", "agent", "install-windows.ps1"), "utf8");
      const windowsMachineInstall = await readFile(join(root, "public", "agent", "install-windows-machine.cmd"), "utf8");
      const unixInstall = await readFile(join(root, "public", "agent", "install-macos-linux.sh"), "utf8");
      const ui = await readFile(join(root, "src", "main.ts"), "utf8");
      const tooltips = await readFile(join(root, "src", "ui", "tooltips.ts"), "utf8");
      const agentFeature = await readFile(join(root, "src", "features", "agent.ts"), "utf8");
      const agentSource = await readFile(join(root, "scripts", "soty-agent.mjs"), "utf8");
      const httpApp = await readFile(join(root, "server", "http-app.js"), "utf8");
      let userWindowsInstallerExists = true;
      try {
        await readFile(join(root, "public", "agent", "install-windows.cmd"), "utf8");
      } catch {
        userWindowsInstallerExists = false;
      }
      assert(windowsInstall.includes("[switch]$InstallCodex"));
      assert(windowsInstall.includes("[string]$CodexProxyUrl"));
      assert(windowsInstall.includes('[string]$Scope = "Machine"'));
      assert(windowsInstall.includes("proxy.env"));
      assert(windowsInstall.includes("SOTY_CODEX_PROXY_URL"));
      assert(windowsInstall.includes("if ($InstallCodex)"));
      assert(windowsInstall.includes("soty-codex-cli:install-skipped:default-light-agent"));
      assert(windowsInstall.includes('$env:SOTY_AGENT_AUTO_UPDATE = "1"'));
      assert(windowsInstall.includes("if (`$code -eq 75)"));
      assert(windowsInstall.includes("Write-AgentConfigSeed"));
      assert(windowsInstall.includes("agent-config.json"));
      assert(!windowsInstall.includes("universal-install-ops"));
      assert(!windowsInstall.includes("ops-skill"));
      assert(!userWindowsInstallerExists);
      assert(windowsMachineInstall.includes("-Scope Machine"));
      assert(windowsMachineInstall.includes("-LaunchAppAtLogon"));
      assert(windowsMachineInstall.includes("Start-Process -FilePath 'powershell.exe' -Verb RunAs"));
      assert(windowsMachineInstall.includes("SOTY_AGENT_DEVICE_ID"));
      assert(windowsMachineInstall.includes("SOTY_AGENT_DEVICE_NICK"));
      assert(agentFeature.includes('"/agent/install-windows-machine.cmd"'));
      assert(agentFeature.includes('"install-soty-agent-machine.cmd"'));
      assert(!agentFeature.includes("/agent/install-windows.cmd"));
      assert(!agentFeature.includes('"install-soty-agent.cmd"'));
      assert(ui.includes('downloadAgentInstallerForDevice("machine", sourceDeviceForInstaller)'));
      assert(!ui.includes('downloadAgentInstallerForDevice("user", sourceDeviceForInstaller)'));
      assert(!ui.includes("machine-button"));
      assert(!ui.includes("canInstallMachineAgent"));
      assert(!ui.includes("Скачать обычный установщик"));
      assert(tooltips.includes("Скачать Soty Agent"));
      assert(!tooltips.includes("Скачать обычный установщик"));
      assert(agentSource.includes('const agentVersion = "0.4.14"'));
      assert(agentSource.includes("void saveAgentConfig();"));
      assert(agentSource.includes("function scheduleUpdate()"));
      assert(agentSource.includes("process.exit(75)"));
      assert(httpApp.includes('app.use("/agent"'));
      assert(httpApp.includes("agent_asset_not_found"));
      assert(unixInstall.includes("INSTALL_CODEX=\"0\""));
      assert(unixInstall.includes("--codex-proxy-url"));
      assert(unixInstall.includes("proxy.env"));
      assert(unixInstall.includes("SOTY_CODEX_PROXY_URL"));
      assert(unixInstall.includes("--install-codex"));
      assert(unixInstall.includes("soty-codex-cli:install-skipped:default-light-agent"));
      assert(!unixInstall.includes("universal-install-ops"));
      assert(!unixInstall.includes("ops-skill"));
      assert(!ui.includes("Первый ответ может занять"));
      assert(!ui.includes("первый запуск может занять"));
      assert(!ui.includes("progressTimer"));
    }],
    ["managed agent auto-updates and exits for runner restart", async () => {
      const updateDir = await mkdtemp(join(tmpdir(), "soty-update-selftest-"));
      const updateAgentPath = join(updateDir, "soty-agent.mjs");
      const nextSource = await readFile(sourceAgentPath, "utf8");
      const oldSource = nextSource.replace('const agentVersion = "0.4.14";', 'const agentVersion = "0.4.13";');
      assert(oldSource.includes('const agentVersion = "0.4.13"'));
      await writeFile(updateAgentPath, oldSource, "utf8");
      const nextHash = sha256(nextSource);
      const updateServer = createServer((request, response) => {
        if (request.url === "/manifest.json") {
          json(response, 200, {
            version: "0.4.14",
            agentUrl: "/soty-agent.mjs",
            sha256: nextHash
          });
          return;
        }
        if (request.url === "/soty-agent.mjs") {
          response.writeHead(200, { "Content-Type": "application/javascript", "Cache-Control": "no-store" });
          response.end(nextSource);
          return;
        }
        response.writeHead(404);
        response.end();
      });
      let child = null;
      try {
        await listen(updateServer, "127.0.0.1", 0);
        const manifestUrl = `http://127.0.0.1:${updateServer.address().port}/manifest.json`;
        child = spawn(process.execPath, [updateAgentPath], {
          env: {
            ...process.env,
            SOTY_AGENT_MANAGED: "1",
            SOTY_AGENT_AUTO_UPDATE: "1",
            SOTY_AGENT_UPDATE_URL: manifestUrl,
            SOTY_AGENT_PORT: "0",
            SOTY_AGENT_TRACE_DIR: join(updateDir, "traces"),
            SOTY_AGENT_ACTION_JOBS_DIR: join(updateDir, "jobs")
          },
          stdio: ["ignore", "pipe", "pipe"]
        });
        const code = await waitForChildClose(child, 25_000);
        assertEqual(code, 75);
        assertEqual(sha256(await readFile(updateAgentPath)), nextHash);
      } finally {
        if (child && child.exitCode === null) {
          child.kill();
          await onceExit(child).catch(() => undefined);
        }
        await closeServer(updateServer);
        await rm(updateDir, { recursive: true, force: true }).catch(() => undefined);
      }
    }],
    ["learning teacher preserves phase and explains post-arm source loss", async () => {
      const report = buildTeacherReport([
        {
          kind: "action-job",
          result: "ok",
          toolkit: "windows-reinstall",
          phase: "arm",
          family: "windows-reinstall",
          route: "agent-source.script",
          proof: "toolkit=windows-reinstall; phase=arm; exitCode=0; rebooting=true; backupProof=ok",
          exitCode: 0,
          createdAt: "2026-05-11T16:36:23.370Z"
        },
        {
          kind: "source-command",
          result: "timeout",
          family: "windows-reinstall",
          route: "agent-source.script",
          proof: "exitCode=124; diagnostic=nonzero-exit; sourceConnected=false; source-stale",
          exitCode: 124,
          createdAt: "2026-05-11T16:38:55.956Z"
        }
      ], { limit: 2 });
      assert(report.topSuccesses.some((item) => item.phase === "arm"));
      assert(report.recommendations.some((item) => item.title === "Stop source probes after managed arm reboot"));
      assert(report.candidates.some((item) => item.marker.includes("post-arm reboot window")));
    }],
    ["learning teacher promotes hidden dialog memory markers", async () => {
      const marker = "soty-memory: goal=Soty UTF-8 identity proof | actual=use WindowsIdentity with UTF-8 output | success=DOMAIN\\User without mojibake | env=agent-dialog";
      const report = buildTeacherReport([
        {
          kind: "agent-runtime",
          result: "ok",
          family: "dialog-memory",
          route: "codex.exec.resume+soty-mcp",
          proof: marker,
          exitCode: 0,
          createdAt: "2026-05-12T14:40:00.000Z"
        }
      ], { limit: 1 });
      assert(report.candidates.some((item) => item.scope === "dialog" && item.marker === marker));
    }],
    ["learning teacher promotes low-quality runtime routes", async () => {
      const report = buildTeacherReport([
        {
          kind: "agent-runtime",
          result: "partial",
          family: "file-work",
          route: "codex.exec.resume+soty-mcp",
          proof: "exitCode=0; final=nonempty; quality=fail; qualityScore=56; missing=hashes,cleanup; tokens=actual; input=1200; output=80; total=1280; cached=0",
          exitCode: 0,
          createdAt: "2026-05-13T08:00:00.000Z"
        }
      ], { limit: 1 });
      assert(report.recommendations.some((item) => item.title === "Improve low-quality automatic route"));
      assert(report.candidates.some((item) => item.marker.includes("route quality")));
    }],
    ["learning teacher promotes reusable route capsules", async () => {
      const report = buildTeacherReport([
        {
          kind: "action-job",
          result: "ok",
          family: "system-check",
          route: "agent-source.script",
          proof: "toolkit=durable-action; phase=verify; exitCode=0; reuseKey=powershell-structured-proof; scriptUse=verify; successCriteria=set; context=windows-link",
          exitCode: 0,
          createdAt: "2026-05-13T08:05:00.000Z"
        }
      ], { limit: 1 });
      assert(report.recommendations.some((item) => item.title === "Promote reusable route capsule"));
      assert(report.candidates.some((item) => item.marker.includes("reusable route capsule powershell-structured-proof")));
    }],
    ["memctl ranks proven routes above rediscovery", async () => {
      const control = buildMemoryControl([
        {
          kind: "action-job",
          result: "ok",
          family: "system-check",
          route: "agent-source.script",
          toolkit: "durable-action",
          phase: "verify",
          platform: "win32",
          proof: "exitCode=0; reuseKey=powershell-structured-proof; scriptUse=verify; successCriteria=set; context=windows-link",
          durationMs: 900,
          createdAt: "2026-05-13T08:05:00.000Z"
        },
        {
          kind: "action-job",
          result: "ok",
          family: "system-check",
          route: "agent-source.script",
          toolkit: "durable-action",
          phase: "verify",
          platform: "win32",
          proof: "exitCode=0; reuseKey=powershell-structured-proof; scriptUse=verify; successCriteria=set; context=windows-link",
          durationMs: 1100,
          createdAt: "2026-05-13T08:06:00.000Z"
        },
        {
          kind: "codex-turn",
          result: "ok",
          family: "system-check",
          route: "codex.exec.resume",
          platform: "win32",
          proof: "tokens=actual; input=70000; output=1000; total=71000; cached=0",
          durationMs: 75000,
          createdAt: "2026-05-13T08:07:00.000Z"
        }
      ], { limit: 3 });
      assertEqual(control.schema, "soty.memctl.v1");
      assert(control.memories.provenRoutes.some((item) => item.route === "agent-source.script" && item.confidence >= 0.7));
      const query = buildMemoryQuery(control, { family: "system-check", platform: "win32" });
      assertEqual(query.schema, "soty.memory.query.v2");
      assertEqual(query.controller, "soty.memctl.v1");
      assertEqual(query.items[0].kind, "proven-route");
      assert(query.items[0].guidance.includes("Prefer agent-source.script"));
    }],
    ["memctl marks newer failures as conflicted instead of proven", async () => {
      const control = buildMemoryControl([
        {
          kind: "action-job",
          result: "ok",
          family: "file-work",
          route: "agent-source.script",
          proof: "exitCode=0; reuseKey=file-hash-proof; successCriteria=set; context=windows-link",
          createdAt: "2026-05-13T08:05:00.000Z"
        },
        {
          kind: "action-job",
          result: "failed",
          family: "file-work",
          route: "agent-source.script",
          proof: "exitCode=7; proof=permission-denied",
          exitCode: 7,
          createdAt: "2026-05-13T08:10:00.000Z"
        }
      ], { limit: 2 });
      const route = control.memories.routeCandidates.find((item) => item.family === "file-work");
      assert(route);
      assertEqual(route.promotion, "conflicted");
      assert(!control.memories.provenRoutes.some((item) => item.family === "file-work"));
      const query = buildMemoryQuery(control, { family: "file-work" });
      assert(query.items.some((item) => item.kind === "stop-gate" || item.kind === "route-guidance"));
    }]
  ];
  assert(cases.length >= 50);
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

async function agentReply(text) {
  return await post("/agent/reply", {
    text,
    context: "selftest",
    source: {
      tunnelId: "selftest-tunnel",
      tunnelLabel: "Selftest",
      deviceId: "dev1",
      deviceNick: "selftest-source",
      appOrigin: "https://xn--n1afe0b.online",
      sourceRelayId: "selftest_relay_00000000000000000001",
      operatorTargets: [
        {
          id: "agent-source:dev1",
          label: "selftest-source",
          deviceIds: ["dev1"],
          hostDeviceId: "dev1",
          access: true,
          host: true,
          selected: true,
          lastActionAt: new Date().toISOString()
        }
      ]
    }
  });
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

async function waitForCancel(jobId) {
  for (let index = 0; index < 40; index += 1) {
    if (mock.cancelCount(jobId) >= 1) {
      return;
    }
    await sleep(100);
  }
  throw new Error(`cancel ${jobId} did not arrive`);
}

async function waitForCall(needle) {
  for (let index = 0; index < 40; index += 1) {
    if (mock.count(needle) >= 1) {
      return;
    }
    await sleep(100);
  }
  throw new Error(`call ${needle} did not arrive`);
}

async function waitForDirectSourceOutput(id) {
  for (let index = 0; index < 80; index += 1) {
    const output = mock.directOutput(id);
    if (Number.isSafeInteger(output?.exitCode)) {
      return output;
    }
    await sleep(100);
  }
  throw new Error(`direct source output ${id} did not arrive`);
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

async function expectDetachedRisk(response, risk) {
  assertEqual(response.status, 202);
  assertEqual(response.body.status, "running");
  assertEqual(response.body.risk, risk);
  const status = await waitForStatus(response.body.statusPath, "ok");
  assertEqual(status.body.result.risk, risk);
  return status;
}

function createMockRelay() {
  const calls = [];
  const cancels = [];
  const sourceJobs = new Map();
  const directSourcePolls = [];
  const directSourceOutputs = new Map();
  let directSourceJobLeased = false;
  const server = createServer(async (request, response) => {
    const url = new URL(request.url || "/", "http://127.0.0.1");
    if (url.pathname === "/agent/manifest.json") {
      json(response, 200, { version: "0.0.0" });
      return;
    }
    if (url.pathname === "/api/agent/source/start") {
      const body = await readBody(request);
      const payload = JSON.parse(body || "{}");
      const text = String(payload.command || payload.script || "");
      calls.push(text);
      if (text.includes("SELFTEST_BAD_JSON")) {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end("{");
        return;
      }
      if (text.includes("SELFTEST_HTTP_500")) {
        json(response, 500, { ok: false, text: "! mock-http", exitCode: 500 });
        return;
      }
      if (text.includes("SELFTEST_AGENT_SOURCE_MISSING")) {
        json(response, 404, mockSourceMissing(payload));
        return;
      }
      const id = String(payload.clientJobId || payload.id || `mock_${sourceJobs.size + 1}`);
      sourceJobs.set(id, mockSourceJob(id, text));
      json(response, 200, { ok: true, id, status: "created", text: "" });
      return;
    }
    if (url.pathname === "/api/agent/source/job") {
      const id = url.searchParams.get("id") || "";
      const job = sourceJobs.get(id);
      if (!job) {
        json(response, 404, { ok: false, status: "missing", text: "! source-job", exitCode: 404 });
        return;
      }
      if (!Number.isSafeInteger(job.exitCode) && Date.now() >= job.finishAt) {
        job.exitCode = job.finalExitCode;
        job.text = job.finalText;
      }
      json(response, 200, {
        ok: Number.isSafeInteger(job.exitCode) ? job.exitCode === 0 : true,
        id,
        status: Number.isSafeInteger(job.exitCode)
          ? job.exitCode === 0
            ? "ok"
            : job.exitCode === 124
              ? "timeout"
              : job.exitCode === 130
                ? "cancelled"
                : "failed"
          : "running",
        text: job.text,
        ...(Number.isSafeInteger(job.exitCode) ? { exitCode: job.exitCode } : {}),
        diagnostic: { reason: Number.isSafeInteger(job.exitCode) ? "nonzero-exit" : "leased" }
      });
      return;
    }
    if (url.pathname === "/api/agent/source/run" || url.pathname === "/api/agent/source/script") {
      const body = await readBody(request);
      const payload = JSON.parse(body || "{}");
      const text = String(payload.command || payload.script || "");
      calls.push(text);
      if (text.includes("SELFTEST_HANG")) {
        await sleep(5000);
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
      if (text.includes("SELFTEST_AGENT_SOURCE_MISSING")) {
        json(response, 404, mockSourceMissing(payload));
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
    if (url.pathname === "/api/agent/source/targets") {
      json(response, 200, {
        ok: true,
        targets: [
          {
            id: "agent-source:dev1",
            label: "selftest-source",
            deviceIds: ["dev1"],
            hostDeviceId: "dev1",
            access: true,
            host: true,
            selected: true,
            lastActionAt: new Date().toISOString()
          }
        ]
      });
      return;
    }
    if (url.pathname === "/api/agent/source/status") {
      json(response, 200, {
        ok: true,
        relayId: url.searchParams.get("relayId") || "",
        deviceId: url.searchParams.get("deviceId") || "",
        runnable: true,
        reason: "ok",
        sourceConnectedMs: 90000,
        source: {
          relayId: url.searchParams.get("relayId") || "",
          deviceId: url.searchParams.get("deviceId") || "",
          deviceNick: "selftest-source",
          access: true,
          connected: true,
          lastSeenAt: new Date().toISOString(),
          lastSeenAgeMs: 10,
          sourceConnectedMs: 90000,
          pendingJobs: 0,
          leasedJobs: 0,
          finishedJobs: 0,
          cancels: 0,
          lastJob: null
        },
        candidates: []
      });
      return;
    }
    if (url.pathname === "/api/agent/source/poll") {
      const query = Object.fromEntries(url.searchParams.entries());
      directSourcePolls.push(query);
      if (String(query.clientCapabilities || "").includes("direct-device-worker") && !directSourceJobLeased) {
        directSourceJobLeased = true;
        json(response, 200, {
          ok: true,
          jobs: [{
            id: "direct_selftest_1",
            type: "script",
            name: "direct-selftest.mjs",
            shell: "node",
            script: "console.log('DIRECT_SOURCE_OK')",
            runAs: "user",
            timeoutMs: 5000
          }]
        });
        return;
      }
      if (query.wait === "1") {
        await sleep(100);
      }
      json(response, 200, { ok: true, jobs: [] });
      return;
    }
    if (url.pathname === "/api/agent/source/output") {
      const body = await readBody(request);
      const payload = JSON.parse(body || "{}");
      const id = String(payload.id || "");
      const previous = directSourceOutputs.get(id) || { text: "", exitCode: undefined };
      const next = {
        text: `${previous.text || ""}${String(payload.text || "")}`,
        exitCode: Number.isSafeInteger(payload.exitCode) ? payload.exitCode : previous.exitCode
      };
      directSourceOutputs.set(id, next);
      json(response, 200, { ok: true });
      return;
    }
    if (url.pathname === "/api/agent/source/cancel") {
      const body = await readBody(request);
      const payload = JSON.parse(body || "{}");
      cancels.push(String(payload.id || ""));
      json(response, 200, { ok: true, id: payload.id || "" });
      return;
    }
    if (url.pathname === "/api/agent/learning/receipts" || url.pathname === "/api/agent/memory/receipts") {
      json(response, 200, { ok: true, accepted: 1 });
      return;
    }
    if (url.pathname === "/api/agent/memory/query") {
      json(response, 200, {
        ok: true,
        schema: "soty.memory.query.v2",
        controller: "soty.memctl.v1",
        receipts: 1,
        generatedAt: new Date().toISOString(),
        source: "mock-memory",
        scope: { kind: "global-sanitized-route-memory", deviceCount: 1, platformCounts: [], agentVersions: [] },
        publishModel: "reviewed-memory-route-then-release",
        stats: { provenRoutes: 0, stopGates: 0, routeFixes: 0 },
        items: []
      });
      return;
    }
    json(response, 404, { ok: false });
  });
  return {
    server,
    count: (needle) => calls.filter((item) => item.includes(needle)).length,
    cancelCount: (id) => cancels.filter((item) => item === id).length,
    lastCommandWith: (needle) => [...calls].reverse().find((item) => item.includes(needle)) || "",
    directPolls: () => directSourcePolls.slice(),
    directOutput: (id) => directSourceOutputs.get(id),
    resetDirectSource: () => {
      directSourcePolls.length = 0;
      directSourceOutputs.clear();
      directSourceJobLeased = false;
    }
  };
}

function mockSourceMissing(payload) {
  return {
    ok: false,
    text: "! agent-source: not-found",
    exitCode: 404,
    diagnostic: {
      reason: "not-found",
      relayId: payload.relayId || "",
      deviceId: payload.deviceId || "",
      sourceConnectedMs: 90000,
      source: null,
      candidates: []
    }
  };
}

function mockSourceJob(id, text) {
  const delay = text.includes("SELFTEST_DELAY") ? 250 : 0;
  if (text.includes("SELFTEST_HANG")) {
    return { id, text: "", finalText: "", finishAt: Number.MAX_SAFE_INTEGER, finalExitCode: 0 };
  }
  if (text.includes("SELFTEST_TIMEOUT")) {
    return { id, text: "", finalText: "! timeout", finishAt: Date.now() + delay, finalExitCode: 124 };
  }
  if (text.includes("SELFTEST_FAIL")) {
    return { id, text: "", finalText: "! mock-fail", finishAt: Date.now() + delay, finalExitCode: 7 };
  }
  const output = text.includes("SELFTEST_LARGE")
    ? `SELFTEST_LARGE ${"x".repeat(20_000)} volume=22 muted=false`
    : `SELFTEST_OK output for ${text} volume=22 muted=false`;
  return { id, text: "", finalText: output, finishAt: Date.now() + delay, finalExitCode: 0 };
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
      SOTY_AGENT_MANAGED: "0",
      SOTY_CODEX_DISABLED: "1",
      SOTY_CODEX_RELAY_FALLBACK: "0"
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

function sourceWorkerQuery(relayId, deviceId, options) {
  return new URLSearchParams({
    relayId,
    deviceId,
    deviceNick: "plane-device",
    wait: options.wait || "0",
    clientProtocol: "soty-source-agent.v1",
    clientCapabilities: "runas,local-agent-health,direct-device-worker",
    localAgentOk: "true",
    localAgentVersion: "0.4.14",
    localAgentScope: options.scope,
    localAgentCompanion: options.companion ? "true" : "false",
    localAgentExecutionPlane: options.executionPlane,
    localAgentAutoUpdate: "true",
    localAgentSystem: options.system ? "true" : "false",
    localAgentSourceWorker: "true"
  }).toString();
}

async function relayRequest(base, method, path, body = undefined) {
  const response = await fetch(`${base}${path}`, {
    method,
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) })
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

function waitForChildClose(child, timeoutMs) {
  return new Promise((resolvePromise, rejectPromise) => {
    const timer = setTimeout(() => {
      child.kill();
      rejectPromise(new Error("child close timeout"));
    }, timeoutMs);
    child.once("close", (code) => {
      clearTimeout(timer);
      resolvePromise(code);
    });
    child.once("error", (error) => {
      clearTimeout(timer);
      rejectPromise(error);
    });
  });
}

function sleep(ms) {
  return new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
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
