#!/usr/bin/env node
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildTeacherReport } from "../server/agent-learning.js";

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
      assertEqual(health.body.version, "0.3.139");
      assertEqual(health.body.automationToolkits.schema, "soty.automation-toolkits.v1");
      assertEqual(health.body.automationToolkits.frontDoor, "soty_toolkit");
      assert(health.body.automationToolkits.available.includes("universal-toolkit"));
      assert(health.body.automationToolkits.available.includes("durable-action"));
      assert(health.body.automationToolkits.available.includes("windows-reinstall"));
      assertEqual(health.body.automationToolkits.defaultKernel, "soty_action");
    }],
    ["toolkit endpoint exposes universal contract", async () => {
      const toolkits = await get("/operator/toolkits");
      assertEqual(toolkits.status, 200);
      assertEqual(toolkits.body.schema, "soty.automation-toolkits.v1");
      assert(toolkits.body.toolkits.some((toolkit) => toolkit.name === "durable-action"));
      assert(toolkits.body.toolkits.some((toolkit) => toolkit.name === "windows-reinstall"));
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
      assert(cli.stdout.includes("soty.automation-toolkits.v1"));
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
    ["release builder reuses committed skill bundle", async () => {
      const cli = await runNode([join(root, "scripts", "build-agent-release.mjs")], {
        ...process.env,
        SOTY_OPS_SKILL_SOURCE: join(tempRoot, "missing-skill")
      }, root);
      assertEqual(cli.code, 0);
      assert(cli.stdout.includes("ops-skill:"));
    }],
    ["release builder keeps ops skill tar hash stable", async () => {
      const manifestPath = join(root, "public", "agent", "manifest.json");
      const first = await runNode([join(root, "scripts", "build-agent-release.mjs")], process.env, root);
      assertEqual(first.code, 0);
      const firstManifest = JSON.parse(await readFile(manifestPath, "utf8"));
      const second = await runNode([join(root, "scripts", "build-agent-release.mjs")], process.env, root);
      assertEqual(second.code, 0);
      const secondManifest = JSON.parse(await readFile(manifestPath, "utf8"));
      assertEqual(secondManifest.opsSkill.tarSha256, firstManifest.opsSkill.tarSha256);
      assertEqual(secondManifest.opsSkill.zipSha256, firstManifest.opsSkill.zipSha256);
    }],
    ["windows reinstall scripts default to managed Cyrillic passwordless account", async () => {
      const agent = await readFile(join(root, "scripts", "soty-agent.mjs"), "utf8");
      const prepare = await readFile(join(root, "scripts", "windows", "soty-prepare-windows-reinstall.ps1"), "utf8");
      const arm = await readFile(join(root, "scripts", "windows", "soty-arm-windows-reinstall.ps1"), "utf8");
      const fastUsb = await readFile(join(root, "scripts", "windows", "soty-make-fast-usb.ps1"), "utf8");
      const managed = await readFile(join(root, "scripts", "windows", "soty-managed-windows-reinstall.ps1"), "utf8");
      assert(agent.includes("turnkey_terminal_rule"));
      assert(agent.includes("soty_toolkit"));
      assert(agent.includes("toolkit_rule"));
      assert(agent.includes("toolkit_quality_rule"));
      assert(agent.includes("automationToolkitStatus"));
      assert(agent.includes("soty_toolkits"));
      assert(agent.includes("large_download_rule"));
      assert(agent.includes("waitForSotyReinstallPrepare"));
      assert(agent.includes("waitForCompletion"));
      assert(agent.includes("sourceManagedWindowsReinstallBootstrap"));
      assert(agent.includes("86400000"));
      assert(agent.includes("mcpInlineToolBudgetMs"));
      assert(agent.includes("maybeRedirectManagedReinstallProbe"));
      assert(agent.includes("managed-reinstall-toolkit-required"));
      assert(agent.includes("managed_reinstall_wait_rule"));
      assert(agent.includes("waitMs"));
      assert(agent.includes("reusedExistingPrepare"));
      assert(agent.includes("isReinstallPrepareActive"));
      assert(agent.includes("post-arm-rebooting"));
      assert(agent.includes("rememberPostArmReboot"));
      assert(managed.includes("Get-MediaStatus"));
      assert(managed.includes("updatedAgeSeconds"));
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
      assertEqual(manifest.version, "0.3.139");
      assertEqual(manifest.windowsReinstall.scripts.length, 4);
      assert(manifest.windowsReinstall.scripts.some((script) => script.name === "managed"));
      assertEqual(manifest.automationToolkits.schema, "soty.automation-toolkits.v1");
      assertEqual(manifest.automationToolkits.policy.entrypoint, "soty_toolkit");
      assertEqual(manifest.automationToolkits.policy.fallbackKernel, "soty_action");
      const universalToolkit = manifest.automationToolkits.toolkits.find((toolkit) => toolkit.name === "universal-toolkit");
      assert(universalToolkit);
      assertEqual(universalToolkit.entryTool, "soty_toolkit");
      const reinstallToolkit = manifest.automationToolkits.toolkits.find((toolkit) => toolkit.name === "windows-reinstall");
      assert(reinstallToolkit);
      assertEqual(reinstallToolkit.entryTool, "soty_reinstall");
      assert(reinstallToolkit.scripts.some((script) => script.name === "managed"));
      const durableKernel = manifest.automationToolkits.toolkits.find((toolkit) => toolkit.name === "durable-action");
      assert(durableKernel);
      assertEqual(durableKernel.entryTool, "soty_action");
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
    if (url.pathname === "/api/agent/source/cancel") {
      const body = await readBody(request);
      const payload = JSON.parse(body || "{}");
      cancels.push(String(payload.id || ""));
      json(response, 200, { ok: true, id: payload.id || "" });
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
    cancelCount: (id) => cancels.filter((item) => item === id).length,
    lastCommandWith: (needle) => [...calls].reverse().find((item) => item.includes(needle)) || ""
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
