#!/usr/bin/env node
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const releaseDir = join(root, "public", "agent");
const runtimePath = join(releaseDir, "soty-connector.mjs");
const compatibilityPath = join(releaseDir, "soty-agent.mjs");
const windowsInstallerPath = join(releaseDir, "install-windows.ps1");
const manifest = JSON.parse(await readFile(join(releaseDir, "manifest.json"), "utf8"));
const runtime = await readFile(runtimePath);
const compatibility = await readFile(compatibilityPath);
const runtimeHash = sha256(runtime);

assert.equal(manifest.schema, "soty.connector.release.v1");
assert.match(manifest.version, /^\d+\.\d+\.\d+$/u);
assert.equal(manifest.connectorUrl, "/agent/soty-connector.mjs");
assert.equal(manifest.agentUrl, manifest.connectorUrl, "legacy 0.x updater must receive the canonical runtime through agentUrl");
assert.equal(manifest.compatibilityUrl, "/agent/soty-agent.mjs");
assert.equal(manifest.sha256, runtimeHash);
assert.equal(sha256(compatibility), runtimeHash, "legacy migration asset must be byte-identical");

// This is the exact minimum shape accepted by the installed 0.4.x updater.
assert.equal(typeof manifest.version, "string");
assert.ok(manifest.version.length <= 40);
assert.equal(typeof manifest.agentUrl, "string");
assert.match(manifest.sha256, /^[a-f0-9]{64}$/u);

assert.equal(manifest.agent?.id, "opencode");
assert.equal(manifest.agent?.provider, "gonka");
assert.equal(manifest.agent?.model, "deepseek-ai/DeepSeek-V4-Flash-0731");
assert.match(manifest.agent?.runtime?.version || "", /^\d+\.\d+\.\d+$/u);
for (const [platform, release] of Object.entries(manifest.agent?.runtime?.platforms || {})) {
  assert.ok(platform);
  assert.match(release.url || "", /^https:\/\//u);
  assert.match(release.sha256 || "", /^[a-f0-9]{64}$/u);
  assert.ok(release.executable);
}
assert.ok(Object.keys(manifest.agent?.runtime?.platforms || {}).length >= 8);

const dataDir = await mkdtemp(join(tmpdir(), "soty-release-selftest-"));
try {
  const output = execFileSync(process.execPath, [runtimePath, "ctl", "release-selftest"], {
    cwd: root,
    encoding: "utf8",
    timeout: 20_000,
    windowsHide: true,
    env: {
      ...process.env,
      NODE_OPTIONS: "",
      SOTY_CONNECTOR_AUTO_UPDATE: "0",
      SOTY_AGENT_AUTO_UPDATE: "0",
      SOTY_CONNECTOR_DATA_DIR: dataDir
    }
  });
  const probe = JSON.parse(output);
  assert.equal(probe.ok, true);
  assert.equal(probe.version, manifest.version);
  assert.equal(probe.wrapperSha256, manifest.sha256);
  assert.equal(probe.agent?.id, "opencode");
  assert.equal(probe.agent?.version, manifest.agent.runtime.version);
} finally {
  await rm(dataDir, { recursive: true, force: true });
}

const runtimeText = runtime.toString("utf8");
assert.match(runtimeText, /soty\.connector\.update-pending\.v1/u);
assert.match(runtimeText, /update-preflight-failed/u);
assert.match(runtimeText, /same-version-hash-mismatch/u);
assert.match(runtimeText, /deferred-busy/u);
assert.match(runtimeText, /scheduleOpenCodeConvergence/u);
assert.match(runtimeText, /soty-agent-user-companion-now/u);
assert.match(runtimeText, /failedCode=/u);
assert.match(runtimeText, /\/api\/connectors\/gonka\/v1/u);
assert.match(runtimeText, /SOTY_CONNECTOR_MODEL_TOKEN/u);
assert.match(runtimeText, /String\(value\)\.trim\(\) === ""/u, "empty numeric settings must use their fallback");
assert.match(runtimeText, /const port = companion \? configuredPort : \(configuredPort \|\| 49_424\);/u, "managed machine updates must migrate legacy port 0 to 49424");
assert.match(runtimeText, /s-1-5-18/iu, "Windows SYSTEM detection must use the locale-independent well-known SID");
assert.doesNotMatch(runtimeText, /NT AUTHORITY/iu, "Windows SYSTEM detection must not depend on a localized account name");
assert.doesNotMatch(runtimeText, /\$args\[0\]/u, "the immediate companion task must embed its launcher path instead of losing a positional PowerShell argument");
assert.doesNotMatch(runtimeText, /SOTY_GONKA_API_KEY|JOIN_GONKA_API_KEY|gate\.joingonka\.ai/u, "Gonka credentials and upstream coordinates must stay on the server");

const windowsInstaller = await readFile(windowsInstallerPath, "utf8");
assert.match(windowsInstaller, /Remove-LegacyUserCompanion/u);
assert.match(windowsInstaller, /soty-agent-user-companion-now/u);
assert.match(windowsInstaller, /SOTY_CONNECTOR_PORT = "49424"/u);
assert.match(windowsInstaller, /ReadToEndAsync/u);
assert.doesNotMatch(windowsInstaller, /BeginOutputReadLine|DataReceivedEventHandler/u);
if (process.platform === "win32") {
  execFileSync("powershell.exe", [
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-Command",
    `[void][ScriptBlock]::Create((Get-Content -LiteralPath '${windowsInstallerPath.replace(/'/gu, "''")}' -Raw))`
  ], { timeout: 15_000, windowsHide: true });
}

process.stdout.write(`agent-release-selftest:ok:${manifest.version}:${runtimeHash}\n`);

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}
