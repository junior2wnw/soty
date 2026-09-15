#!/usr/bin/env node
import { execFile, spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const agent = arg("--agent") || "http://127.0.0.1:49426";
const xray = resolve(arg("--xray") || ".codex-work/core-agent-test/traffic-core/26.7.11-win32-x64/xray.exe");
const expectedIp = arg("--expect-ip");
const port = Number.parseInt(arg("--port") || "10810", 10);
const work = await mkdtemp(join(tmpdir(), "soty-traffic-live-"));
let client = null;
let core = null;

try {
  const created = await agentPost("provision-client", { clientLabel: "Automated live smoke", platform: "test" });
  client = created.client;
  const profile = new URL(created.profile);
  const configPath = join(work, "client.json");
  await writeFile(configPath, `${JSON.stringify(clientConfig(profile, port), null, 2)}\n`, { mode: 0o600 });
  await execFileAsync(xray, ["run", "-test", "-c", configPath], { timeout: 20_000 });

  core = spawn(xray, ["run", "-c", configPath], { stdio: "ignore", windowsHide: true });
  await wait(1000);
  if (core.exitCode != null) throw new Error("live-client-core-exited");

  const direct = expectedIp || await publicIp([]);
  const tunneled = await publicIp(["--proxy", `socks5h://127.0.0.1:${port}`]);
  if (tunneled !== direct) throw new Error(`tunnel-ip-mismatch:${tunneled || "empty"}`);

  await stopCore();
  await agentPost("revoke-client", { clientId: client.id });
  client = null;

  core = spawn(xray, ["run", "-c", configPath], { stdio: "ignore", windowsHide: true });
  await wait(800);
  let revokedBlocked = false;
  try {
    await publicIp(["--max-time", "6", "--proxy", `socks5h://127.0.0.1:${port}`]);
  } catch {
    revokedBlocked = true;
  }
  if (!revokedBlocked) throw new Error("revoked-client-still-routes");
  process.stdout.write(`${JSON.stringify({ ok: true, directIp: direct, tunneledIp: tunneled, revokedBlocked })}\n`);
} finally {
  await stopCore();
  if (client?.id) await agentPost("revoke-client", { clientId: client.id }).catch(() => undefined);
  await rm(work, { recursive: true, force: true });
}

async function agentPost(action, body) {
  const response = await fetch(new URL("/operator/traffic/fabric/core", agent), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ action, ...body })
  });
  const json = await response.json();
  if (!response.ok || json?.ok === false) throw new Error(String(json?.error || `agent-http-${response.status}`));
  return json;
}

async function publicIp(extra) {
  const { stdout } = await execFileAsync("curl.exe", ["--fail", "--silent", "--show-error", "--max-time", "15", ...extra, "https://api.ipify.org"], { timeout: 20_000 });
  const ip = String(stdout || "").trim();
  if (!/^(?:\d{1,3}\.){3}\d{1,3}$/u.test(ip)) throw new Error("invalid-public-ip-response");
  return ip;
}

function clientConfig(profile, listenPort) {
  const host = profile.hostname;
  const transport = profile.searchParams.get("type") || "xhttp";
  const tlsSettings = {
    serverName: profile.searchParams.get("sni") || host,
    alpn: [profile.searchParams.get("alpn") || (transport === "ws" ? "http/1.1" : "h2")],
    fingerprint: profile.searchParams.get("fp") || "chrome"
  };
  const transportSettings = transport === "ws"
    ? { network: "ws", wsSettings: { path: profile.searchParams.get("path"), headers: { Host: profile.searchParams.get("host") || host } } }
    : { network: "xhttp", xhttpSettings: { path: profile.searchParams.get("path"), host: profile.searchParams.get("host") || host, mode: profile.searchParams.get("mode") || "auto" } };
  return {
    log: { loglevel: "warning" },
    inbounds: [{ tag: "local-socks", listen: "127.0.0.1", port: listenPort, protocol: "socks", settings: { udp: true } }],
    outbounds: [{
      tag: "soty-tunnel",
      protocol: "vless",
      settings: { address: host, port: Number(profile.port || 443), id: profile.username, encryption: "none" },
      streamSettings: {
        ...transportSettings,
        security: "tls",
        tlsSettings
      }
    }]
  };
}

async function stopCore() {
  if (!core || core.exitCode != null) return;
  const target = core;
  target.kill();
  await Promise.race([new Promise((resolveExit) => target.once("exit", resolveExit)), wait(3000)]);
  if (target.exitCode == null) target.kill("SIGKILL");
  core = null;
}

function arg(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] || "" : "";
}

function wait(ms) {
  return new Promise((resolveWait) => setTimeout(resolveWait, ms));
}
