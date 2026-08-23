import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { buildTrafficBridgeConfig, buildTrafficClientUri, createTrafficCoreRuntime } from "./agent-modules/traffic-core.mjs";

const bridge = {
  gatewayHost: "pochinit.online",
  gatewaySni: "pochinit.online",
  gatewayHttpHost: "xn--n1afe0b.online",
  gatewayPort: 443,
  gatewayPath: "/api/traffic/tunnel",
  bridgeId: "11111111-1111-4111-8111-111111111111",
  reverseTag: "soty-reverse-test",
  requireVpn: true,
  vpnInterface: "AmneziaVPN",
  vpnAddress: "10.8.1.7"
};
const config = buildTrafficBridgeConfig(bridge);
assert.equal(config.outbounds[1].sendThrough, "10.8.1.7");
assert.equal(config.outbounds[1].streamSettings, undefined);
assert.equal(config.outbounds[2].streamSettings.network, "xhttp");
assert.equal(config.outbounds[2].streamSettings.xhttpSettings.mode, "auto");
assert.equal(config.outbounds[2].streamSettings.xhttpSettings.host, "xn--n1afe0b.online");
assert.equal(config.outbounds[2].streamSettings.tlsSettings.serverName, "pochinit.online");

const profile = buildTrafficClientUri({
  gatewayHost: bridge.gatewayHost,
  gatewaySni: bridge.gatewaySni,
  gatewayHttpHost: bridge.gatewayHttpHost,
  gatewayPath: bridge.gatewayPath,
  clientId: "22222222-2222-4222-8222-222222222222",
  name: "S21"
});
assert.match(profile, /^vless:\/\//u);
assert.match(profile, /type=xhttp/u);
assert.match(profile, /alpn=h2/u);
assert.match(profile, /host=xn--n1afe0b.online/u);
assert.match(profile, /path=%2Fapi%2Ftraffic%2Ftunnel/u);

const files = new Map();
const dirs = new Set();
const archive = Buffer.from("pinned-core-archive");
const digest = createHash("sha256").update(archive).digest("hex");
const join = (...parts) => parts.join("/").replace(/\/+/gu, "/");
const runtime = createTrafficCoreRuntime({
  exists: (path) => files.has(path) || dirs.has(path),
  mkdir: async (path) => { dirs.add(path); },
  remove: async (path) => {
    for (const key of [...files.keys()]) if (key === path || key.startsWith(`${path}/`)) files.delete(key);
    for (const key of [...dirs]) if (key === path || key.startsWith(`${path}/`)) dirs.delete(key);
  },
  rename: async (from, to) => {
    for (const [key, value] of [...files.entries()]) {
      if (key === from || key.startsWith(`${from}/`)) {
        files.delete(key);
        files.set(`${to}${key.slice(from.length)}`, value);
      }
    }
    dirs.delete(from);
    dirs.add(to);
  },
  writeFile: async (path, value) => { files.set(path, value); },
  readJson: async (path) => JSON.parse(String(files.get(path))),
  join,
  download: async () => archive,
  extract: async (_path, destination) => { files.set(join(destination, "xray.exe"), Buffer.from("exe")); },
  sha256: (bytes) => createHash("sha256").update(bytes).digest("hex"),
  runFile: async (_file, args) => args[0] === "version" ? "Xray 26.7.11" : "Configuration OK",
  spawnCore: () => {
    const child = new EventEmitter();
    child.pid = 321;
    child.exitCode = null;
    child.kill = () => {
      child.exitCode = 0;
      queueMicrotask(() => child.emit("exit", 0));
      return true;
    };
    return child;
  },
  onceExit: (child) => new Promise((resolve) => child.once("exit", resolve)),
  wait: async () => undefined,
  now: () => "2026-07-12T00:00:00.000Z"
});
const spec = {
  version: "26.7.11",
  platform: "win32",
  arch: "x64",
  sha256: digest,
  executable: "xray.exe",
  urls: ["https://example.test/xray.zip"]
};
const running = await runtime.configureAndStart(spec, "root", bridge);
assert.equal(running.active, true);
assert.equal(running.version, "26.7.11");
assert.equal(running.pid, 321);
const stopped = await runtime.stop();
assert.equal(stopped.active, false);
assert.equal(stopped.phase, "stopped");

console.log("traffic core self-test passed");
