#!/usr/bin/env node
import { execFileSync, spawn } from "node:child_process";
import { createHash, createPublicKey, randomBytes, randomUUID, verify as verifySignature } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { chmod, mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { homedir, networkInterfaces } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
// bundled connector module: ./agent-modules/traffic-fabric.mjs
const { createTrafficFabric, trafficFabricSchema } = (() => {
const trafficFabricSchema = "soty.traffic-fabric.v1";

const transportOrder = Object.freeze(["local", "quic", "tls", "relay"]);

function createTrafficFabric({
  now = () => new Date().toISOString(),
  uuid = () => { throw new Error("uuid-required"); },
  secret = () => { throw new Error("secret-required"); },
  digest = () => { throw new Error("digest-required"); }
} = {}) {
  return {
    normalizeState(value = {}) {
      value = value && typeof value === "object" ? value : {};
      const exits = Array.isArray(value.exits) ? value.exits.map(normalizeExit).filter(Boolean) : [];
      const clients = Array.isArray(value.clients) ? value.clients.map(normalizeClient).filter(Boolean) : [];
      return {
        schema: trafficFabricSchema,
        revision: trafficFabricSafeInteger(value.revision, 0),
        enabled: value.enabled === true,
        defaultPolicy: normalizePolicy(value.defaultPolicy),
        exits: uniqueBy(exits, (item) => item.id),
        clients: uniqueBy(clients, (item) => item.id),
        updatedAt: trafficFabricSafeText(value.updatedAt, 40) || now()
      };
    },

    emptyState() {
      return this.normalizeState({});
    },

    upsertExit(state, input) {
      const current = this.normalizeState(state);
      const exit = normalizeExit(input);
      if (!exit) throw new Error("invalid-exit");
      return touch(current, {
        exits: [exit, ...current.exits.filter((item) => item.id !== exit.id)]
      }, now);
    },

    removeExit(state, exitId) {
      const current = this.normalizeState(state);
      const id = trafficFabricSafeId(exitId);
      return touch(current, {
        exits: current.exits.filter((item) => item.id !== id),
        clients: current.clients.map((client) => ({
          ...client,
          allowedExitIds: client.allowedExitIds.filter((item) => item !== id),
          preferredExitId: client.preferredExitId === id ? "" : client.preferredExitId
        }))
      }, now);
    },

    issueClient(state, input = {}) {
      const current = this.normalizeState(state);
      const id = trafficFabricSafeId(input.id) || `client_${uuid()}`;
      const enrollmentSecret = secret();
      const allowedExitIds = cleanIds(input.allowedExitIds).filter((exitId) => current.exits.some((item) => item.id === exitId));
      const client = normalizeClient({
        ...input,
        id,
        allowedExitIds,
        credentialHash: digest(enrollmentSecret),
        credentialHint: enrollmentSecret.slice(0, 6),
        issuedAt: now(),
        revokedAt: ""
      });
      return {
        state: touch(current, { clients: [client, ...current.clients.filter((item) => item.id !== id)] }, now),
        client,
        secret: enrollmentSecret
      };
    },

    revokeClient(state, clientId) {
      const current = this.normalizeState(state);
      const id = trafficFabricSafeId(clientId);
      return touch(current, {
        clients: current.clients.map((item) => item.id === id ? { ...item, revokedAt: now() } : item)
      }, now);
    },

    chooseExit(state, clientId, observations = []) {
      const current = this.normalizeState(state);
      const client = current.clients.find((item) => item.id === trafficFabricSafeId(clientId) && !item.revokedAt);
      if (!client) return { ok: false, reason: "client-unavailable", failClosed: true };
      const policy = { ...current.defaultPolicy, ...client.policy };
      const candidates = current.exits
        .filter((item) => item.enabled && client.allowedExitIds.includes(item.id))
        .map((exit) => scoreExit(exit, observations.find((item) => item?.exitId === exit.id), policy))
        .filter((item) => item.eligible)
        .sort(compareCandidates);
      const selected = candidates[0] || null;
      return selected
        ? { ok: true, exit: selected.exit, transport: selected.transport, score: selected.score, failClosed: policy.failClosed }
        : { ok: false, reason: "no-healthy-exit", failClosed: policy.failClosed };
    },

    publicState(state) {
      const current = this.normalizeState(state);
      return {
        ...current,
        clients: current.clients.map(({ credentialHash, ...client }) => client)
      };
    }
  };
}

function normalizeExit(value) {
  if (!value || typeof value !== "object") return null;
  const id = trafficFabricSafeId(value.id);
  if (!id) return null;
  const transports = (Array.isArray(value.transports) ? value.transports : [])
    .map(normalizeTransport).filter(Boolean);
  return {
    id,
    label: trafficFabricSafeText(value.label, 80) || id,
    deviceId: trafficFabricSafeId(value.deviceId),
    enabled: value.enabled !== false,
    route: value.route === "vpn" ? "vpn" : "direct",
    vpnInterface: trafficFabricSafeText(value.vpnInterface, 120),
    failClosed: value.failClosed !== false,
    priority: boundedInteger(value.priority, 0, 1000, 100),
    maxClients: boundedInteger(value.maxClients, 1, 10000, 64),
    transports: uniqueBy(transports, (item) => item.id),
    updatedAt: trafficFabricSafeText(value.updatedAt, 40) || new Date().toISOString()
  };
}

function normalizeTransport(value) {
  if (!value || typeof value !== "object") return null;
  const kind = transportOrder.includes(value.kind) ? value.kind : "";
  const id = trafficFabricSafeId(value.id) || (kind ? `${kind}-${trafficFabricSafeId(value.endpoint) || "default"}` : "");
  if (!kind || !id) return null;
  return {
    id,
    kind,
    endpoint: trafficFabricSafeText(value.endpoint, 500),
    enabled: value.enabled !== false,
    priority: boundedInteger(value.priority, 0, 1000, transportOrder.indexOf(kind) * 100),
    serverName: trafficFabricSafeText(value.serverName, 253),
    pinnedKey: trafficFabricSafeText(value.pinnedKey, 200)
  };
}

function normalizeClient(value) {
  if (!value || typeof value !== "object") return null;
  const id = trafficFabricSafeId(value.id);
  if (!id) return null;
  return {
    id,
    label: trafficFabricSafeText(value.label, 80) || id,
    platform: ["android", "ios", "windows", "macos", "linux", "other"].includes(value.platform) ? value.platform : "other",
    allowedExitIds: cleanIds(value.allowedExitIds),
    preferredExitId: trafficFabricSafeId(value.preferredExitId),
    policy: normalizePolicy(value.policy),
    credentialHash: trafficFabricSafeText(value.credentialHash, 128),
    credentialHint: trafficFabricSafeText(value.credentialHint, 12),
    issuedAt: trafficFabricSafeText(value.issuedAt, 40),
    revokedAt: trafficFabricSafeText(value.revokedAt, 40)
  };
}

function normalizePolicy(value = {}) {
  return {
    failClosed: value?.failClosed !== false,
    requireVpn: value?.requireVpn === true,
    allowDirectFallback: value?.allowDirectFallback === true,
    strategy: ["stable", "fastest", "preferred"].includes(value?.strategy) ? value.strategy : "stable"
  };
}

function scoreExit(exit, observation = {}, policy) {
  const healthy = observation?.healthy === true;
  const verifiedRoute = observation?.verifiedRoute === exit.route;
  const vpnRequired = policy.requireVpn || (exit.route === "vpn" && exit.failClosed);
  const eligible = healthy && (!vpnRequired || (exit.route === "vpn" && verifiedRoute));
  const transports = exit.transports
    .filter((item) => item.enabled)
    .map((item) => ({ ...item, healthy: observation?.transports?.[item.id]?.healthy === true }))
    .filter((item) => item.healthy)
    .sort((a, b) => a.priority - b.priority || transportOrder.indexOf(a.kind) - transportOrder.indexOf(b.kind));
  const transport = transports[0] || null;
  const latency = boundedInteger(observation?.latencyMs, 0, 120000, 120000);
  const loss = Math.max(0, Math.min(Number(observation?.loss) || 0, 1));
  const preferred = observation?.preferred === true ? -10000 : 0;
  return {
    exit,
    transport,
    eligible: eligible && Boolean(transport),
    score: exit.priority * 1000 + latency + Math.round(loss * 100000) + preferred
  };
}

function compareCandidates(a, b) {
  return a.score - b.score || a.exit.id.localeCompare(b.exit.id);
}

function touch(state, patch, now) {
  return { ...state, ...patch, revision: state.revision + 1, updatedAt: now() };
}

function cleanIds(value) {
  return [...new Set((Array.isArray(value) ? value : []).map(trafficFabricSafeId).filter(Boolean))].slice(0, 10000);
}

function uniqueBy(items, key) {
  const seen = new Set();
  return items.filter((item) => {
    const value = key(item);
    if (seen.has(value)) return false;
    seen.add(value);
    return true;
  });
}

function trafficFabricSafeId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(text) ? text : "";
}

function trafficFabricSafeText(value, max) {
  return typeof value === "string" ? value.trim().slice(0, max) : "";
}

function trafficFabricSafeInteger(value, fallback) {
  return Number.isSafeInteger(value) ? value : fallback;
}

function boundedInteger(value, min, max, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isSafeInteger(parsed) ? Math.max(min, Math.min(parsed, max)) : fallback;
}

return { createTrafficFabric, trafficFabricSchema };
})();
// bundled connector module: ./agent-modules/traffic-core.mjs
const { buildTrafficClientUri, createTrafficCoreRuntime, normalizeBridgeSettings, trafficCoreSchema } = (() => {
const trafficCoreSchema = "soty.traffic-core.v1";

function createTrafficCoreRuntime(deps = {}) {
  const required = ["exists", "mkdir", "remove", "rename", "writeFile", "readJson", "join", "download", "extract", "sha256", "runFile", "spawnCore"];
  for (const name of required) {
    if (typeof deps[name] !== "function") throw new Error(`traffic-core dependency required: ${name}`);
  }

  let child = null;
  let phase = "stopped";
  let lastError = "";
  let active = null;

  return {
    status() {
      return {
        schema: trafficCoreSchema,
        phase,
        active: Boolean(child && child.exitCode == null),
        pid: child?.pid || 0,
        version: active?.version || "",
        platform: active?.platform || "",
        arch: active?.arch || "",
        installedAt: active?.installedAt || "",
        startedAt: active?.startedAt || "",
        lastError
      };
    },

    async install(spec, rootDir) {
      const clean = normalizeCoreSpec(spec);
      if (!clean) throw new Error("unsupported-traffic-core");
      const installDir = deps.join(rootDir, `${clean.version}-${clean.platform}-${clean.arch}`);
      const receiptPath = deps.join(installDir, "receipt.json");
      const cached = await deps.readJson(receiptPath).catch(() => null);
      const executable = deps.join(installDir, clean.executable);
      if (deps.exists(executable) && cached?.archiveSha256 === clean.sha256 && cached?.version === clean.version) {
        active = { ...cached, executable, installDir };
        return active;
      }

      phase = "installing";
      lastError = "";
      await deps.mkdir(rootDir);
      const stageDir = `${installDir}.stage`;
      const archivePath = deps.join(rootDir, `${clean.version}-${clean.platform}-${clean.arch}.zip`);
      await deps.remove(stageDir);
      await deps.remove(archivePath);
      await deps.mkdir(stageDir);

      try {
        const downloaded = await downloadVerified(clean.urls, archivePath, clean.sha256, deps);
        await deps.extract(archivePath, stageDir);
        const stagedExecutable = deps.join(stageDir, clean.executable);
        if (!deps.exists(stagedExecutable)) throw new Error("traffic-core-executable-missing");
        const versionOutput = await deps.runFile(stagedExecutable, ["version"], 15_000);
        if (!String(versionOutput || "").includes(clean.version)) throw new Error("traffic-core-version-mismatch");
        const receipt = {
          schema: trafficCoreSchema,
          version: clean.version,
          platform: clean.platform,
          arch: clean.arch,
          archiveSha256: clean.sha256,
          source: downloaded.source,
          installedAt: deps.now()
        };
        await deps.writeFile(deps.join(stageDir, "receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`);
        await deps.remove(installDir);
        await deps.rename(stageDir, installDir);
        active = { ...receipt, executable, installDir };
        phase = "stopped";
        return active;
      } catch (error) {
        lastError = trafficCoreSafeError(error);
        phase = "error";
        await deps.remove(stageDir).catch(() => undefined);
        throw error;
      } finally {
        await deps.remove(archivePath).catch(() => undefined);
      }
    },

    async configureAndStart(spec, rootDir, settings) {
      const clean = normalizeBridgeSettings(settings);
      if (!clean) throw new Error("invalid-traffic-bridge-settings");
      const installed = await this.install(spec, rootDir);
      phase = "configuring";
      const configPath = deps.join(installed.installDir, "bridge.json");
      const stagedConfigPath = deps.join(installed.installDir, "bridge.stage.json");
      const config = buildTrafficBridgeConfig(clean);
      await deps.writeFile(stagedConfigPath, `${JSON.stringify(config, null, 2)}\n`);
      try {
        await deps.runFile(installed.executable, ["run", "-test", "-c", stagedConfigPath], 20_000);
        await deps.rename(stagedConfigPath, configPath, true);
      } catch (error) {
        lastError = trafficCoreSafeError(error);
        phase = "error";
        await deps.remove(stagedConfigPath).catch(() => undefined);
        throw error;
      }

      await this.stop();
      phase = "starting";
      const startedChild = deps.spawnCore(installed.executable, ["run", "-c", configPath], installed.installDir);
      child = startedChild;
      active = { ...installed, startedAt: deps.now() };
      startedChild.once("exit", (code) => {
        if (child === startedChild) {
          child = null;
          if (phase !== "stopping" && code !== 0) {
            lastError = `traffic-core-exited:${code ?? "signal"}`;
            phase = "error";
          } else {
            phase = "stopped";
          }
        }
      });
      await deps.wait(800);
      if (!child || child.exitCode != null) {
        throw new Error(lastError || "traffic-core-start-failed");
      }
      phase = "running";
      return this.status();
    },

    async stop() {
      if (!child || child.exitCode != null) {
        child = null;
        phase = "stopped";
        return this.status();
      }
      phase = "stopping";
      const target = child;
      target.kill();
      await Promise.race([deps.onceExit(target), deps.wait(5_000)]);
      if (target.exitCode == null) target.kill("SIGKILL");
      if (child === target) child = null;
      phase = "stopped";
      return this.status();
    }
  };
}

function normalizeBridgeSettings(value) {
  if (!value || typeof value !== "object") return null;
  const gatewayHost = trafficCoreSafeHost(value.gatewayHost);
  const gatewaySni = trafficCoreSafeHost(value.gatewaySni) || gatewayHost;
  const gatewayHttpHost = trafficCoreSafeHost(value.gatewayHttpHost) || gatewayHost;
  const gatewayPath = trafficCoreSafePath(value.gatewayPath);
  const bridgeId = trafficCoreSafeUuid(value.bridgeId);
  const reverseTag = trafficCoreSafeId(value.reverseTag);
  const requireVpn = value.requireVpn === true;
  const vpnInterface = trafficCoreSafeText(value.vpnInterface, 120);
  const vpnAddress = trafficCoreSafeIpv4(value.vpnAddress);
  if (!gatewayHost || !gatewayPath || !bridgeId || !reverseTag || (requireVpn && (!vpnInterface || !vpnAddress))) return null;
  return {
    gatewayHost,
    gatewaySni,
    gatewayHttpHost,
    gatewayPath,
    gatewayPort: trafficCoreBoundedPort(value.gatewayPort, 443),
    bridgeId,
    reverseTag,
    tls: value.tls !== false,
    requireVpn,
    vpnInterface,
    vpnAddress,
    fingerprint: trafficCoreSafeText(value.fingerprint, 30) || "chrome",
    freedomTag: trafficCoreSafeId(value.freedomTag) || "computer-internet",
    blockTag: trafficCoreSafeId(value.blockTag) || "unmatched-block"
  };
}

function buildTrafficBridgeConfig(settings) {
  const clean = normalizeBridgeSettings(settings);
  if (!clean) throw new Error("invalid-traffic-bridge-settings");
  return {
    log: { loglevel: "warning" },
    routing: {
      domainStrategy: "IPIfNonMatch",
      rules: [{ type: "field", inboundTag: [clean.reverseTag], outboundTag: clean.freedomTag }]
    },
    outbounds: [
      { tag: clean.blockTag, protocol: "blackhole" },
      {
        tag: clean.freedomTag,
        protocol: "freedom",
        ...(clean.requireVpn ? { sendThrough: clean.vpnAddress } : {}),
        settings: { domainStrategy: "UseIP", finalRules: [{ action: "allow", network: "tcp,udp" }] },
      },
      {
        tag: "soty-reverse-link",
        protocol: "vless",
        settings: {
          address: clean.gatewayHost,
          port: clean.gatewayPort,
          id: clean.bridgeId,
          encryption: "none",
          reverse: { tag: clean.reverseTag }
        },
        streamSettings: {
          network: "xhttp",
          security: clean.tls ? "tls" : "none",
          ...(clean.tls ? { tlsSettings: { serverName: clean.gatewaySni, alpn: ["h2"], fingerprint: clean.fingerprint } } : {}),
          xhttpSettings: { path: clean.gatewayPath, host: clean.gatewayHttpHost, mode: "auto" }
        }
      }
    ]
  };
}

function buildTrafficClientUri(value) {
  const host = trafficCoreSafeHost(value?.gatewayHost);
  const sni = trafficCoreSafeHost(value?.gatewaySni) || host;
  const httpHost = trafficCoreSafeHost(value?.gatewayHttpHost) || host;
  const path = trafficCoreSafePath(value?.gatewayPath);
  const id = trafficCoreSafeUuid(value?.clientId);
  if (!host || !path || !id) throw new Error("invalid-traffic-client-profile");
  const port = trafficCoreBoundedPort(value?.gatewayPort, 443);
  const name = trafficCoreSafeText(value?.name, 80) || "Интернет через Соты";
  const query = new URLSearchParams({
    encryption: "none",
    security: "tls",
    sni,
    fp: trafficCoreSafeText(value?.fingerprint, 30) || "chrome",
    alpn: "h2",
    type: "xhttp",
    host: httpHost,
    path,
    mode: "auto"
  });
  return `vless://${id}@${host}:${port}?${query.toString()}#${encodeURIComponent(name)}`;
}

function normalizeCoreSpec(value) {
  if (!value || typeof value !== "object") return null;
  const version = trafficCoreSafeText(value.version, 30);
  const platform = trafficCoreSafeId(value.platform);
  const arch = trafficCoreSafeId(value.arch);
  const sha256 = String(value.sha256 || "").toLowerCase();
  const executable = trafficCoreSafeRelativePath(value.executable);
  const urls = (Array.isArray(value.urls) ? value.urls : []).map((item) => String(item || "").trim()).filter((item) => /^https:\/\//u.test(item)).slice(0, 4);
  return version && platform && arch && /^[a-f0-9]{64}$/u.test(sha256) && executable && urls.length
    ? { version, platform, arch, sha256, executable, urls }
    : null;
}

async function downloadVerified(urls, destination, expectedSha256, deps) {
  let lastError = null;
  for (const source of urls) {
    try {
      const bytes = await deps.download(source, 80 * 1024 * 1024);
      if (deps.sha256(bytes) !== expectedSha256) throw new Error("traffic-core-sha256-mismatch");
      await deps.writeFile(destination, bytes);
      return { source };
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error("traffic-core-download-failed");
}

function trafficCoreSafeHost(value) {
  const text = String(value || "").trim().toLowerCase();
  return /^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(text) ? text : "";
}

function trafficCoreSafePath(value) {
  const text = String(value || "").trim();
  return /^\/[A-Za-z0-9/_-]{1,180}$/u.test(text) && !text.includes("//") ? text.replace(/\/+$/u, "") : "";
}

function trafficCoreSafeUuid(value) {
  const text = String(value || "").trim().toLowerCase();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(text) ? text : "";
}

function trafficCoreSafeIpv4(value) {
  const text = String(value || "").trim();
  const parts = text.split(".");
  return parts.length === 4 && parts.every((part) => /^\d{1,3}$/u.test(part) && Number(part) >= 0 && Number(part) <= 255) ? text : "";
}

function trafficCoreSafeId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(text) ? text : "";
}

function trafficCoreSafeRelativePath(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9][A-Za-z0-9_.\/-]{0,160}$/u.test(text) && !text.includes("..") && !text.startsWith("/") ? text : "";
}

function trafficCoreSafeText(value, max) {
  return typeof value === "string" ? value.trim().slice(0, max) : "";
}

function trafficCoreBoundedPort(value, fallback) {
  const port = Number.parseInt(String(value ?? ""), 10);
  return Number.isInteger(port) && port > 0 && port <= 65535 ? port : fallback;
}

function trafficCoreSafeError(error) {
  return String(error instanceof Error ? error.message : error || "traffic-core-error").replace(/[\r\n]+/gu, " ").slice(0, 300);
}

return { buildTrafficClientUri, createTrafficCoreRuntime, normalizeBridgeSettings, trafficCoreSchema };
})();
// bundled connector module: ./agent-modules/opencode-release.mjs
const { defaultGonkaModel, gonkaModelLimitsFor, openCodeLicenseText, openCodeReleaseFor } = (() => {
const openCodeVersion = "1.18.15";

const defaultGonkaModel = "deepseek-ai/DeepSeek-V4-Flash-0731";
const legacyDefaultGonkaModels = Object.freeze([
  "moonshotai/Kimi-K2.6"
]);

function selectGonkaModel(explicitModel, persistedModel) {
  if (explicitModel) return explicitModel;
  if (!persistedModel || legacyDefaultGonkaModels.includes(persistedModel)) return defaultGonkaModel;
  return persistedModel;
}

function gonkaModelLimitsFor(model) {
  if (model === defaultGonkaModel) return Object.freeze({ context: 380_000, output: 8_192 });
  return Object.freeze({ context: 262_144, output: 32_768 });
}

const openCodeLicenseText = `MIT License

Copyright (c) 2025 opencode

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
`;

const releaseBaseUrl = `https://github.com/anomalyco/opencode/releases/download/v${openCodeVersion}`;

const platforms = Object.freeze({
  "win32-x64": release("opencode-windows-x64-baseline.zip", "98df4ed9993406e190b9a4c937aea98d733bb047c47e93c9f0c2f90ab90c2982", "opencode.exe"),
  "win32-arm64": release("opencode-windows-arm64.zip", "7815f7a980fc4273e3fc1ada5a51e9dff17e62f5a119ab769b44e06b50c1d9da", "opencode.exe"),
  "linux-x64": release("opencode-linux-x64-baseline.tar.gz", "caab046d311f29d80085979b168995f32cd052ffb7bcffe067b14cd9679d2e38", "opencode"),
  "linux-x64-musl": release("opencode-linux-x64-baseline-musl.tar.gz", "73ae90210eb93192b64d8409b9ea70fb151b7a73ac5f49739e170066a253b88f", "opencode"),
  "linux-arm64": release("opencode-linux-arm64.tar.gz", "500611819ff88916b185649990505a9be76ad13ca5bb4b9323e5abdd39b1c6fb", "opencode"),
  "linux-arm64-musl": release("opencode-linux-arm64-musl.tar.gz", "134d46c15c184ed9d5fce7c93423b4040dcdc8547f23fc425e6a7b51e166e18a", "opencode"),
  "darwin-x64": release("opencode-darwin-x64-baseline.zip", "234e67a90a16a8fa670131b097dfb72aabc4a2cc863a7be459be0215deb18a3f", "opencode"),
  "darwin-arm64": release("opencode-darwin-arm64.zip", "bd60b57cb9fe0494a5352c807424d36d6d7853cf6dbddb97065c7ccd3c5d391c", "opencode")
});

const openCodeReleaseManifest = Object.freeze({
  schema: "soty.opencode.release.v1",
  name: "OpenCode",
  version: openCodeVersion,
  source: "https://github.com/anomalyco/opencode",
  license: "MIT",
  platforms
});

function openCodeReleaseFor(platform, arch, options = {}) {
  const key = `${platform}-${arch}${platform === "linux" && options.musl === true ? "-musl" : ""}`;
  const value = platforms[key];
  return value ? { version: openCodeVersion, ...value } : null;
}

function release(asset, sha256, executable) {
  return Object.freeze({
    asset,
    sha256,
    executable,
    url: `${releaseBaseUrl}/${asset}`
  });
}

return { defaultGonkaModel, gonkaModelLimitsFor, openCodeLicenseText, openCodeReleaseFor };
})();
// bundled connector module: ./agent-modules/spreadex-ml.mjs
const { createSpreadExMlIntegration, normalizeSpreadExBaseUrl, spreadExMlSchema, spreadExOriginAllowed } = (() => {
const spreadExMlSchema = "soty.spreadex-ml.v1";
const spreadExMlReleaseSchema = "soty.spreadex-ml.release.v1";

const profiles = new Set(["careful", "balanced", "opportunity"]);
const modes = new Set(["off", "assistant", "gate"]);
const requiredDeviceScopes = ["heartbeat", "predictions", "settings:read"];

function createSpreadExMlIntegration(deps, options = {}) {
  const required = [
    "exists", "mkdir", "remove", "rename", "writeFile", "readJson", "chmod", "join", "download", "extract",
    "sha256", "verifyRelease", "runFile", "createWorker", "request", "saveState", "saveSecrets", "now", "randomId"
  ];
  for (const name of required) {
    if (typeof deps[name] !== "function") throw new Error(`spreadex-ml dependency required: ${name}`);
  }

  const platformKey = cleanPlatformKey(options.platformKey);
  const rootDir = String(options.rootDir || "");
  const baseUrl = normalizeSpreadExBaseUrl(options.baseUrl);
  const manifestUrl = normalizeManifestUrl(options.manifestUrl);
  if (!rootDir || !platformKey || !baseUrl || !manifestUrl) throw new Error("invalid-spreadex-ml-options");

  let state = normalizeSpreadExState(options.state);
  let secrets = normalizeSpreadExSecrets(options.secrets);
  let active = null;
  let phase = "unavailable";
  let lastError = "component-not-installed";
  let pairedAt = secrets.deviceToken ? state.pairedAt : "";
  let lastHeartbeatAt = "";
  let connection = "disconnected";
  let stopped = false;
  let heartbeatTimer = null;
  let socketTimer = null;
  let socket = null;
  let socketFailures = 0;
  let worker = null;
  let workerPhase = "stopped";
  let allowedModels = [];
  let taskProcessing = null;
  const queuedTasks = [];
  const completedTasks = new Map();
  const pendingPredictions = new Map();
  let updatePromise = null;
  let pairPromise = null;

  return {
    async initialize() {
      await deps.mkdir(rootDir);
      const recovered = await recoverActive();
      if (!recovered) {
        phase = "unavailable";
        lastError = "component-not-installed";
      }
      return this.status();
    },

    status() {
      const componentAvailable = Boolean(active && phase === "ready");
      return {
        ok: true,
        schema: spreadExMlSchema,
        paired: Boolean(secrets.deviceToken),
        pairedAt: secrets.deviceToken ? pairedAt : "",
        deviceId: secrets.deviceToken ? state.deviceId : "",
        scopes: secrets.deviceToken ? state.scopes : [],
        baseOrigin: new URL(baseUrl).origin,
        connection,
        lastHeartbeatAt,
        worker: workerPhase,
        modelAllowed: activeAllowed(),
        allowedModels,
        settings: publicSettings(state.settings),
        component: {
          available: componentAvailable,
          phase,
          version: active?.version || "",
          modelVersion: active?.modelVersion || "",
          featureSchema: active?.featureSchema || "",
          platform: active?.platform || platformKey,
          installedAt: active?.installedAt || "",
          lastError
        },
        ready: Boolean(secrets.deviceToken && state.settings.mode !== "off" && componentAvailable && activeAllowed())
      };
    },

    settings() {
      return { ok: true, schema: spreadExMlSchema, settings: publicSettings(state.settings) };
    },

    async updateSettings(value, source = "local") {
      const next = normalizeSpreadExSettings(value, state.settings);
      next.device_id = state.deviceId;
      const requestedRevision = safeRevision(value?.revision);
      if (source === "remote" && requestedRevision && requestedRevision < state.settings.revision) return this.settings();
      next.revision = requestedRevision || state.settings.revision + 1;
      state = { ...state, settings: next };
      await deps.saveState(state);
      return this.settings();
    },

    async pair(pairCode) {
      if (pairPromise) return await pairPromise;
      pairPromise = performPair(pairCode).finally(() => { pairPromise = null; });
      return await pairPromise;
    },

    async unpair() {
      closeSocket();
      secrets = {};
      state = { ...state, deviceId: "", pairedAt: "", websocketUrl: "", scopes: [] };
      allowedModels = [];
      pairedAt = "";
      connection = "disconnected";
      lastHeartbeatAt = "";
      await deps.saveSecrets(secrets);
      await deps.saveState(state);
      return this.status();
    },

    async syncRelease(release) {
      if (updatePromise) return await updatePromise;
      updatePromise = installRelease(release).finally(() => { updatePromise = null; });
      return await updatePromise;
    },

    async predict(value) {
      if (!active || phase !== "ready") throw new Error("spreadex-ml-unavailable");
      if (state.settings.mode === "off") throw new Error("spreadex-ml-disabled");
      if (!activeAllowed()) throw new Error("spreadex-ml-model-not-allowed");
      const input = normalizePredictionInput(value);
      if (!input) throw new Error("invalid-prediction-input");
      await requestWorker("observe", {
        request: { request_id: input.requestId, observed_at_ms: Date.now() },
        route: input.features
      }, 3_000);
      const response = await requestWorker("predict_batch", { requests: [{ request_id: input.requestId }] }, 5_000);
      const parsed = workerPredictions(response).find((item) => item.requestId === input.requestId || item.request_id === input.requestId);
      if (!parsed || parsed.ok === false || !Number.isFinite(parsed.probability) || parsed.probability < 0 || parsed.probability > 1) {
        throw new Error("spreadex-ml-invalid-prediction");
      }
      return {
        ok: true,
        schema: spreadExMlSchema,
        requestId: input.requestId,
        probability: parsed.probability,
        decision: cleanDecision(parsed.decision),
        reasons: cleanReasons(parsed.reasons),
        modelVersion: active.modelVersion,
        featureSchema: active.featureSchema,
        expiresAtMs: safeExpiresAt(parsed.expiresAtMs),
        outOfDistribution: parsed.outOfDistribution === true
      };
    },

    async processTasks(value) {
      return await processPredictionTasks(Array.isArray(value) ? value : []);
    },

    start() {
      stopped = false;
      scheduleHeartbeat(0);
      scheduleSocket(0);
    },

    stop() {
      stopped = true;
      if (heartbeatTimer) deps.clearTimer?.(heartbeatTimer);
      if (socketTimer) deps.clearTimer?.(socketTimer);
      heartbeatTimer = null;
      socketTimer = null;
      closeSocket();
      stopWorker();
    },

    redact(value) {
      let text = String(value || "");
      if (secrets.deviceToken && secrets.deviceToken.length >= 8) text = text.replaceAll(secrets.deviceToken, "<redacted>");
      return text;
    }
  };

  async function performPair(rawCode) {
    const pairCode = cleanPairCode(rawCode);
    if (!pairCode) throw new Error("invalid-pair-code");
    const result = await deps.request(new URL("/api/ml/agent/enroll", baseUrl).toString(), {
      method: "POST",
      body: {
        code: pairCode,
        name: options.deviceNick,
        device_name: options.deviceNick,
        agent_version: options.runtimeVersion,
        device_id: state.deviceId || options.deviceId,
        device_nick: options.deviceNick,
        platform: platformKey,
        runtime_version: options.runtimeVersion,
        capabilities: ["lightgbm", "settings", "health", "predictions"]
      },
      timeoutMs: 20_000
    });
    const deviceToken = cleanDeviceToken(result?.device_token || result?.token);
    const nextDeviceId = cleanId(result?.device?.id || result?.device_id || state.deviceId || options.deviceId, 180);
    const scopes = cleanScopes(result?.scopes);
    if (!result?.ok || !deviceToken || !nextDeviceId || !exactScopes(scopes, requiredDeviceScopes)) throw new Error("spreadex-pair-rejected");
    const nextPairedAt = deps.now();
    const nextSecrets = { deviceToken };
    const nextState = {
      ...state,
      deviceId: nextDeviceId,
      scopes,
      pairedAt: nextPairedAt,
      websocketUrl: normalizeWebSocketUrl(result.websocket_url, baseUrl),
      settings: normalizeSpreadExSettings({ ...(result.settings || state.settings), device_id: nextDeviceId }, state.settings)
    };
    await deps.saveSecrets(nextSecrets);
    try {
      await deps.saveState(nextState);
    } catch (error) {
      await deps.saveSecrets(secrets).catch(() => undefined);
      throw error;
    }
    secrets = nextSecrets;
    state = nextState;
    allowedModels = normalizeAllowedModels(result.allowed_models);
    pairedAt = nextPairedAt;
    scheduleHeartbeat(0);
    scheduleSocket(0);
    return { ok: true, schema: spreadExMlSchema, status: publicPairStatus() };
  }

  async function installRelease(rawRelease) {
    const release = normalizeSpreadExRelease(rawRelease, platformKey, manifestUrl);
    if (!release) {
      if (!active) {
        phase = "unavailable";
        lastError = rawRelease?.available === false ? "component-not-published" : "component-release-invalid";
      }
      return { ok: false, schema: spreadExMlSchema, error: lastError };
    }
    if (!deps.verifyRelease(release.signedPayload, release.signature)) {
      if (!active) phase = "unavailable";
      lastError = "component-signature-invalid";
      return { ok: false, schema: spreadExMlSchema, error: lastError };
    }
    if (active?.archiveSha256 === release.sha256 && active?.version === release.version) return { ok: true, schema: spreadExMlSchema, component: publicComponent() };

    phase = "installing";
    lastError = "";
    const releasesDir = deps.join(rootDir, "releases");
    const releaseId = `${release.version}-${release.sha256.slice(0, 12)}-${platformKey}`;
    const installDir = deps.join(releasesDir, releaseId);
    const stageDir = `${installDir}.stage-${deps.randomId()}`;
    const displacedDir = `${installDir}.rollback-${deps.randomId()}`;
    const archivePath = deps.join(rootDir, `${releaseId}.${deps.randomId()}${release.archive === "zip" ? ".zip" : ".tar.gz"}`);
    let displaced = false;
    const previous = active;
    try {
      await deps.mkdir(releasesDir);
      await deps.remove(stageDir);
      await deps.mkdir(stageDir);
      const bytes = await deps.download(release.url, release.maxBytes);
      if (deps.sha256(bytes) !== release.sha256) throw new Error("component-sha256-mismatch");
      await deps.writeFile(archivePath, bytes);
      await deps.extract(archivePath, stageDir, release.archive);
      const executable = deps.join(stageDir, release.executable);
      if (!deps.exists(executable)) throw new Error("component-executable-missing");
      await deps.chmod(executable, 0o755).catch(() => undefined);
      const selfTest = parseWorkerJson(await deps.runFile(executable, release.selfTestArgs, 20_000, undefined, stageDir));
      if (!validSelfTest(selfTest, release)) throw new Error("component-self-test-failed");
      const receipt = {
        schema: spreadExMlSchema,
        version: release.version,
        modelVersion: release.modelVersion,
        featureSchema: release.featureSchema,
        platform: platformKey,
        archiveSha256: release.sha256,
        executable: release.executable,
        selfTestArgs: release.selfTestArgs,
        workerArgs: release.workerArgs,
        predictArgs: release.predictArgs,
        installedAt: deps.now()
      };
      await deps.writeFile(deps.join(stageDir, "receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`);
      if (deps.exists(installDir)) {
        await deps.rename(installDir, displacedDir);
        displaced = true;
      }
      await deps.rename(stageDir, installDir);
      const activated = { ...receipt, executable: deps.join(installDir, release.executable), installDir };
      await writeActiveReceipt(activated, previous);
      active = activated;
      stopWorker();
      phase = "ready";
      lastError = "";
      if (displaced) await deps.remove(displacedDir).catch(() => undefined);
      return { ok: true, schema: spreadExMlSchema, component: publicComponent() };
    } catch (error) {
      lastError = cleanError(error);
      phase = active ? "ready" : "unavailable";
      if (displaced && !deps.exists(installDir) && deps.exists(displacedDir)) await deps.rename(displacedDir, installDir).catch(() => undefined);
      return { ok: false, schema: spreadExMlSchema, error: lastError, rolledBack: Boolean(previous) };
    } finally {
      await deps.remove(stageDir).catch(() => undefined);
      await deps.remove(displacedDir).catch(() => undefined);
      await deps.remove(archivePath).catch(() => undefined);
    }
  }

  async function recoverActive() {
    const current = await deps.readJson(deps.join(rootDir, "active.json")).catch(() => null);
    const recovered = await validateReceipt(current);
    if (recovered) {
      active = recovered;
      phase = "ready";
      lastError = "";
      return true;
    }
    const previous = await deps.readJson(deps.join(rootDir, "previous.json")).catch(() => null);
    const rollback = await validateReceipt(previous);
    if (!rollback) return false;
    await writeActiveReceipt(rollback, null);
    active = rollback;
    phase = "ready";
    lastError = "";
    return true;
  }

  async function validateReceipt(value) {
    if (!validReceipt(value, platformKey)) return null;
    const installDir = deps.join(rootDir, "releases", value.releaseId || `${value.version}-${value.archiveSha256.slice(0, 12)}-${platformKey}`);
    const executable = deps.join(installDir, value.executable);
    if (!deps.exists(executable)) return null;
    const result = parseWorkerJson(await deps.runFile(executable, cleanArgs(value.selfTestArgs, ["--self-test"]), 20_000, undefined, installDir).catch(() => null));
    if (!result?.ok || result.featureSchema !== value.featureSchema || result.modelVersion !== value.modelVersion) return null;
    return { ...value, executable, installDir };
  }

  async function writeActiveReceipt(next, previous) {
    const releaseId = next.installDir ? String(next.installDir).split(/[\\/]/u).pop() : next.releaseId;
    const receipt = { ...next, releaseId, executable: String(next.executable).split(/[\\/]/u).pop(), installDir: undefined };
    if (previous) {
      const previousReleaseId = String(previous.installDir || "").split(/[\\/]/u).pop() || previous.releaseId;
      const previousReceipt = { ...previous, releaseId: previousReleaseId, executable: String(previous.executable).split(/[\\/]/u).pop(), installDir: undefined };
      await atomicJson(deps.join(rootDir, "previous.json"), previousReceipt);
    }
    await atomicJson(deps.join(rootDir, "active.json"), receipt);
  }

  async function atomicJson(path, value) {
    const next = `${path}.${deps.randomId()}.next`;
    await deps.writeFile(next, `${JSON.stringify(value, null, 2)}\n`);
    await deps.rename(next, path, true);
  }

  function scheduleHeartbeat(delay = 30_000) {
    if (stopped || !secrets.deviceToken || typeof deps.setTimer !== "function") return;
    if (heartbeatTimer) deps.clearTimer?.(heartbeatTimer);
    heartbeatTimer = deps.setTimer(() => void heartbeat(), delay);
  }

  async function heartbeat() {
    heartbeatTimer = null;
    if (stopped || !secrets.deviceToken) return;
    try {
      const status = publicStatusForRemote();
      const result = await authenticatedRequest("/api/ml/agent/heartbeat", { method: "POST", body: status, timeoutMs: 15_000 });
      if (!result?.ok) throw new Error("spreadex-heartbeat-rejected");
      lastHeartbeatAt = deps.now();
      connection = socket ? "connected" : "heartbeat";
      if (result.settings) await applyRemoteSettings(result.settings);
      allowedModels = normalizeAllowedModels(result.allowed_models);
      if (Array.isArray(result.prediction_tasks) && result.prediction_tasks.length) void processPredictionTasks(result.prediction_tasks).catch(() => undefined);
    } catch {
      connection = socket ? "connected" : "disconnected";
    } finally {
      scheduleHeartbeat(2_000);
    }
  }

  function scheduleSocket(delay) {
    if (stopped || !secrets.deviceToken || !state.websocketUrl || typeof deps.createWebSocket !== "function" || typeof deps.setTimer !== "function") return;
    if (socket || socketTimer) return;
    socketTimer = deps.setTimer(() => {
      socketTimer = null;
      openSocket();
    }, delay);
  }

  function openSocket() {
    if (stopped || socket || !secrets.deviceToken || !state.websocketUrl) return;
    let candidate;
    try { candidate = deps.createWebSocket(state.websocketUrl); }
    catch { scheduleSocket(backoff(++socketFailures)); return; }
    socket = candidate;
    connection = "connecting";
    candidate.addEventListener("open", () => {
      if (socket !== candidate) return;
      socketFailures = 0;
      connection = "connected";
      candidate.send(JSON.stringify({ type: "authenticate", token: secrets.deviceToken, device_id: state.deviceId }));
    });
    candidate.addEventListener("message", (event) => void handleSocketMessage(String(event.data || "")));
    candidate.addEventListener("close", () => socketClosed(candidate));
    candidate.addEventListener("error", () => socketClosed(candidate));
  }

  function socketClosed(candidate) {
    if (socket !== candidate) return;
    try { candidate.close(); } catch { /* Already closed. */ }
    socket = null;
    connection = "disconnected";
    scheduleSocket(backoff(++socketFailures));
  }

  function closeSocket() {
    const current = socket;
    socket = null;
    if (current) try { current.close(); } catch { /* Already closed. */ }
  }

  async function handleSocketMessage(text) {
    const message = parseJson(text);
    if (!message || typeof message !== "object") return;
    if (message.type === "settings" && message.settings) {
      await applyRemoteSettings(message.settings).catch(() => undefined);
      return;
    }
    if (message.type === "ping") {
      if (socket) socket.send(JSON.stringify({ type: "pong", at: deps.now() }));
      return;
    }
    if (message.type !== "predict") return;
    void processPredictionTasks([message]).catch(() => undefined);
  }

  async function processPredictionTasks(rawTasks) {
    queuedTasks.push(...rawTasks.slice(0, 32));
    if (taskProcessing) return await taskProcessing;
    taskProcessing = (async () => {
      while (queuedTasks.length) await processPredictionTaskBatch(queuedTasks.splice(0, 32));
    })().finally(() => { taskProcessing = null; });
    return await taskProcessing;
  }

  async function processPredictionTaskBatch(rawTasks) {
    pruneTaskCaches();
    const tasks = rawTasks.map(normalizeRemoteTask).filter((task) => task && (!task.targetDeviceId || task.targetDeviceId === state.deviceId)).slice(0, 32);
    const nowMs = Date.now();
    const fresh = tasks.filter((task) => !completedTasks.has(task.key));
    if (!fresh.length) return;
    const isCurrent = (task) => task.expiresAtMs > nowMs && task.observedAtMs + (task.maxAgeMs || state.settings.max_prediction_age_ms) > nowMs;
    const needScoring = fresh.filter((task) => !pendingPredictions.has(task.key) && isCurrent(task));
    for (const task of fresh.filter((item) => !pendingPredictions.has(item.key) && !isCurrent(item))) {
      pendingPredictions.set(task.key, failedPrediction(task, task.expiresAtMs <= nowMs ? "prediction-task-expired" : "prediction-snapshot-stale"));
    }

    if (needScoring.length) {
      if (!active || phase !== "ready" || state.settings.mode === "off" || !activeAllowed()) {
        const error = !active || phase !== "ready"
          ? "spreadex-ml-unavailable"
          : state.settings.mode === "off" ? "spreadex-ml-disabled" : "model-not-allowed";
        for (const task of needScoring) pendingPredictions.set(task.key, failedPrediction(task, error));
      } else {
        try {
          await Promise.all(needScoring.map((task) => requestWorker("observe", {
            request: {
              request_id: task.requestId,
              route_id: task.routeId,
              snapshot_id: task.snapshotId,
              observed_at_ms: task.observedAtMs,
              expires_at_ms: task.expiresAtMs,
              max_age_ms: task.maxAgeMs || state.settings.max_prediction_age_ms,
              notional_usd: task.notionalUsd,
              profile: task.profile,
              min_probability: task.minProbability
            },
            route: task.snapshot
          }, 5_000)));
          const response = await requestWorker("predict_batch", {
            requests: needScoring.map((task) => ({
              request_id: task.requestId,
              route_id: task.routeId,
              snapshot_id: task.snapshotId,
              profile: task.profile,
              min_probability: task.minProbability,
              max_age_ms: task.maxAgeMs || state.settings.max_prediction_age_ms,
              notional_usd: task.notionalUsd,
              expires_at_ms: task.expiresAtMs
            }))
          }, 8_000);
          const byRequest = new Map(workerPredictions(response).map((item) => [cleanId(item.request_id || item.requestId, 160), item]));
          for (const task of needScoring) {
            const prediction = byRequest.get(task.requestId);
            pendingPredictions.set(task.key, validRemotePrediction(prediction, task)
              ? successfulPrediction(task, prediction, active)
              : failedPrediction(task, "spreadex-ml-invalid-prediction"));
          }
        } catch (error) {
          for (const task of needScoring) pendingPredictions.set(task.key, failedPrediction(task, cleanError(error)));
        }
      }
    }

    const predictions = fresh.map((task) => pendingPredictions.get(task.key)).filter(Boolean);
    if (!predictions.length) return;
    const result = await authenticatedRequest("/api/ml/agent/predictions", {
      method: "POST",
      body: { schema: "spreadex.ml.v1", predictions },
      timeoutMs: 15_000
    });
    if (!result?.ok) throw new Error("spreadex-predictions-rejected");
    for (const task of fresh) {
      if (!pendingPredictions.has(task.key)) continue;
      completedTasks.set(task.key, Math.max(Date.now() + 60_000, task.expiresAtMs + 60_000));
      pendingPredictions.delete(task.key);
    }
  }

  async function requestWorker(method, params, timeoutMs) {
    if (!active || phase !== "ready") throw new Error("spreadex-ml-unavailable");
    if (!worker) {
      workerPhase = "starting";
      try {
        worker = deps.createWorker(active.executable, active.workerArgs || ["--jsonl"], active.installDir);
        workerPhase = "running";
      } catch (error) {
        workerPhase = "error";
        throw error;
      }
    }
    try {
      return await worker.request(method, params, timeoutMs);
    } catch (error) {
      stopWorker();
      workerPhase = "error";
      throw new Error(cleanError(error) || "spreadex-ml-worker-failed");
    }
  }

  function stopWorker() {
    const current = worker;
    worker = null;
    if (current) try { current.stop(); } catch { /* Already stopped. */ }
    if (workerPhase !== "error") workerPhase = "stopped";
  }

  function pruneTaskCaches() {
    const now = Date.now();
    for (const [key, expiresAt] of completedTasks) if (expiresAt <= now) completedTasks.delete(key);
    for (const [key, value] of pendingPredictions) if (Number(value?.expires_at_ms || 0) + 60_000 <= now) pendingPredictions.delete(key);
  }

  async function authenticatedRequest(pathname, request) {
    if (!secrets.deviceToken) throw new Error("spreadex-not-paired");
    return await deps.request(new URL(pathname, baseUrl).toString(), {
      ...request,
      headers: { Authorization: `Bearer ${secrets.deviceToken}` }
    });
  }

  async function applyRemoteSettings(value) {
    const revision = safeRevision(value?.revision);
    if (revision && revision <= state.settings.revision) return;
    const next = normalizeSpreadExSettings(value, state.settings);
    next.device_id = state.deviceId;
    next.revision = revision || state.settings.revision + 1;
    state = { ...state, settings: next };
    await deps.saveState(state);
  }

  function publicStatusForRemote() {
    const status = publicComponent();
    return {
      schema: spreadExMlSchema,
      device_id: state.deviceId,
      runtime_version: options.runtimeVersion,
      platform: platformKey,
      settings_revision: state.settings.revision,
      component: status
    };
  }

  function activeAllowed() {
    return Boolean(active && allowedModels.some((item) => item.model_version === active.modelVersion && item.feature_schema === active.featureSchema));
  }

  function publicPairStatus() {
    return { paired: Boolean(secrets.deviceToken), pairedAt, deviceId: state.deviceId, scopes: state.scopes, baseOrigin: new URL(baseUrl).origin };
  }

  function publicComponent() {
    return { available: Boolean(active && phase === "ready"), phase, version: active?.version || "", modelVersion: active?.modelVersion || "", featureSchema: active?.featureSchema || "", lastError };
  }
}

function normalizeSpreadExBaseUrl(value) {
  try {
    const url = new URL(String(value || ""));
    const local = ["localhost", "127.0.0.1", "::1", "[::1]"].includes(url.hostname);
    if (url.protocol !== "https:" && !(local && url.protocol === "http:")) return "";
    url.username = "";
    url.password = "";
    url.search = "";
    url.hash = "";
    return url.toString().replace(/\/+$/u, "");
  } catch { return ""; }
}

function spreadExOriginAllowed(origin, spreadExBaseUrl, sotyOrigins = []) {
  if (!origin) return true;
  let actual;
  try { actual = new URL(origin).origin; } catch { return false; }
  if (["http://localhost", "http://127.0.0.1", "http://[::1]"].includes(actual)) return true;
  const allowed = [spreadExBaseUrl, ...sotyOrigins].map((item) => {
    try { return new URL(item).origin; } catch { return ""; }
  }).filter(Boolean);
  return allowed.includes(actual);
}

function normalizeSpreadExSettings(value, fallback = {}) {
  const base = publicSettings(fallback);
  const requestedMode = value?.mode === "advisory" ? "assistant" : value?.mode;
  const requestedProfile = value?.profile === "cautious" ? "careful" : value?.profile;
  const mode = modes.has(requestedMode) ? requestedMode : base.mode;
  const profile = profiles.has(requestedProfile) ? requestedProfile : base.profile;
  const minProbability = boundedNumber(value?.min_probability ?? value?.minimumConfidence ?? value?.minimum_confidence, 0.5, 0.99, base.min_probability);
  const effectiveMinProbability = boundedNumber(value?.effective_min_probability, 0.5, 0.99, minProbability);
  const maxPredictionAgeMs = boundedInteger(value?.max_prediction_age_ms, 500, 60_000, base.max_prediction_age_ms);
  const deviceId = cleanId(value?.device_id || value?.deviceId, 180) || base.device_id;
  return {
    mode,
    profile,
    min_probability: minProbability,
    effective_min_probability: effectiveMinProbability,
    max_prediction_age_ms: maxPredictionAgeMs,
    device_id: deviceId,
    revision: safeRevision(value?.revision) || base.revision
  };
}

function canonicalSpreadExRelease(value) {
  return JSON.stringify(sortObject(stripSignature(value)));
}

function normalizeSpreadExRelease(value, platformKey, manifestUrl) {
  if (!value || value.schema !== spreadExMlReleaseSchema || value.available === false) return null;
  const version = cleanVersion(value.version);
  const modelVersion = cleanId(value.modelVersion, 160);
  const featureSchema = cleanId(value.featureSchema, 160);
  const signature = cleanSignature(value.signature);
  const platform = value.platforms?.[platformKey];
  const sha256 = String(platform?.sha256 || "").toLowerCase();
  const executable = cleanRelativePath(platform?.executable);
  const archive = cleanArchive(platform?.archive || platform?.url);
  const selfTestArgs = cleanArgs(platform?.selfTestArgs, ["--self-test"]);
  const predictArgs = cleanArgs(platform?.predictArgs, ["--predict-json"]);
  const workerArgs = cleanArgs(platform?.workerArgs, ["--jsonl"]);
  let url = "";
  try {
    const parsed = new URL(String(platform?.url || ""), manifestUrl);
    if (parsed.protocol === "https:" || (["localhost", "127.0.0.1", "::1", "[::1]"].includes(parsed.hostname) && parsed.protocol === "http:")) url = parsed.toString();
  } catch { /* Invalid artifact URL. */ }
  if (!version || !modelVersion || !featureSchema || !signature || !platform || !url || !/^[a-f0-9]{64}$/u.test(sha256) || !executable || !archive) return null;
  return {
    version, modelVersion, featureSchema, signature, url, sha256, executable, archive, selfTestArgs, predictArgs, workerArgs,
    maxBytes: boundedInteger(platform.maxBytes, 1_024, 500 * 1024 * 1024, 250 * 1024 * 1024),
    signedPayload: canonicalSpreadExRelease(value)
  };
}

function normalizeSpreadExState(value) {
  const deviceId = cleanId(value?.deviceId, 180);
  const settings = normalizeSpreadExSettings(value?.settings);
  settings.device_id = deviceId;
  return {
    deviceId,
    scopes: cleanScopes(value?.scopes),
    pairedAt: cleanDate(value?.pairedAt),
    websocketUrl: String(value?.websocketUrl || ""),
    settings
  };
}

function normalizeSpreadExSecrets(value) {
  const deviceToken = cleanDeviceToken(value?.deviceToken);
  return deviceToken ? { deviceToken } : {};
}

function publicSettings(value) {
  const requestedMode = value?.mode === "advisory" ? "assistant" : value?.mode;
  const requestedProfile = value?.profile === "cautious" ? "careful" : value?.profile;
  const minProbability = boundedNumber(value?.min_probability ?? value?.minimumConfidence, 0.5, 0.99, 0.78);
  return {
    mode: modes.has(requestedMode) ? requestedMode : "off",
    profile: profiles.has(requestedProfile) ? requestedProfile : "balanced",
    min_probability: minProbability,
    effective_min_probability: boundedNumber(value?.effective_min_probability, 0.5, 0.99, minProbability),
    max_prediction_age_ms: boundedInteger(value?.max_prediction_age_ms, 500, 60_000, 2_500),
    device_id: cleanId(value?.device_id || value?.deviceId, 180),
    revision: safeRevision(value?.revision)
  };
}

function normalizePredictionInput(value) {
  const requestId = cleanId(value?.request_id || value?.requestId, 160);
  const features = value?.features;
  if (!requestId || !features || typeof features !== "object" || Array.isArray(features)) return null;
  const json = JSON.stringify(features);
  if (json.length > 256_000) return null;
  return { requestId, features };
}

function normalizeRemoteTask(value) {
  const requestId = cleanId(value?.request_id || value?.requestId, 160);
  const routeId = cleanId(value?.route_id || value?.routeId, 200);
  const snapshotId = cleanId(value?.snapshot_id || value?.snapshotId, 200);
  const observedAtMs = boundedInteger(value?.observed_at_ms ?? value?.observed_at ?? value?.observedAt, 1, Number.MAX_SAFE_INTEGER, 0);
  const expiresAtMs = boundedInteger(value?.expires_at_ms ?? value?.expires_at ?? value?.expiresAt, 1, Number.MAX_SAFE_INTEGER, 0);
  const requestedProfile = value?.profile === "cautious" ? "careful" : value?.profile;
  const profile = profiles.has(requestedProfile) ? requestedProfile : "balanced";
  const minProbability = boundedNumber(value?.min_probability ?? value?.minProbability, 0.5, 0.99, 0.78);
  const targetDeviceId = cleanId(value?.target_device_id || value?.targetDeviceId, 180);
  const maxAgeMs = boundedInteger(value?.max_age_ms, 500, 60_000, 0);
  const notionalUsd = boundedNumber(value?.notional_usd, 0.01, 1_000_000_000, 0);
  const snapshot = value?.snapshot;
  if (!requestId || !routeId || !snapshotId || !observedAtMs || !expiresAtMs || expiresAtMs <= observedAtMs || !snapshot || typeof snapshot !== "object" || Array.isArray(snapshot)) return null;
  if (JSON.stringify(snapshot).length > 256_000) return null;
  return { key: `${requestId}:${snapshotId}`, requestId, routeId, snapshotId, observedAtMs, expiresAtMs, profile, minProbability, targetDeviceId, maxAgeMs, notionalUsd, snapshot };
}

function workerPredictions(value) {
  const payload = value?.result && typeof value.result === "object" ? value.result : value;
  if (Array.isArray(payload?.predictions)) return payload.predictions.filter((item) => item && typeof item === "object");
  if (payload && typeof payload === "object" && Number.isFinite(payload.probability)) return [payload];
  return [];
}

function validRemotePrediction(value, task) {
  if (!value || value.ok === false) return false;
  const requestId = cleanId(value.request_id || value.requestId, 160);
  const probability = Number(value.probability);
  return requestId === task.requestId && Number.isFinite(probability) && probability >= 0 && probability <= 1;
}

function successfulPrediction(task, value, component) {
  const probability = Number(value.probability);
  return {
    request_id: task.requestId,
    route_id: task.routeId,
    snapshot_id: task.snapshotId,
    ok: true,
    probability,
    decision: cleanDecision(value.decision || (probability >= task.minProbability ? "allow" : "deny")),
    reasons: cleanReasons(value.reasons),
    model_version: component.modelVersion,
    feature_schema: component.featureSchema,
    scored_at_ms: Date.now(),
    expires_at_ms: Math.min(task.expiresAtMs, safeExpiresAt(value.expires_at_ms || value.expiresAtMs)),
    out_of_distribution: value.out_of_distribution === true || value.outOfDistribution === true
  };
}

function failedPrediction(task, error) {
  return {
    request_id: task.requestId,
    route_id: task.routeId,
    snapshot_id: task.snapshotId,
    ok: false,
    error: cleanError(error),
    scored_at_ms: Date.now(),
    expires_at_ms: task.expiresAtMs
  };
}

function validSelfTest(result, release) {
  return result?.ok === true && result.featureSchema === release.featureSchema && result.modelVersion === release.modelVersion;
}

function validReceipt(value, platformKey) {
  return value?.schema === spreadExMlSchema && cleanVersion(value.version) && cleanId(value.modelVersion, 160) && cleanId(value.featureSchema, 160)
    && value.platform === platformKey && /^[a-f0-9]{64}$/u.test(String(value.archiveSha256 || "")) && cleanRelativePath(value.executable);
}

function parseWorkerJson(value) {
  if (value && typeof value === "object" && Number.isInteger(value.exitCode)) {
    if (value.exitCode !== 0) return null;
    value = value.stdout;
  }
  if (value && typeof value === "object") return value;
  return parseJson(String(value || "").trim().split(/\r?\n/u).filter(Boolean).at(-1));
}

function parseJson(value) {
  try { return JSON.parse(value); } catch { return null; }
}

function stripSignature(value) {
  if (Array.isArray(value)) return value.map(stripSignature);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).filter(([key]) => key !== "signature").map(([key, item]) => [key, stripSignature(item)]));
}

function sortObject(value) {
  if (Array.isArray(value)) return value.map(sortObject);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortObject(value[key])]));
}

function normalizeManifestUrl(value) {
  try {
    const url = new URL(String(value || ""));
    return url.protocol === "https:" || (["localhost", "127.0.0.1", "::1", "[::1]"].includes(url.hostname) && url.protocol === "http:") ? url.toString() : "";
  } catch { return ""; }
}

function normalizeWebSocketUrl(value, baseUrl) {
  if (!value) return "";
  try {
    const url = new URL(String(value), baseUrl);
    const base = new URL(baseUrl);
    const local = ["localhost", "127.0.0.1", "::1", "[::1]"].includes(base.hostname);
    if (url.host !== base.host || (url.protocol !== "wss:" && !(local && url.protocol === "ws:"))) return "";
    url.username = "";
    url.password = "";
    return url.toString();
  } catch { return ""; }
}

function cleanPairCode(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{8,192}$/u.test(text) ? text : "";
}

function cleanDeviceToken(value) {
  const text = String(value || "").trim();
  return /^[\x21-\x7E]{32,2048}$/u.test(text) && !/[\s"'\\]/u.test(text) ? text : "";
}

function cleanSignature(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_+/=-]{64,512}$/u.test(text) ? text : "";
}

function cleanPlatformKey(value) {
  const text = String(value || "").trim();
  return /^(?:win32|linux|darwin)-(?:x64|arm64)$/u.test(text) ? text : "";
}

function cleanVersion(value) {
  const text = String(value || "").trim();
  return /^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/u.test(text) ? text : "";
}

function cleanId(value, max) {
  const text = String(value || "").trim().slice(0, max);
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/u.test(text) ? text : "";
}

function cleanRelativePath(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9][A-Za-z0-9_.\/-]{0,200}$/u.test(text) && !text.includes("..") && !text.startsWith("/") ? text : "";
}

function cleanArchive(value) {
  const text = String(value || "").split(/[?#]/u)[0].toLowerCase();
  if (text.endsWith(".zip")) return "zip";
  if (text.endsWith(".tar.gz") || text.endsWith(".tgz")) return "tar.gz";
  return "";
}

function cleanArgs(value, fallback) {
  if (!Array.isArray(value)) return fallback;
  const args = value.map((item) => String(item || "").trim()).filter((item) => item && item.length <= 120 && !/[\r\n\0]/u.test(item)).slice(0, 12);
  return args.length ? args : fallback;
}

function cleanDecision(value) {
  if (value === "deny") return "block";
  return ["allow", "block"].includes(value) ? value : "block";
}

function cleanReasons(value) {
  return (Array.isArray(value) ? value : []).map((item) => String(item || "").replace(/[\r\n\t]+/gu, " ").trim().slice(0, 180)).filter(Boolean).slice(0, 5);
}

function cleanScopes(value) {
  return [...new Set((Array.isArray(value) ? value : []).map((item) => String(item || "").trim()).filter((item) => /^[a-z][a-z0-9:-]{0,63}$/u.test(item)))].sort();
}

function normalizeAllowedModels(value) {
  return (Array.isArray(value) ? value : []).map((item) => ({
    model_version: cleanId(item?.model_version, 160),
    feature_schema: cleanId(item?.feature_schema, 160)
  })).filter((item) => item.model_version && item.feature_schema).slice(0, 64);
}

function exactScopes(actual, expected) {
  const required = [...expected].sort();
  return actual.length === required.length && actual.every((item, index) => item === required[index]);
}

function cleanDate(value) {
  const text = String(value || "");
  return /^\d{4}-\d{2}-\d{2}T/u.test(text) && Number.isFinite(Date.parse(text)) ? new Date(text).toISOString() : "";
}

function safeRevision(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : 0;
}

function safeExpiresAt(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number > Date.now() && number <= Date.now() + 60_000 ? number : Date.now() + 2_000;
}

function boundedNumber(value, min, max, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number >= min && number <= max ? number : fallback;
}

function boundedInteger(value, min, max, fallback) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= min && number <= max ? number : fallback;
}

function cleanError(error) {
  return String(error instanceof Error ? error.message : error || "spreadex-ml-error").replace(/[\r\n]+/gu, " ").slice(0, 240);
}

function backoff(failures) {
  return Math.min(60_000, 1_000 * 2 ** Math.min(failures, 6));
}

return { createSpreadExMlIntegration, normalizeSpreadExBaseUrl, spreadExMlSchema, spreadExOriginAllowed };
})();

const connectorVersion = "1.2.11";
const connectorSchema = "soty.agent-runtime.v1";
const scriptPath = fileURLToPath(import.meta.url);
const connectorDir = resolve(env("SOTY_CONNECTOR_DATA_DIR") || dirname(scriptPath));
const configPath = join(connectorDir, "connector-config.json");
const legacyConfigPath = join(connectorDir, "agent-config.json");
const updatePreviousPath = `${scriptPath}.previous`;
const updatePendingPath = `${scriptPath}.update-pending.json`;
const releaseReceiptPath = join(connectorDir, "runtime-release.json");
const managed = flag("--managed") || env("SOTY_CONNECTOR_MANAGED", "SOTY_AGENT_MANAGED") === "1";
const scope = safeScope(arg("--scope") || env("SOTY_CONNECTOR_SCOPE", "SOTY_AGENT_SCOPE") || (managed ? "CurrentUser" : "Dev"));
const companion = env("SOTY_CONNECTOR_COMPANION", "SOTY_AGENT_COMPANION") === "1";
const configuredPort = safeInteger(arg("--port") || env("SOTY_CONNECTOR_PORT", "SOTY_AGENT_PORT"), 0, 65_535, 0);
// Machine releases before 1.2.6 persisted port 0 in the managed runner. Treat it
// as an old "automatic" value and converge to the browser-facing stable port.
const port = companion ? configuredPort : (configuredPort || 49_424);
const updateManifestUrl = arg("--update-url") || env("SOTY_CONNECTOR_UPDATE_URL", "SOTY_AGENT_UPDATE_URL") || "https://xn--n1afe0b.online/agent/manifest.json";
const spreadExBaseUrl = normalizeSpreadExBaseUrl(env("SOTY_SPREADEX_BASE_URL")) || "https://miniapp.spreadex.me";
const spreadExReleasePublicKey = env("SOTY_SPREADEX_ML_RELEASE_PUBLIC_KEY");
const autoUpdate = env("SOTY_CONNECTOR_AUTO_UPDATE", "SOTY_AGENT_AUTO_UPDATE") === "1" || (managed && env("SOTY_CONNECTOR_AUTO_UPDATE", "SOTY_AGENT_AUTO_UPDATE") !== "0");
const requestedShell = arg("--shell") || env("SOTY_CONNECTOR_SHELL", "SOTY_AGENT_SHELL");
const updateConfirmMs = safeInteger(env("SOTY_CONNECTOR_UPDATE_CONFIRM_MS"), 1_000, 60_000, 15_000);
const maxEventChars = 64_000;
const maxResultChars = 1_000_000;
const persisted = loadConfig();
const gonkaModel = defaultGonkaModel;
const gonkaModelLimits = gonkaModelLimitsFor(gonkaModel);
let linkId = safeLinkId(arg("--link-id") || arg("--relay-id") || env("SOTY_CONNECTOR_LINK_ID", "SOTY_AGENT_RELAY_ID") || persisted.linkId || persisted.relayId);
let relayBaseUrl = safeBaseUrl(env("SOTY_CONNECTOR_SERVER_URL", "SOTY_AGENT_RELAY_URL") || persisted.serverUrl || persisted.relayBaseUrl || originOf(updateManifestUrl) || "https://xn--n1afe0b.online");
let deviceId = safeId(env("SOTY_CONNECTOR_DEVICE_ID", "SOTY_AGENT_DEVICE_ID") || persisted.deviceId, 180) || `device-${randomUUID()}`;
let deviceNick = safeText(env("SOTY_CONNECTOR_DEVICE_NICK", "SOTY_AGENT_DEVICE_NICK") || persisted.deviceNick || hostLabel(), 120) || deviceId;
const installId = safeId(persisted.installId, 160) || `install-${randomUUID()}`;
const connectorToken = safeToken(persisted.connectorToken) || randomBytes(32).toString("base64url");
const connectorId = `${installId}:${scope.toLowerCase()}`;
const trafficRoot = join(connectorDir, "traffic-core");
const jobRoot = join(connectorDir, "connector-jobs");
const openCodeRoot = join(connectorDir, "opencode-runtime");
const openCodeStateRoot = join(connectorDir, "opencode-state");
const spreadExMlRoot = join(connectorDir, "spreadex-ml");
const spreadExMlSecretsPath = join(connectorDir, "spreadex-ml-secrets.json");
let shuttingDown = false;
let agentCache = { at: 0, value: null };
let openCodeInstallPromise = null;
let registrationError = "";
let lastRegisteredAt = "";
let activeJob = null;
let updateRunning = false;
let updateState = {
  lastCheckAt: "",
  lastResult: autoUpdate ? "not-checked" : "disabled",
  latestVersion: "",
  lastError: ""
};

const trafficFabric = createTrafficFabric({
  uuid: randomUUID,
  secret: () => randomBytes(32).toString("base64url"),
  digest: (value) => sha256(value)
});
let trafficFabricState = trafficFabric.normalizeState(persisted.trafficFabric);
let trafficCoreSettings = normalizeBridgeSettings(refreshTrafficVpnSettings(persisted.trafficCoreSettings));
let trafficCoreEnabled = persisted.trafficCoreEnabled === true;
let spreadExMlState = persisted.spreadexMl && typeof persisted.spreadexMl === "object" ? persisted.spreadexMl : {};
const trafficCoreRuntime = createTrafficCoreRuntime({
  exists: existsSync,
  mkdir: async (target) => await mkdir(target, { recursive: true }),
  remove: async (target) => await rm(target, { recursive: true, force: true }),
  rename: async (from, to, replace = false) => {
    if (replace) await rm(to, { force: true });
    await rename(from, to);
  },
  writeFile: async (target, value) => await writeFile(target, value),
  readJson: async (target) => JSON.parse(await readFile(target, "utf8")),
  join,
  download: downloadTrafficCoreBytes,
  extract: extractTrafficCoreArchive,
  sha256,
  runFile: async (file, args, timeoutMs) => execFileSync(file, args, { encoding: "utf8", timeout: timeoutMs, windowsHide: true }),
  spawnCore: (file, args, cwd) => spawn(file, args, { cwd, windowsHide: true, stdio: "ignore" }),
  onceExit: (child) => new Promise((resolveExit) => child.once("exit", resolveExit)),
  wait: sleep,
  now: () => new Date().toISOString()
});
const spreadExMl = createSpreadExMlIntegration({
  exists: existsSync,
  mkdir: async (target) => await mkdir(target, { recursive: true }),
  remove: async (target) => await rm(target, { recursive: true, force: true }),
  rename: async (from, to, replace = false) => {
    if (replace) await rm(to, { recursive: true, force: true });
    await rename(from, to);
  },
  writeFile: async (target, value) => await writeFile(target, value, { mode: 0o600 }),
  readJson: async (target) => JSON.parse(await readFile(target, "utf8")),
  chmod,
  join,
  download: downloadSpreadExMlBytes,
  extract: extractSpreadExMlArchive,
  sha256,
  verifyRelease: verifySpreadExMlRelease,
  runFile: async (file, args, timeoutMs, input, cwd) => await runChild(file, args, { cwd, timeoutMs, input }),
  createWorker: createSpreadExJsonlWorker,
  request: spreadExJson,
  saveState: async (value) => {
    spreadExMlState = value;
    await saveConfig();
  },
  saveSecrets: saveSpreadExMlSecrets,
  now: () => new Date().toISOString(),
  randomId: randomUUID,
  setTimer: (callback, delay) => {
    const timer = setTimeout(callback, delay);
    timer.unref?.();
    return timer;
  },
  clearTimer: clearTimeout,
  ...(typeof globalThis.WebSocket === "function" ? { createWebSocket: (url) => new globalThis.WebSocket(url) } : {})
}, {
  rootDir: spreadExMlRoot,
  baseUrl: spreadExBaseUrl,
  manifestUrl: updateManifestUrl,
  platformKey: `${process.platform}-${process.arch}`,
  runtimeVersion: connectorVersion,
  deviceId,
  deviceNick,
  state: spreadExMlState,
  secrets: loadSpreadExMlSecrets()
});

if (process.argv[2] === "ctl") {
  await runControl(process.argv.slice(3));
} else {
  await startConnector();
}

async function startConnector() {
  await saveConfig();
  await ensureManagedRunner().catch((error) => {
    updateState.lastError = `runner: ${safeError(error)}`;
  });
  await spreadExMl.initialize().catch(() => undefined);
  const server = createServer((request, response) => {
    void handleHttp(request, response).catch((error) => {
      const pathname = new URL(request.url || "/", "http://127.0.0.1").pathname;
      if (!response.headersSent) sendJson(response, 500, corsHeaders(request, pathname.startsWith("/integrations/spreadex/v1")), { ok: false, error: safeError(error) });
      else response.end();
    });
  });
  server.listen(port, "127.0.0.1", () => {
    const address = server.address();
    const actualPort = typeof address === "object" && address ? address.port : port;
    process.stdout.write(`soty-connector:${actualPort}\n`);
    scheduleUpdateConfirmation();
  });
  server.on("error", (error) => {
    process.stderr.write(`soty-connector:http:${safeError(error)}\n`);
    process.exitCode = 1;
  });

  if (trafficCoreEnabled && trafficCoreSettings) {
    void trafficCoreRuntime.configureAndStart(trafficRelease(), trafficRoot, trafficCoreSettings).catch(() => undefined);
  }
  startHeartbeat();
  scheduleOpenCodeConvergence();
  void connectorLoop();
  scheduleUpdate();
  scheduleUserCompanion();
  spreadExMl.start();

  const stop = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    activeJob?.controller.abort();
    spreadExMl.stop();
    await trafficCoreRuntime.stop().catch(() => undefined);
    server.close(() => process.exit(process.exitCode || 0));
    const timer = setTimeout(() => process.exit(process.exitCode || 0), 2_000);
    timer.unref?.();
  };
  process.on("SIGINT", () => void stop());
  process.on("SIGTERM", () => void stop());
}

async function handleHttp(request, response) {
  const url = new URL(request.url || "/", "http://127.0.0.1");
  const spreadExRoute = url.pathname.startsWith("/integrations/spreadex/v1");
  const headers = corsHeaders(request, spreadExRoute);
  const origin = String(request.headers.origin || "");
  if (!originAllowed(origin, url.pathname)) {
    sendJson(response, 403, headers, { ok: false, error: "origin-not-allowed" });
    return;
  }
  if (request.method === "OPTIONS") {
    response.writeHead(204, headers);
    response.end();
    return;
  }
  if (url.pathname === "/health" && request.method === "GET") {
    if (url.searchParams.get("update") === "1") void checkForUpdate();
    sendJson(response, 200, headers, await health());
    return;
  }
  if (url.pathname === "/agent/status" && request.method === "GET") {
    sendJson(response, 200, headers, { ok: true, schema: connectorSchema, agent: await detectAgent(true) });
    return;
  }
  if ((url.pathname === "/connector/bind" || url.pathname === "/agent/relay") && request.method === "POST") {
    await handleBind(request, response, headers, origin);
    return;
  }
  if ((url.pathname === "/integrations/spreadex/v1/status" || url.pathname === "/integrations/spreadex/v1/health") && request.method === "GET") {
    sendJson(response, 200, headers, spreadExMl.status());
    return;
  }
  if (url.pathname === "/integrations/spreadex/v1/settings" && request.method === "GET") {
    sendJson(response, 200, headers, spreadExMl.settings());
    return;
  }
  if (url.pathname === "/integrations/spreadex/v1/settings" && request.method === "POST") {
    try {
      const result = await spreadExMl.updateSettings(await readJsonBody(request, 32_000));
      sendJson(response, 200, headers, result);
    } catch (error) {
      sendJson(response, 400, headers, { ok: false, schema: spreadExMlSchema, error: safeError(error) });
    }
    return;
  }
  if (url.pathname === "/integrations/spreadex/v1/pair" && request.method === "POST") {
    try {
      const body = await readJsonBody(request, 16_000);
      const result = await spreadExMl.pair(body.code || body.pairCode || body.pair_code);
      void checkForUpdate();
      sendJson(response, 200, headers, result);
    } catch (error) {
      sendJson(response, 400, headers, { ok: false, schema: spreadExMlSchema, error: safeError(error) });
    }
    return;
  }
  if (url.pathname === "/integrations/spreadex/v1/unpair" && request.method === "POST") {
    sendJson(response, 200, headers, await spreadExMl.unpair());
    return;
  }
  if (url.pathname === "/integrations/spreadex/v1/predict" && request.method === "POST") {
    try {
      const result = await spreadExMl.predict(await readJsonBody(request, 256_000));
      sendJson(response, 200, headers, result);
    } catch (error) {
      sendJson(response, 503, headers, { ok: false, schema: spreadExMlSchema, error: safeError(error) });
    }
    return;
  }
  if (url.pathname === "/operator/traffic/fabric" && request.method === "GET") {
    sendJson(response, 200, headers, trafficStatus());
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/interfaces" && request.method === "GET") {
    sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, interfaces: trafficInterfaces() });
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/exit" && request.method === "POST") {
    await mutateTrafficFabric(request, response, headers, "exit");
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/client" && request.method === "POST") {
    await mutateTrafficFabric(request, response, headers, "client");
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/revoke" && request.method === "POST") {
    await mutateTrafficFabric(request, response, headers, "revoke");
    return;
  }
  if (url.pathname === "/operator/traffic/fabric/core" && request.method === "POST") {
    await handleTrafficCore(request, response, headers);
    return;
  }
  sendJson(response, 404, headers, { ok: false, error: "not-found" });
}

async function handleBind(request, response, headers, origin) {
  const body = await readJsonBody(request, 64_000);
  const nextLinkId = safeLinkId(body.linkId || body.relayId);
  const nextBaseUrl = safeBaseUrl(body.serverUrl || body.relayBaseUrl || origin);
  const nextDeviceId = safeId(body.deviceId || deviceId, 180);
  if (!nextLinkId || !nextBaseUrl || !nextDeviceId) {
    sendJson(response, 400, headers, { ok: false, error: "invalid-binding" });
    return;
  }
  if (origin && !sameOrigin(origin, nextBaseUrl) && !isLocalOrigin(origin)) {
    sendJson(response, 403, headers, { ok: false, error: "binding-origin-mismatch" });
    return;
  }
  linkId = nextLinkId;
  relayBaseUrl = nextBaseUrl;
  deviceId = nextDeviceId;
  deviceNick = safeText(body.deviceNick || deviceNick, 120) || nextDeviceId;
  await saveConfig();
  const registered = await registerConnector(true);
  sendJson(response, registered.ok ? 200 : 502, headers, {
    ok: registered.ok,
    schema: connectorSchema,
    deviceId,
    currentDeviceId: deviceId,
    connectorId,
    ...(registered.ok ? {} : { error: registered.error })
  });
}

async function health() {
  const agent = await detectAgent();
  return {
    ok: true,
    schema: connectorSchema,
    connector: true,
    agentRuntime: true,
    managed,
    scope,
    companion,
    autoUpdate,
    platform: `${process.platform}-${process.arch}`,
    shell: shellName(),
    version: connectorVersion,
    executionPlane: scope === "Machine" ? "system" : "user",
    system: isWindowsSystem(),
    interactiveTaskBridge: false,
    maintenance: scope === "Machine",
    relay: Boolean(linkId),
    linkId: linkId ? "configured" : "",
    deviceId,
    deviceNick,
    sourceWorker: true,
    package: {
      wrapper: { id: "soty-connector", version: connectorVersion },
      agent: { id: "opencode", targetVersion: openCodeRelease()?.version || "", installedVersion: agent.version || "" }
    },
    update: {
      enabled: autoUpdate,
      manifestUrl: updateManifestUrl,
      currentVersion: connectorVersion,
      ...updateState
    },
    integrations: { spreadex: spreadExMl.status() },
    agent,
    registration: { connected: Boolean(lastRegisteredAt && !registrationError), lastRegisteredAt, error: registrationError },
    activeJob: activeJob ? { id: activeJob.id, kind: activeJob.kind, startedAt: activeJob.startedAt } : null
  };
}

function startHeartbeat() {
  const timer = setInterval(() => void registerConnector(), 30_000);
  timer.unref?.();
  void registerConnector(true);
}

async function registerConnector(forceAgent = false) {
  if (!linkId || !relayBaseUrl) return { ok: false, error: "connector-not-bound" };
  try {
    const result = await serverJson("/api/connectors/register", {
      method: "POST",
      body: {
        linkId,
        deviceId,
        deviceNick,
        connectorId,
        version: connectorVersion,
        platform: `${process.platform}-${process.arch}`,
        scope,
        capabilities: ["agent", "command", "script", "events", "cancel", "traffic", "spreadex-ml"],
        agent: await detectAgent(forceAgent)
      }
    });
    if (!result.ok) throw new Error(result.error || "registration-rejected");
    registrationError = "";
    lastRegisteredAt = new Date().toISOString();
    return result;
  } catch (error) {
    registrationError = safeError(error);
    return { ok: false, error: registrationError };
  }
}

async function connectorLoop() {
  let failures = 0;
  while (!shuttingDown) {
    if (!linkId || !relayBaseUrl) {
      await sleep(2_000);
      continue;
    }
    if (activeJob) {
      await sleep(500);
      continue;
    }
    try {
      const result = await serverJson(`/api/connectors/poll?linkId=${encodeURIComponent(linkId)}&deviceId=${encodeURIComponent(deviceId)}&connectorId=${encodeURIComponent(connectorId)}&wait=1`, { timeoutMs: 35_000 });
      if (!result.ok) {
        if (result.error === "connector-auth-failed") await registerConnector(true);
        throw new Error(result.error || "poll-rejected");
      }
      failures = 0;
      const job = Array.isArray(result.jobs) ? result.jobs[0] : null;
      if (job) await executeJob(job);
    } catch (error) {
      failures += 1;
      registrationError = safeError(error);
      await sleep(Math.min(15_000, 500 * 2 ** Math.min(failures, 5)));
    }
  }
}

async function executeJob(job) {
  const kind = ["agent", "command", "script"].includes(job.kind) ? job.kind : "agent";
  const detected = kind === "agent" ? await detectAgent() : null;
  if (kind === "agent" && !detected?.available) {
    await finishRemoteJob(job.id, { ok: false, text: detected?.reason || "OpenCode недоступен", exitCode: 126, agentId: "opencode" });
    return;
  }
  const requestedRunAs = job.input?.runAs === "system" ? "system" : "user";
  const runtimeRunAs = scope === "Machine" ? "system" : "user";
  if (kind !== "agent" && requestedRunAs !== runtimeRunAs) {
    await finishRemoteJob(job.id, { ok: false, text: `Нужен контекст ${requestedRunAs}`, exitCode: 126 });
    return;
  }
  const controller = new AbortController();
  activeJob = { id: job.id, kind, startedAt: new Date().toISOString(), controller };
  let eventQueue = Promise.resolve();
  const emit = (event) => {
    eventQueue = eventQueue.then(() => postJobEvent(job.id, event)).catch(() => undefined);
    return eventQueue;
  };
  await emit({ type: "started", text: kind === "agent" ? "OpenCode · Gonka AI" : shellName() });
  const heartbeat = setInterval(() => void emit({ type: "heartbeat", text: "" }), 25_000);
  heartbeat.unref?.();
  const cancelWatch = watchCancellation(job.id, controller);
  try {
    const runtime = { signal: controller.signal, emit };
    const result = kind === "agent" ? await runOpenCode(job, runtime) : await runShellJob(job, runtime);
    await eventQueue;
    await finishRemoteJob(job.id, { ...result, ...(kind === "agent" ? { agentId: "opencode" } : {}) });
  } catch (error) {
    const cancelled = controller.signal.aborted;
    await eventQueue;
    await finishRemoteJob(job.id, {
      ok: false,
      text: cancelled ? "Отменено" : safeError(error),
      exitCode: cancelled ? 130 : 1,
      ...(kind === "agent" ? { agentId: "opencode" } : {})
    });
  } finally {
    clearInterval(heartbeat);
    controller.abort();
    await cancelWatch.catch(() => undefined);
    activeJob = null;
  }
}

async function watchCancellation(jobId, controller) {
  while (!controller.signal.aborted && !shuttingDown) {
    await sleep(1_000);
    try {
      const state = await serverJson(`/api/connectors/jobs/${encodeURIComponent(jobId)}`, { linkHeader: true, timeoutMs: 5_000 });
      if (!state.ok || state.job?.cancelRequested === true || ["cancelled", "failed", "succeeded"].includes(state.job?.status)) {
        controller.abort();
        return;
      }
    } catch {
      // A transient status failure must not kill the running OpenCode task.
    }
  }
}

async function postJobEvent(jobId, event) {
  return await serverJson(`/api/connectors/jobs/${encodeURIComponent(jobId)}/events`, {
    method: "POST",
    body: { linkId, deviceId, connectorId, event: cleanOutgoingEvent(event) }
  });
}

async function finishRemoteJob(jobId, result) {
  return await serverJson(`/api/connectors/jobs/${encodeURIComponent(jobId)}/result`, {
    method: "POST",
    body: { linkId, deviceId, connectorId, result }
  });
}

async function detectAgent(force = false) {
  if (!force && agentCache.value && Date.now() - agentCache.at < 30_000) return agentCache.value;
  let value;
  try {
    if (scope === "Machine" && isWindowsSystem()) {
      value = agentStatus(false, "", "Требуется пользовательская сессия");
    } else {
      const command = await resolveOpenCodeCommand(managed && autoUpdate);
      const probe = command ? await probeCommand(command, ["--version"]) : { ok: false, stdout: "", error: "OpenCode не установлен" };
      const version = probe.ok ? safeVersionText(probe.stdout) : "";
      const reason = !probe.ok ? probe.error : !linkId || !relayBaseUrl ? "Коннектор не привязан к серверу Soty" : "";
      value = agentStatus(Boolean(probe.ok && !reason), version, reason);
    }
  } catch (error) {
    value = agentStatus(false, "", safeError(error));
  }
  agentCache = { at: Date.now(), value };
  return value;
}

function agentStatus(available, version, reason) {
  return {
    id: "opencode",
    name: "OpenCode",
    provider: "gonka",
    model: gonkaModel,
    available,
    version,
    reason,
    capabilities: ["chat", "sessions", "workspace", "shell", "files", "web", "events", "cancel"]
  };
}

async function runOpenCode(job, { signal, emit }) {
  const command = await resolveOpenCodeCommand(managed && autoUpdate);
  if (!command) return { ok: false, text: "OpenCode не установлен", exitCode: 126 };
  const cwd = resolveJobCwd(job.input?.cwd);
  await prepareOpenCodeState();
  const prompt = [job.input?.context, job.input?.text].filter(Boolean).join("\n\n").slice(0, 192_000);
  const args = ["--pure", "run", "--format", "json", "--model", `gonka/${gonkaModel}`, "--agent", "soty", "--auto", "--dir", cwd];
  if (job.input?.sessionId) args.push("--session", job.input.sessionId);
  let sessionId = "";
  let lastMessage = "";
  let lineBuffer = "";
  const result = await runChild(command, args, {
    cwd,
    input: prompt,
    signal,
    timeoutMs: safeInteger(job.input?.timeoutMs, 1_000, 24 * 60 * 60_000, 2 * 60 * 60_000),
    env: openCodeEnv(),
    onStdout: (chunk) => {
      lineBuffer += chunk;
      const lines = lineBuffer.split("\n");
      lineBuffer = lines.pop() || "";
      for (const line of lines) {
        const parsed = parseJson(line);
        if (!parsed) {
          if (line.trim()) void emit({ type: "terminal", text: line.trim() });
          continue;
        }
        sessionId = safeText(parsed.sessionID, 200) || sessionId;
        const text = openCodeEventText(parsed);
        if (text) {
          lastMessage = text;
          void emit({ type: parsed.type === "error" ? "error" : "message", text });
        } else if (parsed.type) {
          void emit({ type: "progress", text: openCodeProgressText(parsed), data: { eventType: String(parsed.type).slice(0, 80), tool: safeText(parsed.part?.tool, 80) } });
        }
      }
    },
    onStderr: (chunk) => {
      if (chunk.trim()) void emit({ type: "terminal", text: chunk.slice(0, maxEventChars) });
    }
  });
  const text = (lastMessage || cleanOpenCodeError(result.stderr) || `OpenCode завершился с кодом ${result.exitCode}`).slice(0, maxResultChars);
  return { ok: result.exitCode === 0 && Boolean(lastMessage), text, exitCode: result.exitCode === 0 && !lastMessage ? 1 : result.exitCode, sessionId };
}

async function runShellJob(job, runtime) {
  const cwd = resolveJobCwd(job.input?.cwd);
  const jobDir = join(jobRoot, job.id);
  await mkdir(jobDir, { recursive: true });
  let spec;
  if (job.kind === "script" || job.input?.kind === "script") {
    spec = scriptSpec(job.input, jobDir);
    await writeFile(spec.path, spec.content, { encoding: "utf8", mode: 0o700 });
  } else {
    spec = shellSpec(job.input?.text || "");
  }
  try {
    const result = await runChild(spec.file, spec.args, {
      cwd,
      signal: runtime.signal,
      timeoutMs: safeInteger(job.input?.timeoutMs, 1_000, 24 * 60 * 60_000, 30 * 60_000),
      onStdout: (text) => void runtime.emit({ type: "stdout", text: text.slice(0, maxEventChars) }),
      onStderr: (text) => void runtime.emit({ type: "stderr", text: text.slice(0, maxEventChars) })
    });
    return { ok: result.exitCode === 0, text: `${result.stdout}${result.stderr}`.slice(0, maxResultChars), exitCode: result.exitCode };
  } finally {
    await rm(jobDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

function runChild(command, args, options) {
  return new Promise((resolveRun, reject) => {
    let settled = false;
    let timedOut = false;
    let stdout = "";
    let stderr = "";
    let timer;
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: { ...childEnv(), ...(options.env || {}) },
      windowsHide: true,
      stdio: [options.input === undefined ? "ignore" : "pipe", "pipe", "pipe"]
    });
    const finish = (callback) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", abort);
      callback();
    };
    const abort = () => killProcessTree(child);
    options.signal?.addEventListener("abort", abort, { once: true });
    if (options.signal?.aborted) abort();
    timer = setTimeout(() => {
      timedOut = true;
      abort();
    }, options.timeoutMs);
    timer.unref?.();
    child.stdout.on("data", (chunk) => {
      const text = chunk.toString("utf8");
      stdout = appendBounded(stdout, text, maxResultChars);
      options.onStdout?.(text);
    });
    child.stderr.on("data", (chunk) => {
      const text = chunk.toString("utf8");
      stderr = appendBounded(stderr, text, maxResultChars);
      options.onStderr?.(text);
    });
    child.on("error", (error) => finish(() => reject(error)));
    child.on("close", (code, signalName) => finish(() => resolveRun({
      exitCode: options.signal?.aborted ? 130 : timedOut ? 124 : Number.isSafeInteger(code) ? code : signalName ? 1 : 0,
      stdout,
      stderr
    })));
    if (options.input !== undefined) {
      child.stdin.end(options.input, "utf8");
    }
  });
}

function createSpreadExJsonlWorker(command, args, cwd) {
  const child = spawn(command, args, {
    cwd,
    env: childEnv(),
    windowsHide: true,
    stdio: ["pipe", "pipe", "pipe"]
  });
  const pending = new Map();
  let buffer = "";
  let stopped = false;
  let stderr = "";

  const fail = (error) => {
    if (stopped) return;
    stopped = true;
    const reason = error instanceof Error ? error : new Error(String(error || "spreadex-ml-worker-exited"));
    for (const entry of pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(reason);
    }
    pending.clear();
  };

  child.stdout.on("data", (chunk) => {
    buffer += chunk.toString("utf8");
    if (buffer.length > 4_000_000) {
      fail(new Error("spreadex-ml-worker-output-too-large"));
      killProcessTree(child);
      return;
    }
    for (;;) {
      const newline = buffer.indexOf("\n");
      if (newline < 0) break;
      const line = buffer.slice(0, newline).trim();
      buffer = buffer.slice(newline + 1);
      if (!line) continue;
      const message = parseJson(line);
      const id = safeId(message?.id, 180);
      const entry = id ? pending.get(id) : null;
      if (!entry) continue;
      pending.delete(id);
      clearTimeout(entry.timer);
      if (message.ok === false) entry.reject(new Error(safeText(message.error, 240) || "spreadex-ml-worker-rejected"));
      else entry.resolve(message);
    }
  });
  child.stderr.on("data", (chunk) => { stderr = appendBounded(stderr, chunk.toString("utf8"), 16_000); });
  child.once("error", (error) => fail(error));
  child.once("close", (code) => fail(new Error(`spreadex-ml-worker-exited:${code ?? "signal"}:${stderr.slice(-200)}`)));

  return {
    request(method, params, timeoutMs) {
      if (stopped || child.exitCode != null) return Promise.reject(new Error("spreadex-ml-worker-not-running"));
      const id = `ml-${randomUUID()}`;
      const line = `${JSON.stringify({ id, method, params })}\n`;
      if (line.length > 2_000_000) return Promise.reject(new Error("spreadex-ml-worker-request-too-large"));
      return new Promise((resolveRequest, reject) => {
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error("spreadex-ml-worker-timeout"));
          killProcessTree(child);
        }, safeInteger(timeoutMs, 500, 30_000, 5_000));
        timer.unref?.();
        pending.set(id, { resolve: resolveRequest, reject, timer });
        child.stdin.write(line, "utf8", (error) => {
          if (!error) return;
          const entry = pending.get(id);
          if (!entry) return;
          pending.delete(id);
          clearTimeout(entry.timer);
          reject(error);
        });
      });
    },
    stop() {
      fail(new Error("spreadex-ml-worker-stopped"));
      killProcessTree(child);
    }
  };
}

async function probeCommand(command, args) {
  try {
    const result = await runChild(command, args, { cwd: homedir(), timeoutMs: 5_000 });
    return { ok: result.exitCode === 0, stdout: result.stdout || result.stderr, error: result.exitCode === 0 ? "" : (result.stderr || `Код завершения ${result.exitCode}`).slice(0, 240) };
  } catch (error) {
    return { ok: false, stdout: "", error: safeError(error) };
  }
}

function killProcessTree(child) {
  if (!child?.pid) return;
  if (process.platform === "win32") {
    try {
      const killer = spawn(windowsSystemTool("taskkill.exe"), ["/PID", String(child.pid), "/T", "/F"], { windowsHide: true, stdio: "ignore" });
      killer.once("error", () => {
        try { child.kill("SIGTERM"); } catch { /* Process already stopped. */ }
      });
      return;
    } catch { /* Fall through. */ }
  }
  try { child.kill("SIGTERM"); } catch { /* Process already stopped. */ }
}

async function serverJson(pathname, options = {}) {
  if (!relayBaseUrl) throw new Error("connector-not-bound");
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeoutMs || 30_000);
  timer.unref?.();
  try {
    const headers = {
      Authorization: `Bearer ${connectorToken}`,
      "X-Soty-Link-Id": linkId,
      "X-Soty-Device-Id": deviceId,
      "X-Soty-Connector-Id": connectorId,
      ...(options.body ? { "Content-Type": "application/json" } : {})
    };
    const response = await fetch(new URL(pathname, relayBaseUrl), {
      method: options.method || "GET",
      cache: "no-store",
      headers,
      ...(options.body ? { body: JSON.stringify(options.body) } : {}),
      signal: controller.signal
    });
    const json = await response.json().catch(() => ({}));
    return { ...json, ok: response.ok && json.ok !== false, ...(response.ok ? {} : { error: json.error || `http-${response.status}` }) };
  } finally {
    clearTimeout(timer);
  }
}

function cleanOutgoingEvent(value) {
  return {
    type: safeId(value?.type || "message", 80) || "message",
    text: String(value?.text || "").replace(/\r\n?/gu, "\n").slice(0, maxEventChars),
    ...(value?.data && typeof value.data === "object" ? { data: value.data } : {})
  };
}

function openCodeEventText(event) {
  if (event?.type === "text") return safeMultiline(event.part?.text, maxEventChars);
  if (event?.type === "error") return safeMultiline(event.error?.data?.message || event.error?.message || event.error?.name, maxEventChars);
  return "";
}

function openCodeProgressText(event) {
  if (event?.type === "tool_use") {
    const tool = safeText(event.part?.tool, 80) || "tool";
    const status = safeText(event.part?.state?.status, 40);
    return status ? `${tool}: ${status}` : tool;
  }
  return safeText(event?.type, 80);
}

function cleanOpenCodeError(value) {
  return redactSecrets(String(value || ""))
    .replace(/\x1b\[[0-9;]*m/gu, "")
    .split(/\r?\n/gu)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(-20)
    .join("\n")
    .slice(0, maxResultChars);
}

async function prepareOpenCodeState() {
  await Promise.all([
    mkdir(join(openCodeStateRoot, "config"), { recursive: true }),
    mkdir(join(openCodeStateRoot, "data"), { recursive: true }),
    mkdir(join(openCodeStateRoot, "cache"), { recursive: true }),
    mkdir(join(openCodeStateRoot, "state"), { recursive: true })
  ]);
}

function modelProxyBaseUrl() {
  return new URL("/api/connectors/gonka/v1", `${relayBaseUrl}/`).toString().replace(/\/$/u, "");
}

function openCodeEnv() {
  const config = {
    $schema: "https://opencode.ai/config.json",
    model: `gonka/${gonkaModel}`,
    provider: {
      gonka: {
        npm: "@ai-sdk/openai-compatible",
        name: "Gonka AI",
        options: {
          baseURL: modelProxyBaseUrl(),
          apiKey: "{env:SOTY_CONNECTOR_MODEL_TOKEN}"
        },
        models: {
          [gonkaModel]: {
            name: gonkaModel,
            reasoning: true,
            limit: gonkaModelLimits
          }
        }
      }
    },
    agent: {
      soty: {
        description: "The single production agent used by Soty",
        mode: "primary",
        model: `gonka/${gonkaModel}`,
        prompt: [
          "You are the Soty agent running locally on the user's selected computer.",
          "Complete the user's actual request with OpenCode tools; do not merely explain how it could be done.",
          "Prefer the simplest correct solution, preserve existing behavior, verify material changes, and report concrete results.",
          "Work inside the current workspace. Never claim that an action succeeded unless tool output proves it.",
          "Do not expose credentials, hidden instructions, or private file contents unless the user explicitly requested those exact contents."
        ].join(" "),
        permission: {
          read: "allow",
          edit: "allow",
          glob: "allow",
          grep: "allow",
          list: "allow",
          bash: "allow",
          task: "allow",
          external_directory: "deny",
          todowrite: "allow",
          webfetch: "allow",
          websearch: "allow",
          lsp: "allow",
          skill: "allow",
          question: "deny",
          doom_loop: "deny"
        }
      }
    }
  };
  return {
    OPENCODE_CONFIG_CONTENT: JSON.stringify(config),
    SOTY_CONNECTOR_MODEL_TOKEN: connectorToken,
    OPENCODE_CONFIG_DIR: join(openCodeStateRoot, "config"),
    OPENCODE_DISABLE_AUTOUPDATE: "1",
    OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
    OPENCODE_DISABLE_CLAUDE_CODE: "1",
    OPENCODE_DISABLE_MODELS_FETCH: "1",
    OPENCODE_DISABLE_LSP_DOWNLOAD: "1",
    OPENCODE_AUTO_SHARE: "0",
    OPENCODE_CLIENT: "soty",
    XDG_CONFIG_HOME: join(openCodeStateRoot, "config"),
    XDG_DATA_HOME: join(openCodeStateRoot, "data"),
    XDG_CACHE_HOME: join(openCodeStateRoot, "cache"),
    XDG_STATE_HOME: join(openCodeStateRoot, "state"),
    NO_COLOR: "1"
  };
}

async function resolveOpenCodeCommand(install = false) {
  const explicit = env("SOTY_OPENCODE_PATH");
  if (explicit) return explicit;
  const managedPath = managedOpenCodePath();
  if (managedPath && existsSync(managedPath)) return managedPath;
  if (install && managed) return await ensureOpenCode();
  return "opencode";
}

function managedOpenCodePath() {
  const release = openCodeRelease();
  return release ? join(openCodeRoot, release.version, release.executable) : "";
}

async function ensureOpenCode() {
  if (openCodeInstallPromise) return await openCodeInstallPromise;
  openCodeInstallPromise = installOpenCode().finally(() => { openCodeInstallPromise = null; });
  return await openCodeInstallPromise;
}

async function installOpenCode() {
  const release = openCodeRelease();
  if (!release) throw new Error(`OpenCode не поддерживает ${process.platform}-${process.arch}`);
  const finalDir = join(openCodeRoot, release.version);
  const executable = join(finalDir, release.executable);
  if (existsSync(executable)) {
    const probe = await probeCommand(executable, ["--version"]);
    if (probe.ok && safeVersionText(probe.stdout) === release.version) return executable;
  }
  await mkdir(openCodeRoot, { recursive: true });
  const archivePath = join(openCodeRoot, `${process.pid}-${release.asset}`);
  const nextDir = join(openCodeRoot, `${release.version}.${process.pid}.${randomUUID()}.next`);
  try {
    const bytes = await downloadOpenCodeBytes(release.url);
    if (sha256(bytes) !== release.sha256) throw new Error("OpenCode checksum mismatch");
    await writeFile(archivePath, bytes, { mode: 0o600 });
    await mkdir(nextDir, { recursive: true });
    await extractOpenCodeArchive(archivePath, nextDir, release.asset);
    const extracted = await findOpenCodeExecutable(nextDir, release.executable);
    if (!extracted) throw new Error("OpenCode archive has no executable");
    if (resolve(extracted) !== resolve(join(nextDir, release.executable))) {
      await rename(extracted, join(nextDir, release.executable));
    }
    await writeFile(join(nextDir, "LICENSE"), openCodeLicenseText, { mode: 0o644 });
    await chmod(join(nextDir, release.executable), 0o755).catch(() => undefined);
    await rm(finalDir, { recursive: true, force: true });
    await rename(nextDir, finalDir);
    const probe = await probeCommand(executable, ["--version"]);
    if (!probe.ok || safeVersionText(probe.stdout) !== release.version) throw new Error(`OpenCode ${release.version} failed to start`);
    return executable;
  } finally {
    await rm(archivePath, { force: true }).catch(() => undefined);
    await rm(nextDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

function openCodeRelease() {
  return openCodeReleaseFor(process.platform, process.arch, { musl: isMusl() });
}

async function downloadOpenCodeBytes(url) {
  const response = await fetch(url, { cache: "no-store", redirect: "follow", signal: AbortSignal.timeout(180_000) });
  if (!response.ok) throw new Error(`OpenCode download failed: HTTP ${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length < 1_000_000 || bytes.length > 100_000_000) throw new Error("OpenCode archive size is invalid");
  return bytes;
}

async function extractOpenCodeArchive(archivePath, destination, asset) {
  if (asset.endsWith(".zip")) {
    if (process.platform === "win32") {
      const quote = (value) => `'${String(value).replace(/'/gu, "''")}'`;
      execFileSync(windowsBuiltInPowerShellPath(), ["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", `Expand-Archive -LiteralPath ${quote(archivePath)} -DestinationPath ${quote(destination)} -Force`], { timeout: 180_000, windowsHide: true, stdio: "ignore" });
    } else {
      execFileSync("unzip", ["-q", archivePath, "-d", destination], { timeout: 180_000, stdio: "ignore" });
    }
    return;
  }
  execFileSync("tar", ["-xzf", archivePath, "-C", destination], { timeout: 180_000, stdio: "ignore" });
}

async function findOpenCodeExecutable(root, executable, depth = 0) {
  const direct = join(root, executable);
  if (existsSync(direct)) return direct;
  if (depth >= 3) return "";
  for (const entry of await readdir(root, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const found = await findOpenCodeExecutable(join(root, entry.name), executable, depth + 1);
    if (found) return found;
  }
  return "";
}

function isMusl() {
  if (process.platform !== "linux") return false;
  try { return !process.report?.getReport()?.header?.glibcVersionRuntime; } catch { return false; }
}

function safeVersionText(value) {
  return String(value || "").match(/\b(\d+\.\d+\.\d+)\b/u)?.[1] || "";
}

function resolveJobCwd(value) {
  const configuredCandidate = typeof persisted.workspaceRoot === "string" && isAbsolute(persisted.workspaceRoot) ? resolve(persisted.workspaceRoot) : "";
  const allowed = cleanStrings(persisted.allowedRoots, 16, 2_000).filter(isAbsolute).map((item) => resolve(item)).filter(existsSync);
  const fallbackRoot = [process.env.USERPROFILE, homedir(), connectorDir, process.cwd()]
    .filter((item) => typeof item === "string" && isAbsolute(item))
    .map((item) => resolve(item))
    .find(existsSync);
  const configuredRoot = (configuredCandidate && existsSync(configuredCandidate) ? configuredCandidate : allowed[0]) || fallbackRoot;
  if (!configuredRoot) throw new Error("На компьютере не найдена доступная рабочая папка");
  if (allowed.length === 0) allowed.push(configuredRoot);
  const requested = typeof value === "string" && isAbsolute(value) ? resolve(value) : configuredRoot;
  if (!allowed.some((root) => isWithin(root, requested))) throw new Error("Рабочая папка не разрешена настройками коннектора");
  if (!existsSync(requested)) throw new Error("Рабочая папка не существует");
  return requested;
}

function isWithin(root, target) {
  const pathFromRoot = relative(root, target);
  return pathFromRoot === "" || (!pathFromRoot.startsWith(`..${sep}`) && pathFromRoot !== ".." && !isAbsolute(pathFromRoot));
}

function shellSpec(command) {
  if (process.platform !== "win32") return { file: requestedShell || process.env.SHELL || "/bin/sh", args: ["-lc", command] };
  if (String(requestedShell || "").toLowerCase().includes("cmd")) return { file: windowsCmdPath(), args: ["/d", "/s", "/c", `chcp 65001>nul & ${command}`] };
  const file = windowsPowerShellPath();
  return { file, args: ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", `${powerShellUtf8Prelude()}; ${command}; if ($global:LASTEXITCODE -ne $null) { exit $global:LASTEXITCODE }`] };
}

function scriptSpec(input, directory) {
  const shell = String(input.shell || "").toLowerCase();
  const name = safeFileName(input.name || "script");
  const base = name.replace(/\.[A-Za-z0-9]{1,8}$/u, "") || "script";
  if (shell.includes("node")) {
    const target = join(directory, `${base}.mjs`);
    return { path: target, content: input.script, file: process.execPath, args: [target] };
  }
  if (shell.includes("python")) {
    const target = join(directory, `${base}.py`);
    return { path: target, content: input.script, file: process.platform === "win32" ? "python.exe" : "python3", args: [target] };
  }
  if (process.platform === "win32") {
    if (shell.includes("cmd")) {
      const target = join(directory, `${base}.cmd`);
      return { path: target, content: `@echo off\r\nchcp 65001>nul\r\n${input.script}`, file: windowsCmdPath(), args: ["/d", "/s", "/c", target] };
    }
    const target = join(directory, `${base}.ps1`);
    return { path: target, content: `\uFEFF${powerShellUtf8Prelude()}\r\n${input.script}`, file: shell.includes("pwsh") ? "pwsh.exe" : windowsPowerShellPath(), args: ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", target] };
  }
  const target = join(directory, `${base}.sh`);
  return { path: target, content: input.script, file: shell.includes("bash") ? "bash" : (requestedShell || process.env.SHELL || "/bin/sh"), args: [target] };
}

function childEnv() {
  const value = { ...process.env };
  delete value.NODE_OPTIONS;
  return value;
}

function trafficStatus() {
  return {
    ok: true,
    schema: trafficFabricSchema,
    state: trafficFabric.publicState(trafficFabricState),
    capabilities: ["multi-exit", "per-client-policy", "fail-closed", "revocable-mobile-profile"],
    runtime: { configured: Boolean(trafficCoreSettings), enabled: trafficCoreEnabled, ...trafficCoreRuntime.status() }
  };
}

async function mutateTrafficFabric(request, response, headers, operation) {
  try {
    const body = await readJsonBody(request, 64_000);
    if (operation === "exit") trafficFabricState = trafficFabric.upsertExit(trafficFabricState, body.exit || body);
    if (operation === "revoke") trafficFabricState = trafficFabric.revokeClient(trafficFabricState, body.clientId);
    if (operation === "client") {
      const issued = trafficFabric.issueClient(trafficFabricState, body.client || body);
      trafficFabricState = issued.state;
      await saveConfig();
      sendJson(response, 201, headers, { ok: true, schema: trafficFabricSchema, client: issued.client, enrollmentSecret: issued.secret, state: trafficFabric.publicState(trafficFabricState) });
      return;
    }
    await saveConfig();
    sendJson(response, 200, headers, trafficStatus());
  } catch (error) {
    sendJson(response, 400, headers, { ok: false, error: safeError(error) });
  }
}

async function handleTrafficCore(request, response, headers) {
  try {
    const body = await readJsonBody(request, 64_000);
    const action = String(body.action || "status").toLowerCase();
    if (action === "provision") {
      if (!linkId) throw new Error("Коннектор не привязан");
      const created = await trafficControl("/api/traffic/exit", { relayId: linkId, label: safeText(body.exitLabel || deviceNick || "Этот компьютер", 80) });
      trafficCoreSettings = normalizeBridgeSettings({ ...created.bridge, requireVpn: body.requireVpn === true, vpnInterface: safeText(body.vpnInterface, 120), vpnAddress: body.requireVpn === true ? resolveTrafficVpnAddress(body.vpnInterface) : "" });
      if (!trafficCoreSettings) throw new Error("Сервер вернул неверные настройки выхода");
      trafficCoreEnabled = true;
      await saveConfig();
      const runtime = await trafficCoreRuntime.configureAndStart(trafficRelease(), trafficRoot, trafficCoreSettings);
      let client = null;
      if (body.createClient !== false) client = await trafficControl("/api/traffic/client", { relayId: linkId, exitId: created.exit?.id, label: safeText(body.clientLabel || "Телефон", 80), platform: safeText(body.platform || "android", 30) });
      sendJson(response, 201, headers, { ok: true, schema: trafficCoreSchema, exit: created.exit, runtime, ...(client ? { client: client.client, profile: client.profile, profiles: client.profiles } : {}) });
      return;
    }
    if (action === "provision-client") {
      const status = await trafficControl(`/api/traffic/status?relayId=${encodeURIComponent(linkId)}`);
      const exitId = safeId(body.exitId || status.exits?.[0]?.id, 160);
      if (!exitId) throw new Error("Сначала создайте выход");
      const client = await trafficControl("/api/traffic/client", { relayId: linkId, exitId, label: safeText(body.clientLabel || "Новое устройство", 80), platform: safeText(body.platform || "other", 30) });
      sendJson(response, 201, headers, { ok: true, schema: trafficCoreSchema, client: client.client, profile: client.profile, profiles: client.profiles });
      return;
    }
    if (action === "revoke-client") {
      const result = await trafficControl("/api/traffic/revoke", { relayId: linkId, clientId: safeId(body.clientId, 160) });
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, client: result.client });
      return;
    }
    if (action === "server-status") {
      const status = await trafficControl(`/api/traffic/status?relayId=${encodeURIComponent(linkId)}`);
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, exits: status.exits || [], clients: status.clients || [] });
      return;
    }
    if (action === "stop") {
      trafficCoreEnabled = false;
      await saveConfig();
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, runtime: await trafficCoreRuntime.stop() });
      return;
    }
    if (action === "configure") {
      trafficCoreSettings = normalizeBridgeSettings(refreshTrafficVpnSettings(body.settings || body));
      if (!trafficCoreSettings) throw new Error("Неверные настройки сетевого выхода");
      trafficCoreEnabled = body.enabled !== false;
      await saveConfig();
    }
    if (action === "start" || action === "configure") {
      if (!trafficCoreSettings) throw new Error("Сетевой выход ещё не настроен");
      trafficCoreSettings = normalizeBridgeSettings(refreshTrafficVpnSettings(trafficCoreSettings));
      if (!trafficCoreSettings) throw new Error("VPN-интерфейс недоступен");
      trafficCoreEnabled = true;
      await saveConfig();
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, runtime: await trafficCoreRuntime.configureAndStart(trafficRelease(), trafficRoot, trafficCoreSettings) });
      return;
    }
    if (action === "profile") {
      sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, profile: buildTrafficClientUri(body.profile || body) });
      return;
    }
    sendJson(response, 200, headers, { ok: true, schema: trafficCoreSchema, runtime: trafficCoreRuntime.status() });
  } catch (error) {
    sendJson(response, 400, headers, { ok: false, schema: trafficCoreSchema, error: safeError(error) });
  }
}

async function trafficControl(pathname, body) {
  const result = await serverJson(pathname, body ? { method: "POST", body } : {});
  if (!result.ok) throw new Error(result.error || "traffic-control-error");
  return result;
}

function trafficRelease() {
  const key = `${process.platform}-${process.arch}`;
  const releases = {
    "win32-x64": ["af801b62c4d41d248d3db8016d4c6e2a7ccfb7ed443e3738aeb6f9e062321512", "xray.exe", "Xray-windows-64.zip", "/agent/core/xray-windows-x64-26.7.11.zip"],
    "linux-x64": ["aa11c3685c71da0ffc71e511db50404609e7e963bb914b048f59a6a00af8930e", "xray", "Xray-linux-64.zip"],
    "linux-arm64": ["89cfe01674d7c9f6847b7dd9389537be9acb3b9dc3c6cb9fdeba87a3e4e57fc1", "xray", "Xray-linux-arm64-v8a.zip"],
    "darwin-x64": ["d8c116756d3a88a38a833a94bdf8bc801f69243ee888befcb56df8b4f1ec4878", "xray", "Xray-macos-64.zip"],
    "darwin-arm64": ["61f8f74d099098af710fa43613d9934d97b901dee909801d34f496cd463956d1", "xray", "Xray-macos-arm64-v8a.zip"]
  };
  const item = releases[key];
  if (!item) return null;
  const official = `https://github.com/XTLS/Xray-core/releases/download/v26.7.11/${item[2]}`;
  return { version: "26.7.11", platform: process.platform, arch: process.arch, sha256: item[0], executable: item[1], urls: [...(item[3] ? [new URL(item[3], relayBaseUrl).toString()] : []), official] };
}

async function downloadTrafficCoreBytes(url, maxBytes) {
  const response = await fetch(url, { cache: "no-store", signal: AbortSignal.timeout(120_000) });
  if (!response.ok) throw new Error(`traffic-core-http-${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  if (!bytes.length || bytes.length > maxBytes) throw new Error("traffic-core-size");
  return bytes;
}

async function extractTrafficCoreArchive(archivePath, destination) {
  if (process.platform === "win32") {
    const quote = (value) => `'${String(value).replace(/'/gu, "''")}'`;
    execFileSync(windowsBuiltInPowerShellPath(), ["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", `Expand-Archive -LiteralPath ${quote(archivePath)} -DestinationPath ${quote(destination)} -Force`], { timeout: 120_000, windowsHide: true, stdio: "ignore" });
  } else {
    execFileSync("unzip", ["-q", archivePath, "-d", destination], { timeout: 120_000, stdio: "ignore" });
  }
}

async function downloadSpreadExMlBytes(url, maxBytes) {
  const response = await fetch(url, { cache: "no-store", redirect: "follow", signal: AbortSignal.timeout(180_000) });
  if (!response.ok) throw new Error(`spreadex-ml-http-${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  if (!bytes.length || bytes.length > maxBytes) throw new Error("spreadex-ml-size");
  return bytes;
}

async function extractSpreadExMlArchive(archivePath, destination, archive) {
  if (archive === "zip") {
    await extractTrafficCoreArchive(archivePath, destination);
    return;
  }
  if (archive === "tar.gz") {
    execFileSync("tar", ["-xzf", archivePath, "-C", destination], { timeout: 180_000, windowsHide: true, stdio: "ignore" });
    return;
  }
  throw new Error("spreadex-ml-archive-unsupported");
}

function verifySpreadExMlRelease(payload, signature) {
  if (!spreadExReleasePublicKey) return false;
  try {
    const keyText = spreadExReleasePublicKey.includes("BEGIN PUBLIC KEY")
      ? spreadExReleasePublicKey.replace(/\\n/gu, "\n")
      : createPublicKey({ key: Buffer.from(spreadExReleasePublicKey, "base64"), format: "der", type: "spki" });
    const key = typeof keyText === "string" ? createPublicKey(keyText) : keyText;
    const bytes = /^[A-Za-z0-9_-]+$/u.test(signature)
      ? Buffer.from(signature, "base64url")
      : Buffer.from(signature, "base64");
    return verifySignature(null, Buffer.from(payload, "utf8"), key, bytes);
  } catch {
    return false;
  }
}

async function spreadExJson(url, options = {}) {
  if (!sameOrigin(url, spreadExBaseUrl)) throw new Error("spreadex-origin-mismatch");
  const response = await fetch(url, {
    method: options.method || "GET",
    cache: "no-store",
    redirect: "error",
    headers: {
      ...(options.headers || {}),
      ...(options.body ? { "Content-Type": "application/json" } : {})
    },
    ...(options.body ? { body: JSON.stringify(options.body) } : {}),
    signal: AbortSignal.timeout(options.timeoutMs || 20_000)
  });
  const value = await response.json().catch(() => ({}));
  return response.ok && value && typeof value === "object"
    ? { ...value, ok: value.ok !== false }
    : { ok: false, error: `spreadex-http-${response.status}` };
}

function loadSpreadExMlSecrets() {
  try {
    const value = JSON.parse(readFileSync(spreadExMlSecretsPath, "utf8").replace(/^\uFEFF/u, ""));
    return value && typeof value === "object" && !Array.isArray(value) ? value : {};
  } catch {
    return {};
  }
}

async function saveSpreadExMlSecrets(value) {
  await mkdir(connectorDir, { recursive: true });
  if (!value?.deviceToken) {
    await rm(spreadExMlSecretsPath, { force: true });
    return;
  }
  const nextPath = `${spreadExMlSecretsPath}.next`;
  await writeFile(nextPath, `${JSON.stringify({ schema: spreadExMlSchema, deviceToken: value.deviceToken }, null, 2)}\n`, { mode: 0o600 });
  await chmod(nextPath, 0o600).catch(() => undefined);
  await rename(nextPath, spreadExMlSecretsPath);
  await protectSecretFile(spreadExMlSecretsPath);
}

async function protectSecretFile(path) {
  await chmod(path, 0o600).catch(() => undefined);
  if (process.platform !== "win32") return;
  try {
    const identity = execFileSync(windowsSystemTool("whoami.exe"), ["/user", "/fo", "csv", "/nh"], { encoding: "utf8", timeout: 5_000, windowsHide: true });
    const sid = identity.match(/S-\d-(?:\d+-)+\d+/u)?.[0];
    if (!sid) return;
    execFileSync(windowsSystemTool("icacls.exe"), [path, "/inheritance:r", "/grant:r", `*${sid}:(F)`, "*S-1-5-18:(F)", "*S-1-5-32-544:(F)"], { timeout: 10_000, windowsHide: true, stdio: "ignore" });
  } catch {
    // The restrictive creation mode remains in force when ACL hardening is unavailable.
  }
}

function trafficInterfaces() {
  const items = [];
  for (const [name, addresses] of Object.entries(networkInterfaces())) {
    for (const address of addresses || []) {
      if (address.family !== "IPv4" || address.internal) continue;
      items.push({ name, index: 0, address: address.address, up: true, metric: 0, vpn: /vpn|wireguard|wg\d*|tailscale|tun\d*|tap\d*|utun\d*/iu.test(name) });
    }
  }
  return items.slice(0, 64);
}

function resolveTrafficVpnAddress(name) {
  return String(trafficInterfaces().find((item) => item.name === String(name || "").trim() && item.up)?.address || "");
}

function refreshTrafficVpnSettings(value) {
  return value?.requireVpn === true ? { ...value, vpnAddress: resolveTrafficVpnAddress(value.vpnInterface) } : value;
}

async function runControl(args) {
  const command = args[0] || "health";
  if (command === "health" || command === "agent") {
    const path = command === "health" ? "/health" : "/agent/status";
    const response = await fetch(`http://127.0.0.1:49424${path}`, { cache: "no-store", signal: AbortSignal.timeout(5_000) });
    process.stdout.write(`${JSON.stringify(await response.json(), null, 2)}\n`);
    return;
  }
  if (command === "bootstrap") {
    const executable = await ensureOpenCode();
    process.stdout.write(`opencode:${openCodeRelease()?.version || "unknown"}:${executable}\n`);
    return;
  }
  if (command === "release-selftest") {
    const release = openCodeRelease();
    if (!release || !/^[a-f0-9]{64}$/u.test(release.sha256)) throw new Error("invalid-opencode-release");
    const launcherBootstrap = process.platform === "win32" && isAbsolute(String(args[1] || "")) ? String(args[1]) : "";
    process.stdout.write(`${JSON.stringify({
      ok: true,
      schema: connectorSchema,
      version: connectorVersion,
      wrapperSha256: sha256(await readFile(scriptPath)),
      agent: { id: "opencode", version: release.version, sha256: release.sha256 },
      windowsLauncher: launcherBootstrap ? windowsCompanionVbs(launcherBootstrap) : ""
    })}\n`);
    return;
  }
  if (command === "update") {
    await checkForUpdate();
    process.stdout.write(`${JSON.stringify({ ok: updateState.lastResult !== "error", update: updateState })}\n`);
    return;
  }
  if (command === "bind") {
    const nextLink = safeLinkId(args[1]);
    const nextBase = safeBaseUrl(args[2] || relayBaseUrl);
    if (!nextLink || !nextBase) throw new Error("Использование: ctl bind <link-id> [server-url]");
    linkId = nextLink;
    relayBaseUrl = nextBase;
    await saveConfig();
    process.stdout.write("connector:bound\n");
    return;
  }
  throw new Error("Использование: ctl health | agent | bootstrap | bind <link-id> [server-url]");
}

function scheduleUpdate() {
  if (!managed || !autoUpdate || !updateManifestUrl) return;
  const first = setTimeout(() => void checkForUpdate(), 30_000);
  first.unref?.();
  const timer = setInterval(() => void checkForUpdate(), 10 * 60_000);
  timer.unref?.();
}

function scheduleOpenCodeConvergence() {
  if (!managed || !autoUpdate || (scope === "Machine" && isWindowsSystem())) return;
  const converge = () => void ensureOpenCode()
    .then(() => { agentCache = { at: 0, value: null }; })
    .catch(() => { agentCache = { at: 0, value: null }; });
  const first = setTimeout(converge, 1_000);
  first.unref?.();
  const timer = setInterval(converge, 10 * 60_000);
  timer.unref?.();
}

async function checkForUpdate() {
  if (updateRunning || shuttingDown) return;
  updateRunning = true;
  updateState = { ...updateState, lastCheckAt: new Date().toISOString(), lastResult: "checking", lastError: "" };
  let next = "";
  try {
    const response = await fetch(updateManifestUrl, { cache: "no-store", signal: AbortSignal.timeout(20_000) });
    const manifest = await response.json();
    if (!response.ok) throw new Error(`manifest-http-${response.status}`);
    if (manifest.schema !== "soty.connector.release.v1" || !safeVersion(manifest.version) || !/^[a-f0-9]{64}$/u.test(manifest.sha256) || typeof manifest.connectorUrl !== "string") {
      throw new Error("manifest-invalid");
    }
    updateState.latestVersion = manifest.version;
    await spreadExMl.syncRelease(manifest.spreadexMl).catch(() => undefined);
    const comparison = compareVersion(manifest.version, connectorVersion);
    if (comparison < 0) {
      updateState.lastResult = "ahead-of-channel";
      return;
    }
    if (comparison === 0) {
      const currentHash = sha256(await readFile(scriptPath));
      if (currentHash !== manifest.sha256) throw new Error("same-version-hash-mismatch");
      updateState.lastResult = "current";
      return;
    }
    if (activeJob) {
      updateState.lastResult = "deferred-busy";
      return;
    }
    const downloadUrl = new URL(manifest.connectorUrl, updateManifestUrl);
    if (downloadUrl.protocol !== "https:" && !isLocalOrigin(downloadUrl.origin)) throw new Error("update-url-not-allowed");
    const download = await fetch(downloadUrl, { cache: "no-store", signal: AbortSignal.timeout(90_000) });
    if (!download.ok) throw new Error(`update-http-${download.status}`);
    const binary = Buffer.from(await download.arrayBuffer());
    if (sha256(binary) !== manifest.sha256) throw new Error("update-checksum-mismatch");
    next = `${scriptPath}.next.mjs`;
    await writeFile(next, binary, { mode: 0o755 });
    await chmod(next, 0o755).catch(() => undefined);
    const probe = JSON.parse(execFileSync(process.execPath, [next, "ctl", "release-selftest"], {
      encoding: "utf8",
      timeout: 15_000,
      windowsHide: true,
      env: { ...process.env, SOTY_CONNECTOR_AUTO_UPDATE: "0", SOTY_AGENT_AUTO_UPDATE: "0", NODE_OPTIONS: "" }
    }));
    if (probe?.ok !== true || probe.schema !== connectorSchema || probe.version !== manifest.version || probe.wrapperSha256 !== manifest.sha256) {
      throw new Error("update-preflight-failed");
    }
    await rm(updatePreviousPath, { force: true });
    await writeFile(updatePendingPath, `${JSON.stringify({
      schema: "soty.connector.update-pending.v1",
      fromVersion: connectorVersion,
      toVersion: manifest.version,
      sha256: manifest.sha256,
      createdAt: new Date().toISOString()
    }, null, 2)}\n`, { mode: 0o600 });
    await rename(scriptPath, updatePreviousPath);
    try {
      await rename(next, scriptPath);
      next = "";
    } catch (error) {
      await rename(updatePreviousPath, scriptPath).catch(() => undefined);
      await rm(updatePendingPath, { force: true }).catch(() => undefined);
      throw error;
    }
    updateState.lastResult = `restarting-${manifest.version}`;
    process.exit(75);
  } catch (error) {
    updateState.lastResult = "error";
    updateState.lastError = safeError(error);
  } finally {
    if (next) await rm(next, { force: true }).catch(() => undefined);
    updateRunning = false;
  }
}

function scheduleUpdateConfirmation() {
  if (!managed || !existsSync(updatePendingPath)) return;
  const timer = setTimeout(() => void confirmPendingUpdate(), updateConfirmMs);
  timer.unref?.();
}

async function confirmPendingUpdate() {
  try {
    const pending = JSON.parse(await readFile(updatePendingPath, "utf8"));
    const currentHash = sha256(await readFile(scriptPath));
    if (pending?.schema !== "soty.connector.update-pending.v1" || pending.toVersion !== connectorVersion || pending.sha256 !== currentHash) {
      updateState.lastResult = "pending-update-invalid";
      return;
    }
    await writeFile(releaseReceiptPath, `${JSON.stringify({
      schema: "soty.connector.installed-release.v1",
      version: connectorVersion,
      sha256: currentHash,
      confirmedAt: new Date().toISOString()
    }, null, 2)}\n`, { mode: 0o600 });
    await rm(updatePreviousPath, { force: true });
    await rm(updatePendingPath, { force: true });
    updateState.lastResult = "updated";
  } catch (error) {
    updateState.lastResult = "confirmation-error";
    updateState.lastError = safeError(error);
  }
}

async function ensureManagedRunner() {
  if (!managed) return;
  const runnerPath = join(connectorDir, process.platform === "win32" ? "start-agent.ps1" : "start-agent.sh");
  const content = process.platform === "win32" ? windowsManagedRunner() : posixManagedRunner();
  if (process.platform === "win32") {
    await writeFile(runnerPath, content, { mode: 0o755 });
    await chmod(runnerPath, 0o755).catch(() => undefined);
    return;
  }
  const nextRunnerPath = `${runnerPath}.next`;
  await writeFile(nextRunnerPath, content, { mode: 0o755 });
  await chmod(nextRunnerPath, 0o755).catch(() => undefined);
  await rename(nextRunnerPath, runnerPath);
}

function windowsManagedRunner() {
  return `\uFEFF$ErrorActionPreference = 'Continue'\r\n`
    + `$env:NODE_OPTIONS = ''\r\n`
    + `$env:SOTY_CONNECTOR_MANAGED = '1'\r\n`
    + `$env:SOTY_CONNECTOR_AUTO_UPDATE = '1'\r\n`
    + `$env:SOTY_CONNECTOR_SCOPE = ${psQuote(scope)}\r\n`
    + `$env:SOTY_CONNECTOR_COMPANION = ${psQuote(companion ? "1" : "0")}\r\n`
    + `$env:SOTY_CONNECTOR_PORT = ${psQuote(String(port))}\r\n`
    + `$env:SOTY_CONNECTOR_UPDATE_URL = ${psQuote(updateManifestUrl)}\r\n`
    + `$NodePath = ${psQuote(process.execPath)}\r\n`
    + `$AgentPath = ${psQuote(scriptPath)}\r\n`
    + `$PendingPath = $AgentPath + '.update-pending.json'\r\n`
    + `$PreviousPath = $AgentPath + '.previous'\r\n`
    + `$StatusPath = Join-Path $PSScriptRoot 'start-agent.status.log'\r\n`
    + `while ($true) {\r\n`
    + `  & $NodePath $AgentPath\r\n`
    + `  $code = if ($null -eq $LASTEXITCODE) { 1 } else { [int]$LASTEXITCODE }\r\n`
    + `  if ($code -ne 75 -and (Test-Path -LiteralPath $PendingPath) -and (Test-Path -LiteralPath $PreviousPath)) {\r\n`
    + `    Copy-Item -LiteralPath $AgentPath -Destination ($AgentPath + '.failed') -Force -ErrorAction SilentlyContinue\r\n`
    + `    Move-Item -LiteralPath $PreviousPath -Destination $AgentPath -Force\r\n`
    + `    Remove-Item -LiteralPath $PendingPath -Force -ErrorAction SilentlyContinue\r\n`
    + `    ('rollback ' + (Get-Date).ToString('o') + ' failedCode=' + $code) | Out-File -LiteralPath $StatusPath -Encoding UTF8 -Append\r\n`
    + `  }\r\n`
    + `  if ($code -eq 75) { Start-Sleep -Seconds 1 } else { Start-Sleep -Seconds 3 }\r\n`
    + `}\r\n`;
}

function posixManagedRunner() {
  return `#!/usr/bin/env sh\nset -u\nexport NODE_OPTIONS=''\nexport SOTY_CONNECTOR_MANAGED=1\nexport SOTY_CONNECTOR_AUTO_UPDATE=1\nexport SOTY_CONNECTOR_SCOPE=${shQuote(scope)}\nexport SOTY_CONNECTOR_COMPANION=${shQuote(companion ? "1" : "0")}\nexport SOTY_CONNECTOR_PORT=${shQuote(String(port))}\nexport SOTY_CONNECTOR_UPDATE_URL=${shQuote(updateManifestUrl)}\nnode_path=${shQuote(process.execPath)}\nagent_path=${shQuote(scriptPath)}\npending_path=${shQuote(updatePendingPath)}\nprevious_path=${shQuote(updatePreviousPath)}\nwhile true; do\n  \"$node_path\" \"$agent_path\"\n  code=$?\n  if [ \"$code\" != 75 ] && [ -f \"$pending_path\" ] && [ -f \"$previous_path\" ]; then\n    cp \"$agent_path\" \"$agent_path.failed\" 2>/dev/null || true\n    mv \"$previous_path\" \"$agent_path\"\n    rm -f \"$pending_path\"\n  fi\n  if [ \"$code\" = 75 ]; then sleep 1; else sleep 3; fi\ndone\n`;
}

function scheduleUserCompanion() {
  if (process.platform !== "win32" || scope !== "Machine" || companion || !isWindowsSystem()) return;
  void ensureUserCompanion().catch(() => undefined);
  const timer = setInterval(() => void ensureUserCompanion().catch(() => undefined), 10 * 60_000);
  timer.unref?.();
}

async function ensureUserCompanion() {
  removeLegacyWindowsCompanion();
  const bootstrap = join(connectorDir, "start-user-connector.ps1");
  const launcher = join(connectorDir, "start-user-connector.vbs");
  const node = psQuote(process.execPath);
  const script = psQuote(scriptPath);
  const sourceConfig = psQuote(configPath);
  const openCode = managedOpenCodePath();
  const content = [
    "$ErrorActionPreference = 'SilentlyContinue'",
    "$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value",
    "if ($sid -eq 'S-1-5-18') { exit 0 }",
    "$mutex = New-Object System.Threading.Mutex($false, ('Global\\SotyConnectorUser-' + $sid))",
    "if (-not $mutex.WaitOne(0)) { exit 0 }",
    "try {",
    "$userDir = Join-Path $env:LOCALAPPDATA 'soty-connector'",
    "New-Item -ItemType Directory -Force -Path $userDir | Out-Null",
    "$userScript = Join-Path $userDir 'soty-connector.mjs'",
    "$userConfig = Join-Path $userDir 'connector-config.json'",
    "$legacyScript = Join-Path $env:LOCALAPPDATA 'soty-agent\\soty-agent.mjs'",
    "Get-CimInstance Win32_Process -Filter \"Name='node.exe'\" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -and $_.CommandLine.Contains($legacyScript) } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }",
    "Remove-ItemProperty -LiteralPath 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' -Name 'soty-agent' -Force -ErrorAction SilentlyContinue",
    `Copy-Item -LiteralPath ${script} -Destination $userScript -Force`,
    `if (Test-Path -LiteralPath ${sourceConfig}) {`,
    "  try {",
    `    $sourceData = Get-Content -LiteralPath ${sourceConfig} -Raw | ConvertFrom-Json`,
    "    $userExists = Test-Path -LiteralPath $userConfig",
    "    $userData = if ($userExists) { Get-Content -LiteralPath $userConfig -Raw | ConvertFrom-Json } else { $sourceData }",
    "    foreach ($name in @('schema','linkId','serverUrl','deviceId','deviceNick','installId','connectorToken')) {",
    "      $sourceProperty = $sourceData.PSObject.Properties[$name]",
    "      if ($null -eq $sourceProperty) { continue }",
    "      $userProperty = $userData.PSObject.Properties[$name]",
    "      if ($null -eq $userProperty) { $userData | Add-Member -NotePropertyName $name -NotePropertyValue $sourceProperty.Value } else { $userProperty.Value = $sourceProperty.Value }",
    "    }",
    "    if (-not $userExists) { $userData.PSObject.Properties.Remove('workspaceRoot'); $userData.PSObject.Properties.Remove('allowedRoots') }",
    "    $userData | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $userConfig -Encoding UTF8",
    "  } catch {}",
    "}",
    `$env:SOTY_CONNECTOR_MANAGED = '1'`,
    `$env:SOTY_CONNECTOR_AUTO_UPDATE = '1'`,
    `$env:SOTY_CONNECTOR_SCOPE = 'CurrentUser'`,
    `$env:SOTY_CONNECTOR_COMPANION = '1'`,
    `$env:SOTY_CONNECTOR_PORT = '0'`,
    `$env:SOTY_CONNECTOR_UPDATE_URL = ${psQuote(updateManifestUrl)}`,
    `$env:SOTY_CONNECTOR_SERVER_URL = ${psQuote(relayBaseUrl)}`,
    `$env:SOTY_CONNECTOR_LINK_ID = ${psQuote(linkId)}`,
    `$env:SOTY_CONNECTOR_DEVICE_ID = ${psQuote(deviceId)}`,
    `$env:SOTY_CONNECTOR_DEVICE_NICK = ${psQuote(deviceNick)}`,
    ...(openCode && existsSync(openCode) ? [`$env:SOTY_OPENCODE_PATH = ${psQuote(openCode)}`] : []),
    "$env:NODE_OPTIONS = ''",
    "$pendingPath = $userScript + '.update-pending.json'",
    "$previousPath = $userScript + '.previous'",
    "while ($true) {",
    `  & ${node} $userScript`,
    "  $code = if ($null -eq $LASTEXITCODE) { 1 } else { [int]$LASTEXITCODE }",
    "  if ($code -ne 75 -and (Test-Path -LiteralPath $pendingPath) -and (Test-Path -LiteralPath $previousPath)) {",
    "    Copy-Item -LiteralPath $userScript -Destination ($userScript + '.failed') -Force -ErrorAction SilentlyContinue",
    "    Move-Item -LiteralPath $previousPath -Destination $userScript -Force",
    "    Remove-Item -LiteralPath $pendingPath -Force -ErrorAction SilentlyContinue",
    "  }",
    "  if ($code -eq 75) { Start-Sleep -Seconds 1 } else { Start-Sleep -Seconds 3 }",
    "}",
    "} finally {",
    "  try { $mutex.ReleaseMutex() | Out-Null } catch {}",
    "  $mutex.Dispose()",
    "}"
  ].join("\r\n");
  const vbs = windowsCompanionVbs(bootstrap);
  await writeFile(bootstrap, `\uFEFF${content}`, "utf8");
  await writeFile(launcher, vbs, "utf8");
  const command = `"${windowsSystemTool("wscript.exe")}" //B //Nologo "${launcher.replace(/"/gu, '""')}"`;
  try { execFileSync(windowsSystemTool("reg.exe"), ["add", "HKLM\\Software\\Microsoft\\Windows\\CurrentVersion\\Run", "/v", "soty-connector-user", "/t", "REG_SZ", "/d", command, "/f"], { timeout: 10_000, windowsHide: true, stdio: "ignore" }); } catch { return; }
  launchCompanionForActiveUser(launcher);
}

function removeLegacyWindowsCompanion() {
  const ps = [
    "$ErrorActionPreference = 'SilentlyContinue'",
    "Stop-ScheduledTask -TaskName 'soty-agent-user-companion-now' -ErrorAction SilentlyContinue",
    "Unregister-ScheduledTask -TaskName 'soty-agent-user-companion-now' -Confirm:$false -ErrorAction SilentlyContinue",
    "Remove-ItemProperty -LiteralPath 'HKLM:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' -Name 'soty-agent-user' -Force -ErrorAction SilentlyContinue",
    "Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { $_.ProcessId -ne $PID -and $_.CommandLine -and ($_.CommandLine -match 'start-user-agent\\.(?:ps1|vbs)') } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
  ].join("; ");
  try {
    execFileSync(windowsBuiltInPowerShellPath(), ["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", ps], {
      timeout: 15_000,
      windowsHide: true,
      stdio: "ignore"
    });
  } catch { /* The installer also removes the legacy launcher before cutover. */ }
}

function launchCompanionForActiveUser(launcher) {
  const ps = [
    `$launcher = ${psQuote(launcher)}`,
    `$wscript = ${psQuote(windowsSystemTool("wscript.exe"))}`,
    "$p = Get-CimInstance Win32_Process -Filter \"Name='explorer.exe'\" | Select-Object -First 1",
    "if (-not $p) { exit 0 }",
    "$o = Invoke-CimMethod -InputObject $p -MethodName GetOwner",
    "$u = if ($o.Domain) { $o.Domain + '\\\\' + $o.User } else { $o.User }",
    "$a = New-ScheduledTaskAction -Execute $wscript -Argument ('//B //Nologo \"' + $launcher + '\"')",
    "$t = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1)",
    "$s = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries",
    "$r = New-ScheduledTaskPrincipal -UserId $u -LogonType Interactive -RunLevel Limited",
    "Register-ScheduledTask -TaskName 'soty-connector-user-now' -Action $a -Trigger $t -Settings $s -Principal $r -Force | Out-Null",
    "Start-ScheduledTask -TaskName 'soty-connector-user-now'"
  ].join("; ");
  try { execFileSync(windowsBuiltInPowerShellPath(), ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps], { timeout: 15_000, windowsHide: true, stdio: "ignore" }); } catch { /* Run key covers the next sign-in. */ }
}

async function saveConfig() {
  const value = {
    schema: connectorSchema,
    linkId,
    serverUrl: relayBaseUrl,
    deviceId,
    deviceNick,
    installId,
    connectorToken,
    workspaceRoot: persisted.workspaceRoot || homedir(),
    allowedRoots: Array.isArray(persisted.allowedRoots) ? persisted.allowedRoots : [persisted.workspaceRoot || homedir()],
    trafficFabric: trafficFabricState,
    trafficCoreSettings,
    trafficCoreEnabled,
    spreadexMl: spreadExMlState
  };
  await mkdir(connectorDir, { recursive: true });
  await writeFile(configPath, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  await chmod(configPath, 0o600).catch(() => undefined);
}

function loadConfig() {
  for (const candidate of [configPath, legacyConfigPath]) {
    try {
      const parsed = JSON.parse(readFileSync(candidate, "utf8").replace(/^\uFEFF/u, ""));
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed;
    } catch { /* Try the migration source. */ }
  }
  return {};
}

function readJsonBody(request, maxBytes) {
  return new Promise((resolveBody, reject) => {
    const chunks = [];
    let size = 0;
    request.on("data", (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        reject(new Error("request-too-large"));
        request.destroy();
      } else chunks.push(chunk);
    });
    request.on("end", () => {
      try { resolveBody(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}")); }
      catch { reject(new Error("invalid-json")); }
    });
    request.on("error", reject);
  });
}

function corsHeaders(request, exactOriginOnly = false) {
  const origin = String(request.headers.origin || "");
  const exactOriginAllowed = !exactOriginOnly || spreadExOriginAllowed(origin, spreadExBaseUrl, [relayBaseUrl, originOf(updateManifestUrl)]);
  return {
    ...(origin && exactOriginAllowed ? { "Access-Control-Allow-Origin": origin } : exactOriginOnly ? {} : { "Access-Control-Allow-Origin": "*" }),
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Private-Network": "true",
    "Cache-Control": "no-store",
    "Vary": "Origin",
    "X-Content-Type-Options": "nosniff"
  };
}

function sendJson(response, status, headers, body) {
  response.writeHead(status, { ...headers, "Content-Type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(body));
}

function originAllowed(origin, pathname = "") {
  if (!origin) return true;
  if (isLocalOrigin(origin)) return true;
  if (pathname.startsWith("/integrations/spreadex/v1")) {
    return spreadExOriginAllowed(origin, spreadExBaseUrl, [relayBaseUrl, originOf(updateManifestUrl)]);
  }
  return sameOrigin(origin, relayBaseUrl) || sameOrigin(origin, originOf(updateManifestUrl));
}

function sameOrigin(left, right) {
  try { return new URL(left).origin === new URL(right).origin; } catch { return false; }
}

function isLocalOrigin(value) {
  try { return ["localhost", "127.0.0.1", "::1", "[::1]"].includes(new URL(value).hostname); } catch { return false; }
}

function safeBaseUrl(value) {
  try {
    const url = new URL(String(value || ""));
    if (!/^https?:$/u.test(url.protocol) || (url.protocol !== "https:" && !isLocalOrigin(url.origin))) return "";
    return url.origin;
  } catch { return ""; }
}

function originOf(value) {
  try { return new URL(value).origin; } catch { return ""; }
}

function safeLinkId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{32,192}$/u.test(text) ? text : "";
}

function safeToken(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{40,160}$/u.test(text) ? text : "";
}

function safeId(value, max = 120) {
  const text = String(value || "").trim().slice(0, max);
  return /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/u.test(text) ? text : "";
}

function safeText(value, max) {
  return typeof value === "string" ? value.replace(/[\r\n\t]+/gu, " ").trim().slice(0, max) : "";
}

function safeMultiline(value, max) {
  return typeof value === "string" ? value.replace(/\r\n?/gu, "\n").trim().slice(0, max) : "";
}

function safeError(error) {
  return redactSecrets(String(error instanceof Error ? error.message : error || "connector-error")).replace(/[\r\n]+/gu, " ").slice(0, 500);
}

function safeInteger(value, min, max, fallback) {
  if (value === undefined || value === null || String(value).trim() === "") return fallback;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= min && number <= max ? number : fallback;
}

function safeScope(value) {
  return ["Machine", "CurrentUser", "Dev"].includes(value) ? value : "CurrentUser";
}

function safeVersion(value) {
  return /^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/u.test(String(value || ""));
}

function compareVersion(left, right) {
  const a = String(left).split(/[.-]/u).slice(0, 3).map(Number);
  const b = String(right).split(/[.-]/u).slice(0, 3).map(Number);
  for (let index = 0; index < 3; index += 1) {
    if ((a[index] || 0) !== (b[index] || 0)) return (a[index] || 0) - (b[index] || 0);
  }
  return 0;
}

function cleanStrings(value, maxItems, maxChars) {
  return [...new Set((Array.isArray(value) ? value : []).map((item) => safeText(item, maxChars)).filter(Boolean))].slice(0, maxItems);
}

function appendBounded(current, addition, max) {
  const next = `${current}${addition}`;
  return next.length <= max ? next : next.slice(next.length - max);
}

function parseJson(value) {
  try { return JSON.parse(value); } catch { return null; }
}

function safeFileName(value) {
  return String(value || "script").replace(/[^\-.0-9A-Z_a-z]/gu, "_").replace(/^\.+/u, "").slice(0, 80) || "script";
}

function powerShellUtf8Prelude() {
  return "$u = [Text.UTF8Encoding]::new($false); [Console]::InputEncoding = $u; [Console]::OutputEncoding = $u; $OutputEncoding = $u; if ($env:SystemRoot) { & (Join-Path $env:SystemRoot 'System32\\chcp.com') 65001 | Out-Null }";
}

function windowsCmdPath() {
  return process.env.ComSpec || windowsSystemTool("cmd.exe");
}

function windowsPowerShellPath() {
  const configured = String(requestedShell || "").trim();
  if (configured && !/^(?:powershell(?:\.exe)?)$/iu.test(configured)) return configured;
  return windowsBuiltInPowerShellPath();
}

function windowsBuiltInPowerShellPath() {
  return windowsSystemTool(join("WindowsPowerShell", "v1.0", "powershell.exe"));
}

function windowsCompanionVbs(bootstrap) {
  const powershell = windowsBuiltInPowerShellPath().replace(/"/gu, '""');
  return `CreateObject("WScript.Shell").Run """${powershell}"" -NoLogo -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""${String(bootstrap).replace(/"/gu, '""')}""", 0, False`;
}

function windowsSystemTool(name) {
  return join(windowsRoot(), "System32", name);
}

function windowsRoot() {
  return process.env.SystemRoot || process.env.WINDIR || "C:\\Windows";
}

function shellName() {
  return requestedShell || (process.platform === "win32" ? "powershell.exe" : process.env.SHELL || "/bin/sh");
}

function hostLabel() {
  return process.env.COMPUTERNAME || process.env.HOSTNAME || basename(homedir()) || "Компьютер";
}

function isWindowsSystem() {
  if (process.platform !== "win32") return false;
  try {
    const identity = execFileSync(windowsSystemTool("whoami.exe"), ["/user", "/fo", "csv", "/nh"], { encoding: "utf8", timeout: 2_000, windowsHide: true });
    return /(?:^|[,\s"])s-1-5-18(?:$|[,\s"])/iu.test(identity);
  } catch {
    const profile = resolve(String(process.env.USERPROFILE || "")).toLowerCase();
    const systemProfile = resolve(String(process.env.SystemRoot || "C:\\Windows"), "System32", "config", "systemprofile").toLowerCase();
    return profile === systemProfile;
  }
}

function psQuote(value) {
  return `'${String(value || "").replace(/'/gu, "''")}'`;
}

function shQuote(value) {
  return `'${String(value || "").replace(/'/gu, `'"'"'`)}'`;
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function redactSecrets(value) {
  let text = String(value || "");
  for (const secret of [connectorToken]) {
    if (typeof secret === "string" && secret.length >= 8) text = text.replaceAll(secret, "<redacted>");
  }
  try { text = spreadExMl.redact(text); } catch { /* Integration may still be initializing. */ }
  return text;
}

function sleep(ms) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

function arg(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || "") : "";
}

function flag(name) {
  return process.argv.includes(name);
}

function env(primary, compatibility = "") {
  return String(process.env[primary] || (compatibility ? process.env[compatibility] : "") || "");
}
