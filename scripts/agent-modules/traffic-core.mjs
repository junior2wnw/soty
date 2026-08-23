export const trafficCoreSchema = "soty.traffic-core.v1";

export function createTrafficCoreRuntime(deps = {}) {
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

export function normalizeBridgeSettings(value) {
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

export function buildTrafficBridgeConfig(settings) {
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

export function buildTrafficClientUri(value) {
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
