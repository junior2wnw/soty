#!/usr/bin/env node
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { defaultGonkaModel, gonkaModelLimitsFor, openCodeReleaseManifest } from "./agent-modules/opencode-release.mjs";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const sourcePath = join(root, "scripts", "soty-connector.mjs");
const sourceDir = dirname(sourcePath);
const modulesDir = resolve(sourceDir, "agent-modules");
const outputDir = join(root, "public", "agent");
const connectorOutput = join(outputDir, "soty-connector.mjs");
const compatibilityOutput = join(outputDir, "soty-agent.mjs");
const manifestPath = join(outputDir, "manifest.json");
const windowsMachineCmdPath = join(outputDir, "install-windows-machine.cmd");
const windowsReinstallDir = join(outputDir, "windows-reinstall");
const spreadExMlReleasePath = join(root, "release", "spreadex-ml.json");

const trafficCore = {
  schema: "soty.traffic-core.release.v1",
  engine: "xray-core",
  version: "26.7.11",
  platforms: {
    "win32-x64": {
      url: "/agent/core/xray-windows-x64-26.7.11.zip",
      officialUrl: "https://github.com/XTLS/Xray-core/releases/download/v26.7.11/Xray-windows-64.zip",
      sha256: "af801b62c4d41d248d3db8016d4c6e2a7ccfb7ed443e3738aeb6f9e062321512",
      executable: "xray.exe"
    },
    "linux-x64": {
      officialUrl: "https://github.com/XTLS/Xray-core/releases/download/v26.7.11/Xray-linux-64.zip",
      sha256: "aa11c3685c71da0ffc71e511db50404609e7e963bb914b048f59a6a00af8930e",
      executable: "xray"
    },
    "linux-arm64": {
      officialUrl: "https://github.com/XTLS/Xray-core/releases/download/v26.7.11/Xray-linux-arm64-v8a.zip",
      sha256: "89cfe01674d7c9f6847b7dd9389537be9acb3b9dc3c6cb9fdeba87a3e4e57fc1",
      executable: "xray"
    },
    "darwin-x64": {
      officialUrl: "https://github.com/XTLS/Xray-core/releases/download/v26.7.11/Xray-macos-64.zip",
      sha256: "d8c116756d3a88a38a833a94bdf8bc801f69243ee888befcb56df8b4f1ec4878",
      executable: "xray"
    },
    "darwin-arm64": {
      officialUrl: "https://github.com/XTLS/Xray-core/releases/download/v26.7.11/Xray-macos-arm64-v8a.zip",
      sha256: "61f8f74d099098af710fa43613d9934d97b901dee909801d34f496cd463956d1",
      executable: "xray"
    }
  }
};

const reinstallSpecs = [
  ["managed", "soty-managed-windows-reinstall.ps1"],
  ["prepare", "soty-prepare-windows-reinstall.ps1"],
  ["arm", "soty-arm-windows-reinstall.ps1"],
  ["makeFastUsb", "soty-make-fast-usb.ps1"]
];

const source = (await readFile(sourcePath, "utf8")).replace(/\r\n/g, "\n");
const bundled = await bundleLocalModules(source);
const version = bundled.match(/connectorVersion\s*=\s*"([^"]+)"/u)?.[1];
if (!version) throw new Error("Connector version not found");

await mkdir(outputDir, { recursive: true });
await writeFile(connectorOutput, bundled, { mode: 0o755 });
await chmod(connectorOutput, 0o755).catch(() => undefined);
// One compatibility asset lets every installed 0.x runtime migrate through its
// existing updater. It contains the connector byte-for-byte, not the old agent.
await writeFile(compatibilityOutput, bundled, { mode: 0o755 });
await chmod(compatibilityOutput, 0o755).catch(() => undefined);
await normalizeInstallerAssets();
await updateWindowsMachineInstallerRevision(version);
const windowsReinstall = await publishWindowsReinstallScripts();
const spreadexMl = await buildSpreadExMlRelease();

const manifest = {
  schema: "soty.connector.release.v1",
  version,
  architecture: "durable-jobs+opencode+server-gonka-proxy",
  connectorUrl: "/agent/soty-connector.mjs",
  agentUrl: "/agent/soty-connector.mjs",
  compatibilityUrl: "/agent/soty-agent.mjs",
  sha256: sha256(bundled),
  protocol: {
    schema: "soty.connector-job.v2",
    entities: ["device", "link", "thread", "job"],
    delivery: "lease+retry",
    events: "ordered+resumable",
    cancellation: "cooperative+process-tree",
    authentication: "per-installation-bearer-token"
  },
  agent: {
    id: "opencode",
    provider: "gonka",
    model: defaultGonkaModel,
    modelLimits: gonkaModelLimitsFor(defaultGonkaModel),
    transport: "authenticated-server-proxy",
    endpoint: "/api/connectors/gonka/v1/chat/completions",
    runtime: openCodeReleaseManifest
  },
  trafficCore,
  spreadexMl,
  windowsReinstall
};

await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
process.stdout.write(`connector:${version}:${manifest.sha256}\n`);
process.stdout.write(`connector-bytes:${Buffer.byteLength(bundled)}\n`);

async function bundleLocalModules(sourceText) {
  const pattern = /^import\s+\{\s*([^}]+?)\s*\}\s+from\s+["'](\.\/agent-modules\/[^"']+\.mjs)["'];\n?/gmu;
  const seen = new Set();
  return await replaceAsync(sourceText, pattern, async (statement, imports, specifier) => {
    if (seen.has(specifier)) throw new Error(`Duplicate connector module: ${specifier}`);
    seen.add(specifier);
    const modulePath = resolve(sourceDir, specifier);
    if (modulePath !== modulesDir && !modulePath.startsWith(`${modulesDir}${sep}`)) throw new Error(`Module outside connector modules: ${specifier}`);
    const moduleSource = (await readFile(modulePath, "utf8")).replace(/\r\n/g, "\n");
    if (/^import\s/mu.test(moduleSource)) throw new Error(`Nested imports are not supported: ${specifier}`);
    const exported = [...moduleSource.matchAll(/^export\s+(?:function|const|let|var|class)\s+([A-Za-z_$][\w$]*)/gmu)].map((match) => match[1]);
    const imported = imports.split(",").map((item) => item.trim()).filter(Boolean);
    for (const name of imported) {
      if (!exported.includes(name)) throw new Error(`${specifier} does not export ${name}`);
    }
    const body = moduleSource.replace(/^export\s+(?=(?:function|const|let|var|class)\b)/gmu, "");
    return `// bundled connector module: ${specifier}\nconst { ${imported.join(", ")} } = (() => {\n${body}\nreturn { ${imported.join(", ")} };\n})();\n`;
  });
}

async function replaceAsync(value, pattern, replacer) {
  const parts = [];
  let cursor = 0;
  for (const match of value.matchAll(pattern)) {
    parts.push(value.slice(cursor, match.index));
    parts.push(await replacer(...match));
    cursor = match.index + match[0].length;
  }
  parts.push(value.slice(cursor));
  return parts.join("");
}

async function updateWindowsMachineInstallerRevision(version) {
  if (!existsSync(windowsMachineCmdPath)) return;
  const text = await readFile(windowsMachineCmdPath, "utf8");
  const next = text
    .replace(/soty-agent-machine-bootstrap:[^\r\n]+/u, `soty-agent-machine-bootstrap:${version}`)
    .replace(/soty-connector-machine-bootstrap:[^\r\n]+/u, `soty-agent-machine-bootstrap:${version}`)
    .replace(/set "INSTALLER_REVISION=[^"]*"/u, `set "INSTALLER_REVISION=${version}"`);
  if (next !== text) await writeFile(windowsMachineCmdPath, next);
}

// Public installers are copied unchanged by Vite. Windows checkouts may supply
// CRLF, which breaks the Unix shell installer and changes published hashes.
// Canonical LF matches the existing published assets on every build platform.
async function normalizeInstallerAssets() {
  for (const fileName of [
    "install-macos-linux.sh",
    "install-windows.ps1",
    "install-windows-machine-bootstrap.ps1",
    "install-windows-machine.cmd"
  ]) {
    const path = join(outputDir, fileName);
    const text = await readFile(path, "utf8");
    const normalized = text.replace(/\r\n/g, "\n");
    if (normalized !== text) await writeFile(path, normalized);
  }
}

async function publishWindowsReinstallScripts() {
  await mkdir(windowsReinstallDir, { recursive: true });
  const scripts = [];
  for (const [name, fileName] of reinstallSpecs) {
    const sourceFile = join(root, "scripts", "windows", fileName);
    if (!existsSync(sourceFile)) throw new Error(`Windows reinstall script not found: ${sourceFile}`);
    const bytes = Buffer.from((await readFile(sourceFile, "utf8")).replace(/\r\n/g, "\n"), "utf8");
    await writeFile(join(windowsReinstallDir, fileName), bytes, { mode: 0o755 });
    scripts.push({ name, url: `/agent/windows-reinstall/${fileName}`, sha256: sha256(bytes), bytes: bytes.length });
  }
  return { scriptsBaseUrl: "/agent/windows-reinstall/", scripts };
}

async function buildSpreadExMlRelease() {
  if (!existsSync(spreadExMlReleasePath)) {
    return {
      schema: "soty.spreadex-ml.release.v1",
      available: false,
      reason: "platform-worker-not-published",
      featureSchema: "spreadex.ml.features.v1",
      signatureRequired: true,
      signatureAlgorithm: "Ed25519",
      publicKeyId: "spreadex-ml-release-v1",
      platforms: {}
    };
  }
  const value = JSON.parse(await readFile(spreadExMlReleasePath, "utf8"));
  if (value?.schema !== "soty.spreadex-ml.release.v1" || value.available !== true) throw new Error("Invalid SpreadEx ML release schema");
  if (!/^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/u.test(String(value.version || ""))) throw new Error("Invalid SpreadEx ML release version");
  if (!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$/u.test(String(value.modelVersion || ""))) throw new Error("Invalid SpreadEx ML model version");
  if (!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$/u.test(String(value.featureSchema || ""))) throw new Error("Invalid SpreadEx ML feature schema");
  if (!/^[A-Za-z0-9_+/=-]{64,512}$/u.test(String(value.signature || ""))) throw new Error("SpreadEx ML release must have an Ed25519 signature");
  const platforms = value.platforms && typeof value.platforms === "object" ? value.platforms : {};
  if (!Object.keys(platforms).length) throw new Error("SpreadEx ML release has no platforms");
  for (const [platform, asset] of Object.entries(platforms)) {
    if (!/^(?:win32|linux|darwin)-(?:x64|arm64)$/u.test(platform)) throw new Error(`Invalid SpreadEx ML platform: ${platform}`);
    if (typeof asset?.url !== "string" || !asset.url || !/^[a-f0-9]{64}$/u.test(String(asset.sha256 || "").toLowerCase())) throw new Error(`Invalid SpreadEx ML asset: ${platform}`);
    if (!/^[A-Za-z0-9][A-Za-z0-9_.\/-]{0,200}$/u.test(String(asset.executable || "")) || String(asset.executable).includes("..")) throw new Error(`Invalid SpreadEx ML executable: ${platform}`);
  }
  return value;
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}
