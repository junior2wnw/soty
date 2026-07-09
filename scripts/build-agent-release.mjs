#!/usr/bin/env node
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { buildAgentRuntimeManifest, defaultAgentRuntimeCapabilities } from "trustlink-kernel";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const sourcePath = join(root, "scripts", "soty-agent.mjs");
const sourceDir = dirname(sourcePath);
const localAgentModulesDir = resolve(sourceDir, "agent-modules");
const outputDir = join(root, "public", "agent");
const outputPath = join(outputDir, "soty-agent.mjs");
const manifestPath = join(outputDir, "manifest.json");
const windowsMachineCmdPath = join(outputDir, "install-windows-machine.cmd");
const windowsReinstallDir = join(outputDir, "windows-reinstall");
const windowsReinstallRouteProfileId = "soty.os.reinstall.v1";
const generatedAssetRouteProfileId = "soty.artifact.wallpaper.v1";
const retiredOpsSkillArtifacts = [
  join(outputDir, "ops-skill.zip"),
  join(outputDir, "ops-skill.tar.gz")
];
const windowsReinstallScriptSpecs = [
  {
    name: "managed",
    fileName: "soty-managed-windows-reinstall.ps1",
    sourcePath: join(root, "scripts", "windows", "soty-managed-windows-reinstall.ps1")
  },
  {
    name: "prepare",
    fileName: "soty-prepare-windows-reinstall.ps1",
    sourcePath: join(root, "scripts", "windows", "soty-prepare-windows-reinstall.ps1")
  },
  {
    name: "arm",
    fileName: "soty-arm-windows-reinstall.ps1",
    sourcePath: join(root, "scripts", "windows", "soty-arm-windows-reinstall.ps1")
  },
  {
    name: "makeFastUsb",
    fileName: "soty-make-fast-usb.ps1",
    sourcePath: join(root, "scripts", "windows", "soty-make-fast-usb.ps1")
  }
];
const excludedRuntimeFamilies = new Set(["surface"]);

const source = await readFile(sourcePath, "utf8");
const sourceText = await bundleAgentSource(source.replace(/\r\n/g, "\n"));
const version = sourceText.match(/agentVersion\s*=\s*"([^"]+)"/u)?.[1];
if (!version) {
  throw new Error("Agent version not found");
}

await mkdir(outputDir, { recursive: true });
await writeFile(outputPath, sourceText, { mode: 0o755 });
await updateWindowsMachineInstallerRevision(version);
await removeRetiredOpsSkillArtifacts();
const windowsReinstall = await publishWindowsReinstallScripts();
const routeProfiles = buildRouteProfiles(windowsReinstall);
const agentRuntime = buildSotyAgentRuntime();
const automationToolkits = buildAutomationToolkits(windowsReinstall, routeProfiles, agentRuntime);
const openAiToolPlane = buildOpenAiToolPlane();

const manifest = {
  version,
  schema: "soty.agent.release.v2",
  architecture: "gonka-direct-chat-completions+computer-tools+memory-plane",
  agentUrl: "/agent/soty-agent.mjs",
  sha256: sha256(sourceText),
  openAiToolPlane,
  memoryPlane: {
    schema: "soty.memory-plane.v1",
    controller: "soty.memctl.v1",
    backend: "append-only-jsonl",
    querySchema: "soty.memory.query.v2",
    reportSchema: "soty.memory.report.v2",
    routeProfileSchema: "soty.route-profiles.v1",
    healthUrl: "/api/agent/memory/health",
    queryUrl: "/api/agent/memory/query",
    receiptsUrl: "/api/agent/memory/receipts",
    reportUrl: "/api/agent/memory/report"
  },
  computerUsePlane: {
    schema: "soty.computer-use-plane.v1",
    entryTool: "computer",
    legacyEntrypoint: "soty_computer",
    mcpTools: ["computer"],
    standardTools: ["computer"],
    openAiBuiltInTools: openAiToolPlane.builtInTools,
    model: "discover+invoke+durable-jobs+artifacts+source-proof",
    imagePipeline: "openai.image_generation+computer.artifact-save-apply-verify",
    agentRuntimeSchema: agentRuntime.schema,
    routeProfileSchema: "soty.route-profiles.v1",
    capabilities: [
      "discover",
      "status",
      "shell",
      "script",
      "durable-action",
      "turnkey-monitoring",
      "filesystem",
      "soty-room-file-download",
      "artifact",
      "web",
      "network",
      "traffic",
      "process",
      "clipboard",
      "browser",
      "desktop",
      "screen",
      "keyboard",
      "mouse",
      "wallpaper",
      "audio",
      "app",
      "api",
      "transaction",
      "generated-asset-save-apply-verify",
      "managed-windows-reinstall"
    ]
  },
  agentRuntime,
  routeProfiles,
  windowsReinstall,
  automationToolkits
};

await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
process.stdout.write(`agent:${version}:${manifest.sha256}\n`);
process.stdout.write(`memory-plane:${manifest.memoryPlane.schema}\n`);
process.stdout.write(`computer-use-plane:${manifest.computerUsePlane.schema}\n`);
process.stdout.write(`agent-runtime:${manifest.agentRuntime.schema}\n`);
process.stdout.write(`windows-reinstall:${windowsReinstall.scripts.map((script) => `${script.name}:${script.sha256}`).join(",")}\n`);

async function removeRetiredOpsSkillArtifacts() {
  await Promise.all(retiredOpsSkillArtifacts.map((path) => rm(path, { force: true }).catch(() => undefined)));
}

async function bundleAgentSource(sourceText) {
  const localModuleImport = /^import\s+\{\s*([^}]+?)\s*\}\s+from\s+["'](\.\/agent-modules\/[^"']+\.mjs)["'];\n?/gmu;
  const seen = new Set();
  let bundled = sourceText;
  for (;;) {
    let changed = false;
    bundled = await replaceAsync(bundled, localModuleImport, async (statement, imports, specifier) => {
      changed = true;
      if (seen.has(specifier)) {
        throw new Error(`Duplicate local agent module import: ${specifier}`);
      }
      seen.add(specifier);
      const modulePath = resolve(sourceDir, specifier);
      if (modulePath !== localAgentModulesDir && !modulePath.startsWith(`${localAgentModulesDir}${sep}`)) {
        throw new Error(`Local agent module import is outside agent-modules: ${specifier}`);
      }
      const moduleSource = (await readFile(modulePath, "utf8")).replace(/\r\n/g, "\n");
      if (/^import\s/mu.test(moduleSource)) {
        throw new Error(`Nested imports are not supported in local agent module: ${specifier}`);
      }
      const exportedNames = [...moduleSource.matchAll(/^export\s+(?:function|const|let|var|class)\s+([A-Za-z_$][\w$]*)/gmu)].map((match) => match[1]);
      for (const name of imports.split(",").map((part) => part.trim()).filter(Boolean)) {
        if (/\s+as\s+/u.test(name)) {
          throw new Error(`Local agent module import aliases are not supported: ${statement.trim()}`);
        }
        const importedName = name.split(/\s+as\s+/u)[0]?.trim();
        if (!exportedNames.includes(importedName)) {
          throw new Error(`Local agent module ${specifier} does not export ${importedName}`);
        }
      }
      const body = moduleSource.replace(/^export\s+(?=(?:function|const|let|var|class)\b)/gmu, "");
      return `// bundled local agent module: ${specifier}\n${body}\n`;
    });
    if (!changed) {
      break;
    }
  }
  if (/^import\s+\{[^}]+?\}\s+from\s+["']\.\/agent-modules\//mu.test(bundled)) {
    throw new Error("Unbundled local agent module import remains");
  }
  return bundled;
}

async function replaceAsync(value, pattern, replacer) {
  const parts = [];
  let lastIndex = 0;
  for (const match of value.matchAll(pattern)) {
    parts.push(value.slice(lastIndex, match.index));
    parts.push(await replacer(...match));
    lastIndex = match.index + match[0].length;
  }
  parts.push(value.slice(lastIndex));
  return parts.join("");
}

async function updateWindowsMachineInstallerRevision(version) {
  if (!existsSync(windowsMachineCmdPath)) {
    return;
  }
  const text = await readFile(windowsMachineCmdPath, "utf8");
  const next = text
    .replace(/soty-agent-machine-bootstrap:[^\r\n]+/u, `soty-agent-machine-bootstrap:${version}`)
    .replace(/set "INSTALLER_REVISION=[^"]*"/u, `set "INSTALLER_REVISION=${version}"`);
  if (next !== text) {
    await writeFile(windowsMachineCmdPath, next);
  }
}

async function publishWindowsReinstallScripts() {
  await mkdir(windowsReinstallDir, { recursive: true });
  const scripts = [];
  for (const spec of windowsReinstallScriptSpecs) {
    if (!existsSync(spec.sourcePath)) {
      throw new Error(`Windows reinstall script not found: ${spec.sourcePath}`);
    }
    const bytes = await readFile(spec.sourcePath);
    const outputFile = join(windowsReinstallDir, spec.fileName);
    await writeFile(outputFile, bytes, { mode: 0o755 });
    scripts.push({
      name: spec.name,
      url: `/agent/windows-reinstall/${spec.fileName}`,
      sha256: sha256(bytes),
      bytes: bytes.length
    });
  }
  return {
    scriptsBaseUrl: "/agent/windows-reinstall/",
    scripts
  };
}

function buildOpenAiToolPlane() {
  return {
    schema: "openai.responses-tools+mcp.v1",
    centralResolver: "gonka-direct-chat-completions",
    builtInTools: ["web_search", "image_generation", "computer_use_preview", "code_interpreter", "shell", "apply_patch"],
    codexCliFeatureFlags: [],
    webSearch: "native web_search when available; otherwise computer.operation=web on the selected computer",
    mcp: {
      server: "soty",
      entryTool: "computer",
      publicTools: ["computer"],
      legacyAliasesHidden: true
    },
    providerAdapter: {
      role: "direct-agent-transport",
      syntheticToolCallsDefault: false,
      directComputerRecoveryDefault: false,
      directAgentDefault: true,
      codexCliBypassedDefault: true
    },
    rule: "do not reimplement or shadow OpenAI built-in tools as Soty MCP tools"
  };
}

function buildRouteProfiles(windowsReinstall) {
  const scriptProof = windowsReinstall.scripts.map((script) => ({
    name: script.name,
    sha256: script.sha256,
    bytes: script.bytes
  }));
  return {
    schema: "soty.route-profiles.v1",
    model: "memory-derived-route-profile+first-class-capability",
    promotionPolicy: {
      candidateAfter: "one proofed run",
      provenAfter: "two compatible successful runs without newer conflicting failure",
      promotedInto: "manifest-pinned capability, proof checks, eval/selftest"
    },
    profiles: [
      {
        id: windowsReinstallRouteProfileId,
        family: "windows-reinstall",
        title: "Windows reinstall capability profile",
        entryTool: "computer",
        capability: "os-reinstall",
        legacyTool: "soty_reinstall",
        defaultOperation: "reinstall",
        defaultAction: "prepare",
        context: "windows-machine-worker",
        phases: ["preflight", "prepare", "status", "repair", "cancel", "arm"],
        route: [
          "prove selected source device and machine/system worker",
          "resume stale prepare state before starting managed prepare",
          "run repair/status when the user reports a broken or interrupted reinstall workflow",
          "ask clean vs keep-files and require explicit USB-use consent before a new prepare",
          "start managed prepare once with stable idempotency",
          "download Windows media with the guarded parallel/resumable route on the selected PC",
          "prove backup, install media, unattended account, Autounattend, postinstall",
          "ask final reinstall confirmation only after proof is complete",
          "arm reinstall and stop probing while reboot return path is expected"
        ],
        doNot: [
          "do not ask the user to manually download ISO when the source computer is attached",
          "do not open Microsoft download pages as the normal route",
          "do not replace the managed downloader with ad-hoc browser automation",
          "do not start a second prepare while one is active",
          "do not treat stale orphaned prepare jobs as active blockers",
          "do not answer reinstall failure reports from memory without fresh repair/status proof"
        ],
        proof: ["machineWorker", "scriptSha256", "mediaSha256", "backupProof", "installMedia", "autounattend", "setupcomplete", "repairProof", "cancelProof", "postArmReturnPath"],
        scripts: scriptProof,
        learning: {
          reuseKey: windowsReinstallRouteProfileId,
          scriptUse: "prepare/status/repair/cancel/arm",
          successCriteria: "backupProof+installMedia+unattend+postinstall",
          contextFingerprint: "windows-machine-worker",
          receipt: "append-only sanitized route proof"
        }
      },
      {
        id: generatedAssetRouteProfileId,
        family: "generated-image-wallpaper",
        title: "Native image generation to source-device wallpaper",
        entryTool: "computer",
        capability: "generated-asset-save-apply-verify",
        defaultOperation: "artifact",
        defaultAction: "wallpaper",
        context: "codex-generated-image+source-user-desktop",
        phases: ["generate-native", "artifact", "wallpaper", "verify"],
        route: [
          "generate the image with native OpenAI image_gen/image_generation",
          "use the exact generated artifact path when the native image tool exposes one",
          "send the exact bytes with computer operation=artifact using an explicit localPath and targetPath",
          "apply with computer operation=wallpaper or desktop action=wallpaper using the saved source-device path",
          "verify with source-device proof: ok=true, the current wallpaper path equals the requested source-device path, and file SHA-256/bytes"
        ],
        doNot: [
          "do not use curl, wget, public upload hosts, temporary HTTP servers, or pasted base64 for generated images",
          "do not ask for OPENAI_API_KEY on the source device",
          "do not replace the generated artifact with a stock/public image",
          "do not check desktop/display before native generation just to choose size"
        ],
        proof: ["localPath", "targetPath", "artifactSha256", "bytes", "wallpaperPath", "currentWallpaper", "display"],
        learning: {
          reuseKey: generatedAssetRouteProfileId,
          scriptUse: "image_gen/artifact/wallpaper/verify",
          successCriteria: "nativeGeneratedArtifact+sourceSavedBytes+wallpaperApplied+sourceProof",
          contextFingerprint: "codex-generated-image+source-user-desktop",
          receipt: "append-only sanitized route proof"
        }
      }
    ]
  };
}

function buildSotyAgentRuntime() {
  return buildAgentRuntimeManifest({
    runtimeId: "soty-agent",
    entrypoint: "computer",
    capabilities: [
      ...defaultAgentRuntimeCapabilities()
        .filter((capability) => !excludedRuntimeFamilies.has(capability.family))
    ]
  });
}

function buildAutomationToolkits(windowsReinstall, routeProfiles, agentRuntime) {
  const openAiToolPlane = buildOpenAiToolPlane();
  return {
    schema: "soty.automation-toolkits.v2",
    architecture: "gonka-direct-chat-completions+computer-use-plane",
    policy: {
      centralResolver: "gonka-direct-chat-completions",
      entrypoint: "computer",
      legacyEntrypoint: "soty_computer",
      route: "computer-use-plane-with-memory-hints",
      jobKernel: "durable-jobs",
      routeProfiles: "soty.route-profiles.v1",
      agentRuntime: agentRuntime.schema,
      chat: "agent-sysadmin",
      responseStyle: buildResponseStylePolicy(),
      openAiToolPlane,
      diagnostics: {
        trace: "soty.agent.trace.v1",
        eval: "soty-agent-eval"
      },
      terminalStates: ["completed", "failed", "blocked-needs-user", "waiting-confirmation"]
    },
    toolkits: [
      {
        name: "agent-runtime",
        entryTool: "computer",
        kind: "runtime-contract",
        phases: ["discover", "invoke", "prepare", "confirm", "status", "stop", "learn"],
        proof: ["capability", "risk", "confirmation", "jobId", "result", "proof"],
        schema: agentRuntime.schema,
        capabilities: agentRuntime.capabilities.map((capability) => capability.family)
      },
      {
        name: "computer-use-plane",
        entryTool: "computer",
        kind: "front-door",
        phases: ["discover", "route_profiles", "status", "invoke", "jobs", "job_status", "wait", "job_stop"],
        proof: ["sourceDeviceId", "jobId", "statusPath", "resultPath", "exitCode", "artifactSha256"],
        promotion: "Soty MCP computer-use capability for Server Codex; OpenAI built-in tools stay native and are not reimplemented as Soty tools.",
        routeProfiles: routeProfiles.profiles.map((profile) => profile.id)
      },
      {
        name: "capability-gateway",
        entryTool: "computer",
        kind: "legacy-alias",
        phases: ["describe", "start", "status", "stop", "list", "reinstall"],
        proof: ["toolkit", "phase", "jobId", "statusPath", "resultPath", "proof"],
        promotion: "Thin, proofed computer-control plane for Server Codex."
      },
      {
        name: "durable-action",
        entryTool: "jobs",
        kind: "generic-kernel",
        phases: ["start", "status", "wait", "stop"],
        proof: ["jobId", "statusPath", "resultPath", "proof"],
        promotion: "Durable supervised execution for long or repeatable jobs."
      },
      {
        name: "generated-asset",
        entryTool: "computer",
        kind: "managed-toolkit",
        phases: ["image_gen", "artifact", "wallpaper", "verify"],
        proof: ["localPath", "targetPath", "artifactSha256", "bytes", "wallpaperPath", "currentWallpaper", "display"],
        routeProfile: generatedAssetRouteProfileId,
        promotion: "Native OpenAI image generation with Soty artifact transfer and source desktop wallpaper proof."
      },
      {
        name: "windows-reinstall",
        entryTool: "computer",
        kind: "managed-toolkit",
        phases: ["preflight", "prepare", "status", "repair", "cancel", "arm"],
        scriptSet: "windowsReinstall",
        scripts: windowsReinstall.scripts.map((script) => ({
          name: script.name,
          sha256: script.sha256,
          bytes: script.bytes
        })),
        proof: ["backupProof", "installMedia", "unattend", "postinstall", "repairProof", "cancelProof", "rebooting"],
        routeProfile: windowsReinstallRouteProfileId
      }
    ],
    routeProfiles
  };
}

function buildResponseStylePolicy() {
  return {
    schema: "soty.response-style.v1",
    id: "agent-sysadmin",
    displayName: "Агент",
    base: "agent",
    tone: "brief-sysadmin",
    maxUserFacingLines: 0,
    phraseBank: []
  };
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}
