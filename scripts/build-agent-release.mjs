#!/usr/bin/env node
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const sourcePath = join(root, "scripts", "soty-agent.mjs");
const outputDir = join(root, "public", "agent");
const outputPath = join(outputDir, "soty-agent.mjs");
const manifestPath = join(outputDir, "manifest.json");
const windowsMachineCmdPath = join(outputDir, "install-windows-machine.cmd");
const windowsReinstallDir = join(outputDir, "windows-reinstall");
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

const source = await readFile(sourcePath, "utf8");
const sourceText = source.replace(/\r\n/g, "\n");
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
const automationToolkits = buildAutomationToolkits(windowsReinstall, routeProfiles);
const openAiToolPlane = buildOpenAiToolPlane();

const manifest = {
  version,
  schema: "soty.agent.release.v2",
  architecture: "server-codex-brain+openai-built-in-tools+soty-mcp-computer+memory-plane",
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
      "browser",
      "desktop",
      "screen",
      "keyboard",
      "mouse",
      "wallpaper",
      "audio",
      "generated-asset-save-apply-verify",
      "managed-windows-reinstall"
    ]
  },
  routeProfiles,
  windowsReinstall,
  automationToolkits
};

await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
process.stdout.write(`agent:${version}:${manifest.sha256}\n`);
process.stdout.write(`memory-plane:${manifest.memoryPlane.schema}\n`);
process.stdout.write(`computer-use-plane:${manifest.computerUsePlane.schema}\n`);
process.stdout.write(`windows-reinstall:${windowsReinstall.scripts.map((script) => `${script.name}:${script.sha256}`).join(",")}\n`);

async function removeRetiredOpsSkillArtifacts() {
  await Promise.all(retiredOpsSkillArtifacts.map((path) => rm(path, { force: true }).catch(() => undefined)));
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
    builtInTools: ["web_search", "image_generation", "computer_use_preview", "code_interpreter", "shell", "apply_patch"],
    codexCliFeatureFlags: ["image_generation", "computer_use", "browser_use", "tool_search"],
    webSearch: "native --search",
    mcp: {
      server: "soty",
      entryTool: "computer",
      publicTools: ["computer"],
      legacyAliasesHidden: true
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
        id: "soty-windows-reinstall-managed-fast-lane",
        family: "windows-reinstall",
        title: "Managed Windows reinstall fast lane",
        entryTool: "computer",
        capability: "os-reinstall",
        legacyTool: "soty_reinstall",
        defaultOperation: "reinstall",
        defaultAction: "prepare",
        context: "windows-machine-worker",
        phases: ["preflight", "prepare", "status", "repair", "cancel", "arm"],
        route: [
          "prove selected source device and machine/system worker",
          "recover stale prepare state before starting managed prepare",
          "run repair/status when the user reports a broken or interrupted reinstall workflow",
          "start managed prepare once with stable idempotency",
          "download Windows media with the single-stream resumable route on the selected PC",
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
          reuseKey: "soty-windows-reinstall-managed-fast-lane",
          scriptUse: "prepare/status/repair/cancel/arm",
          successCriteria: "backupProof+installMedia+unattend+postinstall",
          contextFingerprint: "windows-machine-worker",
          receipt: "append-only sanitized route proof"
        }
      },
      {
        id: "soty-generated-asset-wallpaper-fast-lane",
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
          "use the exact newest generated_images artifact path when Codex did not expose a direct path",
          "push the exact bytes with computer operation=artifact localPath=/agent/codex-stock-home/generated_images/... targetPath=<source-device-path>",
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
          reuseKey: "soty-generated-asset-wallpaper-fast-lane",
          scriptUse: "image_gen/artifact/wallpaper/verify",
          successCriteria: "nativeGeneratedArtifact+sourceSavedBytes+wallpaperApplied+sourceProof",
          contextFingerprint: "codex-generated-image+source-user-desktop",
          receipt: "append-only sanitized route proof"
        }
      }
    ]
  };
}

function buildAutomationToolkits(windowsReinstall, routeProfiles) {
  const openAiToolPlane = buildOpenAiToolPlane();
  return {
    schema: "soty.automation-toolkits.v2",
    architecture: "openai-built-in-tools+soty-mcp-computer",
    policy: {
      entrypoint: "computer",
      legacyEntrypoint: "soty_computer",
      route: "computer-use-plane-with-memory-hints",
      fallbackKernel: "jobs",
      routeProfiles: "soty.route-profiles.v1",
      chat: "lord-sysadmin",
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
        promotion: "Thin, proofed computer-control surface for Server Codex."
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
        routeProfile: "soty-generated-asset-wallpaper-fast-lane",
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
        routeProfile: "soty-windows-reinstall-managed-fast-lane"
      }
    ],
    routeProfiles
  };
}

function buildResponseStylePolicy() {
  return {
    schema: "soty.response-style.v1",
    id: "lord-sysadmin",
    displayName: "Лорд",
    base: "agent",
    tone: "brief-sysadmin",
    maxUserFacingLines: 0,
    phraseBank: []
  };
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}
