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
await removeRetiredOpsSkillArtifacts();
const windowsReinstall = await publishWindowsReinstallScripts();
const routeProfiles = buildRouteProfiles(windowsReinstall);
const automationToolkits = buildAutomationToolkits(windowsReinstall, routeProfiles);

const manifest = {
  version,
  schema: "soty.agent.release.v2",
  architecture: "server-codex-brain+memory-plane+user-computer-use-plane",
  agentUrl: "/agent/soty-agent.mjs",
  sha256: sha256(sourceText),
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
    standardTools: ["computer", "image_gen", "artifact"],
    model: "discover+invoke+durable-jobs+artifacts+source-proof",
    imagePipeline: "image_gen+source-save-apply-verify",
    routeProfileSchema: "soty.route-profiles.v1"
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
        phases: ["preflight", "prepare", "status", "arm"],
        route: [
          "prove selected source device and machine/system worker",
          "start managed prepare once with stable idempotency",
          "download Windows media with resumable HTTP range route on the selected PC",
          "prove backup, install media, unattended account, Autounattend, postinstall",
          "ask destructive confirmation only after proof is complete",
          "arm reinstall and stop probing while reboot return path is expected"
        ],
        doNot: [
          "do not ask the user to manually download ISO when the source computer is attached",
          "do not open Microsoft download pages as the normal route",
          "do not replace the managed downloader with ad-hoc browser automation",
          "do not start a second prepare while one is active"
        ],
        proof: ["machineWorker", "scriptSha256", "mediaSha256", "backupProof", "installMedia", "autounattend", "setupcomplete", "postArmReturnPath"],
        scripts: scriptProof,
        learning: {
          reuseKey: "soty-windows-reinstall-managed-fast-lane",
          scriptUse: "prepare/status/arm",
          successCriteria: "backupProof+installMedia+unattend+postinstall",
          contextFingerprint: "windows-machine-worker",
          receipt: "append-only sanitized route proof"
        }
      }
    ]
  };
}

function buildAutomationToolkits(windowsReinstall, routeProfiles) {
  return {
    schema: "soty.automation-toolkits.v2",
    architecture: "computer-use-plane",
    policy: {
      entrypoint: "computer",
      legacyEntrypoint: "soty_computer",
      route: "computer-use-plane-with-memory-hints",
      fallbackKernel: "jobs",
      routeProfiles: "soty.route-profiles.v1",
      chat: "lord-sysadmin",
      responseStyle: buildResponseStylePolicy(),
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
        phases: ["discover", "route_profiles", "status", "invoke", "jobs", "job_status", "job_stop"],
        proof: ["sourceDeviceId", "jobId", "statusPath", "resultPath", "exitCode", "artifactSha256"],
        promotion: "Standard computer-use capability for Server Codex; legacy soty_* tools are hidden aliases.",
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
        phases: ["start", "status", "stop"],
        proof: ["jobId", "statusPath", "resultPath", "proof"],
        promotion: "Durable supervised execution for long or repeatable jobs."
      },
      {
        name: "windows-reinstall",
        entryTool: "computer",
        kind: "managed-toolkit",
        phases: ["preflight", "prepare", "status", "arm"],
        scriptSet: "windowsReinstall",
        scripts: windowsReinstall.scripts.map((script) => ({
          name: script.name,
          sha256: script.sha256,
          bytes: script.bytes
        })),
        proof: ["backupProof", "installMedia", "unattend", "postinstall", "rebooting"],
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
