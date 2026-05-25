#!/usr/bin/env node
import { existsSync } from "node:fs";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildAgentReleaseManifest, sha256 } from "./agent-release/manifest.mjs";
import { createAgentReleasePaths } from "./agent-release/paths.mjs";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const {
  sourcePath,
  outputDir,
  outputPath,
  manifestPath,
  windowsMachineCmdPath,
  windowsReinstallDir,
  retiredOpsSkillArtifacts,
  windowsReinstallScriptSpecs
} = createAgentReleasePaths(root);
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
const manifest = buildAgentReleaseManifest({ version, sourceText, windowsReinstall });

await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
process.stdout.write(`agent:${version}:${manifest.sha256}\n`);
process.stdout.write(`memory-plane:${manifest.memoryPlane.schema}\n`);
process.stdout.write(`computer-use-plane:${manifest.computerUsePlane.schema}\n`);
process.stdout.write(`agent-runtime:${manifest.agentRuntime.schema}\n`);
process.stdout.write(`windows-reinstall:${windowsReinstall.scripts.map((script) => `${script.name}:${script.sha256}`).join(",")}\n`);

async function removeRetiredOpsSkillArtifacts() {
  await Promise.all(retiredOpsSkillArtifacts.map((path) => rm(path, { recursive: true, force: true }).catch(() => undefined)));
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
    const bytes = await readNormalizedScriptBytes(spec.sourcePath);
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

async function readNormalizedScriptBytes(path) {
  const text = await readFile(path, "utf8");
  return Buffer.from(text.replace(/\r\n/g, "\n"), "utf8");
}
