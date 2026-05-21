#!/usr/bin/env node
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildAgentReleaseManifest, sha256 } from "./manifest.mjs";
import { createAgentReleasePaths } from "./paths.mjs";

const root = dirname(dirname(dirname(fileURLToPath(import.meta.url))));
const {
  sourcePath,
  outputPath,
  manifestPath,
  windowsMachineCmdPath,
  windowsReinstallDir,
  retiredOpsSkillArtifacts,
  windowsReinstallScriptSpecs
} = createAgentReleasePaths(root);
const errors = [];

const sourceText = (await readFile(sourcePath, "utf8")).replace(/\r\n/g, "\n");
const version = sourceText.match(/agentVersion\s*=\s*"([^"]+)"/u)?.[1];
if (!version) {
  fail("Agent version not found in scripts/soty-agent.mjs");
}

const publishedAgent = await readRequiredText(outputPath, "published agent");
if (publishedAgent !== sourceText) {
  fail("public/agent/soty-agent.mjs is not the normalized scripts/soty-agent.mjs");
}

const windowsReinstall = {
  scriptsBaseUrl: "/agent/windows-reinstall/",
  scripts: []
};
for (const spec of windowsReinstallScriptSpecs) {
  const sourceBytes = await readRequiredBytes(spec.sourcePath, `source ${spec.fileName}`);
  const publishedPath = join(windowsReinstallDir, spec.fileName);
  const publishedBytes = await readRequiredBytes(publishedPath, `published ${spec.fileName}`);
  if (sourceBytes && publishedBytes && Buffer.compare(sourceBytes, publishedBytes) !== 0) {
    fail(`public/agent/windows-reinstall/${spec.fileName} differs from scripts/windows/${spec.fileName}`);
  }
  if (sourceBytes) {
    windowsReinstall.scripts.push({
      name: spec.name,
      url: `/agent/windows-reinstall/${spec.fileName}`,
      sha256: sha256(sourceBytes),
      bytes: sourceBytes.length
    });
  }
}

const manifestText = await readRequiredText(manifestPath, "agent manifest");
if (manifestText && version) {
  const expectedManifest = buildAgentReleaseManifest({ version, sourceText, windowsReinstall });
  const expectedText = `${JSON.stringify(expectedManifest, null, 2)}\n`;
  if (manifestText !== expectedText) {
    fail("public/agent/manifest.json is not generated from current release inputs");
  }
}

if (existsSync(windowsMachineCmdPath) && version) {
  const installerText = await readFile(windowsMachineCmdPath, "utf8");
  if (!installerText.includes(`soty-agent-machine-bootstrap:${version}`) || !installerText.includes(`INSTALLER_REVISION=${version}`)) {
    fail("install-windows-machine.cmd installer revision does not match the agent version");
  }
}

for (const artifactPath of retiredOpsSkillArtifacts) {
  if (existsSync(artifactPath)) {
    fail(`retired release artifact still exists: ${artifactPath}`);
  }
}

if (errors.length) {
  process.stderr.write(`agent release check failed:\n${errors.map((error) => `- ${error}`).join("\n")}\n`);
  process.exitCode = 1;
} else {
  process.stdout.write(`agent release check ok:${version}:${sha256(sourceText)}\n`);
}

function fail(message) {
  errors.push(message);
}

async function readRequiredText(path, label) {
  try {
    return await readFile(path, "utf8");
  } catch (error) {
    fail(`${label} missing or unreadable: ${path} (${error.message})`);
    return "";
  }
}

async function readRequiredBytes(path, label) {
  try {
    return await readFile(path);
  } catch (error) {
    fail(`${label} missing or unreadable: ${path} (${error.message})`);
    return null;
  }
}
