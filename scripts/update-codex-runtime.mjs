#!/usr/bin/env node
import { createHash } from "node:crypto";
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const githubApi = "https://api.github.com/repos/openai/codex/releases/tags";
const npmRegistry = "https://registry.npmjs.org/@openai/codex/latest";

const args = new Set(process.argv.slice(2));
const versionArg = valueAfter("--version") || valueAfter("-v") || "";
const targetDir = resolve(valueAfter("--target") || process.env.SOTY_CODEX_RUNTIME_DIR || "/codex-runtime");
const printLatest = args.has("--print-latest");

const latest = await latestCodexVersion();
if (printLatest) {
  console.log(latest);
  process.exit(0);
}

const codexVersion = normalizeVersion(versionArg || latest);
const tag = `rust-v${codexVersion}`;
const release = await fetchJson(`${githubApi}/${encodeURIComponent(tag)}`);
const platform = runtimePlatform();
const asset = chooseAsset(release.assets || [], platform);
const downloadUrl = asset.browser_download_url;

const workDir = mkdtempSync(join(tmpdir(), "soty-codex-runtime-"));
const archivePath = join(workDir, asset.name);
const extractDir = join(workDir, "extract");
const stageDir = join(workDir, "stage");
mkdirSync(extractDir, { recursive: true });
mkdirSync(join(stageDir, "bin"), { recursive: true });

try {
  download(downloadUrl, archivePath);
  run("tar", ["-xzf", archivePath, "-C", extractDir]);

  const binary = findBinary(extractDir, binaryNameCandidates(asset.name, platform.binaryName));
  if (!binary) {
    throw new Error(`Could not find ${platform.binaryName} in ${asset.name}`);
  }

  const targetBinary = join(stageDir, "bin", platform.binaryName);
  copyFileSync(binary, targetBinary);
  chmodSync(targetBinary, 0o755);

  const manifest = {
    version: tag,
    codexVersion,
    target: platform.target,
    assetName: asset.name,
    downloadUrl,
    binaryRelativePath: `bin/${platform.binaryName}`,
    archiveSha256: sha256File(archivePath),
    binarySha256: sha256File(targetBinary),
    preparedAt: new Date().toISOString()
  };
  writeFileSync(join(stageDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);

  installStage(stageDir, targetDir);
  const check = spawnSync(join(targetDir, "bin", platform.binaryName), ["--version"], { encoding: "utf8" });
  if (check.status !== 0) {
    throw new Error(`Installed codex failed: ${(check.stderr || check.stdout || "").trim()}`);
  }
  console.log(`${check.stdout.trim()} installed at ${targetDir}`);
  console.log(`asset=${asset.name}`);
  console.log(`binarySha256=${manifest.binarySha256}`);
} finally {
  rmSync(workDir, { recursive: true, force: true });
}

function valueAfter(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] || "" : "";
}

function normalizeVersion(value) {
  return String(value || "").trim().replace(/^rust-v/u, "").replace(/^v/u, "");
}

async function latestCodexVersion() {
  const info = await fetchJson(npmRegistry);
  const version = String(info.version || "").trim();
  if (!version) {
    throw new Error("Could not resolve latest @openai/codex version");
  }
  return version;
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      "accept": "application/vnd.github+json, application/json",
      "user-agent": "soty-codex-runtime-updater"
    }
  });
  if (!response.ok) {
    throw new Error(`${url} returned HTTP ${response.status}`);
  }
  return response.json();
}

function runtimePlatform() {
  if (process.platform !== "linux") {
    throw new Error(`Only linux server runtimes are supported by this updater, got ${process.platform}`);
  }
  if (process.arch === "x64") {
    return {
      target: "linux-x64",
      binaryName: "codex",
      preferredAssets: [
        "codex-x86_64-unknown-linux-musl.tar.gz",
        "codex-x86_64-unknown-linux-gnu.tar.gz"
      ]
    };
  }
  if (process.arch === "arm64") {
    return {
      target: "linux-arm64",
      binaryName: "codex",
      preferredAssets: [
        "codex-aarch64-unknown-linux-musl.tar.gz",
        "codex-aarch64-unknown-linux-gnu.tar.gz"
      ]
    };
  }
  throw new Error(`Unsupported linux architecture: ${process.arch}`);
}

function chooseAsset(assets, platform) {
  for (const name of platform.preferredAssets) {
    const asset = assets.find((item) => item.name === name);
    if (asset?.browser_download_url) {
      return asset;
    }
  }
  const fallback = assets.find((item) =>
    /^codex-.+linux.+\.tar\.gz$/u.test(item.name)
    && !item.name.includes("-bundle")
    && !item.name.includes("app-server")
    && !item.name.includes("zsh")
    && !item.name.includes("proxy")
  );
  if (fallback?.browser_download_url) {
    return fallback;
  }
  throw new Error(`No Codex Linux asset found for ${platform.target}`);
}

function download(url, outputPath) {
  const curl = spawnSync("curl", ["-fL", "--retry", "3", "--connect-timeout", "20", "-o", outputPath, url], {
    encoding: "utf8",
    stdio: "pipe"
  });
  if (curl.status !== 0) {
    throw new Error(`curl failed: ${(curl.stderr || curl.stdout || "").trim()}`);
  }
}

function run(command, args) {
  const result = spawnSync(command, args, { encoding: "utf8", stdio: "pipe" });
  if (result.status !== 0) {
    throw new Error(`${command} failed: ${(result.stderr || result.stdout || "").trim()}`);
  }
}

function binaryNameCandidates(assetName, binaryName) {
  return new Set([
    binaryName,
    basename(assetName).replace(/\.tar\.gz$/u, "").replace(/\.tgz$/u, "")
  ]);
}

function findBinary(root, names) {
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      const found = findBinary(path, names);
      if (found) {
        return found;
      }
    } else if (entry.isFile() && names.has(entry.name) && statSync(path).size > 1024 * 1024) {
      return path;
    }
  }
  return "";
}

function installStage(stageDir, targetDir) {
  mkdirSync(dirname(targetDir), { recursive: true });
  const backupDir = existsSync(targetDir)
    ? `${targetDir}.backup-${new Date().toISOString().replace(/[:.]/gu, "-")}`
    : "";
  if (backupDir) {
    renameSync(targetDir, backupDir);
  }
  try {
    renameSync(stageDir, targetDir);
  } catch (error) {
    if (backupDir && existsSync(backupDir) && !existsSync(targetDir)) {
      renameSync(backupDir, targetDir);
    }
    throw error;
  }
}

function sha256File(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}
