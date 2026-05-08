#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { cp, mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const sourcePath = join(root, "scripts", "soty-agent.mjs");
const outputDir = join(root, "public", "agent");
const outputPath = join(outputDir, "soty-agent.mjs");
const manifestPath = join(outputDir, "manifest.json");
const opsSkillSourcePath = process.env.SOTY_OPS_SKILL_SOURCE || join(homedir(), ".codex", "skills", "universal-install-ops");
const opsSkillPackageDir = "universal-install-ops-skill";
let crcTable = null;

const source = await readFile(sourcePath, "utf8");
const sourceText = source.replace(/\r\n/g, "\n");
const version = sourceText.match(/agentVersion\s*=\s*"([^"]+)"/u)?.[1];
if (!version) {
  throw new Error("Agent version not found");
}

await mkdir(outputDir, { recursive: true });
await writeFile(outputPath, sourceText, { mode: 0o755 });
const opsSkill = await buildOpsSkillBundle(opsSkillSourcePath);

const manifest = {
  version,
  agentUrl: "/agent/soty-agent.mjs",
  sha256: createHash("sha256").update(sourceText).digest("hex"),
  opsSkill
};

await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
process.stdout.write(`agent:${version}:${manifest.sha256}\n`);
process.stdout.write(`ops-skill:${opsSkill.tarSha256}\n`);

async function buildOpsSkillBundle(skillSourcePath) {
  if (!existsSync(join(skillSourcePath, "SKILL.md"))) {
    throw new Error(`Ops skill source not found: ${skillSourcePath}`);
  }

  const stageDir = await mkdtemp(join(tmpdir(), "soty-ops-skill-"));
  try {
    const packageRoot = join(stageDir, opsSkillPackageDir);
    await cp(skillSourcePath, packageRoot, {
      recursive: true,
      dereference: true,
      filter: (sourceItem) => isAllowedSkillPath(relative(skillSourcePath, sourceItem))
    });

    const zipPath = join(outputDir, "ops-skill.zip");
    const tarPath = join(outputDir, "ops-skill.tar.gz");
    await writeFile(zipPath, await createZipFromDirectory(packageRoot, opsSkillPackageDir));
    createTarGz(stageDir, tarPath);

    const zipBytes = await readFile(zipPath);
    const tarBytes = await readFile(tarPath);
    const git = skillGitInfo(skillSourcePath);
    return {
      name: "universal-install-ops",
      root: opsSkillPackageDir,
      zipUrl: "/agent/ops-skill.zip",
      zipSha256: sha256(zipBytes),
      zipBytes: zipBytes.length,
      tarUrl: "/agent/ops-skill.tar.gz",
      tarSha256: sha256(tarBytes),
      tarBytes: tarBytes.length,
      ...(git.revision ? { revision: git.revision } : {}),
      ...(git.dirty ? { dirty: true } : {})
    };
  } finally {
    await rm(stageDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

function isAllowedSkillPath(value) {
  const normalized = String(value || "").replace(/\\/gu, "/");
  return !/(^|\/)(\.git|\.skill-memory|__pycache__)(\/|$)/u.test(normalized)
    && !/\.pyc$/iu.test(normalized);
}

function createTarGz(stageDir, tarPath) {
  const result = spawnSync("tar", ["-czf", tarPath, "-C", stageDir, opsSkillPackageDir], {
    encoding: "utf8"
  });
  if (result.status !== 0) {
    throw new Error(`tar failed: ${result.stderr || result.error?.message || result.status}`);
  }
}

async function createZipFromDirectory(rootDir, rootName) {
  const files = await listFiles(rootDir);
  const localRecords = [];
  const centralRecords = [];
  let offset = 0;

  for (const file of files) {
    const absolutePath = join(rootDir, file);
    const fileStat = await stat(absolutePath);
    const data = await readFile(absolutePath);
    const pathBytes = Buffer.from(`${rootName}/${file.replace(/\\/gu, "/")}`, "utf8");
    const crc = crc32(data);
    const stamp = dosDateTime(fileStat.mtime);
    const flags = 0x0800;

    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(flags, 6);
    local.writeUInt16LE(0, 8);
    local.writeUInt16LE(stamp.time, 10);
    local.writeUInt16LE(stamp.date, 12);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(data.length, 18);
    local.writeUInt32LE(data.length, 22);
    local.writeUInt16LE(pathBytes.length, 26);
    local.writeUInt16LE(0, 28);
    localRecords.push(local, pathBytes, data);

    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt16LE(flags, 8);
    central.writeUInt16LE(0, 10);
    central.writeUInt16LE(stamp.time, 12);
    central.writeUInt16LE(stamp.date, 14);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(data.length, 20);
    central.writeUInt32LE(data.length, 24);
    central.writeUInt16LE(pathBytes.length, 28);
    central.writeUInt16LE(0, 30);
    central.writeUInt16LE(0, 32);
    central.writeUInt16LE(0, 34);
    central.writeUInt16LE(0, 36);
    central.writeUInt32LE(0, 38);
    central.writeUInt32LE(offset, 42);
    centralRecords.push(central, pathBytes);
    offset += local.length + pathBytes.length + data.length;
  }

  const centralOffset = offset;
  const centralSize = centralRecords.reduce((sum, item) => sum + item.length, 0);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(0, 4);
  end.writeUInt16LE(0, 6);
  end.writeUInt16LE(files.length, 8);
  end.writeUInt16LE(files.length, 10);
  end.writeUInt32LE(centralSize, 12);
  end.writeUInt32LE(centralOffset, 16);
  end.writeUInt16LE(0, 20);

  return Buffer.concat([...localRecords, ...centralRecords, end]);
}

async function listFiles(rootDir, currentDir = rootDir) {
  const entries = (await readdir(currentDir, { withFileTypes: true }))
    .filter((entry) => isAllowedSkillPath(relative(rootDir, join(currentDir, entry.name))))
    .sort((left, right) => left.name.localeCompare(right.name));
  const files = [];
  for (const entry of entries) {
    const absolutePath = join(currentDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listFiles(rootDir, absolutePath));
    } else if (entry.isFile()) {
      files.push(relative(rootDir, absolutePath).replace(/\\/gu, "/"));
    }
  }
  return files;
}

function dosDateTime(date) {
  const year = Math.max(1980, Math.min(2107, date.getFullYear()));
  return {
    time: (date.getHours() << 11) | (date.getMinutes() << 5) | Math.floor(date.getSeconds() / 2),
    date: ((year - 1980) << 9) | ((date.getMonth() + 1) << 5) | date.getDate()
  };
}

function crc32(bytes) {
  const table = crcTable || (crcTable = createCrcTable());
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc = table[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function createCrcTable() {
  return new Uint32Array(256).map((_, index) => {
    let value = index;
    for (let bit = 0; bit < 8; bit += 1) {
      value = (value & 1) ? (0xedb88320 ^ (value >>> 1)) : (value >>> 1);
    }
    return value >>> 0;
  });
}

function skillGitInfo(skillSourcePath) {
  const revision = spawnSync("git", ["-C", skillSourcePath, "rev-parse", "HEAD"], { encoding: "utf8" });
  const status = spawnSync("git", ["-C", skillSourcePath, "status", "--short"], { encoding: "utf8" });
  return {
    revision: revision.status === 0 ? revision.stdout.trim() : "",
    dirty: status.status === 0 && status.stdout.trim().length > 0
  };
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}
