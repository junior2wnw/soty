#!/usr/bin/env node
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { cp, mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { spawnSync } from "node:child_process";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const sourcePath = join(root, "scripts", "soty-agent.mjs");
const outputDir = join(root, "public", "agent");
const outputPath = join(outputDir, "soty-agent.mjs");
const manifestPath = join(outputDir, "manifest.json");
const opsSkillSourcePath = process.env.SOTY_OPS_SKILL_SOURCE || join(homedir(), ".codex", "skills", "universal-install-ops");
const opsSkillPackageDir = "universal-install-ops-skill";
const windowsReinstallDir = join(outputDir, "windows-reinstall");
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
const windowsReinstall = await publishWindowsReinstallScripts();
const automationToolkits = buildAutomationToolkits(windowsReinstall);

const manifest = {
  version,
  agentUrl: "/agent/soty-agent.mjs",
  sha256: createHash("sha256").update(sourceText).digest("hex"),
  opsSkill,
  windowsReinstall,
  automationToolkits
};

await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
process.stdout.write(`agent:${version}:${manifest.sha256}\n`);
process.stdout.write(`ops-skill:${opsSkill.tarSha256}\n`);
process.stdout.write(`windows-reinstall:${windowsReinstall.scripts.map((script) => `${script.name}:${script.sha256}`).join(",")}\n`);

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

function buildAutomationToolkits(windowsReinstall) {
  return {
    schema: "soty.automation-toolkits.v1",
    policy: {
      entrypoint: "soty_toolkit",
      route: "first-class-toolkit-before-ad-hoc-script",
      fallbackKernel: "soty_action",
      chat: "bare-facts",
      terminalStates: ["completed", "failed", "blocked-needs-user", "waiting-confirmation"]
    },
    toolkits: [
      {
        name: "universal-toolkit",
        entryTool: "soty_toolkit",
        kind: "front-door",
        phases: ["describe", "start", "status", "stop", "list", "reinstall"],
        proof: ["toolkit", "phase", "jobId", "statusPath", "resultPath", "proof"],
        promotion: "Use this before low-level run/script/action. Repeated safe work becomes a manifest-pinned toolkit."
      },
      {
        name: "durable-action",
        entryTool: "soty_action",
        kind: "generic-kernel",
        phases: ["start", "status", "stop"],
        proof: ["jobId", "statusPath", "resultPath", "proof"],
        promotion: "When a family repeats safely, promote it into a manifest-pinned toolkit script with selftests."
      },
      {
        name: "windows-reinstall",
        entryTool: "soty_reinstall",
        kind: "managed-toolkit",
        phases: ["preflight", "prepare", "status", "arm"],
        scriptSet: "windowsReinstall",
        scripts: windowsReinstall.scripts.map((script) => ({
          name: script.name,
          sha256: script.sha256,
          bytes: script.bytes
        })),
        proof: ["backupProof", "installMedia", "unattend", "postinstall", "rebooting"]
      }
    ]
  };
}

async function buildOpsSkillBundle(skillSourcePath) {
  if (!existsSync(join(skillSourcePath, "SKILL.md"))) {
    const existing = await existingOpsSkillBundle();
    if (existing) {
      return existing;
    }
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
    await writeFile(tarPath, await createTarGzFromDirectory(packageRoot, opsSkillPackageDir));

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

async function existingOpsSkillBundle() {
  const manifest = await readJson(manifestPath);
  const opsSkill = manifest?.opsSkill;
  if (!isSafeExistingOpsSkill(opsSkill)) {
    return null;
  }
  const zipPath = join(outputDir, "ops-skill.zip");
  const tarPath = join(outputDir, "ops-skill.tar.gz");
  if (!existsSync(zipPath) || !existsSync(tarPath)) {
    return null;
  }
  const zipBytes = await readFile(zipPath).catch(() => null);
  const tarBytes = await readFile(tarPath).catch(() => null);
  if (!zipBytes || !tarBytes) {
    return null;
  }
  if (sha256(zipBytes) !== opsSkill.zipSha256 || sha256(tarBytes) !== opsSkill.tarSha256) {
    return null;
  }
  if (zipBytes.length !== opsSkill.zipBytes || tarBytes.length !== opsSkill.tarBytes) {
    return null;
  }
  return { ...opsSkill };
}

async function readJson(path) {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch {
    return null;
  }
}

function isSafeExistingOpsSkill(value) {
  return value
    && typeof value === "object"
    && value.name === "universal-install-ops"
    && value.root === opsSkillPackageDir
    && value.zipUrl === "/agent/ops-skill.zip"
    && value.tarUrl === "/agent/ops-skill.tar.gz"
    && /^[a-f0-9]{64}$/u.test(value.zipSha256)
    && /^[a-f0-9]{64}$/u.test(value.tarSha256)
    && Number.isSafeInteger(value.zipBytes)
    && Number.isSafeInteger(value.tarBytes);
}

function isAllowedSkillPath(value) {
  const normalized = String(value || "").replace(/\\/gu, "/");
  return !/(^|\/)(\.git|\.skill-memory|__pycache__)(\/|$)/u.test(normalized)
    && !/\.pyc$/iu.test(normalized);
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

async function createTarGzFromDirectory(rootDir, rootName) {
  const files = await listFiles(rootDir);
  const records = [];
  for (const file of files) {
    const absolutePath = join(rootDir, file);
    const data = await readFile(absolutePath);
    const name = `${rootName}/${file.replace(/\\/gu, "/")}`;
    records.push(createTarHeader(name, data.length), data, zeroPad(data.length, 512));
  }
  records.push(Buffer.alloc(1024));
  return gzipSync(Buffer.concat(records), { level: 9, mtime: 0 });
}

function createTarHeader(path, size) {
  const header = Buffer.alloc(512, 0);
  const { name, prefix } = splitTarPath(path);
  writeTarString(header, name, 0, 100);
  writeTarOctal(header, 0o644, 100, 8);
  writeTarOctal(header, 0, 108, 8);
  writeTarOctal(header, 0, 116, 8);
  writeTarOctal(header, size, 124, 12);
  writeTarOctal(header, 0, 136, 12);
  header.fill(0x20, 148, 156);
  header[156] = "0".charCodeAt(0);
  writeTarString(header, "ustar", 257, 6);
  writeTarString(header, "00", 263, 2);
  writeTarString(header, prefix, 345, 155);

  let checksum = 0;
  for (const byte of header) {
    checksum += byte;
  }
  const checksumText = checksum.toString(8).padStart(6, "0");
  header.write(`${checksumText}\0 `, 148, 8, "ascii");
  return header;
}

function splitTarPath(path) {
  const normalized = String(path || "").replace(/\\/gu, "/").replace(/^\/+/u, "");
  if (Buffer.byteLength(normalized, "utf8") <= 100) {
    return { name: normalized, prefix: "" };
  }
  const parts = normalized.split("/");
  for (let index = parts.length - 1; index > 0; index -= 1) {
    const prefix = parts.slice(0, index).join("/");
    const name = parts.slice(index).join("/");
    if (Buffer.byteLength(prefix, "utf8") <= 155 && Buffer.byteLength(name, "utf8") <= 100) {
      return { name, prefix };
    }
  }
  throw new Error(`Tar path is too long: ${normalized}`);
}

function writeTarString(buffer, value, offset, length) {
  const bytes = Buffer.from(String(value || ""), "utf8");
  if (bytes.length > length) {
    throw new Error(`Tar field is too long: ${value}`);
  }
  bytes.copy(buffer, offset);
}

function writeTarOctal(buffer, value, offset, length) {
  const text = Math.trunc(value).toString(8);
  if (text.length > length - 1) {
    throw new Error(`Tar numeric field is too large: ${value}`);
  }
  buffer.write(`${text.padStart(length - 1, "0")}\0`, offset, length, "ascii");
}

function zeroPad(size, blockSize) {
  const remainder = size % blockSize;
  return remainder === 0 ? Buffer.alloc(0) : Buffer.alloc(blockSize - remainder);
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
