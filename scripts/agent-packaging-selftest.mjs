#!/usr/bin/env node
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";

const root = resolve(import.meta.dirname, "..");
// Exact published CurrentUser 1.2.12 assets from original image 8317e275.
// Installer source Git c0ca is LF; manifest hashes must describe emitted bytes.
const expected = {
  "core/xray-windows-x64-26.7.11.zip": {
    "bytes": 20934014,
    "sha256": "af801b62c4d41d248d3db8016d4c6e2a7ccfb7ed443e3738aeb6f9e062321512"
  },
  "install-macos-linux.sh": {
    "bytes": 16746,
    "sha256": "ae8cc4906377e9a7954e0d1ddd0ef218e2ab8eea4f61cc3a15c013df6640a5f0"
  },
  "install-windows-machine-bootstrap.ps1": {
    "bytes": 5053,
    "sha256": "7ab255474cfc686dcdd53b9aa8c6dba1d732d905d8a4204bea29b8aaf5d7d55a"
  },
  "install-windows-machine.cmd": {
    "bytes": 3639,
    "sha256": "b27e9687ab1f00c2ba7ca734fffb0602ced4460e848d450bb2adb893828803a4"
  },
  "install-windows.ps1": {
    "bytes": 42931,
    "sha256": "9d5113b0f8a095cb0d86fe02bc1da6ccd326be5144d8d3b5f8e3bc0d88112741"
  },
  "manifest.json": {
    "bytes": 6648,
    "sha256": "e9236534f0b02988c0f6840a72fc46b02ffeedae767a4f1ce1306959561fe922"
  },
  "soty-agent.mjs": {
    "bytes": 149433,
    "sha256": "54364a5e517170b1b8844a43c3969bc0f527f21f255ab5baa6626b13a9428439"
  },
  "soty-connector.mjs": {
    "bytes": 149433,
    "sha256": "54364a5e517170b1b8844a43c3969bc0f527f21f255ab5baa6626b13a9428439"
  },
  "windows-reinstall/soty-arm-windows-reinstall.ps1": {
    "bytes": 8194,
    "sha256": "8e245bd6fc2b013678510616fc35c93c04f0f4ed357c284d36cdcafac099fa48"
  },
  "windows-reinstall/soty-make-fast-usb.ps1": {
    "bytes": 14537,
    "sha256": "85625a7524b72ec97ef7fd675ca5b56f659320770c725862b9cbd32834c4be34"
  },
  "windows-reinstall/soty-managed-windows-reinstall.ps1": {
    "bytes": 52607,
    "sha256": "505d7d771b1b4ffdae9ca43b1151757e4401f5fc91f0a9c691f74e80b9142902"
  },
  "windows-reinstall/soty-prepare-windows-reinstall.ps1": {
    "bytes": 94289,
    "sha256": "84ef1cc3594c44f2facd5d2b5ac8036f7119795ce954b29548dfa70eb4abf598"
  }
};
const digest = bytes => createHash("sha256").update(bytes).digest("hex");
const parent = resolve(tmpdir());
const fixture = await mkdtemp(join(parent, "soty-agent-packaging-"));
const checks = [];
async function transform(directory, lineEnding) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) await transform(path, lineEnding);
    else if (/\.(?:mjs|json|sh|ps1|cmd)$/u.test(entry.name)) {
      const text = (await readFile(path, "utf8")).replace(/\r\n/g, "\n");
      await writeFile(path, lineEnding === "CRLF" ? text.replace(/\n/g, "\r\n") : text);
    }
  }
}
try {
  for (const lineEnding of ["LF", "CRLF"]) {
    const target = join(fixture, lineEnding);
    await mkdir(join(target, "scripts"), { recursive: true });
    for (const name of ["build-agent-release.mjs", "soty-connector.mjs", "agent-modules", "windows"]) {
      await cp(join(root, "scripts", name), join(target, "scripts", name), { recursive: true });
    }
    await mkdir(join(target, "public"), { recursive: true });
    await cp(join(root, "public", "agent"), join(target, "public", "agent"), { recursive: true });
    await transform(target, lineEnding);
    const output = execFileSync(process.execPath, [join(target, "scripts", "build-agent-release.mjs")], { encoding: "utf8", windowsHide: true, timeout: 20000 });
    assert.match(output, /connector:1\.2\.12:54364a5e517170b1b8844a43c3969bc0f527f21f255ab5baa6626b13a9428439/u);
    const emitted = join(target, "public", "agent");
    for (const [name, expectedFile] of Object.entries(expected)) {
      const bytes = await readFile(join(emitted, name));
      assert.equal(bytes.length, expectedFile.bytes, `${lineEnding} ${name} bytes`);
      assert.equal(digest(bytes), expectedFile.sha256, `${lineEnding} ${name} raw hash`);
      if (/\.(?:sh|ps1|cmd)$/u.test(name)) assert.equal(bytes.includes(Buffer.from("\r\n")), false, `${name} canonical LF`);
    }
    const manifest = JSON.parse(await readFile(join(emitted, "manifest.json"), "utf8"));
    for (const item of manifest.windowsReinstall.scripts) {
      const bytes = await readFile(join(emitted, item.url.replace("/agent/", "")));
      assert.equal(item.bytes, bytes.length);
      assert.equal(item.sha256, digest(bytes));
    }
    // Rebuilding already normalized output must not change a single asset.
    execFileSync(process.execPath, [join(target, "scripts", "build-agent-release.mjs")], { windowsHide: true, timeout: 20000 });
    for (const [name, expectedFile] of Object.entries(expected)) assert.equal(digest(await readFile(join(emitted, name))), expectedFile.sha256);
    checks.push({ lineEnding, exactAssets: Object.keys(expected).length, manifestSelfConsistent: true, idempotent: true });
  }
  console.log(JSON.stringify({ ok: true, providerCalls: 0, connectorVersion: "1.2.12", checks }, null, 2));
} finally {
  assert.equal(dirname(resolve(fixture)), parent);
  assert.ok(resolve(fixture).startsWith(parent + sep + "soty-agent-packaging-"));
  await rm(fixture, { recursive: true, force: true });
}
