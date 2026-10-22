import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { build, loadConfigFromFile } from "vite";

const root = resolve(import.meta.dirname, "..");
const moduleUrl = process.env.SOTY_UNIX_INSTALLER_MODULE
  ? pathToFileURL(resolve(process.env.SOTY_UNIX_INSTALLER_MODULE))
  : new URL("./unix-installer-release.mjs", import.meta.url);
const { publishUnixInstaller } = await import(moduleUrl.href);
const lf = Buffer.from("#!/usr/bin/env sh\nset -eu\nprintf '%s\\n' 'Привет'\n", "utf8");
const crlf = Buffer.from(lf.toString("utf8").replace(/\n/g, "\r\n"), "utf8");
const windows = Buffer.from("@echo off\r\necho Windows\r\n", "utf8");
const powershell = Buffer.from("Write-Host 'Windows'\r\n", "utf8");

async function fixture(run) {
  const base = await mkdtemp(join(tmpdir(), "soty-unix-installer-test-"));
  try { await run(base); }
  finally {
    assert.equal(dirname(resolve(base)), resolve(tmpdir()));
    assert.ok(basename(base).startsWith("soty-unix-installer-test-"));
    await rm(base, { recursive: true, force: true });
  }
}

test("actual release publisher writes LF without changing Windows scripts", async () => {
  await fixture(async base => {
    const shell = join(base, "install-macos-linux.sh");
    const cmd = join(base, "install-windows.cmd"), ps = join(base, "install-windows.ps1");
    await writeFile(shell, crlf); await writeFile(cmd, windows); await writeFile(ps, powershell);
    await publishUnixInstaller(shell);
    assert.deepEqual(await readFile(shell), lf);
    assert.equal((await readFile(shell)).includes(13), false);
    assert.deepEqual(await readFile(cmd), windows); assert.deepEqual(await readFile(ps), powershell);
    await publishUnixInstaller(shell);
    assert.deepEqual(await readFile(shell), lf);
  });
});

test("actual release publisher rejects bare CR before writing", async () => {
  await fixture(async base => {
    const shell = join(base, "install-macos-linux.sh"), malformed = Buffer.from("#!/usr/bin/env sh\rset -eu\n");
    await writeFile(shell, malformed);
    await assert.rejects(publishUnixInstaller(shell), /unix-installer-bare-cr/);
    assert.deepEqual(await readFile(shell), malformed);
  });
});

test("actual Vite config hook normalizes stale public bytes before the real public copy", async () => {
  const loaded = await loadConfigFromFile({ command: "build", mode: "test" }, join(root, "vite.config.ts"), root);
  const plugins = (loaded?.config.plugins ?? []).flat(Infinity).filter(p => p && p.name === "soty-versioned-worker");
  assert.equal(plugins.length, 1);
  const production = plugins[0];
  assert.equal(production.apply, "build");
  assert.equal(typeof production.configResolved, "function");
  await fixture(async base => {
    const agent = join(base, "public", "agent"); await mkdir(agent, { recursive: true });
    await writeFile(join(base, "index.html"), "<!doctype html><title>Unix installer copy</title>");
    await writeFile(join(agent, "install-macos-linux.sh"), crlf);
    await writeFile(join(agent, "install-windows.cmd"), windows);
    await writeFile(join(agent, "install-windows.ps1"), powershell);
    await build({ configFile: false, root: base, logLevel: "silent",
      plugins: [{ name: "actual-unix-installer-precopy", apply: production.apply, configResolved: production.configResolved }],
      build: { outDir: "dist", emptyOutDir: true, minify: false, sourcemap: false } });
    assert.deepEqual(await readFile(join(agent, "install-macos-linux.sh")), lf);
    assert.deepEqual(await readFile(join(base, "dist", "agent", "install-macos-linux.sh")), lf);
    assert.deepEqual(await readFile(join(base, "dist", "agent", "install-windows.cmd")), windows);
    assert.deepEqual(await readFile(join(base, "dist", "agent", "install-windows.ps1")), powershell);
  });
});
