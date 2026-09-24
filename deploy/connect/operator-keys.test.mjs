import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, unlink, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import os from 'node:os';
import path from 'node:path';
const script = fileURLToPath(new URL('./operator-keys.ps1', import.meta.url));
function init(keyStore, publicDirectory) {
  return new Promise((resolve, reject) => {
    const child = spawn('pwsh.exe', ['-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
      '-File', script, '-Action', 'init', '-KeyStore', keyStore, '-PublicDirectory', publicDirectory],
    { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject); child.on('close', code => resolve({ code, stdout, stderr }));
  });
}
test('atomic DPAPI initialization preserves the durable key and repairs missing public files', { skip: process.platform !== 'win32' }, async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'connect-operator-'));
  t.after(async () => {
    assert.equal(path.dirname(root), path.resolve(os.tmpdir())); assert.match(path.basename(root), /^connect-operator-/);
    await rm(root, { recursive: true, force: true });
  });
  const keyStore = path.join(root, 'private', 'synthetic.dpapi'); const publicDirectory = path.join(root, 'public');
  const results = await Promise.all([init(keyStore, publicDirectory), init(keyStore, publicDirectory)]);
  assert.ok(results.some(result => result.code === 0), 'at least one publisher owns initialization');
  for (const result of results) assert.doesNotMatch(result.stdout + result.stderr, /BEGIN PRIVATE KEY/);
  const protectedBytes = await readFile(keyStore);
  const trustPath = path.join(publicDirectory, 'root.json'), backupPath = path.join(publicDirectory, 'backup-public.pem');
  const trust = await readFile(trustPath), backup = await readFile(backupPath);
  await unlink(trustPath); await unlink(backupPath);
  const repaired = await init(keyStore, publicDirectory); assert.equal(repaired.code, 0, repaired.stderr);
  assert.deepEqual(await readFile(keyStore), protectedBytes);
  assert.deepEqual(await readFile(trustPath), trust); assert.deepEqual(await readFile(backupPath), backup);
  await writeFile(trustPath, '{}');
  const conflict = await init(keyStore, publicDirectory); assert.notEqual(conflict.code, 0);
  assert.deepEqual(await readFile(keyStore), protectedBytes); assert.equal(await readFile(trustPath, 'utf8'), '{}');
});
