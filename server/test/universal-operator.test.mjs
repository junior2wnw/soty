import test from 'node:test';
import assert from 'node:assert/strict';
import { createConnection, createServer } from 'node:net';
import { lstat, mkdir, chmod, unlink, writeFile, readFile, rmdir, symlink } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { startUniversalOperator, readUniversalOperator, universalOperatorPath, UNIVERSAL_OPERATOR_LIMITS } from '../universal-operator.js';
import { captureUniversalPreparedness, UNIVERSAL_RUNTIME_SCHEMA } from '../../modules/app-contract/universal-preparedness.mjs';

const posix = process.platform !== 'win32', directory = '/tmp/soty-operator';
const baseline = () => captureUniversalPreparedness({ compiledLegacyMode: true, universalConfigured: false,
  reviewsConfigured: false, humanProfile: null, humanHttpEnabled: false });
const options = { skip: !posix && 'Windows has no private Unix operator; actual Linux gate required' };
async function exists(path) { try { return await lstat(path); } catch (error) { if (error.code === 'ENOENT') return null; throw error; } }
async function cleanupDirectory() { const stat = await exists(directory); if (stat?.isDirectory() && !stat.isSymbolicLink() && stat.uid === process.getuid()) {
  try { await rmdir(directory); } catch (error) { if (!['ENOTEMPTY', 'EEXIST'].includes(error.code)) throw error; }
} }
async function fixture(t, capture = baseline) {
  assert.equal(await exists(universalOperatorPath), null, 'exclusive fixture container; never remove an unknown preexisting listener');
  const handle = await startUniversalOperator({ capture }); t.after(async () => { await handle.close(); await cleanupDirectory(); }); return handle;
}
async function raw(value, { end = true } = {}) {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ path: universalOperatorPath }), chunks = []; let length = 0;
    const timer = setTimeout(() => { socket.destroy(); reject(new Error('fixture_socket_timeout')); }, 3000);
    socket.once('connect', () => end ? socket.end(value) : socket.write(value));
    socket.on('data', chunk => { length += chunk.length; assert.equal(length <= 65536, true); chunks.push(chunk); });
    socket.on('error', () => {}); socket.once('close', () => { clearTimeout(timer); resolve(Buffer.concat(chunks, length).toString('utf8')); });
  });
}

test('Windows returns unsupported without creating a listener or invoking the host capture', { skip: posix }, async () => {
  let called = 0; const handle = await startUniversalOperator({ capture() { called++; return baseline(); } });
  assert.equal(handle.supported, false); assert.equal(handle.path, null); assert.equal(called, 0); assert.equal(await handle.close(), false);
  await assert.rejects(readUniversalOperator(), error => error.code === 'universal_operator_unsupported');
});

test('operator constructor is closed and rejects accessor/async callbacks before any listener', async () => {
  let called = 0; const accessor = {}; Object.defineProperty(accessor, 'capture', { enumerable: true, get() { called++; return baseline; } });
  for (const input of [accessor, { capture: async () => baseline() }, { capture: baseline, path: '/tmp/other.sock' }, { capture: null }]) {
    await assert.rejects(startUniversalOperator(input), error => error.code === 'universal_operator_capture_invalid');
  }
  assert.equal(called, 0);
});

test('actual Unix listener is owner-bound0700/0600, returns exact measurement and closes idempotently', options, async t => {
  const handle = await fixture(t); assert.equal(handle.supported, true); assert.equal(handle.path, universalOperatorPath);
  const dir = await lstat(directory), socket = await lstat(universalOperatorPath);
  assert.equal(dir.uid, process.getuid()); assert.equal(dir.mode & 0o777, 0o700);
  assert.equal(socket.uid, process.getuid()); assert.equal(socket.mode & 0o777, 0o600); assert.equal(socket.isSocket(), true); assert.equal(socket.nlink, 1);
  assert.deepEqual(await readUniversalOperator(), baseline()); assert.equal(await handle.close(), true); assert.equal(await handle.close(), true);
  assert.equal(await exists(universalOperatorPath), null); await assert.rejects(readUniversalOperator());
});

test('only the exact bounded request is accepted; extra command/body/oversize and missing EOF do not capture', options, async t => {
  let called = 0; await fixture(t, () => { called++; return baseline(); });
  for (const input of [UNIVERSAL_RUNTIME_SCHEMA, UNIVERSAL_RUNTIME_SCHEMA + '\n{}', 'revoke\n', 'x'.repeat(129)]) {
    const result = await raw(input); assert.equal(result.includes('universalConfigured'), false);
  }
  assert.equal(await raw(UNIVERSAL_RUNTIME_SCHEMA + '\n', { end: false }), ''); assert.equal(called, 0);
  assert.deepEqual(await readUniversalOperator(), baseline()); assert.equal(called, 1);
});

test('eight pending sockets are a hard bound and close destroys them without retained capture callbacks', options, async t => {
  let called = 0; const handle = await fixture(t, () => { called++; return baseline(); }), clients = [];
  try {
    for (let i = 0; i < UNIVERSAL_OPERATOR_LIMITS.connections; i++) {
      const socket = createConnection({ path: universalOperatorPath }); socket.on('error', () => {});
      await new Promise(resolve => socket.once('connect', resolve)); clients.push(socket);
    }
    await assert.rejects(readUniversalOperator()); assert.equal(called, 0);
    const closed = clients.map(socket => new Promise(resolve => socket.once('close', resolve))); await handle.close(); await Promise.all(closed);
    assert.equal(called, 0);
  } finally { clients.forEach(socket => socket.destroy()); }
});

test('callback failure/async result/credentials never reach private output', options, async t => {
  const failures = [() => { throw new Error('PRIVATE_SYNTHETIC_KEY_DO_NOT_EMIT'); }, () => Promise.reject(new Error('PRIVATE_SYNTHETIC_KEY_DO_NOT_EMIT')),
    () => ({ ...baseline(), secret: 'PRIVATE_SYNTHETIC_KEY_DO_NOT_EMIT' })];
  for (const capture of failures) {
    const handle = await fixture(t, capture), response = await raw(UNIVERSAL_RUNTIME_SCHEMA + '\n');
    assert.equal(response, '{"error":"universal_operator_unavailable"}\n'); await assert.rejects(readUniversalOperator()); await handle.close();
  }
});

test('active listener cannot be replaced; shutdown never unlinks a substituted regular file', options, async t => {
  const handle = await fixture(t); await assert.rejects(startUniversalOperator({ capture: baseline }), error => error.code === 'universal_operator_socket_busy');
  await unlink(universalOperatorPath); await writeFile(universalOperatorPath, 'FOREIGN_SYNTHETIC_FILE', { mode: 0o600 });
  t.after(async () => { if ((await exists(universalOperatorPath))?.isFile()) await unlink(universalOperatorPath); await cleanupDirectory(); });
  await assert.rejects(readUniversalOperator()); await handle.close(); assert.equal(await readFile(universalOperatorPath, 'utf8'), 'FOREIGN_SYNTHETIC_FILE');
});

test('actual stale socket from a terminated process is replaced only after owned inode checks', options, async t => {
  assert.equal(await exists(universalOperatorPath), null); await mkdir(directory, { mode: 0o700 });
  const code = `const net=require('node:net'),fs=require('node:fs');const s=net.createServer();s.listen(${JSON.stringify(universalOperatorPath)},()=>{fs.chmodSync(${JSON.stringify(universalOperatorPath)},0o600);process.send('ready');});`;
  const child = spawn(process.execPath, ['-e', code], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
  await new Promise((resolve, reject) => { child.once('message', resolve); child.once('error', reject); child.once('exit', () => reject(new Error('fixture_child_early_exit'))); });
  const death = new Promise(resolve => child.once('exit', resolve)); child.kill('SIGKILL'); await death;
  assert.equal((await lstat(universalOperatorPath)).isSocket(), true);
  const handle = await startUniversalOperator({ capture: baseline }); t.after(async () => { await handle.close(); await cleanupDirectory(); });
  assert.deepEqual(await readUniversalOperator(), baseline());
});

test('unsafe directory/symlink and non-socket path are refused without overwriting or following them', options, async t => {
  assert.equal(await exists(universalOperatorPath), null); await mkdir(directory, { mode: 0o700 });
  t.after(async () => { const socket = await exists(universalOperatorPath); if (socket && !socket.isSocket()) await unlink(universalOperatorPath);
    await chmod(directory, 0o700); await cleanupDirectory(); });
  await writeFile(universalOperatorPath, 'FOREIGN', { mode: 0o600 });
  await assert.rejects(startUniversalOperator({ capture: baseline }), error => error.code === 'universal_operator_path_invalid');
  assert.equal(await readFile(universalOperatorPath, 'utf8'), 'FOREIGN'); await unlink(universalOperatorPath);
  await symlink('/tmp/nonexistent-soty-operator-target', universalOperatorPath);
  await assert.rejects(startUniversalOperator({ capture: baseline }), error => error.code === 'universal_operator_path_invalid'); await unlink(universalOperatorPath);
  await chmod(directory, 0o755); await assert.rejects(startUniversalOperator({ capture: baseline }), error => error.code === 'universal_operator_path_invalid');
});
