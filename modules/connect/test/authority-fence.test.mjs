import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService, digestArgs } from '../server/index.mjs';

const PROJECT = 'authority-fence-test', ORIGIN = 'https://authority.test';
const code = expected => error => error?.code === expected;
function fixture(t, extensions = []) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-authority-fence-'));
  const nonce = randomBytes(16).toString('hex'); writeFileSync(path.join(directory, 'owned'), nonce, { flag: 'wx' });
  const databasePath = path.join(directory, 'connect.sqlite');
  const service = createConnectService({ databasePath, projectId: PROJECT, allowedOrigins: [ORIGIN], extensions });
  const workers = [];
  t.after(async () => {
    for (const worker of workers) await worker.terminate();
    service.close();
    const actual = realpathSync(directory);
    assert.equal(path.dirname(actual), parent); assert.match(path.basename(actual), /^soty-authority-fence-/u);
    assert.equal(readFileSync(path.join(actual, 'owned'), 'utf8'), nonce);
    rmSync(actual, { recursive: true });
  });
  async function worker(mode) {
    const shared = new SharedArrayBuffer(8), flags = new Int32Array(shared);
    const value = new Worker(new URL('./support/authority-writer.mjs', import.meta.url), {
      workerData: { mode, databasePath, projectId: PROJECT, origin: ORIGIN, flags: shared },
    });
    workers.push(value); await message(value, 'ready'); return { worker: value, flags };
  }
  return { service, databasePath, worker };
}
function message(worker, phase) {
  return new Promise((resolve, reject) => {
    const cleanup = () => { clearTimeout(timer); worker.off('message', onMessage); worker.off('error', onError); worker.off('exit', onExit); };
    const onMessage = value => { if (value.phase === phase) { cleanup(); resolve(value); } };
    const onError = error => { cleanup(); reject(error); };
    const onExit = () => { cleanup(); reject(new Error('writer_exited_before_' + phase)); };
    const timer = setTimeout(() => { cleanup(); reject(new Error('writer_timeout_' + phase)); }, 5000);
    worker.on('message', onMessage); worker.once('error', onError); worker.once('exit', onExit);
  });
}
function identity() {
  const keys = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { privateKey: keys.privateKey, publicJwk: keys.publicKey.export({ format: 'jwk' }),
    encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
function good(value) { assert.equal(value.ok, true, value.error?.code); return value; }
async function signed(service, actor, op, args = {}) {
  const challenge = good(await service.handle({ op: 'challenge', args: { operation: op, digest: digestArgs(args) }, origin: ORIGIN }));
  return { op, args, origin: ORIGIN, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
    signature: sign('sha256', Buffer.from(challenge.message), { key: actor.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
}
const call = async (service, actor, op, args) => good(await service.handle(await signed(service, actor, op, args)));
const bootstrap = (service, actor) => call(service, actor, 'bootstrap', { label: 'Synthetic owner', encryptionPublicJwk: actor.encryptionPublicJwk });
async function enroll(service, owner, phone) {
  const start = await call(service, phone, 'enrollment.start', { label: 'Synthetic phone', encryptionPublicJwk: phone.encryptionPublicJwk });
  const status = await call(service, owner, 'status', {});
  await call(service, owner, 'enrollment.approve', { requestId: start.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'synthetic' } });
  return call(service, phone, 'enrollment.finish', { requestId: start.requestId, expectedAccountId: status.accountId });
}

test('host fence excludes a real signed revoke until its synchronous observation completes', async t => {
  const f = fixture(t), owner = identity(), phone = identity();
  await bootstrap(f.service, owner); const actor = await enroll(f.service, owner, phone);
  const request = await signed(f.service, owner, 'device.revoke', { deviceId: actor.deviceId });
  const peer = await f.worker('revoke'), completion = message(peer.worker, 'result');
  const result = f.service.withAuthorityFence(() => {
    peer.worker.postMessage(request);
    if (Atomics.load(peer.flags, 0) === 0) Atomics.wait(peer.flags, 0, 0, 2000);
    assert.equal(Atomics.load(peer.flags, 0), 1, 'the independent writer actually attempted the signed request');
    assert.equal(Atomics.wait(peer.flags, 1, 0, 100), 'timed-out');
    assert.equal(f.service.isActorActive(actor), true);
    return 'observed-before-revoke';
  });
  assert.equal(result, 'observed-before-revoke'); good((await completion).result);
  assert.equal(f.service.withAuthorityFence(() => f.service.isActorActive(actor)), false,
    'a revoke committed first is visible inside the next fence');
});

test('busy acquisition never runs the callback and restores the normal signed writer wait', async t => {
  const f = fixture(t), owner = identity(); await bootstrap(f.service, owner);
  const request = await signed(f.service, owner, 'profile.rename', { label: 'After contention' });
  const peer = await f.worker('lock'), released = message(peer.worker, 'released');
  peer.worker.postMessage({ release: true });
  let calls = 0; const started = performance.now();
  assert.throws(() => f.service.withAuthorityFence(() => { calls++; }), code('connect_authority_busy'));
  assert.equal(calls, 0); assert.ok(performance.now() - started < 2500);
  // The lock remains longer than100ms, so an incorrectly retained short policy fails this real write.
  good(await f.service.handle(request)); await released;
  assert.equal(f.service.withAuthorityFence(() => 'available'), 'available');
});

test('nested mutation and close cannot damage an outer fence or consume a signed proof', async t => {
  const f = fixture(t), owner = identity(); await bootstrap(f.service, owner);
  const request = await signed(f.service, owner, 'profile.rename', { label: 'Still authorized' });
  let denied;
  assert.equal(f.service.withAuthorityFence(() => {
    assert.throws(() => f.service.withAuthorityFence(() => assert.fail()), code('connect_transaction_nested'));
    assert.throws(() => f.service.close(), code('connect_transaction_nested'));
    denied = f.service.handle(request);
    return 42;
  }), 42);
  assert.equal((await denied).error.code, 'connect_transaction_nested');
  good(await f.service.handle(request));
});

test('a fence called by a signed extension cannot roll back the outer proof or action transaction', async t => {
  let service;
  const extension = { operations: new Set(['test.nested']), execute() {
    assert.throws(() => service.withAuthorityFence(() => assert.fail()), code('connect_transaction_nested'));
    assert.throws(() => service.close(), code('connect_transaction_nested'));
    return { nestedRefused: true };
  } };
  const f = fixture(t, [extension]); service = f.service;
  const owner = identity(); await bootstrap(service, owner);
  const request = await signed(service, owner, 'test.nested');
  assert.equal(good(await service.handle(request)).nestedRefused, true);
  assert.equal((await service.handle(request)).error.code, 'challenge_consumed');
  good(await call(service, owner, 'profile.rename', { label: 'Outer commit preserved' }));
});

test('async callbacks are refused before invocation and returned promises cannot count as success', async t => {
  const f = fixture(t); let called = false;
  for (const action of [async () => { called = true; }, async function* () { called = true; }]) {
    assert.throws(() => f.service.withAuthorityFence(action), code('connect_authority_callback_invalid'));
  }
  assert.equal(called, false);
  assert.throws(() => f.service.withAuthorityFence(() => Promise.reject(new Error('synthetic async failure'))), code('connect_authority_callback_async'));
  assert.throws(() => f.service.withAuthorityFence(() => ({ then(_resolve, reject) { reject(new Error('synthetic thenable')); } })), code('connect_authority_callback_async'));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(f.service.withAuthorityFence(() => 'recovered'), 'recovered');
  const original = new Error('primary callback');
  assert.throws(() => f.service.withAuthorityFence(() => { throw original; }), error => error === original);
  assert.equal(f.service.withAuthorityFence(() => 1), 1);
});

test('cleanup errors preserve the primary callback failure and the store fails closed until rollback', t => {
  const f = fixture(t), exec = DatabaseSync.prototype.exec, original = new Error('primary');
  let connection;
  try {
    DatabaseSync.prototype.exec = function(sql) {
      if (sql === 'ROLLBACK' || sql === 'PRAGMA busy_timeout=5000') { connection = this; throw new Error('synthetic cleanup failure'); }
      return exec.call(this, sql);
    };
    assert.throws(() => f.service.withAuthorityFence(() => { throw original; }), error => error === original);
  } finally { DatabaseSync.prototype.exec = exec; }
  assert.throws(() => f.service.withAuthorityFence(() => assert.fail()), code('connect_transaction_nested'));
  assert.ok(connection?.isTransaction); exec.call(connection, 'ROLLBACK; PRAGMA busy_timeout=5000');
  assert.equal(f.service.withAuthorityFence(() => 1), 1);
});

test('closed services reject the host fence without invoking supplied work', t => {
  const f = fixture(t); f.service.close(); let called = false;
  assert.throws(() => f.service.withAuthorityFence(() => { called = true; }), code('service_closed'));
  assert.equal(called, false); f.service.close();
});
