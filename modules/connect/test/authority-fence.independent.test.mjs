import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { createConnectService, digestArgs } from '../server/index.mjs';

const ORIGIN = 'https://independent-authority.test';
const PROJECT = 'independent-authority';
const errorCode = expected => error => error?.code === expected;
const checkReply = reply => { assert.equal(reply.ok, true, reply.error?.code); return reply; };

// Own harness: a separately opened real Connect service or SQLite writer. No author fixtures.
if (!isMainThread) {
  const flags = new Int32Array(workerData.flags);
  const signal = index => { Atomics.store(flags, index, 1); Atomics.notify(flags, index); };
  if (workerData.mode === 'signed') {
    const service = createConnectService({ databasePath: workerData.databasePath, projectId: PROJECT, allowedOrigins: [ORIGIN] });
    parentPort.on('message', async message => {
      if (message.op === 'stop-worker') { service.close(); parentPort.close(); return; }
      const original = DatabaseSync.prototype.exec;
      DatabaseSync.prototype.exec = function(sql) {
        if (sql === 'BEGIN IMMEDIATE') signal(0);
        return original.call(this, sql);
      };
      try {
        const result = await service.handle(message.request);
        signal(1); parentPort.postMessage({ type: 'completed', result });
      } finally { DatabaseSync.prototype.exec = original; }
    });
    parentPort.postMessage({ type: 'ready' });
  } else {
    const db = new DatabaseSync(workerData.databasePath);
    db.exec('PRAGMA busy_timeout=1000; BEGIN IMMEDIATE');
    parentPort.once('message', () => setTimeout(() => {
      db.exec('COMMIT'); db.close(); signal(1); parentPort.postMessage({ type: 'released' }); parentPort.close();
    }, 360));
    parentPort.postMessage({ type: 'ready' });
  }
} else {
  const ownPrefix = 'soty-connect-independent-';
  function fixture(t, extensions = []) {
    const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, ownPrefix));
    const marker = randomBytes(24).toString('hex');
    writeFileSync(path.join(directory, 'test-owned'), marker, { flag: 'wx' });
    const databasePath = path.join(directory, 'connect.sqlite'), workers = [], otherStores = [];
    const service = createConnectService({ databasePath, projectId: PROJECT, allowedOrigins: [ORIGIN], extensions });
    let db;
    const exec = DatabaseSync.prototype.exec;
    try {
      DatabaseSync.prototype.exec = function(sql) {
        if (sql === 'PRAGMA busy_timeout=100') db = this;
        return exec.call(this, sql);
      };
      service.withAuthorityFence(() => undefined);
    } finally { DatabaseSync.prototype.exec = exec; }
    assert.ok(db instanceof DatabaseSync);
    t.after(async () => {
      for (const worker of workers) await worker.terminate();
      if (db.isTransaction) db.exec('ROLLBACK');
      service.close();
      for (const store of otherStores) store.close();
      const actual = realpathSync(directory);
      assert.equal(path.dirname(actual), parent);
      assert.ok(path.basename(actual).startsWith(ownPrefix));
      assert.equal(readFileSync(path.join(actual, 'test-owned'), 'utf8'), marker);
      rmSync(actual, { recursive: true });
    });
    async function writer(mode) {
      const flags = new Int32Array(new SharedArrayBuffer(8));
      const worker = new Worker(new URL(import.meta.url), { workerData: { mode, databasePath, flags: flags.buffer } });
      workers.push(worker); await waitFor(worker, 'ready');
      return { worker, flags };
    }
    return { service, db, writer, downstream() {
      const store = new DatabaseSync(path.join(directory, 'downstream.sqlite'));
      store.exec('CREATE TABLE effects(id INTEGER PRIMARY KEY, value TEXT NOT NULL)');
      otherStores.push(store); return store;
    } };
  }
  function waitFor(worker, type) {
    return new Promise((resolve, reject) => {
      const finish = (error, value) => { clearTimeout(timer); worker.off('message', message); worker.off('error', failure); worker.off('exit', exited); error ? reject(error) : resolve(value); };
      const message = value => { if (value.type === type) finish(null, value); };
      const failure = error => finish(error);
      const exited = () => finish(new Error('independent_writer_ended_before_' + type));
      const timer = setTimeout(() => finish(new Error('independent_writer_timeout_' + type)), 4000);
      worker.on('message', message); worker.once('error', failure); worker.once('exit', exited);
    });
  }
  function keys() {
    const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
    const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
    return { key: signing.privateKey, jwk: signing.publicKey.export({ format: 'jwk' }), encryption: encryption.publicKey.export({ format: 'jwk' }) };
  }
  async function request(service, identity, op, args = {}) {
    const challenge = checkReply(await service.handle({ op: 'challenge', args: { operation: op, digest: digestArgs(args) }, origin: ORIGIN }));
    return { op, args, origin: ORIGIN, proof: { publicJwk: identity.jwk, challengeId: challenge.challengeId,
      signature: sign('sha256', Buffer.from(challenge.message), { key: identity.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
  }
  const call = async (service, identity, op, args = {}) => checkReply(await service.handle(await request(service, identity, op, args)));
  const bootstrap = (service, identity) => call(service, identity, 'bootstrap', { label: 'Independent owner', encryptionPublicJwk: identity.encryption });
  async function enroll(service, owner, identity) {
    const status = await call(service, owner, 'status');
    const started = await call(service, identity, 'enrollment.start', { label: 'Independent device', encryptionPublicJwk: identity.encryption });
    await call(service, owner, 'enrollment.approve', { requestId: started.requestId, wrappedKey: { ciphertext: 'synthetic-only' } });
    return call(service, identity, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: status.accountId });
  }
  const timeoutValue = db => db.prepare('PRAGMA busy_timeout').get().timeout;

  test('independent real writer: revoke ordering is observed inside successive authority fences', async t => {
    const f = fixture(t), owner = keys(), creator = keys(), second = keys();
    await bootstrap(f.service, owner);
    const actor = await enroll(f.service, owner, creator), laterActor = await enroll(f.service, owner, second);
    const insideRequest = await request(f.service, owner, 'device.revoke', { deviceId: actor.deviceId });
    const priorRequest = await request(f.service, owner, 'device.revoke', { deviceId: laterActor.deviceId });
    const peer = await f.writer('signed');
    let effects = 0;
    const completed = waitFor(peer.worker, 'completed');
    f.service.withAuthorityFence(() => {
      peer.worker.postMessage({ request: insideRequest });
      if (!Atomics.load(peer.flags, 0)) Atomics.wait(peer.flags, 0, 0, 2000);
      assert.equal(Atomics.load(peer.flags, 0), 1, 'second connection reached actual BEGIN IMMEDIATE');
      assert.equal(Atomics.wait(peer.flags, 1, 0, 140), 'timed-out');
      assert.equal(f.service.isActorActive(actor), true);
      effects++;
    });
    checkReply((await completed).result);
    f.service.withAuthorityFence(() => { if (f.service.isActorActive(actor)) effects++; });
    assert.equal(effects, 1, 'already committed revoke prevents the next authority-dependent action');
    const before = waitFor(peer.worker, 'completed'); peer.worker.postMessage({ request: priorRequest });
    checkReply((await before).result);
    assert.equal(f.service.withAuthorityFence(() => f.service.isActorActive(laterActor)), false);
    assert.equal(f.service.isActorActive({ ...laterActor, accountId: 'different-account' }), false);
  });

  test('independent real contention: exact non-default busy policy is restored after all acquisition paths', async t => {
    const f = fixture(t), identity = keys(); await bootstrap(f.service, identity);
    f.db.exec('PRAGMA busy_timeout=713');
    assert.equal(f.service.withAuthorityFence(() => timeoutValue(f.db)), 100);
    assert.equal(timeoutValue(f.db), 713);
    const signed = await request(f.service, identity, 'profile.rename', { label: 'After the independent writer' });
    const peer = await f.writer('lock'), released = waitFor(peer.worker, 'released');
    peer.worker.postMessage({ releaseLater: true });
    let called = 0;
    assert.throws(() => f.service.withAuthorityFence(() => { called++; }), errorCode('connect_authority_busy'));
    assert.equal(called, 0); assert.equal(timeoutValue(f.db), 713);
    checkReply(await f.service.handle(signed)); await released;
    const downstreamBusy = Object.assign(new Error('downstream busy'), { errcode: 5 });
    assert.throws(() => f.service.withAuthorityFence(() => { throw downstreamBusy; }), error => error === downstreamBusy);
    assert.equal(timeoutValue(f.db), 713);
    assert.equal(f.service.withAuthorityFence(() => 'reusable'), 'reusable');
  });

  test('independent nested entry is refused before input getters or proof consumption; outer failed action still consumes its own proof', async t => {
    let service;
    const extension = { operations: new Set(['independent.refuse']), execute() {
      assert.throws(() => service.withAuthorityFence(() => assert.fail('nested work ran')), errorCode('connect_transaction_nested'));
      assert.throws(() => service.close(), errorCode('connect_transaction_nested'));
      throw new Error('synthetic action failure');
    } };
    const f = fixture(t, [extension]); service = f.service;
    const identity = keys(); await bootstrap(service, identity);
    const signed = await request(service, identity, 'profile.rename', { label: 'Exact pending proof survives' });
    let reads = 0, nested, hostile;
    service.withAuthorityFence(() => {
      hostile = service.handle({ get origin() { reads++; throw new Error('should not be read'); } });
      nested = service.handle(signed);
      assert.throws(() => service.close(), errorCode('connect_transaction_nested'));
    });
    assert.equal((await hostile).error.code, 'connect_transaction_nested');
    assert.equal((await nested).error.code, 'connect_transaction_nested'); assert.equal(reads, 0);
    checkReply(await service.handle(signed));
    const failedAction = await request(service, identity, 'independent.refuse');
    assert.equal((await service.handle(failedAction)).error.code, 'extension_unavailable');
    assert.equal((await service.handle(failedAction)).error.code, 'challenge_consumed');
    const current = await call(service, identity, 'status');
    assert.equal(current.label, 'Exact pending proof survives');
  });

  test('independent synchronous boundary preserves values and rejects deferred callback results', async t => {
    const f = fixture(t), value = Object.freeze({ accepted: true }); let calls = 0;
    assert.equal(f.service.withAuthorityFence(() => value), value);
    for (const invalid of [null, {}, async () => { calls++; }, async function* () { calls++; }]) {
      assert.throws(() => f.service.withAuthorityFence(invalid), errorCode('connect_authority_callback_invalid'));
    }
    assert.equal(calls, 0);
    assert.throws(() => f.service.withAuthorityFence(() => Promise.reject(new Error('deferred rejection'))), errorCode('connect_authority_callback_async'));
    assert.throws(() => f.service.withAuthorityFence(() => ({ then(resolve) { resolve('not a synchronous result'); } })), errorCode('connect_authority_callback_async'));
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(f.db.isTransaction, false);
    assert.equal(f.service.withAuthorityFence(() => 7), 7);
  });

  test('independent primary failure survives a restore fault even when JavaScript throws a falsy value', t => {
    const f = fixture(t), original = DatabaseSync.prototype.exec;
    f.db.exec('PRAGMA busy_timeout=713');
    for (const primary of [undefined, null, false, 0, '', new Error('primary')]) {
      let caught = false, observed;
      try {
        DatabaseSync.prototype.exec = function(sql) {
          if (this === f.db && sql === 'PRAGMA busy_timeout=713') throw new Error('synthetic restore failure');
          return original.call(this, sql);
        };
        try { f.service.withAuthorityFence(() => { throw primary; }); }
        catch (error) { caught = true; observed = error; }
      } finally { DatabaseSync.prototype.exec = original; f.db.exec('PRAGMA busy_timeout=713'); }
      assert.equal(caught, true); assert.equal(observed, primary);
      assert.equal(f.db.isTransaction, false);
    }
    assert.equal(f.service.withAuthorityFence(() => 'still available'), 'still available');
  });

  test('independent downstream SQLite commit survives Connect commit failures without being called no-effect', t => {
    const f = fixture(t), downstream = f.downstream(), original = DatabaseSync.prototype.exec;
    f.db.exec('PRAGMA busy_timeout=713');
    for (const [index, phase] of ['before-commit', 'after-commit', 'restore'].entries()) {
      const failure = new Error('synthetic ' + phase); let rollbackCalls = 0;
      try {
        DatabaseSync.prototype.exec = function(sql) {
          if (this === f.db) {
            if (sql === 'ROLLBACK') rollbackCalls++;
            if (phase === 'before-commit' && sql === 'COMMIT') throw failure;
            if (phase === 'after-commit' && sql === 'COMMIT') { original.call(this, sql); throw failure; }
            if (phase === 'restore' && sql === 'PRAGMA busy_timeout=713') throw failure;
          }
          return original.call(this, sql);
        };
        assert.throws(() => f.service.withAuthorityFence(() => {
          downstream.exec('BEGIN IMMEDIATE');
          downstream.prepare('INSERT INTO effects(id,value) VALUES(?,?)').run(index + 1, 'synthetic committed effect');
          downstream.exec('COMMIT'); return index + 1;
        }), error => error === failure);
      } finally { DatabaseSync.prototype.exec = original; f.db.exec('PRAGMA busy_timeout=713'); }
      assert.equal(downstream.prepare('SELECT count(*) AS n FROM effects').get().n, index + 1);
      assert.equal(rollbackCalls, phase === 'before-commit' ? 1 : 0);
      assert.equal(f.db.isTransaction, false); assert.equal(timeoutValue(f.db), 713);
      assert.equal(f.service.withAuthorityFence(() => 'ready'), 'ready');
    }
  });
}
