import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fork } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService } from '../../connect/server/index.mjs';
import { createCapabilitiesService } from '../server/index.mjs';
import { connectedFixture, PROJECT, ORIGIN, INPUT, identity, signed, code } from './support/native-connected.mjs';

function delegation(f) {
  return createCapabilitiesService({ databasePath: path.join(f.directory, 'caps.sqlite'), projectId: PROJECT,
    clock: () => f.time(), actorActive: actor => f.connect.isActorActive(actor),
    delegation: { audience: ORIGIN, withAuthorityFence: action => f.connect.withAuthorityFence(action) } });
}
function derive(service, token, label = 'Synthetic child') {
  return service.delegation.derive({ actor: service.authenticateCredential({ token, audience: ORIGIN }), label, expiresAt: 9000 });
}
const counts = f => ({
  notes: f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes').get().n),
  proofs: f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n),
  invocations: f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n),
  receipts: f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_receipts').get().n),
  budget: f.sql('caps', db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() }))
});

async function worker(t, f, extra) {
  const child = fork(new URL(import.meta.url), ['--delegation-worker'], { execPath: process.execPath, execArgv: [],
    windowsHide: true, stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
  const queue = [], waiters = [];
  let closed = false, failed = false;
  const exit = new Promise(resolve => child.once('close', (code, signal) => {
    closed = true; for (const waiter of waiters.splice(0)) waiter.reject(new Error('delegation_worker_closed'));
    resolve({ code, signal });
  }));
  child.once('error', () => { failed = true; for (const waiter of waiters.splice(0)) waiter.reject(new Error('delegation_worker_failed')); });
  child.on('message', value => {
    const index = waiters.findIndex(item => item.phase === value.phase);
    if (index < 0) queue.push(value); else waiters.splice(index, 1)[0].resolve(value);
  });
  const wait = phase => new Promise((resolve, reject) => {
    const index = queue.findIndex(item => item.phase === phase);
    if (index >= 0) return resolve(queue.splice(index, 1)[0]);
    if (closed || failed) return reject(new Error('delegation_worker_unavailable'));
    const timer = setTimeout(() => { child.kill(); reject(new Error('delegation_worker_timeout')); }, 10000);
    waiters.push({ phase, resolve(value) { clearTimeout(timer); resolve(value); }, reject(error) { clearTimeout(timer); reject(error); } });
  });
  const kill = async () => { if (!closed) child.kill('SIGKILL'); await exit; };
  // Each test also awaits exit before the fixture can delete its SQLite files.
  t.after(kill);
  const ready = wait('ready'); child.send({ configure: { directory: f.directory, time: f.time(), ...extra } });
  await ready;
  return { wait, exit, kill, start() { child.send({ start: true }); }, release() { child.send({ release: true }); } };
}

if (process.argv.includes('--delegation-worker')) {
  let config, connect, caps, db;
  process.on('message', async message => {
    if (message.configure) {
      config = message.configure;
      if (config.mode === 'lock') db = new DatabaseSync(path.join(config.directory, `${config.store}.sqlite`));
      else {
        connect = createConnectService({ databasePath: path.join(config.directory, 'connect.sqlite'), projectId: PROJECT,
          allowedOrigins: [ORIGIN], clock: () => config.time });
        if (config.mode === 'derive-crash') caps = createCapabilitiesService({ databasePath: path.join(config.directory, 'caps.sqlite'),
          projectId: PROJECT, clock: () => config.time, actorActive: actor => connect.isActorActive(actor),
          delegation: { audience: ORIGIN, withAuthorityFence(action) {
            return connect.withAuthorityFence(() => {
              const result = action();
              process.send({ phase: 'committed' });
              const stopped = new Int32Array(new SharedArrayBuffer(4));
              while (true) Atomics.wait(stopped, 0, 0, 1000);
              // The issued plaintext result never leaves this killed process.
              return result;
            });
          } } });
      }
      process.send({ phase: 'ready' }); return;
    }
    if (message.release) { db.exec('ROLLBACK'); db.close(); process.disconnect(); return; }
    if (!message.start) return;
    try {
      if (config.mode === 'lock') { db.exec('BEGIN IMMEDIATE'); process.send({ phase: 'locked' }); return; }
      if (config.mode === 'derive-crash') derive(caps, config.token);
      else {
        const value = await connect.handle(config.request);
        process.send({ phase: 'result', ok: value.ok, code: value.error?.code });
      }
    } catch (error) { process.send({ phase: 'result', ok: false, code: typeof error?.code === 'string' ? error.code : 'test_failure' }); }
    caps?.close(); connect.close(); process.disconnect();
  });
} else {
  test('signed owner enables delegation; children create private Notes once with isolated histories and owner visibility', async t => {
    const f = await connectedFixture(t), parent = await f.issue({ allowDelegation: true, maxDepth: 1, budget: 3 }), service = delegation(f);
    try {
      const a = derive(service, parent.credential.token, 'Child A'), b = derive(service, parent.credential.token, 'Child B');
      const actorA = f.actor(a.token), actorB = f.actor(b.token), key = 'same_key_different_children';
      const first = f.create(actorA, INPUT, key), second = f.create(actorB, INPUT, key);
      assert.notEqual(first.invocation.invocationId, second.invocation.invocationId);
      const replay = f.native.admit({ actor: actorA, input: INPUT, idempotencyKey: key });
      assert.equal(replay.reused, true); assert.equal(replay.invocation.invocationId, first.invocation.invocationId);
      for (const actor of [parent.actor, actorB]) {
        assert.throws(() => f.native.get({ actor, invocationId: first.invocation.invocationId }), code('invocation_not_found'));
      }
      const history = await f.access('invocations.list', { limit: 20 });
      assert.equal(history.invocations.length, 2);
      assert.deepEqual(new Set(history.invocations.map(item => item.principalId)), new Set([a.principal.id, b.principal.id]));
      const noteId = first.invocation.receipt.artifacts[0].id;
      assert.equal((await f.call('notes.get', { expectedAccountId: f.account.accountId, noteId })).note.body, INPUT.body);
      const events = (await f.access('events.list', { limit: 20 })).events.filter(item => item.actorType === 'service');
      assert.equal(events.length, 2); assert.ok(events.every(item => item.actorId === parent.principal.id));
      assert.deepEqual(counts(f), { notes: 2, proofs: 2, invocations: 2, receipts: 2, budget: { reserved_amount: 0, spent_amount: 2 } });
    } finally { service.close(); }
  });

  test('actual parent credential revoke does not cancel a child original invocation; root grant revoke does', async t => {
    for (const target of ['credential', 'grant']) {
      const f = await connectedFixture(t), parent = await f.issue({ allowDelegation: true, maxDepth: 1 }), service = delegation(f);
      try {
        const child = derive(service, parent.credential.token), actor = f.actor(child.token);
        const admitted = f.native.admit({ actor, input: INPUT, idempotencyKey: 'original_child_authority' });
        f.native.beginAttempt({ invocationId: admitted.invocation.invocationId });
        await f.access(target === 'credential' ? 'credentials.revoke' : 'grants.revoke',
          target === 'credential' ? { credentialId: parent.credential.credential.id } : { grantId: parent.grant.id });
        const done = f.native.execute({ invocationId: admitted.invocation.invocationId });
        assert.equal(done.outcome, target === 'credential' ? 'committed' : 'not_applied');
        assert.equal(counts(f).notes, target === 'credential' ? 1 : 0);
        assert.equal(counts(f).budget.spent_amount, target === 'credential' ? 1 : 0);
      } finally { service.close(); }
    }
  });

  test('second real Connect writer revokes the inherited creator before child dispatch and a retained parent derive', async t => {
    const f = await connectedFixture(t), phone = identity('Second approver');
    const start = await f.call('enrollment.start', { label: phone.label, encryptionPublicJwk: phone.encryptionPublicJwk }, phone);
    await f.call('enrollment.approve', { requestId: start.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'synthetic' } });
    await f.call('enrollment.finish', { requestId: start.requestId, expectedAccountId: f.account.accountId }, phone);
    const parent = await f.issue({ allowDelegation: true, maxDepth: 1 }), service = delegation(f);
    let peer;
    try {
      const retained = service.authenticateCredential({ token: parent.credential.token, audience: ORIGIN });
      const child = derive(service, parent.credential.token), actor = f.actor(child.token);
      const admitted = f.native.admit({ actor, input: INPUT, idempotencyKey: 'creator_revoke_child' });
      f.native.beginAttempt({ invocationId: admitted.invocation.invocationId });
      const request = await signed(f.connect, phone, 'device.revoke', { deviceId: f.account.deviceId });
      peer = await worker(t, f, { mode: 'signed', request });
      const result = peer.wait('result'); peer.start(); assert.equal((await result).ok, true); await peer.exit;
      assert.throws(() => service.delegation.derive({ actor: retained, label: 'blocked', expiresAt: 9000 }), code('access_denied'));
      assert.equal(f.native.execute({ invocationId: admitted.invocation.invocationId }).outcome, 'not_applied');
      assert.equal(counts(f).notes, 0); assert.equal(counts(f).budget.reserved_amount, 0);
    } finally { await peer?.kill(); service.close(); }
  });

  test('two real child processes contend for the last shared root unit without a second admission or Note', async t => {
    const f = await connectedFixture(t), parent = await f.issue({ allowDelegation: true, maxDepth: 1, budget: 1 }), service = delegation(f);
    try {
      const children = [derive(service, parent.credential.token, 'race A'), derive(service, parent.credential.token, 'race B')];
      const peers = [];
      for (let i = 0; i < 2; i++) peers.push(await f.child({ token: children[i].token, key: 'shared_child_race_key' }));
      const pending = peers.map(peer => peer.wait('result')); peers.forEach(peer => peer.start());
      const results = await Promise.all(pending); await Promise.all(peers.map(peer => peer.exit));
      assert.ok(results.every(result => result.ok || ['budget_exceeded', 'connect_authority_busy', 'native_storage_busy'].includes(result.code)));
      for (let i = 0; i < results.length; i++) {
        if (['connect_authority_busy', 'native_storage_busy'].includes(results[i].code)) {
          // Only the idempotent Note operation retries, never delegation/issuance.
          try { const done = f.create(f.actor(children[i].token), INPUT, 'shared_child_race_key'); results[i] = { ok: true, invocationId: done.invocation.invocationId }; }
          catch (error) { assert.equal(error.code, 'budget_exceeded'); results[i] = { ok: false, code: error.code }; }
        }
      }
      assert.equal(results.filter(result => result.ok).length, 1);
      assert.deepEqual(counts(f), { notes: 1, proofs: 1, invocations: 1, receipts: 1, budget: { reserved_amount: 0, spent_amount: 1 } });
    } finally { service.close(); }
  });

  test('actual writer locks fail before issuance, release Connect, and restore the Caps timeout', async t => {
    for (const store of ['connect', 'caps']) {
      const f = await connectedFixture(t), parent = await f.issue({ allowDelegation: true, maxDepth: 1 }), service = delegation(f);
      let peer;
      try {
        const before = f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_principals').get().n);
        const prepare = DatabaseSync.prototype.prepare, seen = [];
        // Observe the exact service handle's PRAGMA through the documented SQL call.
        DatabaseSync.prototype.prepare = function(sql) {
          const statement = prepare.call(this, sql);
          if (sql === 'PRAGMA busy_timeout') { const get = statement.get.bind(statement); statement.get = (...args) => { const value = get(...args); seen.push(value.timeout); return value; }; }
          return statement;
        };
        try {
          peer = await worker(t, f, { mode: 'lock', store });
          const locked = peer.wait('locked'); peer.start(); await locked;
          assert.throws(() => derive(service, parent.credential.token), code(store === 'caps' ? 'delegation_storage_busy' : 'connect_authority_busy'));
        } finally { DatabaseSync.prototype.prepare = prepare; }
        assert.equal(f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_principals').get().n), before);
        if (store === 'caps') assert.equal(f.connect.withAuthorityFence(() => true), true, 'Caps failure released the outer Connect fence');
        peer.release(); await peer.exit;
        const exec = DatabaseSync.prototype.exec, restoration = [];
        DatabaseSync.prototype.exec = function(sql) { if (sql.startsWith('PRAGMA busy_timeout=')) restoration.push(sql); return exec.call(this, sql); };
        try { assert.equal(derive(service, parent.credential.token).grant.parentGrantId, parent.grant.id); }
        finally { DatabaseSync.prototype.exec = exec; }
        assert.ok(seen.includes(5000));
        assert.ok(restoration.includes('PRAGMA busy_timeout=100')); assert.ok(restoration.includes('PRAGMA busy_timeout=5000'));
      } finally { await peer?.kill(); service.close(); }
    }
  });

  test('kill after child Caps COMMIT before caller reply preserves one owner-visible revocable child, never a recoverable token', async t => {
    const f = await connectedFixture(t), parent = await f.issue({ allowDelegation: true, maxDepth: 1 });
    const before = f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_credentials').get().n);
    const peer = await worker(t, f, { mode: 'derive-crash', token: parent.credential.token });
    try {
      const committed = peer.wait('committed'); peer.start(); await committed; await peer.kill();
      f.restart();
      assert.equal(f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_credentials').get().n), before + 1);
      const grants = (await f.access('grants.list', { limit: 20 })).grants;
      const child = grants.find(grant => grant.parentGrantId === parent.grant.id); assert.ok(child);
      const events = (await f.access('events.list', { limit: 20 })).events.filter(event => event.actorType === 'service');
      assert.equal(events.length, 1); assert.equal(events[0].objectId, child.id); assert.equal(events[0].actorId, parent.principal.id);
      assert.equal(Object.hasOwn(child, 'token'), false);
      const result = await f.access('grants.revoke', { grantId: child.id }); assert.notEqual(result.grant.revokedAt, null);
      assert.equal(counts(f).notes, 0); assert.equal(counts(f).invocations, 0); assert.equal(counts(f).budget.spent_amount, 0);
    } finally { await peer.kill(); }
  });
}
