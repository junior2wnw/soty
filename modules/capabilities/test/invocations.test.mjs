import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, basename } from 'node:path';
import { Worker } from 'node:worker_threads';
import { initializeCapabilitiesSchema } from '../server/schema.mjs';
import { createInvocationStore } from '../server/invocations.mjs';

// These callbacks isolate the Invocation contract. Real credential/grant-chain tests
// live in the access/acceptance suite; the database and transaction here are real.
function harness(db, { quota = 20, time = 1000 } = {}) {
  // This isolated harness adds three test-only tables. Only a new database goes
  // through the production exact recognizer; reopening the harness is not a
  // product migration test. Real schema/reopen tests use unmodified layouts.
  const version = db.prepare('PRAGMA user_version').get().user_version;
  if (version === 0) initializeCapabilitiesSchema(db, { projectId: 'invocations-test' });
  else assert.equal(version, 1);
  db.exec('PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;');
  db.exec(`CREATE TABLE IF NOT EXISTS test_policy(subject TEXT PRIMARY KEY,enabled INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS test_budget(account TEXT PRIMARY KEY,limit_amount INTEGER,reserved INTEGER,spent INTEGER);
    CREATE TABLE IF NOT EXISTS test_reservations(id TEXT PRIMARY KEY,account TEXT,amount INTEGER,state TEXT,actual INTEGER);`);
  const trusted = new WeakSet();
  let clock = time, policyFailure = null;
  function actor(accountId = 'account_a', clientId = 'client_a', principalId = 'principal_a', grantId = `grant_${clientId}`) {
    const value = Object.freeze({ accountId, clientId, principalId, credentialId: `credential_${clientId}`, grantId, audience: 'test:audience' });
    trusted.add(value);
    db.prepare('INSERT OR IGNORE INTO test_policy VALUES(?,1)').run(principalId);
    db.prepare('INSERT OR IGNORE INTO test_budget VALUES(?,?,0,0)').run(accountId, quota);
    return value;
  }
  const current = actor();
  function authorize(args) {
    if (policyFailure) throw policyFailure;
    const identity = args.action === 'dispatch' ? args.authorizationSnapshot : args.actor;
    assert.ok(args.action === 'dispatch' || trusted.has(identity), 'fixture_untrusted_actor');
    if (db.prepare('SELECT enabled FROM test_policy WHERE subject=?').get(identity.principalId)?.enabled !== 1) throw Object.assign(new Error('fixture_access_revoked'), { code: 'access_denied' });
    if (args.action === 'dispatch') { assert.equal(identity.audience, 'test:audience'); assert.equal(args.invocation.capabilityDigest, 'd'.repeat(64)); }
    if (args.action === 'invoke') assert.deepEqual(Object.keys(args.input).sort(), ['body', 'title']);
    return Object.freeze({ ...identity, rootGrantId: 'root_grant', policyEpoch: 1, expiresAt: 100000,
      capabilityId: 'notes.createDraft', version: 1, capabilityDigest: 'd'.repeat(64),
      resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'],
      executionBinding: { kind: 'native', handler: 'notes.createDraft', version: 1 }, charges: [{ unit: 'invocations', amount: 1 }] });
  }
  function reserveBudget({ authorization, invocationId, charges }) {
    assert.deepEqual(charges, [{ unit: 'invocations', amount: 1 }]);
    const changed = db.prepare('UPDATE test_budget SET reserved=reserved+1 WHERE account=? AND reserved+spent+1<=limit_amount').run(authorization.accountId);
    assert.equal(changed.changes, 1, 'fixture_budget_exhausted');
    const reservationId = `reservation_${invocationId}`;
    db.prepare('INSERT INTO test_reservations VALUES(?,?,1,?,NULL)').run(reservationId, authorization.accountId, 'reserved');
    return { reservationId };
  }
  function settleBudget({ reservationId, disposition, actualCharges }) {
    const saved = db.prepare('SELECT * FROM test_reservations WHERE id=?').get(reservationId);
    assert.ok(saved);
    const actual = disposition === 'spent' ? (actualCharges?.[0]?.amount ?? 1) : 0;
    if (['spent', 'released'].includes(saved.state)) {
      assert.equal(saved.state, disposition); assert.equal(saved.actual, actual); return;
    }
    if (disposition === 'uncertain') { db.prepare("UPDATE test_reservations SET state='uncertain' WHERE id=?").run(reservationId); return; }
    assert.ok(actual >= 0 && actual <= saved.amount);
    db.prepare('UPDATE test_budget SET reserved=reserved-?,spent=spent+? WHERE account=?').run(saved.amount, actual, saved.account);
    db.prepare('UPDATE test_reservations SET state=?,actual=? WHERE id=?').run(disposition, actual, reservationId);
  }
  function transaction(fn) {
    db.exec('BEGIN IMMEDIATE');
    try { const value = fn(); assert.ok(!value?.then); db.exec('COMMIT'); return value; }
    catch (error) { db.exec('ROLLBACK'); throw error; }
  }
  const store = createInvocationStore({ db, transaction, authorize, reserveBudget, settleBudget, clock: () => clock });
  const admit = (overrides = {}) => store.admit({ actor: current, capabilityId: 'notes.createDraft', version: 1,
    idempotencyKey: 'request_0001', input: { title: 'Private', body: 'Initial secret text' }, ...overrides });
  return { store, current, actor, admit, tick: () => clock++, failPolicy: error => { policyFailure = error; }, revoke: principalId => db.prepare('UPDATE test_policy SET enabled=0 WHERE subject=?').run(principalId),
    budget: () => ({ ...db.prepare('SELECT reserved,spent FROM test_budget WHERE account=?').get('account_a') }) };
}

function memory(t, options) { const db = new DatabaseSync(':memory:'); t.after(() => db.close()); return { db, ...harness(db, options) }; }
const created = { kind: 'created', resourceType: 'note', resourceId: 'note_12345678', revision: 1 };
const receipt = { verificationMethod: 'domain_read', artifacts: [{ type: 'note', id: 'note_12345678', revision: 1 }] };
function finish(store, invocationId, overrides = {}) {
  return store.recordResult({ invocationId, status: 'succeeded', effectState: 'committed', effects: [created], receipt,
    disposition: 'spent', actualCharges: [{ unit: 'invocations', amount: 1 }], ...overrides });
}

test('same request survives key order changes; changed payload conflicts; another client has its own namespace', t => {
  const f = memory(t), first = f.admit();
  assert.equal(first.reused, false);
  const replay = f.admit({ input: { body: 'Initial secret text', title: 'Private' } });
  assert.equal(replay.reused, true); assert.equal(replay.invocation.invocationId, first.invocation.invocationId);
  assert.throws(() => f.admit({ input: { title: 'Different', body: 'Initial secret text' } }), /invocation_request_conflict/u);
  const second = f.admit({ actor: f.actor('account_a', 'client_b', 'principal_b') });
  assert.notEqual(second.invocation.invocationId, first.invocation.invocationId);
  assert.deepEqual(f.budget(), { reserved: 2, spent: 0 });
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_dispatch_intents').get().n, 2);
});

test('admission and reservation roll back together after a real SQLite write failure', t => {
  const f = memory(t);
  f.db.exec("CREATE TEMP TRIGGER test_fail_insert BEFORE INSERT ON cap_invocations BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;");
  assert.throws(() => f.admit(), /synthetic failure/u);
  assert.deepEqual(f.budget(), { reserved: 0, spent: 0 });
  for (const table of ['cap_invocations', 'cap_dispatch_intents', 'test_reservations']) assert.equal(f.db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0);
});

test('restart between dispatch and job binding retains one identity and accepts the reconciled original job', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-invocations-'));
  t.after(async () => { assert.equal(dirname(directory), resolve(tmpdir())); assert.match(basename(directory), /^soty-invocations-/); await rm(directory, { recursive: true, force: true }); });
  const file = join(directory, 'capabilities.sqlite'); let db = new DatabaseSync(file);
  try {
    let f = harness(db); const { invocationId } = f.admit().invocation;
    const dispatch = f.store.beginDispatch({ invocationId });
    db.close(); db = new DatabaseSync(file); f = harness(db);
    const again = f.store.beginDispatch({ invocationId });
    assert.equal(again.internalRequestId, dispatch.internalRequestId);
    assert.equal(f.admit().invocation.invocationId, invocationId);
    f.store.bindJob({ invocationId, internalRequestId: again.internalRequestId, jobId: 'job_original' });
    f.store.bindJob({ invocationId, internalRequestId: again.internalRequestId, jobId: 'job_original' });
    assert.throws(() => f.store.bindJob({ invocationId, internalRequestId: again.internalRequestId, jobId: 'job_duplicate' }), /invocation_binding_mismatch/u);
    assert.deepEqual(f.budget(), { reserved: 1, spent: 0 });
  } finally { db.close(); }
});

test('revocation prevents replay, receipt access and dispatch of already accepted work', t => {
  const f = memory(t), { invocationId } = f.admit().invocation;
  f.revoke(f.current.principalId);
  for (const call of [() => f.admit(), () => f.store.get({ actor: f.current, invocationId }), () => f.store.beginDispatch({ invocationId })]) assert.throws(call, /fixture_access_revoked/u);
  assert.equal(f.store.peekDispatch()[0].state, 'pending');
  assert.deepEqual(f.budget(), { reserved: 1, spent: 0 });
});

test('history is scoped to account/client/principal and never includes stored input or authorization', t => {
  const f = memory(t), { invocationId } = f.admit().invocation;
  const others = [f.actor('account_b', 'client_b', 'principal_b'), f.actor('account_a', 'client_c', 'principal_c')];
  for (const actor of others) {
    assert.deepEqual(f.store.list({ actor }).invocations, []);
    assert.throws(() => f.store.get({ actor, invocationId }), /invocation_not_found/u);
    assert.throws(() => f.store.requestCancel({ actor, invocationId }), /invocation_not_found/u);
  }
  assert.throws(() => f.store.get({ actor: { ...f.current }, invocationId }), /fixture_untrusted_actor/u);
  const serialized = JSON.stringify(f.store.get({ actor: f.current, invocationId }));
  for (const forbidden of ['Initial secret text', 'Private', 'credential_', 'authorization', 'internalRequestId', 'reservation_', 'input_json']) assert.equal(serialized.includes(forbidden), false);
});

test('cursor cannot cross clients and pagination remains stable for equal timestamps', t => {
  const f = memory(t);
  for (let i = 0; i < 3; i++) f.admit({ idempotencyKey: `request_${i}` });
  const first = f.store.list({ actor: f.current, limit: 2 });
  const last = f.store.list({ actor: f.current, limit: 2, cursor: first.nextCursor });
  assert.equal(first.invocations.length, 2); assert.equal(last.invocations.length, 1); assert.equal(last.nextCursor, null);
  assert.equal(new Set([...first.invocations, ...last.invocations].map(item => item.invocationId)).size, 3);
  assert.throws(() => f.store.list({ actor: f.actor('account_a', 'client_b', 'principal_b'), cursor: first.nextCursor }), /invocation_invalid_cursor/u);
});

test('history filters grants before pagination even when one principal has interleaved calls', t => {
  const f = memory(t), sibling = f.actor('account_a', 'client_a', 'principal_a', 'grant_rotated');
  const expected = [];
  for (let i = 0; i < 3; i++) {
    f.tick(); f.admit({ idempotencyKey: `original_key_${i}` });
    f.tick(); expected.push(f.admit({ actor: sibling, idempotencyKey: `rotated_key_${i}` }).invocation.invocationId);
  }
  const first = f.store.list({ actor: sibling, limit: 2 });
  const second = f.store.list({ actor: sibling, limit: 2, cursor: first.nextCursor });
  assert.deepEqual([...first.invocations, ...second.invocations].map(value => value.invocationId), expected);
  assert.throws(() => f.store.list({ actor: f.current, cursor: first.nextCursor }), /invocation_invalid_cursor/u);
  assert.throws(() => f.store.get({ actor: f.current, invocationId: expected[0] }), /invocation_not_found/u);
  assert.throws(() => f.store.requestCancel({ actor: f.current, invocationId: expected[0] }), /invocation_not_found/u);
});

test('the internal owner read model spans only its account and never includes input or execution authority', t => {
  const f = memory(t), first = f.admit().invocation.invocationId;
  const sibling = f.actor('account_a', 'client_b', 'principal_b');
  f.tick(); const second = f.admit({ actor: sibling }).invocation.invocationId;
  f.admit({ actor: f.actor('account_other', 'client_other', 'principal_other') });
  f.revoke(sibling.principalId);
  const one = f.store.listForOwner({ accountId: 'account_a', limit: 1 });
  const two = f.store.listForOwner({ accountId: 'account_a', limit: 1, cursor: one.nextCursor });
  assert.deepEqual([one.invocations[0].invocationId, two.invocations[0].invocationId], [first, second]);
  assert.equal(two.nextCursor, null);
  assert.equal(two.invocations[0].clientId, sibling.clientId);
  assert.equal(two.invocations[0].principalId, sibling.principalId);
  assert.equal(two.invocations[0].grantId, sibling.grantId);
  assert.throws(() => f.store.list({ actor: f.current, cursor: one.nextCursor }), /invocation_invalid_cursor/u);
  assert.throws(() => f.store.listForOwner({ accountId: 'account_other', cursor: one.nextCursor }), /invocation_invalid_cursor/u);
  const serialized = JSON.stringify([one, two]);
  for (const forbidden of ['Initial secret text', 'Private', 'credential_', 'authorization', 'internalRequestId', 'reservation_', 'input_json']) assert.equal(serialized.includes(forbidden), false);
});

test('revocation reconciliation releases only provably unstarted work and never masks an outage as revoked', t => {
  for (const stage of ['pending', 'dispatching', 'bound', 'uncertain']) {
    const f = memory(t), { invocationId } = f.admit().invocation;
    assert.equal(f.store.reconcileAuthorization({ invocationId }).authorized, true);
    if (stage !== 'pending') {
      const dispatch = f.store.beginDispatch({ invocationId });
      if (stage === 'bound') f.store.bindJob({ invocationId, internalRequestId: dispatch.internalRequestId, jobId: 'job_running' });
      if (stage === 'uncertain') f.store.markUncertain({ invocationId, effects: [created] });
    }
    f.failPolicy(new Error('synthetic database outage'));
    assert.throws(() => f.store.reconcileAuthorization({ invocationId }), /synthetic database outage/u);
    assert.deepEqual(f.budget(), { reserved: 1, spent: 0 });
    f.failPolicy(null); f.revoke(f.current.principalId);
    const result = f.store.reconcileAuthorization({ invocationId });
    assert.equal(result.authorized, false);
    assert.equal(result.invocation.status, stage === 'pending' ? 'cancelled' : 'cancel_requested');
    assert.deepEqual(f.budget(), { reserved: stage === 'pending' ? 0 : 1, spent: 0 });
    if (stage === 'pending') {
      assert.equal(result.invocation.receipt.errorCode, 'authorization_no_longer_valid');
      assert.deepEqual(f.store.peekDispatch(), []);
    }
    if (stage === 'uncertain') assert.deepEqual(result.invocation.effects, [created]);
    assert.deepEqual(f.store.reconcileAuthorization({ invocationId }), result);
  }
});

test('cancelling before dispatch releases quota; ambiguous dispatch only requests cancellation', t => {
  const f = memory(t), pending = f.admit().invocation.invocationId;
  assert.equal(f.store.requestCancel({ actor: f.current, invocationId: pending }).invocation.status, 'cancelled');
  assert.deepEqual(f.budget(), { reserved: 0, spent: 0 });
  assert.throws(() => f.store.beginDispatch({ invocationId: pending }), /invocation_dispatch_denied/u);
  const started = f.admit({ idempotencyKey: 'request_started' }).invocation.invocationId;
  const dispatch = f.store.beginDispatch({ invocationId: started });
  assert.equal(f.store.requestCancel({ actor: f.current, invocationId: started }).invocation.status, 'cancel_requested');
  assert.deepEqual(f.budget(), { reserved: 1, spent: 0 });
  assert.equal(f.store.bindJob({ invocationId: started, internalRequestId: dispatch.internalRequestId, jobId: 'job_was_created' }).cancelRequested, true);
  const result = finish(f.store, started);
  assert.equal(result.invocation.status, 'succeeded'); assert.equal(result.invocation.cancelRequested, true);
  assert.deepEqual(result.invocation.effects, [created]);
  assert.deepEqual(f.budget(), { reserved: 0, spent: 1 });
  assert.equal(f.store.requestCancel({ actor: f.current, invocationId: started }).invocation.status, 'succeeded');
});

test('uncertain holds the reservation, cannot redispatch, and timeout cannot erase known effects', t => {
  const f = memory(t, { quota: 1 }), { invocationId } = f.admit().invocation;
  f.store.beginDispatch({ invocationId });
  f.store.markUncertain({ invocationId, effects: [created] });
  const repeated = f.store.markUncertain({ invocationId }).invocation;
  assert.deepEqual(repeated.effects, [created]); assert.equal(repeated.effectState, 'unknown');
  assert.deepEqual(f.budget(), { reserved: 1, spent: 0 });
  assert.throws(() => f.store.beginDispatch({ invocationId }), /invocation_reconcile_required/u);
  assert.throws(() => f.admit({ idempotencyKey: 'request_second' }), /fixture_budget_exhausted/u);
  assert.throws(() => finish(f.store, invocationId, { effectState: 'none', effects: [] }), /invocation_known_effect_conflict/u);
  assert.equal(finish(f.store, invocationId).invocation.status, 'succeeded');
  assert.deepEqual(f.budget(), { reserved: 0, spent: 1 });
});

test('terminal receipt is idempotent, does not accept body/secrets, and does not hide committed effects on failure', t => {
  const f = memory(t), { invocationId } = f.admit().invocation;
  f.store.beginDispatch({ invocationId });
  assert.throws(() => finish(f.store, invocationId, { receipt: { ...receipt, body: 'secret' } }), /invocation_invalid_arguments/u);
  assert.throws(() => finish(f.store, invocationId, { effectState: 'unknown', disposition: 'uncertain' }), /invocation_reconcile_required/u);
  const result = finish(f.store, invocationId, { status: 'failed' });
  assert.equal(result.invocation.status, 'failed'); assert.deepEqual(result.invocation.effects, [created]);
  assert.equal(finish(f.store, invocationId, { status: 'failed' }).reused, true);
  assert.throws(() => finish(f.store, invocationId), /invocation_result_conflict/u);
  assert.deepEqual(f.store.peekDispatch(), []);
  assert.deepEqual(f.store.markUncertain({ invocationId }).invocation, result.invocation);
  assert.deepEqual(f.budget(), { reserved: 0, spent: 1 });
});

test('an executed count reservation cannot be released or discounted to zero', t => {
  const f = memory(t, { quota: 1 }), { invocationId } = f.admit().invocation;
  f.store.beginDispatch({ invocationId });
  for (const status of ['succeeded', 'failed', 'cancelled']) {
    assert.throws(() => finish(f.store, invocationId, { status, disposition: 'released' }), /invocation_settlement_conflict/u);
    assert.throws(() => finish(f.store, invocationId, { status, actualCharges: [{ unit: 'invocations', amount: 0 }] }), /invocation_settlement_conflict/u);
  }
  assert.deepEqual(f.budget(), { reserved: 1, spent: 0 });
  finish(f.store, invocationId);
  assert.deepEqual(f.budget(), { reserved: 0, spent: 1 });
  assert.throws(() => f.admit({ idempotencyKey: 'new_after_finished' }), /fixture_budget_exhausted/u);
});

test('two SQLite connections cannot both reserve the last unit before inserting an Invocation', { timeout: 30000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-invocation-race-'));
  t.after(async () => { assert.equal(dirname(directory), resolve(tmpdir())); assert.match(basename(directory), /^soty-invocation-race-/); await rm(directory, { recursive: true, force: true }); });
  const file = join(directory, 'capabilities.sqlite'), db = new DatabaseSync(file);
  const f = harness(db, { quota: 1 });
  const barrier = new SharedArrayBuffer(4), flag = new Int32Array(barrier);
  const code = `const { parentPort, workerData } = require('node:worker_threads');
    (async()=>{ const { DatabaseSync }=await import('node:sqlite');
      const { initializeCapabilitiesSchema }=await import(workerData.schema);
      const { createInvocationStore }=await import(workerData.store);
      const harness=${harness.toString()}; const assert=(await import('node:assert/strict')).default;
      const db=new DatabaseSync(workerData.file); const f=harness(db,{quota:1});
      parentPort.postMessage({ready:true}); Atomics.wait(new Int32Array(workerData.barrier),0,0);
      try { const r=f.admit({idempotencyKey:workerData.key}); parentPort.postMessage({ok:true,id:r.invocation.invocationId}); }
      catch(e){parentPort.postMessage({ok:false,code:e.message});} finally{db.close();}
    })().catch(e=>{parentPort.postMessage({fatal:e.message});process.exitCode=1;});`;
  const workers = [1, 2].map(index => new Worker(code, { eval: true, workerData: { file, barrier, key: `race_key_${index}`,
    schema: new URL('../server/schema.mjs', import.meta.url).href, store: new URL('../server/invocations.mjs', import.meta.url).href } }));
  try {
    const completed = workers.map(worker => new Promise((resolveWorker, reject) => { worker.on('error', reject); worker.on('message', message => { if (!message.ready) resolveWorker(message); }); }));
    await Promise.all(workers.map(worker => new Promise((resolveReady, reject) => { worker.on('error', reject); worker.on('message', message => { if (message.ready) resolveReady(); else if (message.fatal) reject(new Error(message.fatal)); }); })));
    Atomics.store(flag, 0, 1); Atomics.notify(flag, 0, 2);
    const results = await Promise.all(completed);
    assert.equal(results.filter(result => result.ok).length, 1, JSON.stringify(results));
    assert.match(results.find(result => !result.ok).code, /fixture_budget_exhausted/u);
    assert.equal(db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n, 1);
    assert.equal(db.prepare('SELECT count(*) AS n FROM cap_dispatch_intents').get().n, 1);
    assert.deepEqual(f.budget(), { reserved: 1, spent: 0 });
  } finally { Atomics.store(flag, 0, 1); Atomics.notify(flag, 0, 2); await Promise.all(workers.map(worker => worker.terminate())); db.close(); }
});
