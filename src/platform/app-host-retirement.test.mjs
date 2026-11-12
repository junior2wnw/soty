import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createAppHostRetirement } from './app-host-retirement.mjs';
import { createHostSessionContinuity } from '../../modules/apps/server/host-session-continuity.mjs';
import { FRAME_CONTINUITY_STORAGE, frameContinuityDdl } from '../../modules/apps/server/frame-continuity.mjs';

const KEY = 'soty.app-host-retirement.v1';
const actor = { accountId: 'account-A', deviceId: 'device-A' }, appId = 'app-' + 'a'.repeat(32);
let sequence = 0;
const randomId = () => 'document_' + String(++sequence).padStart(20, '0');
function storage(copy) {
  const values = new Map(copy?.values);
  return { values, failWrites: false, getItem: key => values.get(key) ?? null,
    setItem(key, value) { if (this.failWrites) throw Error('synthetic quota'); values.set(key, value); } };
}
function lockManager() {
  const held = new Map();
  return {
    forDocument(document) {
      return { request(name, options, callback) {
        assert.deepEqual(options, { ifAvailable: true });
        if (held.has(name)) return Promise.resolve(callback(null));
        return new Promise((resolve, reject) => {
          const owner = { document, resolve, reject }; held.set(name, owner);
          Promise.resolve(callback({ name })).then(value => {
            if (held.get(name) === owner) held.delete(name); resolve(value);
          }, reject);
        });
      } };
    },
    terminate(document) {
      for (const [name, owner] of held) if (owner.document === document) { held.delete(name); owner.resolve(); }
    },
  };
}
function fixture(t) {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec('CREATE TABLE local_apps(id TEXT PRIMARY KEY); CREATE TABLE apps_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);');
  db.prepare('INSERT INTO local_apps VALUES(?)').run(appId); db.exec(frameContinuityDdl());
  db.prepare('INSERT INTO apps_meta VALUES(?,?)').run('frame_continuity', FRAME_CONTINUITY_STORAGE);
  const store = createHostSessionContinuity({ db, clock: () => 1000 }), locks = lockManager(), disk = storage(), calls = [];
  const scope = (widget = false, pair = actor) => ({ schema: 'soty.app-host-scope.v1', ...pair, appId, policyEpoch: 1,
    entry: { domainId: 'dom_' + 'b'.repeat(32), origin: 'https://app.example', pathDigest: 'c'.repeat(64) },
    target: { revision: 1, digest: 'd'.repeat(64), profile: 'soty.relay-restricted.v1', bindingFloor: 1 },
    instanceId: widget ? 'widget-11111111-1111-4111-8111-111111111111' : 'main',
    runtimeOrigin: 'https://' + (widget ? 'app-w11111111111141118111111111111111' : 'app-m' + appId.slice(4)) + '-' + 'e'.repeat(24) + '.example', runtimeScopeDigest: 'e'.repeat(64) });
  const create = (widget = false, pair = actor) => {
    const value = store.create(scope(widget, pair)); value.claim = store.activateInitial(value.claim); return value;
  };
  const close = async (pair, proof) => {
    const closed = store.close(pair, proof.appId, proof); calls.push({ pair, proof, closed }); return { closed };
  };
  const helper = (document, options = {}) => createAppHostRetirement({ storage: disk, locks: locks.forDocument(document), close, randomId, ...options });
  const records = () => JSON.parse(disk.getItem(KEY)).records;
  return { db, store, locks, disk, calls, create, scope, helper, records, close };
}
const capture = value => ({ appId, hostSession: value.descriptor });
const nextTurn = () => new Promise(resolve => setImmediate(resolve));

async function reloadOracle(f, useEscrow) {
  const otherTab = f.create(), widget = f.create(true);
  for (let index = 0; index < 20; index++) {
    const document = 'root-' + index, helper = f.helper(document);
    if (useEscrow) assert.equal(await helper.beforeLaunch(actor, () => true), true);
    const value = f.store.create(f.scope());
    if (useEscrow) { helper.record(actor, capture(value)); assert.equal(f.store.admitting(value.claim), true); }
    value.claim = f.store.activateInitial(value.claim);
    f.store.assertClaim(otherTab.claim); f.store.assertClaim(widget.claim);
    if (useEscrow) assert.equal(f.db.prepare("SELECT count(*) n FROM app_frame_continuity WHERE state='open'").get().n, 3);
    f.locks.terminate(document); await nextTurn();
  }
  return { otherTab, widget };
}
test('the unchanged 20-document reload oracle detects the old 17th-family allocation failure', async t => {
  const f = fixture(t); await assert.rejects(reloadOracle(f, false), { code: 'apps_sessions_busy' });
  assert.equal(f.calls.length, 0);
});
test('20 full document reloads reuse one capacity slot and retain another tab and widget', async t => {
  const f = fixture(t), sentinels = await reloadOracle(f, true);
  assert.equal(f.calls.length, 19); assert.equal(f.records().length, 1);
  f.store.assertClaim(sentinels.otherTab.claim); f.store.assertClaim(sentinels.widget.claim);
});
test('cloned sessionStorage while the original lock is held forks only the copied ledger', async t => {
  const f = fixture(t), first = f.helper('original'); await first.beforeLaunch(actor, () => true);
  const value = f.create(); first.record(actor, capture(value)); const original = f.disk.getItem(KEY), copied = storage(f.disk);
  const clone = f.helper('clone', { storage: copied }); assert.equal(await clone.beforeLaunch(actor, () => true), true);
  assert.equal(f.calls.length, 0); assert.equal(f.disk.getItem(KEY), original); f.store.assertClaim(value.claim);
  assert.notEqual(JSON.parse(copied.getItem(KEY)).lineageId, JSON.parse(original).lineageId);
  assert.deepEqual(JSON.parse(copied.getItem(KEY)).records, []);
});
test('active current-document app and widget records survive, only explicitly retired proof drains', async t => {
  const f = fixture(t), helper = f.helper('current'); await helper.beforeLaunch(actor, () => true);
  const main = f.create(), widget = f.create(true), retired = helper.record(actor, capture(main)); helper.record(actor, capture(widget));
  await helper.beforeLaunch(actor, () => true); assert.equal(f.calls.length, 0);
  retired(); assert.equal(f.records().length, 2, 'marking is not a server close ACK');
  await helper.beforeLaunch(actor, () => true); assert.equal(f.calls.length, 1); f.store.assertClaim(widget.claim);
  assert.equal(f.records().length, 1); assert.equal(f.records()[0].frameId, widget.descriptor.frameId);
});
test('wrong account or device does not drain old records and identity loss fences every await', async t => {
  const f = fixture(t), old = f.helper('old'); await old.beforeLaunch(actor, () => true); const value = f.create(); old.record(actor, capture(value));
  f.locks.terminate('old'); await nextTurn(); const helper = f.helper('next');
  for (const pair of [{ ...actor, accountId: 'account-B' }, { ...actor, deviceId: 'device-B' }]) await helper.beforeLaunch(pair, () => true);
  assert.equal(f.calls.length, 0); assert.equal(f.records().length, 1); f.store.assertClaim(value.claim);
  await assert.rejects(helper.beforeLaunch(actor, () => false), { code: 'app_host_retirement_changed' });
  await helper.beforeLaunch(actor, () => true); assert.equal(f.calls.length, 1);
});
test('identity changing during a close ACK keeps the obligation and blocks a fresh launch', async t => {
  const f = fixture(t), old = f.helper('old'); await old.beforeLaunch(actor, () => true); const value = f.create(); old.record(actor, capture(value));
  f.locks.terminate('old'); await nextTurn(); let current = true, interrupt = true;
  const helper = f.helper('next', { close: async (scope, proof) => { const reply = await f.close(scope, proof); if (interrupt) { interrupt = false; current = false; } return reply; } });
  await assert.rejects(helper.beforeLaunch(actor, () => current), { code: 'app_host_retirement_changed' });
  assert.equal(f.records().length, 1); current = true;
  await helper.beforeLaunch(actor, () => current); assert.equal(f.calls.at(-1).closed, false); assert.equal(f.records().length, 0);
});
test('known candidate persisted before confirm survives lost ACK; stale cleanup cannot close another lineage', async t => {
  const f = fixture(t), old = f.helper('old'); await old.beforeLaunch(actor, () => true);
  const sentinel = f.create(), initial = f.create(); old.record(actor, capture(initial)); const candidate = f.store.prepare(initial.claim);
  old.record(actor, capture(candidate)); assert.equal(f.records().length, 2, 'both exact proofs precede the server CAS');
  f.store.promote(candidate.claim); // The document is lost before its confirm ACK.
  f.locks.terminate('old'); await nextTurn(); await f.helper('next').beforeLaunch(actor, () => true);
  assert.deepEqual(f.calls.map(value => value.closed), [false, true]); f.store.assertClaim(sentinel.claim);
  assert.equal(f.records().length, 0);
});
async function confirmedRenewalOracle(f, compact) {
  const helper = f.helper('current'); await helper.beforeLaunch(actor, () => true);
  const initial = f.create(), widget = f.create(true), otherTab = f.create();
  helper.record(actor, capture(initial)); helper.record(actor, capture(widget));
  const otherScope = { ...actor, deviceId: 'device-other' }, otherApp = 'app-' + 'b'.repeat(32);
  helper.record(otherScope, capture(initial)); helper.record(actor, { ...capture(initial), appId: otherApp });
  let candidate = f.store.prepare(initial.claim); helper.record(actor, capture(candidate));
  for (let turn = 0; turn < 600; turn++) {
    f.store.promote(candidate.claim);
    const future = f.store.prepare(candidate.claim); helper.record(actor, capture(future));
    helper.record(actor, capture(candidate), compact);
    const own = f.records().filter(value => value.accountId === actor.accountId && value.deviceId === actor.deviceId
      && value.appId === appId && value.frameId === initial.descriptor.frameId);
    assert.equal(own.length, 2); assert.deepEqual(own.map(value => value.revision).sort((a, b) => a - b), [candidate.descriptor.revision, future.descriptor.revision]);
    assert.equal(f.records().length, 5); assert.equal(f.calls.length, 0);
    f.store.assertClaim(otherTab.claim); f.store.assertClaim(widget.claim); candidate = future;
  }
}
test('600 confirmed renewals compact only proven predecessors and preserve future candidate, widget and other scope', async t => {
  await confirmedRenewalOracle(fixture(t), true);
});
test('the unchanged renewal-growth oracle rejects the original no-compaction behavior', async t => {
  await assert.rejects(confirmedRenewalOracle(fixture(t), false), assert.AssertionError);
});
test('unconfirmed candidates retain predecessors; unknown or nonboolean confirmation cannot compact anything', async t => {
  const f = fixture(t), helper = f.helper('current'); await helper.beforeLaunch(actor, () => true);
  const initial = f.create(), candidate = f.store.prepare(initial.claim); helper.record(actor, capture(initial));
  const before = f.disk.getItem(KEY);
  assert.throws(() => helper.record(actor, capture(candidate), true), { code: 'app_host_retirement_pending' }); assert.equal(f.disk.getItem(KEY), before);
  assert.throws(() => helper.record(actor, capture(initial), 'confirmed'), { code: 'app_host_retirement_unavailable' }); assert.equal(f.disk.getItem(KEY), before);
  helper.record(actor, capture(candidate)); assert.equal(f.records().length, 2);
  const mark = helper.record(actor, capture(initial)); helper.record(actor, capture(initial), true);
  assert.equal(f.records().length, 2, 'confirming the predecessor never removes a higher prepared proof');
  mark(); assert.equal(f.records().find(value => value.revision === 1).retired, true);
});
test('unknown or incomplete signed close result retains the proof; retry uses the same proof', async t => {
  for (const result of [undefined, { ok: true }, { closed: true, ready: true }]) {
    const f = fixture(t), helper = f.helper('current', { close: async () => result }); await helper.beforeLaunch(actor, () => true);
    const value = f.create(), retire = helper.record(actor, capture(value)); retire();
    await assert.rejects(helper.beforeLaunch(actor, () => true), { code: 'app_host_retirement_pending' });
    assert.equal(f.records().length, 1); f.store.assertClaim(value.claim);
  }
});
test('a lost close ACK preserves the obligation and the later exact stale-CAS false clears it', async t => {
  const f = fixture(t); let lose = true;
  const helper = f.helper('current', { close: async (scope, proof) => { const result = await f.close(scope, proof); if (lose) { lose = false; throw Error('lost ACK'); } return result; } });
  await helper.beforeLaunch(actor, () => true); helper.record(actor, capture(f.create()))();
  await assert.rejects(helper.beforeLaunch(actor, () => true), { code: 'app_host_retirement_pending' }); assert.equal(f.records().length, 1);
  await helper.beforeLaunch(actor, () => true); assert.equal(f.records().length, 0); assert.deepEqual(f.calls.map(value => value.closed), [true, false]);
});
test('failed synchronous storage never admits a frame or erases prior obligations', async t => {
  const f = fixture(t), helper = f.helper('current'); await helper.beforeLaunch(actor, () => true);
  const reserved = f.store.create(f.scope()), before = f.disk.getItem(KEY); f.disk.failWrites = true;
  assert.throws(() => helper.record(actor, capture(reserved)), { code: 'app_host_retirement_unavailable' });
  assert.equal(f.disk.getItem(KEY), before); assert.equal(f.store.admitting(reserved.claim), true);
});
test('missing storage/locks and corrupt ledger fail closed without replacing saved bytes', async () => {
  const close = async () => ({ closed: true }), disk = storage(); disk.setItem(KEY, 'unreadable-old-state');
  for (const ports of [{ storage: null, locks: lockManager().forDocument('a') }, { storage: disk, locks: null }, { storage: disk, locks: lockManager().forDocument('b') }]) {
    const helper = createAppHostRetirement({ close, randomId, ...ports }); assert.equal(await helper.beforeLaunch(actor, () => true), false);
    assert.throws(() => helper.record(actor, { appId, hostSession: {} }), { code: 'app_host_retirement_unavailable' });
  }
  assert.equal(disk.getItem(KEY), 'unreadable-old-state');
});
test('unavailable browser randomness is lazy and cannot break Source boot during construction', async () => {
  let calls = 0;
  const helper = createAppHostRetirement({ storage: storage(), locks: null, close: async () => ({ closed: true }),
    randomId() { calls++; throw Error('no secure context'); } });
  assert.equal(calls, 0); assert.equal(await helper.beforeLaunch(actor, () => true), false); assert.equal(calls, 0);
  const unusable = createAppHostRetirement({ storage: storage(), locks: lockManager().forDocument('a'), close: async () => ({ closed: true }),
    randomId() { throw Error('randomness unavailable'); } });
  assert.equal(await unusable.beforeLaunch(actor, () => true), false);
});
test('a stale in-memory document never overwrites a changed storage ledger', async t => {
  const f = fixture(t), helper = f.helper('current'); await helper.beforeLaunch(actor, () => true);
  helper.record(actor, capture(f.create()))();
  const mark = helper.record(actor, capture(f.create())), replacement = JSON.stringify({ schema: 'changed externally' });
  f.disk.setItem(KEY, replacement);
  assert.throws(mark, { code: 'app_host_retirement_unavailable' }); assert.equal(f.disk.getItem(KEY), replacement);
  await assert.rejects(helper.beforeLaunch(actor, () => true), { code: 'app_host_retirement_unavailable' }); assert.equal(f.calls.length, 0);
});
test('duplicate proof updates cannot resurrect a retired obligation, and bad envelopes cannot be stored', async t => {
  const f = fixture(t), helper = f.helper('current'); await helper.beforeLaunch(actor, () => true); const value = f.create();
  helper.record(actor, capture(value))(); helper.record(actor, capture(value)); assert.equal(f.records()[0].retired, true);
  for (const change of [p => ({ ...p, cookie: 'forbidden' }), p => ({ ...p, hostSession: { ...p.hostSession, sourceReady: true } })]) {
    const before = f.disk.getItem(KEY); assert.throws(() => helper.record(actor, change(capture(value)))); assert.equal(f.disk.getItem(KEY), before);
  }
});
test('record and JSON bounds refuse overflow without losing the previous ledger', async t => {
  for (const pair of [actor, { accountId: 'A'.repeat(160), deviceId: 'D'.repeat(160) }]) {
    const f = fixture(t), helper = f.helper('current'); await helper.beforeLaunch(pair, () => true); let accepted = 0;
    for (let index = 0; index < 513; index++) {
      const before = f.disk.getItem(KEY), proof = { appId, hostSession: { schema: 'soty.app-host-session.v1',
        frameId: String(index).padStart(43, 'F'), lease: 'L'.repeat(43), revision: 1, expiresAt: 9999999 } };
      try { helper.record(pair, proof); accepted++; } catch (error) { assert.equal(error.code, 'app_host_retirement_unavailable'); assert.equal(f.disk.getItem(KEY), before); break; }
    }
    assert(accepted > 0 && accepted <= 512); assert(f.disk.getItem(KEY).length <= 256 * 1024);
  }
});
test('initial same-proof admission can extend expiry; a stale reservation deadline never drops its obligation', async t => {
  const f = fixture(t), first = f.helper('old'); await first.beforeLaunch(actor, () => true);
  const value = f.store.create(f.scope()); first.record(actor, capture(value));
  value.claim = f.store.activateInitial(value.claim); assert(value.claim.expiresAt > value.descriptor.expiresAt);
  assert.equal(f.records()[0].expiresAt, value.descriptor.expiresAt, 'the document missed updated admission metadata');
  f.locks.terminate('old'); await nextTurn(); await f.helper('next').beforeLaunch(actor, () => true);
  assert.equal(f.calls.length, 1); assert.equal(f.calls[0].closed, true); assert.equal(f.records().length, 0);
});
