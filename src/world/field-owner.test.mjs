import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createWorldService } from '../../modules/world/server/index.mjs';
import { createHash } from 'node:crypto';
import { createFieldPersistence } from './field-persistence.ts';

const account = { accountId: 'persist_owner', deviceId: 'persist_browser', label: 'Раскладка' };
const other = { accountId: 'persist_other', deviceId: 'persist_other_browser', label: 'Другой' };
const doc = x => ({ schema: 'soty.field.v1', contexts: [{ contextId: 'context', title: 'Мои приложения', x, y: 0 }], shortcuts: [] });
const failure = code => Object.assign(new Error(code), { code });
const defer = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const turn = () => new Promise(done => setImmediate(done));
const until = async check => { const end = Date.now() + 2000; while (!check()) { if (Date.now() > end) assert.fail('Expected controlled phase was not reached'); await new Promise(done => setTimeout(done, 5)); } };
function memoryStore(notifying = false) {
  let record = null, quota = false; const listeners = new Set();
  return { setQuota: value => { quota = value; }, value: () => structuredClone(record),
    port: () => ({ async read() { return structuredClone(record); }, async write(next, expected) {
      if (quota) throw failure('field_local_storage_unavailable');
      if ((record?.localRevision ?? 0) !== expected) throw failure('field_local_revision_conflict');
      record = structuredClone(next);
      if (notifying) for (const listener of listeners) queueMicrotask(listener);
    }, subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); } }) };
}
function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-field-persistence-'));
  const world = createWorldService({ databasePath: join(directory, 'world.sqlite'), projectId: 'field-persistence-tests' });
  let offline = false, loseNextAck = false, writes = 0, gate = null;
  world.execute({ actor: account, op: 'world.profile.get' }); world.execute({ actor: other, op: 'world.profile.get' });
  const models = [];
  const api = { async request(op, args) {
    if (gate) await gate.promise;
    if (offline) throw failure('network_offline');
    const value = world.execute({ actor: account, op, args });
    if (op === 'world.field.put') { writes++; if (loseNextAck) { loseNextAck = false; throw failure('network_lost_ack'); } }
    return value;
  } };
  const create = (store, extra = {}) => { const model = createFieldPersistence({ api, accountId: account.accountId, store, ...extra }); models.push(model); return model; };
  t.after(() => { models.forEach(model => model.dispose()); world.close(); rmSync(directory, { recursive: true, force: true }); });
  return { create, api, world, setOffline: value => { offline = value; }, loseAck: () => { loseNextAck = true; }, writes: () => writes,
    gate: value => { gate = value; }, current: () => world.execute({ actor: account, op: 'world.field.get', args: { expectedAccountId: account.accountId } }) };
}

test('pending worker and screen share one in-flight field GET; unsubscribe leaves owner draining', { timeout: 5000 }, async t => {
  const f = fixture(t), store = memoryStore(), author = f.create(store.port()); await author.load();
  f.setOffline(true); await author.commit(doc(31), { expectedRevision: 0, requestId: 'owner_shared_pending' }); author.dispose(); f.setOffline(false);
  const gate = defer(), calls = []; let events = 0;
  const api = { async request(op, args) { if (op === 'world.field.get') { calls.push(op); await gate.promise; } return f.api.request(op, args); } };
  const owner = f.create(store.port(), { api, localFirst: true });
  const background = owner.load({ skipRemoteIfEmpty: true }); await until(() => calls.length === 1);
  const unobserve = owner.subscribe(() => { events++; });
  assert.equal(owner.getState().pendingCount, 1); assert.deepEqual(owner.getState().document, doc(31));
  const screen = owner.load(); await turn(); assert.equal(calls.length, 1);
  unobserve(); const last = events; gate.resolve();
  const [a, b] = await Promise.all([background, screen]);
  assert.equal(calls.length, 1); assert.equal(events, last); assert.equal(a.pendingCount, 0); assert.equal(b.pendingCount, 0); assert.deepEqual(f.current().document, doc(31));
});

test('empty background check followed by a visible load still performs exactly one fresh GET', async t => {
  const f = fixture(t), store = memoryStore(), calls = [];
  const api = { async request(op, args) { calls.push(op); return f.api.request(op, args); } };
  const owner = f.create(store.port(), { api, localFirst: true });
  const [background, screen] = await Promise.all([owner.load({ skipRemoteIfEmpty: true }), owner.load()]);
  assert.equal(background.state, 'saved'); assert.equal(screen.state, 'saved'); assert.deepEqual(calls, ['world.field.get']);
  await owner.load(); assert.deepEqual(calls, ['world.field.get', 'world.field.get']);
});

test('late result for previous account keeps its durable outbox for the next authenticated generation', async t => {
  const f = fixture(t), store = memoryStore(), author = f.create(store.port()); await author.load();
  f.setOffline(true); await author.commit(doc(41), { expectedRevision: 0, requestId: 'owner_old_account' }); author.dispose(); f.setOffline(false);
  const gate = defer(), entered = defer(); let current = true;
  const api = { async request(op, args) { if (op === 'world.field.get') { entered.resolve(); await gate.promise; } return f.api.request(op, args); } };
  const owner = f.create(store.port(), { api, localFirst: true, isCurrent: () => current });
  const loading = owner.load({ skipRemoteIfEmpty: true }), rejected = assert.rejects(loading, { code: 'field_account_changed' });
  await entered.promise; current = false; owner.dispose(); gate.resolve(); await rejected;
  assert.equal(store.value().pending.length, 1); assert.equal(f.current().revision, 0);
  const next = f.create(store.port(), { localFirst: true }); const restored = await next.load();
  assert.equal(restored.pendingCount, 0); assert.deepEqual(f.current().document, doc(41));
});

test('one owner preserves lost ACK identity and resolves it exactly on reconnect', async t => {
  const f = fixture(t), store = memoryStore(), owner = f.create(store.port(), { localFirst: true }); await owner.load();
  f.loseAck(); await owner.commit(doc(51), { expectedRevision: 0, requestId: 'owner_lost_ack' });
  const stalled = await owner.flush(); assert.equal(stalled.pendingCount, 1); assert.equal(f.current().revision, 1);
  const recovered = await owner.retry(); assert.equal(recovered.pendingCount, 0); assert.equal(f.current().revision, 1); assert.equal(f.writes(), 2);
});

test('one owner retains the local choice and remote version through a conflict', async t => {
  const f = fixture(t), store = memoryStore(), owner = f.create(store.port(), { localFirst: true }); await owner.load();
  f.setOffline(true); await owner.commit(doc(61), { expectedRevision: 0, requestId: 'owner_conflict' }); await owner.flush();
  f.world.execute({ actor: { ...account, deviceId: 'synthetic_second_device' }, op: 'world.field.put', args: { expectedAccountId: account.accountId, expectedRevision: 0, requestId: 'owner_other_device', document: doc(62) } });
  f.setOffline(false); const conflict = await owner.retry();
  assert.equal(conflict.state, 'conflict'); assert.deepEqual(conflict.document, doc(61)); assert.deepEqual(conflict.remote.document, doc(62));
  const resolved = await owner.resolveConflict('local'); assert.equal(resolved.state, 'saved'); assert.deepEqual(f.current().document, doc(61));
});

test('owner retains an undurable edit through a reload and explicit storage recovery', async t => {
  const f = fixture(t), store = memoryStore(), owner = f.create(store.port(), { localFirst: true }); await owner.load();
  store.setQuota(true); await assert.rejects(owner.commit(doc(71), { expectedRevision: 0, requestId: 'owner_volatile' }), { code: 'field_local_storage_unavailable' });
  assert.equal(owner.hasUnsavedChanges(), true); assert.deepEqual(owner.exportPending().document, doc(71));
  const reopened = await owner.load(); assert.equal(reopened.state, 'storage-error'); assert.deepEqual(reopened.document, doc(71));
  store.setQuota(false); const recovered = await owner.retry();
  assert.equal(recovered.state, 'saved'); assert.equal(owner.hasUnsavedChanges(), false); assert.deepEqual(f.current().document, doc(71));
});

test('two account owners in different tabs preserve both edits across a local CAS conflict', async t => {
  const f = fixture(t), store = memoryStore(), a = f.create(store.port(), { localFirst: true }), b = f.create(store.port(), { localFirst: true });
  await a.load(); await b.load(); await a.load(); f.setOffline(true);
  await a.commit(doc(40), { expectedRevision: 0, requestId: 'owner_tab_a' });
  await assert.rejects(b.commit(doc(80), { expectedRevision: 0, requestId: 'owner_tab_b' }), { code: 'field_local_revision_conflict' });
  assert.equal(b.hasUnsavedChanges(), true); assert.deepEqual(b.exportPending().document, doc(80));
  f.setOffline(false); const result = await b.resolveConflict('local');
  assert.equal(result.state, 'saved'); assert.deepEqual(f.current().document, doc(80)); assert.equal(f.current().revision, 2);
  assert.equal(f.writes(), 2); assert.equal(store.value().pending.length, 0);
});

test('storage read failure shares the held remote read but cannot claim local durability', async t => {
  const f = fixture(t), gate = defer(), calls = [];
  const store = { async read() { throw failure('field_local_storage_unavailable'); }, async write() { assert.fail('failed store must not be written'); } };
  const api = { async request(op, args) { if (op === 'world.field.get') { calls.push(op); await gate.promise; } return f.api.request(op, args); } };
  const owner = f.create(store, { api, localFirst: true });
  const background = owner.load({ skipRemoteIfEmpty: true }); const screen = owner.load();
  await until(() => calls.length === 1); gate.resolve(); const [a, b] = await Promise.all([background, screen]);
  assert.equal(calls.length, 1); assert.equal(a.state, 'storage-error'); assert.equal(b.state, 'storage-error');
  assert.equal(a.localDurable, false); assert.equal(b.localDurable, false); assert.equal(f.writes(), 0);
});
