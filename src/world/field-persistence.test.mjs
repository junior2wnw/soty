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

test('three offline arrangements are locally durable and reload drains exact ordered CAS intents', async t => {
  const f = fixture(t), store = memoryStore(), first = f.create(store.port()); await first.load(); f.setOffline(true);
  for (const [index, x] of [10, 20, 30].entries()) {
    const result = await first.commit(doc(x), { expectedRevision: 0, requestId: `offline_${index}` });
    assert.equal(result.status, 'volatile'); assert.equal(result.localDurable, true); assert.equal(first.hasUnsavedChanges(), false);
  }
  assert.equal(store.value().pending.length, 3); assert.equal(f.writes(), 0); first.dispose(); f.setOffline(false);
  const second = f.create(store.port()), state = await second.load();
  assert.equal(state.state, 'saved'); assert.equal(state.revision, 3); assert.deepEqual(state.document, doc(30)); assert.equal(state.pendingCount, 0);
  assert.deepEqual(f.current().document, doc(30)); assert.equal(f.writes(), 3);
});

test('ACK lost after actual World commit survives reload and replay does not advance revision again', async t => {
  const f = fixture(t), store = memoryStore(), first = f.create(store.port()); await first.load(); f.loseAck();
  const pending = await first.commit(doc(18), { expectedRevision: 0, requestId: 'lost_ack_001' });
  assert.equal(pending.status, 'volatile'); assert.equal(f.current().revision, 1); assert.equal(store.value().pending[0].requestId, 'lost_ack_001');
  first.dispose(); const second = f.create(store.port()), result = await second.load();
  assert.equal(result.state, 'saved'); assert.equal(result.revision, 1); assert.deepEqual(result.document, doc(18));
  assert.equal(f.current().revision, 1); assert.equal(f.writes(), 2);
});

test('quota failure dispatches nothing, exposes undurable document and retry persists then sends after storage recovers', async t => {
  const f = fixture(t), store = memoryStore(), model = f.create(store.port()); await model.load(); store.setQuota(true);
  await assert.rejects(model.commit(doc(28), { expectedRevision: 0, requestId: 'quota_intent_001' }), error => error.code === 'field_local_storage_unavailable');
  assert.equal(f.writes(), 0); assert.equal(model.hasUnsavedChanges(), true); assert.equal(model.getState().localDurable, false);
  assert.deepEqual(model.exportPending().document, doc(28)); store.setQuota(false);
  const recovered = await model.retry(); assert.equal(recovered.state, 'saved'); assert.deepEqual(f.current().document, doc(28)); assert.equal(f.writes(), 1);
});

test('competing window fails local CAS without overwriting first durable pending layout', async t => {
  const f = fixture(t), store = memoryStore(), a = f.create(store.port()), b = f.create(store.port()); await a.load(); await b.load(); f.setOffline(true);
  // Loading the second controller updates the shared local revision; re-read A
  // to represent its latest displayed snapshot before the simultaneous edit.
  await a.load();
  await a.commit(doc(40), { expectedRevision: 0, requestId: 'tab_a_001' });
  await assert.rejects(b.commit(doc(80), { expectedRevision: 0, requestId: 'tab_b_001' }), error => error.code === 'field_local_revision_conflict');
  assert.deepEqual(store.value().pending[0].document, doc(40)); assert.deepEqual(b.exportPending().document, doc(80));
  assert.equal(b.hasUnsavedChanges(), true); assert.equal(f.writes(), 0);
});

test('server CAS conflict retains desired coordinates and explicit local choice uses a fresh request and latest revision', async t => {
  const f = fixture(t), store = memoryStore(), model = f.create(store.port()); await model.load();
  f.world.execute({ actor: account, op: 'world.field.put', args: { expectedAccountId: account.accountId, expectedRevision: 0, requestId: 'other_device_001', document: doc(15) } });
  const result = await model.commit(doc(32), { expectedRevision: 0, requestId: 'this_device_001' });
  assert.equal(result.status, 'conflict'); assert.deepEqual(model.exportPending().document, doc(32)); assert.deepEqual(f.current().document, doc(15));
  const resolved = await model.resolveConflict('local');
  assert.equal(resolved.state, 'saved'); assert.equal(resolved.revision, 2); assert.deepEqual(f.current().document, doc(32));
});

test('late account-switch response cannot update model, local cache or dispatch another request', async t => {
  const f = fixture(t), store = memoryStore(); let active = true;
  const model = f.create(store.port(), { isCurrent: () => active }); await model.load();
  const before = store.value(), gate = defer(); f.gate(gate); const flight = model.commit(doc(70), { expectedRevision: 0, requestId: 'account_race_001' });
  await until(() => store.value().pending.length === 1); active = false; gate.resolve();
  await assert.rejects(flight, error => error.code === 'field_account_changed');
  // An admitted request can commit remotely before the profile changes; the
  // exact durable old-account intent remains, never a new-account cache write.
  assert.equal(store.value().scope, before.scope); assert.equal(store.value().pending[0].requestId, 'account_race_001');
  assert.equal(store.value().base.revision, 0);
});

test('late replay ACK retains desired coordinates as a durable conflict while exposing actual newer remote state', async t => {
  const f = fixture(t), store = memoryStore(), model = f.create(store.port()); await model.load(); f.loseAck();
  await model.commit(doc(22), { expectedRevision: 0, requestId: 'accepted_then_other' });
  f.world.execute({ actor: account, op: 'world.field.put', args: { expectedAccountId: account.accountId, expectedRevision: 1, requestId: 'later_layout_001', document: doc(99) } });
  const current = await model.retry(); assert.equal(current.state, 'conflict'); assert.equal(current.revision, 2); assert.deepEqual(current.document, doc(22));
  assert.deepEqual(current.remote.document, doc(99)); assert.equal(current.localDurable, true); assert.equal(f.current().revision, 2);
  model.dispose(); const restored = f.create(store.port()); const reopened = await restored.load();
  assert.equal(reopened.state, 'conflict'); assert.deepEqual(reopened.document, doc(22)); assert.deepEqual(reopened.remote.document, doc(99));
  const chosen = await restored.resolveConflict('local'); assert.equal(chosen.state, 'saved'); assert.equal(chosen.revision, 3); assert.deepEqual(f.current().document, doc(22));
});

test('choosing remote after an accepted-but-superseded intent adopts current without a duplicate PUT', async t => {
  const f = fixture(t), store = memoryStore(), model = f.create(store.port()); await model.load(); f.loseAck();
  await model.commit(doc(22), { expectedRevision: 0, requestId: 'old_accepted_remote_choice' });
  f.world.execute({ actor: account, op: 'world.field.put', args: { expectedAccountId: account.accountId, expectedRevision: 1, requestId: 'new_before_remote_choice', document: doc(99) } });
  await model.retry(); const before = f.writes(); const result = await model.resolveConflict('remote');
  assert.equal(result.state, 'saved'); assert.deepEqual(result.document, doc(99)); assert.equal(result.revision, 2); assert.equal(f.writes(), before);
});

test('simultaneous tab drains merge a proven ACK without erasing another durable arrangement or raising a false conflict', async t => {
  const f = fixture(t), store = memoryStore(true), a = f.create(store.port()), b = f.create(store.port());
  await a.load(); await b.load(); await turn();
  const gate = defer(); f.gate(gate);
  const first = a.commit(doc(16), { expectedRevision: 0, requestId: 'parallel_a_001' });
  await until(() => b.getState().pendingCount === 1);
  const second = b.commit(doc(37), { expectedRevision: 0, requestId: 'parallel_b_001' });
  await until(() => store.value().pending.length === 2); gate.resolve(); f.gate(null);
  const results = await Promise.all([first, second]);
  assert.equal(results.every(value => value.status === 'saved'), true); assert.equal(f.current().revision, 2);
  assert.deepEqual(f.current().document, doc(37)); assert.equal(store.value().pending.length, 0); assert.equal(store.value().conflict, false);
});

test('accepting another window version preserves and reconciles its durable offline outbox', async t => {
  const f = fixture(t), store = memoryStore(), a = f.create(store.port()), b = f.create(store.port());
  await a.load(); await b.load(); await a.load(); f.setOffline(true);
  await a.commit(doc(40), { expectedRevision: 0, requestId: 'keep_other_tab_a' });
  await assert.rejects(b.commit(doc(80), { expectedRevision: 0, requestId: 'discard_this_tab_b' }), error => error.code === 'field_local_revision_conflict');
  f.setOffline(false); const result = await b.resolveConflict('remote');
  assert.equal(result.state, 'saved'); assert.deepEqual(f.current().document, doc(40)); assert.equal(f.current().revision, 1);
  assert.equal(store.value().pending.length, 0);
});

test('choosing own version after local conflict preserves the other authorized intent before appending a new CAS', async t => {
  const f = fixture(t), store = memoryStore(), a = f.create(store.port()), b = f.create(store.port());
  await a.load(); await b.load(); await a.load(); f.setOffline(true);
  await a.commit(doc(40), { expectedRevision: 0, requestId: 'keep_before_tab_a' });
  await assert.rejects(b.commit(doc(80), { expectedRevision: 0, requestId: 'replace_after_tab_b' }), error => error.code === 'field_local_revision_conflict');
  f.setOffline(false); const result = await b.resolveConflict('local');
  assert.equal(result.state, 'saved'); assert.deepEqual(f.current().document, doc(80)); assert.equal(f.current().revision, 2);
  assert.equal(f.writes(), 2);
});

test('read-only quota startup has no navigation guard; discarding a failed new edit restores durable baseline without dispatch', async t => {
  const f = fixture(t), store = memoryStore(); store.setQuota(true);
  const model = f.create(store.port()), initial = await model.load();
  assert.equal(initial.state, 'storage-error'); assert.equal(model.hasUnsavedChanges(), false);
  await assert.rejects(model.commit(doc(33), { expectedRevision: 0, requestId: 'quota_discard_001' }), error => error.code === 'field_local_storage_unavailable');
  assert.equal(model.hasUnsavedChanges(), true); const discarded = await model.discardVolatile();
  assert.equal(model.hasUnsavedChanges(), false); assert.deepEqual(discarded.document.contexts, []); assert.equal(f.writes(), 0);
  store.setQuota(false); const recovered = await model.retry(); assert.equal(recovered.state, 'saved'); assert.equal(f.writes(), 0);
});

test('repeating a still-pending commit reuses its durable intent and does not leave a phantom volatile navigation guard', async t => {
  const f = fixture(t), store = memoryStore(), model = f.create(store.port()); await model.load(); f.setOffline(true);
  await model.commit(doc(65), { expectedRevision: 0, requestId: 'same_pending_001' });
  f.setOffline(false); const result = await model.commit(doc(65), { expectedRevision: 0, requestId: 'same_pending_001' });
  assert.equal(result.status, 'saved'); assert.equal(result.localDurable, true); assert.equal(model.hasUnsavedChanges(), false);
  assert.equal(f.current().revision, 1); assert.equal(f.writes(), 1);
});

test('discarding an undurable conflicting attempt adopts the other window outbox without deleting or sending it', async t => {
  const f = fixture(t), store = memoryStore(), a = f.create(store.port()), b = f.create(store.port());
  await a.load(); await b.load(); await a.load(); f.setOffline(true);
  await a.commit(doc(48), { expectedRevision: 0, requestId: 'durable_other_window' });
  await assert.rejects(b.commit(doc(77), { expectedRevision: 0, requestId: 'undurable_this_window' }), error => error.code === 'field_local_revision_conflict');
  const before = store.value(); const discarded = await b.discardVolatile();
  assert.deepEqual(store.value(), before); assert.deepEqual(discarded.document, doc(48)); assert.equal(b.hasUnsavedChanges(), false);
  assert.equal(discarded.localDurable, true); assert.equal(f.writes(), 0);
});

test('same remote revision with a different internally valid hash cannot silently replace a previously verified arrangement', async t => {
  const f = fixture(t), store = memoryStore(), first = f.create(store.port()); await first.load();
  await first.commit(doc(19), { expectedRevision: 0, requestId: 'known_hash_revision_001' }); first.dispose();
  const altered = doc(88), contentHash = createHash('sha256').update(JSON.stringify(altered)).digest('hex');
  const second = createFieldPersistence({ accountId: account.accountId, store: store.port(), api: { request: async () => ({ revision: 1, document: altered, contentHash, updatedAt: 10 }) } });
  t.after(() => second.dispose()); const state = await second.load();
  assert.equal(state.errorCode, 'field_invalid_receipt'); assert.deepEqual(state.document, doc(19));
  assert.deepEqual(store.value().base.document, doc(19)); assert.equal(store.value().base.revision, 1);
});

test('local-first makes three ordered intentions durable while the first real PUT is blocked; ACKs keep the projected fence stable', { timeout: 3000 }, async t => {
  const f = fixture(t), store = memoryStore(), updates = [], model = f.create(store.port(), { localFirst: true, onChange: value => updates.push(value) });
  await model.load(); const gate = defer(); f.gate(gate);
  for (const [index, x] of [12, 24, 36].entries()) {
    const result = await model.commit(doc(x), { expectedRevision: index, requestId: `local_first_${index}` });
    assert.equal(result.status, 'volatile'); assert.equal(result.localDurable, true); assert.equal(result.revision, index + 1);
  }
  assert.equal(f.writes(), 0); assert.equal(model.hasUnsavedChanges(), false);
  assert.deepEqual(store.value().pending.map(intent => intent.expectedRevision), [0, 1, 2]);
  assert.deepEqual(store.value().pending.map(intent => intent.document.contexts[0].x), [12, 24, 36]);
  assert.equal(model.getState().projectedRevision, 3); const started = updates.length;
  gate.resolve(); const saved = await model.flush();
  assert.equal(saved.state, 'saved'); assert.equal(saved.revision, 3); assert.equal(f.writes(), 3);
  assert.deepEqual(f.current().document, doc(36));
  assert.ok(updates.slice(started).every(value => value.projectedRevision === 3));
});

test('local-first replay reuses its durable intent while a slow RPC is outstanding and rejects altered intent identity', { timeout: 3000 }, async t => {
  const f = fixture(t), store = memoryStore(), model = f.create(store.port(), { localFirst: true }); await model.load();
  const gate = defer(); f.gate(gate);
  await model.commit(doc(26), { expectedRevision: 0, requestId: 'slow_retry_exact' });
  const repeated = await model.commit(doc(26), { expectedRevision: 0, requestId: 'slow_retry_exact' });
  assert.equal(repeated.status, 'volatile'); assert.equal(repeated.revision, 1); assert.equal(store.value().pending.length, 1);
  await assert.rejects(model.commit(doc(26), { expectedRevision: 1, requestId: 'slow_retry_exact' }), error => error.code === 'field_request_conflict');
  assert.equal(model.hasUnsavedChanges(), false); gate.resolve(); await model.flush(); assert.equal(f.writes(), 1);
});

test('cached startup permits a durable edit during slow GET; a legitimately older GET cannot overwrite the verified PUT ACK', { timeout: 3000 }, async t => {
  const f = fixture(t), store = memoryStore(), first = f.create(store.port()); await first.load();
  await first.commit(doc(10), { expectedRevision: 0, requestId: 'cached_startup_base' }); first.dispose();
  const stale = f.current(), gate = defer(); let reading = false;
  const api = { async request(op, args) { if (op === 'world.field.get') { reading = true; await gate.promise; return stale; } return f.api.request(op, args); } };
  const model = f.create(store.port(), { api, localFirst: true }), loading = model.load();
  await until(() => reading); assert.equal(model.getState().revision, 1);
  const queued = await model.commit(doc(20), { expectedRevision: 1, requestId: 'cached_while_get_slow' }); assert.equal(queued.localDurable, true);
  await model.flush(); assert.equal(f.current().revision, 2);
  gate.resolve(); const loaded = await loading;
  assert.equal(loaded.state, 'saved'); assert.equal(loaded.errorCode, undefined); assert.equal(loaded.revision, 2);
  assert.deepEqual(loaded.document, doc(20)); assert.deepEqual(store.value().base.document, doc(20));
});

test('reload replays three locally acknowledged intents while the disposed old controller still has a blocked request; late ACK cannot overwrite its cache', { timeout: 3000 }, async t => {
  const f = fixture(t), store = memoryStore(), gate = defer(); let firstFlight = true;
  const slowApi = { async request(op, args) { if (op === 'world.field.put' && firstFlight) { firstFlight = false; await gate.promise; } return f.api.request(op, args); } };
  const first = f.create(store.port(), { api: slowApi, localFirst: true }); await first.load();
  for (const [index, x] of [14, 28, 42].entries()) await first.commit(doc(x), { expectedRevision: index, requestId: `slow_reload_${index}` });
  assert.equal(store.value().pending.length, 3); assert.equal(f.current().revision, 0); first.dispose();
  const second = f.create(store.port(), { localFirst: true }), confirmed = await second.load();
  assert.equal(confirmed.revision, 3); assert.deepEqual(confirmed.document, doc(42)); assert.equal(confirmed.pendingCount, 0);
  const beforeLateAck = store.value(); gate.resolve(); await until(() => f.writes() === 4); await turn();
  assert.deepEqual(store.value(), beforeLateAck); assert.equal(f.current().revision, 3); assert.deepEqual(f.current().document, doc(42));
});

test('read-only second window follows a shared durable outbox and its verified ACK without a false local conflict or a duplicate request', { timeout: 3000 }, async t => {
  const f = fixture(t), store = memoryStore(true), author = f.create(store.port()), observer = f.create(store.port());
  await author.load(); await observer.load(); await turn();
  const gate = defer(); f.gate(gate); const sending = author.commit(doc(56), { expectedRevision: 0, requestId: 'readonly_observer_ack' });
  await until(() => observer.getState().pendingCount === 1); assert.equal(observer.hasUnsavedChanges(), false);
  gate.resolve(); await sending; await until(() => observer.getState().state === 'saved');
  assert.deepEqual(observer.getState().document, doc(56)); assert.equal(observer.getState().revision, 1);
  assert.equal(observer.getState().errorCode, undefined); assert.equal(f.writes(), 1);
});

test('another window resolving a superseded receipt cannot silently adopt or falsely claim durability for this unresolved desired version', { timeout: 3000 }, async t => {
  const f = fixture(t), store = memoryStore(true), own = f.create(store.port()); await own.load(); f.loseAck();
  await own.commit(doc(22), { expectedRevision: 0, requestId: 'retain_own_conflict' });
  f.world.execute({ actor: account, op: 'world.field.put', args: { expectedAccountId: account.accountId, expectedRevision: 1, requestId: 'other_remote_layout', document: doc(99) } });
  await own.retry(); assert.equal(own.getState().state, 'conflict'); assert.equal(own.getState().localDurable, true);
  const another = f.create(store.port()); await another.load(); await another.resolveConflict('remote');
  await until(() => own.getState().localDurable === false);
  assert.equal(own.getState().state, 'conflict'); assert.deepEqual(own.getState().document, doc(22)); assert.equal(own.hasUnsavedChanges(), true);
  assert.deepEqual(own.exportPending().document, doc(22)); assert.deepEqual(store.value().base.document, doc(99));
  const before = f.writes(), adopted = await own.resolveConflict('remote');
  assert.equal(adopted.state, 'saved'); assert.equal(adopted.localDurable, true); assert.equal(own.hasUnsavedChanges(), false);
  assert.deepEqual(adopted.document, doc(99)); assert.equal(f.writes(), before);
});
