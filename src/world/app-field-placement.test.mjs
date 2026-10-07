import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorldService } from '../../modules/world/server/index.mjs';
import { createFieldPersistence } from './field-persistence.ts';
import { createPersonalFieldDocument, ensureFieldEntityPlacement, createAppFieldPlacementController } from './app-field-placement.mjs';

const APP = `app-${'a'.repeat(32)}`, OTHER_APP = `app-${'b'.repeat(32)}`;
const entity = { kind: 'app', id: APP };
const account = { accountId: 'placement-owner', deviceId: 'placement-device', label: 'Placement fixture' };
const error = code => Object.assign(new Error(code), { code });
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const until = async predicate => { const end = Date.now() + 2000; while (!predicate()) { if (Date.now() > end) assert.fail('Controlled phase was not reached'); await new Promise(done => setTimeout(done, 5)); } };
const document = () => ({ schema: 'soty.field.v1', contexts: [{ contextId: 'work', title: 'Работа', x: 0, y: 0 },
  { contextId: 'home', title: 'Дом', x: 600, y: 0 }], shortcuts: [] });

function memoryStore() {
  let record = null, quota = false;
  return { value: () => structuredClone(record), quota: value => { quota = value; }, port: () => ({
    async read() { return structuredClone(record); },
    async write(next, expected) {
      if (quota) throw error('field_local_storage_unavailable');
      if ((record?.localRevision ?? 0) !== expected) throw error('field_local_revision_conflict');
      record = structuredClone(next);
    }, subscribe() { return () => {}; },
  }) };
}
function fixture(t, initial = document()) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-app-placement-'));
  const world = createWorldService({ databasePath: join(directory, 'world.sqlite'), projectId: 'app-placement-tests' });
  world.execute({ actor: account, op: 'world.profile.get' });
  let sequence = 0, reads = 0, loseAck = false, offline = false, beforePut = null, beforeRead = null;
  const calls = [], controllers = [], stores = [];
  const current = () => world.execute({ actor: account, op: 'world.field.get', args: { expectedAccountId: account.accountId } });
  const remoteWrite = doc => world.execute({ actor: account, op: 'world.field.put', args: {
    expectedAccountId: account.accountId, expectedRevision: current().revision, requestId: `outside_${++sequence}`, document: doc } });
  if (initial) remoteWrite(initial);
  const api = { async request(op, args) {
    calls.push({ op, args: structuredClone(args) });
    if (offline) throw error('network_offline');
    if (op === 'world.field.put' && beforePut) { const callback = beforePut; beforePut = null; await callback(args); }
    if (op === 'world.field.get') { reads++; if (beforeRead) { const callback = beforeRead; beforeRead = null; await callback(); } }
    const value = world.execute({ actor: account, op, args });
    if (op === 'world.field.put' && loseAck) { loseAck = false; throw error('network_lost_ack'); }
    return value;
  } };
  const create = ({ store = memoryStore(), isCurrent = () => true, appId = APP, signal, initialAppIds } = {}) => {
    const persistence = createFieldPersistence({ api, accountId: account.accountId, isCurrent, store: store.port() });
    let ids = 0; const instance = controllers.length;
    const controller = createAppFieldPlacementController({ persistence, accountId: account.accountId, appId, isCurrent,
      randomId: () => `placement_${appId.slice(-4)}_${instance}_${++ids}`, ...(signal ? { signal } : {}), ...(initialAppIds ? { initialAppIds } : {}) });
    controllers.push(controller); stores.push(store); return { controller, persistence, store };
  };
  t.after(() => { controllers.forEach(value => value.dispose()); world.close(); rmSync(directory, { recursive: true, force: true }); });
  return { create, current, remoteWrite, calls, reads: () => reads,
    loseAck: () => { loseAck = true; }, offline: value => { offline = value; },
    beforePut: callback => { beforePut = callback; }, beforeRead: callback => { beforeRead = callback; } };
}
const select = (view, contextId = 'work') => ({ contextId, generation: view.generation });
const appPlacements = doc => doc.shortcuts.filter(shortcut => shortcut.entity.id === APP);

test('pure placement preserves one app reference and unrelated layout; same space is idempotent', () => {
  const before = document(), first = ensureFieldEntityPlacement(before, { entity, contextId: 'work', shortcutId: 'first-shortcut' });
  assert.equal(first.changed, true); assert.equal(first.createdContext, false); assert.deepEqual(before.shortcuts, []);
  const again = ensureFieldEntityPlacement(first.document, { entity, contextId: 'work', shortcutId: 'unused-shortcut' });
  assert.equal(again.changed, false); assert.equal(again.shortcutId, 'first-shortcut'); assert.deepEqual(again.document, first.document);
  const elsewhere = ensureFieldEntityPlacement(again.document, { entity, contextId: 'home', shortcutId: 'second-shortcut' });
  assert.equal(elsewhere.document.shortcuts.length, 2); assert.deepEqual(elsewhere.document.shortcuts.map(value => value.entity), [entity, entity]);
  elsewhere.document.contexts[0].title = 'Detached'; assert.equal(first.document.contexts[0].title, 'Работа');
});

test('first-use seed is shared and closed; existing missing space never becomes a new workspace', () => {
  const seed = createPersonalFieldDocument(); assert.deepEqual(seed.shortcuts.map(value => value.entity.id), ['notes', 'chess']);
  assert.throws(() => ensureFieldEntityPlacement(document(), { entity, contextId: 'missing', shortcutId: 'new', initializePersonal: true }), { code: 'field_context_missing' });
  assert.throws(() => ensureFieldEntityPlacement(document(), { entity: { ...entity, url: 'https://example.invalid' }, contextId: 'work', shortcutId: 'new' }), { code: 'field_document_invalid' });
});

test('controller writes only existing field authority and repeat Add makes no new receipt', async t => {
  const f = fixture(t), { controller } = f.create(); const view = await controller.load();
  const first = await controller.place(select(view)); assert.equal(first.status, 'saved'); assert.equal(first.changed, true);
  const revision = f.current().revision, puts = f.calls.filter(call => call.op === 'world.field.put').length;
  const again = await controller.place(select(first.snapshot)); assert.equal(again.status, 'saved'); assert.equal(again.changed, false);
  assert.equal(again.shortcutId, first.shortcutId); assert.equal(f.current().revision, revision);
  assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, puts);
  assert.equal(f.calls.every(call => ['world.field.get', 'world.field.put'].includes(call.op)), true);
  assert.equal(appPlacements(f.current().document).length, 1);
  assert.equal(JSON.stringify(f.current().document).includes('https:'), false);
});

test('same app in another workspace creates a placement while retaining the first shortcut', async t => {
  const f = fixture(t), { controller } = f.create(), view = await controller.load();
  const first = await controller.place(select(view)), second = await controller.place(select(first.snapshot, 'home'));
  assert.equal(second.status, 'saved'); assert.notEqual(second.shortcutId, first.shortcutId);
  assert.deepEqual(appPlacements(f.current().document).map(value => value.contextId), ['work', 'home']);
});

test('deep-entry first Add seeds builtins once in a single field mutation', async t => {
  const f = fixture(t, null), { controller } = f.create(), view = await controller.load();
  assert.equal(f.current().revision, 0); assert.equal(view.contexts[0].willCreate, true);
  const added = await controller.place(select(view, 'personal')); assert.equal(added.status, 'saved');
  assert.equal(f.current().revision, 1); assert.deepEqual(f.current().document.shortcuts.map(value => value.entity.id), ['notes', 'chess', APP]);
  assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, 1);
});

test('a deliberately emptied existing field adds a space without re-adding deleted builtins', async t => {
  const f = fixture(t, { schema: 'soty.field.v1', contexts: [], shortcuts: [] }), { controller } = f.create();
  const view = await controller.load(), added = await controller.place(select(view, 'personal'));
  assert.equal(added.status, 'saved'); assert.deepEqual(f.current().document.shortcuts.map(value => value.entity.id), [APP]);
});

test('first deep-entry Add retains this account legacy pins exactly once, without a duplicate target', async t => {
  const f = fixture(t, null), { controller } = f.create({ initialAppIds: [APP, OTHER_APP, APP] }), view = await controller.load();
  const added = await controller.place(select(view, 'personal')); assert.equal(added.status, 'saved');
  assert.deepEqual(f.current().document.shortcuts.map(value => value.entity.id), ['notes', 'chess', APP, OTHER_APP]);
  assert.equal(appPlacements(f.current().document).length, 1); assert.equal(added.shortcutId, `migrated-${APP}`);
  f.remoteWrite({ schema: 'soty.field.v1', contexts: [], shortcuts: [] });
  const later = f.create({ initialAppIds: [OTHER_APP] }), emptyView = await later.controller.load();
  await later.controller.place(select(emptyView, 'personal'));
  assert.deepEqual(f.current().document.shortcuts.map(value => value.entity.id), [APP]);
});

test('queued duplicate clicks on an existing workspace settle to one shortcut and one write', async t => {
  const f = fixture(t), { controller } = f.create(), view = await controller.load();
  const [first, second] = await Promise.all([controller.place(select(view)), controller.place(select(view))]);
  assert.equal(first.status, 'saved'); assert.equal(second.changed, false); assert.equal(first.shortcutId, second.shortcutId);
  assert.equal(appPlacements(f.current().document).length, 1); assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, 1);
});

test('unknown ACK stays pending; retry confirms the original request without a second placement', async t => {
  const f = fixture(t), { controller, store } = f.create(), view = await controller.load(); f.loseAck();
  const pending = await controller.place(select(view)); assert.equal(pending.status, 'pending'); assert.equal(pending.snapshot.localDurable, true);
  const requestId = store.value().pending[0].requestId, acceptedRevision = f.current().revision;
  const settled = await controller.retry(); assert.equal(settled.persistence, 'saved'); assert.equal(settled.placements.length, 1);
  assert.equal(f.current().revision, acceptedRevision); assert.equal(appPlacements(f.current().document).length, 1);
  assert.deepEqual(f.calls.filter(call => call.op === 'world.field.put').map(call => call.args.requestId), [requestId, requestId]);
});

test('reload after lost ACK reads/replays durable state and Add observes the existing shortcut', async t => {
  const f = fixture(t), store = memoryStore(), first = f.create({ store }), view = await first.controller.load(); f.loseAck();
  const pending = await first.controller.place(select(view)); first.controller.dispose();
  const second = f.create({ store }), restored = await second.controller.load(), added = await second.controller.place(select(restored));
  assert.equal(added.changed, false); assert.equal(added.shortcutId, pending.shortcutId); assert.equal(appPlacements(f.current().document).length, 1);
  assert.equal(f.current().revision, 2);
});

test('offline intent is locally durable and repeated Add drains it before computing another intent', async t => {
  const f = fixture(t), { controller, store } = f.create(), view = await controller.load(); f.offline(true);
  const pending = await controller.place(select(view)); assert.equal(pending.status, 'pending'); assert.equal(controller.hasUnsavedChanges(), false);
  assert.equal(store.value().pending.length, 1); f.offline(false);
  const repeated = await controller.place(select(pending.snapshot)); assert.equal(repeated.status, 'saved'); assert.equal(repeated.changed, false);
  assert.equal(appPlacements(f.current().document).length, 1); assert.equal(f.calls.filter(call => call.op === 'world.field.put').at(-1).args.requestId,
    f.calls.filter(call => call.op === 'world.field.put')[0].args.requestId);
});

test('storage failure dispatches nothing; explicit retry recovers the exact buffered placement', async t => {
  const f = fixture(t), { controller, store } = f.create(), view = await controller.load(); store.quota(true);
  const failed = await controller.place(select(view)); assert.equal(failed.status, 'storage-error'); assert.equal(controller.hasUnsavedChanges(), true);
  assert.equal(appPlacements(f.current().document).length, 0); assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, 0);
  store.quota(false); const settled = await controller.retry(); assert.equal(settled.persistence, 'saved'); assert.equal(settled.placements[0].shortcutId, failed.shortcutId);
  assert.equal(appPlacements(f.current().document).length, 1);
});

test('fresh explicit server CAS rejection rebases only Add, preserving the other device layout', async t => {
  const f = fixture(t), { controller } = f.create(), view = await controller.load();
  f.beforePut(() => f.remoteWrite(ensureFieldEntityPlacement(f.current().document, { entity: { kind: 'app', id: OTHER_APP }, contextId: 'home', shortcutId: 'outside-shortcut' }).document));
  const added = await controller.place(select(view)); assert.equal(added.status, 'saved'); assert.equal(added.changed, true);
  assert.equal(f.current().revision, 3); assert.equal(f.current().document.shortcuts.some(value => value.shortcutId === 'outside-shortcut'), true);
  assert.equal(appPlacements(f.current().document).length, 1); assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, 2);
  const [first, second] = f.calls.filter(call => call.op === 'world.field.put');
  assert.notEqual(first.args.requestId, second.args.requestId); assert.equal(first.args.document.shortcuts.find(value => value.entity.id === APP).shortcutId,
    second.args.document.shortcuts.find(value => value.entity.id === APP).shortcutId);
});

test('a workspace changed before Add invalidates the displayed chooser and dispatches nothing', async t => {
  const f = fixture(t), { controller } = f.create(), view = await controller.load(), changed = document(); changed.contexts[0].title = 'Другой контекст'; f.remoteWrite(changed);
  const outcome = await controller.place(select(view)); assert.equal(outcome.status, 'conflict'); assert.equal(outcome.errorCode, 'app_field_selection_changed');
  assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, 0);
  await assert.rejects(controller.place(select(view)), { code: 'app_field_selection_changed' });
});

test('workspace removal during the CAS race is not silently recreated', async t => {
  const f = fixture(t), { controller } = f.create(), view = await controller.load();
  f.beforePut(() => { const changed = document(); changed.contexts.shift(); f.remoteWrite(changed); });
  const result = await controller.place(select(view)); assert.equal(result.status, 'conflict'); assert.equal(result.errorCode, 'app_field_selection_changed');
  assert.equal(f.current().document.contexts.some(value => value.contextId === 'work'), false); assert.equal(appPlacements(f.current().document).length, 0);
  assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, 1);
});

test('accepted-but-superseded lost ACK never resurrects a shortcut removed by another device', async t => {
  const f = fixture(t), { controller } = f.create(), view = await controller.load(); f.loseAck();
  const first = await controller.place(select(view)); assert.equal(first.status, 'pending'); f.remoteWrite(document());
  const checked = await controller.retry(); assert.equal(checked.persistence, 'conflict');
  const again = await controller.place(select(checked)); assert.equal(again.status, 'conflict');
  assert.equal(appPlacements(f.current().document).length, 0); assert.equal(f.current().revision, 3);
  assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, 2);
});

test('pre-aborted operation performs no remote read or write and cannot be revived', async t => {
  const f = fixture(t), abort = new AbortController(), { controller } = f.create({ signal: abort.signal }); abort.abort();
  await assert.rejects(async () => controller.load(), { code: 'app_field_aborted' }); assert.equal(f.calls.length, 0);
  await assert.rejects(controller.place({ contextId: 'work', generation: 1 }), { code: 'app_field_aborted' });
});

test('abort after dispatch retains the exact account outbox; late ACK cannot update or start another write', async t => {
  const f = fixture(t), { controller, store } = f.create(), view = await controller.load(), gate = deferred(), abort = new AbortController();
  f.beforePut(() => gate.promise); const flight = controller.place({ ...select(view), signal: abort.signal });
  await until(() => f.calls.some(call => call.op === 'world.field.put')); const pendingId = store.value().pending[0].requestId;
  abort.abort(); gate.resolve(); await assert.rejects(flight, { code: 'app_field_aborted' });
  assert.equal(store.value().pending[0].requestId, pendingId); assert.equal(store.value().base.revision, 1);
  assert.equal(appPlacements(f.current().document).length, 1); assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, 1);
  await assert.rejects(async () => controller.retry(), { code: 'app_field_aborted' });
});

test('account generation A→B→A refuses late effects and keeps the old account intent scoped', async t => {
  const f = fixture(t); let epoch = 0; const initialEpoch = epoch;
  const { controller, store } = f.create({ isCurrent: () => epoch === initialEpoch }), view = await controller.load(), gate = deferred();
  f.beforePut(() => gate.promise); const flight = controller.place(select(view));
  await until(() => f.calls.some(call => call.op === 'world.field.put')); epoch++; epoch++; gate.resolve();
  await assert.rejects(flight, { code: 'field_account_changed' }); assert.equal(store.value().pending.length, 1);
  assert.equal(JSON.parse(store.value().scope)[2], account.accountId); assert.equal(store.value().base.revision, 1);
  await assert.rejects(async () => controller.refresh(), { code: 'field_account_changed' });
});

test('read cancellation disposes the controller and ignores late metadata without any field intent', async t => {
  const f = fixture(t), { controller } = f.create(), gate = deferred(), abort = new AbortController();
  assert.equal(controller.hasUnsavedChanges(), false);
  f.beforeRead(() => gate.promise); const flight = controller.load(abort.signal); await until(() => f.reads() === 1);
  assert.equal(controller.hasUnsavedChanges(), false);
  abort.abort(); gate.resolve(); await assert.rejects(flight, { code: 'app_field_aborted' });
  assert.equal(f.calls.filter(call => call.op === 'world.field.put').length, 0);
});

test('snapshot mutation and observer failure cannot alter chosen workspace or stored field', async t => {
  const f = fixture(t), { controller } = f.create(); controller.subscribe(() => { throw Error('Inert view failed'); });
  const view = await controller.load(); view.contexts[0].contextId = 'foreign';
  assert.equal(controller.getSnapshot().contexts[0].contextId, 'work');
  const added = await controller.place(select(controller.getSnapshot())); assert.equal(added.status, 'saved');
  assert.equal(appPlacements(f.current().document)[0].contextId, 'work');
});

test('two windows settle a shared intent and preserve the later app without overwriting either placement', async t => {
  const f = fixture(t), store = memoryStore(), first = f.create({ store }), view = await first.controller.load(), gate = deferred();
  f.beforePut(() => gate.promise); const held = first.controller.place(select(view));
  await until(() => store.value().pending.length === 1);
  const second = f.create({ store, appId: OTHER_APP }), otherView = await second.controller.load();
  const added = await second.controller.place(select(otherView, 'home')); assert.equal(added.status, 'saved');
  gate.resolve(); const settled = await held; assert.equal(settled.status, 'saved');
  assert.equal(f.current().revision, 3); assert.equal(appPlacements(f.current().document).length, 1);
  assert.equal(f.current().document.shortcuts.filter(value => value.entity.id === OTHER_APP).length, 1);
  assert.equal(store.value().pending.length, 0); assert.equal(store.value().base.revision, 3);
});

test('quota failure while persisting a real ACK keeps the original durable request for reconciliation', async t => {
  const f = fixture(t), { controller, store } = f.create(), view = await controller.load();
  f.beforePut(() => store.quota(true)); const held = await controller.place(select(view));
  assert.equal(held.status, 'pending'); assert.equal(held.snapshot.localDurable, true); assert.equal(store.value().pending.length, 1);
  const requestId = store.value().pending[0].requestId, revision = f.current().revision;
  store.quota(false); const settled = await controller.retry(); assert.equal(settled.persistence, 'saved');
  assert.equal(f.current().revision, revision); assert.equal(appPlacements(f.current().document).length, 1);
  assert.deepEqual(f.calls.filter(value => value.op === 'world.field.put').map(value => value.args.requestId), [requestId, requestId]);
});

test('continually changing server state permits at most one bounded Add rebase', async t => {
  const f = fixture(t), { controller } = f.create(), view = await controller.load(); let moves = 0;
  const race = () => { const next = document(); next.contexts[1].x += ++moves; f.remoteWrite(next); f.beforePut(race); };
  f.beforePut(race); const held = await controller.place(select(view)); assert.equal(held.status, 'conflict');
  assert.equal(f.calls.filter(value => value.op === 'world.field.put').length, 2); assert.equal(appPlacements(f.current().document).length, 0);
  const before = f.calls.length; await controller.retry(); assert.equal(f.calls.length, before);
});

test('the controller bounds queued actions while a read is stalled', async t => {
  const f = fixture(t), { controller } = f.create(), gate = deferred(); f.beforeRead(() => gate.promise);
  const first = controller.load(); await until(() => f.reads() === 1);
  const queued = [controller.refresh(), controller.refresh(), controller.refresh()];
  assert.throws(() => controller.refresh(), { code: 'app_field_busy' }); gate.resolve();
  await Promise.all([first, ...queued]); assert.equal(f.calls.filter(value => value.op === 'world.field.put').length, 0);
});
