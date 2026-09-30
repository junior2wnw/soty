import test from 'node:test';
import assert from 'node:assert/strict';
import { createAppSavedState, dispatchAppSavedIntent, createAppSavedLibraryState, normalizeAppSavedSnapshot } from './app-saved-state.mjs';

const app = `app-${'a'.repeat(32)}`, domain = `dom_${'b'.repeat(32)}`;
const entry = { appId: app, domainId: domain, origin: 'https://app.example', path: '/board?q=a%2Bb#item' };
const stored = (revision = 1, route = entry) => ({ ...route, label: 'Saved name', savedRevision: revision, updatedAt: 5,
  current: { name: 'Current name', status: 'offline', canManage: false } });
const storage = () => { const values = new Map(); return { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) }; };
const locks = () => { let tail = Promise.resolve(); return { request(_key, fn) { const result = tail.then(fn); tail = result.catch(() => {}); return result; } }; };
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const code = expected => error => error?.code === expected;
const intent = (overrides = {}) => ({ entry, saved: true, expectedRevision: 0, currentEntry: null, ...overrides });
const response = pending => ({ requestId: pending.args.requestId, replayed: false, receipt: { appId: app, saved: pending.args.saved,
  revision: pending.args.expectedRevision + 1, committedAt: 10 }, current: { revision: pending.args.expectedRevision + 1,
    entry: pending.args.saved ? stored(pending.args.expectedRevision + 1, pending.entry) : null } });

test('saved lost response reload keeps exact account-global request and historical receipt never replaces current', async t => {
  const disk = storage(), lock = locks(), first = createAppSavedState({ accountId: 'A', storage: disk, locks: lock, randomId: () => 'save-1' });
  t.after(() => first.dispose()); let captured;
  await assert.rejects(dispatchAppSavedIntent({ state: first, isCurrent: () => true, intent: intent(), api: { async request(_op, args) {
    captured = args; throw new Error('lost_response'); } } }), /lost_response/u);
  const second = createAppSavedState({ accountId: 'A', storage: disk, locks: lock }); t.after(() => second.dispose());
  const expected = second.read().pending;
  const result = await dispatchAppSavedIntent({ state: second, isCurrent: () => true, expectedPending: expected, api: { async request(_op, args) {
    assert.deepEqual(args, captured); return { ...response(expected), replayed: true, current: { revision: 2, entry: null } }; } } });
  assert.equal(result.status, 'accepted'); assert.equal(result.response.receipt.saved, true); assert.equal(result.response.current.entry, null);
  assert.equal(second.read().pending, null);
});

test('rendered pending retry never sends a replacement, even when another tab reuses its request id', async t => {
  const disk = storage(), lock = locks(), a = createAppSavedState({ accountId: 'A', storage: disk, locks: lock, randomId: () => 'same-request' });
  const b = createAppSavedState({ accountId: 'A', storage: disk, locks: lock, randomId: () => 'same-request' }); t.after(() => { a.dispose(); b.dispose(); });
  const displayed = await a.prepare(intent()); await b.abandon(displayed);
  const changed = await b.prepare(intent({ entry: { ...entry, path: '/other' } })); let calls = 0;
  await assert.rejects(dispatchAppSavedIntent({ state: a, expectedPending: displayed, isCurrent: () => true, api: { async request() { calls++; } } }), code('app_saved_pending_changed'));
  assert.equal(calls, 0); assert.deepEqual(a.read().pending, changed);
  await assert.rejects(a.pendingForDispatch(), code('app_saved_pending_changed'));
});

test('replace and no-op have explicit semantics; unknown removal wire contains no domain/path', async t => {
  const state = createAppSavedState({ accountId: 'A', storage: storage(), locks: locks() }); t.after(() => state.dispose());
  await assert.rejects(state.prepare(intent({ currentEntry: stored(), expectedRevision: 1 })), code('app_saved_no_change'));
  await assert.rejects(state.prepare(intent({ entry: { ...entry, path: '/other' }, currentEntry: stored(), expectedRevision: 1 })), code('app_saved_replace_required'));
  const pending = await state.prepare(intent({ saved: false, currentEntry: { ...stored(), current: null }, expectedRevision: 1 }));
  assert.equal('path' in pending.args, false); assert.equal('domainId' in pending.args, false);
});

test('storage denial and absent locks cause zero dispatch, wrong receipts cannot clear exact intent', async t => {
  const bad = createAppSavedState({ accountId: 'A', storage: { getItem: () => null, setItem() { throw new Error('quota'); } }, locks: locks() });
  const noLock = createAppSavedState({ accountId: 'B', storage: storage() }); t.after(() => { bad.dispose(); noLock.dispose(); }); let calls = 0;
  for (const state of [bad, noLock]) await assert.rejects(dispatchAppSavedIntent({ state, intent: intent(), isCurrent: () => true, api: { async request() { calls++; } } }));
  assert.equal(calls, 0);
  const good = createAppSavedState({ accountId: 'C', storage: storage(), locks: locks() }); t.after(() => good.dispose());
  const pending = await good.prepare(intent());
  for (const mutate of [r => { r.requestId = 'wrong'; }, r => { r.receipt.saved = false; }, r => { r.receipt.revision = 3; },
    r => { r.receipt.appId = [app]; }, r => { r.current.revision = 0; }]) {
    const value = response(pending); mutate(value); await assert.rejects(good.acknowledge(pending, value)); assert.deepEqual(good.read().pending, pending);
  }
});

test('late ACK settles only its own record and never returns data to another account screen', async t => {
  const state = createAppSavedState({ accountId: 'A', storage: storage(), locks: locks() }); t.after(() => state.dispose());
  const gate = deferred(); let current = true, started;
  const work = dispatchAppSavedIntent({ state, intent: intent(), isCurrent: () => current, api: { request(_op, args) { started = args; return gate.promise; } } });
  while (!started) await new Promise(done => setImmediate(done));
  const pending = state.read().pending; current = false; gate.resolve(response(pending));
  assert.deepEqual(await work, { status: 'stale', pending }); assert.equal(state.read().pending, null);
});

test('library never combines account-head snapshots, resets expired cursors and refuses oversized/duplicate pages', async () => {
  const model = createAppSavedLibraryState({ accountId: 'A' });
  await model.load({ isCurrent: () => true, api: { async request() { return { revision: 2, entries: [stored(2)], nextCursor: 'opaque' }; } } });
  assert.equal(await model.load({ older: true, isCurrent: () => true, api: { async request() { return { revision: 3, entries: [], nextCursor: null }; } } }), 'reset');
  assert.equal(model.read().entries.length, 0); assert.equal(model.read().resetRequired, true);
  await assert.rejects(model.load({ isCurrent: () => true, api: { async request() { return { revision: 2, entries: [stored(2), stored(2)], nextCursor: null }; } } }), code('app_saved_invalid_page'));
  assert.throws(() => normalizeAppSavedSnapshot({ revision: 1, entry: { ...stored(), domainId: [domain] } }, app)); model.dispose();
});

test('a delayed library page cannot reappear after invalidate or after a newer request completes', async () => {
  const model = createAppSavedLibraryState({ accountId: 'A' }), gate = deferred();
  const old = model.load({ isCurrent: () => true, api: { request: () => gate.promise } });
  model.invalidate(); await model.load({ isCurrent: () => true, api: { async request() { return { revision: 3, entries: [], nextCursor: null }; } } });
  gate.resolve({ revision: 1, entries: [stored()], nextCursor: null }); assert.equal(await old, 'stale');
  assert.deepEqual(model.read().entries, []); assert.equal(model.read().revision, 3); model.dispose();
});
