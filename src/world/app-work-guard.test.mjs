import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setImmediate as tick } from 'node:timers/promises';
import { createSotyAppWork } from '../../modules/source-app/browser/app-work.mjs';
import { createAppWorkGuard } from './app-work-guard.mjs';

function fixture({ compatible = true, timeoutMs = 25, save } = {}) {
  const hostListeners = new Set(), childListeners = new Set(), loads = new Set(), origin = 'https://app.example', shell = 'https://soty.example';
  let dirty = false, current = true, count = 0, sent = 0, drop = false, session = '';
  const view = { crypto: { randomUUID }, setTimeout, clearTimeout,
    addEventListener: (_, fn) => hostListeners.add(fn), removeEventListener: (_, fn) => hostListeners.delete(fn) };
  const parent = { postMessage(data, target) { assert.equal(target, shell); if (!drop) queueMicrotask(() => {
    for (const fn of hostListeners) fn({ origin, source: child, data });
  }); } };
  const childView = { parent, addEventListener: (_, fn) => childListeners.add(fn), removeEventListener: (_, fn) => childListeners.delete(fn) };
  const child = { postMessage(data, target) { assert.equal(target, origin); sent++; session = data.session; queueMicrotask(() => {
    for (const fn of childListeners) fn({ origin: shell, source: parent, data });
  }); } };
  const frame = { contentWindow: child, isConnected: true, addEventListener: (_, fn) => loads.add(fn), removeEventListener: (_, fn) => loads.delete(fn) };
  let selected = frame;
  const guard = createAppWorkGuard({ view, getFrame: () => selected, isCurrent: () => current, timeoutMs });
  const enable = () => createSotyAppWork({ parentOrigin: shell, view: childView, isDirty: () => dirty,
    save: async () => { count++; if (save) await save(); else dirty = false; } });
  let sdk = compatible ? enable() : null;
  guard.attach(frame, origin); for (const load of loads) load();
  return { guard, sdk, frame, parent, child, hostListeners, childListeners,
    dirty(value) { dirty = value; sdk?.changed(); }, count: () => count, sent: () => sent,
    session: () => session, drop(value) { drop = value; }, account(value) { current = value; },
    select(value) { selected = value; }, reload() { for (const load of loads) load(); }, enable() { sdk = enable(); },
    receive(event) { for (const fn of hostListeners) fn(event); },
    close() { sdk?.dispose(); guard.dispose(); } };
}

test('compatible app saves once; ordinary presentation changes need no save', async t => {
  const f = fixture(); t.after(f.close); await tick(); assert.equal(f.guard.requiresCheck(), true);
  f.dirty(true); await tick(); assert.equal(f.guard.hasUnsavedChanges(), true);
  await f.guard.flush(); assert.equal(f.count(), 1); assert.equal(f.guard.hasUnsavedChanges(), false);
  await f.guard.flush(); assert.equal(f.count(), 1);
});
test('unmodified legacy iframe does not block the shell', async t => {
  const f = fixture({ compatible: false }); t.after(f.close); await tick();
  assert.equal(f.guard.requiresCheck(), false); assert.equal(f.guard.hasUnconfirmedWork(), true);
  await f.guard.flush(); assert.equal(f.count(), 0);
  f.frame.isConnected = false; assert.equal(f.guard.hasUnconfirmedWork(), false);
});
test('an app initialised after iframe load is discovered without reloading it', async t => {
  const f = fixture({ compatible: false }); t.after(f.close); await tick();
  assert.equal(f.guard.requiresCheck(), false); f.dirty(true); f.enable(); await tick();
  assert.equal(f.guard.requiresCheck(), true); assert.equal(f.guard.hasUnsavedChanges(), true);
  await f.guard.flush(); assert.equal(f.count(), 1); assert.equal(f.guard.hasUnsavedChanges(), false);
});
test('lost response blocks departure, and explicit retry never duplicates an in-flight save', async t => {
  let finish; const f = fixture({ timeoutMs: 35, save: () => new Promise(resolve => { finish = resolve; }) }); t.after(f.close);
  await tick(); f.dirty(true); await tick();
  await assert.rejects(f.guard.flush(), /timeout/); assert.equal(f.guard.hasUnsavedChanges(), true); assert.equal(f.count(), 1);
  const next = f.guard.flush(); await tick(); f.dirty(false); finish(); await next;
  assert.equal(f.count(), 1); assert.equal(f.guard.hasUnsavedChanges(), false);
});
test('a save error or an edit during save keeps work open', async t => {
  const f = fixture({ save: async () => {} }); t.after(f.close); await tick(); f.dirty(true); await tick();
  await assert.rejects(f.guard.flush(), /not_saved/); assert.equal(f.guard.hasUnsavedChanges(), true);
});
test('only exact origin, iframe, session and current account can report a state', async t => {
  const f = fixture({ compatible: false }); t.after(f.close); await tick();
  const sent = f.sent(), hello = { schema: 'soty.app-work.v1', type: 'hello' };
  f.receive({ origin: 'https://attacker.example', source: f.child, data: hello });
  f.receive({ origin: 'https://app.example', source: {}, data: hello });
  f.account(false); f.receive({ origin: 'https://app.example', source: f.child, data: hello });
  f.account(true); f.select({}); f.receive({ origin: 'https://app.example', source: f.child, data: hello });
  f.select(f.frame); assert.equal(f.sent(), sent);
  const data = { schema: 'soty.app-work.v1', type: 'state', session: f.session(), dirty: true, revision: 0 };
  f.receive({ origin: 'https://attacker.example', source: f.child, data });
  f.receive({ origin: 'https://app.example', source: {}, data });
  f.receive({ origin: 'https://app.example', source: f.child, data: { ...data, session: randomUUID() } });
  f.account(false); f.receive({ origin: 'https://app.example', source: f.child, data });
  f.account(true); f.select({}); f.receive({ origin: 'https://app.example', source: f.child, data });
  assert.equal(f.guard.requiresCheck(), false); assert.equal(f.guard.hasUnsavedChanges(), false);
});
test('stale documents cannot answer a new frame challenge', async t => {
  const f = fixture(); t.after(f.close); await tick(); const old = f.session(); f.reload(); await tick();
  assert.notEqual(f.session(), old); f.receive({ origin: 'https://app.example', source: f.child,
    data: { schema: 'soty.app-work.v1', type: 'changed', session: old, dirty: true, revision: 99 } });
  assert.equal(f.guard.hasUnsavedChanges(), false);
});
test('child only accepts save from its configured parent', async t => {
  const f = fixture(); t.after(f.close); await tick(); f.dirty(true); await tick();
  const data = { schema: 'soty.app-work.v1', type: 'save', session: f.session(), requestId: randomUUID() };
  for (const fn of f.childListeners) { fn({ origin: 'https://attacker.example', source: f.parent, data });
    fn({ origin: 'https://soty.example', source: {}, data }); }
  await tick(); assert.equal(f.count(), 0);
});
test('lost check response fails closed after the app was recognised', async t => {
  const f = fixture(); t.after(f.close); await tick(); f.drop(true);
  await assert.rejects(f.guard.flush(), /timeout/); assert.equal(f.guard.hasUnsavedChanges(), true);
});
