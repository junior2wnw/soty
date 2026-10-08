import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import { createCallLifecycle } from '../../modules/personal-agent/call/lifecycle.mjs';

const tick = async () => { for (let count = 0; count < 8; count++) await Promise.resolve(); };
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const view = (accountId = 'A', deviceId = 'D') => ({ schema: 'connect.local-view.v1', accountId, deviceId, label: 'Local',
  current: accountId ? { accountId, deviceId, label: 'Local', active: true, revoked: false, vaultRevision: 0, createdAt: '2026-10-08' } : null,
  profiles: [], pendingEnrollment: null, pendingRecovery: null, recoveryPrepared: false, notificationError: null });

async function bindingModule() {
  const source = await readFile(new URL('./call-identity.ts', import.meta.url), 'utf8');
  const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const module = { exports: {} };
  vm.runInNewContext(compiled, { module, exports: module.exports, queueMicrotask });
  return module.exports;
}

function pages() {
  const listeners = new Map();
  return { addEventListener(type, listener) { const set = listeners.get(type) ?? new Set(); set.add(listener); listeners.set(type, set); },
    removeEventListener(type, listener) { listeners.get(type)?.delete(listener); },
    dispatch(type, persisted = false) { for (const listener of [...(listeners.get(type) ?? [])]) listener({ persisted }); },
    count() { return [...listeners.values()].reduce((sum, set) => sum + set.size, 0); } };
}

function call(overrides = {}) {
  const log = { authorize: [], joins: 0, captures: 0, closes: 0, stops: 0 };
  const track = { kind: 'audio', readyState: 'live', enabled: true, stop() { log.stops++; this.readyState = 'ended'; overrides.onStop?.(); } };
  const lifecycle = createCallLifecycle({ host: { authorize(scope, action) { log.authorize.push({ scope, action }); return overrides.authorize?.(scope, action) ?? {}; } },
    transport: { join() { log.joins++; return { close() { log.closes++; overrides.onClose?.(); }, preparePublication() { return { commit() {}, dispose() {} }; } }; } },
    media: { acquire() { log.captures++; return { getTracks: () => [track] }; } } });
  return { lifecycle, track, log };
}

async function fixture(options = {}) {
  const { bindCallIdentity } = await bindingModule();
  const resource = options.resource ?? call(options.call);
  const pageEvents = options.pages ?? pages();
  let current = options.initial ?? view(), listener, unobserved = 0, reads = 0;
  const client = options.client ?? { getLocalState() { reads++; return options.read ? options.read() : Promise.resolve(current); } };
  const observeAccount = options.observe ?? (callback => { listener = callback; return () => { unobserved++; listener = null; }; });
  const ports = { lifecycle: resource.lifecycle, client, observeAccount, pageEvents };
  const dispose = bindCallIdentity(ports);
  return { ...resource, ports, bindCallIdentity, dispose, pages: pageEvents, client,
    emit(value) { current = value; listener?.(value); }, staleListener: () => listener,
    unobserved: () => unobserved, reads: () => reads };
}

test('actual binding initializes only public data, cross-realm fields work and no call/media is started', async () => {
  const f = await fixture(); await tick();
  assert.equal(f.log.authorize.length, 0); assert.equal(f.log.joins, 0); assert.equal(f.log.captures, 0);
  assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).ok, true);
  assert.equal(f.lifecycle.snapshot().scope.accountId, 'A'); assert.equal(f.lifecycle.snapshot().scope.deviceId, 'D');
  assert.equal(f.lifecycle.snapshot().microphone, 'off'); f.dispose();
});

test('same account/device metadata preserves actual call; new device retires it synchronously', async () => {
  const f = await fixture(); await tick(); await f.lifecycle.join({ roomId: 'R', mode: 'group' }); await f.lifecycle.enableMicrophone();
  const before = f.lifecycle.snapshot().scope;
  f.emit({ ...view(), label: 'Renamed' });
  assert.equal(f.lifecycle.snapshot().scope, before); assert.equal(f.track.readyState, 'live');
  f.emit(view('A', 'D2'));
  assert.equal(f.track.readyState, 'ended'); assert.equal(f.lifecycle.snapshot().state, 'ended');
  assert.equal(f.log.captures, 1); assert.equal(f.log.closes, 1); f.dispose();
});

test('subscribe-first late initial read and A-B-A cannot restore an old active call', async () => {
  const read = deferred(), f = await fixture({ read: () => read.promise });
  f.emit(view()); await f.lifecycle.join({ roomId: 'R', mode: 'group' });
  f.emit(view('B', 'DB')); f.emit(view());
  read.resolve(view('old', 'old-device')); await tick();
  assert.equal(f.lifecycle.snapshot().state, 'ended'); assert.equal(f.log.joins, 1);
  await f.lifecycle.join({ roomId: 'R', mode: 'group' });
  assert.equal(f.lifecycle.snapshot().scope.accountId, 'A'); assert.equal(f.lifecycle.snapshot().scope.deviceId, 'D'); f.dispose();
});

test('invalid/revoked/null/error/inconsistent/unknown fields clear identity, no getter reads', async () => {
  let getters = 0;
  const getter = view(); Object.defineProperty(getter, 'accountId', { get() { getters++; return 'private'; } });
  const currentGetter = view(); Object.defineProperty(currentGetter.current, 'revoked', { get() { getters++; return false; } });
  const states = [view(null, null), { ...view(), current: { ...view().current, revoked: true } },
    { ...view(), current: { ...view().current, active: false } }, { ...view(), current: { ...view().current, deviceId: 'other' } },
    { ...view(), notificationError: { code: 'NOTIFICATION_FAILED', message: 'Synthetic' } }, { ...view(), notificationError: undefined },
    { ...view(), extra: true }, { ...view(), accountId: 'x'.repeat(257) }, { ...view(), schema: 'other' }, getter, currentGetter];
  for (const state of states) {
    const f = await fixture(); await tick(); await f.lifecycle.join({ roomId: 'R', mode: 'one-to-one' });
    f.emit(state); assert.equal(f.lifecycle.snapshot().state, 'ended');
    assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'one-to-one' })).code, 'identity_required');
    f.dispose();
  }
  assert.equal(getters, 0);
});

test('initial read rejection fails closed; old rejection cannot wipe newer committed view', async () => {
  const read = deferred(), f = await fixture({ read: () => read.promise });
  read.reject(new Error('Synthetic read')); await tick();
  assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).code, 'identity_required'); f.dispose();
  const old = deferred(), newer = await fixture({ read: () => old.promise });
  newer.emit(view('B', 'DB')); old.reject(new Error('old read')); await tick();
  await newer.lifecycle.join({ roomId: 'R', mode: 'group' }); assert.equal(newer.lifecycle.snapshot().scope.accountId, 'B'); newer.dispose();
});

test('disposer removes its own listeners; late initial read and captured observer cannot revive', async () => {
  const read = deferred(), f = await fixture({ read: () => read.promise }), stale = f.staleListener();
  f.dispose(); f.dispose(); read.resolve(view()); stale(view('B', 'DB')); await tick();
  assert.equal(f.lifecycle.snapshot().state, 'disposed'); assert.equal(f.log.joins, 0);
  assert.equal(f.unobserved(), 1); assert.equal(f.pages.count(), 0);
});

test('pagehide BFCache ends media but keeps page listeners; resume reads current data, never rejoins/unmutes', async () => {
  const f = await fixture(); await tick(); await f.lifecycle.join({ roomId: 'R', mode: 'group' }); await f.lifecycle.enableMicrophone();
  f.pages.dispatch('pagehide', true);
  assert.equal(f.track.readyState, 'ended'); assert.equal(f.lifecycle.snapshot().state, 'ended'); assert.equal(f.pages.count(), 2);
  assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).code, 'identity_required');
  f.emit(view('B', 'DB')); // Frozen/suspended host consumes no participant identity.
  assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).code, 'identity_required');
  f.pages.dispatch('pageshow', true); await tick();
  assert.equal(f.reads(), 2); assert.equal(f.log.joins, 1); assert.equal(f.log.captures, 1);
  await f.lifecycle.join({ roomId: 'R', mode: 'group' }); assert.equal(f.lifecycle.snapshot().scope.accountId, 'B'); f.dispose();
});

test('resume read is fenced by newer observer/disposal and read failure stays identity-free', async () => {
  const pending = deferred(); let count = 0;
  const f = await fixture({ read: () => ++count === 1 ? Promise.resolve(view()) : pending.promise }); await tick();
  f.pages.dispatch('pagehide', true); f.pages.dispatch('pageshow', true);
  f.emit(view('C', 'DC')); pending.resolve(view('A', 'D')); await tick();
  await f.lifecycle.join({ roomId: 'R', mode: 'group' }); assert.equal(f.lifecycle.snapshot().scope.accountId, 'C'); f.dispose();
  const failed = deferred(); let reads = 0;
  const second = await fixture({ read: () => ++reads === 1 ? Promise.resolve(view()) : failed.promise }); await tick();
  second.pages.dispatch('pagehide', true); second.pages.dispatch('pageshow', true); failed.reject(new Error('resume read')); await tick();
  assert.equal((await second.lifecycle.join({ roomId: 'R', mode: 'group' })).code, 'identity_required'); second.dispose();
});

test('nonpersisted pagehide terminates owner and cleans listeners, pageshow cannot resurrect', async () => {
  const f = await fixture(); await tick(); await f.lifecycle.join({ roomId: 'R', mode: 'group' });
  f.pages.dispatch('pagehide', false); f.pages.dispatch('pageshow', false); await tick();
  assert.equal(f.lifecycle.snapshot().state, 'disposed'); assert.equal(f.pages.count(), 0); assert.equal(f.unobserved(), 1);
});

test('one lifecycle instance cannot acquire a second binding, including after terminal disposal', async () => {
  const f = await fixture(); await tick();
  assert.throws(() => f.bindCallIdentity(f.ports), /call_identity_already_bound/);
  assert.equal(f.pages.count(), 2); f.dispose();
  assert.throws(() => f.bindCallIdentity(f.ports), /call_identity_already_bound/);
  assert.equal(f.pages.count(), 0);
});

test('same real native leases persist across identity switches; capacity is never reset', async () => {
  const outstanding = [], f = await fixture({ call: { authorize() { const work = deferred(); outstanding.push(work); return work.promise; } } }); await tick();
  for (let index = 0; index < 16; index++) {
    f.emit(view(`A${index}`, `D${index}`)); const joining = f.lifecycle.join({ roomId: 'R', mode: 'group' }); await tick();
    f.emit(view(`B${index}`, `DB${index}`)); await joining;
  }
  assert.equal(outstanding.length, 16); assert.equal(f.lifecycle.snapshot().capacity.occupied, 16);
  assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).code, 'call_capacity');
  for (const work of outstanding) work.resolve({}); await tick();
  assert.equal(f.lifecycle.snapshot().capacity.occupied, 0); assert.equal(f.log.joins, 0); f.dispose();
});

test('reentrant close notification applies only newest identity and cannot admit old generation', async () => {
  let f, once = false;
  f = await fixture({ call: { onClose() { if (!once) { once = true; f.emit(view('B', 'DB')); } } } }); await tick();
  await f.lifecycle.join({ roomId: 'R', mode: 'group' }); f.emit(view('C', 'DC')); await tick();
  await f.lifecycle.join({ roomId: 'R', mode: 'group' });
  assert.equal(f.lifecycle.snapshot().scope.accountId, 'B'); assert.equal(f.log.closes, 1); f.dispose();
});

test('actual call_transition during mute gets one newest retry, no stale authorization or extra capture', async () => {
  let f, once = false;
  f = await fixture({ call: { onStop() { if (!once) { once = true; f.emit(view('B', 'DB')); } } } }); await tick();
  await f.lifecycle.join({ roomId: 'R', mode: 'group' }); await f.lifecycle.enableMicrophone();
  f.lifecycle.mute(); const staleJoin = f.lifecycle.join({ roomId: 'R', mode: 'group' }); await tick();
  assert.equal((await staleJoin).code, 'stale'); assert.equal(f.log.authorize.length, 2); assert.equal(f.log.captures, 1);
  await f.lifecycle.join({ roomId: 'R', mode: 'group' }); assert.equal(f.lifecycle.snapshot().scope.accountId, 'B'); f.dispose();
});

test('permanently failing transition is bounded and terminal rather than an everlasting retry', async () => {
  let calls = 0, ended = 0, disposed = 0;
  const lifecycle = { setIdentity() { calls++; return { ok: false, code: 'call_transition' }; }, end() { ended++; }, dispose() { disposed++; } };
  const f = await fixture({ resource: { lifecycle } }); await tick();
  assert.equal(calls, 3, 'two application attempts plus final terminal identity cleanup');
  assert.equal(disposed, 1); assert.ok(ended <= 2); assert.equal(f.pages.count(), 0); assert.equal(f.unobserved(), 1);
  f.emit(view()); await tick(); assert.equal(calls, 3);
});

test('subscription startup throws cleans page listeners and terminal owner', async () => {
  const resource = call(), pageEvents = pages(), { bindCallIdentity } = await bindingModule();
  assert.throws(() => bindCallIdentity({ lifecycle: resource.lifecycle, client: { getLocalState: async () => view() },
    observeAccount() { throw new Error('subscribe failed'); }, pageEvents }), /subscribe failed/);
  assert.equal(pageEvents.count(), 0); assert.equal(resource.lifecycle.snapshot().state, 'disposed');
});

test('getLocalState accessor ending page cannot invoke retired read function', async () => {
  const pageEvents = pages(); let calls = 0;
  const client = { get getLocalState() { pageEvents.dispatch('pagehide', false); return async () => { calls++; return view(); }; } };
  const f = await fixture({ pages: pageEvents, client }); await tick();
  assert.equal(calls, 0); assert.equal(f.lifecycle.snapshot().state, 'disposed'); assert.equal(f.pages.count(), 0);
});

test('reentrant pageshow during close keeps newest resume read without automatic voice effects', async () => {
  let f, once = false;
  f = await fixture({ call: { onClose() {
    if (!once) { once = true; f.emit(view('B', 'DB')); f.pages.dispatch('pageshow', true); }
  } } }); await tick(); await f.lifecycle.join({ roomId: 'R', mode: 'group' });
  f.pages.dispatch('pagehide', true); await tick();
  assert.equal(f.log.joins, 1); assert.equal(f.log.captures, 0);
  assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).ok, true);
  assert.equal(f.lifecycle.snapshot().scope.accountId, 'B'); f.dispose();
});

test('reentrant A-B-A restores latest actual owner identity before fresh explicit join', async () => {
  let f, once = false;
  f = await fixture({ initial: view('A', 'device-A'), call: { onClose() {
    if (!once) { once = true; f.emit(view('A', 'device-A')); }
  } } }); await tick();
  assert.equal((await f.lifecycle.join({ roomId: 'first', mode: 'group' })).ok, true);
  f.emit(view('B', 'device-B')); await tick();
  const next = await f.lifecycle.join({ roomId: 'second', mode: 'group' });
  const actual = f.lifecycle.snapshot().scope;
  assert.equal(next.ok, true, 'the newer coherent identity can start only a fresh explicit intent');
  assert.equal(actual.accountId, 'A', 'reentrant A→B→A must not retain the retired B context behind an A view');
  assert.equal(actual.deviceId, 'device-A'); assert.equal(f.log.captures, 0);
  assert.deepEqual(f.log.authorize.map(item => item.scope.accountId), ['A', 'A']); f.dispose();
});

test('a deferred pageshow from earlier hide cannot resume a later persisted hide', async () => {
  let f, once = false;
  f = await fixture({ call: { onClose() {
    if (!once) { once = true; f.emit(view('B', 'DB')); f.pages.dispatch('pageshow', true); }
  } } }); await tick(); await f.lifecycle.join({ roomId: 'R', mode: 'group' });
  f.pages.dispatch('pagehide', true); f.pages.dispatch('pagehide', true); await tick();
  assert.equal(f.reads(), 1, 'no read may be started by a pageshow queued before the latest hide');
  assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).code, 'identity_required', 'the newer hidden page must remain identity-free');
  f.pages.dispatch('pageshow', true); await tick();
  assert.equal(f.reads(), 2); assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).ok, true);
  assert.equal(f.lifecycle.snapshot().scope.accountId, 'B'); f.dispose();
});

test('nested newer hide and resume retain only newest page ticket after outer cleanup stack', async () => {
  let f, once = false;
  f = await fixture({ call: { onClose() {
    if (!once) {
      once = true; f.emit(view('B', 'DB')); f.pages.dispatch('pageshow', true);
      f.pages.dispatch('pagehide', true); f.pages.dispatch('pageshow', true); f.pages.dispatch('pageshow', true);
    }
  } } }); await tick(); await f.lifecycle.join({ roomId: 'R', mode: 'group' });
  f.pages.dispatch('pagehide', true); await tick();
  assert.equal(f.reads(), 2, 'one initial read and exactly one newest resume read');
  assert.equal(f.log.joins, 1); assert.equal(f.log.captures, 0);
  assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).ok, true);
  assert.equal(f.lifecycle.snapshot().scope.accountId, 'B'); f.dispose();
});
