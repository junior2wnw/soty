import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import { createCallLifecycle } from '../../modules/personal-agent/call/lifecycle.mjs';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { emptyState } from '../../modules/connect/browser/storage.mjs';

const tick = async () => { for (let count = 0; count < 10; count++) await Promise.resolve(); };
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const view = (accountId = 'A', deviceId = 'D') => ({ schema: 'connect.local-view.v1', accountId, deviceId, label: 'Local',
  current: { accountId, deviceId, active: true, revoked: false }, profiles: [], pendingEnrollment: null,
  pendingRecovery: null, recoveryPrepared: false, notificationError: null });

async function compiled(path, ports = {}, extras = {}) {
  const source = await readFile(new URL(path, import.meta.url), 'utf8');
  const code = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const module = { exports: {} };
  vm.runInNewContext(code, { module, exports: module.exports, require: name => ports[name], queueMicrotask, ...extras });
  return module.exports;
}

async function core() {
  let options;
  const exports = await compiled('../core/connect-client.ts', {
    '../../modules/connect/browser/index.mjs': { createConnectClient(value) { options = value; return {}; } },
  });
  return { ...exports, options };
}

test('actual core delivers later observer despite early throw, preserving SDK failure expectation', async () => {
  const c = await core(); let received = 0, callbackRejected = false;
  c.observeAccount(() => { throw new Error('synthetic_observer_failure'); });
  c.observeAccount(() => { received++; });
  const result = c.options.onState(view('B', 'D2'));
  assert.equal(received, 1, 'one failed screen observer must not prevent the later identity observer from receiving a committed change');
  try { await result; } catch { callbackRejected = true; }
  assert.equal(callbackRejected, true, 'SDK must still observe the failure rather than silently losing diagnostics');
});

test('throw plus hanging observer cannot delay synchronous delivery or promptly rejected aggregate', async () => {
  const c = await core(), late = deferred(); let received = 0, rejected = false;
  c.observeAccount(() => { throw new Error('early'); });
  c.observeAccount(() => new Promise(() => {}));
  c.observeAccount(() => late.promise);
  c.observeAccount(() => { received++; });
  const result = c.options.onState(view());
  assert.equal(received, 1);
  void result.catch(() => { rejected = true; }); await tick();
  assert.equal(rejected, true);
  late.reject(new Error('later')); await tick(); // node:test flags unhandled rejections.
});

test('async-only rejected observer is observed by aggregate without starving later listener', async () => {
  const c = await core(); let delivered = false;
  c.observeAccount(async () => { throw new Error('async observer'); });
  c.observeAccount(() => { delivered = true; });
  const result = c.options.onState(view()); assert.equal(delivered, true);
  await assert.rejects(result, /async observer/);
});

test('actual SDK records safe sticky notificationError for failing actual core fanout', async () => {
  const c = await core(); let delivered = 0, channel, errors = 0, network = 0;
  c.observeAccount(() => { throw new Error('private synthetic observer detail'); });
  c.observeAccount(() => { delivered++; });
  const state = emptyState('soty', '/api/connect/rpc'); state.localRevision = 1;
  const sdk = createClientWithStorage({ projectId: 'soty', endpoint: '/api/connect/rpc',
    fetcher() { network++; throw new Error('unexpected_network'); },
    createChannel() { channel = { postMessage() {}, close() {} }; return channel; },
    onState: c.options.onState, onError() { errors++; } }, { read: async () => structuredClone(state) });
  channel.onmessage({ data: { schema: 'connect.local-change.v1', projectId: 'soty', localRevision: 1 } });
  await tick(); const local = await sdk.getLocalState();
  assert.equal(delivered, 1); assert.equal(errors, 1); assert.equal(network, 0);
  assert.equal(local.notificationError.code, 'NOTIFICATION_FAILED');
  assert.ok(!local.notificationError.message.includes('private synthetic')); sdk.dispose();
});

test('fanout uses one listener snapshot, reentrant listener replacement cannot create repeat loop', async () => {
  const c = await core(); let first = 0, later = 0, remove;
  const callback = () => { first++; remove(); remove = c.observeAccount(callback); };
  remove = c.observeAccount(callback); c.observeAccount(() => { later++; });
  await c.options.onState(view()); assert.equal(first, 1); assert.equal(later, 1);
  await c.options.onState(view()); assert.equal(first, 2); assert.equal(later, 2);
});

async function worldFixture({ failRead = false, failMount = false, failBootstrap = false } = {}) {
  const subscribers = new Set(), pageListeners = new Map(), mounts = [], order = [];
  let current = failBootstrap ? view(null, null) : view(), resets = 0;
  const startupError = new Error('synthetic startup failure');
  const client = { getLocalState() { order.push('read'); return failRead ? Promise.reject(startupError) : Promise.resolve(current); },
    async bootstrap() { if (failBootstrap) throw startupError; throw new Error('unexpected bootstrap'); }, async extension() { return {}; } };
  const observeAccount = callback => { order.push('subscribe'); subscribers.add(callback); return () => subscribers.delete(callback); };
  const window = { location: { href: 'https://offline.example/' },
    addEventListener(type, callback) { const set = pageListeners.get(type) ?? new Set(); set.add(callback); pageListeners.set(type, set); },
    removeEventListener(type, callback) { pageListeners.get(type)?.delete(callback); } };
  const binding = await compiled('./call-identity.ts');
  const adapter = await compiled('./world-adapter.ts', {
    '../core/connect-client': { accountClient: client, observeAccount }, './call-identity': binding,
    '../world/app': { mountWorldApp() { if (failMount) throw startupError; const world = { destroy() { world.destroyed = true; }, async refresh() {} }; mounts.push(world); return world; } },
    './local-apps': { createAppActions: () => ({ resetAccount() { resets++; }, destroy() {}, connectDevice() {} }) },
    './assistant': { mountAssistant() {} },
  }, { URL, window });
  let authorizations = 0, captures = 0;
  const lifecycle = createCallLifecycle({ host: { authorize() { authorizations++; return {}; } },
    media: { acquire() { captures++; throw new Error('unexpected capture'); } },
    transport: { join: () => ({ close() {}, preparePublication() {} }) } });
  return { adapter, lifecycle, client, order, mounts, startupError,
    subscribers: () => subscribers.size, pageCount: () => [...pageListeners.values()].reduce((sum, set) => sum + set.size, 0),
    emit(value) { current = value; for (const callback of [...subscribers]) callback(value); },
    dispatch(type, persisted) { for (const callback of [...(pageListeners.get(type) ?? [])]) callback({ persisted }); },
    authorizations: () => authorizations, captures: () => captures, resets: () => resets };
}

test('actual world default branch creates no call owner/binding/media/control', async () => {
  const f = await worldFixture(); await f.adapter.startWorld({}); await tick();
  assert.equal(f.subscribers(), 1); assert.equal(f.pageCount(), 1);
  assert.equal(f.authorizations(), 0); assert.equal(f.captures(), 0);
  assert.equal((await f.lifecycle.join({ roomId: 'R', mode: 'group' })).code, 'identity_required');
  f.dispatch('pagehide', false);
});

test('actual optional world binds before first await and retains owner across world remounts/BFCache', async () => {
  const f = await worldFixture(); await f.adapter.startWorld({}, { callLifecycle: f.lifecycle }); await tick();
  assert.equal(f.order[0], 'subscribe'); assert.equal(f.subscribers(), 2); assert.equal(f.pageCount(), 3);
  await f.lifecycle.join({ roomId: 'R', mode: 'group' });
  f.emit(view('B', 'DB')); assert.equal(f.lifecycle.snapshot().state, 'ended');
  assert.equal(f.mounts.length, 2); assert.equal(f.resets(), 1);
  f.dispatch('pagehide', true); assert.equal(f.mounts[1].destroyed, undefined); assert.equal(f.subscribers(), 2);
  f.dispatch('pageshow', true); await tick(); assert.equal(f.authorizations(), 1); assert.equal(f.captures(), 0);
  f.dispatch('pagehide', false); assert.equal(f.lifecycle.snapshot().state, 'disposed'); assert.equal(f.subscribers(), 0);
});

test('actual optional world startup read/bootstrap/mount failure removes binding and preserves original error', async () => {
  for (const options of [{ failRead: true }, { failBootstrap: true }, { failMount: true }]) {
    const f = await worldFixture(options);
    await assert.rejects(f.adapter.startWorld({}, { callLifecycle: f.lifecycle }), error => error === f.startupError); await tick();
    assert.equal(f.subscribers(), 0); assert.equal(f.pageCount(), 0); assert.equal(f.lifecycle.snapshot().state, 'disposed');
    assert.equal(f.authorizations(), 0); assert.equal(f.captures(), 0);
  }
  const defaultFailure = await worldFixture({ failRead: true });
  await assert.rejects(defaultFailure.adapter.startWorld({}), error => error === defaultFailure.startupError);
  assert.equal(defaultFailure.subscribers(), 0); assert.equal(defaultFailure.pageCount(), 0);
  assert.equal(defaultFailure.lifecycle.snapshot().state, 'idle');
});
