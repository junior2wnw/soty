import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import * as intents from './connection-intent.mjs';
import * as navigation from './connection-navigation.mjs';
import * as voiceDescriptors from './native-voice-descriptor.mjs';

// Exercise the actual adapter. These controlled SDK/fetch/lifecycle ports prove
// continuation ordering, not a signed-service, browser, storage or media call.
const source = await readFile(new URL('./world-adapter.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const turn = () => new Promise(resolve => setImmediate(resolve));
async function until(predicate) { const deadline = Date.now() + 5000; while (!predicate()) { assert.ok(Date.now() < deadline, 'bounded adapter operation completed'); await turn(); } }
const state = (accountId = 'account-A', deviceId = 'device-A') => ({ accountId, deviceId, label: 'Fixture', current: { accountId, deviceId, revoked: false } });
function fixture(t, config = {}) {
  let selected = state(), reads = 0, fetches = 0, actionsDestroyed = 0, detached = 0, disposed = 0, bootstraps = 0;
  const entry = deferred(), observers = new Set(), events = new Map(), mounts = [], requests = [], follows = [], navigationCalls = [], notifications = [];
  const root = { isConnected: true }, window = { location: { href: config.href ?? 'https://shell.example/', origin: 'https://shell.example', assign(value) { navigationCalls.push(value); } },
    addEventListener(name, fn) { const set = events.get(name) ?? new Set(); set.add(fn); events.set(name, set); }, removeEventListener(name, fn) { events.get(name)?.delete(fn); } };
  window.history = { replaceState(_state, _title, value) { window.location.href = new URL(value, window.location.href).href; } };
  const client = { async getLocalState() { const index = ++reads; return config.read ? config.read(index, selected) : selected; },
    async bootstrap() { bootstraps++; throw Error('unexpected automatic bootstrap'); },
    async extension(method, args, context) { requests.push({ method, args, context }); return {}; } };
  const lifecycle = { dispose() { disposed++; } };
  const ports = {
    './notifications-client.mjs': { createNotificationsController(options) {
      const own = { options, refreshes: 0, disposals: [], async refresh() { this.refreshes++; }, dispose(options = {}) { this.disposals.push(options); } };
      notifications.push(own); return own;
    } },
    '../core/connect-client': { accountClient: client, observeAccount(fn) { observers.add(fn); return () => observers.delete(fn); } },
    './connection-hub': { ensureConnectionAccount() { return entry.promise; }, openConnectionHub() { throw Error('warm hub not used'); } },
    './connection-intent.mjs': intents, './connection-navigation.mjs': navigation, './native-voice-descriptor.mjs': voiceDescriptors,
    './call-identity': { bindCallIdentity() { return () => { detached++; }; } },
    './local-apps': { createAppActions: () => ({ connectDevice() {}, agentCreate() {}, destroy() { actionsDestroyed++; }, resetAccount() {} }) },
    '../world/app': { mountWorldApp(_root, options) {
      const mount = { options, destroyed: 0, href: window.location.href }; mounts.push(mount);
      void options.api.request('fixture.actor');
      return { async refresh() { if (config.refresh) await config.refresh(mounts.length); }, destroy() { mount.destroyed++; },
        async followConnection(intent, navigate) { follows.push(intent); if (navigate) await navigate(); } };
    } },
    './assistant': {}, '../sync': {}, '../../modules/connect/ui/index.mjs': {}, qrcode: {},
  };
  const module = { exports: {} };
  vm.runInNewContext(compiled, { module, exports: module.exports, require: name => ports[name] ?? {}, window, URL, AbortSignal, AbortController,
    fetch: () => { fetches++; return config.capabilities ? config.capabilities() : Promise.resolve({ ok: true, async json() { return { nativeVoice: null }; } }); } });
  const result = { root, window, client, mounts, requests, follows, navigationCalls, observers, events, notifications,
    get reads() { return reads; }, get fetches() { return fetches; }, get disposed() { return disposed; }, get detached() { return detached; },
    get actionsDestroyed() { return actionsDestroyed; }, get bootstraps() { return bootstraps; },
    change(next) { selected = next; for (const fn of [...observers]) fn(next); },
    pagehide(persisted = false) { for (const fn of [...(events.get('pagehide') ?? [])]) fn({ persisted }); },
    start(intent = null) { const running = module.exports.startWorld(root, { callLifecycle: lifecycle }); entry.resolve({ state: selected, intent }); return running; },
  };
  t.after(() => result.pagehide()); return result;
}

test('startup subscribes before optional capabilities and mounts the current account rather than the old entry', async t => {
  const cap = deferred(), durable = deferred(), f = fixture(t, { capabilities: () => cap.promise, read: (index, selected) => index === 1 ? durable.promise : selected }), running = f.start();
  await until(() => f.reads === 1);
  try { assert.equal(f.observers.size, 1); assert.equal(f.fetches, 1); f.change(state('account-B', 'device-B')); }
  finally { durable.resolve(state()); cap.resolve({ ok: false }); await running; }
  assert.equal(f.mounts.length, 1); assert.equal(f.requests[0].context.expectedAccountId, 'account-B'); assert.equal(f.bootstraps, 0);
});

test('startup discards a durable snapshot crossed by a later committed identity event', async t => {
  const first = deferred(), f = fixture(t, { read: (index, selected) => index === 1 ? first.promise : selected }), running = f.start();
  await until(() => f.reads === 1); f.change(state('account-B', 'device-B')); first.resolve(state()); await running;
  assert.equal(f.reads, 2); assert.equal(f.mounts.length, 1); assert.equal(f.requests[0].context.expectedAccountId, 'account-B');
});

test('startup identity event tickets reject an account A to B to A stale read', async t => {
  const first = deferred(), f = fixture(t, { read: (index, selected) => index === 1 ? first.promise : selected }), running = f.start();
  await until(() => f.reads === 1); f.change(state('account-B', 'device-B')); f.change(state()); first.resolve(state()); await running;
  assert.equal(f.reads, 2); assert.equal(f.requests[0].context.expectedAccountId, 'account-A');
});

test('startup request timings contain only fixed operation names and numbers; stale profiles cannot append to the current trace', async t => {
  const f = fixture(t); f.root.dataset = {}; await f.start();
  const old = f.mounts[0].options.api, held = deferred();
  f.client.extension = async method => method === 'world.profile.get' ? held.promise : {};
  const pending = old.request('world.profile.get', { privateInput: 'never-in-diagnostics' });
  f.change(state('account-B', 'device-B')); held.resolve({ secret: 'never-in-diagnostics' }); await pending;
  assert.deepEqual(JSON.parse(f.root.dataset.startupRequestTimings), []);
  const current = f.mounts.at(-1).options.api;
  await current.request('world.field.get', { privateInput: 'never-in-diagnostics' });
  await current.request('unknown.private-operation', { privateInput: 'never-in-diagnostics' });
  const trace = JSON.parse(f.root.dataset.startupRequestTimings);
  assert.equal(trace.length, 1); assert.equal(trace[0].operation, 'world.field.get'); assert.equal(trace[0].ok, true);
  assert.equal(typeof trace[0].startedAt, 'number'); assert.ok(trace[0].completedAt >= trace[0].startedAt);
  assert.equal(/never-in-diagnostics|account-B|device-B|private-operation/u.test(JSON.stringify(trace)), false);
});

test('startup never follows an entry intent after its device changes within the same account', async t => {
  const durable = deferred(), f = fixture(t, { read: (index, selected) => index === 1 ? durable.promise : selected }), running = f.start({ kind: 'profile', id: 'profile123', route: '#profile/profile123' });
  await until(() => f.reads === 1); f.change(state('account-A', 'device-new')); durable.resolve(state()); await running;
  assert.equal(f.mounts.length, 1); assert.deepEqual(f.follows, []); assert.deepEqual(f.navigationCalls, []);
  f.change(state('account-A', 'device-newer')); assert.equal(f.mounts.length, 2); assert.equal(f.mounts[0].destroyed, 1);
});

test('nonpersisted pagehide during pending metadata and durable IO closes owners and cannot mount a late world', async t => {
  const cap = deferred(), durable = deferred(), f = fixture(t, { capabilities: () => cap.promise, read: () => durable.promise }), running = f.start();
  await until(() => f.reads === 1); f.pagehide(); durable.resolve(state()); cap.resolve({ ok: false }); await running;
  assert.equal(f.mounts.length, 0); assert.equal(f.observers.size, 0); assert.equal(f.events.get('pagehide')?.size, 0);
  assert.equal(f.actionsDestroyed, 1); assert.equal(f.detached, 1); assert.equal(f.disposed, 1);
});

test('nonpersisted pagehide during durable reread cannot revive the startup', async t => {
  const first = deferred(), f = fixture(t, { read: () => first.promise }), running = f.start();
  await until(() => f.reads === 1); f.pagehide(); first.resolve(state()); await running;
  assert.equal(f.mounts.length, 0); assert.equal(f.observers.size, 0); assert.equal(f.actionsDestroyed, 1);
});

test('optional capabilities JSON error keeps the shell usable and closes owners on pagehide', async t => {
  const expected = Error('synthetic JSON failure'), f = fixture(t, { capabilities: async () => ({ ok: true, json: async () => { throw expected; } }) });
  await f.start(); assert.equal(f.mounts.length, 1); assert.equal(f.observers.size, 1);
  await assert.rejects(f.mounts[0].options.readNativeVoice(), { code: 'native_voice_descriptor_unavailable' });
  assert.equal(f.mounts[0].destroyed, 0); f.pagehide();
  assert.equal(f.observers.size, 0); assert.equal(f.events.get('pagehide')?.size, 0);
  assert.equal(f.actionsDestroyed, 1); assert.equal(f.detached, 1); assert.equal(f.disposed, 1);
});

test('durable startup error never mounts or silently creates another account', async t => {
  const expected = Object.assign(Error('synthetic key mismatch'), { code: 'KEY_MISMATCH' }), f = fixture(t, { read: async () => { throw expected; } });
  await assert.rejects(f.start(), error => error === expected);
  assert.equal(f.mounts.length, 0); assert.equal(f.bootstraps, 0); assert.equal(f.observers.size, 0); assert.equal(f.actionsDestroyed, 1);
});

test('persisted pagehide preserves the mounted DOM and observer for browser restoration', async t => {
  const f = fixture(t); await f.start(); f.pagehide(true);
  assert.equal(f.mounts.length, 1); assert.equal(f.mounts[0].destroyed, 0); assert.equal(f.observers.size, 1);
  assert.equal(f.actionsDestroyed, 0); assert.equal(f.disposed, 0);
});

test('same actor revision refreshes notifications; closing the page keeps its binding, changing actor clears it', async t => {
  const f = fixture(t); await f.start();
  const first = f.notifications[0]; assert.equal(first.refreshes, 1);
  f.change({ ...state(), localRevision: 2, label: 'Renamed' });
  assert.equal(f.mounts.length, 1); assert.equal(first.refreshes, 2); assert.equal(first.disposals.length, 0);
  f.pagehide(true); assert.equal(first.disposals.length, 0);
  f.change({ ...state('account-B', 'device-B'), localRevision: 3 });
  assert.equal(first.disposals.length, 1); assert.equal(first.disposals[0].accountChanged, true);
  const second = f.notifications[1]; assert.equal(second.refreshes, 1);
  f.pagehide(); assert.equal(second.disposals.length, 1); assert.equal(second.disposals[0].accountChanged, undefined);
});

const appLink = () => `https://shell.example/#app/app-${'a'.repeat(32)}?invitation=${'b'.repeat(64)}`;
test('normal application URL is neutral at mount and its actual one-use carrier is followed once', async t => {
  const f = fixture(t, { href: appLink() }); await f.start();
  assert.equal(f.mounts[0].href, 'https://shell.example/#mine'); assert.equal(f.follows.length, 1);
  assert.equal(f.follows[0].kind, 'application'); assert.equal(f.follows[0].launch.takeInvitation(), 'b'.repeat(64));
  assert.equal(f.follows[0].launch.takeInvitation(), null); assert.deepEqual(f.navigationCalls, []);
});
test('cold application continuation owns the URL before mount and preserves the original typed carrier', async t => {
  const input = appLink(), intent = intents.parseConnectionIntent(input, 'https://shell.example'), f = fixture(t, { href: input });
  await f.start(intent); assert.equal(f.mounts[0].href, 'https://shell.example/#mine');
  assert.equal(f.follows.length, 1); assert.equal(f.follows[0], intent); assert.equal(f.follows[0].launch.takeInvitation(), 'b'.repeat(64));
});
test('normal owned application cannot auto-dispatch to an account selected during durable startup', async t => {
  const durable = deferred(), f = fixture(t, { href: appLink(), read: (index, selected) => index === 1 ? durable.promise : selected }), running = f.start();
  await until(() => f.reads === 1); f.change(state('account-B', 'device-B')); durable.resolve(state()); await running;
  assert.equal(f.mounts[0].href, 'https://shell.example/#mine'); assert.equal(f.requests[0].context.expectedAccountId, 'account-B'); assert.deepEqual(f.follows, []); assert.deepEqual(f.navigationCalls, []);
});

test('normal owned application cannot auto-dispatch after a device change in the same account', async t => {
  const durable = deferred(), f = fixture(t, { href: appLink(), read: (index, selected) => index === 1 ? durable.promise : selected }), running = f.start();
  await until(() => f.reads === 1); f.change(state('account-A', 'device-new')); durable.resolve(state()); await running;
  assert.equal(f.mounts[0].href, 'https://shell.example/#mine'); assert.deepEqual(f.follows, []); assert.deepEqual(f.navigationCalls, []);
});

test('owned application scope stays cancelled after account A to B to A', async t => {
  const durable = deferred(), f = fixture(t, { href: appLink(), read: (index, selected) => index === 1 ? durable.promise : selected }), running = f.start();
  await until(() => f.reads === 1); f.change(state('account-B', 'device-B')); f.change(state()); durable.resolve(state()); await running;
  assert.equal(f.mounts[0].href, 'https://shell.example/#mine'); assert.deepEqual(f.follows, []);
});

test('cold owned application cannot auto-dispatch after a startup account change', async t => {
  const input = appLink(), durable = deferred(), intent = intents.parseConnectionIntent(input, 'https://shell.example');
  const f = fixture(t, { href: input, read: (index, selected) => index === 1 ? durable.promise : selected }), running = f.start(intent);
  await until(() => f.reads === 1); f.change(state('account-B', 'device-B')); durable.resolve(state()); await running;
  assert.equal(f.mounts[0].href, 'https://shell.example/#mine'); assert.deepEqual(f.follows, []); assert.deepEqual(f.navigationCalls, []);
  assert.equal(intent.launch.takeInvitation(), 'b'.repeat(64), 'cancelled scope did not redeem or consume the carried invitation');
});

test('world notes and messages routes stay available to the existing router', async t => {
  for (const route of ['#world', '#notes/note123', '#messages/group123']) {
    const href = 'https://shell.example/' + route, f = fixture(t, { href }); await f.start();
    assert.equal(f.mounts[0].href, href); assert.deepEqual(f.follows, []); assert.deepEqual(f.navigationCalls, []);
  }
});
test('a newer route selected during startup is preserved instead of the old owned application', async t => {
  const durable = deferred(), f = fixture(t, { href: appLink(), read: () => durable.promise }), running = f.start();
  await until(() => f.reads === 1); f.root.isConnected = true;
  // Back/Forward updates the trusted browser URL. The old typed intent no longer owns it.
  f.window.location.href = 'https://shell.example/#notes/note123'; durable.resolve(state()); await running;
  assert.equal(f.mounts[0].href, 'https://shell.example/#notes/note123'); assert.deepEqual(f.follows, []);
});

test('fatal durable startup before mount restores the same-scope original URL without consuming its invitation', async t => {
  const href = appLink(), expected = Error('synthetic durable failure'), f = fixture(t, { href, read: async () => { throw expected; } });
  await assert.rejects(f.start(), error => error === expected);
  assert.equal(f.window.location.href, href); assert.equal(f.mounts.length, 0);
  assert.equal(intents.parseConnectionIntent(f.window.location.href, 'https://shell.example').launch.takeInvitation(), 'b'.repeat(64));
});

test('profile and community deep links use one bound typed follow after neutral mount', async t => {
  for (const route of ['#profile/profile123', '#community/community123/chat']) {
    const f = fixture(t, { href: 'https://shell.example/' + route }); await f.start();
    assert.equal(f.mounts[0].href, 'https://shell.example/#mine'); assert.equal(f.follows.length, 1);
    assert.equal(f.follows[0].route, route); assert.deepEqual(f.navigationCalls, []);
  }
});

test('normal host PWA application entry uses the exact single pwa flag without weakening QR parsing', async t => {
  const href = appLink().replace('/#', '/?pwa=1#');
  assert.equal(intents.parseConnectionIntent(href, 'https://shell.example'), null, 'mixed QR input remains closed');
  const f = fixture(t, { href }); await f.start();
  assert.equal(f.mounts[0].href, 'https://shell.example/#mine'); assert.equal(f.follows.length, 1);
  assert.equal(f.follows[0].launch.takeInvitation(), 'b'.repeat(64));
});
test('normal host PWA profile and community entries keep their exact typed destination', async t => {
  for (const route of ['#profile/profile123', '#community/community123/chat']) {
    const href = 'https://shell.example/?pwa=1' + route, f = fixture(t, { href }); await f.start();
    assert.equal(f.mounts[0].href, 'https://shell.example/#mine'); assert.equal(f.follows.length, 1); assert.equal(f.follows[0].route, route);
  }
});
test('duplicate mixed and unknown queries with a recognized typed hash cannot fall through to the normal router', async t => {
  for (const query of ['?pwa=1&pwa=1', '?pwa=1&foreign=1', '?foreign=1', '?pwa=0', '?pwa=%31', '?connect=contact&card='+'a'.repeat(32)]) {
    const f = fixture(t, { href: appLink().replace('/#', '/' + query + '#') });
    await assert.rejects(f.start(), { code: 'connection_route_invalid' });
    assert.equal(f.mounts.length, 0); assert.equal(f.bootstraps, 0); assert.deepEqual(f.follows, []); assert.deepEqual(f.navigationCalls, []);
  }
});
test('a valid explicit cold target supersedes an invalid typed ambient URL without inheriting its invitation', async t => {
  const href = appLink().replace('/#', '/?foreign=1#'), selected = intents.parseConnectionIntent('#profile/profile456', 'https://shell.example');
  const f = fixture(t, { href }); await f.start(selected);
  assert.equal(f.mounts[0].href, 'https://shell.example/#mine'); assert.equal(f.follows.length, 1); assert.equal(f.follows[0], selected);
});

test('first shell mounts while optional voice metadata is pending and exposes the shared stage resolver', async t => {
  const cap = deferred(), f = fixture(t, { capabilities: () => cap.promise }), running = f.start();
  await until(() => f.fetches === 1); await turn();
  try {
    assert.equal(f.mounts.length, 1, 'optional voice metadata cannot block first World mount');
    assert.equal(f.requests[0].context.expectedAccountId, 'account-A'); assert.equal(f.fetches, 1);
    let settled = false; const reading = f.mounts[0].options.readNativeVoice().then(value => { settled = true; return value; });
    await turn(); assert.equal(settled, false, 'pending descriptor is not disabled');
    cap.resolve({ ok: true, async json() { return { nativeVoice: null }; } }); assert.equal(await reading, null);
  } finally { cap.resolve({ ok: false }); await running; }
});
test('pagehide after first shell closes a pending descriptor without reviving a late voice bridge', async t => {
  const cap = deferred(), f = fixture(t, { capabilities: () => cap.promise }), running = f.start();
  await until(() => f.fetches === 1); await turn();
  try {
    assert.equal(f.mounts.length, 1); const reading = f.mounts[0].options.readNativeVoice();
    const rejected = assert.rejects(reading, { code: 'native_voice_descriptor_unavailable' }); f.pagehide();
    cap.resolve({ ok: true, async json() { return { nativeVoice: null }; } }); await rejected;
    assert.equal(f.mounts.length, 1); assert.equal(f.mounts[0].destroyed, 1); assert.equal(f.observers.size, 0);
  } finally { cap.resolve({ ok: false }); await running; }
});
