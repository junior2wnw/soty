import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFile } from 'node:fs/promises';
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { renderBootPage } from '../../modules/apps/server/runtime-pages.mjs';
import { assertApps, textId, requestPath, runtimePath } from '../../modules/apps/server/protocol.mjs';
import { createLaunchPath } from '../../modules/apps/server/launch-path.mjs';
import { synchronous } from '../../modules/apps/server/engagement-transaction.mjs';
import { parseWidgetRuntimeHost, widgetRuntimeOrigin, captureWidgetRuntimeScope, widgetRuntimeScopeDigest } from '../../modules/apps/server/runtime-hosts.mjs';
import { selectedRuntimeProfile } from '../../modules/apps/server/schema.mjs';
import { isRootSignedWorkspaceSource } from '../../modules/source-app/server/standard-profile.mjs';
import { HOST_SESSION_SCHEMA } from '../../modules/apps/server/host-session-continuity.mjs';
import { createAppLauncher } from './app-launch.mjs';
import { createAppBootRecovery } from './app-boot-recovery.mjs';

// Exercise the actual launch/session branches, generated boot script and parent
// adapters together. Authority, DB selector, cookie jar and DOM are ports; this
// does not qualify Connect credentials, a browser cookie policy or live HIVE.
const source = await readFile(new URL('../../modules/apps/server/index.mjs', import.meta.url), 'utf8');
function fragment(first, last) {
  const start = source.indexOf(first), end = source.indexOf(last, start + first.length);
  assert.ok(start >= 0 && end > start, 'actual source branch must exist');
  return source.slice(start, end);
}
const guidePathSource = fragment('  const guidePath =', '  const origins =');
const sessionSource = fragment('  const frameRequests=', '  function setPolicy(');
const routeSource = fragment('  async function routeApp(', '  function handleRequest(');
const launchSource = fragment("    if (op === 'apps.launch') {", "    if (op === 'apps.revoke') {");
const defaultEntrySource = fragment('  function defaultEntryFor(', '  function inspectSource(');
const accessSource = fragment('  function checkAccess(', '  function checkStream(');
const widgetRuntimeSource = fragment('  function rootWidgetId(', '  /** Host-only invalidation:');
const hostSessionSource = fragment('  function hostScope(', '  function execute(');
const appId = 'app-' + 'a'.repeat(32), domainId = 'dom_' + 'b'.repeat(32);
const origin = 'https://hive.fixture.invalid', shellOrigin = 'https://soty.fixture.invalid';
const actor = Object.freeze({ accountId: 'synthetic-account', deviceId: 'synthetic-device' });
const digest = value => createHash('sha256').update(value).digest('hex');
const secret = () => randomBytes(32).toString('base64url');
const equalDigest = (a, b) => typeof a === 'string' && typeof b === 'string'
  && /^[a-f0-9]{64}$/u.test(a) && /^[a-f0-9]{64}$/u.test(b)
  && timingSafeEqual(Buffer.from(a, 'hex'), Buffer.from(b, 'hex'));
const turn = () => new Promise(resolve => setImmediate(resolve));
async function settle() { for (let n = 0; n < 12; n++) await turn(); }

function server() {
  const app = { id: appId, owner_account_id: actor.accountId, state: 'enabled', revision: 999, appHost: { domainId, origin } };
  const domain = { id: domainId, origin }, tickets = new Map(), sessions = new Map(), decisions = new WeakSet();
  const state = { now: 200000, active: true, policyEpoch: 7, targetRevision: 11, targetDigest: 'c'.repeat(64),
    profile: 'soty.relay-restricted.v1', floor: 2, launches: [], requests: [] };
  const live = decision => {
    assertApps(decisions.has(decision) && state.active, 'app_access_revoked', 403);
    assertApps(decision.policyEpoch === state.policyEpoch && decision.targetRevision === state.targetRevision
      && decision.targetDigest === state.targetDigest, 'app_source_changed', 403);
    return decision;
  };
  const environment = {
    assertApps, textId, requestPath, runtimePath, createLaunchPath, selectedRuntimeProfile, isRootSignedWorkspaceSource, synchronous,
    parseWidgetRuntimeHost, widgetRuntimeOrigin, captureWidgetRuntimeScope, widgetRuntimeScopeDigest,
    URL, URLSearchParams, Buffer, tickets, sessions, bootCandidates: new Map(), hostSessions: null, HOST_SESSION_SCHEMA,
    digest, equalDigest, secret, id: appId, app, actor, cookieName: 'soty_app_session', accountSessionMs: 3600000,
    channels: new Map([['synthetic-connector', {}]]), now: () => state.now,
    // The existing canonical main service has no widget authority ports. Keep
    // the actual guard and its dependencies, rather than bypassing its call.
    checkWidgetLaunch: undefined, checkWidgetRuntime: undefined, resolveWidgetRuntime: undefined,
    activeTarget: () => ({ profile: state.profile }),
    db: { prepare: () => ({ get: (id, requested) => id === appId && (requested === undefined || requested === domainId) ? domain : null }) },
    exact(args, allowed) { assertApps(Object.keys(args).every(key => allowed.includes(key)), 'invalid_arguments'); },
    setPolicy() {}, assertOrigin(req, expected) { assertApps(req.headers.origin === expected, 'invalid_origin', 403); },
    headerCount: (req, key) => req.rawHeaders.filter((value, i) => i % 2 === 0 && value.toLowerCase() === key).length,
    readBounded: async req => Buffer.from(req.body || ''),
    publications: {
      decideAccess() {
        assertApps(state.active, 'app_access_revoked', 403);
        const decision = Object.freeze({ appId, domainId, origin, actor, subject: 'account', accessBasis: 'account',
          policyEpoch: state.policyEpoch, targetRevision: state.targetRevision, targetDigest: state.targetDigest,
          profile: state.profile, requiredBindingVersion: state.floor, expiresAt: state.now + 30000,
          route: Object.freeze({ connectorKey: 'synthetic-connector', entryPath: '/' }) });
        decisions.add(decision); return decision;
      },
      recheckAccess: live,
    },
    assertRuntimeBinding: live,
    json(res, status, value) { res.status = status; res.value = value; },
  };
  vm.runInNewContext(defaultEntrySource + '\n' + guidePathSource + '\n' + accessSource + '\n' + widgetRuntimeSource + '\n' + hostSessionSource + '\n' + sessionSource + '\n' + routeSource + '\nglobalThis.route=routeApp;\nglobalThis.canonicalOrigin=canonicalHostOrigin;\n'
    + 'globalThis.launch=function(args){const op="apps.launch";\n' + launchSource + '\n};', environment, { timeout: 1000 });
  function launch(args) {
    state.launches.push(structuredClone(args));
    assert.equal(args.expectedAccountId, actor.accountId);
    const { expectedAccountId: ignored, ...operationArgs } = args;
    const result = environment.launch(operationArgs);
    return structuredClone({ url: result.launchUrl, entry: result.entry, launchBinding: result.launchBinding });
  }
  async function request(method, { body, cookie, check } = {}) {
    state.requests.push(method);
    const headers = { origin, ...(cookie ? { cookie } : {}), ...(check ? { 'x-soty-boot-check': check } : {}) };
    const rawHeaders = ['Origin', origin]; if (cookie) rawHeaders.push('Cookie', cookie);
    if (check) rawHeaders.push('X-Soty-Boot-Check', check);
    const req = { url: '/_soty/session', method, headers, rawHeaders, body };
    const res = { status: 0, value: null, headers: new Map(), setHeader(key, value) { this.headers.set(key.toLowerCase(), value); } };
    try { await environment.route(req, res, app); }
    catch (error) { res.status = error.status || 400; res.value = { ok: false, error: error.code || 'fixture_invalid' }; }
    return res;
  }
  function fetchFor(jar) {
    return async (_url, options) => {
      const response = await request(options.method, { body: options.body, cookie: jar.value, check: options.headers['X-Soty-Boot-Check'] });
      const cookie = response.headers.get('set-cookie'); if (typeof cookie === 'string') jar.value = cookie.split(';')[0];
      return { ok: response.status === 200, status: response.status, json: async () => response.value };
    };
  }
  return { app, environment, state, tickets, sessions, launch, request, fetchFor };
}

function screen(f, jar, { legacy = false, beforeRetry = () => {} } = {}) {
  const listeners = new Map(), timers = new Map(); let timerId = 0;
  const state = { current: true, frame: null, frames: [], failures: 0, hints: 0, errors: [] };
  const view = { crypto, btoa, location: { origin: shellOrigin },
    addEventListener(type, fn) { if (!listeners.has(type)) listeners.set(type, new Set()); listeners.get(type).add(fn); },
    removeEventListener(type, fn) { listeners.get(type)?.delete(fn); },
    setTimeout(fn, delay) { const id = ++timerId; timers.set(id, { fn, delay }); return id; }, clearTimeout(id) { timers.delete(id); } };
  const emit = event => { for (const fn of [...listeners.get('message') || []]) fn(event); };
  const launcher = createAppLauncher({ target: { appId }, accountId: actor.accountId, shellUrl: shellOrigin,
    isCurrent: () => state.current, request: async args => {
      if (f.state.launches.length >= 2) await beforeRetry(state);
      // This explicit custom request port exercises the supported legacy boot
      // path and its canonical-cookie collision. The normal Root adapter keeps
      // the opt-in and requires the host DTO; its current protocol is covered
      // by the signed SDK/host-session integration tests, not this legacy port.
      assert.equal(args.hostSession, true);
      const { hostSession: _hostSession, ...legacyArgs } = args;
      const result = f.launch(Object.freeze(legacyArgs)); if (legacy) delete result.launchBinding; return result;
    } });
  const recovery = createAppBootRecovery({ view, appId, getFrame: () => state.frame, isCurrent: () => state.current,
    recover: current => open(true, current), onFailure: () => { state.failures++; } });
  function mount(url) {
    const frameListeners = new Set(), childListeners = new Map(), elements = new Map(), navigations = [];
    const element = name => {
      if (!elements.has(name)) elements.set(name, { textContent: '', hidden: true, disabled: false, dataset: {},
        setAttribute() {}, removeAttribute() {}, addEventListener() {} });
      return elements.get(name);
    };
    const frame = { src: url, navigations, element,
      addEventListener(_type, fn) { frameListeners.add(fn); }, removeEventListener(_type, fn) { frameListeners.delete(fn); } };
    const parent = { postMessage(data, target) {
      assert.equal(target, shellOrigin); state.hints++;
      emit({ data: structuredClone(data), origin, source: frame.contentWindow, ports: [] });
    } };
    frame.contentWindow = { postMessage(data, target) {
      assert.equal(target, origin);
      for (const fn of [...childListeners.get('message') || []]) fn({ data: structuredClone(data), origin: shellOrigin, source: parent, ports: [] });
    } };
    const window = { parent, addEventListener(type, fn) {
      if (!childListeners.has(type)) childListeners.set(type, new Set()); childListeners.get(type).add(fn);
    } }; window.self = window; window.top = {};
    const address = new URL(url), location = { hash: address.hash, pathname: address.pathname, search: address.search,
      replace(value) { navigations.push(value); } };
    const markup = renderBootPage({ shellUrl: shellOrigin + '/#world' });
    const scripts = [...markup.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gu)]; assert.equal(scripts.length, 1);
    state.frame = frame; state.frames.push(frame);
    recovery.bind(frame, { origin, binding: launcher.binding() });
    vm.runInNewContext(scripts[0][1], { window, location, document: { title: '', body: element('body'), getElementById: element },
      history: { replaceState() { location.hash = ''; } }, URL, AbortController, performance,
      setTimeout: view.setTimeout, clearTimeout: view.clearTimeout, fetch: f.fetchFor(jar) }, { timeout: 1000 });
    for (const fn of [...frameListeners]) fn();
    return frame;
  }
  async function open(automatic = false, stillCurrent = () => true) {
    if (!state.current || !stillCurrent()) return;
    try { const url = await launcher.launch({ requireSameBinding: automatic }); if (url && state.current && stillCurrent()) mount(url); }
    catch (error) { if (state.current && stillCurrent()) { state.errors.push(error.code); state.failures++; } }
  }
  return { state, launcher, recovery, open, dispose() { state.current = false; recovery.dispose(); launcher.dispose(); } };
}

test('actual runtime guard keeps canonical main compatibility and rejects current widget or host authority violations', () => {
  const f = server(); f.launch({ appId, expectedAccountId: actor.accountId });
  const decision = [...f.tickets.values()][0].decision;
  assert.equal(f.environment.checkAccess({ decision }), decision);
  assert.equal(f.environment.canonicalOrigin(f.app), origin);
  assert.equal(f.environment.checkHolderHost({ decision }, f.app), undefined);
  const widgetId = 'widget-11111111-1111-4111-8111-111111111111';
  const holder = { decision, rootInstanceId: widgetId, entryPath: '/' }, calls = [];
  f.environment.checkWidgetRuntime = input => { calls.push(input); return false; };
  assert.throws(() => f.environment.checkAccess(holder), error => error.code === 'apps_widget_runtime_denied');
  assert.equal(calls.length, 1);
  const scope = calls[0];
  assert.equal(scope.widgetId, widgetId); assert.equal(scope.appId, appId); assert.equal(scope.phase, 'request');
  assert.deepEqual({ ...scope.actor }, actor);
  assert.deepEqual({ ...scope.entry }, { domainId, path: '/' });
  assert.deepEqual({ ...scope.target }, { revision: 11, digest: 'c'.repeat(64), profile: 'soty.relay-restricted.v1' });
  for (const value of [scope, scope.actor, scope.entry, scope.target]) assert.equal(Object.isFrozen(value), true);
  f.environment.checkWidgetRuntime = () => Promise.resolve(true);
  assert.throws(() => f.environment.checkAccess(holder), error => error.code === 'apps_async_authority');
  f.environment.checkWidgetRuntime = () => true;
  assert.throws(() => f.environment.checkWidgetRuntimeFor({ ...holder, decision: { ...decision, subject: 'public' } }, 'session'),
    error => error.code === 'apps_widget_runtime_denied');
  const wrongHost = { origin, scope: { widgetId: 'widget-22222222-2222-4222-8222-222222222222' } };
  assert.throws(() => f.environment.checkWidgetRuntimeFor({ ...holder, runtimeHost: wrongHost }, 'session'),
    error => error.code === 'app_widget_runtime_changed');
  assert.throws(() => f.environment.checkHolderHost({ ...holder, runtimeHost: wrongHost }, f.app),
    error => error.code === 'app_widget_runtime_changed');
  f.state.active = false;
  assert.throws(() => f.environment.checkAccess(holder), error => error.code === 'app_access_revoked');
});

test('actual launch branch projects only the original branded decision, not app revision or caller selectors', () => {
  const f = server(), result = f.launch({ appId, expectedAccountId: actor.accountId });
  assert.deepEqual(result.launchBinding, { schema: 'soty.app-launch-binding.v1', policyEpoch: 7, targetRevision: 11,
    targetDigest: 'c'.repeat(64), profile: 'soty.relay-restricted.v1', bindingFloor: 2 });
  assert.equal(result.entry.appId, appId); assert.equal(result.entry.domainId, domainId);
  result.launchBinding.policyEpoch = 999;
  assert.equal([...f.tickets.values()][0].decision.policyEpoch, 7);
  assert.throws(() => f.launch({ appId, expectedAccountId: actor.accountId, launchBinding: result.launchBinding }), /invalid_arguments/u);
  assert.equal(f.tickets.size, 1);
});

test('two same-device boots reproduce cookie overwrite and one fresh signed launch recovers only the failed frame', async () => {
  const f = server(), jar = {}, desktop = screen(f, jar), mobile = screen(f, jar);
  await Promise.all([desktop.open(), mobile.open()]); await settle();
  assert.equal(desktop.state.frames[0].element('status-title').textContent, 'Вход не подтвердился');
  assert.equal(desktop.state.frames.length, 2); assert.deepEqual(desktop.state.frame.navigations, ['/']);
  assert.equal(mobile.state.frames.length, 1); assert.deepEqual(mobile.state.frame.navigations, ['/']);
  assert.equal(desktop.state.hints, 1); assert.equal(mobile.state.hints, 0); assert.equal(desktop.state.failures, 0);
  assert.equal(f.state.launches.length, 3); assert.deepEqual(Object.keys(f.state.launches[2]).sort(), ['appId', 'domainId', 'expectedAccountId', 'path']);
  assert.deepEqual(f.state.requests, ['POST', 'POST', 'GET', 'GET', 'POST', 'GET']);
  assert.equal(f.tickets.size, 0); assert.equal(f.sessions.size, 3);
  assert.equal(desktop.recovery.attempted(), true); assert.equal(mobile.recovery.attempted(), false);
  desktop.dispose(); mobile.dispose();
});

test('a legacy server response preserves the original manual failure rather than guessing a retry provenance', async () => {
  const f = server(), jar = {}, desktop = screen(f, jar, { legacy: true }), mobile = screen(f, jar, { legacy: true });
  await Promise.all([desktop.open(), mobile.open()]); await settle();
  assert.equal(desktop.state.frame.element('status-title').textContent, 'Вход не подтвердился');
  assert.deepEqual(mobile.state.frame.navigations, ['/']); assert.equal(f.state.launches.length, 2);
  assert.equal(desktop.state.hints, 0); assert.equal(desktop.recovery.attempted(), false);
  desktop.dispose(); mobile.dispose();
});

test('source epoch changed after the hint cannot replace the original iframe even with a fresh ticket for the same app', async () => {
  const f = server(), jar = {}, desktop = screen(f, jar, { beforeRetry: () => { f.state.policyEpoch++; } }), mobile = screen(f, jar);
  await Promise.all([desktop.open(), mobile.open()]); await settle();
  assert.equal(desktop.state.frames.length, 1); assert.equal(desktop.state.failures, 1);
  assert.deepEqual(desktop.state.errors, ['app_launch_source_changed']); assert.equal(f.sessions.size, 2);
  assert.equal(f.tickets.size, 1, 'the unused new ticket is not consumed or delivered to an iframe');
  assert.equal(desktop.launcher.binding().policyEpoch, 7); assert.equal(desktop.recovery.attempted(), true);
  desktop.dispose(); mobile.dispose();
});

test('live authority revoked after the hint denies fresh signed admission without replacing the frame', async () => {
  const f = server(), jar = {}, desktop = screen(f, jar, { beforeRetry: () => { f.state.active = false; } }), mobile = screen(f, jar);
  await Promise.all([desktop.open(), mobile.open()]); await settle();
  assert.equal(desktop.state.frames.length, 1); assert.deepEqual(desktop.state.errors, ['app_access_revoked']);
  assert.equal(desktop.state.failures, 1); assert.equal(f.sessions.size, 2); assert.equal(f.tickets.size, 0);
  desktop.dispose(); mobile.dispose();
});

test('Back during an in-flight fresh signed launch suppresses the late response and does not mount a recovered frame', async () => {
  let release, entered = false;
  const f = server(), jar = {}, desktop = screen(f, jar, { beforeRetry: async () => { entered = true; await new Promise(resolve => { release = resolve; }); } }), mobile = screen(f, jar);
  await Promise.all([desktop.open(), mobile.open()]); await settle(); assert.equal(entered, true);
  desktop.dispose(); release(); await settle(); assert.equal(desktop.state.frames.length, 1);
  assert.equal(desktop.state.failures, 0); assert.equal(desktop.launcher.binding(), null); assert.equal(f.sessions.size, 2);
  mobile.dispose();
});
