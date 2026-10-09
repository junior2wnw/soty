import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { EventEmitter } from 'node:events';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { types } from 'node:util';
import { Server } from 'node:http';
import { createGonkaProxy, createApplicationTokenAuthenticator, defaultGonkaProxyModel } from '../gonka-proxy.js';

const startupUrl = new URL('../startup.js', import.meta.url);
const startup = await readFile(startupUrl, 'utf8');

test('actual reusable startup import owns no listener or process signals; missing explicit env rejects', async () => {
  const originalListen = Server.prototype.listen, signals = ['SIGTERM', 'SIGINT', 'disconnect'].map(name => process.listenerCount(name));
  let listens = 0;
  Server.prototype.listen = () => { listens++; throw new Error('physical listener forbidden'); };
  try {
    const imported = await import('../startup.js');
    assert.equal(listens, 0); assert.deepEqual(['SIGTERM', 'SIGINT', 'disconnect'].map(name => process.listenerCount(name)), signals);
    await assert.rejects(imported.createRootStartup(), { code: 'root_startup_configuration_invalid' });
    assert.equal(listens, 0);
  } finally { Server.prototype.listen = originalListen; }
});

test('explicit empty Root provider map prevents ambient primary/fallback/token files from activating actual proxies', async () => {
  const keys = ['SOTY_GONKA_API_KEY', 'SOTY_GONKA_FALLBACK_API_KEY', 'SOTY_GONKA_APPLICATION_TOKENS', 'SOTY_GONKA_APPLICATION_TOKENS_FILE', 'SOTY_GONKA_LOG_METRICS'];
  const before = keys.map(name => process.env[name]);
  for (const name of keys) process.env[name] = name.endsWith('_FILE') ? 'forbidden-private-file' : name.endsWith('METRICS') ? '1' : 'synthetic_ambient_value';
  try {
    const { rootGonkaOptions } = await import('../startup.js'), options = rootGonkaOptions({});
    assert.equal(options.apiKey, ''); assert.equal(options.fallbackApiKey, ''); assert.equal(options.applicationTokens, ''); assert.equal(options.applicationTokensFile, ''); assert.equal(options.onEvent, null);
    let fetches = 0;
    const proxy = createGonkaProxy({ store: {}, ...options, fetchImpl: async () => { fetches++; assert.fail('provider forbidden'); } });
    assert.equal(proxy.ready, false); assert.equal(fetches, 0);
    const authenticate = createApplicationTokenAuthenticator(options.applicationTokens, { filePath: options.applicationTokensFile });
    assert.equal(typeof authenticate, 'function'); assert.equal(fetches, 0);
    for (const name of ['timeoutMs', 'firstTokenTimeoutMs', 'idleTimeoutMs', 'streamTimeoutMs', 'providerConcurrency', 'maximumQueued', 'queueWaitMs', 'internalStream', 'providerStrategy', 'emptyToolFallback']) assert.notEqual(options[name], undefined);
  } finally { keys.forEach((name, index) => before[index] === undefined ? delete process.env[name] : process.env[name] = before[index]); }
});

function controlledStartup() {
  // Execute the actual factory body and helpers with controlled transport and
  // service ports; no production dependency-injection framework is added.
  const code = startup.replace(/^import .*?;\r?\n/gm, '').replaceAll('export function ', 'function ').replaceAll('export async function ', 'async function ').replaceAll('import.meta.url', JSON.stringify(startupUrl.href));
  const calls = [], state = { failure: null, installedClosed: 0, root: null, reentrant: null }, sockets = [];
  class SocketServer { constructor(options) { this.options = options; this.clients = new Set(); sockets.push(this); } close(callback) { calls.push('wss.close'); callback(); } handleUpgrade() { assert.fail('no live WS'); } emit() {} }
  const server = new EventEmitter();
  server.listen = () => { calls.push('listen'); server.emit('listening'); };
  server.close = () => calls.push('server.close'); server.closeAllConnections = () => calls.push('connections.close');
  const app = { locals: { appsService: { handleUpgrade: () => false }, humanCalls: { attachSignaling() { calls.push('human.attach'); } },
    async closeServices() { calls.push('services.close'); if (state.root) state.reentrant = state.root.close(); if (state.failure === 'services') throw new Error('not-public-detail'); } } };
  const values = { createServer: () => server, path, fileURLToPath, types, WebSocketServer: SocketServer,
    createHttpApp(_dist, options) { calls.push('http.create'); assert.equal(options.env.HOST, 'controlled.invalid'); assert.equal(options.gonka.apiKey, ''); return app; },
    waitForRejectedHttpStart: async () => {}, createRoomStore: () => ({ close() { calls.push('rooms.close'); if (state.failure === 'rooms') throw new Error('not-public-detail'); } }),
    attachRealtime() { calls.push('realtime.attach'); }, createTrafficTunnelProxy: options => ({ enabled: false, publicPath: options.publicPath }),
    hasSingleHostHeader: () => true, fenceClosingUpgrade: () => false,
    loadCapabilityConfiguration: () => ({}), loadUniversalConfiguration: () => ({}), loadPersonalAgentConfiguration: () => ({ personalAgent: { enabled: false } }),
    loadNativeVoiceConfiguration: value => { assert.equal(value, ''); return { nativeVoice: { enabled: false } }; },
    loadInstalledApplicationsEntry: async () => ({ httpOptions: {}, sourceReferences: [], async close() { calls.push('installed.close'); state.installedClosed++; } }),
    startUniversalOperator: async () => ({ async close() { calls.push('operator.close'); } }), connectAllowedOrigins: () => ['https://root.invalid'], defaultGonkaProxyModel };
  const create = new Function(...Object.keys(values), code + '\nreturn createRootStartup;')(...Object.values(values));
  return { create, server, state, calls, sockets };
}

test('actual factory controlled lifecycle retains realtime/human caps and starts only explicitly', async () => {
  const f = controlledStartup();
  const root = await f.create({ env: { HOST: 'controlled.invalid', PORT: '8080' } }); f.state.root = root;
  assert.equal(f.calls.includes('listen'), false); assert.equal(root.server.keepAliveTimeout, 65000); assert.equal(root.server.headersTimeout, 70000);
  assert.deepEqual(f.sockets.map(value => value.options.maxPayload), [34000000, 64000]); assert.ok(f.calls.includes('realtime.attach')); assert.ok(f.calls.includes('human.attach'));
  const starting = root.start(); assert.equal(root.start(), starting); await starting;
  assert.equal(f.calls.filter(value => value === 'listen').length, 1);
  const closing = root.close(); assert.equal(root.close(), closing); await closing; assert.equal(f.state.reentrant, closing);
  assert.equal(f.state.installedClosed, 1); await assert.rejects(root.start(), { code: 'root_startup_closed' });
});

test('actual factory close publishes its promise first and attempts all owned closers after one fails', async () => {
  const f = controlledStartup(); const root = await f.create({ env: { HOST: 'controlled.invalid' } }); f.state.root = root; f.state.failure = 'rooms';
  const closing = root.close(); await assert.rejects(closing, { code: 'root_startup_cleanup_failed' });
  assert.equal(f.state.reentrant, closing); assert.ok(f.calls.includes('services.close')); assert.ok(f.calls.includes('installed.close'));
  assert.equal(root.close(), closing);
});

test('an upgrade queued after close destroys its socket before any signaling dispatch', async () => {
  const f = controlledStartup(); const root = await f.create({ env: { HOST: 'controlled.invalid' } });
  const closing = root.close(); let destroyed = 0;
  f.server.emit('upgrade', { url: '/ws/human-call/queued', headers: {} }, { destroy() { destroyed++; } }, Buffer.alloc(0));
  assert.equal(destroyed, 1); await closing;
});

test('actual HTTP closeServices body attempts every service in order despite failure/reentry', async () => {
  const source = await readFile(new URL('../http-app.js', import.meta.url), 'utf8');
  const body = source.slice(source.indexOf('  let servicesCleanup;'), source.indexOf("  app.get('/api/apps/capabilities'"));
  const app = { locals: {} }, calls = []; let reentrant;
  const closer = name => ({ close() { calls.push(name); if (name === 'personal') { reentrant = app.locals.closeServices(); throw new Error('controlled private detail'); } } });
  const values = { app, nativeSources: { enabled: true, ...closer('native') }, personal: closer('personal'), voice: closer('voice'), humanCalls: closer('humanCalls'),
    nativeRecovery: closer('nativeRecovery'), oauthCleanup: closer('oauthCleanup'), unsubscribeRevocations: () => calls.push('revocations'), mcp: closer('mcp'),
    human: closer('human'), universal: closer('universal'), managedReviews: closer('managedReviews'), apps: closer('apps'), world: closer('world'), notes: closer('notes'),
    capabilities: closer('capabilities'), connect: closer('connect'), connectors: { store: closer('connectorStore') } };
  new Function(...Object.keys(values), body)(...Object.values(values));
  const pending = app.locals.closeServices(); await assert.rejects(pending, { code: 'root_services_cleanup_failed' }); assert.equal(reentrant, pending); assert.equal(app.locals.closeServices(), pending);
  assert.deepEqual(calls, ['native', 'personal', 'voice', 'humanCalls', 'nativeRecovery', 'oauthCleanup', 'revocations', 'mcp', 'human', 'universal', 'managedReviews', 'apps', 'world', 'notes', 'capabilities', 'connect', 'connectorStore']);
});

test('close rejects a pending start before cleanup completes and consumes a late listen error', async () => {
  const f = controlledStartup();
  f.server.listen = () => { f.calls.push('listen'); };
  const root = await f.create({ env: { HOST: 'controlled.invalid' } }); f.state.root = root;
  let finishServices, cleanupComplete = false;
  root.app.locals.closeServices = async () => {
    f.calls.push('services.close'); f.state.reentrant = root.close();
    await new Promise(done => { finishServices = done; });
  };
  const starting = root.start();
  assert.equal(root.start(), starting);
  const closing = root.close();
  void closing.then(() => { cleanupComplete = true; });
  try {
    // Deliberately leave the original start promise unobserved for one turn.
    // Internal observation must prevent an unhandled rejection, not resolve it.
    await new Promise(done => setImmediate(done));
    assert.equal(typeof finishServices, 'function'); assert.equal(cleanupComplete, false);
    const outcome = await Promise.race([
      starting.then(() => ({ acknowledged: true }), reason => ({ rejected: reason.code })),
      new Promise(done => setImmediate(() => done({ pending: true }))),
    ]);
    assert.deepEqual(outcome, { rejected: 'root_startup_closed' });
    f.server.emit('listening');
    assert.equal(f.server.listenerCount('listening'), 0);
    assert.equal(f.server.listenerCount('error'), 1);
    assert.doesNotThrow(() => f.server.emit('error', new Error('controlled queued listen error')));
    assert.equal(f.server.listenerCount('error'), 0);
    await assert.rejects(starting, { code: 'root_startup_closed' });
    await assert.rejects(root.start(), { code: 'root_startup_closed' });
    assert.equal(f.calls.filter(value => value === 'listen').length, 1);
    assert.equal(root.close(), closing); assert.equal(f.state.reentrant, closing);
  } finally { finishServices?.(); await closing; }
  assert.equal(cleanupComplete, true); assert.equal(f.state.installedClosed, 1);
});

test('ordinary wrapper preserves normal ready but cannot publish a ready after shutdown', async () => {
  const source = await readFile(new URL('../index.js', import.meta.url), 'utf8');
  const body = source.replace(/^import .*?;\r?\n/gm, '');
  const AsyncFunction = Object.getPrototypeOf(async () => {}).constructor;
  const fixture = pending => {
    const server = new EventEmitter(); server.address = () => ({ port: 12345 });
    let rejectStart, closeCalls = 0;
    const starting = pending ? new Promise((_, reject) => { rejectStart = reject; }) : Promise.resolve();
    const cleanup = Promise.resolve(), handlers = new Map(), ready = [], logs = [];
    const root = { server, trafficTunnel: { enabled: false },
      start() { if (!pending) server.emit('listening'); return starting; },
      close() { closeCalls++; rejectStart?.(Object.assign(new Error('root_startup_closed'), { code: 'root_startup_closed' })); return cleanup; } };
    const processPort = { env: {}, connected: false, send: value => ready.push(value), once: (name, callback) => handlers.set(name, callback) };
    const work = new AsyncFunction('createRootStartup', 'process', 'console', body)(async () => root, processPort, { log: value => logs.push(value) });
    void work.catch(() => {});
    return { server, handlers, ready, logs, work, closeCalls: () => closeCalls };
  };
  const normal = fixture(false); await normal.work;
  assert.deepEqual(normal.ready, [{ type: 'soty:ready', port: 12345 }]);
  assert.deepEqual(normal.logs, ['soty.online listening on 12345', 'traffic tunnel disabled']);
  assert.equal(normal.closeCalls(), 0);
  const retired = fixture(true); await new Promise(done => setImmediate(done));
  assert.equal(typeof retired.handlers.get('SIGINT'), 'function'); retired.handlers.get('SIGINT')();
  await assert.rejects(retired.work, { code: 'root_startup_closed' });
  retired.server.emit('listening');
  assert.equal(retired.ready.length, 0, 'a closed ordinary wrapper cannot send a late ready IPC');
  assert.equal(retired.logs.length, 0, 'a closed ordinary wrapper cannot log a late ready');
  assert.equal(retired.closeCalls(), 2);
});
