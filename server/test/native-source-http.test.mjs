import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, mkdir, writeFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { createHttpApp, waitForRejectedHttpStart } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';

function memory() {
  let value;
  return {
    async read() { return structuredClone(value ?? null); },
    async claim(next) { value ??= structuredClone(next); return structuredClone(value); },
    async compareAndSwap(revision, next) {
      assert.equal(value.localRevision, revision); value = structuredClone(next); return structuredClone(value);
    },
  };
}
const gate = () => { let release; const promise = new Promise(done => { release = done; }); return { promise, release }; };
const noStorage = async p => { await assert.rejects(stat(p), error => error.code === 'ENOENT'); };
async function directory(t, registerCleanup = true) {
  const folder = await mkdtemp(join(tmpdir(), 'native-source-http-'));
  const cleanup = async () => {
    assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    assert.match(basename(folder), /^native-source-http-/u);
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 20 });
  };
  if (registerCleanup) t.after(cleanup);
  const dist = join(folder, 'dist'), dataDir = join(folder, 'data');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<title>Synthetic Native mount host</title>');
  return { folder, dist, dataDir, cleanup };
}
const options = dataDir => ({ dataDir, appHosting: { domainProfile: 'separate-site' },
  namedAppZone: '', discoveryOrigin: '', appOriginTemplate: '', universalAppsEnabled: true });
async function fixture(t, extra = {}) {
  const files = await directory(t, false), sockets = new Set(), clients = [];
  let app, stopped = false;
  const server = createServer((req, res) => app(req, res));
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = 'http://127.0.0.1:' + server.address().port;
  const close = async () => { if (stopped) return; stopped = true; await app?.locals.closeServices(); };
  t.after(async () => { clients.forEach(client => client.dispose()); sockets.forEach(socket => socket.destroy()); await close(); await new Promise(done => server.close(done)); await files.cleanup(); });
  app = createHttpApp(files.dist, { ...options(files.dataDir), connectOrigins: [origin], ...extra });
  return { ...files, origin, app, close,
    async signed() {
      const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
        fetch: (url, init) => fetch(url, { ...init, headers: { ...init.headers, origin } }) }, memory());
      clients.push(client); const account = await client.bootstrap('Synthetic Native mount caller');
      return { client, account };
    },
  };
}

test('omitted and empty mount preserve ordinary static, health and real signed Connect POST', async t => {
  for (const extra of [{}, { nativeSourceHandlers: [] }]) {
    const f = await fixture(t, extra), health = await fetch(f.origin + '/health');
    assert.equal(health.status, 200); assert.equal((await health.json()).ok, true);
    assert.equal(await (await fetch(f.origin + '/')).text(), '<title>Synthetic Native mount host</title>');
    assert.ok((await f.signed()).account.accountId);
  }
});

test('private handler receives raw body before API parsers and true owns its HTTP response', async t => {
  let calls = 0, closes = 0;
  const f = await fixture(t, { nativeSourceHandlers: [{
    async handle(req, res) {
      if (req.url !== '/api/source/device/v1/installation') return false;
      calls++; assert.equal(req.body, undefined);
      const parts = []; for await (const part of req) parts.push(part);
      assert.equal(Buffer.concat(parts).toString(), '{"synthetic":true}');
      res.status(200).json({ mounted: true }); return true;
    }, close() { closes++; },
  }] });
  const response = await fetch(f.origin + '/api/source/device/v1/installation', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"synthetic":true}',
  });
  assert.equal(response.status, 200); assert.deepEqual(await response.json(), { mounted: true });
  assert.equal(response.headers.get('x-content-type-options'), 'nosniff');
  assert.equal(calls, 1); await f.close(); assert.equal(closes, 1);
});

test('false leaves current Root body parsers and signed Connect route available', async t => {
  let connectCalls = 0;
  const f = await fixture(t, { nativeSourceHandlers: [{ handle(req) {
    if (req.url === '/api/connect/rpc') { connectCalls++; assert.equal(req.body, undefined); }
    return false;
  }, close() {} }] });
  const { client, account } = await f.signed();
  const principal = await client.extension('access.principals.create', { expectedAccountId: account.accountId, label: 'Synthetic mount principal' });
  assert.ok(principal.principal.id); assert.ok(connectCalls >= 2);
  assert.equal((await fetch(f.origin + '/health')).status, 200);
});

test('handler throw, rejected proxy and invalid return are closed503 without diagnostics getters', async t => {
  let getters = 0, traps = 0;
  const opaque = Object.create(null); Object.defineProperty(opaque, 'message', { get() { getters++; throw Error('never print'); } });
  const proxy = new Proxy({}, { get() { traps++; throw Error('never print'); }, ownKeys() { traps++; throw Error('never print'); } });
  const f = await fixture(t, { nativeSourceHandlers: [{ async handle(req) {
    if (req.url === '/throw') throw opaque;
    if (req.url === '/reject') throw proxy;
    if (req.url === '/invalid') return { handled: true };
    return false;
  }, close() {} }] });
  for (const route of ['/throw', '/reject', '/invalid']) {
    const response = await fetch(f.origin + route);
    assert.equal(response.status, 503); assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.deepEqual(await response.json(), { error: { code: 'native_source_unavailable' } });
  }
  assert.equal(getters, 0); assert.equal(traps, 0);
});

test('array and record replacement cannot change captured request or shutdown functions', async t => {
  let originalClose = 0, replaced = 0;
  const record = { handle(req, res) { if (req.url !== '/captured') return false; res.json({ captured: true }); return true; }, close() { originalClose++; } };
  const list = [record], f = await fixture(t, { nativeSourceHandlers: list });
  record.handle = () => { replaced++; throw Error('replacement'); }; record.close = () => { replaced++; };
  list[0] = { handle: record.handle, close: record.close }; list.push(list[0]);
  assert.deepEqual(await (await fetch(f.origin + '/captured')).json(), { captured: true });
  await f.close(); assert.equal(originalClose, 1); assert.equal(replaced, 0);
});

test('invalid private CODE descriptors, getters, proxies and duplicates reject before storage or callbacks', async t => {
  const files = await directory(t); let getters = 0, traps = 0, callbacks = 0;
  const handler = () => { callbacks++; return false; }, close = () => { callbacks++; };
  const getter = {}; Object.defineProperty(getter, 'handle', { enumerable: true, get() { getters++; return handler; } }); getter.close = close;
  const proxy = new Proxy({}, { get() { traps++; throw Error('trap'); }, getPrototypeOf() { traps++; throw Error('trap'); } });
  const revoked = Proxy.revocable([], {}); revoked.revoke();
  const functionProxy = new Proxy(handler, { apply() { traps++; } });
  const rows = [null, 'json', {}, [null], [{ handle: handler }], [getter], proxy, revoked.proxy,
    [{ handle: functionProxy, close }], [{ handle: handler, close, approve: handler }],
    [{ handle: handler, close }, { handle: handler, close }], Array(1), Array(65).fill({ handle: handler, close })];
  const base = options(files.dataDir), configs = rows.map(nativeSourceHandlers => ({ ...base, nativeSourceHandlers }));
  const accessor = { ...base }; Object.defineProperty(accessor, 'nativeSourceHandlers', { get() { getters++; return []; } }); configs.push(accessor, proxy);
  for (const config of configs) assert.throws(() => createHttpApp(files.dist, config), error => error.code === 'native_source_configuration_invalid');
  await noStorage(files.dataDir); assert.equal(callbacks, 0); assert.equal(getters, 0); assert.equal(traps, 0);
});

test('accepted callbacks close once on early startup rejection and rejected-start waiter retains async cleanup', async t => {
  const files = await directory(t), pending = gate(); let closes = 0, finished = false, caught;
  try { createHttpApp(files.dist, { ...options(files.dataDir), universalAppsEnabled: 'invalid', nativeSourceHandlers: [{ handle: () => false, close() { closes++; return pending.promise; } }] }); }
  catch (error) { caught = error; }
  assert.equal(caught.code, 'universal_configuration_invalid'); assert.equal(closes, 1);
  const waiting = waitForRejectedHttpStart(caught).then(() => { finished = true; });
  await Promise.resolve(); assert.equal(finished, false); await noStorage(files.dataDir);
  pending.release(); await waiting; await waitForRejectedHttpStart(caught); assert.equal(closes, 1);
});

test('storage startup failure closes only admitted callbacks, retaining original failure', async t => {
  const files = await directory(t); await writeFile(files.dataDir, 'owned synthetic non-directory');
  let closes = 0, caught;
  try { createHttpApp(files.dist, { ...options(files.dataDir), nativeSourceHandlers: [{ handle: () => false, close() { closes++; throw Error('ignored cleanup failure'); } }] }); }
  catch (error) { caught = error; }
  assert.ok(caught); await waitForRejectedHttpStart(caught); assert.equal(closes, 1);
});

test('normal close fences pending requests and does not close another host; identical close refs run once', async t => {
  const entered = gate(), pending = gate(); let closes = 0;
  const close = () => { closes++; };
  const f = await fixture(t, { nativeSourceHandlers: [
    { async handle(req) { if (req.url === '/pending') { entered.release(); await pending.promise; } return false; }, close },
    { handle: () => false, close },
  ] }), other = await fixture(t);
  const response = fetch(f.origin + '/pending'); await entered.promise; await f.close(); pending.release();
  const denied = await response; assert.equal(denied.status, 503); assert.deepEqual(await denied.json(), { error: { code: 'native_source_unavailable' } });
  assert.equal(closes, 1); assert.equal((await fetch(other.origin + '/health')).status, 200);
  assert.equal((await fetch(f.origin + '/health')).status, 503);
});
