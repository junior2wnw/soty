import assert from 'node:assert/strict';
import test from 'node:test';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import { MessageChannel } from 'node:worker_threads';

const source = readFileSync(new URL('../../public/sw.js', import.meta.url), 'utf8');
function worker({ clients = [], latest = clients, present = true } = {}) {
  const handlers = new Map(), records = { activated: 0, committed: [], cache: new Set() };
  const cache = { match: async url => present && records.cache.has(typeof url === 'string' ? url : new URL(url.url).pathname) ? new Response('cached shell') : undefined,
    addAll: async urls => urls.forEach(url => records.cache.add(url)), put: async () => {} };
  let reads = 0;
  vm.runInNewContext(source.replace('const buildAssets = [];', 'const buildAssets = ["/assets/entry.js", "/assets/typeface.woff2"];'), {
    URL, Response, MessageChannel, setTimeout, clearTimeout, fetch: async () => { throw new Error('offline'); },
    caches: { open: async () => cache, match: cache.match, keys: async () => ['soty-online-v20'], delete: async () => true },
    self: { location: { origin: 'https://soty.test' }, addEventListener: (name, handler) => handlers.set(name, handler),
      skipWaiting: async () => { records.activated++; }, clients: { claim: async () => {}, matchAll: async () => ++reads > 1 ? latest : clients } },
  });
  const emit = async (type, data) => {
    let pending = Promise.resolve(), response;
    handlers.get(type)({ data, ports: [{ postMessage: value => { response = value; } }], waitUntil: value => { pending = value; } });
    await pending; return response;
  };
  return { records, handlers, emit };
}
function client(id, ready, committed) { return { id, url: 'https://soty.test/', postMessage(message, ports) {
  if (message.type === 'SOTY_PREPARE_UPDATE') { ports[0].postMessage({ ready }); ports[0].close(); }
  if (message.type === 'SOTY_UPDATE_COMMIT') committed.push(id);
} }; }

test('offline readiness reflects every installed shell and module asset', async () => {
  const runtime = worker();
  assert.equal((await runtime.emit('message', { type: 'SOTY_OFFLINE_STATUS' })).offlineReady, false);
  await runtime.emit('install');
  assert.equal((await runtime.emit('message', { type: 'SOTY_OFFLINE_STATUS' })).offlineReady, true);
  runtime.records.cache.delete('/assets/entry.js');
  assert.equal((await runtime.emit('message', { type: 'SOTY_OFFLINE_STATUS' })).offlineReady, false);
  runtime.records.cache.add('/assets/entry.js');
  runtime.records.cache.delete('/assets/typeface.woff2');
  assert.equal((await runtime.emit('message', { type: 'SOTY_OFFLINE_STATUS' })).offlineReady, false);
});

test('an unsaved draft in either tab blocks update for every tab', async () => {
  const committed = [], runtime = worker({ clients: [client('a', true, committed), client('b', false, committed)] });
  assert.equal((await runtime.emit('message', { type: 'SOTY_ACTIVATE_UPDATE' })).ready, false);
  assert.equal(runtime.records.activated, 0); assert.deepEqual(committed, []);
});

test('update activates and arms reload only after every tab has saved', async () => {
  const committed = [], runtime = worker({ clients: [client('a', true, committed), client('b', true, committed)] });
  assert.equal((await runtime.emit('message', { type: 'SOTY_ACTIVATE_UPDATE' })).ready, true);
  assert.equal(runtime.records.activated, 1); assert.deepEqual(committed, ['a', 'b']);
});

test('a tab opened during preparation must participate before activation', async () => {
  const committed = [], a = client('a', true, committed), b = client('b', true, committed);
  const runtime = worker({ clients: [a], latest: [a, b] });
  assert.equal((await runtime.emit('message', { type: 'SOTY_ACTIVATE_UPDATE' })).ready, false);
  assert.equal(runtime.records.activated, 0); assert.deepEqual(committed, []);
});

test('navigation uses a real cached shell when the server is unreachable; API is never cached', async () => {
  const runtime = worker(); await runtime.emit('install');
  let response;
  runtime.handlers.get('fetch')({ request: { method: 'GET', mode: 'navigate', url: 'https://soty.test/?pwa=1' }, respondWith: promise => { response = promise; } });
  assert.equal(await (await response).text(), 'cached shell');
  runtime.handlers.get('fetch')({ request: { method: 'GET', mode: 'cors', url: 'https://soty.test/api/account' }, respondWith: promise => { response = promise; } });
  await assert.rejects(response, /offline/);
});
