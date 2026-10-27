import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createRelayIdentityFetch } from '../browser/relay-identity-fetch.mjs';

function fixture() {
  const listeners = new Set(), posted = [], calls = [], parent = { postMessage(value, origin) { posted.push({ value, origin }); } };
  const view = { parent, crypto: { randomUUID }, setTimeout, clearTimeout,
    addEventListener(_type, fn) { listeners.add(fn); }, removeEventListener(_type, fn) { listeners.delete(fn); } };
  const fetcher = async (path, options) => { calls.push({ path, options }); return new Response('ok'); };
  const client = createRelayIdentityFetch({ view, parentOrigin: 'https://shell.example', origin: 'https://app.example', fetch: fetcher, timeoutMs: 100 });
  const emit = (index, { source = parent, origin = 'https://shell.example', grant = 'A'.repeat(43) } = {}) => {
    const data = { schema: 'soty.relay-window-identity.v1', type: 'grant', requestId: posted[index].value.requestId, grant };
    for (const listener of [...listeners]) listener({ source, origin, data });
  };
  return { client, view, parent, posted, calls, emit };
}

test('only the exact parent origin/source supplies the grant; caller cannot choose it', async () => {
  const f = fixture(), pending = f.client('/api', { headers: { 'x-soty-window-grant': 'caller-forgery' } });
  await new Promise(done => setImmediate(done));
  assert.equal(f.posted.length, 1); assert.equal(f.posted[0].origin, 'https://shell.example');
  f.emit(0, { source: {} }); f.emit(0, { origin: 'https://other.example' }); assert.equal(f.calls.length, 0);
  f.emit(0); await pending; assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].options.headers.get('x-soty-window-grant'), 'A'.repeat(43));
  assert.equal(f.calls[0].options.headers.get('x-soty-identity-request'), '1');
});

test('standalone, missing parent origin, timeout and abort never send an application request', async () => {
  const f = fixture(), standalone = { ...f.view }; standalone.parent = standalone;
  const noParent = createRelayIdentityFetch({ view: standalone, parentOrigin: 'https://shell.example', origin: 'https://app.example', fetch: () => { throw Error('should not fetch'); } });
  await assert.rejects(noParent('/api'), { code: 'soty_identity_window_unavailable' });
  const noOrigin = createRelayIdentityFetch({ view: f.view, origin: 'https://app.example', fetch: () => { throw Error('should not fetch'); } });
  await assert.rejects(noOrigin('/api'), { code: 'soty_identity_window_unavailable' });
  const controller = new AbortController(), pending = f.client('/api', { signal: controller.signal }); controller.abort(Error('cancelled'));
  await assert.rejects(pending, /cancelled/u); await assert.rejects(f.client('/api'), { code: 'soty_identity_window_unavailable' });
  assert.equal(f.calls.length, 0);
});

test('concurrent requests are correlated and receive only their own response', async () => {
  const f = fixture(), a = f.client('/a'), b = f.client('/b'); await new Promise(done => setImmediate(done)); f.emit(1, { grant: 'B'.repeat(43) }); f.emit(0); await Promise.all([a, b]);
  assert.equal(f.calls.find(value => value.path === '/a').options.headers.get('x-soty-window-grant'), 'A'.repeat(43));
  assert.equal(f.calls.find(value => value.path === '/b').options.headers.get('x-soty-window-grant'), 'B'.repeat(43));
});

test('Root supplies exact trusted parent origins without ancestorOrigins or wildcard', async () => {
  const f = fixture(); let configCalls = 0, appCalls = 0;
  const client = createRelayIdentityFetch({ view: f.view, origin: 'https://app.example', timeoutMs: 500,
    fetch: async path => { if (path === '/_soty/relay-identity-config') { configCalls++;
      return Response.json({ schema: 'soty.relay-identity-config.v1', shellOrigins: ['https://shell.example', 'https://other-shell.example'] }); }
      appCalls++; return new Response('ok'); } });
  const first = client('/a'), second = client('/b');
  await new Promise(done => setTimeout(done, 0));
  assert.equal(configCalls, 1); assert.equal(f.posted.length, 4);
  assert.deepEqual([...new Set(f.posted.map(item => item.origin))], ['https://shell.example', 'https://other-shell.example']);
  f.emit(0, { origin: 'https://untrusted.example' }); assert.equal(appCalls, 0);
  f.emit(0); f.emit(2); await Promise.all([first, second]); assert.equal(appCalls, 2);
});

test('missing, malformed, or untrusted Root config fails before messaging or app request', async () => {
  for (const body of [null, { schema: 'wrong', shellOrigins: ['https://shell.example'] },
    { schema: 'soty.relay-identity-config.v1', shellOrigins: ['*'] },
    { schema: 'soty.relay-identity-config.v1', shellOrigins: ['https://app.example'] }]) {
    const f = fixture(); let calls = 0;
    const client = createRelayIdentityFetch({ view: f.view, origin: 'https://app.example', timeoutMs: 100,
      fetch: async path => { calls++; assert.equal(path, '/_soty/relay-identity-config');
        return body ? Response.json(body) : new Response('', { status: 404 }); } });
    await assert.rejects(client('/api'), { code: 'soty_identity_window_unavailable' });
    assert.equal(calls, 1); assert.equal(f.posted.length, 0);
  }
});

test('a stalled Root config is bounded and a later user action can request it again', async () => {
  const f = fixture(); let calls = 0;
  const client = createRelayIdentityFetch({ view: f.view, origin: 'https://app.example', timeoutMs: 100,
    fetch: async path => { assert.equal(path, '/_soty/relay-identity-config'); calls++; return new Promise(() => {}); } });
  await assert.rejects(client('/api'), { code: 'soty_identity_window_unavailable' });
  await new Promise(done => setImmediate(done));
  await assert.rejects(client('/api'), { code: 'soty_identity_window_unavailable' });
  assert.equal(calls, 2); assert.equal(f.posted.length, 0);
});

test('a Root pre-dispatch refusal refreshes the grant for one safe read, but never repeats a write', async () => {
  for (const method of ['GET', 'POST']) {
    const listeners = new Set(), messages = [], attempts = []; let grants = 0;
    const parent = { postMessage(data, target) {
      assert.equal(target, 'https://shell.example'); messages.push(data.type);
      if (data.type === 'request') { const grant = (++grants === 1 ? 'A' : 'B').repeat(43);
        queueMicrotask(() => { for (const listener of listeners) listener({ source: parent, origin: target,
          data: { schema: data.schema, type: 'grant', requestId: data.requestId, grant } }); }); }
    } };
    const view = { parent, crypto: { randomUUID }, setTimeout, clearTimeout,
      addEventListener(_type, fn) { listeners.add(fn); }, removeEventListener(_type, fn) { listeners.delete(fn); } };
    const client = createRelayIdentityFetch({ view, origin: 'https://app.example', parentOrigin: 'https://shell.example',
      fetch: async (_path, options) => { attempts.push({ grant: options.headers.get('x-soty-window-grant'), key: options.headers.get('idempotency-key') });
        return attempts.length === 1 ? new Response('', { status: 503, headers: { 'x-soty-identity-window': 'not-dispatched' } }) : new Response('ok'); } });
    const options = { method, headers: { 'idempotency-key': 'same-original-request' }, ...(method === 'POST' ? { body: 'write' } : {}) };
    const first = await client('/api', options);
    if (method === 'GET') assert.equal(first.status, 200);
    else { assert.equal(first.status, 503); assert.equal(attempts.length, 1, 'a write is never retried automatically');
      assert.equal((await client('/api', options)).status, 200); }
    assert.deepEqual(attempts, [{ grant: 'A'.repeat(43), key: 'same-original-request' }, { grant: 'B'.repeat(43), key: 'same-original-request' }]);
    assert.deepEqual(messages, ['request', 'invalidate', 'request']);
  }
});

test('unmarked 503 or other failure never triggers grant invalidation or replay', async () => {
  for (const response of [new Response('', { status: 503 }), new Response('', { status: 403, headers: { 'x-soty-identity-window': 'not-dispatched' } })]) {
    const f = fixture(), calls = [];
    const client = createRelayIdentityFetch({ view: f.view, origin: 'https://app.example', parentOrigin: 'https://shell.example',
      fetch: async (path, options) => { calls.push({ path, options }); return response; } });
    const pending = client('/api'); await new Promise(done => setImmediate(done)); f.emit(0); assert.equal(await pending, response);
    assert.equal(calls.length, 1); assert.equal(f.posted.length, 1);
  }
});
