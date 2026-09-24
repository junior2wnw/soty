import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { connect } from 'node:net';
import { createConnectHandler } from '../server/http.mjs';
import { createConnectService, digestArgs } from '../server/index.mjs';
import { createClientWithStorage } from '../browser/client.mjs';

async function fixture(t, options = {}) {
  let handler; const rejections = [];
  const server = createServer((req, res) => {
    handler(req, res).then(handled => { if (!handled) res.writeHead(404).end(); }).catch(error => {
      rejections.push(error); res.writeHead(500).end();
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port; const origin = `http://127.0.0.1:${port}`;
  const service = createConnectService({ databasePath: ':memory:', projectId: 'http-test', allowedOrigins: [origin] });
  handler = createConnectHandler(service, options);
  t.after(async () => {
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); service.close();
    assert.deepEqual(rejections, [], 'the HTTP adapter must not reject its outer request promise');
  });
  return { origin, port, async post(body, headers = {}) {
    const response = await fetch(`${origin}/api/connect/rpc`, { method: 'POST', headers: {
      'content-type': 'application/json', origin, ...headers,
    }, body: typeof body === 'string' ? body : JSON.stringify(body) });
    return { response, body: await response.json() };
  } };
}
const challenge = () => ({ protocol: 1, op: 'challenge', args: { operation: 'status', digest: digestArgs({}) } });

test('browser client authenticates and encrypts a vault through the real HTTP adapter', async t => {
  const f = await fixture(t);
  let saved = null;
  const storage = {
    read: async () => structuredClone(saved),
    claim: async candidate => { saved ||= structuredClone(candidate); return structuredClone(saved); },
    compareAndSwap: async (expected, candidate) => {
      assert.equal(saved.localRevision, expected);
      saved = structuredClone(candidate); return structuredClone(saved);
    }
  };
  const client = createClientWithStorage({ projectId: 'http-test', endpoint: f.origin + '/api/connect/rpc',
    fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin: f.origin } }) }, storage);
  t.after(() => client.dispose());
  const first = await client.bootstrap('HTTP client');
  assert.equal((await client.bootstrap('Retry')).accountId, first.accountId);
  await client.saveVault({ note: 'Across actual HTTP' });
  assert.deepEqual((await client.loadVault(first.accountId)).payload, { note: 'Across actual HTTP' });
});
async function raw(port, request) {
  return new Promise((resolve, reject) => {
    const chunks = []; const socket = connect(port, '127.0.0.1', () => socket.end(request));
    socket.setTimeout(5_000, () => { socket.destroy(); reject(new Error('raw HTTP response timed out')); });
    socket.on('data', chunk => chunks.push(chunk)); socket.on('error', reject);
    socket.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
  });
}

test('raw invalid request URL receives 400 and subsequent service requests still work', async t => {
  const f = await fixture(t);
  const response = await raw(f.port, 'POST http://[ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
  assert.match(response, /^HTTP\/1\.1 400 /);
  assert.match(response, /"code":"invalid_url"/);
  const result = await f.post(challenge());
  assert.equal(result.response.status, 200); assert.equal(result.body.ok, true);
  assert.equal(result.response.headers.get('cache-control'), 'no-store');
  assert.equal(result.response.headers.get('x-content-type-options'), 'nosniff');
});

test('HTTP methods, exact JSON type and supplied origin are enforced before authorization', async t => {
  const f = await fixture(t);
  const method = await fetch(`${f.origin}/api/connect/rpc`, { method: 'GET' });
  assert.equal(method.status, 405); assert.equal((await method.json()).error.code, 'method_not_allowed');
  const type = await f.post(challenge(), { 'content-type': 'application/json-unsupported' });
  assert.equal(type.response.status, 415); assert.equal(type.body.error.code, 'json_required');
  const missing = await fetch(`${f.origin}/api/connect/rpc`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(challenge()) });
  assert.equal(missing.status, 403); assert.equal((await missing.json()).error.code, 'origin_required');
  const opaque = await f.post(challenge(), { origin: 'null' });
  assert.equal(opaque.response.status, 403); assert.equal(opaque.body.error.code, 'origin_required');
  const foreign = await f.post(challenge(), { origin: 'https://untrusted.example' });
  assert.equal(foreign.response.status, 400); assert.equal(foreign.body.ok, false);
  assert.match(foreign.body.error.code, /origin/);
  const otherPath = await fetch(`${f.origin}/unrelated`);
  assert.equal(otherPath.status, 404);
});

test('only explicit supported protocol objects reach the service', async t => {
  const f = await fixture(t);
  for (const body of [{ ...challenge(), protocol: 2 }, { ...challenge(), protocol: '1' }, { op: 'challenge' }, null, []]) {
    const result = await f.post(body);
    assert.equal(result.response.status, 400); assert.equal(result.body.error.code, 'protocol_unsupported');
  }
  const invalid = await f.post('{');
  assert.equal(invalid.response.status, 400); assert.equal(invalid.body.error.code, 'request_failed');
});

test('declared and chunked body limits return a complete 413 response without calling the service', async t => {
  const f = await fixture(t, { maxBytes: 256 });
  const result = await f.post({ ...challenge(), padding: 'x'.repeat(1024) });
  assert.equal(result.response.status, 413); assert.equal(result.body.error.code, 'request_too_large');
  const body = 'x'.repeat(1024);
  const response = await raw(f.port, `POST /api/connect/rpc HTTP/1.1\r\nHost: localhost\r\nOrigin: ${f.origin}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n${body.length.toString(16)}\r\n${body}\r\n0\r\n\r\n`);
  assert.match(response, /^HTTP\/1\.1 413 /); assert.match(response, /"code":"request_too_large"/);
  const next = await f.post(challenge());
  assert.equal(next.response.status, 200);
});
