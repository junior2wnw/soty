import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { createNativeIngress, parseNativeDraftJson, singleHeader, NATIVE_HTTP_LIMITS } from '../capabilities-ingress.js';

const payload = { title: 'План 🚀', body: 'e\u0301\n你好 👩🏽‍💻', idempotencyKey: 'same-request-01' };
const wire = JSON.stringify(payload);
const expectCode = (fn, code) => assert.throws(fn, error => error.code === code);
async function fixture(t, limits = {}) {
  const ingress = createNativeIngress({ limits }), state = { entered: 0, finished: 0 };
  const server = createServer(async (req, res) => {
    let lease;
    try {
      if (req.method === 'GET') { res.writeHead(200).end('{}'); return; }
      lease = ingress.enter(req); state.entered++;
      if (req.headers['x-early-refusal']) throw Object.assign(new Error(), { code: 'authorization_required' });
      const result = await lease.read(req);
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(result));
    } catch (error) {
      const statuses = { ingress_capacity: 429, ingress_rate_limit: 429, payload_too_large: 413,
        unsupported_media_type: 415, unsupported_encoding: 415, request_timeout: 408, authorization_required: 401 };
      if (!res.destroyed) res.writeHead(statuses[error.code] || 400, { connection: 'close' }).end(JSON.stringify({ error: error.code }));
    } finally { lease?.release(); state.finished++; }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const port = server.address().port;
  function client({ method = 'POST', headers = {}, body, end = true } = {}) {
    let req;
    const result = new Promise(resolve => {
      req = request({ host: '127.0.0.1', port, path: '/', method,
        headers: { ...(method === 'POST' ? { 'content-type': 'application/json' } : {}), ...headers } }, res => {
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks).toString() || '{}') }));
        res.on('error', () => resolve({ aborted: true }));
      });
      req.on('error', () => resolve({ aborted: true }));
      if (body !== undefined) req.write(body);
      if (end) req.end();
    });
    return { req, result };
  }
  async function entered(count) {
    for (let i = 0; i < 100 && state.entered < count; i++) await delay(5);
    assert.ok(state.entered >= count, 'requests reached the real server');
  }
  return { client, entered, state };
}

test('flat native JSON accepts standard escapes without normalization and rejects ambiguous object shapes', () => {
  assert.deepEqual({ ...parseNativeDraftJson(wire) }, payload);
  assert.deepEqual({ ...parseNativeDraftJson(' \r\n {"body":"a\\\"b\\\\c", "idempotencyKey":"request-key", "ti\\u0074le":"x"}\t') },
    { title: 'x', body: 'a"b\\c', idempotencyKey: 'request-key' });
  for (const text of [
    '{}', '[]', 'null', '"text"', '{"title":"a","body":"b"}', wire + 'x', '\ufeff' + wire,
    '{"title":"a","title":"b","body":"c","idempotencyKey":"request-key"}',
    '{"title":"a","ti\\u0074le":"b","idempotencyKey":"request-key"}',
    '{"title":"a","body":{"x":"y"},"idempotencyKey":"request-key"}',
    '{"title":"a","body":"b","idempotencyKey":"request-key",}',
    '{"title":"a","body":"\\z","idempotencyKey":"request-key"}',
    '{"title":"a","body":"raw\nnewline","idempotencyKey":"request-key"}',
    '{"title":"a","body":"b","idempotencyKey":"request-key","extra":"value"}',
    '{"__proto__":"a","body":"b","idempotencyKey":"request-key"}',
  ]) expectCode(() => parseNativeDraftJson(text), 'invalid_input');
  const large = { ...payload, body: '漢字\\"'.repeat(50000) };
  assert.deepEqual({ ...parseNativeDraftJson(JSON.stringify(large)) }, large, 'linear scan handles large strings without recursive regex');
});

test('singleton raw headers do not hide duplicate authorization or content types', () => {
  expectCode(() => singleHeader({ rawHeaders: ['Authorization', 'one', 'authorization', 'two'] }, 'authorization'), 'authorization_required');
  expectCode(() => singleHeader({ rawHeaders: ['Content-Type', 'one', 'CONTENT-TYPE', 'two'] }, 'content-type'), 'invalid_input');
  assert.equal(singleHeader({ rawHeaders: ['Authorization', 'exact'] }, 'authorization'), 'exact');
});

test('real HTTP accepts split UTF-8 and rejects invalid bytes, BOM, encoding, type and raw overflow before content admission', async t => {
  const f = await fixture(t, { bodyBytes: 1024 });
  const accepted = f.client({ end: false });
  const bytes = Buffer.from(wire);
  for (let at = 0; at < bytes.length; at += 2) accepted.req.write(bytes.subarray(at, at + 2));
  accepted.req.end();
  assert.deepEqual(await accepted.result, { status: 200, body: payload });
  const cases = [
    { body: Buffer.from([0xff, 0xfe]), status: 400 },
    { body: Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), bytes]), status: 400 },
    { body: wire, headers: { 'content-type': 'text/plain' }, status: 415 },
    { body: wire, headers: { 'content-encoding': 'gzip' }, status: 415 },
    { body: 'x'.repeat(1025), status: 413 },
    { body: wire, headers: { 'content-length': '1025' }, status: 413 },
    { body: wire, headers: { 'x-early-refusal': '1' }, status: 401 },
    { body: '{broken', status: 400 },
  ];
  for (const { status, ...options } of cases) {
    assert.equal((await f.client(options).result).status, status);
    assert.equal((await f.client({ body: wire }).result).status, 200, 'failure releases exactly one reader');
  }
});

test('real concurrent readers reject a third peer request, permit status and recover after an aborted body', async t => {
  const f = await fixture(t, { readers: 2, readersPerPeer: 2 });
  const first = f.client({ body: '{', end: false }), second = f.client({ body: '{', end: false });
  await f.entered(2);
  assert.equal((await f.client({ body: wire }).result).status, 429);
  assert.equal((await f.client({ method: 'GET' }).result).status, 200);
  first.req.destroy(); await first.result;
  for (let i = 0; i < 100 && f.state.finished < 3; i++) await delay(5);
  assert.equal((await f.client({ body: wire }).result).status, 200);
  second.req.destroy(); await second.result;
});

test('body deadline is total and is not extended by incoming chunks; timeout frees admission', async t => {
  const f = await fixture(t, { bodyTimeoutMs: 90, readers: 1, readersPerPeer: 1 });
  const slow = f.client({ body: '{', end: false });
  const chunks = setInterval(() => { if (!slow.req.destroyed) slow.req.write(' '); }, 15);
  try { assert.equal((await slow.result).status, 408); }
  finally { clearInterval(chunks); slow.req.destroy(); }
  assert.equal((await f.client({ body: wire }).result).status, 200);
});

test('attempt limits include early refusals but never block GET status', async t => {
  const f = await fixture(t, { attempts: 2 });
  assert.equal((await f.client({ body: wire, headers: { 'x-early-refusal': '1' } }).result).status, 401);
  assert.equal((await f.client({ body: wire }).result).status, 200);
  assert.equal((await f.client({ body: wire }).result).status, 429);
  assert.equal((await f.client({ method: 'GET' }).result).status, 200);
});

test('bounded peer table never evicts an active limit and recovers only expired inactive records', async () => {
  const ingress = createNativeIngress({ limits: { peers: 1, readers: 2, windowMs: 20 } });
  const peer = remoteAddress => ({ socket: { remoteAddress }, headers: { 'x-forwarded-for': 'ignored' } });
  const first = ingress.enter(peer('192.0.2.1'));
  expectCode(() => ingress.enter(peer('192.0.2.2')), 'ingress_capacity');
  await delay(25);
  expectCode(() => ingress.enter(peer('192.0.2.2')), 'ingress_capacity');
  first.release(); first.release();
  ingress.enter(peer('192.0.2.2')).release();
  for (const [key, maximum] of Object.entries(NATIVE_HTTP_LIMITS)) {
    expectCode(() => createNativeIngress({ limits: { [key]: maximum + 1 } }), 'ingress_configuration_invalid');
    expectCode(() => createNativeIngress({ limits: { [key]: 0 } }), 'ingress_configuration_invalid');
  }
});
