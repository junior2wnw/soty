import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createOAuthIngress, parseOAuthForm, OAuthIngressError } from '../capabilities-oauth-ingress.js';

const type = 'application/x-www-form-urlencoded';
async function fixture(t, { limits = {}, hold } = {}) {
  const ingress = createOAuthIngress({ limits });
  const server = createServer(async (req, res) => {
    let lease;
    try {
      lease = ingress.enter(req, res);
      const form = await lease.readForm();
      if (req.url === '/hold') await hold();
      res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ keys: Object.keys(form), bytes: Buffer.byteLength(form.code ?? '') }));
    } catch (error) {
      const code = error instanceof OAuthIngressError ? error.code : 'unexpected';
      res.statusCode = ['temporarily_unavailable', 'rate_limit'].includes(code) ? 429 : code === 'request_timeout' ? 408 : 400;
      res.setHeader('connection', 'close'); res.end(JSON.stringify({ error: code }));
    } finally { lease?.release(); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const send = async (body, headers = {}, path = '/') => {
    const response = await fetch(origin + path, { method: 'POST', headers: { 'Content-Type': type, ...headers }, body });
    return { status: response.status, body: await response.json() };
  };
  return { origin, send };
}

test('form boundary preserves strings and rejects ambiguity and malformed encodings', () => {
  const value = parseOAuthForm('client_id=soty-codex-cli&scope=notes.createDraft&code=a%2Bb+c');
  assert.equal(Object.getPrototypeOf(value), null); assert.equal(value.code, 'a+b c');
  for (const raw of ['resource=a&resource=b', 'resource=a&%72esource=b', 'code=%', 'code=%C3%28',
    'code=%00', 'code=%0A', 'resource[]=x', 'code=x&', '\ufeffcode=x', 'code', '=x', '__proto__=x']) {
    assert.throws(() => parseOAuthForm(raw), error => error.code === 'invalid_request');
  }
});

test('real wire accepts the exact byte bound and rejects plus one, compression and invalid UTF8', async t => {
  const f = await fixture(t);
  const exact = await f.send('code=' + 'x'.repeat(16379));
  assert.equal(exact.status, 200); assert.equal(exact.body.bytes, 16379);
  const over = await f.send('code=' + 'x'.repeat(16380)); assert.equal(over.body.error, 'payload_too_large');
  assert.equal((await f.send('code=x', { 'Content-Encoding': 'gzip' })).body.error, 'unsupported_encoding');
  assert.equal((await f.send('code=x', { 'Content-Type': 'application/json' })).body.error, 'unsupported_media_type');
  assert.equal((await f.send(Buffer.from([99, 111, 100, 101, 61, 0xc3, 0x28]))).body.error, 'invalid_request');
});

test('chunked body has the same byte bound and a finite body deadline', async t => {
  const f = await fixture(t, { limits: { bodyBytes: 64, bodyTimeoutMs: 100 } });
  const chunked = (overflow = false, wait = false) => new Promise((resolve, reject) => {
    const req = request(f.origin, { method: 'POST', headers: { 'Content-Type': type } }, res => {
      let text = ''; res.on('data', chunk => { text += chunk; });
      res.on('end', () => { req.destroy(); resolve({ status: res.statusCode, body: JSON.parse(text) }); });
    });
    req.on('error', reject); req.write('code=');
    if (wait) return;
    for (let i = 0; i < (overflow ? 60 : 59); i++) req.write('x');
    req.end();
  });
  assert.equal((await chunked()).status, 200);
  assert.equal((await chunked(true)).body.error, 'payload_too_large');
  const timeout = await chunked(false, true); assert.equal(timeout.status, 408); assert.equal(timeout.body.error, 'request_timeout');
  assert.equal((await f.send('code=healthy')).status, 200);
});

test('admission holds its slot through downstream work and releases on response', async t => {
  let unblock, entered;
  const gate = new Promise(resolve => { unblock = resolve; });
  const started = new Promise(resolve => { entered = resolve; });
  t.after(() => unblock());
  const f = await fixture(t, { limits: { requests: 1 }, hold: async () => { entered(); await gate; } });
  const pending = f.send('code=one', {}, '/hold'); await started;
  const denied = await f.send('code=two'); assert.equal(denied.status, 429); assert.equal(denied.body.error, 'temporarily_unavailable');
  unblock(); assert.equal((await pending).status, 200);
  assert.equal((await f.send('code=three')).status, 200);
});

test('bounded URL and attempt rate cannot be enlarged by settings', async t => {
  assert.throws(() => createOAuthIngress({ limits: { requests: 17 } }), error => error.code === 'oauth_configuration_invalid');
  assert.throws(() => createOAuthIngress({ limits: { invented: 1 } }), error => error.code === 'oauth_configuration_invalid');
  const f = await fixture(t, { limits: { urlBytes: 32, attempts: 2 } });
  assert.equal((await f.send('code=x', {}, '/' + 'x'.repeat(33))).body.error, 'invalid_request');
  assert.equal((await f.send('code=x')).status, 200); assert.equal((await f.send('code=x')).status, 200);
  assert.equal((await f.send('code=x', { 'X-Forwarded-For': 'different-client' })).body.error, 'rate_limit');
});
