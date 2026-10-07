import test from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:http';
import { connect } from 'node:net';
import { once } from 'node:events';
import { performance } from 'node:perf_hooks';
import { nativeHttpFixture, nativeIdentity } from './support/native-capability-http.mjs';
import { fenceClosingHttpSocket, fenceClosingUpgrade, isHttpSocketClosing } from '../http-closing-socket.mjs';
import { REJECTION_DRAIN_LIMITS, rejectionDrainSnapshot } from '../capabilities-rejection-drain.js';
const CREATE = '/api/capabilities/v1/notes/drafts', BODY = ' '.repeat(2 * 1024 * 1024 + 1);
const tick = () => new Promise(done => setImmediate(done));
const delay = ms => new Promise(done => setTimeout(done, ms));
function writableOrClosed(stream) {
  return new Promise(resolve => {
    const finish = () => { stream.off('drain', finish); stream.off('close', finish); stream.off('error', finish); resolve(); };
    stream.once('drain', finish); stream.once('close', finish); stream.once('error', finish);
    if (stream.destroyed) finish();
  });
}
async function closedSockets(captures) {
  await Promise.all([...new Set(captures.map(req => req.socket))].map(async socket => {
    // Native closed flags can precede the emitted close callback. The fence
    // remains owned until that event; wait for it rather than a flag/tick.
    if (!isHttpSocketClosing(socket)) return;
    let timer;
    try { await Promise.race([once(socket, 'close'), new Promise((_ok, bad) => { timer = setTimeout(() => bad(Error('native socket close missing')), 500); })]); }
    finally { clearTimeout(timer); }
  }));
}
async function fixture(t) {
  const f = await nativeHttpFixture(t), actor = nativeIdentity('Synthetic bounded rejection');
  const account = await f.bootstrap(actor), identity = await f.issue(actor, account.accountId);
  return { f, actor, account, identity };
}
const ledger = f => f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n);
function framed(f, token, { pattern = [65536], chunked = false, partial = false, huge = false, abort = false } = {}) {
  let client;
  const result = new Promise(resolve => {
    let settled = false;
    const finish = value => { if (settled) return; settled = true; resolve(value); };
    client = request(f.origin + CREATE, { method: 'POST', headers: { authorization: `Bearer ${token}`,
      'content-type': 'application/json', ...(!chunked ? { 'content-length': String(huge ? 2 ** 32 : BODY.length) } : {}) } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk));
      res.once('error', error => finish({ ok: false, code: error.code }));
      res.once('end', () => finish({ ok: true, status: res.statusCode, body: JSON.parse(Buffer.concat(chunks)) }));
    });
    client.once('error', error => finish({ ok: false, code: error.code }));
    client.setTimeout(3500, () => { client.destroy(); finish({ ok: false, code: 'test_timeout' }); });
  });
  const sending = (async () => {
    if (partial || huge || abort) {
      client.write(' '); if (abort) setTimeout(() => client.destroy(), 30); return;
    }
    let index = 0;
    for (let offset = 0; offset < BODY.length;) {
      const size = Math.min(pattern[index++ % pattern.length], BODY.length - offset);
      if (!client.write(BODY.slice(offset, offset + size))) await once(client, 'drain');
      offset += size;
    }
    client.end();
  })().catch(() => {});
  return { result, sending, client };
}
function raw(f, bytes) {
  return new Promise(resolve => {
    const socket = connect({ host: '127.0.0.1', port: f.server.address().port });
    let text = '', errorCode = null, finished = false;
    const finish = () => { if (finished) return; finished = true; resolve({ text, errorCode }); };
    socket.on('data', chunk => { text += chunk.toString('utf8'); if (text.length > 65536) socket.destroy(); });
    socket.once('error', error => { errorCode = error.code; }); socket.once('close', finish);
    socket.setTimeout(3500, () => { errorCode = 'test_timeout'; socket.destroy(); });
    socket.once('connect', () => socket.write(bytes));
  });
}
function first(f, token, chunked) {
  const header = `POST ${CREATE} HTTP/1.1\r\nHost: ${new URL(f.origin).host}\r\nAuthorization: Bearer ${token}\r\nContent-Type: application/json\r\n${chunked ? 'Transfer-Encoding: chunked' : 'Content-Length: ' + BODY.length}\r\n\r\n`;
  if (!chunked) return header + BODY;
  let encoded = '';
  for (let offset = 0; offset < BODY.length; offset += 65535) {
    const chunk = BODY.slice(offset, offset + 65535); encoded += chunk.length.toString(16) + '\r\n' + chunk + '\r\n';
  }
  return header + encoded + '0\r\n\r\n';
}

test('exact original 2MiB+1 counter retake12 and six fragmented uploads reliably receive413', async t => {
  const { f, identity } = await fixture(t), captures = [];
  f.server.prependListener('request', req => { if (req.url === CREATE) captures.push(req); });
  for (let index = 0; index < 12; index++) {
    const response = await f.http(CREATE, { method: 'POST', token: identity.token, body: BODY });
    assert.equal(response.status, 413); assert.deepEqual(response.body, { error: { code: 'payload_too_large' } });
  }
  for (const pattern of [[65536], [1, 31, 4096, 65535, 109], [8191, 257, 32767]]) {
    for (const chunked of [false, true]) {
      const sending = framed(f, identity.token, { pattern, chunked }), result = await sending.result;
      await sending.sending; assert.equal(result.ok, true); assert.equal(result.status, 413);
      assert.deepEqual(result.body, { error: { code: 'payload_too_large' } });
    }
  }
  await closedSockets(captures); assert.equal(ledger(f), 0); assert.deepEqual(rejectionDrainSnapshot(), { active: 0, peers: 0 });
  for (const req of captures) {
    assert.equal(req.listenerCount('data'), 0); assert.equal(req.listenerCount('error'), 0);
    assert.equal(isHttpSocketClosing(req.socket), false);
  }
});

test('raw TCP oversized+following valid REST/MCP/upgrade has one413 and no following route/auth/effect', async t => {
  const { f, identity, actor, account } = await fixture(t);
  const mcpIdentity = await f.issue(actor, account.accountId, { audience: f.origin + '/mcp' });
  let followingRoutes = 0, upgrades = 0, fencedUpgrades = 0;
  const layer = f.app._router.stack.findIndex(value => value.handle === fenceClosingHttpSocket);
  assert.ok(layer >= 0); const nextLayer = f.app._router.stack[layer + 1], original = nextLayer.handle;
  nextLayer.handle = function (req, res, next) {
    if (req.headers['x-causal-following'] === '1') followingRoutes++;
    return original(req, res, next);
  };
  f.server.on('upgrade', (req, socket) => {
    if (fenceClosingUpgrade(req, socket)) { fencedUpgrades++; return; }
    upgrades++; socket.destroy();
  });
  const host = new URL(f.origin).host;
  for (const chunked of [false, true]) {
    for (const kind of ['REST', 'MCP', 'upgrade']) {
      const value = JSON.stringify(kind === 'REST' ? { title: 'following', body: 'must not commit', idempotencyKey: `following-${chunked}` }
        : { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'fixture', version: '1' } } });
      const following = kind === 'upgrade'
        ? `GET /ws/abcdefghijklmnop HTTP/1.1\r\nHost: ${host}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: c3ludGhldGljLW9ubHktMQ==\r\nX-Causal-Following: 1\r\n\r\n`
        : `POST ${kind === 'REST' ? CREATE : '/mcp'} HTTP/1.1\r\nHost: ${host}\r\nAuthorization: Bearer ${kind === 'REST' ? identity.token : mcpIdentity.token}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: ${Buffer.byteLength(value)}\r\nX-Causal-Following: 1\r\n\r\n${value}`;
      const reply = await raw(f, first(f, identity.token, chunked) + following);
      assert.equal(reply.errorCode, null); assert.match(reply.text, /^HTTP\/1\.1 413 /u);
      assert.equal((reply.text.match(/HTTP\/1\.1 /gu) || []).length, 1);
      assert.equal(followingRoutes, 0); assert.equal(upgrades, 0); assert.equal(ledger(f), 0);
    }
  }
  assert.equal(fencedUpgrades, 2); await tick(); assert.deepEqual(rejectionDrainSnapshot(), { active: 0, peers: 0 });
});

test('huge declared, slow, capacity and genuine abort terminate bounded with slots/listeners released', { timeout: 10000 }, async t => {
  const { f, identity } = await fixture(t), captures = [];
  f.server.prependListener('request', req => { if (req.url === CREATE) captures.push(req); });
  const hugeStarted = performance.now(), huge = framed(f, identity.token, { huge: true });
  assert.equal((await huge.result).ok, false); assert.ok(performance.now() - hugeStarted < 1000);
  const abort = framed(f, identity.token, { abort: true }); assert.equal((await abort.result).ok, false);
  const started = performance.now(), slow = [0, 1, 2].map(() => framed(f, identity.token, { partial: true }));
  await delay(50); const pending = rejectionDrainSnapshot(); assert.equal(pending.active, 2); assert.equal(pending.peers, 1);
  const outcomes = await Promise.all(slow.map(value => value.result)); assert.ok(outcomes.every(value => !value.ok));
  assert.ok(performance.now() - started >= REJECTION_DRAIN_LIMITS.wallMs);
  assert.ok(performance.now() - started < REJECTION_DRAIN_LIMITS.wallMs + 1000);
  await closedSockets(captures); assert.equal(ledger(f), 0); assert.deepEqual(rejectionDrainSnapshot(), { active: 0, peers: 0 });
  for (const req of captures) {
    assert.equal(req.listenerCount('data'), 0); assert.equal(req.listenerCount('error'), 0);
    assert.equal(isHttpSocketClosing(req.socket), false);
  }
  const accepted = await f.http(CREATE, { method: 'POST', token: identity.token,
    body: JSON.stringify({ title: 'after rejection', body: 'valid', idempotencyKey: 'after-bounded-drain' }) });
  assert.equal(accepted.status, 201); assert.equal(ledger(f), 1);
});

test('chunked endless sender stops at discard byte bound without waiting for EOF or admitting an effect', { timeout: 10000 }, async t => {
  const { f, identity } = await fixture(t), captures = [], started = performance.now();
  f.server.prependListener('request', req => { if (req.url === CREATE) captures.push(req); });
  let client;
  const outcome = new Promise(resolve => {
    client = request(f.origin + CREATE, { method: 'POST', headers: { authorization: `Bearer ${identity.token}`,
      'content-type': 'application/json' } }, res => {
      res.resume(); res.once('end', () => resolve({ success: true, status: res.statusCode }));
      res.once('error', () => resolve({ success: false }));
    });
    client.once('error', () => resolve({ success: false }));
    client.once('close', () => resolve({ success: false }));
    client.setTimeout(3000, () => client.destroy());
  });
  const chunk = ' '.repeat(65536);
  const sending = (async () => {
    // No terminal HTTP chunk. The server must stop this live writer by bytes.
    for (let bytes = 0; bytes < 5 * 1024 * 1024 && !client.destroyed; bytes += chunk.length) {
      if (!client.write(chunk)) await writableOrClosed(client);
      await tick();
    }
  })().catch(() => {});
  assert.equal((await outcome).success, false); await sending; await closedSockets(captures);
  assert.ok(performance.now() - started < REJECTION_DRAIN_LIMITS.wallMs);
  assert.equal(ledger(f), 0); assert.deepEqual(rejectionDrainSnapshot(), { active: 0, peers: 0 });
  for (const req of captures) { assert.equal(req.listenerCount('data'), 0); assert.equal(req.listenerCount('error'), 0); }
});
