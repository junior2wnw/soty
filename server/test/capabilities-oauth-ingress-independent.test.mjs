import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { generateKeyPairSync } from 'node:crypto';
import { connect } from 'node:net';
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { setTimeout as pause } from 'node:timers/promises';
import express from 'express';
import { Provider } from 'oidc-provider';
import { createOAuthIngress } from '../capabilities-oauth-ingress.js';

const formType = 'application/x-www-form-urlencoded';
function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
function client(port, body, path = '/oauth/revoke', { partial = false, headers = {} } = {}) {
  let req;
  const result = new Promise(resolve => {
    req = request({ hostname: '127.0.0.1', port, path, method: 'POST', agent: false,
      headers: { 'Content-Type': formType, ...(partial ? {} : { 'Content-Length': Buffer.byteLength(body) }), ...headers } }, res => {
      let text = '';
      res.on('data', chunk => { text += chunk; });
      res.once('end', () => resolve({ status: res.statusCode, text }));
      res.once('error', () => resolve({ disconnected: true }));
    });
    req.once('error', () => resolve({ disconnected: true }));
    if (partial) req.write(body); else req.end(body);
  });
  return { req, result };
}

async function memoryProbe() {
  const ingress = createOAuthIngress();
  let bytes = 0;
  const server = createServer(async (req, res) => {
    req.on('data', part => { bytes += part.length; if (bytes === 16384) process.send({ phase: 'held', bytes }); });
    let lease;
    try { lease = ingress.enter(req, res); await lease.readForm(); res.end(); }
    catch { if (!res.destroyed) res.writeHead(400, { connection: 'close' }).end(); }
    finally { lease?.release(); }
  });
  const measure = async () => {
    for (let turn = 0; turn < 3; turn++) { global.gc(); await new Promise(resolve => setImmediate(resolve)); }
    return process.memoryUsage().heapUsed;
  };
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const baseline = await measure();
  process.on('message', async message => {
    if (message === 'measure') process.send({ phase: 'measured', bytes, retainedHeapBytes: await measure() - baseline });
    else if (message === 'finish') {
      server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); process.disconnect();
    }
  });
  process.send({ phase: 'ready', port: server.address().port });
}

if (process.argv[2] === 'oauth-ingress-memory-probe') {
  await memoryProbe();
} else {
async function until(predicate, message) {
  for (let turn = 0; turn < 200; turn++) { if (predicate()) return; await pause(5); }
  assert.fail(message);
}
function childMessage(child, phase) {
  return new Promise((resolve, reject) => {
    const finish = (error, value) => {
      clearTimeout(timer); child.off('message', message); child.off('error', failed); child.off('exit', exited);
      if (error) reject(error); else resolve(value);
    };
    const message = value => { if (value.phase === phase) finish(null, value); };
    const failed = error => finish(error);
    const exited = () => finish(new Error('oauth_probe_exit_' + phase));
    const timer = setTimeout(() => finish(new Error('oauth_probe_timeout_' + phase)), 7000);
    child.on('message', message); child.once('error', failed); child.once('exit', exited);
  });
}
async function httpFixture(t, limits) {
  const ingress = createOAuthIngress({ limits });
  const state = { accepted: 0, admitted: 0, finished: 0, failures: [] };
  const server = createServer(async (req, res) => {
    let lease;
    try {
      lease = ingress.enter(req, res); state.admitted++;
      await lease.readForm(); state.accepted++;
      res.writeHead(204).end();
    } catch (error) {
      state.failures.push(error.code);
      const status = ['temporarily_unavailable', 'rate_limit'].includes(error.code) ? 429 : 400;
      if (!res.destroyed) res.writeHead(status, { connection: 'close', 'content-type': 'application/json' }).end(JSON.stringify({ error: error.code }));
    } finally { lease?.release(); state.finished++; }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  return { state, port: server.address().port };
}
function raw(port, wire) {
  return new Promise((resolve, reject) => {
    const socket = connect(port, '127.0.0.1');
    let text = '';
    socket.setTimeout(2500, () => socket.destroy(new Error('oauth_raw_timeout')));
    socket.on('data', part => { text += part; });
    socket.once('error', reject);
    socket.once('end', () => resolve(text));
    socket.once('connect', () => socket.end(wire));
  });
}

test('independent actual Provider keeps admission until a disconnected request finishes its adapter work', { timeout: 5000 }, async t => {
  const gate = deferred(), entered = deferred(), secondEntered = deferred(), closed = deferred(), completed = deferred();
  let finds = 0, active = 0, peak = 0, admitted = 0;
  // Deliberately held public adapter seam. No tokens or consent are seeded and
  // the real Provider performs the ordinary unknown-token revoke response.
  class Adapter {
    constructor(model) { this.model = model; }
    async find() {
      if (this.model !== 'AccessToken') return undefined;
      finds++; active++; peak = Math.max(peak, active);
      if (finds === 1) entered.resolve(); else secondEntered.resolve();
      try { await gate.promise; return undefined; } finally { active--; }
    }
    async upsert() { assert.fail('unexpected artifact write'); }
    async destroy() {}
    async consume() { assert.fail('unexpected token consumption'); }
    async revokeByGrantId() {}
    async findByUid() { return undefined; }
    async findByUserCode() { return undefined; }
  }
  const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  key.kid = 'independent-ingress'; key.alg = 'RS256'; key.use = 'sig';
  const app = express(), server = createServer(app);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  const provider = new Provider(`http://127.0.0.1:${port}/oauth`, {
    adapter: Adapter,
    clients: [{ client_id: 'independent-ingress', application_type: 'native',
      redirect_uris: ['http://127.0.0.1:19777/callback'], token_endpoint_auth_method: 'none',
      grant_types: ['authorization_code'], response_types: ['code'] }],
    jwks: { keys: [key] }, cookies: { keys: ['synthetic-independent-cookie-key'] },
    responseTypes: ['code'], scopes: [], claims: {},
    features: { devInteractions: { enabled: false }, userinfo: { enabled: false },
      clientIdMetadataDocument: { enabled: false }, revocation: { enabled: true } },
    routes: { revocation: '/revoke' },
  });
  const ingress = createOAuthIngress({ limits: { requests: 1, requestsPerPeer: 1 } });
  provider.use(async (ctx, next) => {
    let lease, ticket;
    try {
      lease = ingress.enter(ctx.req, ctx.res); ticket = ++admitted;
      ctx.req.body = await lease.readForm();
      ctx.res.once('close', () => closed.resolve());
      await next();
    } catch (error) {
      ctx.status = error.code === 'temporarily_unavailable' ? 429 : 400;
      ctx.body = { error: error.code };
    } finally { lease?.release(); if (ticket === 1) completed.resolve(); }
  });
  app.use('/oauth', provider.callback());
  t.after(async () => {
    gate.resolve();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  const body = new URLSearchParams({ client_id: 'independent-ingress', token: 'x'.repeat(43),
    token_type_hint: 'access_token' }).toString();
  const first = client(port, body);
  const started = await Promise.race([entered.promise.then(() => ({ entered: true })), first.result]);
  assert.equal(started.entered, true, 'real revoke reaches the public adapter');
  first.req.destroy(); await first.result; await closed.promise;
  const second = client(port, body);
  const outcome = await Promise.race([second.result, secondEntered.promise.then(() => ({ secondEntered: true }))]);
  assert.equal(outcome.status, 429, 'disconnect must not release the slot while real Provider adapter work is retained');
  assert.equal(peak, 1);
  gate.resolve();
  await completed.promise;
  assert.equal((await client(port, body).result).status, 200, 'completion admits a later ordinary request');
  assert.equal(finds, 2);
  assert.equal(peak, 1);
});

test('independent real one-byte chunks retain bounded heap before the terminating HTTP chunk', { timeout: 10000 }, async t => {
  const child = fork(fileURLToPath(import.meta.url), ['oauth-ingress-memory-probe'], {
    execPath: process.execPath, execArgv: ['--expose-gc', '--max-old-space-size=64'], windowsHide: true,
    env: { PATH: process.env.PATH || '', SystemRoot: process.env.SystemRoot || '', TEMP: process.env.TEMP || '', TMP: process.env.TMP || '' },
    stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
  });
  let socket;
  t.after(async () => {
    socket?.destroy();
    if (child.exitCode === null && child.signalCode === null) {
      const exited = new Promise(resolve => child.once('exit', resolve)); child.kill(); await exited;
    }
  });
  const ready = await childMessage(child, 'ready');
  const form = Buffer.from('code=' + 'x'.repeat(16379));
  assert.equal(form.length, 16384);
  const wire = Buffer.alloc(form.length * 6);
  for (let index = 0; index < form.length; index++) {
    wire.write('1\r\n', index * 6); wire[index * 6 + 3] = form[index]; wire.write('\r\n', index * 6 + 4);
  }
  const held = childMessage(child, 'held');
  socket = connect(ready.port, '127.0.0.1'); socket.on('error', () => {});
  await new Promise(resolve => socket.once('connect', resolve));
  socket.write(`POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: ${formType}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n`);
  socket.write(wire); await held;
  const measured = childMessage(child, 'measured'); child.send('measure');
  const value = await measured;
  t.diagnostic(JSON.stringify({ bodyBytes: value.bytes, retainedHeapBytes: value.retainedHeapBytes }));
  assert.equal(value.bytes, 16384);
  assert.ok(value.retainedHeapBytes < 2 * 1024 * 1024, 'a retained Buffer per one-byte chunk exceeds this heap budget');
  const exited = new Promise(resolve => child.once('exit', resolve));
  socket.end('0\r\n\r\n'); child.send('finish'); await exited;
  assert.equal(child.exitCode, 0);
});

test('independent duplicate raw headers fail before downstream and release admission', async t => {
  const f = await httpFixture(t, { requests: 1, requestsPerPeer: 1 });
  for (const extra of [
    'cOnTeNt-TyPe: application/x-www-form-urlencoded\r\n',
    'Content-Encoding: identity\r\ncOnTeNt-EnCoDiNg: identity\r\n',
  ]) {
    const answer = await raw(f.port, `POST /oauth/token HTTP/1.1\r\nHost: localhost\r\nContent-Type: ${formType}\r\n${extra}Content-Length: 6\r\nConnection: close\r\n\r\ncode=x`);
    assert.match(answer, /^HTTP\/1\.1 400 /u);
    assert.match(answer, /"error":"invalid_request"/u);
  }
  assert.equal(f.state.accepted, 0);
  assert.equal((await client(f.port, 'code=healthy').result).status, 204);
  assert.equal(f.state.accepted, 1);
});

test('independent early-body abort frees exactly one slot and does not refund the peer attempt', async t => {
  const f = await httpFixture(t, { requests: 2, requestsPerPeer: 2, attempts: 5, bodyTimeoutMs: 1500 });
  const first = client(f.port, 'code=', '/', { partial: true });
  const second = client(f.port, 'code=', '/', { partial: true });
  await until(() => f.state.admitted === 2, 'two body readers entered');
  first.req.destroy(); await first.result;
  await until(() => f.state.finished === 1, 'aborted reader settled');
  const replacement = client(f.port, 'code=', '/', { partial: true });
  await until(() => f.state.admitted === 3, 'one replacement entered');
  const denied = await client(f.port, 'code=four', '/', { headers: { 'X-Forwarded-For': '203.0.113.41' } }).result;
  assert.equal(denied.status, 429);
  assert.equal(JSON.parse(denied.text).error, 'temporarily_unavailable');
  replacement.req.destroy(); second.req.destroy(); await Promise.all([replacement.result, second.result]);
  await until(() => f.state.finished === 4, 'body readers and denied request settled');
  assert.equal((await client(f.port, 'code=five').result).status, 204);
  const rate = await client(f.port, 'code=six', '/', { headers: { 'X-Forwarded-For': '203.0.113.42' } }).result;
  assert.equal(rate.status, 429);
  assert.equal(JSON.parse(rate.text).error, 'rate_limit');
  assert.equal(f.state.accepted, 1);
});
}
