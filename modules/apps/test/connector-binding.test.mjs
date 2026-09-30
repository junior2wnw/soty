import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { EventEmitter, once } from 'node:events';
import { createHash, randomBytes } from 'node:crypto';
import { setTimeout as delay, setImmediate as nextTurn } from 'node:timers/promises';
import { WebSocketServer } from 'ws';
import { createLocalAppsRuntime, probeLocalApp } from '../../../scripts/agent-modules/local-apps.mjs';
import { runtimeTargetDigest } from '../server/schema.mjs';

const identity = Object.freeze({ linkId: 'binding_link', hostDeviceId: 'binding_host', connectorId: 'binding_connector', name: 'Fixture' });
const connectorKey = [identity.linkId, identity.hostDeviceId, identity.connectorId].join('|');
const appId = index => `app-${index.toString(16).padStart(32, '0')}`;
const nonce = () => randomBytes(32).toString('base64url');
const hash = text => createHash('sha256').update(text).digest('hex');
const pins = target => ({ appId: target.appId, revision: target.revision, digest: target.digest, profile: target.profile });
function target(index, port, entryPath = '/', revision = 1, extra = {}) {
  const value = { appId: appId(index), revision, ownerAccountId: 'binding_owner', port, entryPath, profile: 'soty.relay-restricted.v1', ...extra };
  return { ...value, digest: runtimeTargetDigest({ ...value, connectorKey }) };
}
async function until(check, timeout = 3500) {
  const deadline = Date.now() + timeout;
  while (!check()) { if (Date.now() >= deadline) throw new Error('connector_fixture_timeout'); await delay(5); }
  return check();
}
const deps = { randomSecret: nonce, digest: hash, createWebSocket: url => new WebSocket(url), httpRequest: request,
  encodeBase64: value => Buffer.from(value).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') };

async function upstream(t, handler) {
  const seen = [], sockets = new Set();
  const server = createServer((req, res) => {
    const entry = { method: req.method, url: req.url, headers: req.headers }; seen.push(entry);
    if (handler) { handler(req, res, entry); return; }
    const chunks = []; req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => { entry.body = Buffer.concat(chunks).toString(); res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end(`answer:${req.url}:${entry.body}`); });
  });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  t.after(async () => { for (const socket of sockets) socket.destroy(); await new Promise(done => server.close(done)); });
  return { port: server.address().port, seen, server };
}
async function fixture(t, { mode = 2, runtimeDeps = {}, blockedPorts = [], ready } = {}) {
  const wss = new WebSocketServer({ host: '127.0.0.1', port: 0 }); await once(wss, 'listening');
  const channelId = nonce(), frames = [], peers = []; let peer;
  wss.on('connection', socket => {
    peer = socket; peers.push(socket); socket.on('error', () => {});
    socket.on('message', bytes => {
      const frame = JSON.parse(bytes.toString()); frames.push(frame);
      if (frame.type === 'auth') socket.send(JSON.stringify(ready ?? { type: 'ready', schema: 'soty.apps-channel.v1',
        ...(mode === 2 ? { bindingVersion: 2, channelId } : {}) }));
      if (frame.type === 'data') socket.send(JSON.stringify({ type: 'ack', id: frame.id, seq: frame.seq }));
    });
  });
  const runtime = createLocalAppsRuntime({ ...deps, ...runtimeDeps }, { serverUrl: `http://127.0.0.1:${wss.address().port}`,
    identity, token: 'synthetic-test-token', blockedPorts });
  t.after(async () => { runtime.stop(); for (const socket of peers) socket.terminate(); await new Promise(done => wss.close(done)); });
  runtime.start();
  await until(() => frames.some(frame => frame.type === 'auth'));
  if (!ready) await until(() => runtime.status().connected);
  const send = value => peer.send(JSON.stringify(value));
  const control = (type, value) => send({ type, channelId, ...value });
  const bind = async (value, syncId = nonce()) => {
    const from = frames.length; control('binding-set', { syncId, target: value });
    const reply = await until(() => frames.slice(from).find(frame => ['binding-ack', 'binding-rejected'].includes(frame.type) && frame.syncId === syncId));
    return { target: value, syncId, reply };
  };
  async function exchange(binding, { path = '/', method = 'GET', body } = {}) {
    const id = randomBytes(16).toString('hex');
    send({ type: mode === 2 ? 'bound-open' : 'open', ...(mode === 2 ? { channelId, syncId: binding.syncId, ...pins(binding.target) }
      : { appId: binding.target.appId }), id, kind: 'http', path, method, headers: {} });
    if (body) {
      send({ type: 'data', id, seq: 1, data: Buffer.from(body).toString('base64') });
      await until(() => frames.find(frame => frame.id === id && (frame.type === 'ack' || frame.type === 'cancel')));
    }
    send({ type: 'end', id });
    await until(() => frames.find(frame => frame.id === id && ['end', 'cancel'].includes(frame.type)));
    return { id, frames: frames.filter(frame => frame.id === id), body: frames.filter(frame => frame.id === id && frame.type === 'data')
      .map(frame => Buffer.from(frame.data, 'base64').toString()).join('') };
  }
  return { runtime, frames, channelId, send, control, bind, exchange, get peer() { return peer; } };
}

class FakeSocket {
  constructor() { this.readyState = 1; this.bufferedAmount = 0; this.listeners = new Map(); this.sent = []; }
  addEventListener(name, callback) { this.listeners.set(name, callback); }
  emit(name, event = {}) { this.listeners.get(name)?.(event); }
  send(value) { this.sent.push(JSON.parse(value)); }
  close() { this.readyState = 3; this.emit('close'); }
  frame(value) { this.emit('message', { data: JSON.stringify(value) }); }
}
function injected(t, { holdWrite = false, clock = Date.now, maxBufferedAmount } = {}) {
  const sockets = [], requests = [];
  const runtime = createLocalAppsRuntime({ ...deps, now: clock,
    createWebSocket: () => { const socket = new FakeSocket(); sockets.push(socket); return socket; },
    httpRequest: options => {
      const req = new EventEmitter(); req.options = options; req.destroyed = false; req.ended = false;
      req.end = () => { req.ended = true; }; req.destroy = () => { req.destroyed = true; };
      req.write = (_bytes, callback) => { if (holdWrite) req.finishWrite = callback; else callback(); };
      requests.push(req); return req;
    },
  }, { serverUrl: 'http://127.0.0.1:5301', identity, token: 'synthetic' });
  const start = (mode = 2) => {
    runtime.start(); const socket = sockets.at(-1), channelId = nonce(); socket.emit('open');
    socket.frame({ type: 'ready', schema: 'soty.apps-channel.v1', ...(mode === 2 ? { bindingVersion: 2, channelId } : {}) });
    if (maxBufferedAmount) socket.bufferedAmount = maxBufferedAmount;
    return { socket, channelId,
      set(value, syncId = nonce()) { socket.frame({ type: 'binding-set', channelId, syncId, target: value }); return { target: value, syncId }; },
      prepare(value, token = nonce()) { socket.frame({ type: 'target-prepare', channelId, nonce: token, target: value }); return token; } };
  };
  t.after(() => runtime.stop());
  return { runtime, sockets, requests, start };
}
const respond = (req, status = 200) => req.emit('response', { statusCode: status, headers: {}, destroy() {} });

test('legacy negotiation remains functional, and new auth advertises both binding versions', async t => {
  const app = await upstream(t), f = await fixture(t, { mode: 1 });
  assert.deepEqual(f.frames.find(frame => frame.type === 'auth').capabilities, { targetBindingVersions: [1, 2] });
  f.send({ type: 'sync', apps: [{ id: appId(1), port: app.port, entryPath: '/' }] });
  await until(() => f.frames.some(frame => frame.type === 'observation'));
  const result = await f.exchange({ target: target(1, app.port) }, { path: '/old', method: 'POST', body: 'kept' });
  assert.equal(result.body, 'answer:/old:kept');
});

test('real 100-frame parser burst installs and ACKs all maximum-path bindings without outstanding-promise overflow', async t => {
  const app = await upstream(t), f = await fixture(t), entryPath = '/' + 'x'.repeat(8191);
  for (let i = 1; i <= 100; i++) f.control('binding-set', { syncId: nonce(), target: target(i, app.port, entryPath) });
  await until(() => f.frames.filter(frame => frame.type === 'binding-ack').length === 100, 5000);
  assert.equal(f.runtime.status().connected, true); assert.equal(f.runtime.status().apps, 100);
  assert.equal(f.frames.filter(frame => frame.type === 'binding-rejected').length, 0);
});

test('binding ACK precedes held HEAD; exact opens relay real bytes and mismatched pins never reach HTTP', async t => {
  let held;
  const app = await upstream(t, (req, res) => { if (req.method === 'HEAD') { held = res; return; } res.end('visible'); });
  const f = await fixture(t), bound = await f.bind(target(1, app.port));
  assert.equal(bound.reply.type, 'binding-ack'); await until(() => held);
  assert.equal(f.frames.some(frame => frame.type === 'bound-observation'), false);
  assert.equal((await f.exchange(bound)).body, 'visible');
  const before = app.seen.filter(entry => entry.method === 'GET').length;
  for (const extra of [{ syncId: nonce() }, { revision: 2 }, { digest: '0'.repeat(64) }, { profile: 'future' }]) {
    const id = randomBytes(16).toString('hex');
    f.control('bound-open', { syncId: bound.syncId, ...pins(bound.target), id, kind: 'http', path: '/', method: 'GET', headers: {}, ...extra });
    await until(() => f.frames.some(frame => frame.id === id && frame.type === 'cancel'));
  }
  assert.equal(app.seen.filter(entry => entry.method === 'GET').length, before); assert.equal(f.runtime.status().connected, true);
  held.end();
});

test('source A→B keeps exact routing; conditional old removal and unrelated sync preserve the new binding', async t => {
  const a = await upstream(t), b = await upstream(t), f = await fixture(t);
  const old = await f.bind(target(1, a.port)), neighbour = await f.bind(target(2, a.port));
  const latest = await f.bind(target(1, b.port, '/', 2));
  f.control('binding-remove', { bindings: [{ appId: old.target.appId, syncId: old.syncId }] });
  const oldResult = await f.exchange(old); assert.equal(oldResult.frames.some(frame => frame.type === 'cancel'), true);
  assert.equal((await f.exchange(latest, { path: '/new' })).body, 'answer:/new:');
  assert.equal((await f.exchange(neighbour, { path: '/neighbour' })).body, 'answer:/neighbour:');
  assert.equal(b.seen.filter(entry => entry.method === 'GET').length, 1);
  const repeated = await f.bind(latest.target, latest.syncId); assert.equal(repeated.reply.type, 'binding-ack');
  assert.equal(f.runtime.status().apps, 2);
});

test('local binding policy refusal removes only that app and never falls back to its former port', async t => {
  const app = await upstream(t), f = await fixture(t);
  const before = await f.bind(target(1, app.port)), neighbour = await f.bind(target(2, app.port));
  const rejected = await f.bind(target(1, 49424, '/', 2));
  assert.equal(rejected.reply.error, 'invalid_app_port'); assert.equal(f.runtime.status().apps, 1);
  assert.equal((await f.exchange(before)).frames.some(frame => frame.type === 'cancel'), true);
  assert.equal((await f.exchange(neighbour)).body, 'answer:/:'); assert.equal(f.runtime.status().connected, true);
});

test('digest mismatch and same-syncId different valid target are protocol faults before untrusted routing', async t => {
  for (const attack of ['digest', 'reused-sync']) {
    const f = injected(t), channel = f.start(), original = target(1, 8200);
    const bound = channel.set(original); await nextTurn(); const requests = f.requests.length;
    const changed = target(1, 8300, '/', 2);
    if (attack === 'digest') changed.digest = original.digest;
    channel.socket.frame({ type: 'binding-set', channelId: channel.channelId,
      syncId: attack === 'digest' ? nonce() : bound.syncId, target: changed });
    assert.equal(f.runtime.status().connected, false); assert.equal(f.requests.length, requests);
  }
});

test('fixed digest vector and tampered target fields use captured connector identity before any request', async t => {
  const original = target(1, 8200, '/board?tag=a%2Bb#item');
  assert.equal(original.digest, 'a1f77ebbf9fec96f6aa7fea2e96718de2bf41e98decaa9aaa34c0371e0eb48f4');
  for (const patch of [{ appId: appId(2) }, { port: 8300 }, { revision: 2 }, { ownerAccountId: 'other_owner' },
    { entryPath: '/changed' }, { profile: 'future' },
    { digest: runtimeTargetDigest({ ...original, connectorKey: 'binding_link|other_host|binding_connector' }) }]) {
    const f = injected(t), channel = f.start(); channel.set({ ...original, ...patch });
    assert.equal(f.runtime.status().connected, false); assert.equal(f.requests.length, 0);
  }
  const f = injected(t), channel = f.start(); channel.set(original); await nextTurn();
  assert.equal(f.runtime.status().apps, 1); assert.equal(f.requests[0].options.path, '/board?tag=a%2Bb');
});

test('bound WebSocket uses exact real upstream; changing one app closes only its socket', async t => {
  const app = await upstream(t), f = await fixture(t), wss = new WebSocketServer({ noServer: true });
  const upgrades = [], messages = [];
  app.server.on('upgrade', (req, socket, head) => {
    upgrades.push(req.url); wss.handleUpgrade(req, socket, head, ws => {
      ws.on('error', () => {}); ws.on('message', bytes => { messages.push(bytes.toString()); ws.send(`echo:${bytes}`); });
    });
  });
  t.after(() => { for (const ws of wss.clients) ws.terminate(); wss.close(); });
  const a = await f.bind(target(1, app.port)), b = await f.bind(target(2, app.port));
  async function open(binding, path) {
    const id = randomBytes(16).toString('hex');
    f.control('bound-open', { syncId: binding.syncId, ...pins(binding.target), id, kind: 'ws', path, method: 'GET',
      headers: { 'sec-websocket-key': randomBytes(16).toString('base64'), 'sec-websocket-version': '13' } });
    const head = await until(() => f.frames.find(frame => frame.id === id && ['head', 'cancel'].includes(frame.type)));
    assert.equal(head.status, 101); return id;
  }
  function sendText(id, text) {
    const payload = Buffer.from(text), mask = Buffer.from([1, 2, 3, 4]);
    const masked = Buffer.from(payload.map((value, index) => value ^ mask[index % 4]));
    const data = Buffer.concat([Buffer.from([0x81, 0x80 | payload.length]), mask, masked]);
    f.send({ type: 'data', id, seq: 1, data: data.toString('base64') });
  }
  const first = await open(a, '/сокет?tag=a%2Bb#item'), second = await open(b, '/neighbour');
  assert.deepEqual(upgrades, ['/%D1%81%D0%BE%D0%BA%D0%B5%D1%82?tag=a%2Bb', '/neighbour']);
  sendText(first, 'first'); await until(() => f.frames.some(frame => frame.id === first && frame.type === 'data'));
  const reply = Buffer.concat(f.frames.filter(frame => frame.id === first && frame.type === 'data').map(frame => Buffer.from(frame.data, 'base64')));
  assert.equal(reply.subarray(2).toString(), 'echo:first');
  await f.bind(target(1, 49424, '/', 2));
  await until(() => f.frames.some(frame => frame.type === 'cancel' && frame.id === first));
  assert.equal(f.runtime.status().streams, 1); sendText(second, 'kept');
  await until(() => messages.includes('kept'));
  await until(() => f.frames.some(frame => frame.id === second && frame.type === 'data'));
  assert.equal(f.frames.some(frame => frame.type === 'cancel' && frame.id === second), false);
});

test('wrong channel, future negotiation and legacy control in v2 fail closed', async t => {
  for (const fault of ['channel', 'legacy', 'future']) {
    if (fault === 'future') {
      const f = await fixture(t, { ready: { type: 'ready', schema: 'soty.apps-channel.v1', bindingVersion: 3, channelId: nonce() } });
      await until(() => f.peer.readyState === 3); assert.equal(f.runtime.status().connected, false);
    } else {
      const f = injected(t), channel = f.start();
      channel.socket.frame(fault === 'legacy' ? { type: 'sync', apps: [] }
        : { type: 'binding-set', channelId: nonce(), syncId: nonce(), target: target(1, 8200) });
      assert.equal(f.runtime.status().connected, false); assert.equal(f.requests.length, 0);
    }
  }
});

test('R2 old HEAD, old socket messages and late close cannot affect a replacement connection', async t => {
  const f = injected(t), first = f.start(), old = first.set(target(1, 8200)); await nextTurn();
  const oldHead = f.requests[0]; f.runtime.stop();
  const second = f.start(), fresh = second.set(target(1, 8300, '/', 2)); await nextTurn();
  respond(oldHead); first.socket.frame({ type: 'binding-remove', channelId: first.channelId, bindings: [{ appId: old.target.appId, syncId: old.syncId }] });
  first.socket.emit('open'); first.socket.emit('close'); await nextTurn();
  assert.equal(second.socket.sent.filter(frame => frame.type === 'bound-observation').length, 0);
  assert.equal(f.runtime.status().connected, true); assert.equal(f.runtime.status().apps, 1);
  respond(f.requests[1]); await nextTurn();
  const observation = second.socket.sent.find(frame => frame.type === 'bound-observation');
  assert.equal(observation.syncId, fresh.syncId); assert.equal(observation.digest, fresh.target.digest);
});

test('same-connection binding change cancels old HEAD and ignores its late answer', async t => {
  const f = injected(t), channel = f.start(); channel.set(target(1, 8200)); await nextTurn();
  const firstHead = f.requests[0], fresh = channel.set(target(1, 8300, '/', 2)); await nextTurn();
  assert.equal(firstHead.destroyed, true); respond(firstHead); await nextTurn();
  assert.equal(channel.socket.sent.filter(frame => frame.type === 'bound-observation').length, 0);
  respond(f.requests[1]); await nextTurn();
  assert.equal(channel.socket.sent.find(frame => frame.type === 'bound-observation').syncId, fresh.syncId);
});

test('a held write callback from old socket never ACKs or deletes the replacement stream with the same ID', async t => {
  const f = injected(t, { holdWrite: true }), first = f.start(), binding = first.set(target(1, 8200));
  const id = 'a'.repeat(32), open = (channel, value) => channel.socket.frame({ type: 'bound-open', channelId: channel.channelId,
    syncId: value.syncId, ...pins(value.target), id, kind: 'http', method: 'POST', path: '/', headers: {} });
  open(first, binding); const oldRequest = f.requests.find(req => req.options.method === 'POST');
  first.socket.frame({ type: 'data', id, seq: 1, data: Buffer.from('old').toString('base64') });
  f.runtime.stop(); const second = f.start(), next = second.set(target(1, 8300, '/', 2)); open(second, next);
  oldRequest.finishWrite(); await nextTurn();
  assert.equal(second.socket.sent.some(frame => frame.type === 'ack' && frame.id === id), false);
  assert.equal(f.runtime.status().streams, 1); assert.equal(f.runtime.status().connected, true);
});

test('late response, upgrade, request error and ACK from old connection never disturb the replacement stream', async t => {
  const f = injected(t), first = f.start(), binding = first.set(target(1, 8200)), id = 'b'.repeat(32);
  const open = (channel, value) => channel.socket.frame({ type: 'bound-open', channelId: channel.channelId,
    syncId: value.syncId, ...pins(value.target), id, kind: 'http', method: 'GET', path: '/', headers: {} });
  open(first, binding); const oldRequest = f.requests.find(req => req.options.method === 'GET');
  f.runtime.stop(); const second = f.start(), latest = second.set(target(1, 8300, '/', 2)); open(second, latest);
  const before = second.socket.sent.length, discarded = [];
  oldRequest.emit('response', { statusCode: 200, headers: {}, destroy: () => discarded.push('response') });
  oldRequest.emit('upgrade', { statusCode: 101, headers: {} }, { destroy: () => discarded.push('socket') }, Buffer.alloc(0));
  oldRequest.emit('error', new Error('late transport error'));
  first.socket.frame({ type: 'ack', id, seq: 1 }); first.socket.emit('close'); await nextTurn();
  assert.deepEqual(discarded, ['response', 'socket']); assert.equal(second.socket.sent.length, before);
  assert.equal(f.runtime.status().streams, 1); assert.equal(f.runtime.status().connected, true);
});

test('prepare is transient, duplicate nonce reuses reply and does not restart HEAD or replace active binding', async t => {
  const app = await upstream(t), candidate = await upstream(t), f = await fixture(t), bound = await f.bind(target(1, app.port));
  const value = target(1, candidate.port, '/candidate', 2), token = nonce();
  f.control('target-prepare', { nonce: token, target: value });
  const response = await until(() => f.frames.find(frame => frame.type === 'target-prepared' && frame.nonce === token));
  assert.equal(response.state, 'responding'); assert.equal(response.httpStatus, 200); assert.equal(f.runtime.status().apps, 1);
  f.control('target-prepare', { nonce: token, target: value });
  await until(() => f.frames.filter(frame => frame.type === 'target-prepared' && frame.nonce === token).length === 2);
  assert.equal(candidate.seen.length, 1); assert.equal((await f.exchange(bound)).body, 'answer:/:');
  const fakeBinding = { target: value, syncId: nonce() };
  assert.equal((await f.exchange(fakeBinding)).frames.some(frame => frame.type === 'cancel'), true);
});

test('prepare limits include in-flight work: four/channel, two/app, without queues or disruption to installed app', async t => {
  const f = injected(t), channel = f.start();
  const values = [target(1, 8200), target(1, 8201, '/', 2), target(2, 8202), target(3, 8203)];
  const tokens = values.map(value => channel.prepare(value));
  assert.equal(f.requests.length, 4);
  const busy = channel.prepare(target(4, 8204));
  assert.equal(channel.socket.sent.find(frame => frame.nonce === busy).error, 'app_prepare_busy');
  channel.prepare(values[0], tokens[0]); assert.equal(f.requests.length, 4);
  for (const req of f.requests) respond(req); await nextTurn();
  assert.equal(channel.socket.sent.filter(frame => frame.type === 'target-prepared').length, 4);
  const thirdSame = injected(t), c = thirdSame.start(); c.prepare(target(1, 8210)); c.prepare(target(1, 8211, '/', 2));
  const refused = c.prepare(target(1, 8212, '/', 3));
  assert.equal(c.socket.sent.find(frame => frame.nonce === refused).error, 'app_prepare_busy'); assert.equal(thirdSame.requests.length, 2);
});

test('HEAD uses browser-equivalent serialized paths and excludes credentials; unsafe or overlong serialized candidates never reach HTTP', async t => {
  const app = await upstream(t), f = await fixture(t);
  const paths = ['/страница?ключ=чай#раздел', '/docs/../board?tag=a%2Bb#item', '/%2e%2e/board?tag=%23one', '/#/dashboard', '/board?tag=a%2Bb&x=%2F#item'];
  for (let i = 0; i < paths.length; i++) {
    const token = nonce(); f.control('target-prepare', { nonce: token, target: target(i + 1, app.port, paths[i]) });
    const reply = await until(() => f.frames.find(frame => frame.nonce === token)); assert.equal(reply.state, 'responding');
    const expected = new URL(paths[i], `http://127.0.0.1:${app.port}`);
    assert.equal(app.seen[i].url, expected.pathname + expected.search);
    assert.equal(app.seen[i].headers.authorization, undefined); assert.equal(app.seen[i].headers.cookie, undefined);
  }
  const before = app.seen.length;
  for (const path of ['/x/..//double', '/%2F_soty/session', '/%2e/_soty/session', '/' + 'я'.repeat(2000)]) {
    const token = nonce(); f.control('target-prepare', { nonce: token, target: target(10, app.port, path) });
    const reply = await until(() => f.frames.find(frame => frame.nonce === token)); assert.equal(reply.type, 'target-rejected'); assert.equal(reply.error, 'invalid_app_path');
  }
  assert.equal(app.seen.length, before); assert.equal(f.runtime.status().connected, true);
});

test('HEAD classifies 200–499 as a response, 500 as unreachable, and does not follow redirect', async t => {
  const app = await upstream(t, (req, res) => { const status = Number(req.url.slice(1)); res.writeHead(status, status === 302 ? { Location: 'https://example.test/private' } : {}); res.end(); });
  const f = await fixture(t);
  for (const status of [200, 401, 404, 302, 500]) {
    const token = nonce(); f.control('target-prepare', { nonce: token, target: target(status, app.port, `/${status}`) });
    const reply = await until(() => f.frames.find(frame => frame.nonce === token));
    assert.equal(reply.state, status < 500 ? 'responding' : 'unreachable'); assert.equal(reply.httpStatus, status);
  }
  assert.equal(app.seen.length, 5);
});

test('prepare timeout and disconnect cancel the HTTP request; late response cannot become a new-channel proof', { timeout: 8000 }, async t => {
  const f = injected(t), channel = f.start(), token = channel.prepare(target(1, 8200));
  await until(() => channel.socket.sent.some(frame => frame.nonce === token), 4500);
  const reply = channel.socket.sent.find(frame => frame.nonce === token);
  assert.equal(reply.state, 'unreachable'); assert.equal(reply.httpStatus, null); assert.equal(f.requests[0].destroyed, true);
  const second = channel.prepare(target(2, 8201)); const requestBeforeStop = f.requests.at(-1);
  f.runtime.stop(); const fresh = f.start(); respond(requestBeforeStop); await nextTurn();
  assert.equal(fresh.socket.sent.some(frame => frame.nonce === second), false); assert.equal(requestBeforeStop.destroyed, true);
});

test('completed nonce cache is bounded independently of HEAD slots and expires without evicting a live nonce', async t => {
  let timestamp = 1000;
  const f = injected(t, { clock: () => timestamp }), channel = f.start();
  for (let i = 0; i < 256; i++) {
    channel.prepare(target(1, 8200)); respond(f.requests.at(-1)); await nextTurn();
  }
  assert.equal(f.requests.length, 256);
  const busy = channel.prepare(target(2, 8200)); assert.equal(channel.socket.sent.find(frame => frame.nonce === busy).error, 'app_prepare_busy');
  assert.equal(f.requests.length, 256);
  timestamp += 30_001; channel.prepare(target(2, 8200)); assert.equal(f.requests.length, 257);
});

test('wrong payload reused under an existing prepare nonce is rejected as a protocol fault', async t => {
  const f = injected(t), channel = f.start(), token = channel.prepare(target(1, 8200));
  channel.prepare(target(1, 8300), token);
  assert.equal(f.runtime.status().connected, false); assert.equal(f.requests.length, 1); assert.equal(f.requests[0].destroyed, true);
});

test('v2 isolated policy errors include unsupported profiles and blocked candidate ports', async t => {
  const f = injected(t), channel = f.start();
  const blocked = channel.prepare(target(1, 49424));
  assert.equal(channel.socket.sent.find(frame => frame.nonce === blocked).error, 'invalid_app_port');
  const future = channel.prepare(target(2, 8200, '/', 1, { profile: 'future.profile.v9' }));
  assert.equal(channel.socket.sent.find(frame => frame.nonce === future).error, 'unsupported_profile');
  assert.equal(f.requests.length, 0); assert.equal(f.runtime.status().connected, true);
});

test('periodic probe admission is bounded to four and removes stale queued bindings', async t => {
  const f = injected(t), channel = f.start(), bindings = [];
  for (let i = 1; i <= 20; i++) bindings.push(channel.set(target(i, 8200 + i)));
  await nextTurn(); assert.equal(f.requests.length, 4);
  channel.socket.frame({ type: 'binding-remove', channelId: channel.channelId,
    bindings: bindings.slice(4).map(binding => ({ appId: binding.target.appId, syncId: binding.syncId })) });
  for (const req of f.requests) respond(req); await nextTurn();
  assert.equal(f.requests.length, 4); assert.equal(f.runtime.status().apps, 4);
});

test('UTF-8 frame limit and outbound backpressure fail closed without silent truncation', async t => {
  const f = injected(t), channel = f.start();
  const frame = { type: 'target-prepare', channelId: channel.channelId, nonce: nonce(), target: target(1, 8200, '/' + 'я'.repeat(38_000)) };
  assert.ok(JSON.stringify(frame).length < 72 * 1024); assert.ok(Buffer.byteLength(JSON.stringify(frame)) > 72 * 1024);
  channel.socket.frame(frame); assert.equal(f.runtime.status().connected, false); assert.equal(f.requests.length, 0);
  const blocked = injected(t, { maxBufferedAmount: 4 * 1024 * 1024 }), c = blocked.start(); c.set(target(1, 8200));
  assert.equal(blocked.runtime.status().connected, false);
});

test('standalone proposal probe keeps boolean API while fixing Unicode and fragment HTTP serialization', async t => {
  const app = await upstream(t);
  assert.equal(await probeLocalApp(deps, { port: app.port, entryPath: '/страница?tag=%2B#part' }), true);
  assert.equal(app.seen[0].url, '/%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0?tag=%2B');
  assert.equal(await probeLocalApp(deps, { port: app.port, entryPath: '/_soty/session' }), false);
  assert.equal(app.seen.length, 1);
});
