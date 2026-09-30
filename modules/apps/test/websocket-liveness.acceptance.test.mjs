import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import WebSocket, { WebSocketServer } from 'ws';
import { createAppsService } from '../server/index.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';

// Independent real gateway/connector/upstream fixture. The connector source is
// unchanged; only explicitly named fault cases delay a packet or write callback.
// These hooks do not establish physical network-loss or browser-RSS guarantees.
const owner = { accountId: 'liveness_independent_owner', deviceId: 'owner_device' };
const identity = { linkId: 'liveness_independent_link', hostDeviceId: 'source_host', connectorId: 'source_connector' };
const FAST = Object.freeze({ quietMs: 250, insertMs: 900, responseMs: 750, frameMs: 900 });
const wait = ms => new Promise(done => setTimeout(done, ms));
const secret = () => randomBytes(32).toString('base64url');
const digest = value => createHash('sha256').update(value).digest('hex');
async function until(check, label, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { const value = await check(); if (value) return value; await wait(10); }
  throw new Error(`liveness_acceptance_timeout:${label}`);
}
function bounded(promise, label, timeout = 5000) {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`liveness_acceptance_timeout:${label}`)), timeout); })])
    .finally(() => clearTimeout(timer));
}
function nextMessage(ws) {
  return bounded(new Promise((done, reject) => {
    const cleanup = () => { ws.off('message', received); ws.off('error', failed); ws.off('close', ended); };
    const received = data => { cleanup(); done(Buffer.from(data)); };
    const failed = error => { cleanup(); reject(error); };
    const ended = () => { cleanup(); reject(new Error('message_connection_closed')); };
    ws.once('message', received); ws.once('error', failed); ws.once('close', ended);
  }), 'message');
}
const closed = ws => ws.readyState === WebSocket.CLOSED ? Promise.resolve() : bounded(new Promise(done => ws.once('close', done)), 'closed');
const send = (ws, bytes, options = {}) => bounded(new Promise((done, reject) => ws.send(bytes, options, error => error ? reject(error) : done())), 'send');

function encodeFrame(opcode, payload = Buffer.alloc(0), { masked = false, fin = true } = {}) {
  payload = Buffer.from(payload);
  const extra = payload.length < 126 ? 0 : payload.length < 65536 ? 2 : 8;
  const header = Buffer.alloc(2 + extra + (masked ? 4 : 0));
  header[0] = (fin ? 128 : 0) | opcode;
  header[1] = (masked ? 128 : 0) | (extra === 0 ? payload.length : extra === 2 ? 126 : 127);
  if (extra === 2) header.writeUInt16BE(payload.length, 2);
  if (extra === 8) header.writeBigUInt64BE(BigInt(payload.length), 2);
  if (!masked) return Buffer.concat([header, payload]);
  const mask = randomBytes(4); mask.copy(header, 2 + extra);
  const body = Buffer.from(payload); for (let i = 0; i < body.length; i++) body[i] ^= mask[i & 3];
  return Buffer.concat([header, body]);
}

// This small independent decoder is used only by the deliberately non-closing
// raw upstream. Ordinary frames are decoded by the installed ws implementation.
function rawFrames(callback) {
  let rest = Buffer.alloc(0);
  return bytes => {
    rest = Buffer.concat([rest, bytes]);
    assert.ok(rest.length <= 2 * 1024 * 1024, 'raw fixture input remains bounded');
    while (rest.length >= 2) {
      const encoded = rest[1] & 127, extra = encoded === 126 ? 2 : encoded === 127 ? 8 : 0;
      const masked = Boolean(rest[1] & 128), offset = 2 + extra + (masked ? 4 : 0);
      if (rest.length < offset) return;
      const length = extra === 2 ? rest.readUInt16BE(2) : extra === 8 ? Number(rest.readBigUInt64BE(2)) : encoded;
      assert.ok(length <= 1024 * 1024, 'raw fixture frame limit');
      if (rest.length < offset + length) return;
      const payload = Buffer.from(rest.subarray(offset, offset + length));
      if (masked) for (let i = 0; i < payload.length; i++) payload[i] ^= rest[2 + extra + (i & 3)];
      const frame = { opcode: rest[0] & 15, fin: Boolean(rest[0] & 128), masked, payload };
      rest = Buffer.from(rest.subarray(offset + length)); callback(frame);
    }
  };
}

async function createUpstream(label, { autoPong = true, echo = true } = {}) {
  const sockets = new Set(), peers = new Map(), raw = new Map();
  const wss = new WebSocketServer({ noServer: true, autoPong, perMessageDeflate: false });
  const server = http.createServer((_req, res) => { res.writeHead(200); res.end(label); });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => {
    if (req.url === '/raw-ignore-close') {
      const accept = createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
      socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
      const record = { socket, frames: [], timer: null }; raw.set(req.url, record);
      const read = rawFrames(frame => {
        record.frames.push({ opcode: frame.opcode, masked: frame.masked });
        if (frame.opcode === 9) socket.write(encodeFrame(10, frame.payload));
        if (frame.opcode === 8 && !record.timer) {
          record.timer = setInterval(() => { if (!socket.destroyed) socket.write(encodeFrame(10, 'still-sending-but-not-closing')); }, 30);
        }
      });
      socket.on('data', read); socket.once('close', () => clearInterval(record.timer)); socket.on('error', () => {});
      if (head.length) read(head); return;
    }
    wss.handleUpgrade(req, socket, head, ws => {
      const record = { ws, messages: [], messageCount: 0, pings: [], pongs: [] };
      peers.set(req.url, record); ws.on('error', () => {});
      ws.on('ping', data => { if (record.pings.length < 128) record.pings.push(Buffer.from(data)); });
      ws.on('pong', data => { if (record.pongs.length < 128) record.pongs.push(Buffer.from(data)); });
      ws.on('message', (data, binary) => {
        record.messageCount++; if (record.messages.length < 64) record.messages.push(Buffer.from(data));
        if (echo) ws.send(data, { binary });
      });
    });
  });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  return { port: server.address().port, peers, raw,
    async close() { for (const item of raw.values()) clearInterval(item.timer); for (const ws of wss.clients) ws.terminate();
      for (const socket of sockets) socket.destroy(); wss.close(); await new Promise(done => server.close(done)); },
  };
}

function wire() {
  const log = [], hooks = { toGateway: null, toConnector: null };
  const record = (direction, frame) => {
    if (log.length < 8192) log.push({ direction, type: frame.type, id: frame.id, seq: frame.seq, error: frame.error,
      path: frame.path, bytes: frame.type === 'data' ? Buffer.from(frame.data, 'base64').length : 0 });
  };
  class Socket extends globalThis.WebSocket {
    constructor(url) { super(url); this.handlers = new Map(); }
    send(data) {
      const frame = JSON.parse(data), packet = { frame, forward: () => { record('toGateway', frame); super.send(data); } };
      if (hooks.toGateway?.(packet) !== false) packet.forward();
    }
    addEventListener(type, listener, options) {
      if (type !== 'message') return super.addEventListener(type, listener, options);
      const wrapped = event => {
        const frame = JSON.parse(event.data), packet = { frame, forward: () => { record('toConnector', frame); listener.call(this, event); } };
        if (hooks.toConnector?.(packet) !== false) packet.forward();
      };
      this.handlers.set(listener, wrapped); return super.addEventListener(type, wrapped, options);
    }
    removeEventListener(type, listener, options) { return super.removeEventListener(type, this.handlers.get(listener) || listener, options); }
  }
  return { Socket, log, hooks };
}

async function fixture(t, { timing = FAST, source = {}, defaultTimers = false } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-ws-liveness-independent-'));
  const clients = new Set(), gatewaySockets = new Set(), socketsByPath = new Map(), headerHolds = new Map(), cleanups = [];
  const A = await createUpstream('A', source), B = await createUpstream('B');
  let service, runtime, policyOffset = 0, authorityChecks = 0;
  const gateway = http.createServer((req, res) => { if (!service?.handleRequest(req, res)) { res.writeHead(404); res.end(); } });
  gateway.on('connection', socket => { gatewaySockets.add(socket); socket.once('close', () => gatewaySockets.delete(socket)); });
  gateway.on('upgrade', (req, socket, head) => {
    headerHolds.get(req.url)?.attach(socket);
    socketsByPath.set(req.url, socket); if (!service?.handleUpgrade(req, socket, head)) socket.destroy();
  });
  t.after(async () => {
    for (const cleanup of cleanups) cleanup(); for (const client of clients) client.terminate(); runtime?.stop(); service?.close();
    for (const socket of gatewaySockets) socket.destroy(); await new Promise(done => gateway.close(done));
    await A.close(); await B.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(directory.split(/[\\/]/u).at(-1), /^soty-ws-liveness-independent-/u);
    await rm(directory, { recursive: true, force: true });
  });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done));
  const port = gateway.address().port, token = secret(), gate = wire();
  service = createAppsService({ databasePath: join(directory, 'apps.sqlite'), shellOrigins: [`http://localhost:${port}`],
    appOriginTemplate: `http://{appId}.legacy.localhost:${port}`, namedAppZone: `http://named.localhost:${port}`,
    actorActive: actor => { authorityChecks++; return actor?.accountId === owner.accountId && actor.deviceId === owner.deviceId; },
    authenticateConnector: async value => value.linkId === identity.linkId && value.deviceId === identity.hostDeviceId && value.connectorId === identity.connectorId && value.token === token,
    now: () => Date.now() + policyOffset, accessAuditMs: 25, ...(defaultTimers ? {} : { webSocketLiveness: timing }) });
  const call = (op, args = {}) => service.execute({ op, args: { ...args, expectedAccountId: owner.accountId }, actor: owner });
  runtime = createLocalAppsRuntime({ createWebSocket: url => new gate.Socket(url), httpRequest: http.request,
    randomSecret: secret, digest, encodeBase64: value => Buffer.from(value).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
  { serverUrl: `http://127.0.0.1:${port}`, identity, token });
  runtime.start(); await until(() => runtime.status().connected, 'actual connector ready');
  const claim = await runtime.claim(); call('apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode: claim.claimCode });
  const register = (upstream, name) => call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name, port: upstream.port, entryPath: '/' }).app;
  const app = register(A, 'Liveness acceptance'), neighbor = register(B, 'Independent neighbor');
  const named = call('apps.domains.claim', { appId: app.id, slug: 'liveness', requestId: 'name-liveness', expectedDomainsRevision: 0 });
  const publication = call('apps.publication.get', { appId: app.id });
  call('apps.publication.update', { appId: app.id, requestId: 'publish-liveness', expectedPolicyEpoch: publication.policyEpoch,
    expectedTargetRevision: publication.activeTargetRevision, launchPolicy: 'anyone', listed: false, activeDomainIds: [named.receipt.domainId],
    exposureAck: { scope: 'whole-port', targetRevision: publication.target.revision, targetDigest: publication.target.digest, profile: publication.target.profile } });
  async function session(id) {
    const launch = await until(() => { try { return call('apps.launch', { appId: id }); } catch (error) {
      if (['app_binding_pending', 'app_offline'].includes(error.code)) return false; throw error;
    } }, 'source binding');
    const url = new URL(launch.launchUrl), reply = await request(port, url.origin, '/_soty/session', {
      method: 'POST', headers: { origin: url.origin }, body: JSON.stringify({ ticket: url.hash.slice(1) }) });
    assert.equal(reply.status, 200); return { origin: url.origin, cookie: reply.headers['set-cookie'][0].split(';')[0] };
  }
  const privateEntry = await session(app.id), neighborEntry = await session(neighbor.id), publicEntry = { origin: named.receipt.origin };
  async function open(path = '/echo', { entry = privateEntry, autoPong = true } = {}) {
    const ws = new WebSocket(`ws://127.0.0.1:${port}${path}`, { autoPong, perMessageDeflate: false,
      headers: { host: new URL(entry.origin).host, origin: entry.origin, ...(entry.cookie ? { cookie: entry.cookie } : {}) } });
    clients.add(ws); ws.on('error', () => {}); ws.once('close', () => clients.delete(ws));
    const pings = [], pongs = [];
    ws.on('ping', data => { if (pings.length < 128) pings.push(Buffer.from(data)); });
    ws.on('pong', data => { if (pongs.length < 128) pongs.push(Buffer.from(data)); });
    await bounded(new Promise((done, reject) => {
      ws.once('open', done); ws.once('error', reject);
      ws.once('unexpected-response', (_req, res) => { res.resume(); ws.terminate(); reject(Object.assign(new Error('upgrade_refused'), { status: res.statusCode })); });
    }), 'upgrade');
    return { ws, pings, pongs };
  }
  function holdClientWrites(path) {
    const socket = socketsByPath.get(path), original = socket.write, held = [];
    assert.ok(socket, 'real gateway socket exists');
    socket.write = function (...args) {
      const at = args.length - 1, callback = args[at];
      if (typeof callback === 'function') args[at] = error => { if (error) callback(error); else held.push(() => callback()); };
      return original.apply(this, args);
    };
    const restore = () => { socket.write = original; for (const resume of held.splice(0)) resume(); };
    cleanups.push(restore); return { held, restore };
  }
  function holdUpgrade(path) {
    const held = []; let restore = () => {};
    headerHolds.set(path, { attach(socket) {
      const original = socket.write;
      socket.write = function (...args) {
        const at = args.length - 1, callback = args[at];
        if (typeof callback === 'function' && Buffer.from(args[0]).subarray(0, 12).toString() === 'HTTP/1.1 101') {
          args[at] = error => { if (error) callback(error); else held.push(() => callback()); };
        }
        return original.apply(this, args);
      };
      restore = () => { socket.write = original; for (const resume of held.splice(0)) resume(); };
    } });
    cleanups.push(() => restore()); return { held, restore: () => restore() };
  }
  return { A, B, gate, service, runtime, call, app, neighbor, open, session, privateEntry, neighborEntry, publicEntry, holdClientWrites, holdUpgrade,
    advance: ms => { policyOffset += ms; }, authorityChecks: () => authorityChecks,
    http: (path, options) => request(port, privateEntry.origin, path, { cookie: privateEntry.cookie, ...options }) };
}
function request(port, origin, path, { method = 'GET', headers = {}, cookie, body } = {}) {
  return bounded(new Promise((done, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path, method, agent: false,
      headers: { host: new URL(origin).host, ...headers, ...(cookie ? { cookie } : {}) } }, res => {
      const chunks = []; res.on('error', reject); res.on('data', data => chunks.push(data));
      res.on('end', () => done({ status: res.statusCode, headers: res.headers, text: Buffer.concat(chunks).toString() }));
    }); req.on('error', reject); req.end(body);
  }), 'HTTP');
}
async function echoed(ws, value = 'still connected') {
  const reply = nextMessage(ws); await send(ws, value); assert.deepEqual(await reply, Buffer.from(value));
}

test('R3 default timers: an actual connector and two silent endpoints survive 130 seconds and then exchange data', {
  timeout: 160_000, skip: process.env.SOTY_WS_LONG_TEST !== '1',
}, async t => {
  const f = await fixture(t, { defaultTimers: true }), client = await f.open('/default-idle');
  const peer = f.A.peers.get('/default-idle'), start = Date.now();
  for (let n = 0; n < 13; n++) { await wait(10_000); assert.equal(client.ws.readyState, WebSocket.OPEN); assert.equal(f.runtime.status().connected, true); }
  assert.ok(Date.now() - start >= 130_000);
  assert.ok(client.pings.length >= 3 && peer.pings.length >= 3, 'both actual inner endpoints received probes');
  assert.equal(client.pongs.length, 0); assert.equal(peer.pongs.length, 0, 'gateway controls did not escape to the opposite peer');
  await echoed(client.ws, 'after real default idle');
  const message = nextMessage(client.ws); await send(peer.ws, 'source remains usable'); assert.equal((await message).toString(), 'source remains usable');
});

test('R3 one-way browser and one-way source data do not require application heartbeat or reset the other endpoint liveness', { timeout: 15_000 }, async t => {
  const f = await fixture(t, { source: { echo: false } });
  const up = await f.open('/browser-only'), down = await f.open('/source-only');
  const upPeer = f.A.peers.get('/browser-only'), downPeer = f.A.peers.get('/source-only');
  for (let n = 0; n < 16; n++) { await send(up.ws, `browser-${n}`); await send(downPeer.ws, `source-${n}`); await wait(90); }
  assert.equal(up.ws.readyState, WebSocket.OPEN); assert.equal(down.ws.readyState, WebSocket.OPEN);
  assert.equal(upPeer.messageCount, 16);
  assert.ok(upPeer.pings.length > 0, 'browser writes alone do not attest source liveness');
  assert.ok(down.pings.length > 0, 'source writes alone do not attest browser liveness');
});

test('R3 a dead browser is removed while the real source, outer channel and neighboring app remain usable', { timeout: 12_000 }, async t => {
  const f = await fixture(t), dead = await f.open('/dead-browser', { autoPong: false });
  const neighbor = await f.open('/neighbor-browser', { entry: f.neighborEntry });
  await closed(dead.ws); assert.ok(dead.pings.length > 0); assert.equal(f.runtime.status().connected, true);
  await echoed(neighbor.ws); await until(() => f.runtime.status().streams === 1, 'dead browser slot released');
  const replacement = await f.open('/replacement-browser'); await echoed(replacement.ws);
});

test('R3 source silence is not kept alive by browser traffic or connector data acknowledgements', { timeout: 12_000 }, async t => {
  const f = await fixture(t, { source: { autoPong: false, echo: false } });
  const dead = await f.open('/dead-source'), neighbor = await f.open('/neighbor-source', { entry: f.neighborEntry });
  let writes = 0;
  const timer = setInterval(() => { if (dead.ws.readyState === WebSocket.OPEN) { dead.ws.send('one-way write'); writes++; } }, 70);
  t.after(() => clearInterval(timer)); await closed(dead.ws); clearInterval(timer);
  assert.ok(writes > 1); assert.ok(f.A.peers.get('/dead-source').pings.length > 0);
  assert.ok(f.gate.log.some(item => item.direction === 'toGateway' && item.type === 'ack'), 'actual connector still acknowledged writes');
  assert.equal(f.runtime.status().connected, true); await echoed(neighbor.ws);
});

test('R3 ordinary frames satisfy liveness, delayed own Pongs are absorbed, and application Ping/Pong remains transparent', { timeout: 12_000 }, async t => {
  const f = await fixture(t, { source: { autoPong: false } }), client = await f.open('/alternate-response');
  const peer = f.A.peers.get('/alternate-response');
  peer.ws.on('ping', payload => {
    if (payload.length !== 32) { peer.ws.pong(payload); return; }
    peer.ws.send('ordinary response instead of correlated pong');
    const timer = setTimeout(() => { if (peer.ws.readyState === WebSocket.OPEN) peer.ws.pong(payload); }, 60); timer.unref();
  });
  await until(() => peer.pings.length >= 3, 'three source probes'); await wait(100);
  assert.equal(client.ws.readyState, WebSocket.OPEN); assert.equal(client.pongs.length, 0, 'late own source Pong did not reach browser');
  assert.equal(peer.pongs.length, 0, 'own browser Pong did not reach source');
  peer.ws.ping('application-source-ping'); client.ws.ping('application-browser-ping');
  await until(() => peer.pongs.some(value => value.toString() === 'application-source-ping') && client.pongs.some(value => value.toString() === 'application-browser-ping'), 'normal control replies');
  await echoed(client.ws);
});

test('R3 split headers and a fragmented 1MiB binary message preserve bytes and ACK chunks before the full frame', { timeout: 15_000 }, async t => {
  const f = await fixture(t), client = await f.open('/fragments'), peer = f.A.peers.get('/fragments');
  const body = Buffer.alloc(1024 * 1024); for (let i = 0; i < body.length; i++) body[i] = i % 251;
  const reply = nextMessage(client.ws);
  await send(client.ws, body.subarray(0, 700_000), { binary: true, fin: false });
  client.ws.ping('between-fragments');
  await send(client.ws, body.subarray(700_000), { binary: true, fin: true });
  assert.deepEqual(await reply, body); assert.deepEqual(peer.messages.at(-1), body);
  const open = f.gate.log.find(item => item.direction === 'toConnector' && item.path === '/fragments');
  const data = f.gate.log.filter(item => item.direction === 'toConnector' && item.id === open.id && item.type === 'data');
  assert.ok(data.length > 20); assert.ok(data.every(item => item.bytes <= 48 * 1024));
  const fragmented = encodeFrame(2, 'split-header-and-mask', { masked: true });
  const second = nextMessage(client.ws);
  for (const piece of [fragmented.subarray(0, 1), fragmented.subarray(1, 4), fragmented.subarray(4, 7), fragmented.subarray(7)]) {
    client.ws._socket.write(piece); await wait(5);
  }
  assert.equal((await second).toString(), 'split-header-and-mask');
});

test('R3 a held completed HTTP101 callback gates both WS directions and its one source chunk without an early ACK', { timeout: 12_000 }, async t => {
  const f = await fixture(t), held = f.holdUpgrade('/held-101'), client = await f.open('/held-101');
  await until(() => held.held.length === 1, 'actual HTTP101 write callback captured');
  const peer = f.A.peers.get('/held-101'), id = f.gate.log.find(item => item.path === '/held-101').id, received = [];
  client.ws.on('message', data => received.push(data.toString()));
  peer.ws.send('source waiting for handshake completion'); client.ws.send('browser waiting for handshake completion');
  const sourcePacket = await until(() => f.gate.log.find(item => item.direction === 'toGateway' && item.id === id && item.type === 'data'), 'source chunk behind101');
  await wait(70);
  assert.deepEqual(received, []); assert.equal(peer.messageCount, 0);
  assert.equal(f.gate.log.some(item => item.direction === 'toConnector' && item.id === id && item.type === 'ack' && item.seq === sourcePacket.seq), false);
  held.restore();
  await until(() => received.includes('source waiting for handshake completion') && received.includes('browser waiting for handshake completion'), 'both directions after101');
  assert.equal(peer.messageCount, 1); await echoed(client.ws);
});

test('R3 malformed mask, RSV, continuation and oversized frames are rejected at the gateway without evicting the connector', { timeout: 15_000 }, async t => {
  const f = await fixture(t), neighbor = await f.open('/neighbor-malformed', { entry: f.neighborEntry });
  const oversized = masked => {
    const bytes = Buffer.alloc(masked ? 14 : 10); bytes[0] = 130; bytes[1] = 127 | (masked ? 128 : 0);
    bytes.writeBigUInt64BE(1_048_577n, 2); if (masked) randomBytes(4).copy(bytes, 10); return bytes;
  };
  const invalidRsv = encodeFrame(1, '', { masked: true }); invalidRsv[0] |= 64;
  const cases = [
    { from: 'client', bytes: encodeFrame(1), code: 'app_websocket_invalid_frame' },
    { from: 'client', bytes: invalidRsv, code: 'app_websocket_invalid_frame' },
    { from: 'client', bytes: encodeFrame(0, '', { masked: true }), code: 'app_websocket_invalid_fragment' },
    { from: 'client', bytes: oversized(true), code: 'app_websocket_message_too_large' },
    { from: 'source', bytes: encodeFrame(1, '', { masked: true }), code: 'app_websocket_invalid_frame' },
    { from: 'source', bytes: oversized(false), code: 'app_websocket_message_too_large' },
  ];
  for (const [n, item] of cases.entries()) {
    const path = `/invalid-${n}`, client = await f.open(path), peer = f.A.peers.get(path);
    const id = f.gate.log.find(entry => entry.path === path).id;
    (item.from === 'client' ? client.ws : peer.ws)._socket.write(item.bytes);
    await closed(client.ws);
    assert.ok(f.gate.log.some(entry => entry.direction === 'toConnector' && entry.type === 'cancel' && entry.id === id && entry.error === item.code), item.code);
    assert.equal(peer.messageCount, 0); assert.equal(f.runtime.status().connected, true);
  }
  await echoed(neighbor.ws);
});

test('R3 many zero-length frames retain bytes without thousands of outer data/ACK or authority operations', { timeout: 12_000 }, async t => {
  const f = await fixture(t, { source: { echo: false } }), client = await f.open('/tiny-frames');
  const peer = f.A.peers.get('/tiny-frames'), before = f.gate.log.length, authBefore = f.authorityChecks();
  const bytes = Buffer.alloc(48 * 1024);
  for (let i = 0; i < bytes.length; i += 6) { bytes[i] = 130; bytes[i + 1] = 128; randomBytes(4).copy(bytes, i + 2); }
  client.ws._socket.write(bytes);
  await until(() => peer.messageCount === 8192, 'all masked zero-length messages', 6000);
  const writes = f.gate.log.slice(before).filter(item => item.direction === 'toConnector' && item.type === 'data');
  assert.ok(writes.length <= 4, `tiny frames created ${writes.length} source chunks`);
  assert.ok(f.authorityChecks() - authBefore < 512, 'one input chunk must not create one database authorization per tiny frame');
  const responseBytes = Buffer.alloc(48 * 1024);
  for (let i = 0; i < responseBytes.length; i += 2) { responseBytes[i] = 130; responseBytes[i + 1] = 0; }
  let count = 0; client.ws.on('message', () => count++); peer.ws._socket.write(responseBytes);
  await until(() => count === 24 * 1024, 'all unmasked zero-length messages', 6000);
  assert.equal(client.ws.readyState, WebSocket.OPEN);
});

test('R3 partial-frame trickle has an absolute deadline and does not stop another application', { timeout: 12_000 }, async t => {
  const f = await fixture(t), client = await f.open('/slow-frame'), neighbor = await f.open('/neighbor-slow', { entry: f.neighborEntry });
  const frame = encodeFrame(2, Buffer.alloc(100), { masked: true }); client.ws._socket.write(frame.subarray(0, 6));
  let offset = 6, sent = 0;
  const timer = setInterval(() => { if (client.ws.readyState === WebSocket.OPEN) { client.ws._socket.write(frame.subarray(offset, ++offset)); sent++; } }, 130);
  t.after(() => clearInterval(timer)); await closed(client.ws); clearInterval(timer);
  assert.ok(sent > 2 && sent < 30, 'receiving more bytes did not reset assembly deadline');
  assert.equal(f.runtime.status().connected, true); await echoed(neighbor.ws);
});

test('R3 a completed real write with its callback withheld cannot pin a stream forever or resume after closure', { timeout: 12_000 }, async t => {
  const f = await fixture(t), client = await f.open('/held-write'), neighbor = await f.open('/neighbor-write', { entry: f.neighborEntry });
  const peer = f.A.peers.get('/held-write'), held = f.holdClientWrites('/held-write');
  const delivered = nextMessage(client.ws); peer.ws.send('physically delivered before callback');
  assert.equal((await delivered).toString(), 'physically delivered before callback');
  await until(() => held.held.length > 0, 'actual write callback captured'); await closed(client.ws);
  const at = f.gate.log.length; held.restore(); await wait(80);
  const id = f.gate.log.find(item => item.path === '/held-write').id;
  assert.equal(f.gate.log.slice(at).some(item => item.id === id && ['data', 'ack'].includes(item.type)), false, 'late callback did not continue a closed stream');
  await echoed(neighbor.ws); assert.equal(f.runtime.status().connected, true);
});

test('R3 withheld actual source ACK times out only its stream; late ACK does not invalidate the channel', { timeout: 12_000 }, async t => {
  const f = await fixture(t), client = await f.open('/held-ack'), neighbor = await f.open('/neighbor-ack', { entry: f.neighborEntry });
  const id = f.gate.log.find(item => item.path === '/held-ack').id, held = [];
  f.gate.hooks.toGateway = packet => { if (packet.frame.type === 'ack' && packet.frame.id === id) { held.push(packet); return false; } };
  await send(client.ws, 'already handed to upstream'); await until(() => held.length > 0, 'real upstream ACK withheld'); await closed(client.ws);
  f.gate.hooks.toGateway = null; for (const packet of held) packet.forward(); await wait(70);
  assert.equal(f.runtime.status().connected, true); await echoed(neighbor.ws);
  const replacement = await f.open('/after-ack-timeout'); await echoed(replacement.ws);
});

test('R3 revocation during a completed write with a held callback fences its late ACK and keeps the neighbor alive', { timeout: 12_000 }, async t => {
  const f = await fixture(t), client = await f.open('/revoked-write'), neighbor = await f.open('/neighbor-revoke', { entry: f.neighborEntry });
  const held = f.holdClientWrites('/revoked-write'), peer = f.A.peers.get('/revoked-write');
  const delivered = nextMessage(client.ws); peer.ws.send('bytes delivered before revoke'); await delivered;
  await until(() => held.held.length > 0, 'write waiting at revoke');
  f.call('apps.revoke', { appId: f.app.id }); await closed(client.ws);
  const at = f.gate.log.length, id = f.gate.log.find(item => item.path === '/revoked-write').id;
  held.restore(); await wait(80);
  assert.equal(f.gate.log.slice(at).some(item => item.id === id && ['data', 'ack'].includes(item.type)), false);
  await echoed(neighbor.ws); assert.equal(f.runtime.status().connected, true);
});

test('R3 target promotion while a relay write awaits completion cannot resume the old binding or close a neighbor', { timeout: 15_000 }, async t => {
  const f = await fixture(t), client = await f.open('/old-binding'), neighbor = await f.open('/neighbor-binding', { entry: f.neighborEntry });
  const held = f.holdClientWrites('/old-binding'), peer = f.A.peers.get('/old-binding');
  const delivered = nextMessage(client.ws); peer.ws.send('already delivered on old target'); await delivered;
  await until(() => held.held.length > 0, 'write awaiting source change');
  const publication = f.call('apps.publication.get', { appId: f.app.id });
  const prepared = await f.service.sourcePreparationExtension.executeAsync({ op: 'apps.source.prepare', actor: owner,
    args: { expectedAccountId: owner.accountId, appId: f.app.id, expectedPolicyEpoch: publication.policyEpoch,
      expectedTargetRevision: publication.activeTargetRevision,
      source: { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, port: f.A.port, entryPath: '/new-source-target' } } });
  f.call('apps.source.promote', { appId: f.app.id, requestId: 'liveness-switch-target', preparationId: prepared.preparationId,
    expectedPolicyEpoch: prepared.expectedPolicyEpoch, expectedTargetRevision: prepared.expectedTargetRevision,
    launchPolicy: 'anyone', listed: false, exposureAck: { scope: 'whole-port', targetRevision: prepared.target.revision,
      targetDigest: prepared.target.digest, profile: prepared.target.profile } });
  await closed(client.ws);
  const at = f.gate.log.length, id = f.gate.log.find(item => item.path === '/old-binding').id;
  held.restore(); await wait(80);
  assert.equal(f.gate.log.slice(at).some(item => item.id === id && ['data', 'ack'].includes(item.type)), false);
  const fresh = await f.open('/new-binding', { entry: await f.session(f.app.id) }); await echoed(fresh.ws);
  await echoed(neighbor.ws); assert.equal(f.runtime.status().connected, true);
});

test('R3 receiving Close stops probes and Pong-only traffic cannot postpone closing forever', { timeout: 12_000 }, async t => {
  const f = await fixture(t), client = await f.open('/raw-ignore-close');
  const raw = f.A.raw.get('/raw-ignore-close'); client.ws.close(1000, 'test-close');
  await until(() => raw.frames.some(item => item.opcode === 8), 'Close passed upstream');
  const afterClose = raw.frames.length; await closed(client.ws);
  assert.equal(raw.frames.slice(afterClose).some(item => item.opcode === 9), false, 'no gateway probe after Close');
  await until(() => f.runtime.status().streams === 0, 'closing slot released');
});

for (const initiator of ['client', 'source']) test(`TCP EOF preserves the ${initiator}-initiated WebSocket Close handshake and its neighbor`, { timeout: 12_000 }, async t => {
  const f = await fixture(t), client = await f.open('/graceful-close'), neighbor = await f.open('/close-neighbor');
  const peer = f.A.peers.get('/graceful-close');
  const result = ws => bounded(new Promise(done => ws.once('close', (code, reason) => done({ code, reason: reason.toString() }))), 'graceful Close');
  const clientClosed = result(client.ws), sourceClosed = result(peer.ws);
  (initiator === 'client' ? client.ws : peer.ws).close(1000, 'finished');
  assert.deepEqual(await clientClosed, { code: 1000, reason: 'finished' });
  assert.deepEqual(await sourceClosed, { code: 1000, reason: 'finished' });
  await until(() => f.runtime.status().streams === 1, 'only the closing stream was released');
  await echoed(neighbor.ws, 'neighbor survives a clean close');
  assert.equal(f.runtime.status().connected, true);
});

test('R3 heartbeat cannot renew an expired account deadline or revive an expired public lease', { timeout: 12_000 }, async t => {
  const f = await fixture(t), account = await f.open('/account-expiry'), anonymous = await f.open('/public-expiry', { entry: f.publicEntry });
  await until(() => account.pings.length && anonymous.pings.length, 'heartbeat before expiry');
  f.advance(30_000); await closed(anonymous.ws);
  assert.equal(account.ws.readyState, WebSocket.OPEN, 'independent account deadline did not inherit public lease');
  f.advance(3_600_000); await closed(account.ws);
  assert.equal(f.runtime.status().connected, true); await until(() => f.runtime.status().streams === 0, 'authorization expired slots freed');
  assert.notEqual((await f.http('/_soty/session')).status, 200);
});

test('R3 exact public and total admission caps remain after repeated probes and closed slots are reusable', { timeout: 20_000 }, async t => {
  const f = await fixture(t, { timing: { quietMs: 1500, insertMs: 3000, responseMs: 3000, frameMs: 3000 } });
  const publicClients = [], privateClients = [];
  for (let i = 0; i < 24; i++) publicClients.push(await f.open(`/public-cap-${i}`, { entry: f.publicEntry }));
  await assert.rejects(f.open('/public-too-many', { entry: f.publicEntry }), error => error.status === 429);
  for (let i = 0; i < 8; i++) privateClients.push(await f.open(`/private-cap-${i}`));
  await assert.rejects(f.open('/total-too-many'), error => error.status === 429);
  await until(() => publicClients.every(item => item.pings.length) && privateClients.every(item => item.pings.length), 'all admitted streams probed');
  publicClients[0].ws.terminate(); await closed(publicClients[0].ws);
  await until(() => f.runtime.status().streams === 31, 'one cap slot released');
  const replacement = await f.open('/cap-replacement', { entry: f.publicEntry }); await echoed(replacement.ws);
  for (const item of [...publicClients.slice(1), ...privateClients]) assert.equal(item.ws.readyState, WebSocket.OPEN);
});

test('R3 malformed trusted timing settings fail before creating the database or admitting transport', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-ws-liveness-options-'));
  t.after(async () => {
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(directory.split(/[\\/]/u).at(-1), /^soty-ws-liveness-options-/u);
    await rm(directory, { recursive: true, force: true });
  });
  for (const [n, timing] of [null, [], false, { quietMs: 0 }, { quietMs: Infinity }, { insertMs: NaN },
    { responseMs: 30_001 }, { frameMs: -1 }, { quietMs: '10' }, { quietMs: 100, unknownOption: 100 }].entries()) {
    const databasePath = join(directory, `invalid-${n}`, 'apps.sqlite');
    assert.throws(() => createAppsService({ databasePath, shellOrigins: ['http://localhost:8080'], webSocketLiveness: timing }),
      error => typeof error.code === 'string' && /websocket|timing/iu.test(error.code));
    assert.equal(existsSync(dirname(databasePath)), false, 'invalid host settings have no storage effect');
  }
});
