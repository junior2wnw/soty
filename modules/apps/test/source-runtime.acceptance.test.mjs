import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import WebSocket, { WebSocketServer } from 'ws';
import { createAppsService } from '../server/index.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';

// Independent black-box fixtures: actual service, connector, SQLite and network.
// The optional wire gates only delay/drop a selected packet or callback. They
// are scheduler fault probes, not a physical packet-loss/backpressure proof.
const owner = { accountId: 'independent_source_owner', deviceId: 'owner_browser' };
const guest = { accountId: 'independent_source_guest', deviceId: 'guest_browser' };
const secret = () => randomBytes(32).toString('base64url');
const digest = value => createHash('sha256').update(value).digest('hex');
const delay = ms => new Promise(done => setTimeout(done, ms));
const identity = letter => ({ linkId: 'independent_source_link', hostDeviceId: `host_${letter}`, connectorId: `connector_${letter}` });

async function until(check, label, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { const value = await check(); if (value) return value; await delay(10); }
  throw new Error(`source_acceptance_timeout:${label}`);
}

async function upstream(label) {
  const hits = [], heldHeads = [], responses = new Set(), sockets = new Set();
  const ws = new WebSocketServer({ noServer: true });
  const server = http.createServer((req, res) => {
    hits.push({ method: req.method, path: req.url });
    responses.add(res); res.once('close', () => responses.delete(res));
    if (req.method === 'HEAD' && req.url === '/held-preparation') { heldHeads.push(res); return; }
    if (req.url === '/continuous') { res.writeHead(200, { 'content-type': 'text/plain' }); res.write(`${label}:begin\n`); return; }
    res.writeHead(200, { 'content-type': 'text/plain' }); res.end(`${label}:${req.url}`);
  });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => {
    hits.push({ method: 'UPGRADE', path: req.url }); ws.handleUpgrade(req, socket, head, client => {
      client.send(`${label}:hello`); client.on('message', value => client.send(`${label}:${value}`));
    });
  });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  return { label, port: server.address().port, hits, heldHeads,
    getHits: () => hits.filter(item => item.method !== 'HEAD'),
    releaseHeads() { for (const res of heldHeads.splice(0)) if (!res.destroyed) { res.writeHead(200); res.end(); } },
    async close() { for (const res of responses) res.destroy(); for (const client of ws.clients) client.terminate();
      for (const socket of sockets) socket.destroy(); ws.close(); await new Promise(done => server.close(done)); },
  };
}

function wireGate() {
  const inbound = [], outbound = [], sockets = new Set();
  const hooks = { receive: null, send: null };
  class ActualSocket extends globalThis.WebSocket {
    constructor(url) { super(url); sockets.add(this); this.listeners = new Map(); }
    send(value) {
      const packet = { socket: this, frame: JSON.parse(value), forwarded: false };
      packet.forward = (frame = packet.frame) => { packet.forwarded = true; super.send(JSON.stringify(frame)); };
      outbound.push(packet);
      if (hooks.send?.(packet) !== false) packet.forward();
    }
    addEventListener(type, listener, options) {
      if (type !== 'message') return super.addEventListener(type, listener, options);
      const wrapped = event => {
        const packet = { socket: this, frame: JSON.parse(event.data), delivered: false };
        packet.deliver = (frame = packet.frame) => {
          packet.delivered = true;
          const message = new MessageEvent('message', { data: JSON.stringify(frame) });
          if (typeof listener === 'function') listener.call(this, message); else listener.handleEvent(message);
        };
        inbound.push(packet); if (hooks.receive?.(packet) !== false) packet.deliver();
      };
      this.listeners.set(listener, wrapped); return super.addEventListener(type, wrapped, options);
    }
    removeEventListener(type, listener, options) {
      return super.removeEventListener(type, this.listeners.get(listener) ?? listener, options);
    }
  }
  return { WebSocket: ActualSocket, inbound, outbound, hooks, sockets };
}

async function fixture(t, { blockedB = [] } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-runtime-independent-'));
  const databasePath = join(directory, 'registry.sqlite'), runtimes = [], applications = [], gatewaySockets = new Set(), clients = new Set();
  let service, clock = Date.now(), activeOwner = true;
  const gateway = http.createServer((req, res) => { if (!service?.handleRequest(req, res)) { res.writeHead(404); res.end(); } });
  gateway.on('connection', socket => { gatewaySockets.add(socket); socket.once('close', () => gatewaySockets.delete(socket)); });
  gateway.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  t.after(async () => {
    for (const client of clients) client.terminate(); for (const value of runtimes) value.runtime.stop(); service?.close();
    for (const socket of gatewaySockets) socket.destroy(); await new Promise(done => gateway.close(done));
    for (const app of applications) await app.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(directory.split(/[\\/]/u).at(-1), /^soty-source-runtime-independent-/u);
    await rm(directory, { recursive: true, force: true });
  });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done));
  const port = gateway.address().port, token = secret(), identities = [identity('A'), identity('B')];
  service = createAppsService({ databasePath, appOriginTemplate: `http://{appId}.legacy.localhost:${port}`,
    namedAppZone: `http://named.localhost:${port}`, shellOrigins: [`http://localhost:${port}`], now: () => clock,
    actorActive: actor => actor?.deviceId === (actor.accountId === owner.accountId ? owner.deviceId : guest.deviceId)
      && (actor.accountId === owner.accountId ? activeOwner : actor.accountId === guest.accountId),
    authenticateConnector: async value => value.token === token && identities.some(id => value.linkId === id.linkId
      && value.deviceId === id.hostDeviceId && value.connectorId === id.connectorId), accessAuditMs: 25,
  });
  assert.equal(typeof service.sourcePreparationExtension?.executeAsync, 'function', 'C2-B prepare adapter is not implemented yet');
  const call = (op, args = {}, actor = owner) => service.execute({ op, args: { ...args, expectedAccountId: actor.accountId }, actor });
  const A = await upstream('A'), B = await upstream('B'); applications.push(A, B);
  async function connect(id, blockedPorts) {
    const wire = wireGate();
    const runtime = createLocalAppsRuntime({ createWebSocket: url => new wire.WebSocket(url), httpRequest: http.request,
      randomSecret: secret, digest, encodeBase64: bytes => Buffer.from(bytes).toString('base64'),
      decodeBase64: text => Buffer.from(text, 'base64'), now: () => clock },
    { serverUrl: `http://127.0.0.1:${port}`, identity: id, token, blockedPorts });
    const item = { runtime, wire, identity: id }; runtimes.push(item); runtime.start();
    await until(() => runtime.status().connected, 'connector authenticated');
    assert.equal(wire.inbound.find(item => item.frame.type === 'ready')?.frame.bindingVersion, 2, 'C2-B connector negotiation is not implemented yet');
    const claim = await runtime.claim(); call('apps.claim', { hostDeviceId: id.hostDeviceId, connectorId: id.connectorId, claimCode: claim.claimCode });
    return item;
  }
  const a = await connect(identities[0], []), b = await connect(identities[1], blockedB);
  const app = call('apps.register', { hostDeviceId: a.identity.hostDeviceId, connectorId: a.identity.connectorId,
    name: 'Source acceptance', port: A.port, entryPath: '/', grants: { accountIds: [guest.accountId], communityIds: [] } }).app;
  const neighbor = call('apps.register', { hostDeviceId: a.identity.hostDeviceId, connectorId: a.identity.connectorId,
    name: 'Unaffected neighbor', port: B.port, entryPath: '/' }).app;
  const claimed = call('apps.domains.claim', { appId: app.id, slug: 'independent-source', requestId: 'name-source', expectedDomainsRevision: 0 });
  const aliasId = claimed.receipt.domainId, aliasOrigin = claimed.receipt.origin;
  const publication = () => call('apps.publication.get', { appId: app.id });
  const initial = publication();
  call('apps.publication.update', { appId: app.id, requestId: 'publish-source', expectedPolicyEpoch: initial.policyEpoch,
    expectedTargetRevision: initial.activeTargetRevision, launchPolicy: 'anyone', listed: false, activeDomainIds: [aliasId],
    exposureAck: exposure(initial.target) });
  async function ready(id = app.id, domainId) {
    return until(() => {
      try { return call('apps.launch', { appId: id, ...(domainId ? { domainId } : {}) }); }
      catch (error) { if (['app_binding_pending', 'app_offline'].includes(error.code)) return false; throw error; }
    }, 'exact binding ACK');
  }
  await ready(); await ready(neighbor.id);
  const canonicalOrigin = new URL((await ready()).launchUrl).origin;
  const httpCall = (path, { origin = aliasOrigin, ...options } = {}) => request(port, origin, path, options);
  async function session(id = app.id, { domainId, actor = owner } = {}) {
    const launch = call('apps.launch', { appId: id, ...(domainId ? { domainId } : {}) }, actor), url = new URL(launch.launchUrl);
    const reply = await request(port, url.origin, '/_soty/session', { method: 'POST', headers: { origin: url.origin }, body: JSON.stringify({ ticket: url.hash.slice(1) }) });
    assert.equal(reply.status, 200, reply.text); assert.ok(reply.headers['set-cookie']);
    return { origin: url.origin, cookie: reply.headers['set-cookie'][0].split(';')[0] };
  }
  async function socket({ origin = aliasOrigin, cookie } = {}) {
    const client = new WebSocket(`ws://127.0.0.1:${port}/live`, { headers: { host: new URL(origin).host, origin, ...(cookie ? { cookie } : {}) } });
    clients.add(client); client.on('error', () => {});
    const hello = await message(client); return { client, hello: hello.toString(), closed: new Promise(done => client.once('close', done)) };
  }
  const prepare = (selection = { source: { hostDeviceId: b.identity.hostDeviceId, connectorId: b.identity.connectorId, port: B.port, entryPath: '/' } }, actor = owner) => {
    const p = publication(); return service.sourcePreparationExtension.executeAsync({ op: 'apps.source.prepare', actor,
      args: { appId: app.id, expectedAccountId: actor.accountId, expectedPolicyEpoch: p.policyEpoch, expectedTargetRevision: p.activeTargetRevision, ...selection } });
  };
  const intent = (prepared, requestId, extra = {}) => ({ appId: app.id, requestId, preparationId: prepared.preparationId,
    expectedPolicyEpoch: prepared.expectedPolicyEpoch, expectedTargetRevision: prepared.expectedTargetRevision,
    launchPolicy: 'anyone', listed: false, exposureAck: exposure(prepared.target), ...extra });
  const rows = () => {
    const db = new DatabaseSync(databasePath, { readOnly: true });
    try { return Object.fromEntries(['local_apps', 'local_app_grants', 'app_domains', 'app_publications', 'app_runtime_targets', 'app_source_heads', 'app_source_receipts']
      .map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()])); } finally { db.close(); }
  };
  return { service, app, neighbor, a, b, A, B, port, call, publication, prepare, intent, ready, session, socket, rows, http: httpCall,
    aliasId, aliasOrigin, canonicalOrigin, advance: ms => { clock += ms; }, deactivateOwner: () => { activeOwner = false; } };
}
const exposure = target => ({ scope: 'whole-port', targetRevision: target.revision, targetDigest: target.digest, profile: target.profile });
function request(port, origin, path, { method = 'GET', headers = {}, cookie, body } = {}) {
  return new Promise((done, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path, method, headers: { host: new URL(origin).host, ...headers, ...(cookie ? { cookie } : {}) }, agent: false }, res => {
      const chunks = []; res.on('data', value => chunks.push(value)); res.on('error', reject);
      res.on('end', () => done({ status: res.statusCode, headers: res.headers, text: Buffer.concat(chunks).toString() }));
    });
    req.on('error', reject); req.setTimeout(4000, () => req.destroy(new Error('source_http_timeout'))); req.end(body);
  });
}
function message(socket) {
  return new Promise((done, reject) => {
    const timer = setTimeout(() => { cleanup(); reject(new Error('source_ws_timeout')); }, 4000);
    const cleanup = () => { clearTimeout(timer); socket.off('message', received); socket.off('error', failed); };
    const received = value => { cleanup(); done(value); }, failed = error => { cleanup(); reject(error); };
    socket.once('message', received); socket.once('error', failed);
  });
}
async function continuous(port, origin) {
  return new Promise((done, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path: '/continuous', headers: { host: new URL(origin).host }, agent: false });
    req.on('error', reject); req.on('response', res => {
      const chunks = []; let resolved = false;
      const closed = new Promise(end => { res.once('close', end); });
      res.on('error', () => {}); res.on('data', data => { chunks.push(data); if (!resolved) { resolved = true; done({ closed, chunks, req }); } });
    }); req.end();
  });
}

test('C2-B A→B→rollback A preserves addresses/grants, fences old tickets and streams, and leaves neighbor working', { timeout: 25_000 }, async t => {
  const f = await fixture(t), domains = f.call('apps.domains.get', { appId: f.app.id }), before = f.rows();
  const oldSession = await f.session(f.app.id, { actor: guest }), neighborSession = await f.session(f.neighbor.id);
  const oldTicket = f.call('apps.launch', { appId: f.app.id, domainId: f.aliasId });
  assert.equal((await f.http('/')).text, 'A:/');
  const stream = await continuous(f.port, f.aliasOrigin); t.after(() => stream.req.destroy());
  const socket = await f.socket(), nearby = await f.socket(neighborSession); assert.equal(socket.hello, 'A:hello');
  const prepared = await f.prepare(), args = f.intent(prepared, 'switch-to-B');
  assert.deepEqual(f.rows(), before, 'preparation did not persist or route the candidate');
  const committed = f.call('apps.source.promote', args); await Promise.all([socket.closed, stream.closed]);
  await f.ready(); assert.equal((await f.http('/')).text, 'B:/');
  assert.equal((await f.http('/', oldSession)).status, 403);
  const obsolete = await f.http('/_soty/session', { method: 'POST', headers: { origin: f.aliasOrigin }, body: JSON.stringify({ ticket: new URL(oldTicket.launchUrl).hash.slice(1) }) });
  assert.notEqual(obsolete.status, 200); assert.equal(obsolete.headers['set-cookie'], undefined);
  assert.ok(Buffer.concat(stream.chunks).toString().startsWith('A:')); assert.doesNotMatch(Buffer.concat(stream.chunks).toString(), /B:/u);
  assert.equal((await f.http('/', neighborSession)).text, 'B:/');
  const echoed = message(nearby.client); nearby.client.send('still-here'); assert.equal((await echoed).toString(), 'B:still-here');
  assert.deepEqual(f.call('apps.domains.get', { appId: f.app.id }), domains);
  assert.deepEqual(f.rows().local_app_grants, before.local_app_grants);
  assert.equal(new URL((await f.ready()).launchUrl).origin, f.canonicalOrigin);
  const freshGuest = await f.session(f.app.id, { actor: guest }); assert.equal((await f.http('/', freshGuest)).text, 'B:/');
  const rollback = await f.prepare({ targetRevision: 1 });
  const returned = f.call('apps.source.promote', f.intent(rollback, 'return-to-A')); await f.ready();
  assert.equal((await f.http('/')).text, 'A:/'); assert.equal(returned.receipt.targetRevision, 1);
  assert.ok(returned.receipt.policyEpoch > committed.receipt.policyEpoch);
  assert.equal(f.rows().app_source_heads.find(row => row.app_id === f.app.id).required_binding_version, 2);
  assert.equal(f.rows().app_runtime_targets.filter(row => row.app_id === f.app.id).length, 2);
  assert.deepEqual(f.call('apps.domains.get', { appId: f.app.id }), domains);
});

test('C2-B accepted effect survives withheld binding delivery and exact replay, without buffering HTTP on the old source', { timeout: 25_000 }, async t => {
  const f = await fixture(t), held = [];
  f.b.wire.hooks.receive = packet => { if (packet.frame.type === 'binding-set' && packet.frame.target.appId === f.app.id) { held.push(packet); return false; } };
  const prepared = await f.prepare(), args = f.intent(prepared, 'lost-promotion-response');
  f.call('apps.source.promote', args); // The caller loses this successful response.
  await until(() => held.length, 'withheld committed binding');
  const priorA = f.A.getHits().length, priorB = f.B.getHits().length;
  assert.equal((await f.http('/')).status, 503);
  assert.equal(f.A.getHits().length, priorA); assert.equal(f.B.getHits().length, priorB);
  assert.throws(() => f.call('apps.launch', { appId: f.app.id }), { code: 'app_binding_pending' });
  const replay = f.call('apps.source.promote', args); assert.equal(replay.replayed, true);
  assert.equal(f.rows().app_runtime_targets.filter(row => row.app_id === f.app.id).length, 2);
  f.b.wire.hooks.receive = null; held[0].deliver(); await f.ready(); assert.equal((await f.http('/')).text, 'B:/');
  const returned = await f.prepare({ targetRevision: 1 }); f.call('apps.source.promote', f.intent(returned, 'newer-return'));
  await f.ready(); f.b.runtime.stop(); f.advance(31_000);
  const historical = f.call('apps.source.promote', args);
  assert.equal(historical.replayed, true); assert.equal(historical.receipt.targetRevision, 2); assert.equal(historical.current.activeTargetRevision, 1);
  assert.equal((await f.http('/')).text, 'A:/');
});

test('C2-B reconnect requires fresh ACK but does not destroy a still-authorized session', { timeout: 20_000 }, async t => {
  const f = await fixture(t), prepared = await f.prepare(); f.call('apps.source.promote', f.intent(prepared, 'move-before-reconnect')); await f.ready();
  const session = await f.session(), socket = await f.socket(session);
  f.b.runtime.stop(); await socket.closed;
  assert.equal((await f.http('/', session)).status, 503);
  assert.equal((await f.http('/_soty/session', session)).status, 200, 'offline is not a DB revocation');
  const held = []; f.b.wire.hooks.send = packet => { if (packet.frame.type === 'binding-ack' && packet.frame.appId === f.app.id) { held.push(packet); return false; } };
  f.b.runtime.start(); await until(() => held.length, 'fresh reconnect ACK');
  assert.equal((await f.http('/', session)).status, 503);
  f.b.wire.hooks.send = null; held[0].forward(); await f.ready();
  assert.equal((await f.http('/', session)).text, 'B:/');
});

test('C2-B an authenticated legacy reconnect denies floor2 traffic without deleting current cookies or unused tickets', { timeout: 20_000 }, async t => {
  const f = await fixture(t), prepared = await f.prepare();
  f.call('apps.source.promote', f.intent(prepared, 'move-before-legacy')); await f.ready();
  const session = await f.session(), unusedTicket = f.call('apps.launch', { appId: f.app.id }), before = f.rows();
  assert.equal((await f.http('/', session)).text, 'B:/');
  f.b.runtime.stop();
  await until(() => !f.call('apps.devices').devices.find(item => item.hostDeviceId === f.b.identity.hostDeviceId)?.online, 'legacy replacement disconnect');
  f.b.wire.hooks.send = packet => {
    if (packet.frame.type === 'auth') {
      const { capabilities: _versions, ...legacy } = packet.frame; packet.forward(legacy); return false;
    }
  };
  f.b.runtime.start(); await until(() => f.b.runtime.status().connected, 'authenticated legacy mode');
  const ready = f.b.wire.inbound.findLast(packet => packet.frame.type === 'ready');
  assert.equal(ready.frame.bindingVersion, undefined);
  f.service.invalidateAccess(); await delay(60);
  const hits = f.B.getHits().length;
  assert.equal((await f.http('/', session)).status, 503);
  assert.throws(() => f.call('apps.launch', { appId: f.app.id }), { code: 'app_source_protocol_required' });
  assert.equal(f.B.getHits().length, hits, 'legacy runtime receives no floor2 upstream request');
  assert.ok(f.b.wire.inbound.filter(packet => packet.socket === ready.socket && packet.frame.type === 'sync')
    .every(packet => !packet.frame.apps.some(app => app.id === f.app.id)), 'floor2 is absent from legacy configuration');
  f.b.runtime.stop(); f.b.wire.hooks.send = null; f.b.runtime.start();
  await until(() => f.b.runtime.status().connected, 'restored v2 authentication'); await f.ready();
  assert.equal((await f.http('/', session)).text, 'B:/', 'same cookie survives temporary mode mismatch and its audit');
  const url = new URL(unusedTicket.launchUrl);
  const admitted = await f.http('/_soty/session', { origin: url.origin, method: 'POST', headers: { origin: url.origin },
    body: JSON.stringify({ ticket: url.hash.slice(1) }) });
  assert.equal(admitted.status, 200, 'unused ticket survives the same temporary transport mismatch');
  assert.deepEqual(f.rows(), before);
});

test('C2-B A→B→A cannot reuse the first target1 ACK and a delayed old remove cannot erase its new binding', { timeout: 20_000 }, async t => {
  const f = await fixture(t);
  const firstAck = f.a.wire.outbound.find(packet => packet.frame.type === 'binding-ack' && packet.frame.appId === f.app.id);
  assert.ok(firstAck);
  const next = await f.prepare(); f.call('apps.source.promote', f.intent(next, 'aba-to-B')); await f.ready();
  const oldRemove = await until(() => f.a.wire.inbound.find(packet => packet.frame.type === 'binding-remove'
    && packet.frame.bindings.some(item => item.appId === f.app.id && item.syncId === firstAck.frame.syncId)), 'original conditional remove');
  const held = [];
  f.a.wire.hooks.send = packet => { if (packet.frame.type === 'binding-ack' && packet.frame.appId === f.app.id) { held.push(packet); return false; } };
  const back = await f.prepare({ targetRevision: 1 }); f.call('apps.source.promote', f.intent(back, 'aba-to-A'));
  await until(() => held.length, 'new target1 ACK');
  const ack = held[0];
  assert.equal(ack.frame.digest, firstAck.frame.digest); assert.equal(ack.frame.channelId, firstAck.frame.channelId);
  assert.notEqual(ack.frame.syncId, firstAck.frame.syncId);
  const before = f.A.getHits().length;
  ack.forward(firstAck.frame); await delay(40);
  assert.equal(f.a.runtime.status().connected, true, 'well-formed stale ACK is not a channel fault');
  assert.throws(() => f.call('apps.launch', { appId: f.app.id }), { code: 'app_binding_pending' });
  assert.equal((await f.http('/')).status, 503); assert.equal(f.A.getHits().length, before);
  f.a.wire.hooks.send = null; ack.forward(); await f.ready();
  oldRemove.deliver(); await delay(40);
  assert.equal((await f.http('/')).text, 'A:/');
  assert.equal(f.rows().app_source_heads.find(row => row.app_id === f.app.id).required_binding_version, 2);
});

test('C2-B completed preparation from a replaced channel or expired lease cannot commit, nor can stale public consent', { timeout: 25_000 }, async t => {
  const f = await fixture(t), oldConsent = exposure(f.publication().target);
  const first = await f.prepare(), before = f.rows();
  f.b.runtime.stop(); await until(() => !f.call('apps.devices').devices.find(item => item.hostDeviceId === f.b.identity.hostDeviceId)?.online, 'server noticed disconnect');
  f.b.runtime.start(); await until(() => f.b.runtime.status().connected, 'new candidate channel');
  assert.throws(() => f.call('apps.source.promote', f.intent(first, 'stale-channel')), { code: 'apps_source_preparation_stale' });
  assert.deepEqual(f.rows(), before);
  const current = await f.prepare();
  assert.throws(() => f.call('apps.source.promote', f.intent(current, 'stale-exposure', { exposureAck: oldConsent })), { code: 'app_exposure_ack_required' });
  assert.deepEqual(f.rows(), before);
  f.advance(31_000);
  assert.throws(() => f.call('apps.source.promote', f.intent(current, 'expired-preparation')), { code: 'apps_source_preparation_expired' });
  assert.deepEqual(f.rows(), before); assert.equal((await f.http('/')).text, 'A:/');
});

test('C2-B old HEAD completion after reconnect cannot attest the new channel or alter its neighbors', { timeout: 20_000 }, async t => {
  const f = await fixture(t), before = f.rows();
  const preparing = f.prepare({ source: { hostDeviceId: f.b.identity.hostDeviceId, connectorId: f.b.identity.connectorId, port: f.B.port, entryPath: '/held-preparation' } });
  const failed = assert.rejects(preparing);
  await until(() => f.B.heldHeads.length, 'actual held HEAD');
  const oldPrepare = f.b.wire.inbound.findLast(packet => packet.frame.type === 'target-prepare');
  f.b.runtime.stop(); f.b.runtime.start(); await until(() => f.b.runtime.status().connected, 'replacement channel');
  f.B.releaseHeads(); await failed; await delay(60);
  assert.equal(f.b.wire.outbound.filter(packet => packet.frame.type === 'target-prepared' && packet.frame.nonce === oldPrepare.frame.nonce && packet.socket !== oldPrepare.socket).length, 0);
  assert.deepEqual(f.rows(), before); assert.equal((await f.http('/')).text, 'A:/');
  const neighbor = await f.session(f.neighbor.id); assert.equal((await f.http('/', neighbor)).text, 'B:/');
});

test('C2-B local policy rejection leaves active source intact and malformed bound-open never reaches upstream', { timeout: 20_000 }, async t => {
  const f = await fixture(t, { blockedB: [49423] }), before = f.rows();
  await assert.rejects(f.prepare({ source: { hostDeviceId: f.b.identity.hostDeviceId, connectorId: f.b.identity.connectorId, port: 49423, entryPath: '/' } }), { code: 'invalid_app_port' });
  assert.deepEqual(f.rows(), before); assert.equal(f.b.runtime.status().connected, true);
  assert.equal((await f.http('/')).text, 'A:/');
  const baseline = f.A.getHits().length;
  f.a.wire.hooks.receive = packet => { if (packet.frame.type === 'bound-open' && packet.frame.appId === f.app.id) {
    packet.deliver({ ...packet.frame, digest: '0'.repeat(64) }); return false;
  } };
  const invalid = await f.http('/tampered').catch(() => null);
  assert.notEqual(invalid?.status, 200); assert.equal(f.A.getHits().length, baseline);
  f.a.wire.hooks.receive = null;
  assert.equal(f.a.runtime.status().connected, true, 'a wrong-pin open only rejects its stream');
  await f.ready(); await f.ready(f.neighbor.id);
  const neighbor = await f.session(f.neighbor.id); assert.equal((await f.http('/', neighbor)).text, 'B:/');
  assert.deepEqual(f.rows(), before);
});

test('C2-B actual prepare HEAD uses navigation serialization while immutable target preserves original path', { timeout: 25_000 }, async t => {
  const f = await fixture(t);
  const cases = [
    ['/страница?ключ=чай#раздел', '/%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0?%D0%BA%D0%BB%D1%8E%D1%87=%D1%87%D0%B0%D0%B9'],
    ['/docs/../board?tag=a%2Bb#item', '/board?tag=a%2Bb'],
    ['/%2e%2e/board?tag=%23one', '/board?tag=%23one'],
    ['/#/dashboard', '/'],
    ['/board?tag=a%2Bb&x=%2F#item', '/board?tag=a%2Bb&x=%2F'],
  ];
  for (const [entryPath, expected] of cases) {
    const start = f.B.hits.length;
    const prepared = await f.prepare({ source: { hostDeviceId: f.b.identity.hostDeviceId, connectorId: f.b.identity.connectorId, port: f.B.port, entryPath } });
    assert.equal(prepared.target.entryPath, entryPath);
    assert.ok(f.B.hits.slice(start).some(hit => hit.method === 'HEAD' && hit.path === expected));
    f.call('apps.source.promote', f.intent(prepared, `path-${prepared.target.revision}`)); await f.ready();
    assert.equal(f.publication().target.entryPath, entryPath);
  }
  const count = f.B.hits.length, before = f.rows();
  await assert.rejects(f.prepare({ source: { hostDeviceId: f.b.identity.hostDeviceId, connectorId: f.b.identity.connectorId, port: f.B.port, entryPath: '/' + 'я'.repeat(2000) } }), { code: 'invalid_app_path' });
  assert.equal(f.B.hits.length, count, 'unrelayable request-target never receives a HEAD'); assert.deepEqual(f.rows(), before);
});
