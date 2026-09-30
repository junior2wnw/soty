import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { randomBytes, createHash } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import WebSocket from 'ws';
import { createAppsService } from '../server/index.mjs';
import { runtimeTargetDigest, RUNTIME_PROFILE } from '../server/schema.mjs';

const actor = { accountId: 'floor-owner', deviceId: 'floor-browser' };
const identities = [
  { linkId: 'floor-link', hostDeviceId: 'floor-host-a', connectorId: 'floor-connector-a' },
  { linkId: 'floor-link', hostDeviceId: 'floor-host-b', connectorId: 'floor-connector-b' },
];
const key = identity => [identity.linkId, identity.hostDeviceId, identity.connectorId].join('|');
const secret = () => randomBytes(32).toString('base64url');
const hash = value => createHash('sha256').update(value).digest('hex');
const delay = ms => new Promise(done => setTimeout(done, ms));
async function until(check) {
  const deadline = Date.now() + 3000;
  while (!check()) { if (Date.now() >= deadline) throw new Error('binding_floor_fixture_timeout'); await delay(5); }
}

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-binding-floor-'));
  const databasePath = join(directory, 'registry.sqlite'), token = secret(), sockets = new Set(), clients = [];
  let service;
  const server = createServer((req, res) => { if (!service?.handleRequest(req, res)) { res.writeHead(404); res.end(); } });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const port = server.address().port, origin = `http://localhost:${port}`;
  const config = { databasePath, shellOrigins: [origin], appOriginTemplate: `http://{appId}.legacy.localhost:${port}`,
    namedAppZone: `http://named.localhost:${port}`, accessAuditMs: 25,
    actorActive: value => value?.accountId === actor.accountId && value.deviceId === actor.deviceId,
    authenticateConnector: async value => value.token === token && identities.some(identity => value.linkId === identity.linkId
      && value.deviceId === identity.hostDeviceId && value.connectorId === identity.connectorId),
  };
  service = createAppsService(config);
  const call = (op, args = {}) => service.execute({ op, args, actor });
  async function connector(identity) {
    // Deliberately historical wire client: no target-binding capability. This
    // must stay legacy after the production connector learns protocol v2.
    const ws = new WebSocket(`ws://127.0.0.1:${port}/api/apps/channel`), frames = [];
    ws.on('error', () => {});
    ws.on('message', bytes => frames.push(JSON.parse(bytes.toString())));
    clients.push(ws);
    await new Promise((done, reject) => { ws.once('open', done); ws.once('error', reject); });
    ws.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...identity, token }));
    await until(() => frames.some(frame => frame.type === 'sync'));
    const claimCode = secret();
    ws.send(JSON.stringify({ type: 'claim', claimDigest: hash(claimCode) }));
    await until(() => frames.some(frame => frame.type === 'claim-ready'));
    if (!call('apps.devices').devices.some(device => device.connectorId === identity.connectorId))
      call('apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode });
    return { ws, frames, syncCount: () => frames.filter(frame => frame.type === 'sync').length };
  }
  t.after(async () => {
    for (const client of clients) client.terminate();
    service.close();
    for (const socket of sockets) socket.destroy();
    await new Promise(done => server.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(directory.split(/[\\/]/u).at(-1), /^soty-binding-floor-/u);
    await rm(directory, { recursive: true, force: true });
  });
  const first = await connector(identities[0]);
  const app = call('apps.register', { ...pickIdentity(identities[0]), name: 'Binding floor', port: 17891 }).app;
  await until(() => first.frames.some(frame => frame.type === 'sync' && frame.apps.some(item => item.id === app.id)));
  const claimed = call('apps.domains.claim', { appId: app.id, slug: 'binding-floor', requestId: 'floor-name', expectedDomainsRevision: 0 });
  const domainId = claimed.receipt.domainId, aliasOrigin = claimed.receipt.origin, host = new URL(aliasOrigin).host;
  const publication = call('apps.publication.get', { appId: app.id });
  call('apps.publication.update', { appId: app.id, requestId: 'floor-publish', expectedPolicyEpoch: publication.policyEpoch,
    expectedTargetRevision: publication.activeTargetRevision, launchPolicy: 'anyone', listed: false, activeDomainIds: [domainId],
    exposureAck: { scope: 'whole-port', targetRevision: publication.target.revision, targetDigest: publication.target.digest, profile: publication.target.profile } });
  const mutate = callback => { const db = new DatabaseSync(databasePath); db.exec('PRAGMA foreign_keys=ON'); try { callback(db); } finally { db.close(); } };
  return { call, app, first, connector, port, host, aliasOrigin, domainId, mutate,
    restart() { service.close(); service = createAppsService(config); },
    http: (path, options) => http(port, host, path, options),
  };
}
const pickIdentity = ({ hostDeviceId, connectorId }) => ({ hostDeviceId, connectorId });

test('sticky binding2 on target1 cannot launch or mint a session or reach HTTP/WS through a v1 connector', { timeout: 12_000 }, async t => {
  const f = await fixture(t);
  const launch = f.call('apps.launch', { appId: f.app.id, domainId: f.domainId });
  f.mutate(db => db.prepare('UPDATE app_source_heads SET required_binding_version=2 WHERE app_id=?').run(f.app.id));
  assert.throws(() => f.call('apps.launch', { appId: f.app.id, domainId: f.domainId }), { code: 'app_source_protocol_required' });
  const session = await f.http('/_soty/session', { method: 'POST', headers: { origin: f.aliasOrigin },
    body: JSON.stringify({ ticket: new URL(launch.launchUrl).hash.slice(1) }) });
  assert.ok([403, 503].includes(session.status));
  assert.equal(session.headers['set-cookie'], undefined);
  assert.equal((await f.http('/')).status, 503);
  const wsStatus = await new Promise((done, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${f.port}/live`, { headers: { host: f.host, origin: f.aliasOrigin } });
    t.after(() => ws.terminate()); ws.on('error', () => {});
    ws.once('open', () => { ws.terminate(); reject(new Error('legacy upstream was opened')); });
    ws.once('unexpected-response', (_req, res) => { res.resume(); done(res.statusCode); });
  });
  assert.equal(wsStatus, 503);
  f.first.ws.send(JSON.stringify({ type: 'observation', appId: f.app.id, state: 'ready' }));
  await delay(20);
  assert.equal(f.call('apps.inspect', { appId: f.app.id }).source.observation.state, 'unknown');
  assert.equal(f.first.frames.filter(frame => frame.type === 'open').length, 0);
  assert.equal(f.call('apps.publication.get', { appId: f.app.id }).activeTargetRevision, 1);
});

test('reopening an Apps4 reader preserves the binding floor and excludes changed apps from legacy sync', { timeout: 12_000 }, async t => {
  const f = await fixture(t);
  f.mutate(db => db.prepare('UPDATE app_source_heads SET required_binding_version=2 WHERE app_id=?').run(f.app.id));
  f.restart();
  const reconnect = await f.connector(identities[0]);
  assert.ok(reconnect.frames.filter(frame => frame.type === 'sync').every(frame => !frame.apps.some(item => item.id === f.app.id)));
  assert.equal(f.call('apps.publication.get', { appId: f.app.id }).requiredBindingVersion, 2);
  assert.throws(() => f.call('apps.launch', { appId: f.app.id }), { code: 'app_source_protocol_required' });
});

test('registration and revoke follow the active target, preserving immutable historical source fields', { timeout: 12_000 }, async t => {
  const f = await fixture(t), second = await f.connector(identities[1]);
  const target = { appId: f.app.id, revision: 2, ownerAccountId: actor.accountId, connectorKey: key(identities[1]),
    port: 17892, entryPath: '/moved', profile: RUNTIME_PROFILE };
  f.mutate(db => {
    db.exec('BEGIN IMMEDIATE');
    db.prepare('INSERT INTO app_runtime_targets VALUES (?,?,?,?,?,?,?,?,?)').run(target.appId, target.revision, target.ownerAccountId,
      target.connectorKey, target.port, target.entryPath, target.profile, runtimeTargetDigest(target), Date.now());
    db.prepare('UPDATE app_source_heads SET required_binding_version=2 WHERE app_id=?').run(f.app.id);
    db.prepare("UPDATE app_publications SET active_target_revision=2,policy_epoch=policy_epoch+1,launch_policy='restricted',listed=0,exposure_ack_revision=NULL,exposure_ack_json=NULL WHERE app_id=?").run(f.app.id);
    db.exec('COMMIT');
  });
  const freed = f.call('apps.register', { ...pickIdentity(identities[0]), name: 'Freed old port', port: 17891 }).app;
  assert.notEqual(freed.id, f.app.id);
  const same = f.call('apps.register', { ...pickIdentity(identities[1]), name: 'Binding floor', port: 17892, entryPath: '/moved' }).app;
  assert.equal(same.id, f.app.id);
  assert.throws(() => f.call('apps.register', { ...pickIdentity(identities[1]), name: 'Another app', port: 17892 }), { code: 'app_port_already_registered' });
  await delay(20);
  const beforeFirst = f.first.syncCount(), beforeSecond = second.syncCount();
  f.call('apps.revoke', { appId: f.app.id });
  await until(() => second.syncCount() > beforeSecond);
  assert.equal(f.first.syncCount(), beforeFirst, 'revoke sync uses the current connector, not the historical one');
  assert.equal(f.call('apps.inspect', { appId: freed.id }).app.state, 'enabled');
  f.mutate(db => {
    const old = db.prepare('SELECT connector_key,port,entry_path FROM local_apps WHERE id=?').get(f.app.id);
    assert.equal(old.connector_key, key(identities[0])); assert.equal(old.port, 17891); assert.equal(old.entry_path, '/');
  });
});

function http(port, host, path, { method = 'GET', headers = {}, body } = {}) {
  return new Promise((done, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path, method, headers: { host, ...headers } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.once('error', reject);
      res.once('end', () => done({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString('utf8') }));
    }); req.once('error', reject); req.end(body);
  });
}
