import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createLocalAppsRuntime } from '../../scripts/agent-modules/local-apps.mjs';

// Real signed Connect HTTP, World/Apps stores, authenticated v2 connector and a
// loopback source. Only browser identity persistence is a memory adapter.
const delay = ms => new Promise(done => setTimeout(done, ms));
const secret = () => randomBytes(32).toString('base64url');
async function until(check, label) {
  const deadline = Date.now() + 5000;
  do { const value = await check(); if (value) return value; await delay(10); } while (Date.now() < deadline);
  assert.fail(`Timed out: ${label}`);
}
function identityStorage() {
  let value = null;
  return {
    async read() { return structuredClone(value); },
    async claim(candidate) { value ||= structuredClone(candidate); return structuredClone(value); },
    async compareAndSwap(revision, candidate) {
      assert.equal(value.localRevision, revision); value = structuredClone(candidate); return structuredClone(value);
    },
  };
}
const errorCode = (expected, status = 400) => error => {
  assert.equal(error?.code, expected); assert.equal(error?.status, status); return true;
};
function exactEntry(value) {
  assert.deepEqual(Object.keys(value).sort(), ['appId', 'domainId', 'origin', 'path']);
  assert.match(value.appId, /^app-[a-f0-9]{32}$/u);
  assert.match(value.domainId, /^dom_[a-f0-9]{32}$/u);
  assert.equal(new URL(value.origin).origin, value.origin);
  assert.equal(typeof value.path, 'string');
  assert.doesNotMatch(JSON.stringify(value), /claimCode|connectorId|hostDeviceId|grants|ticket|expiresAt|Private source label/u);
}
async function environment(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-entry-http-independent-'));
  const dataDir = join(directory, 'data'), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Entry boundary test</title>');
  let app, runtime, serial = 0, registeredApps = 0;
  const clients = [], sockets = new Set(), hits = [];
  const source = createServer((req, res) => { hits.push({ method: req.method, path: req.url }); res.end('Entry source'); });
  const otherSource = createServer((_req, res) => res.end('Other app source'));
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  for (const item of [source, otherSource, server]) item.on('connection', socket => {
    sockets.add(socket); socket.once('close', () => sockets.delete(socket));
  });
  await Promise.all([source, otherSource, server].map(item => new Promise(done => item.listen(0, '127.0.0.1', done))));
  const origin = `http://127.0.0.1:${server.address().port}`, port = server.address().port;
  t.after(async () => {
    runtime?.stop(); for (const client of clients) client.dispose(); await app?.locals.closeServices();
    for (const socket of sockets) socket.destroy();
    await Promise.all([source, otherSource, server].map(item => new Promise(done => item.close(done))));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-entry-http-independent-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  app = createHttpApp(dist, { dataDir, connectOrigins: [origin],
    appOriginTemplate: `http://{appId}.legacy.localhost:${port}`, namedAppZone: `http://named.localhost:${port}` });
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  function client() {
    const value = createClientWithStorage({ projectId: 'soty', endpoint: `${origin}/api/connect/rpc`,
      fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }),
    }, identityStorage()); clients.push(value); return value;
  }
  const owner = client(), ownerAccount = await owner.bootstrap('Entry owner');
  const reader = client(), readerAccount = await reader.bootstrap('Entry reader');
  const identity = { linkId: 'entry_http_link_12345678901234567890', hostDeviceId: 'entry_http_host', connectorId: 'entry_http_connector', name: 'Private source label' };
  const token = secret();
  const registration = await fetch(`${origin}/api/connectors/register`, { method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  assert.equal((await registration.json()).ok, true);
  runtime = createLocalAppsRuntime({ randomSecret: secret, digest: value => createHash('sha256').update(value).digest('hex'),
    createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request,
    encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64'),
  }, { identity, token, serverUrl: origin });
  runtime.start(); await until(() => runtime.status().connected, 'authenticated connector');
  const claim = await runtime.claim(), ids = { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId };
  await owner.extension('apps.claim', { ...ids, claimCode: claim.claimCode });
  const paths = { apps: join(dataDir, 'apps', 'registry.sqlite'), world: join(dataDir, 'world', 'world.sqlite') };
  function inspect(which, read) { const db = new DatabaseSync(paths[which], { readOnly: true }); try { return read(db); } finally { db.close(); } }
  async function create(path = '/initial', grants = {}) {
    const appPort = (++registeredApps === 1 ? source : otherSource).address().port;
    const value = (await owner.extension('apps.register', { ...ids, name: 'Entry app', port: appPort, entryPath: path, grants })).app;
    await until(async () => (await owner.extension('apps.inspect', { appId: value.id })).source.binding.state === 'bound', 'exact source binding');
    return value;
  }
  async function alias(appId) {
    const current = await owner.extension('apps.inspect', { appId });
    const value = await owner.extension('apps.domains.claim', { appId, slug: `entry-${++serial}`, requestId: `entry_claim_${serial}`,
      expectedDomainsRevision: current.addresses.revision });
    return { domainId: value.receipt.domainId, origin: value.receipt.origin };
  }
  async function publish(appId, aliases) {
    const p = await owner.extension('apps.publication.get', { appId });
    await owner.extension('apps.publication.update', { appId, requestId: `entry_publish_${++serial}`, expectedPolicyEpoch: p.policyEpoch,
      expectedTargetRevision: p.activeTargetRevision, launchPolicy: 'anyone', listed: false, activeDomainIds: aliases.map(value => value.domainId),
      exposureAck: { scope: 'whole-port', targetRevision: p.target.revision, targetDigest: p.target.digest, profile: p.target.profile } });
  }
  return { app, owner, ownerAccount, reader, readerAccount, runtime, paths, hits, inspect, create, alias, publish,
    async changeDefault(appId, entryPath) {
      const p = await owner.extension('apps.publication.get', { appId });
      const proof = await owner.extension('apps.source.prepare', { appId, expectedPolicyEpoch: p.policyEpoch,
        expectedTargetRevision: p.activeTargetRevision, source: { ...ids, port: source.address().port, entryPath } });
      await owner.extension('apps.source.promote', { appId, requestId: `entry_source_${++serial}`, preparationId: proof.preparationId,
        expectedPolicyEpoch: p.policyEpoch, expectedTargetRevision: p.activeTargetRevision, launchPolicy: 'restricted', listed: false });
      await until(async () => (await owner.extension('apps.inspect', { appId })).source.binding.state === 'bound', 'promoted binding');
    },
    async offline(appId) { runtime.stop(); await until(async () => (await owner.extension('apps.inspect', { appId })).source.binding.state === 'offline', 'server sees source offline'); },
  };
}

test('D3 signed launch and entry resolver agree on the exact admitted Unicode/query/hash entry', { timeout: 20000 }, async t => {
  const f = await environment(t), path = '/проект?tag=a%2Bb#сцена', app = await f.create(path);
  const launched = await f.owner.extension('apps.launch', { appId: app.id, expectedAccountId: f.ownerAccount.accountId });
  exactEntry(launched.entry); assert.equal(launched.entry.path, path);
  const url = new URL(launched.launchUrl); assert.equal(url.origin, launched.entry.origin);
  assert.equal(url.searchParams.get('path'), path); assert.match(url.hash, /^#[A-Za-z0-9_-]{43}$/u);
  const read = await f.owner.extension('apps.entry.get', { appId: app.id, expectedAccountId: f.ownerAccount.accountId });
  assert.deepEqual(read, { ok: true, entry: launched.entry });
  for (const exactPath of ['/#/dashboard', '/board?tag=a%2Bb#item']) {
    const target = { appId: app.id, domainId: read.entry.domainId, path: exactPath };
    const explicit = await f.owner.extension('apps.launch', target), entry = await f.owner.extension('apps.entry.get', target);
    assert.deepEqual(explicit.entry, entry.entry); assert.equal(entry.entry.path, exactPath);
    assert.equal(new URL(explicit.launchUrl).searchParams.get('path'), exactPath);
  }
});

test('D3 offline entry lookup works before World provisioning without allocating saved or discussion state', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create(), address = await f.alias(app.id);
  await f.publish(app.id, [address]); await f.offline(app.id);
  const counts = () => ({ profiles: f.inspect('world', db => db.prepare('SELECT count(*) AS n FROM profiles').get().n),
    saved: f.inspect('apps', db => db.prepare('SELECT count(*) AS n FROM app_saved_heads').get().n),
    discussion: f.inspect('apps', db => db.prepare('SELECT count(*) AS n FROM app_discussion_heads').get().n) });
  const before = counts(); assert.deepEqual(before, { profiles: 0, saved: 0, discussion: 0 });
  const target = { appId: app.id, domainId: address.domainId, path: '/#/offline-entry' };
  const value = await f.reader.extension('apps.entry.get', target); exactEntry(value.entry);
  assert.equal(value.entry.origin, address.origin); assert.equal(value.entry.path, target.path);
  assert.deepEqual(counts(), before);
  await assert.rejects(f.reader.extension('apps.launch', target), errorCode('app_offline'));
  await assert.rejects(f.reader.extension('apps.entry.get', { appId: app.id }), errorCode('app_unavailable'));
  await assert.rejects(f.reader.extension('apps.entry.get', { ...target, expectedAccountId: f.ownerAccount.accountId }), errorCode('authentication_required'));
  assert.deepEqual(counts(), before);
});

test('D3 a resolved entry survives a source default change only when callers explicitly keep that exact path', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create('/old?value=%2B#old'), old = (await f.owner.extension('apps.entry.get', { appId: app.id })).entry;
  await f.changeDefault(app.id, '/new#screen');
  const current = await f.owner.extension('apps.launch', { appId: app.id });
  assert.equal(current.entry.path, '/new#screen'); assert.equal(current.entry.domainId, old.domainId);
  const explicit = await f.owner.extension('apps.launch', { appId: old.appId, domainId: old.domainId, path: old.path });
  assert.deepEqual(explicit.entry, old); assert.equal(new URL(explicit.launchUrl).searchParams.get('path'), old.path);
  assert.notEqual(explicit.launchUrl, current.launchUrl, 'each admission has its own one-use ticket');
});

test('D3 retired/foreign entries and unsafe paths never silently fall back to another live address', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create(), first = await f.alias(app.id), second = await f.alias(app.id);
  await f.publish(app.id, [first, second]);
  const initial = await f.reader.extension('apps.entry.get', { appId: app.id, domainId: first.domainId, path: '/original' });
  exactEntry(initial.entry);
  const inspection = await f.owner.extension('apps.inspect', { appId: app.id });
  await f.owner.extension('apps.domains.retire', { appId: app.id, domainId: first.domainId, requestId: 'entry_retire_first', expectedDomainsRevision: inspection.addresses.revision });
  for (const actor of [f.owner, f.reader]) await assert.rejects(actor.extension('apps.entry.get', { appId: app.id, domainId: first.domainId, path: '/original' }), errorCode('app_unavailable'));
  assert.equal((await f.reader.extension('apps.entry.get', { appId: app.id, domainId: second.domainId })).entry.origin, second.origin);
  const foreign = await f.create();
  assert.notEqual(foreign.id, app.id, 'negative tuple genuinely names a different app');
  await assert.rejects(f.owner.extension('apps.entry.get', { appId: foreign.id, domainId: second.domainId }), errorCode('app_unavailable'));
  for (const [path, reason] of [['//outside.invalid/', 'invalid_app_path'], ['/_soty/session', 'app_reserved_path'], ['/%2e%2e/_soty/boot', 'app_reserved_path']]) {
    await assert.rejects(f.owner.extension('apps.entry.get', { appId: app.id, path }), errorCode(reason));
  }
});

test('D3 entry lookup uses the World fence, maps short busy refusals to 503 and leaves no engagement rows', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create(), args = { appId: app.id };
  for (const [which, expected] of [['apps', 'apps_entry_busy'], ['world', 'world_authority_busy']]) {
    const blocker = new DatabaseSync(f.paths[which]); blocker.exec('BEGIN IMMEDIATE');
    try { await assert.rejects(f.owner.extension('apps.entry.get', args), errorCode(expected, 503)); }
    finally { blocker.exec('ROLLBACK'); blocker.close(); }
    exactEntry((await f.owner.extension('apps.entry.get', args)).entry);
  }
  assert.equal(f.inspect('apps', db => db.prepare('SELECT count(*) AS n FROM app_saved_heads').get().n), 0);
  assert.equal(f.inspect('apps', db => db.prepare('SELECT count(*) AS n FROM app_discussion_heads').get().n), 0);
  assert.equal(f.inspect('world', db => db.prepare('SELECT count(*) AS n FROM profiles').get().n), 0);
});
