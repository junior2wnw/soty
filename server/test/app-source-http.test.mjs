import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createLocalAppsRuntime } from '../../scripts/agent-modules/local-apps.mjs';

// Actual HTTP composition, signed Connect browser client, native WebSocket and
// two loopback HTTP sources. Memory storage stands in only for browser IDB.
const delay = ms => new Promise(done => setTimeout(done, ms));
async function until(check, label, timeout = 5000) {
  const deadline = Date.now() + timeout;
  do { const result = await check(); if (result) return result; await delay(10); } while (Date.now() < deadline);
  throw new Error(`Timed out: ${label}`);
}
const code = expected => error => { assert.equal(error.code, expected); return true; };
function storage() {
  let saved = null;
  return {
    async read() { return structuredClone(saved); },
    async claim(candidate) { saved ||= structuredClone(candidate); return structuredClone(saved); },
    async compareAndSwap(revision, candidate) {
      assert.equal(saved.localRevision, revision); saved = structuredClone(candidate); return structuredClone(saved);
    },
  };
}
async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-http-'));
  const dataDir = join(directory, 'data'), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Source HTTP test</title>');
  let app, runtime, hold = false;
  const clients = [], connections = new Set(), held = new Set(), probePaths = [];
  const track = server => server.on('connection', socket => {
    connections.add(socket); socket.once('close', () => connections.delete(socket));
  });
  const first = createServer((_req, res) => res.end('source A'));
  const second = createServer((req, res) => {
    if (req.method === 'HEAD') {
      probePaths.push(req.url);
      if (hold) { held.add(res); res.once('close', () => held.delete(res)); return; }
    }
    res.end('source B');
  });
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  for (const item of [first, second, server]) track(item);
  await Promise.all([first, second, server].map(item => new Promise(done => item.listen(0, '127.0.0.1', done))));
  const port = server.address().port, origin = `http://127.0.0.1:${port}`;
  t.after(async () => {
    runtime?.stop(); for (const client of clients) client.dispose();
    await app?.locals.closeServices();
    for (const socket of connections) socket.destroy();
    await Promise.all([first, second, server].map(item => new Promise(done => item.close(done))));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-source-http-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  app = createHttpApp(dist, { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${port}` });
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  const identity = { linkId: 'source_http_link_12345678901234567890', hostDeviceId: 'source_http_host', connectorId: 'source_http_connector', name: 'Source HTTP device' };
  const token = randomBytes(32).toString('base64url');
  const registration = await fetch(`${origin}/api/connectors/register`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  assert.equal((await registration.json()).ok, true, 'local connector registration');
  runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'),
    digest: value => createHash('sha256').update(value).digest('hex'), createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request,
    encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64'),
  }, { identity, token, serverUrl: origin });
  runtime.start(); await until(() => runtime.status().connected, 'connector channel');
  function client() {
    const value = createClientWithStorage({ projectId: 'soty', endpoint: `${origin}/api/connect/rpc`,
      fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }),
    }, storage());
    clients.push(value); return value;
  }
  const owner = client(), account = await owner.bootstrap('Source owner');
  const claim = await runtime.claim(), ids = { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId };
  await owner.extension('apps.claim', { ...ids, claimCode: claim.claimCode });
  const registered = await owner.extension('apps.register', { ...ids, name: 'HTTP source app', port: first.address().port, entryPath: '/' });
  const appId = registered.app.id;
  await until(async () => (await owner.extension('apps.list')).apps.find(value => value.id === appId)?.state === 'ready', 'initial app binding and HEAD');
  const policy = () => owner.extension('apps.publication.get', { appId });
  const prepareArgs = async (entryPath = '/страница?ключ=чай#раздел') => {
    const current = await policy();
    return { appId, expectedAccountId: account.accountId, expectedPolicyEpoch: current.policyEpoch,
      expectedTargetRevision: current.activeTargetRevision, source: { ...ids, port: second.address().port, entryPath } };
  };
  return { app, dataDir, owner, account, appId, client, runtime, ids, origin, probePaths, held, policy, prepareArgs,
    holdProbes() { hold = true; },
    releaseProbes() { hold = false; for (const res of held) res.end(); },
    async enrolledClient() {
      const peer = client(), enrollment = await peer.startEnrollment('Second owner device');
      await owner.approveEnrollment(enrollment.requestId, account.accountId);
      await peer.previewEnrollment(enrollment.requestId);
      const enrolled = await peer.finishEnrollment(enrollment.requestId, account.accountId);
      assert.equal(enrolled.accountId, account.accountId); return peer;
    },
  };
}

test('real signed async prepare promotes once, preserves URL encoding and recovers receipt without the connector', { timeout: 20000 }, async t => {
  const f = await fixture(t), args = await f.prepareArgs();
  const prepared = await f.owner.extension('apps.source.prepare', args);
  assert.equal(prepared.schema, 'soty.app-source-preparation.v1'); assert.equal(prepared.target.revision, 2);
  assert.equal(prepared.target.entryPath, args.source.entryPath);
  assert.ok(f.probePaths.includes('/%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0?%D0%BA%D0%BB%D1%8E%D1%87=%D1%87%D0%B0%D0%B9'));
  const pending = { appId: f.appId, requestId: 'signed-source-switch', preparationId: prepared.preparationId,
    expectedPolicyEpoch: args.expectedPolicyEpoch, expectedTargetRevision: args.expectedTargetRevision, launchPolicy: 'restricted', listed: false };
  const accepted = await f.owner.extension('apps.source.promote', pending);
  assert.equal(accepted.replayed, false); assert.equal(accepted.current.activeTargetRevision, 2);
  await until(async () => (await f.owner.extension('apps.list')).apps.find(value => value.id === f.appId)?.state === 'ready', 'promoted binding');
  const inspection = await f.owner.extension('apps.inspect', { appId: f.appId });
  assert.equal(inspection.source.observation.evidence, 'connector-v2-observation');
  f.runtime.stop();
  const recovered = await f.owner.extension('apps.source.promote', pending);
  assert.equal(recovered.replayed, true); assert.deepEqual(recovered.receipt, accepted.receipt);
  const history = await f.owner.extension('apps.source.history', { appId: f.appId });
  assert.equal(history.targets.length, 2); assert.equal(history.requiredBindingVersion, 2);
  assert.equal(history.activeTargetRevision, 2);
});

test('real signed prepare releases both SQL writers while waiting and rechecks revocation after HEAD', { timeout: 20000 }, async t => {
  const f = await fixture(t), secondOwner = await f.enrolledClient(), args = await f.prepareArgs('/held');
  f.holdProbes();
  const pending = f.owner.extension('apps.source.prepare', args);
  const result = pending.then(value => ({ value }), error => ({ error }));
  await until(() => f.held.size === 1, 'held candidate HEAD');
  // Another connection can take each writer lock while the network check waits.
  for (const relative of [['apps', 'registry.sqlite'], ['connect', 'accounts.sqlite']]) {
    const path = join(f.dataDir, ...relative); assert.ok(existsSync(path));
    const db = new DatabaseSync(path);
    try { db.exec('PRAGMA busy_timeout=1; BEGIN IMMEDIATE; ROLLBACK;'); } finally { db.close(); }
  }
  assert.equal((await secondOwner.status()).accountId, f.account.accountId);
  const ownerDeviceId = f.account.deviceId;
  assert.ok(ownerDeviceId);
  await secondOwner.revokeDevice(ownerDeviceId);
  f.releaseProbes();
  const settled = await result;
  assert.equal(settled.error?.code, 'apps_authentication_required'); assert.equal(settled.value, undefined);
  const history = await secondOwner.extension('apps.source.history', { appId: f.appId });
  assert.equal(history.targets.length, 1); assert.equal(history.activeTargetRevision, 1); assert.equal(history.requiredBindingVersion, 1);
});

test('signed source commands reject foreign ownership and mismatched account before probing', { timeout: 20000 }, async t => {
  const f = await fixture(t), foreign = f.client(); await foreign.bootstrap('Foreign source user');
  const args = await f.prepareArgs('/private'); const before = f.probePaths.length;
  await assert.rejects(f.owner.extension('apps.source.prepare', { ...args, expectedAccountId: 'different-account' }), code('authentication_required'));
  const { expectedAccountId: _account, ...unboundArgs } = args;
  await assert.rejects(foreign.extension('apps.source.prepare', unboundArgs), code('apps_owner_required'));
  await assert.rejects(foreign.extension('apps.source.history', { appId: f.appId }), code('apps_owner_required'));
  assert.equal(f.probePaths.length, before);
  assert.equal((await f.policy()).activeTargetRevision, 1);
});
