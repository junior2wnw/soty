import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes, generateKeyPairSync } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createLocalAppsRuntime } from '../../scripts/agent-modules/local-apps.mjs';
import { createConnectorScopedFactory } from '../../modules/apps/scoped-embed/connector-host.mjs';
import { createResourceSourceProofVerifier } from '../../modules/apps/scoped-embed/resource-proof.mjs';
import { runtimeTargetDigest } from '../../modules/apps/server/schema.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';

const secret = () => randomBytes(32).toString('base64url');
const hash = value => createHash('sha256').update(value).digest('hex');
const ack = target => ({ scope: target.profile === 'soty.relay-restricted.v1' ? 'whole-port' : 'authenticated-workspace',
  targetRevision: target.revision, targetDigest: target.digest, profile: target.profile });
const pause = ms => new Promise(done => setTimeout(done, ms));
async function until(check) {
  const end = Date.now() + 10000;
  do { const value = await check(); if (value) return value; await pause(20); } while (Date.now() < end);
  throw Error('source_activation_fixture_deadline');
}
function storage() {
  let saved = null;
  return { async read() { return structuredClone(saved); },
    async claim(value) { saved ??= structuredClone(value); return structuredClone(saved); },
    async compareAndSwap(revision, value) { assert.equal(saved.localRevision, revision); saved = structuredClone(value); return structuredClone(saved); } };
}
const wire = (port, host, path) => new Promise((done, fail) => {
  const req = request({ hostname: '127.0.0.1', port, path, headers: { host } }, response => {
    let text = ''; response.on('data', part => { text += part; }); response.once('end', () => done({ status: response.statusCode, text }));
  }); req.once('error', fail); req.end();
});

// Real signed Connect RPC, persistent Root services, actual Apps WebSocket,
// maintained connector runtime and MAC-verified loopback readiness. No UI,
// full pre-promote frame or real external TLS qualification is claimed here.
test('signed owner activates the exact inactive Source alias only with the Source2 and Native1 atomic commit', { timeout: 30000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-activation-http-')), dist = join(directory, 'dist'), dataDir = join(directory, 'root');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Own synthetic Root</title>');
  let app, runtime, verifier, sourceProbes = 0, nativeProbes = 0;
  const clients = [], sockets = new Set();
  const native = createServer((req, res) => { if (req.method === 'HEAD') nativeProbes++; res.end('Own synthetic Native entry'); });
  const source = createServer(async (req, res) => {
    if (req.url === '/api/embed/transport-ready' && req.method === 'HEAD') {
      try { const headers = await verifier.verifyReady(req); sourceProbes++; res.writeHead(204, headers).end(); }
      catch { res.writeHead(403).end(); }
    } else res.end('Own synthetic Source HTML');
  });
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  for (const listener of [native, source, server]) listener.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  t.after(async () => {
    runtime?.stop(); clients.forEach(client => client.dispose()); await app?.locals.closeServices();
    sockets.forEach(socket => socket.destroy());
    await Promise.all([native, source, server].map(listener => listener.listening ? new Promise(done => listener.close(done)) : undefined));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-source-activation-http-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 4, retryDelay: 50 });
  });
  await Promise.all([native, source, server].map(listener => new Promise(done => listener.listen(0, '127.0.0.1', done))));
  const port = server.address().port, origin = 'http://127.0.0.1:' + port;
  const options = { dataDir, connectOrigins: [origin], appOriginTemplate: 'https://{appId}.legacy.localhost', namedAppZone: 'https://named.localhost' };
  app = createHttpApp(dist, options);
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  const client = () => {
    const value = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
      fetch: (url, opts) => fetch(url, { ...opts, headers: { ...opts.headers, origin } }) }, storage());
    clients.push(value); return value;
  };
  const owner = client(), account = await owner.bootstrap('Own Source owner');
  const foreign = client(), other = await foreign.bootstrap('Own other person');
  const identity = { linkId: 'activation_http_link_12345678901234567890', hostDeviceId: 'activation_http_host', connectorId: 'activation_http_connector' }, token = secret();
  const registration = await fetch(origin + '/api/connectors/register', { method: 'POST',
    headers: { 'content-type': 'application/json', authorization: 'Bearer ' + token }, body: JSON.stringify({ linkId: identity.linkId,
      deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  assert.equal((await registration.json()).ok, true);
  const startRuntime = scopedFactory => {
    runtime = createLocalAppsRuntime({ randomSecret: secret, digest: hash, createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request,
      encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
    { identity, token, serverUrl: origin, ...(scopedFactory ? { scopedFactory } : {}) });
    runtime.start();
  };
  startRuntime(); await until(() => runtime.status().connected);
  const claim = await runtime.claim(), ids = { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId };
  await owner.extension('apps.claim', { ...ids, claimCode: claim.claimCode });
  const appId = (await owner.extension('apps.register', { ...ids, name: 'Synthetic preserved Native', port: native.address().port, entryPath: '/' })).app.id;
  const inspect = () => owner.extension('apps.inspect', { appId }), policy = () => owner.extension('apps.publication.get', { appId });
  const alias = async slug => {
    const current = await inspect(); const result = await owner.extension('apps.domains.claim', { appId, slug,
      requestId: 'activation_claim_' + slug, expectedDomainsRevision: current.addresses.revision });
    return { domainId: result.receipt.domainId, origin: result.receipt.origin };
  };
  const nativeAlias = await alias('native-activation'), sourceAlias = await alias('source-activation');
  const initial = await policy();
  await owner.extension('apps.publication.update', { appId, requestId: 'activation_native_public', expectedPolicyEpoch: initial.policyEpoch,
    expectedTargetRevision: 1, launchPolicy: 'anyone', listed: true, activeDomainIds: [nativeAlias.domainId], exposureAck: ack(initial.target) });
  const target = { appId, revision: 2, ownerAccountId: account.accountId, connectorKey: [identity.linkId, ids.hostDeviceId, ids.connectorId].join('|'),
    port: source.address().port, entryPath: '/embed', profile: 'soty.selected-human-embed.v2' };
  target.digest = runtimeTargetDigest(target);
  const profile = { schema: target.profile, appId, connector: identity, target: { revision: 2, digest: target.digest }, sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    resource: { registryId: 'soty', tenantId: account.accountId, appId, environmentId: 'production', resourceId: 'workspace',
      selection: { kind: 'soty.workspace.v1', nativeId: 'activation-fixture', incarnationId: 'one' } }, issuer: origin + '/human-identity',
    clientId: 'activation-fixture', embedOrigin: sourceAlias.origin, nativeOrigin: nativeAlias.origin, parentOrigin: origin };
  const key = randomBytes(32), seen = new Set();
  verifier = createResourceSourceProofVerifier({ profile, key, consumeNonce: nonce => { if (seen.has(nonce)) return false; seen.add(nonce); return true; } });
  runtime.stop(); await app.locals.closeServices();
  const jwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' }); Object.assign(jwk, { kid: 'activation-fixture', use: 'sig', alg: 'RS256' });
  app = createHttpApp(dist, { ...options, allowScopedEmbedMigration: true, allowSelectedResourceMigration: true, scopedEmbedProfiles: [profile],
    humanIdentity: { enabled: true, issuer: profile.issuer, registryId: 'soty', environmentId: 'production', clients: [],
      jwks: { keys: [jwk] }, cookieKeys: [secret()], artifactKey: randomBytes(32), artifactKeyId: 'activation-fixture' } });
  startRuntime(createConnectorScopedFactory({ entries: [{ profile, key: key.toString('base64url') }] }));
  await until(async () => (await inspect()).source.integrations?.some(value => value.profile === target.profile));
  const current = await inspect(); assert.equal(current.source.sourceEntryActivation, true);
  assert.equal((await (await fetch(origin + '/api/apps/capabilities')).json()).sourceEntryActivation, true);
  assert.equal(current.addresses.aliases.find(value => value.id === sourceAlias.domainId).active, false);
  const db = new DatabaseSync(join(dataDir, 'apps', 'registry.sqlite'), { readOnly: true });
  const rows = () => Object.fromEntries(['app_runtime_targets', 'app_publications', 'app_publication_domains', 'app_source_receipts', 'app_scoped_embed_admissions']
    .map(name => [name, db.prepare('SELECT * FROM ' + name + ' ORDER BY rowid').all()]));
  try {
    const before = rows(), args = { appId, expectedAccountId: account.accountId, expectedPolicyEpoch: current.publication.policyEpoch,
      expectedTargetRevision: 1, source: { ...ids, port: target.port, entryPath: '/embed', profile: target.profile },
      nativeEntry: { origin: nativeAlias.origin, targetRevision: 1 }, sourceEntryAction: { action: 'activate', domainId: sourceAlias.domainId } };
    const noAction = { ...args }; delete noAction.sourceEntryAction;
    await assert.rejects(owner.extension('apps.source.prepare', noAction), { code: 'app_native_entry_domain_required' });
    await assert.rejects(foreign.extension('apps.source.prepare', { ...args, expectedAccountId: other.accountId }), { code: 'apps_owner_required' });
    assert.equal(sourceProbes, 0); assert.deepEqual(rows(), before);
    const prepared = await owner.extension('apps.source.prepare', args);
    assert.equal(sourceProbes, 1); assert.ok(nativeProbes > 0); assert.deepEqual(rows(), before);
    assert.deepEqual(prepared.sourceEntry, sourceAlias); assert.deepEqual(prepared.sourceEntryAction, args.sourceEntryAction);
    // The public router represents an inactive published route as unavailable.
    assert.equal((await wire(port, new URL(sourceAlias.origin).host, '/embed')).status, 503);
    const promote = { appId, expectedAccountId: account.accountId, requestId: 'activation_signed_commit', preparationId: prepared.preparationId,
      expectedPolicyEpoch: current.publication.policyEpoch, expectedTargetRevision: 1, launchPolicy: 'anyone', listed: true,
      exposureAck: ack(prepared.target), nativeExposureAck: ack(prepared.nativeEntry.target), sourceEntryAction: args.sourceEntryAction };
    const peer = client(), enrollment = await peer.startEnrollment('Own second owner device');
    await owner.approveEnrollment(enrollment.requestId, account.accountId); await peer.previewEnrollment(enrollment.requestId);
    assert.equal((await peer.finishEnrollment(enrollment.requestId, account.accountId)).accountId, account.accountId);
    await assert.rejects(peer.extension('apps.source.promote', promote), { code: 'apps_source_preparation_mismatch' });
    assert.deepEqual(rows(), before);
    const missing = { ...promote }; delete missing.sourceEntryAction;
    await assert.rejects(owner.extension('apps.source.promote', missing), { code: 'app_source_entry_action_mismatch' });
    assert.deepEqual(rows(), before);
    const accepted = await owner.extension('apps.source.promote', promote);
    assert.deepEqual(accepted.receipt.sourceEntry, sourceAlias); assert.deepEqual(accepted.receipt.sourceEntryAction, args.sourceEntryAction);
    assert.deepEqual(accepted.current.activeDomainIds, [nativeAlias.domainId, sourceAlias.domainId].sort());
    await until(async () => (await inspect()).source.binding.state === 'bound');
    assert.equal((await inspect()).source.revision, 2);
    const replay = await owner.extension('apps.source.promote', promote); assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, accepted.receipt);
    assert.equal(rows().app_runtime_targets.length, 2); assert.equal(rows().app_source_receipts.length, 1); assert.equal(rows().app_scoped_embed_admissions.length, 1);
  } finally { db.close(); }
});
