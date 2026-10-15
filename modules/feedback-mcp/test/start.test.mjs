import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync, existsSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { request } from 'node:http';
import { createResourceSourceProofSigner } from '../../apps/scoped-embed/resource-proof.mjs';
import { environment } from '../../human-identity/test/support/renewal-fixture.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../source-app/server/index.mjs';
import { readFeedbackConfiguration, startFeedbackService } from '../start.mjs';

const random = () => randomBytes(32).toString('base64url');
function directory(t) {
  const path = mkdtempSync(join(tmpdir(), 'feedback-start-'));
  t.after(() => {
    assert.equal(dirname(resolve(path)), resolve(tmpdir()));
    assert.match(basename(path), /^feedback-start-/);
    rmSync(path, { recursive: true, force: true, maxRetries: 5, retryDelay: 40 });
  });
  return path;
}

test('Source cache configuration cannot select domain/auth files or leave the data directory', t => {
  const path = directory(t), file = join(path, 'config.json');
  const config = { schema: 'soty.feedback-service.v1', origin: 'https://feedback.example', port: 19244,
    dataDirectory: path, auth: { encryptionKey: random(), cookieKeys: [random()], upstream: {} }, source: {} };
  const read = databasePath => {
    config.source = databasePath === undefined ? {} : { databasePath };
    writeFileSync(file, JSON.stringify(config), { mode: 0o600 });
    return readFeedbackConfiguration(file);
  };
  assert.equal(read().source.databasePath, undefined);
  assert.equal(read(join(path, 'source-native2.sqlite')).source.databasePath, join(path, 'source-native2.sqlite'));
  for (const value of [null, 0, 'source-native2.sqlite', join(path, 'feedback.sqlite'), join(path, 'projects.sqlite'),
    join(path, 'auth.sqlite'), join(path, 'auth', 'source-native2.sqlite'), join(path, '..', 'source-native2.sqlite')]) {
    assert.throws(() => read(value), error => error.code === 'configuration_source_database_invalid');
  }
});

test('a new target uses a separate Source cache and preserves its previous cache, keys and domain data', async t => {
  const upstream = await environment(t), path = directory(t), origin = 'http://127.0.0.1:51357';
  let client;
  await upstream.configureClients(clients => {
    clients[0].redirectUri = origin + '/auth/callback'; clients[0].version++; client = clients[0]; return clients;
  });
  const appId = 'app-' + 'a'.repeat(32);
  const profile = { schema: 'soty.selected-human-embed.v2', appId,
    connector: { linkId: 'fixture', hostDeviceId: 'host', connectorId: 'connector' },
    target: { revision: 1, digest: '1'.repeat(64) }, sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    resource: { registryId: 'soty', environmentId: 'fixture', tenantId: 'owner', appId, resourceId: 'feedback',
      selection: { kind: 'soty.workspace.v1', nativeId: 'feedback', incarnationId: 'one' } },
    issuer: upstream.issuer, clientId: client.id, embedOrigin: origin, nativeOrigin: origin,
    parentOrigin: new URL(upstream.issuer).origin };
  const config = { schema: 'soty.feedback-service.v1', origin, port: 0, dataDirectory: path,
    auth: { encryptionKey: random(), cookieKeys: [random()], upstream: { issuer: upstream.issuer,
      clientId: client.id, clientSecret: client.clientSecret, redirectUri: client.redirectUri,
      renewalProfile: 'soty.human-rp-renewal.v1' } },
    source: { profile, transportKey: random(), connectorPort: 49428, storageKey: random(), storageKeyId: 'fixture' } };
  const keys = { transportKey: config.source.transportKey, storageKey: config.source.storageKey,
    storageKeyId: config.source.storageKeyId, auth: structuredClone(config.auth) };
  let running;
  t.after(async () => { await running?.close(); });
  running = await startFeedbackService(config);
  const identity = running.service.identity(upstream.issuer, 'same-human');
  const project = running.service.execute(running.service.actor(identity), 'project_create',
    { name: 'Retained queue', requestId: 'cache-change-project-1' }).project;
  await running.close(); running = undefined;
  const originalCache = readFileSync(join(path, 'source.sqlite'));
  config.source.profile = { ...profile, target: { revision: 2, digest: '2'.repeat(64) } };
  await assert.rejects(startFeedbackService(config), error => error.code === 'feedback_source_storage_profile_changed');
  config.source.databasePath = join(path, 'source-native2.sqlite');
  running = await startFeedbackService(config);
  assert.equal(running.source.enabled, true);
  const sameIdentity = running.service.identity(upstream.issuer, 'same-human');
  assert.equal(sameIdentity, identity);
  assert.equal(running.service.execute(running.service.actor(sameIdentity), 'projects_list').projects[0].id, project.id);
  await running.close(); running = undefined;
  assert.equal(existsSync(config.source.databasePath), true);
  assert.deepEqual(readFileSync(join(path, 'source.sqlite')), originalCache);
  assert.deepEqual({ transportKey: config.source.transportKey, storageKey: config.source.storageKey,
    storageKeyId: config.source.storageKeyId, auth: config.auth }, keys);
  for (const file of ['source.sqlite', 'source-native2.sqlite']) {
    const db = new DatabaseSync(join(path, file), { readOnly: true });
    try { assert.equal(db.prepare('SELECT count(*) AS n FROM source_sessions').get().n, 0); }
    finally { db.close(); }
  }
});

test('HTTPS deployment accepts MAC-authenticated loopback Source while public and unsigned requests remain closed', async t => {
  const upstream = await environment(t), path = directory(t), origin = 'https://feedback.example';
  let client;
  await upstream.configureClients(clients => {
    clients[0].redirectUri = origin + '/auth/callback'; clients[0].version++; client = clients[0]; return clients;
  });
  const appId = 'app-' + 'b'.repeat(32), transportKey = randomBytes(32);
  const profile = { schema: 'soty.selected-human-embed.v2', appId,
    connector: { linkId: 'fixture', hostDeviceId: 'host', connectorId: 'connector' },
    target: { revision: 2, digest: '2'.repeat(64) }, sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    resource: { registryId: 'soty', environmentId: 'fixture', tenantId: 'owner', appId, resourceId: 'feedback',
      selection: { kind: 'soty.workspace.v1', nativeId: 'feedback', incarnationId: 'one' } },
    issuer: upstream.issuer, clientId: client.id, embedOrigin: origin, nativeOrigin: origin,
    parentOrigin: new URL(upstream.issuer).origin };
  const config = { schema: 'soty.feedback-service.v1', origin, port: 0, dataDirectory: path,
    auth: { encryptionKey: random(), cookieKeys: [random()], upstream: { issuer: upstream.issuer,
      clientId: client.id, clientSecret: client.clientSecret, redirectUri: client.redirectUri,
      renewalProfile: 'soty.human-rp-renewal.v1' } },
    source: { profile, transportKey: transportKey.toString('base64url'), connectorPort: 49428,
      storageKey: random(), storageKeyId: 'fixture' } };
  const running = await startFeedbackService(config);t.after(() => running.close());
  try {
  const signer = createResourceSourceProofSigner({ profile, key: transportKey });
  const send = (url, headers = {}, method = 'GET', body) => new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port: running.server.address().port, path: url,
      method, agent: false, headers: { host: 'feedback.example', connection: 'close', ...headers } }, res => {
      const parts = [];res.on('data', p => parts.push(p));res.on('end', () => resolve({ status: res.statusCode,
        headers: { get: n => res.headers[n.toLowerCase()] ?? null }, body: Buffer.concat(parts).toString() }));
    });req.on('error', reject);req.end(body);
  });
  const probe = signer.probeHeaders();
  const ready = await send('/api/embed/transport-ready', probe.headers, 'HEAD');
  assert.equal(ready.status, 204);assert.equal(signer.verifyReady(ready, probe.request), true);
  assert.equal((await send('/api/embed/transport-ready', probe.headers, 'HEAD')).status, 403, 'consumed nonce cannot replay');
  for (const url of ['/api/embed/transport-ready', '/api/embed/query']) {
    const result = await send(url, {}, url.endsWith('transport-ready') ? 'HEAD' : 'GET');
    assert.ok(result.status >= 400, 'Source proof remains required');
  }
  for (const url of ['/api/embed/query', '/api/embed/invoke']) {
    const result = await send(url, { 'content-type': 'application/json' }, 'POST', '{}');
    assert.equal(result.status, 403, 'unsigned Source data/action requests remain denied');
  }
  // /embed is a public static shell; its queue requests still need Root proof.
  const shell = await send('/embed');assert.equal(shell.status, 200);assert.match(shell.body, /\/assets\/app\.js/);
  for (const url of ['/api/me', '/mcp', '/auth/login', '/health', '/api/embed/../../auth/login',
    '/api/embed/../../api/me', '/api/embed/%2e%2e/../auth/session']) {
    const result = await send(url);assert.equal(result.status, 403);
    assert.equal(JSON.parse(result.body).error.code, 'https_required');
  }
  assert.equal((await send('/api/me', { 'x-forwarded-proto': 'https' })).status, 200, 'existing TLS proxy path works');
  } finally { await running.close(); }
});
