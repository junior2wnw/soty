import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';

for (const version of [0, 1, 2, 3]) test(`actual Root capabilities expose Human sign-in management only for schema ${version}`, async t => {
  const folder = await mkdtemp(join(tmpdir(), 'soty-human-signins-cap-')), dist = join(folder, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Owned fixture</title>');
  let app, client;
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  t.after(async () => {
    client?.dispose(); server.closeAllConnections(); await new Promise(done => server.close(done)); await app?.locals.closeServices();
    assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^soty-human-signins-cap-/u);
    await rm(folder, { recursive: true, force: true });
  });
  const origin = 'http://127.0.0.1:' + server.address().port;
  const privateJwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(privateJwk, { kid: 'signins-capability-fixture', alg: 'RS256', use: 'sig' });
  const clientIds = ['app-signins-fixture'];
  const humanIdentity = version ? { enabled: true, issuer: origin + '/human-identity', registryId: 'soty', environmentId: 'production',
    clients: [{ id: clientIds[0], label: 'Owned fixture', redirectUri: 'http://127.0.0.1:9/callback',
      clientSecret: randomBytes(32).toString('base64url'), version: 1 }], jwks: { keys: [privateJwk] },
    cookieKeys: [randomBytes(32).toString('base64url')], artifactKey: randomBytes(32), artifactKeyId: 'signins-capability-fixture',
    ...(version >= 2 ? { renewal: { admissionEnabled: true, clientIds } } : {}),
    ...(version === 3 ? { deviceConsent: { admissionEnabled: true, clientIds } } : {}) } : undefined;
  app = createHttpApp(dist, { env: {}, appHosting: {}, dataDir: join(folder, 'data'), connectOrigins: [origin], humanIdentity,
    humanIdentityRenewalMigration: version >= 2, humanIdentityDeviceConsentMigration: version === 3 });
  const response = await fetch(origin + '/api/apps/capabilities', { signal: AbortSignal.timeout(5000) });
  assert.equal(response.status, 200); assert.equal(response.headers.get('cache-control'), 'no-store');
  const result = (await response.json()).humanSignins;
  assert.equal(result.schema, 'soty.human-signins-capability.v1');
  assert.equal(result.available, version >= 2); assert.equal(result.sessions, version >= 2);
  assert.equal(result.rememberedConsents, version === 3); assert.equal(result.sourceRootSessions, false);
  assert.equal(result.checkRevoke, version >= 2);
  assert.deepEqual(result.scopes, version >= 2 ? ['current-device', 'account'] : []);
  assert.equal(result.maximumPageSize, version >= 2 ? 20 : 0);
  let stored = null;
  client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
    fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }) }, {
    async read() { return structuredClone(stored); }, async claim(next) { stored ??= structuredClone(next); return structuredClone(stored); },
    async compareAndSwap(revision, next) { assert.equal(stored.localRevision, revision); stored = structuredClone(next); return structuredClone(stored); }
  });
  const account = await client.bootstrap('Owned synthetic profile');
  const call = () => client.extension('identity.human.signins.list', { expectedAccountId: account.accountId, scope: 'current-device' },
    { expectedAccountId: account.accountId });
  if (version >= 2) {
    const body = await call(); assert.equal(body.schema, 'soty.human-signins.v1');
    assert.equal(body.capabilities.available, true); assert.deepEqual(body.entries, []);
  } else await assert.rejects(call(), error => error.code === 'unsupported_operation');
});
