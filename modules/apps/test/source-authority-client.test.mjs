import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import { createHmac, randomBytes } from 'node:crypto';
import { createConnectorScopedFactory } from '../scoped-embed/connector-host.mjs';
import { createSourceAuthorityClient } from '../scoped-embed/source-authority-client.mjs';
import { selectedResourceProfile } from '../scoped-embed/resource-profile.mjs';
import { HIVE_SELECTED_SOURCE } from '../scoped-embed/resource-route-adapters.mjs';

// Synthetic localhost authority fixture; the real installed IPC verifier is used unchanged.
async function fixture(t, { issuerClock, receiverClock, changeContext = value => value } = {}) {
  const raw = { schema: 'soty.selected-human-embed.v2', appId: 'app-' + 'a'.repeat(32),
    connector: { linkId: 'link', hostDeviceId: 'device', connectorId: 'connector' },
    target: { revision: 1, digest: '1'.repeat(64) }, sourceProfile: { ...HIVE_SELECTED_SOURCE },
    resource: { registryId: 'soty', tenantId: 'root-owner', appId: 'app-' + 'a'.repeat(32), environmentId: 'production',
      resourceId: 'selected', selection: { kind: 'hive.project.v1', nativeId: 'selected-native', incarnationId: 'one' } },
    issuer: 'https://root.test/human-identity', clientId: 'hive.selected', embedOrigin: 'https://hive.root.test',
    nativeOrigin: 'https://native.test', parentOrigin: 'https://root.test' };
  const profile = selectedResourceProfile(raw), key = randomBytes(32), observations = [];
  const context = { profileDigest: profile.digest, reference: { id: randomBytes(32).toString('base64url'), version: 1, digest: '9'.repeat(64) } };
  let rootReads = 0;
  const factory = createConnectorScopedFactory({ entries: [{ profile: raw, key: key.toString('base64url') }], clock: receiverClock });
  factory.connect(async parameters => { rootReads++; assert.deepEqual(parameters.connector, profile.connector);
    assert.deepEqual(parameters.reference, context.reference); return changeContext(context); }, profile.connector);
  const server = createServer((req, res) => {
    const chunks = []; req.on('data', bytes => chunks.push(bytes)); req.on('end', () => observations.push(JSON.parse(Buffer.concat(chunks))));
    void factory.sourceAuthority(req, res).catch(error => {
      res.writeHead(error.status === 403 ? 403 : 503, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ code: error.code }));
    });
  });
  t.after(async () => { factory.disconnected(); server.closeAllConnections(); await new Promise((done, reject) => server.close(error => error ? reject(error) : done())); });
  await new Promise((done, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', done); });
  const port = server.address().port, client = createSourceAuthorityClient({ profile: raw, key, connectorPort: port, clock: issuerClock });
  const read = () => client({ reference: context.reference, connector: profile.connector });
  const rawPost = value => new Promise((done, reject) => {
    const text = JSON.stringify(value), signature = createHmac('sha256', key).update('authority\0' + text).digest('base64url');
    const req = httpRequest({ hostname: '127.0.0.1', port, path: '/apps/scoped/authority', method: 'POST', agent: false,
      headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text), 'x-soty-source-mac': signature } }, res => {
      const parts = []; res.on('data', bytes => parts.push(bytes)); res.on('end', () => done({ status: res.statusCode, body: JSON.parse(Buffer.concat(parts)) }));
      res.on('error', reject);
    }); req.on('error', reject); req.end(text);
  });
  const issued = expiresAt => ({ schema: 'soty.selected-source-authority.v1', appId: profile.appId, profileDigest: profile.digest,
    reference: context.reference, nonce: randomBytes(32).toString('base64url'), expiresAt });
  return { read, client, connector: profile.connector, rawPost, issued, observations, context, rootReads: () => rootReads };
}

test('actual Source IPC issuer +/-600ms returns the same current reference and its budget is9s', { timeout: 5000 }, async t => {
  for (const skew of [-600, 600]) {
    const now = 100000, f = await fixture(t, { issuerClock: () => now + skew, receiverClock: () => now });
    assert.deepEqual(await f.read(), f.context);
    assert.equal(f.observations.length, 1);
    assert.equal(f.observations[0].expiresAt, now + skew + 9000);
    assert.equal(f.rootReads(), 1);
  }
});

test('actual Source IPC ahead1s admits and ahead1001ms denies without entering Root authority', { timeout: 5000 }, async t => {
  for (const skew of [1000, 1001]) {
    const f = await fixture(t, { issuerClock: () => 100000 + skew, receiverClock: () => 100000 });
    if (skew === 1000) { assert.deepEqual(await f.read(), f.context); assert.equal(f.rootReads(), 1); }
    else { await assert.rejects(f.read(), { code: 'scoped_embed_authority_denied', status: 403 }); assert.equal(f.rootReads(), 0); }
  }
});

test('installed IPC receiver keeps exact10s upper bound, expired denial and one-use nonce', { timeout: 5000 }, async t => {
  let now = 100000;
  const f = await fixture(t, { issuerClock: () => now, receiverClock: () => now });
  assert.equal((await f.rawPost(f.issued(now))).status, 403);
  assert.equal((await f.rawPost(f.issued(now + 10001))).status, 403);
  assert.equal(f.rootReads(), 0);
  const exact = f.issued(now + 10000);
  assert.equal((await f.rawPost(exact)).status, 200);
  assert.equal((await f.rawPost(exact)).body.code, 'scoped_embed_ipc_replayed');
  assert.equal(f.rootReads(), 1);
  const unused = f.issued(now + 9000);
  now += 9000;
  const expired = await f.rawPost(unused);
  assert.equal(expired.status, 403); assert.equal(expired.body.code, 'scoped_embed_ipc_invalid');
  assert.equal(f.rootReads(), 1);
});

test('current profile/reference and connector mismatches still deny without a replacement authority', { timeout: 5000 }, async t => {
  const clocks = { issuerClock: () => 100000, receiverClock: () => 100000 };
  const wrongProfile = await fixture(t, { ...clocks, changeContext: value => ({ ...value, profileDigest: '0'.repeat(64) }) });
  await assert.rejects(wrongProfile.read(), { code: 'scoped_embed_authority_denied', status: 403 });
  const wrongReference = await fixture(t, { ...clocks, changeContext: value => ({ ...value,
    reference: { ...value.reference, version: value.reference.version + 1 } }) });
  await assert.rejects(wrongReference.read(), { code: 'scoped_embed_authority_mismatch', status: 403 });
  assert.equal(wrongProfile.rootReads(), 1); assert.equal(wrongReference.rootReads(), 1);
  await assert.rejects(wrongReference.client({ reference: wrongReference.context.reference,
    connector: { ...wrongReference.connector, hostDeviceId: 'foreign-device' } }), { code: 'scoped_embed_connector_mismatch', status: 403 });
  assert.equal(wrongReference.observations.length, 1); assert.equal(wrongReference.rootReads(), 1);
});
