import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { selectedResourceProfile } from '../scoped-embed/resource-profile.mjs';
import { HIVE_SELECTED_SOURCE } from '../scoped-embed/resource-route-adapters.mjs';
import { createResourceSourceProofSigner, createResourceSourceProofVerifier } from '../scoped-embed/resource-proof.mjs';
function fixture({ clock = Date.now, consume } = {}) {
  const raw = { schema: 'soty.selected-human-embed.v2', appId: 'app-' + 'a'.repeat(32), connector: { linkId: 'link', hostDeviceId: 'device', connectorId: 'connector' },
    target: { revision: 1, digest: '1'.repeat(64) }, sourceProfile: { ...HIVE_SELECTED_SOURCE }, resource: { registryId: 'soty', tenantId: 'root-owner', appId: 'app-' + 'a'.repeat(32),
      environmentId: 'production', resourceId: 'selected', selection: { kind: 'hive.project.v1', nativeId: '界'.repeat(1365), incarnationId: 'native-scope' } },
    issuer: 'https://root.test/human-identity', clientId: 'hive.selected', embedOrigin: 'https://hive.root.test', nativeOrigin: 'https://native.test', parentOrigin: 'https://root.test' };
  const profile = selectedResourceProfile(raw), key = randomBytes(32), consumed = new Set();
  const context = { schema: 'soty.verified-launch-continuation.v2', reference: { id: randomBytes(32).toString('base64url'), version: 1, digest: '9'.repeat(64) },
    profileDigest: profile.digest, appId: profile.appId, sourceProfile: profile.sourceProfile, resource: profile.resource,
    rootPrincipal: { accountId: 'root-current', deviceId: 'root-device' }, humanPrincipal: { issuer: profile.issuer, subject: 'root-oidc-sub', clientId: profile.clientId,
      clientProfileDigest: '3'.repeat(64), clientGeneration: 1 }, entry: { domainId: 'domain', origin: profile.embedOrigin }, target: profile.target, policyEpoch: 1, expiresAt: clock() + 300000 };
  const signer = createResourceSourceProofSigner({ profile: raw, key, clock }), verifier = createResourceSourceProofVerifier({ profile: raw, key, clock,
    consumeNonce: consume ?? (async nonce => { if (consumed.has(nonce)) return false; consumed.add(nonce); return true; }) });
  const request = (path = '/api/embed/project', body = Buffer.alloc(0), method = 'GET', cookie = '') => new Request(profile.embedOrigin + path,
    { method, headers: { ...signer.headers({ context, path, method, body, cookie }), ...(cookie ? { cookie } : {}) }, ...(body.length ? { body } : {}) });
  return { raw, context, signer, verifier, request };
}
test('full4095-byte Unicode Native locator fits real MAC envelope and stays private/exact', async () => {
  const f = fixture(), request = f.request(); const result = await f.verifier.verify(request);
  assert.equal(result.resource.selection.nativeId, f.context.resource.selection.nativeId);
  assert.ok(Object.isFrozen(result.resource.selection)); await assert.rejects(f.verifier.verify(request), { code: 'scoped_embed_proof_replayed' });
});
test('MAC request binds actual body, cookie, route and Native incarnation; duplicate/foreign requests never consume authority', async () => {
  const f = fixture(), body = Buffer.from('{"operations":[]}');
  const valid = f.request('/api/embed/operations', body, 'POST', 'soty_rp_session=opaque');
  await assert.rejects(f.verifier.verify(valid, { body: Buffer.from('{"operations":[1]}') }), { code: 'scoped_embed_proof_mismatch' });
  const bad = new Request(valid.url, { method: 'POST', body, headers: { ...Object.fromEntries(valid.headers), cookie: 'soty_rp_session=other' } });
  await assert.rejects(f.verifier.verify(bad, { body }), { code: 'scoped_embed_proof_mismatch' });
  const other = new Request('https://other.test/api/embed/operations', { method: 'POST', headers: valid.headers, body });
  await assert.rejects(f.verifier.verify(other, { body }));
  assert.equal((await f.verifier.verify(valid, { body })).resource.selection.incarnationId, 'native-scope');
  const replay = new Request(valid.url, { method: 'POST', headers: valid.headers, body }); await assert.rejects(f.verifier.verify(replay, { body }), { code: 'scoped_embed_proof_replayed' });
  const forged = { ...f.context, resource: { ...f.context.resource, selection: { ...f.context.resource.selection, incarnationId: 'reused-id' } } };
  assert.throws(() => f.signer.headers({ context: forged, method: 'GET', path: '/api/embed/project' }));
});
test('async durable nonce late expiry and concurrent same Request reject; no ready flag can substitute a verifier', async () => {
  let now = 100000, resolve; const f = fixture({ clock: () => now, consume: () => new Promise(done => { resolve = done; }) });
  const request = f.request(), pending = f.verifier.verify(request);
  await assert.rejects(f.verifier.verify(request), { code: 'scoped_embed_proof_replayed' });
  now += 10001; resolve(true); await assert.rejects(pending, { code: 'scoped_embed_proof_expired' });
  assert.throws(() => f.verifier.context(request));
  await assert.rejects(f.verifier.verify(new Request('https://hive.root.test/api/embed/project', { headers: { ready: 'true' } })));
});
test('feedback1.5M allowance never widens project mutation or legacy proof; safe limits precede nonce consumption', async () => {
  const f = fixture(), bytes = Buffer.alloc(1400000);
  const request = f.request('/api/embed/feedback', bytes, 'POST'); assert.equal((await f.verifier.verify(request, { body: bytes })).appId, f.context.appId);
  assert.throws(() => f.request('/api/embed/operations', bytes, 'POST'), { code: 'scoped_embed_request_limit' });
  assert.throws(() => f.request('/api/embed/feedback', Buffer.alloc(1500001), 'POST'), { code: 'scoped_embed_request_limit' });
});
