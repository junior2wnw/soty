import test from 'node:test';
import assert from 'node:assert/strict';
import { createHmac, randomBytes } from 'node:crypto';
import { selectedResourceProfile } from '../scoped-embed/resource-profile.mjs';
import { HIVE_SELECTED_SOURCE } from '../scoped-embed/resource-route-adapters.mjs';
import { createResourceSourceProofSigner, createResourceSourceProofVerifier } from '../scoped-embed/resource-proof.mjs';
function fixture({ clock = Date.now, verifierClock = clock, consume } = {}) {
  const raw = { schema: 'soty.selected-human-embed.v2', appId: 'app-' + 'a'.repeat(32), connector: { linkId: 'link', hostDeviceId: 'device', connectorId: 'connector' },
    target: { revision: 1, digest: '1'.repeat(64) }, sourceProfile: { ...HIVE_SELECTED_SOURCE }, resource: { registryId: 'soty', tenantId: 'root-owner', appId: 'app-' + 'a'.repeat(32),
      environmentId: 'production', resourceId: 'selected', selection: { kind: 'hive.project.v1', nativeId: '界'.repeat(1365), incarnationId: 'native-scope' } },
    issuer: 'https://root.test/human-identity', clientId: 'hive.selected', embedOrigin: 'https://hive.root.test', nativeOrigin: 'https://native.test', parentOrigin: 'https://root.test' };
  const profile = selectedResourceProfile(raw), key = randomBytes(32), consumed = new Set();
  const context = { schema: 'soty.verified-launch-continuation.v2', reference: { id: randomBytes(32).toString('base64url'), version: 1, digest: '9'.repeat(64) },
    profileDigest: profile.digest, appId: profile.appId, sourceProfile: profile.sourceProfile, resource: profile.resource,
    rootPrincipal: { accountId: 'root-current', deviceId: 'root-device' }, humanPrincipal: { issuer: profile.issuer, subject: 'root-oidc-sub', clientId: profile.clientId,
      clientProfileDigest: '3'.repeat(64), clientGeneration: 1 }, entry: { domainId: 'domain', origin: profile.embedOrigin }, target: profile.target, policyEpoch: 1, expiresAt: clock() + 300000 };
  const signer = createResourceSourceProofSigner({ profile: raw, key, clock }), verifier = createResourceSourceProofVerifier({ profile: raw, key, clock: verifierClock,
    consumeNonce: consume ?? (async nonce => { if (consumed.has(nonce)) return false; consumed.add(nonce); return true; }) });
  const request = (path = '/api/embed/project', body = Buffer.alloc(0), method = 'GET', cookie = '') => new Request(profile.embedOrigin + path,
    { method, headers: { ...signer.headers({ context, path, method, body, cookie }), ...(cookie ? { cookie } : {}) }, ...(body.length ? { body } : {}) });
  return { raw, context, signer, verifier, request, key, consumed };
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

const probeRequest = (f, issued) => new Request(f.raw.embedOrigin + '/api/embed/transport-ready', { method: 'HEAD', headers: issued.headers });
const decode = headers => JSON.parse(Buffer.from(headers['x-soty-selected-proof'], 'base64url').toString('utf8'));

test('normal and HEAD proofs admit issuer clocks +/-600ms with the same private profile and no verifier widening', async () => {
  for (const skew of [-600, 600]) {
    const receiverNow = 100000, issuerNow = receiverNow + skew;
    const f = fixture({ clock: () => issuerNow, verifierClock: () => receiverNow });
    const ordinary = f.request();
    assert.equal((await f.verifier.verify(ordinary)).profileDigest, f.context.profileDigest);
    const issued = f.signer.probeHeaders(), reply = await f.verifier.verifyReady(probeRequest(f, issued));
    assert.equal(f.signer.verifyReady({ status: 204, headers: new Headers(reply) }, issued.request), true);
    assert.equal(f.consumed.size, 2);
  }
});

test('issuer budget is9s for probe and normal proof; a shorter Root context stays the exact upper fence', () => {
  const now = 100000, f = fixture({ clock: () => now });
  assert.equal(f.signer.probeHeaders().request.expiresAt, now + 9000);
  assert.equal(decode(f.signer.headers({ context: f.context, method: 'GET', path: '/api/embed/project' })).expiresAt, now + 9000);
  const context = { ...f.context, expiresAt: now + 125 };
  assert.equal(decode(f.signer.headers({ context, method: 'GET', path: '/api/embed/project' })).expiresAt, context.expiresAt);
  assert.throws(() => f.signer.headers({ context: { ...context, expiresAt: now }, method: 'GET', path: '/api/embed/project' }), { code: 'scoped_embed_expired' });
});

test('the1s ahead bound is exact; +1001ms rejects before durable nonce consumption', async () => {
  for (const skew of [1000, 1001]) {
    const f = fixture({ clock: () => 100000 + skew, verifierClock: () => 100000 });
    const issued = f.signer.probeHeaders();
    if (skew === 1000) {
      await f.verifier.verify(f.request());
      await f.verifier.verifyReady(probeRequest(f, issued));
      assert.equal(f.consumed.size, 2);
    } else {
      await assert.rejects(f.verifier.verify(f.request()), { code: 'scoped_embed_proof_expired', status: 401 });
      await assert.rejects(f.verifier.verifyReady(probeRequest(f, issued)), { code: 'scoped_embed_proof_expired', status: 401 });
      assert.equal(f.consumed.size, 0);
    }
  }
});

test('signed nonce is one-use until exact9s expiry; normal and HEAD proofs do not resurrect after expiry', async () => {
  let now = 100000;
  const f = fixture({ clock: () => now });
  const ordinary = f.request(), ordinaryHeaders = Object.fromEntries(ordinary.headers), issued = f.signer.probeHeaders();
  now = 108999;
  await f.verifier.verify(ordinary);
  await f.verifier.verifyReady(probeRequest(f, issued));
  await assert.rejects(f.verifier.verify(new Request(ordinary.url, { headers: ordinaryHeaders })), { code: 'scoped_embed_proof_replayed' });
  await assert.rejects(f.verifier.verifyReady(probeRequest(f, issued)), { code: 'scoped_embed_proof_replayed' });
  now = 109000;
  await assert.rejects(f.verifier.verify(new Request(ordinary.url, { headers: ordinaryHeaders })), { code: 'scoped_embed_proof_expired', status: 401 });
  await assert.rejects(f.verifier.verifyReady(probeRequest(f, issued)), { code: 'scoped_embed_proof_expired', status: 401 });
  const unused = fixture({ clock: () => 100000, verifierClock: () => now });
  await assert.rejects(unused.verifier.verify(unused.request()), { code: 'scoped_embed_proof_expired', status: 401 });
  await assert.rejects(unused.verifier.verifyReady(probeRequest(unused, unused.signer.probeHeaders())), { code: 'scoped_embed_proof_expired', status: 401 });
  assert.equal(unused.consumed.size, 0);
});

test('authentic over10s envelopes still deny and a nonce COMMIT after issued expiry cannot return context or ready', async () => {
  let now = 100000, release;
  const f = fixture({ clock: () => now, consume: () => new Promise(done => { release = done; }) });
  const headers = f.signer.headers({ context: f.context, method: 'GET', path: '/api/embed/project' });
  const proof = { ...decode(headers), expiresAt: now + 10001 };
  const text = Buffer.from(JSON.stringify(proof)).toString('base64url');
  const signature = createHmac('sha256', f.key).update(text).digest('base64url');
  await assert.rejects(f.verifier.verify(new Request(f.raw.embedOrigin + '/api/embed/project', {
    headers: { 'x-soty-selected-proof': text, 'x-soty-selected-mac': signature },
  })), { code: 'scoped_embed_proof_expired', status: 401 });
  assert.equal(release, undefined);
  const request = f.request(), pending = f.verifier.verify(request);
  now = 109000; release(true);
  await assert.rejects(pending, { code: 'scoped_embed_proof_expired', status: 401 });
  assert.throws(() => f.verifier.context(request), { code: 'scoped_embed_transport_required' });
  now = 100000;
  const issued = f.signer.probeHeaders(), probing = f.verifier.verifyReady(probeRequest(f, issued));
  now = 109000; release(true);
  await assert.rejects(probing, { code: 'scoped_embed_proof_expired', status: 401 });
});
