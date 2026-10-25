import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { createBridgeSigner, createBridgeVerifier } from '../../packages/soty-bridge/index.mjs';

const keys = generateKeyPairSync('ed25519');
const issuer = 'https://root.soty.invalid'; const audience = 'urn:test:gateway';
const body = '{"model":"test/model","messages":[{"role":"user","content":"hello"}]}';
const input = { method: 'POST', path: '/internal/soty/v1/chat/completions?exact=1', operation: 'chat.complete', accountId: 'account-a', deviceId: 'device-a', requestId: 'logical-request', authorizationId: 'captured-authority', body };

test('bridge binds bytes, route, audience and current time, then atomically claims nonce', async () => {
  let now = 1800000000000; let claims = 0; const nonces = new Set();
  const signer = createBridgeSigner({ issuer, audience, kid: 'first', privateKey: keys.privateKey, clock: () => now });
  const verifier = createBridgeVerifier({ audience, issuers: [{ issuer, kid: 'first', publicKey: keys.publicKey }], clock: () => now, claimNonce: async proof => { claims++; if (nonces.has(proof.jti)) return false; nonces.add(proof.jti); return true; } });
  const headers = signer.headers(input);
  const proof = await verifier.verify({ ...input, headers });
  assert.equal(proof.accountId, 'account-a'); assert.equal(Object.isFrozen(proof), true); assert.equal(claims, 1);
  await assert.rejects(verifier.verify({ ...input, headers }), { code: 'bridge_replay' });
  for (const alteration of [{ body: `${body} ` }, { method: 'GET' }, { path: '/internal/soty/v1/chat/completions?exact=2' }]) {
    const count = claims;
    await assert.rejects(verifier.verify({ ...input, ...alteration, headers: signer.headers(input) }), { code: 'bridge_invalid' });
    assert.equal(claims, count);
  }
  const freshNonce = signer.headers(input); assert.notEqual(freshNonce['x-soty-bridge'], headers['x-soty-bridge']);
  assert.equal((await verifier.verify({ ...input, headers: freshNonce })).requestId, proof.requestId);
  const expiring = signer.headers(input); now += 30000;
  await assert.rejects(verifier.verify({ ...input, headers: expiring }), { code: 'bridge_invalid' });
  assert.equal(JSON.stringify(proof).includes(body), false);
});

test('bridge rejects unknown keys, wrong audience, malformed/ambiguous proof and missing inference authorization', async () => {
  const signer = createBridgeSigner({ issuer, audience, kid: 'first', privateKey: keys.privateKey });
  const verifier = createBridgeVerifier({ audience, issuers: [{ issuer, kid: 'first', publicKey: keys.publicKey }], claimNonce: async () => true });
  const canonical = proof => `{${Object.keys(proof).sort().map(key => `${JSON.stringify(key)}:${JSON.stringify(proof[key])}`).join(',')}}`;
  for (const headers of [
    { 'x-soty-bridge': 'not-a-proof' },
    createBridgeSigner({ issuer, audience: 'urn:other', kid: 'first', privateKey: keys.privateKey }).headers(input),
    createBridgeSigner({ issuer, audience, kid: 'unknown', privateKey: keys.privateKey }).headers(input),
    { 'x-soty-bridge': [signer.headers(input)['x-soty-bridge']] },
  ]) await assert.rejects(verifier.verify({ ...input, headers }), { code: 'bridge_invalid' });
  const original = signer.headers(input)['x-soty-bridge'];
  const parsed = JSON.parse(Buffer.from(original.split('.')[0], 'base64url').toString('utf8'));
  for (const proof of [{ ...parsed, extra: 'forbidden' }, { ...parsed, exp: parsed.iat + 31 }, { ...parsed, iat: parsed.iat + 10, exp: parsed.iat + 30 }]) {
    const payload = Buffer.from(canonical(proof));
    const headers = { 'x-soty-bridge': `${payload.toString('base64url')}.${sign(null, payload, keys.privateKey).toString('base64url')}` };
    await assert.rejects(verifier.verify({ ...input, headers }), { code: 'bridge_invalid' });
  }
  assert.throws(() => signer.headers({ ...input, authorizationId: undefined }), { code: 'bridge_invalid' });
  assert.throws(() => signer.headers({ ...input, body: { not: 'exact bytes' } }), { code: 'bridge_invalid' });
  assert.throws(() => createBridgeVerifier({ audience, issuers: [{ issuer, kid: 'first', publicKey: keys.privateKey }], claimNonce: async () => true }));
});
