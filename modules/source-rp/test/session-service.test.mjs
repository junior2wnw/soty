import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { randomBytes } from 'node:crypto';
import { createSourceRpSessionService, SourceRpError, sourceRpCipherBinding } from '../server/index.mjs';
import { sqliteSource, digest } from './support/sqlite-source.mjs';
const random = () => randomBytes(32).toString('base64url');
async function fixture(t, change = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'source-rp-contract-')); let time = Date.now();
  const clock = () => time, native = sqliteSource(join(directory, 'source.sqlite'), { clock });
  const marker = { sessionIdHash: digest(random()), profileDigest: digest('approved-profile'), bindingDigest: digest('one-selected-resource'),
    issuer: 'https://root.fixture/human-identity', subject: 'immutable-subject', createdAt: time, sessionExpiresAt: time + 86400000 };
  let refreshCalls = 0, userinfoCalls = 0;
  const protocol = { async ready() {}, async renew(input) { refreshCalls++; return { accessToken: random(), refreshToken: random(), nonce: input.nonce, expiresAt: time + 300000 }; },
    async currentSubject(_token, subject) { userinfoCalls++; return subject; }, ...change };
  const options = { storagePort: native.storagePort, protocol, profileDigest: marker.profileDigest, keyId: native.keyId,
    encrypt: native.encrypt, decrypt: native.decrypt, clock };
  await native.seed(marker, { accessToken: random(), refreshToken: random(), nonce: random() }, time + 300000);
  t.after(() => { native.close(); assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^source-rp-contract-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  return { native, marker, options, service: createSourceRpSessionService(options), protocol,
    advance(ms) { time += ms; }, counts: () => ({ refreshCalls, userinfoCalls }) };
}
test('opaque proof exposes no tokens and userinfo is fresh per use/assertion', async t => {
  const f = await fixture(t), a = await f.service.currentProof(f.marker), b = await f.service.currentProof(f.marker);
  assert.deepEqual(Object.keys(a).sort(), ['assertCurrent', 'expiresAt', 'issuer', 'sessionExpiresAt', 'sessionGeneration', 'sub']);
  assert.equal(a.sessionGeneration, b.sessionGeneration); await a.assertCurrent(); assert.equal(f.counts().userinfoCalls, 3);
  assert.equal(f.counts().refreshCalls, 0); assert.equal(JSON.stringify(a).includes('Token'), false);
});
test('independent services CAS one refresh and absolute deadline survives restart', async t => {
  const f = await fixture(t); f.advance(280000); const second = createSourceRpSessionService(f.options);
  const proofs = await Promise.all([f.service.currentProof(f.marker), second.currentProof(f.marker)]);
  assert.equal(f.counts().refreshCalls, 1); assert.equal(proofs.every(proof => proof.sessionGeneration === 1), true);
  assert.equal(f.native.head(f.marker.sessionIdHash).sessionExpiresAt, f.marker.sessionExpiresAt);
  const restarted = createSourceRpSessionService(f.options); await restarted.currentProof(f.marker);
  assert.equal(f.counts().refreshCalls, 1);
});
test('exact local COMMIT ACK recovers same rotation without a second RT send', async t => {
  const f = await fixture(t); f.advance(280000); f.native.loseCommitAck();
  const proof = await f.service.currentProof(f.marker); assert.equal(proof.sessionGeneration, 1);
  assert.equal(f.counts().refreshCalls, 1); assert.equal(f.native.head(f.marker.sessionIdHash).state, 'idle');
});
test('remote unknown consumes claim; restart never retries old RT, old AT expires', async t => {
  let sends = 0; const f = await fixture(t, { async renew() { sends++; throw new SourceRpError('source_rp_refresh_unknown', 503); } });
  f.advance(280000); await assert.rejects(f.service.currentProof(f.marker), error => error.code === 'source_rp_refresh_unknown');
  assert.equal(f.native.head(f.marker.sessionIdHash).state, 'unknown');
  const restarted = createSourceRpSessionService(f.options); await restarted.currentProof(f.marker); assert.equal(sends, 1);
  f.advance(21000); await assert.rejects(restarted.currentProof(f.marker), error => error.status === 401); assert.equal(sends, 1);
});
test('stale claim becomes unknown without takeover or RT request', async t => {
  const f = await fixture(t); const original = f.native.head(f.marker.sessionIdHash);
  f.native.overwrite({ ...original, state: 'refreshing', claimId: digest('other-process'), claimedAt: original.createdAt });
  f.advance(20001); await f.service.currentProof(f.marker);
  assert.equal(f.native.head(f.marker.sessionIdHash).state, 'unknown'); assert.equal(f.counts().refreshCalls, 0);
});
test('provider discovery outage precedes durable claim and can retry discovery', async t => {
  const f = await fixture(t, { async ready() { throw new SourceRpError('source_rp_provider_unavailable', 503); } }); f.advance(280000);
  await assert.rejects(f.service.currentProof(f.marker), error => error.status === 503);
  assert.equal(f.native.head(f.marker.sessionIdHash).state, 'idle'); assert.equal(f.counts().refreshCalls, 0);
});
test('native authority revoked after awaited claim prevents RT send', async t => {
  const f = await fixture(t); const claim = f.native.storagePort.claim;
  f.native.storagePort.claim = async input => { const result = await claim(input); f.native.revoke(f.marker.sessionIdHash); return result; };
  f.advance(280000); await assert.rejects(f.service.currentProof(f.marker), error => error.status === 401);
  assert.equal(f.counts().refreshCalls, 0); assert.equal(f.native.head(f.marker.sessionIdHash).state, 'revoked');
});
test('native revoke during fresh userinfo refuses proof and already captured assertion', async t => {
  const f = await fixture(t), proof = await f.service.currentProof(f.marker);
  f.protocol.currentSubject = async (_token, subject) => { f.native.revoke(f.marker.sessionIdHash); return subject; };
  await assert.rejects(f.service.currentProof(f.marker), error => error.status === 401);
  await assert.rejects(proof.assertCurrent(), error => error.status === 401);
});
test('semantic resource/profile/key change and extra marker fields cannot reuse encrypted state', async t => {
  const f = await fixture(t);
  await assert.rejects(f.service.currentProof({ ...f.marker, arbitraryGrant: true }), error => error.status === 401);
  await assert.rejects(f.service.currentProof({ ...f.marker, bindingDigest: digest('wider-scope') }), error => error.status === 401);
  f.native.overwrite({ ...f.native.head(f.marker.sessionIdHash), keyId: 'changed-key' });
  await assert.rejects(f.service.currentProof(f.marker), error => error.status === 503);
  assert.notEqual(sourceRpCipherBinding(f.marker, 0), sourceRpCipherBinding({ ...f.marker, bindingDigest: digest('other') }, 0));
});
test('absolute 24h cannot slide on refresh and fails when source marker exceeds it', async t => {
  const f = await fixture(t); await assert.rejects(f.service.currentProof({ ...f.marker, sessionExpiresAt: f.marker.createdAt + 86400001 }), error => error.status === 401);
  f.advance(86400000); await assert.rejects(f.service.currentProof(f.marker), error => error.status === 401); assert.equal(f.counts().refreshCalls, 0);
});
