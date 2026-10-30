import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, createPublicKey, generateKeyPairSync, verify } from 'node:crypto';
import { createTransitAccess } from '../transit-access.js';
import { signedFixture } from '../../modules/connect/test/support/direct-signed-fixture.mjs';

const keys = generateKeyPairSync('ed25519'), profile = { outbounds: [{ type: 'vless', tag: 'entry' }], route: { final: 'entry' } };
const configuration = { endpoint: 'http://fixture.invalid/v1/profile', issuer: 'https://soty.example', key: keys.privateKey, appId: 'app-' + 'a'.repeat(32) };
const sha256 = createHash('sha256').update(JSON.stringify(profile)).digest('hex');
function fixture(t, respond) {
  const actor = Object.freeze({ accountId: 'account-A', deviceId: 'device-A' }); let active = true, calls = 0;
  const service = createTransitAccess({ configuration, now: () => 100_000,
    getConnect: () => ({ withActorAuthorityFence(value, fn) { assert.equal(value, actor); if (!active) throw Error('revoked'); return fn(); } }),
    async fetchImpl(url, options) {
      calls++; assert.equal(url, configuration.endpoint); assert.equal(options.redirect, 'error');
      assert.ok(verify(null, Buffer.concat([Buffer.from('TRANSIT/SOTY-PROFILE/V1\0'), Buffer.from(options.body)]),
        createPublicKey(keys.privateKey), Buffer.from(options.headers['x-transit-signature'], 'base64')));
      const body = JSON.parse(options.body); assert.equal(body.accountId, actor.accountId); assert.equal(body.deviceId, actor.deviceId);
      return respond?.({ revoke() { active = false; }, body }) ?? Response.json({ schema: 'transit.personal-profile.v1', accountId: actor.accountId,
        coreVersion: '1.13.15', profile, sha256, mode: 'free-best-effort' });
    } });
  t.after(() => service.close());
  return { actor, args: { expectedAccountId: actor.accountId }, service, calls: () => calls };
}
test('bridge signs the authenticated actor and returns only their validated response', async t => {
  const f = fixture(t);
  assert.equal((await f.service.executeAsync({ op: 'transit.access.status', actor: f.actor, args: f.args })).enabled, true);
  const result = await f.service.executeAsync({ op: 'transit.access.profile', actor: f.actor, args: f.args });
  assert.deepEqual(result.profile, profile); assert.equal(f.calls(), 1);
});
test('caller cannot select another account or device', async t => {
  const f = fixture(t);
  for (const args of [{ expectedAccountId: 'account-B' }, { ...f.args, accountId: 'account-B' }, { ...f.args, deviceId: 'device-B' }]) {
    await assert.rejects(f.service.executeAsync({ op: 'transit.access.profile', actor: f.actor, args }), { code: 'transit_access_context_changed' });
  }
  assert.equal(f.calls(), 0);
});
test('revocation while the issuer replies suppresses the profile', async t => {
  const f = fixture(t, ({ revoke }) => { revoke(); });
  await assert.rejects(f.service.executeAsync({ op: 'transit.access.profile', actor: f.actor, args: f.args }), /revoked/);
});
test('wrong account, altered bytes, oversized body and issuer outage disclose no profile', async t => {
  for (const value of [{ accountId: 'account-B', sha256 }, { accountId: 'account-A', sha256: '0'.repeat(64) }, null]) {
    const f = fixture(t, () => value ? Response.json({ schema: 'transit.personal-profile.v1', coreVersion: '1.13.15', mode: 'free-best-effort', profile, ...value }) : new Response('x'.repeat(240_001)));
    await assert.rejects(f.service.executeAsync({ op: 'transit.access.profile', actor: f.actor, args: f.args }), { code: 'transit_access_unavailable' });
  }
});

test('real Connect signatures bind two people and reject a foreign account selector', async t => {
  let bridge;
  const connect = signedFixture(t, [{ operations: new Set(['transit.access.profile']), executeAsync: request => bridge.executeAsync(request) }]);
  bridge = createTransitAccess({ configuration, getConnect: () => connect.service,
    async fetchImpl(_url, options) {
      const actor = JSON.parse(options.body), personal = { ...profile, owner: actor.accountId };
      return Response.json({ schema: 'transit.personal-profile.v1', accountId: actor.accountId, coreVersion: '1.13.15', profile: personal,
        sha256: createHash('sha256').update(JSON.stringify(personal)).digest('hex'), mode: 'free-best-effort' });
    } });
  t.after(() => bridge.close());
  const a = await connect.person('A'), b = await connect.person('B');
  assert.equal((await connect.call(a, 'transit.access.profile', { expectedAccountId: a.accountId })).profile.owner, a.accountId);
  assert.equal((await connect.call(b, 'transit.access.profile', { expectedAccountId: b.accountId })).profile.owner, b.accountId);
  const denied = await connect.raw(b, 'transit.access.profile', { expectedAccountId: a.accountId });
  assert.equal(denied.ok, false); assert.equal(denied.error.code, 'transit_access_context_changed'); assert.equal(denied.profile, undefined);
});
