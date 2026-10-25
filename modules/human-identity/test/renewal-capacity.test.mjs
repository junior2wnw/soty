import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { environment } from './support/renewal-fixture.mjs';

const count = f => {
  const db = new DatabaseSync(f.identityPath, { readOnly: true });
  try { return db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings WHERE revoked_at IS NULL').get().n; }
  finally { db.close(); }
};

test('global16 boundary refuses a seventeenth family without evicting people; a reviewed24 profile admits it after restart', { timeout: 60000 }, async t => {
  let denial;
  const f = await environment(t, { rpsCount: 9, serviceDecorator: service => ({ ...service, sdk: { ...service.sdk,
    upsert(input) {
      try { return service.sdk.upsert(input); }
      catch (error) { if (input.model === 'Grant') denial = error.code; throw error; }
    },
  } }) });
  const proofs = [];
  for (const signer of [f.actor, f.outsider]) {
    const browser = f.freshWire();
    for (const rp of f.rps.slice(0, 8)) {
      await f.login(rp, signer, browser, undefined, 86400);
      proofs.push({ token: rp.verificationFixture().accessToken, subject: signer.account.accountId });
    }
  }
  assert.equal(count(f), 16);
  const third = await f.newClient('Independent third profile');
  const flow = await f.begin(f.rps[8], f.freshWire());
  await f.approve(flow, third, { stayInAppSeconds: 86400 });
  const denied = await flow.session.request(flow.location.href + '/complete', { fields: { csrf: flow.context.csrf } });
  assert.ok([400, 429].includes(denied.status)); assert.equal(denial, 'human_identity_capacity');
  assert.equal(count(f), 16);
  const checkProof = async proof => {
    const response = await f.wire.request(f.issuer + '/userinfo', { cookies: false, originHeader: null,
      headers: { authorization: 'Bearer ' + proof.token } });
    assert.equal(response.status, 200); assert.equal(response.body.sub, proof.subject);
  };
  await checkProof(proofs[0]); await checkProof(proofs.at(-1));
  await f.setRenewalLimits({ activeFamilies: 24, familiesPerAccount: 12 });
  await f.restart();
  await f.login(f.rps[8], third, f.freshWire(), undefined, 86400);
  assert.equal(count(f), 17);
  await checkProof(proofs[0]); await checkProof(proofs.at(-1));
});
