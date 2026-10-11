import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { environment } from './support/renewal-fixture.mjs';
import { HumanIdentityError } from '../profile.mjs';

const rows = (f, callback) => { const db = new DatabaseSync(f.identityPath, { readOnly: true }); try { return callback(db); } finally { db.close(); } };
function clock(t) {
  const original = Date.now, start = original(); let offset = 0;
  Date.now = () => start + offset * 1000; t.after(() => { Date.now = original; });
  return seconds => { offset += seconds; };
}
let serial = 0;
const attempt = (flow, actor, requestId = 'continuation-' + (++serial)) => ({
  expectedAccountId: actor.account.accountId, interactionId: flow.context.interactionId,
  browserNonce: flow.context.browserNonce, csrf: flow.context.csrf, requestId
});
const continuation = (flow, actor, args = attempt(flow, actor)) =>
  actor.client.extension('identity.human.continue', args, { expectedAccountId: actor.account.accountId });
async function remember(f, rp = f.rps[0]) {
  const flow = await f.begin(rp); await f.approve(flow, f.actor, { stayInAppSeconds: 86400, rememberForDevice: true });
  const finished = await f.complete(flow); assert.equal((await flow.session.request(finished.callback)).status, 200);
  return flow;
}

test('fresh signed continuation after absolute expiry issues a new finite OIDC family and keeps the RP account', { timeout: 20000 }, async t => {
  const advance = clock(t), f = await environment(t, { deviceConsent: true }); await remember(f);
  const before = await f.wire.request(f.rps[0].origin + '/me'); assert.equal(before.status, 200);
  const end = rows(f, db => db.prepare('SELECT session_expires_at AS value FROM human_identity_grant_bindings').get().value);
  advance(310); assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 200);
  assert.equal(rows(f, db => db.prepare('SELECT session_expires_at AS value FROM human_identity_grant_bindings').get().value), end);
  advance(86100); assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 401);
  const flow = await f.begin(); assert.equal(flow.context.deviceConsent.available, true);
  assert.equal((await continuation(flow, f.actor)).continued, true);
  const finished = await f.complete(flow); assert.equal((await f.wire.request(finished.callback)).status, 200);
  const after = await f.wire.request(f.rps[0].origin + '/me'); assert.equal(after.status, 200);
  assert.ok(after.body.localAccountId === before.body.localAccountId);
  assert.ok(after.body.identity.sub === before.body.identity.sub);
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_device_consents').get().n), 1);
  assert.equal(rows(f, db => db.prepare('SELECT max(session_expires_at-created_at) AS n FROM human_identity_grant_bindings').get().n), 86400);
  assert.equal(f.rps[0].existingLocalRow().balance, 17);
});

test('continuation requires a new signed proof and the exact account/device/client/scopes/profile generation', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }); await remember(f);
  for (const [rp, scope, signer] of [[f.rps[0], 'openid', f.actor], [f.rps[1], undefined, f.actor], [f.rps[0], undefined, f.outsider]]) {
    const flow = await f.begin(rp, f.wire, scope), result = await continuation(flow, signer);
    assert.equal(result.continued, false);
    const denied = await flow.session.request(flow.location.href + '/complete', { fields: { csrf: flow.context.csrf } });
    assert.equal(denied.status, 403);
  }
  const other = await f.newClient('Synthetic second device', false), pending = await other.client.startEnrollment('Synthetic second device');
  await f.actor.client.approveEnrollment(pending.requestId, f.actor.account.accountId);
  await other.client.previewEnrollment(pending.requestId); other.account = await other.client.finishEnrollment(pending.requestId, f.actor.account.accountId);
  assert.equal((await continuation(await f.begin(), other)).continued, false);
  await f.clientVersion(0, 3);
  assert.equal((await continuation(await f.begin(f.rps[0], f.freshWire()), f.actor)).continued, false);
});

test('logout revocation cancels this RP preference and fences staged approval, while another RP continues', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }); await remember(f); await remember(f, f.rps[1]);
  const token = f.rps[0].verificationFixture().refreshToken, flow = await f.begin();
  assert.equal((await continuation(flow, f.actor)).continued, true);
  assert.equal((await f.revoke(f.rps[0], token)).status, 200);
  const denied = await flow.session.request(flow.location.href + '/complete', { fields: { csrf: flow.context.csrf } });
  assert.ok([400, 403].includes(denied.status)); // The maintained engine may reject its revoked OP session first.
  assert.throws(() => f.identity.readApprovedInteraction({ interactionId: flow.context.interactionId,
    browserNonce: flow.context.browserNonce, csrf: flow.context.csrf }), error => error.code === 'human_identity_device_consent_revoked');
  assert.equal((await continuation(await f.begin(f.rps[0], f.freshWire()), f.actor)).continued, false);
  assert.equal((await continuation(await f.begin(f.rps[1], f.freshWire()), f.actor)).continued, true);
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_device_consents WHERE revoked_at IS NOT NULL').get().n), 1);
});

test('a lost signed decision ACK replays one pinned decision; logout cannot reactivate it', { timeout: 20000 }, async t => {
  let lost = false;
  const f = await environment(t, { deviceConsent: true, serviceDecorator: service => ({ ...service, execute(input) {
    const result = service.execute(input);
    if (input.op === 'identity.human.continue' && result.continued && !lost) { lost = true; throw new HumanIdentityError('synthetic_ack_lost', 503); }
    return result;
  } }) }); await remember(f);
  const token = f.rps[0].verificationFixture().refreshToken, flow = await f.begin(), args = attempt(flow, f.actor);
  await assert.rejects(continuation(flow, f.actor, args));
  assert.equal((await continuation(flow, f.actor, args)).continued, true);
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_decisions WHERE account_id=? AND request_id=?').get(args.expectedAccountId, args.requestId).n), 1);
  await assert.rejects(continuation(flow, f.actor, { ...args, csrf: 'a'.repeat(43) }));
  await f.revoke(f.rps[0], token);
  await assert.rejects(continuation(flow, f.actor, args), error => error.code === 'human_identity_device_consent_revoked');
});

test('consent admission off reads Human3, preserves normal24h, and makes continuation unavailable', { timeout: 20000 }, async t => {
  const advance = clock(t), f = await environment(t, { deviceConsent: true }); await remember(f);
  const before = rows(f, db => db.prepare('SELECT id_hash,payload_cipher FROM human_identity_artifacts ORDER BY model,id_hash').all());
  await f.consentAdmission(false); assert.equal(f.identity.schemaVersion, 3);
  const after = rows(f, db => db.prepare('SELECT id_hash,payload_cipher FROM human_identity_artifacts ORDER BY model,id_hash').all());
  assert.ok(before.length === after.length && before.every((row, index) => row.id_hash === after[index].id_hash && Buffer.from(row.payload_cipher).equals(after[index].payload_cipher)));
  advance(310); assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 200);
  assert.equal((await continuation(await f.begin(), f.actor)).continued, false);
  const flow = await f.begin(f.rps[1]); await f.approve(flow, f.actor, { stayInAppSeconds: 86400 });
  assert.equal((await f.wire.request((await f.complete(flow)).callback)).status, 200);
});

test('host family limits admit nine independently registered RPs without changing finite TTLs or grants', { timeout: 30000 }, async t => {
  clock(t);
  let denial;
  const f = await environment(t, { rpsCount: 9, serviceDecorator: service => ({ ...service, sdk: { ...service.sdk, upsert(input) {
    try { return service.sdk.upsert(input); } catch (error) { if (input.model === 'Grant') denial = error.code; throw error; }
  } } }) });
  for (const rp of f.rps.slice(0, 8)) await f.login(rp, f.actor, f.wire, undefined, 86400);
  const flow = await f.begin(f.rps[8]); await f.approve(flow, f.actor, { stayInAppSeconds: 86400 });
  assert.ok([400, 429].includes((await flow.session.request(flow.location.href + '/complete', { fields: { csrf: flow.context.csrf } })).status));
  assert.equal(denial, 'human_identity_capacity');
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings').get().n), 8);
  await f.setRenewalLimits({ activeFamilies: 24, familiesPerAccount: 12 });
  const fresh = await f.begin(f.rps[8], f.freshWire()); await f.approve(fresh, f.actor, { stayInAppSeconds: 86400 });
  assert.equal((await fresh.session.request((await f.complete(fresh)).callback)).status, 200);
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings').get().n), 9);
  assert.equal(rows(f, db => db.prepare('SELECT min(session_expires_at-created_at) AS min,max(session_expires_at-created_at) AS max FROM human_identity_grant_bindings').get().min), 86400);
  await assert.rejects(f.approve(await f.begin(), f.actor, { renewalLimits: { activeFamilies: 99, familiesPerAccount: 99 } }));
});

test('current Root device revocation fences approved continuation and cannot be inherited by a backup device', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }); await remember(f);
  const accessToken = f.rps[0].verificationFixture().accessToken, backup = await f.backupDevice(), flow = await f.begin();
  assert.equal((await continuation(flow, f.actor)).continued, true);
  await backup.client.revokeDevice(f.actor.account.deviceId);
  assert.throws(() => f.identity.readApprovedInteraction({ interactionId: flow.context.interactionId,
    browserNonce: flow.context.browserNonce, csrf: flow.context.csrf }), error => error.code === 'human_identity_device_consent_revoked');
  const denied = await flow.session.request(flow.location.href + '/complete', { fields: { csrf: flow.context.csrf } });
  assert.ok([400, 403].includes(denied.status)); // The maintained engine can reject its revoked OP session before the service.
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings').get().n), 1);
  await assert.rejects(continuation(flow, f.actor));
  const userinfo = await f.wire.request(f.issuer + '/userinfo', { cookies: false, originHeader: null, headers: { authorization: 'Bearer ' + accessToken } });
  assert.equal(userinfo.status, 401);
  assert.equal((await continuation(await f.begin(f.rps[0], f.freshWire()), backup)).continued, false);
});

test('signed remembering is explicit and malformed, foreign callback or injected authority cannot create a consent', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }), flow = await f.begin();
  for (const changes of [{ rememberForDevice: 'true', stayInAppSeconds: 86400 }, { rememberForDevice: true },
    { rememberForDevice: true, stayInAppSeconds: 86400, clientId: f.rps[0].clientId },
    { rememberForDevice: true, stayInAppSeconds: 86400, consentRevision: 1 },
    { rememberForDevice: true, stayInAppSeconds: 86400, decision: 'deny' }, { forgetForDevice: 'true', decision: 'deny' },
    { forgetForDevice: true }, { forgetForDevice: true, decision: 'deny', stayInAppSeconds: 86400 }]) await assert.rejects(f.approve(flow, f.actor, changes));
  const args = attempt(flow, f.actor);
  for (const extra of [{ decision: 'approve' }, { rememberForDevice: true }, { stayInAppSeconds: 86400 }, { clientId: f.rps[0].clientId }]) {
    await assert.rejects(continuation(flow, f.actor, { ...args, ...extra }));
  }
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_device_consents').get().n), 0);
  const changed = new URL(flow.authorization); changed.searchParams.set('redirect_uri', f.rps[1].redirectUri);
  assert.equal((await f.freshWire().request(changed)).status, 400);
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_device_consents').get().n), 0);
});

test('fresh signed forget after 48h cancels the preference without an expired bearer; another RP remains remembered', { timeout: 20000 }, async t => {
  const advance = clock(t), f = await environment(t, { deviceConsent: true }); await remember(f); await remember(f, f.rps[1]);
  const oldToken = f.rps[0].verificationFixture().refreshToken; advance(172801);
  assert.equal((await f.refresh(f.rps[0], oldToken)).status, 400);
  await f.consentAdmission(false);
  const flow = await f.begin(f.rps[0], f.freshWire());
  assert.equal(flow.context.deviceConsent.available, false);
  const result = await f.approve(flow, f.actor, { decision: 'deny', forgetForDevice: true }); assert.equal(result.decision, 'denied');
  await f.consentAdmission(true);
  assert.equal((await continuation(await f.begin(f.rps[0], f.freshWire()), f.actor)).continued, false);
  assert.equal((await continuation(await f.begin(f.rps[1], f.freshWire()), f.actor)).continued, true);
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_device_consents WHERE revoked_at IS NOT NULL').get().n), 1);
});

test('forget closes old pending remember and approved callback in the same browser, with exact replay and no late Grant', { timeout: 20000 }, async t => {
  let lost = false;
  const f = await environment(t, { deviceConsent: true, serviceDecorator: service => ({ ...service, execute(input) {
    const result = service.execute(input);
    if (input.args.forgetForDevice === true && !lost) { lost = true; throw new HumanIdentityError('synthetic_forget_ack_lost', 503); }
    return result;
  } }) }); await remember(f); await remember(f, f.rps[1]);
  const stalePending = await f.begin(), staleApproved = await f.begin();
  await f.approve(staleApproved, f.actor, { stayInAppSeconds: 86400 });
  const captured = f.identity.readApprovedInteraction(staleApproved.context), otherBrowser = await f.begin(f.rps[0], f.freshWire());
  const flow = await f.begin(), args = { ...attempt(flow, f.actor, 'forget-lost-ack'), decision: 'deny', forgetForDevice: true };
  const send = value => f.actor.client.extension('identity.human.approve', value, { expectedAccountId: f.actor.account.accountId });
  await assert.rejects(send(args)); assert.equal((await send(args)).decision, 'denied');
  const revision = rows(f, db => db.prepare('SELECT revision FROM human_identity_device_consents WHERE revoked_at IS NOT NULL').get().revision);
  assert.equal((await send({ ...args, requestId: 'forget-new-id-same-intent' })).decision, 'denied');
  assert.equal(rows(f, db => db.prepare('SELECT revision FROM human_identity_device_consents WHERE revoked_at IS NOT NULL').get().revision), revision);
  await assert.rejects(f.approve(stalePending, f.actor, { stayInAppSeconds: 86400, rememberForDevice: true }), error => error.code === 'human_identity_decision_conflict');
  assert.throws(() => f.identity.readApprovedInteraction(staleApproved.context), error => error.code === 'human_identity_approval_required');
  assert.throws(() => f.identity.sdk.upsert({ model: 'Grant', id: 'stale-captured-grant-' + 'x'.repeat(16),
    payload: { jti: 'stale-captured-grant-' + 'x'.repeat(16), kind: 'Grant', accountId: captured.accountId, clientId: captured.clientId,
      exp: captured.sessionExpiresAt }, expiresIn: 86400, approvedBinding: captured }), error => error.code === 'human_identity_approval_required');
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings').get().n), 2);
  assert.ok([400, 403].includes((await staleApproved.session.request(staleApproved.location.href + '/complete', { fields: { csrf: staleApproved.context.csrf } })).status));
  assert.equal((await continuation(await f.begin(f.rps[1], f.freshWire()), f.actor)).continued, true);
  // A different browser nonce retains its pending request and still needs its
  // own explicit new consent, never authority from the fragment.
  assert.equal((await otherBrowser.session.request(otherBrowser.location.href + '/context')).body.decision, 'pending');
});
