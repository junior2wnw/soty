import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { bearerFixture, INPUT, ORIGIN, code, good } from './support/oauth-bearer.mjs';

const count = (f, store, table) => f.sql(store, db => db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n);
const summary = f => f.sql('caps', db => ({ invocations: db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n,
  reserved: db.prepare('SELECT COALESCE(sum(reserved_amount),0) AS n FROM cap_budgets').get().n,
  spent: db.prepare('SELECT COALESCE(sum(spent_amount),0) AS n FROM cap_budgets').get().n }));

test('opaque bearer is privately branded and rechecks its exact link, instance and audience without the AS key', async t => {
  const f = await bearerFixture(t), family = await f.family(), actor = f.actor(family);
  assert.deepEqual(f.oauth.readiness(), { schemaVersion: 3, available: true });
  assert.equal(f.caps.authorize({ actor, action: 'history' }).accountId, f.owner.accountId);
  assert.throws(() => f.caps.authorize({ actor: { ...actor }, action: 'history' }), code('authorization_required'));
  assert.throws(() => f.caps.authenticateCredential({ token: family.at.jti, audience: family.resource }), code('authorization_required'));
  for (const token of ['soty_cap_' + family.at.jti, family.rt.jti, 'a'.repeat(42)]) {
    assert.throws(() => f.oauth.authenticateBearer({ token, audience: family.resource }), code('authorization_required'));
  }
  assert.throws(() => f.oauth.authenticateBearer({ token: family.at.jti, audience: ORIGIN }), code('authorization_required'));
  assert.throws(() => f.oauth.authenticateBearer({ token: family.at.jti, audience: 'https://foreign.test' }), code('authorization_required'));
  const link = f.credential(family);
  f.sql('caps', db => db.prepare('DELETE FROM cap_oauth_credentials WHERE credential_id=?').run(link.credential_id));
  assert.throws(() => f.caps.authorize({ actor, action: 'history' }), code('authorization_required'));
  f.sql('caps', db => db.prepare('INSERT INTO cap_oauth_credentials(credential_id,connection_id,token_digest,created_at,expires_at) VALUES(?,?,?,?,?)')
    .run(link.credential_id, link.connection_id, link.token_digest, link.created_at, link.expires_at));
  f.reopen({ keyless: true, enabled: false });
  assert.deepEqual(f.oauth.readiness(), { schemaVersion: 3, available: false });
  assert.throws(() => f.caps.authorize({ actor, action: 'history' }), code('authorization_required'));
  assert.equal(f.caps.authorize({ actor: f.actor(family), action: 'history' }).credentialId, link.credential_id);
  assert.throws(() => f.caps.execute({ op: 'access.principals.create', actor: f.actor(family),
    args: { expectedAccountId: family.accountId, label: 'not an owner' } }), code('authorization_required'));
});

test('refresh grants current read/replay but cannot extend the original effect deadline; keyless disabled replay precedes readiness', async t => {
  const f = await bearerFixture(t), first = await f.family(), originalActor = f.actor(first), originalLink = f.credential(first);
  const key = 'oauth_expired_original', admitted = f.native.admit({ actor: originalActor, idempotencyKey: key, input: INPUT });
  const invocationId = admitted.invocation.invocationId;
  f.advance(300001);
  assert.throws(() => f.native.get({ actor: originalActor, invocationId }), code('authorization_required'));
  const refreshed = f.refresh(first), currentActor = f.actor(refreshed);
  assert.notEqual(f.credential(refreshed).credential_id, originalLink.credential_id);
  assert.equal(f.native.get({ actor: currentActor, invocationId }).invocation.invocationId, invocationId);
  assert.equal(f.native.admit({ actor: currentActor, idempotencyKey: key, input: INPUT }).reused, true);
  assert.throws(() => f.native.admit({ actor: currentActor, idempotencyKey: key, input: { ...INPUT, body: 'changed' } }), code('invocation_request_conflict'));
  f.reopen({ keyless: true, enabled: false });
  assert.equal(f.native.readiness().ready, false); assert.equal(f.oauth.readiness().available, false);
  const renewed = f.actor(refreshed);
  assert.equal(f.native.admit({ actor: renewed, idempotencyKey: key, input: INPUT }).reused, true);
  assert.throws(() => f.native.beginAttempt({ invocationId }), code('access_denied'));
  assert.equal(f.native.reconcile({ invocationId }).outcome, 'not_applied');
  assert.equal(count(f, 'notes', 'notes'), 0);
  const original = f.sql('caps', db => JSON.parse(db.prepare('SELECT authorization_json FROM cap_invocations WHERE id=?').get(invocationId).authorization_json));
  assert.equal(original.credentialId, originalLink.credential_id); assert.equal(original.expiresAt, originalLink.expires_at);
  assert.ok(f.sql('caps', db => db.prepare('SELECT 1 FROM cap_oauth_credentials WHERE credential_id=?').get(originalLink.credential_id)));
  assert.equal(f.native.get({ actor: renewed, invocationId }).invocation.status, 'failed');
  assert.deepEqual(summary(f), { invocations: 1, reserved: 0, spent: 0 });
});

test('fresh actor reads only the historical create receipt after 34 human edits, purge and original credential cleanup', async t => {
  const f = await bearerFixture(t), first = await f.family(), actor = f.actor(first), original = f.credential(first);
  const done = f.create(actor, 'oauth_history_original'), receipt = done.invocation.receipt, artifact = receipt.artifacts[0];
  for (let i = 0; i < 34; i++) good(await f.ownerCall('notes.put', { noteId: artifact.id, mutationId: 'oauth_human_edit_' + i,
    expectedRevision: i + 1, title: 'Changed by owner', body: 'private revision ' + i, items: [], pinned: false,
    color: 'plain', state: i === 33 ? 'trashed' : 'active' }));
  good(await f.ownerCall('notes.purge', { noteId: artifact.id, mutationId: 'oauth_human_purge', expectedRevision: 35 }));
  const transient = await f.family(), transientId = f.credential(transient).credential_id;
  f.advance(300001); const refreshed = f.refresh(first);
  for (let i = 0; i < 4; i++) f.oauth.cleanup({ limit: 64 });
  assert.ok(f.sql('caps', db => db.prepare('SELECT 1 FROM cap_credentials WHERE id=?').get(original.credential_id)));
  assert.equal(f.sql('caps', db => db.prepare('SELECT 1 FROM cap_credentials WHERE id=?').get(transientId)), undefined);
  f.reopen({ keyless: true, enabled: false });
  const reply = f.native.get({ actor: f.actor(refreshed), invocationId: done.invocation.invocationId });
  assert.deepEqual(reply.invocation.receipt, receipt); assert.equal(reply.invocation.status, 'succeeded');
  const replay = f.native.admit({ actor: f.actor(refreshed), idempotencyKey: 'oauth_history_original', input: INPUT });
  assert.equal(replay.reused, true); assert.deepEqual(replay.invocation.receipt, receipt);
  assert.ok(!JSON.stringify(reply).includes('private revision'));
  assert.equal(f.sql('notes', db => db.prepare('SELECT state FROM notes WHERE id=?').get(artifact.id).state), 'deleted');
  assert.equal(count(f, 'notes', 'note_native_creates'), 1);
});

test('proof-positive completion survives revoke and AS removal; new consent cannot retry the old occupied key', async t => {
  const lost = new Error('synthetic post-Notes-COMMIT return loss');
  const f = await bearerFixture(t, { port(native) { return { ...native,
    createDraftForInvocation(args) { native.createDraftForInvocation(args); throw lost; } }; } });
  const old = await f.family(), actor = f.actor(old), key = 'oauth_lost_reply_key';
  const invocationId = f.native.admit({ actor, idempotencyKey: key, input: INPUT }).invocation.invocationId;
  f.native.beginAttempt({ invocationId }); assert.throws(() => f.native.execute({ invocationId }), error => error === lost);
  assert.equal(count(f, 'notes', 'note_native_creates'), 1); assert.equal(count(f, 'caps', 'cap_receipts'), 0);
  f.caps.invocations.requestCancel({ actor, invocationId });
  good(await f.ownerCall('oauth.connections.revoke', { connectionId: old.id }));
  assert.throws(() => f.native.get({ actor, invocationId }), code('authorization_required'));
  f.reopen({ noOAuth: true, enabled: false, port: native => native });
  assert.equal(f.oauth, undefined);
  const settled = f.native.reconcile({ invocationId }); assert.equal(settled.outcome, 'committed');
  assert.equal(settled.invocation.status, 'succeeded');
  assert.equal(good(await f.ownerCall('access.invocations.list')).invocations[0].invocationId, invocationId);
  f.reopen({ noOAuth: false }); const newConnection = await f.family(), freshActor = f.actor(newConnection);
  assert.throws(() => f.native.get({ actor: freshActor, invocationId }), code('invocation_not_found'));
  const before = summary(f);
  const captured = [], prepare = DatabaseSync.prototype.prepare;
  DatabaseSync.prototype.prepare = function(sql) {
    const statement = prepare.call(this, sql);
    if (!sql.includes('INDEXED BY cap_invocations_oauth_request')) return statement;
    return { get(...args) { captured.push({ sql, args }); return statement.get(...args); } };
  };
  try {
    for (const input of [INPUT, { title: 'different', body: 'different private request' }]) {
      assert.throws(() => f.native.admit({ actor: freshActor, idempotencyKey: key, input }), code('invocation_request_conflict'));
    }
  } finally { DatabaseSync.prototype.prepare = prepare; }
  assert.equal(captured.length, 2);
  const plan = f.sql('caps', db => db.prepare('EXPLAIN QUERY PLAN ' + captured[0].sql).all(...captured[0].args).map(row => row.detail).join(' '));
  assert.match(plan, /SEARCH i USING COVERING INDEX cap_invocations_oauth_request \(account_id=\? AND request_key=\?\)/u);
  assert.doesNotMatch(plan, /SCAN i|SCAN c|TEMP B-TREE/u);
  t.diagnostic('Captured occupied-key SQL plan: ' + plan);
  assert.deepEqual(summary(f), before); assert.equal(count(f, 'notes', 'note_native_creates'), 1);
  // Expired/revoked historical connection pins continue to occupy the key.
  f.advance(86400001); const later = await f.family();
  assert.throws(() => f.native.admit({ actor: f.actor(later), idempotencyKey: key, input: INPUT }), code('invocation_request_conflict'));
  assert.deepEqual(summary(f), before);
});

test('OAuth namespace separation preserves foreign account/profile/resource and ordinary service behavior', async t => {
  const f = await bearerFixture(t), key = 'oauth_independent_scopes', first = await f.family();
  const original = f.native.admit({ actor: f.actor(first), idempotencyKey: key, input: INPUT }).invocation.invocationId;
  const otherProfile = await f.family({ clientProfile: 'soty-opencode-cli' });
  const otherResource = await f.family({ resource: ORIGIN });
  const foreign = await f.account(), otherAccount = await f.family({ actor: foreign });
  for (const own of [otherProfile, otherResource, otherAccount]) {
    assert.equal(f.native.admit({ actor: f.actor(own), idempotencyKey: key, input: INPUT }).reused, false);
    assert.throws(() => f.native.get({ actor: f.actor(own), invocationId: original }), code('invocation_not_found'));
  }
  const principal = good(await f.ownerCall('access.principals.create', { label: 'Legacy independent client' })).principal;
  const grant = good(await f.ownerCall('access.grants.issue', { principalId: principal.id, capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
    resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'], expiresAt: f.now() + 3600000,
    budget: { unit: 'invocations', limit: 20 } })).grant;
  const credential = good(await f.ownerCall('access.credentials.issue', { grantId: grant.id, audience: ORIGIN }));
  const legacy = f.caps.authenticateCredential({ token: credential.token, audience: ORIGIN });
  assert.equal(f.native.admit({ actor: legacy, idempotencyKey: key, input: INPUT }).reused, false);
  assert.throws(() => f.oauth.authenticateBearer({ token: credential.token, audience: ORIGIN }), code('authorization_required'));
  const refreshed = f.refresh(first);
  assert.equal(f.native.admit({ actor: f.actor(refreshed), idempotencyKey: key, input: INPUT }).reused, true);
  assert.equal(summary(f).invocations, 5); assert.equal(summary(f).reserved, 5); assert.equal(count(f, 'notes', 'notes'), 0);
});

test('common chain rechecks distinguish unrelated revoke from our creator and root revocation', async t => {
  const f = await bearerFixture(t), family = await f.family(), actor = f.actor(family), phone = await f.enroll(), unrelated = await f.enroll();
  good(await f.call(f.owner.signer, 'device.revoke', { deviceId: unrelated.deviceId }));
  assert.equal(f.caps.authorize({ actor, action: 'history' }).accountId, f.owner.accountId);
  const sibling = await f.family(); good(await f.ownerCall('access.grants.revoke', { grantId: f.actor(sibling).grantId }));
  assert.equal(f.caps.authorize({ actor, action: 'history' }).accountId, f.owner.accountId);
  assert.throws(() => f.actor(sibling), code('access_denied'));
  const invocationId = f.native.admit({ actor, idempotencyKey: 'oauth_creator_before_effect', input: INPUT }).invocation.invocationId;
  good(await f.call(phone.signer, 'device.revoke', { deviceId: f.owner.deviceId }));
  assert.throws(() => f.caps.authorize({ actor, action: 'history' }), code('access_denied'));
  assert.throws(() => f.native.beginAttempt({ invocationId }), code('access_denied'));
  assert.equal(f.native.reconcile({ invocationId }).outcome, 'not_applied');
  assert.equal(count(f, 'notes', 'notes'), 0);
});

test('two actual OS connections contest one namespace/key with one admission and one reservation', async t => {
  const f = await bearerFixture(t), left = await f.family(), right = await f.family(), key = 'oauth_two_process_key';
  const a = await f.child({ token: left.at.jti, audience: left.resource, input: INPUT, key });
  const b = await f.child({ token: right.at.jti, audience: right.resource, input: { ...INPUT, body: 'second connection text' }, key });
  a.start(); b.start(); const results = await Promise.all([a.finished, b.finished]);
  assert.equal(results.filter(result => result.admitted === true).length, 1);
  assert.equal(results.filter(result => result.errorCode === 'invocation_request_conflict').length, 1);
  assert.deepEqual(summary(f), { invocations: 1, reserved: 1, spent: 0 }); assert.equal(count(f, 'notes', 'notes'), 0);
  const invocationId = results.find(result => result.admitted).invocationId;
  f.native.beginAttempt({ invocationId }); assert.equal(f.native.execute({ invocationId }).outcome, 'committed');
  assert.equal(count(f, 'notes', 'note_native_creates'), 1); assert.deepEqual(summary(f), { invocations: 1, reserved: 0, spent: 1 });
});

test('readiness reflects fixed native binding and key separately from operational native execution', async t => {
  for (const [options, available, nativeReady] of [
    [{ enabled: false }, true, false], [{ keyless: true }, false, true],
    [{ noNative: true }, false, undefined], [{ notesV1: true }, false, false],
    [{ port: native => ({ ...native, storageIdentity: () => ({ projectId: 'wrong', schemaVersion: 2, registryId: 'a'.repeat(32) }) }) }, false, false],
  ]) {
    const f = await bearerFixture(t, options);
    assert.equal(f.oauth.readiness().available, available); assert.equal(f.native?.readiness().ready, nativeReady);
  }
});
