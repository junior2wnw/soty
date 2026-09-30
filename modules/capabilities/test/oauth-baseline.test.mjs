import test from 'node:test';
import assert from 'node:assert/strict';
import { createAccessStore } from '../server/access.mjs';
import { createCatalog, BUILTIN_CAPABILITIES } from '../server/catalog.mjs';
import { createCapabilitiesService } from '../server/index.mjs';
import { initializeCapabilitiesSchema } from '../server/schema.mjs';
import { database, seedOAuth, addCredential, sha, code, OWNER, PROJECT, ORIGIN, withDroppedGuard } from './support/oauth-baseline.mjs';
import { connectedFixture, PROJECT as NATIVE_PROJECT } from './support/native-connected.mjs';

function fixture(t) {
  const f = database(t); let time = 1000, active = true;
  const catalog = BUILTIN_CAPABILITIES.map(entry => ({ ...entry, executionEnabled: true }));
  const ownerActive = actor => active && actor.accountId === OWNER.accountId && actor.deviceId === OWNER.deviceId;
  const service = createCapabilitiesService({ databasePath: f.databasePath, projectId: PROJECT, clock: () => time, actorActive: ownerActive, catalog });
  f.beforeClose(() => service.close());
  const access = createAccessStore({ db: f.db, clock: () => time, actorActive: ownerActive, catalog: createCatalog(catalog),
    transaction(action) {
      f.db.exec('BEGIN IMMEDIATE');
      try { const result = action(); f.db.exec('COMMIT'); return result; }
      catch (error) { if (f.db.isTransaction) f.db.exec('ROLLBACK'); throw error; }
    } });
  const call = (op, args) => service.execute({ op: 'access.' + op, actor: OWNER, args: { expectedAccountId: OWNER.accountId, ...args } });
  function original(row) {
    return access.authorizeInvocation({ action: 'dispatch', authorizationSnapshot: { credentialId: row.credentialId,
      accountId: row.accountId, clientId: row.clientId, principalId: row.principalId, grantId: row.grantId, audience: row.resource },
    invocation: { accountId: row.accountId, clientId: row.clientId, principalId: row.principalId, grantId: row.grantId,
      capabilityId: 'notes.createDraft', version: 1, capabilityDigest: createCatalog(catalog).get('notes.createDraft', 1).digest } });
  }
  return { ...f, service, access, call, original, time(value) { time = value; }, revokeCreator() { active = false; } };
}

test('OAuth-off baseline denies legacy issue/derive and legacy-shaped tokens for linked authority', t => {
  const f = fixture(t), row = seedOAuth(f.db, { credential: false });
  addCredential(f.db, row, { token: 'soty_cap_' + 'A'.repeat(43) });
  const counts = () => f.db.prepare('SELECT (SELECT count(*) FROM cap_grants) AS grants,(SELECT count(*) FROM cap_credentials) AS credentials').get();
  const before = counts();
  assert.throws(() => f.service.authenticateCredential({ token: row.token, audience: ORIGIN }), code('authorization_required'));
  assert.throws(() => f.call('credentials.issue', { grantId: row.grantId, audience: ORIGIN }), code('oauth_managed_authority'));
  const scope = { principalId: row.principalId, capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
    resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'], expiresAt: 40000,
    allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 1 } };
  assert.throws(() => f.call('grants.issue', scope), code('oauth_managed_authority'));
  const { budget: ignored, ...derived } = scope;
  assert.throws(() => f.call('grants.derive', { ...derived, parentGrantId: row.grantId }), code('oauth_managed_authority'));
  assert.deepEqual(counts(), before);
  const normal = f.call('principals.create', { label: 'Independent service' }).principal;
  const grant = f.call('grants.issue', { ...scope, principalId: normal.id }).grant;
  const credential = f.call('credentials.issue', { grantId: grant.id, audience: ORIGIN });
  const actor = f.service.authenticateCredential({ token: credential.token, audience: ORIGIN });
  assert.equal(f.service.authorize({ actor, action: 'history' }).principalId, normal.id);
});

test('original OAuth authorization requires immutable link even without AS composition/key', t => {
  const f = fixture(t), row = seedOAuth(f.db);
  assert.equal(f.original(row).credentialId, row.credentialId);
  // An arbitrary additional credential on the same client cannot bypass the link.
  f.db.prepare(`INSERT INTO cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at)
    VALUES('rogue',?,?,?,?,?,?,?,?)`).run(sha('rogue'), row.accountId, row.clientId, row.principalId, row.grantId, row.resource, row.expiresAt, row.createdAt);
  assert.throws(() => f.original({ ...row, credentialId: 'rogue' }), code('authorization_required'));
  assert.throws(() => initializeCapabilitiesSchema(f.db, { projectId: PROJECT }), code('capabilities_storage_corrupt'));
  f.db.exec("DELETE FROM cap_credentials WHERE id='rogue'");
  withDroppedGuard(f.db, 'cap_oauth_credential_no_update', () => f.db.prepare('UPDATE cap_oauth_credentials SET token_digest=? WHERE credential_id=?').run(sha('wrong'), row.credentialId));
  assert.throws(() => f.original(row), code('authorization_required'));
});

test('original credential expiry is absolute; newer token/sibling revocation cannot replace it', t => {
  const f = fixture(t), row = seedOAuth(f.db), sibling = seedOAuth(f.db, { suffix: 'sibling' });
  f.call('credentials.revoke', { credentialId: sibling.credentialId });
  assert.equal(f.original(row).credentialId, row.credentialId);
  const old = { ...row };
  addCredential(f.db, row, { id: 'credential_new', createdAt: 300000, expiresAt: 600000 });
  f.time(301000);
  assert.throws(() => f.original(old), code('authorization_required'));
  assert.equal(f.original(row).credentialId, 'credential_new');
  f.revokeCreator(); assert.throws(() => f.original(row), code('access_denied'));
});

test('a foreign leaf with an OAuth root cannot become an unlinked legacy authority after parent tampering', t => {
  const f = fixture(t), row = seedOAuth(f.db);
  const other = f.call('principals.create', { label: 'Unrelated service' }).principal;
  // External corruption of a parent table is not a supported grant operation.
  // The common resolver must still see the immutable OAuth root link.
  f.db.prepare('UPDATE cap_grants SET allow_delegation=1,max_depth=1 WHERE id=?').run(row.grantId);
  f.db.prepare(`INSERT INTO cap_grants SELECT 'bad_child',account_id,?, ?,id,root_id,creator_device_id,
    capabilities_json,resources_json,effects_json,recipients_json,0,0,1,not_before,expires_at,policy_epoch,created_at,revoked_at
    FROM cap_grants WHERE id=?`).run(other.clientId, other.id, row.grantId);
  const token = 'soty_cap_' + 'B'.repeat(43);
  f.db.prepare(`INSERT INTO cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at)
    VALUES('bad_child_credential',?,?,?,?,?,?,?,?)`).run(sha(token), row.accountId, other.clientId, other.id, 'bad_child', row.resource, row.expiresAt, row.createdAt);
  assert.throws(() => f.service.authenticateCredential({ token, audience: ORIGIN }), code('authorization_required'));
  assert.throws(() => f.original({ ...row, credentialId: 'bad_child_credential', clientId: other.clientId,
    principalId: other.id, grantId: 'bad_child' }), code('authorization_required'));
});

test('generic credential revoke closes the complete OAuth family, remains idempotent and leaves sibling', t => {
  const f = fixture(t), row = seedOAuth(f.db), sibling = seedOAuth(f.db, { suffix: 'b' });
  const first = row.credentialId;
  addCredential(f.db, row, { id: 'credential_second' });
  f.time(2000); f.call('credentials.revoke', { credentialId: first });
  f.time(3000); f.call('credentials.revoke', { credentialId: first });
  const connection = f.db.prepare('SELECT * FROM cap_oauth_connections WHERE id=?').get(row.id);
  assert.equal(connection.state, 'revoked'); assert.equal(connection.revoked_at, 2000);
  assert.deepEqual(f.db.prepare('SELECT revoked_at FROM cap_credentials WHERE client_id=?').all(row.clientId).map(item => item.revoked_at), [2000, 2000]);
  assert.equal(f.db.prepare('SELECT policy_epoch FROM cap_grants WHERE id=?').get(row.grantId).policy_epoch, 2);
  assert.throws(() => f.original(row), code('authorization_required')); assert.equal(f.original(sibling).grantId, sibling.grantId);
  assert.equal(initializeCapabilitiesSchema(f.db, { projectId: PROJECT }).schemaVersion, 3);
});

test('ordinary root/principal revoke may leave stored connection active while current access is denied', t => {
  for (const type of ['grant', 'principal']) {
    const f = fixture(t), row = seedOAuth(f.db);
    if (type === 'grant') f.call('grants.revoke', { grantId: row.grantId });
    else f.call('principals.revoke', { principalId: row.principalId });
    assert.equal(f.db.prepare('SELECT state FROM cap_oauth_connections WHERE id=?').get(row.id).state, 'active');
    assert.throws(() => f.original(row), code('access_denied'));
    assert.equal(initializeCapabilitiesSchema(f.db, { projectId: PROJECT }).schemaVersion, 3);
  }
});

test('Caps2 native proof remains reconcilable on exact3/off after real Connect revoke and Notes purge', async t => {
  const f = await connectedFixture(t, { port: port => ({ ...port, createDraftForInvocation(args) {
    port.createDraftForInvocation(args); throw new Error('synthetic_lost_notes_commit_response');
  } }) });
  const identity = await f.issue();
  const admitted = f.native.admit({ actor: identity.actor, idempotencyKey: 'schema3_proof_recovery', input: { title: 'Kept proof', body: 'Private content' } });
  const invocationId = admitted.invocation.invocationId, registryId = f.caps.registryId;
  f.native.beginAttempt({ invocationId });
  assert.throws(() => f.native.execute({ invocationId }), /synthetic_lost_notes_commit_response/u);
  assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 1);
  f.sql('caps', db => initializeCapabilitiesSchema(db, { projectId: NATIVE_PROJECT, allowOAuthMigration: true }));
  assert.equal(f.native.readiness().ready, false); // Instance admitted2 cannot silently upgrade its own pins.
  assert.throws(() => f.native.reconcile({ invocationId }), code('native_unavailable'));
  f.restart({ enabled: false, port: port => port });
  assert.equal(f.caps.schemaVersion, 3); assert.equal(f.caps.registryId, registryId); assert.equal(f.notes.schemaVersion, 2);
  const note = f.sql('notes', db => db.prepare('SELECT id,revision FROM notes').get());
  const actor = f.actor(identity.credential.token);
  await f.call('notes.put', { expectedAccountId: f.account.accountId, noteId: note.id, expectedRevision: note.revision,
    mutationId: 'migration_delete_note', title: 'Changed', body: '', items: [], color: 'plain', pinned: false, state: 'trashed' });
  const deleted = f.sql('notes', db => db.prepare('SELECT revision FROM notes WHERE id=?').get(note.id));
  await f.call('notes.purge', { expectedAccountId: f.account.accountId, noteId: note.id, expectedRevision: deleted.revision, mutationId: 'migration_purge_note' });
  await f.access('credentials.revoke', { credentialId: identity.credential.credential.id });
  const recovered = f.native.reconcile({ invocationId });
  assert.equal(recovered.outcome, 'committed'); assert.equal(recovered.invocation.receipt.artifacts[0].id, note.id);
  assert.throws(() => f.native.get({ actor, invocationId }), code('authorization_required'));
  assert.equal(f.sql('notes', db => db.prepare("SELECT count(*) AS n FROM notes WHERE state!='deleted'").get().n), 0);
  assert.equal(f.sql('caps', db => db.prepare('SELECT spent_amount FROM cap_budgets').get().spent_amount), 1);
  f.restart({ enabled: false });
  assert.equal(f.native.reconcilePage().items.length, 0);
  assert.equal((await f.access('invocations.list')).invocations[0].status, 'succeeded');
});

test('new native execution on3 keeps @1 identity and denies future runtime version drift', async t => {
  const f = await connectedFixture(t);
  f.sql('caps', db => initializeCapabilitiesSchema(db, { projectId: NATIVE_PROJECT, allowOAuthMigration: true }));
  f.restart();
  const identity = await f.issue();
  const result = f.create(identity.actor, { title: '😀', body: 'No normalization: е\u0301' }, 'new_native_schema3');
  assert.equal(result.outcome, 'committed'); assert.equal(f.native.readiness().ready, true);
  f.sql('caps', db => db.exec('PRAGMA user_version=4'));
  assert.equal(f.native.readiness().ready, false);
  assert.throws(() => f.native.reconcilePage(), code('native_unavailable'));
  f.sql('caps', db => db.exec('PRAGMA user_version=3'));
  assert.equal(f.native.reconcile({ invocationId: result.invocation.invocationId }).outcome, 'committed');
});
