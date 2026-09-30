import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { existsSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createCapabilitiesService, BUILTIN_CAPABILITIES } from '../server/index.mjs';
import { createAccessStore } from '../server/access.mjs';
import { createCatalog } from '../server/catalog.mjs';
import { initializeCapabilitiesSchema, inspectCapabilitiesSchema } from '../server/schema.mjs';
import { initializeCapabilitiesSchema as historicalV2 } from './fixtures/capabilities-v2/schema.mjs';

const PROJECT = 'oauth-independent', ORIGIN = 'https://oauth-independent.test', RESOURCE = ORIGIN + '/mcp';
const OWNER = Object.freeze({ accountId: 'account_review_a', deviceId: 'device_review_a' });
const OTHER = Object.freeze({ accountId: 'account_review_b', deviceId: 'device_review_b' });
const NOW = 1_800_000_000_000;
const sha = value => createHash('sha256').update(value).digest('hex');
const fault = expected => error => error?.code === expected;
const catalog = BUILTIN_CAPABILITIES.map(entry => ({ ...entry, executionEnabled: true }));
const registry = createCatalog(catalog);
const scope = { capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
  resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'] };

function physical(databasePath) {
  return Object.fromEntries(['', '-wal'].map(suffix => [suffix,
    existsSync(databasePath + suffix) ? sha(readFileSync(databasePath + suffix)) : null]));
}

function fixture(t, { version = 3 } = {}) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-oauth-independent-'));
  const nonce = randomBytes(24).toString('hex'), marker = path.join(directory, '.owner');
  writeFileSync(marker, nonce);
  const databasePath = path.join(directory, 'capabilities.sqlite');
  const db = new DatabaseSync(databasePath), handles = new Set([db]);
  let service, time = NOW;
  const actorActive = actor => [OWNER, OTHER].some(known => known.accountId === actor.accountId && known.deviceId === actor.deviceId);
  const close = handle => { handle.close(); handles.delete(handle); };
  t.after(() => {
    for (const handle of [...handles].reverse()) close(handle);
    assert.equal(realpathSync(directory), directory); assert.equal(path.dirname(directory), parent);
    assert.match(path.basename(directory), /^soty-oauth-independent-[A-Za-z0-9_-]+$/u);
    assert.equal(readFileSync(marker, 'utf8'), nonce);
    rmSync(directory, { recursive: true });
  });
  const initial = historicalV2(db, { projectId: PROJECT, allowNativeMigration: true });
  assert.equal(initial.schemaVersion, 2);
  if (version === 3) initializeCapabilitiesSchema(db, { projectId: PROJECT, allowOAuthMigration: true });
  db.exec('PRAGMA foreign_keys=ON; PRAGMA wal_autocheckpoint=0');
  function reopen() {
    if (service) close(service);
    service = createCapabilitiesService({ databasePath, projectId: PROJECT, clock: () => time, actorActive, catalog });
    handles.add(service);
    return service;
  }
  reopen();
  function atomic(action) {
    db.exec('BEGIN IMMEDIATE');
    try { const result = action(); db.exec('COMMIT'); return result; }
    catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  const access = createAccessStore({ db, clock: () => time, actorActive, catalog: registry, transaction: atomic });
  function call(op, args, owner = OWNER) {
    return service.execute({ op: 'access.' + op, args: { expectedAccountId: owner.accountId, ...args }, actor: owner });
  }
  function authority(label, { owner = OWNER, delegate = false } = {}) {
    const { principal } = call('principals.create', { label }, owner);
    const { grant } = call('grants.issue', { principalId: principal.id, ...scope,
      expiresAt: time + 86400000, allowDelegation: delegate, maxDepth: delegate ? 2 : 0,
      budget: { unit: 'invocations', limit: 20 } }, owner);
    return { owner, principal, grant };
  }
  function credential(row, audience = RESOURCE) {
    return call('credentials.issue', { grantId: row.grant.id, audience }, row.owner);
  }
  function original(connection, token = connection.token) {
    const snapshot = { accountId: connection.owner.accountId, clientId: connection.principal.clientId,
      principalId: connection.principal.id, grantId: connection.grant.id, credentialId: token.id, audience: RESOURCE };
    return access.authorizeInvocation({ action: 'dispatch', authorizationSnapshot: snapshot,
      invocation: { ...snapshot, capabilityId: 'notes.createDraft', version: 1,
        capabilityDigest: registry.get('notes.createDraft', 1).digest } });
  }
  return { db, databasePath, initial, atomic, call, authority, credential, original, reopen,
    get service() { return service; }, get now() { return time; }, setTime(value) { time = value; } };
}

// Synthetic future-format OAuth storage only. Parents use the real owner access
// service API with a deterministic host predicate, not actual Connect consent.
// Cipher bytes are a structural placeholder; no bearer/AS implementation is used.
function attachOAuth(f, name, owner = OWNER) {
  const row = { ...f.authority(name, { owner }), id: 'oauth_' + name,
    providerGrantId: sha('provider-' + name).slice(0, 32), createdAt: f.now };
  f.atomic(() => {
    f.db.prepare(`INSERT INTO cap_oauth_connections(id,account_id,client_id,principal_id,root_grant_id,
      creator_device_id,issuer,static_client_id,resource,scope,consent_digest,state,created_at,expires_at)
      VALUES(?,?,?,?,?,?,?,'soty-codex-cli',?,'notes.createDraft',?,'active',?,?)`)
      .run(row.id, owner.accountId, row.principal.clientId, row.principal.id, row.grant.id, owner.deviceId,
        ORIGIN + '/oauth', RESOURCE, sha('consent-' + name), row.createdAt, row.grant.expiresAt);
    artifact(f, row, 'Grant', sha(row.providerGrantId), row.createdAt, row.grant.expiresAt);
    f.db.prepare('UPDATE cap_oauth_connections SET provider_grant_id=? WHERE id=?').run(row.providerGrantId, row.id);
    row.token = token(f, row, name);
  });
  return row;
}

function artifact(f, connection, model, idHash, createdAt, expiresAt) {
  f.db.prepare(`INSERT INTO cap_oauth_artifacts(model,id_hash,issuer,profile,key_id,payload_cipher,payload_digest,
    connection_id,provider_grant_id,created_at,expires_at,retain_until)
    VALUES(?,?,?,'oidc-provider-9.12.2-c1','independent-structural-fixture',?,?,?,?,?,?,?)`)
    .run(model, idHash, ORIGIN + '/oauth', Buffer.alloc(30, 9), sha('not encrypted and not an AS proof'),
      connection.id, connection.providerGrantId, createdAt, expiresAt,
      model === 'AccessToken' ? expiresAt : connection.grant.expiresAt);
}

function token(f, row, suffix) {
  const value = { id: 'credential_review_' + suffix, raw: 'soty_cap_' + randomBytes(32).toString('base64url'),
    createdAt: f.now, expiresAt: f.now + 300000 };
  value.digest = sha(value.raw);
  artifact(f, row, 'AccessToken', value.digest, value.createdAt, value.expiresAt);
  f.db.prepare(`INSERT INTO cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,created_at,expires_at)
    VALUES(?,?,?,?,?,?,?,?,?)`).run(value.id, value.digest, row.owner.accountId, row.principal.clientId,
    row.principal.id, row.grant.id, RESOURCE, value.createdAt, value.expiresAt);
  f.db.prepare('INSERT INTO cap_oauth_credentials(credential_id,connection_id,token_digest,created_at,expires_at) VALUES(?,?,?,?,?)')
    .run(value.id, row.id, value.digest, value.createdAt, value.expiresAt);
  return value;
}

function removeGuardForCorruption(db, name, change) {
  assert.match(name, /^cap_oauth_[a-z_]+$/u);
  const { sql } = db.prepare("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?").get(name);
  db.exec(`DROP TRIGGER ${name}`);
  try { change(); } finally { db.exec(sql); }
}

test('genuine historical2 upgrades without rebasing existing delegated legacy authority', t => {
  const known = {
    'schema.mjs': '9853c33995755cd025ab8e6e02c0ea6e72b77d62b70d775d9c7db2303ff696b6',
    'schema-v2.mjs': '5ca8b4025535566c90a1b1d610e37837b4b41e6074117a96fe720504050af25f',
    'validation.mjs': 'b5cd743caa6183af7b82021eeab0381d6bea9dace806bae12b194df18c15fa9d',
    'native-note-contract.mjs': '1f3ab19fa9e6641e9928ce363ab6fdcbcf1421d4785256466a430ce0bcfa9c83',
  };
  for (const [file, digest] of Object.entries(known)) {
    assert.equal(sha(readFileSync(new URL('./fixtures/capabilities-v2/' + file, import.meta.url))), digest);
  }
  const f = fixture(t, { version: 2 }), root = f.authority('Legacy root', { delegate: true });
  const recipient = f.call('principals.create', { label: 'Separate delegated client' }).principal;
  const child = f.call('grants.derive', { parentGrantId: root.grant.id, principalId: recipient.id, ...scope,
    expiresAt: f.now + 600000, allowDelegation: false, maxDepth: 0 }).grant;
  const childCredential = f.call('credentials.issue', { grantId: child.id, audience: RESOURCE });
  const sibling = f.authority('Other account', { owner: OTHER }), siblingCredential = f.credential(sibling);
  const actorBefore = f.service.authenticateCredential({ token: childCredential.token, audience: RESOURCE });
  const before = f.service.authorize({ actor: actorBefore, action: 'history' });
  const objects = f.db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all();
  const tables = objects.filter(row => row.type === 'table' && row.name !== 'cap_metadata').map(row => row.name);
  const rows = () => Object.fromEntries(tables.map(name => [name, f.db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()]));
  const data = rows(), bytes = physical(f.databasePath);
  assert.deepEqual(initializeCapabilitiesSchema(f.db, { projectId: PROJECT }), f.initial);
  assert.deepEqual(physical(f.databasePath), bytes);
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name LIKE 'cap_oauth_%'").get().n, 0);
  const upgraded = initializeCapabilitiesSchema(f.db, { projectId: PROJECT, allowOAuthMigration: true });
  assert.equal(upgraded.schemaVersion, 3); assert.equal(upgraded.registryId, f.initial.registryId);
  assert.deepEqual(rows(), data);
  for (const object of objects) assert.deepEqual(f.db.prepare('SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name=?').get(object.name), object);
  assert.deepEqual(f.service.authorize({ actor: actorBefore, action: 'history' }), before);
  f.reopen(); assert.equal(f.service.schemaVersion, 3); assert.equal(f.service.oauth, undefined);
  const actorAfter = f.service.authenticateCredential({ token: childCredential.token, audience: RESOURCE });
  assert.deepEqual(f.service.authorize({ actor: actorAfter, action: 'history' }), before);
  f.call('grants.revoke', { grantId: root.grant.id });
  assert.throws(() => f.service.authorize({ actor: actorAfter, action: 'history' }), fault('access_denied'));
  const untouched = f.service.authenticateCredential({ token: siblingCredential.token, audience: RESOURCE });
  assert.equal(f.service.authorize({ actor: untouched, action: 'history' }).accountId, OTHER.accountId);
});

test('legacy delegation cannot target a managed principal; failed routes leave no new grant, token or audit', t => {
  const f = fixture(t), managed = attachOAuth(f, 'managed'), legacy = f.authority('Legacy delegator', { delegate: true });
  const counts = () => ['cap_clients', 'cap_principals', 'cap_grants', 'cap_credentials', 'cap_audit']
    .map(name => f.db.prepare(`SELECT count(*) AS n FROM ${name}`).get().n);
  const before = counts();
  assert.throws(() => f.call('grants.derive', { parentGrantId: legacy.grant.id,
    principalId: managed.principal.id, ...scope, expiresAt: f.now + 200000,
    allowDelegation: false, maxDepth: 0 }), fault('oauth_managed_authority'));
  assert.throws(() => f.call('principals.create', { label: 'Attach to OAuth', clientId: managed.principal.clientId }), fault('invalid_input'));
  assert.throws(() => f.call('credentials.issue', { grantId: managed.grant.id, audience: RESOURCE }, OTHER), fault('access_denied'));
  assert.throws(() => f.service.authenticateCredential({ token: managed.token.raw, audience: RESOURCE }), fault('authorization_required'));
  assert.deepEqual(counts(), before);
  const recipient = f.call('principals.create', { label: 'Lawful legacy recipient' }).principal;
  const derived = f.call('grants.derive', { parentGrantId: legacy.grant.id, principalId: recipient.id,
    ...scope, expiresAt: f.now + 200000, allowDelegation: false, maxDepth: 0 }).grant;
  const issued = f.call('credentials.issue', { grantId: derived.id, audience: RESOURCE });
  assert.equal(f.service.authorize({ actor: f.service.authenticateCredential({ token: issued.token, audience: RESOURCE }), action: 'history' }).principalId, recipient.id);
});

test('expired keyless family revoke stays account-scoped after encrypted artifacts disappear', t => {
  const f = fixture(t), family = attachOAuth(f, 'expired'), sibling = attachOAuth(f, 'sibling', OTHER);
  f.setTime(NOW + 300001);
  const fresh = f.atomic(() => token(f, sibling, 'sibling_fresh'));
  f.db.prepare('DELETE FROM cap_oauth_artifacts WHERE connection_id=?').run(family.id);
  f.reopen(); assert.equal(f.service.oauth, undefined);
  assert.throws(() => f.original(family), fault('authorization_required'));
  assert.equal(f.original(sibling, fresh).expiresAt, fresh.expiresAt);
  const unchanged = f.db.prepare('SELECT * FROM cap_oauth_connections WHERE id=?').get(family.id);
  const auditCount = f.db.prepare('SELECT count(*) AS n FROM cap_audit').get().n;
  assert.throws(() => f.call('credentials.revoke', { credentialId: family.token.id }, OTHER), fault('not_found'));
  assert.deepEqual(f.db.prepare('SELECT * FROM cap_oauth_connections WHERE id=?').get(family.id), unchanged);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_audit').get().n, auditCount);
  f.call('credentials.revoke', { credentialId: family.token.id });
  const revokedAt = f.now;
  f.setTime(f.now + 1000); f.call('credentials.revoke', { credentialId: family.token.id });
  const row = f.db.prepare('SELECT * FROM cap_credentials WHERE id=?').get(family.token.id);
  assert.equal(row.expires_at, family.token.expiresAt); assert.equal(row.revoked_at, revokedAt);
  assert.equal(f.db.prepare('SELECT policy_epoch FROM cap_grants WHERE id=?').get(family.grant.id).policy_epoch, 2);
  assert.equal(f.original(sibling, fresh).credentialId, fresh.id);
  assert.equal(initializeCapabilitiesSchema(f.db, { projectId: PROJECT }).schemaVersion, 3);
});

test('reader refuses missing parents, crossed account links and unlinked AT without repairing any main/WAL bytes', t => {
  for (const corruption of ['missing_parent', 'cross_account_link', 'unlinked_access_token']) {
    const f = fixture(t), a = attachOAuth(f, 'first'), b = attachOAuth(f, 'second', OTHER);
    if (corruption === 'missing_parent') {
      f.db.exec('PRAGMA foreign_keys=OFF');
      f.db.prepare('DELETE FROM cap_credentials WHERE id=?').run(a.token.id);
      f.db.exec('PRAGMA foreign_keys=ON');
      assert.ok(f.db.prepare('PRAGMA foreign_key_check').get());
    } else if (corruption === 'cross_account_link') {
      removeGuardForCorruption(f.db, 'cap_oauth_credential_no_update', () =>
        f.db.prepare('UPDATE cap_oauth_credentials SET connection_id=? WHERE credential_id=?').run(b.id, a.token.id));
      assert.equal(f.db.prepare('PRAGMA foreign_key_check').get(), undefined);
    } else {
      artifact(f, a, 'AccessToken', sha('unlinked AT'), f.now, f.now + 300000);
      assert.equal(f.db.prepare('PRAGMA foreign_key_check').get(), undefined);
    }
    assert.equal(inspectCapabilitiesSchema(f.db, { projectId: PROJECT }).schemaVersion, 3, 'DDL identity is separate from row validation');
    const bytes = physical(f.databasePath);
    assert.throws(() => initializeCapabilitiesSchema(f.db, { projectId: PROJECT }), fault('capabilities_storage_corrupt'));
    assert.equal(f.db.isTransaction, false);
    assert.deepEqual(physical(f.databasePath), bytes, corruption);
    assert.throws(() => createCapabilitiesService({ databasePath: f.databasePath, projectId: PROJECT,
      actorActive: () => true, catalog }), fault('capabilities_storage_corrupt'));
    assert.deepEqual(physical(f.databasePath), bytes, corruption + ' service');
  }
});

test('new-ID REPLACE cannot evict an original credential through its unique digest', t => {
  const f = fixture(t), connection = attachOAuth(f, 'replace');
  f.db.exec('PRAGMA recursive_triggers=OFF');
  const before = f.db.prepare('SELECT * FROM cap_credentials WHERE id=?').get(connection.token.id);
  assert.throws(() => f.db.prepare(`INSERT OR REPLACE INTO cap_credentials
    SELECT 'replacement_new_id',digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at,revoked_at
    FROM cap_credentials WHERE id=?`).run(connection.token.id), /oauth_credential_immutable/u);
  assert.deepEqual(f.db.prepare('SELECT * FROM cap_credentials WHERE id=?').get(connection.token.id), before);
  assert.equal(f.db.prepare("SELECT * FROM cap_credentials WHERE id='replacement_new_id'").get(), undefined);
  assert.equal(f.original(connection).credentialId, connection.token.id);
  assert.equal(initializeCapabilitiesSchema(f.db, { projectId: PROJECT }).schemaVersion, 3);
});

test('terminal input-free reference retains its original link while an expired unused sibling may be deleted', t => {
  const f = fixture(t), connection = attachOAuth(f, 'retained');
  const unused = f.atomic(() => token(f, connection, 'unused'));
  // This is a synthetic generic terminal Invocation, not a fabricated native
  // proof. The retention predicate covers every original reference, including
  // terminal input-free history, without relying on decrypted OAuth artifacts.
  f.db.prepare(`INSERT INTO cap_invocations(id,account_id,client_id,principal_id,grant_id,root_grant_id,
    policy_epoch,capability_id,capability_version,capability_digest,request_key,request_digest,internal_request_id,
    input_json,target_json,authorization_json,status,created_at,updated_at,completed_at)
    VALUES('inv_terminal_retained',?,?,?,?,?,1,'fixture.terminal',1,?,?,?,'request_terminal_retained',
      'null','{}',?,'failed',?,?,?)`).run(connection.owner.accountId, connection.principal.clientId,
      connection.principal.id, connection.grant.id, connection.grant.id, sha('generic-terminal-contract'),
      sha('terminal-request-key'), sha('terminal-request-body'), JSON.stringify({ credentialId: connection.token.id,
        expiresAt: connection.token.expiresAt }), f.now, f.now, f.now);
  f.setTime(NOW + 300001);
  f.db.exec('DELETE FROM cap_oauth_artifacts');
  assert.throws(() => f.db.prepare('DELETE FROM cap_oauth_credentials WHERE credential_id=?').run(connection.token.id), /oauth_credential_referenced/u);
  f.atomic(() => {
    assert.equal(f.db.prepare('DELETE FROM cap_oauth_credentials WHERE credential_id=?').run(unused.id).changes, 1);
    assert.equal(f.db.prepare('DELETE FROM cap_credentials WHERE id=?').run(unused.id).changes, 1);
  });
  f.call('credentials.revoke', { credentialId: connection.token.id });
  f.reopen();
  const retained = f.db.prepare('SELECT * FROM cap_credentials WHERE id=?').get(connection.token.id);
  assert.equal(retained.expires_at, connection.token.expiresAt);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_credentials').get().n, 1);
  assert.equal(f.db.prepare('SELECT input_json FROM cap_invocations').get().input_json, 'null');
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_artifacts').get().n, 0);
  assert.throws(() => f.original(connection), fault('authorization_required'));
});

test('partial3 and unknown4 fail before persistent journal changes or new authority writes', t => {
  for (const variant of ['extra_index', 'altered_guard', 'future4']) {
    const f = fixture(t), ordinary = f.authority('Known legacy account'), issued = f.credential(ordinary);
    f.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
    if (variant === 'extra_index') f.db.exec('CREATE INDEX unknown_oauth_index ON cap_oauth_connections(expires_at)');
    else if (variant === 'altered_guard') f.db.exec(`DROP TRIGGER cap_oauth_connection_no_delete;
      CREATE TRIGGER cap_oauth_connection_no_delete BEFORE DELETE ON cap_oauth_connections
      WHEN 0 BEGIN SELECT RAISE(ABORT,'oauth_connection_immutable'); END;`);
    else f.db.exec('PRAGMA user_version=4');
    const bytes = physical(f.databasePath), code = variant === 'future4' ? 'schema_version_unsupported' : 'schema_layout_invalid';
    assert.throws(() => initializeCapabilitiesSchema(f.db, { projectId: PROJECT, allowOAuthMigration: true }), fault(code));
    assert.deepEqual(physical(f.databasePath), bytes);
    if (variant === 'future4') {
      assert.throws(() => f.service.authenticateCredential({ token: issued.token, audience: RESOURCE }), fault('schema_version_unsupported'));
      assert.throws(() => f.credential(ordinary), fault('schema_version_unsupported'));
      assert.deepEqual(physical(f.databasePath), bytes);
    }
  }
});
