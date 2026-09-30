import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { initializeCapabilitiesSchema, inspectCapabilitiesSchema } from '../server/schema.mjs';
import { createCapabilitiesService } from '../server/index.mjs';
import { database, files, objects, code, PROJECT, OWNER, seedOAuth, seedInteraction, insertArtifact, withDroppedGuard, insertInvocation, sha } from './support/oauth-baseline.mjs';

const upgrade = db => initializeCapabilitiesSchema(db, { projectId: PROJECT, allowOAuthMigration: true });
const reopen = db => initializeCapabilitiesSchema(db, { projectId: PROJECT });
const service = databasePath => createCapabilitiesService({ databasePath, projectId: PROJECT, actorActive: () => true });

test('strict default-off preserves actual fresh/v1/v2; only an existing native v2 can opt in', t => {
  for (const version of [0, 1, 2]) {
    const f = database(t, { version });
    const before = objects(f.db), journal = f.db.prepare('PRAGMA journal_mode').get();
    if (version < 2) {
      assert.throws(() => upgrade(f.db), code('oauth_migration_requires_native_v2'));
      assert.throws(() => initializeCapabilitiesSchema(f.db, { projectId: PROJECT, allowOAuthMigration: true, allowNativeMigration: true }), code('oauth_migration_requires_native_v2'));
      assert.deepEqual(objects(f.db), before); assert.deepEqual(f.db.prepare('PRAGMA journal_mode').get(), journal);
    }
    assert.equal(reopen(f.db).schemaVersion, version || 1);
    for (const flag of [null, 1, 'true', {}]) assert.throws(() => initializeCapabilitiesSchema(f.db,
      { projectId: PROJECT, allowOAuthMigration: flag }), code('schema_configuration_invalid'));
    const opened = service(f.databasePath);
    assert.equal(opened.schemaVersion, version || 1); assert.deepEqual(opened.supportedSchemaVersions, [1, 2, 3]);
    assert.equal(opened.oauth, undefined); opened.close();
  }
});

test('real frozen v2 to v3 is additive and preserves registry, semantic pins and all old SQL', t => {
  const f = database(t, { version: 2 });
  const opened = service(f.databasePath), registryId = opened.registryId; opened.close();
  const oldObjects = objects(f.db), contracts = f.db.prepare('SELECT * FROM cap_contracts').all();
  assert.deepEqual(upgrade(f.db), { schemaVersion: 3, registryId });
  assert.deepEqual(objects(f.db).filter(row => oldObjects.some(old => old.name === row.name)), oldObjects);
  const added = objects(f.db).filter(row => !oldObjects.some(old => old.name === row.name));
  assert.equal(added.filter(row => row.type === 'table').length, 4);
  assert.equal(added.filter(row => row.type === 'index').length, 10);
  assert.equal(added.filter(row => row.type === 'trigger').length, 14);
  assert.deepEqual(f.db.prepare('SELECT * FROM cap_contracts').all(), contracts);
  f.close(f.db);
  for (let i = 0; i < 2; i++) {
    const current = service(f.databasePath);
    assert.equal(current.schemaVersion, 3); assert.equal(current.registryId, registryId); assert.equal(current.oauth, undefined);
    current.close();
  }
});

test('frozen old2 refuses real main2 plus WAL3 without changing main/WAL; child exits completely', t => {
  const f = database(t, { version: 2 });
  f.db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA wal_autocheckpoint=0');
  assert.equal(readFileSync(f.databasePath).readUInt32BE(60), 2);
  upgrade(f.db);
  assert.equal(readFileSync(f.databasePath).readUInt32BE(60), 2);
  const before = files(f.databasePath);
  const historicalUrl = new URL('./fixtures/capabilities-v2/schema.mjs', import.meta.url).href;
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', `
    import { DatabaseSync } from 'node:sqlite';
    import { initializeCapabilitiesSchema } from ${JSON.stringify(historicalUrl)};
    const db=new DatabaseSync(process.argv[1]);
    try { initializeCapabilitiesSchema(db,{projectId:${JSON.stringify(PROJECT)}}); process.exitCode=2; }
    catch(error) { process.stdout.write(error.code || 'unknown'); }
    finally { db.close(); }`, f.databasePath], { windowsHide: true, encoding: 'utf8', timeout: 10000 });
  assert.equal(child.error, undefined); assert.equal(child.status, 0); assert.equal(child.stdout, 'schema_version_unsupported');
  const after = files(f.databasePath); assert.equal(after[''], before['']); assert.equal(after['-wal'], before['-wal']);
  assert.equal(reopen(f.db).schemaVersion, 3);
});

test('future4, partial3, wrong project and changed trigger are refused before persistent pragmas', t => {
  for (const change of [db => db.exec('PRAGMA user_version=4'),
    db => db.exec('DROP TABLE cap_oauth_interactions'),
    db => db.exec("DROP TRIGGER cap_oauth_connection_no_delete; CREATE TRIGGER cap_oauth_connection_no_delete BEFORE DELETE ON cap_oauth_connections BEGIN SELECT 1; END"),
    db => withDroppedGuard(db, 'cap_identity_no_update', () => db.exec("UPDATE cap_metadata SET value='another-project' WHERE key='project_id'"))]) {
    const f = database(t); change(f.db);
    f.db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');
    const before = files(f.databasePath), snapshot = objects(f.db);
    assert.throws(() => reopen(f.db), error => ['schema_version_unsupported', 'schema_layout_invalid', 'capabilities_project_mismatch'].includes(error.code));
    assert.equal(f.db.prepare('PRAGMA journal_mode').get().journal_mode, 'delete');
    assert.deepEqual(files(f.databasePath), before); assert.deepEqual(objects(f.db), snapshot);
  }
});

test('invalid old authorization JSON refuses whole migration with no v3 objects/identity changes', t => {
  const f = database(t, { version: 2 });
  insertInvocation(f.db, { authorizationJson: '{' });
  const before = objects(f.db), metadata = f.db.prepare('SELECT * FROM cap_metadata ORDER BY key').all();
  assert.throws(() => upgrade(f.db), code('capabilities_storage_corrupt'));
  assert.equal(f.db.isTransaction, false); assert.equal(f.db.prepare('PRAGMA user_version').get().user_version, 2);
  assert.deepEqual(objects(f.db), before); assert.deepEqual(f.db.prepare('SELECT * FROM cap_metadata ORDER BY key').all(), metadata);
});

test('admission excludes previously issued credentials and other grants/principals', t => {
  for (const kind of ['credential', 'grant', 'principal', 'invocation']) {
    const f = database(t);
    assert.throws(() => seedOAuth(f.db, { beforeConnection(row) {
      if (kind === 'credential') f.db.prepare(`INSERT INTO cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at)
        VALUES('old',?,?,?,?,?,?,?,?)`).run(sha('old'), row.accountId, row.clientId, row.principalId, row.grantId, row.resource, row.expiresAt, row.createdAt);
      if (kind === 'grant') f.db.prepare(`INSERT INTO cap_grants SELECT 'other',account_id,client_id,principal_id,NULL,'other',creator_device_id,
        capabilities_json,resources_json,effects_json,recipients_json,allow_delegation,max_depth,depth,not_before,expires_at,policy_epoch,created_at,revoked_at
        FROM cap_grants WHERE id=?`).run(row.grantId);
      if (kind === 'principal') f.db.prepare(`INSERT INTO cap_principals SELECT 'other',account_id,client_id,kind,label,state,creator_device_id,created_at,revoked_at
        FROM cap_principals WHERE id=?`).run(row.principalId);
      if (kind === 'invocation') insertInvocation(f.db, { clientId: row.clientId });
    } }), /oauth_connection_binding_invalid/u);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_clients').get().n, 0);
  }
});

test('immutable SQL guards reject replace, expiry/rebinding, consumption reset and referenced cleanup', t => {
  const f = database(t), row = seedOAuth(f.db);
  f.db.exec('PRAGMA recursive_triggers=OFF');
  const mutations = [
    () => f.db.exec('INSERT OR REPLACE INTO cap_oauth_connections SELECT * FROM cap_oauth_connections'),
    () => f.db.prepare('UPDATE cap_oauth_connections SET expires_at=expires_at+1 WHERE id=?').run(row.id),
    () => f.db.prepare('UPDATE cap_oauth_connections SET provider_grant_id=? WHERE id=?').run('a'.repeat(32), row.id),
    () => f.db.prepare('DELETE FROM cap_oauth_connections WHERE id=?').run(row.id),
    () => f.db.exec('INSERT OR REPLACE INTO cap_credentials SELECT * FROM cap_credentials'),
    () => f.db.exec('INSERT OR REPLACE INTO cap_oauth_credentials SELECT * FROM cap_oauth_credentials'),
    () => f.db.exec('UPDATE cap_oauth_credentials SET expires_at=expires_at-1'),
    () => f.db.exec('UPDATE cap_credentials SET expires_at=expires_at+1'),
    () => f.db.exec("UPDATE cap_oauth_artifacts SET payload_digest='" + '0'.repeat(64) + "' WHERE model='Grant'"),
    () => f.db.exec('INSERT OR REPLACE INTO cap_oauth_artifacts SELECT * FROM cap_oauth_artifacts')
  ];
  for (const mutate of mutations) assert.throws(mutate, /oauth_.+immutable/u);
  insertInvocation(f.db, { credentialId: row.credentialId, clientId: row.clientId });
  assert.throws(() => f.db.exec('DELETE FROM cap_oauth_credentials'), /oauth_credential_referenced/u);
  assert.equal(reopen(f.db).schemaVersion, 3);
});

test('expired raw artifacts may disappear while immutable credential links survive reopen', t => {
  const f = database(t), row = seedOAuth(f.db);
  f.db.prepare('DELETE FROM cap_oauth_artifacts WHERE connection_id=?').run(row.id);
  assert.equal(reopen(f.db).schemaVersion, 3);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_credentials').get().n, 1);
});

test('stored native redirects stay loopback HTTP, while issuer/resource remain canonical HTTPS', t => {
  for (const redirect of ['http://127.0.0.1:8765/callback', 'http://localhost:8765/oauth/callback', 'http://[::1]:8765/auth']) {
    const f = database(t); seedInteraction(f.db, { redirect }); assert.equal(reopen(f.db).schemaVersion, 3);
  }
  for (const redirect of ['https://remote.test/callback', 'http://remote.test:8765/callback', 'http://127.0.0.1/callback',
    'http://127.0.0.1:8765/callback?state=x', 'http://127.0.0.1:8765/callback#x', 'http://name@127.0.0.1:8765/callback']) {
    const f = database(t); seedInteraction(f.db, { redirect });
    assert.throws(() => reopen(f.db), code('capabilities_storage_corrupt'));
  }
});

test('interaction decisions are write-once and approved pins must match the exact fresh connection', t => {
  const f = database(t), row = seedOAuth(f.db), hash = seedInteraction(f.db, { connection: row });
  const approve = () => f.db.prepare(`UPDATE cap_oauth_interactions SET decision='approved',decided_at=1000,
    decided_account_id=?,decided_device_id=?,connection_id=? WHERE uid_hash=?`).run(row.accountId, row.deviceId, row.id, hash);
  assert.throws(() => f.db.prepare(`UPDATE cap_oauth_interactions SET decision='approved',decided_at=1000,
    decided_account_id='another',decided_device_id=?,connection_id=? WHERE uid_hash=?`).run(row.deviceId, row.id, hash), /oauth_interaction_immutable/u);
  approve(); assert.equal(reopen(f.db).schemaVersion, 3);
  assert.throws(() => f.db.exec('INSERT OR REPLACE INTO cap_oauth_interactions SELECT * FROM cap_oauth_interactions'), /oauth_interaction_immutable/u);
  assert.throws(() => f.db.exec("UPDATE cap_oauth_interactions SET decision='denied',connection_id=NULL"), /oauth_interaction_immutable/u);
  assert.throws(() => f.db.exec('UPDATE cap_oauth_interactions SET expires_at=expires_at+1'), /oauth_interaction_immutable/u);
});

test('consumed RT cannot be restored; short Session updates cannot extend their original retention window', t => {
  const f = database(t), row = seedOAuth(f.db), rawId = 'R'.repeat(43);
  insertArtifact(f.db, { model: 'RefreshToken', rawId, connection: row, expiresAt: row.expiresAt });
  f.db.prepare("UPDATE cap_oauth_artifacts SET consumed_at=2000 WHERE model='RefreshToken'").run();
  assert.throws(() => f.db.exec("UPDATE cap_oauth_artifacts SET consumed_at=NULL WHERE model='RefreshToken'"), /oauth_artifact_immutable/u);
  assert.throws(() => f.db.exec("UPDATE cap_oauth_artifacts SET consumed_at=2001 WHERE model='RefreshToken'"), /oauth_artifact_immutable/u);
  f.db.prepare(`INSERT INTO cap_oauth_artifacts(model,id_hash,issuer,profile,key_id,payload_cipher,payload_digest,
    session_uid_hash,created_at,expires_at,retain_until) VALUES('Session',?,?,'oidc-provider-9.12.2-c1','synthetic',?,?,?,1000,2000,601000)`)
    .run(sha('session'), row.issuer, Buffer.alloc(30, 7), sha('payload'), sha('uid'));
  f.db.exec("UPDATE cap_oauth_artifacts SET expires_at=3000 WHERE model='Session'");
  assert.throws(() => f.db.exec("UPDATE cap_oauth_artifacts SET expires_at=602000 WHERE model='Session'"), /oauth_artifact_immutable/u);
  assert.equal(reopen(f.db).schemaVersion, 3);
});

test('row corruption is refused without repair even after exact guard definitions are restored', t => {
  for (const corrupt of [
    db => db.exec("UPDATE cap_principals SET account_id='other'"),
    db => db.exec("UPDATE cap_grants SET effects_json='[\"read\"]'"),
    db => withDroppedGuard(db, 'cap_oauth_credential_row_guard', () => db.exec('UPDATE cap_credentials SET expires_at=expires_at+1')),
    db => db.exec("UPDATE cap_oauth_artifacts SET payload_cipher=x'00'"),
    db => withDroppedGuard(db, 'cap_oauth_connection_update_guard', () => db.exec("UPDATE cap_oauth_connections SET issuer='https://wrong.test/oauth'")),
  ]) {
    const f = database(t); seedOAuth(f.db); f.db.exec('PRAGMA ignore_check_constraints=ON'); corrupt(f.db);
    f.db.exec('PRAGMA ignore_check_constraints=OFF');
    const before = files(f.databasePath);
    assert.throws(() => reopen(f.db), code('capabilities_storage_corrupt'));
    const after = files(f.databasePath); assert.equal(after[''], before['']); assert.equal(after['-wal'], before['-wal']);
  }
});

test('reference and OAuth key indexes restrict actual lookup among 12000 other invocations', t => {
  const f = database(t), connection = seedOAuth(f.db), targetKey = sha('shared-key');
  f.db.exec('BEGIN IMMEDIATE');
  for (let i = 0; i < 12000; i++) insertInvocation(f.db, { id: 'inv_' + i, accountId: 'account_' + (i % 100), credentialId: 'credential_' + i });
  insertInvocation(f.db, { id: 'inv_target', accountId: OWNER.accountId, clientId: connection.clientId,
    credentialId: connection.credentialId, requestKey: targetKey });
  f.db.exec('COMMIT; ANALYZE');
  const referenceSql = "SELECT 1 FROM cap_invocations WHERE json_extract(authorization_json,'$.credentialId')=? LIMIT 1";
  const namespaceSql = `SELECT 1 FROM cap_invocations i JOIN cap_oauth_connections c ON c.client_id=i.client_id
    WHERE i.account_id=? AND i.request_key=? AND c.account_id=i.account_id AND c.issuer=?
      AND c.static_client_id=? AND c.resource=? AND c.id!=? LIMIT 1`;
  const args = [OWNER.accountId, targetKey, connection.issuer, connection.staticClientId, connection.resource, 'another-connection'];
  assert.equal(f.db.prepare(referenceSql).get(connection.credentialId)['1'], 1);
  assert.equal(f.db.prepare(namespaceSql).get(...args)['1'], 1);
  const referencePlan = f.db.prepare('EXPLAIN QUERY PLAN ' + referenceSql).all(connection.credentialId).map(row => row.detail).join('\n');
  const namespacePlan = f.db.prepare('EXPLAIN QUERY PLAN ' + namespaceSql).all(...args).map(row => row.detail).join('\n');
  assert.match(referencePlan, /SEARCH cap_invocations USING (?:COVERING )?INDEX cap_invocations_original_credential/u);
  assert.match(namespacePlan, /SEARCH i USING COVERING INDEX cap_invocations_oauth_request/u);
  assert.doesNotMatch(referencePlan + namespacePlan, /SCAN (?:cap_invocations|i)(?:\s|$)/u);
});
