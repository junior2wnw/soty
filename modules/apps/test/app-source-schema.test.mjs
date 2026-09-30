import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHistoricalAppsV3, seedHistoricalPublicationV3 } from '../../../deploy/connector/apps-v3.fixture.mjs';
import { migrateAppsSchema, inspectAppsSchema, ensureInitialPublication, requiredBindingVersion, runtimeTargetDigest } from '../server/schema.mjs';

const id = `app-${'a'.repeat(32)}`, revoked = `app-${'b'.repeat(32)}`, owner = 'account_A', key = 'link_A|host_A|connector_A';
const rows = (db, table) => db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
const snapshot = db => ({ version: db.prepare('PRAGMA user_version').get().user_version,
  schema: db.prepare("SELECT name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all(),
  tables: Object.fromEntries(db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*'").all().map(({ name }) => [name, rows(db, name)])) });
function historical(t) {
  const db = new DatabaseSync(':memory:'); db.exec('PRAGMA foreign_keys=ON'); t.after(() => db.close()); createHistoricalAppsV3(db);
  db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(key, owner, JSON.stringify({ linkId: 'link_A', hostDeviceId: 'host_A', connectorId: 'connector_A' }), 'Source', 1);
  for (const [app, state, port] of [[id, 'enabled', 8080], [revoked, 'revoked', 8081]]) {
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(app, owner, key, 'Original', port, '/#/dashboard',
      JSON.stringify({ accountIds: ['friend'], communityIds: [] }), state, 7, 1, 5);
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(app, 'account', 'friend');
    db.prepare('INSERT INTO app_domain_heads VALUES (?,4)').run(app); seedHistoricalPublicationV3(db, app);
  }
  db.exec("INSERT INTO app_domain_zones VALUES ('zone_named','named','https://{slug}.apps.example','apps.example','https','',1)");
  for (const [domain, slug, state, retiredAt] of [['d'.repeat(32), 'named', 'bound', null], ['e'.repeat(32), 'retired', 'tombstone', 3]]) {
    db.prepare("INSERT INTO app_domains VALUES (?,?,?,?,?,?,?,'alias',?,?,?)").run(`dom_${domain}`, 'zone_named', `${slug}.apps.example`,
      `https://${slug}.apps.example`, slug, id, owner, state, 2, retiredAt);
  }
  const target = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=?').get(id);
  const ack = { scope: 'whole-port', targetRevision: 1, targetDigest: target.digest, profile: target.profile };
  db.prepare("UPDATE app_publications SET launch_policy='anyone',listed=1,policy_epoch=4,exposure_ack_revision=1,exposure_ack_json=? WHERE app_id=?").run(JSON.stringify(ack), id);
  db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)').run(id, `dom_${'d'.repeat(32)}`, owner);
  db.prepare('INSERT INTO app_publication_receipts VALUES (?,?,?,?,?,?,?)').run(owner, 'old-receipt', 'old-intent', id, 4, JSON.stringify({ historical: true }), 5);
  return db;
}
function extraTarget(db, revision = 2) {
  const first = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=1').get(id);
  const digest = runtimeTargetDigest({ appId: id, revision, ownerAccountId: owner, connectorKey: key, port: 9000,
    entryPath: '/new', profile: first.profile });
  db.prepare('INSERT INTO app_runtime_targets VALUES (?,?,?,?,?,?,?,?,?)').run(id, revision, owner, key, 9000, '/new', first.profile, digest, 10);
}

test('genuine frozen v3 → latest preserves targets, public consent, grants, origins, tombstones, receipts and revoked apps exactly', t => {
  const db = historical(t), before = snapshot(db);
  assert.equal(inspectAppsSchema(db), 'v3');
  assert.equal(migrateAppsSchema(db).schema, 'soty.apps-registry.v6'); assert.equal(inspectAppsSchema(db), 'v6');
  for (const [table, value] of Object.entries(before.tables)) if (table !== 'apps_meta') assert.deepEqual(rows(db, table), value, table);
  assert.deepEqual(rows(db, 'app_source_heads').map(row => ({ ...row })), [{ app_id: id, required_binding_version: 1 }, { app_id: revoked, required_binding_version: 1 }]);
  assert.deepEqual(rows(db, 'app_source_receipts'), []);
  const migrated = snapshot(db); assert.equal(migrateAppsSchema(db).migrated, false); assert.deepEqual(snapshot(db), migrated);
});

test('a v3 database with any noninitial target is refused atomically, even if target1 is active', t => {
  for (const active of [1, 2]) {
    const db = historical(t); extraTarget(db);
    if (active === 2) db.prepare("UPDATE app_publications SET launch_policy='restricted',listed=0,exposure_ack_revision=NULL,exposure_ack_json=NULL,active_target_revision=2 WHERE app_id=?").run(id);
    const before = snapshot(db);
    assert.throws(() => migrateAppsSchema(db), { code: 'apps_source_history_unsupported' });
    assert.deepEqual(snapshot(db), before); assert.equal(inspectAppsSchema(db), 'v3');
  }
});

test('source floor cannot decrease by UPDATE or REPLACE, change app identity or disappear', t => {
  const db = historical(t); migrateAppsSchema(db); db.prepare('UPDATE app_source_heads SET required_binding_version=2 WHERE app_id=?').run(id);
  for (const mutation of [
    () => db.prepare('UPDATE app_source_heads SET required_binding_version=1 WHERE app_id=?').run(id),
    () => db.prepare('INSERT OR REPLACE INTO app_source_heads VALUES (?,1)').run(id),
    () => db.prepare('UPDATE app_source_heads SET app_id=? WHERE app_id=?').run('missing', id),
    () => db.prepare('DELETE FROM app_source_heads WHERE app_id=?').run(id),
  ]) assert.throws(mutation);
  assert.equal(requiredBindingVersion(db, id), 2);
  assert.throws(() => db.prepare('INSERT OR REPLACE INTO app_runtime_targets SELECT * FROM app_runtime_targets WHERE app_id=?').run(id), /app_runtime_target_immutable/);
});

test('latest reopen and initial-registration helper never backfill a missing source head', t => {
  const db = historical(t); migrateAppsSchema(db);
  const trigger = db.prepare("SELECT sql FROM sqlite_schema WHERE name='app_source_head_no_delete'").get().sql;
  db.exec('DROP TRIGGER app_source_head_no_delete'); db.prepare('DELETE FROM app_source_heads WHERE app_id=?').run(id); db.exec(trigger);
  const before = snapshot(db);
  assert.throws(() => migrateAppsSchema(db), { code: 'apps_registry_corrupt' });
  db.exec('BEGIN IMMEDIATE');
  try { assert.throws(() => ensureInitialPublication(db, db.prepare('SELECT * FROM local_apps WHERE id=?').get(id)), { code: 'apps_registry_corrupt' }); }
  finally { db.exec('ROLLBACK'); }
  assert.deepEqual(snapshot(db), before);
});

test('any historical target above1 requires floor2, including rollback with target1 active', t => {
  const db = historical(t); migrateAppsSchema(db); extraTarget(db);
  const before = snapshot(db);
  assert.throws(() => requiredBindingVersion(db, id), { code: 'apps_registry_corrupt' });
  assert.throws(() => migrateAppsSchema(db), { code: 'apps_registry_corrupt' }); assert.deepEqual(snapshot(db), before);
  db.prepare('UPDATE app_source_heads SET required_binding_version=2 WHERE app_id=?').run(id);
  assert.equal(requiredBindingVersion(db, id), 2); assert.equal(migrateAppsSchema(db).migrated, false);
});

test('existing valid initial state is validated without issuing replacement writes', t => {
  const db = historical(t); migrateAppsSchema(db); const before = snapshot(db);
  db.exec('BEGIN IMMEDIATE'); ensureInitialPublication(db, db.prepare('SELECT * FROM local_apps WHERE id=?').get(id)); db.exec('COMMIT');
  assert.deepEqual(snapshot(db), before);
});

test('future marker and altered source guards fail before any migration write', t => {
  for (const mode of ['future', 'missing-guard']) {
    const db = historical(t); migrateAppsSchema(db);
    if (mode === 'future') db.exec("UPDATE apps_meta SET value='soty.apps-registry.v7' WHERE key='schema'; PRAGMA user_version=7");
    else db.exec('DROP TRIGGER app_source_head_no_replace_downgrade');
    const before = snapshot(db); assert.throws(() => migrateAppsSchema(db), { code: 'apps_schema_unsupported' }); assert.deepEqual(snapshot(db), before);
  }
});
