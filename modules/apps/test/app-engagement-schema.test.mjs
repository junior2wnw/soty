import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHistoricalAppsV4, seedHistoricalRollbackV4 } from '../../../deploy/connector/apps-v4.fixture.mjs';
import { seedHistoricalPublicationV3 } from '../../../deploy/connector/apps-v3.fixture.mjs';
import { inspectAppsSchema, migrateAppsSchema, requiredBindingVersion } from '../server/schema.mjs';

const owner = 'owner_A', app = `app-${'a'.repeat(32)}`, second = `app-${'b'.repeat(32)}`, domain = `dom_${'d'.repeat(32)}`;
const key = 'link_A|host_A|connector_A';
const rows = (db, table) => db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
const snapshot = db => ({ version: db.prepare('PRAGMA user_version').get().user_version,
  definitions: db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all(),
  tables: Object.fromEntries(db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*'").all()
    .map(({ name }) => [name, rows(db, name)])) });
function historical(t) {
  const db = new DatabaseSync(':memory:'); db.exec('PRAGMA foreign_keys=ON'); t.after(() => db.close()); createHistoricalAppsV4(db);
  db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(key, owner,
    JSON.stringify({ linkId: 'link_A', hostDeviceId: 'host_A', connectorId: 'connector_A' }), 'Source', 1);
  for (const [id, state, port] of [[app, 'enabled', 8080], [second, 'revoked', 8090]]) {
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, owner, key, 'Historical label', port, '/#/legacy',
      '{"accountIds":["friend"],"communityIds":["group_A"]}', state, 7, 1, 5);
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'account', 'friend');
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'community', 'group_A');
    db.prepare('INSERT INTO app_domain_heads VALUES (?,4)').run(id); seedHistoricalPublicationV3(db, id);
  }
  seedHistoricalRollbackV4(db, app); db.prepare('INSERT INTO app_source_heads VALUES (?,1)').run(second);
  db.exec("INSERT INTO app_domain_zones VALUES ('named','named','https://{slug}.apps.example','apps.example','https','',1)");
  db.prepare("INSERT INTO app_domains VALUES (?,'named','kept.apps.example','https://kept.apps.example','kept',?,?,'alias','bound',1,NULL)").run(domain, app, owner);
  db.prepare("INSERT INTO app_domains VALUES (?,'named','retired.apps.example','https://retired.apps.example','retired',?,?,'alias','tombstone',1,2)")
    .run(`dom_${'e'.repeat(32)}`, app, owner);
  db.prepare("INSERT INTO app_domain_receipts VALUES (?,?,'intent','retire',?,4,2)").run(owner, 'old-domain', `dom_${'e'.repeat(32)}`);
  return db;
}
function savedRows(db) {
  db.prepare('INSERT INTO app_saved_heads VALUES (?,2)').run('reader_A');
  db.prepare('INSERT INTO app_saved_entries VALUES (?,?,?,?,?,?,?,?)').run('reader_A', app, domain, 'https://kept.apps.example', '/board#item', 'Old name', 1, 5);
  db.prepare('INSERT INTO app_saved_receipts VALUES (?,?,?,?,?,?,?)').run('reader_A', 'a'.repeat(64), 'b'.repeat(64), app, 1, 1, 5);
}

test('literal Apps4 migrates to saved-only Apps5 preserving all prior rows, immutable targets, floor2 and tombstones', t => {
  const db = historical(t), before = snapshot(db); assert.equal(inspectAppsSchema(db), 'v4');
  assert.equal(migrateAppsSchema(db).schema, 'soty.apps-registry.v5'); assert.equal(inspectAppsSchema(db), 'v5');
  for (const [table, value] of Object.entries(before.tables)) if (table !== 'apps_meta') assert.deepEqual(rows(db, table), value, table);
  assert.equal(requiredBindingVersion(db, app), 2);
  assert.equal(db.prepare('SELECT active_target_revision FROM app_publications WHERE app_id=?').get(app).active_target_revision, 1);
  const newTables = Object.keys(snapshot(db).tables).filter(name => !(name in before.tables)).sort();
  assert.deepEqual(newTables, ['app_saved_entries', 'app_saved_heads', 'app_saved_receipts']);
  for (const table of newTables) assert.deepEqual(rows(db, table), []);
  const after = snapshot(db); assert.equal(migrateAppsSchema(db).migrated, false); assert.deepEqual(snapshot(db), after);
});

test('valid saved state reopens without backfill or changes to historical snapshots', t => {
  const db = historical(t); migrateAppsSchema(db); savedRows(db); const before = snapshot(db);
  assert.equal(migrateAppsSchema(db).migrated, false); assert.deepEqual(snapshot(db), before);
});

test('account head guards prevent reset, nonincreasing revision, identity change, DELETE and REPLACE', t => {
  const db = historical(t); migrateAppsSchema(db); savedRows(db);
  for (const sql of ["UPDATE app_saved_heads SET revision=1 WHERE account_id='reader_A'", "UPDATE app_saved_heads SET revision=2 WHERE account_id='reader_A'",
    "UPDATE app_saved_heads SET account_id='reader_B' WHERE account_id='reader_A'", "DELETE FROM app_saved_heads WHERE account_id='reader_A'",
    "INSERT OR REPLACE INTO app_saved_heads VALUES ('reader_A',3)"]) assert.throws(() => db.exec(sql));
  db.exec("UPDATE app_saved_heads SET revision=3 WHERE account_id='reader_A'");
  assert.equal(db.prepare('SELECT revision FROM app_saved_heads').get().revision, 3);
});

test('missing saved head, cross-app address, noninteger data and out-of-head receipt fail closed without repair', t => {
  const cases = [
    db => { const sql = db.prepare("SELECT sql FROM sqlite_schema WHERE name='app_saved_head_no_delete'").get().sql;
      db.exec('PRAGMA foreign_keys=OFF; DROP TRIGGER app_saved_head_no_delete'); db.exec('DELETE FROM app_saved_heads'); db.exec(sql); db.exec('PRAGMA foreign_keys=ON'); },
    db => db.prepare('UPDATE app_saved_entries SET app_id=?').run(second),
    db => db.exec('UPDATE app_saved_entries SET saved_revision=1.5'),
    db => db.exec('UPDATE app_saved_receipts SET committed_revision=1.5'),
    db => db.exec('UPDATE app_saved_receipts SET committed_revision=3'),
  ];
  for (const mutate of cases) {
    const db = historical(t); migrateAppsSchema(db); savedRows(db); mutate(db); const before = snapshot(db);
    assert.throws(() => migrateAppsSchema(db), { code: 'apps_registry_corrupt' }); assert.deepEqual(snapshot(db), before);
  }
});

test('Apps6 and altered Apps5 guards are refused before persistent changes', t => {
  for (const sql of ["UPDATE apps_meta SET value='soty.apps-registry.v6' WHERE key='schema'; PRAGMA user_version=6",
    'DROP TRIGGER app_saved_head_no_replace', 'DROP INDEX app_saved_entry_revision']) {
    const db = historical(t); migrateAppsSchema(db); db.exec(sql); const before = snapshot(db);
    assert.throws(() => migrateAppsSchema(db), { code: 'apps_schema_unsupported' }); assert.deepEqual(snapshot(db), before);
  }
});

test('a failed Apps5 DDL step rolls back every new object and preserves exact Apps4 marker/rows', t => {
  const db = historical(t), before = snapshot(db), originalExec = db.exec;
  db.exec = function (sql) {
    if (sql.includes('CREATE TRIGGER app_saved_head_no_delete')) throw new Error('synthetic_ddl_failure');
    return originalExec.call(this, sql);
  };
  assert.throws(() => migrateAppsSchema(db), /synthetic_ddl_failure/u); db.exec = originalExec;
  assert.deepEqual(snapshot(db), before); assert.equal(inspectAppsSchema(db), 'v4');
  assert.equal(migrateAppsSchema(db).migrated, true); assert.equal(inspectAppsSchema(db), 'v5');
});
