import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { migrateAppsSchema, inspectAppsSchema } from '../server/schema.mjs';
import { createHistoricalAppsV3, seedHistoricalPublicationV3 } from '../../../deploy/connector/apps-v3.fixture.mjs';
import { readScopedApps7 } from '../../../deploy/apps/scoped-schema7-reader.mjs';
import { readScopedApps8, requireApps8BeforeStart } from '../../../deploy/apps/scoped-schema8-reader.mjs';
function source(t) {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close()); db.exec('PRAGMA foreign_keys=ON');
  createHistoricalAppsV3(db); const appId = 'app-' + 'a'.repeat(32), owner = 'owner', key = 'link|host|connector';
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(key, owner, '{"linkId":"link","hostDeviceId":"host","connectorId":"connector"}', 'Source', 1);
  db.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(appId, owner, key, 'Source', 5350, '/embed', '{"accountIds":[],"communityIds":[]}', 'enabled', 1, 1, 1);
  db.prepare('INSERT INTO app_domain_heads VALUES(?,1)').run(appId); seedHistoricalPublicationV3(db, appId);
  db.prepare('INSERT INTO app_publication_receipts VALUES(?,?,?,?,?,?,?)').run(owner, 'request', 'intent', appId, 2, '{"literal":"\\u0430","order":1}', 1);
  migrateAppsSchema(db, { allowScopedEmbedMigration: true }); return db;
}
test('explicit Apps7→8 preserves old tuple/receipt bytes and compatible fallback; literal7 refuses8 before START', t => {
  const db = source(t);
  const names = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*'").all().map(row => row.name);
  const before = new Map(names.map(name => [name, JSON.stringify(db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all())]));
  assert.equal(migrateAppsSchema(db).schema, 'soty.apps-registry.v7');
  assert.equal(migrateAppsSchema(db, { allowSelectedResourceMigration: true }).schema, 'soty.apps-registry.v8');
  assert.equal(inspectAppsSchema(db), 'v8');
  for (const [name, bytes] of before) if (name !== 'apps_meta') assert.equal(JSON.stringify(db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()), bytes);
  assert.equal(db.prepare('PRAGMA foreign_keys').get().foreign_keys, 1); assert.deepEqual(db.prepare('PRAGMA foreign_key_check').all(), []);
  assert.deepEqual(readScopedApps8(db), { schema: 'soty.apps-registry.v8', targets: 1, selected: 0 });
  assert.throws(() => readScopedApps7(db), { code: 'apps_reader7_refused' });
  assert.throws(() => requireApps8BeforeStart({ storedSchema: 'soty.apps-registry.v8', imageReaders: ['soty.apps-registry.v7'] }));
  assert.equal(requireApps8BeforeStart({ storedSchema: 'soty.apps-registry.v8', imageReaders: ['soty.apps-registry.v7', 'soty.apps-registry.v8'] }), true);
  assert.equal(migrateAppsSchema(db, { allowScopedEmbedMigration: false, allowSelectedResourceMigration: false }).migrated, false);
});
test('unknown7 DDL and dropped8 immutable guard are independently rejected without mutation', t => {
  const db = source(t); db.exec('CREATE TABLE unrelated(value TEXT)');
  const before = JSON.stringify(db.prepare('SELECT name,sql FROM sqlite_schema ORDER BY name').all());
  assert.throws(() => migrateAppsSchema(db, { allowSelectedResourceMigration: true }), { code: 'apps_schema_unsupported' });
  assert.equal(JSON.stringify(db.prepare('SELECT name,sql FROM sqlite_schema ORDER BY name').all()), before);
  assert.equal(db.prepare('PRAGMA foreign_keys').get().foreign_keys, 1);
  db.exec('DROP TABLE unrelated'); migrateAppsSchema(db, { allowSelectedResourceMigration: true });
  db.exec('DROP TRIGGER app_scoped_admission_no_update');
  assert.throws(() => readScopedApps8(db), { code: 'apps_reader8_refused' });
  assert.throws(() => inspectAppsSchema(db), { code: 'apps_schema_unsupported' });
});
test('metadata-only fake8 cannot pass an independent exact-layout reader', t => {
  const db = source(t); db.exec("UPDATE apps_meta SET value='soty.apps-registry.v8' WHERE key='schema';PRAGMA user_version=8");
  assert.throws(() => readScopedApps8(db), { code: 'apps_reader8_refused' });
  assert.throws(() => inspectAppsSchema(db), { code: 'apps_schema_unsupported' });
});
