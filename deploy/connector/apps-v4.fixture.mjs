// Test-only historical Apps4 DDL frozen from
// 4fa5f722403dfbb1dc9fdf67965071a398c3537c. This literal fixture does not import
// the current application migration or relabel a newer database as version 4.
import { createHash } from 'node:crypto';
import { createHistoricalAppsV3 } from './apps-v3.fixture.mjs';

const sourcesV4 = `CREATE TABLE app_source_heads (
    app_id TEXT PRIMARY KEY REFERENCES local_apps(id),required_binding_version INTEGER NOT NULL CHECK(required_binding_version IN (1,2)));
  CREATE TABLE app_source_receipts (
    account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,app_id TEXT NOT NULL,
    committed_epoch INTEGER NOT NULL CHECK(committed_epoch BETWEEN 2 AND 9007199254740991),value_json TEXT NOT NULL,created_at INTEGER NOT NULL,
    PRIMARY KEY(account_id,request_key),FOREIGN KEY(app_id,account_id) REFERENCES local_apps(id,owner_account_id));
  CREATE UNIQUE INDEX app_source_receipt_epoch ON app_source_receipts(app_id,committed_epoch);`;

const guardsV4 = [
  "CREATE TRIGGER app_source_head_no_downgrade BEFORE UPDATE ON app_source_heads WHEN NEW.app_id<>OLD.app_id OR NEW.required_binding_version<OLD.required_binding_version BEGIN SELECT RAISE(ABORT,'app_source_binding_downgrade'); END",
  "CREATE TRIGGER app_source_head_no_delete BEFORE DELETE ON app_source_heads BEGIN SELECT RAISE(ABORT,'app_source_head_required'); END",
  "CREATE TRIGGER app_source_head_no_replace_downgrade BEFORE INSERT ON app_source_heads WHEN EXISTS (SELECT 1 FROM app_source_heads WHERE app_id=NEW.app_id AND required_binding_version>NEW.required_binding_version) BEGIN SELECT RAISE(ABORT,'app_source_binding_downgrade'); END",
  "CREATE TRIGGER app_runtime_target_no_replace BEFORE INSERT ON app_runtime_targets WHEN EXISTS (SELECT 1 FROM app_runtime_targets WHERE app_id=NEW.app_id AND revision=NEW.revision) BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END",
];

export function createHistoricalAppsV4(db) {
  createHistoricalAppsV3(db);
  db.exec(sourcesV4);
  for (const sql of guardsV4) db.exec(sql);
  db.exec("UPDATE apps_meta SET value='soty.apps-registry.v4' WHERE key='schema'; PRAGMA user_version=4");
}

// The caller first seeds an original target/publication. Record a synthetic
// source switch followed by rollback: target1 is active, but floor2 is sticky.
export function seedHistoricalRollbackV4(db, id) {
  const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  const policy = db.prepare('SELECT * FROM app_publications WHERE app_id=?').get(id);
  if (!app || !policy || policy.active_target_revision !== 1) throw new Error('historical_publication_missing');
  const profile = 'soty.relay-restricted.v1', port = app.port + 1, entryPath = '/board?tag=a%2Bb#item';
  const digest = createHash('sha256').update(JSON.stringify(['soty.runtime-target.v1', id, 2,
    app.owner_account_id, app.connector_key, port, entryPath, profile])).digest('hex');
  db.prepare('INSERT INTO app_runtime_targets VALUES (?,?,?,?,?,?,?,?,?)').run(id, 2, app.owner_account_id,
    app.connector_key, port, entryPath, profile, digest, 5);
  db.prepare('INSERT INTO app_source_heads VALUES (?,2)').run(id);
  db.prepare('UPDATE app_publications SET policy_epoch=6,updated_at=6 WHERE app_id=?').run(id);
  for (const epoch of [5, 6]) db.prepare('INSERT INTO app_source_receipts VALUES (?,?,?,?,?,?,?)')
    .run(app.owner_account_id, `historical-source-${epoch}`, `historical-intent-${epoch}`, id,
      epoch, JSON.stringify({ synthetic: 'historical-source', activeTargetRevision: epoch === 5 ? 2 : 1 }), epoch);
}
