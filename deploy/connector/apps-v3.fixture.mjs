// Test-only historical Apps v3 DDL frozen from
// 914a91a07584c60bafa372763d873f1923d9987f. Never derive it from the current
// migration or relabel a newer database as version 3.
import { createHash } from 'node:crypto';
import { createHistoricalAppsV2 } from './apps-v2.fixture.mjs';

const publicationsV3 = `CREATE UNIQUE INDEX local_apps_identity_owner ON local_apps(id,owner_account_id);
  CREATE UNIQUE INDEX app_devices_identity_owner ON app_devices(connector_key,owner_account_id);
  CREATE UNIQUE INDEX app_domains_identity_owner ON app_domains(id,app_id,owner_account_id);
  CREATE TABLE app_runtime_targets (
    app_id TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
    owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL,port INTEGER NOT NULL CHECK(port BETWEEN 1024 AND 65535),
    entry_path TEXT NOT NULL,profile TEXT NOT NULL CHECK(profile='soty.relay-restricted.v1'),digest TEXT NOT NULL,
    created_at INTEGER NOT NULL,PRIMARY KEY(app_id,revision),
    FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id),
    FOREIGN KEY(connector_key,owner_account_id) REFERENCES app_devices(connector_key,owner_account_id));
  CREATE TABLE app_publications (
    app_id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,
    launch_policy TEXT NOT NULL CHECK(launch_policy IN ('restricted','anyone')),
    listed INTEGER NOT NULL CHECK(listed IN (0,1)),policy_epoch INTEGER NOT NULL CHECK(policy_epoch BETWEEN 1 AND 9007199254740991),
    active_target_revision INTEGER NOT NULL,exposure_ack_revision INTEGER,exposure_ack_json TEXT,updated_at INTEGER NOT NULL,
    FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id),
    FOREIGN KEY(app_id,active_target_revision) REFERENCES app_runtime_targets(app_id,revision),
    FOREIGN KEY(app_id,exposure_ack_revision) REFERENCES app_runtime_targets(app_id,revision),
    CHECK(listed=0 OR launch_policy='anyone'),
    CHECK((exposure_ack_revision IS NULL AND exposure_ack_json IS NULL) OR (exposure_ack_revision IS NOT NULL AND exposure_ack_json IS NOT NULL)),
    CHECK(launch_policy='restricted' OR (exposure_ack_revision IS NOT NULL AND exposure_ack_revision=active_target_revision AND exposure_ack_json IS NOT NULL)));
  CREATE TABLE app_publication_domains (
    app_id TEXT NOT NULL,domain_id TEXT NOT NULL,owner_account_id TEXT NOT NULL,PRIMARY KEY(app_id,domain_id),
    FOREIGN KEY(app_id) REFERENCES app_publications(app_id),
    FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id),
    FOREIGN KEY(domain_id,app_id,owner_account_id) REFERENCES app_domains(id,app_id,owner_account_id));
  CREATE TABLE app_publication_receipts (
    account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,app_id TEXT NOT NULL,
    committed_epoch INTEGER NOT NULL CHECK(committed_epoch BETWEEN 2 AND 9007199254740991),value_json TEXT NOT NULL,created_at INTEGER NOT NULL,
    PRIMARY KEY(account_id,request_key),FOREIGN KEY(app_id,account_id) REFERENCES local_apps(id,owner_account_id));
  CREATE UNIQUE INDEX app_publication_receipt_epoch ON app_publication_receipts(app_id,committed_epoch);`;

export function createHistoricalAppsV3(db) {
  createHistoricalAppsV2(db);
  db.exec(publicationsV3);
  db.exec("CREATE TRIGGER app_runtime_target_no_update BEFORE UPDATE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END");
  db.exec("CREATE TRIGGER app_runtime_target_no_delete BEFORE DELETE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END");
  db.exec("UPDATE apps_meta SET value='soty.apps-registry.v3' WHERE key='schema'; PRAGMA user_version=3");
}

// Only creates the original v3 target/publication for a synthetic app that the
// caller has already inserted. All fields and the digest format are historical.
export function seedHistoricalPublicationV3(db, id) {
  const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  if (!app) throw new Error('historical_app_missing');
  const profile = 'soty.relay-restricted.v1';
  const digest = createHash('sha256').update(JSON.stringify(['soty.runtime-target.v1', app.id, 1,
    app.owner_account_id, app.connector_key, app.port, app.entry_path, profile])).digest('hex');
  db.prepare('INSERT INTO app_runtime_targets VALUES (?,?,?,?,?,?,?,?,?)').run(app.id, 1, app.owner_account_id,
    app.connector_key, app.port, app.entry_path, profile, digest, app.created_at);
  db.prepare("INSERT INTO app_publications VALUES (?,?,'restricted',0,1,1,NULL,NULL,?)")
    .run(app.id, app.owner_account_id, app.updated_at);
}
