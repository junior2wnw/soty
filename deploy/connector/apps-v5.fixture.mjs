// Test-only historical Apps5 DDL frozen from
// 3794f01febe3c01e2d3d107be1f03dbf4b023b3c. Discussion is not part of this
// format. Do not derive this fixture from the current application migration.
import { createHistoricalAppsV4 } from './apps-v4.fixture.mjs';

const savedV5 = `CREATE TABLE app_saved_heads (
    account_id TEXT PRIMARY KEY NOT NULL,revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991));
  CREATE TABLE app_saved_entries (
    account_id TEXT NOT NULL REFERENCES app_saved_heads(account_id),app_id TEXT NOT NULL REFERENCES local_apps(id),
    domain_id TEXT NOT NULL REFERENCES app_domains(id),origin TEXT NOT NULL CHECK(length(origin) BETWEEN 1 AND 512),
    path TEXT NOT NULL CHECK(length(path) BETWEEN 1 AND 8192),label TEXT NOT NULL CHECK(length(label) BETWEEN 1 AND 64),
    saved_revision INTEGER NOT NULL CHECK(saved_revision BETWEEN 1 AND 9007199254740991),
    updated_at INTEGER NOT NULL CHECK(updated_at BETWEEN 0 AND 9007199254740991),PRIMARY KEY(account_id,app_id));
  CREATE UNIQUE INDEX app_saved_entry_revision ON app_saved_entries(account_id,saved_revision);
  CREATE TABLE app_saved_receipts (
    account_id TEXT NOT NULL REFERENCES app_saved_heads(account_id),request_key TEXT NOT NULL CHECK(length(request_key)=64),
    intent_hash TEXT NOT NULL CHECK(length(intent_hash)=64),app_id TEXT NOT NULL,saved INTEGER NOT NULL CHECK(saved IN (0,1)),
    committed_revision INTEGER NOT NULL CHECK(committed_revision BETWEEN 1 AND 9007199254740991),
    created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),PRIMARY KEY(account_id,request_key));
  CREATE UNIQUE INDEX app_saved_receipt_revision ON app_saved_receipts(account_id,committed_revision);`;

const savedGuardsV5 = [
  "CREATE TRIGGER app_saved_head_no_downgrade BEFORE UPDATE ON app_saved_heads WHEN NEW.account_id<>OLD.account_id OR NEW.revision<=OLD.revision BEGIN SELECT RAISE(ABORT,'app_saved_revision_not_increasing'); END",
  "CREATE TRIGGER app_saved_head_no_delete BEFORE DELETE ON app_saved_heads BEGIN SELECT RAISE(ABORT,'app_saved_head_required'); END",
  "CREATE TRIGGER app_saved_head_no_replace BEFORE INSERT ON app_saved_heads WHEN EXISTS (SELECT 1 FROM app_saved_heads WHERE account_id=NEW.account_id) BEGIN SELECT RAISE(ABORT,'app_saved_head_immutable'); END",
];

export function createHistoricalAppsV5(db) {
  createHistoricalAppsV4(db);
  db.exec(savedV5);
  for (const sql of savedGuardsV5) db.exec(sql);
  db.exec("UPDATE apps_meta SET value='soty.apps-registry.v5' WHERE key='schema'; PRAGMA user_version=5");
}
