// Test-only historical Apps v2 DDL frozen from b4b5200. Never derive this
// fixture from today's migration or relabel a newer database as version 2.
const coreV1 = `CREATE TABLE apps_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
  CREATE TABLE app_devices (connector_key TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,identity_json TEXT NOT NULL,name TEXT NOT NULL,created_at INTEGER NOT NULL);
  CREATE TABLE local_apps (id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL REFERENCES app_devices(connector_key),name TEXT NOT NULL,port INTEGER NOT NULL,entry_path TEXT NOT NULL,grants_json TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
  CREATE INDEX local_apps_owner ON local_apps(owner_account_id);
  CREATE TABLE local_app_grants (app_id TEXT NOT NULL REFERENCES local_apps(id) ON DELETE CASCADE,kind TEXT NOT NULL CHECK(kind IN ('account','community')),principal_id TEXT NOT NULL,PRIMARY KEY(app_id,kind,principal_id));
  CREATE INDEX local_app_grants_principal ON local_app_grants(kind,principal_id,app_id);`;
const domainsV2 = `CREATE TABLE app_domain_zones (
    id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('legacy','named')), origin_template TEXT NOT NULL UNIQUE,
    suffix TEXT NOT NULL, scheme TEXT NOT NULL CHECK(scheme IN ('https','http')), port TEXT NOT NULL, created_at INTEGER NOT NULL);
  CREATE TABLE app_domain_heads (app_id TEXT PRIMARY KEY REFERENCES local_apps(id),revision INTEGER NOT NULL CHECK(revision>=0));
  CREATE TABLE app_domains (
    id TEXT PRIMARY KEY,zone_id TEXT NOT NULL REFERENCES app_domain_zones(id),hostname TEXT NOT NULL UNIQUE,
    origin TEXT NOT NULL UNIQUE,slug TEXT,app_id TEXT NOT NULL REFERENCES local_apps(id),owner_account_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('canonical','alias')),state TEXT NOT NULL CHECK(state IN ('bound','tombstone')),
    created_at INTEGER NOT NULL,retired_at INTEGER,
    CHECK((role='canonical' AND slug IS NULL AND state='bound' AND retired_at IS NULL) OR
      (role='alias' AND slug IS NOT NULL AND ((state='bound' AND retired_at IS NULL) OR (state='tombstone' AND retired_at IS NOT NULL)))));
  CREATE UNIQUE INDEX app_domain_canonical ON app_domains(app_id) WHERE role='canonical';
  CREATE INDEX app_domain_app ON app_domains(app_id,created_at,id);
  CREATE INDEX app_domain_owner ON app_domains(owner_account_id,role);
  CREATE TABLE app_domain_receipts (
    account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('claim','retire')),domain_id TEXT NOT NULL REFERENCES app_domains(id),
    committed_revision INTEGER NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(account_id,request_key));`;

export function createHistoricalAppsV2(db) {
  db.exec(coreV1 + domainsV2);
  db.exec("INSERT INTO apps_meta VALUES ('schema','soty.apps-registry.v2'),('legacy_origin_template',''); PRAGMA user_version=2");
}
