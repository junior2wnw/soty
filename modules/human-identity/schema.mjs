import { DatabaseSync } from 'node:sqlite';
import { HUMAN_IDENTITY_PROFILE, requireHuman } from './profile.mjs';

export const HUMAN_IDENTITY_SCHEMA_VERSION = 1;
export const HUMAN_IDENTITY_LINEAGE = 'soty.human-identity.sqlite.v1';
export const HUMAN_IDENTITY_DDL = `
CREATE TABLE human_identity_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT;
CREATE TABLE human_identity_artifacts(
 model TEXT NOT NULL CHECK(model IN ('Session','Interaction','Grant','AuthorizationCode','AccessToken')),
 id_hash TEXT NOT NULL CHECK(length(id_hash)=64),payload_cipher BLOB NOT NULL CHECK(length(payload_cipher) BETWEEN 30 AND 16412),
 payload_digest TEXT NOT NULL CHECK(length(payload_digest)=64),key_id TEXT NOT NULL,
 account_id TEXT,device_id TEXT,client_id TEXT,grant_hash TEXT,uid_hash TEXT,browser_hash TEXT,
 expires_at INTEGER NOT NULL,consumed_at INTEGER,created_at INTEGER NOT NULL,
 PRIMARY KEY(model,id_hash),CHECK((account_id IS NULL AND device_id IS NULL) OR (account_id IS NOT NULL AND device_id IS NOT NULL))
) STRICT;
CREATE INDEX human_identity_artifact_uid ON human_identity_artifacts(model,uid_hash);
CREATE INDEX human_identity_artifact_grant ON human_identity_artifacts(grant_hash);
CREATE INDEX human_identity_artifact_expiry ON human_identity_artifacts(expires_at);
CREATE INDEX human_identity_artifact_pending ON human_identity_artifacts(model,client_id,browser_hash,expires_at);
CREATE TABLE human_identity_interactions(
 uid_hash TEXT PRIMARY KEY,browser_hash TEXT NOT NULL,csrf_hash TEXT NOT NULL,client_id TEXT NOT NULL,profile_digest TEXT NOT NULL,
 client_generation INTEGER NOT NULL CHECK(client_generation>=1),
 params_digest TEXT NOT NULL,params_cipher BLOB NOT NULL,key_id TEXT NOT NULL,expires_at INTEGER NOT NULL,
 decision TEXT NOT NULL CHECK(decision IN ('pending','approved','denied')),account_id TEXT,device_id TEXT,approved_at INTEGER,
 CHECK((decision='pending' AND account_id IS NULL AND device_id IS NULL AND approved_at IS NULL)
  OR (decision!='pending' AND account_id IS NOT NULL AND device_id IS NOT NULL AND approved_at IS NOT NULL))
) STRICT;
CREATE INDEX human_identity_interaction_expiry ON human_identity_interactions(decision,expires_at);
CREATE TABLE human_identity_decisions(
 account_id TEXT NOT NULL,request_id TEXT NOT NULL,intent_hash TEXT NOT NULL,uid_hash TEXT NOT NULL,
 result_json TEXT NOT NULL CHECK(json_valid(result_json)),created_at INTEGER NOT NULL,PRIMARY KEY(account_id,request_id)
) STRICT;
CREATE INDEX human_identity_decision_uid ON human_identity_decisions(uid_hash);
CREATE TABLE human_identity_grant_bindings(
 grant_hash TEXT PRIMARY KEY,account_id TEXT NOT NULL,device_id TEXT NOT NULL,client_id TEXT NOT NULL,
 interaction_hash TEXT NOT NULL UNIQUE REFERENCES human_identity_interactions(uid_hash),profile_digest TEXT NOT NULL,
 client_generation INTEGER NOT NULL CHECK(client_generation>=1),
 created_at INTEGER NOT NULL,revoked_at INTEGER
) STRICT;
CREATE TABLE human_identity_client_versions(
 client_id TEXT NOT NULL,version INTEGER NOT NULL CHECK(version BETWEEN 1 AND 1000000),
 profile_digest TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(client_id,version)
) STRICT;
CREATE TABLE human_identity_client_heads(
 client_id TEXT PRIMARY KEY,version INTEGER NOT NULL CHECK(version BETWEEN 1 AND 1000000),profile_digest TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 1000000),active INTEGER NOT NULL CHECK(active IN (0,1)),updated_at INTEGER NOT NULL
) STRICT;
`;
function immutable(table, key) {
  return `CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER ${table}_no_replace BEFORE INSERT ON ${table} WHEN EXISTS(SELECT 1 FROM ${table} WHERE ${key})
BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;`;
}
const gcEligible = alias => `human_identity_gc_epoch()>0 AND ${alias}.expires_at<=human_identity_gc_epoch()
 AND (${alias}.decision='pending' OR ${alias}.expires_at<=human_identity_gc_epoch()-3660)
 AND NOT EXISTS(SELECT 1 FROM human_identity_artifacts a WHERE a.expires_at>human_identity_gc_epoch()
  AND (a.id_hash=${alias}.uid_hash AND a.model='Interaction' OR a.grant_hash IN
   (SELECT grant_hash FROM human_identity_grant_bindings WHERE interaction_hash=${alias}.uid_hash)))`;
export const HUMAN_IDENTITY_GUARDS = immutable('human_identity_meta', 'key=NEW.key')
  + `CREATE TRIGGER human_identity_decisions_no_update BEFORE UPDATE ON human_identity_decisions BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER human_identity_decisions_no_replace BEFORE INSERT ON human_identity_decisions
WHEN EXISTS(SELECT 1 FROM human_identity_decisions WHERE account_id=NEW.account_id AND request_id=NEW.request_id)
BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER human_identity_decisions_no_delete BEFORE DELETE ON human_identity_decisions
WHEN NOT EXISTS(SELECT 1 FROM human_identity_interactions i WHERE i.uid_hash=OLD.uid_hash AND ${gcEligible('i')})
BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;`
  + immutable('human_identity_client_versions', 'client_id=NEW.client_id AND version=NEW.version') + `
CREATE TRIGGER human_identity_interaction_pin BEFORE UPDATE ON human_identity_interactions
WHEN NEW.uid_hash<>OLD.uid_hash OR NEW.browser_hash<>OLD.browser_hash OR NEW.csrf_hash<>OLD.csrf_hash OR NEW.client_id<>OLD.client_id
 OR NEW.profile_digest<>OLD.profile_digest OR NEW.client_generation<>OLD.client_generation OR NEW.params_digest<>OLD.params_digest OR NEW.params_cipher<>OLD.params_cipher
 OR NEW.key_id<>OLD.key_id OR NEW.expires_at<>OLD.expires_at OR OLD.decision!='pending'
BEGIN SELECT RAISE(ABORT,'human_identity_interaction_immutable'); END;
CREATE TRIGGER human_identity_interaction_no_replace BEFORE INSERT ON human_identity_interactions
WHEN EXISTS(SELECT 1 FROM human_identity_interactions WHERE uid_hash=NEW.uid_hash)
BEGIN SELECT RAISE(ABORT,'human_identity_interaction_immutable'); END;
CREATE TRIGGER human_identity_interaction_no_delete BEFORE DELETE ON human_identity_interactions
WHEN NOT (${gcEligible('OLD')})
BEGIN SELECT RAISE(ABORT,'human_identity_interaction_immutable'); END;
CREATE TRIGGER human_identity_grant_pin BEFORE UPDATE ON human_identity_grant_bindings
WHEN NEW.grant_hash<>OLD.grant_hash OR NEW.account_id<>OLD.account_id OR NEW.device_id<>OLD.device_id OR NEW.client_id<>OLD.client_id
 OR NEW.interaction_hash<>OLD.interaction_hash OR NEW.profile_digest<>OLD.profile_digest OR NEW.client_generation<>OLD.client_generation OR NEW.created_at<>OLD.created_at
 OR OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL
BEGIN SELECT RAISE(ABORT,'human_identity_grant_immutable'); END;
CREATE TRIGGER human_identity_grant_no_delete BEFORE DELETE ON human_identity_grant_bindings
WHEN NOT EXISTS(SELECT 1 FROM human_identity_interactions i WHERE i.uid_hash=OLD.interaction_hash AND ${gcEligible('i')})
BEGIN SELECT RAISE(ABORT,'human_identity_grant_immutable'); END;
CREATE TRIGGER human_identity_grant_no_replace BEFORE INSERT ON human_identity_grant_bindings
WHEN EXISTS(SELECT 1 FROM human_identity_grant_bindings WHERE grant_hash=NEW.grant_hash)
BEGIN SELECT RAISE(ABORT,'human_identity_grant_immutable'); END;
CREATE TRIGGER human_identity_client_head_no_delete BEFORE DELETE ON human_identity_client_heads
BEGIN SELECT RAISE(ABORT,'human_identity_client_required'); END;
CREATE TRIGGER human_identity_client_head_no_replace BEFORE INSERT ON human_identity_client_heads
WHEN EXISTS(SELECT 1 FROM human_identity_client_heads WHERE client_id=NEW.client_id)
BEGIN SELECT RAISE(ABORT,'human_identity_client_required'); END;
CREATE TRIGGER human_identity_client_head_monotonic BEFORE UPDATE ON human_identity_client_heads
WHEN NEW.client_id<>OLD.client_id OR NEW.version<OLD.version OR NEW.generation<>OLD.generation+1
BEGIN SELECT RAISE(ABORT,'human_identity_client_required'); END;
`;
const normalized = sql => sql.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const layout = db => db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => [row.type, row.name, row.tbl_name, normalized(row.sql)]);
let expected;
function reference() {
  if (!expected) { const db = new DatabaseSync(':memory:'); try { db.exec(HUMAN_IDENTITY_DDL + HUMAN_IDENTITY_GUARDS); expected = JSON.stringify(layout(db)); } finally { db.close(); } }
  return expected;
}
export function initializeHumanIdentitySchema(db, profile) {
  requireHuman(profile?.enabled === true, 'human_identity_disabled', 503);
  db.exec('BEGIN IMMEDIATE');
  try {
    const version = Number(db.prepare('PRAGMA user_version').get().user_version);
    if (version === 0) {
      requireHuman(layout(db).length === 0, 'human_identity_storage_unknown', 503);
      db.exec(HUMAN_IDENTITY_DDL + HUMAN_IDENTITY_GUARDS);
      const insert = db.prepare('INSERT INTO human_identity_meta(key,value) VALUES(?,?)');
      for (const [key, value] of Object.entries({ lineage: HUMAN_IDENTITY_LINEAGE, registry_id: profile.registryId,
        environment_id: profile.environmentId, issuer: profile.issuer, profile: HUMAN_IDENTITY_PROFILE })) insert.run(key, value);
      db.exec('PRAGMA user_version=1');
    } else requireHuman(version === 1, 'human_identity_storage_unknown', 503);
    requireHuman(JSON.stringify(layout(db)) === reference(), 'human_identity_storage_unknown', 503);
    const rows = db.prepare('SELECT key,value FROM human_identity_meta ORDER BY key').all(), values = Object.fromEntries(rows.map(row => [row.key, row.value]));
    requireHuman(rows.length === 5 && values.lineage === HUMAN_IDENTITY_LINEAGE && values.registry_id === profile.registryId
      && values.environment_id === profile.environmentId && values.issuer === profile.issuer && values.profile === HUMAN_IDENTITY_PROFILE,
    'human_identity_storage_identity_mismatch', 503);
    requireHuman(!db.prepare('PRAGMA foreign_key_check').get(), 'human_identity_storage_corrupt', 503);
    db.exec('COMMIT'); return Object.freeze({ schemaVersion: 1 });
  } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
}
