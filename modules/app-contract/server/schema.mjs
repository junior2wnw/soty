import { DatabaseSync } from 'node:sqlite';
import { requireRegistration, literalId } from './authority.mjs';

export const REGISTRATION_SCHEMA_VERSION = 1;
export const REGISTRATION_LINEAGE = 'soty.app-registration.v1';
export const REGISTRATION_DDL = `
CREATE TABLE registration_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT;
CREATE TABLE registration_authorities(
 scope_key TEXT PRIMARY KEY CHECK(length(scope_key)=64),scope_json TEXT NOT NULL CHECK(json_valid(scope_json)),
 owner_id TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 1000000),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64),authority_digest TEXT NOT NULL CHECK(length(authority_digest)=64),
 updated_at INTEGER NOT NULL CHECK(updated_at BETWEEN 0 AND 9007199254740991)
) STRICT;
CREATE TABLE registration_heads(
 scope_key TEXT PRIMARY KEY REFERENCES registration_authorities(scope_key),app_id TEXT NOT NULL,owner_id TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 1000000),generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 1000000),
 descriptor_digest TEXT NOT NULL CHECK(length(descriptor_digest)=64),authority_digest TEXT NOT NULL CHECK(length(authority_digest)=64),
 authority_revision INTEGER NOT NULL CHECK(authority_revision BETWEEN 1 AND 1000000),intent_digest TEXT NOT NULL CHECK(length(intent_digest)=64),
 request_id TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('pending-feedback','ready')),
 feedback_json TEXT CHECK(feedback_json IS NULL OR json_valid(feedback_json)),updated_at INTEGER NOT NULL CHECK(updated_at BETWEEN 0 AND 9007199254740991),
 CHECK((status='pending-feedback' AND feedback_json IS NULL) OR (status='ready' AND feedback_json IS NOT NULL))
) STRICT;
CREATE TABLE registration_versions(
 scope_key TEXT NOT NULL REFERENCES registration_heads(scope_key),generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 1000000),
 committed_revision INTEGER NOT NULL CHECK(committed_revision BETWEEN 1 AND 1000000),
 descriptor_digest TEXT NOT NULL CHECK(length(descriptor_digest)=64),descriptor_json TEXT NOT NULL CHECK(json_valid(descriptor_json) AND length(CAST(descriptor_json AS BLOB))<=65536),
 authority_digest TEXT NOT NULL CHECK(length(authority_digest)=64),authority_revision INTEGER NOT NULL CHECK(authority_revision BETWEEN 1 AND 1000000),
 plan_json TEXT NOT NULL CHECK(json_valid(plan_json)),created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
 PRIMARY KEY(scope_key,generation)
) STRICT;
CREATE TABLE registration_receipts(
 account_id TEXT NOT NULL,request_id TEXT NOT NULL,intent_hash TEXT NOT NULL CHECK(length(intent_hash)=64),
 scope_key TEXT NOT NULL,generation INTEGER NOT NULL,receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json)),
 created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
 PRIMARY KEY(account_id,request_id),FOREIGN KEY(scope_key,generation) REFERENCES registration_versions(scope_key,generation)
) STRICT;
CREATE TABLE registration_reference_history(
 kind TEXT NOT NULL,ref_id TEXT NOT NULL,ref_version INTEGER NOT NULL CHECK(ref_version BETWEEN 1 AND 1000000),
 content_digest TEXT NOT NULL CHECK(length(content_digest)=64),content_json TEXT NOT NULL CHECK(json_valid(content_json)),
 PRIMARY KEY(kind,ref_id,ref_version)
) STRICT;
CREATE TABLE registration_feedback_outbox(
 scope_key TEXT NOT NULL,generation INTEGER NOT NULL,provisioning_key TEXT NOT NULL CHECK(length(provisioning_key)=64),
 request_json TEXT NOT NULL CHECK(json_valid(request_json)),created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
 PRIMARY KEY(scope_key,generation),FOREIGN KEY(scope_key,generation) REFERENCES registration_versions(scope_key,generation)
) STRICT;
CREATE INDEX registration_heads_owner ON registration_heads(owner_id,app_id);
CREATE INDEX registration_receipts_account ON registration_receipts(account_id,created_at);
`;
const immutable = table => `
CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER ${table}_no_replace BEFORE INSERT ON ${table}
WHEN EXISTS(SELECT 1 FROM ${table} WHERE ${table === 'registration_metadata' ? 'key=NEW.key'
  : table === 'registration_receipts' ? 'account_id=NEW.account_id AND request_id=NEW.request_id'
    : table === 'registration_reference_history' ? 'kind=NEW.kind AND ref_id=NEW.ref_id AND ref_version=NEW.ref_version'
      : 'scope_key=NEW.scope_key AND generation=NEW.generation'})
BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;`;
export const REGISTRATION_GUARDS = ['registration_metadata', 'registration_versions', 'registration_receipts',
  'registration_reference_history', 'registration_feedback_outbox'].map(immutable).join('\n') + `
CREATE TRIGGER registration_authorities_no_delete BEFORE DELETE ON registration_authorities BEGIN SELECT RAISE(ABORT,'registration_authority_required'); END;
CREATE TRIGGER registration_authorities_no_replace BEFORE INSERT ON registration_authorities
WHEN EXISTS(SELECT 1 FROM registration_authorities WHERE scope_key=NEW.scope_key) BEGIN SELECT RAISE(ABORT,'registration_authority_required'); END;
CREATE TRIGGER registration_authorities_monotonic BEFORE UPDATE ON registration_authorities
WHEN NEW.scope_key<>OLD.scope_key OR NEW.scope_json<>OLD.scope_json OR NEW.owner_id<>OLD.owner_id
 OR NEW.generation<>OLD.generation+1 OR NEW.fingerprint=OLD.fingerprint
BEGIN SELECT RAISE(ABORT,'registration_authority_required'); END;
CREATE TRIGGER registration_heads_no_delete BEFORE DELETE ON registration_heads BEGIN SELECT RAISE(ABORT,'registration_head_required'); END;
CREATE TRIGGER registration_heads_no_replace BEFORE INSERT ON registration_heads
WHEN EXISTS(SELECT 1 FROM registration_heads WHERE scope_key=NEW.scope_key) BEGIN SELECT RAISE(ABORT,'registration_head_required'); END;
CREATE TRIGGER registration_heads_monotonic BEFORE UPDATE ON registration_heads
WHEN NEW.scope_key<>OLD.scope_key OR NEW.app_id<>OLD.app_id OR NEW.owner_id<>OLD.owner_id OR NEW.revision<>OLD.revision+1
 OR NEW.generation NOT IN (OLD.generation,OLD.generation+1)
 OR (NEW.generation=OLD.generation AND (NEW.descriptor_digest<>OLD.descriptor_digest OR NEW.authority_digest<>OLD.authority_digest
   OR NEW.authority_revision<>OLD.authority_revision OR NEW.intent_digest<>OLD.intent_digest OR NEW.request_id<>OLD.request_id
   OR OLD.status='ready' OR NEW.status<>'ready'))
 OR (NEW.generation=OLD.generation+1 AND NEW.status<>'pending-feedback')
BEGIN SELECT RAISE(ABORT,'registration_head_required'); END;
`;

const tokens = sql => (sql || '').match(/'(?:''|[^'])*'|"(?:""|[^"])*"|[^\s'";]+|;/gu)?.filter(v => v !== ';').join(' ') || '';
const layout = db => db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => [row.type, row.name, row.tbl_name, tokens(row.sql)]);
let expectedLayout;
function expected() {
  if (!expectedLayout) {
    const db = new DatabaseSync(':memory:');
    try { db.exec(REGISTRATION_DDL + REGISTRATION_GUARDS); expectedLayout = JSON.stringify(layout(db)); }
    finally { db.close(); }
  }
  return expectedLayout;
}
/** Independent probes must freeze this format separately; candidate code cannot attest its own reader. */
export function inspectRegistrationSchema(db, { registryId, environmentId } = {}) {
  const version = Number(db.prepare('PRAGMA user_version').get().user_version);
  const objects = layout(db);
  if (version === 0 && objects.length === 0) return { schemaVersion: 0 };
  requireRegistration(version === REGISTRATION_SCHEMA_VERSION, 'registration_schema_unsupported', 503);
  requireRegistration(JSON.stringify(objects) === expected(), 'registration_schema_invalid', 503);
  const metadata = db.prepare('SELECT key,value FROM registration_metadata ORDER BY key').all();
  const values = Object.fromEntries(metadata.map(row => [row.key, row.value]));
  requireRegistration(metadata.length === 3 && values.lineage === REGISTRATION_LINEAGE, 'registration_schema_invalid', 503);
  if (registryId !== undefined) requireRegistration(values.registry_id === registryId, 'registration_registry_mismatch', 503);
  if (environmentId !== undefined) requireRegistration(values.environment_id === environmentId, 'registration_environment_mismatch', 503);
  literalId(values.registry_id); literalId(values.environment_id);
  requireRegistration(!db.prepare('PRAGMA foreign_key_check').get(), 'registration_storage_corrupt', 503);
  return Object.freeze({ schemaVersion: version, registryId: values.registry_id, environmentId: values.environment_id });
}
export function initializeRegistrationSchema(db, { registryId, environmentId }) {
  literalId(registryId); literalId(environmentId);
  requireRegistration(!db.isTransaction, 'registration_nested_transaction', 500);
  db.exec('BEGIN IMMEDIATE');
  try {
    const before = inspectRegistrationSchema(db, { registryId, environmentId });
    if (before.schemaVersion === 0) {
      db.exec(REGISTRATION_DDL + REGISTRATION_GUARDS);
      const insert = db.prepare('INSERT INTO registration_metadata(key,value) VALUES(?,?)');
      for (const [key, value] of [['lineage', REGISTRATION_LINEAGE], ['registry_id', registryId], ['environment_id', environmentId]]) insert.run(key, value);
      db.exec('PRAGMA user_version=1');
    }
    const result = inspectRegistrationSchema(db, { registryId, environmentId });
    db.exec('COMMIT'); return result;
  } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
}
