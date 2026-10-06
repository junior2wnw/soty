import { DatabaseSync } from 'node:sqlite';

export const FEEDBACK_LINEAGE = 'soty.feedback.sqlite.v1';
export const FEEDBACK_SCHEMA_VERSION = 1;
export const FEEDBACK_DDL = `
CREATE TABLE feedback_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT;
CREATE TABLE feedback_installations(
 id TEXT PRIMARY KEY,provisioning_key TEXT NOT NULL UNIQUE,registry_id TEXT NOT NULL,tenant_id TEXT NOT NULL,
 app_id TEXT NOT NULL,environment_id TEXT NOT NULL,created_at INTEGER NOT NULL
) STRICT;
CREATE TABLE feedback_provider_receipts(
 receipt_key TEXT PRIMARY KEY,installation_id TEXT NOT NULL REFERENCES feedback_installations(id),
 proof_json TEXT NOT NULL,created_at INTEGER NOT NULL
) STRICT;
CREATE TABLE feedback_tickets(
 id TEXT PRIMARY KEY,installation_id TEXT NOT NULL REFERENCES feedback_installations(id),app_id TEXT NOT NULL,
 reporter_id TEXT NOT NULL,owner_id TEXT NOT NULL,body TEXT NOT NULL,status TEXT NOT NULL,
 revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE feedback_attachments(
 id TEXT PRIMARY KEY,ticket_id TEXT NOT NULL REFERENCES feedback_tickets(id),ordinal INTEGER NOT NULL,
 kind TEXT NOT NULL,name TEXT NOT NULL,mime_type TEXT NOT NULL,byte_length INTEGER NOT NULL,bytes BLOB NOT NULL,
 UNIQUE(ticket_id,ordinal)
) STRICT;
CREATE TABLE feedback_messages(
 id TEXT PRIMARY KEY,ticket_id TEXT NOT NULL REFERENCES feedback_tickets(id),ordinal INTEGER NOT NULL,
 actor_id TEXT NOT NULL,kind TEXT NOT NULL,body TEXT NOT NULL,created_at INTEGER NOT NULL,
 UNIQUE(ticket_id,ordinal)
) STRICT;
CREATE TABLE feedback_receipts(
 installation_id TEXT NOT NULL REFERENCES feedback_installations(id),account_id TEXT NOT NULL,
 request_key TEXT NOT NULL,intent_digest TEXT NOT NULL,result_json TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_id,request_key)
) STRICT;
CREATE INDEX feedback_ticket_page ON feedback_tickets(installation_id,created_at DESC,id DESC);
CREATE INDEX feedback_reporter_page ON feedback_tickets(installation_id,reporter_id,created_at DESC,id DESC);
PRAGMA user_version=1;
`;

const immutable = (table, sameKey) => `
CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER ${table}_no_replace BEFORE INSERT ON ${table}
WHEN EXISTS(SELECT 1 FROM ${table} WHERE ${sameKey})
BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;`;
export const FEEDBACK_GUARDS = immutable('feedback_meta', 'key=NEW.key')
  + immutable('feedback_installations', 'id=NEW.id OR provisioning_key=NEW.provisioning_key')
  + immutable('feedback_provider_receipts', 'receipt_key=NEW.receipt_key')
  + immutable('feedback_receipts', 'installation_id=NEW.installation_id AND account_id=NEW.account_id AND request_key=NEW.request_key');

export function initializeFeedbackSchema(db, registryId, environmentId) {
  const version = db.prepare('PRAGMA user_version').get().user_version;
  const fail = () => { throw Object.assign(new Error('feedback_storage_unknown'), { code: 'feedback_storage_unknown' }); };
  if (version === 0) {
    if (db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").get().n !== 0) fail();
    db.exec('BEGIN IMMEDIATE');
    try {
      db.exec(FEEDBACK_DDL + FEEDBACK_GUARDS);
      const insert = db.prepare('INSERT INTO feedback_meta VALUES (?,?)');
      for (const [key, value] of Object.entries({ lineage: FEEDBACK_LINEAGE, registry_id: registryId, environment_id: environmentId })) insert.run(key, value);
      db.exec('COMMIT');
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  } else if (version !== 1) fail();
  const meta = Object.fromEntries(db.prepare('SELECT key,value FROM feedback_meta').all().map(row => [row.key, row.value]));
  if (Object.keys(meta).length !== 3 || meta.lineage !== FEEDBACK_LINEAGE || meta.registry_id !== registryId || meta.environment_id !== environmentId) fail();
  const layout = database => database.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").all()
    .map(row => [row.type, row.name, row.tbl_name, String(row.sql).replace(/\s+/gu, ' ').trim()]);
  const reference = new DatabaseSync(':memory:');
  try {
    reference.exec(FEEDBACK_DDL + FEEDBACK_GUARDS);
    if (JSON.stringify(layout(db)) !== JSON.stringify(layout(reference))) fail();
  } finally { reference.close(); }
}
