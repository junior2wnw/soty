import { createHash } from 'node:crypto';
import { createFieldDocument, validateFieldDocument } from '../../field/contract.mjs';
import { assert, exact, identifier, integer, WorldError } from './validation.mjs';

export const FIELD_OPERATIONS = Object.freeze(['world.field.get', 'world.field.put']);
const hash = value => createHash('sha256').update(value).digest('hex');
const documentHash = document => hash(JSON.stringify(document));
const fieldDefinitions = Object.freeze({
  world_field_documents: `CREATE TABLE world_field_documents (
    account_id TEXT PRIMARY KEY REFERENCES profiles(account_id), revision INTEGER NOT NULL CHECK(revision>=1),
    document_json TEXT NOT NULL CHECK(json_valid(document_json)), content_hash TEXT NOT NULL CHECK(length(content_hash)=64),
    updated_at INTEGER NOT NULL CHECK(updated_at>=0))`,
  world_field_receipts: `CREATE TABLE world_field_receipts (
    account_id TEXT NOT NULL REFERENCES profiles(account_id), request_key TEXT NOT NULL CHECK(length(request_key)=64),
    intent_hash TEXT NOT NULL CHECK(length(intent_hash)=64), content_hash TEXT NOT NULL CHECK(length(content_hash)=64),
    accepted_revision INTEGER NOT NULL CHECK(accepted_revision>=1), committed_at INTEGER NOT NULL CHECK(committed_at>=0),
    PRIMARY KEY(account_id,request_key))`,
  world_field_document_revision: `CREATE TRIGGER world_field_document_revision BEFORE UPDATE ON world_field_documents
    WHEN NEW.account_id<>OLD.account_id OR NEW.revision<>OLD.revision+1
    BEGIN SELECT RAISE(ABORT,'field_revision_invalid'); END`,
  world_field_document_no_delete: `CREATE TRIGGER world_field_document_no_delete BEFORE DELETE ON world_field_documents
    BEGIN SELECT RAISE(ABORT,'field_document_required'); END`,
  world_field_document_no_replace: `CREATE TRIGGER world_field_document_no_replace BEFORE INSERT ON world_field_documents
    WHEN EXISTS(SELECT 1 FROM world_field_documents WHERE account_id=NEW.account_id)
    BEGIN SELECT RAISE(ABORT,'field_document_immutable'); END`,
  world_field_receipt_no_update: `CREATE TRIGGER world_field_receipt_no_update BEFORE UPDATE ON world_field_receipts
    BEGIN SELECT RAISE(ABORT,'field_receipt_immutable'); END`,
  world_field_receipt_no_delete: `CREATE TRIGGER world_field_receipt_no_delete BEFORE DELETE ON world_field_receipts
    BEGIN SELECT RAISE(ABORT,'field_receipt_immutable'); END`,
  world_field_receipt_no_replace: `CREATE TRIGGER world_field_receipt_no_replace BEFORE INSERT ON world_field_receipts
    WHEN EXISTS(SELECT 1 FROM world_field_receipts WHERE account_id=NEW.account_id AND request_key=NEW.request_key)
    BEGIN SELECT RAISE(ABORT,'field_receipt_immutable'); END`,
});
const normalizedSql = sql => sql.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase();

/** Additive extension epoch. World schema3 readers preserve these extra objects. */
export function migrateField(db) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const version = db.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get()?.value;
    const objects = db.prepare("SELECT name,sql FROM sqlite_schema WHERE name LIKE 'world_field_%' AND type IN('table','trigger')").all();
    if (version === undefined) {
      assert(objects.length === 0, 'field_storage_metadata_mismatch');
      for (const sql of Object.values(fieldDefinitions)) db.exec(sql);
      db.prepare("INSERT INTO world_meta VALUES ('field_schema_version','1')").run();
    } else {
      assert(version === '1', 'field_schema_unsupported');
      assert(objects.length === Object.keys(fieldDefinitions).length, 'field_storage_schema_mismatch');
      for (const [name, sql] of Object.entries(fieldDefinitions)) {
        const actual = objects.find(item => item.name === name);
        assert(actual && normalizedSql(actual.sql) === normalizedSql(sql), 'field_storage_schema_mismatch');
      }
    }
    // Existing records are validated without rewriting or resetting anything.
    for (const row of db.prepare('SELECT * FROM world_field_documents').iterate()) readDocument(row);
    assert(!db.prepare(`SELECT 1 FROM world_field_receipts r LEFT JOIN world_field_documents d ON d.account_id=r.account_id
      WHERE d.account_id IS NULL OR r.accepted_revision>d.revision LIMIT 1`).get(), 'field_storage_corrupt');
    db.exec('COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

function readDocument(row) {
  if (!row) {
    const document = createFieldDocument();
    return { revision: 0, document, contentHash: documentHash(document), updatedAt: 0 };
  }
  let document;
  try { document = validateFieldDocument(JSON.parse(row.document_json)); }
  catch { throw new WorldError('field_storage_corrupt'); }
  assert(Number.isSafeInteger(row.revision) && row.revision >= 1 && Number.isSafeInteger(row.updated_at) && row.updated_at >= 0
    && JSON.stringify(document) === row.document_json && documentHash(document) === row.content_hash, 'field_storage_corrupt');
  return { revision: row.revision, document, contentHash: row.content_hash, updatedAt: row.updated_at };
}
function receipt(row) { return { revision: row.accepted_revision, contentHash: row.content_hash, committedAt: row.committed_at }; }

/** Called within World's already authenticated transaction; references confer no authority. */
export function fieldOperation(m, op, args, actor, now) {
  exact(args, ['expectedAccountId'], op === 'world.field.get' ? [] : ['expectedRevision', 'requestId', 'document', 'contentHash']);
  identifier(args.expectedAccountId);
  assert(args.expectedAccountId === actor.accountId, 'field_account_changed');
  const current = () => readDocument(m.get('SELECT * FROM world_field_documents WHERE account_id=?', actor.accountId));
  if (op === 'world.field.get') return current();
  assert(Object.hasOwn(args, 'expectedRevision') && Object.hasOwn(args, 'requestId') && Object.hasOwn(args, 'document'), 'invalid_arguments');
  const expectedRevision = integer(args.expectedRevision), requestKey = hash(identifier(args.requestId));
  const document = validateFieldDocument(args.document), contentHash = documentHash(document);
  assert(args.contentHash === undefined || args.contentHash === contentHash, 'field_content_hash_mismatch');
  const intentHash = hash(JSON.stringify(['soty.field.put.v1', expectedRevision, contentHash]));
  const previous = m.get('SELECT * FROM world_field_receipts WHERE account_id=? AND request_key=?', actor.accountId, requestKey);
  if (previous) {
    assert(previous.intent_hash === intentHash, 'field_request_conflict');
    return { replayed: true, receipt: receipt(previous), current: current() };
  }
  const before = current();
  assert(before.revision === expectedRevision, 'field_revision_conflict');
  assert(before.revision < Number.MAX_SAFE_INTEGER, 'field_revision_exhausted');
  const revision = before.revision + 1, serialized = JSON.stringify(document);
  if (before.revision === 0) m.run('INSERT INTO world_field_documents VALUES (?,?,?,?,?)', actor.accountId, revision, serialized, contentHash, now);
  else m.run('UPDATE world_field_documents SET revision=?,document_json=?,content_hash=?,updated_at=? WHERE account_id=?', revision, serialized, contentHash, now, actor.accountId);
  m.run('INSERT INTO world_field_receipts VALUES (?,?,?,?,?,?)', actor.accountId, requestKey, intentHash, contentHash, revision, now);
  return { replayed: false, receipt: { revision, contentHash, committedAt: now }, current: current() };
}
