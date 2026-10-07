import { DatabaseSync } from 'node:sqlite';
import { check, digest } from './validation.mjs';

export const SCHEMA = 'soty.personal-memory.sqlite.v1';
export const SQL = `
  CREATE TABLE memory_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
  CREATE TABLE memory_records(
    id TEXT PRIMARY KEY, revision INTEGER NOT NULL CHECK(revision>0),
    kind TEXT NOT NULL, text TEXT NOT NULL, source TEXT NOT NULL,
    importance REAL NOT NULL, confidence REAL NOT NULL,
    fresh_until INTEGER NOT NULL, retain_until INTEGER NOT NULL,
    created_at INTEGER NOT NULL, content_bytes INTEGER NOT NULL,
    embedding BLOB
  ) STRICT;
  CREATE INDEX memory_retention ON memory_records(retain_until,id);
  CREATE VIRTUAL TABLE memory_search USING fts5(id UNINDEXED,text,tokenize='unicode61');
  CREATE TABLE memory_tombstones(
    id TEXT PRIMARY KEY, revision INTEGER NOT NULL, erased_at INTEGER NOT NULL,
    restore_floor INTEGER NOT NULL, mutation_id TEXT NOT NULL
  ) STRICT;
  CREATE TABLE memory_receipts(
    mutation_id TEXT PRIMARY KEY, intent_digest TEXT NOT NULL,
    result_json TEXT NOT NULL, committed_at INTEGER NOT NULL
  ) STRICT;
  PRAGMA user_version=1;
`;
function vector(db) {
  return db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").all();
}
let expected;
export function assertSchema(db) {
  if (!expected) {
    const reference = new DatabaseSync(':memory:');
    try { reference.exec(SQL); expected = digest(vector(reference)); } finally { reference.close(); }
  }
  check(db.prepare('PRAGMA user_version').get().user_version === 1
    && digest(vector(db)) === expected, 'memory_schema_unsupported');
}
export function isEmptyDatabase(db) {
  return db.prepare("SELECT count(*) AS count FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").get().count === 0
    && db.prepare('PRAGMA user_version').get().user_version === 0;
}
