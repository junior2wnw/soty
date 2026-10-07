import { DatabaseSync } from 'node:sqlite';
import { existsSync } from 'node:fs';
import { ApiError } from './validation.ts';

export const PLANNER_RP_FORMAT = 'planner.source-rp.v2';
export const PLANNER_RP_READER = 2;
const fields: Record<string, string[]> = {
  planner_soty_rp_format: ['id', 'format', 'version'],
  planner_soty_rp_anchors: [
    'session_hash',
    'locator_hash',
    'profile_digest',
    'binding_digest',
    'payload_cipher',
    'key_id',
    'expires_at',
    'created_at',
    'generation',
    'active',
  ],
  planner_soty_rp_heads: ['session_hash', 'document'],
  planner_soty_rebind_receipts: [
    'request_hash',
    'intent_digest',
    'session_hash',
    'continuation_digest',
    'payload_cipher',
    'key_id',
    'expires_at',
    'created_at',
  ],
};
// Reader2 recognizes the exact additive namespace, including uniqueness,
// foreign-key cascade and closed marker constraints; column names alone do
// not establish a compatible durable CAS store.
const definitions: Record<string, string> = {
  planner_soty_rp_format:
    "CREATE TABLE planner_soty_rp_format(id INTEGER PRIMARY KEY CHECK(id=1),format TEXT NOT NULL CHECK(format='planner.source-rp.v2'),version INTEGER NOT NULL CHECK(version=2))",
  planner_soty_rp_anchors:
    'CREATE TABLE planner_soty_rp_anchors(session_hash TEXT PRIMARY KEY,locator_hash TEXT NOT NULL,profile_digest TEXT NOT NULL,binding_digest TEXT NOT NULL,payload_cipher TEXT NOT NULL,key_id TEXT NOT NULL,expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),active INTEGER NOT NULL CHECK(active IN(0,1)))',
  planner_soty_rp_heads:
    'CREATE TABLE planner_soty_rp_heads(session_hash TEXT PRIMARY KEY REFERENCES planner_soty_rp_anchors(session_hash) ON DELETE CASCADE,document TEXT NOT NULL CHECK(json_valid(document)))',
  planner_soty_rebind_receipts:
    'CREATE TABLE planner_soty_rebind_receipts(request_hash TEXT PRIMARY KEY,intent_digest TEXT NOT NULL,session_hash TEXT NOT NULL REFERENCES planner_soty_rp_anchors(session_hash) ON DELETE CASCADE,continuation_digest TEXT NOT NULL,payload_cipher TEXT NOT NULL,key_id TEXT NOT NULL,expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL)',
};
const sqlBytes = (sql: string) =>
  sql
    .trim()
    .replace(/;$/u, '')
    .replace(/'(?:''|[^'])*'|"(?:""|[^"])*"|\s+/gu, (part) => (/^\s+$/u.test(part) ? '' : part));
function need(value: unknown, code: string): asserts value {
  if (!value) throw new ApiError(503, 'Формат входа требует совместимой версии приложения', code);
}
export function inspectPlannerRpFormat(db: DatabaseSync, reader = PLANNER_RP_READER) {
  const objects = db
    .prepare(
      "SELECT type,name,sql FROM sqlite_master WHERE name LIKE 'planner_soty_rp_%' OR name='planner_soty_rebind_receipts' ORDER BY name",
    )
    .all() as { type: string; name: string; sql: string }[];
  if (!objects.length) return 1;
  need(
    objects.length === 4 &&
      objects.every((row) => row.type === 'table' && Object.hasOwn(fields, row.name)),
    'planner_rp_storage_unknown',
  );
  need(
    objects.every((row) => sqlBytes(row.sql) === sqlBytes(definitions[row.name])),
    'planner_rp_storage_unknown',
  );
  const marker = db.prepare('SELECT * FROM planner_soty_rp_format').all() as {
    id: number;
    format: string;
    version: number;
  }[];
  need(
    marker.length === 1 &&
      marker[0].id === 1 &&
      marker[0].format === PLANNER_RP_FORMAT &&
      marker[0].version === 2,
    'planner_rp_storage_unknown',
  );
  need(reader >= 2, 'planner_rp_reader_incompatible');
  for (const [name, expected] of Object.entries(fields)) {
    const actual = db.prepare('PRAGMA table_info(' + name + ')').all() as { name: string }[];
    need(
      actual.map((row) => row.name).join(',') === expected.join(','),
      'planner_rp_storage_unknown',
    );
  }
  need(db.prepare('PRAGMA foreign_key_check').all().length === 0, 'planner_rp_foreign_key_invalid');
  return 2;
}
/** Run before PlannerStore opens/migrates native tables. No request-time DDL. */
export function assertPlannerRpStartup(path: string, reader = PLANNER_RP_READER) {
  if (path === ':memory:' || !existsSync(path)) return 1;
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    return inspectPlannerRpFormat(db, reader);
  } finally {
    db.close();
  }
}
export function installPlannerRpFormat(db: DatabaseSync, allowMigration: boolean) {
  if (inspectPlannerRpFormat(db) === 2) return;
  need(allowMigration === true, 'planner_rp_migration_required');
  db.exec('BEGIN IMMEDIATE');
  try {
    db.exec(`CREATE TABLE planner_soty_rp_format(id INTEGER PRIMARY KEY CHECK(id=1),format TEXT NOT NULL CHECK(format='planner.source-rp.v2'),version INTEGER NOT NULL CHECK(version=2));
      CREATE TABLE planner_soty_rp_anchors(session_hash TEXT PRIMARY KEY,locator_hash TEXT NOT NULL,profile_digest TEXT NOT NULL,binding_digest TEXT NOT NULL,payload_cipher TEXT NOT NULL,key_id TEXT NOT NULL,expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),active INTEGER NOT NULL CHECK(active IN(0,1)));
      CREATE TABLE planner_soty_rp_heads(session_hash TEXT PRIMARY KEY REFERENCES planner_soty_rp_anchors(session_hash) ON DELETE CASCADE,document TEXT NOT NULL CHECK(json_valid(document)));
      CREATE TABLE planner_soty_rebind_receipts(request_hash TEXT PRIMARY KEY,intent_digest TEXT NOT NULL,session_hash TEXT NOT NULL REFERENCES planner_soty_rp_anchors(session_hash) ON DELETE CASCADE,continuation_digest TEXT NOT NULL,payload_cipher TEXT NOT NULL,key_id TEXT NOT NULL,expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL);
      INSERT INTO planner_soty_rp_format VALUES(1,'planner.source-rp.v2',2);`);
    inspectPlannerRpFormat(db);
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}
