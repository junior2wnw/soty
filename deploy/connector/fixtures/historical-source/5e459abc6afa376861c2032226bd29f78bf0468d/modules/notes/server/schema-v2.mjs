import { DatabaseSync } from 'node:sqlite';
import { randomBytes } from 'node:crypto';
import { NotesError, DEFAULT_LIMITS, document, hash, id } from './validation.mjs';

export const SCHEMA_VERSION = 2;
export const SUPPORTED_SCHEMA_VERSIONS = Object.freeze([1, 2]);
const LINEAGES = Object.freeze({ 1: 'soty.notes.sqlite.v1', 2: 'soty.notes.sqlite.v2' });

// The historical layout stays literal. New writes require an explicit migration admission.
const V1_DDL = `CREATE TABLE notes_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE note_accounts(account_id TEXT PRIMARY KEY, bytes INTEGER NOT NULL DEFAULT 0,
          identities INTEGER NOT NULL DEFAULT 0,active INTEGER NOT NULL DEFAULT 0,archived INTEGER NOT NULL DEFAULT 0,trashed INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE notes(rowid INTEGER PRIMARY KEY,account_id TEXT NOT NULL REFERENCES note_accounts(account_id),id TEXT NOT NULL,
          title TEXT NOT NULL,body TEXT NOT NULL,items TEXT NOT NULL,preview TEXT NOT NULL,color TEXT NOT NULL,pinned INTEGER NOT NULL,
          state TEXT NOT NULL CHECK(state IN('active','archived','trashed','deleted')),revision INTEGER NOT NULL,
          bytes INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,UNIQUE(account_id,id));
        CREATE INDEX notes_owner_order ON notes(account_id,state,pinned DESC,updated_at DESC,id ASC);
        CREATE VIRTUAL TABLE notes_fts USING fts5(scope,title,body,items,tokenize='unicode61 remove_diacritics 2',prefix='2 3');
        CREATE TABLE note_receipts(account_id TEXT NOT NULL,note_id TEXT NOT NULL,mutation_id TEXT NOT NULL,digest TEXT NOT NULL,
          result TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(account_id,note_id,mutation_id));
        CREATE INDEX note_receipts_trim ON note_receipts(account_id,note_id,revision DESC);`;
const V2_DDL = `CREATE TABLE note_native_creates(
  source_store_id TEXT NOT NULL
    CHECK(length(source_store_id)=32 AND source_store_id NOT GLOB '*[^0-9a-f]*'),
  invocation_id TEXT NOT NULL
    CHECK(length(invocation_id) BETWEEN 1 AND 160
      AND invocation_id NOT GLOB '*[^A-Za-z0-9_.:-]*'),
  account_id TEXT NOT NULL,
  note_id TEXT NOT NULL
    CHECK(length(note_id)=66 AND substr(note_id,1,2)='n_'
      AND substr(note_id,3) NOT GLOB '*[^0-9a-f]*'),
  mutation_id TEXT NOT NULL
    CHECK(length(mutation_id)=66 AND substr(mutation_id,1,2)='m_'
      AND substr(mutation_id,3) NOT GLOB '*[^0-9a-f]*'),
  input_digest TEXT NOT NULL
    CHECK(length(input_digest)=64 AND input_digest NOT GLOB '*[^0-9a-f]*'),
  capability_digest TEXT NOT NULL
    CHECK(capability_digest='95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204'),
  revision INTEGER NOT NULL CHECK(revision=1),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  PRIMARY KEY(source_store_id,invocation_id),
  UNIQUE(account_id,note_id),
  UNIQUE(account_id,mutation_id),
  FOREIGN KEY(account_id,note_id) REFERENCES notes(account_id,id)
) STRICT;

CREATE TRIGGER note_native_create_no_update
BEFORE UPDATE ON note_native_creates BEGIN
  SELECT RAISE(ABORT,'notes_native_proof_immutable');
END;
CREATE TRIGGER note_native_create_no_delete
BEFORE DELETE ON note_native_creates BEGIN
  SELECT RAISE(ABORT,'notes_native_proof_immutable');
END;
CREATE TRIGGER note_native_create_no_replace
BEFORE INSERT ON note_native_creates
WHEN EXISTS(SELECT 1 FROM note_native_creates
  WHERE (source_store_id=NEW.source_store_id AND invocation_id=NEW.invocation_id)
     OR (account_id=NEW.account_id AND note_id=NEW.note_id)
     OR (account_id=NEW.account_id AND mutation_id=NEW.mutation_id))
BEGIN SELECT RAISE(ABORT,'notes_native_proof_immutable'); END;

CREATE TRIGGER notes_identity_no_update
BEFORE UPDATE ON notes_meta
WHEN OLD.key IN ('project_id','registry_id') OR NEW.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'notes_identity_immutable'); END;
CREATE TRIGGER notes_identity_no_delete
BEFORE DELETE ON notes_meta WHEN OLD.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'notes_identity_immutable'); END;
CREATE TRIGGER notes_identity_no_replace
BEFORE INSERT ON notes_meta
WHEN NEW.key IN ('project_id','registry_id')
 AND EXISTS(SELECT 1 FROM notes_meta WHERE key=NEW.key)
BEGIN SELECT RAISE(ABORT,'notes_identity_immutable'); END;`;

const fail = code => { throw new NotesError(code); };
const assert = (condition, code = 'notes_storage_corrupt') => { if (!condition) fail(code); };
const safe = value => Number.isSafeInteger(value) && value >= 0;
function project(value) {
  assert(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(value), 'notes_project_id_required');
}
function migrationOption(options) {
  assert(options && typeof options === 'object' && !Array.isArray(options)
    && [null, Object.prototype].includes(Object.getPrototypeOf(options))
    && Object.keys(options).every(key => key === 'allowNativeMigration'), 'notes_invalid_arguments');
  const value = options.allowNativeMigration ?? false;
  assert(typeof value === 'boolean' && (options.allowNativeMigration !== null), 'notes_invalid_arguments');
  return value;
}
// Whitespace is insignificant outside SQL strings/quoted identifiers. Literal contents are preserved.
function sqlTokens(sql) {
  return (sql || '').match(/'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|[^\s'"`\[\]]+/gu)?.join(' ').replace(/;$/u, '') || '';
}
function layout(db) {
  return db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
    .map(row => [row.type, row.name, row.tbl_name, sqlTokens(row.sql)]);
}
const expectedLayouts = new Map();
function expectedLayout(version) {
  if (!expectedLayouts.has(version)) {
    const reference = new DatabaseSync(':memory:');
    try {
      reference.exec(V1_DDL);
      if (version === 2) reference.exec(V2_DDL);
      expectedLayouts.set(version, JSON.stringify(layout(reference)));
    } finally { reference.close(); }
  }
  return expectedLayouts.get(version);
}

/** Exact application format recognizer. It performs no persistent PRAGMA or DDL. */
export function inspectNotesSchema(db, projectId) {
  project(projectId);
  const version = Number(db.prepare('PRAGMA user_version').get().user_version);
  assert(version === 0 || SUPPORTED_SCHEMA_VERSIONS.includes(version), 'notes_schema_unsupported');
  const actual = layout(db);
  if (version === 0) {
    assert(actual.length === 0, 'notes_schema_metadata_mismatch');
    return Object.freeze({ schemaVersion: 0, registryId: null });
  }
  assert(JSON.stringify(actual) === expectedLayout(version), 'notes_schema_metadata_mismatch');
  const rows = db.prepare('SELECT key,value FROM notes_meta ORDER BY key').all();
  const metadata = Object.fromEntries(rows.map(row => [row.key, row.value]));
  assert(rows.length === (version === 1 ? 2 : 3)
    && Object.keys(metadata).length === rows.length, 'notes_schema_metadata_mismatch');
  assert(metadata.lineage === LINEAGES[version], 'notes_schema_metadata_mismatch');
  assert(metadata.project_id === projectId, 'notes_project_mismatch');
  if (version === 2) assert(typeof metadata.registry_id === 'string'
    && /^[a-f0-9]{32}$/u.test(metadata.registry_id), 'notes_schema_metadata_mismatch');
  return Object.freeze({ schemaVersion: version, registryId: metadata.registry_id ?? null });
}

function validateRows(db, version) {
  // Preserve the v1 integrity check. WAL mode is established before this check:
  // the DELETE-check -> WAL-switch sequence blocks an immediate checkpoint on
  // the tested SQLite runtime (see the independent diagnostic fixture).
  assert(db.prepare('PRAGMA quick_check').all().every(row => row.quick_check === 'ok'));
  assert(!db.prepare('PRAGMA foreign_key_check').get());
  // Iterate one body at a time; no whole-account text array is materialized.
  const fts = db.prepare('SELECT scope FROM notes_fts WHERE rowid=?');
  for (const row of db.prepare('SELECT * FROM notes ORDER BY rowid').iterate()) {
    try {
      id(row.account_id); id(row.id);
      assert(safe(row.rowid) && row.rowid > 0 && safe(row.revision) && row.revision > 0
        && safe(row.bytes) && safe(row.created_at) && safe(row.updated_at));
      assert(typeof row.preview === 'string' && row.preview.length <= 180);
      if (row.state === 'deleted') {
        assert(row.title === '' && row.body === '' && row.items === '[]' && row.preview === ''
          && row.color === 'plain' && row.pinned === 0 && row.bytes === 0);
        assert(!fts.get(row.rowid));
      } else {
        assert(row.pinned === 0 || row.pinned === 1);
        document({ title: row.title, body: row.body, items: JSON.parse(row.items), color: row.color,
          pinned: Boolean(row.pinned), state: row.state }, DEFAULT_LIMITS);
        // v1 counted its original JSON before UTF-8 binding. Do not rewrite historical accounting.
        assert(fts.get(row.rowid)?.scope === hash(row.account_id));
      }
    } catch { fail('notes_storage_corrupt'); }
  }
  assert(!db.prepare('SELECT 1 FROM notes_fts f LEFT JOIN notes n ON n.rowid=f.rowid WHERE n.rowid IS NULL LIMIT 1').get());
  const usage = db.prepare(`SELECT count(*) AS identities,coalesce(sum(bytes),0) AS bytes,
    coalesce(sum(state='active'),0) AS active,coalesce(sum(state='archived'),0) AS archived,
    coalesce(sum(state='trashed'),0) AS trashed FROM notes WHERE account_id=?`);
  for (const row of db.prepare('SELECT * FROM note_accounts ORDER BY account_id').iterate()) {
    try { id(row.account_id); } catch { fail('notes_storage_corrupt'); }
    const counted = usage.get(row.account_id);
    for (const key of ['identities', 'bytes', 'active', 'archived', 'trashed']) assert(safe(row[key]) && row[key] === counted[key]);
  }
  if (version !== 2) return;
  const note = db.prepare('SELECT revision,created_at FROM notes WHERE account_id=? AND id=?');
  for (const row of db.prepare('SELECT * FROM note_native_creates ORDER BY source_store_id,invocation_id').iterate()) {
    const suffix = hash(JSON.stringify(['soty.native-note.v1', row.source_store_id, row.invocation_id]));
    assert(row.note_id === `n_${suffix}` && row.mutation_id === `m_${suffix}`);
    const created = note.get(row.account_id, row.note_id);
    assert(created && created.revision >= row.revision && created.created_at === row.created_at);
  }
}

export function migrateNotes(db, projectId, options = {}) {
  project(projectId);
  const allowNativeMigration = migrationOption(options);
  assert(!db.isTransaction, 'notes_nested_transaction');
  // Reject unknown/partial formats before BEGIN or any persistent PRAGMA.
  inspectNotesSchema(db, projectId);
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL; PRAGMA journal_mode=WAL; PRAGMA secure_delete=ON;');
  db.exec('BEGIN IMMEDIATE');
  let result;
  try {
    let state = inspectNotesSchema(db, projectId);
    if (state.schemaVersion !== 0) validateRows(db, state.schemaVersion);
    if (state.schemaVersion === 0) {
      db.exec(V1_DDL);
      const insert = db.prepare('INSERT INTO notes_meta(key,value) VALUES(?,?)');
      insert.run('project_id', projectId); insert.run('lineage', LINEAGES[1]);
      db.exec('PRAGMA user_version=1');
      state = inspectNotesSchema(db, projectId);
    }
    if (state.schemaVersion === 1 && allowNativeMigration) {
      // Identity/lineage insertion precedes immutable metadata guards, in this same COMMIT.
      db.prepare('INSERT INTO notes_meta(key,value) VALUES(?,?)').run('registry_id', randomBytes(16).toString('hex'));
      db.prepare("UPDATE notes_meta SET value=? WHERE key='lineage'").run(LINEAGES[2]);
      db.exec(V2_DDL);
      db.exec('PRAGMA user_version=2');
    }
    result = inspectNotesSchema(db, projectId);
    // Existing rows were checked once before DDL; the new proof table is empty.
    db.exec('COMMIT');
  } catch (error) {
    if (db.isTransaction) db.exec('ROLLBACK');
    throw error;
  }
  return result;
}
