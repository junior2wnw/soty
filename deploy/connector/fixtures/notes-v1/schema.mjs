export const SCHEMA_VERSION = 1;
const LINEAGE = 'soty.notes.sqlite.v1';
export function migrateNotes(db, projectId) {
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA secure_delete=ON; BEGIN IMMEDIATE');
  try {
    const version = Number(db.prepare('PRAGMA user_version').get().user_version);
    if (version === 0) {
      if (db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1").get()) throw new Error('notes_schema_metadata_mismatch');
      db.exec(`
        CREATE TABLE notes_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
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
        CREATE INDEX note_receipts_trim ON note_receipts(account_id,note_id,revision DESC);
        PRAGMA user_version=1;
      `);
      const insert = db.prepare('INSERT INTO notes_meta(key,value) VALUES(?,?)');
      insert.run('project_id', projectId); insert.run('lineage', LINEAGE);
    } else if (version !== SCHEMA_VERSION) throw new Error('notes_schema_unsupported');
    const meta = Object.fromEntries(db.prepare('SELECT key,value FROM notes_meta').all().map(row => [row.key, row.value]));
    if (meta.project_id !== projectId || meta.lineage !== LINEAGE) throw new Error('notes_project_mismatch');
    if (db.prepare('PRAGMA quick_check').all().some(row => row.quick_check !== 'ok')) throw new Error('notes_storage_corrupt');
    db.exec('COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}
