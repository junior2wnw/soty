// Test-only literal Notes1 DDL from 6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e.
// Do not derive this fixture from a current application migration.
export function createHistoricalNotesV1(db, projectId = 'soty') {
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
  insert.run('project_id', projectId);
  insert.run('lineage', 'soty.notes.sqlite.v1');
}
