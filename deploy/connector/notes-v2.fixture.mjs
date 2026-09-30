// Test-only additive literal DDL from B1a 5e459abc6afa376861c2032226bd29f78bf0468d.
// Frozen v1 base is reused; never import a current domain migrator here.
import { createHistoricalNotesV1 } from './notes-v1.fixture.mjs';

export const nativeNotesV2DDL = `CREATE TABLE note_native_creates(
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

export function upgradeHistoricalNotesV2(db, { projectId = 'soty', registryId = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' } = {}) {
  db.exec('BEGIN IMMEDIATE');
  try {
    db.exec(nativeNotesV2DDL);
    const put = db.prepare('INSERT INTO notes_meta(key,value) VALUES(?,?)');
    if (db.prepare("SELECT value FROM notes_meta WHERE key='project_id'").get()?.value !== projectId) throw new Error('fixture_project_mismatch');
    put.run('registry_id', registryId);
    db.prepare("UPDATE notes_meta SET value=? WHERE key='lineage'").run('soty.notes.sqlite.v2');
    db.exec('PRAGMA user_version=2; COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

export function createHistoricalNotesV2(db, options = {}) {
  createHistoricalNotesV1(db, options.projectId ?? 'soty');
  upgradeHistoricalNotesV2(db, options);
}
