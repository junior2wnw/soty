// Test-only additive literal DDL from B1a 5e459abc6afa376861c2032226bd29f78bf0468d.
// Frozen v1 base is reused; never import a current domain migrator here.
import { createHistoricalCapabilitiesV1 } from './capabilities-v1.fixture.mjs';

export const nativeCapabilitiesV2DDL = `CREATE UNIQUE INDEX cap_invocations_native_identity
  ON cap_invocations(id,account_id);
CREATE INDEX cap_invocations_account_admission
  ON cap_invocations(account_id,created_at,id);
CREATE INDEX cap_invocations_principal_admission
  ON cap_invocations(account_id,principal_id,created_at,id);
CREATE INDEX cap_invocations_nonterminal
  ON cap_invocations(account_id,principal_id,created_at,id)
  WHERE status NOT IN ('succeeded','failed','cancelled');

CREATE TABLE cap_native_note_intents(
  invocation_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  notes_store_id TEXT NOT NULL
    CHECK(length(notes_store_id)=32 AND notes_store_id NOT GLOB '*[^0-9a-f]*'),
  note_id TEXT NOT NULL
    CHECK(length(note_id)=66 AND substr(note_id,1,2)='n_'
      AND substr(note_id,3) NOT GLOB '*[^0-9a-f]*'),
  mutation_id TEXT NOT NULL
    CHECK(length(mutation_id)=66 AND substr(mutation_id,1,2)='m_'
      AND substr(mutation_id,3) NOT GLOB '*[^0-9a-f]*'),
  input_digest TEXT NOT NULL
    CHECK(length(input_digest)=64 AND input_digest NOT GLOB '*[^0-9a-f]*'),
  input_bytes INTEGER NOT NULL CHECK(input_bytes BETWEEN 1 AND 262144),
  started_at INTEGER CHECK(started_at BETWEEN 0 AND 9007199254740991),
  input_purged_at INTEGER CHECK(input_purged_at BETWEEN 0 AND 9007199254740991),
  UNIQUE(account_id,note_id),
  UNIQUE(account_id,mutation_id),
  FOREIGN KEY(invocation_id,account_id) REFERENCES cap_invocations(id,account_id)
) STRICT;

CREATE TRIGGER cap_native_note_admission
BEFORE INSERT ON cap_native_note_intents
WHEN NOT EXISTS(SELECT 1 FROM cap_invocations i
  WHERE i.id=NEW.invocation_id AND i.account_id=NEW.account_id
    AND i.capability_id='notes.createDraft' AND i.capability_version=1
    AND i.capability_digest='95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204'
    AND i.job_id IS NULL AND i.input_json!='null')
BEGIN SELECT RAISE(ABORT,'native_note_binding_invalid'); END;
CREATE TRIGGER cap_native_note_no_replace
BEFORE INSERT ON cap_native_note_intents
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents
  WHERE invocation_id=NEW.invocation_id
     OR (account_id=NEW.account_id AND note_id=NEW.note_id)
     OR (account_id=NEW.account_id AND mutation_id=NEW.mutation_id))
BEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END;
CREATE TRIGGER cap_native_note_no_delete
BEFORE DELETE ON cap_native_note_intents
BEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END;
CREATE TRIGGER cap_native_note_update_guard
BEFORE UPDATE ON cap_native_note_intents
WHEN NEW.invocation_id IS NOT OLD.invocation_id OR NEW.account_id IS NOT OLD.account_id
  OR NEW.notes_store_id IS NOT OLD.notes_store_id OR NEW.note_id IS NOT OLD.note_id
  OR NEW.mutation_id IS NOT OLD.mutation_id OR NEW.input_digest IS NOT OLD.input_digest
  OR NEW.input_bytes IS NOT OLD.input_bytes
  OR (OLD.started_at IS NOT NULL AND NEW.started_at IS NOT OLD.started_at)
  OR (OLD.input_purged_at IS NOT NULL AND NEW.input_purged_at IS NOT OLD.input_purged_at)
  OR (NEW.input_purged_at IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM cap_invocations i JOIN cap_receipts r ON r.invocation_id=i.id
    WHERE i.id=NEW.invocation_id AND i.input_json='null'
      AND i.status IN ('succeeded','failed','cancelled')))
BEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END;

CREATE TRIGGER cap_native_note_input_guard
BEFORE UPDATE OF input_json ON cap_invocations
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.id)
 AND NEW.input_json IS NOT OLD.input_json
 AND (NEW.input_json!='null' OR NEW.status NOT IN ('succeeded','failed','cancelled')
      OR NOT EXISTS(SELECT 1 FROM cap_receipts r WHERE r.invocation_id=OLD.id))
BEGIN SELECT RAISE(ABORT,'native_note_input_immutable'); END;
CREATE TRIGGER cap_native_receipt_no_update
BEFORE UPDATE ON cap_receipts
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.invocation_id)
BEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END;
CREATE TRIGGER cap_native_receipt_no_delete
BEFORE DELETE ON cap_receipts
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.invocation_id)
BEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END;
CREATE TRIGGER cap_native_receipt_no_replace
BEFORE INSERT ON cap_receipts
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=NEW.invocation_id)
 AND EXISTS(SELECT 1 FROM cap_receipts r WHERE r.invocation_id=NEW.invocation_id)
BEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END;

CREATE TRIGGER cap_identity_no_update
BEFORE UPDATE ON cap_metadata
WHEN OLD.key IN ('project_id','registry_id') OR NEW.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END;
CREATE TRIGGER cap_identity_no_delete
BEFORE DELETE ON cap_metadata WHEN OLD.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END;
CREATE TRIGGER cap_identity_no_replace
BEFORE INSERT ON cap_metadata
WHEN NEW.key IN ('project_id','registry_id')
 AND EXISTS(SELECT 1 FROM cap_metadata WHERE key=NEW.key)
BEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END;`;

export function upgradeHistoricalCapabilitiesV2(db, { projectId = 'soty', registryId = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' } = {}) {
  db.exec('BEGIN IMMEDIATE');
  try {
    db.exec(nativeCapabilitiesV2DDL);
    const put = db.prepare('INSERT INTO cap_metadata(key,value) VALUES(?,?)');
    put.run('project_id', projectId);
    put.run('registry_id', registryId);
    db.prepare("UPDATE cap_metadata SET value=? WHERE key='lineage'").run('soty.capabilities.sqlite.v2');
    db.exec('PRAGMA user_version=2; COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

export function createHistoricalCapabilitiesV2(db, options = {}) {
  createHistoricalCapabilitiesV1(db);
  upgradeHistoricalCapabilitiesV2(db, options);
}
