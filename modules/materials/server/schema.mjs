import { randomBytes } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { materialCheck } from '../contract.mjs';
export const MATERIAL_SCHEMA_VERSION = 2;
const DDL = `CREATE TABLE material_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT;
CREATE TABLE materials(owner_id TEXT NOT NULL,id TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN('image','audio','video')),
 name TEXT NOT NULL,media_type TEXT NOT NULL,size INTEGER NOT NULL CHECK(size>0),file_sha256 TEXT NOT NULL,
 begin_digest TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('pending','active','trashed','cancelled','expired')),
 revision INTEGER NOT NULL CHECK(revision>=0),created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,
 manifest TEXT,manifest_sha256 TEXT,width INTEGER,height INTEGER,duration_ms INTEGER,
 PRIMARY KEY(owner_id,id)) STRICT;
CREATE INDEX material_owner_order ON materials(owner_id,state,updated_at DESC,id ASC);
CREATE TABLE material_chunks(owner_id TEXT NOT NULL,material_id TEXT NOT NULL,part_index INTEGER NOT NULL,
 size INTEGER NOT NULL,sha256 TEXT NOT NULL,bytes BLOB NOT NULL,PRIMARY KEY(owner_id,material_id,part_index),
 FOREIGN KEY(owner_id,material_id) REFERENCES materials(owner_id,id) ON DELETE CASCADE) STRICT;
CREATE TABLE material_receipts(sequence INTEGER PRIMARY KEY,owner_id TEXT NOT NULL,material_id TEXT NOT NULL,
 mutation_id TEXT NOT NULL,digest TEXT NOT NULL,result TEXT NOT NULL,UNIQUE(owner_id,material_id,mutation_id),
 FOREIGN KEY(owner_id,material_id) REFERENCES materials(owner_id,id)) STRICT;
CREATE INDEX material_receipt_trim ON material_receipts(owner_id,material_id,sequence DESC);
CREATE TRIGGER material_identity_no_update BEFORE UPDATE ON material_meta
 WHEN OLD.key IN ('project_id','registry_id','lineage') OR NEW.key IN ('project_id','registry_id','lineage')
 BEGIN SELECT RAISE(ABORT,'materials_identity_immutable'); END;
CREATE TRIGGER material_identity_no_delete BEFORE DELETE ON material_meta
 BEGIN SELECT RAISE(ABORT,'materials_identity_immutable'); END;
CREATE TRIGGER material_identity_no_replace BEFORE INSERT ON material_meta
 WHEN EXISTS(SELECT 1 FROM material_meta WHERE key=NEW.key)
 BEGIN SELECT RAISE(ABORT,'materials_identity_immutable'); END;`;
export const MATERIAL_VAULT_DDL=`CREATE TABLE material_vault_seals(owner_id TEXT NOT NULL,material_id TEXT NOT NULL,claim_id TEXT NOT NULL UNIQUE,vault_id TEXT NOT NULL,cipher_digest TEXT NOT NULL,source_revision INTEGER NOT NULL,sealed_revision INTEGER NOT NULL,source_store_id TEXT NOT NULL,erasure_state TEXT NOT NULL CHECK(erasure_state IN('pending','confirmed')),sealed_at INTEGER NOT NULL,confirmed_at INTEGER,PRIMARY KEY(owner_id,material_id),FOREIGN KEY(owner_id,material_id) REFERENCES materials(owner_id,id)) STRICT;
CREATE TRIGGER material_vault_seal_immutable BEFORE UPDATE ON material_vault_seals WHEN NEW.owner_id<>OLD.owner_id OR NEW.material_id<>OLD.material_id OR NEW.claim_id<>OLD.claim_id OR NEW.vault_id<>OLD.vault_id OR NEW.cipher_digest<>OLD.cipher_digest OR NEW.source_revision<>OLD.source_revision OR NEW.sealed_revision<>OLD.sealed_revision OR NEW.source_store_id<>OLD.source_store_id OR NEW.sealed_at<>OLD.sealed_at OR OLD.erasure_state<>'pending' OR NEW.erasure_state<>'confirmed' BEGIN SELECT RAISE(ABORT,'materials_vault_receipt_immutable'); END;
CREATE TRIGGER material_vault_seal_delete BEFORE DELETE ON material_vault_seals BEGIN SELECT RAISE(ABORT,'materials_vault_receipt_immutable'); END;
CREATE TRIGGER material_vault_seal_replace BEFORE INSERT ON material_vault_seals WHEN EXISTS(SELECT 1 FROM material_vault_seals WHERE(owner_id=NEW.owner_id AND material_id=NEW.material_id) OR claim_id=NEW.claim_id) BEGIN SELECT RAISE(ABORT,'materials_vault_receipt_immutable'); END;`;
const layout = db => JSON.stringify(db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => [row.type, row.name, row.tbl_name, row.sql]));
const expected=new Map();
function expectedLayout(version) { if (!expected.has(version)) { const db = new DatabaseSync(':memory:'); try { db.exec(DDL);if(version===2)db.exec(MATERIAL_VAULT_DDL);expected.set(version,layout(db)); } finally { db.close(); } } return expected.get(version); }
export function openMaterialSchema(db, projectId, {allowVaultMigration=false}={}) {
  materialCheck(typeof allowVaultMigration==='boolean','materials_invalid_arguments');let version = db.prepare('PRAGMA user_version').get().user_version;
  materialCheck([0,1,2].includes(version), 'materials_schema_unsupported');
  const empty = db.prepare("SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1").get() === undefined;
  materialCheck(version !== 0 || empty, 'materials_schema_mismatch');
  if (version) {
    materialCheck(layout(db) === expectedLayout(version), 'materials_schema_mismatch');
    const rows = db.prepare('SELECT key,value FROM material_meta ORDER BY key').all();
    const meta = Object.fromEntries(rows.map(row => [row.key, row.value]));
    materialCheck(rows.length === 3 && meta.lineage === `soty.materials.sqlite.v${version}`
      && /^[a-f0-9]{32}$/u.test(meta.registry_id), 'materials_schema_mismatch');
    materialCheck(meta.project_id === projectId, 'materials_project_mismatch');
  }
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
  if (!version) {
    db.exec('BEGIN IMMEDIATE');
    try {
      db.exec(DDL);
      const insert = db.prepare('INSERT INTO material_meta(key,value) VALUES(?,?)');
      for (const [key,value] of Object.entries({project_id:projectId,registry_id:randomBytes(16).toString('hex'),lineage:'soty.materials.sqlite.v1'})) insert.run(key,value);
      db.exec('PRAGMA user_version=1; COMMIT');version=1;
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  if(version===1&&allowVaultMigration){db.exec('BEGIN IMMEDIATE');try{const trigger=db.prepare("SELECT sql FROM sqlite_schema WHERE name='material_identity_no_update'").get().sql;db.exec(MATERIAL_VAULT_DDL);db.exec('DROP TRIGGER material_identity_no_update');db.prepare("UPDATE material_meta SET value='soty.materials.sqlite.v2' WHERE key='lineage'").run();db.exec(trigger);db.exec('PRAGMA user_version=2; COMMIT');version=2;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
  if(version===2){materialCheck(layout(db)===expectedLayout(2),'materials_schema_mismatch');for(const seal of db.prepare('SELECT * FROM material_vault_seals').iterate()){const row=db.prepare('SELECT * FROM materials WHERE owner_id=? AND id=?').get(seal.owner_id,seal.material_id);materialCheck(row&&row.revision===seal.sealed_revision&&row.size===1&&row.name===''&&row.media_type==='application/octet-stream'&&row.file_sha256==='0'.repeat(64)&&row.manifest===null&&row.manifest_sha256===null&&!db.prepare('SELECT 1 FROM material_chunks WHERE owner_id=? AND material_id=?').get(row.owner_id,row.id)&&!db.prepare('SELECT 1 FROM material_receipts WHERE owner_id=? AND material_id=?').get(row.owner_id,row.id),'materials_storage_corrupt');}}
  materialCheck(db.prepare('PRAGMA quick_check').all().every(row => row.quick_check === 'ok')
    && !db.prepare('PRAGMA foreign_key_check').get(), 'materials_storage_corrupt');
  return Object.freeze({ projectId, registryId: db.prepare("SELECT value FROM material_meta WHERE key='registry_id'").get().value,
    schemaVersion: version });
}
