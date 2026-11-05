import {DatabaseSync} from 'node:sqlite';
import {inspectNotesSchema as inspectLegacy,migrateNotes as migrateLegacy,validateNotesRows} from './schema-v2.mjs';
import {check} from './validation.mjs';
export const VAULT_NOTES_SCHEMA_VERSION=3;
export const VAULT_NOTES_DDL=`CREATE TABLE note_vault_seals(account_id TEXT NOT NULL,note_id TEXT NOT NULL,claim_id TEXT NOT NULL UNIQUE,vault_id TEXT NOT NULL,cipher_digest TEXT NOT NULL,source_revision INTEGER NOT NULL,sealed_revision INTEGER NOT NULL,source_store_id TEXT NOT NULL,erasure_state TEXT NOT NULL CHECK(erasure_state IN('pending','confirmed')),sealed_at INTEGER NOT NULL,confirmed_at INTEGER,PRIMARY KEY(account_id,note_id),FOREIGN KEY(account_id,note_id) REFERENCES notes(account_id,id)) STRICT;
CREATE TRIGGER note_vault_seal_immutable BEFORE UPDATE ON note_vault_seals WHEN NEW.account_id<>OLD.account_id OR NEW.note_id<>OLD.note_id OR NEW.claim_id<>OLD.claim_id OR NEW.vault_id<>OLD.vault_id OR NEW.cipher_digest<>OLD.cipher_digest OR NEW.source_revision<>OLD.source_revision OR NEW.sealed_revision<>OLD.sealed_revision OR NEW.source_store_id<>OLD.source_store_id OR NEW.sealed_at<>OLD.sealed_at OR OLD.erasure_state<>'pending' OR NEW.erasure_state<>'confirmed' BEGIN SELECT RAISE(ABORT,'notes_vault_receipt_immutable'); END;
CREATE TRIGGER note_vault_seal_delete BEFORE DELETE ON note_vault_seals BEGIN SELECT RAISE(ABORT,'notes_vault_receipt_immutable'); END;
CREATE TRIGGER note_vault_seal_replace BEFORE INSERT ON note_vault_seals WHEN EXISTS(SELECT 1 FROM note_vault_seals WHERE(account_id=NEW.account_id AND note_id=NEW.note_id) OR claim_id=NEW.claim_id) BEGIN SELECT RAISE(ABORT,'notes_vault_receipt_immutable'); END;`;
const tokens=sql=>(sql??'').match(/'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|[^\s'"`\[\]]+/gu)?.join(' ').replace(/;$/u,'')??'';
const layout=db=>JSON.stringify(db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row=>[row.type,row.name,row.tbl_name,tokens(row.sql)]));
let expected;
function expectedLayout(){if(!expected){const db=new DatabaseSync(':memory:');try{migrateLegacy(db,'reference',{allowNativeMigration:true});db.exec(VAULT_NOTES_DDL);expected=layout(db);}finally{db.close();}}return expected;}
export function inspectVaultNotesSchema(db,projectId){check(db.prepare('PRAGMA user_version').get().user_version===3,'notes_schema_unsupported');check(layout(db)===expectedLayout(),'notes_schema_metadata_mismatch');const meta=Object.fromEntries(db.prepare('SELECT key,value FROM notes_meta ORDER BY key').all().map(row=>[row.key,row.value]));check(Object.keys(meta).length===3&&meta.project_id===projectId&&meta.lineage==='soty.notes.sqlite.v3'&&/^[a-f0-9]{32}$/u.test(meta.registry_id),'notes_schema_metadata_mismatch');return Object.freeze({schemaVersion:3,registryId:meta.registry_id});}
export function inspectNotesSchema(db,projectId){return Number(db.prepare('PRAGMA user_version').get().user_version)===3?inspectVaultNotesSchema(db,projectId):inspectLegacy(db,projectId);}
export function migrateNotes(db,projectId,options={}){
 check(options&&typeof options==='object'&&!Array.isArray(options)&&Object.keys(options).every(key=>['allowNativeMigration','allowVaultMigration'].includes(key)),'notes_invalid_arguments');for(const key of Object.keys(options))check(typeof options[key]==='boolean','notes_invalid_arguments');const allowVaultMigration=options.allowVaultMigration??false;
 let state=inspectNotesSchema(db,projectId);
 if(state.schemaVersion!==3){state=migrateLegacy(db,projectId,{allowNativeMigration:allowVaultMigration||(options.allowNativeMigration??false)},allowVaultMigration?()=>{db.exec(VAULT_NOTES_DDL);db.prepare("UPDATE notes_meta SET value='soty.notes.sqlite.v3' WHERE key='lineage'").run();db.exec('PRAGMA user_version=3');return inspectVaultNotesSchema(db,projectId);}:undefined);if(!allowVaultMigration)return state;
 }
 db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL; PRAGMA journal_mode=WAL; PRAGMA secure_delete=ON');
 state=inspectVaultNotesSchema(db,projectId);validateNotesRows(db,3,{sealed:row=>Boolean(db.prepare('SELECT 1 FROM note_vault_seals WHERE account_id=? AND note_id=?').get(row.account_id,row.id))});
 for(const seal of db.prepare('SELECT * FROM note_vault_seals').iterate()){const row=db.prepare('SELECT * FROM notes WHERE account_id=? AND id=?').get(seal.account_id,seal.note_id);check(row&&row.revision===seal.sealed_revision&&seal.sealed_revision===seal.source_revision+1&&seal.source_store_id===state.registryId,'notes_storage_corrupt');}
 return state;
}
