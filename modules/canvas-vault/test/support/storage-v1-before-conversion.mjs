import {DatabaseSync} from 'node:sqlite';
import {check} from '../../../canvas-objects/contract.mjs';
const ddl=`CREATE TABLE vault_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE vault_sources(vault_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=1),kind TEXT NOT NULL CHECK(kind IN('native','placement')),object_id TEXT UNIQUE,binding_json TEXT CHECK(binding_json IS NULL OR json_valid(binding_json)),container_json TEXT NOT NULL CHECK(json_valid(container_json)),created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,CHECK((kind='native' AND object_id IS NULL AND binding_json IS NULL) OR(kind='placement' AND object_id IS NOT NULL AND binding_json IS NOT NULL)));
CREATE INDEX vault_owner ON vault_sources(owner_id,created_at,vault_id);
CREATE TABLE vault_commands(account_id TEXT NOT NULL,request_id TEXT NOT NULL,operation TEXT NOT NULL,intent TEXT NOT NULL,receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json)),PRIMARY KEY(account_id,request_id));
CREATE TRIGGER vault_source_immutable BEFORE UPDATE ON vault_sources WHEN NEW.vault_id<>OLD.vault_id OR NEW.owner_id<>OLD.owner_id OR NEW.kind<>OLD.kind OR NEW.object_id IS NOT OLD.object_id OR NEW.binding_json IS NOT OLD.binding_json OR NEW.created_at<>OLD.created_at OR NEW.revision<>OLD.revision+1 BEGIN SELECT RAISE(ABORT,'vault_source_immutable'); END;
CREATE TRIGGER vault_receipt_update BEFORE UPDATE ON vault_commands BEGIN SELECT RAISE(ABORT,'vault_receipt_immutable'); END;
CREATE TRIGGER vault_receipt_delete BEFORE DELETE ON vault_commands BEGIN SELECT RAISE(ABORT,'vault_receipt_immutable'); END;
CREATE TRIGGER vault_receipt_replace BEFORE INSERT ON vault_commands WHEN EXISTS(SELECT 1 FROM vault_commands WHERE account_id=NEW.account_id AND request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'vault_receipt_immutable'); END;`;
const layout=db=>db.prepare("SELECT name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row=>[row.name,row.sql.replace(/\s+/gu,' ').trim()]);
const expected=(()=>{const db=new DatabaseSync(':memory:');try{db.exec(ddl);return layout(db);}finally{db.close();}})();
export function initializeVaultStorage(db,projectId){
 db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA secure_delete=ON; PRAGMA busy_timeout=100');
 const version=Number(db.prepare('PRAGMA user_version').get().user_version),empty=!db.prepare("SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1").get();
 if(version===0&&empty){db.exec('BEGIN IMMEDIATE');try{db.exec(ddl);db.prepare('INSERT INTO vault_meta VALUES(?,?)').run('project_id',projectId);db.exec('PRAGMA user_version=1; COMMIT');}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 else check(version===1,'vault_schema_unsupported',503);
 check(JSON.stringify(layout(db))===JSON.stringify(expected)&&db.prepare("SELECT value FROM vault_meta WHERE key='project_id'").get()?.value===projectId&&db.prepare('PRAGMA quick_check(1)').get().quick_check==='ok','vault_storage_invalid',503);
}
