// Frozen format1 reader; original storage.mjs SHA256 30d301879a6d88fec6bf9620fed1278220a00279baaafa6a43053573ada2f336. Import path only adjusted.
import {DatabaseSync} from 'node:sqlite';
import {check} from '../contract.mjs';
const ddl=`CREATE TABLE canvas_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE canvas_objects(object_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=1),
 binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),presentation_json TEXT NOT NULL CHECK(json_valid(presentation_json)),
 visibility_json TEXT NOT NULL CHECK(json_valid(visibility_json)),hidden_json TEXT NOT NULL CHECK(json_valid(hidden_json)),
 protection_kind TEXT NOT NULL CHECK(protection_kind IN('none','password')),state TEXT NOT NULL CHECK(state IN('active','removed')),
 created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
CREATE INDEX canvas_objects_owner ON canvas_objects(owner_id,state,created_at,object_id);
CREATE TABLE canvas_commands(account_id TEXT NOT NULL,request_id TEXT NOT NULL,intent_hash TEXT NOT NULL,
 receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json)),PRIMARY KEY(account_id,request_id));
CREATE TRIGGER canvas_object_revision BEFORE UPDATE ON canvas_objects
 WHEN NEW.object_id<>OLD.object_id OR NEW.owner_id<>OLD.owner_id OR NEW.binding_json<>OLD.binding_json OR NEW.protection_kind<>OLD.protection_kind OR NEW.created_at<>OLD.created_at OR NEW.revision<>OLD.revision+1
 BEGIN SELECT RAISE(ABORT,'canvas_object_immutable'); END;
CREATE TRIGGER canvas_command_no_update BEFORE UPDATE ON canvas_commands BEGIN SELECT RAISE(ABORT,'canvas_receipt_immutable'); END;
CREATE TRIGGER canvas_command_no_delete BEFORE DELETE ON canvas_commands BEGIN SELECT RAISE(ABORT,'canvas_receipt_immutable'); END;
CREATE TRIGGER canvas_command_no_replace BEFORE INSERT ON canvas_commands
 WHEN EXISTS(SELECT 1 FROM canvas_commands WHERE account_id=NEW.account_id AND request_id=NEW.request_id)
 BEGIN SELECT RAISE(ABORT,'canvas_receipt_immutable'); END;`;
const layout=db=>db.prepare("SELECT name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row=>[row.name,row.sql.replace(/\s+/gu,' ').trim()]);
const expected=(()=>{const db=new DatabaseSync(':memory:');try{db.exec(ddl);return layout(db);}finally{db.close();}})();
export function initializeCanvasStorage(db,projectId){
 db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=100;');
 const version=Number(db.prepare('PRAGMA user_version').get().user_version),empty=!db.prepare("SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1").get();
 if(version===0&&empty){db.exec('BEGIN IMMEDIATE');try{db.exec(ddl);db.prepare('INSERT INTO canvas_meta VALUES(?,?)').run('project_id',projectId);db.exec('PRAGMA user_version=1; COMMIT');}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 else check(version===1,'canvas_schema_unsupported',503);
 check(JSON.stringify(layout(db))===JSON.stringify(expected),'canvas_storage_invalid',503);
 check(db.prepare("SELECT value FROM canvas_meta WHERE key='project_id'").get()?.value===projectId,'canvas_project_mismatch',503);
 check(db.prepare('PRAGMA quick_check(1)').get().quick_check==='ok','canvas_storage_invalid',503);
}
