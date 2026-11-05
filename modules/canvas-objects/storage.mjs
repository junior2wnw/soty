import {DatabaseSync} from 'node:sqlite';
import {check} from './contract.mjs';
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
const migration2=`DROP TRIGGER canvas_object_revision;
ALTER TABLE canvas_objects ADD COLUMN access_revision INTEGER NOT NULL DEFAULT 1 CHECK(access_revision>=1 AND access_revision<=revision);
UPDATE canvas_objects SET access_revision=revision;
CREATE TRIGGER canvas_object_revision BEFORE UPDATE ON canvas_objects
 WHEN NEW.object_id<>OLD.object_id OR NEW.owner_id<>OLD.owner_id OR NEW.binding_json<>OLD.binding_json OR NEW.protection_kind<>OLD.protection_kind OR NEW.created_at<>OLD.created_at OR NEW.revision<>OLD.revision+1
 OR NEW.access_revision NOT IN(OLD.access_revision,OLD.access_revision+1)
 OR ((NEW.visibility_json<>OLD.visibility_json OR NEW.hidden_json<>OLD.hidden_json OR NEW.state<>OLD.state) AND NEW.access_revision<>OLD.access_revision+1)
 BEGIN SELECT RAISE(ABORT,'canvas_object_immutable'); END;`;
const expected=(()=>{const db=new DatabaseSync(':memory:');try{db.exec(ddl);const first=layout(db);db.exec(migration2);return {1:first,2:layout(db)};}finally{db.close();}})();
export function initializeCanvasStorage(db,projectId,{allowFormat2Migration=false}={}){
 check(typeof allowFormat2Migration==='boolean','canvas_configuration_invalid',500);
 let version=Number(db.prepare('PRAGMA user_version').get().user_version);const empty=!db.prepare("SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1").get();
 const verify=()=>{check(JSON.stringify(layout(db))===JSON.stringify(expected[version]),'canvas_storage_invalid',503);check(db.prepare("SELECT value FROM canvas_meta WHERE key='project_id'").get()?.value===projectId,'canvas_project_mismatch',503);check(db.prepare('PRAGMA quick_check(1)').get().quick_check==='ok','canvas_storage_invalid',503);};
 if(!(version===0&&empty)){check([1,2].includes(version),'canvas_schema_unsupported',503);verify();}
 db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=100;');
 if(version===0&&empty){db.exec('BEGIN IMMEDIATE');try{db.exec(ddl);db.prepare('INSERT INTO canvas_meta VALUES(?,?)').run('project_id',projectId);if(allowFormat2Migration)db.exec(migration2);version=allowFormat2Migration?2:1;db.exec(`PRAGMA user_version=${version}; COMMIT`);}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 else if(version===1&&allowFormat2Migration){db.exec('BEGIN IMMEDIATE');try{db.exec(migration2);db.exec('PRAGMA user_version=2; COMMIT');version=2;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 verify();return version;
}
