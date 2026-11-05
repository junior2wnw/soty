import {DatabaseSync} from 'node:sqlite';
import {check} from '../canvas-objects/contract.mjs';
const ddl=`CREATE TABLE canvas_pages_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE canvas_pages(page_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL UNIQUE,revision INTEGER NOT NULL CHECK(revision>=1),enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),audience TEXT NOT NULL CHECK(audience IN('private','unlisted','public')),created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
CREATE TABLE canvas_page_commands(account_id TEXT NOT NULL,request_id TEXT NOT NULL,intent_hash TEXT NOT NULL,receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json)),PRIMARY KEY(account_id,request_id));
CREATE TRIGGER canvas_page_immutable BEFORE UPDATE ON canvas_pages WHEN NEW.page_id<>OLD.page_id OR NEW.owner_id<>OLD.owner_id OR NEW.created_at<>OLD.created_at OR NEW.revision<>OLD.revision+1 BEGIN SELECT RAISE(ABORT,'canvas_page_immutable'); END;
CREATE TRIGGER canvas_page_required BEFORE DELETE ON canvas_pages BEGIN SELECT RAISE(ABORT,'canvas_page_required'); END;
CREATE TRIGGER canvas_page_no_replace BEFORE INSERT ON canvas_pages WHEN EXISTS(SELECT 1 FROM canvas_pages WHERE owner_id=NEW.owner_id OR page_id=NEW.page_id) BEGIN SELECT RAISE(ABORT,'canvas_page_immutable'); END;
CREATE TRIGGER canvas_page_receipt_update BEFORE UPDATE ON canvas_page_commands BEGIN SELECT RAISE(ABORT,'canvas_page_receipt_immutable'); END;
CREATE TRIGGER canvas_page_receipt_delete BEFORE DELETE ON canvas_page_commands BEGIN SELECT RAISE(ABORT,'canvas_page_receipt_immutable'); END;
CREATE TRIGGER canvas_page_receipt_replace BEFORE INSERT ON canvas_page_commands WHEN EXISTS(SELECT 1 FROM canvas_page_commands WHERE account_id=NEW.account_id AND request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'canvas_page_receipt_immutable'); END;`;
const layout=db=>db.prepare("SELECT name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row=>[row.name,row.sql.replace(/\s+/gu,' ').trim()]);
const migration2='ALTER TABLE canvas_pages ADD COLUMN selection_json TEXT CHECK(selection_json IS NULL OR json_valid(selection_json));';
const expected=(()=>{const db=new DatabaseSync(':memory:');try{db.exec(ddl);const first=layout(db);db.exec(migration2);return {1:first,2:layout(db)};}finally{db.close();}})();
export function initializeCanvasPagesStorage(db,projectId,{allowFormat2Migration=false}={}){
 check(typeof allowFormat2Migration==='boolean','canvas_page_configuration_invalid',500);let version=Number(db.prepare('PRAGMA user_version').get().user_version);const empty=!db.prepare("SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1").get();
 const verify=()=>check(JSON.stringify(layout(db))===JSON.stringify(expected[version])&&db.prepare("SELECT value FROM canvas_pages_meta WHERE key='project_id'").get()?.value===projectId&&db.prepare('PRAGMA quick_check(1)').get().quick_check==='ok','canvas_page_storage_invalid',503);
 if(!(version===0&&empty)){check([1,2].includes(version),'canvas_page_schema_unsupported',503);verify();}
 db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=100');
 if(version===0&&empty){db.exec('BEGIN IMMEDIATE');try{db.exec(ddl);db.prepare('INSERT INTO canvas_pages_meta VALUES(?,?)').run('project_id',projectId);if(allowFormat2Migration)db.exec(migration2);version=allowFormat2Migration?2:1;db.exec(`PRAGMA user_version=${version}; COMMIT`);}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 else if(version===1&&allowFormat2Migration){db.exec('BEGIN IMMEDIATE');try{db.exec(migration2);db.exec('PRAGMA user_version=2; COMMIT');version=2;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 verify();return version;
}
