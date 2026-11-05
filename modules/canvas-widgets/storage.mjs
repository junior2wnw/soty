import {DatabaseSync} from 'node:sqlite';
import {check} from './contract.mjs';
const ddl=`CREATE TABLE widget_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE canvas_widgets(widget_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=1),app_id TEXT NOT NULL,
 domain_id TEXT NOT NULL,entry_path TEXT NOT NULL,source_revision INTEGER NOT NULL CHECK(source_revision>=1),source_digest TEXT NOT NULL CHECK(length(source_digest)=64),source_profile TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('active','removed')),created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
CREATE INDEX canvas_widgets_owner ON canvas_widgets(owner_id,state,created_at,widget_id);
CREATE TABLE widget_commands(account_id TEXT NOT NULL,request_id TEXT NOT NULL,intent_hash TEXT NOT NULL,receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json)),PRIMARY KEY(account_id,request_id));
CREATE TRIGGER canvas_widget_immutable BEFORE UPDATE ON canvas_widgets
 WHEN NEW.widget_id<>OLD.widget_id OR NEW.owner_id<>OLD.owner_id OR NEW.app_id<>OLD.app_id OR NEW.domain_id<>OLD.domain_id OR NEW.entry_path<>OLD.entry_path OR NEW.created_at<>OLD.created_at OR NEW.revision<>OLD.revision+1
 BEGIN SELECT RAISE(ABORT,'canvas_widget_immutable'); END;
CREATE TRIGGER widget_receipt_no_update BEFORE UPDATE ON widget_commands BEGIN SELECT RAISE(ABORT,'widget_receipt_immutable'); END;
CREATE TRIGGER widget_receipt_no_delete BEFORE DELETE ON widget_commands BEGIN SELECT RAISE(ABORT,'widget_receipt_immutable'); END;
CREATE TRIGGER widget_receipt_no_replace BEFORE INSERT ON widget_commands WHEN EXISTS(SELECT 1 FROM widget_commands WHERE account_id=NEW.account_id AND request_id=NEW.request_id)
 BEGIN SELECT RAISE(ABORT,'widget_receipt_immutable'); END;`;
const layout=db=>db.prepare("SELECT name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row=>[row.name,row.sql.replace(/\s+/gu,' ').trim()]);
const expected=(()=>{const db=new DatabaseSync(':memory:');try{db.exec(ddl);return layout(db);}finally{db.close();}})();
export function initializeWidgetStorage(db,projectId){db.exec('PRAGMA busy_timeout=100; PRAGMA journal_mode=WAL;');const version=Number(db.prepare('PRAGMA user_version').get().user_version),empty=!db.prepare("SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1").get();
 if(version===0&&empty){db.exec('BEGIN IMMEDIATE');try{db.exec(ddl);db.prepare('INSERT INTO widget_meta VALUES(?,?)').run('project_id',projectId);db.exec('PRAGMA user_version=1; COMMIT');}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}else check(version===1,'canvas_widget_schema_unsupported',503);
 check(JSON.stringify(layout(db))===JSON.stringify(expected)&&db.prepare("SELECT value FROM widget_meta WHERE key='project_id'").get()?.value===projectId,'canvas_widget_storage_invalid',503);check(db.prepare('PRAGMA quick_check(1)').get().quick_check==='ok','canvas_widget_storage_invalid',503);
}
