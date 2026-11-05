import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync,constants} from 'node:sqlite';
import {initializeCanvasPagesStorage} from './storage.mjs';
import {initializeCanvasPagesStorage as oldPages} from './test-fixtures/storage-v1.mjs';
import {initializeCanvasStorage} from '../canvas-objects/storage.mjs';
import {initializeCanvasStorage as oldObjects} from '../canvas-objects/test-fixtures/storage-v1.mjs';

const snapshot=db=>({version:db.prepare('PRAGMA user_version').get().user_version,schema:db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all(),rows:db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name").all().map(({name})=>[name,db.prepare(`SELECT * FROM ${name} ORDER BY 1`).all()])});
for(const [kind,current,old]of[['Objects',initializeCanvasStorage,oldObjects],['Pages',initializeCanvasPagesStorage,oldPages]]){
 test(`${kind} format migration rolls back an actual failed ALTER and preserves the old reader; known2 reopens without opt-in`,()=>{
  const db=new DatabaseSync(':memory:');try{
   old(db,'selection_storage');if(kind==='Pages'){
    db.prepare('INSERT INTO canvas_pages VALUES(?,?,1,?,?,?,?)').run('stable_page','owner',1,'unlisted',100,100);
    db.prepare('INSERT INTO canvas_page_commands VALUES(?,?,?,?)').run('owner','old_request','old_intent',JSON.stringify({pageId:'stable_page',revision:1,committedAt:100}));
   }
   const before=snapshot(db);let denied=0;db.setAuthorizer(action=>{if(action===constants.SQLITE_ALTER_TABLE){denied++;return constants.SQLITE_DENY;}return constants.SQLITE_OK;});
   assert.throws(()=>current(db,'selection_storage',{allowFormat2Migration:true}),/not authorized/u);db.setAuthorizer(null);assert.equal(denied,1);assert.equal(db.isTransaction,false);assert.deepEqual(snapshot(db),before,'failed migration must restore even the dropped Object guard');old(db,'selection_storage');assert.deepEqual(snapshot(db),before);
   assert.equal(current(db,'selection_storage',{allowFormat2Migration:true}),2);const migrated=snapshot(db);assert.equal(current(db,'selection_storage'),2);assert.deepEqual(snapshot(db),migrated,'opt-in controls migration, never reopening');
   assert.throws(()=>old(db,'selection_storage'),{code:kind==='Pages'?'canvas_page_schema_unsupported':'canvas_schema_unsupported'});assert.deepEqual(snapshot(db),migrated);
   if(kind==='Pages'){assert.equal(migrated.rows.find(([name])=>name==='canvas_pages')[1][0].page_id,'stable_page');assert.deepEqual(migrated.rows.find(([name])=>name==='canvas_page_commands')[1],before.rows.find(([name])=>name==='canvas_page_commands')[1]);}
   db.exec('PRAGMA user_version=3');assert.throws(()=>current(db,'selection_storage',{allowFormat2Migration:true}),{code:kind==='Pages'?'canvas_page_schema_unsupported':'canvas_schema_unsupported'});assert.equal(db.prepare('PRAGMA user_version').get().user_version,3);
  }finally{db.close();}
 });
}
