import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import {mkdtempSync,copyFileSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createWorldService} from '../../world/server/index.mjs';
import {convertFieldDocumentToV2} from '../contract.mjs';
import {migrateField as migrateV1,fieldOperation as readV1} from './support/field-store-v1.mjs';

const owner={accountId:'layout_owner',deviceId:'layout_device',label:'Layout owner'},other={accountId:'layout_other',deviceId:'other_device',label:'Other owner'};
const v1=()=>({schema:'soty.field.v1',contexts:[{contextId:'home',title:'Дом',x:120,y:280}],shortcuts:[{shortcutId:'existing',entity:{kind:'app',id:'existing_source'},contextId:'home',slot:[2,-1]}]});
const v2=()=>{const doc=convertFieldDocumentToV2(v1());doc.shortcuts[0].placement={kind:'free',x:72,y:81,width:640,height:360};return doc;};
const code=value=>error=>{assert.equal(error.code,value);return true;};
const digest=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
function fixture(t){
 const parent=realpathSync(tmpdir()),directory=mkdtempSync(join(parent,'soty-field-v2-')),databasePath=join(directory,'world.sqlite'),backup=join(directory,'before-v2.sqlite');
 let service,serial=0;const options={databasePath,projectId:'field_v2_tests',clock:()=>1900000000000};
 const open=()=>{service=createWorldService(options);};open();
 const call=(actor,op,args={})=>service.execute({actor,op:'world.field.'+op,args:{expectedAccountId:actor.accountId,...args}});
 const put=(doc,revision,args={},actor=owner)=>call(actor,'put',{document:doc,expectedRevision:revision,requestId:'field_cmd_'+(++serial),...args});
 const sql=callback=>{const db=new DatabaseSync(databasePath);try{return callback(db);}finally{db.close();}};
 t.after(()=>{service?.close();const actual=realpathSync(directory);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-field-v2-'));rmSync(actual,{recursive:true,force:true});});
 const snapshot=()=>sql(db=>({epoch:db.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get().value,
  documents:db.prepare('SELECT * FROM world_field_documents ORDER BY account_id').all().map(row=>({...row})),receipts:db.prepare('SELECT * FROM world_field_receipts ORDER BY account_id,request_key').all().map(row=>({...row}))}));
 return {call,put,sql,snapshot,databasePath,backup,close(){service?.close();service=null;},open,restart(){service.close();open();}};
}

test('v1 reads/reopen retain bytes/hash/receipt; explicit v2 write alone advances epoch and survives reopen with old receipts intact',t=>{
 const f=fixture(t),args={requestId:'existing_v1_command'},first=f.put(v1(),0,args),before=f.snapshot();assert.equal(before.epoch,'1');assert.equal(before.documents[0].document_json,JSON.stringify(v1()));assert.equal(before.documents[0].content_hash,digest(v1()));
 f.restart();assert.deepEqual(f.call(owner,'get'),first.current);assert.deepEqual(f.snapshot(),before);f.close();copyFileSync(f.databasePath,f.backup);
 f.sql(db=>{migrateV1(db);const model={get:(sql,...args)=>db.prepare(sql).get(...args)};assert.deepEqual(readV1(model,'world.field.get',{expectedAccountId:owner.accountId},owner,1900000000000),first.current);});assert.deepEqual(f.snapshot(),before);f.open();
 const second=f.put(v2(),1),after=f.snapshot();assert.equal(after.epoch,'2');assert.equal(second.current.document.schema,'soty.field.v2');assert.equal(second.current.contentHash,digest(v2()));assert.deepEqual(after.receipts.find(row=>row.accepted_revision===1),before.receipts[0]);
 assert.deepEqual(f.put(v1(),0,args),{replayed:true,receipt:first.receipt,current:second.current},'v1 replay returns its immutable receipt and the current v2 document');
 assert.throws(()=>f.put(v1(),2),code('field_document_downgrade_forbidden'));assert.throws(()=>f.put(v2(),1),code('field_revision_conflict'));assert.deepEqual(f.snapshot(),after);
 f.restart();assert.deepEqual(f.call(owner,'get'),second.current);assert.deepEqual(f.snapshot(),after);
 assert.equal(f.call(other,'get').revision,0);assert.equal(f.call(other,'get').document.schema,'soty.field.v1','epoch2 never silently converts another account');
 assert.throws(()=>f.call(other,'get',{expectedAccountId:owner.accountId}),code('field_account_changed'));
 f.close();assert.throws(()=>f.sql(db=>migrateV1(db)),code('field_schema_unsupported'));assert.deepEqual(f.snapshot(),after,'old reader rejection never rewrites accepted v2 rows');
 const backupDb=new DatabaseSync(f.backup);try{migrateV1(backupDb);assert.equal(backupDb.prepare('SELECT document_json FROM world_field_documents').get().document_json,JSON.stringify(v1()));assert.equal(backupDb.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get().value,'1');}finally{backupDb.close();}
});

test('a failed receipt COMMIT path rolls epoch2 and geometry back together; the identical retry passes the sensitive oracle',t=>{
 const f=fixture(t);f.put(v1(),0);const before=f.snapshot();
 f.sql(db=>db.exec("CREATE TRIGGER fault_field2_receipt BEFORE INSERT ON world_field_receipts WHEN NEW.accepted_revision=2 BEGIN SELECT RAISE(ABORT,'field2_write_fault'); END"));
 const args={requestId:'stable_v2_attempt'};assert.throws(()=>f.put(v2(),1,args),/field2_write_fault/u);assert.deepEqual(f.snapshot(),before,'no v2 metadata marker, document or receipt may escape the failed transaction');
 f.sql(db=>db.exec('DROP TRIGGER fault_field2_receipt'));const accepted=f.put(v2(),1,args);assert.equal(f.snapshot().epoch,'2');assert.deepEqual(accepted.current.document,v2());
 assert.equal(f.put(v2(),1,args).replayed,true);assert.equal(f.snapshot().receipts.length,2);
});

test('invalid geometry and mismatched content hashes do not advance either content revision or the storage epoch',t=>{
 const f=fixture(t);f.put(v1(),0);const before=f.snapshot(),invalid=v2();invalid.shortcuts[0].placement.width=0;
 assert.throws(()=>f.put(invalid,1),code('field_document_invalid'));assert.throws(()=>f.put(v2(),1,{contentHash:'0'.repeat(64)}),code('field_content_hash_mismatch'));assert.deepEqual(f.snapshot(),before);
});
