import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,realpathSync,rmSync} from 'node:fs';
import {join,dirname,basename} from 'node:path';
import {tmpdir} from 'node:os';
import {createCanvasObjectsService} from './server.mjs';
import {initializeCanvasStorage as oldStorage} from './test-fixtures/storage-v1.mjs';
const owner={accountId:'policy_owner',deviceId:'policy_device'},privatePolicy={audience:'private',accountIds:[],communityIds:[]};
function fixture(t,{format2=true}={}){
 const parent=realpathSync(tmpdir()),folder=realpathSync(mkdtempSync(join(parent,'soty-object-policy-'))),file=join(folder,'objects.sqlite');let service,serial=0;
 const issued=new WeakSet([owner]),options={databasePath:file,projectId:'policy_test',actorActive:actor=>actor?.accountId===owner.accountId&&actor.deviceId===owner.deviceId,withActorAuthorityFence:(actor,callback)=>{assert.ok(issued.has(actor));return callback();},
  providers:[{providerId:'soty.notes',withBindingAuthority(_request,callback){return callback({allowed:true,protection:{kind:'none'}});},project:context=>service.withObjectAuthority(context,()=>({kind:'note',text:'same Source body'}))}]};
 const open=flag=>{service=createCanvasObjectsService({...options,allowFormat2Migration:flag});};open(format2);
 t.after(()=>{service.close();assert.equal(dirname(realpathSync(folder)),parent);assert.ok(basename(folder).startsWith('soty-object-policy-'));rmSync(folder,{recursive:true,force:true});});
 const call=(op,args={})=>service.execute({actor:owner,op:'canvas.objects.'+op,args:{expectedAccountId:owner.accountId,...args}});
 const create=()=>call('create',{requestId:'create_'+(++serial),binding:{providerId:'soty.notes',ref:'native_note'},presentation:{mask:{shape:'rect'},fontSize:16},visibility:privatePolicy,hidden:{owner:false,others:false},protection:{kind:'none'}}).current;
 const update=(object,extra)=>call('update',{objectId:object.objectId,expectedRevision:object.revision,requestId:'update_'+(++serial),...extra}).current;
 const sql=callback=>{const db=new DatabaseSync(file);try{return callback(db);}finally{db.close();}};
 return{file,call,create,update,sql,get service(){return service;},restart(flag=format2){service.close();open(flag);}};
}
test('font and shape writes preserve accessRevision while ordinary revision and current metadata advance',t=>{
 const f=fixture(t),first=f.create();assert.equal(first.accessRevision,1);const next=f.update(first,{presentation:{mask:{shape:'ellipse'},fontSize:26}});
 assert.equal(next.revision,2);assert.equal(next.accessRevision,1,'visual settings must not silently revoke a page or password permit');
 const projection=f.service.project({actor:owner,objectIds:[first.objectId]}).items[0];assert.equal(projection.accessRevision,1);assert.equal(projection.presentation.fontSize,26);f.restart();assert.equal(f.call('get',{objectId:first.objectId}).object.accessRevision,1);
 f.service.withObjectRead({actor:owner,objectId:first.objectId},context=>{assert.equal(context.objectRevision,2);assert.equal(context.objectAccessRevision,1);return 'exact read frame and stable policy pin are separate';});
});
test('effective privacy/hidden/state and explicit publication revoke bump policy; identical form payload does not',t=>{
 const f=fixture(t);let value=f.create();assert.equal(value.accessRevision,1);
 value=f.update(value,{presentation:value.presentation,visibility:privatePolicy,hidden:value.hidden});assert.equal(value.accessRevision,1,'the current all-fields form is not an explicit revoke');
 value=f.update(value,{visibility:{audience:'restricted',accountIds:['friend'],communityIds:[]}});assert.equal(value.accessRevision,2);
 value=f.update(value,{hidden:{owner:false,others:true}});assert.equal(value.accessRevision,3);
 value=f.call('remove',{objectId:value.objectId,expectedRevision:value.revision,requestId:'remove'}).current;assert.equal(value.accessRevision,4);
 value=f.call('restore',{objectId:value.objectId,expectedRevision:value.revision,requestId:'restore'}).current;assert.equal(value.accessRevision,5);
 value=f.update(value,{visibility:privatePolicy});assert.equal(value.accessRevision,6);
 const args={objectId:value.objectId,expectedRevision:value.revision,requestId:'only_me_again',revokePublication:true};value=f.call('update',args).current;assert.equal(value.accessRevision,7);assert.equal(f.call('update',args).replayed,true);assert.equal(f.call('update',args).current.accessRevision,7);
 f.restart();assert.equal(f.call('get',{objectId:value.objectId}).object.accessRevision,7);
});
test('format1 is retained by default; explicit migration pins prior policy conservatively and old reader refuses2',t=>{
 const f=fixture(t,{format2:false});let value=f.create();value=f.update(value,{presentation:{mask:{shape:'ellipse'},fontSize:18}});assert.equal(value.accessRevision,value.revision);
 const query=db=>({version:db.prepare('PRAGMA user_version').get().user_version,objects:db.prepare('SELECT object_id,owner_id,revision,binding_json,presentation_json,visibility_json,hidden_json,protection_kind,state,created_at,updated_at FROM canvas_objects').all(),receipts:db.prepare('SELECT * FROM canvas_commands').all()});
 const before=f.sql(query);assert.equal(before.version,1);f.restart(false);assert.deepEqual(f.sql(query),before);f.restart(true);const after=f.sql(query);assert.equal(after.version,2);assert.deepEqual(after.objects,before.objects);assert.deepEqual(after.receipts,before.receipts);assert.equal(f.call('get',{objectId:value.objectId}).object.accessRevision,value.revision);
 f.sql(db=>assert.throws(()=>oldStorage(db,'policy_test'),{code:'canvas_schema_unsupported'}));assert.deepEqual(f.sql(query),after);
 f.restart(false);assert.deepEqual(f.sql(query),after,'the current reader accepts known2 without opting into another migration');assert.equal(f.call('get',{objectId:value.objectId}).object.accessRevision,value.revision);
 f.sql(db=>db.exec('PRAGMA user_version=3'));assert.throws(()=>f.restart(true),{code:'canvas_schema_unsupported'});f.sql(db=>db.exec('PRAGMA user_version=2'));f.restart(true);
});
test('stale CAS and failed durable update change neither policy nor observer-visible state',t=>{
 const f=fixture(t),value=f.create();assert.equal(value.accessRevision,1);
 assert.throws(()=>f.call('update',{objectId:value.objectId,expectedRevision:0,requestId:'stale',revokePublication:true}));assert.equal(f.call('get',{objectId:value.objectId}).object.accessRevision,1);
 f.sql(db=>db.exec("CREATE TRIGGER fixture_policy_abort BEFORE UPDATE ON canvas_objects BEGIN SELECT RAISE(ABORT,'fixture_commit_failure'); END"));
 assert.throws(()=>f.update(value,{revokePublication:true}),/fixture_commit_failure/u);assert.equal(f.call('get',{objectId:value.objectId}).object.accessRevision,1);assert.equal(f.call('get',{objectId:value.objectId}).object.revision,1);f.sql(db=>db.exec('DROP TRIGGER fixture_policy_abort'));
});
