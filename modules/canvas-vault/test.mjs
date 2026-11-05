import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,rmSync,realpathSync,readFileSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createCanvasObjectsService} from '../canvas-objects/server.mjs';
import {CanvasObjectError} from '../canvas-objects/contract.mjs';
import {createCanvasVaultService} from './server.mjs';

const owner={accountId:'owner',deviceId:'owner_device'},otherDevice={accountId:'owner',deviceId:'owner_other_device'},friend={accountId:'friend',deviceId:'friend_device'},outside={accountId:'outside',deviceId:'outside_device'};
const secret='Cipher-only note: not a plaintext cache 8f3b72c1';
const password='Never-in-sqlite-9b41',secondPassword='Rotated-password-a280';
const data=value=>({contentType:'application/vnd.soty.note+json',bytesBase64:Buffer.from(JSON.stringify({title:'Секрет',body:value})).toString('base64url')});
const code=value=>error=>{assert.equal(error.code,value);return true;};
test('format1 status truthfully disables native conversion without changing the ordinary response',async t=>{
 const f=fixture(t),object=f.create({providerId:'soty.notes',ref:'capability_native_plaintext'});
 const plain=await f.call(owner,'status',{objectId:object.objectId});
 assert.deepEqual(plain,{objectId:object.objectId,locked:false,protection:{kind:'none'}});
 const extended=await f.call(owner,'status',{objectId:object.objectId,includeCapabilities:true});
 assert.deepEqual(extended.capabilities,{nativeConversion:false,placementProtection:false});
 const app=f.create({providerId:'soty.apps',ref:'capability_widget'});
 assert.deepEqual((await f.call(owner,'status',{objectId:app.objectId,includeCapabilities:true})).capabilities,{nativeConversion:false,placementProtection:true});
});

function fixture(t,{format2=false}={}){
 const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-canvas-vault-')),vaultPath=join(folder,'vault.sqlite'),objectPath=join(folder,'objects.sqlite');let objects,vault,serial=0,time=1800000000000,depth=0;
 const issued=new WeakSet([owner,otherDevice,friend,outside]),activeDevices=new Set([owner.deviceId,otherDevice.deviceId,friend.deviceId,outside.deviceId]),calls=[],changes=[],invalidations=[];let appAllowed=true,bypassProtection=false;
 const actorActive=actor=>activeDevices.has(actor.deviceId),withActorAuthorityFence=(actor,callback)=>{if(!issued.has(actor))throw new CanvasObjectError('connect_authority_actor_invalid',403);assert.equal(depth,0);depth++;try{return callback();}finally{depth--;}};
 const plainProvider=providerId=>({providerId,withBindingAuthority(request,callback){calls.push('authority:'+providerId);const allowed=providerId!=='soty.apps'||appAllowed;const result=callback({allowed,protection:{kind:'none'}});if(allowed&&providerId==='soty.apps'&&!appAllowed)throw new CanvasObjectError('canvas_provider_access_denied',403);return result;},project(){calls.push('project:'+providerId);return {kind:'widget',value:'upstream app bytes'};}});
 function open(){objects=createCanvasObjectsService({databasePath:objectPath,projectId:'vault_test',actorActive,withActorAuthorityFence,providers:[plainProvider('soty.apps'),plainProvider('soty.notes')],withProtectionAuthority:(request,callback)=>bypassProtection?callback({kind:'none',unlocked:true,generation:0}):vault.withProtectionAuthority(request,callback),clock:()=>time});
  vault=createCanvasVaultService({databasePath:vaultPath,projectId:'vault_test',allowFormat2Migration:format2,actorActive,withActorAuthorityFence,withProtectionTarget:(request,callback)=>objects.withProtectionTarget(request,callback),withObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback),withRuntimeTarget:(request,callback)=>objects.withRuntimeProtectionTarget(request,callback),clock:()=>time,onChange:change=>changes.push(change),onInvalidate:change=>invalidations.push(change)});objects.registerProvider(vault.provider);
 }
 open();t.after(()=>{vault.close();objects.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-canvas-vault-'));rmSync(actual,{recursive:true,force:true});});
 const call=(actor,op,args={})=>vault.execute({actor,op:'canvas.vault.'+op,args:{expectedAccountId:actor.accountId,...args}});
 const mutate=(actor,op,args={})=>call(actor,op,{requestId:'vault_cmd_'+(++serial),...args});
 const objectCall=(actor,op,args={})=>objects.execute({actor,op:'canvas.objects.'+op,args:{expectedAccountId:actor.accountId,...args}});
 const create=(binding,extra={})=>objectCall(owner,'create',{requestId:'object_cmd_'+(++serial),binding,presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:binding.providerId==='soty.vault'?'password':'none'},...extra}).current;
 const sql=callback=>{const db=new DatabaseSync(vaultPath);try{return callback(db);}finally{db.close();}};
 const read=(actor,objectId,index=0)=>objects.withObjectRead({actor,objectId},context=>vault.readContent({context,index}));
 return {call,mutate,create,objectCall,sql,read,calls,changes,invalidations,activeDevices,folder,vaultPath,get objects(){return objects;},get vault(){return vault;},advance(ms){time+=ms;},revokeApp(){appAllowed=false;},bypassProtection(value){bypassProtection=value;},restart(){vault.close();objects.close();open();},async encrypted(extra={}){const source=await mutate(owner,'create',{password,content:data(secret),...extra});return {source,object:create(source.binding)};}};
}

test('optional local key scope is the canonical native Source; old status/unlock wire stays unchanged',async t=>{
 const f=fixture(t),{source,object:a}=await f.encrypted(),b=f.create(source.binding);
 const ordinary=await f.call(owner,'status',{objectId:a.objectId});assert.equal(Object.hasOwn(ordinary,'localKeyScope'),false);
 const first=await f.call(owner,'status',{objectId:a.objectId,includeLocalKeyScope:true}),second=await f.call(owner,'status',{objectId:b.objectId,includeLocalKeyScope:true});
 assert.deepEqual(first.localKeyScope,{schema:'soty.vault.local-scope.v1',kind:'source',id:source.current.vaultId});assert.deepEqual(second.localKeyScope,first.localKeyScope);
 assert.deepEqual(Object.keys(first).filter(key=>key!=='localKeyScope'),Object.keys(ordinary));
 const unlocked=await f.call(owner,'unlock',{objectId:a.objectId,password});assert.equal(Object.hasOwn(unlocked,'localKeyScope'),false);
 const extended=await f.call(owner,'unlock',{objectId:a.objectId,password,includeLocalKeyScope:true});assert.deepEqual(extended.localKeyScope,first.localKeyScope);
 await assert.rejects(f.call(owner,'status',{objectId:a.objectId,includeLocalKeyScope:'true'}),code('canvas_object_invalid'));
});

test('local key metadata requires the real shared Object grant; equal widget bindings keep placement scopes separate',async t=>{
 const f=fixture(t),{object:a}=await f.encrypted();
 await assert.rejects(f.call(friend,'status',{objectId:a.objectId,includeLocalKeyScope:true}),code('canvas_object_unavailable'));
 f.objectCall(owner,'update',{requestId:'scope_share',objectId:a.objectId,expectedRevision:1,visibility:{audience:'restricted',accountIds:[friend.accountId],communityIds:[]}});
 const friendScope=await f.call(friend,'status',{objectId:a.objectId,includeLocalKeyScope:true});assert.equal(friendScope.localKeyScope.kind,'source');assert.equal(Object.hasOwn(friendScope,'ownerId'),false);
 await f.call(friend,'unlock',{objectId:a.objectId,password,includeLocalKeyScope:true});
 f.objectCall(owner,'update',{requestId:'scope_unshare',objectId:a.objectId,expectedRevision:2,visibility:{audience:'private',accountIds:[],communityIds:[]}});
 await assert.rejects(f.call(friend,'status',{objectId:a.objectId,includeLocalKeyScope:true}),code('canvas_object_unavailable'));
 const widgets=[f.create({providerId:'soty.apps',ref:'same_app'}),f.create({providerId:'soty.apps',ref:'same_app'})];
 for(const item of widgets)await f.mutate(owner,'protect',{objectId:item.objectId,expectedObjectRevision:1,password});
 const scopes=await Promise.all(widgets.map(item=>f.call(owner,'status',{objectId:item.objectId,includeLocalKeyScope:true})));
 assert.deepEqual(scopes.map(value=>value.localKeyScope),widgets.map(item=>({schema:'soty.vault.local-scope.v1',kind:'placement',id:item.objectId})));
});

test('create receipt lookup is owner-only read-only metadata and never creates or replays a Source',async t=>{
 const f=fixture(t),requestId='scope_original_create',created=await f.mutate(owner,'create',{requestId,password,content:data(secret)});
 const snapshot=()=>f.sql(db=>JSON.stringify({sources:db.prepare('SELECT * FROM vault_sources').all(),commands:db.prepare('SELECT * FROM vault_commands').all()}));
 const before=snapshot(),result=await f.call(owner,'createReceipt',{sourceRequestId:requestId});
 assert.deepEqual(result,{schema:'soty.vault-create-receipt.v1',state:'committed',sourceRequestId:requestId,localKeyScope:{schema:'soty.vault.local-scope.v1',kind:'source',id:created.current.vaultId},binding:created.binding,revision:1});
 assert.deepEqual(await f.call(owner,'createReceipt',{sourceRequestId:'never_submitted'}),{schema:'soty.vault-create-receipt.v1',state:'missing'});
 assert.deepEqual(await f.call(friend,'createReceipt',{sourceRequestId:requestId}),{schema:'soty.vault-create-receipt.v1',state:'missing'});
 assert.equal(snapshot(),before);for(const value of[secret,password,'receiptKey','cipher','dataKey'])assert.equal(JSON.stringify(result).includes(value),false);
});

test('new encrypted source never stores plaintext/password; actual SQLite reopen requires a new device permit',async t=>{
 const f=fixture(t),{source,object}=await f.encrypted();assert.equal(source.current.revision,1);assert.equal(f.objects.project({actor:owner,objectIds:[object.objectId]}).items[0].view.state,'locked');
 assert.throws(()=>f.read(owner,object.objectId),code('canvas_object_locked'));await f.call(owner,'unlock',{objectId:object.objectId,password});
 assert.equal(f.objects.project({actor:owner,objectIds:[object.objectId]}).items[0].view.kind,'vault');assert.equal(JSON.parse(f.read(owner,object.objectId).bytes).body,secret);
 for(const suffix of ['','-wal','-shm'])if(existsSync(f.vaultPath+suffix)){const bytes=readFileSync(f.vaultPath+suffix);for(const value of [secret,password,Buffer.from(secret).toString('base64url')])assert.equal(bytes.includes(Buffer.from(value)),false,'no open source, password or disk cache');}
 assert.doesNotMatch(JSON.stringify(f.changes),/Cipher-only|Never-in|ciphertext|receiptKey|dataKey|wrappingKey/u);
 f.restart();assert.throws(()=>f.read(owner,object.objectId),code('canvas_object_locked'));await assert.rejects(f.call(owner,'unlock',{objectId:object.objectId,password:'wrong-password'}),code('password_container_unavailable'));
 await f.call(owner,'unlock',{objectId:object.objectId,password});assert.equal(JSON.parse(f.read(owner,object.objectId).bytes).body,secret);
});

test('password never grants Object ACL; permits are scoped to original actor, account, device and Object revision',async t=>{
 const f=fixture(t),{object}=await f.encrypted();await f.call(owner,'unlock',{objectId:object.objectId,password});
 await assert.rejects(f.call(friend,'unlock',{objectId:object.objectId,password}),code('canvas_object_unavailable'));
 await assert.rejects(f.call({...owner},'unlock',{objectId:object.objectId,password}),code('connect_authority_actor_invalid'));
 assert.throws(()=>f.read(otherDevice,object.objectId),code('canvas_object_locked'));
 assert.equal(f.objects.project({actor:friend,objectIds:[object.objectId]}).items[0].available,false);assert.equal(f.objects.project({objectIds:[object.objectId]}).items[0].available,false);
 f.objectCall(owner,'update',{objectId:object.objectId,expectedRevision:1,requestId:'rename_object',presentation:{mask:{shape:'ellipse'}}});assert.throws(()=>f.read(owner,object.objectId),code('canvas_object_locked'));
 await f.call(owner,'unlock',{objectId:object.objectId,password});assert.equal(JSON.parse(f.read(owner,object.objectId).bytes).body,secret);
 f.activeDevices.delete(owner.deviceId);assert.throws(()=>f.read(owner,object.objectId),code('canvas_authentication_required'));
});

test('ciphertext write CAS persists, detects changed retry intent, refreshes AEAD version and invalidates other devices',async t=>{
 const f=fixture(t),{object,source}=await f.encrypted();await f.call(owner,'unlock',{objectId:object.objectId,password});await f.call(otherDevice,'unlock',{objectId:object.objectId,password});const before=f.sql(db=>db.prepare('SELECT container_json FROM vault_sources WHERE vault_id=?').get(source.current.vaultId).container_json);
 const args={objectId:object.objectId,expectedRevision:1,requestId:'edit_once',content:data('Edited encrypted body')},saved=await f.call(owner,'write',args);assert.equal(saved.current.revision,2);assert.equal(JSON.parse(f.read(owner,object.objectId).bytes).body,'Edited encrypted body');assert.throws(()=>f.read(otherDevice,object.objectId),code('canvas_object_locked'));
 assert.equal((await f.call(owner,'write',args)).replayed,true);await assert.rejects(f.call(owner,'write',{...args,content:data('different body')}),code('vault_request_conflict'));
 await assert.rejects(f.mutate(owner,'write',{objectId:object.objectId,expectedRevision:1,content:data('stale write')}),code('vault_revision_conflict'));
 const after=f.sql(db=>db.prepare('SELECT container_json FROM vault_sources WHERE vault_id=?').get(source.current.vaultId).container_json);assert.notEqual(after,before);assert.equal(JSON.parse(after).context.version,2);assert.notEqual(JSON.parse(after).payload.iv,JSON.parse(before).payload.iv);
 f.restart();await f.call(owner,'unlock',{objectId:object.objectId,password});assert.equal(JSON.parse(f.read(owner,object.objectId).bytes).body,'Edited encrypted body');
});

test('failed receipt COMMIT rolls encrypted creation/write back and emits no false successful change',async t=>{
 const f=fixture(t);f.sql(db=>db.exec("CREATE TRIGGER injected_failure BEFORE INSERT ON vault_commands BEGIN SELECT RAISE(ABORT,'vault_write_fault'); END"));
 await assert.rejects(f.mutate(owner,'create',{password,content:data(secret)}),/vault_write_fault/u);assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM vault_sources').get().n),0);assert.equal(f.changes.length,0);
 f.sql(db=>db.exec('DROP TRIGGER injected_failure'));const {object}=await f.encrypted();await f.call(owner,'unlock',{objectId:object.objectId,password});const changes=f.changes.length;
 f.sql(db=>db.exec("CREATE TRIGGER injected_failure BEFORE INSERT ON vault_commands BEGIN SELECT RAISE(ABORT,'vault_write_fault'); END"));const args={requestId:'write_fault_retry',objectId:object.objectId,expectedRevision:1,content:data('fault recovered')};
 await assert.rejects(f.call(owner,'write',args),/vault_write_fault/u);assert.equal(f.changes.length,changes);assert.equal(JSON.parse(f.read(owner,object.objectId).bytes).body,secret);
 f.sql(db=>db.exec('DROP TRIGGER injected_failure'));assert.equal((await f.call(owner,'write',args)).current.revision,2);assert.equal(JSON.parse(f.read(owner,object.objectId).bytes).body,'fault recovered');
});

test('relock cancels in-flight async unlock; expiry, explicit relock and restart prevent content callbacks',async t=>{
 const f=fixture(t),{object}=await f.encrypted(),objectId=object.objectId;
 const pending=f.call(owner,'unlock',{objectId,password});await f.mutate(owner,'relock',{objectId});await assert.rejects(pending,code('vault_unlock_cancelled'));
 await f.call(owner,'unlock',{objectId,password,ttlMs:1000});f.advance(1001);let called=false;assert.throws(()=>f.objects.withObjectRead({actor:owner,objectId},()=>{called=true;return 'leak';}),code('canvas_object_locked'));assert.equal(called,false);assert.ok(f.invalidations.some(value=>value.objectId===objectId));
 await f.call(owner,'unlock',{objectId,password});await f.mutate(owner,'relock',{objectId});assert.throws(()=>f.read(owner,objectId),code('canvas_object_locked'));
});

test('current Object/Source revocation during KDF wins over correct password and never invokes provider.project',async t=>{
 const f=fixture(t),object=f.create({providerId:'soty.apps',ref:'widget-example'}),objectId=object.objectId;await f.mutate(owner,'protect',{objectId,expectedObjectRevision:1,password});f.calls.length=0;
 assert.equal(f.objects.project({actor:owner,objectIds:[objectId]}).items[0].view.state,'locked');assert.deepEqual(f.calls,[],'the password barrier runs BEFORE provider lookup/authority, not after source reads');
 const pending=f.call(owner,'unlock',{objectId,password});f.revokeApp();await assert.rejects(pending,code('canvas_provider_access_denied'));assert.equal(f.calls.some(value=>value.startsWith('project:')),false);
});

test('placement barrier covers runtime as well as content while leaving independent main-app ACL outside its claim',async t=>{
 const f=fixture(t),widgetId='widget-092509bf-99d1-4be6-9d0a-ebcc38142443',object=f.create({providerId:'soty.apps',ref:widgetId}),objectId=object.objectId;
 assert.equal(f.vault.checkWidgetRuntime({widgetId,actor:owner}),true);await f.mutate(owner,'protect',{objectId,expectedObjectRevision:1,password});
 assert.throws(()=>f.vault.checkWidgetRuntime({widgetId,actor:owner}),code('canvas_object_locked'));await f.call(owner,'unlock',{objectId,password});assert.equal(f.vault.checkWidgetRuntime({widgetId,actor:owner}),true);assert.equal(f.objects.project({actor:owner,objectIds:[objectId]}).items[0].view.value,'upstream app bytes');
 assert.throws(()=>f.vault.checkWidgetRuntime({widgetId,actor:otherDevice}),code('canvas_object_locked'));await f.mutate(owner,'relock',{objectId,scope:'all'});assert.throws(()=>f.vault.checkWidgetRuntime({widgetId,actor:owner}),code('canvas_object_locked'));
 assert.equal(f.vault.checkWidgetRuntime({widgetId:'ordinary-unknown-instance',actor:owner}),true,'unrelated ordinary app runtimes retain their own Apps ACL');
 const plaintext=f.create({providerId:'soty.notes',ref:'existing_plaintext'});await assert.rejects(f.mutate(owner,'protect',{objectId:plaintext.objectId,expectedObjectRevision:1,password}),code('vault_conversion_required'));
});

test('password rotation keeps content, rejects the old password and makes retry durable without storing either password',async t=>{
 const f=fixture(t),{object}=await f.encrypted(),objectId=object.objectId;await f.call(otherDevice,'unlock',{objectId,password});const args={requestId:'rotate_once',objectId,expectedRevision:1,oldPassword:password,newPassword:secondPassword};
 const result=await f.call(owner,'rotate',args);assert.equal(result.current.revision,2);assert.equal(JSON.parse(f.read(owner,objectId).bytes).body,secret);assert.throws(()=>f.read(otherDevice,objectId),code('canvas_object_locked'));
 assert.equal((await f.call(owner,'rotate',args)).replayed,true);await f.mutate(owner,'relock',{objectId,scope:'all'});await assert.rejects(f.call(owner,'unlock',{objectId,password}),code('password_container_unavailable'));
 f.restart();await f.call(owner,'unlock',{objectId,password:secondPassword});assert.equal(JSON.parse(f.read(owner,objectId).bytes).body,secret);assert.doesNotMatch(f.sql(db=>JSON.stringify(db.prepare('SELECT * FROM vault_sources').all())),/Never-in|Rotated-password/u);
});

test('a copied or late Object context cannot read ciphertext payload and altered AEAD context/tag fails closed',async t=>{
 const f=fixture(t),{object,source}=await f.encrypted(),objectId=object.objectId;await f.call(owner,'unlock',{objectId,password});let captured;
 f.objects.withObjectRead({actor:owner,objectId},context=>{captured=context;assert.throws(()=>f.vault.readContent({context:JSON.parse(JSON.stringify(context))}),code('canvas_object_authority_required'));return f.vault.readContent({context});});assert.throws(()=>f.vault.readContent({context:captured}),code('canvas_object_authority_required'));
 await f.mutate(owner,'relock',{objectId});f.sql(db=>{const row=db.prepare('SELECT * FROM vault_sources WHERE vault_id=?').get(source.current.vaultId),value=JSON.parse(row.container_json);value.payload.tag='AAAAAAAAAAAAAAAAAAAAAA';value.context.version=2;db.prepare('UPDATE vault_sources SET revision=revision+1,container_json=? WHERE vault_id=?').run(JSON.stringify(value),row.vault_id);});
 await assert.rejects(f.call(owner,'unlock',{objectId,password}),code('password_container_unavailable'));assert.throws(()=>f.read(owner,objectId),code('canvas_object_locked'));
});

test('chunked encrypted content is bounded and project exposes metadata only after unlock',async t=>{
 const f=fixture(t),bytes=Buffer.alloc(600000,79),source=await f.mutate(owner,'create',{password,content:{contentType:'image/webp',bytesBase64:bytes.toString('base64url')}}),object=f.create(source.binding),objectId=object.objectId;
 await f.call(owner,'unlock',{objectId,password});const first=f.read(owner,objectId,0),second=f.read(owner,objectId,1);assert.equal(first.bytes.length,512*1024);assert.deepEqual(Buffer.concat([first.bytes,second.bytes]),bytes);assert.equal(first.chunks,2);assert.throws(()=>f.read(owner,objectId,2),code('vault_content_part_invalid'));
 const item=f.objects.project({actor:owner,objectIds:[objectId]}).items[0];assert.equal(item.view.byteLength,600000);assert.ok(JSON.stringify(item).length<1000);assert.equal('bytes' in item.view,false);
});

test('negative control: disabling the placement gate leaks the real provider; the same locked oracle fails',async t=>{
 const f=fixture(t),object=f.create({providerId:'soty.apps',ref:'widget-negative-control'}),objectId=object.objectId;await f.mutate(owner,'protect',{objectId,expectedObjectRevision:1,password});
 const locked=()=>assert.equal(f.objects.project({actor:owner,objectIds:[objectId]}).items[0].view.state,'locked');locked();f.bypassProtection(true);
 assert.throws(locked,assert.AssertionError);assert.equal(f.objects.project({actor:owner,objectIds:[objectId]}).items[0].view.value,'upstream app bytes');f.bypassProtection(false);locked();
});

test('concurrent same requestId creates exactly one durable ciphertext and detects conflicting intent',async t=>{
 const f=fixture(t),args={requestId:'same_async_creation',password,content:data(secret)},first=f.call(owner,'create',args),second=f.call(owner,'create',args);
 await assert.rejects(f.call(owner,'create',{...args,content:data('other body')}),code('vault_request_conflict'));const [one,two]=await Promise.all([first,second]);assert.equal(one.current.vaultId,two.current.vaultId);assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM vault_sources').get().n),1);
 const replay=await f.call(owner,'create',args);assert.equal(replay.replayed,true);assert.deepEqual(replay.binding,one.binding);await assert.rejects(f.call(owner,'create',{...args,content:data('other body')}),code('vault_request_conflict'));
});

test('format2 upload uses bounded ciphertext parts, denies partial exposure, resumes after restart and returns exact bytes after password',{timeout:20000},async t=>{
 const f=fixture(t,{format2:true}),bytes=Buffer.alloc(1300000);for(let index=0;index<bytes.length;index++)bytes[index]=index%251;const args={requestId:'encrypted_bulk_once',password,contentType:'application/octet-stream',size:bytes.length,metadata:{kind:'file',name:'secret-bulk-filename-48a9'}};
 const begin=await f.call(owner,'upload.begin',args),vaultId=begin.current.vaultId;assert.equal(begin.upload.chunks,3);assert.equal((await f.call(owner,'upload.begin',args)).replayed,true);assert.throws(()=>f.create({providerId:'soty.vault',ref:vaultId}),code('canvas_provider_access_denied'));
 const first=bytes.subarray(0,512*1024).toString('base64url'),part=await f.call(owner,'upload.chunk',{vaultId,index:0,bytesBase64:first});assert.equal(part.replayed,false);assert.equal((await f.call(owner,'upload.chunk',{vaultId,index:0,bytesBase64:first})).replayed,true);await assert.rejects(f.call(owner,'upload.chunk',{vaultId,index:0,bytesBase64:Buffer.alloc(512*1024,9).toString('base64url')}),code('vault_chunk_reused'));await assert.rejects(f.call(friend,'upload.chunk',{vaultId,index:1,bytesBase64:first}),code('vault_owner_required'));
 await assert.rejects(f.mutate(owner,'upload.complete',{vaultId}),code('vault_upload_incomplete'));f.restart();await assert.rejects(f.call(owner,'upload.chunk',{vaultId,index:1,bytesBase64:first}),code('vault_upload_locked'));await assert.rejects(f.call(owner,'upload.resume',{vaultId,password:'incorrect-password'}),code('password_container_unavailable'));const resumed=await f.call(owner,'upload.resume',{vaultId,password});assert.deepEqual(resumed.received,[0]);
 for(let index=1;index<3;index++)await f.call(owner,'upload.chunk',{vaultId,index,bytesBase64:bytes.subarray(index*512*1024,(index+1)*512*1024).toString('base64url')});
 const completeArgs={requestId:'complete_bulk_once',vaultId},complete=await f.call(owner,'upload.complete',completeArgs);assert.equal((await f.call(owner,'upload.complete',completeArgs)).replayed,true);const object=f.create(complete.binding);assert.throws(()=>f.read(owner,object.objectId),code('canvas_object_locked'));await f.call(owner,'unlock',{objectId:object.objectId,password});const output=Buffer.concat([0,1,2].map(index=>f.read(owner,object.objectId,index).bytes));assert.deepEqual(output,bytes);assert.equal(f.objects.project({actor:owner,objectIds:[object.objectId]}).items[0].view.byteLength,bytes.length);
 assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM vault_cipher_chunks').get().n),3);for(const suffix of ['','-wal'])if(existsSync(f.vaultPath+suffix)){const raw=readFileSync(f.vaultPath+suffix);assert.equal(raw.includes(Buffer.from('secret-bulk-filename-48a9')),false);assert.equal(raw.includes(bytes.subarray(0,1000)),false);}f.restart();assert.equal((await f.call(owner,'upload.complete',completeArgs)).replayed,true,'a durable completion can be acknowledged again after the RAM upload lease is gone');await f.call(owner,'unlock',{objectId:object.objectId,password});assert.deepEqual(f.read(owner,object.objectId,2).bytes,bytes.subarray(2*512*1024));
});

test('cipher chunk AEAD binds index/version/owner despite a recomputed ciphertext hash, and a failed chunk write remains retryable',{timeout:20000},async t=>{
 const f=fixture(t,{format2:true}),size=2*512*1024,begin=await f.mutate(owner,'upload.begin',{password,contentType:'application/octet-stream',size,metadata:{kind:'file'}}),vaultId=begin.current.vaultId,data=Buffer.alloc(512*1024,3).toString('base64url');
 f.sql(db=>db.exec("CREATE TRIGGER chunk_fault BEFORE INSERT ON vault_cipher_chunks BEGIN SELECT RAISE(ABORT,'chunk_fault'); END"));await assert.rejects(f.call(owner,'upload.chunk',{vaultId,index:0,bytesBase64:data}),/chunk_fault/u);assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM vault_cipher_chunks').get().n),0);f.sql(db=>db.exec('DROP TRIGGER chunk_fault'));
 await f.call(owner,'upload.chunk',{vaultId,index:0,bytesBase64:data});await f.call(owner,'upload.chunk',{vaultId,index:1,bytesBase64:data});const complete=await f.mutate(owner,'upload.complete',{vaultId}),object=f.create(complete.binding);await f.call(owner,'unlock',{objectId:object.objectId,password});
 f.sql(db=>{const second=db.prepare('SELECT * FROM vault_cipher_chunks WHERE vault_id=? AND part_index=1').get(vaultId);db.prepare('DELETE FROM vault_cipher_chunks WHERE vault_id=? AND part_index=0').run(vaultId);db.prepare('INSERT INTO vault_cipher_chunks VALUES(?,?,0,?,?,?)').run(vaultId,second.version,second.size,second.cipher_json,second.cipher_digest);});assert.throws(()=>f.read(owner,object.objectId,0),code('vault_cipher_invalid'));
});

test('the complete 25MiB boundary crosses fifty encrypted parts, persists, and refuses one extra byte without creating a source',{timeout:30000},async t=>{
 const f=fixture(t,{format2:true}),size=25*1024*1024;await assert.rejects(f.mutate(owner,'upload.begin',{password,contentType:'application/octet-stream',size:size+1,metadata:{kind:'file'}}),code('vault_layout_invalid'));assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM vault_sources').get().n),0);
 const begin=await f.mutate(owner,'upload.begin',{password,contentType:'application/octet-stream',size,metadata:{kind:'file',name:'private-full-boundary.bin'}}),vaultId=begin.current.vaultId;assert.equal(begin.upload.chunks,50);
 for(let index=0;index<50;index++){const bytes=Buffer.alloc(512*1024,index+1);Buffer.from('native-vault-full-boundary-'+index).copy(bytes);await f.call(owner,'upload.chunk',{vaultId,index,bytesBase64:bytes.toString('base64url')});}
 await assert.rejects(f.call(owner,'upload.chunk',{vaultId,index:50,bytesBase64:'AA'}),code('vault_chunk_invalid'));const completed=await f.mutate(owner,'upload.complete',{vaultId}),object=f.create(completed.binding);await f.call(owner,'unlock',{objectId:object.objectId,password});let length=0;
 for(let index=0;index<50;index++){const value=f.read(owner,object.objectId,index);assert.equal(value.chunks,50);assert.equal(value.byteLength,size);assert.equal(value.bytes.length,512*1024);assert.ok(value.bytes.subarray(0,64).includes(Buffer.from('native-vault-full-boundary-'+index)));assert.equal(value.bytes.at(-1),index+1);length+=value.bytes.length;}assert.equal(length,size);
 f.restart();assert.throws(()=>f.read(owner,object.objectId),code('canvas_object_locked'));await f.call(owner,'unlock',{objectId:object.objectId,password});assert.equal(f.read(owner,object.objectId,49).bytes.at(-1),50);
 const pending=await f.mutate(owner,'upload.begin',{password,contentType:'application/octet-stream',size:1,metadata:{kind:'file'}}),pendingId=pending.current.vaultId;await f.call(owner,'upload.chunk',{vaultId:pendingId,index:0,bytesBase64:'QQ'});await assert.rejects(f.call(friend,'upload.cancel',{vaultId:pendingId}),code('vault_upload_unavailable'));assert.equal((await f.call(owner,'upload.cancel',{vaultId:pendingId})).replayed,false);assert.equal((await f.call(owner,'upload.cancel',{vaultId:pendingId})).replayed,true);assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM vault_cipher_chunks WHERE vault_id=?').get(pendingId).n),0);await assert.rejects(f.call(owner,'upload.resume',{vaultId:pendingId,password}),code('vault_upload_unavailable'));
});
