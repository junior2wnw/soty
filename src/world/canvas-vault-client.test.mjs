import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {createCanvasObjectsService} from '../../modules/canvas-objects/server.mjs';
import {createCanvasVaultService} from '../../modules/canvas-vault/server.mjs';
import {createNoteSession} from '../../modules/notes/browser/state.mjs';
import {vaultIndexedPort as indexedPort} from './test-support/vault-indexed-port.mjs';
import {hasRetainedNativeDrafts} from '../../modules/notes/browser/native-local.mjs';
import {createCanvasVaultClient,canvasVaultNoteContent} from './canvas-vault-client.mjs';
import {createCanvasVaultLocalStore,canvasVaultProfile,deriveCanvasVaultKey,sealCanvasVaultRecord,openCanvasVaultRecord,vaultBytes,vaultBase64,createCanvasVaultMemoryCache} from './canvas-vault-local.mjs';

// Actual product IDB store, crypto, Notes session, Objects and Vault all execute.
// The mechanical transport observes this store's real DB and COMMIT boundary.
const until=async predicate=>{for(let index=0;index<100;index++){if(predicate())return;await new Promise(resolve=>setImmediate(resolve));}assert.fail('Expected asynchronous guard did not settle');};
const owner={accountId:'vault_client_owner',deviceId:'vault_client_device'},viewer={accountId:'vault_viewer',deviceId:'vault_viewer_device'},password='Client only synthetic passphrase';
const secret='Private local phrase 9a041e82';
function setup(t){
  let time=1800000000000,active=true,current=true,objects,vault,client;
  const port=indexedPort(),store=createCanvasVaultLocalStore(owner.accountId,'vault-client',{origin:'http://localhost',indexedDB:port.indexedDB});
  const calls=[],timers=new Map();let timerId=0,offlineWrite=false,lostAck=false,lostRotate=false,afterRotate;
  const originals=new WeakSet([owner,viewer]),authority=(actor,callback)=>{assert.ok(originals.has(actor),'Only the original trusted actor enters server authority');return callback();};
  objects=createCanvasObjectsService({databasePath:':memory:',projectId:'vault-client',actorActive:()=>active,withActorAuthorityFence:authority,withProtectionAuthority:(value,callback)=>vault.withProtectionAuthority(value,callback)});
  vault=createCanvasVaultService({databasePath:':memory:',projectId:'vault-client',actorActive:()=>active,withActorAuthorityFence:authority,withProtectionTarget:(value,callback)=>objects.withProtectionTarget(value,callback),withObjectAuthority:(value,callback)=>objects.withObjectAuthority(value,callback),clock:()=>time});
  objects.registerProvider(vault.provider);
  const api={async request(op,args){calls.push({op,args:structuredClone(args)});
    if(op==='canvas.vault.write'&&offlineWrite)throw Object.assign(new Error('network_error'),{code:'network_error'});
    if(op==='canvas.objects.content'){const {bytes,...result}=objects.withObjectRead({actor:owner,objectId:args.objectId},context=>vault.readContent({context,index:args.index??0}));return{...result,bytesBase64:Buffer.from(bytes).toString('base64url')};}
    const result=await vault.execute({op,args:structuredClone(args),actor:owner});
    if(op==='canvas.vault.rotate'){await afterRotate?.();if(lostRotate){lostRotate=false;throw Object.assign(new Error('network_error'),{code:'network_error'});}}
    if(op==='canvas.vault.write'&&lostAck){lostAck=false;throw Object.assign(new Error('network_error'),{code:'network_error'});}return result;
  }};
  const options={api,accountId:owner.accountId,projectId:'vault-client',isCurrent:()=>current,store,clock:()=>time,setTimer:(callback,delay)=>{const id=++timerId;timers.set(id,{callback,delay});return id;},clearTimer:id=>timers.delete(id)};
  client=createCanvasVaultClient(options);
  t.after(()=>{client.dispose();store.close();vault.close();objects.close();});
  return{api,options,store,port,calls,timers,get client(){return client;},get vault(){return vault;},objects,
    offline(value){offlineWrite=value;},loseWriteAck(){lostAck=true;},loseRotateAck(){lostRotate=true;},onRotate(callback){afterRotate=callback;},changeAccount(){current=false;},revoke(){active=false;},
    advance(ms){time+=ms;for(const[id,item]of[...timers])if(item.delay<=ms){timers.delete(id);item.callback();}},
    reopen(){client.dispose();client=createCanvasVaultClient(options);return client;},
    async seed(content=canvasVaultNoteContent({title:'Protected',body:secret}),extra={}){
      const source=await vault.execute({actor:owner,op:'canvas.vault.create',args:{expectedAccountId:owner.accountId,requestId:crypto.randomUUID(),password,content}});
      const object=objects.execute({actor:owner,op:'canvas.objects.create',args:{expectedAccountId:owner.accountId,requestId:crypto.randomUUID(),binding:source.binding,presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'password'},...extra}}).current;
      calls.length=0;return object;
    }};
}
function noteApi(client,objectId){return{async request(op,args){if(op==='notes.get')return{note:await client.readNote(objectId,{fresh:true})};assert.equal(op,'notes.put');return client.writeNote(objectId,args);}};}

test('local AEAD is nonextractable, changes IV on every version, and rejects password/scope/header/cipher tampering',async()=>{
  const profile=canvasVaultProfile('owner','project','operation_1'),key=await deriveCanvasVaultKey(profile,password),header={schema:'soty.vault.local-record.v1',accountId:'owner',projectId:'project',slot:'operation_1',recordId:'record_1',purpose:'create',version:1};
  assert.equal(key.key.extractable,false);await assert.rejects(crypto.subtle.exportKey('raw',key.key));
  const a=await sealCanvasVaultRecord(key,header,{body:secret}),b=await sealCanvasVaultRecord(key,{...header,version:2},{body:secret});
  assert.notEqual(a.cipher.iv,b.cipher.iv);assert.equal((await openCanvasVaultRecord(key,a)).body,secret);
  for(const value of [secret,password])assert.equal(JSON.stringify(a).includes(value),false);
  const wrong=await deriveCanvasVaultKey(profile,'A different synthetic password');await assert.rejects(openCanvasVaultRecord(wrong,a),{code:'vault_local_password_or_data_invalid'});
  await assert.rejects(openCanvasVaultRecord(key,{...a,version:2}),{code:'vault_local_password_or_data_invalid'});
  await assert.rejects(openCanvasVaultRecord(key,{...a,accountId:'other'}),{code:'vault_local_scope_changed'});
  const bytes=vaultBytes(a.cipher.data);bytes[4]^=1;await assert.rejects(openCanvasVaultRecord(key,{...a,cipher:{...a.cipher,data:vaultBase64(bytes)}}),{code:'vault_local_password_or_data_invalid'});
});
test('actual cipher IDB waits for COMMIT, preserves CAS across reopen, and rejects plaintext fields or another account/project',async()=>{
  const port=indexedPort({gateFirstWrite:true}),options={origin:'http://localhost',indexedDB:port.indexedDB},store=createCanvasVaultLocalStore('owner','project',options);
  const profile=canvasVaultProfile('owner','project','operation_1');let complete=false;const pending=store.ensureProfile(profile).then(value=>{complete=true;return value;});await until(()=>port.gated);
  assert.equal(complete,false);assert.equal(port.rows.size,0);port.release();const actual=await pending,key=await deriveCanvasVaultKey(actual,password);
  const value=await sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId:'owner',projectId:'project',slot:'operation_1',recordId:'record_1',purpose:'create',version:1},{body:secret});
  assert.equal(await store.put(value,0),true);assert.equal(await store.put(value,0),false);await assert.rejects(store.put({...value,password},1),{code:'vault_local_invalid'});store.close();
  const reopened=createCanvasVaultLocalStore('owner','project',options);assert.equal((await openCanvasVaultRecord(key,await reopened.get('record_1'))).body,secret);
  for(const[account,project]of[['other','project'],['owner','other']]){const other=createCanvasVaultLocalStore(account,project,options);assert.deepEqual(await other.list(),[]);other.close();}
  for(const row of port.rows.values()){assert.equal(JSON.stringify(row).includes(secret),false);assert.equal(JSON.stringify(row).includes(password),false);}reopened.close();
});
test('locked card content reads require explicit unlock, wrong password does not authorize, and reopening has no RAM key',async t=>{
  const f=setup(t),object=await f.seed();assert.equal(f.client.isUnlocked(object.objectId),false);
  await assert.rejects(f.client.readNote(object.objectId),{code:'canvas_object_locked'});assert.equal(f.calls.length,0);
  await assert.rejects(f.client.unlock(object.objectId,'Wrong test password'),{code:'password_container_unavailable'});assert.equal(f.calls.some(call=>call.op==='canvas.objects.content'),false);
  await f.client.unlock(object.objectId,password);assert.equal((await f.client.readNote(object.objectId)).body,secret);assert.equal(f.client.isUnlocked(object.objectId),true);
  const reopened=f.reopen();assert.equal(reopened.isUnlocked(object.objectId),false);const before=f.calls.length;await assert.rejects(reopened.readNote(object.objectId),{code:'canvas_object_locked'});assert.equal(f.calls.length,before);
});
test('encrypted NoteSession outbox survives a client reopen and retries lost write ACK with the identical request ID',async t=>{
  const f=setup(t),object=await f.seed();await f.client.unlock(object.objectId,password);
  const initial=await f.client.readNote(object.objectId),session=createNoteSession({api:noteApi(f.client,object.objectId),accountId:owner.accountId,note:initial,store:f.client.draftStore(object.objectId),cache:createCanvasVaultMemoryCache(),debounceMs:60000});
  session.edit({body:'Private edited outbox a80c31'});f.loseWriteAck();await session.flush();assert.equal(session.state().dirty,true);assert.equal(session.state().localDurable,true);
  const first=f.calls.find(call=>call.op==='canvas.vault.write').args.requestId;session.dispose();f.advance(2000);const reopened=f.reopen();
  await reopened.unlock(object.objectId,password);const drafts=await reopened.draftStore(object.objectId).list();assert.equal(drafts.length,1);assert.equal(drafts[0].pending.mutationId,first);
  for(const row of f.port.rows.values()){assert.equal(JSON.stringify(row).includes('Private edited outbox'),false);assert.equal(JSON.stringify(row).includes(password),false);}
  const recovered=createNoteSession({api:noteApi(reopened,object.objectId),accountId:owner.accountId,note:initial,draft:drafts[0],store:reopened.draftStore(object.objectId),cache:createCanvasVaultMemoryCache(),debounceMs:60000});
  await recovered.flush();assert.equal(recovered.state().dirty,false);assert.equal((await reopened.readNote(object.objectId)).body,'Private edited outbox a80c31');
  assert.deepEqual(f.calls.filter(call=>call.op==='canvas.vault.write').map(call=>call.args.requestId),[first,first]);
  assert.equal((await f.store.list()).filter(value=>['draft','write'].includes(value.purpose)).length,0,'An acknowledged recovered outbox must not leave an encrypted write that blocks a later password rotation');recovered.dispose();
});

test('a restored encrypted conflict saves the explicit local choice and retires only its settled write intents',async t=>{
  const f=setup(t),object=await f.seed();await f.client.unlock(object.objectId,password);const note=await f.client.readNote(object.objectId),api=noteApi(f.client,object.objectId);
  const create=extra=>{const session=createNoteSession({api,accountId:owner.accountId,note,store:f.client.draftStore(object.objectId),cache:createCanvasVaultMemoryCache(),debounceMs:60000,...extra});t.after(()=>session.dispose());return session;};
  const local=create(),remote=create();f.offline(true);local.edit({body:'Chosen encrypted local draft'});await local.flush();f.offline(false);
  remote.edit({body:'Remote version before choice'});await remote.flush();await local.flush();assert.equal(local.state().conflict,true);
  local.dispose();await local.flushLocal();const [draft]=await f.client.draftStore(object.objectId).list(),restored=create({draft});
  assert.equal(typeof restored.keepLocal,'function','Encrypted recovery needs the same explicit local resolution');await restored.keepLocal();
  assert.equal((await f.client.readNote(object.objectId)).body,'Chosen encrypted local draft');assert.equal((await f.client.status(object.objectId)).revision,3);
  assert.equal(restored.state().dirty,false);assert.equal((await f.store.list()).filter(value=>['draft','write'].includes(value.purpose)).length,0);
  assert.equal([...f.port.rows.values()].some(value=>JSON.stringify(value).includes('Chosen encrypted local draft')),false);
});

test('settled draft cleanup preserves an unrelated encrypted unacknowledged write',async t=>{
  const f=setup(t),object=await f.seed();await f.client.unlock(object.objectId,password);const note=await f.client.readNote(object.objectId),api=noteApi(f.client,object.objectId);
  const create=()=>{const session=createNoteSession({api,accountId:owner.accountId,note,store:f.client.draftStore(object.objectId),debounceMs:60000});t.after(()=>session.dispose());return session;};
  const local=create(),remote=create();remote.edit({body:'Remote'});await remote.flush();local.edit({body:'Explicit local'});await local.flush();assert.equal(local.state().conflict,true);
  const unrelatedId=crypto.randomUUID(),{title,items,color,pinned,state}=note;f.offline(true);
  await assert.rejects(f.client.writeNote(object.objectId,{noteId:note.noteId,mutationId:unrelatedId,expectedRevision:2,title,items,color,pinned,state,body:'Other unacknowledged write'}),{code:'network_error'});f.offline(false);
  const before=await f.store.get('write_'+unrelatedId);await local.keepLocal();const remaining=(await f.store.list()).filter(value=>value.purpose==='write');
  assert.deepEqual(remaining.map(value=>value.recordId),['write_'+unrelatedId]);assert.deepEqual(remaining[0],before);assert.equal((await f.client.readNote(object.objectId)).body,'Explicit local');
});

test('explicit resolution refreshes a head changed by another SDK without relaxing the ordinary read revision fence',async t=>{
  const f=setup(t),object=await f.seed();await f.client.unlock(object.objectId,password);const note=await f.client.readNote(object.objectId),local=createNoteSession({api:noteApi(f.client,object.objectId),accountId:owner.accountId,note,store:f.client.draftStore(object.objectId),debounceMs:60000});t.after(()=>local.dispose());
  const other=createCanvasVaultClient(f.options);t.after(()=>other.dispose());await other.unlock(object.objectId,password);const remote=createNoteSession({api:noteApi(other,object.objectId),accountId:owner.accountId,note,store:other.draftStore(object.objectId),debounceMs:60000});t.after(()=>remote.dispose());
  remote.edit({body:'New head from another SDK'});await remote.flush();await assert.rejects(f.client.readNote(object.objectId),{code:'vault_content_changed'},'The default read retains its original expected revision');
  local.edit({body:'Chosen after an external head change'});await local.flush();assert.equal(local.state().conflict,true);await local.keepLocal();
  assert.equal(local.state().conflict,false,'A deliberate choice must authenticate the current head and save with its exact revision');assert.equal(local.state().dirty,false);assert.equal((await f.client.readNote(object.objectId)).body,'Chosen after an external head change');assert.equal((await f.client.status(object.objectId)).revision,3);
});
test('TTL/relock/account switch synchronously notify scrub consumers and suppress late content reads',async t=>{
  const f=setup(t),object=await f.seed(),events=[];f.client.subscribe((id,reason)=>events.push({id,reason}));await f.client.unlock(object.objectId,password,{ttlMs:1000});
  f.advance(1001);assert.equal(f.client.isUnlocked(object.objectId),false);assert.equal(events.at(-1).reason,'expired');const before=f.calls.length;await assert.rejects(f.client.readNote(object.objectId),{code:'canvas_object_locked'});assert.equal(f.calls.length,before);
  await f.client.unlock(object.objectId,password);await f.client.relock(object.objectId);assert.equal(f.client.isUnlocked(object.objectId),false);assert.ok(events.some(event=>event.reason==='relock'));
  await f.client.unlock(object.objectId,password);f.changeAccount();assert.equal(f.client.isUnlocked(object.objectId),false);assert.equal(events.at(-1).reason,'account');
});
test('secure-crypto/storage failure occurs before any password mutation, and password never substitutes for an Object ACL',async t=>{
  const f=setup(t),object=await f.seed();
  const insecure=createCanvasVaultClient({...f.options,crypto:{}});await assert.rejects(insecure.unlock(object.objectId,password),{code:'vault_secure_context_required'});assert.equal(f.calls.length,0);insecure.dispose();
  const failed=createCanvasVaultClient({...f.options,store:{ensureProfile:async()=>{throw Object.assign(new Error('Storage disabled'),{code:'vault_local_unavailable'});}}});
  await assert.rejects(failed.unlock(object.objectId,password),{code:'vault_local_unavailable'});assert.equal(f.calls.length,0);failed.dispose();
  const outsider=createCanvasVaultClient({...f.options,accountId:viewer.accountId,store:createCanvasVaultLocalStore(viewer.accountId,'vault-client',{origin:'http://localhost',indexedDB:f.port.indexedDB}),api:{request:(op,args)=>f.vault.execute({actor:viewer,op,args})}});
  await assert.rejects(outsider.unlock(object.objectId,password),{code:'canvas_object_unavailable'});assert.equal(outsider.isUnlocked(object.objectId),false);outsider.dispose();
});
test('chunk metadata changes and source/TTL changes between parts never return a partial plaintext payload',async t=>{
  const f=setup(t),object=await f.seed({contentType:'audio/webm',bytesBase64:Buffer.alloc(700000,7).toString('base64url')});
  const api=f.options.api;let corrupt=false,expire=false,reads=0;
  const client=createCanvasVaultClient({...f.options,api:{async request(op,args){const value=await api.request(op,args);if(op==='canvas.objects.content'){reads++;if(expire&&args.index===0)f.advance(300001);if(corrupt&&args.index===1)return{...value,revision:value.revision+1};}return value;}}});t.after(()=>client.dispose());
  await client.unlock(object.objectId,password);corrupt=true;await assert.rejects(client.readContent(object.objectId),{code:'vault_content_changed'});assert.equal(reads,2);
  corrupt=false;expire=true;await assert.rejects(client.readContent(object.objectId),{code:'canvas_object_locked'});
});
test('negative encryption control cannot pass the real AEAD readback oracle merely by base64-encoding plaintext',async()=>{
  const profile=canvasVaultProfile('owner','project','operation_1'),session=await deriveCanvasVaultKey(profile,password),header={schema:'soty.vault.local-record.v1',accountId:'owner',projectId:'project',slot:'operation_1',recordId:'record_1',purpose:'create',version:1};
  const broken={getRandomValues:bytes=>crypto.getRandomValues(bytes),subtle:{encrypt:async(_algorithm,_key,bytes)=>bytes.slice().buffer}};
  const record=await sealCanvasVaultRecord(session,header,{body:secret},{crypto:broken});assert.equal(JSON.parse(new TextDecoder().decode(vaultBytes(record.cipher.data))).body,secret);
  await assert.rejects(openCanvasVaultRecord(session,record),{code:'vault_local_password_or_data_invalid'});
});

test('full native note metadata keeps its original Source identity while RAM and CAS use Vault revision',async t=>{
  const f=setup(t),originalId=crypto.randomUUID(),object=await f.seed(canvasVaultNoteContent({noteId:originalId,title:'Converted shape',body:secret,revision:37,createdAt:123,updatedAt:124}));
  await f.client.unlock(object.objectId,password);const note=await f.client.readNote(object.objectId);assert.notEqual(note.noteId,originalId);assert.equal(note.revision,1);assert.equal(note.createdAt,123);
  const args={noteId:note.noteId,mutationId:crypto.randomUUID(),expectedRevision:1,title:note.title,body:'Protected original Source update',items:[],color:'plain',pinned:false,state:'active'};
  await f.client.writeNote(object.objectId,args);f.advance(2000);await f.client.writeNote(object.objectId,args);
  const payload=JSON.parse(new TextDecoder().decode((await f.client.readContent(object.objectId)).bytes));assert.equal(payload.noteId,originalId);assert.equal(payload.createdAt,123);assert.equal(payload.revision,2);
  const writes=f.calls.filter(call=>call.op==='canvas.vault.write');assert.deepEqual(writes[0].args,writes[1].args,'A later replay retains the exact original payload timestamp/identity bytes');
});
test('password rotation changes the durable local profile without persisting passwords and new controller uses only the new password',async t=>{
  const f=setup(t),object=await f.seed(),nextPassword='A newly rotated client password';await f.client.unlock(object.objectId,password);
  const before=[...f.port.rows.values()].find(row=>row.value.schema==='soty.vault.local-key.v1').value.profileId;
  await f.client.rotate(object.objectId,{oldPassword:password,newPassword:nextPassword});assert.equal((await f.client.readNote(object.objectId)).body,secret);
  const after=[...f.port.rows.values()].find(row=>row.value.schema==='soty.vault.local-key.v1').value.profileId;assert.notEqual(after,before);assert.equal((await f.store.list()).length,0);
  for(const row of f.port.rows.values())for(const marker of[password,nextPassword,secret])assert.equal(JSON.stringify(row).includes(marker),false);
  const reopened=f.reopen();await assert.rejects(reopened.unlock(object.objectId,password),{code:'password_container_unavailable'});await reopened.unlock(object.objectId,nextPassword);assert.equal((await reopened.readNote(object.objectId)).body,secret);
});
test('lost rotate ACK resumes its encrypted intent with original request ID and migrates no plaintext or duplicate Source version',async t=>{
  const f=setup(t),object=await f.seed(),nextPassword='Lost ACK rotated password';await f.client.unlock(object.objectId,password);f.loseRotateAck();
  await assert.rejects(f.client.rotate(object.objectId,{oldPassword:password,newPassword:nextPassword}),{code:'network_error'});assert.ok((await f.store.list(object.objectId)).some(record=>record.purpose==='rotate'));
  f.advance(2000);const reopened=f.reopen();const receipt=await reopened.rotate(object.objectId,{oldPassword:password,newPassword:nextPassword});assert.equal(receipt.replayed,true);assert.equal(receipt.current.revision,2);
  const ids=f.calls.filter(call=>call.op==='canvas.vault.rotate').map(call=>call.args.requestId);assert.equal(ids.length,2);assert.equal(new Set(ids).size,1);assert.equal((await reopened.readNote(object.objectId)).body,secret);
});
test('wrong old password cannot poison a rotation journal, and pending encrypted edits block rotation before server mutation',async t=>{
  const f=setup(t),object=await f.seed(),nextPassword='Valid after wrong old password';
  await assert.rejects(f.client.rotate(object.objectId,{oldPassword:'Wrong current password',newPassword:nextPassword}),{code:'password_container_unavailable'});assert.equal((await f.store.list()).length,0);
  await f.client.unlock(object.objectId,password);const note=await f.client.readNote(object.objectId),session=createNoteSession({api:noteApi(f.client,object.objectId),accountId:owner.accountId,note,store:f.client.draftStore(object.objectId),cache:createCanvasVaultMemoryCache(),debounceMs:60000});
  f.offline(true);session.edit({body:'Pending protected edit'});await session.flush();const before=f.calls.filter(call=>call.op==='canvas.vault.rotate').length;
  await assert.rejects(f.client.rotate(object.objectId,{oldPassword:password,newPassword:nextPassword}),{code:'vault_local_draft_pending'});assert.equal(f.calls.filter(call=>call.op==='canvas.vault.rotate').length,before);
  f.offline(false);await session.flush();session.dispose();await f.client.rotate(object.objectId,{oldPassword:password,newPassword:nextPassword});assert.equal((await f.client.readNote(object.objectId)).body,'Pending protected edit');
});
test('late text with a delayed surface retirement is retained without a false old-key ACK and recovered under the new Source key',async t=>{
  const f=setup(t),object=await f.seed(),nextPassword='Raced profile rotated password';await f.client.unlock(object.objectId,password);
  const note=await f.client.readNote(object.objectId),oldStore=f.client.draftStore(object.objectId),draft={note:{...note,body:'Race preserved private branch'},branchId:crypto.randomUUID(),generation:1,committedGeneration:0,pending:null,conflict:false,savedAt:1};
  // Deliberately delayed renderer retirement: a real NoteSession has not yet
  // received the SDK lock event. Thus text is ACTUALLY accepted in RAM, not an
  // edit ignored by a suspended session. IDB remains the cross-realm fence.
  const editor=createNoteSession({api:noteApi(f.client,object.objectId),accountId:owner.accountId,note,store:oldStore,cache:createCanvasVaultMemoryCache(),debounceMs:60000});t.after(()=>editor.dispose());
  f.onRotate(async()=>{editor.edit({body:draft.note.body});assert.equal(editor.state().note.body,draft.note.body);assert.equal(editor.hasUnsavedChanges(),true);
    await assert.rejects(editor.flushLocal(),{code:'vault_local_rotation_pending'});assert.equal(editor.state().localDurable,false);editor.dispose();await new Promise(resolve=>setImmediate(resolve));assert.equal(hasRetainedNativeDrafts(),true);});
  await f.client.rotate(object.objectId,{oldPassword:password,newPassword:nextPassword});assert.equal(hasRetainedNativeDrafts(),false);
  const recovered=await f.client.draftStore(object.objectId).list();assert.equal(recovered[0].note.body,'Race preserved private branch');
  await assert.rejects(oldStore.put({...draft,branchId:crypto.randomUUID(),savedAt:2}),{code:'vault_local_scope_changed'});
  assert.equal(f.calls.some(value=>value.op==='canvas.vault.write'),false,'RAM recovery is local encryption, not a Source replay');
  for(const row of f.port.rows.values())assert.equal(JSON.stringify(row).includes('Race preserved'),false);
});
