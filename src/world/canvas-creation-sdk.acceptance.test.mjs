import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {DatabaseSync} from 'node:sqlite';
import {join} from 'node:path';
import {createHttpApp} from '../../server/http-app.js';
import {nativeHttpFixture,nativeIdentity,good} from '../../server/test/support/native-capability-http.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {createNativeDraftStore} from '../../modules/notes/browser/native-local.mjs';
import {createNoteCache,createIndexedNoteCacheStorage,readNoteWithCache} from '../../modules/notes/browser/cache.mjs';
import {pngFixture} from '../../modules/world/test/avatar-fixtures.mjs';
import {createCanvasCreateStore,createCanvasCreateFlow} from './canvas-create-flow.mjs';
import {createCanvasVaultLocalStore} from './canvas-vault-local.mjs';
import {createCanvasVaultClient} from './canvas-vault-client.mjs';
import {createNativeCacheTransition} from './native-cache-transition.mjs';

const password='Synthetic signed SDK acceptance password1',newPassword='Synthetic signed SDK acceptance password2';
const originalBody='SDK_OLD_CREATION_INTENT_PRIVATE_20261104',latestBody='Latest actual Source document differs from the old Creation intent';
const id=()=>randomUUID(),sha=value=>createHash('sha256').update(value).digest('hex');
const pause=()=>new Promise(resolve=>setTimeout(resolve,5));
async function until(predicate){const end=Date.now()+10000;while(!predicate()){assert.ok(Date.now()<end,'Expected real SDK/IDB boundary');await pause();}}
function unwrap(result){if(result.ok)return result;throw Object.assign(new Error(result.error?.code??'test_rpc_failed'),{code:result.error?.code});}

async function fixture(t,{kind='note'}={}){
  const http=await nativeHttpFixture(t,{appFactory:(dist,options)=>createHttpApp(dist,{...options,allowNativeVaultMigration:true})});
  const owner=nativeIdentity('Independent creation owner'),foreign=nativeIdentity('Independent unrelated owner');
  const ownerState=await http.bootstrap(owner),foreignState=await http.bootstrap(foreign),accountId=ownerState.accountId,projectId='soty';
  let active=true,loss=true,afterRequest=null,hold=null;
  const calls=[],port=indexedNativePort(),origin=http.origin;
  const api={async request(operation,args={}){
    calls.push({operation,mutationId:args.mutationId,objectId:args.objectId});
    const value=unwrap(await http.call(owner,operation,args));
    if(loss&&operation===(kind==='note'?'notes.put':'materials.commit')){loss=false;throw Object.assign(new Error('network_timeout'),{code:'network_timeout'});}
    if(hold&&!hold.used&&hold.predicate(operation,args)){hold.used=true;hold.hit=true;await hold.promise;}
    if(afterRequest)await afterRequest(operation,args,value);
    return value;
  }};
  const creation=createCanvasCreateStore(accountId,projectId,{origin,indexedDB:port.indexedDB});
  const drafts=createNativeDraftStore(accountId,projectId,{origin,indexedDB:port.indexedDB});
  const cache=createNoteCache(accountId,projectId,{origin,storage:createIndexedNoteCacheStorage(port.indexedDB)});
  const cipher=createCanvasVaultLocalStore(accountId,projectId,{origin,indexedDB:port.indexedDB});
  let placements=0;
  const creator=createCanvasCreateFlow({api,accountId,projectId,isCurrent:()=>active,store:creation,
    place:async()=>{placements++;throw new Error('Source ACK was lost; placement cannot run');}});
  if(kind==='note')await assert.rejects(creator.startNote({title:'Original Source-stage creation title',body:originalBody}),{code:'network_timeout'});
  else await assert.rejects(creator.startFile(new Blob([pngFixture()],{type:'image/png'}),{kind:'image'}),{code:'network_timeout'});
  const records=await creation.list();assert.equal(records.length,1);const original=records[0];assert.equal(original.stage,'source');assert.equal(placements,0);
  const source={providerId:kind==='note'?'soty.notes':'soty.materials',ref:kind==='note'?original.sourceIntent.noteId:original.sourceIntent.materialId,ownerId:accountId};
  if(kind==='note'){
    const existing=await api.request('notes.get',{expectedAccountId:accountId,noteId:source.ref});assert.equal(existing.note.body,originalBody);
    await api.request('notes.put',{expectedAccountId:accountId,...original.sourceIntent,mutationId:id(),expectedRevision:1,body:latestBody});
    await readNoteWithCache({api,accountId,noteId:source.ref,cache});
  }
  const object=unwrap(await http.call(owner,'canvas.objects.create',{expectedAccountId:accountId,requestId:id(),binding:{providerId:source.providerId,ref:source.ref},
    presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}})).current;
  const clients=[],transitions=[];
  const client=(options={})=>{const value=createCanvasVaultClient({api,accountId,projectId,isCurrent:()=>active,store:cipher,...options});clients.push(value);return value;};
  const sdk=client();
  const transition=(vault=sdk,options={})=>{const value=createNativeCacheTransition({api,accountId,projectId,isCurrent:()=>active,vault,store:drafts,cache,creationStore:creation,...options});value.remember(object.objectId,source);transitions.push(value);return value;};
  const flow=transition();
  const call=(actor,operation,args={})=>http.call(actor,operation,{expectedAccountId:actor===owner?accountId:foreignState.accountId,...args});
  const prepare=()=>flow.prepare({objectId:object.objectId,expectedObjectRevision:object.revision,operationId:'sdk_native_'+original.operationId});
  async function convert(){const prepared=await prepare();assert.equal(prepared.state,'ready',JSON.stringify(prepared));
    assert.equal((await creation.sourceFence(source)).state,'pending');assert.equal((await drafts.native.gate(source.providerId,source.ref)).state,'pending');
    const result=await sdk.protect(object.objectId,object.revision,password,{requestId:prepared.ticket.operationId});assert.equal(result.protected,true);
    const pending=await flow.finalize({ticket:prepared.ticket,result});assert.equal(pending.server,'protected');assert.equal(pending.device,'pending');assert.equal(pending.protectionComplete,false);
    assert.equal((await creation.get(original.operationId)).stage,'source');return prepared;
  }
  function holdRequest(predicate){let release;hold={predicate,used:false,hit:false,promise:new Promise(resolve=>{release=resolve;})};hold.release=release;return{get hit(){return hold.hit;},release(){release();}};}
  const close=()=>{active=false;hold?.release?.();for(const value of transitions)value.dispose();for(const value of clients)value.dispose();creator.dispose();creation.close();cipher.close();};
  t.after(()=>{while(port.held)port.release();close();});
  const readVersion=relative=>{const db=new DatabaseSync(join(http.directory,...relative),{readOnly:true});try{return Number(db.prepare('PRAGMA user_version').get().user_version);}finally{db.close();}};
  return{http,owner,foreign,accountId,foreignAccount:foreignState.accountId,deviceId:ownerState.deviceId,api,calls,port,origin,creation,drafts,cache,cipher,creator,original,records,source,object,
    sdk,flow,client,transition,call,prepare,convert,holdRequest,setAfterRequest:value=>{afterRequest=value;},setActive:value=>{active=value;},close,readVersion};
}

test('independent signed Root: lost Source ACK →both writer gates→pending→SDK unlock→cipher COMMIT→exact retirement→reopen restoration',{timeout:40000},async t=>{
  const f=await fixture(t);assert.deepEqual([f.readVersion(['notes','notes.sqlite']),f.readVersion(['canvas','vault.sqlite'])],[3,2]);
  assert.throws(()=>f.http.app.locals.canvasObjectsService.execute({actor:{accountId:f.accountId,deviceId:f.deviceId},op:'canvas.objects.get',
    args:{expectedAccountId:f.accountId,objectId:f.object.objectId}}),{code:'connect_authority_actor_invalid'});
  await f.convert();const writeCount=f.calls.filter(value=>value.operation==='notes.put').length;
  await assert.rejects(f.sdk.archiveCreationIntents(f.object.objectId,f.source,f.records),{code:'canvas_object_locked'});
  await f.sdk.unlock(f.object.objectId,password);
  assert.equal(f.port.databases.get('soty-canvas-vault-local-v1').version,3,'approved Source-scoped metadata requires the v3 local layout');
  assert.equal(f.port.databases.get('soty-canvas-create-v1').version,2);
  assert.equal(f.port.databases.get('soty-notes-drafts-v1').version,2);
  let heldManifest=false,heldRetirement=false,completed=false;
  const cipher={...f.cipher,put(record,version){if(record.recordId.startsWith('creation_manifest_')&&!heldManifest){heldManifest=true;f.port.hold('soty-canvas-vault-local-v1','cipher');}return f.cipher.put(record,version);}};
  const sdk=f.client({store:cipher}),creation={...f.creation,retireSourceIntents(...args){heldRetirement=true;f.port.hold('soty-canvas-create-v1','operations');return f.creation.retireSourceIntents(...args);}};
  await sdk.unlock(f.object.objectId,password);const flow=f.transition(sdk,{creationStore:creation});
  const pending=flow.resumeAfterUnlock(f.object.objectId).then(value=>{completed=true;return value;});void pending.catch(()=>{});
  try{
    await until(()=>heldManifest&&f.port.held);assert.equal(completed,false);assert.equal((await f.creation.get(f.original.operationId)).sourceIntent.body,originalBody);
    assert.equal(f.port.rows('soty-canvas-vault-local-v1','cipher').some(row=>row.value?.recordId?.startsWith('creation_manifest_')),false);
    f.port.release();await until(()=>heldRetirement&&f.port.held);assert.equal(completed,false);
    assert.equal(f.port.rows('soty-canvas-create-v1','operations')[0].value.sourceIntent.body,originalBody,'Creation request success cannot precede retirement COMMIT');
    f.port.release();const result=await pending;assert.equal(result.protectionComplete,true,JSON.stringify(result));
  }finally{while(f.port.held)f.port.release();await pending.catch(()=>{});}
  assert.equal(await f.creation.get(f.original.operationId),null);assert.deepEqual(await f.creator.listPending(),[]);
  assert.equal((await f.creation.sourceFence(f.source)).state,'sealed');assert.equal((await f.drafts.native.gate(f.source.providerId,f.source.ref)).state,'sealed');
  assert.equal(f.calls.filter(value=>value.operation==='notes.put').length,writeCount);await assert.rejects(f.creator.resume(f.original.operationId));
  assert.equal(await f.creation.get(f.original.operationId),null);assert.equal(f.calls.filter(value=>value.operation==='notes.put').length,writeCount);
  assert.equal((await f.call(f.owner,'notes.get',{noteId:f.source.ref})).error.code,'notes_note_sealed');
  assert.equal((await sdk.readNote(f.object.objectId)).body,latestBody);
  const archive=(await sdk.creationRecoveries(f.object.objectId))[0];assert.deepEqual(archive.records,f.records);
  const raw=JSON.stringify(f.port.rows('soty-canvas-vault-local-v1','cipher'));for(const value of [originalBody,f.original.intentHash,password])assert.equal(raw.includes(value),false);
  sdk.dispose();f.sdk.dispose();flow.dispose();f.flow.dispose();
  f.cipher.close();f.creation.close();await f.http.restart({enabled:true});
  const reopenedCipher=createCanvasVaultLocalStore(f.accountId,'soty',{origin:f.origin,indexedDB:f.port.indexedDB}),reopenedCreation=createCanvasCreateStore(f.accountId,'soty',{origin:f.origin,indexedDB:f.port.indexedDB});
  t.after(()=>{reopenedCipher.close();reopenedCreation.close();});
  assert.equal(await reopenedCreation.get(f.original.operationId),null);assert.equal((await reopenedCreation.sourceFence(f.source)).state,'sealed');
  const reopened=f.client({store:reopenedCipher});await assert.rejects(reopened.creationRecoveries(f.object.objectId),{code:'canvas_object_locked'});
  await reopened.unlock(f.object.objectId,password);assert.deepEqual((await reopened.creationRecoveries(f.object.objectId))[0].records,f.records);
  const restored=await reopened.restoreCreationRecovery(f.object.objectId,archive.archiveId,f.original.operationId),draft=(await reopened.draftStore(f.object.objectId).list()).find(value=>value.branchId===restored.branchId);
  assert.equal(draft.note.body,originalBody);assert.equal(draft.pending,null);assert.equal(draft.committedGeneration,0);assert.notEqual(draft.note.noteId,f.source.ref);
  assert.equal(f.calls.filter(value=>value.operation==='notes.put').length,writeCount,'Explicit restore creates a NEW Vault draft, never old Notes replay');
});

test('SDK requires actual local server-validated unlock; wrong/foreign/mismatched Source cannot archive',{timeout:30000},async t=>{
  const f=await fixture(t);await f.convert();
  good(await f.call(f.owner,'canvas.vault.unlock',{objectId:f.object.objectId,password}));assert.equal(f.sdk.isUnlocked(f.object.objectId),false);
  await assert.rejects(f.sdk.archiveCreationIntents(f.object.objectId,f.source,f.records),{code:'canvas_object_locked'});
  await assert.rejects(f.sdk.unlock(f.object.objectId,password+' wrong'));assert.equal(f.sdk.isUnlocked(f.object.objectId),false);
  assert.equal((await f.call(f.foreign,'canvas.vault.unlock',{objectId:f.object.objectId,password})).ok,false);
  assert.equal((await f.call(f.foreign,'canvas.objects.create',{requestId:id(),binding:{providerId:'soty.notes',ref:f.source.ref},presentation:{mask:{shape:'rect'}},
    visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}})).ok,false);
  await f.sdk.unlock(f.object.objectId,password);
  for(const source of [{...f.source,ownerId:f.foreignAccount},{...f.source,ref:'another_owned_source_123'}])
    await assert.rejects(f.sdk.archiveCreationIntents(f.object.objectId,source,f.records),{code:'vault_native_recovery_scope'});
  assert.equal((await f.creation.get(f.original.operationId)).sourceIntent.body,originalBody);assert.equal((await f.cipher.list()).length,0);
});

test('a client without encrypted Creation support retains the old truthful pending state before Source conversion',{timeout:30000},async t=>{
  const f=await fixture(t),old={...f.sdk};delete old.archiveCreationIntents;delete old.verifyCreationRecoveries;
  const flow=f.transition(old),result=await flow.prepare({objectId:f.object.objectId,expectedObjectRevision:f.object.revision,operationId:'old_sdk_no_creation_port'});
  assert.equal(result.state,'pending');assert.equal(result.reason,'native_creation_source_ack_pending');assert.equal(result.protectionComplete,false);
  assert.equal(await f.creation.sourceFence(f.source),null);assert.equal(await f.drafts.native.gate(f.source.providerId,f.source.ref),null);
  assert.equal((await f.call(f.owner,'notes.get',{noteId:f.source.ref})).note.body,latestBody);
  assert.equal((await f.creation.get(f.original.operationId)).sourceIntent.body,originalBody);
  assert.equal(f.calls.some(value=>value.operation==='canvas.vault.protect'),false);
});

test('relock/account loss between archive and actual SDK verify preserves the original plaintext quarantine',{timeout:40000},async t=>{
  for(const reason of ['relock','account'])await t.test(reason,async t=>{
    const f=await fixture(t);await f.convert();await f.sdk.unlock(f.object.objectId,password);
    let archived=false;
    const facade={...f.sdk,async archiveCreationIntents(...args){const receipt=await f.sdk.archiveCreationIntents(...args);archived=true;
      if(reason==='relock')await f.sdk.relock(f.object.objectId);else f.setActive(false);return receipt;}};
    const flow=f.transition(facade),result=await flow.resumeAfterUnlock(f.object.objectId);
    assert.equal(archived,true);assert.equal(result.protectionComplete,false);assert.equal(result.device,'pending');
    assert.equal((await f.creation.get(f.original.operationId)).sourceIntent.body,originalBody);
    assert.equal((await f.creation.sourceFence(f.source)).state,'pending');
  });
});

test('a real second SDK password/profile rotation during verify rejects the held old response and retains plaintext',{timeout:40000},async t=>{
  const f=await fixture(t);await f.convert();await f.sdk.unlock(f.object.objectId,password);
  let verifying=false;
  const hold=f.holdRequest(operation=>verifying&&operation==='canvas.vault.status');
  const facade={...f.sdk,async verifyCreationRecoveries(...args){verifying=true;return f.sdk.verifyCreationRecoveries(...args);}};
  const flow=f.transition(facade),pending=flow.resumeAfterUnlock(f.object.objectId);void pending.catch(()=>{});
  try{
    await until(()=>hold.hit);const second=f.client();await second.unlock(f.object.objectId,password);
    await second.rotate(f.object.objectId,{oldPassword:password,newPassword});hold.release();
    const result=await pending;assert.equal(result.protectionComplete,false);assert.equal(result.device,'pending');
    assert.equal((await f.creation.get(f.original.operationId)).sourceIntent.body,originalBody);
    const nextFlow=f.transition(second),complete=await nextFlow.resumeAfterUnlock(f.object.objectId);assert.equal(complete.protectionComplete,true,JSON.stringify(complete));
    assert.equal(await f.creation.get(f.original.operationId),null);assert.deepEqual((await second.creationRecoveries(f.object.objectId))[0].records,f.records);
  }finally{hold.release();await pending.catch(()=>{});}
});

test('a held read-only Creation copy cannot stall another SDK rotation or return its old authorized snapshot',{timeout:15000},async t=>{
  const f=await fixture(t);await f.convert();await f.sdk.unlock(f.object.objectId,password);
  assert.equal((await f.flow.resumeAfterUnlock(f.object.objectId)).protectionComplete,true);
  const hold=f.holdRequest(operation=>operation==='canvas.vault.status'),reading=f.sdk.creationRecoveries(f.object.objectId);void reading.catch(()=>{});
  try{
    await until(()=>hold.hit);const second=f.client();await second.unlock(f.object.objectId,password);
    // The old read is deliberately still held. A readonly UI must not own the
    // writer drain or make a different current SDK wait for its network reply.
    await second.rotate(f.object.objectId,{oldPassword:password,newPassword});
    hold.release();await assert.rejects(reading,error=>/locked|scope_changed|access_changed/u.test(error.code??''));
    assert.deepEqual((await second.creationRecoveries(f.object.objectId))[0].records,f.records);
    assert.equal((await second.readNote(f.object.objectId)).body,latestBody);
  }finally{hold.release();await reading.catch(()=>{});}
});

async function withoutCipher(f,transition=createNativeCacheTransition){
  await f.convert();await f.sdk.unlock(f.object.objectId,password);
  const receipt=await f.sdk.archiveCreationIntents(f.object.objectId,f.source,f.records);
  for(const row of await f.cipher.list())await f.cipher.remove(row.recordId,row.version);
  const facade={...f.sdk,archiveCreationIntents:async()=>structuredClone(receipt)},flow=transition({api:f.api,accountId:f.accountId,projectId:'soty',isCurrent:()=>true,
    vault:facade,store:f.drafts,cache:f.cache,creationStore:f.creation});flow.remember(f.object.objectId,f.source);
  try{return await flow.resumeAfterUnlock(f.object.objectId);}finally{flow.dispose();}
}
const quarantineOracle=(result,record)=>{assert.equal(result.protectionComplete,false,'Forged archive ACK must not complete protection');assert.ok(record?.sourceIntent,'Original plaintext must remain quarantined');};

test('a forged serialized prior archive receipt cannot purge without actual SDK ciphertext verification',{timeout:30000},async t=>{
  const f=await fixture(t),result=await withoutCipher(f);quarantineOracle(result,await f.creation.get(f.original.operationId));
  assert.match(result.reason,/missing/u);assert.equal((await f.creation.sourceFence(f.source)).state,'pending');
});

test('the identical quarantine oracle detects an intentionally removed verification in the actual Native coordinator',{timeout:30000},async t=>{
  const url=new URL('./native-cache-transition.mjs',import.meta.url),sourceCode=await readFile(url,'utf8');
  let broken=sourceCode.replace('await vault.verifyCreationRecoveries(objectId,source,rows,receipt);active();','void receipt;active();');assert.notEqual(broken,sourceCode);
  broken=broken.replace(/from '([^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`);
  const module=await import('data:text/javascript;base64,'+Buffer.from(broken).toString('base64'));
  const f=await fixture(t),result=await withoutCipher(f,module.createNativeCacheTransition),remaining=await f.creation.get(f.original.operationId);
  assert.equal(result.protectionComplete,true);assert.equal(remaining,null);
  assert.throws(()=>quarantineOracle(result,remaining),/Forged archive ACK/u,'The unchanged oracle must catch the removed actual SDK verification');
});

test('cheap real PNG Source lost-ACK creation archive preserves exact Blob/name/hash through actual SDK and retirement',{timeout:30000},async t=>{
  const f=await fixture(t,{kind:'image'});assert.equal(f.readVersion(['materials','materials.sqlite']),2);await f.convert();await f.sdk.unlock(f.object.objectId,password);
  const before=f.calls.filter(value=>value.operation.startsWith('materials.')).length,result=await f.flow.resumeAfterUnlock(f.object.objectId);assert.equal(result.protectionComplete,true,JSON.stringify(result));
  const recovered=(await f.sdk.creationRecoveries(f.object.objectId))[0].records[0],blob=recovered.sourceIntent.blob;
  assert.equal(sha(new Uint8Array(await blob.arrayBuffer())),sha(pngFixture()));assert.equal(blob.type,'image/png');assert.equal(recovered.sourceIntent.name,f.original.sourceIntent.name);
  assert.equal(recovered.sourceIntent.commitMutationId,f.original.sourceIntent.commitMutationId);assert.equal(await f.creation.get(f.original.operationId),null);
  assert.equal(f.calls.filter(value=>value.operation.startsWith('materials.')).length,before,'No automatic Materials replay');
  assert.equal((await f.call(f.owner,'materials.get',{materialId:f.source.ref})).error.code,'materials_material_sealed');
});
