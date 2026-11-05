import test from 'node:test';
import assert from 'node:assert/strict';
import {createNativeCacheTransition} from './native-cache-transition.mjs';
import {createNativeDraftStore} from '../../modules/notes/browser/native-local.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {inspectNativeCreationRecords} from './canvas-native-creation-inspection.mjs';
import {createCanvasCreateStore} from './canvas-create-flow.mjs';

const accountId='transition_owner',noteId='note_transition_123',objectId='object_transition_123';
const source={providerId:'soty.notes',ref:noteId,ownerId:accountId};
const draft=(branchId='branch_transition_123',dirty=true)=>({note:{noteId,title:'Private title',body:'Synthetic private draft',preview:'Private preview',items:[],color:'plain',pinned:false,state:'active',revision:7,createdAt:1,updatedAt:1},branchId,generation:1,committedGeneration:dirty?0:1,pending:null,conflict:false,savedAt:1});
function fixture({server='unprotected',failSeal=false,failEncrypt=false,creation=[],foreign=false}={}){
  const port=indexedNativePort(),store=createNativeDraftStore(accountId,'soty',{origin:'https://soty.example',indexedDB:port.indexedDB}),calls=[],changes=[],archives=[];
  let current=true,unlocked=false;const sourceState={value:server};
  const api={async request(op,args){calls.push({op,args});if(op==='canvas.objects.get')return{object:{objectId:args.objectId,ownerId:foreign?'other_owner':accountId,revision:1,binding:{providerId:'soty.notes',ref:noteId},state:'active',protection:{kind:sourceState.value==='protected'?'password':'none'}}};
    if(op==='canvas.vault.status')return{objectId:args.objectId,locked:sourceState.value==='protected'&&!unlocked,protection:{kind:sourceState.value==='protected'?'password':'none'},...(sourceState.value==='pending'?{state:'sealing',protected:false}:{})};throw Error('Unexpected Source mutation '+op);}};
  const vault={isUnlocked:()=>unlocked,async archiveNativeDrafts(id,identity,drafts){if(failEncrypt)throw Error('vault_local_quota');archives.push({id,identity,drafts:structuredClone(drafts)});}};
  const cache={async seal(id){calls.push({op:'local.cache.seal',id});if(failSeal)throw Error('notes_cache_unavailable');}};
  const creationStore=createCanvasCreateStore(accountId,'soty',{origin:'https://soty.example',indexedDB:port.indexedDB});
  const options={api,accountId,isCurrent:()=>current,vault,store,cache,creationStore,inspectCreation:async()=>creation,onChange:value=>changes.push(value)};
  const flow=createNativeCacheTransition(options);
  return{flow,options,port,store,calls,changes,archives,sourceState,setCurrent(value){current=value;},unlock(){unlocked=true;},setEncryptFailure(value){failEncrypt=value;}};
}

test('owner prepare requires confirmed clean branches; only protected fresh status can finish device cleanup',async()=>{
  const f=fixture();await f.store.put(draft('branch_clean_123',false));const ready=await f.flow.prepare({objectId,expectedObjectRevision:1,operationId:'operation_clean_123'});
  assert.equal(ready.state,'ready');assert.equal(ready.protectionComplete,false);assert.equal(ready.server,'unprotected');assert.deepEqual(await f.store.list(),[]);
  const pending=await f.flow.finalize({ticket:ready.ticket,result:{state:'protected',protected:true}});assert.equal(pending.protectionComplete,false,'A claimed result cannot replace fresh target/protection reads');assert.equal((await f.store.native.inspect(noteId)).length,1);
  f.sourceState.value='protected';const done=await f.flow.finalize({ticket:ready.ticket});assert.equal(done.protectionComplete,true);assert.equal(done.device,'complete');assert.deepEqual(await f.store.native.inspect(noteId),[]);
  assert.ok(f.calls.every(value=>!/^notes\.|convert|protect$/u.test(value.op)));f.flow.dispose();
});

test('unsaved or conflicting branches do not convert or disappear, and safe never-submitted cancel restores the private draft',async()=>{
  const f=fixture();await f.store.put({...draft(),conflict:true});const result=await f.flow.prepare({objectId,expectedObjectRevision:1,operationId:'operation_dirty_123'});
  assert.equal(result.state,'pending');assert.equal(result.reason,'native_local_draft_pending');assert.equal((await f.store.native.inspect(noteId))[0].note.body,draft().note.body);assert.deepEqual(await f.store.list(),[]);
  await assert.rejects(f.flow.cancel({ticket:result.ticket,neverSubmitted:false}),/native_cache_cancel_unsafe/u);
  assert.equal((await f.flow.cancel({ticket:result.ticket,neverSubmitted:true})).server,'unprotected');assert.equal((await f.store.list()).length,1);f.flow.dispose();
});

test('remote protection quarantines both aliases; wrong/no password or encryption failure cannot purge unsaved text',async()=>{
  const f=fixture({server:'protected',failEncrypt:true}),alias='object_second_alias_123';await f.store.put(draft());await f.store.put(draft('branch_second_123'));
  const suspended=[];const register=(name,id)=>{let recovered=false;return f.store.native.register(noteId,{suspend(){suspended.push(name);},async flush(){},async flushLocal(){},snapshot:()=>draft(id,!recovered),releaseRecovered(){recovered=true;}});};
  const offOne=register('one','branch_transition_123'),offTwo=register('two','branch_second_123');f.flow.remember(objectId,source);f.flow.remember(alias,source);f.flow.observeProtected(objectId);
  const pending=await f.flow.liveReconcile({objectId});assert.equal(pending.device,'pending');assert.equal(pending.protectionComplete,false);assert.ok(suspended.includes('one')&&suspended.includes('two'));assert.deepEqual(await f.store.list(),[]);assert.equal((await f.store.native.inspect(noteId)).length,2);
  f.unlock();const failed=await f.flow.resumeAfterUnlock(objectId);assert.equal(failed.protectionComplete,false);assert.equal((await f.store.native.inspect(noteId)).length,2);
  f.setEncryptFailure(false);const done=await f.flow.resumeAfterUnlock(objectId);assert.equal(done.protectionComplete,true);assert.equal(f.archives[0].drafts.length,2);assert.equal((await f.store.native.inspect(noteId)).length,0);
  assert.equal((await f.flow.liveReconcile({objectId:alias})).protectionComplete,true);assert.equal(f.archives.length,1);offOne();offTwo();f.flow.dispose();
});

test('a cache purge failure or known plaintext creation intent stays pending and retains its recoverable data',async()=>{
  const failed=fixture({server:'protected',failSeal:true});await failed.store.put(draft());const status=await failed.flow.liveReconcile({objectId});assert.equal(status.protectionComplete,false);assert.equal((await failed.store.native.inspect(noteId)).length,1);failed.flow.dispose();
  const creation=[{operationId:'creation_lost_ack',stage:'source',holdsPlaintext:true,sourceConfirmed:false}],f=fixture({creation});
  const prepare=await f.flow.prepare({objectId,expectedObjectRevision:1,operationId:'operation_creation_123'});assert.equal(prepare.reason,'native_creation_source_ack_pending');assert.equal(prepare.state,'pending');assert.equal(await f.store.native.gate('soty.notes',noteId),null);
  f.sourceState.value='protected';assert.equal((await f.flow.liveReconcile({objectId})).reason,'native_creation_pending_recovery');assert.equal(creation.length,1);f.flow.dispose();
});

test('foreign owner, changed identity, and a copied/unbranded ticket cannot drive Source cleanup',async()=>{
  const foreign=fixture({foreign:true});await foreign.store.put(draft());const response=await foreign.flow.prepare({objectId,expectedObjectRevision:1,operationId:'operation_foreign_123'});assert.equal(response.protectionComplete,false);assert.equal(response.reason,'native_cache_owner_source_required');assert.equal(await foreign.store.native.gate('soty.notes',noteId),null);foreign.flow.dispose();
  const f=fixture();const ready=await f.flow.prepare({objectId,expectedObjectRevision:1,operationId:'operation_brand_123'});await assert.rejects(f.flow.finalize({ticket:structuredClone(ready.ticket)}),/native_cache_ticket_changed/u);
  f.setCurrent(false);await assert.rejects(f.flow.finalize({ticket:ready.ticket}),/native_cache_account_changed/u);f.flow.dispose();
});

test('Source creation inspection emits metadata only and never body, file, label or hash',()=>{
  const rows=[{operationId:'creation_one',kind:'note',stage:'source',sourceIntent:{noteId,title:'Private title',body:'Never emit this'},intentHash:'private-fingerprint'},
    {operationId:'creation_two',kind:'note',stage:'complete',sourceRef:noteId,sourceIntent:null},
    {operationId:'creation_other',kind:'image',stage:'source',sourceIntent:{materialId:'material_other_123',blob:new Blob(['private file'])}}];
  const result=inspectNativeCreationRecords(rows,source);assert.deepEqual(result,[{operationId:'creation_one',stage:'source',holdsPlaintext:true,sourceConfirmed:false},{operationId:'creation_two',stage:'complete',holdsPlaintext:false,sourceConfirmed:true}]);assert.doesNotMatch(JSON.stringify(result),/Private|Never|fingerprint|blob|hash|label|body/u);
});

test('a known unsupported server rejection releases an unprotected Source, but an uncertain ACK never does',async()=>{
  const f=fixture();await f.store.put(draft('branch_rejected_123',false));const ready=await f.flow.prepare({objectId,expectedObjectRevision:1,operationId:'operation_rejected_123'});
  const rejected=await f.flow.finalize({ticket:ready.ticket,failure:{kind:'server-rejected',code:'vault_conversion_required'}});
  assert.equal(rejected.server,'unprotected');assert.equal(rejected.device,'complete');assert.equal(await f.store.native.gate('soty.notes',noteId),null);assert.equal((await f.store.list()).length,1);
  const retry=await f.flow.prepare({objectId,expectedObjectRevision:1,operationId:'operation_rejected_123'});assert.equal(retry.state,'ready');
  const unknown=await f.flow.finalize({ticket:retry.ticket,failure:{kind:'transport-uncertain',code:'network_error'}});assert.equal(unknown.device,'pending');assert.notEqual(await f.store.native.gate('soty.notes',noteId),null);assert.deepEqual(await f.store.list(),[]);
  const reopened=createNativeCacheTransition(f.options),reconciled=await reopened.liveReconcile({objectId});
  assert.equal(reconciled.device,'pending','Fresh unprotected metadata cannot settle an earlier uncertain conversion request');assert.equal(reconciled.operationId,'operation_rejected_123','A reopened action must retry the original stable conversion intent');reopened.dispose();
  f.sourceState.value='protected';assert.equal((await f.flow.finalize({ticket:retry.ticket})).protectionComplete,true);f.flow.dispose();
});

test('an explicit false server capability refuses before freezing an editor or writing a local fence',async()=>{
  const f=fixture(),flow=createNativeCacheTransition({...f.options,canConvertNative:()=>false});
  const value=await flow.prepare({objectId,expectedObjectRevision:1,operationId:'operation_unsupported_123'});assert.equal(value.reason,'native_conversion_unavailable');assert.equal(f.calls.length,0);assert.equal(await f.store.native.gate('soty.notes',noteId),null);flow.dispose();f.flow.dispose();
});

test('reopen from a second alias resumes the immutable original target/revision and refuses a new operation or changed original',async()=>{
  const f=fixture(),alias='object_alias_resume_123',operationId='operation_original_alias';
  const first=await f.flow.prepare({objectId,expectedObjectRevision:1,operationId});assert.equal(first.state,'ready');
  await f.flow.finalize({ticket:first.ticket,failure:{kind:'transport-uncertain'}});f.flow.dispose();
  const reopened=createNativeCacheTransition(f.options),state=await reopened.liveReconcile({objectId:alias});assert.equal(state.operationId,operationId);assert.equal(state.device,'pending');
  const wrong=await reopened.prepare({objectId:alias,expectedObjectRevision:1,operationId:'different_intent'});assert.equal(wrong.state,'pending');assert.equal(wrong.operationId,operationId);
  const retry=await reopened.prepare({objectId:alias,expectedObjectRevision:1,operationId});assert.equal(retry.state,'ready');assert.equal(retry.ticket.objectId,objectId,'A Source alias cannot retarget an existing conversion packet');assert.equal(retry.ticket.expectedObjectRevision,1);assert.equal(retry.ticket.operationId,operationId);
  const target=await f.store.native.gate('soty.notes',noteId);assert.deepEqual(target.target,{objectId,expectedObjectRevision:1});
  const originalApi=f.options.api,changed=createNativeCacheTransition({...f.options,api:{async request(op,args){const result=await originalApi.request(op,args);if(op==='canvas.objects.get'&&args.objectId===objectId)result.object.revision=2;return result;}}});
  const rejected=await changed.prepare({objectId:alias,expectedObjectRevision:1,operationId});assert.equal(rejected.state,'pending');assert.equal(rejected.reason,'native_cache_original_target_changed');
  changed.dispose();reopened.dispose();
});

test('a legacy pending gate without original target metadata cannot be converted into a new alias packet',async()=>{
  const f=fixture();await f.store.native.mark({providerId:'soty.notes',ref:noteId,operationId:'old_unrecoverable_intent'});
  const status=await f.flow.liveReconcile({objectId});assert.equal(status.device,'pending');assert.equal(status.operationId,undefined);
  const prepared=await f.flow.prepare({objectId,expectedObjectRevision:1,operationId:'old_unrecoverable_intent'});assert.equal(prepared.state,'pending');assert.equal(prepared.reason,'native_conversion_intent_unavailable');assert.equal(prepared.ticket,undefined);f.flow.dispose();
});
