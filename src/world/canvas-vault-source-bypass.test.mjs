// Independent Source-key review. Production modules are imported unchanged.
// Mechanical IDB tests are not a real-browser/multitab acceptance gate.
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,sep} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import * as local from './canvas-vault-local.mjs';
import {canvasVaultScopeSlot} from './canvas-vault-scope.mjs';
import {createCanvasObjectsService} from '../../modules/canvas-objects/server.mjs';
import {CanvasObjectError} from '../../modules/canvas-objects/contract.mjs';
import {createCanvasVaultService} from '../../modules/canvas-vault/server.mjs';

const accountId='independent_source_owner',projectId='independent_source_project';
const password='Synthetic previous key 20261104',nextPassword='Synthetic next key 20261104';
const source={schema:'soty.vault.local-scope.v1',kind:'source',id:'vault_independent_actual_source'};
const failure=code=>error=>{assert.equal(error.code,code);return true;};

async function fixture(api=local){
  const port=indexedNativePort(),store=api.createCanvasVaultLocalStore(accountId,projectId,{origin:'https://independent-source.example',indexedDB:port.indexedDB});
  const profile=slot=>api.canvasVaultProfile(accountId,projectId,slot);
  const record=(key,id,purpose='recovery',version=1,body={body:'Preserve exact encrypted local history'})=>api.sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:key.profile.slot,recordId:id,purpose,version},body);
  return{api,port,store,profile,record,async canonical(value=source,objectId='object_independent_A'){
    const slot=await canvasVaultScopeSlot(value,projectId),state=await store.ensureScopedProfile(value,profile(slot),{objectId});
    return{...state,key:await api.deriveCanvasVaultKey(state.profile,password)};
  }};
}

async function mutant(before,after){
  let text=await readFile(new URL('./canvas-vault-local.mjs',import.meta.url),'utf8');
  assert.equal(text.split(before).length,2,'mutation must change exactly one production guard');
  text=text.replace(before,after).replace("from './canvas-vault-scope.mjs'",'from '+JSON.stringify(new URL('./canvas-vault-scope.mjs',import.meta.url).href));
  return import('data:text/javascript;base64,'+Buffer.from(text).toString('base64'));
}

async function genericReplacementOracle(api){
  const f=await fixture(api);try{
    const current=await f.canonical(),history=await f.record(current.key,'history_before_generic_replace');await f.store.put(history,0);
    const nextProfile=f.profile(current.profile.slot),nextKey=await api.deriveCanvasVaultKey(nextProfile,nextPassword);
    const replacement=await f.record(nextKey,history.recordId,'recovery',2);
    await assert.rejects(f.store.replaceProfile(current.profile.profileId,nextProfile,[history],[replacement]),failure('vault_local_scope_required'));
    assert.equal((await f.store.inspectScopes()).controls[0].profileId,current.profile.profileId);
    assert.equal((await f.store.get(history.recordId)).version,1);
  }finally{f.store.close();}
}

test('generic replacement cannot bypass a canonical Source control; removing only that guard fails the same oracle',async()=>{
  await genericReplacementOracle(local);
  const broken=await mutant("vaultCheck(!meta.control(clean.slot),'vault_local_scope_required');",'/* negative control: omit canonical Source replacement guard */');
  await assert.rejects(genericReplacementOracle(broken),{code:'ERR_ASSERTION'});
});

async function lateWriterOracle(api,action){
  const f=await fixture(api);try{
    const current=await f.canonical(),history=await f.record(current.key,'protected_history');await f.store.put(history,0);
    const intent=await f.record(current.key,'original_rotation_A','rotate');
    await f.store.beginSourceRekey(current.profile.slot,current.profile.profileId,intent,{inventoryGeneration:current.inventory.generation});
    if(action==='put')await assert.rejects(f.store.put(await f.record(current.key,'late_old_key_writer'),0),failure('vault_local_rotation_pending'));
    else await assert.rejects(f.store.remove(history.recordId,history.version),failure('vault_local_rotation_pending'));
    assert.ok(await f.store.get(history.recordId));
    assert.equal(await f.store.get('late_old_key_writer'),null);
  }finally{f.store.close();}
}

test('ordinary put/remove cannot write through Source rekey; an omitted phase check is caught for both paths',async()=>{
  await lateWriterOracle(local,'put');await lateWriterOracle(local,'remove');
  const broken=await mutant("vaultCheck(!control(slot)||control(slot).phase==='ready','vault_local_rotation_pending');",'/* negative control: old writer ignores Source phase */');
  await assert.rejects(lateWriterOracle(broken,'put'),{code:'ERR_ASSERTION'});
  await assert.rejects(lateWriterOracle(broken,'remove'),{code:'ERR_ASSERTION'});
});

test('operation handoff redirects only the original operation and cannot reopen an Object or bypass a later Source fence',async()=>{
  const f=await fixture();try{
    const operationId='protected_create_original_operation',oldProfile=await f.store.ensureOperationProfile(f.profile(operationId)),oldKey=await local.deriveCanvasVaultKey(oldProfile,password);
    const oldRecord=await f.record(oldKey,'original_create_intent','create',1,{operationId,sourceRequestId:'original_source_request',stage:'source',content:'Encrypted original body'});await f.store.put(oldRecord,0);
    const current=await f.canonical(),next=await f.record(current.key,oldRecord.recordId,'create',2,await local.openCanvasVaultRecord(oldKey,oldRecord));
    assert.equal(await f.store.migrateScopes(current.profile.slot,[{slot:operationId,profileId:oldProfile.profileId,expected:[oldRecord]}],{profileId:current.profile.profileId,expected:[]},[next],{inventoryGeneration:current.inventory.generation}),true);
    assert.equal((await f.store.ensureOperationProfile(f.profile(operationId))).profileId,current.profile.profileId);
    await assert.rejects(f.store.ensureOperationProfile(f.profile('object_independent_A')),failure('vault_local_scope_changed'));
    await assert.rejects(f.store.ensureOperationProfile(f.profile(current.profile.slot)),failure('vault_local_scope_changed'));
    await assert.rejects(f.store.put(await f.record(oldKey,'resurrect_old_operation'),0),failure('vault_local_scope_changed'));
    const snapshot=await f.store.inspectScopes(),intent=await f.record(current.key,'original_A_rekey_after_handoff','rotate');
    await f.store.beginSourceRekey(current.profile.slot,current.profile.profileId,intent,{inventoryGeneration:snapshot.inventory.generation});
    await assert.rejects(f.store.ensureOperationProfile(f.profile(operationId)),failure('vault_local_rotation_pending'));
    await assert.rejects(f.store.remove(next.recordId),failure('vault_local_rotation_pending'));
    assert.deepEqual(await local.openCanvasVaultRecord(current.key,await f.store.get(next.recordId)),await local.openCanvasVaultRecord(oldKey,oldRecord));
  }finally{f.store.close();}
});

test('a pending Source rekey deliberately keeps the old profile identity: profile-id-only freshness is an inadequate oracle',async()=>{
  const f=await fixture();try{
    const current=await f.canonical(),intent=await f.record(current.key,'captured_profile_rotation','rotate',1,{objectId:'original_A',requestId:'original_rotate_request',expectedRevision:1});
    const admitted=await f.store.beginSourceRekey(current.profile.slot,current.profile.profileId,intent,{inventoryGeneration:current.inventory.generation});
    const snapshot=await f.store.inspectScopes(),control=snapshot.controls.find(value=>value.slot===current.profile.slot),profile=snapshot.profiles.find(value=>value.slot===current.profile.slot);
    assert.equal(profile.profileId,current.profile.profileId);assert.equal(control.phase,'rekeying');assert.equal(control.generation,current.control.generation+1);assert.equal(control.generation,admitted.generation);
    const oldProfileOnlyOracle=()=>assert.equal(profile.profileId===current.profile.profileId,false,'old profile identity must not acknowledge a current usable session');
    assert.throws(oldProfileOnlyOracle,{code:'ERR_ASSERTION'});
    await assert.rejects(f.store.ensureProfile(current.profile),failure('vault_local_rotation_pending'));
    const changedArgs=await f.record(current.key,intent.recordId,'rotate',1,{objectId:'retry_from_B',requestId:'different_request',expectedRevision:2});
    const repeated=await f.store.beginSourceRekey(current.profile.slot,current.profile.profileId,changedArgs,{inventoryGeneration:current.inventory.generation});
    assert.equal(repeated.generation,admitted.generation);assert.deepEqual(await local.openCanvasVaultRecord(current.key,await f.store.get(intent.recordId)),{objectId:'original_A',requestId:'original_rotate_request',expectedRevision:1},'retry admission returns the original journal; changed UI arguments are never accepted as a replacement command');
    await assert.rejects(f.store.put(await f.record(current.key,'profile_id_was_not_enough'),0),failure('vault_local_rotation_pending'));
  }finally{f.store.close();}
});

test('closed inventory cannot infer a lost-ACK operation Source from encrypted prior-password history',async()=>{
  const f=await fixture();try{
    const operationId='unknown_legacy_source_stage_operation',oldProfile=await f.store.ensureProfile(f.profile(operationId)),oldKey=await local.deriveCanvasVaultKey(oldProfile,password),requestId='unknown_original_source_request';
    const oldRecord=await f.record(oldKey,'unresolved_source_creation','create',1,{operationId,sourceRequestId:requestId,stage:'source',sourceRef:null,content:'Original source-stage local payload'});await f.store.put(oldRecord,0);
    const markedOperationId='new_unclassified_operation';await f.store.ensureOperationProfile(f.profile(markedOperationId));
    const current=await f.canonical(),headers=await f.store.inspectScopes();
    assert.deepEqual(headers.inventory.profiles.map(value=>({slot:value.slot,state:value.state})),[{slot:operationId,state:'pending'}],'closed inventory counts the old header but does not prove its Source association');
    assert.equal(headers.operations.some(value=>value.slot===markedOperationId),true,'new operation markers are tracked separately from the closed legacy inventory');
    assert.equal(JSON.stringify(headers).includes(requestId),false,'metadata cannot tell whether the unresolved operation committed this Source');
    const wrongKey=await local.deriveCanvasVaultKey(oldProfile,nextPassword);await assert.rejects(local.openCanvasVaultRecord(wrongKey,oldRecord),failure('vault_local_password_or_data_invalid'));
    assert.equal((await local.openCanvasVaultRecord(oldKey,oldRecord)).sourceRequestId,requestId);
    assert.equal((await f.store.get(oldRecord.recordId)).version,1);assert.equal(current.control.phase,'ready','an unrelated current Source need not be blocked by an unresolved operation');
  }finally{f.store.close();}
});

async function serverFixture(t,service=createCanvasVaultService){
  const folder=await mkdtemp(join(tmpdir(),'soty-source-key-review-')),owner={accountId,deviceId:'independent_owner_device'},friend={accountId:'independent_shared_viewer',deviceId:'independent_shared_device'},issued=new WeakSet([owner,friend]);let objects,vault,serial=0;
  const actorActive=actor=>[owner.deviceId,friend.deviceId].includes(actor?.deviceId),withActorAuthorityFence=(actor,callback)=>{if(!issued.has(actor))throw new CanvasObjectError('connect_authority_actor_invalid',403);return callback();};
  objects=createCanvasObjectsService({databasePath:join(folder,'objects.sqlite'),projectId,actorActive,withActorAuthorityFence,withProtectionAuthority:(request,callback)=>vault.withProtectionAuthority(request,callback)});
  vault=service({databasePath:join(folder,'vault.sqlite'),projectId,actorActive,withActorAuthorityFence,withProtectionTarget:(request,callback)=>objects.withProtectionTarget(request,callback),withObjectAuthority:(request,callback)=>objects.withObjectAuthority(request,callback),withRuntimeTarget:(request,callback)=>objects.withRuntimeProtectionTarget(request,callback)});objects.registerProvider(vault.provider);
  t.after(async()=>{vault.close();objects.close();assert.equal(resolve(folder).startsWith(resolve(tmpdir())+sep),true);await rm(folder,{recursive:true,force:true});});
  const call=(op,args,actor=owner)=>vault.execute({actor,op:'canvas.vault.'+op,args:{expectedAccountId:actor.accountId,...args}});
  const objectCall=(op,args)=>objects.execute({actor:owner,op:'canvas.objects.'+op,args:{expectedAccountId:accountId,...args}});
  const create=binding=>objectCall('create',{requestId:'independent_object_'+(++serial),binding,presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'password'}}).current;
  const snapshot=()=>{const db=new DatabaseSync(join(folder,'vault.sqlite'));try{return JSON.stringify({sources:db.prepare('SELECT * FROM vault_sources ORDER BY vault_id').all(),commands:db.prepare('SELECT * FROM vault_commands ORDER BY account_id,request_id').all()});}finally{db.close();}};
  return{call,objectCall,create,owner,friend,snapshot};
}

test('lost-ACK rotation can replay through original active A, but removing A leaves an explicit recovery boundary despite live same-Source B',async t=>{
  const f=await serverFixture(t),created=await f.call('create',{requestId:'independent_source_create',password,content:{contentType:'text/plain',bytesBase64:Buffer.from('Synthetic Source content').toString('base64url')}}),a=f.create(created.binding),b=f.create(created.binding);
  const original={requestId:'independent_rotate_original_A',objectId:a.objectId,expectedRevision:1,oldPassword:password,newPassword:nextPassword};
  const committed=await f.call('rotate',original);assert.equal(committed.current.revision,2);
  assert.equal((await f.call('rotate',original)).replayed,true,'the original command and both passwords are valid before A removal');
  f.objectCall('remove',{requestId:'independent_remove_A',objectId:a.objectId,expectedRevision:1});
  await assert.rejects(f.call('rotate',original),failure('canvas_object_unavailable'));
  const bStatus=await f.call('status',{objectId:b.objectId,includeLocalKeyScope:true});assert.equal(bStatus.revision,2);assert.equal(bStatus.localKeyScope.id,created.current.vaultId);
  assert.equal((await f.call('unlock',{objectId:b.objectId,password:nextPassword})).locked,false);
  await assert.rejects(f.call('rotate',{...original,objectId:b.objectId}),failure('vault_request_conflict'));
  await assert.rejects(f.call('status',{objectId:b.objectId},{...f.owner}),failure('connect_authority_actor_invalid'));
});

async function removedAReceiptOracle(t,service=createCanvasVaultService,{full=true}={}){
  const f=await serverFixture(t,service),created=await f.call('create',{requestId:'receipt_original_source',password,content:{contentType:'text/plain',bytesBase64:Buffer.from('Synthetic receipt Source').toString('base64url')}}),a=f.create(created.binding),b=f.create(created.binding);
  const commandId='receipt_original_rotate_A',original={requestId:commandId,objectId:a.objectId,expectedRevision:1,oldPassword:password,newPassword:nextPassword};
  const committed=await f.call('rotate',original);f.objectCall('remove',{requestId:'receipt_remove_A',objectId:a.objectId,expectedRevision:1});
  f.objectCall('update',{requestId:'receipt_share_B',objectId:b.objectId,expectedRevision:1,visibility:{audience:'restricted',accountIds:[f.friend.accountId],communityIds:[]}});
  const input={commandId,witnessObjectId:b.objectId};
  if(full)await assert.rejects(f.call('rotateReceipt',input),failure('canvas_object_locked'));
  await f.call('unlock',{objectId:b.objectId,password:nextPassword});
  const before=f.snapshot(),result=await f.call('rotateReceipt',input);
  assert.ok(result&&result.state==='committed','fresh same-Source B must read the original A receipt without another rotate');
  assert.equal(result.schema,'soty.vault-rotate-receipt.v1');assert.equal(result.commandId,commandId);assert.deepEqual(result.receipt,committed.receipt);assert.deepEqual(result.current,committed.current);assert.deepEqual(result.localKeyScope,{schema:'soty.vault.local-scope.v1',kind:'source',id:created.current.vaultId});
  assert.equal(f.snapshot(),before,'readonly recovery must not replay, rotate or create SQL rows');
  if(!full)return;
  await assert.rejects(f.call('rotate',original),failure('canvas_object_unavailable'));
  await f.call('unlock',{objectId:b.objectId,password:nextPassword},f.friend);
  await assert.rejects(f.call('rotateReceipt',input,f.friend),failure('canvas_object_unavailable'));
  await assert.rejects(f.call('rotateReceipt',input,{...f.owner}),failure('connect_authority_actor_invalid'));
  assert.deepEqual(await f.call('rotateReceipt',{...input,commandId:'never_submitted_rotate'}),{schema:'soty.vault-rotate-receipt.v1',state:'missing'});
  await assert.rejects(f.call('rotateReceipt',{...input,commandId:'receipt_original_source'}),failure('vault_request_conflict'));
  const other=await f.call('create',{requestId:'receipt_other_source',password,content:{contentType:'text/plain',bytesBase64:Buffer.from('Different Source').toString('base64url')}}),x=f.create(other.binding);await f.call('unlock',{objectId:x.objectId,password});
  await assert.rejects(f.call('rotateReceipt',{...input,witnessObjectId:x.objectId}),failure('vault_binding_changed'));
  assert.equal(f.snapshot().includes('receipt_other_source'),true);
}

test('owner-only readonly receipt settles original removed-A rotation via fresh unlocked B; the same oracle detects a missing server port',async t=>{
  await removedAReceiptOracle(t);
  const url=new URL('../../modules/canvas-vault/server.mjs',import.meta.url);let text=await readFile(url,'utf8');
  const dispatch="if(op==='canvas.vault.rotateReceipt'){";assert.equal(text.split(dispatch).length,2);
  text=text.replace(dispatch,"if(false&&op==='canvas.vault.rotateReceipt'){").replace(/from '(\.{1,2}\/[^']+)'/gu,(_all,path)=>'from '+JSON.stringify(new URL(path,url).href));
  const broken=await import('data:text/javascript;base64,'+Buffer.from(text).toString('base64'));
  await assert.rejects(removedAReceiptOracle(t,broken.createCanvasVaultService,{full:false}),{code:'ERR_ASSERTION'});
});
