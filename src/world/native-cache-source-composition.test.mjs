import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,realpathSync,rmSync,readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createNotesService} from '../../modules/notes/server/index.mjs';
import {createCanvasObjectsService} from '../../modules/canvas-objects/server.mjs';
import {createCanvasVaultService} from '../../modules/canvas-vault/server.mjs';
import {createNoteSession} from '../../modules/notes/browser/state.mjs';
import {createNativeDraftStore} from '../../modules/notes/browser/native-local.mjs';
import {createIndexedNoteCacheStorage,createNoteCache,readNoteWithCache} from '../../modules/notes/browser/cache.mjs';
import {createCanvasVaultClient} from './canvas-vault-client.mjs';
import {createCanvasVaultLocalStore,createCanvasVaultMemoryCache} from './canvas-vault-local.mjs';
import {createNativeCacheTransition} from './native-cache-transition.mjs';
import {createCanvasCreateStore} from './canvas-create-flow.mjs';
import {createCanvasPageLive} from './canvas-page-live.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';

const owner={accountId:'cache_source_owner',deviceId:'cache_source_device'},password='Synthetic Source conversion password1',secret='Late editor text must survive conversion 78c1';
const turn=()=>new Promise(resolve=>setImmediate(resolve));
const until=async predicate=>{for(let index=0;index<500;index++){if(predicate())return;await turn();}assert.fail('Expected Source/cache boundary');};

function fixture(t){
  const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-cache-source-')),projectId='cache_source_test';let notes,objects,vault,releaseAck,held=false,current=true;
  const authority=(actor,callback)=>{assert.equal(actor,owner,'Original Root actor, not a copied JSON selector');return callback();};
  notes=createNotesService({databasePath:join(folder,'notes.sqlite'),projectId,allowNativeMigration:true,allowVaultMigration:true,
    verifyCanvasObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback),verifyVaultCipherReceipt:(proof,callback)=>vault.withCipherReceiptAuthority(proof,callback),readVaultSource:request=>vault.readConvertedSource(request)});
  objects=createCanvasObjectsService({databasePath:join(folder,'objects.sqlite'),projectId,actorActive:actor=>actor.accountId===owner.accountId&&actor.deviceId===owner.deviceId,
    withActorAuthorityFence:authority,withProtectionAuthority:(request,callback)=>vault.withProtectionAuthority(request,callback),providers:[{providerId:'soty.notes',
      withBindingAuthority:(request,callback)=>notes.canvas.withSourceAuthority({actor:request.actor,ownerId:request.objectOwnerId,mode:request.mode,...(request.purpose?{purpose:request.purpose}:{}),noteId:request.ref},callback),
      project:context=>({kind:'note',...notes.canvas.readMetadata({context})})}]});
  vault=createCanvasVaultService({databasePath:join(folder,'vault.sqlite'),projectId,allowFormat2Migration:true,nativeProviders:[{providerId:'soty.notes',...notes.vault}],
    actorActive:actor=>actor.accountId===owner.accountId&&actor.deviceId===owner.deviceId,withActorAuthorityFence:authority,
    withProtectionTarget:(request,callback)=>objects.withProtectionTarget(request,callback),withObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback)});
  const noteId=crypto.randomUUID(),initial={noteId,expectedRevision:0,mutationId:crypto.randomUUID(),title:'Private original title',body:'Original body',items:[],color:'plain',pinned:false,state:'active'};
  notes.execute({actor:owner,op:'notes.put',args:{expectedAccountId:owner.accountId,...initial}});
  const object=()=>objects.execute({actor:owner,op:'canvas.objects.create',args:{expectedAccountId:owner.accountId,requestId:crypto.randomUUID(),binding:{providerId:'soty.notes',ref:noteId},presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}}}).current;
  const first=object(),second=object(),calls=[];
  const api={async request(op,args){calls.push({op,args:structuredClone(args)});
    if(op==='canvas.objects.content'){const{bytes,...result}=objects.withObjectRead({actor:owner,objectId:args.objectId},context=>vault.readContent({context,index:args.index??0}));return{...result,bytesBase64:Buffer.from(bytes).toString('base64url')};}
    const service=op.startsWith('notes.')?notes:op.startsWith('canvas.vault.')?vault:objects,result=await service.execute({actor:owner,op,args:structuredClone(args)});
    if(op==='notes.put'&&args.expectedRevision===1&&!held){held=true;return new Promise(resolve=>{releaseAck=()=>resolve(result);});}return result;
  }};
  const port=indexedNativePort(),origin='https://source-cache.example',store=createNativeDraftStore(owner.accountId,projectId,{origin,indexedDB:port.indexedDB});
  const cache=createNoteCache(owner.accountId,projectId,{origin,storage:createIndexedNoteCacheStorage(port.indexedDB)}),local=createCanvasVaultLocalStore(owner.accountId,projectId,{origin,indexedDB:port.indexedDB});
  const client=createCanvasVaultClient({api,accountId:owner.accountId,projectId,isCurrent:()=>current,store:local});
  const creationStore=createCanvasCreateStore(owner.accountId,projectId,{origin,indexedDB:port.indexedDB});
  const flow=createNativeCacheTransition({api,accountId:owner.accountId,projectId,isCurrent:()=>current,vault:client,store,cache,creationStore,inspectCreation:async()=>[]});
  const sessions=[];t.after(async()=>{releaseAck?.();for(const session of sessions){await session.flush().catch(()=>{});session.dispose();}await turn();flow.dispose();client.dispose();local.close();vault.close();objects.close();notes.close();
    const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-cache-source-'));rmSync(actual,{recursive:true,force:true});});
  return{api,port,store,cache,client,flow,noteId,first,second,calls,notes,objects,vault,sessions,release:()=>releaseAck?.(),get held(){return held;}};
}

test('actual Notes/Object/Vault + Session + IDB fences preserve a late-ACK draft across both stable Native aliases',{timeout:20000},async t=>{
  const f=fixture(t),loaded=await readNoteWithCache({api:f.api,accountId:owner.accountId,noteId:f.noteId,cache:f.cache});
  const one=createNoteSession({api:f.api,accountId:owner.accountId,store:f.store,cache:f.cache,note:loaded.note,debounceMs:60000}),two=createNoteSession({api:f.api,accountId:owner.accountId,store:f.store,cache:f.cache,note:loaded.note,debounceMs:60000});f.sessions.push(one,two);
  one.edit({body:secret});const saving=one.flush();await until(()=>f.held);
  const newer='Newer unsent branch survives explicit recovery f9a1';one.edit({body:newer});await one.flushLocal();
  const source={providerId:'soty.notes',ref:f.noteId,ownerId:owner.accountId};f.flow.remember(f.first.objectId,source);f.flow.remember(f.second.objectId,source);
  const converted=await f.api.request('canvas.vault.protect',{expectedAccountId:owner.accountId,objectId:f.first.objectId,expectedObjectRevision:1,requestId:'source_convert_123',password});assert.equal(converted.protected,true);
  const pending=await f.flow.liveReconcile({objectId:f.first.objectId});assert.equal(pending.server,'protected');assert.equal(pending.device,'pending');assert.equal(pending.protectionComplete,false);
  assert.equal(one.state().note.body,'');assert.equal(two.state().note.body,'');assert.equal(await f.cache.get(f.noteId),null);assert.deepEqual(await f.store.list(),[]);assert.equal((await f.store.native.inspect(f.noteId))[0].note.body,newer);
  await f.client.unlock(f.first.objectId,password);const completed=await f.flow.resumeAfterUnlock(f.first.objectId);assert.equal(completed.protectionComplete,true,'Actual encrypted local COMMIT must precede plaintext purge: '+JSON.stringify(completed));
  const cipherRows=JSON.stringify(await f.port.rows('soty-canvas-vault-local-v1','cipher'));assert.equal(cipherRows.includes(secret),false);assert.equal(cipherRows.includes(newer),false);assert.equal((await f.store.native.inspect(f.noteId)).length,0);
  f.release();await saving;await turn();assert.equal((await f.store.native.inspect(f.noteId)).length,0,'Old ACK/dispose must not put plaintext back');assert.equal(await f.cache.get(f.noteId),null);
  assert.throws(()=>f.notes.execute({actor:owner,op:'notes.get',args:{expectedAccountId:owner.accountId,noteId:f.noteId}}),error=>error.code==='notes_note_sealed');
  const projected=f.objects.project({actor:owner,objectIds:[f.first.objectId,f.second.objectId]}).items;assert.ok(projected.every(value=>value.protection.kind==='password'));assert.equal((await f.client.readNote(f.first.objectId)).body,secret);
  assert.equal((await f.flow.liveReconcile({objectId:f.second.objectId})).protectionComplete,true);
  const recoveries=await f.client.nativeRecoveries(f.first.objectId);assert.equal(recoveries.length,1);assert.equal(recoveries[0].draft.note.body,newer);
  const oldMutation=recoveries[0].draft.pending.mutationId;assert.equal(recoveries[0].draft.pending.document.body,secret,'The encrypted archive retains the exact original pending Notes intent');
  await f.client.restoreNativeRecovery(f.first.objectId,recoveries[0].recordId);
  const encryptedStore=f.client.draftStore(f.first.objectId),restored=(await encryptedStore.list())[0];assert.equal(restored.note.body,newer);assert.equal(restored.pending,null,'Recovery must create a fresh Vault outbox, not replay the former Notes request');
  const recovered=createNoteSession({api:{request:async(op,args)=>{assert.equal(op,'notes.put');return f.client.writeNote(f.first.objectId,args);}},accountId:owner.accountId,store:encryptedStore,cache:createCanvasVaultMemoryCache(),note:await f.client.readNote(f.first.objectId),draft:restored,debounceMs:60000});f.sessions.push(recovered);
  await recovered.flush();assert.equal(recovered.state().dirty,false);assert.equal((await f.client.readNote(f.first.objectId)).body,newer);
  const vaultWrite=f.calls.filter(value=>value.op==='canvas.vault.write');assert.equal(vaultWrite.length,1);assert.notEqual(vaultWrite[0].args.requestId,oldMutation);assert.equal(vaultWrite[0].args.expectedRevision,1,'Native Notes revision2 cannot replace canonical Vault CAS revision1');
  await f.client.pruneNativeRecoveries(f.first.objectId);assert.deepEqual(await f.client.nativeRecoveries(f.first.objectId),[]);
  assert.equal((await f.store.native.inspect(f.noteId)).length,0);assert.equal(await f.cache.get(f.noteId),null);assert.equal(f.calls.filter(value=>value.op==='notes.put').length,1,'Recovering cannot write the sealed legacy Notes Source');
});

test('actual Native conversion can encrypt the only retained RAM branch after plaintext IDB writes fail',{timeout:20000},async t=>{
  const f=fixture(t),loaded=await readNoteWithCache({api:f.api,accountId:owner.accountId,noteId:f.noteId,cache:f.cache}),onlyRam='Volatile original thought recovered into ciphertext 1e2f';
  const brokenStore={...f.store,async put(){throw Error('notes_local_quota');}},session=createNoteSession({api:f.api,accountId:owner.accountId,store:brokenStore,cache:f.cache,note:loaded.note,debounceMs:60000});f.sessions.push(session);
  session.edit({body:onlyRam});await assert.rejects(session.flushLocal(),/notes_local_quota/);session.dispose();await turn();assert.equal(session.hasUnsavedChanges(),true);
  f.flow.remember(f.first.objectId,{providerId:'soty.notes',ref:f.noteId,ownerId:owner.accountId});
  await f.api.request('canvas.vault.protect',{expectedAccountId:owner.accountId,objectId:f.first.objectId,expectedObjectRevision:1,requestId:'ram_convert_123',password});
  assert.equal((await f.flow.liveReconcile({objectId:f.first.objectId})).device,'pending');assert.equal(f.flow.hasUnsavedChanges(),true);assert.equal(session.state().note.body,'');
  await f.client.unlock(f.first.objectId,password);const done=await f.flow.resumeAfterUnlock(f.first.objectId);assert.equal(done.protectionComplete,true,'Actual encrypted COMMIT can recover RAM without requiring another plaintext write');
  assert.equal(f.flow.hasUnsavedChanges(),false);assert.equal(session.hasUnsavedChanges(),false);assert.equal((await f.store.native.inspect(f.noteId)).length,0);
  const recoveries=await f.client.nativeRecoveries(f.first.objectId);assert.equal(recoveries[0].draft.note.body,onlyRam);assert.doesNotMatch(JSON.stringify(await f.port.rows('soty-canvas-vault-local-v1','cipher')),new RegExp(onlyRam));
});

test('an old ACK arriving before password recovery cannot auto-send newer text to a known protected legacy Source',{timeout:20000},async t=>{
  const f=fixture(t),loaded=await readNoteWithCache({api:f.api,accountId:owner.accountId,noteId:f.noteId,cache:f.cache}),session=createNoteSession({api:f.api,accountId:owner.accountId,store:f.store,cache:f.cache,note:loaded.note,debounceMs:60000});f.sessions.push(session);
  session.edit({body:'Submitted before protection'});const saving=session.flush();await until(()=>f.held);session.edit({body:'Newer text must not be auto-sent after protection'});await session.flushLocal();
  f.flow.remember(f.first.objectId,{providerId:'soty.notes',ref:f.noteId,ownerId:owner.accountId});await f.api.request('canvas.vault.protect',{expectedAccountId:owner.accountId,objectId:f.first.objectId,expectedObjectRevision:1,requestId:'old_ack_convert_123',password});
  assert.equal((await f.flow.liveReconcile({objectId:f.first.objectId})).device,'pending');f.release();await saving;await turn();
  assert.equal(f.calls.filter(value=>value.op==='notes.put').length,1,'A suspended Source cannot auto-create a second plaintext request after an older ACK');
  await session.flush();assert.equal(f.calls.filter(value=>value.op==='notes.put').length,1,'Ordinary legacy UI flush also cannot bypass protection suspension');
  assert.equal((await f.store.native.inspect(f.noteId))[0].note.body,'Newer text must not be auto-sent after protection');assert.equal(session.state().note.body,'');
  await f.client.unlock(f.first.objectId,password);assert.equal((await f.flow.resumeAfterUnlock(f.first.objectId)).protectionComplete,true);assert.equal((await f.client.nativeRecoveries(f.first.objectId))[0].draft.note.body,'Newer text must not be auto-sent after protection');
});

const bridgeSource=readFileSync(new URL('./canvas-field-content.ts',import.meta.url),'utf8');
async function remoteLockProjection(t,code=bridgeSource){
  const f=fixture(t),objectId=f.first.objectId;await f.api.request('canvas.vault.protect',{expectedAccountId:owner.accountId,objectId,expectedObjectRevision:1,requestId:'remote_lock_convert',password});await f.client.unlock(objectId,password);
  const note=await f.client.readNote(objectId),encrypted=f.client.draftStore(objectId);await encrypted.put({note:{...note,body:'Encrypted pending text remains after remote relock'},branchId:'remote_pending_branch',generation:1,committedGeneration:0,pending:null,conflict:false,savedAt:1});
  let projection;const context=vm.createContext({Promise,queueMicrotask}),compiled=ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');vm.runInContext(compiled,context);
  const content=context.createCanvasFieldContent({api:f.api,vault:f.client,nativeTransition:{observeProtected(){},async resumeAfterUnlock(){return{applicable:false};}},accountId:owner.accountId,isCurrent:()=>true,projection:()=>projection});t.after(()=>content.dispose());
  await f.vault.execute({actor:owner,op:'canvas.vault.relock',args:{expectedAccountId:owner.accountId,objectId,requestId:'remote_all_relock',scope:'all'}});
  projection=f.objects.project({actor:owner,objectIds:[objectId]}).items[0];assert.equal(projection.view.state,'locked');assert.equal(f.client.isUnlocked(objectId),true,'A remote Source operation has not yet retired this SDK permit');
  const key=content.objectKey({entity:{entity:{kind:'object',id:objectId}}});assert.ok(key.startsWith('vault:locked:'));
  assert.equal(f.client.isUnlocked(objectId),false,'Fresh remote locked metadata must retire the actual old SDK session, not only hide its DOM');
  await assert.rejects(f.client.readNote(objectId),error=>error.code==='canvas_object_locked');
  await f.client.unlock(objectId,password);assert.equal((await f.client.draftStore(objectId).list())[0].note.body,'Encrypted pending text remains after remote relock');
}
test('fresh remote relock metadata retires the actual SDK permit and preserves encrypted pending drafts',remoteLockProjection);
test('negative control: rendering a lock without retiring the SDK session fails the same RAM-session oracle',async t=>{
  const needle="if(view.kind===undefined&&vault.isUnlocked(protectedValue.objectId))vault.invalidate(protectedValue.objectId,'projection');";assert.equal(bridgeSource.split(needle).length,2);
  await assert.rejects(remoteLockProjection(t,bridgeSource.replace(needle,'')),/Fresh remote locked metadata must retire the actual old SDK session/);
});

test('the unchanged locked DTO from before a successful local unlock cannot retire the new permit while refresh is pending',async t=>{
  const f=fixture(t),objectId=f.first.objectId;await f.api.request('canvas.vault.protect',{expectedAccountId:owner.accountId,objectId,expectedObjectRevision:1,requestId:'stale_projection_convert',password});
  let projection=f.objects.project({actor:owner,objectIds:[objectId]}).items[0];assert.equal(projection.view.state,'locked');
  const context=vm.createContext({Promise,queueMicrotask}),compiled=ts.transpileModule(bridgeSource,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');vm.runInContext(compiled,context);
  const content=context.createCanvasFieldContent({api:f.api,vault:f.client,nativeTransition:{observeProtected(){},async resumeAfterUnlock(){return{applicable:false};}},accountId:owner.accountId,isCurrent:()=>true,projection:()=>projection});t.after(()=>content.dispose());
  await f.client.unlock(objectId,password);assert.equal(f.client.isUnlocked(objectId),true);
  const node={entity:{entity:{kind:'object',id:objectId}}};content.objectKey(node);
  assert.equal(f.client.isUnlocked(objectId),true,'A pre-unlock immutable locked DTO is not fresh remote revocation evidence');
  projection=f.objects.project({actor:owner,objectIds:[objectId]}).items[0];assert.equal(projection.view.kind,'note');content.objectKey(node);assert.equal(f.client.isUnlocked(objectId),true);
  await f.vault.execute({actor:owner,op:'canvas.vault.relock',args:{expectedAccountId:owner.accountId,objectId,requestId:'stale_then_remote_lock',scope:'all'}});projection=f.objects.project({actor:owner,objectIds:[objectId]}).items[0];content.objectKey(node);assert.equal(f.client.isUnlocked(objectId),false,'A new current locked DTO still retires that same permit');
});

async function heldPageProjection(t,code=bridgeSource){
  const f=fixture(t),objectId=f.first.objectId;await f.api.request('canvas.vault.protect',{expectedAccountId:owner.accountId,objectId,expectedObjectRevision:1,requestId:'page_held_convert',password});
  let projection=f.objects.project({actor:owner,objectIds:[objectId]}).items[0],releaseOld,releaseCleanup,loads=0,live;const old=structuredClone(projection),applied=[];
  const held=new Promise(resolve=>{releaseOld=resolve;}),cleanup=new Promise(resolve=>{releaseCleanup=resolve;});
  const context=vm.createContext({Promise,queueMicrotask}),compiled=ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');vm.runInContext(compiled,context);
  const content=context.createCanvasFieldContent({api:f.api,vault:f.client,nativeTransition:{observeProtected(){},resumeAfterUnlock:()=>cleanup},accountId:owner.accountId,isCurrent:()=>true,projection:()=>projection,onObjectChanged:()=>live.refresh()});
  const node={entity:{entity:{kind:'object',id:objectId}}};
  live=createCanvasPageLive({events:null,isCurrent:()=>true,online:()=>true,onUnavailable:error=>assert.fail(error.message),
    client:{async load(){loads++;if(loads===1)return held;return{viewRevision:(loads===2?'b':'c').repeat(64),item:f.objects.project({actor:owner,objectIds:[objectId]}).items[0]};},wait:()=>new Promise(()=>{})},
    apply(value){applied.push(value.viewRevision);projection=value.item;content.objectKey(node);}});
  try{
    const obsolete=live.start();await turn();assert.equal(loads,1);await f.client.unlock(objectId,password);
    releaseOld({viewRevision:'a'.repeat(64),item:structuredClone(old)});await obsolete;await turn();await turn();
    assert.equal(f.client.isUnlocked(objectId),true,'A pre-unlock in-flight Page response cannot retire the newly issued SDK permit, even with a new DTO reference');assert.equal(applied.includes('a'.repeat(64)),false,'The old Page generation must never reach ProjectionMap');
    await f.vault.execute({actor:owner,op:'canvas.vault.relock',args:{expectedAccountId:owner.accountId,objectId,requestId:'page_fresh_remote_relock',scope:'all'}});await live.refresh();assert.equal(f.client.isUnlocked(objectId),false,'A read started after remote relock still retires the actual SDK grant');
  }finally{content.dispose();live.dispose();releaseCleanup({applicable:false});releaseOld({viewRevision:'a'.repeat(64),item:old});}
}
test('a held parallel Page read is fenced immediately on unlock before native cleanup, while a new remote relock is applied',heldPageProjection);
test('negative control: waiting for native cleanup before fencing Page metadata fails the same held-read permit oracle',async t=>{
  const needle='refreshBeforeUnlockCleanup(objectId);';assert.equal(bridgeSource.split(needle).length,2);
  await assert.rejects(heldPageProjection(t,bridgeSource.replace(needle,'')),/A pre-unlock in-flight Page response cannot retire the newly issued SDK permit/);
});
