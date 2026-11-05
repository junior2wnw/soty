import {createNativeDraftStore,nativeDraftDirty} from '../../modules/notes/browser/native-local.mjs';
import {createNoteCache} from '../../modules/notes/browser/cache.mjs';
import {createCanvasCreateStore} from './canvas-create-flow.mjs';
import {inspectNativeCreationRecords} from './canvas-native-creation-inspection.mjs';
import {createNoteSession} from '../../modules/notes/browser/state.mjs';

const valid=value=>typeof value==='string'&&/^[A-Za-z0-9_.:-]{1,128}$/u.test(value);
const check=(value,code='native_cache_transition_invalid')=>{if(!value)throw new Error(code);};
const errorCode=error=>/^[A-Za-z0-9_.:-]{1,128}$/u.test(String(error?.code??error?.message))?String(error?.code??error?.message):'native_cache_transition_failed';
const sameSource=(left,right)=>left?.providerId===right?.providerId&&left?.ref===right?.ref&&left?.ownerId===right?.ownerId;
const sourceOf=object=>({providerId:object.binding.providerId,ref:object.binding.ref,ownerId:object.ownerId});
const rowVersion=value=>({branchId:value.branchId,savedAt:value.savedAt,generation:value.generation});

/** A device cleanup coordinator, never a Source authority or conversion writer.
 * Root owns the stable conversion request journal and original signed API. */
export function createNativeCacheTransition({api,accountId,projectId='soty',isCurrent,vault,store:providedStore,cache:providedCache,creationStore:providedCreationStore,inspectCreation,onChange,canConvertNative}={}){
  check(api&&typeof api.request==='function'&&valid(accountId)&&valid(projectId)&&typeof isCurrent==='function'&&vault);
  const store=providedStore??createNativeDraftStore(accountId,projectId),cache=providedCache??createNoteCache(accountId,projectId);
  let creation=providedCreationStore,disposed=false;const sources=new Map(),states=new Map(),tickets=new WeakMap(),flights=new Map(),observed=new Set();
  const creationStore=()=>creation??=createCanvasCreateStore(accountId,projectId);
  const active=()=>check(!disposed&&isCurrent(),'native_cache_account_changed');
  const request=async(op,args)=>{active();const value=await api.request(op,{expectedAccountId:accountId,...args});active();return value;};
  function state(objectId,server='unknown',device='pending',reason,operationId){
    const value={objectId,applicable:true,server,device,protectionComplete:server==='protected'&&device==='complete',...(reason?{reason}:{}),...(operationId?{operationId}:{})};
    const changed=JSON.stringify(states.get(objectId))!==JSON.stringify(value);states.set(objectId,value);if(changed&&!disposed&&isCurrent())try{onChange?.({...value});}catch{}return value;
  }
  function notApplicable(objectId){const value={objectId,applicable:false,server:'unknown',device:'complete',protectionComplete:false},changed=JSON.stringify(states.get(objectId))!==JSON.stringify(value);states.set(objectId,value);if(changed&&!disposed&&isCurrent())try{onChange?.({...value});}catch{}return value;}
  async function target(objectId){
    check(valid(objectId));const result=await request('canvas.objects.get',{objectId}),object=result?.object;
    check(object?.objectId===objectId&&object.ownerId===accountId&&object.state==='active'&&Number.isSafeInteger(object.revision)&&object.revision>0
      &&typeof object.binding?.providerId==='string'&&valid(object.binding.ref),'native_cache_owner_source_required');
    if(['soty.notes','soty.materials'].includes(object.binding.providerId))sources.set(objectId,sourceOf(object));return object;
  }
  async function serverState(object){
    const status=await request('canvas.vault.status',{objectId:object.objectId});
    if(status?.state==='sealing'||status?.protected===false)return'pending';
    if(object.protection?.kind==='password'&&status?.protection?.kind==='password')return'protected';
    return object.protection?.kind==='none'&&status?.protection?.kind==='none'?'unprotected':'unknown';
  }
  async function creationState(source){
    if(inspectCreation)return inspectCreation(source);
    creationStore();
    if(creation.inspectSourceIntents){const result=await creation.inspectSourceIntents(source);active();return result;}
    const records=await creation.list();active();return inspectNativeCreationRecords(records,source);
  }
  function suspend(source){if(source?.providerId==='soty.notes')store.native.suspend(source.ref);}
  async function drain(source,online){
    if(source.providerId!=='soty.notes')return[];
    suspend(source);const ports=store.native.sessions(source.ref);
    for(const port of ports){active();try{await(online?port.flush():port.flushLocal());}catch(error){
      // Once the Source is protected, a failed plaintext write must not prevent
      // archiving the retained RAM branch directly into the existing Vault.
      if(online||!/^notes_(?:local|note_sealed)/u.test(errorCode(error)))throw error;
    }active();}
    return ports;
  }
  const privateRows=source=>source.providerId==='soty.notes'?store.native.inspect(source.ref):Promise.resolve([]);
  async function drainOrphans(source,ports){
    if(source.providerId!=='soty.notes')return;
    const owned=new Set(ports.map(port=>port.snapshot().branchId));
    for(const draft of await privateRows(source)){
      if(owned.has(draft.branchId)||!nativeDraftDirty(draft)||draft.conflict)continue;
      // Reuse the durable Notes outbox/CAS protocol, including its original
      // mutation ID after a lost ACK. Conflicting or offline branches remain.
      const result=await request('notes.get',{noteId:source.ref});check(result?.note?.noteId===source.ref,'native_cache_source_changed');
      const session=createNoteSession({api:{request},accountId,store,cache,note:result.note,draft,debounceMs:60000});
      try{store.native.suspend(source.ref);const port=store.native.sessions(source.ref).find(port=>port.snapshot().branchId===session.state().branchId);check(port,'native_cache_session_port_required');await port.flush();active();}finally{session.dispose();}
    }
  }
  function mergedRows(rows,ports){
    const byBranch=new Map(rows.map(value=>[value.branchId,value]));
    for(const port of ports){const value=port.snapshot();if(!nativeDraftDirty(value))continue;const old=byBranch.get(value.branchId);if(!old||value.generation>old.generation||value.savedAt>old.savedAt)byBranch.set(value.branchId,value);}
    return[...byBranch.values()];
  }
  async function gate(source,operationId,target){active();const journal=creationStore();check(typeof journal.sealSource==='function','native_creation_fence_unavailable');await journal.sealSource(source,operationId,target);active();return store.native.mark({providerId:source.providerId,ref:source.ref,operationId,...(target?{target}:{})});}
  function ticket(object,source,operationId){
    const value={schema:'soty.native-cache-ticket.v1',operationId,objectId:object.objectId,expectedObjectRevision:object.revision,source:{...source}};
    tickets.set(value,{source:{...source},operationId,objectId:object.objectId,expectedObjectRevision:object.revision});return value;
  }
  async function finishCreations(object,source,operationId){
    const objectId=object.objectId,journal=creationStore(),known=await creationState(source);active();
    check(typeof journal.snapshotSourceIntents==='function'&&typeof journal.retireSourceIntents==='function','native_creation_fence_unavailable');
    const fence=await journal.sourceFence(source),rows=await journal.snapshotSourceIntents(source);active();
    check(fence,'native_creation_fence_unavailable');
    if(known.some(value=>value.holdsPlaintext)&&rows.length===0)return state(objectId,'protected','pending','native_creation_pending_recovery',operationId);
    if(rows.length){
      if(typeof vault.archiveCreationIntents!=='function'||typeof vault.verifyCreationRecoveries!=='function')return state(objectId,'protected','pending','native_creation_pending_recovery',operationId);
      if(!vault.isUnlocked(objectId))return state(objectId,'protected','pending','native_password_required_for_creation_recovery',operationId);
      const receipt=await vault.archiveCreationIntents(objectId,source,rows);active();
      const fresh=await target(objectId);check(sameSource(source,sourceOf(fresh))&&await serverState(fresh)==='protected','native_cache_source_changed');
      await vault.verifyCreationRecoveries(objectId,source,rows,receipt);active();
    }
    if(!await journal.retireSourceIntents(source,fence.epoch,rows))return state(objectId,'protected','pending','native_creation_pending_recovery',operationId);
    active();return null;
  }
  async function finish(object,operationId){
    const objectId=object.objectId,source=sourceOf(object),server=await serverState(object);
    if(server!=='protected')return state(objectId,server,'pending',server==='pending'?'native_source_conversion_pending':'native_source_not_protected',operationId);
    state(objectId,'protected','pending','native_device_cleanup_pending',operationId);
    suspend(source);const fence=await gate(source,operationId),ports=await drain(source,false);
    if(source.providerId==='soty.notes'){
      check(typeof cache.seal==='function','native_cache_protection_unsupported');await cache.seal(source.ref);active();
    }
    const creationPending=await finishCreations(object,source,operationId);active();if(creationPending)return creationPending;
    const rows=await privateRows(source),portSnapshots=new Map(ports.map(port=>[port,port.snapshot()])),all=mergedRows(rows,ports),dirty=all.filter(nativeDraftDirty);
    if(dirty.length){
      if(!vault.isUnlocked(objectId))return state(objectId,server,'pending','native_password_required_for_local_draft',operationId);
      check(typeof vault.archiveNativeDrafts==='function','native_encrypted_recovery_unavailable');
      await vault.archiveNativeDrafts(objectId,source,dirty);active();
    }
    // Recheck real owner/binding/protection after encryption or other async work.
    const fresh=await target(objectId);check(sameSource(source,sourceOf(fresh)),'native_cache_source_changed');
    if(await serverState(fresh)!=='protected')return state(objectId,'unknown','pending','native_source_changed',operationId);
    const after=await privateRows(source);active();
    if(after.length!==rows.length||after.some(value=>!rows.some(old=>old.branchId===value.branchId&&old.savedAt===value.savedAt&&old.generation===value.generation)))
      return state(objectId,'protected','pending','native_local_draft_changed',operationId);
    if(ports.some(port=>port.snapshot().generation!==portSnapshots.get(port).generation))return state(objectId,'protected','pending','native_local_draft_changed',operationId);
    const complete=await store.native.finalize({providerId:source.providerId,ref:source.ref,epoch:fence.epoch,expected:rows.map(rowVersion)});active();
    if(!complete)return state(objectId,'protected','pending','native_local_draft_changed',operationId);
    for(const port of ports)port.releaseRecovered(portSnapshots.get(port).generation);
    return state(objectId,'protected','complete',undefined,operationId);
  }
  function serialize(key,task){if(flights.has(key))return flights.get(key);const flight=Promise.resolve().then(task).finally(()=>flights.delete(key));flights.set(key,flight);return flight;}
  async function prepare({objectId,expectedObjectRevision,operationId}){
    active();check(valid(operationId)&&Number.isSafeInteger(expectedObjectRevision)&&expectedObjectRevision>0);
    if(canConvertNative&&canConvertNative()===false)return{state:'pending',...state(objectId,'unknown','pending','native_conversion_unavailable',operationId)};
    return serialize(objectId,async()=>{let source;
      try{
        const object=await target(objectId);check(['soty.notes','soty.materials'].includes(object.binding.providerId),'native_cache_owner_source_required');check(object.revision===expectedObjectRevision,'native_cache_object_changed');source=sourceOf(object);
        const server=await serverState(object);
        if(server==='protected'){const result=await finish(object,operationId);return{state:result.protectionComplete?'complete':'pending',...result};}
        if(server!=='unprotected')return{state:'pending',...state(objectId,server,'pending','native_source_conversion_pending',operationId)};
        const prior=await store.native.gate(source.providerId,source.ref)??await creationStore().sourceFence?.(source);let submission=object;
        if(prior){
          if(prior.state!=='pending'||!prior.target)return{state:'pending',...state(objectId,server,'pending','native_conversion_intent_unavailable')};
          if(prior.operationId!==operationId)return{state:'pending',...state(objectId,server,'pending','native_conversion_operation_pending',prior.operationId)};
          submission=prior.target.objectId===objectId?object:await target(prior.target.objectId);
          check(sameSource(source,sourceOf(submission))&&submission.revision===prior.target.expectedObjectRevision,'native_cache_original_target_changed');
        }
        const pending=await creationState(source);active();
        if(pending.some(value=>value.holdsPlaintext)&&!(typeof vault.archiveCreationIntents==='function'&&typeof vault.verifyCreationRecoveries==='function'))return{state:'pending',...state(objectId,server,'pending','native_creation_source_ack_pending',operationId)};
        suspend(source);await gate(source,operationId,{objectId:submission.objectId,expectedObjectRevision:submission.revision});const ports=await drain(source,true);await drainOrphans(source,ports);const rows=mergedRows(await privateRows(source),ports);active();
        const value=ticket(submission,source,operationId);
        if(rows.some(nativeDraftDirty))return{state:'pending',ticket:value,...state(objectId,server,'pending','native_local_draft_pending',operationId)};
        const fresh=await target(submission.objectId);check(fresh.revision===submission.revision&&sameSource(source,sourceOf(fresh)),'native_cache_source_changed');
        if(submission.objectId!==objectId){const alias=await target(objectId);check(alias.revision===object.revision&&sameSource(source,sourceOf(alias)),'native_cache_source_changed');}
        if(await serverState(fresh)!=='unprotected')return{state:'pending',ticket:value,...state(objectId,'pending','pending','native_source_conversion_pending',operationId)};
        return{state:'ready',ticket:value,...state(objectId,'unprotected','pending',undefined,operationId)};
      }catch(error){return{state:'pending',...state(objectId,'unknown','pending',errorCode(error),operationId)};}
    });
  }
  const bound=value=>{const known=tickets.get(value);check(known&&known.objectId===value.objectId&&known.expectedObjectRevision===value.expectedObjectRevision&&sameSource(known.source,value.source)&&known.operationId===value.operationId,'native_cache_ticket_changed');return known;};
  const failureState=(objectId,error,operationId)=>{const code=errorCode(error),local=/^(?:notes_(?:cache|local)|vault_local|native_(?:local|creation|encrypted|password|cache_protection))/u.test(code);return state(objectId,local?states.get(objectId)?.server??'unknown':'unknown','pending',code,operationId);};
  async function releaseGate(object,known){
    const journal=creationStore(),creationFence=await journal.sourceFence?.(known.source);
    if(creationFence&&(!journal.cancelSourceFence||!await journal.cancelSourceFence(known.source,creationFence.epoch)))return state(object.objectId,'unprotected','pending','native_cache_cancel_unsafe',known.operationId);
    const fence=await store.native.gate(known.source.providerId,known.source.ref);
    if(fence&&!await store.native.cancel({providerId:known.source.providerId,ref:known.source.ref,epoch:fence.epoch}))return state(object.objectId,'unprotected','pending','native_cache_cancel_unsafe',known.operationId);
    if(known.source.providerId==='soty.notes')store.native.resume(known.source.ref);
    return state(object.objectId,'unprotected','complete',undefined,known.operationId);
  }
  async function finalize({ticket:value,result,failure}){
    active();const known=bound(value);
    return serialize(value.objectId,async()=>{try{
      const object=await target(value.objectId);check(sameSource(sourceOf(object),known.source),'native_cache_source_changed');const server=await serverState(object);
      if(server==='unprotected'&&failure?.kind==='server-rejected'&&['vault_conversion_required','vault_format2_migration_required','vault_operation_unsupported'].includes(failure.code))return await releaseGate(object,known);
      if(server==='unprotected'&&result?.schema==='soty.vault-conversion.v1'&&result.state==='pending'&&result.protected===false)return state(value.objectId,'pending','pending','native_source_conversion_pending',value.operationId);
      return await finish(object,value.operationId);
    }catch(error){return failureState(value.objectId,error,value.operationId);}});
  }
  async function liveReconcile({objectId}){
    active();return serialize(objectId,async()=>{try{
      const object=await target(objectId);if(!['soty.notes','soty.materials'].includes(object.binding.providerId))return notApplicable(objectId);
      const server=await serverState(object),prior=await store.native.gate(object.binding.providerId,object.binding.ref)??await creationStore().sourceFence?.(sourceOf(object)),operationId=prior?.operationId??'reconcile_'+objectId;
      // A fresh "none" snapshot cannot settle an old in-flight/lost ACK intent.
      // Only explicit never-submitted cancel or a proven rejection releases it.
      if(server==='unprotected')return prior?state(objectId,server,'pending',prior.target?'native_conversion_result_unknown':'native_conversion_intent_unavailable',prior.target?operationId:undefined):state(objectId,server,'complete');
      if(server==='pending'){suspend(sourceOf(object));await gate(sourceOf(object),operationId);await drain(sourceOf(object),false);return state(objectId,server,'pending','native_source_conversion_pending',operationId);}
      return await finish(object,operationId);
    }catch(error){if(errorCode(error)==='canvas_owner_required'&&!sources.has(objectId))return notApplicable(objectId);return failureState(objectId,error);}});
  }
  async function cancel({ticket:value,neverSubmitted}){
    active();const known=bound(value);check(neverSubmitted===true,'native_cache_cancel_unsafe');
    const object=await target(value.objectId);check(sameSource(sourceOf(object),known.source)&&await serverState(object)==='unprotected','native_cache_cancel_unsafe');
    const result=await releaseGate(object,known);tickets.delete(value);return result;
  }
  const retainedPorts=()=>[...new Set([...sources.values()].filter(source=>source.providerId==='soty.notes').flatMap(source=>store.native.sessions(source.ref)))];
  return{prepare,finalize,liveReconcile,resumeAfterUnlock:objectId=>liveReconcile({objectId}),cancel,
    /** A detached editor with a failed local COMMIT still blocks silent loss. */
    hasUnsavedChanges:()=>retainedPorts().some(port=>port.hasUnsavedChanges?.()===true),
    async flush(){active();for(const port of retainedPorts()){await port.flushLocal();active();}},
    remember(objectId,source){active();check(valid(objectId)&&source?.ownerId===accountId&&['soty.notes','soty.materials'].includes(source.providerId)&&valid(source.ref));sources.set(objectId,{...source});},
    observeProtected(objectId){if(disposed||!isCurrent()||observed.has(objectId))return;observed.add(objectId);const source=sources.get(objectId);try{if(source)suspend(source);}catch{}void liveReconcile({objectId}).catch(()=>{});},
    snapshot:objectId=>states.has(objectId)?{...states.get(objectId)}:null,
    dispose(){if(disposed)return;disposed=true;sources.clear();states.clear();observed.clear();if(!providedCreationStore)creation?.close?.();},
  };
}
