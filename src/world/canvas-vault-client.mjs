import {CanvasVaultError,CANVAS_VAULT_LOCAL_LIMITS,vaultCheck,vaultId,vaultErrorCode,vaultBase64,vaultBytes,vaultClone,canvasVaultProfile,deriveCanvasVaultKey,sealCanvasVaultRecord,openCanvasVaultRecord,createCanvasVaultLocalStore} from './canvas-vault-local.mjs';
import {archiveNativeNoteDrafts,readNativeNoteRecoveries} from './canvas-native-recovery.mjs';
import {readCanvasVaultContent} from './canvas-vault-bulk-reader.mjs';
import {archiveCreationIntents as archiveLocalCreations,verifyCreationRecoveries as verifyLocalCreations,readCreationRecoveries as readLocalCreations} from './canvas-creation-recovery.mjs';
import {canvasVaultScope,canvasVaultScopeSlot,sameCanvasVaultScope,createCanvasVaultRecoveryScope} from './canvas-vault-scope.mjs';
import {migrateCanvasVaultLegacy} from './canvas-vault-source-migration.mjs';
import {registerCanvasVaultDraft,drainCanvasVaultDrafts,recoverCanvasVaultDrafts,canvasVaultDraftSessions} from './canvas-vault-source-sessions.mjs';
const encoder=new TextEncoder(),decoder=new TextDecoder('utf-8',{fatal:true});
const CHUNK=512*1024,NOTE_TYPE='application/vnd.soty.note+json';
const authError=error=>[401,403,404].includes(error?.status)||/locked|revoked|unavailable|access_changed|account_changed|authentication|denied|owner_required/u.test(vaultErrorCode(error));
// Cooperative current-client drains cover aliases and SDK instances in this
// realm; the IDB Source fence covers other tabs and late old-key writers.
const sourceClients=new Map();
function noteDocument(value){
  vaultCheck(value&&typeof value==='object'&&!Array.isArray(value)&&typeof value.title==='string'&&value.title.length<=160&&typeof value.body==='string'&&value.body.length<=100000&&!/[\u0000]/u.test(value.title+value.body),'vault_note_invalid');
  const items=value.items??[];vaultCheck(Array.isArray(items)&&items.length<=200,'vault_note_invalid');const seen=new Set();
  for(const item of items){vaultCheck(vaultId(item?.id)&&!seen.has(item.id)&&typeof item.text==='string'&&item.text.length<=1000&&typeof item.done==='boolean','vault_note_invalid');seen.add(item.id);}
  const color=value.color??'plain';vaultCheck(['plain','honey','sage','lilac','blue','coral'].includes(color),'vault_note_invalid');
  const state=value.state??'active';vaultCheck(['active','archived','trashed'].includes(state),'vault_note_invalid');
  return{title:value.title,body:value.body,items:items.map(item=>({id:item.id,text:item.text,done:item.done})),color,pinned:value.pinned===true,state};
}
function noteMetadata(value){
  const result={};if(value.noteId!==undefined){vaultCheck(vaultId(value.noteId),'vault_note_invalid');result.noteId=value.noteId;}
  for(const key of['revision','createdAt','updatedAt'])if(value[key]!==undefined){vaultCheck(Number.isSafeInteger(value[key])&&value[key]>=(key==='revision'?1:0),'vault_note_invalid');result[key]=value[key];}return result;
}
export function canvasVaultNoteContent(value){const clean={...noteDocument(value),...noteMetadata(value)},bytes=encoder.encode(JSON.stringify(clean));vaultCheck(bytes.length<=CANVAS_VAULT_LOCAL_LIMITS.payloadBytes,'vault_wire_size');return{contentType:NOTE_TYPE,bytesBase64:vaultBase64(bytes)};}
export const CANVAS_VAULT_NOTE_TYPE=NOTE_TYPE;

/** RAM unlock state belongs to this current account, never to a password/token in storage. */
export function createCanvasVaultClient({api,accountId,projectId='soty',isCurrent,onChange,store:providedStore,crypto=globalThis.crypto,clock=Date.now,setTimer=setTimeout,clearTimer=clearTimeout}={}){
  vaultCheck(api&&typeof api.request==='function'&&vaultId(accountId)&&vaultId(projectId)&&typeof isCurrent==='function');
  let disposed=false,local=providedStore;const sessions=new Map(),listeners=new Set(),epochs=new Map(),unlocks=new Map(),localWrites=new Set(),registrations=new Map(),recoveries=new Map();
  const store=()=>local??(local=createCanvasVaultLocalStore(accountId,projectId));
  const current=()=>!disposed&&isCurrent();
  const active=()=>vaultCheck(current(),'vault_account_changed');
  function emit(objectId,reason){for(const listener of [...listeners])try{listener(objectId,reason);}catch{/* One consumer cannot prevent the others scrubbing their DOM. */}
    if(current()&&onChange)queueMicrotask(()=>{if(current())void Promise.resolve().then(()=>onChange(objectId)).catch(()=>{});});}
  function forget(objectId,reason='locked'){
    epochs.set(objectId,(epochs.get(objectId)??0)+1);const session=sessions.get(objectId);if(session){clearTimer(session.timer);sessions.delete(objectId);session.key=null;}
    emit(objectId,reason);
  }
  function sessionFor(objectId){
    const value=sessions.get(objectId);if(!value)return null;
    if(!current()||value.expiresAt<=clock()){forget(objectId,current()?'expired':'account');return null;}return value;
  }
  const unlocked=objectId=>sessionFor(objectId)!==null;
  function required(objectId){active();vaultCheck(vaultId(objectId));const value=sessionFor(objectId);vaultCheck(value,'canvas_object_locked');return value;}
  async function request(operation,args){active();const value=await api.request(operation,{...args,expectedAccountId:accountId});active();return value;}
  async function guarded(objectId,operation,args){const session=required(objectId);try{const value=await request(operation,args);vaultCheck(sessionFor(objectId)===session,'canvas_object_locked');return value;}
    catch(error){if(authError(error)&&sessions.get(objectId)===session)forget(objectId,'denied');throw error;}}
  async function noteIdentity(objectId){const hash=new Uint8Array(await crypto.subtle.digest('SHA-256',encoder.encode(objectId)));return'vault_'+[...hash].map(byte=>byte.toString(16).padStart(2,'0')).join('');}
  async function install(objectId,keySession,value,epoch){
    vaultCheck(value?.objectId===objectId&&value.locked===false&&Number.isSafeInteger(value.revision)&&value.revision>=1&&Number.isSafeInteger(value.expiresAt)&&value.expiresAt>clock(),'vault_unlock_receipt_invalid');
    const noteId=await noteIdentity(objectId);active();vaultCheck((epochs.get(objectId)??0)===epoch,'vault_unlock_cancelled');
    vaultCheck(sameCanvasVaultScope(value.localKeyScope,keySession.sourceScope),'vault_local_scope_changed');
    const old=sessions.get(objectId);if(old)clearTimer(old.timer);const session={...keySession,revision:value.revision,noteId,expiresAt:value.expiresAt,timer:null};
    session.timer=setTimer(()=>{if(sessions.get(objectId)===session)forget(objectId,'expired');},Math.max(1,value.expiresAt-clock()));session.timer?.unref?.();sessions.set(objectId,session);epochs.set(objectId,epoch+1);emit(objectId,'unlocked');return{objectId,expiresAt:value.expiresAt};
  }
  function localTask(callback){const task=(async()=>callback())();localWrites.add(task);void task.finally(()=>localWrites.delete(task)).catch(()=>{});return task;}
  async function drainLocal(){await Promise.resolve();while(localWrites.size){await Promise.allSettled([...localWrites]);await Promise.resolve();}}
  function registerSource(slot){
    if(registrations.has(slot))return registrations.get(slot);const id=JSON.stringify([store().scopeKey??globalThis.location?.origin??'local',accountId,projectId,slot]);
    const handle={async drain(){await drainLocal();},retire(){for(const [objectId,value]of [...sessions])if(value.profile.slot===slot)forget(objectId,'rotate');},retireStale(profileId){for(const [objectId,value]of [...sessions])if(value.profile.slot===slot&&value.profile.profileId!==profileId)forget(objectId,'profile');}};
    if(!sourceClients.has(id))sourceClients.set(id,new Set());sourceClients.get(id).add(handle);registrations.set(slot,{id,handle});return registrations.get(slot);
  }
  async function drainSource(slot){const {id}=registerSource(slot);await drainCanvasVaultDrafts(id);for(const handle of [...sourceClients.get(id)])await handle.drain();}
  function retireSource(slot){const {id}=registerSource(slot);for(const handle of [...sourceClients.get(id)])handle.retire();}
  async function scopeAuthority(objectId,expected,{unlocked=false}={}){
    const value=await request('canvas.vault.status',{objectId,includeLocalKeyScope:true});
    vaultCheck(value?.objectId===objectId&&value.protection?.kind==='password'&&value.state!=='sealing','vault_local_scope_unavailable');const scope=canvasVaultScope(value.localKeyScope);
    vaultCheck(!expected||sameCanvasVaultScope(scope,expected),'vault_local_scope_changed');if(unlocked&&value.locked!==false){forget(objectId,'locked');vaultCheck(false,'canvas_object_locked');}return value;
  }
  async function sourceState(objectId){
    active();vaultCheck(crypto?.subtle,'vault_secure_context_required');vaultCheck(typeof store().inspectScopes==='function'&&typeof store().ensureScopedProfile==='function','vault_local_unavailable');
    // No password/network mutation while the durable version fence is blocked.
    await store().inspectScopes();active();const value=await scopeAuthority(objectId),scope=canvasVaultScope(value.localKeyScope),slot=await canvasVaultScopeSlot(scope,projectId,crypto);active();
    const state=await store().ensureScopedProfile(scope,canvasVaultProfile(accountId,projectId,slot,crypto),{objectId});active();registerSource(slot);return{...state,scope,slot,value};
  }
  function recoveryResult(objectId,value={knownSource:'ready',unresolvedLegacy:[]}){
    const unresolvedLegacy=value.unresolvedLegacy??[],result={schema:'soty.vault.local-recovery-state.v1',objectId,knownSource:value.knownSource,complete:value.knownSource==='ready'&&unresolvedLegacy.length===0,needsPriorLocalPassword:unresolvedLegacy.some(value=>value.reason==='prior-local-password'||value.reason==='source-unknown'),unresolvedLegacy};recoveries.set(objectId,result);return vaultClone(result);
  }
  async function migrateLegacy(objectId,state,key,priorPassword,epoch){
    const same=()=>current()&&(epochs.get(objectId)??0)===epoch;
    const result=await migrateCanvasVaultLegacy({store:store(),accountId,projectId,objectId,scope:state.scope,session:key,priorPassword,request,authorize:()=>scopeAuthority(objectId,state.scope,{unlocked:true}),isCurrent:same,crypto});
    vaultCheck(same(),'vault_unlock_cancelled');return recoveryResult(objectId,result);
  }
  function recoveryScope(objectId,session){const epoch=epochs.get(objectId)??0;return createCanvasVaultRecoveryScope({objectId,source:session.sourceScope,profile:session.profile,isCurrent:()=>current()&&sessionFor(objectId)===session&&(epochs.get(objectId)??0)===epoch});}
  async function recoverRetained(objectId,state,key,epoch){
    const {id}=registerSource(state.slot),same=()=>current()&&(epochs.get(objectId)??0)===epoch;
    for(const handle of [...sourceClients.get(id)])handle.retireStale(key.profile.profileId);
    await recoverCanvasVaultDrafts(id,{isCurrent:same,async write({objectId:origin,noteId,draft}){
      vaultCheck(noteId===await noteIdentity(origin)&&vaultId(draft.branchId),'vault_local_scope_changed');await scopeAuthority(objectId,state.scope,{unlocked:true});vaultCheck(same(),'vault_unlock_cancelled');
      await localTask(async()=>{const recordId='draft_'+draft.branchId,old=await store().get(recordId);let version=old?.version??0;
        if(old){vaultCheck(old.slot===state.slot&&old.purpose==='draft','vault_local_scope_changed');const previous=await openCanvasVaultRecord(key,old,{crypto});
          if(previous.generation>=draft.generation&&JSON.stringify(previous.note)===JSON.stringify(draft.note)&&JSON.stringify(previous.pending)===JSON.stringify(draft.pending))return;vaultCheck(previous.generation<=draft.generation,'vault_local_conflict');}
        const record=await sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:state.slot,recordId,purpose:'draft',version:version+1},draft,{crypto});vaultCheck(await store().put(record,version),'vault_local_conflict');
      });await scopeAuthority(objectId,state.scope,{unlocked:true});vaultCheck(same(),'vault_unlock_cancelled');
    }});
  }
  async function unlock(objectId,password,{ttlMs}={}){
    active();vaultCheck(vaultId(objectId));if(unlocks.has(objectId))throw new CanvasVaultError('vault_unlock_busy');vaultCheck(unlocks.size<2,'vault_busy');
    const epoch=epochs.get(objectId)??0;
    const task=(async()=>{
      // Secure crypto and durable key profile are checked before sending password/network mutation.
      const state=await sourceState(objectId);vaultCheck(state.control.phase==='ready','vault_local_rotation_pending');const profile=state.profile;active();
      const keySession=await deriveCanvasVaultKey(profile,password,{crypto});active();
      // A former private branch is never silently overwritten by a changed password.
      for(const record of await store().list(profile.slot))await openCanvasVaultRecord(keySession,record,{crypto});
      active();vaultCheck((epochs.get(objectId)??0)===epoch,'vault_unlock_cancelled');
      const value=await request('canvas.vault.unlock',{objectId,password,includeLocalKeyScope:true,...(ttlMs===undefined?{}:{ttlMs})});
      vaultCheck((epochs.get(objectId)??0)===epoch,'vault_unlock_cancelled');
      vaultCheck(sameCanvasVaultScope(value.localKeyScope,state.scope),'vault_local_scope_changed');
      await migrateLegacy(objectId,state,keySession,password,epoch);const fresh=await store().ensureScopedProfile(state.scope,profile,{objectId});vaultCheck(fresh.control.phase==='ready'&&fresh.profile.profileId===profile.profileId,'vault_local_rotation_pending');
      await recoverRetained(objectId,state,keySession,epoch);const currentValue=await scopeAuthority(objectId,state.scope,{unlocked:true});return install(objectId,{...keySession,sourceScope:state.scope},currentValue,epoch);
    })().finally(()=>unlocks.delete(objectId));unlocks.set(objectId,task);return task;
  }
  async function status(objectId,{includeCapabilities=false}={}){const value=await request('canvas.vault.status',{objectId,...(includeCapabilities?{includeCapabilities:true}:{})});if(value?.locked!==false)forget(objectId,'locked');return value;}
  async function relock(objectId,{scope='device',requestId=crypto.randomUUID()}={}){active();vaultCheck(vaultId(objectId));forget(objectId,'relock');return request('canvas.vault.relock',{requestId,objectId,scope});}
  async function protect(objectId,expectedObjectRevision,password,{requestId=crypto.randomUUID()}={}){
    active();vaultCheck(crypto?.subtle,'vault_secure_context_required');const result=await request('canvas.vault.protect',{objectId,expectedObjectRevision,password,requestId});forget(objectId,result?.schema==='soty.vault-conversion.v1'&&result.protected!==true?'sealing':'protected');return result;
  }
  async function rotate(objectId,{oldPassword,newPassword,expectedRevision,requestId}={}){
    active();vaultCheck(vaultId(objectId)&&crypto?.subtle,'vault_secure_context_required');vaultCheck(unlocks.size<2,'vault_busy');
    let state=await sourceState(objectId),rows=await store().list(state.slot),record=rows.find(value=>value.purpose==='rotate'),key,payload;
    if(!record){
      await drainSource(state.slot);await unlock(objectId,oldPassword);const session=required(objectId);state=await sourceState(objectId);key={profile:session.profile,key:session.key};
      rows=await store().list(state.slot);record=rows.find(value=>value.purpose==='rotate');
      if(!record){vaultCheck(expectedRevision===undefined||expectedRevision===session.revision,'vault_revision_conflict');
        payload={schema:'soty.vault.rotate.v1',objectId,requestId:requestId??crypto.randomUUID(),expectedRevision:session.revision,newProfile:canvasVaultProfile(accountId,projectId,state.slot,crypto)};
        const recordId='rotate_'+(await noteIdentity(state.slot));record=await sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:state.slot,recordId,purpose:'rotate',version:1},payload,{crypto});
      }
    }
    if(!key)key=await deriveCanvasVaultKey(state.profile,oldPassword,{crypto});
    if(!payload)payload=await openCanvasVaultRecord(key,record,{crypto});
    vaultCheck(payload?.schema==='soty.vault.rotate.v1'&&vaultId(payload.requestId)&&vaultId(payload.objectId)&&Number.isSafeInteger(payload.expectedRevision)&&payload.expectedRevision>=1&&(!requestId||payload.requestId===requestId),'vault_local_invalid');
    vaultCheck(payload.newProfile?.accountId===accountId&&payload.newProfile.projectId===projectId&&payload.newProfile.slot===state.slot,'vault_local_scope_changed');
    const nextKey=await deriveCanvasVaultKey(payload.newProfile,newPassword,{crypto});active();await drainSource(state.slot);await scopeAuthority(objectId,state.scope);
    const latest=await store().inspectScopes(),admission=await store().beginSourceRekey(state.slot,key.profile.profileId,record,{inventoryGeneration:latest.inventory?.generation});
    // Admission may already exist from another caller/reload. Its actual
    // encrypted journal, never the proposed args, owns the Source command.
    record=await store().get(admission.recordId);payload=await openCanvasVaultRecord(key,record,{crypto});
    vaultCheck(payload?.schema==='soty.vault.rotate.v1'&&payload.newProfile?.profileId===nextKey.profile.profileId&&(!requestId||payload.requestId===requestId),'vault_local_conflict');
    retireSource(state.slot);const epoch=epochs.get(objectId)??0;await scopeAuthority(objectId,state.scope);let result,originalAvailable=true;
    if(payload.objectId!==objectId){
      vaultCheck(state.scope.kind==='source','vault_local_scope_changed');const original=(await request('canvas.objects.get',{objectId:payload.objectId}))?.object;
      vaultCheck(original?.ownerId===accountId,'vault_local_scope_changed');const originalScope=await request('canvas.vault.bindingScope',{binding:original.binding});
      vaultCheck(sameCanvasVaultScope(originalScope?.localKeyScope,state.scope),'vault_local_scope_changed');originalAvailable=original.state==='active';
    }
    if(originalAvailable)try{result=await request('canvas.vault.rotate',{requestId:payload.requestId,objectId:payload.objectId,expectedRevision:payload.expectedRevision,oldPassword,newPassword});}
    catch(error){
      if(vaultErrorCode(error)==='vault_revision_conflict')try{
        // A definite CAS refusal is different from a lost transport ACK. Fresh
        // old-password auth + missing original command proves safe cancellation.
        const unchanged=await request('canvas.vault.unlock',{objectId,password:oldPassword,includeLocalKeyScope:true});vaultCheck(sameCanvasVaultScope(unchanged.localKeyScope,state.scope),'vault_local_scope_changed');
        const outcome=await request('canvas.vault.rotateReceipt',{commandId:payload.requestId,witnessObjectId:objectId});
        if(outcome?.state==='missing'&&await store().cancelSourceRekey(admission,record.version)){
          await recoverRetained(objectId,state,key,epoch);const fresh=await scopeAuthority(objectId,state.scope,{unlocked:true});await install(objectId,{...key,sourceScope:state.scope},fresh,epoch);
        }
      }catch{/* Uncertain cancellation remains durable/pending; never discard it. */}
      throw error;
    }
    else{
      // A read outcome with a CURRENT live Source witness B can confirm A's
      // old command after A was removed. It never submits a rotate through B.
      const grant=await request('canvas.vault.unlock',{objectId,password:newPassword,includeLocalKeyScope:true});vaultCheck(sameCanvasVaultScope(grant.localKeyScope,state.scope),'vault_local_scope_changed');
      const outcome=await request('canvas.vault.rotateReceipt',{commandId:payload.requestId,witnessObjectId:objectId});
      vaultCheck(outcome?.state==='committed'&&outcome.commandId===payload.requestId&&sameCanvasVaultScope(outcome.localKeyScope,state.scope),'vault_local_rotation_pending');result={replayed:true,receipt:outcome.receipt,current:outcome.current};
    }
    vaultCheck(result?.current?.revision>payload.expectedRevision&&(state.scope.kind!=='source'||result.current.vaultId===state.scope.id),'vault_write_receipt_invalid');
    const grant=await request('canvas.vault.unlock',{objectId,password:newPassword,includeLocalKeyScope:true});vaultCheck(sameCanvasVaultScope(grant.localKeyScope,state.scope),'vault_local_scope_changed');
    let migrated=false;for(let attempt=0;attempt<4&&!migrated;attempt++){
      const records=await store().list(state.slot),next=[];for(const value of records)if(value.recordId!==record.recordId){const body=await openCanvasVaultRecord(key,value,{crypto}),{cipher,...header}=value;next.push(await sealCanvasVaultRecord(nextKey,{...header,version:header.version+1},body,{crypto}));}
      active();vaultCheck((epochs.get(objectId)??0)===epoch,'vault_unlock_cancelled');await scopeAuthority(objectId,state.scope,{unlocked:true});
      migrated=await store().commitSourceRekey(admission,nextKey.profile,records,next,{retire:records.filter(value=>value.recordId===record.recordId).map(value=>({recordId:value.recordId,version:value.version}))});
    }
    vaultCheck(migrated,'vault_local_rotation_pending');await recoverRetained(objectId,state,nextKey,epoch);const value=await scopeAuthority(objectId,state.scope,{unlocked:true});await install(objectId,{...nextKey,sourceScope:state.scope},value,epoch);
    return{...result,localRecovery:await localRecoveryState(objectId)};
  }
  async function localRecoveryState(objectId){
    const state=await sourceState(objectId),snapshot=await store().inspectScopes(),unresolvedLegacy=[];let knownSource=state.control.phase==='ready'?'ready':'pending';
    for(const entry of snapshot.inventory?.profiles??[]){if(entry.state!=='pending'||entry.slot===state.slot)continue;const records=await store().listExact(entry.slot);if(!records.length)continue;
      if(records.every(value=>value.purpose==='create')){unresolvedLegacy.push({slot:entry.slot,kind:'create',reason:'source-unknown'});continue;}
      if(state.value.ownerId!==accountId)continue;
      let object;try{object=(await request('canvas.objects.get',{objectId:entry.slot}))?.object;}catch(error){if(![403,404].includes(error?.status)&&!['canvas_owner_required','canvas_object_unavailable'].includes(vaultErrorCode(error)))throw error;}
      if(!object){unresolvedLegacy.push({slot:entry.slot,kind:'object',reason:'object-unavailable'});continue;}
      const binding=await request('canvas.vault.bindingScope',{binding:object.binding});if(sameCanvasVaultScope(binding?.localKeyScope,state.scope)){knownSource='pending';unresolvedLegacy.push({slot:entry.slot,kind:'object',reason:'prior-local-password'});}
    }
    if((await store().list(state.slot)).some(value=>value.purpose==='rotate'))knownSource='pending';
    return recoveryResult(objectId,{knownSource,unresolvedLegacy});
  }
  async function recoverLocalPassword(objectId,{currentSourcePassword,priorLocalPassword}={}){
    active();let state=await sourceState(objectId),epoch=epochs.get(objectId)??0;
    // Fresh current Object/password authorization PRECEDES any old-local
    // decryption. This operation never sends a Source write or rotate.
    const grant=await request('canvas.vault.unlock',{objectId,password:currentSourcePassword,includeLocalKeyScope:true});vaultCheck(sameCanvasVaultScope(grant.localKeyScope,state.scope)&&(epochs.get(objectId)??0)===epoch,'vault_local_scope_changed');
    let rows=await store().list(state.slot),intent=rows.find(value=>value.purpose==='rotate'),payload,admission;
    const currentKey=await deriveCanvasVaultKey(state.profile,currentSourcePassword,{crypto});let alreadyCurrent=true;
    try{for(const value of rows)await openCanvasVaultRecord(currentKey,value,{crypto});}catch(error){if(vaultErrorCode(error)!=='vault_local_password_or_data_invalid')throw error;alreadyCurrent=false;}
    let key=alreadyCurrent?currentKey:await deriveCanvasVaultKey(state.profile,priorLocalPassword,{crypto});active();vaultCheck((epochs.get(objectId)??0)===epoch,'vault_unlock_cancelled');
    for(const value of rows)await openCanvasVaultRecord(key,value,{crypto});
    let nextKey;
    if(intent){
      payload=await openCanvasVaultRecord(key,intent,{crypto});vaultCheck(['soty.vault.rotate.v1','soty.vault.local-rekey.v1'].includes(payload?.schema)&&payload.newProfile?.slot===state.slot,'vault_local_invalid');
      if(payload.schema==='soty.vault.rotate.v1'){
        const outcome=await request('canvas.vault.rotateReceipt',{commandId:payload.requestId,witnessObjectId:objectId});
        if(outcome?.state!=='committed')return recoveryResult(objectId,{knownSource:'pending',unresolvedLegacy:[{slot:state.slot,kind:'object',reason:'source-unknown'}]});
        vaultCheck(outcome.commandId===payload.requestId&&outcome.receipt?.revision>payload.expectedRevision&&sameCanvasVaultScope(outcome.localKeyScope,state.scope),'vault_local_scope_changed');
      }
      nextKey=await deriveCanvasVaultKey(payload.newProfile,currentSourcePassword,{crypto});
    }else{
      if(alreadyCurrent)key=currentKey;
      else{
        await drainSource(state.slot);payload={schema:'soty.vault.local-rekey.v1',objectId,sourceScope:state.scope,newProfile:canvasVaultProfile(accountId,projectId,state.slot,crypto)};
        const recordId='local_rekey_'+(await noteIdentity(state.slot));intent=await sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:state.slot,recordId,purpose:'rotate',version:1},payload,{crypto});nextKey=await deriveCanvasVaultKey(payload.newProfile,currentSourcePassword,{crypto});
      }
    }
    if(intent){
      await scopeAuthority(objectId,state.scope,{unlocked:true});const snapshot=await store().inspectScopes();admission=await store().beginSourceRekey(state.slot,key.profile.profileId,intent,{inventoryGeneration:snapshot.inventory?.generation,mode:'password-recovery'});
      intent=await store().get(admission.recordId);payload=await openCanvasVaultRecord(key,intent,{crypto});vaultCheck(payload?.newProfile?.profileId===nextKey.profile.profileId,'vault_local_conflict');
      retireSource(state.slot);epoch=epochs.get(objectId)??0;let committed=false;
      for(let attempt=0;attempt<4&&!committed;attempt++){
        rows=await store().list(state.slot);const next=[];for(const value of rows)if(value.recordId!==intent.recordId){const body=await openCanvasVaultRecord(key,value,{crypto}),{cipher,...header}=value;next.push(await sealCanvasVaultRecord(nextKey,{...header,version:value.version+1},body,{crypto}));}
        await scopeAuthority(objectId,state.scope,{unlocked:true});vaultCheck((epochs.get(objectId)??0)===epoch,'vault_unlock_cancelled');committed=await store().commitSourceRekey(admission,nextKey.profile,rows,next,{retire:[{recordId:intent.recordId,version:intent.version}]});
      }
      vaultCheck(committed,'vault_local_rotation_pending');key=nextKey;state=await sourceState(objectId);
    }
    await migrateLegacy(objectId,state,key,priorLocalPassword,epoch);await recoverRetained(objectId,state,key,epoch);
    const currentValue=await scopeAuthority(objectId,state.scope,{unlocked:true});await install(objectId,{...key,sourceScope:state.scope},currentValue,epoch);return localRecoveryState(objectId);
  }
  async function readContent(objectId,{signal,fresh=false}={}){
    const session=required(objectId),epoch=epochs.get(objectId)??0,revision=session.revision,revoked=new AbortController();let value;
    vaultCheck((signal===undefined||signal instanceof AbortSignal)&&typeof fresh==='boolean','vault_content_reader_invalid');
    const same=()=>current()&&sessionFor(objectId)===session&&(epochs.get(objectId)??0)===epoch;
    const retire=id=>{if(id===objectId&&!same())revoked.abort();},cancel=()=>revoked.abort();listeners.add(retire);
    signal?.addEventListener('abort',cancel,{once:true});if(signal?.aborted)cancel();const readSignal=revoked.signal;
    try{
      let expectedRevision=revision;
      if(fresh){
        const head=await guarded(objectId,'canvas.vault.status',{objectId});
        vaultCheck(head?.objectId===objectId&&head.locked===false&&Number.isSafeInteger(head.revision)&&head.revision>=revision,'vault_content_changed');
        expectedRevision=head.revision;
      }
      // An explicit refresh captures a fresh authorized head. Every part must
      // still match that exact head, with the original local session unchanged.
      value=await readCanvasVaultContent({objectId,signal:readSignal,isCurrent:same,expected:{revision:expectedRevision},readPart:({index})=>guarded(objectId,'canvas.objects.content',{objectId,part:'vault',index})});
      vaultCheck(!readSignal.aborted,'vault_content_cancelled');vaultCheck(same(),'canvas_object_locked');vaultCheck(session.revision===revision,'vault_content_changed');
      session.revision=value.revision;return value;
    }catch(error){value?.bytes.fill(0);if(vaultErrorCode(error)==='vault_content_cancelled'&&!same())throw new CanvasVaultError(current()?'canvas_object_locked':'vault_account_changed');throw error;}
    finally{listeners.delete(retire);signal?.removeEventListener('abort',cancel);}
  }
  async function readNote(objectId,{fresh=false}={}){
    const session=required(objectId),value=await readContent(objectId,{fresh});try{vaultCheck(value.contentType===NOTE_TYPE,'vault_note_invalid');let parsed;try{parsed=JSON.parse(decoder.decode(value.bytes));}catch{throw new CanvasVaultError('vault_note_invalid');}
      const doc=noteDocument(parsed),metadata=noteMetadata(parsed);session.sourceNote=metadata;return{...doc,createdAt:metadata.createdAt??0,updatedAt:metadata.updatedAt??0,noteId:session.noteId,revision:value.revision,preview:(doc.body.trim()||doc.items.map(item=>item.text).join(' · ')).replace(/\s+/gu,' ').slice(0,180)};
    }finally{value.bytes.fill(0);}
  }
  async function writeNote(objectId,args){
    const session=required(objectId);vaultCheck(args.noteId===session.noteId&&vaultId(args.mutationId)&&Number.isSafeInteger(args.expectedRevision)&&args.expectedRevision>=1,'vault_note_invalid');
    const key={profile:session.profile,key:session.key},recordId='write_'+args.mutationId,document=noteDocument(args);let intent;
    await localTask(async()=>{let record=await store().get(recordId);
    if(record){vaultCheck(record.purpose==='write'&&record.slot===session.profile.slot,'vault_local_scope_changed');intent=await openCanvasVaultRecord(key,record,{crypto});}
    else{
      if(!session.sourceNote)await readNote(objectId);required(objectId);
      let branch=null;for(const value of await store().list(session.profile.slot))if(value.purpose==='draft'){const draft=await openCanvasVaultRecord(key,value,{crypto});if(draft.pending?.mutationId===args.mutationId){branch={branchId:draft.branchId,generation:draft.pending.generation};break;}}
      const timestamp=clock(),metadata={noteId:session.sourceNote?.noteId??session.noteId,createdAt:session.sourceNote?.createdAt??timestamp,updatedAt:timestamp,revision:args.expectedRevision+1};
      intent={schema:'soty.vault.note-write.v1',objectId,mutationId:args.mutationId,expectedRevision:args.expectedRevision,document,metadata,branch,content:canvasVaultNoteContent({...document,...metadata})};
      record=await sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:session.profile.slot,recordId,purpose:'write',version:1},intent,{crypto});
      if(!await store().put(record,0)){record=await store().get(recordId);vaultCheck(record,'vault_local_conflict');intent=await openCanvasVaultRecord(key,record,{crypto});}
    }
    vaultCheck(intent?.schema==='soty.vault.note-write.v1'&&intent.objectId===objectId&&intent.mutationId===args.mutationId&&intent.expectedRevision===args.expectedRevision&&JSON.stringify(intent.document)===JSON.stringify(document),'vault_local_conflict');});
    try{const result=await guarded(objectId,'canvas.vault.write',{requestId:args.mutationId,objectId,expectedRevision:args.expectedRevision,content:intent.content});
      const acknowledged=result?.receipt?.revision,head=result?.current?.revision;
      vaultCheck(Number.isSafeInteger(acknowledged)&&acknowledged>args.expectedRevision&&Number.isSafeInteger(head)&&head>=acknowledged,'vault_write_receipt_invalid');
      session.revision=head;session.sourceNote=intent.metadata;
      // A replay's receipt confirms A, not the later body currently at head B.
      // Ask the host to refresh metadata without retiring a valid Object grant
      // or replacing the editor's captured branch with an automatic rebase.
      if(head!==acknowledged&&onChange)queueMicrotask(()=>{if(current()&&sessionFor(objectId)===session)void Promise.resolve(onChange(objectId)).catch(()=>{});});
      return{noteId:session.noteId,revision:acknowledged,updatedAt:intent.metadata.updatedAt};
    }catch(error){if(vaultErrorCode(error)==='vault_revision_conflict')throw new CanvasVaultError('notes_revision_conflict');throw error;}
  }
  function draftStore(objectId){
    const session=required(objectId),keySession={profile:session.profile,key:session.key};
    const {id}=registerSource(session.profile.slot);
    return{native:{register(noteId,port){vaultCheck(noteId===session.noteId,'vault_local_scope_changed');return registerCanvasVaultDraft({scopeKey:id,objectId,noteId,port,isCurrent:()=>{const value=sessionFor(objectId);return current()&&value?.profile.profileId===session.profile.profileId&&sameCanvasVaultScope(value?.sourceScope,session.sourceScope);}});},sessions(noteId){return canvasVaultDraftSessions(id,objectId,noteId);}},put(draft){return localTask(async()=>{vaultCheck(draft?.note?.noteId===session.noteId&&vaultId(draft.branchId),'vault_local_invalid');
      const recordId='draft_'+draft.branchId,old=await store().get(recordId),version=(old?.version??0)+1;
      const record=await sealCanvasVaultRecord(keySession,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:session.profile.slot,recordId,purpose:'draft',version},draft,{crypto});
      vaultCheck(await store().put(record,version-1),'vault_local_conflict');
      // Exact API bytes remain durable until the NoteSession's post-ACK state is durable too.
      for(const value of await store().list(session.profile.slot))if(value.purpose==='write'){
        const intent=await openCanvasVaultRecord(keySession,value,{crypto});
        const settled=Array.isArray(draft.retiredMutations)&&draft.retiredMutations.some(item=>item.mutationId===intent.mutationId&&Number.isSafeInteger(item.generation)&&item.generation>0&&item.generation<=draft.committedGeneration);
        if(intent.objectId===objectId&&intent.expectedRevision<draft.note.revision&&(settled||intent.branch?.branchId===draft.branchId&&intent.branch.generation<=draft.committedGeneration))await store().remove(value.recordId,value.version);
      }
    });},async list(){required(objectId);const result=[];for(const record of await store().list(session.profile.slot))if(record.purpose==='draft'){const draft=await openCanvasVaultRecord(keySession,record,{crypto});if(draft.note?.noteId===session.noteId)result.push(draft);}required(objectId);return result.sort((a,b)=>b.savedAt-a.savedAt);},
    remove(noteId,branchId,savedAt){return localTask(async()=>{const record=await store().get('draft_'+branchId);if(!record||record.slot!==session.profile.slot||record.purpose!=='draft')return;
      const draft=await openCanvasVaultRecord(keySession,record,{crypto});if(draft.note?.noteId===noteId&&(savedAt===undefined||draft.savedAt===savedAt))await store().remove(record.recordId,record.version);
    });}};
  }
  async function archiveNativeDrafts(objectId,source,drafts){
    const session=required(objectId);await readNote(objectId);
    vaultCheck(sessionFor(objectId)===session&&source?.ownerId===accountId&&source.providerId==='soty.notes'&&source.ref===session.sourceNote?.noteId,'vault_native_recovery_scope');
    const key={profile:session.profile,key:session.key};
    const result=await localTask(()=>archiveNativeNoteDrafts({session:key,store:store(),accountId,projectId,objectId,source,drafts,cryptoScope:recoveryScope(objectId,session),crypto}));
    vaultCheck(sessionFor(objectId)===session,'canvas_object_locked');return result;
  }
  async function nativeRecoveries(objectId){
    const session=required(objectId);if(!(await store().list(session.profile.slot)).some(value=>value.recordId.startsWith('native_recovery_')))return[];
    await readNote(objectId);const values=await readNativeNoteRecoveries({session:{profile:session.profile,key:session.key},store:store(),objectId,accountId,cryptoScope:recoveryScope(objectId,session),crypto});
    await scopeAuthority(objectId,session.sourceScope,{unlocked:true});vaultCheck(sessionFor(objectId)===session,'canvas_object_locked');return values.filter(value=>value.source.ref===session.sourceNote?.noteId&&value.source.ownerId===accountId);
  }
  async function restoreNativeRecovery(objectId,recordId){
    const session=required(objectId),current=await readNote(objectId),recovery=(await nativeRecoveries(objectId)).find(value=>value.recordId===recordId);vaultCheck(recovery,'vault_native_recovery_unavailable');
    const branchId='recovered_'+(await noteIdentity(recordId)).slice(6,54),draft={note:{...current,...noteDocument(recovery.draft.note)},branchId,generation:1,committedGeneration:0,pending:null,conflict:false,savedAt:clock()};
    // An explicit recovery creates a NEW Vault outbox; the old Notes mutation
    // remains only in its encrypted archive and is never replayed to another API.
    await draftStore(objectId).put(draft);vaultCheck(sessionFor(objectId)===session,'canvas_object_locked');return{branchId};
  }
  async function pruneNativeRecoveries(objectId){
    const recoveries=await nativeRecoveries(objectId);if(!recoveries.length)return;
    const current=await readNote(objectId),document=JSON.stringify(noteDocument(current));
    for(const recovery of recoveries)if(JSON.stringify(noteDocument(recovery.draft.note))===document)await localTask(()=>store().remove(recovery.recordId,recovery.version));
    required(objectId);
  }
  async function creationRecoveryContext(objectId,expectedSource){
    const session=required(objectId),epoch=epochs.get(objectId)??0;
    const same=()=>current()&&sessionFor(objectId)===session&&(epochs.get(objectId)??0)===epoch;
    const active=()=>vaultCheck(same(),'canvas_object_locked');
    async function validate(source){
      active();const result=await guarded(objectId,'canvas.objects.get',{objectId});active();const object=result?.object;
      if(!(object?.objectId===objectId&&object.ownerId===accountId&&object.state==='active'&&['soty.notes','soty.materials'].includes(object.binding?.providerId)&&object.protection?.kind==='password')){
        forget(objectId,'denied');throw new CanvasVaultError('vault_native_recovery_scope');
      }
      const actual={providerId:object.binding.providerId,ref:object.binding.ref,ownerId:object.ownerId};
      vaultCheck(!source||source.providerId===actual.providerId&&source.ref===actual.ref&&source.ownerId===actual.ownerId,'vault_native_recovery_scope');
      const protection=await status(objectId);active();vaultCheck(protection.locked===false&&protection.protection?.kind==='password','canvas_object_locked');return actual;
    }
    const source=await validate(expectedSource);
    return{options:{session:{profile:session.profile,key:session.key},store:store(),accountId,projectId,objectId,source,isCurrent:same,cryptoScope:recoveryScope(objectId,session),crypto},
      async recheck(){await validate(source);active();}};
  }
  async function archiveCreationIntents(objectId,source,records){
    return localTask(async()=>{const context=await creationRecoveryContext(objectId,source),receipt=await archiveLocalCreations({...context.options,records});await context.recheck();return receipt;});
  }
  async function verifyCreationRecoveries(objectId,source,records,receipt){
    // A read awaiting Source authority must not join the local writer drain:
    // another SDK's rotation would otherwise wait for that very response.
    // Exact session/profile checks reject the late read after rotation instead.
    const context=await creationRecoveryContext(objectId,source),current=await verifyLocalCreations({...context.options,records,...(receipt?{receipt}:{})});await context.recheck();return current;
  }
  async function creationRecoveries(objectId,{archiveId}={}){
    const context=await creationRecoveryContext(objectId),values=await readLocalCreations({...context.options,...(archiveId?{archiveId}:{})});await context.recheck();return values;
  }
  async function restoreCreationRecovery(objectId,archiveId,operationId){
    const session=required(objectId),current=await readNote(objectId),values=await creationRecoveries(objectId,{archiveId});
    vaultCheck(sessionFor(objectId)===session,'canvas_object_locked');const row=values.flatMap(value=>value.records).find(value=>value.operationId===operationId);
    vaultCheck(row?.kind==='note'&&row.stage==='source'&&row.sourceIntent,'vault_native_recovery_unavailable');
    const branchId='creation_'+(await noteIdentity(archiveId+'.'+operationId)).slice(6,54),draft={note:{...current,...noteDocument(row.sourceIntent)},branchId,generation:1,committedGeneration:0,pending:null,conflict:false,savedAt:clock()};
    await draftStore(objectId).put(draft);vaultCheck(sessionFor(objectId)===session,'canvas_object_locked');return{branchId};
  }
  return{unlock,status,relock,protect,rotate,localRecoveryState,recoverLocalPassword,readContent,readNote,writeNote,draftStore,archiveNativeDrafts,nativeRecoveries,restoreNativeRecovery,pruneNativeRecoveries,
    archiveCreationIntents,verifyCreationRecoveries,creationRecoveries,restoreCreationRecovery,isUnlocked:unlocked,generation:objectId=>epochs.get(objectId)??0,noteId:objectId=>sessionFor(objectId)?.noteId??null,
    expiresAt:objectId=>sessionFor(objectId)?.expiresAt??null,
    invalidate(objectId,reason='projection'){forget(objectId,reason);},
    subscribe(listener){listeners.add(listener);return()=>listeners.delete(listener);},
    dispose(){if(disposed)return;disposed=true;for(const objectId of [...sessions.keys()])forget(objectId,'dispose');listeners.clear();void drainLocal().finally(()=>{for(const{id,handle}of registrations.values()){const group=sourceClients.get(id);group?.delete(handle);if(group?.size===0)sourceClients.delete(id);}registrations.clear();if(!providedStore)local?.close?.();});},
  };
}
