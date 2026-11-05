import {validateCanvasMask} from '../../modules/canvas-objects/contract.mjs';
import {canvasCreateFileKind} from './canvas-create-flow.mjs';
import {normalizeCanvasWebmFile} from './canvas-webm-kind.mjs';
import {canvasVaultNoteContent} from './canvas-vault-client.mjs';
import {CanvasVaultError,CANVAS_VAULT_LOCAL_LIMITS,vaultCheck,vaultId,vaultErrorCode,vaultBase64,canvasVaultProfile,deriveCanvasVaultKey,sealCanvasVaultRecord,openCanvasVaultRecord,createCanvasVaultLocalStore} from './canvas-vault-local.mjs';
import {canvasVaultScope,canvasVaultScopeSlot} from './canvas-vault-scope.mjs';
const stages=['source','object','placement','complete'];
const dimensions=kind=>kind==='note'?{width:340,height:300}:kind==='audio'?{width:340,height:156}:kind==='video'?{width:420,height:280}:{width:360,height:280};
function validate(value){
  vaultCheck(value?.schema==='soty.vault.create.v1'&&vaultId(value.operationId)&&vaultId(value.sourceRequestId)&&vaultId(value.objectRequestId)&&stages.includes(value.stage)
    &&['note','image','audio','video'].includes(value.kind)&&Number.isSafeInteger(value.createdAt)&&value.createdAt>=0,'vault_create_journal_invalid');
  validateCanvasMask(value.presentation?.mask);vaultCheck(Number.isFinite(value.presentation.fontSize)&&value.presentation.fontSize>=8&&value.presentation.fontSize<=128,'vault_create_journal_invalid');
  if(value.stage==='source')vaultCheck(value.content&&typeof value.content.contentType==='string'&&typeof value.content.bytesBase64==='string','vault_create_journal_invalid');
  else vaultCheck(vaultId(value.sourceRef),'vault_create_journal_invalid');
  if(['placement','complete'].includes(value.stage))vaultCheck(vaultId(value.objectId),'vault_create_journal_invalid');
  if(value.stage==='complete')vaultCheck(vaultId(value.placement?.shortcutId)&&['saved','pending'].includes(value.placement.status)&&value.placement.localDurable===true,'vault_create_journal_invalid');
  return value;
}
/** Same Source → Object → canonical placement order; every local payload is ciphertext. */
export function createCanvasVaultCreateFlow({api,accountId,projectId='soty',isCurrent,place,onObjectReady,onProgress=()=>{},store:providedStore,crypto=globalThis.crypto}={}){
  vaultCheck(api&&typeof api.request==='function'&&vaultId(accountId)&&vaultId(projectId)&&typeof isCurrent==='function'&&typeof place==='function');
  let disposed=false,local=providedStore;const keys=new Map(),volatile=new Map(),flights=new Map();
  const store=()=>local??(local=createCanvasVaultLocalStore(accountId,projectId));
  const current=()=>vaultCheck(!disposed&&isCurrent(),'vault_account_changed');
  const emit=(id,phase,localDurable)=>{if(!disposed)try{onProgress({operationId:id,phase,localDurable});}catch{/* Observers do not own the committed journal. */}};
  async function request(op,args){current();const value=await api.request(op,{...args,expectedAccountId:accountId});current();return value;}
  async function keyFor(operationId,password,record){
    current();const slot=record?.slot??operationId,candidate=volatile.get(operationId)?.profile??canvasVaultProfile(accountId,projectId,slot,crypto),profile=await store().ensureProfile(candidate);current();
    const cached=keys.get(operationId);if(cached?.profile.profileId===profile.profileId&&cached.profile.salt===profile.salt)return cached;keys.delete(operationId);
    const value=await deriveCanvasVaultKey(profile,password,{crypto});current();return value;
  }
  async function read(operationId,password){
    current();let record;try{record=await store().get(operationId);}catch(error){if(!volatile.has(operationId))throw error;}
    let memory=volatile.get(operationId);
    if(memory&&record&&memory.profile.profileId!==record.cipher.profileId){
      // Only stage/receipt progress is volatile after the initial durable
      // Source intent. A Source rekey's committed row wins over old-key progress.
      volatile.delete(operationId);keys.delete(operationId);memory=null;
    }
    if(memory&&(!record||memory.record.version>record.version)){
      const profile=await store().ensureProfile(memory.profile);vaultCheck(profile.profileId===memory.profile.profileId,'vault_local_conflict');
      vaultCheck(await store().put(memory.record,memory.record.version-1),'vault_local_conflict');record=memory.record;
    }
    vaultCheck(record&&record.purpose==='create'&&record.recordId===operationId,'vault_create_journal_missing');
    let key,payload;try{key=await keyFor(operationId,password,record);payload=validate(await openCanvasVaultRecord(key,record,{crypto}));}
    catch(error){keys.delete(operationId);throw new CanvasVaultError(vaultErrorCode(error),{localDurable:true});}current();keys.set(operationId,key);
    vaultCheck(payload.operationId===operationId,'vault_create_journal_invalid');
    if(record.slot!==operationId)vaultCheck(payload.sourceRef&&record.slot===await canvasVaultScopeSlot({schema:'soty.vault.local-scope.v1',kind:'source',id:payload.sourceRef},projectId,crypto),'vault_local_scope_changed');
    return{record,key,payload};
  }
  async function advance(state,patch){
    const payload=validate({...state.payload,...patch}),{cipher,...header}=state.record,record=await sealCanvasVaultRecord(state.key,{...header,version:state.record.version+1},payload,{crypto});
    current();volatile.set(record.recordId,{profile:state.key.profile,record});
    if(!await store().put(record,state.record.version)){
      const latest=await store().get(record.recordId);vaultCheck(latest&&latest.version>state.record.version,'vault_local_conflict');
      return{record:latest,key:state.key,payload:validate(await openCanvasVaultRecord(state.key,latest,{crypto}))};
    }
    return{record,key:state.key,payload};
  }
  async function handoff(state,sourceRef,password,patch={}){
    const proof=await request('canvas.vault.bindingScope',{binding:{providerId:'soty.vault',ref:sourceRef}});current();
    const source=canvasVaultScope(proof?.localKeyScope);vaultCheck(proof?.state==='available'&&source.kind==='source'&&source.id===sourceRef,'vault_create_source_receipt_invalid');
    const slot=await canvasVaultScopeSlot(source,projectId,crypto),target=await store().ensureScopedProfile(source,canvasVaultProfile(accountId,projectId,slot,crypto));current();vaultCheck(target.control.phase==='ready','vault_local_rotation_pending');
    const key=await deriveCanvasVaultKey(target.profile,password,{crypto});for(const record of await store().listExact(slot))await openCanvasVaultRecord(key,record,{crypto});current();
    if(state.record.slot===slot){keys.set(state.payload.operationId,key);return Object.keys(patch).length?advance({...state,key},patch):{...state,key};}
    const payload=validate({...state.payload,...patch}),{cipher,...header}=state.record,next=await sealCanvasVaultRecord(key,{...header,slot,version:state.record.version+1},payload,{crypto});
    for(let attempt=0;attempt<4;attempt++){
      const snapshot=await store().inspectScopes(),old=await store().listExact(state.record.slot),destination=await store().listExact(slot);current();
      vaultCheck(old.length===1&&old[0].recordId===state.record.recordId&&old[0].version===state.record.version,'vault_local_conflict');
      const moved=await store().migrateScopes(slot,[{slot:state.record.slot,profileId:state.key.profile.profileId,expected:old}],{profileId:target.profile.profileId,expected:destination},[next],{inventoryGeneration:snapshot.inventory?.generation});current();
      if(moved){keys.set(payload.operationId,key);volatile.delete(payload.operationId);return{record:next,key,payload};}
    }
    throw new CanvasVaultError('vault_local_conflict');
  }
  function resume(operationId,password){
    if(flights.has(operationId))return flights.get(operationId);
    const flight=(async()=>{
      let state;try{
        state=await read(operationId,password);
        while(state.payload.stage!=='complete'){
          current();const value=state.payload;emit(operationId,value.stage,true);
          if(value.stage==='source'){
            // Owner-only lookup is read-only. Never submit create merely to
            // discover whether the old command already committed.
            const outcome=await request('canvas.vault.createReceipt',{sourceRequestId:value.sourceRequestId});
            let receipt;if(outcome?.state==='committed'){
              vaultCheck(outcome.sourceRequestId===value.sourceRequestId&&outcome.binding?.providerId==='soty.vault'&&outcome.localKeyScope?.id===outcome.binding.ref,'vault_create_source_receipt_invalid');
              receipt={binding:outcome.binding,current:{ownerId:accountId,kind:'native',vaultId:outcome.binding.ref}};
            }else{vaultCheck(outcome?.state==='missing','vault_create_source_receipt_invalid');receipt=await request('canvas.vault.create',{requestId:value.sourceRequestId,password,content:value.content});}
            vaultCheck(receipt?.binding?.providerId==='soty.vault'&&vaultId(receipt.binding.ref)&&receipt.current?.ownerId===accountId&&receipt.current.kind==='native','vault_create_source_receipt_invalid');
            state=await handoff(state,receipt.binding.ref,password,{stage:'object',sourceRef:receipt.binding.ref,content:null});
          }else if(value.stage==='object'){
            if(state.record.slot===value.operationId){state=await handoff(state,value.sourceRef,password);continue;}
            const receipt=await request('canvas.objects.create',{requestId:value.objectRequestId,binding:{providerId:'soty.vault',ref:value.sourceRef},presentation:value.presentation,
              visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'password'}});
            vaultCheck(vaultId(receipt?.current?.objectId)&&receipt.current.ownerId===accountId&&receipt.current.binding?.ref===value.sourceRef&&receipt.current.protection?.kind==='password','vault_create_object_receipt_invalid');
            state=await advance(state,{stage:'placement',objectId:receipt.current.objectId});
          }else{
            await onObjectReady?.(value.objectId);current();const placement=await place(value.objectId,dimensions(value.kind));current();
            vaultCheck(vaultId(placement?.shortcutId)&&['saved','pending'].includes(placement.status)&&placement.localDurable===true,'vault_create_placement_not_durable');
            state=await advance(state,{stage:'complete',placement:{shortcutId:placement.shortcutId,status:placement.status,localDurable:true}});
          }
        }
        const result={operationId,objectId:state.payload.objectId,...state.payload.placement};emit(operationId,'complete',true);
        // The authoritative field and encrypted Source are confirmed. Cleanup failure leaves a harmless replayable encrypted receipt.
        await store().remove(operationId,state.record.version).catch(()=>{});volatile.delete(operationId);keys.delete(operationId);return result;
      }catch(error){throw new CanvasVaultError(vaultErrorCode(error),{operationId,localDurable:error?.localDurable??Boolean(state)});}
    })().finally(()=>flights.delete(operationId));flights.set(operationId,flight);return flight;
  }
  async function start(kind,content,input,password){
    current();vaultCheck(crypto?.subtle,'vault_secure_context_required');const operationId=crypto.randomUUID(),profile=canvasVaultProfile(accountId,projectId,operationId,crypto),key=await deriveCanvasVaultKey(profile,password,{crypto});current();keys.set(operationId,key);
    const mask=validateCanvasMask(input?.mask??{shape:'rect'}),fontSize=input?.fontSize??16;vaultCheck(Number.isFinite(fontSize)&&fontSize>=8&&fontSize<=128,'vault_note_invalid');
    const payload={schema:'soty.vault.create.v1',operationId,sourceRequestId:crypto.randomUUID(),objectRequestId:crypto.randomUUID(),kind,createdAt:Date.now(),stage:'source',content,sourceRef:null,objectId:null,placement:null,presentation:{mask,fontSize}};
    const record=await sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:operationId,recordId:operationId,purpose:'create',version:1},payload,{crypto});
    volatile.set(operationId,{profile,record});
    try{vaultCheck(typeof store().ensureOperationProfile==='function','vault_local_unavailable');const chosen=await store().ensureOperationProfile(profile);current();vaultCheck(chosen.profileId===profile.profileId,'vault_local_conflict');vaultCheck(await store().put(record,0),'vault_local_conflict');current();}
    catch(error){emit(operationId,'local-error',false);throw new CanvasVaultError(vaultErrorCode(error),{operationId,localDurable:false});}
    return resume(operationId,password);
  }
  return{
    async startNote(input,password){vaultCheck(crypto?.subtle&&typeof crypto.randomUUID==='function','vault_secure_context_required');const now=Date.now();return start('note',canvasVaultNoteContent({noteId:crypto.randomUUID(),title:input?.title??'',body:input?.body,items:[],color:'plain',pinned:false,state:'active',revision:1,createdAt:now,updatedAt:now}),input,password);},
    async startFile(file,password,input={}){
      current();vaultCheck(file instanceof Blob&&file.size>0&&file.size<=CANVAS_VAULT_LOCAL_LIMITS.payloadBytes,'vault_wire_size');
      const normalized=await normalizeCanvasWebmFile(file,input.kind);current();const kind=canvasCreateFileKind(normalized.blob,normalized.kind);
      const bytes=new Uint8Array(await normalized.blob.arrayBuffer());current();try{return await start(kind,{contentType:normalized.blob.type,bytesBase64:vaultBase64(bytes)},input,password);}finally{bytes.fill(0);}
    },
    resume,
    async listPending(){current();return(await store().list()).filter(record=>record.purpose==='create').map(record=>({operationId:record.recordId,label:'Защищённый черновик',localDurable:true}));},
    async removeLocal(operationId){current();await store().remove(operationId);volatile.delete(operationId);keys.delete(operationId);},
    dispose(){if(disposed)return;disposed=true;keys.clear();volatile.clear();if(!providedStore)local?.close?.();},
  };
}
