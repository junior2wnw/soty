const nativeProviders=new Set(['soty.notes','soty.materials']);
const certainRejections=new Set(['vault_conversion_required','vault_format2_migration_required','vault_operation_unsupported']);
const error=code=>Object.assign(new Error(code),{code});

/** Owner verification, Source cache transition, and the existing Vault client
 * stay behind one port. A transport failure is never treated as a rollback. */
export function createCanvasObjectSecurityActions({api,accountId,objectId,isCurrent,vault,nativeTransition}={}){
 const active=()=>{if(!isCurrent())throw error('vault_account_changed');};
 async function owned(targetId=objectId){
  active();const result=await api.request('canvas.objects.get',{expectedAccountId:accountId,objectId:targetId});active();
  const object=result?.object;
  if(object?.objectId!==targetId||object.ownerId!==accountId||!object.binding||!Number.isSafeInteger(object.revision))throw error('vault_owner_required');
  return object;
 }
 async function describe(object){
  const remote=await vault.status(objectId,{includeCapabilities:true});active();
  const value=remote.protection?.kind==='password'?{...remote,locked:remote.locked!==false||!vault.isUnlocked(objectId)}:remote;
  if(nativeProviders.has(object.binding.providerId)){
   if(!nativeTransition)return{...value,device:'pending',protectionComplete:false,reason:'native_coordinator_unavailable',
    capabilities:{nativeConversion:false,placementProtection:false}};
   nativeTransition.remember(objectId,{providerId:object.binding.providerId,ref:object.binding.ref,ownerId:accountId});
   const local=await nativeTransition.liveReconcile({objectId});active();
   return{...value,device:local.device,protectionComplete:local.protectionComplete,...(local.reason?{reason:local.reason}:{}),...(local.operationId?{operationId:local.operationId}:{})};
  }
  return{...value,capabilities:{nativeConversion:false,placementProtection:value.capabilities?.placementProtection===true}};
 }
 async function status(){return describe(await owned());}
 async function finishWithPassword(targetId,password){
  let opened=false;
  try{await vault.unlock(targetId,password);opened=true;active();const state=await nativeTransition.resumeAfterUnlock(targetId);active();return state;}
  finally{
   if(opened&&isCurrent()){
    // Each new temporary grant needs a genuinely new relock operation: an old
    // idempotent receipt cannot close a grant opened AFTER that old receipt.
    await vault.relock(targetId,{scope:'device',requestId:crypto.randomUUID()});active();
   }else if(opened)vault.invalidate?.(targetId,'account');
  }
 }
 async function finishLocal(state,targetId,password){
  if(state.server==='protected'&&state.device==='pending'&&['native_password_required_for_creation_recovery','native_password_required_for_local_draft'].includes(state.reason))return finishWithPassword(targetId,password);
  return state;
 }
 async function protect(password,operationId){
  const object=await owned(),value=await describe(object);active();
  if(nativeProviders.has(object.binding.providerId)){
   if(!nativeTransition)throw error('native_coordinator_unavailable');
   if(value.capabilities?.nativeConversion!==true)throw error('vault_conversion_required');
   if(value.protection?.kind==='password'){
    return finishWithPassword(objectId,password);
   }
   const prepared=await nativeTransition.prepare({objectId,expectedObjectRevision:object.revision,operationId});active();
   if(prepared.state!=='ready')return prepared;
   const ticket=prepared.ticket;
   if(ticket.objectId!==objectId){
    const original=await owned(ticket.objectId);
    if(original.binding.providerId!==object.binding.providerId||original.binding.ref!==object.binding.ref||original.revision!==ticket.expectedObjectRevision)throw error('vault_object_changed');
   }
   let result;
   try{result=await vault.protect(ticket.objectId,ticket.expectedObjectRevision,password,{requestId:ticket.operationId});active();}
   catch(reason){
    active();const state=await nativeTransition.finalize({ticket:prepared.ticket,failure:{kind:certainRejections.has(reason?.code)?'server-rejected':'transport-uncertain',...(typeof reason?.code==='string'?{code:reason.code}:{})}});active();
    if(state.server==='unprotected'&&state.device==='complete')throw reason;
    return finishLocal(state,ticket.objectId,password);
   }
   const state=await nativeTransition.finalize({ticket:prepared.ticket,result});active();return finishLocal(state,ticket.objectId,password);
  }
  if(value.capabilities?.placementProtection!==true)throw error('vault_conversion_required');
  await vault.protect(objectId,object.revision,password,{requestId:operationId});active();
  const confirmed=await vault.status(objectId);active();
  if(confirmed.protection?.kind!=='password')throw error('vault_protection_unconfirmed');
  return{server:'protected',device:'complete',protectionComplete:true};
 }
 async function relock(scope,requestId){await owned();await vault.relock(objectId,{scope,requestId});active();}
 async function rotate(oldPassword,newPassword){
  const object=await owned();
  if(nativeProviders.has(object.binding.providerId)){
   if(!nativeTransition)throw error('native_coordinator_unavailable');
   const local=await nativeTransition.liveReconcile({objectId});active();if(!local.protectionComplete)throw error('vault_local_draft_pending');
  }
  // The encrypted Vault journal owns its retry identity, including after a
  // lost ACK and a new panel. A fresh UI UUID cannot replace that retained ID.
  await vault.rotate(objectId,{oldPassword,newPassword});active();
 }
 async function localRecoveryState(){await owned();const value=await vault.localRecoveryState(objectId);active();return value;}
 async function recoverLocalPassword(passwords){await owned();const value=await vault.recoverLocalPassword(objectId,passwords);active();return value;}
 return{status,protect,relock,rotate,...(typeof vault.localRecoveryState==='function'&&typeof vault.recoverLocalPassword==='function'?{localRecoveryState,recoverLocalPassword}:{})};
}
