import {DatabaseSync} from 'node:sqlite';
import {createHash,randomUUID} from 'node:crypto';
import {mkdirSync} from 'node:fs';
import {dirname} from 'node:path';
import {OBJECT_SCHEMA,OBJECT_LIMITS,check,exact,identity,binding,ownerRepresentation,presentation,visibility,hidden,protection,jsonProjection,boundedContent} from './contract.mjs';
import {initializeCanvasStorage} from './storage.mjs';

export const canvasObjectOperations=new Set(['canvas.objects.create','canvas.objects.get','canvas.objects.list','canvas.objects.update','canvas.objects.remove','canvas.objects.restore','canvas.objects.project']);
const canonical=value=>value&&typeof value==='object'?Array.isArray(value)?value.map(canonical):Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const hash=value=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const sync=value=>{if(value&&typeof value.then==='function'){Promise.resolve(value).catch(()=>{});check(false,'canvas_async_authority',500);}return value;};

/** Host registers trusted provider code. App descriptors and object refs confer no Root authority. */
export function createCanvasObjectsService({databasePath,projectId,actorActive,withActorAuthorityFence,providers=[],withProtectionAuthority,allowFormat2Migration=false,canAccessCommunity=()=>false,canShareCommunity=()=>false,onChange=()=>{},clock=Date.now}={}){
 check(typeof databasePath==='string'&&typeof actorActive==='function'&&typeof withActorAuthorityFence==='function'&&withActorAuthorityFence.constructor?.name!=='AsyncFunction'&&typeof clock==='function'&&typeof onChange==='function','canvas_configuration_invalid',500);identity(projectId);
 mkdirSync(dirname(databasePath),{recursive:true});const db=new DatabaseSync(databasePath),registry=new Map(),contexts=new WeakMap();let closed=false,format,pageSelectionAuthority;
 const nativePageProviders=new Set();
 try{format=initializeCanvasStorage(db,projectId,{allowFormat2Migration});}catch(error){db.close();throw error;}
 const get=(sql,...args)=>db.prepare(sql).get(...args),all=(sql,...args)=>db.prepare(sql).all(...args),run=(sql,...args)=>db.prepare(sql).run(...args);
 function registerProvider(provider){check(!closed,'canvas_closed',503);identity(provider?.providerId);check(typeof provider.withBindingAuthority==='function'&&typeof provider.project==='function'&&provider.withBindingAuthority.constructor?.name!=='AsyncFunction'&&provider.project.constructor?.name!=='AsyncFunction','canvas_provider_invalid',500);check(provider.ownerRepresents===undefined||Array.isArray(provider.ownerRepresents)&&provider.ownerRepresents.length===0||typeof provider.ownerRepresents==='function'&&provider.ownerRepresents.constructor?.name!=='AsyncFunction','canvas_provider_invalid',500);check(!registry.has(provider.providerId),'canvas_provider_conflict',409);registry.set(provider.providerId,provider);}
 try{for(const provider of providers)registerProvider(provider);}catch(error){db.close();throw error;}
 function actorCheck(actor){check(!closed,'canvas_closed',503);check(actor&&actorActive(actor)===true,'canvas_authentication_required',401);identity(actor.accountId);identity(actor.deviceId);}
 function captureActor(actor){actorCheck(actor);return Object.freeze({accountId:actor.accountId,deviceId:actor.deviceId,...(typeof actor.label==='string'?{label:actor.label}:{})});}
 function authority(actor,callback){actorCheck(actor);let entered=false,active=true,result;try{sync(withActorAuthorityFence(actor,()=>{check(active&&!entered,'canvas_authority_invalid',500);entered=true;actorCheck(actor);result=sync(callback(captureActor(actor)));actorCheck(actor);return result;}));check(entered,'canvas_authority_invalid',500);return result;}finally{active=false;}}
 function transaction(callback){check(!db.isTransaction,'canvas_nested_transaction',500);db.exec('BEGIN IMMEDIATE');try{const result=sync(callback());db.exec('COMMIT');return result;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 function row(id){return get('SELECT * FROM canvas_objects WHERE object_id=?',identity(id));}
 function record(value){
  if(!value)return null;identity(value.object_id);identity(value.owner_id);check(Number.isSafeInteger(value.revision)&&value.revision>=1&&['active','removed'].includes(value.state),'canvas_storage_invalid',503);
  const accessRevision=format===2?value.access_revision:value.revision;check(Number.isSafeInteger(accessRevision)&&accessRevision>=1&&accessRevision<=value.revision,'canvas_storage_invalid',503);
  return {schema:OBJECT_SCHEMA,objectId:value.object_id,ownerId:value.owner_id,revision:value.revision,accessRevision,binding:binding(JSON.parse(value.binding_json)),presentation:presentation(JSON.parse(value.presentation_json)),visibility:visibility(JSON.parse(value.visibility_json)),hidden:hidden(JSON.parse(value.hidden_json)),state:value.state,protection:protection({kind:value.protection_kind})};
 }
 for(const value of all('SELECT * FROM canvas_objects'))record(value);
 function own(actor,id){const value=row(id);check(value?.owner_id===actor.accountId,'canvas_owner_required',403);return record(value);}
 function eligible(object,actor){
  if(!object||object.state!=='active')return false;
  if(actor?.accountId===object.ownerId)return !object.hidden.owner;
  if(object.hidden.others)return false;
  if(object.visibility.audience==='public')return true;
  return Boolean(actor&&object.visibility.audience==='restricted'&&(object.visibility.accountIds.includes(actor.accountId)||object.visibility.communityIds.some(id=>sync(canAccessCommunity(actor,id))===true)));
 }
 function pageAuthority(request,callback){
  check(format===2&&typeof pageSelectionAuthority==='function','canvas_page_selection_migration_required',503);
  let entered=false,active=true,result;try{sync(pageSelectionAuthority(request.pageContext,scope=>{
   check(active&&!entered&&scope?.schema==='soty.canvas-page-selection-scope.v1'&&scope.objectId===request.objectId&&scope.originalActor===(request.actor??null),'canvas_page_authority_required',403);entered=true;
   identity(scope.pageOwnerId);check(Number.isSafeInteger(scope.accessRevision)&&scope.accessRevision>=1,'canvas_page_authority_required',403);binding(scope.binding);result=sync(callback(scope));return result;
  }));check(entered,'canvas_page_authority_required',403);return result;}finally{active=false;}
 }
 function frameEligible(object,actor,pageRequest){
  if(!pageRequest)return eligible(object,actor);
  return pageAuthority(pageRequest,scope=>Boolean(object&&object.state==='active'&&!object.hidden.others
   &&object.accessRevision===scope.accessRevision&&hash(object.binding)===hash(scope.binding)
   &&(eligible(object,actor)||object.ownerId===scope.pageOwnerId&&nativePageProviders.has(object.binding.providerId))));
 }
 function share(actor,policy){check(policy.communityIds.every(id=>sync(canShareCommunity(actor,id))===true),'canvas_community_permission_denied',403);}
 function providerAuthority(provider,request,callback){
  let entered=false,active=true,result;try{sync(provider.withBindingAuthority(request,scope=>{check(active&&!entered,'canvas_provider_authority_invalid',500);entered=true;result=sync(callback(scope));return result;}));check(entered,'canvas_provider_authority_invalid',500);return result;}finally{active=false;}
 }
 function sealed(kind,scope){check(scope&&typeof scope==='object','canvas_provider_authority_invalid',500);const actual=scope.protection?.kind??'none';
  if(kind==='password')check(actual==='password'&&scope.protection.sealed===true,'canvas_plaintext_password_forbidden',409);
  else check(actual==='none','canvas_protection_mismatch',409);
 }
 function protectionAuthority(object,actor,callback){
  if(!withProtectionAuthority)return callback({kind:object.protection.kind,unlocked:object.protection.kind==='none',generation:0});
  let entered=false,active=true,result;try{sync(withProtectionAuthority({actor,object},state=>{check(active&&!entered&&state&&['none','password'].includes(state.kind)&&typeof state.unlocked==='boolean'&&(state.state===undefined||state.state==='sealing'),'canvas_protection_authority_invalid',500);entered=true;
   const gate=object.protection.kind==='password'&&state.kind!=='password'?{kind:'password',unlocked:false,generation:0}:state;result=sync(callback(gate));return result;}));check(entered,'canvas_protection_authority_invalid',500);return result;}finally{active=false;}
 }
 function management(object,actor){return protectionAuthority(object,actor,gate=>({...object,protection:{kind:gate.state==='sealing'?'none':gate.kind}}));}
 function command(actor,op,args,callback){
  const requestId=identity(args.requestId),intent=hash([op,args]);
  const replay=()=>{const prior=get('SELECT * FROM canvas_commands WHERE account_id=? AND request_id=?',actor.accountId,requestId);if(!prior)return null;check(prior.intent_hash===intent,'canvas_request_conflict',409);const receipt=JSON.parse(prior.receipt_json);return {replayed:true,receipt,current:own(actor,receipt.objectId)};};
  const previous=replay();if(previous)return {...previous,current:management(previous.current,actor)};
  let change,allocatedObjectId;
  const apply=()=>transaction(()=>{const previous=replay();if(previous)return previous;actorCheck(actor);check(get('SELECT count(*) n FROM canvas_commands WHERE account_id=?',actor.accountId).n<50000,'canvas_storage_limit',409);
   const now=clock();check(Number.isSafeInteger(now)&&now>=0,'canvas_clock_invalid',500);const result=callback(now,allocatedObjectId);actorCheck(actor);const current=own(actor,result.objectId),receipt={objectId:current.objectId,revision:current.revision,committedAt:now};
   run('INSERT INTO canvas_commands VALUES(?,?,?,?)',actor.accountId,requestId,intent,JSON.stringify(receipt));change={objectId:current.objectId,ownerId:current.ownerId,revision:current.revision,accountIds:[...new Set([current.ownerId,...(result.previous?.visibility.accountIds??[]),...current.visibility.accountIds])],publicTransition:[result.previous,current].some(value=>value?.visibility.audience==='public'&&!value.hidden.others&&value.state==='active')};
   return {replayed:false,receipt,current};});
  let result;
  if(op==='canvas.objects.create'){
   const provider=registry.get(args.binding.providerId);check(provider,'canvas_provider_unavailable',503);const objectId='object_'+randomUUID().replaceAll('-','');
   allocatedObjectId=objectId;
   result=providerAuthority(provider,{actor,objectId,objectOwnerId:actor.accountId,ref:args.binding.ref,mode:'create'},scope=>{check(scope?.allowed!==false,'canvas_provider_access_denied',403);sealed(args.protection.kind,scope);return apply();});
  }else result=apply();
  if(change)try{onChange(change);}catch{/* The durable command remains accepted. */}return {...result,current:management(result.current,actor)};
 }
 function execute({actor:transportActor,op,args={}}){return authority(transportActor,actor=>{
  check(canvasObjectOperations.has(op),'canvas_operation_unsupported');check(args.expectedAccountId===actor.accountId,'canvas_account_changed',401);const {expectedAccountId,...input}=args;args=input;
  if(op==='canvas.objects.project'){exact(args,['objectIds']);return project({actor,objectIds:args.objectIds,alreadyFenced:true});}
  if(op==='canvas.objects.get'){exact(args,['objectId']);return {object:management(own(actor,args.objectId),actor)};}
  if(op==='canvas.objects.list'){exact(args,['state','offset'],[]);const state=args.state??'active',offset=args.offset??0;check(['active','removed'].includes(state)&&Number.isSafeInteger(offset)&&offset>=0&&offset<=10000);const rows=all('SELECT * FROM canvas_objects WHERE owner_id=? AND state=? ORDER BY created_at,object_id LIMIT 101 OFFSET ?',actor.accountId,state,offset);return {objects:rows.slice(0,100).map(row=>management(record(row),actor)),nextOffset:rows.length>100?offset+100:null};}
  if(op==='canvas.objects.create'){
   exact(args,['requestId','binding','presentation','visibility','hidden','protection']);args={...args,binding:binding(args.binding),presentation:presentation(args.presentation),visibility:visibility(args.visibility),hidden:hidden(args.hidden),protection:protection(args.protection)};
   share(actor,args.visibility);
   return command(actor,op,args,(now,objectId)=>{check(get('SELECT count(*) n FROM canvas_objects WHERE owner_id=?',actor.accountId).n<OBJECT_LIMITS.objects,'canvas_storage_limit',409);share(actor,args.visibility);
    run(`INSERT INTO canvas_objects VALUES(?,?,1,?,?,?,?,?,?,?,?${format===2?',1':''})`,objectId,actor.accountId,JSON.stringify(args.binding),JSON.stringify(args.presentation),JSON.stringify(args.visibility),JSON.stringify(args.hidden),args.protection.kind,'active',now,now);return {objectId};});
  }
  exact(args,['objectId','requestId','expectedRevision','presentation','visibility','hidden','revokePublication'],['objectId','requestId','expectedRevision']);
  check(Number.isSafeInteger(args.expectedRevision)&&args.expectedRevision>=1);if(op!=='canvas.objects.update')check(!['presentation','visibility','hidden','revokePublication'].some(key=>Object.hasOwn(args,key)));
  else {check(['presentation','visibility','hidden'].some(key=>Object.hasOwn(args,key))||args.revokePublication===true);check(args.revokePublication===undefined||typeof args.revokePublication==='boolean');}
  const normalized={...args,...(!Object.hasOwn(args,'presentation')?{}:{presentation:presentation(args.presentation)}),...(!Object.hasOwn(args,'visibility')?{}:{visibility:visibility(args.visibility)}),...(!Object.hasOwn(args,'hidden')?{}:{hidden:hidden(args.hidden)})};
  return command(actor,op,normalized,now=>{const previous=own(actor,args.objectId);check(previous.revision===args.expectedRevision,'canvas_revision_conflict',409);check(previous.revision<Number.MAX_SAFE_INTEGER,'canvas_revision_exhausted',409);
   const next={...previous,...normalized,state:op==='canvas.objects.remove'?'removed':op==='canvas.objects.restore'?'active':previous.state};share(actor,next.visibility);
   const policyChanged=normalized.revokePublication===true||hash([next.visibility,next.hidden,next.state])!==hash([previous.visibility,previous.hidden,previous.state]);
   check(!policyChanged||previous.accessRevision<Number.MAX_SAFE_INTEGER,'canvas_access_revision_exhausted',409);
   run(`UPDATE canvas_objects SET revision=revision+1,presentation_json=?,visibility_json=?,hidden_json=?,state=?,updated_at=?${format===2?',access_revision=?':''} WHERE object_id=?`,JSON.stringify(next.presentation),JSON.stringify(next.visibility),JSON.stringify(next.hidden),next.state,now,...(format===2?[previous.accessRevision+Number(policyChanged)]:[]),previous.objectId);return {objectId:previous.objectId,previous};});
 });}
 function readFrame(actor,objectId,callback,pageRequest){
  const before=record(row(objectId));if(!frameEligible(before,actor,pageRequest)||pageRequest&&!registry.has(before.binding.providerId))return null;
  return protectionAuthority(before,actor,gate=>{
  const canUnlock=Boolean(actor)&&eligible(before,actor);
  const lockedFrame=frame=>{
   if(!pageRequest)return frame;
   const provider=registry.get(before.binding.providerId);if(!provider)return null;
   // A Page may reveal the selected lock/geometry only while this exact Source
   // still admits the actual visitor. This bounded header follows the lock
   // barrier; it never projects, decrypts or reads the Source payload.
   try{return providerAuthority(provider,{actor,objectId,objectOwnerId:before.ownerId,ref:before.binding.ref,mode:'read',purpose:'protection'},scope=>{
    check(scope&&typeof scope==='object','canvas_provider_authority_invalid',500);if(scope.allowed===false)return null;if(before.protection.kind==='password')sealed('password',scope);const current=record(row(objectId));check(current?.revision===before.revision,'canvas_object_changed',409);return frameEligible(current,actor,pageRequest)?frame:null;
   });}catch(error){if(error?.code==='canvas_provider_access_denied')return null;throw error;}
  };
  if(gate.state==='sealing')return lockedFrame({object:{...before,protection:{kind:'none'}},locked:true,lockState:'sealing',canUnlock:false});
  if(gate.kind==='password'&&!gate.unlocked)return lockedFrame({object:{...before,protection:{kind:'password'}},locked:true,canUnlock});
  const provider=registry.get(before.binding.providerId);if(!provider)return null;
  try{return providerAuthority(provider,{actor,objectId,objectOwnerId:before.ownerId,ref:before.binding.ref,mode:'read'},scope=>transaction(()=>{
   const current=record(row(objectId));if(!frameEligible(current,actor,pageRequest)||scope?.allowed===false)return null;check(current.revision===before.revision,'canvas_object_changed',409);sealed(current.protection.kind==='none'&&gate.kind==='password'&&scope.protection?.kind==='password'?'password':current.protection.kind,scope);
   if(current.protection.kind==='password'&&gate.kind!=='password')return {object:current,locked:true};
   const context=Object.freeze({schema:'soty.canvas-object-authority.v1',right:'read',objectId,objectRevision:current.revision,objectAccessRevision:current.accessRevision,ownerId:current.ownerId,providerId:current.binding.providerId,ref:current.binding.ref,audience:current.visibility.audience,viewer:Object.freeze(actor?{kind:'account',accountId:actor.accountId,deviceId:actor.deviceId}:{kind:'public'})});
   const proof={active:true,actor,pageRequest};contexts.set(context,proof);
   try{const value=sync(callback(context,provider));if(actor)actorCheck(actor);check(frameEligible(record(row(objectId)),actor,pageRequest),'canvas_access_changed',403);return {object:{...current,protection:{kind:gate.kind}},locked:false,value};}finally{proof.active=false;}
  }));}catch(error){if(error?.code!=='canvas_provider_access_denied')throw error;return null;}
  });
 }
 function projectionItem(objectId,frame){return frame?{objectId,available:true,revision:frame.object.revision,accessRevision:frame.object.accessRevision,presentation:frame.object.presentation,protection:frame.object.protection,
  view:frame.locked?{schema:'soty.canvas-object-lock.v1',state:frame.lockState??'locked'}:frame.value,...(frame.locked?{canUnlock:frame.canUnlock===true}:{})}:{objectId,available:false};}
 function ownerRepresents(actor,before){
  if(!actor||before?.ownerId!==actor.accountId)return undefined;
  let result;
  if(before.state==='removed')result=[];
  else{
   const provider=registry.get(before.binding.providerId),declaration=provider?.ownerRepresents;
   if(Array.isArray(declaration)){check(declaration.length===0,'canvas_provider_metadata_invalid',502);result=[];}
   else if(typeof declaration==='function'){
    try{const value=sync(provider.ownerRepresents(Object.freeze({ownerId:before.ownerId,objectId:before.objectId,ref:before.binding.ref})));if(value!==undefined)result=ownerRepresentation(value);}
    catch(error){if(![403,404].includes(error?.status))throw error;}
   }
  }
  actorCheck(actor);const after=record(row(before.objectId));
  check(after?.ownerId===before.ownerId&&after.revision===before.revision&&after.state===before.state&&hash(after.binding)===hash(before.binding),'canvas_object_changed',409);
  return result;
 }
 function project({actor=null,objectIds,alreadyFenced=false}={}){
  check(!closed,'canvas_closed',503);check(Array.isArray(objectIds)&&objectIds.length<=256&&new Set(objectIds).size===objectIds.length);objectIds.forEach(identity);
  const action=currentActor=>{actor=currentActor;if(actor)actorCheck(actor);const items=[];let bytes=0;
   for(const objectId of objectIds){const frame=readFrame(actor,objectId,(context,provider)=>jsonProjection(sync(provider.project(context))));
    const item=projectionItem(objectId,frame);
    const before=frame?.object??record(row(objectId));
    if(!frame&&before?.ownerId===actor?.accountId&&before?.state==='active'&&before.hidden.owner)item.hidden=true;
    const represents=ownerRepresents(actor,before);if(represents!==undefined)item.ownerRepresents=represents;
    bytes+=Buffer.byteLength(JSON.stringify(item),'utf8');check(bytes<=OBJECT_LIMITS.projectionBatchBytes,'canvas_projection_too_large',502);items.push(item);
   }return {schema:'soty.canvas-object-projection.v1',items};};
  return actor&&!alreadyFenced?authority(actor,action):action(actor);
 }
 return Object.freeze({operations:canvasObjectOperations,execute,project(request){exact(request,['actor','objectIds'],['objectIds']);return project({...request,alreadyFenced:false});},registerProvider,unregisterProvider(providerId){registry.delete(identity(providerId));},
  storageIdentity(){check(!closed,'canvas_closed',503);return {schema:'soty.canvas-objects-storage.v1',formatVersion:format};},
  registerPageSelectionAuthority({verifyContext,nativeProviders}){check(!closed&&!pageSelectionAuthority&&typeof verifyContext==='function'&&Array.isArray(nativeProviders)&&nativeProviders.length<=32&&new Set(nativeProviders).size===nativeProviders.length,'canvas_page_configuration_invalid',500);for(const id of nativeProviders){identity(id);check(registry.has(id),'canvas_provider_unavailable',503);}pageSelectionAuthority=verifyContext;for(const id of nativeProviders)nativePageProviders.add(id);},
  pageSelectionConfigured(){return !closed&&format===2&&typeof pageSelectionAuthority==='function';},
  withPageSelectionTarget(request,callback){check(!closed,'canvas_closed',503);exact(request,['actor','objectId','expectedAccessRevision']);check(format===2,'canvas_page_selection_migration_required',503);const actor=captureActor(request.actor),before=record(row(request.objectId));check(before&&before.state==='active'&&!before.hidden.others&&(before.ownerId===actor.accountId||eligible(before,actor)),'canvas_page_selection_unavailable',409);check(before.accessRevision===request.expectedAccessRevision,'canvas_page_selection_stale',409);const provider=registry.get(before.binding.providerId);check(provider,'canvas_provider_unavailable',503);
   const apply=()=>{const value=sync(callback(Object.freeze({...before,binding:Object.freeze({...before.binding})})));actorCheck(actor);const after=record(row(before.objectId));check(after?.revision===before.revision&&after.state==='active'&&!after.hidden.others,'canvas_page_selection_stale',409);return value;};
   return protectionAuthority(before,actor,gate=>providerAuthority(provider,{actor,objectId:before.objectId,objectOwnerId:before.ownerId,ref:before.binding.ref,mode:'read',...(gate.kind==='password'||gate.state==='sealing'?{purpose:'protection'}:{})},scope=>{check(scope?.allowed!==false,'canvas_page_selection_unavailable',409);return apply();}));},
  projectPageObject(request){exact(request,['actor','objectId','pageContext'],['objectId','pageContext']);identity(request.objectId);const read=actor=>projectionItem(request.objectId,readFrame(actor,request.objectId,(context,provider)=>jsonProjection(sync(provider.project(context))),request));return request.actor?authority(request.actor,read):read(null);},
  withPageObjectRead(request,callback){exact(request,['actor','objectId','pageContext'],['objectId','pageContext']);identity(request.objectId);check(typeof callback==='function');const read=actor=>{const frame=readFrame(actor,request.objectId,context=>boundedContent(sync(callback(context))),request);check(frame,'canvas_object_unavailable',404);check(!frame.locked,'canvas_object_locked',403);return frame.value;};return request.actor?authority(request.actor,read):read(null);},
  notifySourceChanged(request){check(!closed,'canvas_closed',503);exact(request,['providerId','ref','ownerId']);identity(request.providerId);identity(request.ref);identity(request.ownerId);
   const matches=all("SELECT * FROM canvas_objects WHERE owner_id=? AND state='active' AND json_extract(binding_json,'$.providerId')=? AND json_extract(binding_json,'$.ref')=?",request.ownerId,request.providerId,request.ref);
   for(const value of matches){const current=record(value);try{onChange({objectId:current.objectId,ownerId:current.ownerId,revision:current.revision,accountIds:[...new Set([current.ownerId,...current.visibility.accountIds])],publicTransition:false});}catch{/* Source is already committed; a hint does not grant reads. */}}
   return {notified:matches.length};},
  withObjectAuthority(context,callback){const proof=contexts.get(context);check(proof?.active&&db.isTransaction,'canvas_object_authority_required',403);if(proof.actor)actorCheck(proof.actor);const current=record(row(context.objectId));check(current?.revision===context.objectRevision&&frameEligible(current,proof.actor,proof.pageRequest),'canvas_access_changed',403);return protectionAuthority(current,proof.actor,gate=>{check(gate.state!=='sealing'&&(gate.kind==='none'||gate.unlocked),'canvas_object_locked',403);return sync(callback(context));});},
  withProtectionTarget(request,callback){exact(request,['actor','objectId','mode']);check(['owner','read'].includes(request.mode));identity(request.objectId);check(typeof callback==='function');return authority(request.actor,actor=>{
   const before=record(row(request.objectId));check(before&&before.state==='active'&&(request.mode==='owner'?before.ownerId===actor.accountId:eligible(before,actor)),'canvas_object_unavailable',404);
   const provider=registry.get(before.binding.providerId);check(provider,'canvas_provider_unavailable',503);
   return providerAuthority(provider,{actor,objectId:before.objectId,objectOwnerId:before.ownerId,ref:before.binding.ref,mode:'read',purpose:'protection'},scope=>{
    check(scope?.allowed!==false,'canvas_provider_access_denied',403);const current=record(row(before.objectId));check(current?.revision===before.revision&&current.state==='active'&&(request.mode==='owner'?current.ownerId===actor.accountId:eligible(current,actor)),'canvas_access_changed',403);
    const value=sync(callback(Object.freeze({...current,binding:Object.freeze({...current.binding}),actor,...(Number.isSafeInteger(scope.sourceRevision)&&typeof scope.sourceStoreId==='string'?{source:Object.freeze({revision:scope.sourceRevision,storeId:scope.sourceStoreId})}:{})})));
    const after=record(row(before.objectId));check(after?.revision===before.revision&&after.state==='active'&&(request.mode==='owner'?after.ownerId===actor.accountId:eligible(after,actor)),'canvas_access_changed',403);return value;
   });});},
  withRuntimeProtectionTarget(request,callback){exact(request,['actor','objectId']);identity(request.objectId);check(typeof callback==='function');const actor=captureActor(request.actor),before=record(row(request.objectId));check(eligible(before,actor),'canvas_object_unavailable',404);
   const value=sync(callback(Object.freeze({...before,binding:Object.freeze({...before.binding}),actor})));actorCheck(actor);const after=record(row(before.objectId));check(after?.revision===before.revision&&eligible(after,actor),'canvas_access_changed',403);return value;},
  withObjectRead(request,callback){check(!closed,'canvas_closed',503);exact(request,['actor','objectId'],['objectId']);identity(request.objectId);check(typeof callback==='function');const read=actor=>{const frame=readFrame(actor,request.objectId,context=>boundedContent(sync(callback(context))));check(frame,'canvas_object_unavailable',404);check(!frame.locked,'canvas_object_locked',403);return frame.value;};return request.actor?authority(request.actor,read):read(null);},
  close(){if(!closed){check(!db.isTransaction,'canvas_nested_transaction',500);db.close();closed=true;registry.clear();}}
 });
}
