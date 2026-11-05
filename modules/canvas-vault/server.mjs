import {DatabaseSync} from 'node:sqlite';
import {randomUUID,createHash} from 'node:crypto';
import {mkdirSync} from 'node:fs';
import {dirname} from 'node:path';
import {check,exact,identity,CanvasObjectError} from '../canvas-objects/contract.mjs';
import {initializeVaultStorage} from './storage.mjs';
import {createVaultCrypto,content,password,newPayload,intent,context,VAULT_CONTENT_BYTES,VAULT_CHUNK_BYTES} from './crypto.mjs';
import {createVaultAdvanced,advancedOperations} from './advanced.mjs';

export const canvasVaultOperations=new Set(['canvas.vault.create','canvas.vault.createReceipt','canvas.vault.bindingScope','canvas.vault.rotateReceipt','canvas.vault.protect','canvas.vault.status','canvas.vault.unlock','canvas.vault.relock','canvas.vault.write','canvas.vault.rotate',...advancedOperations]);
const sync=value=>{if(value&&typeof value.then==='function'){Promise.resolve(value).catch(()=>{});check(false,'vault_async_authority',500);}return value;};
const publicHash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const tuple=actor=>Object.freeze({accountId:identity(actor?.accountId),deviceId:identity(actor?.deviceId)});
const zero=keys=>{keys?.dataKey?.fill(0);keys?.wrappingKey?.fill(0);};
const dto=row=>({schema:'soty.canvas-vault.v1',vaultId:row.vault_id,ownerId:row.owner_id,revision:row.revision,kind:row.kind});

/** A host-only encrypted source and placement barrier. Passwords never confer Object or Source ACL. */
export function createCanvasVaultService({databasePath,projectId,actorActive,withActorAuthorityFence,withProtectionTarget,withObjectAuthority,withRuntimeTarget,placementProviders=['soty.apps'],allowFormat2Migration=false,nativeProviders=[],onChange=()=>{},onInvalidate=()=>{},clock=Date.now}={}){
 check(typeof databasePath==='string'&&typeof actorActive==='function'&&typeof withActorAuthorityFence==='function'&&typeof withProtectionTarget==='function'&&typeof withObjectAuthority==='function'&&typeof onChange==='function'&&typeof onInvalidate==='function'&&typeof clock==='function','vault_configuration_invalid',500);identity(projectId);
 check(Array.isArray(placementProviders)&&placementProviders.length<=32&&placementProviders.every(value=>typeof value==='string'),'vault_configuration_invalid',500);
 check(Array.isArray(nativeProviders)&&nativeProviders.length<=32,'vault_configuration_invalid',500);
 mkdirSync(dirname(databasePath),{recursive:true});const db=new DatabaseSync(databasePath),crypto=createVaultCrypto(),permits=new Map(),epochs=new Map(),attempts=new Map(),inflight=new Map();let closed=false;
 let storage;try{storage=initializeVaultStorage(db,projectId,{allowFormat2Migration});}catch(error){db.close();crypto.close();throw error;}
 const get=(sql,...args)=>db.prepare(sql).get(...args),all=(sql,...args)=>db.prepare(sql).all(...args),run=(sql,...args)=>db.prepare(sql).run(...args);
 const ensureStorage=()=>{check(!closed,'vault_closed',503);check(Number(get('PRAGMA user_version').user_version)===storage.schemaVersion,'vault_store_format_changed',503);};
 const now=()=>{const value=clock();check(Number.isSafeInteger(value)&&value>=0,'vault_clock_invalid',500);return value;};
 function active(actor){ensureStorage();check(actor&&actorActive(actor)===true,'vault_authentication_required',401);return tuple(actor);}
 function authority(original,callback){active(original);let entered=false,live=true,result;try{sync(withActorAuthorityFence(original,()=>{check(live&&!entered,'vault_actor_authority_invalid',500);entered=true;result=sync(callback(active(original)));active(original);return result;}));check(entered,'vault_actor_authority_invalid',500);return result;}finally{live=false;}}
 function target(original,objectId,mode,callback){check(!closed,'vault_closed',503);active(original);let entered=false,live=true,result;try{sync(withProtectionTarget({actor:original,objectId:identity(objectId),mode},object=>{check(live&&!entered,'vault_object_authority_invalid',500);entered=true;const actor=active(original);check(object.actor.accountId===actor.accountId&&object.actor.deviceId===actor.deviceId,'vault_object_authority_invalid',500);result=sync(callback(object,actor));active(original);return result;}));check(entered,'vault_object_authority_invalid',500);return result;}finally{live=false;}}
 function transaction(callback){ensureStorage();check(!db.isTransaction,'vault_transaction_invalid',500);db.exec('BEGIN IMMEDIATE');try{ensureStorage();const result=sync(callback());db.exec('COMMIT');return result;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}
 const source=id=>{ensureStorage();return get('SELECT * FROM vault_sources WHERE vault_id=?',identity(id));};
 function objectSource(object){const row=object.binding.providerId==='soty.vault'?source(object.binding.ref):get('SELECT * FROM vault_sources WHERE object_id=?',object.objectId)??advanced.sourceFor(object);if(!row)return null;
  check(row.owner_id===object.ownerId&&(row.kind==='native'?(object.binding.providerId==='soty.vault'&&row.vault_id===object.binding.ref)||advanced.matchesBinding(object,row):row.object_id===object.objectId&&row.binding_json===JSON.stringify(object.binding)),'vault_binding_changed',403);return row;
 }
 const permitId=(objectId,actor)=>JSON.stringify([objectId,actor.accountId,actor.deviceId]);
 const epoch=(objectId,actor)=>`${epochs.get(objectId)??0}:${epochs.get(permitId(objectId,actor))??0}`;
 function invalidationInfo(objectId){const barrier=get("SELECT owner_id,binding_json FROM vault_sources WHERE kind='placement' AND object_id=?",objectId);if(!barrier)return {};const binding=JSON.parse(barrier.binding_json);return {ownerId:barrier.owner_id,...(binding.providerId==='soty.apps'?{widgetId:binding.ref}:{})};}
 function discard(id,notify=false){const permit=permits.get(id);if(!permit)return;clearTimeout(permit.timer);zero(permit);permits.delete(id);if(notify)try{onInvalidate({objectId:permit.objectId,ownerId:permit.ownerId,...invalidationInfo(permit.objectId),accountId:permit.accountId,deviceId:permit.deviceId});}catch{}}
 function invalidate(objectId,actor){const key=actor?permitId(objectId,actor):objectId;epochs.set(key,(epochs.get(key)??0)+1);for(const [id,permit] of permits)if(permit.objectId===objectId&&(!actor||permit.accountId===actor.accountId&&permit.deviceId===actor.deviceId))discard(id);
  try{onInvalidate({objectId,...invalidationInfo(objectId),...(actor?{accountId:actor.accountId,deviceId:actor.deviceId}:{})});}catch{}
 }
 function permitFor(object,row,actor){if(!actor)return null;active(actor);const id=permitId(object.objectId,actor),permit=permits.get(id);if(!permit)return null;
  if(permit.expiresAt<=now()||permit.objectAccessRevision!==(object.accessRevision??object.revision)||permit.vaultId!==row.vault_id||permit.vaultRevision!==row.revision||permit.binding!==JSON.stringify(object.binding)){discard(id,true);return null;}return permit;
 }
 function install(object,row,actor,keys,ttlMs){const id=permitId(object.objectId,actor);check(permits.has(id)||permits.size<4096,'vault_busy',429);invalidate(object.objectId,actor);const permit={objectId:object.objectId,ownerId:object.ownerId,objectAccessRevision:object.accessRevision??object.revision,binding:JSON.stringify(object.binding),vaultId:row.vault_id,vaultRevision:row.revision,accountId:actor.accountId,deviceId:actor.deviceId,dataKey:Buffer.from(keys.dataKey),wrappingKey:Buffer.from(keys.wrappingKey),receiptKey:keys.body.receiptKey,expiresAt:now()+ttlMs};
  permit.timer=setTimeout(()=>discard(id,true),ttlMs);permit.timer.unref();permits.set(id,permit);return permit;
 }
 function changed(row,objectId,reason,extra={}){try{onChange({...dto(row),...(objectId?{objectId}:{}),reason,...extra});}catch{/* Ciphertext already committed. */}}
 function rate(actor,key){const id=JSON.stringify([actor.accountId,actor.deviceId,key]),time=now();if(!attempts.has(id)&&attempts.size>=4096){for(const [key,values] of attempts)if(time-values.at(-1)>=60000)attempts.delete(key);check(attempts.size<4096,'vault_busy',429);}const window=(attempts.get(id)??[]).filter(value=>time-value<60000);check(window.length<8,'vault_rate_limited',429);window.push(time);attempts.set(id,window);}
 function previous(actor,requestId,op,expectedIntent){const prior=get('SELECT * FROM vault_commands WHERE account_id=? AND request_id=?',actor.accountId,requestId);if(!prior)return null;check(prior.operation===op&&prior.intent===expectedIntent,'vault_request_conflict',409);const receipt=JSON.parse(prior.receipt_json),current=source(receipt.vaultId);check(current?.owner_id===actor.accountId||op==='canvas.vault.relock','vault_owner_required',403);return {replayed:true,receipt,current:dto(current)};}
 function commit(actor,requestId,op,expectedIntent,callback){return transaction(()=>{const replay=previous(actor,requestId,op,expectedIntent);if(replay)return replay;check(get('SELECT count(*) n FROM vault_commands WHERE account_id=?',actor.accountId).n<50000,'vault_storage_limit',409);const row=callback(),receipt={vaultId:row.vault_id,revision:row.revision,committedAt:now()};run('INSERT INTO vault_commands VALUES(?,?,?,?,?)',actor.accountId,requestId,op,expectedIntent,JSON.stringify(receipt));return {replayed:false,receipt,current:dto(row)};});}
 function quota(actor,json,existingLength=0){check(get('SELECT count(*) n FROM vault_sources WHERE owner_id=?',actor.accountId).n<1024||existingLength>0,'vault_storage_limit',409);check(get('SELECT COALESCE(sum(length(container_json)),0) n FROM vault_sources WHERE owner_id=?',actor.accountId).n-existingLength+json.length<=64*1024*1024,'vault_storage_limit',409);}
 function same(object,before,row,captured){check(object.revision===before.revision&&JSON.stringify(object.binding)===JSON.stringify(before.binding),'vault_object_changed',409);check(row?.vault_id===captured.vault_id&&row.revision===captured.revision&&row.container_json===captured.container_json,'vault_revision_conflict',409);}
 const localKeyScope=(object,row)=>({schema:'soty.vault.local-scope.v1',kind:row.kind==='native'?'source':'placement',id:row.kind==='native'?row.vault_id:object.objectId});
 const status=(object,row,actor,includeLocalKeyScope=false)=>{const pending=advanced.pending(object),permit=pending?null:permitFor(object,row,actor),own=actor?.accountId===object.ownerId;return {...(own?dto(row):{schema:'soty.canvas-vault-status.v1',revision:row.revision}),objectId:object.objectId,locked:!permit,protection:{kind:pending?'none':'password'},...(pending?{state:'sealing',protected:false,...(own?{claimId:pending.claim_id}:{})}:{}),...(permit?{expiresAt:permit.expiresAt}:{}),...(includeLocalKeyScope&&!pending?{localKeyScope:localKeyScope(object,row)}:{})};};
 function bodyRead(context,callback){return sync(withObjectAuthority(context,proof=>{check(proof.viewer.kind==='account','vault_object_authority_required',403);const object={objectId:proof.objectId,ownerId:proof.ownerId,revision:proof.objectRevision,accessRevision:proof.objectAccessRevision??proof.objectRevision,binding:{providerId:proof.providerId,ref:proof.ref}},row=objectSource(object);check(row?.kind==='native'&&row.owner_id===proof.ownerId&&advanced.ready(row),'canvas_provider_access_denied',403);
  const actor={accountId:proof.viewer.accountId,deviceId:proof.viewer.deviceId},permit=permitFor(object,row,actor);check(permit,'canvas_object_locked',403);const body=crypto.read(row,permit.dataKey);check(['native','chunked'].includes(body.kind),'vault_storage_invalid',503);return callback(body,row,proof,permit);
 }));}
 const advanced=createVaultAdvanced({format:storage.schemaVersion,nativeProviders,db,get,all,run,transaction,source,crypto,authority,target,active,now,rate,quota,commit,changed,invalidate,bodyRead});
 async function executeOnce({actor:original,op,args={}}){
  check(canvasVaultOperations.has(op),'vault_operation_unsupported');const initial=authority(original,actor=>{check(args.expectedAccountId===actor.accountId,'vault_account_changed',401);return actor;});const {expectedAccountId,...input}=args;args=input;
  if(advancedOperations.has(op))return advanced.execute(original,op,args);
  if(op==='canvas.vault.bindingScope'){
   exact(args,['binding']);exact(args.binding,['providerId','ref']);identity(args.binding.ref);
   check(args.binding.providerId==='soty.vault'||advanced.handles(args.binding.providerId),'vault_content_invalid');
   return authority(original,actor=>{const row=args.binding.providerId==='soty.vault'?source(args.binding.ref):advanced.sourceFor({ownerId:actor.accountId,binding:args.binding});
    if(!row||row.owner_id!==actor.accountId||row.kind!=='native')return {schema:'soty.vault-binding-scope.v1',state:'unavailable'};
    check(advanced.ready(row),'vault_conversion_pending',409);return {schema:'soty.vault-binding-scope.v1',state:'available',localKeyScope:localKeyScope(null,row)};
   });
  }
  if(op==='canvas.vault.rotateReceipt'){
   exact(args,['commandId','witnessObjectId']);identity(args.commandId);identity(args.witnessObjectId);
   return target(original,args.witnessObjectId,'owner',(object,actor)=>{const row=objectSource(object);check(row&&permitFor(object,row,actor),'canvas_object_locked',403);
    const prior=get('SELECT operation,receipt_json FROM vault_commands WHERE account_id=? AND request_id=?',actor.accountId,args.commandId);
    if(!prior)return {schema:'soty.vault-rotate-receipt.v1',state:'missing'};
    check(prior.operation==='canvas.vault.rotate','vault_request_conflict',409);const receipt=JSON.parse(prior.receipt_json);check(receipt.vaultId===row.vault_id,'vault_binding_changed',403);
    return {schema:'soty.vault-rotate-receipt.v1',state:'committed',commandId:args.commandId,receipt,current:dto(row),localKeyScope:localKeyScope(object,row)};
   });
  }
  if(op==='canvas.vault.createReceipt'){
   exact(args,['sourceRequestId']);identity(args.sourceRequestId);
   return authority(original,actor=>{const prior=get('SELECT operation,receipt_json FROM vault_commands WHERE account_id=? AND request_id=?',actor.accountId,args.sourceRequestId);
    if(!prior)return {schema:'soty.vault-create-receipt.v1',state:'missing'};
    check(prior.operation==='canvas.vault.create','vault_request_conflict',409);const receipt=JSON.parse(prior.receipt_json),row=source(receipt.vaultId);
    check(row?.owner_id===actor.accountId&&row.kind==='native'&&advanced.ready(row),'vault_owner_required',403);
    return {schema:'soty.vault-create-receipt.v1',state:'committed',sourceRequestId:args.sourceRequestId,localKeyScope:localKeyScope(null,row),binding:{providerId:'soty.vault',ref:row.vault_id},revision:row.revision};
   });
  }
  if(op==='canvas.vault.status'){
   exact(args,['objectId','includeCapabilities','includeLocalKeyScope'],['objectId']);check((args.includeCapabilities===undefined||typeof args.includeCapabilities==='boolean')&&(args.includeLocalKeyScope===undefined||typeof args.includeLocalKeyScope==='boolean'));
   return target(original,args.objectId,'read',(object,actor)=>{
    const row=objectSource(object),value=row?status(object,row,actor,args.includeLocalKeyScope===true):{objectId:object.objectId,locked:false,protection:{kind:'none'}};
    if(args.includeCapabilities!==true)return value;
    return{...value,capabilities:{nativeConversion:object.ownerId===actor.accountId&&storage.schemaVersion===2&&advanced.handles(object.binding.providerId),
      placementProtection:object.ownerId===actor.accountId&&placementProviders.includes(object.binding.providerId)}};
   });
  }
  if(op==='canvas.vault.create'){
   exact(args,['requestId','password','content']);identity(args.requestId);password(args.password);const data=content(args.content),prior=authority(original,actor=>get('SELECT receipt_json FROM vault_commands WHERE account_id=? AND request_id=?',actor.accountId,args.requestId));rate(initial,'create');let keys;
   try{if(prior){const row=source(JSON.parse(prior.receipt_json).vaultId);check(row?.owner_id===initial.accountId,'vault_owner_required',403);keys=await crypto.open(row,args.password);const result=authority(original,actor=>previous(actor,args.requestId,op,intent(keys.body.receiptKey,[op,data])));return {...result,binding:{providerId:'soty.vault',ref:result.current.vaultId}};}
    const vaultId='vault_'+randomUUID().replaceAll('-',''),row={vault_id:vaultId,owner_id:initial.accountId,revision:1,kind:'native'};keys=await crypto.create(context(row),newPayload('native',data),args.password);const json=JSON.stringify(keys.container);
    const result=authority(original,actor=>commit(actor,args.requestId,op,intent(keys.body.receiptKey,[op,data]),()=>{quota(actor,json);const time=now();run('INSERT INTO vault_sources VALUES(?,?,1,\'native\',NULL,NULL,?,?,?)',vaultId,actor.accountId,json,time,time);return source(vaultId);}));if(!result.replayed)changed(source(result.current.vaultId),null,'create');return {...result,binding:{providerId:'soty.vault',ref:result.current.vaultId}};
   }finally{zero(keys);}
  }
  if(op==='canvas.vault.protect'){
   const native=target(original,args.objectId,'owner',object=>advanced.handles(object.binding.providerId));if(native&&storage.schemaVersion===2)return advanced.convert({actor:original},args);
   exact(args,['requestId','objectId','expectedObjectRevision','password']);identity(args.requestId);password(args.password);check(Number.isSafeInteger(args.expectedObjectRevision)&&args.expectedObjectRevision>=1);const before=target(original,args.objectId,'owner',object=>{check(object.revision===args.expectedObjectRevision,'vault_object_changed',409);check(placementProviders.includes(object.binding.providerId),'vault_conversion_required',409);return object;});const captured=epoch(before.objectId,initial);rate(initial,before.objectId);let keys;
   try{const existing=objectSource(before);if(existing){keys=await crypto.open(existing,args.password);return target(original,args.objectId,'owner',(object,actor)=>previous(actor,args.requestId,op,intent(keys.body.receiptKey,[op,object.objectId,args.expectedObjectRevision]))??check(false,'vault_already_protected',409));}
    const vaultId='vault_'+randomUUID().replaceAll('-',''),row={vault_id:vaultId,owner_id:initial.accountId,revision:1,kind:'placement',object_id:before.objectId,binding_json:JSON.stringify(before.binding)};keys=await crypto.create(context(row),newPayload('placement'),args.password);const json=JSON.stringify(keys.container);
    const result=target(original,args.objectId,'owner',(object,actor)=>{check(object.revision===before.revision&&epoch(object.objectId,actor)===captured,'vault_object_changed',409);return commit(actor,args.requestId,op,intent(keys.body.receiptKey,[op,object.objectId,args.expectedObjectRevision]),()=>{check(!objectSource(object),'vault_already_protected',409);quota(actor,json);const time=now();run('INSERT INTO vault_sources VALUES(?,?,1,\'placement\',?,?,?,?,?)',vaultId,actor.accountId,object.objectId,JSON.stringify(object.binding),json,time,time);return source(vaultId);});});invalidate(before.objectId);changed(source(vaultId),before.objectId,'protect');return result;
   }finally{zero(keys);}
  }
  if(op==='canvas.vault.unlock'){
   exact(args,['objectId','password','ttlMs','includeLocalKeyScope'],['objectId','password']);check(args.includeLocalKeyScope===undefined||typeof args.includeLocalKeyScope==='boolean');password(args.password);const ttlMs=args.ttlMs??300000;check(Number.isSafeInteger(ttlMs)&&ttlMs>=1000&&ttlMs<=900000,'vault_ttl_invalid');const before=target(original,args.objectId,'read',object=>({object,row:objectSource(object)}));check(before.row,'vault_not_protected',409);check(!advanced.pending(before.object)&&advanced.ready(before.row),'vault_conversion_pending',409);const captured=epoch(args.objectId,initial);rate(initial,args.objectId);let keys;
   try{keys=await crypto.open(before.row,args.password);return target(original,args.objectId,'read',(object,actor)=>{const row=objectSource(object);same(object,before.object,row,before.row);check(epoch(args.objectId,actor)===captured,'vault_unlock_cancelled',409);install(object,row,actor,keys,ttlMs);changed(row,object.objectId,'unlock');return status(object,row,actor,args.includeLocalKeyScope===true);});}finally{zero(keys);}
  }
  if(op==='canvas.vault.relock'){
   exact(args,['requestId','objectId','scope'],['requestId','objectId']);identity(args.requestId);const scope=args.scope??'device';check(['device','all'].includes(scope));return target(original,args.objectId,scope==='all'?'owner':'read',(object,actor)=>{const row=objectSource(object);check(row,'vault_not_protected',409);const result=commit(actor,args.requestId,op,publicHash([op,object.objectId,scope]),()=>row);if(!result.replayed){invalidate(object.objectId,scope==='device'?actor:null);changed(row,object.objectId,'relock');}return result;});
  }
  if(op==='canvas.vault.write'){
   exact(args,['requestId','objectId','expectedRevision','content']);identity(args.requestId);check(Number.isSafeInteger(args.expectedRevision)&&args.expectedRevision>=1);const data=content(args.content);let keys;
   try{return target(original,args.objectId,'owner',(object,actor)=>{const row=objectSource(object);check(row?.kind==='native','vault_content_invalid',409);const permit=permitFor(object,row,actor);check(permit,'canvas_object_locked',403);const requestIntent=intent(permit.receiptKey,[op,object.objectId,args.expectedRevision,data]),replay=previous(actor,args.requestId,op,requestIntent);if(replay)return replay;check(row.revision===args.expectedRevision&&row.revision<Number.MAX_SAFE_INTEGER,'vault_revision_conflict',409);
    const oldBody=crypto.read(row,permit.dataKey);if(oldBody.kind==='chunked'){const saved=advanced.writeChunked({actor,object,row,permit,args,data});keys=saved.keys;invalidate(object.objectId);install(object,saved.row,actor,keys,Math.max(1000,permit.expiresAt-now()));changed(saved.row,object.objectId,'write',saved.nativeBinding?{nativeBinding:saved.nativeBinding}:{});return saved.response;}
    const body={schema:'soty.canvas-vault-payload.v1',kind:'native',receiptKey:permit.receiptKey,content:data},next={...row,revision:row.revision+1};keys=crypto.reseal(next,body,permit.wrappingKey,JSON.parse(row.container_json).key.kdf);const json=JSON.stringify(keys.container);
    const result=commit(actor,args.requestId,op,requestIntent,()=>{const current=source(row.vault_id);check(current.revision===row.revision,'vault_revision_conflict',409);quota(actor,json,row.container_json.length);run('UPDATE vault_sources SET revision=revision+1,container_json=?,updated_at=? WHERE vault_id=?',json,now(),row.vault_id);return source(row.vault_id);});const current=source(row.vault_id);invalidate(object.objectId);install(object,current,actor,keys,Math.max(1000,permit.expiresAt-now()));changed(current,object.objectId,'write');return result;
   });}finally{zero(keys);}
  }
  if(op==='canvas.vault.rotate'){
   exact(args,['requestId','objectId','expectedRevision','oldPassword','newPassword']);identity(args.requestId);check(Number.isSafeInteger(args.expectedRevision)&&args.expectedRevision>=1);password(args.oldPassword);password(args.newPassword);const before=target(original,args.objectId,'owner',object=>({object,row:objectSource(object)}));check(before.row,'vault_not_protected',409);const prior=authority(original,actor=>get('SELECT receipt_json FROM vault_commands WHERE account_id=? AND request_id=?',actor.accountId,args.requestId));rate(initial,args.objectId);const captured=epoch(args.objectId,initial);let oldKeys,newKeys;
   try{oldKeys=await crypto.open(before.row,prior?args.newPassword:args.oldPassword);const requestIntent=intent(oldKeys.body.receiptKey,[op,args.objectId,args.expectedRevision]);if(prior)return target(original,args.objectId,'owner',(object,actor)=>previous(actor,args.requestId,op,requestIntent));check(before.row.revision===args.expectedRevision,'vault_revision_conflict',409);
    const next={...before.row,revision:before.row.revision+1};newKeys=await crypto.create(context(next),oldKeys.body,args.newPassword);const json=JSON.stringify(newKeys.container);
    const result=target(original,args.objectId,'owner',(object,actor)=>{const row=objectSource(object);same(object,before.object,row,before.row);check(epoch(args.objectId,actor)===captured,'vault_unlock_cancelled',409);check(permits.has(permitId(object.objectId,actor))||permits.size<4096,'vault_busy',429);const result=commit(actor,args.requestId,op,requestIntent,()=>{quota(actor,json,row.container_json.length);run('UPDATE vault_sources SET revision=revision+1,container_json=?,updated_at=? WHERE vault_id=?',json,now(),row.vault_id);advanced.rotateChunks(row,next,oldKeys,newKeys);return source(row.vault_id);});const current=source(row.vault_id);invalidate(object.objectId);install(object,current,actor,newKeys,300000);const native=advanced.nativeStatus(object);changed(current,object.objectId,'rotate',native?{nativeBinding:native.source}:{});return result;});return result;
   }finally{zero(oldKeys);zero(newKeys);}
  }
 }
 async function execute(request){
  const actor=authority(request.actor,value=>{check(request.args?.expectedAccountId===value.accountId,'vault_account_changed',401);return value;}),requestId=request.args?.requestId;
  if(requestId===undefined)return executeOnce(request);identity(requestId);const key=JSON.stringify([actor.accountId,requestId]),fingerprint=publicHash([request.op,request.args]);
  const running=inflight.get(key);if(running){check(running.fingerprint===fingerprint,'vault_request_conflict',409);const result=await running.promise;authority(request.actor,()=>true);return result;}
  check(inflight.size<64,'vault_busy',429);const promise=executeOnce(request);inflight.set(key,{fingerprint,promise});try{return await promise;}finally{if(inflight.get(key)?.promise===promise)inflight.delete(key);}
 }
 const provider=Object.freeze({providerId:'soty.vault',ownerRepresents:[],withBindingAuthority(request,callback){check(!closed,'vault_closed',503);const row=source(request.ref);check(row?.kind==='native'&&row.owner_id===request.objectOwnerId&&(request.mode!=='create'||request.actor?.accountId===row.owner_id)&&advanced.ready(row),'canvas_provider_access_denied',403);const result=sync(callback({allowed:true,protection:{kind:'password',sealed:true}}));const current=source(request.ref);check(current?.kind==='native'&&current.owner_id===row.owner_id&&(request.purpose==='protection'||current.revision===row.revision&&current.container_json===row.container_json),'canvas_provider_access_denied',403);return result;},project(context){return bodyRead(context,(body,row,proof)=>({schema:'soty.canvas-vault-view.v1',kind:'vault',contentType:body.kind==='chunked'?body.contentType:body.content.contentType,byteLength:body.kind==='chunked'?body.layout.size:Buffer.byteLength(body.content.bytesBase64,'base64url'),revision:row.revision,...(body.kind==='chunked'?{metadata:body.metadata}:{}),canEdit:proof.viewer.accountId===proof.ownerId}));}});
 return Object.freeze({operations:canvasVaultOperations,challengeStorage:'memory',execute,provider,
  withProtectionAuthority({actor,object},callback){ensureStorage();const pending=advanced.pending(object);if(pending)return sync(callback({kind:'none',unlocked:false,generation:pending.claim_id+':'+pending.state,state:'sealing'}));const row=objectSource(object);if(!row)return sync(callback({kind:object.protection.kind,unlocked:object.protection.kind==='none',generation:0}));check(advanced.ready(row),'vault_conversion_pending',409);const permit=permitFor(object,row,actor),generation=actor?epoch(object.objectId,actor):'public';const result=sync(callback({kind:'password',unlocked:Boolean(permit),generation:row.revision+':'+generation}));ensureStorage();const current=objectSource(object);check(current?.revision===row.revision&&current.container_json===row.container_json,'vault_access_changed',403);if(permit)check(permitFor(object,current,actor)===permit,'canvas_object_locked',403);return result;},
  readContent({context,index=0}){check(Number.isSafeInteger(index)&&index>=0,'vault_content_part_invalid');return bodyRead(context,(body,row,proof,permit)=>{if(body.kind==='chunked')return advanced.readContent(row,body,permit.dataKey,index);const bytes=Buffer.from(body.content.bytesBase64,'base64url');try{const chunks=Math.max(1,Math.ceil(bytes.length/VAULT_CHUNK_BYTES));check(index<chunks,'vault_content_part_invalid');return {contentType:body.content.contentType,bytes:Buffer.from(bytes.subarray(index*VAULT_CHUNK_BYTES,(index+1)*VAULT_CHUNK_BYTES)),index,chunks,byteLength:bytes.length,revision:row.revision};}finally{bytes.fill(0);}});},
  withCipherReceiptAuthority:(receipt,callback)=>{ensureStorage();return advanced.withCipherReceiptAuthority(receipt,callback);},
  readConvertedSource:request=>advanced.readConvertedSource(request),
  reconcileConversions:()=>{ensureStorage();return advanced.reconcile();},
  storageIdentity(){ensureStorage();return Object.freeze({projectId,schemaVersion:storage.schemaVersion});},
  checkWidgetRuntime({widgetId,actor}){identity(widgetId);active(actor);const rows=all("SELECT * FROM vault_sources WHERE kind='placement' AND json_extract(binding_json,'$.providerId')='soty.apps' AND json_extract(binding_json,'$.ref')=?",widgetId);if(!rows.length)return true;check(typeof withRuntimeTarget==='function','canvas_object_locked',403);for(const row of rows){let entered=false;sync(withRuntimeTarget({actor,objectId:row.object_id},object=>{entered=true;check(objectSource(object)?.vault_id===row.vault_id&&permitFor(object,row,actor),'canvas_object_locked',403);}));check(entered,'canvas_object_locked',403);}return true;},
  close(){if(!closed){closed=true;for(const id of permits.keys())discard(id);advanced.close();crypto.close();db.close();epochs.clear();attempts.clear();}}
 });
}

export {VAULT_CONTENT_BYTES,VAULT_CHUNK_BYTES};
export {VAULT_FILE_BYTES} from './chunks.mjs';
