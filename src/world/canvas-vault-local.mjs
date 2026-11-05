import {canvasVaultScope,canvasVaultScopeSlot,sameCanvasVaultScope} from './canvas-vault-scope.mjs';
export const CANVAS_VAULT_LOCAL_LIMITS=Object.freeze({payloadBytes:1024*1024,recordBytes:2*1024*1024,totalBytes:8*1024*1024,records:64,profiles:2048,iterations:600000});
// Recovery holds older Source intents, not larger new-create/write payloads.
// One database keeps password rotation atomic across both bounded buckets.
export const CANVAS_VAULT_RECOVERY_LIMITS=Object.freeze({totalBytes:64*1024*1024,records:128});
const encoder=new TextEncoder(),decoder=new TextDecoder('utf-8',{fatal:true});
const localFactories=new WeakMap();let nextFactory=0;
export class CanvasVaultError extends Error{constructor(code='vault_client_failed',details={}){super(code);this.name='CanvasVaultError';this.code=code;Object.assign(this,details);}}
export const vaultCheck=(value,code='vault_client_invalid')=>{if(!value)throw new CanvasVaultError(code);};
export const vaultId=value=>typeof value==='string'&&/^[A-Za-z0-9_.:-]{1,128}$/u.test(value);
export const vaultErrorCode=error=>{const value=String(error?.code||error?.error?.code||error?.message||'');return/^[A-Za-z0-9_.:-]{1,128}$/u.test(value)?value:'vault_client_failed';};
export const vaultClone=value=>structuredClone(value);
function exact(value,keys){vaultCheck(value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===keys.length&&keys.every(key=>Object.hasOwn(value,key)),'vault_local_invalid');}
export function vaultBase64(bytes){let binary='';for(let offset=0;offset<bytes.length;offset+=8192)binary+=String.fromCharCode(...bytes.subarray(offset,offset+8192));return btoa(binary).replaceAll('+','-').replaceAll('/','_').replace(/=+$/u,'');}
export function vaultBytes(value,max=CANVAS_VAULT_LOCAL_LIMITS.recordBytes){
  vaultCheck(typeof value==='string'&&/^[A-Za-z0-9_-]*$/u.test(value)&&value.length<=Math.ceil(max*4/3),'vault_content_invalid');
  let binary;try{binary=atob(value.replaceAll('-','+').replaceAll('_','/'));}catch{throw new CanvasVaultError('vault_content_invalid');}
  const bytes=Uint8Array.from(binary,char=>char.charCodeAt(0));vaultCheck(bytes.length<=max&&vaultBase64(bytes)===value,'vault_content_invalid');return bytes;
}
function passwordBytes(value){vaultCheck(typeof value==='string'&&value.isWellFormed()&&value.length>=8&&encoder.encode(value).length<=1024,'vault_password_invalid');return encoder.encode(value);}
function cryptoPort(value=globalThis.crypto){vaultCheck(value?.subtle&&typeof value.getRandomValues==='function','vault_secure_context_required');return value;}
function profile(value,accountId,projectId,slot){
  exact(value,['schema','accountId','projectId','slot','profileId','salt','iterations']);
  vaultCheck(value.schema==='soty.vault.local-key.v1'&&value.accountId===accountId&&value.projectId===projectId&&value.slot===slot&&vaultId(value.profileId)
    &&value.iterations===CANVAS_VAULT_LOCAL_LIMITS.iterations&&vaultBytes(value.salt,16).length===16,'vault_local_scope_changed');return vaultClone(value);
}
export function canvasVaultProfile(accountId,projectId,slot,crypto=globalThis.crypto){
  const port=cryptoPort(crypto);vaultCheck(vaultId(accountId)&&vaultId(projectId)&&vaultId(slot),'vault_local_scope_changed');
  return{schema:'soty.vault.local-key.v1',accountId,projectId,slot,profileId:port.randomUUID(),salt:vaultBase64(port.getRandomValues(new Uint8Array(16))),iterations:CANVAS_VAULT_LOCAL_LIMITS.iterations};
}
/** Password and nonextractable CryptoKey never enter IndexedDB or serialized DTOs. */
export async function deriveCanvasVaultKey(value,password,{crypto=globalThis.crypto}={}){
  const port=cryptoPort(crypto),clean=profile(value,value.accountId,value.projectId,value.slot),bytes=passwordBytes(password);
  try{
    const material=await port.subtle.importKey('raw',bytes,'PBKDF2',false,['deriveKey']);
    const key=await port.subtle.deriveKey({name:'PBKDF2',hash:'SHA-256',salt:vaultBytes(clean.salt,16),iterations:clean.iterations},material,{name:'AES-GCM',length:256},false,['encrypt','decrypt']);
    return{profile:clean,key};
  }finally{bytes.fill(0);}
}
function header(value,accountId,projectId){
  exact(value,['schema','accountId','projectId','slot','recordId','purpose','version']);
  vaultCheck(value.schema==='soty.vault.local-record.v1'&&value.accountId===accountId&&value.projectId===projectId&&vaultId(value.slot)&&vaultId(value.recordId)
    &&['create','draft','write','rotate','recovery'].includes(value.purpose)&&Number.isSafeInteger(value.version)&&value.version>0,'vault_local_scope_changed');return vaultClone(value);
}
const aad=value=>encoder.encode(JSON.stringify([value.schema,value.accountId,value.projectId,value.slot,value.recordId,value.purpose,value.version]));
export async function sealCanvasVaultRecord(session,value,payload,{crypto=globalThis.crypto}={}){
  const port=cryptoPort(crypto),clean=header(value,session.profile.accountId,session.profile.projectId);
  vaultCheck(clean.slot===session.profile.slot&&session.key?.extractable===false,'vault_local_scope_changed');
  const bytes=encoder.encode(JSON.stringify(payload));vaultCheck(bytes.length<=CANVAS_VAULT_LOCAL_LIMITS.recordBytes*3/4-128,'vault_local_quota');
  const iv=port.getRandomValues(new Uint8Array(12));
  try{const encrypted=await port.subtle.encrypt({name:'AES-GCM',iv,additionalData:aad(clean),tagLength:128},session.key,bytes);
    return{...clean,cipher:{schema:'soty.vault.local-aes-gcm.v1',profileId:session.profile.profileId,salt:session.profile.salt,iv:vaultBase64(iv),data:vaultBase64(new Uint8Array(encrypted))}};
  }finally{bytes.fill(0);}
}
export function validateCanvasVaultRecord(value,accountId,projectId){
  exact(value,['schema','accountId','projectId','slot','recordId','purpose','version','cipher']);const{cipher,...rest}=value;header(rest,accountId,projectId);
  exact(cipher,['schema','profileId','salt','iv','data']);vaultCheck(cipher.schema==='soty.vault.local-aes-gcm.v1'&&vaultId(cipher.profileId)&&vaultBytes(cipher.salt,16).length===16
    &&vaultBytes(cipher.iv,12).length===12&&vaultBytes(cipher.data).length>=16,'vault_local_invalid');
  vaultCheck(encoder.encode(JSON.stringify(value)).length<=CANVAS_VAULT_LOCAL_LIMITS.recordBytes,'vault_local_quota');return vaultClone(value);
}
export async function openCanvasVaultRecord(session,value,{crypto=globalThis.crypto}={}){
  const port=cryptoPort(crypto),record=validateCanvasVaultRecord(value,session.profile.accountId,session.profile.projectId),{cipher,...clean}=record;
  vaultCheck(clean.slot===session.profile.slot&&cipher.profileId===session.profile.profileId&&cipher.salt===session.profile.salt,'vault_local_scope_changed');let bytes;
  try{bytes=new Uint8Array(await port.subtle.decrypt({name:'AES-GCM',iv:vaultBytes(cipher.iv,12),additionalData:aad(clean),tagLength:128},session.key,vaultBytes(cipher.data)));return JSON.parse(decoder.decode(bytes));}
  catch{throw new CanvasVaultError('vault_local_password_or_data_invalid');}finally{bytes?.fill(0);}
}

/** One origin/account/project scoped cipher journal. Every mutation resolves on transaction COMMIT. */
export function createCanvasVaultLocalStore(accountId,projectId,{origin=globalThis.location?.origin,indexedDB=globalThis.indexedDB}={}){
  vaultCheck(vaultId(accountId)&&vaultId(projectId)&&typeof origin==='string'&&/^https?:\/\//u.test(origin),'vault_local_unavailable');
  const scope=JSON.stringify([origin,projectId,accountId]);let opening,closed=false;
  if(indexedDB&&(typeof indexedDB==='object'||typeof indexedDB==='function')&&!localFactories.has(indexedDB))localFactories.set(indexedDB,++nextFactory);
  const scopeKey=JSON.stringify([scope,'idb-realm',indexedDB&&localFactories.get(indexedDB)||0]);
  const maxRows=CANVAS_VAULT_LOCAL_LIMITS.profiles+CANVAS_VAULT_LOCAL_LIMITS.records+CANVAS_VAULT_RECOVERY_LIMITS.records;
  const CONTROL='soty.vault.local-source-control.v1',LINK='soty.vault.local-link.v1',OP='soty.vault.local-operation.v1',INVENTORY='soty.vault.local-inventory.v1';
  const maxMetadata=CANVAS_VAULT_LOCAL_LIMITS.profiles*3;
  function budget(records){
    for(const[recovery,limits]of [[false,CANVAS_VAULT_LOCAL_LIMITS],[true,CANVAS_VAULT_RECOVERY_LIMITS]]){
      const selected=records.filter(value=>(value.purpose==='recovery')===recovery);
      vaultCheck(selected.length<=limits.records&&selected.reduce((sum,value)=>sum+encoder.encode(JSON.stringify(value)).length,0)<=limits.totalBytes,'vault_local_quota');
    }
  }
  const rowKey=(kind,id)=>JSON.stringify([scope,kind,id]);
  const database=()=>{
    vaultCheck(!closed,'vault_local_unavailable');
    if(!opening)opening=new Promise((resolve,reject)=>{
      if(!indexedDB){reject(new CanvasVaultError('vault_local_unavailable'));return;}
      let retired=false;const request=indexedDB.open('soty-canvas-vault-local-v1',3);
      request.onupgradeneeded=()=>{const db=request.result;for(const name of['cipher','sources','inventory'])if(!db.objectStoreNames.contains(name)){const store=db.createObjectStore(name,{keyPath:'key'});store.createIndex('scope','scope');}};
      request.onerror=request.onblocked=()=>{retired=true;reject(new CanvasVaultError('vault_local_unavailable'));};
      request.onsuccess=()=>{const db=request.result;if(closed||retired){db.close();reject(new CanvasVaultError('vault_local_unavailable'));return;}db.onversionchange=()=>{db.close();opening=undefined;};resolve(db);};
    }).catch(error=>{opening=undefined;throw error;});return opening;
  };
  async function read(kind,id){const db=await database();return new Promise((resolve,reject)=>{
    const tx=db.transaction('cipher','readonly'),store=tx.objectStore('cipher'),request=id===undefined?store.index('scope').getAll(scope,maxRows+1):store.get(rowKey(kind,id));let result,failure;
    request.onsuccess=()=>{try{if(id===undefined)vaultCheck(request.result.length<=maxRows,'vault_local_quota');result=id===undefined?request.result.map(row=>row.value):request.result?.value??null;}catch(error){failure=error;tx.abort();}};tx.oncomplete=()=>resolve(result);tx.onabort=tx.onerror=()=>reject(failure??new CanvasVaultError('vault_local_unavailable'));
  });}
  async function linkTarget(slot){const db=await database();return new Promise((resolve,reject)=>{const tx=db.transaction('sources','readonly'),request=tx.objectStore('sources').get(rowKey('metadata','link:'+slot));let value;request.onsuccess=()=>{value=request.result?.value?.targetSlot??slot;};tx.oncomplete=()=>resolve(value);tx.onabort=tx.onerror=()=>reject(new CanvasVaultError('vault_local_unavailable'));});}
  async function write(callback){const db=await database();return new Promise((resolve,reject)=>{
    const tx=db.transaction(['cipher','sources','inventory'],'readwrite'),store=tx.objectStore('cipher'),sources=tx.objectStore('sources'),inventory=tx.objectStore('inventory');let result,failure,waiting=3;
    const requests=[store.index('scope').getAll(scope,maxRows+1),sources.index('scope').getAll(scope,maxMetadata+1),inventory.get(rowKey('inventory','closed'))];
    for(const request of requests)request.onsuccess=()=>{if(--waiting)return;try{
      vaultCheck(requests[0].result.length<=maxRows&&requests[1].result.length<=maxMetadata,'vault_local_quota');
      const values=requests[0].result.map(row=>row.value),metadata=requests[1].result.map(row=>row.value);let catalog=requests[2].result?.value??null;
      const control=slot=>metadata.find(value=>value.schema===CONTROL&&value.slot===slot),link=slot=>metadata.find(value=>value.schema===LINK&&value.slot===slot);
      const meta={values:metadata,control,link,get inventory(){return catalog;},
        put(value){const id=value.schema===LINK?'link:'+value.slot:value.schema===OP?'operation:'+value.slot:'control:'+value.slot;const index=metadata.findIndex(old=>old.schema===value.schema&&old.slot===value.slot);if(index<0){vaultCheck(metadata.length<maxMetadata,'vault_local_quota');metadata.push(value);}else metadata[index]=value;sources.put({key:rowKey('metadata',id),scope,value});},
        remove(value){const id=value.schema===LINK?'link:'+value.slot:value.schema===OP?'operation:'+value.slot:'control:'+value.slot;sources.delete(rowKey('metadata',id));const index=metadata.indexOf(value);if(index>=0)metadata.splice(index,1);},
        catalog(value){catalog=value;inventory.put({key:rowKey('inventory','closed'),scope,value});},
        seal(){if(!catalog){catalog={schema:INVENTORY,generation:1,profiles:values.filter(value=>value.schema==='soty.vault.local-key.v1'&&!control(value.slot)&&!metadata.some(marker=>marker.schema===OP&&marker.slot===value.slot)).map(value=>({slot:value.slot,profileId:value.profileId,state:'pending',destination:null}))};this.catalog(catalog);}return catalog;},
        writable(slot){vaultCheck(!control(slot)||control(slot).phase==='ready','vault_local_rotation_pending');vaultCheck(!catalog?.profiles.some(value=>value.slot===slot&&value.state==='pending'),'vault_local_scope_required');},
      };
      result=callback(store,values,meta);
    }catch(error){failure=error;tx.abort();}};
    tx.oncomplete=()=>resolve(result);tx.onabort=tx.onerror=()=>reject(failure??new CanvasVaultError('vault_local_unavailable'));
  });}
  const versions=list=>JSON.stringify(list.map(value=>[value.recordId,value.version]).sort((a,b)=>a[0].localeCompare(b[0])));
  const profileFor=(values,slot)=>values.find(value=>value.schema==='soty.vault.local-key.v1'&&value.slot===slot);
  const recordsFor=(values,slot)=>values.filter(value=>value.schema==='soty.vault.local-record.v1'&&value.slot===slot);
  function addProfile(store,values,clean){vaultCheck(values.filter(value=>value.schema==='soty.vault.local-key.v1').length<CANVAS_VAULT_LOCAL_LIMITS.profiles,'vault_local_quota');store.put({key:rowKey('profile',clean.slot),scope,value:clean});}
  function replacement(store,values,oldProfileId,clean,expected,next,retire){
    const prior=profileFor(values,clean.slot),current=recordsFor(values,clean.slot);
    if(prior?.profileId!==oldProfileId||versions(current)!==versions(expected))return false;
    vaultCheck(new Set(next.map(value=>value.recordId)).size===next.length&&next.every(value=>current.some(old=>old.recordId===value.recordId&&old.purpose===value.purpose&&value.version===old.version+1)),'vault_local_scope_changed');
    vaultCheck(Array.isArray(retire)&&retire.length<=1&&retire.every(value=>value&&Object.keys(value).length===2&&vaultId(value.recordId)&&Number.isSafeInteger(value.version)
      &&current.some(old=>old.recordId===value.recordId&&old.version===value.version&&old.purpose==='rotate')&&!next.some(next=>next.recordId===value.recordId))&&next.length+retire.length===current.length,'vault_local_scope_changed');
    budget(values.filter(value=>value.schema==='soty.vault.local-record.v1'&&value.slot!==clean.slot).concat(next));
    for(const value of current)store.delete(rowKey('record',value.recordId));store.put({key:rowKey('profile',clean.slot),scope,value:clean});for(const value of next)store.put({key:rowKey('record',value.recordId),scope,value});return true;
  }
  return{
    scopeKey,
    async ensureProfile(candidate){const clean=profile(candidate,accountId,projectId,candidate.slot);return write((store,values,meta)=>{
      const slot=meta.link(clean.slot)?.targetSlot??clean.slot,old=profileFor(values,slot);if(old){vaultCheck(!meta.control(slot)||meta.control(slot).phase==='ready','vault_local_rotation_pending');return profile(old,accountId,projectId,slot);}
      // Compatibility reads remain possible; a v3 caller cannot invent a new
      // legacy Object profile after the first canonical admission closes it.
      vaultCheck(!meta.inventory,'vault_local_scope_required');addProfile(store,values,clean);return clean;
    });},
    async ensureOperationProfile(candidate){const clean=profile(candidate,accountId,projectId,candidate.slot);return write((store,values,meta)=>{const association=meta.link(clean.slot),target=association?.targetSlot;if(target){vaultCheck(association.kind==='operation','vault_local_scope_changed');meta.writable(target);return profileFor(values,target);}vaultCheck(!meta.control(clean.slot),'vault_local_scope_changed');
      let old=profileFor(values,clean.slot);if(!old){addProfile(store,values,clean);old=clean;}meta.put({schema:OP,slot:clean.slot,profileId:old.profileId});return vaultClone(old);
    });},
    async ensureScopedProfile(source,candidate,{objectId}={}){const cleanScope=canvasVaultScope(source),slot=await canvasVaultScopeSlot(cleanScope,projectId),clean=profile(candidate,accountId,projectId,slot);vaultCheck(objectId===undefined||vaultId(objectId),'vault_local_scope_changed');return write((store,values,meta)=>{
      const catalog=meta.seal();let state=meta.control(slot);vaultCheck(!state||sameCanvasVaultScope(state.source,cleanScope),'vault_local_scope_changed');
      let old=profileFor(values,slot);if(!old){addProfile(store,values,clean);old=clean;}
      if(!state){state={schema:CONTROL,slot,source:cleanScope,profileId:old.profileId,generation:1,phase:'ready',operationId:null,recordId:null};meta.put(state);}
      if(cleanScope.kind==='placement'&&catalog.profiles.some(value=>value.slot===slot&&value.state==='pending'))meta.catalog({...catalog,generation:catalog.generation+1,profiles:catalog.profiles.map(value=>value.slot===slot?{...value,state:'migrated',destination:slot}:value)});
      vaultCheck(state.profileId===old.profileId,'vault_local_scope_changed');
      if(objectId&&objectId!==slot){const prior=meta.link(objectId);vaultCheck(!prior||prior.targetSlot===slot,'vault_local_scope_changed');meta.put({schema:LINK,slot:objectId,targetSlot:slot,kind:'object'});}
      return{profile:vaultClone(old),control:vaultClone(state),inventory:vaultClone(meta.inventory)};
    });},
    async inspectScopes(){return write((_store,values,meta)=>({inventory:vaultClone(meta.inventory),profiles:values.filter(value=>value.schema==='soty.vault.local-key.v1').map(vaultClone),controls:meta.values.filter(value=>value.schema===CONTROL).map(vaultClone),links:meta.values.filter(value=>value.schema===LINK).map(vaultClone),operations:meta.values.filter(value=>value.schema===OP).map(vaultClone)}));},
    async get(recordId){const value=await read('record',recordId);return value?validateCanvasVaultRecord(value,accountId,projectId):null;},
    async list(slot){const target=slot===undefined?undefined:await linkTarget(slot),values=await read();return values.filter(value=>value.schema==='soty.vault.local-record.v1'&&(target===undefined||value.slot===target)).map(value=>validateCanvasVaultRecord(value,accountId,projectId));},
    async listExact(slot){const values=await read();return values.filter(value=>value.schema==='soty.vault.local-record.v1'&&value.slot===slot).map(value=>validateCanvasVaultRecord(value,accountId,projectId));},
    async put(value,expectedVersion){const clean=validateCanvasVaultRecord(value,accountId,projectId);vaultCheck(Number.isSafeInteger(expectedVersion)&&expectedVersion>=0&&clean.version===expectedVersion+1,'vault_local_scope_changed');return write((store,values,meta)=>{
      meta.writable(clean.slot);
      const current=values.find(value=>value.schema==='soty.vault.local-record.v1'&&value.recordId===clean.recordId);if((current?.version??0)!==expectedVersion)return false;
      vaultCheck(!current||current.slot===clean.slot&&current.purpose===clean.purpose,'vault_local_scope_changed');
      const keyProfile=values.find(value=>value.schema==='soty.vault.local-key.v1'&&value.slot===clean.slot);
      vaultCheck(keyProfile?.profileId===clean.cipher.profileId&&keyProfile.salt===clean.cipher.salt,'vault_local_scope_changed');
      const records=values.filter(value=>value.schema==='soty.vault.local-record.v1'&&value.recordId!==clean.recordId).concat(clean);
      budget(records);
      store.put({key:rowKey('record',clean.recordId),scope,value:clean});return true;
    });},
    async remove(recordId,expectedVersion){return write((store,values,meta)=>{const value=values.find(value=>value.schema==='soty.vault.local-record.v1'&&value.recordId===recordId);if(value&&(expectedVersion===undefined||value.version===expectedVersion)){meta.writable(value.slot);store.delete(rowKey('record',recordId));}});},
    async replaceProfile(oldProfileId,candidate,expected,records,{retire=[]}={}){
      const clean=profile(candidate,accountId,projectId,candidate.slot),next=records.map(value=>validateCanvasVaultRecord(value,accountId,projectId));
      vaultCheck(next.every(value=>value.slot===clean.slot&&value.cipher.profileId===clean.profileId&&value.cipher.salt===clean.salt),'vault_local_scope_changed');
      return write((store,values,meta)=>{meta.writable(clean.slot);vaultCheck(!meta.control(clean.slot),'vault_local_scope_required');return replacement(store,values,oldProfileId,clean,expected,next,retire);});
    },
    async beginSourceRekey(slot,oldProfileId,intent,{inventoryGeneration,mode='source-rotate'}={}){const record=validateCanvasVaultRecord(intent,accountId,projectId);vaultCheck(record.slot===slot&&record.purpose==='rotate'&&['source-rotate','password-recovery'].includes(mode),'vault_local_scope_changed');return write((store,values,meta)=>{
      const state=meta.control(slot);vaultCheck(state&&state.profileId===oldProfileId,'vault_local_scope_changed');
      if(state.phase==='rekeying'){vaultCheck(state.recordId===record.recordId,'vault_local_rotation_pending');return vaultClone(state);}
      vaultCheck(state.phase==='ready'&&meta.inventory?.generation===inventoryGeneration,'vault_local_conflict');
      const rows=recordsFor(values,slot);if(mode==='source-rotate')vaultCheck(!rows.some(value=>['draft','write'].includes(value.purpose)),'vault_local_draft_pending');const prior=rows.find(value=>value.recordId===record.recordId);
      vaultCheck(!rows.some(value=>value.purpose==='rotate'&&value.recordId!==record.recordId),'vault_local_rotation_pending');vaultCheck(!prior||JSON.stringify(prior)===JSON.stringify(record),'vault_local_conflict');vaultCheck(prior||record.version===1,'vault_local_scope_changed');
      const key=profileFor(values,slot);vaultCheck(key?.profileId===record.cipher.profileId&&key.salt===record.cipher.salt,'vault_local_scope_changed');
      if(!prior){budget(values.filter(value=>value.schema==='soty.vault.local-record.v1').concat(record));store.put({key:rowKey('record',record.recordId),scope,value:record});}
      const next={...state,phase:'rekeying',mode,generation:state.generation+1,operationId:record.recordId,recordId:record.recordId};meta.put(next);return vaultClone(next);
    });},
    async commitSourceRekey(admission,candidate,expected,records,{retire=[]}={}){const clean=profile(candidate,accountId,projectId,candidate.slot),next=records.map(value=>validateCanvasVaultRecord(value,accountId,projectId));vaultCheck(next.every(value=>value.slot===clean.slot&&value.cipher.profileId===clean.profileId&&value.cipher.salt===clean.salt),'vault_local_scope_changed');return write((store,values,meta)=>{
      const state=meta.control(clean.slot);if(!state||state.phase!=='rekeying'||state.generation!==admission.generation||state.recordId!==admission.recordId||state.profileId!==admission.profileId)return false;
      vaultCheck(retire.length===1&&retire[0].recordId===state.recordId,'vault_local_scope_changed');if(!replacement(store,values,state.profileId,clean,expected,next,retire))return false;
      meta.put({...state,profileId:clean.profileId,generation:state.generation+1,phase:'ready',recordId:null,operationId:null});return true;
    });},
    async cancelSourceRekey(admission,expectedRecordVersion){return write((store,values,meta)=>{const state=meta.control(admission.slot),record=values.find(value=>value.recordId===admission.recordId);if(!state||state.phase!=='rekeying'||state.generation!==admission.generation||state.recordId!==admission.recordId||record?.version!==expectedRecordVersion)return false;store.delete(rowKey('record',record.recordId));meta.put({...state,generation:state.generation+1,phase:'ready',recordId:null,operationId:null});return true;});},
    async migrateScopes(slot,from,destination,records,{inventoryGeneration,objectIds=[]}={}){vaultCheck(Array.isArray(from)&&from.length<=CANVAS_VAULT_LOCAL_LIMITS.profiles&&new Set(from.map(value=>value.slot)).size===from.length,'vault_local_scope_changed');const next=records.map(value=>validateCanvasVaultRecord(value,accountId,projectId));return write((store,values,meta)=>{
      const state=meta.control(slot),profile=profileFor(values,slot);vaultCheck(state&&state.phase==='ready','vault_local_rotation_pending');
      if(meta.inventory?.generation!==inventoryGeneration||profile?.profileId!==destination.profileId||versions(recordsFor(values,slot))!==versions(destination.expected))return false;
      const previous=[];for(const entry of from){vaultCheck(entry.slot!==slot&&!meta.control(entry.slot),'vault_local_scope_changed');const old=profileFor(values,entry.slot),rows=recordsFor(values,entry.slot);if(old?.profileId!==entry.profileId||versions(rows)!==versions(entry.expected))return false;
        vaultCheck(meta.inventory.profiles.some(value=>value.slot===entry.slot&&value.profileId===entry.profileId&&value.state==='pending')||meta.values.some(value=>value.schema===OP&&value.slot===entry.slot&&value.profileId===entry.profileId),'vault_local_scope_changed');previous.push(...rows);}
      vaultCheck(next.length===previous.length&&new Set(next.map(value=>value.recordId)).size===next.length&&next.every(value=>value.slot===slot&&value.cipher.profileId===profile.profileId&&value.cipher.salt===profile.salt&&previous.some(old=>old.recordId===value.recordId&&old.purpose===value.purpose&&value.version===old.version+1)),'vault_local_scope_changed');
      const ids=new Set(previous.map(value=>value.recordId));vaultCheck(!recordsFor(values,slot).some(value=>ids.has(value.recordId)),'vault_local_scope_changed');budget(values.filter(value=>value.schema==='soty.vault.local-record.v1'&&!ids.has(value.recordId)).concat(next));
      for(const entry of from){for(const value of recordsFor(values,entry.slot))store.delete(rowKey('record',value.recordId));store.delete(rowKey('profile',entry.slot));const marker=meta.values.find(value=>value.schema===OP&&value.slot===entry.slot);if(marker)meta.remove(marker);meta.put({schema:LINK,slot:entry.slot,targetSlot:slot,kind:marker?'operation':'object'});}
      for(const objectId of objectIds){vaultCheck(vaultId(objectId),'vault_local_scope_changed');const old=meta.link(objectId);vaultCheck(!old||old.targetSlot===slot,'vault_local_scope_changed');if(objectId!==slot)meta.put({schema:LINK,slot:objectId,targetSlot:slot,kind:'object'});}
      for(const value of next)store.put({key:rowKey('record',value.recordId),scope,value});
      const moved=new Set(from.map(value=>value.slot));meta.catalog({...meta.inventory,generation:meta.inventory.generation+1,profiles:meta.inventory.profiles.map(value=>moved.has(value.slot)?{...value,state:'migrated',destination:slot}:value)});return true;
    });},
    close(){closed=true;if(opening)void opening.then(db=>db.close(),()=>{});},
  };
}

/** Plain snapshots are deliberately RAM-only and are discarded with the unlock session. */
export function createCanvasVaultMemoryCache(){
  const values=new Map();let epoch=0;
  return{async capture(){return String(epoch);},async isCurrent(token){return token===String(epoch);},async remember(note,token,verifiedAt=Date.now()){if(token!==String(epoch))return false;values.set(note.noteId,{note:vaultClone(note),verifiedAt});return true;},
    async get(id,token){return token===undefined||token===String(epoch)?vaultClone(values.get(id)??null):null;},async list(){return[...values.values()].map(vaultClone);},async remove(id){values.delete(id);epoch++;},async clear(){values.clear();epoch++;}};
}
