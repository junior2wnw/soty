import {CANVAS_CREATE_LIMITS,validateCanvasCreateRecord} from './canvas-create-flow.mjs';
import {nativeCreationSource} from './canvas-native-creation-inspection.mjs';
import {sealCanvasVaultRecord,openCanvasVaultRecord,vaultCheck,vaultId,vaultBase64,vaultBytes,CANVAS_VAULT_RECOVERY_LIMITS} from './canvas-vault-local.mjs';
import {canvasVaultRecoveryScope} from './canvas-vault-scope.mjs';

export const CANVAS_CREATION_RECOVERY_LIMITS=Object.freeze({records:64,partBytes:512*1024,
  metadataBytes:CANVAS_CREATE_LIMITS.metadataBytes,blobBytes:CANVAS_CREATE_LIMITS.blobBytes});
const MANIFEST='soty.creation-recovery.manifest.v1',PART='soty.creation-recovery.part.v1',RECEIPT='soty.creation-recovery.receipt.v1';
const encoder=new TextEncoder(),decoder=new TextDecoder('utf-8',{fatal:true}),clone=value=>structuredClone(value);
const fail=(condition,suffix='invalid')=>vaultCheck(condition,'vault_local_creation_'+suffix);
const sameSource=(a,b)=>a?.providerId===b?.providerId&&a?.ref===b?.ref&&a?.ownerId===b?.ownerId;
const hashPattern=/^[a-f0-9]{64}$/u;
const exact=(value,keys)=>fail(value&&typeof value==='object'&&!Array.isArray(value)
  &&[Object.prototype,null].includes(Object.getPrototypeOf(value))&&Reflect.ownKeys(value).length===keys.length
  &&keys.every(key=>Object.hasOwn(value,key)));
const compare=(a,b)=>a<b?-1:a>b?1:0;
const order=values=>values.slice().sort((a,b)=>compare(a.operationId,b.operationId));

function context(options){
  const {session,store,accountId,projectId,objectId,source,isCurrent,crypto=globalThis.crypto}=options;
  fail(vaultId(accountId)&&vaultId(projectId)&&vaultId(objectId)&&typeof isCurrent==='function'
    &&isCurrent.constructor?.name!=='AsyncFunction'&&crypto?.subtle,'scope');
  exact(source,['providerId','ref','ownerId']);
  fail(['soty.notes','soty.materials'].includes(source.providerId)&&source.ownerId===accountId&&vaultId(source.ref),'scope');
  const scoped=canvasVaultRecoveryScope(options.cryptoScope,objectId,session?.profile??{}),cryptoSlot=scoped.slot;
  fail(session?.profile?.accountId===accountId&&session.profile.projectId===projectId&&session.profile.slot===cryptoSlot
    &&session.key?.extractable===false&&session.key.algorithm?.name==='AES-GCM','scope');
  fail(store&&['ensureProfile','get','put','list'].every(name=>typeof store[name]==='function'),'scope');
  const profile=clone(session.profile),capturedSource={providerId:source.providerId,ref:source.ref,ownerId:source.ownerId};
  const active=()=>{scoped.active();vaultCheck(isCurrent()===true,'vault_account_changed');fail(session.profile.profileId===profile.profileId&&session.profile.salt===profile.salt,'profile_changed');};
  const wait=async work=>{active();const value=await work();active();return value;};
  const hash=async bytes=>[...new Uint8Array(await wait(()=>crypto.subtle.digest('SHA-256',bytes)))].map(byte=>byte.toString(16).padStart(2,'0')).join('');
  const identity=async values=>hash(encoder.encode(JSON.stringify([accountId,projectId,objectId,capturedSource,...values])));
  const currentProfile=async()=>{const current=await wait(()=>store.ensureProfile(profile));fail(current.profileId===profile.profileId&&current.salt===profile.salt,'profile_changed');};
  active();
  return{session,store,accountId,projectId,objectId,cryptoSlot,scoped:options.cryptoScope!==undefined,source:capturedSource,crypto,profile,active,wait,hash,identity,currentProfile};
}

function jsonSafe(value,depth=0){
  fail(depth<=32);
  if(value===null||typeof value==='string'||typeof value==='boolean')return;
  if(typeof value==='number'){fail(Number.isFinite(value));return;}
  if(Array.isArray(value)){fail(value.length<=8192);for(const item of value)jsonSafe(item,depth+1);return;}
  fail(value&&typeof value==='object'&&[Object.prototype,null].includes(Object.getPrototypeOf(value)));
  const descriptors=Object.getOwnPropertyDescriptors(value),keys=Reflect.ownKeys(descriptors);fail(keys.length<=256);
  for(const key of keys){const property=descriptors[key];fail(typeof key==='string'&&property.enumerable&&Object.hasOwn(property,'value'));jsonSafe(property.value,depth+1);}
}

function cleanRecord(ctx,original){
  const record=validateCanvasCreateRecord(original,ctx.accountId,ctx.projectId),source=nativeCreationSource(record);
  fail(source?.providerId===ctx.source.providerId&&source.ref===ctx.source.ref,'scope');
  let blob=null,file=null;
  if(record.sourceIntent&&Object.hasOwn(record.sourceIntent,'blob')){
    blob=original.sourceIntent.blob;
    fail(blob instanceof Blob&&record.stage==='source'&&['image','audio','video'].includes(record.kind)
      &&blob.size>0&&blob.size<=CANVAS_CREATION_RECOVERY_LIMITS.blobBytes);
    file={type:blob.type,isFile:typeof File==='function'&&blob instanceof File};
    if(file.isFile){file.name=blob.name;file.lastModified=blob.lastModified;}
    record.sourceIntent={...record.sourceIntent};delete record.sourceIntent.blob;
  }
  jsonSafe(record);const metadata=encoder.encode(JSON.stringify(record));
  fail(metadata.length<=CANVAS_CREATION_RECOVERY_LIMITS.metadataBytes,'quota');
  return{record,metadata,blob,file};
}

async function partId(ctx,entry,kind,index){return'creation_part_'+await ctx.identity(['part',entry.operationId,entry.version,kind,index]);}
async function manifestId(ctx,entries){return'creation_manifest_'+await ctx.identity(['manifest',entries.map(value=>[value.operationId,value.version])]);}

async function prepare(ctx,records){
  fail(Array.isArray(records)&&records.length>0&&records.length<=CANVAS_CREATION_RECOVERY_LIMITS.records);
  const values=order(records.map(record=>cleanRecord(ctx,record)) .map(value=>({...value,operationId:value.record.operationId,version:value.record.version})));
  fail(new Set(values.map(value=>value.operationId)).size===values.length);
  fail(values.reduce((sum,value)=>sum+value.metadata.length,0)<=CANVAS_CREATION_RECOVERY_LIMITS.metadataBytes
    &&values.reduce((sum,value)=>sum+(value.blob?.size??0),0)<=CANVAS_CREATION_RECOVERY_LIMITS.blobBytes,'quota');
  const parts=values.reduce((sum,value)=>sum+Math.ceil(value.metadata.length/CANVAS_CREATION_RECOVERY_LIMITS.partBytes)
    +(value.blob?Math.ceil(value.blob.size/CANVAS_CREATION_RECOVERY_LIMITS.partBytes):0),1);
  fail(parts<=CANVAS_VAULT_RECOVERY_LIMITS.records,'quota');
  const entries=[];
  for(const value of values){
    const metadata={bytes:value.metadata.length,sha256:await ctx.hash(value.metadata),parts:[]};
    for(let index=0;index<Math.ceil(value.metadata.length/CANVAS_CREATION_RECOVERY_LIMITS.partBytes);index++){
      const bytes=value.metadata.subarray(index*CANVAS_CREATION_RECOVERY_LIMITS.partBytes,(index+1)*CANVAS_CREATION_RECOVERY_LIMITS.partBytes);
      metadata.parts.push({recordId:await partId(ctx,value,'metadata',index),index,bytes:bytes.length,sha256:await ctx.hash(bytes)});
    }
    let blob=null;
    if(value.blob){
      const bytes=new Uint8Array(await ctx.wait(()=>value.blob.arrayBuffer()));
      try{
        blob={...value.file,bytes:bytes.length,sha256:await ctx.hash(bytes),parts:[]};
        if(value.record.sourceIntent.fileSha256!==undefined)fail(value.record.sourceIntent.fileSha256===blob.sha256,'conflict');
        if(value.record.sourceIntent.size!==undefined)fail(value.record.sourceIntent.size===bytes.length,'conflict');
        for(let index=0;index<Math.ceil(bytes.length/CANVAS_CREATION_RECOVERY_LIMITS.partBytes);index++){
          const part=bytes.subarray(index*CANVAS_CREATION_RECOVERY_LIMITS.partBytes,(index+1)*CANVAS_CREATION_RECOVERY_LIMITS.partBytes);
          blob.parts.push({recordId:await partId(ctx,value,'blob',index),index,bytes:part.length,sha256:await ctx.hash(part)});
        }
      }finally{bytes.fill(0);}
    }
    entries.push({operationId:value.operationId,version:value.version,metadata,blob});
  }
  return{values,manifest:{schema:MANIFEST,accountId:ctx.accountId,projectId:ctx.projectId,objectId:ctx.objectId,source:ctx.source,entries},archiveId:await manifestId(ctx,entries)};
}

function header(ctx,record){
  fail(record&&record.purpose==='recovery'&&record.slot===ctx.cryptoSlot&&record.accountId===ctx.accountId
    &&record.projectId===ctx.projectId&&record.cipher.profileId===ctx.profile.profileId&&record.cipher.salt===ctx.profile.salt,'profile_changed');
}
async function open(ctx,record){header(ctx,record);return ctx.wait(()=>openCanvasVaultRecord(ctx.session,record,{crypto:ctx.crypto}));}
async function write(ctx,recordId,payload){
  let previous=await ctx.wait(()=>ctx.store.get(recordId));
  if(previous){fail(JSON.stringify(await open(ctx,previous))===JSON.stringify(payload),'conflict');return;}
  const record=await ctx.wait(()=>sealCanvasVaultRecord(ctx.session,{schema:'soty.vault.local-record.v1',accountId:ctx.accountId,projectId:ctx.projectId,
    slot:ctx.cryptoSlot,recordId,purpose:'recovery',version:1},payload,{crypto:ctx.crypto}));
  if(!await ctx.wait(()=>ctx.store.put(record,0))){previous=await ctx.wait(()=>ctx.store.get(recordId));fail(previous,'missing');fail(JSON.stringify(await open(ctx,previous))===JSON.stringify(payload),'conflict');}
}
function partPayload(ctx,entry,kind,index,bytes){return{schema:PART,objectId:ctx.objectId,source:ctx.source,operationId:entry.operationId,
  creationVersion:entry.version,kind,index,bytes:bytes.length,data:vaultBase64(bytes)};}

function stream(value,blob=false){
  fail(value&&typeof value==='object');
  exact(value,blob?(value.isFile?['type','isFile','name','lastModified','bytes','sha256','parts']:['type','isFile','bytes','sha256','parts']):['bytes','sha256','parts']);
  fail(Number.isSafeInteger(value.bytes)&&value.bytes>0&&value.bytes<=(blob?CANVAS_CREATION_RECOVERY_LIMITS.blobBytes:CANVAS_CREATION_RECOVERY_LIMITS.metadataBytes)
    &&hashPattern.test(value.sha256)&&Array.isArray(value.parts)&&value.parts.length===Math.ceil(value.bytes/CANVAS_CREATION_RECOVERY_LIMITS.partBytes));
  if(blob){fail(typeof value.type==='string'&&value.type.length<=256&&typeof value.isFile==='boolean');if(value.isFile)fail(typeof value.name==='string'
    &&value.name.length<=1024&&Number.isSafeInteger(value.lastModified)&&value.lastModified>=0);}
  for(let index=0;index<value.parts.length;index++){const part=value.parts[index];exact(part,['recordId','index','bytes','sha256']);fail(vaultId(part.recordId)&&part.index===index
    &&part.bytes===Math.min(CANVAS_CREATION_RECOVERY_LIMITS.partBytes,value.bytes-index*CANVAS_CREATION_RECOVERY_LIMITS.partBytes)&&hashPattern.test(part.sha256));}
}
async function validateManifest(ctx,payload,id){
  exact(payload,['schema','accountId','projectId','objectId','source','entries']);
  fail(payload.schema===MANIFEST&&payload.accountId===ctx.accountId&&payload.projectId===ctx.projectId&&payload.objectId===ctx.objectId&&sameSource(payload.source,ctx.source),'scope');
  exact(payload.source,['providerId','ref','ownerId']);
  fail(Array.isArray(payload.entries)&&payload.entries.length>0&&payload.entries.length<=CANVAS_CREATION_RECOVERY_LIMITS.records);
  fail(payload.entries.every(value=>value&&typeof value==='object')&&new Set(payload.entries.map(value=>value.operationId)).size===payload.entries.length);
  fail(payload.entries.every((entry,index)=>index===0||compare(payload.entries[index-1].operationId,entry.operationId)<0));
  const ids=new Set([id]);let metadata=0,blobs=0;
  for(const entry of payload.entries){exact(entry,['operationId','version','metadata','blob']);fail(vaultId(entry.operationId)&&Number.isSafeInteger(entry.version)&&entry.version>0);stream(entry.metadata);metadata+=entry.metadata.bytes;
    if(entry.blob){stream(entry.blob,true);blobs+=entry.blob.bytes;}
    for(const kind of ['metadata','blob'])for(const part of entry[kind]?.parts??[]){fail(!ids.has(part.recordId)&&part.recordId===await partId(ctx,entry,kind,part.index));ids.add(part.recordId);}
  }
  fail(metadata<=CANVAS_CREATION_RECOVERY_LIMITS.metadataBytes&&blobs<=CANVAS_CREATION_RECOVERY_LIMITS.blobBytes&&ids.size<=CANVAS_VAULT_RECOVERY_LIMITS.records,'quota');
  fail(id===await manifestId(ctx,payload.entries),'scope');return payload;
}

async function readStream(ctx,entry,kind,seen){
  const description=entry[kind],bytes=new Uint8Array(description.bytes);let offset=0;
  try{
    for(const part of description.parts){const record=await ctx.wait(()=>ctx.store.get(part.recordId));fail(record,'missing');const value=await open(ctx,record);
      exact(value,['schema','objectId','source','operationId','creationVersion','kind','index','bytes','data']);
      exact(value.source,['providerId','ref','ownerId']);
      fail(value.schema===PART&&value.objectId===ctx.objectId&&sameSource(value.source,ctx.source)&&value.operationId===entry.operationId
        &&value.creationVersion===entry.version&&value.kind===kind&&value.index===part.index&&value.bytes===part.bytes);
      const chunk=vaultBytes(value.data,CANVAS_CREATION_RECOVERY_LIMITS.partBytes);
      try{fail(chunk.length===part.bytes&&await ctx.hash(chunk)===part.sha256);bytes.set(chunk,offset);offset+=chunk.length;}finally{chunk.fill(0);}
      seen.set(record.recordId,record);
    }
    fail(offset===description.bytes&&await ctx.hash(bytes)===description.sha256);return bytes;
  }catch(error){bytes.fill(0);throw error;}
}
async function verifyStored(ctx,id,expected){
  await ctx.currentProfile();const record=await ctx.wait(()=>ctx.store.get(id));fail(record,'missing');
  const manifest=await validateManifest(ctx,await open(ctx,record),id),seen=new Map([[record.recordId,record]]),records=[];
  if(expected)fail(JSON.stringify(manifest)===JSON.stringify(expected),'conflict');
  for(const entry of manifest.entries){
    const metadata=await readStream(ctx,entry,'metadata',seen);let value;
    try{value=JSON.parse(decoder.decode(metadata));}catch{fail(false);}finally{metadata.fill(0);}
    if(entry.blob){
      const bytes=await readStream(ctx,entry,'blob',seen);
      try{fail(value.sourceIntent&&typeof value.sourceIntent==='object'&&!Object.hasOwn(value.sourceIntent,'blob'));
        if(value.sourceIntent.fileSha256!==undefined)fail(value.sourceIntent.fileSha256===entry.blob.sha256);
        if(value.sourceIntent.size!==undefined)fail(value.sourceIntent.size===entry.blob.bytes);
        if(entry.blob.isFile){fail(typeof File==='function','unavailable');value.sourceIntent.blob=new File([bytes],entry.blob.name,{type:entry.blob.type,lastModified:entry.blob.lastModified});}
        else value.sourceIntent.blob=new Blob([bytes],{type:entry.blob.type});
      }finally{bytes.fill(0);}
    }
    const clean=cleanRecord(ctx,value);try{fail(clean.record.operationId===entry.operationId&&clean.record.version===entry.version);
      const restored=validateCanvasCreateRecord(value,ctx.accountId,ctx.projectId);if(entry.blob)restored.sourceIntent.blob=value.sourceIntent.blob;records.push(restored);
    }finally{clean.metadata.fill(0);}
  }
  // Profile replacement atomically rewrites ALL these records. Check exact
  // committed rows after decrypt, then reject a profile changed during that read.
  await ctx.currentProfile();const current=await ctx.wait(()=>ctx.store.list(ctx.cryptoSlot));
  for(const [id,old] of seen){const fresh=current.find(value=>value.recordId===id);fail(fresh&&JSON.stringify(fresh)===JSON.stringify(old),'changed');header(ctx,fresh);}
  await ctx.currentProfile();ctx.active();
  const receipt=Object.freeze({schema:RECEIPT,archiveId:id,objectId:ctx.objectId,source:Object.freeze({...ctx.source}),profileId:ctx.profile.profileId,
    cipherRecords:Object.freeze([...seen.values()].map(value=>Object.freeze({recordId:value.recordId,version:value.version})).sort((a,b)=>compare(a.recordId,b.recordId))),
    creations:Object.freeze(manifest.entries.map(value=>Object.freeze({operationId:value.operationId,version:value.version})))});
  return{archiveId:id,source:clone(ctx.source),records,receipt};
}

/** Private cipher archive only. The caller owns Source authority, writer
 * fences and exact Creation retirement; this module never replays a Source. */
export async function archiveCreationIntents(options){
  const ctx=context(options);await ctx.currentProfile();const prepared=await prepare(ctx,options.records);
  try{
    for(let item=0;item<prepared.values.length;item++){
      const value=prepared.values[item],entry=prepared.manifest.entries[item];
      for(const part of entry.metadata.parts){const bytes=value.metadata.subarray(part.index*CANVAS_CREATION_RECOVERY_LIMITS.partBytes,(part.index+1)*CANVAS_CREATION_RECOVERY_LIMITS.partBytes);
        await write(ctx,part.recordId,partPayload(ctx,entry,'metadata',part.index,bytes));}
      for(const part of entry.blob?.parts??[]){const bytes=new Uint8Array(await ctx.wait(()=>value.blob.slice(part.index*CANVAS_CREATION_RECOVERY_LIMITS.partBytes,(part.index+1)*CANVAS_CREATION_RECOVERY_LIMITS.partBytes).arrayBuffer()));
        try{fail(bytes.length===part.bytes&&await ctx.hash(bytes)===part.sha256,'conflict');await write(ctx,part.recordId,partPayload(ctx,entry,'blob',part.index,bytes));}finally{bytes.fill(0);}}
    }
    await write(ctx,prepared.archiveId,prepared.manifest);
    return(await verifyStored(ctx,prepared.archiveId,prepared.manifest)).receipt;
  }finally{for(const value of prepared.values)value.metadata.fill(0);}
}

/** A serialized receipt is a selector only. Always authenticate fresh stored
 * manifest/parts against the exact original Creation snapshot before CAS. */
export async function verifyCreationRecoveries(options){
  const ctx=context(options),prepared=await prepare(ctx,options.records);
  try{if(options.receipt)fail(options.receipt.schema===RECEIPT&&options.receipt.archiveId===prepared.archiveId
    &&options.receipt.objectId===ctx.objectId&&sameSource(options.receipt.source,ctx.source),'scope');
    return(await verifyStored(ctx,prepared.archiveId,prepared.manifest)).receipt;
  }finally{for(const value of prepared.values)value.metadata.fill(0);}
}

/** Explicit restoration returns full private original records/Blob in RAM.
 * Prefixes only locate candidates; validated AEAD manifests confer no Source API rights. */
export async function readCreationRecoveries(options){
  const ctx=context(options);await ctx.currentProfile();
  if(options.archiveId!==undefined){fail(vaultId(options.archiveId));return[await verifyStored(ctx,options.archiveId)];}
  const records=await ctx.wait(()=>ctx.store.list(ctx.cryptoSlot)),results=[];let ownParts=false;
  for(const record of records)if(record.purpose==='recovery'&&(record.recordId.startsWith('creation_manifest_')||record.recordId.startsWith('creation_part_'))){
    if(ctx.scoped){const value=await open(ctx,record);if(value.objectId!==ctx.objectId)continue;}
    if(record.recordId.startsWith('creation_manifest_'))results.push(await verifyStored(ctx,record.recordId));else ownParts=true;
  }
  if(!results.length&&ownParts)fail(false,'incomplete');
  ctx.active();return results;
}
