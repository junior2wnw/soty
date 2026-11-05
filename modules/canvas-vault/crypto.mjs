import {scrypt,randomBytes,createHmac,createHash} from 'node:crypto';
import {check} from '../canvas-objects/contract.mjs';
import {inspectPasswordContainer,unwrapPasswordContainerWithKey,readPasswordContainerWithKey,sealPasswordContainerWithKeys} from '../canvas-objects/password-container.mjs';
import {layout} from './chunks.mjs';

export const VAULT_CONTENT_BYTES=8*1024*1024;
export const VAULT_CHUNK_BYTES=512*1024;
const encode=bytes=>Buffer.from(bytes).toString('base64url');
export function password(value){check(typeof value==='string'&&value.isWellFormed()&&value.length>=8&&Buffer.byteLength(value)<=1024,'vault_password_invalid');return value;}
export function content(value){
 check(value&&typeof value==='object'&&Object.keys(value).length===2&&typeof value.contentType==='string'&&/^[a-z0-9][a-z0-9.+-]*\/[a-z0-9][a-z0-9.+-]*(?:;charset=utf-8)?$/u.test(value.contentType)&&value.contentType.length<=128&&typeof value.bytesBase64==='string'&&value.bytesBase64.length<=Math.ceil(VAULT_CONTENT_BYTES*4/3)+4&&/^[A-Za-z0-9_-]*$/u.test(value.bytesBase64),'vault_content_invalid');
 const bytes=Buffer.from(value.bytesBase64,'base64url');check(bytes.byteLength<=VAULT_CONTENT_BYTES&&encode(bytes)===value.bytesBase64,'vault_content_invalid');return {contentType:value.contentType,bytesBase64:value.bytesBase64};
}
export function payload(bytes){
 let value;try{value=JSON.parse(Buffer.from(bytes).toString('utf8'));}catch{check(false,'vault_storage_invalid',503);}
 check(value?.schema==='soty.canvas-vault-payload.v1'&&['native','placement','chunked'].includes(value.kind)&&typeof value.receiptKey==='string'&&Buffer.from(value.receiptKey,'base64url').length===32&&encode(Buffer.from(value.receiptKey,'base64url'))===value.receiptKey,'vault_storage_invalid',503);
 if(value.kind==='native')value.content=content(value.content);else if(value.kind==='chunked'){value.layout=layout(value.layout);check(typeof value.contentType==='string'&&value.contentType.length<=128&&value.metadata&&typeof value.metadata==='object'&&Buffer.byteLength(JSON.stringify(value.metadata))<=65536,'vault_storage_invalid',503);}else check(value.content===undefined,'vault_storage_invalid',503);return value;
}
export const newPayload=(kind,data)=>({schema:'soty.canvas-vault-payload.v1',kind,receiptKey:encode(randomBytes(32)),...(kind==='native'?{content:content(data)}:{})});
export const intent=(key,value)=>createHmac('sha256',Buffer.from(key,'base64url')).update(JSON.stringify(value)).digest('hex');
export const context=row=>({ownerId:row.owner_id,objectId:row.kind==='placement'?row.vault_id+'.'+createHash('sha256').update(JSON.stringify([row.object_id,row.binding_json])).digest('hex'):row.vault_id,version:row.revision});

/** libuv performs scrypt off the event loop; this queue bounds its RAM and work. */
export function createVaultCrypto({parallelism=2,queueLimit=16}={}){
 check(Number.isSafeInteger(parallelism)&&parallelism>=1&&parallelism<=2&&Number.isSafeInteger(queueLimit)&&queueLimit>=0&&queueLimit<=16,'vault_configuration_invalid',500);
 let active=0,closed=false;const waiting=[];
 async function derive(value,salt){password(value);check(!closed,'vault_closed',503);if(active>=parallelism){check(waiting.length<queueLimit,'vault_busy',429);await new Promise((resolve,reject)=>waiting.push({resolve,reject}));}else active++;check(!closed,'vault_closed',503);
  try{return await new Promise((resolve,reject)=>scrypt(value,Buffer.from(salt,'base64url'),32,{N:32768,r:8,p:3,maxmem:64*1024*1024},(error,key)=>error?reject(error):resolve(key)));}
  finally{const next=waiting.shift();if(next)next.resolve();else active--;}
 }
 return Object.freeze({
  async create(scope,body,value){const derivation={name:'scrypt',N:32768,r:8,p:3,salt:encode(randomBytes(16))},wrappingKey=await derive(value,derivation.salt),dataKey=randomBytes(32);let bytes;
   try{bytes=Buffer.from(JSON.stringify(body));return {container:sealPasswordContainerWithKeys({context:scope,bytes,dataKey,wrappingKey,keyDerivation:derivation}),dataKey,wrappingKey,body};}catch(error){dataKey.fill(0);wrappingKey.fill(0);throw error;}finally{bytes?.fill(0);}
  },
  async open(row,value){const container=JSON.parse(row.container_json),scope=context(row),{kdf}=inspectPasswordContainer({container,context:scope}),wrappingKey=await derive(value,kdf.salt);let dataKey,bytes;
   try{dataKey=unwrapPasswordContainerWithKey({container,context:scope,wrappingKey});bytes=readPasswordContainerWithKey({container,context:scope,dataKey});return {container,dataKey,wrappingKey,body:payload(bytes)};}catch(error){dataKey?.fill(0);wrappingKey.fill(0);throw error;}finally{bytes?.fill(0);}
  },
  reseal(row,body,wrappingKey,derivation){const dataKey=randomBytes(32),bytes=Buffer.from(JSON.stringify(body));try{return {container:sealPasswordContainerWithKeys({context:context(row),bytes,dataKey,wrappingKey,keyDerivation:derivation}),dataKey,wrappingKey:Buffer.from(wrappingKey),body};}catch(error){dataKey.fill(0);throw error;}finally{bytes.fill(0);}},
  read(row,dataKey){let bytes;try{bytes=readPasswordContainerWithKey({container:JSON.parse(row.container_json),context:context(row),dataKey});return payload(bytes);}finally{bytes?.fill(0);}},
  close(){closed=true;while(waiting.length)waiting.shift().reject(Object.assign(new Error('vault_closed'),{code:'vault_closed',status:503}));}
 });
}
