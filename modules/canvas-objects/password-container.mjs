import {randomBytes,scryptSync,createCipheriv,createDecipheriv} from 'node:crypto';
import {check,exact,identity,CanvasObjectError} from './contract.mjs';

export const PASSWORD_CONTAINER_SCHEMA='soty.password-container.v1';
export const PASSWORD_CONTAINER_MAX_BYTES=16*1024*1024;
const kdf=Object.freeze({name:'scrypt',N:32768,r:8,p:3});
const encoded=bytes=>Buffer.from(bytes).toString('base64url');
function decoded(value,length){check(typeof value==='string'&&value.length<=Math.ceil(PASSWORD_CONTAINER_MAX_BYTES*4/3)+4&&/^[A-Za-z0-9_-]*$/u.test(value),'password_container_invalid');const bytes=Buffer.from(value,'base64url');check(encoded(bytes)===value&&(length===undefined?bytes.length<=PASSWORD_CONTAINER_MAX_BYTES:bytes.length===length),'password_container_invalid');return bytes;}
function context(value){exact(value,['ownerId','objectId','version']);identity(value.ownerId);identity(value.objectId);check(Number.isSafeInteger(value.version)&&value.version>=1,'password_container_invalid');return {ownerId:value.ownerId,objectId:value.objectId,version:value.version};}
function password(value){check(typeof value==='string'&&value.isWellFormed()&&value.length>=8&&Buffer.byteLength(value,'utf8')<=1024,'password_container_password_invalid');return value;}
const aad=(value,purpose)=>Buffer.from(JSON.stringify([PASSWORD_CONTAINER_SCHEMA,value.ownerId,value.objectId,value.version,purpose]));
function derive(value,salt){return scryptSync(password(value),salt,32,{N:kdf.N,r:kdf.r,p:kdf.p,maxmem:64*1024*1024});}
function encrypt(key,bytes,context,purpose){const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(aad(context,purpose));return {iv:encoded(iv),ciphertext:encoded(Buffer.concat([cipher.update(bytes),cipher.final()])),tag:encoded(cipher.getAuthTag())};}
function decrypt(key,body,scope,purpose){const cipher=createDecipheriv('aes-256-gcm',key,decoded(body.iv,12));cipher.setAAD(aad(scope,purpose));cipher.setAuthTag(decoded(body.tag,16));const first=cipher.update(decoded(body.ciphertext));try{return Buffer.concat([first,cipher.final()]);}finally{first.fill(0);}}
function wrap(key,value,scope){const salt=randomBytes(16),derived=derive(value,salt);try{return {kdf:{...kdf,salt:encoded(salt)},...encrypt(derived,key,scope,'key')};}finally{derived.fill(0);}}
function validate(container,expectedContext){
 exact(container,['schema','context','payload','key']);check(container.schema===PASSWORD_CONTAINER_SCHEMA,'password_container_invalid');const scope=context(container.context),expected=context(expectedContext);check(JSON.stringify(scope)===JSON.stringify(expected),'password_container_unavailable',403);
 exact(container.payload,['iv','ciphertext','tag']);exact(container.key,['kdf','iv','ciphertext','tag']);exact(container.key.kdf,['name','N','r','p','salt']);
 check(['name','N','r','p'].every(name=>container.key.kdf[name]===kdf[name]),'password_container_invalid');decoded(container.key.kdf.salt,16);decoded(container.key.ciphertext,32);
 for(const body of [container.key,container.payload]){decoded(body.iv,12);decoded(body.tag,16);decoded(body.ciphertext);}
 return scope;
}
function unwrap(container,value,scope){const derived=derive(value,decoded(container.key.kdf.salt,16));try{return decrypt(derived,container.key,scope,'key');}finally{derived.fill(0);}}

/** Cryptographic envelope only. Callers still require fresh object/provider authority and rate limits. */
export function sealPasswordContainer({context:input,password:value,bytes}={}){
 const scope=context(input);password(value);check(bytes instanceof Uint8Array&&bytes.byteLength<=PASSWORD_CONTAINER_MAX_BYTES,'password_container_invalid');
 const plain=Buffer.from(bytes),key=randomBytes(32);try{return {schema:PASSWORD_CONTAINER_SCHEMA,context:scope,payload:encrypt(key,plain,scope,'payload'),key:wrap(key,value,scope)};}finally{plain.fill(0);key.fill(0);}
}
export function openPasswordContainer({container,context:expected,password:value}={}){
 const scope=validate(container,expected);password(value);let key;
 try{key=unwrap(container,value,scope);return decrypt(key,container.payload,scope,'payload');}catch{throw new CanvasObjectError('password_container_unavailable',403);}finally{key?.fill(0);}
}
export function rewrapPasswordContainer({container,context:expected,oldPassword,newPassword}={}){
 const scope=validate(container,expected);password(oldPassword);password(newPassword);let key;
 try{key=unwrap(container,oldPassword,scope);return {schema:PASSWORD_CONTAINER_SCHEMA,context:scope,payload:{...container.payload},key:wrap(key,newPassword,scope)};}catch{throw new CanvasObjectError('password_container_unavailable',403);}finally{key?.fill(0);}
}

/** Host key ports: async KDF callers keep these keys in RAM, never in a DTO. */
export function inspectPasswordContainer({container,context:expected}={}){validate(container,expected);return {context:{...container.context},kdf:{...container.key.kdf}};}
export function unwrapPasswordContainerWithKey({container,context:expected,wrappingKey}={}){
 const scope=validate(container,expected);check(wrappingKey instanceof Uint8Array&&wrappingKey.byteLength===32,'password_container_invalid');let key,plain;
 try{key=decrypt(wrappingKey,container.key,scope,'key');plain=decrypt(key,container.payload,scope,'payload');return key;}catch{key?.fill(0);throw new CanvasObjectError('password_container_unavailable',403);}finally{plain?.fill(0);}
}
export function readPasswordContainerWithKey({container,context:expected,dataKey}={}){
 const scope=validate(container,expected);check(dataKey instanceof Uint8Array&&dataKey.byteLength===32,'password_container_invalid');
 try{return decrypt(dataKey,container.payload,scope,'payload');}catch{throw new CanvasObjectError('password_container_unavailable',403);}
}
export function sealPasswordContainerWithKeys({context:input,bytes,dataKey,wrappingKey,keyDerivation}={}){
 const scope=context(input);check(bytes instanceof Uint8Array&&bytes.byteLength<=PASSWORD_CONTAINER_MAX_BYTES,'password_container_invalid');
 check(dataKey instanceof Uint8Array&&dataKey.byteLength===32&&wrappingKey instanceof Uint8Array&&wrappingKey.byteLength===32,'password_container_invalid');
 exact(keyDerivation,['name','N','r','p','salt']);check(['name','N','r','p'].every(name=>keyDerivation[name]===kdf[name]),'password_container_invalid');decoded(keyDerivation.salt,16);
 return {schema:PASSWORD_CONTAINER_SCHEMA,context:scope,payload:encrypt(dataKey,bytes,scope,'payload'),key:{kdf:{...keyDerivation},...encrypt(wrappingKey,dataKey,scope,'key')}};
}
