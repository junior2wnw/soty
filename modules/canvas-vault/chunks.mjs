import {randomBytes,createCipheriv,createDecipheriv,createHash} from 'node:crypto';
import {check,exact} from '../canvas-objects/contract.mjs';
export const VAULT_FILE_BYTES=25*1024*1024;
export const VAULT_PART_BYTES=512*1024;
const encode=value=>Buffer.from(value).toString('base64url');
const decode=(value,length)=>{check(typeof value==='string'&&value.length<=Math.ceil(VAULT_PART_BYTES*4/3)+8&&/^[A-Za-z0-9_-]*$/u.test(value),'vault_cipher_invalid',503);const bytes=Buffer.from(value,'base64url');check(encode(bytes)===value&&(length===undefined||bytes.length===length),'vault_cipher_invalid',503);return bytes;};
export function layout(value){exact(value,['size','chunkBytes','chunks']);check(Number.isSafeInteger(value.size)&&value.size>0&&value.size<=VAULT_FILE_BYTES&&value.chunkBytes===VAULT_PART_BYTES&&value.chunks===Math.ceil(value.size/VAULT_PART_BYTES),'vault_layout_invalid');return {...value};}
const aad=(row,parts,index)=>Buffer.from(JSON.stringify(['soty.vault-cipher-chunk.v1',row.owner_id,row.vault_id,row.revision,index,parts.chunks,parts.size,'chunk']));
export const cipherHash=value=>createHash('sha256').update(typeof value==='string'||value instanceof Uint8Array?value:JSON.stringify(value)).digest('hex');
export function sealChunk(row,parts,index,bytes,key){layout(parts);check(Number.isSafeInteger(index)&&index>=0&&index<parts.chunks&&bytes instanceof Uint8Array&&bytes.length===Math.min(VAULT_PART_BYTES,parts.size-index*VAULT_PART_BYTES)&&key instanceof Uint8Array&&key.length===32,'vault_chunk_invalid');const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(aad(row,parts,index));return {schema:'soty.vault-cipher-chunk.v1',iv:encode(iv),ciphertext:encode(Buffer.concat([cipher.update(bytes),cipher.final()])),tag:encode(cipher.getAuthTag())};}
export function openChunk(row,parts,index,body,key){layout(parts);exact(body,['schema','iv','ciphertext','tag']);check(body.schema==='soty.vault-cipher-chunk.v1'&&Number.isSafeInteger(index)&&index>=0&&index<parts.chunks&&key instanceof Uint8Array&&key.length===32,'vault_cipher_invalid',503);let first;
 try{const decipher=createDecipheriv('aes-256-gcm',key,decode(body.iv,12));decipher.setAAD(aad(row,parts,index));decipher.setAuthTag(decode(body.tag,16));first=decipher.update(decode(body.ciphertext));const bytes=Buffer.concat([first,decipher.final()]);check(bytes.length===Math.min(VAULT_PART_BYTES,parts.size-index*VAULT_PART_BYTES),'vault_cipher_invalid',503);return bytes;}catch{check(false,'vault_cipher_invalid',503);}finally{first?.fill(0);}
}
