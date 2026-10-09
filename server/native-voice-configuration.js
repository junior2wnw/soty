import fs from 'node:fs';
import path from 'node:path';
const need=value=>{if(!value)throw new Error('voice_installation_invalid');};
/** Root-only bounded private host installation. Credentials never enter public
 * config/HTML/RPC; only the genuine current Root actor selects its own row. */
export function loadNativeVoiceConfiguration(selected=process.env.SOTY_NATIVE_VOICE_CONFIG){
  if(!selected)return {nativeVoice:{enabled:false}};
  need(typeof selected==='string'&&path.resolve(selected)===selected);const before=fs.lstatSync(selected);
  need(before.isFile()&&!before.isSymbolicLink()&&before.size<=65536&&before.nlink===1
    &&(process.platform!=='linux'||before.uid===process.getuid()&&(before.mode&0o777)===0o600));
  const fd=fs.openSync(selected,fs.constants.O_RDONLY|(fs.constants.O_NOFOLLOW??0));let value;
  try{const held=fs.fstatSync(fd);need(held.dev===before.dev&&held.ino===before.ino&&held.size===before.size);
    const bytes=fs.readFileSync(fd);try{need(bytes.length<=65536);value=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));}finally{bytes.fill(0);}
    const after=fs.fstatSync(fd);need(after.dev===held.dev&&after.ino===held.ino&&after.size===held.size&&after.mtimeMs===held.mtimeMs&&after.ctimeMs===held.ctimeMs);
  }finally{fs.closeSync(fd);}
  need(value&&Object.keys(value).sort().join(',')==='completeReference,credentials,publicMount,readReference,schema'
    &&value.schema==='soty.native-voice-install.v1'&&Array.isArray(value.credentials)&&value.credentials.length<=128);
  const credentials=new Map();
  for(const row of value.credentials){need(row&&Object.keys(row).sort().join(',')==='accountId,audience,deviceId,reference,token'
    &&typeof row.accountId==='string'&&typeof row.deviceId==='string'&&typeof row.token==='string'&&typeof row.audience==='string'
    &&Object.keys(row.reference).sort().join(',')==='capabilityId,digest,version');
    const key=JSON.stringify([row.accountId,row.deviceId,row.reference.capabilityId,row.reference.version,row.reference.digest]);need(!credentials.has(key));credentials.set(key,{token:row.token,audience:row.audience});}
  return {nativeVoice:{enabled:true,readReference:value.readReference,completeReference:value.completeReference,publicMount:value.publicMount,
    sourceCredentialFor({actor,reference}){const value=credentials.get(JSON.stringify([actor.accountId,actor.deviceId,reference.capabilityId,reference.version,reference.digest]));
      need(value);return Object.freeze({...value});}}};
}
