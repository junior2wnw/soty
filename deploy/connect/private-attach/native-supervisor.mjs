import { createHash } from 'node:crypto';
import { open,realpath } from 'node:fs/promises';
import { constants } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { captureSpec,encodeFrame,captureSignal,signalIsAborted } from './vendor/warm/wire-protocol.mjs';
import { SOURCE_FILES } from './vendor/warm/linux-warm-preflight.mjs';
import { record,fail,failure,codeOf } from './framing.mjs';
import { superviseNativeChild } from './supervisor-core.mjs';

async function boundedHash(path,bound){
  const h=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  try{const before=await h.stat({bigint:true});if(!before.isFile()||before.size>BigInt(bound))fail('bridge_source');
    const hash=createHash('sha256'),buffer=Buffer.alloc(65536);let count=0;
    for(;;){const {bytesRead}=await h.read(buffer,0,buffer.length,null);if(!bytesRead)break;count+=bytesRead;if(count>bound)fail('bridge_source');hash.update(buffer.subarray(0,bytesRead));}
    const after=await h.stat({bigint:true});if(count!==Number(before.size)||['dev','ino','size','mtimeNs','ctimeNs'].some(k=>before[k]!==after[k]))fail('bridge_source');return hash.digest('hex');
  }finally{await h.close();}
}
// No argument-driven executable, entry, mount, target path, or authority boolean.
// Caller must already own a pinned, read-only image + exclusive source closure.
// This source API does not manufacture that host lease or launch a container.
async function run(options){
  const v=record(options,['input','output','spec','runtimeSha256','signal']);
  try{captureSignal(v.signal);}catch{fail('bridge_signal');}
  if(process.platform!=='linux'||typeof v.runtimeSha256!=='string'||!/^[a-f0-9]{64}$/.test(v.runtimeSha256))fail('bridge_profile');
  const spec=captureSpec(v.spec),command=await realpath(process.execPath);
  if(await boundedHash(command,128*1024*1024)!==v.runtimeSha256)fail('bridge_runtime');
  for(const [key,relative]of Object.entries(SOURCE_FILES)){
    if(await boundedHash(fileURLToPath(new URL('./vendor/warm/'+relative,import.meta.url)),1024*1024)!==spec.sourcePins[key])fail('bridge_source');
  }
  if(signalIsAborted(v.signal))fail('bridge_cancelled');
  return superviseNativeChild({...v,spec,command,args:[fileURLToPath(new URL('./vendor/warm/linux-fd-entry.mjs',import.meta.url)),'--warm-spec-b64',encodeFrame(spec).toString('base64')]});
}
export async function runNativeBridge(options){
  try{return await run(options);}catch(error){throw failure(codeOf(error));}
}
