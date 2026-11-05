import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {readCanvasVaultContent,CANVAS_VAULT_READ_LIMITS} from './canvas-vault-bulk-reader.mjs';
import {CANVAS_VAULT_LOCAL_LIMITS} from './canvas-vault-local.mjs';
const objectId='bulk_reader_object',CHUNK=512*1024;
const fault=code=>error=>error?.code===code;
const turn=()=>new Promise(resolve=>setImmediate(resolve));
function source(size=CHUNK*3+11){
  const chunks=Math.max(1,Math.ceil(size/CHUNK)),part=index=>Buffer.alloc(Math.min(CHUNK,size-index*CHUNK),index+1);
  return{size,chunks,part,dto:index=>({contentType:'application/octet-stream',bytesBase64:part(index).toString('base64url'),index,chunks,byteLength:size,revision:7})};
}
const hash=value=>createHash('sha256').update(value).digest('hex');

test('fifty bounded parts reassemble exact25MiB while journal/new creation limits remain1MiB',async()=>{
  const f=source(CANVAS_VAULT_READ_LIMITS.bytes),calls=[];
  const result=await readCanvasVaultContent({objectId,isCurrent:()=>true,expected:{revision:7,contentType:'application/octet-stream',byteLength:f.size},readPart:async request=>{calls.push(request);assert.equal(Object.isFrozen(request),true);assert.deepEqual(request,{objectId,part:'vault',index:calls.length-1});return f.dto(request.index);}});
  const expected=createHash('sha256');for(let index=0;index<f.chunks;index++)expected.update(f.part(index));assert.equal(result.bytes.byteLength,f.size);assert.equal(hash(result.bytes),expected.digest('hex'));assert.equal(calls.length,50);assert.equal(result.revision,7);assert.equal(CANVAS_VAULT_LOCAL_LIMITS.payloadBytes,1024*1024);result.bytes.fill(0);
});
for(const size of[0,1,1024*1024,1024*1024+1])test(`small/legacy read${size} preserves the existing result contract`,async()=>{
  const f=source(size),result=await readCanvasVaultContent({objectId,isCurrent:()=>true,readPart:async request=>({...f.dto(request.index),ok:true})});assert.deepEqual(Object.keys(result).sort(),['bytes','contentType','revision']);assert.equal(result.bytes.length,size);result.bytes.fill(0);
});

async function completeOnlyOracle(reader=readCanvasVaultContent){
  const f=source(CHUNK+13);let release,began=false,resolved=false;
  const pending=reader({objectId,isCurrent:()=>true,readPart:async request=>{if(request.index===1){began=true;await new Promise(resolve=>{release=resolve;});}return f.dto(request.index);}}).then(value=>{resolved=true;return value;});
  try{while(!began&&!resolved)await turn();assert.equal(resolved,false,'No prefix may escape before the final part is verified');}finally{release?.();const result=await pending;result.bytes.fill(0);}
}
test('no first-part bytes are returned before the final part has passed verification',()=>completeOnlyOracle());

test('oversized or malformed manifests fail on part0 with no following request',async()=>{
  for(const patch of[{byteLength:25*1024*1024+1},{chunks:51},{chunks:1},{byteLength:-1},{revision:0},{revision:1.5},{contentType:'text/html\nInjected'},{objectId:'another_object'},{ok:false},{index:1}]){
    const f=source(CHUNK+1);let calls=0;await assert.rejects(readCanvasVaultContent({objectId,isCurrent:()=>true,readPart:async()=>{calls++;return{...f.dto(0),...patch};}}));assert.equal(calls,1);
  }
});
test('read-size rejection is distinct from the unchanged1MiB creation/journal error',async()=>{
  await assert.rejects(readCanvasVaultContent({objectId,isCurrent:()=>true,readPart:async()=>({...source(1).dto(0),byteLength:CANVAS_VAULT_READ_LIMITS.bytes+1})}),fault('vault_read_size'));assert.equal(CANVAS_VAULT_LOCAL_LIMITS.payloadBytes,1024*1024);
});

for(const [field,value]of[['revision',8],['contentType','image/png'],['byteLength',CHUNK*3+12],['chunks',3],['index',0],['bytesBase64',Buffer.alloc(10).toString('base64url')],['objectId','different_object']])test(`part mismatch ${field} never returns the already buffered prefix`,async()=>{
  const f=source();let calls=0,returned=false;
  await assert.rejects(readCanvasVaultContent({objectId,isCurrent:()=>true,readPart:async request=>{calls++;return request.index===1?{...f.dto(1),[field]:value}:f.dto(request.index);}}).then(value=>{returned=true;return value;}));assert.equal(returned,false);assert.equal(calls,2);
});

test('current SDK epoch/account is checked before/after every read and at final handoff',async()=>{
  const f=source(CHUNK+13),captured={epoch:1};let session=captured,calls=0;
  await assert.rejects(readCanvasVaultContent({objectId,isCurrent:()=>session===captured,readPart:async request=>{calls++;const result=f.dto(request.index);if(request.index===1)session={epoch:2};return result;}}),fault('canvas_object_locked'));assert.equal(calls,2);
  calls=0;await assert.rejects(readCanvasVaultContent({objectId,isCurrent:()=>false,readPart:async()=>{calls++;return f.dto(0);}}),fault('canvas_object_locked'));assert.equal(calls,0);
  let checks=0;const tiny=source(1);await assert.rejects(readCanvasVaultContent({objectId,isCurrent:()=>++checks<5,readPart:async request=>tiny.dto(request.index)}),fault('canvas_object_locked'),'The final handoff itself must be fenced, after copying the last part');assert.equal(checks,5);
});

test('cancellation rejects promptly even when the pending transport ignores AbortSignal; no later part is requested',async()=>{
  const f=source(),controller=new AbortController();let release,began=false,calls=0;
  const reading=readCanvasVaultContent({objectId,signal:controller.signal,isCurrent:()=>true,readPart:async(request,options)=>{calls++;assert.equal(options.signal,controller.signal);if(request.index===1){began=true;await new Promise(resolve=>{release=resolve;});}return f.dto(request.index);}});
  while(!began)await turn();controller.abort();await assert.rejects(reading,fault('vault_content_cancelled'));release();await turn();assert.equal(calls,2);
});

test('a fresh Source denial is propagated without returning the prefix or making a retry under another object',async()=>{
  const f=source(),error=Object.assign(Error('canvas_access_denied'),{code:'canvas_access_denied'});let calls=0;
  await assert.rejects(readCanvasVaultContent({objectId,isCurrent:()=>true,readPart:async request=>{calls++;if(request.index===1)throw error;return f.dto(request.index);}}),value=>value===error);assert.equal(calls,2);
});

test('the expected projection is captured at read start and cannot be retargeted while part0 is in flight',async()=>{
  const f=source(1),expected={revision:7};let release,began=false;
  const reading=readCanvasVaultContent({objectId,expected,isCurrent:()=>true,readPart:async()=>{began=true;await new Promise(resolve=>{release=resolve;});return{...f.dto(0),revision:8};}});
  while(!began)await turn();expected.revision=8;release();await assert.rejects(reading,fault('vault_content_changed'));
});

async function mutated(replace){
  const url=new URL('./canvas-vault-bulk-reader.mjs',import.meta.url),source=readFileSync(url,'utf8'),broken=replace(source);assert.notEqual(broken,source);
  const absolute=broken.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`);return import(`data:text/javascript;base64,${Buffer.from(absolute).toString('base64')}`);
}
test('negative control: omitting revision equality fails the same mixed-version rejection oracle',async()=>{
  const module=await mutated(source=>source.replace('&&part.revision===first.revision','')),f=source(CHUNK+13);
  const criterion=()=>assert.rejects(module.readCanvasVaultContent({objectId,isCurrent:()=>true,readPart:async request=>({...f.dto(request.index),revision:request.index?8:7})}));
  await assert.rejects(criterion,/Missing expected rejection/);
});
test('negative control: skipping post-read SDK fencing exposes the bytes and fails the unchanged epoch oracle',async()=>{
  const module=await mutated(source=>source.replaceAll('active();','')),f=source(1);let current=true;
  await assert.rejects(assert.rejects(module.readCanvasVaultContent({objectId,isCurrent:()=>current,readPart:async()=>{current=false;return f.dto(0);}}),fault('canvas_object_locked')),/Missing expected rejection/);
});
test('negative control: returning after part0 fails the unchanged no-early-prefix oracle',async()=>{
  const module=await mutated(source=>source.replace('if(index+1===first.chunks)','if(index===0)'));
  await assert.rejects(completeOnlyOracle(module.readCanvasVaultContent),/No prefix may escape before the final part is verified/);
});
