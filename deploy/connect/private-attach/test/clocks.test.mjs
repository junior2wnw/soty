// V1 peer loads preserved exactly: real 16s child startup; scaled bridge-only
// 1800/225, warm1000 + ten body chunks separated by120ms. Donors unmodified.
// Old probes asserted the bugs and never ENDed a corrected successful path.
// This retake records their SAME counter predicates, then safely closes success.
import test from 'node:test';import assert from 'node:assert/strict';
import {PassThrough} from 'node:stream';import {fileURLToPath} from 'node:url';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';import os from 'node:os';import path from 'node:path';import {performance} from 'node:perf_hooks';import {pathToFileURL} from 'node:url';
import {createHostBridge} from '../host-client.mjs';import {superviseNativeChild} from '../supervisor-core.mjs';
import {encodeFrame,SOURCE_KEYS,PROFILE} from '../vendor/warm/wire-protocol.mjs';
const source=new URL('../',import.meta.url),sleep=ms=>new Promise(r=>setTimeout(r,ms));
const spec={transaction:'1234567890abcdef1234567890abcdef',targetId:'1234567890abcdef1234567890abcdef',nonce:'0123456789abcdef0123456789abcdef',image:'sha256:'+'a'.repeat(64),profile:PROFILE,sourcePins:Object.fromEntries(SOURCE_KEYS.map(k=>[k,'b'.repeat(64)]))};
const receipt={expectedSha256:'c'.repeat(64),expectedManifestSha256:'d'.repeat(64),sourceWitness:{generationId:spec.transaction,checkpointSha256:'e'.repeat(64),inventorySha256:'f'.repeat(64)}};
const write=(out,bytes)=>new Promise((resolve,reject)=>out.write(bytes,e=>e?reject(e):resolve()));
const end=out=>new Promise((resolve,reject)=>out.end(e=>e?reject(e):resolve()));
async function temp(t){const dir=await mkdtemp(path.join(os.tmpdir(),'soty-bridge-clock-'));t.after(async()=>{assert.equal(path.dirname(dir),path.resolve(os.tmpdir()));assert.ok(path.basename(dir).startsWith('soty-bridge-clock-'));await rm(dir,{recursive:true,force:true});});return dir;}
async function scaled(t){
  const dir=await temp(t);
  for(const name of ['framing.mjs','host-client.mjs','supervisor-core.mjs']){
    let body=(await readFile(new URL(name,source),'utf8')).replace(/(['"])\.\/vendor\/([^'"\r\n]+)\1/gu,(_m,_q,relative)=>JSON.stringify(new URL('vendor/'+relative,source).href));
    if(name==='framing.mjs'){assert.equal(body.split('wallMs:120000, idleMs:15000').length,2);body=body.replace('wallMs:120000, idleMs:15000','wallMs:1800, idleMs:225');}
    await writeFile(path.join(dir,name),body,{flag:'wx'});
  }
  const url=pathToFileURL(dir+path.sep);
  return {host:(await import(new URL('host-client.mjs',url))).createHostBridge,supervisor:(await import(new URL('supervisor-core.mjs',url))).superviseNativeChild,
    framing:await import(new URL('framing.mjs',url)),dir};
}
function pair(t,host=createHostBridge,supervisor=superviseNativeChild,entry=fileURLToPath(new URL('test/synthetic-child.mjs',source))){
  const requests=new PassThrough({highWaterMark:65536}),replies=new PassThrough({highWaterMark:65536}),abort=new AbortController();
  const server=supervisor({input:requests,output:replies,spec,command:process.execPath,args:[entry,encodeFrame(spec).toString('base64'),'normal'],signal:abort.signal});
  const client=host({input:replies,output:requests,spec,bodyLimit:1048576,signal:undefined});
  t.after(async()=>{client.cancel();abort.abort();requests.destroy();replies.destroy();await server;});return {client,server,requests,replies};
}
test('same native16s peer load: warm uses remaining120 wall, not premature15 idle',{timeout:25000},async t=>{
  const dir=await temp(t),entry=path.join(dir,'warm-delay.mjs');
  await writeFile(entry,'await new Promise(r=>setTimeout(r,16000));\nawait import('+JSON.stringify(new URL('test/synthetic-child.mjs',source).href)+');\n');
  const p=pair(t,undefined,undefined,entry),started=performance.now();let code;
  try{await p.client.warmBeforeServingStop();}catch(e){code=e.code;}
  const elapsedMs=performance.now()-started;
  // Only harness adaptation: close corrected successful path before server wait.
  p.client.cancel();const r=await p.server;
  const counterexample=code==='bridge_timeout'&&elapsedMs>=14900&&elapsedMs<18000&&r.bytes===0;
  t.diagnostic(JSON.stringify({case:'native-warm16s',elapsedMs,code:code??null,bodyBytes:r.bytes,counterexample}));
  assert.equal(code,undefined);assert.ok(elapsedMs>=16000&&elapsedMs<24000);assert.equal(counterexample,false);assert.equal(r.bytes,0);
});
test('same scaled peer load: warm1000 does not consume body1800; ten120 gaps finish',{timeout:10000},async t=>{
  const s=await scaled(t),p=pair(t,s.host,s.supervisor),started=performance.now();
  await p.client.warmBeforeServingStop();await sleep(1000);await p.client.bindInsideAuthenticatedHook(receipt,{check(){},authenticated:receipt});
  const bodyStarted=performance.now(),acks=[];let bodyAccepted=0,code;
  try{for(let i=0;i<10;i++){await write(p.client.output,Buffer.alloc(64,i));bodyAccepted+=64;acks.push(performance.now());await sleep(120);}}catch(e){code=e.code;}
  const failedAt=performance.now(),bodyElapsedMs=failedAt-bodyStarted,totalElapsedMs=failedAt-started;
  // Only harness adaptation: original peer never sent END on a fixed success.
  if(!code)await end(p.client.output);const r=await p.server;
  const counterexample=r.ok===false&&bodyElapsedMs<1800&&totalElapsedMs>=1750;
  t.diagnostic(JSON.stringify({case:'scaled-prebody-body-clock',bodyAccepted,bodyElapsedMs,totalElapsedMs,code:code??null,counterexample,donorClocksUnchanged:true}));
  assert.equal(code,undefined);assert.equal(r.ok,true);assert.equal(bodyAccepted,640);assert.ok(bodyElapsedMs<1800&&totalElapsedMs>1800);assert.equal(counterexample,false);
});
test('warm absolute expiry cannot be renewed by successful control frames',{timeout:6000},async t=>{
  const {framing:f}=await scaled(t),input=new PassThrough(),output=new PassThrough(),c=new f.Channel(input,output),started=performance.now();
  t.after(()=>{c.dispose();input.destroy();output.destroy();});
  for(let i=0;i<3;i++){await sleep(450);input.write(f.frame(f.TYPE.WARM,0,{i}));await c.read();}
  await assert.rejects(c.read(),e=>e.code==='bridge_timeout');
  assert.ok(performance.now()-started>=1750&&performance.now()-started<2400);assert.throws(()=>c.admitBody(),e=>e.code==='bridge_timeout');
});
test('body absolute expiry is not renewed by repeated ACK progress',{timeout:6000},async t=>{
  const {framing:f}=await scaled(t),input=new PassThrough(),output=new PassThrough(),c=new f.Channel(input,output);
  t.after(()=>{c.dispose();input.destroy();output.destroy();});c.admitBody();const started=performance.now();
  let count=0,code;try{for(let i=0;i<25;i++){await sleep(100);input.write(f.frame(f.TYPE.ACK,i,{i}));await c.read();count++;}}catch(e){code=e.code;}
  assert.equal(code,'bridge_timeout');assert.ok(count>10&&count<22);assert.ok(performance.now()-started>=1800);assert.throws(()=>c.admitBody());
});
test('body admission single-use; cancel/late warm expiry cannot admit or revive',{timeout:6000},async t=>{
  const {framing:f}=await scaled(t);
  for(const mode of ['double','cancel','expire']){
    const input=new PassThrough(),output=new PassThrough(),abort=new AbortController(),c=new f.Channel(input,output,abort.signal);
    try{
      if(mode==='double'){c.admitBody();assert.throws(()=>c.admitBody(),e=>e.code==='bridge_clock_stage');}
      if(mode==='cancel'){abort.abort();assert.throws(()=>c.admitBody(),e=>e.code==='bridge_cancelled');}
      if(mode==='expire'){await sleep(1850);assert.throws(()=>c.admitBody(),e=>e.code==='bridge_timeout');}
    }finally{c.dispose();input.destroy();output.destroy();}
  }
});
test('late final native receipt cannot pass body idle/closure fence',{timeout:7000},async t=>{
  const s=await scaled(t),entry=path.join(s.dir,'delayed-final.mjs');
  let child=await readFile(new URL('test/synthetic-child.mjs',source),'utf8');
  child=child.replace(/(['"])\.\.\/vendor\/([^'"\r\n]+)\1/gu,(_m,_q,relative)=>JSON.stringify(new URL('vendor/'+relative,source).href));
  const needle="process.stdout.write(JSON.stringify(receipt)+'\\n');";assert.equal(child.split(needle).length,2);
  child=child.replace(needle,"await new Promise(r=>setTimeout(r,500));"+needle);await writeFile(entry,child);
  const p=pair(t,s.host,s.supervisor,entry);await p.client.warmBeforeServingStop();await p.client.bindInsideAuthenticatedHook(receipt,{check(){},authenticated:receipt});
  await write(p.client.output,Buffer.from('body'));await assert.rejects(end(p.client.output));
  const r=await p.server;assert.equal(r.ok,false);assert.equal(r.unknown,true);assert.equal((await p.client.completed).ok,false);
});
