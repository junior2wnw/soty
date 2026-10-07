import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { readFile,writeFile } from 'node:fs/promises';
import { Channel,TYPE,LIMIT,frame } from '../framing.mjs';
import { createHostBridge } from '../host-client.mjs';
import { superviseNativeChild } from '../supervisor-core.mjs';
import { composeStagedSender } from '../staged-sender-adapter.mjs';
import { encodeFrame,SOURCE_KEYS,PROFILE } from '../vendor/warm/wire-protocol.mjs';
import { fixture } from './strict-sender-fixture.mjs';
const sha=v=>createHash('sha256').update(v).digest('hex');
const spec={transaction:'1234567890abcdef1234567890abcdef',targetId:'1234567890abcdef1234567890abcdef',nonce:'0123456789abcdef0123456789abcdef',image:'sha256:'+'a'.repeat(64),profile:PROFILE,
  sourcePins:Object.fromEntries(SOURCE_KEYS.map(k=>[k,'b'.repeat(64)]))};
const receipt={expectedSha256:'c'.repeat(64),expectedManifestSha256:'d'.repeat(64),sourceWitness:{generationId:spec.transaction,checkpointSha256:'e'.repeat(64),inventorySha256:'f'.repeat(64)}};
const fence={check(){},authenticated:receipt};
function pair(t,mode='normal'){
  const requests=new PassThrough({highWaterMark:65536}),replies=new PassThrough({highWaterMark:65536});
  const abort=new AbortController();
  const server=superviseNativeChild({input:requests,output:replies,spec,command:process.execPath,
    args:[fileURLToPath(new URL('./synthetic-child.mjs',import.meta.url)),encodeFrame(spec).toString('base64'),mode],signal:abort.signal});
  const client=createHostBridge({input:replies,output:requests,spec,bodyLimit:2*1024*1024,signal:undefined});
  t.after(async()=>{client.cancel();abort.abort();requests.destroy();replies.destroy();await server;});
  return {requests,replies,client,server,abort};
}
async function bind(p){await p.client.warmBeforeServingStop();await p.client.bindInsideAuthenticatedHook(receipt,fence);}
const write=(out,bytes)=>new Promise((resolve,reject)=>out.write(bytes,e=>e?reject(e):resolve()));
const end=out=>new Promise((resolve,reject)=>out.end(e=>e?reject(e):resolve()));

test('native 5FD warm read0, bind EOF, multi-frame body, physical-shaped receipt and actual native close',async t=>{
  const p=pair(t);await bind(p);const body=Buffer.alloc(180003,97);
  for(let i=0;i<body.length;i+=65536)await write(p.client.output,body.subarray(i,i+65536));
  await end(p.client.output);const result=await p.server;assert.equal(result.ok,true);assert.equal(result.nativeClosed,true);assert.equal(result.bytes,body.length);
  const client=await p.client.completed;assert.equal(client.ok,true);assert.equal(client.receipt.plaintextSha256,sha(body));assert.equal(Object.keys(client.receipt).length,8);
});
test('cancel after warm admits zero payload and settles native ownership without false success',async t=>{
  const p=pair(t);await p.client.warmBeforeServingStop();p.client.cancel();const r=await p.server;
  assert.equal(r.ok,false);assert.equal(r.bytes,0);assert.equal((await p.client.completed).unknown,true);
});
test('host denies body before authenticated hook / no receiver body',async t=>{
  const p=pair(t);await p.client.warmBeforeServingStop();await assert.rejects(write(p.client.output,Buffer.from('early')));
  assert.equal((await p.server).bytes,0);
});
test('wrong nonce BIND is rejected with zero body',async t=>{
  const p=pair(t);await p.client.warmBeforeServingStop();
  p.requests.write(frame(TYPE.BIND,0,{nonce:'0'.repeat(32),receipt}));
  const r=await p.server;assert.equal(r.ok,false);assert.equal(r.code,'bridge_nonce');assert.equal(r.bytes,0);
});
test('oversized header rejected before body allocation/adoption',async t=>{
  const p=pair(t);await p.client.warmBeforeServingStop();const header=Buffer.alloc(9);header[0]=TYPE.BODY;header.writeUInt32BE(0xffffffff,1);
  p.requests.write(header);const r=await p.server;assert.equal(r.ok,false);assert.equal(r.bytes,0);assert.equal(r.code,'bridge_size');
});
test('EOF in partial frame fails with zero body and no successful receipt',async t=>{
  const p=pair(t);await p.client.warmBeforeServingStop();p.requests.end(Buffer.from([TYPE.BIND,0]));
  const r=await p.server;assert.equal(r.ok,false);assert.equal(r.bytes,0);
});
test('lost ACK cancels rather than replaying or declaring body accepted',async t=>{
  const p=pair(t);await bind(p);
  // Actual child pipe can receive the bytes, but deliberately sever its attach
  // reply before the client can see ACK. Never infer durable zero from failure.
  const sending=write(p.client.output,Buffer.alloc(32768,4));p.replies.destroy();p.client.cancel();
  await assert.rejects(sending);const r=await p.server;assert.equal(r.ok,false);assert.equal((await p.client.completed).unknown,true);assert.equal(p.client.snapshot().bytes,0);
});
test('stderr quota is sticky failure, not a normal receipt',async t=>{
  const p=pair(t,'stderr-quota');await bind(p);
  await assert.rejects(async()=>{await write(p.client.output,Buffer.alloc(4096));await end(p.client.output);});
  assert.equal((await p.server).ok,false);
});
test('cancel reentry inside authentication fence sends no BIND/body',async t=>{
  const p=pair(t);await p.client.warmBeforeServingStop();
  await assert.rejects(p.client.bindInsideAuthenticatedHook(receipt,{authenticated:receipt,check(){p.client.cancel();}}));
  assert.equal((await p.server).bytes,0);
});
test('duplicate JSON and type/sequence headers rejected by closed framing',async()=>{
  const input=new PassThrough(),output=new PassThrough();input.on('error',()=>{});output.on('error',()=>{});
  const c=new Channel(input,output);const b=Buffer.from('{"x":1,"x":2}\n'),h=Buffer.alloc(9);h[0]=TYPE.BIND;h.writeUInt32BE(b.length,1);
  input.end(Buffer.concat([h,b]));await assert.rejects(c.read(),e=>e.code==='bridge_frame');c.dispose();input.destroy();output.destroy();
});
test('actual staged full-GCM hook composes across native child bridge; body SHA equals authenticated plaintext',async t=>{
  const source=await fixture(t),data=await source.encrypt(),p=pair(t),adapter=composeStagedSender(p.client);
  const lease=await adapter.boundary.warmBeforeServingStop({transaction:spec.transaction,nonce:spec.nonce,image:spec.image,receiverSourceSha256:spec.sourcePins.receiver},{signal:p.client.signal});
  adapter.boundary.noteServingStopped(lease); // synthetic state; no host stop.
  adapter.boundary.captureLaterBackup(lease,{expectedSha256:data.options.expectedSha256,expectedManifestSha256:data.options.expectedManifestSha256,sourceWitness:data.options.sourceWitness});
  const result=await adapter.send(lease,data.options);assert.equal(result.sender.authenticated,true);assert.equal(result.receiver.plaintextSha256,sha(data.plaintext));
  assert.equal(result.productionAuthority,false);assert.equal((await p.server).ok,true);
});
test('corrupt final GCM tag prevents BIND and every body write across native bridge',async t=>{
  const source=await fixture(t),data=await source.encrypt();const corrupt=await readFile(data.options.file);corrupt[corrupt.length-1]^=1;
  await writeFile(data.options.file,corrupt);data.options.expectedSha256=sha(corrupt);
  const p=pair(t),adapter=composeStagedSender(p.client);
  const lease=await adapter.boundary.warmBeforeServingStop({transaction:spec.transaction,nonce:spec.nonce,image:spec.image,receiverSourceSha256:spec.sourcePins.receiver},{signal:p.client.signal});
  adapter.boundary.noteServingStopped(lease);adapter.boundary.captureLaterBackup(lease,{expectedSha256:data.options.expectedSha256,expectedManifestSha256:data.options.expectedManifestSha256,sourceWitness:data.options.sourceWitness});
  await assert.rejects(adapter.send(lease,data.options));const r=await p.server;assert.equal(r.bytes,0);assert.notEqual(r.phase,'body');assert.equal(r.ok,false);
});
test('mismatched PREPARE source pins fails before any native spawn/body',async t=>{
  const p=pair(t);p.requests.write(frame(TYPE.PREPARE,0,{spec:{...spec,sourcePins:{...spec.sourcePins,entry:'1'.repeat(64)}},bodyLimit:1024}));
  const r=await p.server;assert.equal(r.code,'bridge_spec');assert.equal(r.phase,'prepare');assert.equal(r.bytes,0);
});
test('replayed BODY sequence does not write bytes twice',async t=>{
  const p=pair(t);await bind(p);await write(p.client.output,Buffer.from('once'));
  p.requests.write(frame(TYPE.BODY,1,Buffer.from('twice')));const r=await p.server;
  assert.equal(r.ok,false);assert.equal(r.code,'bridge_sequence');assert.equal(r.bytes,4);
});
test('forced cleanup remains unknown even when native child subsequently exits',async t=>{
  const p=pair(t,'hang');await bind(p);p.client.cancel();const r=await p.server;
  assert.equal(r.ok,false);assert.equal(r.unknown,true);assert.equal(r.nativeClosed,false);
});
test('malformed signal and private throwing fence stay fixed-code / getter-free',async t=>{
  let reads=0;const input=new PassThrough(),output=new PassThrough();
  assert.throws(()=>createHostBridge({input,output,spec,bodyLimit:10,signal:{get aborted(){reads++;throw Error('PRIVATE_MARKER');}}}),e=>e.code==='bridge_signal');
  assert.equal(reads,0);input.destroy();output.destroy();
  const p=pair(t);await p.client.warmBeforeServingStop();
  await assert.rejects(p.client.bindInsideAuthenticatedHook(receipt,{authenticated:receipt,check(){throw Error('PRIVATE_MARKER');}}),e=>!String(e).includes('PRIVATE_MARKER'));
  assert.equal((await p.server).bytes,0);
});
