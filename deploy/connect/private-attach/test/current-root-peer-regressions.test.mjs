import test from 'node:test';
import assert from 'node:assert/strict';
import { Writable } from 'node:stream';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { composeStagedSender } from '../staged-sender-adapter.mjs';
import { mockReadonlyFilesystem, SPEC } from '../vendor/warm/synthetic-fixture.mjs';

const sha=b=>createHash('sha256').update(b).digest('hex');
test('every package freeze pin matches exact source bytes',async()=>{
  const freeze=JSON.parse(await readFile(new URL('../../../../docs/implementation/private-attach-current-root-source-freeze-20261008.json',import.meta.url),'utf8'));
  for(const row of freeze.files){
    const bytes=await readFile(new URL('../../../../'+row.path,import.meta.url));
    assert.equal(bytes.length,row.bytes,row.path);assert.equal(sha(bytes),row.sha256,row.path);
  }
});
for(const type of ['tmpfs','ramfs','overlay','aufs','rootfs']) test(`warm rejects ephemeral data filesystem ${type} with no body`,async()=>{
  const fs=mockReadonlyFilesystem();fs.state.mounts.find(row=>row.path==='/owned/target/data').type=type;
  await assert.rejects(fs.preflight(SPEC,()=>{}),{code:'warm_topology_invalid'});
  assert.equal(fs.state.closed.length,fs.state.opened.length);
});
for(const kind of ['getter','proxy','unknown-option','invalid-limits','fake-lease']) test(`invalid ${kind} sender input closes both owned admissions`,async()=>{
  let reads=0,cancels=0,body=0;
  const abort=new AbortController();
  const client={signal:abort.signal,output:new Writable({write(chunk,_enc,cb){body+=chunk.length;cb();}}),
    completed:Promise.resolve({ok:false}),cancel(){cancels++;abort.abort();this.output.destroy();},
    async warmBeforeServingStop(){return {transaction:SPEC.transaction,nonce:SPEC.nonce,image:SPEC.image,sourcePins:SPEC.sourcePins,inputBytes:0};}};
  const adapter=composeStagedSender(client);
  const lease=await adapter.boundary.warmBeforeServingStop({transaction:SPEC.transaction,nonce:SPEC.nonce,image:SPEC.image,receiverSourceSha256:SPEC.sourcePins.receiver});
  adapter.boundary.noteServingStopped(lease);
  const options={file:'unused',privateKeyPem:'synthetic',expectedSha256:'a'.repeat(64),expectedManifestSha256:'b'.repeat(64),
    sourceWitness:{generationId:SPEC.transaction,checkpointSha256:'c'.repeat(64),inventorySha256:'d'.repeat(64)},limits:{}};
  adapter.boundary.captureLaterBackup(lease,{expectedSha256:options.expectedSha256,expectedManifestSha256:options.expectedManifestSha256,sourceWitness:options.sourceWitness});
  let value=options,usedLease=lease;
  if(kind==='getter')Object.defineProperty(value,'file',{enumerable:true,get(){reads++;return 'unused';}});
  if(kind==='proxy')value=new Proxy(value,{ownKeys(){reads++;return Reflect.ownKeys(options);},get(t,k){reads++;return t[k];}});
  if(kind==='unknown-option')value.extra=true;
  if(kind==='fake-lease')usedLease={};
  await assert.rejects(adapter.send(usedLease,value),{code:'bridge_sender_failed'});
  assert.equal(cancels,1);assert.equal(abort.signal.aborted,true);assert.equal(client.output.destroyed,true);assert.equal(body,0);assert.equal(reads,0);
  assert.throws(()=>adapter.boundary.beforeBody(lease));
  await assert.rejects(adapter.boundary.warmBeforeServingStop({transaction:SPEC.transaction,nonce:SPEC.nonce,image:SPEC.image,receiverSourceSha256:SPEC.sourcePins.receiver}));
});
