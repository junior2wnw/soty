import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { Rollout } from './rollout.mjs';
import { SafeError } from './docker-api.mjs';
import { runConnectorCli } from './cli.mjs';
import { currentStorageReaders,storageReaderLabel } from './storage-guard.mjs';
import { currentObservation } from './test-support/current-storage-observation.mjs';
import { fixture,args } from './test-support/rollout-engine.mjs';

test('the frozen full stock import closure and the sole approved runtime patch match their stage pins',()=>{
  const root=new URL('../../',import.meta.url),manifest=JSON.parse(readFileSync(new URL('./cold-verification-stage.stock.json',import.meta.url),'utf8'));
  assert.equal(manifest.stockRevision,'cbeb42586ff501899652b2010188415ce5b68d16');
  assert.equal(manifest.stock.length,60);assert.equal(manifest.patches.length,1);
  assert.equal(manifest.patches[0].path,'deploy/connector/rollout.mjs');
  const entries=new Map(manifest.stock.map(entry=>[entry.path,entry]));
  for(const entry of manifest.stock){
    const pin=manifest.patches.find(patch=>patch.path===entry.path)||entry;
    // Worktree checkout endings may be CRLF; the deliverable is git-archive LF.
    const bytes=Buffer.from(readFileSync(new URL(entry.path,root),'utf8').replaceAll('\r\n','\n'));
    assert.equal(bytes.length,pin.bytes,entry.path);assert.equal(createHash('sha256').update(bytes).digest('hex'),pin.sha256,entry.path);
    for(const dependency of entry.imports)assert.ok(entries.has(dependency),dependency);
  }
  for(const pin of manifest.guardPins)assert.equal(entries.get(pin.path).sha256,pin.sha256);
});

// These are synthetic callback claims, not backup/restore or Docker qualification.
const accepted=()=>({ok:true,sourceUnchanged:true,proof:{
  schema:'soty.rollout-cold-verification.v1',operationId:'1'.repeat(32),
  volumeSha256:'2'.repeat(64),configurationSha256:'3'.repeat(64),
  volumeInventorySha256:'4'.repeat(64),configurationInventorySha256:'5'.repeat(64),qualificationSha256:'6'.repeat(64)
}});
function assertHeld(f){
  assert.equal(f.run.state.phase,'recovery_required');
  assert.equal(f.run.state.failureCode,'cold_verification_unresolved');
  assert.equal(f.map.get(args.originalId).HostConfig.RestartPolicy.Name,'no');
  assert.ok(!f.events.includes('start-old')&&!f.events.includes('start-candidate'));
  assert.ok(!f.events.includes('helper:enter')&&!f.events.includes('helper:rollback'));
  assert.equal(f.events.filter(event=>event==='helper:status').length,1);
  assert.doesNotMatch(JSON.stringify(f.records),/synthetic-sensitive|PRIVATE_SENTINEL|"Env"|"key"/u);
}

test('optional host callback rejects config-shaped values before any operation; omission retains normal rollout',async()=>{
  for(const coldVerification of [null,true,'https://example.test/hook',{command:'anything'}])
    assert.throws(()=>new Rollout({coldVerification}),{code:'cold_verification_invalid'});
  const f=fixture();await f.run.prepare(args);await f.run.promote();
  assert.equal(f.run.state.phase,'committed');assert.equal(Object.hasOwn(f.run.state,'coldVerification'),false);
});

test('cold gate is awaited after reconciled STOP and restart fence, before offline status, enter and candidate START',{timeout:5000},async()=>{
  let reached,release;const started=new Promise(done=>{reached=done;}),gate=new Promise(done=>{release=done;});
  const result=accepted();let calls=0;
  const f=fixture({op:'stop-old',when:'after'},{coldVerification:async function(context){
    calls++;assert.equal(this,undefined);assert.equal(Object.isFrozen(context),true);
    assert.deepEqual(Object.keys(context).sort(),['schema','transaction','originalId','originalImage','candidateId','candidateImage','storageProbeImage','revision'].sort());
    assert.deepEqual(context,{schema:'soty.rollout-cold-context.v1',...args,candidateId:f.run.candidate.Id});
    assert.throws(()=>{context.originalId='other';},TypeError);
    assert.equal(f.run.state.coldVerification.state,'running');
    assert.equal(f.records.at(-1).coldVerification.state,'running');
    f.events.push('cold:running');reached();await gate;f.events.push('cold:done');return result;
  }});
  await f.run.prepare(args);let done=false;
  const pending=f.run.promote().finally(()=>{done=true;});pending.catch(()=>{});
  try{
    await started;
    assert.equal(done,false);assert.equal(f.map.get(args.originalId).State.Running,false);
    assert.equal(f.map.get(args.originalId).HostConfig.RestartPolicy.Name,'no');
    assert.ok(f.events.indexOf('stop:'+args.originalId)<f.events.indexOf('policy:'+args.originalId));
    assert.ok(f.events.indexOf('policy:'+args.originalId)<f.events.indexOf('cold:running'));
    assert.equal(f.events.filter(event=>event==='helper:status').length,1);
    assert.ok(!f.events.includes('helper:enter')&&!f.events.includes('start-candidate'));
  }finally{release();await pending;}
  assert.equal(calls,1);assert.equal(f.run.state.phase,'committed');
  const after=f.events.slice(f.events.indexOf('cold:done'));
  assert.ok(after.indexOf('helper:status')<after.indexOf('helper:enter'));
  assert.ok(after.indexOf('helper:enter')<after.indexOf('start-candidate'));
  assert.equal(f.run.state.coldVerification.state,'verified');
  assert.deepEqual(f.run.state.coldVerification.proof,result.proof);
  result.proof.volumeSha256='7'.repeat(64);
  assert.equal(f.run.state.coldVerification.proof.volumeSha256,'2'.repeat(64));
  assert.ok(Buffer.byteLength(JSON.stringify(f.run.state.coldVerification))<1800);
  assert.doesNotMatch(JSON.stringify(f.records),/synthetic-sensitive|"Env"|"key"/u);
});

test('explicit settled unchanged-source rejection enters existing guarded recovery and never starts candidate',async()=>{
  const f=fixture({}, {coldVerification:async()=>({ok:false,sourceUnchanged:true})});
  await f.run.prepare(args);await assert.rejects(f.run.promote(),{code:'cold_verification_rejected'});
  assert.equal(f.run.state.phase,'restored');assert.equal(f.run.state.coldVerification.state,'rejected');
  assert.equal(f.map.get(args.originalId).State.Running,true);
  assert.equal(f.map.get(args.originalId).HostConfig.RestartPolicy.Name,'unless-stopped');
  assert.ok(f.events.includes('start-old'));assert.ok(!f.events.includes('start-candidate'));
  assert.ok(!f.events.includes('helper:enter')&&!f.events.includes('helper:rollback'));
  assert.equal(f.run.state.applicationStart.storageGuard.schema,'soty.storage-start.v7');
});

test('a failed durable pending record prevents callback execution and leaves recovery fenced',async()=>{
  let calls=0,failed=false;const f=fixture({}, {coldVerification:async()=>{calls++;return accepted();}}),record=f.run.record;
  f.run.record=async state=>{
    if(!failed&&state.coldVerification?.state==='running'){failed=true;throw Error('PRIVATE_SENTINEL');}
    return record(state);
  };
  await f.run.prepare(args);await assert.rejects(f.run.promote(),{code:'recovery_required'});
  assert.equal(calls,0);assertHeld(f);
});

test('a verified cold gate preserves existing guarded recovery after later candidate readiness failure',async()=>{
  let calls=0;const f=fixture({op:'readiness',when:'before'}, {coldVerification:async()=>{calls++;return accepted();}});
  await f.run.prepare(args);await assert.rejects(f.run.promote(),{code:'injected_failure'});
  assert.equal(calls,1);assert.equal(f.run.state.phase,'restored');assert.equal(f.run.state.coldVerification.state,'verified');
  assert.equal(f.map.get(args.originalId).State.Running,true);
  assert.ok(f.events.indexOf('start-candidate')<f.events.indexOf('start-old'));assert.ok(f.events.includes('helper:rollback'));
});

for(const error of [new Error('PRIVATE_SENTINEL'),new SafeError('engine_response_ambiguous'),'PRIVATE_SENTINEL'])
  test(`unknown callback exception ${typeof error==='string'?'string':error.constructor.name} retains the barrier without recovery START`,async()=>{
    const f=fixture({}, {coldVerification:async()=>{throw error;}});
    await f.run.prepare(args);await assert.rejects(f.run.promote(),{code:'recovery_required'});assertHeld(f);
  });

const malformed=[undefined,null,true,false,{ok:false},{ok:false,sourceUnchanged:false},
  {ok:false,sourceUnchanged:true,Env:'PRIVATE_SENTINEL'},
  {...accepted(),sourceUnchanged:false}, {...accepted(),proof:{...accepted().proof,operationId:[]} },
  {...accepted(),proof:{...accepted().proof,volumeSha256:'PRIVATE_SENTINEL'}},
  {...accepted(),proof:{...accepted().proof,Env:'PRIVATE_SENTINEL'}},
  {...accepted(),proof:{...accepted().proof,qualificationSha256:'6'.repeat(65)}}];
for(const [index,result] of malformed.entries())test(`incomplete or unsafe callback result ${index} is unresolved, never a recovery grant`,async()=>{
  const f=fixture({}, {coldVerification:async()=>result});await f.run.prepare(args);
  await assert.rejects(f.run.promote(),{code:'recovery_required'});assertHeld(f);
});

test('proof accessors and hidden fields are not evaluated or persisted',async()=>{
  let reads=0;
  const accessor={...accepted().proof};Object.defineProperty(accessor,'volumeSha256',{enumerable:true,get(){reads++;return '2'.repeat(64);}});
  const hidden={...accepted().proof};Object.defineProperty(hidden,'Env',{value:'PRIVATE_SENTINEL'});
  for(const proof of [accessor,hidden]){
    const f=fixture({}, {coldVerification:async()=>({...accepted(),proof})});await f.run.prepare(args);
    await assert.rejects(f.run.promote(),{code:'recovery_required'});assertHeld(f);
  }
  assert.equal(reads,0);
});

for(const [name,change] of [
  ['restart fence',f=>{f.map.get(args.originalId).HostConfig.RestartPolicy={Name:'always'};}],
  ['original state',f=>{f.map.get(args.originalId).State.Running=true;}],
  ['original configuration',f=>{f.map.get(args.originalId).Config.Env.push('PRIVATE_SENTINEL=1');}],
  ['original name',f=>{f.map.get(args.originalId).Name='/unexpected-original';}],
  ['candidate state',f=>{f.map.get(f.run.candidate.Id).State.Status='exited';}],
  ['candidate name',f=>{f.map.get(f.run.candidate.Id).Name='/unexpected-candidate';}],
  ['candidate configuration',f=>{f.map.get(f.run.candidate.Id).Config.Cmd=['different'];}]
])test(`callback changing ${name} cannot clear its pending-effects barrier`,async()=>{
  const f=fixture({}, {coldVerification:async()=>{change(f);return accepted();}});
  await f.run.prepare(args);await assert.rejects(f.run.promote(),{code:'recovery_required'});
  assert.equal(f.run.state.phase,'recovery_required');assert.equal(f.run.state.failureCode,'cold_verification_unresolved');
  assert.ok(!f.events.includes('start-old')&&!f.events.includes('start-candidate')&&!f.events.includes('helper:enter'));
  assert.doesNotMatch(JSON.stringify(f.records),/PRIVATE_SENTINEL|synthetic-sensitive/u);
});

test('a STOP failure does not invoke the cold callback',async()=>{
  let calls=0;const f=fixture({op:'stop-old',when:'before'},{coldVerification:async()=>{calls++;return accepted();}});
  await f.run.prepare(args);await assert.rejects(f.run.promote());assert.equal(calls,0);assert.equal(f.run.state.phase,'aborted');
});

test('unresolved restart fence blocks the callback and keeps existing ambiguity handling',async()=>{
  let calls=0;const f=fixture({}, {coldVerification:async()=>{calls++;return accepted();}}),request=f.engine.request;
  f.engine.request=async(method,route,body)=>{
    if(method==='POST'&&route===`/containers/${args.originalId}/update`)throw new SafeError('engine_response_ambiguous');
    return request(method,route,body);
  };
  await f.run.prepare(args);await assert.rejects(f.run.promote(),{code:'operation_unresolved'});
  assert.equal(calls,0);assert.equal(f.run.state.phase,'recovery_required');assert.ok(!f.events.includes('start-old'));
});

test('cold proof does not relax actual v7 World/Apps image declarations or fallback guards',async()=>{
  for(const world of [3,6]){
    let calls=0;const f=fixture({}, {coldVerification:async()=>{calls++;return accepted();}});
    const readers=JSON.parse(currentStorageReaders);readers.readers.world=[3];readers.readers.apps=[1,2,3,4,5,6,7,8];
    f.engine.image=async image=>({Id:image,Config:{Labels:{'org.opencontainers.image.revision':args.revision,[storageReaderLabel]:JSON.stringify(readers)}}});
    f.run.storageProbe=async()=>currentObservation({world,apps:8});await f.run.prepare(args);
    if(world===3){
      await f.run.promote();assert.equal(calls,1);assert.equal(f.run.state.applicationStart.storageGuard.world,3);
      assert.equal(f.run.state.applicationStart.storageGuard.apps,8);assert.equal(f.run.state.applicationStart.storageGuard.schema,'soty.storage-start.v7');
    }else{
      await assert.rejects(f.run.promote(),{code:'storage_reader_incompatible'});assert.equal(calls,0);
      assert.ok(!f.events.includes('stop:'+args.originalId)&&!f.events.includes('start-candidate'));
    }
  }
});

test('an unchanged-source rejection still cannot restart an incompatible original image',async()=>{
  let rejected=false;
  const f=fixture({}, {coldVerification:async()=>{rejected=true;return {ok:false,sourceUnchanged:true};}});
  const readers=JSON.parse(currentStorageReaders);readers.readers.world=[3];
  f.engine.image=async image=>({Id:image,Config:{Labels:{'org.opencontainers.image.revision':args.revision,[storageReaderLabel]:JSON.stringify(readers)}}});
  f.run.storageProbe=async()=>currentObservation({world:rejected?6:3});
  await f.run.prepare(args);await assert.rejects(f.run.promote(),{code:'recovery_required'});
  assert.equal(f.run.state.failureCode,'storage_reader_incompatible');assert.equal(f.map.get(args.originalId).State.Running,false);
  assert.equal(f.map.get(args.originalId).HostConfig.RestartPolicy.Name,'no');
  assert.ok(!f.events.includes('start-old')&&!f.events.includes('start-candidate')&&!f.events.includes('helper:rollback'));
});

test('CLI exposes no callback, shell-command, URL or backup bypass flag',async()=>{
  for(const flag of ['cold-verification','backup-command','pause-after-stop','restore-receipt'])
    assert.deepEqual(await runConnectorCli(['promote','--'+flag,'https://example.test/PRIVATE_SENTINEL']),{ok:false,code:'invalid_cli'});
});
