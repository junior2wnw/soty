import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,readFile,writeFile,lstat,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createConnectorStore } from '../../server/connector-store.js';
import { connectorMaintenance } from '../../server/connector-maintenance.js';
import { fixture,args,modelProxies } from './test-support/rollout-engine.mjs';
import { rootPreservationMaintenance } from './root-preservation-release.mjs';
import { productionMaintenance } from './runtime.mjs';
import { SafeError } from './docker-api.mjs';

// Real persistence/maintenance and stock controller/guard; Engine execution,
// v7 probe DTO and cold proof are synthetic. This is not GPG/Docker qualification.
const format={ok:true,schema:'soty.storage-format.v7',rooms:2,apps:14,notes:2,capabilities:3,appRegistration:1,feedback:1,humanIdentity:3,connect:4,world:6};
const accepted=()=>({ok:true,sourceUnchanged:true,proof:{schema:'soty.rollout-cold-verification.v1',operationId:'1'.repeat(32),
  volumeSha256:'2'.repeat(64),configurationSha256:'3'.repeat(64),volumeInventorySha256:'4'.repeat(64),
  configurationInventorySha256:'5'.repeat(64),qualificationSha256:'6'.repeat(64)}});
const unknownBytes=Buffer.from([0,255,9,128,10]);

async function reopen(directory){
  const store=createConnectorStore(directory,{preserveHistory:true});
  try{await store.ready;}finally{await store.close();}
}
async function scenario(t,boundary,{unprotected=false}={}){
  const directory=await mkdtemp(path.join(tmpdir(),'soty-preservation-recovery-'));
  t.after(()=>rm(directory,{recursive:true,force:true}));
  const sqlite=path.join(directory,'connector-store.sqlite'),file=path.join(directory,'independently-owned.bin');
  await reopen(directory);
  const db=new DatabaseSync(sqlite);
  try{db.exec('CREATE TABLE independently_owned_canary(id INTEGER PRIMARY KEY, value BLOB NOT NULL); BEGIN');
    db.prepare('INSERT INTO independently_owned_canary VALUES(?,?)').run(7,unknownBytes);db.exec('COMMIT');
  }finally{db.close();}
  await writeFile(file,unknownBytes,{flag:'wx'});await reopen(directory);
  const before={sqlite:await readFile(sqlite),stat:await lstat(sqlite)};
  const fault=boundary==='readiness'?{op:'readiness',when:'before'}:boundary==='before-start'?{op:'rename-candidate',when:'before'}:{};
  const f=fixture(fault,{coldVerification:async()=>boundary==='cold-rejected'?{ok:false,sourceUnchanged:true}:accepted()});
  // Use productionProbe and actual v7 guard/receipt, not context.probe.
  f.run.storageProbe=undefined;
  const start=f.engine.start,create=f.engine.create,helperOutput=f.engine.helperOutput,ready=f.run.ready;
  const helperResults=new Map(),delegated=[],applicationStarts=[],probeBodies=[],helperVerbs=[];let candidateStarted=false;
  f.engine.create=async(name,config)=>{
    if(config.Labels?.['io.soty.storage.probe'])probeBodies.push(config);
    if(config.Labels?.['io.soty.connector-rollout.helper'])helperVerbs.push(config.Labels['io.soty.connector-rollout.helper']);
    const made=await create(name,config);
    Object.assign(f.map.get(made.Id).State,{Restarting:false,Pid:0,StartedAt:'0001-01-01T00:00:00Z'});return made;
  };
  f.engine.start=async id=>{
    const c=f.map.get(id),probe=c.Config?.Labels?.['io.soty.storage.probe'],verb=c.Config?.Labels?.['io.soty.connector-rollout.helper'];
    if(!probe&&!verb){applicationStarts.push(id);if(id===f.run.candidate?.Id&&boundary==='start-rejected')throw new SafeError('injected_failure');}
    await start(id);
    if(!probe&&!verb&&id===f.run.candidate?.Id)candidateStarted=true;
    Object.assign(c.State,{StartedAt:'2026-01-01T00:00:00Z',Pid:c.State.Running?1:0});
    if(probe)Object.assign(c.State,{Running:false,Status:'exited',ExitCode:0,Pid:0});
    if(verb){
      delegated.push(verb);
      assert.deepEqual(c.Config.Cmd,['server/connector-maintenance.js',verb]);
      helperResults.set(id,await connectorMaintenance(directory,verb==='leave'&&boundary==='admission'?'status':verb));
    }
  };
  f.engine.helperOutput=async id=>{
    const c=f.map.get(id);if(c.Config?.Labels?.['io.soty.storage.probe'])return {...format};
    if(helperResults.has(id)){
      const value=helperResults.get(id),verb=c.Config.Labels['io.soty.connector-rollout.helper'];
      if(verb==='status'&&(boundary==='precheck'||boundary==='pending'&&candidateStarted))
        return {...value,count:1,activeJobs:[{id:'synthetic-pending',status:'queued'}]};
      if(verb==='leave'&&boundary==='admission')return {...value,maintenance:true};
      return value;
    }
    return helperOutput(id);
  };
  f.run.ready=async kind=>{
    const value=await ready(kind);
    if(kind==='candidate'&&boundary==='model')return {...value,modelProxies:{...modelProxies,agentModelProxy:{...modelProxies.agentModelProxy,model:'synthetic-different-model'}}};
    if(kind==='candidate'&&boundary==='storage')return {...value,storageReady:false};
    return value;
  };
  f.run.maintenance=unprotected?productionMaintenance(f.engine):rootPreservationMaintenance(f.engine);
  await f.run.prepare(args);
  return {f,directory,sqlite,file,before,delegated,applicationStarts,probeBodies,helperVerbs,fault};
}

async function preserved(x){
  const stat=await lstat(x.sqlite);
  assert.equal(stat.dev,x.before.stat.dev);assert.equal(stat.ino,x.before.stat.ino);
  assert.deepEqual(await readFile(x.sqlite),x.before.sqlite);assert.deepEqual(await readFile(x.file),unknownBytes);
  for(let i=0;i<2;i++){
    const db=new DatabaseSync(x.sqlite,{readOnly:true});
    try{const rows=db.prepare('SELECT id,value FROM independently_owned_canary').all();
      assert.equal(rows.length,1);assert.equal(rows[0].id,7);assert.deepEqual(Buffer.from(rows[0].value),unknownBytes);
    }finally{db.close();}
    if(i===0)await reopen(x.directory);
  }
}

test('forbidden rollback refuses before any Engine operation or run access',()=>{
  const denied=new Proxy({},{get(){assert.fail('forbidden rollback must not access Engine or run');}});
  const maintenance=rootPreservationMaintenance(denied);
  assert.throws(()=>maintenance('rollback',denied),error=>error instanceof SafeError&&error.code==='release_legacy_rollback_forbidden');
});

test('negative control: unwrapped stock readiness recovery loses the genuine committed SQLite canary',async t=>{
  const x=await scenario(t,'readiness',{unprotected:true});
  await assert.rejects(x.f.run.promote(),error=>error.code==='injected_failure');
  assert.equal(x.fault.used,true);assert.equal(x.f.run.state.phase,'restored');assert.ok(x.delegated.includes('rollback'));
  assert.ok(x.applicationStarts.includes(x.f.run.candidate.Id));assert.ok(x.applicationStarts.includes(args.originalId));
  await assert.rejects(lstat(x.sqlite),error=>error.code==='ENOENT');await reopen(x.directory);
  const db=new DatabaseSync(x.sqlite,{readOnly:true});
  try{assert.equal(db.prepare("SELECT 1 FROM sqlite_schema WHERE name='independently_owned_canary'").get(),undefined);}finally{db.close();}
});

for(const boundary of ['readiness','model','storage','pending','start-rejected'])test(`CURRENT ${boundary} failure refuses legacy recovery and preserves unknown committed row/file`,async t=>{
  const x=await scenario(t,boundary);
  await assert.rejects(x.f.run.promote(),error=>error.code==='recovery_required');
  assert.equal(x.f.run.state.phase,'recovery_required');assert.equal(x.f.run.state.failureCode,'release_legacy_rollback_forbidden');
  assert.equal(x.f.map.get(args.originalId).State.Running,false);assert.equal(x.f.map.get(args.originalId).HostConfig.RestartPolicy.Name,'no');
  assert.equal(x.f.map.get(x.f.run.candidate.Id).State.Running,false);
  assert.ok(x.applicationStarts.includes(x.f.run.candidate.Id));assert.ok(!x.applicationStarts.includes(args.originalId));
  assert.deepEqual(x.delegated,['status','status','enter',...(boundary==='pending'?['status']:[])]);
  assert.ok(!x.helperVerbs.includes('rollback'));assert.ok(!x.helperVerbs.includes('leave'));
  if(boundary==='readiness'){assert.equal(x.fault.used,true);assert.ok(x.f.events.includes('ready:candidate'));}
  assert.ok(x.probeBodies.length>=3);assert.ok(x.probeBodies.every(body=>body.HostConfig.NetworkMode==='none'&&body.HostConfig.Mounts[0].ReadOnly===true));
  assert.equal(x.f.run.state.applicationStart.id,x.f.run.candidate.Id);assert.equal(x.f.run.state.applicationStart.storageGuard.schema,'soty.storage-start.v7');
  await preserved(x);
});

for(const boundary of ['cold-rejected','before-start'])test(`confirmed ${boundary} before candidate START retains guarded same-data recovery without legacy conversion`,async t=>{
  const x=await scenario(t,boundary);
  await assert.rejects(x.f.run.promote(),error=>error.code===(boundary==='cold-rejected'?'cold_verification_rejected':'operation_failed'));
  assert.equal(x.f.run.state.phase,'restored');assert.equal(x.f.map.get(args.originalId).State.Running,true);
  assert.deepEqual(x.applicationStarts,[args.originalId]);assert.ok(!x.delegated.includes('rollback'));
  assert.equal(x.f.run.state.applicationStart.storageGuard.schema,'soty.storage-start.v7');await preserved(x);
});

test('pre-STOP failure does not stop/restart the original or convert its unknown data',async t=>{
  const x=await scenario(t,'precheck');await assert.rejects(x.f.run.promote(),error=>error.code==='precheck_not_quiescent');
  assert.equal(x.f.run.state.phase,'aborted');assert.equal(x.f.map.get(args.originalId).State.Running,true);
  assert.deepEqual(x.applicationStarts,[]);assert.deepEqual(x.delegated,['status']);await preserved(x);
});

test('leave admission ambiguity never delegates rollback or automatically starts the original',async t=>{
  const x=await scenario(t,'admission');await assert.rejects(x.f.run.promote(),error=>error.code==='leave_boundary_ambiguous');
  assert.equal(x.f.run.state.phase,'recovery_required');assert.equal(x.f.map.get(args.originalId).State.Running,false);
  assert.equal(x.f.map.get(x.f.run.candidate.Id).State.Running,true);assert.deepEqual(x.applicationStarts,[x.f.run.candidate.Id]);
  assert.ok(!x.delegated.includes('rollback'));await preserved(x);
});
