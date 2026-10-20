import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, basename, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Worker } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';

const root = process.env.SOTY_VOICE_QUOTA_RACE_ROOT ?? fileURLToPath(new URL('../..',import.meta.url));
const workerLoaderUrl = process.env.SOTY_VOICE_QUOTA_RACE_WORKER_LOADER ?? null;
assert.equal(typeof root,'string','Source root must be a path');
const moduleUrl = pathToFileURL(join(root,'server/voice-orb-quota.js')).href;
const {createVoiceOrbQuota} = await import(moduleUrl);
const {canonical} = await import(pathToFileURL(join(root,'modules/personal-agent/provider/validation.mjs')).href);
const sourceDigest = createHash('sha256').update(createVoiceOrbQuota.toString()).digest('hex');
const actor = {accountId:'synthetic_quota_outcome_account',deviceId:'synthetic_quota_outcome_device'};
const service = {tenantId:'synthetic_quota_operator',serviceId:'synthetic_quota_service',authorizationId:'synthetic_quota_authorization',
  maxStarts:1,maxAccountStarts:1,expiresAt:Date.parse('2099-01-01T00:00:00.000Z')};
const intent = () => ({actor,requestId:'synthetic_same_original_request',intentDigest:'a'.repeat(64),maxDurationMs:15000,signal:new AbortController().signal});
const noAttempt = {transportAttempted:false,providerCallId:null,hangupAccepted:false,responseUsage:[],reason:'cancelled_before_attempt'};
const attempted = {transportAttempted:true,providerCallId:'rtc_synthetic_quota_winner',hangupAccepted:false,responseUsage:[],reason:'attempt_unknown'};
const row = filename => {
  const db=new DatabaseSync(filename,{readOnly:true});
  try { const rows=db.prepare('SELECT attempted,outcome_json,outcome_digest FROM voice_orb_starts').all();assert.equal(rows.length,1);return rows[0]; }
  finally { db.close(); }
};
async function files(t) {
  const directory=await mkdtemp(join(tmpdir(),'soty-quota-outcome-')),stoppers=[];
  t.after(async()=>{
    const results=await Promise.allSettled(stoppers.reverse().map(stop=>Promise.resolve().then(stop)));
    const absolute=await realpath(directory);assert.equal(dirname(absolute),await realpath(tmpdir()));assert.ok(basename(absolute).startsWith('soty-quota-outcome-'));
    await rm(absolute,{recursive:true,force:true,maxRetries:5,retryDelay:100});
    for(const result of results)if(result.status==='rejected')throw result.reason;
  });
  return {databasePath:join(directory,'quota.sqlite'),stoppers};
}
function next(worker,kind) {
  return new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>finish(Error('owned_quota_worker_timeout')),6000);
    const message=value=>{
      if(value.kind==='fatal')finish(Error(value.code));
      else if(value.kind===kind)finish(null,value);
      else finish(Error('owned_quota_worker_protocol'));
    };
    const error=()=>finish(Error('owned_quota_worker_error'));
    const exit=()=>finish(Error('owned_quota_worker_early_exit'));
    function finish(failure,value){clearTimeout(timer);worker.off('message',message);worker.off('error',error);worker.off('exit',exit);failure?reject(failure):resolve(value);}
    worker.on('message',message);worker.once('error',error);worker.once('exit',exit);
  });
}
const workerCode = `const {parentPort,workerData}=require('node:worker_threads');
const {DatabaseSync}=require('node:sqlite');
const {register}=require('node:module');
(async()=>{
  if(workerData.loaderUrl)register(workerData.loaderUrl);
  const {createVoiceOrbQuota}=await import(workerData.moduleUrl);
  const digest=require('node:crypto').createHash('sha256').update(createVoiceOrbQuota.toString()).digest('hex');
  if(digest!==workerData.sourceDigest)throw Error('worker_source_mismatch');
  const quota=createVoiceOrbQuota({databasePath:workerData.databasePath,service:workerData.service});
  const lease=await quota.admit({...workerData.intent,signal:new AbortController().signal});
  parentPort.postMessage({kind:'admitted',sourceDigest:digest});
  parentPort.once('message',async command=>{
    const originalPrepare=DatabaseSync.prototype.prepare;
    try{
      if(command==='hold-record'){
        let armed=true;
        DatabaseSync.prototype.prepare=function(sql,...args){
          const statement=Reflect.apply(originalPrepare,this,[sql,...args]);
          if(!armed||sql!=='SELECT attempted,outcome_digest FROM voice_orb_starts WHERE request_key=?')return statement;
          return new Proxy(statement,{get(target,name){
            if(name==='get')return(...values)=>{
              const captured=target.get(...values);
              if(armed){armed=false;parentPort.postMessage({kind:'read-held'});
                const result=Atomics.wait(new Int32Array(workerData.gate),0,0,5000);
                if(result==='timed-out')throw Error('owned_quota_gate_timeout');}
              return captured;
            };
            const value=Reflect.get(target,name,target);return typeof value==='function'?value.bind(target):value;
          }});
        };
        try{await lease.recordOutcome(workerData.noAttempt);parentPort.postMessage({kind:'recorded',accepted:true});}
        catch(error){parentPort.postMessage({kind:'recorded',accepted:false,code:error.code??'worker_record_failed'});}
      }else if(command==='claim-after-outcome'){
        const claimed=await lease.claimTransport();let outcomeAccepted=true,code=null;
        try{await lease.recordOutcome(workerData.noAttempt);}catch(error){outcomeAccepted=false;code=error.code??'worker_record_failed';}
        parentPort.postMessage({kind:'claim-result',claimed,outcomeAccepted,code});
      }else throw Error('owned_quota_worker_command');
    }catch{parentPort.postMessage({kind:'fatal',code:'owned_quota_worker_failed'});}
    finally{DatabaseSync.prototype.prepare=originalPrepare;quota.close();parentPort.close();}
  });
})().catch(()=>{parentPort.postMessage({kind:'fatal',code:'owned_quota_worker_setup_failed'});parentPort.close();});`;
function writer(t,f,gate) {
  const request=intent();delete request.signal;
  const worker=new Worker(workerCode,{eval:true,workerData:{moduleUrl,sourceDigest,loaderUrl:workerLoaderUrl,
    databasePath:f.databasePath,service,intent:request,noAttempt,gate}});
  const exit=new Promise(resolve=>worker.once('exit',resolve));
  f.stoppers.push(async()=>{if(gate){Atomics.store(new Int32Array(gate),0,1);Atomics.notify(new Int32Array(gate),0);}await worker.terminate();});
  return {worker,exit};
}

test('two real SQLite writers cannot store no-attempt after the other lease wins transport between SELECT and UPDATE',{timeout:15000},async t=>{
  const f=await files(t),gate=new SharedArrayBuffer(4);
  createVoiceOrbQuota({databasePath:f.databasePath,service}).close();
  const {worker:first,exit:firstExit}=writer(t,f,gate);await next(first,'admitted');
  const second=createVoiceOrbQuota({databasePath:f.databasePath,service});f.stoppers.push(()=>second.close());
  const replay=await second.admit(intent());
  const held=next(first,'read-held');first.postMessage('hold-record');await held;
  const completed=next(first,'recorded');
  assert.equal(await replay.claimTransport(),true);
  Atomics.store(new Int32Array(gate),0,1);Atomics.notify(new Int32Array(gate),0);
  const outcome=await completed;assert.equal(await firstExit,0);const observed=row(f.databasePath);
  assert.deepEqual({accepted:outcome.accepted,code:outcome.code,attempted:observed.attempted,outcome:observed.outcome_json},
    {accepted:false,code:'voice_orb_denied',attempted:1,outcome:null});
  await replay.recordOutcome(attempted);await replay.recordOutcome(attempted);
  const committed=row(f.databasePath);assert.equal(committed.attempted,1);assert.equal(JSON.parse(committed.outcome_json).transportAttempted,true);
  assert.equal(second.statusFor(actor).remainingStarts,0);
});

test('a committed no-attempt outcome prevents a second real writer claiming or owning its evidence',{timeout:15000},async t=>{
  const f=await files(t),first=createVoiceOrbQuota({databasePath:f.databasePath,service});f.stoppers.push(()=>first.close());
  const original=await first.admit(intent()),{worker:second,exit:secondExit}=writer(t,f);await next(second,'admitted');
  await original.recordOutcome(noAttempt);await original.recordOutcome(noAttempt);
  const originalRow=row(f.databasePath),result=next(second,'claim-result');second.postMessage('claim-after-outcome');
  assert.deepEqual(await result,{kind:'claim-result',claimed:false,outcomeAccepted:false,code:'voice_orb_denied'});
  assert.equal(await secondExit,0);
  assert.deepEqual(row(f.databasePath),originalRow);assert.equal(originalRow.attempted,0);
  assert.equal(first.statusFor(actor).remainingStarts,0);
});

test('V1 owner proof remains idempotent and rejects changed outcome or a replay owner',{timeout:10000},async t=>{
  const f=await files(t),profile={...actor,tenantId:'synthetic_v1_tenant',authorizationId:'synthetic_v1_authorization',maxStarts:2,expiresAt:service.expiresAt};
  const profiles=new Map([[canonical(actor),profile]]),first=createVoiceOrbQuota({databasePath:f.databasePath,profiles});f.stoppers.push(()=>first.close());
  const owner=await first.admit(intent());assert.equal(await owner.claimTransport(),true);
  await owner.recordOutcome(attempted);await owner.recordOutcome(attempted);const originalRow=row(f.databasePath);
  await assert.rejects(owner.recordOutcome({...attempted,reason:'different_exact_outcome'}),error=>error.code==='voice_orb_denied');
  const second=createVoiceOrbQuota({databasePath:f.databasePath,profiles});f.stoppers.push(()=>second.close());
  const replay=await second.admit(intent());assert.equal(await replay.claimTransport(),false);
  await assert.rejects(replay.recordOutcome(attempted),error=>error.code==='voice_orb_denied');
  assert.deepEqual(row(f.databasePath),originalRow);assert.equal(first.statusFor(actor).reservedStarts,1);
});
