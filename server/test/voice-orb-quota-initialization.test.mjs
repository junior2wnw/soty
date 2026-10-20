import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, basename, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Worker } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';

const root=process.env.SOTY_VOICE_QUOTA_INIT_ROOT??fileURLToPath(new URL('../..',import.meta.url));
const loaderUrl=process.env.SOTY_VOICE_QUOTA_INIT_WORKER_LOADER??null;
const moduleUrl=pathToFileURL(join(root,'server/voice-orb-quota.js')).href;
const {createVoiceOrbQuota}=await import(moduleUrl);
const sourceDigest=createHash('sha256').update(createVoiceOrbQuota.toString()).digest('hex');
const seedActor={accountId:'synthetic_init_seed_account',deviceId:'synthetic_init_seed_device'};
const service=maxStarts=>({tenantId:'synthetic_init_operator',serviceId:'synthetic_init_service',maxStarts,maxAccountStarts:1,expiresAt:Date.parse('2099-01-01T00:00:00.000Z')});
const input=(actor,requestId)=>({actor,requestId,intentDigest:'c'.repeat(64),maxDurationMs:15000,signal:new AbortController().signal});
const readRows=filename=>{const db=new DatabaseSync(filename,{readOnly:true});try{return db.prepare('SELECT * FROM voice_orb_starts ORDER BY request_key').all();}finally{db.close();}};
async function resources(t){
  const directory=await mkdtemp(join(tmpdir(),'soty-quota-init-')),early=[],stoppers=[];
  t.after(async()=>{
    const first=await Promise.allSettled(early.map(stop=>Promise.resolve().then(stop)));
    const last=await Promise.allSettled(stoppers.reverse().map(stop=>Promise.resolve().then(stop)));
    const absolute=await realpath(directory);assert.equal(dirname(absolute),await realpath(tmpdir()));assert.ok(basename(absolute).startsWith('soty-quota-init-'));
    await rm(absolute,{recursive:true,force:true,maxRetries:5,retryDelay:100});
    for(const result of [...first,...last])if(result.status==='rejected')throw result.reason;
  });
  return{databasePath:join(directory,'quota.sqlite'),early,stoppers};
}
async function seed(f,entry){
  const quota=createVoiceOrbQuota({databasePath:f.databasePath,service:entry});
  try{const lease=await quota.admit(input(seedActor,'synthetic_seed_before_concurrent_init'));assert.equal(await lease.claimTransport(),true);
    await lease.recordOutcome({transportAttempted:true,providerCallId:'rtc_synthetic_init_seed',hangupAccepted:false,responseUsage:[],reason:'seed_unknown'});
  }finally{quota.close();}
  const rows=readRows(f.databasePath);assert.equal(rows.length,1);return rows[0];
}
function message(worker,kind){
  return new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>finish(Error('owned_init_worker_timeout')),7000);
    const receive=value=>{if(value.kind==='fatal')finish(Error(JSON.stringify(value)));else if(value.kind===kind)finish(null,value);};
    const error=()=>finish(Error('owned_init_worker_error'));
    const exit=()=>finish(Error('owned_init_worker_early_exit'));
    function finish(failure,value){clearTimeout(timer);worker.off('message',receive);worker.off('error',error);worker.off('exit',exit);failure?reject(failure):resolve(value);}
    worker.on('message',receive);worker.once('error',error);worker.once('exit',exit);
  });
}
const workerCode=`const{parentPort,workerData}=require('node:worker_threads');
const{DatabaseSync}=require('node:sqlite');
const safe=error=>({code:typeof error?.code==='string'&&/^(?:voice_orb_[a-z_]+|ERR_SQLITE_[A-Z_]+)$/.test(error.code)?error.code
  :['worker_source_mismatch','owned_init_gate_timeout'].includes(error?.message)?error.message:'owned_init_worker_failed',
  sqliteErrcode:Number.isSafeInteger(error?.errcode)?error.errcode:null});
(async()=>{
  if(workerData.loaderUrl){const{register}=require('node:module');register(workerData.loaderUrl);}
  const{createVoiceOrbQuota}=await import(workerData.moduleUrl);
  const digest=require('node:crypto').createHash('sha256').update(createVoiceOrbQuota.toString()).digest('hex');
  if(digest!==workerData.sourceDigest)throw Error('worker_source_mismatch');
  parentPort.postMessage({kind:'ready',sourceDigest:digest});
  parentPort.once('message',async()=>{
    let quota,result,stage='construct',journalAttempts=0,closeCalls=0,observed=false,busyInstalled=false;
    const begun=require('node:perf_hooks').performance.now(),originalExec=DatabaseSync.prototype.exec,originalClose=DatabaseSync.prototype.close;
    try{
      if(workerData.gate){parentPort.postMessage({kind:'at-start-gate'});
        if(Atomics.wait(new Int32Array(workerData.gate),0,0,5000)==='timed-out')throw Error('owned_init_gate_timeout');}
      DatabaseSync.prototype.close=function(...args){const value=Reflect.apply(originalClose,this,args);closeCalls++;return value;};
      DatabaseSync.prototype.exec=function(sql){
        if(typeof sql==='string'&&sql.startsWith('PRAGMA busy_timeout='))busyInstalled=true;
        if(typeof sql==='string'&&sql.includes('PRAGMA journal_mode=WAL')){
          journalAttempts++;
          if(!observed&&workerData.observeJournal){observed=true;parentPort.postMessage({kind:'journal-enter',
            busyBeforeJournal:busyInstalled});}
          if(workerData.journalFault==='nonbusy')return Reflect.apply(originalExec,this,['SELECT * FROM synthetic_missing_initializer_table']);
          if(workerData.journalFault==='busy-recovery-once'&&journalAttempts===1)
            throw Object.assign(new Error('synthetic_busy_recovery_boundary'),{code:'ERR_SQLITE_ERROR',errcode:261});
        }
        return Reflect.apply(originalExec,this,[sql]);};
      quota=createVoiceOrbQuota({databasePath:workerData.databasePath,service:workerData.service});stage='admit';
      const lease=await quota.admit({...workerData.intent,signal:new AbortController().signal});const claimed=await lease.claimTransport();
      result={kind:'result',ok:true,claimed};
    }catch(error){result={kind:'result',ok:false,stage,...safe(error)};}
    finally{DatabaseSync.prototype.exec=originalExec;try{quota?.close();}catch(error){result={kind:'result',ok:false,stage:'close',...safe(error)};}
      DatabaseSync.prototype.close=originalClose;
      parentPort.postMessage({...result,journalAttempts,closeCalls,elapsedMs:require('node:perf_hooks').performance.now()-begun});parentPort.close();}
  });
})().catch(error=>{parentPort.postMessage({kind:'fatal',...safe(error)});parentPort.close();});`;
function writer(f,entry,index,{gate,observeJournal=false,journalFault}={}){
  const actor={accountId:'synthetic_init_account_'+index,deviceId:'synthetic_init_device_'+index};
  const intent=input(actor,'synthetic_init_request_'+index);delete intent.signal;
  const worker=new Worker(workerCode,{eval:true,workerData:{moduleUrl,loaderUrl,sourceDigest,databasePath:f.databasePath,service:entry,intent,gate,observeJournal,journalFault}});
  const exit=new Promise(resolve=>worker.once('exit',resolve));
  f.stoppers.push(()=>worker.terminate());
  if(gate)f.early.push(()=>{Atomics.store(new Int32Array(gate),0,1);Atomics.notify(new Int32Array(gate),0);});
  return{worker,exit};
}

test('constructor uses its existing busy budget for a real held WAL transition and retains the durable quota row',{timeout:15000},async t=>{
  const f=await resources(t),entry=service(2),original=await seed(f,entry);
  const lock=new DatabaseSync(f.databasePath);let held=false,releaseTimer;
  const release=()=>{clearTimeout(releaseTimer);if(held){held=false;lock.exec('ROLLBACK');}};
  f.early.push(release);f.stoppers.push(()=>lock.close());
  lock.exec('PRAGMA journal_mode=DELETE');assert.equal(lock.prepare('PRAGMA journal_mode').get().journal_mode,'delete');
  lock.exec('BEGIN IMMEDIATE');held=true;
  const{worker,exit}=writer(f,entry,'held_lock',{observeJournal:true});await message(worker,'ready');
  const entered=message(worker,'journal-enter'),result=message(worker,'result');worker.postMessage('construct');
  const observation=await Promise.race([entered,result.then(outcome=>{throw Error('worker_completed_before_wal_observation:'+JSON.stringify(outcome));})]);
  const released=new Promise((resolve,reject)=>{releaseTimer=setTimeout(()=>{try{release();resolve();}catch{reject(Error('owned_lock_release_failed'));}},250);});
  const outcome=await result;await released;assert.equal(await exit,0);
  assert.equal(outcome.ok,true,JSON.stringify({observation,outcome}));assert.equal(outcome.claimed,true);
  const rows=readRows(f.databasePath);assert.equal(rows.length,2);assert.deepEqual(rows.find(row=>row.request_key===original.request_key),original);
});

test('eight simultaneous two-writer initializations keep one durable counter across all opens without reset',{timeout:30000},async t=>{
  const f=await resources(t),entry=service(17),original=await seed(f,entry);
  for(let round=0;round<8;round++){
    const gate=new SharedArrayBuffer(4),writers=[writer(f,entry,round+'_a',{gate}),writer(f,entry,round+'_b',{gate})];
    await Promise.all(writers.map(({worker})=>message(worker,'ready')));
    const atGate=writers.map(({worker})=>message(worker,'at-start-gate'));
    const results=writers.map(({worker})=>message(worker,'result'));
    writers.forEach(({worker})=>worker.postMessage('construct'));await Promise.all(atGate);
    Atomics.store(new Int32Array(gate),0,1);Atomics.notify(new Int32Array(gate),0);
    const outcomes=await Promise.all(results);assert.deepEqual(await Promise.all(writers.map(value=>value.exit)),[0,0]);
    assert.equal(outcomes.every(value=>value.ok&&value.claimed),true,JSON.stringify({round,outcomes}));
    const rows=readRows(f.databasePath);assert.equal(rows.length,1+(round+1)*2);assert.deepEqual(rows.find(row=>row.request_key===original.request_key),original);
  }
  const quota=createVoiceOrbQuota({databasePath:f.databasePath,service:entry});try{assert.equal(quota.statusFor(seedActor).remainingStarts,0);}finally{quota.close();}
});

test('a write lock held beyond the one startup budget fails closed, closes the actual DB and leaves the seed untouched',{timeout:15000},async t=>{
  const f=await resources(t),entry=service(2),original=await seed(f,entry),lock=new DatabaseSync(f.databasePath);
  let held=false;const release=()=>{if(held){held=false;lock.exec('ROLLBACK');}};
  f.early.push(release);f.stoppers.push(()=>lock.close());
  lock.exec('PRAGMA journal_mode=DELETE; BEGIN IMMEDIATE');held=true;
  const{worker,exit}=writer(f,entry,'over_budget');await message(worker,'ready');
  const completed=message(worker,'result');worker.postMessage('construct');const outcome=await completed;
  assert.equal(await exit,0);release();
  assert.deepEqual({ok:outcome.ok,stage:outcome.stage,code:outcome.code,closeCalls:outcome.closeCalls},
    {ok:false,stage:'construct',code:'ERR_SQLITE_ERROR',closeCalls:1});
  assert.ok(Number.isSafeInteger(outcome.sqliteErrcode),JSON.stringify(outcome));assert.equal(outcome.sqliteErrcode&255,5);
  assert.ok(outcome.elapsedMs>=2500&&outcome.elapsedMs<=3500,JSON.stringify({elapsedMs:outcome.elapsedMs,journalAttempts:outcome.journalAttempts}));
  assert.deepEqual(readRows(f.databasePath),[original]);
});

test('a genuine non-BUSY SQLite journal setup error is not retried and closes its actual DB',{timeout:10000},async t=>{
  const f=await resources(t),entry=service(2),original=await seed(f,entry),{worker,exit}=writer(f,entry,'nonbusy',{journalFault:'nonbusy'});
  await message(worker,'ready');const completed=message(worker,'result');worker.postMessage('construct');const outcome=await completed;
  assert.equal(await exit,0);
  assert.deepEqual({ok:outcome.ok,stage:outcome.stage,code:outcome.code,sqliteErrcode:outcome.sqliteErrcode,journalAttempts:outcome.journalAttempts,closeCalls:outcome.closeCalls},
    {ok:false,stage:'construct',code:'ERR_SQLITE_ERROR',sqliteErrcode:1,journalAttempts:1,closeCalls:1});
  assert.deepEqual(readRows(f.databasePath),[original]);
});

test('an injected SQLite BUSY_RECOVERY boundary is retried before genuine journal setup and one real reservation',{timeout:10000},async t=>{
  const f=await resources(t),entry=service(2),original=await seed(f,entry),{worker,exit}=writer(f,entry,'recovery_once',{journalFault:'busy-recovery-once'});
  await message(worker,'ready');const completed=message(worker,'result');worker.postMessage('construct');const outcome=await completed;
  assert.equal(await exit,0);
  assert.deepEqual({ok:outcome.ok,claimed:outcome.claimed,journalAttempts:outcome.journalAttempts,closeCalls:outcome.closeCalls},
    {ok:true,claimed:true,journalAttempts:2,closeCalls:1});
  const rows=readRows(f.databasePath);assert.equal(rows.length,2);assert.deepEqual(rows.find(row=>row.request_key===original.request_key),original);
});
