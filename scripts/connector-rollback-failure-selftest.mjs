#!/usr/bin/env node
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {DatabaseSync} from 'node:sqlite';
import {createConnectorStore} from '../server/connector-store.js';
const auth={linkId:'r'.repeat(43),deviceId:'synthetic',connectorId:'synthetic:user',token:'r'.repeat(48)};
const registration={...auth,scope:'CurrentUser',capabilities:['command'],agent:{id:'opencode',provider:'gonka',available:true}};
const input={linkId:auth.linkId,deviceId:auth.deviceId,kind:'command',text:'echo synthetic',requestId:'failed-rollback'};
if(process.argv[2]==='--child'){
  const dir=process.argv[3],store=createConnectorStore(dir);let db;
  try{
    await store.register(registration,auth.token);
    db=new DatabaseSync(path.join(dir,'connector-store.sqlite'));
    db.exec("CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END;");
    await assert.rejects(store.createJob(input),{code:'STORE_COMMIT_AMBIGUOUS'});
    await assert.rejects(store.status(auth.linkId),{code:'STORE_COMMIT_AMBIGUOUS'});
    await assert.rejects(store.createJob({...input,requestId:'next'}),{code:'STORE_COMMIT_AMBIGUOUS'});
    assert.equal(Number(db.prepare('SELECT count(*) AS n FROM jobs').get().n),0);
    assert.equal(Number(db.prepare('SELECT count(*) AS n FROM requests').get().n),0);
    console.log(JSON.stringify({ok:true,readsFenced:true,writesFenced:true,committedJobs:0,committedRequests:0}));
  }finally{db?.close();await store.close();}
}else{
  const dir=await mkdtemp(path.join(tmpdir(),'soty-failed-rollback-'));
  let reopened;
  try{
    const child=await promisify(execFile)(process.execPath,['--import',new URL('./fixtures/connector-rollback-failure-preload.mjs',import.meta.url).href,fileURLToPath(import.meta.url),'--child',dir],{windowsHide:true,timeout:15000,maxBuffer:65536});
    const result=JSON.parse(child.stdout);assert.equal(result.ok,true);
    // Closing the isolated failed worker releases its transaction and owner.
    // A fresh process/connection observes durable state and may recover after
    // the synthetic engine fault is removed; no identity was acknowledged.
    const db=new DatabaseSync(path.join(dir,'connector-store.sqlite'));db.exec('DROP TRIGGER fail_event');db.close();
    reopened=createConnectorStore(dir);await reopened.ready;assert.equal(reopened.state.jobs.length,0);assert.equal(reopened.state.requests.length,0);
    const accepted=await reopened.createJob(input);assert.equal(accepted.ok,true);assert.equal((await reopened.createJob(input)).job.id,accepted.job.id);
    console.log(JSON.stringify({ok:true,node:process.version,combinedWriteAndRollbackFailure:result,reopenedRecovery:true,synthetic:true}));
  }finally{await reopened?.close();await rm(dir,{recursive:true,force:true});}
}
