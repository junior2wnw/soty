#!/usr/bin/env node
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,readFile,writeFile,rm,realpath,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createConnectorStore} from '../server/connector-store.js';
import {readConnectorState} from '../server/connector-registry.js';
import {connectorMaintenance} from '../server/connector-maintenance.js';
import {snapshotConnectorAuthority} from '../server/connector-authority.js';

const root=await mkdtemp(path.join(await realpath(tmpdir()),'soty-stopped-boundary-'));
const stores=[];const results=[];
const make=(dir,options)=>{const s=createConnectorStore(dir,options);stores.push(s);return s;};
const linkId='b'.repeat(43),token='s'.repeat(48);
const auth={linkId,deviceId:'boundary-synthetic',connectorId:'boundary:user',token};
const registration={...auth,scope:'CurrentUser',capabilities:['command','script'],agent:{id:'opencode',provider:'gonka',available:true}};
const input={linkId,deviceId:auth.deviceId,kind:'command',text:'echo boundary-synthetic',threadId:'boundary-synthetic'};
const fixtureTime=Date.now()-9*86400000;
let fixture,assigned,queued;
async function test(name,fn){await fn();results.push({name,ok:true});}
async function legacy(name,value=fixture){const dir=path.join(root,name);await mkdir(dir);await writeFile(path.join(dir,'connector-store.json'),JSON.stringify(value));return dir;}
try {
  const seed=make(path.join(root,'seed'),{now:()=>fixtureTime});
  await seed.register(registration,token);
  await seed.createAccessGrant(linkId,{deviceId:auth.deviceId,controllerDeviceId:'controller-synthetic',capabilities:['status','events'],expiresInMs:60000});
  const job=await seed.createJob(input);assert.equal(job.ok,true);
  queued=await readConnectorState(path.join(root,'seed'));
  await seed.poll(auth);assigned=await readConnectorState(path.join(root,'seed'));
  await seed.appendEvent(auth,job.job.id,{type:'message',text:'synthetic full event'});
  await seed.finishJob(auth,job.job.id,{ok:true,exitCode:0,text:'synthetic native result',sessionId:'synthetic-session'});
  fixture=await readConnectorState(path.join(root,'seed'));await seed.close();

  await test('stopped snapshot binds exact legacy and full SQLite readback; all mutation classes remain frozen',async()=>{
    const dir=await legacy('freeze');
    await writeFile(path.join(dir,'connector-store.json.777.next'),'{partial synthetic unacknowledged');
    const before=await connectorMaintenance(dir,'snapshot');
    const entered=await connectorMaintenance(dir,'enter');assert.equal(entered.authority.stateSha256,before.authority.stateSha256);
    const s=make(dir);await s.ready;
    const calls=[()=>s.register(registration,token),()=>s.poll(auth),()=>s.createJob(input),()=>s.appendEvent(auth,job.job.id,{type:'message',text:'not accepted'}),()=>s.finishJob(auth,job.job.id,{ok:true,exitCode:0,text:'not accepted'}),()=>s.cancelJob(linkId,job.job.id),()=>s.createAccessGrant(linkId,{deviceId:auth.deviceId,controllerDeviceId:'controller-synthetic',capabilities:['status'],expiresInMs:60000}),()=>s.revokeAccessGrant(linkId,fixture.accessGrants[0].id)];
    for(const call of calls)assert.equal((await call()).error,'connector-maintenance');
    assert.equal((await s.getJob(linkId,job.job.id)).job.result.text,'synthetic native result');
    const verified=await connectorMaintenance(dir,'verify');
    assert.equal(verified.authority.kind,'sqlite');assert.equal(verified.authority.legacySha256,before.authority.sourceSha256);assert.equal(verified.authority.stateSha256,before.authority.stateSha256);
    await connectorMaintenance(dir,'leave');
    assert.equal((await s.register(registration,token)).ok,true);
    assert.notEqual((await snapshotConnectorAuthority(dir)).stateSha256,before.authority.stateSha256);
    await s.close();
  });
  await test('source bytes changed after enter cannot create or admit a candidate database',async()=>{
    const dir=await legacy('source-drift');await connectorMaintenance(dir,'enter');
    await writeFile(path.join(dir,'connector-store.json'),JSON.stringify(fixture,null,2));
    const s=make(dir);await assert.rejects(s.ready,/source changed/);await s.close();
    await assert.rejects(stat(path.join(dir,'connector-store.sqlite')),{code:'ENOENT'});
  });
  await test('every assigned or potentially previously executed legacy job is fenced without changing files',async()=>{
    for(const [name,value] of [['queued',queued],['leased',assigned],['running',{...assigned,jobs:assigned.jobs.map(j=>({...j,status:'running'}))}],['queued-after-legacy-expiry',{...queued,jobs:queued.jobs.map(j=>({...j,attempts:1}))}]]){
      const dir=await legacy(name,value),before=await readFile(path.join(dir,'connector-store.json'));
      assert.equal((await connectorMaintenance(dir,'snapshot')).count,1);
      await assert.rejects(connectorMaintenance(dir,'enter'),/no queued or assigned/);
      assert.deepEqual(await readFile(path.join(dir,'connector-store.json')),before);
      await assert.rejects(stat(path.join(dir,'connector-store.sqlite')),{code:'ENOENT'});
    }
  });
  await test('pre-admission rollback conserves full authority and retained next file',async()=>{
    const dir=await legacy('rollback');await writeFile(path.join(dir,'connector-store.json.888.next'),JSON.stringify(queued));
    const before=await connectorMaintenance(dir,'snapshot');await connectorMaintenance(dir,'enter');const s=make(dir);await s.ready;await s.close();
    await connectorMaintenance(dir,'rollback');const after=await connectorMaintenance(dir,'snapshot');
    assert.equal(after.authority.stateSha256,before.authority.stateSha256);assert.deepEqual(after.authority.temporaryFiles,before.authority.temporaryFiles);
    await connectorMaintenance(dir,'leave');
  });
  await test('changed native result fails verification and rollback before deleting any database',async()=>{
    const dir=await legacy('damaged-result');await connectorMaintenance(dir,'enter');const s=make(dir);await s.ready;await s.close();
    const db=new DatabaseSync(path.join(dir,'connector-store.sqlite'));
    db.prepare('UPDATE results SET value=? WHERE id=?').run(JSON.stringify({...fixture.jobs[0].result,text:'synthetic changed result'}),job.job.id);db.close();
    await assert.rejects(connectorMaintenance(dir,'verify'),/authority changed/);
    await assert.rejects(connectorMaintenance(dir,'rollback'),/complete state differs/);
    assert.ok((await stat(path.join(dir,'connector-store.sqlite'))).isFile());
    assert.equal(JSON.parse(await readFile(path.join(dir,'connector-store.json'),'utf8')).schema,'soty.connector-store.sqlite.v1');
    await assert.rejects(stat(path.join(dir,'connector-rollback.json')),{code:'ENOENT'});
  });
  console.log(JSON.stringify({ok:true,synthetic:true,tests:results},null,2));
} finally {
  for(const s of stores)await s.close().catch(()=>{});
  // mkdtemp is the sole cleanup target, never a product or user-data directory.
  assert.equal(path.dirname(root),await realpath(tmpdir()));
  await rm(root,{recursive:true,force:true});
}
