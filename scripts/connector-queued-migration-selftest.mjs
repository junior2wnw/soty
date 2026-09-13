import assert from 'node:assert/strict';
import test from 'node:test';
import {mkdtemp,mkdir,writeFile,readFile,rm} from 'node:fs/promises';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {createConnectorStore} from '../server/connector-store.js';
import {readConnectorState} from '../server/connector-registry.js';
import {connectorMaintenance,maintenanceStatus,queuedFingerprint} from '../server/connector-maintenance.js';

async function fixture(fn) {
  const root=await mkdtemp(path.join(tmpdir(),'soty-pending-proof-'));
  const stores=[];
  const make=(dir)=>{const value=createConnectorStore(dir);stores.push(value);return value;};
  try {
    const auth={linkId:'q'.repeat(43),deviceId:'synthetic-pending',connectorId:'fixture:user',token:'t'.repeat(48)};
    const seed=make(path.join(root,'seed'));
    await seed.register({...auth,scope:'CurrentUser',capabilities:['command','script'],agent:{id:'opencode',provider:'gonka',available:true}},auth.token);
    for(let i=0;i<2;i++)assert.equal((await seed.createJob({linkId:auth.linkId,deviceId:auth.deviceId,kind:'command',text:`echo synthetic-${i}`,threadId:`unrelated-${i}`})).ok,true);
    const state=await readConnectorState(path.join(root,'seed'));
    await seed.close();
    const dir=path.join(root,'legacy');await mkdir(dir);
    await writeFile(path.join(dir,'connector-store.json'),JSON.stringify(state));
    await fn({dir,state,make,auth});
  } finally {
    for(const store of stores)await store.close();
    assert.equal(path.dirname(root),path.resolve(tmpdir()));
    assert.match(path.basename(root),/^soty-pending-proof-/);
    await rm(root,{recursive:true,force:true});
  }
}

test('reviewed never-leased jobs survive migration, maintenance and legacy rollback exactly',()=>fixture(async({dir,state,make,auth})=>{
  const before=await maintenanceStatus(dir);
  assert.equal(before.count,2);assert.match(before.queuedSha256,/^[a-f0-9]{64}$/);
  await assert.rejects(connectorMaintenance(dir,'enter'),/exact reviewed/);
  const options={preserveQueuedSha256:before.queuedSha256};
  await connectorMaintenance(dir,'enter',options);
  const migrated=make(dir);await migrated.ready;
  assert.deepEqual((await readConnectorState(dir)).jobs,state.jobs);
  assert.deepEqual((await migrated.poll(auth)).jobs,[]);
  assert.equal((await maintenanceStatus(dir)).queuedSha256,before.queuedSha256);
  await migrated.close();
  await assert.rejects(connectorMaintenance(dir,'rollback'),/approval/);
  const receipt=await connectorMaintenance(dir,'rollback',options);
  assert.equal(receipt.count,2);assert.equal(receipt.queuedSha256,before.queuedSha256);
  assert.deepEqual(JSON.parse(await readFile(path.join(dir,'connector-store.json'),'utf8')).jobs,state.jobs);
  await connectorMaintenance(dir,'leave');
}));

test('lease ambiguity, content drift, new jobs and a mismatched review cannot pass queued approval',()=>fixture(async({dir,state})=>{
  const expected=queuedFingerprint(state.jobs);
  await assert.rejects(connectorMaintenance(dir,'enter',{preserveQueuedSha256:'0'.repeat(64)}),/exact reviewed/);
  for(const change of [j=>j.attempts=1,j=>j.connectorId='fixture:user',j=>j.status='leased',j=>j.events.push({type:'started',seq:2,at:Date.now(),text:'uncertain'})]){
    const changed=structuredClone(state.jobs);change(changed[0]);assert.equal(queuedFingerprint(changed),null);
  }
  const changed=structuredClone(state.jobs);changed[0].input.text='changed body';assert.notEqual(queuedFingerprint(changed),expected);
  assert.notEqual(queuedFingerprint(state.jobs.slice(0,1)),expected);
  await connectorMaintenance(dir,'enter',{preserveQueuedSha256:expected});
  state.jobs[0].input.text='late alteration';
  await writeFile(path.join(dir,'connector-store.json'),JSON.stringify(state));
  await assert.rejects(connectorMaintenance(dir,'rollback',{preserveQueuedSha256:expected}),/pending jobs/);
}));
