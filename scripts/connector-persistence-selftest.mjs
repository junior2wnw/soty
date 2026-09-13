#!/usr/bin/env node
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, writeFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { performance } from "node:perf_hooks";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { createConnectorStore } from "../server/connector-store.js";
import { readConnectorState, readConnectorRegistry } from "../server/connector-registry.js";
import { connectorMaintenance } from "../server/connector-maintenance.js";

const root = await mkdtemp(path.join(tmpdir(),"soty-durable-faults-"));
const stores = [];
const make = (dir, options) => { const store = createConnectorStore(dir,options); stores.push(store); return store; };
const linkId = "p".repeat(43), token = "f".repeat(48);
const auth = { linkId, deviceId: "synthetic", connectorId: "fixture:user", token };
const registration = { ...auth, scope: "CurrentUser", capabilities: ["command","script"], agent: { id:"opencode",provider:"gonka",available:true } };
const input = { linkId,deviceId:auth.deviceId,kind:"command",text:"echo synthetic",threadId:"durability" };
const results = [];
async function test(name, fn) { const start = performance.now(); await fn(); results.push({ name,ok:true,ms:Math.round(performance.now()-start) }); }
let fixture;

try {
  await test("failed SQLite transaction rolls back memory, identity, events and notification", async () => {
    const dir = path.join(root,"fault");
    const store = make(dir);
    await store.register(registration,token);
    const before = await readConnectorState(dir);
    let notifications = 0;
    store.events.on(`${linkId}\u0000${auth.deviceId}`,()=>notifications++);
    // Real engine failure after earlier job/input rows were written, before
    // the transaction commits. No mock of successful persistence is involved.
    const db = new DatabaseSync(path.join(dir,"connector-store.sqlite"));
    db.exec("CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END;");
    await assert.rejects(store.createJob({ ...input,requestId:"disk-failure" }), /synthetic disk failure/u);
    assert.equal(notifications,0);
    assert.deepEqual(await readConnectorState(dir),before);
    assert.equal(store.state.jobs.length,0);
    db.exec("DROP TRIGGER fail_event;");db.close();
    const accepted = await store.createJob({ ...input,requestId:"disk-failure" });
    assert.equal(accepted.ok,true);
    await store.close();
    const restored = make(dir);
    const replay = await restored.createJob({ ...input,requestId:"disk-failure" });
    assert.equal(replay.job.id,accepted.job.id);
    assert.equal(restored.state.jobs.length,1);
    assert.equal(restored.state.requests.length,1);
  });

  await test("committed readers do not await a stalled transaction and no signal precedes commit", async () => {
    const dir = path.join(root,"readers"); const store = make(dir);
    await store.register(registration,token);
    const original = store.persist.bind(store);
    let release; const barrier = new Promise((resolve)=>{release=resolve;});
    let entered; const started = new Promise((resolve)=>{entered=resolve;});
    let notifications=0; const emit=store.events.emit.bind(store.events);
    store.events.emit=(...args)=>{notifications++;return emit(...args);};
    store.persist=async(delta)=>{entered();await barrier;return await original(delta);};
    let acknowledged=false;
    const pending=store.createJob({...input,requestId:"stalled"}).then((value)=>{acknowledged=true;return value;});
    await started;
    const start=performance.now(); assert.equal((await store.status(linkId)).ok,true);
    assert.ok(performance.now()-start<250);
    assert.equal(store.state.jobs.length,0); assert.equal(acknowledged,false); assert.equal(notifications,0);
    release(); const accepted=await pending;
    assert.equal(acknowledged,true);assert.ok(notifications>0);
    assert.equal((await readConnectorState(dir)).jobs[0].id,accepted.job.id);
  });

  await test("migration preserves complete history, native results, grants and bridge proof", async () => {
    const dir = path.join(root,"fixture"); const store=make(dir);
    await store.register(registration,token);
    await store.createAccessGrant(linkId,{deviceId:auth.deviceId,controllerDeviceId:"controller",capabilities:["status","events"],expiresInMs:60000});
    const created=await store.createJob({ ...input,kind:"script",input:{name:"laptop-corporate-bootstrap-ufn-corp-1.0.0.ps1",text:"bootstrap",script:"echo synthetic",runAs:"user"} });
    await store.poll(auth);
    await store.appendEvent(auth,created.job.id,{type:"message",text:"public synthetic ".repeat(2000)});
    await store.finishJob(auth,created.job.id,{ok:true,exitCode:0,text:"UFN-CORP-1.0.0\n"+"ж".repeat(75000),sessionId:"synthetic-session"});
    fixture=await readConnectorState(dir); await store.close();
    const target=path.join(root,"migration");await mkdir(target);
    const content=JSON.stringify(fixture,null,2);await writeFile(path.join(target,"connector-store.json"),content);
    const migrated=make(target);await migrated.ready;
    assert.deepEqual(await readConnectorState(target),fixture);
    const marker=JSON.parse(await readFile(path.join(target,"connector-store.json"),"utf8"));
    assert.equal(marker.schema,"soty.connector-store.sqlite.v1");
    assert.ok((await stat(path.join(target,"connector-store.json"))).size<512);
    const registry=await readConnectorRegistry(target,{jobId:created.job.id});
    assert.equal(registry.routeJob.id,created.job.id);assert.equal(registry.routeJob.result.text,fixture.jobs[0].result.text);
    assert.equal(registry.jobs[0].result.truncated,true);assert.equal(registry.jobs[0].result.text.length,65536);
    assert.equal("script" in registry.jobs[0].input,false);assert.equal("tokenHash" in registry.connectors[0],false);
    assert.equal(JSON.stringify(registry).includes(token),false);
    await migrated.close();
    assert.equal((await connectorMaintenance(target,"enter")).maintenance,true);
    const rollback=await connectorMaintenance(target,"rollback");assert.equal(rollback.rollback,"legacy-json");
    assert.deepEqual(JSON.parse(await readFile(path.join(target,"connector-store.json"),"utf8")),fixture);
    await connectorMaintenance(target,"leave");
    const again=make(target);await again.ready;assert.deepEqual(await readConnectorState(target),fixture);
  });

  await test("malformed, partial and inaccessible legacy stores remain unchanged and fail closed", async () => {
    for (const [index,content] of ["{",JSON.stringify({schema:"soty.connector-store.v2",connectors:[]}),JSON.stringify({...fixture,jobs:[{...fixture.jobs[0],events:[{seq:0}]}]}),JSON.stringify({...fixture,connectors:[{...fixture.connectors[0],tokenHash:"invalid"}]})].entries()) {
      const dir=path.join(root,`invalid-${index}`);await mkdir(dir);const file=path.join(dir,"connector-store.json");await writeFile(file,content);
      const store=make(dir);await assert.rejects(store.ready);await assert.rejects(store.register(registration,token));
      assert.equal(await readFile(file,"utf8"),content);await assert.rejects(stat(path.join(dir,"connector-store.sqlite")),{code:"ENOENT"});
    }
    const dir=path.join(root,"unreadable");await mkdir(path.join(dir,"connector-store.json"),{recursive:true});
    const store=make(dir);await assert.rejects(store.ready);assert.equal((await stat(path.join(dir,"connector-store.json"))).isDirectory(),true);
  });

  await test("missing, corrupt, divergent and partial migration databases never reset a valid registry", async () => {
    const dir=path.join(root,"diverged");await mkdir(dir);const content=JSON.stringify(fixture);await writeFile(path.join(dir,"connector-store.json"),content);
    const store=make(dir);await store.ready;await store.close();
    const marker=await readFile(path.join(dir,"connector-store.json"),"utf8");
    await writeFile(path.join(dir,"connector-store.json"),JSON.stringify({...fixture,jobs:[]}));
    const diverged=make(dir);await assert.rejects(diverged.ready,/diverged/u);await diverged.close();
    await writeFile(path.join(dir,"connector-store.json"),marker);
    await writeFile(path.join(dir,"connector-store.sqlite"),"broken database");
    const broken=make(dir);await assert.rejects(broken.ready);await broken.close();
    assert.equal(await readFile(path.join(dir,"connector-store.sqlite"),"utf8"),"broken database");
    assert.equal(await readFile(path.join(dir,"connector-store.json"),"utf8"),marker);
    const missing=path.join(root,"missing-db");await mkdir(missing);await writeFile(path.join(missing,"connector-store.json"),marker);
    await assert.rejects(make(missing).ready);assert.equal(await readFile(path.join(missing,"connector-store.json"),"utf8"),marker);
    const partial=path.join(root,"partial-db");await mkdir(partial);await writeFile(path.join(partial,"connector-store.json"),content);
    const empty=new DatabaseSync(path.join(partial,"connector-store.sqlite"));empty.close();
    await assert.rejects(make(partial).ready);assert.equal(await readFile(path.join(partial,"connector-store.json"),"utf8"),content);
  });

  await test("maintenance refuses active work and legacy downgrade after accepting request identities", async () => {
    const dir=path.join(root,"admission");const store=make(dir);await store.register(registration,token);
    const created=await store.createJob({...input,requestId:"preserve-id"});
    await assert.rejects(connectorMaintenance(dir,"enter"),/already has/u);
    await store.close();
    await assert.rejects(connectorMaintenance(dir,"enter"),/no queued or assigned/u);
    const resumed=make(dir);await resumed.cancelJob(linkId,created.job.id);
    await resumed.close();await connectorMaintenance(dir,"enter");
    const candidate=make(dir);await candidate.ready;
    assert.equal((await candidate.createJob(input)).error,"connector-maintenance");
    assert.deepEqual((await candidate.poll(auth)).jobs,[]);
    await candidate.close();
    await assert.rejects(connectorMaintenance(dir,"rollback"),/durable request identities/u);
    assert.equal((await readConnectorState(dir)).requests.length,1);
  });

  await test("second serving owner is fenced and revoked grants stay revoked after reopen", async () => {
    const dir=path.join(root,"owners");const a=make(dir);await a.register(registration,token);
    const issued=await a.createAccessGrant(linkId,{deviceId:auth.deviceId,controllerDeviceId:"controller",capabilities:["status"],expiresInMs:60000});
    const delegated={grantId:issued.grant.id,controllerDeviceId:"controller",token:issued.token};
    const b=make(dir);await assert.rejects(b.ready,/already has/u);
    await a.revokeAccessGrant(linkId,issued.grant.id);
    await assert.rejects(b.status(delegated));await b.close();await a.close();
    const c=make(dir);assert.equal((await c.status(delegated)).error,"connector-access-revoked");
  });

  await test("semantically incomplete SQLite rows cannot disappear or fabricate a native outcome", async () => {
    for(const table of ["inputs","results"]) {
      const dir=path.join(root,`missing-${table}`);await mkdir(dir);await writeFile(path.join(dir,"connector-store.json"),JSON.stringify(fixture));
      const original=make(dir);await original.ready;await original.close();
      const marker=await readFile(path.join(dir,"connector-store.json"),"utf8");
      const db=new DatabaseSync(path.join(dir,"connector-store.sqlite"));db.exec(`DELETE FROM ${table};`);
      assert.equal(db.prepare("PRAGMA quick_check").get().quick_check,"ok");db.close();
      const invalid=make(dir);await assert.rejects(invalid.ready);await invalid.close();
      assert.equal(await readFile(path.join(dir,"connector-store.json"),"utf8"),marker);
      await assert.rejects(readConnectorState(dir));
    }
  });

  await test("rollback crash windows have one explicit authority and resume preserves exact outcomes", async () => {
    for(const phase of ["intent","legacy","remove-wal","remove-shm","remove-database"]) {
      const dir=path.join(root,`rollback-${phase}`);await mkdir(dir);await writeFile(path.join(dir,"connector-store.json"),JSON.stringify(fixture));
      const original=make(dir);await original.ready;await original.close();await connectorMaintenance(dir,"enter");
      await assert.rejects(connectorMaintenance(dir,"rollback",{checkpoint:async(step)=>{if(step===phase)throw new Error("synthetic crash");}}),/synthetic crash/u);
      const premature=make(dir);await assert.rejects(premature.ready,/rollback is incomplete/u);await premature.close();
      await assert.rejects(readConnectorRegistry(dir),/rollback is incomplete/u);
      const completed=await connectorMaintenance(dir,"rollback");assert.equal(completed.rollback,"legacy-json");
      assert.deepEqual(JSON.parse(await readFile(path.join(dir,"connector-store.json"),"utf8")),fixture);
      await assert.rejects(stat(path.join(dir,"connector-store.sqlite")),{code:"ENOENT"});
      await assert.rejects(stat(path.join(dir,"connector-rollback.json")),{code:"ENOENT"});
      await connectorMaintenance(dir,"leave");
    }
  });

  await test("interrupted empty schema import rolls back without discarding the valid legacy source", async () => {
    const dir=path.join(root,"empty-import-recovery");await mkdir(dir);await writeFile(path.join(dir,"connector-store.json"),JSON.stringify(fixture));
    const empty=new DatabaseSync(path.join(dir,"connector-store.sqlite"));empty.close();
    await writeFile(path.join(dir,"connector-maintenance.json"),"{}");
    assert.equal((await connectorMaintenance(dir,"rollback")).rollback,"legacy-json");
    assert.deepEqual(JSON.parse(await readFile(path.join(dir,"connector-store.json"),"utf8")),fixture);
  });

  await test("bounded request ledger refuses new work instead of forgetting duplicate identities", async () => {
    const dir=path.join(root,"request-cap");const store=make(dir,{maxRequestRecords:2});await store.register(registration,token);
    const a=await store.createJob({...input,requestId:"one"});const b=await store.createJob({...input,requestId:"two"});
    assert.equal((await store.createJob({...input,requestId:"three"})).error,"connector-request-limit");
    assert.equal((await store.createJob({...input,requestId:"one"})).job.id,a.job.id);
    assert.equal((await store.createJob({...input,text:"different",requestId:"one"})).error,"job-request-conflict");
    await store.cancelJob(linkId,a.job.id);await store.cancelJob(linkId,b.job.id);
    assert.equal(store.state.requests.length,2);
  });

  await test("combined write failure and failed ROLLBACK fences until worker restart", async () => {
    const child = await promisify(execFile)(process.execPath, [fileURLToPath(new URL("./connector-rollback-failure-selftest.mjs", import.meta.url))], { windowsHide: true, timeout: 20000, maxBuffer: 65536 });
    assert.equal(JSON.parse(child.stdout).reopenedRecovery, true);
  });

  console.log(JSON.stringify({ok:true,synthetic:true,tests:results},null,2));
} finally {
  for (const store of stores) await store.close();
  await rm(root,{recursive:true,force:true});
}
