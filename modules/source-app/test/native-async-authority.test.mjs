import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createSourceNativeAuthorityPort, createNativeAuthorityRuntime } from '../server/native-authority.mjs';
import { check, digest } from '../server/wire.mjs';

const binding={identity:{issuer:'https://issuer.test/human-identity',subject:'native-one'},resource:{selection:{kind:'soty.resource.v1',nativeId:'selected',incarnationId:'one'}}};
const args={requestId:'async-effect-0001',input:{title:'Synthetic bounded item'}};
const deferred=()=>{let resolve;return{promise:new Promise(done=>{resolve=done;}),release:()=>resolve()};};

/** Real SQLite is used to exercise an async-authority SPI and final transaction
 * witness, NOT to claim a PostgreSQL adapter or PostgreSQL acceptance. */
async function fixture(t,{beforeCommit,afterCommit,beforeAssert}={}){
  const directory=await mkdtemp(join(tmpdir(),'soty-native-async-')),path=join(directory,'native.sqlite'),db=new DatabaseSync(path),other=new DatabaseSync(path);
  db.exec("CREATE TABLE grants(subject TEXT PRIMARY KEY,resource TEXT NOT NULL,live INTEGER NOT NULL);INSERT INTO grants VALUES('native-one','selected',1);CREATE TABLE effects(id TEXT PRIMARY KEY);CREATE TABLE receipts(id TEXT PRIMARY KEY,input_digest TEXT NOT NULL,result_json TEXT NOT NULL);");
  const proofs=new WeakSet();let checkpoint=null,assertions=0,syncChecks=0;
  function current(proof,scope){const row=db.prepare('SELECT * FROM grants WHERE subject=?').get(scope.identity.subject);
    check(proofs.has(proof)&&row?.live===1&&row.resource===scope.resource.selection.nativeId,'source_app_native_access_denied',403);}
  const runtime=createNativeAuthorityRuntime(createSourceNativeAuthorityPort({
    capture:async()=>{const proof=Object.freeze({});proofs.add(proof);return proof;},
    async assertCurrent(proof,scope){assert.equal(checkpoint,null,'fresh async check never enters Native transaction witness');assertions++;await beforeAssert?.(assertions);current(proof,scope);},
    withCurrent(proof,scope,apply){syncChecks++;assert.ok(checkpoint&&checkpoint.proof===proof&&checkpoint.binding===scope&&checkpoint.until>Date.now(),'only exact live SQL transaction checkpoint is synchronous authority');current(proof,scope);return apply();},
    async execute(proof,scope,input,final){await beforeCommit?.();db.exec('BEGIN IMMEDIATE');checkpoint={proof,binding:scope,until:Date.now()+10000};let result;
      try{result=final.commit(()=>{const prior=db.prepare('SELECT * FROM receipts WHERE id=?').get(input.requestId),inputDigest=digest(input.input);check(!prior||prior.input_digest===inputDigest,'source_app_request_conflict',409);
        if(prior)return JSON.parse(prior.result_json);result={requestId:input.requestId,inputDigest,outcome:'committed'};db.prepare('INSERT INTO effects VALUES(?)').run(input.requestId);
        db.prepare('INSERT INTO receipts VALUES(?,?,?)').run(input.requestId,inputDigest,JSON.stringify(result));return result;});
        current(proof,scope);check(checkpoint.until>Date.now());db.exec('COMMIT');}
      catch(error){db.exec('ROLLBACK');throw error;}finally{checkpoint=null;}
      await afterCommit?.();return result;},
    async read(proof,scope){current(proof,scope);return{privateResult:'Synthetic only'};},
    async readProof(proof,scope,input){current(proof,scope);const row=db.prepare('SELECT result_json,input_digest FROM receipts WHERE id=?').get(input.requestId);
      check(!row||row.input_digest===input.inputDigest,'source_app_request_conflict',409);return row?JSON.parse(row.result_json):{outcome:'not_applied'};},
  }));
  const proof=await runtime.capture(binding);
  t.after(async()=>{runtime.close();other.close();db.close();await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30});});
  return{runtime,proof,revoke(){other.prepare('UPDATE grants SET live=0').run();},restore(){other.prepare('UPDATE grants SET live=1').run();},
    count:name=>db.prepare('SELECT count(*) AS n FROM '+name).get().n,counts:()=>({assertions,syncChecks})};
}
test('optional async Source check uses actual current SQL; synchronous authority only exact final transaction witness, default hooks stay unchanged',async t=>{
  const f=await fixture(t);assert.equal(f.counts().syncChecks,0);assert.throws(()=>f.runtime.withCurrent(f.proof,()=>true));
  const result=await f.runtime.call(f.proof,'execute',args);assert.equal(result.outcome,'committed');assert.equal(f.count('effects'),1);assert.equal(f.count('receipts'),1);
  assert.equal(f.counts().syncChecks,2);assert.equal(f.counts().assertions,3); // capture, pre, post; one rejected outside-tx sync attempt
  const receipt=await f.runtime.call(f.proof,'readProof',{requestId:args.requestId,inputDigest:digest(args.input)});assert.deepEqual(receipt,result);
});
test('Native revoke after async preparation is checked inside real SQL final commit, zero effect/receipt',async t=>{
  const entered=deferred(),gate=deferred(),f=await fixture(t,{beforeCommit:async()=>{entered.release();await gate.promise;}});
  const pending=f.runtime.call(f.proof,'execute',args);await entered.promise;f.revoke();gate.release();
  await assert.rejects(pending,error=>error.code==='source_app_native_access_denied');assert.equal(f.count('effects'),0);assert.equal(f.count('receipts'),0);
});
test('Native post-COMMIT revocation is unknown, readonly recovery requires fresh authority and never executes apply again',async t=>{
  let f;f=await fixture(t,{afterCommit:async()=>f.revoke()});
  await assert.rejects(f.runtime.call(f.proof,'execute',args),error=>error.code==='source_app_effect_unknown');assert.equal(f.count('effects'),1);assert.equal(f.count('receipts'),1);
  await assert.rejects(f.runtime.call(f.proof,'readProof',{requestId:args.requestId,inputDigest:digest(args.input)}));
  f.restore();const proof=await f.runtime.capture(binding);const receipt=await f.runtime.call(proof,'readProof',{requestId:args.requestId,inputDigest:digest(args.input)});
  assert.equal(receipt.outcome,'committed');assert.equal(f.count('effects'),1);
});
test('async post-read SQL check with cross-connection revoke never returns private result',async t=>{
  const reached=deferred(),gate=deferred(),f=await fixture(t,{beforeAssert:async count=>{if(count===3){reached.release();await gate.promise;}}});
  const pending=f.runtime.call(f.proof,'read',{});await reached.promise;f.revoke();gate.release();await assert.rejects(pending,error=>error.code==='source_app_native_access_denied');
});
test('async authority rejects JSON/foreign proof, returned permission DTO and disposal during await',async()=>{
  let calls=0;const reached=deferred(),gate=deferred();let hold=false;
  const runtime=createNativeAuthorityRuntime(createSourceNativeAuthorityPort({capture:async()=>({}),withCurrent:()=>{throw new Error('outside transaction');},
    async assertCurrent(){calls++;if(hold){reached.release();await gate.promise;}}}));
  const proof=await runtime.capture(binding);await assert.rejects(runtime.assertCurrent(JSON.parse(JSON.stringify(proof))));assert.equal(calls,1);
  hold=true;const pending=runtime.assertCurrent(proof);await reached.promise;runtime.close();gate.release();await assert.rejects(pending,error=>error.code==='source_app_authority_invalid');
  const invalid=createNativeAuthorityRuntime(createSourceNativeAuthorityPort({capture:async()=>({}),withCurrent:()=>true,assertCurrent:async()=>({allowed:true})}));
  await assert.rejects(invalid.capture(binding),error=>error.code==='source_app_authority_invalid');invalid.close();
});
