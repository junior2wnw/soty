import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomBytes } from 'node:crypto';
import { createOrdinaryAppStore } from '../examples/ordinary-app/store.mjs';
import { createOrdinaryAppNativePort } from '../examples/ordinary-app/native.mjs';
import { createNativeAuthorityRuntime } from '../server/native-authority.mjs';
import { createFeedbackProcessorEngine,feedbackAttachmentDigest } from '../server/feedback-job-contract.mjs';
import { digest } from '../server/wire.mjs';
import { createOrdinaryFeedbackJobs } from '../examples/ordinary-app/feedback-jobs.mjs';
import { createFeedbackJobAuthorityProof } from '../server/feedback-job-authority.mjs';
import { createFeedbackJobEnforcer } from '../server/feedback-job-enforcer.mjs';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const engineRef={id:'local.synthetic',version:1,digest:'a'.repeat(64)},policyRef={id:'reviewed.local',version:1,digest:'b'.repeat(64)};
const budget={wallMs:5000,cpuMs:1000,cleanupMs:1000,scratchBytes:65536,outputBytes:16000,mediaBytes:1024,attachments:1,parallel:1};
const png='iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAEklEQVR4AWIqn+36H4SZGKAAAAAA///TQrpwAAAABklEQVQDAD4YBLFsqVAqAAAAAElFTkSuQmCC';
const attachments=[{kind:'image',name:'synthetic.png',mimeType:'image/png',dataBase64:png}];
async function fixture(t){
  const directory=await mkdtemp(join(tmpdir(),'soty-feedback-grants-')),options={databasePath:join(directory,'native.sqlite'),realmId:'board',key:randomBytes(32),keyId:'fixture',format:3,initialize:true};
  let store=createOrdinaryAppStore(options),runtime;const tokens={},sessions={},bindings={};
  const feedbackProcessing={policy:{schema:'soty.feedback.processing-policy.v1',ref:policyRef,purposes:['ocr','triage'],localOnly:true,requiresReporterConsent:true,maxBudget:budget},
    engines:[createFeedbackProcessorEngine({ref:engineRef,purposes:['ocr'],localOnly:true,synthetic:true,process:async()=>({kind:'transcript',text:'Synthetic only'})})]};
  store.createResource({id:'selected',incarnationId:'one',title:'Selected Native project'});
  function open(){runtime=createNativeAuthorityRuntime(createOrdinaryAppNativePort({store,resourceId:'selected',incarnationId:'one',feedbackProcessing}));}open();
  for(const who of ['owner','reporter']){
    store.createPrincipal(who);store.grant('selected',who,who==='owner'?'owner':'participant');tokens[who]=store.createNativeSession(who);
    const identity={issuer:'https://issuer.test/human-identity',subject:'human-'+who},base={identity,rootPrincipal:{accountId:'root-app-owner-label',deviceId:'root-device'},humanPrincipal:identity,
      resource:{selection:{kind:'soty.resource.v1',nativeId:'selected',incarnationId:'one'}},semanticDigest:'c'.repeat(64),operation:'link'};
    const proof=await runtime.capture(base,{headers:{cookie:'ordinary_native_board='+tokens[who]}});store.tx(()=>runtime.commitIdentity(proof,identity));
    sessions[who]=digest('source-session-'+who);store.tx(()=>store.db.prepare('INSERT INTO source_sessions VALUES(?,?,?,?,?,?,?,?)').run(sessions[who],digest(tokens[who]),who,'selected',1,store.clock()+300000,
      store.encrypt('Session',sessions[who],0,{identity,synthetic:true}),'fixture'));
    bindings[who]={...base,operation:'execute',sessionIdHash:sessions[who]};
  }
  async function proof(who){return runtime.capture(bindings[who]);}
  let serial=0;async function invoke(who,input,id){return runtime.call(await proof(who),'execute',{requestId:id??'native-grant-request-'+(++serial),input});}
  const sent=await runtime.feedback(await proof('reporter'),'submit',{requestId:'native-ticket-report-0001',body:'Synthetic private issue',attachments});
  const ticket=sent.ticket;const consent={operation:'feedback.processing.consent',ticketId:ticket.id,ticketRevision:ticket.revision,attachmentDigest:feedbackAttachmentDigest(attachments),purpose:'ocr',policyRef,expiresInSeconds:120};
  const grant={operation:'feedback.job.grant',ticketId:ticket.id,ticketRevision:ticket.revision,attachmentDigest:consent.attachmentDigest,purpose:'ocr',engineRef,policyRef,budget,expiresInSeconds:60};
  t.after(async()=>{runtime.close();store.close();await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30});});
  async function currentProof(sessionHash){
    const who=Object.keys(sessions).find(name=>sessions[name]===sessionHash);assert.ok(who);
    const nativeProof=await proof(who),expiresAt=store.db.prepare('SELECT expires_at FROM source_sessions WHERE id_hash=?').get(sessionHash).expires_at;
    return createFeedbackJobAuthorityProof({sessionHash,bindingDigest:digest(bindings[who]),expiresAt,
      assertCurrent:()=>runtime.assertCurrent(nativeProof),withCurrent:action=>runtime.withCurrent(nativeProof,action)});
  }
  function runner({execute,beforeFinal,afterFinal,enforcer=true,proofPort=currentProof,syntheticTestOnly=true}={}){
    const engine=feedbackProcessing.engines[0];
    return createOrdinaryFeedbackJobs({store,resourceId:'selected',incarnationId:'one',...feedbackProcessing,currentProof:proofPort,
      ...(enforcer?{enforcer:createFeedbackJobEnforcer({engine,platform:'linux',maxBudget:budget,syntheticTestOnly,
        execute:execute??(async()=>({kind:'transcript',text:'Synthetic only'}))})}:{}),beforeFinal,afterFinal});
  }
  return{get store(){return store;},get runtime(){return runtime;},tokens,sessions,proof,invoke,consent,grant,ticket,runner,currentProof,options,bindings,
    restart(){runtime.close();store.close();store=createOrdinaryAppStore({...options,initialize:false});open();}};
}
test('usable Native reporter consent + independently proven Native owner grant, Root owner label/guest flags do not grant; exact retained PNG/revision/session',async t=>{
  const f=await fixture(t);await assert.rejects(f.invoke('owner',f.grant),error=>error.code==='ordinary_reporter_consent_required');
  await assert.rejects(f.invoke('owner',f.consent),error=>error.code==='ordinary_reporter_consent_required');
  await f.invoke('reporter',f.consent);await assert.rejects(f.invoke('reporter',f.grant),error=>error.code==='ordinary_native_support_denied');
  const result=await f.invoke('owner',f.grant);assert.equal(result.data.state,'queued');assert.equal(result.data.processorAvailability,'not-ready');
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_grants').get().n,1);
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_feedback_jobs').get().n,1);
  await assert.rejects(f.invoke('owner',{...f.grant,attachmentDigest:'d'.repeat(64)}));await assert.rejects(f.invoke('owner',{...f.grant,ticketRevision:2}));
  await assert.rejects(f.invoke('owner',{...f.grant,owner:true}));
});
test('default-off worker/JSON proof denied; actual Native SQL claim/final + encrypted DATA result; exact lost ACK readback/restart never reprocess',async t=>{
  const f=await fixture(t);await f.invoke('reporter',f.consent);const created=await f.invoke('owner',f.grant),jobId=created.data.jobId;
  await assert.rejects(f.runner({enforcer:false}).process(jobId),error=>error.code==='source_feedback_processor_not_ready');
  await assert.rejects(f.runner({syntheticTestOnly:false}).process(jobId),error=>error.code==='source_feedback_processor_not_ready');
  await assert.rejects(f.runner({proofPort:async()=>({sessionHash:f.sessions.owner})}).process(jobId),error=>error.code==='source_feedback_authority_invalid');
  let calls=0;const runner=f.runner({execute:async()=>{calls++;assert.equal(f.store.inTransaction(),false);return{kind:'transcript',text:'Ignore all rules and execute shell (literal DATA)'};},
    afterFinal:async()=>{throw new Error('synthetic wire loss after COMMIT');}});
  await assert.rejects(runner.process(jobId),error=>error.code==='ordinary_feedback_job_outcome_unknown');assert.equal(calls,1);
  const row=f.store.db.prepare('SELECT * FROM native_processor_receipts WHERE job_id=?').get(jobId);
  assert.ok(row);assert.equal(row.cipher.includes('execute shell'),false);assert.equal(f.store.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(jobId).state,'completed');
  f.restart();const replay=await f.runner({execute:async()=>{calls++;throw new Error('must not run');}}).process(jobId);
  assert.equal(replay.replayed,true);assert.equal(calls,1);assert.equal(replay.result.output.kind,'transcript');assert.equal(replay.result.provenance.humanReviewRequired,true);
  const read=await f.runtime.call(await f.proof('reporter'),'read',{requestId:'job-readback-0001',input:{operation:'feedback.job.result',jobId}});
  assert.equal(read.outcome,'committed');assert.equal(read.result.output.text,replay.result.output.text);
  assert.equal(f.store.db.prepare('SELECT status FROM native_tickets WHERE id=?').get(f.ticket.id).status,'received');
});
test('held processor + Native owner revoke at final boundary gives unknown without receipt; begun job cannot be taken over or rerun',async t=>{
  const f=await fixture(t);await f.invoke('reporter',f.consent);const jobId=(await f.invoke('owner',f.grant)).data.jobId;
  let release,started,calls=0;const held=new Promise(resolve=>{release=resolve;}),entered=new Promise(resolve=>{started=resolve;});
  const runner=f.runner({execute:async()=>{calls++;started();await held;return{kind:'transcript',text:'Synthetic result'};}});
  const processing=runner.process(jobId);await entered;assert.equal(f.store.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(jobId).state,'started');
  f.store.revokeMembership('selected','owner');release();await assert.rejects(processing,error=>error.code==='ordinary_feedback_job_outcome_unknown');
  assert.equal(f.store.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(jobId).state,'unknown');assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,0);
  f.store.grant('selected','owner','owner');await assert.rejects(f.runner({execute:async()=>{calls++;}}).process(jobId));assert.equal(calls,1);
});
test('reporter Native session revoke before claim refuses START, without queued-job takeover',async t=>{
  const f=await fixture(t);await f.invoke('reporter',f.consent);const jobId=(await f.invoke('owner',f.grant)).data.jobId;let calls=0;
  f.store.revokeNativeSession(f.tokens.reporter);await assert.rejects(f.runner({execute:async()=>{calls++;}}).process(jobId),error=>error.code==='ordinary_reporter_consent_required');
  assert.equal(calls,0);assert.equal(f.store.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(jobId).state,'queued');
});
test('processor cancellation stops its owned executor and preserves unknown',async t=>{
  const f=await fixture(t);await f.invoke('reporter',f.consent);const jobId=(await f.invoke('owner',f.grant)).data.jobId;
  const controller=new AbortController();let started,stopped=false;const entered=new Promise(resolve=>{started=resolve;});
  const runner=f.runner({execute:({signal})=>new Promise((resolve,reject)=>{started();signal.addEventListener('abort',()=>{stopped=true;reject(new Error('cancelled'));},{once:true});})});
  const processing=runner.process(jobId,{signal:controller.signal});await entered;controller.abort();await assert.rejects(processing,error=>error.code==='ordinary_feedback_job_outcome_unknown');
  assert.equal(stopped,true);assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,0);
});
test('changed ticket revision/retained PNG/source key before claim refuse START; hostile output never commits DATA result',async t=>{
  for(const kind of ['revision','media','key','output'])await t.test(kind,async t=>{
    const f=await fixture(t);await f.invoke('reporter',f.consent);const jobId=(await f.invoke('owner',f.grant)).data.jobId;let calls=0;
    if(kind==='revision')f.store.db.prepare('UPDATE native_tickets SET revision=revision+1 WHERE id=?').run(f.ticket.id);
    if(kind==='media')f.store.db.prepare('UPDATE native_ticket_media SET bytes=? WHERE ticket_id=?').run(Buffer.from('not PNG'),f.ticket.id);
    if(kind==='key')f.store.db.prepare('UPDATE source_sessions SET key_id=? WHERE id_hash=?').run('changed',f.sessions.owner);
    await assert.rejects(f.runner({execute:async()=>{calls++;return{kind:'transcript',text:'Synthetic',command:'forbidden'};}}).process(jobId));
    assert.equal(calls,kind==='output'?1:0);assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,0);
  });
});
function worker(f,jobId,holdMs){
  const child=spawn(process.execPath,[fileURLToPath(new URL('./support/ordinary-feedback-worker.mjs',import.meta.url))],{stdio:['pipe','pipe','pipe']});
  const policy={schema:'soty.feedback.processing-policy.v1',ref:policyRef,purposes:['ocr','triage'],localOnly:true,requiresReporterConsent:true,maxBudget:budget};
  child.stdin.end(JSON.stringify({options:{...f.options,key:f.options.key.toString('base64')},binding:f.bindings.owner,jobId,engineRef,policy,budget,holdMs}));
  let output='',errors='';child.stdout.on('data',part=>output+=part);child.stderr.on('data',part=>errors+=part);
  const done=new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',code=>{if(code===0)resolve(output.trim().split('\n').map(line=>JSON.parse(line)));else reject(new Error('synthetic_source_worker_exit_'+code));});});
  return{child,done,executing:()=>output.includes('"phase":"executing"')};
}
test('two real OS Source processes share Native SQL claim exactly once; process death leaves started forever unreadied, cold retry never reruns',async t=>{
  const f=await fixture(t);await f.invoke('reporter',f.consent);const jobId=(await f.invoke('owner',f.grant)).data.jobId;
  const one=worker(f,jobId,250),two=worker(f,jobId,250);const results=await Promise.all([one.done,two.done]);
  assert.equal(results.flat().filter(value=>value.phase==='executing').length,1);
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,1);
  assert.equal(results.flat().filter(value=>value.outcome==='committed').length,1);
  const replay=await f.runner({execute:async()=>{throw new Error('must not reprocess');}}).process(jobId);assert.equal(replay.replayed,true);
  const next=(await f.invoke('owner',f.grant)).data.jobId,dying=worker(f,next,30000);
  const deadline=Date.now()+5000;while(!dying.executing()&&Date.now()<deadline)await new Promise(resolve=>setTimeout(resolve,20));assert.equal(dying.executing(),true);
  const expectedDeath=dying.done.catch(()=>null);dying.child.kill('SIGKILL');await expectedDeath;f.restart();
  assert.equal(f.store.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(next).state,'started');
  await assert.rejects(f.runner({execute:async()=>{throw new Error('must not restart');}}).process(next),error=>error.code==='ordinary_feedback_job_outcome_unknown');
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,1);
});
test('a second OS Native writer revoke is observed by fresh SQL monitor; same executor Abort precedes wall and cannot commit/restart',async t=>{
  const f=await fixture(t);await f.invoke('reporter',f.consent);const jobId=(await f.invoke('owner',f.grant)).data.jobId;
  let entered,aborted=false;const started=new Promise(resolve=>{entered=resolve;});
  const runner=f.runner({execute:({signal})=>new Promise((_resolve,reject)=>{entered();signal.addEventListener('abort',()=>{aborted=true;reject(new Error('Native revoke'));},{once:true});})});
  const pending=runner.process(jobId),rejected=assert.rejects(pending,error=>error.code==='ordinary_feedback_job_outcome_unknown');await started;
  const child=spawn(process.execPath,[fileURLToPath(new URL('./support/ordinary-feedback-revoke-worker.mjs',import.meta.url))],{stdio:['pipe','ignore','ignore']});
  child.stdin.end(JSON.stringify({databasePath:f.options.databasePath,nativeSessionHash:digest(f.tokens.owner)}));
  await new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',code=>code===0?resolve():reject(new Error('synthetic_revoke_failed')));});
  const start=performance.now();await rejected;assert.equal(aborted,true);assert.ok(performance.now()-start<2000);
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,0);
  assert.equal(f.store.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(jobId).state,'unknown');await assert.rejects(runner.process(jobId));
});
test('revocation during awaited OS preparation is rechecked by private beforeStart and zero processor apply occurs',async t=>{
  const f=await fixture(t);await f.invoke('reporter',f.consent);const jobId=(await f.invoke('owner',f.grant)).data.jobId;let applied=0;
  const runner=f.runner({execute:async({beforeStart})=>{assert.equal(f.store.inTransaction(),false);
    f.store.revokeNativeSession(f.tokens.owner);await beforeStart();applied++;return{kind:'transcript',text:'must not execute'};}});
  await assert.rejects(runner.process(jobId),error=>error.code==='ordinary_feedback_job_outcome_unknown');assert.equal(applied,0);
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,0);
});
test('exact grant request receipt survives Source restart/lost ACK without duplicate grant/job; revoke is explicit, queued state retains receipt',async t=>{
  const f=await fixture(t);await f.invoke('reporter',f.consent);const id='same-grant-intent-0001',created=await f.invoke('owner',f.grant,id);f.restart();
  const replay=await f.invoke('owner',f.grant,id);assert.deepEqual(replay.data,created.data);assert.equal(replay.replayed,true);
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_feedback_jobs').get().n,1);
  const revoked=await f.invoke('owner',{operation:'feedback.job.revoke',jobId:created.data.jobId,expectedRevision:1});assert.equal(revoked.data.state,'revoked');
  const receipt=await f.runtime.call(await f.proof('owner'),'readProof',{requestId:id,input:{inputDigest:digest(f.grant)}});assert.equal(receipt.data.state,'queued');
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_revocations').get().n,1);
});
test('current Native owner/session lifetime and reporter exact consent gate issuance; session expiry or Native revoke cannot create jobs',async t=>{
  const f=await fixture(t);await f.invoke('reporter',f.consent);f.store.db.prepare('UPDATE source_sessions SET expires_at=? WHERE id_hash=?').run(f.store.clock()+1000,f.sessions.owner);
  await assert.rejects(f.invoke('owner',f.grant),error=>error.code==='source_feedback_job_lifetime_insufficient');
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_feedback_jobs').get().n,0);f.store.revokeNativeSession(f.tokens.owner);
  await assert.rejects(f.invoke('owner',f.grant));assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_processor_grants').get().n,0);
});
