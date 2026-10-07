import test from 'node:test';
import assert from 'node:assert/strict';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';
import { createFeedbackProcessorEngine,feedbackAttachmentDigest } from '../server/feedback-job-contract.mjs';
import { digest } from '../server/wire.mjs';
import { createFeedbackJobEnforcer } from '../server/feedback-job-enforcer.mjs';

const policyRef={id:'local.synthetic-policy',version:1,digest:'b'.repeat(64)},engineRef={id:'local.synthetic-ocr',version:1,digest:'a'.repeat(64)};
const budget={wallMs:5000,cpuMs:1000,cleanupMs:1000,scratchBytes:65536,outputBytes:16000,mediaBytes:1024,attachments:1,parallel:1};
const processing={policy:{schema:'soty.feedback.processing-policy.v1',ref:policyRef,purposes:['ocr'],localOnly:true,requiresReporterConsent:true,maxBudget:budget},
  engines:[createFeedbackProcessorEngine({ref:engineRef,purposes:['ocr'],localOnly:true,synthetic:true,process:async()=>({kind:'transcript',text:'not run'})})]};
const attachments=[{kind:'image',name:'synthetic.png',mimeType:'image/png',dataBase64:'iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAEklEQVR4AWIqn+36H4SZGKAAAAAA///TQrpwAAAABklEQVQDAD4YBLFsqVAqAAAAAElFTkSuQmCC'}];
test('Source3: same installed Apps/profile2/query/invoke/receipt, real Root OIDC + existing Native owner two proofs; reporter consent and grant default not-ready', {timeout:120000},async t=>{
  const f=await createOrdinaryInstalledFixture(t,{feedbackProcessing:processing,libraryNativeOwner:true});
  for(const realm of f.realms){
    await realm.launch();await f.login(realm);
    const call=async(operation,input,requestId)=>f.wire(realm.embedded+'/api/embed/'+operation,{body:{requestId,input}});
    const context=await call('query',{operation:'feedback.processing.context'},'processing-context-'+realm.realmId);
    assert.equal(context.status,200);assert.equal(context.value.data.canGrant,realm.realmId==='library');assert.equal(context.value.data.processorAvailability,'not-ready');
    const report=await f.wire(realm.embedded+'/api/embed/feedback',{body:{requestId:'job-report-'+realm.realmId,body:'Synthetic source-private PNG issue',attachments}});assert.equal(report.status,200);
    const ticket=report.value.data.ticket,ticketContext=await call('query',{operation:'feedback.processing.ticket',ticketId:ticket.id},'ticket-context-'+realm.realmId);
    assert.equal(ticketContext.status,200);assert.equal(ticketContext.value.data.attachmentDigest,feedbackAttachmentDigest(attachments));
    const base={ticketId:ticket.id,ticketRevision:ticket.revision,attachmentDigest:ticketContext.value.data.attachmentDigest,purpose:'ocr',policyRef};
    const consent=await call('invoke',{operation:'feedback.processing.consent',...base,expiresInSeconds:120},'source-consent-0001-'+realm.realmId);assert.equal(consent.status,200);
    const input={operation:'feedback.job.grant',...base,engineRef,budget,expiresInSeconds:60},requestId='source-grant-0001-'+realm.realmId;
    realm.dropPath='/api/embed/invoke';const grant=await call('invoke',input,requestId);
    if(realm.realmId==='board'){
      assert.equal(grant.status,403);assert.equal(realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_feedback_jobs').get().n,0);continue;
    }
    assert.ok([502,503].includes(grant.status));assert.equal(realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_feedback_jobs').get().n,1);
    await realm.restart();const receipt=await call('receipt',{inputDigest:digest(input)},requestId);assert.equal(receipt.status,200);assert.equal(receipt.value.data.outcome,'committed');
    assert.equal(receipt.value.data.data.processorAvailability,'not-ready');
    const jobId=receipt.value.data.data.jobId;await assert.rejects(realm.instance.processing.process(jobId),error=>error.code==='source_feedback_processor_not_ready');
    const session=realm.instance.store.db.prepare('SELECT id_hash FROM source_sessions').get();
    const proof=await realm.instance.bff.currentFeedbackJobProof(session.id_hash);assert.ok(proof);assert.deepEqual(Object.keys(proof),[]);
    realm.instance.store.revokeMembership('selected','native-owner');await assert.rejects(realm.instance.bff.currentFeedbackJobProof(session.id_hash));
  }
  assert.equal(f.realms[0].profile.sourceProfile.digest,f.realms[1].profile.sourceProfile.digest);
  assert.equal(f.realms[0].instance.store.format,3);assert.equal(f.realms[1].instance.store.format,3);
});
test('actual Native revoke reaches the SAME host-owned executor before wall, outside SQL; no result/receipt or automatic rerun', {timeout:120000},async t=>{
  let entered,aborted=false,activeStore;const started=new Promise(resolve=>{entered=resolve;});
  const enforcer=createFeedbackJobEnforcer({engine:processing.engines[0],platform:'linux',maxBudget:budget,syntheticTestOnly:true,
    execute:({signal})=>new Promise((_resolve,reject)=>{entered();signal.addEventListener('abort',()=>{
      assert.equal(activeStore.inTransaction(),false,'abort listener cannot run inside Native SQL');aborted=true;reject(new Error('owned cancellation'));},{once:true});})});
  const f=await createOrdinaryInstalledFixture(t,{feedbackProcessing:{...processing,enforcer},libraryNativeOwner:true}),realm=f.realms[1];
  activeStore=realm.instance.store;await realm.launch();await f.login(realm);
  const call=async(route,input,requestId)=>f.wire(realm.embedded+'/api/embed/'+route,{body:{requestId,input}});
  const report=await f.wire(realm.embedded+'/api/embed/feedback',{body:{requestId:'actual-cancel-report-0001',body:'Synthetic private PNG',attachments}});assert.equal(report.status,200);
  const ticket=report.value.data.ticket,base={ticketId:ticket.id,ticketRevision:ticket.revision,attachmentDigest:feedbackAttachmentDigest(attachments),purpose:'ocr',policyRef};
  assert.equal((await call('invoke',{operation:'feedback.processing.consent',...base,expiresInSeconds:120},'actual-cancel-consent-0001')).status,200);
  const grant=await call('invoke',{operation:'feedback.job.grant',...base,engineRef,budget,expiresInSeconds:60},'actual-cancel-grant-0001');assert.equal(grant.status,200);
  const jobId=grant.value.data.data.jobId,pending=realm.instance.processing.process(jobId);const rejected=assert.rejects(pending,error=>error.code==='ordinary_feedback_job_outcome_unknown');await started;
  const state=await call('query',{operation:'feedback.job.status',jobId},'actual-cancel-status-0001');assert.equal(state.status,200);assert.equal(state.value.data.state,'started');
  const before=performance.now(),revoke=await call('invoke',{operation:'feedback.job.revoke',jobId,expectedRevision:state.value.data.revision},'actual-cancel-revoke-0001');assert.equal(revoke.status,200);
  await rejected;assert.equal(aborted,true);assert.ok(performance.now()-before<2000,'abort must precede 5000ms admitted wall');
  assert.equal(activeStore.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,0);
  assert.equal(activeStore.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(jobId).state,'unknown');
  await assert.rejects(realm.instance.processing.process(jobId));assert.equal(activeStore.db.prepare('SELECT status FROM native_tickets WHERE id=?').get(ticket.id).status,'received');
});
test('installed current Root/RP proof at Source SQL claim/final: synthetic executor result commits once; closing original real Root slot during held process prevents result', {timeout:120000},async t=>{
  let entered,release,held=false,calls=0;const started=new Promise(resolve=>{entered=resolve;}),wait=new Promise(resolve=>{release=resolve;});
  const enforcer=createFeedbackJobEnforcer({engine:processing.engines[0],platform:'linux',maxBudget:budget,syntheticTestOnly:true,
    execute:async()=>{calls++;if(held){entered();await wait;}return{kind:'transcript',text:'Synthetic local result; not OCR quality'};}});
  const f=await createOrdinaryInstalledFixture(t,{feedbackProcessing:{...processing,enforcer},libraryNativeOwner:true}),realm=f.realms[1];
  await realm.launch();await f.login(realm);
  const call=async(input,requestId)=>f.wire(realm.embedded+'/api/embed/invoke',{body:{requestId,input}});
  const report=await f.wire(realm.embedded+'/api/embed/feedback',{body:{requestId:'actual-processing-report-0001',body:'Synthetic private PNG',attachments}});assert.equal(report.status,200);
  const ticket=report.value.data.ticket,base={ticketId:ticket.id,ticketRevision:ticket.revision,attachmentDigest:feedbackAttachmentDigest(attachments),purpose:'ocr',policyRef};
  assert.equal((await call({operation:'feedback.processing.consent',...base,expiresInSeconds:120},'actual-processing-consent-0001')).status,200);
  const input={operation:'feedback.job.grant',...base,engineRef,budget,expiresInSeconds:60};
  const first=await call(input,'actual-processing-grant-0001');assert.equal(first.status,200);
  const result=await realm.instance.processing.process(first.value.data.data.jobId);assert.equal(result.outcome,'committed');assert.equal(calls,1);
  assert.equal(result.result.provenance.synthetic,true);assert.equal(result.result.provenance.humanReviewRequired,true);
  const second=await call(input,'actual-processing-grant-0002');assert.equal(second.status,200);
  const jobId=second.value.data.data.jobId;held=true;const pending=realm.instance.processing.process(jobId);await started;
  await f.reader.client.extension('apps.scoped.close',{appId:realm.appId,handle:realm.lastLaunch.scopedCloseHandle});release();
  await assert.rejects(pending,error=>error.code==='ordinary_feedback_job_outcome_unknown');
  assert.equal(realm.instance.store.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(jobId).state,'unknown');
  assert.equal(realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,1);assert.equal(calls,2);
  await assert.rejects(realm.instance.processing.process(jobId));assert.equal(calls,2);
  assert.equal(realm.instance.store.db.prepare('SELECT status FROM native_tickets WHERE id=?').get(ticket.id).status,'received');
});
