import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createSyntheticLinuxFeedbackProcessor,SYNTHETIC_FEEDBACK_IMAGE} from '../../modules/source-app/server/linux-feedback-enforcer.mjs';
import {createOrdinaryInstalledFixture} from '../../modules/source-app/test/support/ordinary-installed.mjs';
import {feedbackAttachmentDigest} from '../../modules/source-app/server/feedback-job-contract.mjs';
import {digest} from '../../modules/source-app/server/wire.mjs';

// PUBLIC packet code; all accounts/SQL/media are owned temporary synthetic
// fixture state. Actual Root/Human/installed connector is the image runtime.
if(process.platform!=='linux'||process.getuid()!==1000)throw new Error('linux_fixture_not_ready');
const manifest=JSON.parse(await readFile('/probe/manifest.json','utf8'));
assert.equal(manifest.schema,'soty.source-feedback-linux-packet.v1');assert.equal(manifest.image,SYNTHETIC_FEEDBACK_IMAGE);
const results=[],policyRef={id:'reviewed.synthetic-linux-policy',version:1,digest:'b'.repeat(64)};
const budget={wallMs:5000,cpuMs:1000,cleanupMs:1000,scratchBytes:65536,outputBytes:16000,mediaBytes:1024,attachments:1,parallel:1};
const attachments=[{kind:'image',name:'synthetic.png',mimeType:'image/png',dataBase64:'iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAEklEQVR4AWIqn+36H4SZGKAAAAAA///TQrpwAAAABklEQVQDAD4YBLFsqVAqAAAAAElFTkSuQmCC'}];
for(const scenario of ['success','cpu','wall','cancel','scratch','output']){
  let activeController,cancelTimer;
  const evidence=[],processor=createSyntheticLinuxFeedbackProcessor({directory:'/home/ai2/codex-soty-universal-20261007-9f8dcd71/source-feedback-jobs',scenario,onEvidence:item=>{
    evidence.push(item);if(scenario==='cancel'&&item.phase==='started')cancelTimer=setTimeout(()=>activeController.abort(),250);}});
  await processor.prepare();const hooks=[];
  const f=await createOrdinaryInstalledFixture({after:fn=>hooks.push(fn)},{feedbackProcessing:{policy:{schema:'soty.feedback.processing-policy.v1',ref:policyRef,purposes:['ocr'],localOnly:true,requiresReporterConsent:true,maxBudget:budget},engines:[processor.engine],enforcer:processor.enforcer},
    libraryNativeOwner:true,connectorScript:'/app/dist/agent/soty-connector.mjs'});
  try{
    const realm=f.realms[1];await realm.launch();await f.login(realm);
    const invoke=async(input,requestId)=>f.wire(realm.embedded+'/api/embed/invoke',{body:{requestId,input}});
    const report=await f.wire(realm.embedded+'/api/embed/feedback',{body:{requestId:'linux-report-'+scenario+'-0001',body:'Synthetic private PNG; no user media',attachments}});assert.equal(report.status,200);
    const ticket=report.value.data.ticket,base={ticketId:ticket.id,ticketRevision:ticket.revision,attachmentDigest:feedbackAttachmentDigest(attachments),purpose:'ocr',policyRef};
    assert.equal((await invoke({operation:'feedback.processing.consent',...base,expiresInSeconds:120},'linux-consent-'+scenario+'-0001')).status,200);
    const input={operation:'feedback.job.grant',...base,engineRef:processor.ref,budget,expiresInSeconds:60},requestId='linux-grant-'+scenario+'-0001';
    const created=await invoke(input,requestId);assert.equal(created.status,200);const jobId=created.value.data.data.jobId;
    const start=performance.now(),controller=new AbortController();activeController=controller;let outcome='committed';
    try{await realm.instance.processing.process(jobId,{signal:controller.signal});}catch(error){assert.equal(error.code,'ordinary_feedback_job_outcome_unknown');outcome='unknown';}
    finally{clearTimeout(cancelTimer);}
    const elapsedMs=Math.ceil(performance.now()-start),job=realm.instance.store.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(jobId);
    const receipts=realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n;
    assert.equal(outcome,scenario==='success'?'committed':'unknown');assert.equal(job.state,scenario==='success'?'completed':'unknown');assert.equal(receipts,scenario==='success'?1:0);
    if(scenario==='success'){
      await realm.restart();const result=await f.wire(realm.embedded+'/api/embed/query',{body:{requestId:'linux-result-readback-0001',input:{operation:'feedback.job.result',jobId}}});
      assert.equal(result.status,200);assert.equal(result.value.data.outcome,'committed');assert.equal(result.value.data.result.provenance.synthetic,true);
      const receipt=await f.wire(realm.embedded+'/api/embed/receipt',{body:{requestId,input:{inputDigest:digest(input)}}});assert.equal(receipt.value.data.outcome,'committed');
      assert.equal(realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,1);
    }else await assert.rejects(realm.instance.processing.process(jobId));
    assert.equal(realm.instance.store.db.prepare('SELECT status FROM native_tickets WHERE id=?').get(ticket.id).status,'received');
    assert.ok(evidence.some(item=>item.stopped),'owned processor must actually stop');
    if(scenario==='cpu')assert.ok(evidence.some(item=>item.exitCode===137),'real RLIMIT_CPU SIGKILL');
    results.push({scenario,passed:true,outcome,state:job.state,receipts,elapsedMs,evidence,oauthCounts:f.oauthCounts,rootProof:'actual signed Connect/Human + installed HTTP/WS + maintained currentuserinfo',models:false});
  }finally{for(const hook of hooks.reverse())await hook();}
}
process.stdout.write(JSON.stringify({schema:'soty.source-feedback-linux-receipt.v1',nonce:manifest.nonce,image:manifest.image,synthetic:true,models:false,productionReady:false,results,passed:results.every(item=>item.passed)})+'\n');
