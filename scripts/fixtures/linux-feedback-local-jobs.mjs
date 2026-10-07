import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {createSyntheticLocalLinuxFeedbackProcessor as createSyntheticLinuxFeedbackProcessor,SYNTHETIC_FEEDBACK_IMAGE} from '../../modules/source-app/server/linux-feedback-enforcer.mjs';
import {createOrdinaryInstalledFixture} from '../../modules/source-app/test/support/ordinary-installed.mjs';
import {feedbackAttachmentDigest} from '../../modules/source-app/server/feedback-job-contract.mjs';
import {digest} from '../../modules/source-app/server/wire.mjs';
import {LOCAL_LINUX_FEEDBACK_PLACEMENT as placement} from '../../modules/source-app/server/linux-feedback-local-placement.mjs';
import {linuxFeedbackFailure} from '../../modules/source-app/server/linux-feedback-diagnostic.mjs';

// PUBLIC packet code; all accounts/SQL/media are owned temporary synthetic
// fixture state. Actual Root/Human/installed connector is the image runtime.
if(process.platform!=='linux'||process.getuid()!==1000)throw new Error('linux_fixture_not_ready');
const manifest=JSON.parse(await readFile('/probe/manifest.json','utf8'));
assert.equal(manifest.schema,'soty.source-feedback-local-linux-packet.v1');assert.equal(manifest.image,SYNTHETIC_FEEDBACK_IMAGE);assert.equal(manifest.placementDigest,digest(placement));
const results=[],policyRef={id:'reviewed.synthetic-linux-policy',version:1,digest:'b'.repeat(64)};
const budget={wallMs:5000,cpuMs:1000,cleanupMs:1000,scratchBytes:65536,outputBytes:16000,mediaBytes:1024,attachments:1,parallel:1};
const attachments=[{kind:'image',name:'synthetic.png',mimeType:'image/png',dataBase64:'iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAEklEQVR4AWIqn+36H4SZGKAAAAAA///TQrpwAAAABklEQVQDAD4YBLFsqVAqAAAAAElFTkSuQmCC'}];
let failed;
try{for(const scenario of ['success','cpu','wall','cancel','scratch','output','native-revoke']){
  let stage='prepare';
  let activeController,cancelTimer,nativeRevoke,processorStartedAt,nativeTarget;
  const evidence=[],processor=createSyntheticLinuxFeedbackProcessor({directory:'/home/junio/codex-soty-universal-local-20261007-4c8284c7/source-feedback-jobs',scenario:scenario==='native-revoke'?'cancel':scenario,onEvidence:item=>{
    evidence.push(item);if(item.phase==='started')processorStartedAt=performance.now();
    if(scenario==='cancel'&&item.phase==='started')cancelTimer=setTimeout(()=>activeController.abort(),250);
    if(scenario==='native-revoke'&&item.phase==='started'){
      nativeRevoke=(async()=>{
        const child=spawn(process.execPath,['/probe/native-revoke.mjs'],{stdio:['pipe','ignore','ignore'],env:{PATH:'/usr/bin:/bin'},windowsHide:true});
        let force,closed=false,failed=false;const stop=()=>{if(closed)return;failed=true;child.kill('SIGTERM');force??=setTimeout(()=>{if(!closed)child.kill('SIGKILL');},500);};
        const timer=setTimeout(stop,10000);
        const done=new Promise((resolve,reject)=>{child.once('error',()=>{failed=true;});child.once('close',code=>{closed=true;!failed&&code===0?resolve():reject(new Error('native_revoke_failed'));});});done.catch(()=>{});
        try{child.stdin.on('error',()=>{});child.stdin.end(JSON.stringify(nativeTarget));await done;}
        finally{stop();await done.catch(()=>{});clearTimeout(timer);clearTimeout(force);}
      })();nativeRevoke.catch(()=>{});
    }} });
  const hooks=[];let f;
  try{
    await processor.prepare();stage='source_fixture';
    f=await createOrdinaryInstalledFixture({after:fn=>hooks.push(fn)},{feedbackProcessing:{policy:{schema:'soty.feedback.processing-policy.v1',ref:policyRef,purposes:['ocr'],localOnly:true,requiresReporterConsent:true,maxBudget:budget},engines:[processor.engine],enforcer:processor.enforcer},
      libraryNativeOwner:true,connectorScript:'/app/dist/agent/soty-connector.mjs'});
    const realm=f.realms[1];stage='launch';await realm.launch();stage='login';await f.login(realm);
    const invoke=async(input,requestId)=>f.wire(realm.embedded+'/api/embed/invoke',{body:{requestId,input}});
    stage='report';const report=await f.wire(realm.embedded+'/api/embed/feedback',{body:{requestId:'linux-report-'+scenario+'-0001',body:'Synthetic private PNG; no user media',attachments}});assert.equal(report.status,200);
    const ticket=report.value.data.ticket,base={ticketId:ticket.id,ticketRevision:ticket.revision,attachmentDigest:feedbackAttachmentDigest(attachments),purpose:'ocr',policyRef};
    stage='consent';assert.equal((await invoke({operation:'feedback.processing.consent',...base,expiresInSeconds:120},'linux-consent-'+scenario+'-0001')).status,200);
    const input={operation:'feedback.job.grant',...base,engineRef:processor.ref,budget,expiresInSeconds:60},requestId='linux-grant-'+scenario+'-0001';
    stage='grant';const created=await invoke(input,requestId);assert.equal(created.status,200);const jobId=created.value.data.data.jobId;
    if(scenario==='native-revoke')nativeTarget={databasePath:realm.databasePath,nativeSessionHash:realm.instance.store.db.prepare('SELECT native_session_hash FROM native_processor_grants WHERE id=(SELECT grant_id FROM native_feedback_jobs WHERE id=?)').get(jobId).native_session_hash};
    const start=performance.now(),controller=new AbortController();activeController=controller;let outcome='committed';
    stage='process';try{await realm.instance.processing.process(jobId,{signal:controller.signal});}catch(error){assert.equal(error.code,'ordinary_feedback_job_outcome_unknown');outcome='unknown';}
    finally{clearTimeout(cancelTimer);await nativeRevoke;}
    const elapsedMs=Math.ceil(performance.now()-start),job=realm.instance.store.db.prepare('SELECT state FROM native_feedback_jobs WHERE id=?').get(jobId);
    const receipts=realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n;
    stage='result_assertions';assert.equal(outcome,scenario==='success'?'committed':'unknown');assert.equal(job.state,scenario==='success'?'completed':'unknown');assert.equal(receipts,scenario==='success'?1:0);
    if(scenario==='success'){
      stage='readback';await realm.restart();const result=await f.wire(realm.embedded+'/api/embed/query',{body:{requestId:'linux-result-readback-0001',input:{operation:'feedback.job.result',jobId}}});
      assert.equal(result.status,200);assert.equal(result.value.data.outcome,'committed');assert.equal(result.value.data.result.provenance.synthetic,true);
      stage='receipt';const receipt=await f.wire(realm.embedded+'/api/embed/receipt',{body:{requestId,input:{inputDigest:digest(input)}}});assert.equal(receipt.value.data.outcome,'committed');
      assert.equal(realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_processor_receipts').get().n,1);
    }else await assert.rejects(realm.instance.processing.process(jobId));
    stage='cleanup_assertions';assert.equal(realm.instance.store.db.prepare('SELECT status FROM native_tickets WHERE id=?').get(ticket.id).status,'received');
    assert.ok(evidence.some(item=>item.stopped),'owned processor must actually stop');
    const cleanup=evidence.filter(item=>item.phase==='cleanup');assert.equal(cleanup.length,1);
    assert.equal(cleanup[0].cleanupUnknown,false);assert.equal(cleanup[0].containerRemoved,true);assert.equal(cleanup[0].packetRemoved,true);
    if(scenario==='cpu')assert.ok(evidence.some(item=>item.exitCode===137),'real RLIMIT_CPU SIGKILL');
    const nativeRevokeStopMs=scenario==='native-revoke'?Math.ceil(performance.now()-processorStartedAt):undefined;
    if(scenario==='native-revoke'){
      assert.ok(nativeRevoke,'actual running must precede second OS Native revoke');assert.equal(activeController.signal.aborted,false,'no local Abort substitutes Native revoke');
      assert.equal(realm.instance.store.db.prepare('SELECT active FROM native_sessions WHERE id_hash=?').get(nativeTarget.nativeSessionHash).active,0);
      assert.ok(nativeRevokeStopMs<budget.wallMs,'Native SQL monitor must stop owned OS processor before wall');
    }
    results.push({scenario,passed:true,outcome,state:job.state,receipts,elapsedMs,...(nativeRevokeStopMs===undefined?{}:{nativeRevokeStopMs}),evidence,oauthCounts:f.oauthCounts,rootProof:'actual signed Connect/Human + installed HTTP/WS + maintained currentuserinfo',models:false});
  }catch(error){failed={scenario,stage,...linuxFeedbackFailure(error)};results.push({scenario,passed:false,stage,diagnostic:failed,evidence,oauthCounts:f?.oauthCounts??{},models:false});throw error;}
  finally{for(const hook of hooks.reverse())try{await hook();}catch(error){failed??={scenario,stage:'source_cleanup',...linuxFeedbackFailure(error)};}}
  if(failed)break;
}}catch{/* Safe fields only are emitted below. */}
process.stdout.write(JSON.stringify({schema:'soty.source-feedback-linux-receipt.v1',nonce:manifest.nonce,image:manifest.image,placementDigest:digest(placement),synthetic:true,models:false,productionReady:false,results,
  ...(failed?{failure:failed}:{}),passed:!failed&&results.length===7&&results.every(item=>item.passed)})+'\n');
if(failed)process.exitCode=1;
