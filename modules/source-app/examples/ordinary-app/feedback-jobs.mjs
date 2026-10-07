import { check,fields,digest,nonce,jsonCopy } from '../../server/wire.mjs';
import { feedbackProcessingPolicy,feedbackJobIntent,feedbackJobRequest,feedbackAttachmentDigest,assertFeedbackJobLifetime,feedbackProcessorEngine,feedbackProcessorOutput } from '../../server/feedback-job-contract.mjs';
import { feedbackJobAuthority } from '../../server/feedback-job-authority.mjs';
import { feedbackJobEnforcer,assertFeedbackHostBounds } from '../../server/feedback-job-enforcer.mjs';
const services=new WeakMap();
export function requireOrdinaryFeedbackJobs(service,store,resourceId,incarnationId){
  const scope=services.get(service);check(scope?.store===store&&scope.resourceId===resourceId&&scope.incarnationId===incarnationId,'ordinary_feedback_jobs_not_ready',503);return service;
}

/** Source-owned Native SQL, never Root/body ownership. Only the Native adapter
 * calls execute with its private current proof inside final SQL commit. The
 * process runner is separate and defaults not-ready without hardcap hostport. */
export function createOrdinaryFeedbackJobs({store,resourceId,incarnationId,policy:rawPolicy,engines=[],currentProof,enforcer,beforeFinal,afterFinal}){
  check(store.format===3,'ordinary_feedback_jobs_not_ready',503);const {db}=store,policy=feedbackProcessingPolicy(rawPolicy),registry=new Map();
  for(const port of engines){const engine=feedbackProcessorEngine(port),key=digest(engine.ref);check(!registry.has(key));registry.set(key,port);}
  const executor=enforcer===undefined?null:feedbackJobEnforcer(enforcer),running=new Map();
  check(currentProof===undefined||typeof currentProof==='function');
  const workerConfigured=executor!==null&&typeof currentProof==='function';
  const availability=()=>workerConfigured&&executor.syntheticTestOnly?'synthetic-test':'not-ready';
  function current(actor,owner=false){
    const session=db.prepare('SELECT * FROM native_sessions WHERE id_hash=?').get(actor.nativeSessionHash),member=db.prepare('SELECT * FROM native_memberships WHERE resource_id=? AND principal_id=?').get(resourceId,actor.principalId);
    const resource=db.prepare('SELECT * FROM native_resources WHERE id=?').get(resourceId);
    check(resource?.incarnation_id===incarnationId&&session?.active===1&&session.expires_at>store.clock()&&session.principal_id===actor.principalId
      &&session.generation===actor.sessionGeneration&&member?.active===1&&member.revision===actor.membershipRevision,'ordinary_native_access_denied',403);
    check(!owner||member.role==='owner','ordinary_native_support_denied',403);return{session,member};
  }
  function retained(actor,ticketId,revision){const native=current(actor);const row=db.prepare('SELECT * FROM native_tickets WHERE id=? AND resource_id=?').get(ticketId,resourceId);
    check(row&&(row.reporter_id===actor.principalId||native.member.role==='owner'),'ordinary_native_ticket_denied',403);check(row.revision===revision,'feedback_revision_conflict',409);
    const attachments=db.prepare('SELECT metadata_json,bytes FROM native_ticket_media WHERE ticket_id=? ORDER BY ordinal').all(ticketId)
      .map(value=>{const metadata=JSON.parse(value.metadata_json);return{kind:metadata.kind,name:metadata.name,mimeType:metadata.mimeType,dataBase64:Buffer.from(value.bytes).toString('base64')};});
    return{row,input:{ticketId,ticketRevision:revision,body:row.body,attachments}};
  }
  function consent(actor,input,apply=true){
    const args=fields(input,['operation','ticketId','ticketRevision','attachmentDigest','purpose','policyRef','expiresInSeconds']);
    const native=current(actor),source=db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(actor.sourceSessionHash);
    check(source?.active===1&&source.principal_id===actor.principalId&&source.native_session_hash===actor.nativeSessionHash&&source.resource_id===resourceId&&source.expires_at>store.clock(),'ordinary_native_access_denied',403);
    const ticket=retained(actor,args.ticketId,args.ticketRevision);check(ticket.row.reporter_id===actor.principalId,'ordinary_reporter_consent_required',403);
    check(policy.purposes.includes(args.purpose)&&digest(policy.ref)===digest(args.policyRef),'source_feedback_processing_policy_denied',403);
    check(args.attachmentDigest===feedbackAttachmentDigest(ticket.input.attachments),'source_feedback_job_context_changed',409);
    check(Number.isSafeInteger(args.expiresInSeconds)&&args.expiresInSeconds>0&&args.expiresInSeconds<=900);
    if(!apply)return true;
    check(db.prepare('SELECT count(*) AS n FROM native_processing_consents WHERE expires_at>?').get(store.clock()).n<256,'ordinary_feedback_capacity',503);
    const id='consent-'+nonce().slice(0,24),now=store.clock(),expiresAt=Math.min(now+args.expiresInSeconds*1000,native.session.expires_at,source.expires_at);
    db.prepare('INSERT INTO native_processing_consents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(id,args.ticketId,actor.principalId,args.ticketRevision,args.attachmentDigest,args.purpose,digest(policy.ref),actor.nativeSessionHash,native.session.generation,actor.sourceSessionHash,source.key_id,native.member.revision,expiresAt,now);
    return{consentId:id,ticketRevision:args.ticketRevision,expiresAt};
  }
  function grant(actor,input,requestId,apply=true){
    const {operation,...rest}=fields(input,['operation','ticketId','ticketRevision','attachmentDigest','purpose','engineRef','policyRef','budget','expiresInSeconds']);
    const request=feedbackJobRequest({requestId,...rest}),native=current(actor,true),ticket=retained(actor,request.ticketId,request.ticketRevision),intent=feedbackJobIntent(request,ticket.input,policy);
    const engine=registry.get(digest(request.engineRef));check(engine&&engine.purposes.includes(request.purpose),'source_feedback_engine_unapproved',503);
    const consent=db.prepare('SELECT * FROM native_processing_consents WHERE ticket_id=? AND reporter_id=? AND ticket_revision=? AND attachment_digest=? AND purpose=? AND policy_digest=? AND expires_at>? ORDER BY created_at DESC LIMIT 1')
      .get(request.ticketId,ticket.row.reporter_id,request.ticketRevision,request.attachmentDigest,request.purpose,digest(policy.ref),store.clock());const consentEnd=consentCurrent(consent);
    const source=db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(actor.sourceSessionHash);
    check(source?.active===1&&source.principal_id===actor.principalId&&source.native_session_hash===actor.nativeSessionHash&&source.resource_id===resourceId,'ordinary_native_access_denied',403);
    const now=store.clock(),expiresAt=Math.min(now+request.expiresInSeconds*1000,native.session.expires_at,source.expires_at,consentEnd);
    assertFeedbackJobLifetime({now,grantExpiresAt:expiresAt,sessionExpiresAt:native.session.expires_at,keyExpiresAt:source.expires_at,sourceProofExpiresAt:source.expires_at},request.budget);
    if(!apply)return true;
    check(db.prepare("SELECT count(*) AS n FROM native_feedback_jobs WHERE state IN('queued','started','unknown')").get().n<256,'ordinary_feedback_capacity',503);
    const grantId='grant-'+nonce().slice(0,24),jobId='job-'+nonce().slice(0,24),payload={request,ownerSessionGeneration:native.session.generation,membershipRevision:native.member.revision,
      incarnationId,authorityBindingDigest:actor.bindingDigest,policyRef:policy.ref,engineRef:request.engineRef,consentId:consent.id,attachmentDigest:request.attachmentDigest,synthetic:engine.synthetic};
    const cipher=store.encrypt('ProcessorGrant',grantId,0,payload);
    db.prepare('INSERT INTO native_processor_grants VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run(grantId,resourceId,request.ticketId,actor.principalId,actor.nativeSessionHash,actor.sourceSessionHash,
      consent.id,requestId,intent.inputDigest,expiresAt,now,cipher,source.key_id);
    db.prepare("INSERT INTO native_feedback_jobs VALUES(?,?,'queued',1,NULL,NULL,NULL)").run(jobId,grantId);
    return{jobId,grantId,state:'queued',revision:1,expiresAt,synthetic:engine.synthetic,processorAvailability:availability()};
  }
  function status(actor,jobId){const native=current(actor);const row=db.prepare('SELECT j.*,g.ticket_id,g.resource_id FROM native_feedback_jobs j JOIN native_processor_grants g ON g.id=j.grant_id WHERE j.id=?').get(jobId);
    check(row?.resource_id===resourceId,'ordinary_feedback_job_denied',403);const ticket=db.prepare('SELECT reporter_id FROM native_tickets WHERE id=?').get(row.ticket_id);
    check(native.member.role==='owner'||ticket?.reporter_id===actor.principalId,'ordinary_feedback_job_denied',403);
    return{jobId:row.id,state:row.state,revision:row.revision,processorAvailability:availability()};
  }
  function consentCurrent(consent){
    check(consent&&consent.expires_at>store.clock(),'ordinary_reporter_consent_required',403);
    const native=db.prepare('SELECT * FROM native_sessions WHERE id_hash=?').get(consent.native_session_hash);
    const source=db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(consent.source_session_hash);
    const member=db.prepare('SELECT * FROM native_memberships WHERE resource_id=? AND principal_id=?').get(resourceId,consent.reporter_id);
    check(native?.active===1&&native.principal_id===consent.reporter_id&&native.generation===consent.native_session_generation&&native.expires_at>store.clock()
      &&source?.active===1&&source.principal_id===consent.reporter_id&&source.native_session_hash===consent.native_session_hash
      &&source.resource_id===resourceId&&source.key_id===consent.source_key_id&&source.expires_at>store.clock()&&member?.active===1&&member.revision===consent.membership_revision,
      'ordinary_reporter_consent_required',403);
    return Math.min(consent.expires_at,native.expires_at,source.expires_at);
  }
  function processorReceipt(jobId){
    const row=db.prepare('SELECT * FROM native_processor_receipts WHERE job_id=?').get(jobId);if(!row)return null;
    const value=store.decrypt('ProcessorReceipt',jobId,0,row.cipher,row.key_id);
    check(digest(value.output)===row.result_digest&&value.inputDigest===row.input_digest,'ordinary_source_storage_corrupt',503);return value;
  }
  function currentGrant(jobId,proof,lifetime=false){
    const authority=feedbackJobAuthority(proof),job=db.prepare('SELECT * FROM native_feedback_jobs WHERE id=?').get(jobId);
    const row=db.prepare('SELECT * FROM native_processor_grants WHERE id=?').get(job?.grant_id);
    check(row?.resource_id===resourceId&&row.source_session_hash===authority.sessionHash,'ordinary_feedback_job_denied',403);
    check(!db.prepare('SELECT 1 FROM native_processor_revocations WHERE grant_id=?').get(row.id),'ordinary_feedback_grant_revoked',403);
    const payload=store.decrypt('ProcessorGrant',row.id,0,row.cipher,row.key_id);
    check(payload.incarnationId===incarnationId&&payload.authorityBindingDigest===authority.bindingDigest
      &&digest(payload.policyRef)===digest(policy.ref)&&registry.has(digest(payload.engineRef)),'source_feedback_job_context_changed',409);
    const actor={principalId:row.owner_id,nativeSessionHash:row.native_session_hash,sourceSessionHash:row.source_session_hash,
      sessionGeneration:payload.ownerSessionGeneration,membershipRevision:payload.membershipRevision};
    const native=current(actor,true),source=db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(actor.sourceSessionHash);
    check(source?.active===1&&source.principal_id===actor.principalId&&source.native_session_hash===actor.nativeSessionHash&&source.resource_id===resourceId
      &&source.key_id===row.key_id&&source.expires_at>store.clock()&&row.expires_at>store.clock()&&authority.expiresAt>store.clock(),'ordinary_feedback_grant_expired',403);
    const ticket=retained(actor,row.ticket_id,payload.request.ticketRevision),consent=db.prepare('SELECT * FROM native_processing_consents WHERE id=?').get(row.consent_id);
    check(consent?.ticket_id===row.ticket_id&&consent.reporter_id===ticket.row.reporter_id&&consent.ticket_revision===payload.request.ticketRevision
      &&consent.attachment_digest===payload.attachmentDigest&&consent.purpose===payload.request.purpose&&consent.policy_digest===digest(policy.ref),'ordinary_reporter_consent_required',403);
    consentCurrent(consent);const intent=feedbackJobIntent(payload.request,ticket.input,policy);check(intent.inputDigest===row.input_digest,'source_feedback_job_context_changed',409);
    if(lifetime)assertFeedbackJobLifetime({now:store.clock(),grantExpiresAt:row.expires_at,sessionExpiresAt:native.session.expires_at,
      keyExpiresAt:source.expires_at,sourceProofExpiresAt:authority.expiresAt},payload.request.budget);
    return{job,row,payload,input:ticket.input,authority};
  }
  function markUnknown(jobId,claimHash){
    store.tx(()=>db.prepare("UPDATE native_feedback_jobs SET state='unknown',revision=revision+1 WHERE id=? AND claim_hash=? AND state='started'").run(jobId,claimHash));
  }
  const service=Object.freeze({
    policy,
    validate(actor,input,requestId){check(store.inTransaction(),'ordinary_native_transaction_required',503);
      if(input.operation==='feedback.processing.consent')return consent(actor,input,false);
      if(input.operation==='feedback.job.grant')return grant(actor,input,requestId,false);
      const args=fields(input,['operation','jobId','expectedRevision']);current(actor,true);const visible=status(actor,args.jobId);
      check(visible.revision===args.expectedRevision,'feedback_revision_conflict',409);return true;},
    execute(actor,input,requestId){check(store.inTransaction(),'ordinary_native_transaction_required',503);current(actor);
      if(input.operation==='feedback.processing.consent')return consent(actor,input);
      if(input.operation==='feedback.job.grant')return grant(actor,input,requestId);
      if(input.operation==='feedback.job.revoke'){const args=fields(input,['operation','jobId','expectedRevision']);current(actor,true);const visible=status(actor,args.jobId);
        check(visible.revision===args.expectedRevision,'feedback_revision_conflict',409);const row=db.prepare('SELECT grant_id,state FROM native_feedback_jobs WHERE id=?').get(args.jobId);
        db.prepare('INSERT OR IGNORE INTO native_processor_revocations VALUES(?,?)').run(row.grant_id,store.clock());
        if(row.state==='queued')db.prepare("UPDATE native_feedback_jobs SET state='revoked',revision=revision+1,finished_at=? WHERE id=? AND state='queued'").run(store.clock(),args.jobId);
        // A started effect stays unknown until the owned runner confirms its
        // outcome/cancellation; revocation never pretends it rolled back.
        if(row.state==='started')db.prepare("UPDATE native_feedback_jobs SET state='unknown',revision=revision+1 WHERE id=? AND state='started'").run(args.jobId);
        // Abort dispatch may invoke an owned executor listener. Keep that
        // callback outside the Native SQL transaction.
        queueMicrotask(()=>running.get(args.jobId)?.abort());
        return status(actor,args.jobId);}
      throw Object.assign(new Error('ordinary_native_operation_denied'),{code:'ordinary_native_operation_denied',status:403});
    },
    read(actor,input){
      if(input.operation==='feedback.processing.context'){
        fields(input,['operation']);const native=current(actor);return{schema:'soty.feedback.processing-context.v1',localOnly:true,
          policyRef:policy.ref,purposes:policy.purposes,requiresReporterConsent:true,canGrant:native.member.role==='owner',
          processorAvailability:availability(),maxBudget:policy.maxBudget,engines:[...registry.values()].map(engine=>({ref:engine.ref,purposes:engine.purposes,synthetic:engine.synthetic}))};
      }
      if(input.operation==='feedback.processing.ticket'){
        const args=fields(input,['operation','ticketId']);const row=db.prepare('SELECT revision FROM native_tickets WHERE id=? AND resource_id=?').get(args.ticketId,resourceId);
        check(row,'ordinary_native_ticket_denied',403);const ticket=retained(actor,args.ticketId,row.revision),native=current(actor);
        return{ticketId:args.ticketId,ticketRevision:row.revision,attachmentDigest:feedbackAttachmentDigest(ticket.input.attachments),policyRef:policy.ref,
          canConsent:ticket.row.reporter_id===actor.principalId,canGrant:native.member.role==='owner',hasImage:ticket.input.attachments.some(item=>item.kind==='image'),hasAudio:ticket.input.attachments.some(item=>item.kind==='audio')};
      }
      const args=fields(input,['operation','jobId']);check(['feedback.job.status','feedback.job.result'].includes(args.operation),'ordinary_native_operation_denied',403);
      const state=status(actor,args.jobId);return args.operation==='feedback.job.result'?{...state,outcome:state.state==='completed'?'committed':state.state==='queued'?'not_applied':'unknown',result:processorReceipt(args.jobId)}:state;
    },
    async process(jobId,{signal}={}){
      check(workerConfigured,'source_feedback_processor_not_ready',503);
      check(executor.syntheticTestOnly||process.platform===executor.platform,'source_feedback_processor_not_ready',503);
      const row=db.prepare('SELECT g.source_session_hash FROM native_feedback_jobs j JOIN native_processor_grants g ON g.id=j.grant_id WHERE j.id=?').get(jobId);
      check(row,'ordinary_feedback_job_denied',403);
      const proof=await currentProof(row.source_session_hash),authority=feedbackJobAuthority(proof);await authority.assertCurrent();
      const admission=currentGrant(jobId,proof);assertFeedbackHostBounds(enforcer,admission.payload.request.budget);
      let claimed;
      store.tx(()=>authority.withCurrent(()=>{
        const value=currentGrant(jobId,proof),prior=processorReceipt(jobId);if(prior){claimed={prior};return;}
        check(value.job.state==='queued','ordinary_feedback_job_outcome_unknown',503);
        check(db.prepare("SELECT count(*) AS n FROM native_feedback_jobs WHERE state IN('started','unknown')").get().n===0,'ordinary_feedback_processor_busy',503);
        currentGrant(jobId,proof,true);check(digest(value.payload.engineRef)===enforcer.engineDigest,'source_feedback_engine_unapproved',503);
        for(const key of Object.keys(value.payload.request.budget))check(value.payload.request.budget[key]<=executor.maxBudget[key],'source_feedback_job_budget_invalid');
        check(!signal?.aborted,'ordinary_feedback_cancelled',409);
        const claimHash=digest(nonce());
        check(db.prepare("UPDATE native_feedback_jobs SET state='started',revision=revision+1,claim_hash=?,started_at=? WHERE id=? AND state='queued'").run(claimHash,store.clock(),jobId).changes===1,'ordinary_feedback_job_outcome_unknown',503);
        claimed={...value,claimHash};
      }));
      if(claimed.prior)return{outcome:'committed',replayed:true,result:claimed.prior};
      const controller=new AbortController(),onAbort=()=>controller.abort();signal?.addEventListener('abort',onAbort,{once:true});running.set(jobId,controller);
      const timer=setTimeout(()=>controller.abort(),claimed.payload.request.budget.wallMs);
      try{
        // Recheck after SQL claim and immediately before the trusted executor.
        // This port has no async work/network inside the Native transaction.
        assertFeedbackHostBounds(enforcer,claimed.payload.request.budget);
        const raw=await executor.execute({engine:registry.get(digest(claimed.payload.engineRef)),input:claimed.input,
          purpose:claimed.payload.request.purpose,budget:claimed.payload.request.budget,signal:controller.signal});
        check(!controller.signal.aborted,'ordinary_feedback_cancelled',409);
        const output=feedbackProcessorOutput(raw,claimed.payload.request.purpose,claimed.payload.request.budget);
        await beforeFinal?.(jobId);
        const fresh=await currentProof(row.source_session_hash),finalAuthority=feedbackJobAuthority(fresh);await finalAuthority.assertCurrent();
        const receipt=store.tx(()=>finalAuthority.withCurrent(()=>{
          const value=currentGrant(jobId,fresh);check(value.job.state==='started'&&value.job.claim_hash===claimed.claimHash,'ordinary_feedback_job_outcome_unknown',503);
          check(!controller.signal.aborted,'ordinary_feedback_cancelled',409);
          const createdAt=store.clock(),record={jobId,inputDigest:value.row.input_digest,output,createdAt,
            provenance:{engineRef:value.payload.engineRef,policyRef:value.payload.policyRef,ticketId:value.row.ticket_id,ticketRevision:value.payload.request.ticketRevision,
              attachmentDigest:value.payload.attachmentDigest,synthetic:value.payload.synthetic,humanReviewRequired:true}};
          db.prepare('INSERT INTO native_processor_receipts VALUES(?,?,?,?,?,?)').run(jobId,record.inputDigest,digest(output),createdAt,
            store.encrypt('ProcessorReceipt',jobId,0,record),value.row.key_id);
          db.prepare("UPDATE native_feedback_jobs SET state='completed',revision=revision+1,finished_at=? WHERE id=? AND claim_hash=? AND state='started'").run(createdAt,jobId,claimed.claimHash);
          return record;
        }));
        await afterFinal?.(jobId);await finalAuthority.assertCurrent();return{outcome:'committed',replayed:false,result:receipt};
      }catch{
        // If Source has shut down, the durable started row is already unknown
        // and never taken over; a closed DB is not a reason to rerun apply.
        try{markUnknown(jobId,claimed.claimHash);}catch{}
        throw Object.assign(new Error('ordinary_feedback_job_outcome_unknown'),{code:'ordinary_feedback_job_outcome_unknown',status:503});
      }finally{clearTimeout(timer);signal?.removeEventListener('abort',onAbort);running.delete(jobId);}
    },
    close(){for(const controller of running.values())controller.abort();},
  });
  services.set(service,{store,resourceId,incarnationId});return service;
}
