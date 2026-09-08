import { createHash } from 'node:crypto';
import { SafeError } from './docker-api.mjs';
import {addApprovedPolicy,readApprovedPolicy} from './application-policy.mjs';
const clone=x=>structuredClone(x);
const sorted=x=>Array.isArray(x)?x.map(sorted):x&&typeof x==='object'?Object.fromEntries(Object.keys(x).sort().map(k=>[k,sorted(x[k])])):x;
export const hash=x=>createHash('sha256').update(JSON.stringify(sorted(x))).digest('hex');
const requireThat=(ok,code)=>{if(!ok)throw new SafeError(code);};
const label='io.soty.connector-rollout';
export function createConfig(original,image,tx,revision=original.Config.Labels?.['org.opencontainers.image.revision'],applicationPolicy) {
  const config=clone(original.Config),host=clone(original.HostConfig),endpoints={};
  // Inspect includes realised anonymous volume names. Reattach those explicitly;
  // replaying Config.Volumes alone would silently create fresh empty volumes.
  for(const m of original.Mounts||[])if(m.Type==='volume'&&!host.Binds?.some(b=>b.split(':')[1]===m.Destination)&&!host.Mounts?.some(b=>b.Target===m.Destination)) {
    host.Mounts||=[];host.Mounts.push({Type:'volume',Source:m.Name,Target:m.Destination,ReadOnly:!m.RW,VolumeOptions:{NoCopy:true}});
  }
  for(const [name,n] of Object.entries(original.NetworkSettings?.Networks||{})) {
    endpoints[name]={};for(const k of ['IPAMConfig','Links','Aliases','DriverOpts','GwPriority','MacAddress'])if(n[k]!==undefined&&n[k]!==null)endpoints[name][k]=clone(n[k]);
  }
  requireThat(!host.AutoRemove,'original_auto_remove_unsupported');
  requireThat(!String(host.NetworkMode||'').startsWith('container:'),'shared_container_network_unsupported');
  config.Image=image;config.Labels={...config.Labels,[label]:tx,[label+'.original']:original.Id,...(revision?{'org.opencontainers.image.revision':revision}:{})};
  const result={...config,HostConfig:host,NetworkingConfig:{EndpointsConfig:endpoints}};
  addApprovedPolicy(result,applicationPolicy);return result;
}
export function preservationHash(config) {
  const c=clone(config);delete c.Image;
  if(c.Labels){delete c.Labels[label];delete c.Labels[label+'.original'];delete c.Labels['org.opencontainers.image.revision'];}
  // Proven Docker API1.45 roundtrip defaults: null means OOM kill enabled,
  // and an explicitly supplied primary endpoint MAC is also reflected into
  // the deprecated top-level field. Preserve disagreement/nondefault values.
  c.HostConfig.OomKillDisable ??= false;
  const primary=c.NetworkingConfig?.EndpointsConfig?.[c.HostConfig.NetworkMode==='default'?'bridge':c.HostConfig.NetworkMode];
  if(!c.MacAddress&&primary?.MacAddress)c.MacAddress=primary.MacAddress;
  return hash(c);
}
export function safeAuthority(a) {
  const sha=x=>/^[a-f0-9]{64}$/.test(x||'');
  requireThat(a?.schema==='soty.connector-authority.v1'&&['legacy','sqlite'].includes(a.kind)&&sha(a.stateSha256)&&sha(a.sourceSha256)&&(a.legacySha256===null||sha(a.legacySha256))&&Number.isSafeInteger(a.bytes)&&a.bytes>0,'authority_receipt_invalid');
  const counts={};for(const name of ['connectors','accessGrants','jobs','requests','events']){requireThat(Number.isSafeInteger(a.counts?.[name])&&a.counts[name]>=0,'authority_counts_invalid');counts[name]=a.counts[name];}
  requireThat(a.statusCounts&&Object.values(a.statusCounts).every(n=>Number.isSafeInteger(n)&&n>=0)&&Object.values(a.statusCounts).reduce((sum,n)=>sum+n,0)===counts.jobs,'authority_status_counts_invalid');
  requireThat(Array.isArray(a.activeJobs)&&a.activeJobs.every(j=>typeof j.id==='string'&&typeof j.status==='string'),'authority_jobs_invalid');
  requireThat(Object.entries(a.statusCounts).filter(([status])=>!['succeeded','failed','cancelled'].includes(status)).reduce((sum,[,count])=>sum+count,0)===a.activeJobs.length,'authority_nonterminal_count_invalid');
  if(a.kind==='legacy')requireThat(a.legacySha256===a.sourceSha256,'authority_legacy_hash_invalid');
  requireThat(Array.isArray(a.temporaryFiles)&&a.temporaryFiles.length<=32&&a.temporaryFiles.every(f=>/^connector-store\.json\.[a-zA-Z0-9-]+\.next$/.test(f.name)&&Number.isSafeInteger(f.bytes)&&f.bytes>=0&&sha(f.sha256)),'authority_temporary_files_invalid');
  return {schema:a.schema,kind:a.kind,stateSha256:a.stateSha256,sourceSha256:a.sourceSha256,legacySha256:a.legacySha256,bytes:a.bytes,counts,statusCounts:{...a.statusCounts},activeJobs:a.activeJobs.map(j=>({id:j.id,status:j.status})),temporaryFiles:a.temporaryFiles.map(f=>({name:f.name,bytes:f.bytes,sha256:f.sha256}))};
}
export function safeStatus(s) {requireThat(s?.ok===true&&Number.isSafeInteger(s.count)&&s.count>=0&&Array.isArray(s.activeJobs)&&s.activeJobs.length===s.count&&typeof s.maintenance==='boolean','maintenance_status_invalid');requireThat(s.activeJobs.every(j=>typeof j.id==='string'&&typeof j.status==='string'),'maintenance_jobs_invalid');return {ok:true,activeJobs:s.activeJobs.map(j=>({id:j.id,status:j.status})),count:s.count,maintenance:s.maintenance,schema:s.schema,...(s.authority?{authority:safeAuthority(s.authority)}:{})};}
function sameAuthority(expected,current,kind) {
  requireThat(current?.kind===kind&&current.stateSha256===expected.stateSha256&&hash(current.counts)===hash(expected.counts)&&hash(current.temporaryFiles)===hash(expected.temporaryFiles)&&current.activeJobs.length===0,'offline_authority_changed');
  if(kind==='sqlite')requireThat(current.legacySha256===expected.sourceSha256,'import_authority_mismatch');
}
export class Rollout {
  constructor({engine,maintenance,ready,record=async()=>{},attempts=4,sleep=ms=>new Promise(r=>setTimeout(r,ms))}){Object.assign(this,{engine,maintenance,ready,record,attempts,sleep});this.state={phase:'new'};}
  async note(phase,fields={}){this.state={...this.state,...fields,phase};await this.record({...this.state});}
  async reconcile(action,id,predicate,{polls=this.attempts,observationMs=Infinity}={}) {
    let error;try{await action();}catch(e){error=e;}
    const deadline=performance.now()+observationMs;
    for(let i=0;i<polls&&performance.now()<deadline;i++){try{const c=await this.engine.inspect(id);if(predicate(c))return c;}catch{}await this.sleep(250);}
    throw new SafeError(error?.code==='engine_response_ambiguous'?'operation_unresolved':'operation_failed');
  }
  async guard(args) {
    requireThat(/^[a-f0-9]{64}$/.test(args.originalId)&&/^sha256:[a-f0-9]{64}$/.test(args.originalImage)&&/^sha256:[a-f0-9]{64}$/.test(args.candidateImage)&&/^[a-f0-9]{40}$/.test(args.revision)&&/^[a-f0-9]{16,40}$/.test(args.transaction),'invalid_exact_guards');
    const old=await this.engine.inspect(args.originalId);requireThat(old.Id===args.originalId&&old.Image===args.originalImage&&old.Name==='/soty-online-chat','original_identity_mismatch');
    requireThat(old.State.Running,'original_not_running');
    const image=await this.engine.image(args.candidateImage);requireThat(image.Id===args.candidateImage&&image.Config?.Labels?.['org.opencontainers.image.revision']===args.revision,'candidate_revision_mismatch');
    if(args.applicationPolicy)await readApprovedPolicy(args.applicationPolicy.source,args.applicationPolicy.sha256);
    this.args=args;this.original=old;this.originalName=old.Name.slice(1);this.config=createConfig(old,args.candidateImage,args.transaction,args.revision,args.applicationPolicy);this.fingerprint=preservationHash(this.config);
    const baseline=await this.ready('original',this);requireThat(baseline?.ok===true&&baseline.modelProxies,'original_model_readiness_missing');
    this.healthSha256=hash(baseline.modelProxies);
    this.originalPolicySha256=baseline.applicationPolicySha256||null;
    this.state={phase:'guarded',originalId:old.Id,originalImage:old.Image,candidateImage:image.Id,revision:args.revision,transaction:args.transaction,configurationSha256:this.fingerprint,modelReadinessSha256:this.healthSha256,applicationPolicySha256:args.applicationPolicy?.sha256||null};
  }
  validateCandidate(c,{stopped=false}={}) {
    requireThat(c.Id&&c.Image===this.args.candidateImage&&c.Config.Labels?.[label]===this.args.transaction&&c.Config.Labels?.[label+'.original']===this.args.originalId&&c.Config.Labels?.['org.opencontainers.image.revision']===this.args.revision&&(!stopped||!c.State.Running),'candidate_ownership_mismatch');
    requireThat(preservationHash(createConfig(c,this.args.candidateImage,this.args.transaction))===this.fingerprint,'candidate_configuration_mismatch');
  }
  async prepare(args) {
    await this.guard(args);await this.note('guarded');const name='soty-connector-next-'+args.transaction;
    let response;try{response=await this.engine.create(name,this.config);}catch{}
    const c=await this.engine.inspect(response?.Id||name);
    this.validateCandidate(c,{stopped:true});
    this.candidate=c;await this.note('prepared',{candidateId:c.Id});return this.state;
  }
  async checkStoppedResume() {
    const old=await this.engine.inspect(this.args.originalId),candidate=await this.engine.inspect(this.resumeReceipt.candidateId);
    const stop=this.resumeReceipt.originalStop;
    const helpers=await this.engine.helpers(this.args.transaction);requireThat(Array.isArray(helpers)&&helpers.length===0,'resume_helper_unresolved');
    requireThat(old.Id===this.args.originalId&&old.Image===this.args.originalImage&&old.Name==='/soty-online-chat'&&!old.State.Running&&old.State.Status==='exited'&&old.State.FinishedAt===stop.finishedAt&&old.State.ExitCode===stop.exitCode&&!old.State.OOMKilled,'resume_original_changed');
    requireThat(candidate.Id===this.resumeReceipt.candidateId&&candidate.Name==='/soty-connector-next-'+this.args.transaction&&candidate.State.Status==='created'&&!candidate.State.Running&&candidate.State.StartedAt==='0001-01-01T00:00:00Z','resume_candidate_started');
    this.validateCandidate(candidate,{stopped:true});
    const reflected=createConfig(old,this.args.candidateImage,this.args.transaction,this.args.revision,this.args.applicationPolicy);
    const reviewed=createConfig(candidate,this.args.candidateImage,this.args.transaction);
    // Docker clears runtime endpoint MAC on STOP. Only fill an empty MAC from
    // the exact never-started candidate whose complete prepared hash passed.
    for(const [network,endpoint] of Object.entries(reflected.NetworkingConfig.EndpointsConfig)) {
      if(endpoint.MacAddress==='') {
        const mac=reviewed.NetworkingConfig.EndpointsConfig[network]?.MacAddress;
        requireThat(typeof mac==='string'&&/^([0-9a-f]{2}:){5}[0-9a-f]{2}$/i.test(mac),'resume_empty_mac_unproven');
        endpoint.MacAddress=mac;
      }
    }
    requireThat(preservationHash(reflected)===this.fingerprint,'resume_original_configuration_changed');
    if(this.args.applicationPolicy)await readApprovedPolicy(this.args.applicationPolicy.source,this.args.applicationPolicy.sha256);
    this.original=old;this.candidate=candidate;this.config=reflected;
  }
  async resumeAfterStop(args,failed,prepared,approval,binding) {
    requireThat(/^[a-f0-9]{64}$/.test(args.originalId)&&/^sha256:[a-f0-9]{64}$/.test(args.originalImage)&&/^sha256:[a-f0-9]{64}$/.test(args.candidateImage)&&/^[a-f0-9]{40}$/.test(args.revision)&&/^[a-f0-9]{16,40}$/.test(args.transaction),'invalid_exact_guards');
    requireThat(approval?.schema==='soty.controller-stop-resume.v1'&&approval.approved===true&&approval.migrationContract==='soty.stopped-legacy-snapshot.v1','resume_receipt_invalid');
    requireThat(binding&&/^[a-f0-9]{64}$/.test(binding.failedJournalSha256)&&/^[a-f0-9]{64}$/.test(binding.preparedJournalSha256)&&approval.failedJournalSha256===binding.failedJournalSha256&&approval.preparedJournalSha256===binding.preparedJournalSha256,'resume_journal_binding');
    requireThat(failed.phase==='recovery_required'&&failed.failureCode==='offline_authority_unresolved'&&prepared.phase==='prepared'&&!failed.originalStop&&!failed.offlineAuthority&&!failed.migrationContract,'resume_boundary_invalid');
    const helper=failed.maintenanceHelper;
    requireThat(helper?.verb==='status'&&helper.state==='removed'&&helper.name===`soty-connector-helper-${args.transaction}-1`&&/^[a-f0-9]{64}$/.test(helper.id),'resume_helper_unresolved');
    for(const key of ['originalId','originalImage','candidateImage','revision','transaction'])requireThat(args[key]===failed[key]&&args[key]===prepared[key]&&args[key]===approval[key],'resume_identity_mismatch');
    for(const key of ['candidateId','configurationSha256','modelReadinessSha256','applicationPolicySha256'])requireThat(failed[key]===prepared[key]&&failed[key]===approval[key],'resume_receipt_mismatch');
    requireThat(/^[a-f0-9]{64}$/.test(approval.candidateId)&&/^[a-f0-9]{64}$/.test(approval.configurationSha256)&&/^[a-f0-9]{64}$/.test(approval.modelReadinessSha256),'resume_hash_invalid');
    requireThat((args.applicationPolicy?.sha256||null)===approval.applicationPolicySha256&&(approval.originalPolicySha256===null||/^[a-f0-9]{64}$/.test(approval.originalPolicySha256||'')),'resume_policy_mismatch');
    requireThat(typeof approval.originalStop?.finishedAt==='string'&&Number.isFinite(Date.parse(approval.originalStop.finishedAt))&&[0,137,143].includes(approval.originalStop.exitCode),'resume_stop_proof_invalid');
    const expected=safeAuthority(approval.expectedAuthority);
    requireThat(expected.kind==='legacy'&&expected.counts.requests===0&&expected.activeJobs.length===0,'resume_expected_authority_invalid');
    const image=await this.engine.image(args.candidateImage);requireThat(image.Id===args.candidateImage&&image.Config?.Labels?.['org.opencontainers.image.revision']===args.revision,'candidate_revision_mismatch');
    this.args=args;this.resumeReceipt={...approval,expectedAuthority:expected};this.originalName='soty-online-chat';this.fingerprint=approval.configurationSha256;this.healthSha256=approval.modelReadinessSha256;this.originalPolicySha256=approval.originalPolicySha256;
    await this.checkStoppedResume();
    this.state={...failed,resumeFrom:{...binding,phase:failed.phase,failureCode:failed.failureCode},failureCode:null};
    await this.note('stop_resume_validated',{originalStop:approval.originalStop});
    return this.continuePromotion(true);
  }
  async status(){return safeStatus(await this.maintenance('status',this));}
  async promote() {return this.continuePromotion(false);}
  async continuePromotion(resuming) {
    requireThat(this.state.phase===(resuming?'stop_resume_validated':'prepared'),'candidate_not_prepared');
    let stopped=resuming,entered=false,leaveAttempted=false,offlineAuthority;
    try{
      this.validateCandidate(await this.engine.inspect(this.candidate.Id),{stopped:true});
      if(!resuming){
      const originalNow=await this.engine.inspect(this.original.Id);
      requireThat(originalNow.Name==='/'+this.originalName&&originalNow.Image===this.args.originalImage&&originalNow.State.Running&&preservationHash(createConfig(originalNow,this.args.candidateImage,this.args.transaction,this.args.revision,this.args.applicationPolicy))===this.fingerprint,'original_configuration_changed');
      if(this.args.applicationPolicy)await readApprovedPolicy(this.args.applicationPolicy.source,this.args.applicationPolicy.sha256);
      let s=await this.status();requireThat(s.count===0&&!s.maintenance,'precheck_not_quiescent');
      await this.note('stopping_original');
      let stoppedOriginal;
      try{stoppedOriginal=await this.reconcile(()=>this.engine.stop(this.original.Id),this.original.Id,c=>!c.State.Running,{polls:60,observationMs:15000});}catch(error){await this.note('stopping_original',{stopReconciliation:{code:error.code||'operation_failed'}});throw error;}stopped=true;
      await this.note('original_stopped',{originalStop:{exitCode:Number.isInteger(stoppedOriginal.State.ExitCode)?stoppedOriginal.State.ExitCode:null,oomKilled:stoppedOriginal.State.OOMKilled===true,finishedAt:stoppedOriginal.State.FinishedAt||null}});
      }else{
        await this.checkStoppedResume();
      }
      // The stopped exact legacy process is the admission/write barrier. Its
      // successful mutation ACK and delivered poll followed atomic rename;
      // a live queue-empty metric neither exists nor establishes that fact.
      let s=safeStatus(await this.maintenance('snapshot',this));
      requireThat(s.count===0&&s.authority?.activeJobs.length===0,'offline_admission_race');
      requireThat(!s.maintenance&&s.authority.kind==='legacy'&&s.authority.counts.requests===0,'offline_legacy_authority_required');
      if(resuming)requireThat(hash(s.authority)===hash(this.resumeReceipt.expectedAuthority),'resume_authority_changed');
      offlineAuthority=s.authority;
      await this.note('offline_authority_verified',{migrationContract:'soty.stopped-legacy-snapshot.v1',offlineAuthority});
      try{s=safeStatus(await this.maintenance('enter',this));}catch(error){if(error.code==='maintenance_helper_unresolved')throw error;s=safeStatus(await this.maintenance('verify',this));}
      requireThat(s.count===0&&s.maintenance,'maintenance_enter_failed');entered=true;
      sameAuthority(offlineAuthority,s.authority,'legacy');
      requireThat(s.authority.sourceSha256===offlineAuthority.sourceSha256,'offline_source_changed');
      await this.note('maintenance_entered');
      await this.reconcile(()=>this.engine.rename(this.original.Id,'soty-connector-previous-'+this.args.transaction),this.original.Id,c=>c.Name==='/soty-connector-previous-'+this.args.transaction);
      await this.reconcile(()=>this.engine.rename(this.candidate.Id,this.originalName),this.candidate.Id,c=>c.Name==='/'+this.originalName);
      await this.reconcile(()=>this.engine.start(this.candidate.Id),this.candidate.Id,c=>c.State.Running);
      await this.note('candidate_started');
      const running=await this.engine.inspect(this.candidate.Id);this.validateCandidate(running);
      const ready=await this.ready('candidate',this);requireThat(ready?.ok===true&&ready.storageReady===true&&ready.schema==='soty.connector-storage-ready.v1'&&ready.maintenance===true,'candidate_storage_not_ready');
      requireThat(ready.modelProxies&&hash(ready.modelProxies)===this.healthSha256,'candidate_model_readiness_changed');
      requireThat((ready.applicationPolicySha256||null)===(this.args.applicationPolicy?.sha256||this.originalPolicySha256),'candidate_loaded_policy_mismatch');
      if(this.args.applicationPolicy)await readApprovedPolicy(this.args.applicationPolicy.source,this.args.applicationPolicy.sha256);
      s=safeStatus(await this.maintenance('verify',this));
      requireThat(s.maintenance&&s.count===0,'candidate_barrier_changed');
      sameAuthority(offlineAuthority,s.authority,'sqlite');
      await this.note('candidate_authority_verified',{migratedAuthority:s.authority});
      await this.note('candidate_ready');leaveAttempted=true;
      try{s=safeStatus(await this.maintenance('leave',this));}catch(error){if(error.code==='maintenance_helper_unresolved')throw error;s=await this.status();}
      requireThat(!s.maintenance,'maintenance_leave_unresolved');
      await this.note('committed');return this.state;
    }catch(error){
      if(error.code==='maintenance_helper_unresolved'){await this.note('recovery_required',{failureCode:error.code});throw error;}
      if(leaveAttempted){await this.note('recovery_required',{failureCode:'leave_boundary_ambiguous'});throw new SafeError('leave_boundary_ambiguous');}
      try{
        // No mutation before stop is needed when preflight failed. An ambiguous
        // stop must be inspected before restoration can be claimed.
        const old=await this.engine.inspect(this.original.Id);
        if(!stopped&&old.State.Running){if(error.code==='operation_unresolved')throw new SafeError('stop_outcome_unresolved');await this.note('aborted',{failureCode:error.code||'activation_failed'});throw error;}
        // Do not restart c0ca with an unclassified/assigned snapshot: its lease
        // expiry can automatically reoffer an already executing old job.
        requireThat(offlineAuthority,'offline_authority_unresolved');
        // c0ca always reuses connector-store.json.<pid>.next. A restored PID1
        // could overwrite retained unacknowledged evidence on its heartbeat.
        // Keep both authorities fenced for explicit recovery in that case.
        requireThat(offlineAuthority.temporaryFiles.length===0,'legacy_restart_would_overwrite_unacknowledged_evidence');
        const candidate=await this.engine.inspect(this.candidate.Id);
        if(candidate.State.Running)await this.reconcile(()=>this.engine.stop(candidate.Id),candidate.Id,c=>!c.State.Running);
        if(entered){const back=safeStatus(await this.maintenance('rollback',this));requireThat(back.count===0&&back.maintenance,'rollback_not_quiescent');}
        const latest=await this.engine.inspect(this.candidate.Id);
        if(latest.Name==='/'+this.originalName)await this.reconcile(()=>this.engine.rename(latest.Id,'soty-connector-next-'+this.args.transaction),latest.Id,c=>c.Name==='/soty-connector-next-'+this.args.transaction);
        const prev=await this.engine.inspect(this.original.Id);
        if(prev.Name!=='/'+this.originalName)await this.reconcile(()=>this.engine.rename(prev.Id,this.originalName),prev.Id,c=>c.Name==='/'+this.originalName);
        const restored=safeStatus(await this.maintenance('snapshot',this));
        requireThat(restored.count===0,'restoration_active_jobs');
        sameAuthority(offlineAuthority,restored.authority,'legacy');
        await this.note('restoration_authority_verified',{restoredAuthority:restored.authority});
        // Clear the verified marker while BOTH servers are stopped. Starting
        // c0ca first would let heartbeats mutate the state before final proof.
        if(restored.maintenance){let cleared;try{cleared=safeStatus(await this.maintenance('leave',this));}catch(error){if(error.code==='maintenance_helper_unresolved')throw error;cleared=await this.status();}requireThat(!cleared.maintenance,'restoration_marker_uncleared');}
        await this.reconcile(()=>this.engine.start(this.original.Id),this.original.Id,c=>c.State.Running&&c.Image===this.args.originalImage);
        const restoredHealth=await this.ready('original',this);requireThat(restoredHealth?.ok===true&&restoredHealth.modelProxies&&hash(restoredHealth.modelProxies)===this.healthSha256,'original_health_failed');
        await this.note('restored',{failureCode:error.code||'activation_failed'});
      }catch(recovery){if(this.state.phase==='aborted')throw recovery;await this.note('recovery_required',{failureCode:recovery.code||'recovery_failed'});throw new SafeError('recovery_required');}
      throw new SafeError(error.code||'activation_failed');
    }
  }
}
