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
export function safeStatus(s) {requireThat(s?.ok===true&&Number.isSafeInteger(s.count)&&s.count>=0&&Array.isArray(s.activeJobs)&&s.activeJobs.length===s.count&&typeof s.maintenance==='boolean','maintenance_status_invalid');requireThat(s.activeJobs.every(j=>typeof j.id==='string'&&typeof j.status==='string'),'maintenance_jobs_invalid');return {ok:true,activeJobs:s.activeJobs.map(j=>({id:j.id,status:j.status})),count:s.count,maintenance:s.maintenance,schema:s.schema};}
export class Rollout {
  constructor({engine,maintenance,ready,record=async()=>{},attempts=4,sleep=ms=>new Promise(r=>setTimeout(r,ms))}){Object.assign(this,{engine,maintenance,ready,record,attempts,sleep});this.state={phase:'new'};}
  async note(phase,fields={}){this.state={...this.state,...fields,phase};await this.record({...this.state});}
  async reconcile(action,id,predicate) {
    let error;try{await action();}catch(e){error=e;}
    for(let i=0;i<this.attempts;i++){try{const c=await this.engine.inspect(id);if(predicate(c))return c;}catch{}await this.sleep(250);}
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
  async status(){return safeStatus(await this.maintenance('status',this));}
  async promote() {
    requireThat(this.state.phase==='prepared','candidate_not_prepared');
    let stopped=false,entered=false,leaveAttempted=false;
    try{
      this.validateCandidate(await this.engine.inspect(this.candidate.Id),{stopped:true});
      const originalNow=await this.engine.inspect(this.original.Id);
      requireThat(originalNow.Name==='/'+this.originalName&&originalNow.Image===this.args.originalImage&&originalNow.State.Running&&preservationHash(createConfig(originalNow,this.args.candidateImage,this.args.transaction,this.args.revision,this.args.applicationPolicy))===this.fingerprint,'original_configuration_changed');
      if(this.args.applicationPolicy)await readApprovedPolicy(this.args.applicationPolicy.source,this.args.applicationPolicy.sha256);
      let s=await this.status();requireThat(s.count===0&&!s.maintenance,'precheck_not_quiescent');
      await this.note('stopping_original');
      await this.reconcile(()=>this.engine.stop(this.original.Id),this.original.Id,c=>!c.State.Running);stopped=true;
      await this.note('original_stopped');
      s=await this.status();requireThat(s.count===0,'offline_admission_race');
      try{s=safeStatus(await this.maintenance('enter',this));}catch(error){if(error.code==='maintenance_helper_unresolved')throw error;s=await this.status();}
      requireThat(s.count===0&&s.maintenance,'maintenance_enter_failed');entered=true;
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
        const candidate=await this.engine.inspect(this.candidate.Id);
        if(candidate.State.Running)await this.reconcile(()=>this.engine.stop(candidate.Id),candidate.Id,c=>!c.State.Running);
        if(entered){const back=safeStatus(await this.maintenance('rollback',this));requireThat(back.count===0&&back.maintenance,'rollback_not_quiescent');}
        const latest=await this.engine.inspect(this.candidate.Id);
        if(latest.Name==='/'+this.originalName)await this.reconcile(()=>this.engine.rename(latest.Id,'soty-connector-next-'+this.args.transaction),latest.Id,c=>c.Name==='/soty-connector-next-'+this.args.transaction);
        const prev=await this.engine.inspect(this.original.Id);
        if(prev.Name!=='/'+this.originalName)await this.reconcile(()=>this.engine.rename(prev.Id,this.originalName),prev.Id,c=>c.Name==='/'+this.originalName);
        // Rollback helper leaves the marker in place while restoring compatible
        // JSON. Legacy original does not admit through the new marker contract.
        await this.reconcile(()=>this.engine.start(this.original.Id),this.original.Id,c=>c.State.Running&&c.Image===this.args.originalImage);
        const restoredHealth=await this.ready('original',this);requireThat(restoredHealth?.ok===true&&restoredHealth.modelProxies&&hash(restoredHealth.modelProxies)===this.healthSha256,'original_health_failed');
        if(entered){let cleared;try{cleared=safeStatus(await this.maintenance('leave',this));}catch(error){if(error.code==='maintenance_helper_unresolved')throw error;cleared=await this.status();}requireThat(!cleared.maintenance,'restoration_marker_uncleared');}
        await this.note('restored',{failureCode:error.code||'activation_failed'});
      }catch(recovery){if(this.state.phase==='aborted')throw recovery;await this.note('recovery_required',{failureCode:recovery.code||'recovery_failed'});throw new SafeError('recovery_required');}
      throw new SafeError(error.code||'activation_failed');
    }
  }
}
