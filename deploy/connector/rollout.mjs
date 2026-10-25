import { createHash } from 'node:crypto';
import { installedModuleCandidateBase, installedModuleOriginalForRollout, restoreInstalledModuleOriginal, assertInstalledModuleOriginalProof } from './installed-module-rotation.mjs';
import { SafeError } from './docker-api.mjs';
import {addApprovedPolicy,readApprovedPolicy} from './application-policy.mjs';
import {guardStorageStart, storageReaderLabel, storageReaders} from './storage-guard.mjs';
import { applyUniversalPolicy, publicUniversalPolicy, bindUniversalRollout, assertUniversalPolicyCurrent,
  assertUniversalImagePrerequisites, assertUniversalPreparedness, assertSelectedEmbedFeatureOriginal, universalModeLabel } from './universal-policy.mjs';
import { captureUniversalPreparedness, validateUniversalPreparedness } from '../../modules/app-contract/universal-preparedness.mjs';
const clone=x=>structuredClone(x);
const sorted=x=>Array.isArray(x)?x.map(sorted):x&&typeof x==='object'?Object.fromEntries(Object.keys(x).sort().map(k=>[k,sorted(x[k])])):x;
export const hash=x=>createHash('sha256').update(JSON.stringify(sorted(x))).digest('hex');
const requireThat=(ok,code)=>{if(!ok)throw new SafeError(code);};
const label='io.soty.connector-rollout';
export function createConfig(original,image,tx,revision=original.Config.Labels?.['org.opencontainers.image.revision'],applicationPolicy,universalPolicy,imageLabels) {
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
  // Docker adds image label defaults at CREATE. Include their exact values in
  // the prospective fingerprint; captured container labels still take priority.
  config.Image=image;config.Labels={...imageLabels,...config.Labels,[label]:tx,[label+'.original']:original.Id,...(revision?{'org.opencontainers.image.revision':revision}:{})};
  // Reader metadata must come from the immutable image, never an inherited
  // container label from the previous application.
  delete config.Labels[storageReaderLabel];
  delete config.Labels[universalModeLabel];
  delete config.Labels['io.soty.account-ai'];
  const result={...config,HostConfig:host,NetworkingConfig:{EndpointsConfig:endpoints}};
  addApprovedPolicy(result,applicationPolicy);return universalPolicy?applyUniversalPolicy(result,universalPolicy):result;
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
export function safeLatestDataStatus(s){
  requireThat(s?.schema==='soty.connect.maintenance.v1'&&Object.keys(s).sort().join(',')==='count,maintenance,ok,owned,schema'&&s.ok===true
    &&Number.isSafeInteger(s.count)&&s.count>=0&&typeof s.maintenance==='boolean'&&typeof s.owned==='boolean'
    &&(!s.owned||s.maintenance),'maintenance_status_invalid');
  return {ok:true,schema:s.schema,count:s.count,maintenance:s.maintenance,owned:s.owned};
}
export function safeStatus(s) {requireThat(s?.ok===true&&Number.isSafeInteger(s.count)&&s.count>=0&&Array.isArray(s.activeJobs)&&s.activeJobs.length===s.count&&typeof s.maintenance==='boolean','maintenance_status_invalid');requireThat(s.activeJobs.every(j=>typeof j.id==='string'&&typeof j.status==='string'),'maintenance_jobs_invalid');requireThat(s.queuedSha256==null||(/^[a-f0-9]{64}$/.test(s.queuedSha256)&&s.count>0&&s.activeJobs.every(j=>j.status==='queued')),'maintenance_queued_fingerprint_invalid');return {ok:true,activeJobs:s.activeJobs.map(j=>({id:j.id,status:j.status})),count:s.count,maintenance:s.maintenance,schema:s.schema,queuedSha256:s.queuedSha256||null};}
export function checkedFullEncryptedBackup(value,transaction){
  const exact=(object,keys)=>object&&typeof object==='object'&&!Array.isArray(object)&&Object.keys(object).sort().join(',')===keys;
  const digest=value=>typeof value==='string'&&/^[a-f0-9]{64}$/u.test(value);
  const artifact=value=>exact(value,'bytes,entries,fileBytes,inventorySha256,sha256')&&digest(value.sha256)&&digest(value.inventorySha256)
    &&['bytes','entries','fileBytes'].every(key=>Number.isSafeInteger(value[key])&&value[key]>=0)&&value.bytes>0&&value.entries>0;
  requireThat(exact(value,'configuration,encrypted,ok,operationId,sourceCheckpointSha256,volume')&&value.ok===true&&value.encrypted===true
    &&value.operationId===transaction&&digest(value.sourceCheckpointSha256)&&artifact(value.volume)&&artifact(value.configuration),'full_encrypted_backup_invalid');
  return clone(value);
}
export function checkedOriginalLifecycle(runtime,{stopped=false,expected,finishedAt}={}){
  const s=runtime?.State,started=Date.parse(s?.StartedAt),finished=Date.parse(s?.FinishedAt);
  requireThat(s&&Number.isSafeInteger(runtime.RestartCount)&&runtime.RestartCount>=0&&typeof s.StartedAt==='string'&&started>0
    &&Number.isSafeInteger(s.Pid)&&s.Paused===false&&s.Restarting===false&&s.OOMKilled===false
    &&(stopped?s.Running===false&&s.Status==='exited'&&s.Pid===0&&s.ExitCode===0&&typeof s.FinishedAt==='string'&&finished>=started
      :s.Running===true&&s.Status==='running'&&s.Pid>0),stopped?'original_stop_not_clean':'original_lifecycle_invalid');
  const value={startedAt:s.StartedAt,restartCount:runtime.RestartCount,pid:s.Pid};
  if(expected)requireThat(value.startedAt===expected.startedAt&&value.restartCount===expected.restartCount
    &&(stopped||value.pid===expected.pid),'original_lifecycle_changed');
  if(finishedAt!==undefined)requireThat(s.FinishedAt===finishedAt,'original_lifecycle_changed');
  return {...value,...(stopped?{finishedAt:s.FinishedAt}:{})};
}
export function pendingMatches(status, expected) {return expected ? status.count>0&&status.queuedSha256===expected : status.count===0;}
export class Rollout {
  constructor({engine,maintenance,ready,storageProbe,universalMeasurement,allowUniversalFixture=false,installedModuleRotation,fullEncryptedBackup,record=async()=>{},attempts=4,sleep=ms=>new Promise(r=>setTimeout(r,ms))}){Object.assign(this,{engine,maintenance,ready,storageProbe,universalMeasurement,allowUniversalFixture,installedModuleRotation,fullEncryptedBackup,record,attempts,sleep});this.state={phase:'new'};}
  latestData(){return this.args?.rollbackMode==='latest-data';}
  checkedMaintenance(value){
    const status=this.latestData()?safeLatestDataStatus(value):safeStatus(value);
    if(this.latestData())requireThat(!status.maintenance||status.owned,'maintenance_not_owned');
    return status;
  }
  candidateConfiguration(original){const candidateBase=this.installedModuleRotation?installedModuleCandidateBase(this.installedModuleRotation,original,this.args.universalPolicy,this.args.candidateImage):original;return createConfig(candidateBase,this.args.candidateImage,this.args.transaction,this.args.revision,this.args.applicationPolicy,this.args.universalPolicy,this.candidateImageLabels);}
  async restoreOriginalCode(){if(this.installedModuleRotation)await restoreInstalledModuleOriginal(this.installedModuleRotation);}
  async note(phase,fields={}){this.state={...this.state,...fields,phase};await this.record({...this.state});}
  storageContext(){return {engine:this.engine,probeImage:this.args.storageProbeImage,transactionId:this.args.transaction,getState:()=>this.state,record:fields=>this.note(this.state.phase,fields),probe:this.storageProbe,wait:this.sleep};}
  async guardStart(id,{running=false}={}){return guardStorageStart(this.storageContext(),await this.engine.inspect(id),{running});}
  async restartPolicy(id,policy){
    const runtime=await this.engine.inspect(id);
    const storageGuard=policy.Name!=='no'?await this.guardStart(id,{running:runtime.State.Running}):undefined;
    await this.note(this.state.phase,{restartPolicyIntent:{id,policy,...(storageGuard?{storageGuard}:{})}});
    await this.reconcile(()=>this.engine.request('POST',`/containers/${id}/update`,{RestartPolicy:policy}),id,c=>hash(c.HostConfig.RestartPolicy)===hash(policy));
    if(id===this.original.Id)this.originalPolicySuspended=policy.Name==='no';
    await this.note(this.state.phase,{restartPolicyIntent:null});
  }
  async startApplication(id){
    const storageGuard=await this.guardStart(id);
    if(id===this.candidate?.Id)await this.checkUniversalCurrent();
    else if(this.args.universalPolicy&&id===this.original.Id){await this.checkOriginalPreserved();if(this.installedModuleRotation)await assertInstalledModuleOriginalProof(this.installedModuleRotation);}
    await this.note(this.state.phase,{applicationStart:{id,storageGuard,state:'starting'}});
    await this.reconcile(()=>this.engine.start(id),id,c=>c.State.Running&&c.Image===storageGuard.image);
    await this.note(this.state.phase,{applicationStart:{id,storageGuard,state:'confirmed'}});
  }
  async reconcile(action,id,predicate) {
    let error;try{await action();}catch(e){error=e;}
    for(let i=0;i<this.attempts;i++){try{const c=await this.engine.inspect(id);if(predicate(c))return c;}catch{}await this.sleep(250);}
    throw new SafeError(error?.code==='engine_response_ambiguous'?'operation_unresolved':'operation_failed');
  }
  async guard(args) {
    args={...args};
    requireThat(/^[a-f0-9]{64}$/.test(args.originalId)&&/^sha256:[a-f0-9]{64}$/.test(args.originalImage)&&/^sha256:[a-f0-9]{64}$/.test(args.candidateImage)&&/^[a-f0-9]{40}$/.test(args.revision)&&/^[a-f0-9]{16,40}$/.test(args.transaction),'invalid_exact_guards');
    requireThat(!args.preserveQueuedSha256||/^[a-f0-9]{64}$/.test(args.preserveQueuedSha256),'invalid_queued_fingerprint');
    requireThat(args.rollbackMode===undefined||args.rollbackMode==='latest-data','rollback_mode_invalid');
    if(args.rollbackMode==='latest-data')requireThat(/^[a-f0-9]{32}$/u.test(args.transaction)&&!args.preserveQueuedSha256&&!args.legacyRecovery,'latest_data_mode_invalid');
    let old=await this.engine.inspect(args.originalId);requireThat(old.Id===args.originalId&&old.Image===args.originalImage&&old.Name==='/soty-online-chat','original_identity_mismatch');
    requireThat(old.State.Running,'original_not_running');
    const originalLifecycle=args.rollbackMode==='latest-data'?checkedOriginalLifecycle(old):undefined;
    const image=await this.engine.image(args.candidateImage);requireThat(image.Id===args.candidateImage&&image.Config?.Labels?.['org.opencontainers.image.revision']===args.revision,'candidate_revision_mismatch');
    storageReaders(image);
    // The automatic fallback is this exact old image, not a separately built
    // artifact. Reject a reader-manifest transition before any serving change.
    const oldImage=await this.engine.image(old.Image);requireThat(oldImage.Id===old.Image,'storage_image_identity_invalid');
    storageReaders(oldImage);
    requireThat(/^sha256:[a-f0-9]{64}$/.test(args.storageProbeImage||''),'storage_probe_image_required');
    requireThat((await this.engine.image(args.storageProbeImage)).Id===args.storageProbeImage,'storage_probe_image_identity');
    if(args.applicationPolicy)await readApprovedPolicy(args.applicationPolicy.source,args.applicationPolicy.sha256);
    let universalReceipt;
    if(args.universalPolicy){
      universalReceipt=await assertUniversalPolicyCurrent(args.universalPolicy);
      requireThat(typeof this.allowUniversalFixture==='boolean'&&(!universalReceipt.fixtureOnly||this.allowUniversalFixture),'universal_policy_fixture_not_production');
      requireThat(typeof this.universalMeasurement==='function','universal_measurement_required');
      assertUniversalImagePrerequisites(args.universalPolicy,{candidateImage:image,originalImage:oldImage});
      if(universalReceipt.phase==='features'){
        const observed=validateUniversalPreparedness(await this.universalMeasurement(old.Id,this));
        if(universalReceipt.expectedCurrentPreparednessSha256!==undefined){
          // An explicit features update compares the actual current runtime;
          // it never substitutes the proposed profile for that observation.
          requireThat(observed.compiledLegacyMode===false&&observed.universalConfigured===true
            &&hash(observed)===universalReceipt.expectedCurrentPreparednessSha256,'universal_current_runtime_mismatch');
          assertSelectedEmbedFeatureOriginal(args.universalPolicy,old,observed,{allowFixture:this.allowUniversalFixture});
        }else{
          const expected=captureUniversalPreparedness({compiledLegacyMode:true,universalConfigured:false,reviewsConfigured:false,humanHttpEnabled:false,humanProfile:null});
          requireThat(hash(observed)===hash(expected),'universal_baseline_runtime_mismatch');
        }
      }
      requireThat(args.universalWitnessId===undefined||/^[A-Za-z0-9_-]{32}$/u.test(args.universalWitnessId),'universal_witness_receipt_invalid');
    }else requireThat(args.universalWitnessId===undefined,'universal_witness_receipt_invalid');
    if(this.installedModuleRotation){const measured=old;old=installedModuleOriginalForRollout(this.installedModuleRotation,measured);this.originalPolicySuspended=hash(measured.HostConfig.RestartPolicy)!==hash(old.HostConfig.RestartPolicy);}
    this.args=args;this.selectedEmbedFeature=Boolean(universalReceipt?.featureSelectedEmbed);this.original=old;this.candidateImageLabels=clone(image.Config.Labels);this.originalStopConfirmed=false;this.originalName=old.Name.slice(1);this.config=this.candidateConfiguration(old);this.fingerprint=preservationHash(this.config);
    if(args.universalPolicy)bindUniversalRollout(args.universalPolicy,this.universalBinding(this.fingerprint));
    const baseline=await this.ready('original',this);requireThat(baseline?.ok===true&&baseline.modelProxies,'original_model_readiness_missing');
    this.healthSha256=hash(baseline.modelProxies);
    this.candidateHealthSha256=this.healthSha256;
    if(universalReceipt?.environment.some(item=>item.name==='SOTY_ACCOUNT_AI_CONFIG_FILE')){
      // This explicit, privately witnessed feature changes only the AI
      // transport and routing authority. Model choices and public paths stay
      // subject to the original readiness fence; rollback checks the old hash.
      const expected=clone(baseline.modelProxies);
      expected.agentModelProxy.transport='account-scoped-server-bridge';
      for(const name of ['agentModelProxy','applicationModelProxy'])expected[name].providerStrategy='account-gateway';
      this.candidateHealthSha256=hash(expected);
    }
    this.originalPolicySha256=baseline.applicationPolicySha256||null;
    const configurationSha256=universalReceipt?hash({schema:'soty.universal-public-configuration.v1',originalId:old.Id,originalImage:old.Image,
      candidateImage:image.Id,revision:args.revision,transaction:args.transaction,policyDigest:universalReceipt.policyDigest}):this.fingerprint;
    this.state={phase:'guarded',originalId:old.Id,originalImage:old.Image,candidateImage:image.Id,storageProbeImage:args.storageProbeImage,revision:args.revision,transaction:args.transaction,configurationSha256,modelReadinessSha256:this.healthSha256,applicationPolicySha256:args.applicationPolicy?.sha256||null,preserveQueuedSha256:args.preserveQueuedSha256||null,
      ...(universalReceipt?{universalPolicyDigest:universalReceipt.policyDigest,universalWitnessId:args.universalWitnessId||null,universalPhase:universalReceipt.phase}: {})};
    if(args.legacyRecovery)this.state.legacyRecovery=true;
    if(args.rollbackMode){this.state.rollbackMode=args.rollbackMode;this.state.originalLifecycle=originalLifecycle;}
  }
  universalBinding(preservationFingerprint){return {preservationFingerprint,originalId:this.args.originalId,originalImage:this.args.originalImage,
    candidateImage:this.args.candidateImage,revision:this.args.revision,transaction:this.args.transaction};}
  originalForComparison(current){
    const before=this.original.NetworkSettings?.Networks,after=current.NetworkSettings?.Networks;
    const prior=before?.bridge,next=after?.bridge;
    const names=Object.keys(before||{}).sort(),currentNames=Object.keys(after||{}).sort();
    const empty=value=>value===undefined||value===null||value==='';
    // Observed Docker STOP clears both generated endpoint MACs when the default
    // bridge also has an attached bridge network. Recover only that stopped
    // representation for comparison; all captured network intent stays exact.
    if(!this.originalStopConfirmed||current.Id!==this.original.Id||current.Image!==this.original.Image
      ||current.State.Running||current.State.Paused||current.State.Status!=='exited'
      ||!['bridge','default'].includes(this.original.HostConfig.NetworkMode)
      ||current.HostConfig.NetworkMode!==this.original.HostConfig.NetworkMode
      ||!((empty(this.original.Config.MacAddress)&&empty(current.Config.MacAddress))
        ||(this.original.Config.MacAddress===prior?.MacAddress&&current.Config.MacAddress===prior?.MacAddress))
      ||hash(names)!==hash(currentNames)||!prior||!next
      ||['IPAMConfig','Aliases','Links','DriverOpts'].some(key=>prior[key]!=null||next[key]!=null)
      ||names.some(name=>{
        const previous=before[name],observed=after[name];
        return typeof previous.MacAddress!=='string'||!/^(?:[0-9a-f]{2}:){5}[0-9a-f]{2}$/iu.test(previous.MacAddress)
          ||observed.MacAddress!==''||previous.NetworkID!==observed.NetworkID
          ||['IPAMConfig','Aliases','Links','DriverOpts','GwPriority'].some(key=>hash({value:previous[key]})!==hash({value:observed[key]}));
      }))return current;
    const compared=clone(current);
    for(const name of names)compared.NetworkSettings.Networks[name].MacAddress=before[name].MacAddress;
    return compared;
  }
  async checkUniversalCurrent(){
    if(!this.args.universalPolicy)return;
    await assertUniversalPolicyCurrent(this.args.universalPolicy);
    const candidateImage=await this.engine.image(this.args.candidateImage),originalImage=await this.engine.image(this.args.originalImage);
    requireThat(candidateImage.Id===this.args.candidateImage&&originalImage.Id===this.args.originalImage,'universal_image_identity_mismatch');
    assertUniversalImagePrerequisites(this.args.universalPolicy,{candidateImage,originalImage});
    const old=await this.engine.inspect(this.original.Id);
    requireThat(old.Id===this.args.originalId&&old.Image===this.args.originalImage,'original_identity_mismatch');
    requireThat(hash(old.HostConfig.RestartPolicy)===hash(this.original.HostConfig.RestartPolicy)
      ||this.originalPolicySuspended&&hash(old.HostConfig.RestartPolicy)===hash({Name:'no',MaximumRetryCount:0}),'original_configuration_changed');
    old.HostConfig.RestartPolicy=clone(this.original.HostConfig.RestartPolicy);
    const fingerprint=preservationHash(this.candidateConfiguration(this.originalForComparison(old)));
    bindUniversalRollout(this.args.universalPolicy,this.universalBinding(fingerprint));
    if(this.candidate)this.validateCandidate(await this.engine.inspect(this.candidate.Id));
  }
  async checkOriginalPreserved(){
    if(this.selectedEmbedFeature)await assertUniversalPolicyCurrent(this.args.universalPolicy);
    const current=await this.engine.inspect(this.original.Id);
    requireThat(current.Id===this.original.Id&&current.Image===this.original.Image,'original_identity_mismatch');
    requireThat(hash(current.HostConfig.RestartPolicy)===hash(this.original.HostConfig.RestartPolicy)
      ||this.originalPolicySuspended&&hash(current.HostConfig.RestartPolicy)===hash({Name:'no',MaximumRetryCount:0}),'original_configuration_changed');
    current.HostConfig.RestartPolicy=clone(this.original.HostConfig.RestartPolicy);
    requireThat(preservationHash(createConfig(this.originalForComparison(current),current.Image,this.args.transaction))===preservationHash(createConfig(this.original,this.original.Image,this.args.transaction)),
      'original_configuration_changed');
  }
  validateCandidate(c,{stopped=false}={}) {
    requireThat(c.Id&&c.Image===this.args.candidateImage&&c.Config.Labels?.[label]===this.args.transaction&&c.Config.Labels?.[label+'.original']===this.args.originalId&&c.Config.Labels?.['org.opencontainers.image.revision']===this.args.revision&&(!stopped||!c.State.Running),'candidate_ownership_mismatch');
    requireThat(preservationHash(createConfig(c,this.args.candidateImage,this.args.transaction))===this.fingerprint,'candidate_configuration_mismatch');
  }
  async prepare(args) {
    requireThat(!args.universalPolicy||/^[A-Za-z0-9_-]{32}$/u.test(args.universalWitnessId||''),'universal_witness_receipt_invalid');
    await this.guard(args);await this.note('guarded');const name='soty-connector-next-'+args.transaction;
    await this.checkUniversalCurrent();
    let response;try{response=await this.engine.create(name,this.config);}catch{}
    const c=await this.engine.inspect(response?.Id||name);
    this.validateCandidate(c,{stopped:true});
    this.candidate=c;await this.note('prepared',{candidateId:c.Id});return this.state;
  }
  async status(){return this.checkedMaintenance(await this.maintenance('status',this));}
  async promote() {
    requireThat(this.state.phase==='prepared','candidate_not_prepared');
    requireThat(this.state.rollbackMode===this.args.rollbackMode,'prepared_rollback_mode_mismatch');
    if(this.latestData())requireThat(typeof this.fullEncryptedBackup==='function','full_encrypted_backup_required');
    let stopped=false,entered=false,leaveAttempted=false;
    try{
      this.validateCandidate(await this.engine.inspect(this.candidate.Id),{stopped:true});
      const originalNow=await this.engine.inspect(this.original.Id);
      requireThat(originalNow.Name==='/'+this.originalName&&originalNow.Image===this.args.originalImage&&originalNow.State.Running&&preservationHash(this.candidateConfiguration(originalNow))===this.fingerprint,'original_configuration_changed');
      if(this.latestData())checkedOriginalLifecycle(originalNow,{expected:this.state.originalLifecycle});
      await this.checkUniversalCurrent();
      if(this.args.applicationPolicy)await readApprovedPolicy(this.args.applicationPolicy.source,this.args.applicationPolicy.sha256);
      let s=await this.status();requireThat(pendingMatches(s,this.args.preserveQueuedSha256)&&!s.maintenance,'precheck_not_quiescent');
      await this.guardStart(this.original.Id,{running:true});
      await this.note('stopping_original');
      await this.checkUniversalCurrent();
      await this.reconcile(()=>this.engine.stop(this.original.Id),this.original.Id,c=>!c.State.Running);stopped=true;this.originalStopConfirmed=true;
      // A full coordinated backup must see the stopped writer fenced against
      // daemon restart, before either maintenance admission or a new START.
      if(this.latestData()){
        await this.restartPolicy(this.original.Id,{Name:'no',MaximumRetryCount:0});
        this.stoppedOriginalLifecycle=checkedOriginalLifecycle(await this.engine.inspect(this.original.Id),{stopped:true,expected:this.state.originalLifecycle});
      }
      await this.note('original_stopped',this.latestData()?{stoppedOriginalLifecycle:this.stoppedOriginalLifecycle}:{});
      if(!this.latestData())await this.restartPolicy(this.original.Id,{Name:'no',MaximumRetryCount:0});
      s=await this.status();requireThat(pendingMatches(s,this.args.preserveQueuedSha256),'offline_admission_race');
      if(this.latestData()){
        await this.guardStart(this.original.Id);
        await this.note('backing_up');
        let result;
        try{result=await this.fullEncryptedBackup({containerId:this.original.Id,originalImage:this.original.Image,
          transaction:this.args.transaction,storageProbeImage:this.args.storageProbeImage});}
        catch(error){
          const unresolved=['backup_helper_unresolved','full_encrypted_backup_unresolved','engine_response_ambiguous','ETIMEDOUT','ABORT_ERR'].includes(error?.code)
            ||error?.name==='AbortError';
          throw new SafeError(unresolved?'full_encrypted_backup_unresolved':'full_encrypted_backup_failed');
        }
        const backup=checkedFullEncryptedBackup(result,this.args.transaction);
        await this.checkOriginalPreserved();
        const source=await this.engine.inspect(this.original.Id);
        requireThat(!source.State.Running&&hash(source.HostConfig.RestartPolicy)===hash({Name:'no',MaximumRetryCount:0}),'backup_source_changed');
        checkedOriginalLifecycle(source,{stopped:true,expected:this.state.originalLifecycle,finishedAt:this.stoppedOriginalLifecycle.finishedAt});
        await this.guardStart(this.original.Id);
        await this.note('backed_up',{fullEncryptedBackup:backup});
        s=await this.status();requireThat(s.count===0&&!s.maintenance,'offline_admission_race');
      }
      try{s=this.checkedMaintenance(await this.maintenance('enter',this));}catch(error){if(error.code==='maintenance_helper_unresolved')throw error;s=await this.status();}
      requireThat(pendingMatches(s,this.args.preserveQueuedSha256)&&s.maintenance,'maintenance_enter_failed');entered=true;
      await this.note('maintenance_entered');
      await this.reconcile(()=>this.engine.rename(this.original.Id,'soty-connector-previous-'+this.args.transaction),this.original.Id,c=>c.Name==='/soty-connector-previous-'+this.args.transaction);
      await this.reconcile(()=>this.engine.rename(this.candidate.Id,this.originalName),this.candidate.Id,c=>c.Name==='/'+this.originalName);
      await this.startApplication(this.candidate.Id);
      await this.note('candidate_started');
      const running=await this.engine.inspect(this.candidate.Id);this.validateCandidate(running);
      const ready=await this.ready('candidate',this);requireThat(ready?.ok===true&&ready.storageReady===true&&ready.schema==='soty.connector-storage-ready.v1'&&ready.maintenance===true,'candidate_storage_not_ready');
      requireThat(ready.modelProxies&&hash(ready.modelProxies)===this.candidateHealthSha256,'candidate_model_readiness_changed');
      requireThat((ready.applicationPolicySha256||null)===(this.args.applicationPolicy?.sha256||this.originalPolicySha256),'candidate_loaded_policy_mismatch');
      if(this.args.universalPolicy){
        await this.checkUniversalCurrent();
        const measurement=await this.universalMeasurement(this.candidate.Id,this);
        const receipt=assertUniversalPreparedness(this.args.universalPolicy,measurement,{allowFixture:this.allowUniversalFixture});
        await this.note(this.state.phase,{universalPreparedness:receipt});
      }
      if(this.args.applicationPolicy)await readApprovedPolicy(this.args.applicationPolicy.source,this.args.applicationPolicy.sha256);
      s=await this.status();requireThat(pendingMatches(s,this.args.preserveQueuedSha256)&&s.maintenance,'candidate_pending_jobs_changed');
      await this.note('candidate_ready');
      await this.checkUniversalCurrent();
      leaveAttempted=true;
      try{s=this.checkedMaintenance(await this.maintenance('leave',this));}catch(error){if(error.code==='maintenance_helper_unresolved')throw error;s=await this.status();}
      requireThat(!s.maintenance,'maintenance_leave_unresolved');
      await this.note('committed');return this.state;
    }catch(error){
      if(!this.state.activationFailureCode)await this.note(this.state.phase,{activationFailureCode:error instanceof SafeError&&typeof error.code==='string'&&/^[a-z][a-z0-9_]{0,95}$/u.test(error.code)?error.code:'activation_failed'});
      if(error.code==='maintenance_helper_unresolved'||error.code==='storage_helper_unresolved'
        ||(this.latestData()&&['backup_helper_unresolved','full_encrypted_backup_unresolved'].includes(error.code))
        ||(this.latestData()&&stopped&&['original_stop_not_clean','original_lifecycle_changed','original_lifecycle_invalid'].includes(error.code))
        ||(error.code==='operation_unresolved'&&(this.state.applicationStart?.state==='starting'||this.state.restartPolicyIntent))){await this.note('recovery_required',{failureCode:error.code});throw error;}
      if(leaveAttempted){await this.note('recovery_required',{failureCode:'leave_boundary_ambiguous'});throw new SafeError('leave_boundary_ambiguous');}
      try{
        // No mutation before stop is needed when preflight failed. An ambiguous
        // stop must be inspected before restoration can be claimed.
        const old=await this.engine.inspect(this.original.Id);
        if(!stopped&&old.State.Running){if(error.code==='operation_unresolved')throw new SafeError('stop_outcome_unresolved');await this.restoreOriginalCode();if(this.installedModuleRotation&&this.originalPolicySuspended){await this.checkOriginalPreserved();await this.restartPolicy(this.original.Id,this.original.HostConfig.RestartPolicy);}await this.note('aborted',{failureCode:error.code||'activation_failed'});throw error;}
        const candidate=await this.engine.inspect(this.candidate.Id);
        await this.restartPolicy(candidate.Id,{Name:'no',MaximumRetryCount:0});
        const fenced=this.latestData()?await this.engine.inspect(candidate.Id):candidate;
        if(fenced.State.Running)await this.reconcile(()=>this.engine.stop(candidate.Id),candidate.Id,c=>!c.State.Running);
        if(this.latestData())requireThat(!(await this.engine.inspect(candidate.Id)).State.Running,'candidate_stop_unconfirmed');
        // Refuse before the legacy connector rollback helper can mutate storage.
        await this.restoreOriginalCode();
        if(this.latestData()){
          await this.checkOriginalPreserved();
          checkedOriginalLifecycle(await this.engine.inspect(this.original.Id),{stopped:true,expected:this.state.originalLifecycle,finishedAt:this.stoppedOriginalLifecycle?.finishedAt});
        }
        await this.guardStart(this.original.Id);
        if(this.latestData()){
          let current=await this.status();
          requireThat(current.count===0&&(!current.maintenance||current.owned),'rollback_not_quiescent');
          if(!current.maintenance)current=this.checkedMaintenance(await this.maintenance('enter',this));
          requireThat(current.count===0&&current.maintenance&&current.owned,'rollback_not_quiescent');
          entered=true;
        }else if(entered){
          const current=await this.engine.inspect(this.candidate.Id),state=current.State;
          // A confirmed never-started candidate cannot have changed the storage format.
          const neverStarted=current.Id===this.candidate.Id&&current.Image===this.args.candidateImage&&!this.state.applicationStart
            &&state?.Status==='created'&&state.Running===false&&state.Restarting===false&&state.Pid===0&&state.StartedAt==='0001-01-01T00:00:00Z';
          const back=safeStatus(await this.maintenance(neverStarted?'status':'rollback',this));
          requireThat(pendingMatches(back,this.args.preserveQueuedSha256)&&back.maintenance,'rollback_not_quiescent');
        }
        const latest=await this.engine.inspect(this.candidate.Id);
        if(latest.Name==='/'+this.originalName)await this.reconcile(()=>this.engine.rename(latest.Id,'soty-connector-next-'+this.args.transaction),latest.Id,c=>c.Name==='/soty-connector-next-'+this.args.transaction);
        const prev=await this.engine.inspect(this.original.Id);
        if(prev.Name!=='/'+this.originalName)await this.reconcile(()=>this.engine.rename(prev.Id,this.originalName),prev.Id,c=>c.Name==='/'+this.originalName);
        // Rollback helper leaves the marker in place while restoring compatible
        // JSON. Legacy original does not admit through the new marker contract.
        if(this.args.universalPolicy)await this.checkOriginalPreserved();
        await this.restartPolicy(this.original.Id,this.original.HostConfig.RestartPolicy);
        await this.startApplication(this.original.Id);
        const restoredHealth=await this.ready(this.latestData()?'candidate':'original',this);requireThat(restoredHealth?.ok===true&&restoredHealth.modelProxies&&hash(restoredHealth.modelProxies)===this.healthSha256,'original_health_failed');
        if(this.latestData())requireThat(restoredHealth.storageReady===true&&restoredHealth.schema==='soty.connector-storage-ready.v1'&&restoredHealth.maintenance===true,'original_storage_not_ready');
        if(entered){let cleared;try{cleared=this.checkedMaintenance(await this.maintenance('leave',this));}catch(error){if(error.code==='maintenance_helper_unresolved')throw error;cleared=await this.status();}requireThat(!cleared.maintenance,'restoration_marker_uncleared');}
        await this.note('restored',{failureCode:error.code||'activation_failed'});
      }catch(recovery){if(this.state.phase==='aborted')throw recovery;await this.note('recovery_required',{failureCode:recovery.code||'recovery_failed'});throw new SafeError('recovery_required');}
      throw new SafeError(error.code||'activation_failed');
    }
  }
}
