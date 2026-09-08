import { SafeError,httpJson } from './docker-api.mjs';
import { createConfig,safeStatus } from './rollout.mjs';
const label='io.soty.connector-rollout';
export function productionMaintenance(engine,{maxPolls=60,sleep=ms=>new Promise(r=>setTimeout(r,ms))}={}) {
 let sequence=0;
 return async (verb,run)=>{
   if(!['status','enter','leave','rollback'].includes(verb))throw new SafeError('maintenance_verb_invalid');
   const name=`soty-connector-helper-${run.args.transaction}-${++sequence}`;
   const source=createConfig(run.original,run.args.candidateImage,run.args.transaction);
   // Existing mounts/Env stay memory-only. Helper has no published port, network,
   // restart policy, devices/capabilities, or original traffic entrypoint.
   const body={Image:run.args.candidateImage,Env:source.Env,User:source.User,WorkingDir:source.WorkingDir||'/app',Entrypoint:['node'],Cmd:['server/connector-maintenance.js',verb],Tty:false,Labels:{[label]:run.args.transaction,[label+'.helper']:verb},HostConfig:{Binds:source.HostConfig.Binds,Mounts:source.HostConfig.Mounts,GroupAdd:source.HostConfig.GroupAdd,UsernsMode:source.HostConfig.UsernsMode,NetworkMode:'none',RestartPolicy:{Name:'no'},ReadonlyRootfs:true,Memory:805306368,NanoCpus:500000000,PidsLimit:64,CapDrop:['ALL'],SecurityOpt:['no-new-privileges'],Tmpfs:{'/tmp':'rw,noexec,nosuid,size=16777216'}},NetworkingConfig:{EndpointsConfig:{}}};
   let created;try{created=await engine.create(name,body);}catch{}
   const owned=await engine.inspect(created?.Id||name);if(owned.Image!==run.args.candidateImage||owned.Config.Labels?.[label]!==run.args.transaction||owned.Config.Labels?.[label+'.helper']!==verb)throw new SafeError('maintenance_helper_identity');
   try{await engine.start(owned.Id);}catch{}
   let final;
   try{for(let i=0;i<maxPolls;i++){final=await engine.inspect(owned.Id);if(!final.State.Running&&['exited','dead'].includes(final.State.Status))break;await sleep(250);}}catch{throw new SafeError('maintenance_helper_unresolved');}
   if(final?.State.Running||final?.State.Status!=='exited')throw new SafeError('maintenance_helper_unresolved');
   if(final.State.ExitCode!==0)throw new SafeError('maintenance_helper_failed');
   const output=await engine.helperOutput(owned.Id);
   if(verb==='rollback'&&(output?.rollback!=='legacy-json'||output?.count!==0||!Number.isSafeInteger(output?.bytes)||!/^[a-f0-9]{64}$/.test(output?.sha256||'')))throw new SafeError('rollback_receipt_invalid');
   const result=safeStatus(verb==='rollback'?{...output,activeJobs:[]}:output);
   // Failed/ambiguous helper is retained for supervised ID-specific inspection;
   // successful helper can be removed without volumes and without force.
   await engine.remove(owned.Id);return result;
 };
}
export function modelReadiness(health) {
 const fields={};
 for(const name of ['agentModelProxy','applicationModelProxy']){
  const p=health?.[name];
  if(p?.ready!==true||typeof p.model!=='string'||!p.model||typeof p.transport!=='string'||!p.transport||(name==='applicationModelProxy'&&p.path!=='/api/inference/v1/chat/completions'))throw new SafeError('model_readiness_failed');
  fields[name]={ready:p.ready,model:p.model,transport:p.transport,...(name==='applicationModelProxy'?{path:p.path}:{})};
 }
 return fields;
}
export function readiness(origin) {return async kind=>{
 const deadline=Date.now()+60000;
 while(Date.now()<deadline){
  try{
   const health=await httpJson(origin,'/health',Math.min(10000,deadline-Date.now()));
   if(health?.ok!==true)throw new SafeError('health_failed');
   const modelProxies=modelReadiness(health);
   const applicationPolicySha256=health.applicationModelProxy?.policySha256||null;
   if(applicationPolicySha256!==null&&!/^[a-f0-9]{64}$/.test(applicationPolicySha256))throw new SafeError('policy_runtime_hash_invalid');
   if(kind!=='candidate')return {ok:true,modelProxies,applicationPolicySha256};
   const value=await httpJson(origin,'/api/connectors/storage-ready',Math.max(1,Math.min(10000,deadline-Date.now())));
   if(value?.ok===true&&value.storageReady===true&&value.maintenance===true&&value.schema==='soty.connector-storage-ready.v1')return {...value,modelProxies,applicationPolicySha256};
  }catch{}
  await new Promise(r=>setTimeout(r,250));
 }
 throw new SafeError('readiness_deadline');
};}
