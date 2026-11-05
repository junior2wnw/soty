import path from 'node:path';
import {createMonitoringService} from '../modules/monitoring/server/index.mjs';
import {MonitoringError,check,exact,scopeKey,sync,digest} from '../modules/monitoring/server/validation.mjs';
import {createMonitoringCapabilities} from '../modules/monitoring/capabilities.mjs';
import {canonicalJson} from '../modules/connect/server/index.mjs';

/** Canonical local providers, never operations/URLs/executable code from a manifest. */
export function createCanonicalMonitoringActions({connect,publication}={}){
  const actions=[];
  if(typeof connect==='function'){
    const call=(actor,op,args)=>connect().platformAdmission({actor,op,args});
    actions.push({id:'platform.admission',label:'Вход и приглашения',scope:'platform',
      fields:[{name:'inviteOnly',label:'Вход по приглашению',type:'boolean'},{name:'defaultQuota',label:'Приглашений на человека',type:'integer',minimum:0,maximum:100}],
      read({actor}){const state=call(actor,'platform.admission.get',{});check(state.manageAllowed===true,'monitoring_admin_unavailable',403);return{revision:state.revision,current:{inviteOnly:state.inviteOnly,defaultQuota:state.defaultQuota}};},
      prepare({actor,requestId,expectedRevision,input}){const state=call(actor,'platform.admission.get',{});check(state.manageAllowed===true,'monitoring_admin_unavailable',403);check(state.revision===expectedRevision,'monitoring_revision_conflict',409);return{requestId,expectedRevision,...input};},
      execute({actor,intent}){return call(actor,'platform.admission.update',intent);},
      receipt({actor,intent}){const found=call(actor,'platform.admission.receipt',intent);if(found.found)return{state:'committed',revision:found.result.revision};const current=call(actor,'platform.admission.get',{});return{state:current.revision===intent.expectedRevision?'missing':'unknown'};}});
  }
  if(typeof publication==='function'){
    const call=(actor,op,args)=>publication({actor,op,args:{expectedAccountId:actor.accountId,...args}});
    const state=(actor,target)=>{const value=call(actor,'apps.publication.get',{appId:target.appId});check(value.launchPolicy==='anyone'&&value.activeDomainIds.length>0&&value.appState==='enabled','monitoring_admin_unavailable',403);return value;};
    actions.push({id:'app.listing',label:'Показывать приложение в общем каталоге',scope:'app',fields:[{name:'listed',label:'Видно в общем каталоге',type:'boolean'}],
      read({actor,scope}){const current=state(actor,scope);return{revision:current.policyEpoch,current:{listed:current.listed}};},
      prepare({actor,scope,requestId,expectedRevision,input}){const current=state(actor,scope);check(current.policyEpoch===expectedRevision,'monitoring_revision_conflict',409);const profile=current.target.profile;check(['soty.relay-restricted.v1','soty.relay-self-ip.v1','soty.selected-human-embed.v2'].includes(profile),'monitoring_admin_unavailable',403);
        return{appId:scope.appId,requestId,expectedPolicyEpoch:expectedRevision,expectedTargetRevision:current.activeTargetRevision,launchPolicy:'anyone',listed:input.listed,activeDomainIds:[...current.activeDomainIds],exposureAck:{scope:profile==='soty.selected-human-embed.v2'?'authenticated-workspace':'whole-port',targetRevision:current.target.revision,targetDigest:current.target.digest,profile},...(Object.hasOwn(current,'audience')?{audience:input.listed?'public':'link'}:{})};},
      execute({actor,intent}){return call(actor,'apps.publication.update',intent);},
      receipt({actor,intent}){const found=call(actor,'apps.publication.receipt',intent);if(found.found)return{state:'committed',revision:found.result.receipt.policyEpoch};const current=call(actor,'apps.publication.get',{appId:intent.appId});return{state:current.policyEpoch===intent.expectedPolicyEpoch?'missing':'unknown'};}});
  }
  return actions;
}

/** Existing Connect extension composition: no extra Core slot, route or identity scheme. */
export function composeMonitoringExtension(existing,monitoring){
  check(existing?.operations instanceof Set&&typeof existing.execute==='function'&&existing.execute.constructor.name!=='AsyncFunction'&&!existing.executeAsync,'monitoring_extension_invalid',500);
  check(existing.challengeStorage===undefined||existing.challengeStorage==='persistent','monitoring_extension_invalid',500);
  const original=new Set(existing.operations),own=new Set(monitoring.operations);check([...own].every(op=>!original.has(op)),'monitoring_extension_collision',500);
  const execute=existing.execute.bind(existing),originalLimit=existing.maxResultBytes;
  check(originalLimit===undefined||Number.isSafeInteger(originalLimit)&&originalLimit>=1024&&originalLimit<=8*1024*1024,'monitoring_extension_invalid',500);
  return Object.freeze({operations:new Set([...original,...own]),...(originalLimit===undefined?{}:{maxResultBytes:Math.max(originalLimit,65536)}),execute(request){check(original.has(request.op)||own.has(request.op),'monitoring_operation_unavailable',404);const isMonitoring=own.has(request.op),result=isMonitoring?monitoring.execute(request):execute(request);if(isMonitoring||originalLimit!==undefined)check(Buffer.byteLength(canonicalJson(result),'utf8')<=(isMonitoring?65536:originalLimit),isMonitoring?'monitoring_response_limit':'extension_result_too_large',503);return result;}});
}

export function createMonitoringModule({dataDir,databasePath,storageMode='reader',connect,apps,capabilities:getCapabilities=()=>null,platformPolicy={},publication,definitions,adminActions,collectorStatus,beforeCollectionChange,clock,limits,onChange}={}){
  check(typeof connect==='function'&&typeof apps==='function','monitoring_host_required',500);
  exact(platformPolicy,[],['readAccountIds','configureAccountIds','adminAccountIds']);
  const policies={};for(const right of['read','configure','admin']){const input=platformPolicy[right+'AccountIds']??[];check(Array.isArray(input)&&input.length<=32&&input.every(id=>typeof id==='string'&&/^acct_[A-Za-z0-9_-]{32}$/u.test(id)),'monitoring_platform_policy_invalid',500);policies[right]=new Set(input);}
  // Fingerprint the same loaded Sets used by withAuthority, never a later Env read.
  const accessSha256=digest(Object.fromEntries(['read','configure','admin'].map(right=>[right+'AccountIds',[...policies[right]].sort()])));
  let agentFrame=null;
  const inAgent=(actor,callback)=>{const previous=agentFrame;agentFrame=actor;try{return sync(callback());}finally{agentFrame=previous;}};
  const withAuthority=({actor,scope,right},callback)=>{
    const execute=()=>{check(connect().isActorActive(actor)===true,'monitoring_forbidden',403);if(scope.kind==='platform'){check(policies[right==='record'?'configure':right]?.has(actor.accountId),'monitoring_forbidden',403);return callback();}return apps().withAppAuthority({actor,appId:scope.appId,mode:'owner'},()=>callback());};
    return agentFrame===actor?execute():connect().withActorAuthorityFence(actor,execute);
  };
  const service=createMonitoringService({databasePath:databasePath??path.join(dataDir||path.resolve('data'),'monitoring','monitoring.sqlite'),projectId:'soty',mode:storageMode,currentActor:actor=>connect()?.isActorActive(actor)===true,withAuthority,
    ...(definitions?{definitions}:{}),adminActions:adminActions??createCanonicalMonitoringActions({connect,publication}),...(collectorStatus?{collectorStatus}:{}),...(beforeCollectionChange?{beforeCollectionChange}:{}),...(clock?{clock}:{}),...(limits?{limits}:{}),...(onChange?{onChange}:{})});
  let capabilities;try{capabilities=createMonitoringCapabilities({service,connect,getCapabilities,inAgent,storageMode});}catch(error){service.close();throw error;}
  return Object.freeze({service,operations:service.operations,execute:service.execute,observe:service.observe,observeBatch:service.observeBatch,collectionEnabled:service.collectionEnabled,storageIdentity:()=>({...service.storageIdentity(),accessSha256}),
    extension:Object.freeze({operations:service.operations,execute:service.execute}),capabilities,
    composeExtension:existing=>composeMonitoringExtension(existing,service),close:service.close});
}
