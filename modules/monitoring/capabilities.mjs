import {createCatalog} from '../capabilities/server/catalog.mjs';
import {createTrustedReadonlyQueryAdapter} from '../capabilities/server/readonly-queries.mjs';
import {EXTERNAL_ADAPTER_PROFILE} from '../capabilities/server/external-adapters.mjs';
import {canonicalHash,AccessError} from '../capabilities/server/validation.mjs';
import {monitoringCapabilityCatalog,isMonitoringMutation as mutation} from './catalog.mjs';
export {monitoringCapabilityCatalog} from './catalog.mjs';
import {check,scope,scopeKey,exact,MonitoringError} from './server/validation.mjs';

const right=op=>op==='monitoring.admin.invoke'||op==='monitoring.audit.list'?'admin':op==='monitoring.metric.record'?'record':mutation(op)?'configure':'read';
export const monitoringResource=target=>'monitoring:'+scopeKey(scope(target));

export function createMonitoringCapabilities({service,connect,getCapabilities,inAgent,storageMode}){
 const catalog=monitoringCapabilityCatalog({writer:storageMode==='writer'}),registry=createCatalog(catalog),entries=registry.listAll(),byId=new Map(entries.map(entry=>[entry.capabilityId,entry]));
 const reference=entry=>({capabilityId:entry.capabilityId,version:entry.version,digest:entry.digest});
 function parsed(entry,input){registry.validateInput(entry,input);const target=scope(input.scope);let params;try{params=JSON.parse(input.paramsJson);}catch{check(false,'monitoring_invalid_input');}check(params&&typeof params==='object'&&!Array.isArray(params),'monitoring_invalid_input');check(!['expectedAccountId','scope','requestId','eventId'].some(key=>Object.hasOwn(params,key)),'monitoring_invalid_input');return{target,params};}
 function owner(request,input,callback){
  const target=input?scope(input.scope):null,current=getCapabilities();check(current,'monitoring_unavailable',503);
  try{return connect().withAuthorityFence(()=>{
   const options=target?{requiredResources:[monitoringResource(target)]}:{},run=actor=>inAgent(actor,()=>callback(actor));
   return request.actor?current.withOwnerAuthority({actor:request.actor,...options},run):current.withSnapshotOwnerAuthority({authorization:request.authorization,...options},run);
  });}catch(error){
   if(error instanceof MonitoringError&&error.code==='monitoring_forbidden')throw new AccessError('access_denied');
   throw error;
  }
 }
 const commandId=request=>'agent.'+canonicalHash({id:request.requestId,scope:request.input.scope});
 const args=(entry,request,actor)=>{const{target,params}=parsed(entry,request.input);return{...params,scope:target,expectedAccountId:actor.accountId,...(mutation(entry.capabilityId)?{[entry.capabilityId==='monitoring.metric.record'?'eventId':'requestId']:commandId(request)}:{})};};
 function permission(entry,request,callback){return owner(request,request.input,actor=>{const value=args(entry,request,actor);return callback(actor,value);});}
 function readProof(entry,request){return permission(entry,request,(actor,value)=>{const{expectedAccountId,scope:target,requestId,eventId,...params}=value;let intent=params;if(entry.capabilityId==='monitoring.metric.record')intent={metricId:params.metricId,value:params.value,observedAt:params.observedAt};if(entry.capabilityId==='monitoring.admin.invoke'){delete intent.retry;intent={actionId:params.actionId,expectedRevision:params.expectedRevision,input:params.input};}const result=service.readMutation({actor,scope:target,requestId:requestId??eventId,operation:entry.capabilityId,intent});if(!result)return{requestId:request.requestId,inputDigest:request.inputDigest,outcome:'not_applied'};if(entry.capabilityId==='monitoring.admin.invoke'&&result.state!=='committed')return{requestId:request.requestId,inputDigest:request.inputDigest,outcome:'unknown'};const revision=result.config?.revision??result.revision??1,id=scopeKey(target)+':'+commandId(request).slice(6,38);return{requestId:request.requestId,inputDigest:request.inputDigest,outcome:'committed',effects:[{kind:entry.capabilityId==='monitoring.metric.record'?'created':'updated',resourceType:'monitoring',resourceId:id,revision}],receipt:{verificationMethod:'domain_read',artifacts:[{type:'monitoring',id,revision}]}};});}
 const adapters=entries.map(entry=>{
   const withAuthority=(request,callback)=>owner(request,request.input,()=>callback());
   if(!mutation(entry.capabilityId))return{contract:reference(entry),adapter:createTrustedReadonlyQueryAdapter({withAuthority,query(request,signal,current){check(!signal.aborted,'monitoring_cancelled',499);current();const result=permission(entry,request,(actor,value)=>service.execute({op:entry.capabilityId,actor,args:value}));current();const dataJson=JSON.stringify(result);check(Buffer.byteLength(dataJson)<=60000,'monitoring_response_limit',503);return{schema:'soty.monitoring.agent-data.v1',dataJson};}})};
   return{contract:reference(entry),adapter:{profile:EXTERNAL_ADAPTER_PROFILE,withAuthority,execute(request,signal){check(!signal.aborted,'monitoring_cancelled',499);permission(entry,request,(actor,value)=>service.execute({op:entry.capabilityId,actor,args:value}));return readProof(entry,request);},readProof(request,signal){check(!signal.aborted,'monitoring_cancelled',499);return readProof(entry,request);}}};
 });
 function beforeCall({actor,name,args}){if(!['apps_query','apps_invoke'].includes(name))return;const entry=byId.get(args.reference?.capabilityId);if(!entry)return;check(args.reference.version===entry.version&&args.reference.digest===entry.digest,'monitoring_forbidden',403);const{target}=parsed(entry,args.input);owner({actor},args.input,current=>{getCapabilities().authorize({actor,capabilityId:entry.capabilityId,version:entry.version,input:args.input,action:'invoke'});return service.checkAuthority({actor:current,scope:target,right:right(entry.capabilityId)});});}
 function recheck(request){beforeCall(request);if(!['apps_invocation_get','apps_invocation_cancel'].includes(request.name))return;const entry=byId.get(request.value?.invocation?.capabilityId);if(!entry)return;const dispatch=getCapabilities().invocations.inspectDispatch({invocationId:request.value.invocation.invocationId});const{target}=parsed(entry,dispatch.input);owner({actor:request.actor},dispatch.input,actor=>service.checkAuthority({actor,scope:target,right:right(entry.capabilityId)}));}
 return Object.freeze({catalog,queries:adapters.filter((_,index)=>!mutation(entries[index].capabilityId)),effects:adapters.filter((_,index)=>mutation(entries[index].capabilityId)),beforeCall,recheck});
}
