import {createCatalog} from '../modules/capabilities/server/catalog.mjs';
import {createTrustedReadonlyQueryAdapter} from '../modules/capabilities/server/readonly-queries.mjs';
import {EXTERNAL_ADAPTER_PROFILE} from '../modules/capabilities/server/external-adapters.mjs';
import {AccessError,canonicalHash,freezeDeep} from '../modules/capabilities/server/validation.mjs';
import {REFERRAL_SCHEMAS} from '../modules/referrals/descriptor.mjs';
import {appId as checkedAppId} from '../modules/apps/server/protocol.mjs';

const object=(properties,required=Object.keys(properties))=>({type:'object',properties,required,additionalProperties:false});
const text=maxLength=>({type:'string',minLength:1,maxLength});
const count={type:'integer',minimum:1,maximum:Number.MAX_SAFE_INTEGER};
const artifact=object({type:text(128),id:text(128),revision:count});
const proof=object({requestId:text(128),inputDigest:text(64),outcome:{type:'string',enum:['committed','not_applied','unknown'],maxLength:16},
  effects:{type:'array',maxItems:1,items:object({kind:{type:'string',enum:['updated'],maxLength:12},resourceType:text(128),resourceId:text(128),revision:count})},
  receipt:object({verificationMethod:{type:'string',enum:['domain_read'],maxLength:16},artifacts:{type:'array',maxItems:1,items:artifact}})},['requestId','inputDigest','outcome']);
const binding={id:'soty.referrals',version:1,digest:canonicalHash({profile:'soty.referrals-capabilities.v1',rounding:'floor-per-beneficiary;cumulative-refund-of-original-commission'})};
export const REFERRAL_CATALOG=freezeDeep(Object.entries(REFERRAL_SCHEMAS).map(([op,[input,output]])=>{
  const properties={...input.properties};delete properties.expectedAccountId;delete properties.mutationId;
  const configure=op==='config.put';
  return {capabilityId:`referrals.${op}`,version:1,appId:'soty.referrals',title:`Реферальная система · ${op}`,
    description:configure?'Configure future referral commissions for one owned app. Does not create credit, payment or payout.':'Read the current account referral data for one currently accessible app. Other beneficiaries are never returned.',
    visibility:'private',executionEnabled:true,inputSchema:object(properties,input.required.filter(key=>!['expectedAccountId','mutationId'].includes(key))),
    outputSchema:configure?proof:output,resources:['referrals'],effects:configure?['update']:[],recipients:['soty:referrals'],
    executionBinding:{kind:'registered',handler:`referrals.${op}`,version:1,binding}};
}));
export const referralResource=appId=>`referrals:${checkedAppId(appId)}`;

/** Uses the existing grant, invocation and domain-receipt pipeline, including after-response replay. */
export function createReferralCapabilities({capabilities,connect,apps,referrals}){
  const registry=createCatalog(REFERRAL_CATALOG),entries=registry.listAll(),byId=new Map(entries.map(entry=>[entry.capabilityId,entry]));
  const reference=entry=>({capabilityId:entry.capabilityId,version:entry.version,digest:entry.digest});
  const check=value=>{if(!value)throw new AccessError('access_denied');};
  const inputFor=(entry,input)=>{registry.validateInput(entry,input);checkedAppId(input.appId);return input;};
  function owner(request,input,callback){
    return connect().withAuthorityFence(()=>{
      const options=input?{requiredResources:[referralResource(input.appId)]}:{};
      return request.actor?capabilities().withOwnerAuthority({actor:request.actor,...options},callback)
        :capabilities().withSnapshotOwnerAuthority({authorization:request.authorization,...options},callback);
    });
  }
  function authority(request,input,callback){
    return owner(request,input,actor=>{
        if(input){
          const entry=byId.get(request.authorization?.capabilityId||request.capability?.capabilityId);
          if(entry?.effects.length)return apps().withAppAuthority({actor,appId:input.appId,mode:'owner'},()=>callback(actor));
          const visible=apps().execute({op:'apps.directory.resolve',actor,args:{expectedAccountId:actor.accountId,appIds:[input.appId]}});check(visible.items[0]?.available===true);
        }
        return callback(actor);
    });
  }
  const mutationId=request=>`agent-${canonicalHash({requestId:request.requestId,appId:request.input.appId})}`;
  function receiptProof(entry,request,result){
    if(!result)return {requestId:request.requestId,inputDigest:request.inputDigest,outcome:'not_applied'};
    return {requestId:request.requestId,inputDigest:request.inputDigest,outcome:'committed',effects:[{kind:'updated',resourceType:'referral_config',resourceId:result.appId,revision:result.revision}],
      receipt:{verificationMethod:'domain_read',artifacts:[{type:'referral_config',id:result.appId,revision:result.revision}]}};
  }
  const adapters=entries.map(entry=>{
    const withAuthority=(request,callback)=>authority({...request,capability:entry},request.input===undefined?undefined:inputFor(entry,request.input),callback);
    const read=(request,callback)=>owner({...request,capability:entry},inputFor(entry,request.input),actor=>callback(actor));
    if(!entry.effects.length)return {contract:reference(entry),adapter:createTrustedReadonlyQueryAdapter({withAuthority,query(request,signal,current){check(!signal.aborted);current();const result=read(request,actor=>referrals().agent.execute({op:entry.capabilityId,actor,args:{...request.input,expectedAccountId:actor.accountId}}));current();return result;}})};
    return {contract:reference(entry),adapter:{profile:EXTERNAL_ADAPTER_PROFILE,withAuthority,
      execute(request,signal){check(!signal.aborted);return read(request,actor=>receiptProof(entry,request,referrals().agent.execute({op:entry.capabilityId,actor,args:{...request.input,expectedAccountId:actor.accountId,mutationId:mutationId(request)}})));},
      readProof(request,signal){check(!signal.aborted);return read(request,actor=>receiptProof(entry,request,referrals().agent.readConfigReceipt({actor,appId:request.input.appId,mutationId:mutationId(request)})));}}};
  });
  function beforeCall({actor,name,args}){
    if(!['apps_invoke','apps_query'].includes(name))return;
    const entry=byId.get(args.reference?.capabilityId);if(!entry)return;
    check(args.reference.version===entry.version&&args.reference.digest===entry.digest);
    const input=inputFor(entry,args.input);
    authority({actor,capability:entry},input,()=>capabilities().authorize({actor,capabilityId:entry.capabilityId,version:entry.version,input,action:'invoke'}));
  }
  return Object.freeze({catalog:REFERRAL_CATALOG,effects:adapters.filter((_,i)=>entries[i].effects.length),queries:adapters.filter((_,i)=>!entries[i].effects.length),beforeCall,
    recheck(request){beforeCall(request);if(!['apps_invocation_get','apps_invocation_cancel'].includes(request.name))return;const entry=byId.get(request.value.invocation.capabilityId);if(!entry?.effects.length)return;
      const dispatch=capabilities().invocations.inspectDispatch({invocationId:request.value.invocation.invocationId});authority({actor:request.actor,capability:entry},inputFor(entry,dispatch.input),()=>true);}});
}
