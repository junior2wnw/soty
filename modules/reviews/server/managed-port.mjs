import { contractDigest, snapshot } from '../../app-contract/json.mjs';
import { freezeDeep } from '../../capabilities/server/validation.mjs';
import { projectManagedReviewList, ManagedReviewProjectionError } from './managed-projection.mjs';

export const MANAGED_REVIEWS_PROFILE = 'soty.povedai.managed-port.v1';
const operations = Object.freeze(['provision','context','list','collect','publish','moderate']);
export class ManagedReviewsPortError extends Error { constructor(code,status=400){super(code);this.code=code;this.status=status;} }
const need=(value,code='managed_reviews_invalid',status=400)=>{if(!value)throw new ManagedReviewsPortError(code,status);};
const id=value=>need(typeof value==='string'&&value.length>0&&value.length<=200&&!/[\u0000-\u001f\u007f]/u.test(value));
const closed=(value,keys,optional=[])=>need(value&&typeof value==='object'&&!Array.isArray(value)&&keys.every(k=>Object.hasOwn(value,k))&&Object.keys(value).every(k=>keys.includes(k)||optional.includes(k)));
function data(value,maximum=524288){
  let nodes=0;
  function visit(v,depth){need(++nodes<=8192&&depth<=18,'managed_reviews_data_limit');
    if(v===null||typeof v==='boolean')return v;
    if(typeof v==='number'){need(Number.isSafeInteger(v)&&!Object.is(v,-0));return v;}
    if(typeof v==='string'){need(v.length<=10000&&v.isWellFormed());return v;}
    need(v&&typeof v==='object'&&[Object.prototype,null,Array.prototype].includes(Object.getPrototypeOf(v)));
    const d=Object.getOwnPropertyDescriptors(v);need(!Object.getOwnPropertySymbols(v).length);
    if(Array.isArray(v)){need(v.length<=128&&Object.keys(d).length===v.length+1);return Array.from({length:v.length},(_,i)=>{need(d[i]&&'value'in d[i]);return visit(d[i].value,depth+1);});}
    const out={};for(const k of Object.keys(d)){need(!['__proto__','constructor','prototype'].includes(k)&&d[k].enumerable&&'value'in d[k]);out[k]=visit(d[k].value,depth+1);}return out;
  }
  const result=visit(value,0);need(Buffer.byteLength(JSON.stringify(result))<=maximum,'managed_reviews_data_limit');return result;
}
/** Trusted host adapter, not an incoming descriptor or network factory. Both
 * authority fences complete synchronously; Source work happens BETWEEN them.
 * It owns no Root database or cache and never issues Source tenant rights. */
export function createManagedReviewsPort({registryId='soty',environmentId='production',providerRef,source,resolveSourceCredential,withAppAuthority,withRootActorAuthority,timeoutMs=8000}){
  id(registryId);id(environmentId);const provider=freezeDeep(snapshot(providerRef));closed(provider,['id','version','digest']);id(provider.id);
  need(Number.isSafeInteger(provider.version)&&provider.version>0&&/^[a-f0-9]{64}$/u.test(provider.digest),'managed_reviews_provider_invalid',500);
  need(source&&typeof source.withActor==='function'&&operations.every(op=>typeof source[op]==='function')&&typeof resolveSourceCredential==='function'&&typeof withAppAuthority==='function'&&typeof withRootActorAuthority==='function','managed_reviews_host_required',500);
  need(Number.isSafeInteger(timeoutMs)&&timeoutMs>=100&&timeoutMs<=10000);const pending=new Map(),validity=new WeakMap();let disposed=false;
  const callSource=source.withActor.bind(source), methods=Object.fromEntries(operations.map(op=>[op,source[op].bind(source)]));
  function captureAuthority(actor,appId,mode){
    need(!disposed,'managed_reviews_authentication_required',401);
    let rootEntered=false,appEntered=false,rootOpen=true,appOpen=false,poison=false,out;
    const synchronous=result=>{
      if(result&&typeof result.then==='function'){Promise.resolve(result).catch(()=>{});poison=true;throw new ManagedReviewsPortError('managed_reviews_async_authority',500);}
    };
    try{
      const result=withRootActorAuthority(actor,verifiedOwner=>{
        if(!rootOpen||rootEntered){poison=true;throw new ManagedReviewsPortError('managed_reviews_authority_invalid',500);}rootEntered=true;
        // Preserve the original proof object for the trusted host fence. This
        // separate DTO is created only INSIDE that verified current fence.
        const owner=freezeDeep(data(verifiedOwner,1024));closed(owner,['accountId','deviceId']);id(owner.accountId);id(owner.deviceId);
        appOpen=true;try{
          const appResult=withAppAuthority({actor:owner,appId,mode},context=>{
            if(!rootOpen||!appOpen||appEntered){poison=true;throw new ManagedReviewsPortError('managed_reviews_authority_invalid',500);}appEntered=true;
            const ctx=data(context,65536);need(ctx.appId===appId&&ctx.accountId===owner.accountId&&typeof ctx.ownerId==='string'&&ctx.canManage===(ctx.ownerId===ctx.accountId),'managed_reviews_authority_invalid',500);
            id(ctx.ownerId);need(Number.isSafeInteger(ctx.appRevision)&&Number.isSafeInteger(ctx.policyEpoch)&&ctx.target&&Number.isSafeInteger(ctx.target.revision)&&typeof ctx.target.digest==='string','managed_reviews_authority_invalid',500);
            need(!disposed,'managed_reviews_authentication_required',401);
            out=freezeDeep({appId,ownerId:ctx.ownerId,accountId:ctx.accountId,deviceId:owner.deviceId,appRevision:ctx.appRevision,policyEpoch:ctx.policyEpoch,target:ctx.target,entry:ctx.entry,canManage:ctx.canManage});return out;
          });
          synchronous(appResult);need(appEntered&&!poison,'managed_reviews_authority_invalid',500);return out;
        }finally{appOpen=false;}
      });
      synchronous(result);need(rootEntered&&appEntered&&!poison&&!disposed,'managed_reviews_authority_invalid',500);
      validity.set(out,()=>!poison);return out;
    }finally{rootOpen=false;appOpen=false;}
  }
  function bindingProjection(raw,args){
    const output=data(raw);closed(output,['protocol','localSubject','entityType','bindingId','siteId','siteKey','objectId','objectSlug','subjectId','placementId','generation','canDisplay']);
    need(output.protocol==='povedai.managed-reviews.v1'&&JSON.stringify(output.localSubject)===JSON.stringify(args.localSubject)&&output.entityType===({app:'product',project:'project',person:'profile'})[args.localSubject.kind]&&output.generation===1&&typeof output.canDisplay==='boolean','managed_reviews_source_projection_invalid',502);
    for(const key of ['bindingId','siteId','siteKey','objectId','objectSlug','subjectId','placementId'])id(output[key]);return output;
  }
  async function execute({op,actor:rawActor,args:rawArgs}){
    need(operations.includes(op),'managed_reviews_operation_unknown');
    // Neither snapshot nor JSON field validation can recreate Connect or
    // capability authority. The host receives this exact original reference.
    const actor=rawActor;need(actor&&typeof actor==='object'&&!Array.isArray(actor),'managed_reviews_authentication_required',401);
    const args=freezeDeep(data(rawArgs,65536));const required=['appId','localSubject'],optional=op==='provision'?['requestId','title']:op==='collect'?['requestId','body','authorLabel','rating','parentId']:op==='publish'?['requestId','canDisplay']:op==='moderate'?['requestId','reviewId','status']:op==='list'?['limit','beforeId']:[];
    closed(args,required,optional);id(args.appId);closed(args.localSubject,['kind','id']);need(['app','project','person'].includes(args.localSubject.kind));id(args.localSubject.id);
    if(['provision','collect','publish','moderate'].includes(op))id(args.requestId);
    if(op==='provision')need(typeof args.title==='string'&&args.title.trim().length>0&&args.title.length<=120&&!/[\u0000-\u001f\u007f]/u.test(args.title));
    if(op==='collect'){need(typeof args.body==='string'&&args.body.trim().length>0&&args.body.length<=10000);if(args.authorLabel!==undefined)need(typeof args.authorLabel==='string'&&args.authorLabel.trim().length>0&&args.authorLabel.length<=120&&!/[\u0000-\u001f\u007f]/u.test(args.authorLabel));if(args.rating!==undefined)need(Number.isInteger(args.rating)&&args.rating>=1&&args.rating<=5);if(args.parentId!==undefined)id(args.parentId);}
    if(op==='publish')need(typeof args.canDisplay==='boolean');
    if(op==='moderate'){id(args.reviewId);need(['approved','rejected','deleted'].includes(args.status));}
    if(op==='list'){if(args.limit!==undefined)need(Number.isInteger(args.limit)&&args.limit>=1&&args.limit<=30);if(args.beforeId!==undefined)id(args.beforeId);}
    need(args.localSubject.kind!=='app'||args.localSubject.id===args.appId,'managed_reviews_subject_scope_invalid',403);
    const mode=['provision','publish','moderate'].includes(op)?'owner':'participant',before=captureAuthority(actor,args.appId,mode),captured=[before];
    need(pending.size<4,'managed_reviews_busy',503);const controller=new AbortController();
    const scope=freezeDeep({registryId,tenantId:before.ownerId,appId:args.appId,environmentId});
    const selected={...args,scope};delete selected.appId;const input=freezeDeep(selected);
    const work=Promise.resolve().then(async()=>{
      const credential=await resolveSourceCredential(Object.freeze({actor,rootActor:Object.freeze({accountId:before.accountId,deviceId:before.deviceId}),authority:before,scope,providerRef:provider,signal:controller.signal}));
      need(!controller.signal.aborted&&!disposed,'managed_reviews_cancelled',503);need(validity.get(before)(),'managed_reviews_authority_invalid',500);
      const current=captureAuthority(actor,args.appId,mode);captured.push(current);need(validity.get(current)()&&contractDigest(before)===contractDigest(current),'managed_reviews_authority_changed',403);
      // Credential is private, transient and supplied by the trusted Source RP
      // binding, never an author argument. Source independently verifies it.
      let open=true,entered=false,poison=false;
      try{
        const result=await callSource(credential,async sourceActor=>{
          if(!open||entered||controller.signal.aborted||disposed){poison=true;throw new ManagedReviewsPortError('managed_reviews_source_authority_invalid',502);}entered=true;
          if(op==='list'){
            const binding=bindingProjection(await methods.context(sourceActor,freezeDeep({scope,localSubject:args.localSubject})),args);
            need(!controller.signal.aborted&&!disposed,'managed_reviews_cancelled',503);
            return{binding,result:await methods.list(sourceActor,input)};
          }
          return methods[op](sourceActor,input);
        });
        need(entered&&!poison,'managed_reviews_source_authority_invalid',502);return result;
      }finally{open=false;}
    });pending.set(work,controller);work.finally(()=>pending.delete(work)).catch(()=>{});
    let timer;try{
      const raw=await Promise.race([work,new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new ManagedReviewsPortError('managed_reviews_timeout',503));},timeoutMs);})]);
      need(captured.every(value=>validity.get(value)()),'managed_reviews_authority_invalid',500);
      const after=captureAuthority(actor,args.appId,mode);need(validity.get(after)()&&contractDigest(before)===contractDigest(after),'managed_reviews_authority_changed',403);
      if(op==='list')return freezeDeep(projectManagedReviewList(raw.result,{limit:args.limit??20,binding:raw.binding}));
      const output=data(raw);if(['provision','context','publish'].includes(op)){
        bindingProjection(output,args);
        return freezeDeep({...output,providerRef:provider,subjectRef:{id:'povedai:subject/'+output.subjectId,version:1,digest:contractDigest({scope,providerRef:provider,localSubject:args.localSubject,subjectId:output.subjectId,placementId:output.placementId,generation:output.generation})}});
      }
      // Native Source returns a bounded escaped-data projection, never remote
      // script/code or raw identity claims. Metadata receipts are not execution
      // attestation, whole-SDK readiness or a cross-store transaction guarantee.
      if(op==='collect'){closed(output,['reviewId','authorId','rootId','parentId','depth','status']);for(const k of ['reviewId','authorId','rootId'])id(output[k]);need(output.parentId===null||typeof output.parentId==='string');need(Number.isSafeInteger(output.depth)&&output.depth>=0&&output.depth<=64&&output.status==='pending','managed_reviews_source_projection_invalid',502);}
      if(op==='moderate'){closed(output,['reviewId','status','version']);need(output.reviewId===args.reviewId&&output.status===args.status&&Number.isSafeInteger(output.version)&&output.version>=1,'managed_reviews_source_projection_invalid',502);}
      return freezeDeep(output);
    }catch(error){if(error instanceof ManagedReviewsPortError)throw error;if(error instanceof ManagedReviewProjectionError)throw new ManagedReviewsPortError(error.code,502);const code=typeof error?.code==='string'&&/^managed_[a-z0-9_]{1,80}$/u.test(error.code)?error.code:'managed_reviews_source_unavailable';throw new ManagedReviewsPortError(code,[400,401,403,404,409,503].includes(error?.status)?error.status:503);}
    finally{if(timer)clearTimeout(timer);}
  }
  return Object.freeze({profile:MANAGED_REVIEWS_PROFILE,operations:new Set(operations),execute,close(){disposed=true;for(const c of pending.values())c.abort();}});
}
