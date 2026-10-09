import { types } from 'node:util';
import { createTrustedReadonlyQueryAdapter,captureQueryData } from '../capabilities/server/readonly-queries.mjs';
import { EXTERNAL_ADAPTER_PROFILE } from '../capabilities/server/external-adapters.mjs';
import { canonicalHash,freezeDeep,AccessError } from '../capabilities/server/validation.mjs';
import { selectedResource } from '../apps/scoped-embed/resource-profile.mjs';
import { STANDARD_SELECTED_SOURCE_V2 } from '../source-app/server/standard-profile.mjs';
import { validateLiveAnswerText } from './live-answer.ts';

const installed=new WeakMap();
const need=(value,code='voice_source_denied')=>{if(!value)throw new AccessError(code);};
const fields=(value,names)=>need(value&&typeof value==='object'&&!Array.isArray(value)
  &&Object.keys(value).length===names.length&&names.every(key=>Object.hasOwn(value,key)));
const id=value=>need(typeof value==='string'&&/^[A-Za-z0-9_.:-]{8,160}$/u.test(value));
const same=(a,b)=>canonicalHash(a)===canonicalHash(b);
export const VOICE_TASK_TYPE='native-voice-task';

/** Fixed Root CODE adapter. The original connector owns proof keys and cookies;
 * this port receives only the existing scoped stream's bounded response. */
export function createRootVoiceSourceAdapters(raw){
  const scope=captureQueryData(raw);fields(scope,['appId','target','sourceProfile','resource']);
  need(/^app-[a-f0-9]{32}$/u.test(scope.appId));fields(scope.target,['revision','digest']);
  need(Number.isSafeInteger(scope.target.revision)&&scope.target.revision>0&&/^[a-f0-9]{64}$/u.test(scope.target.digest));
  need(same(scope.sourceProfile,STANDARD_SELECTED_SOURCE_V2));selectedResource(scope.resource);
  need(scope.resource.appId===scope.appId&&scope.resource.selection.kind==='soty.resource.v1'
    &&scope.resource.selection.nativeId==='kvartalufa.voice');freezeDeep(scope);
  const attempted=new Set();let closed=false;
  const binding=freezeDeep({id:'voice.source.'+scope.appId,version:1,digest:canonicalHash({schema:'soty.voice-root-source.v1',scope})});
  function controller(){need(!closed,'voice_source_closed');return scope;}
  function makeAuthority(){let host;
    const withAuthority=(request,action)=>{controller();need(host&&typeof action==='function','voice_source_unbound');
      return host.withOwner(request,owner=>host.apps().withScopedVoiceSourceAuthority({owner,scope},action));};
    installed.set(withAuthority,{scope,bind(value){need(!host,'voice_source_already_bound');host=value;},
      owner(request,action){controller();need(host,'voice_source_unbound');return host.withOwner(request,action);},
      call(request,path,args,signal){controller();need(host,'voice_source_unbound');return host.apps().callScopedVoiceSource({scope,path,args,signal,
        withCurrentOwner:action=>host.withOwner(request,action)});}});
    return withAuthority;
  }
  const readAuthority=makeAuthority(),completeAuthority=makeAuthority();
  async function call(authority,request,path,args,signal){need(!signal?.aborted,'voice_source_unknown');
    const response=await installed.get(authority).call(request,path,args,signal);need(!closed&&!signal?.aborted,'voice_source_unknown');
    need(response.status===200,'voice_source_unknown');const envelope=captureQueryData(response.data);fields(envelope,['ok','data']);
    need(envelope.ok===true,'voice_source_unknown');return captureQueryData(envelope.data);}
  function readInput(input){fields(input,['operation','taskId','consentId']);need(input.operation==='voice.task.read');id(input.taskId);id(input.consentId);}
  function completeInput(input){fields(input,['operation','taskId','consentId','result']);need(input.operation==='voice.task.complete');id(input.taskId);id(input.consentId);
    if(Object.keys(input.result??{}).join(',')==='sourceCurrent'){need(input.result.sourceCurrent===true);}
    else {fields(input.result,['answer','speech']);validateLiveAnswerText(input.result);}}
  function requestData(raw,write){const value=captureQueryData(raw);fields(value,write?['requestId','inputDigest','input','authorization']:['requestId','input','authorization']);
    id(value.requestId);(write?completeInput:readInput)(value.input);
    if(write)need(/^[a-f0-9]{64}$/u.test(value.inputDigest)&&value.inputDigest===canonicalHash(value.input));return value;}
  const readAdapter=createTrustedReadonlyQueryAdapter({withAuthority:readAuthority,
    async query(raw,signal,current){const request=requestData(raw,false);need(typeof current==='function'&&current()===true);
      const data=await call(readAuthority,request,'/api/embed/query',{requestId:request.requestId,input:request.input},signal);
      fields(data,data.kind===undefined?['taskId','consent','prompt']:['taskId','consent','kind']);need(data.taskId===request.input.taskId);
      fields(data.consent,['scope','consentId','conversationId','rootPrincipal','expiresAt','transcript','rootBrain']);
      need(data.consent.scope==='kvartalufa.voice'&&data.consent.consentId===request.input.consentId&&data.consent.transcript===true&&data.consent.rootBrain===true);
      fields(data.consent.rootPrincipal,['accountId','deviceId']);need(Number.isSafeInteger(data.consent.expiresAt)&&data.consent.expiresAt>Date.now());
      const sourceHost=installed.get(readAuthority);
      sourceHost.owner(request,owner=>need(data.consent.rootPrincipal.accountId===owner.accountId&&data.consent.rootPrincipal.deviceId===owner.deviceId));
      need(data.kind===undefined?typeof data.prompt==='string'&&data.prompt.trim()&&data.prompt.length<=8000:data.kind==='source-permission');
      need(current()===true);return data;}});
  const completeAdapter=Object.freeze({profile:EXTERNAL_ADAPTER_PROFILE,withAuthority:completeAuthority,
    async execute(raw,signal){const request=requestData(raw,true);need(!attempted.has(request.requestId),'voice_source_reconcile_required');
      need(attempted.size<256,'voice_source_capacity');attempted.add(request.requestId);
      await call(completeAuthority,request,'/api/embed/invoke',{requestId:request.requestId,input:request.input},signal);},
    async readProof(raw,signal){const request=requestData(raw,true),base={requestId:request.requestId,inputDigest:request.inputDigest};let proof;
      try{proof=await call(completeAuthority,request,'/api/embed/receipt',{requestId:request.requestId,input:{inputDigest:request.inputDigest}},signal);}
      catch{return {...base,outcome:'unknown'};}
      if(proof.outcome==='not_found'){fields(proof,['outcome']);return {...base,outcome:attempted.has(request.requestId)?'unknown':'not_applied'};}
      if(proof.outcome==='unknown'){fields(proof,['outcome']);return {...base,outcome:'unknown'};}
      fields(proof,['outcome','result']);need(proof.outcome==='committed');fields(proof.result,['taskId','admitted']);
      need(proof.result.taskId===request.input.taskId&&proof.result.admitted===true,'voice_source_receipt_changed');
      return {...base,outcome:'committed',effects:[{kind:'updated',resourceType:VOICE_TASK_TYPE,resourceId:request.input.taskId,revision:1}],
        receipt:{verificationMethod:'domain_read',artifacts:[{type:VOICE_TASK_TYPE,id:request.input.taskId,revision:1,sha256:canonicalHash(proof.result)}]}};}});
  return Object.freeze({binding,readAdapter,completeAdapter,close(){closed=true;}});
}

/** Bound exactly once by Root's existing externalApplications composition.
 * A copied JSON adapter/profile cannot acquire this constructor's brand. */
export function bindRootVoiceSourceAdapter(adapter,{appId,target,withOwner,apps}){
  const descriptor=Object.getOwnPropertyDescriptor(adapter,'withAuthority');
  const port=descriptor&&Object.hasOwn(descriptor,'value')?installed.get(descriptor.value):undefined;
  if(!port)return false;
  need(port.scope.appId===appId&&same(port.scope.target,target));
  need(typeof withOwner==='function'&&typeof apps==='function'&&!types.isProxy(withOwner)&&!types.isProxy(apps));
  port.bind(Object.freeze({withOwner,apps}));return true;
}
