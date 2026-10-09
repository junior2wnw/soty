import { randomUUID } from 'node:crypto';
import { firstAnswerParagraph,validateLiveAnswerText } from './live-answer.ts';
const need=(ok,code='voice_root_denied')=>{if(!ok)throw new Error(code);};
const fields=(v,keys)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const id=v=>typeof v==='string'&&/^[A-Za-z0-9_.:-]{8,160}$/u.test(v);

/** Root-installed adapter. source is the existing authenticated Source broker
 * assembly below, not a browser-returned permission/answer. All model work stays
 * in Root's current signed/accounted agent. Native retains its audio key/socket. */
export function createRootNativeVoiceExtension({getConnect,agent,source}){
  need(typeof getConnect==='function'&&typeof agent?.executeAsync==='function'
    &&typeof source?.readTask==='function'&&typeof source?.completeTask==='function');
  const owned=new Map(),byTask=new Map();let closed=false;
  const current=actor=>{need(!closed);getConnect().withActorAuthorityFence(actor,()=>true);};
  const key=(actor,requestId)=>JSON.stringify([actor.accountId,actor.deviceId,requestId]);
  return Object.freeze({operations:new Set(['voice.agent.turn','voice.agent.cancel']),
    async executeAsync({op,args,actor}){
      current(actor);
      if(op==='voice.agent.cancel'){
        need(fields(args,['requestId'])&&id(args.requestId));const frame=owned.get(key(actor,args.requestId));need(frame);
        // Consent withdrawal does not remove the current owner's right to stop
        // this adapter's own started request. An unrelated personal turn is not selectable.
        frame.cancelled=true;return agent.executeAsync({op:'agent.cancel',args:{requestId:frame.modelId},actor});
      }
      need(op==='voice.agent.turn'&&fields(args,['requestId','taskId','consentId'])&&Object.values(args).every(id));
      for(const [retiredKey,value]of owned)if(value.finished&&value.retireAt<=Date.now()){owned.delete(retiredKey);byTask.delete(value.taskKey);}
      const ownedKey=key(actor,args.requestId),taskKey=key(actor,`${args.consentId}:${args.taskId}`);
      need(!owned.has(ownedKey)&&!byTask.has(taskKey),'voice_reconcile_required');need(owned.size<128,'voice_capacity');
      const frame={modelId:randomUUID(),cancelled:false,taskKey,actor,retireAt:Date.now()+1800000};owned.set(ownedKey,frame);byTask.set(taskKey,frame);
      try{
        const task=await source.readTask({actor,taskId:args.taskId,consentId:args.consentId,requestId:args.requestId});current(actor);
        need(!frame.cancelled&&task.taskId===args.taskId&&task.consent.scope==='kvartalufa.voice'
          &&task.consent.consentId===args.consentId&&task.consent.rootPrincipal.accountId===actor.accountId
          &&task.consent.rootPrincipal.deviceId===actor.deviceId&&task.consent.transcript===true&&task.consent.rootBrain===true
          &&task.consent.expiresAt>Date.now()&&(task.kind==='source-permission'||typeof task.prompt==='string'&&task.prompt.trim()&&task.prompt.length<=8000));
        frame.retireAt=Math.min(frame.retireAt,task.consent.expiresAt);
        if(task.kind==='source-permission'){
          const delivery=await source.completeTask({actor,taskId:args.taskId,consentId:args.consentId,requestId:args.requestId,result:{sourceCurrent:true}});current(actor);
          need(!frame.cancelled&&delivery.taskId===args.taskId&&delivery.admitted===true);
          return {schema:'soty.native-voice-turn.v1',requestId:args.requestId,delivery:'native_commit_ack',playbackVerified:false};
        }
        const result=await agent.executeAsync({op:'agent.turn',args:{requestId:frame.modelId,prompt:task.prompt},actor});current(actor);
        need(!frame.cancelled&&result.schema==='soty.personal-agent-turn.v1'&&result.requestId===frame.modelId&&result.status==='complete'
          &&Array.isArray(result.accounting)&&result.accounting.length>0);
        const text=validateLiveAnswerText({answer:result.answer,speech:firstAnswerParagraph(result.answer)});
        // Source Native commit independently rechecks its genuine consent/owner
        // and task signal at the exact publication boundary. Lost ACK is unknown.
        const delivery=await source.completeTask({actor,taskId:args.taskId,consentId:args.consentId,requestId:args.requestId,result:text});current(actor);
        need(!frame.cancelled&&delivery.taskId===args.taskId&&delivery.admitted===true);
        return {schema:'soty.native-voice-turn.v1',requestId:args.requestId,delivery:'native_commit_ack',playbackVerified:false};
      } finally{frame.finished=true;}
    },
    close(){if(closed)return;for(const frame of owned.values())if(!frame.finished){
      try{getConnect().withActorAuthorityFence(frame.actor,()=>true);void agent.executeAsync({op:'agent.cancel',args:{requestId:frame.modelId},actor:frame.actor}).catch(()=>{});}catch{}
    }closed=true;},
    status:()=>Object.freeze({sourcePortsImplemented:true,installedHttpMount:false,wholeVoiceReady:false,nativeAudioActivated:false})
  });
}

/** Existing Root capabilities auth/owner fences +coordinator. A fixed Source
 * reference/credential comes from installed private configuration, never RPC. */
export function createRootVoiceSourceBroker({getServices,getConnect,readReference,completeReference,sourceCredentialFor}){
  need(typeof getServices==='function'&&typeof getConnect==='function'&&typeof sourceCredentialFor==='function'&&readReference&&completeReference);
  function principal(actor,reference){getConnect().withActorAuthorityFence(actor,()=>true);
    const services=getServices(),delegated=services.capabilities.authenticateCredential(sourceCredentialFor({actor,reference}));
    services.capabilities.withOwnerAuthority({actor:delegated},owner=>need(owner.accountId===actor.accountId&&owner.deviceId===actor.deviceId));
    return {services,delegated};}
  async function invoke(actor,input,requestId,write){const reference=write?completeReference:readReference,{services,delegated}=principal(actor,reference);
    const contract=services.capabilities.external.getContract({actor:delegated,reference});
    need(contract?.executionEnabled===true,'voice_native_source_unavailable');
    need(Array.isArray(contract.effects)&&(write?contract.effects.length===1:contract.effects.length===0),'voice_source_contract_changed');
    const result=await services.capabilities.external[write?'invoke':'query']({actor:delegated,reference,
      idempotencyKey:`voice_${requestId}_${write?'complete':'read'}`,input});
    principal(actor,reference);need(result.resultUnavailable===false&&result.invocation.status==='succeeded','voice_native_source_unknown');return result.result;}
  return Object.freeze({
    readTask:({actor,taskId,consentId,requestId})=>invoke(actor,{operation:'voice.task.read',taskId,consentId},requestId,false),
    completeTask:({actor,taskId,consentId,requestId,result})=>invoke(actor,{operation:'voice.task.complete',taskId,consentId,result},requestId,true)
  });
}
