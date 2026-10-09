const S='soty.kvartalufa-voice-carrier.v1';
const id=x=>typeof x==='string'&&/^[A-Za-z0-9_.:-]{8,160}$/u.test(x);
const exact=(x,keys)=>x&&typeof x==='object'&&!Array.isArray(x)&&Object.keys(x).length===keys.length&&keys.every(k=>Object.hasOwn(x,k));
/** Ordinary registered app frame only. It does not grant Native recording or
 * Source access; actual signed Root operations enter the fixed broker/current grant. */
export function mountRootVoiceCarrier({view,getFrame,isCurrent,client,mount,appId,accountId}){
  let disposed=false;const owned=new Map();
  const matches=e=>{const frame=getFrame();try{return !disposed&&isCurrent()&&view.location.origin===mount.parentOrigin&&mount.scope==='kvartalufa.voice'&&mount.appId===appId
    &&frame&&new URL(frame.src).origin===mount.frameOrigin&&e.source===frame.contentWindow&&e.origin===mount.frameOrigin;}catch{return false;}};
  const receive=async event=>{
    if(!matches(event))return;const input=event.data;
    const scope=exact(input,['schema','method','requestId'])&&input.method==='scope';
    const task=exact(input,['schema','method','requestId','taskId','consentId'])&&['turn','cancel'].includes(input.method)&&[input.taskId,input.consentId].every(id);
    if((!scope&&!task)||input.schema!==S||!id(input.requestId))return;
    const state=await client.getLocalState();if(!matches(event)||state.accountId!==accountId||!state.deviceId||state.current?.revoked!==false)return;
    if(scope){event.source.postMessage({schema:S,method:'scope',requestId:input.requestId,accountId:state.accountId,deviceId:state.deviceId},mount.frameOrigin);return;}
    const key=JSON.stringify([input.consentId,input.taskId]);
    if(input.method==='turn'){if(owned.has(key)||owned.size>=128)return;owned.set(key,{requestId:input.requestId,accountId:state.accountId,deviceId:state.deviceId,finished:false});}
    else if(!owned.has(key))return;
    const reply=async outcome=>{const original=owned.get(key),after=await client.getLocalState();
      if(!original||disposed||!matches(event)||after.accountId!==original.accountId||after.deviceId!==original.deviceId||after.current?.revoked!==false)return;
      event.source.postMessage({schema:S,method:'result',requestId:original.requestId,taskId:input.taskId,consentId:input.consentId,outcome},mount.frameOrigin);};
    try{const value=await client.extension(input.method==='turn'?'voice.agent.turn':'voice.agent.cancel',input.method==='turn'
      ?{requestId:input.requestId,taskId:input.taskId,consentId:input.consentId}:{requestId:owned.get(key).requestId},{expectedAccountId:accountId});
      if(input.method==='turn'){
        const binding=value?.ok===true&&value.schema==='soty.native-voice-turn.v1'&&value.requestId===input.requestId&&value.taskId===input.taskId
          &&value.consentId===input.consentId&&value.playbackVerified===false;
        if(binding&&exact(value,['ok','schema','requestId','taskId','consentId','kind','delivery','playbackVerified'])
          &&value.kind==='source-permission'&&value.delivery==='native_commit_ack')await reply('committed');
        else if(binding&&exact(value,['ok','schema','requestId','taskId','consentId','delivery','playbackVerified'])
          &&value.delivery==='pre_complete_denied')await reply('pre_complete_denied');
        else if(value?.kind==='source-permission'||value?.delivery!=='native_commit_ack')await reply('unknown');
      }}
    catch{if(input.method==='turn')try{await reply('unknown');}catch{/* Closed frame/local profile cannot publish a result. */}}
    finally{if(input.method==='turn'&&owned.has(key))owned.get(key).finished=true;}
  };
  view.addEventListener('message',receive);
  return()=>{if(disposed)return;disposed=true;view.removeEventListener('message',receive);
    for(const frame of owned.values())if(!frame.finished)void client.extension('voice.agent.cancel',{requestId:frame.requestId},{expectedAccountId:frame.accountId}).catch(()=>{});};
}
