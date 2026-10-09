import { createRootNativeVoiceExtension,createRootVoiceSourceBroker } from '../modules/native-voice/root-agent-extension.mjs';
const need=value=>{if(!value)throw Object.assign(new Error('voice_installation_invalid'),{code:'voice_installation_invalid'});};
const reference=v=>v&&Object.keys(v).sort().join(',')==='capabilityId,digest,version'&&typeof v.capabilityId==='string'
  &&Number.isSafeInteger(v.version)&&v.version>0&&typeof v.digest==='string'&&/^[a-f0-9]{64}$/.test(v.digest);
/** Root host installation only; signed calls never select profile, credential,
 * URL or provider. Existing Root personal agent retains finance/turn receipt ownership. */
export function createNativeVoiceComposition({getConnect,getServices,agent,configuration={}}){
  need(configuration&&typeof configuration==='object'&&!Array.isArray(configuration)
    &&Object.keys(configuration).every(k=>['enabled','readReference','completeReference','sourceCredentialFor','publicMount'].includes(k)));
  if(configuration.enabled!==true)return Object.freeze({operations:new Set(),close(){},publicMount:()=>null,status:()=>({configured:false,wholeVoiceReady:false})});
  need(typeof getConnect==='function'&&typeof getServices==='function'&&typeof agent?.executeAsync==='function'
    &&reference(configuration.readReference)&&reference(configuration.completeReference)&&typeof configuration.sourceCredentialFor==='function');
  const mount=configuration.publicMount;need(mount&&Object.keys(mount).sort().join(',')==='appId,frameOrigin,nativeOrigin,parentOrigin,source'
    &&/^app-[a-f0-9]{32}$/.test(mount.appId));
  for(const key of ['frameOrigin','nativeOrigin','parentOrigin']){const url=new URL(mount[key]);need(url.protocol==='https:'&&url.origin===mount[key]);}
  need(mount.nativeOrigin===mount.frameOrigin);
  need(mount.source&&Object.keys(mount.source).sort().join(',')==='digest,id,version'&&typeof mount.source.id==='string'
    &&Number.isSafeInteger(mount.source.version)&&mount.source.version>0&&/^[a-f0-9]{64}$/.test(mount.source.digest));
  const publicMount=Object.freeze({...mount,source:Object.freeze({...mount.source}),scope:'kvartalufa.voice'});
  const source=createRootVoiceSourceBroker({getConnect,getServices,readReference:configuration.readReference,
    completeReference:configuration.completeReference,sourceCredentialFor:configuration.sourceCredentialFor});
  const extension=createRootNativeVoiceExtension({getConnect,agent,source});let closed=false;
  return Object.freeze({...extension,publicMount:()=>closed?null:publicMount,status:()=>({configured:!closed,wholeVoiceReady:false,nativeAudioActivated:false}),
    close(){if(closed)return;closed=true;extension.close();}});
}
