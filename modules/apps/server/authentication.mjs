import {assertApps} from './protocol.mjs';
import {selectedRuntimeProfile} from './schema.mjs';
import {SELF_IP_PROFILE} from './endpoint-targets.mjs';

/** Public configuration, never an assertion that an app verifies its requests. */
export function appAuthenticationCapability({signer,issuer}={}){
  if(issuer!==undefined){
    let url;try{url=new URL(issuer);}catch{}
    assertApps(typeof issuer==='string'&&url&&url.href===issuer&&url.pathname==='/human-identity'&&!url.search&&!url.hash&&!url.username&&!url.password
      &&(url.protocol==='https:'||url.protocol==='http:'&&['localhost','127.0.0.1','[::1]'].includes(url.hostname)), 'app_identity_configuration_invalid',503);
  }
  return Object.freeze({schema:'soty.app-authentication-capability.v1',default:'soty-window',contract:'soty.relay-window-identity.v1',
    configured:typeof signer==='function',issuer:issuer??null,documentation:'/agents/shared-auth.md',sourceSdk:'/sdk/source-app/index.json'});
}

/** Called under existing app authority; no connector identifier leaves this DTO. */
export function appAuthenticationStatus({capability,appId,target,channel,surface='embedded'}){
  const missing=[];
  if(!capability.configured)missing.push('root_identity_signer_unavailable');
  if(!capability.issuer)missing.push('root_identity_issuer_unavailable');
  const selected=selectedRuntimeProfile(target.profile),relay=['soty.relay-restricted.v1',SELF_IP_PROFILE].includes(target.profile);
  const transport=surface==='native'?'native':selected?'source':relay?'relay':'unsupported';
  if(surface==='native')missing.push('embedded_window_required');
  else if(selected)missing.push('source_identity_contract_required');
  else if(!relay)missing.push('runtime_profile_unsupported');
  if(!channel||channel.ws.readyState!==1)missing.push('connector_offline');
  else{
    if(channel.bindingVersion!==2)missing.push('connector_binding_v2_required');
    if(channel.relayIdentity!==true)missing.push('connector_relay_identity_required');
  }
  return {schema:'soty.app-authentication-status.v1',appId,transport,supported:missing.length===0,missing,
    verification:{browser:'not_checked',server:'not_checked',endToEnd:'not_checked'},authentication:capability};
}
