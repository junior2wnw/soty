export const CALL_ICE_LIMITS=Object.freeze({servers:4,urlsPerServer:4,urlLength:256,usernameLength:128,credentialLength:512,ttlMinMs:60_000,ttlMaxMs:300_000});
const fail=()=>{throw Object.assign(Error('human_call_ice_invalid'),{code:'human_call_ice_invalid'});};
const id=value=>typeof value==='string'&&/^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
const positive=value=>Number.isSafeInteger(value)&&value>0;
export function closedIceRecord(value,required,optional=[]){
  if(!value||typeof value!=='object'||![Object.prototype,null].includes(Object.getPrototypeOf(value)))return false;
  const descriptors=Object.getOwnPropertyDescriptors(value),allowed=new Set([...required,...optional]);
  return Reflect.ownKeys(descriptors).every(key=>typeof key==='string'&&allowed.has(key)&&Object.hasOwn(descriptors[key],'value')&&descriptors[key].enumerable===true)
    &&required.every(key=>Object.hasOwn(descriptors,key));
}
function list(value,maximum){
  if(!Array.isArray(value)||Object.getPrototypeOf(value)!==Array.prototype||value.length<1||value.length>maximum)fail();
  const descriptors=Object.getOwnPropertyDescriptors(value);
  if(Reflect.ownKeys(descriptors).length!==value.length+1||!Object.hasOwn(descriptors,'length'))fail();
  for(let n=0;n<value.length;n++)if(!Object.hasOwn(descriptors,String(n))||!Object.hasOwn(descriptors[String(n)],'value'))fail();
  return value;
}
/** Intentional narrow URL subset. No URI credentials, path, fragment, port0 or
 * external fetch occurs here; trusted host policy controls the actual servers. */
export function canonicalCallIceUrl(value){
  try{
    if(typeof value!=='string'||value.length>CALL_ICE_LIMITS.urlLength)fail();
    const match=/^(stun|stuns|turn|turns):(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9.-]+)(?::([0-9]{1,5}))?(?:\?transport=(udp|tcp))?$/u.exec(value);
    if(!match)fail();const[,scheme,rawHost,portText,transport]=match;
    if((scheme==='stun'||scheme==='stuns')&&transport||scheme==='turns'&&transport==='udp')fail();
    let host=rawHost.toLowerCase();
    if(host.startsWith('[')){if(new URL('https://'+host+'/').hostname.toLowerCase()!==host)fail();}
    else if(host.length>253||host.split('.').some(label=>!label||label.length>63||!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/u.test(label)))fail();
    const port=portText===undefined?undefined:Number(portText);if(port!==undefined&&(!Number.isInteger(port)||port<1||port>65535))fail();
    return scheme+':'+host+(port===undefined?'':':'+port)+(transport?'?transport='+transport:'');
  }catch{fail();}
}
function secret(value,max){return typeof value==='string'&&value.length>0&&value.length<=max&&/^[A-Za-z0-9+/_=:.-]+$/u.test(value);}
export function validateCallIceReply(value,scope,{now=Date.now(),allowedUrls}={}){
  try{
    if(!closedIceRecord(scope,['accountId','deviceId','callId','epoch','joinRef'])
      ||!['accountId','deviceId','callId','joinRef'].every(key=>id(scope[key]))||!positive(scope.epoch))fail();
    if(!Number.isSafeInteger(now)||now<0||!closedIceRecord(value,['schema','accountId','deviceId','callId','epoch','joinRef','revision','expiresAt','iceServers'])
      ||value.schema!=='soty.human-call-ice.v1'||!Object.keys(scope).every(key=>value[key]===scope[key])||!positive(value.revision)
      ||!Number.isSafeInteger(value.expiresAt)||value.expiresAt<=now||value.expiresAt>now+CALL_ICE_LIMITS.ttlMaxMs)fail();
    const servers=list(value.iceServers,CALL_ICE_LIMITS.servers).map(server=>{
      if(!closedIceRecord(server,['urls'],['username','credential']))fail();
      const urls=list(server.urls,CALL_ICE_LIMITS.urlsPerServer).map(url=>{
        const normal=canonicalCallIceUrl(url);if(normal!==url||allowedUrls&&!allowedUrls.includes(url))fail();return normal;
      });
      if(new Set(urls).size!==urls.length)fail();
      const turn=urls.some(url=>url.startsWith('turn:')||url.startsWith('turns:'));
      if(turn!==urls.every(url=>url.startsWith('turn:')||url.startsWith('turns:')))fail();
      if(turn){if(!secret(server.username,CALL_ICE_LIMITS.usernameLength)||!secret(server.credential,CALL_ICE_LIMITS.credentialLength))fail();}
      else if(Object.hasOwn(server,'username')||Object.hasOwn(server,'credential'))fail();
      return Object.freeze({urls:Object.freeze(urls),...(turn?{username:server.username,credential:server.credential}:{})});
    });
    return Object.freeze({schema:value.schema,...scope,revision:value.revision,expiresAt:value.expiresAt,iceServers:Object.freeze(servers)});
  }catch{fail();}
}
