import {CALL_ICE_LIMITS,canonicalCallIceUrl,closedIceRecord,validateCallIceReply} from '../src/transport/human-call-ice.mjs';
const failure=code=>{throw Object.assign(Error(code),{code});};
const validId=value=>typeof value==='string'&&/^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
const keys=['accountId','deviceId','callId','epoch','joinRef'];
const validScope=scope=>closedIceRecord(scope,keys)&&keys.filter(key=>key!=='epoch').every(key=>validId(scope[key]))&&Number.isSafeInteger(scope.epoch)&&scope.epoch>0;
const same=(left,right)=>keys.every(key=>left[key]===right[key]);
function policyList(value,maximum){
  if(!Array.isArray(value)||Object.getPrototypeOf(value)!==Array.prototype||value.length<1||value.length>maximum)failure('human_call_ice_policy_invalid');
  const descriptors=Object.getOwnPropertyDescriptors(value);
  if(Reflect.ownKeys(descriptors).length!==value.length+1)failure('human_call_ice_policy_invalid');
  for(let n=0;n<value.length;n++)if(!Object.hasOwn(descriptors,String(n))||!Object.hasOwn(descriptors[String(n)],'value'))failure('human_call_ice_policy_invalid');
  return value;
}
function policyOf(value){
  if(value===null)return null;
  if(!closedIceRecord(value,['schema','servers','ttlMs'],['issueCredentials'])||value.schema!=='soty.human-call-ice-policy.v1'
    ||!Array.isArray(value.servers)||value.servers.length<1||value.servers.length>CALL_ICE_LIMITS.servers
    ||!Number.isSafeInteger(value.ttlMs)||value.ttlMs<CALL_ICE_LIMITS.ttlMinMs||value.ttlMs>CALL_ICE_LIMITS.ttlMaxMs)failure('human_call_ice_policy_invalid');
  const servers=policyList(value.servers,CALL_ICE_LIMITS.servers).map(server=>{
    if(!closedIceRecord(server,['urls'])||!Array.isArray(server.urls)||server.urls.length<1||server.urls.length>CALL_ICE_LIMITS.urlsPerServer)failure('human_call_ice_policy_invalid');
    const urls=policyList(server.urls,CALL_ICE_LIMITS.urlsPerServer).map(canonicalCallIceUrl);if(new Set(urls).size!==urls.length)failure('human_call_ice_policy_invalid');
    const turn=urls.some(url=>url.startsWith('turn:')||url.startsWith('turns:'));
    if(turn!==urls.every(url=>url.startsWith('turn:')||url.startsWith('turns:')))failure('human_call_ice_policy_invalid');
    return Object.freeze({urls:Object.freeze(urls),turn});
  });
  const turn=servers.some(server=>server.turn);
  if(turn&&typeof value.issueCredentials!=='function'||Object.hasOwn(value,'issueCredentials')&&typeof value.issueCredentials!=='function')failure('human_call_ice_policy_invalid');
  return Object.freeze({servers:Object.freeze(servers),turn,ttlMs:value.ttlMs,issueCredentials:value.issueCredentials});
}

/** Trusted synchronous composition. current(scope) must perform actual host
 * authorization and return true. Never expose issue/retire directly as HTTP. */
export function createHumanCallIce({policy=null,clock=Date.now}={}){
  if(typeof clock!=='function')throw TypeError('human_call_ice_clock_invalid');
  let configured;try{configured=policyOf(policy);}catch{failure('human_call_ice_policy_invalid');}
  const records=new Map(),maxJoins=64;
  const now=()=>{const value=clock();if(!Number.isSafeInteger(value)||value<0)failure('human_call_ice_clock_invalid');return value;};
  const authorize=(scope,current)=>{let ok=false;try{ok=typeof current==='function'&&current(scope)===true;}catch{}if(!ok)failure('human_call_ice_denied');};
  const status=()=>Object.freeze({configured:Boolean(configured),turnConfigured:Boolean(configured?.turn),qualified:false,retainedJoins:records.size});
  return Object.freeze({status,
    issue(scope,args,current){
      if(!configured)failure('human_call_ice_unconfigured');
      if(!validScope(scope)||!closedIceRecord(args,['requestId','expectedRevision'])||!validId(args.requestId)
        ||!Number.isSafeInteger(args.expectedRevision)||args.expectedRevision<0||args.expectedRevision>=Number.MAX_SAFE_INTEGER)failure('human_call_ice_invalid');
      // Join-scoped canonical identity keeps retries exact without an unbounded
      // ledger of every historical random request ID during a long call.
      if(args.requestId!=='ice_'+args.expectedRevision)failure('human_call_ice_request_conflict');
      const bound=Object.freeze({...scope});authorize(bound,current);const at=now(),previous=records.get(bound.joinRef);
      if(previous&&!same(previous.scope,bound))failure('human_call_ice_denied');
      const cached=[previous?.current,previous?.previous].find(value=>value?.requestId===args.requestId);
      if(cached){
        if(cached.expectedRevision!==args.expectedRevision)failure('human_call_ice_request_conflict');
        if(cached.state!=='issued')failure('human_call_ice_unknown');
        if(cached.reply.expiresAt<=at)failure('human_call_ice_expired');
        authorize(bound,current);
        return validateCallIceReply(cached.reply,bound,{now:at,allowedUrls:configured.servers.flatMap(server=>server.urls)});
      }
      if(previous?.current.state!=='issued'&&previous)failure('human_call_ice_unknown');
      if(args.expectedRevision!==(previous?.current.revision??0))failure('human_call_ice_revision_conflict');
      if(previous&&at-previous.current.issuedAt<30_000)failure('human_call_ice_rate_limited');
      if(!previous&&records.size>=maxJoins)failure('human_call_ice_capacity');
      const intent={requestId:args.requestId,expectedRevision:args.expectedRevision,revision:args.expectedRevision+1,
        issuedAt:at,state:'issuing',reply:null};
      const frame={scope:bound,current:intent,previous:previous?.current??null};records.set(bound.joinRef,frame);
      try{
        const expiresAt=Math.floor((at+configured.ttlMs)/1000)*1000;
        let credential;
        if(configured.turn){
          credential=configured.issueCredentials(Object.freeze({...bound,requestId:args.requestId,revision:intent.revision,expiresAt}));
          if(credential&&typeof credential.then==='function'){
            void Promise.resolve(credential).catch(()=>{});failure('human_call_ice_issuer_async');
          }
          if(!closedIceRecord(credential,['username','credential','expiresAt'])||credential.expiresAt!==expiresAt)failure('human_call_ice_issuer_invalid');
        }
        authorize(bound,current);
        if(records.get(bound.joinRef)!==frame)failure('human_call_ice_denied');
        const iceServers=configured.servers.map(server=>({urls:[...server.urls],...(server.turn?{username:credential.username,credential:credential.credential}:{})}));
        const reply=validateCallIceReply({schema:'soty.human-call-ice.v1',...bound,revision:intent.revision,expiresAt,iceServers},bound,
          {now:now(),allowedUrls:configured.servers.flatMap(server=>server.urls)});
        intent.reply=reply;intent.state='issued';return reply;
      }catch(error){
        intent.state='unknown';
        const allowed=['human_call_ice_denied','human_call_ice_issuer_async','human_call_ice_issuer_invalid','human_call_ice_clock_invalid'];
        failure(allowed.includes(error?.code)?error.code:'human_call_ice_unknown');
      }
    },
    retire(joinRef){if(!validId(joinRef))failure('human_call_ice_invalid');records.delete(joinRef);},
    close(){records.clear();configured=null;},
  });
}
