import {validateAppHostSession,validateAppLaunchBinding,sameAppLaunchBinding,validateAppEntry,validateAppLaunchUrl} from './app-launch.mjs';
import {isLiveIdentityLoss} from '../platform/platform-live-client.mjs';

const fail=code=>{throw Object.assign(new Error(code),{code});};
const check=(value,code='app_host_session_changed')=>{if(!value)fail(code);};
const exact=(value,required,optional=[])=>value&&typeof value==='object'&&!Array.isArray(value)
  &&required.every(key=>Object.hasOwn(value,key))&&Object.keys(value).every(key=>required.includes(key)||optional.includes(key));
const sameProof=(a,b)=>['schema','frameId','lease','revision'].every(key=>a?.[key]===b?.[key]);
const fields=['hostSession','hostSessionExpiresAt','entry','launchBinding'];

/** Host continuity only. Neither a page message nor an ordinary cookie is a
 * Source ACK. No application request is read, saved, or replayed here. */
export function createAppHostSessionRenewal({readBinding,isCurrent,context,request,bootstrap,confirm,commit,close,
  shellUrl,cleanupForReply,onState=()=>{},clock=Date.now,schedule=setTimeout,cancel=clearTimeout,randomId=()=>crypto.randomUUID()}){
  let disposed=false,flight=null,pending=null,timer=null,failures=0,started=false,blocked=false,owned=readBinding();
  const notify=(value,error)=>{if(!disposed&&isCurrent())onState(value,error);};
  const live=binding=>!disposed&&isCurrent()&&readBinding()===binding;
  const clearTimer=()=>{if(timer!==null){cancel(timer);timer=null;}};
  const proof=binding=>({frameId:binding.hostSession.frameId,lease:binding.hostSession.lease,revision:binding.hostSession.revision});
  const retire=binding=>{if(binding)void Promise.resolve().then(()=>binding.cleanup?binding.cleanup():close(proof(binding))).catch(()=>{});};
  function validateState(value,binding,kind,action){
    const required=kind==='context'?['schema','state',...fields]:kind==='confirm'?['schema','requestId','confirmed',...fields]
      :['launchUrl','expiresAt','hostSessionRequestId',...fields];
    check(exact(value,required,['runtimeOrigin','ok'])&&(!Object.hasOwn(value,'ok')||value.ok===true));
    if(kind==='context')check(value.schema==='soty.app-host-session-context.v1'&&['current','renew-required','admitting'].includes(value.state));
    if(kind==='confirm')check(value.schema==='soty.app-host-session-confirmation.v1'&&value.confirmed===true&&value.requestId===action.requestId);
    const session=validateAppHostSession(value.hostSession),pin=validateAppLaunchBinding(value.launchBinding);
    const entry=validateAppEntry(value.entry,{appId:binding.entry.appId,domainId:binding.entry.domainId,path:binding.entry.path},shellUrl);
    check(entry.origin===binding.entry.origin&&sameAppLaunchBinding(pin,binding.launchBinding)
      &&(value.runtimeOrigin??entry.origin)===binding.runtimeOrigin);
    check(session.frameId===binding.hostSession.frameId&&session.expiresAt>clock());
    if(kind==='renew')check(session.revision===binding.hostSession.revision+1&&session.lease!==binding.hostSession.lease);
    else check(sameProof(session,binding.hostSession)&&(kind==='context'?session.expiresAt>=binding.hostSession.expiresAt:session.expiresAt===binding.hostSession.expiresAt));
    const until=value.hostSessionExpiresAt;
    check(kind==='context'&&value.state!=='current'?until===null
      :Number.isSafeInteger(until)&&until>clock()&&until<=session.expiresAt);
    if(kind==='renew'){
      check(value.hostSessionRequestId===action.requestId&&Number.isSafeInteger(value.expiresAt)&&value.expiresAt>clock());
      const boot=new URL(validateAppLaunchUrl(value.launchUrl,shellUrl));
      check(boot.origin===binding.runtimeOrigin&&[...boot.searchParams.keys()].join(',')==='path,mode'
        &&boot.searchParams.get('path')===binding.entry.path&&boot.searchParams.get('mode')==='renew');
    }
    const cleanup=cleanupForReply?.(value);check(cleanup===undefined||cleanup===null||typeof cleanup==='function');
    return Object.freeze({...value,hostSession:session,launchBinding:pin,entry,runtimeOrigin:binding.runtimeOrigin,...(cleanup?{cleanup}:{})});
  }
  function next(){
    clearTimer();const binding=readBinding();
    if(!started||blocked||!binding||!live(binding))return;
    const until=binding.hostSessionExpiresAt??clock();
    const delay=failures?Math.min(15000,500*2**Math.min(failures-1,5)):Math.max(250,Math.min(30000,until-clock()-30000));
    timer=schedule(()=>{timer=null;void renew('near-expiry').catch(()=>{});},delay);
  }
  function adopt(action,value){
    if(!live(action.binding))return false;
    if(!commit(action.binding,value))return false;
    owned=readBinding();pending=null;failures=0;return true;
  }
  async function finish(action){
    try{
      const value=validateState(await confirm({...proof(action.reply),requestId:action.requestId}),action.reply,'confirm',action);
      if(!live(action.binding)){retire(action.reply);return false;}
      if(!adopt(action,value))return false;notify('confirmed');return true;
    }catch(error){
      if(error?.code!=='app_host_session_unconfirmed')throw error;
      // Confirm may have committed before its ACK and the API were lost. Only
      // this known candidate can be read/adopted; no successor is discovered.
      let value;
      try{value=validateState(await context({...proof(action.reply),currentEntryPath:action.binding.entry.path}),action.reply,'context');}
      catch(error){if(error?.code==='app_host_session_conflict')return null;throw error;}
      if(!live(action.binding))return false;
      if(!adopt(action,value))return false;
      if(value.state==='current'){notify('confirmed');return true;}
      notify('pending');return false;
    }
  }
  async function run(reason){
    let action=pending;
    let binding=readBinding();if(!binding||!live(binding))return false;
    if(action&&action.binding!==binding){retire(action.reply??action.binding);pending=null;return false;}
    if(action?.reply){
      const outcome=await finish(action);if(outcome!==null)return outcome;
    }
    if(!pending){
      const value=validateState(await context({...proof(binding),currentEntryPath:binding.entry.path}),binding,'context');
      if(!live(binding))return false;
      if(value.hostSession.expiresAt!==binding.hostSession.expiresAt||value.hostSessionExpiresAt!==binding.hostSessionExpiresAt||value.cleanup&&value.cleanup!==binding.cleanup){
        if(!commit(binding,value))return false;binding=readBinding();owned=binding;if(!binding||!live(binding))return false;
      }
      if(value.state==='admitting'){notify('pending');return false;}
      if(reason==='near-expiry'&&value.state==='current'&&value.hostSessionExpiresAt-clock()>30000){
        notify('confirmed');return true;
      }
      action=pending={binding,requestId:randomId(),reply:null,controller:new AbortController()};
    }else action=pending;
    if(!live(action.binding))return false;
    notify('pending');
    const reply=validateState(await request({...proof(action.binding),requestId:action.requestId,
      parentOrigin:new URL(shellUrl).origin,currentEntryPath:action.binding.entry.path}),action.binding,'renew',action);
    action.reply=reply;
    if(!live(action.binding)){retire(reply);return false;}
    try{await bootstrap(reply,{signal:action.controller.signal});}catch{/* The signed check, not a message, decides the outcome. */}
    if(!live(action.binding)){retire(reply);return false;}
    return await finish(action)===true;
  }
  async function renew(reason='near-expiry'){
    if(disposed||blocked||!isCurrent())return false;
    if(flight)return flight;
    const binding=readBinding();if(!binding||!live(binding))return false;
    const until=binding.hostSessionExpiresAt;
    if(reason==='near-expiry'&&failures===0&&!pending&&until!==null&&until-clock()>30000){next();return true;}
    clearTimer();
    const task=(async()=>{try{const result=await run(reason);failures=result?0:failures+1;return result;}
      catch(error){failures++;blocked=['app_host_session_closed','app_host_session_changed','app_host_session_conflict','app_host_session_request_conflict'].includes(error?.code)
        ||error?.status===401||error?.status===403||isLiveIdentityLoss(error);notify(blocked?'denied':'unavailable',error);throw error;}})();
    flight=task;
    try{return await task;}finally{if(flight===task)flight=null;next();}
  }
  return Object.freeze({renew,start(){if(!disposed){owned??=readBinding();started=true;next();}},
    dispose(){if(disposed)return;disposed=true;started=false;clearTimer();pending?.controller.abort();retire(pending?.reply);retire(owned);pending=null;}});
}
