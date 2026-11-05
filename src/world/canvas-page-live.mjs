const HASH=/^[a-f0-9]{64}$/u;
const failure=code=>Object.assign(new Error(code),{code});
const authFailure=error=>error?.status===401||/(?:authentication|account_changed|profile_changed|device_revoked|actor_denied|identity_revoked|local_profile_missing|account_required)/u.test(String(error?.code??'').toLowerCase());

/** One current read cycle. Metadata never changes the client's authority.
 * A replacement read aborts its predecessor and also fences late adapters
 * which incorrectly ignore AbortSignal. Authentication loss is terminal for
 * this identity; the owner creates a new controller after an identity change. */
export function createCanvasPageLive({client,apply,onUnavailable,isCurrent,events=globalThis.window,
  online=()=>globalThis.navigator?.onLine!==false,
  timers={setTimeout:(fn,ms)=>globalThis.setTimeout(fn,ms),clearTimeout:id=>globalThis.clearTimeout(id)},
  retryDelays=[500,1000,2000,4000,8000,15000]}={}){
  if(!client||typeof client.load!=='function'||typeof client.wait!=='function'||typeof apply!=='function'
    ||typeof onUnavailable!=='function'||typeof isCurrent!=='function'||typeof online!=='function'
    ||typeof timers?.setTimeout!=='function'||typeof timers?.clearTimeout!=='function'
    ||events&&(!events.addEventListener||!events.removeEventListener)
    ||!Array.isArray(retryDelays)||!retryDelays.length||retryDelays.length>16
    ||retryDelays.some(value=>!Number.isSafeInteger(value)||value<1||value>30000))throw failure('canvas_page_live_configuration_invalid');
  const delays=[...retryDelays];
  let disposed=false,started=false,generation=0,revision='',flight=null,timer=null,retryStep=0,needsRetry=false,blocked=null,initial=null;
  const clearTimer=()=>{if(timer!==null){timers.clearTimeout(timer);timer=null;}};
  function dispose(){
    if(disposed)return;disposed=true;generation++;clearTimer();flight?.controller.abort();flight=null;revision='';
    events?.removeEventListener('online',onOnline);events?.removeEventListener('offline',onOffline);
  }
  function current(){if(disposed)return false;if(!isCurrent()){dispose();return false;}return true;}
  const owns=operation=>current()&&flight===operation&&operation.generation===generation&&!operation.controller.signal.aborted;
  function interrupt(){generation++;const previous=flight;flight=null;previous?.controller.abort();}
  function begin(kind){clearTimer();interrupt();const operation={kind,generation,controller:new AbortController(),promise:null};flight=operation;return operation;}
  async function call(operation,action){
    const signal=operation.controller.signal;let cancel;
    const interrupted=new Promise((_,reject)=>{cancel=()=>reject(failure('canvas_page_cancelled'));signal.addEventListener('abort',cancel,{once:true});});
    try{return await Promise.race([Promise.resolve().then(()=>{if(signal.aborted)throw failure('canvas_page_cancelled');return action(signal);}),interrupted]);}
    finally{signal.removeEventListener('abort',cancel);}
  }
  function retry(){
    if(timer!==null||!current()||blocked)return;
    const delay=delays[Math.min(retryStep++,delays.length-1)];
    timer=timers.setTimeout(()=>{
      timer=null;if(!current()||blocked)return;
      if(!online()){retry();return;}
      void load().catch(()=>{});
    },delay);
    timer?.unref?.();
  }
  function unavailable(error){
    revision='';needsRetry=true;
    if(authFailure(error)){blocked=error;needsRetry=false;clearTimer();}
    onUnavailable(error);
    if(!blocked)retry();
  }
  function publish(operation,view){
    if(!owns(operation))return false;
    if(!online())throw failure('canvas_page_offline');
    if(!HASH.test(view?.viewRevision??''))throw failure('canvas_page_response_invalid');
    revision=view.viewRevision;apply(view);
    if(!owns(operation))return false;
    retryStep=0;needsRetry=false;return true;
  }
  function follow(){
    if(!current()||!started||blocked||flight||!revision)return;
    if(!online()){onOffline();return;}
    const operation=begin('wait'),after=revision;
    operation.promise=(async()=>{
      try{
        const result=await call(operation,signal=>client.wait(after,{signal}));if(!owns(operation))return;
        if(result?.changed){const view=await call(operation,signal=>client.load(result.view,{signal}));publish(operation,view);}
      }catch(error){if(owns(operation))unavailable(error);}
      finally{if(flight===operation){flight=null;if(current()&&!needsRetry&&!blocked)queueMicrotask(follow);}}
    })();
  }
  function attach(){
    if(started)return;started=true;events?.addEventListener('online',onOnline);events?.addEventListener('offline',onOffline);
  }
  function load(first){
    if(!current())return Promise.reject(failure('canvas_page_cancelled'));
    if(blocked)return Promise.reject(blocked);
    attach();const operation=begin('load');
    operation.promise=(async()=>{
      try{
        if(!online())throw failure('canvas_page_offline');
        const view=await call(operation,signal=>client.load(first,{signal}));publish(operation,view);
      }catch(error){if(owns(operation)){unavailable(error);throw error;}}
      finally{if(flight===operation){flight=null;if(current()&&!needsRetry&&!blocked)queueMicrotask(follow);}}
    })();return operation.promise;
  }
  function onOffline(){
    if(!current()||blocked||needsRetry&&!revision&&!flight)return;
    interrupt();clearTimer();unavailable(failure('canvas_page_offline'));
  }
  function onOnline(){
    if(!current()||blocked||!online()||!needsRetry||flight?.kind==='load')return;
    clearTimer();void load().catch(()=>{});
  }
  return Object.freeze({
    start(){if(!initial)initial=started?(flight?.promise??Promise.resolve()):load();return initial;},
    refresh(first){retryStep=0;return load(first);},dispose,
  });
}
