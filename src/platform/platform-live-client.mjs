const schema='soty.platform-live.v1';
const identityLossCodes=new Set(['ACTIVE_PROFILE_CHANGED','directory_account_changed','field_account_changed','live_authentication_required','live_account_changed','device_revoked','authentication_required','unauthorized','account_mismatch']);
export const isLocalReadConflict=error=>error?.code==='ACTIVE_PROFILE_CHANGED'&&error.cause?.code==='LOCAL_CONFLICT';
export const isLiveIdentityLoss=error=>!isLocalReadConflict(error)&&identityLossCodes.has(String(error?.code||''));
const unsupported=error=>/UNKNOWN_OPERATION|OPERATION_NOT_FOUND|UNSUPPORTED_OPERATION|platform_live_unavailable/iu.test(String(error?.code||''));

/** One account-bound wait, followed by fresh ACL reads. The feed carries no display data. */
export function createPlatformLiveClient({api,accountId,isCurrent=()=>true,watch=()=>[],onInvalidate,onAccessLost=()=>{},onStatus=()=>{},
  page=globalThis.document,schedule=setTimeout,cancel=clearTimeout}) {
  let disposed=false,paused=page?.visibilityState==='hidden',cursor,epoch=0,timer=null,failures=0,inFlight=false;
  const active=()=>!disposed&&!paused&&isCurrent();
  const report=status=>{if(!disposed&&isCurrent())onStatus(status);};
  const clear=()=>{if(timer!==null)cancel(timer);timer=null;};
  const later=delay=>{clear();if(active())timer=schedule(()=>{timer=null;void run();},delay);};
  function watched(){
    const refs=watch();if(!Array.isArray(refs))throw new TypeError('Invalid live watch');
    const result=new Map();
    for(const ref of refs){if(!ref||!['app','person','community','object'].includes(ref.kind)||typeof ref.id!=='string'||!ref.id||ref.id.length>160)continue;result.set(`${ref.kind}:${ref.id}`,{kind:ref.kind,id:ref.id});}
    return [...result.values()].slice(0,256);
  }
  async function run(){
    if(!active()||inFlight)return;
    const generation=epoch;inFlight=true;
    try {
      const result=await api.request('platform.live.wait',{expectedAccountId:accountId,...(cursor?{cursor}:{}),waitMs:15000,watch:watched()});
      if(!active()||generation!==epoch)return;
      if(result?.schema!==schema||typeof result.cursor!=='string'||!result.cursor||result.cursor.length>2048||typeof result.changed!=='boolean'||typeof result.reset!=='boolean')throw new Error('invalid_live_response');
      // Retain the old cursor until every dependent view has completed its fresh read.
      if(result.changed||result.reset)await onInvalidate({reset:result.reset});
      if(!active()||generation!==epoch)return;
      cursor=result.cursor;failures=0;report('live');later(0);
    } catch(error){
      if(!active()||generation!==epoch)return;
      if(isLiveIdentityLoss(error)){paused=true;report('unavailable');try{await onAccessLost(error);}catch{/* Authority stays unavailable even if its view could not refresh. */}return;}
      if(unsupported(error)){paused=true;report('unavailable');return;}
      failures++;report('reconnecting');later(Math.min(15000,500*2**Math.min(failures-1,5)));
    } finally {
      inFlight=false;
      // A visibility change may retire the old wait while a new generation is ready.
      if(active()&&generation!==epoch)later(0);
    }
  }
  const visibility=()=>{paused=page?.visibilityState==='hidden';epoch++;clear();if(paused)report('paused');else{report('connecting');later(0);}};
  page?.addEventListener?.('visibilitychange',visibility);
  report(paused?'paused':'connecting');later(0);
  return {reconnect(){if(disposed)return;paused=page?.visibilityState==='hidden';epoch++;failures=0;clear();if(active()){report('connecting');later(0);}},
    dispose(){disposed=true;epoch++;clear();page?.removeEventListener?.('visibilitychange',visibility);},
    status(){return {paused,disposed,inFlight,failures};}};
}
