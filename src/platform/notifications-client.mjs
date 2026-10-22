const failure = code => Object.assign(new Error(code),{code});
const exact = (value,names) => value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).every(key=>names.includes(key));
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
const notificationId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{32}$/u.test(value);
const bindingId = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const validTarget = value => value?.kind === 'room'
  ? exact(value,['kind','communityId']) && id(value.communityId)
  : exact(value,['kind','threadId','peerAccountId','relationshipId']) && value.kind === 'direct' && [value.threadId,value.peerAccountId,value.relationshipId].every(id);
const keyBytes = value => Uint8Array.from(atob(value.replace(/-/gu,'+').replace(/_/gu,'/')),character=>character.charCodeAt(0));
const sameKey = (left,right) => left && new Uint8Array(left).length === right.length && new Uint8Array(left).every((value,index)=>value===right[index]);

/** App-lifetime controller. Construction/refresh never requests browser permission.
 * The settings panel merely subscribes; its removal does not disable background delivery. */
export function createNotificationsController({client,accountId,isCurrent=()=>true,onOpen=()=>{throw failure('push_notification_unavailable');},
  serviceWorker=globalThis.navigator?.serviceWorker,notification=globalThis.Notification,pushSupported=typeof globalThis.PushManager === 'function',
  location=globalThis.location,history=globalThis.history,makeChannel=()=>new MessageChannel(),
  fingerprint=async value=>[...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value)))].map(byte=>byte.toString(16).padStart(2,'0')).join('')}={}) {
  let disposed=false,generation=0,identity,registration,report,busy=false,opening,pendingNotification,entryHandled=false;
  let phase='checking',code=null;const listeners=new Set();
  const view=()=>({phase,configured:report?.configured===true,supported:Boolean(registration?.active&&notification&&pushSupported),enabled:phase==='on',busy,code});
  const notify=()=>{if(!disposed)for(const listener of listeners){try{listener(view());}catch{}}};
  const current=async expected=>{
    if(disposed||!isCurrent())throw failure('push_actor_denied');const fresh=await client.getLocalState();
    if(disposed||!isCurrent()||fresh.accountId!==accountId||!id(fresh.deviceId)||!Number.isSafeInteger(fresh.localRevision)
      ||expected&&(fresh.deviceId!==expected.deviceId||fresh.localRevision!==expected.localRevision))throw failure('push_actor_denied');return fresh;
  };
  const rpc=async(op,args,owned)=>{await current(owned);const result=await client.extension(op,{expectedAccountId:accountId,...args},{expectedAccountId:accountId});await current(owned);return result;};
  const workerMessage=(type,binding,enabled)=>new Promise((resolve,reject)=>{
    const worker=registration?.active;if(!worker){resolve();return;}
    const channel=makeChannel(),timer=setTimeout(()=>{channel.port1.close();reject(failure('push_worker_unavailable'));},2000);
    channel.port1.onmessage=event=>{clearTimeout(timer);channel.port1.close();event.data?.ok===true?resolve():reject(failure('push_worker_unavailable'));};
    try{worker.postMessage({type,bindingId:binding,...(type==='SOTY_PUSH_BIND'?{enabled:enabled===true}:{})},[channel.port2]);}catch{clearTimeout(timer);channel.port1.close();reject(failure('push_worker_unavailable'));}
  });
  const clearBinding=()=>report?.bindingId?workerMessage('SOTY_PUSH_CLEAR',report.bindingId).catch(()=>{}):Promise.resolve();
  const stopped=()=>{opening?.abort();opening=undefined;};
  async function open(request,binding) {
    if(!notificationId(request)||binding!==undefined&&binding!==report?.bindingId||!identity)return false;
    stopped();const abort=new AbortController();opening=abort;const owned=identity,version=generation;
    const assertCurrent=async()=>{await current(owned);if(opening!==abort||abort.signal.aborted||version!==generation)throw failure('push_actor_denied');};
    try {
      await assertCurrent();pendingNotification={notificationId:request,bindingId:report.bindingId};
      const result=await rpc('notifications.open',{notificationId:request},owned);await assertCurrent();
      if(!exact(result,['ok','schema','target'])||result.ok===false||result.schema!=='soty.push-open.v1'||!validTarget(result.target))throw failure('push_invalid');
      await onOpen({...result.target},{assertCurrent,signal:abort.signal});await assertCurrent();
      if(pendingNotification?.notificationId===request)pendingNotification=undefined;
      if(location?.href&&history){const url=new URL(location.href);if(url.searchParams.get('notification')===request){url.searchParams.delete('notification');history.replaceState(history.state,'',url.href);}}
      return true;
    } catch(cause) { if(!disposed&&version===generation&&opening===abort){code=cause?.code==='push_navigation_blocked'?'push_navigation_blocked':'push_notification_unavailable';notify();}return false; }
    finally {if(opening===abort)opening=undefined;}
  }
  const message=event=>{
    if(event.source!==registration?.active||!exact(event.data,['type','notificationId','bindingId'])||event.data.type!=='SOTY_PUSH_OPEN')return;
    void open(event.data.notificationId,event.data.bindingId);
  };
  serviceWorker?.addEventListener?.('message',message);
  async function refresh() {
    const version=++generation;stopped();phase='checking';code=null;notify();
    try {
      const owned=await current(),result=await rpc('notifications.status',{},owned);
      if(version!==generation||disposed)return view();
      if(!exact(result,['ok','schema','configured','enabled','bindingId','publicKey','subscriptionId'])||result.ok===false||result.schema!=='soty.push-status.v1'
        ||typeof result.configured!=='boolean'||typeof result.enabled!=='boolean'||!bindingId(result.bindingId)
        ||result.configured&&!(typeof result.publicKey==='string'&&/^[A-Za-z0-9_-]{87}$/u.test(result.publicKey))
        ||result.enabled&&!bindingId(result.subscriptionId))throw failure('push_invalid');
      identity=owned;report=result;registration=await serviceWorker?.getRegistration?.('/');await current(owned);
      if(version!==generation||disposed)return view();
      if(!result.configured||!view().supported){phase='unavailable';await workerMessage('SOTY_PUSH_BIND',report.bindingId,false);}
      else if(notification.permission==='denied'){phase='denied';await workerMessage('SOTY_PUSH_BIND',report.bindingId,false);}
      else {
        const subscription=await registration.pushManager.getSubscription();await current(owned);
        const matches=subscription&&sameKey(subscription.options?.applicationServerKey,keyBytes(result.publicKey))&&await fingerprint(subscription.endpoint)===result.subscriptionId;
        await current(owned);if(version!==generation||disposed)return view();
        phase=result.enabled&&notification.permission==='granted'&&matches?'on':'off';
        await workerMessage('SOTY_PUSH_BIND',report.bindingId,phase==='on');
      }
      if(location?.href&&!disposed&&version===generation&&!entryHandled){entryHandled=true;const request=new URL(location.href).searchParams.get('notification');if(request)void open(request);}
    } catch {if(!disposed&&version===generation){phase='unavailable';code='push_unavailable';await clearBinding();report=undefined;identity=undefined;}}
    if(!disposed&&version===generation)notify();return view();
  }
  return Object.freeze({view,refresh,open,subscribe(listener){listeners.add(listener);listener(view());return()=>listeners.delete(listener);},
    async retryOpen() { const pending=pendingNotification;if(!pending||disposed)return false;await refresh();
      if(disposed||pendingNotification!==pending)return false;
      if(!identity||!report){if(isCurrent()){code='push_notification_unavailable';notify();}return false;}
      return open(pending.notificationId,pending.bindingId); },
    async enable() {
      if(disposed||busy||phase!=='off'||!report?.configured||!view().supported||!identity)return;
      if(notification.permission==='denied'){phase='denied';notify();return;}
      const owned=identity,version=generation;busy=true;code=null;notify();
      const active=async()=>{await current(owned);if(version!==generation)throw failure('push_actor_denied');};
      try {
        // Keep the permission request in the original visible button gesture.
        const permission=notification.permission==='granted'?Promise.resolve('granted'):notification.requestPermission();
        const consent=await permission;if(consent!=='granted'){phase=consent==='denied'?'denied':'off';return;}
        await active();const key=keyBytes(report.publicKey);let subscription=await registration.pushManager.getSubscription();await active();
        if(subscription&&!sameKey(subscription.options?.applicationServerKey,key)){await subscription.unsubscribe();await active();subscription=null;}
        if(!subscription){subscription=await registration.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:key});await active();}
        const result=await rpc('notifications.subscribe',{subscription:subscription.toJSON()},owned);
        if(!exact(result,['ok','schema','enabled'])||result.ok===false||result.schema!=='soty.push-subscription.v1'||result.enabled!==true)throw failure('push_invalid');
        if(disposed||version!==generation)return;
        await workerMessage('SOTY_PUSH_BIND',report.bindingId,true);await active();phase='on';
      } catch(cause) {if(!disposed&&version===generation){phase=cause?.code==='push_endpoint_unsupported'?'unavailable':'unknown';
        code=['push_subscription_bound','push_endpoint_unsupported'].includes(cause?.code)?cause.code:'push_unknown';}}
      finally {busy=false;if(!disposed)notify();}
    },
    async disable() {
      if(disposed||busy||!identity)return;const owned=identity,version=generation;busy=true;code=null;notify();
      const active=async()=>{await current(owned);if(version!==generation)throw failure('push_actor_denied');};
      try {
        await clearBinding();const result=await rpc('notifications.unsubscribe',{},owned);
        if(!exact(result,['ok','schema','enabled'])||result.ok===false||result.schema!=='soty.push-subscription.v1'||result.enabled!==false)throw failure('push_invalid');
        const subscription=await registration?.pushManager.getSubscription();await active();if(subscription)await subscription.unsubscribe();await active();
        if(!disposed&&version===generation)phase='off';
      } catch {if(!disposed&&version===generation){phase='unknown';code='push_unknown';}}
      finally {busy=false;if(!disposed)notify();}
    },
    dispose({accountChanged=false}={}) {if(accountChanged){pendingNotification=undefined;void clearBinding();}if(disposed)return;disposed=true;generation++;stopped();serviceWorker?.removeEventListener?.('message',message);listeners.clear();},
  });
}
