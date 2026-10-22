import { createHash, createECDH, ECDH } from 'node:crypto';
import webPush from 'web-push';
import { createPushStore } from './push-store.js';
import { createPushTransport } from './push-transport.js';

const error = code => Object.assign(new Error(code),{code});
const need = (value,code='push_invalid') => { if (!value) throw error(code); };
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
const exact = (value,names) => value && Object.getPrototypeOf(value) === Object.prototype && Object.keys(value).every(key=>names.includes(key));
const hash = value => createHash('sha256').update(value).digest('hex');
const base64 = (value,size) => typeof value === 'string' && /^[A-Za-z0-9_-]+$/u.test(value) && Buffer.from(value,'base64url').length === size && Buffer.from(value,'base64url').toString('base64url') === value;
export function pushEvent(value) { return Boolean(exact(value,['kind','conversationId','messageId']) && ['direct','room'].includes(value.kind) && id(value.conversationId) && id(value.messageId)); }
function target(value,event) {
  if (event.kind === 'room') return exact(value,['kind','communityId']) && value.kind === 'room' && value.communityId === event.conversationId;
  return exact(value,['kind','threadId','peerAccountId','relationshipId']) && value.kind === 'direct' && value.threadId === event.conversationId && id(value.peerAccountId) && id(value.relationshipId);
}
export function validatePushSubscription(value,now) {
  need(exact(value,['endpoint','expirationTime','keys']) && typeof value.endpoint === 'string' && value.endpoint.length <= 4096
    && exact(value.keys,['p256dh','auth']) && base64(value.keys.p256dh,65) && base64(value.keys.auth,16));
  let endpoint; try { endpoint=new URL(value.endpoint); } catch { throw error('push_endpoint_unsupported'); }
  const host=endpoint.hostname;
  const supported=host === 'fcm.googleapis.com' || host === 'web.push.apple.com'
    || host.endsWith('.push.services.mozilla.com') || host === 'push.services.mozilla.com' || host.endsWith('.notify.windows.com');
  need(endpoint.protocol === 'https:' && !endpoint.username && !endpoint.password && !endpoint.hash && !endpoint.port
    && supported && endpoint.href === value.endpoint,'push_endpoint_unsupported');
  need(value.expirationTime === null || Number.isSafeInteger(value.expirationTime) && value.expirationTime > now);
  try { ECDH.convertKey(Buffer.from(value.keys.p256dh,'base64url'),'prime256v1'); } catch { throw error('push_invalid'); }
  return {endpoint:value.endpoint,expirationTime:value.expirationTime,keys:{p256dh:value.keys.p256dh,auth:value.keys.auth}};
}
export function pushConfigurationFromEnvironment(env={}) {
  return {enabled:env.SOTY_PUSH_ENABLED === '1',subject:env.SOTY_PUSH_VAPID_SUBJECT,publicKey:env.SOTY_PUSH_VAPID_PUBLIC_KEY,privateKey:env.SOTY_PUSH_VAPID_PRIVATE_KEY};
}

/** Signed Connect extension plus a post-commit, bounded best-effort notification queue.
 * Every network attempt rechecks current World/Connect authority; payloads contain no previews. */
export function createPushService({databasePath,projectId='soty',configuration={},getConnect,recipientsFor,authorizeDelivery,authorizeOpen,sendNotification,clock=Date.now}={}) {
  const synchronous=value=>typeof value==='function'&&!['AsyncFunction','AsyncGeneratorFunction'].includes(value.constructor?.name);
  const configured=configuration.enabled === true && synchronous(recipientsFor) && synchronous(authorizeDelivery) && synchronous(authorizeOpen);
  let store, stopped=false, closing, work, timer, unsubscribe, unrevoked, immediate;
  const counters={intakeDropped:0,recipientFailures:0};
  const pending=new Map(), operations=new Set(['notifications.status','notifications.subscribe','notifications.unsubscribe','notifications.open']);
  const now=()=>{const value=clock();need(Number.isSafeInteger(value)&&value>=0);return value;};
  let vapid,keyId;
  if (configured) {
    need(base64(configuration.publicKey,65)&&base64(configuration.privateKey,32),'push_configuration_invalid');
    need(typeof configuration.subject === 'string' && /^(?:mailto:[^\s@]+@[^\s@]+|https:\/\/[^\s]+)$/u.test(configuration.subject),'push_configuration_invalid');
    vapid={subject:configuration.subject,publicKey:configuration.publicKey,privateKey:configuration.privateKey};
    try {
      const key=createECDH('prime256v1');key.setPrivateKey(Buffer.from(vapid.privateKey,'base64url'));
      need(key.getPublicKey().equals(Buffer.from(vapid.publicKey,'base64url')),'push_configuration_invalid');
      webPush.getVapidHeaders('https://fcm.googleapis.com',vapid.subject,vapid.publicKey,vapid.privateKey,'aes128gcm');
    } catch { throw error('push_configuration_invalid'); }
    keyId=hash(vapid.publicKey); store=createPushStore({databasePath,projectId});
  }
  const binding = actor => hash(projectId+'\0'+actor.accountId+'\0'+actor.deviceId);
  const actorCurrent = actor => !stopped && id(actor?.accountId) && id(actor?.deviceId) && getConnect?.()?.isActorActive(actor) === true;
  const allowed = (actor,event,opening=false) => {
    if (!actorCurrent(actor)) return false;
    const value=(opening?authorizeOpen:authorizeDelivery)(actor,event);
    return actorCurrent(actor) && target(value,event) ? value : false;
  };
  const send=sendNotification ?? (configured?createPushTransport(vapid):null);
  async function deliver(job) {
    const at=now(), actor={accountId:job.account_id,deviceId:job.device_id};
    let event,subscription;
    try {
      event=JSON.parse(job.event); need(pushEvent(event));
      if (!store.isCurrent(job,keyId,at) || !allowed(actor,event)) { store.settle(job.id,'failed',job.attempts,at); return; }
      subscription=validatePushSubscription(JSON.parse(job.subscription),at);
    } catch { store.settle(job.id,'failed',job.attempts,at); return; }
    const attempts=job.attempts+1;
    try {
      // No awaits separate the fresh check above from this bounded transport dispatch.
      await send(subscription,JSON.stringify({schema:'soty.push.v1',notificationId:job.id,bindingId:binding(actor)}));
      store.settle(job.id,'sent',attempts,at);
    } catch (failure) {
      const status=Number(failure?.statusCode);
      if (status === 404 || status === 410) store.expire(job);
      else if (attempts<4 && (!status || status === 429 || status>=500 && status<=599))
        store.settle(job.id,'pending',attempts,now()+Math.min(300_000,5000*2**(attempts-1)));
      else store.settle(job.id,'failed',attempts,at);
    }
  }
  function enqueuePending() {
    store.prune(now());
    for(const key of store.stageEvents([...pending.values()],now()))pending.delete(key);
    for(const fanout of store.fanouts(now())) {
      if(fanout.accounts===null) {
        let accounts;
        try { accounts=recipientsFor(JSON.parse(fanout.event),{afterAccountId:fanout.account_cursor,limit:256}); }
        catch { counters.recipientFailures++;continue; }
        if(!Array.isArray(accounts)||accounts.length>256||!accounts.every((value,index)=>id(value)&&value>(accounts[index-1]??fanout.account_cursor))) {
          counters.recipientFailures++;continue;
        }
        store.fanoutPage(fanout.id,accounts);
      }
      store.enqueueFanout(fanout.id,keyId,now());
    }
  }
  async function drain() {
    if (!configured || stopped) return;
    if (work) return work;
    work=(async()=>{
      enqueuePending();
      await Promise.all(store.due(now()).map(deliver));
    })().finally(()=>{work=undefined;});
    return work;
  }
  function publish(event) {
    if (!configured || stopped || !pushEvent(event)) return;
    const accepted={kind:event.kind,conversationId:event.conversationId,messageId:event.messageId},key=hash(JSON.stringify(accepted));
    if (!pending.has(key) && pending.size>=128) {counters.intakeDropped++;return;}
    pending.set(key,accepted);
    if (!immediate) immediate=setImmediate(()=>{immediate=undefined;void drain().catch(()=>{});});
  }
  return Object.freeze({operations,execute({op,args={},actor}={}) {
    need(operations.has(op),'push_invalid'); need(actorCurrent(actor),'push_actor_denied');
    need(exact(args,['expectedAccountId',...(op === 'notifications.subscribe'?['subscription']:op === 'notifications.open'?['notificationId']:[])])
      && args.expectedAccountId === actor.accountId,'push_actor_denied');
    if (op === 'notifications.status') {
      const own=store?.own(actor,keyId,now());
      return {schema:'soty.push-status.v1',configured,enabled:Boolean(own),bindingId:binding(actor),...(own?{subscriptionId:own.id}:{}),...(configured?{publicKey:vapid.publicKey}:{})};
    }
    if (op === 'notifications.unsubscribe') { store?.remove(actor); return {schema:'soty.push-subscription.v1',enabled:false}; }
    need(configured,'push_unavailable');
    if (op === 'notifications.subscribe') {
      store.put(actor,validatePushSubscription(args.subscription,now()),keyId,now());
      return {schema:'soty.push-subscription.v1',enabled:true};
    }
    need(typeof args.notificationId === 'string' && /^[A-Za-z0-9_-]{32}$/u.test(args.notificationId),'push_invalid');
    const record=store.open(args.notificationId,actor,now());need(record,'push_notification_unavailable');
    const event=JSON.parse(record.event);need(pushEvent(event),'push_notification_unavailable');
    const current=allowed(actor,event,true);need(current,'push_notification_unavailable');
    return {schema:'soty.push-open.v1',target:current};
  },publish,drain,
  attach({subscribeMessages}) {
    need(!unsubscribe&&!stopped&&typeof subscribeMessages === 'function','push_invalid');
    if (!configured) return;
    unsubscribe=subscribeMessages(publish); unrevoked=getConnect().subscribeRevocations(event=>{if(id(event.deviceId))store.revoke(event.deviceId);});
    timer=setInterval(()=>{void drain().catch(()=>{});},1000);timer.unref?.();
  },
  status:()=>({configured,wholeReady:false,counters:{...counters}}),
  backupTo(destination) { need(configured&&!stopped,'push_unavailable');return store.backupTo(destination); },
  close() { if (closing) return closing;clearInterval(timer);clearImmediate(immediate);unsubscribe?.();unrevoked?.();
    let flushFailure;try {if(configured)enqueuePending();}catch(failure){flushFailure=failure;}
    counters.intakeDropped+=pending.size;stopped=true;pending.clear();
    return closing=Promise.resolve(work).catch(()=>{}).then(()=>{if(flushFailure)throw flushFailure;}).finally(()=>store?.close()); },
  });
}
