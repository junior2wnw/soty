import test from 'node:test';
import assert from 'node:assert/strict';
import { createECDH,createHash,randomBytes } from 'node:crypto';
import { MessageChannel } from 'node:worker_threads';
import { createNotificationsController } from './notifications-client.mjs';

const defer=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return{promise,resolve,reject};};
const tick=()=>new Promise(done=>setImmediate(done));
function fixture(t,{configured=true,permission='default',rpc,requestPermission,onOpen}={}){
  const dh=createECDH('prime256v1');dh.generateKeys();const publicKey=dh.getPublicKey().toString('base64url');
  const local={accountId:'client_account',deviceId:'client_device',localRevision:1},calls=[],messages=[],opened=[],sw=new EventTarget();
  let serverEnabled=false,subscription=null,permissionCalls=0,subscribes=0,unsubscribes=0;
  const hash=value=>createHash('sha256').update(value).digest('hex');
  const worker={postMessage(data,ports){messages.push(data);queueMicrotask(()=>{ports[0].postMessage({ok:true});ports[0].close();});}};
  const registration={active:worker,pushManager:{getSubscription:async()=>subscription,async subscribe(options){subscribes++;
    const value={endpoint:'https://fcm.googleapis.com/fcm/send/'+randomBytes(16).toString('base64url'),options,
      toJSON(){return{endpoint:this.endpoint,expirationTime:null,keys:{p256dh:publicKey,auth:randomBytes(16).toString('base64url')}};},async unsubscribe(){unsubscribes++;if(subscription===value)subscription=null;return true;}};
    subscription=value;return value;}}};
  sw.getRegistration=async()=>registration;
  const notification={permission,requestPermission(){permissionCalls++;return requestPermission?requestPermission(notification):Promise.resolve(notification.permission='granted');}};
  const client={getLocalState:async()=>({...local}),async extension(op,args,options){calls.push({op,args,options});if(rpc){const result=await rpc(op,args,{enable:()=>{serverEnabled=true;}});if(result!==undefined)return result;}
    if(op==='notifications.status')return{schema:'soty.push-status.v1',configured,enabled:serverEnabled,bindingId:'a'.repeat(64),...(configured?{publicKey}:{}),...(serverEnabled&&subscription?{subscriptionId:hash(subscription.endpoint)}:{})};
    if(op==='notifications.subscribe'){serverEnabled=true;return{schema:'soty.push-subscription.v1',enabled:true};}
    if(op==='notifications.unsubscribe'){serverEnabled=false;return{schema:'soty.push-subscription.v1',enabled:false};}
    if(op==='notifications.open')return{schema:'soty.push-open.v1',target:{kind:'room',communityId:'room_owned'}};
    throw Error('unexpected fixture operation');}};
  const controller=createNotificationsController({client,accountId:local.accountId,serviceWorker:sw,notification,pushSupported:true,
    makeChannel:()=>new MessageChannel(),fingerprint:async value=>hash(value),location:{href:'https://soty.test/'},
    onOpen:onOpen??(async(target,context)=>{await context.assertCurrent();opened.push(target);})});
  t.after(()=>controller.dispose());
  return{controller,calls,messages,local,notification,opened,registration,
    counts:()=>({permissionCalls,subscribes,unsubscribes}),server:value=>{serverEnabled=value;},subscription:()=>subscription};
}

test('construction/refresh/default-off and old backend never request browser permission or subscribe',async t=>{
  const f=fixture(t,{configured:false});assert.equal(f.calls.length,0);await f.controller.refresh();await f.controller.enable();
  assert.equal(f.controller.view().phase,'unavailable');assert.deepEqual(f.counts(),{permissionCalls:0,subscribes:0,unsubscribes:0});
  const old=fixture(t,{rpc(){throw Object.assign(new Error('old backend'),{code:'unsupported_operation'});}});
  await old.controller.refresh();await old.controller.enable();assert.equal(old.controller.view().configured,false);assert.equal(old.counts().permissionCalls,0);
});
test('permission is requested only synchronously from explicit Enable; actual subscribe is visible-only and account signed',async t=>{
  const f=fixture(t);await f.controller.refresh();assert.equal(f.controller.view().phase,'off');assert.equal(f.counts().permissionCalls,0);
  const enabled=f.controller.enable();assert.equal(f.counts().permissionCalls,1);await enabled;
  assert.equal(f.controller.view().enabled,true);assert.equal(f.counts().subscribes,1);assert.equal(f.subscription().options.userVisibleOnly,true);
  const registered=f.calls.find(value=>value.op==='notifications.subscribe');assert.deepEqual(registered.options,{expectedAccountId:'client_account'});assert.equal(registered.args.expectedAccountId,'client_account');
  assert.equal(f.messages.at(-1).type,'SOTY_PUSH_BIND');await f.controller.disable();assert.equal(f.counts().unsubscribes,1);assert.equal(f.controller.view().enabled,false);
});
test('lost registration ACK is reconciled by readonly status without another subscription or consent prompt',async t=>{
  let lose=true;const f=fixture(t,{rpc(op,_args,ports){if(op==='notifications.subscribe'&&lose){lose=false;ports.enable();throw TypeError('synthetic lost ACK');}}});
  await f.controller.refresh();await f.controller.enable();assert.equal(f.controller.view().phase,'unknown');await f.controller.refresh();
  assert.equal(f.controller.view().enabled,true);assert.equal(f.calls.filter(value=>value.op==='notifications.subscribe').length,1);assert.equal(f.counts().subscribes,1);assert.equal(f.counts().permissionCalls,1);
});
test('an account/device revision change while permission is pending cannot register a subscription',async t=>{
  const held=defer(),f=fixture(t,{requestPermission:()=>held.promise});await f.controller.refresh();const enabling=f.controller.enable();
  f.local.localRevision+=2;held.resolve('granted');await enabling;assert.equal(f.counts().subscribes,0);assert.equal(f.calls.filter(value=>value.op==='notifications.subscribe').length,0);
});
test('dismissing permission leaves an explicit retry available and an unsupported provider stays off',async t=>{
  const dismissed=fixture(t,{requestPermission:()=>Promise.resolve('default')});await dismissed.controller.refresh();await dismissed.controller.enable();
  assert.equal(dismissed.controller.view().phase,'off');assert.equal(dismissed.counts().subscribes,0);
  const unsupported=fixture(t,{rpc(op){if(op==='notifications.subscribe')throw Object.assign(new Error('unsupported fixture provider'),{code:'push_endpoint_unsupported'});}});
  await unsupported.controller.refresh();await unsupported.controller.enable();assert.equal(unsupported.controller.view().phase,'unavailable');assert.equal(unsupported.controller.view().code,'push_endpoint_unsupported');
  const count=unsupported.calls.length;await unsupported.controller.enable();assert.equal(unsupported.calls.length,count);
});
test('notification lookup cannot change the human screen before fresh signed account/ACL and host guard checks',async t=>{
  const held=defer(),entered=defer();let screen='original_other_chat';
  const f=fixture(t,{onOpen:async(_target,context)=>{entered.resolve();await held.promise;await context.assertCurrent();screen='requested_chat';}});
  await f.controller.refresh();const opening=f.controller.open('N'.repeat(32));await entered.promise;f.local.localRevision+=2;held.resolve();
  assert.equal(await opening,false);assert.equal(screen,'original_other_chat');
  const malformed=fixture(t,{rpc(op){if(op==='notifications.open')return{schema:'soty.push-open.v1',target:{kind:'room',communityId:'room_owned',url:'https://foreign.invalid'}};}});
  await malformed.controller.refresh();assert.equal(await malformed.controller.open('N'.repeat(32)),false);assert.equal(malformed.opened.length,0);
});
test('denied permission is explicit and an account-change dispose clears only its exact previous binding',async t=>{
  const f=fixture(t,{permission:'denied'});await f.controller.refresh();await f.controller.enable();assert.equal(f.controller.view().phase,'denied');assert.equal(f.counts().permissionCalls,0);
  f.controller.dispose({accountChanged:true});await tick();assert.equal(f.messages.at(-1).type,'SOTY_PUSH_CLEAR');assert.equal(f.messages.at(-1).bindingId,'a'.repeat(64));
});
