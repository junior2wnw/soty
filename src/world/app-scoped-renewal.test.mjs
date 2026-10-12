import test from 'node:test';
import assert from 'node:assert/strict';
import {createScopedSlotRenewal,attachRootSsoWakeRecovery} from './app-scoped-renewal.mjs';
const rootPin={id:'soty.standard-workspace',version:2,digest:'23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9'};
// Client lifecycle fixtures only. Actual signed/channel/Source tests live under modules/apps/test.
function fixture(change={},source={id:'source-one',version:1,digest:'a'.repeat(64)}){let time=1000000,current=true,active={handle:'old',source,slot:{}},
  requests=[],swaps=0,closes=0,nextReady=true,hint='ready';
  const context=handle=>({ready:true,scopedSource:active.source,expiresAt:time+(handle==='old'?60000:300000),
    ...(handle==='old'||nextReady?{sourceSession:{ready:true,renewable:true,sessionExpiresAt:time+3600000,accessExpiresAt:time+300000}}:{})});
  const helper=createScopedSlotRenewal({readBinding:()=>active,isCurrent:()=>current,clock:()=>time,
    readContext:async handle=>context(handle),async request(input){requests.push(input);return{handle:'new',source:active.source,slot:{},url:'https://app.example/_soty/boot',
      requestId:input.requestId,expiresAt:time+300000,cleanup:async()=>{closes++;}};},async bootstrap(){return hint;},commit(old,next){if(active.slot!==old.slot)return false;active=next;swaps++;return true;},...change});
  return{helper,counts:()=>({requests,swaps,closes}),setCurrent(value){current=value;},setReady(value){nextReady=value;},setHint(value){hint=value;},active:()=>active};}
test('client renews before 180s capture on a 240s slot and commits only server-ACK/current pins',async()=>{
  const f=fixture();assert.equal(await f.helper.ensure(190000),true);assert.equal(f.counts().swaps,1);assert.equal(f.active().handle,'new');});
test('fake boot ready without server Source ACK cannot commit a native session',async()=>{
  const f=fixture();f.setReady(false);assert.equal(await f.helper.renew(),false);assert.equal(f.counts().swaps,0);assert.equal(f.counts().closes,1);});
test('unknown boot ACK keeps same request intent and recovers current server state',async()=>{
  const f=fixture();f.setReady(false);f.setHint('unknown');assert.equal(await f.helper.renew(),false);assert.equal(f.counts().closes,0);
  f.setReady(true);assert.equal(await f.helper.renew(),true);const requests=f.counts().requests;
  assert.equal(requests.length,2);assert.equal(requests[0].requestId,requests[1].requestId);assert.equal(requests[0].handle,requests[1].handle);});
test('profile switch while awaiting boot closes exact new slot and never swaps old UI',async()=>{
  let begun,release;const started=new Promise(done=>{begun=done;}),waiting=new Promise(done=>{release=done;});
  const f=fixture({async bootstrap(){begun();await waiting;return'ready';}}),pending=f.helper.renew();await started;f.setCurrent(false);release();
  assert.equal(await pending,false);assert.equal(f.counts().swaps,0);assert.equal(f.counts().closes,1);});
test('concurrent requests share one private renewal operation',async()=>{
  const f=fixture();const results=await Promise.all([f.helper.renew(),f.helper.renew()]);assert.equal(results.every(Boolean),true);
  assert.equal(f.counts().requests.length,1);assert.equal(f.counts().swaps,1);});
test('unconfirmed Source declaration cannot enable automatic renewal',async()=>{
  const f=fixture({async readContext(){return{ready:true,scopedSource:{id:'source-one',version:1,digest:'a'.repeat(64)},expiresAt:1100000};}});
  assert.equal(await f.helper.tick(),false);assert.equal(f.counts().requests.length,0);});

function finiteContext(source,newUntil=1300000){return async handle=>({ready:true,scopedSource:source,
  expiresAt:handle==='old'?1080000:1300000,sourceSession:{ready:true,renewable:true,
    sessionExpiresAt:handle==='old'?1060000:newUntil,accessExpiresAt:handle==='old'?1060000:newUntil}});}

test('approved Root SSO acquires a fresh finite Source session within the new Root deadline',async()=>{
  const f=fixture({readContext:finiteContext(rootPin)},rootPin);
  assert.equal(await f.helper.ensure(190000),true);assert.equal(f.counts().swaps,1);assert.equal(f.active().handle,'new');
});

test('RP and a changed workspace digest cannot extend their original session deadline',async()=>{
  for(const source of [{...rootPin,version:1},{...rootPin,digest:'b'.repeat(64)}]){
    const f=fixture({readContext:finiteContext(source)},source);
    assert.equal(await f.helper.renew(),false);assert.equal(f.counts().swaps,0);assert.equal(f.counts().closes,1);
  }
});

test('Root SSO rejects a new Source session beyond its fresh Root deadline',async()=>{
  const f=fixture({readContext:finiteContext(rootPin,1300001)},rootPin);
  assert.equal(await f.helper.renew(),false);assert.equal(f.counts().swaps,0);assert.equal(f.counts().closes,1);
});

test('return after a day asks for one fresh launch and cannot resurrect a closed or changed stage',async()=>{
  const view=new EventTarget(),document=new EventTarget(),controller=new AbortController();
  document.visibilityState='hidden';let current=true,time=1000000,lastLaunch=time,launches=0,finish;
  let binding={handle:'old',source:rootPin,slot:{}};
  const detach=attachRootSsoWakeRecovery({view,document,signal:controller.signal,current:()=>current,
    readBinding:()=>binding,lastLaunchAt:()=>lastLaunch,clock:()=>time,recover:async()=>{launches++;
      await new Promise(done=>{finish=done;});lastLaunch=time;}});
  time+=86400001;view.dispatchEvent(new Event('focus'));await new Promise(setImmediate);assert.equal(launches,0);
  document.visibilityState='visible';current=false;view.dispatchEvent(new Event('focus'));await new Promise(setImmediate);assert.equal(launches,0);
  current=true;binding={...binding,source:{...rootPin,digest:'f'.repeat(64)}};
  view.dispatchEvent(new Event('focus'));await new Promise(setImmediate);assert.equal(launches,0);
  binding={...binding,source:rootPin};view.dispatchEvent(new Event('focus'));document.dispatchEvent(new Event('visibilitychange'));
  await new Promise(setImmediate);assert.equal(launches,1);finish();await new Promise(setImmediate);
  view.dispatchEvent(new Event('focus'));await new Promise(setImmediate);assert.equal(launches,1);
  time+=86400001;view.dispatchEvent(new Event('focus'));binding={...binding,slot:{}};
  await new Promise(setImmediate);assert.equal(launches,1);
  controller.abort();view.dispatchEvent(new Event('focus'));await new Promise(setImmediate);assert.equal(launches,1);detach();
});
