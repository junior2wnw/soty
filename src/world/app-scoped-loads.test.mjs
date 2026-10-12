import test from 'node:test';
import assert from 'node:assert/strict';
import {createScopedSlotRenewal,attachScopedRenewalLoads,attachRootSsoWakeRecovery} from './app-scoped-renewal.mjs';
import {isScopedRuntimeProfile} from './app-launch.mjs';

test('Stage load arms the same single renewal lifecycle for approved1/2; future or declared names never arm it',async()=>{
  for(const profile of ['soty.selected-human-embed.v1','soty.selected-human-embed.v2','soty.selected-human-embed.v3','author-selected']){
    const runtime=new EventTarget();runtime.ownerDocument={visibilityState:'visible'};
    const controller=new AbortController();let probe=0,tick=0,cleared=0,scheduled=[];
    const detach=attachScopedRenewalLoads({runtime,view:{setInterval(fn,ms){assert.equal(ms,30000);scheduled.push(fn);return 1;},clearInterval(){cleared++;}},
      signal:controller.signal,current:()=>true,profile:()=>profile,renewal:{async probe(){probe++;},async tick(){tick++;}}});
    runtime.dispatchEvent(new Event('load'));runtime.dispatchEvent(new Event('load'));
    const accepted=isScopedRuntimeProfile(profile);assert.equal(scheduled.length,accepted?1:0);assert.equal(probe,accepted?2:0);
    if(accepted){scheduled[0]();await Promise.resolve();assert.equal(tick,1);runtime.ownerDocument.visibilityState='hidden';scheduled[0]();assert.equal(tick,1);}
    detach();detach();controller.abort();runtime.dispatchEvent(new Event('load'));assert.equal(cleared,accepted?1:0);
  }
});

test('v2 Stage timer cannot turn client boot hint into readiness without the current Source ACK',async()=>{
  const runtime=new EventTarget();runtime.ownerDocument={visibilityState:'visible'};const controller=new AbortController();
  const source={id:'approved-native',version:2,digest:'a'.repeat(64)},binding={handle:'old',slot:{},source};let tick,requests=0,commits=0,closes=0;
  const renewal=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1000000,
    async readContext(handle){return {ready:true,scopedSource:source,expiresAt:1060000,
      ...(handle==='old'?{sourceSession:{ready:true,renewable:true,sessionExpiresAt:4600000,accessExpiresAt:1300000}}:{})};},
    async request(input){requests++;return{handle:'new',requestId:input.requestId,cleanup:async()=>{closes++;}};},
    async bootstrap(){return'ready';},commit(){commits++;return true;}});
  const detach=attachScopedRenewalLoads({runtime,view:{setInterval(fn){tick=fn;return 1;},clearInterval(){}},signal:controller.signal,
    current:()=>true,profile:()=> 'soty.selected-human-embed.v2',renewal});
  runtime.dispatchEvent(new Event('load'));await Promise.resolve();tick();await new Promise(done=>setImmediate(done));
  assert.equal(requests,1);assert.equal(commits,0);assert.equal(closes,1);detach();controller.abort();renewal.dispose();
});

test('retired/profile-changed Stage never ticks or reports a late rejected callback from an old generation',async()=>{
  const runtime=new EventTarget();runtime.ownerDocument={visibilityState:'visible'};const controller=new AbortController();let current=true,profile='soty.selected-human-embed.v2',tick,reject,failures=0;
  const detach=attachScopedRenewalLoads({runtime,view:{setInterval(fn){tick=fn;return 1;},clearInterval(){}},signal:controller.signal,
    current:()=>current,profile:()=>profile,renewal:{async probe(){},tick(){return new Promise((_,no)=>{reject=no;});}},onTickFailure(){failures++;}});
  runtime.dispatchEvent(new Event('load'));tick();current=false;reject(Error('retired'));await new Promise(done=>setImmediate(done));assert.equal(failures,0);
  current=true;profile='unapproved';tick();detach();controller.abort();
});

test('a visible expired Root slot recovers from the Stage timer without another focus event',async()=>{
  const runtime=new EventTarget(),document=new EventTarget(),view=new EventTarget(),controller=new AbortController();
  document.visibilityState='visible';runtime.ownerDocument=document;let tick,launches=0,failures=0;
  view.setInterval=fn=>{tick=fn;return 1;};view.clearInterval=()=>{};
  const binding={handle:'expired',slot:{},source:{id:'soty.standard-workspace',version:2,digest:'23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9'}};
  const renewal=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1301000,
    async readContext(){throw Object.assign(Error('Expired Root slot'),{code:'app_scoped_context_closed',status:403});},
    request(){assert.fail('An expired context is not renewed as a live Source grant.');},bootstrap(){assert.fail();},commit(){assert.fail();}});
  const wake=attachRootSsoWakeRecovery({view,document,signal:controller.signal,current:()=>true,readBinding:()=>binding,
    lastLaunchAt:()=>1000000,clock:()=>1301000,async recover(){launches++;throw Error('fresh launch failed');},onFailure(){failures++;}});
  const loads=attachScopedRenewalLoads({runtime,view,signal:controller.signal,current:()=>true,
    profile:()=> 'soty.selected-human-embed.v2',readBinding:()=>binding,renewal,onTickFailure:(error,captured)=>wake.recoverExpired(error,captured)});
  runtime.dispatchEvent(new Event('load'));await new Promise(setImmediate);tick();await new Promise(setImmediate);
  assert.equal(launches,1);assert.equal(failures,1);
  tick();tick();await new Promise(setImmediate);assert.equal(launches,1,'failed recovery does not repeat for the same slot');
  loads();wake();controller.abort();renewal.dispose();
});

test('a late timer denial from a replaced slot is not a failure of its successor',async()=>{
  const runtime=new EventTarget();runtime.ownerDocument={visibilityState:'visible'};const controller=new AbortController();
  let tick,reject,failures=0,binding={handle:'old',slot:{}};
  const loads=attachScopedRenewalLoads({runtime,view:{setInterval(fn){tick=fn;return 1;},clearInterval(){}},signal:controller.signal,
    current:()=>true,profile:()=> 'soty.selected-human-embed.v2',readBinding:()=>binding,
    renewal:{async probe(){},tick(){return new Promise((_,no)=>{reject=no;});}},onTickFailure(){failures++;}});
  runtime.dispatchEvent(new Event('load'));tick();binding={handle:'fresh',slot:{}};
  reject(Object.assign(Error('Old slot expired'),{code:'app_scoped_context_closed',status:403}));await new Promise(setImmediate);
  assert.equal(failures,0);loads();controller.abort();
});

test('successful normal renewal does not enter expiry recovery or relaunch the visible frame',async()=>{
  const runtime=new EventTarget();runtime.ownerDocument={visibilityState:'visible'};const controller=new AbortController();let tick,renewed=0,failures=0;
  const loads=attachScopedRenewalLoads({runtime,view:{setInterval(fn){tick=fn;return 1;},clearInterval(){}},signal:controller.signal,
    current:()=>true,profile:()=> 'soty.selected-human-embed.v2',renewal:{async probe(){},async tick(){renewed++;return true;}},onTickFailure(){failures++;}});
  runtime.dispatchEvent(new Event('load'));tick();await new Promise(setImmediate);assert.equal(renewed,1);assert.equal(failures,0);
  loads();controller.abort();
});
