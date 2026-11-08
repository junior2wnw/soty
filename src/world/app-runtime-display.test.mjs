import test from 'node:test';import assert from 'node:assert/strict';
import {createAppRuntimeDisplay} from './app-runtime-display.mjs';
const tickFlush=()=>new Promise(r=>setImmediate(r)),deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return{promise,resolve};};
class Events{listeners=new Map();addEventListener(t,f){if(!this.listeners.has(t))this.listeners.set(t,new Set());this.listeners.get(t).add(f);}removeEventListener(t,f){this.listeners.get(t)?.delete(f);}emit(t,e={}){for(const f of [...(this.listeners.get(t)??[])])f(e);}}
function displayFixture(answer='pending',managedHost=false){
 const frame=new Events(),view=new Events(),tasks=new Map(),states=[],sent=[];let now=0,next=0,current=true,calls=0;
 Object.assign(view,{crypto:{randomUUID:()=> 'a'.repeat(32)},performance:{now:()=>now},setTimeout(fn,ms){const id=++next;tasks.set(id,{fn,at:now+ms});return id;},clearTimeout:id=>tasks.delete(id)});
 frame.contentWindow={postMessage:(v,o)=>sent.push({v,o})};
 const api=createAppRuntimeDisplay({view,frame,origin:'https://frame.example',isCurrent:()=>current,managedHost,readiness:async()=>{calls++;return typeof answer==='function'?await answer():answer;},onChange:v=>states.push(v)});
 const tick=ms=>{const end=now+ms;for(;;){const job=[...tasks].sort((a,b)=>a[1].at-b[1].at)[0];if(!job||job[1].at>end)break;tasks.delete(job[0]);now=job[1].at;job[1].fn();}now=end;};
 const phase=(phase,patch={})=>view.emit('message',{source:frame.contentWindow,origin:'https://frame.example',ports:[],data:{schema:'soty.runtime-display.v1',nonce:'a'.repeat(32),phase},...patch});
 return{api,frame,view,tasks,states,sent,tick,phase,calls:()=>calls,stale:()=>current=false};
}

test('legacy/native document load has a neutral loaded state without claiming authenticated readiness',async()=>{
 const f=displayFixture('unsupported');f.frame.emit('load');await tickFlush();f.tick(12001);
 assert.equal(f.api.snapshot().phase,'loaded');assert.equal(f.api.snapshot().message,'');
 assert.equal(f.api.snapshot().busy,false);assert.equal(f.api.snapshot().canDismiss,false);
 assert.equal(f.api.snapshot().canRetry,false);assert.equal(f.api.snapshot().canExternal,true);
 assert.equal(f.api.dismiss(),false);assert.equal(f.calls(),1);
 f.frame.emit('error');assert.equal(f.api.snapshot().phase,'error');assert.equal(f.api.snapshot().canExternal,true);assert.equal(f.calls(),1);
});

test('neutral legacy load still reports an exact current Boot failure, while unrelated messages cannot change it',async()=>{
 const f=displayFixture('unsupported');f.frame.emit('load');await tickFlush();
 assert.equal(f.api.snapshot().canExternal,true);assert.equal(f.api.snapshot().canRetry,false);
 f.phase('boot_failed',{origin:'https://wrong.example'});assert.equal(f.api.snapshot().phase,'loaded');
 f.phase('boot_navigation');assert.equal(f.api.snapshot().phase,'loaded');assert.equal(f.calls(),1);
 f.phase('boot_failed');assert.equal(f.api.snapshot().phase,'error');assert.equal(f.api.snapshot().canRetry,true);assert.equal(f.api.snapshot().canExternal,true);assert.equal(f.calls(),1);
});
test('load-only empty-document counterexample: visible deadline survives load without authenticated readiness',async()=>{
 const f=displayFixture();f.tick(0);f.frame.emit('load');await tickFlush();assert.equal(f.api.snapshot().busy,true);f.tick(12000);
 assert.equal(f.api.snapshot().phase,'unconfirmed');assert.match(f.api.snapshot().message,/Окно загружено/u);
 assert.equal(f.api.snapshot().canRetry,true);assert.equal(f.api.snapshot().canExternal,true);assert.equal(f.api.snapshot().busy,false);
 assert.equal(f.calls(),1);assert.equal(f.api.dismiss(),true);assert.equal(f.api.snapshot().message,'');
});
test('no-load baseline retains its existing meaningful visible fallback and never replays admission',()=>{
 const f=displayFixture();f.tick(12000);assert.match(f.api.snapshot().message,/Загрузка окна не подтверждена/u);assert.equal(f.calls(),0);
});
test('verified host admission keeps a cold frame pending through a real load at 12.9 seconds',async()=>{
 const f=displayFixture('unsupported');f.tick(4500);f.api.waitForHostAdmission?.(24000);f.tick(7500);
 assert.equal(f.api.snapshot().busy,true,'a still-admitting frame should not show the no-load warning at 12 seconds');
 assert.equal(f.calls(),0);
 f.tick(900);f.frame.emit('load');await tickFlush();f.tick(17500);
 assert.equal(f.api.snapshot().phase,'loaded');
 assert.equal(f.api.snapshot().busy,false);
 assert.equal(f.states.some(state=>state.phase==='unconfirmed'),false);
 assert.equal(f.calls(),1,'host admission does not itself confirm application readiness');
});
test('host admission wait ends at the initial reservation limit and cannot mask a missing context',()=>{
 const bounded=displayFixture();bounded.api.waitForHostAdmission?.(60000);bounded.tick(20000);
 bounded.api.waitForHostAdmission?.(60000);bounded.tick(9999);assert.equal(bounded.api.snapshot().busy,true);
 bounded.tick(1);assert.equal(bounded.api.snapshot().phase,'unconfirmed');
 const unavailable=displayFixture();unavailable.api.waitForHostAdmission?.(29000);unavailable.tick(11000);
 unavailable.api.waitForHostAdmission?.(0);unavailable.tick(1000);assert.equal(unavailable.api.snapshot().phase,'unconfirmed');
 assert.equal(unavailable.calls(),0);
});
test('a load without readiness or an explicit frame error still uses the ordinary fallback during admission',async()=>{
 const unconfirmed=displayFixture();unconfirmed.api.waitForHostAdmission?.(29000);unconfirmed.tick(10000);
 unconfirmed.frame.emit('load');await tickFlush();unconfirmed.tick(2000);
 assert.equal(unconfirmed.api.snapshot().phase,'unconfirmed');assert.equal(unconfirmed.api.snapshot().canRetry,true);
 const failed=displayFixture();failed.api.waitForHostAdmission?.(29000);failed.tick(13000);failed.frame.emit('error');
 assert.equal(failed.api.snapshot().phase,'error');assert.equal(failed.api.snapshot().canRetry,true);
});
test('a managed error-document load during admission cannot hide the eventual retry',async()=>{
 const f=displayFixture('unsupported',true);f.tick(4500);f.api.waitForHostAdmission(24000);f.tick(8300);
 f.frame.emit('load');await tickFlush();
 assert.equal(f.api.snapshot().busy,true,'Chromium load alone cannot end an unconfirmed managed opening');
 assert.equal(f.calls(),0);
 f.tick(15700);
 assert.equal(f.api.snapshot().phase,'unconfirmed');
 assert.equal(f.api.snapshot().canRetry,true);
 assert.match(f.api.snapshot().message,/Попробуйте открыть заново/u);
 f.api.confirmHostTransport?.();await tickFlush();
 assert.equal(f.api.snapshot().phase,'unconfirmed','a timed-out window needs an explicit retry');
});
test('ordinary managed transport confirmation and load settle in either order without claiming Source readiness',async()=>{
 for(const first of ['load','confirmation']){
  const f=displayFixture('unsupported',true);f.api.waitForHostAdmission(28000);f.tick(12900);
  if(first==='load')f.frame.emit('load');else f.api.confirmHostTransport?.();
  await tickFlush();assert.equal(f.api.snapshot().busy,true);assert.equal(f.calls(),0);
  if(first==='load')f.api.confirmHostTransport?.();else f.frame.emit('load');
  await tickFlush();assert.equal(f.api.snapshot().phase,'loaded');assert.equal(f.calls(),1);
  assert.equal(f.states.some(state=>state.phase==='ready'||state.phase==='unconfirmed'),false);
 }
});
test('managed host loss restores the ordinary deadline and an explicit Boot error still wins',async()=>{
 const lost=displayFixture('unsupported',true);lost.api.waitForHostAdmission(28000);lost.tick(11200);
 lost.frame.emit('load');await tickFlush();lost.api.waitForHostAdmission(0);lost.tick(800);
 assert.equal(lost.api.snapshot().phase,'unconfirmed');assert.equal(lost.api.snapshot().canRetry,true);
 const failed=displayFixture('unsupported',true);failed.api.waitForHostAdmission(28000);failed.tick(12800);
 failed.frame.emit('load');failed.phase('boot_failed');
 assert.equal(failed.api.snapshot().phase,'error');assert.equal(failed.api.snapshot().canRetry,true);
});
test('Boot progress is never readiness; only current authenticated Root read clears pending state',async()=>{
 const held=deferred(),f=displayFixture(()=>held.promise);f.tick(0);f.phase('boot_cookie_pending');assert.equal(f.api.snapshot().phase,'authenticating');
 f.frame.emit('load');await tickFlush();assert.equal(f.api.snapshot().busy,true);held.resolve('ready');await tickFlush();assert.equal(f.api.snapshot().phase,'ready');
 assert.equal(f.api.snapshot().message,'');f.tick(12000);assert.equal(f.api.snapshot().phase,'ready');
});
test('late authenticated result after timeout or old-window disposal cannot revive success',async()=>{
 for(const mode of ['timeout','stale','dispose','pagehide']){const held=deferred(),f=displayFixture(()=>held.promise);f.tick(0);f.frame.emit('load');await tickFlush();
  if(mode==='timeout')f.tick(12000);if(mode==='stale')f.stale();if(mode==='dispose')f.api.dispose();if(mode==='pagehide')f.view.emit('pagehide');
  const before=f.states.length;held.resolve('ready');await tickFlush();assert.equal(f.states.length,before);assert.notEqual(f.api.snapshot().phase,'ready');}
});
test('closed peer/origin/nonce/ports/extra/accessor payloads cannot take over Boot progress',()=>{
 const f=displayFixture();f.tick(0);let getter=0;
 for(const patch of [{source:{}},{origin:'https://wrong.example'},{ports:[{}]},{data:{schema:'soty.runtime-display.v1',nonce:'b'.repeat(32),phase:'boot_failed'}},
  {data:{schema:'soty.runtime-display.v1',nonce:'a'.repeat(32),phase:'boot_failed',url:'OWN_PRIVATE'}},
  {data:{schema:'soty.runtime-display.v1',nonce:'a'.repeat(32),get phase(){getter++;throw Error('OWN_PRIVATE');}}}])f.phase('boot_failed',patch);
 assert.equal(f.api.snapshot().phase,'opening');assert.equal(getter,0);assert.equal(f.calls(),0);
});
test('explicit denial/error and unknown readiness stay honest, finite, dismissible and nonblocking',async()=>{
 for(const answer of ['denied','unknown']){const f=displayFixture(answer);f.tick(0);f.frame.emit('load');await tickFlush();
  if(answer==='denied')assert.equal(f.api.snapshot().phase,'denied');else{f.tick(12000);assert.equal(f.api.snapshot().phase,'unconfirmed');}
  assert.equal(f.api.dismiss(),true);assert.equal(f.api.snapshot().busy,false);}
 const e=displayFixture();e.frame.emit('error');assert.equal(e.api.snapshot().phase,'error');assert.equal(e.api.snapshot().canRetry,true);
});
test('bounded progress traffic, duplicate loads and hostile phase spam never exceed three read-only checks',async()=>{
 const f=displayFixture();f.tick(0);for(let i=0;i<20;i++){f.frame.emit('load');f.phase('boot_navigation');await tickFlush();}f.tick(12000);
 assert.equal(f.calls(),3);assert.equal(f.sent.length,5);assert.equal(JSON.stringify(f.states).includes('https:'),false);
});
test('first document load sends one bounded watch before a fast ready response can cancel scheduled probes',async()=>{
 const f=displayFixture('ready');f.tick(0);assert.equal(f.sent.length,1);
 f.frame.emit('load');assert.equal(f.sent.length,2);await tickFlush();assert.equal(f.api.snapshot().phase,'ready');
 for(let i=0;i<20;i++)f.frame.emit('load');f.tick(12001);
 assert.equal(f.sent.length,2);assert.equal(f.calls(),1);
});

test('an old load cannot send a watch after account/route loss, disposal or pagehide',()=>{
 for(const mode of ['stale','dispose','pagehide']){
  const f=displayFixture();f.tick(0);
  if(mode==='stale')f.stale();if(mode==='dispose')f.api.dispose();if(mode==='pagehide')f.view.emit('pagehide');if(mode==='timeout')f.tick(12000);
  const before=f.sent.length;f.frame.emit('load');assert.equal(f.sent.length,before);assert.equal(f.calls(),0);
 }
});

test('first late document load replaces the stale no-load warning without claiming authentication',async()=>{
 for(const answer of ['unsupported','pending','unknown','ready','denied']){
  const f=displayFixture(answer);f.tick(12000);assert.match(f.api.snapshot().message,/Загрузка окна не подтверждена/u);
  const probes=f.sent.length;f.frame.emit('load');await tickFlush();assert.equal(f.sent.length,probes,'deadline does not restart Boot probes');
  assert.equal(f.calls(),1);assert.equal(f.api.snapshot().busy,false);
  if(answer==='unsupported')assert.equal(f.api.snapshot().phase,'loaded');
  else if(answer==='ready'||answer==='denied')assert.equal(f.api.snapshot().phase,answer);
  else {assert.equal(f.api.snapshot().phase,'unconfirmed');assert.match(f.api.snapshot().message,/Окно загружено/u);}
  f.frame.emit('load');await tickFlush();assert.equal(f.calls(),1,'duplicate late loads do not replay reads');
 }
});
test('dismissed timeout cannot be revived by a late load or frame assertion',async()=>{
 const f=displayFixture('ready');f.tick(12000);assert.equal(f.api.dismiss(),true);
 f.frame.emit('load');f.phase('boot_navigation');await tickFlush();assert.equal(f.calls(),0);assert.equal(f.api.snapshot().message,'');
});

test('load/navigation arriving during a pending Root read coalesce into one bounded fresh read',async()=>{
 const held=deferred();let n=0;const f=displayFixture(()=>++n===1?held.promise:'ready');f.tick(0);f.frame.emit('load');await tickFlush();
 f.frame.emit('load');f.phase('boot_navigation');f.phase('boot_navigation');assert.equal(f.calls(),1);
 held.resolve('pending');await tickFlush();assert.equal(f.calls(),2);assert.equal(f.api.snapshot().phase,'ready');
});
test('Source-session ready preserves subsequent current-frame transport error without new Root read',async()=>{
 const f=displayFixture('ready');f.tick(0);f.frame.emit('load');await tickFlush();
 assert.equal(f.api.snapshot().phase,'ready');const calls=f.calls();f.frame.emit('error');
 assert.equal(f.api.snapshot().phase,'error');assert.equal(f.api.snapshot().canRetry,true);
 assert.equal(f.api.snapshot().canExternal,true);assert.equal(f.api.snapshot().canDismiss,true);assert.equal(f.api.snapshot().busy,false);
 f.phase('boot_navigation');f.tick(12001);assert.equal(f.calls(),calls);assert.equal(f.api.snapshot().phase,'error');
});
test('Source-session ready cannot hide a later correlated managed Boot failure',async()=>{
 const f=displayFixture('ready');f.tick(0);f.frame.emit('load');await tickFlush();
 assert.equal(f.api.snapshot().phase,'ready');
 for(const patch of [{source:{}},{origin:'https://wrong.example'},{ports:[{}]},
  {data:{schema:'soty.runtime-display.v1',nonce:'b'.repeat(32),phase:'boot_failed'}},
  {data:{schema:'soty.runtime-display.v1',nonce:'a'.repeat(32),phase:'boot_failed',url:'PRIVATE'}}]){
  f.phase('boot_failed',patch);assert.equal(f.api.snapshot().phase,'ready');
 }
 f.phase('boot_navigation');assert.equal(f.api.snapshot().phase,'ready');
 f.phase('boot_failed');assert.equal(f.api.snapshot().phase,'error');
 assert.match(f.api.snapshot().message,/Не удалось подтвердить запуск/u);
 assert.equal(f.api.snapshot().canRetry,true);assert.equal(f.api.snapshot().canExternal,true);
 assert.equal(f.api.snapshot().canDismiss,true);assert.equal(f.api.snapshot().busy,false);
 f.phase('boot_cookie_pending');f.tick(12001);assert.equal(f.api.snapshot().phase,'error');assert.equal(f.calls(),1);
});

test('a late managed Boot failure never revives a stale, disposed or dismissed ready display',async()=>{
 for(const mode of ['stale','dispose','pagehide','dismiss']){
  const f=displayFixture('ready');f.frame.emit('load');await tickFlush();
  if(mode==='stale')f.stale();if(mode==='dispose')f.api.dispose();if(mode==='pagehide')f.view.emit('pagehide');
  if(mode==='dismiss'){f.frame.emit('error');f.api.dismiss();}
  const before=f.states.length;f.phase('boot_failed');assert.equal(f.states.length,before);assert.equal(f.calls(),1);
 }
});

test('late errors never reactivate disposed/stale or manually dismissed/denied display choices',async()=>{
 for(const mode of ['stale','dispose','pagehide']){
  const f=displayFixture('ready');f.frame.emit('load');await tickFlush();const handler=[...f.frame.listeners.get('error')][0];
  if(mode==='stale')f.stale();if(mode==='dispose')f.api.dispose();if(mode==='pagehide')f.view.emit('pagehide');
  const before=f.states.length;handler();assert.equal(f.states.length,before);assert.equal(f.calls(),1);
 }
 for(const answer of ['denied','unknown']){
  const f=displayFixture(answer);f.frame.emit('load');await tickFlush();if(answer==='unknown')f.tick(12001);
  f.api.dismiss();const before=f.states.length;f.frame.emit('error');assert.equal(f.states.length,before);assert.equal(f.api.snapshot().message,'');
 }
});
