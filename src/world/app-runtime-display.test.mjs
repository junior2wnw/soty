import test from 'node:test';import assert from 'node:assert/strict';
import {createAppRuntimeDisplay} from './app-runtime-display.mjs';
const tickFlush=()=>new Promise(r=>setImmediate(r)),deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return{promise,resolve};};
class Events{listeners=new Map();addEventListener(t,f){if(!this.listeners.has(t))this.listeners.set(t,new Set());this.listeners.get(t).add(f);}removeEventListener(t,f){this.listeners.get(t)?.delete(f);}emit(t,e={}){for(const f of [...(this.listeners.get(t)??[])])f(e);}}
function displayFixture(answer='pending'){
 const frame=new Events(),view=new Events(),tasks=new Map(),states=[],sent=[];let now=0,next=0,current=true,calls=0;
 Object.assign(view,{crypto:{randomUUID:()=> 'a'.repeat(32)},performance:{now:()=>now},setTimeout(fn,ms){const id=++next;tasks.set(id,{fn,at:now+ms});return id;},clearTimeout:id=>tasks.delete(id)});
 frame.contentWindow={postMessage:(v,o)=>sent.push({v,o})};
 const api=createAppRuntimeDisplay({view,frame,origin:'https://frame.example',isCurrent:()=>current,readiness:async()=>{calls++;return typeof answer==='function'?await answer():answer;},onChange:v=>states.push(v)});
 const tick=ms=>{const end=now+ms;for(;;){const job=[...tasks].sort((a,b)=>a[1].at-b[1].at)[0];if(!job||job[1].at>end)break;tasks.delete(job[0]);now=job[1].at;job[1].fn();}now=end;};
 const phase=(phase,patch={})=>view.emit('message',{source:frame.contentWindow,origin:'https://frame.example',ports:[],data:{schema:'soty.runtime-display.v1',nonce:'a'.repeat(32),phase},...patch});
 return{api,frame,view,tasks,states,sent,tick,phase,calls:()=>calls,stale:()=>current=false};
}
test('load-only empty-document counterexample: visible deadline survives load without authenticated readiness',async()=>{
 const f=displayFixture();f.tick(0);f.frame.emit('load');await tickFlush();assert.equal(f.api.snapshot().busy,true);f.tick(12000);
 assert.equal(f.api.snapshot().phase,'unconfirmed');assert.match(f.api.snapshot().message,/Окно загружено/u);
 assert.equal(f.api.snapshot().canRetry,true);assert.equal(f.api.snapshot().canExternal,true);assert.equal(f.api.snapshot().busy,false);
 assert.equal(f.calls(),1);assert.equal(f.api.dismiss(),true);assert.equal(f.api.snapshot().message,'');
});
test('no-load baseline retains its existing meaningful visible fallback and never replays admission',()=>{
 const f=displayFixture();f.tick(12000);assert.match(f.api.snapshot().message,/Загрузка окна не подтверждена/u);assert.equal(f.calls(),0);
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
test('explicit denial/error and unsupported legacy readiness stay honest, finite, dismissible and nonblocking',async()=>{
 for(const answer of ['denied','unsupported','unknown']){const f=displayFixture(answer);f.tick(0);f.frame.emit('load');await tickFlush();
  if(answer==='denied')assert.equal(f.api.snapshot().phase,'denied');else{f.tick(12000);assert.equal(f.api.snapshot().phase,'unconfirmed');}
  assert.equal(f.api.dismiss(),true);assert.equal(f.api.snapshot().busy,false);}
 const e=displayFixture();e.frame.emit('error');assert.equal(e.api.snapshot().phase,'error');assert.equal(e.api.snapshot().canRetry,true);
});
test('bounded progress traffic, duplicate loads and hostile phase spam never exceed three read-only checks',async()=>{
 const f=displayFixture();f.tick(0);for(let i=0;i<20;i++){f.frame.emit('load');f.phase('boot_navigation');await tickFlush();}f.tick(12000);
 assert.equal(f.calls(),3);assert.ok(f.sent.length<=4);assert.equal(JSON.stringify(f.states).includes('https:'),false);
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
test('late errors never reactivate disposed/stale or manually dismissed/denied display choices',async()=>{
 for(const mode of ['stale','dispose','pagehide']){
  const f=displayFixture('ready');f.frame.emit('load');await tickFlush();const handler=[...f.frame.listeners.get('error')][0];
  if(mode==='stale')f.stale();if(mode==='dispose')f.api.dispose();if(mode==='pagehide')f.view.emit('pagehide');
  const before=f.states.length;handler();assert.equal(f.states.length,before);assert.equal(f.calls(),1);
 }
 for(const answer of ['denied','unsupported']){
  const f=displayFixture(answer);f.frame.emit('load');await tickFlush();if(answer==='unsupported')f.tick(12001);
  f.api.dismiss();const before=f.states.length;f.frame.emit('error');assert.equal(f.states.length,before);assert.equal(f.api.snapshot().message,'');
 }
});
