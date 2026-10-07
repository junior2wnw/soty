import test from 'node:test';
import assert from 'node:assert/strict';
import {createCallLifecycle} from './lifecycle.mjs';
const d=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return{promise,resolve,reject};};
const flush=async()=>{for(let i=0;i<32;i++)await Promise.resolve();};
const track=()=>({kind:'audio',readyState:'live',enabled:true,stop(){this.enabled=false;this.readyState='ended';}});
const make=ports=>{const app=createCallLifecycle({host:{authorize:ports.authorize||(()=>({}))},media:{acquire:ports.acquire||(()=>({getTracks:()=>[track()]}))},transport:{join:ports.join||(()=>({close(){},preparePublication(){return{commit(){},dispose(){}};}}))}});app.setIdentity({accountId:'A',deviceId:'D'});return app;};
const join=app=>app.join({roomId:'R',mode:'group'});
test('10000 cancelled joins hold at16 unsettled admission jobs across generations',async()=>{
 const pending=[],app=make({authorize(){const p=d();pending.push(p);return p.promise;}});let denied=0;
 for(let i=0;i<10000;i++){const p=join(app);await Promise.resolve();app.cancel();const result=await p;if(result.code==='call_capacity')denied++;}
 assert.equal(pending.length,16);assert.equal(denied,9984);assert.equal(app.snapshot().capacity.occupied,16);
 for(const p of pending)p.resolve({});await flush();assert.equal(app.snapshot().capacity.occupied,0);
 const next=join(app);await flush();assert.equal(pending.length,17);pending.at(-1).resolve({});await next;app.end();await flush();assert.equal(app.snapshot().capacity.occupied,0);
});
test('10000 mute/enable intents hold at15 late captures plus activecall; late streams allstop and slots recover',async()=>{
 const pending=[],tracks=[],app=make({acquire(){const p=d();pending.push(p);return p.promise;}});await join(app);let denied=0;
 for(let i=0;i<10000;i++){const p=app.enableMicrophone();await flush();app.mute();if((await p).code==='call_capacity')denied++;}
 assert.equal(pending.length,15);assert.equal(denied,9985);assert.equal(app.snapshot().capacity.occupied,16);
 for(const p of pending){const t=track();tracks.push(t);p.resolve({getTracks:()=>[t]});}await flush();assert.ok(tracks.every(t=>t.readyState==='ended'&&!t.enabled));assert.equal(app.snapshot().capacity.occupied,1);
 app.end();await flush();assert.equal(app.snapshot().capacity.occupied,0);
});
test('10000 ended late joins retain slots until late session AND opaque close settle',async()=>{
 const pending=[],closes=[],app=make({join(){const p=d();pending.push(p);return p.promise;}});
 for(let i=0;i<10000;i++){const p=join(app);await flush();app.end();await p;}
 assert.equal(pending.length,16);assert.equal(app.snapshot().capacity.occupied,16);
 for(const p of pending){const close=d();closes.push(close);p.resolve({close:()=>close.promise,preparePublication(){}});}await flush();assert.equal(app.snapshot().capacity.occupied,16);
 assert.equal((await join(app)).code,'call_capacity');for(const p of closes)p.resolve();await flush();assert.equal(app.snapshot().capacity.occupied,0);assert.equal(pending.length,16,'no automatic retry after capacity returns');
});
test('10000 mute attempts cannot bypass unresolved publication disposal slots',async()=>{
 const closes=[],app=make({join:()=>({close(){},preparePublication(){const p=d();closes.push(p);return{commit(){},dispose:()=>p.promise};}})});await join(app);let denied=0;
 for(let i=0;i<10000;i++){const result=await app.enableMicrophone();app.mute();await flush();if(result.code==='call_capacity')denied++;}
 assert.equal(closes.length,15);assert.equal(denied,9985);assert.equal(app.snapshot().capacity.occupied,16);
 for(const p of closes)p.resolve();await flush();assert.equal(app.snapshot().capacity.occupied,1);app.end();await flush();assert.equal(app.snapshot().capacity.occupied,0);
});
test('late aborted mic admission jobs remain bounded across account/device switches',async()=>{
 const pending=[],app=make({authorize:(_s,op)=>{if(op==='join')return{};const p=d();pending.push(p);return p.promise;}});
 for(let i=0;i<10000;i++){await join(app);const p=app.enableMicrophone();await flush();app.setIdentity({accountId:i%2?'A':'B',deviceId:'D'+i});await p;await flush();}
 assert.equal(pending.length,15);assert.equal(app.snapshot().capacity.occupied,15);for(const p of pending)p.resolve({});await flush();assert.equal(app.snapshot().capacity.occupied,0);
});
test('broken stop or disposal cannot manufacture a freed resource slot',async()=>{
 const app=make({acquire:()=>({getTracks:()=>[{kind:'audio',readyState:'live',enabled:true,stop(){throw Error('private');}}]})});await join(app);await app.enableMicrophone();app.end();await flush();assert.equal(app.snapshot().capacity.occupied,1);assert.equal(app.snapshot().localEvidence.stopCallFailures,1);
 const other=make({join:()=>({close(){throw Error('private');},preparePublication(){}})});await join(other);other.end();await flush();assert.equal(other.snapshot().capacity.occupied,1);
});
test('over-limit and mixed unexpected streams stop EVERY returned track before denial',async()=>{
 for(const count of [2,20]){const tracks=Array.from({length:count},track);if(count===2)tracks[0].kind='video';const app=make({acquire:()=>({getTracks:()=>tracks})});await join(app);assert.equal((await app.enableMicrophone()).code,'microphone_failed');assert.ok(tracks.every(t=>t.readyState==='ended'));await flush();assert.equal(app.snapshot().capacity.occupied,1);app.end();}
});
test('uninspectable returned capture conservatively occupies its slot',async()=>{
 const app=make({acquire:()=>({getTracks(){throw Error('private');}})});await join(app);assert.equal((await app.enableMicrophone()).code,'microphone_failed');app.end();await flush();assert.equal(app.snapshot().capacity.occupied,1);
});
test('disposed stage does not release slot while its commit is still unresolved',async()=>{
 const commit=d(),t=track(),app=make({acquire:()=>({getTracks:()=>[t]}),join:()=>({close(){},preparePublication:()=>({commit:()=>commit.promise,dispose(){}})})});await join(app);const job=app.enableMicrophone();await flush();app.end();await job;await flush();assert.equal(t.readyState,'ended');assert.equal(app.snapshot().capacity.occupied,1);commit.resolve();await flush();assert.equal(app.snapshot().capacity.occupied,0);assert.equal(t.enabled,false);
});
test('repeated active intent still coalesces when all16 slots occupied',async()=>{
 const pending=[],app=make({acquire(){const p=d();pending.push(p);return p.promise;}});const joining=join(app);await joining;
 for(let i=0;i<14;i++){const p=app.enableMicrophone();await flush();app.mute();await p;}
 const last=app.enableMicrophone();await flush();assert.equal(app.snapshot().capacity.occupied,16);assert.equal(app.enableMicrophone(),last);assert.equal(join(app),joining);assert.equal(pending.length,15);app.mute();await last;
 for(const p of pending)p.resolve({getTracks:()=>[track()]});await flush();app.end();await flush();assert.equal(app.snapshot().capacity.occupied,0);
});
