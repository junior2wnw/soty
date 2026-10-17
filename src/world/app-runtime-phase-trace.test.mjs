import test from 'node:test';
import assert from 'node:assert/strict';
import {createAppRuntimePhaseTrace,sampleRuntimeFrame} from './app-runtime-phase-trace.mjs';

class Events {
  listeners = new Map();
  addEventListener(type,fn) { if (!this.listeners.has(type)) this.listeners.set(type,new Set()); this.listeners.get(type).add(fn); }
  removeEventListener(type,fn) { this.listeners.get(type)?.delete(fn); }
  emit(type,event={}) { for (const fn of [...(this.listeners.get(type)??[])]) fn(event); }
  count() { return [...this.listeners.values()].reduce((n,set)=>n+set.size,0); }
}
function fixture() {
  let now=0,next=0,alive=true,epoch=0,sampling=true,watch=0;
  const tasks=new Map(),probes=[],copies=[];
  const doc=new Events(),view=new Events(),frame=new Events();
  Object.assign(frame,{isConnected:true,parentElement:null,style:{display:'block',visibility:'visible',opacity:'1'},
    getBoundingClientRect:()=>({left:10,top:20,width:400,height:300}),
    contentWindow:{postMessage:(data,origin)=>probes.push({data,origin})}});
  let selected=frame,hit=frame;
  Object.assign(doc,{elementsFromPoint:()=>[hit,frame]});
  Object.assign(view,{document:doc,performance:{now:()=>now},getComputedStyle:node=>node.style,
    crypto:{randomUUID:()=> (++watch).toString(16).padStart(32,'0')},
    navigator:{clipboard:{writeText:async text=>{copies.push(text);}}},
    setTimeout:(fn,ms)=>{const id=++next;tasks.set(id,{fn,at:now+ms});return id;},clearTimeout:id=>tasks.delete(id)});
  const api=createAppRuntimePhaseTrace({view,isCurrent:()=>alive,getFrame:()=>selected,getEpoch:()=>epoch,canSample:()=>sampling});
  const tick=ms=>{const target=now+ms;while(true){const entry=[...tasks].sort((a,b)=>a[1].at-b[1].at)[0];if(!entry||entry[1].at>target)break;tasks.delete(entry[0]);now=entry[1].at;entry[1].fn();}now=target;};
  const hint=(phase='boot_cookie_pending',extra={})=>view.emit('message',{source:frame.contentWindow,origin:'https://frame.example',ports:[],data:{schema:'soty.runtime-phase.v2',watchId:watch.toString(16).padStart(32,'0'),phase},...extra});
  const start=()=>{api.arm();api.reset();assert.equal(api.beginManual(),true);api.attach(frame,'https://frame.example');api.inserted();};
  return{api,view,doc,frame,tasks,probes,copies,tick,hint,start,watchId:()=>watch.toString(16).padStart(32,'0'),
    jump:ms=>{now+=ms;},
    replace:()=>{selected=new Events();},nextEpoch:()=>epoch++,leave:()=>{alive=false;},hit:node=>{hit=node;},pause:()=>{sampling=false;},resume:()=>{sampling=true;}};
}
const phases=f=>f.api.snapshot()?.events.map(x=>x.phase)??[];

test('off and arm are inert; initial/automatic reset does not consume the manual arm',async()=>{
  const f=fixture();f.api.attach(f.frame,'https://frame.example');f.api.inserted();f.hint();f.tick(20_000);
  assert.equal(f.api.snapshot(),null);assert.equal(f.tasks.size,0);assert.equal(f.view.count()+f.doc.count()+f.frame.count(),0);assert.equal(await f.api.copy(),false);
  f.api.arm();assert.equal(f.api.state(),'armed');assert.equal(f.doc.count()+f.view.count(),0);assert.equal(f.probes.length,0);assert.equal(f.copies.length,0);
  f.api.reset();assert.equal(f.api.state(),'armed');assert.equal(f.api.beginManual(),true);assert.deepEqual(phases(f),['root_launch']);
});

test('a loaded frame without a current boot response remains unobserved; load is never a boot acknowledgement',()=>{
  const f=fixture();f.start();f.tick(0);f.frame.emit('load');f.frame.emit('load');f.tick(12_001);
  assert.equal(phases(f).at(-1),'root_boot_unobserved');
  assert.equal(f.probes.length,5);for(const p of f.probes){assert.deepEqual(p.data,{schema:'soty.runtime-phase-watch.v2',watchId:f.watchId()});assert.equal(p.origin,'https://frame.example');}
  assert.equal(phases(f).includes('root_timeout'),false);assert.equal(f.api.snapshot().observation,'partial');assert.equal('ready'in f.api.snapshot(),false);
  assert.equal(phases(f).includes('boot_document_ack'),false);
  assert.equal(f.api.snapshot().events.filter(x=>x.phase==='root_load').length,1);
  f.frame.emit('load');assert.equal(f.probes.length,5);assert.equal(f.api.snapshot().events.filter(x=>x.phase==='root_load').length,2);
});

test('missing load, late load, error and fresh measurements never imply application ready',()=>{
  const f=fixture();f.start();f.hit({});f.tick(12_000);
  assert.deepEqual(phases(f).slice(-2),['root_timeout','root_boot_unobserved']);assert.equal(f.api.snapshot().view.hit,'none');f.frame.emit('load');
  assert.deepEqual(phases(f).slice(-3),['root_timeout','root_boot_unobserved','root_load']);assert.equal(f.api.snapshot().observation,'partial');
  const g=fixture();g.start();g.frame.emit('error');g.tick(12_000);assert.equal(phases(g).at(-1),'root_error');assert.equal(phases(g).includes('root_timeout'),false);
});

test('exact current peer, origin, epoch and ports are guarded before inspecting child payload',()=>{
  const f=fixture();f.start();const baseline=phases(f);
  let reads=0;
  for(const guards of [{source:{},origin:'https://frame.example'},{source:f.frame.contentWindow,origin:'https://other.example'}]){
    const poison={...guards,ports:[]};Object.defineProperty(poison,'data',{get(){reads++;throw Error('PRIVATE_URL_TICKET');}});f.view.emit('message',poison);
  }
  for(const extra of [{source:{}},{origin:'https://other.example'},{origin:'null'},{ports:[{}]}])f.hint('boot_cookie_pending',extra);
  assert.deepEqual(phases(f),baseline);
  assert.equal(reads,0);
  f.nextEpoch();f.hint();assert.deepEqual(phases(f),baseline);
  const g=fixture();g.start();g.replace();g.hint();assert.deepEqual(phases(g),['root_launch','root_frame','root_attached']);
});

test('malformed, oversized and accessor payloads are rejected without reading or serializing them',()=>{
  const f=fixture();f.start();let reads=0;const getter={schema:'soty.runtime-phase.v2',watchId:f.watchId(),get phase(){reads++;throw Error('PRIVATE');}};
  const watchGetter={schema:'soty.runtime-phase.v2',phase:'boot_cookie_pending',get watchId(){reads++;throw Error('PRIVATE');}};
  const bad=[[],getter,watchGetter,{schema:'soty.runtime-phase.v2',watchId:f.watchId(),phase:'boot_cookie_pending',url:'SECRET'},
    {schema:'soty.runtime-phase.v1',phase:'ready'}, {schema:'soty.app-slot-ready.v1',ready:true,requestId:'SECRET'},
    new Proxy({},{ownKeys(){throw Error('SECRET');}}),{schema:'soty.runtime-phase.v1',phase:'SECRET'.repeat(100000)}];
  for(const data of bad)f.hint(undefined,{data});assert.equal(reads,0);assert.deepEqual(phases(f),['root_launch','root_frame','root_attached']);
  f.hint();assert.deepEqual(phases(f).slice(-2),['boot_document_ack','boot_cookie_pending']);
});

test('current phase acknowledgement is nonce-bound, recorded once, and never authorizes readiness',()=>{
  const f=fixture();f.start();const baseline=phases(f);
  for(const data of [
    {schema:'soty.runtime-phase.v1',phase:'boot_cookie_pending'},
    {schema:'soty.runtime-phase.v2',watchId:'f'.repeat(32),phase:'boot_cookie_pending'},
    {schema:'soty.runtime-phase.v2',watchId:f.watchId(),phase:'ready'},
    {schema:'soty.runtime-phase.v2',watchId:f.watchId(),phase:'boot_cookie_pending',url:'PRIVATE'},
  ])f.hint(undefined,{data});
  assert.deepEqual(phases(f),baseline);
  f.hint();f.hint();f.frame.emit('load');f.tick(12_001);
  assert.equal(phases(f).filter(x=>x==='boot_document_ack').length,1);
  assert.equal(phases(f).includes('root_boot_unobserved'),false);
  assert.equal(f.api.snapshot().observation,'partial');
  assert.equal('ready' in f.api.snapshot(),false);
  assert.equal(JSON.stringify(f.api.snapshot()).includes(f.watchId()),false);
});

test('same window and origin cannot acknowledge a newer observation using a stale watch ID',()=>{
  const f=fixture();f.start();const old=f.watchId();
  f.api.arm();f.api.beginManual();f.api.attach(f.frame,'https://frame.example');f.api.inserted();
  assert.notEqual(f.watchId(),old);
  f.hint(undefined,{data:{schema:'soty.runtime-phase.v2',watchId:old,phase:'boot_navigation'}});
  assert.equal(phases(f).includes('boot_document_ack'),false);
  f.hint('boot_admission_pending');
  assert.deepEqual(phases(f).slice(-2),['boot_document_ack','boot_admission_pending']);
});

test('CSP retains only page-level literal directive/disposition group and never reads URI/error fields',()=>{
  const f=fixture();f.start();let reads=0;
  const event={effectiveDirective:'frame-src',disposition:'enforce'};
  for(const key of ['blockedURI','documentURI','sourceFile','originalPolicy','sample','statusCode','lineNumber','error'])Object.defineProperty(event,key,{get(){reads++;throw Error('PRIVATE');}});
  f.doc.emit('securitypolicyviolation',event);assert.equal(reads,0);assert.equal(phases(f).at(-1),'page_csp_frame_enforce');
  f.doc.emit('securitypolicyviolation',{effectiveDirective:'connect-src',disposition:'report'});assert.equal(phases(f).at(-1),'page_csp_other_report');
  assert.equal(JSON.stringify(f.api.snapshot()).includes('frame.example'),false);
});

test('cap retains the beginning, stops observers/probes and TTL deletes all evidence',()=>{
  const f=fixture();f.start();for(let i=0;i<40;i++)f.hint(i%2?'boot_cookie_pending':'boot_admission_pending');
  const snap=f.api.snapshot();assert.equal(snap.events.length,24);assert.equal(snap.events[0].phase,'root_launch');assert.equal(snap.truncated,true);
  assert.equal(f.view.count()+f.doc.count()+f.frame.count(),0);f.tick(30_000);assert.equal(f.probes.length,0);assert.equal(f.api.snapshot().events.length,24);
  f.tick(90_000);assert.equal(f.api.snapshot(),null);assert.equal(f.tasks.size,0);
});

test('Root DOM sample detects overlay/hidden/disconnected state and never counts an iframe below an overlay as top hit',()=>{
  const f=fixture();assert.deepEqual(sampleRuntimeFrame(f.frame,f.view),{connected:true,hasArea:true,ancestorsVisible:true,hit:'all'});
  f.hit({});assert.equal(sampleRuntimeFrame(f.frame,f.view).hit,'none');
  f.frame.parentElement={parentElement:null,style:{display:'none'}};assert.equal(sampleRuntimeFrame(f.frame,f.view).ancestorsVisible,false);
  f.frame.isConnected=false;assert.deepEqual(sampleRuntimeFrame(f.frame,f.view),{connected:false,hasArea:false,ancestorsVisible:false,hit:'unknown'});
});

test('an open diagnostic dialog cannot replace the last pre-dialog sample with its own overlay',()=>{
  const f=fixture();f.start();assert.equal(f.api.snapshot().view.hit,'all');f.pause();f.hit({});f.tick(12_000);assert.equal(f.api.snapshot().view.hit,'all');
  f.resume();f.api.sample();assert.equal(f.api.snapshot().view.hit,'none');
});

test('explicit copy exports only new closed data; failed clipboard and poison fields never enter the trace',async()=>{
  const f=fixture();Object.defineProperty(f.frame,'src',{get(){throw Error('SECRET_TICKET');}});f.start();
  const poison={get reason(){throw Error('SECRET_ERROR');},message:'SECRET_URL'};f.api.launchFailed(poison);f.hint('boot_cookie_pending',{data:{schema:'soty.runtime-phase.v1',phase:'boot_cookie_pending',error:poison}});
  assert.equal(f.copies.length,0);const snap=f.api.snapshot();snap.events[0].phase='SECRET_MUTATION';assert.equal(await f.api.copy(),true);
  assert.equal(f.copies.length,1);assert.doesNotMatch(f.copies[0],/SECRET|https:|ticket|appId|requestId|origin|ready/u);
  f.view.navigator.clipboard.writeText=async()=>{throw poison;};assert.equal(await f.api.copy(),false);
});

test('reset/clear/dispose and inactive routes remove state, stale callbacks and clipboard access',async()=>{
  for(const method of ['reset','clear','dispose']){
    const f=fixture();f.start();f.api[method]();f.hint();f.frame.emit('load');f.tick(200_000);
    assert.equal(f.api.snapshot(),null);assert.equal(f.probes.length,0);assert.equal(f.tasks.size,0);assert.equal(f.view.count()+f.doc.count()+f.frame.count(),0);
  }
  const f=fixture();f.start();f.leave();f.hint();assert.equal(f.api.snapshot(),null);assert.equal(await f.api.copy(),false);assert.equal(f.copies.length,0);f.api.dispose();
});

test('expiry is checked at access even when background timers have not run',async()=>{
  const armed=fixture();armed.api.arm();armed.jump(120_001);assert.equal(armed.api.beginManual(),false);assert.equal(armed.api.state(),'empty');assert.equal(armed.tasks.size,0);
  const f=fixture();f.start();f.jump(120_001);assert.equal(await f.api.copy(),false);assert.equal(f.copies.length,0);assert.equal(f.api.snapshot(),null);assert.equal(f.tasks.size,0);assert.equal(f.view.count()+f.doc.count()+f.frame.count(),0);
});
