import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import vm from 'node:vm';
import {renderBootPage} from '../server/runtime-pages.mjs';
import {createAppRuntimePhaseTrace} from '../../../src/world/app-runtime-phase-trace.mjs';

class Events {
  listeners = new Map();
  addEventListener(type,fn) { if(!this.listeners.has(type))this.listeners.set(type,new Set());this.listeners.get(type).add(fn); }
  removeEventListener(type,fn) { this.listeners.get(type)?.delete(fn); }
  emit(type,event={}) { for(const fn of [...(this.listeners.get(type)??[])])fn(event); }
}
const waitUntil = async predicate => {
  for(let i=0;i<100;i++) { if(predicate())return;await new Promise(resolve=>setTimeout(resolve,5)); }
  assert.fail('HTTP fixture did not settle');
};

async function fixture(t,{deny=false}={}) {
  const calls=[],ticket='T'.repeat(43),sessionCheck='C'.repeat(43),frameSelector='F'.repeat(43);
  const server=http.createServer(async(req,res)=>{
    let body='';for await(const chunk of req)body+=chunk;
    calls.push({method:req.method,path:req.url,body});
    const reply=(value,status=200)=>{res.writeHead(status,{'content-type':'application/json'});res.end(JSON.stringify(value));};
    if(req.url==='/_soty/boot') {res.writeHead(200,{'content-type':'text/html'});res.end(renderBootPage({shellUrl:origin+'/#mine',scoped:true}));}
    else if(req.url==='/_soty/session'&&req.method==='POST')reply(deny?{ok:false,error:'apps_access_denied'}:{ok:true,entryPath:'/embed',sessionCheck,frame:frameSelector,scopedRequestId:'R'.repeat(43)},deny?403:200);
    else if(req.url==='/_soty/session'&&req.method==='GET')reply({ok:true,sessionCheck});
    else if(req.url==='/api/embed/session-continue')reply({schema:'soty.source-session-continuation.v1',ready:true});
    else reply({ok:false},404);
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+server.address().port;
  t.after(()=>new Promise(resolve=>{server.closeAllConnections();server.close(resolve);}));
  const markup=await(await fetch(origin+'/_soty/boot')).text();
  const script=[...markup.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gu)][0][1];
  const root=new Events(),child=new Events(),document=new Events(),frame=new Events(),tasks=new Map(),elements=new Map();
  let now=0,next=0,navigated=null,trace;
  const element=id=>{
    if(!elements.has(id))elements.set(id,{hidden:true,disabled:false,textContent:'',dataset:{},attrs:new Map(),
      get href(){return this.attrs.get('href');},set href(value){this.attrs.set('href',value);},
      setAttribute(k,v){this.attrs.set(k,v);},removeAttribute(k){this.attrs.delete(k);},addEventListener(){}});
    return elements.get(id);
  };
  Object.assign(frame,{isConnected:true,parentElement:null,style:{display:'block',visibility:'visible',opacity:'1'},
    getBoundingClientRect:()=>({left:0,top:0,width:800,height:600}),
    contentWindow:{postMessage:(data,target)=>{assert.equal(target,origin);child.emit('message',{source:root,origin,ports:[],data});}}});
  Object.assign(document,{elementsFromPoint:()=>[frame]});
  Object.assign(root,{document,performance:{now:()=>now},crypto:{randomUUID:()=> 'a'.repeat(32)},getComputedStyle:n=>n.style,
    setTimeout:(fn,ms)=>{const id=++next;tasks.set(id,{fn,at:now+ms});return id;},clearTimeout:id=>tasks.delete(id)});
  const childTimers=new Map();let childNext=0;
  Object.assign(child,{self:child,top:{},parent:root});
  root.postMessage=(data,target)=>{assert.equal(target,origin);root.emit('message',{source:frame.contentWindow,origin,ports:[],data});};
  const location={hash:'#'+ticket,pathname:'/_soty/boot',search:'',replace:path=>{navigated=path;}};
  const context={window:child,location,performance:{now:()=>now},
    document:{readyState:'complete',body:element('body'),getElementById:element},
    history:{replaceState(){location.hash='';}},URL,AbortController,AbortSignal,TextDecoder,
    setTimeout:(fn,ms)=>{const id=++childNext;childTimers.set(id,{fn,ms});return id;},clearTimeout:id=>childTimers.delete(id),
    fetch:(path,options)=>fetch(new URL(path,origin),options)};
  const begin=()=>{
    trace=createAppRuntimePhaseTrace({view:root,isCurrent:()=>true,getFrame:()=>frame,getEpoch:()=>1});
    trace.arm();trace.beginManual();trace.attach(frame,origin);trace.inserted();
  };
  const tick=ms=>{
    const end=now+ms;
    while(true){const job=[...tasks].sort((a,b)=>a[1].at-b[1].at)[0];if(!job||job[1].at>end)break;tasks.delete(job[0]);now=job[1].at;job[1].fn();}
    now=end;
  };
  t.after(()=>{child.emit('pagehide');trace?.dispose();});
  return {calls,markup,frame,begin,tick,run:()=>vm.runInNewContext(script,context,{timeout:1000}),
    element,location,snapshot:()=>trace.snapshot(),navigated:()=>navigated};
}

test('HTTP document load with its script not run is explicitly unobserved, not an acknowledged Boot',async t=>{
  const f=await fixture(t);assert.match(f.markup,/Проверяем ваш доступ/u);
  f.begin();f.frame.emit('load');f.tick(12000);
  const events=f.snapshot().events.map(x=>x.phase);
  assert.ok(events.includes('root_load'));assert.equal(events.at(-1),'root_boot_unobserved');
  assert.equal(events.includes('boot_document_ack'),false);assert.equal(f.snapshot().observation,'partial');
  assert.equal(f.calls.length,1,'observation cannot create an admission request');
});

test('actual HTTP Boot script and Root correlate phases without changing admission/continuation or claiming readiness',async t=>{
  const f=await fixture(t);f.begin();f.run();f.tick(0);
  await waitUntil(()=>f.navigated()!==null);
  const events=f.snapshot().events.map(x=>x.phase);
  assert.equal(events.filter(x=>x==='boot_document_ack').length,1);
  assert.ok(events.includes('boot_admission_pending'));assert.ok(events.includes('boot_cookie_pending'));
  assert.ok(events.includes('boot_continuation_pending'));assert.equal(events.at(-1),'boot_navigation');
  assert.deepEqual(f.calls.map(x=>[x.method,x.path]),[['GET','/_soty/boot'],['POST','/_soty/session'],['GET','/_soty/session'],['POST','/api/embed/session-continue']]);
  assert.equal(f.navigated(),'/embed?sotyFrame='+'F'.repeat(43));assert.equal(f.location.hash,'');
  assert.equal(f.snapshot().observation,'partial');assert.equal('ready' in f.snapshot(),false);
  const safe=JSON.stringify(f.snapshot());for(const secret of ['T'.repeat(43),'C'.repeat(43),'F'.repeat(43),'R'.repeat(43),'a'.repeat(32),'http:'])assert.equal(safe.includes(secret),false);
});

test('HTTP admission denial stays a visible failure and an observed failure, with no cookie bypass or retry',async t=>{
  const f=await fixture(t,{deny:true});f.begin();f.run();f.tick(0);
  await waitUntil(()=>f.element('status-title').textContent==='Доступ закрыт');
  assert.equal(f.snapshot().events.at(-1).phase,'boot_failed');assert.equal(f.navigated(),null);
  assert.equal(f.calls.length,2);assert.equal(f.element('status-title').textContent,'Доступ закрыт');
  f.tick(12000);assert.equal(f.snapshot().events.map(x=>x.phase).includes('root_boot_unobserved'),false);
});
