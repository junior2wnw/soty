import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {inspectMaterialMedia} from '../../modules/materials/server/media.mjs';

const source=readFileSync(new URL('./canvas-voice-capture.ts',import.meta.url),'utf8');
const shared=readFileSync(new URL('./feedback-media.mjs',import.meta.url),'utf8');
const css=readFileSync(new URL('./canvas-voice-capture.css',import.meta.url),'utf8');
const tone=readFileSync(new URL('../../modules/materials/test/fixtures/tone.webm',import.meta.url));
const toneOgg=readFileSync(new URL('../../modules/materials/test/fixtures/tone.ogg',import.meta.url));
const settle=async()=>{for(let index=0;index<8;index++)await new Promise(resolve=>setImmediate(resolve));};
const flatten=node=>[node,...node.children.flatMap(flatten)];

// Executes both production UI and the shared production capture lifecycle. Only
// the browser DOM, clock and MediaRecorder devices are controlled ports: no user mic.
function harness({code=source,sharedCode=shared,permission='allowed',supported=['audio/webm;codecs=opus'],secure=true,onFile}={}){
  let current=true,clock=0,nextTimer=0,release,rejectPermission;
  const calls={requests:[],recorders:[],stopped:0,files:[],activity:[],timers:new Map()};
  class Node {
    constructor(tag,className='',text=''){this.tagName=tag.toUpperCase();this.className=className;this.textContent=text;this.children=[];this.parentElement=null;this.attributes=new Map();this.dataset={};this.listeners=new Map();this.hidden=false;this.disabled=false;}
    append(...nodes){for(const node of nodes){node.parentElement=this;this.children.push(node);}}
    prepend(node){node.parentElement=this;this.children.unshift(node);}
    remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(node=>node!==this);this.parentElement=null;}
    setAttribute(key,value){this.attributes.set(key,String(value));}
    querySelector(selector){return flatten(this).find(node=>selector==='span:not(.cv-dot)'&&node.tagName==='SPAN'&&node.className!=='cv-dot')??null;}
    addEventListener(type,handler){if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push(handler);}
    removeEventListener(type,handler){this.listeners.set(type,(this.listeners.get(type)??[]).filter(item=>item!==handler));}
    click(){if(!this.disabled)for(const handler of this.listeners.get('click')??[])handler({isTrusted:true});}
  }
  const stream=()=>({getTracks:()=>[{stop(){calls.stopped++;}}]});
  class Recorder {
    state='inactive';static isTypeSupported(value){return supported.includes(value);}
    constructor(input,options){this.stream=input;this.options=options;this.final=options.mimeType.includes('ogg')?toneOgg:tone;calls.recorders.push(this);}
    start(timeslice){this.state='recording';this.timeslice=timeslice;}
    stop(){if(this.state==='inactive')return;this.state='inactive';queueMicrotask(()=>{if(this.final.length)this.ondataavailable?.({data:new Blob([this.final])});this.onstop?.();});}
    data(bytes){this.ondataavailable?.({data:new Blob([bytes])});}
    fail(){this.onerror?.({error:new Error('device failed')});}
  }
  const mediaDevices={getUserMedia:args=>{calls.requests.push(args);
    if(permission==='denied')return Promise.reject(new DOMException('Denied','NotAllowedError'));
    if(permission==='pending')return new Promise((resolve,reject)=>{release=()=>resolve(stream());rejectPermission=reject;});
    return Promise.resolve(stream());
  }};
  const addTimer=(callback,delay,repeat)=>{const id=++nextTimer;calls.timers.set(id,{callback,delay,repeat,due:clock+delay});return id;};
  const element=(tag,className='',text='')=>new Node(tag,className,text);
  const context=vm.createContext({Blob,File,Error,DOMException,AbortController,queueMicrotask,Uint8Array,ArrayBuffer,Map,Set,Date,
    isSecureContext:secure,navigator:{mediaDevices},MediaRecorder:Recorder,performance:{now:()=>clock},
    setInterval:(callback,delay)=>addTimer(callback,delay,true),setTimeout:(callback,delay)=>addTimer(callback,delay,false),
    clearInterval:id=>calls.timers.delete(id),clearTimeout:id=>calls.timers.delete(id),
    el:element,button:(label,_icon,className='')=>{const node=element('button',className);node.append(element('span','',label));return node;},
  });
  const compiled=ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
  vm.runInContext(`${sharedCode.replace(/^export /gmu,'')}\n${compiled}`,context);
  const host=element('div'),controller=new AbortController();
  const api=context.mountCanvasVoiceCapture(host,{isCurrent:()=>current,signal:controller.signal,onFile:file=>{calls.files.push(file);return onFile?.(file);},onRecordingChange:value=>calls.activity.push(value)});
  const root=host.children[0],node=name=>flatten(root).find(node=>node.className.split(/\s+/u).includes(name));
  return{api,host,root,node,calls,controller,current(value){current=value;},release(){assert.ok(release);release();},deny(){assert.ok(rejectPermission);rejectPermission(new DOMException('Denied','NotAllowedError'));},
    advance(milliseconds){clock+=milliseconds;for(const[id,timer]of [...calls.timers])if(timer.due<=clock&&calls.timers.has(id)){if(timer.repeat)timer.due=clock+timer.delay;else calls.timers.delete(id);timer.callback();}},
  };
}

test('mount is silent; explicit record → stop hands one unchanged Opus file to the real Materials validator',async()=>{
  for(const format of['webm','ogg']){
  const h=harness({supported:[`audio/${format};codecs=opus`]});assert.equal(h.calls.requests.length,0);assert.equal(h.calls.timers.size,0);assert.equal(h.node('cv-cancel').hidden,true);
  h.node('cv-record').click();await settle();assert.equal(h.calls.requests.length,1);assert.equal(h.root.dataset.state,'recording');
  assert.equal(h.calls.recorders[0].options.mimeType,`audio/${format};codecs=opus`);assert.equal(h.calls.recorders[0].options.audioBitsPerSecond,48000);assert.equal(h.calls.recorders[0].timeslice,500);
  h.advance(2300);assert.equal(h.node('cv-time').textContent,'0:02');h.node('cv-record').click();await settle();
  assert.equal(h.calls.files.length,1);assert.equal(h.calls.files[0].type,`audio/${format}`);assert.ok(h.calls.files[0].name.endsWith(`.${format}`));
  const bytes=Buffer.from(await h.calls.files[0].arrayBuffer());assert.deepEqual(bytes,format==='ogg'?toneOgg:tone);
  const metadata=await inspectMaterialMedia(bytes,'audio',`audio/${format}`,{fileBytes:2*1024*1024,audioSeconds:120});
  // WebM admission bounds packet timestamps but intentionally reports no decoded
  // duration; Ogg granules provide its exact250ms. Clock bounds are tested below.
  assert.deepEqual(metadata,{width:null,height:null,durationMs:format==='ogg'?250:null});
  assert.equal(h.calls.stopped,1);assert.deepEqual(h.calls.activity,[true,false]);assert.equal(h.calls.timers.size,0);assert.equal(h.root.dataset.state,'idle');h.api.dispose();assert.equal(h.host.children.length,0);
  }
});

test('unsupported and insecure browsers do not ask for a microphone; permission denial is actionable and clean',async()=>{
  for(const options of[{supported:['audio/mp4']},{secure:false},{permission:'denied'}]){
    const h=harness(options);h.node('cv-record').click();await settle();
    assert.equal(h.calls.requests.length,options.permission==='denied'?1:0);assert.equal(h.calls.files.length,0);assert.equal(h.calls.stopped,0);assert.equal(h.calls.timers.size,0);
    assert.match(h.node('cv-status').textContent,options.secure===false?/HTTPS|localhost/u:options.permission==='denied'?/Разрешите доступ/u:/Opus/u);h.api.dispose();
  }
});

test('close, external abort, disable and account switch discard late permission streams without file delivery',async()=>{
  for(const action of['dispose','abort','disable','account','cancel']){
    const h=harness({permission:'pending'});h.node('cv-record').click();assert.equal(h.root.dataset.state,'permission');
    if(action==='dispose')h.api.dispose();else if(action==='abort')h.controller.abort();else if(action==='disable')h.api.setDisabled(true);else if(action==='account'){h.current(false);h.advance(100);}else h.node('cv-cancel').click();
    h.release();await settle();assert.equal(h.calls.recorders.length,0,action);assert.equal(h.calls.stopped,1,action);assert.equal(h.calls.files.length,0,action);assert.equal(h.calls.timers.size,0,action);assert.deepEqual(h.calls.activity,[true,false],action);h.api.dispose();
  }
});

test('recording cancellation, device failure and double start free every track and cannot deliver a file',async()=>{
  for(const action of['dispose','abort','disable','account','cancel','failure']){
    const h=harness();h.node('cv-record').click();h.node('cv-record').click();await settle();assert.equal(h.calls.requests.length,1);
    if(action==='dispose')h.api.dispose();else if(action==='abort')h.controller.abort();else if(action==='disable')h.api.setDisabled(true);else if(action==='account'){h.current(false);h.advance(100);}else if(action==='failure')h.calls.recorders[0].fail();else h.node('cv-cancel').click();
    await settle();assert.equal(h.calls.stopped,1,action);assert.equal(h.calls.files.length,0,action);assert.equal(h.calls.timers.size,0,action);assert.deepEqual(h.calls.activity,[true,false],action);h.api.dispose();
  }
});

test('two-MiB overflow and a capture resumed after120seconds are rejected, not partly accepted',async()=>{
  const oversized=harness();oversized.node('cv-record').click();await settle();oversized.calls.recorders[0].data(new Uint8Array(2*1024*1024+1));await settle();
  assert.equal(oversized.calls.files.length,0);assert.equal(oversized.calls.stopped,1);assert.match(oversized.node('cv-status').textContent,/2 МБ/u);oversized.api.dispose();
  const late=harness();late.node('cv-record').click();await settle();late.advance(121000);await settle();
  assert.equal(late.calls.files.length,0);assert.equal(late.calls.stopped,1);assert.match(late.node('cv-status').textContent,/Не удалось/u);assert.equal(late.calls.timers.size,0);late.api.dispose();
  const bounded=harness();bounded.node('cv-record').click();await settle();bounded.advance(119750);await settle();
  assert.equal(bounded.calls.files.length,1);assert.equal(bounded.calls.stopped,1);assert.equal(bounded.calls.timers.size,0);bounded.api.dispose();
});

test('a stop requested on time still rejects a final chunk delivered after the hard duration boundary',async()=>{
  const h=harness();h.node('cv-record').click();await settle();h.advance(119750);h.advance(2000);await settle();
  assert.equal(h.calls.files.length,0,'Queued stop finalization after120s cannot bypass the wall-clock cap');assert.equal(h.calls.stopped,1);h.api.dispose();
});

test('finalization revoked in the same task cannot invoke onFile, and retry starts a new capture only by a click',async()=>{
  const h=harness();h.node('cv-record').click();await settle();h.node('cv-record').click();h.current(false);await settle();
  assert.equal(h.calls.files.length,0);assert.equal(h.calls.stopped,1);h.current(true);h.api.setDisabled(false);await settle();assert.equal(h.calls.requests.length,1);
  h.node('cv-record').click();await settle();assert.equal(h.calls.requests.length,2);h.calls.recorders[1].final=Buffer.alloc(0);h.node('cv-record').click();await settle();
  assert.equal(h.calls.files.length,0);assert.match(h.node('cv-status').textContent,/пустая/u);assert.equal(h.calls.stopped,2);h.api.dispose();
});

test('late permission cleanup and explicit-gesture oracles detect broken controls',async()=>{
  const withoutLateStop=shared.replace('if (cancelled || signal?.aborted) { stopTracks(); throw new DOMException(\'Cancelled\', \'AbortError\'); }',"if (cancelled || signal?.aborted) { stream = null; throw new DOMException('Cancelled', 'AbortError'); }");
  assert.notEqual(withoutLateStop,shared);const leaked=harness({sharedCode:withoutLateStop,permission:'pending'});leaked.node('cv-record').click();leaked.api.dispose();leaked.release();await settle();
  assert.equal(leaked.calls.stopped,0,'The same stopped-track oracle must reject the deliberately missing late cleanup');assert.throws(()=>assert.equal(leaked.calls.stopped,1));
  const autoSource=source.replace('render(); if (options.signal?.aborted) dispose();','render(); if (options.signal?.aborted) dispose(); else void begin();');assert.notEqual(autoSource,source);
  const auto=harness({code:autoSource});await settle();assert.equal(auto.calls.requests.length,1);assert.throws(()=>assert.equal(auto.calls.requests.length,0),'The silent-mount criterion must fail on automatic capture');auto.api.dispose();
  const staleSource=source.replaceAll('if (!live() || session?.id !== id) return;','');assert.notEqual(staleSource,source);
  const stale=harness({code:staleSource});stale.node('cv-record').click();await settle();stale.node('cv-record').click();stale.current(false);await settle();
  assert.equal(stale.calls.files.length,1);assert.throws(()=>assert.equal(stale.calls.files.length,0),'The finalization authorization criterion must fail if its guards are removed');stale.api.dispose();
});

test('control semantics and motion preferences stay accessible without fabricated waveform',()=>{
  const h=harness();assert.equal(h.root.attributes.get('role'),'group');assert.equal(h.node('cv-status').attributes.get('role'),'status');assert.equal(h.node('cv-time').attributes.get('aria-label'),'Длительность записи');
  assert.match(css,/min-height:44px/u);assert.match(css,/prefers-reduced-motion:reduce/u);assert.match(css,/forced-colors:active/u);assert.match(css,/focus-visible/u);
  assert.doesNotMatch(source,/AudioContext|AnalyserNode|createObjectURL|getDisplayMedia|request\(/u);h.api.dispose();
});
