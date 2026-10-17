import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { renderBootPage, renderStatusPage } from '../server/runtime-pages.mjs';
import { createPalette } from '../../ui/palette.mjs';
import { roundedHexPath } from '../../ui/hex.mjs';

const shellUrl = 'https://soty.example/#app-entry?id=app-test&path=%2F';
const ticket = 'T'.repeat(43), sessionCheck = 'C'.repeat(43), nonce = Buffer.from('1234567890abcdef').toString('base64');
const response = (body, ok = true) => ({ ok, status: ok ? 200 : 403, json: async () => body,
  arrayBuffer:async()=>new TextEncoder().encode(JSON.stringify(body)).buffer });
const pending = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const flush = () => new Promise(resolve => setImmediate(resolve));

function browser(markup, { framed = false, hash = '#' + ticket, responses = [], cleanThrows = false, documentState = 'complete' } = {}) {
  const scripts = [...markup.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gu)];
  assert.equal(scripts.length, 1);
  const elements = new Map(), calls = [], events = [], navigations = [], listeners = new Map(), timers = new Map(), parentMessages = [], registrations=[], forbidden={storageReads:0,telemetryCalls:0};
  let timerId = 0, virtualNow = 0;
  const element = id => {
    if (!elements.has(id)) elements.set(id, { hidden: true, disabled: false, textContent: '', dataset: {}, attributes: new Map(), handlers: new Map(),
      get href() { return this.attributes.get('href'); },
      set href(value) { this.attributes.set('href',value); events.push(['href',id,value]); },
      setAttribute(key, value) { this.attributes.set(key, value); },
      removeAttribute(key) { this.attributes.delete(key); },
      addEventListener(type, callback) { this.handlers.set(type, callback); },
    });
    return elements.get(id);
  };
  const location = { hash, pathname: '/_soty/boot', search: '?path=%2Funtrusted-query',
    replace(path) { events.push(['navigate', path]); navigations.push(path); } };
  const window = { addEventListener(type, callback) { registrations.push({type,fragmentPresent:!!location.hash});if(!listeners.has(type))listeners.set(type,new Set());listeners.get(type).add(callback); },
    removeEventListener(type,callback){listeners.get(type)?.delete(callback);} };
  window.self = window; window.top = framed ? {} : window;
  const parent = { postMessage(data, origin) { parentMessages.push({ data: structuredClone(data), origin }); } }; window.parent = framed ? parent : window;
  const context = { window, location, performance:{now:()=>virtualNow}, document: { title: '', readyState: documentState, body: element('body'), getElementById: element }, URL, AbortController, AbortSignal, TextDecoder,
    history: { replaceState(_state, _title, path) { events.push(['cleanup', path]); if (cleanThrows) throw new Error('blocked history'); location.hash = ''; } },
    setTimeout(callback, delay) { const id = ++timerId; timers.set(id, { callback, delay }); return id; }, clearTimeout(id) { timers.delete(id); },
    fetch(url, options) {
      events.push(['fetch', options.method]); calls.push({ url, options });
      const next = responses.shift();
      if (typeof next === 'function') return next(url, options);
      if (next instanceof Error) return Promise.reject(next);
      if (next !== undefined) return Promise.resolve(next);
      return Promise.reject(new Error('unexpected request'));
    },
  };
  for(const target of [window,context]){
    for(const key of ['localStorage','sessionStorage','indexedDB'])Object.defineProperty(target,key,{get(){forbidden.storageReads++;throw Error('forbidden_storage');}});
    target.navigator={sendBeacon(){forbidden.telemetryCalls++;throw Error('forbidden_beacon');}};
    target.console=Object.fromEntries(['log','warn','error','debug'].map(key=>[key,()=>{forbidden.telemetryCalls++;throw Error('forbidden_console');}]));
  }
  vm.runInNewContext(scripts[0][1], context, { timeout: 1000 });
  return { context, element, calls, events, navigations, timers, parent, parentMessages, registrations,forbidden,
    elapse:ms=>{virtualNow+=ms;},
    phaseHandler:()=>[...(listeners.get('message')??[])].find(callback=>callback.name==='receive'),
    click: (id,event={button:0}) => element(id).handlers.get('click')?.(event),
    emit: (type, event = {}) => {for(const callback of [...(listeners.get(type)??[])])callback(event);},
    expire: () => { for (const value of [...timers.values()]) value.callback(); },
  };
}

const phaseWatch = env => env.emit('message',{data:{schema:'soty.runtime-phase-watch.v1'},origin:'https://soty.example',source:env.parent,ports:[]});
const diagnosticMessages = env => env.parentMessages.filter(x=>x.data.schema==='soty.runtime-phase.v1');
const diagnosticPhases = env => diagnosticMessages(env).map(x=>x.data.phase);
const phaseWatchV2 = (env,watchId='a'.repeat(32),extra={}) => env.emit('message',{
  source:env.parent,origin:'https://soty.example',ports:[],data:{schema:'soty.runtime-phase-watch.v2',watchId},...extra});
const correlatedMessages = env => env.parentMessages.filter(x=>x.data.schema==='soty.runtime-phase.v2');

const scopedResponses = (issued = {}) => [
  response({ok:true,entryPath:'/embed',sessionCheck,frame:'F'.repeat(43),scopedRequestId:'R'.repeat(43),...issued}),
  response({ok:true,sessionCheck}),
  response({schema:'soty.source-session-continuation.v1',ready:true}),
];

test('correlated boot observation echoes only the current watch ID and phases with the existing auth/navigation sequence',async()=>{
  const admission=pending(),cookie=pending(),continuation=pending(),watchId='a'.repeat(32);
  const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:[()=>admission.promise,()=>cookie.promise,()=>continuation.promise]});
  phaseWatchV2(env,watchId);
  assert.equal(env.context.location.hash,'');assert.deepEqual(correlatedMessages(env).map(x=>x.data.phase),['boot_admission_pending']);
  admission.resolve(scopedResponses()[0]);await flush();cookie.resolve(scopedResponses()[1]);await flush();
  continuation.resolve(scopedResponses()[2]);await flush();
  assert.equal(env.calls.length,3);assert.deepEqual(env.navigations,['/embed?sotyFrame='+'F'.repeat(43)]);
  assert.deepEqual(correlatedMessages(env).map(x=>x.data.phase),['boot_admission_pending','boot_cookie_pending','boot_continuation_pending','boot_navigation']);
  for(const message of correlatedMessages(env)){
    assert.equal(message.origin,'https://soty.example');assert.equal(message.data.watchId,watchId);
    assert.deepEqual(Object.keys(message.data).sort(),['phase','schema','watchId']);assert.equal('ready' in message.data,false);
  }
  const serialized=JSON.stringify(correlatedMessages(env));for(const secret of [ticket,sessionCheck,'F'.repeat(43),'R'.repeat(43),'/embed'])assert.equal(serialized.includes(secret),false);
  assert.deepEqual(env.forbidden,{storageReads:0,telemetryCalls:0});
});

test('correlated observer rejects forged peers/origins, getters, extra keys and stale/mixed watches without additional requests',()=>{
  const held=pending(),env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:[()=>held.promise]});
  let reads=0;const poison={schema:'soty.runtime-phase-watch.v2',get watchId(){reads++;throw Error('PRIVATE');}};
  for(const extra of [{source:{}},{origin:'https://evil.example'},{origin:'null'},{ports:[{}]},
    {data:poison},{data:{schema:'soty.runtime-phase-watch.v2',watchId:'bad'}},
    {data:{schema:'soty.runtime-phase-watch.v2',watchId:'a'.repeat(32),url:'PRIVATE'}}])phaseWatchV2(env,'a'.repeat(32),extra);
  assert.equal(reads,0);assert.equal(env.parentMessages.length,0);
  phaseWatchV2(env);assert.equal(correlatedMessages(env).length,1);
  phaseWatchV2(env,'b'.repeat(32));phaseWatch(env);
  assert.equal(correlatedMessages(env).length,1);assert.equal(diagnosticMessages(env).length,0);assert.equal(env.calls.length,1);
  for(let i=0;i<40;i++)phaseWatchV2(env);assert.equal(correlatedMessages(env).length,24);
  assert.equal(env.calls.length,1,'duplicates cannot restart admission or create a second request');
  assert.equal(env.timers.size,1,'the existing admission request deadline remains, not diagnostic timers');
});

test('correlated observation closes on pagehide/expiry and cannot revive from BFCache',()=>{
  for(const close of ['pagehide','expiry']){
    const held=pending(),env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:[()=>held.promise]});
    phaseWatchV2(env);const before=correlatedMessages(env).length;
    if(close==='pagehide'){env.emit('pagehide');env.emit('pageshow',{persisted:true});}
    else env.elapse(120001);
    phaseWatchV2(env);const after=correlatedMessages(env).length;
    assert.equal(after,close==='pagehide'?before+1:before);
    phaseWatchV2(env,'b'.repeat(32));assert.equal(correlatedMessages(env).length,after);
    assert.equal(env.calls.length,1);assert.equal(env.navigations.length,0);
  }
});

test('only an initial scoped boot renders the initially inert native continuation link',()=>{
  const scoped = renderBootPage({shellUrl,scoped:true});
  assert.match(scoped, /<a class="action action-primary" id="native-action" hidden>Продолжить открытие<\/a>/u);
  assert.doesNotMatch(scoped, /id="native-action"[^>]*href=/u);
  for(const page of [renderBootPage({shellUrl}),renderStatusPage({shellUrl}),renderBootPage({shellUrl,scoped:true,renewal:true,parentOrigin:'https://soty.example'})]) {
    assert.doesNotMatch(page,/id="native-action"/u);
  }
});

test('fast initial scoped boot exposes the validated link before automatic navigation and waits for document load without a delay',async()=>{
  const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,documentState:'loading',responses:scopedResponses()});
  phaseWatch(env);await flush();
  const link=env.element('native-action'), destination='/embed?sotyFrame='+'F'.repeat(43);
  assert.equal(env.calls.length,3);assert.equal(link.hidden,false);assert.equal(link.href,destination);
  assert.equal(env.element('actions').hidden,false);assert.deepEqual(env.navigations,[]);
  assert.equal(diagnosticPhases(env).at(-1),'boot_continuation_pending');
  assert.deepEqual([...env.timers.values()].map(x=>x.delay),[120_000],'only the existing diagnostic TTL remains; no navigation delay');
  assert.equal(env.registrations.filter(x=>x.type==='load').length,1);
  env.context.document.readyState='complete';env.emit('load');await flush();
  assert.deepEqual(env.navigations,[destination]);assert.equal(diagnosticPhases(env).at(-1),'boot_navigation');
  assert.ok(env.events.findIndex(x=>x[0]==='href'&&x[1]==='native-action')<env.events.findIndex(x=>x[0]==='navigate'));
  assert.equal(link.href,destination,'a stalled auto attempt keeps the ordinary link');
  env.emit('load');await flush();assert.equal(env.navigations.length,1);assert.equal(env.calls.length,3);
  assert.deepEqual(env.forbidden,{storageReads:0,telemetryCalls:0});
});

test('already-complete scoped document keeps automatic navigation and a manual retry without extra requests or interception',async()=>{
  const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:scopedResponses()});await flush();
  const link=env.element('native-action'), destination=link.href;let prevented=0;
  assert.equal(env.navigations.length,1);assert.equal(link.hidden,false);
  assert.equal(env.registrations.filter(x=>x.type==='load').length,0);
  for(let i=0;i<2;i++)env.click('native-action',{button:0,defaultPrevented:false,preventDefault(){prevented++;}});
  assert.equal(prevented,0);assert.equal(link.href,destination);assert.equal(env.calls.length,3);
  assert.equal(env.navigations.length,1,'the link uses browser default navigation, not another script redirect');
});

test('normal manual activation before initial load suppresses the deferred auto attempt while modified activation keeps it',async()=>{
  for(const modified of [false,true]){
    const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,documentState:'interactive',responses:scopedResponses()});await flush();
    const destination=env.element('native-action').href;let prevented=0;
    env.click('native-action',{button:0,defaultPrevented:false,ctrlKey:modified,preventDefault(){prevented++;}});
    assert.equal(env.element('native-action').href,destination);assert.equal(prevented,0);
    env.context.document.readyState='complete';env.emit('load');await flush();
    assert.equal(env.navigations.length,modified?1:0);assert.equal(env.calls.length,3);
    assert.equal(env.element('native-action').href,destination);
  }
});

test('pagehide and BFCache restoration clear the ephemeral native href and cancel a pending load gate',async()=>{
  for(const documentState of ['loading','complete']){
    const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,documentState,responses:scopedResponses()});await flush();
    assert.equal(env.element('native-action').hidden,false);const attempts=env.navigations.length;
    env.emit('pagehide');await flush();
    assert.equal(env.element('native-action').hidden,true);assert.equal(env.element('native-action').href,undefined);
    assert.equal(env.element('actions').hidden,true);
    env.emit('pageshow',{persisted:true});env.context.document.readyState='complete';env.emit('load');await flush();
    assert.equal(env.element('status-title').textContent,'Нужно открыть заново');
    assert.equal(env.element('native-action').href,undefined);assert.equal(env.element('native-action').hidden,true);
    let prevented=0;env.click('native-action',{button:0,preventDefault(){prevented++;}});
    assert.equal(prevented,1,'a stale activation cannot follow a restored destination');
    assert.equal(env.navigations.length,attempts);assert.equal(env.calls.length,3);
  }
});

test('invalid admission paths, frames and cookie checks never expose a native destination',async()=>{
  const cases=[scopedResponses({entryPath:'https://untrusted.example/embed'}),scopedResponses({entryPath:'/_soty/session'}),scopedResponses({frame:'invalid'}),
    [scopedResponses()[0],response({ok:true,sessionCheck:'X'.repeat(43)})]];
  for(const responses of cases){
    const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses});await flush();
    assert.deepEqual(env.navigations,[]);assert.equal(env.element('native-action').href,undefined);assert.equal(env.element('native-action').hidden,true);
    assert.equal(env.registrations.filter(x=>x.type==='load').length,0);assert.deepEqual(env.forbidden,{storageReads:0,telemetryCalls:0});
  }
});

test('a late admission result from an old generation cannot restore the native href',async()=>{
  const held=pending(),env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:[()=>held.promise]});
  env.emit('pagehide');env.emit('pageshow',{persisted:true});held.resolve(scopedResponses()[0]);await flush();
  assert.equal(env.calls.length,1);assert.deepEqual(env.navigations,[]);
  assert.equal(env.element('native-action').href,undefined);assert.equal(env.element('native-action').hidden,true);
});

test('legacy entry and hidden renewal retain their existing flow without waiting for a document load',async()=>{
  const legacy=browser(renderBootPage({shellUrl}),{documentState:'loading',responses:[response({ok:true,entryPath:'/',sessionCheck}),response({ok:true,sessionCheck})]});await flush();
  assert.deepEqual(legacy.navigations,['/']);assert.equal(legacy.registrations.filter(x=>x.type==='load').length,0);
  const renewal=browser(renderBootPage({shellUrl,scoped:true,renewal:true,parentOrigin:'https://soty.example'}),{framed:true,documentState:'loading',responses:scopedResponses({renewal:true,renewalRequestId:'synthetic-renewal-request'})});await flush();
  assert.equal(renewal.calls.length,3);assert.deepEqual(renewal.navigations,[]);assert.equal(renewal.registrations.filter(x=>x.type==='load').length,0);
  assert.deepEqual(renewal.parentMessages,[{origin:'https://soty.example',data:{schema:'soty.app-slot-ready.v1',requestId:'synthetic-renewal-request',ready:true}}]);
});

test('actual scoped boot hints describe existing awaits only, after fragment cleanup, without adding auth or readiness',async()=>{
  const admission=pending(),cookie=pending(),continuation=pending(),frame='F'.repeat(43),requestId='R'.repeat(43);
  const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:[()=>admission.promise,()=>cookie.promise,()=>continuation.promise]});
  assert.equal(env.events[0][0],'cleanup');assert.ok(env.registrations.length);assert.ok(env.registrations.every(x=>x.fragmentPresent===false));
  assert.equal(env.context.location.hash,'');assert.equal(env.calls.length,1);assert.equal(diagnosticMessages(env).length,0);
  phaseWatch(env);assert.deepEqual(diagnosticPhases(env),['boot_admission_pending']);
  admission.resolve(response({ok:true,entryPath:'/embed',sessionCheck,frame,scopedRequestId:requestId}));await flush();
  assert.equal(env.calls.length,2);assert.equal(diagnosticPhases(env).at(-1),'boot_cookie_pending');
  cookie.resolve(response({ok:true,sessionCheck}));await flush();assert.equal(env.calls.length,3);assert.equal(diagnosticPhases(env).at(-1),'boot_continuation_pending');
  continuation.resolve(response({schema:'soty.source-session-continuation.v1',ready:false}));await flush();
  assert.equal(diagnosticPhases(env).at(-1),'boot_navigation');assert.deepEqual(env.navigations,['/embed?sotyFrame='+frame]);
  for(const {data,origin}of diagnosticMessages(env)){assert.deepEqual(Object.keys(data).sort(),['phase','schema']);assert.equal(origin,'https://soty.example');assert.equal(Object.hasOwn(data,'ready'),false);}
  const text=JSON.stringify(diagnosticMessages(env).map(x=>x.data));for(const secret of[ticket,sessionCheck,frame,requestId,'/embed','https:'])assert.equal(text.includes(secret),false);
  assert.deepEqual(env.forbidden,{storageReads:0,telemetryCalls:0});
});

test('actual scoped boot diagnostic on/off keeps identical HTTP sequence, headers, bodies, aborts, navigation and visible card',async()=>{
  const run=async armed=>{
    const frame='F'.repeat(43),requestId='R'.repeat(43);
    const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:[response({ok:true,entryPath:'/embed',sessionCheck,frame,scopedRequestId:requestId}),response({ok:true,sessionCheck}),response({schema:'soty.source-session-continuation.v1',ready:true})]});
    if(armed)phaseWatch(env);await flush();
    return{calls:env.calls.map(({url,options})=>({url,method:options.method,body:options.body,headers:structuredClone(options.headers),credentials:options.credentials,cache:options.cache,redirect:options.redirect,aborted:options.signal.aborted})),
      navigations:env.navigations,title:env.element('status-title').textContent,unarmedDiagnosticMessages:armed?null:diagnosticMessages(env).length};
  };
  const off=await run(false),on=await run(true);assert.equal(off.unarmedDiagnosticMessages,0);delete off.unarmedDiagnosticMessages;delete on.unarmedDiagnosticMessages;assert.deepEqual(off,on);
});

test('boot phase responder rejects wrong peers/origins, extra IDs/URLs, ports and accessor payloads',async()=>{
  const held=pending(),env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:[()=>held.promise]});let reads=0;
  for(const extra of[{source:{}},{origin:'https://evil.example'},{origin:'null'},{ports:[{}]},
    {data:{schema:'soty.runtime-phase-watch.v1',url:'PRIVATE',requestId:'PRIVATE'}},{data:[]},
    {data:{get schema(){reads++;throw Error('PRIVATE');}}}]){
    env.emit('message',{source:env.parent,origin:'https://soty.example',ports:[],data:{schema:'soty.runtime-phase-watch.v1'},...extra});
  }
  // Exercise the actual new callback directly for an uncloneable Proxy. Other
  // legacy listeners are outside this control; real postMessage cannot clone it.
  assert.equal(typeof env.phaseHandler(),'function');
  assert.doesNotThrow(()=>env.phaseHandler()({source:env.parent,origin:'https://soty.example',ports:[],data:new Proxy({},{ownKeys(){throw Error('PRIVATE');}})}));
  assert.equal(reads,0);assert.equal(diagnosticMessages(env).length,0);assert.equal(env.calls.length,1);
  phaseWatch(env);assert.deepEqual(diagnosticPhases(env),['boot_admission_pending']);env.emit('pagehide');
  assert.equal(diagnosticPhases(env).at(-1),'boot_inactive');assert.equal(env.calls[0].options.signal.aborted,true);
  held.resolve(response({ok:true,entryPath:'/embed',sessionCheck,scopedRequestId:'R'.repeat(43)}));await flush();assert.deepEqual(env.navigations,[]);
  const count=diagnosticMessages(env).length;env.emit('pageshow',{persisted:true});phaseWatch(env);assert.equal(diagnosticMessages(env).length,count);assert.equal(env.element('status-title').textContent,'Нужно открыть заново');
});

test('failed/cancelled boot keeps the visible failure card and exposes no caught error; diagnostics are finite',async()=>{
  const secret='PRIVATE_URL_TICKET_SELECTOR_ERROR';
  const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:[response({ok:false,error:secret},false)]});phaseWatch(env);await flush();
  assert.equal(diagnosticPhases(env).at(-1),'boot_failed');assert.equal(env.navigations.length,0);assert.equal(env.calls.length,1);assert.notEqual(env.element('status-title').textContent,'');
  assert.equal(JSON.stringify(diagnosticMessages(env).map(x=>x.data)).includes(secret),false);
  const capped=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,hash:'#bad'});
  for(let i=0;i<40;i++)phaseWatch(capped);assert.equal(diagnosticMessages(capped).length,24);assert.equal(capped.timers.size,0);
  const expired=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,hash:'#bad'});phaseWatch(expired);
  assert.equal([...expired.timers.values()][0].delay,120_000);expired.expire();phaseWatch(expired);assert.equal(diagnosticMessages(expired).length,1);
  const throttled=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,hash:'#bad'});phaseWatch(throttled);throttled.elapse(120001);phaseWatch(throttled);assert.equal(diagnosticMessages(throttled).length,1);assert.equal(throttled.timers.size,0);
});

test('legacy/status/top-level/hidden renewal cannot send the new phase schema, and fast boot without handshake is honestly unobserved',async()=>{
  const responses=()=>[response({ok:true,entryPath:'/embed',sessionCheck,scopedRequestId:'R'.repeat(43),renewal:true,renewalRequestId:'N'.repeat(43)}),response({ok:true,sessionCheck}),response({schema:'soty.source-session-continuation.v1',ready:true})];
  for(const[page,framed]of[[renderBootPage({shellUrl}),true],[renderStatusPage({shellUrl}),true],
    [renderBootPage({shellUrl,scoped:true}),false],[renderBootPage({shellUrl,scoped:true,renewal:true,parentOrigin:'https://soty.example'}),true]]){
    const env=browser(page,{framed,responses:responses()});phaseWatch(env);await flush();assert.equal(diagnosticMessages(env).length,0);
  }
  const fast=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:responses()});await flush();phaseWatch(fast);
  assert.equal(fast.navigations.length,1);assert.equal(diagnosticMessages(fast).length,0,'missing hint is not proof that boot did not run');
});

test('diagnosed scoped timeout still aborts one existing request and shows its failure without error payload or retry',async()=>{
  const env=browser(renderBootPage({shellUrl,scoped:true}),{framed:true,responses:[(_url,options)=>new Promise((_resolve,reject)=>options.signal.addEventListener('abort',()=>reject(Error('PRIVATE_TIMEOUT_ERROR'))))]});
  phaseWatch(env);const timeout=[...env.timers.values()].find(x=>x.delay===10000);assert.ok(timeout);timeout.callback();await flush();
  assert.equal(env.calls.length,1);assert.equal(env.calls[0].options.signal.aborted,true);assert.equal(env.navigations.length,0);assert.equal(diagnosticPhases(env).at(-1),'boot_failed');
  assert.equal(env.element('status-title').textContent,'Не удалось подключиться');assert.equal(JSON.stringify(diagnosticMessages(env)).includes('PRIVATE_TIMEOUT_ERROR'),false);
  assert.deepEqual(env.forbidden,{storageReads:0,telemetryCalls:0});env.emit('pagehide');
});

test('renderers use shared graphite palette and regular rounded hex geometry, with narrow and reduced-motion styles', () => {
  const page = renderBootPage({ shellUrl, nonce });
  assert.match(page, /^<!doctype html><html lang="ru">/u);
  assert.ok(page.includes(`--sw-bg:${createPalette('dark', 50)['--sw-bg']}`));
  assert.ok(page.includes(roundedHexPath(10, undefined, { x: 11, y: 10 })));
  assert.match(page, /viewBox="0 0 [0-9.]+ [0-9.]+"/u);
  assert.match(page, /height:auto/u); assert.match(page, /prefers-reduced-motion:reduce/u);
  assert.match(page, /max-width:359px/u); assert.match(page, /max-height:440px/u);
  assert.match(page, /@media\(max-height:280px\) and \(min-width:480px\)/u);
  assert.match(page, /body\[data-framed="true"\] \.brand\{display:none\}/u);
  assert.match(page, /min-height:44px/u);
  assert.match(page, /:focus-visible/u); assert.match(page, /min-height:46px/u);
  assert.match(page, /role="status" aria-live="polite"/u);
  assert.equal((page.match(/<script\b/gu) || []).length, 1); assert.ok(page.includes(`<script nonce="${nonce}">`));
  assert.doesNotMatch(page, /<script[^>]+src=|<link[^>]+href=|<img[^>]+src=/u);
});

test('only the recognized cookie-check failure sends one nonce-bound non-authoritative retry hint, before or after watch handshake', async () => {
  const appId='app-'+ 'a'.repeat(32), watch={schema:'soty.app-boot-watch.v1',appId,nonce:'n'.repeat(43)};
  for(const early of [true,false]){
    const env=browser(renderBootPage({shellUrl}),{framed:true,responses:[response({ok:true,entryPath:'/',sessionCheck}),response({ok:false,error:'app_session_check_failed'},false)]});
    const handshake=()=>env.emit('message',{data:watch,origin:'https://soty.example',source:env.parent,ports:[]});
    if(early)handshake();await flush();if(!early)handshake();await flush();
    assert.deepEqual(env.parentMessages,[{origin:'https://soty.example',data:{schema:'soty.app-boot-failure.v1',appId,nonce:watch.nonce,error:'app_session_check_failed'}}]);
    handshake();assert.equal(env.parentMessages.length,1);assert.equal(env.calls.length,2);assert.deepEqual(env.navigations,[]);
  }
});

test('wrong parent/origin/nonce/selectors and every other admission/check error cannot cause automatic recovery hints', async () => {
  const watch={schema:'soty.app-boot-watch.v1',appId:'app-'+ 'a'.repeat(32),nonce:'n'.repeat(43)};
  const env=browser(renderBootPage({shellUrl}),{framed:true,responses:[response({ok:true,entryPath:'/',sessionCheck}),response({ok:false,error:'app_session_check_failed'},false)]});await flush();
  for(const event of [{data:watch,origin:'https://evil.example',source:env.parent},
    {data:watch,origin:'https://soty.example',source:{}},{data:{...watch,accountId:'caller'},origin:'https://soty.example',source:env.parent},
    {data:{...watch,nonce:'bad'},origin:'https://soty.example',source:env.parent}])env.emit('message',event);
  assert.equal(env.parentMessages.length,0);
  for(const error of ['apps_access_denied','app_access_changed','app_access_expired','app_ticket_invalid','private_internal_error']){
    const child=browser(renderBootPage({shellUrl}),{framed:true,responses:[response({ok:true,entryPath:'/',sessionCheck}),response({ok:false,error},false)]});
    child.emit('message',{data:watch,origin:'https://soty.example',source:child.parent,ports:[]});await flush();assert.equal(child.parentMessages.length,0);
  }
});

test('pagehide/history restoration cannot revive a failure hint or reuse its cleared ticket', async()=>{
  const env=browser(renderBootPage({shellUrl}),{framed:true,responses:[response({ok:true,entryPath:'/',sessionCheck}),response({ok:false,error:'app_session_check_failed'},false)]});await flush();env.emit('pagehide');env.emit('pageshow',{persisted:true});
  env.emit('message',{data:{schema:'soty.app-boot-watch.v1',appId:'app-'+ 'a'.repeat(32),nonce:'n'.repeat(43)},origin:'https://soty.example',source:env.parent,ports:[]});assert.equal(env.parentMessages.length,0);assert.equal(env.calls.length,2);
});

test('parent origins are the existing exact embedding allowlist, with no wildcard or arbitrary callback URL',()=>{
  for(const parentOrigins of [[],['*'],['https://soty.example/path'],['https://user:pass@soty.example']])assert.throws(()=>renderBootPage({shellUrl,parentOrigins}));
  assert.doesNotThrow(()=>renderBootPage({shellUrl,parentOrigins:['https://soty.example','https://retained.example']}));
  assert.doesNotThrow(()=>renderBootPage({shellUrl,parentOrigins:Array.from({length:9},(_,index)=>'https://retained'+index+'.example')}),'existing trusted embedding settings have no eight-origin limit');
});

test('only HTTP(S) shell URLs, local runtime paths and canonical 16-byte nonce are accepted', () => {
  for (const bad of ['javascript:alert(1)', 'data:text/html,evil', '//evil.example', '/relative', 'https://user:pass@soty.example',
    'https:\\evil.example', ' https://soty.example', 'https://soty.example/\npath', 'https://soty.example/a b']) {
    assert.throws(() => renderBootPage({ shellUrl: bad }), /invalid_shell_url/u);
  }
  for (const bad of ['https://evil.example/', '//evil.example/', '/%2f%2fevil.example', '/\\evil', '/\n', '/%00', '/_soty/session', '/x/../_soty/session', '/%5fsoty/session', '/%ZZ',
    '/x/..//double', '/%2F_soty/session', '/x%2f..%2f_soty/session', '/x%2f..%2f%2fdouble', '/x%23/../_soty/session', '/bad%00/../safe']) {
    assert.throws(() => renderStatusPage({ publicResetPath: bad }), /invalid_public_reset_path/u);
  }
  for (const bad of ['', 'x" onload="evil', 'x'.repeat(24), 'A'.repeat(21) + 'B==']) {
    assert.throws(() => renderStatusPage({ nonce: bad }), /invalid_page_nonce/u);
  }
  assert.doesNotThrow(() => renderBootPage({ shellUrl: 'http://localhost:5300/#app-entry', publicResetPath: '/search?q=%23one&sort=two', nonce }));
  assert.doesNotThrow(() => renderBootPage({ shellUrl, publicResetPath: '/#/dashboard', nonce }));
});

test('script/HTML injection and prototype-key errors stay data, with no owner or raw technical error displayed', async () => {
  const malicious = '</script><script>globalThis.pwned=true</script>"&\u2028';
  for (const error of [malicious, '__proto__', 'constructor', 'toString', { code: malicious, owner: 'private-owner' }]) {
    const page = renderStatusPage({ error, shellUrl: `https://soty.example/#next=${malicious}`, publicResetPath: '/search?q=</script><svg/onload=evil>&quote="', nonce });
    assert.equal((page.match(/<script\b/gu) || []).length, 1);
    assert.equal((page.match(/<\/script>/gu) || []).length, 1);
    assert.doesNotMatch(page, /private-owner|<svg\/onload|<script>globalThis/u);
    const env = browser(page); await flush();
    assert.equal(env.context.pwned, undefined); assert.equal(env.element('status-title').textContent, 'Приложение недоступно');
    assert.equal(env.calls.length, 0);
  }
});

test('boot clears fragment before its only POST and proves the exact newly issued cookie before navigation', async () => {
  const env = browser(renderBootPage({ shellUrl, nonce }), { responses: [
    response({ ok: true, entryPath: '/project?tab=notes&tag=%23one#/dashboard', sessionCheck }), response({ ok: true, sessionCheck }),
  ] });
  assert.deepEqual(env.events.slice(0, 2), [['cleanup', '/_soty/boot?path=%2Funtrusted-query'], ['fetch', 'POST']]);
  assert.equal(env.context.location.hash, '');
  await flush();
  assert.deepEqual(env.calls.map(call => call.options.method), ['POST', 'GET']);
  assert.deepEqual(JSON.parse(env.calls[0].options.body), { ticket });
  assert.equal(env.calls[1].options.headers['X-Soty-Boot-Check'], sessionCheck);
  assert.equal(env.calls[1].options.body, undefined);
  for (const call of env.calls) {
    assert.equal(call.url, '/_soty/session'); assert.equal(call.options.credentials, 'same-origin');
    assert.equal(call.options.cache, 'no-store'); assert.equal(call.options.redirect, 'error'); assert.ok(call.options.signal);
  }
  assert.deepEqual(env.navigations, ['/project?tab=notes&tag=%23one#/dashboard']); assert.equal(env.timers.size, 0);
  assert.equal(env.element('status-title').textContent, 'Открываем приложение');
});

test('successful POST alone, bare GET and a different session check never count as confirmed entry', async () => {
  for (const checked of [{ ok: true }, { ok: true, sessionCheck: 'X'.repeat(43) }, { ok: false, error: 'constructor' }]) {
    const env = browser(renderBootPage({ shellUrl }), { responses: [response({ ok: true, entryPath: '/', sessionCheck }), response(checked)] });
    await flush();
    assert.equal(env.navigations.length, 0); assert.equal(env.calls.length, 2);
    assert.equal(env.element('status-title').textContent, 'Вход не подтвердился');
    assert.equal(env.element('shell-action').href, shellUrl); assert.equal(env.element('shell-action').hidden, false);
    await flush(); assert.equal(env.calls.length, 2, 'no automatic POST retry or reload');
  }
  const held = pending();
  const env = browser(renderBootPage({ shellUrl }), { responses: [response({ ok: true, entryPath: '/', sessionCheck }), () => held.promise] });
  await flush(); assert.equal(env.calls.length, 2); assert.equal(env.navigations.length, 0);
  held.resolve(response({ ok: true, sessionCheck })); await flush(); assert.deepEqual(env.navigations, ['/']);
});

test('untrusted successful response cannot navigate to another origin or the reserved control namespace', async () => {
  for (const entryPath of ['https://evil.example/', '//evil.example/', '/%2f%2fevil.example/', '/x/../_soty/session', '/%5fsoty/session', '/\\evil', '/%00',
    '/x/..//double', '/%2F_soty/session', '/x%2f..%2f_soty/session', '/x%2f..%2f%2fdouble', '/x%23/../_soty/session', '/bad%00/../safe']) {
    const env = browser(renderBootPage({ shellUrl }), { responses: [response({ ok: true, entryPath, sessionCheck })] });
    await flush(); assert.equal(env.calls.length, 1); assert.deepEqual(env.navigations, []);
  }
});

test('invalid ticket or failed fragment cleanup makes zero requests and offers only fresh top-level recovery', async () => {
  for (const options of [{ hash: '' }, { hash: '#bad' }, { cleanThrows: true }]) {
    const env = browser(renderBootPage({ shellUrl }), options); await flush();
    assert.equal(env.calls.length, 0); assert.equal(env.navigations.length, 0);
    assert.equal(env.element('status-title').textContent, 'Нужно открыть заново');
    assert.equal(env.element('shell-action').href, shellUrl);
  }
});

test('iframe errors never load shell, navigate top, open a popup or retry a consumed ticket', async () => {
  const env = browser(renderBootPage({ shellUrl }), { framed: true, responses: [response({ ok: false, error: 'app_ticket_invalid' }, false)] });
  await flush();
  assert.equal(env.element('shell-action').hidden, true); assert.equal(env.element('shell-action').href, undefined);
  assert.equal(env.element('frame-hint').hidden, false); assert.equal(env.element('actions').hidden, true);
  assert.deepEqual(env.navigations, []); assert.equal(env.calls.length, 1);
  const page = renderStatusPage({ error: 'app_offline', shellUrl });
  assert.ok(page.includes('В меню ⋯ над окном выберите „Открыть отдельно“.'));
  assert.doesNotMatch(page, /target="_top"|target="_blank"|window\.open|top\.location|location\.reload/u);
});

test('framed recovery identifies its layout without hiding the public action or the outer-toolbar hint', () => {
  const page = renderStatusPage({ error: 'app_access_expired', shellUrl, publicResetPath: '/' });
  const framed = browser(page, { framed: true });
  assert.equal(framed.context.document.body.dataset.framed, 'true');
  assert.equal(framed.element('actions').hidden, false);
  assert.equal(framed.element('reset-action').hidden, false);
  assert.equal(framed.element('reset-action').disabled, false);
  assert.equal(framed.element('frame-hint').hidden, false);
  assert.equal(framed.element('shell-action').href, undefined);
  assert.equal(framed.calls.length, 0);
  const separate = browser(page);
  assert.equal(separate.context.document.body.dataset.framed, 'false');
  assert.equal(separate.element('shell-action').href, shellUrl);
  assert.equal(separate.element('frame-hint').hidden, true);
});

test('changed access with permitted public recovery is not presented as a permanent denial', async () => {
  const page = renderStatusPage({ error: 'app_access_changed', shellUrl, publicResetPath: '/' });
  assert.match(page, /<h1 id="status-title">Доступ изменился<\/h1>/u);
  const status = browser(page, { framed: true });
  assert.equal(status.element('status-title').textContent, 'Доступ изменился');
  assert.equal(status.element('status-detail').textContent, 'Откройте приложение заново.');
  assert.equal(status.element('reset-action').hidden, false);
  for (const publicResetPath of ['/', undefined]) {
    const env = browser(renderBootPage({ shellUrl, publicResetPath }), { framed: true, responses: [
      response({ ok: true, entryPath: '/', sessionCheck }), response({ ok: false, error: 'app_access_changed' }, false),
    ] });
    await flush();
    assert.equal(env.element('status-title').textContent, publicResetPath ? 'Доступ изменился' : 'Доступ закрыт');
    assert.equal(env.element('reset-action').hidden, !publicResetPath);
    assert.equal(env.calls.length, 2); assert.deepEqual(env.navigations, []);
  }
});

test('timeout aborts the request without retrying POST or discarding error recovery', async () => {
  const env = browser(renderBootPage({ shellUrl }), { responses: [(_url, options) => new Promise((_resolve, reject) =>
    options.signal.addEventListener('abort', () => reject(new Error('timeout'))))] });
  assert.equal(env.timers.size, 1); assert.equal([...env.timers.values()][0].delay, 10_000);
  env.expire(); await flush();
  assert.equal(env.calls.length, 1); assert.equal(env.calls[0].options.signal.aborted, true); assert.equal(env.timers.size, 0);
  assert.equal(env.element('status-title').textContent, 'Не удалось подключиться'); assert.equal(env.navigations.length, 0);
});

test('public reset requires explicit action, one DELETE acknowledgement and one local navigation, even on double click', async () => {
  const held = pending();
  const env = browser(renderStatusPage({ error: 'app_access_expired', shellUrl, publicResetPath: '/project?q=one#/dashboard' }),
    { framed: true, responses: [() => held.promise] });
  assert.equal(env.calls.length, 0); assert.equal(env.element('reset-action').hidden, false);
  const first = env.click('reset-action'), second = env.click('reset-action');
  assert.equal(env.calls.length, 1); assert.equal(env.calls[0].options.method, 'DELETE');
  assert.equal(env.calls[0].options.headers.Origin, undefined, 'the browser supplies its actual origin');
  assert.equal(env.element('reset-action').disabled, true); assert.equal(env.element('reset-action').hidden, false);
  assert.deepEqual(env.navigations, []);
  held.resolve(response({ ok: true })); await Promise.all([first, second]);
  assert.deepEqual(env.navigations, ['/project?q=one#/dashboard']);
  await env.click('reset-action'); assert.equal(env.calls.length, 1);
});

test('failed public reset stays on a recoverable page and never clears session automatically on an error', async () => {
  const env = browser(renderStatusPage({ error: 'apps_access_denied', shellUrl, publicResetPath: '/' }),
    { responses: [response({ ok: false, error: 'apps_access_denied' }, false), response({ ok: true })] });
  assert.equal(env.calls.length, 0);
  await env.click('reset-action'); assert.equal(env.calls.length, 1); assert.deepEqual(env.navigations, []);
  assert.equal(env.element('status-title').textContent, 'Доступ закрыт'); assert.equal(env.element('reset-action').disabled, false);
  await env.click('reset-action'); assert.equal(env.calls.length, 2); assert.deepEqual(env.navigations, ['/']);
  const privatePage = browser(renderStatusPage({ error: 'apps_access_denied', shellUrl }));
  await privatePage.click('reset-action'); assert.equal(privatePage.calls.length, 0); assert.equal(privatePage.element('reset-action').hidden, true);
});

test('leaving or restoring the page cannot complete a stale request or reuse a ticket from browser history', async () => {
  const late = pending(), reset = pending();
  const env = browser(renderBootPage({ shellUrl, publicResetPath: '/' }), { responses: [() => late.promise, () => reset.promise] });
  env.emit('pagehide'); assert.equal(env.calls[0].options.signal.aborted, true);
  env.emit('pageshow', { persisted: true });
  assert.equal(env.element('status-title').textContent, 'Нужно открыть заново');
  const firstReset = env.click('reset-action');
  late.resolve(response({ ok: true, entryPath: '/stale', sessionCheck })); await flush();
  await env.click('reset-action'); assert.equal(env.calls.length, 2, 'late boot must not clear the new action lock');
  assert.deepEqual(env.navigations, []);
  reset.resolve(response({ ok: true })); await firstReset;
  assert.deepEqual(env.navigations, ['/']);
});

test('status pages disclose only allowlisted human states for capacity and availability failures', async () => {
  for (const [error, expected] of [[429, 'Сейчас много подключений'], [503, 'Приложение недоступно'], ['app_offline', 'Устройство не в сети'],
    ['app_address_retired', 'Адрес недоступен'], ['totally_internal_failure', 'Приложение недоступно']]) {
    const env = browser(renderStatusPage({ error })); await flush();
    assert.equal(env.element('status-title').textContent, expected); assert.equal(env.calls.length, 0);
    assert.equal(env.element('shell-action').hidden, true); assert.equal(env.element('reset-action').hidden, true);
  }
});

test('an HTTP failure at admission is distinguished from a network failure without exposing its raw code', async () => {
  const env = browser(renderBootPage({ shellUrl }), { responses: [response({ ok: false, error: 'private_database_detail' }, false)] });
  await flush();
  assert.equal(env.element('status-title').textContent, 'Приложение недоступно');
  assert.equal(env.element('status-detail').textContent.includes('private_database_detail'), false);
  assert.equal(env.calls.length, 1); assert.deepEqual(env.navigations, []);
});

test('renewal boot selects only its candidate with the checked marker; initial entry keeps the ordinary cookie route',async()=>{
  for(const renewal of[false,true]){
    const issued={ok:true,entryPath:'/embed',sessionCheck,scopedRequestId:'R'.repeat(43),...(renewal?{renewal:true,renewalRequestId:'synthetic-renewal-request'}:{})};
    const env=browser(renderBootPage({shellUrl,scoped:true,renewal,parentOrigin:'https://soty.example'}),{framed:true,responses:[
      response(issued),response({ok:true,sessionCheck}),response({schema:'soty.source-session-continuation.v1',ready:true})]});
    await flush();await flush();
    assert.equal(env.calls.length,3);assert.equal(env.calls[2].url,'/api/embed/session-continue');
    assert.equal(env.calls[2].options.headers['x-soty-boot-check'],renewal?sessionCheck:undefined);
    assert.deepEqual(JSON.parse(env.calls[2].options.body),{requestId:issued.scopedRequestId});
    assert.equal(env.calls[2].options.credentials,'same-origin');
    assert.equal(env.parentMessages.length,renewal?1:0);assert.deepEqual(env.navigations,renewal?[]:['/embed']);
  }
});

test('only explicit verified ready:false is login_required; failed/invalid continuation remains unknown without POST retry',async()=>{
  for(const next of[response({schema:'soty.source-session-continuation.v1',ready:false,reason:'login_required'}),
    response({error:'app_scoped_candidate_changed'},false),response({schema:'soty.source-session-continuation.v1',ready:'caller'})]){
    const env=browser(renderBootPage({shellUrl,scoped:true,renewal:true,parentOrigin:'https://soty.example'}),{framed:true,responses:[
      response({ok:true,entryPath:'/embed',sessionCheck,scopedRequestId:'R'.repeat(43),renewal:true,renewalRequestId:'synthetic-renewal-request'}),response({ok:true,sessionCheck}),next]});
    await flush();await flush();assert.equal(env.calls.length,3);assert.deepEqual(env.navigations,[]);
    assert.equal(env.parentMessages.length,1);const reply=env.parentMessages[0].data;assert.equal(reply.ready,false);
    assert.equal(reply.unknown===true,!next.ok||(await next.json()).ready==='caller');
  }
});

test('workspace frame selector reaches cookie-check, continuation and entry; hidden renewal never navigates the visible frame',async()=>{
  for(const renewal of [false,true]){
    const frame='F'.repeat(43),issued={ok:true,entryPath:'/embed',sessionCheck,frame,scopedRequestId:'R'.repeat(43),
      ...(renewal?{renewal:true,renewalRequestId:'synthetic-renewal-request'}:{})};
    const env=browser(renderBootPage({shellUrl,scoped:true,renewal,parentOrigin:'https://soty.example'}),{framed:true,responses:[
      response(issued),response({ok:true,sessionCheck}),response({schema:'soty.source-session-continuation.v1',ready:true})]});
    await flush();await flush();
    assert.equal(env.calls[0].options.headers['X-Soty-Frame'],undefined,'ticket supplies the initial private authority');
    assert.equal(env.calls[1].options.headers['X-Soty-Frame'],frame);
    assert.equal(env.calls[2].options.headers['x-soty-frame'],frame);
    assert.deepEqual(JSON.parse(env.calls[2].options.body),{requestId:issued.scopedRequestId},'business/auth DTO is unchanged');
    assert.deepEqual(env.navigations,renewal?[]:['/embed?sotyFrame='+frame]);
    assert.equal(env.parentMessages.length,renewal?1:0);
  }
  const env=browser(renderBootPage({shellUrl,scoped:true}),{responses:[response({ok:true,entryPath:'/embed',sessionCheck,frame:'invalid'})]});
  await flush();assert.equal(env.calls.length,1);assert.deepEqual(env.navigations,[]);
});
