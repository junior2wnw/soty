import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {renderBootPage,renderStatusPage} from '../server/runtime-pages.mjs';

const canonicalCss=readFileSync(new URL('../../../vendor/optical-glass/src/ui-kit.css',import.meta.url),'utf8');
const shellUrl='https://root.example/#mine',ticket='T'.repeat(43),check='C'.repeat(43),frame='F'.repeat(43),requestId='R'.repeat(43);
const nonce=Buffer.from('glass-test-nonce').toString('base64');
const response=(value,ok=true)=>({ok,status:ok?200:403,json:async()=>value,arrayBuffer:async()=>new TextEncoder().encode(JSON.stringify(value)).buffer});
const pending=()=>{let resolve;const promise=new Promise(value=>{resolve=value;});return{promise,resolve};};
const flush=()=>new Promise(resolve=>setImmediate(resolve));
function markupOracle(markup){
 assert.equal(markup.split(canonicalCss).length,2,'exact canonical Glass CSS must be inlined once');
 const body=markup.match(/<body\b([^>]*)>/u)?.[1]??'';
 for(const value of ['class="glass-ui"','data-glass-material="working"','data-glass-render-mode="static"','data-glass-tone="light"','data-glass-role="panel"'])assert.ok(body.includes(value),`outer body needs ${value}`);
 assert.doesNotMatch(markup.match(/<main\b[^>]*>/u)?.[0]??'',/data-glass-role/u,'content must not create a second decorative material');
 assert.match(markup,/<meta name="color-scheme" content="light dark">/u);
 assert.doesNotMatch(markup,/<link\b|<script[^>]+src=|<img[^>]+src=/u,'managed pages need no extra asset requests');
 assert.equal((markup.match(/<script\b/gu)??[]).length,1);
}

// Actual serialized page scripts and their DOM/media events execute here.
// The media port models the embedding document's preference. It does NOT
// prove real cross-origin CSS inheritance, physical44px or compositor paint.
function browser(markup,{framed=true,dark=false,responses=[],documentState='complete'}={}){
 const calls=[],events=[],elements=new Map(),listeners=new Map(),mediaListeners=new Set(),timers=new Map(),navigations=[];let id=0;
 const media={matches:dark,addEventListener(type,fn){assert.equal(type,'change');mediaListeners.add(fn);},removeEventListener(type,fn){assert.equal(type,'change');mediaListeners.delete(fn);}};
 const element=id=>{if(!elements.has(id))elements.set(id,{hidden:true,disabled:false,textContent:'',dataset:{},attrs:new Map(),handlers:new Map(),
  setAttribute(k,v){this.attrs.set(k,v);},removeAttribute(k){this.attrs.delete(k);},addEventListener(k,v){this.handlers.set(k,v);},
  get href(){return this.attrs.get('href');},set href(v){this.attrs.set('href',v);}});return elements.get(id);};
 const body=element('body');for(const value of markup.match(/<body\b([^>]*)>/u)?.[1].matchAll(/\bdata-([a-z-]+)="([^"]*)"/gu)??[]){
  const name=value[1].replace(/-([a-z])/gu,(_,letter)=>letter.toUpperCase());body.dataset[name]=value[2];
 }
 body.dataset=new Proxy(body.dataset,{set(target,name,value){events.push(['body',name,value]);target[name]=value;return true;}});
 const location={hash:'#'+ticket,pathname:'/_soty/boot',search:'?path=%2F',replace(path){navigations.push(path);}};
 const window={addEventListener(type,fn){if(!listeners.has(type))listeners.set(type,new Set());listeners.get(type).add(fn);},removeEventListener(type,fn){listeners.get(type)?.delete(fn);},
  matchMedia(query){assert.equal(query,'(prefers-color-scheme: dark)');events.push(['media']);return media;}};
 window.self=window;window.top=framed?{}:window;window.parent=framed?{postMessage(){}}:window;
 const context={window,location,document:{body,title:'',readyState:documentState,getElementById:element},
  history:{replaceState(){events.push(['cleanup']);location.hash='';}},URL,AbortController,AbortSignal,TextDecoder,performance:{now:()=>0},
  setTimeout(fn,ms){timers.set(++id,{fn,ms});return id;},clearTimeout(id){timers.delete(id);},
  fetch(url,options){events.push(['fetch',options.method]);calls.push({url,options});const item=responses.shift();if(typeof item==='function')return item();return item instanceof Error?Promise.reject(item):Promise.resolve(item??response({ok:false,error:'app_offline'},false));},
 };
 const scripts=[...markup.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gu)];assert.equal(scripts.length,1);
 vm.runInNewContext(scripts[0][1],context,{timeout:1000});
 return{body,element,calls,events,navigations,timers,mediaListeners,context,
  changeTheme(value){media.matches=value;for(const listener of [...mediaListeners])listener({matches:value});},
  emit(type,event={}){for(const listener of [...(listeners.get(type)??[])])listener(event);}};
}

test('boot and status use one canonical static working material without external styles',()=>{
 for(const page of[renderBootPage({shellUrl,nonce}),renderStatusPage({shellUrl,error:'apps_access_denied',nonce})])markupOracle(page);
});
for(const dark of[false,true])test(`managed presentation follows its current embedded media preference (${dark?'dark':'light'}) and retires its listener`,()=>{
 const held=pending(),env=browser(renderBootPage({shellUrl}),{dark,responses:[()=>held.promise]});
 assert.equal(env.body.dataset.glassTone,dark?'dark':'light');assert.equal(env.mediaListeners.size,1);
 env.changeTheme(!dark);assert.equal(env.body.dataset.glassTone,dark?'light':'dark');
 const tone=env.body.dataset.glassTone;env.emit('pagehide');assert.equal(env.mediaListeners.size,0);env.changeTheme(dark);assert.equal(env.body.dataset.glassTone,tone);
 assert.equal(env.calls.length,1,'theme changes never create an authorization/application request');
});
test('presentation starts after original hash cleanup and never adds network work to ordinary admission',async()=>{
 const env=browser(renderBootPage({shellUrl}),{responses:[response({ok:true,entryPath:'/',sessionCheck:check}),response({ok:true,sessionCheck:check})]});await flush();
 assert.equal(env.events[0][0],'cleanup');assert.ok(env.events.findIndex(value=>value[0]==='media')>0);
 assert.deepEqual(env.calls.map(value=>[value.url,value.options.method]),[['/_soty/session','POST'],['/_soty/session','GET']]);
 assert.deepEqual(env.navigations,['/']);assert.equal(env.context.location.hash,'');
 assert.equal(env.element('status-title').textContent,'Открываем приложение');assert.equal(env.element('status-detail').textContent,'');
 env.emit('pagehide');
});
test('loading/checking/opening keep one quiet line while the real scoped native continuation stays usable',async()=>{
 const admit=pending(),cookie=pending(),env=browser(renderBootPage({shellUrl,scoped:true}),{documentState:'loading',responses:[()=>admit.promise,()=>cookie.promise,response({schema:'soty.source-session-continuation.v1',ready:true})]});
 const quiet=()=>{assert.equal(env.element('status-title').textContent,'Открываем приложение');assert.equal(env.element('status-detail').textContent,'');};quiet();
 admit.resolve(response({ok:true,entryPath:'/embed',sessionCheck:check,frame,scopedRequestId:requestId}));await flush();quiet();
 cookie.resolve(response({ok:true,sessionCheck:check}));await flush();quiet();
 assert.equal(env.element('native-action').hidden,false);assert.equal(env.element('actions').hidden,false);
 assert.equal(env.element('native-action').href,'/embed?sotyFrame='+frame);assert.deepEqual(env.navigations,[]);
 env.emit('load');await flush();assert.deepEqual(env.navigations,['/embed?sotyFrame='+frame]);env.emit('pagehide');
});
test('error causes and both top-level recovery actions remain intact and material roles stay native',()=>{
 const markup=renderStatusPage({shellUrl,error:'apps_access_denied',publicResetPath:'/public'}),env=browser(markup,{framed:false,dark:true});
 assert.equal(env.element('status-title').textContent,'Доступ закрыт');assert.ok(env.element('status-detail').textContent.length>0);
 assert.equal(env.element('shell-action').hidden,false);assert.equal(env.element('shell-action').href,shellUrl);
 assert.equal(env.element('reset-action').hidden,false);assert.equal(env.element('reset-action').disabled,false);
 assert.match(markup,/<a\b[^>]*data-glass-role="accent"[^>]*id="shell-action"/u);
 assert.match(markup,/<button\b[^>]*data-glass-role="control"[^>]*id="reset-action"/u);env.emit('pagehide');
});
test('canonical forced-colors and reduced-motion policies are present and an omitted shared material is detected',()=>{
 const markup=renderBootPage({shellUrl});assert.ok(canonicalCss.includes('@media (forced-colors: active)'));assert.ok(canonicalCss.includes('@media (prefers-reduced-motion: reduce)'));
 assert.throws(()=>markupOracle(markup.replace(canonicalCss,'')),/exact canonical Glass CSS/u);
});
test('BFCache re-entry refreshes presentation only and a second retirement removes its media listener again',()=>{
 const held=pending(),env=browser(renderBootPage({shellUrl}),{dark:false,responses:[()=>held.promise]});
 env.emit('pagehide');env.changeTheme(true);assert.equal(env.body.dataset.glassTone,'light');
 env.emit('pageshow',{persisted:true});assert.equal(env.body.dataset.glassTone,'dark');assert.equal(env.mediaListeners.size,1);
 env.changeTheme(false);assert.equal(env.body.dataset.glassTone,'light');env.emit('pagehide');assert.equal(env.mediaListeners.size,0);
 assert.equal(env.calls.length,1,'theme restoration cannot reopen the expired boot');assert.deepEqual(env.navigations,[]);
});
test('negative control: omitting presentation initialization is detected without changing admission or hash cleanup',()=>{
 const markup=renderBootPage({shellUrl}),start=markup.lastIndexOf('(function initializeMaterialTheme()');assert.ok(start>0);
 const end=markup.indexOf('</script>',start),broken=markup.slice(0,start)+markup.slice(end);assert.notEqual(broken,markup);
 const held=pending(),env=browser(broken,{dark:true,responses:[()=>held.promise]});
 assert.throws(()=>assert.equal(env.body.dataset.glassTone,'dark','dark embedding must select dark material'),/dark embedding must select dark material/u);
 assert.equal(env.events[0][0],'cleanup');assert.equal(env.calls.length,1);env.emit('pagehide');
});
