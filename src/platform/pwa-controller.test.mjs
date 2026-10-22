import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {setImmediate} from 'node:timers/promises';
import vm from 'node:vm';
import {MessageChannel} from 'node:worker_threads';
import ts from 'typescript';

// Execute the actual page controller; browser install events and promises are
// controllable ports. No browser, install, service worker or network is started.
const source=readFileSync(new URL('./pwa.ts',import.meta.url),'utf8').replace('import.meta.env.DEV','false');
const compiled=ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
const workerSource=readFileSync(new URL('../../public/sw.js',import.meta.url),'utf8');
const pending=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return{promise,resolve,reject};};
function fixture({standalone=false}={}) {
  const window=new EventTarget(),document=new EventTarget(),display=new EventTarget(),timers=new Map(),exports={};
  let timer=0,prompts=0,reloads=0;
  display.matches=standalone;document.visibilityState='visible';
  const context={exports,window,document,AbortController,AbortSignal,Event,URL,matchMedia:()=>display,
    navigator:{onLine:true},location:{origin:'https://soty.example',reload(){reloads++;}},
    fetch:async()=>({ok:true,json:async()=>({ok:true})}),
    setInterval:(fn,ms)=>{const id=++timer;timers.set(id,{fn,ms});return id;},clearInterval:id=>timers.delete(id)};
  vm.runInNewContext(compiled,context,{timeout:1000});
  const control=exports.createPwaController();
  return {control,window,display,context,timers,reloads:()=>reloads,prompts:()=>prompts,
    offer({choice=pending(),prompt=pending()}={}){
      const event=new Event('beforeinstallprompt',{cancelable:true});
      Object.assign(event,{prompt(){prompts++;return prompt.promise;},userChoice:choice.promise});window.dispatchEvent(event);
      return {choice,prompt,event};
    },installed:()=>window.dispatchEvent(new Event('appinstalled')),
    standalone(){display.matches=true;display.dispatchEvent(new Event('change'));}};
}

function updateFixture(t,{lostActivationAck=false}={}) {
  const window=new EventTarget(),document=new EventTarget(),display=new EventTarget(),workers=new EventTarget(),exports={};
  const activationTimers=new Set(),workerHandlers=new Map(),protocol={requests:0,commits:0,activations:0,droppedAcks:0};
  document.visibilityState='visible';display.matches=false;let reloads=0,onActivate,activationWork,beforeActivation,dropCommit=false,foreignController=false;
  class ServiceWorker extends EventTarget {
    scriptURL='https://soty.example/sw.js';
    postMessage(message,[port]) {
      if(message.type==='SOTY_ACTIVATE_UPDATE'&&lostActivationAck){
        protocol.requests++;
        workerHandlers.get('message')({data:message,ports:[{postMessage(){protocol.droppedAcks++;port.close();}}],waitUntil(work){activationWork=work;}});return;
      }
      if (message.type==='SOTY_ACTIVATE_UPDATE'&&onActivate) { onActivate(port);return; }
      port.postMessage(message.type==='SOTY_OFFLINE_STATUS'?{offlineReady:true}:{ready:true});port.close();
    }
  }
  const previous=new ServiceWorker(),next=new ServiceWorker(),foreign=new ServiceWorker(),registration=new EventTarget();
  foreign.scriptURL='https://soty.example/other-worker.js';
  registration.waiting=null;registration.update=async()=>{};
  workers.controller=previous;workers.register=async()=>registration;workers.ready=Promise.resolve(registration);
  const commit=(source=next)=>{const event=new Event('message');Object.assign(event,{source,data:{type:'SOTY_UPDATE_COMMIT'},ports:[]});workers.dispatchEvent(event);};
  const activate=()=>{registration.waiting=null;workers.controller=foreignController?foreign:next;workers.dispatchEvent(new Event('controllerchange'));};
  if(lostActivationAck)vm.runInNewContext(workerSource,{URL,MessageChannel,setTimeout,clearTimeout,self:{
    location:{origin:'https://soty.example'},addEventListener:(type,handler)=>workerHandlers.set(type,handler),
    clients:{matchAll:async()=>[{id:'page',url:'https://soty.example/',postMessage(data,ports=[]){
      if(data.type==='SOTY_UPDATE_COMMIT'){protocol.commits++;if(dropCommit)return;}
      const event=new Event('message');Object.assign(event,{source:next,data,ports});workers.dispatchEvent(event);
    }}]},skipWaiting:async()=>{protocol.activations++;beforeActivation?.();activate();},
  }},{timeout:1000});
  const context={exports,window,document,ServiceWorker,MessageChannel,AbortController,AbortSignal,Event,URL,
    matchMedia:()=>display,navigator:{serviceWorker:workers,onLine:true},
    location:{origin:'https://soty.example',reload(){reloads++;}},fetch:async()=>({ok:true,json:async()=>({ok:true})}),
    setTimeout(fn,ms){if(lostActivationAck&&ms===20_000){const timer={fn};activationTimers.add(timer);return timer;}return setTimeout(fn,ms);},
    clearTimeout(timer){if(!activationTimers.delete(timer))clearTimeout(timer);},setInterval:()=>1,clearInterval(){}};
  vm.runInNewContext(compiled,context,{timeout:1000});const control=exports.createPwaController();t.after(()=>control.destroy());
  return {control,exports,context,protocol,foreignWorker:foreign,reloads:()=>reloads,
    offerUpdate(handler){registration.waiting=next;onActivate=handler;},
    activate,commit,beforeActivation(handler){beforeActivation=handler;},suppressCommit(){dropCommit=true;},useForeignController(){foreignController=true;},replaceWaiting(){registration.waiting=foreign;},
    async workerFinished(){for(let i=0;!activationWork&&i<10;i++)await setImmediate();assert.ok(activationWork);await activationWork;await setImmediate();},
    expireActivationAck(){assert.equal(activationTimers.size,1);const [timer]=activationTimers;activationTimers.delete(timer);timer.fn();}};
}

function formFixture(f) {
  class HTMLElement {isConnected=true;ignored=false;closest(){return this.ignored?this:null;}contains(input){return input===this;}}
  class HTMLInputElement extends HTMLElement {type='text';value='';}
  class HTMLTextAreaElement extends HTMLElement {value='';}
  Object.assign(f.context,{HTMLElement,HTMLInputElement,HTMLTextAreaElement});
  const root=new EventTarget(),input=new HTMLInputElement(),watch=f.context.exports.watchFormEdits(root);
  const fire=(type,target=input)=>{const event=new Event(type);Object.defineProperty(event,'target',{value:target});root.dispatchEvent(event);};
  return {input,watch,fire,newInput:()=>new HTMLInputElement(),edit(value,target=input){fire('focusin',target);fire('beforeinput',target);target.value=value;fire('input',target);}};
}

test('a lost activation ACK cannot report failure after the committed worker already caused one reload',async t=>{
  const f=updateFixture(t,{lostActivationAck:true});await setImmediate();f.offerUpdate();const first=f.control.applyUpdate();
  assert.equal(f.control.applyUpdate(),first);await f.workerFinished();
  assert.deepEqual(f.protocol,{requests:1,commits:1,activations:1,droppedAcks:1});assert.equal(f.reloads(),1);
  f.expireActivationAck();assert.equal(await first,true);assert.equal(f.control.get().update,'idle');
  f.commit();f.activate();await setImmediate();assert.equal(f.reloads(),1);assert.equal(f.protocol.requests,1);
});

test('a lost activation ACK preserves the late edit block and retry only reloads the committed worker',async t=>{
  const f=updateFixture(t,{lostActivationAck:true}),form=formFixture(f);t.after(()=>form.watch.destroy());
  f.exports.registerUpdateGuard(()=>!form.watch.hasUnsavedChanges());await setImmediate();
  f.beforeActivation(()=>form.edit('Synthetic unsaved late edit'));f.offerUpdate();const first=f.control.applyUpdate();await f.workerFinished();
  assert.equal(f.control.get().update,'blocked');assert.equal(f.reloads(),0);
  f.expireActivationAck();assert.equal(await first,false);assert.equal(f.control.get().update,'blocked');
  assert.equal(await f.control.applyUpdate(),false);assert.equal(f.reloads(),0);
  form.watch.markSaved();assert.equal(await f.control.applyUpdate(),true);assert.equal(f.reloads(),1);assert.equal(f.protocol.requests,1);
});

test('watchFormEdits tracks user edits, reverts and explicit saved values without treating prefill as an edit',t=>{
  const f=fixture(),form=formFixture(f);t.after(()=>{form.watch.destroy();f.control.destroy();});
  form.input.value='Synthetic prefill';assert.equal(form.watch.hasUnsavedChanges(),false);
  form.edit('Synthetic edit');assert.equal(form.watch.hasUnsavedChanges(),true);
  form.edit('Synthetic prefill');assert.equal(form.watch.hasUnsavedChanges(),false);
  form.edit('Synthetic saved');form.watch.markSaved(form.input);assert.equal(form.watch.hasUnsavedChanges(),false);
  form.edit('Synthetic later edit');assert.equal(form.watch.hasUnsavedChanges(),true);
});

test('watchFormEdits excludes ignored and disconnected fields while retaining other unsaved edits',t=>{
  const f=fixture(),form=formFixture(f);t.after(()=>{form.watch.destroy();f.control.destroy();});
  const ignored=form.newInput();ignored.ignored=true;form.edit('Synthetic ignored edit',ignored);assert.equal(form.watch.hasUnsavedChanges(),false);
  form.edit('Synthetic unsaved edit');assert.equal(form.watch.hasUnsavedChanges(),true);
  form.input.isConnected=false;assert.equal(form.watch.hasUnsavedChanges(),false);
  const current=form.newInput();form.edit('Synthetic current edit',current);assert.equal(form.watch.hasUnsavedChanges(),true);
  form.watch.destroy();assert.equal(form.watch.hasUnsavedChanges(),false);
});

test('timeout without a delivered commit never reloads even when that worker becomes controller',async t=>{
  const f=updateFixture(t,{lostActivationAck:true});await setImmediate();f.suppressCommit();f.offerUpdate();
  const first=f.control.applyUpdate();await f.workerFinished();assert.equal(f.reloads(),0);f.expireActivationAck();
  assert.equal(await first,false);assert.equal(f.control.get().update,'failed');assert.equal(f.reloads(),0);
});

test('the committed update never reloads a different controlling worker after ACK loss',async t=>{
  const f=updateFixture(t,{lostActivationAck:true});await setImmediate();f.useForeignController();f.offerUpdate();
  const first=f.control.applyUpdate();await f.workerFinished();f.expireActivationAck();
  assert.equal(await first,false);assert.equal(f.reloads(),0);assert.equal(f.protocol.requests,1);
});

test('destroyed page cannot navigate or publish when an activation ACK times out',async t=>{
  const f=updateFixture(t,{lostActivationAck:true}),save=pending();let phase=0;f.exports.registerUpdateGuard(()=>++phase<3?true:save.promise);await setImmediate();
  f.offerUpdate();const first=f.control.applyUpdate();await f.workerFinished();assert.equal(f.reloads(),0);
  f.control.destroy();const before=f.control.get().update;save.resolve(true);f.expireActivationAck();
  assert.equal(await first,false);await setImmediate();assert.equal(f.reloads(),0);assert.equal(f.control.get().update,before);
});

for(const source of ['non-worker','other-origin','other-worker'])test(`a ${source} commit cannot authorize the clicked update after ACK loss`,async t=>{
  const f=updateFixture(t,{lostActivationAck:true});await setImmediate();f.suppressCommit();
  if(source==='other-origin')f.foreignWorker.scriptURL='https://other.example/sw.js';
  f.beforeActivation(()=>f.commit(source==='non-worker'?{scriptURL:'https://soty.example/sw.js'}:f.foreignWorker));
  f.offerUpdate();const first=f.control.applyUpdate();await f.workerFinished();f.expireActivationAck();
  assert.equal(await first,false);assert.equal(f.reloads(),0);assert.equal(f.protocol.requests,1);
});

test('ACK timeout joins an outstanding final save instead of deciding before its result',async t=>{
  const f=updateFixture(t,{lostActivationAck:true}),save=pending();let phase=0,settled=false;
  t.after(()=>save.resolve(false));f.exports.registerUpdateGuard(()=>++phase<3?true:save.promise);await setImmediate();
  f.offerUpdate();const first=f.control.applyUpdate();void first.then(()=>{settled=true;});await f.workerFinished();f.expireActivationAck();
  await setImmediate();assert.equal(settled,false);assert.equal(f.reloads(),0);
  save.resolve(false);assert.equal(await first,false);assert.equal(f.control.get().update,'blocked');assert.equal(f.protocol.requests,1);
});

test('a controller replaced during the final save cannot inherit the earlier worker approval',async t=>{
  const f=updateFixture(t,{lostActivationAck:true}),save=pending();let phase=0;
  t.after(()=>save.resolve(true));f.exports.registerUpdateGuard(()=>++phase<3?true:save.promise);await setImmediate();
  f.offerUpdate();const first=f.control.applyUpdate();await f.workerFinished();f.useForeignController();f.activate();
  save.resolve(true);await setImmediate();f.expireActivationAck();
  assert.equal(await first,false);assert.equal(f.reloads(),0);assert.equal(f.protocol.requests,1);
});

test('a waiting worker replaced during the initial save needs a new explicit click',async t=>{
  const f=updateFixture(t,{lostActivationAck:true}),save=pending();t.after(()=>save.resolve(true));
  f.exports.registerUpdateGuard(()=>save.promise);await setImmediate();f.offerUpdate();const first=f.control.applyUpdate();
  f.replaceWaiting();save.resolve(true);assert.equal(await first,false);assert.equal(f.control.get().update,'available');
  assert.equal(f.protocol.requests,0);assert.equal(f.reloads(),0);
});

test('a committed update reloads once when controllerchange arrives first',async t=>{
  const f=updateFixture(t);await setImmediate();f.activate();await setImmediate();assert.equal(f.reloads(),0);
  f.commit();await setImmediate();assert.equal(f.reloads(),1);
});

test('a normal commit before controllerchange still reloads once',async t=>{
  const f=updateFixture(t);await setImmediate();f.commit();assert.equal(f.reloads(),0);
  f.activate();await setImmediate();assert.equal(f.reloads(),1);
});

test('late commit preserves dirty work and an explicit retry saves before reload',async t=>{
  const f=updateFixture(t);let saved=false;f.exports.registerUpdateGuard(()=>saved);await setImmediate();
  f.activate();f.commit();await setImmediate();assert.equal(f.reloads(),0);assert.equal(f.control.get().update,'blocked');
  assert.equal(await f.control.applyUpdate(),false);assert.equal(f.reloads(),0);
  saved=true;assert.equal(await f.control.applyUpdate(),true);assert.equal(f.reloads(),1);
});

test('duplicate commit while saving does not reload twice',async t=>{
  const f=updateFixture(t),save=pending();f.exports.registerUpdateGuard(()=>save.promise);await setImmediate();
  f.activate();f.commit();f.commit();await setImmediate();assert.equal(f.reloads(),0);
  save.resolve(true);await setImmediate();assert.equal(f.reloads(),1);
});

test('destroy during the activation save prevents late navigation',async t=>{
  const f=updateFixture(t),save=pending();f.exports.registerUpdateGuard(()=>save.promise);await setImmediate();
  f.commit();f.activate();await setImmediate();f.control.destroy();save.resolve(true);await setImmediate();assert.equal(f.reloads(),0);
});

test('a late activation ACK cannot hide a blocked save after controller change',async t=>{
  const f=updateFixture(t);let saved=true;f.exports.registerUpdateGuard(()=>saved);await setImmediate();
  f.offerUpdate(port=>{saved=false;f.activate();f.commit();void setImmediate().then(()=>{port.postMessage({ready:true});port.close();});});
  assert.equal(await f.control.applyUpdate(),false);assert.equal(f.reloads(),0);assert.equal(f.control.get().update,'blocked');
  saved=true;assert.equal(await f.control.applyUpdate(),true);assert.equal(f.reloads(),1);
});

test('the acknowledged worker already controlling this page settles without a second message',async t=>{
  const f=updateFixture(t);await setImmediate();
  f.offerUpdate(port=>{f.activate();port.postMessage({ready:true});port.close();});
  assert.equal(await f.control.applyUpdate(),true);await setImmediate();assert.equal(f.reloads(),1);
});

test('destroy while an explicit retry saves does not navigate the closed page',async t=>{
  const f=updateFixture(t),save=pending();let decision=false;f.exports.registerUpdateGuard(()=>decision);await setImmediate();
  f.activate();f.commit();await setImmediate();assert.equal(f.control.get().update,'blocked');
  decision=save.promise;const retry=f.control.applyUpdate();await setImmediate();f.control.destroy();save.resolve(true);
  assert.equal(await retry,false);assert.equal(f.reloads(),0);assert.equal(await f.control.applyUpdate(),false);
});

test('an explicit retry joining the automatic save flight shares one reload',async t=>{
  const f=updateFixture(t),save=pending();f.exports.registerUpdateGuard(()=>save.promise);await setImmediate();
  f.activate();f.commit();await setImmediate();const retry=f.control.applyUpdate();await setImmediate();
  save.resolve(true);assert.equal(await retry,true);await setImmediate();assert.equal(f.reloads(),1);
});

for(const outcome of ['accepted','dismissed'])test(`appinstalled stays installed when an older prompt later resolves ${outcome}`,async t=>{
  const f=fixture();t.after(()=>f.control.destroy());const offer=f.offer();offer.prompt.resolve();
  const request=f.control.requestInstall();await setImmediate();assert.equal(f.control.get().install,'prompting');
  f.installed();assert.equal(f.control.get().install,'installed');offer.choice.resolve({outcome});
  assert.equal(await request,outcome);assert.equal(f.control.get().install,'installed');assert.equal(f.prompts(),1);
});

test('standalone transition stays installed when an older prompt later fails',async t=>{
  const f=fixture();t.after(()=>f.control.destroy());const offer=f.offer();const request=f.control.requestInstall();
  f.standalone();offer.prompt.reject(Error('Synthetic closed install prompt'));
  assert.equal(await request,'unavailable');assert.equal(f.control.get().install,'installed');
});

for(const outcome of ['accepted','dismissed'])test(`a normal ${outcome} choice is not proof of installation`,async t=>{
  const f=fixture();t.after(()=>f.control.destroy());const offer=f.offer();
  assert.equal(offer.event.defaultPrevented,true);assert.equal(f.control.get().install,'available');
  const request=f.control.requestInstall();assert.equal(f.control.get().install,'prompting');
  offer.prompt.resolve();offer.choice.resolve({outcome});
  assert.equal(await request,outcome);assert.equal(f.control.get().install,outcome==='accepted'?'accepted':'unavailable');
  assert.equal(await f.control.requestInstall(),'unavailable');assert.equal(f.prompts(),1);
  f.installed();assert.equal(f.control.get().install,'installed');
});

test('a rejected prompt remains recoverable through a fresh browser offer',async t=>{
  const f=fixture();t.after(()=>f.control.destroy());const first=f.offer();
  const firstRequest=f.control.requestInstall();first.prompt.reject(Error('Synthetic unavailable prompt'));
  assert.equal(await firstRequest,'unavailable');assert.equal(f.control.get().install,'unavailable');
  const next=f.offer();assert.equal(f.control.get().install,'available');
  const nextRequest=f.control.requestInstall();next.prompt.resolve();next.choice.resolve({outcome:'accepted'});
  assert.equal(await nextRequest,'accepted');assert.equal(f.control.get().install,'accepted');assert.equal(f.prompts(),2);
});

test('repeated clicks invoke one browser prompt while its choice is pending',async t=>{
  const f=fixture();t.after(()=>f.control.destroy());const offer=f.offer();
  const first=f.control.requestInstall();assert.equal(await f.control.requestInstall(),'unavailable');
  offer.prompt.resolve();offer.choice.resolve({outcome:'dismissed'});
  assert.equal(await first,'dismissed');assert.equal(f.prompts(),1);assert.equal(f.control.get().install,'unavailable');
});

test('a standalone launch never offers a second install even if the browser emits a prompt',async t=>{
  const f=fixture({standalone:true});t.after(()=>f.control.destroy());
  assert.equal(f.control.get().install,'installed');const offer=f.offer();
  assert.equal(offer.event.defaultPrevented,true);assert.equal(f.control.get().install,'installed');
  assert.equal(await f.control.requestInstall(),'unavailable');assert.equal(f.prompts(),0);
});

test('installation remains observed after a later prompt event or display-mode exit',async t=>{
  const f=fixture();t.after(()=>f.control.destroy());f.installed();
  f.offer();f.display.matches=false;f.display.dispatchEvent(new Event('change'));
  assert.equal(f.control.get().install,'installed');
  assert.equal(await f.control.requestInstall(),'unavailable');assert.equal(f.prompts(),0);
});

test('an old choice cannot consume a newer browser offer',async t=>{
  const f=fixture();t.after(()=>f.control.destroy());const old=f.offer();
  const oldRequest=f.control.requestInstall();old.prompt.resolve();
  const current=f.offer();assert.equal(f.control.get().install,'available');
  old.choice.resolve({outcome:'dismissed'});assert.equal(await oldRequest,'dismissed');
  assert.equal(f.control.get().install,'available');
  const currentRequest=f.control.requestInstall();current.prompt.resolve();current.choice.resolve({outcome:'accepted'});
  assert.equal(await currentRequest,'accepted');assert.equal(f.control.get().install,'accepted');assert.equal(f.prompts(),2);
});

test('destroy releases handlers and prevents a pending choice from publishing later',async()=>{
  const f=fixture(),seen=[];f.control.subscribe(state=>seen.push(state.install));const offer=f.offer();
  const request=f.control.requestInstall();f.control.destroy();const count=seen.length;
  offer.prompt.resolve();offer.choice.resolve({outcome:'accepted'});
  assert.equal(await request,'accepted');assert.equal(seen.length,count);assert.equal(f.timers.size,0);
  const after=f.offer();assert.equal(after.event.defaultPrevented,false);
  assert.equal(await f.control.requestInstall(),'unavailable');assert.equal(f.prompts(),1);
});

test('a silent standalone transition outranks the pending prompt result',async t=>{
  const f=fixture();t.after(()=>f.control.destroy());const offer=f.offer(),request=f.control.requestInstall();
  f.display.matches=true;offer.prompt.resolve();offer.choice.resolve({outcome:'dismissed'});
  assert.equal(await request,'dismissed');assert.equal(f.control.get().install,'installed');
});
