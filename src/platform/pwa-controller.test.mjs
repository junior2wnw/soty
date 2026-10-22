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

function updateFixture(t) {
  const window=new EventTarget(),document=new EventTarget(),display=new EventTarget(),workers=new EventTarget(),exports={};
  document.visibilityState='visible';display.matches=false;let reloads=0,onActivate;
  class ServiceWorker extends EventTarget {
    scriptURL='https://soty.example/sw.js';
    postMessage(message,[port]) {
      if (message.type==='SOTY_ACTIVATE_UPDATE'&&onActivate) { onActivate(port);return; }
      port.postMessage(message.type==='SOTY_OFFLINE_STATUS'?{offlineReady:true}:{ready:true});port.close();
    }
  }
  const previous=new ServiceWorker(),next=new ServiceWorker(),registration=new EventTarget();
  registration.waiting=null;registration.update=async()=>{};
  workers.controller=previous;workers.register=async()=>registration;workers.ready=Promise.resolve(registration);
  const context={exports,window,document,ServiceWorker,MessageChannel,AbortController,AbortSignal,Event,URL,
    matchMedia:()=>display,navigator:{serviceWorker:workers,onLine:true},
    location:{origin:'https://soty.example',reload(){reloads++;}},fetch:async()=>({ok:true,json:async()=>({ok:true})}),
    setTimeout,clearTimeout,setInterval:()=>1,clearInterval(){}};
  vm.runInNewContext(compiled,context,{timeout:1000});const control=exports.createPwaController();t.after(()=>control.destroy());
  return {control,exports,reloads:()=>reloads,
    offerUpdate(handler){registration.waiting=next;onActivate=handler;},
    activate(){registration.waiting=null;workers.controller=next;workers.dispatchEvent(new Event('controllerchange'));},
    commit(){const event=new Event('message');Object.assign(event,{source:next,data:{type:'SOTY_UPDATE_COMMIT'},ports:[]});workers.dispatchEvent(event);}};
}

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
