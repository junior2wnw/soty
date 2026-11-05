import test from 'node:test';
import assert from 'node:assert/strict';
import { readWorkspaceReturn } from './workspace-return.mjs';
import * as workspaceReturns from './workspace-return.mjs';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import * as appLaunch from './app-launch.mjs';
import {parseCanvasPageRoute} from './canvas-page-client.mjs';
import * as connectionIntents from '../platform/connection-intent.mjs';
import * as connectionNavigation from '../platform/connection-navigation.mjs';
import * as nativeVoice from '../platform/native-voice-descriptor.mjs';
import {createPlatformLiveTestPort} from '../platform/test-support/platform-live-port.mjs';
import {createRetainedAppRuntimes} from './app-retained-runtime.mjs';
import {hasRetainedNativeDrafts,flushRetainedNativeDrafts,registerRetainedNativeDraft} from '../../modules/notes/browser/native-local.mjs';

test('a return retains the exact scoped search and list, but crosses no account boundary', () => {
  const value = { accountId: 'A', route: '#world?q=a%2Bb&kind=app', scroll: 240, presentation: 'list', listQuery: 'доска', focusEntity: 'app:one' };
  assert.deepEqual(readWorkspaceReturn(value, 'A'), value);
  assert.equal(readWorkspaceReturn(value, 'B'), null);
});

test('a return cannot launch another app, escape the shell, or smuggle arbitrary state', () => {
  for (const route of ['https://other.invalid', '#app/one', '#launch/one', '#mine\n#world', '#mine-other'])
    assert.equal(readWorkspaceReturn({ accountId: 'A', route }, 'A'), null);
  assert.deepEqual(readWorkspaceReturn({ accountId: 'A', route: '#messages/thread', scroll: -4, token: 'must-not-copy', presentation: 'admin' }, 'A'),
    { accountId: 'A', route: '#messages/thread', scroll: 0 });
});

const appSource=readFileSync(new URL('./app.ts',import.meta.url),'utf8');
const adapterSource=readFileSync(new URL('../platform/world-adapter.ts',import.meta.url),'utf8');
const entrySource=readFileSync(new URL('../platform/canvas-page-entry.ts',import.meta.url),'utf8');
const pageRoute=`#space/page_${'a'.repeat(32)}`,appId=`app-${'b'.repeat(32)}`;
const launchRoute='#'+appLaunch.formatAppLaunchRoute({appId,domainId:`dom_${'c'.repeat(32)}`,path:'/board?q=one#inside'});
const camera={mine:{x:782.5,y:-110.25,scale:1.37},mineContext:'main_context',mineSelection:'page_shortcut',mineFit:'manual'};
const turn=()=>new Promise(resolve=>setImmediate(resolve));
const until=async predicate=>{for(let index=0;index<100;index++){if(predicate())return;await turn();}assert.fail('Actual cold route flow did not settle');};
const actor=(accountId='account-A',deviceId='device-A')=>({accountId,deviceId,current:{revoked:false},label:'Fixture'});

class RouteElement {
  isConnected=true;dataset={};children=[];style={};attributes=new Map();scrollTop=0;classes=new Set();
  classList={add:(...names)=>names.forEach(name=>this.classes.add(name)),remove:(...names)=>names.forEach(name=>this.classes.delete(name)),toggle:(name,on)=>{if(on)this.classes.add(name);else this.classes.delete(name);}};
  replaceChildren(...nodes){this.children=[];this.append(...nodes);}append(...nodes){for(const node of nodes){node.parentElement=this;this.children.push(node);}}querySelector(){return null;}focus(){}
  setAttribute(name,value){this.attributes.set(name,String(value));}removeAttribute(name){this.attributes.delete(name);}
  remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(node=>node!==this);this.parentElement=null;this.isConnected=false;}
}
const productAst=ts.createSourceFile('product.ts',readFileSync(new URL('./product.ts',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true);
const capabilitiesSource=productAst.statements.filter(ts.isVariableStatement).flatMap(node=>[...node.declarationList.declarations]).find(node=>node.name.getText(productAst)==='capabilities').initializer.getText(productAst);

// Actual entry + adapter + controller methods, with inert view/transport ports.
// This checks navigation state and freshness, not server admission or app frames.
function coldFixture({href='https://shell.example/'+launchRoute,state=actor(),historyReturn,appCode=appSource,adapterCode=adapterSource,entryCode=entrySource,read}={}){
  let selected=state,reads=0;const observers=new Set(),events=new Map(),mounts=[],stages=[],pages=[],historyWrites=[],requests=[];
  const location=new URL(href),history={state:historyReturn?{soty:true,sotyReturn:historyReturn}:null,
    replaceState(value,_title,target){this.state=structuredClone(value);location.href=new URL(target,location.href).href;historyWrites.push({kind:'replace',state:this.state,route:location.hash});},
    pushState(value,_title,target){this.state=structuredClone(value);location.href=new URL(target,location.href).href;historyWrites.push({kind:'push',state:this.state,route:location.hash});}};
  const window={location,history,addEventListener(type,callback,options){if(!events.has(type))events.set(type,new Set());events.get(type).add(callback);options?.signal?.addEventListener('abort',()=>events.get(type)?.delete(callback),{once:true});},removeEventListener(type,callback){events.get(type)?.delete(callback);}};
  const root=new RouteElement(),client={async getLocalState(){const index=++reads;return read?read(index,selected):selected;},async bootstrap(){throw Error('No automatic account bootstrap');},async extension(op,args,context){requests.push({op,args,context});return{};}};
  const observeAccount=callback=>{observers.add(callback);return()=>observers.delete(callback);};
  const pageFactory=options=>{const value={options,element:new RouteElement(),ready:Promise.resolve(),dispose(){value.disposed=true;},hasUnsavedChanges:()=>false,async flush(){}};pages.push(value);return value;};
  const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
  const parsed=ts.createSourceFile('app.ts',appCode,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),appClass=parsed.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='WorldApplication');
  const names=new Set(['transitionAccount','accountTask','writeRoute','openRoute','openApplication','followConnection','restoreApplicationReturn','initialApplicationReturn','appRuntimeStages','isRetainedMain','mainRuntimeOwners','hideMainApplication','returnMainToCanvas']);
  const methods=appClass.members.filter(node=>node.name&&names.has(node.name.getText(parsed))).map(node=>node.getText(parsed)).join('\n');
  const routeContext=vm.createContext({window,location,history,URLSearchParams,HTMLElement:RouteElement,document:{activeElement:{},body:{}},queueMicrotask,Map,Set,Error,
    ...workspaceReturns,...appLaunch,createRetainedAppRuntimes,parseCanvasPageRoute,hasRetainedNativeDrafts,flushRetainedNativeDrafts,loadDeskPreferences:()=>({favorites:[],recent:[]}),createHexFieldState:()=>({}),el:()=>new RouteElement(),
    createCanvasFieldContent:()=>({}),createCanvasPageScreen:pageFactory,
    mountAppStage:(_host,options)=>{stages.push(options);const stage={ready:Promise.resolve(),dispose(){stage.disposed=true;},requiresCloseCheck:()=>false,hasUnsavedChanges:()=>false,matches:next=>appLaunch.sameAppLaunchLocation(options.intent,next,options.app.entry),async resume(next){return stage.matches(next);},updateRoute(next){options.intent=next;}};return stage;},
  });
  vm.runInContext(compile(`const capabilities=${capabilitiesSource};class RouteController{${methods}}globalThis.RouteController=RouteController;`),routeContext);
  const mountController=options=>{
    const controller=new routeContext.RouteController();Object.assign(controller,{options,api:options.api,root,main:new RouteElement(),desk:{favorites:[],recent:[]},deskAccount:'',accountGeneration:0,homeRequest:0,
      screenSequence:0,destroyed:false,agentOrb:null,widgets:null,dialogs:[],accountField:null,homeState:{slots:new Map()},discoveryApps:new Map(),searchTimer:null,toastTimer:null,
      restoreLastRoute:false,canvasPageViews:new Map(),applicationReturn:null,pendingReturnPresentation:null,activeRoute:location.hash,appStage:null,unifiedField:null,fieldBackdrop:null,
      avatars:{setContext(){}},apps:[],communities:[],chatDrafts:{hasVolatile:()=>false},preferences:{},live:{textContent:''},
      renderNavigation(){},saveDesk(){},cleanScreen(){this.screenSequence++;this.canvasPage?.dispose();this.canvasPage=null;this.hideMainApplication();},
      screenHasUnsavedChanges:()=>false,async flushScreen(){},afterNoteSaved(callback){callback();},appRuntime:()=>({}),async loadApps(){return[];},
      navigateReady(view){this.writeRoute(view);},navigate(view){this.writeRoute(view);},destroy(){this.destroyed=true;},
    });
    controller.transitionAccount(selected.accountId);controller.refresh=async()=>{controller.writeRoute('mine');};mounts.push(controller);return controller;
  };
  const module={exports:{}},ports={
    '../core/connect-client':{accountClient:client,observeAccount},'../world/app':{mountWorldApp:(_host,options)=>mountController(options)},
    '../world/workspace-return.mjs':workspaceReturns,'./connection-hub':{async ensureConnectionAccount(){return{state:selected,intent:null};}},
    './connection-intent.mjs':connectionIntents,'./connection-navigation.mjs':connectionNavigation,'./native-voice-descriptor.mjs':nativeVoice,
    './platform-live-client.mjs':createPlatformLiveTestPort(),'./notifications-client.mjs':{createNotificationsController:()=>({async refresh(){},dispose(){}})},
    './call-identity':{bindCallIdentity:()=>()=>{}},'./local-apps':{createAppActions:()=>({resetAccount(){},destroy(){}})},
  };
  vm.runInNewContext(compile(adapterCode),{module,exports:module.exports,require:name=>ports[name]??{},window,URL,AbortSignal,AbortController,fetch:async()=>({ok:true,json:async()=>({nativeVoice:null})})});
  const startWorld=dependencies=>module.exports.startWorld(root,{callLifecycle:{dispose(){}},...dependencies});
  const entryModule={exports:{}},entryPorts={'../core/connect-client':{accountClient:client,observeAccount},'../world/canvas-page-screen':{createCanvasPageScreen:pageFactory},
    '../world/canvas-page-client.mjs':{parseCanvasPageRoute},'../world/workspace-return.mjs':workspaceReturns,'../world/app-launch.mjs':appLaunch,'./world-adapter':{startWorld:(_root,dependencies)=>startWorld(dependencies)}};
  vm.runInNewContext(compile(entryCode),{module:entryModule,exports:entryModule.exports,require:name=>entryPorts[name]??{},window,location,history,URL,AbortController});
  return{root,location,history,mounts,stages,pages,historyWrites,requests,observers,startWorld,
    startPage:()=>entryModule.exports.startCanvasPageEntry(root),get reads(){return reads;},
    change(next){selected=next;for(const callback of [...observers])callback(next);},emit(type){for(const callback of [...(events.get(type)??[])])callback({persisted:false});},
  };
}

test('actual cold public page → app → Back keeps the exact guest route and sanitized camera',async t=>{
  const route=pageRoute+'?view=public',f=coldFixture({href:'https://shell.example/'+route});t.after(()=>f.emit('pagehide'));
  await f.startPage();const original=f.pages[0];original.options.onViewStateChange?.({...camera,fieldDocument:{secret:'must not travel'}});
  original.options.onOpenApp({appId,domainId:`dom_${'c'.repeat(32)}`,path:'/board?q=one#inside'});await until(()=>f.location.hash!==route);f.emit('hashchange');await until(()=>f.stages.length===1);
  f.stages[0].onBack();await turn();assert.equal(f.location.hash,route);await until(()=>f.pages.length===2);
  assert.equal(f.pages[1].options.guestPreview,true);assert.equal(f.pages[1].options.accountId,undefined);
  assert.deepEqual(JSON.parse(JSON.stringify(f.pages[1].options.viewState)),camera);assert.equal(JSON.stringify(f.historyWrites).includes('must not travel'),false);
});

test('actual app reload captures existing sotyReturn before neutral typed mount and returns its camera',async t=>{
  const destination={accountId:'account-A',route:pageRoute,scroll:0,canvasView:camera},f=coldFixture({historyReturn:destination});t.after(()=>f.emit('pagehide'));
  await f.startWorld();assert.equal(f.stages.length,1);f.stages[0].onBack();await turn();assert.equal(f.location.hash,pageRoute);await until(()=>f.pages.length===1);
  assert.deepEqual(JSON.parse(JSON.stringify(f.pages[0].options.viewState)),camera);
});

test('page handoff is a strict route and display-only camera, with no authority or Source data',()=>{
  const raw={accountId:'account-A',deviceId:'device-A',route:pageRoute+'?view=public',launchRoute,canvasView:{...camera,
    mine:{x:99_000_000,y:-99_000_000,scale:900},search:{x:1,y:2,scale:1},fieldDocument:{private:true}},token:'never',permissions:['owner'],sourceBody:'secret'};
  const handoff=workspaceReturns.readCanvasPageHandoff(raw),destination=workspaceReturns.bindCanvasPageReturn(handoff,{accountId:'account-A',deviceId:'device-A',launchRoute});
  assert.deepEqual(destination,{accountId:'account-A',route:raw.route,scroll:0,canvasView:{...camera,mine:{x:3_000_000,y:-3_000_000,scale:2}}});
  assert.equal(JSON.stringify(handoff).includes('secret'),false);assert.equal(JSON.stringify(handoff).includes('fieldDocument'),false);assert.equal(JSON.stringify(handoff).includes('permissions'),false);
  for(const route of[pageRoute+'?view=public&role=owner',pageRoute+'?view=private',pageRoute+'/',pageRoute+'?view=public#mine','#mine',pageRoute+'\n'])
    assert.equal(workspaceReturns.readCanvasPageHandoff({...raw,route}),null,route);
  for(const [accountId,deviceId]of[['account-B','device-A'],['account-A','device-B']])assert.equal(workspaceReturns.bindCanvasPageReturn(handoff,{accountId,deviceId,launchRoute}),null);
  assert.equal(workspaceReturns.bindCanvasPageReturn(handoff,{accountId:'account-A',deviceId:'device-A',launchRoute:'#app/app-'+ 'd'.repeat(32)}),null);
  const guest=workspaceReturns.bindCanvasPageReturn({...raw,accountId:null,deviceId:null},{accountId:'first-legitimate-account',deviceId:'first-legitimate-device',launchRoute});
  assert.equal(guest.accountId,'first-legitimate-account');assert.equal(guest.route,raw.route);
  const invalidCamera=readWorkspaceReturn({accountId:'A',route:pageRoute,canvasView:{mine:{x:NaN,y:0,scale:1},mineContext:'bad\ncontext',mineFit:'admin',source:'secret'}},'A');
  assert.deepEqual(invalidCamera,{accountId:'A',route:pageRoute,scroll:0});
});

test('warm public-page return keeps its latest camera and an exact guest route',async t=>{
  const f=coldFixture({href:'https://shell.example/#mine'});t.after(()=>f.emit('pagehide'));await f.startWorld();
  const controller=f.mounts[0],route=pageRoute+'?view=public';controller.writeRoute(route.slice(1));await controller.openRoute();
  f.pages[0].options.onViewStateChange({...camera,mine:{x:44,y:18,scale:.83}});
  await controller.openApplication({appId,name:'Actual route fixture'},appLaunch.parseAppLaunchRoute(launchRoute));
  f.stages[0].onBack();await turn();assert.equal(f.location.hash,route);await until(()=>f.pages.length===2);
  assert.deepEqual(JSON.parse(JSON.stringify(f.pages[1].options.viewState)),{...camera,mine:{x:44,y:18,scale:.83}});
});

test('cold handoff and legacy reload return cannot survive startup account/device changes or account ABA',async t=>{
  for(const change of['account','device','ABA']){
    let release;const gate=new Promise(resolve=>{release=resolve;}),f=coldFixture({historyReturn:{accountId:'account-A',route:pageRoute,scroll:0,canvasView:camera},read:(index,selected)=>index===1?gate:selected});t.after(()=>f.emit('pagehide'));
    const running=f.startWorld({canvasPageReturn:{route:pageRoute,launchRoute,accountId:'account-A',deviceId:'device-A',canvasView:camera}});
    await until(()=>f.reads===1);
    if(change==='device')f.change(actor('account-A','device-new'));else{f.change(actor('account-B','device-B'));if(change==='ABA')f.change(actor());}
    release(actor());await running;assert.equal(f.stages.length,0,change);assert.equal(f.mounts[0].options.initialWorkspaceReturn,undefined,change);assert.equal(f.mounts[0].initialApplicationReturn,null,change);
  }
});

test('old page callbacks and an old App Back cannot reactivate across identity ABA',async t=>{
  const entry=coldFixture({href:'https://shell.example/'+pageRoute});t.after(()=>entry.emit('pagehide'));await entry.startPage();const old=entry.pages[0];
  old.options.onOpenApp({appId});entry.change(actor('account-B','device-B'));entry.change(actor());await turn();
  assert.equal(entry.mounts.length,0,'ABA during the lazy import cannot hand off the retired page');
  old.options.onOpenApp({appId});await turn();assert.equal(entry.mounts.length,0);
  const app=coldFixture({historyReturn:{accountId:'account-A',route:pageRoute,scroll:0,canvasView:camera}});t.after(()=>app.emit('pagehide'));await app.startWorld();const oldBack=app.stages[0].onBack;
  app.change(actor('account-B','device-B'));app.change(actor());const route=app.location.hash;oldBack();await turn();assert.equal(app.location.hash,route);assert.equal(app.pages.length,0);
});

test('cold-path oracles reject losing history before neutralization and replacing handoff with neutral mine',async t=>{
  const lostHistory=adapterSource.replace('entryReturnState = window.history?.state?.sotyReturn','entryReturnState = undefined');assert.notEqual(lostHistory,adapterSource);
  const broken=appSource.replace('if(inherited)this.applicationReturn=inherited;','if(false)this.applicationReturn=inherited;');assert.notEqual(broken,appSource);
  for(const source of[{adapterCode:lostHistory},{appCode:broken}]){
    const f=coldFixture({...source,historyReturn:{accountId:'account-A',route:pageRoute,scroll:0,canvasView:camera}});t.after(()=>f.emit('pagehide'));await f.startWorld();f.stages[0].onBack();await turn();
    assert.equal(f.location.hash,'#mine');assert.throws(()=>assert.equal(f.location.hash,pageRoute),'The original exact-return oracle must reject the broken control');
  }
});

async function retainedDrainOracle(t,options){
  assert.equal(hasRetainedNativeDrafts(),false,'the Node test worker starts without other retained sessions');
  let dirty=true,flushes=0;
  const release=registerRetainedNativeDraft({hasUnsavedChanges:()=>dirty,async flushLocal(){flushes++;dirty=false;}});t.after(release);
  assert.equal(hasRetainedNativeDrafts(),true);
  const f=coldFixture(options);t.after(()=>f.emit('pagehide'));await f.startWorld();
  assert.equal(flushes,1,'the actual transitionAccount must drain the registered retained session');
  assert.equal(hasRetainedNativeDrafts(),false);
}
test('the VM transitionAccount port executes the real Native retention registry rather than an empty stub',async t=>retainedDrainOracle(t));
test('the same retention oracle detects removal of the production transition drain',async t=>{
  const parsed=ts.createSourceFile('app.ts',appSource,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),appClass=parsed.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='WorldApplication');
  const method=appClass.members.find(node=>node.name?.getText(parsed)==='transitionAccount'),text=method.getText(parsed),drain='void flushRetainedNativeDrafts().catch(()=>{});';
  assert.equal(text.split(drain).length,2,'mutate exactly the transition drain, not constructor or pagehide');
  const broken=appSource.slice(0,method.getStart(parsed))+text.replace(drain,'')+appSource.slice(method.end);
  await assert.rejects(retainedDrainOracle(t,{appCode:broken}),error=>{assert.equal(error.code,'ERR_ASSERTION');assert.match(error.message,/actual transitionAccount must drain/u);return true;});
});
