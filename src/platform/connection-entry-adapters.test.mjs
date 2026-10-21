import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import ts from 'typescript';
import { readFile } from 'node:fs/promises';
import * as intents from './connection-intent.mjs';
import * as nativeVoiceDescriptor from './native-voice-descriptor.mjs';
import * as navigation from './connection-navigation.mjs';

const deferred=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};};
const turn=()=>new Promise(resolve=>setImmediate(resolve));
async function fixture(shell){
  const gate=deferred(), events=new Map(), mounts=[], requests=[];
  let entryOptions, bootstraps=0, detached=0, disposed=0;
  const client={async bootstrap(){bootstraps++;throw Error('unexpected automatic bootstrap');},async getLocalState(){return {accountId:'account-A',deviceId:'device-A'};},async extension(...args){requests.push(args);return {};}};
  const window={location:{href:'https://shell.example/',origin:'https://shell.example',assign(){throw Error('unexpected navigation');}},
    addEventListener(type,fn){const set=events.get(type)??new Set();set.add(fn);events.set(type,set);},removeEventListener(type,fn){events.get(type)?.delete(fn);}};
  const ports={
    '../core/connect-client':{accountClient:client,observeAccount:()=>()=>{}},
    './connection-hub':{ensureConnectionAccount(_host,options){entryOptions=options;return gate.promise;}},
    '../platform/connection-hub':{ensureConnectionAccount(_host,options){entryOptions=options;return gate.promise;}},
    './native-voice-descriptor.mjs':nativeVoiceDescriptor,
    './connection-intent.mjs':intents,'../platform/connection-intent.mjs':intents,
    './connection-navigation.mjs':navigation,'../platform/connection-navigation.mjs':navigation,
    './call-identity':{bindCallIdentity(){return ()=>{detached++;};}},
    '../world/app':{mountWorldApp(_host,options){mounts.push(options);return {async refresh(){},destroy(){},async followConnection(){}};}},
    './local-apps':{createAppActions:()=>({connectDevice(){},agentCreate(){},destroy(){},resetAccount(){}})},
    './assistant':{},'../sync':{},'qrcode':{},'../../modules/connect/ui/index.mjs':{},
  };
  const file=shell==='world'?'./world-adapter.ts':'../features/connect.ts';
  const source=await readFile(new URL(file,import.meta.url),'utf8');
  const compiled=ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
  const module={exports:{}};
  vm.runInNewContext(compiled,{module,exports:module.exports,require:name=>ports[name]??{},window,URL,AbortSignal,AbortController,
    fetch:async()=>({ok:false})});
  const root={isConnected:true},lifecycle={dispose(){disposed++;}};
  return {gate,mounts,requests,root,client,get entryOptions(){return entryOptions;},get bootstraps(){return bootstraps;},get detached(){return detached;},get disposed(){return disposed;},
    start:()=>shell==='world'?module.exports.startWorld(root,{callLifecycle:lifecycle}):module.exports.initializeConnect('Existing name',root),module};
}

for(const shell of ['world','classic']){
  test(`actual ${shell} adapter awaits entry and never starts a parallel bootstrap`,async()=>{
    const f=await fixture(shell), started=f.start();let ready=false;started.then(()=>{ready=true;});await turn();
    assert.equal(ready,false);assert.equal(f.mounts.length,0);assert.equal(f.bootstraps,0);
    assert.equal(f.entryOptions.client,f.client);assert.equal(f.entryOptions.isCurrent(),true);
    f.gate.resolve({state:{accountId:'account-A',deviceId:'device-A'},intent:null});await started;
    assert.equal(f.bootstraps,0);assert.equal(f.mounts.length,shell==='world'?1:0);
  });
  test(`actual ${shell} adapter preserves entry rejection without mounting another identity`,async()=>{
    const f=await fixture(shell), expected=Object.assign(Error('synthetic storage failure'),{code:'LOCAL_STATE_UNAVAILABLE'}),started=f.start();
    f.gate.reject(expected);await assert.rejects(started,error=>error===expected);
    assert.equal(f.mounts.length,0);assert.equal(f.bootstraps,0);
    if(shell==='world'){assert.equal(f.detached,1);assert.equal(f.disposed,1);}
  });
}
test('actual classic adapter retains the typed contact continuation across cold entry',async()=>{
  const f=await fixture('classic'),started=f.start();
  f.gate.resolve({state:{accountId:'account-A',deviceId:'device-A'},intent:{kind:'contact',id:'card_'+ 'a'.repeat(32)}});
  assert.equal(await started,true);assert.equal(f.module.exports.hasPendingConnect(),true);assert.equal(f.bootstraps,0);
});
