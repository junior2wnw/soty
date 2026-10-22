import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash,createECDH } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { validateState } from '../../modules/connect/browser/storage.mjs';
import { createConnectService } from '../../modules/connect/server/index.mjs';
import { createConnectHandler } from '../../modules/connect/server/http.mjs';
import { createNotificationsController } from './notifications-client.mjs';
import * as intents from './connection-intent.mjs';
import * as navigation from './connection-navigation.mjs';

const [coreSource,adapterSource]=await Promise.all(['../core/connect-client.ts','./world-adapter.ts'].map(file=>readFile(new URL(file,import.meta.url),'utf8')));
const compiled=source=>ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const turn=()=>new Promise(done=>setImmediate(done));
async function until(predicate){const deadline=Date.now()+5000;while(!predicate()){assert.ok(Date.now()<deadline,'bounded identity continuation completed');await turn();}}
function load(source,ports,globals={}){
  const module={exports:{}};vm.runInNewContext(compiled(source),{module,exports:module.exports,require:name=>ports[name]??{},URL,AbortController,AbortSignal,...globals});return module.exports;
}

// Real browser SDK keys/CAS/onState, the actual core observer and world adapter,
// and signed loopback Connect HTTP. Only UI/media/browser-push ports are inert;
// synthetic notification targets isolate this test from World delivery tests.
async function fixture(t,{cold=false,initiallyBlocked=false}={}){
  const projectId='soty',calls=[],opened=[],mounts=[],controllers=[],events=new Map();let handler,saved=null,offlineStatus=false,blocked=initiallyBlocked;
  const server=createServer((req,res)=>void handler(req,res));await new Promise(done=>server.listen(0,'127.0.0.1',done));
  const origin='http://127.0.0.1:'+server.address().port,endpoint=origin+'/api/connect/rpc',scope={projectId,endpoint};
  const key=createECDH('prime256v1');key.generateKeys();const publicKey=key.getPublicKey().toString('base64url');
  const notificationId='N'.repeat(32),binding=actor=>createHash('sha256').update(projectId+'\0'+actor.accountId+'\0'+actor.deviceId).digest('hex');
  const service=createConnectService({databasePath:':memory:',projectId,allowedOrigins:[origin],extensions:[{
    operations:new Set(['notifications.status','notifications.open']),
    execute({op,args,actor}){
      assert.equal(args.expectedAccountId,actor.accountId);calls.push({op,accountId:actor.accountId,deviceId:actor.deviceId});
      if(op==='notifications.status')return{schema:'soty.push-status.v1',configured:true,enabled:false,bindingId:binding(actor),publicKey};
      assert.equal(args.notificationId,notificationId);return{schema:'soty.push-open.v1',target:{kind:'room',communityId:'room_identity'}};
    },
  }]});handler=createConnectHandler(service);
  const storage={read:async()=>structuredClone(saved),claim:async value=>{if(saved===null){validateState(value,scope);saved=structuredClone(value);}return structuredClone(saved);},
    compareAndSwap:async(expected,value)=>{assert.equal(saved.localRevision,expected);validateState(value,scope);saved=structuredClone(value);return structuredClone(saved);}};
  const core=load(coreSource,{'../../modules/connect/browser/index.mjs':{createConnectClient(options){return createClientWithStorage({...options,endpoint,
    fetch:async(url,request)=>{const body=JSON.parse(request.body);if(offlineStatus&&body.op==='notifications.status')throw TypeError('synthetic status network outage');
      return fetch(url,{...request,headers:{...request.headers,origin}});}},storage);}}});
  const client=core.accountClient;
  t.after(async()=>{for(const fn of events.get('pagehide')??[])fn({persisted:false});for(const controller of controllers)controller.dispose();
    client.dispose();server.closeAllConnections();await new Promise(done=>server.close(done));service.close();});
  await client.bootstrap('Disposable notification identity');
  const window={location:{href:origin+(cold?'/?notification='+notificationId+'#mine':'/#mine'),origin,assign(){throw Error('unexpected fixture navigation');}},
    addEventListener(name,fn){const set=events.get(name)??new Set();set.add(fn);events.set(name,set);},removeEventListener(name,fn){events.get(name)?.delete(fn);}};
  window.history={replaceState(_state,_title,value){window.location.href=new URL(value,window.location.href).href;}};
  const root={isConnected:true},ports={
    '../core/connect-client':core,
    './notifications-client.mjs':{createNotificationsController(options){const value=createNotificationsController({...options,serviceWorker:undefined,notification:undefined,pushSupported:false,
      location:window.location,history:window.history});controllers.push(value);return value;}},
    './connection-hub':{ensureConnectionAccount:async()=>({state:await client.getLocalState(),intent:null})},
    './connection-intent.mjs':intents,'./connection-navigation.mjs':navigation,
    './native-voice-descriptor.mjs':{createNativeVoiceDescriptor:()=>({read:async()=>null})},
    './call-identity':{bindCallIdentity:()=>()=>{}},
    './local-apps':{createAppActions:()=>({destroy(){},resetAccount(){},connectDevice(){},agentCreate(){}})},
    '../world/app':{mountWorldApp(_root,options){const mount={options,destroyed:false};mounts.push(mount);return{
      refresh:async()=>{},destroy(){mount.destroyed=true;},
      async openNotification(target,context){await context.assertCurrent();if(blocked)throw Object.assign(Error('synthetic unsaved work'),{code:'push_navigation_blocked'});
        opened.push({target,localRevision:(await client.getLocalState()).localRevision});},
    };}},
  };
  const adapter=load(adapterSource,ports,{window,fetch:()=>{throw Error('unexpected capability I/O');}});
  await adapter.startWorld(root,{callLifecycle:{dispose(){}}});await until(()=>calls.some(value=>value.op==='notifications.status'));
  await until(()=>controllers[0]?.view().phase!=='checking');
  return{client,controllers,calls,opened,mounts,notificationId,window,block(value){blocked=value;},offline(value){offlineStatus=value;}};
}

test('a real signed SDK rename refreshes the pinned notification revision through the actual core and world observer',async t=>{
  const f=await fixture(t),controller=f.controllers[0],before=await f.client.getLocalState();
  assert.equal(await controller.open(f.notificationId),true);const statusBefore=f.calls.filter(value=>value.op==='notifications.status').length;
  await f.client.rename('Renamed without changing actor',before.accountId);const after=await f.client.getLocalState();
  assert.equal(after.accountId,before.accountId);assert.equal(after.deviceId,before.deviceId);assert.ok(after.localRevision>before.localRevision);
  await until(()=>f.calls.filter(value=>value.op==='notifications.status').length>statusBefore);await until(()=>controller.view().phase!=='checking');
  assert.equal(f.mounts.length,1);assert.equal(f.controllers.length,1);assert.equal(await controller.open(f.notificationId),true);
  assert.equal(f.opened.length,2);assert.equal(f.opened[1].localRevision,after.localRevision);
  assert.equal((await f.client.getLocalState()).notificationError,null);
  assert.equal(f.calls.filter(value=>value.op==='notifications.open').length,2);
});

test('a retry whose status refresh loses network remains visible as an opening error and can recover with the same signed ID',async t=>{
  const f=await fixture(t),controller=f.controllers[0];f.block(true);
  assert.equal(await controller.open(f.notificationId),false);assert.equal(controller.view().code,'push_navigation_blocked');
  f.offline(true);assert.equal(await controller.retryOpen(),false);
  assert.equal(controller.view().code,'push_notification_unavailable','the global retry toast must remain eligible after a status-network failure');
  f.offline(false);f.block(false);assert.equal(await controller.retryOpen(),true);assert.equal(f.opened.length,1);
});

test('cold opaque entry attempts once; status and same-actor rename do not repeat a blocked navigation',async t=>{
  const f=await fixture(t,{cold:true,initiallyBlocked:true}),controller=f.controllers[0];
  await until(()=>controller.view().code==='push_navigation_blocked');const count=f.calls.filter(value=>value.op==='notifications.open').length;
  await controller.refresh();await f.client.rename('Same actor from cold entry',(await f.client.getLocalState()).accountId);
  await until(()=>controller.view().phase!=='checking');
  assert.equal(f.calls.filter(value=>value.op==='notifications.open').length,count);
  assert.equal(f.opened.length,0);assert.equal(new URL(f.window.location.href).searchParams.has('notification'),true);
  f.block(false);assert.equal(await controller.retryOpen(),true);assert.equal(f.opened.length,1);
  assert.equal(new URL(f.window.location.href).searchParams.has('notification'),false);
});
