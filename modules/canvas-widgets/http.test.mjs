import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer,request} from 'node:http';
import {randomBytes,createHash} from 'node:crypto';
import {mkdtemp,rm,realpath} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import WebSocket from 'ws';
import {createConnectService} from '../connect/server/index.mjs';
import {createConnectHandler} from '../connect/server/http.mjs';
import {createClientWithStorage} from '../connect/browser/client.mjs';
import {createWorldService} from '../world/server/index.mjs';
import {createAppsService} from '../apps/server/index.mjs';
import {createCanvasObjectsService} from '../canvas-objects/server.mjs';
import {createCanvasWidgetsService,createCanvasWidgetProvider} from './server.mjs';
import {createLocalAppsRuntime} from '../../scripts/agent-modules/local-apps.mjs';

const code=value=>error=>{assert.equal(error.code,value);return true;};
const until=async(fn)=>{const deadline=Date.now()+7000;while(!await fn()){assert.ok(Date.now()<deadline,'fixture readiness deadline');await new Promise(resolve=>setTimeout(resolve,15));}};
function store(){let value=null;return {async read(){return structuredClone(value);},async claim(next){value??=structuredClone(next);return structuredClone(value);},async compareAndSwap(revision,next){assert.equal(value.localRevision,revision);value=structuredClone(next);return structuredClone(value);}};}
async function fixture(t){
 const parent=await realpath(tmpdir()),directory=await mkdtemp(join(parent,'soty-widgets-http-'));let apps,widgets,objects,world,connect,handler,serial=0,launchGuard=true;
 const clients=[],runtimes=[],upstreams=[],sockets=new Set();
 const server=createServer((req,res)=>{if(apps?.handleRequest(req,res))return;if(req.url==='/api/connect/rpc'){void handler(req,res);return;}res.writeHead(404).end();});server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
 server.on('upgrade',(req,socket,head)=>{if(!apps.handleUpgrade(req,socket,head))socket.destroy();});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const port=server.address().port,origin=`http://127.0.0.1:${port}`;
 function open(){
  world=createWorldService({databasePath:join(directory,'world.sqlite'),projectId:'soty',withCurrentContacts:(actor,callback)=>connect.withCurrentContacts(actor,callback)});
  apps=createAppsService({databasePath:join(directory,'apps.sqlite'),appOriginTemplate:`http://{appId}.legacy.localhost:${port}`,namedAppZone:`http://named.localhost:${port}`,shellOrigins:[origin],
   actorActive:actor=>connect?.isActorActive(actor)===true,withCurrentContacts:(actor,callback)=>connect.withCurrentContacts(actor,callback),withAuthorityFence:callback=>world.withCommunityAuthorityFence(callback),
   activeCommunityIds:id=>world.activeCommunityIds(id),canAccessCommunity:(actor,id)=>world.canAccessCommunity(actor,id),isGroupAdmin:(actor,id)=>world.isGroupAdmin(actor,id),authenticateConnector:async()=>true,connectorCurrent:()=>true,
   checkWidgetLaunch:request=>launchGuard?widgets.checkLaunch(request):true});
  widgets=createCanvasWidgetsService({databasePath:join(directory,'widgets.sqlite'),projectId:'soty',actorActive:actor=>connect?.isActorActive(actor)===true,withActorAuthorityFence:(actor,callback)=>connect.withActorAuthorityFence(actor,callback),withAppSourceAuthority:(request,callback)=>apps.withWidgetSourceAuthority(request,callback)});
  objects=createCanvasObjectsService({databasePath:join(directory,'objects.sqlite'),projectId:'soty',actorActive:actor=>connect?.isActorActive(actor)===true,withActorAuthorityFence:(actor,callback)=>connect.withActorAuthorityFence(actor,callback),providers:[createCanvasWidgetProvider({widgets,withObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback)})]});
  connect=createConnectService({databasePath:join(directory,'connect.sqlite'),projectId:'soty',allowedOrigins:[origin],extensions:[world,apps,apps.sourcePreparationExtension,widgets,objects]});handler=createConnectHandler(connect);
 }
 open();
 const close=()=>{objects.close();widgets.close();apps.close();world.close();connect.close();};
 t.after(async()=>{for(const runtime of runtimes)runtime.stop();clients.forEach(client=>client.dispose());close();const stopped=new Promise(resolve=>server.close(resolve));sockets.forEach(socket=>socket.destroy());await stopped;for(const source of upstreams)await source.close();const actual=await realpath(directory);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-widgets-http-'));await rm(actual,{recursive:true,force:true,maxRetries:3,retryDelay:50});});
 function clientFor(storage){const client=createClientWithStorage({projectId:'soty',endpoint:origin+'/api/connect/rpc',fetch:(url,options)=>fetch(url,{...options,headers:{...options.headers,origin}})},storage);clients.push(client);return client;}
 async function principal(label){const storage=store(),client=clientFor(storage),account=await client.bootstrap(label);await client.extension('world.profile.get',{}, {expectedAccountId:account.accountId});return {storage,client,account};}
 const alice=await principal('Independent owner A'),bob=await principal('Independent owner B'),carol=await principal('Widget viewer');
 const call=(person,op,args={})=>person.client.extension(op,{...(/^(?:apps|canvas)\./u.test(op)?{expectedAccountId:person.account.accountId}:{}),...args},{expectedAccountId:person.account.accountId});
 async function upstream(label){const sockets=new Set(),server=createServer((req,res)=>{res.writeHead(200,{'content-type':'text/plain'});res.end(label+':'+req.url);});server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const source={port:server.address().port,async close(){const stopped=new Promise(resolve=>server.close(resolve));sockets.forEach(socket=>socket.destroy());await stopped;}};upstreams.push(source);return source;}
 async function ownedApp(person,label,granted=[]){
  const id={linkId:'link_'+label,hostDeviceId:'host_'+label,connectorId:'connector_'+label},source=await upstream(label),runtime=createLocalAppsRuntime({randomSecret:()=>randomBytes(32).toString('base64url'),digest:value=>createHash('sha256').update(value).digest('hex'),createWebSocket:url=>new WebSocket(url),httpRequest:request,encodeBase64:value=>Buffer.from(value).toString('base64'),decodeBase64:value=>Buffer.from(value,'base64')},{serverUrl:origin,identity:{...id,name:label},token:'isolated-fixture'});
  runtimes.push(runtime);runtime.start();await until(()=>runtime.status().connected);const claim=await runtime.claim();await call(person,'apps.claim',{hostDeviceId:id.hostDeviceId,connectorId:id.connectorId,claimCode:claim.claimCode});
  const app=(await call(person,'apps.register',{hostDeviceId:id.hostDeviceId,connectorId:id.connectorId,name:label,port:source.port,entryPath:'/',grants:{accountIds:granted,communityIds:[]}})).app;
  await until(async()=>((await call(person,'apps.list')).apps.find(value=>value.id===app.id)?.state)==='ready');return {app,id,source,runtime};
 }
 const A=await ownedApp(alice,'Application-A'),B=await ownedApp(bob,'Application-B',[alice.account.accountId,carol.account.accountId]);
 const widget=async(person,appId)=>(await call(person,'canvas.widgets.create',{requestId:'widget_'+(++serial),appId})).current;
 const object=async(person,widget)=>(await call(person,'canvas.objects.create',{requestId:'object_'+(++serial),binding:{providerId:'soty.apps',ref:widget.widgetId},presentation:{mask:{shape:'rect'}},visibility:{audience:'public',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}})).current;
 async function appHttp(person,widget){const launch=await call(person,'apps.launch',{appId:widget.appId,...widget.entry,instanceId:widget.widgetId}),url=new URL(launch.launchUrl);
  const http=(path,method='GET',body,cookie)=>new Promise((resolve,reject)=>{const req=request({hostname:'127.0.0.1',port,path,method,headers:{host:url.host,origin:url.origin,...(body?{'content-type':'application/json'}:{}),...(cookie?{cookie}:{})}},res=>{const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,text:Buffer.concat(chunks).toString()}));});req.on('error',reject);req.end(body);});
  const session=await http('/_soty/session','POST',JSON.stringify({ticket:url.hash.slice(1)}));assert.equal(session.status,200);return http('/','GET',undefined,session.headers['set-cookie'][0].split(';')[0]);
 }
 return {alice,bob,carol,A,B,call,widget,object,appHttp,upstream,setLaunchGuard(value){launchGuard=value;},get objects(){return objects;},async restart(){close();open();for(const person of [alice,bob,carol]){person.client.dispose();person.client=clientFor(person.storage);}await until(()=>A.runtime.status().connected&&B.runtime.status().connected);await until(async()=>((await call(bob,'apps.list')).apps[0]?.state)==='ready');}};
}

test('real HTTP: own and foreign app widgets use one provider and existing gateway; source race refuses mint until explicit refresh/restart',{timeout:35000},async t=>{
 const f=await fixture(t),own=await f.widget(f.alice,f.A.app.id),foreign=await f.widget(f.alice,f.B.app.id),ownObject=await f.object(f.alice,own),foreignObject=await f.object(f.alice,foreign);
 const first=await f.call(f.alice,'canvas.objects.project',{objectIds:[ownObject.objectId,foreignObject.objectId]});assert.equal(first.items.length,2);assert.equal(first.items.every(value=>value.view.kind==='widget'&&value.view.runtimeAllowed),true);assert.notEqual(first.items[0].view.app.ownerAccountId,first.items[1].view.app.ownerAccountId);
 assert.equal((await f.appHttp(f.alice,own)).text,'Application-A:/');assert.equal((await f.appHttp(f.carol,foreign)).text,'Application-B:/');
 await f.call(f.bob,'apps.update',{appId:f.B.app.id,grants:{accountIds:[f.alice.account.accountId],communityIds:[]}});assert.equal((await f.call(f.carol,'canvas.objects.project',{objectIds:[foreignObject.objectId]})).items[0].available,false);await assert.rejects(f.appHttp(f.carol,foreign),code('apps_access_denied'));
 const beforeRace=(await f.call(f.alice,'canvas.objects.project',{objectIds:[foreignObject.objectId]})).items[0].view;assert.equal(beforeRace.runtimeAllowed,true);
 const nextSource=await f.upstream('Application-B-new-source'),publication=await f.call(f.bob,'apps.publication.get',{appId:f.B.app.id});
 const prepared=await f.call(f.bob,'apps.source.prepare',{appId:f.B.app.id,expectedPolicyEpoch:publication.policyEpoch,expectedTargetRevision:publication.activeTargetRevision,source:{hostDeviceId:f.B.id.hostDeviceId,connectorId:f.B.id.connectorId,port:nextSource.port,entryPath:'/'}});
 await f.call(f.bob,'apps.source.promote',{appId:f.B.app.id,requestId:'promote_after_projection',preparationId:prepared.preparationId,expectedPolicyEpoch:prepared.expectedPolicyEpoch,expectedTargetRevision:prepared.expectedTargetRevision,launchPolicy:'restricted',listed:false});
 await assert.rejects(f.appHttp(f.alice,foreign),code('canvas_widget_needs_refresh'),'the launch hook must detect the source change after an earlier successful view');
 f.setLaunchGuard(false);await until(async()=>{try{return (await f.appHttp(f.alice,foreign)).text==='Application-B-new-source:/';}catch(error){if(['app_binding_pending','app_offline'].includes(error.code))return false;throw error;}});f.setLaunchGuard(true);await assert.rejects(f.appHttp(f.alice,foreign),code('canvas_widget_needs_refresh'),'the same real HTTP scenario must reject the stale pin once the guard is restored');
 const changed=(await f.call(f.alice,'canvas.objects.project',{objectIds:[foreignObject.objectId]})).items[0].view;assert.equal(changed.state,'needs-refresh');assert.equal(changed.runtimeAllowed,false);assert.deepEqual(changed.entry,beforeRace.entry);
 const refresh={widgetId:foreign.widgetId,expectedRevision:1,requestId:'explicit_source_refresh'},refreshed=await f.call(f.alice,'canvas.widgets.refresh',refresh);assert.equal(refreshed.current.revision,2);assert.equal(refreshed.current.source.revision,prepared.target.revision);
 await until(async()=>{try{return (await f.appHttp(f.alice,refreshed.current)).text==='Application-B-new-source:/';}catch(error){if(['app_binding_pending','app_offline'].includes(error.code))return false;throw error;}});
 assert.equal((await f.call(f.alice,'canvas.widgets.refresh',refresh)).replayed,true);await f.restart();assert.deepEqual((await f.call(f.alice,'canvas.widgets.get',{widgetId:foreign.widgetId})).widget,refreshed.current);assert.equal((await f.appHttp(f.alice,refreshed.current)).text,'Application-B-new-source:/');
 await f.call(f.alice,'canvas.widgets.remove',{widgetId:foreign.widgetId,expectedRevision:2,requestId:'remove_widget'});await assert.rejects(f.appHttp(f.alice,refreshed.current),code('canvas_widget_entry_changed'));
});
