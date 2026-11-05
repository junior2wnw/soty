import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer,request as httpRequest} from 'node:http';
import {createHash,randomBytes,randomUUID} from 'node:crypto';
import {join} from 'node:path';
import WebSocket from 'ws';
import {createHttpApp} from '../http-app.js';
import {nativeHttpFixture,nativeIdentity,good} from './support/native-capability-http.mjs';
import {createLocalAppsRuntime} from '../../scripts/agent-modules/local-apps.mjs';

const id=prefix=>prefix+'_'+randomUUID().replaceAll('-','');
const policy=audience=>({audience,accountIds:[],communityIds:[]});
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(check,label){const end=Date.now()+15000;while(!await check()){assert.ok(Date.now()<end,label);await pause(30);}}
function realmGet(origin,realm,path='/'){
 return new Promise((resolve,reject)=>{
  const req=httpRequest({hostname:'127.0.0.1',port:new URL(origin).port,path,method:'GET',headers:{host:new URL(realm).host}},res=>{
   const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.once('error',reject);res.once('end',()=>resolve({status:res.statusCode,text:Buffer.concat(chunks).toString('utf8')}));
  });req.setTimeout(7000,()=>req.destroy(new Error('synthetic_realm_timeout')));req.once('error',reject);req.end();
 });
}

async function fixture(t){
 const f=await nativeHttpFixture(t,{appFactory:(dist,options)=>{
  const port=new URL(options.connectOrigins[0]).port;
  return createHttpApp(dist,{...options,allowNativeVaultMigration:true,allowCanvasPageSelectionMigration:true,
   appOriginTemplate:`http://{appId}.legacy.localhost:${port}`,namedAppZone:`http://named.localhost:${port}`});
 }});
 // The shared native fixture exposes HTTP only. Attach the normal host WS
 // upgrade path here, retaining its live app getter across isolated restart.
 f.server.on('upgrade',(req,socket,head)=>{if(!f.app.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
 const owner=nativeIdentity('Widget Page contract owner'),viewer=nativeIdentity('Widget Page contract viewer'),accounts=new Map();
 for(const actor of [owner,viewer]){accounts.set(actor,(await f.bootstrap(actor)).accountId);good(await f.call(actor,'world.profile.get'));}
 const call=(actor,op,args={})=>f.call(actor,op,{expectedAccountId:accounts.get(actor),...args});
 const mutate=(actor,op,args={})=>call(actor,op,{requestId:id('request'),...args});
 const wire=async(actor,op,args)=>{const signed=await f.proof(actor,op,{expectedAccountId:accounts.get(actor),...args});const res=await fetch(f.origin+'/api/connect/rpc',{method:'POST',headers:{origin:f.origin,'content-type':'application/json'},body:JSON.stringify({protocol:1,...signed})});return {status:res.status,body:await res.json()};};
 const canary='REAL_APP_WIDGET_PAGE_'+randomUUID(),sockets=new Set(),upstream=createServer((_req,res)=>{res.writeHead(200,{'content-type':'text/html; charset=utf-8'});res.end('<!doctype html><title>Widget contract</title><h1>'+canary+'</h1>');});
 upstream.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});await new Promise(resolve=>upstream.listen(0,'127.0.0.1',resolve));
 let runtime,stopped=false;
 const stop=async()=>{if(stopped)return;stopped=true;runtime?.stop();const closed=new Promise(resolve=>upstream.close(resolve));sockets.forEach(socket=>socket.destroy());await closed;};
 // Each test also calls stop in finally before nativeHttpFixture closes Root.
 t.after(stop);
 try{
  const identity={linkId:id('widget_page_link'),hostDeviceId:id('widget_page_host'),connectorId:id('widget_page_connector')},token=randomBytes(32).toString('base64url');
  const registration=await fetch(f.origin+'/api/connectors/register',{method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+token},body:JSON.stringify({linkId:identity.linkId,deviceId:identity.hostDeviceId,connectorId:identity.connectorId,scope:'Dev',protocol:2,capabilities:['apps']})});assert.equal((await registration.json()).ok,true);
  runtime=createLocalAppsRuntime({randomSecret:()=>randomBytes(32).toString('base64url'),digest:value=>createHash('sha256').update(value).digest('hex'),createWebSocket:url=>new WebSocket(url),httpRequest,
   encodeBase64:value=>Buffer.from(value).toString('base64'),decodeBase64:value=>Buffer.from(value,'base64')},{serverUrl:f.origin,identity:{...identity,name:'Synthetic Page connector'},token});runtime.start();await until(()=>runtime.status().connected,'the actual connector WS must authenticate');
  const claim=await runtime.claim();good(await call(owner,'apps.claim',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,claimCode:claim.claimCode}));
  const app=good(await call(owner,'apps.register',{hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId,name:'Real Page app widget',port:upstream.address().port,entryPath:'/'})).app;
  const ready=()=>until(async()=>good(await call(owner,'apps.list')).apps.some(value=>value.id===app.id&&value.state==='ready'),'owned registered Source must be ready, not merely have a live connector PID');await ready();
  const domains=good(await call(owner,'apps.domains.get',{appId:app.id})),alias=good(await mutate(owner,'apps.domains.claim',{appId:app.id,slug:'page-widget-'+randomBytes(6).toString('hex'),expectedDomainsRevision:domains.revision})).receipt;
  const publication=good(await call(owner,'apps.publication.get',{appId:app.id}));
  good(await mutate(owner,'apps.publication.update',{appId:app.id,expectedPolicyEpoch:publication.policyEpoch,expectedTargetRevision:publication.activeTargetRevision,launchPolicy:'anyone',listed:false,activeDomainIds:[alias.domainId],
   ...(publication.audience===null||publication.audience===undefined?{}:{audience:'link'}),exposureAck:{scope:'whole-port',targetRevision:publication.target.revision,targetDigest:publication.target.digest,profile:publication.target.profile}}));
  const widget=good(await mutate(owner,'canvas.widgets.create',{appId:app.id,entry:{domainId:alias.domainId,path:'/'}})).current;
  const makeObject=async audience=>good(await mutate(owner,'canvas.objects.create',{binding:{providerId:'soty.apps',ref:widget.widgetId},presentation:{mask:{shape:'rect'}},visibility:policy(audience),hidden:{owner:false,others:false},protection:{kind:'none'}})).current;
  const publicObject=await makeObject('public'),privateObject=await makeObject('private'),field=()=>call(owner,'world.field.get').then(good);
  const document={schema:'soty.field.v2',contexts:[{contextId:'private_fixture_context',title:'Own fixture canvas title',x:125,y:275}],shortcuts:[publicObject,privateObject].map((object,index)=>({shortcutId:'fixture_shortcut_'+index,entity:{kind:'object',id:object.objectId},contextId:'private_fixture_context',placement:{kind:'free',x:index*320,y:0,width:280,height:180}}))};
  good(await mutate(owner,'world.field.put',{expectedRevision:(await field()).revision,document}));
  const owned=good(await call(owner,'canvas.objects.project',{objectIds:[publicObject.objectId,privateObject.objectId]}));assert.equal(owned.items.every(item=>item.available&&item.view.kind==='widget'),true,'the same real provider resolves both widgets before publication');
  const origin=owned.items[0].view.entry.origin;assert.equal(owned.items[0].view.entry.domainId,alias.domainId);
  await until(async()=>{const value=await realmGet(f.origin,origin);return value.status===200&&value.text.includes(canary);},'the exact active public named alias must serve the real upstream before the Page oracle');
  const emptyArgs={requestId:id('empty_control'),expectedRevision:0,enabled:true,audience:'unlisted',selection:{schema:'soty.canvas-page-selection.v1',fieldRevision:(await field()).revision,items:[]}},empty=await wire(owner,'canvas.pages.put',emptyArgs);assert.equal(empty.status,200);const page=good(empty.body).current;assert.equal(page.revision,1,'same Root accepts empty unlisted publication');
  const pageArgs=(object,expectedRevision=1)=>({requestId:id('selected_widget_page'),expectedRevision,enabled:true,audience:'unlisted',selection:{schema:'soty.canvas-page-selection.v1',fieldRevision:documentRevision,items:[{shortcutId:document.shortcuts.find(s=>s.entity.id===object.objectId).shortcutId,entity:{kind:'object',id:object.objectId},expectedAccessRevision:object.accessRevision}]}}),documentRevision=(await field()).revision;
  const getPage=()=>call(owner,'canvas.pages.get').then(good),getObject=object=>call(owner,'canvas.objects.get',{objectId:object.objectId}).then(value=>good(value).object),getPublication=()=>call(owner,'apps.publication.get',{appId:app.id}).then(good);
  const receiptCount=requestId=>f.sql(join(f.directory,'canvas','pages.sqlite'),db=>db.prepare('SELECT count(*) n FROM canvas_page_commands WHERE account_id=? AND request_id=?').get(accounts.get(owner),requestId).n);
  return {...f,get app(){return f.app;},owner,viewer,app,widget,alias,publicObject,privateObject,call,mutate,wire,page,pageArgs,getPage,getObject,getPublication,receiptCount,field,ready,stop,canary,origin,pageUrl:'/api/canvas/pages/'+page.pageId};
 }catch(error){await stop();throw error;}
}

async function publishExpected(t,f,object){
 const before=(await f.getPage()).page,args=f.pageArgs(object),response=await f.wire(f.owner,'canvas.pages.put',args);
 if(!response.body.ok){
  const after=(await f.getPage()).page;assert.equal(response.status,400);assert.equal(response.body.error.code,'extension_unavailable','OLD-RED must be the diagnosed provider callback error, not a setup/auth/ACL failure');assert.deepEqual(after,before);assert.equal(f.receiptCount(args.requestId),0);
  t.diagnostic(JSON.stringify({phase:'READYRED-candidate',status:response.status,code:response.body.error.code,provider:'soty.apps',beforeRevision:before.revision,afterRevision:after.revision,receiptsForFailedRequest:0,objectAudience:object.visibility.audience}));
 }
 assert.equal(response.status,200,'a valid real app-widget selection must COMMIT through the signed Root; a void host callback must not become extension_unavailable');
 return {args,response:good(response.body)};
}

test('real public app-widget Page contract: signed HTTP selection COMMIT/CAS/replay/reopen preserves ACL and canonical geometry',{timeout:60000},async t=>{
 const f=await fixture(t);
 try{
  const acl=await f.getPublication(),objectBefore=await f.getObject(f.publicObject),fieldBefore=await f.field(),{args,response}=await publishExpected(t,f,f.publicObject);assert.equal(response.replayed,false);assert.equal(response.current.revision,2);assert.equal(f.receiptCount(args.requestId),1);
  const guest=await f.http(f.pageUrl);assert.equal(guest.status,200);assert.equal(guest.body.items.length,1);assert.equal(guest.body.items[0].entity.id,f.publicObject.objectId);assert.equal(guest.body.items[0].view.kind,'widget');assert.equal(guest.body.items[0].view.widgetId,f.widget.widgetId);assert.equal(guest.body.items[0].view.entry.domainId,f.alias.domainId);assert.equal(guest.body.items[0].view.runtimeAllowed,false,'anonymous metadata is not a signed runtime ticket');assert.equal(JSON.stringify(guest.body).includes(f.privateObject.objectId),false);
  assert.equal(guest.headers['cache-control'],'no-store');assert.equal((await realmGet(f.origin,guest.body.items[0].view.entry.origin)).text.includes(f.canary),true);
  assert.deepEqual(await f.getPublication(),acl);assert.deepEqual(await f.getObject(f.publicObject),objectBefore);assert.deepEqual(await f.field(),fieldBefore);
  const replay=good((await f.wire(f.owner,'canvas.pages.put',args)).body);assert.equal(replay.replayed,true);assert.deepEqual(replay.receipt,response.receipt);assert.equal(f.receiptCount(args.requestId),1);
  const stale=await f.wire(f.owner,'canvas.pages.put',{...args,requestId:id('stale_cas')});assert.equal(stale.body.ok,false);assert.equal(stale.body.error.code,'canvas_page_revision_conflict');assert.equal((await f.getPage()).page.revision,2);
  const conflict=await f.wire(f.owner,'canvas.pages.put',{...args,audience:'public'});assert.equal(conflict.body.ok,false);assert.equal(conflict.body.error.code,'canvas_page_request_conflict');assert.equal(f.receiptCount(args.requestId),1);
  await f.restart({enabled:true});await f.ready();const reopened=await f.http(f.pageUrl);assert.equal(reopened.status,200);assert.equal(reopened.body.viewRevision,guest.body.viewRevision);assert.equal(good((await f.wire(f.owner,'canvas.pages.put',args)).body).replayed,true);assert.deepEqual(await f.getPublication(),acl);assert.deepEqual(await f.getObject(f.publicObject),objectBefore);assert.deepEqual(await f.field(),fieldBefore);
  const current=await f.getPublication();good(await f.mutate(f.owner,'apps.publication.update',{appId:f.app.id,expectedPolicyEpoch:current.policyEpoch,expectedTargetRevision:current.activeTargetRevision,launchPolicy:'restricted',listed:false,activeDomainIds:[],...(current.audience===null||current.audience===undefined?{}:{audience:'private'})}));
  const denied=await f.http(f.pageUrl);assert.equal(denied.status,200);assert.equal(denied.body.items.length,0,'a Page/public Object must not retain a revoked public App Source grant');assert.equal(denied.body.field.shortcuts.length,0);
 }finally{await f.stop();}
});

test('real private app-widget Object remains private after successful Page selection of the same publicly accessible Source',{timeout:60000},async t=>{
 const f=await fixture(t);
 try{
  const acl=await f.getPublication(),before=await f.getObject(f.privateObject),fieldBefore=await f.field(),{args,response}=await publishExpected(t,f,f.privateObject);assert.equal(response.current.revision,2);
  const guest=await f.http(f.pageUrl);assert.equal(guest.status,200);assert.equal(guest.body.items.length,0);assert.equal(guest.body.field.shortcuts.length,0);for(const secret of [f.privateObject.objectId,f.widget.widgetId,f.app.id])assert.equal(JSON.stringify(guest.body).includes(secret),false);
  assert.equal(good(await f.call(f.viewer,'canvas.pages.read',{pageId:f.page.pageId})).items.length,0);assert.equal(good(await f.call(f.viewer,'canvas.objects.project',{objectIds:[f.privateObject.objectId]})).items[0].available,false);
  const owner=good(await f.call(f.owner,'canvas.pages.read',{pageId:f.page.pageId}));assert.equal(owner.items.length,1);assert.equal(owner.items[0].view.kind,'widget');assert.equal(owner.items[0].view.widgetId,f.widget.widgetId);
  assert.deepEqual(await f.getObject(f.privateObject),before);assert.equal(before.visibility.audience,'private');assert.deepEqual(await f.getPublication(),acl);assert.deepEqual(await f.field(),fieldBefore);
  await f.restart({enabled:true});await f.ready();assert.equal((await f.http(f.pageUrl)).body.items.length,0);assert.equal(good((await f.wire(f.owner,'canvas.pages.put',args)).body).replayed,true);assert.deepEqual(await f.getObject(f.privateObject),before);assert.deepEqual(await f.getPublication(),acl);assert.deepEqual(await f.field(),fieldBefore);
 }finally{await f.stop();}
});
