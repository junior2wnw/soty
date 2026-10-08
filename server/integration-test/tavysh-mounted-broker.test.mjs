import test from 'node:test';import assert from 'node:assert/strict';
import {createServer,request} from 'node:http';import {randomBytes,createHash} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';import {tmpdir} from 'node:os';import {join,resolve,dirname,basename} from 'node:path';import {pathToFileURL} from 'node:url';
import {createExistingAppHostComposition} from '../existing-app-host-composition.js';
import {createTavyshDeviceStatusAdapter} from '../tavysh-device-read-adapter.js';
import {captureExternalApplications} from '../external-applications.js';
import {createTrustedReadonlyQueryAdapter} from '../../modules/capabilities/server/readonly-queries.mjs';
import {createHttpApp} from '../http-app.js';
import {createClientWithStorage} from '../../modules/connect/browser/client.mjs';
import {createLocalAppsRuntime} from '../../scripts/agent-modules/local-apps.mjs';
const nativeRoot=process.env.SOTY_TAVYSH_DEVICE_SOURCE_ROOT;
assert.ok(nativeRoot,'Explicit owned Native source is required; no descriptor-only or skipped Native pass.');
const {createNativeDeviceReadFixture}=await import(pathToFileURL(join(nativeRoot,'tests/source-fixtures/native-device-read.mjs')));
const memory=()=>{let value;return{async read(){return structuredClone(value??null);},async claim(next){value??=structuredClone(next);return structuredClone(value);},async compareAndSwap(revision,next){assert.equal(value.localRevision,revision);value=structuredClone(next);return structuredClone(value);}};};
async function until(check){const deadline=Date.now()+10000;do{if(await check())return;await new Promise(done=>setTimeout(done,10));}while(Date.now()<deadline);throw Error('fixture_timeout');}
function catalog({appId,resourceId,binding}){
  const text=maxLength=>({type:'string',maxLength}),count={type:'integer',minimum:0,maximum:512};
  const properties={resourceId:text(160),schema:text(100),status:{...text(30),enum:['draft','ready','archived']},revision:{type:'integer',minimum:1},versions:count,performances:count,publications:count,visibility:{...text(30),enum:['private','circle','unlisted','public']},readOnly:{type:'boolean',enum:[true]},publishes:{type:'boolean',enum:[false]},playsAudio:{type:'boolean',enum:[false]}};
  const capabilityId='app.'+appId+':tavysh.project.status';
  return{capabilityId,version:1,appId,title:'Состояние своего проекта Тавыша',description:'Два текущих Native device proofs; только метаданные, без медиаданных/записи/публикации.',visibility:'private',executionEnabled:true,inputSchema:{type:'object',properties:{},additionalProperties:false},outputSchema:{type:'object',properties,required:Object.keys(properties),additionalProperties:false},resources:[resourceId],effects:[],recipients:[resourceId],executionBinding:{kind:'registered',handler:capabilityId,version:1,binding}};
}
async function rootFixture(t){
  const folder=await mkdtemp(join(tmpdir(),'existing-app-host-')),dist=join(folder,'dist'),dataDir=join(folder,'data');assert.equal(dirname(resolve(folder)),resolve(tmpdir()));await mkdir(dist);await writeFile(join(dist,'index.html'),'<title>Synthetic Root</title>');
  let app,runtime;const sockets=new Set(),clients=[],source=createServer((_req,res)=>res.end('<title>Synthetic Native target</title>')),server=createServer((req,res)=>app?app(req,res):res.writeHead(503).end());
  for(const item of[source,server]){item.on('connection',s=>{sockets.add(s);s.once('close',()=>sockets.delete(s));});await new Promise(done=>item.listen(0,'127.0.0.1',done));}
  const origin='http://127.0.0.1:'+server.address().port,options={dataDir,connectOrigins:[origin],capabilityAudience:origin,appOriginTemplate:'http://{appId}.localhost:'+server.address().port,namedAppZone:'',discoveryOrigin:'',appHosting:{domainProfile:'separate-site'},universalAppsEnabled:true};
  const create=extra=>createHttpApp(dist,{...options,...extra});app=create();server.on('upgrade',(req,socket,head)=>{if(!app.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
  t.after(async()=>{runtime?.stop();clients.forEach(c=>c.dispose());sockets.forEach(s=>s.destroy());await app?.locals.closeServices();await Promise.all([source,server].map(item=>new Promise(done=>item.close(done))));assert.equal(dirname(resolve(folder)),resolve(tmpdir()));assert.match(basename(folder),/^existing-app-host-/);await rm(folder,{recursive:true,force:true,maxRetries:5,retryDelay:20});});
  const token=randomBytes(32).toString('base64url'),identity={linkId:'fixture_existing_apps_link_1234567890',hostDeviceId:'fixture_host',connectorId:'fixture_connector'};
  const registered=await fetch(origin+'/api/connectors/register',{method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+token},body:JSON.stringify({...identity,deviceId:identity.hostDeviceId,scope:'Dev',protocol:2,capabilities:['apps']})});assert.equal(registered.status,200);
  runtime=createLocalAppsRuntime({randomSecret:()=>randomBytes(32).toString('base64url'),digest:v=>createHash('sha256').update(v).digest('hex'),createWebSocket:url=>new WebSocket(url),httpRequest:request,encodeBase64:b=>Buffer.from(b).toString('base64'),decodeBase64:v=>Buffer.from(v,'base64')},{identity,token,serverUrl:origin});runtime.start();await until(()=>runtime.status().connected);
  async function signed(label){const client=createClientWithStorage({projectId:'soty',endpoint:origin+'/api/connect/rpc',fetch:(url,opt)=>fetch(url,{...opt,headers:{...opt.headers,origin}})},memory());clients.push(client);return{client,account:await client.bootstrap(label)};}
  const publisher=await signed('Synthetic app publisher'),reader=await signed('Synthetic Native owner caller'),claim=await runtime.claim();await publisher.client.extension('apps.claim',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,claimCode:claim.claimCode});
  const created=await publisher.client.extension('apps.register',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,name:'Tavysh existing Native fixture',port:source.address().port,entryPath:'/',grants:{accountIds:[reader.account.accountId],communityIds:[]}}),appId=created.app.id,target=created.universalRegistration.descriptor.app.source;
  const ready=()=>until(async()=>(await publisher.client.extension('apps.list')).apps.find(row=>row.id===appId)?.state==='ready');await ready();
  const principal=await reader.client.extension('access.principals.create',{expectedAccountId:reader.account.accountId,label:'Native explicitly approved reader'});
  return{origin,appId,target,publisher,reader,principal:principal.principal,async install(composition){await app.locals.closeServices();app=create(composition.httpOptions);await ready();return app.locals.capabilitiesService.external.contracts[0];},async credential(actor,capabilityId,resourceId){const p=actor===reader?principal.principal:(await actor.client.extension('access.principals.create',{expectedAccountId:actor.account.accountId,label:'No implicit Native role'})).principal;
    const grant=(await actor.client.extension('access.grants.issue',{expectedAccountId:actor.account.accountId,principalId:p.id,capabilities:[{capabilityId,version:1}],resources:[resourceId],effects:[],recipients:[resourceId],expiresAt:Date.now()+600000,allowDelegation:false,maxDepth:0,budget:{unit:'invocations',limit:10}})).grant;
    const token=(await actor.client.extension('access.credentials.issue',{expectedAccountId:actor.account.accountId,grantId:grant.id,audience:origin})).token;return{token,revoke:()=>actor.client.extension('access.grants.revoke',{expectedAccountId:actor.account.accountId,grantId:grant.id})};},async retarget(){const current=await publisher.client.extension('apps.publication.get',{appId});const prepared=await publisher.client.extension('apps.source.prepare',{appId,expectedPolicyEpoch:current.policyEpoch,expectedTargetRevision:current.activeTargetRevision,source:{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,port:source.address().port,entryPath:'/changed-target'}});return publisher.client.extension('apps.source.promote',{appId,preparationId:prepared.preparationId,requestId:'fixture-new-target',expectedPolicyEpoch:prepared.expectedPolicyEpoch,expectedTargetRevision:prepared.expectedTargetRevision,launchPolicy:'restricted',listed:false});},async query(credential,reference,id,input={}){const r=await fetch(origin+'/api/capabilities/v1/app-actions/query',{method:'POST',headers:{origin,'content-type':'application/json',authorization:'Bearer '+credential.token},body:JSON.stringify({reference,idempotencyKey:id,input})});return{status:r.status,body:await r.json()};}};
}

import {createTavyshNativeDeviceBroker} from '../tavysh-native-device-broker.js';
import {loadInstalledExistingApplications} from '../installed-existing-applications.js';
const {mountInstalledTavyshSourcePage}=await import(pathToFileURL(join(nativeRoot,'src/source-host/installed-page.js')));
function nativeDom(){const nodes=[];class Node{constructor(tag){this.tagName=tag;this.children=[];this.listeners=new Map();this.ownerDocument=doc;nodes.push(this);}append(...items){this.children.push(...items);}replaceChildren(...items){this.children=items;}setAttribute(){}addEventListener(name,fn){this.listeners.set(name,fn);}remove(){this.removed=true;}showModal(){this.open=true;}}const doc={createElement:tag=>new Node(tag)};doc.body=new Node('body');return{doc,button:name=>nodes.find(n=>n.tagName==='button'&&n.textContent===name&&!n.disabled&&!n.removed)};}
async function mounted(t){
 const native=await createNativeDeviceReadFixture(),root=await rootFixture(t),resourceId='app.'+root.appId+':native-device-status';
 const broker=createTavyshNativeDeviceBroker({nativeOrigin:'https://native.fixture.test',deviceId:native.identity.id,sourceRelease:native.sourceRelease});
 const sockets=new Set(),server=createServer((req,res)=>{void broker.handle(req,res).then(handled=>{if(!handled)res.writeHead(404).end();});});server.on('connection',s=>{sockets.add(s);s.once('close',()=>sockets.delete(s));});await new Promise(done=>server.listen(0,'127.0.0.1',done));
 const api='http://127.0.0.1:'+server.address().port,dom=nativeDom(),listeners=new Map(),pageWindow={location:{origin:'https://native.fixture.test',protocol:'https:'},addEventListener:(n,f)=>listeners.set(n,f),removeEventListener:n=>listeners.delete(n)};
 const page=await mountInstalledTavyshSourcePage({pageWindow,pageDocument:dom.doc,profileStorage:native.storage,fetchImpl:(path,init)=>fetch(api+path,{...init,headers:{...init.headers,origin:'https://native.fixture.test'}})});assert.equal(page.mounted,true);
 t.after(async()=>{page.close();broker.close();native.port.close();sockets.forEach(s=>s.destroy());await new Promise(done=>server.close(done));});
 const scope=native.scope({appId:root.appId,resourceId,target:{revision:root.target.revision,digest:root.target.digest},rootPrincipal:{accountId:root.reader.account.accountId,clientId:root.principal.clientId,principalId:root.principal.id}});
 const consentPromise=broker.requestConsent(scope);await until(()=>dom.button('Разрешить чтение'));
 const button=dom.button('Разрешить чтение');await button.listeners.get('click')({isTrusted:false});assert.equal(button.disabled,undefined);
 // Only the OWN synthetic Native UI fixture supplies a trusted human event.
 // This is a Source test, not a claim that a physical browser consent happened.
 await button.listeners.get('click')({isTrusted:true});const consent=await consentPromise;
 const bridge=await createTavyshDeviceStatusAdapter({consent,queryDevice:broker.readSigned});t.after(()=>bridge.close());
 const entry=catalog({appId:root.appId,resourceId,binding:bridge.binding});let factoryCalls=0;
 const composition=await loadInstalledExistingApplications({createRegistry:()=>{factoryCalls++;return[{appId:root.appId,target:{revision:root.target.revision,digest:root.target.digest},catalog:entry,adapter:bridge.adapter}];}});assert.equal(factoryCalls,1);
 const reference=await root.install(composition),credential=await root.credential(root.reader,entry.capabilityId,resourceId);
 return{native,root,entry,reference,credential,broker,page,api,query:id=>root.query(credential,reference,id)};
}
test('mounted Native page actual HTTP/P256/owner consent -> existing signed Root/Apps query, publisher denied',async t=>{
 const x=await mounted(t),profile=x.native.records.get('krug.profile.v2'),positive=await x.query('mounted-own-read');assert.equal(positive.status,200);assert.equal(positive.body.result.status,'ready');assert.equal(x.native.records.get('krug.profile.v2'),profile);assert.ok(!JSON.stringify(positive.body).includes('SYNTHETIC PRIVATE'));
 const publisher=await x.root.credential(x.root.publisher,x.entry.capabilityId,x.entry.resources[0]);const deny=await x.root.query(publisher,x.reference,'mounted-publisher');assert.equal(deny.status,403);assert.equal(Object.hasOwn(deny.body,'result'),false);
});
test('mounted Native profile epoch switch denies old Root grant and original profile is not recreated',async t=>{
 const x=await mounted(t);assert.equal((await x.query('mounted-native-positive')).status,200);x.native.persistence.write(x.native.profile,{scope:'other-account',replace:true});
 const deny=await x.query('mounted-native-scope-changed');assert.equal(deny.status,403);assert.equal(Object.hasOwn(deny.body,'result'),false);assert.equal(JSON.stringify(deny.body).includes(x.credential.token),false);
});
test('wrong Native origin and fresh wrong device proof cannot obtain a transport command',async t=>{
 const x=await mounted(t);let r=await fetch(x.api+'/api/source/device/v1/challenge',{method:'POST',headers:{origin:'https://other.test','content-type':'application/json'},body:'{}'});assert.equal(r.status,403);
 r=await fetch(x.api+'/api/source/device/v1/poll',{method:'POST',headers:{origin:'https://native.fixture.test','content-type':'application/json'},body:JSON.stringify({proof:{payload:{nonce:'x'.repeat(43)},device:{deviceId:'dev_'+'a'.repeat(32),publicJwk:{crv:'P-256',kty:'EC',x:'a'.repeat(43),y:'a'.repeat(43)},signature:'a'.repeat(86)}}})});assert.ok(r.status>=400);
});
