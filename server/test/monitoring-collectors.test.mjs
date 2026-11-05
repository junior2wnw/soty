import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer,request} from 'node:http';
import {createHash,randomBytes} from 'node:crypto';
import {readFileSync,existsSync} from 'node:fs';
import {mkdtemp,rm,readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve,basename} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createMonitoringCollectors,readMonitoringConfiguration} from '../monitoring-collectors.js';
import {createMonitoringService} from '../../modules/monitoring/server/index.mjs';
import {createHttpApp} from '../http-app.js';
import {createLocalAppsRuntime} from '../../scripts/agent-modules/local-apps.mjs';
import {nativeHttpFixture,nativeIdentity,good} from './support/native-capability-http.mjs';

const MINUTE=60000,BASE=60_000_000,PLATFORM={kind:'platform'};
const A={kind:'app',appId:'app-'+'a'.repeat(32)},B={kind:'app',appId:'app-'+'b'.repeat(32)};
const SENTINEL='PRIVATE_MONITORING_SENTINEL_6f92a75d';
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(read,label){const deadline=Date.now()+5000;for(;;){const value=await read();if(value)return value;assert.ok(Date.now()<deadline,label);await pause(5);}}
function raw(port,path='/',{headers={},method='GET',body}={}){
 return new Promise((resolveResult,reject)=>{const call=request({hostname:'127.0.0.1',port,path,method,headers},response=>{
  const chunks=[];response.on('data',chunk=>chunks.push(chunk));response.once('error',reject);response.once('end',()=>resolveResult({status:response.statusCode,headers:response.headers,text:Buffer.concat(chunks).toString()}));
 });call.once('error',reject);call.end(body);});
}

// Real HTTP and real SQLite. Only the trusted host clock/source callbacks and
// identity port are controlled here; actual signed Root authority is below.
async function fixture(t,{factory=createMonitoringCollectors,mode='writer',samplePorts={}}={}){
 const directory=await mkdtemp(join(tmpdir(),'soty-monitoring-collectors-')),file=join(directory,'monitoring.sqlite');
 const actor=Object.freeze({accountId:'collector-owner',deviceId:'collector-device'}),issued=new WeakSet([actor]);
 let wall=BASE,mono=0,service,collectors,failCommit=false,closedResponses=0;const steps=[],sockets=new Set();
 const options={databasePath:file,projectId:'soty',mode,clock:()=>wall,currentActor:value=>issued.has(value),
  withAuthority({actor:current},consume){assert.ok(issued.has(current),'only this host-issued identity enters');return consume();},
  beforeCollectionChange:scope=>collectors.flushScope(scope)};
 service=createMonitoringService(options);
 const module={collectionEnabled:scope=>service.collectionEnabled(scope),observeBatch:args=>{if(failCommit)throw Error('DO_NOT_PERSIST_'+SENTINEL);return service.observeBatch(args);}};
 collectors=factory({module,mode,clock:()=>wall,monotonic:()=>mono,...samplePorts});
 const server=createServer((req,res)=>{const step=steps.shift();assert.ok(step,'each request has an explicit trusted host response');wall=step.at;mono+=1000;
  res.once('close',()=>closedResponses++);collectors.middleware(req,res,()=>{if(step.scope?.kind==='app')collectors.tagAppResponse(res,step.scope.appId);mono+=step.duration??10;
   req.resume();if(step.abort)res.destroy();else res.writeHead(step.status??200,{'content-type':'text/plain'}).end('HOST_RESPONSE');});
 });server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});await new Promise(done=>server.listen(0,'127.0.0.1',done));
 t.after(async()=>{collectors.close();service.close();sockets.forEach(socket=>socket.destroy());await new Promise(done=>server.close(done));assert.equal(dirname(resolve(directory)),resolve(tmpdir()));assert.match(basename(directory),/^soty-monitoring-collectors-/u);await rm(directory,{recursive:true,force:true,maxRetries:3,retryDelay:30});});
 const call=(op,args={},scope=A)=>service.execute({op,actor,args:{expectedAccountId:actor.accountId,scope,...args}});
 return{directory,file,collectors,call,get service(){return service;},setTime(value){wall=value;},setMono(value){mono=value;},fail(value){failCommit=value;},
  async send(step,extra={}){const count=closedResponses;steps.push({at:wall,scope:A,...step});let result,error;try{result=await raw(server.address().port,'/?private='+SENTINEL,{headers:{authorization:'Bearer '+SENTINEL,cookie:'private='+SENTINEL,'x-monitoring-app-id':B.appId,...extra.headers},...extra});}catch(reason){error=reason;}
   await until(()=>closedResponses===count+1,'actual HTTP response close');if(error)throw error;return result;},
  query(metricIds,scope=A,from=BASE,to=BASE+MINUTE,maxPoints=1){return call('monitoring.query',{metricIds,from,to,maxPoints},scope).series;},
  reopen(){collectors.flush();service.close();service=createMonitoringService({...options,mode:'reader'});return service;},
  sql(read){const db=new DatabaseSync(file,{readOnly:true});try{return read(db);}finally{db.close();}},
 };
}

async function requestOracle(t,factory=createMonitoringCollectors){
 const f=await fixture(t,{factory});
 const rawSamples=[{at:BASE+MINUTE-1,duration:10,status:200},{at:BASE+MINUTE+1,duration:30,status:503},{at:BASE+MINUTE+2,duration:60,status:200},{at:BASE+MINUTE+3,duration:90,status:200}];
 for(const step of rawSamples)assert.equal((await f.send(step,{method:'POST',body:SENTINEL})).status,step.status);
 assert.equal(f.collectors.flush(),true);f.setTime(BASE+3*MINUTE);
 const query=(metricIds,scope=A)=>f.query(metricIds,scope,BASE,BASE+3*MINUTE,3);
 const [counts,errors,durations]=query(['http.requests','http.errors','http.duration']);
 assert.deepEqual(query(['http.requests'],PLATFORM)[0].points.map(point=>point.value),[1,3,null],'each actual response counts exactly once across finish plus close, in its true minute');
 assert.deepEqual(counts.points.map(point=>point.value),[1,3,null],'each actual response counts exactly once across finish plus close, in its true minute');
 assert.deepEqual(errors.points.map(point=>point.value),[0,1,null],'an observed successful zero is different from a missing interval');
 assert.deepEqual(durations.points.map(point=>point.value),[10,60,null]);
 const coarse=f.query(['http.duration'],A,BASE,BASE+3*MINUTE,1)[0];
 assert.equal(coarse.points[0].value,47.5,'the four raw durations total 190ms: weighting must use samples, not minute means');assert.equal(coarse.points[0].samples,4);
 assert.equal(query(['http.requests'],B)[0].points.every(point=>point.value===null),true,'request header/query cannot select the app scope');
 const rows=f.sql(db=>db.prepare("SELECT samples,sum FROM monitoring_buckets WHERE scope_key=? AND metric_id='http.duration' ORDER BY bucket_at").all('app:'+A.appId));
 assert.deepEqual(rows.map(row=>[row.samples,row.sum]),[[1,10],[3,180]],'independent durable SQL check, not two paths through the same query');
 f.reopen();assert.deepEqual(query(['http.requests','http.errors','http.duration']),[counts,errors,durations]);
 for(const name of [fileName(f.file),fileName(f.file)+'-wal',fileName(f.file)+'-shm']){const target=join(f.directory,name);if(existsSync(target)){const bytes=await readFile(target);assert.equal(bytes.includes(Buffer.from(SENTINEL)),false,'no URL/header/body/exception sentinel persists');assert.equal(bytes.includes(Buffer.from(Buffer.from(SENTINEL).toString('base64'))),false);}}
}
const fileName=file=>basename(file);
test('actual HTTP → durable minute buckets/reopen: count, error, weighted duration, trusted app tags and no request PII',{timeout:15000},t=>requestOracle(t));

async function changedCollector(search,replacement){
 const location=new URL('../monitoring-collectors.js',import.meta.url);let code=readFileSync(location,'utf8');assert.equal(code.split(search).length,2,'mutant changes one exact existing control');
 code=code.replace(search,replacement).replace(/from '([^']+)'/gu,(whole,specifier)=>specifier.startsWith('.')?`from '${new URL(specifier,location).href}'`:whole);
 return(await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'))).createMonitoringCollectors;
}
test('negative count control: retaining both finish/close listeners without the once guard fails the SAME actual HTTP oracle',{timeout:15000},async t=>{
 const broken=await changedCollector("if(finished)return;finished=true;res.off('finish',finish);res.off('close',finish);",'finished=true;');
 await assert.rejects(requestOracle(t,broken),/each actual response counts exactly once/u);
});
test('negative error control: dropping HTTP failure classification fails the SAME actual HTTP oracle',{timeout:15000},async t=>{
 const broken=await changedCollector('(!res.writableFinished||res.statusCode>=400)?1:0','0');await assert.rejects(requestOracle(t,broken),/observed successful zero/u);
});

test('M05 boundary: pre-OFF work persists; OFF events never reappear after ON before flush; app B continues',{timeout:15000},async t=>{
 const f=await fixture(t);await f.send({at:BASE+1});
 f.call('monitoring.config.put',{requestId:'turn-A-off',expectedRevision:0,enabled:false});
 assert.equal(f.query(['http.requests'])[0].points[0].value,1,'disable must commit the accepted pre-OFF buffer before acknowledging the policy');
 await f.send({at:BASE+2});await f.send({at:BASE+3,scope:B});
 f.call('monitoring.config.put',{requestId:'turn-A-on',expectedRevision:1,enabled:true});f.collectors.flush();
 assert.equal(f.query(['http.requests'])[0].points[0].value,1,'an OFF request must not be replayed by a later enabled flush');assert.equal(f.query(['http.requests'],B)[0].points[0].value,1);
 f.call('monitoring.config.put',{requestId:'turn-A-off-again',expectedRevision:2,enabled:false});f.reopen();assert.equal(f.call('monitoring.describe').config.enabled,false);assert.equal(f.call('monitoring.describe',{},B).config.enabled,true);
});
test('an unconfirmed pre-disable flush cannot ACK disabled policy or lose the buffered event',{timeout:15000},async t=>{
 const f=await fixture(t);await f.send({});f.fail(true);
 assert.throws(()=>f.call('monitoring.config.put',{requestId:'failed-off',expectedRevision:0,enabled:false}),{code:'monitoring_collection_flush_unconfirmed'});
 assert.equal(f.call('monitoring.describe').config.enabled,true);f.fail(false);
 f.call('monitoring.config.put',{requestId:'failed-off',expectedRevision:0,enabled:false});assert.equal(f.query(['http.requests'])[0].points[0].value,1);assert.equal(f.call('monitoring.describe').config.enabled,false);
});
test('observer COMMIT failure never breaks the actual HTTP response; retry preserves exactly one accepted observation',{timeout:15000},async t=>{
 const f=await fixture(t);f.fail(true);assert.equal((await f.send({status:200})).status,200);assert.equal(f.collectors.flush(),false);assert.equal(f.collectors.status().failures,1);assert.equal(f.query(['http.requests'])[0].points[0].value,null);
 assert.equal(JSON.stringify(f.collectors.status()).includes(SENTINEL),false);f.fail(false);assert.equal(f.collectors.flush(),true);assert.equal(f.query(['http.requests'])[0].points[0].value,1);f.collectors.flush();assert.equal(f.query(['http.requests'])[0].points[0].value,1);
});
test('aborted actual HTTP is one failed request, not an observed successful zero',{timeout:15000},async t=>{
 const f=await fixture(t);await assert.rejects(f.send({abort:true}));f.collectors.flush();assert.equal(f.query(['http.requests'])[0].points[0].value,1);assert.equal(f.query(['http.errors'])[0].points[0].value,1);
});

test('trusted status samples preserve real zero, counter deltas and inference final outcomes without raw event fields',{timeout:15000},async t=>{
 let cpu={user:0,system:0},push={intakeDropped:5,recipientFailures:2},caps={succeeded:7,failed:3};
 const f=await fixture(t,{samplePorts:{runtime:{cpuUsage:()=>({...cpu}),memoryUsage:()=>({rss:1024}),uptime:()=>10},parallelism:()=>2,
  liveStatus:()=>({scopes:1,waiting:0}),callStatus:()=>({calls:0,ownedSockets:0,pendingSockets:0}),connectorStatus:()=>({online:1}),pushStatus:()=>({counters:{...push}}),capabilityStatus:()=>({...caps})}});
 f.setMono(1000);f.collectors.sample();cpu={user:500000,system:0};push={intakeDropped:7,recipientFailures:2};caps={succeeded:10,failed:4};f.setMono(2000);f.collectors.sample();
 f.collectors.inference({event:'inference_attempt_failed',outcome:'failed',durationMs:100,body:SENTINEL});
 f.collectors.inference({event:'inference_request_finished',outcome:'succeeded',durationMs:40,firstOutputMs:10,prompt:SENTINEL});
 f.collectors.inference({event:'inference_request_finished',outcome:'failed',durationMs:80,firstOutputMs:null,error:SENTINEL});f.collectors.flush();
 const byId=ids=>Object.fromEntries(f.query(ids,PLATFORM).map(series=>[series.metricId,series.points[0]]));
 assert.deepEqual(Object.fromEntries(Object.entries(byId(['process.cpu','live.waiters','calls.active','push.failures'])).map(([id,p])=>[id,p.value])),{'process.cpu':25,'live.waiters':0,'calls.active':0,'push.failures':0});
 assert.deepEqual(Object.fromEntries(Object.entries(byId(['inference.requests','inference.errors','inference.duration','inference.firstoutput'])).map(([id,p])=>[id,p.value])),{'inference.requests':2,'inference.errors':1,'inference.duration':60,'inference.firstoutput':10});
 assert.equal(byId(['capabilities.succeeded'])['capabilities.succeeded'].value,3);assert.equal(byId(['capabilities.failed'])['capabilities.failed'].value,1);assert.equal(f.query(['storage.available'],PLATFORM)[0].points[0].value,null,'an unsupported collector never invents availability');
 f.setTime(BASE+MINUTE);assert.equal(f.query(['calls.active'],PLATFORM,BASE,BASE+2*MINUTE,2)[0].points[1].value,null,'no sampling in the next minute is a gap, not a new zero');
 assert.equal(JSON.stringify(f.sql(db=>db.prepare('SELECT * FROM monitoring_buckets').all())).includes(SENTINEL),false);
});

async function rootFixture(t,mode='writer'){
 let operatorEnv={};const f=await nativeHttpFixture(t,{enabled:false,notesVersion:1,capabilitiesVersion:1,appOrigins:true,appFactory:(dist,options)=>createHttpApp(dist,{...options,env:operatorEnv,monitoring:{storageMode:mode}})});
 const owner=nativeIdentity('Collector actual signed owner'),other=nativeIdentity('Collector actual signed outsider'),account=await f.bootstrap(owner),outside=await f.bootstrap(other);
 return{...f,get app(){return f.app;},owner,other,account,outside,own:(op,args={})=>f.call(owner,op,{expectedAccountId:account.accountId,...args}),
  async restartAsInvitationAdmin(){operatorEnv={SOTY_PLATFORM_ADMIN_ACCOUNT_IDS:account.accountId};await f.restart({enabled:false});}};
}
test('actual Root reader stays fileless and default platform policy never borrows sign-in or invitation rights',{timeout:20000},async t=>{
 const f=await rootFixture(t,'reader'),file=join(f.directory,'monitoring','monitoring.sqlite');assert.equal(existsSync(file),false);
 assert.equal((await raw(f.server.address().port,'/')).status,200);f.app.locals.monitoringCollectors.sample();assert.equal(f.app.locals.monitoringCollectors.status().running,false);assert.equal(existsSync(file),false);
 const denied=await f.own('monitoring.describe',{scope:PLATFORM});assert.equal(denied.ok,false);assert.equal(denied.error.code,'monitoring_forbidden');
 await f.restartAsInvitationAdmin();assert.equal(good(await f.call(f.owner,'platform.admission.get',{})).manageAllowed,true,'the original signed owner really is an invitation admin in this control');
 const stillDenied=await f.own('monitoring.describe',{scope:PLATFORM});assert.equal(stillDenied.ok,false);assert.equal(stillDenied.error.code,'monitoring_forbidden');assert.equal(existsSync(file),false);
 assert.deepEqual(readMonitoringConfiguration({}),{storageMode:'reader',platformPolicy:{readAccountIds:[],configureAccountIds:[],adminAccountIds:[]}});
 assert.throws(()=>readMonitoringConfiguration({SOTY_MONITORING_MODE:'writer',SOTY_MONITORING_ADMIN_ACCOUNT_IDS:'*'}),{code:'monitoring_host_configuration_invalid'});
});

test('actual signed Root and Apps host classify two independent live Source responses; off A leaves B, queries and persisted history intact',{timeout:30000},async t=>{
 const f=await rootFixture(t),sources=[createServer((_req,res)=>res.end('SOURCE_A')),createServer((_req,res)=>res.end('SOURCE_B'))];let runtime;const peers=new Set();
 for(const source of sources){source.on('connection',socket=>{peers.add(socket);socket.once('close',()=>peers.delete(socket));});await new Promise(done=>source.listen(0,'127.0.0.1',done));}
 t.after(async()=>{runtime?.stop();peers.forEach(socket=>socket.destroy());await Promise.all(sources.map(source=>new Promise(done=>source.close(done))));});
 f.server.on('upgrade',(req,socket,head)=>{if(!f.app.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
 const identity={linkId:'collector_root_link_01234567890123456789',hostDeviceId:'collector-host',connectorId:'collector-connector'},token=randomBytes(32).toString('base64url');
 good((await f.http('/api/connectors/register',{method:'POST',token,body:JSON.stringify({linkId:identity.linkId,deviceId:identity.hostDeviceId,connectorId:identity.connectorId,scope:'Dev',protocol:2,capabilities:['apps']})})).body);
 runtime=createLocalAppsRuntime({randomSecret:()=>randomBytes(32).toString('base64url'),digest:value=>createHash('sha256').update(value).digest('hex'),createWebSocket:url=>new globalThis.WebSocket(url),httpRequest:request,
  encodeBase64:bytes=>Buffer.from(bytes).toString('base64'),decodeBase64:value=>Buffer.from(value,'base64')},{identity,token,serverUrl:f.origin});runtime.start();await until(()=>runtime.status().connected,'real Root connector connected');
 const claim=await runtime.claim(),ids={hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId};good(await f.own('apps.claim',{...ids,claimCode:claim.claimCode}));
 const apps=[];for(const [index,source]of sources.entries())apps.push(good(await f.own('apps.register',{...ids,name:'Collector Source '+index,port:source.address().port,entryPath:'/'})).app);
 const session=async appId=>{await until(async()=>good(await f.own('apps.inspect',{appId})).source.binding.state==='bound','actual Source admitted');const launched=good(await f.own('apps.launch',{appId})),url=new URL(launched.launchUrl);
  const response=await raw(f.server.address().port,'/_soty/session',{method:'POST',headers:{host:url.host,origin:url.origin,'content-type':'application/json'},body:JSON.stringify({ticket:url.hash.slice(1)})});assert.equal(response.status,200,response.text);return{host:url.host,cookie:response.headers['set-cookie'][0].split(';')[0]};};
 const sessions=[];for(const app of apps)sessions.push(await session(app.id));f.app.locals.monitoringCollectors.flush();
 const scope=appId=>({kind:'app',appId}),describe=async id=>good(await f.own('monitoring.describe',{scope:scope(id)}));
 const counts=async id=>(await describe(id)).metrics.find(metric=>metric.id==='http.requests').current??0;
 const before=await Promise.all(apps.map(app=>counts(app.id)));
 for(const [index,headers]of sessions.entries()){const response=await raw(f.server.address().port,'/?secret='+SENTINEL,{headers:{...headers,'x-monitoring-app-id':apps[1-index].id,'x-monitoring-scope':'platform'}});assert.equal(response.status,200);assert.equal(response.text,'SOURCE_'+(index?'B':'A'));}
 f.app.locals.monitoringCollectors.flush();for(const [index,app]of apps.entries())assert.equal(await counts(app.id),before[index]+1,'trusted Apps admission, not caller labels, determines the durable scope');
 const first=await describe(apps[0].id);good(await f.own('monitoring.config.put',{scope:scope(apps[0].id),requestId:'root-A-disabled',expectedRevision:first.config.revision,enabled:false}));const atOff=await Promise.all(apps.map(app=>counts(app.id)));
 for(const headers of sessions)assert.equal((await raw(f.server.address().port,'/',{headers})).status,200,'monitoring off never disables the app');f.app.locals.monitoringCollectors.flush();
 assert.equal(await counts(apps[0].id),atOff[0]);assert.equal(await counts(apps[1].id),atOff[1]+1);
 const denied=await f.call(f.other,'monitoring.describe',{expectedAccountId:f.outside.accountId,scope:scope(apps[1].id)});assert.equal(denied.ok,false);assert.equal(denied.error.code,'monitoring_forbidden');
 const deniedPlatform=await f.own('monitoring.describe',{scope:PLATFORM});assert.equal(deniedPlatform.ok,false);
 const file=join(f.directory,'monitoring','monitoring.sqlite'),durable=await counts(apps[1].id);runtime.stop();await f.restart({enabled:false});assert.equal(await counts(apps[1].id),durable);assert.equal((await describe(apps[0].id)).config.enabled,false);assert.equal((await describe(apps[1].id)).config.enabled,true);
 const privateInputs=[SENTINEL,token,...sessions.map(session=>session.cookie.split('=')[1]),f.owner.label,f.other.label];
 for(const suffix of ['', '-wal'])if(existsSync(file+suffix)){const bytes=await readFile(file+suffix);for(const value of privateInputs)assert.equal(bytes.includes(Buffer.from(value)),false,'Root monitoring persists neither query sentinel, actual session/connector credentials nor person labels');}
});
