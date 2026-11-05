import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {mkdirSync,writeFileSync,readFileSync} from 'node:fs';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import WebSocket from 'ws';
import {markerPair,rawHttp} from './support/self-ip-root-fixture.mjs';
import {nativeHttpFixture,nativeIdentity,good} from './support/native-capability-http.mjs';
import {leaseLoopbackPort} from '../../modules/apps/test/support/scoped-connector-startup.mjs';
import {createHttpApp} from '../http-app.js';

const wait=ms=>new Promise(done=>setTimeout(done,ms));
async function until(read,label,ms=10000){const end=Date.now()+ms;for(;;){const value=await read();if(value)return value;assert.ok(Date.now()<end,label);await wait(500);}}

test('immutable deployed connector recovers self-IP after transient capabilities failure during real Root restart, keeping localhost usable',{timeout:65000},async t=>{
 let child;const sockets=new Set();
 t.after(async()=>{for(const ws of sockets)ws.terminate();if(!child||child.exitCode!==null||child.signalCode!==null)return;const ended=new Promise(done=>child.once('exit',done));child.kill('SIGTERM');let timer;await Promise.race([ended,new Promise(done=>{timer=setTimeout(done,2500);})]);clearTimeout(timer);if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await ended;}});
 const source=await markerPair(t),lease=await leaseLoopbackPort();t.after(()=>lease.release());
 const loop=createServer((_req,res)=>res.writeHead(200,{'content-type':'text/plain'}).end('LOOPBACK_STILL_AVAILABLE'));
 await new Promise(done=>loop.listen(0,'127.0.0.1',done));t.after(async()=>{loop.closeAllConnections();await new Promise(done=>loop.close(done));});
 const f=await nativeHttpFixture(t,{appOrigins:true,appFactory:(dist,options)=>createHttpApp(dist,{...options,allowSelfIpTargets:true,localConnectorPort:lease.port})});
 let failCaps=false,capabilityReads=0,failedCapabilityReads=0;
 f.setFront((req,res)=>{if(req.url==='/api/apps/runtime-capabilities'){capabilityReads++;if(failCaps){failedCapabilityReads++;res.writeHead(503,{'content-type':'application/json'}).end('{"ok":false}');return;}}f.app(req,res);});
 f.server.on('upgrade',(req,socket,head)=>{if(!f.app.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
 const runtimeDir=join(f.directory,'installed-runtime');mkdirSync(runtimeDir);writeFileSync(join(runtimeDir,'connector-config.json'),JSON.stringify({workspaceRoot:runtimeDir,allowedRoots:[runtimeDir]}));
 const bytes=readFileSync(new URL('../../public/agent/soty-connector.mjs',import.meta.url)),manifest=JSON.parse(readFileSync(new URL('../../public/agent/manifest.json',import.meta.url),'utf8'));
 const digest=createHash('sha256').update(bytes).digest('hex');assert.equal(manifest.sha256,digest);const installed=join(runtimeDir,'soty-connector.mjs');writeFileSync(installed,bytes);
 await lease.release();child=spawn(process.execPath,[installed,'--port',String(lease.port),'--scope','Dev'],{cwd:runtimeDir,windowsHide:true,stdio:['ignore','pipe','pipe'],env:{
  PATH:[dirname(process.execPath),...(process.env.SystemRoot?[join(process.env.SystemRoot,'System32')]:['/usr/bin','/bin'])].join(process.platform==='win32'?';':':'),...(process.env.SystemRoot?{SystemRoot:process.env.SystemRoot}:{}),
  TEMP:f.directory,TMP:f.directory,USERPROFILE:f.directory,APPDATA:join(f.directory,'appdata'),LOCALAPPDATA:join(f.directory,'localappdata'),SOTY_CONNECTOR_DATA_DIR:runtimeDir,SOTY_CONNECTOR_SERVER_URL:f.origin,
  SOTY_CONNECTOR_LINK_ID:'caps_recovery_link_012345678901234567890',SOTY_CONNECTOR_DEVICE_ID:'caps_recovery_host',SOTY_CONNECTOR_DEVICE_NICK:'Synthetic capability recovery',SOTY_CONNECTOR_AUTO_UPDATE:'0',SOTY_CONNECTOR_MANAGED:'0',SOTY_CONNECTOR_UPDATE_URL:f.origin+'/agent/manifest.json',
 }});let outputBytes=0;child.stdout.on('data',value=>{outputBytes+=value.length;});child.stderr.on('data',value=>{outputBytes+=value.length;});
 const claim=await until(async()=>{assert.equal(child.exitCode,null,`connector exited (output bytes=${outputBytes})`);try{const response=await fetch(`http://127.0.0.1:${lease.port}/apps/claim`,{method:'POST',headers:{origin:f.origin,'content-type':'application/json'},body:'{}'});return response.status===200?response.json():false;}catch{return false;}},'actual deployed connector claim');
 const owner=nativeIdentity('Capability recovery owner'),outsider=nativeIdentity('Capability recovery outsider'),recipient=nativeIdentity('Capability recovery former viewer'),account=await f.bootstrap(owner);await f.bootstrap(outsider);const recipientAccount=await f.bootstrap(recipient);
 const call=(op,args={})=>f.call(owner,op,{expectedAccountId:account.accountId,...args}),ids={hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId};
 good(await call('apps.claim',{...ids,claimCode:claim.claimCode}));
 const device=await until(async()=>good(await call('apps.devices')).devices.find(value=>value.connectorId===claim.connectorId&&value.sourceEndpoints?.state==='ready'),'initial real IP inventory');
 const ip=good(await call('apps.registerEndpoint',{requestId:'capability-recovery-self-ip-001',...ids,name:'Exact own-IP source',inventoryWitness:device.sourceEndpoints.inventoryWitness,
  source:{schema:'soty.app-http-source.v2',endpoint:{scope:'connector-self',scheme:'http',family:'IPv4',address:source.address},port:source.port,entryPath:'/identity'},grants:{accountIds:[],communityIds:[]}})).app;
 const local=good(await call('apps.register',{...ids,name:'Unrelated localhost source',port:loop.address().port,entryPath:'/',grants:{accountIds:[recipientAccount.accountId],communityIds:[]}})).app;
 const inspect=id=>call('apps.inspect',{appId:id}).then(good);
 await until(async()=>(await inspect(ip.id)).source.binding.state==='bound','initial bound self-IP');await until(async()=>(await inspect(local.id)).source.binding.state==='bound','initial bound localhost');
 async function session(id){const launch=good(await call('apps.launch',{appId:id})),url=new URL(launch.launchUrl),result=await rawHttp(f.server.address().port,'/_soty/session',{host:url.host,origin:url.origin,method:'POST',body:{ticket:url.hash.slice(1)}});assert.equal(result.status,200);return{host:url.host,origin:url.origin,cookie:result.headers['set-cookie'][0].split(';')[0]};}
 const initial=await inspect(ip.id);assert.equal((await rawHttp(f.server.address().port,'/identity',await session(ip.id))).text,'SELF_IP_ONLY');
 good(await f.call(recipient,'apps.launch',{appId:local.id}));
 failCaps=true;const beforeFailureReads=failedCapabilityReads;await f.restart({enabled:true});
 await until(async()=>failedCapabilityReads>beforeFailureReads&&good(await call('apps.devices')).devices.some(value=>value.connectorId===claim.connectorId&&value.sourceEndpoints?.state==='upgrade-required'),'reconnected fallback after a real Root restart');
 await until(async()=>(await inspect(local.id)).source.binding.state==='bound','localhost continues in fallback');assert.equal((await rawHttp(f.server.address().port,'/',await session(local.id))).text,'LOOPBACK_STILL_AVAILABLE');
 const unsupported=await call('apps.launch',{appId:ip.id});assert.equal(unsupported.ok,false);assert.equal(unsupported.error.code,'app_binding_pending');
 good(await call('apps.update',{appId:local.id,grants:{accountIds:[],communityIds:[]}}));assert.equal((await f.call(recipient,'apps.launch',{appId:local.id})).ok,false,'a revoked viewer is denied before capability recovery');
 failCaps=false;const readyAt=Date.now();
 await until(async()=>good(await call('apps.devices')).devices.some(value=>value.connectorId===claim.connectorId&&value.sourceEndpoints?.state==='ready'),`self-IP did not recover without manual restart (caps reads=${capabilityReads})`,27000);
 await until(async()=>(await inspect(ip.id)).source.binding.state==='bound','fresh exact source ACK');
 const after=await inspect(ip.id);for(const key of ['revision','digest','profile','endpoint','port','entryPath'])assert.deepEqual(after.source[key],initial.source[key]);assert.deepEqual(after.publication,initial.publication);
 assert.equal((await rawHttp(f.server.address().port,'/identity',await session(ip.id))).text,'SELF_IP_ONLY');assert.equal((await rawHttp(f.server.address().port,'/',await session(local.id))).text,'LOOPBACK_STILL_AVAILABLE');
 const auth=await session(ip.id),ws=new WebSocket(`ws://127.0.0.1:${f.server.address().port}/live`,{headers:{host:auth.host,origin:auth.origin,cookie:auth.cookie}});sockets.add(ws);ws.on('error',()=>{});
 assert.equal(await new Promise((yes,no)=>{const timer=setTimeout(()=>no(Error('fresh self-IP WS timeout')),5000);ws.once('message',bytes=>{clearTimeout(timer);yes(bytes.toString());});ws.once('error',error=>{clearTimeout(timer);no(error);});}),'SELF_IP_WS');
 assert.equal((await f.call(outsider,'apps.launch',{appId:ip.id})).ok,false,'capability refresh grants no outsider access');assert.equal(source.seen.some(row=>row.marker==='LOOPBACK_ONLY'),false,'a self-IP target never silently dials localhost');
 assert.equal((await f.call(recipient,'apps.launch',{appId:local.id})).ok,false,'fresh channel bindings never restore the revoked viewer');
 t.diagnostic(`actual immutable ${manifest.version} sha256=${digest}; caps failures=${failedCapabilityReads}; reads=${capabilityReads}; recovered after ${Date.now()-readyAt}ms without connector restart or reclaim`);
});
