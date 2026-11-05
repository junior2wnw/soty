import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {mkdirSync,writeFileSync,readFileSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {markerPair,rawHttp,until} from './support/self-ip-root-fixture.mjs';
import {nativeHttpFixture,nativeIdentity,good} from './support/native-capability-http.mjs';
import {createHttpApp} from '../http-app.js';

test('built single-file connector 1.4.18 really probes and proxies the selected own IP, rather than localhost',{timeout:70000},async t=>{
 let child;t.after(async()=>{if(!child||child.exitCode!==null)return;const ended=new Promise(resolve=>child.once('exit',resolve));child.kill('SIGTERM');let timer;await Promise.race([ended,new Promise(resolve=>{timer=setTimeout(resolve,2000);})]);clearTimeout(timer);if(child.exitCode===null)child.kill('SIGKILL');});
 const source=await markerPair(t),lease=createServer();await new Promise(resolve=>lease.listen(0,'127.0.0.1',resolve));const localPort=lease.address().port;await new Promise(resolve=>lease.close(resolve));
 const f=await nativeHttpFixture(t,{appOrigins:true,appFactory:(dist,options)=>createHttpApp(dist,{...options,allowSelfIpTargets:true,localConnectorPort:localPort})});
 f.server.on('upgrade',(req,socket,head)=>{if(!f.app.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
 const runtimeDir=join(f.directory,'installed-runtime');mkdirSync(runtimeDir);writeFileSync(join(runtimeDir,'connector-config.json'),JSON.stringify({workspaceRoot:runtimeDir,allowedRoots:[runtimeDir]}));
 const bundle=fileURLToPath(new URL('../../public/agent/soty-connector.mjs',import.meta.url)),manifest=JSON.parse(readFileSync(new URL('../../public/agent/manifest.json',import.meta.url),'utf8'));
 assert.equal(manifest.version,'1.4.18');assert.equal(manifest.sha256,createHash('sha256').update(readFileSync(bundle)).digest('hex'),'actual spawned artifact matches built release hash');
 child=spawn(process.execPath,[bundle,'--port',String(localPort),'--scope','Dev'],{cwd:runtimeDir,windowsHide:true,stdio:['ignore','pipe','pipe'],env:{
  PATH:[dirname(process.execPath),...(process.env.SystemRoot?[join(process.env.SystemRoot,'System32')]:['/usr/bin','/bin'])].join(process.platform==='win32'?';':':'),
  ...(process.env.SystemRoot?{SystemRoot:process.env.SystemRoot}:{}),TEMP:f.directory,TMP:f.directory,USERPROFILE:f.directory,APPDATA:join(f.directory,'appdata'),LOCALAPPDATA:join(f.directory,'localappdata'),
  SOTY_CONNECTOR_DATA_DIR:runtimeDir,SOTY_CONNECTOR_SERVER_URL:f.origin,SOTY_CONNECTOR_LINK_ID:'installed_ip_link_012345678901234567890',SOTY_CONNECTOR_DEVICE_ID:'installed_ip_host',SOTY_CONNECTOR_DEVICE_NICK:'Isolated installed IPv4 fixture',
  SOTY_CONNECTOR_AUTO_UPDATE:'0',SOTY_CONNECTOR_MANAGED:'0',SOTY_CONNECTOR_UPDATE_URL:f.origin+'/agent/manifest.json',
 }});
 let outputBytes=0;child.stdout.on('data',bytes=>{outputBytes+=bytes.length;});child.stderr.on('data',bytes=>{outputBytes+=bytes.length;});
 const claim=await until(async()=>{assert.equal(child.exitCode,null,`isolated connector exited (output bytes=${outputBytes})`);try{const response=await fetch(`http://127.0.0.1:${localPort}/apps/claim`,{method:'POST',headers:{origin:f.origin,'content-type':'application/json'},body:'{}'});return response.status===200?response.json():false;}catch{return false;}},'built connector claim');
 const owner=nativeIdentity('Installed IP connector owner'),account=await f.bootstrap(owner);
 const call=(op,args={})=>f.call(owner,op,{expectedAccountId:account.accountId,...args});
 good(await call('apps.claim',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,claimCode:claim.claimCode}));
 const device=await until(async()=>good(await call('apps.devices')).devices.find(value=>value.connectorId===claim.connectorId&&value.sourceEndpoints?.state==='ready'),'built connector inventory');
 assert.ok(device.sourceEndpoints.addresses.some(value=>value.address===source.address));
 const registered=good(await call('apps.registerEndpoint',{requestId:'installed-self-ip-registration-001',hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,name:'Built connector IP proof',
  inventoryWitness:device.sourceEndpoints.inventoryWitness,source:{schema:'soty.app-http-source.v2',endpoint:{scope:'connector-self',scheme:'http',family:'IPv4',address:source.address},port:source.port,entryPath:'/identity'},grants:{accountIds:[],communityIds:[]}}));
 await until(async()=>good(await call('apps.inspect',{appId:registered.app.id})).source.binding.state==='bound');
 const launch=good(await call('apps.launch',{appId:registered.app.id})),url=new URL(launch.launchUrl),session=await rawHttp(f.server.address().port,'/_soty/session',{host:url.host,origin:url.origin,method:'POST',body:{ticket:url.hash.slice(1)}});
 assert.equal(session.status,200,session.text);const cookie=session.headers['set-cookie'][0].split(';')[0];
 assert.equal((await rawHttp(f.server.address().port,'/identity',{host:url.host,origin:url.origin,cookie})).text,'SELF_IP_ONLY');
 assert.equal(source.seen.filter(value=>value.marker==='LOOPBACK_ONLY').length,0);assert.ok(source.seen.some(value=>value.marker==='SELF_IP_ONLY'&&value.host===`${source.address}:${source.port}`));
});
