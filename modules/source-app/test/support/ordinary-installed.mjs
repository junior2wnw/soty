import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import { createServer as createTlsServer } from 'node:https';
import { randomBytes, generateKeyPairSync } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHttpApp } from '../../../../server/http-app.js';
import { createClientWithStorage } from '../../../connect/browser/client.mjs';
import { runtimeTargetDigest } from '../../../apps/server/schema.mjs';
import { STANDARD_SELECTED_SOURCE_V2 } from '../../server/standard-profile.mjs';
import { createOrdinaryAppServer } from '../../examples/ordinary-app/index.mjs';

const random = () => randomBytes(32).toString('base64url'), pause = ms => new Promise(done => setTimeout(done, ms));
async function until(check, ms = 20000) { const end = Date.now()+ms; while(Date.now()<end) { const value = await check(); if(value)return value; await pause(40); } throw new Error('ordinary_installed_timeout'); }
export async function freePort() { const server=createServer(); await new Promise(done=>server.listen(0,'127.0.0.1',done)); const port=server.address().port; await new Promise(done=>server.close(done)); return port; }
const vault = () => { let value; return { async read(){return structuredClone(value??null);},async claim(next){value??=structuredClone(next);return structuredClone(value);},async compareAndSwap(revision,next){assert.equal(value.localRevision,revision);value=structuredClone(next);return structuredClone(value);} }; };
export function wireHttp(port,path,{host,cookie,origin,method,body,fields,headers={}}={}) {
  return new Promise((resolve,reject)=>{const bytes=fields?new URLSearchParams(fields).toString():body===undefined?null:Buffer.isBuffer(body)?body:JSON.stringify(body);
    const request=httpRequest({hostname:'127.0.0.1',port,path,method:method||(bytes===null?'GET':'POST'),agent:false,headers:{...(host?{host}:{}),...(cookie?{cookie}:{}),...(origin?{origin}:{}),
      ...(bytes!==null?{'content-type':fields?'application/x-www-form-urlencoded':'application/json','content-length':Buffer.byteLength(bytes)}:{}),...headers},signal:AbortSignal.timeout(10000)},response=>{
      const parts=[];let size=0;response.on('data',part=>{size+=part.length;if(size>4194304)response.destroy();else parts.push(part);});response.on('error',reject);response.on('end',()=>{
        const text=Buffer.concat(parts).toString('utf8');let value;try{value=JSON.parse(text);}catch{}resolve({status:response.statusCode,headers:response.headers,text,value});});});request.on('error',reject);request.end(bytes);});
}
/** Temp installed connector HTTP/WS + actual Root Apps/Human dispatcher. No
 * controlled Source authority oracle; all Source root reads traverse channel. */
export async function createOrdinaryInstalledFixture(t, options = {}) {
  const directory=await mkdtemp(join(tmpdir(),'soty-ordinary-installed-')),runtimeDir=join(directory,'runtime'),dataDir=join(directory,'root'),dist=options.distDir??join(directory,'dist');
  await mkdir(runtimeDir);if(!options.distDir){await mkdir(dist);await writeFile(join(dist,'index.html'),'<!doctype html><title>Synthetic signed Root consent</title>');}
  const connectorPort=await freePort(),appTlsPort=options.appPort??await freePort(),realms=[],clients=[],sockets=new Set(),jars=new Map(),oauthCounts={};let root,child,outputBytes=0;
  const handle=(req,res)=>{const route=req.url?.split('?')[0];if(route?.startsWith('/human-identity/')){const kind=['userinfo','token','jwks'].find(name=>route==='/human-identity/'+name)??'other';
    res.once('finish',()=>{const name=kind+':'+res.statusCode;oauthCounts[name]=(oauthCounts[name]??0)+1;});}return root?root(req,res):res.writeHead(503).end();};
  const server=options.backendTls?createTlsServer(options.backendTls,handle):createServer(handle);server.on('connection',s=>{sockets.add(s);s.once('close',()=>sockets.delete(s));});
  server.on('upgrade',(req,socket,head)=>{if(!root?.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
  t.after(async()=>{await stopChild();clients.forEach(client=>client.dispose());for(const realm of realms)await realm.instance?.close();await root?.locals.closeServices();sockets.forEach(socket=>socket.destroy());if(server.listening)await new Promise(done=>server.close(done));await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:40});});
  await new Promise(done=>server.listen(options.backendPort??0,'127.0.0.1',done));const backendPort=server.address().port,origin=options.frontOrigin??'http://127.0.0.1:'+(options.frontPort??backendPort);
  const rootOptions={dataDir,connectOrigins:[origin],appOriginTemplate:'https://{appId}.localhost:'+appTlsPort,localConnectorPort:connectorPort};
  root=createHttpApp(dist,rootOptions);
  const signed=async label=>{const client=createClientWithStorage({projectId:'soty',endpoint:origin+'/api/connect/rpc',scopedAppCleanup:true,
    fetch:(url,opts)=>fetch(url,{...opts,headers:{...opts.headers,origin}})},vault());clients.push(client);return{client,account:await client.bootstrap(label)};};
  const owner=await signed('Synthetic Root app owner'),reader=await signed('Synthetic Native participant'),foreign=await signed('Synthetic foreign profile');
  const hostFile=join(runtimeDir,'selected-hosts.json');
  await writeFile(join(runtimeDir,'connector-config.json'),JSON.stringify({workspaceRoot:runtimeDir,allowedRoots:[runtimeDir],installId:'ordinary_fixture'}));
  async function stopChild(){if(!child)return;const current=child;child=null;if(current.exitCode===null){current.kill('SIGTERM');await Promise.race([new Promise(done=>current.once('exit',done)),pause(2500)]);if(current.exitCode===null)current.kill('SIGKILL');}}
  function startChild(selected){child=spawn(process.execPath,[options.connectorScript??fileURLToPath(new URL('../../../../public/agent/soty-connector.mjs',import.meta.url)),'--port',String(connectorPort),'--scope','Dev'],{
    cwd:runtimeDir,windowsHide:true,stdio:['ignore','pipe','pipe'],env:{PATH:[dirname(process.execPath),process.env.SystemRoot?join(process.env.SystemRoot,'System32'):'/usr/bin','/bin'].join(process.platform==='win32'?';':':'),
      ...(process.env.SystemRoot?{SystemRoot:process.env.SystemRoot}:{}),TEMP:directory,TMP:directory,USERPROFILE:directory,APPDATA:join(directory,'appdata'),LOCALAPPDATA:join(directory,'localappdata'),
      SOTY_CONNECTOR_DATA_DIR:runtimeDir,SOTY_CONNECTOR_SERVER_URL:origin,SOTY_CONNECTOR_LINK_ID:'ordinary_installed_link_123456789012',SOTY_CONNECTOR_DEVICE_ID:'ordinary_host',
      SOTY_CONNECTOR_DEVICE_NICK:'Synthetic Source host',SOTY_CONNECTOR_AUTO_UPDATE:'0',SOTY_CONNECTOR_MANAGED:'0',SOTY_CONNECTOR_UPDATE_URL:origin+'/agent/manifest.json',
      ...(options.extraCa?{NODE_EXTRA_CA_CERTS:options.extraCa}:{}),...(selected?{SOTY_SELECTED_EMBED_HOST_FILE:hostFile}:{})}});
    child.stdout.on('data',part=>{outputBytes+=part.length;});child.stderr.on('data',part=>{outputBytes+=part.length;});}
  startChild(false);const claim=await until(async()=>{assert.equal(child.exitCode,null,'connector safe output bytes='+outputBytes);try{const r=await wireHttp(connectorPort,'/apps/claim',{origin,body:{}});return r.status===200?r.value:null;}catch{return null;}},60000);
  await owner.client.extension('apps.claim',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,claimCode:claim.claimCode});
  for(const [index,realmId]of['board','library'].entries()){
    const port=await freePort(),created=await owner.client.extension('apps.register',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,name:'Synthetic '+realmId,port,entryPath:'/embed',grants:{accountIds:[reader.account.accountId],communityIds:[]}});
    const appId=created.app.id,embedded='https://'+appId+'.localhost:'+appTlsPort,native='http://localhost:'+port;
    const target={appId,revision:2,ownerAccountId:owner.account.accountId,connectorKey:['ordinary_installed_link_123456789012',claim.hostDeviceId,claim.connectorId].join('|'),port,entryPath:'/embed',profile:'soty.selected-human-embed.v2'};target.digest=runtimeTargetDigest(target);
    const profile={schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'ordinary_installed_link_123456789012',hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId},target:{revision:2,digest:target.digest},sourceProfile:STANDARD_SELECTED_SOURCE_V2,
      resource:{registryId:'soty',environmentId:'production',tenantId:owner.account.accountId,appId,resourceId:'ordinary:'+realmId,selection:{kind:'soty.resource.v1',nativeId:'selected',incarnationId:'one'}},
      issuer:origin+'/human-identity',clientId:'ordinary-'+realmId,embedOrigin:embedded,nativeOrigin:native,parentOrigin:origin};
    realms.push({realmId,index,port,appId,embedded,native,target,profile,key:randomBytes(32),cipherKey:randomBytes(32),clientSecret:random(),instance:null,databasePath:join(directory,realmId+'.sqlite')});
  }
  await stopChild();await root.locals.closeServices();
  const jwk=generateKeyPairSync('rsa',{modulusLength:2048}).privateKey.export({format:'jwk'});Object.assign(jwk,{kid:'ordinary-fixture',use:'sig',alg:'RS256'});
  const humanIdentity={enabled:true,issuer:origin+'/human-identity',registryId:'soty',environmentId:'production',clients:realms.map(realm=>({id:realm.profile.clientId,label:'Synthetic '+realm.realmId,
    redirectUri:realm.embedded+'/api/embed/callback',clientSecret:realm.clientSecret})),jwks:{keys:[jwk]},cookieKeys:[random()],artifactKey:randomBytes(32),artifactKeyId:'ordinary-fixture'};
  const activeOptions={...rootOptions,humanIdentity,allowScopedEmbedMigration:true,allowSelectedResourceMigration:true,scopedEmbedProfiles:realms.map(realm=>realm.profile)};
  root=createHttpApp(dist,activeOptions);
  for(const realm of realms){realm.options={profile:realm.profile,transportKey:realm.key,connectorPort,rp:{issuer:realm.profile.issuer,clientId:realm.profile.clientId,clientSecret:realm.clientSecret,redirectUri:realm.embedded+'/api/embed/callback'},
    databasePath:realm.databasePath,realmId:realm.realmId,cipherKey:realm.cipherKey,keyId:'synthetic-'+realm.realmId,appLabel:'Synthetic '+realm.realmId};
    if(options.feedbackProcessing)realm.options.feedbackProcessing=options.feedbackProcessing;
    realm.options.allowEmptyGuest=options.emptyGuestBrowser===true;
    realm.instance=await createOrdinaryAppServer({...realm.options,initialize:true,newResource:{title:'Selected synthetic project',guestEmpty:options.emptyGuestBrowser===true}});
    realm.observeResponse=()=>realm.instance.server.prependListener('request',(req,res)=>{
      if(req.url==='/soty/authorize')realm.lastNativeForm={method:req.method,origin:req.headers.origin??'absent',contentType:req.headers['content-type']?.split(';')[0]??'absent'};
      const callbackDrop=realm.dropNextCallback===true&&req.url?.split('?')[0]==='/api/embed/callback';
      if(realm.dropPath!==req.url&&!callbackDrop)return;realm.dropPath=null;realm.dropNextCallback=false;const end=res.end.bind(res);
      res.end=(...args)=>{if(res.statusCode===200){res.destroy();return res;}return end(...args);};
    });realm.observeResponse();
    if(!options.emptyGuestBrowser){realm.instance.store.createPrincipal('native-participant');realm.instance.store.createPrincipal('native-owner');realm.instance.store.grant('selected','native-participant','participant');realm.instance.store.grant('selected','native-owner','owner');
      realm.nativeToken=realm.instance.store.createNativeSession(options.libraryNativeOwner&&realm.realmId==='library'?'native-owner':'native-participant');}await realm.instance.listen();
  }
  await writeFile(hostFile,JSON.stringify({schema:'soty.selected-embed-hosts.v1',entries:realms.map(realm=>({profile:realm.profile,key:realm.key.toString('base64url')}))}));startChild(true);
  await until(async()=>{try{return(await wireHttp(connectorPort,'/apps/claim',{origin,body:{}})).status===200;}catch{return false;}},60000);
  for(const realm of realms){const pub=await owner.client.extension('apps.publication.get',{appId:realm.appId});const prepared=await owner.client.extension('apps.source.prepare',{appId:realm.appId,expectedPolicyEpoch:pub.policyEpoch,expectedTargetRevision:pub.activeTargetRevision,
    source:{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,port:realm.port,entryPath:'/embed',profile:'soty.selected-human-embed.v2'}});
    await owner.client.extension('apps.source.promote',{appId:realm.appId,requestId:'ordinary-promote-'+realm.realmId,expectedPolicyEpoch:pub.policyEpoch,expectedTargetRevision:pub.activeTargetRevision,preparationId:prepared.preparationId,launchPolicy:'restricted',listed:false});
  }
  await until(async()=>{const list=await owner.client.extension('apps.list');return realms.every(realm=>list.apps.find(app=>app.id===realm.appId)?.state==='ready');});
  async function wire(url,options={}){url=new URL(url);const realm=realms.find(realm=>url.origin===realm.embedded||url.origin===realm.native),app=realm&&url.origin===realm.embedded;
    const physical=app||url.origin===origin?backendPort:realm?.port;assert.ok(physical,'fixed fixture origin');const jar=jars.get(url.hostname)??new Map();jars.set(url.hostname,jar);
    const cookie=[...jar].map(([name,value])=>name+'='+value).join('; ');const reply=await wireHttp(physical,url.pathname+url.search,{host:url.host,cookie,origin:options.body||options.fields?url.origin:undefined,...options});
    for(const raw of reply.headers['set-cookie']??[]){const pair=raw.split(';')[0],index=pair.indexOf('=');jar.set(pair.slice(0,index),pair.slice(index+1));}
    return{...reply,location:reply.headers.location?new URL(reply.headers.location,url):null};}
  for(const realm of realms){const nativeJar=jars.get('localhost')??new Map();jars.set('localhost',nativeJar);if(realm.nativeToken)nativeJar.set('ordinary_native_'+realm.realmId,realm.nativeToken);
    realm.launch=async(signer=reader)=>{const launch=await signer.client.extension('apps.launch',{appId:realm.appId});const url=new URL(launch.launchUrl);
      const boot=await wire(realm.embedded+'/_soty/session',{body:{ticket:url.hash.slice(1)}});assert.equal(boot.status,200);realm.lastLaunch=launch;return launch;};
    realm.restart=async()=>{await realm.instance.close();realm.instance=await createOrdinaryAppServer({...realm.options,initialize:false});realm.observeResponse();await realm.instance.listen();};
  }
  let serial=0;
  async function authorize(realm){let start=await wire(realm.embedded+'/api/embed/login',{body:{}});assert.equal(start.status,200);const native=await wire(start.value.nativeUrl);assert.equal(native.status,200);
    const fields=Object.fromEntries([...native.text.matchAll(/name="([^"]+)" value="([^"]*)"/gu)].map(match=>[match[1],match[2]]));fields.consent='yes';
    const authorize=await wire(realm.native+'/soty/authorize',{fields});assert.equal(authorize.status,303);let response=await wire(authorize.location);assert.ok([302,303].includes(response.status));const flow=response.location;
    await wire(flow);const context=(await wire(flow.href+'/context')).value;
    await reader.client.extension('identity.human.approve',{expectedAccountId:reader.account.accountId,interactionId:context.interactionId,browserNonce:context.browserNonce,csrf:context.csrf,requestId:'ordinary-decision-'+(++serial),decision:'approve'},{expectedAccountId:reader.account.accountId});
    response=await wire(flow.href+'/complete',{fields:{csrf:context.csrf}});assert.equal(response.status,303);response=await wire(response.location);
    if(response.status===200){const action=/<form method="post" action="([^"]+)">/u.exec(response.text)?.[1];const fields=Object.fromEntries([...response.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)].map(match=>[match[1],match[2]]));response=await wire(action,{fields});response=await wire(response.location);}
    assert.ok([302,303].includes(response.status));return{callbackUrl:response.location,source:realm,flow};
  }
  async function complete(callbackUrl){const callback=await wire(callbackUrl);assert.equal(callback.status,200);
    // Locator is not permission: Root already registered the exact one-use
    // completion against the private Source cookie/original current slot.
    const href=/<a href="([^"]+)">/u.exec(callback.text)?.[1];assert.ok(href,'fixed completion anchor exists');assert.ok(href.startsWith('/api/embed/complete-link?intent='));const url=new URL(href,callbackUrl);assert.equal(url.pathname,'/api/embed/complete-link');
    const completed=await wire(url);assert.equal(completed.status,200);assert.match(completed.text,/Приложение подключено/u);return{callback,completed};}
  async function login(realm){const flow=await authorize(realm);return{...flow,...await complete(flow.callbackUrl)};}
  async function restartRoot(){await root.locals.closeServices();root=createHttpApp(dist,activeOptions);
    await until(async()=>{const list=await owner.client.extension('apps.list');return realms.every(realm=>list.apps.find(app=>app.id===realm.appId)?.state==='ready');});}
  async function grantBrowser(accountId){for(const realm of realms)await owner.client.extension('apps.update',{appId:realm.appId,grants:{accountIds:[reader.account.accountId,accountId],communityIds:[]}});}
  return{directory,origin,backendPort,connectorPort,appTlsPort,realms,owner,reader,foreign,root:()=>root,restartRoot,grantBrowser,wire,authorize,complete,login,outputBytes:()=>outputBytes,oauthCounts};
}
