import assert from 'node:assert/strict';
import {createServer,request as httpRequest} from 'node:http';
import {spawn} from 'node:child_process';
import {generateKeyPairSync,randomBytes,randomUUID} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,rm,access} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {createHttpApp} from '../../../../server/http-app.js';
import {createClientWithStorage} from '../../../connect/browser/client.mjs';
import {runtimeTargetDigest,SCOPED_RUNTIME_PROFILE} from '../../server/schema.mjs';

export const sourceRoot=resolve(process.env.SOTY_PLANNER_GATEWAY_SOURCE_ROOT||'C:/Users/Junio/.codex/worktrees/planner-scoped-source');
let sourceAvailable=false,renewalSourceAvailable=false,source,sdk,createSotyBffProtocol;
try{await access(join(sourceRoot,'server/soty-source-host.mjs'));await access(join(sourceRoot,'node_modules/tsx/dist/esm/api/index.mjs'));sourceAvailable=true;}
catch{if(process.env.SOTY_PLANNER_GATEWAY_SOURCE_ROOT)throw new Error('Explicit selected Source package unavailable');}
if(sourceAvailable){const {tsImport}=await import(pathToFileURL(join(sourceRoot,'node_modules/tsx/dist/esm/api/index.mjs')).href);
  source=await tsImport(pathToFileURL(join(sourceRoot,'server/main.ts')).href,import.meta.url);
  ({createSotyBffProtocol}=await tsImport(pathToFileURL(join(sourceRoot,'server/soty-protocol.ts')).href,import.meta.url));
  sdk=await import(pathToFileURL(join(sourceRoot,'server/soty-source-host.mjs')).href);
  try{await access(join(sourceRoot,'server/soty-rp-format.ts'));renewalSourceAvailable=true;}catch{} }
export {sourceAvailable,renewalSourceAvailable};
const random=()=>randomBytes(32).toString('base64url');
const wait=ms=>new Promise(done=>setTimeout(done,ms));
export async function until(check,timeout=10000){const end=Date.now()+timeout;while(Date.now()<end){const value=await check();if(value)return value;await wait(30);}throw new Error('scoped_fixture_timeout');}
export async function freePort(){const server=createServer();await new Promise(done=>server.listen(0,'127.0.0.1',done));const port=server.address().port;await new Promise(done=>server.close(done));return port;}
const memory=()=>{let value;return{async read(){return structuredClone(value??null);},async claim(next){value??=structuredClone(next);return structuredClone(value);},async compareAndSwap(revision,next){assert.equal(value.localRevision,revision);value=structuredClone(next);return structuredClone(value);}};};

/** Synthetic/temp-only environment. All app admission, Connect, installed
 * channel, HTTP and Source/OIDC operations below use actual product services. */
export async function createScopedGatewayFixture({frontPort,appPort,backendPort:requestedBackendPort,distDir,t,renewal=false}={}) {
  assert.ok(sourceAvailable);
  const directory=await mkdtemp(join(tmpdir(),'soty-scoped-full-')),runtimeDir=join(directory,'runtime'),dataDir=join(directory,'root'),dist=distDir||join(directory,'dist');
  await mkdir(runtimeDir);if(!distDir){await mkdir(dist);await writeFile(join(dist,'index.html'),'<!doctype html><title>Synthetic Root interaction</title>');}
  const connectorPort=await freePort(), appTlsPort=appPort||await freePort(),sourceDb=join(directory,'planner.sqlite');
  let root,planner,child;const clients=[],sockets=new Set();let outputBytes=0;
  const server=createServer((req,res)=>root?root(req,res):res.writeHead(503).end());
  server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
  server.on('upgrade',(req,socket,head)=>{if(!root?.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
  let ended=false;if(t)t.after(()=>close());
  await new Promise(done=>server.listen(requestedBackendPort||0,'127.0.0.1',done));
  const backendPort=server.address().port,backendOrigin=`http://127.0.0.1:${backendPort}`,origin=frontPort?`http://127.0.0.1:${frontPort}`:backendOrigin;
  const template=`https://{appId}.localhost:${appTlsPort}`;
  const hostFile=join(runtimeDir,'selected-hosts.json'),configFile=join(runtimeDir,'connector-config.json');
  await writeFile(configFile,JSON.stringify({workspaceRoot:runtimeDir,allowedRoots:[runtimeDir],installId:'fixture_scoped_install'}));
  async function stopChild(){if(!child)return;const old=child;child=null;if(old.exitCode===null){old.kill('SIGTERM');await Promise.race([new Promise(done=>old.once('exit',done)),wait(3000)]);if(old.exitCode===null)old.kill('SIGKILL');}}
  function startChild(selected=false){
    child=spawn(process.execPath,[fileURLToPath(new URL('../../../../public/agent/soty-connector.mjs',import.meta.url)),'--port',String(connectorPort),'--scope','Dev'],{
      windowsHide:true,stdio:['ignore','pipe','pipe'],cwd:runtimeDir,env:{
        PATH:[dirname(process.execPath),process.env.SystemRoot?join(process.env.SystemRoot,'System32'):'/usr/bin','/bin'].join(process.platform==='win32'?';':':'),
        ...(process.env.SystemRoot?{SystemRoot:process.env.SystemRoot}:{}),TEMP:directory,TMP:directory,USERPROFILE:directory,APPDATA:join(directory,'appdata'),LOCALAPPDATA:join(directory,'localappdata'),
        SOTY_CONNECTOR_DATA_DIR:runtimeDir,SOTY_CONNECTOR_SERVER_URL:origin,SOTY_CONNECTOR_LINK_ID:'fixture_scoped_link_12345678901234567',SOTY_CONNECTOR_DEVICE_ID:'fixture_scoped_host',
        SOTY_CONNECTOR_DEVICE_NICK:'Synthetic scoped device',SOTY_CONNECTOR_AUTO_UPDATE:'0',SOTY_CONNECTOR_MANAGED:'0',SOTY_CONNECTOR_UPDATE_URL:origin+'/agent/manifest.json',
        ...(selected?{SOTY_SELECTED_EMBED_HOST_FILE:hostFile}:{})}});
    child.stdout.on('data',bytes=>{outputBytes+=bytes.length;});child.stderr.on('data',bytes=>{outputBytes+=bytes.length;});
  }
  const rootOptions={dataDir,connectOrigins:[origin],appOriginTemplate:template,localConnectorPort:connectorPort};
  root=createHttpApp(dist,rootOptions);
  planner=await source.createPlannerServer({dbPath:sourceDb,port:0,host:'127.0.0.1',scheduler:false});
  const sourcePort=await planner.listen(),native=`http://localhost:${sourcePort}`;
  const seed=await fetch(`http://127.0.0.1:${sourcePort}/api/agent/call`,{method:'POST',headers:{origin:`http://127.0.0.1:${sourcePort}`,'content-type':'application/json'},
    body:JSON.stringify({name:'planner_apply',arguments:{requestId:randomUUID(),operations:[{op:'create',collection:'workspaces',key:'selected',data:{name:'Synthetic selected project',timezone:'UTC'}},
      {op:'create',collection:'objects',workspace:'$selected',key:'one',data:{title:'Synthetic private selected object'}},
      {op:'create',collection:'workspaces',key:'other',data:{name:'Synthetic other private project',timezone:'UTC'}},
      {op:'create',collection:'objects',workspace:'$other',key:'hidden',data:{title:'Synthetic hidden object'}}]}})});
  assert.equal(seed.status,200);const seeded=await seed.json(),workspaceId=seeded.refs.selected,foreignWorkspaceId=seeded.refs.other;
  planner.server.closeAllConnections();await planner.close();planner=null;
  const client=async(label)=>{const storage=memory(),hooks={afterResponse:null};
    const fetcher=async(url,options)=>{const response=await fetch(url,{...options,headers:{...options.headers,origin}});
      if(hooks.afterResponse)await hooks.afterResponse(JSON.parse(options.body).op,response);return response;};
    const value=createClientWithStorage({projectId:'soty',endpoint:backendOrigin+'/api/connect/rpc',scopedAppCleanup:true,fetch:fetcher},storage);
    clients.push(value);return{client:value,account:await value.bootstrap(label),storage,hooks,fetch:fetcher};};
  const owner=await client('Synthetic scoped owner'),reader=await client('Synthetic scoped participant'),foreign=await client('Synthetic foreign participant');
  startChild();
  const claim=await until(async()=>{assert.equal(child.exitCode,null,'installed connector exited; output bytes='+outputBytes);try{const result=await wireHttp(connectorPort,'/apps/claim',{method:'POST',origin,body:{}});return result.status===200?result.body:null;}catch{return null;}},60000);
  await owner.client.extension('apps.claim',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,claimCode:claim.claimCode});
  const created=await owner.client.extension('apps.register',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,name:'Планировщик · Synthetic project',port:sourcePort,entryPath:'/embed',grants:{accountIds:[reader.account.accountId],communityIds:[]}});
  const appId=created.app.id,embedded=`https://${appId}.localhost:${appTlsPort}`,issuer=origin+'/human-identity',clientSecret=random(),sourceKey=randomBytes(32);
  const privateJwk=generateKeyPairSync('rsa',{modulusLength:2048}).privateKey.export({format:'jwk'});Object.assign(privateJwk,{kid:'scoped-fixture',use:'sig',alg:'RS256'});
  const humanIdentity={enabled:true,issuer,registryId:'soty',environmentId:'production',clients:[{id:'planner-scoped-fixture',label:'Synthetic selected Planner',redirectUri:embedded+'/api/embed/callback',clientSecret,...(renewal?{version:2}:{})}],
    jwks:{keys:[privateJwk]},cookieKeys:[random()],artifactKey:randomBytes(32),artifactKeyId:'scoped-fixture',...(renewal?{renewal:{admissionEnabled:true,clientIds:['planner-scoped-fixture']}}:{})};
  const target={appId,revision:2,ownerAccountId:owner.account.accountId,connectorKey:[ 'fixture_scoped_link_12345678901234567',claim.hostDeviceId,claim.connectorId].join('|'),port:sourcePort,entryPath:'/embed',profile:SCOPED_RUNTIME_PROFILE};
  target.digest=runtimeTargetDigest(target);
  const profile={schema:SCOPED_RUNTIME_PROFILE,appId,connector:{linkId:'fixture_scoped_link_12345678901234567',hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId},
    target:{revision:target.revision,digest:target.digest},sourceProfile:{id:'planner.selected-workspace',version:1,digest:'9'.repeat(64)},
    resource:{registryId:'soty',tenantId:owner.account.accountId,environmentId:'production',appId,resourceId:'planner:selected',workspaceId},issuer,clientId:'planner-scoped-fixture',embedOrigin:embedded,nativeOrigin:native,parentOrigin:origin};
  await stopChild();await root.locals.closeServices();
  const selectedOptions={...rootOptions,humanIdentity,humanIdentityRenewalMigration:renewal,allowScopedEmbedMigration:true,scopedEmbedProfiles:[profile]};root=createHttpApp(dist,selectedOptions);
  const rpProfile={issuer,clientId:profile.clientId,clientSecret,redirectUri:embedded+'/api/embed/callback',...(renewal?{renewalProfile:'soty.human-rp-renewal.v1'}:{})},rp=createSotyBffProtocol(rpProfile);
  const authorityReader=sdk.createSourceAuthorityClient({profile,key:sourceKey,connectorPort});
  const verifier=sdk.createSourceProofVerifier({profile,key:sourceKey,consumeNonce(nonce,expires){
    const db=planner.store.db;db.exec('CREATE TABLE IF NOT EXISTS fixture_source_nonce(nonce TEXT PRIMARY KEY,expires_at INTEGER NOT NULL)');
    db.prepare('DELETE FROM fixture_source_nonce WHERE expires_at<=?').run(Date.now());
    if(db.prepare('SELECT count(*) AS n FROM fixture_source_nonce').get().n>=4096)return false;
    try{db.prepare('INSERT INTO fixture_source_nonce VALUES(?,?)').run(nonce,expires);return true;}catch{return false;}}});
  const sourceOptions={dbPath:sourceDb,port:sourcePort,host:'127.0.0.1',scheduler:false,production:true,embed:{nativeOrigin:native,embedOrigin:embedded,parentOrigin:origin,workspaceId,profile:rpProfile,sessionKey:random(),
    ...(renewal?{renewal:{admissionEnabled:true,allowMigration:true}}:{}),
    bridge:{consentDigest:sdk.sourceConsentDigest(profile),verifyReady:request=>verifier.verifyReady(request),verifyRequest:(request,body)=>verifier.verify(request,{body}),continuation:request=>verifier.context(request).reference,context:request=>verifier.context(request),
      assertCurrent:async request=>{await authorityReader({reference:verifier.context(request).reference,connector:profile.connector});}},
    currentSotySubject:sdk.createSourceCurrentSubjectPort({profile,verifier,readAuthority:authorityReader,verifyHuman:async proof=>({issuer,subject:await rp.currentSubject(proof.accessToken,proof.subject)})})}};
  planner=await source.createPlannerServer(sourceOptions);await planner.listen();
  await writeFile(hostFile,JSON.stringify({schema:'soty.selected-embed-hosts.v1',entries:[{profile,key:sourceKey.toString('base64url')}]}));
  startChild(true);
  await until(async()=>{try{return(await wireHttp(connectorPort,'/apps/claim',{method:'POST',origin,body:{}})).status===200;}catch{return false;}},60000);
  const publication=await owner.client.extension('apps.publication.get',{appId});
  const preparation=await owner.client.extension('apps.source.prepare',{appId,expectedPolicyEpoch:publication.policyEpoch,expectedTargetRevision:publication.activeTargetRevision,
    source:{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,port:sourcePort,entryPath:'/embed',profile:SCOPED_RUNTIME_PROFILE}});
  await owner.client.extension('apps.source.promote',{appId,requestId:'fixture-selected-source',expectedPolicyEpoch:publication.policyEpoch,expectedTargetRevision:publication.activeTargetRevision,
    preparationId:preparation.preparationId,launchPolicy:'restricted',listed:false});
  await until(async()=>{const list=await owner.client.extension('apps.list');return list.apps.find(value=>value.id===appId)?.state==='ready';});
  const wire=new FixtureWire({backendPort,sourcePort,embedded,native,origin});let serial=0;
  async function launch(signer=reader){const value=await signer.client.extension('apps.launch',{appId});const url=new URL(value.launchUrl);
    const session=await wire.request(embedded+'/_soty/session',{body:{ticket:url.hash.slice(1)}});assert.equal(session.status,200);return value;}
  async function login(signer=reader,long=renewal){let result=await wire.request(embedded+'/api/embed/login');assert.equal(result.status,302);
    result=await wire.request(result.location);assert.ok([302,303].includes(result.status));const flow=result.location;
    await wire.request(flow);const context=(await wire.request(flow.href+'/context')).body;
    await signer.client.extension('identity.human.approve',{interactionId:context.interactionId,browserNonce:context.browserNonce,csrf:context.csrf,requestId:'scoped-decision-'+(++serial),decision:'approve',expectedAccountId:signer.account.accountId,...(long?{stayInAppSeconds:86400}:{})},{expectedAccountId:signer.account.accountId});
    result=await wire.request(flow.href+'/complete',{fields:{csrf:context.csrf}});assert.equal(result.status,303);result=await wire.request(result.location);
    if(result.status===200){const action=/<form method="post" action="([^"]+)">/.exec(result.text)?.[1];const fields=Object.fromEntries([...result.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/g)].map(match=>[match[1],match[2]]));result=await wire.request(action,{fields});result=await wire.request(result.location);}
    assert.ok([302,303].includes(result.status));return wire.request(result.location,{withoutAppCookie:true});}
  async function nativeConsent(page){const url=/<a href="([^"]+\/soty\/connect\?intent=[A-Za-z0-9_-]+)">/.exec(page.text)?.[1];assert.ok(url);
    const value=await wire.request(url),intent=/name="intent" value="([A-Za-z0-9_-]+)"/.exec(value.text)?.[1];assert.ok(intent);
    const approved=await wire.request(native+'/soty/connect',{fields:{intent}});assert.equal(approved.status,302);return wire.request(approved.location,{withoutAppCookie:true});}
  async function close(){if(ended)return;ended=true;await stopChild();clients.forEach(value=>value.dispose());if(planner){planner.server.closeAllConnections();await planner.close();planner=null;}await root?.locals.closeServices();sockets.forEach(socket=>socket.destroy());await new Promise(done=>server.close(done));
    assert.equal(dirname(resolve(directory)),resolve(tmpdir()));assert.match(basename(directory),/^soty-scoped-full-/);await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:100});}
  return{directory,root:()=>root,server,backendPort,backendOrigin,origin,connectorPort,appTlsPort,sourcePort,embedded,native,appId,owner,reader,foreign,workspaceId,foreignWorkspaceId,target,profile,wire,launch,login,nativeConsent,close,
    planner:()=>planner,async grant(accountId){await owner.client.extension('apps.update',{appId,grants:{accountIds:[reader.account.accountId,accountId],communityIds:[]}});},
    async restartSource(){planner.server.closeAllConnections();await planner.close();planner=await source.createPlannerServer(sourceOptions);await planner.listen();},
    async restartRoot(overrides={}){await root.locals.closeServices();root=createHttpApp(dist,{...selectedOptions,...overrides});},
    sourceOptions,selectedOptions};
}

class FixtureWire {
  constructor(options){Object.assign(this,options);this.cookies=new Map();}
  async request(value,options={}){const url=new URL(value),app=url.origin===this.embedded,physical=app||url.origin===this.origin?this.backendPort:this.sourcePort;
    const cookie=options.withoutAppCookie&&app?'':this.cookies.get(url.hostname)||'';
    const response=await wireHttp(physical,url.pathname+url.search,{host:url.host,cookie,origin:options.fields||options.body?url.origin:undefined,...options});
    if(response.headers['set-cookie']&&!options.withoutAppCookie){const jar=new Map((this.cookies.get(url.hostname)||'').split('; ').filter(Boolean).map(pair=>{const split=pair.indexOf('=');return[pair.slice(0,split),pair.slice(split+1)];}));
      for(const value of response.headers['set-cookie']){const pair=value.split(';')[0],split=pair.indexOf('=');jar.set(pair.slice(0,split),pair.slice(split+1));}this.cookies.set(url.hostname,[...jar].map(([name,value])=>name+'='+value).join('; '));}
    response.location=response.headers.location?new URL(response.headers.location,url):null;return response;}
}
export function wireHttp(port,path,{host,cookie,origin,method,body,fields,headers={}}={}){return new Promise((resolve,reject)=>{
  const bytes=fields?new URLSearchParams(fields).toString():body===undefined?null:Buffer.isBuffer(body)?body:JSON.stringify(body);
  const request=httpRequest({hostname:'127.0.0.1',port,path,method:method||(bytes===null?'GET':'POST'),agent:false,headers:{...(host?{host}:{}),...(cookie?{cookie}:{}),...(origin?{origin}:{}),
    ...(bytes!==null?{'content-type':fields?'application/x-www-form-urlencoded':'application/json','content-length':Buffer.byteLength(bytes)}:{}),...headers},signal:AbortSignal.timeout(10000)},response=>{
    const chunks=[];let size=0;response.on('data',part=>{size+=part.length;if(size>4194304)response.destroy();else chunks.push(part);});response.on('error',reject);response.on('end',()=>{const text=Buffer.concat(chunks).toString('utf8');let body=null;try{body=JSON.parse(text);}catch{}resolve({status:response.statusCode,headers:response.headers,text,body});});});request.on('error',reject);request.end(bytes);});}
