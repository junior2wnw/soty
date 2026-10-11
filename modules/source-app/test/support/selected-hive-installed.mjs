import assert from 'node:assert/strict';
import { createServer,request as httpRequest } from 'node:http';
import { randomBytes,generateKeyPairSync,webcrypto,createHash } from 'node:crypto';
import { mkdtempSync,mkdirSync,writeFileSync,readFileSync,readdirSync,rmSync } from 'node:fs';
import { join,resolve,dirname,basename } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { DatabaseSync } from 'node:sqlite';
import {installFixtureClock} from './soty-clock.mjs';
// Local reuse of HIVE dc5e734e4ce7fd44103ca0ae3755211284543334's real
// selected HTTP fixture. Root options below are constructor-only test code.
export const source=resolve(fileURLToPath(new URL('../../../..',import.meta.url)));
const root=process.env.SOTY_HIVE_NATIVE_SOURCE_ROOT;
const random=()=>randomBytes(32).toString('base64url'),wait=ms=>new Promise(done=>setTimeout(done,ms));
async function port(){const s=createServer();await new Promise(done=>s.listen(0,'127.0.0.1',done));const p=s.address().port;await new Promise(done=>s.close(done));return p;}
async function until(fn){for(let i=0;i<200;i++){const result=await fn();if(result)return result;await wait(100);}throw new Error('selected_fixture_timeout');}
const memory=()=>{let value;return{async read(){return structuredClone(value??null);},async claim(next){value??=structuredClone(next);return structuredClone(value);},async compareAndSwap(revision,next){assert.equal(value.localRevision,revision);value=structuredClone(next);return structuredClone(value);}};};
function wireHttp(p,path,{host,cookie,origin,method,body,fields,headers={}}={}){return new Promise((done,reject)=>{
  const bytes=fields?new URLSearchParams(fields).toString():body===undefined?null:JSON.stringify(body);
  const req=httpRequest({hostname:'127.0.0.1',port:p,path,method:method||(bytes===null?'GET':'POST'),agent:false,headers:{...(host?{host}:{}),...(cookie?{cookie}:{}),...(origin?{origin}:{}),
    ...(bytes!==null?{'content-type':fields?'application/x-www-form-urlencoded':'application/json','content-length':Buffer.byteLength(bytes)}:{}),...headers},signal:AbortSignal.timeout(15000)},res=>{
    const parts=[];let n=0;res.on('data',p=>{n+=p.length;if(n>4194304)res.destroy();else parts.push(p);});res.on('error',reject);res.on('end',()=>{const bytes=Buffer.concat(parts),text=bytes.toString('utf8');let body=null;try{body=JSON.parse(text);}catch{}done({status:res.statusCode,headers:res.headers,text,body,bytes});});});req.on('error',reject);req.end(bytes);
});}
export async function selectedRootEnvironment(t,{renewal=false,selectedMigration=true,sourcePackageRoot=root,rootBindings=()=>({})}={}){
  assert.ok(sourcePackageRoot,'An explicit owned HIVE package is required; no fake Source or skipped Native gate.');
  const packageRoot=resolve(sourcePackageRoot);
  const from=path=>import(pathToFileURL(join(source,path)).href);
  const [{createHttpApp},{createClientWithStorage},{runtimeTargetDigest,RESOURCE_RUNTIME_PROFILE},{HIVE_SELECTED_SOURCE}]=await Promise.all([from('server/http-app.js'),from('modules/connect/browser/client.mjs'),from('modules/apps/server/schema.mjs'),from('modules/apps/scoped-embed/resource-route-adapters.mjs')]);
  const dir=mkdtempSync(join(tmpdir(),'hive-selected-actual-')),filename=join(dir,'hive.sqlite'),runtime=join(dir,'runtime'),dist=join(dir,'dist');mkdirSync(runtime);mkdirSync(dist);writeFileSync(join(dist,'index.html'),'<!doctype html><title>Synthetic Root</title>');
  const clockFile=join(dir,'clock.txt');if(renewal)writeFileSync(clockFile,'0');const clock=renewal?installFixtureClock(clockFile):null;
  const db=new DatabaseSync(filename);try{for(const file of readdirSync(join(packageRoot,'drizzle')).filter(file=>/^00\d\d_.+\.sql$/u.test(file)&&(selectedMigration||!file.startsWith('0011_'))).sort())db.exec(readFileSync(join(packageRoot,'drizzle',file),'utf8'));}finally{db.close();}
  const hivePort=await port(),connectorPort=await port(),appPort=await port(),native=`http://127.0.0.1:${hivePort}`;let app,hive,connector,outputBytes=0;
  let refreshes=0,userinfoReads=0,userinfoBarrier=null;
  const userinfoStatuses={ok:0,limited:0,other:0},userinfoDenials={rate_limit:0,temporarily_unavailable:0,unknown:0};
  const dispatch=(req,res)=>app?app(req,res):res.writeHead(503).end();
  const sockets=new Set(),server=createServer((req,res)=>{if(req.url==='/human-identity/userinfo'){
      userinfoReads++;const end=res.end;
      res.end=function(...args){
        if(res.statusCode===200)userinfoStatuses.ok++;else if(res.statusCode===429){userinfoStatuses.limited++;
          let code='unknown';try{const bytes=args[0];if((typeof bytes==='string'||Buffer.isBuffer(bytes))&&Buffer.byteLength(bytes)<=1024){const body=JSON.parse(String(bytes));
            const candidate=body.error?.code??body.error;if(['rate_limit','temporarily_unavailable'].includes(candidate))code=candidate;}}catch{}
          userinfoDenials[code]++;
        }else userinfoStatuses.other++;
        return end.apply(this,args);
      };
      const barrier=userinfoBarrier;
      if(barrier?.ordinal===userinfoReads){userinfoBarrier=null;barrier.entered();barrier.wait.then(()=>dispatch(req,res),()=>res.writeHead(503).end());return;}
    }
    if(req.url==='/human-identity/token'){const end=res.end;res.end=function(...args){if(req.body?.grant_type==='refresh_token')refreshes++;return end.apply(this,args);};}
    return dispatch(req,res);});server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
  server.on('upgrade',(req,socket,head)=>{if(!app?.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
  await new Promise(done=>server.listen(0,'127.0.0.1',done));const backend=server.address().port,origin=`http://127.0.0.1:${backend}`,issuer=origin+'/human-identity';
  const options={dataDir:join(dir,'root'),connectOrigins:[origin],appOriginTemplate:`https://{appId}.localhost:${appPort}`,localConnectorPort:connectorPort};
  app=createHttpApp(dist,options);const clients=[];
  const env={PATH:[dirname(process.execPath),process.env.SystemRoot?join(process.env.SystemRoot,'System32'):'/usr/bin','/bin'].join(process.platform==='win32'?';':':'),
    ...(process.env.SystemRoot?{SystemRoot:process.env.SystemRoot}:{}),TEMP:dir,TMP:dir,USERPROFILE:dir,APPDATA:join(dir,'appdata'),LOCALAPPDATA:join(dir,'localappdata'),
    HIVE_SQLITE_PATH:filename,HIVE_PUBLIC_ORIGIN:native,HIVE_SOTY_ENABLED:'false',HIVE_SOTY_ISSUER:'',HIVE_SOTY_CLIENT_ID:'',HIVE_SOTY_CLIENT_SECRET:'',HIVE_SOTY_ARTIFACT_KEY:'',HIVE_SOTY_ARTIFACT_KEY_ID:'',HIVE_SOTY_RENEWAL_PROFILE:'',
    HIVE_SELECTED_EMBED_ENABLED:'false',HIVE_SELECTED_EMBED_HOST_FILE:'',HIVE_FEEDBACK_ENABLED:'true',HIVE_FEEDBACK_SOURCE_ID:'hive.fixture',HIVE_FEEDBACK_PARENT_ORIGIN:origin,HIVE_FEEDBACK_CAPTURE_SOURCE_ID:'hive.selected-project',
    HIVE_OIDC_ISSUER:'',HIVE_OIDC_CLIENT_ID:'',HIVE_OIDC_CLIENT_SECRET:'',HIVE_OIDC_REDIRECT_URI:'',HIVE_AGENT_TOKEN:'synthetic-emergency',NODE_NO_WARNINGS:'1'};
  if(renewal)env.HIVE_TEST_CLOCK_FILE=clockFile;
  async function stop(child){if(child?.exitCode===null&&child.signalCode===null){const end=once(child,'exit');child.kill('SIGTERM');await end;}}
  async function startHive(){hive=spawn(process.execPath,[...(renewal?['--import',new URL('./soty-clock.mjs',import.meta.url).href]:[]),'node_modules/vinext/dist/cli.js','start','--port',String(hivePort),'--hostname','127.0.0.1'],{cwd:packageRoot,env,stdio:['ignore','pipe','pipe'],windowsHide:true});
    hive.stdout.on('data',bytes=>outputBytes+=bytes.length);hive.stderr.on('data',bytes=>outputBytes+=bytes.length);
    await until(async()=>{assert.equal(hive.exitCode,null,'Source process running; bounded diagnostics bytes='+outputBytes);try{return(await fetch(native+'/api/health')).ok;}catch{return false;}});}
  const hostFile=join(runtime,'selected-hosts.json'),sourceFile=join(dir,'source-host.json');writeFileSync(join(runtime,'connector-config.json'),JSON.stringify({workspaceRoot:runtime,allowedRoots:[runtime],installId:'fixture_selected_hive'}));
  function startConnector(selected=false){connector=spawn(process.execPath,[...(renewal?['--import',new URL('./soty-clock.mjs',import.meta.url).href]:[]),join(source,'public/agent/soty-connector.mjs'),'--port',String(connectorPort),'--scope','Dev'],{cwd:runtime,windowsHide:true,stdio:['ignore','pipe','pipe'],env:{
    PATH:[dirname(process.execPath),join(process.env.SystemRoot||'C:/Windows','System32')].join(';'),SystemRoot:process.env.SystemRoot||'C:/Windows',TEMP:dir,TMP:dir,USERPROFILE:dir,APPDATA:join(dir,'appdata'),LOCALAPPDATA:join(dir,'localappdata'),
    SOTY_CONNECTOR_DATA_DIR:runtime,SOTY_CONNECTOR_SERVER_URL:origin,SOTY_CONNECTOR_LINK_ID:'fixture_hive_link_1234567890123456789',SOTY_CONNECTOR_DEVICE_ID:'fixture_hive_host',SOTY_CONNECTOR_DEVICE_NICK:'Synthetic HIVE host',SOTY_CONNECTOR_AUTO_UPDATE:'0',SOTY_CONNECTOR_MANAGED:'0',SOTY_CONNECTOR_UPDATE_URL:origin+'/agent/manifest.json',...(renewal?{HIVE_TEST_CLOCK_FILE:clockFile}:{}),...(selected?{SOTY_SELECTED_EMBED_HOST_FILE:hostFile}:{})}});
    connector.stdout.on('data',bytes=>outputBytes+=bytes.length);connector.stderr.on('data',bytes=>outputBytes+=bytes.length);}
  const held=new Set();let closed=false;t.after(async()=>{if(closed)return;closed=true;for(const release of held)release();await stop(connector);await stop(hive);clients.forEach(client=>client.dispose());await app?.locals.closeServices();sockets.forEach(socket=>socket.destroy());server.closeAllConnections();await new Promise(done=>server.close(done));
    clock?.restore();assert.equal(dirname(resolve(dir)),resolve(tmpdir()));assert.match(basename(dir),/^hive-selected-actual-/u);rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:100});});
  async function signer(label){const value=createClientWithStorage({projectId:'soty',endpoint:origin+'/api/connect/rpc',fetch:(url,opts)=>fetch(url,{...opts,headers:{...opts.headers,origin}})},memory());clients.push(value);return{client:value,account:await value.bootstrap(label)};}
  const owner=await signer('Synthetic HIVE owner'),foreign=await signer('Synthetic other Root account');
  await startHive();const jar=new Map();let embedded,pendingBootCheck;
  async function request(value,opts={},cookieJar=jar){const url=new URL(value),physical=url.origin===native?hivePort:backend,key=url.hostname,cookie=cookieJar.get(key)||'';
    const candidate=pendingBootCheck&&url.origin===embedded&&((url.pathname==='/api/embed/session-continue'&&opts.body!==undefined)||(url.pathname==='/_soty/session'&&opts.body===undefined));
    const result=await wireHttp(physical,url.pathname+url.search,{host:url.host,cookie,origin:opts.body!==undefined||opts.fields!==undefined?url.origin:undefined,...opts,
      headers:{...(candidate?{'x-soty-boot-check':pendingBootCheck}:{}),...opts.headers}});
    if(result.headers['set-cookie']){const pairs=new Map(cookie.split('; ').filter(Boolean).map(v=>{const at=v.indexOf('=');return[v.slice(0,at),v.slice(at+1)];}));for(const raw of result.headers['set-cookie']){const pair=raw.split(';')[0],at=pair.indexOf('=');pairs.set(pair.slice(0,at),pair.slice(at+1));}cookieJar.set(key,[...pairs].map(([k,v])=>k+'='+v).join('; '));}
    if(candidate&&url.pathname==='/api/embed/session-continue'&&result.status===200&&result.body?.ready===true)pendingBootCheck=undefined;
    return{...result,location:result.headers.location?new URL(result.headers.location,url):null};}
  const pair=await webcrypto.subtle.generateKey({name:'ECDSA',namedCurve:'P-256'},false,['sign','verify']),jwk=await webcrypto.subtle.exportKey('jwk',pair.publicKey);
  const canonical=JSON.stringify({crv:jwk.crv,kty:jwk.kty,x:jwk.x,y:jwk.y}),keyDigest=Buffer.from(await webcrypto.subtle.digest('SHA-256',new TextEncoder().encode(canonical))).toString('base64url'),deviceId='dev_'+keyDigest.slice(0,32);
  const challenge=await request(native+'/api/account/devices/challenge',{body:{id:deviceId,label:'Synthetic old HIVE device',publicJwk:jwk}});assert.equal(challenge.status,200);
  const signature=Buffer.from(await webcrypto.subtle.sign({name:'ECDSA',hash:'SHA-256'},pair.privateKey,new TextEncoder().encode(`hive.device.register.v1\n${challenge.body.challenge}\n${deviceId}`))).toString('base64url');
  assert.equal((await request(native+'/api/account/devices/register',{body:{id:deviceId,label:'Synthetic old HIVE device',publicJwk:jwk,challenge:challenge.body.challenge,signature}})).status,200);
  const legacy=(await request(native+'/api/account/session')).body;
  const created=await request(native+'/api/hive/projects',{body:{requestId:'native-create',project:{schema:'hive.project',version:1,id:'selected-native',title:'Native old project',graph:{nodes:[{id:'old-cell',title:'Original cell'}],edges:[],groups:[]}}}});assert.equal(created.status,201);
  const locator=await request(native+'/api/soty/selected-resource',{body:{projectId:'selected-native'}});assert.equal(locator.status,200,'locator='+locator.body?.error?.code);
  startConnector();const claim=await until(async()=>{assert.equal(connector.exitCode,null,'connector running; diagnostics bytes='+outputBytes);try{const result=await wireHttp(connectorPort,'/apps/claim',{origin,body:{}});return result.status===200?result.body:null;}catch{return null;}});
  await owner.client.extension('apps.claim',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,claimCode:claim.claimCode});
  const registered=await owner.client.extension('apps.register',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,name:'HIVE synthetic selected',port:hivePort,entryPath:'/embed',grants:{accountIds:[],communityIds:[]}}),appId=registered.app.id;
  embedded=`https://${appId}.localhost:${appPort}`;const clientId='hive.actual-selected',secret=random(),sourceKey=randomBytes(32),privateJwk=generateKeyPairSync('rsa',{modulusLength:2048}).privateKey.export({format:'jwk'});Object.assign(privateJwk,{kid:'hive-selected-fixture',alg:'RS256',use:'sig'});
  const target={appId,revision:2,ownerAccountId:owner.account.accountId,connectorKey:['fixture_hive_link_1234567890123456789',claim.hostDeviceId,claim.connectorId].join('|'),port:hivePort,entryPath:'/embed',profile:RESOURCE_RUNTIME_PROFILE};target.digest=runtimeTargetDigest(target);
  const profile={schema:RESOURCE_RUNTIME_PROFILE,appId,connector:{linkId:'fixture_hive_link_1234567890123456789',hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId},target:{revision:2,digest:target.digest},sourceProfile:HIVE_SELECTED_SOURCE,
    resource:{registryId:'soty',tenantId:owner.account.accountId,appId,environmentId:'production',resourceId:'hive:selected',selection:{kind:locator.body.kind,nativeId:locator.body.nativeId,incarnationId:locator.body.incarnationId}},issuer,clientId,embedOrigin:embedded,nativeOrigin:native,parentOrigin:origin};
  await stop(connector);await stop(hive);await app.locals.closeServices();
  const selectedOptions={...options,allowScopedEmbedMigration:true,allowSelectedResourceMigration:true,scopedEmbedProfiles:[profile],humanIdentityRenewalMigration:renewal,
    humanIdentity:{enabled:true,issuer,registryId:'soty',environmentId:'production',clients:[{id:clientId,label:'Actual Native HIVE',redirectUri:native+'/account/soty/callback',clientSecret:secret}],
      ...(renewal?{renewal:{admissionEnabled:true,clientIds:[clientId]}}:{}),jwks:{keys:[privateJwk]},cookieKeys:[random()],artifactKey:randomBytes(32),artifactKeyId:'fixture'}};
  Object.assign(selectedOptions,rootBindings(profile));
  app=createHttpApp(dist,selectedOptions);writeFileSync(hostFile,JSON.stringify({schema:'soty.selected-embed-hosts.v1',entries:[{profile,key:sourceKey.toString('base64url')}]}));
  writeFileSync(sourceFile,JSON.stringify({schema:'hive.selected-host.v1',profile,macKey:sourceKey.toString('base64url'),artifactKey:random(),artifactKeyId:'fixture',connectorPort}));
  Object.assign(env,{HIVE_SOTY_ENABLED:'true',HIVE_SOTY_ISSUER:issuer,HIVE_SOTY_CLIENT_ID:clientId,HIVE_SOTY_CLIENT_SECRET:secret,HIVE_SOTY_ARTIFACT_KEY:random(),HIVE_SOTY_ARTIFACT_KEY_ID:'fixture',HIVE_SELECTED_EMBED_ENABLED:'true',HIVE_SELECTED_EMBED_HOST_FILE:sourceFile});
  if(renewal)Object.assign(env,{HIVE_SOTY_RENEWAL_PROFILE:'soty.human-rp-renewal.v1',HIVE_SELECTED_RENEWAL_ENABLED:'true'});
  await startHive();startConnector(true);await until(async()=>{assert.equal(connector.exitCode,null,'selected connector running; diagnostics bytes='+outputBytes);try{return(await wireHttp(connectorPort,'/apps/claim',{origin,body:{}})).status===200;}catch{return false;}});
  const publication=await owner.client.extension('apps.publication.get',{appId});
  const preparation=await owner.client.extension('apps.source.prepare',{appId,expectedPolicyEpoch:publication.policyEpoch,expectedTargetRevision:publication.activeTargetRevision,
    source:{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,port:hivePort,entryPath:'/embed',profile:RESOURCE_RUNTIME_PROFILE}});
  await owner.client.extension('apps.source.promote',{appId,requestId:'selected-admission',expectedPolicyEpoch:publication.policyEpoch,expectedTargetRevision:publication.activeTargetRevision,preparationId:preparation.preparationId,launchPolicy:'restricted',listed:false});
  await until(async()=>{const list=await owner.client.extension('apps.list');return list.apps.find(a=>a.id===appId)?.state==='ready';});
  async function launch(){const launch=await owner.client.extension('apps.launch',{appId}),url=new URL(launch.launchUrl);assert.equal((await request(embedded+'/_soty/session',{body:{ticket:url.hash.slice(1)}})).status,200);return launch;}
  let decision=0,lastInteractionId;
  async function login({signer=owner,expectCallback=302,mode='write',report=false,stayInAppSeconds}={}){const start=await request(embedded+'/api/embed/login',{body:{}});assert.equal(start.status,200,'selected start='+start.body?.error?.code);
    const page=await request(start.body.nativeUrl);assert.equal(page.status,200);const fields=Object.fromEntries([...page.text.matchAll(/name="([^"]+)" value="([^"]*)"/gu)].map(m=>[m[1],m[2]]));
    const begin=await request(native+'/account/soty/start',{fields});assert.equal(begin.status,302,'native start='+begin.body?.error?.code);
    const auth=await request(begin.location);assert.ok([302,303].includes(auth.status));await request(auth.location);const ctx=(await request(auth.location.href+'/context')).body;lastInteractionId=ctx.interactionId;
    await signer.client.extension('identity.human.approve',{interactionId:ctx.interactionId,browserNonce:ctx.browserNonce,csrf:ctx.csrf,requestId:'selected-native-'+(++decision),decision:'approve',expectedAccountId:signer.account.accountId,
      ...(stayInAppSeconds?{stayInAppSeconds}:{})},{expectedAccountId:signer.account.accountId});
    const complete=await request(auth.location.href+'/complete',{fields:{csrf:ctx.csrf}});assert.equal(complete.status,303);let resume=await request(complete.location);
    if(resume.status===200){const action=/<form method="post" action="([^"]+)">/u.exec(resume.text)?.[1],form=Object.fromEntries([...resume.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)].map(m=>[m[1],m[2]]));resume=await request(action,{fields:form});resume=await request(resume.location);}
    assert.ok([302,303].includes(resume.status));const callback=await request(resume.location);assert.equal(callback.status,expectCallback,'Native callback='+callback.body?.error?.code);if(expectCallback!==302)return callback;
    const consent=await request(callback.location);assert.equal(consent.status,200,'consent='+consent.headers['x-hive-error-code']);const consentFields=Object.fromEntries([...consent.text.matchAll(/type="hidden" name="([^"]+)" value="([^"]*)"/gu)].map(m=>[m[1],m[2]]));consentFields.mode=mode;if(report)consentFields.report='1';
    const accepted=await request(native+'/soty/connect',{fields:consentFields});assert.equal(accepted.status,302,'Native selected consent');return accepted;
  }
  function sql(callback){const db=new DatabaseSync(filename);try{db.exec('PRAGMA foreign_keys=ON');return callback(db);}finally{db.close();}}
  async function nativeSupport(){
    const ownJar=new Map(),send=(value,opts={})=>request(value,opts,ownJar),pair=await webcrypto.subtle.generateKey({name:'ECDSA',namedCurve:'P-256'},false,['sign','verify']),jwk=await webcrypto.subtle.exportKey('jwk',pair.publicKey);
    const digest=Buffer.from(await webcrypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify({crv:jwk.crv,kty:jwk.kty,x:jwk.x,y:jwk.y})))).toString('base64url'),id='dev_'+digest.slice(0,32);
    const challenge=await send(native+'/api/account/devices/challenge',{body:{id,publicJwk:jwk,label:'Synthetic Native support'}});assert.equal(challenge.status,200);
    const signature=Buffer.from(await webcrypto.subtle.sign({name:'ECDSA',hash:'SHA-256'},pair.privateKey,new TextEncoder().encode(`hive.device.register.v1\n${challenge.body.challenge}\n${id}`))).toString('base64url');
    assert.equal((await send(native+'/api/account/devices/register',{body:{id,publicJwk:jwk,label:'Synthetic Native support',challenge:challenge.body.challenge,signature}})).status,200);
    const status=(await send(native+'/api/account/session')).body;return{accountId:status.account.id,request:send};
  }
  return{request,native,embedded,origin,appId,owner,foreign,legacy,locator:locator.body,launch,login,sql,root:()=>app,profile,
    advance(ms){assert.ok(clock,'controlled clock opt-in only');clock.advance(ms);},refreshes:()=>refreshes,userinfoReads:()=>userinfoReads,
    userinfoCounts:()=>({statuses:{...userinfoStatuses},denials:{...userinfoDenials}}),
    holdUserinfo(after=1){assert.ok(Number.isSafeInteger(after)&&after>=1&&after<=12);assert.equal(userinfoBarrier,null);
      let enter,release;const entered=new Promise(resolve=>{enter=resolve;}),wait=new Promise(resolve=>{release=()=>{if(userinfoBarrier?.wait===wait)userinfoBarrier=null;held.delete(release);resolve();};});held.add(release);
      userinfoBarrier={ordinal:userinfoReads+after,entered:enter,wait};return{entered,release};},
    async install(value){const result=await request(embedded+'/_soty/session',{body:{ticket:new URL(value.launchUrl).hash.slice(1)}});
      if(result.body?.sessionCheck!==undefined){assert.equal(typeof result.body.sessionCheck==='string'&&/^[A-Za-z0-9_-]{43}$/u.test(result.body.sessionCheck),true,'closed candidate check');pendingBootCheck=result.body.sessionCheck;}return result;},
    async restartRoot(){await app.locals.closeServices();app=createHttpApp(dist,selectedOptions);await until(async()=>{try{return(await owner.client.extension('apps.list')).apps.find(a=>a.id===appId)?.state==='ready';}catch{return false;}});},
    async revokeIssuerDevice(){const backup=createClientWithStorage({projectId:'soty',endpoint:origin+'/api/connect/rpc',fetch:(url,opts)=>fetch(url,{...opts,headers:{...opts.headers,origin}})},memory());clients.push(backup);
      const pending=await backup.startEnrollment('Synthetic current-authority second Root device');await owner.client.approveEnrollment(pending.requestId,owner.account.accountId);
      await backup.previewEnrollment(pending.requestId);await backup.finishEnrollment(pending.requestId,owner.account.accountId);
      return backup.revokeDevice((await owner.client.getLocalState()).deviceId);},
    revokeIssuerGrant(){assert.equal(typeof lastInteractionId,'string');const service=app.locals.humanIdentityService;
      const grant=service.sdk.grantForInteraction(createHash('sha256').update('Interaction\0'+lastInteractionId).digest('hex'));assert.ok(Boolean(grant),'actual fixture issuer grant available');
      service.sdk.revokeByGrantId(grant.grantId);},
    async replaceIssuerClientGeneration(){await app.locals.closeServices();app=createHttpApp(dist,{...selectedOptions,humanIdentity:{...selectedOptions.humanIdentity,
      clients:selectedOptions.humanIdentity.clients.map(client=>({...client,version:2}))}});},
    nativeSupport,
    async restart(changes={}){await stop(hive);Object.assign(env,changes);await startHive();},
    counts(){return sql(db=>Object.fromEntries(['hive_accounts','hive_account_devices','hive_projects','hive_soty_identity_links','hive_selected_embed_grants','hive_selected_embed_heads','hive_feedback_tickets'].map(table=>[table,db.prepare('SELECT count(*) AS n FROM '+table).get().n])));}};
}
