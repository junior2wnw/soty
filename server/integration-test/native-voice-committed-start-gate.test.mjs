import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {randomBytes,generateKeyPairSync,randomUUID} from 'node:crypto';
import {spawn} from 'node:child_process';
import {mkdtemp,mkdir,writeFile,rm,realpath} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve,basename} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {createHttpApp} from '../http-app.js';
import {createClientWithStorage} from '../../modules/connect/browser/client.mjs';
import {runtimeTargetDigest} from '../../modules/apps/server/schema.mjs';
import {STANDARD_SELECTED_SOURCE_V2} from '../../modules/source-app/server/standard-profile.mjs';
import {createCatalog} from '../../modules/capabilities/server/catalog.mjs';
import {captureExternalApplications,composeExternalApplications} from '../external-applications.js';
import {createExistingAppHostComposition} from '../existing-app-host-composition.js';
import {createRootVoiceSourceAdapters} from '../../modules/native-voice/root-source-adapters.mjs';
import {mountRootVoiceCarrier} from '../../src/world/native-voice-carrier.mjs';
import {wireHttp,freePort} from '../../modules/source-app/test/support/ordinary-installed.mjs';

// Explicit cross-repository integration only. It is not a server/Docker/World prerequisite.
assert.ok(process.env.SOTY_NATIVE_VOICE_SOURCE_ROOT,'explicit_native_fixture_source_required');
const nativeRoot=resolve(process.env.SOTY_NATIVE_VOICE_SOURCE_ROOT);
const load=leaf=>import(pathToFileURL(join(nativeRoot,leaf)).href);
const {createKvartalVoiceConsentHost}=await load('lib/voice/native-consent.mjs');
const {createNativeVoiceBrainQueue}=await load('lib/voice/native-brain-queue.mjs');
const {createNativeVoiceHttpMount}=await load('lib/voice/http-mount.mjs');
const {createKvartalVoiceSourcePort}=await load('lib/voice/source-port.mjs');
const {createOrdinaryAppStore}=await load('modules/source-app/examples/ordinary-app/store.mjs');
const {createSourceAppBff}=await load('modules/source-app/server/bff.mjs');
const {createNativeVoiceBrowserSession}=await load('lib/voice/browser-session.mjs');
const {notifyRootVoiceTask}=await load('lib/voice/browser-carrier.mjs');
const {SOTY_CONSENT_COOKIE}=await load('lib/voice/soty-cookie.mjs');
const pause=ms=>new Promise(done=>setTimeout(done,ms)),opaque=()=>randomBytes(32).toString('base64url');
const vault=()=>{let value;return {async read(){return structuredClone(value??null);},async claim(next){value??=structuredClone(next);return structuredClone(value);},
 async compareAndSwap(revision,next){assert.equal(value.localRevision,revision);value=structuredClone(next);return structuredClone(value);}};};
async function until(check,ms=20000){const end=Date.now()+ms;while(Date.now()<end){const value=await check();if(value)return value;await pause(25);}throw new Error('voice_gate_fixture_timeout');}

function contracts(appId,binding){
 const ids={read:'app.voice:readTask',complete:'app.voice:completeTask'};
 const base={version:1,appId,title:'Synthetic installed Native Voice',description:'Genuine current signed Native task',visibility:'private',executionEnabled:true,
  resources:['app.voice:current'],recipients:['app.voice:current']};
 const task={type:'string',maxLength:160},consent={type:'object',properties:{scope:{type:'string',maxLength:64},consentId:task,conversationId:task,
  rootPrincipal:{type:'object',properties:{accountId:task,deviceId:task},required:['accountId','deviceId'],additionalProperties:false},
  expiresAt:{type:'integer'},transcript:{type:'boolean'},rootBrain:{type:'boolean'}},required:['scope','consentId','conversationId','rootPrincipal','expiresAt','transcript','rootBrain'],additionalProperties:false};
 const read={...base,capabilityId:ids.read,effects:[],inputSchema:{type:'object',properties:{operation:{type:'string',maxLength:64,enum:['voice.task.read']},taskId:task,consentId:task},required:['operation','taskId','consentId'],additionalProperties:false},
  outputSchema:{type:'object',properties:{taskId:task,consent,kind:{type:'string',maxLength:64,enum:['source-permission']},prompt:{type:'string',maxLength:8000}},
   required:['taskId','consent'],additionalProperties:false},executionBinding:{kind:'registered',handler:ids.read,version:1,binding}};
 const complete={...base,capabilityId:ids.complete,effects:['update'],inputSchema:{type:'object',properties:{operation:{type:'string',maxLength:64,enum:['voice.task.complete']},taskId:task,consentId:task,
  result:{type:'object',properties:{sourceCurrent:{type:'boolean',enum:[true]}},required:['sourceCurrent'],additionalProperties:false}},required:['operation','taskId','consentId','result'],additionalProperties:false},
  outputSchema:{type:'object',properties:{taskId:task,admitted:{type:'boolean'}},required:['taskId','admitted'],additionalProperties:false},executionBinding:{kind:'registered',handler:ids.complete,version:1,binding}};
 const catalog=createCatalog([read,complete]),reference=cap=>({capabilityId:cap.capabilityId,version:1,digest:catalog.get(cap.capabilityId,1).digest});
 return {read,complete,readReference:reference(read),completeReference:reference(complete)};
}

test('real first deny -> original BFF link -> NEW manual SAMElease committed probe; final INSERT fault cannot start capture', {timeout:120000},async t=>{
 const parent=await realpath(tmpdir()),directory=await realpath(await mkdtemp(join(parent,'voice-committed-gate-')));
 const runtimeDir=join(directory,'connector'),dataDir=join(directory,'root'),dist=join(directory,'dist');
 await mkdir(runtimeDir);await mkdir(dist);await writeFile(join(dist,'index.html'),'<!doctype html><title>Synthetic signed Root consent</title>');
 let root,child,bff,store,nativeHost,queue,mount,bridge,browserSession,disposeCarrier,sourceServer,issued;
 const clients=[],sockets=new Set(),streams=new Set(),jars=new Map(),marks={capture:0,connect:0,nativeAllow:0,permission:0,rootTurn:0,nativePublish:0,duplicateTurns:0};
 let taskEvents=[],sourceOutputBytes=0,rootActor,rootAccount,voiceAccount,record,grantCookie;
 const rootServer=createServer((req,res)=>root?root(req,res):res.writeHead(503).end());
 rootServer.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
 rootServer.on('upgrade',(req,socket,head)=>{if(!root?.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
 async function stopChild(){if(!child)return;const current=child;child=null;if(current.exitCode===null&&current.signalCode===null){current.kill('SIGTERM');
  await Promise.race([new Promise(done=>current.once('exit',done)),pause(2500)]);if(current.exitCode===null&&current.signalCode===null){const ended=new Promise(done=>current.once('exit',done));current.kill('SIGKILL');await ended;}}}
 t.after(async()=>{browserSession?.close();disposeCarrier?.();await stopChild();clients.forEach(client=>client.dispose());
  bff?.close();mount?.close();queue?.close();nativeHost?.close();store?.close();bridge?.close();await root?.locals.closeServices();
  sockets.forEach(socket=>socket.destroy());if(rootServer.listening)await new Promise(done=>rootServer.close(done));
  if(sourceServer?.listening){sourceServer.closeAllConnections();await new Promise(done=>sourceServer.close(done));}
  assert.equal(dirname(await realpath(directory)),parent);assert.match(basename(directory),/^voice-committed-gate-/u);await rm(directory,{recursive:true,force:true,maxRetries:4,retryDelay:50});});
 await new Promise(done=>rootServer.listen(0,'127.0.0.1',done));
 const rootPort=rootServer.address().port,origin='http://127.0.0.1:'+rootPort,connectorPort=await freePort(),sourcePort=await freePort(),appTlsPort=await freePort();
 console.log('fixture.root_bootstrap');
 const rootOptions={dataDir,connectOrigins:[origin],appOriginTemplate:'https://{appId}.localhost:'+appTlsPort,localConnectorPort:connectorPort};
 root=createHttpApp(dist,rootOptions);
 const ownerClient=createClientWithStorage({projectId:'soty',endpoint:origin+'/api/connect/rpc',scopedAppCleanup:true,
  fetch:(url,opts)=>fetch(url,{...opts,headers:{...opts.headers,origin}})},vault());clients.push(ownerClient);
 rootAccount=await ownerClient.bootstrap('Synthetic genuine Root Voice owner');
 // Real connector/Apps source channel, kept entirely in own loopback fixture.
 const hostFile=join(runtimeDir,'hosts.json');
 await writeFile(join(runtimeDir,'connector-config.json'),JSON.stringify({workspaceRoot:runtimeDir,allowedRoots:[runtimeDir],installId:'voice_gate_fixture'}));
 function startChild(selected){child=spawn(process.execPath,[fileURLToPath(new URL('../../scripts/soty-connector.mjs',import.meta.url)),'--port',String(connectorPort),'--scope','Dev'],{
  cwd:runtimeDir,windowsHide:true,stdio:['ignore','pipe','pipe'],env:{PATH:[dirname(process.execPath),process.env.SystemRoot?join(process.env.SystemRoot,'System32'):'/usr/bin','/bin'].join(process.platform==='win32'?';':':'),
   ...(process.env.SystemRoot?{SystemRoot:process.env.SystemRoot}:{}),TEMP:directory,TMP:directory,USERPROFILE:directory,APPDATA:join(directory,'appdata'),LOCALAPPDATA:join(directory,'localappdata'),
   SOTY_CONNECTOR_DATA_DIR:runtimeDir,SOTY_CONNECTOR_SERVER_URL:origin,SOTY_CONNECTOR_LINK_ID:'voice_gate_fixture_link_123456789012',SOTY_CONNECTOR_DEVICE_ID:'voice_gate_host',
   SOTY_CONNECTOR_DEVICE_NICK:'Synthetic Source fixture',SOTY_CONNECTOR_AUTO_UPDATE:'0',SOTY_CONNECTOR_MANAGED:'0',SOTY_CONNECTOR_UPDATE_URL:origin+'/agent/manifest.json',
   ...(selected?{SOTY_SELECTED_EMBED_HOST_FILE:hostFile}:{})}});
  child.stdout.on('data',part=>{sourceOutputBytes+=part.length;});child.stderr.on('data',part=>{sourceOutputBytes+=part.length;});}
 console.log('fixture.connector_claim');
 startChild(false);
 const claim=await until(async()=>{assert.equal(child.exitCode,null,'owned_connector_exit');try{const r=await wireHttp(connectorPort,'/apps/claim',{origin,body:{}});return r.status===200?r.value:null;}catch{return null;}},60000);
 await ownerClient.extension('apps.claim',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,claimCode:claim.claimCode});
 console.log('fixture.register_app');
 const registered=await ownerClient.extension('apps.register',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,name:'Synthetic existing Voice',port:sourcePort,entryPath:'/embed',grants:{accountIds:[],communityIds:[]}});
 const appId=registered.app.id,embedded='https://'+appId+'.localhost:'+appTlsPort,nativeOrigin='http://localhost:'+sourcePort;
 const target={appId,revision:2,ownerAccountId:rootAccount.accountId,connectorKey:['voice_gate_fixture_link_123456789012',claim.hostDeviceId,claim.connectorId].join('|'),
  port:sourcePort,entryPath:'/embed',profile:'soty.selected-human-embed.v2'};target.digest=runtimeTargetDigest(target);
 const profile={schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'voice_gate_fixture_link_123456789012',hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId},
  target:{revision:2,digest:target.digest},sourceProfile:STANDARD_SELECTED_SOURCE_V2,resource:{registryId:'soty',environmentId:'production',tenantId:rootAccount.accountId,appId,resourceId:'voice:current',
   selection:{kind:'soty.resource.v1',nativeId:'kvartalufa.voice',incarnationId:'voice-gate-current'}},issuer:origin+'/human-identity',clientId:'voice-gate-fixture',embedOrigin:embedded,nativeOrigin,parentOrigin:origin};
 await stopChild();await root.locals.closeServices();
 const sourceKey=randomBytes(32),clientSecret=opaque(),jwk=generateKeyPairSync('rsa',{modulusLength:2048}).privateKey.export({format:'jwk'});
 Object.assign(jwk,{kid:'voice-gate-fixture',use:'sig',alg:'RS256'});
 const humanIdentity={enabled:true,issuer:profile.issuer,registryId:'soty',environmentId:'production',clients:[{id:profile.clientId,label:'Synthetic actual Voice link',redirectUri:embedded+'/api/embed/callback',clientSecret}],
  jwks:{keys:[jwk]},cookieKeys:[opaque()],artifactKey:randomBytes(32),artifactKeyId:'voice-gate-fixture'};
 const scope={appId,target:profile.target,sourceProfile:profile.sourceProfile,resource:profile.resource};
 bridge=createRootVoiceSourceAdapters(scope);const catalog=contracts(appId,bridge.binding);
 const composition=createExistingAppHostComposition({enabled:true,registry:[{appId,target:profile.target,catalog:catalog.read,adapter:bridge.readAdapter},{appId,target:profile.target,catalog:catalog.complete,adapter:bridge.completeAdapter}]});
 catalog.readReference=composition.sourceReferences.find(v=>v.capabilityId===catalog.read.capabilityId);catalog.completeReference=composition.sourceReferences.find(v=>v.capabilityId===catalog.complete.capabilityId);
 // Browser windows are controlled ports; authority comes from the real SDK,
 // Root Apps channel, BFF/RP/Human flow and signed Native host. No TLS UI claim.
 const browserRootOrigin='https://root-browser-gate.invalid';
 console.log('fixture.root_configure_voice');
 root=createHttpApp(dist,{...rootOptions,humanIdentity,allowScopedEmbedMigration:true,allowSelectedResourceMigration:true,scopedEmbedProfiles:[profile],
  ...composition.httpOptions,
  nativeVoice:{enabled:true,readReference:catalog.readReference,completeReference:catalog.completeReference,sourceCredentialFor:()=>issued,
   publicMount:{appId,frameOrigin:embedded,nativeOrigin:embedded,parentOrigin:browserRootOrigin,source:profile.sourceProfile}}});
 nativeHost=createKvartalVoiceConsentHost({databasePath:join(directory,'native.sqlite'),allowedOrigins:[nativeOrigin],sessionSeconds:600});
 queue=createNativeVoiceBrainQueue({consentHost:nativeHost});
 const conversationId='00000000-0000-4000-8000-000000000031',access={owner:'synthetic_live_cookie_owner',ip:'127.0.0.1'};
 const live={authorizeLive:()=>access,liveConversationScope:()=>conversationId,conversationFor(owner,_ip,id){record={owner,conversationId:id,closeConfirmed:true};return record;},
  bindQueuedNativeBrain(){},notify(_record,event){taskEvents.push(event);notifyRootVoiceTask(nativeView,browserRootOrigin,event);for(const stream of streams)stream.onmessage?.({data:JSON.stringify(event)});}};
 mount=createNativeVoiceHttpMount({consentHost:nativeHost,queue,live});
 const nativeRequest=()=>new Request(nativeOrigin+'/api/connect/rpc',{method:'POST',headers:grantCookie?{cookie:grantCookie}:{}});
 const nativeClient=createClientWithStorage({projectId:'kvartalufa',endpoint:nativeOrigin+'/api/connect/rpc',fetch:async(_url,opts)=>{
  const input=JSON.parse(opts.body);let context;
  if(input.op==='voice.consent.allow')context=mount.beforeSignedConsent(nativeRequest(),input.args);
  const answer=await nativeHost.handle({origin:nativeOrigin,...input});
  if(input.op==='voice.consent.allow'&&answer.ok){++marks.nativeAllow;grantCookie=mount.afterSignedConsent(nativeRequest(),context,answer).split(';')[0];nativeJar.set(SOTY_CONSENT_COOKIE,answer.consentId);}
  return Response.json(answer);
 }},vault());clients.push(nativeClient);const nativeJar=new Map();
 console.log('fixture.native_bootstrap');
 voiceAccount=await nativeClient.bootstrap('Synthetic existing Native Voice profile');
 store=createOrdinaryAppStore({databasePath:join(directory,'source.sqlite'),realmId:'voice-gate',key:randomBytes(32),keyId:'synthetic-voice-gate',format:2,initialize:true});
 store.createResource({id:'kvartalufa.voice',incarnationId:profile.resource.selection.incarnationId,title:'Synthetic actual Native Voice',guestEmpty:false});
 const trackedQueue={...queue,complete(...args){++marks.nativePublish;return queue.complete(...args);}};
 bff=createSourceAppBff({profile,transportKey:sourceKey,connectorPort,storage:store.storage,
  native:createKvartalVoiceSourcePort({consentHost:nativeHost,queue:trackedQueue,store,resourceId:'kvartalufa.voice',incarnationId:profile.resource.selection.incarnationId,nativeOrigin}),
  rp:{issuer:profile.issuer,clientId:profile.clientId,clientSecret,redirectUri:embedded+'/api/embed/callback'},allowCreateEmptyGuest:false,ui:{appLabel:'Synthetic Voice',resourceLabel:'Голосовой разговор'}});
 sourceServer=createServer((req,res)=>{if(req.url==='/embed'){res.writeHead(200,{'content-type':'text/html'}).end('<!doctype html><title>Synthetic Native Voice</title>');return;}
  bff.handleRequest(req,res).then(handled=>{if(!handled)res.writeHead(404).end();});});
 await new Promise(done=>sourceServer.listen(sourcePort,'127.0.0.1',done));
 await writeFile(hostFile,JSON.stringify({schema:'soty.selected-embed-hosts.v1',entries:[{profile,key:sourceKey.toString('base64url')}]}));startChild(true);
 await until(async()=>{try{return(await wireHttp(connectorPort,'/apps/claim',{origin,body:{}})).status===200;}catch{return false;}},60000);
 console.log('fixture.publish_installed_source');
 const publication=await ownerClient.extension('apps.publication.get',{appId});
 const prepared=await ownerClient.extension('apps.source.prepare',{appId,expectedPolicyEpoch:publication.policyEpoch,expectedTargetRevision:publication.activeTargetRevision,
  source:{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,port:sourcePort,entryPath:'/embed',profile:'soty.selected-human-embed.v2'}});
 await ownerClient.extension('apps.source.promote',{appId,requestId:'voice_gate_promote_001',expectedPolicyEpoch:publication.policyEpoch,expectedTargetRevision:publication.activeTargetRevision,
  preparationId:prepared.preparationId,launchPolicy:'restricted',listed:false});
 await until(async()=>(await ownerClient.extension('apps.list')).apps.find(app=>app.id===appId)?.state==='ready');
 console.log('fixture.issue_root_access');
 const principal=(await ownerClient.extension('access.principals.create',{expectedAccountId:rootAccount.accountId,label:'Synthetic Voice gate',clientLabel:'Existing private Voice CODE'})).principal;
 const accessGrant=(await ownerClient.extension('access.grants.issue',{expectedAccountId:rootAccount.accountId,principalId:principal.id,capabilities:[catalog.read,catalog.complete].map(v=>({capabilityId:v.capabilityId,version:1})),
  resources:catalog.read.resources,effects:['update'],recipients:catalog.read.recipients,expiresAt:Date.now()+300000,allowDelegation:false,maxDepth:0,budget:{unit:'invocations',limit:20}})).grant;
 issued=await ownerClient.extension('access.credentials.issue',{expectedAccountId:rootAccount.accountId,grantId:accessGrant.id,audience:origin});issued={token:issued.token,audience:origin};

 async function wire(input,options={}){const url=new URL(input),isApp=url.origin===embedded,isRoot=url.origin===origin;assert.ok(isApp||isRoot||url.origin===nativeOrigin,'owned_fixture_origin');
  const jar=isApp?(jars.get('embed')??new Map()):isRoot?(jars.get('root')??new Map()):nativeJar;jars.set(isApp?'embed':isRoot?'root':'native',jar);
  const cookie=[...jar].map(([name,value])=>name+'='+value).join('; ');
  const reply=await wireHttp(isApp||isRoot?rootPort:sourcePort,url.pathname+url.search,{host:url.host,cookie,origin:options.body||options.fields?url.origin:undefined,...options});
  for(const raw of reply.headers['set-cookie']??[]){const pair=raw.split(';')[0],idx=pair.indexOf('=');jar.set(pair.slice(0,idx),pair.slice(idx+1));}
  return {...reply,location:reply.headers.location?new URL(reply.headers.location,url):null};}
 console.log('fixture.launch_slot');
 const launch=await ownerClient.extension('apps.launch',{appId}),launchUrl=new URL(launch.launchUrl);
 assert.equal((await wire(embedded+'/_soty/session',{body:{ticket:launchUrl.hash.slice(1)}})).status,200,'real_root_launch_slot');
 // Actual browser carrier both directions; virtual window only, no actual UI.
 const rootListeners=new Set(),nativeListeners=new Set(),rootView={location:{origin:browserRootOrigin},
  addEventListener(_name,fn){rootListeners.add(fn);},removeEventListener(_name,fn){rootListeners.delete(fn);}};
 const rootParent={postMessage(data){for(const fn of rootListeners)void fn({data,source:nativeFrame,origin:embedded});}};
 const nativeFrame={postMessage(data){for(const fn of nativeListeners)fn({data,source:rootParent,origin:browserRootOrigin});}};
 const nativeView={parent:rootParent,crypto:{randomUUID},addEventListener(_name,fn){nativeListeners.add(fn);},removeEventListener(_name,fn){nativeListeners.delete(fn);},
  EventSource:class{constructor(){streams.add(this);queueMicrotask(()=>this.onopen?.());}close(){streams.delete(this);}}};
 disposeCarrier=mountRootVoiceCarrier({view:rootView,getFrame:()=>({src:embedded+'/embed',contentWindow:nativeFrame}),isCurrent:()=>true,
  client:{getLocalState:()=>ownerClient.getLocalState(),extension(op,args,opts){if(op==='voice.agent.turn')++marks.rootTurn;return ownerClient.extension(op,args,opts).then(value=>{if(op==='voice.agent.turn')console.log(JSON.stringify({stage:'fixture.root_outcome',keys:Object.keys(value).sort(),ok:value.ok===true,schema:value.schema==='soty.native-voice-turn.v1',knownDenied:value.delivery==='pre_complete_denied',committed:value.delivery==='native_commit_ack'}));return value;}).catch(error=>{const code=Object.getOwnPropertyDescriptor(error,'code')?.value;if(['external_adapter_not_registered','voice_source_unbound','extension_unavailable'].includes(code))console.log('fixture.root_closed_code.'+code);else console.log('fixture.root_closed_code.unclassified');throw error;});}},
  mount:{appId,scope:'kvartalufa.voice',parentOrigin:browserRootOrigin,frameOrigin:embedded},appId,accountId:rootAccount.accountId});
 browserSession=createNativeVoiceBrowserSession({view:nativeView,client:nativeClient,rootOrigin:browserRootOrigin,fetchImpl:async(url,opts={})=>{
  if(url.startsWith('/api/live?voiceConsentBootstrap='))return Response.json({csrf:'synthetic_live_csrf'});
  if(url==='/soty-voice/close'){mount.withdrawOwned(new Request(nativeOrigin+'/soty-voice/close',{headers:{cookie:grantCookie}}),access);return Response.json({schema:'soty.native-voice-close.v1',withdrawn:true,closed:true,unknown:false});}
  assert.equal(url,'/api/live?rootVoice=1');++marks.permission;
  // Abort only stops observing this HTTP fixture. It never proves remote effects0.
  const remote=mount.sourcePermission(new Request(nativeOrigin+'/api/live?rootVoice=1',{method:'POST',headers:{cookie:grantCookie}}),access);
  remote.catch(()=>{});
  return new Promise((yes,no)=>{opts.signal.addEventListener('abort',()=>no(new Error('fixture_observation_aborted')),{once:true});
   remote.then(value=>yes(Response.json(value)),no);});
 }});
 const start=async options=>{await browserSession.prepare(conversationId,options);++marks.capture;++marks.connect;};
 console.log('fixture.first_permission_probe');
 await assert.rejects(start({intent:'user_start'}),/precomplete/u,'known_genuine_root_read_denial');
 assert.equal(marks.nativeAllow,1);assert.equal(marks.nativePublish,0);assert.equal(marks.capture,0);
 const firstTask=taskEvents[0],firstPermission=marks.permission;
 await assert.rejects(start(),/explicit_probe/u);assert.equal(marks.permission,firstPermission,'passive_check_does_not_enqueue');
 const consentBefore=nativeHost.detail(nativeHost.capture(firstTask.consentId,{accountId:rootAccount.accountId,deviceId:rootAccount.deviceId}));
 // Genuine existing BFF login/OIDC Native-owner approval, completion and continue.
 console.log('fixture.actual_bff_link');
 const begin=await wire(embedded+'/api/embed/login',{body:{}});assert.equal(begin.status,200,'bff_login');
 const page=await wire(begin.value.nativeUrl);assert.equal(page.status,200,'original_native_link_form');
 const fields=Object.fromEntries([...page.text.matchAll(/name="([^"]+)" value="([^"]*)"/gu)].map(m=>[m[1],m[2]]));fields.consent='yes';
 let response=await wire(nativeOrigin+'/soty/authorize',{fields});assert.equal(response.status,303,'original_native_owner_consent');
 response=await wire(response.location);assert.ok([302,303].includes(response.status),'human_authorize');const flow=response.location;
 await wire(flow);const human=(await wire(flow.href+'/context')).value;
 await ownerClient.extension('identity.human.approve',{expectedAccountId:rootAccount.accountId,interactionId:human.interactionId,browserNonce:human.browserNonce,csrf:human.csrf,requestId:'voice_gate_human_001',decision:'approve'},{expectedAccountId:rootAccount.accountId});
 response=await wire(flow.href+'/complete',{fields:{csrf:human.csrf}});assert.equal(response.status,303,'human_complete');
 response=await wire(response.location);
 if(response.status===200){const action=/<form method="post" action="([^"]+)">/u.exec(response.text)?.[1];
  const fields=Object.fromEntries([...response.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)].map(m=>[m[1],m[2]]));assert.ok(action,'human_resume_form');
  response=await wire(action,{fields});response=await wire(response.location);}
 assert.ok([302,303].includes(response.status),'human_callback_route');
 response=await wire(response.location);assert.equal(response.status,200,'source_callback');
 const href=/<a href="([^"]+)">/u.exec(response.text)?.[1];assert.ok(href?.startsWith('/api/embed/complete-link?intent='),'source_completion_anchor');
 response=await wire(new URL(href,embedded));assert.equal(response.status,200,'source_complete_link');
 const continuation=await wire(embedded+'/api/embed/session-continue',{body:{requestId:'voice_gate_continue_001'}});
 assert.equal(continuation.status,200,'actual_source_session_continue');assert.equal(continuation.value.ready,true);
 const tokenExists=(jars.get('embed')?.has('soty_rp_session')??false);assert.equal(tokenExists,false,'source_cookie_stays_owned_by_original_root_connector');
 // Root connector owns this Source cookie; no hand-created session/ready flags.
 const actualSourceRows=store.db.prepare('SELECT count(*) AS n FROM source_sessions WHERE active=1').get().n;
 assert.equal(actualSourceRows,1,'original_bff_created_one_source_session');
 console.log('fixture.explicit_linked_probe');
 await start({intent:'user_start'});
 assert.equal(marks.nativeAllow,1,'same_native_lease_after_link');assert.equal(marks.capture,1);assert.equal(marks.connect,1);
 const secondTask=taskEvents[1];assert.notEqual(secondTask.taskId,firstTask.taskId);assert.equal(secondTask.consentId,firstTask.consentId);
 assert.deepEqual(nativeHost.detail(nativeHost.capture(firstTask.consentId,{accountId:rootAccount.accountId,deviceId:rootAccount.deviceId})),consentBefore);
 // Real final SQL INSERT failure occurs AFTER Native queue publishes permission.
 console.log('fixture.final_receipt_fault');
 store.db.exec("CREATE TEMP TRIGGER fail_gate_receipt BEFORE INSERT ON native_receipts WHEN NEW.request_id NOT LIKE '@voice.pending:%' BEGIN SELECT RAISE(ABORT,'synthetic_gate_receipt_fault'); END;");
 await assert.rejects(start({intent:'user_start'}),/unknown/u,'real_final_sql_fault_gate');
 const beforeUnknown={...marks};await assert.rejects(start({intent:'user_start'}),/unknown/u);await assert.rejects(start(),/unknown/u);
 assert.equal(marks.permission,beforeUnknown.permission,'unknown_no_http_replay');assert.equal(marks.nativePublish,2,'one_success_one_fault_publication');
 assert.equal(marks.capture,1,'unknown_cannot_capture');assert.equal(marks.connect,1,'unknown_cannot_connect');
 const history=root.locals.capabilitiesService.invocations.listForOwner({accountId:rootAccount.accountId,limit:20});
 const items=history.items??history.invocations;assert.ok(items.some(row=>row.effectState==='committed'&&row.status==='succeeded'),'real_root_committed_receipt');
 assert.ok(items.some(row=>row.effectState==='unknown'),'real_root_unknown_receipt');
 const rows=store.db.prepare('SELECT request_id FROM native_receipts').all(),finalRows=rows.filter(row=>!row.request_id.startsWith('@voice.pending:')).length;
 assert.equal(finalRows,1,'no_second_committed_native_receipt');
 const observation={schema:'soty.voice-committed-start-gate-integration.v1',genuineRootSdk:true,genuineNativeSdk:true,genuineNativeOwnerConsent:true,
  originalBffLogin:true,actualHumanApproval:true,actualCompleteLink:true,actualSessionContinue:true,actualSourceTokenInOwnedConnector:actualSourceRows===1,
  sourceCookieExposedToBrowser:tokenExists,browserWindowAndLiveAccessFixture:true,physicalTlsBrowserQualified:false,
  knownPreCompleteDenied:true,passiveEnqueueAfterKnownDeny:0,newExplicitTaskAfterLink:true,sameNativeLease:true,nativeConsentCount:marks.nativeAllow,
  strictRootCommittedEnvelope:true,realFinalInsertFault:true,unknownRootReceipt:true,nativePermissionPublishedAfterFault:true,unknownCapture:0,unknownConnect:0,
  nativePublish:marks.nativePublish,finalNativeReceiptCount:finalRows,permissionTasks:marks.permission,modelCalls:0,provider:false,microphone:false,engine:false,sourceOutputBytes};
 if(process.env.SOTY_VOICE_GATE_EVIDENCE_DIR)await writeFile(join(resolve(process.env.SOTY_VOICE_GATE_EVIDENCE_DIR),'ACTUAL-FIRST-LOGIN.json'),JSON.stringify(observation,null,2)+'\n',{flag:'wx'});
});

test('captured private Voice provenance survives repeated installed CODE capture',()=>{
 const appId='app-'+'d'.repeat(32),target={revision:2,digest:'d'.repeat(64)},scope={appId,target,sourceProfile:STANDARD_SELECTED_SOURCE_V2,
  resource:{registryId:'soty',environmentId:'fixture',tenantId:'synthetic_owner',appId,resourceId:'voice:current',selection:{kind:'soty.resource.v1',nativeId:'kvartalufa.voice',incarnationId:'one'}}};
 const bridge=createRootVoiceSourceAdapters(scope),catalog=contracts(appId,bridge.binding);
 let entries=captureExternalApplications([{appId,target,catalog:catalog.complete,adapter:bridge.completeAdapter}]);
 for(let i=0;i<3;i++)entries=captureExternalApplications(entries.map(({appId,target,catalog,adapter})=>({appId,target,catalog:contracts(appId,bridge.binding).complete,adapter})));
 let scoped=0,generic=0;
 const composed=composeExternalApplications(entries,{withConnectFence:fn=>fn(),capabilities:()=>({withSnapshotOwnerAuthority(_req,fn){return fn({accountId:'synthetic_owner',deviceId:'synthetic_device'});}}),
  apps:()=>({withScopedVoiceSourceAuthority(_req,fn){++scoped;return fn();},withAppAuthority(){++generic;throw new Error('generic_should_not_replace_voice_source');}})});
 assert.equal(composed[0].adapter.withAuthority({authorization:{}},()=>true),true);
 assert.equal(scoped,1);assert.equal(generic,0);bridge.close();
});

test('wrong Apps target cannot bind the original private Voice port',()=>{
 const appId='app-'+'e'.repeat(32),target={revision:2,digest:'e'.repeat(64)},scope={appId,target,sourceProfile:STANDARD_SELECTED_SOURCE_V2,
  resource:{registryId:'soty',environmentId:'fixture',tenantId:'synthetic_owner',appId,resourceId:'voice:current',selection:{kind:'soty.resource.v1',nativeId:'kvartalufa.voice',incarnationId:'one'}}};
 const bridge=createRootVoiceSourceAdapters(scope),catalog=contracts(appId,bridge.binding);
 const entries=captureExternalApplications([{appId,target:{revision:3,digest:'f'.repeat(64)},catalog:catalog.complete,adapter:bridge.completeAdapter}]);
 assert.throws(()=>composeExternalApplications(entries,{withConnectFence:fn=>fn(),capabilities:()=>null,apps:()=>null}),/voice_source_denied/u);
 bridge.close();
});

test('cloning a captured adapter cannot manufacture original private capture provenance',()=>{
 const appId='app-'+'f'.repeat(32),target={revision:2,digest:'f'.repeat(64)},scope={appId,target,sourceProfile:STANDARD_SELECTED_SOURCE_V2,
  resource:{registryId:'soty',environmentId:'fixture',tenantId:'synthetic_owner',appId,resourceId:'voice:current',selection:{kind:'soty.resource.v1',nativeId:'kvartalufa.voice',incarnationId:'one'}}};
 const bridge=createRootVoiceSourceAdapters(scope),catalog=contracts(appId,bridge.binding);
 const captured=captureExternalApplications([{appId,target,catalog:catalog.complete,adapter:bridge.completeAdapter}])[0].adapter;
 const clone={...captured},entries=captureExternalApplications([{appId,target,catalog:catalog.complete,adapter:clone}]);
 let scoped=0,attempted=0;
 const composed=composeExternalApplications(entries,{withConnectFence:fn=>fn(),capabilities:()=>({withSnapshotOwnerAuthority(_req,fn){return fn({accountId:'synthetic_owner',deviceId:'synthetic_device'});}}),
  apps:()=>({withScopedVoiceSourceAuthority(){++scoped;throw new Error('clone_should_not_have_voice_provenance');},
   withAppAuthority(_req,fn){++attempted;return fn({target});}})});
 assert.throws(()=>composed[0].adapter.withAuthority({authorization:{}},()=>true),/voice_source_unbound/u);
 assert.equal(scoped,0);assert.equal(attempted,1);bridge.close();
});
