// Closed helper roles for universal-image-canary.mjs. Synthetic keys, loopback only.
// This file is a test harness: it imports runtime modules from the immutable image /app.
import { createServer, request as httpRequest } from 'node:http';
import { randomBytes, createHash, createCipheriv, createDecipheriv, generateKeyPairSync, webcrypto } from 'node:crypto';
import { readFile, open, lstat, readdir, mkdir, rename, writeFile as writeEvidenceFile } from 'node:fs/promises';
import { readFileSync, constants } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { CANARY_BASE, CANARY_SOURCE, CANARY_REVISION, checkDirectoryChain, verifyCanaryHarness } from './universal-image-canary.mjs';

const ORIGIN='http://127.0.0.1:8080',ISSUER=ORIGIN+'/human-identity';
export const CANARY_ROLES=Object.freeze(['initialize','prepare-synthetic-human','baseline-one','feature-one','baseline-two','restored-feature',
  'signed-baseline','signed-feature','check-baseline','check-restored-feature','capture-evidence','compare-evidence',
  'encrypted-backup','invalidate-original-config','encrypted-restore','compare-restored','compare-restored-after-read','native-tests']);
const ROLES=new Set(CANARY_ROLES);
const limit=64*1024*1024,random=()=>randomBytes(32).toString('base64url');
const require=(ok,code)=>{if(!ok)throw Object.assign(new Error(code),{code});};
const hash=value=>createHash('sha256').update(typeof value==='string'||Buffer.isBuffer(value)?value:JSON.stringify(value)).digest('hex');
const role=process.argv[2];let ROOT,SOURCE,NONCE,HARNESS,HARNESS_DIGEST;
function args() {
  const out={};for(let i=3;i<process.argv.length;i+=2){const key=process.argv[i],value=process.argv[i+1];require(['--owned-root','--fixture-source','--entry-mode','--fixture-harness','--harness-sha256'].includes(key)&&!Object.hasOwn(out,key)&&typeof value==='string','canary_helper_arguments');out[key]=value;}
  require(ROLES.has(role)&&out['--fixture-source']===CANARY_SOURCE&&new RegExp('^'+CANARY_BASE+'/canary-[a-f0-9]{32}$','u').test(out['--owned-root']||''),'canary_helper_scope');
  ROOT=out['--owned-root'];SOURCE=out['--fixture-source'];NONCE=ROOT.slice(-32);HARNESS=out['--fixture-harness'];HARNESS_DIGEST=out['--harness-sha256'];
  require(HARNESS===CANARY_BASE+'/harness-'+NONCE&&/^[a-f0-9]{64}$/u.test(HARNESS_DIGEST||''),'canary_harness_scope_invalid');return out;
}
async function owned() {
  require(process.platform==='linux'&&process.env.SOTY_SYNTHETIC_CANARY==='1','canary_linux_opt_in_required');
  await checkDirectoryChain(ROOT,{privateLeaf:true});await checkDirectoryChain(SOURCE);
  await verifyCanaryHarness(HARNESS,HARNESS_DIGEST);
  const marker=JSON.parse(await boundedFile(ROOT+'/canary-owner.json',4096));
  require(marker.schema==='soty.synthetic-canary.v1'&&marker.nonce===NONCE&&marker.revision===CANARY_REVISION,'canary_owner_mismatch');
}
async function boundedFile(file,max=limit) {
  const before=await lstat(file,{bigint:true});require(before.isFile()&&!before.isSymbolicLink()&&before.nlink===1n&&before.size<=BigInt(max),'canary_file_unsafe');
  const fd=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);try{const current=await fd.stat({bigint:true});require(current.dev===before.dev&&current.ino===before.ino&&current.size===before.size,'canary_file_changed');
    const bytes=await fd.readFile();const after=await fd.stat({bigint:true});require(bytes.length<=max&&after.size===current.size&&after.mtimeNs===current.mtimeNs&&after.ctimeNs===current.ctimeNs,'canary_file_changed');return bytes;
  }finally{await fd.close();}
}
async function write(file,bytes,{exclusive=false}={}) {
  require(file.startsWith(ROOT+'/')||file.startsWith('/data/'),'canary_write_scope');
  const parent=path.dirname(file);await mkdir(parent,{recursive:true,mode:0o700});
  await checkDirectoryChain(parent);
  const existing=await lstat(file).catch(error=>{if(error.code==='ENOENT')return null;throw error;});require(!existing||existing.isFile()&&!existing.isSymbolicLink()&&existing.nlink===1,'canary_write_unsafe');
  if(exclusive) {const fd=await open(file,'wx',0o600);try{await fd.writeFile(bytes);await fd.sync();}finally{await fd.close();}return;}
  const temporary=file+'.writing-'+randomBytes(8).toString('hex'),fd=await open(temporary,'wx',0o600);
  try{await fd.writeFile(bytes);await fd.sync();}finally{await fd.close();}await rename(temporary,file);
}
export function sealCanaryPayload(value,key,nonce,purpose) {
  require(key instanceof Uint8Array&&key.byteLength===32&&/^[a-f0-9]{32}$/u.test(nonce)&&typeof purpose==='string'&&/^[a-zA-Z0-9:_-]{1,80}$/u.test(purpose),'canary_custody_invalid');
  const plain=Buffer.from(JSON.stringify(value));require(plain.length<=limit,'canary_private_limit');
  const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(Buffer.from('soty.synthetic-canary.v1\0'+nonce+'\0'+purpose));
  const encrypted=Buffer.concat([cipher.update(plain),cipher.final()]);return Buffer.concat([iv,cipher.getAuthTag(),encrypted]);
}
export function openCanaryPayload(bytes,key,nonce,purpose) {
  require(key instanceof Uint8Array&&key.byteLength===32&&/^[a-f0-9]{32}$/u.test(nonce)&&typeof purpose==='string'&&/^[a-zA-Z0-9:_-]{1,80}$/u.test(purpose),'canary_custody_invalid');
  require(bytes.length>=29&&bytes.length<=limit+28,'canary_private_limit');const cipher=createDecipheriv('aes-256-gcm',key,bytes.subarray(0,12));
  cipher.setAAD(Buffer.from('soty.synthetic-canary.v1\0'+nonce+'\0'+purpose));cipher.setAuthTag(bytes.subarray(12,28));
  return JSON.parse(Buffer.concat([cipher.update(bytes.subarray(28)),cipher.final()]).toString('utf8'));
}
const seal=(value,key,purpose)=>sealCanaryPayload(value,key,NONCE,purpose);
const unseal=(bytes,key,purpose)=>openCanaryPayload(bytes,key,NONCE,purpose);
async function custody() {const bytes=await boundedFile(ROOT+'/custody/fixture-keys.json',16*1024);const stat=await lstat(ROOT+'/custody/fixture-keys.json');require((stat.mode&0o777)===0o600,'canary_key_mode');return JSON.parse(bytes);}
async function privateRead(name) {const keys=await custody();return unseal(await boundedFile(ROOT+'/private-state/'+name+'.enc'),Buffer.from(keys.vaultKey,'base64'),'state:'+name);}
async function privateWrite(name,value,{exclusive=false}={}) {const keys=await custody();await write(ROOT+'/private-state/'+name+'.enc',seal(value,Buffer.from(keys.vaultKey,'base64'),'state:'+name),{exclusive});}
function useClock() {
  // Fixture-only, shared file; no public time control or increased production TTLs.
  Date.now=()=>{const value=JSON.parse(readFileSync(ROOT+'/clock.json','utf8')).now;require(Number.isSafeInteger(value)&&value>0,'canary_clock_invalid');return value;};
}
async function until(check,ms=10000) {const deadline=performance.now()+ms;do{const result=await check();if(result)return result;await new Promise(done=>setTimeout(done,25));}while(performance.now()<deadline);throw Object.assign(new Error('canary_readiness_timeout'),{code:'canary_readiness_timeout'});}
async function json(url,options={}) {const checked=new URL(url);require(checked.hostname==='127.0.0.1'&&['8080','8082','8083'].includes(checked.port),'canary_http_scope');
  const response=await fetch(url,{...options,redirect:'manual',signal:AbortSignal.timeout(10000)});const text=await response.text();require(Buffer.byteLength(text)<=65536,'canary_response_limit');
  return {status:response.status,body:text?JSON.parse(text):null};}

async function initialize() {
  await write(ROOT+'/custody/fixture-keys.json',JSON.stringify({schema:'soty.synthetic-keys.v1',vaultKey:randomBytes(32).toString('base64'),connectorToken:random()}),{exclusive:true});
  await write(ROOT+'/custody/backup-custody.key',randomBytes(32),{exclusive:true});
  await write(ROOT+'/clock.json',JSON.stringify({now:Date.now()}),{exclusive:true});
  // Match retained pre-Universal Notes2/Caps3 data through existing explicit
  // fixture-only migration constructors. Root HTTP does not auto-migrate them.
  const {createNotesService}=await import('/app/modules/notes/server/index.mjs');
  createNotesService({databasePath:'/data/notes/notes.sqlite',projectId:'soty',allowNativeMigration:true}).close();
  const {createCapabilitiesService}=await import('/app/modules/capabilities/server/index.mjs');
  createCapabilitiesService({databasePath:'/data/capabilities/capabilities.sqlite',projectId:'soty',allowNativeMigration:true,actorActive:()=>false}).close();
  createCapabilitiesService({databasePath:'/data/capabilities/capabilities.sqlite',projectId:'soty',allowOAuthMigration:true,actorActive:()=>false}).close();
  const {createRoomStore}=await import('/app/server/room-store.js'),rooms=createRoomStore('/data');
  try {const room=await rooms.load('synthetic_canary_'+NONCE);rooms.claimAuth(room,random());const key=randomBytes(32),iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);
    const encrypted=Buffer.concat([cipher.update('synthetic opaque legacy room'),cipher.final(),cipher.getAuthTag()]);
    rooms.appendUpdate(room,{kind:'update',id:'canary_update_'+NONCE,nonce:iv.toString('base64'),ciphertext:encrypted.toString('base64'),deviceId:'synthetic_room_device'});key.fill(0);
  } finally {rooms.close();}
  return {legacyEncryptedRoomWritten:true};
}
async function prepareHuman() {
  const jwk=generateKeyPairSync('rsa',{modulusLength:2048}).privateKey.export({format:'jwk'});Object.assign(jwk,{kid:'canary-synthetic',use:'sig',alg:'RS256'});
  const config={enabled:true,issuer:ISSUER,registryId:'soty',environmentId:'production',jwks:{keys:[jwk]},cookieKeys:[random()],artifactKey:randomBytes(32).toString('base64'),artifactKeyId:'canary-synthetic',
    clients:['canary-alpha','canary-beta'].map((id,index)=>({id,label:'Synthetic '+id,redirectUri:'http://127.0.0.1:'+(8082+index)+'/oidc/callback',clientSecret:random(),version:2})),
    renewal:{admissionEnabled:true,clientIds:['canary-alpha','canary-beta']},rpKey:randomBytes(32).toString('base64')};
  await write(ROOT+'/custody/synthetic-human.json',JSON.stringify(config),{exclusive:true});return {syntheticPrivateConfigPrepared:true};
}
async function entry(mode) {
  require(mode==='baseline'||mode==='feature','canary_entry_mode');useClock();
  const {forcedLegacyMode}=await import('/app/server/universal-mode.js');require(forcedLegacyMode===(mode==='baseline'),'canary_compiled_mode');
  // Baseline never opens/parses the Human config file and never enables Universal.
  let humanIdentity,rpKey;
  if(mode==='feature') {const config=JSON.parse(await boundedFile(ROOT+'/custody/synthetic-human.json',16*1024));({rpKey,...humanIdentity}=config);humanIdentity.artifactKey=Buffer.from(humanIdentity.artifactKey,'base64');}
  const {createHttpApp}=await import('/app/server/http-app.js');
  const app=createHttpApp('/app/dist',{dataDir:'/data',connectOrigins:[ORIGIN],appHosting:{},appOriginTemplate:'http://{appId}.localhost:8080',namedAppZone:'',discoveryOrigin:'',
    capabilityAudience:ORIGIN,nativeNotesEnabled:true,universalAppsEnabled:mode==='feature',...(humanIdentity?{humanIdentity,humanIdentityRenewalMigration:true}:{})});
  const source=createServer((_req,res)=>res.end('<!doctype html><title>Synthetic canary source</title>'));
  const server=createServer(app);server.keepAliveTimeout=65000;server.headersTimeout=70000;
  server.on('upgrade',(req,socket,head)=>{if(!app.locals.appsService.handleUpgrade(req,socket,head))socket.destroy();});
  await new Promise(done=>source.listen(8081,'127.0.0.1',done));await new Promise(done=>server.listen(8080,'127.0.0.1',done));
  let rps=[];if(mode==='feature') {const {createHumanBffFixture}=await import('./universal-canary-bff.mjs');
    rps=await Promise.all(humanIdentity.clients.map((client,index)=>createHumanBffFixture({clientId:client.id,clientSecret:client.clientSecret,port:8082+index,
      sessionDatabasePath:ROOT+'/private-state/rp-'+index+'.sqlite',sessionKey:Buffer.from(rpKey,'base64')})));await Promise.all(rps.map(rp=>rp.configure(ISSUER)));}
  const {startUniversalOperator}=await import('/app/server/universal-operator.js');const operator=await startUniversalOperator({capture:app.locals.captureUniversalPreparedness});require(operator.supported,'canary_native_port_unavailable');
  await write(ROOT+'/receipts/'+role+'-ready.json',JSON.stringify({ready:true,role,mode,nonce:NONCE}),{exclusive:true});
  let stopPromise;const stop=()=>stopPromise??=(async()=>{
    try {await operator.close();await Promise.all(rps.map(rp=>rp.close()));server.closeAllConnections();source.closeAllConnections();
      await Promise.all([server,source].map(item=>new Promise(done=>item.close(done))));await app.locals.closeServices();process.exitCode=0;
    }catch{process.exitCode=1;} })();
  await new Promise(resolve=>{process.once('SIGTERM',resolve);process.once('SIGINT',resolve);});
  await stop();
}

export async function createSyntheticActorSeed({emptyState,publicJwk,signingDeviceId,sealLocalRoot}) {
    // Synthetic-only extractable generation. Production nonextractable vaults are never exported.
    const [signing,encryption,storage]=await Promise.all([webcrypto.subtle.generateKey({name:'ECDSA',namedCurve:'P-256'},true,['sign','verify']),
      webcrypto.subtle.generateKey({name:'ECDH',namedCurve:'P-256'},true,['deriveBits']),webcrypto.subtle.generateKey({name:'AES-GCM',length:256},true,['encrypt','decrypt'])]);
    const keys={signing:await webcrypto.subtle.exportKey('jwk',signing.privateKey),encryption:await webcrypto.subtle.exportKey('jwk',encryption.privateKey),storage:await webcrypto.subtle.exportKey('jwk',storage)};
    const state=emptyState('soty',ORIGIN+'/api/connect/rpc'),signingPublicJwk=publicJwk(await webcrypto.subtle.exportKey('jwk',signing.publicKey)),
      encryptionPublicJwk=publicJwk(await webcrypto.subtle.exportKey('jwk',encryption.publicKey)),deviceId=await signingDeviceId(signingPublicJwk);
    state.installations=[{deviceId,label:'Synthetic durable owner',createdAt:new Date().toISOString(),signingPublicJwk,encryptionPublicJwk,accountId:null,rootEnvelope:null,vaultRevision:0,revoked:false,enrollmentRequestId:null,recoveryFingerprint:null}];state.activeDeviceId=deviceId;
    const importedStorage=await webcrypto.subtle.importKey('jwk',keys.storage,{name:'AES-GCM'},false,['encrypt','decrypt']),root=randomBytes(32);
    try {state.installations[0].rootEnvelope=await sealLocalRoot({...state.installations[0],storageKey:importedStorage},'soty',root);}finally{root.fill(0);}
    return {keys,state};
}
export async function hydrateSyntheticActorSeed(persisted) {
  const keys={signingPrivateKey:await webcrypto.subtle.importKey('jwk',persisted.keys.signing,{name:'ECDSA',namedCurve:'P-256'},false,['sign']),
    encryptionPrivateKey:await webcrypto.subtle.importKey('jwk',persisted.keys.encryption,{name:'ECDH',namedCurve:'P-256'},false,['deriveBits']),
    storageKey:await webcrypto.subtle.importKey('jwk',persisted.keys.storage,{name:'AES-GCM'},false,['encrypt','decrypt'])};
  return {...structuredClone(persisted.state),installations:persisted.state.installations.map(installation=>({...structuredClone(installation),...keys}))};
}
async function actor() {
  const {emptyState}=await import('/app/modules/connect/browser/storage.mjs');const {publicJwk,signingDeviceId,sealLocalRoot}=await import('/app/modules/connect/browser/crypto.mjs');
  let persisted;try{persisted=await privateRead('actor');}catch(error){if(error.code!=='ENOENT')throw error;
    persisted=await createSyntheticActorSeed({emptyState,publicJwk,signingDeviceId,sealLocalRoot});await privateWrite('actor',persisted,{exclusive:true});}
  const initial=await hydrateSyntheticActorSeed(persisted),keys=Object.fromEntries(['signingPrivateKey','encryptionPrivateKey','storageKey'].map(key=>[key,initial.installations[0][key]]));
  const hydrate=value=>({...structuredClone(value),installations:value.installations.map(installation=>({...structuredClone(installation),...keys}))});
  const save=async value=>{const state=structuredClone(value);for(const installation of state.installations)for(const key of Object.keys(keys))delete installation[key];persisted={...persisted,state};await privateWrite('actor',persisted);return hydrate(state);};
  const storage={async read(){return hydrate(persisted.state);},async claim(value){return hydrate(persisted.state);},async compareAndSwap(revision,value){require(persisted.state.localRevision===revision,'canary_vault_conflict');return save(value);}};
  const {createClientWithStorage}=await import('/app/modules/connect/browser/client.mjs');const client=createClientWithStorage({projectId:'soty',endpoint:ORIGIN+'/api/connect/rpc',
    fetch:(url,options)=>fetch(url,{...options,headers:{...options.headers,origin:ORIGIN}})},storage);
  const account=await client.bootstrap('Synthetic canary owner');return {client,account};
}
async function connector(signer) {
  const keys=await custody(),identity={linkId:'canary_link_'+NONCE,hostDeviceId:'canary_host_'+NONCE,connectorId:'canary_connector_'+NONCE,name:'Synthetic task-owned source'};
  const registered=await json(ORIGIN+'/api/connectors/register',{method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+keys.connectorToken},
    body:JSON.stringify({linkId:identity.linkId,deviceId:identity.hostDeviceId,connectorId:identity.connectorId,scope:'Dev',protocol:2,capabilities:['apps']})});require(registered.status===200&&registered.body.ok===true,'canary_connector_register_failed');
  const {createLocalAppsRuntime}=await import(pathToFileURL(SOURCE+'/scripts/agent-modules/local-apps.mjs'));
  const runtime=createLocalAppsRuntime({randomSecret:random,digest:value=>hash(value),createWebSocket:url=>new globalThis.WebSocket(url),httpRequest,
    encodeBase64:bytes=>Buffer.from(bytes).toString('base64'),decodeBase64:value=>Buffer.from(value,'base64')},{identity,token:keys.connectorToken,serverUrl:ORIGIN});
  runtime.start();await until(()=>runtime.status().connected);const claim=await runtime.claim();
  await signer.client.extension('apps.claim',{hostDeviceId:claim.hostDeviceId,connectorId:claim.connectorId,claimCode:claim.claimCode});return {runtime,registration:{hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId,name:'Synthetic immutable source',port:8081,entryPath:'/',grants:{accountIds:[],communityIds:[]}}};
}
async function signedBaseline() {
  useClock();const owner=await actor();let runtime;
  try {const link=await connector(owner);runtime=link.runtime;const created=await owner.client.extension('apps.register',link.registration);require(created.app?.id&&!created.universalRegistration,'canary_baseline_universal_enabled');
    await until(async()=> (await owner.client.extension('apps.list')).apps.find(item=>item.id===created.app.id)?.state==='ready');
    const note=await owner.client.extension('notes.put',{expectedAccountId:owner.account.accountId,noteId:'canary_note_'+NONCE,mutationId:'canary_mutation_'+NONCE,expectedRevision:0,
      title:'Synthetic canary',body:'Preserved opaque scenario',items:[],color:'plain',pinned:false,state:'active'});
    const principal=(await owner.client.extension('access.principals.create',{expectedAccountId:owner.account.accountId,label:'Synthetic bounded native effect'})).principal;
    const grant=(await owner.client.extension('access.grants.issue',{expectedAccountId:owner.account.accountId,principalId:principal.id,capabilities:[{capabilityId:'notes.createDraft',version:1}],
      resources:['notes:new'],effects:['create'],recipients:['soty:notes'],expiresAt:Date.now()+3600000,budget:{unit:'invocations',limit:2}})).grant;
    const credential=await owner.client.extension('access.credentials.issue',{expectedAccountId:owner.account.accountId,grantId:grant.id,audience:ORIGIN});
    const native=await json(ORIGIN+'/api/capabilities/v1/notes/drafts',{method:'POST',headers:{'content-type':'application/json',origin:ORIGIN,authorization:'Bearer '+credential.token},
      body:JSON.stringify({idempotencyKey:'canary_native_'+NONCE,title:'Synthetic native effect',body:'Exact source receipt'})});require(native.status===201&&native.body.invocation?.status==='succeeded','canary_native_effect_failed');
    require(typeof note.noteId==='string'&&note.revision===1,'canary_note_receipt_invalid');
    await privateWrite('scenario',{accountId:owner.account.accountId,deviceId:owner.account.deviceId,appId:created.app.id,noteId:note.noteId,nativeInvocationId:native.body.invocation.invocationId,registration:link.registration},{exclusive:true});
    return {signedConnect:true,apps:1,notes:2,nativeCommittedEffects:1};
  } finally {runtime?.stop();owner.client.dispose();}
}
async function browser(saved=[]) {
  const jar=new Map(saved.map(value=>[value.host+'\0'+value.path+'\0'+value.name,value]));
  async function request(input,{fields}={}) {const url=new URL(input);require(url.protocol==='http:'&&url.hostname==='127.0.0.1'&&['8080','8082','8083'].includes(url.port),'canary_browser_origin');
    const cookie=[...jar.values()].filter(value=>value.host===url.hostname&&(url.pathname===value.path||url.pathname.startsWith(value.path.endsWith('/')?value.path:value.path+'/'))).map(value=>value.name+'='+value.value).join('; ');
    const response=await fetch(url,{redirect:'manual',signal:AbortSignal.timeout(10000),...(fields?{method:'POST',body:new URLSearchParams(fields)}:{}),headers:{...(cookie?{cookie}:{}),...(fields?{Origin:url.origin,'Sec-Fetch-Site':'same-origin'}:{})}});
    for(const raw of response.headers.getSetCookie()) {const [pair,...attr]=raw.split(';'),equal=pair.indexOf('='),attrs=Object.fromEntries(attr.map(value=>{const i=value.indexOf('=');return i<0?[value.trim().toLowerCase(),true]:[value.slice(0,i).trim().toLowerCase(),value.slice(i+1).trim()];}));
      require(!attrs.domain,'canary_cookie_domain');const value={host:url.hostname,name:pair.slice(0,equal),value:pair.slice(equal+1),path:attrs.path||'/'};const key=value.host+'\0'+value.path+'\0'+value.name;
      if(!value.value||attrs['max-age']==='0')jar.delete(key);else jar.set(key,value);require(jar.size<=128,'canary_cookie_limit');}
    const text=await response.text();require(Buffer.byteLength(text)<=65536,'canary_browser_limit');const redirect=response.headers.get('location');
    return {status:response.status,text,body:response.headers.get('content-type')?.includes('application/json')?JSON.parse(text):null,location:redirect?new URL(redirect,url):null};
  }
  return {request,snapshot:()=>[...jar.values()]};
}
async function login(owner,wire,index) {
  const rp='http://127.0.0.1:'+(8082+index),start=await wire.request(rp+'/login');require(start.status===302,'canary_rp_login');
  const authorize=await wire.request(start.location);require([302,303].includes(authorize.status)&&authorize.location.origin===ORIGIN,'canary_oidc_authorize');
  require((await wire.request(authorize.location)).status===200,'canary_interaction_document');const context=await wire.request(authorize.location.href+'/context');require(context.status===200,'canary_interaction_context');
  await owner.client.extension('identity.human.approve',{expectedAccountId:owner.account.accountId,interactionId:context.body.interactionId,browserNonce:context.body.browserNonce,csrf:context.body.csrf,
    requestId:'canary_human_'+index+'_'+NONCE,decision:'approve',stayInAppSeconds:86400},{expectedAccountId:owner.account.accountId});
  let response=await wire.request(authorize.location.href+'/complete',{fields:{csrf:context.body.csrf}});require(response.status===303,'canary_signed_approval');response=await wire.request(response.location);
  if(response.status===200) {const action=/<form method="post" action="([^"]+)">/u.exec(response.text)?.[1];require(action===ISSUER+'/session/end/confirm','canary_autoform_origin');
    const hidden=Object.fromEntries([...response.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)].map(match=>[match[1],match[2]]));response=await wire.request(action,{fields:hidden});require(response.location,'canary_autoform_redirect');response=await wire.request(response.location);}
  require([302,303].includes(response.status)&&response.location.origin+response.location.pathname===rp+'/oidc/callback','canary_rp_callback_pin');
  require((await wire.request(response.location)).status===200,'canary_jose_code_exchange');const me=await wire.request(rp+'/me');require(me.status===200&&me.body.identity?.issuer===ISSUER&&me.body.localAccountId,'canary_rp_userinfo');return me.body;
}
async function signedFeature() {
  useClock();const owner=await actor(),previous=await privateRead('scenario');let runtime;
  try {require(owner.account.accountId===previous.accountId,'canary_account_replaced');const link=await connector(owner);runtime=link.runtime;
    const current=await owner.client.extension('apps.register',previous.registration);require(current.app.id===previous.appId&&current.universalRegistration?.state==='ready','canary_app_or_registration_replaced');
    const source=current.universalRegistration.descriptor.app.source,context=(await owner.client.extension('apps.feedback.context',{appId:previous.appId})).context;
    const sent=await owner.client.extension('apps.feedback.submit',{appId:previous.appId,installationId:context.installationId,requestId:'canary_feedback_'+NONCE,body:'Synthetic retained feedback',attachments:[]});
    const repeated=await owner.client.extension('apps.feedback.submit',{appId:previous.appId,installationId:context.installationId,requestId:'canary_feedback_'+NONCE,body:'Synthetic retained feedback',attachments:[]});
    require(repeated.replayed&&repeated.receipt.ticketId===sent.receipt.ticketId,'canary_feedback_replay');
    const base={appId:previous.appId,installationId:context.installationId,ticketId:sent.receipt.ticketId};
    const reply=await owner.client.extension('apps.feedback.reply',{...base,requestId:'canary_reply_'+NONCE,expectedRevision:1,body:'Synthetic support response'});
    const ready=await owner.client.extension('apps.feedback.status',{...base,requestId:'canary_status_'+NONCE,expectedRevision:reply.ticket.revision,status:'ready_to_check'});
    const accepted=await owner.client.extension('apps.feedback.accept',{...base,requestId:'canary_accept_'+NONCE,expectedRevision:ready.ticket.revision});require(accepted.ticket.status==='resolved','canary_reporter_acceptance');
    const wire=await browser(),before=[await login(owner,wire,0),await login(owner,wire,1)];require(before[0].identity.sub===before[1].identity.sub&&before[0].localAccountId!==before[1].localAccountId,'canary_cross_app_profile');
    await write(ROOT+'/clock.json',JSON.stringify({now:Date.now()+310000}));
    const after=await Promise.all([8082,8083].map(port=>wire.request('http://127.0.0.1:'+port+'/me')));
    require(after.every((value,index)=>value.status===200&&value.body.localAccountId===before[index].localAccountId&&value.body.identity.sub===before[index].identity.sub),'canary_real_refresh_failed');
    await privateWrite('scenario',{...previous,source,feedback:base,localIds:before.map(value=>value.localAccountId),subject:before[0].identity.sub,cookies:wire.snapshot()});
    return {sameRootAccount:true,sameApp:true,signedFeedback:true,feedbackResolvedByReporter:true,realRpCount:2,realRefreshAfterAccessExpiry:true};
  } finally {runtime?.stop();owner.client.dispose();}
}
async function checkBaseline() {
  useClock();const owner=await actor(),previous=await privateRead('scenario');try{
    require(owner.account.accountId===previous.accountId,'canary_account_replaced');const apps=await owner.client.extension('apps.list');require(apps.apps.some(value=>value.id===previous.appId),'canary_app_lost');
    require((await owner.client.extension('notes.get',{expectedAccountId:owner.account.accountId,noteId:previous.noteId})).note.revision===1,'canary_note_lost');
    const disabled=await json(ISSUER+'/.well-known/openid-configuration');require(disabled.status===503,'canary_baseline_human_enabled');
    let rejected=false;try{await owner.client.extension('apps.feedback.context',{appId:previous.appId});}catch(error){rejected=error.code==='unsupported_operation';}
    require(rejected,'canary_baseline_feedback_enabled');return {signedExistingAccount:true,appsPreserved:true,notesPreserved:true,humanDisabled:true};
  }finally{owner.client.dispose();}
}
async function checkRestoredFeature() {
  useClock();const previous=await privateRead('scenario'),wire=await browser(previous.cookies);
  const replies=await Promise.all([8082,8083].map(port=>wire.request('http://127.0.0.1:'+port+'/me')));
  require(replies.every((value,index)=>value.status===200&&value.body.localAccountId===previous.localIds[index]&&value.body.identity.sub===previous.subject),'canary_restored_rp_session_lost');
  const owner=await actor();try{const current=await owner.client.extension('apps.universal.get',{expectedAccountId:previous.accountId,appId:previous.appId});
    require(current.registration.state==='ready'&&JSON.stringify(current.registration.descriptor.app.source)===JSON.stringify(previous.source),'canary_restored_source_pin_changed');
    const ticket=await owner.client.extension('apps.feedback.get',previous.feedback);require(ticket.ticket.status==='resolved','canary_restored_feedback_lost');
  }finally{owner.client.dispose();}return {sameIssuerAndSubject:true,separateLocalAccountsPreserved:true,privateRpSessionsPreserved:true,sourcePinsPreserved:true};
}

export const CANARY_EVIDENCE_QUERIES=Object.freeze({
  rooms:['rooms-v2.sqlite',{state:'SELECT room_id,sequence,auth FROM room_state LIMIT 3',events:'SELECT room_id,sequence,payload_json FROM room_events ORDER BY sequence LIMIT 8'}],
  connect:['connect/accounts.sqlite',{accounts:'SELECT * FROM accounts LIMIT 4',devices:'SELECT * FROM installations LIMIT 4',vaults:'SELECT * FROM vaults LIMIT 4'}],
  apps:['apps/registry.sqlite',{apps:'SELECT * FROM local_apps LIMIT 4',source:'SELECT * FROM app_runtime_targets ORDER BY app_id,revision LIMIT 8'}],
  notes:['notes/notes.sqlite',{meta:'SELECT * FROM notes_meta ORDER BY key LIMIT 8',notes:'SELECT * FROM notes ORDER BY id LIMIT 8',creates:'SELECT * FROM note_native_creates LIMIT 4'}],
  capabilities:['capabilities/capabilities.sqlite',{meta:'SELECT * FROM cap_metadata ORDER BY key LIMIT 16',invocations:'SELECT * FROM cap_invocations LIMIT 4',receipts:'SELECT * FROM cap_receipts LIMIT 4'}],
  registration:['app-registration/registry.sqlite',{heads:'SELECT * FROM registration_heads LIMIT 4',versions:'SELECT * FROM registration_versions ORDER BY generation LIMIT 4',receipts:'SELECT * FROM registration_receipts LIMIT 4'}],
  feedback:['feedback/feedback.sqlite',{installations:'SELECT * FROM feedback_installations LIMIT 4',tickets:'SELECT * FROM feedback_tickets LIMIT 4',messages:'SELECT * FROM feedback_messages ORDER BY ordinal LIMIT 8'}],
  human:['human-identity/identity.sqlite',{meta:'SELECT * FROM human_identity_meta ORDER BY key LIMIT 6',interactions:'SELECT * FROM human_identity_interactions ORDER BY uid_hash LIMIT 8',families:'SELECT * FROM human_identity_grant_bindings ORDER BY client_id LIMIT 8',
    // Session artifacts can be touched by userinfo. Retained consumed token evidence must survive exactly.
    consumed:'SELECT * FROM human_identity_artifacts WHERE consumed_at IS NOT NULL ORDER BY model,id_hash LIMIT 16'}],
});
function normalize(value) {if(value instanceof Uint8Array)return {cipherBase64:Buffer.from(value).toString('base64')};if(Array.isArray(value))return value.map(normalize);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,item])=>[key,normalize(item)]));return value;}
async function evidence() {
  // Every caller is a stopped-writer RO job. Preserve original WAL/ciphertext;
  // SQLite may create SHM only in this private child's scratch copy.
  const {snapshotStorage}=await import(pathToFileURL(SOURCE+'/deploy/connector/storage-snapshot.mjs'));
  const scratch=await snapshotStorage('/data','/tmp/soty-canary-evidence',48*1024*1024);
  await mkdir(scratch+'/connect',{mode:0o700});
  for(const suffix of ['','-wal','-shm','-journal']) {
    const filename='/data/connect/accounts.sqlite'+suffix;
    const stat=await lstat(filename).catch(error=>{if(error.code==='ENOENT')return null;throw error;});
    if(stat)await writeEvidenceFile(scratch+'/connect/accounts.sqlite'+suffix,await boundedFile(filename,8*1024*1024),{flag:'wx',mode:0o600});
  }
  const result={};for(const [store,[file,queries]] of Object.entries(CANARY_EVIDENCE_QUERIES)) {const filename=scratch+'/'+file;const stat=await lstat(filename);require(stat.isFile()&&!stat.isSymbolicLink()&&stat.size<=limit,'canary_evidence_file');
    const db=new DatabaseSync(filename,{readOnly:true});try{db.exec('PRAGMA query_only=ON');result[store]={version:db.prepare('PRAGMA user_version').get().user_version};
      for(const [key,query]of Object.entries(queries))result[store][key]=normalize(db.prepare(query).all());
    }finally{db.close();}}
  require(result.rooms.events.length===1&&result.connect.accounts.length===1&&result.apps.apps.length===1&&result.notes.notes.length===2
    &&result.notes.creates.length===1&&result.capabilities.invocations.length===1&&result.registration.heads.length===1&&result.feedback.tickets.length===1
    &&result.human.families.length===2&&result.human.families.every(row=>row.stay_in_app_seconds===86400&&row.revoked_at===null)
    &&result.human.consumed.filter(row=>row.model==='RefreshToken').length===2,'canary_actual_write_evidence_missing');
  return result;
}
async function captureEvidence() {await privateWrite('evidence',await evidence(),{exclusive:true});return {versionedProductStoresWritten:7,retainedConnectAccounts:1,immutableSourceTargets:1,humanFamilies:2,consumedRefreshTombstones:2,ciphertextRecordedPrivately:true};}
async function compareEvidence() {require(JSON.stringify(await evidence())===JSON.stringify(await privateRead('evidence')),'canary_private_evidence_changed');return {exactPrivateRowsAndCiphertext:true};}
export async function snapshotCanaryTree(directory,namespace,budget={bytes:0,files:0,directories:0}) {
  require(['data','private'].includes(namespace),'canary_backup_namespace');const files=[];
  async function walk(current,relative='',depth=0) {require(depth<=8&&++budget.directories<=64,'canary_backup_directory_limit');const stat=await lstat(current);require(stat.isDirectory()&&!stat.isSymbolicLink(),'canary_backup_path');
    const entries=await readdir(current,{withFileTypes:true});require(entries.length<=128,'canary_backup_count');for(const entry of entries.sort((a,b)=>a.name.localeCompare(b.name))){require(/^[A-Za-z0-9_.-]{1,128}$/u.test(entry.name)&&!['.','..'].includes(entry.name),'canary_backup_name');const name=relative?relative+'/'+entry.name:entry.name,file=current+'/'+entry.name;
      if(entry.isDirectory())await walk(file,name,depth+1);else{require(entry.isFile()&&!entry.isSymbolicLink()&&++budget.files<=256,'canary_backup_path');const bytes=await boundedFile(file);require((budget.bytes+=bytes.length)<=limit,'canary_backup_size');files.push({namespace,path:name,bytes:bytes.toString('base64')});}}}
  await walk(directory);return files;
}
const CONFIG_FILES=Object.freeze({'fixture-keys.json':'custody/fixture-keys.json','synthetic-human.json':'custody/synthetic-human.json','clock.json':'clock.json'});
async function backupKey() {const key=await boundedFile(ROOT+'/custody/backup-custody.key',32);require(key.length===32&&((await lstat(ROOT+'/custody/backup-custody.key')).mode&0o777)===0o600,'canary_backup_custody_invalid');return key;}
async function backup() {const budget={bytes:0,files:0,directories:0},files=[...await snapshotCanaryTree('/data','data',budget),...await snapshotCanaryTree(ROOT+'/private-state','private',budget)];
  for(const [name,file]of Object.entries(CONFIG_FILES)){const bytes=await boundedFile(ROOT+'/'+file,32*1024);require((budget.bytes+=bytes.length)<=limit&&++budget.files<=256,'canary_backup_size');files.push({namespace:'configuration',path:name,bytes:bytes.toString('base64')});}
  const key=await backupKey();try{await write(ROOT+'/custody/coherent-backup.enc',seal({schema:'soty.synthetic-cold-backup.v1',files},key,'coherent-backup'),{exclusive:true});}finally{key.fill(0);}
  return {encrypted:true,coherentWritersStopped:true,configurationEncrypted:true,externalBackupKeyExcluded:true,fileCount:files.length};}
async function invalidateConfig() {for(const file of Object.values(CONFIG_FILES))await write(ROOT+'/'+file,'{"synthetic_fixture_unavailable":true}');
  return {originalConfigurationUnavailable:true,externalBackupCustodyPreserved:true};}
export function checkedCanaryRestoreFiles(archive) {
  require(archive&&Object.keys(archive).sort().join(',')==='files,schema'&&archive.schema==='soty.synthetic-cold-backup.v1'&&Array.isArray(archive.files)&&archive.files.length<=256,'canary_restore_archive');
  let total=0;const files=new Set(),directories=new Set(),result=[];
  for(const item of archive.files) {require(item&&Object.keys(item).sort().join(',')==='bytes,namespace,path'&&['data','private','configuration'].includes(item.namespace)&&typeof item.path==='string'
      &&item.path.length<=1024&&/^[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*$/u.test(item.path),'canary_restore_path');
    const parts=item.path.split('/'),full=item.namespace+'/'+item.path;require(parts.length<=9&&parts.every(value=>value.length<=128&&value!=='.'&&value!=='..')&&!files.has(full),'canary_restore_path');
    for(let index=1;index<parts.length;index++)directories.add(item.namespace+'/'+parts.slice(0,index).join('/'));
    require(directories.size<=64,'canary_restore_directory_limit');files.add(full);
    require(item.namespace!=='configuration'||Object.hasOwn(CONFIG_FILES,item.path),'canary_restore_configuration');
    require(typeof item.bytes==='string'&&item.bytes.length<=limit*2,'canary_restore_size');const bytes=Buffer.from(item.bytes,'base64');require(bytes.toString('base64')===item.bytes&&(total+=bytes.length)<=limit,'canary_restore_size');
    result.push({namespace:item.namespace,path:item.path,bytes});
  }
  require([...files].every(file=>!directories.has(file)),'canary_restore_path');require(Object.keys(CONFIG_FILES).every(file=>files.has('configuration/'+file)),'canary_restore_configuration');return result;
}
async function restore() {
  require((await readdir('/data')).length===0,'canary_restore_volume_not_empty');const key=await backupKey();let archive;
  try{archive=unseal(await boundedFile(ROOT+'/custody/coherent-backup.enc',limit+28),key,'coherent-backup');}finally{key.fill(0);}
  const items=checkedCanaryRestoreFiles(archive);
  for(const item of items){const file=item.namespace==='configuration'?ROOT+'/'+CONFIG_FILES[item.path]:(item.namespace==='data'?'/data':ROOT+'/private-state')+'/'+item.path;await write(file,item.bytes,{exclusive:item.namespace==='data'});}
  for(const item of items.filter(value=>value.namespace==='configuration'))require((await boundedFile(ROOT+'/'+CONFIG_FILES[item.path],32*1024)).equals(item.bytes),'canary_restore_configuration_changed');
  return {authenticatedEncryptedBackup:true,restoredIntoNewOwnedVolume:true,configurationRestoredFromCiphertext:true,files:archive.files.length};
}
async function nativeTests() {
  // A separate fresh /tmp namespace: never competes with an active operator socket.
  const child=spawn(process.execPath,['--test','--test-reporter=tap','/app/server/test/universal-operator.test.mjs'],{stdio:['ignore','pipe','pipe'],env:{PATH:process.env.PATH,NODE_ENV:'test'}});
  let stdout=Buffer.alloc(0),stderrBytes=0;child.stdout.on('data',chunk=>{stdout=Buffer.concat([stdout,chunk]);if(stdout.length>256*1024)child.kill('SIGKILL');});child.stderr.on('data',chunk=>{stderrBytes+=chunk.length;if(stderrBytes>64*1024)child.kill('SIGKILL');});
  const code=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',resolve);});require(code===0&&stdout.length<=256*1024,'canary_native_tests_failed');process.stdout.write(stdout);
}

if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href) try {
  process.umask(0o077);
  const options=args();await owned();let result;
  if(role==='native-tests')await nativeTests();
  else if(['baseline-one','feature-one','baseline-two','restored-feature'].includes(role))await entry(options['--entry-mode']);
  else {const actions={'initialize':initialize,'prepare-synthetic-human':prepareHuman,'signed-baseline':signedBaseline,'signed-feature':signedFeature,'check-baseline':checkBaseline,
    'check-restored-feature':checkRestoredFeature,'capture-evidence':captureEvidence,'compare-evidence':compareEvidence,'compare-restored':compareEvidence,'compare-restored-after-read':compareEvidence,'encrypted-backup':backup,'invalidate-original-config':invalidateConfig,'encrypted-restore':restore};
    result=await actions[role]();process.stdout.write(JSON.stringify({ok:true,role,...result}));}
} catch(error) {const code=/^canary_[a-z0-9_]{1,90}$/u.test(error?.code||'')?error.code:'canary_helper_failed';
  // Selected error codes only, never messages, stacks, headers or private bodies.
  const sourceCode=/^(?:[a-z][a-z0-9_]{1,90}|ERR_[A-Z0-9_]{1,70})$/u.test(error?.code||'')?error.code:null;
  process.stdout.write(JSON.stringify({ok:false,role:ROLES.has(role)?role:'unknown',code,sourceCode}));process.exitCode=1;}
