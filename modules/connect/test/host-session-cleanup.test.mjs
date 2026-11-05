import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createClientWithStorage} from '../browser/client.mjs';
import {ConnectError} from '../browser/crypto.mjs';
import {validateState} from '../browser/storage.mjs';
import {createConnectService} from '../server/index.mjs';
import {createAppLauncher} from '../../../src/world/app-launch.mjs';
import {createAppHostSessionRenewal} from '../../../src/world/app-host-session-renewal.mjs';

const projectId='host-cleanup-sdk-test',origin='https://host-cleanup.invalid',endpoint=origin+'/rpc';
const appId='app-'+ 'a'.repeat(32),domainId='dom_'+ 'b'.repeat(32);
const clone=value=>value===null?null:structuredClone(value);
const opaque=letter=>letter.repeat(43);
const scope={projectId,endpoint};

// Actual SDK signatures/challenges/active profiles and server revocation; the
// extension returns a synthetic closed descriptor. This is not an Apps grant,
// real cookie admission, browser IndexedDB, or Root restart acceptance.
function memoryStorage(){
 let value=null;
 return{readError:null,async read(){if(this.readError)throw this.readError;return clone(value);},
  async claim(candidate){if(value===null){validateState(candidate,scope);value=clone(candidate);}return clone(value);},
  async compareAndSwap(expected,candidate){if(value?.localRevision!==expected)throw new ConnectError('LOCAL_CONFLICT','Concurrent write');validateState(candidate,scope);value=clone(candidate);return clone(value);}};
}
async function fixture(t,{factory=createClientWithStorage,enabled=true}={}){
 const observed=[],clients=[];let reply=null,afterResponse=null;
 const connect=createConnectService({databasePath:':memory:',projectId,allowedOrigins:[origin],extensions:[{
  operations:new Set(['apps.launch','apps.session.context','apps.session.renew','apps.session.confirm','apps.session.close','apps.scoped.close']),
  execute({op,args,actor}){
   observed.push({op,args:clone(args),accountId:actor.accountId,deviceId:actor.deviceId});
   if(op==='apps.session.close'){assert.deepEqual(Object.keys(args).sort(),['appId','frameId','lease','revision']);return{closed:true};}
   if(op==='apps.scoped.close'){assert.deepEqual(Object.keys(args).sort(),['appId','handle']);return{closed:true};}
   return clone(typeof reply==='function'?reply(op,args):reply);
  },
 }]});
 const fetcher=async(_url,init)=>{
  const request=JSON.parse(init.body),result=await connect.handle({origin,...request});
  if(result.ok&&request.proof&&afterResponse)await afterResponse(request,result);
  return new Response(JSON.stringify(result),{status:result.ok?200:400,headers:{'content-type':'application/json'}});
 };
 const options={projectId,endpoint,fetch:fetcher,scopedAppCleanup:enabled,createChannel:()=>({postMessage(){},close(){}})};
 const storage=memoryStorage(),client=factory(options,storage),seedB=factory(options,memoryStorage());clients.push(client,seedB);
 t.after(()=>{for(const item of clients)item.dispose();connect.close();});
 const a=await client.bootstrap('Synthetic A'),b=await seedB.bootstrap('Synthetic B');
 // Save B through the supported enrollment protocol. No private state is read,
 // transferred between stores or injected into an installation.
 const enrollment=await client.startEnrollment('B on this synthetic browser');
 await seedB.approveEnrollment(enrollment.requestId,b.accountId);await client.previewEnrollment(enrollment.requestId);
 await client.finishEnrollment(enrollment.requestId,b.accountId);await client.switchProfile(a.accountId);
 const second=factory(options,storage);clients.push(second);
 const member=(extra={})=>({schema:'soty.app-host-session.v1',frameId:opaque('F'),lease:opaque('A'),revision:1,expiresAt:Date.now()+86400000,...extra});
 const host=(extra={})=>({entry:{appId,domainId,origin:'https://app.apps.invalid',path:'/'},hostSession:member(),hostSessionExpiresAt:Date.now()+3600000,...extra});
 return{client,second,seedB,a,b,observed,storage,member,host,
  result(value){reply=value;},afterResponse(value){afterResponse=value;},
  launch:()=>client.extension('apps.launch',{appId,hostSession:true},{expectedAccountId:a.accountId}),
  context:value=>client.extension('apps.session.context',{appId,frameId:value.frameId,lease:value.lease,revision:value.revision,currentEntryPath:'/'},{expectedAccountId:a.accountId}),
  closes:()=>observed.filter(item=>item.op==='apps.session.close'),
 };
}

async function ownedCloseOracle(t,factory=createClientWithStorage){
 const f=await fixture(t,{factory});f.result(f.host());const reply=await f.launch(),cleanup=f.client.appSlotCleanup(reply);
 assert.equal(typeof cleanup,'function','a real signed host reply must capture its fixed cleanup');
 assert.equal(f.client.appSlotCleanup(clone(reply)),null,'a JSON copy is not a retained signer');
 await assert.rejects(cleanup({appId:'app-'+ 'f'.repeat(32)}),{code:'INVALID_ARGUMENT'});
 reply.hostSession.lease=opaque('Z');reply.entry.appId='app-'+ 'f'.repeat(32);
 await f.client.switchProfile(f.b.accountId);
 await assert.rejects(f.client.extension('apps.session.close',{appId,frameId:opaque('F'),lease:opaque('A'),revision:1},{expectedAccountId:f.a.accountId}),{code:'ACTIVE_PROFILE_CHANGED'});
 await cleanup();await cleanup();assert.equal(f.closes().length,1);
 assert.deepEqual(f.closes()[0],{op:'apps.session.close',args:{appId,frameId:opaque('F'),lease:opaque('A'),revision:1},accountId:f.a.accountId,deviceId:f.a.deviceId});
 assert.equal((await f.client.getLocalState()).accountId,f.b.accountId,'fixed A cleanup cannot activate or mutate B');
}
test('actual signed SDK cleanup closes only captured A after a normal same-client switch to B',t=>ownedCloseOracle(t));

async function refreshedDeadlineOracle(t,factory=createClientWithStorage){
 t.mock.timers.enable({apis:['Date','setTimeout'],now:1800000000000});
 const f=await fixture(t,{factory}),initial=f.member({expiresAt:Date.now()+30000});
 f.result(f.host({hostSession:initial,hostSessionExpiresAt:initial.expiresAt}));const first=await f.launch(),cleanup=f.client.appSlotCleanup(first);
 assert.equal(typeof cleanup,'function');
 const admitted={...initial,expiresAt:Date.now()+86400000};
 f.result(f.host({schema:'soty.app-host-session-context.v1',state:'current',hostSession:admitted,hostSessionExpiresAt:Date.now()+3600000}));
 const current=await f.context(initial);assert.equal(f.client.appSlotCleanup(current),cleanup,'refresh one owned purpose rather than consuming another capacity slot');
 t.mock.timers.tick(20*60000);await f.client.switchProfile(f.b.accountId);await cleanup();
 assert.equal(f.closes().length,1,'30s initial reservation cleanup must follow fresh admitted HTTP/proof budget at 20min');
 assert.equal(f.closes()[0].accountId,f.a.accountId);assert.equal((await f.client.getLocalState()).accountId,f.b.accountId);
}
test('fresh signed same-proof context refreshes initial30s cleanup through 20min without losing original signer',t=>refreshedDeadlineOracle(t));

test('context without actual HTTP deadline cannot invent or extend a signer retention budget',async t=>{
 t.mock.timers.enable({apis:['Date','setTimeout'],now:1800000000000});const f=await fixture(t),initial=f.member({expiresAt:Date.now()+30000});
 f.result(f.host({hostSession:initial,hostSessionExpiresAt:initial.expiresAt}));const launch=await f.launch(),cleanup=f.client.appSlotCleanup(launch);
 f.result(f.host({schema:'soty.app-host-session-context.v1',state:'renew-required',hostSessionExpiresAt:null}));
 const context=await f.context(initial);assert.equal(f.client.appSlotCleanup(context),cleanup);t.mock.timers.tick(31000);
 await f.client.switchProfile(f.b.accountId);await cleanup();assert.equal(f.closes().length,0,'null HTTP cannot keep the old signer alive past its signed budget');
 await f.client.switchProfile(f.a.accountId);const only=await f.context(initial);assert.equal(f.client.appSlotCleanup(only),null,'no newly retained signer without an actual HTTP deadline');
});

test('host proof and HTTP deadline expire independently and private callback cannot outlive either',async t=>{
 t.mock.timers.enable({apis:['Date','setTimeout'],now:1800000000000});const f=await fixture(t);
 for(const [proofMs,httpMs] of [[30000,30000],[86400000,60000]]){
  const proof=f.member({frameId:opaque(proofMs===30000?'P':'Q'),expiresAt:Date.now()+proofMs});f.result(f.host({hostSession:proof,hostSessionExpiresAt:Date.now()+httpMs}));
  const cleanup=f.client.appSlotCleanup(await f.launch());assert.equal(typeof cleanup,'function');t.mock.timers.tick(httpMs+1);await cleanup();
 }
 assert.equal(f.closes().length,0);
});

test('Source cleanup keeps its original fixed operation and five-minute budget',async t=>{
 t.mock.timers.enable({apis:['Date','setTimeout'],now:1800000000000});const f=await fixture(t);
 f.result({entry:{appId},runtimeProfile:'soty.selected-human-embed.v2',scopedCloseHandle:opaque('S'),scopedSlotExpiresAt:Date.now()+300000});
 const cleanup=f.client.appSlotCleanup(await f.launch());assert.equal(typeof cleanup,'function');t.mock.timers.tick(300001);await cleanup();
 assert.equal(f.observed.some(item=>item.op==='apps.scoped.close'),false);
 f.result({entry:{appId},runtimeProfile:'soty.selected-human-embed.v1',scopedCloseHandle:opaque('T'),scopedSlotExpiresAt:Date.now()+300000});
 const current=f.client.appSlotCleanup(await f.launch());await f.client.switchProfile(f.b.accountId);await current();
 const close=f.observed.find(item=>item.op==='apps.scoped.close');assert.equal(close.accountId,f.a.accountId);assert.deepEqual(close.args,{appId,handle:opaque('T')});
});

test('automatic cleanup of a late A response does not expose it to a cross-tab B activation',async t=>{
 const f=await fixture(t);f.result(f.host());f.afterResponse(async request=>{if(request.op==='apps.launch')await f.second.switchProfile(f.b.accountId);});
 await assert.rejects(f.launch(),{code:'ACTIVE_PROFILE_CHANGED'});assert.equal(f.closes().length,1);assert.equal(f.closes()[0].accountId,f.a.accountId);
 assert.equal((await f.client.getLocalState()).accountId,f.b.accountId);
});

test('revocation, local credential read failure and SDK disposal do not turn cleanup into a general inactive RPC',async t=>{
 const f=await fixture(t);f.result(f.host());const cleanup=f.client.appSlotCleanup(await f.launch());
 f.storage.readError=new ConnectError('LOCAL_UNAVAILABLE','Synthetic unavailable store');await assert.rejects(cleanup(),{code:'LOCAL_UNAVAILABLE'});f.storage.readError=null;assert.equal(f.closes().length,0);
 const recovery=await f.client.prepareRecovery();await f.client.confirmRecovery(recovery);
 await f.client.revokeDevice(f.a.deviceId);await assert.rejects(cleanup(),{code:'DEVICE_REVOKED'});assert.equal(f.closes().length,0);
 f.client.dispose();await assert.rejects(cleanup(),{code:'CLIENT_DISPOSED'});
});

test('malformed/mixed/foreign host descriptors fail closed, and opt-out never captures a signer',async t=>{
 const f=await fixture(t);
 for(const mutate of [value=>({...value,entry:{...value.entry,appId:'app-'+ 'c'.repeat(32)}}),value=>({...value,hostSession:{...value.hostSession,extra:true}}),
  value=>({...value,hostSession:{...value.hostSession,revision:0}}),value=>({...value,hostSession:{...value.hostSession,lease:'short'}}),
  value=>({...value,hostSessionExpiresAt:value.hostSession.expiresAt+1}),value=>({...value,runtimeProfile:'soty.selected-human-embed.v2'})]){
  f.result(mutate(f.host()));await assert.rejects(f.launch(),{code:'INVALID_SERVER_RESPONSE'});
 }
 assert.equal(f.closes().length,0);const other=await fixture(t,{enabled:false});other.result(other.host());const raw=await other.launch();assert.equal(other.client.appSlotCleanup(raw),null);
});

async function mutant(anchor,replacement){
 const url=new URL('../browser/client.mjs',import.meta.url),source=readFileSync(url,'utf8');assert.equal(source.split(anchor).length,2,'change exactly one production guard');
 const code=source.replace(anchor,replacement).replace(/from '(\.\/?[^']+)'/gu,(_match,path)=>"from '"+new URL(path,url).href+"'");
 return(await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'))).createClientWithStorage;
}
test('old missing-host decoder is detected by the unchanged captured-owner close oracle',async t=>{
 const broken=await mutant('if(result.hostSession!==undefined){','if(false){');
 await assert.rejects(ownedCloseOracle(t,broken),/must capture its fixed cleanup/u);
});
test('a same-proof dedupe that keeps the initial30s deadline fails the unchanged20min oracle',async t=>{
 const broken=await mutant('prior.expiresAt=deadline;','/* broken: keep the obsolete reservation deadline */');
 await assert.rejects(refreshedDeadlineOracle(t,broken),/cleanup must follow fresh admitted HTTP\/proof budget/u);
});

test('the signed Core envelope reaches the actual ordinary controller without stripping its private cleanup ownership',async t=>{
 const f=await fixture(t),entry={appId,domainId,origin:'https://app.apps.invalid',path:'/'},pin={schema:'soty.app-launch-binding.v1',policyEpoch:1,targetRevision:2,targetDigest:'d'.repeat(64),profile:'soty.relay-restricted.v1',bindingFloor:2};
 const runtimeOrigin='https://app-m'+appId.slice(4)+'-'+ 'c'.repeat(24)+'.apps.invalid';let member=f.member(),candidate=null,checked=false;
 const dto=value=>({entry,launchBinding:pin,runtimeOrigin,hostSession:value,hostSessionExpiresAt:Date.now()+120000});
 f.result((op,args)=>{
  if(op==='apps.launch')return{launchUrl:runtimeOrigin+'/_soty/boot?path=%2F#'+opaque('T'),...dto(member)};
  if(op==='apps.session.context')return{schema:'soty.app-host-session-context.v1',state:'current',...dto(member)};
  if(op==='apps.session.renew'){candidate={...member,lease:opaque('B'),revision:2};return{launchUrl:runtimeOrigin+'/_soty/boot?path=%2F&mode=renew#'+opaque('U'),expiresAt:Date.now()+30000,hostSessionRequestId:args.requestId,...dto(candidate)};}
  assert.equal(op,'apps.session.confirm');assert.equal(checked,true,'only the explicit synthetic cookie verifier enables confirmation');member=candidate;
  return{schema:'soty.app-host-session-confirmation.v1',requestId:args.requestId,confirmed:true,...dto(member)};
 });
 const launcher=createAppLauncher({target:{appId},accountId:f.a.accountId,instanceId:'main',shellUrl:origin+'/#mine',isCurrent:()=>true,
  async request(){const raw=await f.launch();assert.equal(raw.ok,true,'actual Connect wire envelope, not the hand-authored DTO model');
   return{url:raw.launchUrl,...dto(raw.hostSession),hostSessionCleanup:f.client.appSlotCleanup(raw)};}});
 await launcher.launch();const before=launcher.hostSession(),contextReplies=[];
 const helper=createAppHostSessionRenewal({readBinding:()=>launcher.hostSession(),isCurrent:()=>true,shellUrl:origin+'/#mine',
  context:async args=>{const raw=await f.context(args);contextReplies.push(raw);return raw;},
  request:args=>f.client.extension('apps.session.renew',{appId,...args},{expectedAccountId:f.a.accountId}),
  confirm:args=>f.client.extension('apps.session.confirm',{appId,...args},{expectedAccountId:f.a.accountId}),
  bootstrap:async()=>{checked=true;},commit:(previous,next)=>launcher.commitHostSession(previous,next),
  close:()=>assert.fail('trusted raw-reply cleanup must retain its original actor'),cleanupForReply:raw=>f.client.appSlotCleanup(raw)});
 t.after(()=>{helper.dispose();launcher.dispose();});assert.equal(await helper.renew('reset'),true);assert.equal(launcher.hostSession().hostSession.revision,2);
 assert.equal(launcher.hostSession().slot,before.slot);assert.ok(contextReplies.every(raw=>raw.ok===true));assert.equal(typeof launcher.hostSession().cleanup,'function');
});
