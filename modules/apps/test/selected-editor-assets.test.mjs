import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {HIVE_SELECTED_SOURCE,HIVE_SELECTED_KERNEL_SOURCE,selectedRoute} from '../scoped-embed/resource-route-adapters.mjs';
import {selectedResourceProfile} from '../scoped-embed/resource-profile.mjs';
import {createLocalScopedEmbedBroker} from '../scoped-embed/local-broker.mjs';
function profile(pin=HIVE_SELECTED_SOURCE){return selectedResourceProfile({schema:'soty.selected-human-embed.v2',appId:'app-'+'a'.repeat(32),
  connector:{linkId:'link',hostDeviceId:'device',connectorId:'connector'},target:{revision:1,digest:'1'.repeat(64)},sourceProfile:pin,
  resource:{registryId:'soty',tenantId:'owner',appId:'app-'+'a'.repeat(32),environmentId:'production',resourceId:'locator',selection:{kind:'hive.project.v1',nativeId:' Native / ID ',incarnationId:'scope'}},
  issuer:'https://root.test/human-identity',clientId:'hive.rp',embedOrigin:'https://hive.root.test',nativeOrigin:'https://hive.native.test',parentOrigin:'https://root.test'});}
test('kernel v1 pin/routes remain immutable; UI sourceProfile2 is an explicit separate approval with Apps8 unchanged',()=>{
  assert.deepEqual(HIVE_SELECTED_KERNEL_SOURCE,{id:'hive.selected-project',version:1,digest:'531f81eb2920f23a85ec2b4f89d681b139c8697feae23cbd84e2d1c50c9be732'});
  assert.equal(HIVE_SELECTED_SOURCE.version,2);assert.notEqual(HIVE_SELECTED_SOURCE.digest,HIVE_SELECTED_KERNEL_SOURCE.digest);
  assert.equal(selectedRoute(profile(HIVE_SELECTED_KERNEL_SOURCE),'GET','/assets/editor.js').kind,'public-ui');
  assert.throws(()=>selectedRoute(profile(HIVE_SELECTED_KERNEL_SOURCE),'GET','/_next/static/chunks/FlowEditor-hash.js'));
  assert.equal(selectedRoute(profile(),'POST','/api/embed/operations').requestBytes,1048576);
});
test('UI assets are closed public safe leaves, without query authority/traversal/build internals/native endpoints',()=>{
  for(const path of ['/_next/static/chunks/FlowEditor-A2_3.js','/_next/static/css/layout.hash.css','/_next/static/media/manrope-latin-wght-normal.hash.woff2']){
    const route=selectedRoute(profile(),'GET',path);assert.equal(route.credentialFree,true);assert.equal(route.redirects,false);assert.equal(route.responseBytes,4194304);
  }
  for(const path of ['/_next/data/build/project.json','/_next/static/chunks/a.js?projectId=other','/_next/static/chunks/a.js?x=1','/_next/static/chunks/../media/a.js','/_next/static/chunks/%61.js',
    '/_next/static/chunks/private/a.js','/_next/static/media/a.js','/_next/static/css/a.js','/_next/static/chunks/a.map','/_next/static/unknown/a.js','/_next/static/chunks/a.js#fragment','/api/account/session'])
    assert.throws(()=>selectedRoute(profile(),'GET',path));
  assert.throws(()=>selectedRoute(profile(),'POST','/_next/static/chunks/a.js'));
});
function brokerFixture(t){const p=profile(),context={schema:'soty.verified-launch-continuation.v2',reference:{id:randomBytes(32).toString('base64url'),version:1,digest:'9'.repeat(64)},profileDigest:p.digest,appId:p.appId,sourceProfile:p.sourceProfile,resource:p.resource,
  rootPrincipal:{accountId:'owner',deviceId:'root-device'},humanPrincipal:{issuer:p.issuer,subject:'actual-sub-fixture',clientId:p.clientId,clientProfileDigest:'3'.repeat(64),clientGeneration:1},
  entry:{domainId:'domain',origin:p.embedOrigin},target:p.target,policyEpoch:1,expiresAt:Date.now()+300000};
  let current=true,binding=true,handler;const requests=[];
  const broker=createLocalScopedEmbedBroker({profile:p,localPort:61231,key:randomBytes(32),readAuthority:async()=>{if(!current)throw Object.assign(new Error('revoked'),{code:'scoped_embed_authority_changed'});return context;},assertBinding:()=>binding,
    fetch:async(url,opts)=>{requests.push({url,headers:opts.headers});return handler(url,opts);}});t.after(()=>broker.close());
  return{broker,context,requests,setHandler(fn){handler=fn;},revoke(){current=false;},retire(){binding=false;},p};}
test('public asset dispatch carries no Source cookie/Root subject/MAC, accepts no new cookie/redirect and rechecks original Root after await',async t=>{
  const f=brokerFixture(t);f.setHandler(()=>new Response(JSON.stringify({schema:'soty.source-embed-auth.v1',nativeUrl:f.p.nativeOrigin+'/soty/connect?intent='+'a'.repeat(43)}),{headers:{'content-type':'application/json','set-cookie':'soty_rp_session='+'s'.repeat(43)+'; HttpOnly; Path=/; SameSite=Lax; Max-Age=300'}}));
  await f.broker.dispatch({context:f.context,method:'POST',path:'/api/embed/login',headers:{origin:f.p.embedOrigin},body:Buffer.from('{}')});
  f.setHandler(()=>new Response('synthetic public javascript',{headers:{'content-type':'text/javascript'}}));
  await f.broker.dispatch({context:f.context,method:'GET',path:'/_next/static/chunks/FlowEditor-hash.js'});
  const headerNames=Object.keys(f.requests.at(-1).headers);assert.deepEqual(headerNames.sort(),['connection','host']);
  for(const headers of [{'set-cookie':'soty_rp_session='+'s'.repeat(43)+'; HttpOnly; Path=/; SameSite=Lax; Max-Age=300'},{location:'https://other.test/a.js'}]){
    f.setHandler(()=>new Response('synthetic',{headers}));await assert.rejects(f.broker.dispatch({context:f.context,method:'GET',path:'/_next/static/chunks/a.js'}),{code:'scoped_embed_public_asset_invalid'});
  }
  f.setHandler(()=>{f.revoke();return new Response('protected returned bytes');});await assert.rejects(f.broker.dispatch({context:f.context,method:'GET',path:'/_next/static/chunks/a.js'}),{code:'scoped_embed_authority_changed'});
});
test('actual bounded broker accepts4MiB, denies more and retains valid304 cache responses without cookies',async t=>{
  const f=brokerFixture(t),call=()=>f.broker.dispatch({context:f.context,method:'GET',path:'/_next/static/chunks/a.js'});
  f.setHandler(()=>new Response(new Uint8Array(4194304)));assert.equal((await call()).body.length,4194304);
  f.setHandler(()=>new Response(new Uint8Array(4194305)));await assert.rejects(call(),{code:'scoped_embed_response_limit'});
  f.setHandler(()=>new Response(null,{status:304}));assert.equal((await call()).status,304);
});
const flush=()=>new Promise(resolve=>setImmediate(resolve));
function heldAssets(f){const pending=[];f.setHandler((url,options)=>new Promise((resolve,reject)=>{pending.push({url,resolve});options.signal.addEventListener('abort',()=>reject(Object.assign(new Error('cancelled'),{code:'scoped_embed_cancelled'})),{once:true});}));return pending;}
const assetCall=(f,index,signal)=>f.broker.dispatch({context:f.context,method:'GET',path:'/_next/static/chunks/asset-'+index+'.js'},signal);
test('public FIFO admits17 parallel modules with4 active and unchanged private admission',async t=>{
  const f=brokerFixture(t),pending=heldAssets(f),calls=Array.from({length:17},(_,i)=>assetCall(f,i));await flush();assert.equal(pending.length,4);
  f.setHandler(()=>new Response('{}'));
  assert.equal((await f.broker.dispatch({context:f.context,method:'GET',path:'/api/embed/project'})).status,200,'public preload does not consume private capacity');
  // Queued public requests use a new deterministic handler; initial4 remain held.
  for(const item of pending)item.resolve(new Response('public'));await Promise.all(calls);
  assert.deepEqual(f.requests.filter(r=>r.url.includes('/chunks/')).map(r=>Number(/asset-(\d+)/u.exec(r.url)[1])),Array.from({length:17},(_,i)=>i));
});
test('saturation returns truthful503/RetryAfter without upstream; cancelled/disposed waits release all queue reservations',async t=>{
  const f=brokerFixture(t),pending=heldAssets(f),calls=Array.from({length:36},(_,i)=>assetCall(f,i));await flush();assert.equal(pending.length,4);
  const overflow=await assetCall(f,36);assert.equal(overflow.status,503);assert.equal(overflow.headers['retry-after'],'1');assert.equal(pending.length,4);
  f.broker.close();const outcomes=await Promise.allSettled(calls);assert.ok(outcomes.every(r=>r.status==='rejected'));assert.equal(pending.length,4);
  const next=brokerFixture(t),hold=heldAssets(next),active=Array.from({length:4},(_,i)=>assetCall(next,i));await flush();
  const cancelled=new AbortController(),queued=assetCall(next,4,cancelled.signal);await flush();cancelled.abort();await assert.rejects(queued,{code:'scoped_embed_cancelled'});
  next.setHandler(()=>new Response('public'));for(const item of hold)item.resolve(new Response('public'));await Promise.all(active);assert.equal((await assetCall(next,5)).status,200);
});
test('queued revoke, retired target and profile change recheck before Source dispatch and never fetch the queued leaf',async t=>{
  for(const change of [f=>f.revoke(),f=>f.retire(),f=>f.context.profileDigest='f'.repeat(64)]){
    const f=brokerFixture(t),pending=heldAssets(f),calls=Array.from({length:5},(_,i)=>assetCall(f,i));const outcomes=Promise.allSettled(calls);await flush();assert.equal(pending.length,4);change(f);
    for(const item of pending)item.resolve(new Response('public'));assert.ok((await outcomes).every(r=>r.status==='rejected'));assert.equal(f.requests.length,4);
  }
});
test('queued wait is bounded8seconds, releases timeout reservation and never dispatches a delayed request',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});const f=brokerFixture(t),pending=heldAssets(f),active=Array.from({length:4},(_,i)=>assetCall(f,i));await flush();
  const queued=assetCall(f,4);await flush();t.mock.timers.tick(8000);const denied=await queued;assert.equal(denied.status,503);assert.equal(denied.headers['retry-after'],'1');
  f.setHandler(()=>new Response('public'));for(const item of pending)item.resolve(new Response('public'));await Promise.all(active);assert.equal(f.requests.length,4);assert.equal((await assetCall(f,5)).status,200);
});
