import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve} from 'node:path';
import {randomBytes} from 'node:crypto';
import {createConnectService} from '../../connect/server/index.mjs';
import {createClientWithStorage} from '../../connect/browser/client.mjs';
import {STANDARD_SELECTED_SOURCE,STANDARD_SELECTED_SOURCE_V2,STANDARD_SELECTED_SOURCE_V3,selectedRoute} from '../scoped-embed/resource-route-adapters.mjs';
import {selectedResourceProfile} from '../scoped-embed/resource-profile.mjs';
import {createLocalScopedEmbedBroker} from '../scoped-embed/local-broker.mjs';

function profile(pin=STANDARD_SELECTED_SOURCE_V2){return selectedResourceProfile({schema:'soty.selected-human-embed.v2',appId:'app-'+ 'a'.repeat(32),
  connector:{linkId:'asset-link',hostDeviceId:'asset-device',connectorId:'asset-connector'},target:{revision:1,digest:'1'.repeat(64)},sourceProfile:pin,
  resource:{registryId:'asset-registry',tenantId:'asset-owner',appId:'app-'+ 'a'.repeat(32),environmentId:'fixture',resourceId:'asset-resource',selection:{kind:'soty.resource.v1',nativeId:'opaque Case / A',incarnationId:'asset-incarnation'}},
  issuer:'https://asset-root.invalid/human-identity',clientId:'asset-client',embedOrigin:'https://asset-embed.invalid',nativeOrigin:'https://asset-native.invalid',parentOrigin:'https://asset-root.invalid'});}

async function fixture(t){
  const directory=await mkdtemp(join(tmpdir(),'soty-standard-assets-')),requests=[],sockets=new Set(),p=profile();let actor,handler;
  const connect=createConnectService({databasePath:join(directory,'root.sqlite'),projectId:'soty',allowedOrigins:[p.parentOrigin],extensions:[{
    operations:new Set(['fixture.captureActor']),execute({actor:issued}){connect.withActorAuthorityFence(issued,()=>{actor=issued;});return{captured:true};}}]});
  let state;const client=createClientWithStorage({projectId:'soty',endpoint:p.parentOrigin+'/api/connect/rpc',fetch:async(_url,options)=>{
    const result=await connect.handle({...JSON.parse(options.body),origin:p.parentOrigin});return Response.json(result);}},
    {async read(){return structuredClone(state??null);},async claim(value){state??=structuredClone(value);return structuredClone(state);},async compareAndSwap(revision,value){assert.equal(state.localRevision,revision);state=structuredClone(value);return structuredClone(state);}});
  const account=await client.bootstrap('Synthetic asset Root owner');await client.extension('fixture.captureActor',{});
  const source=createServer((req,res)=>{requests.push({path:req.url,method:req.method,headers:Object.keys(req.headers).sort()});handler(req,res);});
  source.on('connection',s=>{sockets.add(s);s.once('close',()=>sockets.delete(s));});await new Promise(done=>source.listen(0,'127.0.0.1',done));
  const context={schema:'soty.verified-launch-continuation.v2',reference:{id:randomBytes(32).toString('base64url'),version:1,digest:'9'.repeat(64)},profileDigest:p.digest,appId:p.appId,sourceProfile:p.sourceProfile,resource:p.resource,
    rootPrincipal:{accountId:account.accountId,deviceId:account.deviceId},humanPrincipal:{issuer:p.issuer,subject:'fixture-sub',clientId:p.clientId,clientProfileDigest:'3'.repeat(64),clientGeneration:1},
    entry:{domainId:'asset-domain',origin:p.embedOrigin},target:p.target,policyEpoch:1,expiresAt:Date.now()+300000};
  let binding=true;const broker=createLocalScopedEmbedBroker({profile:p,localPort:source.address().port,key:randomBytes(32),
    readAuthority:async()=>connect.withActorAuthorityFence(actor,()=>context),assertBinding:()=>binding});
  t.after(async()=>{broker.close();client.dispose();connect.close();for(const s of sockets)s.destroy();await new Promise(done=>source.close(done));assert.equal(dirname(resolve(directory)),resolve(tmpdir()));await rm(directory,{recursive:true,force:true});});
  return{p,broker,context,requests,client,account,setHandler(fn){handler=fn;},retire(){binding=false;}};
}

test('actual Root broker to Native HTTP serves a declared standard2 asset credential-free after a private cookie exists',async t=>{
  const f=await fixture(t);f.setHandler((_req,res)=>{res.setHeader('content-type','application/json');res.setHeader('set-cookie','soty_rp_session='+ 's'.repeat(43)+'; HttpOnly; Path=/; SameSite=Lax; Max-Age=300');
    res.end(JSON.stringify({schema:'soty.source-embed-auth.v1',nativeUrl:f.p.nativeOrigin+'/soty/connect?intent='+ 'a'.repeat(43)}));});
  assert.equal((await f.broker.dispatch({context:f.context,method:'POST',path:'/api/embed/login',headers:{origin:f.p.embedOrigin},body:Buffer.from('{}')})).status,200);
  f.setHandler((_req,res)=>{res.setHeader('content-type','text/javascript');res.end('/* synthetic public Native bundle */');});
  const result=await f.broker.dispatch({context:f.context,method:'GET',path:'/assets/native-bundle.js'});
  assert.equal(result.status,200);assert.equal(result.body.toString(),'/* synthetic public Native bundle */');
  assert.deepEqual(f.requests.at(-1).headers,['connection','host']);
  console.info(JSON.stringify({case:'standard2_native_asset_proxy',status:result.status,credentialHeaders:0,currentRootCoreActor:true}));
});

test('standard2/3 declared extensions stay closed, pins/data routes unchanged and standard1 remains legacy-denied',()=>{
  for(const pin of [STANDARD_SELECTED_SOURCE_V2,STANDARD_SELECTED_SOURCE_V3]){
    for(const extension of ['js','css','woff2','svg','png']){const r=selectedRoute(profile(pin),'GET','/assets/native-bundle.'+extension);assert.equal(r.credentialFree,true);assert.equal(r.redirects,false);assert.equal(r.responseBytes,4194304);}
    for(const path of ['/assets/a.js?x=1','/assets/%61.js','/assets/../a.js','/assets/private/a.js','/assets/a..js','/assets/a.map','/assets/.js','/assets/a.jpg','/_next/static/a.js','/api/v1/pages/other'])assert.throws(()=>selectedRoute(profile(pin),'GET',path));
    assert.throws(()=>selectedRoute(profile(pin),'POST','/assets/a.js'));assert.equal(selectedRoute(profile(pin),'POST','/api/embed/invoke').requestBytes,65536);
  }
  assert.throws(()=>selectedRoute(profile(STANDARD_SELECTED_SOURCE),'GET','/assets/a.js'));
});

test('actual public proxy rejects response cookies/redirects and retired binding before source fetch',async t=>{
  const f=await fixture(t),call=()=>f.broker.dispatch({context:f.context,method:'GET',path:'/assets/a.js'});
  f.setHandler((_req,res)=>res.end('public'));assert.equal((await call()).status,200);
  f.setHandler((_req,res)=>{res.setHeader('set-cookie','soty_rp_session='+ 's'.repeat(43)+'; Path=/');res.end('must not deliver');});await assert.rejects(call(),{code:'scoped_embed_public_asset_invalid'});
  f.setHandler((_req,res)=>{res.writeHead(302,{location:'https://foreign.invalid/a.js'});res.end();});await assert.rejects(call(),{code:'scoped_embed_public_asset_invalid'});
  const count=f.requests.length;f.retire();await assert.rejects(call(),{code:'scoped_embed_binding_changed'});assert.equal(f.requests.length,count);
});
