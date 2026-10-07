import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {generateKeyPairSync,sign} from 'node:crypto';
import {createConnectService,digestArgs} from '../../connect/server/index.mjs';
import {createConnectHandler} from '../../connect/server/http.mjs';
import {createManagedReviewsPort} from '../server/managed-port.mjs';
import {contractDigest} from '../../app-contract/json.mjs';

function identity(){const a=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),b=generateKeyPairSync('ec',{namedCurve:'prime256v1'});return{privateKey:a.privateKey,publicJwk:a.publicKey.export({format:'jwk'}),encryptionPublicJwk:b.publicKey.export({format:'jwk'})};}
async function fixture(t){
  const directory=await mkdtemp(join(tmpdir(),'soty-managed-brand-')),retained=new Map();let service;
  const extension={operations:new Set(['test.capture']),execute({actor}){retained.set(actor.deviceId,actor);return{captured:true};}};
  const server=createServer((req,res)=>handler(req,res));server.keepAliveTimeout=65000;server.headersTimeout=70000;
  await new Promise(done=>server.listen(0,'127.0.0.1',done));const origin='http://127.0.0.1:'+server.address().port;
  service=createConnectService({databasePath:join(directory,'connect.sqlite'),projectId:'managed-brand',allowedOrigins:[origin],extensions:[extension]});const handler=createConnectHandler(service);
  t.after(async()=>{server.closeAllConnections();await new Promise(done=>server.close(done));service.close();await rm(directory,{recursive:true,force:true});});
  async function post(body){const res=await fetch(origin+'/api/connect/rpc',{method:'POST',headers:{origin,'content-type':'application/json'},body:JSON.stringify({protocol:1,...body})});const result=await res.json();assert.equal(result.ok,true,result.error?.code);return result;}
  async function signed(who,op,args={}){const challenge=await post({op:'challenge',args:{operation:op,digest:digestArgs(args)}});return post({op,args,proof:{challengeId:challenge.challengeId,publicJwk:who.publicJwk,signature:sign('sha256',Buffer.from(challenge.message),{key:who.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}});}
  return{service,retained,signed};
}
test('real signed HTTP Connect7 preserves original actor proof and refuses copies/foreign/revoked actors before Source dispatch',async t=>{
  const f=await fixture(t),owner=identity(),phone=identity(),foreign=identity();
  const a=await f.signed(owner,'bootstrap',{label:'Synthetic owner',encryptionPublicJwk:owner.encryptionPublicJwk});
  await f.signed(foreign,'bootstrap',{label:'Synthetic foreign',encryptionPublicJwk:foreign.encryptionPublicJwk});
  const start=await f.signed(phone,'enrollment.start',{label:'Synthetic second',encryptionPublicJwk:phone.encryptionPublicJwk});
  await f.signed(owner,'enrollment.approve',{requestId:start.requestId,wrappedKey:{schema:'synthetic.encrypted',ciphertext:'synthetic'}});
  await f.signed(phone,'enrollment.finish',{requestId:start.requestId,expectedAccountId:a.accountId});
  await f.signed(owner,'test.capture');await f.signed(phone,'test.capture');await f.signed(foreign,'test.capture');
  const original=f.retained.get(a.deviceId);let calls=0;const subject={kind:'app',id:'app'};
  const receipt=()=>({protocol:'povedai.managed-reviews.v1',localSubject:subject,entityType:'product',bindingId:'binding',siteId:'site',siteKey:'site-key',objectId:'object',objectSlug:'object-slug',subjectId:'subject',placementId:'placement',generation:1,canDisplay:false});
  // Source is explicitly synthetic in this actor test. The separate mixed
  // fixture exercises maintained Human verification + actual Source commit.
  const source={withActor:async(_credential,callback)=>callback({}),...Object.fromEntries(['provision','context','list','collect','publish','moderate'].map(op=>[op,async()=>{calls++;return receipt();}]))};
  const port=createManagedReviewsPort({providerRef:{id:'povedai:managed',version:1,digest:contractDigest({protocol:'fixture'})},source,
    withRootActorAuthority:(actor,callback)=>f.service.withActorAuthorityFence(actor,()=>callback({accountId:actor.accountId,deviceId:actor.deviceId})),
    withAppAuthority:({actor,appId},callback)=>{if(actor.accountId!==a.accountId)throw new Error('foreign');return callback({appId,ownerId:a.accountId,accountId:actor.accountId,appRevision:1,policyEpoch:1,target:{revision:1,digest:'a'.repeat(64)},entry:{origin:'https://app.example',path:'/'},canManage:true});},
    resolveSourceCredential:async request=>{assert.equal(request.actor,original);assert.equal(request.rootActor.accountId,a.accountId);return Object.freeze({synthetic:true});}});t.after(()=>port.close());
  const args={appId:'app',localSubject:subject,requestId:'first',title:'Synthetic'};
  assert.equal((await port.execute({op:'provision',actor:original,args})).canDisplay,false);
  for(const actor of [{...original},JSON.parse(JSON.stringify(original)),{accountId:original.accountId,deviceId:original.deviceId},f.retained.get((await f.signed(foreign,'status')).deviceId)])await assert.rejects(port.execute({op:'provision',actor,args}),/connect_authority_actor_invalid|foreign/u);
  const mobile=f.retained.get((await f.signed(phone,'status')).deviceId);await f.signed(owner,'device.revoke',{deviceId:mobile.deviceId});
  await assert.rejects(port.execute({op:'provision',actor:mobile,args}),/device_revoked/u);assert.equal(calls,1);
});
