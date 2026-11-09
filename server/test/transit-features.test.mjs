import test from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, createHash, generateKeyPairSync, randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { createTransitAccess } from '../transit-access.js';
import { profileFeaturesResponse } from './support/transit-features-fixture.mjs';
const keys=generateKeyPairSync('ed25519'),deliveryKey=Buffer.alloc(32,7),actor={accountId:'account-A',deviceId:'device-A'};
const configuration={endpoint:'http://fixture.invalid/v1/profile',issuer:'https://soty.example',key:keys.privateKey,deliveryKey,appId:'app-'+'a'.repeat(32)};
const status=service=>service.executeAsync({op:'transit.access.status',actor,args:{expectedAccountId:actor.accountId}});
const encrypted=(profile,body)=>{
 const nonce=randomBytes(12),c=createCipheriv('aes-256-gcm',deliveryKey,nonce);c.setAAD(Buffer.concat([Buffer.from('TRANSIT/SUBSCRIPTION-PROFILE-DELIVERY/V1\0'),Buffer.from(body)]));
 const value={schema:'transit.subscription-profile.v1',coreVersion:'1.14.4',profile,sha256:createHash('sha256').update(JSON.stringify(profile)).digest('hex')};
 const bytes=Buffer.concat([c.update(JSON.stringify(value)),c.final()]);return Response.json({schema:'transit.encrypted-subscription-profile.v1',nonce:nonce.toString('base64'),tag:c.getAuthTag().toString('base64'),ciphertext:bytes.toString('base64')});
};
function fixture(t,{flags={},configuration:changes={},hold,old=false,invalid=false}={}){
 let time=100_000,calls=0,active=true,legacy=old;const nonces=[];
 const service=createTransitAccess({configuration:{...configuration,...changes},now:()=>time,
 getConnect:()=>({withActorAuthorityFence(value,fn){assert.equal(value,actor);if(!active)throw Error('revoked');return fn()}}),
 async fetchImpl(url,request){
  assert.equal(new URL(url).pathname,'/v1/features');calls++;nonces.push(JSON.parse(request.body).nonce);if(hold)await hold;
  if(legacy)return Response.json({error:'not_found'},{status:404});
  const response=profileFeaturesResponse(url,request,keys.publicKey,deliveryKey,flags);
  if(invalid){const value=await response.json();value.tag=Buffer.alloc(16).toString('base64');return Response.json(value)}
  return response;
 }});t.after(()=>service.close());return{service,nonces,calls:()=>calls,advance(ms){time+=ms},upgrade(){legacy=false},revoke(){active=false}};
}
test('a shared bounded feature promise negotiates old six-field config without claims or duplicate requests',async t=>{
 let release;const hold=new Promise(resolve=>{release=resolve});const f=fixture(t,{hold,flags:{usage:true}});
 const a=status(f.service),b=status(f.service);await new Promise(resolve=>setImmediate(resolve));assert.equal(f.calls(),1);release();
 for(const result of await Promise.all([a,b]))assert.deepEqual(result,{enabled:true,appId:configuration.appId,usageEnabled:true,subscriptionEnabled:true,minimumCoreVersion:'1.14.4'});
 f.advance(29_999);await status(f.service);assert.equal(f.calls(),1);f.advance(2);await status(f.service);assert.equal(f.calls(),2);assert.notEqual(f.nonces[0],f.nonces[1]);
 const disabled=fixture(t,{flags:{usage:true},configuration:{usageEnabled:false}});assert.equal((await status(disabled.service)).usageEnabled,false);
});
test('an old or unauthenticated issuer disables new features and is rechecked after a short bounded failure cache',async t=>{
 const f=fixture(t,{old:true});const result=await status(f.service);assert.equal(result.enabled,true);assert.equal(result.subscriptionEnabled,false);assert.equal(result.usageEnabled,false);
 f.upgrade();await status(f.service);assert.equal(f.calls(),1);f.advance(2001);assert.equal((await status(f.service)).subscriptionEnabled,true);assert.equal(f.calls(),2);
 const tampered=fixture(t,{invalid:true,flags:{usage:true}});assert.equal((await status(tampered.service)).usageEnabled,false);
 for(const flags of [{usage:'yes'},{extra:true},{schema:'foreign'},{portableCoreVersion:'1.13.15'}]){const wrong=fixture(t,{flags});assert.equal((await status(wrong.service)).subscriptionEnabled,false)}
});
test('owner revocation during feature negotiation suppresses the original actor result',async t=>{
 let release;const hold=new Promise(resolve=>{release=resolve});const f=fixture(t,{hold});const pending=status(f.service);
 await new Promise(resolve=>setImmediate(resolve));f.revoke();release();await assert.rejects(pending,/revoked/);
});
test('a Remote GET lazily negotiates after every Root restart without a UI status call or new DATA fields',async t=>{
 const profile={outbounds:[{type:'vless',tag:'entry'}],route:{final:'entry'}},token=randomBytes(32).toString('base64url');let negotiations=0,gets=0;
 const start=async()=>{
  const service=createTransitAccess({configuration,getConnect:()=>{throw Error('public GET must not need actor')},
  async fetchImpl(url,request){
   if(new URL(url).pathname==='/v1/features'){negotiations++;return profileFeaturesResponse(url,request,keys.publicKey,deliveryKey)}
   assert.equal(new URL(url).pathname,'/v1/subscription/profile');assert.equal(JSON.parse(request.body).tokenHash,createHash('sha256').update(token).digest('hex'));gets++;
   return encrypted(profile,request.body);
  }});
  const server=createServer((req,res)=>void service.serveSubscription(req,res));server.listen(0,'127.0.0.1');await once(server,'listening');
  const stop=async()=>{service.close();await new Promise(resolve=>server.close(resolve))};return{server,stop,url:`http://127.0.0.1:${server.address().port}/transit/subscriptions/${token}/profile.json`};
 };
 for(let index=0;index<2;index++){const f=await start();try{const response=await fetch(f.url);assert.equal(response.status,200);assert.deepEqual(await response.json(),profile)}finally{await f.stop()}}
 assert.equal(negotiations,2);assert.equal(gets,2);
});

test('older issuers retain the signed local-file path while unsupported Remote capabilities stay disabled',async t=>{
 const profile={outbounds:[{type:'vless',tag:'legacy'}],route:{final:'legacy'}},seen=[];
 const service=createTransitAccess({configuration,getConnect:()=>({withActorAuthorityFence(_actor,fn){return fn()}}),
 async fetchImpl(url,request){
  const path=new URL(url).pathname;seen.push(path);if(path==='/v1/features')return Response.json({error:'not_found'},{status:404});
  assert.equal(path,'/v1/profile');const nonce=randomBytes(12),c=createCipheriv('aes-256-gcm',deliveryKey,nonce);
  c.setAAD(Buffer.concat([Buffer.from('TRANSIT/PROFILE-DELIVERY/V1\0'),Buffer.from(request.body)]));
  const value={schema:'transit.personal-profile.v1',accountId:actor.accountId,coreVersion:'1.13.15',mode:'free-best-effort',profile,
   sha256:createHash('sha256').update(JSON.stringify(profile)).digest('hex')};
  const bytes=Buffer.concat([c.update(JSON.stringify(value)),c.final()]);return Response.json({schema:'transit.encrypted-profile.v1',nonce:nonce.toString('base64'),tag:c.getAuthTag().toString('base64'),ciphertext:bytes.toString('base64')});
 }});t.after(()=>service.close());
 assert.equal((await status(service)).subscriptionEnabled,false);
 const result=await service.executeAsync({op:'transit.access.profile',actor,args:{expectedAccountId:actor.accountId}});
 assert.deepEqual(result.profile,profile);assert.deepEqual(seen,['/v1/features','/v1/profile']);
});
