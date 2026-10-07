import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createServer} from 'node:http';
import {environment} from '../../human-identity/test/support/renewal-fixture.mjs';
import {verifyHumanIdToken} from '../../human-identity/examples/bff.mjs';
import {createManagedReviewsPort} from '../server/managed-port.mjs';
import {contractDigest} from '../../app-contract/json.mjs';
import {readonlyAppHttpFixture} from '../../capabilities/test/support/readonly-app-http.mjs';
// Source is a separately versioned checkout/packet. This fixture must be run
// with its installed tsx loader; it is not part of the ordinary Root test glob.
import {JsonReviewStore} from '../../../output/povedai-managed-source/worktree/src/server/store.ts';
import {createManagedReviews} from '../../../output/povedai-managed-source/worktree/src/server/managed-reviews.ts';
import {createApplication} from '../../../output/povedai-managed-source/worktree/src/server/app.ts';
import {createRuntimeStore} from '../../../output/povedai-managed-source/worktree/src/server/runtime-store.ts';

async function sourceFixture(t, root, {claimsByToken,sourceClientId='app-alpha'}={}){
  const directory=await mkdtemp(join(tmpdir(),'povedai-root-proof-')),file=join(directory,'database.json');
  const store=new JsonReviewStore(file,{allowManagedMigration:true}),runtime=createRuntimeStore(undefined,false);await runtime.connect();
  let handler,block,entered,release;const sourceServer=createServer((req,res)=>handler(req,res));sourceServer.keepAliveTimeout=65000;sourceServer.headersTimeout=70000;
  await new Promise(done=>sourceServer.listen(0,'127.0.0.1',done));const origin='http://127.0.0.1:'+sourceServer.address().port;
  const verifier={async verify(credential,signal){
    const accessToken=typeof credential==='string'?credential:credential?.headers?.authorization?.replace(/^Bearer /u,'');
    const proof=claimsByToken.get(accessToken);assert.ok(proof,'fixture token belongs to actual approved RP');
    const claims=await verifyHumanIdToken({token:proof.idToken,jwks:proof.jwks,issuer:root.issuer,clientId:sourceClientId,nonce:proof.expectedNonce});
    const response=await fetch(root.issuer+'/userinfo',{headers:{authorization:'Bearer '+accessToken},signal});
    const body=await response.json();assert.equal(response.status,200,'current issuer userinfo');assert.equal(body.sub,claims.sub);
    if(block){entered();await new Promise(done=>{release=done;});}
    return{issuer:root.issuer,subject:claims.sub,label:body.name||'Profile',expiresAt:Date.now()+3000};
  }};
  const service=createManagedReviews(store,{enabled:true,verifier});
  const config={nodeEnv:'test',port:0,apiKey:'synthetic-key',publicSiteKey:'main',storage:'json',corsOrigins:[],publicUrl:origin,authDemo:false,trustProxy:false,adminName:'Test',sessionTtlSeconds:300,signupEnabled:false,legalVersion:'test',operator:{name:'Test',inn:'1234567890',registrationId:'1234567890123',email:'test@example.test'},version:'synthetic',ipHashSecret:'synthetic-source-fixture-at-least32-characters',aiProvider:'mock',gonkaBaseUrl:'https://example.invalid',gonkaModel:'mock',gonkaMaxTokens:300,gonkaMaxAttempts:1,gonkaTimeoutMs:1000};
  handler=createApplication({store,runtime,config,managedReviews:service}).app;
  t.after(async()=>{block=false;release?.();sourceServer.closeAllConnections();await new Promise(done=>sourceServer.close(done));service.close();await runtime.close();await rm(directory,{recursive:true,force:true});});
  async function http(operation,input,token){const r=await fetch(origin+'/api/managed/v1/'+operation,{method:'POST',headers:{origin,authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify(input)});return{status:r.status,body:await r.json()};}
  return{origin,store,service,http,hold(){block=true;return new Promise(done=>{entered=done;});},resume(){block=false;release?.();}};
}
async function rootTokens(t){
  const root=await environment(t,{renewal:false});await root.login(root.rps[0]);const first=root.rps[0].verificationFixture();await root.login(root.rps[1]);const second=root.rps[1].verificationFixture();
  return{root,first,second,claimsByToken:new Map([[first.accessToken,first],[second.accessToken,second]])};
}
test('actual signed Root approval+maintained JOSE/fresh userinfo and real Povédai HTTP honor Source grants/client/revoke', {timeout:30000},async t=>{
  const tokens=await rootTokens(t),f=await sourceFixture(t,tokens.root,tokens),scope={registryId:'soty',tenantId:tokens.root.actor.account.accountId,appId:'actual-app',environmentId:'test'},localSubject={kind:'app',id:scope.appId};
  const input={scope,localSubject,requestId:'provision',title:'Private actual Source'};
  const denied=await f.http('provision',input,tokens.first.accessToken);assert.equal(denied.status,403);assert.equal(denied.body.error,'managed_source_grant_required');
  const person=await f.service.withActor(tokens.first.accessToken,actor=>f.service.actorProfile(actor));
  await f.service.operator.grant({id:'native-approved-grant',principalId:person.principalId,scope,rights:['provision','collect','reply','moderate','publish'],subjects:[localSubject],keyId:'actual-source-key',generation:1,active:true,expiresAt:Date.now()+60000});
  const created=await f.http('provision',input,tokens.first.accessToken);assert.equal(created.status,200);assert.equal(created.body.canDisplay,false);
  assert.deepEqual((await f.http('provision',input,tokens.first.accessToken)).body,created.body);
  assert.equal((await f.http('provision',{...input,title:'changed'},tokens.first.accessToken)).status,409);
  assert.equal((await f.http('context',{scope,localSubject},tokens.second.accessToken)).status,401,'same subject at wrong RP client is not Source proof');
  assert.equal((await f.http('provision',{...input,requestId:'foreign',scope:{...scope,tenantId:'other'}},tokens.first.accessToken)).status,403);
  assert.equal((await f.store.managedSnapshot()).adminUsers.length,0);
  const review=await f.http('collect',{scope,localSubject,requestId:'review',body:'Private source history'},tokens.first.accessToken);assert.equal(review.status,200);
  assert.equal((await fetch(f.origin+'/api/public/v1/subjects/'+created.body.subjectId)).status,404);
  const pending=await f.http('list',{scope,localSubject,limit:10},tokens.first.accessToken);assert.equal(pending.status,200);assert.equal(pending.body.items.length,1);assert.doesNotMatch(JSON.stringify(pending.body),/issuer|identityHash|principalId|accessToken|email/u);
  await f.service.operator.revokeKey('actual-source-key');assert.equal((await f.http('context',{scope,localSubject},tokens.first.accessToken)).status,403);
  const backup=await tokens.root.backupDevice();await backup.client.revokeDevice(tokens.root.actor.account.deviceId);
  assert.equal((await f.http('context',{scope,localSubject},tokens.first.accessToken)).status,401,'Root device revoke reaches actual issuer userinfo');
});
test('actual signed Root Apps fence+native Povédai commit; revoke during Source await denies result and makes no automatic retry', {timeout:30000},async t=>{
  const apps=await readonlyAppHttpFixture(t),tokens=await rootTokens(t),f=await sourceFixture(t,tokens.root,tokens);
  const caps=apps.app.locals.capabilitiesService,capActor=caps.authenticateCredential({token:apps.httpToken,audience:apps.origin});
  const actor=caps.withOwnerAuthority({actor:capActor},value=>({...value}));
  const scope={registryId:'soty',tenantId:actor.accountId,appId:apps.appId,environmentId:'test'},localSubject={kind:'app',id:apps.appId};
  const person=await f.service.withActor(tokens.first.accessToken,actor=>f.service.actorProfile(actor));
  await f.service.operator.grant({id:'explicit-approved-cross-app-binding',principalId:person.principalId,scope,rights:['provision','collect','reply','moderate','publish'],subjects:[localSubject],keyId:'source-key',generation:1,active:true,expiresAt:Date.now()+60000});
  // An explicit synthetic native Source grant binds this human profile to this
  // Apps owner tuple. There is no inferred email/name/account merge.
  const port=createManagedReviewsPort({registryId:'soty',environmentId:'test',providerRef:{id:'povedai:actual',version:1,digest:contractDigest({protocol:'povedai.managed-reviews.v1'})},source:f.service,
    actorActive:who=>{try{return caps.withOwnerAuthority({actor:capActor},current=>current.accountId===who.accountId&&current.deviceId===who.deviceId);}catch{return false;}},
    withAppAuthority:apps.app.locals.appsService.withAppAuthority,resolveSourceCredential:async()=>tokens.first.accessToken});t.after(()=>port.close());
  const args={appId:apps.appId,localSubject,requestId:'root-source',title:'Actual fenced private Source'};
  const value=await port.execute({op:'provision',actor,args});assert.equal(value.canDisplay,false);assert.match(value.subjectRef.digest,/^[a-f0-9]{64}$/u);
  const pause=f.hold(),query=port.execute({op:'context',actor,args:{appId:apps.appId,localSubject}});await pause;
  await apps.revokeApp();f.resume();await assert.rejects(query,/app_unavailable|managed_reviews_source_unavailable|managed_reviews_authentication_required/u);
  assert.equal((await f.store.managedSnapshot()).managedReviews.subjectBindings.length,1);assert.equal((await f.store.managedSnapshot()).managedReviews.operationReceipts.length,1);
});
