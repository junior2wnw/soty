import test from 'node:test';import assert from 'node:assert/strict';
import{createScopedGateway}from'../scoped-embed/gateway.mjs';import{approvedEmbedProfile}from'../scoped-embed/profile-dispatch.mjs';
import{HIVE_SELECTED_SOURCE}from'../scoped-embed/resource-route-adapters.mjs';
function fixture(v2=true){let time=Date.now(),allowed=true;const actor=Object.freeze({accountId:'root-owner',deviceId:'root-device'}),appId='app-'+'a'.repeat(32);
  const raw={schema:v2?'soty.selected-human-embed.v2':'soty.selected-human-embed.v1',appId,
    connector:{linkId:'link',hostDeviceId:'host',connectorId:'connector'},target:{revision:1,digest:'a'.repeat(64)},
    sourceProfile:v2?HIVE_SELECTED_SOURCE:{id:'planner.selected-workspace',version:1,digest:'b'.repeat(64)},
    resource:{registryId:'soty',tenantId:actor.accountId,appId,environmentId:'production',resourceId:'source:resource',
      ...(v2?{selection:{kind:'hive.project.v1',nativeId:' Project / Exact ',incarnationId:'source-incarnation'}}:{workspaceId:'native-workspace'})},
    issuer:'https://root.test/human-identity',clientId:'native.rp',embedOrigin:'https://app.root.test',nativeOrigin:'https://native.test',parentOrigin:'https://root.test'};
  const profile=approvedEmbedProfile(raw),gateway=createScopedGateway({clock:()=>time,admissions:{profiles:()=>[raw],require:()=>profile},
    withAppAuthority(request,callback){assert.equal(request.actor,actor);if(!allowed)throw Error('current_revoked');return callback({appId,ownerId:actor.accountId,accountId:actor.accountId,
      policyEpoch:1,target:profile.target,entry:{origin:profile.embedOrigin,domainId:'domain'},appRevision:1});},
    withHumanSubjectAuthority(request,callback){assert.equal(request.actor,actor);if(!allowed)throw Error('current_revoked');return callback({issuer:profile.issuer,subject:'exact-sub',
      clientId:profile.clientId,clientGeneration:1,clientProfileDigest:'c'.repeat(64)});}});
  return{actor,appId,gateway,open:()=>gateway.open({actor,appId,domainId:'domain',target:profile.target}),advance(ms){time+=ms;},revoke(){allowed=false;}};
}
test('expiredv2 reference remains closed; only original actor owns an attempt witness, with no cookie/session/Source readiness',()=>{
  const f=fixture(),first=f.open();f.advance(300001);assert.throws(()=>f.gateway.context(first.record));
  assert.throws(()=>f.gateway.read(first.record.context.reference));assert.throws(()=>f.gateway.ownedContext(f.actor,f.appId,first.closeHandle));
  const attempt=f.gateway.ownedAttempt(f.actor,f.appId,first.closeHandle);assert.equal(attempt.record,null);assert.equal(attempt.sourceOnly,true);
  assert.deepEqual(Object.keys(attempt).sort(),['context','record','sourceOnly']);assert.equal(Object.hasOwn(attempt.context,'sourceSession'),false);
  assert.throws(()=>f.gateway.ownedAttempt({...f.actor,deviceId:'other-device'},f.appId,first.closeHandle));
  assert.throws(()=>f.gateway.ownedAttempt({...f.actor,accountId:'other-account'},f.appId,first.closeHandle));
  assert.throws(()=>f.gateway.ownedAttempt(f.actor,'app-'+'b'.repeat(32),first.closeHandle));
  f.revoke();assert.throws(()=>f.open(),'fresh current authority still mandatory');f.gateway.close();
});
test('explicit close, app retirement, expiry bound and dispose retire attempts; v1 requires its old live context',()=>{
  const f=fixture(),a=f.open();f.gateway.abandon(f.actor,f.appId,a.closeHandle);assert.throws(()=>f.gateway.ownedAttempt(f.actor,f.appId,a.closeHandle));
  const b=f.open();f.advance(300001);f.gateway.invalidateApp(f.appId);assert.throws(()=>f.gateway.ownedAttempt(f.actor,f.appId,b.closeHandle));
  const c=f.open();f.advance(86700001);assert.throws(()=>f.gateway.ownedAttempt(f.actor,f.appId,c.closeHandle));f.gateway.close();
  const old=fixture(false),legacy=old.open();old.advance(300001);assert.throws(()=>old.gateway.ownedAttempt(old.actor,old.appId,legacy.closeHandle));old.gateway.close();
});
test('attempt history is bounded without eviction or privilege; explicit disposal releases capacity',()=>{
  const f=fixture(),handles=[];for(let i=0;i<256;i++)handles.push(f.open().closeHandle);f.advance(300001);
  assert.throws(()=>f.open(),{code:'app_scoped_attempt_busy'});assert.equal(f.gateway.ownedAttempt(f.actor,f.appId,handles[0]).sourceOnly,true);
  f.gateway.abandon(f.actor,f.appId,handles[0]);assert.ok(f.open().closeHandle);f.gateway.close();
});
test('explicit invalidation retires an already expired attempt and cannot leave a renewal witness behind',()=>{
  const f=fixture(),opened=f.open();f.advance(300001);
  assert.equal(f.gateway.ownedAttempt(f.actor,f.appId,opened.closeHandle).sourceOnly,true);
  f.gateway.invalidate(opened.record);
  assert.throws(()=>f.gateway.ownedAttempt(f.actor,f.appId,opened.closeHandle),{code:'app_scoped_context_closed'});
  f.gateway.invalidate(opened.record);f.gateway.close();
});
