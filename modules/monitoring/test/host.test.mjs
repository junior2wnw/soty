import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync,sign} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createConnectService,digestArgs} from '../../connect/server/index.mjs';
import {createAppsService} from '../../apps/server/index.mjs';
import {connectorKey} from '../../apps/server/protocol.mjs';
import {createCapabilitiesService} from '../../capabilities/server/index.mjs';
import {createWorldService} from '../../world/server/index.mjs';
import {createUniversalApps} from '../../../server/universal-apps.js';
const {createMonitoringModule}=await import(process.env.SOTY_MONITORING_HOST_SOURCE??new URL('../../../server/monitoring-module.js',import.meta.url));
import {monitoringResource} from '../capabilities.mjs';
const origin='https://monitoring-module.test';
function identity(){const signing=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),encryption=generateKeyPairSync('ec',{namedCurve:'prime256v1'});return{privateKey:signing.privateKey,publicJwk:signing.publicKey.export({format:'jwk'}),encryptionPublicJwk:encryption.publicKey.export({format:'jwk'})};}
async function fixture(t,{monitoringOperator='a',beforeCollectionChange}={}){
 const root=await mkdtemp(join(tmpdir(),'soty-monitoring-host-')),accountFile=join(root,'accounts.sqlite'),appsFile=join(root,'apps.sqlite'),a=identity(),b=identity();let core,apps,caps,module;
 const options={databasePath:accountFile,projectId:'soty',allowedOrigins:[origin],platformAdminAccountIds:[]};
 core=createConnectService(options);
 async function call(keys,op,args={},good=true){const challenge=await core.handle({origin,op:'challenge',args:{operation:op,digest:digestArgs(args)}});assert.equal(challenge.ok,true,challenge.error?.code);const reply=await core.handle({origin,op,args,proof:{challengeId:challenge.challengeId,publicJwk:keys.publicJwk,signature:sign('sha256',Buffer.from(challenge.message),{key:keys.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}});if(good)assert.equal(reply.ok,true,reply.error?.code);return reply;}
 const own=await call(a,'bootstrap',{label:'Synthetic monitoring owner',encryptionPublicJwk:a.encryptionPublicJwk}),other=await call(b,'bootstrap',{label:'Synthetic monitoring member',encryptionPublicJwk:b.encryptionPublicJwk});core.close();
 const world=createWorldService({databasePath:join(root,'world.sqlite'),projectId:'soty',withCurrentContacts:(actor,callback)=>core.withCurrentContacts(actor,callback)});
 apps=createAppsService({databasePath:appsFile,appOriginTemplate:'https://{appId}.legacy.test',namedAppZone:'https://apps.test',shellOrigins:[origin],actorActive:actor=>core?.isActorActive(actor)===true,withCurrentContacts:(actor,callback)=>core.withCurrentContacts(actor,callback),withAuthorityFence:callback=>world.withCommunityAuthorityFence(callback)});
 const universal=createUniversalApps({dataDir:root,apps,actorActive:actor=>core?.isActorActive(actor)===true});
 const operator=monitoringOperator==='a'?own.accountId:other.accountId;
 module=createMonitoringModule({databasePath:join(root,'monitoring.sqlite'),storageMode:'writer',connect:()=>core,apps:()=>apps,capabilities:()=>caps,publication:request=>universal.appsExtension.execute(request),platformPolicy:{readAccountIds:[operator],configureAccountIds:[operator],adminAccountIds:[operator]},...(beforeCollectionChange?{beforeCollectionChange}:{})});
 caps=createCapabilitiesService({databasePath:join(root,'caps.sqlite'),projectId:'soty',actorActive:actor=>core?.isActorActive(actor)===true,catalog:module.capabilities.catalog,readonlyQueries:module.capabilities.queries,externalAdapters:module.capabilities.effects});
 core=createConnectService({...options,platformAdminAccountIds:[own.accountId],extensions:[universal.appsExtension,universal.universalExtension,universal.feedback,universal.reviews,module.composeExtension(caps)]});
 const sql=new DatabaseSync(appsFile),dev={linkId:'monitor-link',hostDeviceId:'monitor-device',connectorId:'monitor-connector'};sql.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(dev),own.accountId,JSON.stringify(dev),'Synthetic connector',1);sql.close();
 const app=(await call(a,'apps.register',{hostDeviceId:dev.hostDeviceId,connectorId:dev.connectorId,name:'Independent Source',port:30589})).app;
 t.after(async()=>{core.close();caps.close();module.close();universal.close();apps.close();world.close();await rm(root,{recursive:true,force:true});});
 const scoped=(args={})=>({expectedAccountId:own.accountId,scope:{kind:'app',appId:app.id},...args});
 return{root,core,caps,module,apps,a,b,own,other,app,call,scoped};
}
test('actual signed Connect human transport uses existing extension and Apps ownership; config survives reopen',async t=>{
 const f=await fixture(t),description=await f.call(f.a,'monitoring.describe',f.scoped());assert.equal(description.config.enabled,true);assert.equal(description.permissions.configure,true);assert.equal(description.actions.find(action=>action.id==='app.listing').enabled,false,'private app cannot be auto-exposed by monitoring');
 const denied=await f.call(f.b,'monitoring.describe',{...f.scoped(),expectedAccountId:f.other.accountId},false);assert.equal(denied.ok,false);assert.equal(denied.error.code,'monitoring_forbidden');
 const command=f.scoped({requestId:'signed-toggle',expectedRevision:0,enabled:false});assert.equal((await f.call(f.a,'monitoring.config.put',command)).config.revision,1);assert.equal((await f.call(f.a,'monitoring.config.put',command)).replayed,true);
 assert.equal(f.module.observe({scope:{kind:'app',appId:f.app.id},metricId:'http.requests',value:1}).recorded,false);
 assert.equal((await f.core.handle({origin,op:'challenge',args:{operation:'monitoring.observeBatch',digest:digestArgs({})}})).ok,false,'trusted host observations are not an additional signed RPC');
 const readOnly=createMonitoringModule({databasePath:join(f.root,'monitoring.sqlite'),connect:()=>f.core,apps:()=>f.apps});try{assert.equal(readOnly.storageIdentity().schemaVersion,1);assert.equal(readOnly.storageIdentity().mode,'reader');}finally{readOnly.close();}
});
test('real platform admission action uses canonical CAS/receipt and monitoring policy never borrows invitation-admin',async t=>{
 const f=await fixture(t),scope={kind:'platform'},base={expectedAccountId:f.own.accountId,scope};
 const described=await f.call(f.a,'monitoring.describe',base),action=described.actions.find(value=>value.id==='platform.admission');assert.equal(action.enabled,true);
 const request={...base,actionId:action.id,requestId:'signed-platform-policy',expectedRevision:action.revision,input:{inviteOnly:false,defaultQuota:7}};
 const result=await f.call(f.a,'monitoring.admin.invoke',request);assert.equal(result.state,'committed');assert.equal(result.revision,action.revision+1);
 assert.equal((await f.call(f.a,'platform.admission.get')).defaultQuota,7);assert.equal((await f.call(f.a,'monitoring.admin.invoke',request)).replayed,true);assert.equal((await f.call(f.a,'monitoring.audit.list',base)).entries.filter(entry=>entry.operation==='platform.admission').length,1);
 const wrong=await f.call(f.b,'monitoring.describe',{expectedAccountId:f.other.accountId,scope},false);assert.equal(wrong.ok,false);
});
test('invitation administrator alone is not monitoring operator; monitoring operator alone cannot mutate invitation policy',async t=>{
 const f=await fixture(t,{monitoringOperator:'b'}),scope={kind:'platform'};
 assert.equal((await f.call(f.a,'platform.admission.get')).manageAllowed,true);
 assert.equal((await f.call(f.a,'monitoring.describe',{expectedAccountId:f.own.accountId,scope},false)).error.code,'monitoring_forbidden');
 const description=await f.call(f.b,'monitoring.describe',{expectedAccountId:f.other.accountId,scope});assert.equal(description.permissions.admin,true);assert.equal(description.actions.find(action=>action.id==='platform.admission').enabled,false);
 const rejected=await f.call(f.b,'monitoring.admin.invoke',{expectedAccountId:f.other.accountId,scope,requestId:'no-canonical-rights',actionId:'platform.admission',expectedRevision:0,input:{inviteOnly:false,defaultQuota:99}},false);assert.equal(rejected.ok,false);assert.equal((await f.call(f.a,'platform.admission.get')).defaultQuota,3);
});
test('real app listing administration preserves current target and exact canonical receipt',async t=>{
 const f=await fixture(t),current=await f.call(f.a,'apps.publication.get',{appId:f.app.id}),domains=await f.call(f.a,'apps.domains.get',{appId:f.app.id});
 const alias=(await f.call(f.a,'apps.domains.claim',{appId:f.app.id,slug:'monitoring-fixture',requestId:'named-alias',expectedDomainsRevision:domains.revision})).receipt;
 await f.call(f.a,'apps.publication.update',{appId:f.app.id,requestId:'publish-direct',expectedPolicyEpoch:current.policyEpoch,expectedTargetRevision:current.activeTargetRevision,launchPolicy:'anyone',listed:false,activeDomainIds:[alias.domainId],exposureAck:{scope:'whole-port',targetRevision:current.target.revision,targetDigest:current.target.digest,profile:current.target.profile}});
 const admitted=(await f.call(f.a,'apps.universal.get',{appId:f.app.id,expectedAccountId:f.own.accountId})).registration;assert.equal(admitted.state,'ready');assert.ok(admitted.feedback.installationId);
 const described=await f.call(f.a,'monitoring.describe',f.scoped()),action=described.actions.find(value=>value.id==='app.listing');assert.equal(action.enabled,true);
 const request=f.scoped({actionId:action.id,requestId:'list-canonical',expectedRevision:action.revision,input:{listed:true}}),result=await f.call(f.a,'monitoring.admin.invoke',request);assert.equal(result.state,'committed');
 const after=await f.call(f.a,'apps.publication.get',{appId:f.app.id});assert.equal(after.listed,true);assert.deepEqual(after.target,current.target);assert.equal((await f.call(f.a,'monitoring.admin.invoke',request)).replayed,true);assert.equal((await f.call(f.a,'apps.publication.get',{appId:f.app.id})).policyEpoch,after.policyEpoch);
 const reconciled=(await f.call(f.a,'apps.universal.get',{appId:f.app.id,expectedAccountId:f.own.accountId})).registration;assert.equal(reconciled.state,'ready');assert.equal(reconciled.feedback.installationId,admitted.feedback.installationId);
});
test('genuine service grant reuses Capabilities query and refuses wrong resources/revoked grant',async t=>{
 const f=await fixture(t),scope={kind:'app',appId:f.app.id};f.module.observe({scope,metricId:'http.requests',value:6});
 const principal=(await f.call(f.a,'access.principals.create',{expectedAccountId:f.own.accountId,label:'Synthetic monitoring agent',clientLabel:'Module fixture'})).principal;
 // Catalog is obtained from the installed host, not inferred from arbitrary app JSON.
 const entry=f.module.capabilities.catalog.find(value=>value.capabilityId==='monitoring.describe');
 const grant=(await f.call(f.a,'access.grants.issue',{expectedAccountId:f.own.accountId,principalId:principal.id,capabilities:[{capabilityId:entry.capabilityId,version:1}],resources:['monitoring',monitoringResource(scope)],effects:[],recipients:['soty:monitoring'],expiresAt:Date.now()+600000,budget:{unit:'invocations',limit:10}})).grant;
 const credential=await f.call(f.a,'access.credentials.issue',{expectedAccountId:f.own.accountId,grantId:grant.id,audience:origin,expiresAt:Date.now()+300000});
 const actor=f.caps.authenticateCredential({token:credential.token,audience:origin});
 const listed=f.caps.external.search({actor,query:'monitoring',limit:20}),ref=listed.items.find(value=>value.reference.capabilityId===entry.capabilityId).reference;
 const query=await f.caps.external.query({actor,reference:ref,idempotencyKey:'real-monitoring-read',input:{scope,paramsJson:'{}'}});const data=JSON.parse(query.result.dataJson);assert.equal(data.metrics.find(metric=>metric.id==='http.requests').current,6);
 await assert.rejects(f.caps.external.query({actor,reference:ref,idempotencyKey:'wrong-app',input:{scope:{kind:'platform'},paramsJson:'{}'}}));
 await f.call(f.a,'apps.revoke',{appId:f.app.id});assert.throws(()=>f.module.capabilities.recheck({actor,name:'apps_query',args:{reference:ref,input:{scope,paramsJson:'{}'}},value:query}),{code:'access_denied'},'late revoked app projection must not be exported');
 await f.call(f.a,'access.grants.revoke',{expectedAccountId:f.own.accountId,grantId:grant.id});await assert.rejects(f.caps.external.query({actor,reference:ref,idempotencyKey:'revoked',input:{scope,paramsJson:'{}'}}));
});
test('genuine agent configure invocation uses domain receipt/idempotency, not a JSON execution grant',async t=>{
 const f=await fixture(t),scope={kind:'app',appId:f.app.id},capabilityId='monitoring.config.put';
 const principal=(await f.call(f.a,'access.principals.create',{expectedAccountId:f.own.accountId,label:'Synthetic configuration agent'})).principal;
 const grant=(await f.call(f.a,'access.grants.issue',{expectedAccountId:f.own.accountId,principalId:principal.id,capabilities:[{capabilityId,version:1}],resources:['monitoring',monitoringResource(scope)],effects:['update'],recipients:['soty:monitoring'],expiresAt:Date.now()+600000,budget:{unit:'invocations',limit:10}})).grant;
 const credential=await f.call(f.a,'access.credentials.issue',{expectedAccountId:f.own.accountId,grantId:grant.id,audience:origin,expiresAt:Date.now()+300000});
 const actor=f.caps.authenticateCredential({token:credential.token,audience:origin}),reference=f.caps.external.contracts.find(value=>value.capabilityId===capabilityId);
 const args={actor,reference,idempotencyKey:'one-real-agent-config',input:{scope,paramsJson:'{"enabled":false,"expectedRevision":0}'}};
 const result=await f.caps.external.invoke(args);assert.equal(result.invocation.status,'succeeded');assert.equal(result.invocation.effectState,'committed');assert.equal((await f.call(f.a,'monitoring.describe',f.scoped())).config.enabled,false);
 const again=await f.caps.external.invoke(args);assert.equal(again.reused,true);assert.equal((await f.call(f.a,'monitoring.describe',f.scoped())).config.revision,1);
 const audit=await f.call(f.a,'monitoring.audit.list',f.scoped());assert.equal(audit.entries.filter(entry=>entry.operation==='collection.setEnabled').length,1);
});
test('signed OFF/ON flush boundary retains pre-OFF data and never backfills observations captured while OFF',async t=>{
 let f,fail=false,hooks=0;const buffer=[];
 function flush(scope){hooks++;if(fail)throw Object.assign(new Error('synthetic held buffer failure'),{code:'monitoring_flush_failed'});const selected=buffer.filter(entry=>entry.scope.appId===scope.appId);if(selected.length)f.module.observeBatch({observations:selected});for(const entry of selected)buffer.splice(buffer.indexOf(entry),1);}
 f=await fixture(t,{beforeCollectionChange:flush});const scope={kind:'app',appId:f.app.id};
 function capture(n){if(!f.module.collectionEnabled(scope))return false;buffer.push({scope,metricId:'http.requests',samples:n,sum:n,min:1,max:1,last:1,observedAt:Date.now()});return true;}
 assert.equal(capture(3),true);await f.call(f.a,'monitoring.config.put',f.scoped({requestId:'before-off',expectedRevision:0,enabled:false}));assert.equal(buffer.length,0);assert.equal((await f.call(f.a,'monitoring.describe',f.scoped())).metrics.find(value=>value.id==='http.requests').current,3,'preOFF data is durable before OFF ACK');assert.equal(capture(7),false);
 await f.call(f.a,'monitoring.config.put',f.scoped({requestId:'back-on',expectedRevision:1,enabled:true}));assert.equal(capture(2),true);flush(scope);
 const now=Date.now(),query=await f.call(f.a,'monitoring.query',f.scoped({metricIds:['http.requests'],from:now-60000,to:now+1000,maxPoints:1}));assert.equal(query.series[0].points[0].value,5,'preOFF3 + postON2; OFF7 must not become a delayed sample');
 fail=true;const rejected=await f.call(f.a,'monitoring.config.put',f.scoped({requestId:'flush-fails',expectedRevision:2,enabled:false}),false);assert.equal(rejected.ok,false);assert.equal(rejected.error.code,'monitoring_flush_failed');assert.equal((await f.call(f.a,'monitoring.describe',f.scoped())).config.enabled,true);
 const previous=hooks;const denied=await f.call(f.b,'monitoring.config.put',{...f.scoped({requestId:'foreign-off',expectedRevision:2,enabled:false}),expectedAccountId:f.other.accountId},false);assert.equal(denied.ok,false);assert.equal(hooks,previous,'foreign rights fail before flushing any buffer');
});
