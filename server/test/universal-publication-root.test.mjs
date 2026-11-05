import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {join} from 'node:path';
import {rootSelfIpFixture} from './support/self-ip-root-fixture.mjs';
import {good} from './support/native-capability-http.mjs';

async function setup(t){
 const f=await rootSelfIpFixture(t),appId=good(await f.ownerCall('apps.registerEndpoint',await f.args())).app.id;
 const get=async()=>good(await f.ownerCall('apps.universal.get',{appId})).registration;
 const initial=await get(),descriptor=structuredClone(initial.descriptor);descriptor.app.title='Accepted human settings, preserve modules';
 good(await f.ownerCall('apps.universal.admit',{appId,expectedRevision:initial.revision,requestId:'publication-custom-descriptor-001',proposal:{kind:'descriptor-json',json:JSON.stringify(descriptor)}}));
 const prior=await get(),domains=good(await f.ownerCall('apps.domains.get',{appId})),alias=good(await f.ownerCall('apps.domains.claim',{appId,requestId:'publication-alias-001',slug:'publication-real-app',expectedDomainsRevision:domains.revision})).receipt;
 async function publication(requestId){const policy=good(await f.ownerCall('apps.publication.get',{appId}));return{appId,requestId,expectedPolicyEpoch:policy.policyEpoch,expectedTargetRevision:policy.activeTargetRevision,
  launchPolicy:'anyone',listed:true,audience:'public',activeDomainIds:[alias.domainId],exposureAck:{scope:'whole-port',targetRevision:policy.target.revision,targetDigest:policy.target.digest,profile:policy.target.profile}};}
 function journal(callback){const db=new DatabaseSync(join(f.directory,'app-registration','publication-journal.sqlite'));try{return callback(db);}finally{db.close();}}
 return{...f,get app(){return f.app;},appId,get,prior,publication,journal};
}
function preserve(before,after){assert.equal(after.state,'ready');assert.equal(after.feedback.installationId,before.feedback.installationId);
 assert.deepEqual({...after.descriptor,app:{...after.descriptor.app,visibility:before.descriptor.app.visibility}},before.descriptor,'accepted title and every module/contract field remain intact');}
function interruptAfterPublication(f){
 const apps=f.app.locals.appsService,original=apps.currentPublicationReconciliationAuthority;let reads=0;
 apps.currentPublicationReconciliationAuthority=()=>{if(++reads===2)throw Object.assign(Error('isolated interruption'),{code:'registration_publication_test_interrupted'});return original();};
 return()=>{apps.currentPublicationReconciliationAuthority=original;};
}

test('real signed audience commit + journal ACK fault → restart + exact receipt restores ready feedback without changing descriptor/modules/installation',{timeout:25000},async t=>{
 const f=await setup(t),args=await f.publication('publication-restart-receipt-001');
 f.journal(db=>db.exec("CREATE TRIGGER test_journal_ack_fault BEFORE UPDATE OF state ON publication_commands WHEN NEW.state='ready' BEGIN SELECT RAISE(ABORT,'test_journal_ack_fault'); END"));
 const lost=await f.ownerCall('apps.publication.update',args);assert.equal(lost.ok,false,'the client does not receive success after failed journal ACK');
 assert.equal(good(await f.ownerCall('apps.inspect',{appId:f.appId})).publication.audience,'public');const admitted=await f.get();preserve(f.prior,admitted);
 f.journal(db=>db.exec('DROP TRIGGER test_journal_ack_fault'));await f.restart({enabled:true});await f.inventory();
 const replay=good(await f.ownerCall('apps.publication.update',args));assert.equal(replay.replayed,true);assert.equal(replay.universalReconciliation.state,'ready');
 const current=await f.get();preserve(f.prior,current);assert.equal(current.generation,admitted.generation,'exact publication retry creates no second admission');
 const sent=good(await f.call(f.owner,'apps.feedback.submit',{appId:f.appId,installationId:current.feedback.installationId,requestId:'publication-restart-feedback-001',body:'Feedback works after publication receipt replay and restart',attachments:[]}));
 assert.ok(good(await f.call(f.owner,'apps.feedback.get',{appId:f.appId,installationId:current.feedback.installationId,ticketId:sent.receipt.ticketId})).ticket);
});

test('real Source A→B→retained A between publication commit and reconciliation remains held across same/new audience receipts',{timeout:25000},async t=>{
 const f=await setup(t),args=await f.publication('publication-source-race-001'),restore=interruptAfterPublication(f);
 const interrupted=await f.ownerCall('apps.publication.update',args);restore();assert.equal(interrupted.ok,false);assert.equal(interrupted.error.code,'registration_publication_test_interrupted');
 assert.equal(good(await f.ownerCall('apps.inspect',{appId:f.appId})).publication.audience,'public');const original=good(await f.ownerCall('apps.inspect',{appId:f.appId}));
 let policy=good(await f.ownerCall('apps.publication.get',{appId:f.appId}));
 let prepared=good(await f.ownerCall('apps.source.prepare',{appId:f.appId,expectedPolicyEpoch:policy.policyEpoch,expectedTargetRevision:policy.activeTargetRevision,
  source:{hostDeviceId:f.identity.hostDeviceId,connectorId:f.identity.connectorId,port:f.source.port,entryPath:'/identity?source=B',profile:'soty.relay-self-ip.v1',endpoint:original.source.endpoint}}));
 good(await f.ownerCall('apps.source.promote',{appId:f.appId,requestId:'publication-race-promote-B',preparationId:prepared.preparationId,expectedPolicyEpoch:prepared.expectedPolicyEpoch,expectedTargetRevision:prepared.expectedTargetRevision,launchPolicy:'anyone',listed:true,exposureAck:{scope:'whole-port',targetRevision:prepared.target.revision,targetDigest:prepared.target.digest,profile:prepared.target.profile}}));
 policy=good(await f.ownerCall('apps.publication.get',{appId:f.appId}));prepared=good(await f.ownerCall('apps.source.prepare',{appId:f.appId,expectedPolicyEpoch:policy.policyEpoch,expectedTargetRevision:policy.activeTargetRevision,targetRevision:original.source.revision}));
 good(await f.ownerCall('apps.source.promote',{appId:f.appId,requestId:'publication-race-rollback-A',preparationId:prepared.preparationId,expectedPolicyEpoch:prepared.expectedPolicyEpoch,expectedTargetRevision:prepared.expectedTargetRevision,launchPolicy:'anyone',listed:true,exposureAck:{scope:'whole-port',targetRevision:prepared.target.revision,targetDigest:prepared.target.digest,profile:prepared.target.profile}}));
 const rolledBack=good(await f.ownerCall('apps.inspect',{appId:f.appId}));assert.equal(rolledBack.source.digest,original.source.digest);assert.equal(rolledBack.source.revision,original.source.revision);
 const replay=good(await f.ownerCall('apps.publication.update',args));assert.equal(replay.replayed,true);assert.equal(replay.universalReconciliation.state,'held');
 const next=good(await f.ownerCall('apps.publication.update',await f.publication('publication-after-real-ABA-002')));assert.equal(next.universalReconciliation.state,'held');
 const held=await f.get();assert.equal(held.state,'authority-stale');assert.equal(held.descriptor,undefined,'metadata is not an execution/feedback admission after Source promotion');
 const explicit=good(await f.ownerCall('apps.universal.admit',{appId:f.appId,expectedRevision:held.revision,requestId:'publication-explicit-source-refresh',proposal:{kind:'author-draft',draft:{title:'Explicit new Source acceptance'}}}));assert.equal(explicit.registration.state,'ready');
});

test('pending automatic reconcile cannot replace a newer explicitly accepted descriptor',{timeout:20000},async t=>{
 const f=await setup(t),args=await f.publication('publication-newer-settings-001'),restore=interruptAfterPublication(f);
 const interrupted=await f.ownerCall('apps.publication.update',args);restore();assert.equal(interrupted.ok,false);
 const stale=await f.get(),descriptor=structuredClone(f.prior.descriptor);descriptor.app.visibility='public';descriptor.app.title='NEWER explicitly accepted human settings';
 const explicit=good(await f.ownerCall('apps.universal.admit',{appId:f.appId,expectedRevision:stale.revision,requestId:'publication-newer-descriptor-002',proposal:{kind:'descriptor-json',json:JSON.stringify(descriptor)}})).registration;
 const replay=good(await f.ownerCall('apps.publication.update',args));assert.equal(replay.universalReconciliation.state,'superseded');const current=await f.get();
 assert.equal(current.generation,explicit.generation);assert.equal(current.descriptor.app.title,descriptor.app.title);assert.deepEqual(current.descriptor,descriptor);assert.equal(current.feedback.installationId,f.prior.feedback.installationId);
});

test('unknown stale metadata without a trusted pre-publication anchor never auto-admits',{timeout:20000},async t=>{
 const f=await setup(t);good(await f.ownerCall('apps.update',{appId:f.appId,name:'Unreconciled external settings change'}));
 f.journal(db=>db.prepare('DELETE FROM publication_anchors WHERE app_id=?').run(f.appId));assert.equal((await f.get()).state,'authority-stale');
 const result=good(await f.ownerCall('apps.publication.update',await f.publication('publication-unknown-stale-001')));assert.equal(result.universalReconciliation.state,'held');
 const held=await f.get();assert.equal(held.state,'authority-stale');assert.equal(held.descriptor,undefined);
});
