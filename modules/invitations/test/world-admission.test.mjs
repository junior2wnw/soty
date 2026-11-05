import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createWorldService} from '../../world/server/index.mjs';
import {createInvitationsService} from '../server.mjs';
import {createInvitationTargetAdapter} from '../target-adapter.mjs';

const person=name=>({accountId:name,deviceId:name+'_device',label:name});
const owner=person('owner'),alice=person('alice'),bob=person('bob'),carol=person('carol');
const code=value=>error=>{assert.equal(error.code,value);return true;};
function fixture(t){
 const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-world-invites-'));
 const worldPath=join(folder,'world.sqlite'),invitationPath=join(folder,'invitations.sqlite');
 let world,invitations,serial=0,now=1900000000000,fault,observationFault=false;
 const admissions=[],changes=[],revoked=new Set();
 function open(){
  world=createWorldService({databasePath:worldPath,projectId:'invitation_tests',clock:()=>now});
  const adapter=createInvitationTargetAdapter({apps:{invitationOwnedTargets:()=>[]},world});
  invitations=createInvitationsService({databasePath:invitationPath,clock:()=>now,actorActive:a=>!revoked.has(a.accountId),...adapter,
   admitTarget:request=>{if(fault==='before-world'){fault=null;throw new Error('fault_before_world_commit');}
    const result=adapter.admitTarget(request);if(fault==='after-world'){fault=null;throw new Error('fault_after_world_commit');}return result;},
   onAccessChanged:event=>changes.push(event),onAdmission:event=>{admissions.push(event);if(observationFault)throw new Error('observer_failed');}});
 }
 open();
 t.after(()=>{invitations.close();world.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-world-invites-'));rmSync(actual,{recursive:true,force:true});});
 const w=(actor,op,args={})=>world.execute({actor,op:'world.'+op,args});
 for(const actor of [owner,alice,bob,carol])w(actor,'profile.get');
 const communityId=w(owner,'community.create',{requestId:'group_create',name:'Private group',joinPolicy:'invite'}).communityId;
 const target={kind:'community',id:communityId};
 const call=(actor,op,args={})=>invitations.execute({actor,op:'invitations.'+op,args:{expectedAccountId:actor.accountId,...args}});
 const mutate=(actor,op,args={})=>call(actor,op,{requestId:'command_'+(++serial),...args});
 const configure=(args={})=>mutate(owner,'configure',{target,expectedRevision:call(owner,'get',{target}).policy.revision,enabled:true,mode:'direct',description:'Join the group',fields:[],...args});
 const issue=(args={},actor=owner)=>mutate(actor,'issue',{target,label:'Invitation',expiresAt:now+86400000,maxUses:0,...args}).link;
 const submit=(link,actor=alice,args={})=>mutate(actor,'submit',{token:link.token,formRevision:1,answers:{},consent:true,...args});
 const query=(path,sql,...args)=>{const db=new DatabaseSync(path);try{return db.prepare(sql).get(...args);}finally{db.close();}};
 const member=actor=>query(worldPath,'SELECT * FROM memberships WHERE community_id=? AND account_id=?',communityId,actor.accountId);
 const claim=actor=>query(invitationPath,'SELECT * FROM admissions WHERE recipient_id=?',actor.accountId);
 return {call,mutate,configure,issue,submit,w,target,member,claim,worldPath,invitationPath,query,admissions,changes,revoked,
  get service(){return invitations;},fault(value){fault=value;},observerFault(){observationFault=true;},advance(ms){now+=ms;},
  restart(){invitations.close();world.close();open();}};
}

test('one bound recipient is admitted as member; its role label never grants management and replay survives restart',t=>{
 const f=fixture(t);f.configure();const link=f.issue({recipientLabel:'Замдиректора — owner',recipientAccountId:alice.accountId,maxUses:1});
 const preview=f.service.preview(link.token);assert.equal(preview.recipientLabel,'Замдиректора — owner');assert.equal(preview.recipientBound,true);assert.equal('recipientAccountId' in preview,false);
 assert.throws(()=>f.submit(link,bob),code('invitation_recipient_mismatch'));assert.equal(f.member(alice),undefined);
 const args={requestId:'stable_redemption'},first=f.submit(link,alice,args);assert.equal(first.request.state,'approved');assert.equal(first.admission.state,'admitted');
 assert.equal(f.member(alice).role,'member');assert.equal(f.member(alice).state,'active');assert.throws(()=>f.issue({},alice),code('invitation_manager_required'));
 assert.equal(f.query(f.invitationPath,'SELECT uses FROM links WHERE id=?',link.id).uses,1);
 assert.equal(f.query(f.invitationPath,'SELECT COUNT(*) n FROM members WHERE app_id=?',f.target.id).n,0,'World remains the only membership authority');
 const revision=f.member(alice).revision;f.restart();assert.deepEqual(f.submit(link,alice,args),first);assert.equal(f.member(alice).revision,revision);
 assert.deepEqual(f.call(alice,'status',{token:link.token}).target,f.target);assert.equal(f.changes.at(-1).target.kind,'community');
 assert.throws(()=>f.call(alice,'status',{token:link.token,expectedAccountId:bob.accountId}),code('invitation_account_changed'));
});

test('a reusable group link consumes one use per recipient, enforces limits, expiry and revocation',t=>{
 const f=fixture(t);f.configure();const reusable=f.issue({maxUses:2});f.submit(reusable,alice);f.submit(reusable,alice);f.submit(reusable,bob);
 assert.equal(f.query(f.invitationPath,'SELECT uses FROM links WHERE id=?',reusable.id).uses,2);assert.throws(()=>f.submit(reusable,carol),code('invitation_unavailable'));
 const revoked=f.issue();f.mutate(owner,'revoke',{target:f.target,linkId:revoked.id});assert.throws(()=>f.submit(revoked,carol),code('invitation_unavailable'));
 const expires=f.issue();f.advance(86400001);assert.throws(()=>f.submit(expires,carol),code('invitation_unavailable'));
 assert.equal(f.member(alice).state,'active','revoking another link does not undo admitted membership');
});

test('approval and creation use current World owner/moderator rights; no parallel role setter is available',t=>{
 const f=fixture(t);f.configure({mode:'approval'});const link=f.issue(),pending=f.submit(link);
 assert.equal(pending.request.state,'pending');assert.equal(f.member(alice),undefined);
 f.w(owner,'membership.invite',{communityId:f.target.id,profileId:bob.accountId});f.w(bob,'membership.join',{communityId:f.target.id});f.w(owner,'membership.role',{communityId:f.target.id,profileId:bob.accountId,role:'moderator'});
 const result=f.mutate(bob,'decide',{target:f.target,applicationId:pending.request.id,expectedRevision:1,decision:'approved'});assert.equal(result.admission.state,'admitted');assert.equal(f.member(alice).role,'member');
 assert.throws(()=>f.mutate(owner,'member.set',{target:f.target,accountId:alice.accountId,label:'owner',role:'director',active:true,expectedRevision:1}),code('invitation_world_membership_managed'));
 f.w(owner,'membership.role',{communityId:f.target.id,profileId:bob.accountId,role:'member'});assert.throws(()=>f.issue({},bob),code('invitation_manager_required'));
});

test('an actual failed Invitations write never reaches World or consumes a use',t=>{
 const f=fixture(t);f.configure();const link=f.issue();const db=new DatabaseSync(f.invitationPath);
 db.exec("CREATE TRIGGER fail_claim BEFORE INSERT ON admissions BEGIN SELECT RAISE(ABORT,'fault_invitation_write'); END");
 try{assert.throws(()=>f.submit(link),/fault_invitation_write/u);}finally{db.exec('DROP TRIGGER fail_claim');db.close();}
 assert.equal(f.member(alice),undefined);assert.equal(f.claim(alice),undefined);assert.equal(f.query(f.invitationPath,'SELECT uses FROM links WHERE id=?',link.id).uses,0);
 assert.equal(f.submit(link).admission.state,'admitted','same oracle passes after the write fault is removed');
});

test('crash after the durable claim keeps a consumed pending admission; restart reconcile completes it exactly once',t=>{
 const f=fixture(t);f.configure();const link=f.issue({maxUses:1});f.fault('before-world');assert.throws(()=>f.submit(link),/fault_before_world_commit/u);
 const claim=f.claim(alice);assert.equal(claim.state,'pending');assert.equal(f.member(alice),undefined);assert.equal(f.query(f.invitationPath,'SELECT uses FROM links WHERE id=?',link.id).uses,1);
 assert.equal(f.call(alice,'status',{token:link.token}).state,'pending-admission');assert.equal(f.call(owner,'request.get',{target:f.target,applicationId:claim.request_id}).request.state,'pending-admission');
 f.restart();const result=f.mutate(alice,'reconcile',{claimId:claim.claim_id});assert.equal(result.admission.state,'admitted');assert.equal(f.member(alice).state,'active');const revision=f.member(alice).revision;
 assert.equal(f.mutate(alice,'reconcile',{claimId:claim.claim_id}).admission.state,'admitted');assert.equal(f.member(alice).revision,revision);
});

test('a failed World receipt write rolls membership back; reconciliation can safely retry the already counted claim',t=>{
 const f=fixture(t);f.configure();const link=f.issue({maxUses:1}),db=new DatabaseSync(f.worldPath);
 db.exec("CREATE TRIGGER fail_admit BEFORE INSERT ON receipts WHEN NEW.operation='world.invitation.admit' BEGIN SELECT RAISE(ABORT,'fault_world_receipt'); END");
 try{assert.throws(()=>f.submit(link),/fault_world_receipt/u);}finally{db.exec('DROP TRIGGER fail_admit');db.close();}
 const claim=f.claim(alice);assert.equal(claim.state,'pending');assert.equal(f.member(alice),undefined);assert.equal(f.query(f.invitationPath,'SELECT uses FROM links WHERE id=?',link.id).uses,1);
 assert.equal(f.mutate(alice,'reconcile',{claimId:claim.claim_id}).admission.state,'admitted');assert.equal(f.member(alice).state,'active');
});

test('crash after the World commit is recovered from its durable receipt and never resurrects a removed member',t=>{
 const f=fixture(t);f.configure();const link=f.issue();f.fault('after-world');assert.throws(()=>f.submit(link),/fault_after_world_commit/u);
 const claim=f.claim(alice);assert.equal(claim.state,'pending');assert.equal(f.member(alice).state,'active');const revision=f.member(alice).revision;
 assert.equal(f.service.readAdmission(claim.claim_id).state,'admitted');f.restart();assert.equal(f.mutate(alice,'reconcile',{claimId:claim.claim_id}).admission.state,'admitted');assert.equal(f.member(alice).revision,revision);
 f.w(owner,'membership.remove',{communityId:f.target.id,profileId:alice.accountId});assert.equal(f.mutate(alice,'reconcile',{claimId:claim.claim_id}).admission.state,'removed');assert.equal(f.member(alice).state,'removed');
 assert.equal(f.query(f.worldPath,"SELECT count(*) n FROM receipts WHERE operation='world.invitation.admit'").n,1);
});

test('link revoke or recipient ban between commits blocks admission while the claimed use stays accounted for',t=>{
 const f=fixture(t);f.configure();const revoked=f.issue();f.fault('before-world');assert.throws(()=>f.submit(revoked),/fault_before_world_commit/u);
 f.mutate(owner,'revoke',{target:f.target,linkId:revoked.id});const first=f.mutate(alice,'reconcile',{claimId:f.claim(alice).claim_id});assert.equal(first.admission.state,'blocked');assert.equal(f.member(alice),undefined);
 const banned=f.issue();f.fault('before-world');assert.throws(()=>f.submit(banned,bob),/fault_before_world_commit/u);
 f.w(owner,'membership.invite',{communityId:f.target.id,profileId:bob.accountId});f.w(owner,'membership.ban',{communityId:f.target.id,profileId:bob.accountId});
 assert.equal(f.mutate(bob,'reconcile',{claimId:f.claim(bob).claim_id}).admission.state,'blocked');assert.equal(f.member(bob).state,'banned');
 assert.equal(f.query(f.invitationPath,'SELECT uses FROM links WHERE id=?',revoked.id).uses,1);assert.equal(f.query(f.invitationPath,'SELECT uses FROM links WHERE id=?',banned.id).uses,1);
});

test('a failed observer cannot undo membership; reconcile repeats the verified admission callback',t=>{
 const f=fixture(t);f.configure();const link=f.issue();f.observerFault();const result=f.submit(link);assert.equal(result.admission.state,'admitted');assert.equal(f.member(alice).state,'active');
 const before=f.admissions.length;f.mutate(alice,'reconcile',{claimId:result.admission.claimId});assert.equal(f.admissions.length,before+1);assert.equal(f.service.readAdmission(result.admission.claimId).state,'admitted');
 assert.throws(()=>f.mutate(carol,'reconcile',{claimId:result.admission.claimId}),code('invitation_manager_required'));
});

test('demoted issuer and disabled admission policy are rechecked after the durable claim',t=>{
 const f=fixture(t);f.configure();f.w(owner,'membership.invite',{communityId:f.target.id,profileId:bob.accountId});f.w(bob,'membership.join',{communityId:f.target.id});f.w(owner,'membership.role',{communityId:f.target.id,profileId:bob.accountId,role:'moderator'});
 const moderatorLink=f.issue({},bob);f.fault('before-world');assert.throws(()=>f.submit(moderatorLink),/fault_before_world_commit/u);
 f.w(owner,'membership.role',{communityId:f.target.id,profileId:bob.accountId,role:'member'});assert.equal(f.mutate(alice,'reconcile',{claimId:f.claim(alice).claim_id}).admission.state,'blocked');assert.equal(f.member(alice),undefined);
 const ownerLink=f.issue();f.fault('before-world');assert.throws(()=>f.submit(ownerLink,carol),/fault_before_world_commit/u);f.configure({enabled:false});
 assert.equal(f.mutate(carol,'reconcile',{claimId:f.claim(carol).claim_id}).admission.state,'blocked');assert.equal(f.member(carol),undefined);
});
