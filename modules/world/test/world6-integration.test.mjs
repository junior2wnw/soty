import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { signedFixture, actor, good, denied } from './support/world6-signed-fixture.mjs';
import { pngFixture, asDataUrl } from './avatar-fixtures.mjs';
import { createCoverDecoder, canonicalizeCover } from '../../cover-art/server/codec.mjs';
const deferred = () => { let resolve; const promise=new Promise(done=>{resolve=done;});return {promise,resolve}; };
const tick = () => new Promise(resolve=>setImmediate(resolve));
async function until(check) { for(let n=0;n<100;n++){if(check())return;await tick();}throw Error('world6_controlled_wait'); }
const own = (person,targetId=person.accountId) => ({expectedAccountId:person.accountId,targetId});

test('direct messaging is advertised only on the own profile with live contact authority', async t => {
  const f=await signedFixture(t),[a,b]=f.actors;
  assert.equal((await f.profile(a)).directMessagesVersion,1);
  const disconnected=await signedFixture(t,{withContacts:false});
  assert.equal((await disconnected.profile(disconnected.actors[0])).directMessagesVersion,undefined);
  good(await f.rpc(a,'world.profile.update',{expectedRevision:1,audience:'public'}));
  assert.equal(good(await f.rpc(b,'world.profile.view',{profileId:a.accountId})).profile.directMessagesVersion,undefined);
});

test('World6 advertises avatar protocol only with actual store, decoder, live bridge and registered async operations', async t => {
  const f=await signedFixture(t),[a,b]=f.actors;assert.equal(f.world.schemaVersion,6);assert.equal((await f.profile(a)).avatarWriteVersion,1);
  const actual=await f.status(a);assert.equal(actual.scope.kind,'profile');assert.equal(actual.scope.ownerAccountId,a.accountId);
  const old=await signedFixture(t,{withDecoder:false});assert.equal((await old.profile(old.actors[0])).avatarWriteVersion,undefined);
  assert.equal(old.world.profileCoverExtension,undefined);
  const unregistered=await signedFixture(t,{registerCover:false});assert.equal((await unregistered.profile(unregistered.actors[0])).avatarWriteVersion,undefined);
  denied(await unregistered.rpc(unregistered.actors[0],'world.profile.cover.status',own(unregistered.actors[0])));
  const noBridge=await signedFixture(t,{withContacts:false});assert.equal((await noBridge.profile(noBridge.actors[0])).avatarWriteVersion,undefined);
  denied(await noBridge.rpc(noBridge.actors[0],'world.direct.list',{expectedAccountId:noBridge.actors[0].accountId}),'direct_authority_unavailable');
  good(await f.rpc(a,'world.profile.update',{expectedRevision:1,audience:'public'}));
  const projection=good(await f.rpc(b,'world.profile.view',{profileId:a.accountId})).profile;
  assert.equal(projection.avatarWriteVersion,undefined);assert.equal(Object.hasOwn(projection,'avatarUrl'),false);
});
test('invalid first legacy request validates closed shape, revision and both raster payloads before private profile provisioning', async t => {
  const f=await signedFixture(t),a=f.actors[0],image=asDataUrl(pngFixture(12,8)),thumb=asDataUrl(pngFixture(2,2));
  const input={expectedRevision:1,avatarUrl:image,thumbnailUrl:thumb},badCrc=Buffer.from(pngFixture());badCrc[29]^=1;
  const absent=()=>{const db=new DatabaseSync(f.worldPath,{readOnly:true});try{
    assert.equal(db.prepare('SELECT count(*) AS n FROM profiles').get().n,0);
    assert.equal(db.prepare('SELECT count(*) AS n FROM profile_avatar_heads').get().n,0);
    assert.equal(db.prepare('SELECT count(*) AS n FROM profile_avatar_versions').get().n,0);
  }finally{db.close();}};
  // Trusted host DTO acceptance is also closed: do not execute accessors or
  // accept a hidden/symbol field before validating/provisioning this actor.
  let accessed=0;
  const getter={...input};Object.defineProperty(getter,'avatarUrl',{enumerable:true,get(){accessed++;return image;}});
  const hidden={...input};Object.defineProperty(hidden,'expectedRevision',{value:1,enumerable:false});
  const symbol={...input,[Symbol('extra')]:true};
  for(const args of [getter,hidden,symbol,Object.assign(Object.create({}),input)]){
    assert.throws(()=>f.world.execute({op:'world.profile.avatar.set',actor:a,args}),{code:'invalid_arguments'});absent();
  }
  assert.equal(accessed,0);
  for(const [args,code] of [
    [{...input,ownerId:a.accountId},'invalid_arguments'],[{avatarUrl:image,thumbnailUrl:thumb},'invalid_arguments'],
    [{...input,expectedRevision:0},'invalid_number'],[{...input,expectedRevision:2},'revision_conflict'],
    [{...input,avatarUrl:'https://example.invalid/image.png'},'invalid_avatar_mime'],
    [{...input,avatarUrl:'data:image/png;base64,####'},'invalid_avatar_data'],
    [{...input,avatarUrl:asDataUrl(badCrc)},'invalid_avatar_data'],
    [{...input,avatarUrl:asDataUrl(pngFixture(513,1))},'avatar_dimensions'],
    [{...input,thumbnailUrl:asDataUrl(pngFixture(193,1))},'avatar_dimensions'],
    [{...input,thumbnailUrl:null},'invalid_avatar_data'],
  ]){denied(await f.rpc(a,'world.profile.avatar.set',args),code);absent();}
  assert.throws(()=>f.world.execute({op:'world.profile.avatar.set',actor:{...a,label:undefined},args:input}),{code:'invalid_text'});absent();
});

test('legacy first-use avatar.set stays synchronous, writes common versions and retains full/room-thumbnail ACL', async t => {
  const f=await signedFixture(t),[a,b,c]=f.actors,image=asDataUrl(pngFixture(12,8)),thumb=asDataUrl(pngFixture(2,2));
  // No prior World.profile.get: legacy first-use provisioning is preserved.
  const changed=good(await f.rpc(a,'world.profile.avatar.set',{expectedRevision:1,avatarUrl:image,thumbnailUrl:thumb})).profile;
  assert.equal(changed.revision,2);assert.equal(changed.avatarRevision,2);
  assert.equal(good(await f.rpc(a,'world.profile.avatar.read',{profileId:a.accountId})).avatarUrl,image);
  const group=good(await f.rpc(a,'world.community.create',{requestId:'legacy_avatar_room',name:'Synthetic private room',joinPolicy:'open'})).community;
  good(await f.rpc(b,'world.membership.join',{communityId:group.communityId}));
  assert.equal(good(await f.rpc(b,'world.profile.avatars',{profileIds:[a.accountId],communityId:group.communityId})).avatars[0].avatarUrl,thumb);
  denied(await f.rpc(b,'world.profile.avatar.read',{profileId:a.accountId,communityId:group.communityId}),'profile_not_found');
  denied(await f.rpc(b,'world.profile.cover.read',{...own(b,a.accountId),communityId:group.communityId}),'invalid_arguments');
  denied(await f.rpc(b,'world.profile.cover.read',own(b,a.accountId)),'cover_target_unavailable');
  denied(await f.rpc(c,'world.profile.avatars',{profileIds:[a.accountId],communityId:group.communityId}),'community_membership_required');
  denied(await f.rpc(a,'world.profile.avatars',{profileIds:Array(25).fill(a.accountId)}),'invalid_avatar_batch');
  const db=new DatabaseSync(f.worldPath,{readOnly:true});try{
    assert.equal(db.prepare("SELECT type FROM sqlite_schema WHERE name='profile_avatars'").get().type,'view');
    assert.equal(db.prepare('SELECT count(*) AS n FROM profile_avatar_versions WHERE target_id=?').get(a.accountId).n,1);
  }finally{db.close();}
  denied(await f.rpc(a,'world.profile.avatar.set',{expectedRevision:1,avatarUrl:image,thumbnailUrl:thumb}),'revision_conflict');
});
test('actual signed upload/commit changes profile pixels and revisions atomically, then replay/restart never reapplies an older choice', async t => {
  const f=await signedFixture(t),a=f.actors[0],first=await f.publish(a);
  assert.equal(first.committed.ownerProjection.revision,2);assert.equal(first.committed.ownerProjection.avatarRevision,2);
  const newRead=good(await f.readBytes(a)),legacy=good(await f.rpc(a,'world.profile.avatar.read',{profileId:a.accountId}));
  assert.equal(legacy.avatarUrl,`data:${newRead.image.mime};base64,${newRead.image.dataBase64}`);
  assert.equal(newRead.image.byteCount,Buffer.from(newRead.image.dataBase64,'base64').length);
  assert.equal(JSON.stringify(await f.profile(a)).includes('base64'),false);
  const second=await f.publish(a,{bytes:pngFixture(32,64),crop:{x:.3,y:.7,zoom:2}}),latest=second.committed.current.version;
  f.reopen();const replay=good(await f.rpc(a,'world.profile.cover.commit',first.commitArgs));
  assert.equal(replay.replayed,true);assert.deepEqual(replay.receipt,first.committed.receipt);assert.equal(replay.current.version,latest);
  assert.equal((await f.profile(a)).revision,3);assert.equal((await f.profile(a)).avatarRevision,3);
  const signed=await f.proof(a,'world.profile.cover.status',own(a));good(await f.connect.handle(signed));denied(await f.connect.handle(signed),'challenge_consumed');
});
test('actual profile SQL failure rolls back cover publication and receipt, then the same signed business intent can retry', async t => {
  const f=await signedFixture(t),a=f.actors[0];await f.profile(a);const context=await f.status(a);
  const prepared=good(await f.rpc(a,'world.profile.cover.prepare',{...own(a),requestId:f.request(),expectedTargetRevision:context.scope.targetRevision,
    expectedPolicyEpoch:context.scope.policyEpoch,expectedCoverRevision:0,mime:'image/png',dataBase64:pngFixture().toString('base64')}));
  const args={...own(a),requestId:f.request(),draftId:prepared.draft.id},db=new DatabaseSync(f.worldPath);
  try{
    db.exec("CREATE TRIGGER world6_test_profile_refuse BEFORE UPDATE OF avatar_revision ON profiles BEGIN SELECT RAISE(ABORT,'controlled_profile_commit_failure'); END");
    denied(await f.rpc(a,'world.profile.cover.commit',args));
    assert.equal(db.prepare('SELECT revision FROM profiles WHERE account_id=?').get(a.accountId).revision,1);
    assert.equal(db.prepare('SELECT count(*) AS n FROM profile_avatar_heads').get().n,0);
    assert.equal(db.prepare('SELECT count(*) AS n FROM profile_avatar_versions').get().n,0);
    assert.equal(db.prepare('SELECT count(*) AS n FROM profile_avatar_receipts WHERE request_key=?').get(args.requestId).n,0);
    assert.equal(db.prepare('SELECT state FROM profile_avatar_drafts WHERE id=?').get(prepared.draft.id).state,'ready');
  }finally{db.exec('DROP TRIGGER IF EXISTS world6_test_profile_refuse');db.close();}
  const committed=good(await f.rpc(a,'world.profile.cover.commit',args));assert.equal(committed.replayed,false);
  assert.equal(committed.ownerProjection.revision,2);assert.equal(committed.ownerProjection.avatarRevision,2);
});

test('profile cover read follows current friends policy; owner-only history and closed args cannot inherit room/App rights', async t => {
  const f=await signedFixture(t),[a,b,c]=f.actors;await f.publish(a);const rel=await f.contact(a,b);
  const before=await f.profile(a);good(await f.rpc(a,'world.profile.update',{expectedRevision:before.revision,audience:'friends'}));
  assert.ok(good(await f.rpc(b,'world.profile.cover.read',own(b,a.accountId))).image.dataBase64);
  denied(await f.rpc(c,'world.profile.cover.read',own(c,a.accountId)),'cover_target_unavailable');
  denied(await f.rpc(b,'world.profile.cover.versions',own(b,a.accountId)),'cover_target_unavailable');
  denied(await f.rpc(b,'world.profile.cover.status',own(b,a.accountId)),'cover_target_unavailable');
  denied(await f.rpc(b,'world.profile.cover.remove',{...own(b,a.accountId),requestId:f.request(),expectedTargetRevision:3,expectedPolicyEpoch:3,expectedCoverRevision:1}),'cover_target_unavailable');
  denied(await f.rpc(a,'world.profile.cover.read',{...own(a),actor:{accountId:c.accountId}}),'invalid_arguments');
  denied(await f.rpc(b,'world.profile.cover.read',{...own(b,a.accountId),expectedAccountId:a.accountId}),'profile_account_changed');
  good(await f.rpc(a,'contacts.remove',{relationshipId:rel}));denied(await f.rpc(b,'world.profile.cover.read',own(b,a.accountId)),'cover_target_unavailable');
  denied(await f.rpc(b,'world.profile.avatar.read',{profileId:a.accountId}),'profile_not_found');
});
test('native decoder awaits outside authority/World locks and stale profile policy rejects the prepared result', async t => {
  const held=deferred();t.after(()=>held.resolve());let started=false;
  const decoder=createCoverDecoder({decode:async input=>{started=true;await held.promise;return canonicalizeCover(input);}});
  const f=await signedFixture(t,{decoder}),a=f.actors[0];await f.profile(a);
  const args={...own(a),requestId:f.request(),expectedTargetRevision:1,expectedPolicyEpoch:1,expectedCoverRevision:0,mime:'image/png',dataBase64:pngFixture(60,60).toString('base64')};
  const pending=f.rpc(a,'world.profile.cover.prepare',args);await until(()=>started);
  // This real signed World mutation must complete while native work is held.
  good(await f.rpc(a,'world.profile.update',{expectedRevision:1,audience:'public'}));held.resolve();
  denied(await pending,'cover_target_changed');const after=await f.status(a);
  assert.equal(after.current.revision,0);assert.equal((await f.profile(a)).revision,2);assert.equal((await f.profile(a)).avatarRevision,null);
});
test('device revoked during native preparation cannot finish or commit through a retained signed actor', async t => {
  const held=deferred();t.after(()=>held.resolve());let started=false;const decoder=createCoverDecoder({decode:async input=>{started=true;await held.promise;return canonicalizeCover(input);}});
  const f=await signedFixture(t,{decoder}),a=f.actors[0],phone=actor('Synthetic phone');await f.profile(a);
  const start=good(await f.rpc(phone,'enrollment.start',{label:phone.label,encryptionPublicJwk:phone.encryptionPublicJwk}));
  good(await f.rpc(a,'enrollment.approve',{requestId:start.requestId,wrappedKey:{schema:'test.encrypted',ciphertext:'synthetic'}}));
  Object.assign(phone,good(await f.rpc(phone,'enrollment.finish',{requestId:start.requestId,expectedAccountId:a.accountId})));
  const context=await f.status(phone),pending=f.rpc(phone,'world.profile.cover.prepare',{...own(phone),requestId:f.request(),expectedTargetRevision:context.scope.targetRevision,
    expectedPolicyEpoch:context.scope.policyEpoch,expectedCoverRevision:0,mime:'image/png',dataBase64:pngFixture().toString('base64')});
  await until(()=>started);good(await f.rpc(a,'device.revoke',{deviceId:phone.deviceId}));held.resolve();denied(await pending,'device_revoked');
  assert.equal((await f.status(a)).current.revision,0);assert.equal((await f.profile(a)).avatarRevision,null);
});
test('restore/remove/legacy share one head and real avatar projection after reload', async t => {
  const f=await signedFixture(t),a=f.actors[0],image=asDataUrl(pngFixture(9,5)),thumb=asDataUrl(pngFixture(2,2));
  good(await f.rpc(a,'world.profile.avatar.set',{expectedRevision:1,avatarUrl:image,thumbnailUrl:thumb}));
  const versions=good(await f.rpc(a,'world.profile.cover.versions',own(a))),old=versions.versions[0].id;
  await f.publish(a);f.reopen();const context=await f.status(a);
  const restored=good(await f.rpc(a,'world.profile.cover.restore',{...own(a),requestId:f.request(),versionId:old,
    expectedTargetRevision:context.scope.targetRevision,expectedPolicyEpoch:context.scope.policyEpoch,expectedCoverRevision:context.scope.coverRevision}));
  assert.equal(restored.ownerProjection.revision,4);assert.equal(restored.ownerProjection.avatarRevision,4);
  assert.equal(good(await f.rpc(a,'world.profile.avatar.read',{profileId:a.accountId})).avatarUrl,image);
  good(await f.rpc(a,'world.profile.avatar.set',{expectedRevision:4,avatarUrl:null,thumbnailUrl:null}));
  assert.equal((await f.profile(a)).revision,5);assert.equal((await f.profile(a)).avatarRevision,null);assert.equal((await f.status(a)).current.mode,'none');
  assert.equal(good(await f.rpc(a,'world.profile.avatar.read',{profileId:a.accountId})).avatarUrl,null);
});
test('real accepted contacts create private immutable DM pair without materializing unknown peer or community grants', async t => {
  const f=await signedFixture(t),[a,b,c]=f.actors,relationshipId=await f.contact(a,b);
  const args={expectedAccountId:a.accountId,peerAccountId:b.accountId,expectedRelationshipId:relationshipId,requestId:'direct_open_one'};
  const opened=good(await f.rpc(a,'world.direct.open',args)),threadId=opened.current.threadId;
  assert.equal(opened.current.peer.label,null);assert.equal(f.world.canAccessCommunity(a.accountId,threadId),false);assert.equal(f.world.isGroupAdmin(a.accountId,threadId),false);
  const db=new DatabaseSync(f.worldPath,{readOnly:true});try{assert.equal(db.prepare('SELECT count(*) AS n FROM profiles WHERE account_id=?').get(b.accountId).n,0);}finally{db.close();}
  const message=good(await f.rpc(a,'world.direct.chat.send',{expectedAccountId:a.accountId,threadId,expectedRelationshipId:relationshipId,clientId:'direct_message_one',text:'Private pair text'})).message;
  assert.equal(good(await f.rpc(a,'world.direct.list',{expectedAccountId:a.accountId})).threads.length,1);
  denied(await f.rpc(c,'world.direct.chat.list',{expectedAccountId:c.accountId,threadId,expectedRelationshipId:relationshipId}),'direct_not_found');
  denied(await f.rpc(a,'world.direct.open',{...args,communityId:threadId}),'invalid_arguments');
  denied(await f.rpc(a,'world.chat.send',{communityId:threadId,clientId:'wrong_group',text:'Denied'}),'community_not_found');
  denied(await f.rpc(a,'world.membership.invite',{communityId:threadId,profileId:c.accountId}),'community_not_found');
  denied(await f.rpc(a,'world.membership.list',{communityId:threadId}),'community_not_found');
  assert.equal(f.world.activeCommunityIds(a.accountId).includes(threadId),false);
  const ordinary=good(await f.rpc(a,'world.community.create',{requestId:'ordinary_room_for_dm',name:'Ordinary room'})).community;
  denied(await f.rpc(a,'world.direct.get',{expectedAccountId:a.accountId,threadId:ordinary.communityId,expectedRelationshipId:relationshipId}),'direct_not_found');
  assert.equal(JSON.stringify(good(await f.rpc(c,'world.discovery.search',{query:'Private pair text'}))).includes('Private pair text'),false);
  f.reopen();const replay=good(await f.rpc(a,'world.direct.open',args));assert.equal(replay.replayed,true);assert.equal(replay.current.threadId,threadId);
  assert.equal(good(await f.rpc(b,'world.direct.chat.list',{expectedAccountId:b.accountId,threadId,expectedRelationshipId:relationshipId})).messages[0].messageId,message.messageId);
});
test('contact remove/re-add needs explicit new relationship open and never rewrites old DM history or intents', async t => {
  const f=await signedFixture(t),[a,b]=f.actors,old=await f.contact(a,b);
  const open={expectedAccountId:a.accountId,peerAccountId:b.accountId,expectedRelationshipId:old,requestId:'direct_old_open'};
  const thread=good(await f.rpc(a,'world.direct.open',open)).current,oldMessage={expectedAccountId:a.accountId,threadId:thread.threadId,expectedRelationshipId:old,clientId:'old_intent',text:'Preserve original text'};
  const sent=good(await f.rpc(a,'world.direct.chat.send',oldMessage)).message;good(await f.rpc(b,'contacts.remove',{relationshipId:old}));
  denied(await f.rpc(a,'world.direct.get',{expectedAccountId:a.accountId,threadId:thread.threadId,expectedRelationshipId:old}),'direct_not_found');
  const fresh=await f.contact(a,b);assert.notEqual(fresh,old);denied(await f.rpc(a,'world.direct.open',open),'direct_contact_required');
  denied(await f.rpc(a,'world.direct.chat.send',{...oldMessage,expectedRelationshipId:fresh}),'direct_not_found');
  const reopened=good(await f.rpc(a,'world.direct.open',{...open,expectedRelationshipId:fresh,requestId:'direct_explicit_fresh'})).current;
  assert.equal(reopened.threadId,thread.threadId);assert.equal(reopened.relationshipId,fresh);
  denied(await f.rpc(a,'world.direct.chat.send',{...oldMessage,expectedRelationshipId:fresh}),'request_conflict');
  const history=good(await f.rpc(a,'world.direct.chat.list',{expectedAccountId:a.accountId,threadId:thread.threadId,expectedRelationshipId:fresh})).messages;
  assert.equal(history[0].messageId,sent.messageId);assert.equal(history[0].text,'Preserve original text');
  good(await f.rpc(b,'contacts.block',{peerAccountId:a.accountId}));denied(await f.rpc(a,'world.direct.get',{expectedAccountId:a.accountId,threadId:thread.threadId,expectedRelationshipId:fresh}),'direct_not_found');
});
