import test from 'node:test';
import assert from 'node:assert/strict';
import { createLegacyAvatarAdapter,profileAvatarViewSql } from '../server/legacy-avatar.mjs';
import { fixture,alice,bob,image,dataUrl } from './support/fixture.mjs';

test('Root World6 legacy migration preserves exact MIME/bytes/profile revisions and serves the same compatibility projection',async t=>{
  const original=await image('#445566',64,64),thumbnail=await image('#445566',24,24),f=await fixture(t,{kind:'profile',legacyPair:{image:original,thumbnail}});
  const row=f.db.prepare('SELECT * FROM profile_avatars WHERE account_id=?').get(alice.accountId);
  assert.equal(f.db.prepare("SELECT type FROM sqlite_schema WHERE name='profile_avatars'").get().type,'view');
  assert.equal(f.db.prepare("SELECT sql FROM sqlite_schema WHERE name='profile_avatars'").get().sql,profileAvatarViewSql());
  assert.equal(row.mime,'image/png');assert.deepEqual(Buffer.from(row.image),original);assert.deepEqual(Buffer.from(row.thumbnail),thumbnail);
  const profile=f.db.prepare('SELECT revision,avatar_revision FROM profiles WHERE account_id=?').get(alice.accountId);assert.equal(profile.revision,7);assert.equal(profile.avatar_revision,7);
  assert.equal(f.db.prepare('SELECT origin FROM profile_avatar_assets').get().origin,'legacy');assert.equal(f.store.audit().ok,true);f.reopen();assert.deepEqual(Buffer.from(f.db.prepare('SELECT image FROM profile_avatars WHERE account_id=?').get(alice.accountId).image),original);
});

test('legacy avatar.set uses the common writer rather than inserting into a view; deletion, CAS and restore survive reload',async t=>{
  const f=await fixture(t,{kind:'profile'}),adapter=createLegacyAvatarAdapter({store:f.store});
  const original=await image('#112233',64,64),thumbnail=await image('#112233',24,24);
  const result=adapter.set(alice,{expectedRevision:7,avatarUrl:dataUrl(original),thumbnailUrl:dataUrl(thumbnail)});assert.equal(result.profile.revision,8);assert.equal(result.profile.avatarRevision,8);
  const version=f.store.read(alice,{targetId:alice.accountId}).current.version;
  assert.throws(()=>adapter.set(alice,{expectedRevision:7,avatarUrl:dataUrl(original),thumbnailUrl:dataUrl(thumbnail)}),error=>error.code==='revision_conflict');
  assert.throws(()=>adapter.set(alice,{expectedRevision:8,avatarUrl:dataUrl(original),thumbnailUrl:null}),error=>error.code==='invalid_avatar_data');
  const removed=adapter.set(alice,{expectedRevision:8,avatarUrl:null,thumbnailUrl:null});assert.equal(removed.profile.revision,9);assert.equal(removed.profile.avatarRevision,null);assert.equal(f.db.prepare('SELECT * FROM profile_avatars WHERE account_id=?').get(alice.accountId),undefined);
  const restored=f.store.restore(alice,{...f.args(alice.accountId),versionId:version});assert.equal(restored.ownerProjection.revision,10);assert.equal(restored.ownerProjection.avatarRevision,10);assert.deepEqual(Buffer.from(f.db.prepare('SELECT image FROM profile_avatars WHERE account_id=?').get(alice.accountId).image),original);f.reopen();assert.equal(f.store.audit().ok,true);
});

test('new World avatar upload canonicalizes and uses current profile revision; another account cannot read full bytes',async t=>{
  const f=await fixture(t,{kind:'profile'}),first=await f.publish();assert.equal(first.committed.ownerProjection.revision,8);
  const read=f.store.read(alice,{targetId:alice.accountId});assert.equal(read.image.mime,'image/webp');assert.deepEqual(Buffer.from(f.db.prepare('SELECT image FROM profile_avatars WHERE account_id=?').get(alice.accountId).image),read.image.bytes);
  assert.throws(()=>f.store.read(bob,{targetId:alice.accountId}),error=>error.code==='cover_target_unavailable');
  f.db.prepare("UPDATE profiles SET audience='public',revision=revision+1 WHERE account_id=?").run(alice.accountId);assert.ok(f.store.read(bob,{targetId:alice.accountId}).image);
  f.db.prepare("UPDATE profiles SET audience='private',revision=revision+1 WHERE account_id=?").run(alice.accountId);assert.throws(()=>f.store.read(bob,{targetId:alice.accountId}),error=>error.code==='cover_target_unavailable');
});

test('profile compatibility callback failure cannot publish an asset or advance only one of the revisions',async t=>{
  let failing=true;const f=await fixture(t,{kind:'profile',onCommitFailure:()=>failing});
  const draft=await f.store.prepareUpload(alice,{...f.args(alice.accountId),bytes:await image(),mime:'image/png'}),args={requestId:f.request(),targetId:alice.accountId,draftId:draft.draft.id};
  assert.throws(()=>f.store.commit(alice,args),/fixture_commit_failure/u);assert.equal(f.store.read(alice,{targetId:alice.accountId}).image,null);assert.equal(f.db.prepare('SELECT revision FROM profiles WHERE account_id=?').get(alice.accountId).revision,7);
  failing=false;assert.equal(f.store.commit(alice,args).ownerProjection.revision,8);assert.equal(f.store.audit().ok,true);
});
