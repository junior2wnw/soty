import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { createWorldService } from '../server/index.mjs';
import { profileAvatarStoreId } from '../server/schema.mjs';
import { assertDirectSchema } from '../server/direct-schema.mjs';
import { assertCoverSchema } from '../../cover-art/server/schema.mjs';
import { signedFixture, temporaryStore, seedWorld5, worldSnapshot, good, PROJECT, hash } from './support/world6-signed-fixture.mjs';
import { asDataUrl, pngFixture } from './avatar-fixtures.mjs';
const legacyAccounts = ['alice','bob','carol'].map(name=>({accountId:'acct_'+name,deviceId:'device_'+name,label:'Synthetic '+name}));
function metadata(path) {
  const db=new DatabaseSync(path,{readOnly:true});try{return {version:db.prepare('PRAGMA user_version').get().user_version,
    meta:db.prepare('SELECT * FROM world_meta ORDER BY key').all(),schema:db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").all()};}finally{db.close();}
}

test('existing World5 defaults refuse migration without changing schema, IDs, ACL, receipts, field or avatar bytes', t => {
  const paths=temporaryStore(t);seedWorld5(paths.worldPath,legacyAccounts);const before=metadata(paths.worldPath),rows=worldSnapshot(paths.worldPath),bytes=hash(readFileSync(paths.worldPath));
  assert.throws(()=>createWorldService({databasePath:paths.worldPath,projectId:PROJECT}),/world6_migration_required/u);
  assert.deepEqual(metadata(paths.worldPath),before);assert.deepEqual(worldSnapshot(paths.worldPath),rows);assert.equal(hash(readFileSync(paths.worldPath)),bytes);
  assert.throws(()=>createWorldService({databasePath:paths.worldPath,projectId:PROJECT,allowAudienceMigration:true}),/world6_migration_required/u);
  assert.throws(()=>createWorldService({databasePath:paths.worldPath,projectId:PROJECT,allowWorld6Migration:'true'}),{code:'invalid_world6_migration_mode'});
  assert.deepEqual(metadata(paths.worldPath),before);
});
test('actual World5 to6 combines Direct and profile art atomically, preserving legacy rows, ACL, sequence and exact rendition bytes', async t => {
  const f=await signedFixture(t,{legacy:true}),[a,b,c]=f.actors;assert.equal(f.world.schemaVersion,6);
  assert.deepEqual(worldSnapshot(f.worldPath),f.legacyBefore);
  const db=new DatabaseSync(f.worldPath);try{
    assert.equal(db.prepare('PRAGMA user_version').get().user_version,6);assert.equal(db.prepare("SELECT value FROM world_meta WHERE key='schema_version'").get().value,'6');
    assertDirectSchema(db);assertCoverSchema(db,'profile');
    assert.equal(db.prepare('SELECT store_id FROM profile_avatar_usage WHERE id=1').get().store_id,profileAvatarStoreId(db, PROJECT));
    const stored=db.prepare('SELECT * FROM profile_avatars WHERE account_id=?').get(a.accountId);
    assert.equal(stored.mime,'image/png');assert.equal(stored.thumbnail_mime,'image/png');assert.deepEqual(Buffer.from(stored.image),f.legacyPixels.image);assert.deepEqual(Buffer.from(stored.thumbnail),f.legacyPixels.thumb);
    assert.equal(db.prepare('SELECT origin FROM profile_avatar_assets WHERE target_id=?').get(a.accountId).origin,'legacy');
    assert.equal(db.prepare('PRAGMA foreign_key_check').all().length,0);
  }finally{db.close();}
  const profile=await f.profile(a);assert.equal(profile.revision,17);assert.equal(profile.avatarRevision,13);assert.equal(profile.avatarWriteVersion,1);
  assert.equal(good(await f.rpc(a,'world.profile.avatar.read',{profileId:a.accountId})).avatarUrl,asDataUrl(f.legacyPixels.image));
  assert.equal(f.world.canAccessCommunity(b.accountId,'community_legacy'),true);assert.equal(f.world.canAccessCommunity(c.accountId,'community_legacy'),false);
  const instance=(await f.status(a)).scope.storeInstanceId;f.reopen();assert.equal((await f.status(a)).scope.storeInstanceId,instance);
  assert.equal(good(await f.rpc(b,'world.chat.list',{communityId:'community_legacy'})).messages[0].seq,42);
  assert.equal((await f.profile(a)).avatarRevision,13);
});
test('empty stores of the same project persist independent nonsecret instances across restart', async t => {
  const first=await signedFixture(t),second=await signedFixture(t),a=first.actors[0],b=second.actors[0];
  await first.profile(a);await second.profile(b);
  const left=(await first.status(a)).scope.storeInstanceId,right=(await second.status(b)).scope.storeInstanceId;
  assert.match(left,/^world_profile_[a-f0-9]{48}$/u);assert.match(right,/^world_profile_[a-f0-9]{48}$/u);assert.notEqual(left,right);
  first.reopen();second.reopen();assert.equal((await first.status(a)).scope.storeInstanceId,left);assert.equal((await second.status(b)).scope.storeInstanceId,right);
});

test('corrupt legacy raster rolls back both new fragments, marker and copy without losing the original bytes', t => {
  const paths=temporaryStore(t);seedWorld5(paths.worldPath,legacyAccounts,{corruptAvatar:true});const before=metadata(paths.worldPath),rows=worldSnapshot(paths.worldPath);
  const raw=new DatabaseSync(paths.worldPath,{readOnly:true}),original=Buffer.from(raw.prepare('SELECT image FROM profile_avatars LIMIT 1').get().image);raw.close();
  assert.throws(()=>createWorldService({databasePath:paths.worldPath,projectId:PROJECT,allowWorld6Migration:true}),error=>error.code==='invalid_avatar_data');
  assert.deepEqual(metadata(paths.worldPath),before);assert.deepEqual(worldSnapshot(paths.worldPath),rows);
  const db=new DatabaseSync(paths.worldPath,{readOnly:true});try{assert.deepEqual(Buffer.from(db.prepare('SELECT image FROM profile_avatars LIMIT 1').get().image),original);
    assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name GLOB 'world_direct_*' OR name GLOB 'profile_avatar_*'").get().n,0);}finally{db.close();}
});
test('reserved format4 attachment sentinel is rejected unchanged even with both migration flags', t => {
  const paths=temporaryStore(t);seedWorld5(paths.worldPath,legacyAccounts);const db=new DatabaseSync(paths.worldPath);
  db.exec("CREATE TABLE chat_attachments(id TEXT PRIMARY KEY,bytes BLOB);INSERT INTO chat_attachments VALUES('sentinel',x'010203');PRAGMA user_version=4;UPDATE world_meta SET value='4' WHERE key='schema_version'");db.close();
  const before=metadata(paths.worldPath),bytes=hash(readFileSync(paths.worldPath));
  assert.throws(()=>createWorldService({databasePath:paths.worldPath,projectId:PROJECT,allowAudienceMigration:true,allowWorld6Migration:true}),/world_schema_unsupported/u);
  assert.deepEqual(metadata(paths.worldPath),before);assert.equal(hash(readFileSync(paths.worldPath)),bytes);
});
test('committed World6 with a missing Direct guard or foreign avatar view refuses admission without repairing drift', async t => {
  for(const mutation of ["DROP TRIGGER world_direct_message_author", "CREATE TRIGGER foreign_direct_sideeffect AFTER INSERT ON world_direct_threads BEGIN SELECT 1;END", "CREATE TRIGGER foreign_avatar_sideeffect INSTEAD OF INSERT ON profile_avatars BEGIN SELECT 1;END", "DROP VIEW profile_avatars;CREATE VIEW profile_avatars AS SELECT account_id,'image/png' AS mime,x'00' AS image,'image/png' AS thumbnail_mime,x'00' AS thumbnail FROM profiles"]){
    const f=await signedFixture(t);f.connect.close();f.world.close();const db=new DatabaseSync(f.worldPath);db.exec(mutation);db.close();const before=metadata(f.worldPath);
    assert.throws(()=>createWorldService({databasePath:f.worldPath,projectId:PROJECT}),/world_direct_schema_mismatch|world_avatar_schema_mismatch/u);
    assert.deepEqual(metadata(f.worldPath),before);
  }
});
test('Direct polling is a real World read snapshot and does not require its writer lock', async t => {
  const f=await signedFixture(t),[a,b]=f.actors,rel=await f.contact(a,b),thread=good(await f.rpc(a,'world.direct.open',{
    expectedAccountId:a.accountId,peerAccountId:b.accountId,expectedRelationshipId:rel,requestId:'direct_read_fixture'})).current;
  const writer=new DatabaseSync(f.worldPath);writer.exec('BEGIN IMMEDIATE');
  try{
    const page=good(await f.rpc(a,'world.direct.chat.list',{expectedAccountId:a.accountId,threadId:thread.threadId,expectedRelationshipId:rel}));assert.equal(page.messages.length,0);
    assert.equal(good(await f.rpc(a,'world.direct.list',{expectedAccountId:a.accountId})).threads[0].threadId,thread.threadId);
  }finally{writer.exec('ROLLBACK');writer.close();}
});
test('legacy host method remains synchronous without upload capability and restarts through the common writer', async t => {
  const f=await signedFixture(t,{withDecoder:false}),a=f.actors[0],pixels=asDataUrl(pngFixture());
  const result=f.world.execute({op:'world.profile.avatar.set',actor:a,args:{expectedRevision:1,avatarUrl:pixels,thumbnailUrl:pixels}});
  assert.equal(typeof result.then,'undefined');assert.equal(result.profile.revision,2);assert.equal(result.profile.avatarWriteVersion,undefined);
  assert.throws(()=>f.world.execute({op:'world.profile.avatar.set',actor:{...a,label:undefined},args:{expectedRevision:2,avatarUrl:null,thumbnailUrl:null}}),{code:'invalid_text'});
  f.reopen();assert.equal(good(await f.rpc(a,'world.profile.avatar.read',{profileId:a.accountId})).avatarUrl,pixels);
});
