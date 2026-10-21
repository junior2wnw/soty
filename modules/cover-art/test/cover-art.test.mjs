import test from 'node:test';
import assert from 'node:assert/strict';
import { createCoverStore } from '../server/index.mjs';
import { canonicalizeCover,createCoverDecoder } from '../server/codec.mjs';
import { fixture,alice,bob,app1,app2,app3,image } from './support/fixture.mjs';
const denied=(fn,code)=>assert.throws(fn,error=>error?.code===code);

for(const format of [12,13,14])test(`upload roundtrip Apps${format}: prepare does not publish; commit/read/restart/restore/remove and replay retain the current head`,async t=>{
  const f=await fixture(t,{format}),source=await image(),args={...f.args(),mime:'image/png',bytes:source};
  const prepared=await f.store.prepareUpload(alice,args);assert.equal(prepared.draft.state,'ready');assert.equal(f.store.read(alice,{targetId:app1}).image,null);
  const repeatedPrepare=await f.store.prepareUpload(alice,args);assert.equal(repeatedPrepare.replayed,true);assert.equal(repeatedPrepare.draft.assetRef,prepared.draft.assetRef);
  const commitArgs={requestId:f.request(),targetId:app1,draftId:prepared.draft.id},first=f.store.commit(alice,commitArgs),firstImage=f.store.read(alice,{targetId:app1});
  assert.equal(first.current.revision,1);assert.equal(firstImage.image.mime,'image/webp');assert.ok(firstImage.image.bytes.length>0);
  const removed=f.store.remove(alice,{...f.args(),mode:'none'});assert.equal(removed.current.revision,2);assert.equal(f.store.read(alice,{targetId:app1}).image,null);
  const replay=f.store.commit(alice,commitArgs);assert.equal(replay.replayed,true);assert.equal(replay.receipt.committedRevision,1);assert.equal(replay.current.revision,2);assert.equal(replay.current.mode,'none');
  const restored=f.store.restore(alice,{...f.args(),versionId:first.receipt.versionId});assert.equal(restored.current.revision,3);assert.deepEqual(f.store.read(alice,{targetId:app1}).image.bytes,firstImage.image.bytes);
  f.reopen();assert.equal(f.store.read(alice,{targetId:app1}).current.revision,3);assert.ok(f.store.versions(alice,{targetId:app1}).versions.some(v=>v.id===first.receipt.versionId&&v.restorable));assert.equal(f.store.audit().ok,true);
});

test('fresh authority denies foreign owner, revoked device/app and current viewer after access removal',async t=>{
  const f=await fixture(t);await f.publish();
  await assert.rejects(f.store.prepareUpload(bob,{...f.args(),mime:'image/png',bytes:await image()}),error=>error.code==='cover_target_unavailable');
  denied(()=>f.store.read(bob,{targetId:app1}),'cover_target_unavailable');f.readers.add(`${bob.accountId}:${app1}`);assert.ok(f.store.read(bob,{targetId:app1}).image);denied(()=>f.store.versions(bob,{targetId:app1}),'cover_target_unavailable');
  f.readers.clear();denied(()=>f.store.read(bob,{targetId:app1}),'cover_target_unavailable');f.revoked.add(alice.deviceId);denied(()=>f.store.read(alice,{targetId:app1}),'device_revoked');
  f.revoked.clear();f.db.prepare("UPDATE local_apps SET state='revoked' WHERE id=?").run(app1);denied(()=>f.store.read(alice,{targetId:app1}),'cover_target_unavailable');
});

test('commit requires the prepared target/policy/cover/device tuple and cannot swap another own app draft',async t=>{
  const f=await fixture(t),prepared=await f.store.prepareUpload(alice,{...f.args(),mime:'image/png',bytes:await image()});
  denied(()=>f.store.commit(alice,{requestId:f.request(),targetId:app2,draftId:prepared.draft.id}),'cover_draft_unavailable');
  denied(()=>f.store.commit({...alice,deviceId:'device_other'},{requestId:f.request(),targetId:app1,draftId:prepared.draft.id}),'cover_draft_unavailable');
  f.db.prepare('UPDATE local_apps SET policy_epoch=2 WHERE id=?').run(app1);
  denied(()=>f.store.commit(alice,{requestId:f.request(),targetId:app1,draftId:prepared.draft.id}),'cover_target_changed');
  assert.equal(f.store.read(alice,{targetId:app1}).image,null);assert.equal(f.store.audit().ok,true);
});

test('an account/policy change while actual decode is held prevents the late result from becoming ready',async t=>{
  let release,entered;const held=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);
  const decoder=createCoverDecoder({decode:async input=>{entered();await held;return canonicalizeCover(input);}}),f=await fixture(t,{decoder});
  const pending=f.store.prepareUpload(alice,{...f.args(),mime:'image/png',bytes:await image()});await started;
  f.db.prepare('UPDATE local_apps SET target_revision=2,policy_epoch=2 WHERE id=?').run(app1);release();
  await assert.rejects(pending,error=>error.code==='cover_target_changed');assert.equal(f.store.read(alice,{targetId:app1}).image,null);assert.equal(f.store.audit().usage.reservedBytes,0);assert.equal(f.store.audit().usage.drafts,0);
});

test('same-key parallel prepare decodes once and a changed body conflicts without a second allocation',async t=>{
  let release,started,decodes=0;const hold=new Promise(resolve=>release=resolve),began=new Promise(resolve=>started=resolve);
  const f=await fixture(t,{decoder:createCoverDecoder({decode:async input=>{decodes++;started();await hold;return canonicalizeCover(input);}})}),args={...f.args(),mime:'image/png',bytes:await image()};
  const pending=f.store.prepareUpload(alice,args);await began;const second=await f.store.prepareUpload(alice,args);assert.equal(second.replayed,true);assert.equal(second.draft.state,'processing');
  await assert.rejects(f.store.prepareUpload(alice,{...args,crop:{x:.2,y:.5,zoom:1}}),error=>error.code==='cover_request_conflict');release();await pending;assert.equal(decodes,1);assert.equal(f.store.audit().usage.drafts,1);
});

test('cancelled in-flight preparation keeps its real decoder slot until completion and cannot publish a late image',async t=>{
  let release,started;const hold=new Promise(resolve=>release=resolve),began=new Promise(resolve=>started=resolve);
  const decoder=createCoverDecoder({decode:async input=>{started();await hold;return canonicalizeCover(input);}}),f=await fixture(t,{decoder});
  const args={...f.args(),mime:'image/png',bytes:await image()},pending=f.store.prepareUpload(alice,args);await began;
  const status=f.store.status(alice,{targetId:app1,requestId:args.requestId});f.store.cancel(alice,{requestId:f.request(),targetId:app1,draftId:status.draft.id});
  assert.equal(decoder.stats().active,1);assert.equal(f.store.audit().usage.reservedBytes,0);release();
  await assert.rejects(pending,error=>error.code==='cover_draft_expired');assert.equal(f.store.read(alice,{targetId:app1}).image,null);assert.equal(f.store.audit().ok,true);
});

test('failure inside the owning DB commit rolls back pointer, revision, receipt and quota changes',async t=>{
  let fail=false;const f=await fixture(t,{onCommitFailure:()=>fail});const first=await f.publish();
  const before=f.store.audit(),prepared=await f.store.prepareUpload(alice,{...f.args(),mime:'image/png',bytes:await image('#112233')}),args={requestId:f.request(),targetId:app1,draftId:prepared.draft.id};fail=true;
  assert.throws(()=>f.store.commit(alice,args),/fixture_commit_failure/u);assert.equal(f.store.read(alice,{targetId:app1}).current.revision,first.committed.current.revision);assert.equal(f.store.status(alice,{targetId:app1,requestId:args.requestId}).receipt,null);assert.equal(f.store.audit().ok,true);
  fail=false;assert.equal(f.store.commit(alice,args).current.revision,2);assert.ok(f.store.audit().usage.encodedBytes>=before.usage.encodedBytes);
});

test('draft owner/global limits, expiry and cleanup account for reservations exactly',async t=>{
  const f=await fixture(t,{limits:{ownerDrafts:2,targetDrafts:2,globalDrafts:2}}),bytes=await image();
  const one=await f.store.prepareUpload(alice,{...f.args(),bytes,mime:'image/png'});await f.store.prepareUpload(alice,{...f.args(app2),bytes,mime:'image/png'});
  await assert.rejects(f.store.prepareUpload(bob,{...f.args(app3),bytes,mime:'image/png'}),error=>error.code==='cover_draft_quota');
  f.advance(31*60_000);assert.equal(f.store.status(alice,{targetId:app1,draftId:one.draft.id}).draft.state,'expired');f.store.collect();assert.equal(f.store.audit().usage.drafts,0);
  await f.store.prepareUpload(bob,{...f.args(app3),bytes,mime:'image/png'});assert.equal(f.store.audit().ok,true);
});

test('encoded byte owner/global quota cannot delete the current/protected previous version to make room',async t=>{
  const reserve=470_000;const f=await fixture(t,{limits:{ownerBytes:reserve-1,globalBytes:reserve-1}});
  await assert.rejects(f.store.prepareUpload(alice,{...f.args(),mime:'image/png',bytes:await image()}),error=>error.code==='cover_storage_quota');assert.equal(f.store.audit().usage.encodedBytes,0);assert.equal(f.store.audit().usage.reservedBytes,0);
  const g=await fixture(t);const first=await g.publish();await g.publish({color:'#123456'});
  assert.throws(()=>g.db.prepare(`DELETE FROM ${g.p}_renditions WHERE asset_id=?`).run(first.committed.receipt.assetId),/cover_asset_live/u);
  assert.throws(()=>g.db.prepare(`UPDATE ${g.p}_versions SET purged_at=? WHERE id=?`).run(g.now,first.committed.receipt.versionId),/cover_version_protected/u);g.store.collect({pressure:true});assert.ok(g.store.read(alice,{targetId:app1,versionId:first.committed.receipt.versionId}).image);
});

test('bounded GC purges eligible old bytes but preserves active view, immutable outcomes and monotonic replay floor',async t=>{
  const f=await fixture(t),first=await f.publish();await f.publish({color:'#112233'});await f.publish({color:'#223344'});const latest=await f.publish({color:'#334455'});
  f.advance(2*86400_000);const result=f.store.collect({pressure:true});assert.ok(result.changed<=32);assert.ok(result.freedBytes<=4*1024*1024);assert.ok(result.freedBytes>0);
  denied(()=>f.store.read(alice,{targetId:app1,versionId:first.committed.receipt.versionId}),'cover_version_expired');assert.equal(f.store.commit(alice,first.commitArgs).current.revision,latest.committed.current.revision);
  f.advance(31*86400_000);for(let round=0;round<8;round++)f.store.collect({pressure:true});
  denied(()=>f.store.commit(alice,first.commitArgs),'cover_request_expired');const floor=f.db.prepare(`SELECT logical_now,minimum_request_time FROM ${f.p}_usage`).get();f.setClock(floor.logical_now-32*86400_000);
  denied(()=>f.store.commit(alice,first.commitArgs),'cover_request_expired');assert.equal(f.store.read(alice,{targetId:app1}).current.revision,latest.committed.current.revision);assert.equal(f.store.audit().ok,true);
});

test('accepted own version can be cropped again without generation; unavailable generation cannot pretend success',async t=>{
  const f=await fixture(t),first=await f.publish();const recrop=await f.store.prepareFromVersion(alice,{...f.args(),versionId:first.committed.receipt.versionId,crop:{x:.3,y:.5,zoom:2}});
  assert.equal(recrop.draft.state,'ready');assert.deepEqual(f.store.generationAvailability(),{available:false,reason:'cover_generation_provider_unavailable'});
  f.store.commit(alice,{requestId:f.request(),targetId:app1,draftId:recrop.draft.id});assert.equal(f.store.audit().ok,true);
});

test('retained recrop receipt replays after its source bytes are GC-purged with fresh authority and exact fingerprint',async t=>{
  let decodes=0;
  const f=await fixture(t,{decoder:createCoverDecoder({decode:input=>{decodes++;return canonicalizeCover(input);}})}),first=await f.publish();
  const args={...f.args(),versionId:first.committed.receipt.versionId,crop:{x:.3,y:.5,zoom:2}},prepared=await f.store.prepareFromVersion(alice,args);
  f.store.commit(alice,{requestId:f.request(),targetId:app1,draftId:prepared.draft.id});
  await f.publish({color:'#223344'});await f.publish({color:'#334455'});const latest=await f.publish({color:'#445566'});
  f.store.collect({pressure:true});
  const source=f.db.prepare(`SELECT purged_at FROM ${f.p}_assets WHERE id=?`).get(first.committed.receipt.assetId);
  assert.notEqual(source.purged_at,null);assert.equal(f.db.prepare(`SELECT count(*) AS n FROM ${f.p}_renditions WHERE asset_id=?`).get(first.committed.receipt.assetId).n,0);
  denied(()=>f.store.read(alice,{targetId:app1,versionId:args.versionId}),'cover_version_expired');
  f.reopen();const before=f.store.audit(),decodedBefore=decodes;
  const replay=await f.store.prepareFromVersion(alice,args);
  assert.equal(replay.replayed,true);assert.deepEqual(replay.receipt,prepared.receipt);assert.equal(replay.draft.state,'committed');assert.deepEqual(replay.current,latest.committed.current);
  assert.equal(decodes,decodedBefore);assert.deepEqual(f.store.audit(),before);
  await assert.rejects(f.store.prepareFromVersion(alice,{...args,crop:{...args.crop,x:.7}}),error=>error.code==='cover_request_conflict');
  await assert.rejects(f.store.prepareFromVersion(alice,{...args,versionId:latest.committed.receipt.versionId}),error=>error.code==='cover_request_conflict');
  await assert.rejects(f.store.prepareFromVersion(alice,{...args,expectedPolicyEpoch:args.expectedPolicyEpoch+1}),error=>error.code==='cover_request_conflict');
  await assert.rejects(f.store.prepareFromVersion(alice,{...f.args(),versionId:args.versionId,crop:args.crop}),error=>error.code==='cover_version_expired');
  await assert.rejects(f.store.prepareFromVersion({...alice,deviceId:'device_other'},args),error=>error.code==='cover_request_conflict');
  await assert.rejects(f.store.prepareFromVersion(bob,args),error=>error.code==='cover_target_unavailable');
  f.revoked.add(alice.deviceId);await assert.rejects(f.store.prepareFromVersion(alice,args),error=>error.code==='device_revoked');f.revoked.clear();
  f.db.prepare('UPDATE local_apps SET target_revision=2,policy_epoch=2 WHERE id=?').run(app1);
  assert.deepEqual((await f.store.prepareFromVersion(alice,args)).current,latest.committed.current);
  f.db.prepare("UPDATE local_apps SET state='revoked' WHERE id=?").run(app1);
  await assert.rejects(f.store.prepareFromVersion(alice,args),error=>error.code==='cover_target_unavailable');
});

test('previous choice undo lasts 24h from replacement even when its original creation is 31 days old',async t=>{
  const f=await fixture(t),first=await f.publish(),versionId=first.committed.receipt.versionId;
  const before=f.db.prepare(`SELECT created_at FROM ${f.p}_versions WHERE id=?`).get(versionId).created_at,bytes=f.store.read(alice,{targetId:app1}).image.bytes;
  f.advance(31*86400_000);const replacedAt=f.now,second=await f.publish({color:'#123456'});
  const h=f.db.prepare(`SELECT * FROM ${f.p}_heads WHERE target_id=?`).get(app1);
  assert.equal(h.previous_version_id,versionId);assert.equal(h.previous_replaced_at,replacedAt);
  const original=f.db.prepare(`SELECT created_at,purged_at FROM ${f.p}_versions WHERE id=?`).get(versionId);
  assert.ok(original,'previous version metadata must survive immediate replacement GC');assert.equal(original.purged_at,null);assert.equal(original.created_at,before);
  assert.throws(()=>f.db.prepare(`UPDATE ${f.p}_versions SET purged_at=? WHERE id=?`).run(f.now,versionId),/cover_version_protected/u);
  assert.throws(()=>f.db.prepare(`DELETE FROM ${f.p}_versions WHERE id=?`).run(versionId),/cover_version_protected/u);
  f.store.collect({pressure:true});assert.deepEqual(f.store.read(alice,{targetId:app1,versionId}).image.bytes,bytes);
  f.advance(86400_000-1);f.store.collect({pressure:true});f.reopen();assert.deepEqual(f.store.read(alice,{targetId:app1,versionId}).image.bytes,bytes);
  const restored=f.store.restore(alice,{...f.args(),versionId});assert.deepEqual(f.store.read(alice,{targetId:app1}).image.bytes,bytes);assert.equal(restored.current.revision,3);
  f.store.restore(alice,{...f.args(),versionId:second.committed.receipt.versionId});
  f.advance(86400_000+1);f.store.collect({pressure:true});
  denied(()=>f.store.read(alice,{targetId:app1,versionId}),'cover_version_expired');assert.equal(f.store.audit().ok,true);
  // Once the latest previous pointer ages out, its metadata may be reclaimed;
  // that tombstone must not force infinite retention or break startup audit.
  f.advance(31*86400_000);for(let round=0;round<8;round++)f.store.collect({pressure:true});f.reopen();assert.equal(f.store.audit().ok,true);
});

test('old markers and missing/tampered DDL are refused without creating tables or rewriting format',async t=>{
  const f=await fixture(t);const before=f.db.prepare('SELECT count(*) AS n FROM sqlite_schema').get().n;f.db.exec('PRAGMA user_version=11');
  denied(()=>createCoverStore(f.ports()),'cover_schema_required');assert.equal(f.db.prepare('SELECT count(*) AS n FROM sqlite_schema').get().n,before);f.db.exec('PRAGMA user_version=14');
  f.db.exec(`DROP TRIGGER ${f.p}_receipt_immutable`);denied(()=>createCoverStore(f.ports()),'cover_schema_metadata_mismatch');
});

test('receipt/head replacement, premature receipt deletion and cross-owner asset rows cannot bypass durable lineage',async t=>{
  const f=await fixture(t),first=await f.publish();
  assert.throws(()=>f.db.prepare(`DELETE FROM ${f.p}_receipts WHERE request_key=?`).run(first.commitArgs.requestId),/cover_retention_immutable/u);
  const receipt=f.db.prepare(`SELECT * FROM ${f.p}_receipts WHERE request_key=?`).get(first.commitArgs.requestId);
  assert.throws(()=>f.db.prepare(`INSERT OR REPLACE INTO ${f.p}_receipts VALUES(?,?,?,?,?,?,?,?)`).run(receipt.owner_id,receipt.request_key,receipt.intent_hash,receipt.action,receipt.target_id,receipt.value_json,receipt.created_at,receipt.expires_at),/cover_receipt_immutable/u);
  assert.throws(()=>f.db.prepare(`DELETE FROM ${f.p}_heads`).run(),/cover_head_lineage/u);
  assert.throws(()=>f.db.prepare(`INSERT INTO ${f.p}_assets VALUES('asset_foreign',?,?,'upload',1,1,'fixture',1,?,NULL)`).run(app1,bob.accountId,f.now),/FOREIGN KEY/u);
  f.db.prepare(`UPDATE ${f.p}_usage SET encoded_bytes=encoded_bytes+1`).run();denied(()=>createCoverStore(f.ports()),'cover_usage_mismatch');
});
