import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtemp,mkdir,readFile,rm} from 'node:fs/promises';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {migrateAppsSchema,ensureCanonicalDomain,ensureInitialPublication} from '../../modules/apps/server/schema.mjs';
import * as frame from '../../modules/apps/server/frame-continuity.mjs';
import {createHostSessionContinuity} from '../../modules/apps/server/host-session-continuity.mjs';
import {canonical,hash} from '../../modules/apps/scoped-embed/profile.mjs';
import {readStorageFormat} from './storage-probe.mjs';
import {assertStorageCompatible,currentStorageReaders,storageReaderLabel} from './storage-guard.mjs';

const appId='app-'+'a'.repeat(32),owner='fixture_owner',deviceId='fixture_device';
const image=readers=>({Id:'sha256:'+'a'.repeat(64),Config:{Labels:{[storageReaderLabel]:JSON.stringify(readers)}}});
async function fixture(t){
  const base=path.resolve(tmpdir()),root=await mkdtemp(path.join(base,'soty-host-storage-'));
  await mkdir(path.join(root,'apps'));const file=path.join(root,'apps/registry.sqlite'),db=new DatabaseSync(file);
  t.after(async()=>{db.close();assert.equal(path.dirname(path.resolve(root)),base);assert.ok(path.basename(root).startsWith('soty-host-storage-'));await rm(root,{recursive:true,force:true});});
  migrateAppsSchema(db,{legacyTemplate:'http://{appId}.localhost'});
  const connector='link|host|connector';
  db.exec('BEGIN IMMEDIATE');
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connector,owner,JSON.stringify({linkId:'link',hostDeviceId:'host',connectorId:'connector'}),'Fixture',1);
  db.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(appId,owner,connector,'Fixture',30000,'/',JSON.stringify({accountIds:[],communityIds:[]}),'enabled',1,1,1);
  const app=db.prepare('SELECT * FROM local_apps WHERE id=?').get(appId);ensureCanonicalDomain(db,app,'http://{appId}.localhost');ensureInitialPublication(db,app);db.exec('COMMIT');
  const entry=db.prepare('SELECT id,origin FROM app_domains WHERE app_id=?').get(appId);
  const scope={schema:'soty.app-host-scope.v1',accountId:owner,deviceId,appId,policyEpoch:1,
    entry:{domainId:entry.id,origin:entry.origin,pathDigest:hash('/ordinary#synthetic-private-fragment')},
    target:{revision:1,digest:'b'.repeat(64),profile:'soty.relay-restricted.v1',bindingFloor:1},
    instanceId:'main',runtimeOrigin:'http://app-m'+'a'.repeat(32)+'-'+'e'.repeat(24)+'.localhost',runtimeScopeDigest:'e'.repeat(64)};
  return{root,file,db,entry,scope,store:()=>createHostSessionContinuity({db})};
}
function nativeRow(f){
  const record={handleHash:'c'.repeat(64),context:{appId,rootPrincipal:{accountId:owner,deviceId},reference:{id:'F'.repeat(43)},
    sourceProfile:{id:'soty.standard-workspace',version:2,digest:'23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9'},
    profileDigest:'d'.repeat(64),policyEpoch:1,target:{revision:1,digest:'b'.repeat(64)},entry:{domainId:f.entry.id,origin:f.entry.origin},
    resource:{id:'fixture'},humanPrincipal:{issuer:'https://root.test',subject:owner}}};
  frame.createFrameContinuity({db:f.db}).attach(record,{family:{frameId:record.context.reference.id},entryPath:'/embed#synthetic-private-fragment'});
}
test('ordinary tagged lease is readable by the maintained independent storage probe and current declared image',async t=>{
  const f=await fixture(t);f.store().create(f.scope);const before=await readFile(f.file),format=await readStorageFormat(f.root);
  assert.equal(format.appsFrameContinuity,2);assert.deepEqual(await readFile(f.file),before,'Probe must stay read-only');
  assertStorageCompatible(image(JSON.parse(currentStorageReaders)),format);
  const previous=JSON.parse(currentStorageReaders);previous.readers.appsFrameContinuity=[1];
  assert.throws(()=>assertStorageCompatible(image(previous),format),{code:'storage_reader_incompatible'},'A literal v1-only image cannot start on new metadata');
  const row=JSON.stringify(f.db.prepare('SELECT * FROM app_frame_continuity').get());assert.ok(!row.includes('synthetic-private-fragment'),'Only path digest is durable');
});
test('legacy v1 Native rows remain readable and do not upgrade without existing migration permission',async t=>{
  const f=await fixture(t);f.db.prepare("UPDATE apps_meta SET value=? WHERE key='frame_continuity'").run('soty.apps-frame-continuity.v1');nativeRow(f);
  const before=JSON.stringify(f.db.prepare('SELECT * FROM app_frame_continuity').all());
  migrateAppsSchema(f.db,{legacyTemplate:'http://{appId}.localhost'});
  assert.equal(f.db.prepare("SELECT value FROM apps_meta WHERE key='frame_continuity'").get().value,'soty.apps-frame-continuity.v1');
  assert.equal(frame.hasFrameContinuitySchema(f.db),true);assert.equal(frame.hasHostSessionContinuitySchema(f.db),false);
  assert.throws(()=>f.store(),{code:'app_host_session_unavailable'});
  assert.equal((await readStorageFormat(f.root)).appsFrameContinuity,1);assert.equal(JSON.stringify(f.db.prepare('SELECT * FROM app_frame_continuity').all()),before);
});
test('explicit v1→v2 migration preserves every original Native row byte and admits tagged leases',async t=>{
  const f=await fixture(t);f.db.prepare("UPDATE apps_meta SET value=? WHERE key='frame_continuity'").run('soty.apps-frame-continuity.v1');nativeRow(f);
  const before=JSON.stringify(f.db.prepare('SELECT * FROM app_frame_continuity').all());
  migrateAppsSchema(f.db,{legacyTemplate:'http://{appId}.localhost',allowFrameContinuityMigration:true});
  assert.equal(frame.hasHostSessionContinuitySchema(f.db),true);assert.equal(JSON.stringify(f.db.prepare('SELECT * FROM app_frame_continuity').all()),before);
  f.store().create(f.scope);assert.equal((await readStorageFormat(f.root)).appsFrameContinuity,2);
});
test('new tagged rows mislabeled as v1 are rejected instead of silently blessed',async t=>{
  const f=await fixture(t);f.store().create(f.scope);f.db.prepare("UPDATE apps_meta SET value=? WHERE key='frame_continuity'").run('soty.apps-frame-continuity.v1');
  await assert.rejects(readStorageFormat(f.root),{code:'storage_format_unreadable'});
  assert.throws(()=>migrateAppsSchema(f.db,{legacyTemplate:'http://{appId}.localhost',allowFrameContinuityMigration:true}),{code:'apps_schema_unsupported'});
  assert.equal(f.db.prepare("SELECT value FROM apps_meta WHERE key='frame_continuity'").get().value,'soty.apps-frame-continuity.v1');
});
test('migration refuses a SQL-valid malformed legacy envelope without changing its marker or bytes',async t=>{
  const f=await fixture(t);f.db.prepare("UPDATE apps_meta SET value=? WHERE key='frame_continuity'").run('soty.apps-frame-continuity.v1');nativeRow(f);
  f.db.prepare('UPDATE app_frame_continuity SET lease_hash=?').run('x'.repeat(64));
  const before=JSON.stringify(f.db.prepare('SELECT * FROM app_frame_continuity').all());
  assert.throws(()=>migrateAppsSchema(f.db,{legacyTemplate:'http://{appId}.localhost',allowFrameContinuityMigration:true}),{code:'apps_schema_unsupported'});
  assert.equal(f.db.prepare("SELECT value FROM apps_meta WHERE key='frame_continuity'").get().value,'soty.apps-frame-continuity.v1');
  assert.equal(JSON.stringify(f.db.prepare('SELECT * FROM app_frame_continuity').all()),before);
});
for(const[name,mutate]of[
  ['raw path',m=>{m.scope.entry.path='/#synthetic-private-fragment';}],
  ['extra credential field',m=>{m.cookie='synthetic-private-fragment';}],
  ['wrong owner',m=>{m.scope.accountId='other_owner';}],
  ['unknown lifecycle',m=>{m.lifecycle='source-ready';}],
  ['wrong origin namespace',m=>{m.scope.runtimeOrigin='https://wrong.test';}],
  ['unknown profile',m=>{m.scope.target.profile='unknown-profile';}],
])test('independent v2 probe rejects correctly rehashed '+name,async t=>{
  const f=await fixture(t);f.store().create(f.scope);const row=f.db.prepare('SELECT * FROM app_frame_continuity').get(),meta=JSON.parse(row.binding_json);mutate(meta);meta.family=hash(meta.scope);
  f.db.prepare('UPDATE app_frame_continuity SET binding_json=?,binding_digest=? WHERE frame_id=?').run(canonical(meta),hash(meta),row.frame_id);
  await assert.rejects(readStorageFormat(f.root),{code:'storage_format_unreadable'});
});
