import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,mkdir,readFile,writeFile,rm,symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join,dirname,basename,resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { migrateWorld as actualWorld6 } from '../../modules/world/server/schema.mjs';
import { migrateWorld as genuineWorld3 } from '../../modules/world/test/support/legacy-world-schema-v3.mjs';
import { migrateWorld as genuineWorld5 } from '../../modules/world/test/support/world5-schema-before.mjs';
import { migrateField } from '../../modules/world/server/field.mjs';
import { migrateAppsSchema } from '../../modules/apps/server/schema.mjs';
import { readStorageFormat,readWorldFormat } from './storage-probe.mjs';
import { snapshotStorage } from './storage-snapshot.mjs';
import { storageReaders,storageReaderLabel,currentStorageReaders,checkedStorageFormat,assertStorageCompatible,
  guardStorageStart,requireStorageStartReceipt } from './storage-guard.mjs';

const id=char=>char.repeat(64), imageId='sha256:'+id('a'), containerId=id('b');
const OLD6='{"version":6,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6,7,8,9,10,11],"notes":[1,2],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1],"humanIdentity":[1,2,3],"connect":[3,4]}}';
const image=(label=currentStorageReaders)=>({Id:imageId,Config:{Labels:{[storageReaderLabel]:label}}});
const empty7=(extra={})=>({ok:true,schema:'soty.storage-format.v7',rooms:'empty',apps:'empty',notes:'empty',capabilities:'empty',
  appRegistration:'empty',feedback:'empty',humanIdentity:'empty',connect:'empty',world:'empty',...extra});
const digest=bytes=>createHash('sha256').update(bytes).digest('hex');
async function folder(t) {
  const root=await mkdtemp(join(tmpdir(),'soty-storage-world7-')),closed=[];
  t.after(async()=>{for(const db of closed)db.close();const absolute=resolve(root);assert.equal(dirname(absolute),resolve(tmpdir()));
    assert.match(basename(absolute),/^soty-storage-world7-/u);await rm(absolute,{recursive:true,force:true});});
  return {root,hold(db){closed.push(db);return db;}};
}
async function worldStore(t,{version=6,field=true,wal=false}={}) {
  const f=await folder(t);await mkdir(join(f.root,'world'));const file=join(f.root,'world','world.sqlite'),db=f.hold(new DatabaseSync(file));
  const migrator=version===3?genuineWorld3:version===5?genuineWorld5:actualWorld6;
  migrator(db,'soty');if(field)migrateField(db);
  if(wal)db.exec('PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;');
  return {...f,file,db};
}
async function appStore(t,base=6) {
  const f=await folder(t);await mkdir(join(f.root,'apps'));const file=join(f.root,'apps','registry.sqlite'),db=f.hold(new DatabaseSync(file));
  db.exec('PRAGMA foreign_keys=ON');migrateAppsSchema(db,{allowCoverMigration:true,allowScopedEmbedMigration:base>=7,allowSelectedResourceMigration:base>=8});
  assert.equal(db.prepare('PRAGMA user_version').get().user_version,base+6,'test requires the integrated actual Apps migrator');
  return {...f,file,db};
}
function legacy(version) {
  const label=JSON.parse(OLD6);
  if(version<6)delete label.readers.connect;
  if(version<5)delete label.readers.humanIdentity;
  if(version<4){delete label.readers.appRegistration;delete label.readers.feedback;}
  label.version=version;
  const format={ok:true,schema:'soty.storage-format.v'+version,...Object.fromEntries(Object.keys(label.readers).map(key=>[key,'empty']))};
  return {label:JSON.stringify(label),format};
}
function startFixture(root,label=currentStorageReaders,probe=()=>readStorageFormat(root)) {
  const events=[],runtime={Id:containerId,Image:imageId,State:{Running:false},Config:{Env:['DATA_DIR=/data']},
    Mounts:[{Type:'volume',Name:'synthetic-world7',Source:'/docker/volumes/synthetic-world7/_data',Destination:'/data',RW:true}]};
  const engine={
    async inspect(){return structuredClone(runtime);},async image(){return image(label);},
    async request(method,path){events.push(method+' '+path);if(path==='/containers/json')return [];
      if(path==='/volumes/synthetic-world7')return {Name:'synthetic-world7',Driver:'local',Scope:'local',Options:null,Mountpoint:runtime.Mounts[0].Source};throw Error('unexpected synthetic engine request');},
    async start(){events.push('START');runtime.State.Running=true;},
  };
  return {events,async start(){const receipt=await guardStorageStart({engine,probe},runtime);requireStorageStartReceipt(receipt,runtime.Id);
    await engine.start(runtime.Id);return receipt;}};
}

test('v7 explicit empty is admissible and historical3..6 labels, formats and receipts keep their old key semantics',async t=>{
  const f=await folder(t),observed=await readStorageFormat(f.root);assert.deepEqual(observed,empty7());
  assert.deepEqual(assertStorageCompatible(image(OLD6),observed),observed);
  const current=storageReaders(image());assert.deepEqual(current.world,[6,7]);assert.deepEqual(current.apps,[1,2,3,4,5,6,7,8,9,10,11,12,13,14]);
  for(const version of [3,4,5,6]){
    const old=legacy(version),parsed=checkedStorageFormat(old.format);assert.deepEqual(parsed,old.format);storageReaders(image(old.label));
    assertStorageCompatible(image(old.label),old.format);
    const {ok,schema,...stores}=parsed,receipt={schema:'soty.storage-start.v'+version,containerId,image:imageId,mountSha256:id('c'),...stores};
    requireStorageStartReceipt(receipt,containerId);
    assert.throws(()=>requireStorageStartReceipt({...receipt,world:'empty'},containerId),{code:'storage_start_guard_missing'});
    assert.throws(()=>checkedStorageFormat({...old.format,world:'empty'}),{code:'storage_probe_invalid'});
    const upgradedClaim=JSON.parse(old.label);upgradedClaim.readers.apps=[12];assert.throws(()=>storageReaders(image(JSON.stringify(upgradedClaim))),{code:'storage_reader_unknown'});
    assert.throws(()=>checkedStorageFormat({...old.format,apps:12}),{code:'storage_probe_invalid'});
  }
  for(const world of [3,5,6]){
    checkedStorageFormat(empty7({world}));assert.throws(()=>assertStorageCompatible(image(OLD6),empty7({world})),{code:'storage_reader_incompatible'});
  }
  for(const world of [3,5])assert.throws(()=>assertStorageCompatible(image(),empty7({world})),{code:'storage_reader_incompatible'});
  assert.throws(()=>checkedStorageFormat(empty7({world:4})),{code:'storage_probe_invalid'});
  const unknown=JSON.parse(currentStorageReaders);unknown.readers.world=[4];assert.throws(()=>storageReaders(image(JSON.stringify(unknown))),{code:'storage_reader_unknown'});
});

for(const version of [3,5])for(const field of [false,true])test(`genuine World${version} field${+field} is recognized independently and current World6 image refuses it unchanged`,async t=>{
  const f=await worldStore(t,{version,field}),before=digest(await readFile(f.file));
  assert.equal(await readWorldFormat(f.root),version);assert.deepEqual(await readStorageFormat(f.root),empty7({world:version}));
  assert.throws(()=>assertStorageCompatible(image(),empty7({world:version})),{code:'storage_reader_incompatible'});
  assert.equal(digest(await readFile(f.file)),before);
});
for(const field of [false,true])test(`actual World6 migrator field${+field} matches literal exact independent layout and stable store metadata`,async t=>{
  const f=await worldStore(t,{field});assert.equal(await readWorldFormat(f.root),6);
  assert.deepEqual(assertStorageCompatible(image(),await readStorageFormat(f.root)),empty7({world:6}));
  assert.match(f.db.prepare('SELECT store_id FROM profile_avatar_usage').get().store_id,/^world_profile_[a-f0-9]{48}$/u);
});

test('actual populated legacy avatar migration is recognized with exact bytes and rejects mismatched persistent usage',async t=>{
  const f=await worldStore(t,{version:3}),png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNwCU37DwAD3wH/3cflkQAAAABJRU5ErkJggg==','base64');
  // This is a synthetic one-pixel PNG, not any user's media.
  f.db.prepare('INSERT INTO profiles(account_id,display_name,search_text,revision,avatar_revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?)').run('synthetic_avatar_owner','Synthetic owner','synthetic owner',7,7,1,1);
  f.db.prepare('INSERT INTO profile_avatars VALUES(?,?,?,?,?)').run('synthetic_avatar_owner','image/png',png,'image/png',png);
  actualWorld6(f.db,'soty',{allowAudienceMigration:true,allowWorld6Migration:true,now:1800000000000});
  assert.equal(await readWorldFormat(f.root),6);assert.equal(f.db.prepare('SELECT origin FROM profile_avatar_assets').get().origin,'legacy');
  assert.deepEqual(Buffer.from(f.db.prepare('SELECT image FROM profile_avatars').get().image),png);
  assert.equal(f.db.prepare('SELECT encoded_bytes FROM profile_avatar_usage').get().encoded_bytes,2*png.length);
  f.db.exec('UPDATE profile_avatar_usage SET encoded_bytes=encoded_bytes+1');
  const before=digest(await readFile(f.file));await assert.rejects(readWorldFormat(f.root),{code:'storage_format_unreadable'});assert.equal(digest(await readFile(f.file)),before);
});

test('World4 attachment sentinel and forged6 marker on actual5 are rejected without rewriting original bytes',async t=>{
  const old=await worldStore(t,{version:3});old.db.exec("CREATE TABLE chat_attachments(id TEXT PRIMARY KEY,bytes BLOB);INSERT INTO chat_attachments VALUES('sentinel',x'010203');PRAGMA user_version=4;UPDATE world_meta SET value='4' WHERE key='schema_version'");
  const before=digest(await readFile(old.file));await assert.rejects(readWorldFormat(old.root),{code:'storage_format_unknown'});assert.equal(digest(await readFile(old.file)),before);
  const fake=await worldStore(t,{version:5});fake.db.exec("PRAGMA user_version=6;UPDATE world_meta SET value='6' WHERE key='schema_version'");
  await assert.rejects(readWorldFormat(fake.root),{code:'storage_format_unreadable'});
});

test('World6 literal inventory refuses changed table, missing index, missing trigger, changed view and foreign attached effect',async t=>{
  const mutations=[
    'ALTER TABLE world_direct_threads ADD COLUMN hidden TEXT',
    'DROP INDEX world_direct_high_seq',
    'DROP TRIGGER profile_avatar_version_undo_delete',
    "DROP VIEW profile_avatars;CREATE VIEW profile_avatars AS SELECT account_id,'image/png' AS mime,x'00' AS image,'image/png' AS thumbnail_mime,x'00' AS thumbnail FROM profiles",
    'CREATE TRIGGER outsider_effect AFTER INSERT ON world_direct_threads BEGIN SELECT 1;END',
  ];
  for(const mutation of mutations){const f=await worldStore(t);f.db.exec(mutation);const before=digest(await readFile(f.file));
    await assert.rejects(readWorldFormat(f.root),{code:'storage_format_unreadable'});assert.equal(digest(await readFile(f.file)),before);}
});

test('World missing and actual empty directory are empty, while orphan journals, unknown files, zero DB and directory links refuse',async t=>{
  const absent=await folder(t);assert.equal(await readWorldFormat(absent.root),'empty');await mkdir(join(absent.root,'world'));assert.equal(await readWorldFormat(absent.root),'empty');
  for(const name of ['world.sqlite-wal','backup.sqlite','world.sqlite']){const f=await folder(t);await mkdir(join(f.root,'world'));await writeFile(join(f.root,'world',name),'');await assert.rejects(readWorldFormat(f.root),{code:'storage_format_unreadable'});}
  const link=await folder(t),target=join(link.root,'target');await mkdir(target);await symlink(target,join(link.root,'world'),process.platform==='win32'?'junction':'dir');
  await assert.rejects(readWorldFormat(link.root),{code:'storage_format_unreadable'});
});

for(const base of [6,7,8])test(`actual Apps${base+6} migrator matches independent cover inventory while preserving base${base}`,async t=>{
  const f=await appStore(t,base),observed=await readStorageFormat(f.root);assert.equal(observed.apps,base+6);assert.equal(observed.world,'empty');
  assert.equal(assertStorageCompatible(image(),observed).apps,base+6);
  assert.throws(()=>assertStorageCompatible(image(OLD6),observed),{code:'storage_reader_incompatible'});
});

test('Apps cover reader refuses fake14 marker and missing cover guard including committed WAL',async t=>{
  const f=await appStore(t,8);f.db.exec('PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;DROP TRIGGER app_cover_version_undo_delete');
  await assert.rejects(readStorageFormat(f.root),{code:'storage_format_unreadable'});
  const g=await folder(t);await mkdir(join(g.root,'apps'));const db=g.hold(new DatabaseSync(join(g.root,'apps','registry.sqlite')));
  migrateAppsSchema(db,{allowAudienceMigration:true,allowSelectedResourceMigration:true});db.exec("UPDATE apps_meta SET value='soty.apps-registry.v14' WHERE key='schema';PRAGMA user_version=14");
  await assert.rejects(readStorageFormat(g.root),{code:'storage_format_unreadable'});
});

test('World7 preSTART rejects an image without World declaration before START on a real World6 store',async t=>{
  const f=await worldStore(t);assert.equal((await readStorageFormat(f.root)).world,6);
  const gate=startFixture(f.root,OLD6);await assert.rejects(gate.start(),{code:'storage_reader_incompatible'});assert.equal(gate.events.includes('START'),false);
});
test('World7 preSTART refuses old observations, current World3/5, and admits explicit empty or supported6 only',async t=>{
  const empty=await folder(t),oldObservation=startFixture(empty.root,currentStorageReaders,async()=>legacy(6).format);
  await assert.rejects(oldObservation.start(),{code:'storage_probe_invalid'});assert.equal(oldObservation.events.includes('START'),false);
  for(const version of [3,5]){const f=await worldStore(t,{version}),gate=startFixture(f.root);await assert.rejects(gate.start(),{code:'storage_reader_incompatible'});assert.equal(gate.events.includes('START'),false);}
  const emptyGate=startFixture(empty.root,OLD6);assert.equal((await emptyGate.start()).schema,'soty.storage-start.v7');assert.equal(emptyGate.events.filter(v=>v==='START').length,1);
  const f=await worldStore(t),good=startFixture(f.root);const receipt=await good.start();assert.equal(receipt.world,6);assert.equal(receipt.schema,'soty.storage-start.v7');
});

test('offline snapshot retains World WAL and never hides a committed reserved4 marker as empty or old main6',async t=>{
  const f=await worldStore(t,{wal:true}),target=join((await folder(t)).root,'snapshot');
  f.db.exec("BEGIN IMMEDIATE;PRAGMA user_version=4;UPDATE world_meta SET value='4' WHERE key='schema_version';COMMIT;");
  const dbBefore=digest(await readFile(f.file)),walBefore=digest(await readFile(f.file+'-wal'));
  await snapshotStorage(f.root,target);assert.equal(digest(await readFile(join(target,'world','world.sqlite'))),dbBefore);
  assert.equal(digest(await readFile(join(target,'world','world.sqlite-wal'))),walBefore);
  await assert.rejects(readWorldFormat(target),{code:'storage_format_unknown'});
  assert.equal(digest(await readFile(f.file)),dbBefore);assert.equal(digest(await readFile(f.file+'-wal')),walBefore);
});

test('probe is standalone Node-only source and Root Docker label exactly matches current reader declaration',async t=>{
  const f=await worldStore(t),source=await readFile(new URL('./storage-probe.mjs',import.meta.url),'utf8');
  const imports=[...source.matchAll(/from\s+['"]([^'"]+)['"]/gu)].map(match=>match[1]);assert.ok(imports.length>=4);assert.ok(imports.every(value=>value.startsWith('node:')));
  const code=source+'\nprocess.stdout.write(JSON.stringify(await readStorageFormat('+JSON.stringify(f.root)+')));';
  const env=Object.fromEntries(Object.entries({SOTY_STORAGE_PROBE:'0',SystemRoot:process.env.SystemRoot}).filter(([,value])=>typeof value==='string'));
  const child=spawnSync(process.execPath,['--input-type=module','-'],{cwd:f.root,encoding:'utf8',env,input:code,maxBuffer:1024*1024,timeout:10000,windowsHide:true});
  assert.equal(child.error,undefined,'standalone spawn error: '+String(child.error?.code??''));
  assert.equal(child.signal,null,'standalone child terminated: '+String(child.signal??''));
  assert.equal(child.status,0,child.stderr);assert.equal(JSON.parse(child.stdout).world,6);
  const docker=await readFile(new URL('../../Dockerfile',import.meta.url),'utf8');
  const label=/^LABEL io\.soty\.storage\.readers=("(?:\\.|[^"\\])*")$/mu.exec(docker);assert.ok(label);assert.equal(JSON.parse(label[1]),currentStorageReaders);
});


// Independent storage-v7 Source-review regressions: actual migrators/reader, synthetic TEMP stores.
function independentWeakenNotNull(db, table, from, to) {
  const original = db.prepare("SELECT sql FROM sqlite_schema WHERE type='table' AND name=?").get(table).sql;
  assert.ok(original.includes(from)); const changed = original.replace(from, to); assert.notEqual(changed, original);
  const version = db.prepare('PRAGMA schema_version').get().schema_version;
  // Synthetic corruption fixture only; Node24 defaults to defensive SQLite.
  db.enableDefensive(false);
  db.exec('PRAGMA writable_schema=ON');
  try { db.prepare("UPDATE sqlite_schema SET sql=? WHERE type='table' AND name=?").run(changed, table); }
  finally { db.exec('PRAGMA writable_schema=OFF'); }
  db.exec('PRAGMA schema_version=' + (version + 1));
  assert.equal(db.prepare('PRAGMA quick_check').get().quick_check, 'ok');
}
test('independent frozen DDL: actual World6 refuses token-merged removal of NOT NULL', async t => {
  const f = await worldStore(t, { version: 6, field: true }); assert.equal(await readWorldFormat(f.root), 6);
  independentWeakenNotNull(f.db, 'profiles', 'display_name TEXT NOT NULL', 'display_name TEXTNOTNULL');
  const name = f.db.prepare('PRAGMA table_info(profiles)').all().find(value => value.name === 'display_name');
  assert.equal(name.type, 'TEXTNOTNULL'); assert.equal(name.notnull, 0, 'the constraint really disappeared');
  await assert.rejects(readWorldFormat(f.root), { code: 'storage_format_unreadable' });
});
test('independent frozen DDL: actual Apps14 refuses token-merged removal of NOT NULL', async t => {
  const f = await appStore(t, 8); assert.equal((await readStorageFormat(f.root)).apps, 14);
  independentWeakenNotNull(f.db, 'app_cover_assets', 'recipe TEXT NOT NULL', 'recipe TEXTNOTNULL');
  const recipe = f.db.prepare('PRAGMA table_info(app_cover_assets)').all().find(value => value.name === 'recipe');
  assert.equal(recipe.type, 'TEXTNOTNULL'); assert.equal(recipe.notnull, 0, 'the constraint really disappeared');
  await assert.rejects(readStorageFormat(f.root), { code: 'storage_format_unreadable' });
});
