import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { migrateAppsSchema, inspectAppsSchema, ensureCanonicalDomain, ensureInitialPublication,
  runtimeTargetDigest, discussionAudienceDigest, RUNTIME_PROFILE, SCOPED_RUNTIME_PROFILE, RESOURCE_RUNTIME_PROFILE } from '../server/schema.mjs';
import { createAppsService } from '../server/index.mjs';
import { createScopedAdmissionRegistry } from '../scoped-embed/admissions.mjs';
import { createCoverDecoder } from '../../cover-art/server/codec.mjs';
import { readStorageFormat } from '../../../deploy/connector/storage-probe.mjs';

const LEGACY='https://{appId}.legacy.example', OWNER='synthetic-owner', APP='app-'+'a'.repeat(32), CONNECTOR='link|host|connector';
const GRANTS={accountIds:['synthetic-reader'],communityIds:['synthetic-group']};
const RAW='{"literal":"\\u0430","order":1}', CONVERSATION='conv_'+'b'.repeat(32), MESSAGE='msg_'+'c'.repeat(32);
const flags=base=>({allowScopedEmbedMigration:base>=7,allowSelectedResourceMigration:base>=8});
const change=base=>base===7?{allowScopedEmbedMigration:true}:{allowSelectedResourceMigration:true};
const baseOf=version=>version>=12?version-6:version-3;
function location(t) {
  const root=mkdtempSync(join(tmpdir(),'soty-layered-base-'));mkdirSync(join(root,'apps'));const closers=[];
  t.after(()=>{for(const close of closers.reverse())close();rmSync(root,{recursive:true,force:true});});
  return {root,file:join(root,'apps','registry.sqlite'),own:close=>closers.push(close)};
}
function readRows(db,name) {
  const rows=db.prepare(`SELECT * FROM "${name}" ORDER BY rowid`).all();
  return JSON.stringify(rows.map(row=>Object.fromEntries(Object.entries(row).map(([k,v])=>
    [k,ArrayBuffer.isView(v)?{base64:Buffer.from(v).toString('base64')}:v]))));
}
function snapshot(db) {
  const tables=db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name").all().map(x=>x.name);
  return {schema:db.prepare('SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name').all(),
    rows:Object.fromEntries(tables.map(name=>[name,readRows(db,name)])),
    userVersion:db.prepare('PRAGMA user_version').get().user_version,
    foreignKeys:db.prepare('PRAGMA foreign_keys').get().foreign_keys};
}
function preserved(db,before) {
  for(const [name,bytes] of Object.entries(before.rows))if(name!=='apps_meta')assert.equal(readRows(db,name),bytes,name+' rows/receipts/BLOBs must be byte-identical');
  const oldMeta=JSON.parse(before.rows.apps_meta).filter(x=>x.key!=='schema');
  assert.deepEqual(db.prepare("SELECT * FROM apps_meta WHERE key<>'schema' ORDER BY rowid").all().map(x=>({...x})),oldMeta);
  // Every pre-existing SQL object except the one expanded target CHECK remains exact.
  const current=new Map(db.prepare('SELECT type,name,tbl_name,sql FROM sqlite_schema').all().map(x=>[x.name,x]));
  for(const old of before.schema)if(old.name!=='app_runtime_targets')assert.deepEqual(current.get(old.name),old,old.name+' exact DDL retained');
}
function seed(t,version,{wal=true}={}) {
  const loc=location(t),db=new DatabaseSync(loc.file);loc.own(()=>db.close());db.exec('PRAGMA foreign_keys=ON');
  if(wal)db.exec('PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0');
  const base=baseOf(version);
  migrateAppsSchema(db,{legacyTemplate:LEGACY,...flags(base),allowAudienceMigration:true,allowCoverMigration:version>=12});
  db.exec('BEGIN IMMEDIATE');
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(CONNECTOR,OWNER,'{"linkId":"link","hostDeviceId":"host","connectorId":"connector"}','Synthetic source',1);
  db.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(APP,OWNER,CONNECTOR,'Synthetic retained app',5350,'/',JSON.stringify(GRANTS),'enabled',1,1,1);
  for(const [kind,id] of [['account','synthetic-reader'],['community','synthetic-group']])db.prepare('INSERT INTO local_app_grants VALUES(?,?,?)').run(APP,kind,id);
  const app=db.prepare('SELECT * FROM local_apps WHERE id=?').get(APP);ensureCanonicalDomain(db,app,LEGACY);ensureInitialPublication(db,app);
  const target={appId:APP,revision:2,ownerAccountId:OWNER,connectorKey:CONNECTOR,port:5351,entryPath:base>=7?'/embed':'/',profile:base>=7?SCOPED_RUNTIME_PROFILE:RUNTIME_PROFILE};
  target.digest=runtimeTargetDigest(target);
  db.prepare('INSERT INTO app_runtime_targets VALUES(?,?,?,?,?,?,?,?,?)').run(APP,2,OWNER,CONNECTOR,target.port,target.entryPath,target.profile,target.digest,2);
  if(base>=7) {
    const profile={schema:SCOPED_RUNTIME_PROFILE,appId:APP,connector:{linkId:'link',hostDeviceId:'host',connectorId:'connector'},target:{revision:2,digest:target.digest},
      sourceProfile:{id:'synthetic.retained-source',version:1,digest:'d'.repeat(64)},
      resource:{registryId:'soty',tenantId:OWNER,environmentId:'fixture',appId:APP,resourceId:'selected',workspaceId:'selected'},
      issuer:'https://root.example/human-identity',clientId:'synthetic-source',embedOrigin:LEGACY.replace('{appId}',APP),nativeOrigin:'http://127.0.0.1:5351',parentOrigin:'https://root.example'};
    createScopedAdmissionRegistry({db,profiles:[profile],clock:()=>2}).commit(target);
  }
  db.prepare('UPDATE app_source_heads SET required_binding_version=2 WHERE app_id=?').run(APP);
  db.prepare('UPDATE app_publications SET active_target_revision=2,policy_epoch=2 WHERE app_id=?').run(APP);
  db.prepare('INSERT INTO app_source_receipts VALUES(?,?,?,?,?,?,?)').run(OWNER,'1'.repeat(64),'2'.repeat(64),APP,2,RAW,2);
  db.prepare('INSERT INTO app_publication_receipts VALUES(?,?,?,?,?,?,?)').run(OWNER,'3'.repeat(64),'4'.repeat(64),APP,2,RAW,2);
  const domain=db.prepare('SELECT * FROM app_domains WHERE app_id=?').get(APP);
  db.prepare('INSERT INTO app_saved_heads VALUES(?,1)').run(OWNER);
  db.prepare('INSERT INTO app_saved_entries VALUES(?,?,?,?,?,?,?,?)').run(OWNER,APP,domain.id,domain.origin,'/','Retained saved entry',1,2);
  db.prepare('INSERT INTO app_saved_receipts VALUES(?,?,?,?,?,?,?)').run(OWNER,'5'.repeat(64),'6'.repeat(64),APP,1,1,2);
  db.prepare('INSERT INTO app_audiences VALUES(?,?)').run(APP,'friends');
  const audienceHash=discussionAudienceDigest(OWNER,'restricted',GRANTS,'friends'),body='Synthetic retained discussion';
  db.prepare('INSERT INTO app_discussion_heads VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run(APP,CONVERSATION,1,OWNER,'restricted',JSON.stringify(GRANTS),audienceHash,1,Buffer.byteLength(body),1,2,100,2);
  db.prepare('INSERT INTO app_discussion_head_audiences VALUES(?,?,?)').run(APP,CONVERSATION,'friends');
  db.prepare('INSERT INTO app_discussion_conversations VALUES(?,?,?,?,?,?,?,?,?,?)').run(CONVERSATION,APP,1,OWNER,'restricted',JSON.stringify(GRANTS),audienceHash,2,1,1);
  db.prepare('INSERT INTO app_discussion_conversation_audiences VALUES(?,?,?)').run(CONVERSATION,APP,'friends');
  db.prepare('INSERT INTO app_discussion_messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(MESSAGE,APP,CONVERSATION,1,OWNER,'Synthetic author','7'.repeat(64),'8'.repeat(64),body,Buffer.byteLength(body),null,2,null,null);
  db.prepare('INSERT INTO app_discussion_changes VALUES(?,?,?,?,?)').run(CONVERSATION,1,MESSAGE,'message',2);
  db.prepare('UPDATE app_discussion_usage SET head_count=1,conversation_count=1,message_count=1,body_bytes=? WHERE id=1').run(Buffer.byteLength(body));
  db.prepare('INSERT INTO app_discussion_rates VALUES(?,?,?)').run(OWNER,2,100);
  if(version>=12) {
    for(const [i,bytes] of [[1,Buffer.from([0,255,128,65])],[2,Buffer.from([66,0,254,127])]]) {
      db.prepare('INSERT INTO app_cover_assets VALUES(?,?,?,?,?,?,?,?,?,?)').run('asset-'+i,APP,OWNER,'upload',2,2,'recipe-synthetic',bytes.length,2,null);
      db.prepare('INSERT INTO app_cover_renditions VALUES(?,?,?,?,?,?,?,?)').run('asset-'+i,'field',256,256,'image/png',createHash('sha256').update(bytes).digest('hex'),bytes.length,bytes);
      db.prepare('INSERT INTO app_cover_versions VALUES(?,?,?,?,?,?,?,?,?,?)').run('version-'+i,APP,OWNER,i,'custom','asset-'+i,'{"x":0.5,"y":0.5,"zoom":1}',i,null,99999);
    }
    db.prepare('INSERT INTO app_cover_heads VALUES(?,?,?,?,?,?,?,?)').run(APP,OWNER,2,3,'version-2','version-1',2,2);
    db.prepare('INSERT INTO app_cover_drafts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run('draft-synthetic',OWNER,'synthetic-device','app',APP,'prepare-synthetic',2,2,2,'upload','ready','asset-1',0,2,90000,99999);
    db.prepare('INSERT INTO app_cover_receipts VALUES(?,?,?,?,?,?,?,?)').run(OWNER,'receipt-synthetic','9'.repeat(64),'commit',APP,RAW,2,99999);
    db.prepare('INSERT INTO app_cover_owner_usage VALUES(?,?,?,?,?,?,?)').run(OWNER,8,0,1,1,2,1);
    db.prepare('UPDATE app_cover_usage SET encoded_bytes=8,draft_count=1,receipt_count=1,logical_now=2 WHERE id=1').run();
  }
  db.exec('COMMIT');assert.equal(inspectAppsSchema(db),'v'+version);
  if(wal)db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  return {...loc,db,target};
}

for(const [from,to] of [[9,10],[9,11],[10,11],[12,13],[12,14],[13,14]])test(`explicit composed ${from}->${to} preserves all data/ACL/cover bytes, WAL and frozen host format`,async t=>{
  const f=seed(t,from),before=snapshot(f.db),main=readFileSync(f.file),targetBase=baseOf(to);
  const result=migrateAppsSchema(f.db,{legacyTemplate:LEGACY,...change(targetBase)});
  assert.equal(result.schema,'soty.apps-registry.v'+to);assert.equal(result.migrated,true);
  assert.equal(inspectAppsSchema(f.db),'v'+to);preserved(f.db,before);
  assert.deepEqual(f.db.prepare('PRAGMA foreign_key_check').all(),[]);assert.equal(f.db.prepare('PRAGMA foreign_keys').get().foreign_keys,1);
  assert.equal(f.db.isTransaction,false);assert.deepEqual(readFileSync(f.file),main,'DDL/marker commit stays in WAL');
  const observed=await readStorageFormat(f.root);assert.equal(observed.apps,to);assert.equal(observed.schema,'soty.storage-format.v7');
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name='layered_runtime_targets_migration'").get().n,0);
  const admissions=f.db.prepare('SELECT count(*) AS n FROM app_scoped_embed_admissions').get().n;
  assert.equal(admissions,baseOf(from)>=7?1:0,'DDL never installs an approved profile/admission');
  assert.equal(f.db.prepare('SELECT profile FROM app_runtime_targets WHERE revision=2').get().profile,f.target.profile);
  assert.equal(migrateAppsSchema(f.db,{legacyTemplate:LEGACY,...change(targetBase)}).migrated,false);
  preserved(f.db,before);
});

test('cover12->13->14 retains cover state across both explicit base steps, previous-version undo, draft CAS and exact receipt bytes',t=>{
  const f=seed(t,12),before=snapshot(f.db);
  migrateAppsSchema(f.db,{legacyTemplate:LEGACY,allowScopedEmbedMigration:true});preserved(f.db,before);
  migrateAppsSchema(f.db,{legacyTemplate:LEGACY,allowSelectedResourceMigration:true});preserved(f.db,before);
  assert.equal(inspectAppsSchema(f.db),'v14');assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_scoped_embed_admissions').get().n,0);
  assert.deepEqual({...f.db.prepare('SELECT active_version_id,previous_version_id,previous_replaced_at FROM app_cover_heads').get()},
    {active_version_id:'version-2',previous_version_id:'version-1',previous_replaced_at:2});
  assert.equal(f.db.prepare('SELECT cover_revision FROM app_cover_drafts').get().cover_revision,2);
  assert.equal(f.db.prepare('SELECT value_json FROM app_cover_receipts').get().value_json,RAW);
});

test('no base flags is a true no-op; only cover/audience flags cannot admit Source profiles',t=>{
  const f=seed(t,12),before=snapshot(f.db);
  assert.equal(migrateAppsSchema(f.db,{legacyTemplate:LEGACY,allowCoverMigration:true,allowAudienceMigration:true}).migrated,false);
  assert.deepEqual(snapshot(f.db),before);
  migrateAppsSchema(f.db,{legacyTemplate:LEGACY,allowSelectedResourceMigration:true});
  const current=snapshot(f.db),candidate={...f.target,revision:3,profile:RESOURCE_RUNTIME_PROFILE,entryPath:'/embed'};candidate.digest=runtimeTargetDigest(candidate);
  const registry=createScopedAdmissionRegistry({db:f.db,profiles:[]});
  assert.throws(()=>registry.verifyCandidate(candidate),{code:'app_scoped_admission_required'});
  f.db.exec('BEGIN IMMEDIATE');try{assert.throws(()=>registry.commit(candidate),{code:'app_scoped_admission_required'});}finally{f.db.exec('ROLLBACK');}
  assert.deepEqual(snapshot(f.db),current,'Migration flags do not supply Native selection/host authority');
});

test('known corruption and unknown DDL are refused without repair or persistent change',t=>{
  for(const kind of ['unknown-DDL','corrupt-FK']) {
    const f=seed(t,12);
    if(kind==='unknown-DDL')f.db.exec('CREATE TABLE unrelated(value TEXT)');
    else {f.db.exec('PRAGMA foreign_keys=OFF');f.db.prepare('INSERT INTO app_cover_assets VALUES(?,?,?,?,?,?,?,?,?,?)').run('foreign-asset','app-'+'f'.repeat(32),OWNER,'upload',2,2,'synthetic',1,2,null);f.db.exec('PRAGMA foreign_keys=ON');}
    const before=snapshot(f.db);
    assert.throws(()=>migrateAppsSchema(f.db,{legacyTemplate:LEGACY,allowSelectedResourceMigration:true}),{code:kind==='unknown-DDL'?'apps_schema_unsupported':'apps_registry_corrupt'});
    assert.deepEqual(snapshot(f.db),before);assert.equal(f.db.isTransaction,false);
  }
});

test('after real final-epoch write failure rolls back every DDL/row and restores FK mode',t=>{
  for(const foreignKeys of [0,1]) {
    const f=seed(t,12);f.db.exec('PRAGMA foreign_keys='+foreignKeys);const before=snapshot(f.db);let reached=false;
    const fault=new Proxy(f.db,{get(target,key){
      if(key==='exec')return sql=>{const result=target.exec(sql);if(/^PRAGMA user_version=14;?$/u.test(sql)){reached=true;assert.equal(target.isTransaction,true);throw Error('synthetic-after-final-epoch');}return result;};
      const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
    }});
    assert.throws(()=>migrateAppsSchema(fault,{legacyTemplate:LEGACY,allowSelectedResourceMigration:true}),/synthetic-after-final-epoch/u);
    assert.equal(reached,true,'Reach actual SQL mutation rather than preflight rejection');assert.equal(f.db.isTransaction,false);
    assert.deepEqual(snapshot(f.db),before);assert.equal(inspectAppsSchema(f.db),'v12');
  }
});

test('template mismatch, non-boolean flags and caller-owned transaction do not mutate the layered store',t=>{
  const f=seed(t,12),before=snapshot(f.db);
  assert.throws(()=>migrateAppsSchema(f.db,{legacyTemplate:'https://{appId}.other.example',allowSelectedResourceMigration:true}),{code:'apps_origin_template_changed'});
  assert.throws(()=>migrateAppsSchema(f.db,{legacyTemplate:LEGACY,allowScopedEmbedMigration:1}),{code:'apps_audience_migration_configuration_invalid'});
  assert.deepEqual(snapshot(f.db),before);
  f.db.exec('BEGIN IMMEDIATE');
  try {assert.throws(()=>migrateAppsSchema(f.db,{legacyTemplate:LEGACY,allowScopedEmbedMigration:true}),{code:'apps_audience_base_upgrade_required'});assert.equal(f.db.isTransaction,true);assert.deepEqual(snapshot(f.db),before);}
  finally {f.db.exec('ROLLBACK');}
  assert.deepEqual(snapshot(f.db),before);
});

test('actual fresh decoder factory remains cover12, then explicit startup flags compose to13/14 without registry grants',t=>{
  const loc=location(t),decoder=createCoverDecoder();let apps;
  loc.own(()=>apps?.close());
  const options={databasePath:loc.file,appOriginTemplate:LEGACY,shellOrigins:['https://root.example'],coverDecoder:decoder,withCurrentContacts:(_actor,fn)=>fn(),withAuthorityFence:fn=>fn()};
  const inspect=fn=>{const db=new DatabaseSync(loc.file);try{return fn(db);}finally{db.close();}};
  apps=createAppsService(options);assert.equal(inspect(inspectAppsSchema),'v12');assert.ok(apps.coverExtension);apps.close();apps=null;
  apps=createAppsService({...options,allowScopedEmbedMigration:true});assert.equal(inspect(inspectAppsSchema),'v13');apps.close();apps=null;
  apps=createAppsService({...options,allowSelectedResourceMigration:true});assert.equal(inspect(inspectAppsSchema),'v14');
  assert.equal(inspect(db=>db.prepare('SELECT count(*) AS n FROM app_scoped_embed_admissions').get().n),0);
  assert.equal(inspect(db=>db.prepare('SELECT count(*) AS n FROM app_runtime_targets').get().n),0);
});


test('another real connection installs cover12 at the writer gate; explicit requester composes to14 without losing the winning store',t=>{
  const loc=location(t),db=new DatabaseSync(loc.file),winner=new DatabaseSync(loc.file);
  loc.own(()=>db.close());loc.own(()=>winner.close());db.exec('PRAGMA foreign_keys=ON');winner.exec('PRAGMA foreign_keys=ON');
  migrateAppsSchema(db,{legacyTemplate:LEGACY});let raced=false,winning;
  const gate=new Proxy(db,{get(target,key){
    if(key==='exec')return sql=>{if(sql==='BEGIN IMMEDIATE'&&!raced){raced=true;migrateAppsSchema(winner,{legacyTemplate:LEGACY,allowCoverMigration:true});winning=snapshot(winner);}return target.exec(sql);};
    const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
  }});
  assert.equal(migrateAppsSchema(gate,{legacyTemplate:LEGACY,allowSelectedResourceMigration:true}).schema,'soty.apps-registry.v14');
  assert.equal(raced,true);assert.equal(inspectAppsSchema(db),'v14');preserved(db,winning);
  assert.equal(db.prepare('PRAGMA foreign_keys').get().foreign_keys,1);assert.equal(db.isTransaction,false);
  assert.equal(db.prepare('SELECT count(*) AS n FROM app_scoped_embed_admissions').get().n,0);
});
