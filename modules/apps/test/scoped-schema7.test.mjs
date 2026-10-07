import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { migrateAppsSchema, inspectAppsSchema } from '../server/schema.mjs';
import { createHistoricalAppsV3, seedHistoricalPublicationV3 } from '../../../deploy/connector/apps-v3.fixture.mjs';
import { readScopedApps7, requireApps7BeforeStart } from '../../../deploy/apps/scoped-schema7-reader.mjs';
import { inspectAppsSchema as oldReader } from '../../../deploy/apps/fixtures/apps6-reader.literal.mjs';

test('explicit v6→v7 preserves every existing tuple/receipt/source byte and FK; literal independent readers agree, old reader refuses before START', (t) => {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close()); db.exec('PRAGMA foreign_keys=ON');
  createHistoricalAppsV3(db);
  const appId='app-'+'a'.repeat(32), owner='synthetic_owner', key='link|host|connector';
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(key,owner,JSON.stringify({linkId:'link',hostDeviceId:'host',connectorId:'connector'}),'Synthetic Source',1);
  db.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(appId,owner,key,'Synthetic',5350,'/embed',JSON.stringify({accountIds:[],communityIds:[]}),'enabled',1,1,1);
  db.prepare('INSERT INTO app_domain_heads VALUES(?,1)').run(appId); seedHistoricalPublicationV3(db,appId);
  db.prepare('INSERT INTO app_publication_receipts VALUES(?,?,?,?,?,?,?)').run(owner,'synthetic_key','synthetic_intent',appId,2,'{"literal":"\\u0430","order":1}',1);
  migrateAppsSchema(db); assert.equal(oldReader(db),'v6');
  const tables=db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*'").all().map(row=>row.name);
  const before=new Map(tables.map(name=>[name, JSON.stringify(db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all())]));
  assert.equal(migrateAppsSchema(db).schema,'soty.apps-registry.v6');
  assert.equal(migrateAppsSchema(db,{allowScopedEmbedMigration:true}).schema,'soty.apps-registry.v7');
  assert.equal(inspectAppsSchema(db),'v7');
  for(const [name,bytes] of before) if(name!=='apps_meta') assert.equal(JSON.stringify(db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()),bytes,name);
  assert.equal(db.prepare('PRAGMA foreign_keys').get().foreign_keys,1);
  assert.deepEqual(db.prepare('PRAGMA foreign_key_check').all(),[]);
  assert.deepEqual(readScopedApps7(db),{schema:'soty.apps-registry.v7',targets:1,selected:0});
  assert.throws(()=>oldReader(db),{code:'apps_schema_unsupported'});
  assert.throws(()=>requireApps7BeforeStart({storedSchema:'soty.apps-registry.v7',imageReaders:['soty.apps-registry.v6']}));
  assert.equal(requireApps7BeforeStart({storedSchema:'soty.apps-registry.v7',imageReaders:['soty.apps-registry.v6','soty.apps-registry.v7']}),true);
  assert.equal(migrateAppsSchema(db).migrated,false);
});

test('v7 upgrade refuses unknown DDL without any persistent alteration or FK-mode change', t=>{
  const db=new DatabaseSync(':memory:');t.after(()=>db.close());db.exec('PRAGMA foreign_keys=ON');migrateAppsSchema(db);
  db.exec('CREATE TABLE unknown_object(value TEXT)');
  const before=JSON.stringify(db.prepare("SELECT name,sql FROM sqlite_schema ORDER BY name").all());
  assert.throws(()=>migrateAppsSchema(db,{allowScopedEmbedMigration:true}),{code:'apps_schema_unsupported'});
  assert.equal(JSON.stringify(db.prepare("SELECT name,sql FROM sqlite_schema ORDER BY name").all()),before);
  assert.equal(db.prepare('PRAGMA user_version').get().user_version,6);
  assert.equal(db.prepare('PRAGMA foreign_keys').get().foreign_keys,1);
});
