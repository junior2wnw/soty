import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { migrateAppsSchema, inspectAppsSchema, runtimeTargetDigest, ensureInitialPublication } from '../server/schema.mjs';
import { createEndpointRegistration } from '../server/endpoint-registration.mjs';
import { SELF_IP_PROFILE, ENDPOINT_SCHEMA, endpointDigestTuple, normalizeSelfEndpoint, targetFromRow } from '../server/endpoint-targets.mjs';

// Literal canonical Git blob from 450204c317e56d1c092046ca9864a05fc985149b; never the current HEAD reader.
const historicalBytes=readFileSync(new URL('./fixtures/apps-schema-before-self-ip-450204.mjs',import.meta.url));
assert.equal(createHash('sha256').update(historicalBytes).digest('hex'),'834d2a270758c7bf3ce5c71d195eca6fd96643a6bc1ce8dbc387d0d3de2fe1c7','historical Apps schema byte pin');
const historical=historicalBytes.toString('utf8');
const oldSource=historical.replace(/from (['"])(\.{1,2}\/[^'"]+)\1/gu,(_all,_quote,specifier)=>`from ${JSON.stringify(new URL(specifier,new URL('../server/schema.mjs',import.meta.url)).href)}`);
const old=await import('data:text/javascript;base64,'+Buffer.from(oldSource).toString('base64'));
const fresh=(options,historical=false)=>{const db=new DatabaseSync(':memory:');db.exec('PRAGMA foreign_keys=ON');(historical?old.migrateAppsSchema:migrateAppsSchema)(db,options);return db;};
const owner=Object.freeze({accountId:'endpoint_owner',deviceId:'endpoint_browser'}),device={connector_key:'endpoint_link|endpoint_host|endpoint_connector',owner_account_id:owner.accountId};
const witness=Object.freeze({}),endpoint={scope:'connector-self',scheme:'http',family:'IPv4',address:'192.168.1.20'};
const args={requestId:'endpoint-registration-replay-001',name:'Real endpoint registration',hostDeviceId:'endpoint_host',connectorId:'endpoint_connector',
  source:{schema:'soty.app-http-source.v2',endpoint,port:30000,entryPath:'/'},grants:{accountIds:[],communityIds:[]},inventoryWitness:{}};

test('explicit endpoint storage retains each legacy/scoped/resource + audience/cover format; genuine old reader refuses upgraded DDL',()=>{
  for(const options of [{},{allowScopedEmbedMigration:true},{allowSelectedResourceMigration:true}])for(const layer of [{},{allowAudienceMigration:true},{allowCoverMigration:true}]){
    const db=fresh({...options,...layer},true);try{
      const before=inspectAppsSchema(db);assert.equal(old.inspectAppsSchema(db),before,'control: historical reader accepts the unchanged original format');
      migrateAppsSchema(db,{...options,...layer,allowSelfIpTargets:true});assert.equal(inspectAppsSchema(db),before);
      assert.throws(()=>old.inspectAppsSchema(db),{code:'apps_schema_unsupported'},'old reader must refuse endpoint layout before any localhost interpretation');
      assert.throws(()=>migrateAppsSchema(db,options),{code:'apps_endpoint_storage_capability_required'});
      migrateAppsSchema(db,{...options,...layer,allowSelfIpTargets:true});assert.equal(db.prepare('PRAGMA foreign_key_check').get(),undefined);
    }finally{db.close();}
  }
});
test('endpoint extension survives later explicit base/audience/cover upgrades and keeps target CHECK closed',()=>{
  const db=fresh({allowSelfIpTargets:true});try{
    for(const options of [{allowScopedEmbedMigration:true},{allowSelectedResourceMigration:true},{allowAudienceMigration:true},{allowCoverMigration:true}]){
      migrateAppsSchema(db,{...options,allowSelfIpTargets:true});assert.ok(inspectAppsSchema(db));
      assert.match(db.prepare("SELECT sql FROM sqlite_schema WHERE name='app_runtime_targets'").get().sql,/soty\.relay-self-ip\.v1/);
    }
  }finally{db.close();}
});
test('v1 digest bytes remain identical; v2 address is mandatory and different addresses cannot share a pin',()=>{
  const legacy={appId:'app-'+'a'.repeat(32),revision:1,ownerAccountId:owner.accountId,connectorKey:device.connector_key,port:30000,entryPath:'/',profile:'soty.relay-restricted.v1'};
  assert.equal(runtimeTargetDigest(legacy),old.runtimeTargetDigest(legacy));
  const first={...legacy,schema:ENDPOINT_SCHEMA,profile:SELF_IP_PROFILE,endpoint},second={...first,endpoint:{...endpoint,address:'192.168.1.21'}};
  assert.notEqual(runtimeTargetDigest(first),runtimeTargetDigest(second));assert.throws(()=>runtimeTargetDigest({...first,endpoint:undefined}));
  assert.throws(()=>normalizeSelfEndpoint({...endpoint,address:'010.0.0.1'}));
  const brokenLegacyDigest=value=>old.runtimeTargetDigest({...value,profile:'soty.relay-restricted.v1'});
  assert.throws(()=>assert.notEqual(brokenLegacyDigest(first),brokenLegacyDigest(second)),'negative control catches address-ignored digest math');
  assert.equal(endpointDigestTuple(first)[0],ENDPOINT_SCHEMA);
});
test('failed receipt write rolls app/target/endpoint back before notification; exact retry/replay stays one committed app',async()=>{
  const db=fresh({allowSelfIpTargets:true});let calls=0,notifications=0,inventoryLive=true,active=true;
  db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(device.connector_key,owner.accountId,JSON.stringify({linkId:'endpoint_link',hostDeviceId:'endpoint_host',connectorId:'endpoint_connector'}),'Test device',1);
  const assertActor=actor=>assert.ok(active&&actor===owner,'original active actor required'),proofs=new WeakSet();
  const registry=createEndpointRegistration({db,enabled:true,blockedPorts:[],assertActor,
    withAuthorityFence(callback,actor){assertActor(actor);return callback();},
    transaction(callback){assert.equal(db.isTransaction,false);db.exec('BEGIN IMMEDIATE');try{const result=callback();db.exec('COMMIT');return result;}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}},
    resolveDevice(actor){assertActor(actor);return device;},inventoryFor(){assert.ok(inventoryLive);return witness;},assertGrants(){},
    async prepareTarget(){assert.equal(db.isTransaction,false);calls++;const evidence={};proofs.add(evidence);return evidence;},verifyPreparedTarget({evidence}){assert.equal(db.isTransaction,true);return proofs.has(evidence);},
    insertApp(actor,intent,target){db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(target.appId,actor.accountId,device.connector_key,intent.name,target.port,target.entryPath,JSON.stringify(intent.grants),'enabled',1,1,1);ensureInitialPublication(db,{id:target.appId,owner_account_id:actor.accountId,connector_key:device.connector_key,port:target.port,entry_path:target.entryPath,created_at:1,updated_at:1},target);},
    readApp(id){const row=db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);return{id:row.id,name:row.name};},afterCommit(){assert.equal(db.isTransaction,false);notifications++;}});
  const invoke=input=>registry.executeAsync({op:'apps.registerEndpoint',actor:owner,args:input});
  try{
    db.exec("CREATE TRIGGER test_receipt_write_failure BEFORE INSERT ON app_endpoint_registration_receipts BEGIN SELECT RAISE(ABORT,'test_disk_write_failure'); END");
    await assert.rejects(invoke(args),/test_disk_write_failure/);
    for(const table of ['local_apps','app_runtime_targets','app_runtime_endpoints','app_publications','app_endpoint_registration_receipts'])assert.equal(db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n,0,table);
    assert.equal(notifications,0);db.exec('DROP TRIGGER test_receipt_write_failure');
    const committed=await invoke(args);assert.equal(committed.registrationReceipt.committed,true);assert.equal(calls,2);assert.equal(notifications,1);
    const stored=db.prepare('SELECT * FROM app_runtime_targets').get();assert.equal(runtimeTargetDigest(targetFromRow(db,stored)),stored.digest);
    inventoryLive=false;const replay=await invoke({...args,inventoryWitness:{stale:true}});assert.equal(replay.app.id,committed.app.id);assert.equal(calls,2,'replay requires no fresh probe/lease');
    await assert.rejects(invoke({...args,name:'Retargeted request'}),{code:'apps_endpoint_request_conflict'});
    inventoryLive=true;const pending=invoke({...args,requestId:'endpoint-second-await-revoke-001'});active=false;
    await assert.rejects(pending,/original active actor required/);await assert.rejects(invoke(args));assert.equal(db.prepare('SELECT count(*) AS n FROM local_apps').get().n,1);
  }finally{db.close();}
});
