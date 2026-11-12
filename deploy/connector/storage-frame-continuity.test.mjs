import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtemp,mkdir,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {migrateAppsSchema,ensureCanonicalDomain,ensureInitialPublication} from '../../modules/apps/server/schema.mjs';
import {createFrameContinuity} from '../../modules/apps/server/frame-continuity.mjs';
import {canonical,hash} from '../../modules/apps/scoped-embed/profile.mjs';
import {readStorageFormat} from './storage-probe.mjs';
import {assertStorageCompatible,currentStorageReaders,storageReaderLabel} from './storage-guard.mjs';
const image=v=>({Id:'sha256:'+'a'.repeat(64),Config:{Labels:{[storageReaderLabel]:typeof v==='string'?v:JSON.stringify(v)}}});
async function fixture(t,options={}){const root=await mkdtemp(join(tmpdir(),'soty-frame-reader-'));t.after(()=>rm(root,{recursive:true,force:true}));await mkdir(join(root,'apps'));const file=join(root,'apps/registry.sqlite'),d=new DatabaseSync(file);migrateAppsSchema(d,{legacyTemplate:'http://{appId}.localhost',...options});return{root,file,d};}
test('independent passive DDL pin, readonly probe and literal previous reader label refuse new capability',async t=>{
 for(const options of [{},{allowSelectedResourceMigration:true},{allowSelectedResourceMigration:true,allowCoverMigration:true},{allowSelectedResourceMigration:true,allowSelfIpTargets:true,allowCoverMigration:true}]){
  const{root,file,d}=await fixture(t,options);const rows=d.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name LIKE 'app_frame_continuity%' ORDER BY name").all().map(r=>[r.type,r.name,r.tbl_name,r.sql.replace(/\s+/gu,' ').trim()]);
  assert.equal(rows.length,3);assert.equal(createHash('sha256').update(JSON.stringify(rows)).digest('hex'),'ef452bfad580903943b9a73830d895f068f9577ad6cb398ecd44c24e84212a5c');d.close();
  const before=await readFile(file),format=await readStorageFormat(root);assert.equal(format.appsFrameContinuity,2);assert.deepEqual(await readFile(file),before);assertStorageCompatible(image(currentStorageReaders),format);
  const previous=JSON.parse(currentStorageReaders);previous.version=8;previous.readers.world=[6,7];delete previous.readers.appsFrameContinuity;delete previous.readers.monitoring;assert.throws(()=>assertStorageCompatible(image(previous),format),{code:'storage_reader_incompatible'});
 }
});
for(const [name,breakIt,code]of[
 ['future',d=>d.exec("UPDATE apps_meta SET value='soty.apps-frame-continuity.v3' WHERE key='frame_continuity'"),'storage_format_unknown'],
 ['missing marker',d=>d.exec("DELETE FROM apps_meta WHERE key='frame_continuity'"),'storage_format_unreadable'],
 ['changed index',d=>d.exec('DROP INDEX app_frame_continuity_owner'),'storage_format_unreadable'],
 ['unknown extra',d=>d.exec('CREATE TABLE frame_bypass(data TEXT)'),'storage_format_unreadable']
])test('frame host probe refuses '+name,async t=>{const{root,d}=await fixture(t);breakIt(d);d.close();await assert.rejects(readStorageFormat(root),{code});});

test('populated passive scope uses a digest only; a correctly hashed raw path field still fails the independent reader',async t=>{
 const{root,file,d}=await fixture(t),appId='app-'+'a'.repeat(32),owner='fixture_owner',connector='link|host|connector';
 d.exec('BEGIN IMMEDIATE');
 d.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connector,owner,JSON.stringify({linkId:'link',hostDeviceId:'host',connectorId:'connector'}),'Fixture',1);
 d.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(appId,owner,connector,'Fixture',30000,'/',JSON.stringify({accountIds:[],communityIds:[]}),'enabled',1,1,1);
 const app=d.prepare('SELECT * FROM local_apps WHERE id=?').get(appId);ensureCanonicalDomain(d,app,'http://{appId}.localhost');ensureInitialPublication(d,app);
 d.exec('COMMIT');
 const entry=d.prepare('SELECT id,origin FROM app_domains WHERE app_id=?').get(appId),record={handleHash:'a'.repeat(64),context:{appId,rootPrincipal:{accountId:owner,deviceId:'fixture_device'},reference:{id:'F'.repeat(43)},
  sourceProfile:{id:'soty.standard-workspace',version:2,digest:'23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9'},profileDigest:'b'.repeat(64),policyEpoch:1,
  target:{revision:1,digest:'c'.repeat(64)},entry:{domainId:entry.id,origin:entry.origin},resource:{id:'selected'},humanPrincipal:{issuer:'https://root.test',subject:owner}}};
 createFrameContinuity({db:d}).attach(record,{family:{frameId:record.context.reference.id},entryPath:'/embed#invite='+ 's'.repeat(43)});d.close();
 assert.equal((await readStorageFormat(root)).appsFrameContinuity,2);
 const edit=new DatabaseSync(file);try{const scope=JSON.parse(edit.prepare('SELECT binding_json FROM app_frame_continuity').get().binding_json);scope.entry.path='/embed#invite='+ 's'.repeat(43);
  edit.prepare('UPDATE app_frame_continuity SET binding_json=?,binding_digest=?').run(canonical(scope),hash(scope));}finally{edit.close();}
 await assert.rejects(readStorageFormat(root),{code:'storage_format_unreadable'},'valid outer digest cannot hide a raw credential-bearing field');
});
