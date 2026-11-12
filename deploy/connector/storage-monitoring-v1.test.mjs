import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import {mkdtemp,mkdir,readFile,writeFile,rm,stat,unlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {MONITORING_DDL} from '../../modules/monitoring/server/schema.mjs';
import {readStorageFormat} from './storage-probe.mjs';
import {snapshotStorage} from './storage-snapshot.mjs';
import {assertStorageCompatible,checkedStorageFormat,currentStorageReaders,guardStorageStart,requireStorageStartReceipt,storageReaderLabel,storageReaders} from './storage-guard.mjs';
import {prepareUniversalPolicy,disposeUniversalPolicy,assertUniversalImagePrerequisites,UNIVERSAL_POLICY_SCHEMA,universalModeLabel} from './universal-policy.mjs';

const sha=value=>createHash('sha256').update(value).digest('hex');
const layoutDigest='6e0001fe89f391981e64ee86563ad0161b5e3aec5968eaddf46f4bb703eec5a0';
const image=readers=>({Id:'sha256:'+'a'.repeat(64),Config:{Labels:{[storageReaderLabel]:typeof readers==='string'?readers:JSON.stringify(readers),[universalModeLabel]:'1'}}});
async function fixture(t,{empty=false,policy=false}={}){
 const base=path.resolve(tmpdir()),prefix=policy?'soty-universal-policy-':'soty-monitoring-storage-',root=await mkdtemp(path.join(base,prefix)),source=path.join(root,'source');await mkdir(source);let db;
 t.after(async()=>{db?.close();assert.equal(path.dirname(path.resolve(root)),base);assert.ok(path.basename(root).startsWith(prefix));await rm(root,{recursive:true,force:true});});
 const file=path.join(source,'monitoring','monitoring.sqlite');
 if(!empty){await mkdir(path.dirname(file));db=new DatabaseSync(file);db.exec(MONITORING_DDL);db.prepare('INSERT INTO monitoring_meta VALUES(1,?,?,?)').run('soty','soty.monitoring.sqlite.v1','aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa');}
 return{root,source,file,get db(){return db;},close(){db?.close();db=null;}};
}
function reader(version){const value=JSON.parse(currentStorageReaders);value.version=version;if(version<10){value.readers.world=[6,7];value.readers.feedback=[1];}if(version===9)delete value.readers.monitoring;else value.readers.monitoring=[1];return value;}

test('frozen monitoring1 store is independently observed, read-only, and declared in current reader envelope10',async t=>{
 const f=await fixture(t);assert.equal(sha(MONITORING_DDL),'be38b10a2d6d5d11773a7eb8038f1ecd109fc15bc132532c1683794794e20372');
 const rows=f.db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all();
 assert.equal(rows.length,9);assert.equal(sha(JSON.stringify(rows.map(r=>[r.type,r.name,r.tbl_name,r.sql.replace(/\s+/gu,' ').trim()]))),layoutDigest);
 f.close();const before=await readFile(f.file),format=await readStorageFormat(f.source);
 assert.equal(format.monitoring,1);assert.equal(format.schema,'soty.storage-format.v10');assert.deepEqual(await readFile(f.file),before);
 assert.deepEqual(storageReaders(image(currentStorageReaders)).monitoring,[1]);assertStorageCompatible(image(currentStorageReaders),format);
});
test('missing reader-only monitoring store is explicit empty and probing never creates it',async t=>{
 const f=await fixture(t,{empty:true}),format=await readStorageFormat(f.source);assert.equal(format.monitoring,'empty');
 await assert.rejects(stat(path.dirname(f.file)),{code:'ENOENT'});assertStorageCompatible(image(reader(9)),format);
});
test('actual monitoring data refuses a legacy9 image, and unknown or extra current envelope fields are rejected',async t=>{
 const f=await fixture(t);f.close();const format=await readStorageFormat(f.source);assert.equal(format.monitoring,1);
 assert.throws(()=>assertStorageCompatible(image(reader(9)),format),{code:'storage_reader_incompatible'});
 for(const value of [{...format,monitoring:2},{...format,monitoring:'1'},{...format,futureStore:1}])assert.throws(()=>checkedStorageFormat(value),{code:'storage_probe_invalid'});
 const legacy={...format,schema:'soty.storage-format.v9'};delete legacy.monitoring;checkedStorageFormat(legacy);
 assert.throws(()=>checkedStorageFormat({...legacy,monitoring:1}),{code:'storage_probe_invalid'});
 for(const value of [{...reader(10),version:12},{...reader(10),readers:{...reader(10).readers,monitoring:[2]}},{...reader(10),readers:{...reader(10).readers,monitoring:[1,1]}}])assert.throws(()=>storageReaders(image(value)),{code:'storage_reader_unknown'});
});
for(const[name,sql,code]of[
 ['future format','PRAGMA user_version=2','storage_format_unknown'],
 ['extra table','CREATE TABLE unknown(value)','storage_format_unreadable'],
 ['dropped index','DROP INDEX monitoring_retention','storage_format_unreadable'],
 ['wrong project',"UPDATE monitoring_meta SET project_id='other'",'storage_format_unknown'],
 ['wrong lineage',"UPDATE monitoring_meta SET lineage='soty.monitoring.sqlite.v2'",'storage_format_unknown'],
 ['invalid registry',"UPDATE monitoring_meta SET registry_id='------------------------------------'",'storage_format_unknown'],
 ['extra metadata row',"PRAGMA ignore_check_constraints=ON; INSERT INTO monitoring_meta VALUES(2,'soty','soty.monitoring.sqlite.v1','aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')",'storage_format_unknown'],
])test('monitoring probe refuses '+name+' without changing evidence',async t=>{
 const f=await fixture(t);f.db.exec(sql);f.close();const before=await readFile(f.file);await assert.rejects(readStorageFormat(f.source),{code});assert.deepEqual(await readFile(f.file),before);
});
test('monitoring file census refuses unknown files and an orphan WAL instead of treating them as empty',async t=>{
 const f=await fixture(t);f.close();const unknown=path.join(path.dirname(f.file),'unknown.json');await writeFile(unknown,'evidence');
 await assert.rejects(readStorageFormat(f.source),{code:'storage_format_unreadable'});await unlink(unknown);await unlink(f.file);await writeFile(f.file+'-wal','orphan');
 await assert.rejects(readStorageFormat(f.source),{code:'storage_format_unreadable'});
});
test('private snapshot includes monitoring main and WAL, validates the copied store, and counts its bytes against the same aggregate budget',async t=>{
 const f=await fixture(t);f.db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; INSERT INTO monitoring_configs VALUES('platform',1,1,1)");
 const before=[await readFile(f.file),await readFile(f.file+'-wal')],target=path.join(f.root,'snapshot');await snapshotStorage(f.source,target);
 assert.ok(await stat(path.join(target,'monitoring','monitoring.sqlite')).then(()=>true,()=>false),'Snapshot must include the actual monitoring database');
 assert.deepEqual(await readFile(path.join(target,'monitoring','monitoring.sqlite')),before[0]);assert.deepEqual(await readFile(path.join(target,'monitoring','monitoring.sqlite-wal')),before[1]);
 assert.equal((await readStorageFormat(target)).monitoring,1);assert.deepEqual(await readFile(f.file),before[0]);assert.deepEqual(await readFile(f.file+'-wal'),before[1]);
 const privateDb=new DatabaseSync(path.join(target,'monitoring','monitoring.sqlite'),{readOnly:true});try{assert.equal(privateDb.prepare('SELECT enabled FROM monitoring_configs').get().enabled,1);}finally{privateDb.close();}
 f.close();const bytes=(await stat(f.file)).size;await assert.rejects(snapshotStorage(f.source,path.join(f.root,'too-small'),bytes-1),{code:'storage_format_unreadable'});
});
test('fresh START requires monitoring-aware10 observation; a historical9 observation cannot attest its absence',async t=>{
 const f=await fixture(t);f.close();const format=await readStorageFormat(f.source),runtime={Id:'b'.repeat(64),Image:'sha256:'+'a'.repeat(64),State:{Running:false},Config:{Env:['DATA_DIR=/data']},Mounts:[{Type:'volume',Name:'fixture',Source:'/fixture/data',Destination:'/data',RW:true}]};
 const engine={async inspect(){return structuredClone(runtime);},async image(){return image(currentStorageReaders);},async request(method,url){assert.equal(method,'GET');if(url==='/containers/json')return[];assert.equal(url,'/volumes/fixture');return{Name:'fixture',Driver:'local',Scope:'local',Mountpoint:'/fixture/data',Options:{}};}};
 const old={...format,schema:'soty.storage-format.v9'};delete old.monitoring;
 await assert.rejects(guardStorageStart({engine,probe:async()=>old},runtime),{code:'storage_probe_invalid'});
 const receipt=await guardStorageStart({engine,probe:async()=>format},runtime);assert.equal(receipt.schema,'soty.storage-start.v10');assert.equal(receipt.monitoring,1);requireStorageStartReceipt(receipt,runtime.Id);
 const legacy={...receipt,schema:'soty.storage-start.v9'};delete legacy.monitoring;requireStorageStartReceipt(legacy,runtime.Id);
});
test('maintained universal image gate accepts known9 and monitoring-aware10 reader envelopes without accepting a future envelope',async t=>{
 const f=await fixture(t,{empty:true,policy:true}),handle=await prepareUniversalPolicy({schema:UNIVERSAL_POLICY_SCHEMA,phase:'legacy-baseline',human:null,reviews:null},{shellOrigins:['https://monitoring.fixture.invalid'],fixtureRoot:f.root});
 t.after(()=>disposeUniversalPolicy(handle));
 for(const version of[9,10])assert.doesNotThrow(()=>assertUniversalImagePrerequisites(handle,{candidateImage:image(reader(version)),originalImage:image(reader(version))}));
 assert.throws(()=>assertUniversalImagePrerequisites(handle,{candidateImage:image(reader(12)),originalImage:image(reader(10))}),{code:'storage_reader_unknown'});
});
