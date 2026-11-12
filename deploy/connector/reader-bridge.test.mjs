import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {mkdtemp,mkdir,readFile,readdir,writeFile,rm,unlink,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {spawn,spawnSync} from 'node:child_process';
import {once} from 'node:events';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {MONITORING_DDL} from '../../modules/monitoring/server/schema.mjs';
import {initializeWidgetStorage} from '../../modules/canvas-widgets/storage.mjs';
import {migrateNotes} from '../../modules/notes/server/schema.mjs';
import {migrateAppsSchema} from '../../modules/apps/server/schema.mjs';
import {createHistoricalNotesV1} from './notes-v1.fixture.mjs';
import {currentStorageReaders,storageReaderLabel,storageReaders,assertStorageCompatible,assertStorageReaderManifestCompatible,checkedStorageFormat,requireStorageStartReceipt} from './storage-guard.mjs';
import {readerBridgeReaders,readerBridgePolicy,runReaderBridge,assertReaderBridgeRuntime,assertReaderBridgeOriginal} from './reader-bridge.mjs';
import {buildReaderBridgeContext} from './build-reader-bridge.mjs';

const sha=value=>createHash('sha256').update(value).digest('hex');
const image=label=>({Id:'sha256:'+'a'.repeat(64),Config:{Labels:{[storageReaderLabel]:label}}});
const empty=()=>({ok:true,schema:'soty.storage-format.v10',...Object.fromEntries(Object.keys(JSON.parse(currentStorageReaders).readers).map(key=>[key,'empty']))});
async function fixture(t){
  const root=await mkdtemp(path.join(tmpdir(),'soty-reader-bridge-test-')),data=path.join(root,'data'),temp=path.join(root,'private');
  await mkdir(data);await mkdir(temp);
  t.after(async()=>{assert.equal(path.dirname(root),path.resolve(tmpdir()));assert.match(path.basename(root),/^soty-reader-bridge-test-/u);await rm(root,{recursive:true,force:true});});
  return{root,data,temp};
}
async function store(f,directory,name,initialize){await mkdir(path.join(f.data,directory),{recursive:true});const file=path.join(f.data,directory,name),db=new DatabaseSync(file);try{initialize(db);}finally{db.close();}return file;}
async function rejectedBeforeOriginal(f,code){let starts=0;await assert.rejects(runReaderBridge({dataDir:f.data,temporaryParent:f.temp,loadOriginal:async()=>{starts++;}}),{code});assert.equal(starts,0);assert.deepEqual(await readdir(f.temp),[]);}

test('manifest11 explicitly supports absent-only [] without changing current10 or historical strict arrays',()=>{
  const current=JSON.parse(currentStorageReaders),bridge=JSON.parse(readerBridgeReaders);
  assert.equal(current.version,10);assert.equal(bridge.version,11);assert.equal(Object.keys(bridge.readers).length,21);
  assert.deepEqual(storageReaders(image(readerBridgeReaders)),bridge.readers);assert.deepEqual(bridge.readers.notes,[1,2]);
  assert.deepEqual(bridge.readers.appsEndpoint,[]);assert.deepEqual(bridge.readers.appsFrameContinuity,[]);
  const counts={3:4,4:6,5:7,6:8,7:9,8:19,9:20,10:21};
  for(const version of[3,4,5,6,7,8,9,10]){
    const readers=Object.fromEntries(Object.entries(current.readers).slice(0,counts[version]).map(([name,values])=>[name,values.filter(n=>!(name==='apps'&&version<7&&n>11)&&!(name==='notes'&&version<8&&n>2)&&!(name==='world'&&version<10&&n>7))]));
    const declaration={version,readers};storageReaders(image(JSON.stringify(declaration)));
    for(const name of Object.keys(readers)){const changed=structuredClone(declaration);changed.readers[name]=[];assert.throws(()=>storageReaders(image(JSON.stringify(changed))),{code:'storage_reader_unknown'},'manifest'+version+' must still reject empty '+name);}
  }
});
test('manifest11 retains exact keys, known formats, types and duplicate refusal; only version12 remains future',()=>{
  const bridge=JSON.parse(readerBridgeReaders);
  const variants=[{...bridge,version:12},{...bridge,extra:true},{...bridge,readers:{...bridge.readers,extra:[]}}];
  for(const value of[null,'1',[[1]],['1'],[99],[1,1]])variants.push({...bridge,readers:{...bridge.readers,monitoring:value}});
  const missing=structuredClone(bridge);delete missing.readers.appsEndpoint;variants.push(missing);
  for(const value of variants)assert.throws(()=>storageReaders(image(JSON.stringify(value))),{code:'storage_reader_unknown'});
});
test('partial declaration admits empty unsupported overlays and genuine separate-store formats, with v10 observations only',()=>{
  const format={...empty(),canvasObjects:2,canvasWidgets:1,canvasVault:2,canvasPages:2,materials:2,invitations:2,referrals:1,appAgentDirectory:1,universalPublication:1,monitoring:1};
  assert.deepEqual(assertStorageCompatible(image(readerBridgeReaders),format),format);
  assert.deepEqual(assertStorageReaderManifestCompatible(readerBridgeReaders,format),format);
  assert.throws(()=>checkedStorageFormat({...format,schema:'soty.storage-format.v11'}),{code:'storage_probe_invalid'});
  assert.throws(()=>requireStorageStartReceipt({...format,schema:'soty.storage-start.v11'},'a'.repeat(64)),{code:'storage_start_guard_missing'});
  for(const [name,values]of[['notes',[3]],['appsEndpoint',[1]],['appsFrameContinuity',[1,2]]])for(const value of values)assert.throws(()=>assertStorageCompatible(image(readerBridgeReaders),{...empty(),[name]:value}),{code:'storage_reader_incompatible'});
});
test('a genuine separate monitoring/widget store is read from a private snapshot and removed before original import',async t=>{
  const f=await fixture(t),monitoring=await store(f,'monitoring','monitoring.sqlite',db=>{db.exec(MONITORING_DDL);db.prepare('INSERT INTO monitoring_meta VALUES(1,?,?,?)').run('soty','soty.monitoring.sqlite.v1','aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa');});
  const widget=await store(f,'canvas','widgets.sqlite',db=>initializeWidgetStorage(db,'soty'));
  const before=await Promise.all([monitoring,widget].map(file=>readFile(file)));let starts=0;
  const format=await runReaderBridge({dataDir:f.data,temporaryParent:f.temp,loadOriginal:async()=>{assert.deepEqual(await readdir(f.temp),[]);assert.deepEqual(await Promise.all([monitoring,widget].map(file=>readFile(file))),before);starts++;}});
  assert.equal(format.monitoring,1);assert.equal(format.canvasWidgets,1);assert.equal(format.appsEndpoint,'empty');assert.equal(starts,1);
});
test('genuine Notes3 must refuse before the original body and preserve source bytes',async t=>{
  const f=await fixture(t),file=await store(f,'notes','notes.sqlite',db=>migrateNotes(db,'soty',{allowNativeMigration:true,allowVaultMigration:true})),before=await readFile(file);
  await rejectedBeforeOriginal(f,'storage_reader_incompatible');assert.deepEqual(await readFile(file),before);
});
test('genuine endpoint/frame overlays must refuse before the original body even when the underlying apps family is known',async t=>{
  const f=await fixture(t),file=await store(f,'apps','registry.sqlite',db=>migrateAppsSchema(db,{legacyTemplate:'http://{appId}.localhost',allowSelfIpTargets:true})),before=await readFile(file);
  await rejectedBeforeOriginal(f,'storage_reader_incompatible');assert.deepEqual(await readFile(file),before);
});
for(const [name,mutation,code]of[['unknown version','PRAGMA user_version=99','storage_format_unknown'],['unrecognized schema','CREATE TABLE bypass(data)','storage_format_unreadable']])test('genuine new-family '+name+' refuses before original import',async t=>{
  const f=await fixture(t),file=await store(f,'monitoring','monitoring.sqlite',db=>{db.exec(MONITORING_DDL);db.prepare('INSERT INTO monitoring_meta VALUES(1,?,?,?)').run('soty','soty.monitoring.sqlite.v1','aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa');db.exec(mutation);}),before=await readFile(file);
  await rejectedBeforeOriginal(f,code);assert.deepEqual(await readFile(file),before);
});
test('unknown committed WAL cannot be hidden by a supported main header and creates no SHM in source',async t=>{
  const f=await fixture(t),file=await store(f,'notes','notes.sqlite',createHistoricalNotesV1);
  const script=`import{DatabaseSync}from'node:sqlite';const db=new DatabaseSync(${JSON.stringify(file)});db.exec('PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;PRAGMA user_version=99');process.stdout.write('ready');setInterval(()=>{},1000);`;
  const child=spawn(process.execPath,['--input-type=module','-e',script],{stdio:['ignore','pipe','pipe'],windowsHide:true});t.after(()=>{if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');});
  const ready=await Promise.race([once(child.stdout,'data'),once(child,'exit').then(()=>{throw Error('WAL fixture exited');})]);assert.equal(String(ready[0]),'ready');const ended=once(child,'exit');child.kill('SIGKILL');await ended;await unlink(file+'-shm');
  const before=await Promise.all([file,file+'-wal'].map(name=>readFile(name)));assert.equal(before[0].readUInt32BE(60),1);
  await rejectedBeforeOriginal(f,'storage_format_unknown');assert.deepEqual(await Promise.all([file,file+'-wal'].map(name=>readFile(name))),before);await assert.rejects(stat(file+'-shm'),{code:'ENOENT'});
});
test('private snapshot cannot overlap DATA, and a runtime or preserved-entry pin mismatch fails closed',async t=>{
  const f=await fixture(t);await assert.rejects(runReaderBridge({dataDir:f.data,temporaryParent:f.data,loadOriginal:async()=>assert.fail('must not start')}),{code:'reader_bridge_directory_invalid'});
  assert.throws(()=>assertReaderBridgeRuntime('24.15.0',Buffer.from('different executable')),{code:'reader_bridge_runtime_invalid'});
  assert.throws(()=>assertReaderBridgeRuntime('24.22.1',Buffer.from('different executable')),{code:'reader_bridge_runtime_invalid'});
  assert.throws(()=>assertReaderBridgeOriginal(Buffer.alloc(readerBridgePolicy.originalEntryBytes)),{code:'reader_bridge_original_invalid'});
});
test('existing esbuild creates a pinned node-only context, leaves runtime directives inherited and refuses an unapproved external package',async t=>{
  const f=await fixture(t),sourceRoot=fileURLToPath(new URL('../../',import.meta.url));
  const dependencyRoot=process.env.SOTY_READER_BRIDGE_TEST_DEPENDENCY_ROOT||sourceRoot;
  const context=await buildReaderBridgeContext({sourceRoot,dependencyRoot,outputDirectory:path.join(f.root,'build'),baseImageTag:'soty-reader-bridge-base:c5821dd1'});
  assert.equal(context.baseImage,readerBridgePolicy.baseImage);assert.equal(context.nodeOnlyClosure,true);assert.ok(context.externalImports.every(name=>name.startsWith('node:')));
  assert.equal(sha(await readFile(path.join(f.root,'build','reader-bridge.bundle.mjs'))),context.bundle.sha256);
  const imported=spawnSync(process.execPath,['--input-type=module','-e',`await import(${JSON.stringify(pathToFileURL(path.join(f.root,'build','reader-bridge.bundle.mjs')).href)})`],
    {env:{...process.env,SOTY_WORLD_FIELD_PROBE:'0'},encoding:'utf8',windowsHide:true});
  assert.equal(imported.status,0,imported.stderr); // The shipped bundle has no adjacent operator source files.
  const dockerfile=await readFile(path.join(f.root,'build','Dockerfile'),'utf8');assert.ok(!dockerfile.split('\n').some(line=>/^(CMD|ENTRYPOINT|USER|ENV|VOLUME)\s/iu.test(line)));
  assert.ok(dockerfile.includes(context.bundle.sha256));assert.ok(dockerfile.includes(readerBridgePolicy.originalEntrySha256));assert.ok(dockerfile.includes('COPYFILE_EXCL'));
  assert.ok(dockerfile.includes('ARG REVISION\n'));assert.ok(dockerfile.includes('LABEL org.opencontainers.image.revision=${REVISION}'));
  const buildRun=dockerfile.split('\n').find(line=>line.startsWith('RUN '));assert.ok(buildRun.startsWith('RUN --network=none node '));
  const buildCode=buildRun.slice(buildRun.indexOf("-e '")+4,-1);
  for(const revision of['','too-short','A'.repeat(40),'g'.repeat(40),'a'.repeat(40)+'\n']){
    const refused=spawnSync(process.execPath,['--input-type=module','-e',buildCode],{env:{...process.env,REVISION:revision},encoding:'utf8',windowsHide:true});
    assert.equal(refused.status,1);assert.match(refused.stderr,/^Error: reader_bridge_revision_invalid\r?$/mu);assert.doesNotMatch(refused.stderr,/^Error: reader_bridge_build_pin_invalid\r?$/mu);
  }
  const reader=JSON.parse(dockerfile.split('\n').find(line=>line.startsWith('LABEL io.soty.storage.readers=')).slice('LABEL io.soty.storage.readers='.length));assert.equal(reader,readerBridgeReaders);
  const malicious=path.join(f.root,'malicious');await mkdir(path.join(malicious,'deploy','connector'),{recursive:true});await writeFile(path.join(malicious,'deploy','connector','reader-bridge.mjs'),"import 'unapproved-package';export const value=1;");
  await assert.rejects(buildReaderBridgeContext({sourceRoot:malicious,dependencyRoot,outputDirectory:path.join(f.root,'bad-build'),baseImageTag:'soty-reader-bridge-base:c5821dd1'}),{code:'reader_bridge_external_dependency'});
});
