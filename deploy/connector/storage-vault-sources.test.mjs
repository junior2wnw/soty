import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import {mkdtemp,mkdir,readFile,rm,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,basename,resolve} from 'node:path';
import {migrateNotes} from '../../modules/notes/server/schema.mjs';
import {inspectNotesSchema as inspectNotesReader2} from '../../modules/notes/server/schema-v2.mjs';
import {openMaterialSchema} from '../../modules/materials/server/schema.mjs';
import {initializeVaultStorage} from '../../modules/canvas-vault/storage.mjs';
import {initializeVaultStorage as oldVaultReader} from '../../modules/canvas-vault/test/support/storage-v1-before-conversion.mjs';
import {prepareMaterialUpload,uploadMaterial} from '../../modules/materials/browser/client.mjs';
import {pngFixture} from '../../modules/world/test/avatar-fixtures.mjs';
import {createHttpApp} from '../../server/http-app.js';
import {nativeHttpFixture,nativeIdentity,good} from '../../server/test/support/native-capability-http.mjs';
import {readStorageFormat} from './storage-probe.mjs';
import {assertStorageCompatible,checkedStorageFormat,currentStorageReaders,storageReaderLabel,storageReaders} from './storage-guard.mjs';
import {snapshotStorage} from './storage-snapshot.mjs';
import {currentObservation} from './test-support/current-storage-observation.mjs';

const image=declaration=>({Id:'sha256:'+'a'.repeat(64),Config:{Labels:{[storageReaderLabel]:JSON.stringify(declaration)}}});
const current=()=>JSON.parse(currentStorageReaders);
const freshDirectory=async t=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-vault-source-probe-'));
  t.after(()=>{assert.equal(dirname(resolve(directory)),resolve(tmpdir()));assert.ok(basename(directory).startsWith('soty-vault-source-probe-'));return rm(directory,{recursive:true,force:true});});
  return directory;
};

test('Notes3 and Materials2 keep registry identity, have exact read-only probe formats and survive an offline snapshot',async t=>{
  const directory=await freshDirectory(t);
  for(const [store,filename,open,previous,expected]of[
    ['notes','notes.sqlite',migrateNotes,2,3],['materials','materials.sqlite',openMaterialSchema,1,2],
  ]){
    await mkdir(join(directory,store));const db=new DatabaseSync(join(directory,store,filename));
    try{
      const before=open(db,'soty',store==='notes'?{allowNativeMigration:true}:{});assert.equal(before.schemaVersion,previous);
      assert.equal(open(db,'soty').schemaVersion,previous,'default-off opener must not migrate existing data');
      const after=open(db,'soty',{allowVaultMigration:true});assert.equal(after.schemaVersion,expected);assert.equal(after.registryId,before.registryId);
      assert.equal(open(db,'soty').schemaVersion,expected,'the new reader can reopen its explicitly migrated store');
      if(store==='notes')assert.throws(()=>inspectNotesReader2(db,'soty'),{code:'notes_schema_unsupported'},'actual preceding Notes reader must reject format3');
    }finally{db.close();}
  }
  const files=[join(directory,'notes','notes.sqlite'),join(directory,'materials','materials.sqlite')],before=await Promise.all(files.map(file=>readFile(file)));
  const observed=await readStorageFormat(directory);assert.deepEqual(observed,currentObservation({notes:3,materials:2}));
  assert.deepEqual(await Promise.all(files.map(file=>readFile(file))),before,'host probe must not migrate or rewrite either Source');
  assertStorageCompatible(image(current()),observed);
  const snapshotParent=await freshDirectory(t),destination=join(snapshotParent,'snapshot');await snapshotStorage(directory,destination);
  assert.deepEqual(await readStorageFormat(destination),observed);
});

test('native sealing reader capabilities are explicit per store; historical DTOs and labels cannot claim Notes3',()=>{
  for(const [store,version]of[['notes',3],['materials',2],['canvasVault',2]]){
    const old=current();old.readers[store]=old.readers[store].filter(value=>value!==version);
    assert.throws(()=>assertStorageCompatible(image(old),currentObservation({[store]:version})),{code:'storage_reader_incompatible'});
  }
  const historical=current();historical.version=7;historical.readers.world=[6,7];
  for(const store of['canvasObjects','canvasWidgets','canvasVault','canvasPages','materials','invitations','referrals','appAgentDirectory','universalPublication','appsEndpoint','appsFrameContinuity','monitoring'])delete historical.readers[store];
  assert.throws(()=>storageReaders(image(historical)),{code:'storage_reader_unknown'},'historical epoch7 must not be relabelled with the new Notes3 reader');
  historical.readers.notes=[1,2];assert.deepEqual(storageReaders(image(historical)).notes,[1,2]);
  const {canvasObjects,canvasWidgets,canvasVault,canvasPages,materials,invitations,referrals,appAgentDirectory,universalPublication,appsEndpoint,appsFrameContinuity,monitoring,...oldFormat}=currentObservation({notes:3});
  oldFormat.schema='soty.storage-format.v7';assert.throws(()=>checkedStorageFormat(oldFormat),{code:'storage_probe_invalid'});
});

test('Vault2 is explicitly migrated, default-off reopens it, and the literal former reader refuses the committed schema',async t=>{
  const directory=await freshDirectory(t);await mkdir(join(directory,'canvas'));const db=new DatabaseSync(join(directory,'canvas','vault.sqlite'));
  try{
    assert.equal(initializeVaultStorage(db,'soty').schemaVersion,1);assert.equal((await readStorageFormat(directory)).canvasVault,1);
    assert.equal(initializeVaultStorage(db,'soty',{allowFormat2Migration:true}).schemaVersion,2);
    assert.throws(()=>oldVaultReader(db,'soty'),{code:'vault_schema_unsupported'});
    assert.equal(initializeVaultStorage(db,'soty').schemaVersion,2);assert.equal((await readStorageFormat(directory)).canvasVault,2);
  }finally{db.close();}
});

for(const [name,mutate,code]of[
  ['future version',db=>db.exec('PRAGMA user_version=99'),'storage_format_unknown'],
  ['unknown table',db=>db.exec('CREATE TABLE unreviewed_extra(data TEXT)'),'storage_format_unreadable'],
  ['changed header column',db=>db.exec('ALTER TABLE vault_uploads RENAME COLUMN chunk_bytes TO unknown_chunk_bytes'),'storage_format_unreadable'],
  ['missing cipher chunk guard',db=>db.exec('DROP TRIGGER vault_cipher_chunk_replace'),'storage_format_unreadable'],
  ['missing conversion intent guard',db=>db.exec('DROP TRIGGER vault_conversion_identity'),'storage_format_unreadable'],
  ['missing conversion command guard',db=>db.exec('DROP TRIGGER vault_conversion_command_delete'),'storage_format_unreadable'],
])test(`Vault2 exact schema refuses ${name}`,async t=>{
  const directory=await freshDirectory(t);await mkdir(join(directory,'canvas'));const db=new DatabaseSync(join(directory,'canvas','vault.sqlite'));
  try{initializeVaultStorage(db,'soty',{allowFormat2Migration:true});assert.equal((await readStorageFormat(directory)).canvasVault,2);mutate(db);await assert.rejects(readStorageFormat(directory),{code});}
  finally{db.close();}
});

test('actual isolated Root migrates populated native stores, seals notes/image into Vault2 chunks, reopens flag-off and retains exact offline snapshot/floor',{timeout:30000},async t=>{
  const factory=enabled=>(dist,options)=>createHttpApp(dist,{...options,allowNativeVaultMigration:enabled});
  const f=await nativeHttpFixture(t,{enabled:false,appFactory:factory(false)}),owner=nativeIdentity('Storage Vault synthetic owner'),account=await f.bootstrap(owner);
  let sequence=0;const call=(op,args={})=>f.call(owner,op,{expectedAccountId:account.accountId,...args});
  const mutate=(op,args={})=>call(op,{requestId:'storage_vault_request_'+String(++sequence).padStart(4,'0'),...args});
  const noteId='storage_vault_native_note',body='Synthetic native note for the bounded storage migration gate';
  good(await call('notes.put',{noteId,mutationId:'storage_vault_note_create',expectedRevision:0,title:'Storage gate',body,items:[],color:'plain',pinned:false,state:'active'}));
  const png=pngFixture(3,2),material=await uploadMaterial({request:async(op,args)=>good(await f.call(owner,op,args))},{accountId:account.accountId,isCurrent:()=>true},await prepareMaterialUpload(new Blob([png],{type:'image/png'}),{kind:'image',name:'synthetic-storage.png'}));
  const object=async(providerId,ref,protectedSource=false)=>good(await mutate('canvas.objects.create',{binding:{providerId,ref},presentation:{mask:{shape:'rect'}},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:protectedSource?'password':'none'}})).current;
  const noteObject=await object('soty.notes',noteId),imageObject=await object('soty.materials',material.materialId),password='Synthetic-vault-storage-5297';
  const legacyContent=Buffer.from(JSON.stringify({title:'Legacy ciphertext survives',body:'Synthetic previous Vault1 content'}));
  const legacy=good(await mutate('canvas.vault.create',{password,content:{contentType:'application/vnd.soty.note+json',bytesBase64:legacyContent.toString('base64url')}}));
  const legacyObject=await object('soty.vault',legacy.current.vaultId,true);
  const before=await readStorageFormat(f.directory);assert.equal(before.notes,2);assert.equal(before.materials,1);assert.equal(before.canvasVault,1);
  const identities=()=>Object.fromEntries([['notes','notes/notes.sqlite','notes_meta'],['materials','materials/materials.sqlite','material_meta']].map(([key,path,table])=>[key,f.sql(join(f.directory,path),db=>db.prepare(`SELECT value FROM ${table} WHERE key='registry_id'`).get().value)]));
  const originalIdentity=identities();await f.restart({enabled:false,appFactory:factory(true)});
  const migrated=await readStorageFormat(f.directory);assert.equal(migrated.notes,3);assert.equal(migrated.materials,2);assert.equal(migrated.canvasVault,2);assert.deepEqual(identities(),originalIdentity);
  assertStorageCompatible(image(current()),migrated);const previous=current();previous.readers.canvasVault=[1];assert.throws(()=>assertStorageCompatible(image(previous),migrated),{code:'storage_reader_incompatible'});
  for(const currentObject of[noteObject,imageObject]){const result=good(await mutate('canvas.vault.convert',{objectId:currentObject.objectId,expectedObjectRevision:currentObject.revision,password}));assert.equal(result.protected,true,JSON.stringify(result));}
  assert.equal((await call('notes.get',{noteId})).error.code,'notes_note_sealed');assert.equal((await call('materials.get',{materialId:material.materialId})).error.code,'materials_material_sealed');
  const vaultFile=join(f.directory,'canvas','vault.sqlite'),counts=f.sql(vaultFile,db=>({sources:db.prepare('SELECT count(*) n FROM vault_sources').get().n,chunks:db.prepare('SELECT count(*) n FROM vault_cipher_chunks').get().n,conversions:db.prepare("SELECT count(*) n FROM vault_conversions WHERE state='active'").get().n,commands:db.prepare('SELECT count(*) n FROM vault_conversion_commands').get().n,encryptedPlainBytes:db.prepare('SELECT sum(size) n FROM vault_cipher_chunks').get().n,cipherJsonBytes:db.prepare('SELECT sum(length(cipher_json)) n FROM vault_cipher_chunks').get().n,headerJsonBytes:db.prepare('SELECT sum(length(container_json)) n FROM vault_sources').get().n}));
  assert.equal(counts.sources,3);assert.equal(counts.chunks,2);assert.equal(counts.conversions,2);assert.equal(counts.commands,2);
  await f.restart({enabled:false,appFactory:factory(false)});assert.deepEqual(await readStorageFormat(f.directory),migrated);
  for(const currentObject of[noteObject,imageObject,legacyObject])good(await call('canvas.vault.unlock',{objectId:currentObject.objectId,password}));
  const note=good(await call('canvas.objects.content',{objectId:noteObject.objectId,part:'vault'}));assert.equal(JSON.parse(Buffer.from(note.bytesBase64,'base64url')).body,body);
  const imageContent=good(await call('canvas.objects.content',{objectId:imageObject.objectId,part:'vault'}));assert.deepEqual(Buffer.from(imageContent.bytesBase64,'base64url'),png);
  const legacyRead=good(await call('canvas.objects.content',{objectId:legacyObject.objectId,part:'vault'}));assert.deepEqual(Buffer.from(legacyRead.bytesBase64,'base64url'),legacyContent);
  await f.app.locals.closeServices();const snapshotParent=await freshDirectory(t),destination=join(snapshotParent,'snapshot');await snapshotStorage(f.directory,destination);assert.deepEqual(await readStorageFormat(destination),migrated);
  const sourceBytes={},snapshotBytes={};for(const path of['notes/notes.sqlite','materials/materials.sqlite','canvas/vault.sqlite']){sourceBytes[path]=(await stat(join(f.directory,path))).size;snapshotBytes[path]=(await stat(join(destination,path))).size;assert.equal(snapshotBytes[path],sourceBytes[path]);
    const digest=bytes=>createHash('sha256').update(bytes).digest('hex');assert.equal(digest(await readFile(join(destination,path))),digest(await readFile(join(f.directory,path))),'offline snapshot preserves actual source/cipher bytes: '+path);}
  await f.restart({enabled:false,appFactory:(dist,options)=>createHttpApp(dist,{...options,dataDir:destination,allowNativeVaultMigration:false})});
  for(const currentObject of[noteObject,imageObject,legacyObject])good(await call('canvas.vault.unlock',{objectId:currentObject.objectId,password}));
  assert.equal(JSON.parse(Buffer.from(good(await call('canvas.objects.content',{objectId:noteObject.objectId,part:'vault'})).bytesBase64,'base64url')).body,body);
  assert.deepEqual(Buffer.from(good(await call('canvas.objects.content',{objectId:imageObject.objectId,part:'vault'})).bytesBase64,'base64url'),png);
  assert.deepEqual(Buffer.from(good(await call('canvas.objects.content',{objectId:legacyObject.objectId,part:'vault'})).bytesBase64,'base64url'),legacyContent);
  t.diagnostic(JSON.stringify({gate:'Native3/Materials2/Vault2 isolated Root',noteBodyBytes:Buffer.byteLength(body),imageBytes:png.length,legacyVaultPlainBytes:legacyContent.length,...counts,sourceBytes,snapshotBytes,limits:{fileBytes:25*1024*1024,chunkBytes:512*1024,exercisedScale:'small fixture only'}}));
});

for(const [store,filename,open,version,guard]of[
  ['notes','notes.sqlite',migrateNotes,3,'note_vault_seal_immutable'],
  ['materials','materials.sqlite',openMaterialSchema,2,'material_vault_seal_immutable'],
])test(`${store}${version}: unknown version, missing seal guard and extra schema are denied`,async t=>{
  const directory=await freshDirectory(t);await mkdir(join(directory,store));const db=new DatabaseSync(join(directory,store,filename));
  try{
    open(db,'soty',{allowVaultMigration:true});assert.equal((await readStorageFormat(directory))[store],version);
    db.exec('PRAGMA user_version=99');await assert.rejects(readStorageFormat(directory),{code:'storage_format_unknown'});db.exec(`PRAGMA user_version=${version}`);
    const sql=db.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(guard).sql;db.exec(`DROP TRIGGER ${guard}`);
    await assert.rejects(readStorageFormat(directory),{code:'storage_format_unreadable'},'the probe must detect a removed immutable seal guard');db.exec(sql);
    db.exec('CREATE TABLE unreviewed_extra(data TEXT)');await assert.rejects(readStorageFormat(directory),{code:'storage_format_unreadable'});db.exec('DROP TABLE unreviewed_extra');
    assert.equal((await readStorageFormat(directory))[store],version);
  }finally{db.close();}
});
