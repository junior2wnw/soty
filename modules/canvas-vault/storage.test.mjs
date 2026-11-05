import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {initializeVaultStorage} from './storage.mjs';
import {initializeVaultStorage as oldVaultReader} from './test/support/storage-v1-before-conversion.mjs';
import {createCanvasVaultService} from './server.mjs';
import {createNotesService} from '../notes/server/index.mjs';
import {migrateNotes,inspectNotesSchema} from '../notes/server/schema-vault.mjs';
import {createMaterialsService} from '../materials/server/index.mjs';
import {openMaterialSchema} from '../materials/server/schema.mjs';
const projectId='native_format_migration',actor={accountId:'format_owner',deviceId:'format_device'};
const code=value=>error=>{assert.equal(error.code,value);return true;};
function fixture(t){const base=realpathSync(tmpdir()),directory=mkdtempSync(join(base,'soty-vault-formats-')),handles=new Set(),closers=[];
 const open=name=>{const db=new DatabaseSync(join(directory,name+'.sqlite'));handles.add(db);return db;};
 const close=db=>{handles.delete(db);db.close();};
 t.after(async()=>{for(const close of closers)await close();for(const db of handles)db.close();assert.equal(dirname(realpathSync(directory)).toLowerCase(),base.toLowerCase());assert.ok(basename(directory).startsWith('soty-vault-formats-'));rmSync(directory,{recursive:true,force:true});});return {directory,open,close,onClose:callback=>closers.push(callback)};
}
const snapshot=db=>({version:db.prepare('PRAGMA user_version').get().user_version,layout:db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row=>({...row})),
 rows:db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' AND name NOT GLOB '*_fts*' ORDER BY name").all().map(({name})=>[name,db.prepare('SELECT * FROM '+name).all().map(row=>({...row}))])});
function faultBeforeCommit(db){return {prepare:db.prepare.bind(db),get isTransaction(){return db.isTransaction;},exec(sql){if(/\bCOMMIT\b/u.test(sql))throw new Error('format_commit_fault');return db.exec(sql);}};}

test('explicit Vault1→2 is atomic, keeps ciphertext/receipts unchanged and the literal old reader refuses its committed schema',t=>{
 const f=fixture(t),db=f.open('vault');oldVaultReader(db,projectId);db.prepare("INSERT INTO vault_sources VALUES('vault_fixture','format_owner',1,'native',NULL,NULL,?,1,1)").run('{"ciphertext":"opaque-fixture"}');db.prepare('INSERT INTO vault_commands VALUES(?,?,?,?,?)').run(actor.accountId,'format_create','canvas.vault.create','opaque-hmac','{"vaultId":"vault_fixture","revision":1}');
 const before=snapshot(db);assert.deepEqual(initializeVaultStorage(db,projectId),{schemaVersion:1});assert.deepEqual(snapshot(db),before,'default open never migrates the accepted source format');
 assert.throws(()=>initializeVaultStorage(faultBeforeCommit(db),projectId,{allowFormat2Migration:true}),/format_commit_fault/u);assert.deepEqual(snapshot(db),before,'DDL, reader epoch and existing rows roll back together');
 assert.deepEqual(initializeVaultStorage(db,projectId,{allowFormat2Migration:true}),{schemaVersion:2});assert.equal(db.prepare('SELECT container_json FROM vault_sources').get().container_json,'{"ciphertext":"opaque-fixture"}');assert.equal(db.prepare('SELECT intent FROM vault_commands').get().intent,'opaque-hmac');
 const upgraded=snapshot(db);assert.throws(()=>oldVaultReader(db,projectId),code('vault_schema_unsupported'));assert.deepEqual(snapshot(db),upgraded,'old reader does not downgrade the new layout');f.close(db);const reopened=f.open('vault');assert.equal(initializeVaultStorage(reopened,projectId).schemaVersion,2);assert.deepEqual(snapshot(reopened),upgraded);
});

test('Notes1→3 and Materials1→2 preserve the entire historical schema/data on a pre-COMMIT fault',async t=>{
 const f=fixture(t),notes=createNotesService({databasePath:join(f.directory,'notes.sqlite'),projectId});notes.execute({actor,op:'notes.put',args:{expectedAccountId:actor.accountId,noteId:'format_note',mutationId:'format_note_create',expectedRevision:0,title:'Original title',body:'Original source survives failed migration',items:[],color:'plain',pinned:false,state:'active'}});notes.close();
 const noteDb=f.open('notes'),noteBefore=snapshot(noteDb);assert.equal(noteBefore.version,1);assert.throws(()=>migrateNotes(faultBeforeCommit(noteDb),projectId,{allowVaultMigration:true}),/format_commit_fault/u);assert.deepEqual(snapshot(noteDb),noteBefore,'even the intermediate Native identity/format2 must not commit');assert.equal(inspectNotesSchema(noteDb,projectId).schemaVersion,1);assert.equal(migrateNotes(noteDb,projectId,{allowVaultMigration:true}).schemaVersion,3);assert.equal(noteDb.prepare('SELECT body FROM notes').get().body,'Original source survives failed migration');
 const materials=createMaterialsService({databasePath:join(f.directory,'materials.sqlite'),projectId});await materials.close();const matDb=f.open('materials'),matBefore=snapshot(matDb);assert.throws(()=>openMaterialSchema(faultBeforeCommit(matDb),projectId,{allowVaultMigration:true}),/format_commit_fault/u);assert.deepEqual(snapshot(matDb),matBefore);assert.equal(openMaterialSchema(matDb,projectId,{allowVaultMigration:true}).schemaVersion,2);
});

test('already-open upgraded readers fail closed after another process advances the store epoch, before old reads or writes',async t=>{
 const f=fixture(t),notePath=join(f.directory,'notes.sqlite'),matPath=join(f.directory,'materials.sqlite'),vaultPath=join(f.directory,'vault.sqlite');
 const notes=createNotesService({databasePath:notePath,projectId,allowNativeMigration:true}),materials=createMaterialsService({databasePath:matPath,projectId}),vault=createCanvasVaultService({databasePath:vaultPath,projectId,actorActive:()=>true,withActorAuthorityFence:(_actor,callback)=>callback(),withProtectionTarget:()=>{throw new Error('old_target_must_not_be_read');},withObjectAuthority:()=>{throw new Error('old_object_must_not_be_read');}});
 f.onClose(async()=>{vault.close();notes.close();await materials.close();});
 notes.execute({actor,op:'notes.put',args:{expectedAccountId:actor.accountId,noteId:'stale_note',mutationId:'stale_note_create',expectedRevision:0,title:'Stale source',body:'No old reader bypass',items:[],color:'plain',pinned:false,state:'active'}});
 migrateNotes(f.open('notes'),projectId,{allowVaultMigration:true});openMaterialSchema(f.open('materials'),projectId,{allowVaultMigration:true});initializeVaultStorage(f.open('vault'),projectId,{allowFormat2Migration:true});
 const oracle=read=>assert.throws(read,code('notes_store_format_changed'));oracle(()=>notes.execute({actor,op:'notes.get',args:{expectedAccountId:actor.accountId,noteId:'stale_note'}}));assert.throws(()=>notes.vault.seal(actor.accountId,'stale_note'),code('notes_store_format_changed'));
 assert.throws(()=>oracle(()=>f.open('notes').prepare('SELECT body FROM notes').get()),assert.AssertionError,'guard-disabled old-reader control fails the same oracle');
 await assert.rejects(materials.execute({actor,op:'materials.list',args:{expectedAccountId:actor.accountId}}),code('materials_store_format_changed'));assert.throws(()=>materials.vault.seal(actor.accountId,'old_material'),code('materials_store_format_changed'));
 await assert.rejects(vault.execute({actor,op:'canvas.vault.status',args:{expectedAccountId:actor.accountId,objectId:'old_object'}}),code('vault_store_format_changed'));assert.throws(()=>vault.provider.withBindingAuthority({ref:'old_vault'},()=>{}),code('vault_store_format_changed'));assert.throws(()=>vault.storageIdentity(),code('vault_store_format_changed'));
 assert.equal(notes.challengeStorage,'memory');assert.equal(materials.challengeStorage,'memory');assert.equal(vault.challengeStorage,'memory');assert.throws(()=>{notes.challengeStorage='sqlite';},TypeError);
});
