import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { randomBytes } from 'node:crypto';
import { mkdtemp,rm,copyFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createOrdinaryAppStore } from '../examples/ordinary-app/store.mjs';
import { readOrdinaryFormat2 } from '../examples/ordinary-app/reader2.mjs';
import { readOrdinaryFormat3 } from '../examples/ordinary-app/reader3.mjs';

const snapshot=db=>Object.fromEntries(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'native_meta' ORDER BY name").all().map(({name})=>[name,db.prepare('SELECT * FROM '+name+' ORDER BY rowid').all()]));
test('explicit Native2→3 reader-before-write keeps every old row/receipt/cipher/FK; frozen independent reader2 refuses3; compatible baseline/cold restart',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-native3-reader-'));t.after(()=>rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30}));
  const options={databasePath:join(directory,'native.sqlite'),realmId:'board',key:randomBytes(32),keyId:'fixture',initialize:true,format:2};
  let store=createOrdinaryAppStore(options);store.createResource({id:'selected',incarnationId:'one',title:'Retained Native resource'});store.createPrincipal('owner');store.grant('selected','owner','owner');store.createNativeSession('owner');
  await store.storage.createInteraction({idHash:'a'.repeat(64),revision:0,phase:'pending',context:{synthetic:true},createdAt:Date.now(),expiresAt:Date.now()+300000});
  store.tx(()=>store.db.prepare('INSERT INTO native_receipts VALUES(?,?,?,?,?)').run('selected','owner','old-receipt-0001','b'.repeat(64),'{}'));
  const before=snapshot(store.db);store.close();assert.equal(readOrdinaryFormat3(options.databasePath,'board').format,2);
  assert.throws(()=>createOrdinaryAppStore({...options,initialize:false,format:3}),error=>error.code==='ordinary_source_jobs_migration_required');assert.equal(readOrdinaryFormat2(options.databasePath,'board').format,2);
  store=createOrdinaryAppStore({...options,initialize:false,format:3,allowFeedbackJobsMigration:true});const after=snapshot(store.db);
  for(const [name,rows]of Object.entries(before))assert.deepEqual(after[name],rows);assert.equal(store.db.prepare('PRAGMA foreign_key_check').all().length,0);
  assert.equal(readOrdinaryFormat3(options.databasePath,'board').objects,29);store.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');store.close();
  assert.throws(()=>readOrdinaryFormat2(options.databasePath,'board'),error=>error.code==='ordinary_source_reader_refused');await copyFile(options.databasePath,join(directory,'cold.sqlite'));
  store=createOrdinaryAppStore({...options,databasePath:join(directory,'cold.sqlite'),initialize:false,format:2});assert.equal(store.format,3);
  const cold=snapshot(store.db);for(const [name,rows]of Object.entries(before))assert.deepEqual(cold[name],rows);store.close();
  const db=new DatabaseSync(options.databasePath);db.exec('DROP TRIGGER native_processor_grant_immutable');db.close();
  assert.throws(()=>createOrdinaryAppStore({...options,initialize:false,format:3}),error=>error.code==='ordinary_source_reader_refused');
});
