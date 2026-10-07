import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm, copyFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { createOrdinaryAppStore } from '../examples/ordinary-app/store.mjs';
import { readOrdinaryFormat1 } from '../examples/ordinary-app/reader.mjs';
import { readOrdinaryFormat2 } from '../examples/ordinary-app/reader2.mjs';

const snapshot = db => Object.fromEntries(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'native_meta' ORDER BY name").all()
  .map(({ name }) => [name, db.prepare('SELECT * FROM ' + name + ' ORDER BY rowid').all()]));
test('explicit Native Source1→2 keeps every prior row/cipher/receipt/FK; frozen literal reader1 refuses2 before startup; compatible1,2 cold/restart', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-ordinary-format-')); t.after(() => rm(directory, { recursive:true, force:true, maxRetries:5, retryDelay:30 }));
  const options = { databasePath:join(directory,'source.sqlite'),realmId:'board',key:randomBytes(32),keyId:'fixture',initialize:true };
  let store = createOrdinaryAppStore(options); store.createResource({id:'selected',incarnationId:'one',title:'Preserved selected resource'});
  store.createPrincipal('native-one'); store.grant('selected','native-one','owner'); const token = store.createNativeSession('native-one');
  await store.storage.createInteraction({idHash:'a'.repeat(64),revision:0,phase:'pending',context:{synthetic:true},createdAt:Date.now(),expiresAt:Date.now()+300000});
  store.tx(() => store.db.prepare('INSERT INTO native_receipts VALUES(?,?,?,?,?)').run('selected','native-one','format-receipt-0001','b'.repeat(64),'{"outcome":"committed"}'));
  const before = snapshot(store.db); store.close();
  assert.equal(readOrdinaryFormat2(options.databasePath,'board').format,1);
  assert.throws(() => createOrdinaryAppStore({...options,initialize:false,format:2}), error => error.code==='ordinary_source_migration_required');
  assert.equal(readOrdinaryFormat1(options.databasePath,'board').format,1);
  store = createOrdinaryAppStore({...options,initialize:false,format:2,allowLoginProofMigration:true});
  const after = snapshot(store.db); delete after.native_login_proofs; assert.deepEqual(after,before);
  assert.equal(store.db.prepare('PRAGMA foreign_key_check').all().length,0);
  assert.equal(store.db.prepare('SELECT active FROM native_sessions WHERE id_hash=?').get((await import('../server/wire.mjs')).digest(token)).active,1);
  store.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); store.close();
  assert.throws(() => readOrdinaryFormat1(options.databasePath,'board'), error => error.code==='ordinary_source_reader_refused');
  assert.equal(readOrdinaryFormat2(options.databasePath,'board').format,2);
  await copyFile(options.databasePath,join(directory,'cold.sqlite'));
  store = createOrdinaryAppStore({...options,databasePath:join(directory,'cold.sqlite'),initialize:false,format:2});
  const cold = snapshot(store.db); delete cold.native_login_proofs; assert.deepEqual(cold,before); assert.equal(store.format,2); store.close();
  // The current compatible baseline can still run profile1 on format2.
  store = createOrdinaryAppStore({...options,initialize:false,format:1}); assert.equal(store.format,2); store.close();
  const unknown = new DatabaseSync(options.databasePath); unknown.exec('DROP TRIGGER native_login_proof_immutable'); unknown.close();
  assert.throws(() => createOrdinaryAppStore({...options,initialize:false,format:2}), error => error.code==='ordinary_source_reader_refused');
});
