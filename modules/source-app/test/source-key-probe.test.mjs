import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {createOrdinaryAppStore} from '../examples/ordinary-app/store.mjs';
import {probeInstalledSourceKey} from '../install/key-probe.mjs';

test('existing actual Interaction/NativeLoginProof ciphertext denies wrong key/AAD/KeyId before Source listener without writes',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'source-key-probe-')),databasePath=join(directory,'native.sqlite'),key=randomBytes(32),realmId='probe';
  t.after(()=>rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30}));
  const store=createOrdinaryAppStore({databasePath,realmId,key,keyId:'original',initialize:true,format:3});
  await store.storage.createInteraction({idHash:'synthetic-interaction',revision:1,phase:'pending',expiresAt:Date.now()+10000,synthetic:true});
  store.tx(()=>store.db.prepare('INSERT INTO native_login_proofs VALUES(?,?,?,?,?,?)').run('synthetic-proof','synthetic-interaction','a'.repeat(64),Date.now()+10000,
    store.encrypt('NativeLoginProof','synthetic-proof',0,{synthetic:true}), 'original'));
  store.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');store.close();const before=await readFile(databasePath);
  const input={databasePath,realmId,keyId:'original',cipherKey:key};const result=probeInstalledSourceKey(input);assert.equal(result.keyCorrectness,'proved');assert.equal(result.checkedModels,2);assert.equal(result.permissionProved,false);
  for(const change of [{cipherKey:randomBytes(32)},{keyId:'wrong'},{realmId:'foreign'}])assert.throws(()=>probeInstalledSourceKey({...input,...change}));
  assert.deepEqual(await readFile(databasePath),before,'readonly probe did not modify Native bytes');
});
test('empty encrypted state allows explicit bootstrap with key correctness unproved; malformed key never passes',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'source-key-empty-')),databasePath=join(directory,'native.sqlite'),cipherKey=randomBytes(32);
  t.after(()=>rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30}));
  const store=createOrdinaryAppStore({databasePath,realmId:'empty',key:cipherKey,keyId:'only',initialize:true,format:3});store.close();
  const input={databasePath,realmId:'empty',cipherKey,keyId:'only'};assert.equal(probeInstalledSourceKey(input).keyCorrectness,'unproved_empty');
  assert.throws(()=>probeInstalledSourceKey({...input,cipherKey:Buffer.alloc(31)}));
});
