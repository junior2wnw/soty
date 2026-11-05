import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,rmSync,readFileSync,existsSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createMaterialsService} from '../server/index.mjs';
import {materialManifest} from '../contract.mjs';
import {pngFixture} from '../../world/test/avatar-fixtures.mjs';
import {openMaterialSchema as oldReader} from './support/schema-v1-before-vault.mjs';
const sha=bytes=>createHash('sha256').update(bytes).digest('hex'),actor={accountId:'materials_vault_owner',deviceId:'materials_vault_device'},materialId='material_vault_4719',secretName='material-private-filename-sentinel-812f.png';
const code=value=>error=>{assert.equal(error.code,value);return true;};
async function fixture(t){const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-materials-vault-')),databasePath=join(folder,'materials.sqlite'),proofs=new WeakSet(),changes=[];let materials;
 const open=options=>materials=createMaterialsService({databasePath,projectId:'materials_vault_test',...options,onVaultChange:change=>changes.push(change),verifyVaultCipherReceipt:(proof,callback)=>{assert.ok(proofs.has(proof),'only current host-branded cipher receipt');return callback(proof);}});open();
 t.after(async()=>{await materials.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-materials-vault-'));rmSync(actual,{recursive:true,force:true});});
 const call=(op,args={})=>materials.execute({actor,op:'materials.'+op,args:{expectedAccountId:actor.accountId,...args}}),bytes=pngFixture(),beginArgs={materialId,mutationId:'begin_material_4719',name:secretName,kind:'image',mediaType:'image/png',size:bytes.length,fileSha256:sha(bytes)};
 await call('begin',beginArgs);await call('chunk',{materialId,index:0,sha256:sha(bytes),data:bytes.toString('base64')});await call('commit',{materialId,mutationId:'commit_material_4719',manifestSha256:sha(materialManifest(bytes.length,[{bytes:bytes.length,sha256:sha(bytes)}]))});
 const sql=callback=>{const db=new DatabaseSync(databasePath);try{return callback(db);}finally{db.close();}};
 return {call,bytes,beginArgs,databasePath,sql,changes,get materials(){return materials;},async migrate(){await materials.close();open({allowVaultMigration:true});},async reopen(){await materials.close();open();},proof(){const proof=Object.freeze({claimId:'claim_materials_4719',providerId:'soty.materials',ownerId:actor.accountId,ref:materialId,sourceStoreId:materials.storageIdentity().registryId,sourceRevision:1,vaultId:'vault_materials_4719',cipherDigest:'b'.repeat(64)});proofs.add(proof);return proof;}};
}

test('Materials2 explicit migration keeps source identity/data and old reader fails closed; capture chunk proof expires',async t=>{
 const f=await fixture(t),before=await f.call('get',{materialId}),registry=f.materials.storageIdentity().registryId;assert.throws(()=>f.materials.vault.capture({ownerId:actor.accountId,sourceId:materialId,expectedRevision:1},()=>{}),code('materials_vault_migration_required'));await f.migrate();assert.equal(f.materials.storageIdentity().schemaVersion,2);assert.equal(f.materials.storageIdentity().registryId,registry);assert.deepEqual(await f.call('get',{materialId}),before);
 let captured;const bytes=f.materials.vault.capture({ownerId:actor.accountId,sourceId:materialId,expectedRevision:1},frame=>{captured=frame;assert.equal(frame.metadata.material.name,secretName);return frame.readChunk(0);});assert.deepEqual(bytes,f.bytes);assert.throws(()=>captured.readChunk(0),code('materials_vault_capture_expired'));assert.throws(()=>f.sql(db=>oldReader(db,'materials_vault_test')),code('materials_schema_unsupported'));
});

test('erasure clears real file chunks/filename/hash/receipts before confirmation; old read/get/replay cannot restore plaintext',async t=>{
 const f=await fixture(t);await f.migrate();const proof=f.proof();assert.throws(()=>f.materials.vault.eraseIfUnchanged({cipherReceipt:{...proof}}),/host-branded/u);assert.equal(f.materials.vault.eraseIfUnchanged({cipherReceipt:proof}).state,'pending');assert.equal(f.materials.vault.eraseIfUnchanged({cipherReceipt:proof}).sealedRevision,2);
 await assert.rejects(f.call('get',{materialId}),code('materials_material_sealed'));await assert.rejects(f.call('read',{materialId,index:0}),code('materials_material_sealed'));await assert.rejects(f.call('begin',f.beginArgs),code('materials_material_sealed'));await assert.rejects(f.call('chunk',{materialId,index:0,sha256:sha(f.bytes),data:f.bytes.toString('base64')}),code('materials_material_sealed'));await assert.rejects(f.call('commit',{materialId,mutationId:'commit_material_4719',manifestSha256:sha(materialManifest(f.bytes.length,[{bytes:f.bytes.length,sha256:sha(f.bytes)}]))}),code('materials_material_sealed'));assert.equal((await f.call('status',{materialId})).state,'sealed');assert.equal((await f.call('list')).materials[0].sealed,true);
 assert.equal(f.materials.vault.confirmErasure({claimId:proof.claimId}).state,'confirmed');assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM material_chunks').get().n),0);assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM material_receipts').get().n),0);
 for(const suffix of ['','-wal','-shm'])if(existsSync(f.databasePath+suffix)){const bytes=readFileSync(f.databasePath+suffix);for(const value of [Buffer.from(secretName),Buffer.from(sha(f.bytes)),f.bytes])assert.equal(bytes.includes(value),false);}
 await new Promise(resolve=>queueMicrotask(resolve));assert.ok(f.changes.some(change=>change.sealed));await f.reopen();assert.equal(f.materials.vault.readErasureReceipt({claimId:proof.claimId}).state,'confirmed');await assert.rejects(f.call('read',{materialId,index:0}),code('materials_material_sealed'));
});

test('Source receipt write failure keeps actual material bytes/revision, then exact retry succeeds; reader pin prevents proof',async t=>{
 const f=await fixture(t);await f.migrate();const proof=f.proof();f.sql(db=>db.exec("CREATE TRIGGER material_seal_fault BEFORE INSERT ON material_vault_seals BEGIN SELECT RAISE(ABORT,'material_seal_fault'); END"));assert.throws(()=>f.materials.vault.eraseIfUnchanged({cipherReceipt:proof}),/material_seal_fault/u);assert.deepEqual(Buffer.from((await f.call('read',{materialId,index:0})).data,'base64'),f.bytes);assert.equal(f.materials.vault.readErasureReceipt({claimId:proof.claimId}),null);f.sql(db=>db.exec('DROP TRIGGER material_seal_fault'));
 const reader=new DatabaseSync(f.databasePath);try{reader.exec('BEGIN');reader.prepare('SELECT bytes FROM material_chunks').get();assert.equal(f.materials.vault.eraseIfUnchanged({cipherReceipt:proof}).state,'pending');assert.throws(()=>f.materials.vault.confirmErasure({claimId:proof.claimId}),code('materials_vault_checkpoint_pending'));reader.exec('COMMIT');assert.equal(f.materials.vault.confirmErasure({claimId:proof.claimId}).state,'confirmed');}finally{reader.close();}
});
