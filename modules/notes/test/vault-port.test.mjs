import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,readFileSync,existsSync,realpathSync,copyFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import {createNotesService} from '../server/index.mjs';
import {inspectNotesSchema as inspectReader2} from '../server/schema-v2.mjs';
const actor={accountId:'notes_vault_owner',deviceId:'notes_vault_device'},secret='notesealuniqueplaintextsentinel4918ff';
const code=value=>error=>{assert.equal(error.code,value);return true;};
function fixture(t){const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-notes-vault-')),databasePath=join(folder,'notes.sqlite'),changes=[],proofs=new WeakSet();let notes;
 const open=options=>notes=createNotesService({databasePath,projectId:'notes_vault_test',allowNativeMigration:true,...options,onChange:change=>changes.push(change),verifyVaultCipherReceipt:(proof,callback)=>{assert.ok(proofs.has(proof),'only current host-branded cipher receipt');return callback(proof);}});open();
 t.after(()=>{notes.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-notes-vault-'));rmSync(actual,{recursive:true,force:true});});
 const call=(op,args={})=>notes.execute({actor,op:'notes.'+op,args:{expectedAccountId:actor.accountId,...args}}),document={noteId:'notes_vault_note',expectedRevision:0,mutationId:'notes_vault_create',title:secret,body:secret,items:[{id:'item_111',text:secret,done:false}],color:'plain',pinned:false,state:'active'};
 const sql=callback=>{const db=new DatabaseSync(databasePath);try{return callback(db);}finally{db.close();}};
 return {call,document,databasePath,folder,changes,sql,get notes(){return notes;},migrate(){notes.close();open({allowVaultMigration:true});},reopen(){notes.close();open();},proof(){const proof=Object.freeze({claimId:'claim_notes_4819',providerId:'soty.notes',ownerId:actor.accountId,ref:document.noteId,sourceStoreId:notes.registryId,sourceRevision:1,vaultId:'vault_notes_4819',cipherDigest:'a'.repeat(64)});proofs.add(proof);return proof;}};
}

test('Notes3 explicit migration preserves old data/identity; Source capture is scoped, versioned and cannot outlive its callback',t=>{
 const f=fixture(t);f.call('put',f.document);const before=f.call('get',{noteId:f.document.noteId}),registry=f.notes.registryId;
 assert.throws(()=>f.notes.vault.capture({ownerId:actor.accountId,sourceId:f.document.noteId,expectedRevision:1},()=>{}),code('notes_vault_migration_required'));f.migrate();assert.equal(f.notes.schemaVersion,3);assert.equal(f.notes.registryId,registry);assert.deepEqual(f.call('get',{noteId:f.document.noteId}),before);
 let captured;const body=f.notes.vault.capture({ownerId:actor.accountId,sourceId:f.document.noteId,expectedRevision:1},frame=>{captured=frame;assert.equal(frame.sourceStoreId,registry);return JSON.parse(frame.readChunk(0));});assert.equal(body.body,secret);assert.throws(()=>captured.readChunk(0),code('notes_vault_capture_expired'));
 assert.throws(()=>f.sql(db=>inspectReader2(db,'notes_vault_test')),code('notes_schema_unsupported'));f.reopen();assert.equal(f.notes.schemaVersion,3);assert.equal(f.call('get',{noteId:f.document.noteId}).note.body,secret);
});

test('real Source erasure COMMIT clears body/preview/items/FTS/receipts and old APIs cannot resurrect it; checkpoint before confirmation',async t=>{
 const f=fixture(t);f.call('put',f.document);f.migrate();const proof=f.proof();assert.throws(()=>f.notes.vault.eraseIfUnchanged({cipherReceipt:{...proof}}),/host-branded/u);
 const pending=f.notes.vault.eraseIfUnchanged({cipherReceipt:proof});assert.equal(pending.state,'pending');assert.equal(f.notes.vault.readErasureReceipt({claimId:proof.claimId}).state,'pending');assert.equal(f.notes.vault.eraseIfUnchanged({cipherReceipt:proof}).sealedRevision,2);
 assert.throws(()=>f.call('get',{noteId:f.document.noteId}),code('notes_note_sealed'));assert.throws(()=>f.call('put',f.document),code('notes_note_sealed'));assert.throws(()=>f.call('purge',{noteId:f.document.noteId,expectedRevision:2,mutationId:'purge_old'}),code('notes_note_sealed'));
 assert.equal(f.call('list',{query:secret}).notes.length,0);const listed=f.call('list').notes[0];assert.equal(listed.sealed,true);assert.equal(JSON.stringify(listed).includes(secret),false);
 const confirmed=f.notes.vault.confirmErasure({claimId:proof.claimId});assert.equal(confirmed.state,'confirmed');assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM note_receipts').get().n),0);assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM notes_fts').get().n),0);
 for(const suffix of ['','-wal','-shm'])if(existsSync(f.databasePath+suffix))assert.equal(readFileSync(f.databasePath+suffix).includes(Buffer.from(secret)),false,'current DB and WAL must no longer contain the old marker');
 await new Promise(resolve=>queueMicrotask(resolve));assert.ok(f.changes.some(change=>change.sealed===true));f.reopen();assert.equal(f.notes.vault.readErasureReceipt({claimId:proof.claimId}).state,'confirmed');assert.throws(()=>f.call('get',{noteId:f.document.noteId}),code('notes_note_sealed'));
});

test('Source revision race and receipt fault preserve the original note/search; same exact proof succeeds after removing fault',t=>{
 const f=fixture(t);f.call('put',f.document);f.migrate();const proof=f.proof();f.sql(db=>db.exec("CREATE TRIGGER source_erase_fault BEFORE INSERT ON note_vault_seals BEGIN SELECT RAISE(ABORT,'source_erase_fault'); END"));assert.throws(()=>f.notes.vault.eraseIfUnchanged({cipherReceipt:proof}),/source_erase_fault/u);assert.equal(f.call('get',{noteId:f.document.noteId}).note.body,secret);assert.equal(f.call('list',{query:secret}).notes.length,1);assert.equal(f.notes.vault.readErasureReceipt({claimId:proof.claimId}),null);
 f.sql(db=>db.exec('DROP TRIGGER source_erase_fault'));f.call('put',{...f.document,expectedRevision:1,mutationId:'source_raced',body:'A newer clear body'});assert.throws(()=>f.notes.vault.eraseIfUnchanged({cipherReceipt:proof}),code('notes_vault_source_changed'));assert.equal(f.call('get',{noteId:f.document.noteId}).note.body,'A newer clear body');
});

test('an actual old SQLite reader prevents checkpoint proof; no confirmation until reader closes and retry succeeds',t=>{
 const f=fixture(t);f.call('put',f.document);f.migrate();const reader=new DatabaseSync(f.databasePath);try{reader.exec('BEGIN');reader.prepare('SELECT body FROM notes').get();const proof=f.proof();f.notes.vault.eraseIfUnchanged({cipherReceipt:proof});assert.throws(()=>f.notes.vault.confirmErasure({claimId:proof.claimId}),code('notes_vault_checkpoint_pending'));assert.equal(f.notes.vault.readErasureReceipt({claimId:proof.claimId}).state,'pending');reader.exec('COMMIT');assert.equal(f.notes.vault.confirmErasure({claimId:proof.claimId}).state,'confirmed');}finally{reader.close();}
});

test('a capability-created note keeps its external evidence and is refused until the external ledger can prove erasure',t=>{
 const f=fixture(t),sourceStoreId='a'.repeat(32),invocationId='vault_capability_note',digest=value=>createHash('sha256').update(value).digest('hex'),suffix=digest(JSON.stringify(['soty.native-note.v1',sourceStoreId,invocationId]));f.document.noteId='n_'+suffix;f.document.mutationId='m_'+suffix;f.call('put',f.document);f.migrate();
 f.sql(db=>{const createdAt=db.prepare('SELECT created_at FROM notes WHERE id=?').get(f.document.noteId).created_at;db.prepare('INSERT INTO note_native_creates VALUES(?,?,?,?,?,?,?,?,?)').run(sourceStoreId,invocationId,actor.accountId,f.document.noteId,f.document.mutationId,digest(JSON.stringify({body:secret,title:secret})),'95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204',1,createdAt);});
 assert.throws(()=>f.notes.vault.capture({ownerId:actor.accountId,sourceId:f.document.noteId,expectedRevision:1},()=>{}),code('notes_vault_external_erasure_required'));assert.throws(()=>f.notes.vault.eraseIfUnchanged({cipherReceipt:f.proof()}),code('notes_vault_external_erasure_required'));assert.equal(f.call('get',{noteId:f.document.noteId}).note.body,secret);assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM note_native_creates').get().n),1);assert.equal(f.notes.vault.seal(actor.accountId,f.document.noteId),null);f.reopen();assert.equal(f.call('get',{noteId:f.document.noteId}).note.body,secret);
});
