import {check,keys,id} from './validation.mjs';
const CHUNK_BYTES=512*1024;
const sync=value=>{if(value&&typeof value.then==='function'){Promise.resolve(value).catch(()=>{});check(false,'notes_vault_async_forbidden');}return value;};

/** Human-note conversion only. A cipher receipt is a current, host-branded proof, never request JSON. */
export function createNotesVaultPort({db,storage,get,run,transaction,ensureOpen,fullNote,metadata,verifyCipherReceipt,onChange,clock}){
 const available=()=>{ensureOpen();check(storage.schemaVersion===3,'notes_vault_migration_required');};
 const seal=(ownerId,noteId)=>{ensureOpen();return storage.schemaVersion===3?(get('SELECT * FROM note_vault_seals WHERE account_id=? AND note_id=?',ownerId,noteId)??null):null;};
 const receipt=row=>row?Object.freeze({schema:'soty.native-vault-erasure.v1',claimId:row.claim_id,providerId:'soty.notes',ownerId:row.account_id,ref:row.note_id,sourceStoreId:row.source_store_id,sourceRevision:row.source_revision,sealedRevision:row.sealed_revision,vaultId:row.vault_id,cipherDigest:row.cipher_digest,state:row.erasure_state,confirmedAt:row.confirmed_at}):null;
 function current(ownerId,noteId,expectedRevision){const row=get("SELECT * FROM notes WHERE account_id=? AND id=? AND state IN('active','archived')",id(ownerId),id(noteId));check(row&&!seal(ownerId,noteId),'notes_vault_source_unavailable');check(row.revision===expectedRevision,'notes_vault_source_changed');
  check(!get('SELECT 1 FROM note_native_creates WHERE account_id=? AND note_id=?',ownerId,noteId),'notes_vault_external_erasure_required');return row;
 }
 return Object.freeze({seal,
  capture(args,callback){available();keys(args,['ownerId','sourceId','expectedRevision']);check(typeof callback==='function'&&Number.isSafeInteger(args.expectedRevision)&&args.expectedRevision>=1);return transaction(()=>{const row=current(args.ownerId,args.sourceId,args.expectedRevision),bytes=Buffer.from(JSON.stringify(fullNote(row)));let live=true;
   try{const frame=Object.freeze({schema:'soty.native-vault-capture.v1',providerId:'soty.notes',ownerId:row.account_id,ref:row.id,sourceStoreId:storage.registryId,revision:row.revision,size:bytes.length,contentType:'application/vnd.soty.note+json',metadata:Object.freeze({kind:'note',note:metadata(row)}),readChunk(index){check(live&&Number.isSafeInteger(index)&&index>=0&&index<Math.max(1,Math.ceil(bytes.length/CHUNK_BYTES)),'notes_vault_capture_expired');current(args.ownerId,args.sourceId,args.expectedRevision);return Buffer.from(bytes.subarray(index*CHUNK_BYTES,(index+1)*CHUNK_BYTES));}});const result=sync(callback(frame));current(args.ownerId,args.sourceId,args.expectedRevision);return result;}finally{live=false;bytes.fill(0);}
  },{write:false});},
  eraseIfUnchanged(args){available();keys(args,['cipherReceipt']);check(typeof verifyCipherReceipt==='function','notes_vault_unavailable');let entered=false,live=true,change;
   try{const result=sync(verifyCipherReceipt(args.cipherReceipt,proof=>{check(live&&!entered&&proof.providerId==='soty.notes'&&proof.sourceStoreId===storage.registryId,'notes_vault_receipt_invalid');entered=true;
    return transaction(()=>{const existing=seal(proof.ownerId,proof.ref);if(existing){check(existing.claim_id===proof.claimId&&existing.vault_id===proof.vaultId&&existing.cipher_digest===proof.cipherDigest,'notes_vault_receipt_conflict');return receipt(existing);}
     const row=current(proof.ownerId,proof.ref,proof.sourceRevision),time=Math.max(clock(),row.updated_at+1);check(Number.isSafeInteger(time)&&time>=0);db.exec('PRAGMA secure_delete=ON');
     run("INSERT INTO notes_fts(notes_fts,rank) VALUES('secure-delete',1)");run('DELETE FROM notes_fts WHERE rowid=?',row.rowid);
     run("UPDATE notes SET title='',body='',items='[]',preview='',color='plain',pinned=0,bytes=0,revision=revision+1,updated_at=? WHERE rowid=?",time,row.rowid);run('DELETE FROM note_receipts WHERE account_id=? AND note_id=?',row.account_id,row.id);run('UPDATE note_accounts SET bytes=bytes-? WHERE account_id=?',row.bytes,row.account_id);
     run("INSERT INTO note_vault_seals VALUES(?,?,?,?,?,?,?,?, 'pending',?,NULL)",row.account_id,row.id,proof.claimId,proof.vaultId,proof.cipherDigest,row.revision,row.revision+1,storage.registryId,time);change={accountId:row.account_id,noteId:row.id,revision:row.revision+1,sealed:true};return receipt(seal(row.account_id,row.id));
    });}));check(entered,'notes_vault_receipt_invalid');if(change&&onChange)queueMicrotask(()=>{try{onChange(Object.freeze(change));}catch{}});return result;
   }finally{live=false;}
  },
  readErasureReceipt(args){available();keys(args,['claimId']);id(args.claimId);return receipt(get('SELECT * FROM note_vault_seals WHERE claim_id=?',args.claimId));},
  confirmErasure(args){available();keys(args,['claimId']);id(args.claimId);check(!db.isTransaction,'notes_nested_transaction');const before=get('SELECT * FROM note_vault_seals WHERE claim_id=?',args.claimId);check(before,'notes_vault_receipt_missing');if(before.erasure_state==='confirmed')return receipt(before);
   const row=get('SELECT * FROM notes WHERE account_id=? AND id=?',before.account_id,before.note_id);check(row&&row.revision===before.sealed_revision&&row.bytes===0&&row.title===''&&row.body===''&&row.items==='[]'&&row.preview===''&&!get('SELECT 1 FROM notes_fts WHERE rowid=?',row.rowid)&&!get('SELECT 1 FROM note_receipts WHERE account_id=? AND note_id=?',row.account_id,row.id),'notes_vault_erasure_incomplete');
   db.exec('PRAGMA secure_delete=ON');run("INSERT INTO notes_fts(notes_fts) VALUES('optimize')");const timeout=get('PRAGMA busy_timeout').timeout;let checked;try{db.exec('PRAGMA busy_timeout=100');checked=get('PRAGMA wal_checkpoint(TRUNCATE)');}finally{db.exec(`PRAGMA busy_timeout=${timeout}`);}check(checked.busy===0&&checked.log===0&&checked.checkpointed===0&&Number(get('PRAGMA secure_delete').secure_delete)===1,'notes_vault_checkpoint_pending');
   return transaction(()=>{const fresh=seal(before.account_id,before.note_id);check(fresh?.claim_id===before.claim_id&&fresh.sealed_revision===row.revision,'notes_vault_erasure_changed');if(fresh.erasure_state==='pending')run("UPDATE note_vault_seals SET erasure_state='confirmed',confirmed_at=? WHERE claim_id=?",clock(),args.claimId);return receipt(seal(before.account_id,before.note_id));});
  }
 });
}
