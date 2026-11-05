import {sealCanvasVaultRecord,openCanvasVaultRecord,vaultCheck,vaultId} from './canvas-vault-local.mjs';
import {canvasVaultRecoveryScope} from './canvas-vault-scope.mjs';
const encoder=new TextEncoder();
const clone=value=>structuredClone(value);
const sameSource=(a,b)=>a?.providerId===b?.providerId&&a?.ref===b?.ref&&a?.ownerId===b?.ownerId;
const hash=async(value,crypto)=>[...new Uint8Array(await crypto.subtle.digest('SHA-256',encoder.encode(JSON.stringify(value))))].map(byte=>byte.toString(16).padStart(2,'0')).join('');
function cleanDraft(value,source){
  vaultCheck(value?.note?.noteId===source.ref&&vaultId(value.branchId)&&Number.isSafeInteger(value.generation)&&value.generation>=0
    &&Number.isSafeInteger(value.committedGeneration)&&value.committedGeneration>=0&&value.committedGeneration<=value.generation
    &&Number.isSafeInteger(value.savedAt)&&value.savedAt>=0&&typeof value.conflict==='boolean','vault_native_recovery_invalid');
  vaultCheck(typeof value.note.title==='string'&&typeof value.note.body==='string'&&Array.isArray(value.note.items),'vault_native_recovery_invalid');
  vaultCheck(value.pending===null||value.pending&&vaultId(value.pending.mutationId)&&Number.isSafeInteger(value.pending.expectedRevision)
    &&Number.isSafeInteger(value.pending.generation)&&value.pending.document&&typeof value.pending.document.body==='string','vault_native_recovery_invalid');
  return clone({note:value.note,branchId:value.branchId,generation:value.generation,committedGeneration:value.committedGeneration,pending:value.pending,conflict:value.conflict,savedAt:value.savedAt});
}
const validPayload=(value,source)=>value?.schema==='soty.native-note-recovery.v1'&&sameSource(value.source,source)&&value.draft?.note?.noteId===source.ref;
async function currentProfile(store,session){const value=await store.ensureProfile(session.profile);vaultCheck(value.profileId===session.profile.profileId&&value.salt===session.profile.salt,'vault_local_scope_changed');}

/** Reuses the existing Vault AEAD/profile/store. There is no saved password,
 * alternate cipher, Source write, old mutation replay or plaintext fallback. */
export async function archiveNativeNoteDrafts({session,store,accountId,projectId,objectId,source,drafts,cryptoScope,crypto=globalThis.crypto}){
  const scope=canvasVaultRecoveryScope(cryptoScope,objectId,session?.profile??{}),slot=scope.slot;scope.active();
  vaultCheck(session?.profile?.accountId===accountId&&session.profile.projectId===projectId&&session.profile.slot===slot
    &&source?.providerId==='soty.notes'&&source.ownerId===accountId&&vaultId(source.ref)&&Array.isArray(drafts)&&drafts.length<=32,'vault_native_recovery_scope');
  const receipts=[];
  for(const original of drafts){
    const draft=cleanDraft(original,source),recordId='native_recovery_'+(await hash([accountId,projectId,source.providerId,source.ref,draft.branchId],crypto)).slice(0,56);
    const previous=await store.get(recordId);let expected=0;
    if(previous){
      vaultCheck(previous.slot===slot&&previous.purpose==='draft','vault_native_recovery_other_object');
      const old=await openCanvasVaultRecord(session,previous,{crypto});vaultCheck(validPayload(old,source),'vault_native_recovery_invalid');
      if(old.draft.generation>draft.generation||old.draft.generation===draft.generation&&old.draft.savedAt>draft.savedAt){receipts.push({recordId,version:previous.version,branchId:draft.branchId,generation:old.draft.generation,savedAt:old.draft.savedAt});continue;}
      if(old.draft.generation===draft.generation&&old.draft.savedAt===draft.savedAt){vaultCheck(JSON.stringify(old.draft)===JSON.stringify(draft),'vault_local_conflict');receipts.push({recordId,version:previous.version,branchId:draft.branchId,generation:draft.generation,savedAt:draft.savedAt});continue;}
      expected=previous.version;
    }
    const value=await sealCanvasVaultRecord(session,{schema:'soty.vault.local-record.v1',accountId,projectId,slot,recordId,purpose:'draft',version:expected+1},
      {schema:'soty.native-note-recovery.v1',originObjectId:objectId,source:clone(source),draft},{crypto});
    vaultCheck(await store.put(value,expected),'vault_local_conflict');
    receipts.push({recordId,version:value.version,branchId:draft.branchId,generation:draft.generation,savedAt:draft.savedAt});
  }
  await currentProfile(store,session);scope.active();return{schema:'soty.native-note-recovery-receipt.v1',objectId,records:receipts};
}

export async function readNativeNoteRecoveries({session,store,objectId,accountId,cryptoScope,crypto=globalThis.crypto}){
  const scope=canvasVaultRecoveryScope(cryptoScope,objectId,session?.profile??{}),slot=scope.slot;scope.active();
  vaultCheck(session?.profile?.accountId===accountId&&session.profile.slot===slot,'vault_native_recovery_scope');
  const result=[];
  for(const record of await store.list(slot))if(record.purpose==='draft'&&record.recordId.startsWith('native_recovery_')){
    const value=await openCanvasVaultRecord(session,record,{crypto});
    vaultCheck(value?.schema==='soty.native-note-recovery.v1'&&value.source?.providerId==='soty.notes'&&value.source.ownerId===accountId,'vault_native_recovery_invalid');
    if(cryptoScope&&value.originObjectId!==objectId)continue;
    result.push({recordId:record.recordId,version:record.version,source:clone(value.source),draft:cleanDraft(value.draft,value.source)});
  }
  await currentProfile(store,session);scope.active();return result;
}
