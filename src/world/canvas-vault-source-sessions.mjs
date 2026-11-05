import {registerRetainedNativeDraft} from '../../modules/notes/browser/native-local.mjs';
import {vaultCheck} from './canvas-vault-local.mjs';
// Private RAM only. The application shell receives only the existing global
// boolean/local-drain port. Account/origin/Source namespaces never mix.
const sources=new Map();
export function registerCanvasVaultDraft({scopeKey,objectId,noteId,port,isCurrent}){
  vaultCheck(typeof scopeKey==='string'&&typeof objectId==='string'&&typeof noteId==='string'&&typeof isCurrent==='function'&&port&&typeof port.snapshot==='function'&&typeof port.releaseRecovered==='function','vault_local_invalid');
  if(!sources.has(scopeKey))sources.set(scopeKey,new Set());const entry={objectId,noteId,port,isCurrent},group=sources.get(scopeKey);group.add(entry);
  const unguard=registerRetainedNativeDraft(port);let live=true;
  return()=>{if(!live)return;live=false;unguard();group.delete(entry);if(group.size===0)sources.delete(scopeKey);};
}
export async function drainCanvasVaultDrafts(scopeKey){
  for(const {port}of [...(sources.get(scopeKey)??[])])try{await port.flushLocal();}catch{throw Object.assign(new Error('vault_local_draft_pending'),{code:'vault_local_draft_pending'});}
}
/** Private captured DraftStore port, never a public loader/global UI result. */
export function canvasVaultDraftSessions(scopeKey,objectId,noteId){return[...(sources.get(scopeKey)??[])].filter(value=>value.objectId===objectId&&value.noteId===noteId).map(value=>value.port);}
export async function recoverCanvasVaultDrafts(scopeKey,{isCurrent,write}){
  vaultCheck(typeof isCurrent==='function'&&typeof write==='function','vault_local_invalid');
  for(const entry of [...(sources.get(scopeKey)??[])]){
    if(entry.port.hasUnsavedChanges?.()!==true||entry.isCurrent()&&entry.port.isDisposed?.()!==true)continue;
    vaultCheck(isCurrent()===true,'canvas_object_locked');const snapshot=entry.port.snapshot();vaultCheck(snapshot?.note?.noteId===entry.noteId,'vault_local_scope_changed');
    await write({objectId:entry.objectId,noteId:entry.noteId,draft:snapshot});
    vaultCheck(isCurrent()===true,'canvas_object_locked');const current=entry.port.snapshot();vaultCheck(current.generation===snapshot.generation,'vault_local_conflict');
    // The session releases its private snapshot ONLY after encrypted COMMIT.
    entry.port.releaseRecovered(snapshot.generation);
  }
}
