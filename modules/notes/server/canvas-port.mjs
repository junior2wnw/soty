import {check,id} from './validation.mjs';

/** Private provider port. Only the live Canvas Objects authority frame selects
 * the owner/source; serialized request data cannot select somebody else's note. */
export function createNotesCanvasPort({get,transaction,ensureOpen,metadata,fullNote,verifyObjectAuthority,seal=()=>null,readVaultSource,sourceStoreId}){
  const synchronous=value=>{if(value&&typeof value.then==='function'){void Promise.resolve(value).catch(()=>{});check(false,'notes_canvas_async_forbidden');}return value;};
  const current=(ownerId,noteId)=>{
    ensureOpen();id(ownerId);id(noteId);
    const row=get("SELECT * FROM notes WHERE account_id=? AND id=? AND state IN('active','archived')",ownerId,noteId);
    check(row,'notes_note_not_found');return row;
  };
  function verify(context,consume){
    check(typeof verifyObjectAuthority==='function','notes_canvas_authority_required');
    let entered=false,active=true,result;
    try{
      synchronous(verifyObjectAuthority(context,scope=>{
        check(active&&!entered&&scope?.providerId==='soty.notes'&&scope.right==='read','notes_canvas_authority_required');entered=true;
        result=synchronous(consume({ownerId:id(scope.ownerId),noteId:id(scope.ref)}));return result;
      }));check(entered,'notes_canvas_authority_required');return result;
    }finally{active=false;}
  }
  function read(context,project){
    return verify(context,scope=>transaction(()=>{
      const before=current(scope.ownerId,scope.noteId),result=project(before);
      verify(context,again=>{check(again.ownerId===scope.ownerId&&again.noteId===scope.noteId,'notes_canvas_authority_required');const after=current(scope.ownerId,scope.noteId);check(after.revision===before.revision,'notes_revision_conflict');});
      return result;
    },{write:false}));
  }
  return Object.freeze({
    withSourceAuthority({actor,noteId,ownerId=actor?.accountId,mode,purpose},callback){
      ensureOpen();check(['create','read'].includes(mode)&&typeof callback==='function','notes_canvas_authority_required');
      check(purpose===undefined||purpose==='protection','notes_canvas_authority_required');
      if(mode==='create')check(actor&&actor.accountId===ownerId,'notes_canvas_authority_required');
      const before=current(ownerId,noteId),wasSealed=seal(ownerId,noteId),result=synchronous(callback(Object.freeze({allowed:true,sourceRevision:before.revision,sourceStoreId,protection:Object.freeze(wasSealed?{kind:'password',sealed:true}:{kind:'none'})})));
      const after=current(ownerId,noteId),nowSealed=seal(ownerId,noteId);check(after.revision===before.revision||purpose==='protection'&&!wasSealed&&nowSealed?.source_revision===before.revision&&nowSealed?.sealed_revision===after.revision,'notes_revision_conflict');return result;
    },
    readMetadata({context}){return read(context,row=>{if(!seal(row.account_id,row.id))return {note:metadata(row)};check(typeof readVaultSource==='function','notes_vault_unavailable');return synchronous(readVaultSource({context,part:'noteMetadata'}));});},
    readNote({context}){return read(context,row=>{if(!seal(row.account_id,row.id))return {note:fullNote(row)};check(typeof readVaultSource==='function','notes_vault_unavailable');return synchronous(readVaultSource({context,part:'note'}));});},
  });
}
