/** Namespaced opaque identities. This module owns no field geometry or durable note database. */
export function nativeFieldRef(value) {
  if(!value||value.kind!=='builtin'||typeof value.id!=='string')return null;
  const match=/^(notes|materials)\/([A-Za-z0-9][A-Za-z0-9_-]{7,95})$/u.exec(value.id);
  return match?{provider:match[1],id:match[2]}:null;
}
export function nativeNoteFontSize(value) {return Number.isFinite(value)?Math.min(128,Math.max(8,value)):16;}
/** A live directory refresh must never replace a focused editor or an unconfirmed branch. */
export function mayRefreshNativeNote(state,focused,remoteRevision) {
  return Boolean(state&&!focused&&!state.dirty&&!state.saving&&!state.conflict
    &&Number.isSafeInteger(remoteRevision)&&remoteRevision>state.note.revision);
}
export function nativeNoteStatus(state,stale=false) {
  return state.localError?'Не сохранено на устройстве':state.conflict?'Две версии':state.saving?'Сохраняем…'
    :state.dirty?'На устройстве':stale?'Копия на устройстве':state.note.revision?'Сохранено':'Новая записка';
}
