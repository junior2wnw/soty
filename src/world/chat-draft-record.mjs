const id = value => typeof value === 'string' && /^[A-Za-z0-9_-]{3,160}$/u.test(value);
const text = value => typeof value === 'string' && value.length <= 6000;
const valid = head => head && id(head.id) && id(head.writer) && text(head.text) && (head.replyTo === null || id(head.replyTo));
const fail = code => { throw Object.assign(new Error(code), { code }); };
const snapshot = value => ({id:value.id,writer:value.writer,text:value.text,replyTo:value.replyTo});
export function readDraftRecord(value) {
  if (!value) return {revision:0,heads:[]};
  if (value.kind !== 'chat-draft-heads.v1' || !Number.isSafeInteger(value.revision) || value.revision < 0
    || !Array.isArray(value.heads) || !value.heads.every(valid) || new Set(value.heads.map(h=>h.id)).size !== value.heads.length) fail('chat_draft_storage_corrupt');
  return {revision:value.revision,heads:value.heads.map(snapshot)};
}
function next(record, heads) {
  if(record.revision>=Number.MAX_SAFE_INTEGER)fail('chat_draft_storage_corrupt');
  return {kind:'chat-draft-heads.v1',revision:record.revision+1,heads};
}
export function initializeDraftRecord(value, legacy, legacyId) {
  const record=readDraftRecord(value); if(value)return null;
  if(!legacy?.text && !legacy?.replyTo)return next(record,[]);
  const head={id:legacyId,writer:'legacy',text:legacy.text??'',replyTo:legacy.replyTo??null};if(!valid(head))fail('chat_draft_storage_corrupt');
  return next(record,[head]);
}
export function writeDraftRecord(value, proposal) {
  const record=readDraftRecord(value),head=snapshot(proposal);if(!valid(head) || proposal.baseId!==null&&!id(proposal.baseId))fail('chat_draft_invalid');
  const existing=record.heads.find(item=>item.id===head.id);
  if(existing){if(JSON.stringify(existing)!==JSON.stringify(head))fail('chat_draft_changed');return {record:null,head:existing};}
  const index=record.heads.findIndex(item=>item.id===proposal.baseId);
  const heads=record.heads.map(snapshot);if(index>=0)heads[index]=head;else heads.push(head);
  return {record:next(record,heads),head};
}
export function selectDraftRecord(value, expectedIds, selectedId, selectedVersion, writer) {
  const record=readDraftRecord(value);
  if(!Array.isArray(expectedIds)||expectedIds.some(value=>!id(value))||new Set(expectedIds).size!==expectedIds.length
    || JSON.stringify([...expectedIds].sort())!==JSON.stringify(record.heads.map(h=>h.id).sort()))fail('chat_draft_changed');
  const chosen=record.heads.find(h=>h.id===selectedId);if(!chosen)fail('chat_draft_changed');
  const selected={...chosen,id:selectedVersion,writer};if(!valid(selected))fail('chat_draft_invalid');
  return {record:next(record,[selected]),head:selected};
}
export function acknowledgeDraftRecord(value, draftId) {
  const record=readDraftRecord(value);if(!id(draftId))fail('chat_draft_invalid');
  const heads=record.heads.filter(head=>head.id!==draftId);return heads.length===record.heads.length?null:next(record,heads);
}
export function recoverLegacyDraftRecord(value, candidate, draftId) {
  const record=readDraftRecord(value),head={id:draftId,writer:'legacy_recovered_'+candidate.source,text:candidate.text,replyTo:candidate.replyTo};
  if(!['v1','v2'].includes(candidate.source)||!valid(head)||(!head.text&&!head.replyTo))fail('chat_draft_invalid');
  const heads=record.heads.map(snapshot),index=heads.findIndex(item=>item.writer===head.writer);
  if(index>=0){if(heads[index].text===head.text&&heads[index].replyTo===head.replyTo)return null;heads[index]=head;}
  else if(!heads.some(item=>item.writer==='legacy'&&item.text===head.text&&item.replyTo===head.replyTo))heads.push(head);
  else return null;
  return next(record,heads);
}
