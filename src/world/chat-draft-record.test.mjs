import test from 'node:test';
import assert from 'node:assert/strict';
import { initializeDraftRecord, writeDraftRecord, selectDraftRecord, acknowledgeDraftRecord, readDraftRecord, recoverLegacyDraftRecord } from './chat-draft-record.mjs';
const edit=(baseId,id,writer,text,replyTo=null)=>({baseId,id,writer,text,replyTo});

test('legacy text is imported once, and a following edit remains a single draft',()=>{
  const old=initializeDraftRecord(null,{text:'Before update',replyTo:null},'legacy_001');assert.equal(old.heads[0].text,'Before update');
  assert.equal(initializeDraftRecord(old,{text:'Different stale cache'},'legacy_002'),null);
  const updated=writeDraftRecord(old,edit('legacy_001','draft_002','tab_001','Next')).record;
  assert.deepEqual(updated.heads.map(h=>h.text),['Next']);
});

test('two edits based on the same prior state are both retained, including identical text',()=>{
  for(const second of ['Second','First']){
    const first=writeDraftRecord(null,edit(null,'draft_001','tab_001','First')).record;
    const both=writeDraftRecord(first,edit(null,'draft_002','tab_002',second)).record;
    assert.equal(both.heads.length,2);assert.deepEqual(both.heads.map(h=>h.text),['First',second]);
  }
});

test('choosing an earlier snapshot cannot remove a newer edit from another tab',()=>{
  const a=writeDraftRecord(null,edit(null,'draft_001','tab_001','First')).record;
  const two=writeDraftRecord(a,edit(null,'draft_002','tab_002','Second')).record;
  const changed=writeDraftRecord(two,edit('draft_002','draft_003','tab_002','Newer')).record;
  assert.throws(()=>selectDraftRecord(changed,['draft_001','draft_002'],'draft_001','selected_001','tab_001'),{code:'chat_draft_changed'});
  assert.deepEqual(changed.heads.map(h=>h.text),['First','Newer']);
  const selected=selectDraftRecord(changed,['draft_001','draft_003'],'draft_001','selected_002','tab_001').record;
  assert.deepEqual(selected.heads.map(h=>h.text),['First']);
});

test('acknowledgement removes only the exact sent version, never later identical text or another tab',()=>{
  const first=writeDraftRecord(null,edit(null,'draft_001','tab_001','Same')).record;
  const later=writeDraftRecord(first,edit('draft_001','draft_002','tab_001','Same')).record;
  assert.equal(acknowledgeDraftRecord(later,'draft_001'),null);assert.equal(later.heads[0].id,'draft_002');
  const both=writeDraftRecord(later,edit(null,'draft_003','tab_002','Other')).record;
  assert.deepEqual(acknowledgeDraftRecord(both,'draft_002').heads.map(h=>h.text),['Other']);
});

test('malformed or stale data is rejected without modifying the previous record',()=>{
  const first=writeDraftRecord(null,edit(null,'draft_001','tab_001','Keep')).record, before=structuredClone(first);
  assert.throws(()=>writeDraftRecord(first,edit(null,'draft_001','tab_001','Changed')),{code:'chat_draft_changed'});
  assert.throws(()=>readDraftRecord({kind:'chat-draft-heads.v1',revision:1,heads:[{id:'wrong'}]}),{code:'chat_draft_storage_corrupt'});
  assert.deepEqual(first,before);
});

test('late legacy text is a separate version and an identical later old write can be recovered',()=>{
  const current=writeDraftRecord(null,edit(null,'draft_001','tab_001','Current')).record;
  const candidate={source:'v1',digest:'a'.repeat(64),text:'Old tab thought',replyTo:null};
  const both=recoverLegacyDraftRecord(current,candidate,'recovered_001');assert.deepEqual(both.heads.map(head=>head.text),['Current','Old tab thought']);
  assert.equal(recoverLegacyDraftRecord(both,candidate,'recovered_002'),null);
  const selected=selectDraftRecord(both,both.heads.map(head=>head.id),'draft_001','selected_001','tab_001').record;
  const sent=acknowledgeDraftRecord(selected,'selected_001');assert.deepEqual(sent.heads,[]);
  assert.equal(recoverLegacyDraftRecord(sent,candidate,'recovered_003'),null);
  const repeated=recoverLegacyDraftRecord(sent,{...candidate,fresh:true},'recovered_003');assert.equal(repeated.heads[0].text,'Old tab thought');
  const changed=recoverLegacyDraftRecord(repeated,{...candidate,digest:'b'.repeat(64),text:'Another old edit'},'recovered_004');assert.equal(changed.heads.length,1);assert.equal(changed.heads[0].text,'Another old edit');
});
