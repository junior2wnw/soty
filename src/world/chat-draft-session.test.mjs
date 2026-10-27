import test from 'node:test';
import assert from 'node:assert/strict';
import { createChatDraftSession } from './chat-draft-session.mjs';
import { createTestChatDraftDisk as diskFixture } from './test-support/chat-draft-disk.mjs';
const create=(disk,accountId='alice',conversationKey='room',legacy={text:'',replyTo:null})=>createChatDraftSession({disk,accountId,conversationKey,legacy});

test('normal edits autosave and another tab follows a later sequential edit',async t=>{
  const disk=diskFixture(),a=create(disk),b=create(disk);t.after(()=>{a.close();b.close();});await Promise.all([a.ready,b.ready]);
  a.edit('First',null);assert.equal(a.hasUnsavedChanges(),true);assert.equal(await a.flush(),true);assert.equal(b.read().value.text,'First');
  b.edit('First plus second',null);assert.equal(await b.flush(),true);assert.equal(a.read().value.text,'First plus second');assert.equal(a.read().heads.length,1);
});

test('two edits before notification retain both heads; a stale choice never removes a newer edit',async t=>{
  const disk=diskFixture(),a=create(disk),b=create(disk);t.after(()=>{a.close();b.close();});await Promise.all([a.ready,b.ready]);
  a.edit('First',null);b.edit('Second',null);assert.equal(await a.flush(),true);assert.equal(await b.flush(),true);await Promise.all([a.refresh(),b.refresh()]);
  assert.deepEqual(a.read().heads.map(h=>h.text).sort(),['First','Second']);const observed=a.read().heads,selected=observed.find(h=>h.text==='First');
  b.edit('Second newer',null);assert.equal(await b.flush(),true);
  assert.equal(await a.select(observed.map(h=>h.id),selected.id),false);await a.refresh();assert.deepEqual(a.read().heads.map(h=>h.text).sort(),['First','Second newer']);
  const fresh=a.read().heads;assert.equal(await a.select(fresh.map(h=>h.id),fresh.find(h=>h.text==='First').id),true);assert.deepEqual(a.read().heads.map(h=>h.text),['First']);
});

test('a pending flush survives screen disposal and storage refusal keeps text available for recovery',async()=>{
  const disk=diskFixture(),session=create(disk);await session.ready;let release;disk.hold(new Promise(resolve=>{release=resolve;}));
  session.edit('Keep this',null);assert.equal(session.hasUnsavedChanges(),true);const saving=session.flush();session.close();disk.hold(null);release();assert.equal(await saving,true);
  const reopened=create(disk);await reopened.ready;assert.equal(reopened.read().value.text,'Keep this');disk.setFailure(true);
  reopened.edit('Still here',null);assert.equal(await reopened.flush(),false);assert.equal(reopened.read().value.text,'Still here');assert.equal(reopened.hasUnsavedChanges(),true);
  disk.setFailure(false);assert.equal(await reopened.flush(),true);assert.equal(reopened.hasUnsavedChanges(),false);reopened.close();
});

test('acknowledging a captured old ID preserves later identical text and another account',async t=>{
  const disk=diskFixture(),a=create(disk),b=create(disk,'bob');t.after(()=>{a.close();b.close();});await Promise.all([a.ready,b.ready]);
  a.edit('Same',null);await a.flush();const old=a.read().selectedId;b.edit('Bob private',null);await b.flush();
  a.edit('Another',null);a.edit('Same',null);await a.flush();assert.notEqual(a.read().selectedId,old);
  await a.acknowledge(old);assert.equal(a.read().value.text,'Same');assert.equal(b.read().value.text,'Bob private');
});

test('an initial storage-open failure retries without losing text entered while it was unavailable',async t=>{
  const disk=diskFixture();disk.setFailure(true);const session=create(disk);t.after(()=>session.close());
  await assert.rejects(session.ready,{code:'attachment_local_storage'});session.edit('During outage',null);assert.equal(await session.flush(),false);
  assert.equal(session.read().value.text,'During outage');assert.equal(session.hasUnsavedChanges(),true);
  disk.setFailure(false);assert.equal(await session.flush(),true);assert.equal(session.read().value.text,'During outage');
  const reopened=create(disk);t.after(()=>reopened.close());await reopened.ready;assert.equal(reopened.read().value.text,'During outage');
});

test('late old-tab text survives a tombstone without becoming an automatic send',async t=>{
  const disk=diskFixture(),first=create(disk);t.after(()=>first.close());await first.ready;first.edit('Already sent',null);await first.flush();await first.acknowledge(first.read().selectedId);
  const candidate={source:'v1',text:'Unsent old-tab text',replyTo:null};first.recoverLegacy(candidate);assert.equal(await first.flush(),true);
  assert.equal(first.read().value.text,'');assert.deepEqual(first.read().heads.map(head=>[head.writer,head.text]),[['legacy_recovered_v1','Unsent old-tab text']]);
  const head=first.read().heads[0];assert.equal(await first.select([head.id],head.id),true);assert.equal(first.read().value.text,'Unsent old-tab text');
  await first.acknowledge(first.read().selectedId);const reopened=create(disk);t.after(()=>reopened.close());await reopened.ready;
  reopened.recoverLegacy(candidate);await reopened.flush();assert.equal(reopened.read().value.text,'');assert.equal(reopened.read().heads[0].text,candidate.text);
});

test('storage refusal keeps the old-tab text in memory until it is durably recovered',async t=>{
  const disk=diskFixture(),session=create(disk);t.after(()=>session.close());await session.ready;disk.setFailure(true);
  session.recoverLegacy({source:'v1',text:'Keep old text',replyTo:null});assert.equal(await session.flush(),false);
  assert.equal(session.hasUnsavedChanges(),true);assert.equal(session.read().pendingLegacy[0].text,'Keep old text');assert.equal(session.read().error,'attachment_local_storage');
  disk.setFailure(false);assert.equal(await session.flush(),true);assert.equal(session.hasUnsavedChanges(),false);assert.equal(session.read().heads[0].text,'Keep old text');
});

test('an identical new legacy write survives acknowledgement of the modern message',async t=>{
  const disk=diskFixture(),session=create(disk);t.after(()=>session.close());await session.ready;
  session.edit('Same text',null);await session.flush();const sent=session.read().selectedId;
  session.recoverLegacy({source:'v1',text:'Same text',replyTo:null});await session.flush();assert.equal(session.read().heads.length,2);
  await session.acknowledge(sent);assert.equal(session.read().value.text,'');assert.equal(session.read().heads[0].text,'Same text');
});

test('sequential typing in one old storage slot stays one recovered version',async t=>{
  const disk=diskFixture(),session=create(disk);t.after(()=>session.close());await session.ready;
  for(let i=1;i<=100;i++)session.recoverLegacy({source:'v1',text:'x'.repeat(i),replyTo:null});
  await session.flush();assert.equal(session.read().heads.length,1);assert.equal(session.read().heads[0].text,'x'.repeat(100));
});
