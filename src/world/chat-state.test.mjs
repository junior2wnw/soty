import test from 'node:test';
import assert from 'node:assert/strict';
import { createChatDraftStore, readChatForward } from './chat-state.mjs';

function storage() {
  const entries = new Map();
  return { getItem: key => entries.get(key), setItem: (key, value) => entries.set(key, value), removeItem: key => entries.delete(key) };
}
test('old writes are recognized while current IndexedDB clients do not create false legacy versions',()=>{
  const disk=storage(),old=createChatDraftStore(disk),current=createChatDraftStore(disk,undefined,{indexedDB:true}),seen=[];
  current.subscribe('alice','room',(_draft,legacy)=>{if(legacy)seen.push(legacy);});
  current.edit('alice','room','Modern');const modernKey='soty.chat.draft.v3:alice:room',key='soty.chat.draft.v1:alice:room',modern=disk.getItem(modernKey);
  assert.equal(JSON.parse(modern)._sotyWriter,'soty.chat.idb.v1');assert.deepEqual(current.legacyCandidates('alice','room'),[]);
  current.storageChanged(modernKey,modern);assert.deepEqual(seen,[]);assert.equal(old.read('alice','room').text,'');
  old.edit('alice','room','Old tab');const legacy=disk.getItem(key);current.storageChanged(key,legacy);
  assert.deepEqual(seen,[{source:'v1',text:'Old tab',replyTo:null,fresh:true}]);
  current.setAttachments('alice','room',['file_001']);assert.equal(current.read('alice','room').text,'Modern');assert.deepEqual(current.read('alice','room').attachments,['file_001']);
  current.setAttachments('alice','room',[]);assert.equal(disk.getItem(key),legacy,'Modern writes never overwrite the old key');
  old.edit('alice','room','Newer old text');const newer=disk.getItem(key);current.storageChanged(key,newer);current.storageChanged(key,legacy);
  assert.equal(seen.at(-1).text,'Newer old text','A delayed storage event must not restore an older value');
  old.edit('alice','room','Old tab');
  const v2='soty.chat.draft.v2:alice:room',oldV2=JSON.stringify({text:'Old attachment draft',replyTo:null,attachments:['old_ref_001'],pending:{clientId:'old_intent_001',text:'Old attachment draft',replyTo:null,attachments:['old_ref_001']}});
  disk.setItem(v2,oldV2);
  assert.deepEqual(current.legacyCandidates('alice','room').map(item=>item.text),['Old tab','Old attachment draft']);
  current.storageChanged(v2,oldV2);current.setAttachments('alice','room',['modern_ref_001']);current.edit('alice','room','Modern next');
  assert.equal(disk.getItem(v2),oldV2,'Modern changes preserve old attachment intent');
  assert.equal(current.read('alice','room').text,'Modern next');assert.deepEqual(current.read('alice','room').attachments,['modern_ref_001']);
});

test('modern v3 attachment metadata follows a different modern tab',()=>{
  const disk=storage(),a=createChatDraftStore(disk,undefined,{indexedDB:true}),b=createChatDraftStore(disk,undefined,{indexedDB:true});
  b.setAttachments('alice','room',['old_ref_001']);
  a.setAttachments('alice','room',['new_ref_001']);const key='soty.chat.draft.v3:alice:room';
  b.storageChanged(key,disk.getItem(key));assert.deepEqual(b.read('alice','room').attachments,['new_ref_001']);
});
test('chat draft survives reload and retries a lost ACK with the same id', () => {
  const disk = storage(); const first = createChatDraftStore(disk, () => 'request-123');
  first.edit('account-a', 'room-a', 'Привет'); const send = first.beginSend('account-a', 'room-a', 'Привет');
  const reopened = createChatDraftStore(disk, () => 'must-not-be-used');
  assert.equal(reopened.read('account-a', 'room-a').text, 'Привет');
  assert.equal(reopened.beginSend('account-a', 'room-a', 'Привет').clientId, send.clientId);
  reopened.acknowledge('account-a', 'room-a', send.clientId);
  assert.equal(createChatDraftStore(disk).read('account-a', 'room-a').text, '');
});
test('late chat ACK preserves newer text, another room and another account', () => {
  const drafts = createChatDraftStore(storage(), () => 'request-123');
  const send = drafts.beginSend('alice', 'room', 'Старое');
  drafts.edit('alice', 'room', 'Новое'); drafts.edit('alice', 'other', 'Другая группа'); drafts.edit('bob', 'room', 'Другой человек');
  assert.equal(drafts.acknowledge('alice', 'room', send.clientId).text, 'Новое');
  assert.equal(drafts.read('alice', 'other').text, 'Другая группа');
  assert.equal(drafts.read('bob', 'room').text, 'Другой человек');
});
test('storage failure retains the draft in memory and prevents a safe update claim', () => {
  let failing = true; const disk = storage();
  const drafts = createChatDraftStore({ ...disk, setItem(key, value) { if (failing) throw new Error('quota'); disk.setItem(key, value); } });
  assert.equal(drafts.edit('alice', 'room', 'Не потерять'), false);
  assert.equal(drafts.hasVolatile(), true); assert.equal(drafts.read('alice', 'room').text, 'Не потерять');
  failing = false; assert.equal(drafts.retrySave('alice', 'room'), true); assert.equal(drafts.hasVolatile(), false);
  assert.equal(createChatDraftStore(disk).read('alice', 'room').text, 'Не потерять');
});

test('an acknowledged draft with failed cleanup stays a dirty tombstone until storage removal succeeds', () => {
  const disk = storage(); let denyRemoval = false, sends = 0;
  const failing = { ...disk, removeItem(key) { if (denyRemoval) throw Error('Synthetic storage access revoked'); disk.removeItem(key); } },
    drafts = createChatDraftStore(failing, () => `request-${++sends}`),
    pending = drafts.beginSend('alice', 'room', 'Already acknowledged');
  assert.equal(pending.durable, true); denyRemoval = true;
  const acknowledged = drafts.acknowledge('alice', 'room', pending.clientId);
  assert.equal(acknowledged.text, ''); assert.equal(acknowledged.pending, null);
  assert.equal(drafts.isVolatile('alice', 'room'), true); assert.equal(drafts.hasVolatile(), true);
  assert.equal(createChatDraftStore(disk).read('alice', 'room').pending.clientId, pending.clientId, 'Failed storage cannot claim durable cleanup');
  drafts.storageChanged('soty.chat.draft.v1:alice:room');
  assert.equal(drafts.read('alice', 'room').text, '', 'Stale disk metadata cannot revive the live tombstone');
  assert.equal(drafts.read('alice', 'room').pending, null);
  assert.equal(drafts.retrySave('alice', 'room'), false); assert.equal(drafts.hasVolatile(), true);
  denyRemoval = false; assert.equal(drafts.retrySave('alice', 'room'), true);
  assert.equal(drafts.hasVolatile(), false); assert.deepEqual(createChatDraftStore(disk).read('alice', 'room'), { text: '', replyTo: null, pending: null });
  assert.equal(sends, 1, 'Cleanup recovery must not create another send intent');
});

test('failed explicit clearing is visible without changing another account or a later draft', () => {
  const disk = storage(); let denyRemoval = false;
  const drafts = createChatDraftStore({ ...disk, removeItem(key) { if (denyRemoval) throw Error('Synthetic remove denied'); disk.removeItem(key); } });
  drafts.edit('alice', 'room', 'Discard this local draft'); drafts.edit('bob', 'room', 'Bob private');
  denyRemoval = true; assert.equal(drafts.edit('alice', 'room', ''), false);
  assert.equal(drafts.hasVolatile(), true); assert.equal(drafts.read('alice', 'room').text, '');
  drafts.edit('alice', 'room', 'A later thought');
  assert.equal(drafts.isVolatile('alice', 'room'), false, 'A later successful durable edit supersedes the tombstone');
  assert.equal(createChatDraftStore(disk).read('alice', 'room').text, 'A later thought');
  assert.equal(drafts.read('bob', 'room').text, 'Bob private');
});

test('actual World ACK cleanup failure recovers locally without another SQL message', async t => {
  const { createWorldService } = await import('../../modules/world/server/index.mjs');
  const world = createWorldService({ databasePath: ':memory:', projectId: 'draft-ack-test' }); t.after(() => world.close());
  const actor = { accountId: 'acct_draft_ack', deviceId: 'dev_draft_ack', label: 'Synthetic draft owner' },
    call = (op, args = {}) => world.execute({ actor, op: 'world.' + op, args });
  call('profile.get'); const communityId = call('community.create', { requestId: 'draft_room_001', name: 'Synthetic local room' }).community.communityId;
  const disk = storage(); let denyRemoval = false;
  const drafts = createChatDraftStore({ ...disk, removeItem(key) { if (denyRemoval) throw Error('Synthetic cleanup denied'); disk.removeItem(key); } }, () => 'draft_send_001'),
    pending = drafts.beginSend(actor.accountId, communityId, 'One real stored message'),
    accepted = call('chat.send', { communityId, clientId: pending.clientId, text: pending.text, replyTo: pending.replyTo }).message;
  assert.equal(call('chat.list', { communityId }).messages[0].messageId, accepted.messageId);
  denyRemoval = true; drafts.acknowledge(actor.accountId, communityId, pending.clientId);
  assert.equal(drafts.hasVolatile(), true); assert.equal(drafts.read(actor.accountId, communityId).pending, null);
  denyRemoval = false; assert.equal(drafts.retrySave(actor.accountId, communityId), true);
  assert.equal(createChatDraftStore(disk).read(actor.accountId, communityId).text, '');
  assert.equal(call('chat.list', { communityId }).messages.length, 1);
});
test('an ACK updates a reopened composer and preserves newer text written in another tab', () => {
  const disk = storage(); const first = createChatDraftStore(disk, () => 'request-123');
  const pending = first.beginSend('alice', 'room', 'Отправляю');
  const visible = []; const unsubscribe = first.subscribe('alice', 'room', draft => visible.push(draft.text));
  const second = createChatDraftStore(disk); second.edit('alice', 'room', 'Следующая мысль');
  first.acknowledge('alice', 'room', pending.clientId);
  assert.deepEqual(visible, ['Следующая мысль']); assert.equal(createChatDraftStore(disk).read('alice', 'room').text, 'Следующая мысль');
  unsubscribe();
});

test('a reply survives reload and a lost ACK without creating a different request', () => {
  const disk = storage(); const first = createChatDraftStore(disk, () => 'reply-request-123');
  first.edit('alice', 'room', 'Да, договорились', 'msg-original');
  const pending = first.beginSend('alice', 'room', 'Да, договорились');
  const reopened = createChatDraftStore(disk, () => assert.fail('A retry reuses its client id'));
  assert.equal(reopened.read('alice', 'room').replyTo, 'msg-original');
  assert.equal(reopened.beginSend('alice', 'room', 'Да, договорились').replyTo, 'msg-original');
  assert.equal(reopened.beginSend('alice', 'room', 'Да, договорились').clientId, pending.clientId);
  const acknowledged = reopened.acknowledge('alice', 'room', pending.clientId);
  assert.equal(acknowledged.text, ''); assert.equal(acknowledged.replyTo, null);
});

test('a changed reply stays a next draft until the unknown original send is acknowledged', () => {
  let request = 0; const drafts = createChatDraftStore(storage(), () => `request-${++request}`);
  const first = drafts.beginSend('alice', 'room', 'Да', 'msg-first');
  drafts.edit('alice', 'room', 'Да', 'msg-second');
  const retry = drafts.beginSend('alice', 'room', 'Да', 'msg-second');
  assert.equal(retry.clientId, first.clientId); assert.equal(retry.replyTo, 'msg-first');
  const late = drafts.acknowledge('alice', 'room', first.clientId);
  assert.equal(late.text, 'Да'); assert.equal(late.replyTo, 'msg-second');
  const second = drafts.beginSend('alice', 'room', 'Да');
  assert.notEqual(second.clientId, first.clientId); assert.equal(second.replyTo, 'msg-second');
  drafts.acknowledge('alice', 'room', first.clientId);
  assert.equal(drafts.read('alice', 'room').pending.clientId, second.clientId);
});

test('reply changes cannot replace a lost send intent and legacy drafts remain readable', () => {
  const disk = storage(); let request = 0; const drafts = createChatDraftStore(disk, () => `request-${++request}`);
  const first = drafts.beginSend('alice', 'room', 'Да', 'msg-first');
  drafts.edit('alice', 'room', 'Да', 'msg-second');
  const retry = drafts.beginSend('alice', 'room', 'Да', 'msg-second');
  assert.equal(retry.clientId, first.clientId); assert.equal(retry.replyTo, 'msg-first');
  drafts.acknowledge('alice', 'room', first.clientId);
  const second = drafts.beginSend('alice', 'room', 'Да');
  assert.notEqual(second.clientId, first.clientId);
  disk.setItem('soty.chat.draft.v1:bob:legacy', JSON.stringify({ text: 'До обновления', pending: { clientId: 'legacy-request', text: 'До обновления' } }));
  const legacy = createChatDraftStore(disk, () => assert.fail('Legacy sends remain replay safe'));
  assert.equal(legacy.beginSend('bob', 'legacy', 'До обновления').clientId, 'legacy-request');
  assert.equal(legacy.read('bob', 'legacy').replyTo, null);
  drafts.edit('alice', 'room', '', 'msg-empty-reply');
  assert.equal(createChatDraftStore(disk).read('alice', 'room').replyTo, 'msg-empty-reply');
});

test('lost SQL send ACK, changed next text and reload recover one original message before the next intent', async t => {
  const { createWorldService } = await import('../../modules/world/server/index.mjs');
  const world = createWorldService({ databasePath: ':memory:', projectId: 'lost-ack-recovery' }); t.after(() => world.close());
  const alice = { accountId: 'acct_lost_alice', deviceId: 'dev_lost_alice', label: 'Alice' },
    bob = { accountId: 'acct_lost_bob', deviceId: 'dev_lost_bob', label: 'Bob' },
    call = (actor, op, args = {}) => world.execute({ actor, op: 'world.' + op, args });
  call(alice, 'profile.get'); call(bob, 'profile.get');
  const room = call(alice, 'community.create', { requestId: 'lost_room_001', name: 'Disposable recovery room' }).community.communityId;
  call(bob, 'membership.join', { communityId: room });
  const disk = storage(); let requests = 0, drafts = createChatDraftStore(disk, () => `lost_intent_${++requests}`);
  const pending = drafts.beginSend(alice.accountId, room, 'Original message');
  const accepted = call(alice, 'chat.send', { communityId: room, clientId: pending.clientId, text: pending.text }).message;
  // The reply is deliberately lost after the real SQL transaction committed.
  drafts.edit(alice.accountId, room, 'Next thought'); drafts = createChatDraftStore(disk, () => `lost_intent_${++requests}`);
  const retry = drafts.beginSend(alice.accountId, room, 'Next thought');
  assert.equal(retry.clientId, pending.clientId); assert.equal(retry.text, 'Original message');
  assert.equal(call(alice, 'chat.send', { communityId: room, clientId: retry.clientId, text: retry.text }).message.messageId, accepted.messageId);
  assert.equal(call(bob, 'chat.list', { communityId: room }).messages.length, 1);
  const own = call(alice, 'chat.list', { communityId: room }).messages[0];
  assert.equal(own.clientId, pending.clientId); assert.equal(own.read, false);
  assert.equal(Object.hasOwn(call(bob, 'chat.list', { communityId: room }).messages[0], 'clientId'), false);
  drafts.acknowledge(alice.accountId, room, own.clientId);
  assert.equal(drafts.read(alice.accountId, room).text, 'Next thought'); assert.equal(drafts.read(alice.accountId, room).pending, null);
  const next = drafts.beginSend(alice.accountId, room, 'Next thought'); assert.notEqual(next.clientId, pending.clientId);
  call(alice, 'chat.send', { communityId: room, clientId: next.clientId, text: next.text });
  assert.deepEqual(call(bob, 'chat.list', { communityId: room }).messages.map(value => value.text), ['Original message', 'Next thought']);
  assert.equal(requests, 2);
});
test('forward chat catch-up closes a gap larger than two pages in order', async () => {
  const rows = Array.from({ length: 155 }, (_, index) => ({ seq: 101 + index, messageId: `msg-${index}` }));
  const seen = [], cursors = [];
  const result = await readChatForward({ after: 100, fetchPage: async cursor => { cursors.push(cursor); const next = rows.filter(row => row.seq > cursor); return { messages: next.slice(0, 60), hasMore: next.length > 60 }; }, append: message => seen.push(message.seq) });
  assert.deepEqual(cursors, [100, 160, 220]); assert.deepEqual(seen, rows.map(row => row.seq));
  assert.deepEqual(result, { cursor: 255, hasMore: false });
});
test('chat catch-up yields at a page budget and stops after screen disposal', async () => {
  const rows = Array.from({ length: 300 }, (_, index) => ({ seq: index + 1 })); const seen = [];
  const fetchPage = async cursor => { const next = rows.filter(row => row.seq > cursor); return { messages: next.slice(0, 60), hasMore: next.length > 60 }; };
  const first = await readChatForward({ after: 0, fetchPage, append: row => seen.push(row.seq), pageBudget: 2 });
  assert.deepEqual(first, { cursor: 120, hasMore: true });
  const second = await readChatForward({ after: first.cursor, fetchPage, append: row => seen.push(row.seq) });
  assert.equal(second.cursor, 300); assert.equal(seen.length, 300);
  let active = true;
  const disposed = await readChatForward({ after: 300, fetchPage: async () => { active = false; return { messages: [{ seq: 301 }], hasMore: false }; }, active: () => active, append: () => assert.fail('Disposed screen receives no messages') });
  assert.equal(disposed.cursor, 300);
});

test('accepting another tab’s pending message retains this tab’s identical new draft', () => {
  const disk=storage(),drafts=createChatDraftStore(disk,()=> 'new_intent');
  drafts.edit('alice','room','Same text');
  const own=drafts.beginSend('alice','room','Same text');assert.equal(own.clientId,'new_intent');
  drafts.recoverPending('alice','room',{clientId:'older_intent',text:'Same text',replyTo:null,attachments:[]});
  const reopened=createChatDraftStore(disk);
  assert.equal(reopened.read('alice','room').pending.clientId,'older_intent');
  reopened.acknowledge('alice','room','older_intent');
  assert.equal(reopened.read('alice','room').text,'Same text');assert.equal(reopened.read('alice','room').pending,null);
});

test('a second tab discovering the first tab pending preserves its identical new draft', () => {
  const disk=storage(),first=createChatDraftStore(disk,()=> 'intent_first'),second=createChatDraftStore(disk,()=> 'intent_second');
  first.edit('alice','room','Same text');second.read('alice','room');
  assert.equal(first.beginSend('alice','room','Same text').clientId,'intent_first');
  const reused=second.beginSend('alice','room','Same text');assert.equal(reused.clientId,'intent_first');assert.equal(reused.reusedOther,true);
  second.acknowledge('alice','room',reused.clientId);
  assert.equal(createChatDraftStore(disk).read('alice','room').text,'Same text');assert.equal(createChatDraftStore(disk).read('alice','room').pending,null);
});

test('typing a new draft after send and returning to the same text is not erased by the old acknowledgement', () => {
  const disk=storage(),drafts=createChatDraftStore(disk,()=> 'intent_first');
  drafts.beginSend('alice','room','Same text');drafts.edit('alice','room','New text');drafts.edit('alice','room','Same text');
  drafts.acknowledge('alice','room','intent_first');assert.equal(createChatDraftStore(disk).read('alice','room').text,'Same text');
});
