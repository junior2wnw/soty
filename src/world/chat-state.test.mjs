import test from 'node:test';
import assert from 'node:assert/strict';
import { createChatDraftStore, readChatForward } from './chat-state.mjs';

function storage() {
  const entries = new Map();
  return { getItem: key => entries.get(key), setItem: (key, value) => entries.set(key, value), removeItem: key => entries.delete(key) };
}
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

test('changing only the reply target creates a new request and a late ACK preserves it', () => {
  let request = 0; const drafts = createChatDraftStore(storage(), () => `request-${++request}`);
  const first = drafts.beginSend('alice', 'room', 'Да', 'msg-first');
  drafts.edit('alice', 'room', 'Да', 'msg-second');
  const late = drafts.acknowledge('alice', 'room', first.clientId);
  assert.equal(late.text, 'Да'); assert.equal(late.replyTo, 'msg-second');
  const second = drafts.beginSend('alice', 'room', 'Да');
  assert.notEqual(second.clientId, first.clientId); assert.equal(second.replyTo, 'msg-second');
  drafts.acknowledge('alice', 'room', first.clientId);
  assert.equal(drafts.read('alice', 'room').pending.clientId, second.clientId);
});

test('reply changes reset idempotency before a retry and legacy drafts remain readable', () => {
  const disk = storage(); let request = 0; const drafts = createChatDraftStore(disk, () => `request-${++request}`);
  const first = drafts.beginSend('alice', 'room', 'Да', 'msg-first');
  const second = drafts.beginSend('alice', 'room', 'Да', 'msg-second');
  assert.notEqual(second.clientId, first.clientId);
  disk.setItem('soty.chat.draft.v1:bob:legacy', JSON.stringify({ text: 'До обновления', pending: { clientId: 'legacy-request', text: 'До обновления' } }));
  const legacy = createChatDraftStore(disk, () => assert.fail('Legacy sends remain replay safe'));
  assert.equal(legacy.beginSend('bob', 'legacy', 'До обновления').clientId, 'legacy-request');
  assert.equal(legacy.read('bob', 'legacy').replyTo, null);
  drafts.edit('alice', 'room', '', 'msg-empty-reply');
  assert.equal(createChatDraftStore(disk).read('alice', 'room').replyTo, 'msg-empty-reply');
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
