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
