import test from 'node:test';
import assert from 'node:assert/strict';
import { blankNote, createNoteSession } from '../browser/state.mjs';
import { createNotesService } from '../server/index.mjs';
const actor = { accountId: 'account_browser', deviceId: 'device_browser' };
function memoryStore() {
  const records = new Map(); return {
    async put(value) { records.set(value.branchId, structuredClone(value)); },
    async list() { return [...records.values()].map(value => structuredClone(value)); },
    async remove(noteId, branchId, savedAt) { const value = records.get(branchId); if (value?.note.noteId === noteId && (savedAt === undefined || value.savedAt === savedAt)) records.delete(branchId); },
  };
}
function fixture(t, transport) {
  const service = createNotesService({ databasePath: ':memory:', projectId: 'notes-browser-test' }); t.after(() => service.close());
  const request = async (op, args) => service.execute({ op, args, actor });
  const api = { request: transport ? transport(request) : request }; const store = memoryStore();
  const create = extra => { const session = createNoteSession({ api, accountId: actor.accountId, store, note: blankNote(), debounceMs: 60000, ...extra }); t.after(() => session.dispose()); return session; };
  const read = noteId => service.execute({ op: 'notes.get', args: { expectedAccountId: actor.accountId, noteId }, actor }).note;
  return { create, store, read, api };
}
test('lost server ACK survives reload; newer local edits follow the same outbox exactly once', async t => {
  let loseAck = true; const sent = [];
  const { create, store, read } = fixture(t, request => async (op, args) => {
    sent.push(structuredClone(args)); const result = await request(op, args);
    if (loseAck) { loseAck = false; throw new Error('network_lost'); } return result;
  });
  const session = create(); const noteId = session.state().note.noteId;
  session.edit({ body: 'Первая мысль' }); await session.flush();
  assert.equal(session.state().dirty, true); assert.equal(session.state().localDurable, true); assert.equal(read(noteId).revision, 1);
  session.edit({ body: 'Первая мысль и продолжение' }); session.dispose();
  // Dispose queues a durable snapshot; flush waits its completion without losing the original pending mutation.
  const draft = await new Promise(resolve => setTimeout(async () => resolve((await store.list())[0]), 0));
  assert.equal(draft.pending.expectedRevision, 0); assert.equal(draft.note.body, 'Первая мысль и продолжение');
  const restored = create({ note: draft.note, draft }); await restored.flush();
  assert.equal(read(noteId).body, 'Первая мысль и продолжение'); assert.equal(read(noteId).revision, 2);
  assert.equal(sent[0].mutationId, sent[1].mutationId); assert.equal(sent[0].body, sent[1].body);
  assert.equal(restored.state().dirty, false); assert.equal((await store.list()).length, 0);
});

test('typing during a delayed request does not get marked saved by the older acknowledgement', async t => {
  let release; let begun; const started = new Promise(resolve => { begun = resolve; }); const gate = new Promise(resolve => { release = resolve; }); let first = true;
  const { create, read } = fixture(t, request => async (op, args) => { if (first) { first = false; begun(); await gate; } return request(op, args); });
  const session = create(); const noteId = session.state().note.noteId;
  session.edit({ body: 'A' }); const saving = session.retry(); await started;
  session.edit({ body: 'AB', items: [{ id: 'item_browser', text: 'Проверить', done: true }] });
  assert.equal(session.state().dirty, true); release(); await saving;
  assert.equal(read(noteId).body, 'AB'); assert.equal(read(noteId).items[0].done, true); assert.equal(read(noteId).revision, 2);
  assert.equal(session.state().dirty, false);
});

test('backend recovery during an older request timeout retries the same outbox once and coalesces signals', async t => {
  let release; let begun; const started = new Promise(resolve => { begun = resolve; }); const gate = new Promise(resolve => { release = resolve; }); const sent = [];
  const { create, store, read } = fixture(t, request => async (op, args) => {
    sent.push(structuredClone(args)); const ack = await request(op, args);
    if (sent.length === 1) { begun(); await gate; throw new Error('NETWORK_TIMEOUT'); }
    return ack;
  });
  const session = create(); session.edit({ body: 'Сервер уже вернулся, но прежний ответ ещё ожидается' });
  const initial = session.retry(); await started;
  const recovery = session.retry(); const duplicate = session.retry();
  assert.equal(recovery, duplicate); assert.equal(sent.length, 1);
  release(); await Promise.all([initial, recovery, duplicate]);
  assert.equal(sent.length, 2); assert.deepEqual(sent[1], sent[0]);
  assert.equal(read(session.state().note.noteId).revision, 1);
  assert.equal(session.state().dirty, false); assert.equal(session.state().error, ''); assert.equal((await store.list()).length, 0);
});

test('one recovery signal does not become a retry loop when the follow-up also fails', async t => {
  let release; let begun; const started = new Promise(resolve => { begun = resolve; }); const gate = new Promise(resolve => { release = resolve; }); const sent = [];
  const { create, store } = fixture(t, () => async (_op, args) => {
    sent.push(structuredClone(args)); if (sent.length === 1) { begun(); await gate; }
    throw new Error('NETWORK_TIMEOUT');
  });
  const session = create(); session.edit({ body: 'Повтор остаётся ограниченным' });
  const initial = session.flush(); await started; const recovery = session.retry(); const duplicate = session.retry();
  release(); await Promise.all([initial, recovery, duplicate]);
  assert.equal(sent.length, 2); assert.deepEqual(sent[1], sent[0]);
  assert.equal(session.state().dirty, true); assert.equal(session.state().localDurable, true);
  assert.equal((await store.list())[0].pending.mutationId, sent[0].mutationId);
});

test('disposing during a pending recovery or local persist cannot start another network request', async t => {
  let release; let begun; const started = new Promise(resolve => { begun = resolve; }); const gate = new Promise(resolve => { release = resolve; }); let sends = 0;
  const { create, store } = fixture(t, () => async () => { sends++; begun(); await gate; throw new Error('NETWORK_TIMEOUT'); });
  const session = create(); session.edit({ body: 'Черновик после выхода остаётся локально' });
  const initial = session.retry(); await started; const recovery = session.retry(); session.dispose(); release();
  await Promise.all([initial, recovery]); await session.retry(); await session.flush();
  assert.equal(sends, 1); assert.equal(session.state().dirty, true);
  assert.equal((await store.list())[0].note.body, 'Черновик после выхода остаётся локально');

  let releaseLocal; let beganLocal; const localStarted = new Promise(resolve => { beganLocal = resolve; }); const localGate = new Promise(resolve => { releaseLocal = resolve; });
  const durable = memoryStore(); let firstWrite = true;
  const pendingLocal = create({ store: { ...durable, async put(value) { if (firstWrite) { firstWrite = false; beganLocal(); await localGate; } return durable.put(value); } } });
  pendingLocal.edit({ body: 'Уходим до завершения IndexedDB' }); const savingLocal = pendingLocal.retry(); await localStarted;
  pendingLocal.dispose(); releaseLocal(); await savingLocal; await pendingLocal.flush();
  assert.equal(sends, 1); assert.equal((await durable.list())[0].note.body, 'Уходим до завершения IndexedDB');
});

test('two concurrent branches preserve conflicting text and do not send repeated failed overwrites', async t => {
  const { create, store, read } = fixture(t); const initial = create(); initial.edit({ title: 'Общая для моих устройств', body: 'Начало' }); await initial.flush();
  const note = read(initial.state().note.noteId); const laptop = create({ note }); const phone = create({ note });
  laptop.edit({ body: 'Версия компьютера' }); await laptop.flush();
  phone.edit({ body: 'Версия телефона' }); await phone.flush();
  assert.equal(phone.state().conflict, true); assert.equal(phone.state().note.body, 'Версия телефона'); assert.equal(phone.hasUnsavedChanges(), false);
  assert.equal(read(note.noteId).body, 'Версия компьютера');
  const draft = (await store.list()).find(value => value.branchId === phone.state().branchId); assert.equal(draft.conflict, true);
  await phone.retry(); assert.equal(read(note.noteId).revision, 2);
  const restored = create({ draft, note: draft.note }); await restored.flush();
  assert.equal(restored.state().conflict, true); assert.equal((await store.list()).length, 1);
});

test('offline drafting stays recoverable; quota failure never reports durable and never sends a nonrecoverable write', async t => {
  let sends = 0; const { create, store } = fixture(t, () => async () => { sends++; throw new Error('offline'); });
  const session = create(); session.edit({ body: 'Без сети' }); await session.flush();
  assert.equal(session.state().dirty, true); assert.equal(session.state().localDurable, true); assert.equal((await store.list())[0].note.body, 'Без сети');
  const failing = create({ store: { ...memoryStore(), async put() { throw new Error('notes_local_quota'); } } });
  const before = sends; failing.edit({ body: 'Недостаточно места' }); await assert.rejects(failing.flush(), /notes_local_quota/);
  assert.equal(failing.state().localDurable, false); assert.equal(failing.hasUnsavedChanges(), true); assert.equal(sends, before);
});

test('recovering a draft cannot delete a newer branch written by an already open tab', async t => {
  const { create, store } = fixture(t, () => async () => { throw new Error('offline'); });
  const original = create(); original.edit({ body: 'Ранний черновик' }); await original.flush();
  const earlier = (await store.list())[0]; original.edit({ body: 'Открытая вкладка продолжила' }); await original.flush();
  const recovered = create({ draft: earlier, note: earlier.note }); await recovered.flush();
  const values = await store.list(); assert.equal(values.length, 2);
  assert.ok(values.some(value => value.note.body === 'Открытая вкладка продолжила'));
  assert.ok(values.some(value => value.note.body === 'Ранний черновик'));
});

test('retiring a resolved branch then disposing cannot resurrect it in draft recovery', async t => {
  const { create, store } = fixture(t, () => async () => { throw new Error('offline'); });
  const retired = create(); retired.edit({ body: 'Эта ветка уже сохранена отдельной копией' }); await retired.flush();
  assert.equal((await store.list()).length, 1);
  await retired.discardBranch(); retired.dispose();
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.equal((await store.list()).length, 0);
});
