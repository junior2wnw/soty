import test from 'node:test';
import assert from 'node:assert/strict';
import { createAssistantAnswerNote } from './assistant-answer-note.ts';
import { createNoteSession } from '../../modules/notes/browser/state.mjs';
import { createNotesService } from '../../modules/notes/server/index.mjs';

const actor = { accountId: 'answer_owner', deviceId: 'answer_device' };
const answer = 'Спасибо за обращение.\nУточните, на каком шаге возникла ошибка.';
function fixture(t, transport = request => request) {
  const service = createNotesService({ databasePath: ':memory:', projectId: 'assistant-answer-note-test' });
  t.after(() => service.close());
  let currentAccount = actor.accountId, localAvailable = true;
  const records = new Map(), calls = [];
  const store = {
    async put(value) { if (!localAvailable) throw new Error('notes_local_unavailable'); records.set(value.branchId, structuredClone(value)); },
    async list() { return [...records.values()].map(value => structuredClone(value)); },
    async remove(noteId, branchId, savedAt) { const value = records.get(branchId);
      if (value?.note.noteId === noteId && (savedAt === undefined || value.savedAt === savedAt)) records.delete(branchId); },
  };
  const request = transport(async (op, args) => {
    calls.push({ op, args: structuredClone(args) });
    return service.execute({ op, args, actor });
  });
  const api = { request };
  const note = createAssistantAnswerNote({ accountId: actor.accountId, answer, api, store, isCurrent: async () => currentAccount === actor.accountId });
  t.after(() => note.dispose());
  const read = noteId => service.execute({ op: 'notes.get', args: { expectedAccountId: actor.accountId, noteId }, actor }).note;
  return { note, api, store, calls, read, account(value) { currentAccount = value; }, local(value) { localAvailable = value; } };
}

test('explicit save creates one readable ordinary Note and coalesces repeated clicks', async t => {
  const f = fixture(t);
  assert.deepEqual(f.note.view(), { phase: 'idle' }); assert.equal(f.calls.length, 0);
  const saving = f.note.save(); assert.equal(f.note.save(), saving);
  await saving;
  const result = f.note.view(); assert.equal(result.phase, 'saved');
  const stored = f.read(result.noteId);
  assert.equal(stored.body, answer); assert.equal(stored.title, 'Черновик ответа помощника'); assert.equal(stored.revision, 1);
  assert.deepEqual(stored.items, []); assert.equal(stored.state, 'active');
  await f.note.save(); assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].op, 'notes.put'); assert.equal(f.calls[0].args.expectedAccountId, actor.accountId);
  assert.equal((await f.store.list()).length, 0);
});

test('lost ACK remains a durable Notes draft and recovers the same mutation after remount', async t => {
  let loseAck = true;
  const f = fixture(t, request => async (op, args) => {
    const result = await request(op, args);
    if (loseAck) { loseAck = false; throw new Error('network_lost'); }
    return result;
  });
  await f.note.save();
  assert.equal(f.note.view().phase, 'pending');
  const draft = (await f.store.list())[0];
  assert.equal(draft.pending.mutationId, f.calls[0].args.mutationId);
  assert.equal(f.read(draft.note.noteId).revision, 1);
  f.note.dispose(); await new Promise(resolve => setImmediate(resolve));
  const recovered = (await f.store.list())[0];
  const session = createNoteSession({ api: f.api, accountId: actor.accountId, store: f.store,
    note: recovered.note, draft: recovered, debounceMs: 60000 });
  t.after(() => session.dispose());
  await session.flush();
  assert.deepEqual(f.calls[1], f.calls[0]);
  assert.equal(session.state().dirty, false); assert.equal(f.read(draft.note.noteId).revision, 1);
  assert.equal(f.read(draft.note.noteId).body, answer);
});

test('account switch before save refuses the write', async t => {
  const f = fixture(t); f.account('another_owner');
  await assert.rejects(f.note.save(), /notes_account_changed/);
  assert.equal(f.calls.length, 0); assert.deepEqual(f.note.view(), { phase: 'closed' });
});

test('account switch while the ACK is pending cannot expose a saved note to the new account', async t => {
  let release, started;
  const gate = new Promise(resolve => { release = resolve; }), beginning = new Promise(resolve => { started = resolve; });
  const f = fixture(t, request => async (op, args) => {
    const result = await request(op, args); started(); await gate; return result;
  });
  const saving = f.note.save(); await beginning;
  f.account('another_owner'); release(); await assert.rejects(saving, /notes_account_changed/);
  assert.deepEqual(f.note.view(), { phase: 'closed' });
  assert.equal(f.calls.length, 1); assert.equal(f.calls[0].args.expectedAccountId, actor.accountId);
  const draft = (await f.store.list())[0];
  assert.equal(draft.pending.mutationId, f.calls[0].args.mutationId);
});

test('local outbox failure prevents dispatch; retry saves the same answer through Notes', async t => {
  const f = fixture(t); f.local(false);
  await assert.rejects(f.note.save(), /notes_local_unavailable/);
  assert.equal(f.calls.length, 0); assert.equal(f.note.view().phase, 'local-error');
  f.local(true); await f.note.save();
  assert.equal(f.note.view().phase, 'saved'); assert.equal(f.calls.length, 1);
  assert.equal(f.read(f.note.view().noteId).body, answer);
});

test('wrong ACK is not success and a manual check replays the exact Notes mutation', async t => {
  let wrong = true;
  const f = fixture(t, request => async (op, args) => {
    const result = await request(op, args);
    if (wrong) { wrong = false; return { ...result, noteId: 'wrong_note' }; }
    return result;
  });
  await f.note.save(); assert.equal(f.note.view().phase, 'pending');
  await f.note.save(); assert.equal(f.note.view().phase, 'saved');
  assert.deepEqual(f.calls[1], f.calls[0]); assert.equal(f.read(f.note.view().noteId).revision, 1);
});

test('an unavailable account check is visible and cannot create a local or server note', async () => {
  const note = createAssistantAnswerNote({ accountId: actor.accountId, answer,
    api: { request() { assert.fail('no dispatch'); } }, store: { put() { assert.fail('no local write'); } },
    isCurrent: async () => { throw new Error('offline'); } });
  await assert.rejects(note.save(), /offline/);
  assert.deepEqual(note.view(), { phase: 'unavailable' }); note.dispose();
});
