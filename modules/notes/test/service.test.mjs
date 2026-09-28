import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createNotesService } from '../server/index.mjs';

const alice = Object.freeze({ accountId: 'account_alice', deviceId: 'device_alice' });
const bob = Object.freeze({ accountId: 'account_bobby', deviceId: 'device_bobby' });
function fixture(t, options = {}) {
  const base = resolve(tmpdir()); const directory = mkdtempSync(join(base, 'soty-notes-test-'));
  const databasePath = join(directory, 'notes.sqlite'); let now = 100;
  const service = createNotesService({ databasePath, projectId: 'notes-test', clock: () => now++, ...options });
  t.after(() => { service.close(); assert.equal(dirname(resolve(directory)), base); assert.ok(directory.startsWith(join(base, 'soty-notes-test-'))); rmSync(directory, { recursive: true, force: true }); });
  const call = (op, args = {}, actor = alice) => service.execute({ op: `notes.${op}`, args: { expectedAccountId: actor.accountId, ...args }, actor });
  return { service, call, databasePath };
}
let serial = 0;
const putArgs = (overrides = {}) => ({ noteId: 'note_initial', mutationId: `mutation_${++serial}`, expectedRevision: 0,
  title: 'Идея', body: 'Личная записка', items: [], color: 'plain', pinned: false, state: 'active', ...overrides });
const fail = (fn, code) => assert.throws(fn, error => error.code === code);

test('owner boundary applies to every projection and rejects account selection or stale UI identity', t => {
  const { call } = fixture(t); call('put', putArgs({ title: 'Секретный проект' }));
  assert.equal(call('list', {}, bob).notes.length, 0);
  fail(() => call('get', { noteId: 'note_initial' }, bob), 'notes_note_not_found');
  fail(() => call('purge', { noteId: 'note_initial', mutationId: 'purge_bobby', expectedRevision: 1 }, bob), 'notes_note_not_found');
  fail(() => call('put', putArgs({ expectedRevision: 1, title: 'Spoof' }), bob), 'notes_note_not_found');
  fail(() => call('list', { expectedAccountId: alice.accountId }, bob), 'notes_account_changed');
  fail(() => call('list', { accountId: bob.accountId }), 'notes_invalid_arguments');
  // Same client-generated ID may belong independently to two accounts.
  call('put', putArgs({ title: 'Боб' }), bob);
  assert.equal(call('get', { noteId: 'note_initial' }).note.title, 'Секретный проект');
  assert.equal(call('get', { noteId: 'note_initial' }, bob).note.title, 'Боб');
});

test('a lost acknowledgement is replay-safe after restart; stale branches cannot overwrite a newer revision', t => {
  const { call, service, databasePath } = fixture(t); const first = putArgs(); const ack = call('put', first);
  const latest = call('put', putArgs({ expectedRevision: 1, body: 'Телефон уже дописал' })); assert.equal(latest.revision, 2);
  assert.deepEqual(call('put', first), { ...ack, replayed: true });
  fail(() => call('put', { ...first, body: 'Тот же mutation ID с другим текстом' }), 'notes_mutation_reused');
  fail(() => call('put', putArgs({ expectedRevision: 1, body: 'Старая вкладка' })), 'notes_revision_conflict');
  service.close(); const reopened = createNotesService({ databasePath, projectId: 'notes-test' });
  try {
    assert.deepEqual(reopened.execute({ op: 'notes.put', args: { ...first, expectedAccountId: alice.accountId }, actor: alice }), { ...ack, replayed: true });
    assert.equal(reopened.execute({ op: 'notes.get', args: { expectedAccountId: alice.accountId, noteId: first.noteId }, actor: alice }).note.body, 'Телефон уже дописал');
  } finally { reopened.close(); }
});

test('bounded receipts cannot resurrect a stale create, and trash/purge removes contents from indexed search', t => {
  const { call } = fixture(t, { limits: { receiptsPerNote: 2 } }); const first = putArgs({ body: 'Уникальный маркер' }); call('put', first);
  for (let revision = 1; revision <= 3; revision++) call('put', putArgs({ expectedRevision: revision, body: 'Уникальный маркер' }));
  fail(() => call('put', first), 'notes_revision_conflict');
  fail(() => call('purge', { noteId: first.noteId, expectedRevision: 4, mutationId: 'purge_first' }), 'notes_trash_required');
  call('put', putArgs({ expectedRevision: 4, state: 'trashed', body: 'Уникальный маркер' }));
  assert.equal(call('list', { query: 'маркер' }).notes.length, 0);
  assert.equal(call('list', { query: 'маркер', bucket: 'trashed' }).notes.length, 1);
  const purge = { noteId: first.noteId, expectedRevision: 5, mutationId: 'purge_second' }; const ack = call('purge', purge);
  assert.deepEqual(call('purge', purge), { ...ack, replayed: true });
  fail(() => call('put', first), 'notes_note_deleted');
  fail(() => call('get', { noteId: first.noteId }), 'notes_note_not_found');
  assert.equal(call('list', { query: 'маркер', bucket: 'trashed' }).notes.length, 0);
  assert.deepEqual(call('list').usage.counts, { active: 0, archived: 0, trashed: 0 });
  assert.equal(call('list').usage.bytes, 0);
});

test('keyset pages preserve pin/time/id order and bounded metadata; search indexes body and checklist only for its owner', t => {
  const { call } = fixture(t, { clock: () => 100 });
  for (let index = 0; index < 53; index++) call('put', putArgs({ noteId: `note_${String(index).padStart(5, '0')}`, title: `Записка ${index}`, pinned: index % 4 === 0,
    body: index % 2 ? 'Телескоп и звёзды' : 'Море', items: [{ id: 'checklist_one', text: 'Купить штатив', done: false }] }));
  call('put', putArgs({ noteId: 'note_foreign', body: 'Телескоп и звёзды' }), bob);
  const found = []; let cursor;
  do { const page = call('list', { limit: 7, ...(cursor ? { cursor } : {}) }); assert.ok(page.notes.length <= 7); found.push(...page.notes); cursor = page.nextCursor; } while (cursor);
  assert.equal(found.length, 53); assert.equal(new Set(found.map(note => note.noteId)).size, 53);
  assert.ok(found.slice(0, 14).every(note => note.pinned)); assert.ok(found.slice(14).every(note => !note.pinned));
  assert.equal(Object.hasOwn(found[0], 'body'), false); assert.equal(Object.hasOwn(found[0], 'items'), false);
  assert.equal(call('list', { query: 'ТЕЛЕ звёз', limit: 40 }).notes.length, 26);
  assert.equal(call('list', { query: 'штатив', limit: 40 }).notes.length, 40);
  assert.equal(call('list', { query: '***" OR scope:', limit: 40 }).notes.length, 0);
  const firstCursor = call('list', { limit: 1 }).nextCursor;
  fail(() => call('list', { cursor: firstCursor }, bob), 'notes_invalid_cursor');
  fail(() => call('list', { cursor: firstCursor, query: 'море' }), 'notes_invalid_cursor');
  fail(() => call('list', { cursor: 'invalid' }), 'notes_invalid_cursor');
  fail(() => call('list', { limit: 10000 }), 'notes_invalid_arguments');
});

test('quotas and validation roll back document, search index and usage together', t => {
  const { call } = fixture(t, { limits: { notes: 2, identities: 3, noteBytes: 800, accountBytes: 900 } });
  const first = putArgs({ body: 'a'.repeat(350) }); call('put', first);
  const original = call('get', { noteId: first.noteId }).note;
  fail(() => call('put', putArgs({ expectedRevision: 1, body: 'x'.repeat(801) })), 'notes_note_too_large');
  fail(() => call('put', putArgs({ noteId: 'note_another', body: 'b'.repeat(400) })), 'notes_storage_quota');
  assert.deepEqual(call('get', { noteId: first.noteId }).note, original);
  call('put', putArgs({ noteId: 'note_another', body: '' }));
  fail(() => call('put', putArgs({ noteId: 'note_thirdid', body: '' })), 'notes_count_quota');
  const usage = call('list').usage; assert.equal(usage.counts.active, 2); assert.ok(usage.bytes <= 900);
  fail(() => call('put', putArgs({ expectedRevision: 1, items: [{ id: 'check_duplicate', text: 'A', done: false }, { id: 'check_duplicate', text: 'B', done: false }] })), 'notes_invalid_arguments');
  assert.deepEqual(call('list').usage, usage);
});

test('state transitions update counters atomically and permanent deletion is never automatic', t => {
  const { call } = fixture(t); call('put', putArgs());
  call('put', putArgs({ expectedRevision: 1, state: 'archived' }));
  assert.deepEqual(call('list').usage.counts, { active: 0, archived: 1, trashed: 0 });
  call('put', putArgs({ expectedRevision: 2, state: 'trashed' }));
  assert.deepEqual(call('list').usage.counts, { active: 0, archived: 0, trashed: 1 });
  call('put', putArgs({ expectedRevision: 3, state: 'active' }));
  assert.equal(call('get', { noteId: 'note_initial' }).note.body, 'Личная записка');
});

test('catalog uses owner/state index and bounded identities after tombstoning', t => {
  const { call, databasePath } = fixture(t, { limits: { identities: 1 } }); call('put', putArgs({ state: 'trashed' }));
  call('purge', { noteId: 'note_initial', expectedRevision: 1, mutationId: 'purge_identity' });
  fail(() => call('put', putArgs({ noteId: 'note_otherid' })), 'notes_identity_quota');
  const db = new DatabaseSync(databasePath); try {
    const plans = db.prepare('EXPLAIN QUERY PLAN SELECT id,title,preview FROM notes WHERE account_id=? AND state=? ORDER BY pinned DESC,updated_at DESC,id ASC LIMIT ?').all(alice.accountId, 'active', 40);
    assert.ok(plans.some(row => row.detail.includes('notes_owner_order')));
    assert.ok(plans.every(row => !row.detail.includes('SCAN notes') && !row.detail.includes('TEMP B-TREE')));
    assert.equal(db.prepare('SELECT body,title FROM notes').get().body, '');
  } finally { db.close(); }
});

test('project/database identity is fail-closed', t => {
  const { service, databasePath } = fixture(t); service.close();
  assert.throws(() => createNotesService({ databasePath, projectId: 'other-project' }), /notes_project_mismatch/);
});
