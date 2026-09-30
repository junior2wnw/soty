import test from 'node:test';
import assert from 'node:assert/strict';
import { createNoteCache, NOTE_CACHE_LIMITS, noteCacheErrorPolicy, planNoteCacheWrite, readNoteWithCache } from '../browser/cache.mjs';
import { blankNote, createNoteSession } from '../browser/state.mjs';
import { createNotesService } from '../server/index.mjs';

const clone = value => structuredClone(value);
const fail = code => Object.assign(new Error(code), { code });
const actor = { accountId: 'account_cache', deviceId: 'device_cache' };
const origin = 'https://notes.example';
// Transactional in-memory driver for cache policy/network/session composition.
// The actual IndexedDB driver remains part of the separate real-browser gate.
function memoryStorage() {
  const records = new Map(); let epoch = crypto.randomUUID();
  return {
    records,
    async capture() { return epoch; },
    async read(scope, noteId, token, now) {
      if (token && token !== epoch) return null;
      const key = JSON.stringify([scope, noteId]); const record = records.get(key);
      if (!record) return null; record.usedAt = now; return clone(record);
    },
    async list(scope, token) { return token && token !== epoch ? [] : [...records.values()].filter(record => record.scope === scope).map(clone); },
    async put(candidate, token, limits) {
      if (token !== epoch) return false;
      const plan = planNoteCacheWrite([...records.values()], candidate, limits);
      if (plan.accepted) { for (const key of plan.remove) records.delete(key); records.set(candidate.key, clone(candidate)); }
      return plan.accepted;
    },
    async invalidate(scope, noteId) {
      epoch = crypto.randomUUID();
      for (const [key, record] of records) if (record.scope === scope && (noteId === undefined || record.note.noteId === noteId)) records.delete(key);
    },
  };
}
function memoryDrafts() {
  const records = new Map(); return {
    async put(value) { records.set(value.branchId, clone(value)); },
    async list() { return [...records.values()].map(clone); },
    async remove(noteId, branchId, savedAt) { const record = records.get(branchId); if (record?.note.noteId === noteId && (savedAt === undefined || savedAt === record.savedAt)) records.delete(branchId); },
  };
}
function fixture(t) {
  const service = createNotesService({ databasePath: ':memory:', projectId: 'cache-test' }); t.after(() => service.close());
  const api = { request: async (op, args) => service.execute({ op, args, actor }) };
  const storage = memoryStorage(); let clock = 100;
  const cache = createNoteCache(actor.accountId, 'cache-test', { origin, storage, clock: () => ++clock });
  const store = memoryDrafts();
  const session = extra => { const value = createNoteSession({ api, accountId: actor.accountId, store, cache, note: blankNote(), debounceMs: 60000, ...extra }); t.after(() => value.dispose()); return value; };
  const read = (noteId, extra) => readNoteWithCache({ api, accountId: actor.accountId, noteId, cache, ...extra });
  return { service, api, storage, cache, store, session, read };
}
const acknowledged = (body = 'Подтверждено', extra = {}) => ({ ...blankNote(), body, preview: body.slice(0, 180), revision: 1, ...extra });
const offline = { request: async () => { throw fail('NETWORK_ERROR'); } };

test('an acknowledged clean note survives outbox cleanup and a later offline open', async t => {
  const { session, store, cache, read } = fixture(t); const current = session();
  current.edit({ title: 'Сохранённая мысль', body: 'Есть и после закрытия сервера' }); await current.flush();
  assert.equal(current.state().dirty, false); assert.deepEqual(await store.list(), []);
  const result = await read(current.state().note.noteId, { api: offline });
  assert.equal(result.source, 'cache'); assert.equal(result.note.body, 'Есть и после закрытия сервера'); assert.equal(result.note.revision, 1);
  assert.ok(result.verifiedAt > 0); assert.equal((await cache.list()).length, 1);
});

test('server get populates only the requested own note, and older revisions cannot regress its snapshot', async t => {
  const { session, cache, read } = fixture(t); const writer = session({ cache: undefined });
  writer.edit({ body: 'Первая' }); await writer.flush(); const first = await read(writer.state().note.noteId);
  assert.equal(first.source, 'server'); assert.equal(first.cached, true);
  writer.edit({ body: 'Вторая' }); await writer.flush(); await read(writer.state().note.noteId);
  assert.equal(await cache.remember(first.note, await cache.capture()), false);
  const saved = await cache.get(first.note.noteId); assert.equal(saved.note.body, 'Вторая'); assert.equal(saved.note.revision, 2);
  assert.equal(await cache.remember({ ...saved.note, body: 'Другая запись с той же версией' }, await cache.capture()), false);
});

test('scope isolation covers origin, project and account, even in a deliberately shared test backend', async () => {
  const storage = memoryStorage(); const one = createNoteCache('account_1', 'project_1', { storage, origin }); const note = acknowledged();
  await one.remember(note, await one.capture());
  for (const [account, project, site] of [['account_2', 'project_1', origin], ['account_1', 'project_2', origin], ['account_1', 'project_1', 'https://other.example']]) {
    const other = createNoteCache(account, project, { storage, origin: site }); assert.equal(await other.get(note.noteId), null); assert.deepEqual(await other.list(), []);
  }
  assert.equal((await one.get(note.noteId)).note.body, note.body);
});

test('count/byte LRU bounds evict clean snapshots only; touches change victim and dirty outbox stays intact', async () => {
  const storage = memoryStorage(); let now = 0;
  const cache = createNoteCache('account_1', 'project_1', { origin, storage, clock: () => ++now, limits: { scopeCount: 2, count: 3, scopeBytes: 1800, bytes: 2600, entryBytes: 1500 } });
  const dirtyStore = memoryDrafts(); const dirty = { note: acknowledged('Черновик, который нельзя потерять'), branchId: 'branch', pending: { mutationId: 'pending' } }; await dirtyStore.put(dirty);
  const [a, b, c] = ['A', 'B', 'C'].map(body => acknowledged(body));
  for (const note of [a, b]) assert.equal(await cache.remember(note, await cache.capture()), true);
  await cache.get(a.noteId); await cache.remember(c, await cache.capture());
  assert.equal(await cache.get(b.noteId), null); assert.equal((await cache.list()).length, 2);
  const large = acknowledged('Ж'.repeat(900)); assert.equal(await cache.remember(large, await cache.capture()), false);
  const d = acknowledged('D'.repeat(650)); await cache.remember(d, await cache.capture());
  assert.ok([...storage.records.values()].reduce((sum, record) => sum + record.bytes, 0) <= 1800);
  assert.deepEqual(await dirtyStore.list(), [dirty]);
});

test('the origin-wide count/bytes budget also bounds many account/project scopes', async () => {
  const storage = memoryStorage(); const limits = { count: 3, bytes: 2400, scopeCount: 2, scopeBytes: 1500, entryBytes: 1400 };
  for (let index = 0; index < 9; index++) {
    const cache = createNoteCache(`account_${index}`, `project_${index}`, { origin, storage, limits, clock: () => index + 1 });
    await cache.remember(acknowledged('A'.repeat(200)), await cache.capture());
  }
  assert.ok(storage.records.size <= 3); assert.ok([...storage.records.values()].reduce((sum, record) => sum + record.bytes, 0) <= 2400);
  assert.ok(NOTE_CACHE_LIMITS.count >= storage.records.size);
});

test('only the actual Connect network errors can fall back; parse/server/auth/deleted errors do not', async t => {
  const { cache, read } = fixture(t); const note = acknowledged();
  for (const code of ['NETWORK_ERROR', 'NETWORK_TIMEOUT']) {
    await cache.remember(note, await cache.capture()); assert.equal((await read(note.noteId, { api: { request: async () => { throw fail(code); } } })).source, 'cache');
  }
  for (const code of ['INVALID_SERVER_RESPONSE', 'SERVER_ERROR', 'rate_limited', 'notes_revision_conflict', 'UNKNOWN', 'Failed to fetch']) {
    await assert.rejects(read(note.noteId, { api: { request: async () => { throw fail(code); } } }), error => error.code === code);
  }
  assert.equal(noteCacheErrorPolicy({ code: 'notes_note_not_found' }), 'missing');
  assert.equal(noteCacheErrorPolicy({ code: 'NETWORK_ERROR', status: 403 }), 'authorization');
  assert.equal(noteCacheErrorPolicy({ code: 'NETWORK_ERROR', status: 500 }), 'other');
  assert.equal(noteCacheErrorPolicy({ code: 'NETWORK_TIMEOUT', status: 404 }), 'other');
  await assert.rejects(read(note.noteId, { api: offline, allowStale: false }), /NETWORK_ERROR/);
});

test('confirmed not-found removes the snapshot and an older in-flight read cannot put it back', async t => {
  const { cache, read } = fixture(t); const note = acknowledged(); await cache.remember(note, await cache.capture());
  let release; let begin; const began = new Promise(resolve => { begin = resolve; }); const gate = new Promise(resolve => { release = resolve; });
  const pending = read(note.noteId, { api: { request: async () => { begin(); await gate; return { note }; } } }); await began;
  await assert.rejects(read(note.noteId, { api: { request: async () => { throw fail('notes_note_not_found'); } } }), /notes_note_not_found/);
  release(); await assert.rejects(pending, /notes_cache_invalidated/); assert.equal(await cache.get(note.noteId), null);
  await assert.rejects(read(note.noteId, { api: offline }), /NETWORK_ERROR/);
});

test('revocation clears this scope, preserves other account snapshots and fences stale reads after reload', async t => {
  const { cache, storage, read } = fixture(t); const note = acknowledged(); await cache.remember(note, await cache.capture());
  const other = createNoteCache('account_other', 'cache-test', { storage, origin }); const otherNote = acknowledged('Чужая копия'); await other.remember(otherNote, await other.capture());
  const staleToken = await cache.capture();
  await assert.rejects(read(note.noteId, { api: { request: async () => { throw fail('DEVICE_REVOKED'); } } }), /DEVICE_REVOKED/);
  const reopened = createNoteCache(actor.accountId, 'cache-test', { storage, origin });
  assert.equal(await reopened.remember(note, staleToken), false); assert.equal(await reopened.get(note.noteId), null);
  assert.equal((await other.get(otherNote.noteId)).note.body, 'Чужая копия');
});

test('cache failure does not roll back a server ACK or keep a clean note in the durable outbox', async t => {
  const { session, store, service } = fixture(t);
  const current = session({ cache: { capture: async () => 'token', remember: async () => { throw new Error('QuotaExceededError'); } } });
  current.edit({ body: 'Сервер сохранил' }); await current.flush();
  assert.equal(current.state().dirty, false); assert.equal(current.state().error, ''); assert.equal(current.state().cacheError, 'notes_cache_unavailable'); assert.equal(current.hasUnsavedChanges(), false);
  assert.deepEqual(await store.list(), []);
  assert.equal(service.execute({ op: 'notes.get', args: { expectedAccountId: actor.accountId, noteId: current.state().note.noteId }, actor }).note.body, 'Сервер сохранил');
});

test('ACK A caches only A while newer B stays in the durable outbox after another network failure', async t => {
  const { session, api, cache, store } = fixture(t); let release; let began;
  const gate = new Promise(resolve => { release = resolve; }); const started = new Promise(resolve => { began = resolve; }); let calls = 0;
  const current = session({ api: { request: async (op, args) => {
    calls++; if (calls > 1) throw fail('NETWORK_ERROR'); const ack = await api.request(op, args); began(); await gate; return ack;
  } } });
  current.edit({ body: 'A' }); const saving = current.retry(); await started; current.edit({ body: 'B' }); release(); await saving;
  const saved = await cache.get(current.state().note.noteId);
  assert.equal(saved.note.body, 'A'); assert.equal(saved.note.preview, 'A'); assert.equal(saved.note.revision, 1);
  assert.equal(current.state().note.body, 'B'); assert.equal(current.state().dirty, true);
  const draft = (await store.list())[0]; assert.equal(draft.note.body, 'B'); assert.equal(draft.pending.expectedRevision, 1);
});

test('editing a stale snapshot uses CAS; concurrent changes conflict and a purged note is never resurrected', async t => {
  const { session, cache, api, read } = fixture(t); const first = session(); first.edit({ body: 'Исходная' }); await first.flush();
  const noteId = first.state().note.noteId; const stale = await read(noteId, { api: offline });
  first.edit({ body: 'Другое устройство' }); await first.flush();
  const editing = session({ note: stale.note }); editing.edit({ body: 'Локальное изменение' }); await editing.flush();
  assert.equal(editing.state().conflict, true); assert.equal((await api.request('notes.get', { noteId, expectedAccountId: actor.accountId })).note.body, 'Другое устройство');
  first.edit({ state: 'trashed' }); await first.flush(); const trashed = first.state().note;
  await api.request('notes.purge', { noteId, expectedAccountId: actor.accountId, expectedRevision: trashed.revision, mutationId: crypto.randomUUID() });
  const deleted = session({ note: stale.note }); deleted.edit({ body: 'Нельзя воскресить удалённое' }); await deleted.flush();
  assert.equal(deleted.state().conflict, true); assert.equal(deleted.state().error, 'notes_note_deleted'); assert.equal(await cache.get(noteId), null);
  await assert.rejects(api.request('notes.get', { noteId, expectedAccountId: actor.accountId }), /notes_note_not_found/);
});

test('account/screen disposal during a read never applies its content or starts a stale fallback', async t => {
  const { cache, read } = fixture(t); const note = acknowledged(); let active = true; let release; let begun;
  const gate = new Promise(resolve => { release = resolve; }); const started = new Promise(resolve => { begun = resolve; });
  const pending = read(note.noteId, { active: () => active, api: { request: async () => { begun(); await gate; return { note }; } } });
  await started; active = false; release(); await assert.rejects(pending, /notes_account_changed/); assert.equal(await cache.get(note.noteId), null);
});

test('purge fence rejects delayed acknowledged writes; an unsent draft is never admitted as a snapshot', async t => {
  const { cache } = fixture(t); const note = acknowledged(); const beforePurge = await cache.capture();
  await cache.remove(note.noteId); assert.equal(await cache.remember(note, beforePurge), false);
  await assert.rejects(cache.remember({ ...note, revision: 0 }, await cache.capture()), /notes_cache_invalid_snapshot/);
  await assert.rejects(cache.remember({ ...note, state: 'deleted' }, await cache.capture()), /notes_cache_invalid_snapshot/);
});

test('a storage failure during the final read fence cannot bypass a concurrent authoritative clear', async () => {
  const backend = memoryStorage(); let captureCount = 0; let release; let begin;
  const started = new Promise(resolve => { begin = resolve; }); const gate = new Promise((_resolve, reject) => { release = reject; });
  const storage = { ...backend, async capture() { if (++captureCount === 2) { begin(); return gate; } return backend.capture(); } };
  const cache = createNoteCache(actor.accountId, 'cache-test', { origin, storage }); const note = acknowledged();
  const pending = readNoteWithCache({ api: { request: async () => ({ note }) }, accountId: actor.accountId, noteId: note.noteId, cache });
  await started; await cache.clear(); release(new Error('notes_cache_unavailable'));
  await assert.rejects(pending, /notes_cache_invalidated/); assert.equal(await cache.get(note.noteId), null);
});

for (const invalidation of ['remove', 'another-instance-clear']) test(`final fence failure is closed after ${invalidation}, without relying on a local blocked flag`, async () => {
  const backend = memoryStorage(); let captureCount = 0; let release; let begin;
  const started = new Promise(resolve => { begin = resolve; }); const gate = new Promise((_resolve, reject) => { release = reject; });
  const storage = { ...backend, async capture() { if (++captureCount === 2) { begin(); return gate; } return backend.capture(); } };
  const cache = createNoteCache(actor.accountId, 'cache-test', { origin, storage }); const note = acknowledged();
  const pending = readNoteWithCache({ api: { request: async () => ({ note }) }, accountId: actor.accountId, noteId: note.noteId, cache });
  await started;
  if (invalidation === 'remove') await cache.remove(note.noteId);
  else await createNoteCache(actor.accountId, 'cache-test', { origin, storage: backend }).clear();
  release(new Error('notes_cache_unavailable'));
  await assert.rejects(pending, /notes_cache_invalidated/); assert.equal(await cache.get(note.noteId), null);
});

test('cache storage unavailable from the start does not reject an authoritative online read or create offline authority', async () => {
  const cache = createNoteCache(actor.accountId, 'cache-test', { origin, storage: { async capture() { throw new Error('notes_cache_unavailable'); } } });
  const note = acknowledged();
  const read = await readNoteWithCache({ api: { request: async () => ({ note }) }, accountId: actor.accountId, noteId: note.noteId, cache });
  assert.equal(read.source, 'server'); assert.equal(read.cached, false); assert.equal(read.note.body, note.body);
  await assert.rejects(readNoteWithCache({ api: offline, accountId: actor.accountId, noteId: note.noteId, cache }), /NETWORK_ERROR/);
});
