import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createRoomStore } from '../room-store.js';

const roomId = 'independent_storage_acceptance';
const auth = 'synthetic-acceptance-auth';
const author = 'synthetic-author';
const chunk = (fileId, index, total, totalBytes, bytes = 3, extra = {}) => ({
  kind: 'chunk', id: `${fileId}_${index}`, fileId, index, total, totalBytes, bytes,
  nonce: 'synthetic-nonce', ciphertext: Buffer.alloc(bytes + 16, 7).toString('base64url'),
  ...(index === 0 ? { metaNonce: 'synthetic-meta-nonce', metaCiphertext: 'synthetic-meta-ciphertext' } : {}),
  deviceId: author, ...extra,
});
async function fixture(t, options = {}) {
  const parent = resolve(tmpdir()), directory = await mkdtemp(join(parent, 'soty-room-acceptance-'));
  const stores = [];
  function open() { const store = createRoomStore(directory, options); stores.push(store); return store; }
  t.after(async () => {
    for (const store of stores.reverse()) store.close();
    const target = resolve(directory);
    assert.equal(dirname(target), parent); assert.match(basename(target), /^soty-room-acceptance-/u);
    await rm(target, { recursive: true, force: true });
  });
  const store = open(), room = await store.load(roomId); store.claimAuth(room, auth);
  return { store, room, open };
}

test('an invalid last chunk rolls back only that attempt and the valid retry completes the same file', async t => {
  const { store, room } = await fixture(t);
  store.appendFile(room, chunk('partial-file', 0, 2, 6));
  const before = store.stats(room);
  assert.throws(() => store.appendFile(room, chunk('partial-file', 1, 2, 6, 2)), /file_transfer_invalid/u);
  assert.deepEqual(store.stats(room), before, 'no partial counters, receipt or sequence escapes rollback');
  assert.equal(store.receipt(room, 'partial-file_1'), null);
  store.appendFile(room, chunk('partial-file', 1, 2, 6));
  const after = store.stats(room);
  assert.equal(after.reservedBytes, 6); assert.equal(after.events, 2);
  assert.equal(after.files[0].received_bytes, 6); assert.equal(after.files[0].status, 'complete');
});

test('a sender that restarts can retrieve its own committed file, not just files from other authors', async t => {
  const { store, room, open } = await fixture(t);
  store.appendFile(room, chunk('own-file', 0, 1, 3));
  store.close();
  const restarted = open(), reloaded = await restarted.load(roomId);
  restarted.claimAuth(reloaded, auth);
  const event = restarted.nextEvent(reloaded, 0, []);
  assert.ok(event, 'author identity must not permanently suppress durable history');
  assert.equal(event.payload.fileId, 'own-file');
  assert.equal(event.payload.deviceId, author);
});

test('history capacity cannot prevent deletion of an existing file or leave its reservation occupied', async t => {
  const { store, room } = await fixture(t, { limits: { roomEvents: 1, totalEvents: 1 } });
  store.appendFile(room, chunk('full-history-file', 0, 1, 3));
  store.appendFile(room, { kind: 'delete', id: 'delete-existing-file', fileId: 'full-history-file', deviceId: author });
  assert.equal(store.stats(room).reservedBytes, 0);
  assert.equal(store.stats(room).files[0].status, 'deleted');
  assert.throws(() => store.appendFile(room, chunk('full-history-file', 0, 1, 3)), /file_deleted/u);
});

test('unrecognised payload properties cannot consume storage or be replayed outside the declared file contract', async t => {
  const { store, room } = await fixture(t);
  const packet = chunk('bounded-file', 0, 1, 3, 3, { padding: 'x'.repeat(1_000_000), forgedAuthority: { account: 'not-a-real-account' } });
  let rejected = false;
  try { store.appendFile(room, packet); } catch (error) {
    assert.match(error.code ?? error.message, /invalid|unsupported/u); rejected = true;
  }
  if (rejected) { assert.equal(store.stats(room).events, 0); assert.equal(store.stats(room).reservedBytes, 0); return; }
  const event = store.nextEvent(room);
  assert.equal(Object.hasOwn(event.payload, 'padding'), false);
  assert.equal(Object.hasOwn(event.payload, 'forgedAuthority'), false);
  assert.equal(event.payload.ciphertext, packet.ciphertext);
  assert.ok(event.wireBytes < 4096, 'three encrypted bytes cannot retain one megabyte of unknown metadata');
});

test('closed rooms remain closed across restart and never expose or recreate accepted file contents', async t => {
  const { store, room, open } = await fixture(t);
  store.appendFile(room, chunk('closed-file', 0, 1, 3));
  store.closeRoom(room, { deviceId: author, at: '2026-09-30T00:00:00.000Z' });
  store.close();
  const restarted = open(), closed = await restarted.load(roomId);
  assert.equal(restarted.nextEvent(closed), null);
  assert.equal(restarted.stats(closed).reservedBytes, 0);
  assert.throws(() => restarted.claimAuth(closed, auth), /room_closed/u);
  assert.throws(() => restarted.appendFile(closed, chunk('closed-file', 0, 1, 3)), /room_closed/u);
});

test('looking up unknown room IDs cannot durably allocate unauthenticated room rows', async t => {
  const { store } = await fixture(t);
  for (let index = 0; index < 12; index++) await store.load(`unknown_room_${index}`);
  const reader = new DatabaseSync(store.filename, { readOnly: true });
  try {
    assert.equal(reader.prepare('SELECT count(*) AS count FROM room_state').get().count, 1,
      'only the explicitly authenticated fixture room should have durable state');
  } finally { reader.close(); }
});

test('expired disconnected join requests cannot permanently occupy every room-cache slot', async t => {
  const { store } = await fixture(t, { limits: { cachedRooms: 2 } });
  const connected = await store.load(roomId, { retain: true });
  const abandoned = await store.load('abandoned_join_room');
  const expiredAt = Date.now() - 11 * 60_000;
  abandoned.waiting.set('expired-join', { joinRequestId: 'expired-join', joinRequest: { requestId: 'expired-join' },
    joinCreatedAt: expiredAt, disconnectedAt: expiredAt, rateStartedAt: expiredAt, acceptedAt: 0, deniedAt: 0, ws: null });
  const next = await store.load('new_room_after_expiry');
  assert.equal(next.id, 'new_room_after_expiry');
  assert.equal((await store.load(roomId)), connected, 'a retained active room must keep the same peer registry');
  store.release(connected);
});
