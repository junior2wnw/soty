import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, rm, copyFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, basename } from 'node:path';
import { createServer } from 'node:http';
import { DatabaseSync } from 'node:sqlite';
import WebSocket, { WebSocketServer } from 'ws';
import { createRoomStore } from '../room-store.js';
import { attachRealtime } from '../realtime.js';

const ROOM = 'room_file_durability';
const pause = ms => new Promise(resolveWait => setTimeout(resolveWait, ms));
async function until(predicate) { for (let n = 0; n < 1000; n++) { if (predicate()) return; await pause(5); } assert.fail('Timed out waiting for relay state'); }
const file = (id, extra = {}) => ({ kind: 'chunk', id: id + '_0', fileId: id, index: 0, total: 1, totalBytes: 3, bytes: 3,
  nonce: 'synthetic_nonce', ciphertext: Buffer.alloc(19, 5).toString('base64url'), metaNonce: 'meta_nonce', metaCiphertext: 'meta_ciphertext', ...extra });
async function sandbox(t) {
  const base = resolve(tmpdir()), dir = await mkdtemp(join(base, 'soty-file-durability-')), cleanups = [];
  t.after(async () => { for (const cleanup of cleanups.reverse()) await cleanup();
    assert.equal(dirname(resolve(dir)), base); assert.match(basename(dir), /^soty-file-durability-/u); await rm(dir, { recursive: true, force: true }); });
  function openStore(options) { const store = createRoomStore(dir, options); cleanups.push(() => store.close()); return store; }
  return { dir, cleanups, openStore };
}
async function fixture(t) {
  const f = await sandbox(t), actual = f.openStore(), loaded = new Set();
  let beforeAppend = async () => undefined;
  const calls = [];
  const store = { ...actual,
    async load(id, options) { const value = await actual.load(id, options); loaded.add(value); return value; },
    async appendFile(room, payload) { calls.push(payload.id); await beforeAppend(room, payload); return actual.appendFile(room, payload); } };
  const server = createServer(), wss = new WebSocketServer({ noServer: true });
  attachRealtime(wss, store);
  server.on('upgrade', (request, socket, head) => wss.handleUpgrade(request, socket, head, ws => wss.emit('connection', ws, request, ROOM)));
  await new Promise(resolveListen => server.listen(0, '127.0.0.1', resolveListen));
  const clients = [];
  f.cleanups.push(async () => {
    for (const ws of clients) ws.terminate();
    for (const ws of wss.clients) ws.terminate();
    await new Promise(resolveClose => wss.close(resolveClose));
    await new Promise(resolveClose => server.close(resolveClose));
  });
  async function connect(id, { auth = 'synthetic_auth', autoAck = true } = {}) {
    const ws = new WebSocket('ws://127.0.0.1:' + server.address().port), messages = [];
    clients.push(ws); ws.on('message', raw => {
      const message = JSON.parse(raw.toString()); messages.push(message);
      if (autoAck && Number.isSafeInteger(message.sequence)) ws.send(JSON.stringify({ type: 'replay.ack', sequence: message.sequence }));
    }); ws.on('error', () => undefined);
    await new Promise((resolveOpen, reject) => { ws.once('open', resolveOpen); ws.once('error', reject); });
    ws.send(JSON.stringify({ type: 'hello', deviceId: id, nick: id, roomAuth: auth, replay: 'ack-v1' }));
    await until(() => messages.some(message => message.type === 'hello') || ws.readyState === WebSocket.CLOSED);
    return { ws, messages, send(message) { ws.send(JSON.stringify(message)); },
      ack(id) { return messages.filter(message => message.type === 'ack' && message.id === id); } };
  }
  return { ...f, actual, loaded, calls, connect, beforeAppend(callback) { beforeAppend = callback; } };
}

test('two first websocket handshakes share one room and durable auth; hello contains metadata only', async t => {
  const f = await fixture(t);
  const [first, second] = await Promise.all([f.connect('first'), f.connect('second')]);
  assert.equal(f.loaded.size, 1); assert.equal((await f.actual.load(ROOM)).peers.size, 2);
  assert.equal((await f.openStore().load(ROOM)).state.auth, 'synthetic_auth');
  for (const client of [first, second]) assert.deepEqual(client.messages.find(message => message.type === 'hello').files, []);
});

test('concurrent duplicate waits for commit; no ACK or replay before persistence and receipt is unique', async t => {
  const f = await fixture(t), a = await f.connect('sender'), b = await f.connect('observer');
  let release; const barrier = new Promise(resolveBarrier => { release = resolveBarrier; });
  f.beforeAppend(async () => barrier);
  const packet = file('one');
  a.send({ type: 'file', file: packet }); a.send({ type: 'file', file: packet });
  await until(() => f.calls.length === 1); await pause(15);
  const room = await f.actual.load(ROOM);
  assert.equal(f.actual.stats(room).events, 0); assert.equal(f.actual.receipt(room, packet.id), null);
  assert.equal(a.ack(packet.id).length, 0); assert.equal(b.messages.filter(message => message.type === 'file').length, 0);
  release(); await until(() => a.ack(packet.id).length === 2);
  assert.equal(f.actual.stats(room).events, 1);
  const reopened = f.openStore(); assert.equal(reopened.stats(await reopened.load(ROOM)).files[0].status, 'complete');
});

test('failed operation cannot leak into a later unrelated update; same author retry persists once', async t => {
  const f = await fixture(t), a = await f.connect('sender'), b = await f.connect('other');
  let rejectSave; const barrier = new Promise((_yes, no) => { rejectSave = no; });
  let blocked = true;
  f.beforeAppend(async (_room, packet) => { if (blocked && packet.id === 'failed_0') { blocked = false; await barrier; } });
  a.send({ type: 'file', file: file('failed') }); await until(() => f.calls.length === 1);
  b.send({ type: 'update', update: { id: 'unrelated_update', kind: 'update', nonce: 'nonce', ciphertext: 'ciphertext' } });
  rejectSave(new Error('synthetic persistence failure'));
  await until(() => a.ws.readyState === WebSocket.CLOSED && b.ack('unrelated_update').length === 1);
  assert.equal(a.ack('failed_0').length, 0);
  const room = await f.actual.load(ROOM);
  assert.equal(f.actual.stats(room).files.length, 0); assert.equal(f.actual.stats(room).events, 1);
  const retry = await f.connect('sender'); retry.send({ type: 'file', file: file('failed') });
  await until(() => retry.ack('failed_0').length === 1);
  assert.equal(f.actual.stats(room).events, 2);
});

test('actual SQLite failure after chunk insertion rolls back reservation, chunk and receipt; repaired retry ACKs', async t => {
  const f = await fixture(t), a = await f.connect('sender'), room = await f.actual.load(ROOM);
  const db = new DatabaseSync(f.actual.filename); f.cleanups.push(() => db.close());
  db.exec("CREATE TRIGGER injected_failure BEFORE INSERT ON room_receipts WHEN NEW.message_id='failed_0' BEGIN SELECT RAISE(ABORT,'synthetic persistence failure'); END");
  a.send({ type: 'file', file: file('failed') });
  await until(() => a.ws.readyState === WebSocket.CLOSED || a.messages.some(message => message.type === 'file.error'));
  assert.equal(a.ack('failed_0').length, 0);
  assert.equal(f.actual.stats(room).events, 0); assert.equal(f.actual.stats(room).reservedBytes, 0);
  assert.equal(f.actual.stats(room).files.length, 0); assert.equal(f.actual.receipt(room, 'failed_0'), null);
  db.exec('DROP TRIGGER injected_failure');
  const retry = a.ws.readyState === WebSocket.OPEN ? a : await f.connect('sender');
  retry.send({ type: 'file', file: file('failed') });
  await until(() => retry.ack('failed_0').length === 1);
  assert.equal(f.actual.stats(room).events, 1);
});

test('corrupt legacy room stays unchanged and fails closed; failed import can be repaired without restarting', async t => {
  const f = await sandbox(t), path = join(f.dir, ROOM + '.json'), store = f.openStore();
  for (const corrupted of ['{"auth":', '{"auth":42,"updates":[],"files":[]}', '{"auth":"synthetic_auth","updates":{},"files":[]}']) {
    await writeFile(path, corrupted);
    const results = await Promise.allSettled([store.load(ROOM), store.load(ROOM)]);
    assert.equal(results.filter(value => value.status === 'rejected' && value.reason.message === 'room_state_unavailable').length, 2);
    assert.equal(await readFile(path, 'utf8'), corrupted);
  }
  await writeFile(path, JSON.stringify({ auth: 'synthetic_auth', updates: [], files: [] }));
  assert.equal((await store.load(ROOM)).state.auth, 'synthetic_auth');
});

test('fingerprinted retry rejects changed bytes/type/author and deleted IDs cannot resurrect', async t => {
  const f = await sandbox(t), store = f.openStore(), room = await store.load(ROOM);
  store.claimAuth(room, 'synthetic_auth');
  const packet = file('one', { deviceId: 'a' });
  store.appendFile(room, packet);
  assert.equal(store.appendFile(room, { ...packet, createdAt: 'presentation changed' }).duplicate, true);
  for (const changed of [{ ...packet, ciphertext: Buffer.alloc(19, 6).toString('base64url') }, { ...packet, deviceId: 'b' }])
    assert.throws(() => store.appendFile(room, changed), /message_identity_conflict/u);
  assert.throws(() => store.appendUpdate(room, { id: packet.id, kind: 'update', nonce: 'nonce', ciphertext: 'ciphertext' }), /message_identity_conflict/u);
  store.appendFile(room, { kind: 'delete', id: 'delete_one', fileId: 'one', deviceId: 'a' });
  assert.equal(store.stats(room).reservedBytes, 0);
  assert.throws(() => store.appendFile(room, packet), /file_deleted/u);
  assert.throws(() => store.appendFile(room, { ...packet, id: 'different_id' }), /file_deleted/u);
  assert.equal(store.nextEvent(room).payload.kind, 'delete');
});

test('whole-file reservations are atomic across two connections and explicit deletion frees capacity', async t => {
  const f = await sandbox(t), options = { limits: { roomBytes: 10, totalBytes: 10, receivingFiles: 1 } };
  const first = f.openStore(options), second = f.openStore(options);
  const a = await first.load(ROOM), b = await second.load(ROOM), other = await second.load('other_room');
  first.claimAuth(a, 'synthetic_auth'); second.claimAuth(other, 'synthetic_auth');
  first.appendFile(a, file('six', { total: 2, totalBytes: 6 }));
  assert.equal(second.stats(b).reservedBytes, 6);
  assert.throws(() => second.appendFile(b, file('five', { totalBytes: 5, total: 2 })), /room_file_capacity/u);
  assert.throws(() => second.appendFile(other, file('five', { totalBytes: 5, total: 2 })), /storage_file_capacity/u);
  assert.throws(() => second.appendFile(b, file('three', { totalBytes: 3, total: 2 })), /room_transfer_capacity/u);
  second.appendFile(b, { id: 'delete_six', kind: 'delete', fileId: 'six' });
  second.appendFile(b, file('five', { totalBytes: 5, total: 2 }));
  assert.equal(first.stats(a).reservedBytes, 5);
});

test('legacy import counts actual chunks, preserves partial data and over-limit data, and is restart-idempotent', async t => {
  const f = await sandbox(t), source = JSON.stringify({ auth: 'old_auth', updates: [], files: [
    file('complete'), file('partial', { total: 2, totalBytes: 6 }),
  ] });
  await writeFile(join(f.dir, ROOM + '.json'), source);
  const options = { limits: { roomBytes: 1, totalBytes: 1 } };
  const store = f.openStore(options), room = await store.load(ROOM);
  assert.deepEqual(store.stats(room).files.map(item => item.status), ['complete', 'incomplete']);
  assert.equal(store.stats(room).reservedBytes, 9);
  assert.throws(() => store.appendFile(room, file('new')), /room_file_capacity/u);
  assert.equal(await readFile(join(f.dir, ROOM + '.json'), 'utf8'), source);
  const reopened = f.openStore(options); assert.equal(reopened.stats(await reopened.load(ROOM)).events, 2);
});

test('oversized legacy JSON requires streaming migration, not empty-room reset; malformed completeness is rejected atomically', async t => {
  const f = await sandbox(t), path = join(f.dir, ROOM + '.json');
  await writeFile(path, ' '.repeat(101));
  const capped = f.openStore({ limits: { legacyReadBytes: 100 } });
  await assert.rejects(capped.load(ROOM), /room_legacy_streaming_import_required/u);
  const malformed = JSON.stringify({ files: [file('bad', { totalBytes: 4 })] });
  await writeFile(path, malformed);
  const store = f.openStore(); await assert.rejects(store.load(ROOM), /file_transfer_invalid/u);
  assert.equal(await readFile(path, 'utf8'), malformed);
  await writeFile(path, JSON.stringify({ files: [file('good')] }));
  assert.equal(store.stats(await store.load(ROOM)).events, 1);
});

test('authenticated replay keeps one frame in flight until application ACK; wrong auth sees no file history', async t => {
  const f = await fixture(t), room = await f.actual.load(ROOM);
  f.actual.claimAuth(room, 'synthetic_auth');
  for (let i = 0; i < 8; i++) f.actual.appendFile(room, file('history_' + i, { deviceId: 'owner' }));
  const denied = await f.connect('intruder', { auth: 'wrong' });
  assert.equal(denied.messages.some(value => value.type === 'hello' || value.type === 'file'), false);
  const observer = await f.connect('observer', { autoAck: false });
  await until(() => observer.messages.filter(value => value.type === 'file').length === 1);
  await pause(150); assert.equal(observer.messages.filter(value => value.type === 'file').length, 1);
  const first = observer.messages.find(value => value.type === 'file');
  observer.send({ type: 'replay.ack', sequence: first.sequence + 1 });
  await pause(30); assert.equal(observer.messages.filter(value => value.type === 'file').length, 1);
  observer.send({ type: 'replay.ack', sequence: first.sequence });
  await until(() => observer.messages.filter(value => value.type === 'file').length === 2);
});

test('quiesced SQLite backup restores committed chunks, receipts, auth and reservations', async t => {
  const f = await sandbox(t), store = f.openStore(), room = await store.load(ROOM);
  store.claimAuth(room, 'synthetic_auth'); store.appendFile(room, file('persisted'));
  const filename = store.filename; store.close();
  const backup = join(f.dir, 'backup.sqlite'); await copyFile(filename, backup);
  await copyFile(backup, filename);
  const restored = f.openStore(), value = await restored.load(ROOM);
  assert.equal(value.state.auth, 'synthetic_auth');
  assert.equal(restored.stats(value).reservedBytes, 3);
  assert.deepEqual(restored.nextEvent(value).payload, file('persisted'));
  assert.equal(restored.receipt(value, 'persisted_0').disposition, 'stored');
});

test('four interrupted transfers remain manageable after reload; explicit ACKed discard frees a slot', async t => {
  const f = await fixture(t), sender = await f.connect('sender');
  for (let index = 0; index < 4; index++) {
    const packet = file('interrupted_' + index, { total: 2, totalBytes: 6 });
    sender.send({ type: 'file', file: packet }); await until(() => sender.ack(packet.id).length === 1);
  }
  const fresh = await f.connect('sender');
  const hello = fresh.messages.find(value => value.type === 'hello');
  assert.equal(hello.pendingFiles.length, 4);
  assert.deepEqual(hello.pendingFiles.map(value => [value.totalBytes, value.receivedBytes]), Array.from({ length: 4 }, () => [6, 3]));
  const reopened = f.openStore(), reopenedRoom = await reopened.load(ROOM);
  assert.equal(reopened.pendingFiles(reopenedRoom).length, 4, 'inventory is durable, not an in-memory sender queue');
  fresh.send({ type: 'file', file: file('fifth', { total: 2, totalBytes: 6 }) });
  await until(() => fresh.messages.some(value => value.type === 'file.error' && value.id === 'fifth_0'));
  assert.equal(fresh.ack('fifth_0').length, 0);
  fresh.send({ type: 'file', file: { kind: 'delete', id: 'explicit_discard', fileId: 'interrupted_0' } });
  await until(() => fresh.ack('explicit_discard').length === 1);
  assert.equal(reopened.pendingFiles(reopenedRoom).length, 3);
  assert.equal(reopened.stats(reopenedRoom).reservedBytes, 18);
  fresh.send({ type: 'file', file: file('fifth', { total: 2, totalBytes: 6 }) });
  await until(() => fresh.ack('fifth_0').length === 1);
  assert.equal(reopened.stats(reopenedRoom).reservedBytes, 24);
});
