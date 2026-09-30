import test from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { mkdtemp, open, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { createServer } from 'node:http';
import WebSocket, { WebSocketServer } from 'ws';
import { createRoomStore } from '../room-store.js';
import { attachRealtime } from '../realtime.js';

const pause = ms => new Promise(resolveWait => setTimeout(resolveWait, ms));
const size = 512_000_000, chunkSize = 256_000, roomId = 'large_file_isolated';
const seal = (key, value) => {
  const nonce = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, nonce);
  const body = Buffer.concat([cipher.update(value), cipher.final(), cipher.getAuthTag()]);
  return { nonce: nonce.toString('base64url'), ciphertext: body.toString('base64url') };
};
const unseal = (key, value) => {
  const bytes = Buffer.from(value.ciphertext, 'base64url'), cipher = createDecipheriv('aes-256-gcm', key, Buffer.from(value.nonce, 'base64url'));
  cipher.setAuthTag(bytes.subarray(-16));
  return Buffer.concat([cipher.update(bytes.subarray(0, -16)), cipher.final()]);
};

test('512 MB real file survives encrypted relay ACK, storage restart, bounded replay and byte-exact disk restore', {
  skip: process.env.SOTY_LARGE_FILE_TEST !== '1', timeout: 600_000,
}, async t => {
  const base = resolve(tmpdir()), dir = await mkdtemp(join(base, 'soty-file-large-')), key = randomBytes(32), inputHash = createHash('sha256');
  let store, server, wss, clients = [], peakRss = process.memoryUsage().rss, peakHeap = process.memoryUsage().heapUsed, maxBuffered = 0;
  const sample = () => { const memory = process.memoryUsage(); peakRss = Math.max(peakRss, memory.rss); peakHeap = Math.max(peakHeap, memory.heapUsed); };
  const monitor = setInterval(sample, 100);
  async function stop() {
    for (const ws of clients) ws.terminate(); clients = [];
    if (wss) { for (const ws of wss.clients) ws.terminate(); await new Promise(done => wss.close(done)); wss = null; }
    if (server) { await new Promise(done => server.close(done)); server = null; }
    store?.close(); store = null;
  }
  t.after(async () => { clearInterval(monitor); await stop(); assert.equal(dirname(resolve(dir)), base);
    assert.match(basename(dir), /^soty-file-large-/u); await rm(dir, { recursive: true, force: true }); });
  async function start() {
    store = createRoomStore(dir); server = createServer(); wss = new WebSocketServer({ noServer: true, maxPayload: 1_000_000 });
    attachRealtime(wss, store);
    server.on('upgrade', (request, socket, head) => wss.handleUpgrade(request, socket, head, ws => wss.emit('connection', ws, request, roomId)));
    await new Promise(done => server.listen(0, '127.0.0.1', done));
  }
  async function connect(deviceId, onMessage) {
    const ws = new WebSocket('ws://127.0.0.1:' + server.address().port); clients.push(ws);
    let hello; const ready = new Promise(done => { hello = done; });
    ws.on('message', raw => {
      const value = JSON.parse(raw.toString());
      if (value.type === 'hello') { assert.deepEqual(value.files, []); assert.ok(raw.byteLength < 2048); hello(); }
      else onMessage(value, ws);
    });
    await new Promise((done, no) => { ws.once('open', done); ws.once('error', no); });
    ws.send(JSON.stringify({ type: 'hello', deviceId, nick: deviceId, roomAuth: 'isolated_test_auth', replay: 'ack-v1' })); await ready;
    return ws;
  }
  const sourcePath = join(dir, 'source.bin'), destinationPath = join(dir, 'restored.bin');
  const source = await open(sourcePath, 'w');
  try {
    for (let index = 0; index < size / chunkSize; index++) {
      const bytes = Buffer.allocUnsafe(chunkSize);
      for (let n = 0; n < bytes.length; n++) bytes[n] = (n * 31 + index * 17 + (n >>> 8)) & 255;
      inputHash.update(bytes); await source.write(bytes);
    }
    await source.sync();
  } finally { await source.close(); }
  const expectedHash = inputHash.digest('hex');
  assert.equal((await stat(sourcePath)).size, size);
  await start();
  let confirm = null, rejectAck = null, confirmed = 0;
  const sender = await connect('sender', (message, ws) => {
    if (message.type === 'file' && Number.isSafeInteger(message.sequence)) ws.send(JSON.stringify({ type: 'replay.ack', sequence: message.sequence }));
    if (message.type === 'ack') { confirmed++; const done = confirm; confirm = null; done?.(message.id); }
    if (message.type === 'file.error') rejectAck?.(new Error(message.code));
  });
  sender.on('close', () => rejectAck?.(new Error('sender disconnected')));
  const input = await open(sourcePath, 'r'), started = performance.now();
  try {
    for (let index = 0; index < size / chunkSize; index++) {
      const bytes = Buffer.allocUnsafe(chunkSize), { bytesRead } = await input.read(bytes, 0, bytes.length, index * chunkSize);
      assert.equal(bytesRead, chunkSize);
      const encrypted = seal(key, bytes), meta = index === 0 ? seal(key, Buffer.from(JSON.stringify({ name: 'Проверка 512 МБ 🐝.bin', size }))) : null;
      const payload = { kind: 'chunk', id: 'large_file_' + index, fileId: 'large_file', index, total: size / chunkSize, totalBytes: size, bytes: bytesRead,
        ...encrypted, ...(meta ? { metaNonce: meta.nonce, metaCiphertext: meta.ciphertext } : {}) };
      const acknowledged = new Promise((yes, no) => { confirm = yes; rejectAck = no; });
      const wire = JSON.stringify({ type: 'file', file: payload });
      sender.send(wire); maxBuffered = Math.max(maxBuffered, sender.bufferedAmount);
      assert.equal(await acknowledged, payload.id); rejectAck = null;
      if ((index + 1) % 250 === 0) { sample(); t.diagnostic('Persisted and ACKed ' + (index + 1) * chunkSize + ' bytes'); }
      // Match production outbox pacing and the existing relay's abuse budget.
      await pause(Math.ceil(Buffer.byteLength(wire) / 4_000_000 * 1000));
    }
  } finally { await input.close(); }
  assert.equal(confirmed, size / chunkSize);
  const room = await store.load(roomId), metadata = store.stats(room);
  assert.equal(metadata.files[0].status, 'complete'); assert.equal(metadata.reservedBytes, size); assert.equal(metadata.events, size / chunkSize);
  const databaseBytes = (await stat(store.filename)).size;
  await stop(); await start();
  const output = await open(destinationPath, 'w'), outputHash = createHash('sha256');
  let received = 0, operations = 0, maxOperations = 0, receiveReject;
  const finished = new Promise((yes, no) => {
    receiveReject = no;
    void connect('receiver', (message, ws) => {
      if (message.type !== 'file') return;
      operations++; maxOperations = Math.max(maxOperations, operations);
      void (async () => {
        assert.equal(message.file.index, received / chunkSize);
        const bytes = unseal(key, message.file); outputHash.update(bytes);
        await output.write(bytes); received += bytes.length;
        // ACK only after the output accepted this chunk, never merely on parse.
        operations--; ws.send(JSON.stringify({ type: 'replay.ack', sequence: message.sequence }));
        if (received === size) yes();
      })().catch(no);
    }).catch(no);
  });
  try { await finished; await output.sync(); } catch (error) { receiveReject(error); throw error; } finally { await output.close(); }
  assert.equal((await stat(destinationPath)).size, size); assert.equal(outputHash.digest('hex'), expectedHash);
  assert.equal(maxOperations, 1); assert.ok(maxBuffered <= 1_000_000);
  sample();
  t.diagnostic(JSON.stringify({ size, chunks: confirmed, sha256: expectedHash, databaseBytes, peakRss, peakHeap,
    maxBuffered, maxReceiveOperations: maxOperations, elapsedSeconds: Math.round((performance.now() - started) / 1000) }));
});
