import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { registerHooks } from 'node:module';
import { readFileSync } from 'node:fs';
import ts from 'typescript';
import { createTrustLinkRoom } from 'trustlink-kernel';
import { createFileOutbox, maxFileBytes, fileChunkBytes, maxPendingFileBytes, maxPendingFileChunks,
  maxSocketBufferedBytes, maxControlQueueCount } from './file-transfer.mjs';

// Exercise the real TypeScript client and crypto, without a browser/network or
// a copied implementation. Only relative TS resolution and syntax are adapted.
registerHooks({
  resolve(specifier, context, next) {
    try { return next(specifier, context); }
    catch (error) {
      if (!specifier.startsWith('.') || /\.[a-z]+$/u.test(specifier)) throw error;
      for (const suffix of ['.ts', '/index.ts']) { try { return next(specifier + suffix, context); } catch { /* try index */ } }
      throw error;
    }
  },
  load(url, context, next) {
    if (url.startsWith('file:') && url.endsWith('.ts')) return { format: 'module', shortCircuit: true,
      source: ts.transpileModule(readFileSync(new URL(url), 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText };
    return next(url, context);
  },
});
const { TunnelSync } = await import('../sync.ts');
const { downloadReceivedFile } = await import('../features/files.ts');
const { encryptForTunnel } = await import('../trustlink/codec.ts');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const digest = async blob => createHash('sha256').update(new Uint8Array(await blob.arrayBuffer())).digest('hex');
async function until(predicate) { for (let n = 0; n < 500; n++) { if (predicate()) return; await pause(5); } assert.fail('Timed out waiting for test condition'); }

class FakeSocket {
  static OPEN = 1; static CLOSING = 2; static CLOSED = 3; static instances = [];
  readyState = 0; bufferedAmount = 0; sent = []; onSend = null;
  constructor() { FakeSocket.instances.push(this); }
  send(wire) { if (this.readyState !== 1) throw new Error('closed'); const message = JSON.parse(wire); this.sent.push(message); this.onSend?.(message); }
  close() { this.readyState = 3; this.onclose?.(); }
  receive(message) { this.onmessage?.({ data: JSON.stringify(message) }); }
  async open() {
    this.readyState = 1; this.onopen?.();
    await until(() => this.sent.some(message => message.type === 'hello'));
    this.receive({ type: 'hello', peers: [], updates: [], files: [] });
    await pause(0);
  }
}
function browser(t) {
  const saved = Object.fromEntries(['window', 'document', 'WebSocket'].map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]));
  const clicked = [];
  const document = Object.assign(new EventTarget(), { visibilityState: 'visible',
    body: { append() {} }, createElement() { return { style: {}, click() { clicked.push(this.href); }, remove() {} }; } });
  const window = Object.assign(new EventTarget(), { location: { protocol: 'http:', host: 'unit.test' },
    setTimeout: (fn, ms) => setTimeout(fn, ms).unref(), clearTimeout,
    setInterval: (fn, ms) => setInterval(fn, ms).unref(), clearInterval });
  Object.assign(globalThis, { window, document, WebSocket: FakeSocket });
  const restore = () => { for (const key of Object.keys(saved)) { if (saved[key]) Object.defineProperty(globalThis, key, saved[key]); else delete globalThis[key]; } };
  return { clicked, restore };
}
function pair(t) {
  const { clicked, restore } = browser(t), room = createTrustLinkRoom({ label: 'synthetic transfer' });
  const tunnel = { id: room.id, key: room.secret, label: 'test', unread: false, createdAt: '', updatedAt: '' };
  const files = [];
  const callbacks = { onText() {}, onTerminal() {}, onChess() {}, onActivity() {}, onRemoteChange() {}, onLiveDraft() {},
    onFile(file) { files.push(file); }, onFileDeleted() {}, onKnock() {}, onRemoteRequest() {}, onRemoteGrant() {}, onRemoteCommand() {},
    onRemoteScript() {}, onRemoteCancel() {}, onRemoteOutput() {}, onPeers() {}, onJoinRequest() {}, onClosed() {}, onState() {} };
  const sender = new TunnelSync(tunnel, { id: 'dev_sender', nick: 'Sender' }, callbacks), ws = FakeSocket.instances.at(-1);
  const receiver = new TunnelSync(tunnel, { id: 'dev_receiver', nick: 'Receiver' }, callbacks);
  t.after(() => { sender.destroy(); receiver.destroy(); for (const url of clicked) URL.revokeObjectURL(url); restore(); });
  return { sender, receiver, ws, files, clicked, tunnel };
}

test('window admission, socket buffering, reconnect and timeout are bounded and ACK-driven', async t => {
  let clock = 0, socket = { readyState: 1, bufferedAmount: maxSocketBufferedBytes, sent: [], send(wire) { this.sent.push(wire); } };
  const outbox = createFileOutbox({ socket: () => socket, clock: () => clock, timeoutMs: 100, bytesPerSecond: 1e9 });
  t.after(() => outbox.close());
  const pending = [];
  for (let i = 0; i < maxPendingFileChunks; i++) pending.push(outbox.reserve(`id_${i}`, 100).send(`{"id":${i}}`));
  assert.throws(() => outbox.reserve('overflow', 100), /file_transfer_busy/u);
  assert.equal(socket.sent.length, 0); assert.ok(outbox.stats().reservedBytes <= maxPendingFileBytes);
  outbox.ack('id_0'); assert.equal(outbox.stats().pending, maxPendingFileChunks, 'a never-sent ID is not an ACK');
  socket.bufferedAmount = 0; outbox.pump();
  assert.equal(socket.sent.length, 1);
  const original = socket.sent[0];
  socket = { ...socket, sent: [] }; clock++; outbox.pump();
  assert.equal(socket.sent[0], original, 'lost ACK replays exactly the same serialized chunk');
  outbox.ack('id_0'); await pending[0]; outbox.ack('id_0');
  clock = 100; outbox.pump();
  const results = await Promise.allSettled(pending);
  assert.equal(results.filter(value => value.status === 'rejected').length, maxPendingFileChunks - 1);
  assert.deepEqual(outbox.stats(), { pending: 0, reservedBytes: 0, committed: 0 });
});

test('real sender and receiver preserve binary SHA256, Unicode names and empty files; sent download uses actual content', async t => {
  const f = pair(t); await f.ws.open();
  const deliveries = [];
  f.ws.onSend = message => {
    if (message.type !== 'file') return;
    deliveries.push(f.receiver.applyFile({ ...message.file, deviceId: 'dev_sender', deviceNick: 'Sender' }));
    setImmediate(() => f.ws.receive({ type: 'ack', id: message.file.id }));
  };
  for (const data of [Uint8Array.from({ length: fileChunkBytes * 2 + 123 }, (_, i) => i % 251), new Uint8Array()]) {
    const source = new File([data], 'Отчёт 🐝.bin', { type: 'application/octet-stream' });
    const sent = await f.sender.sendFile(source); await Promise.all(deliveries);
    const received = f.files.at(-1);
    assert.equal(received.name, source.name); assert.equal(received.size, data.length);
    assert.equal(await digest(received.blob), await digest(source));
    assert.equal(await digest(sent.blob), await digest(source));
    downloadReceivedFile(sent);
    assert.equal(await digest(await (await fetch(f.clicked.at(-1))).blob()), await digest(source));
    assert.equal(f.sender.controlQueue.length, 0); assert.equal(f.sender.fileOutbox.stats().pending, 0);
  }
});

test('a synthetic 512 MB offline source reads one slice only, holds bounded ciphertext and rejects on destroy', async t => {
  const f = pair(t), slices = [];
  const source = { size: maxFileBytes, name: 'large.bin', type: '', arrayBuffer() { assert.fail('whole-file read'); },
    slice(start, end) { slices.push([start, end]); return new Blob([new Uint8Array(end - start)]); } };
  const sending = f.sender.sendFile(source); sending.catch(() => undefined);
  await until(() => f.sender.fileOutbox.stats().committed === 1);
  await pause(40);
  assert.deepEqual(slices, [[0, fileChunkBytes]]);
  assert.equal(f.sender.controlQueue.length, 0);
  assert.ok(f.sender.fileOutbox.stats().reservedBytes <= 400_000);
  f.sender.destroy();
  await assert.rejects(sending, /file_transfer_closed/u);
  assert.deepEqual(f.sender.fileOutbox.stats(), { pending: 0, reservedBytes: 0, committed: 0 });
});

test('unconstrained streaming callers are rejected before encryption instead of becoming waiting closures', async t => {
  const f = pair(t), chunk = new Uint8Array(fileChunkBytes), pending = [];
  for (let index = 0; index < 50; index++) pending.push(f.sender.sendFileChunkFromBytes('stream_1', { name: 'stream.bin', size: chunk.length * 50 }, chunk, index, 50));
  pending.forEach(promise => promise.catch(() => undefined));
  await pause(30);
  assert.equal(f.sender.fileOutbox.stats().pending, maxPendingFileChunks);
  assert.ok(f.sender.fileOutbox.stats().reservedBytes <= maxPendingFileBytes);
  assert.equal(f.sender.controlQueue.length, 0);
  f.sender.destroy();
  const results = await Promise.allSettled(pending);
  assert.equal(results.filter(value => value.reason?.code === 'file_transfer_busy').length, 50 - maxPendingFileChunks);
  assert.equal(results.filter(value => value.reason?.code === 'file_transfer_closed').length, maxPendingFileChunks);
});

test('a reconnect preserves encrypted chunk identity and the promise resolves only after the new relay ACK', async t => {
  const f = pair(t); await f.ws.open();
  const promise = f.sender.sendFile(new File(['reconnect exact bytes'], 'again.txt'));
  let settled = false; promise.then(() => { settled = true; });
  await until(() => f.ws.sent.some(message => message.type === 'file'));
  const first = f.ws.sent.find(message => message.type === 'file');
  assert.equal(settled, false);
  f.sender.closeAndReconnect(f.ws);
  const next = FakeSocket.instances.at(-1); await next.open();
  await until(() => next.sent.some(message => message.type === 'file'));
  assert.deepEqual(next.sent.find(message => message.type === 'file'), first);
  assert.equal(settled, false); next.receive({ type: 'ack', id: first.file.id });
  assert.equal((await promise).size, 21);
});

test('duplicate relay/direct chunks and malformed cumulative sizes cannot corrupt or multiply a received file', async t => {
  const f = pair(t); await f.ws.open();
  const messages = [];
  f.ws.onSend = message => { if (message.type === 'file') { messages.push({ ...message.file, deviceId: 'dev_sender' }); setImmediate(() => f.ws.receive({ type: 'ack', id: message.file.id })); } };
  const source = new File([new Uint8Array(fileChunkBytes + 3).fill(9)], 'two.bin');
  await f.sender.sendFile(source);
  await Promise.all([...messages, ...messages].reverse().map(message => f.receiver.applyFile(message)));
  assert.equal(f.files.length, 1); assert.equal(await digest(f.files[0].blob), await digest(source));
  const body = await encryptForTunnel(f.tunnel, new Uint8Array([1, 2]));
  const meta = await encryptForTunnel(f.tunnel, new TextEncoder().encode(JSON.stringify({ name: 'invalid', size: 1 })));
  await assert.rejects(f.receiver.applyFile({ kind: 'chunk', id: 'invalid_0', fileId: 'invalid', index: 0, total: 1, totalBytes: 1, bytes: 2,
    ...body, metaNonce: meta.nonce, metaCiphertext: meta.ciphertext, deviceId: 'dev_sender' }), /file_transfer_invalid/u);
  assert.equal(f.files.length, 1); assert.equal(f.receiver.fileTransfers.size, 0);
});

test('offline controls and congested RTC buffers have a hard cap without duplicating file controls', async t => {
  const f = pair(t);
  for (let i = 0; i < maxControlQueueCount; i++) f.sender.sendKnock();
  assert.throws(() => f.sender.sendKnock(), /file_transfer_busy/u);
  let directSends = 0;
  f.sender.p2pPeers.set('test', { pc: { sctp: { maxMessageSize: 65536 }, close() {} }, channel: {
    readyState: 'open', bufferedAmount: maxSocketBufferedBytes, send() { directSends++; }, close() {} }, retryTimer: 0 });
  f.sender.broadcastDirect({ type: 'notice.knock', knock: { id: 'knock_test' } });
  assert.equal(directSends, 0);
});

test('a crypto burst reconnects instead of queuing unlimited work, and sequential relay replay recovers every byte', async t => {
  const f = pair(t), original = FakeSocket.instances.at(-1); await original.open();
  const body = await encryptForTunnel(f.tunnel, new Uint8Array([5, 6]));
  const meta = await encryptForTunnel(f.tunnel, new TextEncoder().encode(JSON.stringify({ name: 'burst.bin', size: 2 })));
  const messages = Array.from({ length: 12 }, (_, index) => ({ kind: 'chunk', id: `burst_${index}_0`, fileId: `burst_${index}`,
    index: 0, total: 1, totalBytes: 2, bytes: 2, ...body, metaNonce: meta.nonce, metaCiphertext: meta.ciphertext, deviceId: 'dev_sender' }));
  const promises = messages.map(message => f.receiver.applyFile(message));
  assert.equal(f.receiver.incomingFileOperations, 4);
  assert.ok(f.receiver.incomingFileBytes <= 2_048_000);
  const results = await Promise.allSettled(promises);
  assert.equal(results.filter(value => value.status === 'rejected').length, 8);
  assert.equal(original.readyState, FakeSocket.CLOSED);
  const next = FakeSocket.instances.at(-1); await next.open();
  next.receive({ type: 'hello', updates: [], peers: [], files: messages });
  await until(() => f.files.length === messages.length);
  assert.equal(f.receiver.incomingFileOperations, 0); assert.equal(f.receiver.incomingFileBytes, 0);
  for (const file of f.files) assert.deepEqual(new Uint8Array(await file.blob.arrayBuffer()), new Uint8Array([5, 6]));
});

test('stale hello cannot mark a replacement connection ready or call UI after destroy', async t => {
  const f = pair(t); await f.ws.open();
  let release; const barrier = new Promise(done => { release = done; });
  f.sender.applyFile = async () => barrier;
  f.sender.ready = false;
  const receiving = f.sender.handleRawMessage(JSON.stringify({ type: 'hello', peers: [], updates: [], files: [{}] }), f.ws);
  await pause(0);
  const replacement = new FakeSocket(); f.sender.ws = replacement;
  release(); await receiving;
  assert.equal(f.sender.ready, false);
  let releaseAgain; const second = new Promise(done => { releaseAgain = done; });
  replacement.readyState = 1;
  f.sender.applyFile = async () => second;
  const afterDestroy = f.sender.handleRawMessage(JSON.stringify({ type: 'hello', peers: [], updates: [], files: [{}] }), replacement);
  await pause(0); f.sender.destroy(); releaseAgain(); await afterDestroy;
  assert.equal(f.sender.ready, false);
});

test('legacy false byte declaration is rejected before crypto admission and typed storage failure is visible', async t => {
  const f = pair(t), receiverWs = FakeSocket.instances.at(-1); await receiverWs.open();
  await assert.rejects(f.receiver.applyFile({ id: 'legacy_false', bytes: 0, nonce: 'nonce',
    metaNonce: 'meta', metaCiphertext: 'meta', ciphertext: 'A'.repeat(1_000_000), deviceId: 'other' }), /file_transfer_invalid/u);
  assert.equal(f.receiver.incomingFileOperations, 0); assert.equal(f.receiver.fileTransfers.size, 0);
  const body = await encryptForTunnel(f.tunnel, new Uint8Array([7]));
  const meta = await encryptForTunnel(f.tunnel, new TextEncoder().encode(JSON.stringify({ name: 'large.bin', size: 20_000_000 })));
  const errors = []; f.receiver.callbacks.onFileError = value => errors.push(value);
  await f.receiver.handleRawMessage(JSON.stringify({ type: 'file', sequence: 1, file: { id: 'large_0', fileId: 'large',
    kind: 'chunk', index: 0, total: 80, totalBytes: 20_000_000, bytes: 1, ...body,
    metaNonce: meta.nonce, metaCiphertext: meta.ciphertext, deviceId: 'other' } }), receiverWs);
  assert.deepEqual(errors, [{ fileId: 'large', code: 'file_storage_unavailable' }]);
  assert.equal(f.files.length, 0);
  assert.ok(receiverWs.sent.some(value => value.type === 'replay.skip' && value.sequence === 1));
  assert.equal(receiverWs.sent.some(value => value.type === 'replay.ack' && value.sequence === 1), false);
});

test('sender identity does not suppress its own file on a fresh local session', async t => {
  const f = pair(t), bytes = new Uint8Array([3, 2, 1]);
  const body = await encryptForTunnel(f.tunnel, bytes);
  const meta = await encryptForTunnel(f.tunnel, new TextEncoder().encode(JSON.stringify({ name: 'mine.bin', size: 3 })));
  await f.sender.applyFile({ id: 'mine_0', fileId: 'mine', kind: 'chunk', index: 0, total: 1, totalBytes: 3, bytes: 3,
    ...body, metaNonce: meta.nonce, metaCiphertext: meta.ciphertext, deviceId: 'dev_sender' });
  assert.equal(f.files.length, 1); assert.deepEqual(new Uint8Array(await f.files[0].blob.arrayBuffer()), bytes);
});

test('pending inventory decrypts names after reload and explicit discard waits for the delete ACK', async t => {
  const f = pair(t); await f.ws.open();
  const meta = await encryptForTunnel(f.tunnel, new TextEncoder().encode(JSON.stringify({ name: 'Черновик 🐝.zip', size: 6 })));
  const changes = []; f.sender.callbacks.onFilePending = files => changes.push(files);
  await f.sender.handleMessage({ type: 'files.pending', files: [{ fileId: 'interrupted', totalBytes: 6, receivedBytes: 3,
    totalChunks: 2, receivedChunks: 1, deviceId: 'dev_sender', nick: 'Sender', metaNonce: meta.nonce, metaCiphertext: meta.ciphertext }] });
  assert.equal(changes.at(-1)[0].name, 'Черновик 🐝.zip'); assert.equal(f.files.length, 0);
  let finished = false;
  const discarded = f.sender.discardFileTransfer('interrupted').then(() => { finished = true; });
  await until(() => f.ws.sent.some(message => message.type === 'file' && message.file.kind === 'delete'));
  assert.equal(finished, false); assert.equal(changes.at(-1).length, 1);
  const deletion = f.ws.sent.find(message => message.type === 'file' && message.file.kind === 'delete');
  f.ws.receive({ type: 'ack', id: deletion.file.id });
  await discarded; assert.equal(finished, true); assert.deepEqual(changes.at(-1), []);
  await assert.rejects(f.sender.sendFileChunkFromBytes('interrupted', { name: 'old', size: 6 }, new Uint8Array([4, 5, 6]), 1, 2), /file_transfer_cancelled/u);
});
