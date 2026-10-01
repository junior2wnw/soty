import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync } from 'node:crypto';
import { mkdtemp, mkdir, open, readFile, writeFile, rename, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { Readable, Writable, Duplex } from 'node:stream';
import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import { getEventListeners } from 'node:events';
import os from 'node:os';
import path from 'node:path';
import { encryptBackup } from './backup.mjs';
import { verifyEncryptedBackup } from './verify-backup.mjs';
import { inspectRestorableBackup, sendAuthenticatedBackup } from './restore-backup.mjs';

// All bytes/keys are synthetic. No transport, real archive, shell key handling,
// or production ACL claim. The fixture does not import the shared parser.
const keys = generateKeyPairSync('rsa', { modulusLength: 3072 });
const privateKeyPem = keys.privateKey.export({ type: 'pkcs8', format: 'pem' });
const publicKey = keys.publicKey.export({ type: 'spki', format: 'pem' });
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const GEN = '1234567890abcdef1234567890abcdef';
const CHECKPOINT = sha('sender independent synthetic cold checkpoint');
const LIMITS = { archiveBytes: 2_097_152, plaintextBytes: 2_097_152, fileBytes: 524_288,
  extractedBytes: 1_048_576, entries: 16, headers: 64, pathBytes: 8192, pathDepth: 8,
  externalFiles: 4, externalBytes: 4096, wallMs: 30_000, idleMs: 5000 };
const CODES = new Set(['restore_archive_invalid', 'restore_incomplete', 'restore_authentication_failed',
  'restore_limit_exceeded', 'restore_timeout', 'restore_io_failed', 'restore_cleanup_pending']);
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const turn = () => new Promise(resolve => setImmediate(resolve));
const octal = bytes => parseInt(bytes.toString('ascii').replace(/\0.*$/s, '').trim() || '0', 8);
const text = bytes => bytes.toString('utf8').replace(/\0.*$/s, '');
const compare = (a, b) => a < b ? -1 : a > b ? 1 : 0;

async function fixture(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-sender-library-'));
  t.after(async () => {
    assert.equal(path.dirname(root), path.resolve(os.tmpdir())); assert.match(path.basename(root), /^soty-sender-library-/);
    await rm(root, { recursive: true, force: true });
  });
  const source = path.join(root, 'source'); await mkdir(source);
  const db = new DatabaseSync(path.join(source, 'connector-store.sqlite'));
  try { db.exec("CREATE TABLE fixture(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO fixture VALUES(1,'synthetic-only')"); }
  finally { db.close(); }
  await writeFile(path.join(source, 'payload.bin'), Buffer.alloc(180_003, 97));
  const packed = spawnSync('tar', ['--format=ustar', '-C', source, '-cf', '-', '.'], {
    windowsHide: true, stdio: 'pipe', timeout: 10_000, maxBuffer: 2_097_152,
  });
  assert.equal(packed.status, 0, 'system tar must create the bounded fixture');
  const tar = packed.stdout, records = [];
  for (let at = 0; at + 512 <= tar.length;) {
    const header = tar.subarray(at, at + 512); if (header.every(byte => byte === 0)) break;
    const size = octal(header.subarray(124, 136));
    const name = text(header.subarray(0, 100)).replace(/^\.\//, '').replace(/\/$/, '');
    const directory = header[156] === 53;
    records.push({ start: at, end: at + 512 + Math.ceil(size / 512) * 512,
      file: { path: directory && name === '.' ? '' : name, type: directory ? 'directory' : 'file', size,
        sha256: directory ? null : sha(tar.subarray(at + 512, at + 512 + size)),
        uid: octal(header.subarray(108, 116)), gid: octal(header.subarray(116, 124)), mode: octal(header.subarray(100, 108)) } });
    at += 512 + Math.ceil(size / 512) * 512;
  }
  const config = Buffer.from('synthetic private restore settings');
  const inventory = { files: records.map(row => row.file).sort((a, b) => compare(a.path, b.path)),
    stores: [{ id: 'connect', required: true, present: true, format: 'synthetic.sqlite.v1',
      identitySha256: sha('sender-source-identity'), paths: ['connector-store.sqlite'] }],
    external: [{ id: 'runtime-config', required: true, present: true, size: config.length,
      sha256: sha(config), uid: 10003, gid: 10004, mode: 0o600 }] };
  const sourceWitness = { generationId: GEN, checkpointSha256: CHECKPOINT, inventorySha256: sha(JSON.stringify(inventory)) };
  const metadata = { offline: true, dataFormat: 'tar', original: { State: { Running: false },
    Mounts: [{ Destination: '/data', Type: 'volume' }] }, secrets: {},
    restoreManifest: { version: 1, generationId: GEN, checkpointSha256: CHECKPOINT, inventory },
    restoreFiles: { 'runtime-config': config.toString('base64') } };
  let sequence = 0;
  return { root, tar, records, metadata, sourceWitness, async encrypt(archive = tar, meta = metadata) {
    const file = path.join(root, `fixture-${++sequence}.enc`);
    await encryptBackup({ output: file, publicKey, metadata: meta, stream: Readable.from([archive]) });
    const ciphertext = await readFile(file), metaBytes = Buffer.from(JSON.stringify(meta)), size = Buffer.alloc(4);
    size.writeUInt32BE(metaBytes.length);
    return { options: { file, privateKeyPem, expectedSha256: sha(ciphertext),
      expectedManifestSha256: sha(JSON.stringify(meta.restoreManifest)), sourceWitness: structuredClone(sourceWitness), limits: { ...LIMITS } },
    ciphertext, plaintext: Buffer.concat([size, metaBytes, archive]) };
  } };
}

function collector(hooks = {}) {
  const state = { writes: 0, bytes: 0, ends: 0, closes: 0, destroys: 0, finals: 0, chunks: [], borrowed: [], order: [] };
  const closed = deferred();
  const output = new Writable({ highWaterMark: hooks.highWaterMark ?? 1,
    write(chunk, encoding, callback) {
      state.writes++; state.bytes += chunk.length; state.borrowed.push(chunk);
      assert.ok(chunk.length <= 65536 && state.bytes <= LIMITS.plaintextBytes, 'fixture output remains bounded');
      if (hooks.write) hooks.write(chunk, callback, state);
      else { state.chunks.push(Buffer.from(chunk)); setImmediate(callback); }
    },
    final(callback) { state.finals++; if (hooks.final) hooks.final(callback, state); else callback(); },
    ...(hooks.destroy ? { destroy(error, callback) { state.destroys++; hooks.destroy(callback, state); } } : {}),
  });
  // Observe actual public write callbacks and drain order without replacing the
  // native _write/onwrite machinery. No stream event is forged by these tests.
  const write = output.write, end = output.end;
  output.write = function (chunk, callback) {
    return Reflect.apply(write, this, [chunk, error => { state.order.push('callback'); callback(error); }]);
  };
  output.end = function (...args) { state.ends++; return Reflect.apply(end, this, args); };
  output.on('drain', () => state.order.push('drain'));
  output.on('error', () => {}); // Fixture-owned observer; never prints errors.
  output.on('close', () => { state.closes++; closed.resolve(); });
  return { output, state, closed: closed.promise };
}
function safeError(error, expected) {
  assert.ok(error instanceof Error && CODES.has(error.code), 'only a fixed sender refusal is observable');
  if (expected) assert.ok(error.code === expected, 'refusal must match the causal branch');
  assert.ok(error.message === error.code && error.stack === `Error: ${error.code}`);
  assert.deepEqual(Object.getOwnPropertyNames(error).sort(), ['code', 'message', 'stack']);
}
async function refused(options, sink, expected) {
  let error; try { await sendAuthenticatedBackup({ ...options, output: sink.output }); } catch (caught) { error = caught; }
  safeError(error, expected); return error;
}
async function observedReads(t, hooks = {}) {
  const probe = await open(new URL('./restore-sender.test.mjs', import.meta.url), 'r');
  const prototype = Object.getPrototypeOf(probe); await probe.close();
  const originalRead = prototype.read, observed = new Set(), completed = new Map();
  const state = { calls: 0, passes: 0, observed, completed };
  t.mock.method(prototype, 'read', async function (...args) {
    if (!observed.has(this)) {
      observed.add(this); const close = this.close;
      t.mock.method(this, 'close', async function (...closeArgs) {
        const result = await Reflect.apply(close, this, closeArgs);
        completed.set(this, (completed.get(this) ?? 0) + 1);
        await hooks.afterClose?.(this, state); return result;
      });
    }
    state.calls++; if (args[3] === 0 && args[2] === 12) state.passes++;
    await hooks.before?.(this, args, state);
    const result = await Reflect.apply(originalRead, this, args);
    await hooks.after?.(this, args, result, state); return result;
  });
  return state;
}
function allClosed(reads) {
  assert.equal(reads.observed.size, 1, 'both passes must share one real archive descriptor');
  assert.equal(reads.completed.size, 1);
  assert.ok([...reads.observed].every(handle => handle.fd === -1 && reads.completed.get(handle) === 1));
}
function releasedListeners(sink, signal) {
  assert.equal(sink.output.listenerCount('drain'), 1); assert.equal(sink.output.listenerCount('error'), 1);
  assert.equal(sink.output.listenerCount('finish'), 0); assert.equal(sink.output.listenerCount('close'), 1);
  if (signal) assert.equal(getEventListeners(signal, 'abort').length, 0);
}

test('sender emits exact framing through native HWM1 drain before callback after two same-FD authenticated passes', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), sink = collector();
  const legacy = await verifyEncryptedBackup(x.options), dry = await inspectRestorableBackup(x.options);
  assert.ok(Object.entries(legacy).every(([key, value]) => dry[key] === value), 'legacy counts and receipt remain unchanged');
  const reads = await observedReads(t);
  try {
    const receipt = await sendAuthenticatedBackup({ ...x.options, output: sink.output });
    assert.deepEqual(Object.keys(receipt).sort(), ['authenticated', 'archiveSha256', 'plaintextSha256', 'manifestSha256', 'plaintextBytes'].sort());
    assert.ok(receipt.authenticated && receipt.archiveSha256 === x.options.expectedSha256
      && receipt.manifestSha256 === x.options.expectedManifestSha256 && receipt.plaintextSha256 === sha(x.plaintext));
    assert.equal(receipt.plaintextBytes, x.plaintext.length);
    assert.ok(Buffer.concat(sink.state.chunks).equals(x.plaintext), 'wire bytes equal independently assembled framing');
    assert.ok(sink.state.borrowed.every(chunk => chunk.every(byte => byte === 0)), 'all consumed transient chunks were wiped');
    assert.equal(reads.passes, 2); allClosed(reads);
    assert.equal(sink.state.ends, 1); assert.equal(sink.state.closes, 1); assert.ok(sink.output.closed && sink.output.writableFinished);
    assert.ok(sink.state.order.length >= 4 && sink.state.order.every((item, i) => item === (i % 2 ? 'callback' : 'drain')));
    releasedListeners(sink);
  } finally { t.mock.restoreAll(); }
});

test('first-pass crypto and completeness failures write zero bytes and never end their adopted output', { timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt();
  const tag = Buffer.from(x.ciphertext); tag[tag.length - 1] ^= 1;
  const badTag = path.join(f.root, 'bad-tag.enc'), truncated = path.join(f.root, 'truncated.enc');
  await writeFile(badTag, tag); await writeFile(truncated, x.ciphertext.subarray(0, -40));
  const omitted = structuredClone(f.metadata);
  omitted.restoreManifest.inventory.files = omitted.restoreManifest.inventory.files.filter(row => row.path !== 'connector-store.sqlite');
  omitted.restoreManifest.inventory.stores = [];
  const parts = f.records.filter(row => row.file.path !== 'connector-store.sqlite').map(row => f.tar.subarray(row.start, row.end));
  const incomplete = await f.encrypt(Buffer.concat([...parts, Buffer.alloc(1024)]), omitted);
  const wrongKey = generateKeyPairSync('rsa', { modulusLength: 3072 }).privateKey.export({ type: 'pkcs8', format: 'pem' });
  const cases = [
    [{ ...x.options, privateKeyPem: 'synthetic invalid private key canary' }, 'restore_authentication_failed'],
    [{ ...x.options, privateKeyPem: wrongKey }, 'restore_authentication_failed'],
    [{ ...x.options, file: badTag, expectedSha256: sha(tag) }, 'restore_authentication_failed'],
    [{ ...x.options, file: truncated, expectedSha256: sha(x.ciphertext.subarray(0, -40)) }, null],
    [{ ...x.options, expectedSha256: '0'.repeat(64) }, 'restore_authentication_failed'],
    [{ ...x.options, expectedManifestSha256: '0'.repeat(64) }, 'restore_incomplete'],
    [{ ...x.options, sourceWitness: { ...x.options.sourceWitness, checkpointSha256: '0'.repeat(64) } }, 'restore_incomplete'],
    [incomplete.options, 'restore_incomplete'],
  ];
  for (const [options, code] of cases) {
    const sink = collector(); await refused(options, sink, code);
    assert.equal(sink.state.writes, 0); assert.equal(sink.state.ends, 0); assert.ok(sink.output.closed);
  }
});

test('late real second-pass tag mutation reaches output but cannot authenticate or send EOF', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), sink = collector(); let changed = false;
  const reads = await observedReads(t, { async before(handle, args, state) {
    if (state.passes === 2 && args[3] === x.ciphertext.length - 16 && args[2] === 16) {
      const writer = await open(x.options.file, 'r+');
      try { const bytes = Buffer.from(x.ciphertext.subarray(-16)); bytes[0] ^= 1;
        await writer.write(bytes, 0, bytes.length, x.ciphertext.length - 16); await writer.sync(); changed = true;
      } finally { await writer.close(); }
    }
  } });
  try {
    await refused(x.options, sink, 'restore_authentication_failed');
    assert.ok(changed && sink.state.bytes > 0, 'mutated tag was read only after the first authenticated pass');
    assert.ok(Buffer.concat(sink.state.chunks).equals(x.plaintext), 'GCM final, not parser data, rejects this case');
    assert.equal(reads.passes, 2); allClosed(reads); assert.equal(sink.state.ends, 0); assert.ok(sink.output.closed);
  } finally { t.mock.restoreAll(); }
});

test('real append truncation header and ciphertext changes between passes never produce sender success or EOF', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t);
  for (const mutation of ['append', 'truncate', 'header', 'ciphertext']) await t.test(mutation, { concurrency: false }, async phase => {
    const x = await f.encrypt(), sink = collector(); let changed = false, eofReads = 0;
    const reads = await observedReads(phase, { async after(handle, args, result, state) {
      if (args[3] !== x.ciphertext.length || args[2] !== 1 || result.bytesRead !== 0 || ++eofReads !== 2) return;
      assert.equal(state.passes, 1);
      const writer = await open(x.options.file, 'r+');
      try {
        if (mutation === 'append') await writer.write(Buffer.from([1]), 0, 1, x.ciphertext.length);
        else if (mutation === 'truncate') await writer.truncate(x.ciphertext.length - 35);
        else {
          const at = mutation === 'header' ? 12 : 12 + x.ciphertext.readUInt32BE(8) + 10;
          await writer.write(Buffer.from([x.ciphertext[at] ^ 1]), 0, 1, at);
        }
        await writer.sync(); changed = true;
      } finally { await writer.close(); }
    } });
    try { await refused(x.options, sink); assert.ok(changed); allClosed(reads); assert.equal(sink.state.ends, 0); assert.ok(sink.output.closed); }
    finally { phase.mock.restoreAll(); }
  });
});

test('a newly valid GCM archive replacing the same-FD bytes between passes cannot replace the trusted archive pin', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), replacement = await f.encrypt(), sink = collector();
  assert.equal(replacement.ciphertext.length, x.ciphertext.length);
  assert.notEqual(replacement.options.expectedSha256, x.options.expectedSha256);
  assert.equal((await verifyEncryptedBackup(replacement.options)).authenticated, true);
  let eofReads = 0, changed = false;
  const reads = await observedReads(t, { async after(handle, args, result, state) {
    if (args[3] !== x.ciphertext.length || args[2] !== 1 || result.bytesRead !== 0 || ++eofReads !== 2) return;
    assert.equal(state.passes, 1);
    const writer = await open(x.options.file, 'r+');
    try { await writer.writeFile(replacement.ciphertext); await writer.sync(); changed = true; }
    finally { await writer.close(); }
  } });
  try {
    await refused(x.options, sink, 'restore_authentication_failed');
    assert.ok(changed && Buffer.concat(sink.state.chunks).equals(x.plaintext), 'new valid ciphertext must still fail the original archive pin');
    assert.equal(reads.passes, 2); allClosed(reads); assert.equal(sink.state.ends, 0); assert.ok(sink.output.closed);
  } finally { t.mock.restoreAll(); }
});

test('pathname replacement cannot reopen or switch the descriptor used by the second pass', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), sink = collector(); let changed = false;
  const reads = await observedReads(t, { async before(handle, args, state) {
    if (!changed && state.passes === 2 && args[3] === 0 && args[2] === 12) {
      changed = true; await rename(x.options.file, path.join(f.root, 'original-owned.enc'));
      await writeFile(x.options.file, Buffer.from('replacement path canary, never a valid encrypted archive'));
    }
  } });
  try {
    let error, result; try { result = await sendAuthenticatedBackup({ ...x.options, output: sink.output }); } catch (caught) { error = caught; }
    // Filesystems differ in whether rename changes ctime; either outcome must
    // use the original FD and bytes. A detected identity change is a refusal.
    if (error) { safeError(error, 'restore_authentication_failed'); assert.equal(sink.state.ends, 0); }
    else assert.ok(result.plaintextSha256 === sha(x.plaintext));
    assert.ok(changed && Buffer.concat(sink.state.chunks).equals(x.plaintext)); allClosed(reads); assert.equal(reads.passes, 2);
  } finally { t.mock.restoreAll(); }
});

test('held native write blocks next file read and keeps borrowed plaintext intact through abort and early native close', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), controller = new AbortController(), entered = deferred();
  let release, borrowed, before, settled = false, reasonReads = 0;
  const sink = collector({ write(chunk, callback) {
    borrowed = chunk; before = Buffer.from(chunk); release = callback; entered.resolve();
  } }); // Ordinary immediate native _destroy; do not hold or forge close.
  Object.defineProperty(controller.signal, 'reason', { get() { reasonReads++; throw new Error('synthetic secret reason'); } });
  const reads = await observedReads(t);
  const pending = sendAuthenticatedBackup({ ...x.options, output: sink.output, signal: controller.signal })
    .then(value => ({ value }), error => ({ error })).finally(() => { settled = true; });
  try {
    await entered.promise; const calls = reads.calls;
    await turn(); assert.equal(reads.calls, calls); assert.equal(settled, false);
    controller.abort('synthetic abort canary'); await sink.closed; await turn();
    assert.ok(sink.output.closed && !settled, 'actual close must not settle the outstanding write');
    assert.ok(borrowed.equals(before), 'borrowed chunk must not be wiped before the actual supplied callback');
    assert.equal(reads.calls, calls); assert.equal(sink.state.ends, 0);
    release(); release = null; const result = await pending; safeError(result.error, 'restore_io_failed');
    assert.ok(borrowed.every(byte => byte === 0)); assert.equal(reasonReads, 0); allClosed(reads);
    assert.ok(!sink.state.order.includes('drain'), 'destroyed Writable need not drain; callback still allows refusal');
    releasedListeners(sink, controller.signal);
  } finally { release?.(); await pending; t.mock.restoreAll(); }
});

test('success awaits held final then actual held destroy close after the archive FD has closed', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), finalEntered = deferred(), destroyEntered = deferred();
  let finalDone, destroyDone, settled = false;
  const sink = collector({ final(callback) { finalDone = callback; finalEntered.resolve(); },
    destroy(callback) { destroyDone = callback; destroyEntered.resolve(); } });
  const reads = await observedReads(t);
  const pending = sendAuthenticatedBackup({ ...x.options, output: sink.output }).finally(() => { settled = true; });
  try {
    await finalEntered.promise; allClosed(reads); await turn();
    assert.equal(settled, false); assert.equal(sink.state.ends, 1); assert.equal(sink.output.writableFinished, false);
    finalDone(); finalDone = null; await destroyEntered.promise; await turn();
    assert.ok(sink.output.writableFinished && !sink.output.closed && !settled);
    destroyDone(); destroyDone = null; assert.equal((await pending).authenticated, true); assert.ok(sink.output.closed);
  } finally { finalDone?.(); destroyDone?.(); await pending; t.mock.restoreAll(); }
});

test('abort after end and early close reports cleanup pending for an uncompleted native final', { timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), entered = deferred(), controller = new AbortController(); let finalDone;
  const sink = collector({ final(callback) { finalDone = callback; entered.resolve(); } });
  const pending = sendAuthenticatedBackup({ ...x.options, output: sink.output, signal: controller.signal })
    .then(value => ({ value }), error => ({ error }));
  try {
    await entered.promise; controller.abort('synthetic post-end canary'); await sink.closed;
    const result = await pending; safeError(result.error, 'restore_cleanup_pending');
    assert.equal(sink.state.ends, 1); assert.ok(sink.output.closed && !sink.output.writableFinished);
    assert.ok(sink.output._writableState.pendingcb > 0, 'close is not a final-operation completion proof');
  } finally { finalDone?.(); await turn(); }
});

test('manual finish during a held native final cannot turn actual close into authenticated success', { timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), entered = deferred(); let finalDone;
  const sink = collector({ final(callback) { finalDone = callback; entered.resolve(); } });
  const pending = sendAuthenticatedBackup({ ...x.options, output: sink.output })
    .then(value => ({ value }), error => ({ error }));
  try {
    await entered.promise; assert.equal(sink.output.writableFinished, false);
    sink.output.emit('finish'); // Malformed wrapper event; destroy and close below remain native.
    sink.output.destroy(); await sink.closed;
    const result = await pending; safeError(result.error, 'restore_cleanup_pending');
    assert.equal(sink.state.ends, 1); assert.ok(sink.output.closed && !sink.output.writableFinished);
    assert.ok(sink.output._writableState.pendingcb > 0, 'manual finish is not native final completion');
  } finally { finalDone?.(); await turn(); }
});

test('write final and destroy errors are fixed refusals with correct pre-end and post-end distinctions', { timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt();
  for (const stage of ['write', 'final', 'destroy']) {
    const hook = stage === 'write' ? { write(chunk, callback) { callback(new Error('synthetic payload/error canary')); } }
      : { [stage](callback) { callback(new Error('synthetic private filesystem canary')); } };
    const sink = collector(hook); await refused(x.options, sink, 'restore_io_failed');
    assert.equal(sink.state.ends, stage === 'write' ? 0 : 1); assert.ok(sink.output.closed);
  }
});

test('premature native output close refuses before end and still closes the real archive descriptor', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), sink = collector(); let interrupted = false;
  const reads = await observedReads(t, { async after() {
    if (!interrupted) { interrupted = true; sink.output.destroy(); await sink.closed; }
  } });
  try { await refused(x.options, sink, 'restore_io_failed'); allClosed(reads); assert.equal(sink.state.ends, 0); assert.equal(sink.state.writes, 0); }
  finally { t.mock.restoreAll(); }
});

test('failed actual file close cannot send end or become a successful receipt', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), sink = collector();
  const reads = await observedReads(t, { afterClose() { throw new Error('synthetic descriptor close canary'); } });
  try { await refused(x.options, sink, 'restore_cleanup_pending'); allClosed(reads); assert.equal(sink.state.ends, 0); assert.ok(sink.output.closed); }
  finally { t.mock.restoreAll(); }
});

test('sender captures strict pins and limits before its first asynchronous file operation', { timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), sink = collector();
  const pending = sendAuthenticatedBackup({ ...x.options, output: sink.output });
  x.options.sourceWitness.inventorySha256 = '0'.repeat(64); x.options.limits.fileBytes = 1;
  assert.ok((await pending).plaintextSha256 === sha(x.plaintext));
});

test('closed option and fresh Writable profiles reject before adoption without exposing hostile values', { timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(); let unknownReads = 0;
  for (const input of [null, undefined, { ...x.options, get output() { throw new Error('synthetic output canary'); } },
    { ...x.options, output: collector().output, get file() { throw new Error('synthetic C:/private/canary'); } },
    { ...x.options, output: collector().output, get unexpected() { unknownReads++; throw new Error('synthetic unexpected'); } }]) {
    let error; try { await sendAuthenticatedBackup(input); } catch (caught) { error = caught; } safeError(error);
  }
  assert.equal(unknownReads, 0);
  const make = options => new Writable({ ...options, write(chunk, encoding, callback) { callback(); } });
  const outputs = [make({ emitClose: false }), make({ autoDestroy: false }), make({ objectMode: true }),
    make({ highWaterMark: 0 }), make({ highWaterMark: 65537 }),
    new Duplex({ read() {}, write(chunk, encoding, callback) { callback(); } }), make({})];
  outputs.at(-1).cork();
  for (const output of outputs) {
    let error; try { await sendAuthenticatedBackup({ ...x.options, output }); } catch (caught) { error = caught; }
    safeError(error, 'restore_archive_invalid'); assert.equal(output.destroyed, false, 'invalid output remains caller-owned'); output.destroy();
  }
  for (const amount of [0, -1, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1, '4096']) {
    const sink = collector(); await refused({ ...x.options, limits: { ...LIMITS, archiveBytes: amount } }, sink, 'restore_limit_exceeded');
    assert.equal(sink.output.destroyed, false); sink.output.destroy();
  }
  const sink = collector(); await refused({ ...x.options, file: 'relative.enc' }, sink, 'restore_archive_invalid');
  assert.equal(sink.output.destroyed, false); sink.output.destroy();
});

test('first-pass byte and entry limit plus one failures cannot write plaintext', { timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt();
  for (const delta of [{ archiveBytes: x.ciphertext.length - 1 }, { plaintextBytes: x.plaintext.length - 1 },
    { fileBytes: 180_002 }, { entries: f.metadata.restoreManifest.inventory.files.length - 1 }]) {
    const sink = collector(); await refused({ ...x.options, limits: { ...LIMITS, ...delta } }, sink, 'restore_limit_exceeded');
    assert.equal(sink.state.writes, 0); assert.equal(sink.state.ends, 0); assert.ok(sink.output.closed);
  }
});

test('wall spans both passes and idle safe points use real read and supplied write progress', { concurrency: false, timeout: 15_000 }, async t => {
  const f = await fixture(t);
  for (const mode of ['control', 'wall', 'idle']) await t.test(mode, { concurrency: false }, async phase => {
    const x = await f.encrypt(); let clock = 0, elapsedRead = false;
    const sink = collector({ write(chunk, callback, state) {
      state.chunks.push(Buffer.from(chunk)); if (mode === 'idle') clock += 101; setImmediate(callback);
    } });
    const reads = await observedReads(phase, { after(handle, args, result, state) {
      if (result.bytesRead > 0) { clock++; elapsedRead = true; }
      if (mode === 'wall' && state.passes === 2 && args[3] === 0 && args[2] === 12) clock = 1001;
    } });
    phase.mock.method(performance, 'now', () => clock);
    try {
      const options = { ...x.options, limits: { ...LIMITS, wallMs: 1000, idleMs: mode === 'wall' ? 2000 : 100 } };
      if (mode === 'control') assert.equal((await sendAuthenticatedBackup({ ...options, output: sink.output })).authenticated, true);
      else { await refused(options, sink, 'restore_timeout'); assert.equal(sink.state.ends, 0); }
      assert.ok(elapsedRead); allClosed(reads); assert.ok(sink.output.closed);
      if (mode === 'wall') assert.equal(reads.passes, 2);
      if (mode === 'idle') assert.ok(sink.state.writes > 0);
    } finally { phase.mock.restoreAll(); }
  });
});

test('already aborted native signal destroys only adopted output and never reads the reason', { timeout: 15_000 }, async t => {
  const f = await fixture(t), x = await f.encrypt(), controller = new AbortController(), sink = collector(); let reasonReads = 0;
  controller.abort('synthetic reason'); Object.defineProperty(controller.signal, 'reason', { get() { reasonReads++; throw new Error('synthetic secret'); } });
  await refused({ ...x.options, signal: controller.signal }, sink, 'restore_io_failed');
  assert.equal(reasonReads, 0); assert.equal(sink.state.writes, 0); assert.equal(sink.state.ends, 0); assert.ok(sink.output.closed);
});
