import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, open, readFile, writeFile, lstat, stat, readdir, rm, lchown, chmod, symlink, statfs } from 'node:fs/promises';
import { constants } from 'node:fs';
import { Readable } from 'node:stream';
import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import os from 'node:os';
import path from 'node:path';
import { extractOwnedBackup } from './restore-backup.mjs';

// Independent, synthetic plaintext framing. No production decrypt/manifest
// builder is used to produce the witness or expected restored bytes.
const CHUNK = 65536, GEN = '1234567890abcdef1234567890abcdef';
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const CHECKPOINT = sha('sink-only independent synthetic checkpoint');
const LIMITS = Object.freeze({ plaintextBytes: 4 * 1024 * 1024, fileBytes: 1024 * 1024, extractedBytes: 4 * 1024 * 1024,
  entries: 32, headers: 64, pathBytes: 4096, pathDepth: 8, externalFiles: 4, externalBytes: 4096,
  wallMs: 30_000, idleMs: 5000, freeSpaceReserveBytes: 4096 });
const CODES = new Set(['restore_platform_unavailable', 'restore_target_invalid', 'restore_incomplete', 'restore_archive_invalid',
  'restore_limit_exceeded', 'restore_timeout', 'restore_io_failed', 'restore_cleanup_pending']);
const RECEIPT = ['extracted', 'targetId', 'manifestSha256', 'plaintextSha256', 'plaintextBytes', 'entries', 'fileBytes', 'readbackVerified'].sort();
const compare = (a, b) => a < b ? -1 : a > b ? 1 : 0;
function field(header, at, size, value) {
  header.fill(0, at, at + size); header.write(value.toString(8).padStart(size - 1, '0'), at, size - 1, 'ascii');
}
function checksum(header) {
  header.fill(32, 148, 156); field(header, 148, 7, header.reduce((sum, value) => sum + value, 0)); header[155] = 32;
}
function tarRecord(file, body = Buffer.alloc(0)) {
  const header = Buffer.alloc(512), name = file.path === '' ? './' : file.path;
  assert.ok(Buffer.byteLength(name) <= 100, 'short independent ustar fixture');
  header.write(name); field(header, 100, 8, file.mode); field(header, 108, 8, file.uid); field(header, 116, 8, file.gid);
  field(header, 124, 12, body.length); field(header, 136, 12, 1); header[156] = file.type === 'directory' ? 53 : 48;
  header.write('ustar\0', 257); header.write('00', 263); checksum(header);
  return { header, body };
}
function pack(records) {
  return Buffer.concat([...records.flatMap(r => [r.header, r.body, Buffer.alloc((512 - r.body.length % 512) % 512)]), Buffer.alloc(1024)]);
}
function projection(inventory) {
  return {
    files: inventory.files.map(f => ({ path: f.path, type: f.type, size: f.size, sha256: f.sha256, uid: f.uid, gid: f.gid, mode: f.mode }))
      .sort((a, b) => compare(a.path, b.path)),
    stores: inventory.stores.map(s => ({ id: s.id, required: s.required, present: s.present, format: s.format,
      identitySha256: s.identitySha256, paths: [...s.paths].sort(compare) })).sort((a, b) => compare(a.id, b.id)),
    external: inventory.external.map(f => ({ id: f.id, required: f.required, present: f.present, size: f.size,
      sha256: f.sha256, uid: f.uid, gid: f.gid, mode: f.mode })).sort((a, b) => compare(a.id, b.id)),
  };
}
function assemble(inventory, bodies, external, records) {
  const projected = projection(inventory);
  const restoreManifest = { version: 1, generationId: GEN, checkpointSha256: CHECKPOINT, inventory: projected };
  const metadata = { offline: true, dataFormat: 'tar', original: { State: { Running: false }, Mounts: [{ Type: 'volume', Destination: '/data' }] },
    secrets: {}, restoreManifest, restoreFiles: { 'runtime-config': external.toString('base64') } };
  const bytes = Buffer.from(JSON.stringify(metadata)), length = Buffer.alloc(4); length.writeUInt32BE(bytes.length);
  return { frame: Buffer.concat([length, bytes, pack(records ?? inventory.files.map(f => tarRecord(f, bodies.get(f.path))))]),
    manifestSha256: sha(JSON.stringify(restoreManifest)),
    witness: { generationId: GEN, checkpointSha256: CHECKPOINT, inventorySha256: sha(JSON.stringify(projected)) } };
}
function stream(bytes) {
  return Readable.from((function* () { for (let at = 0; at < bytes.length; at += CHUNK) yield bytes.subarray(at, at + CHUNK); })(),
    { objectMode: false, highWaterMark: CHUNK });
}
async function pin(root) {
  const handle = await open(root, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try {
    const value = await handle.stat({ bigint: true });
    const info = await readFile(`/proc/self/fdinfo/${handle.fd}`, 'utf8');
    return { path: root, dev: value.dev, ino: value.ino, mountId: Number(/^mnt_id:\s*(\d+)$/m.exec(info)[1]) };
  } finally { await handle.close(); }
}
async function removeOwnedFixture(root) {
  assert.equal(path.dirname(root), path.resolve(os.tmpdir())); assert.match(path.basename(root), /^soty-sink-library-/);
  // Test cleanup only, after assertions. Do not follow links, and do not use
  // these mutations to read restored bytes. The retained RO audit is separate.
  const walk = async current => {
    const value = await lstat(current);
    if (!value.isDirectory() || value.isSymbolicLink()) return;
    await lchown(current, process.geteuid(), process.getegid()); await chmod(current, 0o700);
    for (const name of await readdir(current)) await walk(path.join(current, name));
  };
  await walk(root); await rm(root, { recursive: true, force: true });
}
async function fixture(t, { foreign = false } = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-sink-library-')); await chmod(root, 0o700);
  t.after(() => removeOwnedFixture(root));
  const namespace = path.join(root, 'target'); await mkdir(namespace, { mode: 0o700 });
  await mkdir(path.join(namespace, 'data'), { mode: 0o700 }); await mkdir(path.join(namespace, 'config'), { mode: 0o700 });
  const ns = await stat('/proc/self/ns/mnt', { bigint: true });
  const target = { targetId: '9876543210abcdef9876543210abcdef', mountNamespace: { dev: ns.dev, ino: ns.ino },
    namespace: await pin(namespace), dataRoot: await pin(path.join(namespace, 'data')), configRoot: await pin(path.join(namespace, 'config')) };
  const sqlitePath = path.join(root, 'source.sqlite'), db = new DatabaseSync(sqlitePath);
  db.exec("CREATE TABLE fixture(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO fixture VALUES(1,'synthetic-only')"); db.close();
  const bodies = new Map([['connector-store.sqlite', await readFile(sqlitePath)], ['notes.sqlite-wal', Buffer.from('synthetic WAL sidecar bytes')],
    ['rooms/deep/value.json', Buffer.alloc(170_003, 97)], ['empty.txt', Buffer.alloc(0)]]);
  const uid = process.geteuid(), gid = process.getegid();
  const descriptor = (name, body = null) => ({ path: name, type: body === null ? 'directory' : 'file', size: body?.length ?? 0,
    sha256: body === null ? null : sha(body), uid, gid, mode: body === null ? 0o700 : 0o600 });
  const files = [descriptor(''), descriptor('rooms'), descriptor('rooms/deep'), ...[...bodies].map(([name, body]) => descriptor(name, body))];
  if (foreign) for (const file of files) if (file.path !== '') {
    file.uid = 10001; file.gid = 10002; file.mode = file.type === 'directory' ? 0o500 : 0o600;
  }
  const external = Buffer.from('synthetic private operational configuration');
  const inventory = { files, stores: [{ id: 'connect', required: true, present: true, format: 'fixture.sqlite.v1',
    identitySha256: sha('independent store identity'), paths: ['connector-store.sqlite', 'notes.sqlite-wal'] }],
  external: [{ id: 'runtime-config', required: true, present: true, size: external.length, sha256: sha(external),
    uid: foreign ? 10003 : uid, gid: foreign ? 10004 : gid, mode: 0o600 }] };
  const framed = assemble(inventory, bodies, external);
  const sentinel = path.join(root, 'outside-sentinel'); await writeFile(sentinel, 'unchanged sentinel', { mode: 0o600 });
  const probe = await open(path.join(root, 'prototype'), 'w+'), prototype = Object.getPrototypeOf(probe); await probe.close();
  return { root, target, bodies, inventory, external, ...framed, prototype, sentinel,
    options(overrides = {}) { return { input: stream(framed.frame), target, expectedManifestSha256: framed.manifestSha256,
      sourceWitness: { ...framed.witness }, limits: { ...LIMITS }, ...overrides }; },
    async empty() { assert.equal((await readdir(target.dataRoot.path)).length, 0); assert.equal((await readdir(target.configRoot.path)).length, 0); },
    async sentinelUnchanged() { assert.ok(sha(await readFile(sentinel)) === sha('unchanged sentinel'), 'outside sentinel must remain unchanged'); } };
}
async function refused(options, expected) {
  let error;
  try { await extractOwnedBackup(options); } catch (caught) { error = caught; }
  assert.ok(error instanceof Error && CODES.has(error.code), 'fixed extraction refusal required');
  if (expected) assert.equal(error.code, expected);
  assert.ok(error.message === error.code && error.stack === `Error: ${error.code}`, 'no input/path/reason in diagnostics');
  assert.deepEqual(Object.getOwnPropertyNames(error).sort(), ['code', 'message', 'stack']);
}
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }

// One explicit platform branch, not skipped tests. Linux adapters can pin the
// complete nested name set; Windows proves only unavailable before any I/O.
test('private extraction obeys the current platform contract', { concurrency: 1, timeout: 120_000 }, async t => {
  if (process.platform !== 'linux') {
    let touched = 0;
    const hostile = new Proxy({}, { get() { touched++; throw new Error('synthetic private input'); }, ownKeys() { touched++; return []; } });
    await refused(hostile, 'restore_platform_unavailable'); assert.equal(touched, 0); return;
  }
  assert.equal(process.geteuid(), 0, 'Linux acceptance requires the reviewed root CHOWN/FOWNER helper profile');

  await t.test('restores exact main sidecar config empty and nested bytes with a closed receipt', async t => {
    const f = await fixture(t), options = f.options(), receipt = await extractOwnedBackup(options);
    assert.deepEqual(Object.keys(receipt).sort(), RECEIPT);
    assert.equal(receipt.extracted, true); assert.equal(receipt.readbackVerified, true); assert.equal(receipt.targetId, f.target.targetId);
    assert.ok(receipt.plaintextSha256 === sha(f.frame) && receipt.manifestSha256 === f.manifestSha256, 'whole framing and manifest pins');
    assert.equal(receipt.plaintextBytes, f.frame.length); assert.equal(receipt.entries, f.inventory.files.length);
    assert.equal(receipt.fileBytes, [...f.bodies.values()].reduce((sum, bytes) => sum + bytes.length, f.external.length));
    for (const [name, bytes] of f.bodies) assert.ok(sha(await readFile(path.join(f.target.dataRoot.path, name))) === sha(bytes), 'independent restored file equality');
    assert.ok(sha(await readFile(path.join(f.target.configRoot.path, 'runtime-config'))) === sha(f.external), 'independent config equality');
    for (const file of f.inventory.files) {
      const value = await lstat(path.join(f.target.dataRoot.path, file.path));
      assert.equal(value.uid, file.uid); assert.equal(value.gid, file.gid); assert.equal(value.mode & 0o7777, file.mode);
    }
    assert.equal(options.input.closed, true); await f.sentinelUnchanged();
    await refused(f.options(), 'restore_target_invalid');
  });

  await t.test('preserves foreign ownership and restrictive postorder metadata after same-FD readback', async t => {
    const f = await fixture(t, { foreign: true });
    const read = f.prototype.read, chown = f.prototype.chown, chmodFile = f.prototype.chmod;
    const readHandles = new Set(), changed = [], completed = [], directoryModes = [];
    t.mock.method(f.prototype, 'read', async function (...args) { const result = await read.apply(this, args); if (result.bytesRead) readHandles.add(this); return result; });
    t.mock.method(f.prototype, 'chown', async function (uid, gid) {
      const before = await this.stat();
      if (before.isFile() && before.size > 0) assert.ok(readHandles.has(this), 'real positional read occurred on this exact FD before foreign ownership');
      await chown.call(this, uid, gid);
      if (uid >= 10001) {
        const expected = { handle: this, uid, gid, regular: before.isFile() }; changed.push(expected);
        const close = this.close;
        t.mock.method(this, 'close', async function () {
          const value = await this.stat();
          assert.equal(value.uid, expected.uid); assert.equal(value.gid, expected.gid);
          assert.equal(value.mode & 0o7777, expected.regular ? 0o600 : 0o500);
          const result = await close.call(this); completed.push(this); return result;
        });
      }
    });
    t.mock.method(f.prototype, 'chmod', async function (mode) {
      await chmodFile.call(this, mode); const value = await this.stat();
      if (value.isDirectory() && value.uid === 10001) directoryModes.push(value.mode & 0o7777);
    });
    await extractOwnedBackup(f.options());
    assert.equal(changed.length, f.inventory.files.length - 1 + 1); assert.equal(completed.length, changed.length);
    assert.deepEqual(directoryModes, [0o500, 0o500]);
    const regular = await lstat(path.join(f.target.dataRoot.path, 'connector-store.sqlite'));
    assert.equal(regular.uid, 10001); assert.equal(regular.gid, 10002); assert.equal(regular.mode & 0o7777, 0o600);
    assert.equal((await lstat(f.target.namespace.path)).mode & 0o7777, 0o700);
    assert.equal((await lstat(f.target.configRoot.path)).mode & 0o7777, 0o700);
    // No permission change or foreign-file reopen is used as readback proof.
    await f.sentinelUnchanged(); t.mock.restoreAll();
  });

  await t.test('rejects target inode mount namespace aliases symlink ancestors and contamination before input', async t => {
    const f = await fixture(t);
    const variants = [
      { ...f.target, dataRoot: { ...f.target.dataRoot, ino: f.target.dataRoot.ino + 1n } },
      { ...f.target, dataRoot: { ...f.target.dataRoot, mountId: f.target.dataRoot.mountId + 1 } },
      { ...f.target, mountNamespace: { ...f.target.mountNamespace, ino: f.target.mountNamespace.ino + 1n } },
      { ...f.target, configRoot: { ...f.target.configRoot, dev: f.target.dataRoot.dev, ino: f.target.dataRoot.ino } },
    ];
    for (const target of variants) {
      const input = stream(f.frame); let reads = 0; const read = input._read; input._read = function (...args) { reads++; return read.apply(this, args); };
      await refused(f.options({ input, target }), 'restore_target_invalid'); assert.equal(reads, 0);
    }
    const alias = path.join(f.root, 'alias'); await symlink(f.target.namespace.path, alias);
    const linked = { ...f.target, namespace: { ...f.target.namespace, path: alias }, dataRoot: { ...f.target.dataRoot, path: `${alias}/data` },
      configRoot: { ...f.target.configRoot, path: `${alias}/config` } };
    await refused(f.options({ target: linked }), 'restore_target_invalid');
    await writeFile(path.join(f.target.dataRoot.path, 'preexisting'), 'kept');
    await refused(f.options(), 'restore_target_invalid');
    assert.ok(sha(await readFile(path.join(f.target.dataRoot.path, 'preexisting'))) === sha('kept')); await f.sentinelUnchanged();
  });

  await t.test('requires manifest witness and all config hashes before the first write', async t => {
    const f = await fixture(t);
    for (const options of [f.options({ expectedManifestSha256: '0'.repeat(64) }),
      f.options({ sourceWitness: { ...f.witness, inventorySha256: '0'.repeat(64) } })]) {
      await refused(options, 'restore_incomplete'); await f.empty();
    }
    const tampered = Buffer.from(f.frame), metaSize = tampered.readUInt32BE(0);
    const metadata = JSON.parse(tampered.subarray(4, metaSize + 4).toString());
    const bytes = Buffer.from(metadata.restoreFiles['runtime-config'], 'base64'); bytes[0] ^= 1;
    metadata.restoreFiles['runtime-config'] = bytes.toString('base64');
    const changed = Buffer.from(JSON.stringify(metadata)); assert.equal(changed.length, metaSize); changed.copy(tampered, 4);
    await refused(f.options({ input: stream(tampered) }), 'restore_incomplete'); await f.empty(); await f.sentinelUnchanged();
  });

  await t.test('rejects traversal links unsupported types duplicate entries and changed payload', async t => {
    const variants = [
      records => { records[0].header.fill(0, 0, 100); records[0].header.write('../escape'); checksum(records[0].header); },
      records => { records[3].header[156] = 50; records[3].header.write('outside', 157); checksum(records[3].header); },
      records => { records[3].header[156] = 54; checksum(records[3].header); },
      records => { records.splice(1, 0, { header: Buffer.from(records[0].header), body: Buffer.alloc(0) }); },
      records => { records.find(r => r.body.length > 100_000).body[200] ^= 1; },
    ];
    for (const mutate of variants) {
      const f = await fixture(t), records = f.inventory.files.map(file => tarRecord(file, Buffer.from(f.bodies.get(file.path) ?? Buffer.alloc(0))));
      mutate(records); const altered = assemble(f.inventory, f.bodies, f.external, records);
      await refused(f.options({ input: stream(altered.frame) })); await f.sentinelUnchanged();
    }
  });

  await t.test('enforces plaintext file entry and real free-space bounds', async t => {
    for (const [key, value] of [['plaintextBytes', 1], ['fileBytes', 8191], ['entries', 1]]) {
      const f = await fixture(t); await refused(f.options({ limits: { ...LIMITS, [key]: value } }), 'restore_limit_exceeded'); await f.empty();
    }
    const f = await fixture(t), available = await statfs(f.target.dataRoot.path, { bigint: true });
    assert.ok(available.blocks * available.bsize < BigInt(Number.MAX_SAFE_INTEGER), 'bounded local fixture filesystem capacity');
    await refused(f.options({ limits: { ...LIMITS, freeSpaceReserveBytes: Number.MAX_SAFE_INTEGER } }), 'restore_limit_exceeded');
    await f.empty(); await f.sentinelUnchanged();
  });

  await t.test('handles partial writes with one operation in flight and verifies actual stored bytes', async t => {
    const f = await fixture(t), original = f.prototype.write; let current = 0, maximum = 0, calls = 0;
    t.mock.method(f.prototype, 'write', async function (buffer, offset, length, position) {
      current++; maximum = Math.max(maximum, current); calls++;
      try { return await original.call(this, buffer, offset, Math.min(length, 997), position); }
      finally { current--; }
    });
    const receipt = await extractOwnedBackup(f.options());
    assert.equal(maximum, 1); assert.ok(calls > 170); assert.equal(receipt.readbackVerified, true);
    assert.ok(sha(await readFile(path.join(f.target.dataRoot.path, 'rooms/deep/value.json'))) === sha(f.bodies.get('rooms/deep/value.json')));
    t.mock.restoreAll();
  });

  await t.test('detects actual stored-byte corruption independently of the input payload digest', async t => {
    const f = await fixture(t), original = f.prototype.write; let altered = false;
    t.mock.method(f.prototype, 'write', async function (buffer, offset, length, position) {
      if (altered || !length) return original.call(this, buffer, offset, length, position);
      altered = true; const changed = Buffer.from(buffer.subarray(offset, offset + length)); changed[0] ^= 1;
      try { return await original.call(this, changed, 0, changed.length, position); } finally { changed.fill(0); }
    });
    await refused(f.options(), 'restore_incomplete'); assert.equal(altered, true);
    assert.ok(sha(await readFile(path.join(f.target.configRoot.path, 'runtime-config'))) !== sha(f.external), 'real stored bytes were changed');
    assert.equal((await readdir(f.target.dataRoot.path)).length, 0); await f.sentinelUnchanged(); t.mock.restoreAll();
  });

  await t.test('waits for a held real write before abort cleanup and never accepts a partial result', async t => {
    const f = await fixture(t), original = f.prototype.write;
    const entered = deferred(), release = deferred(), controller = new AbortController();
    let held, closed = false, settled = false;
    t.mock.method(f.prototype, 'write', async function (...args) {
      const result = await original.apply(this, args);
      if (!held) {
        held = this; const close = this.close;
        t.mock.method(this, 'close', async function () { const result = await close.call(this); closed = true; return result; });
        entered.resolve(); await release.promise;
      }
      return result;
    });
    const pending = refused(f.options({ signal: controller.signal }), 'restore_io_failed').then(() => { settled = true; });
    try {
      await entered.promise; controller.abort(new Error('private abort reason must not escape'));
      await new Promise(resolve => setImmediate(resolve)); assert.equal(settled, false); assert.equal(closed, false);
    } finally { release.resolve(); }
    await pending; assert.equal(closed, true); assert.equal(held.fd, -1); await f.sentinelUnchanged(); t.mock.restoreAll();
  });

  await t.test('closes held input and every owned descriptor on abort without reading the reason', async t => {
    const f = await fixture(t), originalStat = f.prototype.stat, originalRead = f.prototype.read, controller = new AbortController();
    let reads = 0, reasonReads = 0;
    const observed = new Set(), completed = new Map();
    const observe = handle => {
      if (observed.has(handle)) return; observed.add(handle); const close = handle.close;
      t.mock.method(handle, 'close', async function () {
        const result = await close.call(this); completed.set(this, (completed.get(this) ?? 0) + 1); return result;
      });
    };
    Object.defineProperty(controller.signal, 'reason', { get() { reasonReads++; throw new Error('private reason'); } });
    t.mock.method(f.prototype, 'stat', function (...args) { observe(this); return originalStat.apply(this, args); });
    t.mock.method(f.prototype, 'read', function (...args) { observe(this); return originalRead.apply(this, args); });
    const requested = deferred(); const input = new Readable({ highWaterMark: CHUNK, read() { reads++; requested.resolve(); } });
    const pending = refused(f.options({ input, signal: controller.signal }), 'restore_io_failed');
    await requested.promise; controller.abort('private reason'); await pending;
    assert.ok(reads > 0 && observed.size >= 3);
    assert.equal(completed.size, observed.size, 'every observed FileHandle completed its real close');
    assert.ok([...observed].every(handle => completed.get(handle) === 1 && handle.fd === -1), 'each observed handle is actually closed exactly once');
    assert.equal(input.closed, true); assert.equal(reasonReads, 0); await f.empty(); t.mock.restoreAll();
  });

  await t.test('cooperative timeout still awaits actual descriptor closes with virtual monotonic time', async t => {
    const f = await fixture(t), now = performance.now(), original = f.prototype.read;
    let virtual = now, advanced = false, expiredHandle = null, closed = false;
    t.mock.method(performance, 'now', () => virtual);
    t.mock.method(f.prototype, 'read', async function (...args) {
      const result = await original.apply(this, args);
      if (!advanced && result.bytesRead > 0) {
        advanced = true; expiredHandle = this; const close = this.close;
        t.mock.method(this, 'close', async function () { const result = await close.call(this); closed = true; return result; });
        virtual += LIMITS.wallMs + 1;
      }
      return result;
    });
    await refused(f.options(), 'restore_timeout'); assert.equal(advanced, true); assert.equal(closed, true); assert.equal(expiredHandle.fd, -1);
    t.mock.restoreAll(); await f.empty();
  });

  await t.test('does not return success before owned input close and normalizes a failed file close', async t => {
    const f = await fixture(t), entered = deferred(), release = deferred(), input = stream(f.frame), destroy = input._destroy;
    input._destroy = function (error, callback) {
      entered.resolve(); release.promise.then(() => destroy.call(this, error, callback));
    };
    let settled = false;
    const pending = extractOwnedBackup(f.options({ input })).then(result => { settled = true; return result; });
    try { await entered.promise; await new Promise(resolve => setImmediate(resolve)); assert.equal(settled, false); }
    finally { release.resolve(); }
    assert.equal((await pending).readbackVerified, true); assert.equal(input.closed, true);

    const failure = await fixture(t), write = failure.prototype.write; let victim;
    t.mock.method(failure.prototype, 'write', async function (...args) {
      if (!victim) {
        victim = this; const close = this.close;
        t.mock.method(this, 'close', async function () { await close.call(this); throw new Error('synthetic private close failure'); });
      }
      return write.apply(this, args);
    });
    await refused(failure.options(), 'restore_cleanup_pending'); assert.equal(victim.fd, -1);
    await failure.sentinelUnchanged(); t.mock.restoreAll();
  });

  await t.test('rejects truncated disconnected and hostile inputs with fixed diagnostics', async t => {
    const f = await fixture(t);
    await refused(f.options({ input: stream(f.frame.subarray(0, 3)) }), 'restore_archive_invalid'); await f.empty();
    const disconnected = new Readable({ highWaterMark: CHUNK, read() { this.destroy(new Error('synthetic private transport error')); } });
    await refused(f.options({ input: disconnected }), 'restore_io_failed'); assert.equal(disconnected.closed, true); await f.empty();
    const options = f.options(); Object.defineProperty(options, 'target', { get() { throw new Error('private target path'); } });
    await refused(options, 'restore_io_failed');
    let calls = 0; await refused({ ...f.options(), output() { calls++; } }, 'restore_archive_invalid'); assert.equal(calls, 0);
    await refused(f.options({ input: Readable.from(['text'], { objectMode: true }) }), 'restore_archive_invalid');
    await refused(f.options({ limits: { ...LIMITS, wallMs: Infinity } }), 'restore_limit_exceeded'); await f.empty();
  });

  await t.test('rejects emitClose false before adopting or reading the input', { timeout: 2000 }, async t => {
    const f = await fixture(t); let reads = 0, at = 0, closeEvents = 0;
    // Ordinary constructor option and default Node _destroy. No forged event,
    // stub bytes, destroy replacement or production clock/cleanup hook.
    const input = new Readable({ emitClose: false, highWaterMark: CHUNK, read() {
      reads++;
      if (at === f.frame.length) this.push(null);
      else { const end = Math.min(at + CHUNK, f.frame.length); this.push(f.frame.subarray(at, end)); at = end; }
    } });
    input.on('close', () => { closeEvents++; });
    try {
      await refused(f.options({ input }), 'restore_archive_invalid');
      assert.equal(reads, 0); assert.equal(input.destroyed, false); assert.equal(input.closed, false);
      await f.empty(); await f.sentinelUnchanged();
    } finally {
      // Admission refused ownership. Only the fixture owner now closes its
      // ordinary stream; closed becomes true although no close event occurs.
      input.destroy(); await new Promise(resolve => setImmediate(resolve));
    }
    assert.equal(input.closed, true); assert.equal(closeEvents, 0);
  });
});
