import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync } from 'node:crypto';
import { mkdtemp, mkdir, open, readFile, writeFile, readdir, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import os from 'node:os';
import path from 'node:path';
import { encryptBackup } from './backup.mjs';
import { verifyEncryptedBackup } from './verify-backup.mjs';
import { inspectRestorableBackup } from './restore-backup.mjs';

// Synthetic keys only. Fixture tar is created by the installed system tar;
// explicit fixture UID/mode edits do not claim Windows/Linux ownership proof.
const keys = generateKeyPairSync('rsa', { modulusLength: 3072 });
const privateKeyPem = keys.privateKey.export({ type: 'pkcs8', format: 'pem' });
const publicKey = keys.publicKey.export({ type: 'spki', format: 'pem' });
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const GEN = '1234567890abcdef1234567890abcdef';
const CHECKPOINT = sha('independent synthetic cold-source checkpoint');
const LIMITS = Object.freeze({ archiveBytes: 16 * 1024 * 1024, plaintextBytes: 16 * 1024 * 1024,
  fileBytes: 8 * 1024 * 1024, extractedBytes: 16 * 1024 * 1024, entries: 256, headers: 512,
  pathBytes: 64 * 1024, pathDepth: 16, externalFiles: 8, externalBytes: 256 * 1024, wallMs: 30_000, idleMs: 2_000 });
const ERROR_CODES = new Set(['restore_authentication_failed', 'restore_incomplete', 'restore_archive_invalid',
  'restore_limit_exceeded', 'restore_timeout', 'restore_io_failed']);
const SAFE_FIELDS = ['ok', 'authenticated', 'offline', 'archiveEntries', 'roomFiles', 'sqliteFiles', 'emptySqliteFiles',
  'externalFiles', 'verifiedFileBytes', 'sha256', 'strictProfile', 'inventoryMatched'].sort();
const text = bytes => bytes.toString('utf8').split('\0')[0];
const octal = bytes => parseInt(text(bytes).trim(), 8) || 0;
function field(header, offset, size, value) {
  header.fill(0, offset, offset + size);
  header.write(value.toString(8).padStart(size - 1, '0'), offset, size - 1, 'ascii');
}
function checksum(header) {
  header.fill(32, 148, 156);
  field(header, 148, 7, header.reduce((sum, byte) => sum + byte, 0)); header[155] = 32;
}
function records(tar) {
  const result = [];
  for (let at = 0; at + 512 <= tar.length;) {
    const header = tar.subarray(at, at + 512);
    if (header.every(byte => byte === 0)) break;
    const size = octal(header.subarray(124, 136)), end = at + 512 + Math.ceil(size / 512) * 512;
    assert.ok(end <= tar.length, 'fixture tar must have complete records');
    result.push({ name: text(header.subarray(0, 100)).replace(/^\.\//, '').replace(/\/$/, ''),
      header: Buffer.from(header), body: Buffer.from(tar.subarray(at + 512, at + 512 + size)) });
    at = end;
  }
  return result;
}
function assemble(items, endBlocks = 2) {
  return Buffer.concat([...items.flatMap(({ header, body }) => [header, body, Buffer.alloc((512 - body.length % 512) % 512)]),
    Buffer.alloc(512 * endBlocks)]);
}
function newRecord(name, body = Buffer.alloc(0), type = '0', link = '') {
  const header = Buffer.alloc(512);
  assert.ok(Buffer.byteLength(name) <= 100, 'fixture short name bound');
  header.write(name); field(header, 100, 8, type === '5' ? 0o700 : 0o600);
  field(header, 108, 8, 10001); field(header, 116, 8, 10002); field(header, 124, 12, body.length);
  field(header, 136, 12, 1); header[156] = type.charCodeAt(0); header.write(link, 157, 100);
  header.write('ustar\0', 257); header.write('00', 263); checksum(header);
  return { name, header, body };
}
function paxRecord(key, value) {
  const tail = Buffer.from(` ${key}=${value}\n`); let length = tail.length + 1;
  while (String(length).length + tail.length !== length) length = String(length).length + tail.length;
  return newRecord('PaxHeader', Buffer.concat([Buffer.from(String(length)), tail]), 'x');
}
function rename(item, name) {
  const copy = { ...item, header: Buffer.from(item.header), body: Buffer.from(item.body) };
  copy.header.fill(0, 0, 100); copy.header.write(name); checksum(copy.header); return copy;
}
function fileDescriptor(name, body = null) {
  return { path: name, type: body === null ? 'directory' : 'file', size: body?.length ?? 0,
    sha256: body === null ? null : sha(body), uid: 10001, gid: 10002, mode: body === null ? 0o700 : 0o600 };
}
function sortedInventory(inventory) {
  // Independent fixture projection, with literal key order. No reader helper is
  // imported to build the trusted pin or expected logical source inventory.
  const order = (a, b) => a < b ? -1 : a > b ? 1 : 0;
  return {
    files: inventory.files.map(f => ({ path: f.path, type: f.type, size: f.size, sha256: f.sha256, uid: f.uid, gid: f.gid, mode: f.mode }))
      .sort((a, b) => order(a.path, b.path)),
    stores: inventory.stores.map(s => ({ id: s.id, required: s.required, present: s.present, format: s.format,
      identitySha256: s.identitySha256, paths: [...s.paths].sort(order) })).sort((a, b) => order(a.id, b.id)),
    external: inventory.external.map(f => ({ id: f.id, required: f.required, present: f.present, size: f.size,
      sha256: f.sha256, uid: f.uid, gid: f.gid, mode: f.mode })).sort((a, b) => order(a.id, b.id)),
  };
}
function manifestFor(inventory) {
  return { version: 1, generationId: GEN, checkpointSha256: CHECKPOINT, inventory: sortedInventory(inventory) };
}
function witnessFor(inventory) {
  return { generationId: GEN, checkpointSha256: CHECKPOINT, inventorySha256: sha(JSON.stringify(sortedInventory(inventory))) };
}
async function fixture(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'connect-restore-dry-'));
  t.after(async () => {
    assert.equal(path.dirname(root), path.resolve(os.tmpdir())); assert.match(path.basename(root), /^connect-restore-dry-/);
    await rm(root, { recursive: true, force: true });
  });
  const source = path.join(root, 'source'); await mkdir(path.join(source, 'rooms'), { recursive: true });
  const bodies = new Map();
  for (const name of ['connector-store.sqlite', 'notes.sqlite']) {
    const db = new DatabaseSync(path.join(source, name));
    db.exec("CREATE TABLE fixture(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO fixture VALUES(1,'synthetic-only')"); db.close();
    bodies.set(name, await readFile(path.join(source, name)));
  }
  bodies.set('rooms/alpha.json', Buffer.alloc(180_003, 97));
  // A WAL-shaped durable sidecar is inventoried as bytes; R1a does not claim it
  // is a valid SQLite transaction, checkpoint or domain proof.
  bodies.set('notes.sqlite-wal', Buffer.from('synthetic durable sidecar'));
  bodies.set('empty.txt', Buffer.alloc(0));
  for (const [name, body] of bodies) if (!name.endsWith('.sqlite')) await writeFile(path.join(source, name), body);
  const packed = spawnSync('tar', ['--format=ustar', '-C', source, '-cf', '-', '.'], {
    windowsHide: true, stdio: 'pipe', timeout: 10_000, maxBuffer: 8 * 1024 * 1024,
  });
  assert.equal(packed.status, 0, 'system tar must create the bounded fixture in memory');
  let items = records(packed.stdout);
  // Pin synthetic uid/gid/mode independently of host tar's Windows mapping.
  items = items.map(item => {
    const header = Buffer.from(item.header);
    field(header, 100, 8, header[156] === 53 ? 0o700 : 0o600);
    field(header, 108, 8, 10001); field(header, 116, 8, 10002); checksum(header);
    return { ...item, header };
  });
  const externalBody = Buffer.from('synthetic restore configuration; never printed');
  const inventory = {
    files: [fileDescriptor(''), fileDescriptor('rooms'), ...[...bodies].map(([name, body]) => fileDescriptor(name, body))],
    stores: [
      { id: 'connect', required: true, present: true, format: 'soty.connector.fixture.v1', identitySha256: sha('connect-fixture'), paths: ['connector-store.sqlite'] },
      { id: 'notes', required: true, present: true, format: 'soty.notes.fixture.v2', identitySha256: sha('notes-fixture'), paths: ['notes.sqlite', 'notes.sqlite-wal'] },
      { id: 'optional-apps', required: false, present: false, format: null, identitySha256: null, paths: [] },
    ],
    external: [
      { id: 'runtime-config', required: true, present: true, size: externalBody.length, sha256: sha(externalBody), uid: 10003, gid: 10004, mode: 0o600 },
      { id: 'optional-archive-key', required: false, present: false, size: 0, sha256: null, uid: 0, gid: 0, mode: 0 },
    ],
  };
  // Captured before the producer/manifest builder is called; omission tests keep
  // this original witness while replacing both producer manifest and tar.
  const sourceWitness = Object.freeze(witnessFor(inventory));
  const metadata = { offline: true, dataFormat: 'tar', original: { State: { Running: false },
    Mounts: [{ Destination: '/data', Type: 'volume' }], Config: { Env: ['SYNTHETIC_ONLY=not-a-secret'] } },
  secrets: {}, restoreManifest: manifestFor(inventory), restoreFiles: { 'runtime-config': externalBody.toString('base64') } };
  let sequence = 0;
  return { root, items, bodies, inventory, sourceWitness, metadata, tar: assemble(items), async encrypted(tar = assemble(items), meta = metadata) {
    const file = path.join(root, `fixture-${++sequence}.enc`);
    await encryptBackup({ output: file, publicKey, metadata: meta, stream: Readable.from([tar]) });
    const bytes = await readFile(file);
    return { file, privateKeyPem, expectedSha256: sha(bytes), expectedManifestSha256: sha(JSON.stringify(meta.restoreManifest ?? 'missing full manifest')),
      sourceWitness, limits: { ...LIMITS } };
  } };
}
async function refused(options, expected) {
  let error;
  try { await inspectRestorableBackup(options); } catch (caught) { error = caught; }
  assert.ok(error instanceof Error, 'dry inspection must refuse');
  assert.ok(ERROR_CODES.has(error.code), 'refusal must use a fixed code');
  if (expected) assert.ok(error.code === expected, 'refusal stage must match the causal input');
  assert.ok(error.message === error.code && error.stack === `Error: ${error.code}`, 'refusal must contain no caller data');
  assert.equal(Object.hasOwn(error, 'cause'), false);
  assert.deepEqual(Object.getOwnPropertyNames(error).sort(), ['code', 'message', 'stack']);
}

test('strict dry inspection accepts an encrypted system tar and returns only safe counts after unchanged R0 checks', async t => {
  const f = await fixture(t), options = await f.encrypted();
  const beforeEntries = await readdir(f.root), beforeArchive = sha(await readFile(options.file));
  const strict = await inspectRestorableBackup(options), old = await verifyEncryptedBackup(options);
  assert.deepEqual(Object.keys(strict).sort(), SAFE_FIELDS);
  assert.equal(strict.authenticated, true); assert.equal(strict.inventoryMatched, true);
  assert.equal(strict.strictProfile, 'soty.restore-manifest.v1'); assert.equal(strict.externalFiles, 1);
  assert.equal(strict.sqliteFiles, 2); assert.equal(strict.archiveEntries, f.inventory.files.length);
  assert.equal(strict.verifiedFileBytes, f.inventory.files.reduce((sum, file) => sum + file.size, 0) + f.inventory.external[0].size);
  assert.ok(Object.entries(old).every(([key, value]) => strict[key] === value), 'shared reader must preserve every legacy receipt field');
  assert.ok(strict.sha256 === options.expectedSha256, 'whole ciphertext pin must match');
  assert.ok(sha(await readFile(options.file)) === beforeArchive, 'inspection must not rewrite its archive');
  assert.ok(JSON.stringify(await readdir(f.root)) === JSON.stringify(beforeEntries), 'inspection must create no output file');
});

test('legacy missing-manifest and authentic link archives remain R0 compatible but fail strict dry admission', async t => {
  const f = await fixture(t);
  const legacy = { ...f.metadata }; delete legacy.restoreManifest; delete legacy.restoreFiles;
  const noManifest = await f.encrypted(f.tar, legacy);
  noManifest.expectedManifestSha256 = sha('a separately required manifest');
  assert.equal((await verifyEncryptedBackup(noManifest)).authenticated, true);
  await refused(noManifest, 'restore_incomplete');
  const link = newRecord('./alias', Buffer.alloc(0), '2', './empty.txt');
  const options = await f.encrypted(assemble([...f.items, link]));
  assert.equal((await verifyEncryptedBackup(options)).authenticated, true);
  await refused(options, 'restore_archive_invalid');
});

test('archive pin, manifest pin and each independent source-witness field are binding', async t => {
  const f = await fixture(t), options = await f.encrypted();
  await refused({ ...options, expectedSha256: '0'.repeat(64) }, 'restore_authentication_failed');
  await refused({ ...options, expectedManifestSha256: '0'.repeat(64) }, 'restore_incomplete');
  for (const [key, value] of [['generationId', '0'.repeat(32)], ['checkpointSha256', '0'.repeat(64)], ['inventorySha256', '0'.repeat(64)]])
    await refused({ ...options, sourceWitness: { ...options.sourceWitness, [key]: value } }, 'restore_incomplete');
});

test('joint database omission from newly authenticated tar and manifest cannot erase the independent cold-source witness', async t => {
  const f = await fixture(t);
  const inventory = structuredClone(f.inventory);
  inventory.files = inventory.files.filter(file => !['notes.sqlite', 'notes.sqlite-wal'].includes(file.path));
  inventory.stores = inventory.stores.filter(store => store.id !== 'notes');
  const metadata = { ...f.metadata, restoreManifest: manifestFor(inventory) };
  const options = await f.encrypted(assemble(f.items.filter(item => !['notes.sqlite', 'notes.sqlite-wal'].includes(item.name))), metadata);
  assert.equal((await verifyEncryptedBackup(options)).authenticated, true);
  // Valid new producer/archive pins, old independently recorded source witness.
  await refused(options, 'restore_incomplete');
});

test('a same-length payload change under valid GCM fails its declared file hash', async t => {
  const f = await fixture(t);
  const items = f.items.map(item => ({ ...item, body: Buffer.from(item.body) }));
  items.find(item => item.name === 'rooms/alpha.json').body[27] ^= 1;
  const options = await f.encrypted(assemble(items));
  assert.equal((await verifyEncryptedBackup(options)).authenticated, true);
  await refused(options, 'restore_incomplete');
});

test('exact inventory rejects missing or extra entries, ownership drift, external omission and changed external bytes', async t => {
  const f = await fixture(t);
  const changed = f.items.map(item => ({ ...item, header: Buffer.from(item.header) }));
  const header = changed.find(item => item.name === 'empty.txt').header;
  field(header, 108, 8, 10009); checksum(header);
  for (const tar of [assemble(f.items.filter(item => item.name !== 'empty.txt')),
    assemble([...f.items, newRecord('./extra.txt', Buffer.from('extra'))]), assemble(changed)])
    await refused(await f.encrypted(tar), 'restore_incomplete');
  await refused(await f.encrypted(f.tar, { ...f.metadata, restoreFiles: {} }), 'restore_incomplete');
  const changedExternal = Buffer.from(f.metadata.restoreFiles['runtime-config'], 'base64'); changedExternal[0] ^= 1;
  await refused(await f.encrypted(f.tar, { ...f.metadata, restoreFiles: { 'runtime-config': changedExternal.toString('base64') } }), 'restore_incomplete');
});

test('effective aliases, PAX duplicates, traversal, links, special types and malformed UTF-8 are refused before a dry receipt', async t => {
  const f = await fixture(t), base = f.items.find(item => item.name === 'empty.txt');
  const malformed = rename(base, './invalid'); malformed.header[2] = 0xff; checksum(malformed.header);
  const cases = [
    [...f.items, rename(base, '././empty.txt')],
    [...f.items, paxRecord('path', './empty.txt'), newRecord('./another')],
    [...f.items, newRecord('../escape')],
    [...f.items, newRecord('/absolute')],
    [...f.items, newRecord('./bad\u0001name')],
    [...f.items, newRecord('./hard', Buffer.alloc(0), '1', './empty.txt')],
    [...f.items, newRecord('./contiguous', Buffer.alloc(0), '7')],
    [...f.items, newRecord('./fifo', Buffer.alloc(0), '6')],
    [...f.items, malformed],
  ];
  for (const items of cases) await refused(await f.encrypted(assemble(items)), 'restore_archive_invalid');
});

test('UTF-8 path component bound is bytes, with a valid 255-byte edge and a shorter-in-characters 256-byte refusal', async t => {
  const f = await fixture(t);
  for (const [name, accepted] of [['é'.repeat(127) + 'x', true], ['é'.repeat(128), false]]) {
    const inventory = structuredClone(f.inventory); inventory.files.push(fileDescriptor(name, Buffer.alloc(0)));
    const metadata = { ...f.metadata, restoreManifest: manifestFor(inventory) };
    const options = await f.encrypted(assemble([...f.items, paxRecord('path', `./${name}`), newRecord('./long-name')]), metadata);
    options.sourceWitness = witnessFor(inventory);
    if (accepted) assert.equal((await inspectRestorableBackup(options)).inventoryMatched, true);
    else await refused(options, 'restore_limit_exceeded');
  }
});

test('finite byte and count limits reject exact limit plus one without silently increasing the profile', async t => {
  const f = await fixture(t), options = await f.encrypted();
  const plaintextBytes = 4 + Buffer.byteLength(JSON.stringify(f.metadata)) + f.tar.length;
  const archivedBytes = (await readFile(options.file)).length;
  const exact = await inspectRestorableBackup({ ...options, limits: { ...LIMITS, plaintextBytes, archiveBytes: archivedBytes } });
  assert.equal(exact.authenticated, true);
  const pathBytes = f.inventory.files.reduce((sum, file) => sum + Buffer.byteLength(file.path), 0);
  for (const limits of [
    { plaintextBytes: plaintextBytes - 1 }, { archiveBytes: archivedBytes - 1 }, { fileBytes: 180_002 },
    { extractedBytes: exact.verifiedFileBytes - 1 }, { entries: f.inventory.files.length - 1 },
    { headers: records(f.tar).length + 1 }, { pathBytes: pathBytes - 1 }, { pathDepth: 1 },
    { externalFiles: 1 }, { externalBytes: f.inventory.external[0].size - 1 },
  ]) await refused({ ...options, limits: { ...LIMITS, ...limits } }, 'restore_limit_exceeded');
});

test('new dry API still requires final GCM authentication, valid tar ending and a usable private key', async t => {
  const f = await fixture(t), options = await f.encrypted(), original = await readFile(options.file);
  const tag = Buffer.from(original); tag[tag.length - 1] ^= 1;
  const badTag = path.join(f.root, 'bad-tag.enc'); await writeFile(badTag, tag);
  const truncated = path.join(f.root, 'truncated.enc'); await writeFile(truncated, original.subarray(0, -40));
  await refused({ ...options, file: badTag, expectedSha256: sha(tag) }, 'restore_authentication_failed');
  await refused({ ...options, file: truncated, expectedSha256: sha(original.subarray(0, -40)) });
  await refused({ ...options, privateKeyPem: 'synthetic invalid private key' }, 'restore_authentication_failed');
  const wrong = generateKeyPairSync('rsa', { modulusLength: 3072 }).privateKey.export({ type: 'pkcs8', format: 'pem' });
  await refused({ ...options, privateKeyPem: wrong }, 'restore_authentication_failed');
  await refused(await f.encrypted(assemble(f.items, 1)), 'restore_archive_invalid');
});

test('hostile options, unknown output, limits and abort reasons never escape the fixed dry error boundary', async t => {
  const f = await fixture(t), options = await f.encrypted();
  let outputReads = 0, reasonReads = 0;
  const unknownOutput = { ...options, get output() { outputReads++; throw new Error('synthetic private output canary'); } };
  const throwing = { ...options, get file() { throw new Error('synthetic C:/private/canary', { cause: 'private synthetic cause' }); } };
  const controller = new AbortController(); controller.abort('synthetic private abort reason');
  Object.defineProperty(controller.signal, 'reason', { get() { reasonReads++; throw new Error('synthetic reason getter'); } });
  const hostileSignal = { get aborted() { throw new Error('synthetic signal getter'); } };
  for (const value of [null, undefined, throwing, unknownOutput, { ...options, signal: hostileSignal },
    { ...options, signal: controller.signal }, { ...options, privateKeyPem: 'x'.repeat(64 * 1024 + 1) }]) await refused(value);
  for (const value of [0, -1, Infinity, Number.MAX_SAFE_INTEGER + 1, '1024', NaN])
    await refused({ ...options, limits: { ...LIMITS, fileBytes: value } }, 'restore_limit_exceeded');
  await refused({ ...options, limits: { ...LIMITS, surprising: 1 } }, 'restore_archive_invalid');
  assert.equal(outputReads, 0); assert.equal(reasonReads, 0);
});

test('trusted limits and witness are captured before the first asynchronous archive read', async t => {
  const f = await fixture(t), options = await f.encrypted();
  options.sourceWitness = { ...options.sourceWitness };
  const pending = inspectRestorableBackup(options);
  options.limits.fileBytes = 1; options.sourceWitness.inventorySha256 = '0'.repeat(64);
  options.expectedSha256 = '0'.repeat(64);
  assert.equal((await pending).inventoryMatched, true);
});

test('cooperative restore deadlines use real file reads and await actual descriptor close', { concurrency: false }, async t => {
  const f = await fixture(t), options = await f.encrypted();
  const probe = await open(options.file, 'r');
  let prototype;
  try { prototype = Object.getPrototypeOf(probe); } finally { await probe.close(); }
  const originalRead = prototype.read;
  assert.equal(typeof originalRead, 'function');
  // No ESM export replacement, synthetic read result or production clock input.
  // Each case moves only the monotonic clock after a real successful read.
  const cases = [
    { name: 'sufficient limits accept the same real encrypted fixture', wallMs: 1000, idleMs: 100, stepMs: 1, outcome: 'success' },
    { name: 'wall deadline expires despite regular read progress', wallMs: 3, idleMs: 100, stepMs: 1, outcome: 'wall' },
    { name: 'idle deadline expires at the completed-read safe point', wallMs: 1000, idleMs: 2, stepMs: 3, outcome: 'idle' },
  ];
  for (const plan of cases) await t.test(plan.name, { concurrency: false }, async phase => {
    let virtualMs = 0, successfulReads = 0, largestReadGap = 0;
    const observed = new Set(), completedClose = new Set();
    try {
      phase.mock.method(performance, 'now', () => virtualMs);
      phase.mock.method(prototype, 'read', async function (...args) {
        if (!observed.has(this)) {
          observed.add(this);
          // close can be an instance method in Node. Wrap the exact handle's
          // real method, record completion only after its Promise fulfills.
          const originalClose = this.close;
          phase.mock.method(this, 'close', async function (...closeArgs) {
            const result = await Reflect.apply(originalClose, this, closeArgs);
            completedClose.add(this); return result;
          });
        }
        const result = await Reflect.apply(originalRead, this, args);
        if (result.bytesRead > 0) {
          virtualMs += plan.stepMs; successfulReads++;
          largestReadGap = Math.max(largestReadGap, plan.stepMs);
        }
        return result;
      });
      const input = { ...options, limits: { ...options.limits, wallMs: plan.wallMs, idleMs: plan.idleMs } };
      if (plan.outcome === 'success') assert.equal((await inspectRestorableBackup(input)).inventoryMatched, true);
      else await refused(input, 'restore_timeout');
    } finally { phase.mock.restoreAll(); }
    assert.equal(observed.size, 1, 'inspection must use one actual archive descriptor');
    assert.equal(completedClose.size, 1, 'the original close Promise must have fulfilled before API settlement');
    assert.ok([...observed].every(handle => completedClose.has(handle) && handle.fd === -1), 'the actual observed descriptor must be closed');
    assert.ok(successfulReads > 0, 'a timeout must occur after real archive I/O, not input validation');
    if (plan.outcome === 'wall') {
      assert.ok(virtualMs > plan.wallMs && largestReadGap < plan.idleMs, 'only the absolute wall deadline must expire');
      assert.ok(successfulReads > 1, 'regular progress must not renew the wall deadline');
    } else if (plan.outcome === 'idle') {
      assert.ok(largestReadGap > plan.idleMs && virtualMs < plan.wallMs, 'only the completed-read idle deadline must expire');
    } else assert.ok(virtualMs < plan.wallMs && largestReadGap < plan.idleMs, 'control must remain within both deadlines');
  });
});
