import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import { spawn, spawnSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import os from 'node:os';
import path from 'node:path';
import { encryptBackup } from './backup.mjs';
import { verifyEncryptedBackup } from './verify-backup.mjs';

const script = fileURLToPath(new URL('./verify-backup.mjs', import.meta.url));
const keys = generateKeyPairSync('rsa', { modulusLength: 3072 });
const privateKeyPem = keys.privateKey.export({ type: 'pkcs8', format: 'pem' });
const publicKey = keys.publicKey.export({ type: 'spki', format: 'pem' });
const metadata = { offline: true, dataFormat: 'tar', original: { State: { Running: false },
  Mounts: [{ Destination: '/data', Type: 'volume' }], Config: { Env: ['SECRET=synthetic-only'] } }, secrets: {} };
async function fixture(t, { emptySqlite = false } = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'connect-verify-'));
  t.after(async () => {
    assert.equal(path.dirname(root), path.resolve(os.tmpdir())); assert.match(path.basename(root), /^connect-verify-/);
    await rm(root, { recursive: true, force: true });
  });
  const source = path.join(root, 'source'); await mkdir(path.join(source, 'rooms'), { recursive: true });
  const db = new DatabaseSync(path.join(source, 'connector-store.sqlite'));
  db.exec('CREATE TABLE fixture(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO fixture(value) VALUES (\'synthetic\')'); db.close();
  if (emptySqlite) {
    // This is the real SQLite lifecycle found in the Linux canary volume:
    // opening and closing a database without writes leaves an empty file.
    const emptyFile = path.join(source, 'not-yet-written.sqlite');
    const empty = new DatabaseSync(emptyFile); empty.close();
    assert.equal((await readFile(emptyFile)).length, 0);
  }
  await writeFile(path.join(source, 'rooms', 'sample.json'), Buffer.alloc(180_000, 0x61));
  await writeFile(path.join(source, 'legacy-flat-room.json'), '{historical room bytes are preserved without JSON parsing');
  const archive = path.join(root, 'synthetic.tar');
  const packed = spawnSync('tar', ['-C', source, '-cf', archive, '.'], { windowsHide: true, stdio: 'pipe' });
  assert.equal(packed.status, 0, 'system tar must build the synthetic fixture');
  return { root, tar: await readFile(archive), async encrypt(tar, meta = metadata, name = 'archive') {
    const output = path.join(root, `${name}.enc`);
    await encryptBackup({ output, publicKey, metadata: meta, stream: Readable.from([tar]) }); return output;
  } };
}
async function verify(file, input = JSON.stringify({ file, privateKeyPem })) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [script], { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject); child.on('close', code => resolve({ code, stdout, stderr }));
    // A verifier rejecting an oversized input may close its pipe before the
    // parent finishes writing. The actual exit/output still decide the test.
    child.stdin.on('error', error => { if (error.code !== 'EPIPE') reject(new Error('verification_child_input_failed')); });
    child.stdin.end(input);
  });
}
function headerAt(tar, wanted) {
  for (let at = 0; at + 512 <= tar.length; ) {
    const h = tar.subarray(at, at + 512);
    if (h.every(byte => byte === 0)) return wanted === null ? at : -1;
    const name = h.subarray(0, 100).toString().replace(/\0.*$/s, '');
    if (name.endsWith(wanted)) return at;
    const size = parseInt(h.subarray(124, 136).toString().trim(), 8) || 0;
    at += 512 + Math.ceil(size / 512) * 512;
  }
  return -1;
}
function checksum(header) {
  header.fill(32, 148, 156); const sum = [...header].reduce((a, b) => a + b, 0);
  header.write(`${sum.toString(8).padStart(6, '0')}\0 `, 148);
}
function failed(result) {
  assert.equal(result.code, 1); assert.equal(result.stdout, '');
  assert.deepEqual(JSON.parse(result.stderr), { ok: false, code: 'backup_verification_failed' });
  assert.ok(!result.stderr.includes('synthetic-only')); assert.ok(!result.stderr.includes('PRIVATE KEY'));
}

test('streaming verification accepts a real tar with SQLite and reports only after authentication', async t => {
  const f = await fixture(t); const file = await f.encrypt(f.tar);
  const result = await verify(file); assert.equal(result.code, 0, result.stderr);
  const receipt = JSON.parse(result.stdout);
  assert.equal(receipt.authenticated, true); assert.equal(receipt.offline, true);
  assert.equal(receipt.sqliteFiles, 1); assert.equal(receipt.roomFiles, 2);
  assert.equal(receipt.sha256, createHash('sha256').update(await readFile(file)).digest('hex'));
  assert.ok(!result.stdout.includes('SECRET')); assert.ok(!result.stdout.includes(f.root));
});

test('a real unopened-for-writing SQLite file is preserved, while the required connector store cannot be empty', async t => {
  const f = await fixture(t, { emptySqlite: true });
  const result = await verify(await f.encrypt(f.tar));
  assert.equal(result.code, 0, result.stderr);
  const receipt = JSON.parse(result.stdout);
  assert.equal(receipt.sqliteFiles, 1); assert.equal(receipt.emptySqliteFiles, 1);
  assert.equal(receipt.authenticated, true); assert.equal(receipt.roomFiles, 2);

  const requiredEmpty = Buffer.from(f.tar);
  const fullAt = headerAt(requiredEmpty, 'connector-store.sqlite');
  const emptyAt = headerAt(requiredEmpty, 'not-yet-written.sqlite');
  assert.ok(fullAt >= 0 && emptyAt >= 0);
  for (const [at, name] of [[fullAt, './other.sqlite'], [emptyAt, './connector-store.sqlite']]) {
    const header = requiredEmpty.subarray(at, at + 512);
    header.fill(0, 0, 100); header.write(name); checksum(header);
  }
  failed(await verify(await f.encrypt(requiredEmpty, metadata, 'required-empty')));
});

test('tampered tag and truncated ciphertext never produce a success receipt', async t => {
  const f = await fixture(t); const file = await f.encrypt(f.tar); const bytes = await readFile(file);
  const tag = Buffer.from(bytes); tag[tag.length - 1] ^= 1;
  const altered = path.join(f.root, 'tag.enc'); await writeFile(altered, tag); failed(await verify(altered));
  const truncated = path.join(f.root, 'truncated.enc'); await writeFile(truncated, bytes.subarray(0, -40)); failed(await verify(truncated));
});

test('authenticated archives still require valid tar checksums, padding, endings and safe paths', async t => {
  const f = await fixture(t);
  const badChecksum = Buffer.from(f.tar); badChecksum[0] ^= 1;
  const unsafe = Buffer.from(f.tar); unsafe.fill(0, 0, 100); unsafe.write('../escape'); checksum(unsafe.subarray(0, 512));
  const end = headerAt(f.tar, null); assert.ok(end > 0);
  const oneEnd = Buffer.concat([f.tar.subarray(0, end), Buffer.alloc(512)]);
  const noEnd = f.tar.subarray(0, end - 1);
  const nonzeroTail = Buffer.concat([f.tar, Buffer.alloc(512, 1)]);
  let i = 0;
  for (const tar of [badChecksum, unsafe, oneEnd, noEnd, nonzeroTail]) failed(await verify(await f.encrypt(tar, metadata, `bad-tar-${i++}`)));
});

test('a renamed database or SQLite-looking filename without a database header is rejected', async t => {
  const f = await fixture(t); const offset = headerAt(f.tar, 'connector-store.sqlite'); assert.ok(offset >= 0);
  const renamed = Buffer.from(f.tar); const header = renamed.subarray(offset, offset + 512);
  header.fill(0, 0, 100); header.write('./other.sqlite'); checksum(header);
  failed(await verify(await f.encrypt(renamed, metadata, 'missing-required')));
  const badDatabase = Buffer.from(f.tar); badDatabase.fill(0, offset + 512, offset + 528);
  failed(await verify(await f.encrypt(badDatabase, metadata, 'not-sqlite')));
});

test('metadata and header bounds are checked before allocating archive-sized buffers', async t => {
  const f = await fixture(t);
  failed(await verify(await f.encrypt(f.tar, { ...metadata, offline: false }, 'online')));
  failed(await verify(await f.encrypt(f.tar, { ...metadata, excessive: 'x'.repeat(4 * 1024 * 1024) }, 'metadata-size')));
  const file = await f.encrypt(f.tar, metadata, 'header-size'); const bytes = await readFile(file);
  bytes.writeUInt32BE(0xffffffff, 8); await writeFile(file, bytes); failed(await verify(file));
});

test('import with hostile stdin neither reads the stream nor writes a CLI response', async () => {
  const moduleUrl = new URL('./verify-backup.mjs', import.meta.url).href;
  const program = `
    const stdin = process.stdin;
    const originalRead = stdin.read, originalIterator = stdin[Symbol.asyncIterator];
    let readCalls = 0, iteratorCalls = 0;
    stdin.read = function () { readCalls++; throw new Error('synthetic_hostile_stdin_read'); };
    stdin[Symbol.asyncIterator] = function () { iteratorCalls++; throw new Error('synthetic_hostile_stdin_iterator'); };
    const imported = await import(${JSON.stringify(moduleUrl)});
    const result = { readCalls, iteratorCalls, hasApi: typeof imported.verifyEncryptedBackup === 'function' };
    stdin.read = originalRead; stdin[Symbol.asyncIterator] = originalIterator;
    stdin.destroy();
    process.send(result, () => process.disconnect());
  `;
  const result = await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '--eval', program], {
      windowsHide: true, timeout: 10_000, stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    });
    let stdout = '', stderr = '', report = null, childError = false;
    child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('message', value => { report = value; });
    child.on('error', () => { childError = true; });
    child.on('close', code => resolve({ code, stdout, stderr, report, childError }));
    child.stdin.on('error', error => { if (error.code !== 'EPIPE') reject(new Error('import_probe_input_failed')); });
    child.stdin.end('{synthetic_hostile_stdin_payload');
  });
  assert.equal(result.childError, false); assert.equal(result.code, 0);
  assert.ok(result.stdout.length === 0, 'import must not print stdout');
  assert.ok(result.stderr.length === 0, 'import must not print stderr');
  assert.deepEqual(result.report, { readCalls: 0, iteratorCalls: 0, hasApi: true });
});

test('actual API and CLI return the same safe receipt for an encrypted SQLite tar fixture', async t => {
  const f = await fixture(t, { emptySqlite: true }); const file = await f.encrypt(f.tar);
  const receipt = await verifyEncryptedBackup({ file, privateKeyPem });
  const cli = await verify(file);
  assert.equal(cli.code, 0); assert.ok(cli.stderr.length === 0, 'successful CLI must have no stderr');
  let cliReceipt;
  try { cliReceipt = JSON.parse(cli.stdout); } catch { assert.fail('CLI receipt must be JSON'); }
  const fields = ['ok', 'authenticated', 'offline', 'archiveEntries', 'roomFiles', 'sqliteFiles', 'emptySqliteFiles', 'sha256'].sort();
  assert.deepEqual(Object.keys(receipt).sort(), fields); assert.deepEqual(Object.keys(cliReceipt).sort(), fields);
  assert.deepEqual(receipt, cliReceipt);
  assert.equal(receipt.ok, true); assert.equal(receipt.authenticated, true); assert.equal(receipt.offline, true);
  assert.equal(receipt.roomFiles, 2); assert.equal(receipt.sqliteFiles, 1); assert.equal(receipt.emptySqliteFiles, 1);
  assert.equal(receipt.sha256, createHash('sha256').update(await readFile(file)).digest('hex'));
});

test('API failures expose only the fixed error for wrong keys, damaged archives and throwing options', async t => {
  const f = await fixture(t); const file = await f.encrypt(f.tar); const bytes = await readFile(file);
  const badTag = Buffer.from(bytes); badTag[badTag.length - 1] ^= 1;
  const tagged = path.join(f.root, 'synthetic-secret-tag.enc'); await writeFile(tagged, badTag);
  const truncated = path.join(f.root, 'synthetic-secret-truncated.enc'); await writeFile(truncated, bytes.subarray(0, -40));
  const badTar = Buffer.from(f.tar); badTar[0] ^= 1;
  const corrupt = await f.encrypt(badTar, metadata, 'synthetic-secret-corrupt-tar');
  const wrongKeys = generateKeyPairSync('rsa', { modulusLength: 3072 });
  const wrongPem = wrongKeys.privateKey.export({ type: 'pkcs8', format: 'pem' });
  const throwing = { get file() { throw new Error('synthetic-secret C:/synthetic/private/archive.enc', { cause: 'synthetic-private-cause' }); } };
  const cases = [
    { file, privateKeyPem: wrongPem },
    { file, privateKeyPem: 'synthetic-invalid-private-key' },
    { file: tagged, privateKeyPem }, { file: truncated, privateKeyPem }, { file: corrupt, privateKeyPem },
    null, undefined, throwing,
  ];
  for (const options of cases) {
    let failure;
    try { await verifyEncryptedBackup(options); } catch (error) { failure = error; }
    assert.ok(failure instanceof Error, 'API must reject with its safe error');
    // Boolean assertions avoid echoing any regressed error/secret into a test log.
    assert.ok(failure.message === 'backup_verification_failed', 'API message must be fixed');
    assert.ok(failure.code === 'backup_verification_failed', 'API code must be fixed');
    assert.ok(failure.stack === 'Error: backup_verification_failed', 'API stack must contain no path');
    assert.equal(Object.hasOwn(failure, 'cause'), false);
    assert.deepEqual(Object.getOwnPropertyNames(failure).sort(), ['code', 'message', 'stack']);
    assert.ok(JSON.stringify(failure) === '{"code":"backup_verification_failed"}', 'API JSON must contain only the safe code');
  }
});

test('CLI preserves its exact 64 KiB stdin limit while the API takes an object', async t => {
  const f = await fixture(t); const file = await f.encrypt(f.tar);
  const encoded = Buffer.from(JSON.stringify({ file, privateKeyPem }));
  assert.ok(encoded.length < 64 * 1024);
  const atLimit = Buffer.concat([encoded, Buffer.alloc(64 * 1024 - encoded.length, 32)]);
  const accepted = await verify(file, atLimit);
  assert.equal(accepted.code, 0); assert.ok(accepted.stderr.length === 0, 'limit input must verify');
  const expected = await verifyEncryptedBackup({ file, privateKeyPem });
  let actual;
  try { actual = JSON.parse(accepted.stdout); } catch { assert.fail('limit receipt must be JSON'); }
  assert.deepEqual(actual, expected);
  failed(await verify(file, Buffer.concat([atLimit, Buffer.from(' ')])));
});
