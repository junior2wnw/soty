// Synthetic fixture reused from existing restore-sender.test.mjs; no production key/data.
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

import { encryptBackup } from '../../backup.mjs';
import { verifyEncryptedBackup } from '../../verify-backup.mjs';
import { inspectRestorableBackup, sendAuthenticatedBackup } from '../../restore-backup.mjs';

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


export {fixture,sendAuthenticatedBackup};
