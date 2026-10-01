import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readFile, lstat, link, symlink, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { DatabaseSync } from 'node:sqlite';
import path from 'node:path';
import os from 'node:os';
import { captureColdInventory } from './cold-inventory.mjs';
import { encryptBackup, validateColdSourceMounts } from './backup.mjs';
import { inspectRestorableBackup } from './restore-backup.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const GEN = '1234567890abcdef1234567890abcdef', CHECKPOINT = hash('independent cold checkpoint');
const LIMITS = { archiveBytes: 4 * 1024 * 1024, plaintextBytes: 4 * 1024 * 1024,
  fileBytes: 1024 * 1024, extractedBytes: 2 * 1024 * 1024, entries: 64, headers: 128,
  pathBytes: 16384, pathDepth: 16, externalFiles: 4, externalBytes: 65536, wallMs: 30000, idleMs: 5000 };
const SAFE_CODES = ['backup_cold_invalid', 'backup_cold_changed', 'backup_cold_limit_exceeded', 'backup_cold_io_failed'];
function fixed(error) {
  return SAFE_CODES.includes(error?.code) && error.message === error.code && error.stack === `Error: ${error.code}`
    && !Object.hasOwn(error, 'cause');
}
async function fixture(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-cold-inventory-'));
  t.after(async () => {
    assert.equal(path.dirname(root), path.resolve(os.tmpdir()));
    assert.match(path.basename(root), /^soty-cold-inventory-/);
    await rm(root, { recursive: true, force: true });
  });
  const dataRoot = path.join(root, 'data'); await mkdir(dataRoot);
  for (const name of ['connector-store.sqlite', 'notes.sqlite']) {
    const db = new DatabaseSync(path.join(dataRoot, name));
    db.exec("CREATE TABLE fixture(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO fixture VALUES(1,'synthetic-only')"); db.close();
  }
  await mkdir(path.join(dataRoot, 'nested'));
  await writeFile(path.join(dataRoot, 'nested', 'text.txt'), 'one synthetic body');
  await writeFile(path.join(dataRoot, 'notes.sqlite-wal'), 'synthetic WAL-shaped bytes');
  const config = path.join(root, 'private-config.bin'), secret = Buffer.from('synthetic-private-configuration-canary');
  await writeFile(config, secret);
  const stores = [{ id: 'connect', required: true, present: true, format: 'fixture.sqlite.v1',
    identitySha256: hash('independent connector identity'), paths: ['connector-store.sqlite'] },
  { id: 'notes', required: true, present: true, format: 'fixture.sqlite.v1',
    identitySha256: hash('independent notes identity'), paths: ['notes.sqlite', 'notes.sqlite-wal'] }];
  const externalFiles = [{ id: 'runtime-config', required: true, path: config }, { id: 'optional-keys', required: false, path: null }];
  return { root, dataRoot, config, secret, options: { dataRoot, generationId: GEN, checkpointSha256: CHECKPOINT,
    stores, externalFiles, limits: { ...LIMITS } } };
}
async function independentInventory(f) {
  // Literal fixture membership and source reads, independent of the producer's
  // traversal, sorting, manifest builder, and returned sourceWitness.
  const files = [];
  for (const name of ['', 'connector-store.sqlite', 'nested', 'nested/text.txt', 'notes.sqlite', 'notes.sqlite-wal']) {
    const target = name ? path.join(f.dataRoot, ...name.split('/')) : f.dataRoot;
    const stat = await lstat(target), directory = stat.isDirectory();
    files.push({ path: name, type: directory ? 'directory' : 'file', size: directory ? 0 : stat.size,
      sha256: directory ? null : hash(await readFile(target)), uid: stat.uid, gid: stat.gid, mode: stat.mode & 0o7777 });
  }
  const configStat = await lstat(f.config);
  return { files, stores: structuredClone(f.options.stores), external: [
    { id: 'optional-keys', required: false, present: false, size: 0, sha256: null, uid: 0, gid: 0, mode: 0 },
    { id: 'runtime-config', required: true, present: true, size: f.secret.length, sha256: hash(f.secret),
      uid: configStat.uid, gid: configStat.gid, mode: configStat.mode & 0o7777 },
  ] };
}
function patchTarMetadata(tar, files) {
  // Windows bsdtar maps owner/modes differently from NTFS stat. These synthetic
  // headers bind the actual scanned profile; this is not Linux ownership proof.
  const result = Buffer.from(tar), byName = new Map(files.map(f => [f.path, f]));
  const text = value => value.toString('utf8').split('\0')[0];
  const number = value => parseInt(text(value).trim(), 8) || 0;
  const put = (header, offset, size, value) => {
    header.fill(0, offset, offset + size); header.write(value.toString(8).padStart(size - 1, '0'), offset, size - 1, 'ascii');
  };
  for (let at = 0; at + 512 <= result.length;) {
    const header = result.subarray(at, at + 512); if (header.every(v => v === 0)) break;
    const name = text(header.subarray(0, 100)).replace(/^\.\//, '').replace(/\/$/, '');
    const info = byName.get(name === '.' ? '' : name); assert.ok(info, 'system tar membership matches independent fixture');
    put(header, 100, 8, info.mode); put(header, 108, 8, info.uid); put(header, 116, 8, info.gid);
    const size = number(header.subarray(124, 136)); header.fill(32, 148, 156);
    put(header, 148, 7, header.reduce((sum, byte) => sum + byte, 0)); header[155] = 32;
    at += 512 + Math.ceil(size / 512) * 512;
  }
  return result;
}

test('cold inventory matches independent complete tree, WAL and explicit external witness', async t => {
  const f = await fixture(t), expected = await independentInventory(f);
  const actual = await captureColdInventory(f.options);
  assert.equal(JSON.stringify(actual.restoreManifest.inventory) === JSON.stringify(expected), true);
  assert.equal(actual.sourceWitness.inventorySha256, hash(JSON.stringify(expected)));
  assert.equal(actual.sourceWitness.generationId, GEN);
  assert.equal(actual.manifestSha256, hash(JSON.stringify({ version: 1, generationId: GEN, checkpointSha256: CHECKPOINT, inventory: expected })));
  assert.equal(Buffer.from(actual.restoreFiles['runtime-config'], 'base64').equals(f.secret), true);
  assert.equal(Object.keys(actual.restoreFiles).length, 1);
});

test('cold producer metadata and system tar pass the existing genuine encrypted strict reader', async t => {
  const f = await fixture(t), independent = await independentInventory(f);
  const captured = await captureColdInventory(f.options);
  const packed = spawnSync('tar', ['--format=ustar', '-C', f.dataRoot, '-cf', '-', '.'], {
    windowsHide: true, stdio: 'pipe', timeout: 10000, maxBuffer: LIMITS.plaintextBytes,
  });
  assert.equal(packed.status, 0, 'system tar exits successfully');
  const tar = patchTarMetadata(packed.stdout, independent.files);
  const keys = generateKeyPairSync('rsa', { modulusLength: 3072 });
  const file = path.join(f.root, 'synthetic.enc');
  const metadata = { offline: true, dataFormat: 'tar', original: { State: { Running: false }, Mounts: [{ Destination: '/data', Type: 'volume' }] },
    secrets: {}, restoreManifest: captured.restoreManifest, restoreFiles: captured.restoreFiles };
  await encryptBackup({ output: file, publicKey: keys.publicKey.export({ type: 'spki', format: 'pem' }), metadata, stream: Readable.from([tar]) });
  const ciphertext = await readFile(file);
  assert.equal(ciphertext.includes(f.secret), false);
  const receipt = await inspectRestorableBackup({ file, privateKeyPem: keys.privateKey.export({ type: 'pkcs8', format: 'pem' }),
    expectedSha256: hash(ciphertext), expectedManifestSha256: captured.manifestSha256,
    sourceWitness: { generationId: GEN, checkpointSha256: CHECKPOINT, inventorySha256: hash(JSON.stringify(independent)) }, limits: { ...LIMITS } });
  assert.equal(receipt.inventoryMatched, true); assert.equal(receipt.authenticated, true);
  assert.equal(receipt.externalFiles, 1); assert.equal(receipt.sqliteFiles, 2);
});

test('cold scan catches changed body, extra file and missing required store without manifest authority', async t => {
  const f = await fixture(t), before = hash(JSON.stringify(await independentInventory(f)));
  await writeFile(path.join(f.dataRoot, 'nested', 'text.txt'), 'two synthetic body');
  const changed = await captureColdInventory(f.options);
  assert.notEqual(changed.sourceWitness.inventorySha256, before);
  await writeFile(path.join(f.dataRoot, 'extra.bin'), 'extra');
  const extra = await captureColdInventory(f.options);
  assert.notEqual(extra.sourceWitness.inventorySha256, changed.sourceWitness.inventorySha256);
  await rm(path.join(f.dataRoot, 'notes.sqlite'));
  await assert.rejects(captureColdInventory(f.options), error => fixed(error) && error.code === 'backup_cold_changed');
});

test('cold inventory rejects hard links and linked data roots with fixed diagnostics', async t => {
  const f = await fixture(t);
  const hard = path.join(f.dataRoot, 'duplicate.bin'); await link(path.join(f.dataRoot, 'nested', 'text.txt'), hard);
  await assert.rejects(captureColdInventory(f.options), fixed); await rm(hard);
  const alias = path.join(f.root, 'alias'); await symlink(f.dataRoot, alias, process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(captureColdInventory({ ...f.options, dataRoot: alias }), fixed);
});

test('cold inventory finite file, entry and external bounds reject before unbounded collection', async t => {
  const f = await fixture(t);
  for (const delta of [{ fileBytes: 1 }, { entries: 1 }, { entries: 2 }, { externalBytes: 1 }, { pathBytes: 1 }])
    await assert.rejects(captureColdInventory({ ...f.options, limits: { ...LIMITS, ...delta } }),
      error => fixed(error) && error.code === 'backup_cold_limit_exceeded');
  await assert.rejects(captureColdInventory({ ...f.options, externalFiles: [{ id: 'runtime-config', required: true, path: null }] }), fixed);
});

test('cold inventory invalid options and missing external files never expose canary paths or raw causes', async t => {
  const f = await fixture(t);
  for (const options of [null, undefined, { get dataRoot() { throw new Error('synthetic-private-input-canary'); } },
    { ...f.options, externalFiles: [{ id: 'runtime-config', required: true, path: path.join(f.root, 'synthetic-private-missing-canary') }] }])
    await assert.rejects(captureColdInventory(options), fixed);
  assert.equal((await readFile(f.config)).equals(f.secret), true);
});

test('cold mount profile admits only the exact attested public RO feed and preserves secret-file requirements', () => {
  const controls = [{ source: '/owned/public/releases', destination: '/run/connect-releases',
    purpose: 'public-signed-release-feed', attestationSha256: hash('independent operator public-feed attestation') }];
  const cold = { externalFiles: [{ id: 'application-tokens', required: true, path: '/owned/private/tokens.json' }], controlMounts: controls };
  const mounts = [{ Type: 'volume', Destination: '/data', Name: 'owned-data', RW: true },
    { Type: 'bind', Source: '/owned/private/tokens.json', Destination: '/run/secrets/soty-application-tokens.json', RW: false },
    { Type: 'bind', Source: '/owned/public/releases', Destination: '/run/connect-releases', RW: false }];
  assert.doesNotThrow(() => validateColdSourceMounts(mounts, cold));
  const invalid = error => error?.code === 'backup_cold_invalid' && error.message === 'backup_cold_invalid';
  assert.throws(() => validateColdSourceMounts(mounts, { ...cold, controlMounts: [] }), invalid);
  assert.throws(() => validateColdSourceMounts(mounts, { ...cold, externalFiles: [] }), invalid);
  assert.throws(() => validateColdSourceMounts(mounts.slice(0, 2), cold), invalid);
  for (const delta of [{ RW: true }, { Type: 'volume' }, { Source: '/different/feed' }, { Destination: '/data/feed' }])
    assert.throws(() => validateColdSourceMounts([...mounts.slice(0, 2), { ...mounts[2], ...delta }], cold), invalid);
  for (const controlMounts of [[...controls, ...controls], [{ ...controls[0], attestationSha256: '' }],
    [{ ...controls[0], purpose: 'private-config' }]])
    assert.throws(() => validateColdSourceMounts(mounts, { ...cold, controlMounts }), invalid);
});
