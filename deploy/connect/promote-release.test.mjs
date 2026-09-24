import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, readdir, symlink, unlink } from 'node:fs/promises';
import { Readable } from 'node:stream';
import os from 'node:os';
import path from 'node:path';
import { createRelease, sha256 } from './update-engine.mjs';
import { promoteRelease, main } from './promote-release.mjs';

const pair = generateKeyPairSync('ed25519');
async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), 'connect-promote-')), directory = path.join(root, 'module'), releaseDirectory = path.join(root, 'feed'), trustFile = path.join(root, 'trust.json');
  await mkdir(directory); await mkdir(releaseDirectory);
  await writeFile(trustFile, JSON.stringify({ threshold: 1, keys: { test: pair.publicKey.export({ type: 'spki', format: 'pem' }) } }));
  const marker = path.join(root, 'must-not-execute');
  await writeFile(path.join(directory, 'index.mjs'), `import { writeFileSync } from 'node:fs';writeFileSync(${JSON.stringify(marker)}, 'executed');`);
  async function release(sequence = 1, version = '0.1.1', options = {}) {
    await writeFile(path.join(directory, 'package.json'), JSON.stringify({ name: '@soty/connect', version, type: 'module', connectCompatibility: { protocol: 1, storage: 1, minReader: 1 } }));
    return Buffer.from(JSON.stringify(await createRelease({ directory, privateKey: pair.privateKey, keyId: 'test', sequence, expiresAt: '2099-01-01', ...options })) + '\n');
  }
  const args = { trustFile, releaseDirectory };
  return { root, directory, marker, args, release, names: () => readdir(releaseDirectory), stable: () => readFile(path.join(releaseDirectory, 'stable.json')) };
}

test('promotion verifies signed bytes without execution, preserves exact bytes and retries idempotently', async () => {
  const f = await fixture(), bytes = await f.release(), artifactFile = path.join(f.root, 'incoming.json');
  await writeFile(artifactFile, bytes); const trustBefore = await readFile(f.args.trustFile);
  const receipt = await promoteRelease({ ...f.args, artifactFile });
  assert.equal(receipt.status, 'promoted'); assert.equal(receipt.sha256, sha256(bytes)); assert.equal(receipt.bytes, bytes.length);
  assert.deepEqual(await f.stable(), bytes); assert.deepEqual(await readFile(path.join(f.args.releaseDirectory, 'release-1.json')), bytes);
  assert.equal((await promoteRelease({ ...f.args, bytes })).status, 'current');
  assert.deepEqual(await readFile(f.args.trustFile), trustBefore);
  await assert.rejects(readFile(f.marker), error => error.code === 'ENOENT');
  assert.deepEqual((await f.names()).sort(), ['release-1.json', 'stable.json']);
});

test('sequence and version advance, while exact-byte conflicts and rollback preserve the current channel', async () => {
  const f = await fixture(), first = await f.release(); await promoteRelease({ ...f.args, bytes: first });
  await assert.rejects(promoteRelease({ ...f.args, bytes: Buffer.concat([first, Buffer.from('\n')]) }), /promote_sequence_conflict/);
  const second = await f.release(2, '0.1.2'); await promoteRelease({ ...f.args, bytes: second });
  await assert.rejects(promoteRelease({ ...f.args, bytes: first }), /promote_sequence_rollback/);
  await assert.rejects(promoteRelease({ ...f.args, bytes: await f.release(3, '0.1.1') }), /promote_version_rollback/);
  await assert.rejects(promoteRelease({ ...f.args, bytes: await f.release(3, '0.2.0') }), /promote_release_invalid/);
  assert.deepEqual(await f.stable(), second); assert.deepEqual(await readFile(path.join(f.args.releaseDirectory, 'release-1.json')), first);
});

test('an interrupted immutable-to-stable switch finishes only with identical bytes', async () => {
  const f = await fixture(); await promoteRelease({ ...f.args, bytes: await f.release() });
  const second = await f.release(2, '0.1.2'); await writeFile(path.join(f.args.releaseDirectory, 'release-2.json'), second);
  await assert.rejects(promoteRelease({ ...f.args, bytes: Buffer.concat([second, Buffer.from(' ')]) }), /promote_sequence_conflict/);
  assert.equal((await promoteRelease({ ...f.args, bytes: second })).status, 'promoted'); assert.deepEqual(await f.stable(), second);
});

test('untrusted signature, changed content, expiration, oversized input and malformed UTF8 fail without publication', async () => {
  const f = await fixture(), bytes = await f.release(), altered = JSON.parse(bytes); altered.signed.rollout = 0;
  const content = JSON.parse(bytes); content.contents['index.mjs'] = Buffer.from('different bytes').toString('base64');
  const wrongKey = generateKeyPairSync('ed25519');
  for (const invalid of [Buffer.from(JSON.stringify(altered)), Buffer.from(JSON.stringify(content)), await f.release(1, '0.1.1', { privateKey: wrongKey.privateKey }), await f.release(1, '0.1.1', { expiresAt: '2000-01-01' }), Buffer.alloc(12 * 1024 * 1024 + 1), Buffer.from([0xff, 0xfe]), Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), bytes])]) {
    await assert.rejects(promoteRelease({ ...f.args, bytes: invalid }), /promote_/);
  }
  assert.deepEqual(await f.names(), []);
});

test('tampered history and symlinked ancestors are rejected without overwriting feed or trust', async () => {
  const f = await fixture(), first = await f.release(); await promoteRelease({ ...f.args, bytes: first });
  await writeFile(path.join(f.args.releaseDirectory, 'stable.json'), Buffer.concat([first, Buffer.from(' ')]));
  await assert.rejects(promoteRelease({ ...f.args, bytes: await f.release(2, '0.1.2') }), /promote_history_invalid/);
  const alias = path.join(f.root, 'alias'); await symlink(f.args.releaseDirectory, alias, process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(promoteRelease({ ...f.args, releaseDirectory: alias, bytes: first }), /promote_symlink/);
  await assert.rejects(promoteRelease({ ...f.args, trustFile: path.join(f.args.releaseDirectory, 'trust.json'), bytes: first }), /promote_trust_inside_feed/);
  assert.ok(!(await f.names()).includes('release-2.json'));
});

test('shared publisher lock refuses concurrent ownership and parallel identical deliveries are safe', async () => {
  const f = await fixture(), bytes = await f.release(), lock = path.join(f.args.releaseDirectory, '.publish.lock');
  await writeFile(lock, 'another publisher'); await assert.rejects(promoteRelease({ ...f.args, bytes }), /promote_locked/); await unlink(lock);
  const results = await Promise.allSettled([promoteRelease({ ...f.args, bytes }), promoteRelease({ ...f.args, bytes })]);
  assert.ok(results.some(r => r.status === 'fulfilled'));
  for (const result of results) if (result.status === 'rejected') assert.equal(result.reason.code, 'promote_locked');
  assert.deepEqual(await f.stable(), bytes); assert.deepEqual((await f.names()).sort(), ['release-1.json', 'stable.json']);
});

test('stdin entry point accepts binary chunks and emits only a content-free receipt', async () => {
  const f = await fixture(), bytes = await f.release();
  const receipt = await main([f.args.trustFile, f.args.releaseDirectory, '-'], Readable.from([bytes.subarray(0, 71), bytes.subarray(71)]));
  assert.equal(receipt.sha256, sha256(bytes)); assert.deepEqual(await f.stable(), bytes);
  assert.ok(!JSON.stringify(receipt).includes('contents')); assert.ok(!JSON.stringify(receipt).includes(f.root));
});
