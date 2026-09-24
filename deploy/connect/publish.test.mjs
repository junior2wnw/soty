import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, readdir } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import os from 'node:os';
import path from 'node:path';
import { verifyRelease, sha256 } from '../../modules/connect/update/index.mjs';

const publisher = fileURLToPath(new URL('./publish.mjs', import.meta.url));
const pair = generateKeyPairSync('ed25519');
const privateKeyPem = pair.privateKey.export({ type: 'pkcs8', format: 'pem' });
const publicKeyPem = pair.publicKey.export({ type: 'spki', format: 'pem' });
const trust = { threshold: 1, keys: { 'test-key': publicKeyPem } };
const compatibility = { protocol: 1, storage: 1, minReader: 1 };
async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), 'connect-publish-'));
  const directory = path.join(root, 'module'); const outputDirectory = path.join(root, 'feed');
  const trustFile = path.join(root, 'trust.json');
  await mkdir(directory); await writeFile(trustFile, JSON.stringify(trust));
  const setVersion = version => writeFile(path.join(directory, 'package.json'), JSON.stringify({
    name: '@soty/connect', version, type: 'module', connectCompatibility: compatibility,
  }));
  await setVersion('0.1.0'); await writeFile(path.join(directory, 'index.mjs'), 'export const ready = true;\n');
  return { root, directory, outputDirectory, trustFile, setVersion,
    input: { config: { directory, outputDirectory, trustFile, keyId: 'test-key' }, privateKeyPem } };
}
async function run(input, args = []) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [publisher, ...args], { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = ''; let stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject); child.stdin.on('error', error => { if (error.code !== 'EPIPE') reject(error); });
    child.on('close', code => resolve({ code, stdout, stderr }));
    child.stdin.end(typeof input === 'string' ? input : JSON.stringify(input));
  });
}
function safeOutput(result) {
  assert.doesNotMatch(result.stdout + result.stderr, /BEGIN (?:PRIVATE|PUBLIC) KEY/);
  assert.ok(!`${result.stdout}${result.stderr}`.includes(privateKeyPem.split('\n')[1]));
}

test('stdin publisher emits pinned signed immutable bytes, atomic stable bytes and a key-free receipt', async () => {
  const f = await fixture(); const beforeTrust = await readFile(f.trustFile);
  const result = await run(f.input); safeOutput(result); assert.equal(result.code, 0, result.stderr);
  const receipt = JSON.parse(result.stdout);
  assert.equal(receipt.sequence, 1); assert.equal(receipt.keyId, 'test-key');
  assert.equal(receipt.releaseFile, 'release-1.json');
  assert.ok(Date.parse(receipt.expiresAt) - Date.now() > 89 * 24 * 60 * 60 * 1000);
  assert.ok(Date.parse(receipt.expiresAt) - Date.now() <= 90 * 24 * 60 * 60 * 1000);
  const immutable = await readFile(path.join(f.outputDirectory, receipt.releaseFile));
  assert.deepEqual(await readFile(path.join(f.outputDirectory, 'stable.json')), immutable);
  assert.equal(receipt.releaseSha256, sha256(immutable));
  assert.equal(verifyRelease(JSON.parse(immutable), trust).manifest.sequence, 1);
  assert.deepEqual(await readFile(f.trustFile), beforeTrust);
  assert.deepEqual((await readdir(f.outputDirectory)).sort(), ['release-1.json', 'stable.json']);
  assert.ok(!result.stdout.includes(f.root));
});

test('sequences only advance and existing releases remain byte-for-byte immutable', async () => {
  const f = await fixture(); assert.equal((await run(f.input)).code, 0);
  const first = await readFile(path.join(f.outputDirectory, 'release-1.json'));
  await f.setVersion('0.1.1'); const second = await run(f.input);
  assert.equal(second.code, 0, second.stderr); assert.equal(JSON.parse(second.stdout).sequence, 2);
  const stable = await readFile(path.join(f.outputDirectory, 'stable.json'));
  const retry = await run({ ...f.input, config: { ...f.input.config, sequence: 1 } });
  assert.equal(retry.code, 1); assert.equal(JSON.parse(retry.stderr).error, 'publish_sequence_rollback');
  assert.deepEqual(await readFile(path.join(f.outputDirectory, 'release-1.json')), first);
  assert.deepEqual(await readFile(path.join(f.outputDirectory, 'stable.json')), stable);
  await f.setVersion('0.1.0'); assert.equal((await run(f.input)).code, 1);
  assert.deepEqual(await readFile(path.join(f.outputDirectory, 'stable.json')), stable);
});

test('a release left ahead of stable after interruption burns its sequence without overwrite', async () => {
  const f = await fixture(); assert.equal((await run(f.input)).code, 0);
  const first = await readFile(path.join(f.outputDirectory, 'stable.json'));
  assert.equal((await run(f.input)).code, 0);
  const orphan = await readFile(path.join(f.outputDirectory, 'release-2.json'));
  await writeFile(path.join(f.outputDirectory, 'stable.json'), first);
  const result = await run(f.input); assert.equal(result.code, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).sequence, 3);
  assert.deepEqual(await readFile(path.join(f.outputDirectory, 'release-2.json')), orphan);
});

test('tampering with either current immutable bytes or stable metadata blocks publication', async () => {
  for (const name of ['release-1.json', 'stable.json']) {
    const f = await fixture(); assert.equal((await run(f.input)).code, 0);
    const original = await readFile(path.join(f.outputDirectory, name));
    const changed = JSON.parse(original); changed.signed.rollout = 0;
    await writeFile(path.join(f.outputDirectory, name), JSON.stringify(changed));
    const result = await run(f.input); safeOutput(result);
    assert.equal(result.code, 1); assert.equal(JSON.parse(result.stderr).error, 'publish_existing_tampered');
    assert.ok(!(await readdir(f.outputDirectory)).includes('release-2.json'));
  }
});

test('an unpinned key, overlapping paths, bad expiry and existing lock fail without exposing input', async () => {
  const f = await fixture();
  const wrong = generateKeyPairSync('ed25519').privateKey.export({ type: 'pkcs8', format: 'pem' });
  for (const input of [
    { ...f.input, privateKeyPem: wrong },
    { ...f.input, config: { ...f.input.config, outputDirectory: path.join(f.directory, 'feed') } },
    { ...f.input, config: { ...f.input.config, expiresAt: '2000-01-01' } },
    { ...f.input, config: { ...f.input.config, expiresAt: '2999-01-01' } },
  ]) {
    const result = await run(input); safeOutput(result); assert.equal(result.code, 1);
  }
  await mkdir(f.outputDirectory); await writeFile(path.join(f.outputDirectory, '.publish.lock'), 'another publisher');
  const locked = await run(f.input); assert.equal(JSON.parse(locked.stderr).error, 'publish_locked');
  assert.deepEqual(await readdir(f.outputDirectory), ['.publish.lock']);
});

test('configuration and private PEM are accepted only on stdin and parse failures never echo them', async () => {
  const f = await fixture();
  const argument = await run(f.input, ['config.json']);
  assert.equal(argument.code, 1); assert.equal(JSON.parse(argument.stderr).error, 'publish_stdin_only'); safeOutput(argument);
  const invalid = await run(`{"config": ${JSON.stringify(privateKeyPem)},`);
  assert.equal(invalid.code, 1); assert.equal(JSON.parse(invalid.stderr).error, 'publish_input_invalid'); safeOutput(invalid);
  const invalidKey = await run({ ...f.input, privateKeyPem: 'secret-marker-invalid-pem' });
  assert.equal(invalidKey.code, 1); assert.ok(!invalidKey.stderr.includes('secret-marker'));
});

test('concurrent publishers either serialize increasing releases or reject the exclusive lock', async () => {
  const f = await fixture(); const results = await Promise.all([run(f.input), run(f.input)]);
  const receipts = results.filter(result => result.code === 0).map(result => JSON.parse(result.stdout));
  assert.ok(receipts.length >= 1);
  assert.equal(new Set(receipts.map(receipt => receipt.sequence)).size, receipts.length);
  for (const result of results.filter(result => result.code !== 0)) assert.equal(JSON.parse(result.stderr).error, 'publish_locked');
  const stable = JSON.parse(await readFile(path.join(f.outputDirectory, 'stable.json')));
  assert.equal(stable.signed.sequence, Math.max(...receipts.map(receipt => receipt.sequence)));
  assert.equal(verifyRelease(stable, trust).manifest.sequence, stable.signed.sequence);
});
