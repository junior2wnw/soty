import test from 'node:test';
import assert from 'node:assert/strict';
import { Writable } from 'node:stream';
import { writeFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fixture, sendAuthenticatedBackup as originalSend } from '../../test/strict-sender-fixture.mjs';
import { sendAuthenticatedBackup as stagedSend } from './staged-test-adapter.mjs';
import { createReadinessBoundary } from './readiness-boundary.mjs';
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const SPEC = Object.freeze({ transaction: '1234567890abcdef1234567890abcdef', nonce: 'a'.repeat(32),
  image: 'sha256:' + 'b'.repeat(64), receiverSourceSha256: 'c'.repeat(64) });
const warm = spec => ({ ...spec, inputBytes: 0, verifiedBeforeStop: true });
const bound = expected => ({ transaction: expected.transaction, nonce: expected.nonce,
  expectedSha256: expected.expectedSha256, expectedManifestSha256: expected.expectedManifestSha256, ...expected.sourceWitness, inputBytes: 0 });
function output() {
  const chunks = [];
  const stream = new Writable({ highWaterMark: 65536, emitClose: true, autoDestroy: true,
    write(chunk, _encoding, callback) { chunks.push(Buffer.from(chunk)); callback(); } });
  return { stream, bytes: () => Buffer.concat(chunks) };
}
async function admitted(t, ports = {}) {
  const f = await fixture(t), encrypted = await f.encrypt();
  const boundary = createReadinessBoundary({ prepareBeforeStop: async spec => warm(spec),
    bindAfterAuthentication: async expected => bound(expected), ...ports });
  const lease = await boundary.warmBeforeServingStop(SPEC);
  boundary.noteServingStopped(lease);
  boundary.captureLaterBackup(lease, { expectedSha256: encrypted.options.expectedSha256, expectedManifestSha256: encrypted.options.expectedManifestSha256,
    sourceWitness: encrypted.options.sourceWitness });
  return { f, encrypted, boundary, lease };
}

test('actual original sender spends its existing budget on delayed first-write startup', async t => {
  const f = await fixture(t), encrypted = await f.encrypt();
  let writeEntered = false, startupFinished = false;
  const stream = new Writable({ highWaterMark: 65536, emitClose: true, autoDestroy: true,
    write(_chunk, _encoding, callback) {
      writeEntered = true;
      setTimeout(() => { startupFinished = true; callback(); }, 300);
    } });
  await assert.rejects(originalSend({ ...encrypted.options, output: stream,
    limits: { ...encrypted.options.limits, wallMs: 150, idleMs: 120 } }), { code: 'restore_timeout' });
  assert.equal(writeEntered, true); assert.equal(startupFinished, true);
  assert.equal(stream.closed, true);
});

test('warming before stop removes startup from transfer without moving late bind outside its budget', async t => {
  const f = await fixture(t), encrypted = await f.encrypt();
  let serving = true, boundCalls = 0;
  const collected = output();
  const boundary = createReadinessBoundary({ prepareBeforeStop: async spec => {
    await delay(300); assert.equal(serving, true); return warm(spec);
  }, bindAfterAuthentication: async (expected, fence) => {
    fence.check(); boundCalls++; assert.equal(serving, false); assert.equal(collected.bytes().length, 0); return bound(expected);
  } });
  const lease = await boundary.warmBeforeServingStop(SPEC);
  serving = false; boundary.noteServingStopped(lease);
  boundary.captureLaterBackup(lease, { expectedSha256: encrypted.options.expectedSha256, expectedManifestSha256: encrypted.options.expectedManifestSha256, sourceWitness: encrypted.options.sourceWitness });
  const receipt = await stagedSend({ ...encrypted.options, output: collected.stream,
    beforeBody: boundary.beforeBody(lease), limits: { ...encrypted.options.limits, wallMs: 150, idleMs: 120 } });
  assert.equal(receipt.authenticated, true); assert.equal(boundCalls, 1);
  assert.equal(collected.bytes().equals(encrypted.plaintext), true);
});

test('full GCM tag failure permits neither readiness binding nor a plaintext byte', async t => {
  let binds = 0;
  const f = await admitted(t, { bindAfterAuthentication: async expected => { binds++; return bound(expected); } });
  const corrupted = Buffer.from(f.encrypted.ciphertext); corrupted[corrupted.length - 1] ^= 1;
  await writeFile(f.encrypted.options.file, corrupted);
  const collected = output();
  await assert.rejects(stagedSend({ ...f.encrypted.options, expectedSha256: sha(corrupted), output: collected.stream,
    beforeBody: f.boundary.beforeBody(f.lease) }), { code: 'restore_authentication_failed' });
  assert.equal(binds, 0); assert.equal(collected.bytes().length, 0);
});

test('matching generation is unavailable at warm stage and cannot be invented from a copied lease', async t => {
  const f = await fixture(t), encrypted = await f.encrypt();
  const boundary = createReadinessBoundary({ prepareBeforeStop: async spec => warm(spec), bindAfterAuthentication: async expected => bound(expected) });
  const lease = await boundary.warmBeforeServingStop(SPEC);
  assert.throws(() => boundary.beforeBody(lease), { code: 'readiness_stage_invalid' });
  assert.throws(() => boundary.noteServingStopped({}), { code: 'readiness_unowned_lease' });
  boundary.noteServingStopped(lease);
  assert.throws(() => boundary.captureLaterBackup(lease, { expectedSha256: encrypted.options.expectedSha256, expectedManifestSha256: encrypted.options.expectedManifestSha256,
    sourceWitness: { ...encrypted.options.sourceWitness, generationId: 'd'.repeat(32) } }), { code: 'readiness_backup_invalid' });
});

for (const [name, mutate] of [
  ['copied nonce', value => { value.nonce = 'd'.repeat(32); }],
  ['stale generation', value => { value.generationId = 'd'.repeat(32); }],
  ['different manifest', value => { value.expectedManifestSha256 = 'd'.repeat(64); }],
  ['different ciphertext', value => { value.expectedSha256 = 'd'.repeat(64); }],
  ['hidden extra field', value => { value.unreviewed = true; }],
  ['payload already read', value => { value.inputBytes = 1; }],
]) test('actual sender denies spoofed bound ready: ' + name, async t => {
  const f = await admitted(t, { bindAfterAuthentication: async expected => { const value = bound(expected); mutate(value); return value; } });
  const collected = output();
  await assert.rejects(stagedSend({ ...f.encrypted.options, output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease) }), { code: 'restore_io_failed' });
  assert.equal(collected.bytes().length, 0);
});

test('cancel during a late bind closes admission permanently and never starts the second pass', async t => {
  const d = deferred(), entered = deferred();
  const f = await admitted(t, { bindAfterAuthentication: async expected => { entered.resolve(); await d.promise; return bound(expected); } });
  const collected = output();
  const sending = stagedSend({ ...f.encrypted.options, output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease) });
  await entered.promise; f.boundary.cancel(f.lease); d.resolve();
  await assert.rejects(sending, { code: 'restore_io_failed' });
  assert.equal(collected.bytes().length, 0);
  assert.throws(() => f.boundary.beforeBody(f.lease), { code: 'readiness_stage_invalid' });
});

test('late readiness after timeout cannot emit bytes or reset the existing wall/idle', async t => {
  const d = deferred(), entered = deferred();
  const f = await admitted(t, { bindAfterAuthentication: async expected => { entered.resolve(); await d.promise; return bound(expected); } });
  const collected = output();
  const sending = stagedSend({ ...f.encrypted.options, output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease),
    limits: { ...f.encrypted.options.limits, wallMs: 150, idleMs: 120 } });
  await entered.promise;
  await assert.rejects(sending, { code: 'restore_timeout' });
  d.resolve(); await delay(10);
  assert.equal(collected.bytes().length, 0); assert.equal(collected.stream.closed, true);
});

test('native close during bind denies late ready without a plaintext write', async t => {
  const d = deferred(), entered = deferred();
  const f = await admitted(t, { bindAfterAuthentication: async expected => { entered.resolve(); await d.promise; return bound(expected); } });
  const collected = output();
  const sending = stagedSend({ ...f.encrypted.options, output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease) });
  await entered.promise; collected.stream.destroy();
  await assert.rejects(sending, { code: 'restore_io_failed' });
  d.resolve(); await delay(10);
  assert.equal(collected.bytes().length, 0); assert.equal(collected.stream.closed, true);
});

test('a different authenticated ciphertext cannot use the readiness lease of a previous backup', async t => {
  let binds = 0;
  const f = await admitted(t, { bindAfterAuthentication: async expected => { binds++; return bound(expected); } });
  const second = await f.f.encrypt();
  assert.notEqual(second.options.expectedSha256, f.encrypted.options.expectedSha256);
  const collected = output();
  await assert.rejects(stagedSend({ ...second.options, output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease) }), { code: 'restore_io_failed' });
  assert.equal(binds, 0); assert.equal(collected.bytes().length, 0);
});

test('file drift during binding is rejected again before the second pass sends plaintext', async t => {
  let current;
  const f = await admitted(t, { bindAfterAuthentication: async expected => {
    const changed = Buffer.from(current.encrypted.ciphertext); changed[changed.length - 1] ^= 1;
    await writeFile(current.encrypted.options.file, changed);
    return bound(expected);
  } });
  current = f;
  const collected = output();
  await assert.rejects(stagedSend({ ...f.encrypted.options, output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease) }), { code: 'restore_authentication_failed' });
  assert.equal(collected.bytes().length, 0);
});

test('throwing telemetry and private-error getters never replace the actual sender failure', async t => {
  let reads = 0;
  const privateError = new Error();
  Object.defineProperty(privateError, 'message', { get() { reads++; throw Error('must_not_read'); } });
  const f = await admitted(t, { emit: () => { throw privateError; }, bindAfterAuthentication: async () => { throw privateError; } });
  const collected = output();
  await assert.rejects(stagedSend({ ...f.encrypted.options, output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease) }), { code: 'restore_io_failed' });
  assert.equal(reads, 0); assert.equal(collected.bytes().length, 0);
});

test('current sender keeps both authenticated passes and hook between them', async () => {
  const pins=JSON.parse(await readFile(new URL('../../current-core-pins.json',import.meta.url),'utf8'));
  assert.deepEqual(pins.productionProfile,{wallMs:120000,idleMs:15000});
  assert.equal(pins.currentCoreRevision,'90335314a663441d34148e8c312e3e4d42350eb7');
  assert.equal(pins.files.length,5);
  for(const file of pins.files) assert.equal(sha(await readFile(new URL('../../'+file.path,import.meta.url))),file.sha256);
  const sender=await readFile(new URL('../../../restore-backup.mjs',import.meta.url),'utf8');
  const first=sender.indexOf('const first = await readEncryptedPass(passOptions, null, specialization);');
  const hook=sender.indexOf('await owner.beforeBody(beforeBody,');
  const second=sender.indexOf('const second = await readEncryptedPass(passOptions, owner, specialization);');
  assert.ok(first>=0 && hook>first && second>hook);
  assert.match(sender,/export const sendAuthenticatedBackup = value => sendBackup\(value\)/u);
  assert.match(sender,/send: input => sendBackup\(input, specialization\)/u);
});
