import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, rename, unlink } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { createRelease, verifyRelease, applyRelease, fetchRelease } from '../update/index.mjs';

const pair = generateKeyPairSync('ed25519');
const trust = { keys: { release: pair.publicKey }, threshold: 1 };
const compatibility = { protocol: 1, storage: 1, minReader: 1 };
async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), 'connect-update-'));
  const target = path.join(root, 'module'); const candidate = path.join(root, 'candidate');
  for (const [dir, version] of [[target, '0.1.0'], [candidate, '0.1.1']]) {
    await mkdir(path.join(dir, 'test'), { recursive: true });
    await writeFile(path.join(dir, 'package.json'), JSON.stringify({ name: '@soty/connect', version, type: 'module', connectCompatibility: compatibility }));
    await writeFile(path.join(dir, 'test', 'probe.test.mjs'), "import test from 'node:test';test('candidate executes',()=>{});");
  }
  const release = await createRelease({ directory: candidate, privateKey: pair.privateKey, keyId: 'release', sequence: 1, expiresAt: '2099-01-01T00:00:00Z' });
  return { root, target, release, stateDir: path.join(root, 'update-state'), trust };
}
test('signed release content, expiration, monotonic sequence and key threshold', async () => {
  const f = await fixture();
  assert.equal(verifyRelease(f.release, trust).manifest.version, '0.1.1');
  const changed = structuredClone(f.release); changed.contents['package.json'] = Buffer.from('{}').toString('base64');
  assert.throws(() => verifyRelease(changed, trust), /release_hash/);
  assert.throws(() => verifyRelease(f.release, { keys: {}, threshold: 1 }), /release_signature/);
  assert.throws(() => verifyRelease(f.release, trust, { lastSequence: 1 }), /release_rollback/);
  assert.throws(() => verifyRelease(f.release, trust, { now: Date.parse('2100-01-01') }), /release_expired/);
  assert.throws(() => verifyRelease(f.release, trust, { version: '1.0.0' }), /release_requires_migration/);
});
test('validated code update keeps independent account data and remembers anti-rollback state', async () => {
  const f = await fixture();
  const dataFile = path.join(f.root, 'account-data'); await writeFile(dataFile, 'keep-stable-account');
  const result = await applyRelease({ ...f, validate: async () => {} });
  assert.equal(result.status, 'updated');
  assert.equal(JSON.parse(await readFile(path.join(f.target, 'package.json'), 'utf8')).version, '0.1.1');
  assert.equal(await readFile(dataFile, 'utf8'), 'keep-stable-account');
  assert.equal((await applyRelease({ ...f, validate: async () => { throw new Error('must not reapply'); } })).status, 'current');
});
test('host validation failure restores previous code without downgrading account data', async () => {
  const f = await fixture();
  await assert.rejects(applyRelease({ ...f, validate: async () => { throw new Error('host failed'); } }), /host failed/);
  assert.equal(JSON.parse(await readFile(path.join(f.target, 'package.json'), 'utf8')).version, '0.1.0');
  assert.equal(JSON.parse(await readFile(path.join(f.stateDir, 'state.json'), 'utf8')).pending, null);
});
test('unsafe paths, accidental embedded storage, unsigned metadata, lock and http source are rejected', async () => {
  const f = await fixture();
  const unsafe = structuredClone(f.release); unsafe.signed.files[0].path = '../data.json';
  assert.throws(() => verifyRelease(unsafe, trust), /release_path_invalid/);
  const changed = structuredClone(f.release); changed.signed.rollout = 5;
  assert.throws(() => verifyRelease(changed, trust), /release_signature/);
  await assert.rejects(applyRelease({ ...f, stateDir: path.join(f.target, 'state'), validate: async () => {} }), /external/);
  await mkdir(f.stateDir); await writeFile(path.join(f.stateDir, 'update.lock'), 'busy');
  await assert.rejects(applyRelease({ ...f, validate: async () => {} }), /update_locked/);
  await assert.rejects(fetchRelease('http://example.org/release.json'), /https/);
});
test('rollout can defer a valid update before touching installed files', async () => {
  const f = await fixture();
  const release = await createRelease({ directory: path.join(f.root, 'candidate'), privateKey: pair.privateKey, keyId: 'release', sequence: 1, expiresAt: '2099-01-01', rollout: 0 });
  const result = await applyRelease({ ...f, release, validate: async () => { throw new Error('not called'); } });
  assert.equal(result.status, 'deferred');
});

test('a final journal write failure restores and reactivates old code even while the journal stays unavailable', async () => {
  const f = await fixture();
  const stateFile = path.join(f.stateDir, 'state.json');
  const dataFile = path.join(f.root, 'account-data'); await writeFile(dataFile, 'keep-stable-account');
  const activated = [];
  await assert.rejects(applyRelease({ ...f, validate: async () => {}, activate: async target => {
    const version = JSON.parse(await readFile(path.join(target, 'package.json'), 'utf8')).version;
    activated.push(version);
    if (version === '0.1.1') {
      // Deterministic filesystem fault: the journal destination can no longer be
      // atomically replaced, while code paths and external account data work.
      await unlink(stateFile); await mkdir(stateFile);
    }
  } }), error => {
    assert.equal(error.code, 'update_recovery_required');
    assert.equal(error.interventionRequired, true);
    assert.deepEqual(error.recovery, { codeRestored: true, hostReactivated: true, journalRestored: false });
    assert.ok(error.cause);
    assert.ok(error.errors.length >= 2);
    return true;
  });
  assert.deepEqual(activated, ['0.1.1', '0.1.0']);
  assert.equal(JSON.parse(await readFile(path.join(f.target, 'package.json'), 'utf8')).version, '0.1.0');
  assert.equal(await readFile(dataFile, 'utf8'), 'keep-stable-account');
});

async function interruptedUpdate(f) {
  const stage = `${f.target}.stage-interrupted`; const backup = `${f.target}.previous-interrupted`;
  const release = await createRelease({ directory: path.join(f.root, 'candidate'), privateKey: pair.privateKey,
    keyId: 'release', sequence: 1, expiresAt: '2099-01-01', rollout: 0 });
  await mkdir(f.stateDir);
  await rename(path.join(f.root, 'candidate'), stage);
  await rename(f.target, backup); await rename(stage, f.target);
  await writeFile(path.join(f.stateDir, 'state.json'), JSON.stringify({ format: 1, target: f.target,
    lastSequence: 0, pending: { stage, backup, nextSequence: 1 } }));
  return release;
}

test('startup recovery reactivates restored code before a deferred candidate can return', async () => {
  const f = await fixture(); const release = await interruptedUpdate(f);
  let runningVersion = '0.1.1'; const activated = [];
  const result = await applyRelease({ ...f, release, validate: async () => assert.fail('deferred update cannot validate'),
    activate: async target => {
      runningVersion = JSON.parse(await readFile(path.join(target, 'package.json'), 'utf8')).version;
      activated.push(runningVersion);
      assert.ok(JSON.parse(await readFile(path.join(f.stateDir, 'state.json'), 'utf8')).pending,
        'recovery must stay pending until host activation succeeds');
    } });
  assert.equal(result.status, 'deferred');
  assert.deepEqual(activated, ['0.1.0']);
  assert.equal(runningVersion, '0.1.0');
  assert.equal(JSON.parse(await readFile(path.join(f.stateDir, 'state.json'), 'utf8')).pending, null);
});

test('failed startup reactivation retains pending recovery and the next run retries it', async () => {
  const f = await fixture(); const release = await interruptedUpdate(f);
  await assert.rejects(applyRelease({ ...f, release, activate: async () => { throw new Error('host unavailable'); } }), error => {
    assert.equal(error.code, 'update_recovery_required');
    assert.equal(error.interventionRequired, true);
    assert.deepEqual(error.recovery, { codeRestored: true, hostReactivated: false, journalRestored: false });
    return true;
  });
  assert.equal(JSON.parse(await readFile(path.join(f.target, 'package.json'), 'utf8')).version, '0.1.0');
  assert.ok(JSON.parse(await readFile(path.join(f.stateDir, 'state.json'), 'utf8')).pending);
  const activated = [];
  const result = await applyRelease({ ...f, release, activate: async target => {
    activated.push(JSON.parse(await readFile(path.join(target, 'package.json'), 'utf8')).version);
  } });
  assert.equal(result.status, 'deferred');
  assert.deepEqual(activated, ['0.1.0']);
  assert.equal(JSON.parse(await readFile(path.join(f.stateDir, 'state.json'), 'utf8')).pending, null);
});

test('Windows case variants cannot put the update journal inside the module',
  { skip: process.platform !== 'win32' }, async () => {
    const f = await fixture();
    await assert.rejects(applyRelease({ ...f, stateDir: path.join(f.target.toUpperCase(), 'state'), validate: async () => {} }),
      /update_state_must_be_external/);
    await assert.rejects(applyRelease({ ...f, target: f.target.toUpperCase(), stateDir: f.root.toUpperCase(), validate: async () => {} }),
      /update_state_must_be_external/);
    assert.equal(JSON.parse(await readFile(path.join(f.target, 'package.json'), 'utf8')).version, '0.1.0');
  });
