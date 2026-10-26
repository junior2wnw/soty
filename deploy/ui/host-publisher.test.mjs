import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { randomBytes } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { sha256, encryptConfig, decryptConfig, writeEncryptedConfigs, swapConfig } from './config-swap.mjs';
import { validateHostPlan, installPublicRelease, verifyInstalledRelease, executeHostPlan, inspectHostPlan, verifyCaddyProcess } from './host-publisher.mjs';
import { createUiRelease, writeUiRelease } from './release.mjs';
import { fixtureDist } from './test-support.mjs';

const linux = process.platform === 'linux', root = linux && process.getuid() === 0;
const transaction = '1'.repeat(32), digest = '2'.repeat(64);
function fixture(t) {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(tmpdir(), 'soty-ui-host-')));
  t.after(() => {
    const unseal = current => { fs.chmodSync(current, 0o700); for (const entry of fs.readdirSync(current, { withFileTypes: true })) if (entry.isDirectory()) unseal(path.join(current, entry.name)); };
    unseal(directory); fs.rmSync(directory, { recursive: true, force: true });
  });
  fs.chmodSync(directory, 0o700);
  const filename = path.join(directory, 'Caddyfile'), baseline = Buffer.from('baseline private fake'), candidate = Buffer.from('candidate private fake');
  fs.writeFileSync(filename, baseline, { mode: 0o640 });
  return { directory, filename, baseline, candidate, key: randomBytes(32) };
}
function plan(directory) {
  return { schema: 'soty.ui.host.v1', action: 'apply', transaction, releaseId: digest, receiptDirectory: directory,
    releaseRoot: directory + '/' + digest, configPath: directory + '/Caddyfile', profile: { uid: 0, gid: 0, mode: 0o644 },
    process: { pid: 2147483647, uid: 0, startTicks: '12345', argvSha256: digest },
    baseline: { file: directory + '/baseline.enc', cipherSha256: digest, plainSha256: '3'.repeat(64) },
    candidate: { file: directory + '/candidate.enc', cipherSha256: digest, plainSha256: '4'.repeat(64) },
    overlay: { canonicalSha256: digest, currentReleaseId: digest, currentRoot: directory + '/' + digest,
      currentSha256: '3'.repeat(64), nextSha256: '4'.repeat(64), activeSha256: digest } };
}

test('AES-GCM authenticates key, purpose, ciphertext and exact plaintext hash', t => {
  const f = fixture(t), raw = encryptConfig(f.baseline, f.key, 'baseline');
  assert.deepEqual(decryptConfig(raw, f.key, 'baseline', sha256(f.baseline)), f.baseline);
  for (const [key, purpose, expected, source] of [
    [randomBytes(32), 'baseline', sha256(f.baseline), raw], [f.key, 'candidate', sha256(f.baseline), raw],
    [f.key, 'baseline', sha256(f.candidate), raw],
    [f.key, 'baseline', sha256(f.baseline), Buffer.from(raw.toString().replace('ciphertext":"', 'ciphertext":"AAAA'))],
  ]) assert.throws(() => decryptConfig(source, key, purpose, expected), error => ['encrypted_config_invalid', 'decrypted_config_mismatch'].includes(error.code));
});

test('encrypted backup files are fresh, mode600, authenticated and contain no plaintext', { skip: !linux }, t => {
  const f = fixture(t), pins = writeEncryptedConfigs({ ...f, directory: f.directory });
  for (const purpose of ['baseline', 'candidate']) {
    const raw = fs.readFileSync(pins[purpose].file);
    assert.equal(raw.includes(f[purpose]), false); assert.equal(fs.statSync(pins[purpose].file).mode & 0o777, 0o600);
    assert.deepEqual(decryptConfig(raw, f.key, purpose, pins[purpose].plainSha256), f[purpose]);
  }
  assert.throws(() => writeEncryptedConfigs({ ...f, directory: f.directory }), { code: 'EEXIST' });
});

test('atomic swap checks bytes and preserves exact owner/group/mode before signalling', { skip: !linux }, t => {
  const f = fixture(t), before = fs.statSync(f.filename), phases = []; let signals = 0;
  const result = swapConfig({ filename: f.filename, expectedSha256: sha256(f.baseline), next: f.candidate,
    nextSha256: sha256(f.candidate), transaction, profile: { uid: before.uid, gid: before.gid, mode: before.mode & 0o777 },
    signal() { signals++; assert.deepEqual(fs.readFileSync(f.filename), f.candidate); }, record: phase => phases.push(phase) });
  assert.equal(signals, 1); assert.equal(result.activeReloadVerified, false);
  assert.deepEqual(phases, ['replace-intent', 'replaced', 'signal-sent']);
  const after = fs.statSync(f.filename);
  assert.deepEqual([after.uid, after.gid, after.mode & 0o777], [before.uid, before.gid, before.mode & 0o777]);
});

test('baseline mismatch and late inode/byte drift refuse without signal', { skip: !linux }, t => {
  const f = fixture(t); let signals = 0;
  const args = { filename: f.filename, expectedSha256: sha256(f.baseline), next: f.candidate,
    nextSha256: sha256(f.candidate), transaction, signal() { signals++; } };
  assert.throws(() => swapConfig({ ...args, expectedSha256: digest }), { code: 'current_config_changed' });
  const foreign = Buffer.from('concurrent operator bytes');
  assert.throws(() => swapConfig({ ...args, record(phase) { if (phase === 'replace-intent') { fs.unlinkSync(f.filename); fs.writeFileSync(f.filename, foreign); } } }), { code: 'current_config_changed' });
  assert.equal(signals, 0); assert.deepEqual(fs.readFileSync(f.filename), foreign);
});

test('an existing temporary file belongs to someone else and is never deleted', { skip: !linux }, t => {
  const f = fixture(t), temporary = path.join(f.directory, '.soty-ui-' + transaction + '.tmp'); fs.writeFileSync(temporary, 'foreign');
  assert.throws(() => swapConfig({ filename: f.filename, expectedSha256: sha256(f.baseline), next: f.candidate,
    nextSha256: sha256(f.candidate), transaction, signal() { assert.fail('must not signal'); } }), { code: 'EEXIST' });
  assert.equal(fs.readFileSync(temporary, 'utf8'), 'foreign'); assert.deepEqual(fs.readFileSync(f.filename), f.baseline);
});

test('lost signal response retains next bytes and requires recovery; no automatic replay or restore', { skip: !linux }, t => {
  const f = fixture(t), phases = []; let signals = 0;
  assert.throws(() => swapConfig({ filename: f.filename, expectedSha256: sha256(f.baseline), next: f.candidate,
    nextSha256: sha256(f.candidate), transaction, record: phase => phases.push(phase),
    signal() { signals++; throw Object.assign(new Error('private transport detail'), { code: 'EIO' }); } }), { recoveryRequired: true, replaced: true });
  assert.equal(signals, 1); assert.deepEqual(fs.readFileSync(f.filename), f.candidate);
  assert.deepEqual(phases, ['replace-intent', 'replaced', 'reload-unconfirmed']);
});

test('plans reject an arbitrary signal and all unreviewed fields/pins', t => {
  const approved = plan('/owned'); assert.equal(validateHostPlan(approved), approved);
  for (const change of [value => value.signal = 'SIGTERM', value => value.action = 'reload', value => value.process.pid = 1,
    value => value.process.startTicks = '0', value => value.configPath += '/../other', value => value.baseline.plainSha256 = value.candidate.plainSha256,
    value => value.process.extra = true, value => value.profile.mode = 0o666]) {
    const altered = structuredClone(approved); change(altered); assert.throws(() => validateHostPlan(altered), { code: 'host_plan_invalid' });
  }
});

test('finite public install excludes private files and seals a fresh immutable directory', { skip: !linux }, t => {
  const f = fixture(t), input = fixtureDist(path.join(f.directory, 'input'), { '.env': 'fake secret', 'assets/private.js.map': 'private' });
  const release = createUiRelease(input), result = installPublicRelease({ input, manifest: { id: release.id, manifest: release.manifest }, base: f.directory });
  assert.equal(result.signalsSent, 0); assert.equal(createUiRelease(result.root).id, release.id);
  assert.equal(fs.existsSync(path.join(result.root, '.env')), false); assert.equal(fs.existsSync(path.join(result.root, 'assets/private.js.map')), false);
  assert.equal(fs.statSync(result.root).mode & 0o777, 0o555);
  assert.equal(fs.statSync(path.join(result.root, 'index.html')).mode & 0o777, 0o444);
  assert.equal(verifyInstalledRelease(result.root, release.id).id, release.id);
  assert.throws(() => installPublicRelease({ input, manifest: { id: release.id, manifest: release.manifest }, base: f.directory }), { code: 'EEXIST' });
  assert.throws(() => installPublicRelease({ input, manifest: { id: digest, manifest: release.manifest }, base: f.directory }), { code: 'public_inventory_changed' });
  fs.chmodSync(path.join(result.root, 'index.html'), 0o644);
  assert.throws(() => verifyInstalledRelease(result.root, release.id), { code: 'installed_release_changed' });
});

test('lost execute response is inspected read-only and the same action cannot run again', { skip: !root }, t => {
  const f = fixture(t), value = plan(f.directory);
  fs.writeFileSync(path.join(f.directory, 'apply-' + transaction + '.jsonl'), JSON.stringify({ phase: 'reload-unconfirmed' }) + '\n');
  fs.writeFileSync(f.filename, f.candidate); value.candidate.plainSha256 = sha256(f.candidate); value.overlay.nextSha256 = sha256(f.candidate);
  assert.throws(() => executeHostPlan(value, f.key), { code: 'EEXIST' });
  const state = inspectHostPlan(value); assert.equal(state.diskState, 'candidate'); assert.equal(state.signalsSent, 0);
  assert.equal(state.activeReloadVerified, false); assert.deepEqual(state.phases, ['reload-unconfirmed']);
});

test('wrong key/ciphertext and nonexistent exact process leave config untouched', { skip: !root }, t => {
  const f = fixture(t), pins = writeEncryptedConfigs({ ...f, directory: f.directory });
  const release = createUiRelease(fixtureDist(path.join(f.directory, 'input'))), installed = installPublicRelease({ input: path.join(f.directory, 'input'), manifest: release, base: f.directory });
  let index = 1;
  for (const change of [value => {}, value => value.baseline.cipherSha256 = digest, value => value.process.startTicks = '456']) {
    const value = { ...plan(f.directory), ...pins, releaseId: release.id, releaseRoot: installed.root, transaction: String(index++).repeat(32) };
    value.overlay.currentSha256 = pins.baseline.plainSha256; value.overlay.nextSha256 = pins.candidate.plainSha256;
    change(value);
    assert.throws(() => executeHostPlan(value, index === 2 ? randomBytes(32) : f.key), error => ['encrypted_config_invalid', 'encrypted_file_changed', 'caddy_process_changed'].includes(error.code));
    assert.deepEqual(fs.readFileSync(f.filename), f.baseline);
  }
  assert.throws(() => verifyCaddyProcess(plan(f.directory)), { code: 'caddy_process_changed' });
});

function publishPlan(f, input, expectedReleaseId, counter = 'a') {
  const release = createUiRelease(input), manifestFile = path.join(f.directory, 'manifest-' + counter + '.json');
  fs.writeFileSync(manifestFile, JSON.stringify({ id: release.id, manifest: release.manifest }));
  return { schema: 'soty.ui.host.v1', action: 'publish', transaction: counter.repeat(32), releaseId: release.id,
    receiptDirectory: f.directory, input, manifestFile, base: f.directory, expectedReleaseId };
}
function publishFixture(t) {
  const f = fixture(t), oldInput = fixtureDist(path.join(f.directory, 'old'));
  const first = publishPlan(f, oldInput, null); executeHostPlan(first);
  const input = fixtureDist(path.join(f.directory, 'next'), { 'index.html': 'NEXT UI', 'assets/new-12345678.js': 'new lazy asset' });
  return { ...f, oldInput, first, next: publishPlan(f, input, first.releaseId, 'b') };
}

test('publish needs no Caddy/config/key and inspect is read-only; CAS and immutable retention fail closed', { skip: !root }, t => {
  const f = publishFixture(t), current = path.join(f.directory, 'soty-ui/current'), config = fs.readFileSync(f.filename);
  const planFile = path.join(f.directory, 'publish.json'); fs.writeFileSync(planFile, JSON.stringify(f.next));
  const child = spawnSync(process.execPath, [fileURLToPath(new URL('./host-publisher.mjs', import.meta.url)), 'execute', planFile], { encoding: 'utf8', timeout: 10000 });
  assert.equal(child.status, 0, child.stdout); const result = JSON.parse(child.stdout);
  assert.equal(Object.hasOwn(result, 'activeReloadVerified'), false);
  assert.equal(result.currentReleaseId, f.next.releaseId); assert.equal(result.signalsSent, 0); assert.equal(result.reloadRequired, false);
  assert.deepEqual(fs.readFileSync(f.filename), config);
  const before = fs.lstatSync(current), journal = fs.readFileSync(path.join(f.directory, 'publish-' + f.next.transaction + '.jsonl'));
  assert.equal(inspectHostPlan(f.next).state, 'published');
  assert.equal(fs.lstatSync(current).ino, before.ino); assert.deepEqual(fs.readFileSync(path.join(f.directory, 'publish-' + f.next.transaction + '.jsonl')), journal);
  assert.throws(() => executeHostPlan(f.next), { code: 'EEXIST' });
  assert.throws(() => executeHostPlan({ ...f.next, transaction: 'c'.repeat(32) }), { code: 'current_release_changed' });
  const oldWithoutNewAssets = publishPlan(f, f.oldInput, f.next.releaseId, 'd');
  assert.throws(() => executeHostPlan(oldWithoutNewAssets), { code: 'ui_immutable_url_missing' });
  assert.equal(fs.lstatSync(current).ino, before.ino);
});

test('publish rejects tampered release and a current link escaping controlled releases', { skip: !root }, t => {
  const f = publishFixture(t), target = installPublicRelease({ input: f.next.input, manifest: createUiRelease(f.next.input), base: f.directory });
  fs.chmodSync(path.join(target.root, 'index.html'), 0o644); fs.writeFileSync(path.join(target.root, 'index.html'), 'TAMPERED');
  assert.throws(() => executeHostPlan(f.next), { code: 'installed_release_changed' });
  const current = path.join(f.directory, 'soty-ui/current'); assert.equal(fs.readlinkSync(current), 'releases/' + f.first.releaseId);
  fs.unlinkSync(current); fs.symlinkSync(f.oldInput, current);
  assert.throws(() => executeHostPlan({ ...f.next, transaction: 'c'.repeat(32) }), { code: 'current_pointer_invalid' });
  assert.equal(fs.readlinkSync(current), f.oldInput);
});

test('OS lock excludes another real publisher process and releases after success', { skip: !root }, t => {
  const f = publishFixture(t), competing = { ...f.next, transaction: 'c'.repeat(32) }, planFile = path.join(f.directory, 'competing.json');
  fs.writeFileSync(planFile, JSON.stringify(competing));
  const rename = fs.renameSync; let response;
  fs.renameSync = (from, to) => {
    if (to === path.join(f.directory, 'soty-ui/current')) {
      const child = spawnSync(process.execPath, [fileURLToPath(new URL('./host-publisher.mjs', import.meta.url)), 'execute', planFile], { encoding: 'utf8', timeout: 5000 });
      assert.equal(child.status, 1); response = JSON.parse(child.stdout); assert.equal(response.code, 'publish_busy');
    }
    return rename(from, to);
  };
  try { executeHostPlan(f.next); } finally { fs.renameSync = rename; }
  assert.equal(response.ok, false); assert.equal(inspectHostPlan(f.next).state, 'published');
  const rollbackStage = path.join(f.directory, 'rollback');
  const composed = createUiRelease(f.oldInput, [path.join(f.directory, 'soty-ui/releases', f.next.releaseId)]);
  // Rollback changes mutable UI while retaining every URL used by newer tabs.
  writeUiRelease(composed, rollbackStage);
  const rollback = publishPlan(f, rollbackStage, f.next.releaseId, 'd');
  assert.equal(executeHostPlan(rollback).published, true); assert.equal(inspectHostPlan(rollback).state, 'published');
  assert.equal(fs.readFileSync(path.join(f.directory, 'soty-ui/current/assets/new-12345678.js'), 'utf8'), 'new lazy asset');
});

for (const position of ['before', 'after']) test(`SIGKILL ${position} atomic rename: inspect reconciles without replay and OS lock is released`, { skip: !root }, t => {
  const f = publishFixture(t), planFile = path.join(f.directory, 'crash.json'); fs.writeFileSync(planFile, JSON.stringify(f.next));
  const script = `import fs from 'node:fs'; import {executeHostPlan} from ${JSON.stringify(new URL('./host-publisher.mjs', import.meta.url).href)};
    const plan=JSON.parse(fs.readFileSync(process.argv[1])); const rename=fs.renameSync;
    fs.renameSync=(from,to)=>{if(to.endsWith('/soty-ui/current') && ${JSON.stringify(position)}==='before')process.kill(process.pid,'SIGKILL');
      const result=rename(from,to); if(to.endsWith('/soty-ui/current'))process.kill(process.pid,'SIGKILL'); return result;}; executeHostPlan(plan);`;
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', script, planFile], { encoding: 'utf8', timeout: 10000 });
  assert.equal(child.signal, 'SIGKILL');
  const state = inspectHostPlan(f.next); assert.equal(state.readOnly, true); assert.equal(state.signalsSent, 0);
  assert.equal(state.state, position === 'before' ? 'not-applied' : 'published');
  assert.deepEqual(state.phases, ['started', 'locked', 'installed', 'switch-intent']);
  assert.throws(() => executeHostPlan(f.next), { code: 'EEXIST' });
  let recovery;
  if (position === 'before') recovery = { ...f.next, transaction: 'd'.repeat(32) };
  else {
    const stage = path.join(f.directory, 'rollback');
    writeUiRelease(createUiRelease(f.oldInput, [path.join(f.directory, 'soty-ui/releases', f.next.releaseId)]), stage);
    recovery = publishPlan(f, stage, f.next.releaseId, 'd');
  }
  assert.equal(executeHostPlan(recovery).published, true);
  assert.equal(inspectHostPlan(recovery).state, 'published');
});
