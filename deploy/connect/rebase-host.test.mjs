import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, readdir, rm, rmdir, cp, unlink, symlink, stat } from 'node:fs/promises';
import { generateKeyPairSync } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';
import os from 'node:os';
import { rebaseHost, main } from './rebase-host.mjs';
import { HostController, moduleTree, runCommand, validateConfig } from './host-controller.mjs';
import { createRelease, canonical, sha256 } from './update-engine.mjs';

const execute = promisify(execFile);
const id = n => n.toString(16).padStart(64, '0');
const imageId = 'sha256:' + id(3);
const pair = generateKeyPairSync('ed25519');
const clone = value => structuredClone(value);
const json = async file => JSON.parse(await readFile(file, 'utf8'));
const missing = async file => { await assert.rejects(stat(file), { code: 'ENOENT' }); };

async function fixture(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-rebase-'));
  t.after(async () => { await rm(root, { recursive: true, force: true }); });
  const oldSource = path.join(root, 'source-old'), sourceRoot = path.join(root, 'source-new');
  const oldState = path.join(root, 'state-old'), stateDir = path.join(root, 'state-new');
  const oldTarget = path.join(oldSource, 'modules', 'connect'), target = path.join(sourceRoot, 'modules', 'connect');
  const oldConfigFile = path.join(root, 'old-config.json'), newConfigFile = path.join(root, 'new-config.json');
  const oldHostFile = path.join(oldState, 'host-state.json'), oldModuleDir = path.join(oldState, 'module-update'), oldModuleFile = path.join(oldModuleDir, 'state.json');
  const releaseDirectory = path.join(root, 'releases'), trustFile = path.join(root, 'trust.json');
  const packageFile = version => JSON.stringify({ name: '@soty/connect', type: 'module', version, connectCompatibility: { protocol: 1, storage: 1, minReader: 1 } });
  for (const [source, version] of [[oldSource, '0.1.0'], [sourceRoot, '0.1.2']]) {
    const module = path.join(source, 'modules', 'connect');
    await mkdir(path.join(module, 'test'), { recursive: true });
    await writeFile(path.join(module, 'package.json'), packageFile(version));
    await writeFile(path.join(module, 'test', 'contract.test.mjs'), "import test from 'node:test'; test('fixture module',()=>{});\n");
    await writeFile(path.join(source, 'host.mjs'), `export const pinned = '${version}';\n`);
    for (const args of [['init', '-q'], ['add', '.'], ['-c', 'core.autocrlf=false', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false', 'commit', '-qm', 'pinned fixture']]) await runCommand(['git', ...args], { cwd: source });
  }
  const head = async source => (await runCommand(['git', 'rev-parse', 'HEAD'], { cwd: source, capture: true })).trim();
  const previousRevision = await head(oldSource), revision = await head(sourceRoot);
  // Both checkouts intentionally differ from HEAD only inside Connect, exactly
  // like an installed signed module and a newly prepared baseline generation.
  await writeFile(path.join(oldTarget, 'package.json'), packageFile('0.1.1'));
  await cp(oldTarget, target, { recursive: true });
  const module = await moduleTree(oldTarget);
  await mkdir(oldModuleDir, { recursive: true });
  await mkdir(releaseDirectory);
  await writeFile(trustFile, JSON.stringify({ keys: { fixture: pair.publicKey.export({ type: 'spki', format: 'pem' }) }, threshold: 1 }));
  const config = validateConfig({
    source: 'https://releases.example.invalid/stable.json', channel: 'stable', deploymentId: 'preserved-deployment',
    trustFile, sourceRoot: oldSource, stateDir: oldState, releaseDirectory, revision: previousRevision,
    runtimeName: 'soty-online-chat', healthOrigin: 'http://127.0.0.1:18182', initialRuntimeHasConnect: false,
    dockerSocket: path.join(root, 'docker.sock'), backupCommand: [process.execPath, path.join(oldSource, 'backup.mjs'), '{containerId}', '--preserved-policy'],
  });
  await writeFile(oldConfigFile, JSON.stringify(config, null, 2) + '\n');
  const release = await createRelease({ directory: oldTarget, privateKey: pair.privateKey, keyId: 'fixture', sequence: 7, expiresAt: '2099-01-01' });
  const host = {
    schema: 'soty.connect.host.v1', revision: previousRevision, sourceRoot: oldSource,
    images: {
      [module.tree]: { image: imageId, version: '0.1.1', hasConnect: true, revision: previousRevision },
      [id(4)]: { image: 'sha256:' + id(5), version: '0.1.0', hasConnect: false, revision: previousRevision, baseline: true },
    },
    active: { tree: module.tree, image: imageId, containerId: id(1) }, transaction: null,
    lastTransaction: { id: 'a'.repeat(32), outcome: 'admitted', backup: { receiptPath: path.join(root, 'backup.receipt.json'), sha256: id(9) } },
  };
  const journal = { format: 1, target: oldTarget, lastSequence: 7, releaseHash: sha256(canonical(release.signed)), version: '0.1.1', previous: oldTarget + '.previous-7', pending: null };
  await writeFile(oldHostFile, JSON.stringify(host, null, 2) + '\n');
  await writeFile(oldModuleFile, JSON.stringify(journal, null, 2) + '\n');
  const labels = { 'org.opencontainers.image.revision': previousRevision, 'io.soty.connect.tree': module.tree };
  const runtime = {
    Id: id(1), Image: imageId, Name: '/soty-online-chat', State: { Running: true, Status: 'running' },
    Config: { Image: imageId, Labels: clone(labels), Env: ['DATA_DIR=/data', 'SYNTHETIC_SECRET=must-not-be-journalled'], User: 'node', WorkingDir: '/app', Cmd: ['node', 'server/index.js'], Volumes: { '/data': {} }, ExposedPorts: { '8080/tcp': {} } },
    HostConfig: { NetworkMode: 'bridge', RestartPolicy: { Name: 'always', MaximumRetryCount: 0 }, PortBindings: { '8080/tcp': [{ HostIp: '127.0.0.1', HostPort: '18182' }] } },
    Mounts: [{ Type: 'volume', Name: 'live-data', Source: '/docker/volumes/live-data', Destination: '/data', RW: true }, { Type: 'bind', Source: releaseDirectory, Destination: '/run/connect-releases', RW: false }],
    NetworkSettings: { Networks: { bridge: { Aliases: ['soty'] } } },
  };
  const image = { Id: imageId, Config: { Labels: clone(labels) } };
  const calls = [];
  const engine = {
    async inspect(key) { calls.push(['inspect', key]); assert.ok([id(1), 'soty-online-chat'].includes(key)); return clone(runtime); },
    async image(key) { calls.push(['image', key]); assert.equal(key, imageId); return clone(image); },
    // Any Docker API mutation is a test failure, even one that a caller catches.
    async request(method) { calls.push(['forbidden', method]); assert.fail('rebase must never call Docker request'); },
    async create() { assert.fail('rebase must never create a container'); },
    async stop() { assert.fail('rebase must never stop a container'); },
    async start() { assert.fail('rebase must never start a container'); },
  };
  let readiness = 0;
  const ready = async options => { readiness++; assert.equal(options.entry.version, '0.1.1'); assert.equal(options.maintenance, false); return { modelsHash: id(7), policyHash: null }; };
  const options = { oldConfigFile, newConfigFile, sourceRoot, stateDir, revision };
  return { root, options, config, oldSource, oldState, oldTarget, target, oldHostFile, oldModuleFile, oldModuleDir, module, host, journal, runtime, image, release, calls, deps: { engine, ready }, readiness: () => readiness };
}

test('rebase publishes complete journals, preserves sequence and old bytes, and maps the actual older image revision', async t => {
  const f = await fixture(t);
  const before = await Promise.all([f.options.oldConfigFile, f.oldHostFile, f.oldModuleFile].map(file => readFile(file)));
  const runtimeBefore = clone(f.runtime);
  const result = await rebaseHost({ ...f.options, appOriginTemplate: 'https://{appId}.soty.example.org' }, f.deps);
  assert.equal(result.status, 'prepared');
  assert.equal(result.revision, f.options.revision); assert.equal(result.baselineRevision, f.config.revision);
  assert.notEqual(result.revision, result.baselineRevision);
  assert.equal(result.lastSequence, 7); assert.equal(result.releaseHash, f.journal.releaseHash);
  assert.deepEqual(f.runtime, runtimeBefore); assert.ok(f.readiness() > 0);
  assert.ok(f.calls.every(([verb]) => ['inspect', 'image'].includes(verb)));
  const config = await json(f.options.newConfigFile);
  assert.deepEqual(config, { ...f.config, sourceRoot: f.options.sourceRoot, stateDir: f.options.stateDir, revision: f.options.revision, initialRuntimeHasConnect: true, storageProbeImage: imageId, appOriginTemplate: 'https://{appId}.soty.example.org' });
  const host = await json(path.join(config.stateDir, 'host-state.json'));
  assert.deepEqual(host.images, { [f.module.tree]: { ...f.host.images[f.module.tree], baseline: true } });
  assert.deepEqual(host.active, f.host.active); assert.equal(host.transaction, null); assert.equal(host.lastTransaction, undefined);
  const module = await json(path.join(config.stateDir, 'module-update', 'state.json'));
  assert.deepEqual(module, { ...f.journal, target: f.target, previous: null });
  assert.equal(result.priorHostStateSha256, sha256(before[1]));
  for (const [index, file] of [f.options.oldConfigFile, f.oldHostFile, f.oldModuleFile].entries()) assert.ok((await readFile(file)).equals(before[index]));
  await missing(path.join(f.oldState, 'host-controller.lock')); await missing(path.join(f.oldModuleDir, 'update.lock'));
  assert.doesNotMatch(JSON.stringify({ result, config, host, module }), /must-not-be-journalled|SYNTHETIC_SECRET/);
  if (process.platform !== 'win32') assert.equal((await stat(f.options.newConfigFile)).mode & 0o777, 0o600);
  // New generation can load and verify the baseline without creating state or
  // confusing source revision with the immutable image's older build revision.
  const controller = new HostController(config, f.deps);
  await controller.load(); await controller.verifyActive(); await controller.checkSource();
});

test('migrated journal still rejects an older signed release through the real controller and update engine', async t => {
  const f = await fixture(t);
  await rebaseHost(f.options, f.deps);
  const release = await createRelease({ directory: f.oldTarget, privateKey: pair.privateKey, keyId: 'fixture', sequence: 6, expiresAt: '2099-01-01' });
  const config = await json(f.options.newConfigFile);
  const controller = new HostController(config, { ...f.deps, fetch: async () => release });
  await assert.rejects(controller.run(), { code: 'release_rollback' });
  assert.equal((await json(path.join(config.stateDir, 'module-update', 'state.json'))).lastSequence, 7);
  assert.deepEqual(await moduleTree(f.target), f.module);
  assert.ok(f.calls.every(([verb]) => ['inspect', 'image'].includes(verb)));
});

test('pending host or module recovery blocks migration without publishing a new generation', async t => {
  const f = await fixture(t);
  for (const [file, value] of [[f.oldHostFile, { ...f.host, transaction: { phase: 'starting', nextId: id(2) } }], [f.oldModuleFile, { ...f.journal, pending: { stage: f.oldTarget + '.stage-8', backup: f.oldTarget + '.previous-8' } }]]) {
    const before = await readFile(file);
    await writeFile(file, JSON.stringify(value));
    await assert.rejects(rebaseHost(f.options, f.deps), { code: 'rebase_pending_recovery_required' });
    await missing(f.options.newConfigFile); await missing(f.options.stateDir);
    assert.equal(await readFile(file, 'utf8'), JSON.stringify(value));
    await writeFile(file, before);
  }
});

test('refuses baseline drift in either old active module or new baseline', async t => {
  const f = await fixture(t);
  for (const [target, code] of [[f.oldTarget, 'rebase_active_tree_changed'], [f.target, 'rebase_baseline_mismatch']]) {
    const file = path.join(target, 'changed.mjs'); await writeFile(file, 'export const changed = true;');
    await assert.rejects(rebaseHost(f.options, f.deps), { code });
    await missing(f.options.newConfigFile); await missing(f.options.stateDir);
    await unlink(file);
  }
});

test('validates actual pinned HEAD and rejects host changes outside Connect in either source', async t => {
  const f = await fixture(t);
  await assert.rejects(rebaseHost({ ...f.options, revision: 'f'.repeat(40) }, f.deps), { code: 'source_revision_changed' });
  for (const source of [f.oldSource, f.options.sourceRoot]) {
    const file = path.join(source, 'unreviewed.txt'); await writeFile(file, 'unexpected host change');
    await assert.rejects(rebaseHost(f.options, f.deps), { code: 'source_host_changed' });
    await unlink(file);
  }
  await missing(f.options.newConfigFile); await missing(f.options.stateDir);
});

test('refuses changed running container, image labels, module labels, revision, and health binding', async t => {
  const f = await fixture(t), original = clone(f.runtime), labels = clone(f.image.Config.Labels);
  const variants = [
    [() => { f.runtime.State.Running = false; }, 'rebase_active_runtime_changed'],
    [() => { f.runtime.Image = 'sha256:' + id(8); }, 'rebase_active_runtime_changed'],
    [() => { f.runtime.Name = '/foreign'; }, 'rebase_active_runtime_changed'],
    [() => { f.runtime.Config.Labels['io.soty.connect.tree'] = id(8); }, 'rebase_image_identity_changed'],
    [() => { f.image.Config.Labels['io.soty.connect.tree'] = id(8); }, 'rebase_image_identity_changed'],
    [() => { f.image.Config.Labels['org.opencontainers.image.revision'] = f.options.revision; }, 'rebase_image_identity_changed'],
    [() => { f.runtime.HostConfig.PortBindings['8080/tcp'][0].HostPort = '18183'; }, 'rebase_health_binding_mismatch'],
  ];
  for (const [mutate, code] of variants) {
    mutate(); await assert.rejects(rebaseHost(f.options, f.deps), { code });
    Object.assign(f.runtime, clone(original)); f.image.Config.Labels = clone(labels);
  }
  await missing(f.options.newConfigFile); await missing(f.options.stateDir);
});

test('never steals an existing host or module lock and releases its own first lock if the second is busy', async t => {
  const f = await fixture(t), hostLock = path.join(f.oldState, 'host-controller.lock'), moduleLock = path.join(f.oldModuleDir, 'update.lock');
  for (const [file, code] of [[hostLock, 'rebase_host_locked'], [moduleLock, 'rebase_module_locked']]) {
    await writeFile(file, 'other-owner');
    await assert.rejects(rebaseHost(f.options, f.deps), { code });
    assert.equal(await readFile(file, 'utf8'), 'other-owner');
    if (file === moduleLock) await missing(hostLock);
    await unlink(file);
  }
  await missing(f.options.newConfigFile); await missing(f.options.stateDir);
});

test('a concurrent rebase cannot observe or reset the sequence while the first owns both locks', async t => {
  const f = await fixture(t);
  let reached; const waiting = new Promise(resolve => { reached = resolve; });
  let resume; const gate = new Promise(resolve => { resume = resolve; });
  const first = rebaseHost(f.options, { ...f.deps, ready: async options => { await f.deps.ready(options); reached(); await gate; } });
  await waiting;
  try {
    await assert.rejects(rebaseHost(f.options, f.deps), { code: 'rebase_host_locked' });
    await missing(f.options.newConfigFile); await missing(f.options.stateDir);
  } finally { resume(); }
  assert.equal((await first).lastSequence, 7);
});

test('existing state or config cannot be replaced, including a config created during readiness', async t => {
  const f = await fixture(t);
  await mkdir(f.options.stateDir);
  await writeFile(path.join(f.options.stateDir, 'preserved'), 'owner');
  await assert.rejects(rebaseHost(f.options, f.deps), { code: 'rebase_state_exists' });
  assert.equal(await readFile(path.join(f.options.stateDir, 'preserved'), 'utf8'), 'owner');
  await unlink(path.join(f.options.stateDir, 'preserved'));
  await rmdir(f.options.stateDir);
  await writeFile(f.options.newConfigFile, 'other-config');
  await assert.rejects(rebaseHost(f.options, f.deps), { code: 'rebase_config_exists' });
  assert.equal(await readFile(f.options.newConfigFile, 'utf8'), 'other-config');
  await unlink(f.options.newConfigFile);
  await assert.rejects(rebaseHost(f.options, { ...f.deps, ready: async () => { await writeFile(f.options.newConfigFile, 'raced-config'); } }), { code: 'rebase_config_exists' });
  assert.equal(await readFile(f.options.newConfigFile, 'utf8'), 'raced-config');
  await missing(f.options.stateDir);
});

test('does not reset or accept invalid sequence, release hash, version or target binding', async t => {
  const f = await fixture(t);
  for (const change of [{ lastSequence: 0 }, { lastSequence: Number.MAX_SAFE_INTEGER + 1 }, { releaseHash: '' }, { version: '0.1.0' }, { target: f.target }]) {
    await writeFile(f.oldModuleFile, JSON.stringify({ ...f.journal, ...change }));
    await assert.rejects(rebaseHost(f.options, f.deps), { code: change.version ? 'rebase_baseline_mapping_invalid' : 'rebase_module_state_invalid' });
  }
  await missing(f.options.newConfigFile); await missing(f.options.stateDir);
});

test('rechecks trees and journals after readiness and preserves external changes for operator review', async t => {
  const f = await fixture(t);
  const added = path.join(f.target, 'unexpected.mjs');
  await assert.rejects(rebaseHost(f.options, { ...f.deps, ready: async () => { await writeFile(added, 'changed'); } }), { code: 'rebase_baseline_mismatch' });
  await unlink(added);
  const changedJournal = JSON.stringify({ ...f.journal, lastSequence: 8 });
  await assert.rejects(rebaseHost(f.options, { ...f.deps, ready: async () => { await writeFile(f.oldModuleFile, changedJournal); } }), { code: 'rebase_state_changed' });
  assert.equal(await readFile(f.oldModuleFile, 'utf8'), changedJournal);
  await missing(f.options.newConfigFile); await missing(f.options.stateDir);
});

test('readiness failure leaves both old journals intact and publishes nothing', async t => {
  const f = await fixture(t), before = await readFile(f.oldModuleFile);
  await assert.rejects(rebaseHost(f.options, { ...f.deps, ready: async () => { throw Object.assign(new Error('not ready'), { code: 'readiness_deadline' }); } }), { code: 'readiness_deadline' });
  assert.ok((await readFile(f.oldModuleFile)).equals(before));
  await missing(f.options.newConfigFile); await missing(f.options.stateDir);
  await missing(path.join(f.oldState, 'host-controller.lock'));
});

test('failure after preparing both journals leaves an unpublished generation and never retries over it', async t => {
  const f = await fixture(t);
  let commands = 0;
  const command = async (argv, options) => {
    // Each complete snapshot checks HEAD and status in both real repositories.
    // The third snapshot runs after both new journals have been synced.
    if (++commands === 9) {
      await missing(f.options.newConfigFile);
      assert.equal((await json(path.join(f.options.stateDir, 'module-update', 'state.json'))).lastSequence, 7);
      assert.equal((await json(path.join(f.options.stateDir, 'host-state.json'))).active.image, imageId);
      throw Object.assign(new Error('interrupted before publication'), { code: 'injected_verification_failure' });
    }
    return runCommand(argv, options);
  };
  const before = await readFile(f.oldModuleFile);
  await assert.rejects(rebaseHost(f.options, { ...f.deps, command }), { code: 'injected_verification_failure' });
  await missing(f.options.newConfigFile);
  assert.ok((await readFile(f.oldModuleFile)).equals(before));
  await assert.rejects(rebaseHost(f.options, f.deps), { code: 'rebase_state_exists' });
  await missing(path.join(f.oldState, 'host-controller.lock'));
});

test('new paths stay outside source, module, old journal and feed trees; symlink aliases are rejected', async t => {
  const f = await fixture(t);
  for (const stateDir of [path.join(f.oldState, 'nested'), path.join(f.oldSource, 'new-state'), path.join(f.config.releaseDirectory, 'new-state')]) {
    await assert.rejects(rebaseHost({ ...f.options, stateDir }, f.deps), { code: 'rebase_paths_overlap' });
  }
  await assert.rejects(rebaseHost({ ...f.options, newConfigFile: path.join(f.target, 'config.json') }, f.deps), { code: 'rebase_config_must_be_external' });
  const alias = path.join(f.root, 'alias');
  await symlink(f.oldState, alias, process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(rebaseHost({ ...f.options, stateDir: path.join(alias, 'nested') }, f.deps), { code: 'rebase_symlink' });
  await missing(f.options.newConfigFile); await missing(f.options.stateDir);
});

test('preserves an existing application origin, rejects invalid override, and validates CLI without echoing config', async t => {
  const f = await fixture(t);
  f.config.appOriginTemplate = 'https://{appId}.existing.example.org';
  await writeFile(f.options.oldConfigFile, JSON.stringify(f.config));
  await assert.rejects(rebaseHost({ ...f.options, appOriginTemplate: 'https://user:synthetic-secret@example.org' }, f.deps), { code: 'config_app_origin_invalid' });
  await rebaseHost(f.options, f.deps);
  assert.equal((await json(f.options.newConfigFile)).appOriginTemplate, f.config.appOriginTemplate);
  await assert.rejects(main(['--unknown', 'synthetic-secret']), { code: 'rebase_usage_invalid' });
  await assert.rejects(main(['--old-config', 'relative.json', '--new-config', '/x', '--source-root', '/y', '--state-dir', '/z', '--revision', 'a'.repeat(40)]), { code: 'rebase_absolute_paths_required' });
  const script = path.join(import.meta.dirname, 'rebase-host.mjs');
  const result = await execute(process.execPath, [script, '--unknown', 'synthetic-secret'], { windowsHide: true }).then(() => assert.fail('invalid CLI must fail'), error => error);
  assert.equal(result.code, 1);
  assert.deepEqual(JSON.parse(result.stderr.trim()), { ok: false, code: 'rebase_usage_invalid' });
  assert.doesNotMatch(result.stdout + result.stderr, /synthetic-secret/);
  assert.deepEqual((await readdir(f.options.stateDir)).sort(), ['host-state.json', 'module-update']);
});
