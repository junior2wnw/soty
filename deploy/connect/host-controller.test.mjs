import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, unlink, rename } from 'node:fs/promises';
import { generateKeyPairSync } from 'node:crypto';
import path from 'node:path';
import os from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { migrateAppsSchema } from '../../modules/apps/server/schema.mjs';
import { HostController, candidateConfig, moduleTree, validateConfig, productionReady, atomicState, originalPreservationHash } from './host-controller.mjs';
import { createRelease, recoverRelease } from '../../modules/connect/update/index.mjs';
import { storageReaderLabel, currentStorageReaders } from '../connector/storage-guard.mjs';
import { readStorageFormat } from '../connector/storage-probe.mjs';
import { createHistoricalAppsV2 } from '../connector/apps-v2.fixture.mjs';
import { createHistoricalAppsV3 } from '../connector/apps-v3.fixture.mjs';
import { createHistoricalAppsV4 } from '../connector/apps-v4.fixture.mjs';
import { createHistoricalAppsV5 } from '../connector/apps-v5.fixture.mjs';

const id = n => n.toString(16).padStart(64, '0');
const image = n => 'sha256:' + id(n);
const revision = 'a'.repeat(40);
const clones = value => structuredClone(value);
const compatibility = { protocol: 1, storage: 1, minReader: 1 };
const pair = generateKeyPairSync('ed25519');

function original() {
  return { Id: id(1), Image: image(1), Name: '/soty-online-chat', State: { Running: true, Status: 'running' },
    Config: { Image: image(1), Env: ['DATA_DIR=/data', 'SECRET_TEST=must-never-reach-journal', 'SOTY_CONNECT_ORIGINS=https://other.example'], User: 'node', WorkingDir: '/app', Cmd: ['node', 'server/index.js'], Labels: { 'org.opencontainers.image.revision': 'b'.repeat(40) }, Volumes: { '/data': {} }, ExposedPorts: { '8080/tcp': {} } },
    HostConfig: { Binds: ['/host/config:/run/config:ro'], NetworkMode: 'bridge', RestartPolicy: { Name: 'always', MaximumRetryCount: 0 }, PortBindings: { '8080/tcp': [{ HostIp: '127.0.0.1', HostPort: '18182' }] }, Memory: 123456, CapDrop: ['ALL'], OomKillDisable: false },
    Mounts: [{ Type: 'volume', Name: 'existing-data', Source: '/docker/volumes/existing-data', Destination: '/data', RW: true }, { Type: 'bind', Source: '/host/config', Destination: '/run/config', RW: false }],
    NetworkSettings: { Networks: { bridge: { Aliases: ['soty'], DriverOpts: { keep: 'yes' } }, custom: { IPAMConfig: { IPv4Address: '172.20.0.9' }, Aliases: ['custom-soty'] } } } };
}

class Engine {
  constructor() { this.items = new Map([[id(1), original()]]); this.images = new Map([[image(1), { Id: image(1), Config: { Labels: { [storageReaderLabel]: currentStorageReaders } } }]]); this.events = []; this.next = 2; this.drop = null; this.ignore = null; }
  async inspect(key) { const value = this.items.get(key) || [...this.items.values()].find(c => c.Name === '/' + key); if (!value) throw Object.assign(new Error('missing'), { code: 'engine_http_404' }); return clones(value); }
  async image(key) { if (!this.images.has(key)) throw new Error('image missing'); return clones(this.images.get(key)); }
  async create(name, config) {
    assert.ok(![...this.items.values()].some(c => c.Name === '/' + name));
    const { HostConfig, NetworkingConfig, ...Config } = clones(config), key = id(this.next++);
    // Docker merges image labels into the container even when create supplies labels.
    Config.Labels = { ...(this.images.get(Config.Image)?.Config?.Labels || {}), ...(Config.Labels || {}) };
    const mounts = (HostConfig.Mounts || []).map(m => ({ Type: m.Type, ...(m.Type === 'volume' ? { Name: m.Source } : {}), Source: m.Type === 'volume' ? '/docker/volumes/' + m.Source : m.Source, Destination: m.Target, RW: !m.ReadOnly }));
    for (const bind of HostConfig.Binds || []) { const [Source, Destination, flags] = bind.split(':'); mounts.push({ Type: 'bind', Source, Destination, RW: flags !== 'ro' }); }
    this.items.set(key, { Id: key, Image: Config.Image, Name: '/' + name, Config, HostConfig, Mounts: mounts, NetworkSettings: { Networks: NetworkingConfig.EndpointsConfig }, State: { Running: false, Status: 'created' } });
    this.events.push('create'); return { Id: key };
  }
  async stop(key) { this.events.push('stop:' + key); if (this.ignore === 'stop') return; const c = this.items.get(key); c.State = { Running: false, Status: 'exited', ExitCode: 0 }; for (const endpoint of Object.values(c.NetworkSettings.Networks)) delete endpoint.MacAddress; if (this.drop === 'stop') throw new Error('lost'); }
  async start(key) { this.events.push('start:' + key); if (this.ignore === 'start') return; assert.ok(![...this.items.values()].some(c => c.Id !== key && c.State.Running), 'two application writers'); const c = this.items.get(key); c.State = { Running: true, Status: 'running' }; if (this.drop === 'start') throw new Error('lost'); }
  async rename(key, name) { this.events.push('rename'); assert.ok(![...this.items.values()].some(c => c.Id !== key && c.Name === '/' + name)); this.items.get(key).Name = '/' + name; }
  async request(method, route, body) { if (method === 'GET' && route.startsWith('/volumes/')) { const Name = decodeURIComponent(route.slice('/volumes/'.length)); const mount = [...this.items.values()].flatMap(c => c.Mounts || []).find(m => m.Name === Name); return { Name, Driver: 'local', Scope: 'local', Options: null, Mountpoint: mount?.Source }; } if (method === 'GET' && route === '/containers/json') return clones([...this.items.values()].filter(c => c.State.Running)); assert.equal(method, 'POST'); const match = route.match(/^\/containers\/([a-f0-9]{64})\/update$/); assert.ok(match); this.items.get(match[1]).HostConfig.RestartPolicy = clones(body.RestartPolicy); this.events.push('policy:' + match[1]); return {}; }
}

async function fixture(options = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-host-')), sourceRoot = path.join(root, 'source'), stateDir = path.join(root, 'state'), releaseDirectory = path.join(root, 'releases');
  const target = path.join(sourceRoot, 'modules', 'connect'), candidate = path.join(root, 'candidate');
  for (const [directory, version] of [[target, '0.1.0'], [candidate, '0.1.1']]) {
    await mkdir(path.join(directory, 'test'), { recursive: true });
    await writeFile(path.join(directory, 'package.json'), JSON.stringify({ name: '@soty/connect', version, type: 'module', connectCompatibility: compatibility }));
    await writeFile(path.join(directory, 'test', 'small.test.mjs'), "import test from 'node:test';test('signed module candidate',()=>{});");
  }
  await mkdir(releaseDirectory);
  const trustFile = path.join(root, 'trust.json'); await writeFile(trustFile, JSON.stringify({ keys: { test: pair.publicKey.export({ type: 'spki', format: 'pem' }) }, threshold: 1 }));
  const release = await createRelease({ directory: candidate, privateKey: pair.privateKey, keyId: 'test', sequence: 1, expiresAt: '2099-01-01' });
  const bootstrapRelease = path.join(releaseDirectory, 'initial.json'); await writeFile(bootstrapRelease, JSON.stringify(release));
  const config = { source: 'https://soty.example/releases/stable.json', trustFile, sourceRoot, stateDir, releaseDirectory, revision, storageProbeImage: image(1), runtimeName: 'soty-online-chat', healthOrigin: 'http://127.0.0.1:18182', initialRuntimeHasConnect: false, dockerSocket: path.join(root, 'docker.sock'), backupCommand: [process.execPath, path.join(sourceRoot, 'backup.mjs'), '{containerId}'] };
  const engine = new Engine(); let marker = false, owner = false, fetches = 0, backups = 0;
  const deps = {
    engine, pause: async () => {}, checkSource: async () => {}, fetch: async () => { fetches++; return release; },
    storageProbe: async () => ({ ok: true, schema: 'soty.storage-format.v3', notes: 'empty', capabilities: 'empty', rooms: 1, apps: 'empty' }),
    ready: async ({ entry, maintenance }) => { if (options.healthFailure && entry.hasConnect) throw Object.assign(new Error('unsafe raw details'), { code: 'readiness_deadline' }); assert.equal(marker, maintenance); return { modelsHash: 'c'.repeat(64), policyHash: null }; },
    probe: async (verb, runtime) => {
      engine.events.push('probe:' + verb); if (verb === 'enter') { assert.equal(runtime.State.Running, false); marker = true; owner = true; }
      if (verb === 'leave') { marker = false; owner = false; }
      const count = options.offlineRace && !runtime.State.Running && !marker ? 1 : 0;
      return { ok: true, schema: 'soty.connect.maintenance.v1', count, maintenance: options.externalMaintenance || marker, owned: owner };
    },
    command: async (argv, details) => {
      if (argv[0] === 'docker') {
        assert.equal(details.timeoutMs, 1200000); const file = argv[argv.indexOf('--iidfile') + 1];
        const tree = argv[argv.indexOf('--label') + 1].split('=')[1];
        engine.images.set(image(2), { Id: image(2), Config: { Labels: { 'org.opencontainers.image.revision': revision, 'io.soty.connect.tree': tree, [storageReaderLabel]: currentStorageReaders } } }); await writeFile(file, image(2)); return;
      }
      backups++; engine.events.push('backup'); assert.equal((await engine.inspect(argv[2])).State.Running, false);
      if (options.backupFailure) throw Object.assign(new Error('secret backup detail'), { code: 'command_failed' });
      return JSON.stringify({ ok: true, encrypted: true, receiptPath: path.join(root, 'backup.receipt.json'), sha256: 'd'.repeat(64) });
    },
  };
  const create = () => new HostController(config, deps);
  return { root, target, config, engine, deps, create, bootstrapRelease, counters: () => ({ fetches, backups }), setMarker: value => { marker = value; }, readState: async () => JSON.parse(await readFile(path.join(stateDir, 'host-state.json'), 'utf8')) };
}

test('candidate preserves runtime fields, realised volumes and networks with only feed additions', async () => {
  const f = await fixture(); const old = original(), copy = clones(old), result = candidateConfig(old, image(2), 'a'.repeat(32), f.config);
  assert.deepEqual(old, copy); assert.equal(result.HostConfig.Memory, old.HostConfig.Memory);
  assert.deepEqual(result.HostConfig.PortBindings, old.HostConfig.PortBindings);
  assert.deepEqual(result.NetworkingConfig.EndpointsConfig.custom, old.NetworkSettings.Networks.custom);
  assert.ok(result.HostConfig.Mounts.some(m => m.Type === 'volume' && m.Source === 'existing-data' && m.Target === '/data'));
  assert.ok(result.HostConfig.Mounts.some(m => m.Source === f.config.releaseDirectory && m.ReadOnly && m.Target === '/run/connect-releases'));
  assert.ok(result.Env.includes('SECRET_TEST=must-never-reach-journal'));
  assert.ok(result.Env.includes('SOTY_CONNECT_RELEASE_DIR=/run/connect-releases'));
  assert.ok(result.Env.find(x => x.startsWith('SOTY_CONNECT_ORIGINS=')).includes('https://xn--n1afe0b.online,https://soty.pochinit.online'));
  old.Mounts.push({ Source: '/var/run/docker.sock', Destination: '/var/run/docker.sock' });
  assert.throws(() => candidateConfig(old, image(2), 'a'.repeat(32), f.config), /socket_forbidden/);
});

test('host can explicitly configure an isolated HTTPS application origin without changing credentials', async () => {
  const f = await fixture();
  const config = validateConfig({ ...f.config, appOriginTemplate: 'https://{appId}.soty.example.org' });
  const result = candidateConfig(original(), image(2), 'a'.repeat(32), config);
  assert.ok(result.Env.includes('SOTY_APP_ORIGIN_TEMPLATE=https://{appId}.soty.example.org'));
  assert.ok(result.Env.includes('SECRET_TEST=must-never-reach-journal'));
  for (const invalid of ['http://{appId}.example.org', 'https://example.org/{appId}', 'https://{appId}.example.org/','https://user:password@{appId}.example.org']) assert.throws(() => validateConfig({ ...f.config, appOriginTemplate: invalid }), /config_app_origin_invalid/);
});

test('original fingerprint tolerates only observed endpoint MAC clearing and guards explicit requested settings', () => {
  const live = original(), tx = 'e'.repeat(32);
  live.NetworkSettings.Networks.bridge.MacAddress = '02:42:ac:11:00:02';
  const stopped = clones(live); delete stopped.NetworkSettings.Networks.bridge.MacAddress;
  assert.equal(originalPreservationHash(live, image(1), tx), originalPreservationHash(stopped, image(1), tx));
  live.Config.MacAddress = '02:42:ac:11:00:03'; stopped.Config.MacAddress = live.Config.MacAddress;
  assert.equal(originalPreservationHash(live, image(1), tx), originalPreservationHash(stopped, image(1), tx));
  stopped.Config.MacAddress = '02:42:ac:11:00:04';
  assert.notEqual(originalPreservationHash(live, image(1), tx), originalPreservationHash(stopped, image(1), tx));
  stopped.Config.MacAddress = live.Config.MacAddress; stopped.NetworkSettings.Networks.custom.IPAMConfig.IPv4Address = '172.20.0.10';
  assert.notEqual(originalPreservationHash(live, image(1), tx), originalPreservationHash(stopped, image(1), tx));
});

test('activation passes the stopped-original guard when Docker clears its assigned endpoint MAC', async () => {
  const f = await fixture(); f.engine.items.get(id(1)).NetworkSettings.Networks.bridge.MacAddress = '02:42:ac:11:00:02';
  assert.equal((await f.create().run()).status, 'updated');
  assert.equal(f.counters().backups, 1);
  assert.equal((await f.engine.inspect(id(1))).NetworkSettings.Networks.bridge.MacAddress, undefined);
  assert.equal((await f.engine.inspect('soty-online-chat')).NetworkSettings.Networks.bridge.MacAddress, '02:42:ac:11:00:02');
});

test('candidate explicitly pins inherited image module-tree label and rejects a changed tree', async () => {
  const f = await fixture(), tree = 'f'.repeat(64);
  const config = candidateConfig(original(), image(2), 'a'.repeat(32), f.config, tree);
  assert.equal(config.Labels['io.soty.connect.tree'], tree);
  assert.throws(() => candidateConfig(original(), image(2), 'a'.repeat(32), f.config, 'invalid'), /candidate_tree_invalid/);
  let prepared;
  f.deps.write = async (file, value) => { if (value.transaction?.phase === 'prepared') prepared = clones(value); return atomicState(file, value); };
  await f.create().run();
  const state = await f.readState(), candidate = await f.engine.inspect(state.active.containerId);
  assert.equal(candidate.Config.Labels['io.soty.connect.tree'], state.active.tree);
  const controller = f.create(); controller.state = prepared; controller.validateCandidate(candidate);
  candidate.Config.Labels['io.soty.connect.tree'] = '0'.repeat(64);
  assert.throws(() => controller.validateCandidate(candidate), /candidate_identity_changed/);
});

test('full bootstrap builds and maps before sole-writer switch, encrypted backup, no journal secrets', async () => {
  const f = await fixture(), before = await moduleTree(f.target);
  assert.deepEqual(await f.create().run({ bootstrapRelease: f.bootstrapRelease }), { ok: true, status: 'updated', version: '0.1.1' });
  assert.deepEqual(f.counters(), { fetches: 0, backups: 1 });
  const state = await f.readState(); assert.equal(state.transaction, null); assert.equal(state.lastTransaction.outcome, 'admitted');
  assert.equal(state.images[before.tree].image, image(1)); assert.equal(state.images[before.tree].hasConnect, false);
  assert.equal(state.images[state.active.tree].image, image(2)); assert.notEqual(before.tree, state.active.tree);
  assert.equal((await f.engine.inspect(id(1))).HostConfig.RestartPolicy.Name, 'no');
  assert.equal((await f.engine.inspect('soty-online-chat')).HostConfig.RestartPolicy.Name, 'always');
  const events = f.engine.events; assert.ok(events.indexOf('stop:' + id(1)) < events.indexOf('backup')); assert.ok(events.indexOf('backup') < events.indexOf('probe:enter')); assert.ok(events.indexOf('probe:enter') < events.indexOf('start:' + id(2)));
  assert.ok(!JSON.stringify(state).includes('SECRET_TEST')); assert.ok(!JSON.stringify(state).includes('must-never'));
  await assert.rejects(f.create().run({ bootstrapRelease: f.bootstrapRelease }), /bootstrap_not_initial/);
});

test('candidate health failure restores old immutable image on latest data without legacy rollback', async () => {
  const f = await fixture({ healthFailure: true }); const data = path.join(f.root, 'latest-data'); await writeFile(data, 'durable latest sqlite data');
  await assert.rejects(f.create().run(), /readiness_deadline/);
  assert.equal((await f.engine.inspect('soty-online-chat')).Image, image(1));
  assert.equal((await f.engine.inspect(id(2))).State.Running, false); assert.equal((await f.engine.inspect(id(2))).HostConfig.RestartPolicy.Name, 'no');
  assert.equal((await f.engine.inspect(id(1))).HostConfig.RestartPolicy.Name, 'always');
  assert.equal((await f.readState()).lastTransaction.outcome, 'restored');
  assert.equal(JSON.parse(await readFile(path.join(f.target, 'package.json'), 'utf8')).version, '0.1.0');
  assert.equal(await readFile(data, 'utf8'), 'durable latest sqlite data'); assert.ok(!f.engine.events.some(x => x.includes('rollback')));
});

test('rollback fences automatic daemon restart before stopping the candidate', async () => {
  const f = await fixture({ healthFailure: true });
  const stop = f.engine.stop.bind(f.engine);
  f.engine.stop = async key => {
    await stop(key);
    if (key === id(2) && f.engine.items.get(key).HostConfig.RestartPolicy.Name === 'always') {
      f.engine.items.get(key).State = { Running: true, Status: 'running' };
    }
  };
  await assert.rejects(f.create().run(), /readiness_deadline/);
  assert.equal((await f.engine.inspect(id(1))).State.Running, true);
  assert.equal((await f.engine.inspect(id(2))).State.Running, false);
  assert.ok(f.engine.events.indexOf('policy:' + id(2)) < f.engine.events.indexOf('stop:' + id(2)));
});

test('interrupted rollback resumes after its journaled candidate restart-policy change', async () => {
  const f = await fixture({ healthFailure: true }); let interrupted = false;
  f.deps.write = async (file, value) => {
    if (!interrupted && value.transaction?.phase === 'restoring_original') { interrupted = true; throw new Error('simulated process interruption'); }
    return atomicState(file, value);
  };
  await assert.rejects(f.create().run(), /update_recovery_required/);
  const pending = await f.readState();
  assert.equal(pending.transaction.candidateRestartDisabled, true);
  assert.equal((await f.engine.inspect(id(2))).HostConfig.RestartPolicy.Name, 'no');
  assert.equal((await f.engine.inspect(id(1))).State.Running, false);
  f.deps.fetch = async () => {
    const state = await f.readState(); assert.equal(state.transaction, null); assert.equal(state.lastTransaction.outcome, 'restored');
    assert.equal((await f.engine.inspect('soty-online-chat')).Id, id(1));
    throw Object.assign(new Error('test stops after complete recovery'), { code: 'test_recovery_complete' });
  };
  await assert.rejects(f.create().run(), /test_recovery_complete/);
  const state = await f.readState(); assert.equal(state.transaction, null); assert.equal(state.active.image, image(1));
});

test('candidate restart-policy normalization rejects unjournaled and unrelated mutations', async () => {
  const f = await fixture({ healthFailure: true });
  f.deps.write = async (file, value) => {
    if (value.transaction?.phase === 'restoring_original') throw new Error('simulated process interruption');
    return atomicState(file, value);
  };
  await assert.rejects(f.create().run(), /update_recovery_required/);
  const controller = f.create(); controller.state = await f.readState();
  const candidate = await f.engine.inspect(id(2)); controller.validateCandidate(candidate);
  controller.state.transaction.candidateRestartDisabled = false;
  assert.throws(() => controller.validateCandidate(candidate), /candidate_identity_changed/);
  controller.state.transaction.candidateRestartDisabled = true;
  candidate.HostConfig.RestartPolicy = { Name: 'on-failure', MaximumRetryCount: 5 };
  assert.throws(() => controller.validateCandidate(candidate), /candidate_identity_changed/);
});

test('offline admission race restarts original, preserving the newly queued job without a backup or candidate start', async () => {
  const f = await fixture({ offlineRace: true });
  await assert.rejects(f.create().run(), /offline_admission_race/);
  assert.equal((await f.engine.inspect(id(1))).State.Running, true);
  assert.equal((await f.engine.inspect(id(2))).State.Running, false);
  assert.equal(f.counters().backups, 0); assert.ok(!f.engine.events.includes('probe:enter'));
});

test('backup failure restarts original and never starts candidate or mutates maintenance', async () => {
  const f = await fixture({ backupFailure: true }); await assert.rejects(f.create().run(), /command_failed/);
  assert.equal((await f.engine.inspect(id(1))).State.Running, true); assert.equal((await f.engine.inspect(id(2))).State.Running, false);
  assert.ok(!f.engine.events.includes('probe:enter')); assert.ok(!f.engine.events.includes('start:' + id(2)));
});

test('existing maintenance aborts before fetch and does not clear the operator marker', async () => {
  const f = await fixture({ externalMaintenance: true }); await assert.rejects(f.create().run(), /baseline_not_quiescent/);
  assert.equal(f.counters().fetches, 0); assert.ok(!f.engine.events.includes('probe:leave')); assert.ok(!f.engine.events.some(x => x.startsWith('stop:')));
});

test('lost stop response is reconciled from the exact container state without a duplicate stop', async () => {
  const f = await fixture(); f.engine.drop = 'stop'; await f.create().run();
  assert.equal(f.engine.events.filter(x => x === 'stop:' + id(1)).length, 1);
});

test('unresolved start remains journaled and recovery blocks network fetch and second activation', async () => {
  const f = await fixture(); f.engine.ignore = 'start';
  await assert.rejects(f.create().run(), /update_recovery_required/);
  const state = await f.readState(); assert.equal(state.transaction.operation.kind, 'start');
  const fetched = f.counters().fetches, started = f.engine.events.filter(x => x.startsWith('start:')).length;
  await assert.rejects(f.create().run(), /update_recovery_required/);
  assert.equal(f.counters().fetches, fetched); assert.equal(f.engine.events.filter(x => x.startsWith('start:')).length, started);
});

test('stale host lock is never stolen or interpreted as permission to fetch', async () => {
  const f = await fixture(); await mkdir(f.config.stateDir); await writeFile(path.join(f.config.stateDir, 'host-controller.lock'), '{"pid":123}');
  await assert.rejects(f.create().run(), /host_locked_operator_verification_required/); assert.equal(f.counters().fetches, 0);
});

test('readiness requires SQLite endpoint and exact module capabilities, initial baseline may omit Connect', async () => {
  const config = (await fixture()).config; const routes = [];
  const health = { ok: true, agentModelProxy: { ready: true, model: 'm', transport: 'relay' }, applicationModelProxy: { ready: true, model: 'm', transport: 'relay', path: '/api/inference/v1/chat/completions' } };
  const get = async (_origin, route) => { routes.push(route); if (route === '/health') return health; if (route.endsWith('storage-ready')) return { ok: true, schema: 'soty.connector-storage-ready.v1', storageReady: true, maintenance: false }; throw new Error('404'); };
  await productionReady(config, { get, pause: async () => {}, deadlineMs: 10 })({ entry: { hasConnect: false }, maintenance: false });
  assert.ok(!routes.includes('/api/connect/capabilities'));
  health.agentModelProxy.providers = [{ active: 1, queued: 0 }];
  await assert.rejects(productionReady(config, { get, pause: async () => {}, deadlineMs: 10 })({ entry: { hasConnect: false }, maintenance: false, idle: true }), /readiness_deadline/);
  health.agentModelProxy.providers[0].active = 0;
  await productionReady(config, { get, pause: async () => {}, deadlineMs: 10 })({ entry: { hasConnect: false }, maintenance: false, idle: true });
  await assert.rejects(productionReady(config, { get, pause: async () => {}, deadlineMs: 10 })({ entry: { hasConnect: true, version: '0.1.1' }, maintenance: false }), /readiness_deadline/);
});

test('recovery API restores interrupted module and activates it without a release or network', async () => {
  const f = await fixture(), stateDir = path.join(f.root, 'updater-recovery'), stage = f.target + '.stage-test', backup = f.target + '.previous-test';
  await mkdir(stateDir); await rename(f.target, backup); await mkdir(f.target);
  await writeFile(path.join(f.target, 'package.json'), '{"name":"@soty/connect","version":"0.1.1"}');
  await writeFile(path.join(stateDir, 'state.json'), JSON.stringify({ format: 1, target: f.target, lastSequence: 0, pending: { stage, backup, nextSequence: 1 } }));
  let activated = false;
  assert.deepEqual(await recoverRelease({ target: f.target, stateDir, activate: async target => { activated = true; assert.equal(JSON.parse(await readFile(path.join(target, 'package.json'), 'utf8')).version, '0.1.0'); } }), { status: 'recovered', restartRequired: false });
  assert.ok(activated); assert.equal(JSON.parse(await readFile(path.join(stateDir, 'state.json'), 'utf8')).pending, null);
  await writeFile(path.join(stateDir, 'update.lock'), 'dead pid');
  await assert.rejects(recoverRelease({ target: f.target, stateDir }), /update_locked/);
  await unlink(path.join(stateDir, 'update.lock'));
});

test('configuration rejects embedded state, unpinned backup executable and non-HTTPS source', async () => {
  const f = await fixture();
  assert.throws(() => validateConfig({ ...f.config, source: 'http://example.org' }), /config_source_invalid/);
  assert.throws(() => validateConfig({ ...f.config, stateDir: path.join(f.config.sourceRoot, 'state') }), /config_state_must_be_external/);
  assert.throws(() => validateConfig({ ...f.config, backupCommand: ['node', 'backup', '{containerId}'] }), /config_backup_command_invalid/);
  assert.throws(() => validateConfig({ ...f.config, runtimeName: 'soty-connect-canary-test' }), /config_runtime_invalid/);
  assert.throws(() => validateConfig({ ...f.config, canary: true, runtimeName: 'soty-connect-canary-test' }), /config_runtime_invalid/);
  assert.equal(validateConfig({ ...f.config, canary: true, runtimeName: 'soty-connect-canary-test', healthOrigin: 'http://127.0.0.1:18183' }).canary, true);
});

test('host recovery engine is pinned with the whole host release and equal to reviewed portable updater', async () => {
  assert.equal(await readFile(new URL('./update-engine.mjs', import.meta.url), 'utf8'), await readFile(new URL('../../modules/connect/update/index.mjs', import.meta.url), 'utf8'));
});

test('v2 candidate failure never restarts JSON-only original or restores its automatic restart policy', async () => {
  const f = await fixture({ healthFailure: true });
  f.engine.images.get(image(1)).Config.Labels[storageReaderLabel] = JSON.stringify({ version: 3, readers: { notes: [1], capabilities: [1], rooms: [1], apps: [1, 2] } });
  f.deps.storageProbe = async () => ({ ok: true, schema: 'soty.storage-format.v3', notes: 'empty', capabilities: 'empty', rooms: f.engine.events.includes('start:' + id(2)) ? 2 : 1, apps: 'empty' });
  const probes = [], probe = f.deps.probe;
  f.deps.probe = async (verb, runtime, ...rest) => { probes.push({ verb, id: runtime.Id, afterCandidate: f.engine.events.includes('start:' + id(2)) }); return probe(verb, runtime, ...rest); };
  await assert.rejects(f.create().run());
  const old = await f.engine.inspect(id(1));
  assert.equal(old.State.Running, false); assert.equal(old.HostConfig.RestartPolicy.Name, 'no');
  assert.ok(!f.engine.events.includes('start:' + id(1)));
  assert.ok(!probes.some(p => p.id === id(1) && p.afterCandidate), 'refuse before old offline status/enter/leave helper');
  assert.ok((await f.readState()).transaction);
});

for (const previous of [2, 3, 4, 5]) test(`real Apps v${previous} to v6 migration after candidate failure blocks an Apps${previous}-only original before any rollback helper or automatic restart`, async () => {
  const f = await fixture(), dataDir = path.join(f.root, 'data'); await mkdir(dataDir);
  await mkdir(path.join(dataDir, 'apps'));
  const file = path.join(dataDir, 'apps', 'registry.sqlite'), originalDb = new DatabaseSync(file);
  try { if (previous === 5) createHistoricalAppsV5(originalDb); else if (previous === 4) createHistoricalAppsV4(originalDb); else if (previous === 3) createHistoricalAppsV3(originalDb); else createHistoricalAppsV2(originalDb); } finally { originalDb.close(); }
  assert.equal((await readStorageFormat(dataDir)).apps, previous);
  f.engine.images.get(image(1)).Config.Labels[storageReaderLabel] = JSON.stringify({ version: 3, readers: { notes: [1], capabilities: [1], rooms: [1, 2], apps: Array.from({ length: previous }, (_, i) => i + 1) } });
  f.engine.items.get(id(1)).Config.Labels[storageReaderLabel] = currentStorageReaders;
  f.deps.storageProbe = async () => readStorageFormat(dataDir);
  const ready = f.deps.ready, probes = [], probe = f.deps.probe;
  let migratedBytes;
  f.deps.ready = async details => {
    if (details.entry.hasConnect) {
      const db = new DatabaseSync(file);
      try { migrateAppsSchema(db); } finally { db.close(); }
      migratedBytes = await readFile(file);
      throw Object.assign(new Error('candidate not ready'), { code: 'readiness_deadline' });
    }
    return ready(details);
  };
  f.deps.probe = async (verb, runtime, ...rest) => {
    probes.push({ verb, id: runtime.Id, migrated: Boolean(migratedBytes) }); return probe(verb, runtime, ...rest);
  };
  await assert.rejects(f.create().run());
  assert.deepEqual(await readStorageFormat(dataDir), { ok: true, schema: 'soty.storage-format.v3', notes: 'empty', capabilities: 'empty', rooms: 'empty', apps: 6 });
  const old = await f.engine.inspect(id(1));
  assert.equal(old.State.Running, false); assert.equal(old.HostConfig.RestartPolicy.Name, 'no');
  assert.ok(!f.engine.events.includes('start:' + id(1)));
  assert.ok(!probes.some(item => item.id === id(1) && item.migrated));
  assert.ok((await f.readState()).transaction);
  const oldStarts = f.engine.events.filter(event => event === 'start:' + id(1)).length;
  await assert.rejects(f.create().run());
  assert.equal(f.engine.events.filter(event => event === 'start:' + id(1)).length, oldStarts);
  assert.equal((await f.engine.inspect(id(1))).HostConfig.RestartPolicy.Name, 'no');
  assert.deepEqual(await readFile(path.join(dataDir, 'apps', 'registry.sqlite')), migratedBytes, 'recovery keeps current data');
});

test('unlabelled candidate fails before stopping original even if its container could inherit a label', async () => {
  const f = await fixture(); const command = f.deps.command;
  f.engine.items.get(id(1)).Config.Labels[storageReaderLabel] = currentStorageReaders;
  f.deps.command = async (...a) => { const result = await command(...a); if (a[0][0] === 'docker') delete f.engine.images.get(image(2)).Config.Labels[storageReaderLabel]; return result; };
  await assert.rejects(f.create().run());
  assert.equal((await f.engine.inspect(id(1))).State.Running, true);
  assert.ok(!f.engine.events.some(e => e.startsWith('stop:')));
});

test('first v3 activation refuses the actual old v2 image before creating a candidate or stopping the writer', async () => {
  const f = await fixture();
  f.engine.items.get(id(1)).Config.Labels[storageReaderLabel] = currentStorageReaders;
  f.engine.images.get(image(1)).Config.Labels[storageReaderLabel] = '{"version":2,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6]}}';
  await assert.rejects(f.create().run());
  assert.equal((await f.engine.inspect(id(1))).State.Running, true);
  assert.equal((await f.engine.inspect(id(1))).HostConfig.RestartPolicy.Name, 'always');
  assert.ok(!f.engine.events.some(event => event === 'create' || /^(?:stop:|start:|policy:)/u.test(event)));
  assert.equal((await f.readState()).transaction, null);
});

for (const store of ['notes', 'capabilities']) for (const version of [store === 'capabilities' ? 4 : 3, 2]) test(`controller checks fresh ${store}${version} compatibility before stopping ${version === 2 ? 'reader1' : 'the writer'}`, async () => {
  const f = await fixture();
  if (version === 2) f.engine.images.get(image(1)).Config.Labels[storageReaderLabel] = '{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1],"capabilities":[1]}}';
  f.deps.storageProbe = async () => ({ ok: true, schema: 'soty.storage-format.v3', rooms: 1, apps: 'empty', notes: 'empty', capabilities: 'empty', [store]: version });
  await assert.rejects(f.create().run());
  assert.equal((await f.engine.inspect(id(1))).State.Running, true);
  assert.equal((await f.engine.inspect(id(1))).HostConfig.RestartPolicy.Name, 'always');
  assert.ok(!f.engine.events.some(event => /^(?:stop:|start:)/u.test(event) || event === 'policy:' + id(1) || event === 'probe:enter'));
  // Recovery may disable the never-started candidate's restart policy. It
  // must preserve that evidence while leaving the serving writer untouched.
  const state = await f.readState(); assert.ok(state.transaction);
  const candidate = await f.engine.inspect(state.transaction.nextId);
  assert.equal(candidate.State.Running, false); assert.equal(candidate.State.Status, 'created');
  assert.equal(candidate.HostConfig.RestartPolicy.Name, 'no');
});

test('old pending start without durable format receipt cannot be approved by recovery', async () => {
  const f = await fixture(); f.engine.ignore = 'start';
  await assert.rejects(f.create().run());
  const state = await f.readState();
  assert.equal(state.transaction.operation.kind, 'start');
  delete state.transaction.operation.storageGuard;
  await atomicState(path.join(f.config.stateDir, 'host-state.json'), state);
  const starts = f.engine.events.filter(e => e.startsWith('start:')).length;
  await assert.rejects(f.create().run());
  assert.equal(f.engine.events.filter(e => e.startsWith('start:')).length, starts);
  assert.equal((await f.readState()).transaction.operation.storageGuard, undefined);
  assert.equal((await f.engine.inspect(id(1))).State.Running, false);
});

test('legacy v1/v2 receipts cannot settle delayed START or automatic-restart results even with a copied current label', async () => {
  for (const version of [1, 2]) for (const kind of ['start', 'restartPolicy']) {
    const f = await fixture(); f.engine.ignore = 'start';
    await assert.rejects(f.create().run());
    const state = await f.readState(), operation = state.transaction.operation;
    assert.equal(operation.kind, 'start'); assert.equal(operation.storageGuard.schema, 'soty.storage-start.v3');
    operation.storageGuard.schema = `soty.storage-start.v${version}`;
    delete operation.storageGuard.notes; delete operation.storageGuard.capabilities;
    if (version === 1) delete operation.storageGuard.apps;
    operation.kind = kind;
    const candidate = f.engine.items.get(operation.id);
    candidate.Config.Labels[storageReaderLabel] = currentStorageReaders;
    if (kind === 'start') candidate.State = { Running: true, Status: 'running' };
    else { operation.value = { Name: 'always', MaximumRetryCount: 0 }; candidate.HostConfig.RestartPolicy = clones(operation.value); }
    await atomicState(path.join(f.config.stateDir, 'host-state.json'), state);
    const controller = f.create(); controller.state = state;
    const events = [...f.engine.events];
    await assert.rejects(controller.reconcileOperation(), /storage_start_guard_missing/);
    assert.deepEqual(f.engine.events, events);
    assert.equal((await f.readState()).transaction.operation.storageGuard.schema, `soty.storage-start.v${version}`);
    assert.equal((await f.engine.inspect(id(1))).State.Running, false);
  }
});

test('recovered v3 START receipt rechecks current Apps format and never repeats a previously submitted start', async () => {
  const f = await fixture(); f.engine.ignore = 'start';
  await assert.rejects(f.create().run());
  const state = await f.readState(), operation = state.transaction.operation;
  assert.equal(operation.storageGuard.schema, 'soty.storage-start.v3'); assert.equal(operation.storageGuard.apps, 'empty');
  f.engine.items.get(operation.id).State = { Running: true, Status: 'running' };
  f.deps.storageProbe = async () => ({ ok: true, schema: 'soty.storage-format.v3', notes: 'empty', capabilities: 'empty', rooms: 1, apps: 9 });
  const controller = f.create(); controller.state = state;
  const starts = f.engine.events.filter(event => event.startsWith('start:')).length;
  await assert.rejects(controller.reconcileOperation(), /storage_probe_invalid/);
  assert.equal(f.engine.events.filter(event => event.startsWith('start:')).length, starts);
  assert.ok(controller.state.transaction.operation);
  controller.storageProbeOverride = async () => ({ ok: true, schema: 'soty.storage-format.v3', notes: 'empty', capabilities: 'empty', rooms: 1, apps: 6 });
  await controller.reconcileOperation();
  assert.equal(controller.state.transaction.operation, null);
  assert.equal(f.engine.events.filter(event => event.startsWith('start:')).length, starts);
});

for (const previous of [2, 3, 4, 5, 6, 7]) test(`pending Apps${previous} START receipt cannot settle against Apps${previous + 1} data or repeat an already submitted start`, async () => {
  const f = await fixture(); f.engine.ignore = 'start';
  const command = f.deps.command;
  f.deps.command = async (...args) => {
    const value = await command(...args);
    if (args[0][0] === 'docker') f.engine.images.get(image(2)).Config.Labels[storageReaderLabel]
      = JSON.stringify({ version: 3, readers: { notes: [1], capabilities: [1], rooms: [1, 2], apps: Array.from({ length: previous }, (_, i) => i + 1) } });
    return value;
  };
  f.deps.storageProbe = async () => ({ ok: true, schema: 'soty.storage-format.v3', notes: 'empty', capabilities: 'empty', rooms: 1, apps: previous });
  await assert.rejects(f.create().run());
  const state = await f.readState(), operation = state.transaction.operation;
  assert.equal(operation.kind, 'start'); assert.equal(operation.storageGuard.apps, previous);
  f.engine.items.get(operation.id).State = { Running: true, Status: 'running' };
  f.deps.storageProbe = async () => ({ ok: true, schema: 'soty.storage-format.v3', notes: 'empty', capabilities: 'empty', rooms: 1, apps: previous + 1 });
  const controller = f.create(); controller.state = state;
  const events = [...f.engine.events];
  await assert.rejects(controller.reconcileOperation(), /storage_reader_incompatible/);
  assert.deepEqual(f.engine.events, events);
  assert.ok(controller.state.transaction.operation); assert.equal(controller.state.transaction.operation.storageGuard.apps, previous);
});

test('restored journal from an older controller needs a fresh compatible reader proof before settlement', async () => {
  const f = await fixture({ healthFailure: true }); let restored;
  f.deps.write = async (file, value) => { if (value.transaction?.phase === 'restored') restored = clones(value); return atomicState(file, value); };
  await assert.rejects(f.create().run());
  assert.equal(restored.transaction.phase, 'restored');
  const c = f.create(); c.state = restored;
  delete f.engine.images.get(image(1)).Config.Labels[storageReaderLabel];
  await assert.rejects(c.settle(), /storage_reader_unknown/);
  assert.equal(c.state.transaction.phase, 'restored');
});

test('changed pinned host source refuses before baseline load, recovery or restored catch settlement effects', async () => {
  const f = await fixture(); const calls = [];
  f.deps.checkSource = async () => { calls.push('source'); throw Object.assign(new Error('changed'), { code: 'source_host_changed' }); };
  f.deps.recover = async () => { calls.push('recover'); };
  const c = f.create();
  c.state = { transaction: { phase: 'restored' } };
  c.load = async () => { calls.push('load'); };
  await assert.rejects(c.run(), /source_host_changed/);
  assert.ok(!calls.includes('load')); assert.ok(!calls.includes('recover'));
  assert.deepEqual(f.engine.events, []);
  assert.equal(f.counters().fetches, 0);
  assert.equal(c.state.transaction.phase, 'restored');
});

test('recovery activation and settlement recheck the host pin before helper or Docker inspection', async () => {
  const f = await fixture();
  f.deps.checkSource = async () => { throw Object.assign(new Error('changed'), { code: 'source_revision_changed' }); };
  const c = f.create(); c.state = { transaction: { phase: 'restored' } };
  f.engine.inspect = async () => { throw new Error('must not inspect'); };
  await assert.rejects(c.activate(f.target), { code: 'source_revision_changed' });
  await assert.rejects(c.settle(), { code: 'source_revision_changed' });
  assert.deepEqual(f.engine.events, []);
});
