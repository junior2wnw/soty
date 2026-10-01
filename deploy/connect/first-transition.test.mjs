import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm, access } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { HostController, candidateConfig, moduleTree, originalPreservationHash, atomicState } from './host-controller.mjs';
import { readFirstTransitionFile } from './first-transition.mjs';
import { preservationHash } from '../connector/rollout.mjs';
import { currentStorageReaders, storageReaderLabel } from '../connector/storage-guard.mjs';

const id = n => n.toString(16).padStart(64, '0'), image = n => 'sha256:' + id(n), clone = structuredClone;
const oldRevision = 'a'.repeat(40), hostRevision = 'b'.repeat(40), buildRevision = 'c'.repeat(40);
const noRestart = { Name: 'no', MaximumRetryCount: 0 };

class Engine {
  constructor(old) { this.items = new Map([[old.Id, old]]); this.images = new Map(); this.events = []; this.next = 10; this.startUnobserved = false; }
  async inspect(key) { const c = this.items.get(key) || [...this.items.values()].find(c => c.Name === '/' + key); if (!c) throw Object.assign(new Error(), { code: 'engine_http_404' }); return clone(c); }
  async image(key) { assert.ok(this.images.has(key)); return clone(this.images.get(key)); }
  async create(name, body) {
    assert.ok(![...this.items.values()].some(c => c.Name === '/' + name));
    const { HostConfig, NetworkingConfig, ...Config } = clone(body), key = id(this.next++);
    Config.Labels = { ...this.images.get(Config.Image).Config.Labels, ...Config.Labels };
    const Mounts = HostConfig.Mounts.map(m => ({ Type: m.Type, Name: m.Type === 'volume' ? m.Source : undefined,
      Source: m.Type === 'volume' ? '/volumes/' + m.Source : m.Source, Destination: m.Target, RW: !m.ReadOnly }));
    this.items.set(key, { Id: key, Image: Config.Image, Name: '/' + name, Config, HostConfig, Mounts,
      NetworkSettings: { Networks: NetworkingConfig.EndpointsConfig }, State: { Running: false, Status: 'created', StartedAt: '0001-01-01T00:00:00Z' } });
    this.events.push(['create', key]); return { Id: key };
  }
  async stop(key) { const c = this.items.get(key); this.events.push(['stop', key]); c.State.Running = false; c.State.Status = 'exited'; c.State.ExitCode = 0; if (this.dropStop) throw new Error('lost response'); }
  async start(key) {
    this.events.push(['start', key]); if (this.startUnobserved) return;
    assert.ok(![...this.items.values()].some(c => c.Id !== key && c.State.Running), 'only one actual writer');
    const c = this.items.get(key); c.State.Running = true; c.State.Status = 'running'; c.State.StartedAt = '2026-10-01T00:00:00Z';
  }
  async rename(key, name) { assert.ok(![...this.items.values()].some(c => c.Id !== key && c.Name === '/' + name)); this.events.push(['rename', key]); this.items.get(key).Name = '/' + name; }
  async request(method, route, body) {
    if (method === 'GET' && route.startsWith('/volumes/')) return { Name: 'data', Mountpoint: '/volumes/data', Driver: 'local', Scope: 'local', Options: null };
    if (method === 'GET' && route === '/containers/json') return clone([...this.items.values()].filter(c => c.State.Running));
    const match = route.match(/^\/containers\/([a-f0-9]{64})\/update$/); assert.equal(method, 'POST'); assert.ok(match);
    this.events.push(['policy', match[1], clone(body.RestartPolicy)]); this.items.get(match[1]).HostConfig.RestartPolicy = clone(body.RestartPolicy); return {};
  }
}

async function fixture(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-first-transition-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const sourceRoot = path.join(root, 'old'), nextRoot = path.join(root, 'next'), stateDir = path.join(root, 'state'), releaseDirectory = path.join(root, 'feed');
  for (const [directory, text] of [[sourceRoot, 'old'], [nextRoot, 'new']]) {
    await mkdir(path.join(directory, 'modules', 'connect'), { recursive: true });
    await writeFile(path.join(directory, 'modules', 'connect', 'package.json'), JSON.stringify({ name: '@soty/connect', version: '0.1.0' }));
    await writeFile(path.join(directory, 'modules', 'connect', 'index.mjs'), `export default '${text}';`);
  }
  await mkdir(path.join(stateDir, 'module-update'), { recursive: true }); await mkdir(releaseDirectory);
  const trustFile = path.join(root, 'trust.json'); await writeFile(trustFile, '{}');
  const beforeModule = await moduleTree(path.join(sourceRoot, 'modules', 'connect')), afterModule = await moduleTree(path.join(nextRoot, 'modules', 'connect'));
  const old = { Id: id(1), Image: image(1), Name: '/soty-online-chat', State: { Running: true, Status: 'running', StartedAt: '2026-09-28T01:15:45Z' },
    Config: { Image: image(1), Env: ['DATA_DIR=/data', 'SECRET_CANARY=not-in-state'], User: 'node', WorkingDir: '/app', Cmd: ['node', 'server/index.js'], Labels: { 'org.opencontainers.image.revision': oldRevision }, Volumes: { '/data': {} }, ExposedPorts: { '8080/tcp': {} } },
    HostConfig: { Mounts: [{ Type: 'volume', Source: 'data', Target: '/data', ReadOnly: false, VolumeOptions: { NoCopy: true } }], Binds: [], NetworkMode: 'bridge', RestartPolicy: { Name: 'always', MaximumRetryCount: 0 }, PortBindings: { '8080/tcp': [{ HostIp: '127.0.0.1', HostPort: '18182' }] }, OomKillDisable: false },
    Mounts: [{ Type: 'volume', Name: 'data', Source: '/volumes/data', Destination: '/data', RW: true }], NetworkSettings: { Networks: { bridge: {} } } };
  const engine = new Engine(old);
  engine.images.set(image(1), { Id: image(1), Config: { Labels: {} } });
  for (const n of [2, 3]) engine.images.set(image(n), { Id: image(n), Config: { Labels: { [storageReaderLabel]: currentStorageReaders, 'org.opencontainers.image.revision': buildRevision, 'io.soty.connect.tree': afterModule.tree } } });
  const config = { sourceRoot, stateDir, releaseDirectory, trustFile, revision: oldRevision, source: 'https://fixture.invalid/release', healthOrigin: 'http://127.0.0.1:18182', runtimeName: 'soty-online-chat', initialRuntimeHasConnect: true, dockerSocket: path.join(root, 'docker.sock'), storageProbeImage: image(2), backupCommand: [process.execPath, path.join(root, 'backup.mjs'), '{containerId}'] };
  const tx = 'd'.repeat(32), request = { schema: 'soty.connect.first-transition.v1', id: tx,
    outgoing: { id: old.Id, image: old.Image, startedAt: old.State.StartedAt, configurationSha256: originalPreservationHash(old, old.Image, tx) },
    handoff: { sourceRoot: nextRoot, revision: hostRevision, tree: afterModule.tree }, admissionReceiptFile: path.join(root, 'admission.json'), restoreReceiptFile: path.join(root, 'restore.json') };
  for (const [role, n] of [['candidate', 2], ['recovery', 3]]) { const body = candidateConfig(old, image(n), tx, { ...config, revision: buildRevision }, afterModule.tree); body.HostConfig.RestartPolicy = clone(noRestart); request[role] = { image: image(n), buildRevision, configurationSha256: preservationHash(body) }; }
  // Order is intentional only for JSON equality; the production validator has
  // a closed field set, independent of either mock engine or helper result.
  const admission = { schema: 'soty.connect.first-transition.admission.v1', transactionId: tx, outgoing: request.outgoing, candidate: request.candidate, recovery: request.recovery, handoff: request.handoff, admissionClosed: true, imageGatePassed: true };
  await writeFile(request.admissionReceiptFile, JSON.stringify(admission));
  const moduleBefore = { format: 1, target: path.join(sourceRoot, 'modules', 'connect'), lastSequence: 8, releaseHash: id(91), version: '0.1.0', pending: null };
  await atomicState(path.join(stateDir, 'module-update', 'state.json'), moduleBefore);
  const hostBefore = { schema: 'soty.connect.host.v1', sourceRoot, revision: oldRevision,
    images: { [beforeModule.tree]: { image: image(1), version: '0.1.0', hasConnect: true, revision: oldRevision } }, active: { tree: beforeModule.tree, image: image(1), containerId: id(1) }, transaction: null };
  await atomicState(path.join(stateDir, 'host-state.json'), hostBefore);
  let marker = false, backups = 0, normalRecovery = 0, candidateUnhealthy = false;
  const deps = { engine, pause: async () => {}, checkSource: async () => {},
    recover: async () => { normalRecovery++; throw new Error('normal recovery forbidden in first mode'); },
    storageProbe: async () => ({ ok: true, schema: 'soty.storage-format.v3', rooms: 2, apps: 6, notes: 2, capabilities: 3 }),
    ready: async ({ entry, maintenance }) => { if (candidateUnhealthy && entry.image === image(2)) throw Object.assign(new Error(), { code: 'readiness_deadline' }); assert.equal(maintenance, marker); return { modelsHash: id(50), policyHash: null }; },
    probe: async (verb, runtime) => { if (runtime.Id === id(1)) assert.equal(verb, 'status'); if (verb === 'enter') { assert.equal(runtime.State.Running, false); marker = true; } if (verb === 'leave') marker = false; return { ok: true, schema: 'soty.connect.maintenance.v1', count: 0, maintenance: marker, owned: marker }; },
    backup: async ({ containerId, transactionId, checkpointSha256 }) => { backups++; assert.equal(containerId, id(1)); assert.equal((await engine.inspect(containerId)).State.Running, false); return { ok: true, encrypted: true, complete: true, receiptPath: path.join(root, 'synthetic.enc'), sha256: id(51), manifestSha256: id(52), sourceWitness: { generationId: transactionId, checkpointSha256, inventorySha256: id(53) } }; } };
  const state = () => readFirstTransitionFile(path.join(stateDir, 'host-state.json'));
  const writeEvidence = async changes => { const s = await state(), b = s.transaction.backup;
    await writeFile(request.restoreReceiptFile, JSON.stringify({ schema: 'soty.connect.first-transition.restore.v1', transactionId: tx,
      archiveSha256: b.sha256, manifestSha256: b.manifestSha256, sourceWitness: b.sourceWitness,
      candidate: request.candidate, recovery: request.recovery, handoff: request.handoff,
      restored: true, applicationReady: true, admissionClosed: true, sourceStillCold: true, ...changes })); };
  return { root, config, request, deps, engine, hostBefore, moduleBefore, state, writeEvidence,
    controller: (c = config) => new HostController(c, deps), unhealthy: value => { candidateUnhealthy = value; }, counts: () => ({ backups, normalRecovery }) };
}

test('first transition cold-pauses before any START, then binds real build revision and preserves predecessor/sequence', async t => {
  const f = await fixture(t);
  assert.equal((await f.controller().run({ firstTransition: f.request })).status, 'awaiting_restore_evidence');
  assert.equal(f.engine.events.filter(e => e[0] === 'start').length, 0);
  assert.deepEqual(f.engine.events.filter(e => e[1] === id(1)).map(e => e[0]), ['policy', 'stop']);
  await f.writeEvidence(); assert.equal((await f.controller().run({ firstTransition: f.request })).status, 'baseline_serving');
  const state = await f.state(); assert.equal(state.schema, 'soty.connect.host.v2'); assert.equal(state.sourceRoot, f.request.handoff.sourceRoot);
  assert.equal(state.revision, hostRevision); assert.equal(state.images[f.request.handoff.tree].revision, buildRevision);
  assert.deepEqual(state.firstTransition.predecessor.images, f.hostBefore.images);
  assert.deepEqual(state.firstTransition.previousModuleState, f.moduleBefore);
  const module = await readFirstTransitionFile(path.join(f.config.stateDir, 'module-update', 'state.json'));
  assert.deepEqual(module, { ...f.moduleBefore, target: path.join(f.request.handoff.sourceRoot, 'modules', 'connect') });
  assert.equal(JSON.stringify(state).includes('SECRET_CANARY'), false); assert.equal(f.counts().backups, 1);
  assert.equal(f.counts().normalRecovery, 0); assert.equal((await f.engine.inspect(id(1))).State.Running, false);
  await assert.rejects(f.controller().run(), /host_state_invalid/);
});

test('complete encrypted backup is insufficient: archive, manifest, witness and exact profiles gate every START', async t => {
  const f = await fixture(t); await f.controller().run({ firstTransition: f.request });
  for (const delta of [{ archiveSha256: id(99) }, { manifestSha256: id(99) }, { sourceWitness: { generationId: f.request.id, checkpointSha256: id(99), inventorySha256: id(53) } }, { candidate: { ...f.request.candidate, configurationSha256: id(99) } }, { applicationReady: false }, { sourceStillCold: false }]) {
    await f.writeEvidence(delta); await assert.rejects(f.controller().run({ firstTransition: f.request }), /first_transition_restore_required/);
    assert.equal(f.engine.events.some(e => e[0] === 'start'), false);
  }
  assert.equal(f.counts().backups, 1); assert.equal((await f.state()).transaction.phase, 'backed_up');
});

test('pending first mode dispatches before normal recovery and denies START, restart enable and old RW helper', async t => {
  const f = await fixture(t); await f.controller().run({ firstTransition: f.request });
  await assert.rejects(f.controller().run(), /first_transition_explicit_resume_required/); assert.equal(f.counts().normalRecovery, 0);
  const h = f.controller(); h.state = await f.state();
  await assert.rejects(h.action('start', id(1), undefined, () => true), /outgoing_stop_only/);
  await assert.rejects(h.action('restartPolicy', id(1), { Name: 'always' }, () => true), /outgoing_stop_only/);
  await assert.rejects(h.probe('enter', await f.engine.inspect(id(1))), /outgoing_stop_only/);
  await assert.rejects(h.restore(), /outgoing_stop_only/);
  h.state.transaction.operation = { kind: 'start', id: id(1) }; await assert.rejects(h.reconcileOperation(), /outgoing_stop_only/);
});

test('failed C admission recovers only to explicit R on latest files without a second backup or restore', async t => {
  const f = await fixture(t); await f.controller().run({ firstTransition: f.request }); await f.writeEvidence(); f.unhealthy(true);
  await assert.rejects(f.controller().run({ firstTransition: f.request }), /readiness_deadline/);
  // A write after the legitimate START is represented outside the adapter. R
  // receives the same volume; no library/file restore port exists here.
  const witness = path.join(f.root, 'post-start-write'); await writeFile(witness, 'must survive');
  const result = await f.controller().run({ firstTransition: f.request, recoverFirstTransition: true });
  assert.equal(result.recovery, true); assert.equal((await f.state()).active.image, image(3)); assert.equal(await readFile(witness, 'utf8'), 'must survive');
  assert.equal(f.counts().backups, 1); assert.equal(f.engine.events.some(e => e[0] === 'start' && e[1] === id(1)), false);
  const c = [...f.engine.items.values()].find(c => c.Image === image(2)); const r = [...f.engine.items.values()].find(c => c.Image === image(3));
  assert.equal(c.State.Running, false); assert.equal(r.State.Running, true); assert.deepEqual(r.Mounts, c.Mounts);
});

test('unknown START ACK is never replayed and a held module lock prevents all Docker mutation', async t => {
  const f = await fixture(t); const lock = path.join(f.config.stateDir, 'module-update', 'update.lock'); await writeFile(lock, 'foreign');
  await assert.rejects(f.controller().run({ firstTransition: f.request }), /update_locked/); assert.deepEqual(f.engine.events, []); await rm(lock);
  await f.controller().run({ firstTransition: f.request }); await f.writeEvidence(); f.engine.startUnobserved = true;
  await assert.rejects(f.controller().run({ firstTransition: f.request }), /docker_operation_unresolved/);
  await assert.rejects(f.controller().run({ firstTransition: f.request }), /docker_operation_unresolved/);
  assert.equal(f.engine.events.filter(e => e[0] === 'start').length, 1);
});

test('handoff crash after module target publication resumes exact metadata only and old configuration then refuses', async t => {
  const f = await fixture(t); await f.controller().run({ firstTransition: f.request }); await f.writeEvidence(); let interrupted = false;
  f.deps.write = async (file, value) => { if (!interrupted && path.basename(file) === 'host-state.json' && value.transaction === null && value.firstTransition) { interrupted = true; throw new Error('simulated lost host publication'); } await atomicState(file, value); };
  await assert.rejects(f.controller().run({ firstTransition: f.request }), /controller_failed/);
  assert.equal((await f.state()).transaction.phase, 'handoff'); const starts = f.engine.events.filter(e => e[0] === 'start').length;
  assert.equal((await f.controller().run({ firstTransition: f.request })).status, 'baseline_serving');
  assert.equal(f.engine.events.filter(e => e[0] === 'start').length, starts); assert.equal(f.counts().backups, 1);
  const nextConfig = { ...f.config, sourceRoot: f.request.handoff.sourceRoot, revision: hostRevision };
  assert.equal((await f.controller(nextConfig).run({ firstTransition: f.request })).status, 'current');
  await assert.rejects(f.controller().run({ firstTransition: f.request }), /host_state_invalid/);
});

test('mismatched actual image revision and private admission each refuse before policy or STOP', async t => {
  const f = await fixture(t); f.engine.images.get(image(2)).Config.Labels['org.opencontainers.image.revision'] = 'e'.repeat(40);
  await assert.rejects(f.controller().run({ firstTransition: f.request }), /first_transition_image_changed/); assert.deepEqual(f.engine.events, []);
  f.engine.images.get(image(2)).Config.Labels['org.opencontainers.image.revision'] = buildRevision;
  await writeFile(f.request.admissionReceiptFile, JSON.stringify({ admissionClosed: true }));
  await assert.rejects(f.controller().run({ firstTransition: f.request }), /first_transition_admission_required/); assert.deepEqual(f.engine.events, []);
});

test('strict current storage guard still rejects a future format before any C or R RW helper', async t => {
  const f = await fixture(t); await f.controller().run({ firstTransition: f.request }); await f.writeEvidence();
  f.deps.storageProbe = async () => ({ ok: true, schema: 'soty.storage-format.v3', rooms: 2, apps: 6, notes: 2, capabilities: 4 });
  await assert.rejects(f.controller().run({ firstTransition: f.request }), /storage_probe_invalid/);
  assert.equal(f.engine.events.some(e => e[0] === 'start'), false); assert.equal(f.counts().backups, 1);
  await access(path.join(f.config.stateDir, 'host-state.json'));
});
