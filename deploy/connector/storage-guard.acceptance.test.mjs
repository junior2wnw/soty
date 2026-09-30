import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { HostController } from '../connect/host-controller.mjs';
import { SafeError } from './docker-api.mjs';
import { guardStorageStart, reconcileStorageProbe, storageReaderLabel, currentStorageReaders } from './storage-guard.mjs';

const id = n => n.toString(16).padStart(64, '0');
const imageId = n => `sha256:${id(n)}`;
const clone = value => structuredClone(value);
const format = rooms => ({ ok: true, schema: 'soty.storage-format.v1', rooms });

function gateFixture({ running = false, production = false } = {}) {
  const runtime = { Id: id(1), Image: imageId(2), Name: '/soty-online-chat',
    State: { Running: running, Status: running ? 'running' : 'created' },
    Config: { Env: ['DATA_DIR=/data', 'SYNTHETIC_PASSWORD=fixture-only'] }, HostConfig: { Mounts: [] },
    Mounts: [{ Type: 'volume', Name: 'soty-data-acceptance', Source: '/var/lib/docker/volumes/soty-data-acceptance/_data', Destination: '/data', RW: true }] };
  const objects = new Map([[runtime.Id, runtime]]), journal = {}, calls = [];
  const volumes = new Map([[runtime.Mounts[0].Name, { Name: runtime.Mounts[0].Name, Mountpoint: runtime.Mounts[0].Source,
    Driver: 'local', Scope: 'local', Options: null }]]);
  let observed = format(2), probes = 0, sequence = 9;
  const engine = {
    async inspect(key) { const value = objects.get(key) ?? [...objects.values()].find(value => value.Name === `/${key}`); if (!value) throw new SafeError('engine_http_404'); return clone(value); },
    async image(key) { return { Id: key, Config: { Labels: { [storageReaderLabel]: currentStorageReaders } } }; },
    async request(method, endpoint) {
      assert.equal(method, 'GET');
      if (endpoint.startsWith('/volumes/')) { const value = volumes.get(decodeURIComponent(endpoint.slice('/volumes/'.length))); if (!value) throw new SafeError('engine_http_404'); return clone(value); }
      assert.equal(endpoint, '/containers/json'); return clone([...objects.values()].filter(value => value.State.Running));
    },
    async create(name, body) {
      calls.push({ operation: 'create', body: clone(body) }); const Id = id(++sequence);
      objects.set(Id, { Id, Name: `/${name}`, Image: body.Image, Config: { Labels: clone(body.Labels) },
        State: { Running: false, Status: 'created' }, Mounts: [{ ...runtime.Mounts[0], RW: false }] });
      return { Id };
    },
    async start(key) { calls.push({ operation: 'start', id: key }); objects.get(key).State = { Running: false, Status: 'exited', ExitCode: 0 }; },
    async helperOutput() { return clone(observed); },
  };
  const context = { engine, probeImage: imageId(3), transactionId: 'd'.repeat(32), maxPolls: 2, wait: async () => {},
    getState: () => journal, record: async value => { Object.assign(journal, clone(value)); },
    ...(!production ? { probe: async () => { probes++; return clone(observed); } } : {}) };
  return { runtime, objects, volumes, journal, calls, engine, context, probes: () => probes, setObserved: value => { observed = value; } };
}

test('the checked /data view must match the runtime: missing env, duplicate env, nested mounts and volume subpath are rejected before a probe', async () => {
  const variants = [
    runtime => { runtime.Config.Env = []; },
    runtime => { runtime.Config.Env = ['DATA_DIR=/data', 'DATA_DIR=/app/data']; },
    runtime => { runtime.Mounts.push({ Type: 'bind', Source: '/srv/other.sqlite', Destination: '/data/rooms-v2.sqlite', RW: true }); },
    runtime => {
      runtime.HostConfig.Mounts = [{ Type: 'volume', Source: runtime.Mounts[0].Name, Target: '/data', VolumeOptions: { Subpath: 'account-data' } }];
    },
  ];
  for (const change of variants) {
    const f = gateFixture(); change(f.runtime);
    await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_data_(directory_unsupported|mount_overlay|subpath_unsupported)/);
    assert.equal(f.probes(), 0); assert.equal(f.calls.length, 0);
  }
});

test('a permitted running runtime does not exempt an overlapping ancestor writer; read-only observers and sibling paths remain allowed', async () => {
  const f = gateFixture({ running: true });
  f.objects.set(id(4), { Id: id(4), State: { Running: true }, Mounts: [{ Type: 'bind', Source: '/var/lib/docker/volumes', Destination: '/host', RW: true }] });
  await assert.rejects(guardStorageStart(f.context, f.runtime, { running: true }), /storage_other_writer/);
  assert.equal(f.probes(), 0);
  f.objects.get(id(4)).Mounts[0].RW = false;
  f.objects.set(id(5), { Id: id(5), State: { Running: true }, Mounts: [{ Type: 'bind', Source: '/var/lib/docker/volumes/soty-data-acceptance/_data-archive', Destination: '/archive', RW: true }] });
  const receipt = await guardStorageStart(f.context, f.runtime, { running: true });
  assert.equal(receipt.containerId, f.runtime.Id); assert.equal(receipt.rooms, 2);
  assert.doesNotMatch(JSON.stringify(receipt), /fixture-only|SYNTHETIC_PASSWORD|\/var\/lib\/docker/);
});

test('a volume-subpath introduced while checking is rejected even when the root name and source stay identical', async () => {
  const f = gateFixture();
  f.context.probe = async () => {
    f.runtime.HostConfig.Mounts.push({ Type: 'volume', Source: f.runtime.Mounts[0].Name, Target: '/data', VolumeOptions: { Subpath: 'different-view' } });
    return format(2);
  };
  await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_data_subpath_unsupported/);
});

test('the managed local-volume profile rejects ambiguous backing stores and rechecks volume metadata after the probe', async () => {
  const changes = [
    volume => { volume.Driver = 'remote-plugin'; },
    volume => { volume.Scope = 'global'; },
    volume => { volume.Options = { type: 'none', o: 'bind', device: '/srv/alias' }; },
    volume => { volume.Name = 'another-volume'; },
    volume => { volume.Mountpoint = '/var/lib/docker/volumes/another-volume/_data'; },
  ];
  for (const change of changes) {
    const f = gateFixture(); change(f.volumes.get(f.runtime.Mounts[0].Name));
    await assert.rejects(guardStorageStart(f.context, f.runtime), error => error.code?.startsWith('storage_'));
    assert.equal(f.probes(), 0); assert.equal(f.calls.length, 0);
  }
  const bound = gateFixture(); bound.runtime.Mounts[0] = { Type: 'bind', Source: '/srv/soty/data', Destination: '/data', RW: true };
  await assert.rejects(guardStorageStart(bound.context, bound.runtime), error => error.code?.startsWith('storage_'));
  assert.equal(bound.probes(), 0);
  const late = gateFixture();
  late.context.probe = async () => { late.volumes.get(late.runtime.Mounts[0].Name).Options = { device: '/srv/late-alias' }; return format(2); };
  await assert.rejects(guardStorageStart(late.context, late.runtime), error => error.code?.startsWith('storage_'));
});

test('a retained running probe is never restarted; invalid terminal output keeps the receipt until a valid exact helper can be reconciled', async () => {
  const f = gateFixture({ production: true }), helper = { Id: id(20), Name: '/soty-storage-probe-fixture', Image: imageId(3),
    Config: { Labels: { 'io.soty.storage.probe': f.context.transactionId } }, State: { Running: true, Status: 'running' }, Mounts: [] };
  f.objects.set(helper.Id, helper);
  f.journal.storageGuardHelper = { id: helper.Id, name: helper.Name.slice(1), image: helper.Image, state: 'starting' };
  await assert.rejects(reconcileStorageProbe(f.context), /storage_helper_unresolved/);
  assert.equal(f.calls.length, 0); assert.ok(f.journal.storageGuardHelper);
  helper.State = { Running: false, Status: 'exited', ExitCode: 0 }; f.setObserved({ ok: true, rooms: 2 });
  await assert.rejects(reconcileStorageProbe(f.context), /storage_probe_invalid/);
  assert.equal(f.calls.length, 0); assert.ok(f.journal.storageGuardHelper);
  f.setObserved(format(2)); await reconcileStorageProbe(f.context);
  assert.equal(f.journal.storageGuardHelper, null); assert.equal(f.calls.length, 0);
});

test('failure to durably record either helper intent prevents the corresponding Docker side effect', async () => {
  for (const failAt of [1, 2]) {
    const f = gateFixture({ production: true }); let records = 0;
    f.context.record = async fields => {
      if (++records === failAt) throw new SafeError('fixture_journal_failure');
      Object.assign(f.journal, clone(fields));
    };
    await assert.rejects(guardStorageStart(f.context, f.runtime), /fixture_journal_failure/);
    assert.equal(f.calls.filter(call => call.operation === 'create').length, failAt === 1 ? 0 : 1);
    assert.equal(f.calls.filter(call => call.operation === 'start').length, 0);
  }
});

test('an unpinned host source cannot perform baseline probes, recovery or restored settlement before rejecting its source', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-guard-acceptance-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir())); assert.match(path.basename(root), /^soty-guard-acceptance-/);
    await rm(root, { recursive: true, force: true });
  });
  const sourceRoot = path.join(root, 'source'), releaseDirectory = path.join(root, 'feed');
  await mkdir(sourceRoot); await mkdir(releaseDirectory);
  for (const code of ['source_revision_changed', 'source_host_changed']) {
    const calls = [], config = { source: 'https://soty.example/release.json', trustFile: path.join(root, 'trust.json'), sourceRoot,
      stateDir: path.join(root, code), releaseDirectory, revision: 'f'.repeat(40), runtimeName: 'soty-online-chat',
      healthOrigin: 'http://127.0.0.1:18182', initialRuntimeHasConnect: false, dockerSocket: path.join(root, 'docker.sock'),
      backupCommand: [process.execPath, path.join(root, 'backup.mjs'), '{containerId}'] };
    const controller = new HostController(config, {
      checkSource: async () => { calls.push('checkSource'); throw new SafeError(code); },
      recover: async () => { calls.push('recover'); },
    });
    controller.load = async () => { calls.push('load'); controller.state = { transaction: { phase: 'restored' } }; };
    controller.settle = async () => { calls.push('settle'); };
    controller.verifyActive = async () => { calls.push('verifyActive'); };
    await assert.rejects(controller.run(), error => error.code === code);
    assert.deepEqual(calls, ['checkSource']);
  }
});
