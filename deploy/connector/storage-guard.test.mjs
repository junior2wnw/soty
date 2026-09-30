import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createRoomStore } from '../../server/room-store.js';
import { readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, checkedStorageFormat, currentStorageReaders, guardStorageStart,
  reconcileStorageProbe, requireStorageStartReceipt, storageReaderLabel } from './storage-guard.mjs';

const id = n => n.toString(16).padStart(64, '0');
const imageId = n => 'sha256:' + id(n);
const clone = x => structuredClone(x);
const format = rooms => ({ ok: true, schema: 'soty.storage-format.v1', rooms });
const image = (n, readers = currentStorageReaders) => ({ Id: imageId(n), Config: { Labels: readers ? { [storageReaderLabel]: readers } : {} } });
const jsonOnly = JSON.stringify({ version: 1, readers: { rooms: [1] } });

async function directory(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-format-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir()));
    assert.match(path.basename(root), /^soty-format-/u);
    await rm(root, { recursive: true, force: true });
  });
  return root;
}

test('empty volume is explicit, legacy files require reader1, coexisting v2 wins without modifying old files', async t => {
  const root = await directory(t);
  assert.deepEqual(await readStorageFormat(root), format('empty'));
  assertStorageCompatible(image(2), await readStorageFormat(root));
  await writeFile(path.join(root, 'connector-store.json'), '{}');
  assert.deepEqual(await readStorageFormat(root), format('empty'));
  const legacy = path.join(root, 'room_1234567890123456.json');
  await writeFile(legacy, '{"auth":"synthetic","files":[],"updates":[]}');
  assert.deepEqual(await readStorageFormat(root), format(1));
  const store = createRoomStore(root); store.close();
  const before = await readFile(legacy), dbBefore = await readFile(path.join(root, 'rooms-v2.sqlite'));
  assert.deepEqual(await readStorageFormat(root), format(2));
  assert.throws(() => assertStorageCompatible(image(1, jsonOnly), format(2)), /storage_reader_incompatible/);
  assert.deepEqual(await readFile(legacy), before);
  assert.deepEqual(await readFile(path.join(root, 'rooms-v2.sqlite')), dbBefore);
});

test('empty, corrupt, orphan and unknown SQLite are not interpreted as an empty or legacy volume', async t => {
  for (const variant of ['zero', 'garbage', 'orphan', 'unknown', 'missing-tables']) {
    const root = await directory(t), filename = path.join(root, 'rooms-v2.sqlite');
    if (variant === 'zero') await writeFile(filename, '');
    if (variant === 'garbage') await writeFile(filename, Buffer.alloc(4096, 7));
    if (variant === 'orphan') await writeFile(filename + '-wal', Buffer.alloc(100));
    if (variant === 'unknown' || variant === 'missing-tables') {
      const db = new DatabaseSync(filename); db.exec('CREATE TABLE test(value); PRAGMA user_version=' + (variant === 'unknown' ? '3' : '2')); db.close();
    }
    await assert.rejects(readStorageFormat(root), /storage_format_(unreadable|unknown)/);
  }
});

test('probe observes committed WAL user_version and never ignores it with immutable mode', async t => {
  const root = await directory(t); const store = createRoomStore(root); store.close();
  const db = new DatabaseSync(path.join(root, 'rooms-v2.sqlite'));
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA user_version=3');
    await assert.rejects(readStorageFormat(root), /storage_format_unknown/);
    assert.equal(db.prepare('PRAGMA user_version').get().user_version, 3);
  } finally { db.close(); }
});

test('strict image reader manifest rejects missing, extended and malformed claims', () => {
  for (const text of [undefined, '', '{}', '{"version":2,"readers":{"rooms":[1,2]}}', '{"version":1,"readers":{"rooms":[3]}}', '{"version":1,"readers":{"rooms":[1,1]}}', '{"version":1,"readers":{"rooms":[1]},"allow":true}']) {
    assert.throws(() => assertStorageCompatible(image(1, text || null), format(1)), /storage_reader_unknown/);
  }
  assert.throws(() => checkedStorageFormat({ ok: true, rooms: 2 }), /storage_probe_invalid/);
  assert.throws(() => requireStorageStartReceipt(null, id(1)), /storage_start_guard_missing/);
});

function fixture({ readers = currentStorageReaders, rooms = 2, pending = false } = {}) {
  const runtime = { Id: id(1), Image: imageId(2), Name: '/soty-online-chat', State: { Running: false, Status: 'created' },
    Config: { Env: ['DATA_DIR=/data', 'TOKEN=synthetic-never-in-probe'], Labels: { [storageReaderLabel]: currentStorageReaders } },
    Mounts: [{ Type: 'volume', Name: 'live-data', Source: '/docker/volumes/live-data/_data', Destination: '/data', RW: true }] };
  const images = new Map([[imageId(2), image(2, readers)], [imageId(3), image(3, null)]]);
  const containers = new Map([[runtime.Id, runtime]]), events = [], state = {}, receipts = [];
  const volume = { Name: 'live-data', Driver: 'local', Scope: 'local', Options: null, Mountpoint: runtime.Mounts[0].Source };
  let helperCount = 0, result = format(rooms), dropStart = false;
  const engine = {
    async inspect(key) { const c = containers.get(key) || [...containers.values()].find(c => c.Name === '/' + key); if (!c) throw new Error('404'); return clone(c); },
    async image(key) { if (!images.has(key)) throw new Error('404'); return clone(images.get(key)); },
    async request(method, route) { assert.equal(method, 'GET'); if (route === '/volumes/live-data') return clone(volume); assert.equal(route, '/containers/json'); return clone([...containers.values()].filter(c => c.State.Running)); },
    async create(name, body) {
      events.push({ verb: 'create', body: clone(body) });
      if (pending) throw new Error('lost');
      const Id = id(10 + ++helperCount), { HostConfig, NetworkingConfig, ...Config } = clone(body);
      containers.set(Id, { Id, Image: body.Image, Name: '/' + name, Config, HostConfig,
        Mounts: [{ ...runtime.Mounts[0], RW: false }], State: { Running: false, Status: 'created' } });
      return { Id };
    },
    async start(key) { events.push({ verb: 'start', id: key }); containers.get(key).State = { Running: false, Status: 'exited', ExitCode: result.ok ? 0 : 1 }; if (dropStart) throw new Error('lost'); },
    async helperOutput() { return clone(result); },
  };
  const context = { engine, probeImage: imageId(3), transactionId: 'a'.repeat(32), getState: () => state,
    record: async fields => { Object.assign(state, fields); receipts.push(clone(fields)); }, maxPolls: 2, wait: async () => {} };
  return { runtime, containers, images, engine, context, state, events, receipts, volume,
    setResult: value => { result = value; }, dropStart: () => { dropStart = true; } };
}

test('start gate uses actual image, pinned helper, only read-only data mount and no candidate secrets/code', async () => {
  const f = fixture(); f.dropStart();
  const receipt = await guardStorageStart(f.context, f.runtime);
  requireStorageStartReceipt(receipt, f.runtime.Id);
  const created = f.events.find(e => e.verb === 'create').body;
  assert.equal(created.Image, imageId(3)); assert.notEqual(created.Image, f.runtime.Image);
  assert.deepEqual(created.Env, ['SOTY_STORAGE_PROBE=1']);
  assert.equal(created.HostConfig.NetworkMode, 'none'); assert.equal(created.HostConfig.ReadonlyRootfs, true);
  assert.equal(created.HostConfig.PortBindings, undefined); assert.equal(created.HostConfig.Binds, undefined);
  assert.deepEqual(created.HostConfig.Mounts, [{ Type: 'volume', Source: 'live-data', Target: '/data', ReadOnly: true, VolumeOptions: { NoCopy: true } }]);
  assert.doesNotMatch(JSON.stringify(created), /synthetic-never-in-probe|server\/index\.js/);
  assert.equal(f.events.filter(e => e.verb === 'start').length, 1);
  assert.notEqual(f.events.find(e => e.verb === 'start').id, f.runtime.Id);
  assert.equal(f.state.storageGuardHelper, null);
  assert.doesNotMatch(JSON.stringify(f.receipts), /TOKEN|synthetic-never-in-probe|live-data/);
});

test('container label cannot make a legacy or unlabelled image compatible with v2', async () => {
  for (const readers of [jsonOnly, null]) {
    const f = fixture({ readers });
    await assert.rejects(guardStorageStart(f.context, f.runtime), readers ? /storage_reader_incompatible/ : /storage_reader_unknown/);
    assert.ok(!f.events.some(e => e.verb === 'start' && e.id === f.runtime.Id));
  }
});

test('another writer or a changed volume prevents application start, including after the probe', async () => {
  for (const late of [false, true]) {
    const f = fixture();
    const addWriter = () => f.containers.set(id(90), { Id: id(90), State: { Running: true }, Mounts: clone(f.runtime.Mounts) });
    if (late) { const output = f.engine.helperOutput; f.engine.helperOutput = async () => { addWriter(); return output(); }; }
    else addWriter();
    await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_other_writer/);
  }
  const f = fixture(); const output = f.engine.helperOutput;
  f.engine.helperOutput = async () => { f.runtime.Mounts[0].Name = 'other-data'; return output(); };
  await assert.rejects(guardStorageStart(f.context, { ...f.runtime }), /storage_runtime_changed/);
});

test('nested data mounts cannot hide another room database from the isolated format probe', async () => {
  const f = fixture();
  f.runtime.Mounts.push({ Type: 'bind', Source: '/another/rooms-v2.sqlite', Destination: '/data/rooms-v2.sqlite', RW: true });
  await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_data_mount_overlay/);
  assert.equal(f.events.length, 0);
});

test('an absent or ambiguous DATA_DIR never assumes the mounted data is the application data', async () => {
  for (const Env of [[], ['DATA_DIR='], ['DATA_DIR=/app/data'], ['DATA_DIR=/data', 'DATA_DIR=/data']]) {
    const f = fixture(); f.runtime.Config.Env = Env;
    await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_data_directory_unsupported/);
    assert.equal(f.events.length, 0);
  }
});

test('volume subpath refuses before probing the parent volume or starting any helper', async () => {
  const f = fixture();
  f.runtime.HostConfig = { Mounts: [{ Type: 'volume', Source: 'live-data', Target: '/data', VolumeOptions: { Subpath: 'tenant' } }] };
  await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_data_subpath_unsupported/);
  assert.equal(f.events.length, 0);
});

test('only the exact standard local volume without driver options is supported', async () => {
  for (const change of [{ Driver: 'custom' }, { Scope: 'global' }, { Options: { type: 'none', o: 'bind', device: '/alias' } },
    { Name: 'other-data' }, { Mountpoint: '/different/root' }]) {
    const f = fixture(); Object.assign(f.volume, change);
    await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_volume_profile_unsupported/);
    assert.equal(f.events.length, 0);
  }
  const bind = fixture(); bind.runtime.Mounts[0].Type = 'bind';
  await assert.rejects(guardStorageStart(bind.context, bind.runtime), /storage_data_mount_invalid/);
  assert.equal(bind.events.length, 0);
  const late = fixture(); const output = late.engine.helperOutput;
  late.engine.helperOutput = async () => { late.volume.Options = { device: '/changed' }; return output(); };
  await assert.rejects(guardStorageStart(late.context, late.runtime), /storage_volume_profile_unsupported/);
});

test('unresolved helper creation is journaled and never blindly created or started again', async () => {
  const f = fixture({ pending: true });
  await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_helper_unresolved/);
  assert.equal(f.state.storageGuardHelper.state, 'creating');
  await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_helper_unresolved/);
  assert.equal(f.events.filter(e => e.verb === 'create').length, 1);
  assert.equal(f.events.filter(e => e.verb === 'start').length, 0);
});

test('failed probe remains explicit and never becomes a successful start receipt', async () => {
  const f = fixture(); f.setResult({ ok: false, code: 'storage_format_unreadable' });
  await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_format_unreadable/);
  assert.ok(f.state.storageGuardHelper);
  await assert.rejects(reconcileStorageProbe(f.context), /storage_format_unreadable/);
  assert.equal(f.events.filter(e => e.verb === 'start').length, 1);
});

test('runtime image and reader labels in both Dockerfiles match the current accepted reader contract', async () => {
  for (const file of [new URL('../../Dockerfile', import.meta.url), new URL('./Dockerfile', import.meta.url)]) {
    const source = await readFile(file, 'utf8');
    assert.ok(source.includes('LABEL ' + storageReaderLabel + '="' + currentStorageReaders.replaceAll('"', '\\"') + '"'));
  }
});
