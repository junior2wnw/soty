import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { spawnSync } from 'node:child_process';
import { createRoomStore } from '../../server/room-store.js';
import { readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, checkedStorageFormat, currentStorageReaders, guardStorageStart,
  reconcileStorageProbe, requireStorageStartReceipt, storageReaderLabel } from './storage-guard.mjs';

const id = n => n.toString(16).padStart(64, '0');
const imageId = n => 'sha256:' + id(n);
const clone = x => structuredClone(x);
const format = (rooms, apps = 'empty', notes = 'empty', capabilities = 'empty') => ({ ok: true, schema: 'soty.storage-format.v3', rooms, apps, notes, capabilities });
const image = (n, readers = currentStorageReaders) => ({ Id: imageId(n), Config: { Labels: readers ? { [storageReaderLabel]: readers } : {} } });
const jsonOnly = JSON.stringify({ version: 3, readers: { rooms: [1], apps: [1, 2], notes: [1], capabilities: [1] } });

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
  for (const text of [undefined, '', '{}', '{"version":1,"readers":{"rooms":[1,2]}}', '{"version":2,"readers":{"rooms":[1,2]}}',
    '{"version":2,"readers":{"rooms":[1,2],"apps":[7]}}', '{"version":2,"readers":{"rooms":[1,2,3],"apps":[1,2,3]}}',
    '{"version":2,"readers":{"rooms":[1,1],"apps":[1]}}',
    '{"version":2,"readers":{"rooms":[1],"apps":[1,1]}}', '{"version":2,"readers":{"rooms":[],"apps":[1]}}',
    '{"version":2,"readers":{"rooms":[1],"apps":[]}}', '{"version":2,"readers":{"rooms":[1],"apps":[1]},"allow":true}',
    '{"version":2,"readers":{"rooms":[1],"apps":[1],"notes":[1]}}']) {
    assert.throws(() => assertStorageCompatible(image(1, text || null), format(1)), /storage_reader_unknown/);
  }
  assert.throws(() => checkedStorageFormat({ ok: true, rooms: 2 }), /storage_probe_invalid/);
  assert.throws(() => requireStorageStartReceipt(null, id(1)), /storage_start_guard_missing/);
  const baseline = JSON.parse(currentStorageReaders);
  for (const store of ['rooms', 'apps', 'notes', 'capabilities']) {
    for (const readers of [[], [1, 1], ['1'], [null], '1', [[1]], store === 'rooms' ? [3] : store === 'apps' ? [7] : [2]]) {
      const value = clone(baseline); value.readers[store] = readers;
      assert.throws(() => assertStorageCompatible(image(1, JSON.stringify(value)), format('empty')), /storage_reader_unknown/);
    }
    const value = clone(baseline); delete value.readers[store];
    assert.throws(() => assertStorageCompatible(image(1, JSON.stringify(value)), format('empty')), /storage_reader_unknown/);
  }
  for (const value of [{ ...baseline, allow: true }, { ...baseline, version: '3' },
    { ...baseline, readers: { ...baseline.readers, world: [3] } }]) {
    assert.throws(() => assertStorageCompatible(image(1, JSON.stringify(value)), format('empty')), /storage_reader_unknown/);
  }
});

function fixture({ readers = currentStorageReaders, rooms = 2, apps = 'empty', notes = 'empty', capabilities = 'empty', pending = false } = {}) {
  const runtime = { Id: id(1), Image: imageId(2), Name: '/soty-online-chat', State: { Running: false, Status: 'created' },
    Config: { Env: ['DATA_DIR=/data', 'TOKEN=synthetic-never-in-probe'], Labels: { [storageReaderLabel]: currentStorageReaders } },
    Mounts: [{ Type: 'volume', Name: 'live-data', Source: '/docker/volumes/live-data/_data', Destination: '/data', RW: true }] };
  const images = new Map([[imageId(2), image(2, readers)], [imageId(3), image(3, null)]]);
  const containers = new Map([[runtime.Id, runtime]]), events = [], state = {}, receipts = [];
  const volume = { Name: 'live-data', Driver: 'local', Scope: 'local', Options: null, Mountpoint: runtime.Mounts[0].Source };
  let helperCount = 0, result = format(rooms, apps, notes, capabilities), dropStart = false;
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
  assert.equal(receipt.schema, 'soty.storage-start.v3'); assert.equal(receipt.rooms, 2); assert.equal(receipt.apps, 'empty');
  assert.equal(receipt.notes, 'empty'); assert.equal(receipt.capabilities, 'empty');
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

test('container label cannot make a legacy, rooms-only or unlabelled image compatible with v2', async () => {
  for (const readers of [jsonOnly, '{"version":1,"readers":{"rooms":[1,2]}}', null]) {
    const f = fixture({ readers });
    await assert.rejects(guardStorageStart(f.context, f.runtime), readers === jsonOnly ? /storage_reader_incompatible/ : /storage_reader_unknown/);
    assert.ok(!f.events.some(e => e.verb === 'start' && e.id === f.runtime.Id));
  }
});

test('Apps version is an independent reader requirement and is retained in the start receipt', async () => {
  const readers = JSON.stringify({ version: 3, readers: { rooms: [1, 2], apps: [1], notes: [1], capabilities: [1] } });
  const old = fixture({ readers, apps: 2 });
  await assert.rejects(guardStorageStart(old.context, old.runtime), /storage_reader_incompatible/);
  assert.ok(!old.events.some(event => event.verb === 'start' && event.id === old.runtime.Id));
  assertStorageCompatible(image(4, readers), format(2, 1));
  const current = fixture({ apps: 2 });
  const receipt = await guardStorageStart(current.context, current.runtime);
  requireStorageStartReceipt(receipt, current.runtime.Id); assert.equal(receipt.apps, 2);
});

for (const version of [3, 4, 5, 6]) test(`Apps${version} is accepted independently of the Rooms ceiling and blocks an Apps${version - 1}-only actual image`, async () => {
  const readers = JSON.stringify({ version: 3, readers: { rooms: [1, 2], apps: Array.from({ length: version - 1 }, (_, i) => i + 1), notes: [1], capabilities: [1] } });
  const old = fixture({ readers, apps: version });
  await assert.rejects(guardStorageStart(old.context, old.runtime), /storage_reader_incompatible/);
  assert.ok(!old.events.some(event => event.verb === 'start' && event.id === old.runtime.Id));
  assertStorageCompatible(image(4, readers), format(2, version - 1));
  const current = fixture({ apps: version });
  const receipt = await guardStorageStart(current.context, current.runtime);
  requireStorageStartReceipt(receipt, current.runtime.Id); assert.equal(receipt.apps, version); assert.equal(receipt.rooms, 2);
  for (const apps of ['empty', 1, 2, 3, 4, 5, 6]) assertStorageCompatible(image(2), format(2, apps));
  assert.throws(() => checkedStorageFormat(format(3, version)), /storage_probe_invalid/);
  assert.throws(() => assertStorageCompatible(image(2), format(2, 7)), /storage_probe_invalid/);
});

test('old or extended probe and start receipts never authorize a restart', () => {
  const good = { schema: 'soty.storage-start.v3', containerId: id(1), image: imageId(2), mountSha256: id(3), rooms: 2, apps: 2, notes: 1, capabilities: 1 };
  requireStorageStartReceipt(good, id(1));
  for (const value of [{ ok: true, schema: 'soty.storage-format.v1', rooms: 2 },
    { ok: true, schema: 'soty.storage-format.v2', rooms: 2, apps: 6 },
    { ...format(2), schema: 'soty.storage-format.v1' }, { ...format(2), schema: 'soty.storage-format.v2' }, { ...format(2), apps: 7 }, { ...format(2, 6), rooms: 3 },
    { ...format(2), apps: undefined }, { ...format(2), complete: true }]) {
    assert.throws(() => checkedStorageFormat(value), /storage_probe_invalid/);
  }
  for (const value of [{ schema: 'soty.storage-start.v1', containerId: id(1), image: imageId(2), mountSha256: id(3), rooms: 2 },
    { schema: 'soty.storage-start.v2', containerId: id(1), image: imageId(2), mountSha256: id(3), rooms: 2, apps: 6 },
    { ...good, schema: 'soty.storage-start.v1' }, { ...good, schema: 'soty.storage-start.v2' }, { ...good, apps: 7 }, { ...good, rooms: 3 },
    { ...good, apps: undefined }, { ...good, allowed: true }]) {
    assert.throws(() => requireStorageStartReceipt(value, id(1)), /storage_start_guard_missing/);
  }
});

test('bridge requires actual v3 even on empty stores and preserves every independent format in start evidence', async () => {
  for (const notes of ['empty', 1]) for (const capabilities of ['empty', 1]) {
    const f = fixture({ rooms: 2, apps: 6, notes, capabilities });
    const receipt = await guardStorageStart(f.context, f.runtime);
    requireStorageStartReceipt(receipt, f.runtime.Id);
    assert.deepEqual([receipt.rooms, receipt.apps, receipt.notes, receipt.capabilities], [2, 6, notes, capabilities]);
    const old = fixture({ readers: '{"version":2,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6]}}', notes, capabilities });
    await assert.rejects(guardStorageStart(old.context, old.runtime), /storage_reader_unknown/);
    assert.equal(old.events.length, 0, 'a copied v3 container label cannot authorize even a helper for an actual v2 image');
  }
});

test('new store output and evidence require scalar v1 or empty, never coerced or omitted fields', () => {
  const goodFormat = format(2, 6, 1, 1);
  const goodReceipt = { schema: 'soty.storage-start.v3', containerId: id(1), image: imageId(2), mountSha256: id(3),
    rooms: 2, apps: 6, notes: 1, capabilities: 1 };
  for (const store of ['notes', 'capabilities']) {
    for (const value of [undefined, null, 0, 2, '1', [1], ['empty'], true, {}, 'unknown']) {
      assert.throws(() => checkedStorageFormat({ ...goodFormat, [store]: value }), /storage_probe_invalid/);
      assert.throws(() => requireStorageStartReceipt({ ...goodReceipt, [store]: value }, id(1)), /storage_start_guard_missing/);
    }
    const output = { ...goodFormat }, receipt = { ...goodReceipt };
    delete output[store]; delete receipt[store];
    assert.throws(() => checkedStorageFormat(output), /storage_probe_invalid/);
    assert.throws(() => requireStorageStartReceipt(receipt, id(1)), /storage_start_guard_missing/);
  }
  for (const field of ['containerId', 'image', 'mountSha256']) {
    const receipt = { ...goodReceipt, [field]: [goodReceipt[field]] };
    assert.throws(() => requireStorageStartReceipt(receipt, receipt.containerId), /storage_start_guard_missing/);
  }
  assert.throws(() => assertStorageCompatible({ ...image(1), Id: [imageId(1)] }, goodFormat), /storage_image_identity_invalid/);
  assert.throws(() => assertStorageCompatible(image(1, [currentStorageReaders]), goodFormat), /storage_reader_unknown/);
});

test('a retained v2 helper is refused without repeating CREATE or START or rewriting its journal', async () => {
  const f = fixture();
  await guardStorageStart(f.context, f.runtime);
  const helper = [...f.containers.values()].find(c => c.Id !== f.runtime.Id);
  f.state.storageGuardHelper = { id: helper.Id, name: helper.Name.slice(1), image: helper.Image, state: 'starting' };
  f.setResult({ ok: true, schema: 'soty.storage-format.v2', rooms: 2, apps: 6 });
  const before = clone(f.state.storageGuardHelper), eventCount = f.events.length;
  await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_probe_invalid/);
  assert.equal(f.events.length, eventCount);
  assert.deepEqual(f.state.storageGuardHelper, before);
});

test('future Notes or Capabilities cannot become successful helper or start receipts', async () => {
  for (const store of ['notes', 'capabilities']) {
    const f = fixture({ notes: 1, capabilities: 1 });
    f.setResult({ ...format(2, 6, 1, 1), [store]: 2 });
    await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_probe_invalid/);
    assert.ok(f.state.storageGuardHelper);
    assert.ok(!f.events.some(event => event.verb === 'start' && event.id === f.runtime.Id));
  }
});

for (const previous of [3, 4, 5]) test(`a completed Apps${previous} helper receipt is reconciled once but cannot replace a fresh Apps${previous + 1} probe`, async () => {
  const readers = JSON.stringify({ version: 3, readers: { rooms: [1, 2], apps: Array.from({ length: previous }, (_, index) => index + 1), notes: [1], capabilities: [1] } });
  const f = fixture({ readers, apps: previous });
  const receipt = await guardStorageStart(f.context, f.runtime);
  assert.equal(receipt.apps, previous);
  const old = [...f.containers.values()].find(c => c.Id !== f.runtime.Id);
  // A crash left the already executed helper recorded, with an old result.
  f.state.storageGuardHelper = { id: old.Id, name: old.Name.slice(1), image: old.Image, state: 'starting' };
  f.engine.helperOutput = async id => format(2, id === old.Id ? previous : previous + 1);
  await assert.rejects(guardStorageStart(f.context, f.runtime), /storage_reader_incompatible/);
  assert.equal(f.events.filter(e => e.verb === 'start' && e.id === old.Id).length, 1);
  assert.equal(f.events.filter(e => e.verb === 'create').length, 2);
  assert.ok(!f.events.some(e => e.verb === 'start' && e.id === f.runtime.Id));
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

test('the full application image declares the accepted readers beside its real module and dependency copies', async () => {
  const source = await readFile(new URL('../../Dockerfile', import.meta.url), 'utf8');
  assert.ok(source.includes('LABEL ' + storageReaderLabel + '="' + currentStorageReaders.replaceAll('"', '\\"') + '"'));
  assert.match(source, /^COPY --from=build \/app\/modules \.\/modules$/mu);
  assert.match(source, /^COPY --from=build \/app\/node_modules \.\/node_modules$/mu);
  assert.match(source, /^COPY --from=build \/app\/contracts \.\/contracts$/mu);
});

test('the historical backend overlay fails before any source copy or label and instructs a full application build', async () => {
  const source = await readFile(new URL('./Dockerfile', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /^\s*(?:COPY|ADD|LABEL)\s/mu);
  const instruction = source.match(/^RUN (\[[^\r\n]+\])$/mu);
  assert.ok(instruction); assert.ok(source.indexOf('FROM ${BASE_IMAGE}') < instruction.index);
  assert.equal(source.trim().slice(instruction.index), instruction[0]);
  const argv = JSON.parse(instruction[1]); assert.deepEqual(argv.slice(0, 3), ['node', '--input-type=module', '-e']);
  const result = spawnSync(process.execPath, argv.slice(1), { encoding: 'utf8', timeout: 5000 });
  assert.equal(result.error, undefined); assert.equal(result.status, 1); assert.equal(result.stdout, '');
  assert.match(result.stderr, /backend overlay is unsupported.*full application.*root Dockerfile/u);
});
