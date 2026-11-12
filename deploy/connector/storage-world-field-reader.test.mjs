import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, mkdir, mkdtemp, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { currentObservation } from './test-support/current-storage-observation.mjs';
import { currentStorageReaders, guardStorageStart, requireStorageStartReceipt } from './storage-guard.mjs';
import { checkWorldFieldReader, WORLD_FIELD_READER_RESULT, worldFieldReaderResult } from './world-field-reader-probe.mjs';

const id = n => String(n).repeat(64), imageId = 'sha256:' + id(1), helperId = id(3), transaction = 'a'.repeat(32);
const hash = value => createHash('sha256').update(value).digest('hex');
const fail = code => Object.assign(Error(code), { code });
function fixture({ world = 8, declared = [6, 7], changeReaders, output,
  createDrop = false, missingCreate = false, startDrop = false, unresolvedStart = false, runningTimeout = false, mutate } = {}) {
  output ??= worldFieldReaderResult(world - 2);
  const label = JSON.parse(currentStorageReaders); label.readers.world = declared; changeReaders?.(label.readers);
  const image = { Id: imageId, Config: { Labels: { 'io.soty.storage.readers': JSON.stringify(label) },
    Env: ['NODE_ENV=production', 'DATA_DIR=/data'], Volumes: { '/data': {} } } };
  const runtime = { Id: id(2), Image: imageId, State: { Running: false }, Config: { Env: ['DATA_DIR=/data'] },
    Mounts: [{ Type: 'volume', Name: 'owned-data', Source: '/volumes/owned-data/_data', Destination: '/data', RW: true }] };
  let state = {}, helper, creates = 0, starts = 0, applicationStarts = 0;
  const observed = currentObservation({ world });
  const engine = {
    async image(value) { assert.equal(value, imageId); return structuredClone(image); },
    async request(method, path) {
      if (path === '/containers/json') return [];
      if (path === '/volumes/owned-data') return { Name: 'owned-data', Driver: 'local', Scope: 'local',
        Mountpoint: runtime.Mounts[0].Source, Options: null };
      throw Error('unexpected request');
    },
    async create(name, body) {
      creates++;
      if (missingCreate) throw fail('engine_response_ambiguous');
      const env = new Map(image.Config.Env.map(value => [value.split('=')[0], value]));
      for (const value of body.Env) env.set(value.split('=')[0], value);
      helper = { Id: helperId, Name: '/' + name, Image: body.Image,
        Config: { ...structuredClone(body), Env: [...env.values()] }, HostConfig: structuredClone(body.HostConfig),
        Mounts: [], State: { Running: false, Status: 'created', ExitCode: 0, OOMKilled: false } };
      mutate?.(helper);
      if (createDrop) throw fail('engine_response_ambiguous');
      return { Id: helperId };
    },
    async inspect(value) {
      if (value === runtime.Id) return structuredClone(runtime);
      if (helper && [helperId, helper.Name.slice(1)].includes(value)) return structuredClone(helper);
      throw fail('engine_http_404');
    },
    async start(value) {
      assert.equal(value, helperId); starts++;
      if (runningTimeout) helper.State = { Running: true, Status: 'running', ExitCode: 0, OOMKilled: false };
      else if (!unresolvedStart) helper.State = { Running: false, Status: 'exited', ExitCode: output.ok ? 0 : 1, OOMKilled: false };
      if (startDrop || unresolvedStart) throw fail('engine_response_ambiguous');
    },
    async helperOutput(value) { assert.equal(value, helperId); return structuredClone(output); },
  };
  const context = { engine, transactionId: transaction, getState: () => state,
    record: async fields => { state = { ...state, ...fields }; }, probe: async () => observed, wait: async () => {}, maxPolls: 2 };
  return { context, runtime, observed, get state() { return state; },
    counts: () => ({ creates, starts, applicationStarts }),
    async startApplication() { const receipt = await guardStorageStart(context, runtime); requireStorageStartReceipt(receipt, runtime.Id); applicationStarts++; return receipt; } };
}

test('actual field runtime passes the host literal free-placement, frozen layout and no-rewrite conformance', async () => {
  assert.deepEqual(await checkWorldFieldReader(new URL('../../modules/world/server/field.mjs', import.meta.url).href), WORLD_FIELD_READER_RESULT);
  assert.deepEqual(await checkWorldFieldReader(new URL('../../modules/world/server/field.mjs', import.meta.url).href, 7), worldFieldReaderResult(7));
  await assert.rejects(checkWorldFieldReader(new URL('../../modules/world/server/field.mjs', import.meta.url).href, 5));
});

async function mutantModule(t, old, replacement) {
  const root = await mkdtemp(join(tmpdir(), 'soty-world-field-reader-'));
  t.after(async () => { assert.equal(dirname(resolve(root)), resolve(tmpdir())); await rm(root, { recursive: true, force: true }); });
  for (const file of ['modules/world/server/field.mjs', 'modules/field/contract.mjs', 'modules/world/server/validation.mjs']) {
    const target = join(root, file); await mkdir(dirname(target), { recursive: true });
    let bytes = await readFile(new URL('../../' + file, import.meta.url), 'utf8');
    if (file.endsWith('/field.mjs')) {
      assert.equal(bytes.split(old).length, 2); bytes = bytes.replace(old, replacement);
    }
    await writeFile(target, bytes);
  }
  return pathToFileURL(join(root, 'modules/world/server/field.mjs')).href;
}

test('conformance rejects an epoch1-only migrator even when its own v2 writer accepts the fixture', async t => {
  const url = await mutantModule(t, "assert(['1','2'].includes(version), 'field_schema_unsupported')", "assert(version==='1', 'field_schema_unsupported')");
  await assert.rejects(checkWorldFieldReader(url));
});

test('a target cannot redefine both the synthetic field layout and its own layout oracle', async t => {
  const url = await mutantModule(t, 'CHECK(revision>=1)', 'CHECK(revision>=0)');
  await assert.rejects(checkWorldFieldReader(url));
});

test('qualification exercises the requested physical-world base instead of assuming World6 implies World7', async t => {
  const url = await mutantModule(t, "db.exec('BEGIN IMMEDIATE');",
    "if(db.prepare('PRAGMA user_version').get().user_version===7)throw Error('world7_field_unsupported');db.exec('BEGIN IMMEDIATE');");
  assert.deepEqual(await checkWorldFieldReader(url, 6), worldFieldReaderResult(6));
  await assert.rejects(checkWorldFieldReader(url, 7));
  const f = fixture({ world: 9, output: WORLD_FIELD_READER_RESULT });
  await assert.rejects(f.startApplication(), { code: 'storage_reader_incompatible' });
  assert.equal(f.counts().applicationStarts, 0);
});

for (const world of [8, 9]) test(`legacy image obtains an image-bound start.v11 proof for logicalWorld${world}`, async () => {
  const f = fixture({ world }), receipt = await f.startApplication();
  assert.equal(receipt.schema, 'soty.storage-start.v11'); assert.equal(receipt.world, world);
  assert.equal(receipt.worldFieldReader.image, imageId); assert.equal(receipt.worldFieldReader.helperId, helperId);
  assert.equal(receipt.worldFieldReader.worldFormat, world); assert.equal(receipt.worldFieldReader.baseWorldFormat, world - 2);
  assert.equal(f.state.storageWorldFieldProof.resultSha256, receipt.worldFieldReader.resultSha256);
  assert.equal(f.state.storageGuardHelper, null); assert.deepEqual(f.counts(), { creates: 1, starts: 1, applicationStarts: 1 });
  await f.startApplication(); assert.deepEqual(f.counts(), { creates: 2, starts: 2, applicationStarts: 2 }, 'every guard obtains a fresh proof');
});

test('explicit format8/9 declaration needs no legacy refinement and retains strict start.v10', async () => {
  const f = fixture({ declared: [6, 7, 8, 9] }), receipt = await f.startApplication();
  assert.equal(receipt.schema, 'soty.storage-start.v10'); assert.equal(Object.hasOwn(receipt, 'worldFieldReader'), false);
  assert.deepEqual(f.counts(), { creates: 0, starts: 0, applicationStarts: 1 });
});

test('all other stores and the physical-world base are checked before any conformance CREATE', async () => {
  for (const f of [fixture({ declared: [7] }), fixture({ changeReaders(readers) { readers.notes = [1]; } })]) {
    if (f.observed.world === 8 && f.context && f.observed.notes === 'empty') f.observed.notes = 3;
    await assert.rejects(f.startApplication(), { code: 'storage_reader_incompatible' });
    assert.deepEqual(f.counts(), { creates: 0, starts: 0, applicationStarts: 0 });
  }
});

for (const [label, output] of [
  ['false proof', { ok: false, schema: 'soty.world-field-reader.v1', code: 'world_field_reader_incompatible' }],
  ['wrong fixture', { ...WORLD_FIELD_READER_RESULT, fixtureSha256: id(9) }],
  ['wrong layout', { ...WORLD_FIELD_READER_RESULT, layoutSha256: id(9) }],
  ['extra key', { ...WORLD_FIELD_READER_RESULT, trusted: true }],
]) test(`a ${label} result never admits the application`, async () => {
  const f = fixture({ output }); await assert.rejects(f.startApplication(), { code: 'storage_reader_incompatible' });
  assert.equal(f.counts().applicationStarts, 0); assert.ok(f.state.storageGuardHelper);
});

for (const [label, mutate] of [
  ['image', helper => { helper.Image = 'sha256:' + id(9); }],
  ['script', helper => { helper.Config.Cmd[2] += '\n// changed'; }],
  ['production mount', helper => { helper.Mounts.push({ Type: 'volume', Source: '/production', Destination: '/data' }); }],
  ['inherited node options', helper => { helper.Config.Env.push('NODE_OPTIONS=--require=/private'); }],
  ['physical-world input', helper => { helper.Config.Env = helper.Config.Env.map(value => value === 'SOTY_WORLD_FIELD_BASE=6' ? 'SOTY_WORLD_FIELD_BASE=7' : value); }],
  ['capability', helper => { helper.HostConfig.CapAdd = ['DAC_READ_SEARCH']; }],
  ['open stdin', helper => { helper.Config.OpenStdin = true; }],
  ['host PID namespace', helper => { helper.HostConfig.PidMode = 'host'; }],
  ['host IPC namespace', helper => { helper.HostConfig.IpcMode = 'host'; }],
  ['host UTS namespace', helper => { helper.HostConfig.UTSMode = 'host'; }],
  ['host user namespace', helper => { helper.HostConfig.UsernsMode = 'host'; }],
  ['host cgroup namespace', helper => { helper.HostConfig.CgroupnsMode = 'host'; }],
  ['host device', helper => { helper.HostConfig.Devices = [{ PathOnHost: '/dev/sda', PathInContainer: '/dev/host', CgroupPermissions: 'r' }]; }],
  ['device request', helper => { helper.HostConfig.DeviceRequests = [{ Count: -1, Capabilities: [['gpu']] }]; }],
  ['device cgroup rule', helper => { helper.HostConfig.DeviceCgroupRules = ['a *:* rwm']; }],
  ['additional group', helper => { helper.HostConfig.GroupAdd = ['docker']; }],
  ['volume inheritance', helper => { helper.HostConfig.VolumesFrom = ['production']; }],
  ['host container-id file', helper => { helper.HostConfig.ContainerIDFile = '/production/helper.id'; }],
]) test(`a helper with a mismatched ${label} cannot START`, async () => {
  const f = fixture({ mutate }); await assert.rejects(f.startApplication());
  assert.equal(f.counts().starts, 0); assert.equal(f.counts().applicationStarts, 0);
});

for (const options of [{ createDrop: true }, { startDrop: true }]) test(`lost ${options.createDrop ? 'CREATE' : 'START'} response is reconciled without repeating the mutation`, async () => {
  const f = fixture(options); await f.startApplication();
  assert.deepEqual(f.counts(), { creates: 1, starts: 1, applicationStarts: 1 });
});

for (const options of [{ missingCreate: true }, { unresolvedStart: true }, { runningTimeout: true }]) test(`an unresolved ${options.missingCreate ? 'CREATE' : options.runningTimeout ? 'running timeout' : 'START'} blocks a later helper and application START`, async () => {
  const f = fixture(options); await assert.rejects(f.startApplication(), { code: 'storage_helper_unresolved' });
  const before = f.counts(); await assert.rejects(f.startApplication(), { code: 'storage_helper_unresolved' });
  assert.deepEqual(f.counts(), before); assert.equal(before.applicationStarts, 0); assert.ok(f.state.storageGuardHelper);
});

test('a recorded prior proof never bypasses a failed fresh target-image qualification', async () => {
  const accepted = fixture(), prior = await accepted.startApplication();
  const denied = fixture({ output: { ok: false, schema: 'soty.world-field-reader.v1', code: 'world_field_reader_incompatible' } });
  denied.state.storageWorldFieldProof = prior.worldFieldReader;
  await assert.rejects(denied.startApplication(), { code: 'storage_reader_incompatible' });
  assert.deepEqual(denied.counts(), { creates: 1, starts: 1, applicationStarts: 0 });
});

test('start.v11 receipt rejects forged binding/fixture/script and never widens v3..v10', async () => {
  const f = fixture(), valid = await f.startApplication();
  for (const patch of [{ image: 'sha256:' + id(9) }, { fixtureSha256: id(9) }, { scriptSha256: id(9) },
    { worldFormat: 9 }, { baseWorldFormat: 7 }, { helperId: 'not-an-id' }, { extra: true }]) {
    assert.throws(() => requireStorageStartReceipt({ ...valid, worldFieldReader: { ...valid.worldFieldReader, ...patch } }, valid.containerId), { code: 'storage_start_guard_missing' });
  }
  assert.throws(() => requireStorageStartReceipt({ ...valid, schema: 'soty.storage-start.v10' }, valid.containerId), { code: 'storage_start_guard_missing' });
  assert.throws(() => requireStorageStartReceipt({ ...valid, extra: true }, valid.containerId), { code: 'storage_start_guard_missing' });
  assert.match(hash(await readFile(new URL('./world-field-reader-probe.mjs', import.meta.url))), /^[a-f0-9]{64}$/u);
});
