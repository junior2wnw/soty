import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { SafeError } from './docker-api.mjs';

export const storageReaderLabel = 'io.soty.storage.readers';
export const currentStorageReaders = '{"version":2,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5]}}';
const ID = /^[a-f0-9]{64}$/u, IMAGE = /^sha256:[a-f0-9]{64}$/u;
const requireThat = (ok, code) => { if (!ok) throw new SafeError(code); };
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const keys = (value, expected) => value !== null && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).sort().join(',') === expected;
const supported = { rooms: [1, 2], apps: [1, 2, 3, 4, 5] };
const knownFormat = (store, value) => value === 'empty' || supported[store].includes(value);
const knownReaders = (store, value) => Array.isArray(value) && value.length > 0 && value.length <= supported[store].length
  && value.every(version => supported[store].includes(version)) && new Set(value).size === value.length;

export function storageReaders(image) {
  requireThat(IMAGE.test(image?.Id || ''), 'storage_image_identity_invalid');
  let value;
  try { value = JSON.parse(image.Config?.Labels?.[storageReaderLabel]); } catch { throw new SafeError('storage_reader_unknown'); }
  requireThat(keys(value, 'readers,version') && value.version === 2 && keys(value.readers, 'apps,rooms')
    && knownReaders('rooms', value.readers.rooms) && knownReaders('apps', value.readers.apps), 'storage_reader_unknown');
  return value.readers;
}

export function checkedStorageFormat(value) {
  requireThat(keys(value, 'apps,ok,rooms,schema') && value.ok === true && value.schema === 'soty.storage-format.v2'
    && knownFormat('rooms', value.rooms) && knownFormat('apps', value.apps), 'storage_probe_invalid');
  return { ok: true, schema: value.schema, rooms: value.rooms, apps: value.apps };
}

export function assertStorageCompatible(image, value) {
  const readers = storageReaders(image), format = checkedStorageFormat(value);
  requireThat(['rooms', 'apps'].every(store => format[store] === 'empty' || readers[store].includes(format[store])), 'storage_reader_incompatible');
  return format;
}

function dataMount(runtime) {
  const dirs = (runtime.Config?.Env || []).filter(v => v.startsWith('DATA_DIR='));
  requireThat(dirs.length === 1 && dirs[0] === 'DATA_DIR=/data', 'storage_data_directory_unsupported');
  const mounts = (runtime.Mounts || []).filter(m => m.Destination === '/data');
  requireThat(mounts.length === 1 && mounts[0].Type === 'volume' && mounts[0].RW === true, 'storage_data_mount_invalid');
  requireThat(!(runtime.Mounts || []).some(m => typeof m.Destination === 'string' && m.Destination.startsWith('/data/')), 'storage_data_mount_overlay');
  // Mounts.Source/Name alone do not identify a volume-subpath view. The probe
  // supports only a complete /data root and must never attest its parent volume.
  requireThat(!(runtime.HostConfig?.Mounts || []).some(m => m.Target === '/data'
    && m.VolumeOptions?.Subpath !== undefined && m.VolumeOptions.Subpath !== ''), 'storage_data_subpath_unsupported');
  const mount = mounts[0];
  requireThat(typeof mount.Source === 'string' && path.posix.isAbsolute(mount.Source), 'storage_data_mount_invalid');
  requireThat(typeof mount.Name === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}$/u.test(mount.Name), 'storage_data_mount_invalid');
  return { type: mount.Type, source: mount.Source, name: mount.Name };
}

async function assertLocalVolume(engine, mount) {
  const volume = await engine.request('GET', '/volumes/' + encodeURIComponent(mount.name));
  const noOptions = volume?.Options == null || (typeof volume.Options === 'object'
    && !Array.isArray(volume.Options) && Object.keys(volume.Options).length === 0);
  requireThat(volume?.Name === mount.name && volume.Driver === 'local' && volume.Scope === 'local'
    && volume.Mountpoint === mount.source && noOptions, 'storage_volume_profile_unsupported');
}

const overlaps = (a, b) => a === b || a.startsWith(b.replace(/\/$/u, '') + '/') || b.startsWith(a.replace(/\/$/u, '') + '/');
async function assertWriters(engine, mount, allowedId) {
  const items = await engine.request('GET', '/containers/json');
  requireThat(Array.isArray(items), 'storage_writer_check_failed');
  for (const c of items) {
    if (c.Id === allowedId) continue;
    const conflict = (c.Mounts || []).some(m => m.RW !== false && ((mount.name && m.Name === mount.name)
      || (typeof m.Source === 'string' && overlaps(m.Source, mount.source))));
    requireThat(!conflict, 'storage_other_writer');
  }
}

async function pollHelper(context, receipt) {
  const { engine, wait = pause, maxPolls = 60 } = context;
  let final;
  for (let i = 0; i < maxPolls; i++) {
    try {
      const c = await engine.inspect(receipt.id || receipt.name);
      requireThat(c.Name === '/' + receipt.name && c.Image === receipt.image
        && c.Config?.Labels?.['io.soty.storage.probe'] === context.transactionId, 'storage_helper_identity');
      if (!c.State.Running && c.State.Status === 'exited') { final = c; break; }
    } catch (e) { if (e.code === 'storage_helper_identity') throw e; }
    if (i + 1 < maxPolls) await wait(250);
  }
  requireThat(final, 'storage_helper_unresolved');
  const output = await engine.helperOutput(final.Id);
  if (final.State.ExitCode !== 0) {
    const code = ['storage_format_unknown', 'storage_format_unreadable', 'storage_directory_invalid'].includes(output?.code) ? output.code : 'storage_probe_failed';
    throw new SafeError(code);
  }
  return checkedStorageFormat(output);
}

export async function reconcileStorageProbe(context) {
  const receipt = context.getState()?.storageGuardHelper;
  if (!receipt) return;
  await pollHelper(context, receipt);
  // Retain the stopped read-only helper as exact-ID evidence; never restart it.
  await context.record({ storageGuardHelper: null });
}

async function productionProbe(context, mount) {
  const { engine, getState, record, transactionId, probeImage, wait = pause, maxPolls = 60 } = context;
  requireThat(/^[a-f0-9]{16,40}$/u.test(transactionId || ''), 'storage_transaction_invalid');
  requireThat(IMAGE.test(probeImage || ''), 'storage_probe_image_required');
  requireThat((await engine.image(probeImage)).Id === probeImage, 'storage_probe_image_identity');
  await reconcileStorageProbe(context);
  const sequence = (getState()?.storageGuardSequence || 0) + 1;
  const name = `soty-storage-probe-${transactionId}-${sequence}`;
  const script = await readFile(new URL('./storage-probe.mjs', import.meta.url), 'utf8');
  const data = { Type: 'volume', Source: mount.name, Target: '/data', ReadOnly: true, VolumeOptions: { NoCopy: true } };
  const body = { Image: probeImage, User: '0:0', WorkingDir: '/', Env: ['SOTY_STORAGE_PROBE=1'], Entrypoint: ['node'],
    Cmd: ['--input-type=module', '-e', script], Tty: false, Labels: { 'io.soty.storage.probe': transactionId },
    HostConfig: { Mounts: [data], NetworkMode: 'none', RestartPolicy: { Name: 'no' }, ReadonlyRootfs: true,
      Memory: 134217728, NanoCpus: 500000000, PidsLimit: 16, CapDrop: ['ALL'], CapAdd: ['DAC_READ_SEARCH'],
      SecurityOpt: ['no-new-privileges'], Tmpfs: { '/tmp': 'rw,noexec,nosuid,size=16777216' } }, NetworkingConfig: { EndpointsConfig: {} } };
  let receipt = { name, image: probeImage, state: 'creating' };
  await record({ storageGuardSequence: sequence, storageGuardHelper: receipt });
  try { await engine.create(name, body); } catch { /* Resolve exact identity, never repeat CREATE. */ }
  let created;
  for (let i = 0; i < maxPolls; i++) {
    try { created = await engine.inspect(name); break; } catch { /* Docker may still apply CREATE. */ }
    if (i + 1 < maxPolls) await wait(250);
  }
  requireThat(created && ID.test(created.Id) && created.Name === '/' + name && created.Image === probeImage
    && created.Config?.Labels?.['io.soty.storage.probe'] === transactionId, 'storage_helper_unresolved');
  requireThat(!created.State.Running && created.State.Status === 'created', 'storage_helper_unresolved');
  receipt = { ...receipt, id: created.Id, state: 'starting' };
  await record({ storageGuardHelper: receipt });
  try { await engine.start(created.Id); } catch { /* A dropped response is not a reason to re-run. */ }
  const result = await pollHelper(context, receipt);
  await record({ storageGuardHelper: null });
  return result;
}

export async function guardStorageStart(context, runtime, { running = false } = {}) {
  const { engine } = context;
  requireThat(ID.test(runtime?.Id || '') && IMAGE.test(runtime.Image || ''), 'storage_runtime_identity');
  const current = await engine.inspect(runtime.Id);
  requireThat(current.Id === runtime.Id && current.Image === runtime.Image && current.State.Running === running, 'storage_runtime_changed');
  const mount = dataMount(current), mountHash = hash(mount);
  const image = await engine.image(runtime.Image);
  requireThat(image.Id === runtime.Image, 'storage_image_identity_invalid');
  storageReaders(image); // A copied container label cannot attest an old image.
  await assertLocalVolume(engine, mount);
  await assertWriters(engine, mount, running ? current.Id : undefined);
  const observed = context.probe ? await context.probe(current) : await productionProbe(context, mount);
  const format = assertStorageCompatible(image, observed);
  const after = await engine.inspect(runtime.Id);
  requireThat(after.Image === runtime.Image && after.State.Running === running && hash(dataMount(after)) === mountHash, 'storage_runtime_changed');
  await assertLocalVolume(engine, mount);
  await assertWriters(engine, mount, running ? current.Id : undefined);
  return { schema: 'soty.storage-start.v2', containerId: current.Id, image: current.Image, mountSha256: mountHash,
    rooms: format.rooms, apps: format.apps };
}

export function requireStorageStartReceipt(value, id) {
  requireThat(keys(value, 'apps,containerId,image,mountSha256,rooms,schema') && value.schema === 'soty.storage-start.v2'
    && value.containerId === id && ID.test(value.containerId || '') && IMAGE.test(value.image || '')
    && ID.test(value.mountSha256 || '') && knownFormat('rooms', value.rooms) && knownFormat('apps', value.apps), 'storage_start_guard_missing');
}
