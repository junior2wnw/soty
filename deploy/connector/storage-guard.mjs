import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { SafeError } from './docker-api.mjs';

export const storageReaderLabel = 'io.soty.storage.readers';
export const currentStorageReaders = '{"version":5,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1],"humanIdentity":[1]}}';
const ID = /^[a-f0-9]{64}$/u, IMAGE = /^sha256:[a-f0-9]{64}$/u;
const requireThat = (ok, code) => { if (!ok) throw new SafeError(code); };
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const keys = (value, expected) => value !== null && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === expected.split(',').length && Object.keys(value).sort().join(',') === expected;
const matches = (pattern, value) => typeof value === 'string' && pattern.test(value);
// Parser knowledge is separate from the current image declaration above. Only
// an actual image's explicit reader declaration may admit its formats before START.
const supported = { rooms: [1, 2], apps: [1, 2, 3, 4, 5, 6], notes: [1, 2], capabilities: [1, 2, 3], appRegistration: [1], feedback: [1], humanIdentity: [1] };
const legacyStores = ['rooms', 'apps', 'notes', 'capabilities'], universalStores = [...legacyStores, 'appRegistration', 'feedback'], stores = Object.keys(supported);
const storesFor = version => version === 3 ? legacyStores : version === 4 ? universalStores : stores;
const formatVersion = schema => schema === 'soty.storage-format.v3' ? 3 : schema === 'soty.storage-format.v4' ? 4 : schema === 'soty.storage-format.v5' ? 5 : null;
const knownFormat = (store, value) => value === 'empty' || supported[store].includes(value);
const knownReaders = (store, value) => Array.isArray(value) && value.length > 0 && value.length <= supported[store].length
  && value.every(version => supported[store].includes(version)) && new Set(value).size === value.length;

export function storageReaders(image) {
  requireThat(matches(IMAGE, image?.Id), 'storage_image_identity_invalid');
  let value;
  const label = image.Config?.Labels?.[storageReaderLabel];
  requireThat(typeof label === 'string', 'storage_reader_unknown');
  try { value = JSON.parse(label); } catch { throw new SafeError('storage_reader_unknown'); }
  requireThat(keys(value, 'readers,version') && [3, 4, 5].includes(value.version)
    && keys(value.readers, storesFor(value.version).slice().sort().join(','))
    && storesFor(value.version).every(store => knownReaders(store, value.readers[store])), 'storage_reader_unknown');
  return value.readers;
}

export function checkedStorageFormat(value) {
  const version = formatVersion(value?.schema);
  requireThat(version && keys(value, [...storesFor(version), 'ok', 'schema'].sort().join(',')) && value.ok === true
    && storesFor(version).every(store => knownFormat(store, value[store])), 'storage_probe_invalid');
  return { ok: true, schema: value.schema, ...Object.fromEntries(storesFor(version).map(store => [store, value[store]])) };
}

export function assertStorageCompatible(image, value) {
  const readers = storageReaders(image), format = checkedStorageFormat(value);
  const version = formatVersion(format.schema);
  requireThat(storesFor(version).every(store => Array.isArray(readers[store])
    && (format[store] === 'empty' || readers[store].includes(format[store]))), 'storage_reader_incompatible');
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
    let mounts = c.Mounts || [];
    // Docker's list projection can omit an anonymous volume's Source. An empty
    // string is not a filesystem root; inspect its exact container before
    // deciding whether the runtime overlaps this managed data volume.
    if (mounts.some(m => m.Type !== 'tmpfs' && (typeof m.Source !== 'string' || m.Source === ''))) {
      requireThat(matches(ID, c.Id), 'storage_writer_check_failed');
      const full = await engine.inspect(c.Id);
      requireThat(full.Id === c.Id && Array.isArray(full.Mounts) && typeof full.State?.Running === 'boolean', 'storage_writer_check_failed');
      if (!full.State.Running) continue;
      mounts = full.Mounts;
      requireThat(mounts.every(m => m.Type === 'tmpfs' || (typeof m.Source === 'string' && path.posix.isAbsolute(m.Source))), 'storage_writer_check_failed');
    }
    const conflict = mounts.some(m => m.Type !== 'tmpfs' && m.RW !== false && ((mount.name && m.Name === mount.name)
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

async function productionProbe(context, mount, { running = false } = {}) {
  const { engine, getState, record, transactionId, probeImage, wait = pause, maxPolls = 60 } = context;
  requireThat(/^[a-f0-9]{16,40}$/u.test(transactionId || ''), 'storage_transaction_invalid');
  requireThat(IMAGE.test(probeImage || ''), 'storage_probe_image_required');
  requireThat((await engine.image(probeImage)).Id === probeImage, 'storage_probe_image_identity');
  await reconcileStorageProbe(context);
  const sequence = (getState()?.storageGuardSequence || 0) + 1;
  const name = `soty-storage-probe-${transactionId}-${sequence}`;
  let script = await readFile(new URL('./storage-probe.mjs', import.meta.url), 'utf8');
  if (!running) {
    const snapshot = await readFile(new URL('./storage-snapshot.mjs', import.meta.url), 'utf8');
    script = snapshot + '\n' + script.replace("await readStorageFormat('/data')", "await readStorageFormat(await snapshotStorage('/data', '/tmp/soty-storage-snapshot'))");
  }
  const data = { Type: 'volume', Source: mount.name, Target: '/data', ReadOnly: true, VolumeOptions: { NoCopy: true } };
  const body = { Image: probeImage, User: '0:0', WorkingDir: '/', Env: ['SOTY_STORAGE_PROBE=1'], Entrypoint: ['node'],
    Cmd: ['--input-type=module', '-e', script], Tty: false, Labels: { 'io.soty.storage.probe': transactionId },
    HostConfig: { Mounts: [data], NetworkMode: 'none', RestartPolicy: { Name: 'no' }, ReadonlyRootfs: true,
      Memory: 536870912, NanoCpus: 500000000, PidsLimit: 16, CapDrop: ['ALL'], CapAdd: ['DAC_READ_SEARCH'],
      SecurityOpt: ['no-new-privileges'], Tmpfs: { '/tmp': 'rw,noexec,nosuid,size=268435456' } }, NetworkingConfig: { EndpointsConfig: {} } };
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
  requireThat(matches(ID, runtime?.Id) && matches(IMAGE, runtime.Image), 'storage_runtime_identity');
  const current = await engine.inspect(runtime.Id);
  requireThat(current.Id === runtime.Id && current.Image === runtime.Image && current.State.Running === running, 'storage_runtime_changed');
  const mount = dataMount(current), mountHash = hash(mount);
  const image = await engine.image(runtime.Image);
  requireThat(image.Id === runtime.Image, 'storage_image_identity_invalid');
  storageReaders(image); // A copied container label cannot attest an old image.
  await assertLocalVolume(engine, mount);
  await assertWriters(engine, mount, running ? current.Id : undefined);
  const observed = context.probe ? await context.probe(current) : await productionProbe(context, mount, { running });
  const format = assertStorageCompatible(image, observed);
  const after = await engine.inspect(runtime.Id);
  requireThat(after.Image === runtime.Image && after.State.Running === running && hash(dataMount(after)) === mountHash, 'storage_runtime_changed');
  await assertLocalVolume(engine, mount);
  await assertWriters(engine, mount, running ? current.Id : undefined);
  const version = formatVersion(format.schema);
  return { schema: `soty.storage-start.v${version}`, containerId: current.Id, image: current.Image, mountSha256: mountHash,
    ...Object.fromEntries(storesFor(version).map(store => [store, format[store]])) };
}

export function requireStorageStartReceipt(value, id) {
  const version = value?.schema === 'soty.storage-start.v3' ? 3 : value?.schema === 'soty.storage-start.v4' ? 4 : value?.schema === 'soty.storage-start.v5' ? 5 : null;
  requireThat(version && keys(value, [...storesFor(version), 'containerId', 'image', 'mountSha256', 'schema'].sort().join(','))
    && value.containerId === id && matches(ID, value.containerId) && matches(IMAGE, value.image)
    && matches(ID, value.mountSha256) && storesFor(version).every(store => knownFormat(store, value[store])), 'storage_start_guard_missing');
}
