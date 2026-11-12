import { readFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { SafeError } from './docker-api.mjs';
import { WORLD_FIELD_FIXTURE_SHA256, WORLD_FIELD_LAYOUT_SHA256, worldFieldReaderResult } from './world-field-reader-probe.mjs';

export const storageReaderLabel = 'io.soty.storage.readers';
export const currentStorageReaders = '{"version":10,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6,7,8,9,10,11,12,13,14],"notes":[1,2,3],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1,2],"humanIdentity":[1,2,3],"connect":[3,4,5],"world":[6,7,8,9],"canvasObjects":[1,2],"canvasWidgets":[1],"canvasVault":[1,2],"canvasPages":[1,2],"materials":[1,2],"invitations":[1,2],"referrals":[1],"appAgentDirectory":[1],"universalPublication":[1],"appsEndpoint":[1],"appsFrameContinuity":[1,2],"monitoring":[1]}}';
const ID = /^[a-f0-9]{64}$/u, IMAGE = /^sha256:[a-f0-9]{64}$/u;
const requireThat = (ok, code) => { if (!ok) throw new SafeError(code); };
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const fieldProbe = readFileSync(new URL('./world-field-reader-probe.mjs', import.meta.url), 'utf8');
const fieldProbeSha256 = createHash('sha256').update(fieldProbe).digest('hex');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const keys = (value, expected) => value !== null && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === expected.split(',').length && Object.keys(value).sort().join(',') === expected;
const matches = (pattern, value) => typeof value === 'string' && pattern.test(value);
// Parser knowledge is separate from an image's declaration. Only the START
// guard may refine a legacy World claim after actual-image field conformance.
const supported = { rooms: [1, 2], apps: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14], notes: [1, 2, 3], capabilities: [1, 2, 3], appRegistration: [1], feedback: [1, 2], humanIdentity: [1, 2, 3], connect: [3, 4, 5], world: [3, 5, 6, 7, 8, 9],
  canvasObjects:[1,2],canvasWidgets:[1],canvasVault:[1,2],canvasPages:[1,2],materials:[1,2],invitations:[1,2],referrals:[1],appAgentDirectory:[1],universalPublication:[1],appsEndpoint:[1],appsFrameContinuity:[1,2],monitoring:[1] };
const legacyStores = ['rooms', 'apps', 'notes', 'capabilities'], universalStores = [...legacyStores, 'appRegistration', 'feedback'];
const identityStores = [...universalStores, 'humanIdentity'], connectedStores = [...identityStores, 'connect'], stores = [...connectedStores, 'world'];
const liveStores=[...stores,'canvasObjects','canvasWidgets','canvasVault','canvasPages','materials','invitations','referrals','appAgentDirectory','universalPublication','appsEndpoint'];
const continuityStores=[...liveStores,'appsFrameContinuity'];
const monitoringStores=[...continuityStores,'monitoring'];
const storesFor = version => version === 3 ? legacyStores : version === 4 ? universalStores : version === 5 ? identityStores : version === 6 ? connectedStores : version===7?stores:version===8?liveStores:version===9?continuityStores:monitoringStores;
const formatVersion = schema => schema === 'soty.storage-format.v3' ? 3 : schema === 'soty.storage-format.v4' ? 4 : schema === 'soty.storage-format.v5' ? 5 : schema === 'soty.storage-format.v6' ? 6 : schema === 'soty.storage-format.v7' ? 7 : schema==='soty.storage-format.v8'?8:schema==='soty.storage-format.v9'?9:schema==='soty.storage-format.v10'?10:null;
const versionsFor = (store, version) => version < 7 && store === 'apps' ? supported.apps.filter(value => value <= 11)
  : version < 8 && store === 'notes' ? supported.notes.filter(value => value <= 2)
  : version < 10 && store === 'world' ? supported.world.filter(value => value <= 7) : supported[store];
const knownFormat = (store, value, version) => value === 'empty' || versionsFor(store, version).includes(value);
const knownReaders = (store, value, version) => Array.isArray(value) && (value.length > 0 || version === 11) && value.length <= versionsFor(store, version).length
  && value.every(item => versionsFor(store, version).includes(item)) && new Set(value).size === value.length;

function declaredStorageReaders(label) {
  let value;
  requireThat(typeof label === 'string', 'storage_reader_unknown');
  try { value = JSON.parse(label); } catch { throw new SafeError('storage_reader_unknown'); }
  // Reader manifest11 adds explicit unsupported([]) capabilities only. It does
  // not introduce storage-format11 or another stored format. START receipt11
  // independently carries a legacy image's field-reader qualification proof.
  requireThat(keys(value, 'readers,version') && [3, 4, 5, 6, 7,8,9,10,11].includes(value.version)
    && keys(value.readers, storesFor(value.version).slice().sort().join(','))
    && storesFor(value.version).every(store => knownReaders(store, value.readers[store], value.version)), 'storage_reader_unknown');
  return value.readers;
}

export function storageReaders(image) {
  requireThat(matches(IMAGE, image?.Id), 'storage_image_identity_invalid');
  return declaredStorageReaders(image.Config?.Labels?.[storageReaderLabel]);
}

export function checkedStorageFormat(value) {
  const version = formatVersion(value?.schema);
  requireThat(version && keys(value, [...storesFor(version), 'ok', 'schema'].sort().join(',')) && value.ok === true
    && storesFor(version).every(store => knownFormat(store, value[store], version)), 'storage_probe_invalid');
  return { ok: true, schema: value.schema, ...Object.fromEntries(storesFor(version).map(store => [store, value[store]])) };
}

function compatibleStorageReaders(readers, value) {
  const format = checkedStorageFormat(value), version = formatVersion(format.schema);
  requireThat(storesFor(version).every(store => version >= 7 && format[store] === 'empty'
    || Array.isArray(readers[store]) && (format[store] === 'empty' || readers[store].includes(format[store]))), 'storage_reader_incompatible');
  return format;
}

// Pure declaration validation for a pinned prestart reader. The external START
// guard still obtains the actual image and validates its own reader label.
export function assertStorageReaderManifestCompatible(label, value) {
  return compatibleStorageReaders(declaredStorageReaders(label), value);
}

export function assertStorageCompatible(image, value) {
  return compatibleStorageReaders(storageReaders(image), value);
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

const fieldEnvironment = base => ['PATH=/usr/local/bin:/usr/bin:/bin', 'LANG=C.UTF-8', 'NODE_OPTIONS=', 'NODE_PATH=',
  'DATA_DIR=/tmp/soty-field-probe', 'SOTY_WORLD_FIELD_PROBE=1', 'SOTY_WORLD_FIELD_BASE=' + base];
const fieldTmpfs = { '/tmp': 'rw,noexec,nosuid,size=268435456', '/data': 'ro,noexec,nosuid,nodev,size=4096,mode=0500' };
function fieldHelperProfile(value, receipt, image) {
  const c = value.Config, h = value.HostConfig, caps = values => (values || []).map(x => x.replace(/^CAP_/u, '').toUpperCase()).sort();
  const expected = new Map((image.Config?.Env || []).map(value => [value.slice(0, value.indexOf('=')), value]));
  for (const value of fieldEnvironment(receipt.worldFormat - 2)) expected.set(value.slice(0, value.indexOf('=')), value);
  const actual = new Map((c?.Env || []).map(value => [value.slice(0, value.indexOf('=')), value]));
  requireThat(image.Id === receipt.image && c?.User === '0:0' && c.WorkingDir === '/' && c.Tty === false
    && c.OpenStdin === false && Array.isArray(c.Entrypoint) && hash(c.Entrypoint) === hash(['/usr/local/bin/node']) && c.Cmd?.length === 3 && c.Cmd[0] === '--input-type=module'
    && c.Cmd[1] === '-e' && typeof c.Cmd[2] === 'string' && createHash('sha256').update(c.Cmd[2]).digest('hex') === fieldProbeSha256
    && c.Healthcheck?.Test?.length === 1 && c.Healthcheck.Test[0] === 'NONE'
    && Array.isArray(c.Env) && actual.size === c.Env.length && hash([...actual].sort()) === hash([...expected].sort())
    && h?.NetworkMode === 'none' && h.ReadonlyRootfs === true && h.Privileged === false
    && h.Memory === 536870912 && h.NanoCpus === 500000000 && h.PidsLimit === 16
    && h.RestartPolicy?.Name === 'no' && !h.PublishAllPorts && Object.keys(h.PortBindings || {}).length === 0
    && (h.Mounts || []).length === 0 && (h.Binds || []).length === 0 && (h.VolumesFrom || []).length === 0
    && h.PidMode === '' && h.IpcMode === 'private' && h.UTSMode === '' && h.UsernsMode === '' && h.CgroupnsMode === 'private'
    && (h.Devices || []).length === 0 && (h.DeviceRequests || []).length === 0 && (h.DeviceCgroupRules || []).length === 0
    && (h.GroupAdd || []).length === 0 && h.ContainerIDFile === ''
    && h.Tmpfs && hash(Object.entries(h.Tmpfs).sort()) === hash(Object.entries(fieldTmpfs).sort())
    && hash(caps(h.CapDrop)) === hash(['ALL']) && caps(h.CapAdd).length === 0
    && (h.SecurityOpt || []).some(value => value === 'no-new-privileges' || value === 'no-new-privileges:true')
    && Array.isArray(value.Mounts) && value.Mounts.every(mount => mount.Type === 'tmpfs' && ['/tmp', '/data'].includes(mount.Destination)), 'storage_helper_profile');
}

function checkedFieldProof(value, image, world) {
  requireThat(keys(value, 'baseWorldFormat,fixtureSha256,helperId,image,layoutSha256,resultSha256,schema,scriptSha256,transactionId,worldFormat')
    && value.schema === 'soty.world-field-reader-proof.v1' && value.image === image && matches(IMAGE, value.image)
    && value.worldFormat === world && [8, 9].includes(world) && value.baseWorldFormat === world - 2
    && matches(ID, value.helperId) && /^[a-f0-9]{16,40}$/u.test(value.transactionId || '')
    && value.fixtureSha256 === WORLD_FIELD_FIXTURE_SHA256 && value.layoutSha256 === WORLD_FIELD_LAYOUT_SHA256
    && value.scriptSha256 === fieldProbeSha256 && value.resultSha256 === hash(worldFieldReaderResult(world - 2)), 'storage_start_guard_missing');
  return value;
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
  requireThat(receipt.kind === undefined || receipt.kind === 'world-field-reader', 'storage_helper_identity');
  if (receipt.kind === 'world-field-reader') await fieldHelperProfile(final, receipt, await engine.image(receipt.image));
  const output = await engine.helperOutput(final.Id);
  if (receipt.kind === 'world-field-reader') {
    requireThat(final.State.ExitCode === 0 && !final.State.OOMKilled && hash(output) === hash(worldFieldReaderResult(receipt.worldFormat - 2)), 'storage_reader_incompatible');
    return checkedFieldProof({ schema: 'soty.world-field-reader-proof.v1', image: receipt.image, helperId: final.Id,
      transactionId: context.transactionId, worldFormat: receipt.worldFormat, baseWorldFormat: receipt.worldFormat - 2,
      scriptSha256: fieldProbeSha256, fixtureSha256: WORLD_FIELD_FIXTURE_SHA256,
      layoutSha256: WORLD_FIELD_LAYOUT_SHA256, resultSha256: hash(output) }, receipt.image, receipt.worldFormat);
  }
  if (final.State.ExitCode !== 0) {
    const code = ['storage_format_unknown', 'storage_format_unreadable', 'storage_directory_invalid'].includes(output?.code) ? output.code : 'storage_probe_failed';
    throw new SafeError(code);
  }
  return checkedStorageFormat(output);
}

export async function reconcileStorageProbe(context) {
  const receipt = context.getState()?.storageGuardHelper;
  if (!receipt) return;
  const result = await pollHelper(context, receipt);
  // Retain the stopped read-only helper as exact-ID evidence; never restart it.
  await context.record({ storageGuardHelper: null, ...(receipt.kind === 'world-field-reader' ? { storageWorldFieldProof: result } : {}) });
}

async function runStorageHelper(context, image, body, fields = {}) {
  const { engine, getState, record, transactionId, wait = pause, maxPolls = 60 } = context;
  requireThat(/^[a-f0-9]{16,40}$/u.test(transactionId || '') && typeof getState === 'function' && typeof record === 'function', 'storage_transaction_invalid');
  await reconcileStorageProbe(context);
  const sequence = (getState()?.storageGuardSequence || 0) + 1, name = `soty-storage-probe-${transactionId}-${sequence}`;
  let receipt = { name, image, state: 'creating', ...fields };
  await record({ storageGuardSequence: sequence, storageGuardHelper: receipt });
  try { await engine.create(name, body); } catch { /* Resolve exact identity, never repeat CREATE. */ }
  let created;
  for (let i = 0; i < maxPolls; i++) {
    try { created = await engine.inspect(name); break; } catch { /* Docker may still apply CREATE. */ }
    if (i + 1 < maxPolls) await wait(250);
  }
  requireThat(created && ID.test(created.Id) && created.Name === '/' + name && created.Image === image
    && created.Config?.Labels?.['io.soty.storage.probe'] === transactionId, 'storage_helper_unresolved');
  requireThat(!created.State.Running && created.State.Status === 'created', 'storage_helper_unresolved');
  if (fields.kind === 'world-field-reader') fieldHelperProfile(created, receipt, await engine.image(image));
  receipt = { ...receipt, id: created.Id, state: 'starting' };
  await record({ storageGuardHelper: receipt });
  try { await engine.start(created.Id); } catch { /* A dropped response is not a reason to re-run. */ }
  const result = await pollHelper(context, receipt);
  await record({ storageGuardHelper: null, ...(fields.kind === 'world-field-reader' ? { storageWorldFieldProof: result } : {}) });
  return result;
}

async function productionProbe(context, mount, running) {
  const { engine, transactionId, probeImage } = context;
  requireThat(/^[a-f0-9]{16,40}$/u.test(transactionId || ''), 'storage_transaction_invalid');
  requireThat(IMAGE.test(probeImage || ''), 'storage_probe_image_required');
  requireThat((await engine.image(probeImage)).Id === probeImage, 'storage_probe_image_identity');
  let script = await readFile(new URL('./storage-probe.mjs', import.meta.url), 'utf8');
  // Online SQLite is captured transactionally. Closed WAL stores still use the
  // bounded private copy and original content fences before frozen parsing.
  const snapshot = await readFile(new URL('./storage-snapshot.mjs', import.meta.url), 'utf8');
  script = snapshot + '\n' + script.replace("await readStorageFormat('/data')", `await readStorageFormat(await snapshotStorage('/data', '/tmp/soty-storage-snapshot', undefined, { online: ${running} }))`);
  const data = { Type: 'volume', Source: mount.name, Target: '/data', ReadOnly: true, VolumeOptions: { NoCopy: true } };
  const body = { Image: probeImage, User: '0:0', WorkingDir: '/', Env: ['SOTY_STORAGE_PROBE=1'], Entrypoint: ['node'],
    Cmd: ['--input-type=module', '-e', script], Tty: false, Labels: { 'io.soty.storage.probe': transactionId },
    HostConfig: { Mounts: [data], NetworkMode: 'none', RestartPolicy: { Name: 'no' }, ReadonlyRootfs: true,
      Memory: 536870912, NanoCpus: 500000000, PidsLimit: 16, CapDrop: ['ALL'], CapAdd: ['DAC_READ_SEARCH'],
      SecurityOpt: ['no-new-privileges'], Tmpfs: { '/tmp': 'rw,noexec,nosuid,size=268435456' } }, NetworkingConfig: { EndpointsConfig: {} } };
  return runStorageHelper(context, probeImage, body);
}

async function qualifyWorldFieldReader(context, image, worldFormat) {
  const body = { Image: image.Id, User: '0:0', WorkingDir: '/', Env: fieldEnvironment(worldFormat - 2), Entrypoint: ['/usr/local/bin/node'],
    Cmd: ['--input-type=module', '-e', fieldProbe], Tty: false, OpenStdin: false, Healthcheck: { Test: ['NONE'] },
    Labels: { 'io.soty.storage.probe': context.transactionId },
    HostConfig: { Mounts: [], Binds: [], VolumesFrom: [], NetworkMode: 'none', RestartPolicy: { Name: 'no' },
      PidMode: '', IpcMode: 'private', UTSMode: '', UsernsMode: '', CgroupnsMode: 'private', ContainerIDFile: '',
      Devices: [], DeviceRequests: [], DeviceCgroupRules: [], GroupAdd: [],
      ReadonlyRootfs: true, Privileged: false, PublishAllPorts: false, PortBindings: {}, Memory: 536870912,
      NanoCpus: 500000000, PidsLimit: 16, CapDrop: ['ALL'], CapAdd: [], SecurityOpt: ['no-new-privileges'], Tmpfs: fieldTmpfs },
    NetworkingConfig: { EndpointsConfig: {} } };
  return runStorageHelper(context, image.Id, body, { kind: 'world-field-reader', worldFormat });
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
  const observed = context.probe ? await context.probe(current) : await productionProbe(context, mount, running);
  // Historical observations cannot attest absence of the new independent
  // stores or endpoint/journal overlays. START requires the full v10 probe.
  requireThat(formatVersion(observed?.schema) === 10, 'storage_probe_invalid');
  const format = checkedStorageFormat(observed);
  let worldFieldReader;
  if ([8, 9].includes(format.world) && !storageReaders(image).world?.includes(format.world)) {
    // This checks every other store and the declared physical-world reader
    // before qualifying only the missing epoch2 capability in the real image.
    assertStorageCompatible(image, { ...format, world: format.world - 2 });
    worldFieldReader = await qualifyWorldFieldReader(context, image, format.world);
  } else assertStorageCompatible(image, format);
  const after = await engine.inspect(runtime.Id);
  requireThat(after.Image === runtime.Image && after.State.Running === running && hash(dataMount(after)) === mountHash, 'storage_runtime_changed');
  await assertLocalVolume(engine, mount);
  await assertWriters(engine, mount, running ? current.Id : undefined);
  const version = formatVersion(format.schema);
  return { schema: worldFieldReader ? 'soty.storage-start.v11' : `soty.storage-start.v${version}`,
    containerId: current.Id, image: current.Image, mountSha256: mountHash,
    ...Object.fromEntries(storesFor(version).map(store => [store, format[store]])), ...(worldFieldReader ? { worldFieldReader } : {}) };
}

export function requireStorageStartReceipt(value, id) {
  const qualified = value?.schema === 'soty.storage-start.v11';
  const version = value?.schema === 'soty.storage-start.v3' ? 3 : value?.schema === 'soty.storage-start.v4' ? 4 : value?.schema === 'soty.storage-start.v5' ? 5 : value?.schema === 'soty.storage-start.v6' ? 6 : value?.schema === 'soty.storage-start.v7' ? 7 : value?.schema==='soty.storage-start.v8'?8:value?.schema==='soty.storage-start.v9'?9:value?.schema==='soty.storage-start.v10'||qualified?10:null;
  requireThat(version && keys(value, [...storesFor(version), 'containerId', 'image', 'mountSha256', 'schema', ...(qualified ? ['worldFieldReader'] : [])].sort().join(','))
    && value.containerId === id && matches(ID, value.containerId) && matches(IMAGE, value.image)
    && matches(ID, value.mountSha256) && storesFor(version).every(store => knownFormat(store, value[store], version)), 'storage_start_guard_missing');
  if (qualified) checkedFieldProof(value.worldFieldReader, value.image, value.world);
}
