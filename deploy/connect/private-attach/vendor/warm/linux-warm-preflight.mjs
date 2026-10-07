// Read-only mechanical expansion of pinned strict-receiver topology/empty checks.
// No backup metadata, chmod, target creation, cleanup or data/config-file reads.
import { constants } from 'node:fs';
import { posix } from 'node:path';
import { open, lstat, stat, realpath, opendir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { fail, failure, failureCode, captureSpec, SOURCE_KEYS } from './wire-protocol.mjs';

export const SOURCE_FILES = Object.freeze({
  receiver: '../../strict-receiver.mjs',
  restore: '../../../restore-backup.mjs',
  format: '../../../backup-format.mjs',
  sink: '../../../restore-sink.mjs',
  inventory: '../../receiver-limits.mjs',
  protocol: 'wire-protocol.mjs', warmProbe: 'linux-warm-preflight.mjs', driver: 'warm-receiver.mjs', entry: 'linux-fd-entry.mjs',
});
const RETAINED = Object.freeze({
  receiver: '15ba92a26eb3f3a0457d8be89396a022fe60df72ab6b762ee2b5caba88377e1c',
  restore: 'aea0951ddf0b400734a85bdfc0be6f36de889679562b8c46cf000b55176f5eed',
  format: '083859bfb497031c7e70473f35ea208c2ff6ad707323f822184c68867f5d0b79',
  sink: '5bc3246b7ce7869085c604f6b6686668d20be69d46b90bb4ea24fb1ba1c2ddb3',
  inventory: '7c28281649fd848c33eb2df9c8480161cff13b4d190493a4ebb52838887c7a68',
});
const ROOTS = Object.freeze(['/owned', '/owned/target', '/owned/target/data', '/owned/target/config']);
const IDENTITY = ['dev', 'ino', 'uid', 'gid', 'mode'];
const FILE_IDENTITY = [...IDENTITY, 'size', 'nlink', 'mtimeNs', 'ctimeNs'];
const same = (a, b, keys = IDENTITY) => keys.every(key => a[key] === b[key]);
const decode = value => value.replace(/\\([0-7]{3})/gu, (_match, octal) => String.fromCharCode(parseInt(octal, 8)));
function mountRows(text) {
  return text.split('\n').filter(Boolean).map(line => {
    const values = line.split(' '), separator = values.indexOf('-');
    if (separator < 6 || !/^[1-9]\d*$/u.test(values[0]) || !values[4]?.startsWith('/')) fail('warm_topology_invalid');
    const id = Number(values[0]); if (!Number.isSafeInteger(id)) fail('warm_topology_invalid');
    return { id, path: decode(values[4]), options: values[5].split(','), type: values[separator + 1] };
  });
}
const effectiveMount = (rows, path) => rows.filter(row => path === row.path || path.startsWith(row.path === '/' ? '/' : row.path + '/'))
  .sort((a, b) => b.path.length - a.path.length)[0];

async function prepare(specValue, { io, uid, sourceRoot, check }) {
  const spec = captureSpec(specValue), handles = new Set(), dirs = [], files = [];
  if (Object.keys(RETAINED).some(key => spec.sourcePins[key] !== RETAINED[key])) fail('warm_source_pin_invalid');
  let closed = false;
  const step = async operation => { check(); const result = await operation(); check(); return result; };
  const acquire = async (path, flags) => {
    check(); const handle = await io.open(path, flags); handles.add(handle); check(); return handle;
  };
  const release = async handle => { await handle.close(); handles.delete(handle); };
  const textFile = async (path, bound) => {
    const handle = await acquire(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const buffer = Buffer.alloc(bound + 1); let length = 0;
    try {
      for (;;) {
        const result = await step(() => handle.read(buffer, length, buffer.length - length, length));
        if (result.bytesRead < 0 || result.bytesRead > buffer.length - length) fail('warm_source_invalid');
        length += result.bytesRead;
        if (length > bound) fail('warm_source_limit');
        if (!result.bytesRead) break;
      }
      return buffer.subarray(0, length).toString('utf8');
    } finally { buffer.fill(0); await release(handle); }
  };
  const mountId = async handle => {
    const text = await textFile(`/proc/self/fdinfo/${handle.fd}`, 4096);
    const matches = [...text.matchAll(/^mnt_id:\s*([1-9]\d*)$/gmu)];
    if (matches.length !== 1 || !Number.isSafeInteger(Number(matches[0][1]))) fail('warm_topology_invalid');
    return Number(matches[0][1]);
  };
  const sourcePath = key => { const path=posix.resolve(sourceRoot, SOURCE_FILES[key]); if (!path.startsWith('/operator/')) fail('warm_source_root_invalid'); return path; };
  const readTopology = async () => {
    const rows = mountRows(await textFile('/proc/self/mountinfo', 1024 * 1024));
    for (const [path, type] of [['/owned', 'tmpfs'], ['/owned/target/config', 'tmpfs'], ['/owned/target/data', null]]) {
      const matches = rows.filter(row => row.path === path);
      if (matches.length !== 1) fail('warm_topology_invalid');
      const row = matches[0];
      if (!row.options.includes('rw') || type && (row.type !== type || !row.options.includes('noexec') || !row.options.includes('nosuid'))
        || !type && row.type === 'tmpfs') fail('warm_topology_invalid');
    }
    for (const key of SOURCE_KEYS) {
      const mount = effectiveMount(rows, sourcePath(key));
      if (!mount?.options.includes('ro') || mount.options.includes('rw')) fail('warm_source_not_readonly');
    }
    return rows;
  };
  const directory = async path => {
    if (await step(() => io.realpath(path)) !== path) fail('warm_target_invalid');
    const before = await step(() => io.lstat(path, { bigint: true }));
    if (!before.isDirectory() || before.isSymbolicLink() || before.uid !== uid || (before.mode & 0o7777n) !== 0o700n)
      fail('warm_target_invalid');
    const handle = await acquire(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    const opened = await step(() => handle.stat({ bigint: true })), id = await mountId(handle);
    const after = await step(() => io.lstat(path, { bigint: true }));
    if (!opened.isDirectory() || !same(before, opened) || !same(opened, after)) fail('warm_target_drift');
    const record = { path, handle, observed: opened, mountId: id }; dirs.push(record); return record;
  };
  const names = async (path, max) => {
    check(); const handle = await io.opendir(path, { bufferSize: 4 }); handles.add(handle);
    try {
      check(); const output = [];
      while (output.length <= max) {
        const entry = await step(() => handle.read()); if (!entry) break; output.push(entry.name);
      }
      if (output.length > max) fail('warm_target_contaminated'); return output.sort();
    } finally { await release(handle); }
  };
  const empty = async () => {
    if ((await names('/owned', 1)).join(',') !== 'target' || (await names('/owned/target', 2)).join(',') !== 'config,data'
      || (await names('/owned/target/data', 0)).length || (await names('/owned/target/config', 0)).length)
      fail('warm_target_contaminated');
  };
  const hashSource = async record => {
    const { path, handle, observed } = record;
    if (await step(() => io.realpath(path)) !== path) fail('warm_source_drift');
    const current = await step(() => io.lstat(path, { bigint: true }));
    const opened = await step(() => handle.stat({ bigint: true }));
    if (!current.isFile() || current.isSymbolicLink() || current.nlink !== 1n || current.uid !== uid
      || !same(current, opened, FILE_IDENTITY) || observed && !same(observed, current, FILE_IDENTITY)) fail('warm_source_drift');
    const buffer = Buffer.alloc(65536), hash = createHash('sha256'); let total = 0;
    try {
      for (;;) {
        const { bytesRead } = await step(() => handle.read(buffer, 0, buffer.length, total));
        if (!Number.isSafeInteger(bytesRead) || bytesRead < 0 || bytesRead > buffer.length) fail('warm_source_invalid');
        total += bytesRead; if (total > Number(current.size) || total > 512 * 1024) fail('warm_source_limit');
        if (!bytesRead) break; hash.update(buffer.subarray(0, bytesRead));
      }
      if (total !== Number(current.size) || hash.digest('hex') !== spec.sourcePins[record.key]
        || !same(current, await step(() => handle.stat({ bigint: true })), FILE_IDENTITY)
        || !same(current, await step(() => io.lstat(path, { bigint: true })), FILE_IDENTITY)) fail('warm_source_drift');
    } finally { buffer.fill(0); }
    record.observed = current;
  };
  const close = async () => {
    closed = true; let failed = false;
    for (const handle of [...handles]) { try { await release(handle); } catch { failed = true; } }
    if (failed) throw failure('warm_cleanup_pending');
  };
  try {
    if (!sourceRoot.startsWith('/operator/') || await step(() => io.realpath(sourceRoot)) !== sourceRoot) fail('warm_source_root_invalid');
    const sourceInfo = await step(() => io.lstat(sourceRoot, { bigint: true }));
    if (!sourceInfo.isDirectory() || sourceInfo.isSymbolicLink() || sourceInfo.uid !== uid) fail('warm_source_root_invalid');
    const namespace = await step(() => io.stat('/proc/self/ns/mnt', { bigint: true }));
    const initialTopology = await readTopology();
    for (const path of ROOTS) await directory(path);
    const data = dirs.find(record => record.path === '/owned/target/data'), config = dirs.find(record => record.path === '/owned/target/config');
    const base = dirs[0];
    if (new Set([base.mountId, data.mountId, config.mountId]).size !== 3
      || [base, data, config].some(record => effectiveMount(initialTopology, record.path)?.id !== record.mountId)) fail('warm_topology_invalid');
    await empty();
    for (const key of SOURCE_KEYS) {
      const path = sourcePath(key), before = await step(() => io.lstat(path, { bigint: true }));
      if (!before.isFile() || before.isSymbolicLink() || before.nlink !== 1n || before.uid !== uid || before.size > 512n * 1024n)
        fail('warm_source_invalid');
      const handle = await acquire(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
      const record = { path, handle, key, observed: before, mountId: await mountId(handle) };
      if (effectiveMount(initialTopology, path)?.id !== record.mountId) fail('warm_source_drift');
      files.push(record); await hashSource(record);
    }
    const recheck = async () => {
      if (closed) fail('warm_lease_closed');
      const currentNamespace = await step(() => io.stat('/proc/self/ns/mnt', { bigint: true }));
      if (!same(namespace, currentNamespace, ['dev', 'ino'])) fail('warm_target_drift');
      const rows = await readTopology();
      if (!same(sourceInfo, await step(() => io.lstat(sourceRoot, { bigint: true })))) fail('warm_source_drift');
      for (const record of dirs) {
        if (await step(() => io.realpath(record.path)) !== record.path) fail('warm_target_drift');
        const observed = await step(() => record.handle.stat({ bigint: true })), pathValue = await step(() => io.lstat(record.path, { bigint: true }));
        if (!observed.isDirectory() || !pathValue.isDirectory() || !same(record.observed, observed) || !same(record.observed, pathValue)
          || await mountId(record.handle) !== record.mountId || effectiveMount(rows, record.path)?.id !== record.mountId) fail('warm_target_drift');
      }
      await empty();
      for (const record of files) {
        if (await mountId(record.handle) !== record.mountId || effectiveMount(rows, record.path)?.id !== record.mountId) fail('warm_source_drift');
        await hashSource(record);
      }
      const afterRows = await readTopology();
      if (dirs.some(record => effectiveMount(afterRows, record.path)?.id !== record.mountId)
        || files.some(record => effectiveMount(afterRows, record.path)?.id !== record.mountId)) fail('warm_target_drift');
      check();
    };
    await recheck();
    return Object.freeze({ recheck, close });
  } catch (error) {
    let code = failureCode(error); if (code === 'warm_receiver_failed') code = 'warm_preflight_failed';
    try { await close(); } catch { code = 'warm_cleanup_pending'; }
    throw failure(code);
  }
}

export async function prepareLinuxWarm(spec, check) {
  if (process.platform !== 'linux' || typeof process.geteuid !== 'function') fail('warm_linux_required');
  return prepare(spec, { io: { open, lstat, stat, realpath, opendir }, uid: BigInt(process.geteuid()),
    sourceRoot: fileURLToPath(new URL('./', import.meta.url)).replace(/\/$/u, ''), check });
}

// Explicit fixture seam: not host verification, mount proof or an admission authority.
export async function prepareWarmForSyntheticFixture(spec, { io, uid = 0n, sourceRoot = '/operator/root/deploy/connect/private-attach/vendor/warm', check = () => {} }) {
  return prepare(spec, { io, uid, sourceRoot, check });
}
