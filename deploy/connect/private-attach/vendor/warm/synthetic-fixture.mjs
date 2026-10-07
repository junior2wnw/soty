// In-memory filesystem and native channels. No Linux/physical/host proof is claimed.
import { readFile } from 'node:fs/promises';
import { posix } from 'node:path';
import { createHash } from 'node:crypto';
import { PassThrough } from 'node:stream';
import { SOURCE_FILES, prepareWarmForSyntheticFixture } from './linux-warm-preflight.mjs';
import { PROFILE, WireOwner, beginFrame, encodeFrame, WARM_LEASE_MS } from './wire-protocol.mjs';
import { createWarmReceiverForSyntheticFixture, createWarmControlClientForSyntheticFixture } from './warm-receiver.mjs';
export const sha = bytes => createHash('sha256').update(bytes).digest('hex');
export const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
export const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
export const PUBLIC_SOURCE_BYTES = Object.fromEntries(await Promise.all(Object.entries(SOURCE_FILES).map(async ([key, path]) =>
  [key, await readFile(new URL(path, import.meta.url))])));
export const SPEC = Object.freeze({ transaction: '1234567890abcdef1234567890abcdef', targetId: '1234567890abcdef1234567890abcdef',
  nonce: 'e'.repeat(32), image: 'sha256:' + 'b'.repeat(64), profile: PROFILE,
  sourcePins: Object.freeze(Object.fromEntries(Object.entries(PUBLIC_SOURCE_BYTES).map(([key, bytes]) => [key, sha(bytes)]))) });
export const RECEIPT = Object.freeze({ expectedSha256: '4'.repeat(64), expectedManifestSha256: '5'.repeat(64),
  sourceWitness: Object.freeze({ generationId: SPEC.transaction, checkpointSha256: '6'.repeat(64), inventorySha256: '7'.repeat(64) }) });
export const BODY = Buffer.alloc(1024, 83);
export const fence = receipt => Object.freeze({ check() {}, authenticated: receipt });
const information = (record, type) => ({ ...record,
  isDirectory: () => type === 'directory', isFile: () => type === 'file', isSymbolicLink: () => type === 'symlink' });

export function mockReadonlyFilesystem() {
  const opened = [], closed = [], fdRecords = new Map(), sourceRoot = '/operator/root/deploy/connect/private-attach/vendor/warm';
  let fdSequence = 40;
  const dirs = new Map();
  ['/owned', '/owned/target', '/owned/target/data', '/owned/target/config', sourceRoot].forEach((path, index) => dirs.set(path,
    { dev: path.includes('/data') ? 2n : 1n, ino: BigInt(index + 10), uid: 0n, gid: 0n, mode: 0o40700n,
      size: 0n, nlink: 1n, mtimeNs: 1n, ctimeNs: 1n }));
  const files = new Map(Object.entries(SOURCE_FILES).map(([key, relativePath], index) => [posix.resolve(sourceRoot, relativePath),
    { bytes: PUBLIC_SOURCE_BYTES[key], stat: { dev: 20n, ino: BigInt(index + 200), uid: 0n, gid: 0n,
      mode: 0o100444n, size: BigInt(PUBLIC_SOURCE_BYTES[key].length), nlink: 1n, mtimeNs: 1n, ctimeNs: 1n } }]));
  const state = { opened, closed, dirs, files, sourceRoot, hook: async () => {},
    namespace: { dev: 8n, ino: 100n },
    entries: new Map([['/owned', ['target']], ['/owned/target', ['data', 'config']], ['/owned/target/data', []], ['/owned/target/config', []]]),
    mounts: [
      { id: 1, path: '/', options: 'ro', type: 'overlay' },
      { id: 100, path: '/owned', options: 'rw,noexec,nosuid', type: 'tmpfs' },
      { id: 101, path: '/owned/target/data', options: 'rw', type: 'ext4' },
      { id: 102, path: '/owned/target/config', options: 'rw,noexec,nosuid', type: 'tmpfs' },
      { id: 500, path: '/operator', options: 'ro', type: 'ext4' },
    ],
  };
  const mountId = path => state.mounts.filter(row => path === row.path || path.startsWith(row.path === '/' ? '/' : row.path + '/'))
    .sort((a, b) => b.path.length - a.path.length)[0].id;
  const metadata = path => {
    if (dirs.has(path)) return information(dirs.get(path), 'directory');
    if (files.has(path)) return information(files.get(path).stat, 'file');
    throw Error('fixture_path_not_whitelisted');
  };
  const procBytes = path => {
    if (path === '/proc/self/mountinfo') return Buffer.from(state.mounts.map(row =>
      `${row.id} 1 0:1 / ${row.path} ${row.options} - ${row.type} fixture rw`).join('\n') + '\n');
    const match = /^\/proc\/self\/fdinfo\/(\d+)$/u.exec(path);
    if (match) return Buffer.from(`mnt_id:\t${fdRecords.get(Number(match[1])).mountId}\n`);
    return null;
  };
  const io = {
    async realpath(path) { await state.hook('realpath', path); return path; },
    async lstat(path) { await state.hook('lstat', path); return metadata(path); },
    async stat(path) { await state.hook('stat', path); if (path !== '/proc/self/ns/mnt') throw Error('fixture_path_not_whitelisted'); return { ...state.namespace }; },
    async open(path, flags) {
      await state.hook('open', path); opened.push({ path, flags });
      const fd = ++fdSequence, bytes = procBytes(path), file = files.get(path), directory = dirs.get(path);
      if (bytes === null && !file && !directory) throw Error('fixture_path_not_whitelisted');
      const record = { mountId: bytes ? 1 : mountId(path) }; fdRecords.set(fd, record);
      return {
        fd,
        async stat() { await state.hook('handle.stat', path); return directory ? information(directory, 'directory') : information(file.stat, 'file'); },
        async read(buffer, offset, length, position) {
          await state.hook('handle.read', path);
          const source = bytes || file.bytes, count = Math.min(length, Math.max(0, source.length - position));
          source.copy(buffer, offset, position, position + count); return { bytesRead: count };
        },
        async close() { await state.hook('handle.close', path); closed.push(fd); },
      };
    },
    async opendir(path) {
      await state.hook('opendir', path);
      const names = [...state.entries.get(path)], fd = ++fdSequence; let index = 0;
      opened.push({ path, directoryRead: true });
      return { async read() { await state.hook('directory.read', path); return index < names.length ? { name: names[index++] } : null; },
        async close() { await state.hook('directory.close', path); closed.push(fd); } };
    },
  };
  return { io, state, preflight: (spec, check) => prepareWarmForSyntheticFixture(spec, { io, check }) };
}

export async function consumeSyntheticBody(control, input) {
  const parts = [];
  for await (const part of input) parts.push(part);
  const body = Buffer.concat(parts);
  return { extracted: true, targetId: control.targetId, manifestSha256: control.expectedManifestSha256,
    plaintextSha256: sha(body), plaintextBytes: body.length, entries: 1, fileBytes: body.length, readbackVerified: true };
}
export async function manualBegin(env) {
  const wire = new WireOwner({ signal: env.abort.signal, limits: { wallMs: 1000, idleMs: 500 } });
  try {
    env.control.write(encodeFrame(beginFrame(SPEC)));
    const response = await wire.readFrame(env.announcements, { requireEOF: false, allowPreviouslyOwnedRead: true });
    if (response.schema !== 'soty.receiver-control-begin-ack.v1') throw Error('synthetic_begin_ack_invalid');
  } finally { wire.release(); }
}
export function environment({ preflight, receive, limits = { wallMs: 1000, idleMs: 500 }, leaseMs = WARM_LEASE_MS, clientLeaseMs = leaseMs, payloadInput, emit } = {}) {
  const phases = [], counts = { receiveCalls: 0, rechecks: 0, closed: 0 };
  const warm = preflight || (async (_spec, check) => { check(); return {
    async recheck() { check(); counts.rechecks++; }, async close() { counts.closed++; },
  }; });
  const receiver = receive || consumeSyntheticBody;
  const control = new PassThrough({ highWaterMark: 8192 }), announcements = new PassThrough({ highWaterMark: 8192 });
  const payload = payloadInput || new PassThrough({ highWaterMark: 65536 }), abort = new AbortController();
  const driver = createWarmReceiverForSyntheticFixture({ preflight: warm, limits, leaseMs,
    receive: (closedControl, input) => { counts.receiveCalls++; return receiver(closedControl, input); } });
  const client = createWarmControlClientForSyntheticFixture({ spec: SPEC, announcementInput: announcements, controlOutput: control, signal: abort.signal, limits, leaseMs: clientLeaseMs });
  const rawRun = driver({ spec: SPEC, controlInput: control, payloadInput: payload, announcementOutput: announcements, signal: abort.signal,
    emit(value) { phases.push(value); emit?.(value); } });
  rawRun.catch(() => {});
  const close = async () => { client.cancel(); abort.abort(); payload.destroy(); await rawRun.catch(() => {}); };
  return { control, announcements, payload, client, rawRun, counts, phases, abort, close };
}
