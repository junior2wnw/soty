// Private Linux sink for the one shared restore parser. No decrypt, tar codec,
// recursive cleanup, destination mapping callback or public stream API.
import { open, opendir, lstat, stat, mkdir, statfs } from 'node:fs/promises';
import { constants } from 'node:fs';
import { createHash } from 'node:crypto';
import { formatFailure } from './backup-format.mjs';

const CHUNK = 64 * 1024;
const DIR = constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW;
const FILE = constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW | constants.O_RDWR;
const targetError = () => { throw formatFailure('restore_target_invalid'); };
const ioError = () => { throw formatFailure('restore_io_failed'); };
const same = (a, b) => a.dev === b.dev && a.ino === b.ino;
const identity = value => ({ dev: value.dev, ino: value.ino });
function exact(value, keys) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || Reflect.ownKeys(value).length !== keys.length || keys.some(key => !Object.hasOwn(value, key))) targetError();
}
function absolute(value) {
  if (typeof value !== 'string' || value.length > 4096 || !value.isWellFormed() || Buffer.byteLength(value) > 4096
      || !value.startsWith('/') || value === '/' || /[\u0000-\u001f\u007f-\u009f\\]/u.test(value)) targetError();
  const parts = value.slice(1).split('/');
  if (parts.some(part => !part || part === '.' || part === '..' || Buffer.byteLength(part) > 255)) targetError();
  return value;
}
function pin(value) {
  exact(value, ['path', 'dev', 'ino', 'mountId']);
  const captured = { path: absolute(value.path), dev: value.dev, ino: value.ino, mountId: value.mountId };
  if (typeof captured.dev !== 'bigint' || captured.dev < 0n || typeof captured.ino !== 'bigint' || captured.ino <= 0n
      || !Number.isSafeInteger(captured.mountId) || captured.mountId <= 0) targetError();
  return Object.freeze(captured);
}
export function captureRestoreTarget(value) {
  exact(value, ['targetId', 'mountNamespace', 'namespace', 'dataRoot', 'configRoot']);
  const targetId = value.targetId;
  const namespacePin = value.mountNamespace;
  exact(namespacePin, ['dev', 'ino']);
  const mountNamespace = { dev: namespacePin.dev, ino: namespacePin.ino };
  if (typeof targetId !== 'string' || !/^[a-f0-9]{32}$/.test(targetId)
      || typeof mountNamespace.dev !== 'bigint' || mountNamespace.dev < 0n
      || typeof mountNamespace.ino !== 'bigint' || mountNamespace.ino <= 0n) targetError();
  const namespace = pin(value.namespace), dataRoot = pin(value.dataRoot), configRoot = pin(value.configRoot);
  if (dataRoot.path !== `${namespace.path}/data` || configRoot.path !== `${namespace.path}/config`
      || same(namespace, dataRoot) || same(namespace, configRoot) || same(dataRoot, configRoot)) targetError();
  return Object.freeze({ targetId, mountNamespace: Object.freeze(mountNamespace), namespace, dataRoot, configRoot });
}

class LinuxSink {
  handles = new Set(); ancestors = new Map(); directories = new Map(); roots = {}; current = null;
  prepared = false; finished = false; closing = null;
  constructor(target, limits, check) {
    this.target = target; this.limits = limits; this.check = check; this.uid = BigInt(process.geteuid());
  }
  async step(action, progress = false) {
    this.check(); const result = await action(); this.check(progress); return result;
  }
  async acquire(path, flags, mode) {
    this.check(); const handle = await open(path, flags, mode); this.handles.add(handle); this.check(); return handle;
  }
  async release(handle) { await handle.close(); this.handles.delete(handle); }
  async mountId(handle) {
    // Fixed kernel path, never archive input. One bounded proc read, also owned
    // and awaited on all paths; IDs belong to this helper's mount namespace.
    const info = await this.acquire(`/proc/self/fdinfo/${handle.fd}`, constants.O_RDONLY);
    const bytes = Buffer.alloc(4097);
    try {
      let at = 0;
      for (;;) {
        const { bytesRead } = await this.step(() => info.read(bytes, at, bytes.length - at, at));
        at += bytesRead;
        if (at === bytes.length) targetError();
        if (!bytesRead) break;
      }
      const matches = [...bytes.subarray(0, at).toString('ascii').matchAll(/^mnt_id:\s*([1-9]\d*)$/gm)];
      const value = matches.length === 1 ? Number(matches[0][1]) : NaN;
      if (!Number.isSafeInteger(value) || value <= 0) targetError();
      return value;
    } finally { bytes.fill(0); await this.release(info); }
  }
  privateDirectory(value) {
    if (!value.isDirectory() || value.uid !== this.uid || (value.mode & 0o7777n) !== 0o700n) targetError();
  }
  async directory(path, expected, { privateMode = false, keep = false } = {}) {
    const before = await this.step(() => lstat(path, { bigint: true }));
    if (!before.isDirectory() || expected && !same(before, expected)) targetError();
    const handle = await this.acquire(path, DIR);
    try {
      const observed = await this.step(() => handle.stat({ bigint: true }));
      const mountId = await this.mountId(handle), after = await this.step(() => lstat(path, { bigint: true }));
      if (!observed.isDirectory() || !same(before, observed) || !same(observed, after)
          || !after.isDirectory() || expected && (!same(expected, observed) || expected.mountId !== mountId)) targetError();
      if (privateMode) this.privateDirectory(observed);
      const record = { path, ...identity(observed), mountId, handle: keep ? handle : null };
      return record;
    } finally { if (!keep) await this.release(handle); }
  }
  async initialize() {
    if (!same(await this.step(() => stat('/proc/self/ns/mnt', { bigint: true })), this.target.mountNamespace)) targetError();
    let path = '';
    for (const part of ['', ...this.target.namespace.path.slice(1).split('/')]) {
      path = part === '' ? '/' : path === '/' ? `/${part}` : `${path}/${part}`;
      const record = await this.directory(path);
      this.ancestors.set(path, record);
    }
    for (const name of ['namespace', 'dataRoot', 'configRoot'])
      this.roots[name] = await this.directory(this.target[name].path, this.target[name], { privateMode: true, keep: true });
    await this.empty();
  }
  async empty() {
    const names = await this.names(this.roots.namespace.path, 3);
    if (names.length !== 2 || !names.includes('data') || !names.includes('config')) targetError();
    for (const name of ['dataRoot', 'configRoot']) if ((await this.names(this.roots[name].path, 1)).length) targetError();
    await this.checkRoots();
  }
  async names(path, bound) {
    this.check(); const directory = await opendir(path, { bufferSize: 4 }); this.handles.add(directory);
    try {
      this.check(); const names = [];
      while (names.length < bound) {
        const entry = await this.step(() => directory.read());
        if (!entry) break; names.push(entry.name);
      }
      return names;
    } finally { await this.release(directory); }
  }
  async checkRoots() {
    if (!same(await this.step(() => stat('/proc/self/ns/mnt', { bigint: true })), this.target.mountNamespace)) targetError();
    for (const record of this.ancestors.values()) {
      const value = await this.step(() => lstat(record.path, { bigint: true }));
      if (!value.isDirectory() || !same(record, value)) targetError();
    }
    for (const [name, record] of Object.entries(this.roots)) {
      const value = await this.step(() => record.handle.stat({ bigint: true }));
      const pathValue = await this.step(() => lstat(record.path, { bigint: true }));
      if (!value.isDirectory() || !pathValue.isDirectory() || !same(record, value) || !same(record, pathValue)
          || record.mountId !== await this.mountId(record.handle)) targetError();
      if (name !== 'dataRoot' || !this.finished) this.privateDirectory(value);
    }
  }
  async prepare({ dataBytes, externalBytes }) {
    if (this.prepared) targetError();
    await this.empty();
    const grouped = new Map();
    for (const [name, amount] of [['dataRoot', dataBytes], ['configRoot', externalBytes]]) {
      const root = this.roots[name], key = root.dev.toString();
      const group = grouped.get(key) ?? { root, amount: 0n };
      group.amount += BigInt(amount); grouped.set(key, group);
    }
    for (const { root, amount } of grouped.values()) {
      const space = await this.step(() => statfs(root.path, { bigint: true }));
      if (space.bavail < 0n || space.bsize <= 0n || space.bavail * space.bsize < amount + BigInt(this.limits.freeSpaceReserveBytes))
        throw formatFailure('restore_limit_exceeded');
    }
    await this.checkRoots(); this.prepared = true;
  }
  async parents(name) {
    await this.checkRoots();
    let prefix = '';
    for (const part of name.split('/').slice(0, -1)) {
      prefix = prefix ? `${prefix}/${part}` : part;
      const record = this.directories.get(prefix);
      if (!record) targetError();
      await this.directory(record.path, record, { privateMode: true });
    }
  }
  async begin(file) {
    if (!this.prepared || this.current || this.finished) targetError();
    await this.parents(file.path);
    if (file.type === 'directory') {
      if (this.directories.has(file.path)) targetError();
      if (file.path === '') {
        this.directories.set('', { ...this.roots.dataRoot, desired: file }); return;
      }
      const path = `${this.roots.dataRoot.path}/${file.path}`;
      await this.step(() => mkdir(path, { mode: 0o700 }));
      const record = await this.directory(path, null, { privateMode: true });
      if (record.dev !== this.roots.dataRoot.dev || record.mountId !== this.roots.dataRoot.mountId) targetError();
      this.directories.set(file.path, { ...record, desired: file });
    } else await this.file(this.roots.dataRoot, file.path, file);
  }
  async file(root, name, desired) {
    if (this.current) targetError();
    await this.checkRoots();
    const handle = await this.acquire(`${root.path}/${name}`, FILE, 0o600);
    const observed = await this.step(() => handle.stat({ bigint: true }));
    const mountId = await this.mountId(handle);
    if (!observed.isFile() || observed.nlink !== 1n || observed.dev !== root.dev || mountId !== root.mountId
        || observed.uid !== this.uid || (observed.mode & 0o7777n) !== 0o600n || observed.size !== 0n) targetError();
    this.current = { handle, ...identity(observed), mountId, desired, written: 0 };
  }
  async write(bytes) {
    const file = this.current;
    if (!file || bytes.length > CHUNK || bytes.length > file.desired.size - file.written) ioError();
    for (let offset = 0; offset < bytes.length;) {
      const { bytesWritten } = await this.step(() => file.handle.write(bytes, offset, bytes.length - offset, file.written), false);
      if (!Number.isInteger(bytesWritten) || bytesWritten <= 0 || bytesWritten > bytes.length - offset) ioError();
      file.written += bytesWritten; offset += bytesWritten; this.check(true);
    }
  }
  async attributes(handle, expected, desired, regular) {
    await this.step(() => handle.chown(desired.uid, desired.gid));
    await this.step(() => handle.chmod(desired.mode));
    await this.step(() => handle.sync());
    const value = await this.step(() => handle.stat({ bigint: true }));
    if (!same(expected, value) || (regular ? !value.isFile() || value.nlink !== 1n : !value.isDirectory())
        || value.uid !== BigInt(desired.uid) || value.gid !== BigInt(desired.gid)
        || (value.mode & 0o7777n) !== BigInt(desired.mode) || expected.mountId !== await this.mountId(handle)) targetError();
  }
  async end() {
    const file = this.current;
    if (!file || file.written !== file.desired.size) ioError();
    await this.step(() => file.handle.sync());
    const bytes = Buffer.alloc(CHUNK), digest = createHash('sha256');
    try {
      let at = 0;
      while (at < file.written) {
        const { bytesRead } = await this.step(() => file.handle.read(bytes, 0, Math.min(bytes.length, file.written - at), at));
        if (!bytesRead) ioError();
        digest.update(bytes.subarray(0, bytesRead)); at += bytesRead; this.check(true);
      }
      const end = await this.step(() => file.handle.read(bytes, 0, 1, at));
      const info = await this.step(() => file.handle.stat({ bigint: true }));
      if (end.bytesRead || info.size !== BigInt(file.written) || !same(file, info)
          || digest.digest('hex') !== file.desired.sha256) throw formatFailure('restore_incomplete');
    } finally { bytes.fill(0); }
    await this.attributes(file.handle, file, file.desired, true);
    await this.release(file.handle); this.current = null; this.check();
  }
  async config(file, bytes) {
    if (!this.prepared || this.finished) targetError();
    await this.file(this.roots.configRoot, file.id, file);
    for (let at = 0; at < bytes.length; at += CHUNK) await this.write(bytes.subarray(at, at + CHUNK));
    await this.end();
  }
  async finish() {
    if (!this.prepared || this.current || this.finished || !this.directories.has('')) targetError();
    // Reopen only one directory at a time. Its ancestors still have temporary
    // helper-owned 0700 metadata; no FD array proportional to entry count.
    const depth = name => name === '' ? 0 : name.split('/').length;
    const ordered = [...this.directories.entries()].sort(([a], [b]) => depth(b) - depth(a) || (a < b ? -1 : a > b ? 1 : 0));
    for (const [name, record] of ordered) {
      await this.parents(name);
      const held = name === '', opened = held ? this.roots.dataRoot : await this.directory(record.path, record, { privateMode: true, keep: true });
      try { await this.attributes(opened.handle, record, record.desired, false); }
      finally { if (!held) await this.release(opened.handle); }
      if (held) this.finished = true;
    }
    for (const root of [this.roots.configRoot, this.roots.namespace]) await this.step(() => root.handle.sync());
    await this.checkRoots();
  }
  close() {
    if (!this.closing) this.closing = (async () => {
      let failed = false;
      for (const handle of [...this.handles]) {
        try { await this.release(handle); } catch { failed = true; }
      }
      if (failed || this.handles.size) throw formatFailure('restore_cleanup_pending');
    })();
    return this.closing;
  }
}
export async function openRestoreSink(target, limits, check) {
  const sink = new LinuxSink(target, limits, check);
  try { await sink.initialize(); return sink; }
  catch (error) { await sink.close(); throw error; }
}
