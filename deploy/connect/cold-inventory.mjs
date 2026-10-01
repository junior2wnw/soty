// Private cold-source collector. Its CLI stdout is a supervisor-owned pipe,
// never a public receipt: it includes encrypted-backup configuration material.
import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, open, opendir, realpath } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const HEX = /^[a-f0-9]{64}$/, ID = /^[a-z][a-z0-9._-]{0,95}$/;
const LIMITS = ['archiveBytes', 'plaintextBytes', 'fileBytes', 'extractedBytes', 'entries', 'headers',
  'pathBytes', 'pathDepth', 'externalFiles', 'externalBytes', 'wallMs', 'idleMs'];
const utf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const digest = value => createHash('sha256').update(value).digest('hex');
const order = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const knownErrors = new WeakSet();
function refuse(code = 'backup_cold_invalid') {
  const error = Object.assign(new Error(code), { code, stack: `Error: ${code}` });
  knownErrors.add(error); throw error;
}
function exact(value, keys) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || Reflect.ownKeys(value).length !== keys.length || keys.some(k => !Object.hasOwn(value, k))) refuse();
}
function absolute(value) {
  if (typeof value !== 'string' || !value.isWellFormed() || Buffer.byteLength(value) > 4096
      || /[\u0000-\u001f\u007f-\u009f]/u.test(value) || !path.isAbsolute(value) || path.resolve(value) !== value) refuse();
  return value;
}
function relative(value, limits, root = false) {
  if (typeof value !== 'string' || !value.isWellFormed() || Buffer.byteLength(value) > 4096
      || /[\u0000-\u001f\u007f-\u009f\\]/u.test(value) || value.startsWith('/') || /^[A-Za-z]:/.test(value)) refuse();
  const parts = value === '' && root ? [] : value.split('/');
  if (parts.some(p => !p || p === '.' || p === '..')) refuse();
  if (parts.length > limits.pathDepth || parts.some(p => Buffer.byteLength(p) > 255)) refuse('backup_cold_limit_exceeded');
  return value;
}
function same(a, b) {
  return ['dev', 'ino', 'mode', 'uid', 'gid', 'size', 'nlink', 'mtimeNs', 'ctimeNs'].every(k => a[k] === b[k]);
}
function attributes(stat) {
  const uid = Number(stat.uid), gid = Number(stat.gid), mode = Number(stat.mode & 0o7777n);
  if (![uid, gid].every(n => Number.isSafeInteger(n) && n >= 0 && n <= 4294967294) || mode > 0o777) refuse();
  return { uid, gid, mode };
}

export async function captureColdInventory(value) {
  try {
    exact(value, ['dataRoot', 'generationId', 'checkpointSha256', 'stores', 'externalFiles', 'limits']);
    const dataRoot = absolute(value.dataRoot), generationId = value.generationId, checkpointSha256 = value.checkpointSha256;
    if (typeof generationId !== 'string' || !/^[a-f0-9]{32}$/.test(generationId)
        || typeof checkpointSha256 !== 'string' || !HEX.test(checkpointSha256)) refuse();
    exact(value.limits, LIMITS);
    const limits = {};
    for (const key of LIMITS) {
      if (!Number.isSafeInteger(value.limits[key]) || value.limits[key] <= 0) refuse('backup_cold_limit_exceeded');
      limits[key] = value.limits[key];
    }
    if (!Array.isArray(value.stores) || !value.stores.length || !Array.isArray(value.externalFiles)) refuse();
    if (value.stores.length > limits.entries || value.externalFiles.length > limits.externalFiles) refuse('backup_cold_limit_exceeded');
    // Capture trusted configuration before the first filesystem await.
    const stores = value.stores.map(s => {
      exact(s, ['id', 'required', 'present', 'format', 'identitySha256', 'paths']);
      if (typeof s.id !== 'string' || !ID.test(s.id) || typeof s.required !== 'boolean' || typeof s.present !== 'boolean'
          || !Array.isArray(s.paths) || s.required && !s.present) refuse();
      if (s.paths.length > limits.entries) refuse('backup_cold_limit_exceeded');
      if (s.present ? typeof s.format !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9._:/@+-]{0,127}$/.test(s.format)
          || typeof s.identitySha256 !== 'string' || !HEX.test(s.identitySha256) || !s.paths.length
        : s.format !== null || s.identitySha256 !== null || s.paths.length) refuse();
      const paths = s.paths.map(p => relative(p, limits)).sort(order);
      if (new Set(paths).size !== paths.length) refuse();
      return { id: s.id, required: s.required, present: s.present, format: s.format, identitySha256: s.identitySha256, paths };
    }).sort((a, b) => order(a.id, b.id));
    const externalInputs = value.externalFiles.map(f => {
      exact(f, ['id', 'required', 'path']);
      if (typeof f.id !== 'string' || !ID.test(f.id) || typeof f.required !== 'boolean' || f.required && f.path === null) refuse();
      return { id: f.id, required: f.required, path: f.path === null ? null : absolute(f.path) };
    }).sort((a, b) => order(a.id, b.id));
    if (new Set(stores.map(s => s.id)).size !== stores.length
        || new Set(externalInputs.map(f => f.id)).size !== externalInputs.length) refuse();
    const started = performance.now(); let progress = started, fileBytes = 0, externalBytes = 0, pathBytes = 0;
    const check = advanced => {
      const now = performance.now();
      if (now - started > limits.wallMs || now - progress > limits.idleMs) refuse('backup_cold_timeout');
      if (advanced) progress = now;
    };
    const add = (current, amount, max) => {
      if (!Number.isSafeInteger(amount) || amount < 0 || amount > max - current) refuse('backup_cold_limit_exceeded');
      return current + amount;
    };
    async function noLinks(filename) {
      const base = path.parse(filename).root, parts = filename.slice(base.length).split(path.sep).filter(Boolean);
      let current = base;
      for (const part of parts) {
        current = path.join(current, part); check();
        if ((await lstat(current, { bigint: true })).isSymbolicLink()) refuse();
      }
      check(); if (await realpath(filename) !== filename) refuse();
    }
    await noLinks(dataRoot);
    const rootStat = await lstat(dataRoot, { bigint: true }); check();
    if (!rootStat.isDirectory()) refuse();
    const files = [], restoreFiles = Object.create(null);
    async function file(filename, before, collect) {
      if (!before.isFile() || before.nlink !== 1n) refuse();
      const size = Number(before.size);
      if (!Number.isSafeInteger(size) || size > limits.fileBytes) refuse('backup_cold_limit_exceeded');
      if (collect) externalBytes = add(externalBytes, size, Math.min(limits.externalBytes, 3 * 1024 * 1024));
      else fileBytes = add(fileBytes, size, limits.extractedBytes);
      add(fileBytes, externalBytes, limits.extractedBytes);
      const handle = await open(filename, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
      const chunk = Buffer.alloc(65536), retained = collect ? Buffer.alloc(size) : null, hash = createHash('sha256');
      try {
        if (!same(before, await handle.stat({ bigint: true }))) refuse('backup_cold_changed');
        let position = 0;
        while (position < size) {
          check(); const result = await handle.read(chunk, 0, Math.min(chunk.length, size - position), position); check(result.bytesRead > 0);
          if (!result.bytesRead) refuse('backup_cold_changed');
          hash.update(chunk.subarray(0, result.bytesRead)); retained?.set(chunk.subarray(0, result.bytesRead), position);
          position += result.bytesRead;
        }
        const end = await handle.read(chunk, 0, 1, size); check();
        if (end.bytesRead || !same(before, await handle.stat({ bigint: true })) || !same(before, await lstat(filename, { bigint: true })))
          refuse('backup_cold_changed');
        return { size, sha256: hash.digest('hex'), ...attributes(before), encoded: retained?.toString('base64') };
      } finally { chunk.fill(0); retained?.fill(0); await handle.close(); }
    }
    async function visit(name) {
      relative(name, limits, true); check();
      if (files.length >= limits.entries) refuse('backup_cold_limit_exceeded');
      pathBytes = add(pathBytes, Buffer.byteLength(name), limits.pathBytes);
      const filename = name ? path.join(dataRoot, ...name.split('/')) : dataRoot;
      const before = await lstat(filename, { bigint: true }); check();
      if (before.dev !== rootStat.dev || before.isSymbolicLink()) refuse();
      if (before.isDirectory()) {
        files.push({ path: name, type: 'directory', size: 0, sha256: null, ...attributes(before) });
        const names = [], directory = await opendir(filename, { encoding: 'buffer' });
        try {
          for (;;) {
            check(); const entry = await directory.read(); check(true); if (!entry) break;
            if (names.length >= limits.entries - files.length) refuse('backup_cold_limit_exceeded');
            const child = typeof entry.name === 'string' ? entry.name : utf8.decode(entry.name);
            relative(name ? `${name}/${child}` : child, limits); names.push(child);
          }
        } finally { await directory.close(); }
        for (const child of names.sort(order)) await visit(name ? `${name}/${child}` : child);
        if (!same(before, await lstat(filename, { bigint: true }))) refuse('backup_cold_changed');
      } else {
        const info = await file(filename, before, false);
        files.push({ path: name, type: 'file', size: info.size, sha256: info.sha256, uid: info.uid, gid: info.gid, mode: info.mode });
      }
    }
    await visit(''); files.sort((a, b) => order(a.path, b.path));
    const byPath = new Map(files.map(f => [f.path, f]));
    for (const store of stores) for (const name of store.paths) if (byPath.get(name)?.type !== 'file') refuse('backup_cold_changed');
    const external = [];
    for (const source of externalInputs) {
      if (source.path === null) {
        external.push({ id: source.id, required: source.required, present: false, size: 0, sha256: null, uid: 0, gid: 0, mode: 0 }); continue;
      }
      await noLinks(source.path); check();
      const info = await file(source.path, await lstat(source.path, { bigint: true }), true);
      external.push({ id: source.id, required: source.required, present: true, size: info.size, sha256: info.sha256, uid: info.uid, gid: info.gid, mode: info.mode });
      restoreFiles[source.id] = info.encoded;
    }
    const inventory = { files, stores, external };
    const restoreManifest = { version: 1, generationId, checkpointSha256, inventory };
    const sourceWitness = { generationId, checkpointSha256, inventorySha256: digest(JSON.stringify(inventory)) };
    const result = { restoreManifest, restoreFiles, sourceWitness, manifestSha256: digest(JSON.stringify(restoreManifest)) };
    if (Buffer.byteLength(JSON.stringify(result)) > 4 * 1024 * 1024) refuse('backup_cold_limit_exceeded');
    check(); return result;
  } catch (error) {
    if (knownErrors.has(error)) throw error;
    refuse('backup_cold_io_failed');
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    const chunks = []; let size = 0;
    for await (const chunk of process.stdin) {
      size += chunk.length; if (size > 65536) refuse('backup_cold_limit_exceeded'); chunks.push(chunk);
    }
    const control = Buffer.concat(chunks);
    let value; try { value = JSON.parse(utf8.decode(control)); } finally { control.fill(0); for (const chunk of chunks) chunk.fill(0); }
    const result = await captureColdInventory(value);
    await new Promise((resolve, reject) => process.stdout.write(JSON.stringify(result), error => error ? reject(error) : resolve()));
  } catch { process.stderr.write('{"code":"backup_cold_failed"}\n'); process.exitCode = 1; }
}
