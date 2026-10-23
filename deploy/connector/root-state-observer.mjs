import { constants, openSync, readSync, closeSync, statSync } from 'node:fs';
import { lstat, realpath, open, opendir, mkdtemp, unlink, rmdir, statfs } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { endianness } from 'node:os';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { FEEDBACK_CATALOG_ADMISSION, captureFeedbackCatalogAdmission, inspectFeedbackCatalogAdmission } from './feedback-catalog-admission.mjs';

// Host-only observation, not a STOP/single-writer attestation. The caller owns
// the read-only source mount, isolated helper, swap policy and resource fence.
export const ROOT_STATE_OBSERVER_LIMITS = Object.freeze({
  entries: 20_000, bytes: 192 * 1024 ** 2, depth: 32, tables: 4096,
  rows: 300_000, logicalBytes: 256 * 1024 ** 2, valueBytes: 8 * 1024 ** 2, wallMs: 60_000,
});
const SCHEMA = 'soty.root-state-observation.v1';
const MAGIC = Buffer.from('SQLite format 3\0');
const READ = constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK;
const DIR = READ | constants.O_DIRECTORY;
const CODES = new Set(['root_state_invalid', 'root_state_changed', 'root_state_limit',
  'root_state_ram_invalid', 'root_state_sqlite_invalid', 'root_state_timeout',
  'root_state_cleanup_failed', 'root_state_unavailable', 'root_state_catalog_admission_invalid']);
const refuse = code => { throw Object.assign(new Error(code), { code, stack: `Error: ${code}` }); };
const safe = error => CODES.has(error?.code) ? error.code : 'root_state_unavailable';
const hash = () => createHash('sha256');
const stamp = s => [s.dev, s.ino, s.mode, s.uid, s.gid, s.nlink, s.size, s.mtimeNs, s.ctimeNs].join(':');
const rights = s => [Number(s.mode & 0o7777n), s.uid.toString(), s.gid.toString()];
const sameRights = (a, b) => JSON.stringify(rights(a)) === JSON.stringify(rights(b));
const fdPath = handle => `/proc/self/fd/${handle.fd}`;
const quote = name => '"' + name.replaceAll('"', '""') + '"';
const within = (a, b) => { const r = path.relative(a, b); return r === '' || !r.startsWith('../') && !path.isAbsolute(r); };

function frame(digest, value) {
  const data = Buffer.isBuffer(value) ? value : Buffer.from(value);
  const size = Buffer.allocUnsafe(4); size.writeUInt32BE(data.length);
  digest.update(size).update(data);
}
function options(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)
    || Object.keys(input).some(key => !['source', 'ramRoot', 'limits', 'feedbackCatalogAdmission'].includes(key))) refuse('root_state_invalid');
  for (const key of ['source', 'ramRoot']) if (typeof input[key] !== 'string' || !path.isAbsolute(input[key])
    || !input[key].isWellFormed() || input[key].includes('\0')) refuse('root_state_invalid');
  const cap = { ...ROOT_STATE_OBSERVER_LIMITS };
  if (input.limits !== undefined) {
    if (!input.limits || typeof input.limits !== 'object' || Array.isArray(input.limits)) refuse('root_state_invalid');
    for (const [key, value] of Object.entries(input.limits)) {
      if (!Object.hasOwn(cap, key) || !Number.isSafeInteger(value) || value <= 0 || value > cap[key]) refuse('root_state_invalid');
      cap[key] = value;
    }
  }
  const source = path.resolve(input.source), ramRoot = path.resolve(input.ramRoot);
  if (within(source, ramRoot) || within(ramRoot, source)) refuse('root_state_invalid');
  return { source, ramRoot, cap, ...(input.feedbackCatalogAdmission === undefined ? {} : {
    feedbackCatalogAdmission: captureFeedbackCatalogAdmission(input.feedbackCatalogAdmission) }) };
}
async function physicalDirectory(file) {
  if (await realpath(file) !== file) refuse('root_state_invalid');
  const before = await lstat(file, { bigint: true });
  if (!before.isDirectory()) refuse('root_state_invalid');
  const handle = await open(file, DIR);
  if (stamp(before) !== stamp(await handle.stat({ bigint: true }))) { await handle.close(); refuse('root_state_changed'); }
  return handle;
}
async function ramCheck(root) {
  const s = await lstat(root, { bigint: true }), fs = await statfs(root, { bigint: true });
  if (!s.isDirectory() || await realpath(root) !== root || s.uid !== BigInt(process.geteuid())
    || (s.mode & 0o7777n) !== 0o700n || fs.type !== 0x01021994n) refuse('root_state_ram_invalid');
  return { stat: s, available: fs.bavail * fs.bsize };
}

async function readFile(handle, before, cap, check, output) {
  if (!before.isFile() || before.nlink !== 1n || stamp(before) !== stamp(await handle.stat({ bigint: true }))) refuse('root_state_changed');
  const digest = hash(), buffer = Buffer.allocUnsafe(64 * 1024); let position = 0, header = Buffer.alloc(0);
  while (true) {
    check(); const { bytesRead } = await handle.read(buffer, 0, buffer.length, position);
    if (!bytesRead) break;
    position += bytesRead;
    if (position > Number(before.size) || position > cap.bytes) refuse('root_state_changed');
    const chunk = buffer.subarray(0, bytesRead); digest.update(chunk);
    if (header.length < 136) header = Buffer.concat([header, chunk.subarray(0, 136 - header.length)]);
    if (output) await output.writeFile(chunk);
  }
  if (BigInt(position) !== before.size || stamp(before) !== stamp(await handle.stat({ bigint: true }))) refuse('root_state_changed');
  return { sha256: digest.digest('hex'), header };
}

// Directory FDs anchor every traversal; no untrusted intermediate symlink is followed.
async function inventory(root, cap, check, expected) {
  const entries = new Map(); let bytes = 0;
  async function visit(handle, relative) {
    check(); const before = await handle.stat({ bigint: true });
    add(relative, before, 'directory');
    const names = [], directory = await opendir(fdPath(handle), { encoding: 'buffer', bufferSize: 32 });
    for await (const entry of directory) {
      if (names.length + entries.size >= cap.entries) refuse('root_state_limit');
      names.push(new TextDecoder('utf-8', { fatal: true }).decode(entry.name));
    }
    names.sort();
    for (const name of names) {
      check(); const rel = relative === '.' ? name : relative + '/' + name;
      if (rel.split('/').length > cap.depth || Buffer.byteLength(rel) > 4096) refuse('root_state_limit');
      const filename = fdPath(handle) + '/' + name, s = await lstat(filename, { bigint: true });
      if (s.isDirectory()) {
        const child = await open(filename, DIR);
        try { if (stamp(s) !== stamp(await child.stat({ bigint: true }))) refuse('root_state_changed'); await visit(child, rel); }
        finally { await child.close(); }
      } else if (s.isFile() && s.nlink === 1n) {
        bytes += Number(s.size); if (!Number.isSafeInteger(bytes) || bytes > cap.bytes) refuse('root_state_limit');
        const e = add(rel, s, 'file');
        if (!expected) {
          const file = await open(filename, READ);
          try { Object.assign(e, await readFile(file, s, cap, check)); } finally { await file.close(); }
        }
        if (stamp(s) !== stamp(await lstat(filename, { bigint: true }))) refuse('root_state_changed');
      } else refuse('root_state_invalid');
    }
    if (stamp(before) !== stamp(await handle.stat({ bigint: true }))) refuse('root_state_changed');
  }
  function add(rel, stat, kind) {
    if (entries.size >= cap.entries || entries.has(rel)) refuse('root_state_limit');
    if (expected && (!expected.has(rel) || stamp(expected.get(rel).stat) !== stamp(stat))) refuse('root_state_changed');
    const entry = { rel, stat, kind }; entries.set(rel, entry); return entry;
  }
  await visit(root, '.');
  if (expected && entries.size !== expected.size) refuse('root_state_changed');
  return entries;
}
async function copyEntry(root, entries, entry, destination, cap, check) {
  const parts = entry.rel.split('/'), handles = []; let parent = root, file, output;
  try {
    for (let i = 0; i < parts.length - 1; i++) {
      const handle = await open(fdPath(parent) + '/' + parts[i], DIR); handles.push(handle); parent = handle;
      if (stamp(await handle.stat({ bigint: true })) !== stamp(entries.get(parts.slice(0, i + 1).join('/')).stat)) refuse('root_state_changed');
    }
    file = await open(fdPath(parent) + '/' + parts.at(-1), READ);
    output = await open(destination, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
    const result = await readFile(file, entry.stat, cap, check, output);
    if (result.sha256 !== entry.sha256) refuse('root_state_changed');
  } finally {
    await output?.close(); await file?.close();
    for (const handle of handles.reverse()) await handle.close();
  }
}

// Validate committed WAL frames; SQLite ignores obsolete or unused aborted tails.
function walChecksum(buffer, little, pair = [0, 0]) {
  let [a, b] = pair;
  for (let i = 0; i < buffer.length; i += 8) {
    a = (a + (little ? buffer.readUInt32LE(i) : buffer.readUInt32BE(i)) + b) >>> 0;
    b = (b + (little ? buffer.readUInt32LE(i + 4) : buffer.readUInt32BE(i + 4)) + a) >>> 0;
  }
  return [a, b];
}
function validateWal(file, pageSize) {
  const length = statSync(file).size; if (!length) return;
  const fd = openSync(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const header = Buffer.alloc(32);
    if (readSync(fd, header, 0, 32, 0) !== 32) refuse('root_state_sqlite_invalid');
    const magic = header.readUInt32BE(0), little = magic === 0x377f0682;
    if (![0x377f0682, 0x377f0683].includes(magic) || header.readUInt32BE(4) !== 3007000
      || header.readUInt32BE(8) !== pageSize || (length - 32) % (pageSize + 24) !== 0) refuse('root_state_sqlite_invalid');
    let pair = walChecksum(header.subarray(0, 24), little);
    if (pair[0] !== header.readUInt32BE(24) || pair[1] !== header.readUInt32BE(28)) refuse('root_state_sqlite_invalid');
    const frameBuffer = Buffer.alloc(pageSize + 24); let checksumFailed = false;
    for (let at = 32; at < length; at += frameBuffer.length) {
      if (readSync(fd, frameBuffer, 0, frameBuffer.length, at) !== frameBuffer.length) refuse('root_state_sqlite_invalid');
      // Reset WALs can retain several old generations, including before a new commit.
      if (!frameBuffer.subarray(8, 16).equals(header.subarray(16, 24))) break;
      if (!frameBuffer.readUInt32BE(0)) refuse('root_state_sqlite_invalid');
      if (!checksumFailed) {
        pair = walChecksum(frameBuffer.subarray(0, 8), little, pair);
        pair = walChecksum(frameBuffer.subarray(24), little, pair);
        checksumFailed = pair[0] !== frameBuffer.readUInt32BE(16) || pair[1] !== frameBuffer.readUInt32BE(20);
      }
      // Once invalid, inspect commit markers only; never resume checksum/application.
      if (checksumFailed && frameBuffer.readUInt32BE(4)) refuse('root_state_sqlite_invalid');
    }
  } finally { closeSync(fd); }
}
function validateShm(entry, pageSize) {
  if (entry.stat.size === 0n) return;
  const h = entry.header, little = endianness() === 'LE', u32 = at => little ? h.readUInt32LE(at) : h.readUInt32BE(at);
  const p = little ? h.readUInt16LE(14) : h.readUInt16BE(14);
  if (entry.stat.size < 32768n || entry.stat.size % 32768n !== 0n || h.length < 96
    || !h.subarray(0, 48).equals(h.subarray(48, 96)) || u32(0) !== 3007000
    || h[12] !== 1 || h[13] > 1 || (p === 1 ? 65536 : p) !== pageSize) refuse('root_state_sqlite_invalid');
  const pair = walChecksum(h.subarray(0, 40), little);
  if (pair[0] !== u32(40) || pair[1] !== u32(44)) refuse('root_state_sqlite_invalid');
}

async function runDatabase(file, cap, milliseconds, wal, feedbackCatalogAdmission) {
  // The helper still needs an OS hard deadline/memory fence: isolate termination
  // cannot promise immediate preemption while SQLite is inside native code.
  const worker = new Worker(new URL(import.meta.url), { workerData: { kind: SCHEMA, file, cap, wal, ...(feedbackCatalogAdmission ? { feedbackCatalogAdmission } : {}) },
    env: {}, execArgv: [], stdout: true, stderr: true,
    resourceLimits: { maxOldGenerationSizeMb: 128, maxYoungGenerationSizeMb: 16, stackSizeMb: 4 } });
  worker.stdout.resume(); worker.stderr.resume();
  let result, timedOut = false;
  const timer = setTimeout(() => { timedOut = true; void worker.terminate(); }, Math.max(1, milliseconds));
  try {
    await new Promise((resolve, reject) => {
      worker.once('message', message => { result = message; });
      worker.once('error', () => reject(Object.assign(Error('root_state_sqlite_invalid'), { code: 'root_state_sqlite_invalid' })));
      worker.once('exit', code => code === 0 ? resolve() : reject(Object.assign(Error('root_state_sqlite_invalid'), { code: 'root_state_sqlite_invalid' })));
    });
    if (timedOut) refuse('root_state_timeout');
    if (result?.ok !== true) refuse(CODES.has(result?.code) ? result.code : 'root_state_sqlite_invalid');
    return result;
  } catch (error) { if (timedOut) refuse('root_state_timeout'); throw error; }
  finally { clearTimeout(timer); await worker.terminate(); }
}
async function cleanWork(work, identity) {
  const now = await lstat(work, { bigint: true });
  if (now.dev !== identity.dev || now.ino !== identity.ino || !now.isDirectory()
    || now.uid !== BigInt(process.geteuid()) || (now.mode & 0o7777n) !== 0o700n) refuse('root_state_cleanup_failed');
  const directory = await opendir(work);
  for await (const entry of directory) {
    if (!['state.sqlite', 'state.sqlite-wal', 'state.sqlite-shm', 'state.sqlite-journal'].includes(entry.name)) refuse('root_state_cleanup_failed');
    const file = path.join(work, entry.name), s = await lstat(file, { bigint: true });
    if (!s.isFile() || s.nlink !== 1n || s.uid !== BigInt(process.geteuid())) refuse('root_state_cleanup_failed');
    await unlink(file);
  }
}

/** No paths/rows/SQL/configuration escape this port. Equality uses treeSha256,
 * which includes all POSIX ownership/modes, schema, persistent PRAGMAs and rows.
 * Only verified exact DB-WAL/SHM groups normalize physical checkpoint effects;
 * ungrouped files (including orphan sidecars and rollback journals) stay raw. */
export async function observeRootState(input) {
  let root, work, workIdentity, failure, result;
  try {
    if (process.platform !== 'linux') refuse('root_state_unavailable');
    const { source, ramRoot, cap, feedbackCatalogAdmission } = options(input), started = Date.now();
    const check = () => { if (Date.now() - started >= cap.wallMs) refuse('root_state_timeout'); };
    const ram = await ramCheck(ramRoot); root = await physicalDirectory(source);
    const rootIdentity = stamp(await root.stat({ bigint: true }));
    const entries = await inventory(root, cap, check);
    const databases = [...entries.values()].filter(e => e.kind === 'file' && e.header.subarray(0, 16).equals(MAGIC));
    const sidecars = new Set(), dbDigests = new Map(), normalizedDbDigests = new Map();
    let tables = 0, rows = 0, records = 0, logicalBytes = 0, normalizedRemoved = 0, catalogProof;
    work = await mkdtemp(path.join(ramRoot, 'root-state-')); workIdentity = await lstat(work, { bigint: true });
    for (const entry of databases) {
      check(); if (entry.header.length < 100) refuse('root_state_sqlite_invalid');
      const encodedPageSize = entry.header.readUInt16BE(16), pageSize = encodedPageSize === 1 ? 65536 : encodedPageSize;
      if (pageSize < 512 || pageSize > 65536 || pageSize & (pageSize - 1)) refuse('root_state_sqlite_invalid');
      const wal = entries.get(entry.rel + '-wal'), shm = entries.get(entry.rel + '-shm'), journal = entries.get(entry.rel + '-journal');
      for (const auxiliary of [wal, shm].filter(Boolean)) {
        if (auxiliary.kind !== 'file' || auxiliary.header.subarray(0, 16).equals(MAGIC) || sidecars.has(auxiliary.rel)
          || !sameRights(auxiliary.stat, entry.stat)) refuse('root_state_sqlite_invalid');
        sidecars.add(auxiliary.rel);
      }
      // Source SHM is a nondurable cache; its index is rebuilt only in RAM.
      if (journal && (journal.kind !== 'file' || journal.stat.size !== 0n)) refuse('root_state_sqlite_invalid');
      const needed = entry.stat.size + (wal?.stat.size ?? 0n) + 4n * 1024n ** 2n;
      if (needed > ram.available || (await ramCheck(ramRoot)).available < needed) refuse('root_state_limit');
      try {
        const target = path.join(work, 'state.sqlite');
        await copyEntry(root, entries, entry, target, cap, check);
        if (wal) await copyEntry(root, entries, wal, target + '-wal', cap, check);
        // Never reuse source SHM: SQLite rebuilds its derived index only in RAM.
        const admission = feedbackCatalogAdmission && entry.rel === FEEDBACK_CATALOG_ADMISSION.path
          ? { relativePath: entry.rel, input: feedbackCatalogAdmission } : undefined;
        const observed = await runDatabase(target, { ...cap, tables: cap.tables - tables,
          rows: cap.rows - records, logicalBytes: cap.logicalBytes - logicalBytes }, cap.wallMs - (Date.now() - started), Boolean(wal), admission);
        dbDigests.set(entry.rel, observed.sha256); tables += observed.tables; rows += observed.rows;
        records += observed.records; logicalBytes += observed.logicalBytes;
        if (admission) {
          catalogProof = observed.catalogAdmission;
          normalizedRemoved = observed.rows - observed.normalizedRows;
          normalizedDbDigests.set(entry.rel, observed.normalizedSha256);
        }
      } finally { await cleanWork(work, workIdentity); }
    }
    if (feedbackCatalogAdmission && !catalogProof) refuse('root_state_catalog_admission_invalid');
    await inventory(root, cap, check, entries);
    if (await realpath(source) !== source || stamp(await lstat(source, { bigint: true })) !== rootIdentity
      || stamp(await root.stat({ bigint: true })) !== rootIdentity) refuse('root_state_changed');
    const ramAfter = await ramCheck(ramRoot);
    if (ramAfter.stat.dev !== ram.stat.dev || ramAfter.stat.ino !== ram.stat.ino) refuse('root_state_ram_invalid');
    const digest = hash(), normalizedDigest = feedbackCatalogAdmission ? hash() : null;
    const treeFrame = value => { frame(digest, value); if (normalizedDigest) frame(normalizedDigest, value); };
    treeFrame(SCHEMA); let files = 0, directories = 0;
    for (const [rel, entry] of [...entries].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) {
      if (sidecars.has(rel)) continue;
      treeFrame(JSON.stringify([rel, entry.kind, rights(entry.stat)]));
      if (entry.kind === 'directory') directories++;
      else {
        files++;
        treeFrame(dbDigests.has(rel) ? 'sqlite' : 'raw');
        const value = dbDigests.get(rel) ?? JSON.stringify([entry.stat.size.toString(), entry.sha256]);
        frame(digest, value);
        if (normalizedDigest) frame(normalizedDigest, normalizedDbDigests.get(rel) ?? value);
      }
    }
    result = Object.freeze({ ok: true, schema: SCHEMA, treeSha256: digest.digest('hex'),
      files, directories, sqliteDatabases: databases.length, tables, rows });
    if (feedbackCatalogAdmission) result = Object.freeze({ ok: true, schema: 'soty.feedback-catalog-observation.v1',
      raw: result, normalized: Object.freeze({ ...result, treeSha256: normalizedDigest.digest('hex'), rows: rows - normalizedRemoved }),
      admission: Object.freeze(catalogProof) });
  } catch (error) { failure = safe(error); }
  finally {
    try { if (work) { await cleanWork(work, workIdentity); await rmdir(work); } }
    catch { failure = 'root_state_cleanup_failed'; }
    try { await root?.close(); } catch { failure ??= 'root_state_unavailable'; }
  }
  if (failure) refuse(failure);
  return result;
}

async function sqliteObservation(file, cap, wal, feedbackCatalogAdmission) {
  const { DatabaseSync } = await import('node:sqlite');
  if (wal) {
    const fd = openSync(file, constants.O_RDONLY | constants.O_NOFOLLOW), header = Buffer.alloc(100);
    try { if (readSync(fd, header, 0, 100, 0) !== 100) refuse('root_state_sqlite_invalid'); }
    finally { closeSync(fd); }
    const p = header.readUInt16BE(16); validateWal(file + '-wal', p === 1 ? 65536 : p);
  }
  const db = new DatabaseSync(file, { readOnly: true, allowExtension: false, defensive: true,
    enableDoubleQuotedStringLiterals: false, timeout: 0 });
  let records = 0, logicalBytes = 0, rows = 0;
  function atom(digest, value) {
    let tag, bytes;
    if (value === null) { tag = 'null'; bytes = Buffer.alloc(0); }
    else if (typeof value === 'bigint') { tag = 'integer'; bytes = Buffer.from(value.toString()); }
    else if (typeof value === 'number') { tag = 'real'; bytes = Buffer.allocUnsafe(8); bytes.writeDoubleBE(value); }
    else if (typeof value === 'string') { tag = 'text'; bytes = Buffer.from(value); }
    else if (ArrayBuffer.isView(value)) { tag = 'blob'; bytes = Buffer.from(value.buffer, value.byteOffset, value.byteLength); }
    else refuse('root_state_sqlite_invalid');
    logicalBytes += bytes.length + tag.length + 8;
    if (bytes.length > cap.valueBytes || logicalBytes > cap.logicalBytes) refuse('root_state_limit');
    frame(digest, tag); frame(digest, bytes);
  }
  function record(values) {
    if (++records > cap.rows) refuse('root_state_limit');
    const digest = hash(); for (const value of values) atom(digest, value); return digest.digest('hex');
  }
  function statement(sql) { const s = db.prepare(sql); s.setReturnArrays(true); s.setReadBigInts(true); return s; }
  function collection(sql, accept = () => {}) {
    const hashes = [];
    for (const row of statement(sql).iterate()) { hashes.push(record(row)); accept(row); }
    return hashes.sort();
  }
  try {
    db.exec('PRAGMA query_only=ON; PRAGMA trusted_schema=OFF; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-2048; BEGIN');
    const integrity = statement('PRAGMA integrity_check(1)').get();
    if (integrity?.length !== 1 || integrity[0] !== 'ok') refuse('root_state_sqlite_invalid');
    if (statement('PRAGMA foreign_key_check').get() !== undefined) refuse('root_state_sqlite_invalid');
    const digest = hash(), normalizedDigest = feedbackCatalogAdmission ? hash() : null;
    let catalog;
    const stateFrame = value => { frame(digest, value); if (normalizedDigest) frame(normalizedDigest, value); };
    stateFrame('soty.root-sqlite-state.v1');
    for (const pragma of ['application_id', 'user_version', 'encoding', 'page_size', 'auto_vacuum', 'journal_mode', 'schema_version']) {
      stateFrame(pragma); stateFrame(JSON.stringify(collection('PRAGMA ' + pragma)));
    }
    const encoding = db.prepare('PRAGMA encoding').get().encoding;
    const decoder = new TextDecoder(encoding.toLowerCase(), { fatal: true }), schemaTables = new Set(), schemaViews = new Set();
    const schema = collection('SELECT CAST(type AS BLOB),CAST(name AS BLOB),CAST(tbl_name AS BLOB),rootpage,CAST(sql AS BLOB) FROM sqlite_schema', row => {
      const type = decoder.decode(row[0]), name = decoder.decode(row[1]);
      if (type === 'table') schemaTables.add(name);
      if (type === 'view') schemaViews.add(name);
    });
    stateFrame(JSON.stringify(schema));
    const tables = [];
    const list = collection('PRAGMA main.table_list', row => {
      if (row[0] === 'main' && row[1] !== 'sqlite_schema') {
        if (row[2] === 'view') { if (!schemaViews.delete(row[1])) refuse('root_state_sqlite_invalid'); return; }
        if (!schemaTables.delete(row[1]) || !['table', 'virtual', 'shadow'].includes(row[2])) refuse('root_state_sqlite_invalid');
        tables.push({ name: row[1], withoutRowid: row[4] === 1n });
        if (tables.length > cap.tables) refuse('root_state_limit');
      }
    });
    if (schemaTables.size || schemaViews.size) refuse('root_state_sqlite_invalid');
    stateFrame(JSON.stringify(list));
    for (const table of tables.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0)) {
      const columns = [], metadata = collection('PRAGMA main.table_xinfo(' + quote(table.name) + ')', row => {
        // Virtual input/pseudo columns are not persistent; all shadow tables,
        // ordinary and generated columns remain covered, including FTS content.
        if (row[6] !== 1n) columns.push(row[1]);
      });
      if (!columns.length || columns.length > 2048) refuse('root_state_sqlite_invalid');
      if (feedbackCatalogAdmission && table.name === 'cap_contracts') catalog = inspectFeedbackCatalogAdmission(
        db, feedbackCatalogAdmission.relativePath, feedbackCatalogAdmission.input);
      const expressions = columns.map(quote);
      if (!table.withoutRowid) {
        const rowid = ['rowid', '_rowid_', 'oid'].find(name => !columns.some(c => c.toLowerCase() === name));
        if (!rowid) refuse('root_state_sqlite_invalid');
        expressions.unshift(quote(rowid));
      }
      const selection = expressions.map(c => `typeof(${c}),length(CAST(${c} AS BLOB)),CASE WHEN length(CAST(${c} AS BLOB))>${cap.valueBytes} THEN NULL WHEN typeof(${c})='text' THEN CAST(${c} AS BLOB) ELSE ${c} END`).join(',');
      const hashes = [], normalizedHashes = normalizedDigest ? [] : null;
      for (const row of statement('SELECT ' + selection + ' FROM ' + quote(table.name) + ' NOT INDEXED').iterate()) {
        for (let i = 0; i < row.length; i += 3) {
          if (row[i + 1] !== null && row[i + 1] > BigInt(cap.valueBytes)) refuse('root_state_limit');
          const type = row[i], value = row[i + 2];
          if (!['null', 'integer', 'real', 'text', 'blob'].includes(type)
            || (type === 'null' ? value !== null : type === 'integer' ? typeof value !== 'bigint'
              : type === 'real' ? typeof value !== 'number' : !ArrayBuffer.isView(value))) refuse('root_state_sqlite_invalid');
        }
        rows++; const rowHash = record(row); hashes.push(rowHash);
        if (normalizedHashes && !(table.name === 'cap_contracts' && catalog?.rowids.has(row[2]))) normalizedHashes.push(rowHash);
      }
      stateFrame(table.name); stateFrame(JSON.stringify(metadata));
      frame(digest, JSON.stringify(hashes.sort()));
      if (normalizedDigest) frame(normalizedDigest, JSON.stringify(normalizedHashes.sort()));
    }
    if (feedbackCatalogAdmission && !catalog) refuse('root_state_catalog_admission_invalid');
    return { ok: true, sha256: digest.digest('hex'), tables: tables.length, rows, records, logicalBytes,
      ...(feedbackCatalogAdmission ? { normalizedSha256: normalizedDigest.digest('hex'), normalizedRows: rows - catalog.rowids.size,
        catalogAdmission: catalog.proof } : {}) };
  } finally { try { db.exec('ROLLBACK'); } finally { db.close(); } }
}

if (!isMainThread && workerData?.kind === SCHEMA) {
  try { parentPort.postMessage(await sqliteObservation(workerData.file, workerData.cap, workerData.wal, workerData.feedbackCatalogAdmission)); }
  catch (error) { parentPort.postMessage({ ok: false, code: CODES.has(error?.code) ? error.code : 'root_state_sqlite_invalid' }); }
  finally { parentPort.close(); }
}
