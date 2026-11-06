import { lstat as snapshotStat, opendir as snapshotDir, mkdir as snapshotMkdir, copyFile as snapshotCopy, open as snapshotOpen } from 'node:fs/promises';
import { join as snapshotJoin, relative as snapshotRelative } from 'node:path';
import { DatabaseSync as SnapshotDatabase, backup as snapshotBackup } from 'node:sqlite';

async function snapshotCopyFile(from, to) {
  await snapshotCopy(from, to);
}

// Stopped and cold-WAL copies retain the original content fences. Online SQLite
// uses a pinned read transaction, not a claim of one atomic instant across DBs.
export async function snapshotStorage(source, target, maxBytes = 192 * 1024 * 1024, { online = false, maxMilliseconds = 10000 } = {}) {
  if (online) return snapshotOnlineStorage(source, target, maxBytes, maxMilliseconds);
  const refuse = () => { throw Object.assign(new Error('storage_format_unreadable'), { code: 'storage_format_unreadable' }); };
  const root = await snapshotStat(source);
  if (!root.isDirectory() || root.isSymbolicLink()) refuse();
  await snapshotMkdir(target, { mode: 0o700 });
  let bytes = 0; const observed = [[source, root]];
  let hasRoomDatabase = false;
  try { await snapshotStat(snapshotJoin(source, 'rooms-v2.sqlite')); hasRoomDatabase = true; }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  const ignoredJson = new Set(['connector-store.json', 'connector-maintenance.json', 'connector-rollback.json', 'traffic-control.json', 'traffic-exit-pool.json']);
  async function copy(from, to, depth = 0) {
    const before = await snapshotStat(from);
    if (before.isSymbolicLink() || depth > 8) refuse();
    if (before.isDirectory()) {
      await snapshotMkdir(to, { mode: 0o700 });
      for await (const entry of await snapshotDir(from)) await copy(snapshotJoin(from, entry.name), snapshotJoin(to, entry.name), depth + 1);
    } else if (before.isFile()) {
      bytes += before.size; if (bytes > maxBytes) refuse();
      await snapshotCopyFile(from, to);
    } else refuse();
    observed.push([from, before]);
  }
  for await (const entry of await snapshotDir(source)) {
    if (['connect', 'apps', 'notes', 'capabilities', 'app-registration', 'feedback', 'human-identity', 'world', 'canvas', 'materials', 'invitations', 'referrals', 'app-agent-directory', 'monitoring'].includes(entry.name) || /^rooms-v2\.sqlite(?:-wal|-shm|-journal)?$/.test(entry.name)
      || (!hasRoomDatabase && !ignoredJson.has(entry.name) && /^[A-Za-z0-9_-]{16,96}\.json$/.test(entry.name))) await copy(snapshotJoin(source, entry.name), snapshotJoin(target, entry.name));
  }
  for (const [file, before] of observed) {
    const after = await snapshotStat(file);
    if (after.ino !== before.ino || after.size !== before.size || after.mtimeMs !== before.mtimeMs || after.ctimeMs !== before.ctimeMs) refuse();
  }
  return target;
}

async function snapshotOnlineStorage(source, target, maxBytes, maxMilliseconds) {
  const refuse = () => { throw Object.assign(new Error('storage_format_unreadable'), { code: 'storage_format_unreadable' }); };
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > 192 * 1024 * 1024
    || !Number.isFinite(maxMilliseconds) || maxMilliseconds <= 0 || maxMilliseconds > 10000) refuse();
  const deadline = performance.now() + maxMilliseconds;
  const checkTime = () => { if (performance.now() >= deadline) refuse(); };
  const stores = new Set(['connect', 'apps', 'notes', 'capabilities', 'app-registration', 'feedback', 'human-identity', 'world', 'canvas', 'materials', 'invitations', 'referrals', 'app-agent-directory', 'monitoring']);
  const ignored = new Set(['connector-store.json', 'connector-maintenance.json', 'connector-rollback.json', 'traffic-control.json', 'traffic-exit-pool.json']);
  async function inventory() {
    let bytes = 0;
    const entries = new Map(), root = await snapshotStat(source); checkTime();
    if (!root.isDirectory() || root.isSymbolicLink()) refuse();
    entries.set(source, root);
    let rooms = false;
    try { await snapshotStat(snapshotJoin(source, 'rooms-v2.sqlite')); rooms = true; }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    async function visit(file, depth = 0) {
      const value = await snapshotStat(file); checkTime();
      if (value.isSymbolicLink() || depth > 8) refuse();
      entries.set(file, value);
      if (value.isDirectory()) {
        for await (const entry of await snapshotDir(file)) await visit(snapshotJoin(file, entry.name), depth + 1);
      } else if (value.isFile()) { bytes += value.size; if (bytes > maxBytes) refuse(); }
      else refuse();
    }
    for await (const entry of await snapshotDir(source)) {
      if (stores.has(entry.name) || /^rooms-v2\.sqlite(?:-wal|-shm|-journal)?$/.test(entry.name)
        || (!rooms && !ignored.has(entry.name) && /^[A-Za-z0-9_-]{16,96}\.json$/.test(entry.name))) await visit(snapshotJoin(source, entry.name));
    }
    return entries;
  }
  try {
    const before = await inventory(), sqlBacked = new Set(), copied = new Set();
    let capturedBytes = 0;
    const reserve = bytes => { checkTime(); capturedBytes += bytes; if (capturedBytes > maxBytes) refuse(); };
    const destination = file => snapshotJoin(target, snapshotRelative(source, file));
    await snapshotMkdir(target, { mode: 0o700 });
    for (const [file, value] of before) if (file !== source && value.isDirectory()) await snapshotMkdir(destination(file), { mode: 0o700 });
    for (const [file, value] of before) {
      checkTime();
      if (!value.isFile() || !file.endsWith('.sqlite')) continue;
      const family = [file, ...['-wal', '-shm', '-journal'].map(suffix => file + suffix)].filter(name => before.has(name));
      // Opening a cold WAL on a writable test filesystem could itself create SHM.
      // Inspect only its header; the actual source mount remains kernel read-only.
      const header = Buffer.alloc(20), handle = await snapshotOpen(file, 'r');
      try { await handle.read(header, 0, header.length, 0); } finally { await handle.close(); }
      const coldWal = (header[18] === 2 || header[19] === 2) && !before.has(file + '-shm');
      let db;
      if (!coldWal) {
        try {
          db = new SnapshotDatabase(file, { readOnly: true });
          db.exec('PRAGMA query_only=ON; PRAGMA busy_timeout=1000; BEGIN');
          db.prepare('SELECT name FROM sqlite_schema LIMIT 1').get();
        } catch (error) {
          db?.close(); db = undefined;
          // Only the already qualified RO-open-unavailable condition may use a
          // fenced raw copy. Corrupt, busy and backup failures do not fall back.
          if (error.code !== 'ERR_SQLITE_ERROR' || error.errcode !== 14) throw error;
        }
      }
      if (db) {
        try {
          const pageSize = db.prepare('PRAGMA page_size').get().page_size, pages = db.prepare('PRAGMA page_count').get().page_count;
          if (!Number.isSafeInteger(pageSize) || pageSize < 512 || pageSize > 65536 || !Number.isSafeInteger(pages) || pages < 0) refuse();
          const bytes = pageSize * pages; reserve(bytes);
          const count = await snapshotBackup(db, destination(file), { rate: 100, progress: ({ totalPages }) => {
            checkTime(); if (totalPages !== pages) refuse();
          } });
          checkTime();
          if (count !== pages || (await snapshotStat(destination(file))).size !== bytes) refuse();
          family.forEach(name => { sqlBacked.add(name); copied.add(name); });
        } finally { db.close(); }
      }
    }
    for (const [file, value] of before) if (value.isFile() && !copied.has(file)) {
      reserve(value.size);
      await snapshotCopyFile(file, destination(file));
    }
    const after = await inventory();
    if (before.size !== after.size) refuse();
    for (const [file, value] of before) {
      const current = after.get(file);
      if (!current || current.dev !== value.dev || current.ino !== value.ino || current.mode !== value.mode
        || current.isDirectory() !== value.isDirectory() || current.isFile() !== value.isFile()) refuse();
      if (!sqlBacked.has(file) && (current.size !== value.size || current.mtimeMs !== value.mtimeMs || current.ctimeMs !== value.ctimeMs)) refuse();
    }
    checkTime();
    return target;
  } catch { refuse(); }
}
