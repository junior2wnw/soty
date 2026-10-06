import { lstat as snapshotStat, opendir as snapshotDir, mkdir as snapshotMkdir, copyFile as snapshotCopy } from 'node:fs/promises';
import { join as snapshotJoin } from 'node:path';

// Only for an offline volume after the writer gate. SQLite can create its SHM
// in private RAM while the source remains mounted read-only; WAL is retained.
export async function snapshotStorage(source, target, maxBytes = 192 * 1024 * 1024) {
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
      await snapshotCopy(from, to);
    } else refuse();
    observed.push([from, before]);
  }
  for await (const entry of await snapshotDir(source)) {
    if (['apps', 'notes', 'capabilities', 'app-registration', 'feedback', 'human-identity'].includes(entry.name) || /^rooms-v2\.sqlite(?:-wal|-shm|-journal)?$/.test(entry.name)
      || (!hasRoomDatabase && !ignoredJson.has(entry.name) && /^[A-Za-z0-9_-]{16,96}\.json$/.test(entry.name))) await copy(snapshotJoin(source, entry.name), snapshotJoin(target, entry.name));
  }
  for (const [file, before] of observed) {
    const after = await snapshotStat(file);
    if (after.ino !== before.ino || after.size !== before.size || after.mtimeMs !== before.mtimeMs || after.ctimeMs !== before.ctimeMs) refuse();
  }
  return target;
}
