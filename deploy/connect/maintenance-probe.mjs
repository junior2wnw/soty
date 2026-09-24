// This source is passed to `node -e` in the serving container or a bounded helper.
// It never imports the legacy maintenance rollback, writes SQLite, or returns IDs.
import { DatabaseSync } from 'node:sqlite';
import { readFile, stat, open, unlink } from 'node:fs/promises';
import path from 'node:path';

async function syncDirectory(dataDir) {
  const directory = await open(dataDir, 'r');
  try { await directory.sync(); } finally { await directory.close(); }
}
export async function probe({ dataDir = process.env.DATA_DIR || '/data', verb = 'status', transaction, sync = syncDirectory } = {}) {
  const fail = code => { throw Object.assign(new Error(code), { code }); };
  if (!['status', 'enter', 'leave'].includes(verb)) fail('probe_verb_invalid');
  if (verb !== 'status' && !/^[a-f0-9]{32}$/.test(transaction || '')) fail('probe_transaction_invalid');
  const marker = path.join(dataDir, 'connector-maintenance.json');
  try { await stat(path.join(dataDir, 'connector-rollback.json')); fail('legacy_rollback_pending'); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  let db; let count;
  try {
    await stat(path.join(dataDir, 'connector-store.sqlite'));
    db = new DatabaseSync(path.join(dataDir, 'connector-store.sqlite'), { readOnly: true });
    if (db.prepare("SELECT value FROM meta WHERE key='schema'").get()?.value !== 'soty.connector-sqlite.v1') fail('probe_schema_invalid');
    count = db.prepare("SELECT count(*) AS count FROM jobs WHERE COALESCE(json_extract(value,'$.status'),'unknown') NOT IN ('succeeded','failed','cancelled')").get().count;
  } finally { db?.close(); }
  let value = null;
  try { value = JSON.parse(await readFile(marker, 'utf8')); }
  catch (error) { if (error.code !== 'ENOENT') fail('probe_marker_invalid'); }
  if (verb === 'enter') {
    if (count !== 0) fail('probe_active_jobs');
    if (value && value.connectTransaction !== transaction) fail('probe_external_maintenance');
    if (!value) {
      value = { schema: 'soty.connector-maintenance.v1', connectTransaction: transaction };
      const handle = await open(marker, 'wx', 0o600);
      try { await handle.writeFile(JSON.stringify(value)); await handle.sync(); } finally { await handle.close(); }
    }
  }
  if (verb === 'leave' && value) {
    if (value.connectTransaction !== transaction) fail('probe_external_maintenance');
    await unlink(marker); value = null;
  }
  if (verb !== 'status') await sync(dataDir);
  return { ok: true, schema: 'soty.connect.maintenance.v1', count, maintenance: Boolean(value), owned: Boolean(value && value.connectTransaction === transaction) };
}

// Imported on the host only to read this file; execution uses an explicit flag.
if (process.env.SOTY_CONNECT_PROBE === '1') {
  try { process.stdout.write(JSON.stringify(await probe({ verb: process.argv[1], transaction: process.argv[2] }))); }
  catch { process.stdout.write(JSON.stringify({ ok: false, code: 'probe_failed' })); process.exitCode = 1; }
}
