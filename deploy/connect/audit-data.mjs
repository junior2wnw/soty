// Run through `docker exec -i <exact container> node --input-type=module`.
// Read-only, aggregate evidence; no tokens, record contents, or account IDs leave the process.
import { DatabaseSync } from 'node:sqlite';
import { readdir, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
const root = '/data';
const result = { format: 'soty.storage-audit.v1', rootJsonFiles: 0, rootJsonNamesSha256: '', connector: {}, connect: null };
const files = (await readdir(root)).filter(name => name.endsWith('.json') && name !== 'connector-maintenance.json').sort();
result.rootJsonFiles = files.length;
result.rootJsonNamesSha256 = createHash('sha256').update(JSON.stringify(files)).digest('hex');
for (const [key, file, tables] of [
  ['connector', 'connector-store.sqlite', ['records', 'jobs', 'inputs', 'results', 'events', 'requests']],
  ['connect', 'connect/accounts.sqlite', ['accounts', 'devices', 'contacts', 'enrollments', 'recoveries']],
]) {
  try { await stat(path.join(root, file)); } catch (e) { if (e.code === 'ENOENT') continue; throw e; }
  const db = new DatabaseSync(path.join(root, file), { readOnly: true });
  try {
    db.exec('PRAGMA query_only=ON; BEGIN');
    const check = db.prepare('PRAGMA integrity_check').all();
    if (check.length !== 1 || Object.values(check[0])[0] !== 'ok') throw new Error('storage_integrity_failed');
    const available = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(r => r.name));
    const counts = {};
    for (const table of tables) if (available.has(table)) counts[table] = db.prepare(`SELECT count(*) AS n FROM "${table}"`).get().n;
    result[key] = { integrity: 'ok', counts };
    if (key === 'connector') {
      result.connector.recordKinds = Object.fromEntries(db.prepare('SELECT kind,count(*) AS n FROM records GROUP BY kind ORDER BY kind').all().map(r => [r.kind, r.n]));
      const hash = createHash('sha256');
      for (const table of ['jobs', 'inputs', 'results', 'events']) {
        const values = db.prepare(`SELECT * FROM "${table}" ORDER BY rowid`).all();
        hash.update(table); hash.update(JSON.stringify(values));
      }
      result.connector.historySha256 = hash.digest('hex');
    }
  } finally { db.close(); }
}
console.log(JSON.stringify(result));
