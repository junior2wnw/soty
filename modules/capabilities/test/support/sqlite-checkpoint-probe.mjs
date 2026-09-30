// Standalone diagnostic, not a product PASS gate. It reports the installed
// runtime's real prepared-PRAGMA/checkpoint behavior without pinning a bug.
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';

const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-checkpoint-probe-'));
const results = [];
let sqliteVersion;
try {
  for (const [index, sql, method, walFirst] of [
    [0, null, null, false], [1, 'PRAGMA quick_check(1)', 'get', false], [2, 'PRAGMA quick_check(1)', 'all', false],
    [3, 'PRAGMA foreign_key_check', 'get', false], [4, 'PRAGMA user_version', 'get', false],
    [5, 'SELECT type,name,sql FROM sqlite_schema ORDER BY name', 'all', false],
    [6, 'PRAGMA quick_check', 'all', false], [7, 'PRAGMA quick_check', 'all', true],
    [8, 'PRAGMA quick_check(1)', 'all', true],
  ]) {
    const file = join(directory, `${index}.sqlite`), db = new DatabaseSync(file);
    try {
      sqliteVersion ??= db.prepare('SELECT sqlite_version() AS version').get().version;
      db.exec('BEGIN IMMEDIATE; CREATE TABLE item(id INTEGER); COMMIT');
      if (walFirst) db.exec('PRAGMA journal_mode=WAL');
      db.exec('BEGIN IMMEDIATE');
      if (sql) db.prepare(sql)[method]();
      db.exec('COMMIT');
      if (!walFirst) db.exec('PRAGMA journal_mode=WAL');
      try { db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); results.push({ sql, method, walFirst, checkpoint: 'ok' }); }
      catch (error) { results.push({ sql, method, walFirst, checkpoint: error.message, sqliteCode: error.errcode }); }
    } finally { db.close(); }
    // Observe the connection/mode transition; this does not diagnose Node internals.
    const reopened = new DatabaseSync(file);
    try { reopened.exec('PRAGMA wal_checkpoint(TRUNCATE)'); results.at(-1).afterReopen = 'ok'; }
    finally { reopened.close(); }
  }
} finally {
  if (dirname(resolve(directory)) !== base || !basename(directory).startsWith('soty-checkpoint-probe-')) throw Error('unsafe_fixture_path');
  rmSync(directory, { recursive: true, force: true });
}
console.log(JSON.stringify({ nodeVersion: process.version, sqliteVersion, results }, null, 2));
