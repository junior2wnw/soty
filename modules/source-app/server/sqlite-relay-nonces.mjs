import { DatabaseSync } from 'node:sqlite';
import { closeSync, linkSync, lstatSync, openSync, unlinkSync } from 'node:fs';
import { basename, dirname, isAbsolute, join } from 'node:path';
import { randomBytes } from 'node:crypto';

const META = "CREATE TABLE soty_nonce_meta (id INTEGER PRIMARY KEY CHECK(id=1), schema TEXT NOT NULL, last_ms INTEGER NOT NULL) STRICT";
const USES = "CREATE TABLE soty_nonce_uses (id TEXT PRIMARY KEY, expires_at INTEGER NOT NULL) STRICT";
const EXPIRY = 'CREATE INDEX soty_nonce_expiry ON soty_nonce_uses(expires_at)';
const SCHEMA = 'soty.relay-nonce-store.v1', APP_ID = 1397707865;
const unavailable = () => { throw Object.assign(new Error('soty_identity_store_unavailable'), { code: 'soty_identity_store_unavailable', status: 503 }); };

// Initialize off to the side, then atomically link without replacing an existing file.
function initialize(path) {
  const temporary = join(dirname(path), '.' + basename(path) + '-' + randomBytes(16).toString('hex') + '.tmp');
  let created = false, db;
  try {
    const fd = openSync(temporary, 'wx', 0o600); created = true; closeSync(fd);
    db = new DatabaseSync(temporary);
    db.exec(`BEGIN IMMEDIATE; ${META}; ${USES}; ${EXPIRY}; INSERT INTO soty_nonce_meta VALUES(1,'${SCHEMA}',0); PRAGMA application_id=${APP_ID}; PRAGMA user_version=1; COMMIT;`);
    db.close(); db = undefined;
    try { linkSync(temporary, path); } catch (error) { if (error?.code !== 'EEXIST') throw error; }
  } finally {
    try { db?.close(); } finally { if (created) unlinkSync(temporary); }
  }
}

/** A dedicated local SQLite database; never point this at an application's data database. */
export function createSqliteRelayNonceStore({ databasePath, maxItems = 10000, busyTimeoutMs = 1000, clock = Date.now } = {}) {
  if (typeof databasePath !== 'string' || !isAbsolute(databasePath) || databasePath.includes('\0')
    || !Number.isSafeInteger(maxItems) || maxItems < 1 || maxItems > 100000
    || !Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 0 || busyTimeoutMs > 5000 || typeof clock !== 'function') unavailable();
  let db, closed = false;
  try {
    try { lstatSync(databasePath); } catch (error) { if (error?.code !== 'ENOENT') throw error; initialize(databasePath); }
    const stat = lstatSync(databasePath);
    if (!stat.isFile() || stat.isSymbolicLink()) unavailable();
    db = new DatabaseSync(databasePath);
    db.exec(`PRAGMA busy_timeout=${busyTimeoutMs}; PRAGMA trusted_schema=OFF;`);
    const objects = db.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all();
    const meta = db.prepare('SELECT * FROM soty_nonce_meta').all();
    if (objects.length !== 3 || objects[0].name !== 'soty_nonce_expiry' || objects[0].type !== 'index' || objects[0].sql !== EXPIRY
      || objects[1].name !== 'soty_nonce_meta' || objects[1].type !== 'table' || objects[1].sql !== META
      || objects[2].name !== 'soty_nonce_uses' || objects[2].type !== 'table' || objects[2].sql !== USES
      || db.prepare('PRAGMA application_id').get().application_id !== APP_ID || db.prepare('PRAGMA user_version').get().user_version !== 1
      || meta.length !== 1 || meta[0].id !== 1 || meta[0].schema !== SCHEMA || !Number.isSafeInteger(meta[0].last_ms) || meta[0].last_ms < 0
      || db.prepare('PRAGMA quick_check').get().quick_check !== 'ok') unavailable();
    if (db.prepare('PRAGMA journal_mode=WAL').get()?.journal_mode !== 'wal') unavailable();
    db.exec('PRAGMA synchronous=FULL;');
    if (db.prepare('PRAGMA synchronous').get()?.synchronous !== 2) unavailable();
  } catch { try { db?.close(); } catch {} unavailable(); }
  return Object.freeze({
    consume(id, expiresAt) {
      if (closed || typeof id !== 'string' || !/^[A-Za-z0-9_-]{32}$/u.test(id) || !Number.isSafeInteger(expiresAt)) unavailable();
      try {
        db.exec('BEGIN IMMEDIATE');
        const now = clock();
        if (!Number.isSafeInteger(now) || now < 0 || expiresAt > now + 11000) unavailable();
        const last = db.prepare('SELECT last_ms FROM soty_nonce_meta WHERE id=1').get()?.last_ms;
        if (!Number.isSafeInteger(last) || now < last) unavailable();
        db.prepare('UPDATE soty_nonce_meta SET last_ms=? WHERE id=1').run(now);
        db.prepare('DELETE FROM soty_nonce_uses WHERE expires_at<=?').run(now);
        if (expiresAt <= now) { db.exec('COMMIT'); return false; }
        if (db.prepare('SELECT 1 FROM soty_nonce_uses WHERE id=?').get(id)) { db.exec('COMMIT'); return false; }
        if (db.prepare('SELECT count(*) AS n FROM soty_nonce_uses').get().n >= maxItems) unavailable();
        db.prepare('INSERT INTO soty_nonce_uses VALUES(?,?)').run(id, expiresAt);
        db.exec('COMMIT'); return true;
      } catch { try { if (db.isTransaction) db.exec('ROLLBACK'); } catch {} unavailable(); }
    },
    close() { if (!closed) { closed = true; db.close(); } },
  });
}
