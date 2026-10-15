import { DatabaseSync } from 'node:sqlite';
import { createCipheriv, createDecipheriv, randomBytes, createHash } from 'node:crypto';
import { mkdirSync, lstatSync, chmodSync } from 'node:fs';
import { dirname, isAbsolute } from 'node:path';
import { AccessError, canonicalJson } from '../modules/capabilities/server/validation.mjs';

const need = value => { if (!value) throw new AccessError('agent_source_storage_invalid'); };
export const SOURCE_CREDENTIAL_LIMITS = Object.freeze({ rows: 8192, cipherBytes: 32 * 1024 * 1024, databaseBytes: 64 * 1024 * 1024 });
const quota = value => { if (!value) throw new AccessError('agent_source_storage_full'); };
const hash = value => createHash('sha256').update(canonicalJson(value)).digest('hex');
// Dedicated namespace: OAuth artifacts and personal memory are not credential vaults.
// Row identity is authenticated, including installation device and application.
export function createAgentSourceCredentialStore({ databasePath, key: supplied, keyId }) {
  need(supplied instanceof Uint8Array && supplied.length === 32 && typeof keyId === 'string' && /^[\w.-]{1,64}$/u.test(keyId));
  need(databasePath === ':memory:' || typeof databasePath === 'string' && isAbsolute(databasePath));
  if (databasePath !== ':memory:') {
    mkdirSync(dirname(databasePath), { recursive: true, mode: 0o700 });
    const directory = lstatSync(dirname(databasePath)); need(directory.isDirectory() && !directory.isSymbolicLink());
    if (process.platform !== 'win32') need((directory.mode & 0o077) === 0 && directory.uid === process.getuid());
    try { const stat = lstatSync(databasePath); need(stat.isFile() && !stat.isSymbolicLink()); quota(stat.size <= SOURCE_CREDENTIAL_LIMITS.databaseBytes); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  const key = Buffer.from(supplied), db = new DatabaseSync(databasePath); let closed = false;
  try {
    const pages = Math.floor(SOURCE_CREDENTIAL_LIMITS.databaseBytes / db.prepare('PRAGMA page_size').get().page_size);
    quota(db.prepare('PRAGMA page_count').get().page_count <= pages);
    quota(db.prepare(`PRAGMA max_page_count=${pages}`).get().max_page_count === pages);
    db.exec('PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;');
    const rows = db.prepare("SELECT name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").all();
    if (!rows.length) db.exec('BEGIN IMMEDIATE; CREATE TABLE agent_source_credentials(id TEXT PRIMARY KEY, cipher BLOB NOT NULL) STRICT; PRAGMA user_version=1; COMMIT;');
    need(db.prepare('PRAGMA user_version').get().user_version === 1);
    const schema = db.prepare("SELECT name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").all();
    need(schema.length === 1 && schema[0].name === 'agent_source_credentials'
      && schema[0].sql === 'CREATE TABLE agent_source_credentials(id TEXT PRIMARY KEY, cipher BLOB NOT NULL) STRICT');
    const usage = db.prepare('SELECT count(*) AS rows, coalesce(sum(length(cipher)),0) AS bytes FROM agent_source_credentials').get();
    quota(usage.rows <= SOURCE_CREDENTIAL_LIMITS.rows && usage.bytes <= SOURCE_CREDENTIAL_LIMITS.cipherBytes);
    if (databasePath !== ':memory:') chmodSync(databasePath, 0o600);
  } catch (error) { db.close(); key.fill(0); throw error; }
  function identity(actor, appId) {
    need(!closed && typeof actor?.accountId === 'string' && typeof actor?.deviceId === 'string' && typeof appId === 'string');
    return hash(['soty.agent-source-credentials.v1', actor.accountId, actor.deviceId, appId]);
  }
  const aad = id => Buffer.from(canonicalJson(['soty.agent-source-credentials.v1', keyId, id]));
  function writeValue(actor, appId, value) {
    const id = identity(actor, appId), plain = Buffer.from(canonicalJson(value)); need(plain.length <= 65000);
    try {
      const old = db.prepare('SELECT length(cipher) AS bytes FROM agent_source_credentials WHERE id=?').get(id);
      const usage = db.prepare('SELECT count(*) AS rows, coalesce(sum(length(cipher)),0) AS bytes FROM agent_source_credentials').get();
      quota(usage.rows + (old ? 0 : 1) <= SOURCE_CREDENTIAL_LIMITS.rows
        && usage.bytes - (old?.bytes ?? 0) + plain.length + 28 <= SOURCE_CREDENTIAL_LIMITS.cipherBytes);
      const nonce = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, nonce); cipher.setAAD(aad(id));
      const data = Buffer.concat([cipher.update(plain), cipher.final()]);
      db.prepare('INSERT INTO agent_source_credentials VALUES(?,?) ON CONFLICT(id) DO UPDATE SET cipher=excluded.cipher')
        .run(id, Buffer.concat([nonce, cipher.getAuthTag(), data]));
    } finally { plain.fill(0); }
  }
  return Object.freeze({
    read(actor, appId) {
      const id = identity(actor, appId), row = db.prepare('SELECT cipher FROM agent_source_credentials WHERE id=?').get(id);
      if (!row) return null;
      try {
        const bytes = Buffer.from(row.cipher); need(bytes.length >= 29 && bytes.length <= 65536);
        const decipher = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
        decipher.setAAD(aad(id)); decipher.setAuthTag(bytes.subarray(12, 28));
        const plain = Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]);
        try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(plain)); } finally { plain.fill(0); }
      } catch { throw new AccessError('agent_source_storage_invalid'); }
    },
    write(actor, appId, value) {
      need(!closed && !db.isTransaction); db.exec('BEGIN IMMEDIATE');
      try { writeValue(actor, appId, value); db.exec('COMMIT'); }
      catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
    },
    writePair(actor, first, second) {
      need(!closed && !db.isTransaction); db.exec('BEGIN IMMEDIATE');
      try { writeValue(actor, first.id, first.value); writeValue(actor, second.id, second.value); db.exec('COMMIT'); }
      catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
    },
    close() { if (!closed) { closed = true; db.close(); key.fill(0); } },
  });
}
