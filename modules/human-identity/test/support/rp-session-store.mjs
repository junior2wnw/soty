import { DatabaseSync } from 'node:sqlite';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { canonicalOAuthJson, snapshotOAuthJson } from '../../../capabilities/server/oauth-profile.mjs';
const hash = value => createHash('sha256').update(value).digest('hex');
const require = value => { if (!value) throw Object.assign(new Error('rp_session_conflict'), { code: 'rp_session_conflict' }); };
export function createPrivateRpSessionStore({ databasePath, key, clientId, now = Date.now }) {
  require(key instanceof Uint8Array && key.byteLength === 32 && typeof clientId === 'string');
  const secret = Buffer.from(key), db = new DatabaseSync(databasePath);
  db.exec(`PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA busy_timeout=100;
    CREATE TABLE IF NOT EXISTS rp_sessions(id_hash TEXT PRIMARY KEY,revision INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('active','refreshing','blocked')),
      lease_hash TEXT,payload_cipher BLOB NOT NULL,created_at INTEGER NOT NULL,expires_at INTEGER NOT NULL) STRICT;`);
  const pageSize = Number(db.prepare('PRAGMA page_size').get().page_size); db.exec(`PRAGMA max_page_count=${Math.floor(16 * 1024 * 1024 / pageSize)}`);
  let closed = false;
  const get = sid => db.prepare('SELECT * FROM rp_sessions WHERE id_hash=?').get(hash(sid));
  const aad = sid => Buffer.from(canonicalOAuthJson([clientId, hash(sid), 'private-rp-session.v1']));
  const seal = (sid, payload) => { const text = canonicalOAuthJson(snapshotOAuthJson(payload)), nonce = randomBytes(12), cipher = createCipheriv('aes-256-gcm', secret, nonce);
    cipher.setAAD(aad(sid)); const encrypted = Buffer.concat([cipher.update(text), cipher.final()]);return Buffer.concat([nonce, cipher.getAuthTag(), encrypted]); };
  const open = (sid, bytes) => { const payload = Buffer.from(bytes), decipher = createDecipheriv('aes-256-gcm', secret, payload.subarray(0,12));
    decipher.setAAD(aad(sid)); decipher.setAuthTag(payload.subarray(12,28)); return JSON.parse(Buffer.concat([decipher.update(payload.subarray(28)), decipher.final()]).toString()); };
  function atomic(callback) { require(!closed && !db.isTransaction); db.exec('BEGIN IMMEDIATE'); try { const value = callback(); db.exec('COMMIT'); return value; } catch(error) { db.exec('ROLLBACK'); throw error; } }
  return Object.freeze({
    create(sid, payload) { return atomic(() => {db.prepare('DELETE FROM rp_sessions WHERE rowid IN(SELECT rowid FROM rp_sessions WHERE expires_at<=? LIMIT 128)').run(now());
      require(db.prepare('SELECT count(*) AS n FROM rp_sessions').get().n < 128);
      db.prepare('INSERT INTO rp_sessions VALUES(?,1,\'active\',NULL,?,?,?)').run(hash(sid), seal(sid,payload), now(), now()+86400000); }); },
    read(sid) { require(!closed); const row = sid && get(sid); if (!row || row.expires_at <= now()) return null;
      return { revision: row.revision, state: row.state, payload: open(sid,row.payload_cipher) }; },
    replaceActive(sid, revision, payload) { return atomic(() => { const changed = db.prepare("UPDATE rp_sessions SET payload_cipher=?,revision=revision+1 WHERE id_hash=? AND revision=? AND state='active' AND expires_at>?")
      .run(seal(sid,payload),hash(sid),revision,now());require(changed.changes===1); }); },
    beginRefresh(sid, revision) { return atomic(() => { const lease = randomBytes(24).toString('base64url');
      const changed = db.prepare("UPDATE rp_sessions SET state='refreshing',lease_hash=?,revision=revision+1 WHERE id_hash=? AND revision=? AND state='active' AND expires_at>?")
        .run(hash(lease), hash(sid), revision, now()); require(changed.changes===1); return { lease, revision: revision+1 }; }); },
    completeRefresh(sid, lease, revision, payload) { return atomic(() => {
      const changed = db.prepare("UPDATE rp_sessions SET state='active',lease_hash=NULL,payload_cipher=?,revision=revision+1 WHERE id_hash=? AND state='refreshing' AND lease_hash=? AND revision=? AND expires_at>?")
        .run(seal(sid,payload), hash(sid), hash(lease), revision, now()); require(changed.changes===1); }); },
    failRefresh(sid, lease, revision) { return atomic(() => { const changed = db.prepare("UPDATE rp_sessions SET state='blocked',lease_hash=NULL,revision=revision+1 WHERE id_hash=? AND state='refreshing' AND lease_hash=? AND revision=?")
      .run(hash(sid),hash(lease),revision); require(changed.changes===1); }); },
    close() { if(closed)return;closed=true;db.close();secret.fill(0); },
  });
}
