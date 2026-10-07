// Synthetic Source storage contract. This is not a production Planner/PG adapter.
import { DatabaseSync } from 'node:sqlite';
import { createHash, createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { SourceRpError, sourceRpCipherBinding } from '../../server/index.mjs';

export const digest = value => createHash('sha256').update(value).digest('hex');
export function sqliteSource(path, { key = randomBytes(32), keyId = 'fixture-key', clock = Date.now } = {}) {
  const db = new DatabaseSync(path); db.exec(`PRAGMA journal_mode=WAL;PRAGMA busy_timeout=5000;
    CREATE TABLE IF NOT EXISTS source_authority(hash TEXT PRIMARY KEY,profile TEXT NOT NULL,binding TEXT NOT NULL,issuer TEXT NOT NULL,
      subject TEXT NOT NULL,deadline INTEGER NOT NULL,created INTEGER NOT NULL,generation INTEGER NOT NULL,active INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS rp_heads(hash TEXT PRIMARY KEY,document TEXT NOT NULL);`);
  let lostCommit = false;
  const read = hash => { const row = db.prepare('SELECT document FROM rp_heads WHERE hash=?').get(hash); return row ? JSON.parse(row.document) : null; };
  const capture = marker => {
    const row = db.prepare('SELECT * FROM source_authority WHERE hash=?').get(marker.sessionIdHash);
    if (!row || !row.active || row.profile !== marker.profileDigest || row.binding !== marker.bindingDigest || row.issuer !== marker.issuer
      || row.subject !== marker.subject || row.deadline !== marker.sessionExpiresAt || row.created !== marker.createdAt || row.deadline <= clock())
      throw new SourceRpError('authentication_required', 401);
    return { generation: row.generation };
  };
  const authority = (marker, expected) => {
    if (capture(marker).generation !== expected.generation) throw new SourceRpError('authentication_required', 401);
  };
  const transaction = callback => {
    db.exec('BEGIN IMMEDIATE'); try { const result = callback(); db.exec('COMMIT'); return result; }
    catch (error) { db.exec('ROLLBACK'); throw error; }
  };
  const write = head => db.prepare('INSERT OR REPLACE INTO rp_heads(hash,document) VALUES(?,?)').run(head.sessionIdHash, JSON.stringify(head));
  async function encrypt(model, binding, value) {
    const iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, iv);
    cipher.setAAD(Buffer.from(JSON.stringify([model, binding, keyId])));
    return Buffer.concat([iv, cipher.update(JSON.stringify(value)), cipher.final(), cipher.getAuthTag()]).toString('base64url');
  }
  async function decrypt(model, binding, value, expectedKey) {
    if (expectedKey !== keyId) throw new SourceRpError('source_rp_storage_key_unavailable', 503);
    const bytes = Buffer.from(value, 'base64url'), cipher = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
    cipher.setAAD(Buffer.from(JSON.stringify([model, binding, keyId]))); cipher.setAuthTag(bytes.subarray(-16));
    return JSON.parse(Buffer.concat([cipher.update(bytes.subarray(12, -16)), cipher.final()]).toString());
  }
  const storagePort = {
    async read(hash) { return read(hash); },
    async captureSourceAuthority(marker) { return capture(marker); },
    async assertSourceAuthority(marker, snapshot) { authority(marker, snapshot); },
    async claim({ marker, authority: snapshot, expected, claimId, claimedAt }) {
      return transaction(() => { authority(marker, snapshot); const head = read(marker.sessionIdHash);
        if (!head || JSON.stringify(head) !== JSON.stringify(expected) || head.state !== 'idle') return false;
        write({ ...head, state: 'refreshing', claimId, claimedAt, updatedAt: claimedAt }); return true; });
    },
    async finish({ marker, authority: snapshot, expected, claimId, proofCipher, accessExpiresAt, updatedAt }) {
      const result = transaction(() => { authority(marker, snapshot); const head = read(marker.sessionIdHash);
        if (!head || head.revision !== expected.revision || head.state !== 'refreshing' || head.claimId !== claimId
          || head.proofCipher !== expected.proofCipher || head.bindingDigest !== expected.bindingDigest) return false;
        write({ ...head, revision: head.revision + 1, state: 'idle', claimId: null, claimedAt: null,
          lastAttemptId: claimId, proofCipher, accessExpiresAt, updatedAt }); return true; });
      if (lostCommit) { lostCommit = false; throw new SourceRpError('fixture_commit_ack_lost', 503); } return result;
    },
    async block({ expected, claimId, state, updatedAt }) {
      transaction(() => { const head = read(expected.sessionIdHash);
        if (head?.revision === expected.revision && head.state === 'refreshing' && head.claimId === claimId)
          write({ ...head, state, updatedAt }); });
    },
    async compactExpired({ now, limit }) {
      const rows = db.prepare('SELECT hash FROM source_authority WHERE deadline<=? ORDER BY deadline LIMIT ?').all(now, limit);
      transaction(() => rows.forEach(row => { db.prepare('DELETE FROM rp_heads WHERE hash=?').run(row.hash);
        db.prepare('DELETE FROM source_authority WHERE hash=?').run(row.hash); }));
    },
  };
  return { db, storagePort, encrypt, decrypt, key, keyId,
    async seed(marker, proof, accessExpiresAt) {
      const proofCipher = await encrypt('SourceRpRenewal', sourceRpCipherBinding(marker, 0), proof);
      transaction(() => {
        db.prepare('INSERT INTO source_authority VALUES(?,?,?,?,?,?,?,1,1)').run(marker.sessionIdHash, marker.profileDigest,
          marker.bindingDigest, marker.issuer, marker.subject, marker.sessionExpiresAt, marker.createdAt);
        write({ sessionIdHash: marker.sessionIdHash, profileDigest: marker.profileDigest, bindingDigest: marker.bindingDigest,
          sessionExpiresAt: marker.sessionExpiresAt, accessExpiresAt, revision: 0, state: 'idle', proofCipher, keyId,
          claimId: null, claimedAt: null, lastAttemptId: null, createdAt: marker.createdAt, updatedAt: marker.createdAt });
      });
    },
    revoke(hash) { db.prepare('UPDATE source_authority SET active=0,generation=generation+1 WHERE hash=?').run(hash); },
    switchBinding(hash, binding) { db.prepare('UPDATE source_authority SET binding=?,generation=generation+1 WHERE hash=?').run(binding, hash); },
    head: read, overwrite: write, loseCommitAck() { lostCommit = true; }, close() { db.close(); },
  };
}
