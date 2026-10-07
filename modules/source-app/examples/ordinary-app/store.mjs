import { DatabaseSync } from 'node:sqlite';
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { existsSync } from 'node:fs';
import { ORDINARY_SCHEMA, ORDINARY_SCHEMA_2, ORDINARY_META_2, ORDINARY_LOGIN_PROOF_DDL } from './schema.mjs';
import { verifyOrdinaryReader1 } from './reader.mjs';
import { verifyOrdinaryReader2 } from './reader2.mjs';
import { verifyOrdinaryReader3 } from './reader3.mjs';
import { ORDINARY_SCHEMA_3, ORDINARY_META_3, ORDINARY_JOB_DDL } from './job-schema.mjs';
import { check, fields, digest, jsonCopy, nonce, opaque, syncResult } from '../../server/wire.mjs';

/** The example has one actual Native SQL authority. Only host configuration
 * creates it; no Root/body owner flag or RP shadow creates Native permissions. */
export function createOrdinaryAppStore(options) {
  const value = fields(options, ['databasePath', 'realmId', 'key', 'keyId'], ['initialize', 'clock', 'format', 'allowLoginProofMigration','allowFeedbackJobsMigration']);
  check(typeof value.databasePath === 'string' && typeof value.realmId === 'string' && /^[a-z][a-z0-9.-]{0,63}$/u.test(value.realmId)
    && Buffer.isBuffer(value.key) && value.key.length === 32 && /^[a-zA-Z0-9_.-]{1,64}$/u.test(value.keyId));
  check(existsSync(value.databasePath) || value.initialize === true, 'ordinary_source_storage_not_ready', 503);
  const db = new DatabaseSync(value.databasePath), clock = value.clock ?? Date.now, key = Buffer.from(value.key);
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;');
  const objects = db.prepare("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").all();
  const requestedFormat = value.format ?? 1; check([1,2,3].includes(requestedFormat) && (value.allowLoginProofMigration === undefined || typeof value.allowLoginProofMigration === 'boolean')
    &&(value.allowFeedbackJobsMigration===undefined||typeof value.allowFeedbackJobsMigration==='boolean'));
  if (objects.length === 0) {
    check(value.initialize === true, 'ordinary_source_storage_not_ready', 503);
    db.exec('BEGIN IMMEDIATE'); try { db.exec(requestedFormat===3?ORDINARY_SCHEMA_3:requestedFormat === 2 ? ORDINARY_SCHEMA_2 : ORDINARY_SCHEMA); db.prepare('INSERT INTO native_meta VALUES(?,?)').run(requestedFormat, value.realmId); db.exec('COMMIT'); }
    catch (error) { db.exec('ROLLBACK'); db.close(); throw error; }
  }
  let reader;
  try {
    reader = verifyOrdinaryReader3(db, value.realmId);
    if (requestedFormat === 2 && reader.format === 1) {
      check(value.allowLoginProofMigration === true, 'ordinary_source_migration_required', 503);
      verifyOrdinaryReader1(db, value.realmId); db.exec('BEGIN IMMEDIATE');
      try {
        // native_meta has no inbound FK. Preserve every other Native/BFF row
        // verbatim; the only replaced row is this explicitly versioned marker.
        db.exec('DROP TABLE native_meta; ' + ORDINARY_META_2 + ';'); db.prepare('INSERT INTO native_meta VALUES(2,?)').run(value.realmId);
        db.exec(ORDINARY_LOGIN_PROOF_DDL); verifyOrdinaryReader2(db, value.realmId); db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      reader = verifyOrdinaryReader2(db, value.realmId);
    }
    if(requestedFormat===3&&reader.format<3){
      check(reader.format===2&&value.allowFeedbackJobsMigration===true,'ordinary_source_jobs_migration_required',503);
      verifyOrdinaryReader2(db,value.realmId);db.exec('BEGIN IMMEDIATE');
      try{db.exec('DROP TABLE native_meta; '+ORDINARY_META_3+';');db.prepare('INSERT INTO native_meta VALUES(3,?)').run(value.realmId);
        db.exec(ORDINARY_JOB_DDL);verifyOrdinaryReader3(db,value.realmId);db.exec('COMMIT');}catch(error){db.exec('ROLLBACK');throw error;}
      reader=verifyOrdinaryReader3(db,value.realmId);
    }
  } catch (error) { db.close(); throw error; }
  db.exec('PRAGMA journal_mode=WAL;');
  let closed = false, transaction = false;
  function current() { check(!closed, 'ordinary_source_closed', 503); }
  function tx(action) {
    current(); check(!transaction, 'ordinary_source_transaction_invalid', 503); transaction = true; db.exec('BEGIN IMMEDIATE');
    try { const result = syncResult(action()); db.exec('COMMIT'); return result; }
    catch (error) { db.exec('ROLLBACK'); throw error; } finally { transaction = false; }
  }
  const aad = (model, id, revision) => JSON.stringify(['soty.ordinary-source.v1', value.realmId, model, id, revision, value.keyId]);
  function encrypt(model, id, revision, input) {
    current(); const iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, iv); cipher.setAAD(Buffer.from(aad(model, id, revision)));
    const bytes = Buffer.concat([cipher.update(JSON.stringify(jsonCopy(input)), 'utf8'), cipher.final()]);
    return Buffer.concat([iv, cipher.getAuthTag(), bytes]).toString('base64');
  }
  function decrypt(model, id, revision, cipher, keyId) {
    current(); check(keyId === value.keyId, 'ordinary_source_storage_key_unavailable', 503);
    try { const bytes = Buffer.from(cipher, 'base64'); check(bytes.length > 28);
      const decipher = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12)); decipher.setAAD(Buffer.from(aad(model, id, revision))); decipher.setAuthTag(bytes.subarray(12, 28));
      return jsonCopy(JSON.parse(Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]).toString('utf8')));
    } catch { throw Object.assign(new Error('ordinary_source_storage_corrupt'), { code: 'ordinary_source_storage_corrupt', status: 503 }); }
  }
  function interaction(row) { return row ? decrypt('Interaction', row.id_hash, row.revision, row.cipher, row.key_id) : null; }
  function session(row) { if (!row) return null; const payload = decrypt('Session', row.id_hash, 0, row.cipher, row.key_id);
    return { ...payload, active: row.active === 1, expiresAt: row.expires_at }; }
  const storage = Object.freeze({
    async consumeNonce(input, expiresAt) {
      return tx(() => { db.prepare('DELETE FROM source_nonces WHERE expires_at<=?').run(clock());
        if (db.prepare('SELECT count(*) AS n FROM source_nonces').get().n >= 4096) return false;
        if (db.prepare('SELECT nonce FROM source_nonces WHERE nonce=?').get(input)) return false;
        db.prepare('INSERT INTO source_nonces VALUES(?,?)').run(input, expiresAt); return true; });
    },
    async createInteraction(record) {
      const cipher = encrypt('Interaction', record.idHash, record.revision, record);
      tx(() => { check(db.prepare('SELECT count(*) AS n FROM source_interactions WHERE expires_at>?').get(clock()).n < 256, 'ordinary_source_capacity', 503);
        db.prepare('INSERT INTO source_interactions VALUES(?,?,?,?,?,?)').run(record.idHash, record.revision, record.phase, record.expiresAt, cipher, value.keyId); });
    },
    async getInteraction(hash) { current(); return interaction(db.prepare('SELECT * FROM source_interactions WHERE id_hash=?').get(hash)); },
    async claimInteraction(hash, revision, protocolIntent, finalRememberNative) {
      const prior = interaction(db.prepare('SELECT * FROM source_interactions WHERE id_hash=?').get(hash)); if (!prior) return false;
      return tx(() => {
        const current = db.prepare('SELECT phase,revision,expires_at FROM source_interactions WHERE id_hash=?').get(hash);
        if (current?.phase !== 'pending' || current.revision !== revision || current.expires_at <= clock()) return false;
        const nativeLoginMarker = finalRememberNative ? syncResult(finalRememberNative()) : undefined;
        const next = { ...prior, phase: 'claimed', revision: revision + 1, protocolIntent, ...(nativeLoginMarker ? { nativeLoginMarker } : {}) };
        const cipher = encrypt('Interaction', hash, next.revision, next);
        return db.prepare("UPDATE source_interactions SET phase='claimed',revision=?,cipher=? WHERE id_hash=? AND revision=? AND phase='pending' AND expires_at>?")
          .run(next.revision, cipher, hash, revision, clock()).changes === 1;
      });
    },
    async claimCallback(hash, revision) {
      const prior = interaction(db.prepare('SELECT * FROM source_interactions WHERE id_hash=?').get(hash)); if (!prior) return false;
      const next = { ...prior, phase: 'exchanging', revision: revision + 1 }, cipher = encrypt('Interaction', hash, next.revision, next);
      return tx(() => db.prepare("UPDATE source_interactions SET phase='exchanging',revision=?,cipher=? WHERE id_hash=? AND revision=? AND phase='claimed' AND expires_at>?")
        .run(next.revision, cipher, hash, revision, clock()).changes === 1);
    },
    async completeInteraction(input, finalNativeLink) {
      const prior = interaction(db.prepare('SELECT * FROM source_interactions WHERE id_hash=?').get(input.idHash)); if (!prior) return false;
      const next = { ...prior, phase: 'completed', revision: input.revision + 1, completionToken: input.completionToken };
      const cipher = encrypt('Interaction', input.idHash, next.revision, next), sessionCipher = encrypt('Session', input.session.idHash, 0, input.session);
      return tx(() => {
        const row = db.prepare('SELECT * FROM source_interactions WHERE id_hash=?').get(input.idHash);
        if (row?.phase !== 'exchanging' || row.revision !== input.revision || row.expires_at <= clock()) return false;
        const native = finalNativeLink();
        check(native && typeof native.principalId === 'string' && typeof native.nativeSessionHash === 'string' && typeof native.resourceId === 'string', 'ordinary_source_native_link_required', 403);
        db.prepare('INSERT INTO source_sessions VALUES(?,?,?,?,?,?,?,?)').run(input.session.idHash, native.nativeSessionHash, native.principalId, native.resourceId,
          1, input.session.expiresAt, sessionCipher, value.keyId);
        db.prepare('INSERT INTO source_completions VALUES(?,?,?,0)').run(input.session.completionHash, input.idHash, input.session.idHash);
        db.prepare("UPDATE source_interactions SET phase='completed',revision=?,cipher=? WHERE id_hash=?").run(next.revision, cipher, input.idHash); return true;
      });
    },
    async readSession(hash) { current(); return session(db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(hash)); },
    async readCompletion(hash) { current(); return session(db.prepare('SELECT s.* FROM source_sessions s JOIN source_completions c ON c.session_hash=s.id_hash WHERE c.id_hash=? OR c.interaction_hash=?').get(hash, hash)); },
    async consumeCompletion(hash, sessionHash, finalNativeAssert) {
      return tx(() => { const row = db.prepare('SELECT * FROM source_completions WHERE id_hash=? AND session_hash=?').get(hash, sessionHash); if (!row) return null;
        const currentSession = session(db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(sessionHash));
        check(currentSession?.active && currentSession.expiresAt > clock(), 'authentication_required', 401); finalNativeAssert();
        // Immutable association means an unknown ACK can reconcile this exact
        // consumed locator/session under fresh original Source/Root authority.
        db.prepare('UPDATE source_completions SET consumed=1 WHERE id_hash=?').run(hash); return { token: currentSession.cookieToken }; });
    },
    async readTokenProof(currentSession) { return { accessToken: currentSession.accessToken, expiresAt: currentSession.expiresAt }; },
    async revokeSession(hash) { tx(() => { db.prepare('UPDATE source_sessions SET active=0 WHERE id_hash=?').run(hash); }); },
  });
  return Object.freeze({ db, storage, clock, realmId: value.realmId, format:reader.format, tx, inTransaction: () => transaction, encrypt, decrypt,
    /** Trusted Native provisioning only. These functions are never HTTP/RPC. */
    createResource({ id, incarnationId, title, guestEmpty = false }) { tx(() => { db.prepare('INSERT INTO native_resources VALUES(?,?,?,?,?)').run(id, incarnationId, value.realmId, title, guestEmpty ? 1 : 0); }); },
    createPrincipal(id) { tx(() => { db.prepare('INSERT INTO native_principals VALUES(?,?)').run(id, value.realmId); }); },
    grant(resourceId, principalId, role) { tx(() => { db.prepare('INSERT INTO native_memberships VALUES(?,?,?,1,1) ON CONFLICT(resource_id,principal_id) DO UPDATE SET role=excluded.role,active=1,revision=revision+1').run(resourceId, principalId, role); }); },
    createNativeSession(principalId) { const token = nonce(); tx(() => { db.prepare('INSERT INTO native_sessions VALUES(?,?,1,1,?)').run(digest(token), principalId, clock() + 86400000); }); return token; },
    revokeNativeSession(token) { check(opaque(token)); tx(() => { db.prepare('UPDATE native_sessions SET active=0,generation=generation+1 WHERE id_hash=?').run(digest(token)); }); },
    revokeMembership(resourceId, principalId) { tx(() => { db.prepare('UPDATE native_memberships SET active=0,revision=revision+1 WHERE resource_id=? AND principal_id=?').run(resourceId, principalId); }); },
    close() { if (closed) return; closed = true; key.fill(0); db.close(); },
  });
}
