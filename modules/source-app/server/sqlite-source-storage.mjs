import { check, fields, syncResult } from './wire.mjs';

/** BFF persistence on the host's existing synchronous SQLite connection.
 * The host owns schema, transactions, encryption/AAD, clock and connection life.
 * Source tables retain the ordinary adapter's exact column order and CAS rules;
 * their Native foreign keys are owned and verified by the host schema. */
export function createSqliteSourceStorage(options) {
  const { database: db, current, transaction: tx, encrypt, decrypt, clock, keyId } = fields(options,
    ['database', 'current', 'transaction', 'encrypt', 'decrypt', 'clock', 'keyId']);
  check(db && typeof db.prepare === 'function' && [current, tx, encrypt, decrypt, clock].every(fn => typeof fn === 'function')
    && typeof keyId === 'string' && /^[a-zA-Z0-9_.-]{1,64}$/u.test(keyId));
  function interaction(row) { return row ? decrypt('Interaction', row.id_hash, row.revision, row.cipher, row.key_id) : null; }
  function session(row) { if (!row) return null; const payload = decrypt('Session', row.id_hash, 0, row.cipher, row.key_id);
    return { ...payload, active: row.active === 1, expiresAt: row.expires_at }; }
  return Object.freeze({
    async consumeNonce(input, expiresAt) {
      return tx(() => { db.prepare('DELETE FROM source_nonces WHERE expires_at<=?').run(clock());
        if (db.prepare('SELECT count(*) AS n FROM source_nonces').get().n >= 4096) return false;
        if (db.prepare('SELECT nonce FROM source_nonces WHERE nonce=?').get(input)) return false;
        db.prepare('INSERT INTO source_nonces VALUES(?,?)').run(input, expiresAt); return true; });
    },
    async createInteraction(record) {
      const cipher = encrypt('Interaction', record.idHash, record.revision, record);
      tx(() => { check(db.prepare('SELECT count(*) AS n FROM source_interactions WHERE expires_at>?').get(clock()).n < 256, 'ordinary_source_capacity', 503);
        db.prepare('INSERT INTO source_interactions VALUES(?,?,?,?,?,?)').run(record.idHash, record.revision, record.phase, record.expiresAt, cipher, keyId); });
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
          1, input.session.expiresAt, sessionCipher, keyId);
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
    async readTokenProof(currentSession) {
      current(); const row = db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(currentSession.idHash);
      check(row?.active === 1 && row.expires_at > clock(), 'authentication_required', 401);
      const payload = decrypt('Session', row.id_hash, 0, row.cipher, row.key_id);
      // Token expiry belongs to the sealed login result; a shorter mutable
      // Source deadline remains independently enforced by readSession/BFF.
      // Older sealed records retain their original conservative token bound.
      return { accessToken: payload.accessToken, expiresAt: payload.accessExpiresAt ?? payload.expiresAt };
    },
    async revokeSession(hash) { tx(() => { db.prepare('UPDATE source_sessions SET active=0 WHERE id_hash=?').run(hash); }); },
  });
}
