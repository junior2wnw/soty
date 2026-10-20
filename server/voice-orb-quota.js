import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { canonical } from '../modules/personal-agent/provider/validation.mjs';
import { voiceOrbError } from './voice-orb-transport.js';

const hash = value => createHash('sha256').update(canonical(value)).digest('hex');
const need = value => { if (!value) throw voiceOrbError('voice_orb_denied'); };
/** A finite session-start QUOTA, never money, a hard-dollar ceiling or verified
 * usage settlement. Every attempted start stays reserved after unknown usage.
 * Operator-installed profile authorization is separate from a user's per-turn
 * permission (including Notes); neither can be minted by model tool arguments. */
export function createVoiceOrbQuota({ databasePath, profiles }) {
  need(typeof databasePath === 'string' && profiles instanceof Map);
  if (databasePath !== ':memory:') {
    need(path.isAbsolute(databasePath)); fs.mkdirSync(path.dirname(databasePath), { recursive: true, mode: 0o700 });
    const fd = fs.openSync(databasePath, fs.constants.O_CREAT | fs.constants.O_RDWR | (fs.constants.O_NONBLOCK || 0) | (fs.constants.O_NOFOLLOW || 0), 0o600);
    try { const stat = fs.fstatSync(fd); need(stat.isFile());
      if (process.platform !== 'win32') { need(stat.uid === process.getuid()); fs.fchmodSync(fd, 0o600); }
    } finally { fs.closeSync(fd); }
  }
  const db = new DatabaseSync(databasePath); let closed = false;
  try { db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=3000;
    CREATE TABLE IF NOT EXISTS voice_orb_starts (
      request_key TEXT PRIMARY KEY, grant_key TEXT NOT NULL, intent_digest TEXT NOT NULL,
      attempted INTEGER NOT NULL DEFAULT 0 CHECK(attempted IN(0,1)), created_at INTEGER NOT NULL,
      outcome_json TEXT, outcome_digest TEXT
    ); CREATE INDEX IF NOT EXISTS voice_orb_grant ON voice_orb_starts(grant_key);`); }
  catch (error) { db.close(); throw error; }
  const findProfile = actor => profiles.get(canonical({ accountId: actor.accountId, deviceId: actor.deviceId }));
  const grantKey = profile => hash({ accountId: profile.accountId, deviceId: profile.deviceId, tenantId: profile.tenantId, authorizationId: profile.authorizationId });
  const count = key => Number(db.prepare('SELECT COUNT(*) AS used FROM voice_orb_starts WHERE grant_key=?').get(key).used);
  return Object.freeze({
    profileFor(actor) { need(!closed); const profile = findProfile(actor); need(profile && Date.now() < profile.expiresAt); return profile; },
    statusFor(actor) {
      need(!closed); const profile = findProfile(actor);
      if (!profile || Date.now() >= profile.expiresAt) return { allowed: false, remainingStarts: 0, usageFinalized: false };
      const reservedStarts = count(grantKey(profile));
      return { allowed: reservedStarts < profile.maxStarts, authorizedStarts: profile.maxStarts, reservedStarts,
        remainingStarts: Math.max(0, profile.maxStarts - reservedStarts), usageFinalized: false };
    },
    async admit({ actor, requestId, intentDigest, maxDurationMs, signal }) {
      need(!closed && !signal.aborted); const profile = findProfile(actor);
      need(profile && Date.now() + maxDurationMs <= profile.expiresAt);
      const requestKey = hash({ projectId: 'soty', accountId: actor.accountId, deviceId: actor.deviceId, requestId });
      const key = grantKey(profile);
      let createdHere = false, claimedHere = false;
      db.exec('BEGIN IMMEDIATE');
      try {
        const prior = db.prepare('SELECT * FROM voice_orb_starts WHERE request_key=?').get(requestKey);
        if (prior) need(prior.intent_digest === intentDigest && prior.grant_key === key);
        else {
          need(count(key) < profile.maxStarts);
          db.prepare('INSERT INTO voice_orb_starts(request_key,grant_key,intent_digest,created_at) VALUES(?,?,?,?)')
            .run(requestKey, key, intentDigest, Date.now()); createdHere = true;
        }
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      return Object.freeze({
        assertCurrent: () => !closed && !signal.aborted && Date.now() < profile.expiresAt,
        async claimTransport() {
          need(!closed && !signal.aborted && Date.now() < profile.expiresAt);
          const won = Number(db.prepare('UPDATE voice_orb_starts SET attempted=1 WHERE request_key=? AND attempted=0 AND outcome_json IS NULL').run(requestKey).changes) === 1;
          if (won) claimedHere = true; return won;
        },
        async recordOutcome(proof) {
          need(!closed);
          const safe = { transportAttempted: proof.transportAttempted === true, providerCallId: proof.providerCallId ?? null,
            hangupAccepted: proof.hangupAccepted === true, terminationVerified: false, accountingFinalized: false,
            responseUsage: proof.responseUsage, reason: proof.reason };
          const value = canonical(safe), digest = hash(safe), previous = db.prepare('SELECT attempted,outcome_digest FROM voice_orb_starts WHERE request_key=?').get(requestKey);
          // A replay which lost claimTransport owns no original outcome. It
          // cannot turn a crashed attempted request into no-attempt evidence.
          need(previous && (claimedHere || createdHere && previous.attempted === 0)
            && safe.transportAttempted === (previous.attempted === 1)
            && (previous.outcome_digest === null || previous.outcome_digest === digest));
          db.prepare('UPDATE voice_orb_starts SET outcome_json=?,outcome_digest=? WHERE request_key=? AND outcome_json IS NULL').run(value, digest, requestKey);
          // No quota credit or money is returned on an ACK, timeout, abort or
          // incomplete usage. Re-authorizing more starts is an operator action.
        },
      });
    },
    close() { if (!closed) { closed = true; db.close(); } },
  });
}
