import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { canonical } from '../modules/personal-agent/provider/validation.mjs';
import { voiceOrbError } from './voice-orb-transport.js';

const hash = value => createHash('sha256').update(canonical(value)).digest('hex');
const need = value => { if (!value) throw voiceOrbError('voice_orb_denied'); };
/** A finite session-start QUOTA, never money, a hard-dollar ceiling or verified
 * usage settlement. Every attempted start stays reserved after unknown usage.
 * Operator-installed profile authorization is separate from a user's per-turn
 * permission (including Notes); neither can be minted by model tool arguments. */
export function createVoiceOrbQuota({ databasePath, profiles, service }) {
  const shared = service !== undefined;
  need(typeof databasePath === 'string' && (shared ? profiles === undefined && service && typeof service === 'object' : profiles instanceof Map));
  if (shared) need(typeof service.tenantId === 'string' && typeof service.serviceId === 'string'
    && Number.isSafeInteger(service.maxStarts) && service.maxStarts >= 1 && service.maxStarts <= 1000000
    && Number.isSafeInteger(service.maxAccountStarts) && service.maxAccountStarts >= 1 && service.maxAccountStarts <= service.maxStarts
    && Number.isFinite(service.expiresAt));
  if (shared) service = Object.freeze({ ...service });
  if (databasePath !== ':memory:') {
    need(path.isAbsolute(databasePath)); fs.mkdirSync(path.dirname(databasePath), { recursive: true, mode: 0o700 });
    const fd = fs.openSync(databasePath, fs.constants.O_CREAT | fs.constants.O_RDWR | (fs.constants.O_NONBLOCK || 0) | (fs.constants.O_NOFOLLOW || 0), 0o600);
    try { const stat = fs.fstatSync(fd); need(stat.isFile());
      if (process.platform !== 'win32') { need(stat.uid === process.getuid()); fs.fchmodSync(fd, 0o600); }
    } finally { fs.closeSync(fd); }
  }
  const db = new DatabaseSync(databasePath); let closed = false;
  try {
    const deadline = performance.now() + 3000, waiter = new Int32Array(new SharedArrayBuffer(4));
    const remaining = () => Math.max(0, Math.floor(deadline - performance.now()));
    let lastBusy;
    const budget = () => {
      const left = remaining(); if (left <= 0) throw lastBusy ?? voiceOrbError('voice_orb_denied');
      db.exec(`PRAGMA busy_timeout=${left}`);
    };
    for (;;) {
      budget();
      try { db.exec('PRAGMA journal_mode=WAL'); break; }
      catch (error) {
        // SQLite may skip its busy handler for a lock which risks deadlock.
        // Only this idempotent journal setup may retry, within one budget.
        if (error?.code !== 'ERR_SQLITE_ERROR' || !Number.isSafeInteger(error.errcode)
          || error.errcode < 0 || (error.errcode & 255) !== 5) throw error;
        lastBusy = error; const left = remaining(); if (left <= 0) throw error;
        Atomics.wait(waiter, 0, 0, Math.min(25, left));
      }
    }
    budget();
    // Each schema statement runs once with the remaining common budget.
    // Its errors never enter the journal retry loop.
    db.exec('PRAGMA synchronous=FULL');
    budget();
    db.exec(`CREATE TABLE IF NOT EXISTS voice_orb_starts (
      request_key TEXT PRIMARY KEY, grant_key TEXT NOT NULL, intent_digest TEXT NOT NULL,
      attempted INTEGER NOT NULL DEFAULT 0 CHECK(attempted IN(0,1)), created_at INTEGER NOT NULL,
      outcome_json TEXT, outcome_digest TEXT
    );`);
    budget();
    db.exec('CREATE INDEX IF NOT EXISTS voice_orb_grant ON voice_orb_starts(grant_key);');
    if (remaining() <= 0) throw lastBusy ?? voiceOrbError('voice_orb_denied');
    db.exec('PRAGMA busy_timeout=3000'); // Preserve the existing later quota-operation budget.
  }
  catch (error) { db.close(); throw error; }
  // Stable service/account counters exclude devices, keys, OpenAI projects,
  // authorization IDs and dates. Routine rotation cannot reset an allowance.
  // V1 hexadecimal grant keys never enter this explicit V2 namespace.
  const prefix = shared ? 'shared.v2:' + hash({ tenantId: service.tenantId, serviceId: service.serviceId }) + ':' : null;
  const findProfile = actor => {
    if (!shared) return profiles.get(canonical({ accountId: actor.accountId, deviceId: actor.deviceId }));
    return typeof actor?.accountId === 'string' && actor.accountId.length > 0 && typeof actor.deviceId === 'string' && actor.deviceId.length > 0 ? service : undefined;
  };
  const grantKey = (profile, actor) => shared ? prefix + hash({ accountId: actor.accountId })
    : hash({ accountId: profile.accountId, deviceId: profile.deviceId, tenantId: profile.tenantId, authorizationId: profile.authorizationId });
  const count = key => Number(db.prepare('SELECT COUNT(*) AS used FROM voice_orb_starts WHERE grant_key=?').get(key).used);
  const total = () => Number(db.prepare('SELECT COUNT(*) AS used FROM voice_orb_starts WHERE grant_key>=? AND grant_key<?').get(prefix, prefix + '~').used);
  const accountLimit = profile => shared ? profile.maxAccountStarts : profile.maxStarts;
  return Object.freeze({
    profileFor(actor) { need(!closed); const profile = findProfile(actor); need(profile && Date.now() < profile.expiresAt); return profile; },
    statusFor(actor) {
      need(!closed); const profile = findProfile(actor);
      if (!profile || Date.now() >= profile.expiresAt) return { allowed: false, remainingStarts: 0, usageFinalized: false };
      const reservedStarts = count(grantKey(profile, actor)), authorizedStarts = accountLimit(profile);
      const remainingStarts = Math.max(0, Math.min(authorizedStarts - reservedStarts, shared ? profile.maxStarts - total() : Infinity));
      return { allowed: remainingStarts > 0, authorizedStarts, reservedStarts, remainingStarts, usageFinalized: false };
    },
    async admit({ actor, requestId, intentDigest, maxDurationMs, signal }) {
      need(!closed && !signal.aborted); const profile = findProfile(actor);
      need(profile && Date.now() + maxDurationMs <= profile.expiresAt);
      const requestKey = hash({ projectId: 'soty', accountId: actor.accountId, deviceId: actor.deviceId, requestId });
      const key = grantKey(profile, actor);
      let createdHere = false, claimedHere = false;
      db.exec('BEGIN IMMEDIATE');
      try {
        const prior = db.prepare('SELECT * FROM voice_orb_starts WHERE request_key=?').get(requestKey);
        if (prior) need(prior.intent_digest === intentDigest && prior.grant_key === key);
        else {
          need(count(key) < accountLimit(profile) && (!shared || total() < profile.maxStarts));
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
          // Another SQLite writer may claim transport after the read above.
          // Commit only against the current attempted state and exact outcome.
          const updated = db.prepare('UPDATE voice_orb_starts SET outcome_json=?,outcome_digest=? WHERE request_key=? AND attempted=? AND (outcome_json IS NULL OR outcome_digest=?)')
            .run(value, digest, requestKey, safe.transportAttempted ? 1 : 0, digest);
          need(Number(updated.changes) === 1);
          // No quota credit or money is returned on an ACK, timeout, abort or
          // incomplete usage. Re-authorizing more starts is an operator action.
        },
      });
    },
    close() { if (!closed) { closed = true; db.close(); } },
  });
}
