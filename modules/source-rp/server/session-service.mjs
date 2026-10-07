import { randomBytes } from 'node:crypto';
import { SourceRpError, check } from './protocol.mjs';

export const SOURCE_RP_LIMITS = Object.freeze({ seconds: 86400, heads: 4096, perAccount: 8, inFlight: 16,
  gcBatch: 128, rotations: 512, earlyRefreshMs: 30000, minimumAccessRemainingMaxMs: 240000, waitMs: 6000, staleClaimMs: 20000 });
const keys = Object.freeze(['sessionIdHash', 'profileDigest', 'bindingDigest', 'sessionExpiresAt', 'accessExpiresAt', 'revision',
  'state', 'proofCipher', 'keyId', 'claimId', 'claimedAt', 'lastAttemptId', 'createdAt', 'updatedAt']);
const markerKeys = Object.freeze(['sessionIdHash', 'profileDigest', 'bindingDigest', 'issuer', 'subject', 'sessionExpiresAt', 'createdAt']);
const hash = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/u.test(value);
const stamp = value => Number.isSafeInteger(value) && value > 0;
const exact = (value, fields) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === fields.length && fields.every(key => Object.hasOwn(value, key));
const equalHead = (a, b) => keys.every(key => a[key] === b[key]);
function hostMinimum(input) {
  if (input === undefined) return 0;
  check(input && typeof input === 'object' && !Array.isArray(input)
    && [Object.prototype, null].includes(Object.getPrototypeOf(input)), 'source_rp_options_invalid', 400);
  const fields = Object.getOwnPropertyDescriptors(input);
  check(Reflect.ownKeys(fields).every(key => key === 'minimumAccessRemainingMs'
    && fields[key].enumerable && Object.hasOwn(fields[key], 'value')), 'source_rp_options_invalid', 400);
  const value = fields.minimumAccessRemainingMs ? fields.minimumAccessRemainingMs.value : 0;
  check(Number.isSafeInteger(value) && value >= 0 && value <= SOURCE_RP_LIMITS.minimumAccessRemainingMaxMs,
    'source_rp_options_invalid', 400);
  return value;
}
function closedProof(value) {
  check(exact(value, ['accessToken', 'refreshToken', 'nonce'])
    && [value.accessToken, value.refreshToken].every(token => typeof token === 'string' && token.length >= 16
      && token.length <= 4096 && !/[\u0000-\u0020\u007f]/u.test(token))
    && typeof value.nonce === 'string' && /^[A-Za-z0-9_-]{16,128}$/u.test(value.nonce), 'source_rp_storage_corrupt', 503);
  return value;
}
/** Stable AAD. The Source encryptor must authenticate this exact value and keyId. */
export function sourceRpCipherBinding(marker, revision) {
  return JSON.stringify(['soty.source-rp-session.v1', marker.sessionIdHash, marker.profileDigest, marker.bindingDigest, revision]);
}

/** Durable source-owned storage ports. Network and token decryption are never inside a DB transaction. */
export function createSourceRpSessionService(options) {
  const { storagePort: storage, protocol, profileDigest, keyId, encrypt, decrypt } = options;
  const now = options.clock ?? Date.now, inFlight = new Map();
  check(hash(profileDigest) && id(keyId) && typeof now === 'function' && typeof encrypt === 'function' && typeof decrypt === 'function'
    && ['read', 'captureSourceAuthority', 'assertSourceAuthority', 'claim', 'finish', 'block'].every(name => typeof storage?.[name] === 'function')
    && ['ready', 'renew', 'currentSubject'].every(name => typeof protocol?.[name] === 'function'), 'source_rp_configuration_invalid', 503);

  function validateMarker(input) {
    check(exact(input, markerKeys) && hash(input.sessionIdHash) && input.profileDigest === profileDigest && hash(input.bindingDigest)
      && typeof input.issuer === 'string' && input.issuer.length <= 1024 && typeof input.subject === 'string'
      && input.subject.length > 0 && input.subject.length <= 128 && stamp(input.createdAt) && stamp(input.sessionExpiresAt)
      && input.sessionExpiresAt > input.createdAt && input.sessionExpiresAt - input.createdAt <= SOURCE_RP_LIMITS.seconds * 1000,
    'authentication_required', 401);
    return Object.freeze({ ...input });
  }
  function validate(marker, head) {
    check(head && head.sessionIdHash === marker.sessionIdHash && head.profileDigest === marker.profileDigest
      && head.bindingDigest === marker.bindingDigest && head.sessionExpiresAt === marker.sessionExpiresAt
      && head.createdAt === marker.createdAt, 'authentication_required', 401);
    check(head.keyId === keyId, 'source_rp_storage_key_unavailable', 503);
    check(exact(head, keys) && stamp(head.accessExpiresAt) && head.accessExpiresAt <= head.sessionExpiresAt
      && Number.isSafeInteger(head.revision) && head.revision >= 0 && head.revision <= SOURCE_RP_LIMITS.rotations
      && ['idle', 'refreshing', 'unknown', 'revoked'].includes(head.state)
      && typeof head.proofCipher === 'string' && head.proofCipher.length >= 16 && head.proofCipher.length <= 32768
      && stamp(head.updatedAt) && head.updatedAt >= head.createdAt
      && (head.claimId === null || hash(head.claimId)) && (head.lastAttemptId === null || hash(head.lastAttemptId))
      && (head.claimedAt === null || stamp(head.claimedAt))
      && (head.state !== 'refreshing' || hash(head.claimId) && stamp(head.claimedAt)), 'source_rp_storage_corrupt', 503);
    check(head.sessionExpiresAt > now(), 'account_session_expired', 401);
    check(head.state !== 'revoked', 'authentication_required', 401);
    return Object.freeze({ ...head });
  }
  async function tokenProof(marker, head) {
    try { return closedProof(await decrypt('SourceRpRenewal', sourceRpCipherBinding(marker, head.revision), head.proofCipher, head.keyId)); }
    catch { throw new SourceRpError('source_rp_storage_corrupt', 503); }
  }
  async function read(marker) { return validate(marker, await storage.read(marker.sessionIdHash)); }
  async function authority(marker, captured) { await storage.assertSourceAuthority(marker, captured); }
  async function closeClaim(head, claimId, state) {
    await storage.block({ expected: head, claimId, state, updatedAt: now() });
  }
  async function wait(marker, captured) {
    const end = performance.now() + SOURCE_RP_LIMITS.waitMs;
    while (performance.now() < end) {
      const head = await read(marker); await authority(marker, captured);
      if (head.state !== 'refreshing') return head;
      if (head.claimedAt + SOURCE_RP_LIMITS.staleClaimMs <= now()) {
        await closeClaim(head, head.claimId, 'unknown'); return read(marker);
      }
      await new Promise(done => setTimeout(done, 25));
    }
    throw new SourceRpError('account_provider_unavailable', 503);
  }
  async function rotate(marker, captured, original) {
    const proof = await tokenProof(marker, original);
    await protocol.ready(); await authority(marker, captured);
    const claimId = randomBytes(32).toString('hex'), claimedAt = now();
    const claimed = await storage.claim({ marker, authority: captured, expected: original, claimId, claimedAt });
    if (!claimed) return wait(marker, captured);
    let cipher;
    try {
      // Storage CAS checks source authority. Recheck after an asynchronous storage adapter and just before RT send.
      await authority(marker, captured);
      const next = await protocol.renew({ ...proof, subject: marker.subject });
      await authority(marker, captured);
      check(stamp(next.expiresAt) && next.expiresAt > now(), 'account_session_expired', 401);
      const expires = Math.min(next.expiresAt, original.sessionExpiresAt);
      const nextProof = closedProof({ accessToken: next.accessToken, refreshToken: next.refreshToken, nonce: next.nonce });
      check(nextProof.refreshToken !== proof.refreshToken, 'source_rp_refresh_unknown', 503);
      cipher = await encrypt('SourceRpRenewal', sourceRpCipherBinding(marker, original.revision + 1), nextProof);
      check(typeof cipher === 'string' && cipher.length >= 16 && cipher.length <= 32768, 'source_rp_storage_corrupt', 503);
      await authority(marker, captured);
      const committed = await storage.finish({ marker, authority: captured, expected: original, claimId,
        proofCipher: cipher, accessExpiresAt: expires, updatedAt: now() });
      check(committed, 'authentication_required', 401);
    } catch (error) {
      // Recover only the exact local durable commit; never retry a remotely consumed RT after unknown ACK.
      const current = await storage.read(marker.sessionIdHash);
      if (cipher && current?.state === 'idle' && current.revision === original.revision + 1 && current.lastAttemptId === claimId
        && current.proofCipher === cipher) { await authority(marker, captured); return validate(marker, current); }
      const revoked = typeof error?.status === 'number' && error.status >= 400 && error.status < 500;
      await closeClaim(original, claimId, revoked ? 'revoked' : 'unknown');
      if (revoked) throw error;
      throw new SourceRpError('source_rp_refresh_unknown', 503);
    }
    const head = await read(marker); await authority(marker, captured); return head;
  }
  return Object.freeze({
    async currentProof(inputMarker, hostOptions) {
      // Private Source orchestration only. This option creates no permission,
      // never enters HTTP/author JSON, and cannot extend the absolute deadline.
      const minimum = hostMinimum(hostOptions);
      const marker = validateMarker(inputMarker), captured = await storage.captureSourceAuthority(marker);
      let head = await read(marker); await authority(marker, captured);
      if (head.state === 'refreshing') head = await wait(marker, captured);
      if (head.state === 'idle' && head.accessExpiresAt < head.sessionExpiresAt
        && head.sessionExpiresAt - now() >= minimum
        && head.accessExpiresAt - now() <= Math.max(SOURCE_RP_LIMITS.earlyRefreshMs, minimum)) {
        check(head.revision < SOURCE_RP_LIMITS.rotations, 'authentication_required', 401);
        const flightKey = marker.profileDigest + ':' + marker.bindingDigest + ':' + marker.sessionIdHash;
        let pending = inFlight.get(flightKey);
        if (!pending) {
          check(inFlight.size < SOURCE_RP_LIMITS.inFlight, 'account_provider_unavailable', 503);
          pending = rotate(marker, captured, head).finally(() => inFlight.delete(flightKey)); inFlight.set(flightKey, pending);
        }
        head = await pending;
      }
      // Unknown can only use the old AT until its original expiry, with fresh issuer userinfo each time.
      check(head.accessExpiresAt > now(), 'authentication_required', 401);
      const proof = await tokenProof(marker, head); await authority(marker, captured);
      const subject = await protocol.currentSubject(proof.accessToken, marker.subject);
      check(subject === marker.subject, 'authentication_required', 401);
      await authority(marker, captured); const current = await read(marker);
      check(equalHead(current, head), 'account_provider_unavailable', 503);
      check(current.accessExpiresAt > now(), 'authentication_required', 401);
      const assertCurrent = async () => {
        await authority(marker, captured); const live = await read(marker);
        check(equalHead(live, head) && live.accessExpiresAt > now(), 'authentication_required', 401);
        const actual = await protocol.currentSubject(proof.accessToken, marker.subject);
        check(actual === marker.subject, 'authentication_required', 401);
        await authority(marker, captured); const after = await read(marker);
        check(equalHead(after, head) && after.accessExpiresAt > now(), 'authentication_required', 401);
      };
      return Object.freeze({ issuer: marker.issuer, sub: subject, expiresAt: head.accessExpiresAt,
        sessionExpiresAt: marker.sessionExpiresAt, sessionGeneration: head.revision, assertCurrent });
    },
    async compactExpired() { await storage.compactExpired?.({ now: now(), limit: SOURCE_RP_LIMITS.gcBatch }); },
  });
}
