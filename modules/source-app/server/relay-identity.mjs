import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { relayRequestDigest } from '../shared/relay-identity.mjs';

let jose;
const maintainedJose = () => jose ??= import(pathToFileURL(createRequire(createRequire(import.meta.url).resolve('openid-client')).resolve('jose')).href);
const denied = () => { throw Object.assign(new Error('soty_identity_invalid'), { code: 'soty_identity_invalid', status: 401 }); };

/** Verify a Root assertion received on the application's trusted HTTP boundary. */
export function createRelayIdentityVerifier({ issuer, appId, fetch: fetcher = globalThis.fetch, clock = Date.now, consume } = {}) {
  let url;
  try { url = new URL(issuer); } catch { denied(); }
  if (url.protocol !== 'https:' || url.username || url.password || url.href !== issuer || url.pathname !== '/human-identity' || url.search || url.hash
    || !/^app-[a-f0-9]{32}$/u.test(appId) || typeof fetcher !== 'function' || typeof clock !== 'function'
    || consume !== undefined && typeof consume !== 'function') denied();
  const expectedJwks = issuer + '/jwks', seen = new Map(); let jwks;
  // A process-local cache suffices only for a single application worker. Hosts
  // with multiple workers supply an atomic shared consume(jti, expiresAt).
  const consumeOnce = consume ?? ((jti, expiresAt) => {
    const now = clock(); for (const [id, expiry] of seen) if (expiry <= now) seen.delete(id);
    if (seen.has(jti) || seen.size >= 10000) return false;
    seen.set(jti, expiresAt); return true;
  });
  return async function verifyRelayIdentity(req) {
    const token = req?.headers?.['x-soty-identity'];
    if (typeof token !== 'string' || token.length > 8192 || !Array.isArray(req.rawHeaders)
      || req.rawHeaders.filter((_, i) => i % 2 === 0 && req.rawHeaders[i].toLowerCase() === 'x-soty-identity').length !== 1) denied();
    let requestDigest;
    try { requestDigest = relayRequestDigest(req.method, req.url); } catch { denied(); }
    try {
      const { jwtVerify, createRemoteJWKSet, customFetch } = await maintainedJose();
      jwks ??= createRemoteJWKSet(new URL(expectedJwks), { timeoutDuration: 5000, [customFetch]: (input, init) => {
        if (new URL(input).href !== expectedJwks) denied();
        return fetcher(input, { ...init, redirect: 'error' });
      } });
      const now = clock();
      const { payload, protectedHeader } = await jwtVerify(token, jwks, { algorithms: ['RS256'], issuer,
        audience: 'urn:soty:app:' + appId, typ: 'soty-relay-identity+jwt', currentDate: new Date(now) });
      if (Object.keys(protectedHeader).some(key => !['alg', 'typ', 'kid'].includes(key))
        || Object.keys(payload).sort().join(',') !== 'aud,exp,iat,iss,jti,requestDigest,sub'
        || payload.aud !== 'urn:soty:app:' + appId || typeof payload.sub !== 'string' || !payload.sub || payload.sub.length > 128
        || typeof payload.jti !== 'string' || !/^[A-Za-z0-9_-]{32}$/u.test(payload.jti)
        || payload.requestDigest !== requestDigest || !Number.isSafeInteger(payload.iat) || !Number.isSafeInteger(payload.exp)
        || payload.exp <= payload.iat || payload.exp - payload.iat > 10 || payload.iat * 1000 > now + 1000 || payload.exp * 1000 <= now) denied();
      if (await consumeOnce(payload.jti, payload.exp * 1000) !== true || payload.exp * 1000 <= clock()) denied();
      return Object.freeze({ issuer: payload.iss, subject: payload.sub });
    } catch { denied(); }
  };
}
