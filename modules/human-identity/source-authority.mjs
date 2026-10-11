import { createPrivateKey } from 'node:crypto';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { closed, data, digest, nonce, requireHuman as require } from './profile.mjs';

/** Private host constructor, never a Connect operation. Only the scoped gateway
 * supplies an original-actor-fenced context; signing is followed by its fresh
 * Human/Apps/reference check. Reuse the issuer key, not the connector's MAC key. */
export function createSourceAuthoritySigner({ profile, clock = Date.now }) {
  require(profile?.enabled && typeof clock === 'function', 'human_identity_disabled', 503);
  const jwk = profile.providerKeys().jwks.keys[0], key = createPrivateKey({ key: jwk, format: 'jwk' }), kid = jwk.kid;
  const requireProvider = createRequire(createRequire(import.meta.url).resolve('oidc-provider'));
  let jose;
  return async function signSourceAuthority(input) {
    closed(input, ['context', 'sourceChallenge']);
    const context = data(input.context), sourceChallenge = nonce(input.sourceChallenge), now = clock();
    require(Number.isSafeInteger(now) && now > 0 && Number.isSafeInteger(context.expiresAt), 'human_identity_clock_invalid', 503);
    require(context.humanPrincipal?.issuer === profile.issuer && profile.client(context.humanPrincipal.clientId)
      && typeof context.humanPrincipal.subject === 'string' && context.humanPrincipal.subject === context.rootPrincipal?.accountId,
      'human_identity_subject_mismatch', 403);
    const iat = Math.floor(now / 1000), exp = Math.floor(Math.min(now + 10000, context.expiresAt) / 1000);
    require(exp > iat, 'human_identity_authority_expired', 401);
    jose ??= import(pathToFileURL(requireProvider.resolve('jose')).href);
    const { SignJWT } = await jose;
    const witness = await new SignJWT({ iss: profile.issuer, sub: context.humanPrincipal.subject,
      aud: 'urn:soty:source-authority', iat, exp, sourceChallenge, contextDigest: digest(context) })
      .setProtectedHeader({ alg: 'RS256', kid, typ: 'soty-source-authority+jwt' }).sign(key);
    require(exp * 1000 > clock(), 'human_identity_authority_expired', 401);
    return witness;
  };
}
