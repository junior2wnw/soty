import * as oidc from 'openid-client';

export const SOURCE_RP_PROFILE = 'soty.human-rp-renewal.v1';
export class SourceRpError extends Error {
  constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}
export function check(value, code = 'source_rp_invalid', status = 400) {
  if (!value) throw new SourceRpError(code, status);
}
const token = value => typeof value === 'string' && value.length >= 16 && value.length <= 4096 && !/[\u0000-\u0020\u007f]/u.test(value);
function approvedUrl(value) {
  check(typeof value === 'string' && value.length <= 1024 && !/[\s%\\?#]/u.test(value), 'source_rp_configuration_invalid', 503);
  let url; try { url = new URL(value); } catch { throw new SourceRpError('source_rp_configuration_invalid', 503); }
  check(url.href === value && !url.username && !url.password && (url.protocol === 'https:' || url.protocol === 'http:'
    && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)), 'source_rp_configuration_invalid', 503);
  return url;
}

/** Maintained OIDC protocol only. Source owns sessions, identity associations and ACL. */
export function createSourceRpProtocol(input, options = {}) {
  check(input && Object.keys(input).every(key => ['issuer', 'clientId', 'clientSecret', 'redirectUri', 'renewalProfile'].includes(key)),
    'source_rp_configuration_invalid', 503);
  const profile = Object.freeze({ ...input }), issuer = approvedUrl(profile.issuer), callback = approvedUrl(profile.redirectUri);
  const now = options.clock ?? Date.now;
  check(typeof now === 'function' && issuer.pathname === '/human-identity' && callback.pathname !== '/'
    && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/u.test(profile.clientId)
    && /^[A-Za-z0-9_-]{43,128}$/u.test(profile.clientSecret)
    && (profile.renewalProfile === undefined || profile.renewalProfile === SOURCE_RP_PROFILE), 'source_rp_configuration_invalid', 503);
  let pending;
  async function configuration() {
    pending ??= oidc.discovery(issuer, profile.clientId,
      { token_endpoint_auth_method: 'client_secret_basic', id_token_signed_response_alg: 'RS256' },
      oidc.ClientSecretBasic(profile.clientSecret), { timeout: 5,
        execute: [oidc.enableNonRepudiationChecks, ...(issuer.protocol === 'http:' ? [oidc.allowInsecureRequests] : [])] })
      .then(config => {
        const metadata = config.serverMetadata(); check(metadata.issuer === profile.issuer, 'source_rp_issuer_mismatch', 503);
        for (const [key, path] of [['authorization_endpoint', '/authorize'], ['token_endpoint', '/token'],
          ['userinfo_endpoint', '/userinfo'], ['jwks_uri', '/jwks']])
          check(metadata[key] === profile.issuer + path, 'source_rp_configuration_invalid', 503);
        return config;
      }).catch(() => { pending = undefined; throw new SourceRpError('source_rp_provider_unavailable', 503); });
    return pending;
  }
  return Object.freeze({ profile,
    async ready() { await configuration(); },
    async start() {
      const verifier = oidc.randomPKCECodeVerifier(), state = oidc.randomState(), nonce = oidc.randomNonce();
      const location = oidc.buildAuthorizationUrl(await configuration(), { redirect_uri: profile.redirectUri, scope: 'openid profile',
        response_type: 'code', state, nonce, code_challenge: await oidc.calculatePKCECodeChallenge(verifier), code_challenge_method: 'S256' });
      return { verifier, state, nonce, location: location.href };
    },
    async exchange(current, intent) {
      try {
        check(current instanceof URL && current.origin + current.pathname === profile.redirectUri
          && current.searchParams.get('iss') === profile.issuer, 'source_rp_issuer_mismatch');
        const tokens = await oidc.authorizationCodeGrant(await configuration(), current,
          { pkceCodeVerifier: intent.verifier, expectedState: intent.state, expectedNonce: intent.nonce });
        const claims = tokens.claims();
        check(claims?.iss === profile.issuer && typeof claims.sub === 'string' && claims.sub.length > 0 && claims.sub.length <= 128,
          'source_rp_claims_invalid');
        check(token(tokens.access_token) && Number.isSafeInteger(tokens.expires_in) && tokens.expires_in > 0
          && tokens.expires_in <= 300, 'source_rp_claims_invalid');
        const live = await oidc.fetchUserInfo(await configuration(), tokens.access_token, claims.sub);
        check(live.sub === claims.sub, 'source_rp_subject_mismatch');
        check(tokens.refresh_token === undefined || profile.renewalProfile === SOURCE_RP_PROFILE, 'source_rp_renewal_profile_required', 503);
        check(tokens.refresh_token === undefined || token(tokens.refresh_token), 'source_rp_claims_invalid');
        return { issuer: profile.issuer, subject: claims.sub, accessToken: tokens.access_token, expiresAt: now() + tokens.expires_in * 1000,
          ...(tokens.refresh_token ? { refreshToken: tokens.refresh_token, nonce: intent.nonce } : {}) };
      } catch (error) {
        if (error instanceof SourceRpError) throw error;
        throw new SourceRpError('source_rp_proof_invalid', 403);
      }
    },
    async renew(inputProof) {
      check(profile.renewalProfile === SOURCE_RP_PROFILE, 'source_rp_renewal_profile_required', 503);
      check(token(inputProof?.refreshToken) && typeof inputProof.subject === 'string'
        && /^[A-Za-z0-9_-]{16,128}$/u.test(inputProof.nonce), 'source_rp_storage_corrupt', 503);
      let userinfoPhase = false;
      try {
        const tokens = await oidc.refreshTokenGrant(await configuration(), inputProof.refreshToken);
        const claims = tokens.claims();
        // A lawful refresh may omit ID token/nonce; the maintained SDK checks any present signature/audience.
        check(!claims || claims.iss === profile.issuer && claims.sub === inputProof.subject
          && (claims.nonce === undefined || claims.nonce === inputProof.nonce), 'source_rp_claims_invalid');
        check(token(tokens.access_token) && Number.isSafeInteger(tokens.expires_in) && tokens.expires_in > 0 && tokens.expires_in <= 300
          && token(tokens.refresh_token) && tokens.refresh_token !== inputProof.refreshToken, 'source_rp_claims_invalid');
        const expiresAt = now() + tokens.expires_in * 1000;
        userinfoPhase = true;
        const live = await oidc.fetchUserInfo(await configuration(), tokens.access_token, inputProof.subject);
        check(live.sub === inputProof.subject, 'source_rp_subject_mismatch');
        return { accessToken: tokens.access_token, refreshToken: tokens.refresh_token, nonce: inputProof.nonce, expiresAt };
      } catch (error) {
        if (error instanceof oidc.ResponseBodyError && error.error === 'invalid_grant') throw new SourceRpError('source_rp_refresh_revoked', 401);
        if (userinfoPhase && (error instanceof oidc.ResponseBodyError || error instanceof oidc.WWWAuthenticateChallengeError)
          && [401, 403].includes(error.status)) throw new SourceRpError('source_rp_refresh_revoked', 401);
        // No response, malformed response or failed fresh userinfo cannot prove whether the RT was consumed.
        throw new SourceRpError('source_rp_refresh_unknown', 503);
      }
    },
    async currentSubject(accessToken, expectedSubject) {
      try {
        check(token(accessToken) && typeof expectedSubject === 'string', 'authentication_required', 401);
        const live = await oidc.fetchUserInfo(await configuration(), accessToken, expectedSubject);
        check(live.sub === expectedSubject, 'source_rp_subject_mismatch'); return live.sub;
      } catch (error) {
        if (error instanceof SourceRpError && error.status >= 500) throw error;
        if (error instanceof SourceRpError || (error instanceof oidc.ResponseBodyError || error instanceof oidc.WWWAuthenticateChallengeError)
          && [401, 403].includes(error.status) || error instanceof oidc.ClientError && error.code === 'OAUTH_JSON_ATTRIBUTE_COMPARISON_FAILED')
          throw new SourceRpError('authentication_required', 401);
        throw new SourceRpError('account_provider_unavailable', 503);
      }
    },
  });
}
