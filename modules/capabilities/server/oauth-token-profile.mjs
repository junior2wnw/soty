import { OAUTH_CLIENTS, OAUTH_SCOPE, canonicalOAuthJson, oauthCheck, oauthData, oauthProviderId,
  oauthRedirect, oauthSeconds, oauthSynchronous, oauthTime, snapshotOAuthJson } from './oauth-profile.mjs';

export const OAUTH_TOKEN_MODELS = Object.freeze(['AuthorizationCode', 'RefreshToken', 'AccessToken']);
const maximum = Object.freeze({ AuthorizationCode: 60, RefreshToken: 86400, AccessToken: 300 });
const optional = (value, key, check) => { if (value[key] !== undefined) check(value[key]); };
export function oauthOpaqueId(value) {
  oauthCheck(typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value)); return value;
}

/** This is a snapshot from the authenticated Provider context, never a body or
 * an external actor. Missing resource/scope can only inherit the bound Grant. */
export function snapshotOAuthTokenRequest(value) {
  oauthData(value, ['clientId', 'resource', 'scope', 'grantType']);
  const result = snapshotOAuthJson(value);
  oauthCheck(OAUTH_CLIENTS.includes(result.clientId));
  for (const key of ['resource', 'scope', 'grantType']) optional(result, key, item => oauthCheck(typeof item === 'string'));
  return Object.freeze(result);
}

export function oauthTokenRequestOutcome({ request, connection, model, consuming = false }) {
  if (request.clientId !== connection.static_client_id) return 'invalid_grant';
  if (request.resource !== undefined && request.resource !== connection.resource) return 'invalid_target';
  if (request.scope !== undefined && request.scope !== OAUTH_SCOPE) return 'invalid_grant';
  const required = model === 'AuthorizationCode' ? 'authorization_code' : 'refresh_token';
  if (consuming) return request.grantType === required ? null : 'invalid_grant';
  if (model === 'AuthorizationCode') return request.grantType === undefined ? null : 'invalid_grant';
  return ['authorization_code', 'refresh_token'].includes(request.grantType) ? null : 'invalid_grant';
}

export function createOAuthTokenProfile(configuration) {
  return Object.freeze({
    snapshot({ model, id, payload, connection, nowMs, expiresIn, grant, allowExpired = false }) {
      oauthCheck(OAUTH_TOKEN_MODELS.includes(model), 'oauth_unavailable'); oauthOpaqueId(id); oauthTime(nowMs);
      const value = snapshotOAuthJson(payload);
      const fields = ['iat', 'exp', 'jti', 'kind', 'accountId', 'clientId', 'grantId', 'scope', 'expiresWithSession', 'sessionUid'];
      oauthData(value, fields.concat(model === 'AuthorizationCode'
        ? ['authTime', 'codeChallenge', 'codeChallengeMethod', 'redirectUri', 'resource']
        : model === 'RefreshToken' ? ['authTime', 'resource', 'gty', 'iiat', 'rotations'] : ['aud', 'extra', 'gty']));
      oauthCheck(value.kind === model && value.jti === id && value.accountId === connection.account_id
        && value.clientId === connection.static_client_id && value.grantId === connection.provider_grant_id
        && value.scope === OAUTH_SCOPE);
      oauthProviderId(value.grantId);
      const iat = oauthSeconds(value.iat), exp = oauthSeconds(value.exp), expiresAt = exp * 1000;
      oauthCheck(iat >= Math.floor(connection.created_at / 1000) && iat * 1000 <= nowMs && exp > iat
        && exp - iat <= maximum[model] && expiresAt <= connection.expires_at && (allowExpired || expiresAt > nowMs));
      if (grant !== undefined) oauthCheck(grant.jti === value.grantId && expiresAt <= grant.exp * 1000);
      if (expiresIn !== undefined) oauthCheck(typeof expiresIn === 'number' && Number.isFinite(expiresIn)
        && expiresIn > 0 && expiresIn <= maximum[model]);
      optional(value, 'expiresWithSession', item => oauthCheck(item === false));
      optional(value, 'sessionUid', oauthProviderId);
      optional(value, 'authTime', item => oauthCheck(oauthSeconds(item) <= iat));
      if (model === 'AuthorizationCode') {
        oauthCheck(value.resource === connection.resource && value.codeChallengeMethod === 'S256');
        oauthOpaqueId(value.codeChallenge); oauthRedirect(value.redirectUri);
        oauthCheck(oauthSynchronous(configuration.isRegisteredRedirect(Object.freeze({
          clientId: value.clientId, redirectUri: value.redirectUri }))) === true);
      } else {
        oauthCheck((model === 'AccessToken' ? value.aud : value.resource) === connection.resource);
        optional(value, 'gty', item => oauthCheck(typeof item === 'string'
          && /^(?:authorization_code|authorization_code refresh_token)$/u.test(item)));
        if (model === 'RefreshToken') {
          optional(value, 'iiat', item => oauthCheck(oauthSeconds(item) >= Math.floor(connection.created_at / 1000) && item <= iat));
          optional(value, 'rotations', item => oauthCheck(Number.isSafeInteger(item) && item >= 0));
        } else optional(value, 'extra', item => oauthData(item, []));
      }
      const json = canonicalOAuthJson(value);
      return { payload: JSON.parse(json), json, expiresAt,
        retainUntil: model === 'AccessToken' ? expiresAt : connection.expires_at };
    },
  });
}
