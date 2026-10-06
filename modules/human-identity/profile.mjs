import { createHash, createPrivateKey } from 'node:crypto';
import { snapshotOAuthJson, canonicalOAuthJson } from '../capabilities/server/oauth-profile.mjs';

export const HUMAN_IDENTITY_PROFILE = 'oidc-provider-9.12.2-human-v1';
export const HUMAN_IDENTITY_PATH = '/human-identity';
export class HumanIdentityError extends Error {
  constructor(code = 'human_identity_invalid', status = 400) { super(code); this.name = 'HumanIdentityError'; this.code = code; this.status = status; }
}
export const requireHuman = (ok, code = 'human_identity_invalid', status = 400) => { if (!ok) throw new HumanIdentityError(code, status); };
export function closed(value, required, optional = [], code = 'human_identity_fields_invalid') {
  requireHuman(value && typeof value === 'object' && !Array.isArray(value) && [Object.prototype, null].includes(Object.getPrototypeOf(value)), code);
  const properties = Object.getOwnPropertyDescriptors(value);
  requireHuman(Reflect.ownKeys(properties).every(key => typeof key === 'string' && properties[key].enumerable && 'value' in properties[key]
    && (required.includes(key) || optional.includes(key))) && required.every(key => Object.hasOwn(properties, key)), code);
}
export const digest = value => createHash('sha256').update(typeof value === 'string' || value instanceof Uint8Array ? value : canonicalOAuthJson(value)).digest('hex');
export const data = value => snapshotOAuthJson(value);
export const id = value => {
  requireHuman(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/u.test(value) && !value.includes('..') && !value.includes('//'), 'human_identity_id_invalid'); return value;
};
export const nonce = value => { requireHuman(typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value), 'human_identity_browser_mismatch', 403); return value; };
export const uid = value => { requireHuman(typeof value === 'string' && /^[A-Za-z0-9_-]{16,128}$/u.test(value), 'human_identity_interaction_invalid'); return value; };
export const synchronous = value => {
  if (value && typeof value.then === 'function') { Promise.resolve(value).catch(() => {}); throw new HumanIdentityError('human_identity_async_boundary', 500); }
  return value;
};
function originFor(value) {
  requireHuman(typeof value === 'string' && value.length <= 512 && !/[\s%\\?#]/u.test(value), 'human_identity_configuration_invalid', 503);
  let url; try { url = new URL(value); } catch { throw new HumanIdentityError('human_identity_configuration_invalid', 503); }
  requireHuman(!url.username && !url.password && value === url.origin && (url.protocol === 'https:'
    || url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)), 'human_identity_configuration_invalid', 503);
  return value;
}
function redirectFor(value) {
  requireHuman(typeof value === 'string' && value.length <= 1024 && !/[\s%\\?#]/u.test(value), 'human_identity_configuration_invalid', 503);
  let url; try { url = new URL(value); } catch { throw new HumanIdentityError('human_identity_configuration_invalid', 503); }
  originFor(url.origin); requireHuman(value === url.href && url.pathname !== '/', 'human_identity_configuration_invalid', 503); return value;
}
function keysFor(input) {
  const jwks = data(input); closed(jwks, ['keys']);
  requireHuman(Array.isArray(jwks.keys) && jwks.keys.length >= 1 && jwks.keys.length <= 2, 'human_identity_configuration_invalid', 503);
  const seen = new Set();
  for (const key of jwks.keys) {
    closed(key, ['kty', 'kid', 'use', 'alg', 'n', 'e', 'd', 'p', 'q', 'dp', 'dq', 'qi']);
    requireHuman(key.kty === 'RSA' && key.use === 'sig' && key.alg === 'RS256' && typeof key.kid === 'string'
      && /^[A-Za-z0-9_-]{1,64}$/u.test(key.kid) && !seen.has(key.kid), 'human_identity_configuration_invalid', 503);
    seen.add(key.kid);
    try {
      const imported = createPrivateKey({ key, format: 'jwk' }), bits = imported.asymmetricKeyDetails?.modulusLength;
      requireHuman(imported.asymmetricKeyType === 'rsa' && bits >= 2048 && bits <= 4096, 'human_identity_configuration_invalid', 503);
    } catch { throw new HumanIdentityError('human_identity_configuration_invalid', 503); }
  }
  return jwks;
}

/** Approved host configuration only. Keys are not generated, fetched, loaded from files or emitted in DTOs. */
export function createHumanIdentityHostProfile(options, { shellOrigins = [] } = {}) {
  if (options === undefined) return null;
  const code = 'human_identity_configuration_invalid';
  closed(options, ['enabled', 'issuer', 'registryId', 'environmentId'],
    ['clients', 'jwks', 'cookieKeys', 'artifactKey', 'artifactKeyId'], code);
  requireHuman(typeof options.enabled === 'boolean' && typeof options.issuer === 'string' && options.issuer.endsWith(HUMAN_IDENTITY_PATH), code, 503);
  const origin = originFor(options.issuer.slice(0, -HUMAN_IDENTITY_PATH.length));
  requireHuman(shellOrigins.includes(origin), code, 503); id(options.registryId); id(options.environmentId);
  const enabled = options.enabled, issuer = options.issuer;
  if (!enabled) return Object.freeze({ enabled: false, issuer, origin, secure: origin.startsWith('https:'), registryId: options.registryId, environmentId: options.environmentId });
  const clients = data(options.clients); requireHuman(Array.isArray(clients) && clients.length >= 1 && clients.length <= 64, code, 503);
  const ids = new Set(), redirects = new Set();
  for (const client of clients) {
    closed(client, ['id', 'label', 'redirectUri', 'clientSecret'], ['version'], code); id(client.id); redirectFor(client.redirectUri);
    if (client.version === undefined) client.version = 1;
    requireHuman(Number.isSafeInteger(client.version) && client.version >= 1 && client.version <= 1000000, code, 503);
    requireHuman(typeof client.label === 'string' && client.label.isWellFormed() && client.label.trim().length > 0 && client.label.length <= 120
      && !/[\u0000-\u001f\u007f]/u.test(client.label)
      && typeof client.clientSecret === 'string' && /^[A-Za-z0-9_-]{43,128}$/u.test(client.clientSecret)
      && !ids.has(client.id) && !redirects.has(client.redirectUri), code, 503);
    ids.add(client.id); redirects.add(client.redirectUri);
  }
  const jwks = keysFor(options.jwks), cookies = data(options.cookieKeys);
  requireHuman(Array.isArray(cookies) && cookies.length >= 1 && cookies.length <= 2
    && cookies.every(key => typeof key === 'string' && /^[A-Za-z0-9_-]{43,128}$/u.test(key)) && new Set(cookies).size === cookies.length, code, 503);
  requireHuman(options.artifactKey instanceof Uint8Array && options.artifactKey.byteLength === 32
    && typeof options.artifactKeyId === 'string' && /^[A-Za-z0-9_-]{1,64}$/u.test(options.artifactKeyId), code, 503);
  const artifactKey = Buffer.from(options.artifactKey), keyId = options.artifactKeyId;
  const protocolDigest = digest({ profile: HUMAN_IDENTITY_PROFILE, issuer, scopes: ['openid', 'profile'],
    responseTypes: ['code'], pkce: 'S256', clientAuth: 'client_secret_basic', subjectType: 'public' });
  const publicClients = clients.map(({ id: clientId, label, redirectUri, version }) => Object.freeze({ id: clientId, label, redirectUri, version,
    profileDigest: digest({ protocolDigest, id: clientId, version, redirectUri }) }));
  return Object.freeze({ enabled, issuer, origin, secure: origin.startsWith('https:'), registryId: options.registryId, environmentId: options.environmentId,
    profile: HUMAN_IDENTITY_PROFILE, protocolDigest, profileDigest: protocolDigest,
    publicClients: Object.freeze(publicClients),
    client(clientId) { return publicClients.find(client => client.id === clientId); },
    isRegisteredRedirect(clientId, redirectUri) { return publicClients.some(client => client.id === clientId && client.redirectUri === redirectUri); },
    providerKeys() { return { jwks: data(jwks), cookieKeys: [...cookies] }; },
    encryptionKey() { return { key: Buffer.from(artifactKey), keyId }; },
    providerClients() { return clients.map(client => ({ client_id: client.id, client_secret: client.clientSecret, redirect_uris: [client.redirectUri],
      application_type: 'web', subject_type: 'public', token_endpoint_auth_method: 'client_secret_basic',
      grant_types: ['authorization_code'], response_types: ['code'], id_token_signed_response_alg: 'RS256' })); },
  });
}
