import { createPrivateKey } from 'node:crypto';
import { validateDiscoveryOrigin } from './capabilities-discovery.js';
import { OAuthIngressError } from './capabilities-oauth-ingress.js';

const check = value => { if (!value) throw new OAuthIngressError('oauth_configuration_invalid'); };
const ownRecord = value => value !== null && typeof value === 'object' && !Array.isArray(value)
  && [Object.prototype, null].includes(Object.getPrototypeOf(value));
const id = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,64}$/u.test(value);
const encoded = value => typeof value === 'string' && /^[A-Za-z0-9_-]+$/u.test(value);
export const OAUTH_SCOPE = 'notes.createDraft';

// Public native IDs identify a connection profile, not an attested executable.
// Paths were observed in the pinned CLI preflight. Only the loopback port varies.
const profiles = Object.freeze([
  Object.freeze({ id: 'soty-codex-cli', label: 'Codex CLI', path: '/callback' }),
  Object.freeze({ id: 'soty-opencode-cli', label: 'OpenCode CLI', path: '/mcp/oauth/callback' }),
]);

export function isRegisteredOAuthRedirect({ clientId, redirectUri } = {}) {
  const profile = profiles.find(item => item.id === clientId);
  if (!profile || typeof redirectUri !== 'string' || redirectUri.length > 256) return false;
  // URL normalisation must not turn userinfo, encoding, IPv6, localhost or an
  // unregistered path into the observed literal callback. An explicit port is required.
  const match = /^http:\/\/127\.0\.0\.1:([1-9][0-9]{0,4})(\/[^?#\s%\\]*)$/u.exec(redirectUri);
  return Boolean(match && Number(match[1]) <= 65535 && match[2] === profile.path);
}

function copyJwks(value) {
  check(ownRecord(value) && Object.keys(value).length === 1 && Array.isArray(value.keys)
    && value.keys.length >= 1 && value.keys.length <= 2);
  const kids = new Set();
  return { keys: value.keys.map(key => {
    const fields = ['kty', 'kid', 'use', 'alg', 'n', 'e', 'd', 'p', 'q', 'dp', 'dq', 'qi'];
    check(ownRecord(key) && Object.keys(key).length === fields.length
      && fields.every(field => Object.hasOwn(key, field)));
    check(key.kty === 'RSA' && key.use === 'sig' && key.alg === 'RS256' && id(key.kid) && !kids.has(key.kid));
    kids.add(key.kid);
    check(['n', 'e', 'd', 'p', 'q', 'dp', 'dq', 'qi'].every(field => encoded(key[field]) && key[field].length <= 1024));
    const copied = { ...key };
    try {
      const imported = createPrivateKey({ key: copied, format: 'jwk' });
      check(imported.asymmetricKeyType === 'rsa' && imported.asymmetricKeyDetails.modulusLength >= 2048
        && imported.asymmetricKeyDetails.modulusLength <= 4096);
    } catch { throw new OAuthIngressError('oauth_configuration_invalid'); }
    return copied;
  }) };
}

/** Validate trusted host configuration before any database is opened. Secret
 * bytes live in this closure, never in the public status/metadata projection.
 * This function never generates, replaces, reads from disk, or fetches keys. */
export function createOAuthHostProfile(options, { shellOrigins = [], audience = '' } = {}) {
  if (options === undefined) return null;
  const allowed = ['enabled', 'issuer', 'jwks', 'cookieKeys', 'artifactKey', 'artifactKeyId'];
  check(ownRecord(options) && Object.keys(options).every(key => allowed.includes(key))
    && typeof options.enabled === 'boolean' && typeof options.issuer === 'string');
  check(options.issuer.endsWith('/oauth'));
  let origin;
  try { origin = validateDiscoveryOrigin({ discoveryOrigin: options.issuer.slice(0, -6), shellOrigins }); }
  catch { throw new OAuthIngressError('oauth_configuration_invalid'); }
  check(origin !== '' && options.issuer === `${origin}/oauth` && audience === origin);
  const resources = Object.freeze({ http: audience, mcp: `${origin}/mcp` });
  const secretFields = ['jwks', 'cookieKeys', 'artifactKey', 'artifactKeyId'];
  const hasSecrets = secretFields.some(field => Object.hasOwn(options, field));
  check(!options.enabled || hasSecrets);
  let jwks, cookieKeys, artifactKey, artifactKeyId;
  if (hasSecrets) {
    check(secretFields.every(field => Object.hasOwn(options, field)));
    jwks = copyJwks(options.jwks);
    check(Array.isArray(options.cookieKeys) && options.cookieKeys.length >= 1 && options.cookieKeys.length <= 2
      && options.cookieKeys.every(key => encoded(key) && key.length >= 43 && key.length <= 128)
      && new Set(options.cookieKeys).size === options.cookieKeys.length);
    check(options.artifactKey instanceof Uint8Array && options.artifactKey.byteLength === 32 && id(options.artifactKeyId));
    cookieKeys = [...options.cookieKeys]; artifactKey = Buffer.from(options.artifactKey); artifactKeyId = options.artifactKeyId;
  }
  const issuer = options.issuer, enabled = options.enabled, secure = origin.startsWith('https:');
  return Object.freeze({
    origin, issuer, resources, enabled, secure, profiles,
    isRegisteredRedirect: isRegisteredOAuthRedirect,
    domainConfiguration(withAuthorityFence) {
      check(typeof withAuthorityFence === 'function');
      return { issuer, resources, withAuthorityFence, isRegisteredRedirect: isRegisteredOAuthRedirect,
        ...(artifactKey ? { artifactKey: Buffer.from(artifactKey), artifactKeyId } : {}) };
    },
    providerKeys() {
      check(enabled && jwks && cookieKeys);
      return { jwks: { keys: jwks.keys.map(key => ({ ...key })) }, cookieKeys: [...cookieKeys] };
    },
    clients() {
      return profiles.map(profile => ({ client_id: profile.id,
        redirect_uris: [`http://127.0.0.1${profile.path}`], application_type: 'native',
        token_endpoint_auth_method: 'none', grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] }));
    },
    protectedResource(resource) {
      check(Object.values(resources).includes(resource));
      return { resource, authorization_servers: [issuer], scopes_supported: [OAUTH_SCOPE],
        bearer_methods_supported: ['header'], resource_name: 'Соты — создание записок' };
    },
  });
}

/** Reserve protocol namespaces even while AS is disabled. Encoded/case-mutated
 * variants are rejected by the router, never treated as the ordinary SPA. */
export function isOAuthNamespace(target) {
  if (typeof target !== 'string') return false;
  let pathname = target.split('?')[0];
  for (let pass = 0; pass < 3; pass++) {
    if (/^\/(?:oauth|mcp)(?:\/|$)/iu.test(pathname)
      || /^\/\.well-known\/oauth-(?:authorization-server|protected-resource)(?:\/|$)/iu.test(pathname)) return true;
    try { const decoded = decodeURIComponent(pathname); if (decoded === pathname) break; pathname = decoded; }
    catch { break; }
  }
  return false;
}

export function reserveOAuthNamespaces(app) {
  app.use((req, res, next) => {
    if (!isOAuthNamespace(req.originalUrl || req.url)) { next(); return; }
    if (!req.complete || !req.readableEnded) { res.shouldKeepAlive = false; res.set('Connection', 'close'); }
    res.status(503).set({ 'Cache-Control': 'no-store', 'Pragma': 'no-cache', 'Retry-After': '60' })
      .json({ error: 'temporarily_unavailable' });
  });
}
