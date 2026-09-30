import { AccessError } from './validation.mjs';

export const OAUTH_PROFILE = 'oidc-provider-9.12.2-c1';
export const OAUTH_MODELS = Object.freeze(['Session', 'Interaction', 'Grant', 'AuthorizationCode', 'RefreshToken', 'AccessToken']);
export const OAUTH_CLIENTS = Object.freeze(['soty-codex-cli', 'soty-opencode-cli']);
export const OAUTH_SCOPE = 'notes.createDraft';
export const OAUTH_PAYLOAD_BYTES = 16384;
const MAX_SECONDS = Math.floor(Number.MAX_SAFE_INTEGER / 1000);
const forbidden = new Set(['__proto__', 'prototype', 'constructor']);
export function oauthCheck(value, code = 'oauth_invalid_artifact') { if (!value) throw new AccessError(code); }
export function oauthData(value, keys, code = 'oauth_invalid_artifact') {
  oauthCheck(value && typeof value === 'object' && !Array.isArray(value)
    && [Object.prototype, null].includes(Object.getPrototypeOf(value)), code);
  for (const key of Reflect.ownKeys(value)) {
    oauthCheck(typeof key === 'string' && !forbidden.has(key) && (!keys || keys.includes(key)), code);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    oauthCheck(descriptor && Object.hasOwn(descriptor, 'value') && descriptor.enumerable, code);
  }
  return value;
}
export function oauthString(value, max = 160) {
  oauthCheck(typeof value === 'string' && value.isWellFormed() && value.length <= max && Buffer.byteLength(value) <= max);
  return value;
}
export function oauthId(value) {
  oauthString(value); oauthCheck(/^[A-Za-z0-9][A-Za-z0-9_.:@/-]*$/u.test(value) && !value.includes('..'));
  return value;
}
export function oauthProviderId(value) {
  oauthString(value); oauthCheck(/^[A-Za-z0-9_-]{16,160}$/u.test(value)); return value;
}
export function oauthSeconds(value) {
  oauthCheck(Number.isSafeInteger(value) && value >= 0 && value <= MAX_SECONDS); return value;
}
export function oauthTime(value) {
  oauthCheck(Number.isSafeInteger(value) && value >= 0, 'clock_invalid'); return value;
}
export function oauthUri(value) {
  oauthString(value, 2048); oauthCheck(!/[\s#\\]/u.test(value));
  let url; try { url = new URL(value); } catch { oauthCheck(false); }
  oauthCheck(!url.username && !url.password && (url.protocol === 'https:'
    || (url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)))
    && (value === url.href || value === url.origin));
  return url;
}
export function oauthRedirect(value) {
  oauthString(value, 2048);
  const parts = /^http:\/\/(localhost|127\.0\.0\.1|\[::1\]):([1-9][0-9]{0,4})(\/[^\s?#\\]*)$/u.exec(value);
  oauthCheck(parts && Number(parts[2]) <= 65535);
  const url = new URL(value);
  // URL.port removes the explicitly supplied default :80. Validate the raw
  // decimal port and canonical pathname separately, without losing that fact.
  oauthCheck(url.pathname === parts[3]);
  return value;
}
export function oauthSynchronous(value, code = 'oauth_context_invalid') {
  if (value && typeof value.then === 'function') {
    Promise.resolve(value).catch(() => {}); // Contain a trusted wiring error; never await under SQLite.
    throw new AccessError(code);
  }
  return value;
}

/** Snapshot data properties without invoking accessors. Optional undefined
 * fields survive until exact shape checks, then canonical JSON omits them. */
export function snapshotOAuthJson(value) {
  let nodes = 0, bytes = 0;
  function budget(n) { bytes += n; oauthCheck(bytes <= OAUTH_PAYLOAD_BYTES); }
  function copy(item, depth) {
    oauthCheck(++nodes <= 1024 && depth <= 12);
    if (item === undefined) return item;
    if (item === null || typeof item === 'boolean') { budget(JSON.stringify(item).length); return item; }
    if (typeof item === 'string') { oauthString(item, OAUTH_PAYLOAD_BYTES); budget(Buffer.byteLength(JSON.stringify(item))); return item; }
    if (typeof item === 'number') { oauthCheck(Number.isFinite(item)); budget(JSON.stringify(item).length); return item; }
    if (Array.isArray(item)) {
      oauthCheck(item.length <= 1024 && Reflect.ownKeys(item).length === item.length + 1);
      budget(2 + Math.max(0, item.length - 1));
      const out = [];
      for (let i = 0; i < item.length; i++) {
        const d = Object.getOwnPropertyDescriptor(item, String(i));
        oauthCheck(d && Object.hasOwn(d, 'value') && d.enumerable && d.value !== undefined);
        out.push(copy(d.value, depth + 1));
      }
      return out;
    }
    oauthData(item);
    const keys = Object.keys(item); oauthCheck(keys.length <= 1024);
    const out = Object.create(null);
    budget(2); let members = 0;
    for (const key of keys) {
      oauthString(key, OAUTH_PAYLOAD_BYTES);
      const child = Object.getOwnPropertyDescriptor(item, key).value;
      if (child !== undefined) budget(Buffer.byteLength(JSON.stringify(key)) + 1 + (members++ ? 1 : 0));
      out[key] = copy(child, depth + 1);
    }
    return out;
  }
  return copy(value, 0);
}
export function canonicalOAuthJson(value) {
  const snapshot = snapshotOAuthJson(value);
  function encode(item) {
    if (item === null || typeof item !== 'object') { oauthCheck(item !== undefined); return JSON.stringify(item); }
    if (Array.isArray(item)) return '[' + item.map(encode).join(',') + ']';
    return '{' + Object.keys(item).filter(key => item[key] !== undefined).sort()
      .map(key => JSON.stringify(key) + ':' + encode(item[key])).join(',') + '}';
  }
  const result = encode(snapshot); oauthCheck(Buffer.byteLength(result) <= OAUTH_PAYLOAD_BYTES); return result;
}
const asyncFunction = fn => fn?.constructor?.name === 'AsyncFunction';
export function normalizeOAuthConfiguration(value) {
  if (value === undefined) return null;
  const code = 'oauth_configuration_invalid';
  try {
    oauthData(value, ['issuer', 'resources', 'withAuthorityFence', 'isRegisteredRedirect', 'artifactKey', 'artifactKeyId'], code);
    const { issuer, resources, withAuthorityFence, isRegisteredRedirect, artifactKey, artifactKeyId } = value;
    const origin = oauthUri(issuer).origin; oauthCheck(issuer === origin + '/oauth', code);
    oauthData(resources, ['http', 'mcp'], code);
    oauthCheck(resources.http === origin && resources.mcp === origin + '/mcp', code);
    for (const callback of [withAuthorityFence, isRegisteredRedirect]) oauthCheck(typeof callback === 'function' && !asyncFunction(callback), code);
    const hasKey = artifactKey !== undefined;
    oauthCheck(hasKey === (artifactKeyId !== undefined), code);
    if (hasKey) {
      oauthCheck(artifactKey instanceof Uint8Array && artifactKey.byteLength === 32, code);
      oauthCheck(typeof artifactKeyId === 'string' && /^[A-Za-z0-9._:-]{1,64}$/u.test(artifactKeyId), code);
    }
    // Private module result only. The copied key never becomes a service DTO.
    return Object.freeze({ issuer, origin, resources: Object.freeze({ http: resources.http, mcp: resources.mcp }),
      withAuthorityFence, isRegisteredRedirect, artifactKey: hasKey ? Buffer.from(artifactKey) : null, artifactKeyId: artifactKeyId ?? null });
  } catch { throw new AccessError(code); }
}

function optional(value, key, check) { if (value[key] !== undefined) check(value[key]); }
function boolean(value) { oauthCheck(typeof value === 'boolean'); }
function strings(value, maximum = 16) {
  oauthCheck(Array.isArray(value) && value.length <= maximum); value.forEach(item => oauthString(item, 160));
}
function result(value) {
  oauthData(value, ['login', 'consent', 'error', 'error_description']);
  optional(value, 'error', item => oauthString(item, 160)); optional(value, 'error_description', item => oauthString(item, 1024));
  if (value.login !== undefined) {
    oauthData(value.login, ['accountId', 'remember', 'ts', 'acr', 'amr']); oauthId(value.login.accountId);
    optional(value.login, 'remember', boolean); optional(value.login, 'ts', oauthSeconds);
    optional(value.login, 'acr', item => oauthString(item, 160)); optional(value.login, 'amr', strings);
  }
  if (value.consent !== undefined) { oauthData(value.consent, ['grantId']); oauthProviderId(value.consent.grantId); }
  oauthCheck(value.error === undefined || (value.login === undefined && value.consent === undefined));
}
export function createOAuthUnboundProfile(configuration) {
  const { issuer, resources, isRegisteredRedirect } = configuration;
  function redirect(clientId, value) {
    oauthRedirect(value);
    const admitted = oauthSynchronous(isRegisteredRedirect(Object.freeze({ clientId, redirectUri: value })));
    oauthCheck(admitted === true);
  }
  return Object.freeze({
    snapshot({ model, id, payload, nowMs, expiresIn, allowExpired = false }) {
      oauthCheck(['Session', 'Interaction'].includes(model), 'oauth_unavailable');
      oauthProviderId(id); oauthTime(nowMs);
      const value = snapshotOAuthJson(payload);
      const base = ['iat', 'exp', 'jti', 'kind'];
      oauthData(value, base.concat(model === 'Session'
        ? ['uid', 'acr', 'amr', 'accountId', 'loginTs', 'transient', 'state', 'authorizations']
        : ['cid', 'params', 'prompt', 'result', 'returnTo', 'session', 'grantId', 'lastSubmission', 'trusted']));
      oauthCheck(value.kind === model && value.jti === id);
      const createdAt = oauthSeconds(value.iat) * 1000, expiresAt = oauthSeconds(value.exp) * 1000;
      oauthCheck(createdAt <= nowMs && expiresAt > createdAt && expiresAt - createdAt <= 600000
        && (allowExpired || expiresAt > nowMs));
      if (expiresIn !== undefined) oauthCheck(typeof expiresIn === 'number' && Number.isFinite(expiresIn) && expiresIn > 0 && expiresIn <= 600);
      if (model === 'Session') {
        oauthProviderId(value.uid);
        optional(value, 'accountId', oauthId); optional(value, 'loginTs', oauthSeconds); optional(value, 'transient', boolean);
        optional(value, 'acr', item => oauthString(item, 160)); optional(value, 'amr', strings);
        if (value.state !== undefined) oauthData(value.state);
        if (value.authorizations !== undefined) {
          oauthData(value.authorizations, OAUTH_CLIENTS);
          for (const authorization of Object.values(value.authorizations)) {
            if (authorization === undefined) continue;
            oauthData(authorization, ['sid', 'grantId', 'persistsLogout']);
            optional(authorization, 'sid', oauthProviderId); optional(authorization, 'grantId', oauthProviderId);
            optional(authorization, 'persistsLogout', boolean);
            oauthCheck(authorization.grantId === undefined || value.accountId !== undefined);
          }
        }
      } else {
        oauthProviderId(value.cid);
        oauthData(value.params, ['client_id', 'redirect_uri', 'response_type', 'scope', 'resource', 'code_challenge', 'code_challenge_method', 'state', 'prompt']);
        const params = value.params;
        oauthCheck(OAUTH_CLIENTS.includes(params.client_id) && params.response_type === 'code' && params.scope === OAUTH_SCOPE
          && [resources.http, resources.mcp].includes(params.resource) && params.code_challenge_method === 'S256');
        oauthCheck(typeof params.code_challenge === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(params.code_challenge));
        redirect(params.client_id, params.redirect_uri);
        optional(params, 'state', item => oauthString(item, 512));
        optional(params, 'prompt', item => oauthCheck(['login', 'consent', 'login consent', 'consent login'].includes(item)));
        oauthData(value.prompt, ['name', 'reasons', 'details']); oauthId(value.prompt.name);
        strings(value.prompt.reasons, 32); oauthData(value.prompt.details);
        optional(value, 'result', result);
        optional(value, 'lastSubmission', result); optional(value, 'trusted', boolean); optional(value, 'grantId', oauthProviderId);
        oauthCheck(value.returnTo === `${issuer}/authorize/${id}`, 'oauth_invalid_artifact');
        if (value.session !== undefined) {
          oauthData(value.session, ['accountId', 'uid', 'cookie', 'acr', 'amr']); oauthId(value.session.accountId);
          optional(value.session, 'uid', oauthProviderId); optional(value.session, 'cookie', oauthProviderId);
          optional(value.session, 'acr', item => oauthString(item, 160)); optional(value.session, 'amr', strings);
        }
      }
      const json = canonicalOAuthJson(value);
      return { payload: JSON.parse(json), json, createdAt, expiresAt,
        retainUntil: Math.min(createdAt + 600000, Number.MAX_SAFE_INTEGER) };
    },
  });
}
