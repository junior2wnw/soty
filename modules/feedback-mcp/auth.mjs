import { DatabaseSync } from 'node:sqlite';
import { createHash, createCipheriv, createDecipheriv, randomBytes, generateKeyPairSync, timingSafeEqual } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { Provider, errors, interactionPolicy } from 'oidc-provider';
import { createSourceRpProtocol } from '../source-rp/server/protocol.mjs';
import { createSourceRpSessionService, sourceRpCipherBinding } from '../source-rp/server/session-service.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const random = () => randomBytes(32).toString('base64url');
const same = (a, b) => typeof a === 'string' && typeof b === 'string' && Buffer.byteLength(a) === Buffer.byteLength(b)
  && timingSafeEqual(Buffer.from(a), Buffer.from(b));
const escape = value => String(value).replace(/[&<>"']/gu, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
export class FeedbackAuthError extends Error {
  constructor(code = 'authentication_required', status = 401) { super(code); this.code = code; this.status = status; }
}
const check = (condition, code, status) => { if (!condition) throw new FeedbackAuthError(code, status); };
const safeURL = value => {
  try { const url = new URL(value); return url.protocol === 'https:' && !url.username && !url.password && !url.hash
    || url.protocol === 'http:' && ['127.0.0.1', '[::1]', 'localhost'].includes(url.hostname) && !url.username && !url.password && !url.hash; }
  catch { return false; }
};

/** Independent resource server and confidential HumanRP. Secrets and OAuth
 * records are encrypted at rest; lookup handles are SHA-256 digests. No Root
 * cookie, capability token, or unverified account assertion is accepted. */
export async function createFeedbackAuth(config, { service } = {}) {
  check(config && Object.keys(config).every(k => ['origin', 'dataDirectory', 'encryptionKey', 'cookieKeys', 'upstream', 'clients'].includes(k)), 'auth_configuration_invalid', 500);
  const { origin, dataDirectory, encryptionKey, cookieKeys, upstream, clients = [] } = config;
  check(safeURL(origin) && new URL(origin).origin === origin && typeof dataDirectory === 'string' && dataDirectory.length > 0
    && /^[A-Za-z0-9_-]{43}$/u.test(encryptionKey ?? '') && Buffer.from(encryptionKey, 'base64url').length === 32
    && Array.isArray(cookieKeys) && cookieKeys.length > 0 && cookieKeys.length <= 3
    && cookieKeys.every(key => typeof key === 'string' && key.length >= 43 && key.length <= 128)
    && upstream?.redirectUri === origin + '/auth/callback' && Array.isArray(clients) && clients.length <= 64
    && ['identity', 'actor'].every(k => typeof service?.[k] === 'function'), 'auth_configuration_invalid', 500);
  const issuer = origin + '/oauth', audience = origin + '/mcp', secure = origin.startsWith('https:'),
    cookieName = secure ? '__Host-feedback_session' : 'feedback_session', loginCookie = secure ? '__Host-feedback_login' : 'feedback_login',
    key = Buffer.from(encryptionKey, 'base64url'), keyId = hash(encryptionKey).slice(0, 24),
    profileDigest = hash(JSON.stringify([upstream.issuer, upstream.clientId, upstream.redirectUri, upstream.renewalProfile ?? null])),
    protocol = createSourceRpProtocol(upstream);
  mkdirSync(dataDirectory, { recursive: true, mode: 0o700 });
  const db = new DatabaseSync(join(dataDirectory, 'auth.sqlite'));
  db.exec('PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA max_page_count=32768;');
  check([0, 1].includes(db.prepare('PRAGMA user_version').get().user_version), 'auth_storage_version', 503);
  db.exec(`CREATE TABLE IF NOT EXISTS records(model TEXT NOT NULL,id TEXT NOT NULL,payload TEXT NOT NULL,
    expires INTEGER NOT NULL,grant_id TEXT,uid TEXT,consumed INTEGER,PRIMARY KEY(model,id)) STRICT;
    CREATE INDEX IF NOT EXISTS records_grant ON records(grant_id);
    CREATE INDEX IF NOT EXISTS records_uid ON records(model,uid);
    CREATE INDEX IF NOT EXISTS records_expiry ON records(expires); PRAGMA user_version=1;`);
  let closed = false;
  function encrypt(model, id, value) {
    const iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, iv);
    cipher.setAAD(Buffer.from(JSON.stringify([model, id, keyId])));
    return Buffer.concat([iv, cipher.update(JSON.stringify(value)), cipher.final(), cipher.getAuthTag()]).toString('base64url');
  }
  function decrypt(model, id, value, expectedKey = keyId) {
    try {
      check(expectedKey === keyId, 'auth_storage_unavailable', 503);
      const bytes = Buffer.from(value, 'base64url'), cipher = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
      cipher.setAAD(Buffer.from(JSON.stringify([model, id, keyId]))); cipher.setAuthTag(bytes.subarray(-16));
      return JSON.parse(Buffer.concat([cipher.update(bytes.subarray(12, -16)), cipher.final()]).toString());
    } catch { throw new FeedbackAuthError('auth_storage_unavailable', 503); }
  }
  const read = (model, id) => {
    check(!closed, 'auth_unavailable', 503);
    const row = db.prepare('SELECT * FROM records WHERE model=? AND id=? AND expires>?').get(model, hash(id), Date.now());
    if (!row) return undefined;
    const payload = decrypt(model, hash(id), row.payload);
    return row.consumed ? { ...payload, consumed: row.consumed } : payload;
  };
  function write(model, id, payload, expires) {
    check(Number.isSafeInteger(expires) && expires > Date.now(), 'auth_storage_invalid', 503);
    db.prepare(`INSERT INTO records VALUES(?,?,?,?,?,?,NULL) ON CONFLICT(model,id) DO UPDATE SET
      payload=excluded.payload,expires=excluded.expires,grant_id=excluded.grant_id,uid=excluded.uid`).run(model, hash(id),
      encrypt(model, hash(id), payload), expires, payload.grantId ? hash(payload.grantId) : null, payload.uid ? hash(payload.uid) : null);
  }
  const remove = (model, id) => db.prepare('DELETE FROM records WHERE model=? AND id=?').run(model, hash(id));
  const transaction = fn => { db.exec('BEGIN IMMEDIATE'); try { const result = fn(); db.exec('COMMIT'); return result; }
    catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; } };
  const gc = () => db.prepare('DELETE FROM records WHERE rowid IN (SELECT rowid FROM records WHERE expires<=? LIMIT 128)').run(Date.now());
  function session(id) { const row = read('Browser', id); check(row && row.marker.profileDigest === profileDigest); return row; }
  function capture(marker) {
    const row = session(marker.sessionIdHash);
    check(JSON.stringify(row.marker) === JSON.stringify(marker)); return { generation: row.generation };
  }
  const assertAuthority = (marker, captured) => check(capture(marker).generation === captured.generation);
  const writeHead = head => write('Renewal', head.sessionIdHash, head, head.sessionExpiresAt);
  const renewal = createSourceRpSessionService({ protocol, profileDigest, keyId, encrypt, decrypt,
    storagePort: {
      read: id => read('Renewal', id), captureSourceAuthority: capture, assertSourceAuthority: assertAuthority,
      claim: ({ marker, authority, expected, claimId, claimedAt }) => transaction(() => {
        assertAuthority(marker, authority); const head = read('Renewal', marker.sessionIdHash);
        if (!head || JSON.stringify(head) !== JSON.stringify(expected) || head.state !== 'idle') return false;
        writeHead({ ...head, state: 'refreshing', claimId, claimedAt, updatedAt: claimedAt }); return true;
      }),
      finish: ({ marker, authority, expected, claimId, proofCipher, accessExpiresAt, updatedAt }) => transaction(() => {
        assertAuthority(marker, authority); const head = read('Renewal', marker.sessionIdHash);
        if (!head || head.revision !== expected.revision || head.state !== 'refreshing' || head.claimId !== claimId
          || head.proofCipher !== expected.proofCipher || head.bindingDigest !== expected.bindingDigest) return false;
        writeHead({ ...head, revision: head.revision + 1, state: 'idle', claimId: null, claimedAt: null,
          lastAttemptId: claimId, proofCipher, accessExpiresAt, updatedAt }); return true;
      }),
      block: ({ expected, claimId, state, updatedAt }) => transaction(() => {
        const head = read('Renewal', expected.sessionIdHash);
        if (head?.revision === expected.revision && head.state === 'refreshing' && head.claimId === claimId)
          writeHead({ ...head, state, updatedAt });
      }),
    } });
  async function currentSession(id) {
    const row = session(id); let expiresAt, revision;
    try {
      if (row.renewable) {
        const proof = await renewal.currentProof(row.marker); expiresAt = proof.expiresAt; revision = proof.sessionGeneration;
      } else {
        check(row.proof.expiresAt > Date.now());
        check(await protocol.currentSubject(row.proof.accessToken, row.marker.subject) === row.marker.subject);
        expiresAt = row.proof.expiresAt;
      }
    } catch (error) {
      // Once revocation/expiry is known locally, previously branded actors
      // must fail too. A provider outage is not evidence of revocation.
      if (error.status === 401) transaction(() => { remove('Browser', id); remove('Renewal', id); });
      throw error;
    }
    const current = () => {
      if (closed || Date.now() >= expiresAt) return false;
      const live = read('Browser', id);
      if (!live || live.generation !== row.generation || live.marker.profileDigest !== profileDigest) return false;
      if (row.renewable) { const head = read('Renewal', id); return head?.revision === revision && head.state !== 'revoked'; }
      return true;
    };
    check(current()); return { row, current, expiresAt };
  }
  function cookies(req) {
    const result = new Map(); for (const part of String(req.headers.cookie ?? '').split(';')) {
      const index = part.indexOf('='); if (index < 0) continue;
      const name = part.slice(0, index).trim(); check(!result.has(name), 'invalid_cookie', 400); result.set(name, part.slice(index + 1).trim());
    } return result;
  }
  const cookie = (name, value, seconds) => `${name}=${value}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${seconds}${secure ? '; Secure' : ''}`;
  function browserId(req) { const value = cookies(req).get(cookieName); return /^[A-Za-z0-9_-]{43}$/u.test(value ?? '') ? hash(value) : null; }
  async function browserContext(req) {
    const id = browserId(req); if (!id) return null;
    try { const live = await currentSession(id); return { actor: service.actor(live.row.accountId, { current: live.current, expiresAt: live.expiresAt }),
      csrfToken: live.row.csrf, expiresAt: live.row.marker.sessionExpiresAt }; }
    catch (error) { if (error.status === 401) return null; throw error; }
  }
  function csrfHeaderCount(req, name) {
    if (!Array.isArray(req.rawHeaders)) return Object.hasOwn(req.headers, name) ? 1 : 0;
    let count = 0;
    for (let index = 0; index < req.rawHeaders.length; index += 2)
      if (req.rawHeaders[index].toLowerCase() === name) count++;
    return count;
  }
  function csrfProof(req, supplied, fromForm = false) {
    const id = browserId(req), row = id && read('Browser', id);
    check(row && csrfHeaderCount(req, 'origin') === 1 && req.headers.origin === origin
      && csrfHeaderCount(req, 'x-csrf-token') === 0
      && csrfHeaderCount(req, 'x-feedback-csrf') === (fromForm ? 0 : 1)
      && same(row.csrf, supplied), 'csrf_required', 403);
  }
  const assertCsrf = req => csrfProof(req, req.headers['x-feedback-csrf']);
  function validateClient(client) {
    check(client && Array.isArray(client.redirect_uris) && client.redirect_uris.length >= 1 && client.redirect_uris.length <= 8
      && client.redirect_uris.every(uri => typeof uri === 'string' && uri.length <= 2048 && safeURL(uri) && !uri.includes('*'))
      && ['none', 'client_secret_basic'].includes(client.token_endpoint_auth_method ?? 'none')
      && (!client.response_types || client.response_types.every(type => type === 'code'))
      && (!client.grant_types || client.grant_types.every(type => ['authorization_code', 'refresh_token'].includes(type)))
      && (!client.client_name || typeof client.client_name === 'string' && client.client_name.length <= 100), 'invalid_client_metadata', 400);
    return client;
  }
  clients.forEach(validateClient);
  class Adapter {
    constructor(model) { this.model = model; }
    async upsert(id, payload, expiresIn) {
      gc(); check(db.prepare('SELECT count(*) n FROM records').get().n < 20000, 'auth_capacity', 503);
      if (this.model === 'Client') {
        validateClient(payload); check(db.prepare("SELECT count(*) n FROM records WHERE model='Client'").get().n < 1024, 'auth_capacity', 503);
      }
      write(this.model, id, payload, Date.now() + Math.min(expiresIn ?? 30 * 86400, 30 * 86400) * 1000);
    }
    async find(id) { return read(this.model, id); }
    async findByUid(uid) {
      const row = db.prepare('SELECT id,payload FROM records WHERE model=? AND uid=? AND expires>?').get(this.model, hash(uid), Date.now());
      return row ? decrypt(this.model, row.id, row.payload) : undefined;
    }
    async findByUserCode() { return undefined; }
    async consume(id) {
      const result = db.prepare('UPDATE records SET consumed=? WHERE model=? AND id=? AND consumed IS NULL AND expires>?')
        .run(Math.floor(Date.now() / 1000), this.model, hash(id), Date.now());
      if (result.changes !== 1) throw new errors.InvalidGrant('authorization unavailable');
    }
    async destroy(id) { remove(this.model, id); }
    async revokeByGrantId(id) {
      transaction(() => { db.prepare('DELETE FROM records WHERE grant_id=?').run(hash(id)); remove('Grant', id); remove('GrantLink', id); });
    }
  }
  const signing = transaction(() => {
    const existing = read('Config', 'signing'); if (existing) return existing;
    // Only a new, empty auth store may create its signing identity. Losing this
    // record in an existing deployment requires deliberate operator recovery.
    check(db.prepare('SELECT count(*) n FROM records').get().n === 0, 'auth_signing_key_unavailable', 503);
    const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
    const initial = { keys: [{ ...privateKey.export({ format: 'jwk' }), use: 'sig', alg: 'RS256', kid: random() }] };
    write('Config', 'signing', initial, 8640000000000000); return initial;
  });
  const policy = interactionPolicy.base();
  // Every authorization is bound to a fresh HumanRP session and an explicit
  // consent. Provider SSO cookies alone cannot recreate revoked authority.
  policy.get('login').checks.add(new interactionPolicy.Check('feedback_current_identity', 'Current identity required', 'login_required',
    ctx => ctx.oidc.result?.login ? false : true));
  const provider = new Provider(issuer, {
    adapter: Adapter, clients, jwks: signing,
    cookies: { keys: cookieKeys, names: { session: 'feedback_oauth_session', interaction: 'feedback_oauth_interaction', resume: 'feedback_oauth_resume' },
      long: { httpOnly: true, secure, sameSite: 'lax', path: '/' }, short: { httpOnly: true, secure, sameSite: 'lax', path: '/' } },
    responseTypes: ['code'], scopes: ['openid', 'feedback'], claims: { openid: ['sub'] },
    clientAuthMethods: ['none', 'client_secret_basic'], pkce: { required: () => true },
    acceptQueryParamAccessTokens: false, clockTolerance: 0,
    clientBasedCORS: (_ctx, client, requestOrigin) => client.redirectUris.some(uri => new URL(uri).origin === requestOrigin),
    findAccount: async (ctx, accountId, token) => {
      const grantId = token?.grantId ?? ctx.oidc.result?.consent?.grantId;
      const binding = grantId && read('GrantLink', grantId); if (!binding || binding.accountId !== accountId) return undefined;
      try { const live = await currentSession(binding.sessionId); if (live.row.accountId !== accountId) return undefined; }
      catch (error) { if (error.status === 401) return undefined; throw new errors.OIDCProviderError(503, 'temporarily_unavailable'); }
      return { accountId, claims: async () => ({ sub: accountId }) };
    },
    loadExistingGrant: ctx => ctx.oidc.result?.consent?.grantId ? provider.Grant.find(ctx.oidc.result.consent.grantId) : undefined,
    features: { devInteractions: { enabled: false }, registration: { enabled: true }, clientIdMetadataDocument: { enabled: false },
      userinfo: { enabled: false }, introspection: { enabled: false }, rpInitiatedLogout: { enabled: false },
      pushedAuthorizationRequests: { enabled: false }, requestObjects: { enabled: false }, dPoP: { enabled: false },
      revocation: { enabled: true, allowedPolicy: (_ctx, client, token) => client.clientId === token.clientId },
      resourceIndicators: { enabled: true, defaultResource: () => audience, useGrantedResource: () => true,
        getResourceServerInfo: (_ctx, resource) => {
          if (resource !== audience) throw new errors.InvalidTarget();
          return { scope: 'feedback', audience, accessTokenFormat: 'opaque', accessTokenTTL: 300 };
        } } },
    ttl: { Session: 86400, Interaction: 600, Grant: 86400, AuthorizationCode: 60, AccessToken: 300, IdToken: 60, RefreshToken: 86400 },
    issueRefreshToken: (_ctx, client) => client.grantTypeAllowed('refresh_token'), rotateRefreshToken: true, revokeGrantPolicy: () => true,
    expiresWithSession: () => false,
    interactions: { policy, url: (_ctx, interaction) => `${origin}/auth/interaction/${interaction.uid}` },
    routes: { authorization: '/authorize', resume: '/authorize/:uid', token: '/token', jwks: '/jwks', revocation: '/revoke', registration: '/register' },
    renderError: async ctx => { ctx.status = ctx.status >= 400 ? ctx.status : 400; ctx.body = { error: 'authorization_failed' }; },
  });
  provider.on('server_error', () => {}); provider.on('error', () => {});
  // Reverse proxy must overwrite forwarded headers; the host handler validates
  // canonical Host. TLS terminates on that configured origin in production.
  provider.proxy = secure;
  const buckets = new Map();
  function budget(req, limit) {
    const bucketKey = hash(String(req.socket.remoteAddress) + ':' + new URL(req.url, origin).pathname), minute = Math.floor(Date.now() / 60000);
    let row = buckets.get(bucketKey); if (!row || row.minute !== minute) { row = { minute, count: 0 }; buckets.set(bucketKey, row); }
    check(++row.count <= limit && buckets.size <= 4096, 'rate_limit', 429);
    if (buckets.size > 1024) for (const [id, value] of buckets) if (value.minute < minute) buckets.delete(id);
  }
  async function body(req, json = false) {
    check(String(req.headers['content-type'] ?? '').split(';')[0] === (json ? 'application/json' : 'application/x-www-form-urlencoded'), 'invalid_body', 400);
    check(req.headers['content-length'] === undefined || /^\d{1,6}$/u.test(req.headers['content-length'])
      && Number(req.headers['content-length']) <= 16384, 'body_too_large', 413);
    let data = ''; for await (const chunk of req) { data += chunk; check(Buffer.byteLength(data) <= 16384, 'body_too_large', 413); }
    try { if (json) return JSON.parse(data); const params = new URLSearchParams(data);
      check([...params.keys()].length === new Set(params.keys()).size, 'invalid_body', 400); return Object.fromEntries(params); }
    catch { throw new FeedbackAuthError('invalid_body', 400); }
  }
  const json = (res, value, status = 200) => { res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(value)); };
  const redirect = (res, location) => { res.writeHead(303, { location, 'cache-control': 'no-store' }); res.end(); };
  async function authenticateBearer(header, options = {}) {
    check(options.audience === undefined || options.audience === audience);
    check(typeof header === 'string' && /^Bearer [A-Za-z0-9_-]{32,256}$/u.test(header));
    const value = header.slice(7), token = await provider.AccessToken.find(value);
    check(token && token.aud === audience && token.scope?.split(' ').includes('feedback') && !token.isExpired);
    const binding = read('GrantLink', token.grantId); check(binding && binding.accountId === token.accountId && binding.clientId === token.clientId);
    const live = await currentSession(binding.sessionId); check(live.row.accountId === token.accountId);
    const expiresAt = Math.min(live.expiresAt, token.exp * 1000);
    const current = () => live.current() && Boolean(read('AccessToken', value))
      && Boolean(read('Grant', token.grantId)) && Boolean(read('GrantLink', token.grantId));
    check(current()); return service.actor(token.accountId, { expiresAt, current });
  }
  async function handleRequest(req, res) {
    const url = new URL(req.url, origin), path = url.pathname;
    if (!path.startsWith('/auth/') && !path.startsWith('/oauth/') && !path.startsWith('/.well-known/')) return false;
    try {
      check(req.headers.host === new URL(origin).host, 'invalid_host', 400); budget(req, path === '/oauth/register' ? 10 : 120); gc();
      res.setHeader('cache-control', 'no-store'); res.setHeader('referrer-policy', 'no-referrer'); res.setHeader('x-content-type-options', 'nosniff');
      if (['/.well-known/oauth-protected-resource', '/.well-known/oauth-protected-resource/mcp'].includes(path) && req.method === 'GET') {
        res.setHeader('access-control-allow-origin', '*');
        json(res, { resource: audience, authorization_servers: [issuer], scopes_supported: ['feedback'], bearer_methods_supported: ['header'] }); return true;
      }
      if (['/.well-known/oauth-authorization-server/oauth', '/oauth/.well-known/oauth-authorization-server'].includes(path) && req.method === 'GET') {
        res.setHeader('access-control-allow-origin', '*');
        json(res, { issuer, authorization_endpoint: issuer + '/authorize', token_endpoint: issuer + '/token', registration_endpoint: issuer + '/register',
          revocation_endpoint: issuer + '/revoke', response_types_supported: ['code'], grant_types_supported: ['authorization_code', 'refresh_token'],
          code_challenge_methods_supported: ['S256'], token_endpoint_auth_methods_supported: ['none', 'client_secret_basic'], scopes_supported: ['feedback'] }); return true;
      }
      if (path === '/auth/login' && req.method === 'GET') {
        const next = url.searchParams.get('returnTo') ?? '/';
        check(next === '/' || /^\/auth\/interaction\/[A-Za-z0-9_-]{1,256}$/u.test(next), 'invalid_return_path', 400);
        const intent = await protocol.start(), binding = random();
        write('Login', intent.state, { ...intent, binding: hash(binding), next }, Date.now() + 300000);
        res.setHeader('set-cookie', cookie(loginCookie, binding, 300)); redirect(res, intent.location); return true;
      }
      if (path === '/auth/callback' && req.method === 'GET') {
        const state = url.searchParams.get('state'); check(typeof state === 'string' && state.length <= 256);
        const intent = transaction(() => { const value = read('Login', state); check(value && same(value.binding, hash(cookies(req).get(loginCookie) ?? '')));
          remove('Login', state); return value; });
        const proof = await protocol.exchange(url, intent), raw = random(), id = hash(raw), createdAt = Date.now(),
          expiresAt = proof.refreshToken ? createdAt + 86400000 : proof.expiresAt,
          accountId = service.identity(proof.issuer, proof.subject), marker = { sessionIdHash: id, profileDigest, bindingDigest: hash(accountId),
            issuer: proof.issuer, subject: proof.subject, sessionExpiresAt: expiresAt, createdAt }, csrf = random();
        transaction(() => {
          const old = browserId(req); if (old) { remove('Browser', old); remove('Renewal', old); }
          write('Browser', id, { marker, accountId, csrf, generation: 1, renewable: Boolean(proof.refreshToken),
            ...(!proof.refreshToken ? { proof } : {}) }, expiresAt);
          if (proof.refreshToken) writeHead({ sessionIdHash: id, profileDigest, bindingDigest: marker.bindingDigest, sessionExpiresAt: expiresAt,
            accessExpiresAt: Math.min(proof.expiresAt, expiresAt), revision: 0, state: 'idle',
            proofCipher: encrypt('SourceRpRenewal', sourceRpCipherBinding(marker, 0), { accessToken: proof.accessToken, refreshToken: proof.refreshToken, nonce: proof.nonce }),
            keyId, claimId: null, claimedAt: null, lastAttemptId: null, createdAt, updatedAt: createdAt });
        });
        res.setHeader('set-cookie', [cookie(cookieName, raw, Math.floor((expiresAt - createdAt) / 1000)), cookie(loginCookie, '', 0)]);
        redirect(res, intent.next); return true;
      }
      if (path === '/auth/session' && req.method === 'GET') {
        const context = await browserContext(req); json(res, context ? { authenticated: true, accountId: context.actor.accountId,
          csrfToken: context.csrfToken, expiresAt: context.expiresAt } : { authenticated: false }); return true;
      }
      if (path === '/auth/logout' && req.method === 'POST') {
        assertCsrf(req); const id = browserId(req); transaction(() => { remove('Browser', id); remove('Renewal', id); });
        res.setHeader('set-cookie', cookie(cookieName, '', 0)); json(res, { ok: true }); return true;
      }
      if (/^\/auth\/interaction\/[A-Za-z0-9_-]{1,256}$/u.test(path) && ['GET', 'POST'].includes(req.method)) {
        const details = await provider.interactionDetails(req, res); check(path.endsWith('/' + details.uid), 'invalid_interaction', 400);
        const context = await browserContext(req);
        if (!context) { check(req.method === 'GET'); redirect(res, '/auth/login?returnTo=' + encodeURIComponent(path)); return true; }
        const client = await provider.Client.find(details.params.client_id); check(client, 'invalid_client', 400);
        if (req.method === 'GET') {
          const redirectUri = details.params.redirect_uri;
          check(typeof redirectUri === 'string' && safeURL(redirectUri) && client.redirectUriAllowed(redirectUri), 'invalid_redirect_uri', 400);
          // Chromium applies form-action to the entire POST -> resume -> callback
          // redirect chain. Permit only this client's validated callback origin.
          const redirectOrigin = new URL(redirectUri).origin;
          res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'content-security-policy': `default-src 'none';base-uri 'none';frame-ancestors 'none';form-action 'self' ${redirectOrigin}` });
          res.end(`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="referrer" content="same-origin"><title>Доступ к отзывам</title><h1>Подключить ${escape(client.clientName ?? client.clientId)}?</h1><p>Приложение получит доступ к вашим проектам и отзывам: чтение, создание и изменения.</p><p>Адрес возврата: ${escape(details.params.redirect_uri)}</p><form method="post"><input type="hidden" name="csrf" value="${escape(context.csrfToken)}"><button name="decision" value="approve">Разрешить</button><button name="decision" value="deny">Отказать</button></form></html>`); return true;
        }
        const data = await body(req); csrfProof(req, data.csrf, true); check(['approve', 'deny'].includes(data.decision), 'invalid_decision', 400);
        if (data.decision === 'deny') { await provider.interactionFinished(req, res, { error: 'access_denied' }, { mergeWithLastSubmission: false }); return true; }
        const live = await currentSession(browserId(req)); check(live.row.accountId === context.actor.accountId);
        const grant = new provider.Grant({ accountId: context.actor.accountId, clientId: client.clientId });
        grant.addResourceScope(audience, 'feedback'); grant.addOIDCScope('feedback');
        if (String(details.params.scope).split(' ').includes('openid')) grant.addOIDCScope('openid');
        const grantId = await grant.save();
        write('GrantLink', grantId, { sessionId: browserId(req), accountId: context.actor.accountId, clientId: client.clientId }, live.row.marker.sessionExpiresAt);
        await provider.interactionFinished(req, res, { login: { accountId: context.actor.accountId, remember: true }, consent: { grantId } }, { mergeWithLastSubmission: false }); return true;
      }
      if (path.startsWith('/oauth/')) {
        if (path === '/oauth/register' && req.method === 'POST') {
          const data = validateClient(await body(req, true));
          check(Object.keys(data).every(k => ['client_name', 'redirect_uris', 'response_types', 'grant_types', 'token_endpoint_auth_method', 'scope'].includes(k))
            && (!data.scope || data.scope === 'feedback' || data.scope === 'openid feedback'), 'invalid_client_metadata', 400);
          req.body = { ...data, token_endpoint_auth_method: data.token_endpoint_auth_method ?? 'none',
            response_types: ['code'], grant_types: data.grant_types ?? ['authorization_code', 'refresh_token'], scope: data.scope ?? 'feedback' };
        } else if (req.method === 'POST') req.body = await body(req);
        // Never derive issuer links from untrusted forwarding headers, even if
        // a deployment's reverse proxy accidentally forwards client values.
        req.headers['x-forwarded-host'] = new URL(origin).host;
        req.headers['x-forwarded-proto'] = secure ? 'https' : 'http';
        const original = req.url, originalUrl = req.originalUrl; req.originalUrl = original; req.url = req.url.slice('/oauth'.length);
        try { await provider.callback()(req, res); } finally { req.url = original; req.originalUrl = originalUrl; }
        return true;
      }
      json(res, { error: 'not_found' }, 404); return true;
    } catch (error) {
      if (!res.headersSent && !res.destroyed) json(res, { error: error instanceof FeedbackAuthError ? error.code : 'authorization_failed' },
        Number.isInteger(error.status) && error.status >= 400 && error.status <= 599 ? error.status : 503);
      return true;
    }
  }
  return Object.freeze({ handleRequest, authenticateBearer, browserContext, browserActor: async req => (await browserContext(req))?.actor ?? null,
    assertCsrf, close() { closed = true; db.close(); key.fill(0); } });
}
