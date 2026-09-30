import { AsyncLocalStorage } from 'node:async_hooks';
import { Provider, errors } from 'oidc-provider';
import { OAUTH_SCOPE } from './capabilities-oauth-profile.js';
import { createOAuthIngress, OAuthIngressError } from './capabilities-oauth-ingress.js';
import { AccessError } from '../modules/capabilities/server/validation.mjs';

const epoch = () => Math.floor(Date.now() / 1000);
const boundedModels = new Set(['Grant', 'AuthorizationCode', 'RefreshToken', 'AccessToken']);
const tokenModels = new Set(['AuthorizationCode', 'RefreshToken', 'AccessToken']);
const storedModels = new Set(['Session', 'Interaction', ...boundedModels]);
const failGrant = () => { throw new errors.InvalidGrant('connection unavailable'); };
function domainCall(action, { grantBinding = false } = {}) {
  try { return action(); }
  catch (error) {
    if (!(error instanceof AccessError)) throw new errors.OIDCProviderError(500, 'server_error');
    if (grantBinding && error.code === 'oauth_grant_conflict') throw error;
    if (['oauth_unavailable', 'oauth_storage_key_unavailable', 'oauth_quota_exceeded', 'service_closed',
      'capabilities_storage_corrupt', 'oauth_storage_busy', 'connect_authority_busy', 'native_storage_busy'].includes(error.code)) {
      throw new errors.OIDCProviderError(503, 'temporarily_unavailable');
    }
    if (error.code === 'oauth_invalid_target') throw new errors.InvalidTarget('resource mismatch');
    throw new errors.InvalidGrant('connection unavailable');
  }
}

/** Request bindings come only from the pinned provider's authenticated context,
 * not raw body/client_id or a caller-supplied imitation of the context. */
function requestBinding() {
  const current = Provider.ctx?.oidc;
  if (!current?.client) return undefined;
  const params = current.params || {};
  return Object.freeze({ clientId: current.client.clientId,
    ...Object.fromEntries([['resource', params.resource], ['scope', params.scope], ['grantType', params.grant_type]]
      .filter(([, value]) => value !== undefined)) });
}

/** Every HTML document that posts into the native callback redirect chain needs
 * the same exact form destination. Keep the provider's script hash and every
 * other shell directive intact. Never admit a callback from an unchecked URL. */
export function oauthCallbackPolicy(profile, policy, parameters) {
  if (!profile.isRegisteredRedirect({ clientId: parameters?.client_id, redirectUri: parameters?.redirect_uri })
    || typeof policy !== 'string') throw new OAuthIngressError('invalid_request');
  const directives = policy.split(';');
  const forms = directives.filter(item => /^form-action(?:\s|$)/u.test(item.trim()));
  if (forms.length !== 1 || forms[0].trim() !== "form-action 'self'") throw new OAuthIngressError('invalid_request');
  return directives.map(item => item.trim() === "form-action 'self'"
    ? `form-action 'self' ${parameters.redirect_uri}` : item).join(';');
}

export function createSotyOAuthProvider({ profile, oauth }) {
  if (!profile?.enabled || oauth?.readiness().available !== true) throw new errors.TemporarilyUnavailable('authorization unavailable');
  const staged = new AsyncLocalStorage(), ingress = createOAuthIngress();
  const keyConfiguration = profile.providerKeys();
  class Adapter {
    constructor(model) { this.model = model; }
    async upsert(id, payload, expiresIn) {
      if (!storedModels.has(this.model)) throw new errors.InvalidRequest('unsupported artifact');
      domainCall(() => oauth.artifactStore.upsert({ model: this.model, id, payload, expiresIn,
        ...(tokenModels.has(this.model) ? { request: requestBinding() } : {}),
        ...(this.model === 'Grant' ? { stagedGrant: staged.getStore()?.context } : {}) }), { grantBinding: this.model === 'Grant' });
    }
    async find(id) {
      if (this.model === 'Client') return undefined;
      if (!storedModels.has(this.model)) throw new errors.InvalidRequest('unsupported artifact');
      return domainCall(() => oauth.artifactStore.find({ model: this.model, id }));
    }
    async findByUid(uid) { return this.model === 'Session' ? domainCall(() => oauth.artifactStore.findByUid({ uid })) : undefined; }
    async findByUserCode() { return undefined; }
    async consume(id) {
      const result = domainCall(() => oauth.artifactStore.consume({ model: this.model, id, request: requestBinding() }));
      if (result.status === 'invalid_target') throw new errors.InvalidTarget('resource mismatch');
      if (result.status !== 'consumed') failGrant();
    }
    async destroy(id) { domainCall(() => oauth.artifactStore.destroy({ model: this.model, id })); }
    async revokeByGrantId(providerGrantId) { domainCall(() => oauth.artifactStore.revokeByGrantId({ providerGrantId })); }
  }
  const shortSessionTTL = (_ctx, session) => {
    const remaining = (Number.isSafeInteger(session.iat) ? session.iat : epoch()) + 600 - epoch();
    if (remaining <= 0) throw new errors.SessionNotFound('short session expired');
    return Math.min(600, remaining);
  };
  const grantTTL = (_ctx, grant) => {
    const binding = staged.getStore();
    const end = binding ? Math.floor(binding.connection.expiresAt / 1000) : grant.exp;
    const remaining = end - epoch();
    if (!Number.isSafeInteger(remaining) || remaining <= 0) failGrant();
    return remaining;
  };
  const tokenTTL = maximum => ctx => {
    const remaining = ctx?.oidc?.grant?.exp - epoch();
    if (!Number.isSafeInteger(remaining) || remaining <= 0) failGrant();
    return Math.min(maximum, remaining);
  };
  const cookiePrefix = profile.secure ? '__Secure-soty_oauth_' : 'soty_oauth_';
  const provider = new Provider(profile.issuer, {
    adapter: Adapter, clients: profile.clients(), jwks: keyConfiguration.jwks,
    cookies: { keys: keyConfiguration.cookieKeys,
      names: { session: cookiePrefix + 'session', interaction: cookiePrefix + 'interaction', resume: cookiePrefix + 'resume' },
      long: { httpOnly: true, secure: profile.secure, sameSite: 'lax', path: '/oauth', maxAge: 600000 },
      short: { httpOnly: true, secure: profile.secure, sameSite: 'lax', path: '/oauth', maxAge: 600000 } },
    responseTypes: ['code'], scopes: [], claims: {}, pkce: { required: () => true },
    clockTolerance: 0, acceptQueryParamAccessTokens: false, clientBasedCORS: () => false,
    findAccount: async (_ctx, accountId) => ({ accountId, claims: async () => ({ sub: accountId }) }),
    // A previous browser session is not consent to a new connection. Only the
    // current interaction's signed decision supplies the grant on resume.
    loadExistingGrant: async ctx => ctx.oidc.result?.consent?.grantId
      ? provider.Grant.find(ctx.oidc.result.consent.grantId) : undefined,
    features: { devInteractions: { enabled: false }, registration: { enabled: false },
      clientIdMetadataDocument: { enabled: false }, userinfo: { enabled: false }, introspection: { enabled: false },
      rpInitiatedLogout: { enabled: false },
      pushedAuthorizationRequests: { enabled: false }, requestObjects: { enabled: false }, dPoP: { enabled: false },
      revocation: { enabled: true, allowedPolicy: (_ctx, client, token) => client.clientId === token.clientId },
      resourceIndicators: { enabled: true, getResourceServerInfo(_ctx, resource) {
        if (!Object.values(profile.resources).includes(resource)) throw new errors.InvalidTarget('resource mismatch');
        return { scope: OAUTH_SCOPE, accessTokenFormat: 'opaque', accessTokenTTL: 300 };
      } } },
    ttl: { Session: shortSessionTTL, Interaction: 600, Grant: grantTTL,
      AccessToken: tokenTTL(300), AuthorizationCode: tokenTTL(60), RefreshToken: tokenTTL(86400) },
    issueRefreshToken: () => true, rotateRefreshToken: true, revokeGrantPolicy: () => true,
    expiresWithSession: () => false,
    routes: { authorization: '/authorize', resume: '/authorize/:uid', token: '/token', revocation: '/revoke', jwks: '/jwks',
      end_session: '/session/end' },
    interactions: { url: (_ctx, interaction) => `${profile.issuer}/interaction/${interaction.uid}` },
    renderError: async ctx => {
      ctx.type = 'html'; ctx.set('Cache-Control', 'no-store');
      ctx.body = '<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Подключение — Соты</title><main><h1>Не удалось подключиться</h1><p>Вернитесь в клиент и начните подключение ещё раз.</p><a href="/">Открыть Соты</a></main></html>';
    },
  });
  provider.on('server_error', () => {}); // The HTTP boundary returns a safe code; never log raw request/token errors.
  provider.on('error', () => {}); // Also suppress Koa's separate default raw-error logger.
  provider.use(async (ctx, next) => {
    let lease;
    ctx.set('Cache-Control', 'no-store'); ctx.set('Pragma', 'no-cache');
    try {
      lease = ingress.enter(ctx.req, ctx.res);
      if (ctx.method === 'POST') ctx.req.body = await lease.readForm();
      await next();
      // The maintained provider uses an internal autoform when the selected
      // account differs from its short AS session. That new HTML document has
      // its own CSP, independent of the preceding Soty consent page.
      if (ctx.oidc?.route === 'resume' && ctx.status === 200 && ctx.type === 'text/html'
        && ctx.oidc.entities.Interaction) {
        ctx.set('Content-Security-Policy', oauthCallbackPolicy(profile,
          ctx.response.get('Content-Security-Policy'), ctx.oidc.entities.Interaction.params));
      }
    } catch (error) {
      if (ctx.res.destroyed || ctx.res.headersSent) return;
      const code = error instanceof OAuthIngressError ? error.code : '';
      const status = { payload_too_large: 413, request_timeout: 408, unsupported_media_type: 415,
        unsupported_encoding: 415, rate_limit: 429, temporarily_unavailable: 503, invalid_request: 400 }[code] || 500;
      if (!ctx.req.complete || !ctx.req.readableEnded) { ctx.res.shouldKeepAlive = false; ctx.set('Connection', 'close'); }
      ctx.set('Cache-Control', 'no-store'); ctx.set('Pragma', 'no-cache');
      if (status === 429 || status === 503) ctx.set('Retry-After', '60');
      ctx.status = status; ctx.type = 'application/json';
      ctx.body = { error: status >= 500 || status === 429 ? 'temporarily_unavailable' : 'invalid_request' };
    } finally { lease?.release(); }
  });

  async function bindGrant({ interactionId, browserNonce }) {
    for (let attempt = 0; attempt < 2; attempt++) {
      const binding = oauth.beginGrantBinding({ interactionId, browserNonce });
      try {
        if (binding.providerGrantId) {
          const grant = await provider.Grant.find(binding.providerGrantId);
          if (!grant) failGrant();
          return { grantId: binding.providerGrantId, accountId: binding.connection.accountId };
        }
        const grant = new provider.Grant({ accountId: binding.connection.accountId, clientId: binding.connection.staticClientId });
        grant.addResourceScope(binding.connection.resource, OAUTH_SCOPE);
        const grantId = await staged.run(binding, () => grant.save());
        return { grantId, accountId: binding.connection.accountId };
      } catch (error) {
        if (error?.code !== 'oauth_grant_conflict' || attempt !== 0) throw error;
      } finally { oauth.endGrantBinding(binding.context); }
    }
    failGrant();
  }
  return Object.freeze({ provider, ingress, bindGrant });
}
