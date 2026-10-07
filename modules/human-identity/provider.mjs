import { AsyncLocalStorage } from 'node:async_hooks';
import { Provider, errors, interactionPolicy } from 'oidc-provider';
import { HumanIdentityError } from './profile.mjs';
import { createOAuthIngress, OAuthIngressError } from '../../server/capabilities-oauth-ingress.js';

const FAILURE = '<!doctype html><html lang="ru"><meta charset="utf-8"><title>Вход в Соты</title><p>Вход сейчас недоступен. Вернитесь в Соты и повторите действие.</p></html>';
function domain(action) {
  try { return action(); }
  catch (error) {
    if (error instanceof HumanIdentityError && error.status < 500 && error.status !== 429) throw new errors.InvalidGrant('identity unavailable');
    throw new errors.OIDCProviderError(503, 'temporarily_unavailable');
  }
}

/** Maintained OIDC engine; the signed Connect bridge is a private interaction authentication port. */
export function createHumanIdentityProvider({ profile, service, ingressLimits = {}, userinfoLimits = {} }) {
  if (!profile?.enabled || !service) throw new HumanIdentityError('human_identity_disabled', 503);
  const staged = new AsyncLocalStorage(), requests = new AsyncLocalStorage(),
    ingress = createOAuthIngress({ limits: ingressLimits, userinfoBudget: service.userinfoBudget, userinfoLimits }), providerKeys = profile.providerKeys();
  const tokenRequest = () => { const ctx = Provider.ctx?.oidc; return ctx?.client ? Object.freeze({ clientId: ctx.client.clientId, grantType: ctx.params?.grant_type }) : undefined; };
  class Adapter {
    constructor(model) { this.model = model; }
    async upsert(id, payload, expiresIn) {
      domain(() => service.sdk.upsert({ model: this.model, id, payload, expiresIn,
        ...(this.model === 'Grant' ? { approvedBinding: staged.getStore() } : {}),
        ...(this.model === 'Interaction' ? { browserNonce: requests.getStore()?.browserNonce } : {}),
        clientId: Provider.ctx?.oidc?.client?.clientId, request: tokenRequest() }));
    }
    async find(id) { return this.model === 'Client' ? undefined : domain(() => service.sdk.find(this.model, id)); }
    async findByUid(uid) { return this.model === 'Session' ? domain(() => service.sdk.findByUid(uid)) : undefined; }
    async findByUserCode() { return undefined; }
    async consume(id) { const result = domain(() => service.sdk.consume(this.model, id, tokenRequest()));
      if (this.model === 'RefreshToken') {
        if (result?.status !== 'consumed') throw new errors.InvalidGrant('human session unavailable');
        const context = requests.getStore(), grantId = Provider.ctx?.oidc?.entities?.RefreshToken?.grantId;
        if (!context || typeof grantId !== 'string') throw new errors.OIDCProviderError(503, 'temporarily_unavailable');
        context.rotatedGrantId = grantId;
      }
    }
    async destroy(id) { domain(() => service.sdk.destroy(this.model, id)); }
    async revokeByGrantId(id) { domain(() => service.sdk.revokeByGrantId(id)); }
  }
  const policy = interactionPolicy.base();
  policy.get('login').checks.add(new interactionPolicy.Check('connect_device_proof', 'Current Soty device proof is required', 'login_required',
    ctx => ctx.oidc.result?.login ? interactionPolicy.Check.NO_NEED_TO_PROMPT : interactionPolicy.Check.REQUEST_PROMPT));
  const prefix = (profile.secure ? '__Host-' : '') + 'soty_human_';
  const provider = new Provider(profile.issuer, {
    adapter: Adapter, clients: profile.providerClients(), jwks: providerKeys.jwks,
    // The maintained SDK pins resume to /authorize/:uid after short options.
    // __Host- requires Path=/ and Chrome would reject that scoped cookie.
    // Keep the SDK's narrow path, Secure and absent Domain; other cookies stay __Host-.
    cookies: { keys: providerKeys.cookieKeys, names: { session: prefix + 'session', interaction: prefix + 'interaction',
      resume: (profile.secure ? '__Secure-' : '') + 'soty_human_resume' },
      long: { httpOnly: true, secure: profile.secure, sameSite: 'lax', path: profile.secure ? '/' : '/human-identity', maxAge: 600000 },
      short: { httpOnly: true, secure: profile.secure, sameSite: 'lax', path: profile.secure ? '/' : '/human-identity', maxAge: 300000 } },
    responseTypes: ['code'], scopes: ['openid', 'profile'], claims: { openid: ['sub'], profile: ['name', 'preferred_username'] },
    clientAuthMethods: ['client_secret_basic'], pkce: { required: () => true },
    subjectTypes: ['public'], clockTolerance: 0, acceptQueryParamAccessTokens: false, clientBasedCORS: () => false,
    findAccount: async (ctx, accountId, token) => {
      const current = domain(() => service.accountClaims({ accountId,
        grantId: token?.grantId || ctx.oidc.result?.consent?.grantId || ctx.oidc.grant?.jti, sessionUid: ctx.oidc.session?.uid }));
      return current ? { accountId, claims: async () => current } : undefined;
    },
    loadExistingGrant: async ctx => ctx.oidc.result?.consent?.grantId ? provider.Grant.find(ctx.oidc.result.consent.grantId) : undefined,
    features: { devInteractions: { enabled: false }, registration: { enabled: false }, clientIdMetadataDocument: { enabled: false },
      userinfo: { enabled: true }, introspection: { enabled: false }, rpInitiatedLogout: { enabled: false },
      pushedAuthorizationRequests: { enabled: false }, requestObjects: { enabled: false }, dPoP: { enabled: false },
      revocation: { enabled: true, allowedPolicy: (_ctx, client, token) => client.clientId === token.clientId } },
    ttl: service.renewalEnabled ? { Session: 600, Interaction: 300, Grant: () => service.renewal.grantTTL(staged.getStore()),
      AuthorizationCode: 60, AccessToken: ctx => {
        const grantId = ctx.oidc.grant?.jti, clientId = ctx.oidc.client?.clientId;
        return service.renewal.issueRefreshToken(grantId, clientId) ? Math.min(300, service.renewal.remaining(grantId, clientId)) : 300;
      }, IdToken: 60, RefreshToken: ctx => service.renewal.remaining(ctx.oidc.grant.jti, ctx.oidc.client.clientId) }
      : { Session: 600, Interaction: 300, Grant: 600, AuthorizationCode: 60, AccessToken: 300, IdToken: 60 },
    issueRefreshToken: (_ctx, client, source) => service.renewalEnabled && client.grantTypeAllowed('refresh_token')
      && service.renewal.issueRefreshToken(source.grantId, client.clientId),
    ...(service.renewalEnabled ? { rotateRefreshToken: true, revokeGrantPolicy: () => true } : {}), expiresWithSession: () => false,
    routes: { authorization: '/authorize', resume: '/authorize/:uid', token: '/token', userinfo: '/userinfo', jwks: '/jwks',
      revocation: '/revoke', end_session: '/session/end' },
    interactions: { policy, url: (_ctx, interaction) => `${profile.issuer}/interaction/${interaction.uid}` },
    renderError: async ctx => { ctx.type = 'html'; ctx.set('Cache-Control', 'no-store');
      ctx.set('Content-Security-Policy', "default-src 'none';frame-ancestors 'none';base-uri 'none'"); ctx.body = FAILURE; },
  });
  provider.on('server_error', () => {}); provider.on('error', () => {});
  provider.use(async (ctx, next) => {
    let lease; ctx.set('Cache-Control', 'no-store'); ctx.set('Pragma', 'no-cache');
    if (!ctx.response.get('Content-Security-Policy')) ctx.set('Content-Security-Policy',
      "default-src 'none';script-src 'self';base-uri 'none';form-action 'self';frame-ancestors 'self'");
    try {
      const budgetReference = service.userinfoBudget?.capture(ctx.req);
      lease = ingress.enter(ctx.req, ctx.res, budgetReference); if (ctx.method === 'POST') ctx.req.body = await lease.readForm();
      await next();
      // Adapter commits do not make the whole SDK pipeline atomic. A failure
      // after consume closes this family; its consumed RT tombstone is retained.
      if (ctx.status >= 400 && requests.getStore()?.rotatedGrantId) domain(() => service.sdk.revokeByGrantId(requests.getStore().rotatedGrantId));
      if (ctx.oidc?.route === 'resume' && ctx.status === 200 && ctx.type === 'text/html' && ctx.oidc.entities.Interaction) {
        const parameters = ctx.oidc.entities.Interaction.params;
        if (!profile.isRegisteredRedirect(parameters.client_id, parameters.redirect_uri)) throw new HumanIdentityError('human_identity_client_mismatch', 403);
        const policy = ctx.response.get('Content-Security-Policy'), directives = policy.split(';');
        const forms = directives.filter(part => /^form-action(?:\s|$)/u.test(part.trim()));
        if (forms.length !== 1 || forms[0].trim() !== "form-action 'self'") throw new HumanIdentityError('human_identity_invalid', 500);
        // The SDK account-switch document performs a native POST redirect chain.
        // Keep its script hash, preserve a same-origin Origin and admit only the registered RP callback.
        ctx.set('Referrer-Policy', 'same-origin');
        ctx.set('Content-Security-Policy', directives.map(part => part.trim() === "form-action 'self'"
          ? `form-action 'self' ${parameters.redirect_uri}` : part).join(';'));
      }
    } catch (error) {
      if (requests.getStore()?.rotatedGrantId) {
        try { domain(() => service.sdk.revokeByGrantId(requests.getStore().rotatedGrantId)); } catch { /* A failed store cannot mint a successful response. */ }
      }
      if (ctx.res.headersSent || ctx.res.destroyed) return;
      ctx.status = error instanceof OAuthIngressError && error.code === 'rate_limit' ? 429 : 503;
      ctx.body = { error: 'temporarily_unavailable' };
    } finally { lease?.release(); }
  });
  async function finish(req, res, approval) {
    if (approval.decision === 'denied') return provider.interactionFinished(req, res, { error: 'access_denied' }, { mergeWithLastSubmission: false });
    let existing = domain(() => service.sdk.grantForInteraction(approval.interactionHash)), grantId = existing?.grantId;
    if (!grantId) {
      const grant = new provider.Grant({ accountId: approval.accountId, clientId: approval.clientId }); grant.addOIDCScope(approval.scopes);
      try { grantId = await staged.run(approval, () => grant.save()); }
      catch (error) {
        existing = domain(() => service.sdk.grantForInteraction(approval.interactionHash));
        if (!existing?.grantId) throw error; grantId = existing.grantId;
      }
    }
    return provider.interactionFinished(req, res, { login: { accountId: approval.accountId, remember: true, amr: ['soty-connect-proof'] },
      consent: { grantId } }, { mergeWithLastSubmission: false });
  }
  return Object.freeze({ provider, ingress, finish,
    dispatch(req, res, browserNonce) { return requests.run({ browserNonce }, () => provider.callback()(req, res)); } });
}
