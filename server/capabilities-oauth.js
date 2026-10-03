import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import path from 'node:path';
import { createSotyOAuthProvider, oauthCallbackPolicy } from './capabilities-oauth-provider.js';
import { isOAuthNamespace } from './capabilities-oauth-profile.js';
import { OAuthIngressError, oauthSingleHeader, parseOAuthForm } from './capabilities-oauth-ingress.js';
import { OAUTH_FAILURE_DOCUMENT, OAUTH_FAILURE_POLICY } from './capabilities-oauth-document.js';

const check = (value, code = 'invalid_request') => { if (!value) throw new OAuthIngressError(code); };
const UID = '[A-Za-z0-9_-]{16,128}';
const interactionPath = new RegExp(`^/oauth/interaction/(${UID})(?:/(context|complete))?$`, 'u');
const resumePath = new RegExp(`^/oauth/authorize/${UID}$`, 'u');
const AUTHORIZE_FIELDS = new Set(['client_id', 'redirect_uri', 'response_type', 'response_mode', 'scope', 'resource',
  'code_challenge', 'code_challenge_method', 'state', 'prompt']);
const epoch = () => Math.floor(Date.now() / 1000);

function browserBinding(profile) {
  const cookieName = (profile.secure ? '__Host-' : '') + 'soty_oauth_browser';
  const keys = profile.providerKeys().cookieKeys;
  const mac = (body, key) => createHmac('sha256', key).update('soty.oauth.browser.v1\0' + body).digest('base64url');
  function read(req) {
    const raw = oauthSingleHeader(req, 'cookie');
    if (raw === undefined) return null;
    check(raw.length <= 8192);
    const found = raw.split(';').map(item => item.trim()).filter(item => item.startsWith(cookieName + '='));
    check(found.length <= 1);
    if (!found.length) return null;
    const value = found[0].slice(cookieName.length + 1);
    const parsed = /^([A-Za-z0-9_-]{43})\.([0-9]{1,12})\.([A-Za-z0-9_-]{43})$/u.exec(value);
    if (!parsed) return null;
    const now = epoch(), issued = Number(parsed[2]);
    if (!Number.isSafeInteger(issued) || issued > now || now - issued >= 600) return null;
    const received = Buffer.from(parsed[3]), body = parsed[1] + '.' + parsed[2];
    if (!keys.some(key => timingSafeEqual(Buffer.from(mac(body, key)), received))) return null;
    return parsed[1];
  }
  return Object.freeze({
    read,
    ensure(req, res) {
      // A second interaction must not inherit the first page's nearly expired
      // cookie lifetime. Keep the same binding for other tabs, but refresh its
      // signed timestamp; durable interaction deadlines never move.
      const nonce = read(req) || randomBytes(32).toString('base64url'), body = nonce + '.' + epoch();
      res.append('Set-Cookie', `${cookieName}=${body}.${mac(body, keys[0])}; Path=${profile.secure ? '/' : '/oauth'}; Max-Age=600; HttpOnly; SameSite=Lax${profile.secure ? '; Secure' : ''}`);
      return nonce;
    },
  });
}

function safeFailure(req, res, error, consentDocument = false) {
  if (res.destroyed || res.writableEnded || res.headersSent) return;
  res.set('Referrer-Policy', 'no-referrer');
  const code = typeof error?.code === 'string' ? error.code : '';
  const unavailable = ['oauth_unavailable', 'temporarily_unavailable', 'oauth_quota_exceeded', 'service_closed',
    'oauth_storage_busy', 'oauth_storage_key_unavailable', 'capabilities_storage_corrupt',
    'connect_authority_busy', 'native_storage_busy'].includes(code);
  const missing = ['oauth_interaction_expired', 'oauth_interaction_not_found', 'not_found'].includes(code);
  const status = unavailable ? 503 : missing ? 410 : code === 'rate_limit' ? 429
    : code === 'access_denied' ? 403 : code === 'method_not_allowed' ? 405 : 400;
  if (!req.complete || !req.readableEnded) { res.shouldKeepAlive = false; res.set('Connection', 'close'); }
  if (unavailable || status === 429) res.set('Retry-After', '60');
  if (consentDocument) {
    res.status(status).set('Content-Security-Policy', OAUTH_FAILURE_POLICY).type('html').send(OAUTH_FAILURE_DOCUMENT);
    return;
  }
  res.status(status).json({ error: unavailable ? 'temporarily_unavailable' : missing ? 'interaction_expired'
    : ['access_denied', 'method_not_allowed'].includes(code) ? code : 'invalid_request' });
}

/** Mount only on the trusted shell. Resource servers still perform their own
 * live permission checks; an AS session/cookie is never a resource credential. */
export function attachCapabilitiesOAuth(app, { profile, service, distDir } = {}) {
  if (!profile) return null;
  const oauth = service?.oauth, runtime = profile.enabled ? createSotyOAuthProvider({ profile, oauth }) : null;
  const { provider, ingress, bindGrant } = runtime ?? {}, browser = runtime ? browserBinding(profile) : null;
  // Only the outer boundary admits transport/Host and overwrites forwarding
  // headers from the trusted Express result before Koa uses them.
  if (provider) provider.proxy = true;
  const expectedHost = new URL(profile.origin).host;
  const callback = provider?.callback();
  const noBody = req => check(oauthSingleHeader(req, 'transfer-encoding') === undefined
    && [undefined, '0'].includes(oauthSingleHeader(req, 'content-length')));
  const actualMethod = (req, res, method) => {
    if (req.method !== method) { res.set('Allow', method); throw new OAuthIngressError('method_not_allowed'); }
  };
  app.use(async (req, res, next) => {
    const target = req.originalUrl || req.url;
    if (!isOAuthNamespace(target)) { next(); return; }
    let lease, consentDocument = false;
    res.set({ 'Cache-Control': 'no-store', 'Pragma': 'no-cache', 'Referrer-Policy': 'no-referrer',
      'X-Content-Type-Options': 'nosniff' });
    try {
      check(Buffer.byteLength(target) <= 8192 && !/[^\u0021-\u007e]|[#\\]/u.test(target));
      check(oauthSingleHeader(req, 'host')?.toLowerCase() === expectedHost);
      const origin = oauthSingleHeader(req, 'origin');
      check(origin === undefined || origin === profile.origin, 'access_denied');
      check(!profile.secure || req.secure === true, 'access_denied');
      // Trust no arbitrary Forwarded host/protocol inside the provider.
      delete req.headers.forwarded; delete req.headers['x-forwarded-host'];
      req.headers['x-forwarded-proto'] = profile.secure ? 'https' : 'http';
      const split = target.indexOf('?'), pathname = split < 0 ? target : target.slice(0, split);
      check(!pathname.includes('%'));
      const query = split < 0 ? '' : target.slice(split + 1);
      if (pathname === '/.well-known/oauth-protected-resource' || pathname === '/.well-known/oauth-protected-resource/mcp') {
        actualMethod(req, res, 'GET'); check(split < 0); noBody(req);
        res.json(profile.protectedResource(pathname.endsWith('/mcp') ? profile.resources.mcp : profile.resources.http)); return;
      }
      // Stable public resource metadata remains discoverable when issuance is
      // paused. No Provider, signing key or browser session exists in that mode.
      if (!runtime) throw new OAuthIngressError('temporarily_unavailable');
      const interaction = interactionPath.exec(pathname);
      if (interaction) {
        check(split < 0);
        const [, uid, action] = interaction;
        actualMethod(req, res, action === 'complete' ? 'POST' : 'GET');
        lease = ingress.enter(req, res);
        let completionAccountId;
        if (action === 'complete') {
          check(origin === profile.origin, 'access_denied');
          const site = oauthSingleHeader(req, 'sec-fetch-site');
          check(site === undefined || site === 'same-origin', 'access_denied');
          const form = await lease.readForm();
          check(Object.keys(form).length === 1 && typeof form.expectedAccountId === 'string'
            && form.expectedAccountId.length > 0 && form.expectedAccountId.length <= 160);
          completionAccountId = form.expectedAccountId;
        } else noBody(req);
        // Only the admitted consent document has a human-facing error page.
        // Context and completion remain their existing JSON/protocol surfaces.
        consentDocument = !action;
        const details = await provider.interactionDetails(req, res);
        check(details.uid === uid);
        const nonce = action ? browser.read(req) : browser.ensure(req, res);
        check(nonce, 'access_denied');
        if (!action) {
          oauth.prepareInteraction({ interactionId: uid, browserNonce: nonce });
          // A native form navigation under no-referrer sends Origin: null.
          // Preserve the same-origin Origin guard without leaking a referrer
          // to the registered callback on another origin.
          res.set('Referrer-Policy', 'same-origin');
          // Chromium applies form-action to redirects following the completion
          // POST. Permit this already validated exact native callback on this
          // consent document only; the rest of the shell retains 'self'.
          res.set('Content-Security-Policy', oauthCallbackPolicy(profile,
            res.get('Content-Security-Policy'), details.params));
          // The entry dispatches this path to the dedicated consent controller.
          await new Promise((resolve, reject) => res.sendFile(path.join(distDir, 'index.html'), error => error ? reject(error) : resolve()));
          return;
        }
        const context = oauth.readInteraction({ interactionId: uid, browserNonce: nonce });
        if (action === 'context') {
          const label = profile.profiles.find(item => item.id === context.clientProfile)?.label;
          check(label);
          res.json({ ...context, browserNonce: nonce, clientLabel: label }); return;
        }
        check(context.decision !== 'pending', 'access_denied');
        check(completionAccountId === context.decidedAccountId, 'access_denied');
        if (context.decision === 'denied') {
          await provider.interactionFinished(req, res, { error: 'access_denied', error_description: 'Connection denied' }, { mergeWithLastSubmission: false });
        } else {
          const grant = await bindGrant({ interactionId: uid, browserNonce: nonce });
          await provider.interactionFinished(req, res, { login: { accountId: grant.accountId, remember: false },
            consent: { grantId: grant.grantId } }, { mergeWithLastSubmission: false });
        }
        return;
      }
      if (pathname === '/mcp') {
        check(split < 0);
        // A host that installs the separate MCP adapter handles this namespace
        // before OAuth. Standalone OAuth composition retains a finite fallback.
        res.set('WWW-Authenticate', `Bearer resource_metadata="${profile.origin}/.well-known/oauth-protected-resource/mcp"`);
        if (!req.complete || !req.readableEnded) { res.shouldKeepAlive = false; res.set('Connection', 'close'); }
        res.status(503).json({ error: 'transport_unavailable' }); return;
      }
      const discoveryAlias = pathname === '/.well-known/oauth-authorization-server/oauth';
      const authorize = pathname === '/oauth/authorize', resume = resumePath.test(pathname);
      const providerRead = discoveryAlias || ['/oauth/.well-known/openid-configuration', '/oauth/jwks'].includes(pathname);
      const sessionConfirm = pathname === '/oauth/session/end/confirm';
      const providerWrite = sessionConfirm || ['/oauth/token', '/oauth/revoke'].includes(pathname);
      check(authorize || resume || providerRead || providerWrite);
      actualMethod(req, res, providerWrite ? 'POST' : 'GET');
      if (sessionConfirm) {
        check(origin === profile.origin, 'access_denied');
        const site = oauthSingleHeader(req, 'sec-fetch-site');
        check(site === undefined || site === 'same-origin', 'access_denied');
        // The provider alone checks its session-bound XSRF and performs the
        // account switch. Public RP logout routes remain disabled.
      }
      if (!providerWrite) noBody(req);
      if (authorize) {
        const parameters = parseOAuthForm(query);
        check(Object.keys(parameters).every(name => AUTHORIZE_FIELDS.has(name)));
        check(parameters.response_type === 'code' && parameters.scope === 'notes.createDraft'
          && Object.values(profile.resources).includes(parameters.resource)
          && parameters.code_challenge_method === 'S256' && /^[A-Za-z0-9_-]{43}$/u.test(parameters.code_challenge || '')
          && profile.isRegisteredRedirect({ clientId: parameters.client_id, redirectUri: parameters.redirect_uri }));
        if (parameters.state !== undefined) check(Buffer.byteLength(parameters.state) <= 512);
        if (parameters.prompt !== undefined) check(parameters.prompt === 'none');
        if (parameters.response_mode !== undefined) check(parameters.response_mode === 'query');
      } else check(split < 0);
      // Match Express's standard provider mount while retaining originalUrl for
      // the outer bounded ingress. The RFC 8414 alias has no /oauth prefix from
      // which the provider could infer its mount, so supply the trusted baseUrl
      // for both discovery paths as an ordinary Express mount would.
      req.baseUrl = '/oauth';
      req.url = discoveryAlias ? '/.well-known/openid-configuration' : target.slice('/oauth'.length);
      callback(req, res);
    } catch (error) { safeFailure(req, res, error, consentDocument); }
    finally { lease?.release(); }
  });
  return Object.freeze({ enabled: profile.enabled, issuer: profile.issuer });
}
