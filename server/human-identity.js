import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import path from 'node:path';
import { HUMAN_IDENTITY_PATH, HumanIdentityError, requireHuman as require } from '../modules/human-identity/profile.mjs';
import { createHumanIdentityProvider } from './human-identity-provider.js';
import { oauthSingleHeader, parseOAuthForm } from './capabilities-oauth-ingress.js';

const UID = '[A-Za-z0-9_-]{16,128}', interactionPattern = new RegExp(`^/human-identity/interaction/(${UID})(?:/(context|complete))?$`, 'u');
const fields = new Set(['client_id', 'redirect_uri', 'response_type', 'response_mode', 'scope', 'state', 'nonce', 'code_challenge', 'code_challenge_method', 'prompt']);
const epoch = () => Math.floor(Date.now() / 1000);
export function isHumanIdentityNamespace(target) {
  if (typeof target !== 'string') return false;
  let value = target.split('?')[0];
  for (let i = 0; i < 3; i++) {
    if (/^\/human-identity(?:\/|$)/iu.test(value)) return true;
    try { const decoded = decodeURIComponent(value); if (decoded === value) break; value = decoded; } catch { break; }
  }
  return false;
}
function browserBinding(profile) {
  const keys = profile.providerKeys().cookieKeys, name = (profile.secure ? '__Host-' : '') + 'soty_human_browser';
  const mac = (text, key) => createHmac('sha256', key).update('soty.human.browser.v1\0' + text).digest('base64url');
  function read(req) {
    const raw = oauthSingleHeader(req, 'cookie'); if (raw === undefined) return null;
    require(raw.length <= 8192, 'human_identity_browser_mismatch', 403);
    const found = raw.split(';').map(part => part.trim()).filter(part => part.startsWith(name + '=')); require(found.length <= 1, 'human_identity_browser_mismatch', 403);
    if (!found.length) return null;
    const match = /^([A-Za-z0-9_-]{43})\.([0-9]{1,12})\.([A-Za-z0-9_-]{43})$/u.exec(found[0].slice(name.length + 1));
    if (!match || Number(match[2]) > epoch() || epoch() - Number(match[2]) >= 600) return null;
    const received = Buffer.from(match[3]), text = match[1] + '.' + match[2];
    if (!keys.some(key => timingSafeEqual(Buffer.from(mac(text, key)), received))) return null; return match[1];
  }
  return { read, ensure(req, res) {
    const value = read(req) || randomBytes(32).toString('base64url'), text = value + '.' + epoch();
    res.append('Set-Cookie', `${name}=${text}.${mac(text, keys[0])};Path=${profile.secure ? '/' : HUMAN_IDENTITY_PATH};Max-Age=600;HttpOnly;SameSite=Lax${profile.secure ? ';Secure' : ''}`);
    return value;
  } };
}
function failure(req, res, error) {
  if (res.headersSent || res.destroyed || res.writableEnded) return;
  const status = error instanceof HumanIdentityError ? error.status : 400;
  if (!req.complete || !req.readableEnded) { res.shouldKeepAlive = false; res.set('Connection', 'close'); }
  res.status(status).json({ error: status >= 500 || status === 429 ? 'temporarily_unavailable' : status === 410 ? 'interaction_expired' : 'access_denied' });
}
/** Reserve the human issuer even while disabled; only approved host configuration enables protocol routes. */
export function attachHumanIdentity(app, { profile, service, distDir } = {}) {
  const runtime = profile?.enabled && service ? createHumanIdentityProvider({ profile, service }) : null;
  const binding = runtime ? browserBinding(profile) : null;
  if (runtime) runtime.provider.proxy = true;
  app.use(async (req, res, next) => {
    const target = req.originalUrl || req.url;
    if (!isHumanIdentityNamespace(target)) return next();
    let lease; res.set({ 'Cache-Control': 'no-store', Pragma: 'no-cache', 'Referrer-Policy': 'no-referrer', 'X-Content-Type-Options': 'nosniff' });
    try {
      require(runtime, 'human_identity_disabled', 503);
      require(Buffer.byteLength(target) <= 8192 && !/[\s#\\]/u.test(target), 'human_identity_invalid');
      require(oauthSingleHeader(req, 'host')?.toLowerCase() === new URL(profile.origin).host, 'human_identity_origin_mismatch', 403);
      const origin = oauthSingleHeader(req, 'origin');
      require(origin === undefined || origin === profile.origin, 'human_identity_origin_mismatch', 403);
      require(!profile.secure || req.secure === true, 'human_identity_origin_mismatch', 403);
      delete req.headers.forwarded; delete req.headers['x-forwarded-host']; req.headers['x-forwarded-proto'] = profile.secure ? 'https' : 'http';
      const split = target.indexOf('?'), pathname = split < 0 ? target : target.slice(0, split), query = split < 0 ? '' : target.slice(split + 1);
      require(!pathname.includes('%'), 'human_identity_invalid');
      const interaction = interactionPattern.exec(pathname);
      if (interaction) {
        require(split < 0 && req.method === (interaction[2] === 'complete' ? 'POST' : 'GET'), 'human_identity_invalid');
        const details = await runtime.provider.interactionDetails(req, res); require(details.uid === interaction[1], 'human_identity_invalid');
        const browserNonce = interaction[2] === 'complete' ? binding.read(req) : binding.ensure(req, res);
        require(browserNonce, 'human_identity_browser_mismatch', 403);
        if (interaction[2] === 'complete') {
          require(origin === profile.origin && [undefined, 'same-origin'].includes(oauthSingleHeader(req, 'sec-fetch-site')), 'human_identity_origin_mismatch', 403);
          lease = runtime.ingress.enter(req, res); const input = await lease.readForm();
          require(Object.keys(input).length === 1 && typeof input.csrf === 'string', 'human_identity_invalid');
          const approval = service.readApprovedInteraction({ interactionId: details.uid, browserNonce, csrf: input.csrf });
          await runtime.finish(req, res, approval); return;
        }
        require(oauthSingleHeader(req, 'transfer-encoding') === undefined && [undefined, '0'].includes(oauthSingleHeader(req, 'content-length')), 'human_identity_invalid');
        const context = service.prepareInteraction({ interactionId: details.uid, browserNonce, parameters: details.params });
        if (interaction[2] === 'context') { res.json(context); return; }
        const client = profile.client(details.params.client_id); require(client && profile.isRegisteredRedirect(client.id, details.params.redirect_uri), 'human_identity_invalid');
        res.set('Referrer-Policy', 'same-origin');
        res.set('Content-Security-Policy', `default-src 'self';script-src 'self';style-src 'self' 'unsafe-inline';img-src 'self' data:;connect-src 'self';frame-ancestors 'self';base-uri 'self';form-action 'self' ${client.redirectUri}`);
        if (distDir) await new Promise((done, reject) => res.sendFile(path.join(distDir, 'index.html'), error => error ? reject(error) : done()));
        else res.type('html').send('<!doctype html><html lang="ru"><meta charset="utf-8"><title>Вход в Соты</title><p>Подтвердите вход текущим профилем Сот. Создание профиля требует отдельного явного действия.</p></html>');
        return;
      }
      const authorize = pathname === HUMAN_IDENTITY_PATH + '/authorize', resume = new RegExp(`^/human-identity/authorize/${UID}$`, 'u').test(pathname);
      const sessionConfirm = pathname === HUMAN_IDENTITY_PATH + '/session/end/confirm';
      const write = sessionConfirm || ['/token', '/revoke'].some(part => pathname === HUMAN_IDENTITY_PATH + part);
      const read = ['/userinfo', '/jwks', '/.well-known/openid-configuration'].some(part => pathname === HUMAN_IDENTITY_PATH + part);
      require(authorize || resume || write || read, 'human_identity_invalid'); require(req.method === (write ? 'POST' : 'GET'), 'human_identity_invalid');
      if (authorize) {
        const params = parseOAuthForm(query);
        require(Object.keys(params).every(name => fields.has(name)) && params.response_type === 'code' && ['openid', 'openid profile'].includes(params.scope)
          && params.code_challenge_method === 'S256' && /^[A-Za-z0-9_-]{43}$/u.test(params.code_challenge || '')
          && profile.isRegisteredRedirect(params.client_id, params.redirect_uri), 'human_identity_invalid');
        for (const name of ['state', 'nonce']) require(/^[A-Za-z0-9_-]{16,128}$/u.test(params[name] || ''), 'human_identity_invalid');
        require(params.response_mode === undefined || params.response_mode === 'query', 'human_identity_invalid');
        require(params.prompt === undefined || params.prompt === 'login', 'human_identity_invalid');
      } else require(split < 0, 'human_identity_invalid');
      if (sessionConfirm) require(origin === profile.origin && [undefined, 'same-origin'].includes(oauthSingleHeader(req, 'sec-fetch-site')), 'human_identity_origin_mismatch', 403);
      const browserNonce = authorize ? binding.ensure(req, res) : binding.read(req);
      req.baseUrl = HUMAN_IDENTITY_PATH; req.url = target.slice(HUMAN_IDENTITY_PATH.length); runtime.dispatch(req, res, browserNonce);
    } catch (error) { failure(req, res, error); }
    finally { lease?.release(); }
  });
  return Object.freeze({ enabled: Boolean(runtime), issuer: profile?.issuer || null });
}
