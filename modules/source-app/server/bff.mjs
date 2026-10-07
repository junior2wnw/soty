import { createSourceRpProtocol } from '../../source-rp/server/index.mjs';
import { selectedResourceProfile, resourceConsentDigest } from '../../apps/scoped-embed/resource-profile.mjs';
import { createResourceSourceProofVerifier } from '../../apps/scoped-embed/resource-proof.mjs';
import { createSourceAuthorityClient } from '../../apps/scoped-embed/source-authority-client.mjs';
import { createNativeAuthorityRuntime } from './native-authority.mjs';
import { STANDARD_SELECTED_SOURCE, STANDARD_SELECTED_SOURCE_V2 } from './standard-profile.mjs';
import { validateSourceFeedbackInput } from './feedback-wire.mjs';
import { feedbackOutput } from '../shared/feedback-wire.mjs';
import { parseSourceJson } from '../shared/strict-json.mjs';
import { sourceEmbedRoute, sourceBodyHeaders } from './router.mjs';
import { fields, check, digest, nonce, opaque, requestId, jsonCopy, deepFreeze, syncResult, SourceAppError } from './wire.mjs';

const COOKIE = 'soty_rp_session', LINK = 'soty_rp_link';
const cookie = (req, name) => {
  const header = String(req.headers.cookie || '');
  check(Buffer.byteLength(header) <= 4096, 'source_app_cookie_invalid', 401);
  const values = header.split(';').map(value => value.trim()).filter(value => value.startsWith(name + '='));
  check(values.length <= 1, 'source_app_cookie_invalid', 401);
  const value = values[0]?.slice(name.length + 1); check(value === undefined || opaque(value), 'source_app_cookie_invalid', 401); return value;
};
const cookieHeader = (name, value, seconds) => name + '=' + value + '; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=' + seconds;
function html(text) { return String(text).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'); }

/** Actual maintained BFF and fixed router; a durable Source storage port and
 * Native domain hooks are mandatory. This module never stores Root credentials
 * or turns an OIDC identity into a Native permission on its own. */
export function createSourceAppBff(options) {
  const value = fields(options, ['profile', 'transportKey', 'connectorPort', 'storage', 'native', 'rp'],
    ['clock', 'allowCreateEmptyGuest', 'ui', 'rpSessions']);
  const profile = selectedResourceProfile(value.profile), clock = value.clock ?? Date.now;
  // Ports are not cookie boundaries. Only Native correlation cookies are
  // namespaced; the fixed embed cookies stay private to each Root broker slot.
  const INTENT = 'soty_native_intent_' + profile.appId.slice(4), CSRF = 'soty_native_csrf_' + profile.appId.slice(4);
  const embedOidc = digest(profile.sourceProfile) === digest(STANDARD_SELECTED_SOURCE_V2);
  check((embedOidc || digest(profile.sourceProfile) === digest(STANDARD_SELECTED_SOURCE)) && profile.resource.selection.kind === 'soty.resource.v1', 'source_app_profile_invalid', 503);
  check(typeof clock === 'function' && (value.allowCreateEmptyGuest === undefined || typeof value.allowCreateEmptyGuest === 'boolean'));
  const labels = value.ui === undefined ? { appLabel: 'Приложение', resourceLabel: 'Выбранный ресурс' }
    : fields(value.ui, ['appLabel', 'resourceLabel']);
  check(Object.values(labels).every(label => typeof label === 'string' && label.isWellFormed() && label.trim().length > 0 && label.length <= 160
    && !/[\u0000-\u001f\u007f]/u.test(label)), 'source_app_configuration_invalid', 503);
  const store = fields(value.storage, ['consumeNonce', 'createInteraction', 'getInteraction', 'claimInteraction', 'claimCallback', 'completeInteraction',
    'readSession', 'readCompletion', 'consumeCompletion', 'readTokenProof', 'revokeSession']);
  Object.values(store).forEach(method => check(typeof method === 'function'));
  const native = createNativeAuthorityRuntime(value.native);
  const protocol = createSourceRpProtocol(value.rp, { clock });
  check(value.rp.issuer === profile.issuer && value.rp.clientId === profile.clientId
    && value.rp.redirectUri === (embedOidc ? profile.embedOrigin + '/api/embed/callback' : profile.nativeOrigin + '/soty/callback'), 'source_app_profile_invalid', 503);
  const verifier = createResourceSourceProofVerifier({ profile, key: value.transportKey, consumeNonce: store.consumeNonce, clock });
  const readAuthority = createSourceAuthorityClient({ profile, key: value.transportKey, connectorPort: value.connectorPort, clock });
  const semanticDigest = resourceConsentDigest(profile);
  const responseLimits = new WeakMap(); let closed = false;
  async function storageCommit(method, args, action) {
    let open = true, entered = false, poisoned = false;
    const final = () => {
      if (!open || entered) { poisoned = true; throw new SourceAppError('source_app_authority_invalid', 503); }
      entered = true; return syncResult(action());
    };
    try {
      const result = await method(...args, final); open = false;
      check(entered && !poisoned, 'source_app_commit_unknown', 503); return result;
    } catch (error) {
      if (entered) throw new SourceAppError('source_app_commit_unknown', 503); throw error;
    } finally { open = false; }
  }
  async function postCommit(context, authority) {
    try { await freshRoot(context); native.withCurrent(authority, () => true); }
    catch { throw new SourceAppError('source_app_effect_unknown', 503); }
  }
  async function freshRoot(context) {
    check(!closed, 'source_app_closed', 503);
    const actual = await readAuthority({ reference: context.reference, connector: profile.connector });
    check(digest(actual) === digest(context) && actual.expiresAt > clock(), 'source_app_root_changed', 403); return actual;
  }
  function binding(context, identity, operation, sessionIdHash) {
    return deepFreeze({ identity: { issuer: identity.issuer, subject: identity.subject ?? identity.sub },
      rootPrincipal: context.rootPrincipal, humanPrincipal: context.humanPrincipal, resource: profile.resource,
      semanticDigest, operation, ...(sessionIdHash ? { sessionIdHash } : {}) });
  }
  async function proveSession(session, context, operation, hostOptions) {
    await freshRoot(context);
    check(session && session.active === true && session.semanticDigest === semanticDigest && session.expiresAt > clock()
      && session.identity.issuer === profile.issuer && session.identity.subject === context.humanPrincipal.subject
      && digest(session.rootPrincipal) === digest(context.rootPrincipal), 'authentication_required', 401);
    // Basic is bound to the original Root slot. It cannot silently become a
    // new-slot/resume authority merely because actor/resource strings match.
    check(session.rootReferenceDigest === digest(context.reference), 'authentication_required', 401);
    let proof;
    if (session.rpMarker) {
      check(value.rpSessions && typeof value.rpSessions.currentProof === 'function', 'source_app_long_session_not_ready', 503);
      proof = await value.rpSessions.currentProof(session.rpMarker, hostOptions);
    } else {
      const secret = await store.readTokenProof(session);
      check(secret.expiresAt > clock(), 'authentication_required', 401);
      const subject = await protocol.currentSubject(secret.accessToken, session.identity.subject);
      check(subject === context.humanPrincipal.subject, 'authentication_required', 401);
      proof = { issuer: profile.issuer, sub: subject, expiresAt: secret.expiresAt, sessionExpiresAt: session.expiresAt };
    }
    await freshRoot(context);
    const authority = await native.capture(binding(context, { issuer: proof.issuer, subject: proof.sub }, operation, session.idHash));
    native.withCurrent(authority, () => true);
    return { session, proof, authority };
  }
  async function currentSession(req, context, operation, hostOptions) {
    const token = cookie(req, COOKIE); check(token, 'authentication_required', 401);
    return proveSession(await store.readSession(digest(token)), context, operation, hostOptions);
  }
  async function completeIdentity(interaction, identity, authority) {
    check(identity.issuer === profile.issuer && identity.subject === interaction.context.humanPrincipal.subject, 'source_app_identity_mismatch', 403);
    await freshRoot(interaction.context); native.withCurrent(authority, () => true);
    const sessionToken = nonce(), completion = nonce();
    const result = await storageCommit(store.completeInteraction, [{ idHash: interaction.idHash, revision: interaction.revision + 1, completionToken: completion,
      session: { idHash: digest(sessionToken), identity: { issuer: identity.issuer, subject: identity.subject },
        rootPrincipal: interaction.context.rootPrincipal, rootReferenceDigest: digest(interaction.context.reference), semanticDigest, active: true,
        createdAt: interaction.createdAt, expiresAt: Math.min(identity.expiresAt, interaction.createdAt + 300000),
        accessToken: identity.accessToken, cookieToken: sessionToken, completionHash: digest(completion) } }],
      () => native.commitIdentity(authority, { issuer: identity.issuer, subject: identity.subject }, { createEmptyGuest: value.allowCreateEmptyGuest === true }));
    check(result === true, 'source_app_commit_unknown', 503); await postCommit(interaction.context, authority);
    return store.getInteraction(interaction.idHash);
  }
  function send(res, status, data, headers = {}) {
    const bytes = Buffer.from(JSON.stringify(data)), limit = responseLimits.get(res);
    check(bytes.length <= (limit?.responseBytes ?? 1500000), ['write', 'feedback-write'].includes(limit?.kind)
      ? 'source_app_effect_unknown' : 'source_app_response_invalid', 503);
    res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'referrer-policy': 'no-referrer', ...headers }); res.end(bytes);
  }
  function page(res, title, body, headers = {}, status = 200) {
    res.writeHead(status, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store', 'referrer-policy': 'no-referrer',
      'content-security-policy': "default-src 'none'; style-src 'self'; form-action 'self'; frame-ancestors 'none'", ...headers });
    res.end('<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>' + html(title) + '</title><main><h1>' + html(title) + '</h1>' + body + '</main></html>');
  }
  async function body(req, limit) {
    const parts = []; let bytes = 0;
    for await (const part of req) { bytes += part.length; check(bytes <= limit, 'source_app_payload_limit', 413); parts.push(part); }
    return Buffer.concat(parts, bytes);
  }
  function parse(bytes) { try { return jsonCopy(parseSourceJson(new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes)), { bytes: 1500000 }); } catch (error) { if (error instanceof SourceAppError) throw error; throw new SourceAppError('source_app_input_invalid'); } }
  async function nativeRoute(req, res, url) {
    check(req.headers.host === new URL(profile.nativeOrigin).host, 'source_app_origin_denied', 403);
    if (['GET', 'HEAD'].includes(req.method)) sourceBodyHeaders(req, 0);
    if (url.pathname === '/soty/connect') {
      check(req.method === 'GET' && [...url.searchParams.keys()].join(',') === 'intent' && opaque(url.searchParams.get('intent')));
      const interaction = await store.getInteraction(digest(url.searchParams.get('intent')));
      check(interaction && interaction.expiresAt > clock(), 'source_app_intent_unavailable', 401);
      await freshRoot(interaction.context);
      if (embedOidc && interaction.phase === 'completed') {
        const session = await store.readCompletion(interaction.idHash); await proveSession(session, interaction.context, 'completion');
        res.writeHead(303, { location: profile.embedOrigin + '/api/embed/callback?state=' + encodeURIComponent(url.searchParams.get('intent')),
          'cache-control': 'no-store', 'referrer-policy': 'no-referrer' }); res.end(); return;
      }
      check(interaction.phase === 'pending', 'source_app_intent_unavailable', 401);
      const csrf = nonce();
      page(res, 'Подключить «' + labels.appLabel + '»', '<p>Вход Сот подтвердит человека. Приложение отдельно проверит доступ к выбранному ресурсу.</p>'
        + (value.allowCreateEmptyGuest === true ? '<p>Для нового пустого проекта приложение создаст отдельный профиль после подтверждения входа. Доступ к существующим данным не добавляется.</p>' : '')
        + '<form method="post" action="/soty/authorize"><input type="hidden" name="intent" value="' + html(url.searchParams.get('intent')) + '">'
        + '<input type="hidden" name="csrf" value="' + csrf + '"><input type="hidden" name="scope" value="' + semanticDigest + '">'
        + '<p>' + html(labels.resourceLabel) + '. Подключение не расширяет ваши права в приложении.</p>'
        + '<label><input type="checkbox" name="consent" value="yes" required>Подключить этот выбранный ресурс с моими текущими правами</label>'
        + '<button type="submit">Войти через Соты</button></form>',
      { 'set-cookie': [cookieHeader(INTENT, url.searchParams.get('intent'), 300), cookieHeader(CSRF, csrf, 300)],
        ...(embedOidc ? { 'referrer-policy': 'origin',
          // Chromium checks form redirects as well as the first same-origin
          // POST. Only this reviewed issuer may receive the fixed OIDC flow.
          'content-security-policy': "default-src 'none'; style-src 'self'; form-action 'self' " + new URL(profile.issuer).origin + "; frame-ancestors 'none'" } : {}) });
      return;
    }
    if (url.pathname === '/soty/authorize') {
      check(req.method === 'POST' && req.headers.origin === profile.nativeOrigin, 'source_app_origin_denied', 403);
      check(typeof req.headers['content-type'] === 'string' && /^application\/x-www-form-urlencoded(?:\s*;\s*charset=utf-8)?$/iu.test(req.headers['content-type'])
        && req.headers['content-encoding'] === undefined, 'source_app_content_type_invalid', 415);
      const bytes = await body(req, 4096); let decoded;
      try { decoded = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes); } catch { throw new SourceAppError('source_app_input_invalid'); }
      const form = new URLSearchParams(decoded);
      check([...form.keys()].sort().join(',') === 'consent,csrf,intent,scope' && [...form.keys()].every(key => form.getAll(key).length === 1)
        && form.get('scope') === semanticDigest && form.get('consent') === 'yes', 'source_app_csrf_denied', 403);
      check(cookie(req, INTENT) === form.get('intent'), 'source_app_intent_superseded', 409);
      check(cookie(req, CSRF) === form.get('csrf'), 'source_app_csrf_denied', 403);
      const interaction = await store.getInteraction(digest(form.get('intent')));
      check(interaction && interaction.phase === 'pending' && interaction.expiresAt > clock(), 'source_app_intent_unavailable', 401);
      await freshRoot(interaction.context);
      const intent = await protocol.start(); await freshRoot(interaction.context);
      if (embedOidc) {
        // Reuse the cryptorandom43 original handoff correlation. PKCE/nonce
        // remain maintained SDK values; no vendor49/auth algorithm changes.
        const location = new URL(intent.location); location.searchParams.set('state', form.get('intent')); intent.state = form.get('intent'); intent.location = location.href;
        const authority = await native.capture(binding(interaction.context, interaction.context.humanPrincipal, 'link'), req);
        native.withCurrent(authority, () => true); await freshRoot(interaction.context);
        const remembered = await storageCommit(store.claimInteraction, [interaction.idHash, interaction.revision, intent],
          () => native.rememberLogin(authority, { interactionIdHash: interaction.idHash, expiresAt: interaction.expiresAt }));
        check(remembered === true, 'source_app_intent_unavailable', 409); await freshRoot(interaction.context); native.withCurrent(authority, () => true);
      } else check(await store.claimInteraction(interaction.idHash, interaction.revision, intent) === true, 'source_app_intent_unavailable', 409);
      res.writeHead(303, { location: intent.location, 'cache-control': 'no-store', 'referrer-policy': 'no-referrer' }); res.end(); return;
    }
    if (url.pathname === '/soty/callback') {
      check(!embedOidc, 'source_app_route_unavailable', 404);
      check(req.method === 'GET', 'source_app_method_invalid', 405);
      const browser = cookie(req, INTENT); check(browser, 'source_app_intent_unavailable', 401);
      const interaction = await store.getInteraction(digest(browser));
      check(interaction && interaction.expiresAt > clock(), 'source_app_intent_unavailable', 401);
      check(interaction.protocolIntent?.state === url.searchParams.get('state'), 'source_app_intent_superseded', 409);
      check(['claimed', 'completed'].includes(interaction.phase), 'source_app_intent_unavailable', 401);
      check([...url.searchParams].length <= 5 && [...url.searchParams].every(([key, value]) => ['code', 'state', 'iss', 'error', 'error_description'].includes(key)
        && url.searchParams.getAll(key).length === 1 && value.length <= 4096 && value.isWellFormed()
        && !/[\u0000-\u001f\u007f]/u.test(value)) && url.searchParams.get('state') === interaction.protocolIntent.state
        && url.searchParams.get('iss') === profile.issuer && typeof url.searchParams.get('code') === 'string'
        && url.searchParams.get('code').length > 0 && !url.searchParams.has('error'), 'source_app_callback_denied', 403);
      await freshRoot(interaction.context);
      if (interaction.phase === 'completed') {
        // Only readonly recovery of this exact already-committed intent. Source
        // exchange/Native link is not run again after an unknown callback ACK.
        const session = await store.readCompletion(interaction.idHash); await proveSession(session, interaction.context, 'completion');
        res.writeHead(303, { location: profile.embedOrigin + '/api/embed/callback?state=' + encodeURIComponent(browser), 'cache-control': 'no-store', 'referrer-policy': 'no-referrer' }); res.end(); return;
      }
      check(await store.claimCallback(interaction.idHash, interaction.revision) === true, 'source_app_intent_unavailable', 409);
      const identity = await protocol.exchange(new URL(req.url, profile.nativeOrigin), interaction.protocolIntent);
      check(identity.issuer === profile.issuer && identity.subject === interaction.context.humanPrincipal.subject, 'source_app_identity_mismatch', 403);
      await freshRoot(interaction.context);
      // The Native capture receives the real browser request, not a caller-
      // supplied principal. A missing legacy proof may only use explicit
      // new-empty policy implemented by the trusted Source constructor.
      const authority = await native.capture(binding(interaction.context, identity, 'link'), req);
      const sessionToken = nonce(), completion = nonce();
      const result = await storageCommit(store.completeInteraction, [{ idHash: interaction.idHash, revision: interaction.revision + 1,
        completionToken: completion,
        session: { idHash: digest(sessionToken), identity: { issuer: identity.issuer, subject: identity.subject },
          rootPrincipal: interaction.context.rootPrincipal, rootReferenceDigest: digest(interaction.context.reference), semanticDigest, active: true,
          createdAt: interaction.createdAt, expiresAt: Math.min(identity.expiresAt, interaction.createdAt + 300000),
          accessToken: identity.accessToken, cookieToken: sessionToken, completionHash: digest(completion) } }], () => {
        return native.commitIdentity(authority, { issuer: identity.issuer, subject: identity.subject }, { createEmptyGuest: value.allowCreateEmptyGuest === true });
      });
      check(result === true, 'source_app_commit_unknown', 503);
      await postCommit(interaction.context, authority);
      // Completion is a fixed one-use locator, not Source cookie/AT/sub/grant.
      res.writeHead(303, { location: profile.embedOrigin + '/api/embed/callback?state=' + encodeURIComponent(browser), 'cache-control': 'no-store', 'referrer-policy': 'no-referrer' }); res.end(); return;
    }
    throw new SourceAppError('source_app_route_unavailable', 404);
  }
  async function embedRoute(req, res, url) {
    const route = sourceEmbedRoute(req, url); sourceBodyHeaders(req, route.requestBytes); responseLimits.set(res, route);
    if (url.pathname === '/api/embed/transport-ready') {
      const headers = await verifier.verifyReady(req); res.writeHead(204, { ...headers, 'cache-control': 'no-store' }); res.end(); return;
    }
    const bytes = ['GET', 'HEAD'].includes(req.method) ? Buffer.alloc(0) : await body(req, route.requestBytes);
    const context = await verifier.verify(req, { body: bytes }); await freshRoot(context);
    if (!['GET', 'HEAD'].includes(req.method)) check(req.headers.origin === profile.embedOrigin, 'source_app_origin_denied', 403);
    const input = bytes.length ? parse(bytes) : {};
    if (url.pathname === '/api/embed/login') {
      fields(input, []); check(req.method === 'POST' && req.headers.origin === profile.embedOrigin, 'source_app_origin_denied', 403);
      if (embedOidc) {
        const original = cookie(req, 'soty_rp_intent');
        if (original) {
          const prior = await store.getInteraction(digest(original));
          if (prior?.phase === 'completed') {
            check(prior.expiresAt > clock() && digest(prior.context) === digest(context), 'source_app_completion_denied', 403);
            const session = await store.readCompletion(prior.idHash); await proveSession(session, context, 'completion');
            send(res, 200, { schema: 'soty.source-embed-auth.v1', nativeUrl: profile.nativeOrigin + '/soty/connect?intent=' + original }); return;
          }
        }
      }
      const token = nonce(); await store.createInteraction({ idHash: digest(token), revision: 0, phase: 'pending',
        context, createdAt: clock(), expiresAt: Math.min(context.expiresAt, clock() + 300000) }); await freshRoot(context);
      send(res, 200, { schema: 'soty.source-embed-auth.v1', nativeUrl: profile.nativeOrigin + '/soty/connect?intent=' + token },
        embedOidc ? { 'set-cookie': cookieHeader('soty_rp_intent', token, 300) } : {}); return;
    }
    if (url.pathname === '/api/embed/complete-link') {
      check(req.method === 'GET' && [...url.searchParams.keys()].join(',') === 'intent' && opaque(url.searchParams.get('intent')));
      const session = await store.readCompletion(digest(url.searchParams.get('intent')));
      check(session && session.semanticDigest === semanticDigest && session.identity.subject === context.humanPrincipal.subject
        && digest(session.rootPrincipal) === digest(context.rootPrincipal) && session.expiresAt > clock(), 'source_app_completion_denied', 403);
      const current = await proveSession(session, context, 'completion');
      const result = await storageCommit(store.consumeCompletion, [digest(url.searchParams.get('intent')), session.idHash], () => {
        return native.withCurrent(current.authority, () => true);
      });
      check(result && opaque(result.token), 'source_app_completion_denied', 403);
      await postCommit(context, current.authority);
      const headers = { 'set-cookie': cookieHeader(COOKIE, result.token, Math.max(1, Math.floor((session.expiresAt - clock()) / 1000))) };
      if (embedOidc) page(res, 'Приложение подключено', '<p>Вернитесь в открытую вкладку Сот. Приложение проверит подключение и покажет данные с вашими текущими правами.</p>', headers);
      else send(res, 200, { ready: true }, headers); return;
    }
    if (url.pathname === '/api/embed/callback') {
      check(req.method === 'GET' && opaque(url.searchParams.get('state')));
      let interaction = await store.getInteraction(digest(url.searchParams.get('state')));
      check(interaction && digest(interaction.context.reference) === digest(context.reference)
        && digest(interaction.context) === digest(context) && interaction.expiresAt > clock(), 'source_app_completion_denied', 403);
      if (embedOidc && interaction.phase === 'claimed') {
        check(url.searchParams.get('iss') === profile.issuer && typeof url.searchParams.get('code') === 'string' && url.searchParams.get('code').length > 0
          && !url.searchParams.has('error') && interaction.protocolIntent.state === url.searchParams.get('state'), 'source_app_callback_denied', 403);
        const authority = await native.recoverLogin(binding(context, context.humanPrincipal, 'link'), interaction.nativeLoginMarker,
          { interactionIdHash: interaction.idHash, expiresAt: interaction.expiresAt });
        await freshRoot(context); native.withCurrent(authority, () => true);
        check(await store.claimCallback(interaction.idHash, interaction.revision) === true, 'source_app_intent_unavailable', 409);
        const identity = await protocol.exchange(new URL(req.url, profile.embedOrigin), interaction.protocolIntent);
        interaction = await completeIdentity(interaction, identity, authority);
      }
      check(interaction.phase === 'completed' && (embedOidc || [...url.searchParams.keys()].join(',') === 'state'), 'source_app_completion_denied', 403);
      const session = await store.readCompletion(interaction.idHash); await proveSession(session, context, 'completion');
      check(opaque(interaction.completionToken), 'source_app_completion_denied', 403);
      // Root's private broker retains this HttpOnly link locator; it is not a
      // session token/subject/grant, and does not revive a closed Root slot.
      page(res, 'Вход подтверждён', embedOidc
        ? '<p>Завершите подключение выбранного ресурса.</p><a href="/api/embed/complete-link?intent=' + html(encodeURIComponent(interaction.completionToken)) + '">Завершить подключение</a>'
        : '<p>Вернитесь в приложение Сот.</p>', { 'set-cookie': cookieHeader(LINK, interaction.completionToken, 300) }); return;
    }
    if (url.pathname === '/api/embed/session-status') {
      try { await currentSession(req, context, 'context'); send(res, 200, { ready: true }); }
      catch (error) { if (error.status === 401 || error.status === 403) send(res, 200, { ready: false }); else throw error; } return;
    }
    if (url.pathname === '/api/embed/session-continue') {
      const args = fields(input, ['requestId']); check(requestId(args.requestId));
      const current = await currentSession(req, context, 'continue', { minimumAccessRemainingMs: 190000 });
      // Basic continuation never changes Source authority or creates a long
      // session. S2 durable resume/rebind requires an explicit storage consumer.
      send(res, 200, { schema: 'soty.source-session-continuation.v1', ready: true, renewable: false,
        sessionExpiresAt: Math.min(current.proof.sessionExpiresAt, context.expiresAt), accessExpiresAt: current.proof.expiresAt,
        receiptDigest: digest({ requestId: args.requestId, sessionIdHash: current.session.idHash, context: context.reference }) }); return;
    }
    const operation = url.pathname === '/api/embed/query' ? 'read' : url.pathname === '/api/embed/invoke' ? 'execute' : url.pathname === '/api/embed/receipt' ? 'readProof' : null;
    if (operation) {
      const args = fields(input, ['requestId', 'input'], ['reference']); check(requestId(args.requestId));
      const current = await currentSession(req, context, operation);
      const data = await native.call(current.authority, operation, args);
      if (operation === 'execute') await postCommit(context, current.authority);
      else { await freshRoot(context); native.withCurrent(current.authority, () => true); }
      send(res, 200, { ok: true, data }); return;
    }
    const feedbackOperation = new Map([
      ['GET /api/embed/feedback/context', 'context'], ['GET /api/embed/feedback/ticket', 'get'],
      ['POST /api/embed/feedback/reply', 'reply'], ['POST /api/embed/feedback/status', 'status'], ['POST /api/embed/feedback/accept', 'accept'],
      ['GET /api/embed/feedback', 'list'], ['POST /api/embed/feedback', 'submit'],
    ]).get(req.method + ' ' + url.pathname);
    if (feedbackOperation) {
      const args = validateSourceFeedbackInput(feedbackOperation, ['GET', 'HEAD'].includes(req.method) ? Object.fromEntries(url.searchParams) : input);
      const current = await currentSession(req, context, 'feedback.' + feedbackOperation);
      const data = await native.feedback(current.authority, feedbackOperation, args);
      if (['submit', 'reply', 'status', 'accept'].includes(feedbackOperation)) await postCommit(context, current.authority);
      else { await freshRoot(context); native.withCurrent(current.authority, () => true); }
      let output;
      try { output = feedbackOutput(feedbackOperation, data); }
      catch { throw new SourceAppError(['submit', 'reply', 'status', 'accept'].includes(feedbackOperation) ? 'source_app_effect_unknown' : 'source_app_response_invalid', 503); }
      send(res, 200, { ok: true, data: output }); return;
    }
    if (url.pathname === '/api/embed/context') {
      const current = await currentSession(req, context, 'context');
      send(res, 200, { ready: true, resource: profile.resource.selection, authentication: 'verified-native-source',
        sessionExpiresAt: current.proof.sessionExpiresAt, accessExpiresAt: current.proof.expiresAt }); return;
    }
    throw new SourceAppError('source_app_route_unavailable', 404);
  }
  return Object.freeze({ profile, async handleRequest(req, res) {
    let url;
    try { url = new URL(req.url, profile.nativeOrigin); }
    catch { send(res, 400, { ok: false, error: { code: 'source_app_input_invalid', retryable: false } }); return true; }
    if (!url.pathname.startsWith('/soty/') && !url.pathname.startsWith('/api/embed/')) return false;
    try {
      check(!closed && req.url === url.pathname + url.search && req.url.startsWith('/') && !req.url.startsWith('//')
        && !url.pathname.includes('%') && !url.pathname.includes('..'), 'source_app_route_unavailable', 404);
      if (url.pathname.startsWith('/soty/')) await nativeRoute(req, res, url); else await embedRoute(req, res, url);
    } catch (error) {
      if (!res.headersSent && url.pathname.startsWith('/soty/')) {
        const superseded = error.code === 'source_app_intent_superseded', unknown = !error.status || error.status >= 500;
        page(res, superseded ? 'Откройте вход заново' : unknown ? 'Вход пока не подтверждён' : 'Не удалось подтвердить вход',
          '<p>' + (superseded ? 'Этот вход заменён более новым. Вернитесь в Соты и начните вход снова.'
            : unknown ? 'Вернитесь в Соты и проверьте завершение входа. Если ответа нет, повторите вход явно; новый профиль создавать не нужно.'
              : 'Подтвердите свой профиль в приложении и откройте вход заново из Сот.') + '</p>', {}, Number.isInteger(error.status) ? error.status : 503);
      } else if (!res.headersSent) send(res, Number.isInteger(error.status) ? error.status : 503, { ok: false, error: { code: error.code || 'source_app_unknown', retryable: !error.status || error.status >= 500 } });
      else res.destroy();
    }
    return true;
  }, close() { closed = true; native.close(); } });
}
