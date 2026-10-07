import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import { capture, closed, continuation, hash, identifier, need } from './profile.mjs';
import { selectedResourceProfile, RESOURCE_SOURCE_PROOF, RESOURCE_LAUNCH_CONTEXT, RESOURCE_TRANSPORT_LIMITS } from './resource-profile.mjs';
import { selectedRoute, selectedRouteAdapter } from './resource-route-adapters.mjs';

export const RESOURCE_PROOF_HEADER = 'x-soty-selected-proof';
export const RESOURCE_MAC_HEADER = 'x-soty-selected-mac';
const opaque = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value);
const mac = (key, value) => createHmac('sha256', key).update(value).digest('base64url');
function hostKey(key) { need(Buffer.isBuffer(key) && key.length === 32, 'scoped_embed_key_required'); return Buffer.from(key); }
function fields(request) {
  if (request instanceof Request) {
    const url = new URL(request.url);
    return { method: request.method, path: url.pathname + url.search, host: request.headers.get('host') ?? url.host,
      cookie: request.headers.get('cookie') ?? '', get: key => request.headers.get(key), count: () => 1 };
  }
  need(request && typeof request === 'object' && request.headers && typeof request.url === 'string', 'scoped_embed_proof_invalid', 403);
  return { method: request.method, path: request.url, host: request.headers.host, cookie: request.headers.cookie ?? '',
    get: key => request.headers[key], count: key => (request.rawHeaders ?? []).filter((v, i) => i % 2 === 0 && v.toLowerCase() === key).length };
}
function currentContext(input, profile) {
  const value = capture(input);
  closed(value, ['schema', 'reference', 'profileDigest', 'appId', 'sourceProfile', 'resource', 'rootPrincipal', 'humanPrincipal',
    'entry', 'target', 'policyEpoch', 'expiresAt']);
  continuation(value.reference); closed(value.rootPrincipal, ['accountId', 'deviceId']); Object.values(value.rootPrincipal).forEach(identifier);
  closed(value.humanPrincipal, ['issuer', 'subject', 'clientId', 'clientProfileDigest', 'clientGeneration']);
  const human = value.humanPrincipal;
  need(human.issuer === profile.issuer && human.clientId === profile.clientId && typeof human.subject === 'string'
    && human.subject.length > 0 && human.subject.length <= 128 && human.subject.isWellFormed()
    && /^[a-f0-9]{64}$/u.test(human.clientProfileDigest) && Number.isSafeInteger(human.clientGeneration) && human.clientGeneration > 0,
    'scoped_embed_human_principal_invalid', 403);
  closed(value.entry, ['domainId', 'origin']); identifier(value.entry.domainId);
  need(value.schema === RESOURCE_LAUNCH_CONTEXT && value.profileDigest === profile.digest && value.appId === profile.appId
    && hash(value.sourceProfile) === hash(profile.sourceProfile) && hash(value.resource) === hash(profile.resource)
    && hash(value.target) === hash(profile.target) && value.entry.origin === profile.embedOrigin
    && Number.isSafeInteger(value.policyEpoch) && value.policyEpoch > 0 && Number.isSafeInteger(value.expiresAt) && value.expiresAt > 0,
    'scoped_embed_context_mismatch', 403);
  return value;
}
function bodyBytes(body) { need(body instanceof Uint8Array, 'scoped_embed_body_invalid'); return Buffer.from(body); }

/** Private connector-only key. The signed launch and Native/OIDC login remain
 * separate proofs; this MAC grants neither an account nor a project role. */
export function createResourceSourceProofSigner({ profile: raw, key, clock = Date.now } = {}) {
  const profile = selectedResourceProfile(raw), secret = hostKey(key); selectedRouteAdapter(profile);
  return Object.freeze({
    headers({ context, method, path, body = Buffer.alloc(0), cookie = '' }) {
      const captured = currentContext(context, profile), route = selectedRoute(profile, method, path), bytes = bodyBytes(body);
      need(captured.expiresAt > clock(), 'scoped_embed_expired', 401);
      need(bytes.length <= route.requestBytes && typeof cookie === 'string' && Buffer.byteLength(cookie) <= 4096,
        'scoped_embed_request_limit', 413);
      const proof = { schema: RESOURCE_SOURCE_PROOF, context: captured, method, path, bodyDigest: hash(bytes.toString('base64')),
        cookieDigest: hash(cookie), nonce: randomBytes(32).toString('base64url'), expiresAt: Math.min(captured.expiresAt, clock() + RESOURCE_TRANSPORT_LIMITS.proofMs) };
      const text = Buffer.from(JSON.stringify(proof)).toString('base64url');
      need(text.length <= RESOURCE_TRANSPORT_LIMITS.proofHeaderBytes, 'scoped_embed_proof_limit', 413);
      return Object.freeze({ [RESOURCE_PROOF_HEADER]: text, [RESOURCE_MAC_HEADER]: mac(secret, text) });
    },
    probeHeaders() {
      const proof = { schema: 'soty.selected-source-probe.v2', profileDigest: profile.digest,
        nonce: randomBytes(32).toString('base64url'), expiresAt: clock() + RESOURCE_TRANSPORT_LIMITS.proofMs };
      const text = Buffer.from(JSON.stringify(proof)).toString('base64url');
      return { request: Object.freeze(proof), headers: { 'x-soty-selected-probe': text, [RESOURCE_MAC_HEADER]: mac(secret, 'probe\0' + text) } };
    },
    verifyReady(response, expected) {
      const text = response.headers.get('x-soty-selected-ready'), signature = response.headers.get(RESOURCE_MAC_HEADER);
      need(opaque(signature) && typeof text === 'string' && text.length <= 2048, 'scoped_embed_probe_invalid', 503);
      need(timingSafeEqual(Buffer.from(signature), Buffer.from(mac(secret, 'ready\0' + text))), 'scoped_embed_probe_invalid', 503);
      let value; try { value = capture(JSON.parse(Buffer.from(text, 'base64url').toString('utf8'))); } catch { need(false, 'scoped_embed_probe_invalid', 503); }
      need(response.status === 204 && hash(value) === hash(expected) && value.expiresAt > clock(), 'scoped_embed_probe_invalid', 503);
      return true;
    },
  });
}

/** Async nonce consumption supports real D1/PostgreSQL transactions. Consumers
 * must atomically insert one nonce and bound/compact expired transport rows. */
export function createResourceSourceProofVerifier({ profile: raw, key, consumeNonce, clock = Date.now } = {}) {
  const profile = selectedResourceProfile(raw), secret = hostKey(key); selectedRouteAdapter(profile);
  need(typeof consumeNonce === 'function', 'scoped_embed_nonce_store_required');
  const seen = new WeakMap(), verifying = new WeakSet();
  function authenticate(text, signature, purpose = '') {
    need(typeof text === 'string' && /^[A-Za-z0-9_-]+$/u.test(text) && text.length <= RESOURCE_TRANSPORT_LIMITS.proofHeaderBytes
      && opaque(signature) && timingSafeEqual(Buffer.from(signature), Buffer.from(mac(secret, purpose + text))), 'scoped_embed_proof_invalid', 403);
    try { return capture(JSON.parse(Buffer.from(text, 'base64url').toString('utf8'))); }
    catch { need(false, 'scoped_embed_proof_invalid', 403); }
  }
  async function consume(proof) {
    const before = clock(); need(opaque(proof.nonce) && Number.isSafeInteger(proof.expiresAt) && proof.expiresAt > before
      && proof.expiresAt <= before + RESOURCE_TRANSPORT_LIMITS.proofMs, 'scoped_embed_proof_expired', 401);
    need(await consumeNonce(proof.nonce, proof.expiresAt) === true, 'scoped_embed_proof_replayed', 403);
    need(proof.expiresAt > clock(), 'scoped_embed_proof_expired', 401);
  }
  return Object.freeze({
    async verify(request, { body = Buffer.alloc(0) } = {}) {
      need(!seen.has(request) && !verifying.has(request), 'scoped_embed_proof_replayed', 403); verifying.add(request);
      try {
        const f = fields(request);
        need(f.host === new URL(profile.embedOrigin).host && f.count(RESOURCE_PROOF_HEADER) <= 1 && f.count(RESOURCE_MAC_HEADER) <= 1,
          'scoped_embed_proof_invalid', 403);
        const proof = authenticate(f.get(RESOURCE_PROOF_HEADER), f.get(RESOURCE_MAC_HEADER));
        closed(proof, ['schema', 'context', 'method', 'path', 'bodyDigest', 'cookieDigest', 'nonce', 'expiresAt']);
        const context = currentContext(proof.context, profile), route = selectedRoute(profile, f.method, f.path), bytes = bodyBytes(body);
        need(proof.schema === RESOURCE_SOURCE_PROOF && proof.method === f.method && proof.path === f.path
          && bytes.length <= route.requestBytes && proof.bodyDigest === hash(bytes.toString('base64')) && proof.cookieDigest === hash(f.cookie)
          && context.expiresAt >= proof.expiresAt, 'scoped_embed_proof_mismatch', 403);
        await consume(proof); seen.set(request, context); return context;
      } finally { verifying.delete(request); }
    },
    async verifyReady(request) {
      const f = fields(request);
      need(f.method === 'HEAD' && f.path === '/api/embed/transport-ready' && f.host === new URL(profile.embedOrigin).host,
        'scoped_embed_probe_invalid', 403);
      const text = f.get('x-soty-selected-probe'), proof = authenticate(text, f.get(RESOURCE_MAC_HEADER), 'probe\0');
      closed(proof, ['schema', 'profileDigest', 'nonce', 'expiresAt']);
      need(proof.schema === 'soty.selected-source-probe.v2' && proof.profileDigest === profile.digest, 'scoped_embed_probe_invalid', 403);
      await consume(proof);
      return Object.freeze({ 'x-soty-selected-ready': text, [RESOURCE_MAC_HEADER]: mac(secret, 'ready\0' + text) });
    },
    context(request) { const context = seen.get(request); need(context, 'scoped_embed_transport_required', 401); return context; },
  });
}
