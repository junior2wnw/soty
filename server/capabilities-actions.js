import { AccessError } from '../modules/capabilities/server/validation.mjs';
import { InvocationError } from '../modules/capabilities/server/invocations.mjs';
import { NotesError } from '../modules/notes/server/validation.mjs';
import { ConnectError } from '../modules/connect/server/index.mjs';
import { validateDiscoveryOrigin } from './capabilities-discovery.js';
import { CapabilityHttpError, createNativeIngress, parseDelegationJson, singleHeader } from './capabilities-ingress.js';
import { CAPABILITIES_BASE as BASE, NOTES_DRAFT_PATH as CREATE, INVOCATIONS_PATH as HISTORY,
  INVOCATION_ID_PATTERN, NATIVE_NOTE_ID_PATTERN, SERVICE_DELEGATION_PATH, SERVICE_DELEGATION_BODY_BYTES } from './capabilities-http-contract.js';

const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const INVOCATION_ID = new RegExp(INVOCATION_ID_PATTERN, 'u'), NOTE_ID = new RegExp(NATIVE_NOTE_ID_PATTERN, 'u');
const requireValue = (value, code = 'internal_error') => { if (!value) throw new CapabilityHttpError(code); };
const safeId = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$/u.test(value) && !value.includes('..');
const timestamp = value => Number.isSafeInteger(value) && value >= 0;

export function validateCapabilityAudience({ audience = '', shellOrigins = [], enabled = false } = {}) {
  requireValue(typeof enabled === 'boolean' && (enabled === false || audience !== ''), 'capability_configuration_invalid');
  try { return validateDiscoveryOrigin({ discoveryOrigin: audience, shellOrigins }); }
  catch { throw new CapabilityHttpError('capability_configuration_invalid'); }
}

function routeFor(target) {
  const split = target.indexOf('?'), pathname = split < 0 ? target : target.slice(0, split);
  let decoded;
  try { decoded = decodeURIComponent(pathname); } catch { decoded = pathname; }
  const privateNamespace = [pathname, decoded].some(value =>
    [ `${BASE}/notes`, HISTORY, `${BASE}/grants` ].some(prefix => value === prefix || value.startsWith(`${prefix}/`)));
  if (!privateNamespace) return null;
  requireValue(Buffer.byteLength(target) <= 8192 && !/[^\u0021-\u007e]|[#\\]/u.test(target)
    && pathname === decoded && !pathname.includes('%') && split < 0, 'invalid_input');
  if (pathname === CREATE) return { kind: 'create', method: 'POST' };
  if (pathname === SERVICE_DELEGATION_PATH) return { kind: 'derive', method: 'POST' };
  requireValue(!pathname.startsWith(`${BASE}/grants`), 'invalid_input');
  const invocationId = pathname.startsWith(`${HISTORY}/`) ? pathname.slice(HISTORY.length + 1) : '';
  requireValue(INVOCATION_ID.test(invocationId), 'invocation_not_found');
  return { kind: 'get', method: 'GET', invocationId };
}

const BAD_INPUT = new Set(['invalid_input', 'invalid_unicode', 'expiry_invalid', 'invocation_invalid_arguments', 'notes_invalid_arguments']);
const LARGE = new Set(['payload_too_large', 'invocation_payload_too_large', 'notes_note_too_large']);
const LIMITED = new Set(['ingress_capacity', 'ingress_rate_limit', 'budget_exceeded', 'quota_exceeded', 'native_admission_limit', 'native_rate_limit', 'native_ledger_limit']);
const UNAVAILABLE = new Set(['capability_disabled', 'native_unavailable', 'native_store_mismatch', 'native_storage_busy',
  'connect_authority_busy', 'service_closed', 'notes_storage_corrupt', 'capabilities_storage_corrupt',
  'oauth_unavailable', 'oauth_storage_busy', 'oauth_storage_key_unavailable',
  'native_legacy_invocation_unsupported', 'native_attempt_not_started', 'delegation_unavailable',
  'delegation_context_invalid', 'delegation_storage_busy']);
function errorResponse(error) {
  if (!(error instanceof CapabilityHttpError || error instanceof AccessError || error instanceof InvocationError
    || error instanceof NotesError || error instanceof ConnectError)) return { status: 500, code: 'internal_error' };
  const code = error.code;
  if (BAD_INPUT.has(code)) return { status: 400, code: 'invalid_input' };
  if (LARGE.has(code)) return { status: 413, code: 'payload_too_large' };
  if (LIMITED.has(code)) return { status: 429, code, retry: 60 };
  if (UNAVAILABLE.has(code)) return { status: 503, code: 'service_unavailable', retry: 1 };
  const statuses = { authorization_required: 401, access_denied: 403, delegation_denied: 403, invocation_not_found: 404, not_found: 404,
    invocation_request_conflict: 409, unsupported_media_type: 415, unsupported_encoding: 415,
    request_timeout: 408, method_not_allowed: 405 };
  return Object.hasOwn(statuses, code) ? { status: statuses[code], code: code === 'not_found' ? 'invocation_not_found' : code }
    : { status: 500, code: 'internal_error' };
}

/** Explicit public fields: a future internal projection must not accidentally
 * disclose an input, credential, account or storage identity over this route. */
function projectInvocation(value) {
  requireValue(value && INVOCATION_ID.test(value.invocationId)
    && value.capabilityId === 'notes.createDraft' && value.version === 1
    && typeof value.status === 'string' && /^[a-z_]{1,40}$/u.test(value.status)
    && typeof value.cancelRequested === 'boolean' && ['none', 'committed', 'partial', 'unknown'].includes(value.effectState)
    && timestamp(value.createdAt) && timestamp(value.updatedAt)
    && (value.completedAt === undefined || timestamp(value.completedAt))
    && Array.isArray(value.effects) && value.effects.length <= 32);
  const effects = value.effects.map(effect => {
    requireValue(['created', 'updated', 'deleted', 'published', 'sent', 'charged'].includes(effect.kind)
      && safeId(effect.resourceType) && safeId(effect.resourceId)
      && (effect.revision === undefined || timestamp(effect.revision)));
    return { kind: effect.kind, resourceType: effect.resourceType, resourceId: effect.resourceId,
      ...(effect.revision === undefined ? {} : { revision: effect.revision }) };
  });
  const output = { invocationId: value.invocationId, capabilityId: value.capabilityId, version: 1,
    status: value.status, cancelRequested: value.cancelRequested, effectState: value.effectState,
    effects, createdAt: value.createdAt, updatedAt: value.updatedAt,
    ...(value.completedAt === undefined ? {} : { completedAt: value.completedAt }) };
  if (value.receipt !== undefined) {
    const receipt = value.receipt;
    requireValue(receipt && ['domain_read', 'artifact_hash', 'test', 'handler_assertion', 'unverified'].includes(receipt.verificationMethod)
      && Array.isArray(receipt.artifacts) && receipt.artifacts.length <= 32
      && (receipt.errorCode === undefined || typeof receipt.errorCode === 'string' && /^[a-z][a-z0-9_]{1,79}$/u.test(receipt.errorCode)));
    output.receipt = { verificationMethod: receipt.verificationMethod,
      artifacts: receipt.artifacts.map(artifact => {
        requireValue(safeId(artifact.type) && safeId(artifact.id) && (artifact.revision === undefined || timestamp(artifact.revision))
          && (artifact.sha256 === undefined || typeof artifact.sha256 === 'string' && /^[a-f0-9]{64}$/u.test(artifact.sha256)));
        return { type: artifact.type, id: artifact.id, ...(artifact.revision === undefined ? {} : { revision: artifact.revision }),
          ...(artifact.sha256 === undefined ? {} : { sha256: artifact.sha256 }) };
      }), ...(receipt.errorCode === undefined ? {} : { errorCode: receipt.errorCode }) };
  }
  return output;
}

/** Fixed Notes operations shared by HTTP and MCP. The origin is a trusted host
 * configuration, distinct from the audience bound to the presented credential.
 * No actorless result or current Note content crosses this port. */
export function createCapabilityOperations({ service, origin = '' } = {}) {
  requireValue(service && typeof service.authenticateCredential === 'function'
    && typeof origin === 'string', 'capability_configuration_invalid');
  const coordinator = () => {
    requireValue(service.nativeNotes, 'native_unavailable');
    return service.nativeNotes;
  };
  const read = ({ actor, invocationId, reused } = {}) => {
    const invocation = projectInvocation(coordinator().get({ actor, invocationId }).invocation);
    const body = { invocation, ...(reused === undefined ? {} : { reused }) };
    if (invocation.status === 'succeeded' && invocation.effectState === 'committed'
      && invocation.receipt?.verificationMethod === 'domain_read') {
      const artifacts = invocation.receipt.artifacts;
      if (artifacts.length === 1 && artifacts[0].type === 'note' && NOTE_ID.test(artifacts[0].id)
        && artifacts[0].revision === 1 && invocation.effects.length === 1
        && invocation.effects[0].kind === 'created' && invocation.effects[0].resourceType === 'note'
        && invocation.effects[0].resourceId === artifacts[0].id && invocation.effects[0].revision === 1) {
        requireValue(origin, 'native_unavailable');
        body.result = { noteId: artifacts[0].id, revision: 1, url: `${origin}/#notes/${artifacts[0].id}` };
      }
    }
    return { status: 200, body };
  };
  const authenticate = ({ authorization, audience = origin } = {}) => {
    requireValue(origin && (audience === origin || audience === `${origin}/mcp`), 'authorization_required');
    requireValue(typeof authorization === 'string' && authorization.length <= 128, 'authorization_required');
    const bearer = /^Bearer ([^\s,]+)$/iu.exec(authorization);
    requireValue(bearer, 'authorization_required');
    const token = bearer[1];
    if (token.startsWith('soty_cap_')) return service.authenticateCredential({ token, audience });
    requireValue(typeof service.oauth?.authenticateBearer === 'function', 'authorization_required');
    return service.oauth.authenticateBearer({ token, audience });
  };
  const create = ({ actor, title, body, idempotencyKey } = {}) => {
    const native = coordinator();
    const admitted = native.admit({ actor, idempotencyKey, input: { title, body } });
    const invocationId = admitted.invocation.invocationId, reused = admitted.reused;
    if (!TERMINAL.has(admitted.invocation.status)) {
      try {
        const recovered = native.reconcile({ invocationId });
        if (recovered.outcome === 'retryable') {
          const attempt = native.beginAttempt({ invocationId });
          if (attempt.started) native.execute({ invocationId });
        }
      } catch { /* A durable state and a fresh authorized read decide the result. */ }
    }
    const result = read({ actor, invocationId, reused });
    const invocation = result.body.invocation;
    result.status = !TERMINAL.has(invocation.status) ? 202
      : invocation.status === 'succeeded' && reused === false ? 201 : 200;
    return result;
  };
  return Object.freeze({ authenticate, create, read, failure: errorResponse });
}

function send(res, status, value) {
  if (res.destroyed || res.writableEnded) return;
  const body = Buffer.from(JSON.stringify(value)); requireValue(body.length <= 65536);
  res.status(status).set({ 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': String(body.length) }).end(body);
}

/** The only route that returns a new one-shot service secret. Select fields
 * explicitly so future domain internals cannot become part of this response. */
function projectDelegation(value, audience) {
  const { principal, grant, credential, token } = value ?? {};
  requireValue(principal && grant && credential && typeof token === 'string'
    && /^soty_cap_[A-Za-z0-9_-]{43}$/u.test(token));
  const select = (source, keys) => Object.fromEntries(keys.map(key => [key, source[key]]));
  requireValue(['id', 'accountId', 'clientId'].every(key => safeId(principal[key]))
    && typeof principal.label === 'string' && principal.label.length >= 1 && principal.label.length <= 100
    && principal.kind === 'service' && principal.state === 'active' && timestamp(principal.createdAt)
    && principal.revokedAt === null);
  requireValue(['id', 'accountId', 'principalId', 'clientId', 'parentGrantId', 'rootGrantId'].every(key => safeId(grant[key]))
    && grant.accountId === principal.accountId && grant.clientId === principal.clientId && grant.principalId === principal.id
    && grant.capabilities?.length === 1 && grant.capabilities[0].capabilityId === 'notes.createDraft' && grant.capabilities[0].version === 1
    && ['resources', 'effects', 'recipients'].every(key => Array.isArray(grant[key]) && grant[key].length === 1)
    && grant.resources[0] === 'notes:new' && grant.effects[0] === 'create' && grant.recipients[0] === 'soty:notes'
    && grant.allowDelegation === false && grant.maxDepth === 0 && Number.isSafeInteger(grant.depth) && grant.depth > 0
    && timestamp(grant.expiresAt) && timestamp(grant.createdAt) && grant.revokedAt === null && timestamp(grant.policyEpoch));
  requireValue(safeId(credential.id) && credential.audience === audience && credential.grantId === grant.id
    && credential.expiresAt === grant.expiresAt && timestamp(credential.expiresAt) && timestamp(credential.createdAt));
  return {
    principal: select(principal, ['id', 'accountId', 'clientId', 'label', 'kind', 'state', 'createdAt', 'revokedAt']),
    grant: { ...select(grant, ['id', 'accountId', 'principalId', 'clientId', 'parentGrantId', 'rootGrantId', 'allowDelegation',
      'maxDepth', 'depth', 'expiresAt', 'createdAt', 'revokedAt', 'policyEpoch']),
      capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'] },
    credential: select(credential, ['id', 'audience', 'expiresAt', 'createdAt', 'grantId']), token,
  };
}

/** Host composition only. This is not a generic invocation/executor gateway. */
export function attachCapabilitiesActions(app, { service, audience = '', ingressOptions, resourceMetadata = null } = {}) {
  requireValue(service && typeof service.authenticateCredential === 'function', 'capability_configuration_invalid');
  requireValue(resourceMetadata === null || (audience !== ''
    && resourceMetadata === `${audience}/.well-known/oauth-protected-resource`), 'capability_configuration_invalid');
  const ingress = createNativeIngress(ingressOptions);
  const operations = createCapabilityOperations({ service, origin: audience });
  const host = audience ? new URL(audience).host : null;
  const secureResource = audience !== '' && new URL(audience).protocol === 'https:';
  const status = () => {
    let ready = false;
    try { ready = Boolean(audience) && service.nativeNotes?.readiness().ready === true; } catch { /* Fail closed. */ }
    return { notesCreateEnabled: ready, audience: audience || null };
  };
  app.use(async (req, res, next) => {
    let lease, route;
    try {
      route = routeFor(req.originalUrl || req.url);
      if (!route) { next(); return; }
      res.set({ 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer' });
      requireValue(audience, 'native_unavailable');
      const requestedHost = singleHeader(req, 'host');
      requireValue(typeof requestedHost === 'string' && requestedHost.toLowerCase() === host, 'invalid_input');
      const origin = singleHeader(req, 'origin');
      requireValue(origin === undefined || origin === audience, 'access_denied');
      // Express derives this only from TLS or the host's explicitly trusted
      // proxy. An arbitrary Forwarded/X-Forwarded-Proto header is not authority.
      requireValue(!secureResource || req.secure === true, 'access_denied');
      if (req.method !== route.method) { res.set('Allow', route.method); throw new CapabilityHttpError('method_not_allowed'); }
      if (route.kind !== 'get') lease = ingress.enter(req);
      else requireValue(singleHeader(req, 'transfer-encoding') === undefined
        && [undefined, '0'].includes(singleHeader(req, 'content-length')), 'invalid_input');
      // No lock spans the network read. Admission and the final read repeat the
      // live authority check under Connect -> Capabilities.
      const authorization = singleHeader(req, 'authorization');
      const actor = operations.authenticate({ authorization, audience });
      if (route.kind === 'derive') requireValue(/^Bearer soty_cap_[A-Za-z0-9_-]{43}$/iu.test(authorization), 'delegation_denied');
      else requireValue(service.nativeNotes, 'native_unavailable');
      let result;
      if (route.kind === 'create') {
        const { title, body, idempotencyKey } = await lease.read(req);
        result = operations.create({ actor, title, body, idempotencyKey });
      } else if (route.kind === 'derive') {
        const { label, expiresAt } = await lease.read(req, { parseJson: parseDelegationJson, maximumBytes: SERVICE_DELEGATION_BODY_BYTES });
        requireValue(typeof service.delegation?.derive === 'function', 'delegation_unavailable');
        const issued = service.delegation.derive({ actor, label, expiresAt });
        // A COMMIT may precede a lost/denied response. Never issue a replacement
        // key here; the owner can inspect and revoke the existing child.
        operations.authenticate({ authorization, audience });
        result = { status: 201, body: projectDelegation(issued, audience) };
      } else result = operations.read({ actor, invocationId: route.invocationId });
      if (route.kind === 'create') res.set('Location', `${audience}${HISTORY}/${result.body.invocation.invocationId}`);
      send(res, result.status, result.body);
    } catch (error) {
      res.set({ 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer' });
      const safe = errorResponse(error);
      if (!req.complete || !req.readableEnded) {
        // Discard unread bytes without buffering while the small rejection is
        // flushed. Leaving IncomingMessage paused can reset the TCP connection
        // before a sender receives its structured 413 on an oversized request.
        req.resume(); res.shouldKeepAlive = false; res.set('Connection', 'close');
      }
      if (safe.status === 401) res.set('WWW-Authenticate', `Bearer realm="soty"${resourceMetadata ? `, resource_metadata="${resourceMetadata}"` : ''}`);
      if (safe.retry && route?.kind !== 'derive') res.set('Retry-After', String(safe.retry));
      send(res, safe.status, { error: { code: safe.code } });
    } finally { lease?.release(); }
  });
  return Object.freeze({ status });
}
