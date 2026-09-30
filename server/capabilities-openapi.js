import { buildDiscoveryOpenApi } from '../modules/capabilities/server/openapi.mjs';
import { BUILTIN_CAPABILITIES } from '../modules/capabilities/server/catalog.mjs';
import { canonicalJson, freezeDeep } from '../modules/capabilities/server/validation.mjs';
import { NATIVE_HTTP_LIMITS } from './capabilities-ingress.js';
import { CAPABILITIES_BASE as BASE, NOTES_DRAFT_PATH, INVOCATIONS_PATH, INVOCATION_ID_PATTERN,
  NATIVE_NOTE_ID_PATTERN } from './capabilities-http-contract.js';

const ref = name => ({ $ref: `#/components/schemas/${name}` });
const object = (properties, required = Object.keys(properties)) => ({ type: 'object', properties, required, additionalProperties: false });
const timestamp = { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER };
const json = (description, schema) => ({ description, content: { 'application/json': { schema } } });
const header = description => ({ description, schema: { type: 'string' } });
const copy = value => structuredClone(value);

/** Composed HTTP surface. The domain's standalone discovery-only contract stays
 * reusable; the host adds only the private operations it actually attaches. */
export function buildCapabilitiesOpenApi() {
  const document = copy(buildDiscoveryOpenApi());
  const note = BUILTIN_CAPABILITIES.find(entry => entry.capabilityId === 'notes.createDraft' && entry.version === 1);
  document.info = { title: 'Soty capabilities HTTP API', version: '1.1.0', description:
    'Public discovery and two private, typed Notes operations. Availability is deployment-specific: read /status before a new invocation. '
    + 'Public discovery requires no credentials and never grants execution rights. Private routes require an owner-issued, audience-bound service credential and live grant; cookies are not authentication. '
    + 'No OAuth, MCP, generic execution or reading of Notes content is described by this document. Public routes support GET/HEAD and public,no-cache with ETag; private routes and status use no-store. '
    + 'Never retry an uncertain write with a new idempotency key: get its Invocation or repeat the original request.' };
  document.tags.push({ name: 'Private Notes', description: 'Create-only scope; own historical receipts contain no current Note content or existence check.' });
  const schemas = document.components.schemas;
  schemas.Status = object({ notesCreateEnabled: { type: 'boolean' }, audience: { type: ['string', 'null'], maxLength: 512,
    description: 'Explicit canonical resource origin; null means the private HTTP surface is not configured. A configured origin does not imply execution is enabled.' } });
  schemas.NativeDraftRequest = copy(note.inputSchema);
  schemas.NativeDraftRequest.properties.title['x-soty-max-utf16-code-units'] = 160;
  schemas.NativeDraftRequest.properties.body['x-soty-max-utf16-code-units'] = 100000;
  schemas.NativeDraftRequest.properties.idempotencyKey = { type: 'string', minLength: 8, maxLength: 160,
    pattern: '^[^\\u0000-\\u0020\\u007f]+$', 'x-soty-max-utf16-code-units': 160,
    description: 'Stable caller-chosen key for one intention. Reuse it for the identical title/body after a lost response. A different payload with the same key is a conflict. It conveys no authority.' };
  schemas.NativeDraftRequest.required.push('idempotencyKey');
  schemas.NativeDraftRequest.description = 'Exactly three strings, well-formed Unicode, with no duplicate (including escaped), unknown or server-selected fields. '
    + 'No normalization of title/body. The pinned capability and Notes document each have an additional 262144-byte budget; the larger HTTP limit only accommodates JSON escaping. '
    + 'Limits for title/body additionally count UTF-16 code units, so 80 astral emoji exhaust a 160-unit title.';
  schemas.NativeDraftRequest['x-soty-max-raw-bytes'] = NATIVE_HTTP_LIMITS.bodyBytes;
  schemas.NativeDraftRequest['x-soty-max-canonical-input-bytes'] = 262144;
  schemas.NativeInvocationId = { type: 'string', pattern: INVOCATION_ID_PATTERN, maxLength: 40 };
  schemas.NativeEffect = object({ kind: { enum: ['created', 'updated', 'deleted', 'published', 'sent', 'charged'] },
    resourceType: ref('Identifier'), resourceId: ref('Identifier'), revision: timestamp }, ['kind', 'resourceType', 'resourceId']);
  schemas.NativeArtifact = object({ type: ref('Identifier'), id: ref('Identifier'), revision: timestamp, sha256: ref('Digest') }, ['type', 'id']);
  schemas.NativeReceipt = object({ verificationMethod: { enum: ['domain_read', 'artifact_hash', 'test', 'handler_assertion', 'unverified'] },
    artifacts: { type: 'array', items: ref('NativeArtifact'), maxItems: 32 },
    errorCode: { type: 'string', pattern: '^[a-z][a-z0-9_]{1,79}$' } }, ['verificationMethod', 'artifacts']);
  schemas.NativeInvocation = object({ invocationId: ref('NativeInvocationId'), capabilityId: { const: 'notes.createDraft' }, version: { const: 1 },
    status: { type: 'string', pattern: '^[a-z_]{1,40}$', description: 'Only succeeded, failed and cancelled are terminal. Other states do not prove absence of an effect.' },
    cancelRequested: { type: 'boolean' }, effectState: { enum: ['none', 'committed', 'partial', 'unknown'] },
    effects: { type: 'array', items: ref('NativeEffect'), maxItems: 32 }, createdAt: timestamp, updatedAt: timestamp,
    completedAt: timestamp, receipt: ref('NativeReceipt') },
  ['invocationId', 'capabilityId', 'version', 'status', 'cancelRequested', 'effectState', 'effects', 'createdAt', 'updatedAt']);
  schemas.NativeNoteResult = object({ ...copy(note.outputSchema.properties),
    noteId: { type: 'string', pattern: NATIVE_NOTE_ID_PATTERN }, revision: { const: 1 },
    url: { type: 'string', maxLength: 1024, description: 'Authenticated PWA link to the mutable Note. Receipt revision 1 is historical; this URL does not promise the Note still exists or reveal later edits.' } });
  schemas.NativeReadResponse = object({ invocation: ref('NativeInvocation'), result: ref('NativeNoteResult') }, ['invocation']);
  schemas.NativeCreateResponse = object({ invocation: ref('NativeInvocation'), reused: { type: 'boolean' }, result: ref('NativeNoteResult') }, ['invocation', 'reused']);
  const failures = {
    400: 'Malformed path, query, headers, JSON, Unicode or input.',
    401: 'Missing, invalid, expired or wrong-audience credential.',
    403: 'Current grant, creator device or required scope no longer allows the request.',
    404: 'Unknown or inaccessible Invocation; foreign identities are indistinguishable.',
    405: 'Wrong method. The Allow header names the supported method.',
    408: 'The full request body did not arrive within 15 seconds; no new admission occurred.',
    409: 'Idempotency key already denotes a different request.',
    413: 'HTTP, canonical input or Notes document byte budget exceeded.',
    415: 'Use application/json with UTF-8 and no compression.',
    429: 'Ingress, native admission or grant budget exhausted. Retry-After is an interval, not a promise of renewed permission.',
    500: 'Controlled internal error. Read the known Invocation or retry the original key; an error is not proof of no effect.',
    503: 'Missing configuration, disabled execution, incompatible storage or transient storage contention. Historical replay remains possible when authorized and the required stores are readable.',
  };
  const errors = statuses => Object.fromEntries(statuses.map(code => [code, { ...json(failures[code], ref('Error')),
    ...([429, 503].includes(code) ? { headers: { 'Retry-After': header('Seconds before retrying; authorization and limits are checked again.') } }
      : code === 401 ? { headers: { 'WWW-Authenticate': header('Bearer realm="soty"') } }
        : code === 405 ? { headers: { Allow: header('Only the declared route method is accepted, including on HEAD and OPTIONS.') } } : {}) }]));
  const privateOperation = { tags: ['Private Notes'], security: [{ CapabilityBearer: [] }] };
  document.components.securitySchemes = { CapabilityBearer: { type: 'http', scheme: 'bearer',
    description: 'Opaque credential issued by the verified owner for this exact canonical audience and a create-only delegation grant. The existing signed Connect owner flow manages credentials. No cookie, public catalog entry or forwarded host grants this authority.' } };
  document.paths[NOTES_DRAFT_PATH] = { post: { ...privateOperation, operationId: 'createNoteDraft', summary: 'Create one private Note',
    description: 'Only notes.createDraft@1. No query parameters, redirects, compressed body or wildcard browser CORS. Host must match the configured audience; Origin, when present, must equal it. '
      + 'Each auth/header must be unambiguous. Body deadline is 15 seconds, with at most 8 body readers per process, 2 per socket peer and 60 attempts per peer per minute. '
      + 'Admission rechecks authority after the body; the final response uses a fresh authorized history read. Loss of authority after the effect can deny the response while the Note remains committed. '
      + 'A matching replay returns its historical receipt, including after human edits or deletion, without recreating the Note. Temporary disable prevents new work but does not erase unresolved intent.',
    requestBody: { required: true, content: { 'application/json': { schema: ref('NativeDraftRequest'),
      example: { title: 'План на завтра', body: 'Закончить макет\nПроверить результат', idempotencyKey: 'tomorrow-plan-001' } } } },
    responses: { ...Object.fromEntries([[201, 'A new Note was committed and verified.'], [200, 'Reused historical result or terminal failure; inspect invocation.status.'],
      [202, 'Accepted but not terminal. Poll the Location URI or retry the same input and key.']].map(([code, description]) => [code,
      { ...json(description, ref('NativeCreateResponse')), headers: { Location: header('Canonical URL of this Invocation; it contains no credential.') } }])),
    ...errors([400, 401, 403, 404, 405, 408, 409, 413, 415, 429, 500, 503]) } } };
  document.paths[`${INVOCATIONS_PATH}/{invocationId}`] = { get: { ...privateOperation, operationId: 'getOwnNoteInvocation',
    summary: 'Read the status and receipt of your Notes invocation',
    description: 'No query parameters or body. Requires current authority in the original account/client/principal/grant scope. A renewed credential can read its own history, but cannot replace an expired original credential to execute the old intention. '
      + 'Returns no title, body, input, account identifier, later edits or existence check. This read neither creates a Note nor retries execution. No HEAD or OPTIONS alias.',
    parameters: [{ name: 'invocationId', in: 'path', required: true, schema: ref('NativeInvocationId') }],
    responses: { 200: json('Content-free historical status; optional result only for a verified committed creation.', ref('NativeReadResponse')),
      ...errors([400, 401, 403, 404, 405, 500, 503]) } } };
  document.paths[`${BASE}/status`].get.description = 'Current native Notes readiness and explicitly configured audience. Readiness does not grant access, reserve capacity or migrate storage.';
  document.paths[`${BASE}/openapi.json`].get.description = 'OpenAPI 3.1.2 for public discovery and the attached typed private HTTP operations. Local references only; schemas do not grant execution.';
  canonicalJson(document, { maxBytes: 256 * 1024 });
  return freezeDeep(document);
}
