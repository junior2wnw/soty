import { buildDiscoveryOpenApi } from '../modules/capabilities/server/openapi.mjs';
import { BUILTIN_CAPABILITIES } from '../modules/capabilities/server/catalog.mjs';
import { canonicalJson, freezeDeep } from '../modules/capabilities/server/validation.mjs';
import { NATIVE_HTTP_LIMITS } from './capabilities-ingress.js';
import { externalCapabilityTools, EXTERNAL_HTTP_ROUTES, externalOpenApiSchema } from './external-capabilities-contract.js';
import { CAPABILITIES_BASE as BASE, NOTES_DRAFT_PATH, INVOCATIONS_PATH, INVOCATION_ID_PATTERN,
  NATIVE_NOTE_ID_PATTERN, SERVICE_DELEGATION_PATH, SERVICE_DELEGATION_BODY_BYTES } from './capabilities-http-contract.js';

const ref = name => ({ $ref: `#/components/schemas/${name}` });
const object = (properties, required = Object.keys(properties)) => ({ type: 'object', properties, required, additionalProperties: false });
const timestamp = { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER };
const json = (description, schema) => ({ description, content: { 'application/json': { schema } } });
const header = description => ({ description, schema: { type: 'string' } });
const copy = value => structuredClone(value);

/** Composed HTTP surface. The domain's standalone discovery-only contract stays
 * reusable; the host adds only the private operations it actually attaches. */
export function buildCapabilitiesOpenApi({ oauthConfigured = false, mcpConfigured = false, externalConfigured = false } = {}) {
  const document = copy(buildDiscoveryOpenApi());
  const note = BUILTIN_CAPABILITIES.find(entry => entry.capabilityId === 'notes.createDraft' && entry.version === 1);
  document.info = { title: 'Soty capabilities HTTP API', version: '1.3.0', description:
    'Public discovery and two private, typed Notes operations. Availability is deployment-specific: read /status before a new invocation. '
    + 'Public discovery requires no credentials and never grants execution rights. Private routes require an audience-bound bearer credential and live grant; cookies are not authentication. '
    + (oauthConfigured ? 'This host supports owner-approved OAuth connections and manually issued service credentials. OAuth issuer and resource metadata are separate endpoints; configuration does not imply issuance is currently enabled. '
      : 'Private access uses owner-issued service credentials. ')
    + (mcpConfigured ? 'The separate POST /mcp endpoint uses stateless Streamable HTTP. Obtain its tool definitions through MCP; this document describes the typed HTTP operations. '
      : 'MCP transport is not configured on this host. ')
    + (externalConfigured ? 'Typed application actions are installed by the trusted host and independently enforce current source authority. No arbitrary commands, URLs or keys can be submitted for execution. '
      : !oauthConfigured && !mcpConfigured ? 'No OAuth, MCP, generic execution or reading of Notes content is described by this document. '
        : 'No generic execution or reading of current Notes content is exposed. ')
    + 'Public routes support GET/HEAD and public,no-cache with ETag; private routes and status use no-store. '
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
  schemas.ServiceDelegationRequest = object({
    label: { type: 'string', minLength: 1, maxLength: 100, 'x-soty-max-utf16-code-units': 100,
      description: 'Well-formed Unicode label for the separate service client. No normalization.' }, expiresAt: timestamp,
  });
  schemas.ServiceDelegationRequest['x-soty-max-raw-bytes'] = SERVICE_DELEGATION_BODY_BYTES;
  schemas.ServiceDelegationRequest.description = 'Exactly label and expiresAt; no duplicate decoded keys, nested values or authority fields. '
    + 'Expiry is a safe integer in milliseconds, in the future, within the current credential, every ancestor grant and the configured maximum TTL.';
  schemas.DerivedServicePrincipal = object({ id: ref('Identifier'), accountId: ref('Identifier'), clientId: ref('Identifier'),
    label: copy(schemas.ServiceDelegationRequest.properties.label), kind: { const: 'service' }, state: { const: 'active' },
    createdAt: timestamp, revokedAt: { type: 'null' } });
  const singleton = item => ({ type: 'array', minItems: 1, maxItems: 1, items: item });
  schemas.DerivedServiceGrant = object({ id: ref('Identifier'), accountId: ref('Identifier'), principalId: ref('Identifier'), clientId: ref('Identifier'),
    parentGrantId: ref('Identifier'), rootGrantId: ref('Identifier'),
    capabilities: singleton(object({ capabilityId: { const: 'notes.createDraft' }, version: { const: 1 } })),
    resources: singleton({ const: 'notes:new' }), effects: singleton({ const: 'create' }), recipients: singleton({ const: 'soty:notes' }),
    allowDelegation: { const: false }, maxDepth: { const: 0 }, depth: { type: 'integer', minimum: 1 },
    expiresAt: timestamp, createdAt: timestamp, revokedAt: { type: 'null' }, policyEpoch: timestamp });
  schemas.DerivedServiceCredential = object({ id: ref('Identifier'), audience: { type: 'string', maxLength: 512 },
    expiresAt: timestamp, createdAt: timestamp, grantId: ref('Identifier') });
  schemas.ServiceDelegationResponse = object({ principal: ref('DerivedServicePrincipal'), grant: ref('DerivedServiceGrant'),
    credential: ref('DerivedServiceCredential'), token: { type: 'string', pattern: '^soty_cap_[A-Za-z0-9_-]{43}$',
      'x-soty-one-shot-secret': true, description: 'Returned only on this successful response; never stored as plaintext or recoverable from the owner list.' } });
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
    description: 'Opaque credential for this exact canonical HTTP audience and a live create-only grant. '
      + (oauthConfigured ? 'A verified owner can approve an OAuth connection or issue a service credential. The MCP audience is separate and its tokens are not valid on these HTTP routes. '
        : 'The existing signed Connect owner flow issues and manages service credentials. ')
      + 'No cookie, public catalog entry or forwarded host grants this authority.' } };
  if (oauthConfigured) document['x-soty-oauth-discovery'] = {
    authorizationServer: '/.well-known/oauth-authorization-server/oauth',
    httpResource: '/.well-known/oauth-protected-resource',
    ...(mcpConfigured ? { mcpResource: '/.well-known/oauth-protected-resource/mcp' } : {}),
  };
  if (mcpConfigured) document['x-soty-mcp'] = {
    endpoint: '/mcp', transport: 'streamable-http', stateless: true, methods: ['POST'],
    protocolVersions: ['2026-07-28', '2025-11-25'],
    tools: ['catalog_search', 'catalog_get', 'notes_create_draft', 'invocations_get'],
    description: 'A current bearer bound to the separate canonical /mcp resource is required even for initialization and tool listing. Supported revisions have distinct framing; no session, GET event channel or resumable stream is exposed.',
  };
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
  const delegationFailures = errors([400, 401, 403, 405, 408, 413, 415, 429, 500, 503]);
  for (const status of [429, 503]) delete delegationFailures[status].headers;
  delegationFailures[403].description = 'Current parent authority cannot delegate, including an OAuth connection or exhausted delegation depth.';
  delegationFailures[429].description = 'Ingress or existing account principal/grant/credential quota exhausted. No automatic retry of issuance.';
  delegationFailures[500].description = 'Controlled failure; issuance may already have committed. Do not repeat automatically. The owner must inspect the child list/audit and revoke exact IDs.';
  delegationFailures[503].description = 'Delegation unavailable or storage/authority contention. An ambiguous response is not proof that issuance failed. No automatic repeat or replacement token.';
  document.tags.push({ name: 'Private access', description: 'Explicit, bounded service delegation; not a generic agent orchestrator.' });
  document.paths[SERVICE_DELEGATION_PATH] = { post: { tags: ['Private access'], security: [{ CapabilityBearer: [] }],
    operationId: 'deriveNotesServiceGrant', summary: 'Connect one bounded service helper',
    description: 'A current ordinary service credential for canonical H is required; OAuth-linked and /mcp credentials cannot delegate. '
      + 'Exactly one leaf client/principal/grant/credential is issued under the real Connect authority fence. Only new private Notes are allowed; no further delegation, separate budget or parent history access. '
      + 'The existing root invocation budget is shared. Expiry cannot exceed the issuing credential or any ancestor. '
      + 'No query/path alias, cookies, compression or wildcard CORS. Host/Origin and HTTPS are enforced even with the authorization server disabled. '
      + '16KiB raw body, the same process ingress pool and 15s deadline as Notes. Issuance does not require native execution readiness. '
      + 'The plaintext token is returned once with no-store. A lost response may leave the child committed: never automatically repeat issuance; inspect owner lists/service audit and revoke exact IDs. '
      + 'Parent grant, principal or creator revocation closes descendants. Revoking only the issuing key blocks future parent requests but does not disable existing helpers.',
    requestBody: { required: true, content: { 'application/json': { schema: ref('ServiceDelegationRequest') } } },
    responses: { 201: json('One child was issued. Protect the one-shot token; current authority is checked again before responding.', ref('ServiceDelegationResponse')),
      ...delegationFailures } } };
  document.paths[`${BASE}/status`].get.description = 'Current native Notes readiness and explicitly configured audience. Readiness does not grant access, reserve capacity or migrate storage.';
  document.paths[`${BASE}/openapi.json`].get.description = 'OpenAPI 3.1.2 for public discovery and the attached typed private HTTP operations. Local references only; schemas do not grant execution.';
  if (externalConfigured) {
    const tools = externalCapabilityTools(schemas);
    document.tags.push({ name: 'Private application actions', description: 'Only installed, pinned actions allowed by the current root grant and independent source authority.' });
    if (mcpConfigured) document['x-soty-mcp'].tools.push(...tools.map(tool => tool.name));
    for (const [route, toolName] of Object.entries(EXTERNAL_HTTP_ROUTES)) {
      const tool = tools.find(value => value.name === toolName), prefix = `AppAction_${route}`;
      schemas[prefix + 'Input'] = externalOpenApiSchema(tool.inputSchema);
      schemas[prefix + 'Output'] = externalOpenApiSchema(tool.outputSchema);
      const responses = { 200: json('Authorized typed result. A receipt is historical; metadata is not an execution grant.', ref(prefix + 'Output')),
        ...errors([400, 401, 403, 404, 405, 408, 409, 413, 415, 429, 500, 503]) };
      responses[413].description = 'The 64 KiB raw request or installed action input budget was exceeded.';
      if (['invoke', 'get', 'cancel'].includes(route)) responses[202] = json('An admitted action is not terminal; retain its exact invocation and intention.', ref(prefix + 'Output'));
      if (route === 'invoke') responses[201] = json('A new action has a source-verified committed result.', ref(prefix + 'Output'));
      document.paths[`${BASE}/app-actions/${route}`] = { post: {
        tags: ['Private application actions'], security: [{ CapabilityBearer: [] }], operationId: toolName,
        summary: tool.description.split('.')[0], description: tool.description + ' Exact Host and Origin, audience-bound bearer, current Connect/App/Source authority, and a closed duplicate-free UTF-8 JSON body are required. '
          + '64 KiB raw body; no cookie authentication, query alias, command forwarding or endpoint selection. Rejected and unknown outcomes never prove absence of an effect.',
        requestBody: { required: true, content: { 'application/json': { schema: ref(prefix + 'Input') } } }, responses,
      } };
    }
  }
  canonicalJson(document, { maxBytes: 256 * 1024 });
  return freezeDeep(document);
}
