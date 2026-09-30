import { canonicalJson, freezeDeep } from './validation.mjs';
import { CAPABILITY_VALIDATION_PROFILE } from './documentation.mjs';
import { DISCOVERY_LIMITS } from './discovery.mjs';

const ref = name => ({ $ref: `#/components/schemas/${name}` });
const object = properties => ({ type: 'object', properties, required: Object.keys(properties), additionalProperties: false });
const list = (items, maxItems) => ({ type: 'array', items, ...(maxItems === undefined ? {} : { maxItems }) });
const string = (maxLength = 16384) => ({ type: 'string', maxLength });
const int = (minimum, maximum) => ({ type: 'integer', minimum, maximum });
const id = () => ref('Identifier');
const digest = () => ref('Digest');
const version = () => ref('Version');
const ids = () => ({ ...list(id(), 32), uniqueItems: true });
const BASE = '/api/capabilities/v1';
const VERSION_PATH = `${BASE}/catalog/{id}/versions/{version}`;

const schemas = {
  Identifier: { type: 'string', minLength: 1, maxLength: 160, pattern: '^(?!.*\\.\\.)[A-Za-z0-9][A-Za-z0-9_.:@/-]*$' },
  Version: int(1, 1000000),
  Digest: { type: 'string', pattern: '^[a-f0-9]{64}$', minLength: 64, maxLength: 64 },
  Cursor: { type: ['string', 'null'], maxLength: DISCOVERY_LIMITS.cursorBytes, pattern: '^[A-Za-z0-9_-]+$' },
  Links: object(Object.fromEntries(['html', 'detail', 'contract', 'inputSchema', 'outputSchema'].map(name => [name,
    { type: 'string', maxLength: 1024, pattern: '^/(agents|api/capabilities/v1)/' }]))),
  CatalogItem: object({ capabilityId: id(), version: version(), appId: id(), title: string(), summary: string(),
    digest: digest(), executionEnabled: { type: 'boolean' }, match: { enum: ['exact-id', 'id-prefix', 'text', 'browse'] }, links: ref('Links') }),
  CatalogPage: object({ scope: { const: 'public' }, revision: digest(), items: list(ref('CatalogItem'), DISCOVERY_LIMITS.maxPage),
    total: int(0, DISCOVERY_LIMITS.versions), cursor: ref('Cursor') }),
  // This describes the schema document as data. It does not replace a pinned
  // capability input/output schema or turn it into an executable API operation.
  SchemaDocument: { type: 'object', required: ['type'], additionalProperties: false, properties: {
    $schema: { const: 'https://json-schema.org/draft/2020-12/schema' },
    type: { enum: ['object', 'array', 'string', 'integer', 'number', 'boolean', 'null'] },
    title: string(160), description: string(2000), properties: { type: 'object', maxProperties: 32, additionalProperties: ref('SchemaDocument') },
    required: ids(), additionalProperties: { const: false }, items: ref('SchemaDocument'),
    minItems: int(0, 128), maxItems: int(0, 128), minLength: int(0, 100000), maxLength: int(0, 100000),
    minimum: { type: 'number' }, maximum: { type: 'number' }, enum: { ...list({}, 64), minItems: 1 },
  }, description: 'Exact schema data in the bounded Soty v1 subset. JSON Schema string length uses Unicode characters; the legacy runtime additionally limits UTF-16 code units, canonical JSON bytes and control characters. See documentation.validation.' },
  ExecutionBinding: object({ kind: { const: 'native' }, handler: id(), version: { const: 1 } }),
  Charge: object({ unit: { const: 'invocations' }, amount: { const: 1 } }),
  Example: object({ input: {}, output: {} }),
  LocaleDocumentation: object({ title: string(), summary: string(), useWhen: list(string()), notFor: list(string()), examples: list(ref('Example'), 128) }),
  Documentation: object({ revision: digest(), contractDigest: digest(), locales: object({ ru: ref('LocaleDocumentation'), en: ref('LocaleDocumentation') }),
    validation: { const: CAPABILITY_VALIDATION_PROFILE } }),
  CapabilityDetail: object({ scope: { const: 'public' }, capability: ref('CapabilityVersion'), documentation: ref('Documentation'), links: ref('Links') }),
  Status: object({ notesCreateEnabled: { const: false }, audience: { type: 'null' } }),
  Error: object({ error: object({ code: { type: 'string', pattern: '^[a-z][a-z0-9_]{0,95}$' } }) }),
  OpenApiDocument: { type: 'object', required: ['openapi', 'info', 'paths'], properties: {
    openapi: { const: '3.1.2' }, info: { type: 'object' }, paths: { type: 'object' },
  }, additionalProperties: true },
};
const semanticFields = { capabilityId: id(), version: version(), appId: id(), title: string(160), description: string(2000), visibility: { const: 'public' },
  inputSchema: ref('SchemaDocument'), outputSchema: ref('SchemaDocument'), resources: ids(), effects: ids(), recipients: ids(), executionBinding: ref('ExecutionBinding') };
schemas.SemanticContract = object(semanticFields);
schemas.CapabilityVersion = object({ ...semanticFields, executionEnabled: { type: 'boolean' }, digest: digest(), charges: { ...list(ref('Charge'), 1), minItems: 1 } });

const params = {
  CapabilityId: { name: 'id', in: 'path', required: true, schema: id(),
    description: 'Case-sensitive ID, percent-encoded as one path segment, including / and @. Decoded exactly once; unknown and private IDs are indistinguishable.' },
  Version: { name: 'version', in: 'path', required: true, schema: version(),
    description: 'Canonical positive decimal integer, without leading zeros.' },
  Query: { name: 'query', in: 'query', required: false, schema: { type: 'string', default: '', maxLength: 200,
    'x-soty-max-utf16-code-units': 200, 'x-soty-max-utf8-bytes': 800 },
    description: 'Well-formed Unicode, at most 200 UTF-16 code units and 800 UTF-8 bytes before NFKC/lowercase/trim. At most 12 whitespace tokens after normalization; all tokens must match public text. Unknown or duplicate query parameters are rejected.' },
  Limit: { name: 'limit', in: 'query', required: false, schema: { ...int(1, DISCOVERY_LIMITS.maxPage), default: DISCOVERY_LIMITS.defaultPage },
    description: 'Maximum items; the 64KiB response byte limit may produce a shorter page. Use the returned cursor.' },
  Cursor: { name: 'cursor', in: 'query', required: false, schema: { type: 'string', minLength: 1, maxLength: 512, pattern: '^[A-Za-z0-9_-]+$' },
    description: 'Public pagination position bound to query and catalog revision. Invalid or stale cursor: 400 cursor_invalid; restart from the first page.' },
  Kind: { name: 'kind', in: 'path', required: true, schema: { enum: ['input', 'output'] } },
};
const parameter = name => ({ $ref: `#/components/parameters/${name}` });
const versionParams = [parameter('CapabilityId'), parameter('Version')];
const jsonResponse = (description, schema) => ({ description, content: { 'application/json': { schema } } });
const errorResponses = Object.fromEntries(['400', '404', '405', '414', '500'].map(code => [code, { $ref: '#/components/responses/Error' }]));
function get(operationId, summary, description, schema, parameters = [], cache = true) {
  return { operationId, summary, description, tags: ['Public discovery'], security: [], parameters,
    responses: { '200': jsonResponse(description, schema), ...(cache ? { '304': { description: 'Unchanged public representation. No body.' } } : {}), ...errorResponses } };
}

const document = {
  openapi: '3.1.2', jsonSchemaDialect: 'https://json-schema.org/draft/2020-12/schema',
  info: { title: 'Soty public capability discovery', version: '1.0.0',
    description: 'Read-only, unauthenticated discovery of explicitly public capability versions. Cookies or bearer tokens do not change the public view. A capability description and its schema do not grant execution access. Notes creation is disabled in this release; there are no write, OAuth or MCP endpoints in this document. HEAD returns GET headers without a body. Successful documents use public,no-cache and ETag; errors and readiness use no-store.' },
  servers: [{ url: '/' }], security: [], tags: [{ name: 'Public discovery', description: 'Public metadata only; no user Notes or private descriptors.' }],
  paths: {
    [`${BASE}/catalog`]: { get: get('searchPublicCapabilities', 'Search public capability versions',
      'Compact Russian/English lexical search. Filter public metadata before ranking, totals and pagination. Exact ID precedes prefix ID and text matches; ties use ordinal ID and numeric version. Total counts public matches. Response at most 64KiB; a single item at most 4KiB.',
      ref('CatalogPage'), [parameter('Query'), parameter('Limit'), parameter('Cursor')]) },
    [VERSION_PATH]: { get: get('getPublicCapability', 'Read a public capability version',
      'Exact registered capability and independently revised documentation. The semantic digest excludes operational executionEnabled and generated digest/charges. Detail at most 384KiB. Documentation is plain data, not instructions granting permissions.', ref('CapabilityDetail'), versionParams) },
    [`${VERSION_PATH}/contract.json`]: { get: get('getPublicCapabilityContract', 'Download the exact semantic contract bytes',
      'Canonical JSON object, without a wrapper or trailing newline. SHA-256 of the UTF-8 response body equals capability.digest. Serialization is sorted own keys plus ECMAScript JSON serialization; JCS conformance is not claimed. The contract does not contain executionEnabled, digest, charges or documentation.',
      ref('SemanticContract'), versionParams) },
    [`${VERSION_PATH}/schemas/{kind}`]: { get: get('getPublicCapabilitySchema', 'Download an exact input or output schema',
      'The original schema object, without injected $id or $schema. Its body hash is not the whole capability digest. This GET returns schema data and does not execute the described capability. The existing runtime profile in documentation.validation imposes additional legacy limits.',
      ref('SchemaDocument'), [...versionParams, parameter('Kind')]) },
    [`${BASE}/status`]: { get: get('getCapabilityReadiness', 'Read current pilot readiness',
      'Notes execution is disabled and has no execution audience in this read-only release. This status grants no access.', ref('Status'), [], false) },
    [`${BASE}/openapi.json`]: { get: get('getDiscoveryOpenApi', 'Read this OpenAPI document',
      'OpenAPI 3.1.2 describes only implemented public read routes. All component references are local; no schema URL is fetched by the service.', ref('OpenApiDocument')) },
  },
  components: { schemas, parameters: params, responses: { Error: jsonResponse('Controlled error code only; no request content, credentials or internal exception detail.', ref('Error')) } },
};

// A fixed bounded document, not a cache keyed by URL, Host or query.
canonicalJson(document, { maxBytes: 128 * 1024 });
freezeDeep(document);
export function buildDiscoveryOpenApi() { return document; }
