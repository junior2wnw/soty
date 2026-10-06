import { freezeDeep } from '../modules/capabilities/server/validation.mjs';

const object = (properties, required = Object.keys(properties)) => ({ type: 'object', properties, required, additionalProperties: false });

// One typed contract supplies MCP validators and the composed HTTP document.
// It contains no installed application names, URLs, credentials or authority.
export function externalCapabilityTools(base) {
  function standalone(input) {
    const needed = {}, pending = new Set();
    function visit(value) {
      if (Array.isArray(value)) return value.map(visit);
      if (!value || typeof value !== 'object') return value;
      return Object.fromEntries(Object.entries(value).map(([key, child]) => {
        if (key !== '$ref') return [key, visit(child)];
        const name = child.slice(child.lastIndexOf('/') + 1); pending.add(name); return [key, `#/$defs/${name}`];
      }));
    }
    const root = visit(input);
    for (const name of pending) {
      if (!base[name]) throw new Error('external_schema_invalid');
      needed[name] = visit(base[name]);
    }
    return { ...root, ...(pending.size ? { $defs: needed } : {}) };
  }
  const reference = object({ capabilityId: base.Identifier, version: base.Version, digest: base.Digest });
  const invocation = structuredClone(base.NativeInvocation);
  invocation.properties.capabilityId = base.Identifier; invocation.properties.version = base.Version;
  const response = standalone(object({ schema: { const: 'soty.external-invocation.v1' }, invocation,
    reused: { type: 'boolean' } }, ['schema', 'invocation']));
  const metadata = {
    reference, appId: base.Identifier, title: { type: 'string', maxLength: 160 }, description: { type: 'string', maxLength: 2000 },
    resources: { type: 'array', items: base.Identifier, maxItems: 32 }, effects: { type: 'array', items: base.Identifier, maxItems: 32 },
    recipients: { type: 'array', items: base.Identifier, maxItems: 32 }, executionEnabled: { type: 'boolean' }, availability: { const: 'unprobed' },
  };
  const tools = [
    { name: 'apps_catalog_search', description: 'Discover only application actions permitted by your current grant and app/resource authority. Availability is unprobed. Metadata does not grant rights.',
      inputSchema: object({ query: { type: 'string', maxLength: 200 }, limit: { type: 'integer', minimum: 1, maximum: 20 }, cursor: { type: 'string', maxLength: 512 } }, []),
      outputSchema: object({ schema: { const: 'soty.authorized-app-capabilities.v1' }, scope: { const: 'authorized' }, revision: base.Digest,
        items: { type: 'array', items: object(metadata), maxItems: 20 }, total: { type: 'integer', minimum: 0, maximum: 64 }, cursor: base.Cursor }), readOnly: true },
    { name: 'apps_catalog_get', description: 'Read one exact permitted application contract. The binding is a pinned reference; it cannot select a command, endpoint or key.',
      inputSchema: object({ reference }), outputSchema: standalone(object({ ...metadata, schema: { const: 'soty.authorized-app-capability.v1' }, scope: { const: 'authorized' },
        inputSchema: { $ref: '#/components/schemas/SchemaDocument' }, outputSchema: { $ref: '#/components/schemas/SchemaDocument' },
        binding: object({ id: base.Identifier, version: base.Version, digest: base.Digest }),
        skills: { type: 'array', maxItems: 0 }, docs: { type: 'array', maxItems: 0 } })), readOnly: true },
    { name: 'apps_invoke', description: 'Request one approved application action. Keep the exact reference, input and idempotencyKey after a lost response. An unknown outcome requires proof recovery; never invent a new request ID.',
      inputSchema: object({ reference, idempotencyKey: base.NativeDraftRequest.properties.idempotencyKey, input: { type: 'object' } }), outputSchema: response, readOnly: false },
    { name: 'apps_invocation_get', description: 'Read your historical application receipt under current grant and app/resource access. This never retries execution or reveals stored input.',
      inputSchema: object({ invocationId: base.NativeInvocationId }), outputSchema: response, readOnly: true },
    { name: 'apps_invocation_cancel', description: 'Request cancellation. A dispatched action may already have committed; cancellation does not undo an effect or release uncertain quota.',
      inputSchema: object({ invocationId: base.NativeInvocationId }), outputSchema: response, readOnly: false },
  ];
  return freezeDeep(tools.map(({ readOnly, ...tool }) => ({ ...tool,
    annotations: { readOnlyHint: readOnly, destructiveHint: !readOnly, idempotentHint: true, openWorldHint: false } })));
}

export const EXTERNAL_HTTP_ROUTES = Object.freeze({ catalog: 'apps_catalog_search', contract: 'apps_catalog_get',
  invoke: 'apps_invoke', get: 'apps_invocation_get', cancel: 'apps_invocation_cancel' });

// Standalone JSON Schemas carry their own $defs. OpenAPI references instead
// address the already declared document components; do not leave dangling
// document-root #/$defs references after embedding a schema in the document.
export function externalOpenApiSchema(value) {
  if (Array.isArray(value)) return value.map(externalOpenApiSchema);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.entries(value).filter(([key]) => key !== '$defs').map(([key, child]) =>
    [key, key === '$ref' ? child.replace(/^#\/\$defs\//u, '#/components/schemas/') : externalOpenApiSchema(child)]));
}
