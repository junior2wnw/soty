import { ProtocolError, INVALID_PARAMS } from '@modelcontextprotocol/server';
import { AjvJsonSchemaValidator } from '@modelcontextprotocol/server/validators/ajv';
import { buildCapabilitiesOpenApi } from './capabilities-openapi.js';
import { CapabilityHttpError } from './capabilities-ingress.js';
import { createCatalog } from '../modules/capabilities/server/catalog.mjs';
import { AccessError, freezeDeep } from '../modules/capabilities/server/validation.mjs';
import { DISCOVERY_LIMITS } from '../modules/capabilities/server/discovery.mjs';
import { createExternalCapabilityOperations } from './external-capabilities.js';

const object = (properties, required = Object.keys(properties)) => ({ type: 'object', properties, required, additionalProperties: false });
const catalog = createCatalog(), note = catalog.get('notes.createDraft', 1);
const validator = new AjvJsonSchemaValidator();

// OpenAPI references become local JSON Schema references, with only the
// reachable definitions included. This changes no domain schema semantics.
function standalone(name, schemas) {
  const definitions = {}, pending = new Set();
  function visit(value) {
    if (Array.isArray(value)) return value.map(visit);
    if (!value || typeof value !== 'object') return value;
    return Object.fromEntries(Object.entries(value).map(([key, item]) => {
      if (key !== '$ref') return [key, visit(item)];
      if (typeof item !== 'string' || !item.startsWith('#/components/schemas/')) throw new Error('mcp_schema_invalid');
      const target = item.slice('#/components/schemas/'.length);
      if (!Object.hasOwn(schemas, target)) throw new Error('mcp_schema_invalid');
      pending.add(target); return [key, `#/$defs/${target}`];
    }));
  }
  const root = visit(schemas[name]);
  for (const target of pending) if (!Object.hasOwn(definitions, target)) definitions[target] = visit(schemas[target]);
  return { ...root, ...(pending.size ? { $defs: definitions } : {}) };
}

function definitions() {
  const schemas = buildCapabilitiesOpenApi().components.schemas;
  const catalogSearch = object({ query: { type: 'string', maxLength: DISCOVERY_LIMITS.queryCodeUnits },
    limit: { type: 'integer', minimum: 1, maximum: DISCOVERY_LIMITS.maxPage }, cursor: structuredClone(schemas.Cursor) }, []);
  const values = [
    { name: 'catalog_search', description: 'Find public capabilities in Russian or English. Use the returned cursor with the same query for another page. Public metadata grants no execution rights.',
      inputSchema: catalogSearch, outputSchema: standalone('CatalogPage', schemas), readOnly: true },
    { name: 'catalog_get', description: 'Read one exact public capability version, its schemas, safety profile and examples. Unknown and private versions are indistinguishable.',
      inputSchema: object({ capabilityId: structuredClone(schemas.Identifier), version: structuredClone(schemas.Version) }),
      outputSchema: standalone('CapabilityDetail', schemas), readOnly: true },
    { name: 'notes_create_draft', description: 'Create one private Note with title and body. Use one stable idempotencyKey for the same intention; retry the identical input after a lost response. Never choose a new key automatically. The receipt describes creation, not current Note content.',
      inputSchema: standalone('NativeDraftRequest', schemas), outputSchema: standalone('NativeCreateResponse', schemas), readOnly: false },
    { name: 'invocations_get', description: 'Read your historical Notes invocation status and creation receipt. This does not execute, read current Note text or prove the Note still exists.',
      inputSchema: object({ invocationId: structuredClone(schemas.NativeInvocationId) }), outputSchema: standalone('NativeReadResponse', schemas), readOnly: true },
  ];
  return values.map(({ readOnly, ...tool }) => freezeDeep({ ...tool,
    annotations: { readOnlyHint: readOnly, destructiveHint: false, idempotentHint: true, openWorldHint: false } }));
}
export const MCP_TOOLS = Object.freeze(definitions());
const checks = new Map(MCP_TOOLS.map(tool => [tool.name, { tool,
  input: validator.getValidator(tool.inputSchema), output: validator.getValidator(tool.outputSchema) }]));
const catalogCodes = new Set(['invalid_input', 'query_invalid', 'cursor_invalid', 'not_found']);
const errorResult = code => ({ isError: true, content: [{ type: 'text', text: JSON.stringify({ error: { code } }) }] });

/** Request-local actor and invocation reference stay outside SDK authInfo/wire. */
export function createMcpTools({ service, operations, context }) {
  const external = createExternalCapabilityOperations({service,origin:context.origin ?? ''});
  async function externalCall(params) {
    if(Object.keys(params).some(key=>!['name','arguments','_meta'].includes(key))||!external.check(params.name,params.arguments??{}))return errorResult('invalid_input');
    try {
      context.current(); const args=params.arguments??{};
      const value=await external.call({actor:context.authenticate(),name:params.name,args});
      context.current(); external.recheck({actor:context.authenticate(),name:params.name,args,value});
      context.externalRead={name:params.name,args,value};
      return {content:[{type:'text',text:JSON.stringify(value)}],structuredContent:value};
    }catch(error){return errorResult(external.failure(error).code);}
  }
  return Object.freeze({
    list(params) {
      if (params !== undefined && (params === null || typeof params !== 'object'
        || Object.keys(params).some(key => key !== '_meta'))) throw new ProtocolError(INVALID_PARAMS, 'Invalid tool listing parameters');
      return { tools: service.external ? [...MCP_TOOLS,...external.tools] : MCP_TOOLS };
    },
    call(params) {
      if(service.external&&external.tools.some(tool=>tool.name===params?.name))return externalCall(params);
      const check = checks.get(params?.name);
      if (!check) throw new ProtocolError(INVALID_PARAMS, 'Unknown tool');
      if (Object.keys(params).some(key => !['name', 'arguments', '_meta'].includes(key))
        || !check.input(params.arguments ?? {}).valid) return errorResult('invalid_input');
      try {
        context.current();
        const actor = context.authenticate(), args = params.arguments ?? {};
        let value;
        if (params.name === 'catalog_search') value = service.catalog.search(args);
        else if (params.name === 'catalog_get') value = service.catalog.get(args);
        else {
          const result = params.name === 'notes_create_draft'
            ? operations.create({ actor, ...args }) : operations.read({ actor, ...args });
          value = result.body;
          context.privateRead = { invocationId: value.invocation.invocationId,
            ...(Object.hasOwn(value, 'reused') ? { reused: value.reused } : {}) };
          if (value.result) catalog.validateOutput(note, { noteId: value.result.noteId, revision: value.result.revision });
        }
        if (!check.output(value).valid) throw new CapabilityHttpError('internal_error');
        return { content: [{ type: 'text', text: JSON.stringify(value) }], structuredContent: value };
      } catch (error) {
        const code = params.name.startsWith('catalog_') && error instanceof AccessError && catalogCodes.has(error.code)
          ? error.code : operations.failure(error).code;
        return errorResult(code);
      }
    },
    outputSchema(name) { return checks.get(name)?.tool.outputSchema ?? (service.external ? external.outputSchema(name) : undefined); },
  });
}
