import { createHash } from 'node:crypto';
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const text = maxLength => ({ type: 'string', maxLength });
const object = (properties, required = []) => ({ type: 'object', properties, required, additionalProperties: false });
const tools = Object.freeze([
  { name: 'app_directory_search', description: 'Discover current opted-in Soty apps. No execution authority is granted.',
    inputSchema: object({ query: text(100), limit: { type: 'integer', minimum: 1, maximum: 60 }, cursor: text(512) }) },
  { name: 'app_directory_get', description: 'Read one currently accessible opted-in app and its pinned capability metadata.',
    inputSchema: object({ appId: text(128) }, ['appId']) },
  { name: 'app_directory_modules', description: 'Discover installed reusable modules and provider contracts. A descriptor is not an execution grant.', inputSchema: object({}) },
  { name: 'app_directory_query', description: 'Call an explicitly host-admitted readonly app binding through existing Soty apps_query authority. Requires its own current grant.',
    inputSchema: object({ appId: text(128), capabilityId: text(160), input: { type: 'object', additionalProperties: true }, idempotencyKey: text(128) }, ['appId', 'capabilityId', 'input', 'idempotencyKey']) },
].map(tool => Object.freeze({ ...tool, annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false } })));
export const APP_AGENT_DIRECTORY_MCP_TOOLS = tools;
/** Merge these tools into the existing authenticated Root MCP router; no second OAuth or MCP server. */
export function createAppAgentDirectoryMcpTools({ directory, moduleCatalog }) {
  return Object.freeze({ tools,
    async call({ name, args = {}, actor }) {
      const allowed = { app_directory_search: ['query', 'limit', 'cursor'], app_directory_get: ['appId'],
        app_directory_modules: [], app_directory_query: ['appId', 'capabilityId', 'input', 'idempotencyKey'] }[name];
      if (!allowed || !args || typeof args !== 'object' || Array.isArray(args) || Object.keys(args).some(key => !allowed.includes(key)))
        throw Object.assign(new Error('directory_invalid_input'), { code: 'directory_invalid_input' });
      if (name === 'app_directory_search') return directory.list({ ...args, actor });
      if (name === 'app_directory_get') return directory.get({ ...args, actor });
      if (name === 'app_directory_modules') return moduleCatalog.list();
      return directory.query({ ...args, actor });
    },
    recheck({ name, args = {}, actor, value }) {
      if (name === 'app_directory_query') { directory.recheckQuery({ ...args, actor, value }); return; }
      const current = name === 'app_directory_search' ? directory.list({ ...args, actor }) : name === 'app_directory_get'
        ? directory.get({ ...args, actor }) : moduleCatalog.list();
      if (digest(current) !== digest(value)) throw Object.assign(new Error('directory_source_changed'), { code: 'directory_source_changed' });
    },
  });
}
