import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

// The same actual MCP protocol client is used by human CLI users and agent trials.
export async function connectPlanner(options = {}) {
  const origin = options.url ?? process.env.PLANNER_URL ?? 'http://127.0.0.1:4317';
  const url = new URL('/mcp', origin);
  const token = options.token ?? process.env.PLANNER_MCP_TOKEN;
  const client = new Client(
    { name: options.name ?? 'planner-agent', version: '1.0.0' },
    { capabilities: {} },
  );
  await client.connect(
    new StreamableHTTPClientTransport(url, {
      requestInit: { headers: token ? { Authorization: `Bearer ${token}` } : {} },
    }),
  );
  return client;
}
export function jsonResult(result) {
  const value =
    result.structuredContent ?? JSON.parse(result.content.find((c) => c.type === 'text').text);
  return { isError: result.isError === true, ...value };
}
