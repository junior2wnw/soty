import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import type { Tool } from '@modelcontextprotocol/sdk/types.js';
import { createPlannerMcp } from './mcp.ts';
import { ApiError } from './validation.ts';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const configuredPort = Number(process.env.PLANNER_PORT ?? 4317);
if (!Number.isInteger(configuredPort) || configuredPort < 1 || configuredPort > 65535)
  throw new Error('PLANNER_PORT must be between 1 and 65535.');
const url = new URL(process.env.PLANNER_URL ?? `http://127.0.0.1:${configuredPort}`);
if (
  !['http:', 'https:'].includes(url.protocol) ||
  url.username ||
  url.password ||
  url.search ||
  url.hash ||
  url.pathname !== '/'
) {
  throw new Error('PLANNER_URL must be an HTTP(S) origin, without credentials, path or query.');
}
const loopback = ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname);
const token = process.env.PLANNER_MCP_TOKEN;
if (!loopback && url.protocol !== 'https:') throw new Error('Remote MCP requires HTTPS.');
let ownedApp: Awaited<ReturnType<(typeof import('./main.ts'))['createPlannerServer']>> | undefined;
async function healthy() {
  try {
    const response = await fetch(new URL('/health', url), {
      signal: AbortSignal.timeout(1200),
      redirect: 'error',
    });
    const body = await response.json();
    return response.ok && body.ok === true && body.storage === 'sqlite';
  } catch {
    return false;
  }
}
if (
  !(await healthy()) &&
  !process.env.PLANNER_URL &&
  process.env.PLANNER_MCP_AUTOSTART !== 'false'
) {
  const { createPlannerServer } = await import('./main.ts');
  ownedApp = await createPlannerServer({
    development: false,
    dbPath: process.env.PLANNER_DB_PATH
      ? resolve(process.env.PLANNER_DB_PATH)
      : resolve(root, 'data', 'planner.sqlite'),
    port: configuredPort,
  });
  try {
    await ownedApp.listen();
  } catch (error) {
    await ownedApp.close();
    ownedApp = undefined;
    if ((error as NodeJS.ErrnoException).code !== 'EADDRINUSE') throw error;
  }
}
async function request(path: string, input?: unknown) {
  let response: Response;
  try {
    response = await fetch(new URL(path, url), {
      method: input === undefined ? 'GET' : 'POST',
      redirect: 'error',
      signal: AbortSignal.timeout(20000),
      headers: {
        Origin: url.origin,
        ...(input === undefined ? {} : { 'Content-Type': 'application/json' }),
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: input === undefined ? undefined : JSON.stringify(input),
    });
  } catch {
    throw new ApiError(
      503,
      'Планировщик недоступен. Запустите start.cmd или проверьте PLANNER_URL.',
      'planner_unavailable',
    );
  }
  const result = await response.json().catch(() => ({}));
  if (!response.ok)
    throw new ApiError(
      response.status,
      result.error ?? 'Ошибка подключения MCP',
      result.code ?? 'connection_error',
      result.details,
    );
  return result;
}
const discovery = await request('/api/agent/tools');
const mcp = createPlannerMcp({
  tools: discovery.tools as Tool[],
  call: async (name, args) => request('/api/agent/call', { name, arguments: args }),
});
await mcp.connect(new StdioServerTransport());
let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  await mcp.close();
  if (ownedApp) await ownedApp.close();
}
process.stdin.once('end', () => {
  void close();
});
process.on('SIGINT', () => {
  void close();
});
process.on('SIGTERM', () => {
  void close();
});
