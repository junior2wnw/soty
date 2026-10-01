import { Server, createMcpHandler, PROTOCOL_VERSION_META_KEY, ProtocolErrorCode,
  deserializeMessage, serializeMessage } from '@modelcontextprotocol/server';
import { createCapabilityOperations } from './capabilities-actions.js';
import { validateDiscoveryOrigin } from './capabilities-discovery.js';
import { createMcpIngress, McpIngressError, singleHeader } from './capabilities-mcp-ingress.js';
import { createMcpTools } from './capabilities-mcp-tools.js';

const LEGACY_REVISIONS = Object.freeze(['2025-11-25', '2025-06-18']);
export const MCP_REVISIONS = Object.freeze(['2026-07-28', ...LEGACY_REVISIONS]);
const requireValue = (value, code = 'invalid_input', protocolCode = -32600) => {
  if (!value) throw new McpIngressError(code, protocolCode);
};
const instructions = 'Create-only access to private Notes. Use a stable idempotencyKey for one intention; after a lost response, read its Invocation or retry identical input and key. A creation receipt is historical and does not reveal current Note text or existence. Catalog metadata is public and does not grant rights.';
// SEP-2243 HeaderMismatch is a serving-ladder constant in SDK2.2.0, not
// included in its public ProtocolErrorCode enum.
const protocolCodes = new Set([...Object.values(ProtocolErrorCode).filter(Number.isInteger), -32020]);
const safeVersion = value => typeof value === 'string'
  && /^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$/u.test(value) ? value : 'unknown';

function route(target) {
  const path = target.split('?')[0]; let decoded;
  try { decoded = decodeURIComponent(path); } catch { decoded = path; }
  if (![path, decoded].some(value => value.toLowerCase() === '/mcp' || value.toLowerCase().startsWith('/mcp/'))) return false;
  requireValue(target === '/mcp'); return true;
}
function revision(headers, body) {
  const header = headers.get('mcp-protocol-version'), declared = body.params?._meta?.[PROTOCOL_VERSION_META_KEY];
  const supported = value => {
    if (MCP_REVISIONS.includes(value)) return;
    const error = new McpIngressError('invalid_input', -32022);
    error.protocolData = { supported: [...MCP_REVISIONS], requested: safeVersion(value) };
    throw error;
  };
  if (header !== null) supported(header);
  if (declared !== undefined) supported(declared);
  if (body.method === 'initialize' && !LEGACY_REVISIONS.includes(body.params?.protocolVersion)) {
    supported(body.params?.protocolVersion);
    throw new McpIngressError('invalid_input', -32020);
  }
  if (header === null) {
    // Only the first legacy initialize may omit its header. Do not inherit
    // the SDK transport's historical 2025-03-26 default on later requests.
    if (declared !== undefined) return; // SDK rejects missing modern header.
    requireValue(body.method === 'initialize' && LEGACY_REVISIONS.includes(body.params?.protocolVersion), 'invalid_input', -32020);
  }
}
function wireFailure(res, failure, id, protocolCode, protocolData) {
  const buffer = Buffer.from(JSON.stringify({ jsonrpc: '2.0', id: id ?? null,
    error: { code: protocolCode ?? -32603, message: failure.code, ...(protocolData ? { data: protocolData } : {}) } }));
  res.statusCode = failure.status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Content-Length', String(buffer.length));
  if (failure.retry) res.setHeader('Retry-After', String(failure.retry));
  res.end(buffer);
}

// The SDK's header mismatch diagnostics include raw request header values.
// Decode only its finite JSON HTTP error through the public SDK codec, retain
// the standard code and replace diagnostics with a closed safe projection.
// Success bytes and all SSE bodies are never parsed or rewritten here.
function checkedSdkFailure(response, buffer, body, headers) {
  if (response.status < 400) return buffer;
  requireValue(response.headers.get('content-type')?.split(';')[0].trim().toLowerCase() === 'application/json', 'internal_error');
  let message;
  try { message = deserializeMessage(buffer.toString('utf8')); } catch { throw new McpIngressError('internal_error'); }
  requireValue(message.jsonrpc === '2.0' && Object.hasOwn(message, 'error') && !Object.hasOwn(message, 'result')
    && message.id === (body.id ?? null) && protocolCodes.has(message.error.code), 'internal_error');
  const code = message.error.code;
  let data;
  if (code === ProtocolErrorCode.UnsupportedProtocolVersion) data = { supported: [...MCP_REVISIONS],
    requested: safeVersion(body.params?._meta?.[PROTOCOL_VERSION_META_KEY] ?? headers.get('mcp-protocol-version')) };
  if (code === ProtocolErrorCode.MissingRequiredClientCapability) {
    const required = message.error.data?.requiredCapabilities, allowed = { roots: [], sampling: ['tools'], elicitation: ['form', 'url'] };
    requireValue(required && typeof required === 'object' && !Array.isArray(required)
      && Object.keys(required).length > 0 && Object.keys(required).length <= 3, 'internal_error');
    const safe = {};
    for (const [key, value] of Object.entries(required)) {
      requireValue(Object.hasOwn(allowed, key) && value && typeof value === 'object' && !Array.isArray(value), 'internal_error');
      safe[key] = {};
      for (const [member, leaf] of Object.entries(value)) {
        requireValue(allowed[key].includes(member) && leaf && typeof leaf === 'object' && !Array.isArray(leaf)
          && Object.keys(leaf).length === 0, 'internal_error');
        safe[key][member] = {};
      }
    }
    data = { requiredCapabilities: safe };
  }
  return Buffer.from(serializeMessage({ jsonrpc: '2.0', id: message.id, error: { code,
    message: code === ProtocolErrorCode.InternalError ? 'internal_error' : 'invalid_input', ...(data ? { data } : {}) } }));
}

/** Maintained SDK codec/dispatch, with a finite buffering and authority gate.
 * The Web Request is an identity key, never a serialized actor container. */
export function attachCapabilitiesMcp(app, { service, origin = '', limits } = {}) {
  requireValue(app && typeof app.use === 'function', 'capability_configuration_invalid');
  requireValue(validateDiscoveryOrigin({ discoveryOrigin: origin, shellOrigins: origin ? [origin] : [] }) === origin,
    'capability_configuration_invalid');
  const operations = createCapabilityOperations({ service, origin }), ingress = createMcpIngress(limits);
  const contexts = new WeakMap(), exchanges = new Set(), host = origin ? new URL(origin) : null;
  let closed = false;
  const sdk = createMcpHandler(({ requestInfo }) => {
    const context = contexts.get(requestInfo);
    requireValue(context, 'internal_error'); context.current();
    const server = new Server({ name: 'soty-capabilities', version: '1.0.0' }, {
      capabilities: { tools: {} }, instructions, supportedProtocolVersions: [...MCP_REVISIONS],
    });
    context.server = server;
    // SDK errors remain controlled protocol results; never log request data.
    server.onerror = () => {};
    const tools = createMcpTools({ service, operations, context });
    server.setRequestHandler('tools/list', request => tools.list(request.params));
    server.setRequestHandler('tools/call', request => server.projectCallToolResult(
      tools.call(request.params), tools.outputSchema(request.params?.name)));
    return server;
  }, { legacy: 'stateless', responseMode: 'json', maxRequestBodySize: ingress.limits.bodyBytes,
    keepAliveMs: 0, maxSubscriptions: 0, onerror: () => {} });

  async function handle(req, res) {
    let lease, context, webRequest, body, completion;
    res.setHeader('Cache-Control', 'no-store'); res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'no-referrer');
    try {
      requireValue(!closed && host, 'native_unavailable');
      requireValue(singleHeader(req, 'host')?.toLowerCase() === host.host);
      const presentedOrigin = singleHeader(req, 'origin');
      requireValue(presentedOrigin === undefined || presentedOrigin === origin, 'access_denied');
      requireValue(host.protocol !== 'https:' || req.secure === true, 'access_denied');
      if (req.method !== 'POST') { res.setHeader('Allow', 'POST'); throw new McpIngressError('method_not_allowed', -32601); }
      const authorization = singleHeader(req, 'authorization');
      const authenticate = () => operations.authenticate({ authorization, audience: `${origin}/mcp` });
      authenticate();
      const headers = new Headers();
      for (const name of ['content-type', 'content-encoding', 'accept', 'mcp-protocol-version', 'mcp-method', 'mcp-name', 'mcp-session-id', 'last-event-id']) {
        const value = singleHeader(req, name);
        if (value !== undefined) headers.set(name, value);
      }
      lease = ingress.enter(req, res);
      const admitted = await lease.read(); body = admitted.body;
      lease.current(); authenticate(); revision(headers, body);
      webRequest = new Request(`${origin}/mcp`, { method: 'POST', headers, body: admitted.text, signal: lease.signal });
      context = { authenticate, current: lease.current, privateRead: null, server: null };
      contexts.set(webRequest, context);
      const response = await sdk.fetch(webRequest, { parsedBody: body });
      lease.current();
      const collected = await lease.collect(response);
      const buffer = checkedSdkFailure(response, collected, body, headers);
      lease.current();
      // Install the slow-writer deadline before the last synchronous authority
      // check. No private byte is written until every SDK await has completed.
      completion = lease.completion();
      const actor = authenticate();
      if (context.privateRead) operations.read({ actor, ...context.privateRead });
      res.statusCode = response.status;
      const type = response.headers.get('content-type');
      if (type) res.setHeader('Content-Type', type);
      res.setHeader('Content-Length', String(buffer.length));
      res.end(buffer);
      await completion;
    } catch (error) {
      if (!res.destroyed && !res.writableEnded && !lease?.signal.aborted) {
        const failure = operations.failure(error);
        if (failure.status === 401 && origin) res.setHeader('WWW-Authenticate',
          `Bearer realm="soty", resource_metadata="${origin}/.well-known/oauth-protected-resource/mcp"`);
        // Stop reading an inadmissible body; the connection cannot be reused
        // with unread bytes. This is distinct from rolling back an effect.
        if (!req.complete || !req.readableEnded) res.setHeader('Connection', 'close');
        completion ??= lease?.completion();
        wireFailure(res, failure, body?.id, error instanceof McpIngressError ? error.protocolCode : null,
          error instanceof McpIngressError ? error.protocolData : undefined);
        if (completion) await completion;
      } else if (!res.destroyed) res.destroy();
    } finally {
      // Socket close alone never releases the admitted slot. Explicitly abort
      // legacy requests too: sdk.close() only covers the modern in-flight set.
      lease?.stop();
      try { await context?.server?.close(); } catch { /* no private diagnostics */ }
      if (webRequest) contexts.delete(webRequest);
      lease?.release();
    }
  }
  app.use((req, res, next) => {
    try { if (!route(req.originalUrl || req.url)) { next(); return; } }
    catch (error) {
      res.setHeader('Cache-Control', 'no-store'); res.setHeader('Referrer-Policy', 'no-referrer');
      if (!req.complete || !req.readableEnded) res.setHeader('Connection', 'close');
      res.setHeader('X-Content-Type-Options', 'nosniff'); wireFailure(res, operations.failure(error), null, -32600); return;
    }
    const task = handle(req, res); exchanges.add(task);
    void task.then(() => exchanges.delete(task), () => { exchanges.delete(task); if (!res.destroyed) res.destroy(); });
  });
  return Object.freeze({ async close() {
    closed = true; ingress.close(); await sdk.close(); await Promise.allSettled([...exchanges]);
  } });
}
