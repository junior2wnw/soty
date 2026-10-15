import { createServer } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { types } from 'node:util';
import { WebSocketServer } from 'ws';
import { createHttpApp, waitForRejectedHttpStart } from './http-app.js';
import { createRoomStore } from './room-store.js';
import { attachRealtime } from './realtime.js';
import { createTrafficTunnelProxy } from './traffic-tunnel-proxy.js';
import { hasSingleHostHeader } from './app-domain-policy.mjs';
import { fenceClosingUpgrade } from './http-closing-socket.mjs';
import { loadCapabilityConfiguration } from './capabilities-configuration.js';
import { loadUniversalConfiguration } from './universal-configuration.js';
import { loadPersonalAgentConfiguration } from './personal-agent-configuration.js';
import { loadNativeVoiceConfiguration } from './native-voice-configuration.js';
import { loadInstalledApplicationsEntry } from './installed-applications-entry.js';
import { startUniversalOperator } from './universal-operator.js';
import { connectAllowedOrigins } from './connect-module.js';
import { defaultGonkaProxyModel } from './gonka-proxy.js';

const invalid = () => { throw Object.assign(new Error('root_startup_configuration_invalid'), { code: 'root_startup_configuration_invalid' }); };
function captureEnvironment(env) {
  if (!env || typeof env !== 'object' || types.isProxy(env) || Array.isArray(env)) invalid();
  const fields = Object.getOwnPropertyDescriptors(env), out = {};
  for (const name of Reflect.ownKeys(fields)) {
    const field = fields[name];
    if (typeof name !== 'string' || !Object.hasOwn(field, 'value') || typeof field.value !== 'string') invalid();
    out[name] = field.value;
  }
  return Object.freeze(out);
}

/** Every env-backed provider field is explicit, including empty credentials.
 * Passing undefined would reactivate ambient Native defaults in the proxy. */
export function rootGonkaOptions(env) {
  return Object.freeze({ baseUrl: env.SOTY_GONKA_BASE_URL || 'https://gate.joingonka.ai/v1', apiKey: env.SOTY_GONKA_API_KEY || '',
    model: defaultGonkaProxyModel, upstreamModel: env.SOTY_GONKA_UPSTREAM_MODEL || defaultGonkaProxyModel,
    timeoutMs: env.SOTY_GONKA_REQUEST_TIMEOUT_MS ?? 55000, fallbackBaseUrl: env.SOTY_GONKA_FALLBACK_BASE_URL || '',
    fallbackApiKey: env.SOTY_GONKA_FALLBACK_API_KEY || '', fallbackModels: env.SOTY_GONKA_FALLBACK_MODELS ?? '*',
    firstTokenTimeoutMs: env.SOTY_GONKA_FIRST_TOKEN_TIMEOUT_MS ?? (env.SOTY_GONKA_PROVIDER_STRATEGY === 'fallback' ? 20000 : 45000),
    idleTimeoutMs: env.SOTY_GONKA_IDLE_TIMEOUT_MS ?? 30000, streamTimeoutMs: env.SOTY_GONKA_STREAM_TIMEOUT_MS ?? 1800000,
    providerConcurrency: env.SOTY_GONKA_PROVIDER_CONCURRENCY ?? 8, maximumQueued: env.SOTY_GONKA_MAX_QUEUED ?? 64,
    queueWaitMs: env.SOTY_GONKA_QUEUE_WAIT_MS ?? 5000, internalStream: env.SOTY_GONKA_INTERNAL_STREAM !== '0',
    providerStrategy: env.SOTY_GONKA_PROVIDER_STRATEGY || 'race', emptyToolFallback: env.SOTY_GONKA_EMPTY_TOOL_FALLBACK !== '0',
    onEvent: null, fetchImpl: fetch, applicationTokens: env.SOTY_GONKA_APPLICATION_TOKENS || '',
    applicationTokensFile: env.SOTY_GONKA_APPLICATION_TOKENS_FILE || '' });
}

/** Import is inert. The caller supplies Root settings explicitly and owns when
 * to start listening; this instance never owns process signals, IPC or exit. */
export async function createRootStartup({ env: rawEnv, rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..'), httpOptions = {} } = {}) {
  const env = captureEnvironment(rawEnv);
  const distDir = env.SOTY_DIST_DIR ? path.resolve(env.SOTY_DIST_DIR) : path.join(rootDir, 'dist');
  const dataDir = env.DATA_DIR || path.join(rootDir, 'data'), port = Number.parseInt(env.PORT || '8080', 10), host = env.HOST || '0.0.0.0';
  const operatorFlag = env.SOTY_UNIVERSAL_OPERATOR_ENABLED ?? '';
  if (!['', '0', '1'].includes(operatorFlag)) invalid();
  const trafficTunnel = combineTunnelProxies([
    createTrafficTunnelProxy({ target: env.SOTY_TRAFFIC_TUNNEL_TARGET || '', publicPath: env.SOTY_TRAFFIC_TUNNEL_PATH || '/api/traffic/tunnel' }),
    createTrafficTunnelProxy({ target: env.SOTY_TRAFFIC_WS_TARGET || '', publicPath: env.SOTY_TRAFFIC_WS_PATH || '/api/traffic/ws' }),
  ]);
  let app, server, wss, humanWss, store, installedApplications, operator = { async close() {} }, startPromise, cancelStart, cleanup, closing = false;
  function close() {
    if (cleanup) return cleanup;
    closing = true; let resolve, reject;
    cleanup = new Promise((done, fail) => { resolve = done; reject = fail; });
    cancelStart?.();
    const run = async () => {
      let failed = false;
      const attempt = async action => { try { await action(); } catch { failed = true; } };
      for (const sockets of [wss?.clients, humanWss?.clients]) for (const client of sockets ?? []) await attempt(() => client.close(1001, 'server_shutdown'));
      await attempt(() => { server?.close(); server?.closeAllConnections(); });
      await attempt(() => wss && new Promise((done, fail) => wss.close(error => error ? fail(error) : done())));
      await attempt(() => store?.close());
      await attempt(() => humanWss && new Promise((done, fail) => humanWss.close(error => error ? fail(error) : done())));
      await attempt(() => operator.close());
      await attempt(() => app?.locals.closeServices());
      await attempt(() => installedApplications?.close());
      if (failed) throw Object.assign(new Error('root_startup_cleanup_failed'), { code: 'root_startup_cleanup_failed' });
    };
    void run().then(resolve, reject); return cleanup;
  }
  try {
    const capabilityConfiguration = loadCapabilityConfiguration(env), universalConfiguration = loadUniversalConfiguration(env);
    const personalAgentConfiguration = loadPersonalAgentConfiguration(env), nativeVoiceConfiguration = loadNativeVoiceConfiguration(env.SOTY_NATIVE_VOICE_CONFIG || '');
    installedApplications = await loadInstalledApplicationsEntry(env);
    const personalAgent = { ...personalAgentConfiguration.personalAgent, browserPilotEnabled: true,
      ...(installedApplications.sourceCredentialFor ? { sourceReferences: installedApplications.sourceReferences,
        sourceCredentialFor: installedApplications.sourceCredentialFor } : {}) };
    try { app = createHttpApp(distDir, { env, dataDir, trafficTunnel, ...capabilityConfiguration, ...universalConfiguration,
      ...personalAgentConfiguration, ...installedApplications.httpOptions, personalAgent, ...nativeVoiceConfiguration,
      ...httpOptions, env, gonka: rootGonkaOptions(env) }); }
    catch (error) { await waitForRejectedHttpStart(error); throw error; }
    server = createServer(app); server.keepAliveTimeout = 65000; server.headersTimeout = 70000;
    wss = new WebSocketServer({ noServer: true, maxPayload: 34000000 });
    humanWss = new WebSocketServer({ noServer: true, maxPayload: 64000 });
    const humanOrigins = new Set(connectAllowedOrigins(httpOptions.connectOrigins, env));
    store = createRoomStore(dataDir);
    if (operatorFlag === '1') operator = await startUniversalOperator({ capture: app.locals.captureUniversalPreparedness });
    attachRealtime(wss, store); app.locals.humanCalls.attachSignaling(humanWss);
    server.on('upgrade', (request, socket, head) => {
      if (closing) { socket.destroy(); return; }
      if (fenceClosingUpgrade(request, socket)) return;
      if (!hasSingleHostHeader(request)) { socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); return; }
      let url; try { url = new URL(request.url || '/', 'http://localhost'); }
      catch { socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); return; }
      if (app.locals.appsService.handleUpgrade(request, socket, head)) return;
      if (trafficTunnel.handleUpgrade(request, socket, head)) return;
      if (url.pathname === '/ws/human-call' || url.pathname.startsWith('/ws/human-call/')) {
        const call = url.pathname.match(/^\/ws\/human-call\/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$/u);
        if (!call || url.search || !humanOrigins.has(request.headers.origin)) { socket.destroy(); return; }
        humanWss.handleUpgrade(request, socket, head, ws => humanWss.emit('connection', ws, request, call[1])); return;
      }
      const match = url.pathname.match(/^\/ws\/([A-Za-z0-9_-]{16,96})$/u);
      if (!match?.[1] || !isAllowedOrigin(request)) { socket.destroy(); return; }
      wss.handleUpgrade(request, socket, head, ws => wss.emit('connection', ws, request, match[1]));
    });
    function start() {
      if (closing) return Promise.reject(Object.assign(new Error('root_startup_closed'), { code: 'root_startup_closed' }));
      if (startPromise) return startPromise;
      let resolve, reject, settled = false;
      startPromise = new Promise((done, fail) => { resolve = done; reject = fail; });
      // Observe cancellation internally without changing the caller's promise.
      void startPromise.catch(() => {});
      const cancel = () => {
        if (settled) return;
        settled = true; cancelStart = undefined;
        server.removeListener('listening', ready);
        // Keep the once error handler: an already-queued listen error must not
        // become an unhandled EventEmitter error after cancellation.
        reject(Object.assign(new Error('root_startup_closed'), { code: 'root_startup_closed' }));
      };
      const ready = () => {
        if (closing) { cancel(); return; }
        if (settled) return;
        settled = true; cancelStart = undefined;
        server.removeListener('error', error); resolve();
      };
      const error = reason => {
        server.removeListener('listening', ready);
        if (settled) return;
        settled = true; cancelStart = undefined; reject(reason);
      };
      cancelStart = cancel;
      server.once('listening', ready); server.once('error', error);
      try { server.listen(port, host); } catch (reason) { error(reason); }
      return startPromise;
    }
    return Object.freeze({ app, server, start, close, trafficTunnel });
  } catch (error) { await close().catch(() => {}); throw error; }
}

function isAllowedOrigin(request) {
  const origin = request.headers.origin; if (!origin) return true;
  const host = request.headers.host; if (!host) return false;
  try { return new URL(origin).host === host; } catch { return false; }
}
function combineTunnelProxies(proxies) {
  const enabled = proxies.filter(proxy => proxy.enabled);
  return { enabled: enabled.length > 0, publicPath: enabled.map(proxy => proxy.publicPath).join(', '),
    handleRequest: (request, response) => enabled.some(proxy => proxy.handleRequest(request, response)),
    handleUpgrade: (request, socket, head) => enabled.some(proxy => proxy.handleUpgrade(request, socket, head)) };
}
