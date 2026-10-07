import { createServer } from "node:http";
import { fenceClosingUpgrade } from './http-closing-socket.mjs';
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import { createHttpApp, waitForRejectedHttpStart } from "./http-app.js";
import { createRoomStore } from "./room-store.js";
import { attachRealtime } from "./realtime.js";
import { createTrafficTunnelProxy } from "./traffic-tunnel-proxy.js";
import { hasSingleHostHeader } from './app-domain-policy.mjs';
import { loadCapabilityConfiguration } from './capabilities-configuration.js';
import { loadUniversalConfiguration } from './universal-configuration.js';
import { startUniversalOperator } from './universal-operator.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const distDir = process.env.SOTY_DIST_DIR ? path.resolve(process.env.SOTY_DIST_DIR) : path.join(rootDir, "dist");
const dataDir = process.env.DATA_DIR || path.join(rootDir, "data");
const port = Number.parseInt(process.env.PORT || "8080", 10);
const host = process.env.HOST || "0.0.0.0";
const capabilityConfiguration = loadCapabilityConfiguration();
const universalConfiguration = loadUniversalConfiguration();
const operatorFlag = process.env.SOTY_UNIVERSAL_OPERATOR_ENABLED ?? '';
if (!['', '0', '1'].includes(operatorFlag)) throw Object.assign(new Error('universal_operator_configuration_invalid'), { code: 'universal_operator_configuration_invalid' });

const trafficTunnel = combineTunnelProxies([
  createTrafficTunnelProxy(),
  createTrafficTunnelProxy({
    target: process.env.SOTY_TRAFFIC_WS_TARGET,
    publicPath: process.env.SOTY_TRAFFIC_WS_PATH || "/api/traffic/ws"
  })
]);
let app;
try { app = createHttpApp(distDir, { dataDir, trafficTunnel, ...capabilityConfiguration, ...universalConfiguration }); }
catch (error) { await waitForRejectedHttpStart(error); throw error; }
const server = createServer(app);
// The front proxy keeps idle connections for 30 seconds. Closing them first
// can race a reused POST connection and produce an avoidable reset.
server.keepAliveTimeout = 65_000;
server.headersTimeout = 70_000;
const wss = new WebSocketServer({ noServer: true, maxPayload: 34_000_000 });
const store = createRoomStore(dataDir);
let operator = { async close() {} };
if (operatorFlag === '1') {
  try { operator = await startUniversalOperator({ capture: app.locals.captureUniversalPreparedness }); }
  catch (error) { store.close(); await app.locals.closeServices(); throw error; }
}

attachRealtime(wss, store);

server.on("upgrade", (request, socket, head) => {
  if (fenceClosingUpgrade(request, socket)) return;
  if (!hasSingleHostHeader(request)) {
    socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
    return;
  }
  let url;
  try { url = new URL(request.url || "/", "http://localhost"); }
  catch {
    socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
    return;
  }
  if (app.locals.appsService.handleUpgrade(request, socket, head)) return;
  if (trafficTunnel.handleUpgrade(request, socket, head)) {
    return;
  }
  const match = url.pathname.match(/^\/ws\/([A-Za-z0-9_-]{16,96})$/u);
  if (!match?.[1] || !isAllowedOrigin(request)) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit("connection", ws, request, match[1]);
  });
});

server.listen(port, host, () => {
  console.log(`soty.online listening on ${port}`);
  console.log(`traffic tunnel ${trafficTunnel.enabled ? `enabled on ${trafficTunnel.publicPath}` : "disabled"}`);
  if (process.send) process.send({ type: 'soty:ready', port: server.address().port });
});

let closing = false;
function shutdown() {
  if (closing) return;
  closing = true;
  // A disconnect listener refs fork's IPC channel on POSIX. Close it during
  // signal shutdown too, otherwise a supervised child outlives its services.
  if (process.connected) process.disconnect();
  for (const client of wss.clients) client.close(1001, 'server_shutdown');
  server.close(); server.closeAllConnections();
  const closeRooms = new Promise((resolveClose, rejectClose) => wss.close(() => {
    try { store.close(); resolveClose(); } catch (error) { rejectClose(error); }
  }));
  const closeServices = async () => { await operator.close(); await app.locals.closeServices(); };
  void Promise.all([closeRooms, closeServices()]).catch(() => { process.exitCode = 1; });
}
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
// Only supervised local development has an IPC parent. Losing it must not
// leave an API process behind; a normal production start has no IPC channel.
if (process.send) process.once('disconnect', shutdown);

function isAllowedOrigin(request) {
  const origin = request.headers.origin;
  if (!origin) {
    return true;
  }
  const host = request.headers.host;
  if (!host) {
    return false;
  }
  try {
    return new URL(origin).host === host;
  } catch {
    return false;
  }
}

function combineTunnelProxies(proxies) {
  const enabled = proxies.filter((proxy) => proxy.enabled);
  return {
    enabled: enabled.length > 0,
    publicPath: enabled.map((proxy) => proxy.publicPath).join(", "),
    handleRequest: (request, response) => enabled.some((proxy) => proxy.handleRequest(request, response)),
    handleUpgrade: (request, socket, head) => enabled.some((proxy) => proxy.handleUpgrade(request, socket, head))
  };
}
