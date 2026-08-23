import { createServer } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import { createHttpApp } from "./http-app.js";
import { createRoomStore } from "./room-store.js";
import { attachRealtime } from "./realtime.js";
import { createTrafficTunnelProxy } from "./traffic-tunnel-proxy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(rootDir, "dist");
const dataDir = process.env.DATA_DIR || path.join(rootDir, "data");
const port = Number.parseInt(process.env.PORT || "8080", 10);

const trafficTunnel = combineTunnelProxies([
  createTrafficTunnelProxy(),
  createTrafficTunnelProxy({
    target: process.env.SOTY_TRAFFIC_WS_TARGET,
    publicPath: process.env.SOTY_TRAFFIC_WS_PATH || "/api/traffic/ws"
  })
]);
const app = createHttpApp(distDir, { dataDir, trafficTunnel });
const server = createServer(app);
const wss = new WebSocketServer({ noServer: true, maxPayload: 34_000_000 });
const store = createRoomStore(dataDir);

attachRealtime(wss, store);

server.on("upgrade", (request, socket, head) => {
  if (trafficTunnel.handleUpgrade(request, socket, head)) {
    return;
  }
  const url = new URL(request.url || "/", "http://localhost");
  const match = url.pathname.match(/^\/ws\/([A-Za-z0-9_-]{16,96})$/u);
  if (!match?.[1] || !isAllowedOrigin(request)) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit("connection", ws, request, match[1]);
  });
});

server.listen(port, "0.0.0.0", () => {
  console.log(`soty.online listening on ${port}`);
  console.log(`traffic tunnel ${trafficTunnel.enabled ? `enabled on ${trafficTunnel.publicPath}` : "disabled"}`);
});

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
