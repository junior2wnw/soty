import { createServer } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import { createHttpApp } from "./http-app.js";
import { createRoomStore } from "./room-store.js";
import { attachRealtime } from "./realtime.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(rootDir, "dist");
const dataDir = process.env.DATA_DIR || path.join(rootDir, "data");
const port = Number.parseInt(process.env.PORT || "8080", 10);

const app = createHttpApp(distDir, { dataDir });
const server = createServer(app);
const wss = new WebSocketServer({ noServer: true, maxPayload: 34_000_000 });
const store = createRoomStore(dataDir);

attachRealtime(wss, store);

server.on("upgrade", (request, socket, head) => {
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
