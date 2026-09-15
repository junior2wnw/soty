import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { WebSocket, WebSocketServer } from "ws";
import { createTrafficTunnelProxy } from "../server/traffic-tunnel-proxy.js";

const upstreamServer = createServer();
const upstreamWss = new WebSocketServer({ noServer: true });
upstreamServer.on("upgrade", (request, socket, head) => {
  assert.equal(request.url, "/inside");
  upstreamWss.handleUpgrade(request, socket, head, (ws) => upstreamWss.emit("connection", ws));
});
upstreamWss.on("connection", (ws) => ws.on("message", (message) => ws.send(message)));
upstreamServer.on("request", (request, response) => {
  const chunks = [];
  request.on("data", (chunk) => chunks.push(chunk));
  request.on("end", () => {
    response.writeHead(200, { "content-type": "application/octet-stream", "x-upstream-path": request.url });
    response.end(Buffer.concat(chunks));
  });
});
upstreamServer.listen(0, "127.0.0.1");
await once(upstreamServer, "listening");
const upstreamPort = upstreamServer.address().port;

const proxy = createTrafficTunnelProxy({
  target: `ws://127.0.0.1:${upstreamPort}/inside`,
  publicPath: "/api/traffic/tunnel"
});
const publicServer = createServer((_request, response) => response.writeHead(404).end());
publicServer.removeAllListeners("request");
publicServer.on("request", (request, response) => {
  if (!proxy.handleRequest(request, response)) {
    response.writeHead(404).end();
  }
});
publicServer.on("upgrade", (request, socket, head) => {
  if (!proxy.handleUpgrade(request, socket, head)) {
    socket.destroy();
  }
});
publicServer.listen(0, "127.0.0.1");
await once(publicServer, "listening");
const publicPort = publicServer.address().port;

const ws = new WebSocket(`ws://127.0.0.1:${publicPort}/api/traffic/tunnel`);
await once(ws, "open");
ws.send("soty-tunnel-proxy-ok");
const [reply] = await once(ws, "message");
assert.equal(reply.toString(), "soty-tunnel-proxy-ok");
ws.close();
await once(ws, "close");

const httpResponse = await fetch(`http://127.0.0.1:${publicPort}/api/traffic/tunnel/session/7?part=2`, {
  method: "POST",
  body: "xhttp-packet"
});
assert.equal(httpResponse.status, 200);
assert.equal(httpResponse.headers.get("x-upstream-path"), "/inside/session/7?part=2");
assert.equal(await httpResponse.text(), "xhttp-packet");

assert.equal(createTrafficTunnelProxy().enabled, false);
assert.throws(() => createTrafficTunnelProxy({ target: "file:///tmp/socket" }), /http\(s\)/u);
assert.throws(() => createTrafficTunnelProxy({ target: "ws://127.0.0.1/a", publicPath: "relative" }), /absolute path/u);

upstreamWss.close();
publicServer.close();
upstreamServer.close();
await Promise.all([once(publicServer, "close"), once(upstreamServer, "close")]);
console.log("traffic tunnel proxy self-test passed");
