import net from "node:net";
import tls from "node:tls";
import http from "node:http";
import https from "node:https";

const DEFAULT_PUBLIC_PATH = "/api/traffic/tunnel";
const CONNECT_TIMEOUT_MS = 10_000;
const HOP_BY_HOP_HEADERS = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade",
  "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto"
]);

export function createTrafficTunnelProxy({
  target = process.env.SOTY_TRAFFIC_TUNNEL_TARGET,
  publicPath = process.env.SOTY_TRAFFIC_TUNNEL_PATH || DEFAULT_PUBLIC_PATH
} = {}) {
  const targetUrl = parseTarget(target);
  const normalizedPublicPath = normalizePath(publicPath);

  return {
    enabled: Boolean(targetUrl),
    publicPath: normalizedPublicPath,
    handleRequest(request, response) {
      const requestUrl = new URL(request.url || "/", "http://localhost");
      if (!targetUrl || !matchesPath(requestUrl.pathname, normalizedPublicPath)) {
        return false;
      }

      proxyHttpRequest({ request, response, requestUrl, targetUrl, publicPath: normalizedPublicPath });
      return true;
    },
    handleUpgrade(request, clientSocket, head = Buffer.alloc(0)) {
      const requestUrl = new URL(request.url || "/", "http://localhost");
      if (!targetUrl || requestUrl.pathname !== normalizedPublicPath) {
        return false;
      }

      proxyUpgrade({ request, clientSocket, head, targetUrl });
      return true;
    }
  };
}

function proxyHttpRequest({ request, response, requestUrl, targetUrl, publicPath }) {
  const secure = targetUrl.protocol === "https:" || targetUrl.protocol === "wss:";
  const transport = secure ? https : http;
  const requestBase = new URL(targetUrl);
  requestBase.protocol = secure ? "https:" : "http:";
  const suffix = requestUrl.pathname.slice(publicPath.length);
  const upstreamPath = `${targetUrl.pathname.replace(/\/+$/u, "")}${suffix}${requestUrl.search}`;
  const headers = sanitizeHeaders(request.headers, targetUrl.host);
  const upstream = transport.request(requestBase, {
    method: request.method,
    path: upstreamPath,
    headers
  });

  const connectTimer = setTimeout(() => upstream.destroy(new Error("traffic tunnel connect timeout")), CONNECT_TIMEOUT_MS);
  upstream.once("socket", (socket) => {
    if (!socket.connecting) {
      clearTimeout(connectTimer);
      return;
    }
    socket.once(secure ? "secureConnect" : "connect", () => clearTimeout(connectTimer));
  });
  upstream.once("response", (upstreamResponse) => {
    clearTimeout(connectTimer);
    const responseHeaders = sanitizeHeaders(upstreamResponse.headers);
    response.writeHead(upstreamResponse.statusCode || 502, responseHeaders);
    upstreamResponse.pipe(response);
  });
  upstream.once("error", () => {
    clearTimeout(connectTimer);
    if (!response.headersSent) {
      response.writeHead(502, { "content-type": "application/json", "cache-control": "no-store" });
      response.end('{"ok":false,"error":"traffic_tunnel_unavailable"}');
    } else {
      response.destroy();
    }
  });
  request.once("aborted", () => upstream.destroy());
  response.once("close", () => {
    if (!response.writableEnded) {
      upstream.destroy();
    }
  });
  request.pipe(upstream);
}

function proxyUpgrade({ request, clientSocket, head, targetUrl }) {
  const secure = targetUrl.protocol === "wss:" || targetUrl.protocol === "https:";
  const port = Number(targetUrl.port || (secure ? 443 : 80));
  const connect = secure ? tls.connect : net.connect;
  const upstream = connect({
    host: targetUrl.hostname,
    port,
    ...(secure ? { servername: targetUrl.hostname } : {})
  });

  let connected = false;
  const fail = () => {
    upstream.destroy();
    if (!clientSocket.destroyed) {
      if (!connected) {
        clientSocket.end("HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n");
      } else {
        clientSocket.destroy();
      }
    }
  };

  upstream.setTimeout(CONNECT_TIMEOUT_MS, fail);
  upstream.once("error", fail);
  clientSocket.once("error", () => upstream.destroy());
  clientSocket.once("close", () => upstream.destroy());

  upstream.once("connect", () => {
    connected = true;
    upstream.setTimeout(0);
    upstream.setNoDelay(true);
    clientSocket.setNoDelay(true);

    upstream.write(serializeUpgradeRequest(request, targetUrl));
    if (head.length > 0) {
      upstream.write(head);
    }
    clientSocket.pipe(upstream).pipe(clientSocket);
  });
}

function serializeUpgradeRequest(request, targetUrl) {
  const query = targetUrl.search || "";
  const lines = [`GET ${targetUrl.pathname}${query} HTTP/1.1`];
  const ignored = new Set(["host", "connection", "upgrade", "content-length"]);

  for (const [name, value] of Object.entries(request.headers)) {
    if (ignored.has(name.toLowerCase()) || value == null) {
      continue;
    }
    const values = Array.isArray(value) ? value : [value];
    for (const item of values) {
      lines.push(`${name}: ${String(item).replace(/[\r\n]/gu, "")}`);
    }
  }

  lines.push(`host: ${targetUrl.host}`, "connection: Upgrade", "upgrade: websocket", "", "");
  return lines.join("\r\n");
}

function parseTarget(value) {
  const source = String(value || "").trim();
  if (!source) {
    return null;
  }

  const target = new URL(source);
  if (!new Set(["http:", "https:", "ws:", "wss:"]).has(target.protocol)) {
    throw new Error("SOTY_TRAFFIC_TUNNEL_TARGET must use http(s):// or ws(s)://");
  }
  if (target.username || target.password || target.hash) {
    throw new Error("SOTY_TRAFFIC_TUNNEL_TARGET must not contain credentials or a fragment");
  }
  return target;
}

function matchesPath(pathname, publicPath) {
  return pathname === publicPath || pathname.startsWith(`${publicPath}/`);
}

function sanitizeHeaders(source, host) {
  const result = {};
  for (const [name, value] of Object.entries(source || {})) {
    if (HOP_BY_HOP_HEADERS.has(name.toLowerCase()) || value == null) {
      continue;
    }
    result[name] = value;
  }
  if (host) {
    result.host = host;
  }
  return result;
}

function normalizePath(value) {
  const source = String(value || "").trim();
  if (!/^\/[A-Za-z0-9/_-]{1,160}$/u.test(source) || source.includes("//")) {
    throw new Error("SOTY_TRAFFIC_TUNNEL_PATH must be a simple absolute path");
  }
  return source.length > 1 ? source.replace(/\/+$/u, "") : source;
}
