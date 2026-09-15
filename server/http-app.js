import express from "express";
import path from "node:path";
import { attachAccountTransfer } from "./account-transfer.js";
import { attachConnectorApi } from "./connector-api.js";
import { attachCanonicalIdentityApi } from "./identity-wire-v1-api.js";
import { attachSotyIdentityAdapterApi } from "./soty-identity-adapter-api.js";
import { attachTrafficControl } from "./traffic-control.js";

export function createHttpApp(distDir, { dataDir, trafficTunnel } = {}) {
  const app = express();
  app.disable("x-powered-by");
  app.use((req, res, next) => {
    if (!trafficTunnel?.handleRequest(req, res)) {
      next();
    }
  });
  const devConnectSrc = String(process.env.SOTY_DEV_CONNECT_SRC || "")
    .split(/\s+/u)
    .map((item) => item.trim())
    .filter(Boolean)
    .join(" ");
  app.use((_req, res, next) => {
    res.setHeader("Content-Security-Policy", [
      "default-src 'self'",
      "base-uri 'none'",
      "object-src 'none'",
      "script-src 'self'",
      "style-src 'self' 'unsafe-inline'",
      "img-src 'self' blob: data:",
      "font-src 'self'",
      `connect-src 'self' wss://xn--n1afe0b.online http://127.0.0.1:49424 http://localhost:49424${devConnectSrc ? ` ${devConnectSrc}` : ""}`,
      "manifest-src 'self'",
      "worker-src 'self'",
      "frame-ancestors 'none'",
      "form-action 'self'"
    ].join("; "));
    res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
    res.setHeader("Cross-Origin-Resource-Policy", "same-origin");
    res.setHeader("Origin-Agent-Cluster", "?1");
    res.setHeader("Permissions-Policy", [
      "camera=(self)",
      "microphone=(self)",
      "geolocation=()",
      "payment=()",
      "usb=()",
      "serial=()",
      "hid=()",
      "bluetooth=()"
    ].join(", "));
    res.setHeader("Referrer-Policy", "no-referrer");
    res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("X-Frame-Options", "DENY");
    next();
  });
  attachAccountTransfer(app, { dataDir });
  attachCanonicalIdentityApi(app, { dataDir });
  attachSotyIdentityAdapterApi(app, { dataDir });
  const connectors = attachConnectorApi(app, { dataDir });
  app.get("/health", (_req, res) => res.json({
    ok: true,
    agentModelProxy: connectors.modelProxy,
    applicationModelProxy: connectors.applicationModelProxy
  }));
  app.get("/ready", (_req, res) => {
    const ready = connectors.modelProxy.ready === true;
    res.status(ready ? 200 : 503).json({
      ok: ready,
      agentModelProxy: connectors.modelProxy,
      applicationModelProxy: connectors.applicationModelProxy
    });
  });
  attachTrafficControl(app, { dataDir, isRelayConnected: (linkId) => connectors.store.isConnected(linkId) });
  app.use(express.static(distDir, {
    etag: true,
    index: false,
    setHeaders: (res, filePath) => {
      if (filePath.endsWith("index.html") || filePath.endsWith("sw.js") || filePath.endsWith("manifest.webmanifest")) {
        res.setHeader("Cache-Control", "no-store");
        return;
      }
      if (filePath.includes(`${path.sep}assets${path.sep}`)) {
        res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
      }
    }
  }));
  app.use("/agent", (_req, res) => {
    res.status(404).json({ ok: false, error: "connector_asset_not_found" });
  });
  app.get("*", (_req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.sendFile(path.join(distDir, "index.html"));
  });
  return app;
}
