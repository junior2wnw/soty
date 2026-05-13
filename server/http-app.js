import express from "express";
import path from "node:path";
import { attachAgentLearning } from "./agent-learning.js";
import { attachAgentRelay } from "./agent-relay.js";

export function createHttpApp(distDir, { dataDir } = {}) {
  const app = express();
  app.disable("x-powered-by");
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
      `connect-src 'self' wss://xn--n1afe0b.online http://127.0.0.1:49424 http://localhost:49424 ws://127.0.0.1:49424 ws://localhost:49424${devConnectSrc ? ` ${devConnectSrc}` : ""}`,
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
      "microphone=()",
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
  app.get("/health", (_req, res) => res.json({ ok: true }));
  attachAgentRelay(app);
  attachAgentLearning(app, { dataDir });
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
  app.get("*", (_req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.sendFile(path.join(distDir, "index.html"));
  });
  return app;
}
