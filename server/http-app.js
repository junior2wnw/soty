import express from "express";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { attachAgentLearning } from "./agent-learning.js";
import { attachAgentRelay } from "./agent-relay.js";
import { attachFrontendCapabilities } from "./frontend-capabilities.js";
import { attachLegal } from "./legal.js";
import { attachPayments } from "./payments.js";
import { attachSpaces } from "./spaces.js";

export function createHttpApp(distDir, { dataDir } = {}) {
  const app = express();
  app.disable("x-powered-by");
  const devConnectSrc = String(process.env.SOTY_DEV_CONNECT_SRC || "")
    .split(/\s+/u)
    .map((item) => item.trim())
    .filter(Boolean)
    .join(" ");
  const miniAppFrameSrc = String(process.env.SOTY_MINI_APP_FRAME_SRC || "")
    .split(/\s+/u)
    .map((item) => item.trim())
    .filter(Boolean)
    .join(" ");
  app.use((req, res, next) => {
    const miniAppAsset = req.path === "/mini-apps/manifest.json" || req.path.startsWith("/mini-apps/");
    const miniAppRunner = req.path === "/mini-app-runner.html" || req.path === "/mini-app-runner.js";
    const scriptSrc = miniAppRunner ? "script-src 'self' 'unsafe-inline'" : "script-src 'self'";
    res.setHeader("Content-Security-Policy", [
      "default-src 'self'",
      "base-uri 'none'",
      "object-src 'none'",
      scriptSrc,
      "style-src 'self' 'unsafe-inline'",
      "img-src 'self' blob: data:",
      "font-src 'self'",
      `connect-src 'self' wss://xn--n1afe0b.online http://127.0.0.1:49424 http://localhost:49424 ws://127.0.0.1:49424 ws://localhost:49424${devConnectSrc ? ` ${devConnectSrc}` : ""}`,
      `frame-src 'self' https: http://127.0.0.1:* http://localhost:*${miniAppFrameSrc ? ` ${miniAppFrameSrc}` : ""}`,
      "manifest-src 'self' blob:",
      "worker-src 'self'",
      `frame-ancestors ${miniAppAsset || miniAppRunner ? "'self'" : "'none'"}`,
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
    res.setHeader("X-Frame-Options", miniAppAsset || miniAppRunner ? "SAMEORIGIN" : "DENY");
    next();
  });
  app.get("/health", (_req, res) => res.json({ ok: true }));
  attachLegal(app);
  attachPayments(app);
  attachSpaces(app, { dataDir });
  attachAgentRelay(app);
  attachAgentLearning(app, { dataDir });
  attachFrontendCapabilities(app, { distDir });
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
    res.status(404).json({ ok: false, error: "agent_asset_not_found" });
  });
  app.use("/mini-apps", (_req, res) => {
    res.status(404).json({ ok: false, error: "mini_app_asset_not_found" });
  });
  app.get("*", async (req, res, next) => {
    res.setHeader("Cache-Control", "no-store");
    try {
      const cardHead = await personalRouteHead(req.path, { dataDir });
      if (!cardHead) {
        res.sendFile(path.join(distDir, "index.html"));
        return;
      }
      const html = await readFile(path.join(distDir, "index.html"), "utf8");
      res.type("html").send(applyPersonalRouteHead(html, cardHead));
    } catch (error) {
      next(error);
    }
  });
  return app;
}

async function personalRouteHead(pathname, { dataDir } = {}) {
  const rawParts = String(pathname || "").split("/").filter(Boolean);
  const parts = rawParts[0] === "pwa" ? rawParts.slice(1) : rawParts;
  const first = parts[0] || "";
  if (!first.startsWith("@")) {
    return null;
  }
  const handle = cleanPersonalRoutePart(safeDecodeURIComponent(first.slice(1)));
  const slug = cleanPersonalRoutePart(safeDecodeURIComponent(parts[1] || ""));
  if (!handle || parts.length > 2) {
    return null;
  }
  const encodedHandle = encodeURIComponent(handle);
  const encodedSlug = encodeURIComponent(slug);
  const iconVersion = await readPersonalPhotoVersion(dataDir, handle);
  const iconVersionSuffix = iconVersion ? `?v=${encodeURIComponent(iconVersion)}` : "";
  const iconHref = slug
    ? `/icon/space/${encodedHandle}/${encodedSlug}.svg`
    : `/icon/space/${encodedHandle}.svg`;
  return {
    title: titleFromRoutePart(slug || handle),
    manifestHref: slug
      ? `/manifest/space/${encodedHandle}/${encodedSlug}.json`
      : `/manifest/space/${encodedHandle}.json`,
    iconHref: `${iconHref}${iconVersionSuffix}`,
    iconType: "image/svg+xml",
    appleIconHref: `/photo/space/${encodedHandle}.jpg${iconVersionSuffix}`,
    appleIconType: "image/jpeg"
  };
}

async function readPersonalPhotoVersion(dataDir, handle) {
  try {
    const root = dataDir || path.join(process.cwd(), "data");
    const record = JSON.parse(await readFile(path.join(root, "profile-photos", `${handle}.json`), "utf8"));
    return cleanPersonalAssetVersion(record?.version || "");
  } catch {
    return "";
  }
}

function cleanPersonalAssetVersion(value) {
  return String(value || "").replace(/[^a-z0-9_-]/giu, "").slice(0, 32);
}

function applyPersonalRouteHead(html, head) {
  const title = escapeHtml(head.title || "soty.online");
  const titleAttr = escapeAttr(head.title || "soty.online");
  const manifestHref = escapeAttr(head.manifestHref);
  const iconHref = escapeAttr(head.iconHref);
  const iconType = escapeAttr(head.iconType || "image/svg+xml");
  const appleIconHref = escapeAttr(head.appleIconHref || head.iconHref);
  const appleIconType = escapeAttr(head.appleIconType || head.iconType || "image/svg+xml");
  const nextHtml = ensureHeadTag(
    html
      .replace(/<title>.*?<\/title>/su, `<title>${title}</title>`)
      .replace(/<meta name="theme-color" content="[^"]*"\s*\/?>/u, '<meta name="theme-color" content="#000000" />')
      .replace(/<link rel="icon" href="[^"]*"[^>]*>/u, `<link rel="icon" href="${iconHref}" type="${iconType}" />`),
    /<link rel="manifest" href="[^"]*"\s*\/?>/u,
    `<link rel="manifest" href="${manifestHref}" />`
  );
  return ensureHeadTag(
    ensureHeadTag(
      ensureHeadTag(
        nextHtml,
        /<meta name="apple-mobile-web-app-title" content="[^"]*"\s*\/?>/u,
        `<meta name="apple-mobile-web-app-title" content="${titleAttr}" />`
      ),
      /<link rel="shortcut icon" href="[^"]*"[^>]*>/u,
      `<link rel="shortcut icon" href="${iconHref}" type="${iconType}" />`
    ),
    /<link rel="apple-touch-icon" href="[^"]*"[^>]*>/u,
    `<link rel="apple-touch-icon" href="${appleIconHref}" type="${appleIconType}" />`
  );
}

function ensureHeadTag(html, pattern, tag) {
  if (pattern.test(html)) {
    return html.replace(pattern, tag);
  }
  return html.replace("</head>", `    ${tag}\n  </head>`);
}

function titleFromRoutePart(value) {
  const text = String(value || "")
    .replace(/[-_.]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, 96);
  return text.replace(/^\p{Ll}/u, (char) => char.toLocaleUpperCase("ru-RU")) || "soty.online";
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;");
}

function escapeAttr(value) {
  return escapeHtml(value).replace(/"/gu, "&quot;");
}

function cleanPersonalRoutePart(value) {
  try {
    return String(value || "")
      .normalize("NFKC")
      .replace(/^@/u, "")
      .replace(/[^\p{L}\p{N}._-]+/gu, "-")
      .replace(/^-+|-+$/gu, "")
      .slice(0, 64)
      .toLowerCase();
  } catch {
    return "";
  }
}

function safeDecodeURIComponent(value) {
  try {
    return decodeURIComponent(String(value || ""));
  } catch {
    return String(value || "");
  }
}
