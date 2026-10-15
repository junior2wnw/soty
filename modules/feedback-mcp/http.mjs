import express from "express";
import { fileURLToPath } from "node:url";
import {
  parseMcpJson,
  singleHeader,
} from "../../server/capabilities-mcp-ingress.js";
import { createFeedbackMcp } from "./mcp.mjs";
import { callFeedbackTool, FEEDBACK_TOOLS, toolError } from "./tools.mjs";
import { check, exact } from "./service.mjs";

const publicOperations = new Set([
  "ticket_submit",
  "tickets_list",
  "ticket_get",
  "ticket_reply",
  "ticket_accept",
]);
const publicDirectory = fileURLToPath(new URL("./public/", import.meta.url));
const safe = (error) => JSON.parse(toolError(error).content[0].text);
let bodyReaders = 0;
const bodyPeers = new Map();
function status(error) {
  if (
    Number.isInteger(error?.status) &&
    error.status >= 400 &&
    error.status <= 599
  )
    return error.status;
  return /capacity|busy|unavailable/.test(error?.code ?? "") ? 503 : 400;
}
function readJson(req, maximum = 1500000) {
  check(
    singleHeader(req, "content-type")?.split(";")[0].trim().toLowerCase() ===
      "application/json",
    "json_required",
    415,
  );
  check(!singleHeader(req, "content-encoding"), "encoding_unsupported", 415);
  const length = singleHeader(req, "content-length");
  check(
    length === undefined ||
      (/^\d{1,8}$/u.test(length) && Number(length) <= maximum),
    "payload_too_large",
    413,
  );
  const peer = req.ip ?? req.socket.remoteAddress;
  check(
    bodyReaders < 8 && (bodyPeers.get(peer) ?? 0) < 2,
    "request_capacity",
    503,
  );
  bodyReaders++;
  bodyPeers.set(peer, (bodyPeers.get(peer) ?? 0) + 1);
  return new Promise((resolve, reject) => {
    let settled = false;
    let size = 0;
    const chunks = [];
    const done = (error, value) => {
      if (settled) return;
      settled = true;
      bodyReaders--;
      const remaining = (bodyPeers.get(peer) ?? 1) - 1;
      if (remaining) bodyPeers.set(peer, remaining);
      else bodyPeers.delete(peer);
      clearTimeout(timer);
      req.off("data", data);
      req.off("end", end);
      req.off("error", failed);
      req.off("aborted", failed);
      req.once("error", () => {});
      error ? reject(error) : resolve(value);
    };
    const failed = () =>
      done(
        Object.assign(new Error(), { code: "request_aborted", status: 400 }),
      );
    const data = (chunk) => {
      size += chunk.length;
      if (size > maximum) {
        done(
          Object.assign(new Error(), {
            code: "payload_too_large",
            status: 413,
          }),
        );
        req.pause();
      } else chunks.push(chunk);
    };
    const end = () => {
      try {
        const text = new TextDecoder("utf-8", { fatal: true }).decode(
          Buffer.concat(chunks),
        );
        const value = parseMcpJson(
          '{"jsonrpc":"2.0","method":"rest","params":' + text + "}",
        ).params;
        check(value && typeof value === "object" && !Array.isArray(value));
        done(null, value);
      } catch (error) {
        done(error);
      }
    };
    const timer = setTimeout(() => {
      done(
        Object.assign(new Error(), { code: "request_timeout", status: 408 }),
      );
      req.destroy();
    }, 10000);
    timer.unref();
    req.on("data", data);
    req.on("end", end);
    req.on("error", failed);
    req.on("aborted", failed);
  });
}

export function createFeedbackHttp({
  service,
  auth,
  source,
  frameOrigins = [],
}) {
  const app = express(),
    origin = service.origin,
    host = new URL(origin).host,
    peers = new Map();
  let activeRequests = 0;
  app.disable("x-powered-by");
  app.set("trust proxy", "loopback");
  const mcp = createFeedbackMcp({
    service,
    authenticateBearer: (...args) => auth.authenticateBearer(...args),
  });
  app.use((req, res, next) => {
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("Referrer-Policy", "no-referrer");
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("Permissions-Policy", "camera=(), geolocation=()");
    try {
      check(activeRequests < 64, "request_capacity", 503);
      activeRequests++;
      let finished = false;
      const release = () => {
        if (!finished) {
          finished = true;
          activeRequests--;
        }
      };
      res.once("finish", release);
      res.once("close", release);
      res.setTimeout(20000, () => res.destroy());
      check(singleHeader(req, "host")?.toLowerCase() === host, "invalid_host");
      check(
        new URL(origin).protocol !== "https:" || req.secure,
        "https_required",
        403,
      );
      // Never log URLs, cookies, authorization headers, form content or errors.
      const peer = req.ip;
      for (const [key, value] of peers)
        if (Date.now() - value.since > 60000) peers.delete(key);
      if (!peers.has(peer)) {
        check(peers.size < 2048, "rate_limit", 429);
        peers.set(peer, { since: Date.now(), count: 0 });
      }
      check(++peers.get(peer).count <= 180, "rate_limit", 429);
      const publicFrame = /^\/p\/fp_[a-f0-9]{32}$/u.test(req.path);
      const ancestors = publicFrame
        ? "*"
        : ["'self'", ...frameOrigins].join(" ");
      res.setHeader(
        "Content-Security-Policy",
        `default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data: blob:; media-src 'self' data: blob:; connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors ${ancestors}`,
      );
      next();
    } catch (error) {
      res.status(status(error)).json(safe(error));
    }
  });
  const asyncRoute = (fn) => (req, res, next) =>
    Promise.resolve(fn(req, res, next)).catch(next);
  app.use(
    asyncRoute(async (req, res, next) => {
      if (await auth.handleRequest(req, res)) return;
      next();
    }),
  );
  if (source)
    app.use(
      asyncRoute(async (req, res, next) => {
        if (await source.handleRequest(req, res)) return;
        next();
      }),
    );
  app.all(
    "/mcp",
    asyncRoute((req, res) => mcp.handle(req, res)),
  );
  app.get("/health", (_req, res) =>
    res.json({ ok: true, service: "soty-feedback-mcp", version: "1.0.0" }),
  );
  app.get("/api/config", (_req, res) =>
    res.json({
      origin,
      mcpUrl: `${origin}/mcp`,
      loginUrl: "/auth/login",
      guideUrl: "/guide",
    }),
  );
  app.get(
    "/api/me",
    asyncRoute(async (req, res) => {
      const context = auth.browserContext
        ? await auth.browserContext(req)
        : { actor: await auth.browserActor(req) };
      res.json({
        authenticated: Boolean(context?.actor),
        ...(context?.actor
          ? { csrfToken: context.csrfToken, expiresAt: context.expiresAt }
          : {}),
      });
    }),
  );
  app.post(
    "/api/tools/:name",
    asyncRoute(async (req, res) => {
      check(singleHeader(req, "origin") === origin, "origin_denied", 403);
      await auth.assertCsrf(req);
      const actor = await auth.browserActor(req);
      check(actor, "authentication_required", 401);
      const args = await readJson(req);
      const result = callFeedbackTool(service, actor, req.params.name, args);
      res.json(result);
    }),
  );
  app.get("/api/tools", (_req, res) => res.json({ tools: FEEDBACK_TOOLS }));
  app.use(
    "/api/public/:projectId",
    asyncRoute(async (req, res, next) => {
      check(
        /^fp_[a-f0-9]{32}$/u.test(req.params.projectId),
        "project_unavailable",
        404,
      );
      const project = service.publicProject(req.params.projectId),
        presented = singleHeader(req, "origin");
      const allowed = new Set([
        origin,
        ...(project.website ? [new URL(project.website).origin] : []),
      ]);
      check(
        presented === undefined || allowed.has(presented),
        "origin_denied",
        403,
      );
      if (presented) {
        res.setHeader("Access-Control-Allow-Origin", presented);
        res.setHeader("Vary", "Origin");
      }
      if (req.method === "OPTIONS") {
        res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        res.setHeader(
          "Access-Control-Allow-Headers",
          "Content-Type, Authorization",
        );
        res.status(204).end();
        return;
      }
      req.feedbackProject = project;
      next();
    }),
  );
  app.get(
    "/api/public/:projectId",
    asyncRoute(async (req, res) =>
      res.json({
        project: req.feedbackProject,
        limits: {
          bodyChars: 8000,
          maxAttachments: 3,
          totalAttachmentBytes: 1048576,
          maxAudioSeconds: 120,
        },
      }),
    ),
  );
  app.post(
    "/api/public/:projectId/session",
    asyncRoute(async (req, res) => {
      exact(await readJson(req), []);
      const { token, expiresAt } = service.guest(req.params.projectId);
      res.status(201).json({ token, expiresAt });
    }),
  );
  app.post(
    "/api/public/:projectId/tools/:operation",
    asyncRoute(async (req, res) => {
      check(
        publicOperations.has(req.params.operation),
        "unknown_operation",
        404,
      );
      const header = singleHeader(req, "authorization");
      check(
        typeof header === "string" &&
          /^Bearer fbg_[A-Za-z0-9_-]{43}$/u.test(header),
        "authentication_required",
        401,
      );
      const { actor } = service.guest(req.params.projectId, header.slice(7));
      const args = await readJson(req);
      check(!Object.hasOwn(args, "projectId"), "invalid_arguments");
      res.json(
        service.execute(actor, req.params.operation, {
          ...args,
          projectId: req.params.projectId,
        }),
      );
    }),
  );
  app.get("/widget.js", (_req, res) =>
    res
      .type("text/javascript")
      .sendFile("widget.js", { root: publicDirectory }),
  );
  app.get("/assets/source-frame-fetch.js", (_req, res) =>
    res
      .type("text/javascript")
      .sendFile(
        fileURLToPath(
          new URL("../source-app/browser/frame-fetch.mjs", import.meta.url),
        ),
      ),
  );
  app.get("/assets/feedback-media.mjs", (_req, res) =>
    res
      .type("text/javascript")
      .sendFile(
        fileURLToPath(
          new URL("../../src/world/feedback-media.mjs", import.meta.url),
        ),
      ),
  );
  app.use(
    "/assets",
    express.static(publicDirectory, {
      index: false,
      dotfiles: "deny",
      fallthrough: false,
      maxAge: 0,
    }),
  );
  app.get(["/", "/embed", "/guide", "/p/:projectId"], (req, res, next) => {
    if (
      req.path.startsWith("/p/") &&
      !/^fp_[a-f0-9]{32}$/u.test(req.params.projectId)
    ) {
      next();
      return;
    }
    res.sendFile("index.html", { root: publicDirectory });
  });
  app.use((_req, res) =>
    res.status(404).json({ error: { code: "not_found" } }),
  );
  app.use((error, req, res, _next) => {
    if (res.headersSent) {
      res.destroy();
      return;
    }
    if (!req.complete || !req.readableEnded)
      res.setHeader("Connection", "close");
    if (error?.code === "rate_limit") res.setHeader("Retry-After", "60");
    res.status(status(error)).json(safe(error));
  });
  return {
    app,
    async close() {
      await mcp.close();
    },
  };
}
