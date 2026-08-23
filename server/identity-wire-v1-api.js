import express from "express";
import path from "node:path";
import {
  CanonicalIdentityError,
  createCanonicalIdentityService,
  readCanonicalIdentityConfig
} from "./identity-wire-v1.js";

const jsonParser = express.json({ limit: "32kb", type: "application/json" });

export function attachCanonicalIdentityApi(app, {
  dataDir,
  config,
  authenticateServiceEvent
} = {}) {
  const settings = config || readCanonicalIdentityConfig();
  if (settings.enabled !== true) {
    app.use("/api/identity/v1", (_req, res) => {
      setNoStore(res);
      res.status(404).json({ ok: false, error: "identity_v1_disabled" });
    });
    return { enabled: false, service: null };
  }
  const service = createCanonicalIdentityService({
    filePath: path.join(dataDir || "data", "identity-v1", "event-consumer-state.json"),
    runtimeMode: settings.runtimeMode,
    clientId: settings.clientId,
    targetOrigin: settings.targetOrigin,
    issuers: settings.issuers,
    ...(typeof settings.now === "function" ? { now: settings.now } : {})
  });
  const limiter = createRateLimiter();
  const guard = (req, res, next) => {
    setNoStore(res);
    if (!limiter(req.ip || req.socket?.remoteAddress || "unknown")) {
      res.status(429).json({ ok: false, error: "identity_v1_rate_limited" });
      return;
    }
    next();
  };

  app.get("/api/identity/v1/capabilities", guard, (_req, res) => {
    res.json(service.capabilities());
  });

  app.get("/api/identity/v1/readiness", guard, (_req, res) => {
    const readiness = service.readiness({
      serviceAuthenticationAvailable: typeof authenticateServiceEvent === "function",
      oidcBffSessionAvailable: false
    });
    res.status(503).json(readiness);
  });

  app.post(
    "/api/identity/v1/service-events",
    guard,
    authenticate(typeof authenticateServiceEvent === "function" ? authenticateServiceEvent : null),
    parseJson,
    route(async (req, res) => {
      const result = await service.applyServiceEvent(req.body);
      const status = result.status === "quarantined" ? 202 : result.status === "security_incident" ? 409 : 200;
      res.status(status).json(result);
    })
  );

  return { enabled: true, service };
}

function authenticate(callback) {
  return (req, res, next) => {
    if (!callback) {
      res.status(503).json({ ok: false, error: "identity_v1_service_authentication_unavailable" });
      return;
    }
    Promise.resolve(callback(req)).then((accepted) => {
      if (accepted !== true) {
        res.status(401).json({ ok: false, error: "identity_v1_service_authentication_required" });
        return;
      }
      next();
    }).catch(() => {
      res.status(401).json({ ok: false, error: "identity_v1_service_authentication_required" });
    });
  };
}

function parseJson(req, res, next) {
  jsonParser(req, res, (error) => {
    if (!error) {
      next();
      return;
    }
    res.status(400).json({ ok: false, error: "identity_v1_json_invalid" });
  });
}

function route(handler) {
  return (req, res) => {
    Promise.resolve(handler(req, res)).catch((error) => {
      const known = error instanceof CanonicalIdentityError;
      const status = known ? error.status : 500;
      const code = known ? error.code : "identity_v1_internal_error";
      if (!res.headersSent && !res.destroyed) res.status(status).json({ ok: false, error: code });
    });
  };
}

function setNoStore(res) {
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Pragma", "no-cache");
}

function createRateLimiter() {
  const buckets = new Map();
  return (key) => {
    const now = Date.now();
    const bucket = buckets.get(key);
    if (!bucket || now - bucket.startedAt >= 60_000) {
      buckets.set(key, { startedAt: now, count: 1 });
      return true;
    }
    bucket.count += 1;
    return bucket.count <= 120;
  };
}
