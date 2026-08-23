import express from "express";
import path from "node:path";
import {
  createSotyIdentityAdapterService,
  readSotyIdentityAdapterConfig
} from "./soty-identity-adapter.js";

const jsonParser = express.json({ limit: "32kb", type: "application/json" });
const rateWindowMs = 60_000;
const maxRequestsPerWindow = 120;

export function attachSotyIdentityAdapterApi(app, { dataDir, config } = {}) {
  const settings = config || readSotyIdentityAdapterConfig();
  app.use("/api/identity-compat", (_req, res) => {
    setNoStore(res);
    res.status(410).json({ ok: false, error: "identity_compat_contract_withdrawn" });
  });
  if (settings.enabled !== true) {
    app.use("/api/identity-adapter", (_req, res) => {
      setNoStore(res);
      res.status(404).json({ ok: false, error: "identity_adapter_disabled" });
    });
    return { enabled: false, service: null };
  }
  const service = createSotyIdentityAdapterService({
    filePath: path.join(dataDir || "data", "soty-identity-adapter", "state.json"),
    projectId: settings.projectId,
    audience: settings.audience,
    issuers: settings.issuers,
    runtimeMode: settings.runtimeMode,
    ...(typeof settings.now === "function" ? { now: settings.now } : {}),
    ...(settings.maxTokenTtlSec ? { maxTokenTtlSec: settings.maxTokenTtlSec } : {}),
    ...(settings.clockSkewSec !== undefined ? { clockSkewSec: settings.clockSkewSec } : {}),
    ...(settings.replayRetentionMs ? { replayRetentionMs: settings.replayRetentionMs } : {})
  });
  const allow = createRateLimiter();
  const guard = (req, res, next) => {
    setNoStore(res);
    if (!allow(req.ip || req.socket?.remoteAddress || "unknown")) {
      res.status(429).json({ ok: false, error: "identity_rate_limited" });
      return;
    }
    next();
  };

  app.get("/api/identity-adapter/experimental/capabilities", guard, (_req, res) => {
    res.json(service.capabilities());
  });

  app.get("/api/identity-adapter/experimental/readiness", guard, (_req, res) => {
    const readiness = service.readiness();
    res.status(readiness.ok ? 200 : 503).json(readiness);
  });

  app.post("/api/identity-adapter/experimental/exchanges/consume", guard, requireServerToServer, parseJson, (_req, res) => {
    res.status(503).json({ ok: false, error: "identity_legacy_workflow_disabled" });
  });

  app.post("/api/identity-adapter/experimental/events/apply", guard, requireServerToServer, parseJson, (_req, res) => {
    res.status(503).json({ ok: false, error: "identity_legacy_event_disabled" });
  });

  return { enabled: true, service };
}

function requireServerToServer(req, res, next) {
  if (req.headers.origin || req.headers.referer) {
    res.status(403).json({ ok: false, error: "identity_server_to_server_required" });
    return;
  }
  next();
}

function parseJson(req, res, next) {
  jsonParser(req, res, (error) => {
    if (!error) {
      next();
      return;
    }
    setNoStore(res);
    res.status(400).json({ ok: false, error: "identity_json_invalid" });
  });
}

function setNoStore(res) {
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Pragma", "no-cache");
}

function createRateLimiter() {
  const buckets = new Map();
  return (key) => {
    const now = Date.now();
    const current = buckets.get(key);
    if (!current || now - current.startedAt >= rateWindowMs) {
      buckets.set(key, { startedAt: now, count: 1 });
      if (buckets.size > 10_000) {
        for (const [candidate, bucket] of buckets) {
          if (now - bucket.startedAt >= rateWindowMs) buckets.delete(candidate);
        }
      }
      return true;
    }
    current.count += 1;
    return current.count <= maxRequestsPerWindow;
  };
}
