import express from "express";
import { createConnectorStore } from "./connector-store.js";
import { createApplicationTokenAuthenticator, createGonkaProxy } from "./gonka-proxy.js";

const jsonParser = express.json({ limit: "1mb", type: "application/json" });
const modelJsonParser = express.json({ limit: "4mb", type: "application/json" });

export function attachConnectorApi(app, { dataDir, gonka } = {}) {
  const root = dataDir || "data";
  const store = createConnectorStore(root);
  const modelProxy = createGonkaProxy({ store, ...gonka });
  const applicationAuthenticator = createApplicationTokenAuthenticator(gonka?.applicationTokens);

  app.post("/api/connectors/register", jsonParser, route(async (req, res) => {
    respond(res, await store.register(req.body, bearerToken(req)));
  }));

  app.get("/api/connectors/status", route(async (req, res) => {
    respond(res, await store.status(linkId(req), String(req.query.deviceId || "")));
  }));

  app.post("/api/connectors/jobs", jsonParser, route(async (req, res) => {
    const result = await store.createJob({ ...req.body, linkId: linkId(req, req.body) });
    respond(res, result, result.ok ? 201 : undefined);
  }));

  app.get("/api/connectors/jobs/:id", route(async (req, res) => {
    respond(res, await store.getJob(linkId(req), req.params.id));
  }));

  app.get("/api/connectors/jobs/:id/events", route(async (req, res) => {
    const after = Number.parseInt(String(req.query.after || "0"), 10) || 0;
    let result = await store.getEvents(linkId(req), req.params.id, after);
    if (result.ok && result.events.length === 0 && result.done !== true && req.query.wait === "1") {
      const job = result.job;
      await store.waitForChange(linkId(req), job.deviceId || "", 25_000, responseSignal(res));
      result = await store.getEvents(linkId(req), req.params.id, after);
    }
    respond(res, result);
  }));

  app.post("/api/connectors/jobs/:id/cancel", jsonParser, route(async (req, res) => {
    respond(res, await store.cancelJob(linkId(req, req.body), req.params.id));
  }));

  app.get("/api/connectors/poll", route(async (req, res) => {
    const auth = connectorAuth(req);
    const waitMs = req.query.wait === "1" ? 25_000 : 0;
    respond(res, await store.poll(auth, waitMs, responseSignal(res)));
  }));

  app.post("/api/connectors/jobs/:id/events", jsonParser, route(async (req, res) => {
    respond(res, await store.appendEvent(connectorAuth(req, req.body), req.params.id, req.body?.event || req.body));
  }));

  app.post("/api/connectors/jobs/:id/result", jsonParser, route(async (req, res) => {
    respond(res, await store.finishJob(connectorAuth(req, req.body), req.params.id, req.body?.result || req.body));
  }));

  app.post("/api/connectors/gonka/v1/chat/completions", modelJsonParser, (req, res) => {
    void modelProxy.handleChatCompletions(req, res).catch(() => {
      respond(res, { ok: false, error: "model-proxy-internal-error" }, 500);
    });
  });

  app.post("/api/inference/v1/chat/completions", modelJsonParser, (req, res) => {
    void modelProxy.handleChatCompletions(req, res, {
      authenticateToken: applicationAuthenticator,
      client: "application"
    }).catch(() => {
      respond(res, { ok: false, error: "model-proxy-internal-error" }, 500);
    });
  });

  return {
    store,
    modelProxy: {
      ready: modelProxy.ready,
      model: modelProxy.model,
      transport: modelProxy.transport
    },
    applicationModelProxy: {
      ready: modelProxy.ready && applicationAuthenticator.ready,
      model: modelProxy.model,
      transport: "application-token-server-proxy",
      path: "/api/inference/v1/chat/completions"
    }
  };
}

function route(handler) {
  return (req, res) => {
    Promise.resolve(handler(req, res)).catch((error) => {
      console.error("[soty] Connector API request failed", error);
      respond(res, { ok: false, error: "connector-internal-error" }, 500);
    });
  };
}

function connectorAuth(req, body = {}) {
  return {
    linkId: linkId(req, body),
    deviceId: String(req.headers["x-soty-device-id"] || req.query.deviceId || body?.deviceId || ""),
    connectorId: String(req.headers["x-soty-connector-id"] || req.query.connectorId || body?.connectorId || ""),
    token: bearerToken(req)
  };
}

function responseSignal(res) {
  const controller = new AbortController();
  res.once("close", () => controller.abort());
  return controller.signal;
}

function linkId(req, body = {}) {
  return String(req.headers["x-soty-link-id"] || req.query.linkId || body?.linkId || "");
}

function bearerToken(req) {
  const match = /^Bearer\s+([A-Za-z0-9_-]{40,160})$/u.exec(String(req.headers.authorization || ""));
  return match?.[1] || "";
}

function respond(res, result, preferredStatus) {
  const status = preferredStatus || (result?.ok ? 200 : errorStatus(result?.error));
  if (!res.headersSent && !res.destroyed) res.status(status).json(result);
}

function errorStatus(error) {
  if (error === "job-not-found") return 404;
  if (error === "connector-auth-failed") return 401;
  if (error === "connector-queue-full") return 503;
  return 400;
}
