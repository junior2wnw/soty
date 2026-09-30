import express from "express";
import path from "node:path";
import { attachAccountTransfer } from "./account-transfer.js";
import { attachConnectModule, connectAllowedOrigins } from "./connect-module.js";
import { attachConnectorApi } from "./connector-api.js";
import { attachCanonicalIdentityApi } from "./identity-wire-v1-api.js";
import { attachSotyIdentityAdapterApi } from "./soty-identity-adapter-api.js";
import { attachTrafficControl } from "./traffic-control.js";
import { attachConnectReleaseSource } from "./connect-release-source.js";
import { createWorldService } from "../modules/world/server/index.mjs";
import { createNotesService } from "../modules/notes/server/index.mjs";
import { createAppsService } from "../modules/apps/server/index.mjs";
import { createAppJobsExtension } from "./apps-jobs.js";
import { createCapabilitiesService } from "../modules/capabilities/server/index.mjs";

export function createHttpApp(distDir, { dataDir, trafficTunnel, connectOrigins, gonka, appOriginTemplate = process.env.SOTY_APP_ORIGIN_TEMPLATE || '', localConnectorPort = Number(process.env.SOTY_LOCAL_CONNECTOR_PORT || 49424) } = {}) {
  const app = express();
  const safeConnectorPort = Number.isSafeInteger(localConnectorPort) && localConnectorPort >= 1024 && localConnectorPort <= 65535 ? localConnectorPort : 49424;
  const localConnectorOrigin = `http://127.0.0.1:${safeConnectorPort}`;
  app.disable("x-powered-by");
  if (process.env.SOTY_TRUST_PROXY) app.set('trust proxy', process.env.SOTY_TRUST_PROXY.split(',').map(value => value.trim()).filter(Boolean));
  app.use((req, res, next) => {
    if (app.locals.appsService?.handleRequest(req, res)) return;
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
      `connect-src 'self' wss://xn--n1afe0b.online http://127.0.0.1:49424 http://localhost:49424 ${localConnectorOrigin}${devConnectSrc ? ` ${devConnectSrc}` : ""}`,
      "manifest-src 'self'",
      "worker-src 'self'",
      `frame-src 'self'${appOriginTemplate ? ` ${appOriginTemplate.replace('{appId}', '*')}` : ''}`,
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
  attachConnectReleaseSource(app, { directory: process.env.SOTY_CONNECT_RELEASE_DIR || path.join(dataDir || path.resolve('data'), 'connect-releases') });
  attachAccountTransfer(app, { dataDir });
  const world = createWorldService({ databasePath: path.join(dataDir || path.resolve('data'), 'world', 'world.sqlite'), projectId: 'soty' });
  const notes = createNotesService({ databasePath: path.join(dataDir || path.resolve('data'), 'notes', 'notes.sqlite'), projectId: 'soty' });
  const connectors = attachConnectorApi(app, { dataDir, gonka });
  const shellOrigins = connectAllowedOrigins(connectOrigins);
  let connect;
  const capabilities = createCapabilitiesService({
    databasePath: path.join(dataDir || path.resolve('data'), 'capabilities', 'capabilities.sqlite'),
    actorActive: actor => connect?.isActorActive(actor) === true,
  });
  const apps = createAppsService({ dataDir, appOriginTemplate, shellOrigins,
    actorActive: actor => connect?.isActorActive(actor) === true,
    canAccessCommunity: (accountId, communityId) => world.canAccessCommunity(accountId, communityId),
    activeCommunityIds: accountId => world.activeCommunityIds(accountId),
    isGroupAdmin: (accountId, communityId) => world.isGroupAdmin(accountId, communityId),
    subscribeMembership: listener => world.subscribeMembership(listener),
    authenticateConnector: async auth => { await connectors.store.writeQueue; await connectors.store.readable(); return Boolean(connectors.store.authenticate({ ...auth, deviceId: auth.hostDeviceId || auth.deviceId })); },
  });
  const appJobs = createAppJobsExtension({ store: connectors.store, actorActive: actor => connect?.isActorActive(actor) === true,
    inferenceReady: () => connectors.modelProxy.ready === true,
    resolveOwnedDevice: (actor, ids) => apps.resolveOwnedDevice(actor, ids.hostDeviceId, ids.connectorId) });
  app.locals.appsService = apps;
  // Public, content-free permission check for the TLS edge. Only a registered
  // application on the configured isolated origin can request a certificate.
  app.get('/api/apps/tls-allow', (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.status(apps.allowsTlsDomain(req.query.domain) ? 204 : 403).end();
  });
  app.locals.worldService = world;
  app.locals.capabilitiesService = capabilities;
  app.get('/api/capabilities/v1/status', (_req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.json(app.locals.capabilitiesApiStatus?.() ?? { notesCreateEnabled: false, audience: null });
  });
  app.locals.connectService = connect = attachConnectModule(app, { dataDir, origins: shellOrigins, extensions: [world, apps, appJobs, notes, capabilities],
    canRequestContact: (actorId, targetId) => world.canRequestContact(actorId, targetId) });
  const unsubscribeRevocations = connect.subscribeRevocations(event => apps.invalidateAccess(event));
  app.locals.closeServices = async () => { unsubscribeRevocations(); apps.close(); world.close(); notes.close(); capabilities.close(); connect.close(); await connectors.store.close(); };
  app.get('/api/apps/capabilities', (_req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.json({ configured: apps.configured, agentConfigured: connectors.modelProxy.ready === true, localConnectorOrigin, protocol: 1 });
  });
  attachCanonicalIdentityApi(app, { dataDir });
  attachSotyIdentityAdapterApi(app, { dataDir });
  app.get("/health", (_req, res) => res.json({
    ok: true,
    agentModelProxy: connectors.modelProxy,
    applicationModelProxy: connectors.applicationModelProxy
  }));
  app.get("/ready", async (_req, res) => {
    let storageReady = false;
    try { await connectors.store.readable(); storageReady = !connectors.store.maintenance(); } catch { /* Report unavailable without exposing storage data. */ }
    const ready = storageReady && connectors.modelProxy.ready === true && connectors.applicationModelProxy.ready === true;
    res.status(ready ? 200 : 503).json({
      ok: ready,
      storageReady,
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
