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
import { createCapabilitiesService, BUILTIN_CAPABILITIES, AccessError } from "../modules/capabilities/server/index.mjs";
import { hasSingleHostHeader, legacyAppFrameSource, validateNamedAppZone } from './app-domain-policy.mjs';
import { attachCapabilitiesDiscovery, validateDiscoveryOrigin } from './capabilities-discovery.js';
import { attachCapabilitiesActions, validateCapabilityAudience } from './capabilities-actions.js';
import { buildCapabilitiesOpenApi } from './capabilities-openapi.js';
import { startNativeRecovery } from './capabilities-recovery.js';

export function createHttpApp(distDir, { dataDir, trafficTunnel, connectOrigins, gonka, capabilityAudience = '', nativeNotesEnabled = false, appOriginTemplate = process.env.SOTY_APP_ORIGIN_TEMPLATE || '', namedAppZone = process.env.SOTY_NAMED_APP_ZONE || '', discoveryOrigin = process.env.SOTY_DISCOVERY_ORIGIN || '', localConnectorPort = Number(process.env.SOTY_LOCAL_CONNECTOR_PORT || 49424) } = {}) {
  const shellOrigins = connectAllowedOrigins(connectOrigins);
  // Validate before opening any storage: a rejected configuration cannot migrate data.
  const admittedNamedZone = validateNamedAppZone({ namedAppZone, shellOrigins, appOriginTemplate });
  const admittedDiscoveryOrigin = validateDiscoveryOrigin({ discoveryOrigin, shellOrigins });
  const admittedCapabilityAudience = validateCapabilityAudience({ audience: capabilityAudience, shellOrigins, enabled: nativeNotesEnabled });
  const legacyFrameSource = legacyAppFrameSource(appOriginTemplate);
  const app = express();
  const safeConnectorPort = Number.isSafeInteger(localConnectorPort) && localConnectorPort >= 1024 && localConnectorPort <= 65535 ? localConnectorPort : 49424;
  const localConnectorOrigin = `http://127.0.0.1:${safeConnectorPort}`;
  app.disable("x-powered-by");
  if (process.env.SOTY_TRUST_PROXY) app.set('trust proxy', process.env.SOTY_TRUST_PROXY.split(',').map(value => value.trim()).filter(Boolean));
  app.use((req, res, next) => {
    if (!hasSingleHostHeader(req)) { res.status(400).set('Cache-Control', 'no-store').end(); return; }
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
      `frame-src 'self'${[...new Set([legacyFrameSource, ...(app.locals.appsService?.frameSources() || [])].filter(Boolean))].map(origin => ` ${origin}`).join('')}`,
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
  let world, notes, connectors, capabilities, connect, apps;
  const failedStart = () => {
    for (const service of [apps, capabilities, notes, world, connect]) {
      try { service?.close(); } catch { /* Preserve the startup failure. */ }
    }
    try { void connectors?.store.close().catch(() => {}); } catch { /* Preserve the startup failure. */ }
  };
  try {
    world = createWorldService({ databasePath: path.join(dataDir || path.resolve('data'), 'world', 'world.sqlite'), projectId: 'soty' });
    notes = createNotesService({ databasePath: path.join(dataDir || path.resolve('data'), 'notes', 'notes.sqlite'), projectId: 'soty',
      verifyNativeContext: (token, mode) => {
        if (!capabilities?.nativeNotes) throw new AccessError('native_unavailable');
        return capabilities.nativeNotes.verifyContext(token, mode);
      } });
    connectors = attachConnectorApi(app, { dataDir, gonka });
    capabilities = createCapabilitiesService({
      databasePath: path.join(dataDir || path.resolve('data'), 'capabilities', 'capabilities.sqlite'),
      projectId: 'soty', actorActive: actor => connect?.isActorActive(actor) === true,
      catalog: BUILTIN_CAPABILITIES.map(entry => ({ ...entry,
        executionEnabled: entry.capabilityId === 'notes.createDraft' && entry.version === 1 && nativeNotesEnabled })),
      nativeNotes: { notes: notes.native, withAuthorityFence: action => {
        if (!connect) throw new AccessError('native_unavailable');
        return connect.withAuthorityFence(action);
      } },
    });
  } catch (error) { failedStart(); throw error; }
  try { apps = createAppsService({ dataDir, appOriginTemplate, namedAppZone: admittedNamedZone, shellOrigins,
    validateNamedZone: zone => validateNamedAppZone({ namedAppZone: zone, shellOrigins, appOriginTemplate }),
    actorActive: actor => connect?.isActorActive(actor) === true,
    canAccessCommunity: (accountId, communityId) => world.canAccessCommunity(accountId, communityId),
    activeCommunityIds: accountId => world.activeCommunityIds(accountId),
    isGroupAdmin: (accountId, communityId) => world.isGroupAdmin(accountId, communityId),
    withAuthorityFence: callback => world.withCommunityAuthorityFence(callback),
    readCommunityAuthority: (actor, ownerAccountId, relevantCommunityIds) => world.appCommunityAuthority(actor.accountId, ownerAccountId, relevantCommunityIds),
    subscribeMembership: listener => world.subscribeMembership(listener),
    authenticateConnector: async auth => { await connectors.store.writeQueue; await connectors.store.readable(); return Boolean(connectors.store.authenticate({ ...auth, deviceId: auth.hostDeviceId || auth.deviceId })); },
  }); } catch (error) {
    // Retained zones still need validation when new claims are disabled. A
    // rejected startup must not leave already opened services alive.
    failedStart();
    throw error;
  }
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
  app.locals.notesService = notes;
  app.locals.capabilitiesService = capabilities;
  app.locals.capabilitiesApiStatus = attachCapabilitiesActions(app, { service: capabilities, audience: admittedCapabilityAudience }).status;
  attachCapabilitiesDiscovery(app, { catalog: capabilities.catalog, origin: admittedDiscoveryOrigin,
    openApi: buildCapabilitiesOpenApi(),
    status: () => app.locals.capabilitiesApiStatus?.() ?? { notesCreateEnabled: false, audience: null } });
  try {
    app.locals.connectService = connect = attachConnectModule(app, { dataDir, origins: shellOrigins, extensions: [world, apps, apps.sourcePreparationExtension, appJobs, notes, capabilities],
      canRequestContact: (actorId, targetId) => world.canRequestContact(actorId, targetId) });
  } catch (error) { failedStart(); throw error; }
  const unsubscribeRevocations = connect.subscribeRevocations(event => apps.invalidateAccess(event));
  // Disabling new execution must still allow recovery of an already committed
  // effect. Ordinary v1/mixed stores never start the native recovery scanner.
  const nativeRecovery = notes.schemaVersion === 2 && capabilities.schemaVersion === 2
    ? startNativeRecovery({ coordinator: capabilities.nativeNotes }) : null;
  app.locals.nativeRecovery = nativeRecovery;
  app.locals.closeServices = async () => { nativeRecovery?.close(); unsubscribeRevocations(); apps.close(); world.close(); notes.close(); capabilities.close(); connect.close(); await connectors.store.close(); };
  app.get('/api/apps/capabilities', (_req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.json({ configured: apps.configured, agentConfigured: connectors.modelProxy.ready === true, localConnectorOrigin, protocol: 1, targetBindingVersions: [1, 2] });
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
