import express from "express";
import { createAgentSourceAccess } from './agent-source-access.js';
import path from "node:path";
import { attachAccountTransfer } from "./account-transfer.js";
import { attachConnectModule, connectAllowedOrigins } from "./connect-module.js";
import { attachConnectorApi } from "./connector-api.js";
import { attachCanonicalIdentityApi } from "./identity-wire-v1-api.js";
import { attachSotyIdentityAdapterApi } from "./soty-identity-adapter-api.js";
import { attachTrafficControl } from "./traffic-control.js";
import { attachConnectReleaseSource } from "./connect-release-source.js";
import { createWorldService } from "../modules/world/server/index.mjs";
import { createCoverDecoder } from '../modules/cover-art/server/codec.mjs';
import { createNotesService } from "../modules/notes/server/index.mjs";
import { createAppsService } from "../modules/apps/server/index.mjs";
import { createAppJobsExtension } from "./apps-jobs.js";
import { createCapabilitiesService, BUILTIN_CAPABILITIES, AccessError } from "../modules/capabilities/server/index.mjs";
import { hasSingleHostHeader, legacyAppFrameSource, validateNamedAppZone } from './app-domain-policy.mjs';
import { attachCapabilitiesDiscovery, validateDiscoveryOrigin } from './capabilities-discovery.js';
import { attachCapabilitiesActions, validateCapabilityAudience } from './capabilities-actions.js';
import { buildCapabilitiesOpenApi } from './capabilities-openapi.js';
import { startNativeRecovery } from './capabilities-recovery.js';
import { createOAuthHostProfile, reserveOAuthNamespaces } from './capabilities-oauth-profile.js';
import { attachCapabilitiesOAuth } from './capabilities-oauth.js';
import { startOAuthCleanup } from './capabilities-oauth-cleanup.js';
import { readAppHostingConfig } from './app-hosting-config.mjs';
import { attachCapabilitiesMcp } from './capabilities-mcp.js';
import { createUniversalApps } from './universal-apps.js';
import { forcedLegacyMode } from './universal-mode.js';
import { createReviewsService } from '../modules/reviews/server/index.mjs';
import { createManagedRootReviews } from '../modules/reviews/server/managed-root.mjs';
import { createHumanIdentityHostProfile } from '../modules/human-identity/profile.mjs';
import { createHumanIdentityService } from '../modules/human-identity/service.mjs';
import { createSourceAuthoritySigner, createRootWorkspaceSubjectAuthority } from '../modules/human-identity/source-authority.mjs';
import { validateHumanIdentityRenewalLimits } from './universal-configuration.js';
import { approvedEmbedProfile } from '../modules/apps/scoped-embed/profile-dispatch.mjs';
import { attachHumanIdentity } from './human-identity.js';
import { captureExternalApplications, composeExternalApplications } from './external-applications.js';
import { attachExternalCapabilities } from './external-capabilities.js';
import { createAppFeedbackCapabilities } from './app-feedback-capabilities.js';
import { captureUniversalPreparedness } from '../modules/app-contract/universal-preparedness.mjs';
import { fenceClosingHttpSocket } from './http-closing-socket.mjs';
import { createPersonalAgentComposition } from './personal-agent-composition.js';
import { createNativeVoiceComposition } from './native-voice-composition.js';
import { createVoiceOrbComposition } from './voice-orb-composition.js';
import { createHumanCallModule } from './human-call-module.js';
import { createPushService, pushConfigurationFromEnvironment } from './push-service.js';
import { createAccountAiService } from './account-ai.js';
import { captureNativeSourceHandlers } from './native-source-handlers.js';

// Startup remains synchronous for callers. Its error privately retains the
// asynchronous worker shutdown so supervisors can wait before retrying or
// removing a rejected host's storage; no cleanup handle is serialized.
const rejectedStarts = new WeakMap();
export async function waitForRejectedHttpStart(error) {
  const cleanup = error && (typeof error === 'object' || typeof error === 'function')
    ? rejectedStarts.get(error) : undefined;
  if (cleanup) { await cleanup; rejectedStarts.delete(error); }
}

export function createHttpApp(distDir, options = {}) {
  const nativeSources = captureNativeSourceHandlers(options);
  try { return createHttpAppWithNativeSources(distDir, options, nativeSources); }
  catch (error) {
    if (nativeSources.enabled) {
      const cleanup = Promise.all([rejectedStarts.get(error), nativeSources.close()]).then(() => undefined);
      if (error && (typeof error === 'object' || typeof error === 'function')) rejectedStarts.set(error, cleanup);
    }
    throw error;
  }
}

function createHttpAppWithNativeSources(distDir, { env = process.env, dataDir, trafficTunnel, connectOrigins, gonka, capabilityAudience = '', nativeNotesEnabled = false, oauth, appHosting = readAppHostingConfig(), appOriginTemplate = env.SOTY_APP_ORIGIN_TEMPLATE || '', namedAppZone = env.SOTY_NAMED_APP_ZONE ?? appHosting.namedAppZone ?? '', discoveryOrigin = env.SOTY_DISCOVERY_ORIGIN ?? appHosting.discoveryOrigin ?? '', localConnectorPort = Number(env.SOTY_LOCAL_CONNECTOR_PORT || 49424), universalAppsEnabled = env.SOTY_UNIVERSAL_APPS_ENABLED !== 'false', humanIdentity, humanIdentityRenewalMigration = false, humanIdentityDeviceConsentMigration = false, humanIdentityRenewalLimits, reviewsConfiguration, allowReviewsFixtureOrigins = false, externalApplications,
  allowScopedEmbedMigration = false, allowSelectedResourceMigration = false, allowAudienceMigration = false, allowWorld7Migration = false, scopedEmbedProfiles = [], scopedEmbedRegistryConfigured = false, personalAgent, accountAi, agentSourceAccess, managedReviewsSource, nativeVoice, voiceOrb, notifications, localExecutorCurrent = false } = {}, nativeSources) {
  if (typeof allowAudienceMigration !== 'boolean') throw new AccessError('apps_audience_migration_configuration_invalid');
  if (typeof allowWorld7Migration !== 'boolean') throw new AccessError('world_attachment_migration_configuration_invalid');
  if (typeof localExecutorCurrent !== 'boolean') throw new AccessError('local_executor_configuration_invalid');
  if (typeof universalAppsEnabled !== 'boolean') throw new AccessError('universal_configuration_invalid');
  if (typeof humanIdentityRenewalMigration !== 'boolean') throw new AccessError('universal_configuration_invalid');
  if (typeof humanIdentityDeviceConsentMigration !== 'boolean') throw new AccessError('universal_configuration_invalid');
  humanIdentityRenewalLimits = validateHumanIdentityRenewalLimits(humanIdentityRenewalLimits);
  if(typeof allowScopedEmbedMigration!=='boolean'||typeof allowSelectedResourceMigration!=='boolean'||typeof scopedEmbedRegistryConfigured!=='boolean'||!Array.isArray(scopedEmbedProfiles)||scopedEmbedProfiles.length>64)throw new AccessError('universal_configuration_invalid');
  const universalEnabled = universalAppsEnabled && !forcedLegacyMode;
  allowScopedEmbedMigration = universalEnabled && allowScopedEmbedMigration;
  allowSelectedResourceMigration = universalEnabled && allowSelectedResourceMigration;
  scopedEmbedRegistryConfigured = universalEnabled && scopedEmbedRegistryConfigured;
  scopedEmbedProfiles = universalEnabled ? scopedEmbedProfiles.map(value=>{const {digest:_derived,...pin}=approvedEmbedProfile(value);return Object.freeze(pin);}) : [];
  const externalEntries = universalEnabled ? captureExternalApplications(externalApplications) : [];
  const shellOrigins = connectAllowedOrigins(connectOrigins, env);
  let world, notes, connectors, capabilities, connect, apps, universal, reviews, managedReviews, human, humanCalls, personal, voice, orbVoice, push,
    nativeRecovery, oauthCleanup, mcp, unsubscribeRevocations, sourceAccess, accountAiService;
  const appFeedback = universalEnabled ? createAppFeedbackCapabilities({ capabilities: () => capabilities,
    connect: () => connect, apps: () => apps, feedback: () => universal.feedback }) : null;
  const coverDecoder = createCoverDecoder(), registeredCoverOperations = new Set();
  // Validate before opening any storage: a rejected configuration cannot migrate data.
  const domainProfile = appHosting.domainProfile || 'separate-site';
  const admittedNamedZone = validateNamedAppZone({ namedAppZone, shellOrigins, appOriginTemplate, domainProfile });
  const retainedNamedAppZones = (appHosting.retainedNamedAppZones || []).map(zone =>
    validateNamedAppZone({ namedAppZone: zone, shellOrigins, appOriginTemplate, domainProfile }));
  const admittedDiscoveryOrigin = validateDiscoveryOrigin({ discoveryOrigin, shellOrigins });
  const admittedCapabilityAudience = validateCapabilityAudience({ audience: capabilityAudience, shellOrigins, enabled: nativeNotesEnabled });
  const oauthProfile = createOAuthHostProfile(oauth, { shellOrigins, audience: admittedCapabilityAudience });
  const humanProfile = universalEnabled ? createHumanIdentityHostProfile(humanIdentity, { shellOrigins }) : null;
  if (humanProfile && (humanProfile.registryId !== 'soty' || humanProfile.environmentId !== 'production')) throw new AccessError('human_identity_configuration_invalid');
  // Validate optional review pins before opening any product database. These
  // captured closures are called only after the actual Connect/Apps wiring.
  if (universalEnabled) reviews = createReviewsService({ configuration: reviewsConfiguration, allowFixtureOrigins: allowReviewsFixtureOrigins,
    actorActive: actor => connect?.isActorActive(actor) === true,
    withAppAuthority: (request, callback) => apps.withAppAuthority(request, callback) });
  if (universalEnabled) managedReviews = createManagedRootReviews({ configuration: reviewsConfiguration, managedSource: managedReviewsSource, allowFixtureOrigins: allowReviewsFixtureOrigins,
    withRootActorAuthority: (actor, callback) => connect.withActorAuthorityFence(actor, () => callback({ accountId: actor.accountId, deviceId: actor.deviceId })),
    withAppAuthority: (request, callback) => apps.withAppAuthority(request, callback) });
  const reviewOrigins = reviews?.origins() ?? [];
  const legacyFrameSource = legacyAppFrameSource(appOriginTemplate);
  const app = express();
  const safeConnectorPort = Number.isSafeInteger(localConnectorPort) && localConnectorPort >= 1024 && localConnectorPort <= 65535 ? localConnectorPort : 49424;
  const localConnectorOrigin = `http://127.0.0.1:${safeConnectorPort}`;
  app.disable("x-powered-by");
  if (env.SOTY_TRUST_PROXY) app.set('trust proxy', env.SOTY_TRUST_PROXY.split(',').map(value => value.trim()).filter(Boolean));
  app.use(fenceClosingHttpSocket);
  app.use((req, res, next) => {
    if (!hasSingleHostHeader(req)) { res.status(400).set('Cache-Control', 'no-store').end(); return; }
    if (app.locals.appsService?.handleRequest(req, res)) return;
    if (!trafficTunnel?.handleRequest(req, res)) {
      next();
    }
  });
  const devConnectSrc = String(env.SOTY_DEV_CONNECT_SRC || "")
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
      "media-src 'self' blob: data:",
      "font-src 'self'",
      `connect-src 'self' wss://xn--n1afe0b.online http://127.0.0.1:* http://localhost:49424 ${localConnectorOrigin}${devConnectSrc ? ` ${devConnectSrc}` : ""}${reviewOrigins.map(origin => ` ${origin}`).join('')}`,
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
      `microphone=(self${voice?.publicMount()?' "'+voice.publicMount().frameOrigin+'"':''})`,
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
  if (nativeSources.enabled) app.use(nativeSources.middleware);
  attachConnectReleaseSource(app, { directory: env.SOTY_CONNECT_RELEASE_DIR || path.join(dataDir || path.resolve('data'), 'connect-releases') });
  attachAccountTransfer(app, { dataDir });
  const failedStart = error => {
    const pending = [];
    const close = action => {
      try { pending.push(Promise.resolve(action()).catch(() => {})); }
      catch { /* Preserve the startup failure. */ }
    };
    close(() => nativeRecovery?.close()); close(() => oauthCleanup?.close()); close(() => unsubscribeRevocations?.());
    close(() => mcp?.close());
    for (const service of [push, orbVoice, voice, personal, accountAiService, sourceAccess, humanCalls, human, universal, managedReviews, reviews, apps, capabilities, notes, world, connect]) {
      close(() => service?.close());
    }
    close(() => connectors?.store.close());
    const cleanup = Promise.all(pending).then(() => undefined);
    if (error && (typeof error === 'object' || typeof error === 'function')) rejectedStarts.set(error, cleanup);
  };
  try {
    if (accountAi) {
      accountAiService = createAccountAiService({ configuration: accountAi, getConnect: () => connect,
        admitNewWork: () => Boolean(connectors && !connectors.store.maintenance()),
        databasePath: path.join(dataDir || path.resolve('data'), 'account-ai', 'bridge.sqlite') });
      app.post('/internal/soty-ai/v1/authorize-dispatch', express.raw({ type: 'application/json', limit: '16kb' }), (req, res) => {
        res.setHeader('Cache-Control', 'no-store');
        if (!Buffer.isBuffer(req.body)) { res.status(403).json({ authorized: false }); return; }
        let body; try { body = new TextDecoder('utf-8', { fatal: true }).decode(req.body); }
        catch { res.status(403).json({ authorized: false }); return; }
        void accountAiService.authorizeDispatch({ method: req.method, route: req.originalUrl, body, headers: req.headers })
          .then(value => res.json(value), () => res.status(403).json({ authorized: false }));
      }, (_error, _req, res, _next) => { res.status(403).set('Cache-Control', 'no-store').json({ authorized: false }); });
    }
    sourceAccess = createAgentSourceAccess({ entries: externalEntries, getConnect: () => connect, getCapabilities: () => capabilities,
      getApps: () => apps, configuration: agentSourceAccess, databasePath: path.join(dataDir || path.resolve('data'), 'agent-source-access', 'credentials.sqlite'),
      audience: admittedCapabilityAudience ? admittedCapabilityAudience.replace(/\/$/u, '') + '/mcp' : null,
      legacyResolver: personalAgent?.sourceCredentialFor });
    personal = createPersonalAgentComposition({ getConnect: () => connect, getServices: () => ({ notes, capabilities, apps, connectors }),
      configuration: { ...personalAgent, ...(accountAiService ? { enabled: true, gateway: { accountBridge: accountAiService } } : {}), sourceReferences: personalAgent?.sourceReferences ?? externalEntries.map(entry => entry.contract),
        sourceCredentialFor: sourceAccess.sourceCredentialFor } });
    voice=createNativeVoiceComposition({getConnect:()=>connect,getServices:()=>({capabilities}),agent:personal,configuration:nativeVoice});
    orbVoice = createVoiceOrbComposition({ getConnect: () => connect, agent: personal, configuration: voiceOrb });
    push = createPushService({ databasePath: path.join(dataDir || path.resolve('data'), 'push', 'push.sqlite'), projectId: 'soty',
      configuration: notifications?.configuration ?? pushConfigurationFromEnvironment(env), getConnect: () => connect,
      recipientsFor: (event, page) => world.notificationRecipients(event, page), authorizeDelivery: (actor, event) => world.canNotify(actor, event),
      authorizeOpen: (actor, event) => world.notificationTarget(actor, event),
      ...(notifications?.sendNotification ? { sendNotification: notifications.sendNotification } : {}),
      ...(notifications?.clock ? { clock: notifications.clock } : {}) });
    world = createWorldService({ databasePath: path.join(dataDir || path.resolve('data'), 'world', 'world.sqlite'), projectId: 'soty',
      allowWorld7Migration,
      withCurrentContacts: (actor, callback) => connect.withCurrentContacts(actor, callback),
      profileCoverDecoder: coverDecoder, profileCoverRegistered: operation => registeredCoverOperations.has(operation) });
    humanCalls = createHumanCallModule({ getConnect: () => connect, getWorld: () => world, allowedOrigins: shellOrigins });
    notes = createNotesService({ databasePath: path.join(dataDir || path.resolve('data'), 'notes', 'notes.sqlite'), projectId: 'soty',
      verifyNativeContext: (token, mode) => {
        if (!capabilities?.nativeNotes) throw new AccessError('native_unavailable');
        return capabilities.nativeNotes.verifyContext(token, mode);
      } });
    connectors = attachConnectorApi(app, { dataDir, gonka, accountAi: accountAiService,
      ...(accountAiService || localExecutorCurrent || env.SOTY_LOCAL_EXECUTOR_CURRENT === '1' ? { storeOptions: {
        ...(accountAiService ? { legacyModelSponsorEligible: binding => apps?.isUnownedConnector({ linkId: binding.linkId, hostDeviceId: binding.deviceId, connectorId: binding.connectorId }) === true } : {}),
        ownedJobActive: job => {
          const actor = { accountId: job.ownerAccountId, deviceId: job.sourceActorDeviceId };
          if (!connect?.isActorActive(actor)) return false;
          const current = apps?.resolveOwnedDevice(actor, job.deviceId, job.requestedConnectorId);
          return current?.linkId === job.linkId;
        },
      } } : {}) });
    capabilities = createCapabilitiesService({
      databasePath: path.join(dataDir || path.resolve('data'), 'capabilities', 'capabilities.sqlite'),
      projectId: 'soty', actorActive: actor => connect?.isActorActive(actor) === true,
      catalog: [...BUILTIN_CAPABILITIES.map(entry => ({ ...entry,
        executionEnabled: entry.capabilityId === 'notes.createDraft' && entry.version === 1 && nativeNotesEnabled })),...externalEntries.map(entry=>entry.catalog), ...(appFeedback?.catalog ?? [])],
      externalAdapters: [...composeExternalApplications(externalEntries.filter(entry=>!entry.readonly),{withConnectFence:action=>connect.withAuthorityFence(action),capabilities:()=>capabilities,apps:()=>apps}), ...(appFeedback?.effects ?? [])],
      readonlyQueries: [...composeExternalApplications(externalEntries.filter(entry=>entry.readonly),{withConnectFence:action=>connect.withAuthorityFence(action),capabilities:()=>capabilities,apps:()=>apps}), ...(appFeedback?.queries ?? [])],
      externalGuidance: externalEntries.flatMap(entry=>entry.guidance),
      nativeNotes: { notes: notes.native, withAuthorityFence: action => {
        if (!connect) throw new AccessError('native_unavailable');
        return connect.withAuthorityFence(action);
      } },
      ...(admittedCapabilityAudience ? { delegation: { audience: admittedCapabilityAudience, withAuthorityFence: action => {
        if (!connect) throw new AccessError('delegation_unavailable');
        return connect.withAuthorityFence(action);
      } } } : {}),
      ...(oauthProfile ? { oauth: oauthProfile.domainConfiguration(action => {
        if (!connect) throw new AccessError('oauth_unavailable');
        return connect.withAuthorityFence(action);
      }) } : {}),
    });
  } catch (error) { failedStart(error); throw error; }
  try {
    const rootWorkspaceSubject = humanProfile?.enabled ? createRootWorkspaceSubjectAuthority({ profile: humanProfile, scopedProfiles: scopedEmbedProfiles,
      withActorAuthorityFence: (actor, callback) => connect.withActorAuthorityFence(actor, callback) }) : undefined;
    apps = createAppsService({ dataDir, appOriginTemplate, namedAppZone: admittedNamedZone, retainedNamedAppZones, shellOrigins,
    namedCanonical: universalEnabled,
    readAppGuide: universalEnabled ? input => universal?.registration.readGuide(input) : undefined,
    allowScopedEmbedMigration,allowSelectedResourceMigration,allowAudienceMigration,scopedEmbedProfiles,coverDecoder,
    withHumanSubjectAuthority:humanProfile?.enabled?(request,callback)=>{if(!human)throw Object.assign(new Error('app_scoped_human_required'),{status:503,code:'app_scoped_human_required'});return request.rootProfileDigest === undefined ? human.withSubjectAuthority(request,callback) : rootWorkspaceSubject(request,callback);}:undefined,
    signSourceAuthority: humanProfile?.enabled ? createSourceAuthoritySigner({ profile: humanProfile, scopedProfiles: scopedEmbedProfiles }) : undefined,
    allowShellZoneRoot: domainProfile === 'shell-subdomains-v1',
    validateNamedZone: zone => validateNamedAppZone({ namedAppZone: zone, shellOrigins, appOriginTemplate, domainProfile }),
    actorActive: actor => connect?.isActorActive(actor) === true,
    withCurrentContacts: (actor, callback) => connect.withCurrentContacts(actor, callback),
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
    failedStart(error);
    throw error;
  }
  const appJobs = createAppJobsExtension({ store: connectors.store, actorActive: actor => connect?.isActorActive(actor) === true,
    inferenceReady: () => connectors.modelProxy.ready === true,
    resolveOwnedDevice: (actor, ids) => apps.resolveOwnedDevice(actor, ids.hostDeviceId, ids.connectorId) });
  try {
    if (universalEnabled) universal = createUniversalApps({ dataDir, apps, reviews, actorActive: actor => connect?.isActorActive(actor) === true });
    if (humanProfile?.enabled) human = createHumanIdentityService({ databasePath: path.join(dataDir || path.resolve('data'), 'human-identity', 'identity.sqlite'),
      profile: humanProfile, actorActive: actor => connect?.isActorActive(actor) === true,
      withAuthorityFence: action => connect.withAuthorityFence(action),
      withSubjectAuthorityFence: (actor, action) => connect.withActorAuthorityFence(actor, action),
      readProfile: () => ({}), allowRenewalMigration: humanIdentityRenewalMigration,
      allowDeviceConsentMigration: humanIdentityDeviceConsentMigration, renewalLimits: humanIdentityRenewalLimits });
  }
  catch (error) { failedStart(error); throw error; }
  app.locals.appsService = apps;
  app.locals.universalApps = universal;
  app.locals.humanIdentityService = human;
  app.get('/api/apps/catalog', (_req, res) => {
    res.setHeader('Cache-Control', 'no-store'); res.setHeader('Access-Control-Allow-Origin', '*');
    try { res.json(apps.publicCatalog()); } catch { res.status(503).json({ ok: false, code: 'apps_catalog_unavailable' }); }
  });
  // Public, content-free permission check for the TLS edge. Only a registered
  // application on the configured isolated origin can request a certificate.
  app.get('/api/apps/tls-allow', (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.status(apps.allowsTlsDomain(req.query.domain) ? 204 : 403).end();
  });
  app.locals.worldService = world;
  app.locals.pushService = push;
  app.locals.humanCalls = humanCalls;
  app.locals.notesService = notes;
  // Host-only service handle, like notesService/appsService; never part of an agent tool or HTTP DTO.
  app.locals.connectorService = connectors;
  app.locals.capabilitiesService = capabilities;
  app.locals.capabilitiesApiStatus = attachCapabilitiesActions(app, { service: capabilities, audience: admittedCapabilityAudience,
    appFeedbackReady: () => Boolean(!servicesCleanup && mcp && connect && universal?.feedback && appFeedback?.catalog.length === 7
      && appFeedback.catalog.every(entry => capabilities.external?.contracts.some(contract => contract.capabilityId === entry.capabilityId && contract.version === entry.version))),
    resourceMetadata: oauthProfile ? `${oauthProfile.origin}/.well-known/oauth-protected-resource` : null }).status;
  app.locals.externalCapabilities = attachExternalCapabilities(app,{service:capabilities,origin:admittedCapabilityAudience,appFeedback});
  attachCapabilitiesDiscovery(app, { catalog: capabilities.catalog, origin: admittedDiscoveryOrigin,
    openApi: buildCapabilitiesOpenApi({ oauthConfigured: Boolean(oauthProfile), mcpConfigured: Boolean(admittedCapabilityAudience), externalConfigured: Boolean(capabilities.external), guidanceConfigured: Boolean(capabilities.externalGuidance),queryConfigured:capabilities.external?.queryConfigured===true }),
    status: () => app.locals.capabilitiesApiStatus?.() ?? { notesCreateEnabled: false, audience: null } });
  try {
    const coverExtensions = [world.profileCoverExtension, apps.coverExtension].filter(Boolean);
    const extensions = [world, universal?.appsExtension ?? apps, apps.sourcePreparationExtension, appJobs, notes, capabilities, humanCalls, push,
      ...coverExtensions, ...(universal ? [universal.universalExtension, universal.feedback, universal.reviews] : []), ...(managedReviews ? [managedReviews] : []), ...(human ? [human] : []), ...(accountAiService ? [accountAiService] : []), sourceAccess, personal, orbVoice, ...(voice.operations.size ? [voice] : [])];
    app.locals.connectService = connect = attachConnectModule(app, { env, dataDir, origins: shellOrigins, extensions,
      canRequestContact: (actorId, targetId) => world.canRequestContact(actorId, targetId) });
    // Connect validates every operation and duplicate before returning. Advertise
    // profile writes only after that exact async registry has been installed.
    for (const extension of coverExtensions) for (const operation of extension.operations) registeredCoverOperations.add(operation);
    if (oauthProfile?.enabled && (capabilities.nativeNotes?.readiness().ready !== true
      || capabilities.oauth?.readiness().available !== true)) throw new AccessError('oauth_unavailable');
    // MCP reads/replays use the existing keyless authority resolver even when
    // authorization-server issuance is off. Its adapter precedes the OAuth
    // fallback namespace guard and does not enable native execution.
    app.locals.capabilitiesMcp = mcp = attachCapabilitiesMcp(app, { service: capabilities, origin: admittedCapabilityAudience, appFeedback });
    app.locals.oauthStatus = attachCapabilitiesOAuth(app, { profile: oauthProfile, service: capabilities, distDir });
    app.locals.humanIdentityStatus = attachHumanIdentity(app, { profile: humanProfile, service: human, distDir });
    unsubscribeRevocations = connect.subscribeRevocations(event => {
      apps.invalidateAccess(event); connectors.store.recheckModelAuthorizations();
    });
    accountAiService?.start();
    // Timers start only after actual Connect and provider admission. Disabled
    // issuance still permits bounded keyless cleanup and committed recovery.
    nativeRecovery = notes.schemaVersion === 2 && [2, 3].includes(capabilities.schemaVersion)
      ? startNativeRecovery({ coordinator: capabilities.nativeNotes }) : null;
    oauthCleanup = oauthProfile && capabilities.schemaVersion === 3
      ? startOAuthCleanup({ oauth: capabilities.oauth }) : null;
    push.attach({ subscribeMessages: listener => world.subscribeMessages(listener) });
  } catch (error) { failedStart(error); throw error; }
  app.locals.nativeRecovery = nativeRecovery;
  app.locals.oauthCleanup = oauthCleanup;
  app.locals.personalAgent = personal; app.locals.nativeVoice=voice; app.locals.voiceOrb = orbVoice;
  app.locals.accountAi = accountAiService;
  app.locals.captureUniversalPreparedness = () => captureUniversalPreparedness({
    compiledLegacyMode: forcedLegacyMode, universalConfigured: Boolean(universal), reviewsConfigured: Boolean(reviews && universal?.reviews),
    humanProfile, humanHttpEnabled: app.locals.humanIdentityStatus.enabled,
    selectedProfiles: scopedEmbedProfiles, selectedMigrationConfigured: allowScopedEmbedMigration,
      ...(allowSelectedResourceMigration||scopedEmbedProfiles.some(profile=>profile.schema==='soty.selected-human-embed.v2')?{selectedResourceMigrationConfigured:allowSelectedResourceMigration}:{}),
    selectedRegistryConfigured: scopedEmbedRegistryConfigured,
    ...(reviews ? { reviewsPreparedness: reviews.preparedness() } : {}),
  });
  let servicesCleanup;
  app.locals.closeServices = () => {
    if (servicesCleanup) return servicesCleanup;
    let complete, reject;
    servicesCleanup = new Promise((resolve, fail) => { complete = resolve; reject = fail; });
    const cleanup = async () => {
      let failed = false;
      const attempt = async action => { try { await action(); } catch { failed = true; } };
      if (nativeSources.enabled) await attempt(() => nativeSources.close());
      await attempt(() => push.close());
      await attempt(() => orbVoice.close());
      await attempt(() => personal.close());
      await attempt(() => accountAiService?.close());
      await attempt(() => sourceAccess.close());
      await attempt(() => voice.close());
      await attempt(() => humanCalls.close());
      await attempt(() => nativeRecovery?.close());
      await attempt(() => oauthCleanup?.close());
      await attempt(() => unsubscribeRevocations());
      await attempt(() => mcp?.close());
      await attempt(() => human?.close());
      await attempt(() => universal?.close());
      await attempt(() => managedReviews?.close());
      await attempt(() => apps.close());
      await attempt(() => world.close());
      await attempt(() => notes.close());
      await attempt(() => capabilities.close());
      await attempt(() => connect.close());
      await attempt(() => connectors.store.close());
      if (failed) throw Object.assign(new Error('root_services_cleanup_failed'), { code: 'root_services_cleanup_failed' });
    };
    void cleanup().then(complete, reject);
    return servicesCleanup;
  };
  app.get('/api/apps/capabilities', (_req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.json({ configured: apps.configured, agentConfigured: connectors.modelProxy.ready === true, localConnectorOrigin, protocol: 1, targetBindingVersions: [1, 2], universalConfigured: Boolean(universal), humanIdentityConfigured: Boolean(human),
      humanSignins: human?.signinCapabilities ?? { schema: 'soty.human-signins-capability.v1', available: false, sessions: false,
        rememberedConsents: false, checkRevoke: false, scopes: [], sourceRootSessions: false, maximumPageSize: 0 },
      personalAgent: personal.status(), nativeVoice: voice.publicMount(), voiceOrb: orbVoice.status(), notifications: { configured: push.status().configured } });
  });
  app.get('/api/calls/capabilities', (_req, res) => {
    const state = humanCalls.status();
    res.setHeader('Cache-Control', 'no-store');
    res.json({ configured: state.signalingMounted, participantLimit: state.participantLimit,
      turnConfigured: state.turnConfigured, remoteMediaQualified: state.remoteMediaQualified });
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
  // Protocol URLs never become a cached-looking application shell when the
  // optional authorization server or MCP transport is unavailable.
  reserveOAuthNamespaces(app);
  app.use(express.static(distDir, {
    etag: true,
    index: false,
    setHeaders: (res, filePath) => {
      if (filePath.endsWith("index.html") || filePath.endsWith("sw.js") || filePath.endsWith("manifest.webmanifest")) {
        res.setHeader("Cache-Control", "no-store");
        return;
      }
      const publicPath = path.relative(distDir, filePath).split(path.sep).join('/');
      // Card and square-field pipelines retain versioned, content-hashed renditions.
      const immutableCover = /^app-art\/[a-z0-9-]+\/v\d{3,}-[a-f0-9]{12}\/cover-(?:160|320|640|960|1440)\.[a-f0-9]{12}\.webp$/u.test(publicPath);
      if (filePath.includes(`${path.sep}assets${path.sep}`) || immutableCover) {
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
