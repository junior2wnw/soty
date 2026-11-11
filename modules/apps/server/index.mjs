import { renderAppGuide } from '../../app-contract/guide.mjs';
import { scopedSubjectRequest } from '../scoped-embed/authority.mjs';
import { createHash, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Writable } from 'node:stream';
import { WebSocketServer } from 'ws';
import { AppsError, assertApps, textId, appId, appName, appPort, requestPath, runtimePath, cleanGrants, connectorKey, cleanHeaders, CHANNEL_SCHEMA, CHUNK_BYTES, FRAME_BYTES, LIMITS } from './protocol.mjs';
import { createWebSocketRelay, normalizeWebSocketLivenessTiming } from './websocket-relay.mjs';
import { migrateAppsSchema, inspectAppsSchema, requiredBindingVersion, selectedRuntimeProfile, runtimeTargetDigest } from './schema.mjs';
import {createFrameContinuity,hasFrameContinuitySchema,hasHostSessionContinuitySchema} from './frame-continuity.mjs';
import {createHostSessionContinuity,HOST_SESSION_SCHEMA} from './host-session-continuity.mjs';
import { createDomainRegistry, domainOperations, readNamedOrigins } from './domains.mjs';
import { createPublicationRegistry, publicationOperations } from './publications.mjs';
import { normalizeLegacyTemplate, normalizeNamedAppZone, normalizeDomainLimits, validateNamedOrigins } from './domain-policy.mjs';
import { createHostClassifier } from './hosts.mjs';
import { parseWidgetRuntimeHost, widgetRuntimeOrigin, captureWidgetRuntimeScope, widgetRuntimeScopeDigest,parseMainRuntimeHost,mainRuntimeOrigin,captureMainRuntimeScope } from './runtime-hosts.mjs';
import { renderBootPage, renderStatusPage } from './runtime-pages.mjs';
import { describeSourceObservation } from './source-observation.mjs';
import { createAppInspection } from './inspection.mjs';
import { createSourceRegistry } from './sources.mjs';
import { readPairedNativeEntry, requireNativeEntry, assertNativePortAvailable } from './native-entry.mjs';
import { createRuntimeBindings } from './runtime-bindings.mjs';
import { createLaunchPath, createScopedLaunchPath, checkedScopedEntryPath } from './launch-path.mjs';
import { createEngagementEntryResolver } from './engagement-access.mjs';
import { createSavedRegistry, savedOperations } from './saved.mjs';
import { createDiscussionRegistry, discussionOperations } from './discussions.mjs';
import { createDiscussionReadStore } from './discussion-reads.mjs';
import { createEngagementTransaction } from './engagement-transaction.mjs';
import { createAppDirectory, directoryOperations } from './directory.mjs';
import { createAppAuthorityPort } from './authority-port.mjs';
import {createWidgetSourceAuthority} from './widget-source-authority.mjs';
import {createPublicCanvasAuthority} from './public-canvas-authority.mjs';
import { createScopedAdmissionRegistry } from '../scoped-embed/admissions.mjs';
import { createScopedGateway, scopedRenewalBindingDigest } from '../scoped-embed/gateway.mjs';
import { approvedEmbedProfile, embedRoute } from '../scoped-embed/profile-dispatch.mjs';
import { selectedNativeHandoff, HIVE_SELECTED_SOURCE, HIVE_SELECTED_KERNEL_SOURCE } from '../scoped-embed/resource-route-adapters.mjs';
import { STANDARD_SELECTED_SOURCE_V3, isWorkspaceSource, isRootSignedWorkspaceSource } from '../../source-app/server/standard-profile.mjs';
import { hash as scopedHash, capture as scopedCapture, SCOPED_EMBED_LIMITS } from '../scoped-embed/profile.mjs';
import { parseSourceJson } from '../../source-app/shared/strict-json.mjs';
import { COVER_APP_FORMATS, hasAudienceSchema, readAppAudience } from './audience.mjs';
import { synchronous } from './engagement-transaction.mjs';
import { createAppCoverAdapter, appCoverOperations } from './cover.mjs';
import { createPreparedBrowse } from './prepared-browse.mjs';
import { SELF_IP_PROFILE, ENDPOINT_SCHEMA, SELF_ADDRESSES_SCHEMA, targetFromRow, readRuntimeEndpoint, normalizeSelfInventory } from './endpoint-targets.mjs';
import { createEndpointRegistration } from './endpoint-registration.mjs';
import {appAuthenticationCapability,appAuthenticationStatus} from './authentication.mjs';

export const operations = new Set(['apps.devices', 'apps.claim', 'apps.list', 'apps.catalog', 'apps.register', 'apps.update', 'apps.revoke', 'apps.launch', 'apps.session.renew', 'apps.session.confirm', 'apps.session.context', 'apps.session.close', 'apps.relay.identity', 'apps.relay.identity.close', 'apps.scoped.close', 'apps.entry.get', 'apps.inspect', 'apps.authentication.get', 'apps.source.promote', 'apps.source.history', ...domainOperations, ...publicationOperations, ...savedOperations, ...discussionOperations, ...directoryOperations]);
export function appIdForCreationJob(jobId) {
  assertApps(typeof jobId === 'string' && /^job_[a-f0-9]{32}$/u.test(jobId), 'invalid_identifier');
  return `app-${jobId.slice(4)}`;
}
const cookieName = 'soty_app_session';
const accountSessionMs = 3_600_000, publicLeaseMs = 30_000, publicStreams = 24;
const secret = () => randomBytes(32).toString('base64url');
const digest = value => createHash('sha256').update(value).digest('hex');
const equalDigest = (a, b) => typeof a === 'string' && typeof b === 'string' && /^[a-f0-9]{64}$/u.test(a) && /^[a-f0-9]{64}$/u.test(b) && timingSafeEqual(Buffer.from(a, 'hex'), Buffer.from(b, 'hex'));

export function createAppsService({ dataDir = 'data', databasePath = join(dataDir, 'apps', 'registry.sqlite'), appOriginTemplate = '', namedAppZone = '', namedCanonical = false, retainedNamedAppZones = [], domainLimits = {}, validateNamedZone, shellOrigins = [], allowShellZoneRoot = false, actorActive = () => false,
  canAccessCommunity = () => false, isGroupAdmin = () => false, activeCommunityIds, subscribeMembership, withAuthorityFence: withWorldAuthorityFence, withCurrentContacts,
  readCommunityAuthority, discussionLimits, webSocketLiveness, authenticateConnector = async () => false, connectorCurrent = () => false, onHeartbeat = () => {}, now = Date.now, blockedPorts = [], connectorAuthCheckMs = 10_000, accessAuditMs = 10_000,
  allowScopedEmbedMigration = false, allowSelectedResourceMigration = false, allowAudienceMigration = false, allowCoverMigration = false, allowSelfIpTargets = false, allowFrameContinuityMigration = false,
  coverDecoder, coverLimits, scopedEmbedProfiles = [], withHumanSubjectAuthority, signSourceAuthority, signRelayIdentity, relayIdentityIssuer,
  identitySignTimeoutMs = 8000, readAppGuide, onChange = () => {}, onRuntimeRequest = () => {}, invitationAccess,checkWidgetLaunch,checkWidgetRuntime,resolveWidgetRuntime } = {}) {
  assertApps(typeof onChange === 'function', 'apps_change_listener_invalid', 503);
  assertApps(typeof onRuntimeRequest === 'function'&&!['AsyncFunction','AsyncGeneratorFunction'].includes(onRuntimeRequest.constructor?.name), 'apps_observer_configuration_invalid', 503);
  assertApps(checkWidgetLaunch===undefined||typeof checkWidgetLaunch==='function'&&checkWidgetLaunch.constructor?.name!=='AsyncFunction','apps_widget_configuration_invalid',503);
  assertApps(checkWidgetRuntime===undefined||typeof checkWidgetRuntime==='function'&&!['AsyncFunction','AsyncGeneratorFunction'].includes(checkWidgetRuntime.constructor?.name)
    &&typeof checkWidgetLaunch==='function','apps_widget_configuration_invalid',503);
  assertApps(resolveWidgetRuntime===undefined||typeof resolveWidgetRuntime==='function'&&!['AsyncFunction','AsyncGeneratorFunction'].includes(resolveWidgetRuntime.constructor?.name)
    &&typeof checkWidgetLaunch==='function'&&typeof checkWidgetRuntime==='function','apps_widget_configuration_invalid',503);
  assertApps(typeof allowCoverMigration === 'boolean', 'apps_cover_migration_configuration_invalid', 503);
  assertApps(coverDecoder === undefined || coverDecoder && typeof coverDecoder.decode === 'function'
    && typeof withCurrentContacts === 'function' && typeof withWorldAuthorityFence === 'function'
    && !['AsyncFunction','AsyncGeneratorFunction'].includes(withWorldAuthorityFence.constructor?.name), 'apps_cover_configuration_invalid', 503);
  assertApps(coverLimits === undefined || coverDecoder !== undefined, 'apps_cover_configuration_invalid', 503);
  assertApps(withCurrentContacts === undefined || typeof withCurrentContacts === 'function'
    && !['AsyncFunction', 'AsyncGeneratorFunction'].includes(withCurrentContacts.constructor?.name), 'apps_contact_authority_invalid', 503);
  let contactsFrame = null;
  function withContactAuthority(actor, callback) {
    if (actor === undefined || withCurrentContacts === undefined) return callback();
    assertActor(actor);
    if (contactsFrame) {
      assertApps(contactsFrame.accountId === actor.accountId && contactsFrame.deviceId === actor.deviceId, 'apps_contact_authority_invalid', 503);
      return synchronous(callback(), 'apps_async_authority');
    }
    let active = true, entered = false, outcome;
    try {
      synchronous(withCurrentContacts(actor, view => {
        assertApps(active && !entered && view?.accountId === actor.accountId && view?.deviceId === actor.deviceId
          && typeof view.has === 'function' && typeof view.accountIds === 'function', 'apps_contact_authority_invalid', 503);
        entered = true; contactsFrame = view;
        try { outcome = synchronous(callback(), 'apps_async_authority'); return outcome; }
        finally { contactsFrame = null; }
      }), 'apps_async_authority');
      assertApps(entered, 'apps_contact_authority_invalid', 503); return outcome;
    } finally { active = false; }
  }
  const withAuthorityFence = (callback, actor) => {
    assertApps(typeof withWorldAuthorityFence === 'function' && withWorldAuthorityFence.constructor?.name !== 'AsyncFunction',
      'apps_authority_fence_required', 503);
    return withContactAuthority(actor, () => withWorldAuthorityFence(callback));
  };
  function isCurrentFriend(actor, ownerAccountId) {
    if (!contactsFrame || contactsFrame.accountId !== actor.accountId || contactsFrame.deviceId !== actor.deviceId) return false;
    return contactsFrame.has(ownerAccountId) === true;
  }
  function currentContactIds(actor) {
    if (!contactsFrame || contactsFrame.accountId !== actor.accountId || contactsFrame.deviceId !== actor.deviceId) return [];
    return contactsFrame.accountIds();
  }
  // Trusted host/test settings may shorten deadlines, never disable or widen
  // the bounded transport profile. Reject invalid settings before opening data.
  const webSocketTiming = normalizeWebSocketLivenessTiming(webSocketLiveness);
  assertApps(typeof namedCanonical === 'boolean', 'apps_named_canonical_configuration_invalid', 500);
  assertApps(Array.isArray(scopedEmbedProfiles)&&scopedEmbedProfiles.length<=64&&typeof allowScopedEmbedMigration==='boolean'&&typeof allowSelectedResourceMigration==='boolean','app_scoped_configuration_invalid',503);
  scopedEmbedProfiles=scopedEmbedProfiles.map(value=>{const {digest:_derived,...pin}=approvedEmbedProfile(value);return Object.freeze(pin);});
  assertApps(signSourceAuthority===undefined || typeof signSourceAuthority==='function','app_scoped_configuration_invalid',503);
  assertApps(signRelayIdentity===undefined || typeof signRelayIdentity==='function','app_identity_configuration_invalid',503);
  const authenticationCapability=appAuthenticationCapability({signer:signRelayIdentity,issuer:relayIdentityIssuer});
  assertApps(Number.isSafeInteger(identitySignTimeoutMs) && identitySignTimeoutMs >= 1 && identitySignTimeoutMs <= 8000,
    'app_identity_configuration_invalid', 503);
  assertApps(!scopedEmbedProfiles.some(profile=>isRootSignedWorkspaceSource(profile.sourceProfile)) || typeof signSourceAuthority==='function',
    'app_scoped_identity_witness_unavailable',503);
  assertApps(readAppGuide === undefined || typeof readAppGuide === 'function', 'app_guide_configuration_invalid', 500);
  const guidePath = value => value === '/_soty/guide' ? value : runtimePath(value);
  const guideLaunchPath = value => value === '/_soty/guide'
    ? { entryPath: value, bootPath: '/_soty/boot?' + new URLSearchParams({ path: value }) } : createLaunchPath(value);
  const origins = new Set(shellOrigins.map(value => new URL(value).origin));
  assertApps(origins.size > 0, 'apps_shell_origins_required');
  const bootRefusalOrigins = captureBootRefusalOrigins(origins);
  const template = validateTemplate(appOriginTemplate, origins);
  const namedZone = normalizeNamedAppZone(namedAppZone), limits = normalizeDomainLimits(domainLimits);
  assertApps(Array.isArray(retainedNamedAppZones) && retainedNamedAppZones.length <= 8, 'invalid_retained_app_zones');
  const retainedZones = retainedNamedAppZones.map(normalizeNamedAppZone);
  assertApps(retainedZones.every(Boolean), 'invalid_retained_app_zones');
  validateNamedOrigins(retainedZones, { shellOrigins: [...origins], validateNamedZone, allowShellZoneRoot });
  validateNamedOrigins([namedZone], { shellOrigins: [...origins], validateNamedZone, allowShellZoneRoot });
  mkdirSync(dirname(databasePath), { recursive: true });
  const db = new DatabaseSync(databasePath);
  let domains, publications;
  try {
    db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;');
    const schema = inspectAppsSchema(db);
    if (['v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12', 'v13', 'v14'].includes(schema)) validateNamedOrigins(readNamedOrigins(db), { shellOrigins: [...origins], validateNamedZone, allowShellZoneRoot });
    // A configured decoder initializes covers for a genuinely empty store.
    // Existing formats still require the explicit offline migration permission.
    migrateAppsSchema(db, { legacyTemplate: template, now, allowScopedEmbedMigration, allowSelectedResourceMigration, allowAudienceMigration, allowSelfIpTargets, allowFrameContinuityMigration,
      allowCoverMigration: allowCoverMigration || schema === 'empty' && coverDecoder !== undefined });
    publications = createPublicationRegistry({ db, now, assertActor, canUse, withContactAuthority, onChanged: event => invalidateAccess({ appId: event.appId }) });
    domains = createDomainRegistry({ db, now, assertActor, legacyTemplate: template, namedAppZone: namedZone, namedCanonical, retainedNamedAppZones: retainedZones, domainLimits: limits,
      shellOrigins: [...origins], validateNamedZone, allowShellZoneRoot, onRetireInTransaction: publications.retireInTransaction,
      onPolicyChanged: publications.notifyChanged });
    db.exec('PRAGMA journal_mode=WAL;');
  } catch (error) { db.close(); throw error; }
  const hostClassifier = createHostClassifier({ db, shellOrigins: [...origins], allowShellZoneRoot, resolveRuntimeHost });
  const scopedAdmissions = createScopedAdmissionRegistry({ db, profiles: scopedEmbedProfiles, clock: now });
  const channels = new Map(), tickets = new Map(), sessions = new Map(), live = new Map(), relayWindows = new Map();
  const voiceSourceCalls=new Set(),contextSourceCalls=new Set();
  const voiceSourceActors=new WeakMap();
  const backendSourceContexts=new Map();
  const backendSourceAcquisitions=new Set();
  let inspection, saved, discussions, discussionReads, directory, entryRead, appAuthority, widgetSourceAuthority, publicCanvasAuthority, scopedGateway, covers;
  let reviewedPublicationFrame = null;
  function publicationReconciliationSnapshot(authority) {
    return Object.freeze({ ...authority,
      sourceChangeEpoch: db.prepare('SELECT coalesce(max(committed_epoch),0) AS epoch FROM app_source_receipts WHERE app_id=?').get(authority.appId).epoch });
  }
  const scopedRenewals=new Map();
  const bootCandidates=new Map();
  const hostSessions=hasHostSessionContinuitySchema(db)?createHostSessionContinuity({db,clock:now}):null;
  try {
    inspection = createAppInspection({ db, assertActor, domains, publications, inspectSource, inspectBinding, inspectIntegrations, inspectLaunchEntry, now,
      shellOrigin: [...origins][0], nameClaimsEnabled: Boolean(namedZone), namedAppZone: namedZone });
    const resolveEntry = createEngagementEntryResolver({ db, assertActor, publications, inspectSource });
    appAuthority = createAppAuthorityPort({ db, assertActor, withAuthorityFence, resolveEntry,requireScopedTarget:target=>scopedAdmissions.require(target) });
    const widgetSourceOptions={db,assertActor,withAuthorityFence,resolveEntry,resolveDefaultEntry:({app,target,actor})=>defaultEntryFor(app,target,actor),
      publications,requireScopedTarget:target=>scopedAdmissions.require(target),inspectSource};
    widgetSourceAuthority=createWidgetSourceAuthority(widgetSourceOptions);publicCanvasAuthority=createPublicCanvasAuthority(widgetSourceOptions);
    if(scopedEmbedProfiles.length && typeof withHumanSubjectAuthority==='function') scopedGateway=createScopedGateway({
      ...(hasFrameContinuitySchema(db)?{frameContinuity:createFrameContinuity({db,clock:now})}:{}),
      admissions:scopedAdmissions,withAppAuthority:appAuthority,withHumanSubjectAuthority,signSourceAuthority,clock:now,
      onClose(record) {
        for(const [key,value] of backendSourceContexts)if(value.record===record)backendSourceContexts.delete(key);
        if(record.backendSource)for(const [key,session] of sessions)if(session.scopedRecord===record)sessions.delete(key);
        for(const [key,session] of bootCandidates)if(session.scopedRecord===record){bootCandidates.delete(key);if(session.promotion?.state==='pending')sessions.delete(session.sessionKey);}
        // Closing an old slot after a successful cutover must not retire its
        // successor. Expiry alone still permits the separately fenced v2
        // Source-anchor attempt; it never promotes a cookie by itself.
        if(record.context.expiresAt>now())for(const session of sessions.values())if(session.scopedRecord===record)retirePrimary(session);
        for(const [key,value]of scopedRenewals)if(value.record===record)scopedRenewals.delete(key);
        for(const stream of [...live.values()])if(stream.scopedRecord===record)closeStream(stream,'app_access_revoked');
        const channel=channels.get(connectorKey(record.profile.connector));
        if(channel?.bindingVersion===2)send(channel,{type:'scoped-context-closed',channelId:channel.channelId,appId:record.context.appId,reference:record.context.reference});
      },
    });
    const readEntryTransaction = createEngagementTransaction({ db, assertActor, withAuthorityFence, responseBytes: 32 * 1024,
      busyCode: 'apps_entry_busy', timeoutCode: 'apps_entry_timeout_invalid', responseCode: 'apps_entry_response_too_large' });
    entryRead = (actor, args) => {
      exact(args, ['appId', 'domainId', 'path', 'current', 'surface']);
      assertApps(args.current === undefined || args.current === true && args.domainId === undefined && args.path === undefined,
        'invalid_app_current_entry');
      assertApps(args.surface === undefined || args.surface === 'native' && args.current === true, 'invalid_app_current_entry');
      const requested = { appId: appId(args.appId), ...(args.domainId === undefined ? {} : { domainId: textId(args.domainId) }),
        ...(args.path === undefined ? {} : { path: guideLaunchPath(args.path).entryPath }) };
      return readEntryTransaction(actor, captured => {
        // An app-only locator follows the currently admitted entry. Exact
        // saved/deep links stay pinned; metadata never opens a scoped context.
        if (args.current === true || args.domainId === undefined && args.path === undefined) {
          const app = row(requested.appId), target = activeTarget(requested.appId);
          assertApps(app?.state === 'enabled' && target, 'app_unavailable', 404);
          if (args.surface === 'native') {
            const paired = readPairedNativeEntry(db, app.id);
            if (!paired) {
              // Absence is useful only to an actor who can already reach the
              // app. Do not turn private app IDs into a Native-pair oracle.
              if (!canUse(captured, app)) {
                const address = db.prepare(`SELECT d.id FROM app_domains d JOIN app_publication_domains p
                  ON p.app_id=d.app_id AND p.domain_id=d.id AND p.owner_account_id=d.owner_account_id
                  WHERE d.app_id=? AND d.role='alias' AND d.state='bound' ORDER BY d.created_at,d.id LIMIT 1`).get(app.id);
                assertApps(address && resolveEntry({ actor: captured, appId: app.id, domainId: address.id }), 'app_unavailable', 404);
              }
              return { entry: null };
            }
            const entry = requireNativeEntry({ db, appId: app.id, origin: paired.origin,
              primaryDigest: target.digest, nativeDigest: paired.target.digest, scopedAdmission: scopedAdmissions });
            const decision = publications.decideAccess({ domainId: entry.domainId, origin: entry.origin, actor: captured });
            assertApps(decision.policyEpoch === entry.policyEpoch && decision.targetRevision === target.revision
              && decision.targetDigest === target.digest, 'app_native_entry_closed', 403);
            assertRuntimeBinding(decision, { requireReady: true });
            // Public locator only: Native still authenticates its own person.
            // No Root ticket, cookie, identity witness or session is created.
            return { entry: { appId: app.id, domainId: entry.domainId, origin: entry.origin, path: runtimePath(entry.target.entryPath) } };
          }
          const preferred = defaultEntryFor(app, target, captured);
          const entry = resolveEntry({ actor: captured, appId: app.id, ...preferred });
          assertApps(entry, 'app_unavailable', 404);
          return { entry: { appId: entry.appId, domainId: entry.domainId, origin: entry.origin, path: entry.path } };
        }
        const guide = requested.path === '/_soty/guide';
        const entry = resolveEntry({ actor: captured, ...requested, ...(guide ? { path: undefined } : {}) });
        if (guide) assertApps(!selectedRuntimeProfile(activeTarget(requested.appId)?.profile), 'app_guide_unsupported', 404);
        assertApps(entry, 'app_unavailable', 404);
        return { entry: { appId: entry.appId, domainId: entry.domainId, origin: entry.origin, path: guide ? '/_soty/guide' : entry.path } };
      });
    };
    saved = createSavedRegistry({ db, now, assertActor, withAuthorityFence, resolveEntry });
    directory = createAppDirectory({ db, assertActor, withAuthorityFence, activeCommunityIds, currentContactIds, canUse, resolveEntry,
      invitationAppIds:accountId=>invitationAccess?.appIds(accountId)??[],
      resolveDefaultEntry({ actor, app }) {
        const target = activeTarget(app.id);
        if (!target || !selectedRuntimeProfile(target.profile)) return undefined;
        const preferred = inspectLaunchEntry({ app: { id: app.id, ownerAccountId: app.owner_account_id, state: app.state },
          target: { appId: app.id, revision: target.revision, digest: target.digest, profile: target.profile,
            connectorKey: target.connector_key, port: target.port, entryPath: target.entry_path } }, actor);
        return preferred ? resolveEntry({ actor, appId: app.id, ...preferred }) : null;
      } });
    // Read markers are optional metadata. Their failure must not prevent Apps
    // or Root from starting; inbox reports unknown, never a false zero unread.
    try { discussionReads = createDiscussionReadStore({ filename: databasePath === ':memory:' ? ':memory:' : `${databasePath}.reads.sqlite`, now }); }
    catch { discussionReads = undefined; }
    discussions = createDiscussionRegistry({ db, now, assertActor, withAuthorityFence, resolveEntry, canUse, isCurrentFriend,
      readStore: discussionReads,
      knownEntries(actor) {
        return { apps: appCandidates(actor, true).filter(app => app.state === 'enabled' && (canUse(actor, app) || app.owner_account_id === actor.accountId)).map(app => ({ appId: app.id })),
          saved: db.prepare('SELECT app_id,domain_id,path,updated_at FROM app_saved_entries WHERE account_id=? ORDER BY saved_revision DESC').all(actor.accountId)
            .map(entry => ({ appId: entry.app_id, domainId: entry.domain_id, path: entry.path, at: entry.updated_at })) };
      },
      readCommunityAuthority(actor, ownerAccountId, relevantCommunityIds) {
        assertApps(typeof readCommunityAuthority === 'function', 'apps_authority_fence_required', 503);
        return readCommunityAuthority(actor, ownerAccountId, relevantCommunityIds);
      },
      authorLabel: discussionAuthorLabel, limits: discussionLimits });
    if (coverDecoder !== undefined && COVER_APP_FORMATS[inspectAppsSchema(db)]) covers = createAppCoverAdapter({ db, assertActor, withAuthorityFence, resolveEntry,
      requireScopedTarget: target => scopedAdmissions.require(target), decoder: coverDecoder, clock: now, limits: coverLimits });
  } catch (error) { discussions?.close(); discussionReads?.close(); db.close(); throw error; }
  const wss = new WebSocketServer({ noServer: true, maxPayload: FRAME_BYTES, perMessageDeflate: false });
  let closed = false;
  const coverExtension = covers ? Object.freeze({ operations: new Set(appCoverOperations), async executeAsync({ op, args = {}, actor }) {
    assertApps(args && typeof args === 'object' && !Array.isArray(args) && Object.hasOwn(args, 'expectedAccountId'), 'apps_cover_account_required');
    const result = await covers.execute({ op, actor, args: authenticatedArgs(actor,args) });
    if (['apps.cover.commit','apps.cover.restore','apps.cover.remove'].includes(op)) notifyChange(actor,args.appId);
    return result;
  } }) : null;
  const row = id => db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  const publiclyListed = id => Boolean(typeof id==='string' && db.prepare("SELECT 1 FROM app_publications p JOIN local_apps a ON a.id=p.app_id WHERE a.id=? AND a.state='enabled' AND p.launch_policy='anyone' AND p.listed=1").get(id));
  function notifyChange(actor,id,wasPublic=false) {
    try { onChange({accountIds:[actor.accountId],...(id?{ref:{kind:'app',id}}:{}),publicTransition:wasPublic||publiclyListed(id)}); }
    catch { /* A failed observer cannot undo a committed app write. */ }
  }
  const binding = key => db.prepare('SELECT * FROM app_devices WHERE connector_key=?').get(key);
  const activeTarget = id => db.prepare(`SELECT t.* FROM app_publications p
    JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision AND t.owner_account_id=p.owner_account_id
    WHERE p.app_id=?`).get(id);
  const targetTuple = target => targetFromRow(db, target);
  const runtimeBindings = createRuntimeBindings({ channels, send, now, blockedPorts,
    scopedTarget(target, { candidate }) {
      assertApps(typeof withHumanSubjectAuthority === 'function', 'app_scoped_human_required', 503);
      if (candidate) scopedAdmissions.verifyCandidate(target); else scopedAdmissions.require(target);
      return true;
    },
    onBindingInvalidated(channel, id) {
      for (const stream of channel.streams.values()) if (stream.appId === id) closeStream(stream, 'app_source_changed');
    } });
  const preparedBrowse = createPreparedBrowse({ channels, send, secret, assertActor, resolveOwnedDevice, authenticateConnector, connectorCurrent, blockedPorts });
  const sources = createSourceRegistry({ db, now, assertActor, publications, blockedPorts,
    scopedAdmission: scopedAdmissions,
    endpointAdmission(target) {
      assertApps(allowSelfIpTargets, 'apps_endpoint_connector_upgrade_required', 503);
      const device = binding(target.connectorKey), channel = channels.get(target.connectorKey);
      assertApps(device?.owner_account_id === target.ownerAccountId, 'apps_device_not_owned', 403);
      endpointInventory(device, channel?.selfInventory && { channelId: channel.channelId, revision: channel.selfInventory.revision, digest: channel.selfInventory.digest }, target.endpoint);
    },
    prepareTarget: runtimeBindings.prepareTarget, verifyPreparedTarget: runtimeBindings.verifyPreparedTarget,
    onChanged(event) {
      invalidateAccess({ appId: event.appId });
      // A replayed receipt can describe an older transition. Re-read the actual
      // active route and remove any previously desired binding of this app.
      const keys = new Set([event.oldConnectorKey, event.newConnectorKey, activeTarget(event.appId)?.connector_key]);
      for (const channel of channels.values()) {
        if (keys.has(channel.key) || (channel.bindingVersion === 2 && runtimeBindings.getState(channel, event.appId).state !== 'unavailable')) sync(channel);
      }
    } });
  const sourcePreparationExtension = Object.freeze({ operations: new Set(['apps.source.prepare','apps.scoped.context','apps.scoped.renew','apps.prepared.browse']),
    async executeAsync({ op, args = {}, actor }) {
      if (op === 'apps.prepared.browse') return preparedBrowse.browse(actor, authenticatedArgs(actor, args));
      if(op==='apps.scoped.renew'){
        args=authenticatedArgs(actor,args);exact(args,['appId','handle','requestId','shortSessionHandoff','frameId','currentEntryPath']);const id=appId(args.appId);
        const currentEntryPath=args.currentEntryPath===undefined?undefined:createScopedLaunchPath(args.currentEntryPath).entryPath;
        assertApps(args.frameId===undefined||typeof args.frameId==='string'&&/^[A-Za-z0-9_-]{43}$/.test(args.frameId),'app_scoped_renew_invalid',403);
        assertApps(args.shortSessionHandoff===undefined||args.shortSessionHandoff===true,'app_scoped_renew_invalid',403);
        assertApps(scopedGateway&&typeof args.handle==='string'&&/^[A-Za-z0-9_-]{43}$/.test(args.handle)
          &&typeof args.requestId==='string'&&/^[A-Za-z0-9_-]{16,128}$/.test(args.requestId),'app_scoped_renew_invalid',403);
        for(const [key,value]of scopedRenewals)if(value.record.context.expiresAt<=now())scopedRenewals.delete(key);
        const key=digest(id+'\0'+args.requestId),intent=digest(JSON.stringify([args.handle,args.shortSessionHandoff===true,args.frameId??null,currentEntryPath??null])),prior=scopedRenewals.get(key);
        assertActor(actor);
        let issued;
        if(prior){assertApps(prior.intent===intent&&prior.accountId===actor.accountId&&prior.deviceId===actor.deviceId,'app_scoped_renew_conflict',409);
          scopedGateway.context(prior.record);issued=prior;
        }else{
          assertApps(scopedRenewals.size<256,'app_scoped_renew_busy',429);
          let attempt,recovery;
          try{attempt=scopedGateway.ownedAttempt(actor,id,args.handle);}catch(error){
            if(error?.code!=='app_scoped_context_closed'||!args.frameId||args.shortSessionHandoff)throw error;
            recovery=scopedGateway.recoverFrame(actor,id,args.frameId,args.handle);
            assertApps(currentEntryPath!==undefined&&scopedHash(currentEntryPath)===recovery.scope.entry.pathDigest,'app_scoped_context_closed',403);
          }
          const {record:old,context:first,sourceOnly,shortSession,rootInstanceId}=attempt??{rootInstanceId:recovery.scope.rootInstanceId??undefined},instanceId=rootInstanceId??first?.instanceId;
          assertApps(!args.shortSessionHandoff||shortSession===true,'app_scoped_short_session_required',403);
          if(!recovery&&!sourceOnly){assertApps(old?.session&&sessions.get(old.session.sessionKey)===old.session,'app_scoped_context_closed',403);
            const oldDecision=publications.recheckAccess(old.session.decision);assertRuntimeBinding(oldDecision,{requireReady:true});checkWidgetRuntimeFor({...old.session,decision:oldDecision},'renew');}
          await Promise.resolve();assertActor(actor);
          if(recovery){const fresh=scopedGateway.recoverFrame(actor,id,args.frameId,args.handle);assertApps(fresh.revision===recovery.revision,'app_scoped_context_closed',403);}
          else{const {record,context:fresh,shortSession:freshShort,rootInstanceId:freshRootInstanceId}=scopedGateway.ownedAttempt(actor,id,args.handle);
            assertApps(record===old&&rootInstanceId===freshRootInstanceId&&JSON.stringify(first)===JSON.stringify(fresh)&&(!args.shortSessionHandoff||freshShort===true),'app_scoped_context_closed',403);}
          const priorEntry=recovery?.scope.entry??first.entry;
          const domain=db.prepare('SELECT * FROM app_domains WHERE app_id=? AND id=?').get(id,priorEntry.domainId);
          assertApps(domain&&domain.origin===priorEntry.origin,'app_scoped_context_closed',403);
          const decision=publications.decideAccess({domainId:domain.id,origin:domain.origin,actor});assertRuntimeBinding(decision,{requireReady:true});
          if(instanceId?.startsWith('widget-')&&checkWidgetLaunch)synchronous(checkWidgetLaunch({phase:'renew',actor,widgetId:instanceId,appId:id,
            entry:{domainId:domain.id},target:{revision:decision.targetRevision,digest:decision.targetDigest,profile:decision.profile}}),'apps_async_authority');
          checkWidgetRuntimeFor({decision,entryPath:priorEntry.path,rootInstanceId},'renew');
          const previousSession=old?.session??[...sessions.values()].find(value=>value.scopedRecord?.context.reference.id===first?.reference.id);
          const entryPath=createScopedLaunchPath(recovery?currentEntryPath:previousSession?.entryPath??currentEntryPath??'/embed').entryPath;
          if(currentEntryPath!==undefined)assertApps(currentEntryPath===entryPath,'app_scoped_context_closed',403);
          const scoped=scopedGateway.open({actor,appId:id,domainId:domain.id,target:targetTuple(activeTarget(id)),instanceId}),next=scoped.record.context;
          voiceSourceActors.set(scoped.record,actor);
          const same=recovery?isRootSignedWorkspaceSource(next.sourceProfile):args.shortSessionHandoff===true?scopedRenewalBindingDigest(next)===scopedRenewalBindingDigest(first)
            :next.instanceId===first.instanceId&&(sourceOnly?next.appId===first.appId&&decision.profile==='soty.selected-human-embed.v2'
            &&['rootPrincipal','humanPrincipal','sourceProfile','resource'].every(field=>JSON.stringify(next[field])===JSON.stringify(first[field]))
            :['profileDigest','appId','policyEpoch'].every(field=>next[field]===first[field])
              &&['rootPrincipal','humanPrincipal','sourceProfile','resource','target','entry'].every(field=>JSON.stringify(next[field])===JSON.stringify(first[field])));
          if(!same){scopedGateway.invalidate(scoped.record);assertApps(false,'app_scoped_context_closed',403);}
          const frameId=recovery?.frameId??(isRootSignedWorkspaceSource(first.sourceProfile)?previousSession?.family?.frameId:undefined);
          if(isRootSignedWorkspaceSource(next.sourceProfile)&&!frameId){scopedGateway.invalidate(scoped.record);assertApps(false,'app_scoped_context_closed',403);}
          try{if(frameId&&scopedGateway.frameId(scoped.record)){
            assertApps(args.frameId===undefined||args.frameId===frameId,'app_scoped_context_closed',403);scopedGateway.prepareFrame(scoped.record,frameId,recovery,entryPath);
          }}catch(error){scopedGateway.invalidate(scoped.record);throw error;}
          issued={record:scoped.record,handle:scoped.closeHandle,decision,intent,accountId:actor.accountId,deviceId:actor.deviceId,frameId,
            entryPath,
            ...(args.shortSessionHandoff?{bindingDigest:scopedRenewalBindingDigest(first)}:{})};scopedRenewals.set(key,issued);
        }
        assertActor(actor);scopedGateway.context(issued.record);const decision=publications.recheckAccess(issued.decision);assertRuntimeBinding(decision,{requireReady:true});
        const issuedInstanceId=issued.record.rootInstanceId??issued.record.context.instanceId;
        if(issuedInstanceId?.startsWith('widget-')&&checkWidgetLaunch)synchronous(checkWidgetLaunch({phase:'renew',actor,widgetId:issuedInstanceId,appId:id,
          entry:{domainId:decision.domainId},target:{revision:decision.targetRevision,digest:decision.targetDigest,profile:decision.profile}}),'apps_async_authority');
        checkWidgetRuntimeFor({decision,entryPath:issued.entryPath,scopedRecord:issued.record},'renew');
        const {entryPath,bootPath}=createScopedLaunchPath(issued.entryPath);
        assertApps(tickets.size<4096,'apps_launch_busy',429);const ticket=secret();tickets.set(digest(ticket),{decision,entryPath,scopedRecord:issued.record,rebind:true,renewalRequestId:args.requestId,frameId:issued.frameId,frameRecovery:scopedGateway.isFrameRecovery(issued.record)});
        return{launchUrl:`${decision.origin}${bootPath}&mode=renew#${ticket}`,expiresAt:decision.expiresAt,
          runtimeProfile:decision.profile,scopedCloseHandle:issued.handle,scopedSlotExpiresAt:issued.record.context.expiresAt,scopedSource:{...issued.record.context.sourceProfile},
          scopedRenewalRequestId:args.requestId,...(issued.bindingDigest?{scopedRenewalBindingDigest:issued.bindingDigest}:{}),
          ...(scopedGateway.frameId(issued.record)?{scopedFrameId:issued.frameId}:{}),
          entry:{appId:id,domainId:decision.domainId,origin:decision.origin,path:entryPath}};
      }
      if(op==='apps.scoped.context') {
        args=authenticatedArgs(actor,args);exact(args,['appId','handle']);const id=appId(args.appId);
        assertApps(scopedGateway,'app_scoped_context_closed',403);
        const read=()=>{
          assertActor(actor);const {record,context}=scopedGateway.ownedContext(actor,id,args.handle);
          assertApps(record.session&&sessions.get(record.session.sessionKey)===record.session,'app_scoped_context_closed',403);
          const decision=publications.recheckAccess(record.session.decision);assertRuntimeBinding(decision,{requireReady:true});
          const sourceSession=scopedGateway.sourceContext(record);
          return {ready:true,appId:id,scopedSource:{...context.sourceProfile},target:{...context.target},expiresAt:context.expiresAt,
            ...(record.sourceSession?.shortSession===true?{renewalBindingDigest:scopedRenewalBindingDigest(context)}:{}),
            ...(sourceSession?{sourceSession}:{} )};
        };
        // This deferred signed operation never joins the old slot actor to the
        // new dispatch transaction. Original authority is freshly fenced twice.
        const first=read();await Promise.resolve();const fresh=read();
        assertApps(JSON.stringify(first)===JSON.stringify(fresh),'app_scoped_context_closed',403);return fresh;
      }
      assertApps(op === 'apps.source.prepare', 'unsupported_operation');
      args = authenticatedArgs(actor, args);
      return sources.execute({ op, actor, args });
    } });
  function bindingFloor(id) {
    return requiredBindingVersion(db, id);
  }
  function assertRuntimeBinding(decision, { requireReady = false } = {}) {
    const channel = channels.get(decision.route.connectorKey), floor = bindingFloor(decision.appId);
    // Temporary disconnection does not revoke an otherwise current account
    // session. A known legacy channel can never serve a source with floor2.
    if (channel && floor === 2) assertApps(channel.bindingVersion === 2, 'app_source_protocol_required', 503);
    if (!requireReady) return null;
    assertApps(channel && channel.ws.readyState === 1, 'app_offline', 503);
    return channel.bindingVersion === 2 ? runtimeBindings.requireBinding(channel, decision) : null;
  }
  function authenticatedArgs(actor, args) {
    assertApps(!closed, 'apps_closed', 503); assertActor(actor);
    assertApps(args && typeof args === 'object' && !Array.isArray(args), 'invalid_arguments');
    if (Object.hasOwn(args, 'expectedAccountId')) {
      assertApps(args.expectedAccountId === actor.accountId, 'authentication_required', 401);
      const { expectedAccountId: _expectedAccountId, ...operationArgs } = args;
      return operationArgs;
    }
    return args;
  }
  function assertActor(actor) {
    assertApps(actor && textId(actor.accountId) && textId(actor.deviceId) && actorActive(actor) === true, 'apps_authentication_required', 401);
  }
  function canUse(actor, app) {
    if (!app || app.state !== 'enabled' || actorActive(actor) !== true) return false;
    if (app.owner_account_id === actor.accountId) return true;
    if(invitationAccess?.hasAccess({appId:app.id,ownerId:app.owner_account_id,accountId:actor.accountId})===true)return true;
    const audience = readAppAudience(db, app.id);
    if (audience !== null) return audience === 'friends' && isCurrentFriend(actor, app.owner_account_id);
    const grants = JSON.parse(app.grants_json);
    return grants.accountIds.includes(actor.accountId) || grants.communityIds.some(id => canAccessCommunity(actor.accountId, id) === true && isGroupAdmin(app.owner_account_id, id) === true);
  }
  function assertGrants(actor, grants) {
    assertApps(grants.communityIds.every(id => isGroupAdmin(actor.accountId, id) === true), 'apps_community_admin_required', 403);
  }
  function persistGrants(id, grants) {
    db.prepare('DELETE FROM local_app_grants WHERE app_id=?').run(id);
    const insert = db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)');
    for (const principal of grants.accountIds) insert.run(id, 'account', principal);
    for (const principal of grants.communityIds) insert.run(id, 'community', principal);
  }
  function transaction(callback) { db.exec('BEGIN IMMEDIATE'); try { const value = callback(); db.exec('COMMIT'); return value; } catch (error) { db.exec('ROLLBACK'); throw error; } }
  function resolveOwnedDevice(actor, hostDeviceId, connectorId) {
    assertActor(actor); textId(hostDeviceId); if (connectorId !== undefined) textId(connectorId);
    const matches = db.prepare('SELECT * FROM app_devices WHERE owner_account_id=?').all(actor.accountId).map(item => JSON.parse(item.identity_json))
      .filter(item => item.hostDeviceId === hostDeviceId && (connectorId === undefined || item.connectorId === connectorId));
    assertApps(matches.length > 0, 'apps_device_not_owned', 403);
    // A user companion is preferable for OpenCode; caller can select a specific
    // connector when it needs machine-scope terminal capabilities instead.
    return matches.find(item => !item.connectorId.endsWith(':machine')) || matches[0];
  }
  /** Host-only negative ownership projection for explicitly pinned legacy
   * service credentials. This grants neither an account nor app authority. */
  function isUnownedConnector(identity) {
    assertApps(!closed, 'apps_closed', 503);
    return !db.prepare('SELECT 1 FROM app_devices WHERE connector_key=?').get(connectorKey(identity));
  }
  function publicCatalog() {
    assertApps(!closed, 'apps_closed', 503);
    const rows = db.prepare(`SELECT a.* FROM local_apps a JOIN app_publications p
      ON p.app_id=a.id AND p.owner_account_id=a.owner_account_id
      WHERE a.state='enabled' AND p.launch_policy='anyone' AND p.listed=1
      ORDER BY a.created_at DESC,a.id LIMIT 100`).all();
    const apps = [];
    for (const app of rows) {
      const address = db.prepare(`SELECT ad.id,ad.origin FROM app_publication_domains pd
        JOIN app_domains ad ON ad.id=pd.domain_id AND ad.app_id=pd.app_id AND ad.owner_account_id=pd.owner_account_id
        JOIN app_domain_zones z ON z.id=ad.zone_id AND z.kind='named'
        WHERE pd.app_id=? AND pd.owner_account_id=? AND ad.role='alias' AND ad.state='bound'
        ORDER BY ad.created_at,ad.id LIMIT 1`).get(app.id, app.owner_account_id);
      if (!address) continue;
      let decision; try { decision = publications.decideAccess({ domainId: address.id, origin: address.origin }); } catch { continue; }
      if (decision.subject !== 'public') continue;
      const row = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=? AND owner_account_id=?')
        .get(app.id, decision.targetRevision, app.owner_account_id);
      if (!row || row.digest !== decision.targetDigest) continue;
      const target = { connectorKey: row.connector_key, revision: row.revision, digest: row.digest, entryPath: row.entry_path };
      const observed = inspectSource({ app, target });
      apps.push({ id: app.id, name: app.name, ownerAccountId: app.owner_account_id,
        state: ({ offline: 'offline', unknown: 'starting', responding: 'ready', unreachable: 'stopped' })[observed.state],
        publication: { launchPolicy: 'anyone', activeNamedAddressCount: 1,
          ...(hasAudienceSchema(db) ? { audience: readAppAudience(db, app.id), listed: true } : {}) },
        entry: { domainId: address.id, origin: address.origin, path: target.entryPath } });
    }
    return { schema: 'soty.app-catalog.v1', apps };
  }
  function appCandidates(actor, discussionsOnly = false) {
    const audienceCandidates = hasAudienceSchema(db);
    const discussion = discussionsOnly ? ` AND EXISTS(SELECT 1 FROM app_discussion_heads h
      JOIN app_discussion_conversations c ON c.id=h.current_id WHERE h.app_id=local_apps.id AND c.message_seq>0)` : '';
    return typeof activeCommunityIds === 'function' ? db.prepare(`SELECT * FROM local_apps WHERE id IN (
        SELECT id FROM local_apps WHERE owner_account_id=? UNION SELECT value FROM json_each(?) UNION
      SELECT app_id FROM local_app_grants WHERE kind='account' AND principal_id=?
        ${audienceCandidates ? 'AND NOT EXISTS(SELECT 1 FROM app_audiences mode WHERE mode.app_id=local_app_grants.app_id)' : ''} UNION
      SELECT app_id FROM local_app_grants WHERE kind='community' AND principal_id IN (SELECT value FROM json_each(?))
        ${audienceCandidates ? 'AND NOT EXISTS(SELECT 1 FROM app_audiences mode WHERE mode.app_id=local_app_grants.app_id)' : ''}
      ${audienceCandidates ? `UNION SELECT a.id FROM local_apps a JOIN app_audiences mode ON mode.app_id=a.id
        WHERE mode.audience='friends' AND a.owner_account_id IN(SELECT value FROM json_each(?))` : ''})${discussion} ORDER BY created_at`)
        .all(actor.accountId,JSON.stringify(invitationAccess?.appIds(actor.accountId)??[]), actor.accountId, JSON.stringify(activeCommunityIds(actor.accountId)),
        ...(audienceCandidates ? [JSON.stringify(currentContactIds(actor))] : [])) : db.prepare(`SELECT * FROM local_apps WHERE 1=1${discussion} ORDER BY created_at`).all();
  }
  function publicApp(app, actor) {
    // One statement observes the current app, target and publication together.
    // A candidates row can predate another writer's revoke or source change.
    const current = db.prepare(`SELECT a.*,t.connector_key AS target_connector_key,t.revision AS target_revision,
      t.digest AS target_digest,t.port AS target_port,t.entry_path AS target_entry_path,t.profile AS target_profile,
      d.identity_json,d.name AS device_name,p.launch_policy,p.listed,
      CASE WHEN a.state='enabled' THEN (SELECT count(*) FROM app_publication_domains pd
        JOIN app_domains ad ON ad.id=pd.domain_id AND ad.app_id=pd.app_id AND ad.owner_account_id=pd.owner_account_id
        JOIN app_domain_zones z ON z.id=ad.zone_id AND z.kind='named'
        WHERE pd.app_id=a.id AND pd.owner_account_id=a.owner_account_id AND ad.role='alias' AND ad.state='bound')
        ELSE 0 END AS active_named_address_count
      FROM local_apps a JOIN app_publications p ON p.app_id=a.id AND p.owner_account_id=a.owner_account_id
      JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision AND t.owner_account_id=p.owner_account_id
      JOIN app_devices d ON d.connector_key=t.connector_key AND d.owner_account_id=a.owner_account_id
      WHERE a.id=?`).get(app.id);
    assertApps(current, 'apps_registry_corrupt', 500);
    assertApps(current.owner_account_id === actor.accountId || canUse(actor, current), 'apps_access_denied', 403);
    const identity = JSON.parse(current.identity_json);
    const observed = inspectSource({ app: current, target: { connectorKey: current.target_connector_key, revision: current.target_revision, digest: current.target_digest } });
    const legacyState = { offline: 'offline', unknown: 'starting', responding: 'ready', unreachable: 'stopped' };
    const state = current.state === 'revoked' ? 'revoked' : legacyState[observed.state];
    let entry;
    if (current.state === 'enabled' && actor.accountId === current.owner_account_id && selectedRuntimeProfile(current.target_profile)) {
      entry = inspectLaunchEntry({ app: { id: current.id, ownerAccountId: current.owner_account_id, state: current.state },
        target: { appId: current.id, revision: current.target_revision, digest: current.target_digest, profile: current.target_profile,
          connectorKey: current.target_connector_key, port: current.target_port, entryPath: current.target_entry_path } }, actor);
    } else if (current.state === 'enabled' && actor.accountId === current.owner_account_id && current.active_named_address_count > 0) {
      const address = db.prepare('SELECT ad.id,ad.origin FROM app_publication_domains pd JOIN app_domains ad '
        + 'ON ad.id=pd.domain_id AND ad.app_id=pd.app_id AND ad.owner_account_id=pd.owner_account_id '
        + "JOIN app_domain_zones z ON z.id=ad.zone_id AND z.kind='named' "
        + "WHERE pd.app_id=? AND pd.owner_account_id=? AND ad.role='alias' AND ad.state='bound' ORDER BY ad.created_at,ad.id LIMIT 1")
        .get(current.id, current.owner_account_id);
      if (address) {
        try { entry = { domainId: address.id, origin: address.origin, path: runtimePath(current.target_entry_path) }; }
        catch { /* Preserve inspection of a legacy registration with an invalid path. */ }
      }
    }
    return { id: current.id, name: current.name, ownerAccountId: current.owner_account_id, hostDeviceId: identity.hostDeviceId,
      state, createdAt: current.created_at, updatedAt: current.updated_at,
      ...(actor.accountId === current.owner_account_id ? { connectorId: identity.connectorId, deviceName: current.device_name,
        port: current.target_port, entryPath: current.target_entry_path, grants: JSON.parse(current.grants_json),
        ...(current.target_profile === SELF_IP_PROFILE ? { endpoint: readRuntimeEndpoint(db, { app_id: current.id, revision: current.target_revision, digest: current.target_digest, profile: current.target_profile }) } : {}),
        publication: { launchPolicy: current.launch_policy, activeNamedAddressCount: current.active_named_address_count,
          ...(hasAudienceSchema(db) ? { audience: readAppAudience(db, current.id), listed: Boolean(current.listed) } : {}) },
        ...(entry ? { entry } : {}) } : {}) };
  }
  // Navigation metadata only. Launch/instance creation still recheck the exact
  // requested domain, current publication, admission and Source origin fence.
  function inspectLaunchEntry({ app, target }, actor) {
    if (app.state !== 'enabled' || !selectedRuntimeProfile(target.profile)) return null;
    let profile;
    try { profile = scopedAdmissions.require({ ...target, ownerAccountId: app.ownerAccountId }); }
    catch (error) {
      if (['app_scoped_admission_required', 'app_scoped_admission_mismatch', 'app_scoped_migration_required'].includes(error.code)) return null;
      throw error;
    }
    const domain = db.prepare(`SELECT ad.id,ad.origin FROM app_domains ad
      WHERE ad.app_id=? AND ad.owner_account_id=? AND ad.origin=? AND ad.state='bound'
      AND (ad.role='canonical' OR EXISTS(SELECT 1 FROM app_publication_domains pd
        WHERE pd.app_id=ad.app_id AND pd.owner_account_id=ad.owner_account_id AND pd.domain_id=ad.id))`).get(app.id, app.ownerAccountId, profile.embedOrigin);
    if (!domain) return null;
    let decision;
    try { decision = publications.decideAccess({ domainId: domain.id, origin: domain.origin, actor }); }
    catch (error) { if (error instanceof AppsError && error.code === 'apps_access_denied') return null; throw error; }
    if (decision.appId !== app.id || decision.targetRevision !== target.revision || decision.targetDigest !== target.digest
      || decision.profile !== target.profile || decision.route.connectorKey !== target.connectorKey
      || decision.route.port !== target.port || decision.route.entryPath !== target.entryPath) return null;
    return { domainId: domain.id, origin: domain.origin, path: target.entryPath };
  }
  function defaultEntryFor(app, target, actor) {
    assertApps(target, 'app_unavailable', 404);
    let preferred = selectedRuntimeProfile(target.profile) ? inspectLaunchEntry({
      app: { id: app.id, ownerAccountId: app.owner_account_id, state: app.state },
      target: { appId: app.id, revision: target.revision, digest: target.digest, profile: target.profile,
        connectorKey: target.connector_key, port: target.port, entryPath: target.entry_path },
    }, actor) : undefined;
    assertApps(!selectedRuntimeProfile(target.profile) || preferred, 'app_unavailable', 404);
    if (!selectedRuntimeProfile(target.profile)) {
      const address = db.prepare(`SELECT d.id FROM app_publication_domains pd
        JOIN app_domains d ON d.id=pd.domain_id AND d.app_id=pd.app_id AND d.owner_account_id=pd.owner_account_id
        JOIN app_domain_zones z ON z.id=d.zone_id AND z.kind='named'
        WHERE pd.app_id=? AND pd.owner_account_id=? AND d.role='alias' AND d.state='bound'
        ORDER BY d.created_at,d.id LIMIT 1`).get(app.id, app.owner_account_id);
      if (address) preferred = { domainId: address.id };
    }
    return preferred;
  }
  function inspectSource({ app, target }) {
    if (app.state !== 'enabled') return describeSourceObservation({ connected: true, now: now() });
    const channel = channels.get(target.connectorKey);
    const candidate = channel && (channel.bindingVersion === 2 || bindingFloor(app.id) === 1) ? channel.observations.get(app.id) : undefined;
    const observed = candidate?.targetRevision === target.revision && candidate?.targetDigest === target.digest ? candidate : undefined;
    return describeSourceObservation({ connected: Boolean(channel), observed, now: now() });
  }
  function inspectBinding({ app, target, requiredBindingVersion: floor }) {
    const channel = channels.get(target.connectorKey);
    if (!channel || channel.ws.readyState !== 1) return { state: 'offline' };
    if (channel.bindingVersion !== 2) return { state: floor === 2 ? 'update-required' : 'legacy' };
    return { state: runtimeBindings.getState(channel, app.id, target).state };
  }
  function inspectIntegrations({ app, target }) {
    const channel = channels.get(target.connectorKey);
    if (app.state !== 'enabled' || !withHumanSubjectAuthority || channel?.ws.readyState !== 1 || channel.bindingVersion !== 2) return [];
    const next = db.prepare('SELECT max(revision) AS revision FROM app_runtime_targets WHERE app_id=?').get(app.id).revision + 1;
    if (!Number.isSafeInteger(next)) return [];
    const result = [];
    for (const profile of scopedAdmissions.profiles()) {
      if (profile.appId !== app.id || profile.target.revision !== next || !channel.runtimeProfiles.includes(profile.schema)) continue;
      // The approved next target can use a different port from the current
      // relay. Its digest is an installed pin, not a claim about the old port.
      const candidate = { appId: app.id, ownerAccountId: app.ownerAccountId, revision: next, digest: profile.target.digest,
        connectorKey: target.connectorKey, entryPath: '/embed', profile: profile.schema };
      try { scopedAdmissions.verifyCandidate(candidate); }
      catch { continue; }
      // An installed option is a hint. The existing prepare still probes the
      // exact connector and rechecks owner, admission and current revisions.
      result.push({ profile: profile.schema, entryPath: '/embed' });
    }
    return result;
  }
  function deviceProjection(item) {
    const identity = JSON.parse(item.identity_json), channel = channels.get(item.connector_key);
    const online = channel?.ws.readyState === 1;
    return { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: item.name, online, claimed: true,
      bindingVersion: online ? channel.bindingVersion : null,
      ...(allowSelfIpTargets ? { sourceEndpoints: { schema: 'soty.connector-source-endpoints.v1',
        state: !online ? 'offline' : channel.bindingVersion !== 2 || !channel.runtimeProfiles.includes(SELF_IP_PROFILE) ? 'upgrade-required'
          : !channel.selfInventory || now() - channel.selfInventoryReceivedAt > 15_000 ? 'pending' : 'ready',
        ...(online && channel.selfInventory && now() - channel.selfInventoryReceivedAt <= 15_000 ? {
          inventoryWitness: { channelId: channel.channelId, revision: channel.selfInventory.revision, digest: channel.selfInventory.digest },
          addresses: channel.selfInventory.addresses } : {}) } } : {}) };
  }
  function closeStream(stream, error = 'app_stream_closed', notify = true) {
    if (stream.closed) return;
    stream.closed = true; clearTimeout(stream.timer);
    stream.relay?.close();
    stream.upgradeHead = null;
    if (live.get(stream.id) === stream) live.delete(stream.id);
    if (stream.channel.streams.get(stream.id) === stream) stream.channel.streams.delete(stream.id);
    for (const pending of stream.pending.values()) { clearTimeout(pending.timer); pending.reject(new AppsError(error, 502)); }
    stream.pending.clear();
    if (notify) send(stream.channel, { type: 'cancel', id: stream.id, error });
    if (stream.kind === 'ws') { if (error === 'app_stream_complete') stream.socket.end(); else stream.socket.destroy(); }
    else if (!stream.res.writableEnded) {
      if (!stream.res.headersSent) respondAppFailure(stream.req, stream.res, error === 'app_access_revoked' ? 403
        : ['app_offline', 'app_identity_unavailable'].includes(error) ? 503 : 502, error, false);
      else stream.res.destroy();
    }
  }
  function checkAccess(holder, { renewPublic = false } = {}) {
    // Refresh only an unexpired anonymous lease. Account sessions, including
    // signed public visitors, keep their original absolute deadline.
    holder.decision = publications.recheckAccess(holder.decision,
      renewPublic && holder.decision.subject === 'public' ? { ttlMs: publicLeaseMs } : {});
    checkWidgetRuntimeFor(holder, 'request');
    assertRuntimeBinding(holder.decision);
    return holder.decision;
  }
  function rebindPlainHostStream(stream) {
    const previous=stream.session,claim=previous.hostSession;
    if(!claim||previous.windowKey||stream.scopedRecord)return null;
    // Native cookie requests identify the exact logical family, not a physical
    // document. Another checked member of that same family can retain the
    // existing transport; explicit member-proof grants never take this path.
    for(const candidate of sessions.values()){
      if(candidate.hostCompatibility||candidate.hostChecked!==true||!candidate.hostSession
        ||candidate.hostSession.frameId===claim.frameId||candidate.hostSession.family!==claim.family
        ||candidate.runtimeHost?.origin!==previous.runtimeHost?.origin)continue;
      try{
        hostSessions.assertClaim(candidate.hostSession);assertApps(!hostSessions.admitting(candidate.hostSession),'app_host_session_unconfirmed',409);
        assertApps(scopedHash(hostScope(previous))===claim.family&&scopedHash(hostScope(candidate))===claim.family,'app_host_session_changed',403);
        const decision=checkAccess(candidate);checkWidgetRuntimeFor(candidate,stream.kind==='ws'?'ws':'stream');
        assertApps(channels.get(stream.channel.key)===stream.channel&&stream.channel.ws.readyState===1
          &&decision.route.connectorKey===stream.channel.key&&assertRuntimeBinding(decision,{requireReady:true})===stream.runtimeBinding,'app_host_session_changed',403);
        const{hostRenewal:_oldRenewal,...retained}=previous;
        stream.session={...retained,decision,sessionKey:candidate.sessionKey,hostSession:candidate.hostSession,
          ...(candidate.hostRenewal?{hostRenewal:candidate.hostRenewal}:{})};
        return decision;
      }catch{/* A stale candidate or another scope never authorizes fallback. */}
    }
    return null;
  }
  function checkStream(stream) {
    assertApps(!stream.closed, 'app_stream_closed', 502);
    try {
      if(stream.scopedRecord)scopedGateway.context(stream.scopedRecord);
      const decision = checkAccess(stream.session, { renewPublic: true });
      checkWidgetRuntimeFor(stream.session, stream.kind === 'ws' ? 'ws' : 'stream');
      assertApps(channels.get(stream.channel.key) === stream.channel && stream.channel.ws.readyState === 1, 'app_offline', 503);
      if (stream.channel.bindingVersion === 2) {
        runtimeBindings.assertBindingCurrent(stream.channel, stream.runtimeBinding);
        assertApps(runtimeBindings.requireBinding(stream.channel, decision) === stream.runtimeBinding, 'app_binding_pending', 503);
      }
      return decision;
    }
    catch (error) {const retained=rebindPlainHostStream(stream);if(retained)return retained;closeStream(stream, 'app_access_revoked'); throw error; }
  }
  function retainAccess(holder) {
    // Expiry, rights, epoch and persistent binding floor are authority. A
    // temporarily connected older runtime is only transport unavailability;
    // it must not destroy this session before a compatible device returns.
    try { holder.decision = publications.recheckAccess(holder.decision); checkWidgetRuntimeFor(holder,'renew'); return true; } catch { return false; }
  }
  function rootWidgetId(holder) { return holder.rootInstanceId ?? holder.scopedRecord?.rootInstanceId; }
  function runtimeWidget(widgetId) {
    assertApps(typeof resolveWidgetRuntime === 'function', 'app_widget_runtime_changed', 403);
    const widget = synchronous(resolveWidgetRuntime(widgetId), 'apps_async_authority');
    assertApps(widget?.widgetId === widgetId && widget.state === 'active', 'app_widget_runtime_changed', 403);
    return widget;
  }
  function widgetDomain(widget) {
    return runtimeDomain(widget.entry?.domainId,widget.appId);
  }
  function runtimeDomain(domainId,appId) {
    const domain = db.prepare(`SELECT d.*,z.suffix,z.scheme,z.port FROM app_domains d
      JOIN app_domain_zones z ON z.id=d.zone_id WHERE d.id=? AND d.app_id=? AND d.state='bound'`).get(domainId,appId);
    assertApps(domain && domain.owner_account_id === row(appId)?.owner_account_id, 'app_widget_runtime_changed', 403);
    const url = new URL(domain.origin);
    assertApps(url.hostname.slice(url.hostname.indexOf('.') + 1) === domain.suffix && url.protocol === domain.scheme + ':'
      && url.port === domain.port, 'app_widget_runtime_zone_unsupported', 503);
    return domain;
  }
  const runtimeZone = domain => `${domain.scheme}://${domain.suffix}${domain.port ? ':' + domain.port : ''}`;
  function resolveRuntimeHost(authority, { tls = false } = {}) {
    const widgetLocator=parseWidgetRuntimeHost(authority.hostname),mainLocator=parseMainRuntimeHost(authority.hostname),locator=widgetLocator??mainLocator;
    if(!locator||widgetLocator&&!resolveWidgetRuntime)return null;
    try {
      const widget=widgetLocator?runtimeWidget(locator.widgetId):null,id=widget?.appId??locator.appId,app=row(id);
      if(app?.state!=='enabled'||selectedRuntimeProfile(activeTarget(id)?.profile))return null;
      let domain;
      if(widget)domain=widgetDomain(widget);
      else{
        const issued=[...tickets.values(),...sessions.values(),...relayWindows.values()].find(holder=>holder.runtimeHost?.scope.schema==='soty.main-runtime-scope.v1'
          &&holder.decision.appId===id&&new URL(holder.runtimeHost.origin).hostname===authority.hostname);
        if(issued)domain=runtimeDomain(issued.decision.domainId,id);
        else{
          const candidates=db.prepare("SELECT d.*,z.suffix,z.scheme,z.port FROM app_domains d JOIN app_domain_zones z ON z.id=d.zone_id WHERE d.app_id=? AND d.state='bound' ORDER BY d.role='canonical' DESC").all(id);
          const candidate=candidates.find(value=>value.suffix===locator.suffix&&new URL(value.origin).hostname.split('.').slice(1).join('.')===value.suffix);
          if(!candidate)return null;domain=runtimeDomain(candidate.id,id);
        }
      }
      const origin=widget?widgetRuntimeOrigin(locator.widgetId,locator.scopeHash,runtimeZone(domain)):mainRuntimeOrigin(id,locator.scopeHash,runtimeZone(domain));
      if (new URL(origin).hostname !== authority.hostname) return null;
      if (tls) {
        if (!origin.startsWith('https://')) return null;
        let issued = false;
        // TLS admits only a current already-issued host, not every possible hash
        // for a public widget ID. Existing bounded holders are the sole index.
        for (const collection of [tickets, sessions, relayWindows]) {
          for (const holder of collection.values()) if (holder.runtimeHost?.origin === origin && holder.decision.expiresAt > now()
            &&(holder.hostTicketExpiresAt===undefined||holder.hostTicketExpiresAt>now())) {
            try {
              const decision = publications.recheckAccess(holder.decision);
              checkWidgetRuntimeFor({ ...holder, decision }, 'tls'); issued = true; break;
            } catch { /* A stale/locked issued holder cannot admit a certificate. */ }
          }
          if (issued) break;
        }
        if (!issued) return null;
      }
      return Object.freeze({ kind: widget?'widget-runtime':'main-runtime', appId:id, domainId: domain.id, origin,
        canonicalOrigin: domain.origin, hostname: authority.hostname, state: domain.state,
        ...(widget?{widgetId:locator.widgetId}:{}),scopeHash:locator.scopeHash });
    } catch { return null; }
  }
  function createWidgetHostBinding(decision, widgetId, path) {
    const widget = runtimeWidget(widgetId), domain = widgetDomain(widget);
    assertApps(domain.id === decision.domainId && domain.origin === decision.origin, 'app_widget_runtime_changed', 403);
    const scope = captureWidgetRuntimeScope(decision, widget, path), scopeDigest = widgetRuntimeScopeDigest(scope);
    return Object.freeze({ origin: widgetRuntimeOrigin(widgetId, scopeDigest.slice(0, 24), runtimeZone(domain)), scope, scopeDigest });
  }
  function createMainHostBinding(decision,path) {
    const domain=runtimeDomain(decision.domainId,decision.appId);
    assertApps(domain.origin===decision.origin,'app_widget_runtime_changed',403);
    const scope=captureMainRuntimeScope(decision,path),scopeDigest=widgetRuntimeScopeDigest(scope);
    return Object.freeze({origin:mainRuntimeOrigin(decision.appId,scopeDigest.slice(0,24),runtimeZone(domain)),scope,scopeDigest});
  }
  function checkWidgetHostBinding(holder) {
    if (!holder.runtimeHost) return;
    const before = holder.runtimeHost;
    const main=before.scope.schema==='soty.main-runtime-scope.v1';
    assertApps(main?!rootWidgetId(holder)||rootWidgetId(holder)==='main':rootWidgetId(holder)===before.scope.widgetId,'app_widget_runtime_changed',403);
    const fresh=main?createMainHostBinding(holder.decision,holder.entryPath):createWidgetHostBinding(holder.decision,before.scope.widgetId,holder.entryPath);
    assertApps(fresh.origin === before.origin && fresh.scopeDigest === before.scopeDigest
      && JSON.stringify(fresh.scope) === JSON.stringify(before.scope), 'app_widget_runtime_changed', 403);
    if (!main&&checkWidgetLaunch) synchronous(checkWidgetLaunch({ phase: 'renew', actor: holder.decision.actor,
      widgetId: before.scope.widgetId, appId: holder.decision.appId,
      entry: { domainId: holder.decision.domainId, path: holder.entryPath }, target: { revision: holder.decision.targetRevision,
        digest: holder.decision.targetDigest, profile: holder.decision.profile } }), 'apps_async_authority');
  }
  const canonicalHostOrigin = app => app.appHost.canonicalOrigin ?? app.appHost.origin;
  const internalRuntimeHost=app=>['widget-runtime','main-runtime'].includes(app.appHost.kind);
  function checkHolderHost(holder, app) {
    const internal=internalRuntimeHost(app),main=app.appHost.kind==='main-runtime';
    assertApps(Boolean(holder.runtimeHost) === internal && (!internal || holder.runtimeHost.origin === app.appHost.origin
      &&(main?holder.runtimeHost.scope.schema==='soty.main-runtime-scope.v1'&&holder.decision.appId===app.id:rootWidgetId(holder)===app.appHost.widgetId)
      && holder.runtimeHost.scopeDigest.slice(0, 24) === app.appHost.scopeHash), 'app_widget_runtime_changed', 403);
  }
  function checkWidgetRuntimeFor(holder, phase) {
    checkWidgetHostBinding(holder);
    if(holder.hostSession)hostSessions.assertClaim(holder.hostSession,{candidate:Boolean(holder.hostRenewal)});
    const widgetId = rootWidgetId(holder); if (!checkWidgetRuntime || !widgetId?.startsWith('widget-')) return;
    const decision = holder.decision;
    assertApps(decision?.subject === 'account' && decision.actor, 'apps_widget_runtime_denied', 403);
    assertApps(synchronous(checkWidgetRuntime(Object.freeze({ widgetId, appId: decision.appId,
      actor: Object.freeze({ accountId: decision.actor.accountId, deviceId: decision.actor.deviceId }),
      entry: Object.freeze({ domainId: decision.domainId, path: holder.entryPath ?? decision.route.entryPath }),
      target: Object.freeze({ revision: decision.targetRevision, digest: decision.targetDigest, profile: decision.profile }), phase })), 'apps_async_authority') === true,
    'apps_widget_runtime_denied', 403);
  }
  /** Host-only invalidation: the IDs originated in reviewed Root launch state,
   * never a Source header/body. Main application sessions are independent. */
  function invalidateWidget(filter) {
    assertApps(filter && typeof filter === 'object' && !Array.isArray(filter) && Object.keys(filter).every(key=>['widgetId','accountId','deviceId'].includes(key))
      && typeof filter.widgetId === 'string' && /^widget-[a-f0-9-]{36}$/u.test(filter.widgetId), 'app_instance_invalid');
    if(filter.accountId!==undefined)textId(filter.accountId);if(filter.deviceId!==undefined)textId(filter.deviceId);
    hostSessions?.invalidateWidget(filter);
    const matchesWidget = holder => rootWidgetId(holder) === filter.widgetId && (!filter.accountId || holder.decision?.actor?.accountId === filter.accountId)
      && (!filter.deviceId || holder.decision?.actor?.deviceId === filter.deviceId);
    const records = new Set();
    for(const collection of [tickets,sessions,relayWindows])for(const [key,holder]of [...collection])if(matchesWidget(holder)){
      collection.delete(key);if(holder.scopedRecord)records.add(holder.scopedRecord);
      if(holder.checkDigest)bootCandidates.delete(holder.checkDigest);
    }
    for(const [key,value]of [...scopedRenewals])if(matchesWidget({decision:value.decision,scopedRecord:value.record})){
      scopedRenewals.delete(key);records.add(value.record);
    }
    for(const stream of [...live.values()])if(matchesWidget(stream.session)) { if(stream.scopedRecord)records.add(stream.scopedRecord);closeStream(stream,'app_access_revoked'); }
    for(const record of records)scopedGateway?.invalidate(record);
    return { invalidated: true };
  }
  function invalidateAccess(filter = {}) {
    for (const [key, item] of tickets) if (matches(item, filter) && !retainAccess(item)) {tickets.delete(key);if(item.scopedRecord)scopedGateway.invalidate(item.scopedRecord);}
    for (const [key, item] of sessions) if (matches(item, filter) && !retainAccess(item)) {sessions.delete(key);if(item.scopedRecord)scopedGateway.invalidate(item.scopedRecord);}
    for (const [key, item] of relayWindows) if (matches(item, filter) && !retainAccess(item)) relayWindows.delete(key);
    for (const stream of live.values()) if (matches(stream.session, filter)) { try { checkStream(stream); } catch {} }
  }
  function invalidateConnector({ hostDeviceId, connectorId } = {}) {
    for (const channel of channels.values()) if ((!hostDeviceId || channel.identity.hostDeviceId === hostDeviceId) && (!connectorId || channel.identity.connectorId === connectorId)) {
      channels.delete(channel.key); runtimeBindings.drop(channel); preparedBrowse.drop(channel);
      for (const stream of channel.streams.values()) closeStream(stream, 'app_offline', false); channel.ws.terminate();
    }
  }
  const unsubscribe = subscribeMembership?.(event => { invalidateAccess({ communityId: event.communityId }); });
  const auditTimer = setInterval(() => {
    invalidateAccess();
  }, Math.max(25, Math.min(10_000, Number.isFinite(accessAuditMs) ? accessAuditMs : 10_000)));
  auditTimer.unref();
  const heartbeatTimer = setInterval(() => {
    for (const channel of channels.values()) {
      if (now() - channel.lastSeenAt > 45_000) { channel.ws.terminate(); continue; }
      channel.ws.ping();
      // Reconcile changes from another legitimate registry writer without
      // resetting unchanged per-app ACKs or replaying user HTTP requests.
      try { sync(channel); } catch { channel.ws.terminate(); }
    }
    try { onHeartbeat(); } catch { /* The current channel heartbeat stays independent of recovery. */ }
  }, 10_000);
  heartbeatTimer.unref();
  const connectorAuditTimer = setInterval(() => {
    for (const channel of channels.values()) {
      if (channel.checking) continue; channel.checking = true;
      Promise.resolve(authenticateConnector(channel.auth)).then(valid => {
        if (!valid && channels.get(channel.key) === channel) invalidateConnector(channel.identity);
      }).catch(() => { if (channels.get(channel.key) === channel) invalidateConnector(channel.identity); }).finally(() => { channel.checking = false; });
    }
  }, Math.max(250, Math.min(30_000, connectorAuthCheckMs)));
  connectorAuditTimer.unref();
  function matches(item, filter) {
    const decision = item.decision;
    return (!filter.appId || decision.appId === filter.appId) && (!filter.accountId || decision.actor?.accountId === filter.accountId) && (!filter.deviceId || decision.actor?.deviceId === filter.deviceId);
  }
  function sync(channel) {
    if (!channel || channels.get(channel.key) !== channel || channel.ws.readyState !== 1) return;
    if (channel.bindingVersion === 2) {
      const targets = db.prepare(`SELECT t.* FROM local_apps a
        JOIN app_publications p ON p.app_id=a.id AND p.owner_account_id=a.owner_account_id
        JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision AND t.owner_account_id=p.owner_account_id
        WHERE t.connector_key=? AND a.state='enabled'`).all(channel.key).map(targetTuple);
      runtimeBindings.sync(channel, targets); return;
    }
    // Legacy connector v1 receives the route selected by the single current
    // target. It does not attest a target revision or immutable source code.
    const apps = db.prepare(`SELECT a.id,t.port,t.entry_path FROM local_apps a
      JOIN app_publications p ON p.app_id=a.id AND p.owner_account_id=a.owner_account_id
      JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision AND t.owner_account_id=p.owner_account_id
      JOIN app_source_heads h ON h.app_id=a.id
      WHERE t.connector_key=? AND a.state='enabled' AND h.required_binding_version=1`).all(channel.key)
      .map(app => ({ id: app.id, port: app.port, entryPath: app.entry_path }));
    send(channel, { type: 'sync', apps });
  }
  function registerLocalApp(actor, args, job = null) {
    exact(args, ['hostDeviceId', 'connectorId', 'name', 'port', 'entryPath', 'grants']);
    const device = db.prepare('SELECT * FROM app_devices WHERE owner_account_id=?').all(actor.accountId).find(item => {
      const identity = JSON.parse(item.identity_json); return identity.hostDeviceId === args.hostDeviceId && identity.connectorId === args.connectorId;
    });
    assertApps(device, 'apps_device_not_owned', 403);
    const name = appName(args.name), port = appPort(args.port, blockedPorts), entryPath = requestPath(args.entryPath ?? '/'), grants = cleanGrants(args.grants);
    assertApps(!entryPath.startsWith('/_soty/'), 'reserved_app_path'); assertGrants(actor, grants);
    const jobAppId = job ? appIdForCreationJob(job.id) : '';
    const id = transaction(() => {
      assertActor(actor); assertApps(binding(device.connector_key)?.owner_account_id === actor.accountId, 'apps_device_not_owned', 403); assertGrants(actor, grants);
      if (job) {
        const identity = JSON.parse(device.identity_json);
        assertApps(identity.linkId === job.linkId && identity.hostDeviceId === job.deviceId && identity.connectorId === job.connectorId, 'apps_device_not_owned', 403);
        const prior = row(jobAppId);
        if (prior) { assertApps(prior.owner_account_id === actor.accountId, 'app_job_id_conflict', 409); return prior.id; }
      }
      assertNativePortAvailable(db, '', device.connector_key, port);
      const existing = db.prepare(`SELECT a.*,t.entry_path AS current_entry_path FROM local_apps a
        JOIN app_publications p ON p.app_id=a.id AND p.owner_account_id=a.owner_account_id
        JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision AND t.owner_account_id=p.owner_account_id
        WHERE t.connector_key=? AND t.port=? AND a.state='enabled'`).get(device.connector_key, port);
      if (existing) {
        assertApps(!job && activeTarget(existing.id)?.profile === 'soty.relay-restricted.v1' && existing.owner_account_id === actor.accountId && existing.name === name && existing.current_entry_path === entryPath
          && existing.grants_json === JSON.stringify(grants), 'app_port_already_registered', 409);
        publications.execute({ actor, op: 'apps.publication.get', args: { appId: existing.id } });
        return existing.id;
      }
      const id = jobAppId || `app-${randomBytes(16).toString('hex')}`;
      insertRegisteredApp(actor, { id, connectorKey: device.connector_key, name, port, entryPath, grants });
      return id;
    });
    notifyChange(actor,id); sync(channels.get(device.connector_key)); return { app: publicApp(row(id), actor), authentication:authenticationStatus(id) };
  }
  function authenticationStatus(id,surface='embedded'){
    const target=activeTarget(id);
    assertApps(target,'app_unavailable',404);
    return appAuthenticationStatus({capability:authenticationCapability,appId:id,target,channel:channels.get(target.connector_key),surface});
  }
  function insertRegisteredApp(actor, { id, connectorKey: key, name, port, entryPath, grants }, initialTarget) {
    assertApps(db.isTransaction, 'apps_transaction_required', 500);
    assertApps(!row(id), 'apps_endpoint_request_conflict', 409);
    assertApps(db.prepare('SELECT count(*) AS n FROM local_apps WHERE owner_account_id=?').get(actor.accountId).n < 100, 'apps_limit_reached', 429);
    const timestamp = now();
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, actor.accountId, key, name, port, entryPath, JSON.stringify(grants), 'enabled', 1, timestamp, timestamp);
    persistGrants(id, grants); domains.ensureCanonicalForApp(row(id)); publications.initForApp(row(id), initialTarget);
  }
  function endpointDevice(actor, intent) {
    const identity = resolveOwnedDevice(actor, intent.hostDeviceId, intent.connectorId), device = binding(connectorKey(identity));
    assertApps(device?.owner_account_id === actor.accountId, 'apps_device_not_owned', 403); return device;
  }
  function endpointInventory(device, witness, endpoint) {
    const channel = channels.get(device.connector_key);
    assertApps(channel?.ws.readyState === 1, 'app_offline', 503);
    assertApps(channel.bindingVersion === 2 && channel.runtimeProfiles.includes(SELF_IP_PROFILE), 'apps_endpoint_connector_upgrade_required', 503);
    const inventory = channel.selfInventory;
    assertApps(inventory && now() >= channel.selfInventoryReceivedAt && now() - channel.selfInventoryReceivedAt <= 15_000, 'apps_endpoint_inventory_pending', 503);
    assertApps(witness && typeof witness === 'object' && !Array.isArray(witness) && Object.keys(witness).sort().join(',') === 'channelId,digest,revision'
      && witness.channelId === channel.channelId && witness.revision === inventory.revision && witness.digest === inventory.digest, 'apps_endpoint_interface_unavailable', 409);
    assertApps(inventory.addresses.some(item => item.address === endpoint.address), 'apps_endpoint_not_on_selected_device', 403);
    return inventory;
  }
  const endpointRegistry = createEndpointRegistration({ db, enabled: allowSelfIpTargets, blockedPorts, assertActor, withAuthorityFence, transaction,
    resolveDevice: endpointDevice, inventoryFor: endpointInventory, assertGrants,
    prepareTarget: runtimeBindings.prepareTarget, verifyPreparedTarget: runtimeBindings.verifyPreparedTarget,
    insertApp(actor, intent, target) {
      assertNativePortAvailable(db, '', target.connectorKey, target.port);
      assertApps(!db.prepare(`SELECT 1 FROM local_apps a JOIN app_publications p ON p.app_id=a.id
        JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision
        WHERE a.state='enabled' AND t.connector_key=? AND t.port=? LIMIT 1`).get(target.connectorKey, target.port), 'app_port_already_registered', 409);
      insertRegisteredApp(actor, { id: target.appId, connectorKey: target.connectorKey, name: intent.name, port: target.port, entryPath: target.entryPath, grants: intent.grants }, target);
    }, readApp: (id, actor) => publicApp(row(id), actor),
    afterCommit(actor, id) { notifyChange(actor, id); sync(channels.get(activeTarget(id)?.connector_key)); } });
  const endpointRegistrationExtension = Object.freeze({ operations: endpointRegistry.operations,
    async executeAsync({ op, actor, args = {} }) {
      const result=await endpointRegistry.executeAsync({ op, actor, args: authenticatedArgs(actor, args) });
      return {...result,authentication:appAuthority({actor,appId:result.app.id,mode:'participant'},()=>authenticationStatus(result.app.id))};
    } });
  function registerCreationJob(actor, job) {
    return withContactAuthority(actor, () => {
      assertActor(actor);
      assertApps(job && job.schema === 'soty.connector-job.v4' && job.status === 'succeeded' && job.autoRegister === true
        && job.ownerAccountId === actor.accountId && job.sourceActorDeviceId === actor.deviceId
        && job.proposal?.sourceJobId === job.id, 'app_job_authority_invalid', 403);
      return registerLocalApp(actor, { hostDeviceId: job.deviceId, connectorId: job.connectorId, name: job.proposal.name,
        port: job.proposal.port, entryPath: job.proposal.entryPath, grants: { accountIds: [], communityIds: [] } }, job);
    });
  }
  // Host-only: caller supplies a verified durable prepared-project receipt.
  function registerPreparedApp(actor, receipt, manifest) {
    return withContactAuthority(actor, () => {
      assertActor(actor);
      assertApps(receipt && receipt.ownerAccountId === actor.accountId && receipt.sourceActorDeviceId === actor.deviceId
        && /^job_[a-f0-9]{32}$/u.test(receipt.id || ''), 'prepared_authority_invalid', 403);
      return registerLocalApp(actor, { hostDeviceId: receipt.hostDeviceId, connectorId: receipt.connectorId,
        name: manifest.name, port: manifest.port, entryPath: manifest.entryPath, grants: { accountIds: [], communityIds: [] } },
      { id: receipt.id, linkId: receipt.linkId, deviceId: receipt.hostDeviceId, connectorId: receipt.connectorId });
    });
  }
  function creationJobApp(actor, jobId) {
    return withContactAuthority(actor, () => {
      assertActor(actor); const app = row(appIdForCreationJob(jobId));
      if (!app) return null;
      assertApps(app.owner_account_id === actor.accountId, 'apps_owner_required', 403);
      return publicApp(app, actor);
    });
  }
  function hostScope(holder) {
    const decision=holder.decision;
    assertApps(decision?.subject==='account'&&decision.actor&&!selectedRuntimeProfile(decision.profile),'app_host_session_changed',403);
    const instance=rootWidgetId(holder)??'main';
    assertApps(instance==='main'||holder.runtimeHost,'app_host_session_unavailable',503);
    return {schema:'soty.app-host-scope.v1',accountId:decision.actor.accountId,deviceId:decision.actor.deviceId,appId:decision.appId,
      policyEpoch:decision.policyEpoch,entry:{domainId:decision.domainId,origin:decision.origin,pathDigest:digest(holder.entryPath)},
      target:{revision:decision.targetRevision,digest:decision.targetDigest,profile:decision.profile,bindingFloor:decision.requiredBindingVersion},
      instanceId:instance,runtimeOrigin:holder.runtimeHost?.origin??decision.origin,runtimeScopeDigest:holder.runtimeHost?.scopeDigest??null};
  }
  function hostBindingReply(holder,descriptor) {
    const decision=holder.decision;
    return {hostSession:descriptor,hostSessionExpiresAt:Object.hasOwn(holder,'hostSessionExpiresAt')?holder.hostSessionExpiresAt:decision.expiresAt,
      entry:{appId:decision.appId,domainId:decision.domainId,origin:decision.origin,path:holder.entryPath},
      launchBinding:{schema:'soty.app-launch-binding.v1',policyEpoch:decision.policyEpoch,targetRevision:decision.targetRevision,
        targetDigest:decision.targetDigest,profile:decision.profile,bindingFloor:decision.requiredBindingVersion},
      ...(holder.runtimeHost?{runtimeOrigin:holder.runtimeHost.origin}:{})};
  }
  function freshHostHolder(actor,id,claim,path,candidate=false) {
    assertApps(hostSessions&&typeof path==='string'&&digest(runtimePath(path))===claim.scope.entry.pathDigest,'app_host_session_changed',403);
    const scope=claim.scope,decision=publications.decideAccess({domainId:scope.entry.domainId,origin:scope.entry.origin,actor,ttlMs:accountSessionMs});
    assertApps(decision.appId===id,'app_host_session_changed',403);assertRuntimeBinding(decision,{requireReady:true});
    const holder={decision,entryPath:path,...(scope.instanceId!=='main'?{rootInstanceId:scope.instanceId}:{}),hostSession:claim,...(candidate?{hostRenewal:{}}:{})};
    holder.runtimeHost=holder.rootInstanceId?createWidgetHostBinding(decision,holder.rootInstanceId,path):createMainHostBinding(decision,path);
    checkWidgetRuntimeFor(holder,'renew');
    assertApps(scopedHash(hostScope(holder))===claim.family,'app_host_session_changed',403);
    return holder;
  }
  const hostCookieName=claim=>`__Host-${cookieName}_h${claim.frameId}_${claim.revision}_${claim.leaseHash}`;
  const hostSelector=family=>`h_${family}`;
  function hostBase(req) {
    const name=`__Host-${cookieName}`,values=String(req.headers.cookie||'').split(';').map(part=>part.trim()).filter(part=>part.split('=',1)[0].trim()===name);
    assertApps(values.length<=1,'app_session_required',401);
    return values[0]?.slice(name.length+1);
  }
  function hostCookieToken(req,claim) {
    const name=hostCookieName(claim),values=String(req.headers.cookie||'').split(';').map(part=>part.trim()).filter(part=>part.split('=',1)[0].trim()===name);
    assertApps(values.length<=1,'app_session_required',401);
    const token=values[0]?.slice(name.length+1);return typeof token==='string'&&/^[A-Za-z0-9_-]{43}$/.test(token)?token:null;
  }
  function hostCookieHeaders(req,session,{initial=false}={}) {
    const claim=session.hostSession,base=`; HttpOnly; Path=/; SameSite=None; Secure; Partitioned`,headers=[];
    if(initial)headers.push(`__Host-${cookieName}=${hostSelector(claim.family)}${base}; Max-Age=86400`);
    headers.push(`${hostCookieName(claim)}=${session.hostCookieToken}${base}; Max-Age=3600`);
    const issuedHeaders=headers.length;
    // Expire only names observed in this request and proven obsolete for this
    // exact family. A delayed header cannot name a future successor cookie.
    const names=String(req.headers.cookie||'').split(';').map(part=>part.trim().split('=',1)[0]);
    const pattern=new RegExp(`^__Host-${cookieName}_h([A-Za-z0-9_-]{43})_([0-9]{1,16})_([a-f0-9]{64})$`);
    for(const name of names.slice(0,64)){
      const match=pattern.exec(name);if(!match||name===hostCookieName(claim)||headers.length-issuedHeaders>=16)continue;
      const state=hostSessions.cookieState(match[1],match[3]),revision=Number(match[2]);
      if(state?.family===claim.family&&Number.isSafeInteger(revision)&&revision>=1
        &&(!state.open||revision<=state.revision&&match[3]!==state.leaseHash))headers.push(`${name}=${base}; Max-Age=0`);
    }
    return headers;
  }
  function hostSessionFor(req,app) {
    const base=hostBase(req);if(!base?.startsWith('h_'))return null;
    assertApps(/^h_[a-f0-9]{64}$/.test(base),'apps_access_denied',403);
    const matching=[];let denied;
    for(const session of sessions.values())if(session.hostSession&&base===hostSelector(session.hostSession.family)){
      const token=hostCookieToken(req,session.hostSession);if(!token||sessions.get(digest(token))!==session)continue;
      try{checkHolderHost(session,app);hostSessions.assertClaim(session.hostSession);assertApps(!hostSessions.admitting(session.hostSession),'app_host_session_unconfirmed',409);
        assertApps(session.decision.appId===app.id&&session.decision.domainId===app.appHost.domainId&&session.decision.origin===canonicalHostOrigin(app),'apps_access_denied',403);
        checkAccess(session);assertApps(scopedHash(hostScope(session))===session.hostSession.family,'app_host_session_changed',403);matching.push(session);
      }catch(error){denied=error;}
    }
    if(!matching.length){if(denied)throw denied;assertApps(false,'apps_access_denied',403);}
    // All members in the immutable full-scope family have identical authority.
    // This does not select another widget, actor, target or policy as fallback.
    return matching.sort((a,b)=>b.hostCreatedAt-a.hostCreatedAt)[0];
  }
  function hostAttempt(id,frameId,requestId) {
    const matches=new Set();for(const holder of [...tickets.values(),...sessions.values()])if(holder.hostRenewal?.appId===id
      &&holder.hostRenewal.claim.frameId===frameId&&holder.hostRenewal.requestId===requestId)matches.add(holder.hostRenewal);
    assertApps(matches.size<=1,'app_host_session_conflict',409);return [...matches][0];
  }
  function admitHostSession(req,res,ticket,decision) {
    assertApps(ticket.hostTicketExpiresAt>now(),'app_ticket_invalid',403);
    const attempt=ticket.hostRenewal;
    const cookieNames=String(req.headers.cookie||'').split(';').map(part=>part.trim().split('=',1)[0]);
    assertApps(cookieNames.filter(name=>name.startsWith(`__Host-${cookieName}_h`)).length<=64,'apps_sessions_busy',429);
    if(attempt)assertApps(hostBase(req)===hostSelector(ticket.hostSession.family),'app_host_session_changed',403);
    let session=attempt?.session;
    if(!session||sessions.get(session.sessionKey)!==session){
      assertApps(sessions.size<4096,'apps_sessions_busy',429);
      const token=secret(),key=digest(token),sessionCheck=secret();
      session={decision,sessionKey:key,entryPath:ticket.entryPath,hostSession:ticket.hostSession,hostCookieToken:token,hostCreatedAt:now(),
        ...(ticket.rootInstanceId?{rootInstanceId:ticket.rootInstanceId}:{}),...(ticket.runtimeHost?{runtimeHost:ticket.runtimeHost}:{}),
        ...(attempt?{hostRenewal:attempt}:{}),checkDigest:digest(sessionCheck),checkToken:sessionCheck,checkExpiresAt:now()+30000,family:{head:key,revision:0}};
      sessions.set(key,session);if(attempt)attempt.session=session;
    }else{session.decision=decision;checkWidgetRuntimeFor(session,'session');}
    res.setHeader('Set-Cookie',hostCookieHeaders(req,session,{initial:!attempt}));
    json(res,200,{ok:true,entryPath:ticket.entryPath,sessionCheck:session.checkToken,
      ...(attempt?{hostSessionRenewal:true,hostSessionRequestId:attempt.requestId,hostSessionParentOrigin:attempt.parentOrigin}:{})});
  }
  function checkedHostCandidate(req,app,session) {
    assertApps(hostBase(req)===hostSelector(session.hostSession.family),'app_host_session_changed',403);
    const token=hostCookieToken(req,session.hostSession);
    const authority=session.hostCompatibility??session;
    assertApps(token&&sessions.get(digest(token))===authority,'app_session_check_failed',403);
    if(session.hostCompatibility){hostSessions.assertClaim(authority.hostSession);checkAccess(authority);}
    checkHolderHost(session,app);checkAccess(session);
    assertApps(scopedHash(hostScope(session))===session.hostSession.family,'app_host_session_changed',403);
    return session;
  }
  function admitHostCompatibility(req,res,ticket,decision,app) {
    const current=hostSessionFor(req,app),holder={...ticket,decision};
    assertApps(current&&scopedHash(hostScope(holder))===current.hostSession.family,'app_host_session_changed',403);
    for(const[key,value]of sessions)if(value.hostCompatibility&&value.checkExpiresAt<=now())sessions.delete(key);
    assertApps(sessions.size<4096&&[...sessions.values()].filter(value=>value.hostCompatibility&&value.hostSession.frameId===current.hostSession.frameId).length<4,'apps_sessions_busy',429);
    const key=digest(secret()),check=secret(),{hostRenewal:_priorRenewal,...base}=current;
    const session={...base,decision,sessionKey:key,hostCompatibility:current,checkDigest:digest(check),checkToken:check,checkExpiresAt:now()+30000};
    sessions.set(key,session);
    // Explicit legacy/external launch may use an already-current exact family,
    // but cannot replace its selector, lease or auth cookie.
    json(res,200,{ok:true,entryPath:ticket.entryPath,sessionCheck:check});
  }
  function hostPromotionConsumers(attempt,session,holder) {
    const claim=attempt.claim,prior=value=>value?.frameId===claim.frameId&&value.revision===claim.priorRevision&&value.leaseHash===claim.priorLeaseHash;
    const currentBinding=assertRuntimeBinding(holder.decision,{requireReady:true}),channel=channels.get(holder.decision.route.connectorKey);
    const sameScope=value=>!value.scopedRecord&&value.runtimeHost?.origin===holder.runtimeHost?.origin
      &&value.hostSession.family===claim.family&&scopedHash(hostScope(value))===claim.family;
    const streams=[...live.values()].filter(value=>prior(value.session.hostSession));
    const windows=[...relayWindows.values()].filter(value=>prior(value.hostSession));
    // Validate before the durable CAS. Promotion only replaces transport
    // authority for this exact member/head; existing Source I/O is not replayed.
    for(const stream of streams)assertApps(!stream.closed&&sameScope(stream.session)&&stream.channel===channel
      &&stream.runtimeBinding===currentBinding,'app_host_session_changed',403);
    for(const window of windows)assertApps(sameScope(window),'app_host_session_changed',403);
    return()=>{
      for(const stream of streams)stream.session={...stream.session,decision:holder.decision,sessionKey:session.sessionKey,
        hostSession:claim,hostRenewal:attempt};
      // Keep the independently bounded window-grant deadline.
      for(const window of windows)window.hostSession=claim;
    };
  }
  function hostOperation(op,args,actor,id) {
    assertApps(hostSessions,'app_host_session_unavailable',503);
    const common=['appId','frameId','lease','revision'];
    exact(args,op==='apps.session.renew'?[...common,'requestId','parentOrigin','currentEntryPath']:
      op==='apps.session.confirm'?[...common,'requestId']:op==='apps.session.context'?[...common,'currentEntryPath']:common);
    const descriptor={schema:HOST_SESSION_SCHEMA,frameId:args.frameId,lease:args.lease,revision:args.revision};
    const candidates=new Set([...tickets.values(),...sessions.values()].map(value=>value.hostRenewal).filter(Boolean));
    if(op==='apps.session.close'){
      const candidate=[...candidates].find(value=>value.appId===id&&value.descriptor.frameId===args.frameId&&value.descriptor.lease===args.lease&&value.descriptor.revision===args.revision);
      const closed=hostSessions.close(actor,id,args,candidate?.claim);
      if(closed){for(const collection of [tickets,sessions,relayWindows])for(const[key,value]of collection)if(value.hostSession?.frameId===args.frameId)collection.delete(key);
        for(const stream of [...live.values()])if(stream.session.hostSession?.frameId===args.frameId&&!rebindPlainHostStream(stream))closeStream(stream,'app_access_revoked');}
      else if(candidate)for(const collection of[tickets,sessions])for(const[key,value]of collection)if(value.hostRenewal===candidate)collection.delete(key);
      return{closed};
    }
    if(op==='apps.session.context'){
      const claim=hostSessions.current(args,actor,id),holder=freshHostHolder(actor,id,claim,args.currentEntryPath);
      const session=[...sessions.values()].find(value=>!value.hostCompatibility&&value.hostSession?.frameId===claim.frameId&&value.hostSession.leaseHash===claim.leaseHash);
      const admitting=hostSessions.admitting(claim),current=Boolean(!admitting&&session?.hostChecked===true&&session.decision.expiresAt>now());
      return{schema:'soty.app-host-session-context.v1',state:admitting?'admitting':current?'current':'renew-required',...hostBindingReply({...holder,hostSessionExpiresAt:current?session.decision.expiresAt:null},{...descriptor,expiresAt:claim.expiresAt})};
    }
    assertApps(typeof args.requestId==='string'&&/^[A-Za-z0-9_-]{16,128}$/.test(args.requestId),'app_host_session_request_conflict',409);
    let attempt=hostAttempt(id,args.frameId,args.requestId);
    if(op==='apps.session.confirm'){
      assertApps(attempt&&attempt.descriptor.lease===args.lease&&attempt.descriptor.revision===args.revision,'app_host_session_unconfirmed',409);
      const holder=freshHostHolder(actor,id,attempt.claim,attempt.entryPath,true),session=attempt.session;
      assertApps(session&&sessions.get(session.sessionKey)===session&&session.hostChecked===true&&session.checkExpiresAt>now(),'app_host_session_unconfirmed',409);
      const promoteConsumers=hostPromotionConsumers(attempt,session,holder);
      hostSessions.promote(attempt.claim);session.decision=holder.decision;session.hostSessionExpiresAt=holder.decision.expiresAt;promoteConsumers();
      return{schema:'soty.app-host-session-confirmation.v1',requestId:args.requestId,confirmed:true,...hostBindingReply(session,attempt.descriptor)};
    }
    assertApps(op==='apps.session.renew'&&origins.has(args.parentOrigin),'app_host_session_changed',403);
    const intent=digest(JSON.stringify([args.frameId,args.lease,args.revision,args.currentEntryPath,args.parentOrigin]));
    let holder;
    if(attempt){assertApps(attempt.intent===intent,'app_host_session_request_conflict',409);holder=freshHostHolder(actor,id,attempt.claim,attempt.entryPath,true);}
    else{
      const prior=hostSessions.current(args,actor,id);holder=freshHostHolder(actor,id,prior,args.currentEntryPath);
      const pending=[...candidates].filter(value=>value.claim.frameId===prior.frameId&&value.claim.priorRevision===prior.revision&&value.claim.priorLeaseHash===prior.leaseHash);
      assertApps(pending.length<2,'apps_sessions_busy',429);
      const next=hostSessions.prepare(prior);attempt={appId:id,requestId:args.requestId,intent,descriptor:next.descriptor,claim:next.claim,entryPath:holder.entryPath,parentOrigin:args.parentOrigin};
    }
    for(const[key,value]of tickets)if(value.hostRenewal===attempt)tickets.delete(key);
    assertApps(tickets.size<4096,'apps_launch_busy',429);const token=secret();
    const ticketExpiresAt=now()+30000;
    tickets.set(digest(token),{...holder,hostSession:attempt.claim,hostRenewal:attempt,hostTicketExpiresAt:ticketExpiresAt});
    const {bootPath}=guideLaunchPath(holder.entryPath),binding=hostBindingReply(holder,attempt.descriptor);
    return{launchUrl:`${holder.runtimeHost?.origin??holder.decision.origin}${bootPath}&mode=renew#${token}`,expiresAt:ticketExpiresAt,...binding,hostSessionRequestId:args.requestId};
  }
  function execute(request) {
    const mutating=['apps.update','apps.revoke','apps.domains.claim','apps.domains.retire','apps.publication.update','apps.source.promote','apps.claim'].includes(request.op);
    const wasPublic=mutating&&publiclyListed(request.args?.appId);
    const result=withContactAuthority(request.actor, () => executeCurrent(request));
    if(mutating)notifyChange(request.actor,request.args?.appId,wasPublic);
    return result;
  }
  function executeCurrent({ op, args = {}, actor }) {
    if (directoryOperations.includes(op)) assertApps(Object.hasOwn(args, 'expectedAccountId'), 'apps_directory_account_required');
    if(op==='apps.authentication.get')assertApps(Object.hasOwn(args,'expectedAccountId'),'apps_authentication_account_required');
    args = authenticatedArgs(actor, args); assertApps(operations.has(op), 'unsupported_operation');
    if (directoryOperations.includes(op)) return directory.execute({ op, actor, args });
    if (savedOperations.has(op)) return saved.execute({ op, actor, args });
    if (discussionOperations.has(op)) return discussions.execute({ op, actor, args });
    if (op === 'apps.source.promote' || op === 'apps.source.history') return sources.execute({ op, actor, args });
    if (domainOperations.has(op)) return domains.execute({ actor, op, args });
    if (publicationOperations.has(op)) return publications.execute({ actor, op, args });
    if (op === 'apps.inspect') return inspection.read(actor, args);
    if (op === 'apps.entry.get') return entryRead(actor, args);
    if(op==='apps.authentication.get'){
      exact(args,['appId','surface']);
      assertApps(args.surface===undefined||['embedded','native'].includes(args.surface),'invalid_arguments');
      return appAuthority({actor,appId:args.appId,mode:'participant'},()=>authenticationStatus(args.appId,args.surface));
    }
    if (op === 'apps.devices') {
      exact(args, []);
      return { devices: db.prepare('SELECT * FROM app_devices WHERE owner_account_id=? ORDER BY created_at').all(actor.accountId).map(deviceProjection) };
    }
    if (op === 'apps.claim') {
      exact(args, ['hostDeviceId', 'connectorId', 'claimCode']);
      const channel = [...channels.values()].find(item => item.identity.hostDeviceId === textId(args.hostDeviceId) && item.identity.connectorId === textId(args.connectorId));
      assertApps(channel && typeof args.claimCode === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(args.claimCode) && channel.claimExpiresAt > now() && equalDigest(channel.claimDigest, digest(args.claimCode)), 'apps_claim_unavailable', 403);
      const existing = binding(channel.key);
      assertApps(!existing || existing.owner_account_id === actor.accountId, 'apps_device_already_owned', 403);
      if (!existing) db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(channel.key, actor.accountId, JSON.stringify(channel.identity), channel.name, now());
      channel.claimDigest = ''; channel.claimExpiresAt = 0; send(channel, { type: 'claimed' }); sync(channel);
      return { device: deviceProjection(binding(channel.key)) };
    }
    if (op === 'apps.catalog') { exact(args, []); return publicCatalog(); }
    if (op === 'apps.list') {
      exact(args, ['communityId']); const community = args.communityId === undefined ? '' : textId(args.communityId);
      if (community) assertApps(canAccessCommunity(actor.accountId, community) === true, 'apps_access_denied', 403);
      const candidates = appCandidates(actor);
      return { configured: Boolean(template || readNamedOrigins(db).length), apps: candidates.filter(app =>
        (canUse(actor, app) || app.owner_account_id === actor.accountId) && (!community || JSON.parse(app.grants_json).communityIds.includes(community))).map(app => publicApp(app, actor)) };
    }
    if (op === 'apps.register') {
      return registerLocalApp(actor, args);
    }
    const id = appId(args.appId), app = row(id);
    if(op.startsWith('apps.session.'))return hostOperation(op,args,actor,id);
    if (op === 'apps.relay.identity.close') {
      exact(args, ['appId', 'grant']);
      assertApps(typeof args.grant === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(args.grant), 'app_identity_window_unavailable', 403);
      const key = digest(args.grant), holder = relayWindows.get(key);
      assertApps(!holder || holder.decision.appId === id && holder.decision.actor?.accountId === actor.accountId
        && holder.decision.actor?.deviceId === actor.deviceId, 'app_identity_window_unavailable', 403);
      relayWindows.delete(key);
      for (const stream of live.values()) if (stream.session.windowKey === key && stream.session.decision.appId === id && stream.session.decision.actor?.accountId === actor.accountId
        && stream.session.decision.actor?.deviceId === actor.deviceId) closeStream(stream, 'app_access_revoked');
      return { closed: true };
    }
    if (op === 'apps.relay.identity') {
      exact(args, ['appId', 'domainId', 'launchBinding', 'instanceId', 'runtimeOrigin', 'hostSession']);
      const hasInstance = Object.hasOwn(args, 'instanceId'), hasRuntime = Object.hasOwn(args, 'runtimeOrigin');
      assertApps(hasInstance === hasRuntime && (!hasInstance || typeof args.instanceId === 'string'
        && /^(?:main|widget-[a-f0-9-]{36})$/u.test(args.instanceId) && typeof args.runtimeOrigin === 'string'), 'app_identity_window_unavailable', 403);
      const hasHostSession=Object.hasOwn(args,'hostSession');
      assertApps(!hasHostSession||hasInstance&&args.hostSession&&typeof args.hostSession==='object'&&!Array.isArray(args.hostSession)
        &&Object.keys(args.hostSession).sort().join(',')==='frameId,lease,revision','app_identity_window_unavailable',403);
      const pin = args.launchBinding;
      assertApps(pin && typeof pin === 'object' && !Array.isArray(pin)
        && Object.keys(pin).sort().join(',') === 'bindingFloor,policyEpoch,profile,schema,targetDigest,targetRevision'
        && pin.schema === 'soty.app-launch-binding.v1', 'app_identity_window_unavailable', 503);
      const domain = db.prepare('SELECT * FROM app_domains WHERE app_id=? AND id=?').get(id, textId(args.domainId));
      assertApps(domain && app?.state === 'enabled', 'apps_access_denied', 403);
      const decision = publications.decideAccess({ domainId: domain.id, origin: domain.origin, actor, ttlMs: 30_000 });
      assertApps(!selectedRuntimeProfile(decision.profile) && decision.subject === 'account'
        && decision.policyEpoch === pin.policyEpoch && decision.targetRevision === pin.targetRevision
        && decision.targetDigest === pin.targetDigest && decision.profile === pin.profile
        && decision.requiredBindingVersion === pin.bindingFloor, 'app_access_changed', 403);
      assertRuntimeBinding(decision, { requireReady: true });
      const holder = { decision };
      if (hasInstance) {
        let original;
        if(hasHostSession){
          assertApps(hostSessions,'app_host_session_unavailable',503);
          const claim=hostSessions.current(args.hostSession,actor,id);
          assertApps(!hostSessions.admitting(claim),'app_host_session_unconfirmed',409);
          original=[...sessions.values()].find(value=>!value.hostCompatibility&&value.hostChecked===true&&value.decision.expiresAt>now()
            &&value.hostSession?.frameId===claim.frameId&&value.hostSession.leaseHash===claim.leaseHash&&value.hostSession.revision===claim.revision
            &&value.runtimeHost?.origin===args.runtimeOrigin);
          assertApps(original,'app_identity_window_unavailable',403);holder.hostSession=claim;
        }
        if(args.instanceId==='main'){
          assertApps(original?.runtimeHost.scope.schema==='soty.main-runtime-scope.v1','app_identity_window_unavailable',403);
          holder.entryPath=original.entryPath;holder.runtimeHost=createMainHostBinding(decision,holder.entryPath);
        }else{
          const widget = runtimeWidget(args.instanceId);
          holder.rootInstanceId = args.instanceId; holder.entryPath = widget.entry.path;
          holder.runtimeHost = createWidgetHostBinding(decision, args.instanceId, holder.entryPath);
        }
        assertApps(holder.runtimeHost.origin === args.runtimeOrigin, 'app_widget_runtime_changed', 403);
        if(hasHostSession)assertApps(scopedHash(hostScope(holder))===holder.hostSession.family,'app_host_session_changed',403);
        checkWidgetRuntimeFor(holder, 'identity');
      }
      const channel = channels.get(decision.route.connectorKey);
      assertApps(channel?.bindingVersion === 2 && channel.relayIdentity === true && signRelayIdentity, 'app_identity_unavailable', 503);
      for (const [key, value] of relayWindows) if (value.decision.expiresAt <= now()) relayWindows.delete(key);
      assertApps(relayWindows.size < 4096, 'apps_sessions_busy', 429);
      const grant = secret(); relayWindows.set(digest(grant), holder);
      return { schema: 'soty.relay-window-grant.v1', grant, expiresAt: decision.expiresAt };
    }
    if(op==='apps.scoped.close') {
      exact(args,['appId','handle']);assertApps(scopedGateway,'app_scoped_context_closed',403);
      return scopedGateway.abandon(actor,id,args.handle);
    }
    if (op === 'apps.launch') {
      exact(args, ['appId', 'domainId', 'path', 'instanceId','hostSession']);
      assertApps(args.hostSession===undefined||args.hostSession===true,'invalid_arguments');
      assertApps(args.instanceId === undefined || typeof args.instanceId === 'string' && /^(?:main|widget-[a-f0-9-]{36})$/u.test(args.instanceId), 'app_instance_invalid', 400);
      assertApps(app?.state === 'enabled', 'apps_access_denied', 403);
      const domainId = args.domainId === undefined && args.path === undefined
        ? defaultEntryFor(app, activeTarget(id), actor)?.domainId : args.domainId;
      const domain = domainId === undefined
        ? db.prepare("SELECT * FROM app_domains WHERE app_id=? AND role='canonical'").get(id)
        : db.prepare('SELECT * FROM app_domains WHERE app_id=? AND id=?').get(id, textId(domainId));
      assertApps(domain, args.domainId === undefined ? 'apps_origin_not_configured' : 'apps_access_denied', args.domainId === undefined ? 503 : 403);
      const decision = publications.decideAccess({ domainId: domain.id, origin: domain.origin, actor });
      if(args.instanceId?.startsWith('widget-')&&checkWidgetLaunch)synchronous(checkWidgetLaunch({actor,widgetId:args.instanceId,appId:id,
        entry:{domainId:domain.id,path:runtimePath(args.path??decision.route.entryPath)},target:{revision:decision.targetRevision,digest:decision.targetDigest,profile:decision.profile}}),'apps_async_authority');
      assertRuntimeBinding(decision, { requireReady: true });
      // Saved main/widget root entries follow only an explicitly admitted Root-SSO workspace.
      const savedRootEntry = args.path === '/' && decision.route.entryPath === '/embed' && selectedRuntimeProfile(decision.profile)
        && isRootSignedWorkspaceSource(scopedAdmissions.require(targetTuple(activeTarget(id))).sourceProfile);
      if (args.path === '/_soty/guide') assertApps(!selectedRuntimeProfile(decision.profile), 'app_guide_unsupported', 404);
      const launchPath = savedRootEntry ? '/embed' : args.path ?? decision.route.entryPath;
      const { entryPath, bootPath } = selectedRuntimeProfile(decision.profile) ? createScopedLaunchPath(launchPath) : guideLaunchPath(launchPath);
      assertApps(channels.has(decision.route.connectorKey), 'app_offline', 503);
      assertApps(tickets.size < 4096, 'apps_launch_busy', 429);
      const ticket = secret();
      let scoped;
      if(selectedRuntimeProfile(decision.profile)) {
        assertApps(scopedGateway,'app_scoped_admission_required',503);
        scoped=scopedGateway.open({actor,appId:id,domainId:domain.id,target:targetTuple(activeTarget(id)),instanceId:args.instanceId});
        voiceSourceActors.set(scoped.record,actor);
      }
      const launchHolder = { decision, entryPath,...(args.instanceId?.startsWith('widget-')?{rootInstanceId:args.instanceId}:{}),...(scoped?{scopedRecord:scoped.record}:{}) };
      if (!scoped && args.instanceId?.startsWith('widget-') && resolveWidgetRuntime
        && synchronous(resolveWidgetRuntime(args.instanceId), 'apps_async_authority'))
        launchHolder.runtimeHost = createWidgetHostBinding(decision, args.instanceId, entryPath);
      if(!scoped&&args.hostSession===true&&!args.instanceId?.startsWith('widget-'))launchHolder.runtimeHost=createMainHostBinding(decision,entryPath);
      let hostSession;
      if(!scoped&&args.hostSession===true){assertApps(hostSessions,'app_host_session_unavailable',503);const initial=hostSessions.create(hostScope(launchHolder));
        launchHolder.hostSession=initial.claim;launchHolder.hostTicketExpiresAt=now()+30000;hostSession=initial.descriptor;}
      try {checkWidgetRuntimeFor(launchHolder,'session');}catch(error){if(scoped)scopedGateway.invalidate(scoped.record);if(hostSession)hostSessions.close(actor,id,hostSession);throw error;}tickets.set(digest(ticket), launchHolder);
      return { launchUrl: `${launchHolder.runtimeHost?.origin ?? domain.origin}${bootPath}#${ticket}`, expiresAt: decision.expiresAt,
        ...(launchHolder.runtimeHost ? { runtimeOrigin: launchHolder.runtimeHost.origin } : {}),
        ...(hostSession?{hostSession,hostSessionExpiresAt:Math.min(decision.expiresAt,hostSession.expiresAt)}:{}),
        ...(scoped?{runtimeProfile:decision.profile,scopedCloseHandle:scoped.closeHandle,scopedSlotExpiresAt:scoped.record.context.expiresAt,
          scopedSource:{...scoped.record.context.sourceProfile},...(scopedGateway.frameId(scoped.record)?{scopedFrameId:scopedGateway.frameId(scoped.record)}:{})}:{}),
        ...(!scoped?{launchBinding:{schema:'soty.app-launch-binding.v1',policyEpoch:decision.policyEpoch,targetRevision:decision.targetRevision,targetDigest:decision.targetDigest,profile:decision.profile,bindingFloor:decision.requiredBindingVersion}}:{}),
        entry: { appId: id, domainId: domain.id, origin: domain.origin, path: entryPath } };
    }
    if (op === 'apps.revoke') {
      exact(args, ['appId']);
      const revoked = transaction(() => {
        assertActor(actor); const current = row(id);
        assertApps(current && current.owner_account_id === actor.accountId, 'apps_owner_required', 403);
        if (current.state === 'enabled') {
          db.prepare("UPDATE local_apps SET state='revoked',revision=revision+1,updated_at=? WHERE id=?").run(now(), id);
          publications.revokeInTransaction(id);
        }
        const target = activeTarget(id);
        assertApps(target?.owner_account_id === actor.accountId, 'apps_registry_corrupt', 500);
        return target.connector_key;
      });
      invalidateAccess({ appId: id }); sync(channels.get(revoked)); return { app: publicApp(row(id), actor) };
    }
    exact(args, ['appId', 'name', 'grants', 'expectedRevision']);
    if (args.expectedRevision !== undefined) assertApps(Number.isSafeInteger(args.expectedRevision) && args.expectedRevision >= 1, 'invalid_app_revision');
    const requestedName = args.name === undefined ? undefined : appName(args.name), requestedGrants = args.grants === undefined ? undefined : cleanGrants(args.grants);
    transaction(() => {
      assertActor(actor); const current = row(id);
      assertApps(current && current.owner_account_id === actor.accountId, 'apps_owner_required', 403);
      assertApps(current.state === 'enabled', 'app_revoked', 409);
      assertApps(args.expectedRevision === undefined || current.revision === args.expectedRevision, 'app_revision_conflict', 409);
      const grants = requestedGrants ?? JSON.parse(current.grants_json);
      // A name-only change preserves existing grants, including grants that
      // have become ineffective after a community role change.
      if (requestedGrants !== undefined) assertGrants(actor, grants);
      const grantsChanged = requestedGrants !== undefined && JSON.stringify(cleanGrants(JSON.parse(current.grants_json))) !== JSON.stringify(requestedGrants);
      db.prepare('UPDATE local_apps SET name=?,grants_json=?,revision=revision+1,updated_at=? WHERE id=?')
        .run(requestedName ?? current.name, requestedGrants === undefined ? current.grants_json : JSON.stringify(grants), now(), id);
      persistGrants(id, grants);
      if (grantsChanged) publications.grantsChangedInTransaction(id);
    });
    invalidateAccess({ appId: id }); return { app: publicApp(row(id), actor) };
  }
  function appForRequest(req) {
    const host = hostClassifier.classifyHost(req.headers.host);
    if (host.kind === 'outside') return null;
    if (!['canonical', 'alias', 'widget-runtime','main-runtime'].includes(host.kind)) return { hostStatus: host.kind };
    return { ...(row(host.appId) || { missing: true }), appHost: host };
  }
  function hostFailure(app) {
    if (app.appHost?.kind === 'alias') {
      if (app.appHost.state === 'tombstone') return { status: 410, error: 'app_address_retired' };
      if (!db.prepare('SELECT 1 FROM app_publication_domains WHERE app_id=? AND domain_id=?').get(app.appHost.appId, app.appHost.domainId))
        return { status: 503, error: 'app_named_runtime_unavailable' };
    }
    if (!app.hostStatus) return null;
    if (app.hostStatus === 'invalid') return { status: 400, error: 'invalid_app_host' };
    return { status: 404, error: 'app_not_found' };
  }
  // An error document never grants entry. Its only additional viewer is the
  // pre-captured trusted shell; request headers/query cannot select ancestors.
  function respondRuntimeFailure(req, res, status, error, page) {
    if (bootRefusalOrigins.length && req.method === 'GET' && typeof req.url === 'string'
      && /^\/_soty\/boot(?:\?|$)/u.test(req.url)
      && typeof req.headers.accept === 'string' && req.headers.accept.includes('text/html')) {
      const refusalStatus = Number.isInteger(status) && status >= 400 && status <= 599 ? status : 503;
      respondFailure(req, res, refusalStatus, 'app_unavailable', { frameOrigins: bootRefusalOrigins }); return;
    }
    respondFailure(req, res, status, error, page);
  }
  function setStatusPolicy(res) {
    res.setHeader('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'; sandbox");
    res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer'); res.setHeader('Cache-Control', 'no-store');
  }
  function shellRecoveryFor(app, path) {
    const target = activeTarget(app.id);
    if (target && selectedRuntimeProfile(target.profile)) {
      // Recover through the current admitted address, not the failing alias or
      // an omitted domain that would resolve back to an unrelated canonical.
      const profile = scopedAdmissions.require(targetTuple(target));
      assertApps(origins.has(profile.parentOrigin), 'app_scoped_parent_unavailable', 503);
      const domain = db.prepare(`SELECT ad.id FROM app_domains ad
        WHERE ad.app_id=? AND ad.owner_account_id=? AND ad.origin=? AND ad.state='bound'
        AND (ad.role='canonical' OR EXISTS(SELECT 1 FROM app_publication_domains pd
          WHERE pd.app_id=ad.app_id AND pd.owner_account_id=ad.owner_account_id AND pd.domain_id=ad.id))`)
        .get(app.id, target.owner_account_id, profile.embedOrigin);
      assertApps(domain, 'app_unavailable', 503);
      const url = new URL('/', profile.parentOrigin);
      url.hash = `launch/${app.id}/${domain.id}?${new URLSearchParams({ path: checkedScopedEntryPath(path) ?? '/embed' })}`;
      return { url: url.href, currentSource: true };
    }
    const url = new URL('/', [...origins][0]);
    url.hash = `launch/${app.appHost.appId}/${app.appHost.domainId}${path === undefined ? '' : '?' + new URLSearchParams({ path: guidePath(path) })}`;
    return { url: url.href, currentSource: false };
  }
  function publicResetFor(app, path) {
    if (internalRuntimeHost(app)) return undefined;
    try {
      publications.decideAccess({ domainId: app.appHost.domainId, origin: app.appHost.origin });
      return guidePath(path);
    } catch { return undefined; }
  }
  function respondAppFailure(req, res, status, error, canRedirect = true) {
    let page;
    try {
    const app = appForRequest(req);
    const failure = app && hostFailure(app);
    if (failure) { setStatusPolicy(res); respondRuntimeFailure(req, res, failure.status, failure.error); return; }
    if (app?.appHost&&internalRuntimeHost(app)) { setStatusPolicy(res); respondRuntimeFailure(req, res, status, error); return; }
    if (app?.appHost && !app.missing) {
      let path;
      try { path = runtimePath(req.url || '/'); } catch { /* Internal endpoints never redirect into the shell. */ }
      if (!path && req.method === 'GET' && ['/_soty/ingress-check', '/_soty/ingress-check?'].includes(req.url)
        && String(req.headers.accept || '').includes('text/html')) {
        // Native ingress returns this refusal as the iframe document. Keep its
        // status readable, but offer only a fresh Root launch of the trusted entry.
        const target = activeTarget(app.id);
        if (target) {
          runtimePath(target.entry_path);
          page = { shellUrl: shellRecoveryFor(app).url, frameOrigins: [...origins] };
        }
      }
      if (path) {
        const recovery = shellRecoveryFor(app, path), shellUrl = recovery.url;
        const publicResetPath = recovery.currentSource ? undefined : publicResetFor(app, path);
        page = { shellUrl, publicResetPath, frameOrigins: [...origins] };
        const accessFailure = ['app_session_required', 'apps_access_denied', 'app_access_revoked', 'app_access_expired', 'app_access_changed', 'apps_authentication_required'].includes(error)
          || recovery.currentSource && ['app_frame_required', 'scoped_embed_source_changed'].includes(error);
        if (canRedirect && !publicResetPath && accessFailure && req.method === 'GET'
          && req.headers['sec-fetch-mode'] === 'navigate' && req.headers['sec-fetch-dest'] === 'document') {
          res.statusCode = 302; res.setHeader('Location', shellUrl); res.setHeader('Cache-Control', 'no-store'); res.setHeader('Referrer-Policy', 'no-referrer'); res.end(); return;
        }
      }
    }
    } catch {
      // A recovery view cannot outlive or bypass unavailable authority/storage.
      // Fall back to a content-free status without exposing an unchecked action.
      page = undefined; status = 503; error = 'app_unavailable';
    }
    respondRuntimeFailure(req, res, status, error, page);
  }
  const frameRequests=new WeakMap();
  function requestFrame(req) {return frameRequests.get(req);}
  function parseRequestFrame(req) {
    const header=req.headers['x-soty-frame'],url=new URL(req.url||'/', 'https://frame.invalid'),values=url.searchParams.getAll('sotyFrame');
    assertApps(headerCount(req,'x-soty-frame')<=1&&values.length<=1,'app_frame_invalid',403);
    const frame=header??values[0];
    assertApps(frame===undefined||typeof frame==='string'&&/^[A-Za-z0-9_-]{43}$/u.test(frame)
      &&(values.length===0||header===undefined||header===values[0]),'app_frame_invalid',403);
    return frame;
  }
  function primaryKey(req, frame=requestFrame(req)) {
    const name = `__Host-${cookieName}${frame?'_'+frame:''}`;
    const values = String(req.headers.cookie || '').split(';').map(part => part.trim()).filter(part => part.split('=', 1)[0].trim() === name);
    assertApps(values.length<=1,'app_session_required',401);
    if(!values.length)return null;
    const token=values[0].slice(name.length+1);assertApps(/^[A-Za-z0-9_-]{43}$/u.test(token),'app_session_required',401);
    return digest(token);
  }
  function primaryCookie(value,frame) {return `__Host-${cookieName}${frame?'_'+frame:''}=${value}; HttpOnly; Path=/; SameSite=None; Secure; Partitioned; Max-Age=3600`;}
  // The ordinary ingress cookie remains compatible. Private workspace traffic
  // selects exactly one frame cookie; its public name conveys no authority.
  function sessionCookies(value,session) {return session.family.frameId?[primaryCookie(value),primaryCookie(value,session.family.frameId)]:primaryCookie(value);}
  function retirePrimary(session) {if(session.family?.head===session.sessionKey){session.family.head=null;session.family.revision++;}}
  function discardCandidate(session){
    if(bootCandidates.get(session.checkDigest)===session)bootCandidates.delete(session.checkDigest);
    if(session.promotion?.state==='pending'&&sessions.get(session.sessionKey)===session)sessions.delete(session.sessionKey);
    delete session.checkToken;if(session.promotion)delete session.promotion.token;
  }
  function sweepCandidates(){for(const session of bootCandidates.values())if(session.checkExpiresAt<=now()||sessions.get(session.sessionKey)!==session)discardCandidate(session);}
  function checkCandidate(session,req,app,frame=requestFrame(req)){
    const p=session?.promotion;
    assertApps(session&&p&&sessions.get(session.sessionKey)===session&&session.scopedRecord?.session===session
      &&session.checkExpiresAt>now()
      &&session.decision.appId===app.id&&session.decision.domainId===app.appHost.domainId&&session.decision.origin===app.appHost.origin,
      'app_scoped_candidate_changed',403);
    assertApps(frame===session.family.frameId,'app_frame_invalid',403);
    const key=primaryKey(req,frame);
    const familyCurrent=p.state==='promoted'?session.family.head===session.sessionKey
      :session.family.head===p.baseHead&&session.family.revision===p.baseRevision;
    if(!familyCurrent)discardCandidate(session);
    assertApps(familyCurrent&&(p.state==='promoted'?key===p.baseCookieKey||key===session.sessionKey:key===p.baseCookieKey),'app_scoped_candidate_changed',403);
    checkAccess(session);scopedGateway.context(session.scopedRecord);return session;
  }
  function candidateFor(req,app){
    sweepCandidates();const check=req.headers['x-soty-boot-check'];
    assertApps(headerCount(req,'x-soty-boot-check')===1&&typeof check==='string'&&/^[A-Za-z0-9_-]{43}$/u.test(check),'app_session_check_failed',403);
    return checkCandidate(bootCandidates.get(digest(check)),req,app);
  }
  function promoteCandidate(stream){
    const session=candidateFor(stream.req,stream.candidateApp),p=session.promotion;
    assertApps(session===stream.bootCandidateSession&&session.sessionKey===stream.session.sessionKey
      &&scopedGateway.sourceContext(session.scopedRecord)?.ready===true,'app_scoped_candidate_unconfirmed',403);
    if(p.state==='pending'){scopedGateway.promoteFrame(session.scopedRecord);session.family.head=session.sessionKey;session.family.revision++;p.state='promoted';}
    stream.res.setHeader('Set-Cookie',sessionCookies(p.token,session));
  }
  function sessionFor(req, app, { requireCookie = false } = {}) {
    const hostSession=hostSessionFor(req,app);if(hostSession)return hostSession;
    const key=primaryKey(req);
    if (key===null) {
      assertApps(!requireCookie && app.appHost.kind === 'alias', 'app_session_required', 401);
      return { decision: publications.decideAccess({ domainId: app.appHost.domainId, origin: app.appHost.origin }) };
    }
    const session = sessions.get(key);
    assertApps(session && session.decision.appId === app.id && session.decision.domainId === app.appHost.domainId && session.decision.origin === canonicalHostOrigin(app), 'apps_access_denied', 403);
    checkHolderHost(session, app);
    assertApps(session.promotion?.state!=='pending','app_scoped_candidate_unconfirmed',403);
    const frame=requestFrame(req);
    assertApps(!frame||session.family.frameId===frame&&session.family.head===key,'app_frame_invalid',403);
    checkAccess(session);
    return session;
  }
  function publicFrameSession(req,app) {
    // Only compiled credential-free assets and fixed Native ingress use this.
    // The base cookie fixes the actor/app/target scope even after its frame closes;
    // a different presented frame may supply liveness, never a different identity.
    const base=sessionFor(req,app,{requireCookie:true});
    if(requestFrame(req)){scopedGateway.context(base.scopedRecord);return base;}
    const scope=context=>{const {reference,instanceId,expiresAt,...binding}=context;return scopedHash(binding);};
    assertApps(isRootSignedWorkspaceSource(base.scopedRecord?.context.sourceProfile),'app_scoped_context_closed',403);
    const binding=scope(base.scopedRecord.context);
    const current=session=>{
      if(!session?.family.frameId||session.family.head!==session.sessionKey||session.promotion?.state==='pending'
        ||!session.scopedRecord||scope(session.scopedRecord.context)!==binding)return false;
      try{checkAccess(session);scopedGateway.context(session.scopedRecord);return true;}
      catch(error){if(![401,403].includes(error.status))throw error;return false;}
    };
    if(current(base))return base;
    const frames=String(req.headers.cookie||'').split(';').map(part=>/^__Host-soty_app_session_([A-Za-z0-9_-]{43})=/u.exec(part.trim())?.[1]).filter(Boolean);
    assertApps(frames.length<=SCOPED_EMBED_LIMITS.continuations&&new Set(frames).size===frames.length,'app_session_required',403);
    for(const frame of frames){const session=sessions.get(primaryKey(req,frame));
      if(session?.family.frameId===frame&&current(session))return session;}
    assertApps(false,'app_scoped_context_closed',403);
  }
  function setPolicy(res, origin, scoped, rootWorkspace = false, nativeOrigin = null) {
    const frameOrigins = [...origins].join(' ');
    // An admitted Root-signed workspace runs its existing bundled compiler and
    // opaque, credential-free code preview. The exact reviewed pin selects this
    // policy; neither an app request nor its response can enable it. Native CSP
    // still intersects below. Only this admitted workspace's exact Native API
    // may receive browser requests; response headers cannot add destinations.
    const scripts = rootWorkspace ? "'self' 'unsafe-inline' blob: 'wasm-unsafe-eval'" : "'self' 'unsafe-inline'";
    const workers = rootWorkspace ? "'self'" : "'none'";
    if (rootWorkspace && nativeOrigin) assertApps(typeof nativeOrigin === 'string'
      && !/[\s\u0000-\u001f\u007f;,*'"\\<>]/u.test(nativeOrigin)
      && new URL(nativeOrigin).origin === nativeOrigin, 'app_native_origin_invalid', 403);
    const connect = rootWorkspace && nativeOrigin && nativeOrigin !== origin ? `'self' ${nativeOrigin}` : "'self'";
    res.setHeader('Content-Security-Policy', `default-src 'self'; script-src ${scripts}; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; media-src 'self' blob:${scoped?' data:':''}; connect-src ${connect}; worker-src ${workers}; object-src 'none'; base-uri 'self'; form-action 'self'; frame-src 'none'; frame-ancestors ${frameOrigins}; sandbox allow-scripts allow-forms allow-same-origin allow-downloads${scoped?' allow-popups allow-popups-to-escape-sandbox':''}`);
    res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=(), payment=(), usb=(), serial=(), hid=(), bluetooth=()');
    res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer'); res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Origin-Agent-Cluster', '?1');
    if (new URL(origin).protocol === 'https:') res.setHeader('Strict-Transport-Security', 'max-age=31536000');
  }
  function sendGuide(req, res, app, session) {
    const decision = checkAccess(session);
    assertApps(!selectedRuntimeProfile(decision.profile), 'app_guide_unsupported', 404);
    assertRuntimeBinding(decision, { requireReady: true });
    assertApps(channels.get(decision.route.connectorKey)?.ws.readyState === 1, 'app_offline', 503);
    const current = db.prepare(`SELECT a.name,a.owner_account_id,p.launch_policy FROM local_apps a
      JOIN app_publications p ON p.app_id=a.id AND p.owner_account_id=a.owner_account_id WHERE a.id=? AND a.state='enabled'`).get(decision.appId);
    assertApps(current && typeof readAppGuide === 'function', 'app_guide_unavailable', 404);
    const view = readAppGuide({ appId: decision.appId, ownerId: current.owner_account_id, title: current.name,
      visibility: current.launch_policy === 'anyone' ? 'public' : 'private',
      source: { id: 'apps:' + decision.appId + '/target', revision: decision.targetRevision, digest: decision.targetDigest } });
    assertApps(view && typeof view.then !== 'function', 'app_guide_unavailable', 404);
    const html = renderAppGuide(view);
    checkAccess(session); // Projection cannot outlive a changed actor, policy, or target.
    res.setHeader('Content-Security-Policy', `default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors ${[...origins].join(' ')}; sandbox`);
    res.setHeader('Cache-Control', 'private, no-store');
    res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.setHeader('Content-Length', Buffer.byteLength(html));
    res.statusCode = 200; res.end(req.method === 'HEAD' ? undefined : html);
  }
  async function routeApp(req, res, app) {
    assertApps(!app.missing, 'app_not_found', 404);
    const target=activeTarget(app.id),selected=selectedRuntimeProfile(target?.profile);
    const scopedProfile=selected?scopedAdmissions.require(targetTuple(target)):null;
    const nativeIngress = new URL(req.url || '/', app.appHost.origin);
    if (nativeIngress.pathname === '/_soty/ingress-check' && req.headers['x-soty-ingress-surface'] !== undefined) {
      assertApps(req.method === 'GET' && !nativeIngress.search, 'invalid_app_ingress', 403);
      for (const name of ['x-soty-ingress-surface','x-soty-ingress-app','x-soty-ingress-primary','x-soty-ingress-target'])
        assertApps(headerCount(req, name) === 1, 'invalid_app_ingress', 403);
      assertApps(req.headers['x-soty-ingress-surface'] === 'native' && req.headers['x-soty-ingress-app'] === app.id,
        'invalid_app_ingress', 403);
      assertOrigin(req, app.appHost.origin, false);
      const entry = requireNativeEntry({ db, appId: app.id, origin: app.appHost.origin,
        primaryDigest: req.headers['x-soty-ingress-primary'], nativeDigest: req.headers['x-soty-ingress-target'], scopedAdmission: scopedAdmissions });
      // Native has no second desired binding. Its independent candidate proof
      // was committed in the receipt; the CURRENT primary connector must still
      // be authenticated, online and bound to the exact primary Source target.
      const decision = publications.decideAccess({ domainId: entry.domainId, origin: entry.origin });
      assertApps(decision.policyEpoch === entry.policyEpoch, 'app_native_entry_closed', 403);
      assertRuntimeBinding(decision, { requireReady: true });
      json(res, 200, { schema: 'soty.native-entry-policy.v1', appId: app.id, origin: entry.origin,
        primaryTargetRevision: entry.primaryTargetRevision, primaryTargetDigest: entry.primaryTargetDigest,
        nativeTargetRevision: entry.target.revision, nativeTargetDigest: entry.target.digest,
        policyEpoch: entry.policyEpoch, admissionKeyHash: entry.requestKeyHash, leaseMs: 1000 });
      return;
    }
    const rootWorkspace=selected&&isRootSignedWorkspaceSource(scopedProfile.sourceProfile),frame=rootWorkspace?parseRequestFrame(req):undefined;
    setPolicy(res, app.appHost.origin, selected, rootWorkspace, rootWorkspace && scopedProfile.schema === 'soty.selected-human-embed.v2' ? scopedProfile.nativeOrigin : null);
    if(frame)frameRequests.set(req,frame);
    let path=requestPath(req.url||'/');const frameUrl=new URL(path,app.appHost.origin);
    if(rootWorkspace&&frameUrl.searchParams.has('sotyFrame')){
      assertApps(frameUrl.pathname==='/embed'&&['GET','HEAD'].includes(req.method),'app_frame_invalid',403);
      frameUrl.searchParams.delete('sotyFrame');
      path=frameUrl.pathname+frameUrl.search;
    }
    assertApps(!rootWorkspace||!(['/embed'].includes(frameUrl.pathname)||frameUrl.pathname.startsWith('/api/embed/'))||frame,'app_frame_required',403);
    assertOrigin(req, app.appHost.origin, !['GET', 'HEAD'].includes(req.method));
    const internalUrl = new URL(path, app.appHost.origin);
    assertApps(req.headers['x-soty-boot-check']===undefined||path==='/_soty/session'&&req.method==='GET'
      ||selected&&path==='/api/embed/session-continue'&&req.method==='POST','app_scoped_candidate_route_denied',403);
    if (internalUrl.pathname === '/_soty/relay-identity-config') {
      assertApps(!selected && path === '/_soty/relay-identity-config' && req.method === 'GET', 'app_identity_unavailable', 403);
      if (internalRuntimeHost(app)) sessionFor(req, app, { requireCookie: true });
      json(res, 200, { schema: 'soty.relay-identity-config.v1', shellOrigins: [...origins] }); return;
    }
    if (internalUrl.pathname === '/_soty/ingress-check') {
      // A fixed operator-managed upstream may keep its own host-only cookies.
      // This check grants no identity or arbitrary destination: ingress pins
      // the application and current source digest and rechecks live authority.
      assertApps(req.method === 'GET' && !internalUrl.search, 'invalid_app_ingress', 403);
      const target = activeTarget(app.id);
      assertApps(headerCount(req, 'x-soty-ingress-app') === 1 && req.headers['x-soty-ingress-app'] === app.id
        && headerCount(req, 'x-soty-ingress-target') === 1 && target
        && equalDigest(req.headers['x-soty-ingress-target'], target.digest), 'app_source_changed', 403);
      const session = rootWorkspace?publicFrameSession(req,app):sessionFor(req, app);
      const decision = checkAccess(session, { renewPublic: true });
      assertRuntimeBinding(decision, { requireReady: true });
      res.statusCode = 204; res.end(); return;
    }
    if (internalUrl.pathname === '/_soty/boot' && req.method === 'GET') {
      assertApps([...internalUrl.searchParams.keys()].every(key => key === 'path'||(selected||hostSessions)&&key==='mode') && internalUrl.searchParams.getAll('path').length <= 1
        &&internalUrl.searchParams.getAll('mode').length<=1&&(!internalUrl.searchParams.has('mode')||internalUrl.searchParams.get('mode')==='renew'),'invalid_app_path');
      const recoveryPath = guidePath(internalUrl.searchParams.get('path') || '/'), nonce = pageNonce(), recovery = shellRecoveryFor(app, recoveryPath);
      const html = renderBootPage({ nonce, shellUrl: recovery.url, publicResetPath: recovery.currentSource ? undefined : publicResetFor(app, recoveryPath),scoped:selected,
        renewal:selected&&internalUrl.searchParams.get('mode')==='renew',hostRenewal:!selected&&internalUrl.searchParams.get('mode')==='renew',parentOrigin:scopedProfile?.parentOrigin,parentOrigins:[...origins] });
      setManagedPagePolicy(res, nonce, [...origins]);
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.end(html); return;
    }
    if (path === '/_soty/session' && req.method === 'GET') {
      const check=req.headers['x-soty-boot-check'];
      const candidate=typeof check==='string'?[...sessions.values()].find(value=>value.hostSession&&equalDigest(value.checkDigest,digest(check))):null;
      const session=candidate?checkedHostCandidate(req,app,candidate):typeof check==='string'&&bootCandidates.has(digest(check))?candidateFor(req,app):sessionFor(req,app,{requireCookie:true});
      if (check !== undefined) {
        assertApps(headerCount(req, 'x-soty-boot-check') === 1 && typeof check === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(check)
          && session.checkExpiresAt > now() && equalDigest(session.checkDigest, digest(check)), 'app_session_check_failed', 403);
      }
      if(session.hostSession&&check!==undefined){
        if(!session.hostRenewal&&!session.hostCompatibility&&!session.hostChecked)session.hostSession=hostSessions.activateInitial(session.hostSession);
        session.hostChecked=true;
      }
      json(res, 200, { ok: true, ...(check === undefined ? {} : { sessionCheck: check }) }); return;
    }
    if (path === '/_soty/session' && req.method === 'POST') {
      assertApps(req.headers['x-soty-boot-check']===undefined,'app_scoped_candidate_route_denied',403);
      const body = JSON.parse((await readBounded(req, 2048)).toString('utf8'));
      assertApps(body && typeof body === 'object' && !Array.isArray(body) && Object.keys(body).length === 1
        && typeof body.ticket === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(body.ticket), 'app_ticket_invalid', 403);
      const key = digest(body.ticket), ticket = tickets.get(key); tickets.delete(key);
      assertApps(ticket && ticket.decision.appId === app.id && ticket.decision.domainId === app.appHost.domainId && ticket.decision.origin === canonicalHostOrigin(app), 'app_ticket_invalid', 403);
      checkHolderHost(ticket, app);
      // The request body was asynchronous: only this fresh branded recheck may
      // mint a session. A stale app row or successful launch is not authority.
      const decision = publications.recheckAccess(ticket.decision, { ttlMs: accountSessionMs });
      checkWidgetRuntimeFor({...ticket,decision},'session');
      assertRuntimeBinding(decision, { requireReady: true });
      if(ticket.hostSession){admitHostSession(req,res,ticket,decision);return;}
      if(!selected&&hostBase(req)?.startsWith('h_')){admitHostCompatibility(req,res,ticket,decision,app);return;}
      const frameId=isRootSignedWorkspaceSource(ticket.scopedRecord?.context.sourceProfile)
        ?ticket.frameId??ticket.scopedRecord.context.reference.id:undefined;
      assertApps(!frame||frame===frameId,'app_frame_invalid',403);
      sweepCandidates();const previous=ticket.rebind?ticket.scopedRecord?.session:null,baseCookieKey=primaryKey(req,frameId);
      let session,sessionCheck;
      if(previous?.promotion&&previous.checkExpiresAt>now()&&sessions.get(previous.sessionKey)===previous){
        session=checkCandidate(previous,req,app,frameId);sessionCheck=session.checkToken;
      }else{
        assertApps(sessions.size < 4096, 'apps_sessions_busy', 429);
        const value=secret(),sessionKey=digest(value);sessionCheck=secret();
        let family={head:sessionKey,revision:0,...(frameId?{frameId}:{})},promotion;
        if(ticket.rebind){
          assertApps(ticket.scopedRecord&&bootCandidates.size<256,'app_scoped_candidate_busy',429);
          const base=baseCookieKey?sessions.get(baseCookieKey):null;
          const recovered=ticket.frameRecovery===true&&scopedGateway.isFrameRecovery(ticket.scopedRecord);
          assertApps(!baseCookieKey||!base&&recovered||base&&base.decision.appId===app.id&&base.decision.domainId===app.appHost.domainId&&base.decision.origin===app.appHost.origin
            &&base.family?.head!==null&&base.family.frameId===frameId
            &&JSON.stringify(base.scopedRecord?.context.rootPrincipal)===JSON.stringify(ticket.scopedRecord.context.rootPrincipal)
            &&JSON.stringify(base.scopedRecord?.context.humanPrincipal)===JSON.stringify(ticket.scopedRecord.context.humanPrincipal),'app_scoped_candidate_changed',403);
          family=base?.family??{head:null,revision:0,...(frameId?{frameId}:{})};
          promotion={state:'pending',token:value,baseCookieKey,baseHead:family.head,baseRevision:family.revision};
        }
        session={decision,sessionKey,entryPath:ticket.entryPath,...(ticket.rootInstanceId?{rootInstanceId:ticket.rootInstanceId}:{}),checkDigest:digest(sessionCheck),checkToken:sessionCheck,checkExpiresAt:now()+30_000,family,
          ...(ticket.runtimeHost?{runtimeHost:ticket.runtimeHost}:{}),
          ...(ticket.scopedRecord?{scopedRecord:ticket.scopedRecord}:{}),...(promotion?{promotion}:{})};
        if(ticket.scopedRecord)scopedGateway.attach(ticket.scopedRecord,session);sessions.set(sessionKey,session);
        if(promotion)bootCandidates.set(session.checkDigest,session);
        else res.setHeader('Set-Cookie',sessionCookies(value,session));
      }
      // CHIPS keys this session by both application host and embedding site.
      // A preview embedded in Soty does not depend on unrestricted third-party
      // cookies, and cannot reuse the session from an unrelated top-level site.
      json(res, 200, { ok: true, entryPath: ticket.entryPath, sessionCheck,...(frameId?{frame:frameId}:{}),
        ...(ticket.scopedRecord?{scopedRequestId:ticket.scopedRecord.context.reference.id}:{}),...(ticket.rebind?{renewal:true,renewalRequestId:ticket.renewalRequestId}:{}) }); return;
    }
    if (path === '/_soty/session' && req.method === 'DELETE') {
      assertApps(!internalRuntimeHost(app), 'app_session_required', 401);
      // Reset is an explicit new anonymous entry. It never bypasses the current
      // public policy, and cannot remove a session belonging to another address.
      publications.decideAccess({ domainId: app.appHost.domainId, origin: app.appHost.origin });
      const name = `__Host-${cookieName}`;
      for (const part of String(req.headers.cookie || '').split(';')) {
        const value = part.trim(); if (!value.startsWith(`${name}=`)) continue;
        const token = value.slice(name.length + 1); if (!/^[A-Za-z0-9_-]{43}$/u.test(token)) continue;
        const key = digest(token), session = sessions.get(key);
        if (session && session.decision.appId === app.id && session.decision.domainId === app.appHost.domainId && session.decision.origin === app.appHost.origin) {
          sessions.delete(key);
          retirePrimary(session);
          for (const stream of live.values()) if (stream.session.sessionKey === key) closeStream(stream, 'app_access_revoked');
        }
      }
      const cleared = `${name}=; HttpOnly; Path=/; SameSite=None; Secure; Max-Age=0`;
      res.setHeader('Set-Cookie', [`${cleared}; Partitioned`, cleared]);
      if (!req.readableEnded) { res.shouldKeepAlive = false; res.setHeader('Connection', 'close'); }
      json(res, 200, { ok: true }); return;
    }
    if (internalUrl.pathname === '/_soty/guide') {
      assertApps(path === '/_soty/guide' && ['GET','HEAD'].includes(req.method), 'app_guide_path_invalid', 404);
      assertApps(!selected, 'app_guide_unsupported', 404);
      sendGuide(req, res, app, sessionFor(req, app)); return;
    }
    runtimePath(path);
    const callback=selected&&['/api/embed/callback','/api/embed/complete-link'].includes(internalUrl.pathname);
    if(callback)assertApps(req.method==='GET','app_scoped_callback_invalid',403);
    const callbackRecord=callback?scopedGateway.callback(app.id,path):null;
    const route=selected?embedRoute(scopedProfile,req.method,path):null;
    const candidate=req.headers['x-soty-boot-check']!==undefined;
    assertApps(!candidate||selected&&route?.kind==='auth-continue'&&req.method==='POST','app_scoped_candidate_route_denied',403);
    const identityRequested = req.headers['x-soty-identity-request'] !== undefined;
    let windowKey;
    if (identityRequested) {
      assertApps(headerCount(req, 'x-soty-identity-request') === 1 && req.headers['x-soty-identity-request'] === '1' && !selected,
        'app_identity_request_invalid', 400);
      const grant = req.headers['x-soty-window-grant'];
      assertApps(headerCount(req, 'x-soty-window-grant') === 1 && typeof grant === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(grant),
        'app_identity_window_unavailable', 503);
      windowKey = digest(grant);
    }
    const holder = identityRequested ? relayWindows.get(windowKey) : undefined;
    if (identityRequested) {
      if (!holder || holder.decision.appId !== app.id || holder.decision.domainId !== app.appHost.domainId
        || holder.decision.origin !== canonicalHostOrigin(app) || holder.decision.subject !== 'account'
        || Boolean(holder.runtimeHost) !== internalRuntimeHost(app)
        || holder.runtimeHost && (holder.runtimeHost.origin !== app.appHost.origin || rootWidgetId(holder) !== app.appHost.widgetId)) {
        // This response is generated by Root before a stream or upstream dispatch.
        // Upstream response headers are allowlisted and cannot set this marker.
        res.setHeader('X-Soty-Identity-Window', 'not-dispatched');
        assertApps(false, 'app_identity_window_unavailable', 503);
      }
      checkHolderHost(holder, app);
      checkWidgetRuntimeFor(holder, 'identity');
    }
    const session=identityRequested?{ ...holder, decision: publications.recheckAccess(holder.decision, { ttlMs: accountSessionMs }), windowKey }
      :callbackRecord?callbackRecord.session:candidate?candidateFor(req,app)
      :rootWorkspace&&route?.credentialFree?publicFrameSession(req,app):sessionFor(req,app);
    if(selected){assertApps(session.scopedRecord,'app_scoped_context_closed',403);scopedGateway.context(session.scopedRecord);}
    assertApps(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'].includes(req.method), 'app_method_denied', 405);
    if (req.headers['idempotency-key'] !== undefined) {
      const key = req.headers['idempotency-key'];
      assertApps(headerCount(req, 'idempotency-key') === 1 && typeof key === 'string' && key.length > 0
        && key.length <= 16 * 1024 && !/[\r\n\u0000]/u.test(key), 'app_idempotency_key_invalid', 400);
      // Selected Source uses its own explicit request/receipt protocol. The
      // ordinary relay must never silently lose a write's duplicate key.
      assertApps(!selected, 'app_idempotency_key_unsupported', 503);
      const channel = channels.get(session.decision.route.connectorKey);
      assertApps(channel && channel.ws.readyState === 1, 'app_offline', 503);
      assertApps(channel.bindingVersion === 2 && channel.relayIdempotencyKey === true, 'app_idempotency_key_unavailable', 503);
    }
    const stream = openStream(app, session, { kind: 'http', req, res });
    stream.requestPath=path;
    if (!selected && path === '/guide' && ['GET','HEAD'].includes(req.method)) stream.guideApp = app;
    stream.bootCandidate=candidate;
    if(candidate){stream.candidateApp=app;stream.bootCandidateSession=session;}
    res.once('close', () => closeStream(stream, 'app_client_closed'));
    try {
      const headers = cleanHeaders(req.headers, selected ? 'request' : 'ordinary-request');
      if (selected && req.headers.origin) headers.origin = req.headers.origin;
      let identityWitness;
      if (identityRequested) {
        assertApps(stream.channel.bindingVersion === 2 && stream.channel.relayIdentity === true && typeof signRelayIdentity === 'function',
          'app_identity_unavailable', 503);
        const original = stream.session.decision;
        let timer, onClose;
        try {
          const stopped = new Promise((_, reject) => {
            timer = setTimeout(() => reject(new AppsError('app_identity_unavailable', 503)), identitySignTimeoutMs); timer.unref?.();
            onClose = () => reject(new AppsError('app_access_revoked', 403)); res.once('close', onClose);
            if (stream.closed) onClose();
          });
          identityWitness = await Promise.race([Promise.resolve().then(() => signRelayIdentity({ appId: app.id, accountId: original.actor.accountId,
            method: req.method, path, expiresAt: original.expiresAt })), stopped]);
        } finally { clearTimeout(timer); res.off('close', onClose); }
        const fresh = checkStream(stream);
        assertApps(relayWindows.get(windowKey) === holder && holder.decision.expiresAt > now() && typeof identityWitness === 'string'
          && identityWitness.length > 0 && identityWitness.length <= 8192 && fresh.actor?.accountId === original.actor.accountId
          && fresh.actor?.deviceId === original.actor.deviceId && fresh.policyEpoch === original.policyEpoch
          && fresh.targetRevision === original.targetRevision && fresh.targetDigest === original.targetDigest,
          'app_access_revoked', 403);
      }
      sendStream(stream, { type: 'open', id: stream.id, kind: 'http', appId: app.id, path, method: req.method, headers,
        ...(identityWitness ? { identityWitness } : {}) });
      let count = 0;
      for await (const chunk of req) { count += chunk.length; assertApps(count <= (route?route.requestBytes:LIMITS.requestBytes), 'app_request_too_large', 413); await sendChunks(stream, chunk); }
      sendStream(stream, { type: 'end', id: stream.id });
    } catch (error) { closeStream(stream, [401, 403].includes(error.status) ? 'app_access_revoked' : error.code || 'app_request_failed'); }
  }
  function handleRequest(req, res) {
    if (closed) { setStatusPolicy(res); respondRuntimeFailure(req, res, 503, 'app_server_closed'); return true; }
    const app = appForRequest(req); if (!app) return false;
    // Classification comes from this registry, never an incoming metric label.
    if(app.id){try{onRuntimeRequest({appId:app.id,response:res});}catch{/* Observation cannot change an app response. */}}
    const failure = hostFailure(app);
    if (failure) { setStatusPolicy(res); respondRuntimeFailure(req, res, failure.status, failure.error); return true; }
    void routeApp(req, res, app).catch(error => { if (!res.headersSent) respondAppFailure(req, res, error.status || 400, error.code || 'app_invalid_request'); else res.destroy(); }); return true;
  }
  function voiceSourceSession(owner,scope) {
    assertApps(!closed&&scopedGateway,'voice_source_unavailable',503);assertActor(owner);
    const candidates=[...sessions.values()].filter(session=>{
      const record=session.scopedRecord,p=record?.context.rootPrincipal;
      if(record?.context.appId!==scope.appId||p?.accountId!==owner.accountId||p.deviceId!==owner.deviceId)return false;
      const state=record.sourceSession;
      // Expired anchors are deliberately retained for explicit continuation.
      // They are not a live voice lease and cannot shadow a current ready slot.
      if(record.context.expiresAt<=now()||state?.ready!==true||state.accessExpiresAt<=now()||state.sessionExpiresAt<=now())return false;
      try{return scopedGateway.sourceContext(record)?.ready===true;}
      catch(error){if(['app_scoped_context_closed','scoped_embed_expired'].includes(error?.code))return false;throw error;}
    });
    assertApps(candidates.length===1,'voice_source_session_unavailable',403);
    const session=candidates[0],record=session.scopedRecord,context=scopedGateway.context(record),profile=record.profile;
    assertApps(record.session===session&&sessions.get(session.sessionKey)===session&&session.promotion?.state!=='pending'
      &&profile.schema==='soty.selected-human-embed.v2'&&context.appId===scope.appId
      &&context.rootPrincipal.accountId===owner.accountId&&context.rootPrincipal.deviceId===owner.deviceId
      &&scopedHash(context.target)===scopedHash(scope.target)&&scopedHash(context.sourceProfile)===scopedHash(scope.sourceProfile)
      &&scopedHash(context.resource)===scopedHash(scope.resource),'voice_source_scope_changed',403);
    const decision=checkAccess(session);assertApps(decision.appId===scope.appId,'voice_source_scope_changed',403);
    const runtimeBinding=assertRuntimeBinding(decision,{requireReady:true}),channel=channels.get(decision.route.connectorKey);
    assertApps(channel?.bindingVersion===2&&channel.ws.readyState===1,'voice_source_unavailable',503);
    return {session,record,context,runtimeBinding,channel};
  }
  function withScopedVoiceSourceAuthority({owner,scope},callback) {
    assertApps(typeof callback==='function','voice_source_denied',403);
    // Original scoped authority checks Human + Apps outside the later held Apps
    // transaction; never nest Apps transactions or substitute an owner claim.
    const held=voiceSourceSession(owner,scope);
    const actor=voiceSourceActors.get(held.record);
    assertApps(actor&&actor.accountId===owner.accountId&&actor.deviceId===owner.deviceId&&typeof withHumanSubjectAuthority==='function','voice_source_actor_unavailable',403);
    let entered=false,result;
    // Original Human host port holds the ORIGINAL issued actor's Connect fence.
    // An account/device projection cannot join or replace that actor authority.
    const returned=withHumanSubjectAuthority(scopedSubjectRequest(actor,held.record.profile),human=>{
      assertApps(!entered&&scopedHash(human)===scopedHash(held.context.humanPrincipal),'voice_source_scope_changed',403);entered=true;
      result=appAuthority({actor:owner,appId:scope.appId,mode:'participant',domainId:held.context.entry.domainId,path:'/embed'},snapshot=>{
        assertApps(snapshot.target.revision===scope.target.revision&&snapshot.target.digest===scope.target.digest
          &&snapshot.policyEpoch===held.context.policyEpoch&&snapshot.entry.origin===held.context.entry.origin
          &&sessions.get(held.session.sessionKey)===held.session&&held.record.session===held.session,'voice_source_scope_changed',403);
        return callback();
      });return result;
    });
    assertApps(entered&&returned===result&&!returned?.then,'voice_source_authority_invalid',503);
    const fresh=voiceSourceSession(owner,scope);
    assertApps(fresh.record===held.record&&fresh.session===held.session&&fresh.channel===held.channel
      &&fresh.runtimeBinding===held.runtimeBinding&&scopedHash(fresh.context)===scopedHash(held.context),'voice_source_scope_changed',403);
    return result;
  }
  async function callScopedVoiceSource({scope,path,args,signal,withCurrentOwner}) {
    const deadline=performance.now()+SCOPED_EMBED_LIMITS.callMs;
    assertApps(typeof withCurrentOwner==='function'&&['/api/embed/query','/api/embed/invoke','/api/embed/receipt'].includes(path)
      &&signal instanceof AbortSignal&&!signal.aborted,'voice_source_denied',403);
    assertApps(voiceSourceCalls.size<SCOPED_EMBED_LIMITS.inflight,'voice_source_capacity',429);
    let held,stream;
    const current=()=>withCurrentOwner(owner=>{
      assertApps(!signal.aborted&&performance.now()<deadline,'voice_source_unknown',503);
      const value=voiceSourceSession(owner,scope);
      assertApps(!held||value.record===held.record&&value.session===held.session&&value.channel===held.channel
        &&value.runtimeBinding===held.runtimeBinding&&scopedHash(value.context)===scopedHash(held.context),'voice_source_scope_changed',403);
      assertApps(!signal.aborted&&performance.now()<deadline,'voice_source_unknown',503);held??=value;return true;
    });
    assertApps(current()===true,'voice_source_denied',403);
    const route=embedRoute(held.record.profile,'POST',path),body=Buffer.from(JSON.stringify(args));
    assertApps(body.length<=route.requestBytes,'voice_source_body_bound',413);
    let timer,settled=false,finish,fail,status,headersSent=false,bytes=0;
    const parts=[],headers=new Map();
    const response=new Promise((yes,no)=>{finish=yes;fail=no;});
    const sink=new Writable({write(chunk,_encoding,next){
      bytes+=chunk.length;
      if(bytes>route.responseBytes){next(new AppsError('voice_source_response_bound',502));return;}
      parts.push(Buffer.from(chunk));next();
    }});
    sink.setHeader=(name,value)=>{assertApps(!headersSent,'voice_source_response_changed',502);headers.set(name.toLowerCase(),value);};
    sink.getHeader=name=>headers.get(name.toLowerCase());
    Object.defineProperty(sink,'headersSent',{get:()=>headersSent});
    sink.writeHead=code=>{assertApps(Number.isSafeInteger(code)&&code>=100&&code<=599&&!headersSent,'voice_source_response_changed',502);status=code;headersSent=true;return sink;};
    const cleanup=()=>{clearTimeout(timer);signal.removeEventListener('abort',abort);voiceSourceCalls.delete(sink);};
    const reject=()=>{if(settled)return;settled=true;cleanup();fail(new AppsError('voice_source_unknown',503));};
    const abort=()=>{if(stream&&!stream.closed)closeStream(stream,'app_client_closed');sink.destroy();reject();};
    sink.once('error',reject);sink.once('close',()=>{if(!sink.writableFinished)reject();});
    sink.once('finish',()=>{if(settled)return;try{
      assertApps(!signal.aborted&&current()===true&&Number.isSafeInteger(status),'voice_source_unknown',503);
      assertApps(/^application\/json(?:;|$)/iu.test(headers.get('content-type')??''),'voice_source_response_invalid',502);
      const data=parseSourceJson(new TextDecoder('utf-8',{fatal:true}).decode(Buffer.concat(parts,bytes)));
      settled=true;cleanup();finish(Object.freeze({status,data}));
    }catch{reject();}});
    voiceSourceCalls.add(sink);signal.addEventListener('abort',abort,{once:true});
    timer=setTimeout(abort,Math.max(1,deadline-performance.now()));timer.unref?.();
    try{
      assertApps(current()===true&&!signal.aborted,'voice_source_denied',403);
      const req={method:'POST',url:path,headers:{origin:held.record.profile.embedOrigin,'content-type':'application/json',accept:'application/json'}};
      stream=openStream({id:scope.appId},held.session,{kind:'http',req,res:sink});
      assertApps(stream.channel===held.channel&&stream.runtimeBinding===held.runtimeBinding&&stream.scopedRecord===held.record,'voice_source_scope_changed',403);
      sendStream(stream,{type:'open',id:stream.id,kind:'http',appId:scope.appId,path,method:'POST',headers:req.headers});
      await sendChunks(stream,body);
      assertApps(current()===true&&!signal.aborted,'voice_source_scope_changed',403);
      sendStream(stream,{type:'end',id:stream.id});
    }catch{abort();}
    return response;
  }
  // Exact current selection; never search for another account/device/project slot.
  function scopedSourceTarget(owner,id) {
    assertApps(!closed&&scopedGateway,'context_source_unavailable',503);assertActor(owner);
    const target=activeTarget(id);assertApps(target,'context_source_denied',403);
    const profile=scopedAdmissions.require(targetTuple(target));
    // A headless acquisition uses ONLY the already admitted Native Root-SSO
    // bootstrap. A finite RP/owner login is never manufactured by the agent.
    assertApps(isRootSignedWorkspaceSource(profile.sourceProfile),'context_source_denied',403);
    const domain=db.prepare('SELECT * FROM app_domains WHERE app_id=? AND origin=?').get(id,profile.embedOrigin);
    assertApps(domain,'context_source_denied',403);
    const decision=publications.decideAccess({domainId:domain.id,origin:domain.origin,actor:owner});
    assertRuntimeBinding(decision,{requireReady:true});
    return {target,profile,domain,decision};
  }
  function canOpenScopedSourceContext({owner,appId:id}) {
    try {scopedSourceTarget(owner,appId(id));return true;}
    catch(error) {if([401,403,404].includes(error.status))return false;throw error;}
  }
  async function openScopedSourceContext({owner,appId:id,signal,withCurrentOwner}) {
    id=appId(id);assertActor(owner);
    assertApps(signal instanceof AbortSignal&&!signal.aborted&&typeof withCurrentOwner==='function','context_source_denied',403);
    let scoped;
    const key=digest(JSON.stringify([id,owner.accountId,owner.deviceId]));
    assertApps(!backendSourceAcquisitions.has(key),'context_source_capacity',429);
    backendSourceAcquisitions.add(key);
    try {
      for(const value of [...backendSourceContexts.values()])if(value.record.context.expiresAt<=now())scopedGateway.invalidate(value.record);
      const prior=backendSourceContexts.get(key);
      if(prior&&prior.record.context.expiresAt>now()) {
        withCurrentOwner(current=>withScopedContextSourceAuthority({owner:current,scope:{appId:id},handle:prior.handle},()=>true));
        return Object.freeze({appId:id,handle:prior.handle});
      }
      if(prior)scopedGateway.invalidate(prior.record);
      withCurrentOwner(current=>{
        assertApps(current.accountId===owner.accountId&&current.deviceId===owner.deviceId&&!signal.aborted,'context_source_denied',403);
        const {target,domain,decision}=scopedSourceTarget(current,id);
        assertApps(sessions.size<4096&&backendSourceContexts.size<SCOPED_EMBED_LIMITS.continuations,'apps_sessions_busy',429);
        scoped=scopedGateway.open({actor:current,appId:id,domainId:domain.id,target:targetTuple(target),instanceId:'widget-'+randomUUID()});
        voiceSourceActors.set(scoped.record,current);
        // The existing opaque instance locator isolates Native default selection
        // from main/visible slots. No widget, browser cookie or navigation exists.
        // The connector retains Native cookies privately, exactly as for a slot.
        const sessionKey=digest(secret()),checkToken=secret();
        const session={decision,sessionKey,entryPath:'/embed',checkDigest:digest(checkToken),checkToken,checkExpiresAt:now()+30_000,
          family:{head:sessionKey,revision:0,frameId:scoped.record.context.reference.id},scopedRecord:scoped.record};
        scoped.record.backendSource=true;scopedGateway.attach(scoped.record,session);sessions.set(sessionKey,session);
      });
      assertApps(scoped,'context_source_denied',403);
      const response=await callSelectedSource({scope:{appId:id},handle:scoped.closeHandle,path:'/api/embed/session-continue',method:'POST',
        body:Buffer.from(JSON.stringify({requestId:randomUUID()})),kind:'auth-continue',signal,withCurrentOwner,contextRead:false,bootstrap:true});
      assertApps(response.status===200&&response.data?.ready===true,'context_source_session_unavailable',403);
      withCurrentOwner(current=>withScopedContextSourceAuthority({owner:current,scope:{appId:id},handle:scoped.closeHandle},()=>true));
      backendSourceContexts.set(key,{record:scoped.record,handle:scoped.closeHandle});
      return Object.freeze({appId:id,handle:scoped.closeHandle});
    } catch(error) {if(scoped)scopedGateway.invalidate(scoped.record);throw error;}
    finally {backendSourceAcquisitions.delete(key);}
  }
  function contextSourceSession(owner,scope,handle,{bootstrap=false}={}) {
    assertApps(!closed&&scopedGateway,'context_source_unavailable',503);assertActor(owner);
    const {record,context}=scopedGateway.ownedContext(owner,scope.appId,handle),session=record.session,profile=record.profile;
    assertApps(session&&sessions.get(session.sessionKey)===session&&session.promotion?.state!=='pending'
      &&profile.schema==='soty.selected-human-embed.v2'&&(scopedGateway.sourceContext(record)?.ready===true
        ||bootstrap&&record.backendSource===true&&isRootSignedWorkspaceSource(profile.sourceProfile)),
      'context_source_session_unavailable',403);
    if(scope.target)assertApps(scopedHash(context.target)===scopedHash(scope.target)
      &&scopedHash(context.sourceProfile)===scopedHash(scope.sourceProfile)&&scopedHash(context.resource)===scopedHash(scope.resource),
      'context_source_scope_changed',403);
    const decision=checkAccess(session),runtimeBinding=assertRuntimeBinding(decision,{requireReady:true}),channel=channels.get(decision.route.connectorKey);
    assertApps(channel?.bindingVersion===2&&channel.ws.readyState===1,'context_source_unavailable',503);
    return {session,record,context,runtimeBinding,channel};
  }
  function withScopedContextSourceAuthority({owner,scope,handle},callback) {
    assertApps(typeof callback==='function','context_source_denied',403);
    // Original scoped authority checks Human + Apps outside the later held Apps
    // transaction; never nest Apps transactions or substitute an owner claim.
    const held=contextSourceSession(owner,scope,handle);
    const actor=voiceSourceActors.get(held.record);
    assertApps(actor&&actor.accountId===owner.accountId&&actor.deviceId===owner.deviceId&&typeof withHumanSubjectAuthority==='function','context_source_actor_unavailable',403);
    let entered=false,result;
    // Original Human host port holds the ORIGINAL issued actor's Connect fence.
    // An account/device projection cannot join or replace that actor authority.
    const returned=withHumanSubjectAuthority(scopedSubjectRequest(actor,held.record.profile),human=>{
      assertApps(!entered&&scopedHash(human)===scopedHash(held.context.humanPrincipal),'context_source_scope_changed',403);entered=true;
      result=appAuthority({actor:owner,appId:scope.appId,mode:'participant',domainId:held.context.entry.domainId,path:'/embed'},snapshot=>{
        assertApps(snapshot.target.revision===held.context.target.revision&&snapshot.target.digest===held.context.target.digest
          &&snapshot.policyEpoch===held.context.policyEpoch&&snapshot.entry.origin===held.context.entry.origin
          &&sessions.get(held.session.sessionKey)===held.session&&held.record.session===held.session,'context_source_scope_changed',403);
        return callback(held.context);
      });return result;
    });
    assertApps(entered&&returned===result&&!returned?.then,'context_source_authority_invalid',503);
    const fresh=contextSourceSession(owner,scope,handle);
    assertApps(fresh.record===held.record&&fresh.session===held.session&&fresh.channel===held.channel
      &&fresh.runtimeBinding===held.runtimeBinding&&scopedHash(fresh.context)===scopedHash(held.context),'context_source_scope_changed',403);
    return result;
  }
  async function callScopedContextSource({scope,handle,requestId,signal,withCurrentOwner}) {
    const standard=scope.resource?.selection?.kind==='soty.resource.v1'&&scopedHash(scope.sourceProfile)===scopedHash(STANDARD_SELECTED_SOURCE_V3)
      ||scope.resource?.selection?.kind==='soty.workspace.v1'&&isWorkspaceSource(scope.sourceProfile);
    assertApps(!standard||typeof requestId==='string'&&/^[A-Za-z0-9_.:-]{16,128}$/u.test(requestId),'context_source_denied',403);
    const path=standard?'/api/embed/query':'/api/embed/context',method=standard?'POST':'GET';
    // Fixed read operation, never a caller/model-selected URL, method or payload.
    const body=standard?Buffer.from(JSON.stringify({requestId,input:{operation:'soty.current_context'}})):null;
    return callSelectedSource({scope,handle,path,method,body,kind:'read',signal,withCurrentOwner,contextRead:true});
  }
  /** Host-only Native port. Three existing routes, the ORIGINAL selected
   * handle/cookie/connector slot and the existing finite stream budget only. */
  async function callScopedNativeSource({scope,handle,path,requestId,input,contextRef,signal,withCurrentOwner}) {
    const workspace=scope.resource?.selection?.kind==='soty.workspace.v1'&&isWorkspaceSource(scope.sourceProfile);
    const standard=(scope.resource?.selection?.kind==='soty.resource.v1'
      &&scopedHash(scope.sourceProfile)===scopedHash(STANDARD_SELECTED_SOURCE_V3)||workspace)
      &&['/api/embed/query','/api/embed/invoke','/api/embed/receipt'].includes(path);
    const hive=scope.resource?.selection?.kind==='hive.project.v1'
      &&[HIVE_SELECTED_SOURCE,HIVE_SELECTED_KERNEL_SOURCE].some(pin=>scopedHash(scope.sourceProfile)===scopedHash(pin))
      &&['/api/embed/project','/api/embed/operations','/api/embed/changes'].includes(path);
    assertApps((standard||hive)&&typeof requestId==='string'&&/^[A-Za-z0-9_.:-]{16,128}$/u.test(requestId),'context_source_denied',403);
    const data=scopedCapture(input);let method='POST',body,kind='read';
    assertApps(workspace?/^[A-Za-z0-9_-]{43}$/u.test(contextRef):contextRef===undefined,'context_source_denied',403);
    if(standard){body=Buffer.from(JSON.stringify({requestId,input:data,...(workspace?{contextRef}:{})}));kind=path==='/api/embed/invoke'?'write':'read';}
    else if(path==='/api/embed/operations'){
      assertApps(Object.keys(data).sort().join(',')==='expectedRevision,operations'&&Number.isSafeInteger(data.expectedRevision)&&data.expectedRevision>=1,'context_source_denied',403);
      body=Buffer.from(JSON.stringify({schema:'hive.operations',version:2,requestId,...data}));kind='write';
    }else{
      method='GET';body=null;
      assertApps(path==='/api/embed/project'?Object.keys(data).length===0:Object.keys(data).join(',')==='after'
        &&Number.isSafeInteger(data.after)&&data.after>=0,'context_source_denied',403);
      if(path==='/api/embed/changes')path+='?after='+data.after+'&limit=1';
    }
    return callSelectedSource({scope,handle,path,method,body,kind,signal,withCurrentOwner,contextRead:false});
  }
  async function callSelectedSource({scope,handle,path,method,body,kind,signal,withCurrentOwner,contextRead,bootstrap=false}) {
    const deadline=performance.now()+SCOPED_EMBED_LIMITS.callMs;
    assertApps(typeof withCurrentOwner==='function'
      &&signal instanceof AbortSignal&&!signal.aborted,'context_source_denied',403);
    assertApps(contextSourceCalls.size<SCOPED_EMBED_LIMITS.inflight,'context_source_capacity',429);
    let held,stream;
    const current=()=>withCurrentOwner(owner=>{
      assertApps(!signal.aborted&&performance.now()<deadline,'context_source_unknown',503);
      const value=contextSourceSession(owner,scope,handle,{bootstrap});
      assertApps(!held||value.record===held.record&&value.session===held.session&&value.channel===held.channel
        &&value.runtimeBinding===held.runtimeBinding&&scopedHash(value.context)===scopedHash(held.context),'context_source_scope_changed',403);
      assertApps(!signal.aborted&&performance.now()<deadline,'context_source_unknown',503);held??=value;return true;
    });
    assertApps(current()===true,'context_source_denied',403);
    const route=embedRoute(held.record.profile,method,path);
    assertApps(route.kind===kind&&(body?body.length<=route.requestBytes:route.requestBytes===0),'context_source_route_denied',403);
    let timer,settled=false,finish,fail,status,headersSent=false,bytes=0;
    const parts=[],headers=new Map();
    const response=new Promise((yes,no)=>{finish=yes;fail=no;});
    const sink=new Writable({write(chunk,_encoding,next){
      bytes+=chunk.length;
      if(bytes>Math.min(route.responseBytes,65536)){next(new AppsError('context_source_response_bound',502));return;}
      parts.push(Buffer.from(chunk));next();
    }});
    sink.setHeader=(name,value)=>{assertApps(!headersSent,'context_source_response_changed',502);headers.set(name.toLowerCase(),value);};
    sink.getHeader=name=>headers.get(name.toLowerCase());
    Object.defineProperty(sink,'headersSent',{get:()=>headersSent});
    sink.writeHead=code=>{assertApps(Number.isSafeInteger(code)&&code>=100&&code<=599&&!headersSent,'context_source_response_changed',502);status=code;headersSent=true;return sink;};
    const cleanup=()=>{clearTimeout(timer);signal.removeEventListener('abort',abort);contextSourceCalls.delete(sink);};
    const reject=()=>{if(settled)return;settled=true;cleanup();if(stream&&!stream.closed)closeStream(stream,'app_client_closed');fail(new AppsError('context_source_unknown',503));};
    const abort=()=>{if(stream&&!stream.closed)closeStream(stream,'app_client_closed');sink.destroy();reject();};
    sink.once('error',reject);sink.once('close',()=>{if(!sink.writableFinished)reject();});
    sink.once('finish',()=>{if(settled)return;try{
      assertApps(!signal.aborted&&current()===true&&Number.isSafeInteger(status),'context_source_unknown',503);
      assertApps(/^application\/json(?:;|$)/iu.test(headers.get('content-type')??''),'context_source_response_invalid',502);
      const data=parseSourceJson(new TextDecoder('utf-8',{fatal:true}).decode(Buffer.concat(parts,bytes)));
      settled=true;cleanup();finish(Object.freeze({status,data}));
    }catch{reject();}});
    contextSourceCalls.add(sink);signal.addEventListener('abort',abort,{once:true});
    timer=setTimeout(abort,Math.max(1,deadline-performance.now()));timer.unref?.();
    try{
      assertApps(current()===true&&!signal.aborted,'context_source_denied',403);
      const req={method,url:path,headers:{origin:held.record.profile.embedOrigin,accept:contextRead?'application/json; profile="urn:soty:current-context:v1"':'application/json',
        ...(body?{'content-type':'application/json'}:{})}};
      stream=openStream({id:scope.appId},held.session,{kind:'http',req,res:sink});
      assertApps(stream.channel===held.channel&&stream.runtimeBinding===held.runtimeBinding&&stream.scopedRecord===held.record,'context_source_scope_changed',403);
      sendStream(stream,{type:'open',id:stream.id,kind:'http',appId:scope.appId,path,method,headers:req.headers});
      if(body)await sendChunks(stream,body);
      assertApps(current()===true&&!signal.aborted,'context_source_scope_changed',403);
      sendStream(stream,{type:'end',id:stream.id});
    }catch{abort();}
    return response;
  }
  function openStream(app, session, values) {
    const decision = checkAccess(session, { renewPublic: true });
    const runtimeBinding = assertRuntimeBinding(decision, { requireReady: true });
    const channel = channels.get(decision.route.connectorKey); assertApps(channel && channel.ws.readyState === 1, 'app_offline', 503);
    assertApps(channel.streams.size < LIMITS.streams, 'app_device_busy', 429);
    if (decision.accessBasis === 'public') assertApps([...channel.streams.values()].filter(item => item.session.decision.accessBasis === 'public').length < publicStreams, 'app_device_busy', 429);
    const id = randomBytes(16).toString('hex'), stream = { ...values, id, appId: app.id, channel, runtimeBinding, session: { decision, sessionKey: session.sessionKey, entryPath:session.entryPath,
      ...(session.runtimeHost?{runtimeHost:session.runtimeHost}:{}),
      ...(session.hostSession?{hostSession:session.hostSession}:{}),
      ...(session.hostRenewal?{hostRenewal:session.hostRenewal}:{}),
      ...(session.windowKey?{windowKey:session.windowKey}:{}),
      ...(session.rootInstanceId?{rootInstanceId:session.rootInstanceId}:{}),...(session.scopedRecord?{scopedRecord:session.scopedRecord}:{}) }, scopedRecord:session.scopedRecord, pending: new Map(), sendSeq: 0, recvSeq: 0, received: 0, closed: false, head: false, receiving: false };
    assertApps(!stream.scopedRecord||stream.kind==='http','app_scoped_websocket_denied',403);
    stream.timer = setTimeout(() => closeStream(stream, 'app_response_timeout'), LIMITS.headMs); stream.timer.unref();
    channel.streams.set(id, stream); live.set(id, stream); return stream;
  }
  function sendStream(stream, frame) {
    checkStream(stream);
    if (frame.type === 'open' && stream.channel.bindingVersion === 2) frame = { ...frame, type: 'bound-open', ...runtimeBindings.openPins(stream.runtimeBinding) };
    if(frame.type==='bound-open'&&stream.scopedRecord)frame={...frame,context:scopedGateway.context(stream.scopedRecord)};
    assertApps(send(stream.channel, frame), 'app_offline', 503);
  }
  async function sendChunks(stream, bytes) {
    checkStream(stream);
    // For WS this is the relay's only source writer. It receives already
    // inspected ordered bytes, including its masked control frames.
    for (let offset = 0; offset < bytes.length; offset += CHUNK_BYTES) {
      checkStream(stream); const seq = ++stream.sendSeq;
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => { stream.pending.delete(seq); reject(new AppsError('app_ack_timeout')); }, LIMITS.ackMs); timer.unref();
        stream.pending.set(seq, { resolve, reject, timer });
        try { sendStream(stream, { type: 'data', id: stream.id, seq, data: bytes.subarray(offset, offset + CHUNK_BYTES).toString('base64') }); }
        catch (error) { clearTimeout(timer); stream.pending.delete(seq); reject(error); }
      });
      checkStream(stream);
    }
  }
  async function relayClientBytes(stream, bytes) {
    // A TCP read/upgrade head may exceed one tunnel chunk. Do not let it create
    // concurrent relay input or retain an unbounded queue of pending ACKs.
    for (let offset = 0; offset < bytes.length; offset += CHUNK_BYTES) {
      await stream.relay.clientBytes(bytes.subarray(offset, offset + CHUNK_BYTES));
      checkStream(stream);
    }
  }
  function send(channel, frame) {
    if (closed || !channel || channels.get(channel.key) !== channel || channel.ws.readyState !== 1) return false;
    const payload = JSON.stringify(frame), bytes = Buffer.byteLength(payload, 'utf8');
    if (bytes > FRAME_BYTES || channel.ws.bufferedAmount + bytes > 4 * 1024 * 1024) return false;
    try { channel.ws.send(payload); return true; } catch { return false; }
  }
  function handleUpgrade(req, socket, head) {
    // An upgrade already queued by HTTP may outlive service shutdown/rebind.
    // Refuse before any host-classifier or registry statement touches the DB.
    if (closed) { socket.end('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nCache-Control: no-store\r\nContent-Length: 0\r\n\r\n'); return true; }
    let url;
    try { url = new URL(req.url || '/', 'http://localhost'); }
    catch { socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); return true; }
    const app = appForRequest(req);
    const failure = app && hostFailure(app);
    if (failure) {
      const phrase = { 400: 'Bad Request', 404: 'Not Found', 410: 'Gone', 503: 'Service Unavailable' }[failure.status];
      socket.end(`HTTP/1.1 ${failure.status} ${phrase}\r\nConnection: close\r\nCache-Control: no-store\r\nContent-Length: 0\r\n\r\n`); return true;
    }
    if (url.pathname === '/api/apps/channel' && !app) {
      if (req.headers.origin || url.search || wss.clients.size >= 256) { socket.destroy(); return true; }
      wss.handleUpgrade(req, socket, head, ws => acceptChannel(ws)); return true;
    }
    if (!app) return false;
    let stream;
    try {
      assertApps(!app.missing && req.method === 'GET' && req.headers.upgrade?.toLowerCase() === 'websocket', 'app_upgrade_denied', 403);
      assertApps(req.headers['x-soty-boot-check']===undefined,'app_scoped_candidate_route_denied',403);
      assertOrigin(req, app.appHost.origin, true);
      const path = runtimePath(req.url || '/');
      const session = sessionFor(req, app); stream = openStream(app, session, { kind: 'ws', socket, req, upgradeHead: head });
      socket.pause(); socket.on('error', () => closeStream(stream, 'app_client_closed')); socket.once('close', () => closeStream(stream, 'app_client_closed'));
      const headers = {};
      for (const name of ['sec-websocket-key', 'sec-websocket-version', 'sec-websocket-protocol']) if (typeof req.headers[name] === 'string' && req.headers[name].length < 2048) headers[name] = req.headers[name];
      headers.origin = canonicalHostOrigin(app);
      sendStream(stream, { type: 'open', id: stream.id, kind: 'ws', appId: app.id, path, method: 'GET', headers });
    } catch (error) {
      if (stream) closeStream(stream, error.code || 'app_upgrade_denied');
      else {
        const status = [400, 401, 403, 404, 429, 503].includes(error.status) ? error.status : 403;
        const phrase = { 400: 'Bad Request', 401: 'Unauthorized', 403: 'Forbidden', 404: 'Not Found', 429: 'Too Many Requests', 503: 'Service Unavailable' }[status];
        socket.end(`HTTP/1.1 ${status} ${phrase}\r\nConnection: close\r\nCache-Control: no-store\r\nContent-Length: 0\r\n\r\n`);
      }
    }
    return true;
  }
  function acceptChannel(ws) {
    let channel = null, authenticating = false, admissionClosed = false;
    const terminate = () => { admissionClosed = true; if (channel) runtimeBindings.drop(channel); ws.terminate(); };
    const timer = setTimeout(terminate, 5000); timer.unref();
    ws.on('error', () => {});
    ws.on('pong', () => { if (channel) channel.lastSeenAt = now(); });
    ws.on('message', async (bytes, binary) => {
      let stream;
      try {
        if (closed || admissionClosed || (channel && channels.get(channel.key) !== channel)) return;
        assertApps(!binary && bytes.length <= FRAME_BYTES, 'app_bad_frame'); const frame = JSON.parse(bytes.toString('utf8'));
        assertApps(frame && typeof frame === 'object' && !Array.isArray(frame), 'app_bad_frame');
        if (!channel) {
          assertApps(!authenticating && frame.type === 'auth' && frame.schema === CHANNEL_SCHEMA, 'app_auth_required'); authenticating = true;
          const identity = { linkId: textId(frame.linkId), hostDeviceId: textId(frame.hostDeviceId), connectorId: textId(frame.connectorId) };
          assertApps(await authenticateConnector({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, token: frame.token }), 'app_connector_auth_failed');
          if (closed || admissionClosed || ws.readyState !== 1) return;
          let bindingVersion = 1, relayIdentity = false, relayIdempotencyKey = false, browsePrepared = false, inspectPrepared = false, manifestPrepared = false;
          let runtimeProfiles = ['soty.relay-restricted.v1'];
          if (frame.capabilities !== undefined) {
            const versions = frame.capabilities?.targetBindingVersions;
            assertApps(frame.capabilities && typeof frame.capabilities === 'object' && !Array.isArray(frame.capabilities)
              && Array.isArray(versions) && versions.length > 0 && versions.length <= 8
              && versions.every(value => Number.isSafeInteger(value) && value >= 1) && new Set(versions).size === versions.length
              && (versions.includes(1) || versions.includes(2)), 'app_source_protocol_required');
            bindingVersion = versions.includes(2) ? 2 : 1;
            if (frame.capabilities.relayIdentity !== undefined) {
              assertApps(typeof frame.capabilities.relayIdentity === 'boolean', 'app_source_protocol_required');
              relayIdentity = frame.capabilities.relayIdentity;
            }
            if (frame.capabilities.relayIdempotencyKey !== undefined) {
              assertApps(typeof frame.capabilities.relayIdempotencyKey === 'boolean', 'app_source_protocol_required');
              relayIdempotencyKey = frame.capabilities.relayIdempotencyKey;
            }
            if (frame.capabilities.preparedBrowse !== undefined) {
              assertApps(typeof frame.capabilities.preparedBrowse === 'boolean', 'app_source_protocol_required');
              browsePrepared = frame.capabilities.preparedBrowse;
            }
            if (frame.capabilities.preparedInspect !== undefined) {
              assertApps(typeof frame.capabilities.preparedInspect === 'boolean', 'app_source_protocol_required');
              inspectPrepared = frame.capabilities.preparedInspect;
            }
            if (frame.capabilities.preparedManifest !== undefined) {
              assertApps(typeof frame.capabilities.preparedManifest === 'boolean', 'app_source_protocol_required');
              manifestPrepared = frame.capabilities.preparedManifest;
            }
            if (frame.capabilities.runtimeProfiles !== undefined) {
              const profiles = frame.capabilities.runtimeProfiles;
              assertApps(Array.isArray(profiles) && profiles.length > 0 && profiles.length <= 8
                && profiles.every(value => ['soty.relay-restricted.v1','soty.selected-human-embed.v1','soty.selected-human-embed.v2', ...(allowSelfIpTargets ? [SELF_IP_PROFILE] : [])].includes(value))
                && new Set(profiles).size === profiles.length, 'app_source_protocol_required');
              runtimeProfiles = profiles;
            }
          }
          const key = connectorKey(identity), previous = channels.get(key);
          if (previous) {
            runtimeBindings.drop(previous); preparedBrowse.drop(previous);
            for (const oldStream of previous.streams.values()) closeStream(oldStream, 'app_offline', false);
            previous.ws.terminate();
          }
          channel = { ws, key, identity: Object.freeze(identity), bindingVersion, relayIdentity, relayIdempotencyKey, preparedBrowse: browsePrepared, preparedInspect: inspectPrepared, preparedManifest: manifestPrepared, runtimeProfiles: Object.freeze(runtimeProfiles), ...(bindingVersion === 2 ? { channelId: secret() } : {}),
            authorityWitnesses:0, auth: { linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, token: frame.token }, name: typeof frame.name === 'string' ? frame.name.slice(0, 80) : identity.hostDeviceId,
            claimDigest: '', claimExpiresAt: 0, streams: new Map(), observations: new Map(), lastSeenAt: now() };
          channels.set(key, channel); clearTimeout(timer);
          assertApps(send(channel, { type: 'ready', schema: CHANNEL_SCHEMA, ...(bindingVersion === 2 ? { bindingVersion, channelId: channel.channelId } : {}) }), 'app_offline', 503);
          sync(channel); return;
        }
        channel.lastSeenAt = now();
        assertApps(frame.type !== 'auth', 'app_auth_repeated');
        if (frame.type === 'self-addresses') {
          assertApps(allowSelfIpTargets && channel.bindingVersion === 2 && channel.runtimeProfiles.includes(SELF_IP_PROFILE)
            && Object.keys(frame).sort().join(',') === 'channelId,inventory,type' && frame.channelId === channel.channelId, 'apps_endpoint_inventory_invalid');
          const inventory = normalizeSelfInventory(frame.inventory, { connectorKey: channel.key, channelId: channel.channelId });
          const prior = channel.selfInventory;
          assertApps(!prior || inventory.revision > prior.revision || inventory.revision === prior.revision && inventory.digest === prior.digest, 'apps_endpoint_inventory_invalid');
          channel.selfInventory = prior?.digest === inventory.digest ? prior : inventory; channel.selfInventoryReceivedAt = now();
          if (prior?.digest !== inventory.digest) { runtimeBindings.refreshRejected(channel); sync(channel); }
          return;
        }
        if(frame.type==='scoped-authority-request') {
          assertApps(channel.bindingVersion===2 && frame.channelId===channel.channelId && /^[A-Za-z0-9_-]{43}$/.test(frame.nonce||'')
            && Object.keys(frame).every(key=>['type','channelId','nonce','reference','sourceChallenge'].includes(key))
            && (frame.sourceChallenge===undefined || /^[A-Za-z0-9_-]{43}$/.test(frame.sourceChallenge)), 'app_bad_frame');
          let signingChannel;
          try {
            assertApps(scopedGateway,'app_scoped_admission_required',503);
            const boundChannel=channel, identity=channel.identity;
            if(frame.sourceChallenge!==undefined) {
              // Match the installed connector's 32 pending authority reads;
              // several widgets share this channel. No queue or new setting.
              assertApps(boundChannel.authorityWitnesses<LIMITS.streams,'app_scoped_authority_busy',503);
              boundChannel.authorityWitnesses++;signingChannel=boundChannel;
            }
            const result=frame.sourceChallenge===undefined ? {context:scopedGateway.read(frame.reference,identity)}
              : await scopedGateway.readWitness(frame.reference,identity,frame.sourceChallenge);
            if(frame.sourceChallenge!==undefined) {
              assertApps(channel===boundChannel && channels.get(boundChannel.key)===boundChannel && boundChannel.ws.readyState===1
                && await authenticateConnector(boundChannel.auth),'app_scoped_authority_unavailable',403);
              assertApps(channel===boundChannel && channels.get(boundChannel.key)===boundChannel && boundChannel.ws.readyState===1
                && scopedHash(scopedGateway.read(frame.reference,identity))===scopedHash(result.context),'app_scoped_authority_unavailable',403);
            }
            send(boundChannel,{type:'scoped-authority-result',channelId:boundChannel.channelId,nonce:frame.nonce,ok:true,...result});
          } catch(error) {send(channel,{type:'scoped-authority-result',channelId:channel.channelId,nonce:frame.nonce,ok:false,status:error.status===403||error.status===401?403:503,code:'app_scoped_authority_unavailable'});}
          finally {if(signingChannel)signingChannel.authorityWitnesses--;}
          return;
        }
        if (preparedBrowse.handle(channel, frame) || runtimeBindings.handleFrame(channel, frame)) return;
        assertApps(!['binding-ack', 'binding-rejected', 'bound-observation', 'target-prepared', 'target-rejected', 'sync', 'open', 'bound-open', 'binding-set', 'binding-remove', 'target-prepare', 'ready'].includes(frame.type), 'app_bad_frame');
        if (frame.type === 'claim') {
          assertApps(/^[a-f0-9]{64}$/u.test(frame.claimDigest || ''), 'app_claim_digest_invalid'); channel.claimDigest = frame.claimDigest; channel.claimExpiresAt = now() + 5 * 60_000;
          send(channel, { type: 'claim-ready', claimDigest: frame.claimDigest }); return;
        }
        if (frame.type === 'observation') {
          assertApps(channel.bindingVersion === 1, 'app_bad_observation');
          const app = row(appId(frame.appId)), target = app && activeTarget(app.id);
          assertApps(target?.connector_key === channel.key && ['ready', 'stopped'].includes(frame.state), 'app_bad_observation');
          if (app.state !== 'enabled' || bindingFloor(app.id) !== 1) return;
          // These pins associate a legacy report with current configuration;
          // they are not an acknowledgement or attestation from connector v1.
          channel.observations.set(app.id, { state: frame.state, at: now(), targetRevision: target.revision, targetDigest: target.digest }); return;
        }
        stream = channel.streams.get(frame.id); if (!stream) return;
        checkStream(stream);
        if (frame.type === 'ack') {
          const item = stream.pending.get(frame.seq); assertApps(item, 'app_bad_ack'); stream.pending.delete(frame.seq); clearTimeout(item.timer); item.resolve(); return;
        }
        if (frame.type === 'cancel') { closeStream(stream, ['app_stopped', 'app_response_timeout'].includes(frame.error) ? frame.error : 'app_upstream_failed', false); return; }
        if (frame.type === 'head') {
          checkWidgetRuntimeFor(stream.session,'response');
          assertApps(!stream.head && Number.isInteger(frame.status) && frame.status >= 100 && frame.status <= 599, 'app_bad_head');
          assertApps(stream.kind === 'ws' || frame.status >= 200, 'app_bad_head');
          stream.head = true;
          if (stream.kind === 'http' && stream.guideApp && frame.status === 404 && !stream.scopedRecord) {
            sendGuide(stream.req, stream.res, stream.guideApp, stream.session);
            closeStream(stream, 'app_stream_complete'); return;
          }
          if (stream.kind === 'ws') {
            assertApps(frame.status === 101, 'app_upgrade_failed'); const headers = cleanHeaders(frame.headers, 'upgrade');
            assertApps(headers['sec-websocket-accept'] && !headers['sec-websocket-extensions'], 'app_upgrade_failed');
            checkStream(stream);
            stream.relay = createWebSocketRelay({
              toClient: bytes => writeChunk(stream.socket, bytes),
              toSource: bytes => sendChunks(stream, bytes),
              assertActive: () => { checkStream(stream); },
              onFailure: error => closeStream(stream, error.code || 'app_websocket_failed'),
              timing: webSocketTiming,
            });
            // Keep the original head deadline until HTTP101 is actually written.
            // A concurrent source data callback can hold one bounded chunk behind
            // this gate, but cannot emit WS bytes or ACK ahead of the handshake.
            stream.upgradeReady = writeChunk(stream.socket, Buffer.from(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n${Object.entries(headers).map(([k, v]) => `${k}: ${v}\r\n`).join('')}\r\n`)).then(() => {
              checkStream(stream); clearTimeout(stream.timer); stream.timer = null;
              stream.relay.start();
            });
            await stream.upgradeReady;
            checkStream(stream);
            void (async () => {
              try {
                let initial = stream.upgradeHead; stream.upgradeHead = null;
                if (initial.length) await relayClientBytes(stream, initial);
                initial = null;
                for await (const chunk of stream.socket) await relayClientBytes(stream, chunk);
                // An upgraded TCP socket may remain half-open after EOF. A
                // WebSocket cannot resume its input, so release its quota and
                // source immediately instead of waiting for a future write.
                closeStream(stream, 'app_client_closed');
              } catch (error) { closeStream(stream, error.code || 'app_client_closed'); }
            })();
          } else {
            clearTimeout(stream.timer); stream.timer = setTimeout(() => closeStream(stream, 'app_idle_timeout'), LIMITS.idleMs); stream.timer.unref();
            for (const [key, value] of Object.entries(cleanHeaders(frame.headers, 'response'))) stream.res.setHeader(key, value);
            if(stream.scopedRecord) {
              const profile=stream.scopedRecord.profile,routeDefinition=embedRoute(profile,stream.req.method,stream.requestPath??stream.req.url),route=routeDefinition.kind;
              if(routeDefinition.credentialFree&&frame.headers?.['retry-after']!==undefined){assertApps(frame.status===503&&frame.headers['retry-after']==='1','app_bad_head');stream.res.setHeader('Retry-After','1');}
              if(frame.auth!==undefined) {
                assertApps(frame.auth?.kind==='start'?route==='auth-start'&&[200,302].includes(frame.status):frame.auth?.kind==='cancel'?route==='auth-start'&&frame.status===200:frame.auth?.kind==='continued'?route==='auth-continue'&&frame.status===200:frame.auth?.kind==='completion'&&route==='auth-callback'&&frame.status===200,'app_scoped_auth_invalid',403);
                // Candidate routing/CAS is checked before recording readiness.
                // A losing or retired candidate cannot leave a ready projection.
                if(stream.bootCandidate&&frame.auth.kind==='continued')candidateFor(stream.req,stream.candidateApp);
                scopedGateway.captureHead(stream.scopedRecord,frame.auth);
                if(stream.bootCandidate&&frame.auth.kind==='continued'&&frame.auth.ack.ready===true)promoteCandidate(stream);
              }
              const sourcePolicy=frame.headers?.['content-security-policy'];
              if(sourcePolicy){assertApps(typeof sourcePolicy==='string'&&sourcePolicy.length<=16384&&!/[\r\n\0]/.test(sourcePolicy),'app_bad_head');
                stream.res.setHeader('Content-Security-Policy',[stream.res.getHeader('Content-Security-Policy'),sourcePolicy]);}
              if(frame.location) {
                const location=new URL(frame.location,profile.embedOrigin);
                if(location.origin===profile.embedOrigin)embedRoute(profile,'GET',location.pathname+location.search);
                else if(profile.schema==='soty.selected-human-embed.v2'&&route==='auth-start'&&location.origin===profile.nativeOrigin)
                  selectedNativeHandoff(profile,location.href);
                else assertApps(route==='auth-start'&&location.origin===new URL(profile.issuer).origin&&location.pathname==='/human-identity/authorize'
                  && location.searchParams.get('client_id')===profile.clientId&&location.searchParams.get('redirect_uri')===profile.embedOrigin+'/api/embed/callback'
                  && location.searchParams.get('response_type')==='code'&&location.searchParams.get('code_challenge_method')==='S256','app_scoped_redirect_denied',403);
                stream.res.setHeader('Location',location.href);
              }
            } else {assertApps(frame.auth===undefined,'app_bad_head');if (frame.location) stream.res.setHeader('Location', requestPath(frame.location));}
            stream.res.writeHead(frame.status);
          }
          return;
        }
        if (frame.type === 'data') {
          assertApps(stream.head && !stream.receiving && frame.seq === stream.recvSeq + 1 && typeof frame.data === 'string' && frame.data.length <= CHUNK_BYTES * 4 / 3 + 4 && /^[A-Za-z0-9+/]*={0,2}$/u.test(frame.data), 'app_bad_data');
          const chunk = Buffer.from(frame.data, 'base64'); assertApps(chunk.length <= CHUNK_BYTES, 'app_bad_data');
          stream.received += chunk.length;
          const responseLimit=stream.scopedRecord?embedRoute(stream.scopedRecord.profile,stream.req.method,stream.requestPath??stream.req.url).responseBytes:LIMITS.responseBytes;
          if (stream.kind !== 'ws' && stream.received > responseLimit) { closeStream(stream, 'app_response_too_large'); return; }
          stream.receiving = true; stream.recvSeq = frame.seq;
          if (stream.kind === 'ws') {
            await stream.upgradeReady;
            checkStream(stream);
            await stream.relay.sourceBytes(chunk);
          } else {
            stream.timer.refresh();
            await writeChunk(stream.res, chunk);
          }
          stream.receiving = false;
          sendStream(stream, { type: 'ack', id: stream.id, seq: frame.seq }); return;
        }
        if (frame.type === 'end') {
          assertApps(stream.head && !stream.receiving, 'app_bad_end');
          if (stream.kind === 'ws') stream.socket.end(); else stream.res.end();
          closeStream(stream, 'app_stream_complete', false); return;
        }
        throw new AppsError('app_bad_frame');
      } catch (error) {
        // Once a frame belongs to an admitted stream, failures are local to it.
        // An expired lease or a bad upstream response cannot evict neighbours.
        // Unattributed framing/authentication faults still terminate the channel.
        if (stream) closeStream(stream, error.code || 'app_upstream_failed');
        else terminate();
      }
    });
    ws.once('close', () => {
      admissionClosed = true; clearTimeout(timer); if (!channel) return;
      runtimeBindings.drop(channel); preparedBrowse.drop(channel);
      if (channels.get(channel.key) === channel) channels.delete(channel.key);
      for (const stream of channel.streams.values()) closeStream(stream, 'app_offline', false);
    });
  }
  function allowsTlsDomain(domain) {
    return hostClassifier.allowsTlsDomain(domain);
  }
  return { operations, execute, registerCreationJob, registerPreparedApp, creationJobApp, inspectPrepared: preparedBrowse.inspect, readPreparedManifest: preparedBrowse.readManifest, publicCatalog, sourcePreparationExtension, endpointRegistrationExtension,
    hostRuntimeStats(){assertApps(!closed,'apps_closed',503);return{online:[...channels.values()].filter(channel=>channel.ws.readyState===1).length};},
    runtimeCapabilities({authentication=false}={}) { return { schema: 'soty.apps-runtime-capabilities.v1', channelSchema: CHANNEL_SCHEMA, targetBindingVersions: [1, 2],
      runtimeProfiles: ['soty.relay-restricted.v1', 'soty.selected-human-embed.v1', 'soty.selected-human-embed.v2', ...(allowSelfIpTargets ? [SELF_IP_PROFILE] : [])],...(authentication?{authentication:authenticationCapability}:{}),
      ...(allowSelfIpTargets ? { selfIpTargetSchema: ENDPOINT_SCHEMA } : {}) }; },
    coverExtension, handleRequest, handleUpgrade, invalidateAccess, invalidateWidget, invalidateConnector, resolveOwnedDevice, isUnownedConnector, allowsTlsDomain,
    withScopedVoiceSourceAuthority,callScopedVoiceSource,withScopedContextSourceAuthority,callScopedContextSource,callScopedNativeSource,
    canOpenScopedSourceContext,openScopedSourceContext,
    withAppAuthority: appAuthority,
    withWidgetSourceAuthority:widgetSourceAuthority,
    withPublicCanvasAuthority:publicCanvasAuthority,
    withPublicAppAuthority(request,callback){
      assertApps(request&&typeof request==='object'&&!Array.isArray(request)&&Object.keys(request).length===1&&Object.hasOwn(request,'appId')&&typeof callback==='function','invalid_arguments');
      const id=appId(request.appId);assertApps(!closed,'apps_closed',503);
      return withAuthorityFence(()=>transaction(()=>{
        const app=row(id),policy=db.prepare('SELECT * FROM app_publications WHERE app_id=?').get(id);
        assertApps(app?.state==='enabled'&&policy?.owner_account_id===app.owner_account_id&&policy.launch_policy==='anyone'&&policy.listed===1,'apps_access_denied',403);
        const audience=readAppAudience(db,id);assertApps(audience===null||audience==='public','apps_access_denied',403);
        const address=db.prepare(`SELECT d.id,d.origin FROM app_publication_domains p
          JOIN app_domains d ON d.id=p.domain_id AND d.app_id=p.app_id AND d.owner_account_id=p.owner_account_id
          JOIN app_domain_zones z ON z.id=d.zone_id AND z.kind='named'
          WHERE p.app_id=? AND p.owner_account_id=? AND d.role='alias' AND d.state='bound' ORDER BY d.created_at,d.id LIMIT 1`).get(id,app.owner_account_id);
        assertApps(address,'apps_access_denied',403);
        const decision=publications.decideAccess({domainId:address.id,origin:address.origin});
        assertApps(decision.subject==='public'&&decision.accessBasis==='public','apps_access_denied',403);
        return synchronous(callback(Object.freeze({appId:id,ownerId:app.owner_account_id,appRevision:app.revision,policyEpoch:policy.policy_epoch,title:app.name,
          visibility:'public',audience:'public',listed:true,canManage:false,
          target:Object.freeze({revision:decision.targetRevision,digest:decision.targetDigest,profile:decision.profile}),
          entry:Object.freeze({appId:id,domainId:address.id,origin:address.origin,path:runtimePath(decision.route.entryPath)})})),'apps_async_authority');
      }));
    },
    withInvitationTarget({appId:id,actor},callback){
      appId(id);if(actor!==null)assertActor(actor);
      return withAuthorityFence(()=>transaction(()=>{const app=row(id);assertApps(app?.state==='enabled','invitation_unavailable',404);
        return synchronous(callback(Object.freeze({appId:id,kind:'app',ownerId:app.owner_account_id,title:app.name})),'apps_async_authority');}),actor??undefined);
    },
    invitationOwnedTargets(actor){assertActor(actor);return db.prepare("SELECT id FROM local_apps WHERE owner_account_id=? AND state='enabled'").all(actor.accountId).map(app=>({kind:'app',id:app.id}));},
    withReviewedAppAuthority(request, callback) {
      return appAuthority({ ...request, mode: 'owner' }, authority => {
        const { appId, ownerId, accountId, appRevision, policyEpoch, target, visibility, grants } = authority, prior = reviewedPublicationFrame;
        reviewedPublicationFrame = publicationReconciliationSnapshot(authority);
        try { return callback(Object.freeze({ appId, ownerId, accountId, appRevision, policyEpoch, target, visibility, grants })); }
        finally { reviewedPublicationFrame = prior; }
      });
    },
    withPublicationReconciliationAuthority(request,callback) {
      return appAuthority({...request,mode:'owner'},authority=>synchronous(callback(publicationReconciliationSnapshot(authority)),'apps_async_authority'));
    },
    currentPublicationReconciliationAuthority() {
      assertApps(reviewedPublicationFrame && db.isTransaction, 'apps_publication_authority_unavailable', 500);
      return reviewedPublicationFrame;
    },
    policy: Object.freeze({
      decideAccess(value) { assertApps(!closed, 'apps_closed', 503); return publications.decideAccess(value); },
      recheckAccess(value, options) { assertApps(!closed, 'apps_closed', 503); return publications.recheckAccess(value, options); },
    }),
    classifyHost: hostClassifier.classifyHost,
    frameSources() {
      return [...new Set(db.prepare('SELECT scheme,suffix,port FROM app_domain_zones').all()
        .map(zone => `${zone.scheme}://*.${zone.suffix}${zone.port ? `:${zone.port}` : ''}`))];
    },
    configured: Boolean(template || readNamedOrigins(db).length), origins: template ? [new URL(template.replace('{appId}', 'app-00000000000000000000000000000000')).origin] : [],
    close() { if (closed) return; scopedGateway?.close();closed = true; covers?.close(); discussions.close(); sources.close(); runtimeBindings.close(); preparedBrowse.close(); clearInterval(auditTimer); clearInterval(heartbeatTimer); clearInterval(connectorAuditTimer); unsubscribe?.(); for (const channel of channels.values()) channel.ws.terminate(); for (const stream of live.values()) closeStream(stream, 'app_server_closed', false); tickets.clear(); sessions.clear(); bootCandidates.clear(); wss.close(); db.close(); },
  };
}
function discussionAuthorLabel(actor) {
  const value = actor?.label;
  return typeof value === 'string' && value.length > 0 && value.length <= 80 && value.isWellFormed()
    && value === value.normalize('NFC').trim() && !/[\u0000-\u001f\u007f]/u.test(value) ? value : 'Участник';
}
function exact(value, allowed) { assertApps(Object.keys(value).every(key => allowed.includes(key)), 'unexpected_argument'); }
function assertOrigin(req, origin, required) {
  assertApps(headerCount(req, 'origin') <= 1 && (req.headers.origin === origin || (!required && req.headers.origin === undefined)), 'app_origin_denied', 403);
}
function headerCount(req, name) { let count = 0; for (let i = 0; i < (req.rawHeaders?.length || 0); i += 2) if (req.rawHeaders[i].toLowerCase() === name) count++; return count; }
const pageNonce = () => randomBytes(16).toString('base64');
// Match the normal managed Boot viewer set, captured before any admission or
// storage failure. An invalid/non-web configured set gets no embedded fallback.
function captureBootRefusalOrigins(origins) {
  const values = [...origins];
  const trusted = values.length > 0 && values.every(value => {
    if (typeof value !== 'string') return false;
    try { const url = new URL(value); return ['https:', 'http:'].includes(url.protocol) && url.origin === value; }
    catch { return false; }
  });
  return Object.freeze(trusted ? values : []);
}
function setManagedPagePolicy(res, nonce, frameOrigins = []) {
  res.setHeader('Content-Security-Policy', `default-src 'none'; script-src 'nonce-${nonce}'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors ${frameOrigins.length ? frameOrigins.join(' ') : "'none'"}; sandbox allow-scripts allow-same-origin`);
  res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer'); res.setHeader('Cache-Control', 'no-store');
}
function validateTemplate(value, origins) {
  const template = normalizeLegacyTemplate(value);
  if (!template) return '';
  const probe = new URL(template.replace('{appId}', 'app-00000000000000000000000000000000'));
  assertApps(!origins.has(probe.origin), 'apps_origin_not_isolated'); return template;
}
function json(res, status, body) { if (res.destroyed || res.writableEnded) return; res.statusCode = status; res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(body)); }
function respondFailure(req, res, status, error, page = {}) {
  // A rejected upload may still have unread bytes. Mark this HTTP connection
  // non-reusable before ending the response: exiting its async body iterator
  // destroys the request, so advertising keep-alive would strand the next GET.
  if (!req.readableEnded && !res.headersSent) { res.shouldKeepAlive = false; res.setHeader('Connection', 'close'); }
  if (!String(req.headers.accept || '').includes('text/html')) {
    const message = error === 'app_identity_window_unavailable' ? 'Не удалось подтвердить вход. Откройте приложение заново или проверьте вход в Соты.'
      : error === 'app_idempotency_key_unavailable' ? 'Обновите коннектор на устройстве приложения и повторите проверку.'
      : error === 'app_idempotency_key_unsupported' ? 'Этот режим приложения не поддерживает передачу ключа операции.' : undefined;
    json(res, status, { ok: false, error, ...(message ? { message } : {}) }); return;
  }
  const nonce = pageNonce(); setManagedPagePolicy(res, nonce, page.frameOrigins);
  res.statusCode = status; res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.setHeader('Cache-Control', 'no-store');
  res.end(renderStatusPage({ error, nonce, shellUrl: page.shellUrl, publicResetPath: page.publicResetPath }));
}
async function readBounded(stream, limit) { const parts = []; let count = 0; for await (const chunk of stream) { count += chunk.length; assertApps(count <= limit, 'app_body_too_large', 413); parts.push(chunk); } return Buffer.concat(parts); }
function writeChunk(stream, chunk) { return new Promise((resolve, reject) => { if (stream.destroyed) return reject(new AppsError('app_client_closed')); stream.write(chunk, error => error ? reject(error) : resolve()); }); }
