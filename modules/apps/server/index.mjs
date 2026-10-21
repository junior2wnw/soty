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
import { createDomainRegistry, domainOperations, readNamedOrigins } from './domains.mjs';
import { createPublicationRegistry, publicationOperations } from './publications.mjs';
import { normalizeLegacyTemplate, normalizeNamedAppZone, normalizeDomainLimits, validateNamedOrigins } from './domain-policy.mjs';
import { createHostClassifier } from './hosts.mjs';
import { renderBootPage, renderStatusPage } from './runtime-pages.mjs';
import { describeSourceObservation } from './source-observation.mjs';
import { createAppInspection } from './inspection.mjs';
import { createSourceRegistry } from './sources.mjs';
import { createRuntimeBindings } from './runtime-bindings.mjs';
import { createLaunchPath } from './launch-path.mjs';
import { createEngagementEntryResolver } from './engagement-access.mjs';
import { createSavedRegistry, savedOperations } from './saved.mjs';
import { createDiscussionRegistry, discussionOperations } from './discussions.mjs';
import { createEngagementTransaction } from './engagement-transaction.mjs';
import { createAppDirectory, directoryOperations } from './directory.mjs';
import { createAppAuthorityPort } from './authority-port.mjs';
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

export const operations = new Set(['apps.devices', 'apps.claim', 'apps.list', 'apps.catalog', 'apps.register', 'apps.update', 'apps.revoke', 'apps.launch', 'apps.scoped.close', 'apps.entry.get', 'apps.inspect', 'apps.source.promote', 'apps.source.history', ...domainOperations, ...publicationOperations, ...savedOperations, ...discussionOperations, ...directoryOperations]);
const cookieName = 'soty_app_session';
const accountSessionMs = 3_600_000, publicLeaseMs = 30_000, publicStreams = 24;
const secret = () => randomBytes(32).toString('base64url');
const digest = value => createHash('sha256').update(value).digest('hex');
const equalDigest = (a, b) => typeof a === 'string' && typeof b === 'string' && /^[a-f0-9]{64}$/u.test(a) && /^[a-f0-9]{64}$/u.test(b) && timingSafeEqual(Buffer.from(a, 'hex'), Buffer.from(b, 'hex'));

export function createAppsService({ dataDir = 'data', databasePath = join(dataDir, 'apps', 'registry.sqlite'), appOriginTemplate = '', namedAppZone = '', namedCanonical = false, retainedNamedAppZones = [], domainLimits = {}, validateNamedZone, shellOrigins = [], allowShellZoneRoot = false, actorActive = () => false,
  canAccessCommunity = () => false, isGroupAdmin = () => false, activeCommunityIds, subscribeMembership, withAuthorityFence: withWorldAuthorityFence, withCurrentContacts,
  readCommunityAuthority, discussionLimits, webSocketLiveness, authenticateConnector = async () => false, now = Date.now, blockedPorts = [], connectorAuthCheckMs = 10_000, accessAuditMs = 10_000,
  allowScopedEmbedMigration = false, allowSelectedResourceMigration = false, allowAudienceMigration = false, allowCoverMigration = false,
  coverDecoder, coverLimits, scopedEmbedProfiles = [], withHumanSubjectAuthority, signSourceAuthority, readAppGuide } = {}) {
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
    migrateAppsSchema(db, { legacyTemplate: template, now, allowScopedEmbedMigration, allowSelectedResourceMigration, allowAudienceMigration,
      allowCoverMigration: allowCoverMigration || schema === 'empty' && coverDecoder !== undefined });
    publications = createPublicationRegistry({ db, now, assertActor, canUse, withContactAuthority, onChanged: event => invalidateAccess({ appId: event.appId }) });
    domains = createDomainRegistry({ db, now, assertActor, legacyTemplate: template, namedAppZone: namedZone, namedCanonical, retainedNamedAppZones: retainedZones, domainLimits: limits,
      shellOrigins: [...origins], validateNamedZone, allowShellZoneRoot, onRetireInTransaction: publications.retireInTransaction,
      onPolicyChanged: publications.notifyChanged });
    db.exec('PRAGMA journal_mode=WAL;');
  } catch (error) { db.close(); throw error; }
  const hostClassifier = createHostClassifier({ db, shellOrigins: [...origins], allowShellZoneRoot });
  const scopedAdmissions = createScopedAdmissionRegistry({ db, profiles: scopedEmbedProfiles, clock: now });
  const channels = new Map(), tickets = new Map(), sessions = new Map(), live = new Map();
  const voiceSourceCalls=new Set(),contextSourceCalls=new Set();
  const voiceSourceActors=new WeakMap();
  const backendSourceContexts=new Map();
  const backendSourceAcquisitions=new Set();
  let inspection, saved, discussions, directory, entryRead, appAuthority, scopedGateway, covers;
  const scopedRenewals=new Map();
  const bootCandidates=new Map();
  try {
    inspection = createAppInspection({ db, assertActor, domains, publications, inspectSource, inspectBinding, inspectIntegrations, inspectLaunchEntry, now,
      shellOrigin: [...origins][0], nameClaimsEnabled: Boolean(namedZone), namedAppZone: namedZone });
    const resolveEntry = createEngagementEntryResolver({ db, assertActor, publications, inspectSource });
    appAuthority = createAppAuthorityPort({ db, assertActor, withAuthorityFence, resolveEntry,requireScopedTarget:target=>scopedAdmissions.require(target) });
    if(scopedEmbedProfiles.length && typeof withHumanSubjectAuthority==='function') scopedGateway=createScopedGateway({
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
      exact(args, ['appId', 'domainId', 'path', 'current']);
      assertApps(args.current === undefined || args.current === true && args.domainId === undefined && args.path === undefined,
        'invalid_app_current_entry');
      const requested = { appId: appId(args.appId), ...(args.domainId === undefined ? {} : { domainId: textId(args.domainId) }),
        ...(args.path === undefined ? {} : { path: guideLaunchPath(args.path).entryPath }) };
      return readEntryTransaction(actor, captured => {
        // Explicit recovery asks for a locator for the currently admitted Source.
        // It never rewrites an exact saved/deep link or opens a scoped context.
        if (args.current === true) {
          const app = row(requested.appId), target = activeTarget(requested.appId);
          assertApps(app?.state === 'enabled' && target, 'app_unavailable', 404);
          const preferred = selectedRuntimeProfile(target.profile) ? inspectLaunchEntry({
            app: { id: app.id, ownerAccountId: app.owner_account_id, state: app.state },
            target: { appId: app.id, revision: target.revision, digest: target.digest, profile: target.profile,
              connectorKey: target.connector_key, port: target.port, entryPath: target.entry_path },
          }, captured) : undefined;
          assertApps(!selectedRuntimeProfile(target.profile) || preferred, 'app_unavailable', 404);
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
    directory = createAppDirectory({ db, assertActor, withAuthorityFence, activeCommunityIds, currentContactIds, canUse, resolveEntry });
    discussions = createDiscussionRegistry({ db, now, assertActor, withAuthorityFence, resolveEntry, canUse, isCurrentFriend,
      readCommunityAuthority(actor, ownerAccountId, relevantCommunityIds) {
        assertApps(typeof readCommunityAuthority === 'function', 'apps_authority_fence_required', 503);
        return readCommunityAuthority(actor, ownerAccountId, relevantCommunityIds);
      },
      authorLabel: discussionAuthorLabel, limits: discussionLimits });
    if (coverDecoder !== undefined && COVER_APP_FORMATS[inspectAppsSchema(db)]) covers = createAppCoverAdapter({ db, assertActor, withAuthorityFence, resolveEntry,
      requireScopedTarget: target => scopedAdmissions.require(target), decoder: coverDecoder, clock: now, limits: coverLimits });
  } catch (error) { db.close(); throw error; }
  const wss = new WebSocketServer({ noServer: true, maxPayload: FRAME_BYTES, perMessageDeflate: false });
  let closed = false;
  const coverExtension = covers ? Object.freeze({ operations: new Set(appCoverOperations), async executeAsync({ op, args = {}, actor }) {
    assertApps(args && typeof args === 'object' && !Array.isArray(args) && Object.hasOwn(args, 'expectedAccountId'), 'apps_cover_account_required');
    return covers.execute({ op, actor, args: authenticatedArgs(actor,args) });
  } }) : null;
  const row = id => db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  const binding = key => db.prepare('SELECT * FROM app_devices WHERE connector_key=?').get(key);
  const activeTarget = id => db.prepare(`SELECT t.* FROM app_publications p
    JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision AND t.owner_account_id=p.owner_account_id
    WHERE p.app_id=?`).get(id);
  const targetTuple = target => ({appId:target.app_id,revision:target.revision,ownerAccountId:target.owner_account_id,
    connectorKey:target.connector_key,port:target.port,entryPath:target.entry_path,profile:target.profile,digest:target.digest});
  const runtimeBindings = createRuntimeBindings({ channels, send, now, blockedPorts,
    scopedTarget(target, { candidate }) {
      assertApps(typeof withHumanSubjectAuthority === 'function', 'app_scoped_human_required', 503);
      if (candidate) scopedAdmissions.verifyCandidate(target); else scopedAdmissions.require(target);
      return true;
    },
    onBindingInvalidated(channel, id) {
      for (const stream of channel.streams.values()) if (stream.appId === id) closeStream(stream, 'app_source_changed');
    } });
  const sources = createSourceRegistry({ db, now, assertActor, publications, blockedPorts,
    scopedAdmission: scopedAdmissions,
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
  const sourcePreparationExtension = Object.freeze({ operations: new Set(['apps.source.prepare','apps.scoped.context','apps.scoped.renew']),
    async executeAsync({ op, args = {}, actor }) {
      if(op==='apps.scoped.renew'){
        args=authenticatedArgs(actor,args);exact(args,['appId','handle','requestId','shortSessionHandoff']);const id=appId(args.appId);
        assertApps(args.shortSessionHandoff===undefined||args.shortSessionHandoff===true,'app_scoped_renew_invalid',403);
        assertApps(scopedGateway&&typeof args.handle==='string'&&/^[A-Za-z0-9_-]{43}$/.test(args.handle)
          &&typeof args.requestId==='string'&&/^[A-Za-z0-9_-]{16,128}$/.test(args.requestId),'app_scoped_renew_invalid',403);
        for(const [key,value]of scopedRenewals)if(value.record.context.expiresAt<=now())scopedRenewals.delete(key);
        const key=digest(id+'\0'+args.requestId),intent=digest(JSON.stringify([args.handle,args.shortSessionHandoff===true])),prior=scopedRenewals.get(key);
        assertActor(actor);
        let issued;
        if(prior){assertApps(prior.intent===intent&&prior.accountId===actor.accountId&&prior.deviceId===actor.deviceId,'app_scoped_renew_conflict',409);
          scopedGateway.context(prior.record);issued=prior;
        }else{
          assertApps(scopedRenewals.size<256,'app_scoped_renew_busy',429);
          const {record:old,context:first,sourceOnly,shortSession}=scopedGateway.ownedAttempt(actor,id,args.handle);
          assertApps(!args.shortSessionHandoff||shortSession===true,'app_scoped_short_session_required',403);
          if(!sourceOnly){assertApps(old?.session&&sessions.get(old.session.sessionKey)===old.session,'app_scoped_context_closed',403);
            const oldDecision=publications.recheckAccess(old.session.decision);assertRuntimeBinding(oldDecision,{requireReady:true});}
          await Promise.resolve();assertActor(actor);const {record,context:fresh,shortSession:freshShort}=scopedGateway.ownedAttempt(actor,id,args.handle);
          assertApps(record===old&&JSON.stringify(first)===JSON.stringify(fresh)&&(!args.shortSessionHandoff||freshShort===true),'app_scoped_context_closed',403);
          const domain=db.prepare('SELECT * FROM app_domains WHERE app_id=? AND id=?').get(id,first.entry.domainId);
          assertApps(domain&&domain.origin===first.entry.origin,'app_scoped_context_closed',403);
          const decision=publications.decideAccess({domainId:domain.id,origin:domain.origin,actor});assertRuntimeBinding(decision,{requireReady:true});
          const scoped=scopedGateway.open({actor,appId:id,domainId:domain.id,target:targetTuple(activeTarget(id)),instanceId:first.instanceId}),next=scoped.record.context;
          voiceSourceActors.set(scoped.record,actor);
          const same=args.shortSessionHandoff===true?scopedRenewalBindingDigest(next)===scopedRenewalBindingDigest(first)
            :next.instanceId===first.instanceId&&(sourceOnly?next.appId===first.appId&&decision.profile==='soty.selected-human-embed.v2'
            &&['rootPrincipal','humanPrincipal','sourceProfile','resource'].every(field=>JSON.stringify(next[field])===JSON.stringify(first[field]))
            :['profileDigest','appId','policyEpoch'].every(field=>next[field]===first[field])
              &&['rootPrincipal','humanPrincipal','sourceProfile','resource','target','entry'].every(field=>JSON.stringify(next[field])===JSON.stringify(first[field])));
          if(!same){scopedGateway.invalidate(scoped.record);assertApps(false,'app_scoped_context_closed',403);}
          const previousSession=old?.session??[...sessions.values()].find(value=>value.scopedRecord?.context.reference.id===first.reference.id);
          const frameId=isRootSignedWorkspaceSource(first.sourceProfile)?previousSession?.family?.frameId:undefined;
          if(isRootSignedWorkspaceSource(first.sourceProfile)&&!frameId){scopedGateway.invalidate(scoped.record);assertApps(false,'app_scoped_context_closed',403);}
          issued={record:scoped.record,handle:scoped.closeHandle,decision,intent,accountId:actor.accountId,deviceId:actor.deviceId,frameId,
            ...(args.shortSessionHandoff?{bindingDigest:scopedRenewalBindingDigest(first)}:{})};scopedRenewals.set(key,issued);
        }
        assertActor(actor);scopedGateway.context(issued.record);const decision=publications.recheckAccess(issued.decision);assertRuntimeBinding(decision,{requireReady:true});
        assertApps(tickets.size<4096,'apps_launch_busy',429);const ticket=secret();tickets.set(digest(ticket),{decision,entryPath:'/embed',scopedRecord:issued.record,rebind:true,renewalRequestId:args.requestId,frameId:issued.frameId});
        return{launchUrl:`${decision.origin}/_soty/boot?path=/embed&mode=renew#${ticket}`,expiresAt:decision.expiresAt,
          runtimeProfile:decision.profile,scopedCloseHandle:issued.handle,scopedSlotExpiresAt:issued.record.context.expiresAt,scopedSource:{...issued.record.context.sourceProfile},
          scopedRenewalRequestId:args.requestId,...(issued.bindingDigest?{scopedRenewalBindingDigest:issued.bindingDigest}:{}),
          entry:{appId:id,domainId:decision.domainId,origin:decision.origin,path:'/embed'}};
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
      bindingVersion: online ? channel.bindingVersion : null };
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
      if (!stream.res.headersSent) respondAppFailure(stream.req, stream.res, error === 'app_access_revoked' ? 403 : error === 'app_offline' ? 503 : 502, error, false);
      else stream.res.destroy();
    }
  }
  function checkAccess(holder, { renewPublic = false } = {}) {
    // Refresh only an unexpired anonymous lease. Account sessions, including
    // signed public visitors, keep their original absolute deadline.
    holder.decision = publications.recheckAccess(holder.decision,
      renewPublic && holder.decision.subject === 'public' ? { ttlMs: publicLeaseMs } : {});
    assertRuntimeBinding(holder.decision);
    return holder.decision;
  }
  function checkStream(stream) {
    assertApps(!stream.closed, 'app_stream_closed', 502);
    try {
      if(stream.scopedRecord)scopedGateway.context(stream.scopedRecord);
      const decision = checkAccess(stream.session, { renewPublic: true });
      assertApps(channels.get(stream.channel.key) === stream.channel && stream.channel.ws.readyState === 1, 'app_offline', 503);
      if (stream.channel.bindingVersion === 2) {
        runtimeBindings.assertBindingCurrent(stream.channel, stream.runtimeBinding);
        assertApps(runtimeBindings.requireBinding(stream.channel, decision) === stream.runtimeBinding, 'app_binding_pending', 503);
      }
      return decision;
    }
    catch (error) { closeStream(stream, 'app_access_revoked'); throw error; }
  }
  function retainAccess(holder) {
    // Expiry, rights, epoch and persistent binding floor are authority. A
    // temporarily connected older runtime is only transport unavailability;
    // it must not destroy this session before a compatible device returns.
    try { holder.decision = publications.recheckAccess(holder.decision); return true; } catch { return false; }
  }
  function invalidateAccess(filter = {}) {
    for (const [key, item] of tickets) if (matches(item, filter) && !retainAccess(item)) {tickets.delete(key);if(item.scopedRecord)scopedGateway.invalidate(item.scopedRecord);}
    for (const [key, item] of sessions) if (matches(item, filter) && !retainAccess(item)) {sessions.delete(key);if(item.scopedRecord)scopedGateway.invalidate(item.scopedRecord);}
    for (const stream of live.values()) if (matches(stream.session, filter)) { try { checkStream(stream); } catch {} }
  }
  function invalidateConnector({ hostDeviceId, connectorId } = {}) {
    for (const channel of channels.values()) if ((!hostDeviceId || channel.identity.hostDeviceId === hostDeviceId) && (!connectorId || channel.identity.connectorId === connectorId)) {
      channels.delete(channel.key); runtimeBindings.drop(channel);
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
        WHERE t.connector_key=? AND a.state='enabled'`).all(channel.key).map(target => ({
          appId: target.app_id, revision: target.revision, digest: target.digest, ownerAccountId: target.owner_account_id,
          connectorKey: target.connector_key, port: target.port, entryPath: target.entry_path, profile: target.profile,
        }));
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
  function execute(request) { return withContactAuthority(request.actor, () => executeCurrent(request)); }
  function executeCurrent({ op, args = {}, actor }) {
    if (directoryOperations.includes(op)) assertApps(Object.hasOwn(args, 'expectedAccountId'), 'apps_directory_account_required');
    args = authenticatedArgs(actor, args); assertApps(operations.has(op), 'unsupported_operation');
    if (directoryOperations.includes(op)) return directory.execute({ op, actor, args });
    if (savedOperations.has(op)) return saved.execute({ op, actor, args });
    if (discussionOperations.has(op)) return discussions.execute({ op, actor, args });
    if (op === 'apps.source.promote' || op === 'apps.source.history') return sources.execute({ op, actor, args });
    if (domainOperations.has(op)) return domains.execute({ actor, op, args });
    if (publicationOperations.has(op)) return publications.execute({ actor, op, args });
    if (op === 'apps.inspect') return inspection.read(actor, args);
    if (op === 'apps.entry.get') return entryRead(actor, args);
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
      const audienceCandidates = hasAudienceSchema(db);
      const candidates = typeof activeCommunityIds === 'function' ? db.prepare(`SELECT * FROM local_apps WHERE id IN (
        SELECT id FROM local_apps WHERE owner_account_id=? UNION
        SELECT app_id FROM local_app_grants WHERE kind='account' AND principal_id=?
          ${audienceCandidates ? 'AND NOT EXISTS(SELECT 1 FROM app_audiences mode WHERE mode.app_id=local_app_grants.app_id)' : ''} UNION
        SELECT app_id FROM local_app_grants WHERE kind='community' AND principal_id IN (SELECT value FROM json_each(?))
          ${audienceCandidates ? 'AND NOT EXISTS(SELECT 1 FROM app_audiences mode WHERE mode.app_id=local_app_grants.app_id)' : ''}
        ${audienceCandidates ? `UNION SELECT a.id FROM local_apps a JOIN app_audiences mode ON mode.app_id=a.id
          WHERE mode.audience='friends' AND a.owner_account_id IN(SELECT value FROM json_each(?))` : ''}) ORDER BY created_at`)
        .all(actor.accountId, actor.accountId, JSON.stringify(activeCommunityIds(actor.accountId)),
          ...(audienceCandidates ? [JSON.stringify(currentContactIds(actor))] : [])) : db.prepare('SELECT * FROM local_apps ORDER BY created_at').all();
      return { configured: Boolean(template || readNamedOrigins(db).length), apps: candidates.filter(app =>
        (canUse(actor, app) || app.owner_account_id === actor.accountId) && (!community || JSON.parse(app.grants_json).communityIds.includes(community))).map(app => publicApp(app, actor)) };
    }
    if (op === 'apps.register') {
      exact(args, ['hostDeviceId', 'connectorId', 'name', 'port', 'entryPath', 'grants']);
      const device = db.prepare('SELECT * FROM app_devices WHERE owner_account_id=?').all(actor.accountId).find(item => {
        const identity = JSON.parse(item.identity_json); return identity.hostDeviceId === args.hostDeviceId && identity.connectorId === args.connectorId;
      });
      assertApps(device, 'apps_device_not_owned', 403);
      const name = appName(args.name), port = appPort(args.port, blockedPorts), entryPath = requestPath(args.entryPath ?? '/'), grants = cleanGrants(args.grants);
      assertApps(!entryPath.startsWith('/_soty/'), 'reserved_app_path'); assertGrants(actor, grants);
      const id = transaction(() => {
        assertActor(actor); assertApps(binding(device.connector_key)?.owner_account_id === actor.accountId, 'apps_device_not_owned', 403); assertGrants(actor, grants);
        const existing = db.prepare(`SELECT a.*,t.entry_path AS current_entry_path FROM local_apps a
          JOIN app_publications p ON p.app_id=a.id AND p.owner_account_id=a.owner_account_id
          JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision AND t.owner_account_id=p.owner_account_id
          WHERE t.connector_key=? AND t.port=? AND a.state='enabled'`).get(device.connector_key, port);
        if (existing) {
          assertApps(existing.owner_account_id === actor.accountId && existing.name === name && existing.current_entry_path === entryPath
            && existing.grants_json === JSON.stringify(grants), 'app_port_already_registered', 409);
          publications.execute({ actor, op: 'apps.publication.get', args: { appId: existing.id } });
          return existing.id;
        }
        assertApps(db.prepare('SELECT count(*) AS n FROM local_apps WHERE owner_account_id=?').get(actor.accountId).n < 100, 'apps_limit_reached', 429);
        const id = `app-${randomBytes(16).toString('hex')}`, timestamp = now();
        db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, actor.accountId, device.connector_key, name, port, entryPath, JSON.stringify(grants), 'enabled', 1, timestamp, timestamp);
        persistGrants(id, grants); domains.ensureCanonicalForApp(row(id)); publications.initForApp(row(id));
        return id;
      });
      sync(channels.get(device.connector_key)); return { app: publicApp(row(id), actor) };
    }
    const id = appId(args.appId), app = row(id);
    if(op==='apps.scoped.close') {
      exact(args,['appId','handle']);assertApps(scopedGateway,'app_scoped_context_closed',403);
      return scopedGateway.abandon(actor,id,args.handle);
    }
    if (op === 'apps.launch') {
      exact(args, ['appId', 'domainId', 'path', 'instanceId']);
      assertApps(args.instanceId === undefined || typeof args.instanceId === 'string' && /^(?:main|widget-[a-f0-9-]{36})$/u.test(args.instanceId), 'app_instance_invalid', 400);
      assertApps(app?.state === 'enabled', 'apps_access_denied', 403);
      const domain = args.domainId === undefined
        ? db.prepare("SELECT * FROM app_domains WHERE app_id=? AND role='canonical'").get(id)
        : db.prepare('SELECT * FROM app_domains WHERE app_id=? AND id=?').get(id, textId(args.domainId));
      assertApps(domain, args.domainId === undefined ? 'apps_origin_not_configured' : 'apps_access_denied', args.domainId === undefined ? 503 : 403);
      const decision = publications.decideAccess({ domainId: domain.id, origin: domain.origin, actor });
      assertRuntimeBinding(decision, { requireReady: true });
      // Saved main/widget root entries follow only an explicitly admitted Root-SSO workspace.
      const savedRootEntry = args.path === '/' && decision.route.entryPath === '/embed' && selectedRuntimeProfile(decision.profile)
        && isRootSignedWorkspaceSource(scopedAdmissions.require(targetTuple(activeTarget(id))).sourceProfile);
      if (args.path === '/_soty/guide') assertApps(!selectedRuntimeProfile(decision.profile), 'app_guide_unsupported', 404);
      const { entryPath, bootPath } = guideLaunchPath(savedRootEntry ? '/embed' : args.path ?? decision.route.entryPath);
      assertApps(channels.has(decision.route.connectorKey), 'app_offline', 503);
      assertApps(tickets.size < 4096, 'apps_launch_busy', 429);
      const ticket = secret();
      let scoped;
      if(selectedRuntimeProfile(decision.profile)) {
        assertApps(scopedGateway && entryPath==='/embed','app_scoped_admission_required',503);
        scoped=scopedGateway.open({actor,appId:id,domainId:domain.id,target:targetTuple(activeTarget(id)),instanceId:args.instanceId});
        voiceSourceActors.set(scoped.record,actor);
      }
      tickets.set(digest(ticket), { decision, entryPath,...(scoped?{scopedRecord:scoped.record}:{}) });
      return { launchUrl: `${domain.origin}${bootPath}#${ticket}`, expiresAt: decision.expiresAt,
        ...(scoped?{runtimeProfile:decision.profile,scopedCloseHandle:scoped.closeHandle,scopedSlotExpiresAt:scoped.record.context.expiresAt,
          scopedSource:{...scoped.record.context.sourceProfile}}:{}),
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
    if (!['canonical', 'alias'].includes(host.kind)) return { hostStatus: host.kind };
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
  function shellUrlFor(app, path) {
    const url = new URL('/', [...origins][0]);
    url.hash = `launch/${app.appHost.appId}/${app.appHost.domainId}${path === undefined ? '' : '?' + new URLSearchParams({ path: guidePath(path) })}`;
    return url.href;
  }
  function publicResetFor(app, path) {
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
          page = { shellUrl: shellUrlFor(app), frameOrigins: [...origins] };
        }
      }
      if (path) {
        const publicResetPath = publicResetFor(app, path), shellUrl = shellUrlFor(app, path);
        page = { shellUrl, publicResetPath, frameOrigins: [...origins] };
        const accessFailure = ['app_session_required', 'apps_access_denied', 'app_access_revoked', 'app_access_expired', 'app_access_changed', 'apps_authentication_required'].includes(error);
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
    if(p.state==='pending'){session.family.head=session.sessionKey;session.family.revision++;p.state='promoted';}
    stream.res.setHeader('Set-Cookie',sessionCookies(p.token,session));
  }
  function sessionFor(req, app, { requireCookie = false } = {}) {
    const key=primaryKey(req);
    if (key===null) {
      assertApps(!requireCookie && app.appHost.kind === 'alias', 'app_session_required', 401);
      return { decision: publications.decideAccess({ domainId: app.appHost.domainId, origin: app.appHost.origin }) };
    }
    const session = sessions.get(key);
    assertApps(session && session.decision.appId === app.id && session.decision.domainId === app.appHost.domainId && session.decision.origin === app.appHost.origin, 'apps_access_denied', 403);
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
      assertApps([...internalUrl.searchParams.keys()].every(key => key === 'path'||selected&&key==='mode') && internalUrl.searchParams.getAll('path').length <= 1
        &&internalUrl.searchParams.getAll('mode').length<=1&&(!internalUrl.searchParams.has('mode')||internalUrl.searchParams.get('mode')==='renew'),'invalid_app_path');
      const recoveryPath = guidePath(internalUrl.searchParams.get('path') || '/'), nonce = pageNonce();
      const html = renderBootPage({ nonce, shellUrl: shellUrlFor(app, recoveryPath), publicResetPath: publicResetFor(app, recoveryPath),scoped:selected,
        renewal:selected&&internalUrl.searchParams.get('mode')==='renew',parentOrigin:scopedProfile?.parentOrigin,parentOrigins:[...origins] });
      setManagedPagePolicy(res, nonce, [...origins]);
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.end(html); return;
    }
    if (path === '/_soty/session' && req.method === 'GET') {
      const check=req.headers['x-soty-boot-check'];
      const session=typeof check==='string'&&bootCandidates.has(digest(check))?candidateFor(req,app):sessionFor(req,app,{requireCookie:true});
      if (check !== undefined) {
        assertApps(headerCount(req, 'x-soty-boot-check') === 1 && typeof check === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(check)
          && session.checkExpiresAt > now() && equalDigest(session.checkDigest, digest(check)), 'app_session_check_failed', 403);
      }
      json(res, 200, { ok: true, ...(check === undefined ? {} : { sessionCheck: check }) }); return;
    }
    if (path === '/_soty/session' && req.method === 'POST') {
      assertApps(req.headers['x-soty-boot-check']===undefined,'app_scoped_candidate_route_denied',403);
      const body = JSON.parse((await readBounded(req, 2048)).toString('utf8'));
      assertApps(body && typeof body === 'object' && !Array.isArray(body) && Object.keys(body).length === 1
        && typeof body.ticket === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(body.ticket), 'app_ticket_invalid', 403);
      const key = digest(body.ticket), ticket = tickets.get(key); tickets.delete(key);
      assertApps(ticket && ticket.decision.appId === app.id && ticket.decision.domainId === app.appHost.domainId && ticket.decision.origin === app.appHost.origin, 'app_ticket_invalid', 403);
      // The request body was asynchronous: only this fresh branded recheck may
      // mint a session. A stale app row or successful launch is not authority.
      const decision = publications.recheckAccess(ticket.decision, { ttlMs: accountSessionMs });
      assertRuntimeBinding(decision, { requireReady: true });
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
          assertApps(!baseCookieKey||base&&base.decision.appId===app.id&&base.decision.domainId===app.appHost.domainId&&base.decision.origin===app.appHost.origin
            &&base.family?.head!==null&&base.family.frameId===frameId
            &&JSON.stringify(base.scopedRecord?.context.rootPrincipal)===JSON.stringify(ticket.scopedRecord.context.rootPrincipal)
            &&JSON.stringify(base.scopedRecord?.context.humanPrincipal)===JSON.stringify(ticket.scopedRecord.context.humanPrincipal),'app_scoped_candidate_changed',403);
          family=base?.family??{head:null,revision:0,...(frameId?{frameId}:{})};
          promotion={state:'pending',token:value,baseCookieKey,baseHead:family.head,baseRevision:family.revision};
        }
        session={decision,sessionKey,entryPath:ticket.entryPath,checkDigest:digest(sessionCheck),checkToken:sessionCheck,checkExpiresAt:now()+30_000,family,
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
    const session=callbackRecord?callbackRecord.session:candidate?candidateFor(req,app)
      :rootWorkspace&&route?.credentialFree?publicFrameSession(req,app):sessionFor(req,app);
    if(selected){assertApps(session.scopedRecord,'app_scoped_context_closed',403);scopedGateway.context(session.scopedRecord);}
    assertApps(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'].includes(req.method), 'app_method_denied', 405);
    const stream = openStream(app, session, { kind: 'http', req, res });
    stream.requestPath=path;
    if (!selected && path === '/guide' && ['GET','HEAD'].includes(req.method)) stream.guideApp = app;
    stream.bootCandidate=candidate;
    if(candidate){stream.candidateApp=app;stream.bootCandidateSession=session;}
    res.once('close', () => closeStream(stream, 'app_client_closed'));
    try {
      const headers=cleanHeaders(req.headers);if(selected&&req.headers.origin)headers.origin=req.headers.origin;
      sendStream(stream, { type: 'open', id: stream.id, kind: 'http', appId: app.id, path, method: req.method, headers });
      let count = 0;
      for await (const chunk of req) { count += chunk.length; assertApps(count <= (route?route.requestBytes:LIMITS.requestBytes), 'app_request_too_large', 413); await sendChunks(stream, chunk); }
      sendStream(stream, { type: 'end', id: stream.id });
    } catch (error) { closeStream(stream, error.code || 'app_request_failed'); }
  }
  function handleRequest(req, res) {
    if (closed) { setStatusPolicy(res); respondRuntimeFailure(req, res, 503, 'app_server_closed'); return true; }
    const app = appForRequest(req); if (!app) return false;
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
    const id = randomBytes(16).toString('hex'), stream = { ...values, id, appId: app.id, channel, runtimeBinding, session: { decision, sessionKey: session.sessionKey }, scopedRecord:session.scopedRecord, pending: new Map(), sendSeq: 0, recvSeq: 0, received: 0, closed: false, head: false, receiving: false };
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
      headers.origin = app.appHost.origin;
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
          let bindingVersion = 1;
          let runtimeProfiles = ['soty.relay-restricted.v1'];
          if (frame.capabilities !== undefined) {
            const versions = frame.capabilities?.targetBindingVersions;
            assertApps(frame.capabilities && typeof frame.capabilities === 'object' && !Array.isArray(frame.capabilities)
              && Array.isArray(versions) && versions.length > 0 && versions.length <= 8
              && versions.every(value => Number.isSafeInteger(value) && value >= 1) && new Set(versions).size === versions.length
              && (versions.includes(1) || versions.includes(2)), 'app_source_protocol_required');
            bindingVersion = versions.includes(2) ? 2 : 1;
            if (frame.capabilities.runtimeProfiles !== undefined) {
              const profiles = frame.capabilities.runtimeProfiles;
              assertApps(Array.isArray(profiles) && profiles.length > 0 && profiles.length <= 8
                && profiles.every(value => ['soty.relay-restricted.v1','soty.selected-human-embed.v1','soty.selected-human-embed.v2'].includes(value))
                && new Set(profiles).size === profiles.length, 'app_source_protocol_required');
              runtimeProfiles = profiles;
            }
          }
          const key = connectorKey(identity), previous = channels.get(key);
          if (previous) {
            runtimeBindings.drop(previous);
            for (const oldStream of previous.streams.values()) closeStream(oldStream, 'app_offline', false);
            previous.ws.terminate();
          }
          channel = { ws, key, identity: Object.freeze(identity), bindingVersion, runtimeProfiles: Object.freeze(runtimeProfiles), ...(bindingVersion === 2 ? { channelId: secret() } : {}),
            authorityWitnesses:0, auth: { linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, token: frame.token }, name: typeof frame.name === 'string' ? frame.name.slice(0, 80) : identity.hostDeviceId,
            claimDigest: '', claimExpiresAt: 0, streams: new Map(), observations: new Map(), lastSeenAt: now() };
          channels.set(key, channel); clearTimeout(timer);
          assertApps(send(channel, { type: 'ready', schema: CHANNEL_SCHEMA, ...(bindingVersion === 2 ? { bindingVersion, channelId: channel.channelId } : {}) }), 'app_offline', 503);
          sync(channel); return;
        }
        channel.lastSeenAt = now();
        assertApps(frame.type !== 'auth', 'app_auth_repeated');
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
        if (runtimeBindings.handleFrame(channel, frame)) return;
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
      runtimeBindings.drop(channel);
      if (channels.get(channel.key) === channel) channels.delete(channel.key);
      for (const stream of channel.streams.values()) closeStream(stream, 'app_offline', false);
    });
  }
  function allowsTlsDomain(domain) {
    return hostClassifier.allowsTlsDomain(domain);
  }
  return { operations, execute, publicCatalog, sourcePreparationExtension, coverExtension, handleRequest, handleUpgrade, invalidateAccess, invalidateConnector, resolveOwnedDevice, allowsTlsDomain,
    withScopedVoiceSourceAuthority,callScopedVoiceSource,withScopedContextSourceAuthority,callScopedContextSource,callScopedNativeSource,
    canOpenScopedSourceContext,openScopedSourceContext,
    withAppAuthority: appAuthority,
    withReviewedAppAuthority(request, callback) {
      return appAuthority({ ...request, mode: 'owner' }, ({ appId, ownerId, accountId, appRevision, policyEpoch, target, visibility, grants }) =>
        callback(Object.freeze({ appId, ownerId, accountId, appRevision, policyEpoch, target, visibility, grants })));
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
    close() { if (closed) return; scopedGateway?.close();closed = true; covers?.close(); discussions.close(); sources.close(); runtimeBindings.close(); clearInterval(auditTimer); clearInterval(heartbeatTimer); clearInterval(connectorAuditTimer); unsubscribe?.(); for (const channel of channels.values()) channel.ws.terminate(); for (const stream of live.values()) closeStream(stream, 'app_server_closed', false); tickets.clear(); sessions.clear(); bootCandidates.clear(); wss.close(); db.close(); },
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
  if (!String(req.headers.accept || '').includes('text/html')) { json(res, status, { ok: false, error }); return; }
  const nonce = pageNonce(); setManagedPagePolicy(res, nonce, page.frameOrigins);
  res.statusCode = status; res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.setHeader('Cache-Control', 'no-store');
  res.end(renderStatusPage({ error, nonce, shellUrl: page.shellUrl, publicResetPath: page.publicResetPath }));
}
async function readBounded(stream, limit) { const parts = []; let count = 0; for await (const chunk of stream) { count += chunk.length; assertApps(count <= limit, 'app_body_too_large', 413); parts.push(chunk); } return Buffer.concat(parts); }
function writeChunk(stream, chunk) { return new Promise((resolve, reject) => { if (stream.destroyed) return reject(new AppsError('app_client_closed')); stream.write(chunk, error => error ? reject(error) : resolve()); }); }
