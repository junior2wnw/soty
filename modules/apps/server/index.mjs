import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { WebSocketServer } from 'ws';
import { AppsError, assertApps, textId, appId, appName, appPort, requestPath, runtimePath, cleanGrants, connectorKey, cleanHeaders, createWebSocketLimiter, CHANNEL_SCHEMA, CHUNK_BYTES, FRAME_BYTES, LIMITS } from './protocol.mjs';
import { migrateAppsSchema, inspectAppsSchema, requiredBindingVersion } from './schema.mjs';
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

export const operations = new Set(['apps.devices', 'apps.claim', 'apps.list', 'apps.register', 'apps.update', 'apps.revoke', 'apps.launch', 'apps.inspect', 'apps.source.promote', 'apps.source.history', ...domainOperations, ...publicationOperations, ...savedOperations, ...discussionOperations]);
const cookieName = 'soty_app_session';
const accountSessionMs = 3_600_000, publicLeaseMs = 30_000, publicStreams = 24;
const secret = () => randomBytes(32).toString('base64url');
const digest = value => createHash('sha256').update(value).digest('hex');
const equalDigest = (a, b) => typeof a === 'string' && typeof b === 'string' && /^[a-f0-9]{64}$/u.test(a) && /^[a-f0-9]{64}$/u.test(b) && timingSafeEqual(Buffer.from(a, 'hex'), Buffer.from(b, 'hex'));

export function createAppsService({ dataDir = 'data', databasePath = join(dataDir, 'apps', 'registry.sqlite'), appOriginTemplate = '', namedAppZone = '', domainLimits = {}, validateNamedZone, shellOrigins = [], actorActive = () => false,
  canAccessCommunity = () => false, isGroupAdmin = () => false, activeCommunityIds, subscribeMembership, withAuthorityFence,
  readCommunityAuthority, discussionLimits, authenticateConnector = async () => false, now = Date.now, blockedPorts = [], connectorAuthCheckMs = 10_000, accessAuditMs = 10_000 } = {}) {
  const origins = new Set(shellOrigins.map(value => new URL(value).origin));
  assertApps(origins.size > 0, 'apps_shell_origins_required');
  const template = validateTemplate(appOriginTemplate, origins);
  const namedZone = normalizeNamedAppZone(namedAppZone), limits = normalizeDomainLimits(domainLimits);
  validateNamedOrigins([namedZone], { shellOrigins: [...origins], validateNamedZone });
  mkdirSync(dirname(databasePath), { recursive: true });
  const db = new DatabaseSync(databasePath);
  let domains, publications;
  try {
    db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;');
    const schema = inspectAppsSchema(db);
    if (['v2', 'v3', 'v4', 'v5', 'v6'].includes(schema)) validateNamedOrigins(readNamedOrigins(db), { shellOrigins: [...origins], validateNamedZone });
    migrateAppsSchema(db, { legacyTemplate: template, now });
    publications = createPublicationRegistry({ db, now, assertActor, canUse, onChanged: event => invalidateAccess({ appId: event.appId }) });
    domains = createDomainRegistry({ db, now, assertActor, legacyTemplate: template, namedAppZone: namedZone, domainLimits: limits,
      shellOrigins: [...origins], validateNamedZone, onRetireInTransaction: publications.retireInTransaction,
      onPolicyChanged: publications.notifyChanged });
    db.exec('PRAGMA journal_mode=WAL;');
  } catch (error) { db.close(); throw error; }
  const hostClassifier = createHostClassifier({ db, shellOrigins: [...origins] });
  const channels = new Map(), tickets = new Map(), sessions = new Map(), live = new Map();
  let inspection, saved, discussions;
  try {
    inspection = createAppInspection({ db, assertActor, domains, publications, inspectSource, inspectBinding, now,
      shellOrigin: [...origins][0], nameClaimsEnabled: Boolean(namedZone), namedAppZone: namedZone });
    const resolveEntry = createEngagementEntryResolver({ db, assertActor, publications, inspectSource });
    saved = createSavedRegistry({ db, now, assertActor, withAuthorityFence, resolveEntry });
    discussions = createDiscussionRegistry({ db, now, assertActor, withAuthorityFence, resolveEntry, canUse,
      readCommunityAuthority(actor, ownerAccountId, relevantCommunityIds) {
        assertApps(typeof readCommunityAuthority === 'function', 'apps_authority_fence_required', 503);
        return readCommunityAuthority(actor, ownerAccountId, relevantCommunityIds);
      },
      authorLabel: discussionAuthorLabel, limits: discussionLimits });
  } catch (error) { db.close(); throw error; }
  const wss = new WebSocketServer({ noServer: true, maxPayload: FRAME_BYTES, perMessageDeflate: false });
  let closed = false;
  const row = id => db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  const binding = key => db.prepare('SELECT * FROM app_devices WHERE connector_key=?').get(key);
  const activeTarget = id => db.prepare(`SELECT t.* FROM app_publications p
    JOIN app_runtime_targets t ON t.app_id=p.app_id AND t.revision=p.active_target_revision AND t.owner_account_id=p.owner_account_id
    WHERE p.app_id=?`).get(id);
  const runtimeBindings = createRuntimeBindings({ channels, send, now, blockedPorts,
    onBindingInvalidated(channel, id) {
      for (const stream of channel.streams.values()) if (stream.appId === id) closeStream(stream, 'app_source_changed');
    } });
  const sources = createSourceRegistry({ db, now, assertActor, publications, blockedPorts,
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
  const sourcePreparationExtension = Object.freeze({ operations: new Set(['apps.source.prepare']),
    async executeAsync({ op, args = {}, actor }) {
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
  function publicApp(app, actor) {
    const target = activeTarget(app.id);
    assertApps(target?.owner_account_id === app.owner_account_id, 'apps_registry_corrupt', 500);
    const device = binding(target.connector_key);
    assertApps(device?.owner_account_id === app.owner_account_id, 'apps_registry_corrupt', 500);
    const observed = inspectSource({ app, target: { connectorKey: target.connector_key, revision: target.revision, digest: target.digest } });
    const legacyState = { offline: 'offline', unknown: 'starting', responding: 'ready', unreachable: 'stopped' };
    const state = app.state === 'revoked' ? 'revoked' : legacyState[observed.state];
    return { id: app.id, name: app.name, ownerAccountId: app.owner_account_id, hostDeviceId: JSON.parse(device.identity_json).hostDeviceId,
      state, createdAt: app.created_at, updatedAt: app.updated_at,
      ...(actor.accountId === app.owner_account_id ? { connectorId: JSON.parse(device.identity_json).connectorId, deviceName: device.name,
        port: target.port, entryPath: target.entry_path, grants: JSON.parse(app.grants_json) } : {}) };
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
  function deviceProjection(item) {
    const identity = JSON.parse(item.identity_json), channel = channels.get(item.connector_key);
    const online = channel?.ws.readyState === 1;
    return { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: item.name, online, claimed: true,
      bindingVersion: online ? channel.bindingVersion : null };
  }
  function closeStream(stream, error = 'app_stream_closed', notify = true) {
    if (stream.closed) return;
    stream.closed = true; clearTimeout(stream.timer);
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
    for (const [key, item] of tickets) if (matches(item, filter) && !retainAccess(item)) tickets.delete(key);
    for (const [key, item] of sessions) if (matches(item, filter) && !retainAccess(item)) sessions.delete(key);
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
  function execute({ op, args = {}, actor }) {
    args = authenticatedArgs(actor, args); assertApps(operations.has(op), 'unsupported_operation');
    if (savedOperations.has(op)) return saved.execute({ op, actor, args });
    if (discussionOperations.has(op)) return discussions.execute({ op, actor, args });
    if (op === 'apps.source.promote' || op === 'apps.source.history') return sources.execute({ op, actor, args });
    if (domainOperations.has(op)) return domains.execute({ actor, op, args });
    if (publicationOperations.has(op)) return publications.execute({ actor, op, args });
    if (op === 'apps.inspect') return inspection.read(actor, args);
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
    if (op === 'apps.list') {
      exact(args, ['communityId']); const community = args.communityId === undefined ? '' : textId(args.communityId);
      if (community) assertApps(canAccessCommunity(actor.accountId, community) === true, 'apps_access_denied', 403);
      const candidates = typeof activeCommunityIds === 'function' ? db.prepare(`SELECT * FROM local_apps WHERE id IN (
        SELECT id FROM local_apps WHERE owner_account_id=? UNION
        SELECT app_id FROM local_app_grants WHERE kind='account' AND principal_id=? UNION
        SELECT app_id FROM local_app_grants WHERE kind='community' AND principal_id IN (SELECT value FROM json_each(?))) ORDER BY created_at`)
        .all(actor.accountId, actor.accountId, JSON.stringify(activeCommunityIds(actor.accountId))) : db.prepare('SELECT * FROM local_apps ORDER BY created_at').all();
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
    if (op === 'apps.launch') {
      exact(args, ['appId', 'domainId', 'path']);
      assertApps(app?.state === 'enabled', 'apps_access_denied', 403);
      const domain = args.domainId === undefined
        ? db.prepare("SELECT * FROM app_domains WHERE app_id=? AND role='canonical'").get(id)
        : db.prepare('SELECT * FROM app_domains WHERE app_id=? AND id=?').get(id, textId(args.domainId));
      assertApps(domain, args.domainId === undefined ? 'apps_origin_not_configured' : 'apps_access_denied', args.domainId === undefined ? 503 : 403);
      const decision = publications.decideAccess({ domainId: domain.id, origin: domain.origin, actor });
      assertRuntimeBinding(decision, { requireReady: true });
      const { entryPath, bootPath } = createLaunchPath(args.path ?? decision.route.entryPath);
      assertApps(channels.has(decision.route.connectorKey), 'app_offline', 503);
      assertApps(tickets.size < 4096, 'apps_launch_busy', 429);
      const ticket = secret();
      tickets.set(digest(ticket), { decision, entryPath });
      return { launchUrl: `${domain.origin}${bootPath}#${ticket}`, expiresAt: decision.expiresAt };
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
  function setStatusPolicy(res) {
    res.setHeader('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'; sandbox");
    res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer'); res.setHeader('Cache-Control', 'no-store');
  }
  function shellUrlFor(app, path) {
    const url = new URL('/', [...origins][0]);
    url.hash = `launch/${app.appHost.appId}/${app.appHost.domainId}?${new URLSearchParams({ path: runtimePath(path) })}`;
    return url.href;
  }
  function publicResetFor(app, path) {
    try {
      publications.decideAccess({ domainId: app.appHost.domainId, origin: app.appHost.origin });
      return runtimePath(path);
    } catch { return undefined; }
  }
  function respondAppFailure(req, res, status, error, canRedirect = true) {
    let page;
    try {
    const app = appForRequest(req);
    const failure = app && hostFailure(app);
    if (failure) { setStatusPolicy(res); respondFailure(req, res, failure.status, failure.error); return; }
    if (app?.appHost && !app.missing) {
      let path;
      try { path = runtimePath(req.url || '/'); } catch { /* Internal endpoints never redirect into the shell. */ }
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
      status = 503; error = 'app_unavailable';
    }
    respondFailure(req, res, status, error, page);
  }
  function sessionFor(req, app, { requireCookie = false } = {}) {
    const name = `__Host-${cookieName}`;
    const values = String(req.headers.cookie || '').split(';').map(part => part.trim()).filter(part => part.split('=', 1)[0].trim() === name);
    if (values.length === 0) {
      assertApps(!requireCookie && app.appHost.kind === 'alias', 'app_session_required', 401);
      return { decision: publications.decideAccess({ domainId: app.appHost.domainId, origin: app.appHost.origin }) };
    }
    assertApps(values.length === 1, 'app_session_required', 401);
    const token = values[0].slice(name.length + 1); assertApps(/^[A-Za-z0-9_-]{43}$/u.test(token), 'app_session_required', 401);
    const session = sessions.get(digest(token));
    assertApps(session && session.decision.appId === app.id && session.decision.domainId === app.appHost.domainId && session.decision.origin === app.appHost.origin, 'apps_access_denied', 403);
    checkAccess(session);
    return session;
  }
  function setPolicy(res, origin) {
    const frameOrigins = [...origins].join(' ');
    res.setHeader('Content-Security-Policy', `default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; media-src 'self' blob:; connect-src 'self'; worker-src 'none'; object-src 'none'; base-uri 'self'; form-action 'self'; frame-src 'none'; frame-ancestors ${frameOrigins}; sandbox allow-scripts allow-forms allow-same-origin`);
    res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=(), payment=(), usb=(), serial=(), hid=(), bluetooth=()');
    res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer'); res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Origin-Agent-Cluster', '?1');
    if (new URL(origin).protocol === 'https:') res.setHeader('Strict-Transport-Security', 'max-age=31536000');
  }
  async function routeApp(req, res, app) {
    assertApps(!app.missing, 'app_not_found', 404); setPolicy(res, app.appHost.origin);
    const path = requestPath(req.url || '/');
    assertOrigin(req, app.appHost.origin, !['GET', 'HEAD'].includes(req.method));
    const internalUrl = new URL(path, app.appHost.origin);
    if (internalUrl.pathname === '/_soty/boot' && req.method === 'GET') {
      assertApps([...internalUrl.searchParams.keys()].every(key => key === 'path') && internalUrl.searchParams.getAll('path').length <= 1, 'invalid_app_path');
      const recoveryPath = runtimePath(internalUrl.searchParams.get('path') || '/'), nonce = pageNonce();
      const html = renderBootPage({ nonce, shellUrl: shellUrlFor(app, recoveryPath), publicResetPath: publicResetFor(app, recoveryPath) });
      setManagedPagePolicy(res, nonce, [...origins]);
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.end(html); return;
    }
    if (path === '/_soty/session' && req.method === 'GET') {
      const session = sessionFor(req, app, { requireCookie: true }), check = req.headers['x-soty-boot-check'];
      if (check !== undefined) {
        assertApps(headerCount(req, 'x-soty-boot-check') === 1 && typeof check === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(check)
          && session.checkExpiresAt > now() && equalDigest(session.checkDigest, digest(check)), 'app_session_check_failed', 403);
      }
      json(res, 200, { ok: true, ...(check === undefined ? {} : { sessionCheck: check }) }); return;
    }
    if (path === '/_soty/session' && req.method === 'POST') {
      const body = JSON.parse((await readBounded(req, 2048)).toString('utf8'));
      assertApps(body && typeof body === 'object' && !Array.isArray(body) && Object.keys(body).length === 1
        && typeof body.ticket === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(body.ticket), 'app_ticket_invalid', 403);
      const key = digest(body.ticket), ticket = tickets.get(key); tickets.delete(key);
      assertApps(ticket && ticket.decision.appId === app.id && ticket.decision.domainId === app.appHost.domainId && ticket.decision.origin === app.appHost.origin, 'app_ticket_invalid', 403);
      // The request body was asynchronous: only this fresh branded recheck may
      // mint a session. A stale app row or successful launch is not authority.
      const decision = publications.recheckAccess(ticket.decision, { ttlMs: accountSessionMs });
      assertRuntimeBinding(decision, { requireReady: true });
      assertApps(sessions.size < 4096, 'apps_sessions_busy', 429);
      const value = secret(), sessionKey = digest(value), sessionCheck = secret();
      sessions.set(sessionKey, { decision, sessionKey, entryPath: ticket.entryPath, checkDigest: digest(sessionCheck), checkExpiresAt: now() + 30_000 });
      // CHIPS keys this session by both application host and embedding site.
      // A preview embedded in Soty does not depend on unrestricted third-party
      // cookies, and cannot reuse the session from an unrelated top-level site.
      res.setHeader('Set-Cookie', `__Host-${cookieName}=${value}; HttpOnly; Path=/; SameSite=None; Secure; Partitioned; Max-Age=3600`);
      json(res, 200, { ok: true, entryPath: ticket.entryPath, sessionCheck }); return;
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
          for (const stream of live.values()) if (stream.session.sessionKey === key) closeStream(stream, 'app_access_revoked');
        }
      }
      const cleared = `${name}=; HttpOnly; Path=/; SameSite=None; Secure; Max-Age=0`;
      res.setHeader('Set-Cookie', [`${cleared}; Partitioned`, cleared]);
      if (!req.readableEnded) { res.shouldKeepAlive = false; res.setHeader('Connection', 'close'); }
      json(res, 200, { ok: true }); return;
    }
    runtimePath(path);
    const session = sessionFor(req, app);
    assertApps(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'].includes(req.method), 'app_method_denied', 405);
    const stream = openStream(app, session, { kind: 'http', req, res });
    res.once('close', () => closeStream(stream, 'app_client_closed'));
    try {
      sendStream(stream, { type: 'open', id: stream.id, kind: 'http', appId: app.id, path, method: req.method, headers: cleanHeaders(req.headers) });
      let count = 0;
      for await (const chunk of req) { count += chunk.length; assertApps(count <= LIMITS.requestBytes, 'app_request_too_large', 413); await sendChunks(stream, chunk); }
      sendStream(stream, { type: 'end', id: stream.id });
    } catch (error) { closeStream(stream, error.code || 'app_request_failed'); }
  }
  function handleRequest(req, res) {
    const app = appForRequest(req); if (!app) return false;
    const failure = hostFailure(app);
    if (failure) { setStatusPolicy(res); respondFailure(req, res, failure.status, failure.error); return true; }
    void routeApp(req, res, app).catch(error => { if (!res.headersSent) respondAppFailure(req, res, error.status || 400, error.code || 'app_invalid_request'); else res.destroy(); }); return true;
  }
  function openStream(app, session, values) {
    const decision = checkAccess(session, { renewPublic: true });
    const runtimeBinding = assertRuntimeBinding(decision, { requireReady: true });
    const channel = channels.get(decision.route.connectorKey); assertApps(channel && channel.ws.readyState === 1, 'app_offline', 503);
    assertApps(channel.streams.size < LIMITS.streams, 'app_device_busy', 429);
    if (decision.accessBasis === 'public') assertApps([...channel.streams.values()].filter(item => item.session.decision.accessBasis === 'public').length < publicStreams, 'app_device_busy', 429);
    const id = randomBytes(16).toString('hex'), stream = { ...values, id, appId: app.id, channel, runtimeBinding, session: { decision, sessionKey: session.sessionKey }, pending: new Map(), sendSeq: 0, recvSeq: 0, received: 0, closed: false, head: false, receiving: false,
      ...(values.kind === 'ws' ? { requestLimiter: createWebSocketLimiter({ masked: true }), responseLimiter: createWebSocketLimiter({ masked: false }) } : {}) };
    stream.timer = setTimeout(() => closeStream(stream, 'app_response_timeout'), LIMITS.headMs); stream.timer.unref();
    channel.streams.set(id, stream); live.set(id, stream); return stream;
  }
  function sendStream(stream, frame) {
    checkStream(stream);
    if (frame.type === 'open' && stream.channel.bindingVersion === 2) frame = { ...frame, type: 'bound-open', ...runtimeBindings.openPins(stream.runtimeBinding) };
    assertApps(send(stream.channel, frame), 'app_offline', 503);
  }
  async function sendChunks(stream, bytes) {
    checkStream(stream);
    if (stream.kind === 'ws') stream.requestLimiter.push(bytes);
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
  function send(channel, frame) {
    if (closed || !channel || channels.get(channel.key) !== channel || channel.ws.readyState !== 1) return false;
    const payload = JSON.stringify(frame), bytes = Buffer.byteLength(payload, 'utf8');
    if (bytes > FRAME_BYTES || channel.ws.bufferedAmount + bytes > 4 * 1024 * 1024) return false;
    try { channel.ws.send(payload); return true; } catch { return false; }
  }
  function handleUpgrade(req, socket, head) {
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
          if (frame.capabilities !== undefined) {
            const versions = frame.capabilities?.targetBindingVersions;
            assertApps(frame.capabilities && typeof frame.capabilities === 'object' && !Array.isArray(frame.capabilities)
              && Array.isArray(versions) && versions.length > 0 && versions.length <= 8
              && versions.every(value => Number.isSafeInteger(value) && value >= 1) && new Set(versions).size === versions.length
              && (versions.includes(1) || versions.includes(2)), 'app_source_protocol_required');
            bindingVersion = versions.includes(2) ? 2 : 1;
          }
          const key = connectorKey(identity), previous = channels.get(key);
          if (previous) {
            runtimeBindings.drop(previous);
            for (const oldStream of previous.streams.values()) closeStream(oldStream, 'app_offline', false);
            previous.ws.terminate();
          }
          channel = { ws, key, identity: Object.freeze(identity), bindingVersion, ...(bindingVersion === 2 ? { channelId: secret() } : {}),
            auth: { linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, token: frame.token }, name: typeof frame.name === 'string' ? frame.name.slice(0, 80) : identity.hostDeviceId,
            claimDigest: '', claimExpiresAt: 0, streams: new Map(), observations: new Map(), lastSeenAt: now() };
          channels.set(key, channel); clearTimeout(timer);
          assertApps(send(channel, { type: 'ready', schema: CHANNEL_SCHEMA, ...(bindingVersion === 2 ? { bindingVersion, channelId: channel.channelId } : {}) }), 'app_offline', 503);
          sync(channel); return;
        }
        channel.lastSeenAt = now();
        assertApps(frame.type !== 'auth', 'app_auth_repeated');
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
          stream.head = true; clearTimeout(stream.timer); stream.timer = setTimeout(() => closeStream(stream, 'app_idle_timeout'), LIMITS.idleMs); stream.timer.unref();
          if (stream.kind === 'ws') {
            assertApps(frame.status === 101, 'app_upgrade_failed'); const headers = cleanHeaders(frame.headers, 'upgrade');
            assertApps(headers['sec-websocket-accept'] && !headers['sec-websocket-extensions'], 'app_upgrade_failed');
            checkStream(stream);
            stream.socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n${Object.entries(headers).map(([k, v]) => `${k}: ${v}\r\n`).join('')}\r\n`);
            void (async () => { try { if (stream.upgradeHead.length) await sendChunks(stream, stream.upgradeHead); for await (const chunk of stream.socket) await sendChunks(stream, chunk); sendStream(stream, { type: 'end', id: stream.id }); } catch (error) { closeStream(stream, error.code || 'app_client_closed'); } })();
          } else {
            for (const [key, value] of Object.entries(cleanHeaders(frame.headers, 'response'))) stream.res.setHeader(key, value);
            if (frame.location) stream.res.setHeader('Location', requestPath(frame.location));
            stream.res.writeHead(frame.status);
          }
          return;
        }
        if (frame.type === 'data') {
          assertApps(stream.head && !stream.receiving && frame.seq === stream.recvSeq + 1 && typeof frame.data === 'string' && frame.data.length <= CHUNK_BYTES * 4 / 3 + 4 && /^[A-Za-z0-9+/]*={0,2}$/u.test(frame.data), 'app_bad_data');
          const chunk = Buffer.from(frame.data, 'base64'); assertApps(chunk.length <= CHUNK_BYTES, 'app_bad_data');
          if (stream.kind === 'ws') { try { stream.responseLimiter.push(chunk); } catch (error) { closeStream(stream, error.code || 'app_websocket_invalid_frame'); return; } }
          stream.received += chunk.length;
          if (stream.kind !== 'ws' && stream.received > LIMITS.responseBytes) { closeStream(stream, 'app_response_too_large'); return; }
          stream.receiving = true; stream.recvSeq = frame.seq; stream.timer.refresh();
          await writeChunk(stream.kind === 'ws' ? stream.socket : stream.res, chunk); stream.receiving = false;
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
  return { operations, execute, sourcePreparationExtension, handleRequest, handleUpgrade, invalidateAccess, invalidateConnector, resolveOwnedDevice, allowsTlsDomain,
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
    close() { if (closed) return; closed = true; discussions.close(); sources.close(); runtimeBindings.close(); clearInterval(auditTimer); clearInterval(heartbeatTimer); clearInterval(connectorAuditTimer); unsubscribe?.(); for (const channel of channels.values()) channel.ws.terminate(); for (const stream of live.values()) closeStream(stream, 'app_server_closed', false); tickets.clear(); sessions.clear(); wss.close(); db.close(); },
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
