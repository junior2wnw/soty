import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { WebSocketServer } from 'ws';
import { AppsError, assertApps, textId, appId, appName, appPort, requestPath, cleanGrants, connectorKey, cleanHeaders, createWebSocketLimiter, CHANNEL_SCHEMA, CHUNK_BYTES, FRAME_BYTES, LIMITS } from './protocol.mjs';

export const operations = new Set(['apps.devices', 'apps.claim', 'apps.list', 'apps.register', 'apps.update', 'apps.revoke', 'apps.launch']);
const cookieName = 'soty_app_session';
const secret = () => randomBytes(32).toString('base64url');
const digest = value => createHash('sha256').update(value).digest('hex');
const equalDigest = (a, b) => typeof a === 'string' && typeof b === 'string' && /^[a-f0-9]{64}$/u.test(a) && /^[a-f0-9]{64}$/u.test(b) && timingSafeEqual(Buffer.from(a, 'hex'), Buffer.from(b, 'hex'));

export function createAppsService({ dataDir = 'data', databasePath = join(dataDir, 'apps', 'registry.sqlite'), appOriginTemplate = '', shellOrigins = [], actorActive = () => false,
  canAccessCommunity = () => false, isGroupAdmin = () => false, activeCommunityIds, subscribeMembership, authenticateConnector = async () => false, now = Date.now, blockedPorts = [], connectorAuthCheckMs = 10_000 } = {}) {
  const origins = new Set(shellOrigins.map(value => new URL(value).origin));
  assertApps(origins.size > 0, 'apps_shell_origins_required');
  const template = validateTemplate(appOriginTemplate, origins);
  mkdirSync(dirname(databasePath), { recursive: true });
  const db = new DatabaseSync(databasePath);
  db.exec('PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;');
  db.exec(`CREATE TABLE IF NOT EXISTS apps_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS app_devices (connector_key TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,identity_json TEXT NOT NULL,name TEXT NOT NULL,created_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS local_apps (id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL REFERENCES app_devices(connector_key),name TEXT NOT NULL,port INTEGER NOT NULL,entry_path TEXT NOT NULL,grants_json TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS local_apps_owner ON local_apps(owner_account_id);
    CREATE TABLE IF NOT EXISTS local_app_grants (app_id TEXT NOT NULL REFERENCES local_apps(id) ON DELETE CASCADE,kind TEXT NOT NULL CHECK(kind IN ('account','community')),principal_id TEXT NOT NULL,PRIMARY KEY(app_id,kind,principal_id));
    CREATE INDEX IF NOT EXISTS local_app_grants_principal ON local_app_grants(kind,principal_id,app_id);`);
  const schema = db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get();
  assertApps(!schema || schema.value === 'soty.apps-registry.v1', 'apps_schema_unsupported');
  db.prepare("INSERT OR IGNORE INTO apps_meta(key,value) VALUES ('schema','soty.apps-registry.v1')").run();
  db.exec(`INSERT OR IGNORE INTO local_app_grants SELECT a.id,'account',g.value FROM local_apps a,json_each(a.grants_json,'$.accountIds') g;
    INSERT OR IGNORE INTO local_app_grants SELECT a.id,'community',g.value FROM local_apps a,json_each(a.grants_json,'$.communityIds') g;`);
  const channels = new Map(), tickets = new Map(), sessions = new Map(), live = new Map();
  const wss = new WebSocketServer({ noServer: true, maxPayload: FRAME_BYTES, perMessageDeflate: false });
  let closed = false;
  const row = id => db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  const binding = key => db.prepare('SELECT * FROM app_devices WHERE connector_key=?').get(key);
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
    const device = binding(app.connector_key), channel = channels.get(app.connector_key);
    const observed = channel?.observations.get(app.id);
    const state = app.state === 'revoked' ? 'revoked' : !channel ? 'offline' : observed?.state || 'starting';
    return { id: app.id, name: app.name, ownerAccountId: app.owner_account_id, hostDeviceId: JSON.parse(device.identity_json).hostDeviceId,
      state, createdAt: app.created_at, updatedAt: app.updated_at,
      ...(actor.accountId === app.owner_account_id ? { connectorId: JSON.parse(device.identity_json).connectorId, port: app.port, entryPath: app.entry_path, grants: JSON.parse(app.grants_json) } : {}) };
  }
  function closeStream(stream, error = 'app_stream_closed', notify = true) {
    if (stream.closed) return;
    stream.closed = true; clearTimeout(stream.timer); live.delete(stream.id); stream.channel.streams.delete(stream.id);
    for (const pending of stream.pending.values()) { clearTimeout(pending.timer); pending.reject(new AppsError(error, 502)); }
    stream.pending.clear();
    if (notify) send(stream.channel, { type: 'cancel', id: stream.id, error });
    if (stream.kind === 'ws') { if (error === 'app_stream_complete') stream.socket.end(); else stream.socket.destroy(); }
    else if (!stream.res.writableEnded) {
      if (!stream.res.headersSent) respondFailure(stream.req, stream.res, error === 'app_access_revoked' ? 403 : error === 'app_offline' ? 503 : 502, error);
      else stream.res.destroy();
    }
  }
  function invalidateAccess(filter = {}) {
    for (const [key, item] of tickets) if (matches(item, filter) && !canUse(item.actor, row(item.appId))) tickets.delete(key);
    for (const [key, item] of sessions) if (matches(item, filter) && !canUse(item.actor, row(item.appId))) sessions.delete(key);
    for (const stream of live.values()) if (matches(stream.session, filter) && !canUse(stream.session.actor, row(stream.appId))) closeStream(stream, 'app_access_revoked');
  }
  function invalidateConnector({ hostDeviceId, connectorId } = {}) {
    for (const channel of channels.values()) if ((!hostDeviceId || channel.identity.hostDeviceId === hostDeviceId) && (!connectorId || channel.identity.connectorId === connectorId)) {
      channels.delete(channel.key); for (const stream of channel.streams.values()) closeStream(stream, 'app_offline', false); channel.ws.terminate();
    }
  }
  const unsubscribe = subscribeMembership?.(event => { invalidateAccess({ communityId: event.communityId }); });
  const auditTimer = setInterval(() => {
    for (const [key, item] of tickets) if (item.expiresAt <= now()) tickets.delete(key);
    for (const [key, item] of sessions) if (item.expiresAt <= now()) sessions.delete(key);
    for (const stream of live.values()) if (stream.session.expiresAt <= now() || !canUse(stream.session.actor, row(stream.appId))) closeStream(stream, 'app_access_revoked');
    for (const channel of channels.values()) {
      if (now() - channel.lastSeenAt > 45_000) { channel.ws.terminate(); continue; }
      channel.ws.ping();
    }
  }, 10_000);
  auditTimer.unref();
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
    return (!filter.appId || item.appId === filter.appId) && (!filter.accountId || item.actor.accountId === filter.accountId) && (!filter.deviceId || item.actor.deviceId === filter.deviceId);
  }
  function sync(channel) {
    if (!channel) return;
    const apps = db.prepare("SELECT id,port,entry_path FROM local_apps WHERE connector_key=? AND state='enabled'").all(channel.key)
      .map(app => ({ id: app.id, port: app.port, entryPath: app.entry_path }));
    send(channel, { type: 'sync', apps });
  }
  function execute({ op, args = {}, actor }) {
    assertApps(!closed, 'apps_closed', 503); assertActor(actor); assertApps(operations.has(op), 'unsupported_operation');
    assertApps(args && typeof args === 'object' && !Array.isArray(args), 'invalid_arguments');
    if (op === 'apps.devices') {
      exact(args, []);
      return { devices: db.prepare('SELECT * FROM app_devices WHERE owner_account_id=? ORDER BY created_at').all(actor.accountId).map(item => {
        const identity = JSON.parse(item.identity_json); return { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: item.name, online: channels.has(item.connector_key), claimed: true };
      }) };
    }
    if (op === 'apps.claim') {
      exact(args, ['hostDeviceId', 'connectorId', 'claimCode']);
      const channel = [...channels.values()].find(item => item.identity.hostDeviceId === textId(args.hostDeviceId) && item.identity.connectorId === textId(args.connectorId));
      assertApps(channel && typeof args.claimCode === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(args.claimCode) && channel.claimExpiresAt > now() && equalDigest(channel.claimDigest, digest(args.claimCode)), 'apps_claim_unavailable', 403);
      const existing = binding(channel.key);
      assertApps(!existing || existing.owner_account_id === actor.accountId, 'apps_device_already_owned', 403);
      if (!existing) db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(channel.key, actor.accountId, JSON.stringify(channel.identity), channel.name, now());
      channel.claimDigest = ''; channel.claimExpiresAt = 0; send(channel, { type: 'claimed' }); sync(channel);
      return { device: { hostDeviceId: channel.identity.hostDeviceId, connectorId: channel.identity.connectorId, name: channel.name, online: true, claimed: true } };
    }
    if (op === 'apps.list') {
      exact(args, ['communityId']); const community = args.communityId === undefined ? '' : textId(args.communityId);
      if (community) assertApps(canAccessCommunity(actor.accountId, community) === true, 'apps_access_denied', 403);
      const candidates = typeof activeCommunityIds === 'function' ? db.prepare(`SELECT * FROM local_apps WHERE id IN (
        SELECT id FROM local_apps WHERE owner_account_id=? UNION
        SELECT app_id FROM local_app_grants WHERE kind='account' AND principal_id=? UNION
        SELECT app_id FROM local_app_grants WHERE kind='community' AND principal_id IN (SELECT value FROM json_each(?))) ORDER BY created_at`)
        .all(actor.accountId, actor.accountId, JSON.stringify(activeCommunityIds(actor.accountId))) : db.prepare('SELECT * FROM local_apps ORDER BY created_at').all();
      return { configured: Boolean(template), apps: candidates.filter(app =>
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
      const existing = db.prepare("SELECT * FROM local_apps WHERE connector_key=? AND port=? AND state='enabled'").get(device.connector_key, port);
      if (existing) {
        assertApps(existing.name === name && existing.entry_path === entryPath && existing.grants_json === JSON.stringify(grants), 'app_port_already_registered', 409);
        return { app: publicApp(existing, actor) };
      }
      assertApps(db.prepare('SELECT count(*) AS n FROM local_apps WHERE owner_account_id=?').get(actor.accountId).n < 100, 'apps_limit_reached', 429);
      const id = `app-${randomBytes(16).toString('hex')}`, timestamp = now();
      transaction(() => { db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, actor.accountId, device.connector_key, name, port, entryPath, JSON.stringify(grants), 'enabled', 1, timestamp, timestamp); persistGrants(id, grants); });
      sync(channels.get(device.connector_key)); return { app: publicApp(row(id), actor) };
    }
    const id = appId(args.appId), app = row(id);
    if (op === 'apps.launch') {
      exact(args, ['appId']); assertApps(canUse(actor, app), 'apps_access_denied', 403); assertApps(template, 'apps_origin_not_configured', 503);
      assertApps(channels.has(app.connector_key), 'app_offline', 503);
      assertApps(tickets.size < 4096, 'apps_launch_busy', 429);
      const ticket = secret(), expiresAt = now() + 30_000;
      tickets.set(digest(ticket), { appId: id, actor: { accountId: actor.accountId, deviceId: actor.deviceId }, revision: app.revision, expiresAt });
      return { launchUrl: `${originFor(id)}/_soty/boot#${ticket}`, expiresAt };
    }
    assertApps(app && app.owner_account_id === actor.accountId, 'apps_owner_required', 403);
    if (op === 'apps.revoke') {
      exact(args, ['appId']); db.prepare("UPDATE local_apps SET state='revoked',revision=revision+1,updated_at=? WHERE id=?").run(now(), id);
      invalidateAccess({ appId: id }); sync(channels.get(app.connector_key)); return { app: publicApp(row(id), actor) };
    }
    exact(args, ['appId', 'name', 'grants']); assertApps(app.state === 'enabled', 'app_revoked', 409);
    const name = args.name === undefined ? app.name : appName(args.name), grants = args.grants === undefined ? JSON.parse(app.grants_json) : cleanGrants(args.grants);
    assertGrants(actor, grants);
    transaction(() => { db.prepare('UPDATE local_apps SET name=?,grants_json=?,revision=revision+1,updated_at=? WHERE id=?').run(name, JSON.stringify(grants), now(), id); persistGrants(id, grants); });
    invalidateAccess({ appId: id }); return { app: publicApp(row(id), actor) };
  }
  function originFor(id) { return template.replace('{appId}', id); }
  function appForRequest(req) {
    if (!template) return null;
    const host = String(req.headers.host || '').toLowerCase();
    const match = host.match(/app-[a-f0-9]{32}/u);
    if (!match || new URL(originFor(match[0])).host !== host) return null;
    return row(match[0]) || { id: match[0], missing: true };
  }
  function sessionFor(req, app) {
    const name = `__Host-${cookieName}`;
    const values = String(req.headers.cookie || '').split(';').map(part => part.trim()).filter(part => part.startsWith(`${name}=`));
    assertApps(values.length === 1, 'app_session_required', 401);
    const token = values[0].slice(name.length + 1); assertApps(/^[A-Za-z0-9_-]{43}$/u.test(token), 'app_session_required', 401);
    const session = sessions.get(digest(token));
    assertApps(session && session.appId === app.id && session.expiresAt > now() && canUse(session.actor, app), 'apps_access_denied', 403);
    return session;
  }
  function setPolicy(res, id) {
    const frameOrigins = [...origins].join(' ');
    res.setHeader('Content-Security-Policy', `default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; media-src 'self' blob:; connect-src 'self'; worker-src 'none'; object-src 'none'; base-uri 'self'; form-action 'self'; frame-src 'none'; frame-ancestors ${frameOrigins}; sandbox allow-scripts allow-forms allow-same-origin`);
    res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=(), payment=(), usb=(), serial=(), hid=(), bluetooth=()');
    res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer'); res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Origin-Agent-Cluster', '?1');
    if (new URL(originFor(id)).protocol === 'https:') res.setHeader('Strict-Transport-Security', 'max-age=31536000');
  }
  async function routeApp(req, res, app) {
    setPolicy(res, app.id); assertApps(!app.missing, 'app_not_found', 404);
    const path = requestPath(req.url || '/');
    if (path === '/_soty/boot' && req.method === 'GET') {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.end('<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Соты · Открываем приложение</title><body><p role="status">Открываем приложение…</p><script>const t=location.hash.slice(1);history.replaceState(null,"",location.pathname);fetch("/_soty/session",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ticket:t})}).then(async r=>{const v=await r.json();if(!r.ok)throw Error(v.error);location.replace(v.entryPath)}).catch(()=>{document.querySelector("p").textContent="Доступ закончился. Откройте приложение заново в Сотах."})</script></body></html>'); return;
    }
    if (path === '/_soty/session' && req.method === 'POST') {
      assertApps(req.headers.origin === originFor(app.id), 'app_origin_denied', 403);
      const body = JSON.parse((await readBounded(req, 2048)).toString('utf8'));
      assertApps(typeof body.ticket === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(body.ticket), 'app_ticket_invalid', 403);
      const key = digest(body.ticket), ticket = tickets.get(key); tickets.delete(key);
      assertApps(ticket && ticket.appId === app.id && ticket.expiresAt > now() && ticket.revision === app.revision && canUse(ticket.actor, app), 'app_ticket_invalid', 403);
      assertApps(sessions.size < 4096, 'apps_sessions_busy', 429);
      const value = secret(), session = { ...ticket, expiresAt: now() + 60 * 60_000 }; sessions.set(digest(value), session);
      // CHIPS keys this session by both application host and embedding site.
      // A preview embedded in Soty does not depend on unrestricted third-party
      // cookies, and cannot reuse the session from an unrelated top-level site.
      res.setHeader('Set-Cookie', `__Host-${cookieName}=${value}; HttpOnly; Path=/; SameSite=None; Secure; Partitioned; Max-Age=3600`);
      json(res, 200, { ok: true, entryPath: app.entry_path }); return;
    }
    assertApps(!path.startsWith('/_soty/'), 'app_reserved_path', 404);
    const session = sessionFor(req, app);
    assertApps(!req.headers.origin || req.headers.origin === originFor(app.id), 'app_origin_denied', 403);
    assertApps(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'].includes(req.method), 'app_method_denied', 405);
    const stream = openStream(app, session, { kind: 'http', req, res });
    send(stream.channel, { type: 'open', id: stream.id, kind: 'http', appId: app.id, path, method: req.method, headers: cleanHeaders(req.headers) });
    res.once('close', () => closeStream(stream, 'app_client_closed'));
    try {
      let count = 0;
      for await (const chunk of req) { count += chunk.length; assertApps(count <= LIMITS.requestBytes, 'app_request_too_large', 413); await sendChunks(stream, chunk); }
      send(stream.channel, { type: 'end', id: stream.id });
    } catch (error) { closeStream(stream, error.code || 'app_request_failed'); }
  }
  function handleRequest(req, res) {
    const app = appForRequest(req); if (!app) return false;
    void routeApp(req, res, app).catch(error => { if (!res.headersSent) respondFailure(req, res, error.status || 400, error.code || 'app_invalid_request'); else res.destroy(); }); return true;
  }
  function openStream(app, session, values) {
    const channel = channels.get(app.connector_key); assertApps(channel && channel.ws.readyState === 1, 'app_offline', 503);
    assertApps(channel.streams.size < LIMITS.streams, 'app_device_busy', 429);
    const id = randomBytes(16).toString('hex'), stream = { ...values, id, appId: app.id, channel, session, pending: new Map(), sendSeq: 0, recvSeq: 0, received: 0, closed: false, head: false, receiving: false,
      ...(values.kind === 'ws' ? { requestLimiter: createWebSocketLimiter({ masked: true }), responseLimiter: createWebSocketLimiter({ masked: false }) } : {}) };
    stream.timer = setTimeout(() => closeStream(stream, 'app_response_timeout'), LIMITS.headMs); stream.timer.unref();
    channel.streams.set(id, stream); live.set(id, stream); return stream;
  }
  async function sendChunks(stream, bytes) {
    if (stream.kind === 'ws') stream.requestLimiter.push(bytes);
    for (let offset = 0; offset < bytes.length; offset += CHUNK_BYTES) {
      assertApps(!stream.closed, 'app_stream_closed'); const seq = ++stream.sendSeq;
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => { stream.pending.delete(seq); reject(new AppsError('app_ack_timeout')); }, LIMITS.ackMs); timer.unref();
        stream.pending.set(seq, { resolve, reject, timer });
        if (!send(stream.channel, { type: 'data', id: stream.id, seq, data: bytes.subarray(offset, offset + CHUNK_BYTES).toString('base64') })) { clearTimeout(timer); stream.pending.delete(seq); reject(new AppsError('app_offline')); }
      });
    }
  }
  function send(channel, frame) {
    if (!channel || channel.ws.readyState !== 1 || channel.ws.bufferedAmount > 4 * 1024 * 1024) return false;
    channel.ws.send(JSON.stringify(frame)); return true;
  }
  function handleUpgrade(req, socket, head) {
    let url;
    try { url = new URL(req.url || '/', 'http://localhost'); }
    catch { socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); return true; }
    if (url.pathname === '/api/apps/channel' && !appForRequest(req)) {
      if (req.headers.origin || url.search || wss.clients.size >= 256) { socket.destroy(); return true; }
      wss.handleUpgrade(req, socket, head, ws => acceptChannel(ws)); return true;
    }
    const app = appForRequest(req); if (!app) return false;
    try {
      assertApps(!app.missing && req.method === 'GET' && req.headers.upgrade?.toLowerCase() === 'websocket', 'app_upgrade_denied', 403);
      assertApps(req.headers.origin === originFor(app.id), 'app_origin_denied', 403);
      const path = requestPath(req.url || '/'); assertApps(!path.startsWith('/_soty/'), 'app_reserved_path', 404);
      const session = sessionFor(req, app), stream = openStream(app, session, { kind: 'ws', socket, req, upgradeHead: head });
      socket.pause(); socket.on('error', () => closeStream(stream, 'app_client_closed')); socket.once('close', () => closeStream(stream, 'app_client_closed'));
      const headers = {};
      for (const name of ['sec-websocket-key', 'sec-websocket-version', 'sec-websocket-protocol']) if (typeof req.headers[name] === 'string' && req.headers[name].length < 2048) headers[name] = req.headers[name];
      headers.origin = originFor(app.id);
      send(stream.channel, { type: 'open', id: stream.id, kind: 'ws', appId: app.id, path, method: 'GET', headers });
    } catch { socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); }
    return true;
  }
  function acceptChannel(ws) {
    let channel = null, authenticating = false;
    const timer = setTimeout(() => ws.terminate(), 5000); timer.unref();
    ws.on('error', () => {});
    ws.on('pong', () => { if (channel) channel.lastSeenAt = now(); });
    ws.on('message', async (bytes, binary) => {
      try {
        assertApps(!binary && bytes.length <= FRAME_BYTES, 'app_bad_frame'); const frame = JSON.parse(bytes.toString('utf8'));
        if (!channel) {
          assertApps(!authenticating && frame.type === 'auth' && frame.schema === CHANNEL_SCHEMA, 'app_auth_required'); authenticating = true;
          const identity = { linkId: textId(frame.linkId), hostDeviceId: textId(frame.hostDeviceId), connectorId: textId(frame.connectorId) };
          assertApps(await authenticateConnector({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, token: frame.token }), 'app_connector_auth_failed');
          if (ws.readyState !== 1) return;
          const key = connectorKey(identity); channels.get(key)?.ws.terminate();
          channel = { ws, key, identity, auth: { linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, token: frame.token }, name: typeof frame.name === 'string' ? frame.name.slice(0, 80) : identity.hostDeviceId,
            claimDigest: '', claimExpiresAt: 0, streams: new Map(), observations: new Map(), lastSeenAt: now() };
          channels.set(key, channel); clearTimeout(timer); send(channel, { type: 'ready', schema: CHANNEL_SCHEMA }); sync(channel); return;
        }
        channel.lastSeenAt = now();
        if (frame.type === 'claim') {
          assertApps(/^[a-f0-9]{64}$/u.test(frame.claimDigest || ''), 'app_claim_digest_invalid'); channel.claimDigest = frame.claimDigest; channel.claimExpiresAt = now() + 5 * 60_000;
          send(channel, { type: 'claim-ready', claimDigest: frame.claimDigest }); return;
        }
        if (frame.type === 'observation') {
          const app = row(appId(frame.appId)); assertApps(app?.connector_key === channel.key && ['ready', 'stopped'].includes(frame.state), 'app_bad_observation');
          channel.observations.set(app.id, { state: frame.state, at: now() }); return;
        }
        const stream = channel.streams.get(frame.id); if (!stream) return;
        if (frame.type === 'ack') {
          const item = stream.pending.get(frame.seq); assertApps(item, 'app_bad_ack'); stream.pending.delete(frame.seq); clearTimeout(item.timer); item.resolve(); return;
        }
        if (frame.type === 'cancel') { closeStream(stream, ['app_stopped', 'app_response_timeout'].includes(frame.error) ? frame.error : 'app_upstream_failed', false); return; }
        assertApps(canUse(stream.session.actor, row(stream.appId)), 'app_access_revoked', 403);
        if (frame.type === 'head') {
          assertApps(!stream.head && Number.isInteger(frame.status) && frame.status >= 100 && frame.status <= 599, 'app_bad_head');
          stream.head = true; clearTimeout(stream.timer); stream.timer = setTimeout(() => closeStream(stream, 'app_idle_timeout'), LIMITS.idleMs); stream.timer.unref();
          if (stream.kind === 'ws') {
            assertApps(frame.status === 101, 'app_upgrade_failed'); const headers = cleanHeaders(frame.headers, 'upgrade');
            assertApps(headers['sec-websocket-accept'] && !headers['sec-websocket-extensions'], 'app_upgrade_failed');
            stream.socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n${Object.entries(headers).map(([k, v]) => `${k}: ${v}\r\n`).join('')}\r\n`);
            void (async () => { try { if (stream.upgradeHead.length) await sendChunks(stream, stream.upgradeHead); for await (const chunk of stream.socket) await sendChunks(stream, chunk); send(channel, { type: 'end', id: stream.id }); } catch { closeStream(stream, 'app_client_closed'); } })();
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
          send(channel, { type: 'ack', id: stream.id, seq: frame.seq }); return;
        }
        if (frame.type === 'end') {
          if (stream.kind === 'ws') stream.socket.end(); else stream.res.end();
          closeStream(stream, 'app_stream_complete', false); return;
        }
        throw new AppsError('app_bad_frame');
      } catch (error) {
        if (channel && typeof error?.code === 'string' && ['app_access_revoked', 'app_response_too_large'].includes(error.code)) {
          for (const stream of channel.streams.values()) if (!canUse(stream.session.actor, row(stream.appId))) closeStream(stream, 'app_access_revoked');
        } else ws.terminate();
      }
    });
    ws.once('close', () => {
      clearTimeout(timer); if (!channel) return;
      if (channels.get(channel.key) === channel) channels.delete(channel.key);
      for (const stream of channel.streams.values()) closeStream(stream, 'app_offline', false);
    });
  }
  function allowsTlsDomain(domain) {
    if (!template.startsWith('https://') || typeof domain !== 'string' || domain.length > 253 || !/^[a-z0-9.-]+$/u.test(domain)) return false;
    const id = domain.match(/^app-[a-f0-9]{32}(?=\.)/u)?.[0];
    return Boolean(id && new URL(originFor(id)).hostname === domain && row(id)?.state === 'enabled');
  }
  return { operations, execute, handleRequest, handleUpgrade, invalidateAccess, invalidateConnector, resolveOwnedDevice, allowsTlsDomain,
    configured: Boolean(template), origins: template ? [new URL(template.replace('{appId}', 'app-00000000000000000000000000000000')).origin] : [],
    close() { if (closed) return; closed = true; clearInterval(auditTimer); clearInterval(connectorAuditTimer); unsubscribe?.(); for (const channel of channels.values()) channel.ws.terminate(); for (const stream of live.values()) closeStream(stream, 'app_server_closed', false); wss.close(); db.close(); },
  };
}
function exact(value, allowed) { assertApps(Object.keys(value).every(key => allowed.includes(key)), 'unexpected_argument'); }
function validateTemplate(value, origins) {
  if (!value) return '';
  assertApps(typeof value === 'string' && value.split('{appId}').length === 2, 'invalid_apps_origin');
  const probe = new URL(value.replace('{appId}', 'app-00000000000000000000000000000000'));
  assertApps(probe.pathname === '/' && !probe.search && !probe.hash && !probe.username && !probe.password && probe.hostname.includes('app-00000000000000000000000000000000'), 'invalid_apps_origin');
  assertApps(probe.protocol === 'https:' || (probe.protocol === 'http:' && probe.hostname.endsWith('.localhost')), 'apps_origin_requires_https');
  assertApps(!origins.has(probe.origin), 'apps_origin_not_isolated'); return value.replace(/\/$/u, '');
}
function json(res, status, body) { if (res.destroyed || res.writableEnded) return; res.statusCode = status; res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(body)); }
function respondFailure(req, res, status, error) {
  if (!String(req.headers.accept || '').includes('text/html')) { json(res, status, { ok: false, error }); return; }
  const message = ['apps_access_denied', 'app_access_revoked'].includes(error) ? ['Доступ закрыт', 'Владелец может пригласить вас снова.']
    : error === 'app_offline' ? ['Устройство не в сети', 'Приложение появится, когда устройство подключится.']
      : ['app_session_required', 'app_ticket_invalid'].includes(error) ? ['Откройте приложение снова', 'Вернитесь к его соте.']
        : error === 'app_stopped' ? ['Приложение остановлено', 'Владелец может запустить его на своём устройстве.']
          : ['Приложение недоступно', 'Попробуйте открыть его немного позже.'];
  res.statusCode = status; res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.setHeader('Cache-Control', 'no-store');
  res.end(`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>${message[0]}</title><style>body{min-height:90vh;display:grid;place-content:center;text-align:center;margin:0;padding:24px;font:16px system-ui;background:#f8f6ef;color:#30372f}span{font-size:52px;color:#b59353}h1{font-size:22px;font-weight:550;margin:18px 0 10px}p{font-size:13px;color:#787d73;max-width:290px;line-height:1.6}</style><span aria-hidden="true">⬡</span><h1>${message[0]}</h1><p>${message[1]}</p></html>`);
}
async function readBounded(stream, limit) { const parts = []; let count = 0; for await (const chunk of stream) { count += chunk.length; assertApps(count <= limit, 'app_body_too_large', 413); parts.push(chunk); } return Buffer.concat(parts); }
function writeChunk(stream, chunk) { return new Promise((resolve, reject) => { if (stream.destroyed) return reject(new AppsError('app_client_closed')); stream.write(chunk, error => error ? reject(error) : resolve()); }); }
