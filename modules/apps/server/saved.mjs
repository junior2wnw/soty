import { createHash } from 'node:crypto';
import { AppsError, assertApps, appId, appName, runtimePath, textId } from './protocol.mjs';
import { createLaunchPath } from './launch-path.mjs';

export const savedOperations = new Set(['apps.saved.get', 'apps.saved.list', 'apps.saved.set']);
export const SAVED_LIMITS = Object.freeze({ active: 200, receipts: 128, page: 20, maxPage: 50, responseBytes: 256 * 1024 });
const hash = value => createHash('sha256').update(value).digest('hex');
const exact = (value, keys) => assertApps(value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).every(key => keys.includes(key)), 'unexpected_argument');
const revision = value => { assertApps(Number.isSafeInteger(value) && value >= 0, 'invalid_saved_revision'); return value; };
const domainId = value => { assertApps(typeof value === 'string' && /^dom_[a-f0-9]{32}$/u.test(value), 'invalid_app_domain_id'); return value; };
const isAsync = value => value && typeof value.then === 'function';
function synchronous(value, code) {
  if (isAsync(value)) { Promise.resolve(value).catch(() => {}); throw new AppsError(code, 500); }
  return value;
}
const bytes = value => Buffer.byteLength(JSON.stringify(value), 'utf8');

/** Saved entries are the account's own last-chosen route, never an authorization
 * grant. The host fence must keep World authority stable around this one Apps
 * transaction; no callback in this registry may await or perform network I/O. */
export function createSavedRegistry({ db, now = Date.now, assertActor, withAuthorityFence, resolveEntry }) {
  assertApps(db && typeof now === 'function' && typeof assertActor === 'function' && typeof resolveEntry === 'function',
    'apps_saved_dependencies_required', 500);

  function authenticate(actor) {
    assertApps(synchronous(assertActor(actor), 'apps_async_authority') !== false, 'apps_authentication_required', 401);
    textId(actor?.accountId); textId(actor?.deviceId);
  }
  function run(actor, callback) {
    authenticate(actor);
    const captured = Object.freeze({ accountId: actor.accountId, deviceId: actor.deviceId });
    assertApps(typeof withAuthorityFence === 'function' && withAuthorityFence.constructor?.name !== 'AsyncFunction',
      'apps_authority_fence_required', 503);
    let active = true, entered = false, outcome;
    try {
      const value = withAuthorityFence(() => {
        assertApps(active && !entered, 'apps_authority_fence_invalid', 500); entered = true;
        assertApps(!db.isTransaction, 'apps_nested_transaction', 500);
        const priorTimeout = db.prepare('PRAGMA busy_timeout').get().timeout;
        assertApps(Number.isSafeInteger(priorTimeout) && priorTimeout >= 0, 'apps_saved_timeout_invalid', 500);
        db.exec('PRAGMA busy_timeout=100');
        try {
          db.exec('BEGIN IMMEDIATE');
          authenticate(captured);
          outcome = synchronous(callback(captured), 'apps_async_transaction');
          assertApps(bytes(outcome) <= SAVED_LIMITS.responseBytes, 'apps_saved_response_too_large', 500);
          db.exec('COMMIT'); return outcome;
        } catch (error) {
          if (db.isTransaction) db.exec('ROLLBACK');
          if (Number.isInteger(error?.errcode) && [5, 6].includes(error.errcode & 255)) throw new AppsError('apps_saved_busy', 503);
          throw error;
        } finally { db.exec(`PRAGMA busy_timeout=${priorTimeout}`); }
      });
      synchronous(value, 'apps_authority_fence_invalid');
      assertApps(entered, 'apps_authority_fence_invalid', 500);
      return outcome;
    } finally { active = false; }
  }
  function head(accountId) {
    const value = db.prepare('SELECT revision FROM app_saved_heads WHERE account_id=?').get(accountId)?.revision;
    assertApps(value === undefined || (Number.isSafeInteger(value) && value >= 1), 'apps_registry_corrupt', 500);
    return value ?? 0;
  }
  function resolved(actor, id, domain, path) {
    const result = synchronous(resolveEntry({ actor, appId: id, ...(domain === undefined ? {} : { domainId: domain }),
      ...(path === undefined ? {} : { path }) }), 'apps_async_authority');
    if (result === null) return null;
    assertApps(result && typeof result === 'object' && !Array.isArray(result) && result.appId === id
      && (domain === undefined || result.domainId === domain) && (path === undefined || result.path === path), 'apps_saved_entry_invalid', 500);
    domainId(result.domainId); createLaunchPath(result.path); appName(result.name);
    assertApps(result.name === result.name.trim() && ['ready', 'starting', 'offline', 'stopped'].includes(result.status)
      && typeof result.canManage === 'boolean' && typeof result.origin === 'string' && result.origin.length <= 512, 'apps_saved_entry_invalid', 500);
    let origin;
    try { origin = new URL(result.origin); } catch { throw new AppsError('apps_saved_entry_invalid', 500); }
    assertApps(['http:', 'https:'].includes(origin.protocol) && origin.origin === result.origin
      && !origin.username && !origin.password, 'apps_saved_entry_invalid', 500);
    // Cross-check the trusted resolver's identity against the durable address.
    const bound = db.prepare('SELECT app_id,origin FROM app_domains WHERE id=?').get(result.domainId);
    assertApps(bound?.app_id === id && bound.origin === result.origin, 'apps_saved_entry_invalid', 500);
    return { appId: id, domainId: result.domainId, origin: result.origin, path: result.path,
      name: result.name, status: result.status, canManage: result.canManage };
  }
  function entry(actor, row) {
    if (!row) return null;
    const value = resolved(actor, row.app_id, row.domain_id, row.path);
    return { appId: row.app_id, domainId: row.domain_id, origin: row.origin, path: row.path, label: row.label,
      savedRevision: row.saved_revision, updatedAt: row.updated_at,
      current: value ? { name: value.name, status: value.status, canManage: value.canManage } : null };
  }
  function current(actor, id) {
    return { revision: head(actor.accountId), entry: entry(actor,
      db.prepare('SELECT * FROM app_saved_entries WHERE account_id=? AND app_id=?').get(actor.accountId, id)) };
  }
  function receipt(row) {
    return { appId: row.app_id, saved: Boolean(row.saved), revision: row.committed_revision, committedAt: row.created_at };
  }
  function set(actor, intent) {
    const { id, saved, expectedRevision, requestId, domain, path } = intent;
    const requestKey = hash(requestId), intentHash = hash(JSON.stringify(['soty.app-saved.intent.v1', id, saved, expectedRevision, domain ?? null, path ?? null]));
    const previous = db.prepare('SELECT * FROM app_saved_receipts WHERE account_id=? AND request_key=?').get(actor.accountId, requestKey);
    if (previous) {
      assertApps(previous.intent_hash === intentHash, 'apps_saved_request_conflict', 409);
      return { requestId, replayed: true, receipt: receipt(previous), current: current(actor, id) };
    }
    const before = head(actor.accountId);
    assertApps(before === expectedRevision, 'apps_saved_revision_conflict', 409);
    assertApps(before < Number.MAX_SAFE_INTEGER, 'apps_saved_revision_exhausted', 409);
    const selected = saved ? resolved(actor, id, domain, path) : null;
    assertApps(!saved || selected !== null, 'app_unavailable', 404);
    const existing = db.prepare('SELECT 1 FROM app_saved_entries WHERE account_id=? AND app_id=?').get(actor.accountId, id);
    if (saved && !existing) assertApps(db.prepare('SELECT count(*) AS n FROM app_saved_entries WHERE account_id=?').get(actor.accountId).n < SAVED_LIMITS.active,
      'apps_saved_capacity', 409);
    const timestamp = now(), next = before + 1;
    assertApps(Number.isSafeInteger(timestamp) && timestamp >= 0, 'apps_saved_clock_invalid', 500);
    if (before === 0) db.prepare('INSERT INTO app_saved_heads VALUES (?,?)').run(actor.accountId, next);
    else db.prepare('UPDATE app_saved_heads SET revision=? WHERE account_id=?').run(next, actor.accountId);
    if (saved) db.prepare(`INSERT INTO app_saved_entries VALUES (?,?,?,?,?,?,?,?)
      ON CONFLICT(account_id,app_id) DO UPDATE SET domain_id=excluded.domain_id,origin=excluded.origin,path=excluded.path,
      label=excluded.label,saved_revision=excluded.saved_revision,updated_at=excluded.updated_at`)
      .run(actor.accountId, id, selected.domainId, selected.origin, selected.path, selected.name, next, timestamp);
    else db.prepare('DELETE FROM app_saved_entries WHERE account_id=? AND app_id=?').run(actor.accountId, id);
    db.prepare('INSERT INTO app_saved_receipts VALUES (?,?,?,?,?,?,?)').run(actor.accountId, requestKey, intentHash, id, saved ? 1 : 0, next, timestamp);
    db.prepare(`DELETE FROM app_saved_receipts WHERE account_id=? AND committed_revision NOT IN
      (SELECT committed_revision FROM app_saved_receipts WHERE account_id=? ORDER BY committed_revision DESC LIMIT ?)`)
      .run(actor.accountId, actor.accountId, SAVED_LIMITS.receipts);
    const accepted = { app_id: id, saved, committed_revision: next, created_at: timestamp };
    return { requestId, replayed: false, receipt: receipt(accepted), current: current(actor, id) };
  }
  function cursorScope(actor) { return hash(JSON.stringify(['soty.app-saved.cursor.v1', actor.accountId, actor.deviceId])); }
  function cursor(actor, revision, after) {
    return Buffer.from(JSON.stringify({ version: 1, scope: cursorScope(actor), revision, after })).toString('base64url');
  }
  function readCursor(value, actor, currentRevision) {
    assertApps(typeof value === 'string' && value.length <= 512 && /^[A-Za-z0-9_-]+$/u.test(value), 'invalid_saved_cursor');
    let parsed;
    try { parsed = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')); } catch { throw new AppsError('invalid_saved_cursor'); }
    exact(parsed, ['version', 'scope', 'revision', 'after']);
    assertApps(parsed.version === 1 && parsed.scope === cursorScope(actor) && Number.isSafeInteger(parsed.revision) && parsed.revision >= 1
      && Number.isSafeInteger(parsed.after) && parsed.after >= 1 && parsed.after <= parsed.revision
      && cursor(actor, parsed.revision, parsed.after) === value, 'invalid_saved_cursor');
    assertApps(parsed.revision === currentRevision, 'apps_saved_cursor_expired', 409);
    return parsed.after;
  }
  function list(actor, args) {
    const revision = head(actor.accountId), limit = args.limit ?? SAVED_LIMITS.page;
    const after = args.cursor === undefined ? null : readCursor(args.cursor, actor, revision);
    const rows = db.prepare(`SELECT * FROM app_saved_entries WHERE account_id=? AND (? IS NULL OR saved_revision<?)
      ORDER BY saved_revision DESC LIMIT ?`).all(actor.accountId, after, after, limit + 1);
    const entries = [];
    let nextCursor = null;
    for (let i = 0; i < Math.min(rows.length, limit); i++) {
      const value = entry(actor, rows[i]);
      const following = i + 1 < rows.length ? cursor(actor, revision, rows[i].saved_revision) : null;
      if (bytes({ revision, entries: [...entries, value], nextCursor: following }) > SAVED_LIMITS.responseBytes) {
        assertApps(entries.length > 0, 'apps_saved_response_too_large', 500);
        nextCursor = cursor(actor, revision, rows[i - 1].saved_revision); break;
      }
      entries.push(value); nextCursor = following;
    }
    return { revision, entries, nextCursor };
  }
  return Object.freeze({ execute({ op, args = {}, actor }) {
    assertApps(savedOperations.has(op), 'unknown_operation');
    if (op === 'apps.saved.get') {
      exact(args, ['appId']); const id = appId(args.appId); return run(actor, identity => current(identity, id));
    }
    if (op === 'apps.saved.list') {
      exact(args, ['limit', 'cursor']);
      assertApps(args.limit === undefined || (Number.isSafeInteger(args.limit) && args.limit >= 1 && args.limit <= SAVED_LIMITS.maxPage), 'invalid_saved_limit');
      const input = { ...args }; return run(actor, identity => list(identity, input));
    }
    exact(args, ['appId', 'saved', 'expectedRevision', 'requestId', 'domainId', 'path']);
    assertApps(typeof args.saved === 'boolean', 'invalid_saved_state');
    assertApps(args.saved || (!Object.hasOwn(args, 'domainId') && !Object.hasOwn(args, 'path')), 'unexpected_argument');
    const intent = { id: appId(args.appId), saved: args.saved, expectedRevision: revision(args.expectedRevision), requestId: textId(args.requestId),
      domain: args.domainId === undefined ? undefined : domainId(args.domainId), path: args.path === undefined ? undefined : runtimePath(args.path) };
    return run(actor, identity => set(identity, intent));
  } });
}
