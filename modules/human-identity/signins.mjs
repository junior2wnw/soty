import { createHash, createHmac, timingSafeEqual } from 'node:crypto';
import { canonicalOAuthJson } from '../capabilities/server/oauth-profile.mjs';
import { requireHuman as need, closed, data, digest } from './profile.mjs';

export const HUMAN_SIGNIN_OPERATIONS = Object.freeze(['identity.human.signins.list', 'identity.human.signins.revoke', 'identity.human.signins.checkRevoke']);
export const HUMAN_SIGNIN_LIMITS = Object.freeze({ page: 20, resultBytes: 32768, cursorBytes: 1024 });
const opaque = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value);
const revision = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const scopeOf = value => need(['current-device', 'account'].includes(value), 'human_identity_signins_scope_invalid');
const tuple = row => [row.account_id, row.device_id, row.client_id, row.profile_digest, row.client_generation];

/** Private administrative projection over the existing Human ledger. A locator
 * or a MAC is never identity: service.execute holds its original signed actor
 * fence and the synchronous SQLite transaction around every method below. */
export function createHumanSigninManagement({ db, key, keyId, issuer, schemaVersion, epoch, clientAuthority,
  actorActive, rowLimits }) {
  const available = schemaVersion === 2 || schemaVersion === 3;
  const capabilities = Object.freeze({ schema: 'soty.human-signins-capability.v1', available,
    sessions: available, rememberedConsents: schemaVersion === 3, checkRevoke: available,
    scopes: Object.freeze(available ? ['current-device', 'account'] : []),
    sourceRootSessions: false, maximumPageSize: available ? HUMAN_SIGNIN_LIMITS.page : 0 });
  const mac = (purpose, value) => createHmac('sha256', key).update(canonicalOAuthJson(
    ['soty.human-signins.v1', purpose, issuer, keyId, value])).digest('base64url');
  const same = (a, b) => typeof a === 'string' && typeof b === 'string' && a.length === b.length
    && timingSafeEqual(Buffer.from(a), Buffer.from(b));
  const identity = (kind, row) => kind === 'oidc-session'
    ? [...tuple(row), row.grant_hash, row.interaction_hash, row.created_at]
    : [...tuple(row), row.consent_hash, row.created_at, row.remembered_interaction_hash];
  const locator = entry => mac('entry', [entry.kind, identity(entry.kind, entry.row)]);
  // Family identity is immutable; only its terminal revoked_at can change.
  // Consent revision also increases on regrant, so a stale revoke cannot cross ABA.
  const expected = entry => digest(['soty.human-signin-version.v1', entry.kind,
    identity(entry.kind, entry.row), entry.kind === 'remembered-consent' ? entry.row.revision : 'active']);
  const requestId = (actor, scope, entryId, expectedRevision) =>
    mac('revoke-request', [actor.accountId, actor.deviceId, scope, entryId, expectedRevision]);
  const all = (actor, scope) => {
    need(available && db.isTransaction, 'human_identity_signins_unavailable', 503);
    const device = scope === 'current-device' ? ' AND device_id=?' : '';
    const params = [actor.accountId, ...(device ? [actor.deviceId] : [])];
    const grants = db.prepare(`SELECT b.*,CASE WHEN b.stay_in_app_seconds=86400 THEN NULL ELSE e.expires_at END AS short_expires_at
      FROM human_identity_grant_bindings b LEFT JOIN
      (SELECT grant_hash,max(expires_at) AS expires_at FROM human_identity_artifacts
       WHERE model IN ('Grant','AccessToken') AND grant_hash IS NOT NULL GROUP BY grant_hash) e ON e.grant_hash=b.grant_hash
      WHERE b.account_id=?` + device + ' LIMIT ?')
      .all(...params, rowLimits.grantBindings + 1);
    const consents = schemaVersion === 3
      ? db.prepare('SELECT * FROM human_identity_device_consents WHERE account_id=?' + device + ' LIMIT ?').all(...params, rowLimits.consents + 1) : [];
    need(grants.length <= rowLimits.grantBindings && consents.length <= rowLimits.consents,
      'human_identity_signins_capacity', 503);
    return [...grants.map(row => ({ kind: 'oidc-session', row })), ...consents.map(row => ({ kind: 'remembered-consent', row }))];
  };
  function project(entry, actor, scope) {
    const { kind, row } = entry, entryId = locator(entry), current = clientAuthority(row.client_id);
    const activeDevice = actorActive({ accountId: row.account_id, deviceId: row.device_id }) === true;
    const activeClient = Boolean(current && current.generation === row.client_generation && current.client.profileDigest === row.profile_digest);
    const expiry = kind === 'oidc-session' ? row.stay_in_app_seconds === 86400
      ? row.session_expires_at : row.short_expires_at : null;
    const expired = expiry !== null && expiry <= epoch();
    const version = expected(entry);
    return { entryId, kind, clientId: row.client_id, clientLabel: current?.client.label ?? row.client_id,
      deviceId: row.device_id, currentDevice: row.device_id === actor.deviceId,
      clientGeneration: row.client_generation, createdAt: row.created_at * 1000,
      expiresAt: expiry === null ? null : expiry * 1000,
      status: !activeDevice ? 'device-revoked' : !activeClient ? 'client-changed' : expired ? 'expired' : 'active',
      renewable: kind === 'oidc-session' && row.stay_in_app_seconds === 86400,
      remembered: kind === 'remembered-consent', revision: version,
      revokeScope: kind === 'oidc-session' ? 'exact-session' : 'app-device-version',
      revokeRequestId: requestId(actor, scope, entryId, version) };
  }
  function cursor(actor, scope, snapshot, offset) {
    const raw = Buffer.from(canonicalOAuthJson({ accountId: actor.accountId, deviceId: actor.deviceId, scope, snapshot, offset })).toString('base64url');
    return raw + '.' + mac('page', raw);
  }
  function readCursor(value, actor, scope, snapshot) {
    need(typeof value === 'string' && value.length <= HUMAN_SIGNIN_LIMITS.cursorBytes,
      'human_identity_signins_cursor_invalid');
    const parts = value.split('.'), [raw, signature] = parts;
    need(parts.length === 2 && raw && opaque(signature) && same(signature, mac('page', raw)), 'human_identity_signins_cursor_invalid');
    let parsed;
    try {
      const bytes = Buffer.from(raw, 'base64url');
      need(bytes.toString('base64url') === raw, 'human_identity_signins_cursor_invalid');
      parsed = data(JSON.parse(bytes.toString('utf8')));
    } catch { need(false, 'human_identity_signins_cursor_invalid'); }
    closed(parsed, ['accountId', 'deviceId', 'scope', 'snapshot', 'offset']);
    need(parsed.accountId === actor.accountId && parsed.deviceId === actor.deviceId && parsed.scope === scope
      && revision(parsed.snapshot) && Number.isSafeInteger(parsed.offset) && parsed.offset >= 1,
    'human_identity_signins_cursor_invalid');
    need(parsed.snapshot === snapshot, 'human_identity_signins_changed', 409);
    return parsed.offset;
  }
  function list(actor, args) {
    closed(args, ['expectedAccountId', 'scope'], ['limit', 'cursor']); scopeOf(args.scope);
    need(args.expectedAccountId === actor.accountId, 'human_identity_account_mismatch', 403);
    const limit = args.limit ?? HUMAN_SIGNIN_LIMITS.page;
    need(Number.isSafeInteger(limit) && limit >= 1 && limit <= HUMAN_SIGNIN_LIMITS.page, 'human_identity_signins_limit_invalid');
    const entries = all(actor, args.scope).filter(entry => entry.row.revoked_at === null);
    const sortKey = entry => entry.kind + ':' + (entry.row.grant_hash ?? entry.row.consent_hash);
    entries.sort((a, b) => b.row.created_at - a.row.created_at || (sortKey(a) < sortKey(b) ? -1 : sortKey(a) > sortKey(b) ? 1 : 0));
    const clients = [...new Set(entries.map(entry => entry.row.client_id))].sort();
    const heads = clients.length ? db.prepare(`SELECT client_id,profile_digest,generation,active FROM human_identity_client_heads
      WHERE client_id IN (${clients.map(() => '?').join(',')}) ORDER BY client_id`).all(...clients) : [];
    // Hash each small validated ledger record independently. Do not relax the
    // OAuth16KiB artifact grammar merely to fingerprint a bounded large list.
    const hash = createHash('sha256').update('soty.human-signins-snapshot.v1\0');
    for (const entry of entries) hash.update(digest([entry.kind, entry.row])).update('\0');
    for (const head of heads) hash.update(digest(head)).update('\0');
    const snapshot = hash.digest('hex'), offset = args.cursor === undefined ? 0 : readCursor(args.cursor, actor, args.scope, snapshot);
    need(offset <= entries.length, 'human_identity_signins_cursor_invalid');
    const page = entries.slice(offset, offset + limit).map(entry => project(entry, actor, args.scope));
    const result = { schema: 'soty.human-signins.v1', scope: args.scope, capabilities, entries: page,
      nextCursor: offset + page.length < entries.length ? cursor(actor, args.scope, snapshot, offset + page.length) : null };
    // The closed DTO is constructed here from checked scalar fields, not from
    // caller objects or token artifacts; its own API response budget is32KiB.
    need(Buffer.byteLength(JSON.stringify(result)) <= HUMAN_SIGNIN_LIMITS.resultBytes,
      'human_identity_signins_capacity', 503);
    return result;
  }
  function resolveIntent(actor, args) {
    closed(args, ['expectedAccountId', 'scope', 'entryId', 'expectedRevision', 'requestId']); scopeOf(args.scope);
    need(args.expectedAccountId === actor.accountId, 'human_identity_account_mismatch', 403);
    need(opaque(args.entryId) && revision(args.expectedRevision) && opaque(args.requestId), 'human_identity_signins_request_invalid');
    need(same(args.requestId, requestId(actor, args.scope, args.entryId, args.expectedRevision)), 'human_identity_decision_conflict', 409);
    const entry = all(actor, args.scope).find(value => same(locator(value), args.entryId));
    if (!entry) return null;
    const { row, kind } = entry;
    // A terminal consent replay checks the exact previous revision. Any regrant
    // changes its remembered interaction identity and/or revision, and fails.
    const original = kind === 'remembered-consent' && row.revoked_at !== null
      ? { ...entry, row: { ...row, revision: row.revision - 1 } } : entry;
    return { entry, matches: same(expected(original), args.expectedRevision) };
  }
  function checkRevoke(actor, args) {
    const found = resolveIntent(actor, args), exact = found?.matches ? found.entry : null;
    return { schema: 'soty.human-signin-status.v1', entryId: args.entryId, requestId: args.requestId, scope: args.scope,
      status: !exact ? 'unavailable' : exact.row.revoked_at === null ? 'active' : 'revoked',
      revokedAt: exact?.row.revoked_at == null ? null : exact.row.revoked_at * 1000 };
  }
  function revoke(actor, args) {
    const found = resolveIntent(actor, args);
    need(found, 'human_identity_signin_unavailable', 404);
    need(found.matches, 'human_identity_signins_changed', 409);
    const { row, kind } = found.entry;
    const already = row.revoked_at !== null, now = epoch();
    if (!already) {
      if (kind === 'oidc-session') {
        // Do not call revokeFamily: that existing RP/token-security policy also
        // forgets all tuple consents. This explicit command closes one family.
        const changed = db.prepare('UPDATE human_identity_grant_bindings SET revoked_at=? WHERE grant_hash=? AND revoked_at IS NULL')
          .run(now, row.grant_hash);
        need(changed.changes === 1, 'human_identity_signins_changed', 409);
      } else {
        need(schemaVersion === 3, 'human_identity_signins_unavailable', 503);
        // The target tuple is server-read. It may name another device owned by
        // the same account; it is never substituted for the original caller.
        const target = tuple(row);
        db.prepare(`UPDATE human_identity_device_consents SET revoked_at=?,updated_at=?,revision=revision+1
          WHERE account_id=? AND device_id=? AND client_id=? AND profile_digest=? AND client_generation=? AND revoked_at IS NULL`)
          .run(now, now, ...target);
        db.prepare(`UPDATE human_identity_grant_bindings SET revoked_at=?
          WHERE account_id=? AND device_id=? AND client_id=? AND profile_digest=? AND client_generation=? AND revoked_at IS NULL`)
          .run(now, ...target);
        // Approved requests of that exact person/device/version must not mint
        // a new family after forget. Pending unclaimed browser flows have no
        // actor yet and are not another person's revocable permission.
        db.prepare(`DELETE FROM human_identity_artifacts WHERE model='Interaction' AND id_hash IN
          (SELECT uid_hash FROM human_identity_interactions WHERE account_id=? AND device_id=?
           AND client_id=? AND profile_digest=? AND client_generation=? AND decision='approved')`).run(...target);
      }
    }
    return { schema: 'soty.human-signin-revocation.v1', entryId: args.entryId, requestId: args.requestId,
      scope: args.scope, kind, revokeScope: kind === 'oidc-session' ? 'exact-session' : 'app-device-version',
      outcome: already ? 'already_revoked' : 'revoked', revokedAt: (already ? row.revoked_at : now) * 1000 };
  }
  return Object.freeze({ capabilities, execute(op, actor, args) {
    need(available, 'human_identity_signins_unavailable', 503);
    if (op === HUMAN_SIGNIN_OPERATIONS[0]) return list(actor, args);
    if (op === HUMAN_SIGNIN_OPERATIONS[2]) return checkRevoke(actor, args);
    need(op === HUMAN_SIGNIN_OPERATIONS[1], 'unsupported_operation'); return revoke(actor, args);
  } });
}
