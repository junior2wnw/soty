import { DatabaseSync } from 'node:sqlite';
import { createHash, createPublicKey, ECDH, randomBytes, timingSafeEqual, verify } from 'node:crypto';
import { mkdirSync, realpathSync } from 'node:fs';
import { basename, dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { migrateDatabase, READER_EPOCH, SCHEMA_VERSION } from './schema.mjs';

export { READER_EPOCH, SCHEMA_VERSION };
const MODULE_ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
function physicalPath(file) {
  try { return realpathSync(file); }
  catch (error) {
    if (error.code !== 'ENOENT') throw error;
    const parent = dirname(file);
    if (parent === file) throw error;
    return resolve(physicalPath(parent), basename(file));
  }
}
function moduleContains(file) {
  const fold = value => process.platform === 'win32' ? value.toLowerCase() : value;
  const root = fold(physicalPath(MODULE_ROOT));
  return [file, physicalPath(file)].some(value => fold(value) === root || fold(value).startsWith(root + sep));
}
const CHALLENGE_MS = 90_000;
const ENROLLMENT_MS = 5 * 60_000;
const RECOVERY_PREPARE_MS = 15 * 60_000;
const CONTACT_MS = 7 * 24 * 60 * 60_000;
const MAX_ENVELOPE_BYTES = 2 * 1024 * 1024;
const MAX_WRAPPED_BYTES = 256 * 1024;
const MAX_ARGS_BYTES = MAX_ENVELOPE_BYTES + 16 * 1024;
const OPERATIONS = new Set([
  'bootstrap', 'status', 'profile.rename', 'card.get', 'card.rotate', 'contacts.request', 'contacts.list',
  'contacts.accept', 'contacts.decline', 'contacts.cancel', 'contacts.remove', 'contacts.block', 'contacts.unblock',
  'contacts.sendInvite', 'contacts.dismissInvite',
  'enrollment.start', 'enrollment.inspect', 'enrollment.approve', 'enrollment.preview', 'enrollment.finish',
  'device.revoke', 'vault.get', 'vault.put', 'recovery.set', 'recovery.confirm', 'recovery.use',
]);

export class ConnectError extends Error {
  constructor(code, message = code) { super(message); this.name = 'ConnectError'; this.code = code; }
}

function fail(code, message) { throw new ConnectError(code, message); }
function assert(value, code) { if (!value) fail(code); }
function record(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
}

// JSON only, sorted object keys, no undefined, prototype objects, nonfinite numbers or cycles.
// This is the local connect protocol's canonical JSON, not a replacement for STABLE JCS.
export function canonicalJson(value) {
  const seen = new Set();
  const visit = (item, depth) => {
    assert(depth <= 32, 'invalid_json');
    if (item === null || typeof item === 'boolean' || typeof item === 'string') return JSON.stringify(item);
    if (typeof item === 'number') { assert(Number.isFinite(item), 'invalid_json'); return JSON.stringify(item); }
    assert(typeof item === 'object' && !seen.has(item), 'invalid_json');
    seen.add(item);
    let result;
    if (Array.isArray(item)) {
      assert(item.length <= 50_000 && Object.keys(item).length === item.length, 'invalid_json');
      result = `[${item.map((child) => visit(child, depth + 1)).join(',')}]`;
    } else {
      assert(record(item), 'invalid_json');
      const keys = Object.keys(item).sort();
      assert(keys.length <= 10_000, 'invalid_json');
      result = `{${keys.map((key) => `${JSON.stringify(key)}:${visit(item[key], depth + 1)}`).join(',')}}`;
    }
    seen.delete(item);
    return result;
  };
  return visit(value, 0);
}

export function digestArgs(args) { return sha256(canonicalJson(args)); }
function sha256(value) { return createHash('sha256').update(value).digest('base64url'); }
function randomId(prefix) { return `${prefix}_${randomBytes(24).toString('base64url')}`; }
function exactArgs(args, required = [], optional = []) {
  assert(record(args), 'invalid_arguments');
  const allowed = new Set([...required, ...optional]);
  assert(Object.keys(args).every((key) => allowed.has(key)) && required.every((key) => Object.hasOwn(args, key)), 'invalid_arguments');
}
function textId(value) { assert(typeof value === 'string' && /^[A-Za-z0-9_-]{3,160}$/u.test(value), 'invalid_identifier'); return value; }
function label(value) {
  assert(typeof value === 'string', 'invalid_label');
  const result = value.normalize('NFC').trim();
  assert(result.length > 0 && result.length <= 80 && !/[\u0000-\u001f\u007f]/u.test(result), 'invalid_label');
  return result;
}
function base64(value, bytes, code) {
  assert(typeof value === 'string' && /^[A-Za-z0-9_-]+$/u.test(value), code);
  const decoded = Buffer.from(value, 'base64url');
  assert(decoded.length === bytes && decoded.toString('base64url') === value, code);
  return decoded;
}
function same(left, right) {
  if (typeof left !== 'string' || typeof right !== 'string') return false;
  const a = Buffer.from(left); const b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}
export function normalizePublicJwk(value) {
  assert(record(value) && value.kty === 'EC' && value.crv === 'P-256' && !Object.hasOwn(value, 'd'), 'invalid_public_key');
  assert(Object.keys(value).every((key) => ['kty', 'crv', 'x', 'y', 'ext', 'key_ops', 'use', 'alg'].includes(key)), 'invalid_public_key');
  const x = base64(value.x, 32, 'invalid_public_key');
  const y = base64(value.y, 32, 'invalid_public_key');
  try { ECDH.convertKey(Buffer.concat([Buffer.from([4]), x, y]), 'prime256v1'); }
  catch { fail('invalid_public_key'); }
  return { kty: 'EC', crv: 'P-256', x: value.x, y: value.y };
}
export function deviceIdForKey(value) { return `dev_${sha256(canonicalJson(normalizePublicJwk(value)))}`; }
function envelopeJson(value, max = MAX_ENVELOPE_BYTES) {
  assert(record(value), 'invalid_envelope');
  const encoded = canonicalJson(value);
  assert(Buffer.byteLength(encoded, 'utf8') <= max, 'envelope_too_large');
  // Reject obvious accidental plaintext key export. Ciphertext remains opaque to this service.
  const check = (item) => {
    if (!item || typeof item !== 'object') return;
    if (record(item) && item.kty) assert(!Object.hasOwn(item, 'd') && !Object.hasOwn(item, 'k'), 'private_key_forbidden');
    for (const [key, child] of Object.entries(item)) {
      assert(!['privateKey', 'privateJwk', 'rootKey', 'recoverySecret'].includes(key), 'private_key_forbidden');
      check(child);
    }
  };
  check(value);
  return encoded;
}
function publicError(error) {
  return { ok: false, error: { code: error instanceof ConnectError ? error.code : 'storage_unavailable',
    message: error instanceof ConnectError ? error.message : 'Connect storage is unavailable' } };
}
function originValue(value) {
  assert(typeof value === 'string' && value.length <= 512, 'origin_not_allowed');
  let parsed;
  try { parsed = new URL(value); } catch { fail('origin_not_allowed'); }
  assert(parsed.origin === value && !parsed.username && !parsed.password, 'origin_not_allowed');
  const local = ['localhost', '127.0.0.1', '[::1]'].includes(parsed.hostname);
  assert(parsed.protocol === 'https:' || (local && parsed.protocol === 'http:'), 'origin_not_allowed');
  return value;
}
function roomInviteUrl(value, origin) {
  assert(typeof value === 'string' && value.length <= 4096, 'invalid_invitation_url');
  let url;
  try { url = new URL(value); } catch { fail('invalid_invitation_url'); }
  const code = url.searchParams.get('j');
  assert(url.origin === origin && url.pathname === '/' && !url.hash && !url.username && !url.password
    && [...url.searchParams.keys()].length === 1 && typeof code === 'string'
    && code.length >= 4 && code.length <= 2048 && /^[A-Za-z0-9_.-]+$/u.test(code), 'invalid_invitation_url');
  return url.toString();
}

/** Local product identity + contacts. Not an OIDC provider or shared identity authority. */
export function createConnectService({ databasePath, projectId, allowedOrigins, clock = Date.now } = {}) {
  assert(typeof databasePath === 'string' && databasePath.length > 0, 'database_path_required');
  assert(typeof projectId === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(projectId), 'project_id_required');
  assert(Array.isArray(allowedOrigins) && allowedOrigins.length > 0 && allowedOrigins.length <= 64, 'allowed_origins_required');
  const origins = new Set(allowedOrigins.map(originValue));
  assert(typeof clock === 'function', 'invalid_clock');
  const file = databasePath === ':memory:' ? databasePath : resolve(databasePath);
  if (file !== ':memory:') {
    assert(!moduleContains(file), 'database_must_be_outside_module');
    mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
  }
  const db = new DatabaseSync(file);
  try { migrateDatabase(db, projectId); } catch (error) { db.close(); throw error; }
  let closed = false;
  const now = () => {
    const value = clock();
    assert(Number.isSafeInteger(value) && value >= 0, 'invalid_clock');
    return value;
  };
  const get = (sql, ...params) => db.prepare(sql).get(...params);
  const all = (sql, ...params) => db.prepare(sql).all(...params);
  const run = (sql, ...params) => db.prepare(sql).run(...params);
  function transaction(action) {
    db.exec('BEGIN IMMEDIATE');
    try { const result = action(); db.exec('COMMIT'); return result; }
    catch (error) { db.exec('ROLLBACK'); throw error; }
  }
  function hitLimit(key, max, interval, timestamp) {
    const bucket = Math.floor(timestamp / interval);
    const stored = get('SELECT bucket,count FROM rate_limits WHERE key=?', key);
    if (stored?.bucket === bucket && stored.count >= max) fail('rate_limited');
    run(`INSERT INTO rate_limits(key,bucket,count) VALUES (?,?,1)
      ON CONFLICT(key) DO UPDATE SET bucket=excluded.bucket,count=CASE WHEN rate_limits.bucket=excluded.bucket THEN rate_limits.count+1 ELSE 1 END`, key, bucket);
  }
  function cleanup(timestamp) {
    run('DELETE FROM challenges WHERE expires_at<?', timestamp - 60 * 60_000);
    run("UPDATE contact_requests SET state='expired' WHERE state='pending' AND expires_at<=?", timestamp);
    run("UPDATE recovery_methods SET state='expired' WHERE state='pending' AND expires_at<=?", timestamp);
  }
  function installation(deviceId) { return get('SELECT * FROM installations WHERE id=?', deviceId); }
  function authenticated(deviceId) {
    const device = installation(deviceId);
    assert(device && device.state === 'active' && device.account_id, device?.state === 'revoked' ? 'device_revoked' : 'authentication_required');
    const account = get('SELECT * FROM accounts WHERE id=?', device.account_id);
    assert(account, 'authentication_required');
    return { device, account };
  }
  function accountReply(accountId, deviceId) {
    const account = get('SELECT id,label FROM accounts WHERE id=?', accountId);
    return { accountId: account.id, deviceId, label: account.label };
  }
  function assertCandidate(deviceId, encryption) {
    const existing = installation(deviceId);
    assert(existing?.state !== 'revoked', 'device_revoked');
    assert(!existing || existing.state === 'pending', 'device_already_registered');
    assert(!existing || existing.encryption_jwk === encryption, 'encryption_key_mismatch');
    return existing;
  }
  function insertInstallation(ctx, encryption, deviceLabel, timestamp, accountId = null, enrolledBy = null) {
    run(`INSERT INTO installations(id,public_jwk,encryption_jwk,account_id,label,state,created_at,activated_at,enrolled_by)
      VALUES (?,?,?,?,?,?,?,?,?)`, ctx.deviceId, ctx.publicJson, encryption, accountId, deviceLabel, accountId ? 'active' : 'pending', timestamp, accountId ? timestamp : null, enrolledBy);
  }
  function activeCard(accountId, timestamp) {
    let card = get('SELECT * FROM cards WHERE account_id=? AND revoked_at IS NULL', accountId);
    if (!card) {
      const id = randomId('card');
      run('INSERT INTO cards(id,account_id,created_at) VALUES (?,?,?)', id, accountId, timestamp);
      card = get('SELECT * FROM cards WHERE id=?', id);
    }
    return card;
  }
  function blocked(left, right) {
    return Boolean(get('SELECT 1 FROM blocks WHERE (owner_id=? AND peer_id=?) OR (owner_id=? AND peer_id=?)', left, right, right, left));
  }
  function pair(left, right) { return [left, right].sort(); }
  function currentRelationship(left, right) {
    return get("SELECT * FROM relationships WHERE first_id=? AND second_id=? AND state='active'", ...pair(left, right));
  }
  function enrolment(id, timestamp, { allowFinished = false } = {}) {
    const item = get('SELECT * FROM enrollments WHERE id=?', textId(id));
    assert(item && item.state !== 'cancelled', 'enrollment_unavailable');
    if (!(item.state === 'finished' && allowFinished)) assert(item.expires_at > timestamp, 'enrollment_expired');
    return item;
  }
  function act(op, args, ctx, timestamp) {
    if (op === 'bootstrap') {
      exactArgs(args, ['label', 'encryptionPublicJwk']);
      const deviceLabel = label(args.label);
      const encryption = canonicalJson(normalizePublicJwk(args.encryptionPublicJwk));
      const known = installation(ctx.deviceId);
      if (known) {
        assert(known.state !== 'revoked', 'device_revoked');
        assert(known.state === 'active', 'enrollment_pending');
        assert(known.encryption_jwk === encryption, 'encryption_key_mismatch');
        return accountReply(known.account_id, known.id);
      }
      hitLimit('bootstrap:origin:' + ctx.origin, 120, 60_000, timestamp);
      const accountId = randomId('acct');
      run('INSERT INTO accounts(id,label,created_at) VALUES (?,?,?)', accountId, deviceLabel, timestamp);
      insertInstallation(ctx, encryption, deviceLabel, timestamp, accountId);
      return accountReply(accountId, ctx.deviceId);
    }
    if (op === 'enrollment.start') {
      exactArgs(args, ['label', 'encryptionPublicJwk']);
      const deviceLabel = label(args.label);
      const encryption = canonicalJson(normalizePublicJwk(args.encryptionPublicJwk));
      const known = assertCandidate(ctx.deviceId, encryption);
      const existing = get("SELECT * FROM enrollments WHERE device_id=? AND state IN ('pending','approved') AND expires_at>? ORDER BY created_at DESC LIMIT 1", ctx.deviceId, timestamp);
      if (existing) {
        assert(existing.start_digest === ctx.digest, 'enrollment_conflict');
        return { requestId: existing.id, expiresAt: existing.expires_at };
      }
      hitLimit('enrollment:origin:' + ctx.origin, 120, 60_000, timestamp);
      if (!known) insertInstallation(ctx, encryption, deviceLabel, timestamp);
      const requestId = randomId('enroll');
      run(`INSERT INTO enrollments(id,device_id,label,encryption_jwk,start_digest,state,created_at,expires_at)
        VALUES (?,?,?,?,?,'pending',?,?)`, requestId, ctx.deviceId, deviceLabel, encryption, ctx.digest, timestamp, timestamp + ENROLLMENT_MS);
      return { requestId, expiresAt: timestamp + ENROLLMENT_MS };
    }
    if (op === 'enrollment.preview') {
      exactArgs(args, ['requestId']);
      const item = enrolment(args.requestId, timestamp, { allowFinished: true });
      assert(item.device_id === ctx.deviceId, 'enrollment_unavailable');
      const recipient = installation(ctx.deviceId);
      assert(recipient?.state !== 'revoked', 'device_revoked');
      if (item.state === 'pending') {
        assertCandidate(ctx.deviceId, item.encryption_jwk);
        return { requestId: item.id, recipientDeviceId: ctx.deviceId, status: 'pending', expiresAt: item.expires_at, account: null, source: null };
      }
      assert(item.account_id && ['approved', 'finished'].includes(item.state), 'enrollment_unavailable');
      if (item.state === 'finished') assert(recipient?.state === 'active' && recipient.account_id === item.account_id, 'enrollment_unavailable');
      else assert(authenticated(item.approved_by).account.id === item.account_id, 'enrollment_unavailable');
      const account = get('SELECT id,label FROM accounts WHERE id=?', item.account_id);
      const source = installation(item.approved_by);
      assert(account && source, 'enrollment_unavailable');
      // No encrypted account key or account mutation is exposed by this read.
      return { requestId: item.id, recipientDeviceId: ctx.deviceId, status: item.state, expiresAt: item.expires_at,
        account: { accountId: account.id, label: account.label }, source: { deviceId: source.id, label: source.label } };
    }
    if (op === 'enrollment.finish') {
      exactArgs(args, ['requestId', 'expectedAccountId']);
      const expectedAccountId = textId(args.expectedAccountId);
      const item = enrolment(args.requestId, timestamp, { allowFinished: true });
      assert(item.device_id === ctx.deviceId, 'enrollment_unavailable');
      const device = installation(ctx.deviceId);
      assert(device?.state !== 'revoked', 'device_revoked');
      assert(item.account_id, 'enrollment_not_approved');
      assert(item.account_id === expectedAccountId, 'enrollment_account_mismatch');
      if (item.state === 'finished') {
        assert(device?.state === 'active' && device.account_id === item.account_id, 'enrollment_unavailable');
        return JSON.parse(item.receipt_json);
      }
      assert(item.state === 'approved' && item.account_id, 'enrollment_not_approved');
      const source = authenticated(item.approved_by);
      assert(source.account.id === item.account_id, 'enrollment_unavailable');
      assertCandidate(ctx.deviceId, item.encryption_jwk);
      run("UPDATE installations SET account_id=?,label=?,state='active',activated_at=?,enrolled_by=? WHERE id=? AND state='pending'", item.account_id, item.label, timestamp, item.approved_by, ctx.deviceId);
      const result = { ...accountReply(item.account_id, ctx.deviceId), wrappedKey: JSON.parse(item.wrapped_key) };
      run("UPDATE enrollments SET state='finished',finished_at=?,receipt_until=NULL,receipt_json=? WHERE id=?", timestamp, canonicalJson(result), item.id);
      return result;
    }
    if (op === 'recovery.use') {
      exactArgs(args, ['accountId', 'recoveryId', 'secret', 'encryptionPublicJwk', 'label']);
      const accountId = textId(args.accountId);
      const recoveryId = textId(args.recoveryId);
      assert(typeof args.secret === 'string' && /^[A-Za-z0-9_-]{43,128}$/u.test(args.secret), 'recovery_unavailable');
      const method = get('SELECT * FROM recovery_methods WHERE id=? AND account_id=?', recoveryId, accountId);
      assert(method && same(method.verifier, sha256(args.secret)), 'recovery_unavailable');
      const known = installation(ctx.deviceId);
      assert(known?.state !== 'revoked', 'device_revoked');
      if (method.state === 'consumed') {
        assert(method.consumed_by === ctx.deviceId && method.consumed_digest === ctx.digest
          && known?.state === 'active' && known.account_id === accountId, 'recovery_unavailable');
        return JSON.parse(method.receipt_json);
      }
      assert(method.state === 'verified', 'recovery_unavailable');
      const encryption = canonicalJson(normalizePublicJwk(args.encryptionPublicJwk));
      const deviceLabel = label(args.label);
      assertCandidate(ctx.deviceId, encryption);
      if (known) run("UPDATE installations SET account_id=?,label=?,state='active',activated_at=?,enrolled_by=? WHERE id=?", accountId, deviceLabel, timestamp, recoveryId, ctx.deviceId);
      else insertInstallation(ctx, encryption, deviceLabel, timestamp, accountId, recoveryId);
      run("UPDATE enrollments SET state='cancelled' WHERE device_id=? AND state IN ('pending','approved')", ctx.deviceId);
      const result = { ...accountReply(accountId, ctx.deviceId), wrappedKey: JSON.parse(method.wrapped_key), recoveryConsumed: true };
      run(`UPDATE recovery_methods SET state='consumed',consumed_at=?,consumed_by=?,consumed_digest=?,receipt_until=NULL,receipt_json=? WHERE id=?`,
        timestamp, ctx.deviceId, ctx.digest, canonicalJson(result), method.id);
      return result;
    }

    const { device, account } = authenticated(ctx.deviceId);
    if (op === 'profile.rename') {
      exactArgs(args, ['label']);
      run('UPDATE accounts SET label=? WHERE id=?', label(args.label), account.id);
      return accountReply(account.id, device.id);
    }
    if (op === 'status') {
      exactArgs(args);
      const recovery = get("SELECT id FROM recovery_methods WHERE account_id=? AND state='verified'", account.id);
      return { ...accountReply(account.id, device.id), projectId,
        devices: all('SELECT id,label,state,created_at,activated_at,revoked_at FROM installations WHERE account_id=? ORDER BY created_at,id', account.id)
          .map((item) => ({ deviceId: item.id, label: item.label, state: item.state, createdAt: item.created_at, activatedAt: item.activated_at, revokedAt: item.revoked_at })),
        recovery: { verified: Boolean(recovery), ...(recovery ? { recoveryId: recovery.id } : {}) } };
    }
    if (op === 'card.get' || op === 'card.rotate') {
      exactArgs(args);
      if (op === 'card.rotate') {
        hitLimit('card-rotate:' + account.id, 30, 60 * 60_000, timestamp);
        run("UPDATE contact_requests SET state='cancelled' WHERE recipient_id=? AND state='pending'", account.id);
        run('UPDATE cards SET revoked_at=? WHERE account_id=? AND revoked_at IS NULL', timestamp, account.id);
      }
      return { cardId: activeCard(account.id, timestamp).id, label: account.label, projectId, purpose: 'contact.discover' };
    }
    if (op === 'contacts.request') {
      exactArgs(args, ['cardId']);
      const card = get('SELECT * FROM cards WHERE id=? AND revoked_at IS NULL', textId(args.cardId));
      assert(card && card.account_id !== account.id && !blocked(account.id, card.account_id), 'contact_unavailable');
      const relation = currentRelationship(account.id, card.account_id);
      if (relation) return { relationshipId: relation.id, status: 'active' };
      const existing = get("SELECT * FROM contact_requests WHERE state='pending' AND ((sender_id=? AND recipient_id=?) OR (sender_id=? AND recipient_id=?)) ORDER BY created_at LIMIT 1", account.id, card.account_id, card.account_id, account.id);
      if (existing) return { requestId: existing.id, status: 'pending', direction: existing.sender_id === account.id ? 'outgoing' : 'incoming' };
      hitLimit('contact-request:' + account.id, 30, 60 * 60_000, timestamp);
      assert(get("SELECT count(*) AS n FROM contact_requests WHERE sender_id=? AND state='pending'", account.id).n < 50, 'request_limit');
      const requestId = randomId('contact');
      run(`INSERT INTO contact_requests(id,sender_id,recipient_id,card_id,state,created_at,expires_at) VALUES (?,?,?,?,'pending',?,?)`, requestId, account.id, card.account_id, card.id, timestamp, timestamp + CONTACT_MS);
      return { requestId, status: 'pending', direction: 'outgoing' };
    }
    if (op === 'contacts.list') {
      exactArgs(args);
      const requests = all(`SELECT r.*,a.label AS peer_label FROM contact_requests r JOIN accounts a ON a.id=CASE WHEN r.sender_id=? THEN r.recipient_id ELSE r.sender_id END
        WHERE r.state='pending' AND (r.sender_id=? OR r.recipient_id=?) ORDER BY r.created_at,r.id`, account.id, account.id, account.id);
      const shape = (item) => ({ requestId: item.id, peerAccountId: item.sender_id === account.id ? item.recipient_id : item.sender_id,
        label: item.peer_label, createdAt: item.created_at, expiresAt: item.expires_at });
      return { requests: { incoming: requests.filter((r) => r.recipient_id === account.id).map(shape), outgoing: requests.filter((r) => r.sender_id === account.id).map(shape) },
        contacts: all(`SELECT r.*,a.id AS peer_id,a.label AS peer_label FROM relationships r JOIN accounts a ON a.id=CASE WHEN r.first_id=? THEN r.second_id ELSE r.first_id END
          WHERE r.state='active' AND (r.first_id=? OR r.second_id=?) ORDER BY r.created_at,r.id`, account.id, account.id, account.id)
          .map((r) => ({ relationshipId: r.id, peerAccountId: r.peer_id, label: r.peer_label, createdAt: r.created_at })),
        invitations: all(`SELECT i.*,a.label AS peer_label FROM contact_invitations i JOIN accounts a ON a.id=i.sender_id
          JOIN relationships r ON r.id=i.relationship_id AND r.state='active'
          WHERE i.recipient_id=? AND i.dismissed_at IS NULL AND i.invalidated_at IS NULL AND i.expires_at>?
          AND NOT EXISTS (SELECT 1 FROM blocks b WHERE (b.owner_id=i.sender_id AND b.peer_id=i.recipient_id) OR (b.owner_id=i.recipient_id AND b.peer_id=i.sender_id))
          ORDER BY i.created_at,i.id`, account.id, timestamp)
          .map((r) => ({ invitationId: r.id, relationshipId: r.relationship_id, peerAccountId: r.sender_id, peerLabel: r.peer_label,
            label: r.label, url: r.url, createdAt: r.created_at, expiresAt: r.expires_at })),
        blocked: all('SELECT b.peer_id,a.label FROM blocks b JOIN accounts a ON a.id=b.peer_id WHERE b.owner_id=? ORDER BY b.created_at', account.id)
          .map((r) => ({ peerAccountId: r.peer_id, label: r.label })) };
    }
    if (op === 'contacts.sendInvite') {
      exactArgs(args, ['relationshipId', 'url', 'label']);
      const relation = get("SELECT * FROM relationships WHERE id=? AND state='active'", textId(args.relationshipId));
      assert(relation && (relation.first_id === account.id || relation.second_id === account.id), 'contact_unavailable');
      const peerId = relation.first_id === account.id ? relation.second_id : relation.first_id;
      assert(!blocked(account.id, peerId), 'contact_unavailable');
      const url = roomInviteUrl(args.url, ctx.origin);
      const inviteLabel = label(args.label);
      const existing = get(`SELECT id FROM contact_invitations WHERE relationship_id=? AND sender_id=? AND url=? AND label=?
        AND dismissed_at IS NULL AND invalidated_at IS NULL AND expires_at>?`, relation.id, account.id, url, inviteLabel, timestamp);
      if (existing) return { invitationId: existing.id, status: 'pending' };
      hitLimit('contact-invite:' + account.id, 60, 60 * 60_000, timestamp);
      const invitationId = randomId('invite');
      run(`INSERT INTO contact_invitations(id,relationship_id,sender_id,recipient_id,url,label,created_at,expires_at)
        VALUES (?,?,?,?,?,?,?,?)`, invitationId, relation.id, account.id, peerId, url, inviteLabel, timestamp, timestamp + CONTACT_MS);
      return { invitationId, status: 'pending' };
    }
    if (op === 'contacts.dismissInvite') {
      exactArgs(args, ['invitationId']);
      const invite = get('SELECT * FROM contact_invitations WHERE id=? AND recipient_id=?', textId(args.invitationId), account.id);
      assert(invite, 'invitation_unavailable');
      run('UPDATE contact_invitations SET dismissed_at=COALESCE(dismissed_at,?) WHERE id=?', timestamp, invite.id);
      return { dismissed: true };
    }
    if (op === 'contacts.accept' || op === 'contacts.decline' || op === 'contacts.cancel') {
      exactArgs(args, ['requestId']);
      const request = get('SELECT * FROM contact_requests WHERE id=?', textId(args.requestId));
      assert(request && (op === 'contacts.cancel' ? request.sender_id === account.id : request.recipient_id === account.id), 'contact_unavailable');
      if (op === 'contacts.decline') {
        assert(request.state !== 'accepted', 'request_already_accepted');
        if (request.state === 'pending') run("UPDATE contact_requests SET state='declined' WHERE id=?", request.id);
        return { declined: true };
      }
      if (op === 'contacts.cancel') {
        assert(request.state !== 'accepted', 'request_already_accepted');
        if (request.state === 'pending') run("UPDATE contact_requests SET state='cancelled' WHERE id=?", request.id);
        return { cancelled: true };
      }
      assert(!blocked(request.sender_id, request.recipient_id), 'contact_unavailable');
      if (request.state === 'accepted') {
        const relation = currentRelationship(request.sender_id, request.recipient_id);
        assert(relation && relation.id === request.relationship_id, 'contact_unavailable');
        return { relationshipId: relation.id, status: 'active' };
      }
      assert(request.state === 'pending' && request.expires_at > timestamp, 'contact_unavailable');
      assert(get('SELECT 1 FROM cards WHERE id=? AND revoked_at IS NULL', request.card_id), 'contact_unavailable');
      const [first, second] = pair(request.sender_id, request.recipient_id);
      const relationshipId = randomId('rel');
      run("INSERT INTO relationships(id,first_id,second_id,state,created_at) VALUES (?,?,?,'active',?)", relationshipId, first, second, timestamp);
      run("UPDATE contact_requests SET state='accepted',relationship_id=? WHERE id=?", relationshipId, request.id);
      return { relationshipId, status: 'active' };
    }
    if (op === 'contacts.remove') {
      exactArgs(args, ['relationshipId']);
      const relation = get('SELECT * FROM relationships WHERE id=?', textId(args.relationshipId));
      assert(relation && (relation.first_id === account.id || relation.second_id === account.id), 'contact_unavailable');
      run("UPDATE relationships SET state='ended',ended_at=COALESCE(ended_at,?) WHERE id=?", timestamp, relation.id);
      run('UPDATE contact_invitations SET invalidated_at=COALESCE(invalidated_at,?) WHERE relationship_id=?', timestamp, relation.id);
      return { removed: true };
    }
    if (op === 'contacts.block') {
      exactArgs(args, ['peerAccountId']);
      const peerId = textId(args.peerAccountId);
      assert(peerId !== account.id && get('SELECT 1 FROM accounts WHERE id=?', peerId), 'contact_unavailable');
      run('INSERT OR IGNORE INTO blocks(owner_id,peer_id,created_at) VALUES (?,?,?)', account.id, peerId, timestamp);
      run("UPDATE contact_requests SET state='cancelled' WHERE state='pending' AND ((sender_id=? AND recipient_id=?) OR (sender_id=? AND recipient_id=?))", account.id, peerId, peerId, account.id);
      run("UPDATE relationships SET state='ended',ended_at=? WHERE first_id=? AND second_id=? AND state='active'", timestamp, ...pair(account.id, peerId));
      run('UPDATE contact_invitations SET invalidated_at=COALESCE(invalidated_at,?) WHERE (sender_id=? AND recipient_id=?) OR (sender_id=? AND recipient_id=?)', timestamp, account.id, peerId, peerId, account.id);
      return { blocked: true };
    }
    if (op === 'contacts.unblock') {
      exactArgs(args, ['peerAccountId']);
      run('DELETE FROM blocks WHERE owner_id=? AND peer_id=?', account.id, textId(args.peerAccountId));
      return { unblocked: true };
    }
    if (op === 'enrollment.inspect') {
      exactArgs(args, ['requestId']);
      const item = enrolment(args.requestId, timestamp);
      assert(item.state === 'pending' || item.account_id === account.id, 'enrollment_unavailable');
      const recipient = installation(item.device_id);
      assert(recipient?.state !== 'revoked', 'enrollment_unavailable');
      return { requestId: item.id, deviceId: item.device_id, label: item.label, publicJwk: JSON.parse(recipient.public_jwk),
        encryptionPublicJwk: JSON.parse(item.encryption_jwk), expiresAt: item.expires_at, status: item.state, projectId, accountId: account.id };
    }
    if (op === 'enrollment.approve') {
      exactArgs(args, ['requestId', 'wrappedKey']);
      const item = enrolment(args.requestId, timestamp);
      const wrapped = envelopeJson(args.wrappedKey, MAX_WRAPPED_BYTES);
      assertCandidate(item.device_id, item.encryption_jwk);
      if (item.state === 'approved') {
        assert(item.account_id === account.id && item.approved_by === device.id && item.wrapped_key === wrapped, 'enrollment_conflict');
        return { requestId: item.id, approved: true };
      }
      assert(item.state === 'pending', 'enrollment_unavailable');
      run("UPDATE enrollments SET state='approved',account_id=?,approved_by=?,approved_at=?,wrapped_key=? WHERE id=?", account.id, device.id, timestamp, wrapped, item.id);
      return { requestId: item.id, approved: true };
    }
    if (op === 'device.revoke') {
      exactArgs(args, ['deviceId']);
      const target = installation(textId(args.deviceId));
      assert(target?.account_id === account.id, 'device_not_found');
      if (target.state === 'revoked') return { revoked: true, deviceId: target.id };
      const count = get("SELECT count(*) AS n FROM installations WHERE account_id=? AND state='active'", account.id).n;
      const recovery = get("SELECT 1 FROM recovery_methods WHERE account_id=? AND state='verified'", account.id);
      assert(count > 1 || recovery, 'last_device_requires_recovery');
      run("UPDATE installations SET state='revoked',revoked_at=? WHERE id=?", timestamp, target.id);
      run("UPDATE enrollments SET state='cancelled' WHERE state IN ('pending','approved') AND (approved_by=? OR device_id=?)", target.id, target.id);
      return { revoked: true, deviceId: target.id };
    }
    if (op === 'vault.get') {
      exactArgs(args);
      const vault = get('SELECT revision,envelope FROM vaults WHERE account_id=?', account.id);
      return vault ? { revision: vault.revision, envelope: JSON.parse(vault.envelope) } : { revision: 0, envelope: null };
    }
    if (op === 'vault.put') {
      exactArgs(args, ['expectedRevision', 'envelope']);
      assert(Number.isSafeInteger(args.expectedRevision) && args.expectedRevision >= 0, 'invalid_revision');
      const envelope = envelopeJson(args.envelope);
      const existing = get('SELECT revision FROM vaults WHERE account_id=?', account.id);
      assert((existing?.revision ?? 0) === args.expectedRevision, 'revision_conflict');
      const revision = args.expectedRevision + 1;
      assert(Number.isSafeInteger(revision), 'invalid_revision');
      run(`INSERT INTO vaults(account_id,revision,envelope,updated_at) VALUES (?,?,?,?)
        ON CONFLICT(account_id) DO UPDATE SET revision=excluded.revision,envelope=excluded.envelope,updated_at=excluded.updated_at`, account.id, revision, envelope, timestamp);
      return { revision };
    }
    if (op === 'recovery.set') {
      exactArgs(args, ['verifier', 'wrappedKey']);
      base64(args.verifier, 32, 'invalid_verifier');
      const wrapped = envelopeJson(args.wrappedKey, MAX_WRAPPED_BYTES);
      const reused = get('SELECT * FROM recovery_methods WHERE account_id=? AND verifier=? ORDER BY created_at DESC LIMIT 1', account.id, args.verifier);
      if (reused) {
        assert(reused.state === 'pending' && reused.wrapped_key === wrapped && reused.expires_at > timestamp, 'recovery_material_reused');
        return { recoveryId: reused.id, verified: false, expiresAt: reused.expires_at };
      }
      run("UPDATE recovery_methods SET state='superseded' WHERE account_id=? AND state='pending'", account.id);
      const recoveryId = randomId('recovery');
      run(`INSERT INTO recovery_methods(id,account_id,verifier,wrapped_key,state,created_by,created_at,expires_at)
        VALUES (?,?,?,?,'pending',?,?,?)`, recoveryId, account.id, args.verifier, wrapped, device.id, timestamp, timestamp + RECOVERY_PREPARE_MS);
      return { recoveryId, verified: false, expiresAt: timestamp + RECOVERY_PREPARE_MS };
    }
    if (op === 'recovery.confirm') {
      exactArgs(args, ['recoveryId', 'verifier']);
      const method = get('SELECT * FROM recovery_methods WHERE id=? AND account_id=?', textId(args.recoveryId), account.id);
      assert(method && same(method.verifier, args.verifier), 'recovery_unavailable');
      if (method.state === 'verified') return { recoveryId: method.id, verified: true };
      assert(method.state === 'pending' && method.expires_at > timestamp, 'recovery_unavailable');
      run("UPDATE recovery_methods SET state='superseded' WHERE account_id=? AND state='verified'", account.id);
      run("UPDATE recovery_methods SET state='verified',verified_at=?,expires_at=NULL WHERE id=?", timestamp, method.id);
      return { recoveryId: method.id, verified: true };
    }
    fail('unsupported_operation');
  }

  return {
    projectId,
    schemaVersion: SCHEMA_VERSION,
    readerEpoch: READER_EPOCH,
    close() { if (!closed) { closed = true; db.close(); } },
    async handle(input) {
      try {
        assert(!closed, 'service_closed');
        assert(record(input), 'invalid_request');
        const origin = originValue(input.origin);
        assert(origins.has(origin), 'origin_not_allowed');
        const { op, args = {} } = input;
        assert(typeof op === 'string', 'unsupported_operation');
        const argsJson = canonicalJson(args);
        assert(Buffer.byteLength(argsJson, 'utf8') <= MAX_ARGS_BYTES, 'arguments_too_large');
        const timestamp = now();
        if (op === 'challenge') {
          exactArgs(args, ['operation', 'digest']);
          assert(OPERATIONS.has(args.operation), 'unsupported_operation');
          base64(args.digest, 32, 'invalid_digest');
          return transaction(() => {
            cleanup(timestamp);
            hitLimit('challenge:origin:' + origin, 1200, 60_000, timestamp);
            const challengeId = randomId('challenge');
            const expiresAt = timestamp + CHALLENGE_MS;
            const message = canonicalJson({ schema: 'connect.proof.v1', projectId, origin, operation: args.operation, digest: args.digest, challengeId, expiresAt });
            run('INSERT INTO challenges(id,origin,operation,digest,message,expires_at) VALUES (?,?,?,?,?,?)', challengeId, origin, args.operation, args.digest, message, expiresAt);
            return { ok: true, challengeId, message, expiresAt };
          });
        }
        if (op === 'card.resolve') {
          exactArgs(args, ['cardId']);
          return transaction(() => {
            hitLimit('card-resolve:origin:' + origin, 1200, 60_000, timestamp);
            try {
              const card = get('SELECT c.id,a.label FROM cards c JOIN accounts a ON a.id=c.account_id WHERE c.id=? AND c.revoked_at IS NULL', textId(args.cardId));
              assert(card, 'card_unavailable');
              return { ok: true, cardId: card.id, label: card.label, projectId, purpose: 'contact.discover' };
            } catch (error) { return publicError(error); }
          });
        }
        assert(OPERATIONS.has(op), 'unsupported_operation');
        const proof = input.proof;
        assert(record(proof), 'proof_required');
        textId(proof.challengeId);
        const publicJwk = normalizePublicJwk(proof.publicJwk);
        const publicJson = canonicalJson(publicJwk);
        const deviceId = deviceIdForKey(publicJwk);
        const signature = base64(proof.signature, 64, 'invalid_signature');
        const digest = sha256(argsJson);
        return transaction(() => {
          cleanup(timestamp);
          const challenge = get('SELECT * FROM challenges WHERE id=?', proof.challengeId);
          assert(challenge && challenge.expires_at > timestamp, 'challenge_expired');
          assert(challenge.consumed_at === null, 'challenge_consumed');
          assert(challenge.origin === origin && challenge.operation === op && same(challenge.digest, digest), 'challenge_mismatch');
          let valid = false;
          try { valid = verify('sha256', Buffer.from(challenge.message, 'utf8'), { key: createPublicKey({ key: publicJwk, format: 'jwk' }), dsaEncoding: 'ieee-p1363' }, signature); }
          catch { /* Return only a fixed, non-sensitive error. */ }
          assert(valid, 'invalid_signature');
          run('UPDATE challenges SET consumed_at=? WHERE id=?', timestamp, challenge.id);
          // Valid proofs remain consumed even if the requested operation fails. All operation
          // writes roll back together, independently of the proof and persisted rate limit.
          try {
            hitLimit('signed:' + deviceId, 240, 60_000, timestamp);
            if (op === 'recovery.use') hitLimit('recovery:origin:' + origin, 120, 60_000, timestamp);
          } catch (error) { return publicError(error); }
          db.exec('SAVEPOINT connect_action');
          try {
            const result = act(op, args, { origin, publicJwk, publicJson, deviceId, digest }, timestamp);
            db.exec('RELEASE connect_action');
            return { ok: true, ...result };
          } catch (error) {
            db.exec('ROLLBACK TO connect_action');
            db.exec('RELEASE connect_action');
            return publicError(error);
          }
        });
      } catch (error) { return publicError(error); }
    },
  };
}
