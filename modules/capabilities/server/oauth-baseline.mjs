import { createHash } from 'node:crypto';
import { AccessError, canonicalJson } from './validation.mjs';

const fail = () => { throw new AccessError('capabilities_storage_corrupt'); };
const check = value => { if (!value) fail(); };
const safe = value => Number.isSafeInteger(value) && value >= 0;
const id = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$/u.test(value) && !value.includes('..');
const providerId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{16,160}$/u.test(value);
const hash = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const digest = value => createHash('sha256').update(value).digest('hex');
const client = value => ['soty-codex-cli', 'soty-opencode-cli'].includes(value);
const nullableTime = value => value === null || safe(value);
const scope = Object.freeze({ capabilities_json: '[{"capabilityId":"notes.createDraft","version":1}]',
  resources_json: '["notes:new"]', effects_json: '["create"]', recipients_json: '["soty:notes"]' });

function uri(value) {
  check(typeof value === 'string' && value.isWellFormed() && Buffer.byteLength(value) <= 2048 && !/[\s#\\]/u.test(value));
  let url; try { url = new URL(value); } catch { fail(); }
  check(!url.username && !url.password && (url.protocol === 'https:'
    || (url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))));
  check(value === url.href || value === url.origin);
  return url;
}
function issuer(value) { const url = uri(value); check(value === `${url.origin}/oauth`); return url.origin; }
function nativeRedirect(value) {
  check(typeof value === 'string' && value.isWellFormed() && Buffer.byteLength(value) <= 2048);
  const parts = /^http:\/\/(localhost|127\.0\.0\.1|\[::1\]):([1-9][0-9]{0,4})(\/[^\s?#\\]*)$/u.exec(value);
  check(parts && Number(parts[2]) <= 65535);
  check(new URL(value).pathname === parts[3]);
}
function authorityUris(row) {
  const origin = issuer(row.issuer);
  uri(row.resource);
  check(row.resource === origin || row.resource === `${origin}/mcp`);
  check(client(row.static_client_id));
}
function connectionCheck(db, row) {
  for (const field of ['id', 'account_id', 'client_id', 'principal_id', 'root_grant_id', 'creator_device_id']) check(id(row[field]));
  authorityUris(row);
  check(row.scope === 'notes.createDraft' && hash(row.consent_digest));
  check(row.provider_grant_id === null || providerId(row.provider_grant_id));
  check(safe(row.created_at) && safe(row.expires_at) && row.expires_at > row.created_at && row.expires_at - row.created_at <= 86400000);
  check(nullableTime(row.revoked_at) && ((row.state === 'active' && row.revoked_at === null)
    || (row.state === 'revoked' && row.revoked_at !== null && row.revoked_at >= row.created_at)));
  const c = db.prepare('SELECT * FROM cap_clients WHERE id=?').get(row.client_id);
  const p = db.prepare('SELECT * FROM cap_principals WHERE id=?').get(row.principal_id);
  const g = db.prepare('SELECT * FROM cap_grants WHERE id=?').get(row.root_grant_id);
  const b = db.prepare("SELECT * FROM cap_budgets WHERE root_grant_id=? AND unit='invocations'").get(row.root_grant_id);
  check(c && p && g && b && c.account_id === row.account_id && p.account_id === row.account_id && g.account_id === row.account_id);
  check(p.client_id === c.id && p.kind === 'service' && p.creator_device_id === row.creator_device_id
    && g.client_id === c.id && g.principal_id === p.id && g.creator_device_id === row.creator_device_id);
  for (const parent of [c, p]) {
    check(safe(parent.created_at) && parent.created_at <= row.created_at && nullableTime(parent.revoked_at));
    check((parent.state === 'active' && parent.revoked_at === null)
      || (parent.state === 'revoked' && parent.revoked_at !== null && parent.revoked_at >= parent.created_at));
  }
  check(safe(c.policy_epoch) && c.policy_epoch > 0 && safe(g.policy_epoch) && g.policy_epoch > 0);
  check(g.root_id === g.id && g.parent_id === null && g.depth === 0 && g.max_depth === 0 && g.allow_delegation === 0);
  check(safe(g.created_at) && safe(g.not_before) && g.created_at <= row.created_at && g.not_before <= row.created_at
    && g.expires_at === row.expires_at && nullableTime(g.revoked_at) && (g.revoked_at === null || g.revoked_at >= g.created_at));
  for (const [field, expected] of Object.entries(scope)) {
    let parsed; try { parsed = canonicalJson(JSON.parse(g[field])); } catch { fail(); }
    check(parsed === expected);
  }
  check(safe(b.limit_amount) && b.limit_amount >= 1 && b.limit_amount <= 20 && safe(b.reserved_amount)
    && safe(b.spent_amount) && b.reserved_amount + b.spent_amount <= b.limit_amount);
  if (row.state === 'revoked') check(g.revoked_at !== null);
  return { client: c, principal: p, grant: g, budget: b };
}

function artifactCheck(db, row) {
  check(['Session', 'Interaction', 'Grant', 'AuthorizationCode', 'RefreshToken', 'AccessToken'].includes(row.model));
  issuer(row.issuer);
  check(hash(row.id_hash) && hash(row.payload_digest) && row.profile === 'oidc-provider-9.12.2-c1');
  check(typeof row.key_id === 'string' && /^[A-Za-z0-9._:-]{1,64}$/u.test(row.key_id));
  check(row.payload_cipher instanceof Uint8Array && row.payload_cipher.byteLength >= 30 && row.payload_cipher.byteLength <= 16412);
  check(safe(row.created_at) && safe(row.expires_at) && safe(row.retain_until)
    && row.expires_at > row.created_at && row.retain_until >= row.expires_at);
  check(row.session_uid_hash === null || hash(row.session_uid_hash));
  check(nullableTime(row.consumed_at) && (row.consumed_at === null || (['AuthorizationCode', 'RefreshToken'].includes(row.model)
    && row.consumed_at >= row.created_at && row.consumed_at < row.expires_at)));
  if (['Session', 'Interaction'].includes(row.model)) {
    check(row.connection_id === null && row.provider_grant_id === null && row.retain_until - row.created_at <= 600000);
    check(row.model !== 'Session' || hash(row.session_uid_hash));
    return;
  }
  check(id(row.connection_id) && providerId(row.provider_grant_id));
  const connection = db.prepare('SELECT * FROM cap_oauth_connections WHERE id=?').get(row.connection_id);
  check(connection && row.issuer === connection.issuer && row.provider_grant_id === connection.provider_grant_id
    && row.created_at >= connection.created_at && row.expires_at <= connection.expires_at);
  if (row.model === 'Grant') check(row.id_hash === digest(row.provider_grant_id));
  if (row.model === 'AuthorizationCode') check(row.expires_at - row.created_at <= 60000);
  if (row.model === 'AccessToken') check(row.expires_at - row.created_at <= 300000 && row.retain_until === row.expires_at);
  else check(row.retain_until === connection.expires_at);
  if (row.model === 'AccessToken') {
    const link = db.prepare('SELECT * FROM cap_oauth_credentials WHERE token_digest=?').get(row.id_hash);
    check(link && link.connection_id === row.connection_id && link.created_at === row.created_at && link.expires_at === row.expires_at);
  }
}

function credentialCheck(db, link, connection, row) {
  check(link && row && connection && id(link.credential_id) && id(link.connection_id) && hash(link.token_digest));
  check(link.credential_id === row.id && link.connection_id === connection.id && link.token_digest === row.digest);
  check(row.account_id === connection.account_id && row.client_id === connection.client_id
    && row.principal_id === connection.principal_id && row.grant_id === connection.root_grant_id && row.audience === connection.resource);
  check(safe(link.created_at) && safe(link.expires_at) && link.expires_at > link.created_at
    && link.expires_at - link.created_at <= 300000 && link.created_at >= connection.created_at && link.expires_at <= connection.expires_at);
  check(row.created_at === link.created_at && row.expires_at === link.expires_at && nullableTime(row.revoked_at)
    && (row.revoked_at === null || row.revoked_at >= row.created_at) && connection.provider_grant_id !== null);
  if (connection.state === 'revoked') check(row.revoked_at !== null);
  // Original credentials legitimately outlive their expired encrypted AT.
  const artifact = db.prepare("SELECT * FROM cap_oauth_artifacts WHERE model='AccessToken' AND id_hash=?").get(link.token_digest);
  if (artifact) check(artifact.connection_id === connection.id && artifact.provider_grant_id === connection.provider_grant_id
    && artifact.issuer === connection.issuer && artifact.created_at === link.created_at && artifact.expires_at === link.expires_at);
}

/** Plain storage invariants only. Ciphertext authenticity needs the later AS port/key. */
export function validateOAuthStorageRows(db) {
  try {
    for (const connection of db.prepare('SELECT * FROM cap_oauth_connections').iterate()) connectionCheck(db, connection);
    for (const row of db.prepare('SELECT * FROM cap_oauth_interactions').iterate()) {
      authorityUris(row); nativeRedirect(row.redirect_uri);
      check(hash(row.uid_hash) && hash(row.request_digest) && hash(row.browser_nonce_hash));
      check(safe(row.duration_ms) && row.duration_ms >= 1000 && row.duration_ms <= 86400000
        && safe(row.budget_limit) && row.budget_limit >= 1 && row.budget_limit <= 20);
      check(safe(row.created_at) && safe(row.expires_at) && row.expires_at > row.created_at && row.expires_at - row.created_at <= 600000);
      if (row.decision === 'pending') check(row.decided_at === null && row.decided_account_id === null && row.decided_device_id === null && row.connection_id === null);
      else {
        check(['approved', 'denied'].includes(row.decision) && safe(row.decided_at) && row.decided_at >= row.created_at
          && row.decided_at < row.expires_at && id(row.decided_account_id) && id(row.decided_device_id));
        if (row.decision === 'denied') check(row.connection_id === null);
        else {
          check(id(row.connection_id));
          const c = db.prepare('SELECT * FROM cap_oauth_connections WHERE id=?').get(row.connection_id);
          const b = c && db.prepare("SELECT limit_amount FROM cap_budgets WHERE root_grant_id=? AND unit='invocations'").get(c.root_grant_id);
          check(c && b && c.issuer === row.issuer && c.static_client_id === row.static_client_id && c.resource === row.resource
            && c.consent_digest === row.request_digest && c.account_id === row.decided_account_id && c.creator_device_id === row.decided_device_id
            && c.created_at === row.decided_at && c.expires_at - c.created_at === row.duration_ms && b.limit_amount === row.budget_limit);
        }
      }
    }
    for (const row of db.prepare('SELECT * FROM cap_oauth_artifacts').iterate()) artifactCheck(db, row);
    for (const link of db.prepare('SELECT * FROM cap_oauth_credentials').iterate()) {
      credentialCheck(db, link, db.prepare('SELECT * FROM cap_oauth_connections WHERE id=?').get(link.connection_id),
        db.prepare('SELECT * FROM cap_credentials WHERE id=?').get(link.credential_id));
    }
    // A linked client cannot carry a second, unlinked legacy credential, even
    // when the AS is absent. All three authority pins participate in this test.
    check(!db.prepare(`SELECT 1 FROM cap_credentials k
      JOIN cap_oauth_connections c ON c.client_id=k.client_id OR c.principal_id=k.principal_id OR c.root_grant_id=k.grant_id
      LEFT JOIN cap_oauth_credentials l ON l.credential_id=k.id AND l.connection_id=c.id
      WHERE l.credential_id IS NULL LIMIT 1`).get());
  } catch (error) { if (error?.code === 'capabilities_storage_corrupt') throw error; fail(); }
}

/** Always-on access policy, independent of OAuth issuance/configuration. */
export function createOAuthBaselineGuard({ db }) {
  function available() {
    const version = db.prepare('PRAGMA user_version').get().user_version;
    if ([1, 2].includes(version)) return false;
    if (version !== 3) throw new AccessError('schema_version_unsupported');
    return true;
  }
  function managed({ clientId, principalId, grantId }) {
    if (!available()) return null;
    const rows = db.prepare(`SELECT * FROM cap_oauth_connections
      WHERE client_id=? OR principal_id=? OR root_grant_id=?
        OR root_grant_id=(SELECT root_id FROM cap_grants WHERE id=?) LIMIT 2`)
      .all(clientId ?? null, principalId ?? null, grantId ?? null, grantId ?? null);
    if (rows.length > 1) throw new AccessError('authorization_required');
    return rows[0] ?? null;
  }
  function validateCredential(row, time) {
    const connection = managed({ clientId: row.client_id, principalId: row.principal_id, grantId: row.grant_id });
    if (!connection) {
      if (available() && db.prepare('SELECT 1 FROM cap_oauth_credentials WHERE credential_id=?').get(row.id)) throw new AccessError('authorization_required');
      return null;
    }
    try {
      connectionCheck(db, connection);
      credentialCheck(db, db.prepare('SELECT * FROM cap_oauth_credentials WHERE credential_id=?').get(row.id), connection, row);
      check(connection.state === 'active' && connection.expires_at > time);
    } catch { throw new AccessError('authorization_required'); }
    return connection;
  }
  function assertUnmanaged(reference) {
    if (managed(reference)) throw new AccessError('oauth_managed_authority');
  }
  function revokeConnection(connection, time) {
    // The caller holds the ordinary signed Access transaction. Safety revoke
    // never depends on the old token being live or on decrypting AS artifacts.
    check(db.isTransaction && safe(time) && time >= connection.created_at);
    db.prepare(`UPDATE cap_credentials SET revoked_at=COALESCE(revoked_at,?)
      WHERE id IN (SELECT credential_id FROM cap_oauth_credentials WHERE connection_id=?)`).run(time, connection.id);
    const grant = db.prepare('SELECT revoked_at,policy_epoch FROM cap_grants WHERE id=?').get(connection.root_grant_id);
    check(grant && safe(grant.policy_epoch) && grant.policy_epoch > 0);
    if (grant.revoked_at === null) {
      check(grant.policy_epoch < Number.MAX_SAFE_INTEGER);
      db.prepare('UPDATE cap_grants SET revoked_at=?,policy_epoch=policy_epoch+1 WHERE id=?').run(time, connection.root_grant_id);
    }
    if (connection.state === 'active') db.prepare("UPDATE cap_oauth_connections SET state='revoked',revoked_at=? WHERE id=?").run(time, connection.id);
    return true;
  }
  function revokeForCredential(row, time) {
    const connection = managed({ clientId: row.client_id, principalId: row.principal_id, grantId: row.grant_id });
    return connection ? revokeConnection(connection, time) : false;
  }
  function legacyCredentialCount(accountId) {
    if (!available()) return db.prepare('SELECT count(*) AS n FROM cap_credentials WHERE account_id=?').get(accountId).n;
    return db.prepare(`SELECT count(*) AS n FROM cap_credentials k WHERE k.account_id=? AND NOT EXISTS(
      SELECT 1 FROM cap_oauth_credentials l JOIN cap_oauth_connections c ON c.id=l.connection_id
      WHERE l.credential_id=k.id AND l.token_digest=k.digest AND l.created_at=k.created_at AND l.expires_at=k.expires_at
        AND c.account_id=k.account_id AND c.client_id=k.client_id AND c.principal_id=k.principal_id
        AND c.root_grant_id=k.grant_id AND c.resource=k.audience)`).get(accountId).n;
  }
  return Object.freeze({ managed, validateCredential, assertUnmanaged, revokeForCredential, legacyCredentialCount,
    validateConnection(connection) { return connectionCheck(db, connection); }, revokeConnection });
}
