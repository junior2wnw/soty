import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { canonicalOAuthJson } from '../capabilities/server/oauth-profile.mjs';
import { HumanIdentityError, HUMAN_IDENTITY_PROFILE, requireHuman as require, closed, data, digest, id, uid, nonce, synchronous } from './profile.mjs';
import { initializeHumanIdentitySchema } from './schema.mjs';

export const HUMAN_IDENTITY_OPERATIONS = Object.freeze(['identity.human.approve']);
const MODELS = new Set(['Session', 'Interaction', 'Grant', 'AuthorizationCode', 'AccessToken']);
const TOKEN_MODELS = new Set(['AuthorizationCode', 'AccessToken']);
const LIMITS = Object.freeze({ interactions: 4096, decisions: 8192, artifacts: 8192, grantBindings: 4096 });
export const HUMAN_IDENTITY_RUNTIME = Object.freeze({ maxArtifactSeconds: 3600, decidedRetentionSeconds: 3660,
  pendingPerBrowser: 8, pendingPerClient: 64, decisionsPerInteraction: 16, compactionBatch: 128 });
const stringify = value => canonicalOAuthJson(value);
const actorOf = value => { const actor = data({ accountId: value?.accountId, deviceId: value?.deviceId }); id(actor.accountId); id(actor.deviceId); return Object.freeze(actor); };
const hashId = (model, value) => digest(model + '\0' + value);

/** Private SDK/store ports are not incoming operations. Connect signs approvals and fences later token use. */
export function createHumanIdentityService({ databasePath, profile, actorActive, withAuthorityFence, readProfile = () => ({}), maxDatabaseBytes = 64 * 1024 * 1024,
  now = Date.now } = {}) {
  require(profile?.enabled === true, 'human_identity_disabled', 503);
  for (const fn of [actorActive, withAuthorityFence, readProfile]) require(typeof fn === 'function' && fn.constructor?.name !== 'AsyncFunction', 'human_identity_authority_required', 503);
  require(typeof databasePath === 'string' && databasePath.length > 0, 'human_identity_database_required', 503);
  require(Number.isSafeInteger(maxDatabaseBytes) && maxDatabaseBytes >= 65536 && maxDatabaseBytes <= 64 * 1024 * 1024,
    'human_identity_database_quota_invalid', 503);
  require(typeof now === 'function' && now.constructor?.name !== 'AsyncFunction', 'human_identity_clock_invalid', 503);
  const epoch = () => { const value = synchronous(now()); require(Number.isSafeInteger(value) && value > 0, 'human_identity_clock_invalid', 503); return Math.floor(value / 1000); };
  const encryption = profile.encryptionKey(), key = Buffer.from(encryption.key), keyId = encryption.keyId;
  encryption.key.fill(0);
  mkdirSync(dirname(databasePath), { recursive: true }); const db = new DatabaseSync(databasePath); let stopped = false, gcEpoch = 0;
  // Guarded deletion is available only inside this private bounded maintenance transaction.
  db.function('human_identity_gc_epoch', () => gcEpoch);
  try {
    db.exec('PRAGMA foreign_keys=ON;PRAGMA busy_timeout=100;PRAGMA synchronous=FULL;');
    initializeHumanIdentitySchema(db, profile);
    const pageSize = Number(db.prepare('PRAGMA page_size').get().page_size);
    require(Number.isSafeInteger(pageSize) && pageSize >= 512, 'human_identity_storage_corrupt', 503);
    // Operational allocation cap; overquota persisted data is never shrunk, evicted or repaired.
    db.exec(`PRAGMA max_page_count=${Math.floor(maxDatabaseBytes / pageSize)}`); db.exec('PRAGMA journal_mode=WAL;');
    transaction(configureClients);
  } catch (error) { db.close(); key.fill(0); throw error; }
  function transaction(callback) {
    require(!stopped, 'human_identity_closed', 503); require(!db.isTransaction, 'human_identity_nested_transaction', 500);
    try { db.exec('BEGIN IMMEDIATE'); const result = synchronous(callback()); db.exec('COMMIT'); return result; }
    catch (error) {
      if (db.isTransaction) db.exec('ROLLBACK');
      if (Number.isInteger(error?.errcode) && [5, 6].includes(error.errcode & 255)) throw new HumanIdentityError('human_identity_storage_busy', 503);
      if (Number.isInteger(error?.errcode) && (error.errcode & 255) === 13) throw new HumanIdentityError('human_identity_storage_full', 503);
      throw error;
    }
  }
  function authenticate(actor) { require(synchronous(actorActive(actor)) === true, 'human_identity_actor_revoked', 403); }
  function fenced(actorInput, action) {
    const actor = actorOf(actorInput); let active = true, entered = false, outcome;
    try {
      const result = withAuthorityFence(() => {
        require(active && !entered, 'human_identity_authority_fence_invalid', 500); entered = true;
        authenticate(actor); outcome = transaction(() => { authenticate(actor); const result = synchronous(action(actor)); authenticate(actor); return result; }); return outcome;
      });
      synchronous(result); require(entered, 'human_identity_authority_fence_invalid', 500); return outcome;
    } finally { active = false; }
  }
  function aad(model, hash) { return Buffer.from(stringify([profile.issuer, HUMAN_IDENTITY_PROFILE, model, hash, keyId])); }
  function encrypt(model, hash, payload) {
    const text = stringify(data(payload)), iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, iv);
    cipher.setAAD(aad(model, hash)); const ciphertext = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
    return { bytes: Buffer.concat([iv, cipher.getAuthTag(), ciphertext]), digest: digest(text) };
  }
  function decrypt(model, hash, bytes, storedKeyId, expectedDigest) {
    require(storedKeyId === keyId, 'human_identity_storage_key_unavailable', 503);
    try {
      require(bytes instanceof Uint8Array && bytes.byteLength >= 30 && bytes.byteLength <= 16412, 'human_identity_storage_corrupt', 503);
      const value = Buffer.from(bytes), decipher = createDecipheriv('aes-256-gcm', key, value.subarray(0, 12));
      decipher.setAAD(aad(model, hash)); decipher.setAuthTag(value.subarray(12, 28));
      const text = Buffer.concat([decipher.update(value.subarray(28)), decipher.final()]).toString('utf8');
      require(expectedDigest === undefined || digest(text) === expectedDigest, 'human_identity_storage_corrupt', 503); return data(JSON.parse(text));
    } catch (error) { if (error instanceof HumanIdentityError) throw error; throw new HumanIdentityError('human_identity_storage_corrupt', 503); }
  }
  function writeCapacity() {
    const pages = Number(db.prepare('PRAGMA page_count').get().page_count), size = Number(db.prepare('PRAGMA page_size').get().page_size);
    require(pages * size <= maxDatabaseBytes, 'human_identity_storage_full', 503);
  }
  function capacity(table, limit) {
    compactExpired();
    writeCapacity(); require(db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n < limit, 'human_identity_capacity', 429);
  }
  function compactExpired() {
    require(db.isTransaction, 'human_identity_nested_transaction', 500);
    gcEpoch = epoch();
    const eligible = `i.expires_at<=? AND (i.decision='pending' OR i.expires_at<=?-3660)
      AND NOT EXISTS(SELECT 1 FROM human_identity_artifacts a WHERE a.expires_at>?
       AND (a.model='Interaction' AND a.id_hash=i.uid_hash OR a.grant_hash IN
        (SELECT grant_hash FROM human_identity_grant_bindings WHERE interaction_hash=i.uid_hash)))`;
    const batch = HUMAN_IDENTITY_RUNTIME.compactionBatch;
    try {
      const artifacts = db.prepare('DELETE FROM human_identity_artifacts WHERE rowid IN (SELECT rowid FROM human_identity_artifacts WHERE expires_at<=? ORDER BY expires_at LIMIT ?)').run(gcEpoch, batch).changes;
      const decisions = db.prepare(`DELETE FROM human_identity_decisions WHERE rowid IN (SELECT d.rowid FROM human_identity_decisions d
        JOIN human_identity_interactions i ON i.uid_hash=d.uid_hash WHERE ${eligible} LIMIT ?)`).run(gcEpoch, gcEpoch, gcEpoch, batch).changes;
      const bindings = db.prepare(`DELETE FROM human_identity_grant_bindings WHERE rowid IN (SELECT b.rowid FROM human_identity_grant_bindings b
        JOIN human_identity_interactions i ON i.uid_hash=b.interaction_hash WHERE ${eligible} LIMIT ?)`).run(gcEpoch, gcEpoch, gcEpoch, batch).changes;
      const interactions = db.prepare(`DELETE FROM human_identity_interactions WHERE rowid IN (SELECT i.rowid FROM human_identity_interactions i
        WHERE ${eligible} AND NOT EXISTS(SELECT 1 FROM human_identity_decisions d WHERE d.uid_hash=i.uid_hash)
         AND NOT EXISTS(SELECT 1 FROM human_identity_grant_bindings b WHERE b.interaction_hash=i.uid_hash) LIMIT ?)`).run(gcEpoch, gcEpoch, gcEpoch, batch).changes;
      return Object.freeze({ artifacts, decisions, bindings, interactions });
    } finally { gcEpoch = 0; }
  }
  function pendingCapacity(clientId, browserHash) {
    compactExpired();
    require(db.prepare("SELECT count(*) AS n FROM human_identity_artifacts WHERE model='Interaction' AND client_id=? AND expires_at>?").get(clientId, epoch()).n
      < HUMAN_IDENTITY_RUNTIME.pendingPerClient, 'human_identity_pending_client_capacity', 429);
    require(db.prepare("SELECT count(*) AS n FROM human_identity_artifacts WHERE model='Interaction' AND browser_hash=? AND expires_at>?").get(browserHash, epoch()).n
      < HUMAN_IDENTITY_RUNTIME.pendingPerBrowser, 'human_identity_pending_browser_capacity', 429);
  }
  function configureClients() {
    const current = db.prepare('SELECT * FROM human_identity_client_heads').all(), allowed = new Map(profile.publicClients.map(client => [client.id, client]));
    for (const prior of current) if (!allowed.has(prior.client_id) && prior.active === 1) {
      db.prepare('UPDATE human_identity_client_heads SET active=0,generation=generation+1,updated_at=? WHERE client_id=?').run(epoch(), prior.client_id);
    }
    for (const client of profile.publicClients) {
      const pin = db.prepare('SELECT * FROM human_identity_client_versions WHERE client_id=? AND version=?').get(client.id, client.version);
      require(!pin || pin.profile_digest === client.profileDigest, 'human_identity_client_version_conflict', 409);
      if (!pin) {
        capacity('human_identity_client_versions', 4096);
        db.prepare('INSERT INTO human_identity_client_versions VALUES(?,?,?,?)').run(client.id, client.version, client.profileDigest, epoch());
      }
      const prior = current.find(row => row.client_id === client.id);
      if (!prior) {
        capacity('human_identity_client_heads', 4096);
        db.prepare('INSERT INTO human_identity_client_heads VALUES(?,?,?,1,1,?)').run(client.id, client.version, client.profileDigest, epoch());
      } else {
        require(client.version >= prior.version, 'human_identity_client_version_rollback', 409);
        if (!prior.active || prior.version !== client.version || prior.profile_digest !== client.profileDigest) {
          db.prepare('UPDATE human_identity_client_heads SET version=?,profile_digest=?,generation=generation+1,active=1,updated_at=? WHERE client_id=?')
            .run(client.version, client.profileDigest, epoch(), client.id);
        }
      }
    }
  }
  function clientAuthority(clientId) {
    const client = profile.client(clientId), head = db.prepare('SELECT * FROM human_identity_client_heads WHERE client_id=?').get(clientId);
    return client && head?.active === 1 && head.version === client.version && head.profile_digest === client.profileDigest
      ? { client, generation: head.generation } : null;
  }
  function paramsFor(input) {
    const value = data(input), client = profile.client(value.client_id);
    require(client && profile.isRegisteredRedirect(value.client_id, value.redirect_uri) && value.response_type === 'code'
      && ['openid', 'openid profile'].includes(value.scope) && value.code_challenge_method === 'S256', 'human_identity_client_mismatch', 403);
    nonce(value.code_challenge);
    for (const name of ['state', 'nonce']) require(typeof value[name] === 'string' && /^[A-Za-z0-9_-]{16,128}$/u.test(value[name]), 'human_identity_context_invalid');
    return Object.fromEntries(['client_id', 'redirect_uri', 'response_type', 'scope', 'state', 'nonce', 'code_challenge_method', 'code_challenge']
      .map(name => [name, value[name]]));
  }
  const interaction = value => db.prepare('SELECT * FROM human_identity_interactions WHERE uid_hash=?').get(hashId('Interaction', value));
  function publicContext(row, interactionId, browserNonce) {
    const stored = decrypt('LoginIntent', row.uid_hash, row.params_cipher, row.key_id), authority = clientAuthority(row.client_id), client = authority?.client;
    require(client && row.profile_digest === client.profileDigest && row.client_generation === authority.generation, 'human_identity_profile_changed', 403);
    return { schema: 'soty.human-login-context.v1', interactionId, browserNonce, csrf: stored.csrf,
      client: { id: client.id, label: client.label }, scopes: stored.parameters.scope.split(' '), expiresAt: row.expires_at * 1000, decision: row.decision };
  }
  function browserMatch(row, browserNonce, csrf) {
    require(row && row.expires_at > epoch(), 'human_identity_interaction_expired', 410);
    require(row.browser_hash === digest(nonce(browserNonce)) && row.csrf_hash === digest(nonce(csrf)), 'human_identity_browser_mismatch', 403);
    const authority = clientAuthority(row.client_id);
    require(authority && row.profile_digest === authority.client.profileDigest && row.client_generation === authority.generation, 'human_identity_profile_changed', 403);
  }
  function bindingForHash(hash) { return db.prepare('SELECT * FROM human_identity_grant_bindings WHERE grant_hash=?').get(hash); }
  function activeBinding(binding) {
    const authority = binding && clientAuthority(binding.client_id);
    return Boolean(binding && binding.revoked_at === null && authority && binding.profile_digest === authority.client.profileDigest && binding.client_generation === authority.generation
      && synchronous(actorActive({ accountId: binding.account_id, deviceId: binding.device_id })) === true);
  }
  function bindingFromPayload(model, payload, approvedBinding, clientId) {
    if (model === 'Grant') {
      require(approvedBinding && payload.accountId === approvedBinding.accountId && payload.clientId === approvedBinding.clientId,
        'human_identity_grant_unreviewed', 403); return approvedBinding;
    }
    if (TOKEN_MODELS.has(model)) {
      const binding = bindingForHash(hashId('Grant', payload.grantId));
      require(activeBinding(binding) && payload.accountId === binding.account_id && payload.clientId === binding.client_id,
        'human_identity_actor_revoked', 403); return { accountId: binding.account_id, deviceId: binding.device_id, clientId: binding.client_id };
    }
    if (model === 'Session' && payload.accountId) {
      const grants = payload.authorizations || {}, selected = clientId && grants[clientId]?.grantId;
      const candidates = [...new Set([selected, ...Object.values(grants).map(value => value?.grantId)].filter(Boolean))];
      const binding = candidates.map(value => bindingForHash(hashId('Grant', value))).find(value => activeBinding(value) && value.account_id === payload.accountId);
      require(binding, 'human_identity_session_unreviewed', 403);
      return { accountId: binding.account_id, deviceId: binding.device_id, clientId: binding.client_id, grantHash: binding.grant_hash };
    }
    return null;
  }
  function findArtifact(model, rawId) {
    require(MODELS.has(model) && typeof rawId === 'string' && rawId.length <= 256, 'human_identity_artifact_invalid');
    const row = db.prepare('SELECT * FROM human_identity_artifacts WHERE model=? AND id_hash=?').get(model, hashId(model, rawId));
    if (!row || row.expires_at <= epoch()) return undefined;
    if (row.account_id && synchronous(actorActive({ accountId: row.account_id, deviceId: row.device_id })) !== true) return undefined;
    if (row.grant_hash && model !== 'Session' && !activeBinding(bindingForHash(row.grant_hash))) return undefined;
    const payload = decrypt(model, row.id_hash, row.payload_cipher, row.key_id, row.payload_digest);
    return row.consumed_at === null ? payload : { ...payload, consumed: row.consumed_at };
  }
  return Object.freeze({
    operations: new Set(HUMAN_IDENTITY_OPERATIONS), schemaVersion: 1,
    prepareInteraction({ interactionId, browserNonce, parameters }) {
      uid(interactionId); nonce(browserNonce); const captured = paramsFor(parameters), intent = digest(captured);
      return transaction(() => {
        const sdk = db.prepare("SELECT browser_hash,client_id,expires_at FROM human_identity_artifacts WHERE model='Interaction' AND id_hash=?").get(hashId('Interaction', interactionId));
        require(sdk && sdk.expires_at > epoch(), 'human_identity_interaction_expired', 410);
        require(sdk.browser_hash === digest(browserNonce) && sdk.client_id === captured.client_id, 'human_identity_browser_mismatch', 403);
        let row = interaction(interactionId);
        if (row) {
          require(row.params_digest === intent && row.browser_hash === digest(browserNonce), 'human_identity_browser_mismatch', 403);
          require(row.expires_at > epoch(), 'human_identity_interaction_expired', 410);
        } else {
          capacity('human_identity_interactions', LIMITS.interactions);
          const authority = clientAuthority(captured.client_id); require(authority, 'human_identity_profile_changed', 403);
          const csrf = randomBytes(32).toString('base64url'), hash = hashId('Interaction', interactionId), encrypted = encrypt('LoginIntent', hash, { parameters: captured, csrf });
          db.prepare(`INSERT INTO human_identity_interactions VALUES(?,?,?,?,?,?,?,?,?,?,'pending',NULL,NULL,NULL)`)
            .run(hash, digest(browserNonce), digest(csrf), captured.client_id, authority.client.profileDigest, authority.generation, intent, encrypted.bytes, keyId, Math.min(sdk.expires_at, epoch() + 300));
          row = interaction(interactionId);
        }
        return publicContext(row, interactionId, browserNonce);
      });
    },
    compactExpiredRuntime() { return transaction(compactExpired); },
    execute({ op, actor: actorInput, args: input }) {
      require(op === 'identity.human.approve', 'unsupported_operation');
      const args = data(input); closed(args, ['expectedAccountId', 'interactionId', 'browserNonce', 'csrf', 'requestId', 'decision']);
      const actor = actorOf(actorInput); require(args.expectedAccountId === actor.accountId, 'human_identity_account_mismatch', 403);
      uid(args.interactionId); id(args.requestId); require(['approve', 'deny'].includes(args.decision), 'human_identity_decision_invalid');
      // Connect already holds its proof transaction; never nest another Connect fence here.
      return transaction(() => {
        authenticate(actor); const row = interaction(args.interactionId); browserMatch(row, args.browserNonce, args.csrf);
        const intent = digest({ interaction: row.uid_hash, browser: row.browser_hash, csrf: row.csrf_hash, params: row.params_digest,
          accountId: actor.accountId, deviceId: actor.deviceId, decision: args.decision });
        const prior = db.prepare('SELECT * FROM human_identity_decisions WHERE account_id=? AND request_id=?').get(actor.accountId, args.requestId);
        if (prior) { require(prior.intent_hash === intent, 'human_identity_intent_conflict', 409); return JSON.parse(prior.result_json); }
        const decision = args.decision === 'approve' ? 'approved' : 'denied';
        require(row.decision === 'pending' || row.decision === decision && row.account_id === actor.accountId && row.device_id === actor.deviceId,
          'human_identity_decision_conflict', 409);
        require(db.prepare('SELECT count(*) AS n FROM human_identity_decisions WHERE uid_hash=?').get(row.uid_hash).n < HUMAN_IDENTITY_RUNTIME.decisionsPerInteraction,
          'human_identity_decision_capacity', 429); capacity('human_identity_decisions', LIMITS.decisions);
        if (row.decision === 'pending') db.prepare('UPDATE human_identity_interactions SET decision=?,account_id=?,device_id=?,approved_at=? WHERE uid_hash=? AND decision=\'pending\'')
          .run(decision, actor.accountId, actor.deviceId, epoch(), row.uid_hash);
        const result = { schema: 'soty.human-login-decision.v1', interactionId: args.interactionId, requestId: args.requestId, decision };
        db.prepare('INSERT INTO human_identity_decisions VALUES(?,?,?,?,?,?)').run(actor.accountId, args.requestId, intent, row.uid_hash, stringify(result), epoch());
        authenticate(actor); return result;
      });
    },
    readApprovedInteraction({ interactionId, browserNonce, csrf }) {
      uid(interactionId); const row = interaction(interactionId); browserMatch(row, browserNonce, csrf);
      require(row.decision !== 'pending', 'human_identity_approval_required', 403);
      return fenced({ accountId: row.account_id, deviceId: row.device_id }, () => ({ accountId: row.account_id, deviceId: row.device_id,
        clientId: row.client_id, interactionHash: row.uid_hash, profileDigest: row.profile_digest, clientGeneration: row.client_generation, decision: row.decision,
        scopes: decrypt('LoginIntent', row.uid_hash, row.params_cipher, row.key_id).parameters.scope }));
    },
    accountClaims({ accountId, grantId, sessionUid }) {
      let binding = grantId ? bindingForHash(hashId('Grant', grantId)) : null;
      if (grantId && !activeBinding(binding)) return null;
      if (!grantId && sessionUid) {
        const session = db.prepare("SELECT account_id,device_id,client_id FROM human_identity_artifacts WHERE model='Session' AND uid_hash=? AND expires_at>? LIMIT 1")
          .get(digest(sessionUid), epoch());
        if (session) binding = { ...session, sessionOnly: true };
      }
      if (!binding || binding.account_id !== accountId || !binding.sessionOnly && !activeBinding(binding)) return null;
      return fenced({ accountId: binding.account_id, deviceId: binding.device_id }, () => {
        if (grantId) require(activeBinding(bindingForHash(hashId('Grant', grantId))), 'human_identity_actor_revoked', 403);
        const current = data(synchronous(readProfile({ accountId }))); closed(current, [], ['name', 'preferred_username']);
        for (const value of Object.values(current)) require(typeof value === 'string' && value.length <= 160, 'human_identity_profile_invalid');
        return { sub: accountId, ...current };
      });
    },
    sdk: Object.freeze({
      upsert({ model, id: rawId, payload: input, expiresIn, approvedBinding, clientId, browserNonce }) {
        require(MODELS.has(model) && typeof rawId === 'string' && rawId.length >= 16 && rawId.length <= 256
          && Number.isSafeInteger(expiresIn) && expiresIn > 0 && expiresIn <= 3600, 'human_identity_artifact_invalid');
        const payload = data(input), binding = bindingFromPayload(model, payload, approvedBinding, clientId);
        const apply = () => {
          if (TOKEN_MODELS.has(model)) require(activeBinding(bindingForHash(hashId('Grant', payload.grantId))), 'human_identity_actor_revoked', 403);
          const hash = hashId(model, rawId), prior = db.prepare('SELECT * FROM human_identity_artifacts WHERE model=? AND id_hash=?').get(model, hash);
          require(!prior || prior.expires_at > epoch(), 'human_identity_interaction_expired', 410);
          let browserHash = prior?.browser_hash || null, artifactClientId = binding?.clientId || null;
          if (model === 'Interaction') {
            artifactClientId = payload.params?.client_id;
            require(clientAuthority(artifactClientId), 'human_identity_client_mismatch', 403);
            if (!prior) { browserHash = digest(nonce(browserNonce)); pendingCapacity(artifactClientId, browserHash); }
            else require(prior.client_id === artifactClientId && (browserNonce === undefined || prior.browser_hash === digest(nonce(browserNonce))), 'human_identity_browser_mismatch', 403);
          }
          if (!prior) capacity('human_identity_artifacts', LIMITS.artifacts);
          if (prior?.account_id) require(binding && prior.account_id === binding.accountId && prior.device_id === binding.deviceId
            && (model === 'Session' || prior.client_id === binding.clientId),
            'human_identity_artifact_binding_conflict', 409);
          if (TOKEN_MODELS.has(model)) require(!prior || prior.payload_digest === digest(stringify(payload)), 'human_identity_artifact_binding_conflict', 409);
          if (model === 'Grant') {
            const grantHash = hashId('Grant', rawId), existing = bindingForHash(grantHash);
            if (!existing) {
              capacity('human_identity_grant_bindings', LIMITS.grantBindings);
              const approval = db.prepare('SELECT * FROM human_identity_interactions WHERE uid_hash=?').get(binding.interactionHash);
              const authority = clientAuthority(binding.clientId);
              require(approval && approval.expires_at > epoch() && approval.decision === 'approved' && approval.account_id === binding.accountId && approval.device_id === binding.deviceId
                && approval.client_id === binding.clientId && authority && approval.profile_digest === authority.client.profileDigest
                && approval.client_generation === authority.generation && binding.clientGeneration === authority.generation, 'human_identity_grant_unreviewed', 403);
              const occupied = db.prepare('SELECT grant_hash FROM human_identity_grant_bindings WHERE interaction_hash=?').get(binding.interactionHash);
              require(!occupied, 'human_identity_grant_conflict', 409);
              db.prepare('INSERT INTO human_identity_grant_bindings VALUES(?,?,?,?,?,?,?,?,NULL)').run(grantHash, binding.accountId, binding.deviceId,
                binding.clientId, binding.interactionHash, authority.client.profileDigest, authority.generation, epoch());
            } else require(prior && activeBinding(existing), 'human_identity_actor_revoked', 403);
          }
          const encrypted = encrypt(model, hash, payload), grantHash = model === 'Grant' ? hash : payload.grantId ? hashId('Grant', payload.grantId) : binding?.grantHash || null;
          db.prepare(`INSERT INTO human_identity_artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(model,id_hash) DO UPDATE SET payload_cipher=excluded.payload_cipher,payload_digest=excluded.payload_digest,
              expires_at=excluded.expires_at,uid_hash=excluded.uid_hash,account_id=excluded.account_id,
              device_id=excluded.device_id,client_id=excluded.client_id,grant_hash=excluded.grant_hash`)
            .run(model, hash, encrypted.bytes, encrypted.digest, keyId, binding?.accountId || null, binding?.deviceId || null,
              artifactClientId, grantHash, payload.uid ? digest(payload.uid) : null, browserHash, epoch() + expiresIn, prior?.consumed_at ?? null, epoch());
        };
        return binding ? fenced(binding, apply) : transaction(apply);
      },
      find(model, rawId) { require(!stopped, 'human_identity_closed', 503); return findArtifact(model, rawId); },
      grantForInteraction(interactionHash) {
        const binding = db.prepare('SELECT * FROM human_identity_grant_bindings WHERE interaction_hash=?').get(interactionHash);
        if (!activeBinding(binding)) return undefined;
        const row = db.prepare("SELECT * FROM human_identity_artifacts WHERE model='Grant' AND id_hash=? AND expires_at>?").get(binding.grant_hash, epoch());
        if (!row) return undefined;
        const payload = decrypt('Grant', row.id_hash, row.payload_cipher, row.key_id, row.payload_digest);
        return { grantId: payload.jti, accountId: binding.account_id, deviceId: binding.device_id, clientId: binding.client_id };
      },
      findByUid(rawUid) {
        require(!stopped, 'human_identity_closed', 503);
        const row = db.prepare("SELECT * FROM human_identity_artifacts WHERE model='Session' AND uid_hash=? AND expires_at>? LIMIT 1").get(digest(rawUid), epoch());
        if (!row || row.account_id && synchronous(actorActive({ accountId: row.account_id, deviceId: row.device_id })) !== true) return undefined;
        return decrypt('Session', row.id_hash, row.payload_cipher, row.key_id, row.payload_digest);
      },
      consume(model, rawId) {
        const row = db.prepare('SELECT * FROM human_identity_artifacts WHERE model=? AND id_hash=?').get(model, hashId(model, rawId));
        require(row?.account_id && row.expires_at > epoch() && row.consumed_at === null, 'human_identity_grant_invalid', 403);
        return fenced({ accountId: row.account_id, deviceId: row.device_id }, () => {
          const changed = db.prepare('UPDATE human_identity_artifacts SET consumed_at=? WHERE model=? AND id_hash=? AND consumed_at IS NULL')
            .run(epoch(), model, row.id_hash); require(changed.changes === 1, 'human_identity_grant_invalid', 403);
        });
      },
      destroy(model, rawId) { return transaction(() => db.prepare('DELETE FROM human_identity_artifacts WHERE model=? AND id_hash=?').run(model, hashId(model, rawId))); },
      revokeByGrantId(rawGrantId) {
        return transaction(() => {
          const hash = hashId('Grant', rawGrantId);
          db.prepare('UPDATE human_identity_grant_bindings SET revoked_at=? WHERE grant_hash=? AND revoked_at IS NULL').run(epoch(), hash);
          db.prepare('DELETE FROM human_identity_artifacts WHERE grant_hash=?').run(hash);
        });
      },
    }),
    close() { if (stopped) return false; stopped = true; db.close(); key.fill(0); return true; },
  });
}
