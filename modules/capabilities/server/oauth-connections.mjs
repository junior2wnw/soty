import { createHash } from 'node:crypto';
import { AccessError, canonicalHash, newId } from './validation.mjs';
import { createOAuthArtifactStore } from './oauth-artifacts.mjs';
import { createOAuthArtifactCodec } from './oauth-crypto.mjs';
import { createOAuthTokenStore } from './oauth-tokens.mjs';
import { OAUTH_TOKEN_MODELS } from './oauth-token-profile.mjs';
import { OAUTH_PROFILE, OAUTH_SCOPE, canonicalOAuthJson, oauthCheck, oauthData, oauthId,
  oauthProviderId, oauthString, oauthSynchronous, oauthTime, snapshotOAuthJson, snapshotOAuthGrant } from './oauth-profile.mjs';

export const OAUTH_OPERATIONS = Object.freeze(['oauth.connections.approve', 'oauth.connections.deny',
  'oauth.connections.list', 'oauth.connections.revoke']);
const sha = value => createHash('sha256').update(value).digest('hex');
const denied = error => ['access_denied', 'authorization_required'].includes(error?.code);
const auxiliary = model => model === 'Session' || model === 'Interaction';
const integer = (value, min, max, code = 'oauth_invalid_artifact') => {
  oauthCheck(Number.isSafeInteger(value) && value >= min && value <= max, code); return value;
};

/** Fixed owner/Grant/token coordinator. Bearers resolve only existing linked
 * credentials; no public arbitrary issuer or authority factory is exposed. */
export function createOAuthConnections({ db, projectId, registryId, schemaVersion, clock, transaction, ensureOpen,
  configuration, access, authority, nativeBindingReady = () => false, monotonic = () => performance.now() }) {
  const { issuer, withAuthorityFence } = configuration;
  let closed = false, running = false, fencing = false, auxCore;
  const contexts = new WeakMap(), activeContexts = new Set();
  const get = (sql, ...args) => db.prepare(sql).get(...args);
  const artifact = (model, hash) => get('SELECT * FROM cap_oauth_artifacts WHERE model=? AND id_hash=?', model, hash);
  const connection = value => get('SELECT * FROM cap_oauth_connections WHERE id=?', value);
  const codec = schemaVersion === 3 ? createOAuthArtifactCodec({ registryId, issuer,
    artifactKey: configuration.artifactKey, artifactKeyId: configuration.artifactKeyId }) : null;
  const support = createOAuthArtifactStore({ db, projectId, registryId, schemaVersion, clock, transaction, ensureOpen, configuration,
    captureCore(value) { auxCore = value; } });
  function identity() {
    ensureOpen(); oauthCheck(!closed, 'service_closed');
    oauthCheck(schemaVersion === 3 && get('PRAGMA user_version').user_version === 3, 'oauth_unavailable');
    const rows = db.prepare('SELECT key,value FROM cap_metadata ORDER BY key').all();
    const values = Object.fromEntries(rows.map(row => [row.key, row.value]));
    oauthCheck(rows.length === 3 && values.lineage === 'soty.capabilities.sqlite.v3'
      && values.project_id === projectId && values.registry_id === registryId, 'capabilities_storage_corrupt');
  }
  function atomic(action) {
    ensureOpen(); oauthCheck(!closed, 'service_closed'); oauthCheck(!running, 'nested_transaction');
    running = true; let accepting = true, called = false;
    try {
      const result = transaction(() => {
        oauthCheck(accepting && !called && running, 'oauth_context_invalid'); called = true;
        identity(); const result = oauthSynchronous(action(oauthTime(clock()))); identity(); return result;
      }, { busyMs: 100 });
      oauthSynchronous(result); oauthCheck(called, 'oauth_context_invalid'); return result;
    } catch (error) {
      if (error instanceof AccessError) throw error;
      if ([5, 6].includes(error?.errcode & 255)) throw new AccessError('oauth_storage_busy');
      throw new AccessError('capabilities_storage_corrupt');
    } finally { accepting = false; running = false; }
  }
  function fenced(action) {
    ensureOpen(); oauthCheck(!closed && !fencing && !running, 'oauth_context_invalid');
    fencing = true; let accepting = true, called = false;
    try {
      const result = withAuthorityFence(() => {
        oauthCheck(accepting && !called && fencing, 'oauth_context_invalid'); called = true;
        return atomic(action);
      });
      oauthSynchronous(result); oauthCheck(called, 'oauth_context_invalid'); return result;
    } finally { accepting = false; fencing = false; }
  }
  function nonce(value) {
    oauthCheck(typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value), 'oauth_interaction_not_found');
    return sha(value);
  }
  function proposal(id, nonceHash, time, { terminalReplay = false } = {}) {
    const row = get('SELECT * FROM cap_oauth_interactions WHERE uid_hash=?', sha(oauthProviderId(id)));
    oauthCheck(row && row.issuer === issuer && row.browser_nonce_hash === nonceHash
      && (row.expires_at > time || (terminalReplay && row.decision !== 'pending')), 'oauth_interaction_not_found');
    return row;
  }
  function intentDigest(payload, durationMs, budgetLimit) {
    const p = payload.params;
    return canonicalHash(['soty.oauth-consent.v1', issuer, sha(payload.jti), p.client_id, p.redirect_uri,
      p.resource, p.scope, p.code_challenge, 'S256', p.state === undefined ? null : sha(p.state),
      durationMs, budgetLimit, payload.exp * 1000]);
  }
  function currentIntent(row, id, time) {
    const payload = auxCore.findInTransaction({ model: 'Interaction', id }, time);
    oauthCheck(payload && intentDigest(payload, row.duration_ms, row.budget_limit) === row.request_digest,
      'oauth_interaction_conflict');
    return payload;
  }
  function presentation(row, id) {
    return { interactionId: id, contextDigest: row.request_digest, clientProfile: row.static_client_id,
      resource: row.resource, scope: OAUTH_SCOPE, durationMs: row.duration_ms, budgetLimit: row.budget_limit,
      expiresAt: row.expires_at, decision: row.decision, checkedAt: oauthTime(clock()), decidedAccountId: row.decided_account_id };
  }
  function live(row, time) {
    oauthCheck(row && row.issuer === issuer, 'access_denied'); authority.live(row, time); return row;
  }
  function prepared(args) {
    oauthData(args, ['interactionId', 'browserNonce']);
    return { id: oauthProviderId(args.interactionId), nonceHash: nonce(args.browserNonce) };
  }
  function pruneContexts(time) {
    const tick = monotonic(); oauthCheck(typeof tick === 'number' && Number.isFinite(tick) && tick >= 0, 'clock_invalid');
    for (const state of activeContexts) if (!state.live || state.deadline <= tick || state.expiresAt <= time) {
      state.live = false; activeContexts.delete(state);
    }
    return tick;
  }
  function staging(token, time) {
    pruneContexts(time);
    const state = token && typeof token === 'object' ? contexts.get(token) : null;
    oauthCheck(state?.live && activeContexts.has(state), 'oauth_context_invalid');
    const row = get('SELECT * FROM cap_oauth_interactions WHERE uid_hash=?', state.interactionHash);
    oauthCheck(row?.decision === 'approved' && row.connection_id === state.connectionId
      && row.browser_nonce_hash === state.nonceHash && row.expires_at > time, 'oauth_context_invalid');
    return live(connection(state.connectionId), time);
  }
  function grantPayload(row, own, time, id) {
    oauthCheck(row.model === 'Grant' && row.id_hash === sha(id) && row.issuer === issuer
      && row.profile === OAUTH_PROFILE && row.provider_grant_id === id && row.connection_id === own.id
      && own.provider_grant_id === id && row.session_uid_hash === null && row.consumed_at === null
      && Number.isSafeInteger(row.created_at) && row.created_at >= own.created_at && row.created_at <= time
      && row.expires_at > row.created_at && row.expires_at <= own.expires_at
      && row.retain_until === own.expires_at, 'capabilities_storage_corrupt');
    try {
      const payload = codec.open({ model: 'Grant', idHash: row.id_hash, profile: row.profile, keyId: row.key_id,
        payloadCipher: row.payload_cipher, payloadDigest: row.payload_digest });
      const checked = snapshotOAuthGrant({ id, payload, connection: own, nowMs: time, allowExpired: true });
      oauthCheck(checked.expiresAt === row.expires_at, 'capabilities_storage_corrupt'); return checked;
    } catch (error) {
      if (error?.code === 'oauth_storage_key_unavailable') throw error;
      throw new AccessError('capabilities_storage_corrupt');
    }
  }
  function saveGrant(args) {
    oauthData(args, ['model', 'id', 'payload', 'expiresIn', 'request', 'stagedGrant']);
    oauthCheck(args.model === 'Grant' && args.request === undefined, 'oauth_context_invalid');
    const id = oauthProviderId(args.id), payload = snapshotOAuthJson(args.payload);
    const { expiresIn, stagedGrant } = args;
    return fenced(time => {
      oauthCheck(codec?.available(), 'oauth_storage_key_unavailable');
      const previous = artifact('Grant', sha(id));
      let own;
      if (previous) {
        own = live(connection(previous.connection_id), time);
        if (stagedGrant !== undefined) oauthCheck(staging(stagedGrant, time).id === own.id, 'oauth_context_invalid');
      } else own = staging(stagedGrant, time);
      oauthCheck(own.provider_grant_id === null || own.provider_grant_id === id, 'oauth_grant_conflict');
      const next = snapshotOAuthGrant({ id, payload, connection: own, nowMs: time, expiresIn });
      if (previous) {
        const current = grantPayload(previous, own, time, id);
        oauthCheck(current.json === next.json && current.expiresAt === next.expiresAt, 'oauth_invalid_artifact'); return;
      }
      oauthCheck(get('SELECT count(*) AS n FROM cap_oauth_artifacts WHERE connection_id=?', own.id).n < 1024
        && get('SELECT count(*) AS n FROM cap_oauth_artifacts').n < 65536, 'oauth_quota_exceeded');
      const encrypted = codec.seal({ model: 'Grant', idHash: sha(id), payload: next.payload });
      db.prepare(`INSERT INTO cap_oauth_artifacts(model,id_hash,issuer,profile,key_id,payload_cipher,payload_digest,
        connection_id,provider_grant_id,created_at,expires_at,retain_until)
        VALUES('Grant',?,?,?,?,?,?,?,?,?,?,?)`).run(sha(id), issuer, encrypted.profile, encrypted.keyId,
        encrypted.payloadCipher, encrypted.payloadDigest, own.id, id, time, next.expiresAt, own.expires_at);
      const changed = db.prepare('UPDATE cap_oauth_connections SET provider_grant_id=? WHERE id=? AND provider_grant_id IS NULL').run(id, own.id);
      oauthCheck(changed.changes === 1, 'oauth_grant_conflict');
      live(connection(own.id), oauthTime(clock()));
    });
  }
  function revoke(own, time) {
    authority.revoke(own, time);
    for (const state of activeContexts) if (state.connectionId === own.id) { state.live = false; activeContexts.delete(state); }
  }
  function sourceId(args) { oauthData(args, ['model', 'id']); oauthCheck(args.model === 'Grant', 'oauth_unavailable'); return oauthProviderId(args.id); }
  const tokens = createOAuthTokenStore({ db, configuration, clock, codec, fenced, live, revoke,
    boundGrant(own, time) {
      const row = artifact('Grant', sha(own.provider_grant_id));
      oauthCheck(row && row.expires_at > time, 'access_denied');
      return grantPayload(row, own, time, own.provider_grant_id).payload;
    } });
  const artifactStore = Object.freeze({
    upsert(args) {
      oauthData(args, ['model', 'id', 'payload', 'expiresIn', 'request', 'stagedGrant']);
      if (args.model === 'Grant') return saveGrant(args);
      if (OAUTH_TOKEN_MODELS.includes(args.model)) return tokens.upsert(args);
      if (!auxiliary(args.model)) throw new AccessError('oauth_unavailable');
      // Every successful interaction completion is tied to the signed decision.
      const payload = snapshotOAuthJson(args.payload);
      const input = { ...args, payload };
      if (args.model !== 'Interaction' || (!payload?.result?.login && !payload?.result?.consent)) return support.upsert(input);
      return fenced(time => {
        const row = get('SELECT * FROM cap_oauth_interactions WHERE uid_hash=?', sha(oauthProviderId(input.id)));
        oauthCheck(row?.decision === 'approved' && row.expires_at > time, 'oauth_context_invalid');
        const own = live(connection(row.connection_id), time);
        oauthCheck(payload.result.login?.accountId === own.account_id && own.provider_grant_id !== null
          && payload.result.consent?.grantId === own.provider_grant_id
          && intentDigest(payload, row.duration_ms, row.budget_limit) === row.request_digest, 'oauth_context_invalid');
        auxCore.upsertInTransaction(input, time);
      });
    },
    find(args) {
      oauthData(args, ['model', 'id']);
      if (auxiliary(args.model)) return support.find(args);
      if (OAUTH_TOKEN_MODELS.includes(args.model)) return tokens.find(args);
      const id = sourceId(args);
      return fenced(time => {
        oauthCheck(codec?.available(), 'oauth_storage_key_unavailable');
        const row = artifact('Grant', sha(id)); if (!row || row.expires_at <= time) return undefined;
        let own; try { own = live(connection(row.connection_id), time); } catch (error) { if (denied(error)) return undefined; throw error; }
        return grantPayload(row, own, time, id).payload;
      });
    },
    findByUid: support.findByUid,
    destroy(args) {
      oauthData(args, ['model', 'id']); if (auxiliary(args.model)) return support.destroy(args);
      if (OAUTH_TOKEN_MODELS.includes(args.model)) return tokens.destroy(args);
      const id = sourceId(args);
      return fenced(time => {
        const own = get('SELECT * FROM cap_oauth_connections WHERE provider_grant_id=? AND issuer=?', id, issuer);
        if (!own) return;
        revoke(own, time); db.prepare("DELETE FROM cap_oauth_artifacts WHERE model='Grant' AND id_hash=?").run(sha(id));
      });
    },
    consume(args) { return tokens.consume(args); },
    revokeByGrantId(args) {
      oauthData(args, ['providerGrantId']); const id = oauthProviderId(args.providerGrantId);
      return fenced(time => {
        const own = get('SELECT * FROM cap_oauth_connections WHERE provider_grant_id=? AND issuer=?', id, issuer);
        if (own) revoke(own, time);
      });
    },
  });
  function execute({ op, args, actor }) {
    oauthCheck(OAUTH_OPERATIONS.includes(op), 'operation_unknown');
    const keys = op.endsWith('.list') ? ['expectedAccountId', 'limit', 'cursor']
      : op.endsWith('.revoke') ? ['expectedAccountId', 'connectionId']
        : ['expectedAccountId', 'interactionId', 'contextDigest', 'browserNonce'];
    oauthData(args, keys); access.verifyOwner({ actor, args });
    return atomic(time => {
      const owner = access.verifyOwner({ actor, args });
      if (op.endsWith('.list')) {
        const limit = integer(args.limit ?? 20, 1, 50); let cursor;
        const fingerprint = canonicalHash(['soty.oauth-owner-list.v1', owner.accountId]);
        if (args.cursor !== undefined && args.cursor !== null) {
          oauthString(args.cursor, 600);
          try {
            cursor = JSON.parse(Buffer.from(args.cursor, 'base64url').toString('utf8'));
            oauthData(cursor, ['fingerprint', 'createdAt', 'id']);
            oauthCheck(cursor.fingerprint === fingerprint); integer(cursor.createdAt, 0, Number.MAX_SAFE_INTEGER); oauthId(cursor.id);
          } catch { throw new AccessError('cursor_invalid'); }
        }
        const rows = db.prepare(`SELECT * FROM cap_oauth_connections WHERE account_id=?
          ${cursor ? 'AND (created_at<? OR (created_at=? AND id<?))' : ''}
          ORDER BY created_at DESC,id DESC LIMIT ?`).all(owner.accountId,
          ...(cursor ? [cursor.createdAt, cursor.createdAt, cursor.id] : []), limit + 1);
        const selected = rows.slice(0, limit), last = selected.at(-1);
        return { connections: selected.map(row => {
          let active = true; try { authority.live(row, time); } catch (error) { if (denied(error)) active = false; else throw error; }
          const b = get("SELECT * FROM cap_budgets WHERE root_grant_id=? AND unit='invocations'", row.root_grant_id);
          return { id: row.id, clientProfile: row.static_client_id, resource: row.resource, createdAt: row.created_at,
            expiresAt: row.expires_at, revokedAt: row.revoked_at, active,
            budget: { limit: b.limit_amount, reserved: b.reserved_amount, spent: b.spent_amount,
              remaining: b.limit_amount - b.reserved_amount - b.spent_amount } };
        }), nextCursor: rows.length > limit ? Buffer.from(JSON.stringify({ fingerprint, createdAt: last.created_at, id: last.id })).toString('base64url') : null };
      }
      if (op.endsWith('.revoke')) {
        const own = get('SELECT * FROM cap_oauth_connections WHERE id=? AND account_id=?', oauthId(args.connectionId), owner.accountId);
        oauthCheck(own, 'not_found'); revoke(own, time); return { connectionId: own.id, revoked: true };
      }
      const id = oauthProviderId(args.interactionId), row = proposal(id, nonce(args.browserNonce), time, { terminalReplay: true });
      oauthCheck(typeof args.contextDigest === 'string' && args.contextDigest === row.request_digest, 'oauth_interaction_conflict');
      const approval = op.endsWith('.approve'), decision = approval ? 'approved' : 'denied';
      if (row.decision !== 'pending') {
        oauthCheck(row.decision === decision && row.decided_account_id === owner.accountId, 'oauth_interaction_conflict');
        return approval ? { connectionId: row.connection_id, approved: true, replayed: true } : { denied: true, replayed: true };
      }
      let connectionId = null;
      if (approval) {
        currentIntent(row, id, time);
        oauthCheck(get("SELECT count(*) AS n FROM cap_oauth_connections WHERE account_id=? AND state='active' AND expires_at>?", owner.accountId, time).n < 16
          && get('SELECT count(*) AS n FROM cap_oauth_connections').n < 10000
          && get('SELECT count(*) AS n FROM cap_principals WHERE account_id=?', owner.accountId).n < 1000
          && get('SELECT count(*) AS n FROM cap_grants WHERE account_id=?', owner.accountId).n < 10000, 'oauth_quota_exceeded');
        const expiresAt = integer(time + row.duration_ms, time + 1, Number.MAX_SAFE_INTEGER);
        const created = authority.create({ owner, clientProfile: row.static_client_id, expiresAt, budgetLimit: row.budget_limit, time });
        connectionId = newId('oauth');
        db.prepare(`INSERT INTO cap_oauth_connections(id,account_id,client_id,principal_id,root_grant_id,creator_device_id,
          issuer,static_client_id,resource,scope,consent_digest,state,created_at,expires_at)
          VALUES(?,?,?,?,?,?,?,?,?,? ,?,'active',?,?)`).run(connectionId, owner.accountId, created.clientId, created.principalId,
          created.rootGrantId, owner.deviceId, issuer, row.static_client_id, row.resource, OAUTH_SCOPE, row.request_digest, time, expiresAt);
      }
      const changed = db.prepare(`UPDATE cap_oauth_interactions SET decision=?,decided_at=?,decided_account_id=?,decided_device_id=?,connection_id=?
        WHERE uid_hash=? AND decision='pending'`).run(decision, time, owner.accountId, owner.deviceId, connectionId, row.uid_hash);
      oauthCheck(changed.changes === 1, 'oauth_interaction_conflict');
      db.prepare('INSERT INTO cap_audit(id,account_id,kind,object_type,object_id,actor_type,actor_id,created_at) VALUES(?,?,?,?,?,?,?,?)')
        .run(newId('event'), owner.accountId, op, 'oauth_connection', connectionId ?? row.uid_hash, 'connect', owner.deviceId, time);
      access.verifyOwner({ actor, args });
      return approval ? { connectionId, approved: true, replayed: false } : { denied: true, replayed: false };
    });
  }
  const oauth = Object.freeze({
    readiness() {
      ensureOpen();
      try {
        identity();
        return { schemaVersion, available: codec?.available() === true && nativeBindingReady() === true };
      } catch { return { schemaVersion, available: false }; }
    },
    prepareInteraction(args) {
      oauthData(args, ['interactionId', 'browserNonce', 'durationMs', 'budgetLimit']);
      const id = oauthProviderId(args.interactionId), nonceHash = nonce(args.browserNonce);
      const durationMs = integer(args.durationMs ?? 86400000, 1000, 86400000), budgetLimit = integer(args.budgetLimit ?? 20, 1, 20);
      return atomic(time => {
        const previous = get('SELECT * FROM cap_oauth_interactions WHERE uid_hash=?', sha(id));
        if (previous) {
          oauthCheck(previous.issuer === issuer && previous.browser_nonce_hash === nonceHash && previous.duration_ms === durationMs
            && previous.budget_limit === budgetLimit && previous.expires_at > time, 'oauth_interaction_conflict');
          if (previous.decision === 'pending') currentIntent(previous, id, time);
          return presentation(previous, id);
        }
        const payload = auxCore.findInTransaction({ model: 'Interaction', id }, time);
        oauthCheck(payload, 'oauth_interaction_not_found');
        oauthCheck(get("SELECT count(*) AS n FROM cap_oauth_interactions WHERE expires_at>? AND decision='pending'", time).n < 256
          && get('SELECT count(*) AS n FROM cap_oauth_interactions WHERE expires_at>?', time).n < 1024, 'oauth_quota_exceeded');
        const digest = intentDigest(payload, durationMs, budgetLimit), p = payload.params;
        db.prepare(`INSERT INTO cap_oauth_interactions(uid_hash,issuer,static_client_id,resource,redirect_uri,request_digest,browser_nonce_hash,
          duration_ms,budget_limit,created_at,expires_at,decision) VALUES(?,?,?,?,?,?,?,?,?,?,?,'pending')`)
          .run(sha(id), issuer, p.client_id, p.resource, p.redirect_uri, digest, nonceHash, durationMs, budgetLimit, time, payload.exp * 1000);
        return presentation(proposal(id, nonceHash, time), id);
      });
    },
    readInteraction(args) {
      const { id, nonceHash } = prepared(args);
      return atomic(time => {
        const row = proposal(id, nonceHash, time);
        if (row.decision === 'pending') currentIntent(row, id, time);
        return presentation(row, id);
      });
    },
    beginGrantBinding(args) {
      const { id, nonceHash } = prepared(args);
      return fenced(time => {
        oauthCheck(codec?.available(), 'oauth_storage_key_unavailable');
        const row = proposal(id, nonceHash, time); oauthCheck(row.decision === 'approved', 'oauth_context_invalid');
        const own = live(connection(row.connection_id), time), tick = pruneContexts(time);
        oauthCheck(activeContexts.size < 16, 'oauth_quota_exceeded');
        const context = Object.freeze(Object.create(null));
        const state = { live: true, connectionId: own.id, interactionHash: row.uid_hash, nonceHash,
          deadline: tick + 30000, expiresAt: Math.min(row.expires_at, own.expires_at) };
        contexts.set(context, state); activeContexts.add(state);
        return { context, providerGrantId: own.provider_grant_id, connection: { id: own.id, accountId: own.account_id,
          staticClientId: own.static_client_id, issuer: own.issuer, resource: own.resource, scope: own.scope, expiresAt: own.expires_at } };
      });
    },
    endGrantBinding(context) {
      const state = context && typeof context === 'object' ? contexts.get(context) : null;
      if (state) { state.live = false; activeContexts.delete(state); }
    },
    authenticateBearer(args) {
      oauthData(args, ['token', 'audience']);
      const { token, audience } = args;
      oauthCheck(typeof token === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(token)
        && Object.values(configuration.resources).includes(audience), 'authorization_required');
      // Resource-server access survives AS key/operational unavailability. The
      // immutable digest/link and current common authority are its evidence.
      return fenced(() => authority.authenticate({ tokenDigest: sha(token), issuer, audience }));
    },
    cleanup(args = {}) {
      oauthData(args, ['limit']); const limit = integer(args.limit ?? 64, 1, 64);
      return atomic(time => {
        pruneContexts(time);
        return tokens.cleanup(time, limit);
      });
    }, artifactStore,
  });
  return Object.freeze({ oauth, execute, close() {
    oauthCheck(!running && !fencing, 'nested_transaction'); closed = true;
    for (const state of activeContexts) state.live = false;
    activeContexts.clear(); support.close(); codec?.close();
  } });
}
