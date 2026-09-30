import { createHash } from 'node:crypto';
import { AccessError } from './validation.mjs';
import { createOAuthArtifactCodec } from './oauth-crypto.mjs';
import { OAUTH_MODELS, OAUTH_PROFILE, createOAuthUnboundProfile, oauthCheck, oauthData,
  oauthProviderId, oauthSynchronous, oauthTime } from './oauth-profile.mjs';

const digest = value => createHash('sha256').update(value).digest('hex');
const unbound = model => model === 'Session' || model === 'Interaction';
const safeTime = value => Number.isSafeInteger(value) && value >= 0;

/** Internal first increment: durable auxiliary AS records only. This factory
 * deliberately admits no Grant/token, actor, consent or execution authority.
 * The service does not expose it before the complete coordinator is ready. */
export function createOAuthArtifactStore({ db, projectId, registryId, schemaVersion, clock,
  transaction, ensureOpen, configuration, captureCore }) {
  oauthCheck(typeof clock === 'function' && typeof transaction === 'function' && typeof ensureOpen === 'function', 'oauth_configuration_invalid');
  const { issuer } = configuration;
  const codec = schemaVersion === 3 ? createOAuthArtifactCodec({ registryId, issuer,
    artifactKey: configuration.artifactKey, artifactKeyId: configuration.artifactKeyId }) : null;
  const profile = createOAuthUnboundProfile(configuration);
  let closed = false, running = false;
  const get = (sql, ...args) => db.prepare(sql).get(...args);
  const rowFor = (model, idHash) => get('SELECT * FROM cap_oauth_artifacts WHERE model=? AND id_hash=?', model, idHash);
  function identity() {
    ensureOpen(); oauthCheck(!closed, 'service_closed');
    oauthCheck(schemaVersion === 3 && get('PRAGMA user_version').user_version === 3, 'oauth_unavailable');
    const rows = db.prepare('SELECT key,value FROM cap_metadata ORDER BY key').all();
    const values = Object.fromEntries(rows.map(row => [row.key, row.value]));
    oauthCheck(rows.length === 3 && values.lineage === 'soty.capabilities.sqlite.v3'
      && values.project_id === projectId && values.registry_id === registryId, 'capabilities_storage_corrupt');
  }
  function atomic(action, keyRequired = true) {
    ensureOpen(); oauthCheck(!closed, 'service_closed');
    oauthCheck(!running, 'nested_transaction');
    oauthCheck(schemaVersion === 3, 'oauth_unavailable');
    if (keyRequired) oauthCheck(codec?.available(), 'oauth_storage_key_unavailable');
    running = true;
    let accepting = true, invoked = false;
    try {
      const result = transaction(() => {
        oauthCheck(accepting && !invoked && running, 'oauth_context_invalid'); invoked = true;
        identity();
        const result = oauthSynchronous(action(oauthTime(clock())));
        identity();
        return result;
      }, { busyMs: 100 });
      oauthSynchronous(result);
      oauthCheck(invoked, 'oauth_context_invalid');
      return result;
    } catch (error) {
      if (error instanceof AccessError) throw error;
      if ([5, 6].includes(error?.errcode & 255)) throw new AccessError('oauth_storage_busy');
      throw new AccessError('capabilities_storage_corrupt');
    } finally { accepting = false; running = false; }
  }
  function modelId(args, keys) {
    oauthData(args, keys);
    oauthCheck(OAUTH_MODELS.includes(args.model));
    oauthCheck(unbound(args.model), 'oauth_unavailable');
    oauthProviderId(args.id);
    return digest(args.id);
  }
  function metadataReferences(payload) {
    if (payload.kind !== 'Session' || !payload.authorizations) return;
    for (const [clientId, reference] of Object.entries(payload.authorizations)) {
      if (!reference?.grantId) continue;
      const connection = get('SELECT account_id,static_client_id,issuer FROM cap_oauth_connections WHERE provider_grant_id=?', reference.grantId);
      // Remembered, possibly expired/revoked metadata is not current authority.
      oauthCheck(connection && connection.account_id === payload.accountId
        && connection.static_client_id === clientId && connection.issuer === issuer);
    }
  }
  function checked(row, time, expectedId) {
    try {
      oauthCheck(row && unbound(row.model) && row.issuer === issuer && row.profile === OAUTH_PROFILE
        && row.connection_id === null && row.provider_grant_id === null && row.consumed_at === null
        && safeTime(row.created_at) && safeTime(row.expires_at) && safeTime(row.retain_until)
        && row.expires_at > row.created_at && row.expires_at <= row.retain_until
        && row.retain_until - row.created_at <= 600000, 'capabilities_storage_corrupt');
      const payload = codec.open({ model: row.model, idHash: row.id_hash, profile: row.profile,
        keyId: row.key_id, payloadDigest: row.payload_digest, payloadCipher: row.payload_cipher });
      oauthProviderId(payload.jti);
      oauthCheck(digest(payload.jti) === row.id_hash && (expectedId === undefined || payload.jti === expectedId));
      const snapshot = profile.snapshot({ model: row.model, id: payload.jti, payload, nowMs: time, allowExpired: true });
      oauthCheck(snapshot.createdAt === row.created_at && snapshot.expiresAt === row.expires_at
        && snapshot.retainUntil === row.retain_until
        && row.session_uid_hash === (row.model === 'Session' ? digest(payload.uid) : null));
      metadataReferences(snapshot.payload);
      return snapshot;
    } catch (error) {
      if (error?.code === 'oauth_storage_key_unavailable') throw error;
      throw new AccessError('capabilities_storage_corrupt');
    }
  }
  function quota() {
    const auxiliary = get("SELECT count(*) AS n FROM cap_oauth_artifacts WHERE model IN ('Session','Interaction')").n;
    const total = get('SELECT count(*) AS n FROM cap_oauth_artifacts').n;
    oauthCheck(auxiliary < 1024 && total < 65536, 'oauth_quota_exceeded');
  }
  function findInTransaction(args, time) {
    const idHash = modelId(args, ['model', 'id']);
    oauthCheck(db.isTransaction, 'oauth_context_invalid'); identity();
    oauthCheck(codec?.available(), 'oauth_storage_key_unavailable');
    const row = rowFor(args.model, idHash);
    if (!row || row.expires_at <= time) return undefined;
    return checked(row, time, args.id).payload;
  }
  function upsertInTransaction(args, time) {
    oauthCheck(db.isTransaction, 'oauth_context_invalid'); identity();
    oauthCheck(codec?.available(), 'oauth_storage_key_unavailable');
    const idHash = modelId(args, ['model', 'id', 'payload', 'expiresIn', 'request', 'stagedGrant']);
    const { model, id, payload, expiresIn } = args;
    oauthCheck(args.request === undefined && args.stagedGrant === undefined, 'oauth_context_invalid');
    const next = profile.snapshot({ model, id, payload, nowMs: time, expiresIn });
    metadataReferences(next.payload);
    const current = rowFor(model, idHash);
    const sessionUidHash = model === 'Session' ? digest(next.payload.uid) : null;
    if (current) {
      const previous = checked(current, time, id);
      oauthCheck(current.expires_at > time && previous.createdAt === next.createdAt
        && current.session_uid_hash === sessionUidHash && previous.retainUntil === next.retainUntil);
    } else {
      quota();
      if (sessionUidHash) oauthCheck(!get("SELECT 1 FROM cap_oauth_artifacts WHERE model='Session' AND session_uid_hash=?", sessionUidHash));
    }
    const sealed = codec.seal({ model, idHash, payload: next.payload });
    if (current) db.prepare(`UPDATE cap_oauth_artifacts SET payload_cipher=?,payload_digest=?,expires_at=?
      WHERE model=? AND id_hash=?`).run(sealed.payloadCipher, sealed.payloadDigest, next.expiresAt, model, idHash);
    else db.prepare(`INSERT INTO cap_oauth_artifacts(model,id_hash,issuer,profile,key_id,payload_cipher,payload_digest,
      connection_id,provider_grant_id,session_uid_hash,created_at,expires_at,retain_until,consumed_at)
      VALUES(?,?,?,?,?,?,?,NULL,NULL,?,?,?,?,NULL)`).run(model, idHash, issuer, sealed.profile, sealed.keyId,
      sealed.payloadCipher, sealed.payloadDigest, sessionUidHash, next.createdAt, next.expiresAt, next.retainUntil);
  }
  if (captureCore !== undefined) {
    oauthCheck(typeof captureCore === 'function', 'oauth_configuration_invalid');
    oauthSynchronous(captureCore(Object.freeze({ findInTransaction, upsertInTransaction })));
  }
  return Object.freeze({
    hasKey() { return !closed && Boolean(codec?.available()); },
    upsert(args) {
      modelId(args, ['model', 'id', 'payload', 'expiresIn', 'request', 'stagedGrant']);
      const { model, id, payload, expiresIn } = args;
      // No context silently gains a use outside its future Grant-only port.
      oauthCheck(args.request === undefined && args.stagedGrant === undefined, 'oauth_context_invalid');
      return atomic(time => upsertInTransaction({ model, id, payload, expiresIn }, time));
    },
    find(args) {
      const idHash = modelId(args, ['model', 'id']);
      const { model, id } = args;
      return atomic(time => {
        const row = rowFor(model, idHash);
        if (!row || row.expires_at <= time) return undefined;
        return checked(row, time, id).payload;
      });
    },
    findByUid(args) {
      oauthData(args, ['uid']); oauthProviderId(args.uid);
      return atomic(time => {
        const row = get("SELECT * FROM cap_oauth_artifacts WHERE model='Session' AND session_uid_hash=?", digest(args.uid));
        if (!row || row.expires_at <= time) return undefined;
        return checked(row, time).payload;
      });
    },
    destroy(args) {
      const idHash = modelId(args, ['model', 'id']);
      const { model, id } = args;
      return atomic(time => {
        const row = rowFor(model, idHash);
        if (!row) return;
        checked(row, time, id); // Wrong key/corruption is never a reset.
        db.prepare('DELETE FROM cap_oauth_artifacts WHERE model=? AND id_hash=?').run(model, idHash);
      });
    },
    consume() { throw new AccessError('oauth_unavailable'); },
    revokeByGrantId() { throw new AccessError('oauth_unavailable'); },
    cleanup(args = {}) {
      oauthData(args, ['limit']);
      const limit = args.limit ?? 64;
      oauthCheck(Number.isSafeInteger(limit) && limit >= 1 && limit <= 64);
      return atomic(time => {
        // One indexed identity batch. This increment cleans only auxiliary
        // artifacts; future credential/proposal cleanup must share this budget.
        const rows = db.prepare(`SELECT model,id_hash FROM cap_oauth_artifacts INDEXED BY cap_oauth_artifacts_retention
          WHERE retain_until<=? AND model IN ('Session','Interaction')
          ORDER BY retain_until,model,id_hash LIMIT ?`).all(time, limit);
        for (const row of rows) db.prepare('DELETE FROM cap_oauth_artifacts WHERE model=? AND id_hash=?').run(row.model, row.id_hash);
        return { artifactsDeleted: rows.length, interactionsDeleted: 0, credentialsDeleted: 0 };
      }, false);
    },
    close() { oauthCheck(!running, 'nested_transaction'); closed = true; codec?.close(); },
  });
}
