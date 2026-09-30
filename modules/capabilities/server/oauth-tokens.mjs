import { createHash } from 'node:crypto';
import { AccessError, newId } from './validation.mjs';
import { OAUTH_PROFILE, oauthCheck, oauthData, oauthProviderId, oauthTime, snapshotOAuthJson } from './oauth-profile.mjs';
import { OAUTH_TOKEN_MODELS, createOAuthTokenProfile, oauthOpaqueId,
  oauthTokenRequestOutcome, snapshotOAuthTokenRequest } from './oauth-token-profile.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const denied = error => ['access_denied', 'authorization_required'].includes(error?.code);
const safe = value => Number.isSafeInteger(value) && value >= 0;

/** Private composition, sharing the coordinator's one Connect/Caps fence and
 * codec. It mints durable credential rows, never an external bearer actor. */
export function createOAuthTokenStore({ db, configuration, clock, codec, fenced, live, revoke, boundGrant }) {
  const { issuer } = configuration, profile = createOAuthTokenProfile(configuration);
  const get = (sql, ...args) => db.prepare(sql).get(...args);
  const artifact = (model, hash) => get('SELECT * FROM cap_oauth_artifacts WHERE model=? AND id_hash=?', model, hash);
  const connection = id => get('SELECT * FROM cap_oauth_connections WHERE id=?', id);
  let cleanupCategory = 0, credentialCursor = null;
  function key() { oauthCheck(codec?.available(), 'oauth_storage_key_unavailable'); }
  function input(args, fields) {
    oauthData(args, fields); oauthCheck(OAUTH_TOKEN_MODELS.includes(args.model), 'oauth_unavailable');
    return { model: args.model, id: oauthOpaqueId(args.id) };
  }
  function pins(row, own) {
    oauthCheck(row && own && row.issuer === issuer && row.issuer === own.issuer && row.profile === OAUTH_PROFILE
      && row.connection_id === own.id && row.provider_grant_id === own.provider_grant_id && row.session_uid_hash === null
      && safe(row.created_at) && safe(row.expires_at) && safe(row.retain_until) && row.created_at >= own.created_at
      && row.expires_at > row.created_at && row.expires_at <= own.expires_at
      && row.retain_until === (row.model === 'AccessToken' ? row.expires_at : own.expires_at)
      && (row.consumed_at === null || (row.model !== 'AccessToken' && safe(row.consumed_at)
        && row.consumed_at >= row.created_at && row.consumed_at < row.expires_at)), 'capabilities_storage_corrupt');
  }
  function credential(row, own) {
    const link = get('SELECT * FROM cap_oauth_credentials WHERE token_digest=?', row.id_hash);
    const k = link && get('SELECT * FROM cap_credentials WHERE id=?', link.credential_id);
    oauthCheck(k && link.connection_id === own.id && link.token_digest === k.digest && k.digest === row.id_hash
      && k.account_id === own.account_id && k.client_id === own.client_id && k.principal_id === own.principal_id
      && k.grant_id === own.root_grant_id && k.audience === own.resource && k.created_at === row.created_at
      && k.expires_at === row.expires_at && link.created_at === row.created_at && link.expires_at === row.expires_at
      && (k.revoked_at === null || (safe(k.revoked_at) && k.revoked_at >= k.created_at)), 'capabilities_storage_corrupt');
    return k;
  }
  function checked(row, own, time, id) {
    pins(row, own);
    oauthCheck(row.id_hash === sha(id) && row.created_at <= time, 'capabilities_storage_corrupt');
    try {
      const payload = codec.open({ model: row.model, idHash: row.id_hash, profile: row.profile,
        keyId: row.key_id, payloadCipher: row.payload_cipher, payloadDigest: row.payload_digest });
      const result = profile.snapshot({ model: row.model, id, payload, connection: own, nowMs: time, allowExpired: true });
      oauthCheck(result.expiresAt === row.expires_at && result.retainUntil === row.retain_until, 'capabilities_storage_corrupt');
      if (row.model === 'AccessToken') credential(row, own);
      return result;
    } catch (error) {
      if (error?.code === 'oauth_storage_key_unavailable') throw error;
      throw new AccessError('capabilities_storage_corrupt');
    }
  }
  function current(row, time) {
    try { return live(row && connection(row.connection_id), time); }
    catch (error) { if (denied(error)) return null; throw error; }
  }
  function requestCheck(request, own, model, consuming = false) {
    const status = oauthTokenRequestOutcome({ request, connection: own, model, consuming });
    if (!consuming && status) throw new AccessError(status === 'invalid_target' ? 'oauth_invalid_target' : 'oauth_context_invalid');
    return status;
  }
  function pruneCredentials(time, limit) {
    if (!limit) return { visited: 0, deleted: 0 };
    // Advancing a private keyset, including retained references, prevents one
    // permanently retained first page from starving later transient records.
    const rows = db.prepare(`SELECT credential_id,expires_at FROM cap_oauth_credentials INDEXED BY cap_oauth_credentials_expiry
      WHERE expires_at<=? ${credentialCursor ? 'AND (expires_at,credential_id)>(?,?)' : ''}
      ORDER BY expires_at,credential_id LIMIT ?`).all(time,
      ...(credentialCursor ? [credentialCursor.at, credentialCursor.id] : []), limit);
    let deleted = 0;
    for (const row of rows) {
      credentialCursor = { at: row.expires_at, id: row.credential_id };
      if (get("SELECT 1 FROM cap_invocations WHERE json_extract(authorization_json,'$.credentialId')=? LIMIT 1", row.credential_id)) continue;
      const link = get('SELECT * FROM cap_oauth_credentials WHERE credential_id=?', row.credential_id);
      const own = connection(link.connection_id), raw = artifact('AccessToken', link.token_digest);
      if (raw) { pins(raw, own); credential(raw, own); }
      else {
        const k = get('SELECT * FROM cap_credentials WHERE id=?', row.credential_id);
        oauthCheck(k && own && k.digest === link.token_digest && k.account_id === own.account_id
          && k.client_id === own.client_id && k.principal_id === own.principal_id && k.grant_id === own.root_grant_id
          && k.audience === own.resource && k.created_at === link.created_at && k.expires_at === link.expires_at,
        'capabilities_storage_corrupt');
      }
      // A raw expired AT is another cleanup identity. Keep its link until its
      // own artifact pass, so every completed transaction remains reopenable.
      if (raw) continue;
      db.prepare('DELETE FROM cap_oauth_credentials WHERE credential_id=?').run(row.credential_id);
      db.prepare('DELETE FROM cap_credentials WHERE id=?').run(row.credential_id); deleted++;
    }
    if (rows.length < limit) credentialCursor = null;
    return { visited: rows.length, deleted };
  }
  function cleanup(time, limit) {
    oauthCheck(db.isTransaction, 'oauth_context_invalid');
    let left = limit; const result = { artifactsDeleted: 0, interactionsDeleted: 0, credentialsDeleted: 0 };
    const first = cleanupCategory; cleanupCategory = (cleanupCategory + 1) % 3;
    for (let step = 0; step < 3 && left; step++) {
      const category = (first + step) % 3;
      if (category === 0) {
        const rows = db.prepare(`SELECT model,id_hash FROM cap_oauth_artifacts INDEXED BY cap_oauth_artifacts_retention
          WHERE retain_until<=? ORDER BY retain_until,model,id_hash LIMIT ?`).all(time, left);
        for (const row of rows) db.prepare('DELETE FROM cap_oauth_artifacts WHERE model=? AND id_hash=?').run(row.model, row.id_hash);
        result.artifactsDeleted += rows.length; left -= rows.length;
      } else if (category === 1) {
        const rows = db.prepare(`SELECT uid_hash FROM cap_oauth_interactions
          WHERE expires_at<=? ORDER BY expires_at,uid_hash LIMIT ?`).all(time, left);
        for (const row of rows) db.prepare('DELETE FROM cap_oauth_interactions WHERE uid_hash=?').run(row.uid_hash);
        result.interactionsDeleted += rows.length; left -= rows.length;
      } else {
        const removed = pruneCredentials(time, left); left -= removed.visited; result.credentialsDeleted += removed.deleted;
      }
    }
    return result;
  }
  const api = {
    upsert(args) {
      const { model, id } = input(args, ['model', 'id', 'payload', 'expiresIn', 'request', 'stagedGrant']);
      oauthCheck(args.stagedGrant === undefined, 'oauth_context_invalid');
      const payload = snapshotOAuthJson(args.payload), request = snapshotOAuthTokenRequest(args.request), expiresIn = args.expiresIn;
      oauthData(payload); oauthProviderId(payload.grantId);
      return fenced(time => {
        key(); const own = live(get('SELECT * FROM cap_oauth_connections WHERE provider_grant_id=? AND issuer=?', payload.grantId, issuer), time);
        requestCheck(request, own, model);
        const grant = boundGrant(own, time);
        const next = profile.snapshot({ model, id, payload, connection: own, nowMs: time, expiresIn, grant });
        const previous = artifact(model, sha(id));
        if (previous) {
          const stored = checked(previous, own, time, id);
          oauthCheck(stored.json === next.json && previous.expires_at > time, 'oauth_invalid_artifact');
          if (model === 'AccessToken') oauthCheck(credential(previous, own).revoked_at === null, 'access_denied');
          return;
        }
        // This bounded cleanup is inside the same transaction. Failed quota or
        // write admission does not commit partial housekeeping/issuance.
        cleanup(time, 64);
        oauthCheck(get('SELECT count(*) AS n FROM cap_oauth_artifacts WHERE connection_id=?', own.id).n < 1024
          && get('SELECT count(*) AS n FROM cap_oauth_artifacts').n < 65536, 'oauth_quota_exceeded');
        if (model === 'AccessToken') oauthCheck(get(`SELECT count(*) AS n FROM (
          SELECT 1 FROM cap_oauth_credentials l INDEXED BY cap_oauth_credentials_expiry
          JOIN cap_credentials k ON k.id=l.credential_id
          WHERE l.expires_at>? AND k.account_id=? AND k.revoked_at IS NULL LIMIT 64)`,
        time, own.account_id).n < 64, 'oauth_quota_exceeded');
        const sealed = codec.seal({ model, idHash: sha(id), payload: next.payload });
        db.prepare(`INSERT INTO cap_oauth_artifacts(model,id_hash,issuer,profile,key_id,payload_cipher,payload_digest,
          connection_id,provider_grant_id,created_at,expires_at,retain_until)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`).run(model, sha(id), issuer, sealed.profile, sealed.keyId,
          sealed.payloadCipher, sealed.payloadDigest, own.id, own.provider_grant_id, time, next.expiresAt, next.retainUntil);
        if (model === 'AccessToken') {
          const credentialId = newId('credential');
          db.prepare(`INSERT INTO cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at)
            VALUES(?,?,?,?,?,?,?,?,?)`).run(credentialId, sha(id), own.account_id, own.client_id, own.principal_id,
            own.root_grant_id, own.resource, next.expiresAt, time);
          db.prepare(`INSERT INTO cap_oauth_credentials(credential_id,connection_id,token_digest,created_at,expires_at)
            VALUES(?,?,?,?,?)`).run(credentialId, own.id, sha(id), time, next.expiresAt);
        }
        const committedAt = oauthTime(clock());
        oauthCheck(next.expiresAt > committedAt, 'oauth_invalid_artifact'); live(connection(own.id), committedAt);
      });
    },
    find(args) {
      const { model, id } = input(args, ['model', 'id']);
      return fenced(time => {
        key(); const row = artifact(model, sha(id));
        if (!row || row.expires_at <= time) return undefined;
        const own = current(row, time); if (!own) return undefined;
        const value = checked(row, own, time, id).payload;
        if (model === 'AccessToken' && credential(row, own).revoked_at !== null) return undefined;
        return row.consumed_at === null ? value : { ...value, consumed: row.consumed_at / 1000 };
      });
    },
    consume(args) {
      const { model, id } = input(args, ['model', 'id', 'request']);
      oauthCheck(model !== 'AccessToken', 'oauth_unavailable');
      const request = snapshotOAuthTokenRequest(args.request);
      return fenced(time => {
        key(); const row = artifact(model, sha(id));
        if (!row || row.expires_at <= time) return { status: 'invalid_grant' };
        const own = current(row, time); if (!own) return { status: 'invalid_grant' };
        checked(row, own, time, id);
        const refusal = requestCheck(request, own, model, true); if (refusal) return { status: refusal };
        if (row.consumed_at !== null) { revoke(own, time); return { status: 'invalid_grant' }; }
        boundGrant(own, time);
        const updated = db.prepare('UPDATE cap_oauth_artifacts SET consumed_at=? WHERE model=? AND id_hash=? AND consumed_at IS NULL')
          .run(time, model, sha(id));
        if (updated.changes !== 1) { revoke(own, time); return { status: 'invalid_grant' }; }
        const committedAt = oauthTime(clock());
        oauthCheck(row.expires_at > committedAt, 'oauth_context_invalid'); live(connection(own.id), committedAt);
        return { status: 'consumed' };
      });
    },
    destroy(args) {
      const { model, id } = input(args, ['model', 'id']);
      return fenced(time => {
        const row = artifact(model, sha(id));
        const link = !row && model === 'AccessToken' ? get('SELECT * FROM cap_oauth_credentials WHERE token_digest=?', sha(id)) : null;
        const own = connection(row?.connection_id ?? link?.connection_id ?? null);
        if (!own || own.issuer !== issuer) return;
        if (row) pins(row, own);
        revoke(own, time);
        if (row) db.prepare('DELETE FROM cap_oauth_artifacts WHERE model=? AND id_hash=?').run(model, sha(id));
      });
    }, cleanup,
  };
  return Object.freeze(api);
}
