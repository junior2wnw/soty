import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { join, dirname, resolve, basename } from 'node:path';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { createOrdinaryHttpFixture } from './support/ordinary-http.mjs';
import { createSourceRpProtocol, createSourceRpSessionService, SOURCE_RP_PROFILE } from '../../source-rp/server/index.mjs';
import { sqliteSource } from '../../source-rp/test/support/sqlite-source.mjs';
import { resourceConsentDigest } from '../../apps/scoped-embed/resource-profile.mjs';
import { digest, check } from '../server/wire.mjs';

const random = () => randomBytes(32).toString('base64url');
function controlledClock(t) {
  const Original = globalThis.Date; let offset = 0;
  class FixtureDate extends Original { constructor(...args) { super(...(args.length ? args : [Original.now() + offset])); }
    static now() { return Original.now() + offset; } }
  globalThis.Date = FixtureDate; t.after(() => { globalThis.Date = Original; });
  return seconds => { offset += seconds * 1000; };
}

// Actual OIDC/RP49 and encrypted Source/Native SQLite. Only Root authority IPC
// and the host-owned lookup/CAS adapter are controlled fixture ports.
function renewableHost(t, realm, { root, client }) {
  const base = realm.store.storage, semanticDigest = resourceConsentDigest(realm.profile);
  if (!realm.rpNative) {
    const directory = mkdtempSync(join(tmpdir(), 'soty-resume-rp-'));
    realm.rpNative = sqliteSource(join(directory, 'source.sqlite'));
    t.after(() => { realm.rpNative.close(); assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
      assert.match(basename(directory), /^soty-resume-rp-/u); rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
    realm.refreshCalls = 0; realm.lookups = 0; realm.rebinds = 0;
  }
  const protocol = createSourceRpProtocol({ issuer: root.issuer, clientId: client.id, clientSecret: client.clientSecret,
    redirectUri: client.redirectUri, renewalProfile: SOURCE_RP_PROFILE });
  const service = createSourceRpSessionService({ storagePort: realm.rpNative.storagePort,
    protocol: { ...protocol, async renew(input) { realm.refreshCalls++; return protocol.renew(input); } },
    profileDigest: realm.profile.digest, keyId: realm.rpNative.keyId, encrypt: realm.rpNative.encrypt, decrypt: realm.rpNative.decrypt });
  realm.storage = { ...base,
    async findRenewableSession(input) {
      realm.lookups++; await realm.beforeLookup?.();
      if (realm.lookupOverride) return realm.lookupOverride(input);
      const principal = realm.store.db.prepare('SELECT principal_id FROM native_links WHERE issuer=? AND subject=?')
        .get(input.context.humanPrincipal.issuer, input.context.humanPrincipal.subject)?.principal_id;
      if (!principal) return null;
      // Bound BEFORE decryption, to this Native account and exact namespace.
      const rows = realm.store.db.prepare('SELECT id_hash FROM source_sessions WHERE principal_id=? AND resource_id=? AND active=1 AND expires_at>? ORDER BY id_hash LIMIT 9')
        .all(principal, 'selected', Date.now());
      check(rows.length <= 8, 'source_fixture_capacity', 503);
      const candidates = [];
      for (const row of rows) {
        const session = await base.readSession(row.id_hash);
        if (session.rpMarker && session.semanticDigest === input.semanticDigest
          && session.identity.issuer === input.context.humanPrincipal.issuer && session.identity.subject === input.context.humanPrincipal.subject
          && digest(session.rootPrincipal) === digest(input.context.rootPrincipal)) candidates.push(session);
      }
      candidates.sort((a, b) => b.createdAt - a.createdAt || a.idHash.localeCompare(b.idHash));
      return candidates.length ? { idHash: candidates[0].idHash } : null;
    },
    async rebindSession(input, final) {
      const result = realm.store.tx(() => {
        const row = realm.store.db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(input.idHash);
        if (!row || row.active !== 1 || row.expires_at <= Date.now()) return false;
        const session = realm.store.decrypt('Session', row.id_hash, 0, row.cipher, row.key_id);
        if (!session.rpMarker || session.rootReferenceDigest !== input.expectedReference) return false;
        final(); const cipher = realm.store.encrypt('Session', row.id_hash, 0,
          { ...session, rootReferenceDigest: digest(input.context.reference) });
        const changed = realm.store.db.prepare('UPDATE source_sessions SET cipher=? WHERE id_hash=? AND cipher=? AND active=1')
          .run(cipher, row.id_hash, row.cipher).changes === 1;
        if (changed) realm.rebinds++; return changed;
      });
      if (realm.loseRebindAck) { realm.loseRebindAck = false; throw new Error('fixture_ack_lost'); }
      return result;
    },
  };
  if (realm.omitResume) delete realm.storage.findRenewableSession;
  return { storage: realm.storage,
    rp: { issuer: root.issuer, clientId: client.id, clientSecret: client.clientSecret,
      redirectUri: client.redirectUri, renewalProfile: SOURCE_RP_PROFILE },
    rpSessions: {
      async prepareSession(basic, identity) {
        const marker = { sessionIdHash: basic.idHash, profileDigest: realm.profile.digest, bindingDigest: semanticDigest,
          issuer: identity.issuer, subject: identity.subject, createdAt: basic.createdAt, sessionExpiresAt: basic.createdAt + 86400000 };
        await realm.rpNative.seed(marker, { accessToken: identity.accessToken, refreshToken: identity.refreshToken, nonce: identity.nonce }, identity.expiresAt);
        return { ...basic, expiresAt: marker.sessionExpiresAt, rpMarker: marker };
      },
      async currentProof(marker, options, frame) {
        await frame.assertRootCurrent(); await realm.beforeProof?.();
        const proof = await service.currentProof(marker, options); await frame.assertRootCurrent(); return proof;
      },
    } };
}

function reopen(realm, actor) {
  realm.context = { ...realm.context, reference: { id: random(), version: 1, digest: digest(random()) },
    rootPrincipal: { accountId: actor.account.accountId, deviceId: actor.account.deviceId },
    humanPrincipal: { ...realm.context.humanPrincipal, subject: actor.account.accountId }, expiresAt: Date.now() + 300000 };
  realm.embedJar.clear();
}
async function login(realm, actor, long = true) {
  const start = await realm.beginNative(), authorize = await realm.authorizeNative(start);
  const finished = await realm.completeOidc(authorize, actor, long ? 86400 : 0); await realm.finishNative(finished.callback);
  return realm.embedJar.get('soty_rp_session');
}
const continued = (realm, id = 'resume-request-0001') => realm.request('/api/embed/session-continue', { method: 'POST', data: { requestId: id } });
function noCredential(body, secrets) {
  const encoded = JSON.stringify(body); for (const secret of secrets) assert.equal(encoded.includes(secret), false);
  assert.deepEqual(Object.keys(body).sort(), ['accessExpiresAt', 'ready', 'receiptDigest', 'renewable', 'schema', 'sessionExpiresAt']);
}

test('two Source2 apps/two Root actors: actual HTTP >300s fresh-slot resume, encrypted restart and lost ACK keep one finite session', { timeout: 60000 }, async t => {
  const advance = controlledClock(t);
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true, renewal: true, asyncNativeAuthority: true,
    configureBff: (realm, host) => renewableHost(t, realm, host) });
  const actors = [f.root.actor, f.root.outsider], records = [];
  for (const realm of f.realms) {
    for (const [index, actor] of actors.entries()) {
      reopen(realm, actor);
      if (index) realm.nativeToken = realm.store.createNativeSession('native-owner');
      f.nativeJar.set('ordinary_native_' + realm.realmId, realm.nativeToken);
      const token = await login(realm, actor), session = await realm.storage.readSession(digest(token));
      records.push({ realm, actor, token, session });
    }
  }
  advance(310); await f.root.restart(); f.realms.forEach(realm => realm.restart());
  for (const { realm, actor, token, session } of records) {
    reopen(realm, actor);
    assert.equal((await realm.request('/api/embed/context')).status, 401, 'ordinary reads do not perform resume lookup');
    const result = await continued(realm); assert.equal(result.status, 200, result.value?.error?.code);
    assert.equal(realm.embedJar.get('soty_rp_session'), token);
    noCredential(result.value, [token, session.identity.subject, session.accessToken]);
    assert.equal(result.value.renewable, true); assert.equal(result.value.sessionExpiresAt, realm.context.expiresAt);
    assert.equal((await realm.storage.readSession(digest(token))).expiresAt, session.expiresAt);
    assert.equal(realm.rpNative.head(digest(token)).revision, 1);
    assert.equal((await realm.request('/api/embed/context')).status, 200);
  }
  const { realm, actor, token } = records[0], sends = realm.refreshCalls;
  reopen(realm, actor); realm.loseRebindAck = true;
  assert.equal((await continued(realm, 'lost-rebind-0001')).status, 503);
  assert.equal(realm.embedJar.has('soty_rp_session'), false);
  const rebinds = realm.rebinds, recovered = await continued(realm, 'lost-rebind-0001'); assert.equal(recovered.status, 200);
  assert.equal(realm.rebinds, rebinds); assert.equal(realm.refreshCalls, sends);
  reopen(realm, actor); realm.dropPath = '/api/embed/session-continue';
  await assert.rejects(continued(realm, 'lost-http-ack-0001'));
  assert.equal(realm.embedJar.has('soty_rp_session'), false);
  const expected = digest({ requestId: 'lost-http-ack-0001', sessionIdHash: digest(token), context: realm.context.reference });
  assert.equal((await continued(realm, 'lost-http-ack-0001')).value.receiptDigest, expected);
  assert.equal(realm.refreshCalls, sends);
  advance(86400); reopen(realm, actor); assert.equal((await continued(realm)).status, 401);
});

test('resume is optional and fail-closed for Basic, forged/cross-actor locators, invalid cookies, revoke and Root change across await', { timeout: 60000 }, async t => {
  controlledClock(t);
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true, renewal: true,
    configureBff: (realm, host) => renewableHost(t, realm, host) });
  const [realm, other] = f.realms;
  const basic = await login(other, f.root.actor, false); reopen(other, f.root.actor);
  other.lookupOverride = () => ({ idHash: digest(basic) }); assert.equal((await continued(other)).status, 401);
  const token = await login(realm, f.root.actor), idHash = digest(token);
  realm.omitResume = true; realm.restart(); reopen(realm, f.root.actor);
  assert.equal((await continued(realm)).status, 401, 'omitted host hook preserves cookie-required behavior');
  realm.omitResume = false; realm.restart();
  const prior = realm.store.db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(idHash);
  const payload = realm.store.decrypt('Session', idHash, 0, prior.cipher, prior.key_id);
  for (const change of [{ semanticDigest: digest('different-app-namespace') },
    { rootPrincipal: { ...payload.rootPrincipal, deviceId: 'different-device' } }]) {
    realm.store.db.prepare('UPDATE source_sessions SET cipher=? WHERE id_hash=?')
      .run(realm.store.encrypt('Session', idHash, 0, { ...payload, ...change }), idHash);
    realm.lookupOverride = () => ({ idHash });
    assert.equal((await continued(realm)).status, 401); assert.equal(realm.embedJar.size, 0);
  }
  realm.store.db.prepare('UPDATE source_sessions SET cipher=? WHERE id_hash=?').run(prior.cipher, idHash);
  reopen(realm, f.root.outsider); realm.lookupOverride = () => ({ idHash });
  assert.equal((await continued(realm)).status, 401); assert.equal(realm.embedJar.size, 0);
  reopen(realm, f.root.actor); realm.lookupOverride = () => ({ idHash: digest(basic) });
  assert.equal((await continued(realm)).status, 401);
  realm.lookupOverride = null; realm.embedJar.set('soty_rp_session', random()); const lookups = realm.lookups;
  assert.equal((await continued(realm)).status, 401); assert.equal(realm.lookups, lookups, 'present invalid cookie never falls back to a different session');
  realm.embedJar.clear(); realm.beforeLookup = () => { realm.rootLive = false; };
  assert.equal((await continued(realm)).value.error.code, 'scoped_embed_authority_denied'); assert.equal(realm.embedJar.size, 0);
  realm.beforeLookup = null; realm.rootLive = true;
  realm.beforeLookup = () => { reopen(realm, f.root.outsider); };
  assert.equal((await continued(realm)).value.error.code, 'scoped_embed_authority_mismatch'); assert.equal(realm.embedJar.size, 0);
  realm.beforeLookup = null; reopen(realm, f.root.actor);
  realm.beforeProof = () => { realm.store.revokeNativeSession(realm.nativeToken); };
  assert.equal((await continued(realm)).value.error.code, 'source_app_commit_unknown'); assert.equal(realm.embedJar.size, 0);
  realm.beforeProof = null;
  const row = other.store.db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(digest(basic));
  const original = other.store.decrypt('Session', row.id_hash, 0, row.cipher, row.key_id);
  other.store.db.prepare('UPDATE source_sessions SET cipher=? WHERE id_hash=?').run(other.store.encrypt('Session', row.id_hash, 0,
    { ...original, rpMarker: {}, cookieToken: random() }), row.id_hash);
  assert.equal((await continued(other)).status, 401, 'sealed cookie hash must match canonical session id');
});

test('fresh Source resume after actual signed Root device revoke delivers no cookie or renewed authority', { timeout: 30000 }, async t => {
  controlledClock(t);
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true, renewal: true,
    configureBff: (realm, host) => renewableHost(t, realm, host) });
  const [realm] = f.realms; await login(realm, f.root.actor);
  const backup = await f.root.backupDevice(), original = await f.root.actor.client.getLocalState();
  await backup.client.revokeDevice(original.deviceId);
  reopen(realm, f.root.actor); const result = await continued(realm);
  assert.equal(result.status, 401); assert.equal(realm.embedJar.size, 0); assert.equal(realm.refreshCalls, 0);
});

test('Native logout during the last Root HTTP authority await denies resumed readiness and cookie', { timeout: 30000 }, async t => {
  controlledClock(t);
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true, renewal: true, asyncNativeAuthority: true,
    configureBff: (realm, host) => renewableHost(t, realm, host) });
  const [realm] = f.realms, token = await login(realm, f.root.actor), idHash = digest(token);
  realm.embedJar.clear();
  let checks = 0, revokeOnRootRead = false, revokedDuringRootRead = false;
  // No reference change or rebind: capture/assert twice in proveSession, then
  // the fifth async Native check precedes the cookie branch's last Root read.
  realm.beforeAsyncCheck = () => { if (++checks === 5) revokeOnRootRead = true; };
  const duringRootRead = () => {
    if (!revokeOnRootRead || revokedDuringRootRead) return;
    assert.equal(realm.store.inTransaction(), false);
    realm.store.revokeNativeSession(realm.nativeToken); revokedDuringRootRead = true;
  };
  realm.ipc.prependListener('request', duringRootRead);
  try {
    const result = await continued(realm, 'resume-native-last-await-0001');
    assert.equal(revokedDuringRootRead, true, 'revoke happens while the final Root HTTP response is pending');
    assert.equal(result.status, 403);
    assert.equal(result.value.error.code, 'ordinary_native_access_denied');
    assert.equal(realm.embedJar.size, 0, 'no resumed cookie after Native logout');
    assert.equal(realm.refreshCalls, 0); assert.equal(realm.rebinds, 0);
    assert.equal(realm.rpNative.head(idHash).revision, 0);
    assert.equal(realm.store.db.prepare('SELECT active FROM native_sessions WHERE id_hash=?').get(digest(realm.nativeToken)).active, 0);
  } finally { realm.beforeAsyncCheck = null; realm.ipc.off('request', duringRootRead); }
});
