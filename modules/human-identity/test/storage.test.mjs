import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createServer } from 'node:http';
import { randomBytes, generateKeyPairSync } from 'node:crypto';
import { mkdtempSync, rmSync, existsSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHumanIdentityHostProfile, digest } from '../profile.mjs';
import { createHumanIdentityService, HUMAN_IDENTITY_RUNTIME } from '../service.mjs';
import { attachHumanIdentity } from '../../../server/human-identity.js';

const random = () => randomBytes(32).toString('base64url');
const privateJwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
Object.assign(privateJwk, { kid: 'isolated-storage-key', alg: 'RS256', use: 'sig' });
const origin = 'http://127.0.0.1:19191';
function options() {
  return { enabled: true, issuer: origin + '/human-identity', registryId: 'REG.soty', environmentId: 'fixture',
    clients: [{ id: 'approved-client', label: 'Approved RP', redirectUri: 'http://127.0.0.1:19292/oidc/callback', clientSecret: random() }],
    jwks: { keys: [privateJwk] }, cookieKeys: [random()], artifactKey: randomBytes(32), artifactKeyId: 'fixture-key' };
}
function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-human-store-')), databasePath = join(directory, 'human-identity', 'identity.sqlite'), services = [];
  t.after(() => {
    services.forEach(value => value.close()); assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-human-store-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  });
  const config = options(), open = (change = {}) => {
    const profile = createHumanIdentityHostProfile(change.options || config, { shellOrigins: [origin] });
    const service = createHumanIdentityService({ databasePath, profile, actorActive: () => true, withAuthorityFence: callback => callback(), ...change });
    services.push(service); return service;
  };
  return { directory, databasePath, config, open };
}
const throws = (fn, code) => assert.throws(fn, error => error.code === code);
test('subject projection requires its private actor fence and permanently rejects late, repeated or async callbacks', t => {
  const f = fixture(t), actor = Object.freeze({ accountId: 'synthetic-account', deviceId: 'synthetic-device' });
  const request = { actor, issuer: f.config.issuer, clientId: 'approved-client' }; let late;
  const missing = f.open(); throws(() => missing.withSubjectAuthority(request, () => assert.fail()), 'human_identity_subject_authority_required');
  const deferred = f.open({ withSubjectAuthorityFence: (actual, callback) => { assert.equal(actual, actor); late = callback; } });
  throws(() => deferred.withSubjectAuthority(request, () => assert.fail()), 'human_identity_authority_fence_invalid');
  throws(() => late(), 'human_identity_authority_fence_invalid');
  const repeated = f.open({ withSubjectAuthorityFence: (actual, callback) => {
    assert.equal(actual, actor); callback(); return callback();
  } });
  let calls = 0;
  throws(() => repeated.withSubjectAuthority(request, () => { calls++; }), 'human_identity_authority_fence_invalid');
  assert.equal(calls, 1);
  const valid = f.open({ withSubjectAuthorityFence: (actual, callback) => { assert.equal(actual, actor); return callback(); } });
  const result = valid.withSubjectAuthority(request, subject => ({ issuer: subject.issuer, subject: subject.subject, generation: subject.clientGeneration }));
  assert.equal(result.subject, actor.accountId); assert.equal(result.issuer, f.config.issuer); assert.equal(result.generation, 1);
  throws(() => valid.withSubjectAuthority(request, () => Promise.resolve('not synchronous')), 'human_identity_async_boundary');
  const revoked = f.open({ actorActive: () => false, withSubjectAuthorityFence: (_actor, callback) => callback() });
  throws(() => revoked.withSubjectAuthority(request, () => assert.fail()), 'human_identity_actor_revoked');
});
test('human issuer stays reserved and disabled without approved configuration, and never creates a store or development signing key', async t => {
  const f = fixture(t); assert.equal(createHumanIdentityHostProfile(undefined), null);
  const disabled = createHumanIdentityHostProfile({ enabled: false, issuer: origin + '/human-identity', registryId: 'REG.soty', environmentId: 'fixture' }, { shellOrigins: [origin] });
  assert.equal(disabled.enabled, false); assert.equal('providerKeys' in disabled, false);
  throws(() => createHumanIdentityService({ databasePath: f.databasePath, profile: disabled }), 'human_identity_disabled');
  assert.equal(existsSync(f.databasePath), false);
  const app = express(); attachHumanIdentity(app, { profile: disabled }); const server = createServer(app);
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  t.after(async () => { server.closeAllConnections(); await new Promise(done => server.close(done)); });
  for (const path of ['/human-identity/authorize', '/Human-Identity/authorize', '/human%2Didentity/authorize', '/%68uman-identity/authorize']) {
    const result = await fetch(`http://127.0.0.1:${server.address().port}${path}`); assert.equal(result.status, 503);
  }
  const missing = options(); delete missing.jwks;
  assert.throws(() => createHumanIdentityHostProfile(missing, { shellOrigins: [origin] })); assert.equal(existsSync(f.databasePath), false);
});

test('approved host profile rejects control/ill-formed labels, mutable accessors, unknown fields and weak/missing key inputs without echoing material', () => {
  for (const label of [' ', 'a'.repeat(121), 'label\nnew', 'label\u0000', '\ud800']) {
    const value = options(); value.clients[0].label = label; assert.throws(() => createHumanIdentityHostProfile(value, { shellOrigins: [origin] }));
  }
  let calls = 0; const value = options(); Object.defineProperty(value, 'cookieKeys', { enumerable: true, get() { calls++; return [random()]; } });
  assert.throws(() => createHumanIdentityHostProfile(value, { shellOrigins: [origin] })); assert.equal(calls, 0);
  const future = options(); future.client_registration = true;
  throws(() => createHumanIdentityHostProfile(future, { shellOrigins: [origin] }), 'human_identity_configuration_invalid');
  const weak = options(); weak.artifactKey = new Uint8Array(8);
  throws(() => createHumanIdentityHostProfile(weak, { shellOrigins: [origin] }), 'human_identity_configuration_invalid');
  const display = createHumanIdentityHostProfile(options(), { shellOrigins: [origin] });
  assert.equal(JSON.stringify(display.publicClients).includes('clientSecret'), false);
});

test('encrypted SDK artifacts bind model/id/issuer/key and cannot be copied to another identity or silently opened with a replaced key', t => {
  const f = fixture(t), service = f.open(), first = random(), second = random(), secret = 'private-synthetic-nonce';
  for (const rawId of [first, second]) service.sdk.upsert({ model: 'Interaction', id: rawId,
    payload: { jti: rawId, kind: 'Interaction', uid: rawId, params: { client_id: 'approved-client', marker: secret } }, expiresIn: 300, browserNonce: random() });
  assert.equal(service.sdk.find('Interaction', first).params.marker, secret);
  const db = new DatabaseSync(f.databasePath);
  try {
    const one = digest('Interaction\0' + first), two = digest('Interaction\0' + second);
    const row = db.prepare('SELECT payload_cipher FROM human_identity_artifacts WHERE id_hash=?').get(one);
    assert.equal(Buffer.from(row.payload_cipher).includes(Buffer.from(secret)), false);
    db.prepare('UPDATE human_identity_artifacts SET payload_cipher=? WHERE id_hash=?').run(row.payload_cipher, two);
  } finally { db.close(); }
  throws(() => service.sdk.find('Interaction', second), 'human_identity_storage_corrupt'); service.close();
  const changed = { ...f.config, artifactKey: randomBytes(32) }, other = f.open({ options: changed });
  throws(() => other.sdk.find('Interaction', first), 'human_identity_storage_corrupt'); other.close();
  const keyId = f.open({ options: { ...f.config, artifactKeyId: 'different-key' } });
  throws(() => keyId.sdk.find('Interaction', first), 'human_identity_storage_key_unavailable');
});

test('real SQLite FULL maps to bounded safe refusal; persisted oversized data is readable without eviction or a new allocation', t => {
  const f = fixture(t), service = f.open({ maxDatabaseBytes: 256 * 1024 }); let last, full = false;
  for (let index = 0; index < 80; index++) {
    const rawId = random();
    try { service.sdk.upsert({ model: 'Interaction', id: rawId,
      payload: { jti: rawId, kind: 'Interaction', uid: rawId, params: { client_id: 'approved-client', marker: 'x'.repeat(12000) } }, expiresIn: 300, browserNonce: random() }); last = rawId; }
    catch (error) { assert.equal(error.code, 'human_identity_storage_full'); assert.equal(error.status, 503); full = true; break; }
  }
  assert.equal(full, true); assert.ok(last); assert.equal(service.sdk.find('Interaction', last).params.marker.length, 12000);
  service.close(); const small = f.open({ maxDatabaseBytes: 128 * 1024 });
  assert.equal(small.sdk.find('Interaction', last).params.marker.length, 12000);
  throws(() => small.sdk.upsert({ model: 'Interaction', id: random(), payload: { kind: 'Interaction', uid: random(), params: { client_id: 'approved-client' } }, expiresIn: 300, browserNonce: random() }), 'human_identity_storage_full');
});

test('unknown schema/issuer metadata and missing guards are refused before serving, without repairing stored history', t => {
  for (const damage of ['version', 'issuer', 'guard']) {
    const f = fixture(t), service = f.open(); service.close(); const changed = f.open(); changed.close();
    const db = new DatabaseSync(f.databasePath);
    try {
      if (damage === 'version') db.exec('PRAGMA user_version=99');
      else if (damage === 'guard') db.exec('DROP TRIGGER human_identity_client_head_monotonic');
      else {
        const guards = db.prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name='human_identity_meta'").all();
        for (const guard of guards) db.exec(`DROP TRIGGER ${guard.name}`);
        db.prepare("UPDATE human_identity_meta SET value='https://wrong.example/human-identity' WHERE key='issuer'").run();
        for (const guard of guards) db.exec(guard.sql);
      }
    } finally { db.close(); }
    const before = digest(readFileSync(f.databasePath));
    throws(() => f.open(), damage === 'issuer' ? 'human_identity_storage_identity_mismatch' : 'human_identity_storage_unknown');
    assert.equal(digest(readFileSync(f.databasePath)), before);
  }
});

const loginParams = (clientId = 'approved-client') => ({ client_id: clientId, redirect_uri: `http://127.0.0.1:${clientId === 'other-client' ? '19393' : '19292'}/oidc/callback`,
  response_type: 'code', scope: 'openid', state: random(), nonce: random(), code_challenge_method: 'S256', code_challenge: random() });
function capture(service, { browser = random(), clientId = 'approved-client', ttl = 5 } = {}) {
  const interactionId = random(), parameters = loginParams(clientId);
  service.sdk.upsert({ model: 'Interaction', id: interactionId, payload: { jti: interactionId, kind: 'Interaction', params: parameters }, expiresIn: ttl, browserNonce: browser });
  return service.prepareInteraction({ interactionId, browserNonce: browser, parameters });
}
function decide(service, context, requestId = 'request.' + random(), decision = 'approve') {
  return service.execute({ op: 'identity.human.approve', actor: { accountId: 'account.fixture', deviceId: 'device.fixture' },
    args: { expectedAccountId: 'account.fixture', interactionId: context.interactionId, browserNonce: context.browserNonce, csrf: context.csrf, requestId, decision } });
}
function totals(path) {
  const db = new DatabaseSync(path);
  try { return Object.fromEntries(['artifacts', 'interactions', 'decisions', 'grant_bindings', 'client_versions', 'client_heads']
    .map(name => [name, db.prepare(`SELECT count(*) AS n FROM human_identity_${name}`).get().n])); } finally { db.close(); }
}

test('anonymous pending quotas isolate browsers and clients, exact current context still replays, and expiry restores capacity without reviving an old UID', t => {
  const f = fixture(t); f.config.clients.push({ ...f.config.clients[0], id: 'other-client', redirectUri: 'http://127.0.0.1:19393/oidc/callback' });
  let clock = Date.now(); const service = f.open({ now: () => clock }), browser = random(), first = capture(service, { browser });
  for (let i = 1; i < HUMAN_IDENTITY_RUNTIME.pendingPerBrowser; i++) capture(service, { browser });
  throws(() => capture(service, { browser }), 'human_identity_pending_browser_capacity');
  assert.deepEqual(service.prepareInteraction({ interactionId: first.interactionId, browserNonce: first.browserNonce,
    parameters: service.sdk.find('Interaction', first.interactionId).params }), first);
  for (let i = HUMAN_IDENTITY_RUNTIME.pendingPerBrowser; i < HUMAN_IDENTITY_RUNTIME.pendingPerClient; i++) capture(service);
  throws(() => capture(service), 'human_identity_pending_client_capacity');
  assert.equal(capture(service, { clientId: 'other-client' }).client.id, 'other-client');
  const before = totals(f.databasePath); clock += 6000;
  const compacted = service.compactExpiredRuntime(); assert.equal(compacted.artifacts, before.artifacts); assert.equal(compacted.interactions, before.interactions);
  assert.equal(totals(f.databasePath).interactions, 0); assert.equal(totals(f.databasePath).client_versions, 2);
  throws(() => service.prepareInteraction({ interactionId: first.interactionId, browserNonce: first.browserNonce, parameters: loginParams() }), 'human_identity_interaction_expired');
  assert.equal(capture(service, { browser }).decision, 'pending');
});

test('decided ACKs are immutable during their live window and retained until every dependent SDK proof expires; finite compaction keeps client history', t => {
  const f = fixture(t); let clock = Date.now(); const service = f.open({ now: () => clock }), context = capture(service), requestId = 'request.' + random();
  const receipt = decide(service, context, requestId), approval = service.readApprovedInteraction(context), grant = random();
  service.sdk.upsert({ model: 'Grant', id: grant, payload: { jti: grant, kind: 'Grant', clientId: approval.clientId, accountId: approval.accountId }, expiresIn: 3600, approvedBinding: approval });
  assert.deepEqual(decide(service, context, requestId), receipt); assert.equal(service.compactExpiredRuntime().decisions, 0);
  const db = new DatabaseSync(f.databasePath);
  try { assert.throws(() => db.exec('DELETE FROM human_identity_decisions')); assert.throws(() => db.exec('DELETE FROM human_identity_client_versions')); } finally { db.close(); }
  clock += 200000; const session = random();
  service.sdk.upsert({ model: 'Session', id: session, payload: { jti: session, kind: 'Session', uid: random(), accountId: approval.accountId,
    authorizations: { [approval.clientId]: { grantId: grant } } }, expiresIn: 3600, clientId: approval.clientId });
  throws(() => decide(service, context, requestId), 'human_identity_interaction_expired');
  clock += 3470000; assert.equal(service.compactExpiredRuntime().decisions, 0); assert.equal(totals(f.databasePath).grant_bindings, 1);
  assert.ok(service.sdk.find('Session', session)); clock += 131000;
  const cleared = service.compactExpiredRuntime(); assert.equal(cleared.decisions, 1); assert.equal(cleared.bindings, 1); assert.equal(cleared.interactions, 1);
  assert.deepEqual(totals(f.databasePath), { artifacts: 0, interactions: 0, decisions: 0, grant_bindings: 0, client_versions: 1, client_heads: 1 });
  throws(() => decide(service, context, requestId), 'human_identity_interaction_expired');
  throws(() => service.prepareInteraction({ interactionId: context.interactionId, browserNonce: context.browserNonce, parameters: loginParams() }), 'human_identity_interaction_expired');
});

test('bounded expiry batches reclaim a saturated anonymous artifact pool, never remove an active proof, and permit new admission', t => {
  const f = fixture(t); let clock = Date.now(); const service = f.open({ now: () => clock }), old = capture(service), db = new DatabaseSync(f.databasePath);
  try {
    const row = db.prepare('SELECT * FROM human_identity_artifacts LIMIT 1').get();
    const insert = db.prepare('INSERT INTO human_identity_artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'); db.exec('BEGIN');
    for (let i = 1; i < 8192; i++) insert.run(row.model, digest('expired' + i), row.payload_cipher, row.payload_digest, row.key_id,
      null, null, row.client_id, null, null, row.browser_hash, row.expires_at, null, row.created_at);
    db.exec('COMMIT');
  } finally { db.close(); }
  clock += 6000; const active = capture(service), changed = service.compactExpiredRuntime();
  assert.ok(changed.artifacts > 0 && changed.artifacts <= HUMAN_IDENTITY_RUNTIME.compactionBatch);
  assert.ok(totals(f.databasePath).artifacts < 8192); assert.ok(service.sdk.find('Interaction', active.interactionId)); assert.equal(service.sdk.find('Interaction', old.interactionId), undefined);
});

test('fresh decision IDs cannot flood one live interaction; exact lost-ACK replay remains valid at the local limit', t => {
  const f = fixture(t), service = f.open(), context = capture(service, { ttl: 300 }), firstId = 'request.' + random(), receipt = decide(service, context, firstId);
  for (let i = 1; i < HUMAN_IDENTITY_RUNTIME.decisionsPerInteraction; i++) decide(service, context);
  throws(() => decide(service, context), 'human_identity_decision_capacity'); assert.deepEqual(decide(service, context, firstId), receipt);
  assert.equal(totals(f.databasePath).decisions, HUMAN_IDENTITY_RUNTIME.decisionsPerInteraction); assert.equal(service.compactExpiredRuntime().decisions, 0);
});
