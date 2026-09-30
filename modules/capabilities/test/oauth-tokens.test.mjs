import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { Provider } from 'oidc-provider';
import { tokensFixture, opaque, sha, CLIENT, ORIGIN, raceWriters } from './support/oauth-tokens.mjs';
import { code, good } from './support/oauth-connections.mjs';
import { insertInvocation } from './support/oauth-baseline.mjs';
import { config } from './support/oauth-artifacts.mjs';
import { createOAuthTokenStore } from '../server/oauth-tokens.mjs';

const models = ['AuthorizationCode', 'RefreshToken', 'AccessToken'];
const counts = db => ['cap_oauth_artifacts', 'cap_credentials', 'cap_oauth_credentials']
  .map(table => db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n);

test('bound tokens persist encrypted with exact non-aligned expiry and immutable AT credential/link', async t => {
  const f = await tokensFixture(t), payloads = models.map(model => f.payload(model));
  for (const payload of payloads) {
    f.put(payload); assert.deepEqual(f.find(payload), payload);
    const row = f.artifact(payload);
    assert.equal(row.created_at, f.now()); assert.notEqual(row.created_at, payload.iat * 1000);
    assert.equal(row.expires_at, payload.exp * 1000);
    assert.equal(row.retain_until, payload.kind === 'AccessToken' ? row.expires_at : f.primary.expiresAt);
    assert.equal(Buffer.from(row.payload_cipher).includes(Buffer.from(payload.jti)), false);
  }
  const at = payloads[2], original = { ...f.link(at) }, before = counts(f.db);
  f.put(at); assert.deepEqual(counts(f.db), before); assert.deepEqual({ ...f.link(at) }, original);
  assert.throws(() => f.put({ ...at, exp: at.exp - 1 }), code('oauth_invalid_artifact'));
  assert.deepEqual({ ...f.link(at) }, original);
  const borrowed = f.find(payloads[0]); borrowed.redirectUri = 'https://attacker.invalid/';
  assert.deepEqual(f.find(payloads[0]), payloads[0]);
  f.reopen();
  for (const payload of payloads) assert.deepEqual(f.find(payload), payload);
  for (const filename of [f.files.caps, f.files.caps + '-wal']) {
    let bytes; try { bytes = readFileSync(filename); } catch (error) { if (error.code === 'ENOENT') continue; throw error; }
    for (const payload of payloads) assert.equal(bytes.includes(Buffer.from(payload.jti)), false);
  }
  assert.deepEqual(f.oauth.readiness(), { schemaVersion: 3, available: false });
  assert.throws(() => f.oauth.authenticateBearer({ token: at.jti, audience: f.primary.resource }), code('oauth_unavailable'));
});

test('actual pinned Provider models serialize, save, find and consume through the encrypted token port', async t => {
  const f = await tokensFixture(t); t.mock.timers.enable({ apis: ['Date'], now: f.now() });
  const observed = []; let request;
  class Adapter {
    constructor(model) { this.model = model; }
    async upsert(id, payload, expiresIn) {
      observed.push({ model: this.model, id, payload: structuredClone(payload), expiresIn });
      f.oauth.artifactStore.upsert({ model: this.model, id, payload, expiresIn, request });
    }
    async find(id) { return f.oauth.artifactStore.find({ model: this.model, id }); }
    async consume(id) { const result = f.oauth.artifactStore.consume({ model: this.model, id, request }); assert.equal(result.status, 'consumed'); }
  }
  const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(key, { kid: 'fixture', use: 'sig', alg: 'RS256' });
  const provider = new Provider(ORIGIN + '/oauth', { adapter: Adapter,
    clients: [{ client_id: CLIENT, application_type: 'native', token_endpoint_auth_method: 'none',
      redirect_uris: ['http://127.0.0.1:19876/callback'], grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] }],
    jwks: { keys: [key] }, cookies: { keys: ['fixture-only-cookie-key'] }, responseTypes: ['code'], scopes: [], claims: {},
    expiresWithSession: () => false, issueRefreshToken: () => true, features: { devInteractions: { enabled: false },
      registration: { enabled: false }, clientIdMetadataDocument: { enabled: false }, userinfo: { enabled: false } },
    ttl: { AuthorizationCode: 60, AccessToken: 300, RefreshToken: () => f.primary.grant.exp - Math.floor(Date.now() / 1000) } });
  const client = await provider.Client.find(CLIENT);
  for (const model of models) {
    const sample = f.payload(model); delete sample.iat; delete sample.exp; delete sample.jti; delete sample.kind;
    const token = new provider[model]({ client, ...sample }); request = f.request(model);
    const value = await token.save(); assert.match(value, /^[A-Za-z0-9_-]{43}$/u);
    const row = observed.find(item => item.model === model);
    assert.equal(row.id, value); assert.equal(row.payload.kind, model);
    assert.equal(row.payload.exp * 1000, f.artifact({ kind: model, jti: value }).expires_at);
    const found = await provider[model].find(value); assert.ok(found); assert.equal(found.accountId, f.owner.accountId);
    if (model !== 'AccessToken') {
      request = f.request(model, f.primary, true); await found.consume();
      assert.ok((await provider[model].find(value)).consumed);
    } else assert.ok(row.payload.extra === undefined || Object.keys(row.payload.extra).length === 0);
  }
  assert.equal(observed.length, 3); assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_credentials').get().n, 1);
  // This is real library model serialization with a synthetic closed request
  // snapshot. HTTP client authentication/Provider.ctx belongs to host tests.
});

test('fresh request binding rejects wrong resource before consume and permits the subsequent correct request', async t => {
  const f = await tokensFixture(t);
  for (const model of ['AuthorizationCode', 'RefreshToken']) {
    const payload = f.payload(model); f.put(payload);
    const exact = f.request(model, f.primary, true), original = { ...f.artifact(payload) };
    for (const [patch, status] of [
      [{ resource: ORIGIN }, 'invalid_target'], [{ clientId: 'soty-opencode-cli' }, 'invalid_grant'],
      [{ scope: 'openid' }, 'invalid_grant'], [{ grantType: 'client_credentials' }, 'invalid_grant'],
    ]) {
      assert.deepEqual(f.consume(payload, { ...exact, ...patch }), { status });
      assert.deepEqual({ ...f.artifact(payload) }, original);
      assert.equal(f.db.prepare('SELECT state FROM cap_oauth_connections WHERE id=?').get(f.primary.id).state, 'active');
    }
    assert.deepEqual(f.consume(payload, { clientId: CLIENT, grantType: exact.grantType }), { status: 'consumed' });
    assert.ok(f.find(payload).consumed);
  }
});

test('token profile rejects unsupported claims, temporal widening and untrusted request shapes without writes', async t => {
  const f = await tokensFixture(t), before = counts(f.db);
  const bad = [
    ['AccessToken', { exp: Math.floor(f.now() / 1000) + 301 }],
    ['AccessToken', { aud: [f.primary.resource] }], ['AccessToken', { extra: { secret: 'forbidden' } }],
    ['AccessToken', { expiresWithSession: true }], ['AccessToken', { claims: {} }],
    ['AccessToken', { iat: Math.floor(f.now() / 1000) + 1 }], ['AccessToken', { exp: undefined }],
    ['AuthorizationCode', { codeChallengeMethod: 'plain' }], ['AuthorizationCode', { codeChallenge: 'short' }],
    ['AuthorizationCode', { redirectUri: 'http://127.0.0.1:19876/other' }],
    ['AuthorizationCode', { exp: Math.floor(f.now() / 1000) + 61 }],
    ['RefreshToken', { exp: f.primary.grant.exp + 1 }], ['RefreshToken', { rotations: -1 }],
    ['RefreshToken', { iiat: Math.floor(f.now() / 1000) + 1 }], ['RefreshToken', { consumed: 1 }],
    ['RefreshToken', { gty: 'client_credentials' }], ['RefreshToken', { accountId: 'foreign' }],
  ];
  for (const [model, patch] of bad) assert.throws(() => f.put(f.payload(model, f.primary, patch)), code('oauth_invalid_artifact'));
  const at = f.payload('AccessToken');
  for (const request of [undefined, {}, { clientId: CLIENT, resource: [f.primary.resource] },
    { clientId: CLIENT, grantType: 'authorization_code', actor: { accountId: f.owner.accountId } }]) {
    assert.throws(() => f.oauth.artifactStore.upsert({ model: at.kind, id: at.jti, payload: at, request }), code('oauth_invalid_artifact'));
  }
  assert.throws(() => f.put(at, { ...f.request(at.kind), resource: ORIGIN }), code('oauth_invalid_target'));
  assert.throws(() => f.put(at, f.request(at.kind), { stagedGrant: {} }), code('oauth_context_invalid'));
  assert.throws(() => f.put(at, f.request(at.kind), { expiresIn: Infinity }), code('oauth_invalid_artifact'));
  assert.deepEqual(counts(f.db), before);
  f.put(at, f.request(at.kind), { expiresIn: undefined }); assert.ok(f.link(at));
});

test('AT artifact, credential and link roll back together; an outer post-COMMIT error preserves one exact issuance', async t => {
  const f = await tokensFixture(t), at = f.payload('AccessToken'), before = counts(f.db);
  f.db.exec("CREATE TRIGGER test_abort_oauth_link BEFORE INSERT ON cap_oauth_credentials BEGIN SELECT RAISE(ABORT,'injected'); END");
  try { assert.throws(() => f.put(at), code('capabilities_storage_corrupt')); assert.deepEqual(counts(f.db), before); }
  finally { f.db.exec('DROP TRIGGER test_abort_oauth_link'); }
  f.afterFence(() => { throw new Error('injected outer fence failure after commit'); });
  assert.throws(() => f.put(at), /injected outer fence failure/u);
  f.afterFence(null);
  const committed = { ...f.link(at) }; assert.ok(committed.credential_id);
  assert.deepEqual(counts(f.db), before.map(n => n + 1));
  f.reopen(); f.put(at); assert.deepEqual({ ...f.link(at) }, committed);
});

test('reuse commits family revoke, late issuance fails and an independent sibling remains live', async t => {
  const f = await tokensFixture(t), sibling = await f.family();
  const rt = f.payload('RefreshToken'), at = f.payload('AccessToken'), other = f.payload('RefreshToken', sibling);
  f.put(rt); f.put(at); f.put(other, f.request(other.kind, sibling));
  assert.deepEqual(f.consume(rt), { status: 'consumed' });
  f.afterFence(() => { throw new Error('lost invalid-grant response'); });
  assert.throws(() => f.consume(rt), /lost invalid-grant response/u); f.afterFence(null);
  assert.equal(f.db.prepare('SELECT state FROM cap_oauth_connections WHERE id=?').get(f.primary.id).state, 'revoked');
  assert.equal(f.link(at).revoked_at, f.now()); assert.equal(f.find(at), undefined);
  assert.throws(() => f.put(f.payload('AccessToken')), code('access_denied'));
  f.reopen(); assert.deepEqual(f.consume(rt), { status: 'invalid_grant' });
  assert.deepEqual(f.find(other), other);
  assert.deepEqual(f.consume(other, f.request(other.kind, sibling, true)), { status: 'consumed' });
});

test('expired/unknown sources do not consume; retained AT identity permits keyless family revoke after raw cleanup', async t => {
  const f = await tokensFixture(t), codeToken = f.payload('AuthorizationCode'), at = f.payload('AccessToken');
  f.put(codeToken); f.put(at); f.advance(60001);
  assert.deepEqual(f.consume(codeToken), { status: 'invalid_grant' }); assert.equal(f.artifact(codeToken).consumed_at, null);
  assert.deepEqual(f.consume({ kind: 'RefreshToken', jti: opaque() }), { status: 'invalid_grant' });
  assert.equal(f.db.prepare('SELECT state FROM cap_oauth_connections WHERE id=?').get(f.primary.id).state, 'active');
  // Simulate a valid cleanup seam: expired raw AT can disappear while its
  // immutable original credential link is retained for a later pass.
  f.advance(240001);
  for (let i = 0; i < 4 && f.artifact(at); i++) f.oauth.cleanup({ limit: 1 });
  assert.equal(f.artifact(at), undefined); assert.ok(f.link(at));
  f.reopen({ keyless: true });
  assert.throws(() => f.find(at), code('oauth_storage_key_unavailable'));
  f.oauth.artifactStore.destroy({ model: at.kind, id: at.jti });
  assert.equal(f.db.prepare('SELECT state FROM cap_oauth_connections WHERE id=?').get(f.primary.id).state, 'revoked');
  assert.equal(f.link(at).revoked_at, f.now());
});

test('two actual OS writers serialize RT consume and commit reuse revocation', async t => {
  const f = await tokensFixture(t), rt = f.payload('RefreshToken'); f.put(rt);
  const args = { model: rt.kind, id: rt.jti, request: f.request(rt.kind, f.primary, true) };
  const results = await raceWriters(t, f, [{ op: 'consume', args }, { op: 'consume', args }]);
  assert.deepEqual(results.map(item => item.status).sort(), ['consumed', 'invalid_grant']);
  assert.equal(f.db.prepare('SELECT state FROM cap_oauth_connections WHERE id=?').get(f.primary.id).state, 'revoked');
  assert.notEqual(f.artifact(rt).consumed_at, null); f.reopen();
  assert.throws(() => f.put(f.payload('AccessToken')), code('access_denied'));
});

test('two actual OS writers racing consume with revoke cannot leave a live family or admit a late AT', async t => {
  const f = await tokensFixture(t), rt = f.payload('RefreshToken'); f.put(rt);
  const results = await raceWriters(t, f, [
    { op: 'consume', args: { model: rt.kind, id: rt.jti, request: f.request(rt.kind, f.primary, true) } },
    { op: 'destroy', args: { model: rt.kind, id: rt.jti } },
  ]);
  assert.ok(results.some(item => item.status === 'destroyed'));
  assert.ok(results.some(item => ['consumed', 'invalid_grant'].includes(item.status)));
  assert.equal(f.db.prepare('SELECT state FROM cap_oauth_connections WHERE id=?').get(f.primary.id).state, 'revoked');
  f.reopen(); assert.throws(() => f.put(f.payload('AccessToken')), code('access_denied'));
});

test('account AT quota spans connections; retries, signed history and revoke remain possible at capacity', async t => {
  const f = await tokensFixture(t), sibling = await f.family(), tokens = [];
  for (let i = 0; i < 64; i++) {
    const own = i % 2 ? sibling : f.primary, at = f.payload('AccessToken', own);
    f.put(at, f.request(at.kind, own)); tokens.push(at);
  }
  // Capture the actual service's admission count, including parameters. The
  // temporary test tap is restored before any await or other test runs.
  const queries = [], prepare = DatabaseSync.prototype.prepare;
  DatabaseSync.prototype.prepare = function(sql) {
    const statement = prepare.call(this, sql);
    if (!sql.includes('count(*) AS n') || !sql.includes('cap_oauth_credentials l')) return statement;
    return { get(...args) { queries.push({ sql, args }); return statement.get(...args); } };
  };
  try { assert.throws(() => f.put(f.payload('AccessToken')), code('oauth_quota_exceeded')); }
  finally { DatabaseSync.prototype.prepare = prepare; }
  assert.equal(queries.length, 1);
  const quotaPlan = f.db.prepare('EXPLAIN QUERY PLAN ' + queries[0].sql).all(...queries[0].args).map(row => row.detail).join(' ');
  assert.match(quotaPlan, /SEARCH l USING COVERING INDEX cap_oauth_credentials_expiry \(expires_at>\?\)/u);
  assert.doesNotMatch(quotaPlan, /SCAN k|SCAN l|SCAN cap_credentials|TEMP B-TREE/u);
  const first = { ...f.link(tokens[0]) }; f.put(tokens[0]); assert.deepEqual({ ...f.link(tokens[0]) }, first);
  const list = good(await f.ownerCall('oauth.connections.list')); assert.equal(list.connections.length, 2);
  f.oauth.artifactStore.destroy({ model: 'AccessToken', id: tokens[0].jti });
  const next = f.payload('AccessToken', sibling); f.put(next, f.request(next.kind, sibling)); assert.ok(f.link(next));
  assert.equal(f.link(tokens[1]).revoked_at, null);
});

test('bounded cleanup advances past retained original references and reopens without dangling AT links', async t => {
  const f = await tokensFixture(t), retained = [], count = 64;
  const own = f.db.prepare('SELECT * FROM cap_oauth_connections WHERE id=?').get(f.primary.id);
  for (let i = 0; i < count; i++) {
    const at = f.payload('AccessToken'); f.put(at); retained.push(f.link(at).credential_id);
    // Explicit future reference rows test the existing persisted index/delete
    // invariant. T1 has no bearer admission and this is not a native effect.
    insertInvocation(f.db, { id: 'inv_token_reference_' + i, accountId: own.account_id, clientId: own.client_id,
      principalId: own.principal_id, grantId: own.root_grant_id, credentialId: retained[i] });
  }
  f.advance(300001);
  for (let i = 0; i < 4; i++) { const at = f.payload('AccessToken'); f.put(at); }
  f.advance(300001);
  let removed = 0;
  for (let i = 0; i < 12; i++) {
    const page = f.oauth.cleanup({ limit: 16 });
    assert.ok(Object.values(page).reduce((a, b) => a + b, 0) <= 16); removed += page.credentialsDeleted;
  }
  assert.equal(removed, 4);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_credentials').get().n, count);
  for (const credentialId of retained) assert.ok(f.db.prepare('SELECT 1 FROM cap_credentials WHERE id=?').get(credentialId));
  const referencePlan = f.db.prepare("EXPLAIN QUERY PLAN SELECT 1 FROM cap_invocations WHERE json_extract(authorization_json,'$.credentialId')=? LIMIT 1").all(retained[0]);
  assert.match(referencePlan.map(row => row.detail).join(' '), /cap_invocations_original_credential/u);
  // Capture the production cleanup's actual SQL/parameters, then explain that
  // statement on the same real DB. This tests range-seek work, not a copied
  // ideal query. Only this private cleanup call bypasses the service wrapper.
  const queries = [];
  const observedDb = new Proxy(f.db, { get(target, name) {
    if (name !== 'prepare') return Reflect.get(target, name, target);
    return sql => {
      const statement = target.prepare(sql);
      if (!sql.includes('INDEXED BY cap_oauth_credentials_expiry')) return statement;
      return { all(...args) { queries.push({ sql, args }); return statement.all(...args); } };
    };
  } });
  const cleanupStore = createOAuthTokenStore({ db: observedDb, configuration: config(), clock: f.now });
  f.db.exec('BEGIN IMMEDIATE');
  try { for (let i = 0; i < 3; i++) cleanupStore.cleanup(f.now(), 1); }
  finally { f.db.exec('ROLLBACK'); }
  assert.ok(queries.length >= 2);
  const continuation = queries[1];
  const cleanupPlan = f.db.prepare('EXPLAIN QUERY PLAN ' + continuation.sql).all(...continuation.args).map(row => row.detail).join(' ');
  assert.match(cleanupPlan, /COVERING INDEX cap_oauth_credentials_expiry.*\(\(expires_at,credential_id\)>/u);
  assert.doesNotMatch(cleanupPlan, /TEMP B-TREE|MULTI-INDEX OR/u);
  f.reopen();
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_credentials').get().n, count);
});
