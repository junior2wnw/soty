import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { Provider } from 'oidc-provider';
import { initializeCapabilitiesSchema } from '../server/schema.mjs';
import { validateOAuthStorageRows } from '../server/oauth-baseline.mjs';
import { createCapabilitiesService } from '../server/index.mjs';
import { config, fixture, session, interaction, code, id, sha, NOW, ORIGIN, PROJECT } from './support/oauth-artifacts.mjs';
import { seedOAuth } from './support/oauth-baseline.mjs';

const put = (store, payload, extras = {}) => store.upsert({ model: payload.kind, id: payload.jti, payload, ...extras });
const rows = db => db.prepare('SELECT * FROM cap_oauth_artifacts ORDER BY model,id_hash').all();

test('durable AEAD payloads reopen, expose independent clones and never persist raw auxiliary IDs or state', t => {
  const f = fixture(t, { file: true }), store = f.store(), s = session(), i = interaction();
  put(store, s); put(store, i);
  const before = rows(f.db);
  assert.equal(before.length, 2);
  for (const row of before) {
    assert.equal(row.connection_id, null); assert.equal(row.provider_grant_id, null);
    assert.equal(row.consumed_at, null);
    assert.equal(Buffer.from(row.payload_cipher).includes(Buffer.from('private-state-plaintext-canary')), false);
  }
  i.params.state = 'caller mutation';
  const clone = store.find({ model: 'Interaction', id: i.jti }); clone.params.state = 'reader mutation';
  assert.equal(store.find({ model: 'Interaction', id: i.jti }).params.state, 'private-state-plaintext-canary');
  assert.equal(store.findByUid({ uid: s.uid }).jti, s.jti);
  assert.equal(store.find({ model: 'Session', id: id('absent') }), undefined);
  store.close(); f.close(f.db);
  const db = f.open();
  assert.equal(initializeCapabilitiesSchema(db, { projectId: PROJECT }).schemaVersion, 3);
  const reopened = f.store({ handle: db });
  assert.deepEqual(reopened.find({ model: 'Session', id: s.jti }), s);
  assert.equal(reopened.find({ model: 'Interaction', id: i.jti }).params.state, 'private-state-plaintext-canary');
  assert.deepEqual(rows(db), before);
  const ordinary = createCapabilitiesService({ databasePath: f.databasePath, projectId: PROJECT, actorActive: () => true });
  assert.equal(ordinary.oauth, undefined); assert.equal(ordinary.schemaVersion, 3); ordinary.close();
});

test('Session resave/reset keeps original iat window and one UID; expired IDs cannot be revived', t => {
  const f = fixture(t), store = f.store(), original = session();
  put(store, original);
  const reset = { ...original, jti: id('new-id') };
  assert.throws(() => put(store, reset), code('oauth_invalid_artifact'));
  f.advance(10000);
  assert.throws(() => put(store, { ...original, exp: original.exp + 10 }), code('oauth_invalid_artifact'));
  assert.deepEqual(store.find({ model: 'Session', id: original.jti }), original);
  store.destroy({ model: 'Session', id: original.jti }); put(store, reset, { expiresIn: 590 });
  const row = rows(f.db)[0];
  assert.equal(row.created_at, original.iat * 1000); assert.equal(row.retain_until, (original.iat + 600) * 1000);
  assert.equal(store.findByUid({ uid: original.uid }).jti, reset.jti);
  assert.throws(() => put(store, { ...reset, iat: reset.iat + 1 }), code('oauth_invalid_artifact'));
  const shorter = { ...session('short'), exp: original.iat + 20 };
  put(store, shorter); f.advance(10000);
  assert.equal(store.find({ model: 'Session', id: shorter.jti }), undefined);
  assert.throws(() => put(store, { ...shorter, exp: original.exp }), code('oauth_invalid_artifact'));
  f.advance(580000); assert.equal(store.findByUid({ uid: original.uid }), undefined);
});

test('missing/mismatched key and altered authenticated/plain pins refuse without deleting or replacing artifacts', t => {
  const f = fixture(t), store = f.store(), payload = session(); put(store, payload);
  const original = rows(f.db);
  const missing = config(); delete missing.artifactKey; delete missing.artifactKeyId;
  for (const [options, expected] of [[missing, 'oauth_storage_key_unavailable'],
    [config({ artifactKeyId: 'wrong-key-id' }), 'oauth_storage_key_unavailable'],
    [config({ artifactKey: Buffer.alloc(32, 5) }), 'capabilities_storage_corrupt']]) {
    const other = f.store({ config: options });
    for (const operation of [() => other.find({ model: 'Session', id: payload.jti }),
      () => put(other, payload), () => other.destroy({ model: 'Session', id: payload.jti })]) assert.throws(operation, code(expected));
    assert.deepEqual(rows(f.db), original);
  }
  const cipher = Buffer.from(original[0].payload_cipher); cipher[12] ^= 1;
  f.db.prepare('UPDATE cap_oauth_artifacts SET payload_cipher=?').run(cipher);
  assert.throws(() => store.find({ model: 'Session', id: payload.jti }), code('capabilities_storage_corrupt'));
  assert.equal(rows(f.db).length, 1);
  f.db.prepare('UPDATE cap_oauth_artifacts SET payload_cipher=?,expires_at=expires_at-1').run(original[0].payload_cipher);
  assert.throws(() => store.findByUid({ uid: payload.uid }), code('capabilities_storage_corrupt'));
  assert.equal(rows(f.db).length, 1);
});

test('Session remembered grant references require same account/profile but are not live authority; borrowed Interaction destroy stays local', t => {
  const f = fixture(t), store = f.store();
  // Explicit structural future binding, not real consent or issuance.
  const connection = seedOAuth(f.db, { credential: false, accountId: 'owner_1', issuer: ORIGIN + '/oauth', resource: ORIGIN });
  const s = { ...session(), accountId: 'owner_1', authorizations: {
    'soty-codex-cli': { grantId: connection.providerGrantId, sid: id('sid'), persistsLogout: true } } };
  put(store, s);
  assert.throws(() => put(store, { ...s, accountId: 'owner_other' }), code('oauth_invalid_artifact'));
  const i = { ...interaction(), grantId: connection.providerGrantId,
    session: { accountId: 'owner_1', uid: s.uid, cookie: s.jti } };
  put(store, i); store.destroy({ model: 'Interaction', id: i.jti });
  assert.equal(f.db.prepare('SELECT state FROM cap_oauth_connections').get().state, 'active');
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM cap_oauth_artifacts WHERE model='Grant'").get().n, 1);
  assert.equal(store.findByUid({ uid: s.uid }).authorizations['soty-codex-cli'].persistsLogout, true);
});

test('this increment cannot create bound models, consume tokens, use a staged context or enable OAuth on v1/v2', t => {
  const f = fixture(t), store = f.store();
  for (const model of ['Grant', 'AuthorizationCode', 'RefreshToken', 'AccessToken']) {
    for (const action of [() => store.upsert({ model, id: id(model), payload: {} }),
      () => store.find({ model, id: id(model) }), () => store.destroy({ model, id: id(model) })]) {
      assert.throws(action, code('oauth_unavailable'));
    }
  }
  assert.throws(() => put(store, session(), { stagedGrant: {} }), code('oauth_context_invalid'));
  assert.throws(() => store.consume({ model: 'RefreshToken', id: id('rt') }), code('oauth_unavailable'));
  assert.throws(() => store.revokeByGrantId({ providerGrantId: id('grant') }), code('oauth_unavailable'));
  assert.equal(rows(f.db).length, 0);
  for (const version of [1, 2]) {
    const old = fixture(t, { version }), unavailable = old.store();
    assert.throws(() => put(unavailable, session()), code('oauth_unavailable'));
    assert.equal(old.db.prepare('PRAGMA user_version').get().user_version, version);
    assert.equal(old.db.prepare("SELECT 1 FROM sqlite_schema WHERE name='cap_oauth_artifacts'").get(), undefined);
  }
});

test('auxiliary quota does not evict live rows and indexed cleanup consumes at most 64 identities', t => {
  const f = fixture(t), store = f.store();
  for (let i = 0; i < 1024; i++) put(store, session('quota-' + i));
  assert.throws(() => put(store, interaction('overflow')), code('oauth_quota_exceeded'));
  assert.equal(rows(f.db).length, 1024);
  put(store, session('quota-0'));
  assert.equal(store.cleanup({ limit: 64 }).artifactsDeleted, 0);
  store.destroy({ model: 'Session', id: id('quota-0') }); put(store, interaction('new-slot'));
  f.advance(600000);
  assert.deepEqual(store.cleanup({ limit: 64 }), { artifactsDeleted: 64, interactionsDeleted: 0, credentialsDeleted: 0 });
  assert.equal(rows(f.db).length, 960);
  for (const limit of [0, 65, 1.5, '1']) assert.throws(() => store.cleanup({ limit }), code('oauth_invalid_artifact'));
  const plan = f.db.prepare(`EXPLAIN QUERY PLAN SELECT model,id_hash FROM cap_oauth_artifacts INDEXED BY cap_oauth_artifacts_retention
    WHERE retain_until<=? AND model IN ('Session','Interaction') ORDER BY retain_until,model,id_hash LIMIT ?`).all(f.now(), 64);
  assert.ok(plan.some(row => /SEARCH cap_oauth_artifacts USING (?:COVERING )?INDEX cap_oauth_artifacts_retention \(retain_until</u.test(row.detail)));
  validateOAuthStorageRows(f.db);
});

test('actual second writer yields a controlled bounded busy refusal without changing existing artifacts', t => {
  const f = fixture(t, { file: true }), store = f.store(), payload = session(); put(store, payload);
  const writer = f.open(), before = rows(f.db);
  writer.exec('BEGIN IMMEDIATE');
  try { assert.throws(() => put(store, interaction()), code('oauth_storage_busy')); }
  finally { writer.exec('ROLLBACK'); }
  assert.equal(f.db.isTransaction, false); assert.deepEqual(rows(f.db), before);
  put(store, interaction()); assert.equal(rows(f.db).length, 2);
});

test('captured transaction callback cannot run late, twice or through a Promise; caller mutation during redirect cannot rewrite pins', async t => {
  const f = fixture(t); let late;
  const lateStore = f.store({ transaction(fn) { late = fn; return undefined; } });
  assert.throws(() => put(lateStore, session()), code('oauth_context_invalid'));
  assert.throws(() => late(), code('oauth_context_invalid')); assert.equal(rows(f.db).length, 0);
  const promiseStore = f.store({ transaction() { return Promise.reject(new Error('synthetic transaction fault')); } });
  assert.throws(() => put(promiseStore, session()), code('oauth_context_invalid'));
  await new Promise(resolve => setImmediate(resolve));
  const payload = interaction(), args = { model: 'Interaction', id: payload.jti, payload };
  const expectedId = payload.jti;
  const mutating = f.store({ config: config({ isRegisteredRedirect() { args.id = id('mutated'); args.model = 'Grant'; return true; } }) });
  mutating.upsert(args);
  assert.equal(rows(f.db)[0].model, 'Interaction'); assert.equal(rows(f.db)[0].id_hash, sha(expectedId));
});

test('AS-off baseline accepts literal callback :80 and rejects padded, out-of-range and foreign structural redirects', t => {
  for (const [redirectUri, accepted] of [['http://127.0.0.1:80/callback', true], ['http://[::1]:80/callback', true],
    ['http://127.0.0.1:080/callback', false], ['http://127.0.0.1:65536/callback', false],
    ['http://someone@127.0.0.1:80/callback', false], ['http://127.0.0.1:80/callback#fragment', false],
    ['https://foreign.invalid/callback', false]]) {
    const f = fixture(t);
    f.db.prepare(`INSERT INTO cap_oauth_interactions(uid_hash,issuer,static_client_id,resource,redirect_uri,request_digest,
      browser_nonce_hash,duration_ms,budget_limit,created_at,expires_at,decision)
      VALUES(?,?,?,?,?,?,?,86400000,20,1000,601000,'pending')`)
      .run(sha('interaction'), ORIGIN + '/oauth', 'soty-codex-cli', ORIGIN, redirectUri, sha('request'), sha('nonce'));
    if (accepted) assert.equal(initializeCapabilitiesSchema(f.db, { projectId: PROJECT }).schemaVersion, 3);
    else assert.throws(() => initializeCapabilitiesSchema(f.db, { projectId: PROJECT }), code('capabilities_storage_corrupt'));
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_interactions').get().n, 1);
  }
});

test('actual pinned Provider Session and first Interaction serialize through the encrypted store, including resetIdentifier', async t => {
  assert.equal(JSON.parse(readFileSync(new URL('../../../node_modules/oidc-provider/package.json', import.meta.url))).version, '9.12.2');
  t.mock.timers.enable({ apis: ['Date'], now: NOW });
  const f = fixture(t, { clock: Date.now }), store = f.store();
  class Adapter {
    constructor(model) { this.model = model; }
    async upsert(id, payload, expiresIn) { store.upsert({ model: this.model, id, payload, expiresIn }); }
    async find(id) { return store.find({ model: this.model, id }); }
    async findByUid(uid) { return store.findByUid({ uid }); }
    async destroy(id) { store.destroy({ model: this.model, id }); }
  }
  const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(key, { kid: 'fixture', use: 'sig', alg: 'RS256' });
  const provider = new Provider(ORIGIN + '/oauth', { adapter: Adapter, clients: [],
    jwks: { keys: [key] }, cookies: { keys: ['fixture-only-cookie-signing-key-not-production'] },
    features: { devInteractions: { enabled: false } }, expiresWithSession: () => false });
  const original = new provider.Session(); await original.save(600);
  const loaded = await provider.Session.findByUid(original.uid);
  assert.ok(loaded); const initialIat = loaded.iat, oldId = loaded.jti;
  t.mock.timers.tick(10000);
  await assert.rejects(() => loaded.save(600), code('oauth_invalid_artifact'));
  await loaded.save(590); loaded.resetIdentifier(); await loaded.save(590);
  const current = store.findByUid({ uid: original.uid });
  assert.equal(current.iat, initialIat); assert.equal(current.exp, initialIat + 600);
  assert.notEqual(current.jti, oldId); assert.equal(store.find({ model: 'Session', id: oldId }), undefined);
  assert.equal(rows(f.db)[0].created_at, initialIat * 1000);
  const template = interaction('actual', Date.now());
  const first = new provider.Interaction(template.jti, { params: template.params, prompt: template.prompt,
    cid: template.cid, returnTo: template.returnTo, session: loaded, lastSubmission: undefined });
  await first.save(600);
  const saved = store.find({ model: 'Interaction', id: first.jti });
  assert.equal(saved.result, undefined); assert.equal(saved.params.state, template.params.state);
  assert.equal(saved.trusted, undefined);
});
