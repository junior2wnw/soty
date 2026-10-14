import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { generateKeyPairSync, createPublicKey, randomBytes } from 'node:crypto';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHumanIdentityHostProfile } from '../../human-identity/profile.mjs';
import { createHumanIdentityService } from '../../human-identity/service.mjs';
import { createSourceAuthoritySigner, createRootWorkspaceSubjectAuthority } from '../../human-identity/source-authority.mjs';
import { createScopedGateway } from '../scoped-embed/gateway.mjs';
import { createConnectorScopedFactory } from '../scoped-embed/connector-host.mjs';
import { createSourceAuthorityClient, isVerifiedRootContext } from '../scoped-embed/source-authority-client.mjs';
import { selectedResourceProfile } from '../scoped-embed/resource-profile.mjs';
import { hash } from '../scoped-embed/profile.mjs';
import { STANDARD_WORKSPACE_SOURCE, STANDARD_ROOT_WORKSPACE_SOURCE } from '../../source-app/server/standard-profile.mjs';

const random = () => randomBytes(32).toString('base64url');
const keyPair = generateKeyPairSync('rsa', { modulusLength: 2048 });
const jwk = { ...keyPair.privateKey.export({ format: 'jwk' }), kid: 'root-witness-fixture', alg: 'RS256', use: 'sig' };
const publicJwk = { ...createPublicKey(keyPair.privateKey).export({ format: 'jwk' }), kid: jwk.kid, alg: 'RS256', use: 'sig' };
const providerRequire = createRequire(import.meta.resolve('oidc-provider'));
const { jwtVerify, createLocalJWKSet, SignJWT } = await import(pathToFileURL(providerRequire.resolve('jose')).href);

// Real Human SQLite/key/signature and real installed connector HTTP. The Root
// Apps authority callbacks are controlled unit fixtures, not claimed live SSO.
async function fixture(t, pin = STANDARD_ROOT_WORKSPACE_SOURCE) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-root-witness-')), databasePath = join(directory, 'human.sqlite');
  const actor = Object.freeze({ accountId: 'reader', deviceId: 'reader-device' });
  let now = Date.now(), active = true, appActive = true, epoch = 1, human, factory, gateway, hold, transform = value => value;
  const server = createServer((req, res) => {
    if (req.url === '/human-identity/jwks') { res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ keys: [publicJwk] })); return; }
    void factory.sourceAuthority(req, res).catch(error => { res.writeHead(error.status === 403 || error.status === 401 ? 403 : 503,
      { 'content-type': 'application/json' }); res.end(JSON.stringify({ code: error.code })); });
  });
  t.after(async () => { factory?.disconnected(); gateway?.close(); human?.close(); server.closeAllConnections();
    if (server.listening) await new Promise(done => server.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 }); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const port = server.address().port, origin = 'http://127.0.0.1:' + port, appId = 'app-' + 'a'.repeat(32);
  const config = { enabled: true, issuer: origin + '/human-identity', registryId: 'soty', environmentId: 'fixture',
    clients: [{ id: 'existing-native-rp', label: 'Existing Native RP', redirectUri: 'http://localhost:19401/account/soty/callback', clientSecret: random() }],
    jwks: { keys: [jwk] }, cookieKeys: [random()], artifactKey: randomBytes(32), artifactKeyId: 'fixture' };
  const hostProfile = createHumanIdentityHostProfile(config, { shellOrigins: [origin] });
  function openHuman() { human = createHumanIdentityService({ databasePath, profile: createHumanIdentityHostProfile(config, { shellOrigins: [origin] }),
    now: () => now, actorActive: candidate => active && candidate.accountId === actor.accountId && candidate.deviceId === actor.deviceId,
    withAuthorityFence: callback => callback(), withSubjectAuthorityFence: (candidate, callback) => { assert.equal(candidate, actor); return callback(); } }); }
  openHuman();
  const raw = { schema: 'soty.selected-human-embed.v2', appId, connector: { linkId: 'link', hostDeviceId: 'host', connectorId: 'connector' },
    target: { revision: 1, digest: 'a'.repeat(64) }, sourceProfile: pin,
    resource: { registryId: 'soty', tenantId: 'publisher', environmentId: 'fixture', appId, resourceId: 'namespace',
      selection: { kind: 'soty.workspace.v1', nativeId: 'namespace', incarnationId: 'one' } },
    issuer: hostProfile.issuer, clientId: 'existing-native-rp', parentOrigin: origin,
    embedOrigin: 'https://' + appId + '.localhost:24444', nativeOrigin: 'http://localhost:19401' };
  const profile = selectedResourceProfile(raw), actualSigner = createSourceAuthoritySigner({ profile: hostProfile, scopedProfiles: [raw], clock: () => now });
  const rootSubject = createRootWorkspaceSubjectAuthority({ profile: hostProfile, scopedProfiles: [raw], withActorAuthorityFence(candidate, callback) { assert.equal(candidate, actor); assert.ok(active); return callback(); } });
  const signed = [], challenges = [];
  gateway = createScopedGateway({ admissions: { profiles: () => [raw], require: () => profile }, clock: () => now,
    withHumanSubjectAuthority: (request, callback) => request.rootProfileDigest === undefined ? human.withSubjectAuthority(request, callback) : rootSubject(request, callback),
    withAppAuthority(request, callback) { assert.equal(request.actor, actor); assert.ok(appActive, 'fixture app still admitted');
      return callback({ appId, ownerId: 'publisher', accountId: actor.accountId, policyEpoch: epoch, target: raw.target,
        entry: { origin: raw.embedOrigin, domainId: 'domain' } }); },
    async signSourceAuthority(input) { if (hold) { const current = hold; hold = null; current.enter(); await current.wait; }
      const result = await actualSigner(input); signed.push(result); return result; } });
  const opened = gateway.open({ actor, appId, domainId: 'domain', target: raw.target, instanceId: 'main' });
  const context = opened.record.context, key = randomBytes(32);
  factory = createConnectorScopedFactory({ entries: [{ profile: raw, key: key.toString('base64url') }], clock: () => now });
  factory.connect(async request => { challenges.push(request.sourceChallenge); return transform(request.sourceChallenge === undefined
    ? gateway.read(request.reference, request.connector) : await gateway.readWitness(request.reference, request.connector, request.sourceChallenge), request); }, raw.connector);
  const readAuthority = createSourceAuthorityClient({ profile: raw, key, connectorPort: port, clock: () => now });
  return { context, signed, challenges, actualSigner, gateway, profile, key, hostProfile, raw, rootSubject, actor, now: () => now,
    read: () => readAuthority({ reference: context.reference, connector: raw.connector }),
    transform(value) { transform = value; },
    hold() { let enter, release; const entered = new Promise(done => { enter = done; }), wait = new Promise(done => { release = done; }); hold = { enter, wait }; return { entered, release }; },
    revokeActor() { active = false; }, revokeApp() { appActive = false; }, changePolicy() { epoch++; }, changeTarget() { raw.target = { ...raw.target, revision: raw.target.revision + 1 }; },
    changeClient() { human.close(); config.clients[0].version = 2; openHuman(); },
    close() { gateway.abandon(actor, appId, opened.closeHandle); }, advance(ms) { now += ms; },
    artifactCounts() { const db = new DatabaseSync(databasePath, { readOnly: true }); try { return ['human_identity_artifacts', 'human_identity_grant_bindings']
      .map(table => Number(db.prepare('SELECT count(*) AS n FROM ' + table).get().n)); } finally { db.close(); } } };
}

test('Root issuer witness binds original Source nonce/full current context and uses no OIDC family', async t => {
  const f = await fixture(t), before = f.artifactCounts(), result = await f.read();
  assert.deepEqual(result, f.context); assert.equal(isVerifiedRootContext(result), true);
  assert.equal(Object.isFrozen(result.humanPrincipal), true); assert.equal(Object.isFrozen(result.rootPrincipal), true);
  const { payload, protectedHeader } = await jwtVerify(f.signed[0], createLocalJWKSet({ keys: [publicJwk] }), {
    issuer: f.profile.issuer, audience: 'urn:soty:source-authority', algorithms: ['RS256'], typ: 'soty-source-authority+jwt', currentDate: new Date(f.now()) });
  assert.equal(protectedHeader.kid, jwk.kid); assert.equal(payload.sourceChallenge, f.challenges[0]);
  assert.equal(payload.contextDigest, hash(f.context)); assert.equal(payload.sub, 'reader');
  assert.ok(payload.exp * 1000 <= Math.min(f.now() + 10000, f.context.expiresAt));
  assert.deepEqual(f.artifactCounts(), before);
  await assert.rejects(jwtVerify(f.signed[0], createLocalJWKSet({ keys: [publicJwk] }), { audience: 'existing-native-rp' }));
  await assert.rejects(jwtVerify(f.signed[0], createLocalJWKSet({ keys: [publicJwk] }), { typ: 'JWT' }));
  f.advance(10000); assert.equal(isVerifiedRootContext(result), false);
});

test('a connector with its real MAC cannot substitute an actor, nonce, context or issuer witness', async t => {
  const f = await fixture(t); await f.read(); const prior = f.signed[0];
  for (const change of [value => ({ ...value, identityWitness: prior }),
    value => ({ ...value, context: { ...value.context, humanPrincipal: { ...value.context.humanPrincipal, subject: 'victim' }, rootPrincipal: { accountId: 'victim', deviceId: 'victim-device' } } }),
    value => ({ ...value, context: { ...value.context, instanceId: 'widget-11111111-1111-4111-8111-111111111111' } }),
    value => ({ ...value, identityWitness: value.identityWitness.slice(0, -6) + 'AAAAAA' }),
    async (value, request) => ({ ...value, identityWitness: await new SignJWT({ iss: f.profile.issuer, sub: value.context.humanPrincipal.subject,
      aud: 'urn:soty:source-authority', iat: Math.floor(f.now() / 1000), exp: Math.floor(f.now() / 1000) + 10,
      sourceChallenge: request.sourceChallenge, contextDigest: hash(value.context) })
      .setProtectedHeader({ alg: 'HS256', kid: jwk.kid, typ: 'soty-source-authority+jwt' }).sign(f.key) })]) {
    f.transform(change); await assert.rejects(f.read(), { code: 'source_root_witness_invalid' });
  }
});

test('async signing never releases proof after actor/app/policy revoke, close or slot expiry', async t => {
  for (const method of ['revokeActor', 'revokeApp', 'changePolicy', 'changeTarget', 'close', 'expire']) {
    await t.test(method, async sub => {
      const f = await fixture(sub), gate = f.hold(), pending = f.read(); await gate.entered;
      if (method === 'expire') f.advance(300001); else f[method](); gate.release();
      await assert.rejects(pending, { code: 'scoped_embed_authority_denied' });
    });
  }
});

test('workspace1 retains its unsigned RP authority response and cannot request the new witness', async t => {
  const f = await fixture(t, STANDARD_WORKSPACE_SOURCE); assert.deepEqual(await f.read(), f.context);
  assert.deepEqual(f.challenges, [undefined]); assert.equal(f.signed.length, 0);
  await assert.rejects(f.gateway.readWitness(f.context.reference, f.profile.connector, random()), { code: 'app_scoped_identity_witness_unavailable' });
});

test('Root Workspace2 remains independent of RP client generation; Workspace1 still rejects it', async t => {
  const root = await fixture(t); root.changeClient(); assert.deepEqual(await root.read(), root.context);
  const legacy = await fixture(t, STANDARD_WORKSPACE_SOURCE); legacy.changeClient(); await assert.rejects(legacy.read(), { code: 'scoped_embed_authority_denied' });
});


test('Root subject and signer reject unconfigured profile, wrong issuer, stale target and disabled issuer', async t => {
  const f = await fixture(t), request = { actor: f.actor, issuer: f.profile.issuer, clientId: f.profile.clientId, rootProfileDigest: f.profile.digest };
  for (const changed of [{ ...request, rootProfileDigest: 'f'.repeat(64) }, { ...request, clientId: 'unregistered-other' }, { ...request, issuer: 'https://foreign.invalid' }]) {
    let called = false;
    assert.throws(() => f.rootSubject(changed, () => { called = true; })); assert.equal(called, false);
  }
  assert.throws(() => createRootWorkspaceSubjectAuthority({ profile: { enabled: false }, scopedProfiles: [f.raw], withActorAuthorityFence: () => {} }), { code: 'human_identity_disabled' });
  assert.throws(() => createSourceAuthoritySigner({ profile: { enabled: false }, scopedProfiles: [f.raw] }), { code: 'human_identity_disabled' });
  const unconfigured = createSourceAuthoritySigner({ profile: f.hostProfile, scopedProfiles: [] });
  await assert.rejects(unconfigured({ context: f.context, sourceChallenge: random() }), { code: 'human_identity_subject_mismatch' });
  for (const changed of [{ ...f.context, target: { ...f.context.target, revision: 2 } }, { ...f.context, profileDigest: 'f'.repeat(64) },
    { ...f.context, humanPrincipal: { ...f.context.humanPrincipal, clientGeneration: 2 } }])
    await assert.rejects(f.actualSigner({ context: changed, sourceChallenge: random() }), { code: 'human_identity_subject_mismatch' });
});
