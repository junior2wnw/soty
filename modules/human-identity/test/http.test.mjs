import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createServer } from 'node:http';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync, mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createClientWithStorage } from '../../connect/browser/client.mjs';
import { attachConnectModule } from '../../../server/connect-module.js';
import { createHumanIdentityHostProfile } from '../profile.mjs';
import { createHumanIdentityService } from '../service.mjs';
import { attachHumanIdentity } from '../../../server/human-identity.js';
import { createHumanBffFixture, verifyHumanIdToken } from '../examples/bff.mjs';

const random = () => randomBytes(32).toString('base64url');
function memoryVault() {
  let value = null;
  return { async read() { return structuredClone(value); }, async claim(next) { value ??= structuredClone(next); return structuredClone(value); },
    async compareAndSwap(revision, next) { assert.equal(value.localRevision, revision); value = structuredClone(next); return structuredClone(value); } };
}
function browser() {
  const jar = new Map();
  function save(response, url) {
    for (const raw of response.headers.getSetCookie()) {
      const [pair, ...attributes] = raw.split(';'), equal = pair.indexOf('=');
      const attrs = Object.fromEntries(attributes.map(value => { const split = value.indexOf('='); return split < 0 ? [value.trim().toLowerCase(), true]
        : [value.slice(0, split).trim().toLowerCase(), value.slice(split + 1).trim()]; }));
      assert.equal(attrs.domain, undefined, 'host-only cookies');
      const cookie = { host: url.hostname, name: pair.slice(0, equal), value: pair.slice(equal + 1), path: attrs.path || '/', secure: attrs.secure === true };
      const key = cookie.host + '\0' + cookie.path + '\0' + cookie.name;
      if (!cookie.value || attrs['max-age'] === '0') jar.delete(key); else jar.set(key, cookie);
    }
  }
  async function request(input, { fields, headers = {}, cookies = true, originHeader } = {}) {
    const url = new URL(input); assert.equal(url.hostname, '127.0.0.1', 'fixture never contacts a non-loopback provider/app');
    const cookie = [...jar.values()].filter(value => value.host === url.hostname && (!value.secure || url.protocol === 'https:')
      && (url.pathname === value.path || url.pathname.startsWith(value.path.endsWith('/') ? value.path : value.path + '/')))
      .map(value => value.name + '=' + value.value).join('; ');
    const response = await fetch(url, { redirect: 'manual', signal: AbortSignal.timeout(5000),
      ...(fields ? { method: 'POST', body: new URLSearchParams(fields) } : {}),
      headers: { ...(cookies && cookie ? { cookie } : {}), ...(fields && originHeader !== null ? { Origin: originHeader || url.origin, 'Sec-Fetch-Site': 'same-origin' } : {}), ...headers } });
    if (cookies) save(response, url);
    const text = await response.text(); assert.ok(Buffer.byteLength(text) <= 65536, 'bounded fixture responses');
    const location = response.headers.get('location');
    return { status: response.status, text, headers: response.headers, location: location ? new URL(location, url) : null,
      body: response.headers.get('content-type')?.includes('application/json') ? JSON.parse(text) : null };
  }
  return { request, setCookie(origin, raw) { const url = new URL(origin), equal = raw.indexOf('='); jar.set(url.hostname + '\0/\0' + raw.slice(0, equal),
    { host: url.hostname, path: '/', name: raw.slice(0, equal), value: raw.slice(equal + 1), secure: false }); } };
}
async function environment(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-human-oidc-')), clients = [];
  const secrets = [random(), random()], rps = await Promise.all(['app-alpha', 'app-beta'].map((clientId, index) => createHumanBffFixture({ clientId, clientSecret: secrets[index] })));
  let app, identity, connect;
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  await new Promise(done => server.listen(0, '127.0.0.1', done)); const origin = `http://127.0.0.1:${server.address().port}`, issuer = origin + '/human-identity';
  const privateJwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(privateJwk, { kid: 'human-fixture', alg: 'RS256', use: 'sig' });
  const options = { enabled: true, issuer, registryId: 'REG.soty', environmentId: 'fixture',
    clients: rps.map((rp, index) => ({ id: rp.clientId, label: 'Fixture ' + rp.clientId, redirectUri: rp.redirectUri, clientSecret: secrets[index] })),
    jwks: { keys: [privateJwk] }, cookieKeys: [random()], artifactKey: randomBytes(32), artifactKeyId: 'human-fixture' };
  let profile = createHumanIdentityHostProfile(options, { shellOrigins: [origin] });
  const identityPath = join(directory, 'human-identity', 'identity.sqlite'), distDir = join(directory, 'dist'); mkdirSync(distDir);
  writeFileSync(join(distDir, 'index.html'), '<!doctype html><title>Fixture root vault login</title><p>Existing Soty profile approval</p>');
  const create = () => {
    app = express();
    identity = createHumanIdentityService({ databasePath: identityPath, profile,
      actorActive: actor => connect?.isActorActive(actor) === true,
      withAuthorityFence: callback => connect.withAuthorityFence(callback),
      readProfile: () => ({ name: 'Same display name', preferred_username: 'shared-profile' }) });
    connect = attachConnectModule(app, { dataDir: directory, origins: [origin], extensions: [identity] });
    attachHumanIdentity(app, { profile, service: identity, distDir });
    app.use((_req, res) => res.status(404).json({ error: 'not_found' }));
  };
  create(); await Promise.all(rps.map(rp => rp.configure(issuer)));
  t.after(async () => {
    clients.forEach(client => client.dispose()); server.closeAllConnections(); await new Promise(done => server.close(done));
    identity?.close(); connect?.close(); await Promise.all(rps.map(rp => rp.close()));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-human-oidc-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  });
  async function newClient(label, bootstrap = true) {
    const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
      fetch: (url, opts) => fetch(url, { ...opts, headers: { ...opts.headers, origin } }) }, memoryVault()); clients.push(client);
    return { client, ...(bootstrap ? { account: await client.bootstrap(label) } : {}) };
  }
  const actor = await newClient('Primary Soty vault'), outsider = await newClient('Independent other profile');
  const wire = browser(); let serial = 0;
  async function begin(rp = rps[0], session = wire, scope) {
    const start = await session.request(rp.origin + '/login' + (scope ? '?scope=' + encodeURIComponent(scope) : ''));
    assert.equal(start.status, 302); const authorize = await session.request(start.location);
    assert.ok([302, 303].includes(authorize.status), 'standard authorize reaches current-device interaction');
    assert.equal(authorize.location.origin, origin); const document = await session.request(authorize.location);
    assert.equal(document.status, 200, 'real root interaction document'); assert.doesNotMatch(document.text, /password|username|type="password"/iu);
    const context = await session.request(authorize.location.href + '/context');
    assert.equal(context.status, 200, 'context safe protocol status=' + context.status);
    return { rp, session, location: authorize.location, context: context.body, authorization: start.location };
  }
  async function approve(flow, signer = actor, changes = {}) {
    const args = { expectedAccountId: signer.account.accountId, interactionId: flow.context.interactionId,
      browserNonce: flow.context.browserNonce, csrf: flow.context.csrf, requestId: 'decision-' + (++serial), decision: 'approve', ...changes };
    return signer.client.extension('identity.human.approve', args, { expectedAccountId: signer.account.accountId });
  }
  async function complete(flow, fields = { csrf: flow.context.csrf }) {
    let response = await flow.session.request(flow.location.href + '/complete', { fields });
    assert.equal(response.status, 303, 'signed proof completed; safe error=' + (response.body?.error || 'none'));
    response = await flow.session.request(response.location);
    if (response.status === 200) {
      const action = /<form method="post" action="([^"]+)">/u.exec(response.text)?.[1];
      assert.equal(action, issuer + '/session/end/confirm');
      const hidden = Object.fromEntries([...response.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)].map(match => [match[1], match[2]]));
      response = await flow.session.request(action, { fields: hidden }); response = await flow.session.request(response.location);
    }
    assert.ok([302, 303].includes(response.status), 'standard resume issues exact RP callback, safe status=' + response.status);
    assert.equal(response.location.origin + response.location.pathname, flow.rp.redirectUri);
    return { ...flow, callback: response.location };
  }
  async function login(rp = rps[0], signer = actor, session = wire, scope) {
    const flow = await begin(rp, session, scope); await approve(flow, signer); const finished = await complete(flow);
    const callback = await session.request(finished.callback); assert.equal(callback.status, 200, 'BFF verified callback; safe error=' + (callback.body?.error || 'none'));
    return finished;
  }
  return { origin, issuer, rps, actor, outsider, wire, begin, approve, complete, login, newClient, directory, identityPath, profile,
    get identity() { return identity; }, get connect() { return connect; },
    async restart() { identity.close(); connect.close(); create(); },
    async exchangeCode(flow, { clientId = flow.rp.clientId, verifier = flow.rp.pendingFixture().verifier } = {}) {
      const client = options.clients.find(value => value.id === clientId);
      return wire.request(issuer + '/token', { cookies: false, originHeader: null, headers: { authorization: 'Basic ' + Buffer.from(clientId + ':' + client.clientSecret).toString('base64') },
        fields: { grant_type: 'authorization_code', code: flow.callback.searchParams.get('code'), redirect_uri: flow.rp.redirectUri, code_verifier: verifier } });
    },
    async configureClients(change) {
      identity.close(); connect.close(); options.clients = change(structuredClone(options.clients));
      profile = createHumanIdentityHostProfile(options, { shellOrigins: [origin] }); create();
    },
    tryClientConfiguration(change) {
      const candidate = createHumanIdentityHostProfile({ ...options, clients: change(structuredClone(options.clients)) }, { shellOrigins: [origin] });
      return createHumanIdentityService({ databasePath: identityPath, profile: candidate,
        actorActive: actor => connect.isActorActive(actor), withAuthorityFence: callback => connect.withAuthorityFence(callback) });
    },
    async addThirdRp() {
      const secret = random(), rp = await createHumanBffFixture({ clientId: 'app-gamma', clientSecret: secret }); rps.push(rp);
      options.clients.push({ id: rp.clientId, label: 'Independent gamma', redirectUri: rp.redirectUri, clientSecret: secret });
      identity.close(); connect.close(); profile = createHumanIdentityHostProfile(options, { shellOrigins: [origin] }); create();
      await rp.configure(issuer); return rp;
    },
    async backupDevice() {
      const backup = await newClient('Backup', false), pending = await backup.client.startEnrollment('Backup existing profile');
      await actor.client.approveEnrollment(pending.requestId, actor.account.accountId);
      await backup.client.previewEnrollment(pending.requestId);
      backup.account = await backup.client.finishEnrollment(pending.requestId, actor.account.accountId); return backup;
    },
  };
}

test('two independently served BFFs use one maintained OIDC issuer and current signed Soty profile without forms or account merging', { timeout: 20000 }, async t => {
  const f = await environment(t); await f.login(f.rps[0]); await f.login(f.rps[1]);
  const a = await f.wire.request(f.rps[0].origin + '/me'), b = await f.wire.request(f.rps[1].origin + '/me');
  assert.equal(a.status, 200); assert.equal(b.status, 200);
  assert.equal(a.body.identity.issuer, f.issuer); assert.equal(a.body.identity.sub, f.actor.account.accountId);
  assert.equal(a.body.identity.sub, b.body.identity.sub); assert.equal(a.body.identity.name, b.body.identity.name);
  assert.notEqual(a.body.localAccountId, b.body.localAccountId);
  for (const rp of f.rps) { assert.equal(rp.existingLocalRow().balance, 17); assert.equal(rp.existingLocalRow().marker, 'owned local row');
    assert.notEqual(rp.currentLink(f.actor.account.accountId), rp.existingLocalRow().id); }
});

test('issuer artifacts survive real restart encrypted; public context and persistent bytes exclude keys, tokens and nonce/state bodies', { timeout: 20000 }, async t => {
  const f = await environment(t), flow = await f.begin();
  assert.deepEqual(Object.keys(flow.context).sort(), ['schema', 'interactionId', 'browserNonce', 'csrf', 'client', 'scopes', 'expiresAt', 'decision'].sort());
  const decision = await f.approve(flow); assert.equal(decision.decision, 'approved');
  const retry = await f.approve(flow); assert.equal(retry.decision, 'approved');
  await f.restart(); const finished = await f.complete(flow); const result = await f.wire.request(finished.callback); assert.equal(result.status, 200);
  const secrets = f.rps[0].verificationFixture(), bytes = Buffer.concat([readFileSync(f.identityPath), readFileSync(f.identityPath + '-wal')]);
  for (const value of [flow.context.csrf, flow.context.browserNonce, flow.authorization.searchParams.get('state'), flow.authorization.searchParams.get('nonce'),
    secrets.idToken, secrets.accessToken]) assert.equal(bytes.includes(Buffer.from(value)), false, 'secret material not present in store plaintext');
  const db = new DatabaseSync(f.identityPath, { readOnly: true });
  try { assert.ok(db.prepare('SELECT count(*) AS n FROM human_identity_artifacts').get().n > 0); }
  finally { db.close(); }
});

test('current device revocation rejects both RP sessions and stale signed approval while an enrolled backup keeps the same profile', { timeout: 20000 }, async t => {
  const f = await environment(t), backup = await f.backupDevice(); await f.login(f.rps[0]); await f.login(f.rps[1]);
  const pending = await f.begin(), before = await f.actor.client.getLocalState();
  await backup.client.revokeDevice(before.deviceId);
  for (const rp of f.rps) assert.equal((await f.wire.request(rp.origin + '/me')).status, 401);
  await assert.rejects(f.approve(pending), error => typeof error.code === 'string');
  await f.login(f.rps[0], backup); assert.equal((await f.wire.request(f.rps[0].origin + '/me')).body.identity.sub, f.actor.account.accountId);
});

test('adding a third RP preserves existing grants; removal/readd and redirect changes fence only the affected client', { timeout: 20000 }, async t => {
  const f = await environment(t); await f.login(f.rps[0]); await f.login(f.rps[1]);
  const gamma = await f.addThirdRp();
  for (const rp of f.rps.slice(0, 2)) assert.equal((await f.wire.request(rp.origin + '/me')).status, 200, 'another RP cannot revoke an existing client');
  await f.login(gamma);
  let alpha;
  await f.configureClients(clients => { alpha = clients.find(client => client.id === 'app-alpha'); return clients.filter(client => client.id !== 'app-alpha'); });
  assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 401);
  assert.equal((await f.wire.request(f.rps[1].origin + '/me')).status, 200);
  await f.configureClients(clients => [...clients, alpha]);
  assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 401, 'same-version readd cannot revive pre-removal tokens');
  assert.equal((await f.wire.request(f.rps[1].origin + '/me')).status, 200);
  await f.login(f.rps[0]);
  assert.throws(() => f.tryClientConfiguration(clients => clients.map(client => client.id === 'app-alpha'
    ? { ...client, redirectUri: client.redirectUri + '-new' } : client)), error => error.code === 'human_identity_client_version_conflict');
  assert.equal((await f.wire.request(f.rps[1].origin + '/me')).status, 200);
  await f.configureClients(clients => clients.map(client => client.id === 'app-alpha' ? { ...client, version: 2, redirectUri: client.redirectUri + '-new' } : client));
  assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 401);
  assert.equal((await f.wire.request(f.rps[1].origin + '/me')).status, 200);
  assert.throws(() => f.tryClientConfiguration(clients => clients.map(client => client.id === 'app-alpha' ? { ...alpha, version: 1 } : client)),
    error => error.code === 'human_identity_client_version_rollback');
});

test('wrong browser/CSRF/Origin and mixed approved actors cannot finish or change a signed decision', { timeout: 20000 }, async t => {
  const f = await environment(t), flow = await f.begin(), wrong = browser();
  assert.ok((await wrong.request(flow.location.href + '/context')).status >= 400);
  await assert.rejects(f.approve(flow, f.actor, { browserNonce: random() }), error => error.code === 'human_identity_browser_mismatch');
  await assert.rejects(f.approve(flow, f.actor, { csrf: random() }), error => error.code === 'human_identity_browser_mismatch');
  const args = { requestId: 'stable-decision' }, approved = await f.approve(flow, f.actor, args);
  const replay = await f.approve(flow, f.actor, args); assert.deepEqual(replay, approved);
  await assert.rejects(f.approve(flow, f.actor, { ...args, decision: 'deny' }), error => error.code === 'human_identity_intent_conflict');
  await assert.rejects(f.approve(flow, f.outsider), error => error.code === 'human_identity_decision_conflict');
  const noOrigin = await f.wire.request(flow.location.href + '/complete', { fields: { csrf: flow.context.csrf }, headers: { Origin: 'https://foreign.example' } });
  assert.equal(noOrigin.status, 403);
  const badCsrf = await f.wire.request(flow.location.href + '/complete', { fields: { csrf: random() } }); assert.equal(badCsrf.status, 403);
  const otherFlow = await f.begin(f.rps[1]);
  const mixed = await f.wire.request(otherFlow.location.href + '/complete', { fields: { csrf: flow.context.csrf } }); assert.equal(mixed.status, 403);
  await assert.rejects(f.approve(otherFlow, f.actor, { interactionId: flow.context.interactionId, requestId: 'mixed-client' }),
    error => error.code === 'human_identity_browser_mismatch');
});

test('standard PKCE and client credentials reject mixed redemption; state/issuer/nonce/audience/signature are independently enforced by real BFFs', { timeout: 20000 }, async t => {
  const f = await environment(t), flow = await f.begin(); await f.approve(flow); const finished = await f.complete(flow);
  const badPkce = await f.exchangeCode(finished, { verifier: random() }); assert.equal(badPkce.status, 400); assert.equal(badPkce.body.error, 'invalid_grant');
  const wrongClient = await f.exchangeCode(finished, { clientId: 'app-beta' }); assert.equal(wrongClient.status, 400); assert.equal(wrongClient.body.error, 'invalid_grant');
  const badState = new URL(finished.callback); badState.searchParams.set('state', random());
  assert.equal((await f.wire.request(badState)).status, 401); assert.equal(f.rps[0].lastError, 'bff_state_mismatch');
  const badIssuer = new URL(finished.callback); badIssuer.searchParams.set('iss', f.origin + '/oauth');
  assert.equal((await f.wire.request(badIssuer)).status, 401); assert.equal(f.rps[0].lastError, 'bff_issuer_mismatch');
  // Capture the real Alpha Basic header in the fixture's own BFF callback, not a seeded issuer token.
  const success = await f.wire.request(finished.callback); assert.equal(success.status, 200);
  const verification = f.rps[0].verificationFixture();
  for (const change of [{ clientId: 'app-beta' }, { issuer: f.origin + '/oauth' }, { nonce: random() }]) {
    await assert.rejects(verifyHumanIdToken({ token: verification.idToken, jwks: verification.jwks, issuer: f.issuer,
      clientId: 'app-alpha', nonce: verification.expectedNonce, ...change }));
  }
  const parts = verification.idToken.split('.'); parts[2] = (parts[2][0] === 'A' ? 'B' : 'A') + parts[2].slice(1);
  await assert.rejects(verifyHumanIdToken({ token: parts.join('.'), jwks: verification.jwks, issuer: f.issuer, clientId: 'app-alpha', nonce: verification.expectedNonce }));
  assert.equal((await f.wire.request(finished.callback)).status, 401, 'used state/code cannot create another app session');
});

test('openid-only consent exposes only the common subject, while legacy account linking requires both independent proofs', { timeout: 20000 }, async t => {
  const f = await environment(t); await f.login(f.rps[0], f.actor, f.wire, 'openid');
  const me = await f.wire.request(f.rps[0].origin + '/me'); assert.equal(me.status, 200);
  assert.deepEqual(Object.keys(me.body.identity).sort(), ['issuer', 'sub']);
  const old = f.rps[0].existingLocalRow();
  const absent = await f.wire.request(f.rps[0].origin + '/oidc/link', { fields: { csrf: me.body.linkCsrf } });
  assert.equal(absent.status, 401, 'issuer auth alone does not prove ownership of an old app account');
  f.wire.setCookie(f.rps[0].origin, f.rps[0].authenticateExistingLocalAccount());
  const wrong = await f.wire.request(f.rps[0].origin + '/oidc/link', { fields: { csrf: random() } }); assert.equal(wrong.status, 401);
  const linked = await f.wire.request(f.rps[0].origin + '/oidc/link', { fields: { csrf: me.body.linkCsrf } });
  assert.equal(linked.status, 200); assert.equal(linked.body.localAccountId, old.id);
  assert.deepEqual(f.rps[0].existingLocalRow(), old, 'existing local data/roles are preserved, not migrated by a matching name');
  assert.equal((await f.wire.request(f.rps[0].origin + '/me')).body.localAccountId, old.id);
});

test('completion lost ACK reuses one durable grant, and genuine SDK account-switch form preserves same-origin CSRF and exact callback CSP', { timeout: 20000 }, async t => {
  const f = await environment(t), flow = await f.begin(); await f.approve(flow);
  for (let attempt = 0; attempt < 2; attempt++) {
    const completed = await f.wire.request(flow.location.href + '/complete', { fields: { csrf: flow.context.csrf } }); assert.equal(completed.status, 303);
  }
  const db = new DatabaseSync(f.identityPath, { readOnly: true });
  try { assert.equal(db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings').get().n, 1); } finally { db.close(); }
  const finished = await f.complete(flow); assert.equal((await f.wire.request(finished.callback)).status, 200);
  const switchFlow = await f.begin(f.rps[0]); await f.approve(switchFlow, f.outsider);
  const completed = await f.wire.request(switchFlow.location.href + '/complete', { fields: { csrf: switchFlow.context.csrf } });
  const autoform = await f.wire.request(completed.location); assert.equal(autoform.status, 200);
  assert.equal(autoform.headers.get('referrer-policy'), 'same-origin');
  assert.ok(autoform.headers.get('content-security-policy').includes("form-action 'self' " + f.rps[0].redirectUri));
  assert.ok(autoform.headers.get('content-security-policy').includes('sha256-'), 'vendor script hash remains enforced');
});
