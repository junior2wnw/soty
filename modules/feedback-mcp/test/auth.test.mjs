import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomBytes, createHash } from 'node:crypto';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { environment } from '../../human-identity/test/support/renewal-fixture.mjs';
import { createFeedbackAuth } from '../auth.mjs';
import { createProjectService } from '../service.mjs';
const random = () => randomBytes(32).toString('base64url');

async function fixture(t) {
  const upstream = await environment(t), directory = mkdtempSync(join(tmpdir(), 'feedback-auth-'));
  let auth;
  const server = createServer(async (req, res) => {
    if (!auth || !await auth.handleRequest(req, res)) { res.writeHead(404); res.end(); }
  });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`, service = createProjectService({ origin, dataDirectory: directory });
  let client;
  await upstream.configureClients(clients => { clients[0].redirectUri = origin + '/auth/callback'; clients[0].version++; client = clients[0]; return clients; });
  const config = { origin, dataDirectory: directory, encryptionKey: random(), cookieKeys: [random()], upstream: { issuer: upstream.issuer,
    clientId: client.id, clientSecret: client.clientSecret, redirectUri: client.redirectUri, renewalProfile: 'soty.human-rp-renewal.v1' } };
  auth = await createFeedbackAuth(config, { service });
  t.after(async () => { server.closeAllConnections(); await new Promise(done => server.close(done)); auth.close(); service.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^feedback-auth-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  async function login(wire = upstream.wire, signer = upstream.actor, renewable = true, mutateCallback) {
    const start = await wire.request(origin + '/auth/login'); assert.equal(start.status, 303);
    const authorization = await wire.request(start.location); assert.equal(authorization.status, 303);
    assert.equal((await wire.request(authorization.location)).status, 200);
    const context = await wire.request(authorization.location.href + '/context'); assert.equal(context.status, 200);
    const flow = { rp: { redirectUri: config.upstream.redirectUri }, session: wire, location: authorization.location, context: context.body };
    await upstream.approve(flow, signer, renewable ? { stayInAppSeconds: 86400 } : {});
    const finished = await upstream.complete(flow);
    if (mutateCallback) { mutateCallback(finished.callback); return wire.request(finished.callback); }
    const callback = await wire.request(finished.callback);
    assert.equal(callback.status, 303, 'verified upstream callback');
    const browser = await wire.request(origin + '/auth/session'); assert.equal(browser.body.authenticated, true);
    return { wire, browser: browser.body, callback: finished.callback };
  }
  async function register(changes = {}) {
    const response = await fetch(origin + '/oauth/register', { method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ client_name: 'Test MCP', redirect_uris: ['http://127.0.0.1:54321/callback'],
        token_endpoint_auth_method: 'none', grant_types: ['authorization_code', 'refresh_token'], ...changes }) });
    const body = await response.json(); return { status: response.status, body };
  }
  async function authorize(login, client, changes = {}) {
    const verifier = random(), challenge = createHash('sha256').update(verifier).digest('base64url'),
      params = { client_id: client.client_id, redirect_uri: client.redirect_uris[0], response_type: 'code', scope: 'feedback',
        resource: origin + '/mcp', code_challenge: challenge, code_challenge_method: 'S256', state: random(), ...changes };
    const response = await login.wire.request(origin + '/oauth/authorize?' + new URLSearchParams(params));
    return { response, verifier, params };
  }
  async function consent(login, client) {
    const start = await authorize(login, client); assert.equal(start.response.status, 303);
    const page = await login.wire.request(start.response.location); assert.equal(page.status, 200);
    assert.match(page.text, /Разрешить/u);
    const response = await login.wire.request(start.response.location, { fields: { csrf: login.browser.csrfToken, decision: 'approve' } });
    assert.equal(response.status, 303);
    const finished = await login.wire.request(response.location); assert.equal(finished.status, 303);
    assert.equal(finished.location.origin + finished.location.pathname, client.redirect_uris[0]);
    assert.equal(finished.location.searchParams.get('error'), null);
    return { code: finished.location.searchParams.get('code'), verifier: start.verifier };
  }
  async function token(client, grant, changes = {}) {
    return upstream.wire.request(origin + '/oauth/token', { cookies: false, originHeader: null,
      fields: { grant_type: 'authorization_code', client_id: client.client_id, code: grant.code,
        code_verifier: grant.verifier, redirect_uri: client.redirect_uris[0], resource: origin + '/mcp', ...changes } });
  }
  return { upstream, directory, config, origin, service, login, register, authorize, consent, token, get auth() { return auth; },
    async restart() { auth.close(); auth = await createFeedbackAuth(config, { service }); } };
}

test('real HumanRP + independent maintained OAuth: consent, isolation, PKCE, replay, refresh, revocation and persistence', async t => {
  const f = await fixture(t), logged = await f.login();
  const replay = await logged.wire.request(logged.callback); assert.equal(replay.status, 401);
  const metadata = await fetch(f.origin + '/.well-known/oauth-authorization-server/oauth').then(r => r.json());
  assert.equal(metadata.issuer, f.origin + '/oauth');
  const registered = await f.register(); assert.equal(registered.status, 201); const client = registered.body;
  assert.equal((await f.register({ redirect_uris: ['https://example.com/*'] })).status, 400);
  assert.equal((await f.register({ jwks_uri: 'http://127.0.0.1/private' })).status, 400);
  const wrongResource = await f.authorize(logged, client, { resource: f.upstream.issuer });
  assert.ok(wrongResource.response.status >= 400 || wrongResource.response.location?.searchParams.has('error'));
  const missingPkce = await f.authorize(logged, client, { code_challenge: '', code_challenge_method: '' });
  assert.ok(missingPkce.response.status >= 400 || missingPkce.response.location?.searchParams.has('error'));
  const bad = await f.consent(logged, client), wrong = await f.token(client, bad, { code_verifier: random() });
  assert.equal(wrong.status, 400);
  const grant = await f.consent(logged, client), issued = await f.token(client, grant);
  assert.equal(issued.status, 200); assert.equal(typeof issued.body.refresh_token, 'string');
  const actor = await f.auth.authenticateBearer('Bearer ' + issued.body.access_token, { audience: f.origin + '/mcp' });
  const project = f.service.execute(actor, 'project_create', { name: 'Private', requestId: 'auth-test' }).project;
  assert.equal(f.service.execute(actor, 'projects_list').projects[0].id, project.id);
  await assert.rejects(f.auth.authenticateBearer('Bearer ' + issued.body.access_token, { audience: f.upstream.issuer }));
  await assert.rejects(f.auth.authenticateBearer('Bearer ' + random()));
  await f.restart();
  assert.equal((await f.auth.authenticateBearer('Bearer ' + issued.body.access_token)).accountId, actor.accountId);
  // A second verified owner must not inherit the first owner's projects.
  const outsider = await f.login(f.upstream.freshWire(), f.upstream.outsider), outsideGrant = await f.consent(outsider, client), outsideToken = await f.token(client, outsideGrant),
    outsideActor = await f.auth.authenticateBearer('Bearer ' + outsideToken.body.access_token);
  assert.deepEqual(f.service.execute(outsideActor, 'projects_list').projects, []);
  assert.throws(() => f.service.execute(outsideActor, 'integration_get', { projectId: project.id }), e => e.status === 404);
  const refresh = await f.upstream.wire.request(f.origin + '/oauth/token', { cookies: false, originHeader: null,
    fields: { grant_type: 'refresh_token', client_id: client.client_id, refresh_token: issued.body.refresh_token, resource: f.origin + '/mcp' } });
  assert.equal(refresh.status, 200); assert.equal(refresh.body.refresh_token !== issued.body.refresh_token, true);
  const refreshedActor = await f.auth.authenticateBearer('Bearer ' + refresh.body.access_token);
  const revoke = await f.upstream.wire.request(f.origin + '/oauth/revoke', { cookies: false, originHeader: null,
    fields: { client_id: client.client_id, token: refresh.body.access_token } }); assert.equal(revoke.status, 200);
  await assert.rejects(f.auth.authenticateBearer('Bearer ' + refresh.body.access_token));
  assert.throws(() => f.service.execute(refreshedActor, 'projects_list'), e => e.status === 401);
  const replayCode = await f.token(client, grant); assert.equal(replayCode.status, 400);
  const replayGrant = await f.consent(logged, client), replayIssued = await f.token(client, replayGrant);
  const refreshFields = { grant_type: 'refresh_token', client_id: client.client_id, refresh_token: replayIssued.body.refresh_token, resource: f.origin + '/mcp' };
  const rotated = await f.upstream.wire.request(f.origin + '/oauth/token', { cookies: false, originHeader: null, fields: refreshFields });
  assert.equal(rotated.status, 200);
  const refreshReplay = await f.upstream.wire.request(f.origin + '/oauth/token', { cookies: false, originHeader: null, fields: refreshFields });
  assert.equal(refreshReplay.status, 400);
  await assert.rejects(f.auth.authenticateBearer('Bearer ' + rotated.body.access_token));
  const cleanGrant = await f.consent(logged, client), cleanToken = await f.token(client, cleanGrant),
    beforeLogout = await f.auth.authenticateBearer('Bearer ' + cleanToken.body.access_token);
  const csrfDenied = await logged.wire.request(f.origin + '/auth/logout', { fields: {} }); assert.equal(csrfDenied.status, 403);
  const legacyCsrf = await logged.wire.request(f.origin + '/auth/logout', { fields: {}, headers: { 'x-csrf-token': logged.browser.csrfToken } });
  assert.equal(legacyCsrf.status, 403);
  const ambiguousCsrf = await logged.wire.request(f.origin + '/auth/logout', { fields: {},
    headers: { 'x-csrf-token': logged.browser.csrfToken, 'x-feedback-csrf': logged.browser.csrfToken } });
  assert.equal(ambiguousCsrf.status, 403);
  const duplicateCsrf = await logged.wire.request(f.origin + '/auth/logout', { fields: {},
    headers: { 'X-Feedback-CSRF': logged.browser.csrfToken, 'x-feedback-csrf': logged.browser.csrfToken } });
  assert.equal(duplicateCsrf.status, 403);
  const logout = await logged.wire.request(f.origin + '/auth/logout', { fields: {}, headers: { 'X-Feedback-CSRF': logged.browser.csrfToken } });
  assert.equal(logout.status, 200);
  await assert.rejects(f.auth.authenticateBearer('Bearer ' + cleanToken.body.access_token));
  assert.throws(() => f.service.execute(beforeLogout, 'projects_list'), e => e.status === 401);
  // Neither the OAuth bearer nor confidential upstream secret is plaintext in
  // the main SQLite file or its journal (actual encrypted durable records).
  for (const suffix of ['', '-wal']) {
    let bytes; try { bytes = readFileSync(join(f.directory, 'auth.sqlite' + suffix)); } catch { continue; }
    for (const secret of [issued.body.access_token, issued.body.refresh_token, f.config.upstream.clientSecret]) assert.equal(bytes.includes(Buffer.from(secret)), false);
  }
});

test('strict confidential upstream config rejects wrong callback and extra fields', async () => {
  await assert.rejects(createFeedbackAuth({ origin: 'https://feedback.example', dataDirectory: '.', encryptionKey: random(), cookieKeys: [random()],
    upstream: { issuer: 'https://root.example/human-identity', clientId: 'feedback', clientSecret: random(), redirectUri: 'https://attacker.example/callback' } },
  { service: { actor() {}, identity() {} } }), error => error.code === 'auth_configuration_invalid');
});

test('real Root renewal survives 300 seconds and restart, issuer mixup and current device revocation fail closed', async t => {
  const NativeDate = globalThis.Date; let offset = 0;
  class TestDate extends NativeDate { constructor(...args) { super(...(args.length ? args : [NativeDate.now() + offset])); }
    static now() { return NativeDate.now() + offset; } }
  globalThis.Date = TestDate; t.after(() => { globalThis.Date = NativeDate; });
  const f = await fixture(t);
  const wrongIssuer = await f.login(f.upstream.freshWire(), f.upstream.actor, true, url => url.searchParams.set('iss', 'https://wrong.example/human-identity'));
  assert.equal(wrongIssuer.status, 400);
  const basic = await f.login(f.upstream.freshWire(), f.upstream.actor, false);
  assert.ok(basic.browser.expiresAt <= Date.now() + 300000, 'Basic300 remains bounded');
  const logged = await f.login(), registered = await f.register(), client = registered.body,
    grant = await f.consent(logged, client), issued = await f.token(client, grant);
  offset += 310000; await f.restart(); await f.upstream.restart();
  const refresh = await f.upstream.wire.request(f.origin + '/oauth/token', { cookies: false, originHeader: null,
    fields: { grant_type: 'refresh_token', client_id: client.client_id, refresh_token: issued.body.refresh_token, resource: f.origin + '/mcp' } });
  assert.equal(refresh.status, 200, 'actual Root refresh with durable Source family');
  const actor = await f.auth.authenticateBearer('Bearer ' + refresh.body.access_token);
  assert.equal(actor.accountId, logged.browser.accountId);
  assert.equal((await basic.wire.request(f.origin + '/auth/session')).body.authenticated, false);
  const backup = await f.upstream.backupDevice(), original = await f.upstream.actor.client.getLocalState();
  await backup.client.revokeDevice(original.deviceId);
  await assert.rejects(f.auth.authenticateBearer('Bearer ' + refresh.body.access_token), e => e.status === 401);
  assert.throws(() => f.service.execute(actor, 'projects_list'), e => e.status === 401);
  const denied = await f.upstream.wire.request(f.origin + '/oauth/token', { cookies: false, originHeader: null,
    fields: { grant_type: 'refresh_token', client_id: client.client_id, refresh_token: refresh.body.refresh_token, resource: f.origin + '/mcp' } });
  assert.equal(denied.status, 400);
});
