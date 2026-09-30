import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { createServer } from 'node:http';
import express from 'express';
import { Provider } from 'oidc-provider';
import { fixture, config, CLIENT } from './support/oauth-artifacts.mjs';

// Actual Provider HTTP login -> resume -> next consent, stopping before code
// issuance. Login is synthetic test UI; this is not signed Connect consent.
test('actual Provider resume persists empty trusted keys on the next consent interaction', async t => {
  const app = express(), server = createServer(app);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const origin = `http://127.0.0.1:${server.address().port}`, issuer = origin + '/oauth';
  const f = fixture(t, { clock: Date.now,
    configuration: config({ issuer, resources: { http: origin, mcp: origin + '/mcp' } }) });
  const store = f.store(), observed = [], problems = [];
  class Adapter {
    constructor(model) { this.model = model; }
    async upsert(id, payload, expiresIn) {
      if (this.model === 'Interaction') observed.push({ id, prompt: payload.prompt.name,
        trusted: payload.trusted, previousLogin: payload.lastSubmission?.login?.accountId });
      try { store.upsert({ model: this.model, id, payload, expiresIn }); }
      catch (error) { problems.push(error.code); throw error; }
    }
    async find(id) { return store.find({ model: this.model, id }); }
    async findByUid(uid) { return store.findByUid({ uid }); }
    async destroy(id) { store.destroy({ model: this.model, id }); }
  }
  const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(key, { kid: 'test', use: 'sig', alg: 'RS256' });
  const provider = new Provider(issuer, {
    adapter: Adapter,
    clients: [{ client_id: CLIENT, application_type: 'native', token_endpoint_auth_method: 'none',
      redirect_uris: ['http://127.0.0.1:19876/callback'], grant_types: ['authorization_code'], response_types: ['code'] }],
    jwks: { keys: [key] }, cookies: { keys: ['fixture-only-cookie-key-not-production'] },
    responseTypes: ['code'], scopes: [], claims: {}, pkce: { required: () => true },
    routes: { authorization: '/authorize', resume: '/authorize/:uid' }, expiresWithSession: () => false,
    findAccount: async (_ctx, accountId) => accountId === 'fixture-owner'
      ? { accountId, claims: async () => ({ sub: accountId }) } : undefined,
    features: { devInteractions: { enabled: false }, registration: { enabled: false },
      clientIdMetadataDocument: { enabled: false }, requestObjects: { enabled: false },
      pushedAuthorizationRequests: { enabled: false }, userinfo: { enabled: false },
      resourceIndicators: { enabled: true, getResourceServerInfo(_ctx, resource) {
        assert.equal(resource, origin + '/mcp');
        return { scope: 'notes.createDraft', accessTokenTTL: 300, accessTokenFormat: 'opaque' };
      } } },
    ttl: { Interaction: 600, Session: (_ctx, token) => Math.min(600,
      (token.iat ?? Math.floor(Date.now() / 1000)) + 600 - Math.floor(Date.now() / 1000)) },
    interactions: { url: (_ctx, interaction) => `${issuer}/interaction/${interaction.uid}` },
  });
  provider.on('server_error', () => {});
  app.get('/oauth/interaction/:uid', async (req, res, next) => {
    try {
      const details = await provider.interactionDetails(req, res);
      assert.equal(details.uid, req.params.uid);
      if (details.prompt.name === 'login') {
        await provider.interactionFinished(req, res, { login: { accountId: 'fixture-owner', remember: false } });
      } else { assert.equal(details.prompt.name, 'consent'); res.json({ prompt: 'consent' }); }
    } catch (error) { problems.push(error.code ?? 'fixture_route_failure'); next(error); }
  });
  app.use('/oauth', provider.callback());
  app.use((_error, _req, res, _next) => res.status(500).json({ error: 'fixture_failure' }));
  const params = new URLSearchParams({ client_id: CLIENT, redirect_uri: 'http://127.0.0.1:19876/callback',
    response_type: 'code', scope: 'notes.createDraft', resource: origin + '/mcp',
    code_challenge: 'a'.repeat(43), code_challenge_method: 'S256', state: 'opaque-fixture-state' });
  const cookies = new Map(); let next = `${issuer}/authorize?${params}`, finished = false;
  for (let i = 0; i < 6; i++) {
    const response = await fetch(next, { redirect: 'manual', signal: AbortSignal.timeout(2000),
      headers: { cookie: [...cookies].map(([name, value]) => `${name}=${value}`).join('; ') } });
    for (const value of response.headers.getSetCookie()) {
      const pair = value.split(';', 1)[0], equals = pair.indexOf('=');
      cookies.set(pair.slice(0, equals), pair.slice(equals + 1));
    }
    if (response.status === 200) {
      assert.deepEqual(await response.json(), { prompt: 'consent' }); finished = true; break;
    }
    const location = response.headers.get('location'); await response.text();
    const shapes = observed.map(row => `${row.prompt}:${row.trusted === undefined ? 'absent' : JSON.stringify(row.trusted)}`).join(',');
    assert.ok([302, 303].includes(response.status), `Provider flow status ${response.status}; safe codes ${problems.join(',')}; trusted shapes ${shapes}`);
    assert.ok(location); next = new URL(location, issuer).href;
    assert.equal(new URL(next).origin, origin, 'this regression never reaches the external code callback');
  }
  const second = observed.find(row => row.prompt === 'consent');
  assert.equal(finished, true); assert.ok(second); assert.deepEqual(second.trusted, []);
  assert.equal(second.previousLogin, 'fixture-owner'); assert.deepEqual(problems, []);
  assert.deepEqual(store.find({ model: 'Interaction', id: second.id }).trusted, []);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_credentials').get().n, 0);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_connections').get().n, 0);
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM cap_oauth_artifacts WHERE model NOT IN ('Session','Interaction')").get().n, 0);
});
