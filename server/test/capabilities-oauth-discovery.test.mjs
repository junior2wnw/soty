import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { oauthNativeFixture, nativeIdentity } from './support/oauth-native-http.mjs';

test('RFC 8414 alias advertises mounted endpoints which complete real authorization, native execution and revocation', async t => {
  const f = await oauthNativeFixture(t);
  const resource = await f.wire.request('/.well-known/oauth-protected-resource');
  assert.equal(resource.status, 200);
  assert.deepEqual(resource.body.authorization_servers, [f.issuer]);
  const discovery = await f.wire.request('/.well-known/oauth-authorization-server/oauth');
  const canonical = await f.wire.request('/oauth/.well-known/openid-configuration');
  assert.equal(discovery.status, 200); assert.equal(canonical.status, 200);
  assert.deepEqual(discovery.body, canonical.body, 'both discovery routes describe the same mounted provider');
  const metadata = discovery.body;
  assert.equal(metadata.issuer, f.issuer);
  assert.deepEqual(metadata.scopes_supported, ['notes.createDraft']);
  assert.deepEqual(metadata.response_modes_supported, ['query']);
  assert.deepEqual(metadata.token_endpoint_auth_methods_supported, ['none']);
  for (const [name, suffix] of Object.entries({ authorization_endpoint: '/authorize', token_endpoint: '/token',
    revocation_endpoint: '/revoke', jwks_uri: '/jwks' })) {
    assert.equal(metadata[name], f.issuer + suffix, 'advertised endpoint retains the issuer mount: ' + name);
  }
  const keys = await f.wire.request(metadata.jwks_uri);
  assert.equal(keys.status, 200); assert.match(keys.headers.get('content-type'), /^application\/jwk-set\+json/u);
  const jwks = JSON.parse(keys.text);
  assert.ok(Array.isArray(jwks.keys) && jwks.keys.length > 0);
  assert.ok(jwks.keys.every(key => key.d === undefined), 'discovery returns public keys only');

  const actor = nativeIdentity('Владелец обнаруженного клиента'), account = await f.bootstrap(actor);
  const client = 'soty-codex-cli', redirectUri = 'http://127.0.0.1:19876/callback';
  const verifier = randomBytes(32).toString('base64url'), state = randomBytes(24).toString('base64url'), session = f.browser();
  const query = new URLSearchParams({ client_id: client, redirect_uri: redirectUri, response_type: 'code', resource: f.origin,
    response_mode: metadata.response_modes_supported[0],
    scope: metadata.scopes_supported.join(' '), code_challenge_method: 'S256', code_challenge: createHash('sha256').update(verifier).digest('base64url'), state });
  for (const mode of ['form_post', 'fragment']) {
    const unsupported = new URLSearchParams(query); unsupported.set('response_mode', mode);
    assert.equal((await session.request(metadata.authorization_endpoint + '?' + unsupported)).status, 400);
  }
  const start = await session.request(metadata.authorization_endpoint + '?' + query);
  assert.ok([302, 303].includes(start.status) && start.location?.origin === f.origin, 'discovered authorize reaches this issuer');
  const pathname = start.location.pathname;
  assert.ok(/^\/oauth\/interaction\/[A-Za-z0-9_-]{16,128}$/u.test(pathname), 'only the mounted consent is followed');
  assert.equal((await session.request(pathname)).status, 200);
  const context = await session.request(pathname + '/context'); assert.equal(context.status, 200);
  const flow = await f.complete(await f.decide({ client, redirectUri, verifier, state, session, pathname,
    resource: f.origin, context: context.body }, actor, account.accountId));
  const tokens = f.tokens(await f.wire.request(metadata.token_endpoint, { cookies: false, originHeader: null,
    fields: { grant_type: 'authorization_code', client_id: client, redirect_uri: redirectUri, resource: f.origin,
      code: flow.code, code_verifier: verifier } }));
  const create = () => f.http('/api/capabilities/v1/notes/drafts', { method: 'POST', token: tokens.access_token,
    body: JSON.stringify({ title: 'Discovered client', body: 'Real endpoints', idempotencyKey: 'discovery-mount-native' }) });
  assert.equal((await create()).status, 201);
  const revoke = await f.wire.request(metadata.revocation_endpoint, { cookies: false, originHeader: null,
    fields: { client_id: client, token: tokens.refresh_token, token_type_hint: 'refresh_token' } });
  assert.equal(revoke.status, 200);
  assert.ok([401, 403].includes((await create()).status), 'discovered revocation actually removes authority');
});
