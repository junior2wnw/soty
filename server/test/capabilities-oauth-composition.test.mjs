import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';

const CREATE = '/api/capabilities/v1/notes/drafts', HISTORY = '/api/capabilities/v1/invocations/';
const post = (f, token, input) => f.http(CREATE, { method: 'POST', token, body: JSON.stringify(input) });
const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
const jwk = { ...privateKey.export({ format: 'jwk' }), kid: 'host-composition-test', use: 'sig', alg: 'RS256' };
const secrets = () => ({ jwks: { keys: [jwk] }, cookieKeys: [randomBytes(32).toString('base64url')],
  artifactKey: randomBytes(32), artifactKeyId: 'host-composition-test' });

test('configured keyless AS-off serves exact public resource metadata without a Provider session or key', async t => {
  const f = await nativeHttpFixture(t, { capabilitiesVersion: 3, oauth: origin => ({ enabled: false, issuer: `${origin}/oauth` }) });
  assert.deepEqual(f.app.locals.oauthStatus, { enabled: false, issuer: `${f.origin}/oauth` });
  for (const suffix of ['', '/mcp']) {
    const response = await f.http('/.well-known/oauth-protected-resource' + suffix);
    assert.equal(response.status, 200);
    assert.equal(response.body.resource, f.origin + suffix);
    assert.deepEqual(response.body.authorization_servers, [`${f.origin}/oauth`]);
    assert.deepEqual(response.body.scopes_supported, ['notes.createDraft']);
    assert.equal(response.headers['set-cookie'], undefined);
    assert.equal(response.headers['cache-control'], 'no-store');
  }
  for (const pathname of ['/oauth/authorize', '/oauth/token', '/.well-known/oauth-authorization-server/oauth', '/mcp']) {
    const response = await f.http(pathname);
    assert.equal(response.status, 503); assert.equal(response.body.error, 'temporarily_unavailable');
    assert.equal(response.headers['set-cookie'], undefined);
  }
  assert.equal((await f.http('/.well-known/oauth-protected-resource?')).status, 400);
  assert.equal((await f.http('/.well-known/oauth-protected-resource', { method: 'POST' })).status, 405);
  const unauthenticated = await post(f, undefined, { title: 'x', body: '', idempotencyKey: 'unauthenticated' });
  assert.equal(unauthenticated.status, 401);
  assert.equal(unauthenticated.headers['www-authenticate'],
    `Bearer realm="soty", resource_metadata="${f.origin}/.well-known/oauth-protected-resource"`);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_oauth_artifacts').get().n), 0);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n), 0);
});

test('AS-off composition preserves real signed legacy create, own read/replay with execution off and safety revoke', async t => {
  const f = await nativeHttpFixture(t, { capabilitiesVersion: 3, oauth: origin => ({ enabled: false, issuer: `${origin}/oauth` }) });
  const owner = nativeIdentity('Владелец'), account = await f.bootstrap(owner), caller = await f.issue(owner, account.accountId);
  const input = { title: 'Общий результат', body: 'Приватный текст', idempotencyKey: 'configured-as-off-result' };
  const first = await post(f, caller.token, input);
  assert.equal(first.status, 201);
  const id = first.body.invocation.invocationId;
  await f.restart({ enabled: false });
  assert.equal(f.app.locals.capabilitiesApiStatus().notesCreateEnabled, false);
  assert.equal((await f.http(HISTORY + id, { token: caller.token })).status, 200);
  const replay = await post(f, caller.token, input);
  assert.equal(replay.status, 200); assert.equal(replay.body.reused, true);
  assert.deepEqual(replay.body.result, first.body.result);
  assert.equal((await post(f, caller.token, { ...input, idempotencyKey: 'new-disabled-execution' })).status, 503);
  good(await f.call(owner, 'access.principals.revoke', { expectedAccountId: account.accountId, principalId: caller.principalId }));
  assert.ok([401, 403].includes((await f.http(HISTORY + id, { token: caller.token })).status));
  assert.equal(f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 1);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n), 1);
});

test('actual AS startup refuses old or disabled native stores without silently migrating them', async t => {
  for (const [notesVersion, capabilitiesVersion, nativeEnabled] of [[1, 2, true], [2, 2, true], [2, 3, false]]) {
    await t.test(`Notes${notesVersion}/Caps${capabilitiesVersion}/native${nativeEnabled}`, async tt => {
      let enabled = false;
      const f = await nativeHttpFixture(tt, { notesVersion, capabilitiesVersion, enabled: nativeEnabled,
        oauth: origin => ({ enabled, issuer: `${origin}/oauth`, ...(enabled ? secrets() : {}) }) });
      const version = file => f.sql(file, db => db.prepare('PRAGMA user_version').get().user_version);
      const before = [version(f.notesFile), version(f.capsFile)];
      enabled = true;
      await assert.rejects(f.restart({ enabled: nativeEnabled }), error => error?.code === 'oauth_unavailable');
      assert.equal(f.app, undefined, 'failed host is never exposed by the fixture listener');
      assert.deepEqual([version(f.notesFile), version(f.capsFile)], before);
      assert.equal((await f.http('/health')).status, 503);
    });
  }
});
