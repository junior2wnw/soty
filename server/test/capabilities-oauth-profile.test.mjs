import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { connect } from 'node:net';
import { createOAuthHostProfile, isRegisteredOAuthRedirect } from '../capabilities-oauth-profile.js';
import { nativeHttpFixture } from './support/native-capability-http.mjs';

const origin = 'https://soty-profile.test', issuer = origin + '/oauth';
const trusted = { audience: origin, shellOrigins: [origin] };
const fault = error => error?.code === 'oauth_configuration_invalid';

test('host profile validates canonical trust and keeps disabled keyless resource binding explicit', () => {
  assert.equal(createOAuthHostProfile(undefined, trusted), null);
  const profile = createOAuthHostProfile({ enabled: false, issuer }, trusted);
  assert.equal(profile.enabled, false); assert.equal(profile.secure, true);
  assert.deepEqual(profile.resources, { http: origin, mcp: origin + '/mcp' });
  assert.equal(Object.hasOwn(profile.domainConfiguration(action => action()), 'artifactKey'), false);
  assert.throws(() => profile.providerKeys(), fault);
  assert.deepEqual(profile.protectedResource(origin).authorization_servers, [issuer]);
  assert.deepEqual(profile.protectedResource(origin + '/mcp').scopes_supported, ['notes.createDraft']);
  assert.throws(() => profile.protectedResource('https://other.test'), fault);
  for (const options of [null, {}, { enabled: true, issuer }, { enabled: 1, issuer },
    { enabled: false, issuer: issuer + '/' }, { enabled: false, issuer: 'https://SOTY-PROFILE.test/oauth' },
    { enabled: false, issuer, audience: 'https://other.test' }, { enabled: false, issuer, cookieKeys: [] }]) {
    assert.throws(() => createOAuthHostProfile(options, trusted), fault);
  }
  assert.throws(() => createOAuthHostProfile({ enabled: false, issuer }, { ...trusted, audience: origin + '/mcp' }), fault);
  assert.throws(() => createOAuthHostProfile({ enabled: false, issuer }, { ...trusted, shellOrigins: [] }), fault);
  assert.throws(() => createOAuthHostProfile({ enabled: false, issuer: 'http://soty-profile.test/oauth' },
    { audience: 'http://soty-profile.test', shellOrigins: ['http://soty-profile.test'] }), fault);
});

test('enabled profile captures configured private material without exposing it in public metadata', () => {
  const privateJwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(privateJwk, { kid: 'test-signing', alg: 'RS256', use: 'sig' });
  const config = { enabled: true, issuer, jwks: { keys: [privateJwk] },
    cookieKeys: [randomBytes(32).toString('base64url')], artifactKey: randomBytes(32), artifactKeyId: 'test-artifacts' };
  const profile = createOAuthHostProfile(config, trusted);
  const captured = profile.domainConfiguration(action => action());
  const originalKey = Buffer.from(captured.artifactKey), originalKid = profile.providerKeys().jwks.keys[0].kid;
  config.artifactKey.fill(0); privateJwk.kid = 'changed'; config.cookieKeys.length = 0;
  assert.equal(Buffer.compare(profile.domainConfiguration(action => action()).artifactKey, originalKey), 0);
  assert.equal(profile.providerKeys().jwks.keys[0].kid, originalKid);
  captured.artifactKey.fill(1);
  assert.equal(Buffer.compare(profile.domainConfiguration(action => action()).artifactKey, originalKey), 0);
  const publicText = JSON.stringify({ profile, clients: profile.clients(), metadata: profile.protectedResource(origin) });
  assert.equal(publicText.includes('test-artifacts'), false);
  assert.equal(publicText.includes(originalKey.toString('base64')), false);
  assert.equal(publicText.includes(privateJwk.d), false);
  assert.equal(profile.clients().length, 2);
});

test('registered callbacks accept only the observed static client path and explicit IPv4 loopback port', () => {
  for (const [clientId, path] of [['soty-codex-cli', '/callback'], ['soty-opencode-cli', '/mcp/oauth/callback']]) {
    for (const port of [1, 80, 19876, 55943, 65535]) assert.equal(isRegisteredOAuthRedirect({ clientId, redirectUri: `http://127.0.0.1:${port}${path}` }), true);
    for (const uri of [`http://127.0.0.1${path}`, `http://127.0.0.1:0${path}`, `http://127.0.0.1:65536${path}`,
      `http://127.0.0.1:0123${path}`, `http://localhost:19876${path}`, `http://[::1]:19876${path}`,
      `https://127.0.0.1:19876${path}`, `http://127.0.0.1:19876${path}?q=1`, `http://127.0.0.1:19876${path}#fragment`,
      `http://user@127.0.0.1:19876${path}`, `http://127.0.0.1:19876${path}/suffix`,
      `http://127.0.0.1:19876${path.replace('callback', '%63allback')}`]) {
      assert.equal(isRegisteredOAuthRedirect({ clientId, redirectUri: uri }), false);
    }
  }
  assert.equal(isRegisteredOAuthRedirect({ clientId: 'other', redirectUri: 'http://127.0.0.1:19876/callback' }), false);
  assert.equal(isRegisteredOAuthRedirect({ clientId: 'soty-codex-cli', redirectUri: 'http://127.0.0.1:19876/mcp/oauth/callback' }), false);
});

test('actual disabled host reserves OAuth and MCP paths without returning the SPA or discovery promises', async t => {
  const f = await nativeHttpFixture(t, { enabled: false, capabilitiesVersion: 3 });
  for (const route of ['/oauth', '/oauth/authorize', '/oauth/unknown', '/mcp', '/mcp/unknown',
    '/.well-known/oauth-protected-resource', '/.well-known/oauth-protected-resource/mcp',
    '/.well-known/oauth-authorization-server/oauth', '/oauth/.well-known/openid-configuration',
    '/%6fAuth/authorize', '/%256fauth/authorize']) {
    const response = await fetch(f.origin + route, { redirect: 'manual' });
    assert.equal(response.status, 503, route); assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.match(response.headers.get('content-type'), /^application\/json/u);
    assert.deepEqual(await response.json(), { error: 'temporarily_unavailable' });
    assert.equal(response.headers.has('set-cookie'), false);
  }
  const metadataPost = await fetch(f.origin + '/oauth/token', { method: 'POST' });
  assert.equal(metadataPost.status, 503);
  const ordinary = await fetch(f.origin + '/api/apps/capabilities');
  assert.equal(ordinary.status, 200); assert.equal(Object.hasOwn(await ordinary.json(), 'configured'), true);
});

test('disabled OAuth rejects an unfinished POST and closes its connection without draining the declared body', async t => {
  const f = await nativeHttpFixture(t, { enabled: false, capabilitiesVersion: 3 });
  const target = new URL(f.origin), socket = connect(Number(target.port), '127.0.0.1');
  t.after(() => socket.destroy());
  const result = new Promise((resolve, reject) => {
    let response = '';
    const deadline = setTimeout(() => { socket.destroy(); reject(new Error('disabled OAuth kept an unfinished request alive')); }, 2000);
    socket.setEncoding('utf8');
    socket.on('data', data => { response += data; assert.ok(response.length < 16384); });
    socket.once('error', error => { clearTimeout(deadline); reject(error); });
    socket.once('close', () => { clearTimeout(deadline); resolve(response); });
    socket.once('connect', () => socket.write(`POST /oauth/token HTTP/1.1\r\nHost: ${target.host}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 1048576\r\nConnection: keep-alive\r\n\r\nx`));
  });
  const response = await result;
  assert.match(response, /^HTTP\/1\.1 503 /u);
  assert.match(response, /\r\nconnection: close\r\n/iu);
});
