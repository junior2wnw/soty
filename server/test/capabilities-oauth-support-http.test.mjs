import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import { createHash, generateKeyPairSync, randomBytes } from 'node:crypto';
import express from 'express';
import path from 'node:path';
import { writeFileSync } from 'node:fs';
import { createOAuthHostProfile } from '../capabilities-oauth-profile.js';
import { createSotyOAuthProvider } from '../capabilities-oauth-provider.js';
import { attachCapabilitiesOAuth } from '../capabilities-oauth.js';
import { fixture as artifactFixture } from '../../modules/capabilities/test/support/oauth-artifacts.mjs';

// Node's fetch implementation normalises Host. Use the actual HTTP wire API
// for an edge-to-origin request with an explicitly selected canonical Host.
function proxyRequest(url, headers) {
  return new Promise((resolve, reject) => {
    const req = httpRequest(url, { headers }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(new Response(Buffer.concat(chunks), { status: res.statusCode,
        headers: Object.entries(res.headers).flatMap(([name, value]) => (Array.isArray(value) ? value : [value]).map(item => [name, item])) })));
      res.on('error', reject);
    }); req.on('error', reject); req.end();
  });
}

// Real HTTP + pinned production Provider wrapper + encrypted SQLite support
// store. This facade enables ONLY the auxiliary seam: no production readiness,
// owner approval, bound Grant/token, Connect authority or CLI proof is claimed.
async function fixture(t, { hosted = false, secure = false, trustedProxy = false, html = false } = {}) {
  let app;
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  t.after(async () => { server.closeAllConnections(); await new Promise(done => server.close(done)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  const origin = secure ? 'https://consent.soty.test' : baseUrl;
  const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(key, { kid: 'http-support-test', alg: 'RS256', use: 'sig' });
  const profile = createOAuthHostProfile({ enabled: true, issuer: origin + '/oauth', jwks: { keys: [key] },
    cookieKeys: [randomBytes(32).toString('base64url')], artifactKey: randomBytes(32), artifactKeyId: 'http-support-test' },
  { shellOrigins: [origin], audience: origin });
  const f = artifactFixture(t, { file: html, configuration: profile.domainConfiguration(() => assert.fail('no authority fence expected')),
    clock: Date.now });
  const store = f.store();
  const presentations = new Map();
  const distDir = html ? path.dirname(f.databasePath) : undefined;
  if (html) writeFileSync(path.join(distDir, 'index.html'), '<!doctype html><title>synthetic consent shell</title>');
  const oauth = {
    readiness: () => ({ available: true }), artifactStore: store,
    beginGrantBinding: () => assert.fail('no consent/binding in this support test'),
    // Only the browser binding/HTTP lifecycle is under test; this is expressly
    // not the signed owner-decision domain coordinator.
    prepareInteraction({ interactionId, browserNonce }) {
      const payload = store.find({ model: 'Interaction', id: interactionId }); assert.ok(payload);
      const prior = presentations.get(interactionId);
      if (prior) assert.equal(prior.nonce === browserNonce, true, 'the existing presentation retains its nonce');
      else presentations.set(interactionId, { nonce: browserNonce, payload });
    },
    readInteraction({ interactionId, browserNonce }) {
      const entry = presentations.get(interactionId); assert.ok(entry);
      assert.equal(entry.nonce === browserNonce, true, 'presentation cookie must match');
      return { interactionId, clientProfile: entry.payload.params.client_id, decision: 'pending',
        checkedAt: Date.now(), expiresAt: entry.payload.exp * 1000 };
    },
  };
  app = express();
  app.use((_req, res, next) => { res.set('Content-Security-Policy', "default-src 'self'; form-action 'self'; frame-ancestors 'none'"); next(); });
  if (trustedProxy) app.set('trust proxy', 'loopback');
  if (hosted) attachCapabilitiesOAuth(app, { profile, service: { oauth }, distDir });
  else app.use('/oauth', createSotyOAuthProvider({ profile, oauth }).provider.callback());
  const authorize = () => {
    const state = randomBytes(24).toString('base64url'), verifier = randomBytes(32).toString('base64url');
    return { state, query: new URLSearchParams({ client_id: 'soty-codex-cli', redirect_uri: 'http://127.0.0.1:53736/callback',
      response_type: 'code', scope: 'notes.createDraft', resource: origin + '/mcp', state,
      code_challenge_method: 'S256', code_challenge: createHash('sha256').update(verifier).digest('base64url') }) };
  };
  return { ...f, origin, baseUrl, store, authorize, presentations };
}

test('actual initial authorize persists encrypted Interaction without authority or client request context', async t => {
  const f = await fixture(t), request = f.authorize();
  const response = await fetch(`${f.origin}/oauth/authorize?${request.query}`, { redirect: 'manual' });
  assert.ok([302, 303].includes(response.status));
  const location = new URL(response.headers.get('location'), f.origin);
  assert.equal(location.searchParams.get('error'), null, 'initial authorization must reach consent');
  assert.equal(location.origin, f.origin);
  assert.match(location.pathname, /^\/oauth\/interaction\/[A-Za-z0-9_-]{16,128}$/u);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  await response.text();
  const rows = f.db.prepare('SELECT * FROM cap_oauth_artifacts ORDER BY model').all();
  // The pinned provider does not save a new untouched Session before login.
  assert.deepEqual(rows.map(row => row.model), ['Interaction']);
  for (const row of rows) {
    assert.equal(row.connection_id, null); assert.equal(row.provider_grant_id, null);
    assert.equal(Buffer.from(row.payload_cipher).includes(Buffer.from(request.state)), false);
  }
  const interactionId = location.pathname.split('/').at(-1);
  const payload = f.store.find({ model: 'Interaction', id: interactionId });
  assert.equal(payload.params.state === request.state, true);
  assert.equal(payload.returnTo, `${f.origin}/oauth/authorize/${interactionId}`);
  for (const table of ['cap_clients', 'cap_grants', 'cap_credentials', 'cap_oauth_connections']) {
    assert.equal(f.db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0);
  }
});

test('actual provider ingress errors remain bounded JSON/no-store instead of escaping to Koa plain500', async t => {
  const f = await fixture(t);
  const cases = [
    { body: 'a=1&a=2', type: 'application/x-www-form-urlencoded', status: 400 },
    { body: 'a=%GG', type: 'application/x-www-form-urlencoded', status: 400 },
    { body: '{}', type: 'application/json', status: 415 },
    { body: 'a=' + 'x'.repeat(16384), type: 'application/x-www-form-urlencoded', status: 413 },
  ];
  for (const entry of cases) {
    const response = await fetch(f.origin + '/oauth/token', { method: 'POST',
      headers: { 'Content-Type': entry.type }, body: entry.body, redirect: 'manual' });
    assert.equal(response.status, entry.status); assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.match(response.headers.get('content-type'), /^application\/json/u);
    assert.deepEqual(await response.json(), { error: 'invalid_request' });
  }
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_artifacts').get().n, 0);
});

test('actual trusted proxy establishes the canonical HTTPS issuer while forged forwarded host is ignored', async t => {
  const f = await fixture(t, { hosted: true, secure: true, trustedProxy: true });
  const response = await proxyRequest(`${f.baseUrl}/oauth/authorize?${f.authorize().query}`,
    { Host: 'consent.soty.test', 'X-Forwarded-Proto': 'https', 'X-Forwarded-Host': 'untrusted.invalid',
      Forwarded: 'for=127.0.0.1;proto=http;host=untrusted.invalid' });
  assert.ok([302, 303].includes(response.status));
  const location = new URL(response.headers.get('location'));
  assert.equal(location.searchParams.get('error'), null);
  assert.equal(location.origin, f.origin); assert.match(location.pathname, /^\/oauth\/interaction\//u);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  const cookies = response.headers.getSetCookie();
  assert.ok(cookies.length > 0 && cookies.every(cookie => /; secure(?:;|$)/iu.test(cookie)));
  assert.equal(cookies.some(cookie => cookie.includes('untrusted.invalid')), false);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_artifacts').get().n, 1);
  await response.text();
});

test('actual untrusted transport cannot spoof HTTPS with forwarding headers or reach the provider', async t => {
  const f = await fixture(t, { hosted: true, secure: true });
  const response = await proxyRequest(`${f.baseUrl}/oauth/authorize?${f.authorize().query}`,
    { Host: 'consent.soty.test', 'X-Forwarded-Proto': 'https', Forwarded: 'proto=https;host=consent.soty.test' });
  assert.equal(response.status, 403); assert.deepEqual(await response.json(), { error: 'access_denied' });
  assert.equal(response.headers.get('cache-control'), 'no-store'); assert.equal(response.headers.getSetCookie().length, 0);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_artifacts').get().n, 0);
});

test('a fresh consent near the old browser-cookie expiry preserves its binding for its own interaction window', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 1800000000000 });
  const f = await fixture(t, { hosted: true, html: true }), jar = new Map();
  function save(response) {
    for (const cookie of response.headers.getSetCookie()) {
      const [pair, ...attributes] = cookie.split(';').map(item => item.trim());
      const at = pair.indexOf('='), name = pair.slice(0, at);
      const fields = Object.fromEntries(attributes.map(item => { const i = item.indexOf('='); return [item.slice(0, i < 0 ? undefined : i).toLowerCase(), i < 0 ? '' : item.slice(i + 1)]; }));
      const cookiePath = fields.path || '/';
      jar.set(name + cookiePath, { pair, path: cookiePath, expiresAt: fields['max-age'] === undefined ? Infinity : Date.now() + Number(fields['max-age']) * 1000 });
    }
  }
  async function get(url) {
    const pathname = new URL(url).pathname;
    const cookie = [...jar.values()].filter(item => item.expiresAt > Date.now()
      && (pathname === item.path || pathname.startsWith(item.path.endsWith('/') ? item.path : item.path + '/'))).map(item => item.pair).join('; ');
    const response = await fetch(url, { redirect: 'manual', headers: cookie ? { Cookie: cookie } : {} });
    save(response); return response;
  }
  async function open() {
    const response = await get(`${f.origin}/oauth/authorize?${f.authorize().query}`);
    assert.ok([302, 303].includes(response.status));
    const location = new URL(response.headers.get('location')); assert.equal(location.origin, f.origin);
    const page = await get(location.href); assert.equal(page.status, 200);
    assert.ok(page.headers.get('content-security-policy').includes("form-action 'self' http://127.0.0.1:53736/callback"));
    await page.text(); await response.text(); return location.href;
  }
  const first = await open(); t.mock.timers.tick(590000);
  const second = await open(); assert.notEqual(first, second);
  const values = [...f.presentations.values()]; assert.equal(values[0].nonce === values[1].nonce, true);
  t.mock.timers.tick(20000);
  const context = await get(second + '/context');
  assert.equal(context.status, 200, 'a new interaction must not lose the still-bound cookie at the old cookie deadline');
  const view = await context.json(); assert.equal(view.expiresAt - view.checkedAt, 580000);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_oauth_connections').get().n, 0);
});
