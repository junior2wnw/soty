import test from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, createHash, generateKeyPairSync, randomBytes, verify } from 'node:crypto';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createTransitAccess, loadTransitAccessConfiguration } from '../transit-access.js';
import { profileFeaturesResponse } from './support/transit-features-fixture.mjs';

const keys = generateKeyPairSync('ed25519'), deliveryKey = Buffer.alloc(32, 7);
const hash = value => createHash('sha256').update(value).digest('hex');
const base = { endpoint: 'http://fixture.invalid/v1/profile', issuer: 'https://soty.example', key: keys.privateKey,
  deliveryKey, appId: 'app-' + 'a'.repeat(32), subscriptionBaseUrl: 'https://soty.example/transit/subscriptions' };
const envelope = (value, body, domain, schema) => {
  const nonce = randomBytes(12), cipher = createCipheriv('aes-256-gcm', deliveryKey, nonce);
  cipher.setAAD(Buffer.concat([Buffer.from(domain), Buffer.from(body)]));
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(value)), cipher.final()]);
  return { schema, nonce: nonce.toString('base64'), tag: cipher.getAuthTag().toString('base64'), ciphertext: ciphertext.toString('base64') };
};
async function fixture(t, options = {}) {
  const actors = ['A', 'B'].map(id => Object.freeze({ accountId: 'account-' + id, deviceId: 'device-' + id }));
  const tokens = new Map(), invalid = new Set(), requests = [], profiles = new Map(); let active = true, generation = 1;
  const service = createTransitAccess({ configuration: base, now: () => 100_000,
    getConnect: () => ({ withActorAuthorityFence(actor, fn) { assert.ok(actors.includes(actor)); if (!active) throw Error('revoked'); return fn(); } }),
    async fetchImpl(url, request) {
      const features = profileFeaturesResponse(url, request, keys.publicKey, deliveryKey); if (features) return features;
      const path = new URL(url).pathname, value = JSON.parse(request.body);
      requests.push({ path, value, body: request.body });
      assert.equal(request.redirect, 'error'); assert.equal(request.cache, 'no-store');
      const fetch = path === '/v1/subscription/profile', action = path.split('/').at(-1);
      const domain = fetch ? 'TRANSIT/SOTY-SUBSCRIPTION-FETCH/V1\0' : `TRANSIT/SOTY-SUBSCRIPTION-${action.toUpperCase()}/V1\0`;
      assert.ok(verify(null, Buffer.concat([Buffer.from(domain), Buffer.from(request.body)]), keys.publicKey, Buffer.from(request.headers['x-transit-signature'], 'base64')));
      assert.equal(value.schema, fetch ? 'soty.transit-subscription-fetch.v1' : `soty.transit-subscription-${action}.v1`);
      assert.equal(value.issuer, base.issuer); assert.equal(value.expiresAt, 120_000); assert.match(value.nonce, /^[a-f0-9]{32}$/);
      if (options.respond) return options.respond({ request, value, path, revoke() { active = false; } });
      if (fetch) {
        assert.equal(value.accountId, undefined); assert.equal(value.deviceId, undefined);
        const owner = [...tokens].find(([, token]) => hash(token) === value.tokenHash && !invalid.has(token))?.[0];
        if (!owner) return Response.json({ error: 'access_subscription_not_found' }, { status: 404 });
        const profile = { outbounds: [{ type: 'vless', uuid: owner, tag: 'entry' }], route: { final: 'entry' }, generation };
        profiles.set(owner, profile);
        return Response.json(envelope({ schema: 'transit.subscription-profile.v1', coreVersion: '1.13.15', profile, sha256: hash(JSON.stringify(profile)) }, request.body,
          'TRANSIT/SUBSCRIPTION-PROFILE-DELIVERY/V1\0', 'transit.encrypted-subscription-profile.v1'));
      }
      let token = tokens.get(value.accountId);
      if (!token || action === 'rotate') { if (token) invalid.add(token); token = randomBytes(32).toString('base64url'); tokens.set(value.accountId, token); }
      if (action === 'revoke') invalid.add(token);
      const revoked = invalid.has(token);
      return Response.json(envelope({ schema: 'transit.personal-subscription.v1', accountId: value.accountId, revoked, token: revoked ? null : token }, request.body,
        'TRANSIT/SUBSCRIPTION-DELIVERY/V1\0', 'transit.encrypted-subscription.v1'));
    } });
  const server = createServer((req, res) => { if (service.handlesSubscription(req)) void service.serveSubscription(req, res); else { res.writeHead(404); res.end(); } });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(async () => { service.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const call = (action = '', actor = actors[0], args = { expectedAccountId: actor.accountId }) => service.executeAsync({ op: 'transit.access.subscription' + action, actor, args });
  const get = (url, init) => fetch(`http://127.0.0.1:${server.address().port}${new URL(url).pathname}`, init);
  return { call, get, service, requests, actors, profiles, advance() { generation++; } };
}

test('ordinary Remote GET returns fresh normal JSON while the private hop carries signed hash-only requests and encrypted replies', async t => {
  const f = await fixture(t), a = (await f.call()).subscription, b = (await f.call('', f.actors[1])).subscription;
  assert.notEqual(a.url, b.url);
  const response = await f.get(a.url);
  assert.equal(response.status, 200); assert.equal(response.headers.get('cache-control'), 'no-store, private');
  assert.equal(response.headers.get('referrer-policy'), 'no-referrer'); assert.equal(response.headers.get('x-content-type-options'), 'nosniff');
  const value = await response.json(); assert.equal(value.outbounds[0].uuid, 'account-A'); assert.equal(value.schema, undefined);
  const token = new URL(a.url).pathname.split('/')[3];
  const request = f.requests.at(-1); assert.equal(request.value.tokenHash, hash(token)); assert.ok(!request.body.includes(token));
  f.advance(); assert.equal((await (await f.get(a.url)).json()).generation, value.generation + 1);
  assert.equal((await f.call()).subscription.url, a.url);
  await assert.rejects(f.call('', f.actors[1], { expectedAccountId: 'account-A' }), { code: 'transit_access_context_changed' });
  const rotated = (await f.call('.rotate')).subscription;
  assert.notEqual(rotated.url, a.url); assert.equal((await f.get(a.url)).status, 404);
  assert.equal((await f.get(rotated.url)).status, 200); await f.call('.revoke');
  assert.equal((await f.get(rotated.url)).status, 404); assert.equal((await f.get(b.url)).status, 200);
  assert.deepEqual((await f.call()).subscription, { url: null, revoked: true });
});

test('malformed paths, methods, rate limits and private failure do not disclose credentials', async t => {
  const f = await fixture(t), { url } = (await f.call()).subscription;
  for (const method of ['HEAD', 'POST']) assert.equal((await f.get(url, { method })).status, 404);
  assert.equal((await f.get(url.replace(/.{43}\/profile/, 'unknown/profile'))).status, 404);
  // All responses, including errors, prohibit client and shared cache storage.
  for (let index = 0; index < 30; index++) assert.equal((await f.get(url)).status, 200);
  const limited = await f.get(url); assert.equal(limited.status, 429); assert.equal(limited.headers.get('retry-after'), '60');
  assert.equal(limited.headers.get('cache-control'), 'no-store, private');
  assert.ok(!(await limited.text()).includes('account-A'));
});

test('a reply for a foreign actor, another request or revoked authority cannot produce a capability URL', async t => {
  for (const mode of ['foreign', 'replay', 'revoked']) {
    const f = await fixture(t, { respond({ request, value, revoke }) {
      if (mode === 'revoked') revoke();
      return Response.json(envelope({ schema: 'transit.personal-subscription.v1', accountId: mode === 'foreign' ? 'account-B' : value.accountId,
        revoked: false, token: 'a'.repeat(43) }, mode === 'replay' ? request.body + ' ' : request.body,
      'TRANSIT/SUBSCRIPTION-DELIVERY/V1\0', 'transit.encrypted-subscription.v1'));
    } });
    await assert.rejects(f.call(), mode === 'revoked' ? /revoked/ : { code: 'transit_access_unavailable' });
  }
});

test('configuration enables only the canonical HTTPS route on this issuer; older configurations remain valid', t => {
  const dir = mkdtempSync(join(tmpdir(), 'soty-subscriptions-')); t.after(() => rmSync(dir, { recursive: true }));
  const file = join(dir, 'access.json'), value = { schema: 'soty.transit-access.v1', endpoint: base.endpoint, issuer: base.issuer,
    privateKey: keys.privateKey.export({ format: 'pem', type: 'pkcs8' }), deliveryKey: deliveryKey.toString('base64'), appId: base.appId };
  const load = changes => { writeFileSync(file, JSON.stringify({ ...value, ...changes })); return loadTransitAccessConfiguration({ SOTY_TRANSIT_ACCESS_CONFIG_FILE: file }); };
  assert.equal(load({}).subscriptionBaseUrl, undefined); assert.equal(load({ subscriptionBaseUrl: base.subscriptionBaseUrl }).subscriptionBaseUrl, base.subscriptionBaseUrl);
  for (const subscriptionBaseUrl of ['http://soty.example/transit/subscriptions', 'https://elsewhere.example/transit/subscriptions', base.subscriptionBaseUrl + '/', base.subscriptionBaseUrl + '?x=1', 1]) {
    assert.throws(() => load({ subscriptionBaseUrl }), { code: 'transit_access_configuration_invalid' });
  }
});

test('an issuer restart reconnects once with a new nonce and the original total deadline', async t => {
  let calls = 0, firstBody, firstSignal;
  const f = await fixture(t, { respond({ request, value }) {
    if (++calls === 1) {
      firstBody = request.body; firstSignal = request.signal;
      throw Object.assign(new TypeError('fetch failed'), { cause: { code: 'UND_ERR_SOCKET' } });
    }
    assert.notEqual(request.body, firstBody); assert.equal(request.signal, firstSignal);
    return Response.json(envelope({ schema: 'transit.personal-subscription.v1', accountId: value.accountId, revoked: false, token: 'a'.repeat(43) }, request.body,
      'TRANSIT/SUBSCRIPTION-DELIVERY/V1\0', 'transit.encrypted-subscription.v1'));
  } });
  assert.ok((await f.call()).subscription.url); assert.equal(calls, 2);
  let failures = 0;
  const unavailable = await fixture(t, { respond() { failures++; throw Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNREFUSED' } }); } });
  await assert.rejects(unavailable.call(), { code: 'transit_access_unavailable' }); assert.equal(failures, 2);
});

test('the actual Root HTTP app serves the private issuer capability route without session or Connect credentials', { timeout: 45000 }, async t => {
  const [{ nativeHttpFixture, nativeIdentity, good }, { createHttpApp }] = await Promise.all([
    import('./support/native-capability-http.mjs'), import('../http-app.js'),
  ]);
  const token = randomBytes(32).toString('base64url'); let owner;
  const issuer = createServer(async (req, res) => {
    const parts = []; for await (const part of req) parts.push(part);
    const body = Buffer.concat(parts).toString(), value = JSON.parse(body), management = req.url === '/v1/subscription/get';
    if (req.url === '/v1/features') {
      const response = profileFeaturesResponse('http://fixture.invalid/v1/features', { body, headers: req.headers }, keys.publicKey, deliveryKey);
      res.writeHead(200, { 'content-type': 'application/json' }); res.end(await response.text()); return;
    }
    const domain = management ? 'TRANSIT/SOTY-SUBSCRIPTION-GET/V1\0' : 'TRANSIT/SOTY-SUBSCRIPTION-FETCH/V1\0';
    assert.ok(verify(null, Buffer.concat([Buffer.from(domain), Buffer.from(body)]), keys.publicKey, Buffer.from(req.headers['x-transit-signature'], 'base64')));
    if (management) owner = value.accountId;
    else assert.equal(value.tokenHash, hash(token));
    const profile = { outbounds: [{ type: 'vless', uuid: owner, tag: 'entry' }], route: { final: 'entry' } };
    const reply = management ? { schema: 'transit.personal-subscription.v1', accountId: value.accountId, revoked: false, token }
      : { schema: 'transit.subscription-profile.v1', coreVersion: '1.13.15', profile, sha256: hash(JSON.stringify(profile)) };
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(envelope(reply, body, management ? 'TRANSIT/SUBSCRIPTION-DELIVERY/V1\0' : 'TRANSIT/SUBSCRIPTION-PROFILE-DELIVERY/V1\0',
      management ? 'transit.encrypted-subscription.v1' : 'transit.encrypted-subscription-profile.v1')));
  });
  issuer.listen(0, '127.0.0.1'); await once(issuer, 'listening');
  t.after(() => new Promise(resolve => issuer.close(resolve)));
  const root = await nativeHttpFixture(t, { appFactory(dist, options) {
    writeFileSync(join(options.dataDir, 'transit-access.json'), JSON.stringify({ schema: 'soty.transit-access.v1',
      endpoint: `http://127.0.0.1:${issuer.address().port}/v1/profile`, issuer: base.issuer,
      privateKey: keys.privateKey.export({ format: 'pem', type: 'pkcs8' }), deliveryKey: deliveryKey.toString('base64'),
      appId: base.appId, subscriptionBaseUrl: base.subscriptionBaseUrl }));
    return createHttpApp(dist, options);
  } });
  const person = nativeIdentity('Remote profile owner'), account = await root.bootstrap(person);
  const managed = good(await root.call(person, 'transit.access.subscription', { expectedAccountId: account.accountId }));
  const response = await root.http(new URL(managed.subscription.url).pathname);
  assert.equal(response.status, 200); assert.equal(response.body.outbounds[0].uuid, account.accountId);
  assert.equal(response.headers['set-cookie'], undefined); assert.equal(response.headers['cache-control'], 'no-store, private');
});
