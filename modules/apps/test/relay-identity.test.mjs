import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createAppsService } from '../server/index.mjs';
import { createRelayIdentitySigner } from '../../human-identity/source-authority.mjs';
import { createRelayIdentityVerifier } from '../../source-app/server/relay-identity.mjs';
import { createRelayIdentityFetch } from '../../source-app/browser/relay-identity-fetch.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';

const secret = () => randomBytes(32).toString('base64url');
const hash = value => createHash('sha256').update(value).digest('hex');
async function until(check) {
  const end = Date.now() + 5000;
  while (!check()) { if (Date.now() >= end) throw Error('fixture_timeout'); await delay(10); }
}
function http(port, path, { origin, cookie, method = 'GET', headers = {}, body } = {}) {
  const url = new URL(origin);
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path, method, headers: { host: url.host,
      ...(cookie ? { cookie } : {}), ...(method === 'GET' ? {} : { origin }), ...headers } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.on('end', () => resolve({ status: res.statusCode,
        headers: res.headers, text: Buffer.concat(chunks).toString() }));
    });
    req.on('error', reject); req.end(body);
  });
}

test('signed Root identity is explicitly requested, isolated by app/person, and stops at revocation', { timeout: 20000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-relay-identity-'));
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const privateJwk = { ...pair.privateKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const publicJwk = { ...pair.publicKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const issuer = 'https://soty.example/human-identity';
  const profile = { enabled: true, issuer, providerKeys: () => ({ jwks: { keys: [privateJwk] } }) };
  const actualSigner = createRelayIdentitySigner({ profile });
  let gate, failSigner = false, service, runtime;
  const root = createServer((req, res) => service?.handleRequest(req, res) || res.writeHead(404).end());
  root.on('upgrade', (req, socket, head) => { if (!service.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => root.listen(0, '127.0.0.1', done));
  const port = root.address().port, rootOrigin = `http://127.0.0.1:${port}`;
  const owner = { accountId: 'alice', deviceId: 'alice-device' }, bob = { accountId: 'bob', deviceId: 'bob-device' };
  const revoked = new Set();
  service = createAppsService({ databasePath: join(directory, 'apps.sqlite'), shellOrigins: [rootOrigin],
    appOriginTemplate: `http://{appId}.localhost:${port}`, namedAppZone: `http://apps.localhost:${port}`,
    actorActive: actor => [owner, bob].some(person => person.accountId === actor?.accountId && person.deviceId === actor?.deviceId) && !revoked.has(actor.deviceId),
    canAccessCommunity: () => false, isGroupAdmin: () => false, authenticateConnector: async () => true,
    identitySignTimeoutMs: 300,
    signRelayIdentity: async input => { if (failSigner) throw Object.assign(Error('synthetic'), { code: 'app_identity_unavailable' });
      const proof = await actualSigner(input); if (gate) { gate.enter(); await gate.wait; } return proof; } });
  const identity = { linkId: 'link', hostDeviceId: 'host', connectorId: 'connector', name: 'fixture' };
  runtime = createLocalAppsRuntime({ randomSecret: secret, digest: hash, createWebSocket: url => new WebSocket(url), httpRequest: request,
    encodeBase64: value => Buffer.from(value).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
  { serverUrl: rootOrigin, identity, token: 'fixture' });
  const servers = [];
  t.after(async () => { gate?.release(); runtime.stop(); service.close(); root.closeAllConnections();
    await new Promise(done => root.close(done)); for (const server of servers) { server.closeAllConnections(); await new Promise(done => server.close(done)); }
    await rm(directory, { recursive: true, force: true }); });
  runtime.start(); await until(() => runtime.status().connected);
  const claim = await runtime.claim();
  const call = (op, args, actor = owner) => service.execute({ op, actor, args: { ...args, expectedAccountId: actor.accountId } });
  call('apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode: claim.claimCode });
  const apps = [];
  for (let index = 0; index < 2; index++) {
    let verifier; const seen = [], records = new Map();
    const upstream = createServer((req, res) => {
      if (req.method === 'HEAD') { res.writeHead(200).end(); return; }
      seen.push({ method: req.method, path: req.url, token: req.headers['x-soty-identity'] });
      void (async () => {
        try { const person = await verifier(req); if (req.method === 'POST') records.set(person.subject, (records.get(person.subject) ?? 0) + 1);
          res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ subject: person.subject, count: records.get(person.subject) ?? 0 })); }
        catch { res.writeHead(401).end('identity required'); }
      })();
    });
    await new Promise(done => upstream.listen(0, '127.0.0.1', done)); servers.push(upstream);
    const app = call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId,
      name: 'fixture ' + index, port: upstream.address().port, entryPath: '/', grants: { accountIds: [bob.accountId] } }).app;
    verifier = createRelayIdentityVerifier({ issuer, appId: app.id, fetch: async url => {
      assert.equal(new URL(url).href, issuer + '/jwks'); return new Response(JSON.stringify({ keys: [publicJwk] }), { headers: { 'content-type': 'application/json' } });
    } });
    apps.push({ app, seen, records, verifier });
  }
  await until(() => runtime.status().apps === 2);
  async function session(app, actor) {
    const result = call('apps.launch', { appId: app.id }, actor), url = new URL(result.launchUrl);
    const boot = await http(port, '/_soty/session', { origin: url.origin, method: 'POST',
      headers: { 'content-type': 'application/json' }, body: JSON.stringify({ ticket: url.hash.slice(1) }) });
    assert.equal(boot.status, 200); return { origin: url.origin, cookie: boot.headers['set-cookie'][0].split(';')[0] };
  }
  let aliceSession, bobSession, otherSession;
  await until(() => { try { aliceSession = call('apps.launch', { appId: apps[0].app.id }, owner); return true; } catch { return false; } });
  aliceSession = await session(apps[0].app, owner); bobSession = await session(apps[0].app, bob); otherSession = await session(apps[1].app, owner);
  const fetchApp = (session, method = 'GET', headers = {}) => createRelayIdentityFetch({ origin: session.origin,
    fetch: (path, options) => http(port, path, { ...session, method: options.method, headers: Object.fromEntries(options.headers) })
  })('/items?x=1', { method, headers });
  assert.equal((await http(port, '/items?x=1', aliceSession)).status, 401);
  assert.equal(apps[0].seen.at(-1).token, undefined, 'identity is never sent on an ordinary request');
  const alice = await fetchApp(aliceSession, 'POST', { 'x-soty-identity': 'browser-forgery' });
  assert.equal(alice.status, 200, JSON.stringify({ text: alice.text, upstreamSawProof: !!apps[0].seen.at(-1)?.token })); assert.deepEqual(JSON.parse(alice.text), { subject: 'alice', count: 1 });
  assert.equal(alice.text.includes(apps[0].seen.at(-1).token), false);
  assert.deepEqual(JSON.parse((await fetchApp(bobSession)).text), { subject: 'bob', count: 0 });
  assert.deepEqual(JSON.parse((await fetchApp(otherSession)).text), { subject: 'alice', count: 0 });
  const normalized = await http(port, '/folder/../items?x=1', { ...aliceSession, headers: { 'x-soty-identity-request': '1' } });
  assert.equal(normalized.status, 200); assert.equal(apps[0].seen.at(-1).path, '/items?x=1');
  const old = apps[0].seen.find(value => value.method === 'POST').token;
  const replay = { headers: { 'x-soty-identity': old }, rawHeaders: ['x-soty-identity', old], method: 'POST', url: '/items?x=1' };
  await assert.rejects(apps[0].verifier(replay), { code: 'soty_identity_invalid' });
  await assert.rejects(apps[1].verifier(replay), { code: 'soty_identity_invalid' });
  const fresh = await actualSigner({ appId: apps[0].app.id, accountId: 'alice', method: 'GET', path: '/items?x=1', expiresAt: Date.now() + 30000 });
  const changed = { ...replay, method: 'POST', headers: { 'x-soty-identity': fresh }, rawHeaders: ['x-soty-identity', fresh] };
  await assert.rejects(apps[0].verifier(changed), { code: 'soty_identity_invalid' });
  await assert.rejects(apps[0].verifier({ ...changed, headers: { 'x-soty-identity': 'unsigned-json' } }), { code: 'soty_identity_invalid' });

  let unchanged = apps[0].seen.length; failSigner = true;
  assert.equal((await fetchApp(aliceSession)).status, 503); failSigner = false;
  assert.equal(apps[0].seen.length, unchanged, 'signer errors finish without dispatch');
  let enterTimeout, releaseTimeout;
  gate = { entered: new Promise(done => { enterTimeout = done; }), wait: new Promise(done => { releaseTimeout = done; }),
    enter: () => enterTimeout(), release: () => releaseTimeout() };
  const timed = fetchApp(aliceSession); await gate.entered; assert.equal((await timed).status, 503);
  gate.release(); gate = null; await delay(30);
  assert.equal(apps[0].seen.length, unchanged, 'late signer completion cannot dispatch a timed-out request');
  let enter, release; gate = { entered: new Promise(done => { enter = done; }), wait: new Promise(done => { release = done; }), enter: () => enter(), release: () => release() };
  const before = apps[0].seen.length, pending = fetchApp(bobSession, 'POST'); await gate.entered;
  call('apps.update', { appId: apps[0].app.id, grants: {} }); gate.release(); gate = null;
  const denied = await pending; assert.equal(denied.status, 403); assert.equal(apps[0].seen.length, before, 'revocation during signing never dispatches the write');

  const domains = call('apps.domains.get', { appId: apps[0].app.id });
  const aliasId = call('apps.domains.claim', { appId: apps[0].app.id, slug: 'relay-auth-fixture',
    expectedDomainsRevision: domains.revision, requestId: 'claim-relay-auth' }).receipt.domainId;
  const alias = call('apps.domains.get', { appId: apps[0].app.id }).domains.find(value => value.id === aliasId);
  const publication = call('apps.publication.get', { appId: apps[0].app.id });
  call('apps.publication.update', { appId: apps[0].app.id, requestId: 'publish-relay-auth', expectedPolicyEpoch: publication.policyEpoch,
    expectedTargetRevision: publication.activeTargetRevision, launchPolicy: 'anyone', listed: false, activeDomainIds: [alias.id],
    exposureAck: { scope: 'whole-port', targetRevision: publication.target.revision, targetDigest: publication.target.digest, profile: publication.target.profile } });
  const publicBefore = apps[0].seen.length;
  assert.equal((await fetchApp({ origin: alias.origin })).status, 401);
  assert.equal(apps[0].seen.length, publicBefore, 'anonymous access never receives an identity proof');
});

test('browser helper cannot send the request flag to another origin or follow a redirect', async () => {
  let calls = 0, captured;
  const sotyFetch = createRelayIdentityFetch({ origin: 'https://app.example', fetch: async (path, options) => { calls++; captured = { path, options }; return new Response(); } });
  await sotyFetch('/items?x=1', { headers: { accept: 'application/json' } });
  assert.equal(captured.path, '/items?x=1'); assert.equal(captured.options.headers.get('x-soty-identity-request'), '1');
  assert.equal(captured.options.credentials, 'same-origin'); assert.equal(captured.options.redirect, 'error');
  assert.throws(() => sotyFetch('https://other.example/items'), /invalid_soty_request/u);
  assert.throws(() => sotyFetch('/items', { mode: 'no-cors' }), /invalid_soty_request/u);
  assert.throws(() => sotyFetch('/items', { redirect: 'follow' }), /invalid_soty_request/u); assert.equal(calls, 1);
});

test('verifier rejects expired, foreign-issuer and duplicate assertions', async () => {
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 }), issuer = 'https://soty.example/human-identity';
  const jwk = { ...pair.privateKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const publicJwk = { ...pair.publicKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  let now = Date.now();
  const signer = createRelayIdentitySigner({ profile: { enabled: true, issuer, providerKeys: () => ({ jwks: { keys: [jwk] } }) }, clock: () => now });
  const appId = 'app-' + '1'.repeat(32), fetcher = async () => new Response(JSON.stringify({ keys: [publicJwk] }));
  const verifier = createRelayIdentityVerifier({ issuer, appId, fetch: fetcher, clock: () => now });
  const foreign = createRelayIdentityVerifier({ issuer: 'https://other.example/human-identity', appId, fetch: fetcher, clock: () => now });
  const token = await signer({ appId, accountId: 'alice', method: 'GET', path: '/items', expiresAt: now + 30000 });
  const req = { method: 'GET', url: '/items', headers: { 'x-soty-identity': token }, rawHeaders: ['x-soty-identity', token] };
  await assert.rejects(foreign(req), { code: 'soty_identity_invalid' });
  await assert.rejects(verifier({ ...req, rawHeaders: [...req.rawHeaders, 'X-Soty-Identity', token] }), { code: 'soty_identity_invalid' });
  now += 11000;
  await assert.rejects(verifier(req), { code: 'soty_identity_invalid' });
});
