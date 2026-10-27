import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { DatabaseSync } from 'node:sqlite';
import { createAppsService } from '../server/index.mjs';
import { createRelayIdentitySigner } from '../../human-identity/source-authority.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';

const installed = process.env.SOTY_SOURCE_TEST_SDK_DIRECTORY;
const serverModule = installed ? pathToFileURL(join(installed, 'dist', 'server.mjs')).href : null;
const { createRelayIdentityVerifier } = await import(serverModule ?? '../../source-app/server/relay-identity.mjs');
const { createSqliteRelayNonceStore } = await import(serverModule ?? '../../source-app/server/sqlite-relay-nonces.mjs');
const { createSqliteRelayIdentity } = await import(serverModule ?? '../../source-app/server/sqlite-relay-identity.mjs');
const { createRelayIdentityFetch } = await import(installed ? pathToFileURL(join(installed, 'dist', 'browser.mjs')).href : '../../source-app/browser/relay-identity-fetch.mjs');
// Exercise the exact module shipped in the generated single-file connector,
// as well as source. The full bundle's process/update path has separate tests.
const bundle = await readFile(new URL('../../../public/agent/soty-connector.mjs', import.meta.url), 'utf8');
const moduleStart = bundle.indexOf('// bundled connector module: ./agent-modules/local-apps.mjs\n');
const moduleEnd = bundle.indexOf('// bundled connector module:', moduleStart + 1);
assert.ok(moduleStart >= 0 && moduleEnd > moduleStart, 'connector release module is missing');
const bundledModule = bundle.slice(moduleStart, moduleEnd) + '\nexport { createLocalAppsRuntime };\n';
const { createLocalAppsRuntime: bundledRuntime } = await import('data:text/javascript;base64,' + Buffer.from(bundledModule).toString('base64'));

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

for (const [mode, makeRuntime] of [['source', createLocalAppsRuntime], ['released bundle', bundledRuntime]])
test(`signed Root identity and idempotency stay isolated through ${mode}`, { timeout: 20000, skip: 'Ordinary relay identity is intentionally unavailable until it is bound to a window.' }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-relay-identity-'));
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const privateJwk = { ...pair.privateKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const publicJwk = { ...pair.publicKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const issuer = 'https://soty.example/human-identity';
  const profile = { enabled: true, issuer, providerKeys: () => ({ jwks: { keys: [privateJwk] } }) };
  const actualSigner = createRelayIdentitySigner({ profile });
  let gate, failSigner = false, service, runtime, legacyWire = false, legacyAuthSeen = false;
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
  runtime = makeRuntime({ randomSecret: secret, digest: hash, createWebSocket: url => {
    const ws = new WebSocket(url), send = ws.send.bind(ws);
    ws.send = data => { const frame = JSON.parse(data); if (legacyWire && frame.type === 'auth') {
      delete frame.capabilities.relayIdempotencyKey; legacyAuthSeen = true; return send(JSON.stringify(frame));
    } return send(data); };
    return ws;
  }, httpRequest: request,
    encodeBase64: value => Buffer.from(value).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
  { serverUrl: rootOrigin, identity, token: 'fixture' });
  const servers = [], stores = [], businessStores = [];
  t.after(async () => { gate?.release(); runtime.stop(); service.close(); root.closeAllConnections();
    await new Promise(done => root.close(done)); for (const server of servers) { server.closeAllConnections(); await new Promise(done => server.close(done)); }
    for (const store of stores) store.close(); for (const close of businessStores) close();
    await rm(directory, { recursive: true, force: true }); });
  runtime.start(); await until(() => runtime.status().connected);
  const claim = await runtime.claim();
  const call = (op, args, actor = owner) => service.execute({ op, actor, args: { ...args, expectedAccountId: actor.accountId } });
  call('apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode: claim.claimCode });
  const apps = [];
  for (let index = 0; index < 2; index++) {
    let verifier; const seen = [], businessPath = join(directory, `business-${index}.sqlite`);
    let business = new DatabaseSync(businessPath);
    business.exec('CREATE TABLE records (issuer TEXT NOT NULL, subject TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(issuer,subject))');
    business.exec('CREATE TABLE receipts (issuer TEXT NOT NULL, subject TEXT NOT NULL, request_id TEXT NOT NULL, input_hash TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(issuer,subject,request_id))');
    businessStores.push(() => business.close());
    const upstream = createServer((req, res) => {
      if (req.method === 'HEAD') { res.writeHead(200).end(); return; }
      seen.push({ method: req.method, path: req.url, token: req.headers['x-soty-identity'], key: req.headers['idempotency-key'], authorization: req.headers.authorization });
      void (async () => {
        try { const person = await verifier(req);
          if (req.method === 'GET' && req.url.startsWith('/receipt?')) {
            const key = new URL(req.url, 'http://fixture.invalid').searchParams.get('key');
            const receipt = business.prepare('SELECT count FROM receipts WHERE issuer=? AND subject=? AND request_id=?').get(person.issuer, person.subject, key);
            if (!receipt) { res.writeHead(404).end(); return; }
            res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ subject: person.subject, count: receipt.count })); return;
          }
          if (req.method === 'POST' && req.url === '/idempotent') {
            const key = req.headers['idempotency-key']; if (typeof key !== 'string' || !key) { res.writeHead(400).end(); return; }
            const chunks = []; for await (const chunk of req) chunks.push(chunk);
            const raw = Buffer.concat(chunks).toString('utf8'), inputHash = hash(raw);
            let count; business.exec('BEGIN IMMEDIATE');
            try {
              const prior = business.prepare('SELECT input_hash,count FROM receipts WHERE issuer=? AND subject=? AND request_id=?').get(person.issuer, person.subject, key);
              if (prior && prior.input_hash !== inputHash) { business.exec('ROLLBACK'); res.writeHead(409).end(); return; }
              if (prior) count = prior.count;
              else {
                business.prepare('INSERT INTO records VALUES(?,?,1) ON CONFLICT(issuer,subject) DO UPDATE SET count=count+1').run(person.issuer, person.subject);
                count = business.prepare('SELECT count FROM records WHERE issuer=? AND subject=?').get(person.issuer, person.subject).count;
                business.prepare('INSERT INTO receipts VALUES(?,?,?,?,?)').run(person.issuer, person.subject, key, inputHash, count);
              }
              business.exec('COMMIT');
            } catch (error) { business.exec('ROLLBACK'); throw error; }
            if (key === 'lost-reply' && !seen.some(item => item.key === key && item.lost)) { seen.at(-1).lost = true; res.destroy(); return; }
            res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ subject: person.subject, count })); return;
          }
          if (req.method === 'POST') business.prepare('INSERT INTO records VALUES(?,?,1) ON CONFLICT(issuer,subject) DO UPDATE SET count=count+1').run(person.issuer, person.subject);
          const count = business.prepare('SELECT count FROM records WHERE issuer=? AND subject=?').get(person.issuer, person.subject)?.count ?? 0;
          res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ subject: person.subject, count })); }
        catch (error) { res.writeHead(error.status === 503 ? 503 : 401).end('identity unavailable'); }
      })();
    });
    await new Promise(done => upstream.listen(0, '127.0.0.1', done)); servers.push(upstream);
    const app = call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId,
      name: 'fixture ' + index, port: upstream.address().port, entryPath: '/', grants: { accountIds: [bob.accountId] } }).app;
    const noncePath = join(directory, `nonce-${index}.sqlite`);
    const makeIdentity = () => createSqliteRelayIdentity({ issuer, appId: app.id, databasePath: noncePath, fetch: async url => {
      assert.equal(new URL(url).href, issuer + '/jwks'); return new Response(JSON.stringify({ keys: [publicJwk] }), { headers: { 'content-type': 'application/json' } });
    } });
    let store = makeIdentity(); stores.push(store); verifier = store.verify;
    apps.push({ app, seen, get verifier() { return verifier; }, restartStore() { business.close(); business = new DatabaseSync(businessPath); },
      restartVerifier() { store.close(); store = makeIdentity(); stores[index] = store; verifier = store.verify; } });
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
  const fetchApp = (session, method = 'GET', headers = {}, path = '/items?x=1', body) => createRelayIdentityFetch({ origin: session.origin,
    fetch: (url, options) => http(port, url, { ...session, method: options.method, headers: Object.fromEntries(options.headers), body: options.body })
  })(path, { method, headers, ...(body === undefined ? {} : { body }) });
  assert.equal((await http(port, '/items?x=1', aliceSession)).status, 401);
  assert.equal(apps[0].seen.at(-1).token, undefined, 'identity is never sent on an ordinary request');
  const alice = await fetchApp(aliceSession, 'POST', { 'x-soty-identity': 'browser-forgery' });
  assert.equal(alice.status, 200, JSON.stringify({ text: alice.text, upstreamSawProof: !!apps[0].seen.at(-1)?.token })); assert.deepEqual(JSON.parse(alice.text), { subject: 'alice', count: 1 });
  assert.equal(alice.text.includes(apps[0].seen.at(-1).token), false);
  apps[0].restartStore();
  assert.deepEqual(JSON.parse((await fetchApp(aliceSession)).text), { subject: 'alice', count: 1 }, 'personal data survives closing and reopening the app store');
  assert.deepEqual(JSON.parse((await fetchApp(bobSession)).text), { subject: 'bob', count: 0 });
  assert.deepEqual(JSON.parse((await fetchApp(otherSession)).text), { subject: 'alice', count: 0 });
  const operation = { 'idempotency-key': 'same-action', authorization: 'must-not-reach-app' }, body = JSON.stringify({ item: 'bread' });
  const firstWrite = await fetchApp(aliceSession, 'POST', operation, '/idempotent', body);
  const repeatedWrite = await fetchApp(aliceSession, 'POST', operation, '/idempotent', body);
  assert.equal(firstWrite.status, 200); assert.equal(repeatedWrite.status, 200);
  assert.deepEqual(JSON.parse(firstWrite.text), { subject: 'alice', count: 2 });
  assert.deepEqual(JSON.parse(repeatedWrite.text), JSON.parse(firstWrite.text), 'same operation must use the app receipt');
  assert.equal(apps[0].seen.at(-1).key, 'same-action'); assert.equal(apps[0].seen.at(-1).authorization, undefined, 'other forbidden headers remain filtered');
  assert.equal((await fetchApp(aliceSession, 'POST', operation, '/idempotent', JSON.stringify({ item: 'changed' }))).status, 409);
  const bobWrite = await fetchApp(bobSession, 'POST', operation, '/idempotent', body);
  assert.deepEqual(JSON.parse(bobWrite.text), { subject: 'bob', count: 1 }, 'same key must not merge people');
  const otherWrite = await fetchApp(otherSession, 'POST', operation, '/idempotent', body);
  assert.deepEqual(JSON.parse(otherWrite.text), { subject: 'alice', count: 1 }, 'same key must not merge applications');
  const lostBefore = apps[0].seen.filter(value => value.key === 'lost-reply').length;
  const lost = await fetchApp(aliceSession, 'POST', { 'idempotency-key': 'lost-reply' }, '/idempotent', body);
  assert.equal(lost.status, 502); await delay(100);
  assert.equal(apps[0].seen.filter(value => value.key === 'lost-reply').length, lostBefore + 1, 'transport must not retry a lost response');
  const readback = await fetchApp(aliceSession, 'GET', {}, '/receipt?key=lost-reply');
  assert.deepEqual(JSON.parse(readback.text), { subject: 'alice', count: 3 }, 'read-only app receipt confirms the original write');
  assert.equal(apps[0].seen.filter(value => value.key === 'lost-reply').length, lostBefore + 1, 'readback does not send the write again');
  const recovered = await fetchApp(aliceSession, 'POST', { 'idempotency-key': 'lost-reply' }, '/idempotent', body);
  assert.deepEqual(JSON.parse(recovered.text), { subject: 'alice', count: 3 }, 'app receipt returns the original result without a second write');
  const priorSeen = apps[0].seen.length;
  const duplicate = await http(port, '/idempotent', { ...aliceSession, method: 'POST', headers: { 'x-soty-identity-request': '1', 'idempotency-key': ['one', 'two'] } });
  assert.equal(duplicate.status, 400); assert.equal(apps[0].seen.length, priorSeen, 'duplicate keys must fail before reaching the app');
  const normalized = await http(port, '/folder/../items?x=1', { ...aliceSession, headers: { 'x-soty-identity-request': '1' } });
  assert.equal(normalized.status, 200); assert.equal(apps[0].seen.at(-1).path, '/items?x=1');
  const old = apps[0].seen.find(value => value.method === 'POST').token;
  const replay = { headers: { 'x-soty-identity': old }, rawHeaders: ['x-soty-identity', old], method: 'POST', url: '/items?x=1' };
  await assert.rejects(apps[0].verifier(replay), { code: 'soty_identity_invalid' });
  apps[0].restartVerifier();
  await assert.rejects(apps[0].verifier(replay), { code: 'soty_identity_invalid' }, 'restarting the verifier cannot revive a consumed proof');
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
  runtime.stop(); legacyWire = true; runtime.start();
  await until(() => legacyAuthSeen && runtime.status().connected && runtime.status().apps === 2);
  await until(() => { try { call('apps.launch', { appId: apps[0].app.id }); return true; } catch { return false; } });
  aliceSession = await session(apps[0].app, owner);
  const legacyBefore = apps[0].seen.length;
  const unsupported = await fetchApp(aliceSession, 'POST', { 'idempotency-key': 'old-connector' }, '/idempotent', body);
  assert.equal(unsupported.status, 503);
  assert.match(unsupported.text, /app_idempotency_key_unavailable/u); assert.match(unsupported.text, /Обновите коннектор/u);
  assert.equal(apps[0].seen.length, legacyBefore, 'a connector that does not advertise support must not receive the request');
  const unsupportedPage = await fetchApp(aliceSession, 'POST', { 'idempotency-key': 'old-connector', accept: 'text/html' }, '/idempotent', body);
  assert.equal(unsupportedPage.status, 503); assert.match(unsupportedPage.text, /Обновите коннектор/u);
  assert.equal(apps[0].seen.length, legacyBefore, 'HTML error must also refuse before delivery');
  assert.equal((await fetchApp(aliceSession)).status, 200, 'requests without an idempotency key still work on the older protocol');
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

test('verifier rejects expired, foreign-issuer and duplicate assertions', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-relay-verifier-'));
  const pair = generateKeyPairSync('rsa', { modulusLength: 2048 }), issuer = 'https://soty.example/human-identity';
  const jwk = { ...pair.privateKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  const publicJwk = { ...pair.publicKey.export({ format: 'jwk' }), kid: 'fixture', alg: 'RS256', use: 'sig' };
  let now = Date.now();
  const signer = createRelayIdentitySigner({ profile: { enabled: true, issuer, providerKeys: () => ({ jwks: { keys: [jwk] } }) }, clock: () => now });
  const appId = 'app-' + '1'.repeat(32), fetcher = async () => new Response(JSON.stringify({ keys: [publicJwk] }));
  const store = createSqliteRelayNonceStore({ databasePath: join(directory, 'nonces.sqlite'), clock: () => now });
  t.after(async () => { store.close(); await rm(directory, { recursive: true, force: true }); });
  const verifier = createRelayIdentityVerifier({ issuer, appId, fetch: fetcher, clock: () => now, consume: store.consume });
  const foreign = createRelayIdentityVerifier({ issuer: 'https://other.example/human-identity', appId, fetch: fetcher, clock: () => now, consume: store.consume });
  assert.throws(() => createRelayIdentityVerifier({ issuer, appId, fetch: fetcher }), { code: 'soty_identity_store_unavailable' });
  const unavailable = createRelayIdentityVerifier({ issuer, appId, fetch: fetcher, clock: () => now, consume() { throw Error('disk failure'); } });
  const token = await signer({ appId, accountId: 'alice', method: 'GET', path: '/items', expiresAt: now + 30000 });
  const req = { method: 'GET', url: '/items', headers: { 'x-soty-identity': token }, rawHeaders: ['x-soty-identity', token] };
  await assert.rejects(foreign(req), { code: 'soty_identity_invalid' });
  await assert.rejects(unavailable(req), { code: 'soty_identity_store_unavailable', status: 503 });
  await assert.rejects(verifier({ ...req, rawHeaders: [...req.rawHeaders, 'X-Soty-Identity', token] }), { code: 'soty_identity_invalid' });
  now += 11000;
  await assert.rejects(verifier(req), { code: 'soty_identity_invalid' });
});
