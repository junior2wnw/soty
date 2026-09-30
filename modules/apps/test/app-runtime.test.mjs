import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import WebSocket from 'ws';
import { createAppsService } from '../server/index.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';
import { createSampleApp } from '../examples/sample-app.mjs';
import { createHttpApp } from '../../../server/http-app.js';

const owner = { accountId: 'runtime_owner', deviceId: 'runtime_owner_device' };
const visitor = { accountId: 'runtime_visitor', deviceId: 'runtime_visitor_device' };
const randomSecret = () => randomBytes(32).toString('base64url');

async function setup(t, { legacy = true } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-runtime-author-'));
  const sample = await createSampleApp();
  let service, timestamp = Date.now();
  const server = createServer((req, res) => { if (!service?.handleRequest(req, res)) { res.writeHead(404); res.end(); } });
  server.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port, token = randomSecret();
  const identity = { linkId: 'runtime_link', hostDeviceId: 'runtime_host', connectorId: 'runtime_connector', name: 'Runtime test' };
  service = createAppsService({ databasePath: join(directory, 'apps.sqlite'),
    shellOrigins: [`http://localhost:${port}`], namedAppZone: `http://named.localhost:${port}`,
    appOriginTemplate: legacy ? `http://{appId}.legacy.localhost:${port}` : '',
    actorActive: actor => [owner, visitor].some(item => item.accountId === actor?.accountId && item.deviceId === actor?.deviceId),
    authenticateConnector: async auth => auth.token === token && auth.linkId === identity.linkId && auth.connectorId === identity.connectorId && auth.deviceId === identity.hostDeviceId,
    now: () => timestamp, accessAuditMs: 25,
  });
  const runtime = createLocalAppsRuntime({ randomSecret, digest: value => createHash('sha256').update(value).digest('hex'),
    createWebSocket: url => new WebSocket(url), httpRequest: request,
    encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64'),
  }, { serverUrl: `http://127.0.0.1:${port}`, identity, token });
  t.after(async () => { runtime.stop(); service.close(); await sample.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); await rm(directory, { recursive: true, force: true }); });
  runtime.start(); await until(() => runtime.status().connected);
  const call = (op, args, actor = owner) => service.execute({ op, args, actor });
  const claim = await runtime.claim(); call('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  const app = call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: 'Real loopback app', port: sample.port }).app;
  const claimed = call('apps.domains.claim', { appId: app.id, slug: 'real-project', requestId: 'name-a', expectedDomainsRevision: 0 });
  const domainId = claimed.receipt.domainId, origin = claimed.receipt.origin, host = new URL(origin).host;
  await until(() => call('apps.list').apps[0].state === 'ready');
  const publish = (launchPolicy = 'anyone', activeDomainIds = [domainId]) => {
    const view = call('apps.publication.get', { appId: app.id });
    return call('apps.publication.update', { appId: app.id, requestId: randomSecret(), expectedPolicyEpoch: view.policyEpoch,
      expectedTargetRevision: view.activeTargetRevision, launchPolicy, listed: false, activeDomainIds,
      ...(launchPolicy === 'anyone' ? { exposureAck: { scope: 'whole-port', targetRevision: view.target.revision, targetDigest: view.target.digest, profile: view.target.profile } } : {}),
    });
  };
  const http = (path, options = {}) => sendHttp(port, host, path, options);
  const launch = async (actor = owner, path = '/') => {
    const launch = call('apps.launch', { appId: app.id, domainId, path }, actor);
    const response = await http('/_soty/session', { method: 'POST', headers: { origin }, body: JSON.stringify({ ticket: new URL(launch.launchUrl).hash.slice(1) }) });
    assert.equal(response.status, 200, response.body); assert.equal(JSON.parse(response.body).entryPath, path);
    return response.headers['set-cookie'][0].split(';')[0];
  };
  return { directory, sample, service, runtime, port, app, origin, host, domainId, call, publish, http, launch, advance: ms => { timestamp += ms; } };
}

test('named-only publication serves real HTTP, assets, mutations and websocket through the actual connector', { timeout: 15_000 }, async t => {
  const f = await setup(t, { legacy: false });
  assert.equal(f.service.configured, true); assert.equal(f.call('apps.list').configured, true);
  assert.equal((await f.http('/')).status, 503, 'claim by itself cannot open a runtime');
  f.publish();
  assert.throws(() => f.call('apps.launch', { appId: f.app.id }), /apps_origin_not_configured/u);
  const page = await f.http('/'); assert.equal(page.status, 200); assert.match(page.body, /Покупки/u);
  assert.match(page.headers['content-security-policy'], /connect-src 'self'/u); assert.equal(page.headers['set-cookie'], undefined);
  assert.equal((await f.http('/app.js')).status, 200);
  for (const origin of [undefined, 'null', `http://localhost:${f.port}`, 'https://elsewhere.example']) {
    assert.equal((await f.http('/api/items', { method: 'POST', headers: origin ? { origin } : {}, body: '{"text":"forbidden"}' })).status, 403);
  }
  assert.ok(!f.sample.requests.some(item => item.headers.origin), 'visitor Origin is not a forwarded identity');
  const created = await f.http('/api/items', { method: 'POST', headers: { origin: f.origin, authorization: 'Bearer test-not-a-real-secret', cookie: 'unrelated=ignored', 'content-type': 'application/json' }, body: '{"text":"Public item"}' });
  assert.equal(created.status, 200); assert.match(created.body, /Public item/u); assert.equal(created.headers['set-cookie'], undefined);
  const forwarded = f.sample.requests.findLast(item => item.path === '/api/items');
  assert.equal(forwarded.headers.authorization, undefined); assert.equal(forwarded.headers.cookie, undefined);
  const socket = new WebSocket(`ws://127.0.0.1:${f.port}/live`, { headers: { Host: f.host, Origin: f.origin } });
  t.after(() => socket.terminate());
  assert.match((await message(socket)).toString(), /Public item/u);
  const echoed = message(socket); socket.send('named runtime'); assert.equal((await echoed).toString(), 'named runtime');
  assert.equal((await f.http('/redirect')).status, 502, 'upstream external redirects stay forbidden');
  assert.equal((await f.http('/api/items')).status, 200, 'one rejected response does not kill the connector');
  const closed = new Promise(resolve => socket.once('close', resolve)); f.publish('restricted'); await closed;
  assert.equal((await f.http('/api/items')).status, 403);
  const cookie = await f.launch(owner, '/api/items');
  assert.equal((await f.http('/api/items', { headers: { cookie } })).status, 200);
});

test('account sessions retain an absolute deadline and invalid presented credentials never become a guest', { timeout: 15_000 }, async t => {
  const f = await setup(t); f.publish(); const cookie = await f.launch(visitor);
  const verified = await f.http('/_soty/session', { headers: { cookie } });
  assert.equal(verified.status, 200); assert.equal(verified.headers['set-cookie'], undefined);
  assert.equal((await f.http('/_soty/session')).status, 401);
  for (const value of ['__Host-soty_app_session', '__Host-soty_app_session=bad', `${cookie}; ${cookie}`]) {
    assert.ok([401, 403].includes((await f.http('/', { headers: { cookie: value } })).status));
  }
  f.advance(3_599_999); assert.equal((await f.http('/', { headers: { cookie } })).status, 200);
  f.advance(1); assert.equal((await f.http('/', { headers: { cookie } })).status, 403);
  assert.equal((await f.http('/')).status, 200, 'a distinct cookie-free visit is a new public decision');
  const canonicalHost = `${f.app.id}.legacy.localhost:${f.port}`;
  assert.ok([401, 403].includes((await sendHttp(f.port, canonicalHost, '/')).status));
});

test('the actual shell CSP admits only its validated managed frame zones, also after new claims are disabled', { timeout: 15_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-named-frame-'));
  await writeFile(join(directory, 'index.html'), '<!doctype html><title>Shell CSP probe</title>');
  t.after(() => rm(directory, { recursive: true, force: true }));
  for (const namedAppZone of ['http://named.localhost:8080', '']) {
    const app = createHttpApp(directory, { dataDir: join(directory, 'data'), appOriginTemplate: '', namedAppZone,
      connectOrigins: ['http://localhost:8080'], gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
    const server = createServer(app);
    try {
      await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
      const response = await sendHttp(server.address().port, 'localhost:8080', '/');
      assert.equal(response.status, 200);
      const frames = response.headers['content-security-policy'].split(';').map(part => part.trim()).find(part => part.startsWith('frame-src'));
      assert.equal(frames, "frame-src 'self' http://*.named.localhost:8080");
    } finally { await app.locals.closeServices(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
  }
});

async function until(check) { const deadline = Date.now() + 5_000; while (!check()) { if (Date.now() >= deadline) throw new Error('test_condition_timeout'); await new Promise(resolve => setTimeout(resolve, 10)); } }
function message(socket) { return new Promise((resolve, reject) => { socket.once('message', resolve); socket.once('error', reject); }); }
function sendHttp(port, host, path, { method = 'GET', headers = {}, body } = {}) {
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path, method, headers: { host, ...headers } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.once('error', reject);
      res.once('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString('utf8') }));
    }); req.once('error', reject); req.end(body);
  });
}
