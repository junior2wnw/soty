import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createAppsService } from '../server/index.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';
import { createRelayIdentityFetch } from '../../source-app/browser/relay-identity-fetch.mjs';

const bundle = await readFile(new URL('../../../public/agent/soty-connector.mjs', import.meta.url), 'utf8');
const start = bundle.indexOf('// bundled connector module: ./agent-modules/local-apps.mjs\n');
const end = bundle.indexOf('// bundled connector module:', start + 1);
assert.ok(start >= 0 && end > start);
const { createLocalAppsRuntime: bundled } = await import('data:text/javascript;base64,' + Buffer.from(bundle.slice(start, end) + '\nexport { createLocalAppsRuntime };\n').toString('base64'));
const secret = () => randomBytes(32).toString('base64url');
const hash = value => createHash('sha256').update(value).digest('hex');
async function until(check) { const deadline = Date.now() + 5000; while (!check()) { if (Date.now() > deadline) throw Error('fixture_timeout'); await delay(10); } }
function http(port, path, { origin, cookie, method = 'GET', headers = {}, body } = {}) {
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path, method, headers: { host: new URL(origin).host,
      ...(cookie ? { cookie } : {}), ...(method === 'GET' ? {} : { origin }), ...headers } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, text: Buffer.concat(chunks).toString() }));
    });
    req.on('error', reject); req.end(body);
  });
}

for (const [mode, makeRuntime] of [['source', createLocalAppsRuntime], ['released connector', bundled]])
test(`ordinary relay identity is refused before signing or upstream on ${mode}`, { timeout: 20000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-relay-disabled-'));
  let service, runtime, signatures = 0, legacy = false, legacyReady = false; const seen = [], revoked = new Set();
  const source = createServer((req, res) => { if (req.method === 'HEAD') { res.writeHead(200).end(); return; }
    seen.push({ path: req.url, identity: req.headers['x-soty-identity'], key: req.headers['idempotency-key'] });
    res.writeHead(200, { 'content-type': 'text/plain' }).end('ordinary app'); });
  const root = createServer((req, res) => service?.handleRequest(req, res) || res.writeHead(404).end());
  root.on('upgrade', (req, socket, head) => { if (!service.handleUpgrade(req, socket, head)) socket.destroy(); });
  await Promise.all([source, root].map(server => new Promise(done => server.listen(0, '127.0.0.1', done))));
  const port = root.address().port, rootOrigin = `http://127.0.0.1:${port}`;
  const alice = { accountId: 'alice', deviceId: 'alice-device' }, bob = { accountId: 'bob', deviceId: 'bob-device' };
  service = createAppsService({ databasePath: join(directory, 'apps.sqlite'), shellOrigins: [rootOrigin], appOriginTemplate: `http://{appId}.localhost:${port}`,
    actorActive: actor => [alice, bob].some(value => value.accountId === actor?.accountId && value.deviceId === actor?.deviceId) && !revoked.has(actor.deviceId),
    canAccessCommunity: () => false, isGroupAdmin: () => false, authenticateConnector: async () => true,
    signRelayIdentity: () => { signatures++; throw Error('must never sign'); } });
  const host = { linkId: 'link', hostDeviceId: 'host', connectorId: 'connector', name: 'fixture' };
  runtime = makeRuntime({ randomSecret: secret, digest: hash, createWebSocket: url => {
    const socket = new WebSocket(url), send = socket.send.bind(socket);
    socket.send = data => { const frame = JSON.parse(data); if (legacy && frame.type === 'auth') { delete frame.capabilities.relayIdempotencyKey; legacyReady = true; return send(JSON.stringify(frame)); } return send(data); };
    return socket;
  }, httpRequest: request, encodeBase64: value => Buffer.from(value).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') }, { serverUrl: rootOrigin, identity: host, token: 'synthetic' });
  t.after(async () => { runtime.stop(); service.close(); root.closeAllConnections(); source.closeAllConnections();
    await Promise.all([root, source].map(server => new Promise(done => server.close(done)))); await rm(directory, { recursive: true, force: true }); });
  runtime.start(); await until(() => runtime.status().connected);
  const claim = await runtime.claim(), call = (op, args, actor = alice) => service.execute({ op, actor, args: { ...args, expectedAccountId: actor.accountId } });
  call('apps.claim', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, claimCode: claim.claimCode });
  const app = call('apps.register', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, name: 'Fixture', port: source.address().port, entryPath: '/', grants: { accountIds: [bob.accountId] } }).app;
  await until(() => runtime.status().apps === 1);
  await until(() => { try { call('apps.launch', { appId: app.id }); return true; } catch { return false; } });
  async function session(actor) {
    const url = new URL(call('apps.launch', { appId: app.id }, actor).launchUrl);
    const boot = await http(port, '/_soty/session', { origin: url.origin, method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ ticket: url.hash.slice(1) }) });
    assert.equal(boot.status, 200); return { origin: url.origin, cookie: boot.headers['set-cookie'][0].split(';')[0] };
  }
  const a = await session(alice), b = await session(bob), callIdentity = (session, method = 'GET', headers = {}) =>
    createRelayIdentityFetch({ origin: session.origin, fetch: (path, options) => http(port, path, { ...session, method: options.method, headers: Object.fromEntries(options.headers) }) })('/items', { method, headers });
  const before = seen.length;
  for (const session of [a, b]) for (const method of ['GET', 'POST']) {
    const response = await callIdentity(session, method, { 'x-soty-frame': 'A'.repeat(43) });
    assert.equal(response.status, 503); assert.equal(JSON.parse(response.text).error, 'app_identity_window_unavailable');
  }
  assert.equal(seen.length, before); assert.equal(signatures, 0);
  const html = await callIdentity(a, 'GET', { accept: 'text/html' });
  assert.equal(html.status, 503); assert.match(html.text, /Общий вход временно недоступен/u); assert.equal(seen.length, before);
  const ordinary = await http(port, '/ordinary', { ...a, headers: { 'x-soty-identity': 'untrusted-browser-value' } });
  assert.equal(ordinary.status, 200); assert.equal(ordinary.text, 'ordinary app'); assert.equal(seen.at(-1).identity, undefined);
  const write = await http(port, '/ordinary', { ...a, method: 'POST', headers: { 'idempotency-key': 'safe-fixture-key' }, body: 'record' });
  assert.equal(write.status, 200); assert.equal(seen.at(-1).key, 'safe-fixture-key');
  revoked.add(bob.deviceId); const revokedBefore = seen.length;
  assert.ok([401, 403].includes((await callIdentity(b)).status)); assert.equal(seen.length, revokedBefore); assert.equal(signatures, 0);
  runtime.stop(); legacy = true; runtime.start(); await until(() => legacyReady && runtime.status().connected && runtime.status().apps === 1);
  await until(() => { try { call('apps.launch', { appId: app.id }); return true; } catch { return false; } });
  const a2 = await session(alice), legacyBefore = seen.length;
  const unsupported = await http(port, '/ordinary', { ...a2, method: 'POST', headers: { 'idempotency-key': 'old-connector' }, body: 'record' });
  assert.equal(unsupported.status, 503); assert.match(unsupported.text, /app_idempotency_key_unavailable/u); assert.equal(seen.length, legacyBefore);
  assert.equal((await http(port, '/ordinary', a2)).status, 200);
});
