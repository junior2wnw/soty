import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { connect } from 'node:net';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { createAppsService } from '../server/index.mjs';
import { ensureCanonicalDomain } from '../server/schema.mjs';
import { createHostClassifier, parseAppAuthority } from '../server/hosts.mjs';

const id = `app-${'a'.repeat(32)}`;
const actor = { accountId: 'acct_owner', deviceId: 'device_owner' };
const legacy = 'https://{appId}.legacy.example';
const named = 'https://named.example';

async function fixture(t, overrides = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'soty-domain-hosts-'));
  const databasePath = join(dir, 'apps.sqlite');
  const options = { databasePath, appOriginTemplate: legacy, namedAppZone: named, shellOrigins: ['https://legacy.example'],
    actorActive: value => value?.accountId === actor.accountId && value?.deviceId === actor.deviceId, ...overrides };
  createAppsService(options).close();
  const seed = new DatabaseSync(databasePath);
  seed.exec('PRAGMA foreign_keys=ON; BEGIN IMMEDIATE;');
  seed.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run('connector', actor.accountId,
    JSON.stringify({ linkId: 'test_link', hostDeviceId: 'test_host', connectorId: 'test_connector' }), 'Computer', 1);
  seed.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, actor.accountId, 'connector', 'Private title never public',
    9000, '/', JSON.stringify({ accountIds: [], communityIds: [] }), 'enabled', 1, 1, 1);
  ensureCanonicalDomain(seed, seed.prepare('SELECT * FROM local_apps WHERE id=?').get(id), options.appOriginTemplate);
  seed.exec('COMMIT'); seed.close();
  const services = [], inspections = [], cleanups = [];
  const open = config => { const service = createAppsService({ ...options, ...config }); services.push(service); return service; };
  const inspect = () => { const db = new DatabaseSync(databasePath); inspections.push(db); return db; };
  const service = open();
  const call = (op, args) => service.execute({ actor, op, args });
  const claimed = call('apps.domains.claim', { appId: id, slug: 'friendly-name', requestId: 'claim-host', expectedDomainsRevision: 0 });
  t.after(async () => { for (const cleanup of cleanups) await cleanup(); for (const db of inspections) db.close(); for (const item of services) item.close(); await rm(dir, { recursive: true, force: true }); });
  return { service, open, inspect, call, claimed, cleanups };
}

test('authority parsing is strict and does not interpret URLs, encoded host names or repaired ports', () => {
  assert.deepEqual(parseAppAuthority('APP.EXAMPLE:443'), { hostname: 'app.example', port: '443' });
  assert.deepEqual(parseAppAuthority('[::1]:8080'), { hostname: '[::1]', port: '8080' });
  assert.deepEqual(parseAppAuthority('127.0.0.1'), { hostname: '127.0.0.1', port: '' });
  for (const value of [undefined, null, [], ['app.example'], '', ' app.example', 'app.example ', 'app.example.',
    'app.example:0', 'app.example:0443', 'app.example:65536', 'app.example:', 'app.example:443:80', 'app.example/path',
    'app.example?x', 'app.example#x', 'user@app.example', 'https://app.example', 'app%2eexample', 'app\\example',
    'app..example', '-app.example', 'app-.example', 'аpp.example', 'a'.repeat(64) + '.example', '::1', '[::fffff]', '[::1]suffix',
    'app.example\u0000', 'app.example\r\nHost: other.example']) assert.equal(parseAppAuthority(value), null, String(value));
});

test('exact addresses win; unknown, nested, apex and alternate-port app authorities remain in the app namespace', async t => {
  const f = await fixture(t);
  const canonicalHost = `${id}.legacy.example`, aliasHost = 'friendly-name.named.example';
  assert.equal(f.service.classifyHost(canonicalHost).kind, 'canonical');
  assert.equal(f.service.classifyHost(`${canonicalHost}:443`).kind, 'canonical');
  assert.equal(f.service.classifyHost(canonicalHost.toUpperCase()).kind, 'canonical');
  const alias = f.service.classifyHost(aliasHost);
  assert.equal(alias.kind, 'alias'); assert.equal(alias.domainId, f.claimed.receipt.domainId); assert.equal(alias.state, 'bound');
  for (const host of [`${canonicalHost}:80`, `${aliasHost}:8443`, `nested.${aliasHost}`, 'unknown.named.example', 'named.example',
    'unknown.legacy.example', `app-${'0'.repeat(32)}.legacy.example`, `prefix.${canonicalHost}`]) {
    assert.equal(f.service.classifyHost(host).kind, 'unknown-app-zone', host);
  }
  assert.equal(f.service.classifyHost('legacy.example').kind, 'outside', 'exact existing shell is retained in legacy namespace');
  assert.equal(f.service.classifyHost('legacy.example:8443').kind, 'unknown-app-zone');
  assert.equal(f.service.classifyHost('friendly-name.named.example.evil.test').kind, 'outside', 'suffix confusion is not a managed app host');
  assert.equal(f.service.classifyHost('friendly-name%2enamed.example').kind, 'invalid');
});

test('nondefault ports and legacy templates with a static hostname prefix are matched exactly', async t => {
  const f = await fixture(t, { appOriginTemplate: 'https://fixed.pre-{appId}.legacy.example:8443' });
  const host = `fixed.pre-${id}.legacy.example`;
  assert.equal(f.service.classifyHost(`${host}:8443`).kind, 'canonical');
  assert.equal(f.service.classifyHost(host).kind, 'unknown-app-zone');
  assert.equal(f.service.classifyHost(`${host}:443`).kind, 'unknown-app-zone');
  assert.equal(f.service.allowsTlsDomain(host), true, 'certificate ownership does not depend on its serving port');
});

test('trusted shell exceptions never override allocated addresses or any retained named namespace', async t => {
  const f = await fixture(t);
  const classifier = createHostClassifier({ db: f.inspect(), shellOrigins: [
    `https://${id}.legacy.example`, `https://${id}.legacy.example:8443`, 'https://friendly-name.named.example',
    'https://unknown.named.example', 'https://named.example', 'https://legacy.example',
  ] });
  assert.equal(classifier.classifyHost(`${id}.legacy.example`).kind, 'canonical');
  assert.equal(classifier.classifyHost(`${id}.legacy.example:8443`).kind, 'unknown-app-zone');
  assert.equal(classifier.classifyHost('friendly-name.named.example').kind, 'alias');
  assert.equal(classifier.classifyHost('unknown.named.example').kind, 'unknown-app-zone');
  assert.equal(classifier.classifyHost('named.example').kind, 'unknown-app-zone');
  assert.equal(classifier.classifyHost('legacy.example').kind, 'outside');
  f.service.close();
  const disabled = f.open({ namedAppZone: '' });
  assert.equal(disabled.classifyHost('friendly-name.named.example').kind, 'alias');
  assert.equal(disabled.classifyHost('unknown.named.example').kind, 'unknown-app-zone');
});

test('TLS admission is exact retained HTTPS ownership, including tombstones, never runtime authorization', async t => {
  const f = await fixture(t);
  const hostname = 'friendly-name.named.example';
  assert.equal(f.service.allowsTlsDomain(hostname), true);
  assert.equal(f.service.allowsTlsDomain(`${id}.legacy.example`), true);
  for (const value of [undefined, [hostname], 'unknown.named.example', `${hostname}.evil.test`, `nested.${hostname}`, `${hostname}:443`,
    `https://${hostname}`, hostname.toUpperCase(), `${hostname}.`, 'named.example']) assert.equal(f.service.allowsTlsDomain(value), false, String(value));
  f.call('apps.domains.retire', { appId: id, domainId: f.claimed.receipt.domainId, requestId: 'retire-host', expectedDomainsRevision: 1 });
  assert.equal(f.service.classifyHost(hostname).state, 'tombstone'); assert.equal(f.service.allowsTlsDomain(hostname), true);
  f.call('apps.revoke', { appId: id }); assert.equal(f.service.allowsTlsDomain(`${id}.legacy.example`), true);
  assert.throws(() => f.call('apps.launch', { appId: id }), /apps_access_denied/u);
  const local = await fixture(t, { appOriginTemplate: 'http://{appId}.legacy.localhost:8080', namedAppZone: 'http://named.localhost:8080', shellOrigins: ['http://localhost:8080'] });
  assert.equal(local.service.allowsTlsDomain(`${id}.legacy.localhost`), false);
  assert.equal(local.service.allowsTlsDomain('friendly-name.named.localhost'), false);
});

function http(port, host, path = '/', headers = {}) {
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path, headers: { host, connection: 'close', ...headers } }, res => {
      const parts = []; res.on('data', chunk => parts.push(chunk)); res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(parts).toString('utf8') }));
    }); req.once('error', reject); req.end();
  });
}
function upgrade(port, host) {
  return new Promise((resolve, reject) => {
    const socket = connect(port, '127.0.0.1'); let result = '';
    socket.setTimeout(2000, () => socket.destroy(new Error('upgrade_timeout')));
    socket.once('error', reject); socket.on('data', chunk => { result += chunk; }); socket.once('end', () => resolve(result));
    socket.once('connect', () => socket.write(`GET /api/apps/channel HTTP/1.1\r\nHost: ${host}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n`));
  });
}

test('real HTTP and upgrade keep alias/tombstone hosts on status pages with no shell, cookies or connector channel', async t => {
  const f = await fixture(t); let fallthrough = 0;
  const server = createServer((req, res) => { if (!f.service.handleRequest(req, res)) { fallthrough++; res.end('SHELL_SENTINEL'); } });
  server.on('upgrade', (req, socket, head) => { if (!f.service.handleUpgrade(req, socket, head)) { fallthrough++; socket.destroy(); } });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve)); const port = server.address().port;
  f.cleanups.push(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  for (const path of ['/', '/_soty/boot', '/_soty/session', '/api/connect/capabilities', '/api/apps/channel']) {
    const response = await http(port, 'friendly-name.named.example', path, { 'x-forwarded-host': 'legacy.example' });
    assert.equal(response.status, 503); assert.equal(response.headers['set-cookie'], undefined); assert.equal(response.headers.location, undefined);
    assert.match(response.headers['content-security-policy'], /default-src 'none'/u); assert.equal(response.headers['cache-control'], 'no-store');
    assert.doesNotMatch(response.body, /SHELL_SENTINEL|Private title|acct_owner|test_connector|app-aaaa/u);
  }
  assert.match(await upgrade(port, 'friendly-name.named.example'), /^HTTP\/1\.1 503 /u);
  for (const host of ['unknown.named.example', 'nested.friendly-name.named.example', 'named.example', 'unknown.legacy.example']) {
    assert.equal((await http(port, host, '/api/connect/capabilities')).status, 404);
    assert.match(await upgrade(port, host), /^HTTP\/1\.1 404 /u);
  }
  assert.equal(fallthrough, 0);
  f.call('apps.domains.retire', { appId: id, domainId: f.claimed.receipt.domainId, requestId: 'retire-http', expectedDomainsRevision: 1 });
  assert.equal((await http(port, 'friendly-name.named.example')).status, 410);
  assert.match(await upgrade(port, 'friendly-name.named.example'), /^HTTP\/1\.1 410 /u);
  assert.equal(fallthrough, 0);
  assert.equal((await http(port, 'legacy.example', '/', { 'x-forwarded-host': 'friendly-name.named.example' })).body, 'SHELL_SENTINEL');
  assert.equal(fallthrough, 1, 'forwarded host is ignored in both directions');
});
