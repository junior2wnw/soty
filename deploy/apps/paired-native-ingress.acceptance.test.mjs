import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import WebSocket, { WebSocketServer } from 'ws';
import { createReleasePlan, nativeIngress } from './release.mjs';
import { deployment, pairedDeployment, manifest, nativePolicy } from './fixtures.mjs';

for (const paired of [false, true]) test('real Caddy ' + (paired ? 'paired' : 'primary') + ' Native policy route is fixed, non-recursive and separate from Native Human data',
  { skip: !process.env.SOTY_CADDY_BIN }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'soty-paired-caddy-')), sockets = new Set(), auth = [], data = [];
  let child, plan, available = true, expectedPrimary, expectedNative, authUpgrades = 0;
  const gateway = createServer((req, res) => {
    const record = { path: req.url, surface: req.headers['x-soty-ingress-surface'], app: req.headers['x-soty-ingress-app'],
      primary: req.headers['x-soty-ingress-primary'], native: req.headers['x-soty-ingress-target'], upgrade: req.headers.upgrade };
    auth.push(record);
    const policy = record.surface === 'native';
    const valid = available && record.path === '/_soty/ingress-check' && record.app === plan.appId
      && record.native === expectedNative && (!policy || record.primary === expectedPrimary)
      && (paired ? policy : policy || record.surface === undefined)
      && (policy || !req.headers.cookie?.includes('__Host-soty_app_session=invalid'));
    if (!valid) { res.writeHead(403).end(); return; }
    if (policy) res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(nativePolicy(plan)));
    else res.writeHead(204).end();
  });
  gateway.on('upgrade', (_req, socket) => { authUpgrades++; socket.destroy(); });
  const app = createServer((req, res) => {
    data.push({ path: req.url, cookie: req.headers.cookie, headers: Object.keys(req.headers).filter(name => name.startsWith('x-soty-ingress-')) });
    if (req.url.startsWith('/private') && !req.headers.cookie?.includes('__Host-own_app=own')) { res.writeHead(401).end(); return; }
    res.setHeader('Content-Security-Policy', "default-src 'none'"); res.end('Native application');
  });
  const wss = new WebSocketServer({ noServer: true });
  app.on('upgrade', (req, socket, head) => {
    if (!req.headers.cookie?.includes('__Host-own_app=own')) { socket.end('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n'); return; }
    wss.handleUpgrade(req, socket, head, ws => { ws.on('error', () => {}); ws.once('message', bytes => ws.send(bytes)); });
  });
  for (const server of [gateway, app]) server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done)); await new Promise(done => app.listen(0, '127.0.0.1', done));
  const listener = createServer(); await new Promise(done => listener.listen(0, '127.0.0.1', done)); const port = listener.address().port;
  await new Promise(done => listener.close(done));
  t.after(async () => {
    for (const ws of wss.clients) ws.terminate(); wss.close();
    if (child?.exitCode === null) { child.kill('SIGTERM'); await new Promise(done => child.once('exit', done)); }
    for (const socket of sockets) socket.destroy();
    await Promise.all([new Promise(done => gateway.close(done)), new Promise(done => app.close(done))]);
    assert.equal(dirname(resolve(root)), resolve(tmpdir())); assert.ok(root.includes('soty-paired-caddy-'));
    await rm(root, { recursive: true, force: true });
  });
  const value = paired ? pairedDeployment() : deployment();
  if (paired) value.nativeEntry.target.port = app.address().port; else value.source.port = app.address().port;
  plan = createReleasePlan({ manifest: { ...manifest(), port: app.address().port }, deployment: value,
    mode: 'native', gatewayPort: gateway.address().port });
  expectedPrimary = plan.source.digest; expectedNative = plan.nativeEntry?.target.digest ?? plan.source.digest;
  const file = join(root, 'Caddyfile'); await writeFile(file, nativeIngress(plan));
  const adapted = spawnSync(process.env.SOTY_CADDY_BIN, ['adapt', '--config', file, '--adapter', 'caddyfile'], { encoding: 'utf8', windowsHide: true });
  assert.equal(adapted.status, 0, 'the generated Native policy route must adapt');
  const adaptedConfig = JSON.parse(adapted.stdout), configuration = { admin: { disabled: true }, apps: { http: { servers: { probe: {
    listen: ['127.0.0.1:' + port], automatic_https: { disable: true },
    routes: Object.values(adaptedConfig.apps.http.servers).flatMap(server => server.routes),
  } } } } };
  const config = join(root, 'probe.json'); await writeFile(config, JSON.stringify(configuration));
  child = spawn(process.env.SOTY_CADDY_BIN, ['run', '--config', config], { windowsHide: true, stdio: 'ignore',
    env: { ...process.env, XDG_CONFIG_HOME: root, XDG_DATA_HOME: root } }); child.on('error', () => {});
  const get = (path, headers = {}, method = 'GET') => new Promise((done, reject) => {
    const outgoing = request({ hostname: '127.0.0.1', port, path, method, headers: { Host: 'project.example', ...headers },
      signal: AbortSignal.timeout(3000) }, incoming => { const chunks = []; incoming.on('data', part => chunks.push(part));
      incoming.once('end', () => done({ status: incoming.statusCode, body: Buffer.concat(chunks).toString('utf8'), headers: incoming.headers })); });
    outgoing.once('error', reject); outgoing.end();
  });
  let ready = false;
  for (let n = 0; n < 60; n++) { try { await get('/_soty/ingress-check'); ready = true; break; } catch {} await new Promise(done => setTimeout(done, 50)); }
  assert.equal(ready, true);
  const beforePolicy = auth.length, beforeData = data.length;
  const policy = await get('/_soty/ingress-check?discard=this', { 'X-Soty-Ingress-App': 'forged', 'X-Soty-Ingress-Surface': 'unknown',
    'X-Soty-Ingress-Primary': '0'.repeat(64), 'X-Soty-Ingress-Target': '0'.repeat(64) });
  assert.equal(policy.status, 200); assert.deepEqual(JSON.parse(policy.body), nativePolicy(plan));
  assert.equal(auth.length, beforePolicy + 1, 'policy GET goes directly to Root once'); assert.equal(data.length, beforeData);
  assert.equal(auth.at(-1).path, '/_soty/ingress-check'); assert.equal(auth.at(-1).surface, 'native');
  assert.equal(auth.at(-1).primary, expectedPrimary); assert.equal(auth.at(-1).native, expectedNative);
  assert.notEqual((await get('/_soty/ingress-check', {}, 'POST')).status, 200, 'public policy reader is exact GET');
  assert.equal(data.length, beforeData, 'policy routes never enter the Native upstream');
  const response = await get('/private?keep=Native&sort=alpha', { Cookie: '__Host-soty_app_session=valid; __Host-own_app=own' });
  assert.equal(response.status, 200); assert.equal(data.at(-1).path, '/private?keep=Native&sort=alpha');
  assert.match(data.at(-1).cookie, /__Host-own_app=own/u); assert.equal(data.at(-1).cookie.includes('__Host-soty_app_session'), false);
  assert.deepEqual(data.at(-1).headers, []); assert.match(response.headers['content-security-policy'], /default-src 'none'/u);
  assert.equal((await get('/private')).status, 401, 'public Root policy is not Native Human authentication');
  if (!paired) {
    const before = data.length;
    assert.equal((await get('/private', { Cookie: '__Host-soty_app_session=invalid; __Host-own_app=own',
      'X-Soty-Ingress-Surface': 'native', 'X-Soty-Ingress-Primary': expectedPrimary })).status, 403);
    assert.equal(data.length, before); assert.equal(auth.at(-1).surface, undefined);
  } else assert.equal((await get('/_soty/boot?path=%2F')).status, 404);
  const beforeWrite = data.length;
  assert.equal((await get('/private', {}, 'POST')).status, 403);
  assert.equal((await get('/private', { Origin: plan.source.launchEntry?.origin ?? 'https://foreign.example' }, 'POST')).status, 403);
  assert.equal((await get('/ws', { Origin: plan.source.launchEntry?.origin ?? 'https://foreign.example',
    Connection: 'Upgrade', Upgrade: 'WebSocket', 'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==', 'Sec-WebSocket-Version': '13' })).status, 403);
  assert.equal(data.length, beforeWrite);
  assert.equal((await get('/private', { Origin: plan.origin, Cookie: '__Host-own_app=own' }, 'POST')).status, 200);
  const ws = new WebSocket('ws://127.0.0.1:' + port + '/ws?keep=stream', { headers: { Host: 'project.example', Origin: plan.origin, Cookie: '__Host-own_app=own' } });
  ws.on('error', () => {}); await new Promise((done, reject) => { ws.once('open', done); ws.once('error', reject); });
  const echoed = new Promise(done => ws.once('message', bytes => done(bytes.toString()))); ws.send('Native echo'); assert.equal(await echoed, 'Native echo');
  ws.terminate(); assert.equal(authUpgrades, 0); assert.equal(auth.at(-1).upgrade, undefined);
  const beforeClosed = data.length; expectedNative = '0'.repeat(64);
  assert.equal((await get('/_soty/ingress-check')).status, 403); assert.equal((await get('/private')).status, 403); assert.equal(data.length, beforeClosed);
  expectedNative = plan.nativeEntry?.target.digest ?? plan.source.digest; expectedPrimary = '0'.repeat(64);
  assert.equal((await get('/_soty/ingress-check')).status, 403);
  if (paired) assert.equal((await get('/private')).status, 403);
});
