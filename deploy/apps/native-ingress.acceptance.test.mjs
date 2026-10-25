import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, basename, dirname, resolve } from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReleasePlan, nativeIngress } from './release.mjs';
import { deployment, manifest } from './fixtures.mjs';

test('real Caddy preserves app query/cookie/CSP and closes the ingress when access or source pin fails', { skip: !process.env.SOTY_CADDY_BIN }, async t => {
  const binary = process.env.SOTY_CADDY_BIN, authRequests = [], appRequests = [], upgraded = [], sockets = new Set();
  let authUpgrades = 0;
  let expectedPin = 'd'.repeat(64);
  const root = await mkdtemp(join(tmpdir(), 'soty-native-caddy-test-')); let child;
  const gateway = createServer((req, res) => {
    authRequests.push({ path: req.url, pin: req.headers['x-soty-ingress-target'], upgrade: req.headers.upgrade, key: req.headers['sec-websocket-key'] });
    if (req.url.startsWith('/_soty/boot?')) { res.end('boot'); return; }
    if (req.url !== '/_soty/ingress-check' || req.headers['x-soty-ingress-target'] !== expectedPin
      || req.headers.cookie?.includes('__Host-soty_app_session=invalid')) { res.writeHead(403); res.end(); return; }
    res.writeHead(204); res.end();
  });
  gateway.on('upgrade', (_req, socket) => { authUpgrades++; socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n'); });
  const app = createServer((req, res) => {
    appRequests.push({ path: req.url, cookie: req.headers.cookie, pin: req.headers['x-soty-ingress-target'] });
    res.setHeader('Content-Security-Policy', "default-src 'none'");
    res.setHeader('Set-Cookie', '__Host-own_app=session; Secure; HttpOnly; Path=/; SameSite=Lax');
    res.end('application');
  });
  app.on('upgrade', (req, socket) => {
    sockets.add(socket); socket.on('close', () => sockets.delete(socket));
    upgraded.push({ origin: req.headers.origin, cookie: req.headers.cookie });
    const accept = createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
    socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
    // Echo one valid masked client frame: exercise the actual upgraded tunnel.
    socket.once('data', frame => {
      const size = frame[1] & 127, mask = frame.subarray(2, 6), payload = frame.subarray(6, 6 + size);
      const text = Buffer.from(payload.map((byte, i) => byte ^ mask[i % 4]));
      socket.write(Buffer.concat([Buffer.from([0x81, text.length]), text]));
    });
  });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done)); await new Promise(done => app.listen(0, '127.0.0.1', done));
  const listener = createServer(); await new Promise(done => listener.listen(0, '127.0.0.1', done)); const port = listener.address().port;
  await new Promise(done => listener.close(done));
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    if (child && child.exitCode === null) { child.kill('SIGTERM'); await new Promise(done => child.once('exit', done)); }
    await Promise.all([new Promise(done => gateway.close(done)), new Promise(done => app.close(done))]);
    assert.equal(dirname(resolve(root)), resolve(tmpdir())); assert.match(basename(root), /^soty-native-caddy-test-/u);
    await rm(root, { recursive: true, force: true });
  });
  const value = deployment(); value.source.port = app.address().port;
  const local = { ...manifest(), port: value.source.port };
  const plan = createReleasePlan({ manifest: local, deployment: value, mode: 'native', gatewayPort: gateway.address().port });
  const file = join(root, 'Caddyfile'); await writeFile(file, nativeIngress(plan));
  const adapted = spawnSync(binary, ['adapt', '--config', file, '--adapter', 'caddyfile'], { encoding: 'utf8', windowsHide: true });
  assert.equal(adapted.status, 0, 'generated Caddyfile must adapt');
  const adaptedConfig = JSON.parse(adapted.stdout);
  const config = { admin: { disabled: true }, apps: { http: { servers: { probe: {
    listen: ['127.0.0.1:' + port], automatic_https: { disable: true },
    routes: Object.values(adaptedConfig.apps.http.servers).flatMap(server => server.routes),
  } } } } };
  const configFile = join(root, 'probe.json'); await writeFile(configFile, JSON.stringify(config));
  child = spawn(binary, ['run', '--config', configFile], { windowsHide: true, stdio: 'ignore', env: { ...process.env, XDG_CONFIG_HOME: root, XDG_DATA_HOME: root } });
  child.on('error', () => {});
  const get = (path, headers = {}, method = 'GET') => new Promise((done, fail) => {
    const req = request({ host: '127.0.0.1', port, path, method, headers: { Host: 'project.example', ...headers } }, res => {
      res.resume(); res.on('end', () => done({ status: res.statusCode, headers: res.headers }));
    }); req.on('error', fail); req.setTimeout(3000, () => req.destroy(new Error('probe timeout'))); req.end();
  });
  let ready = false;
  for (let retry = 0; retry < 60; retry++) {
    try { await get('/_soty/boot?path=%2F'); ready = true; break; } catch {}
    if (child.exitCode !== null) break;
    await new Promise(done => setTimeout(done, 50));
  }
  assert.equal(ready, true, 'temporary Caddy listener must start');
  const response = await get('/?project=preserved&include=project', { Cookie: '__Host-soty_app_session=valid; __Host-own_app=own' });
  assert.equal(response.status, 200);
  assert.equal(authRequests.at(-1).path, '/_soty/ingress-check');
  assert.equal(appRequests.at(-1).path, '/?project=preserved&include=project');
  assert.equal(appRequests.at(-1).cookie.includes('__Host-soty_app_session'), false);
  assert.equal(appRequests.at(-1).cookie.includes('__Host-own_app=own'), true);
  assert.equal(appRequests.at(-1).pin, undefined);
  assert.match(response.headers['content-security-policy'], /default-src 'none'/u);
  assert.match(response.headers['content-security-policy'], /frame-ancestors 'self' https:\/\/shell\.example/u);
  assert.match(response.headers['set-cookie'][0], /__Host-own_app=/u);
  const websocket = (origin = plan.origin, { upgrade = 'websocket', cookie = '__Host-soty_app_session=valid; __Host-own_app=own' } = {}) => new Promise((done, fail) => {
    const req = request({ host: '127.0.0.1', port, path: '/ws?channel=fixture', headers: {
      Host: 'project.example', ...(origin === null ? {} : { Origin: origin }), Upgrade: upgrade, Connection: 'Upgrade',
      'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==', 'Sec-WebSocket-Version': '13',
      Cookie: cookie,
    } });
    req.on('response', res => { res.resume(); done({ status: res.statusCode }); });
    req.on('error', fail);req.setTimeout(3000, () => req.destroy(new Error('websocket probe timeout')));
    req.on('upgrade', (res, socket, head) => {
      const timer = setTimeout(() => { socket.destroy(); fail(new Error('websocket echo timeout')); }, 3000);
      socket.once('error', error => { clearTimeout(timer); fail(error); });
      const received = data => { clearTimeout(timer);socket.destroy();done({ status: res.statusCode, echo: data.subarray(2).toString() }); };
      if (head.length) received(head); else socket.once('data', received);
      socket.write(Buffer.from([0x81, 0x82, 1, 2, 3, 4, 'o'.charCodeAt(0) ^ 1, 'k'.charCodeAt(0) ^ 2]));
    });req.end();
  });
  assert.deepEqual(await websocket(), { status: 101, echo: 'ok' });
  assert.equal(authUpgrades, 0, 'forward_auth must remain an ordinary HTTP request');
  assert.equal(authRequests.at(-1).upgrade, undefined);assert.equal(authRequests.at(-1).key, undefined);
  assert.equal(upgraded.at(-1).origin, plan.origin);assert.match(upgraded.at(-1).cookie, /__Host-own_app=own/u);
  assert.equal(upgraded.at(-1).cookie.includes('__Host-soty_app_session'), false);
  assert.deepEqual(await websocket(plan.origin, { upgrade: 'WebSocket' }), { status: 101, echo: 'ok' });
  const beforeWs = upgraded.length;
  assert.equal((await websocket('https://foreign.example')).status, 403);assert.equal(upgraded.length, beforeWs);
  assert.equal((await websocket('https://foreign.example', { upgrade: 'WebSocket' })).status, 403);
  assert.equal((await websocket(null)).status, 403);
  assert.equal((await websocket(plan.origin, { cookie: '__Host-soty_app_session=invalid' })).status, 403);
  assert.equal(upgraded.length, beforeWs);
  const beforeWrites = appRequests.length;
  for (const method of ['POST', 'PUT', 'PATCH', 'DELETE']) {
    assert.equal((await get('/write', {}, method)).status, 403);
    assert.equal((await get('/write', { Origin: 'https://foreign.example' }, method)).status, 403);
  }
  assert.equal(appRequests.length, beforeWrites);
  assert.equal((await get('/write', { Origin: plan.origin }, 'POST')).status, 200);
  const before = appRequests.length;
  assert.equal((await get('/?include=project', { Cookie: '__Host-soty_app_session=invalid' })).status, 403);
  assert.equal(appRequests.length, before);
  // Change the gateway's expected pin, rather than client headers which Caddy overrides.
  expectedPin = 'e'.repeat(64);
  assert.equal((await get('/?source=changed')).status, 403); assert.equal(appRequests.length, before);
  assert.equal((await websocket()).status, 403);assert.equal(upgraded.length, beforeWs);
  assert.equal(authRequests.at(-1).pin, 'd'.repeat(64));
});
