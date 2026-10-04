import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, basename, dirname, resolve } from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { createReleasePlan, nativeIngress } from './release.mjs';
import { deployment, manifest } from './fixtures.mjs';

test('real Caddy preserves app query/cookie/CSP and closes the ingress when access or source pin fails', { skip: !process.env.SOTY_CADDY_BIN }, async t => {
  const binary = process.env.SOTY_CADDY_BIN, authRequests = [], appRequests = [];
  let expectedPin = 'd'.repeat(64);
  const root = await mkdtemp(join(tmpdir(), 'soty-native-caddy-test-')); let child;
  const gateway = createServer((req, res) => {
    authRequests.push({ path: req.url, pin: req.headers['x-soty-ingress-target'] });
    if (req.url.startsWith('/_soty/boot?')) { res.end('boot'); return; }
    if (req.url !== '/_soty/ingress-check' || req.headers['x-soty-ingress-target'] !== expectedPin
      || req.headers.cookie?.includes('__Host-soty_app_session=invalid')) { res.writeHead(403); res.end(); return; }
    res.writeHead(204); res.end();
  });
  const app = createServer((req, res) => {
    appRequests.push({ path: req.url, cookie: req.headers.cookie, pin: req.headers['x-soty-ingress-target'] });
    res.setHeader('Content-Security-Policy', "default-src 'none'");
    res.setHeader('Set-Cookie', '__Host-own_app=session; Secure; HttpOnly; Path=/; SameSite=Lax');
    res.end('application');
  });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done)); await new Promise(done => app.listen(0, '127.0.0.1', done));
  const listener = createServer(); await new Promise(done => listener.listen(0, '127.0.0.1', done)); const port = listener.address().port;
  await new Promise(done => listener.close(done));
  t.after(async () => {
    if (child && child.exitCode === null) { child.kill('SIGTERM'); await new Promise(done => child.once('exit', done)); }
    await Promise.all([new Promise(done => gateway.close(done)), new Promise(done => app.close(done))]);
    assert.equal(dirname(resolve(root)), resolve(tmpdir())); assert.match(basename(root), /^soty-native-caddy-test-/u);
    await rm(root, { recursive: true, force: true });
  });
  const value = deployment(); value.source.port = app.address().port;
  const local = { ...manifest(), port: value.source.port };
  const plan = createReleasePlan({ manifest: local, deployment: value, mode: 'native', gatewayPort: gateway.address().port });
  const file = join(root, 'Caddyfile'); await writeFile(file, nativeIngress(plan));
  const adapted = spawnSync(binary, ['adapt', '--config', file, '--adapter', 'caddyfile'], { encoding: 'utf8' });
  assert.equal(adapted.status, 0, 'generated Caddyfile must adapt');
  const adaptedConfig = JSON.parse(adapted.stdout);
  const config = { admin: { disabled: true }, apps: { http: { servers: { probe: {
    listen: ['127.0.0.1:' + port], automatic_https: { disable: true },
    routes: Object.values(adaptedConfig.apps.http.servers).flatMap(server => server.routes),
  } } } } };
  const configFile = join(root, 'probe.json'); await writeFile(configFile, JSON.stringify(config));
  child = spawn(binary, ['run', '--config', configFile], { stdio: 'ignore', env: { ...process.env, XDG_CONFIG_HOME: root, XDG_DATA_HOME: root } });
  child.on('error', () => {});
  const get = (path, headers = {}) => new Promise((done, fail) => {
    const req = request({ host: '127.0.0.1', port, path, headers: { Host: 'project.example', ...headers } }, res => {
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
  const before = appRequests.length;
  assert.equal((await get('/?include=project', { Cookie: '__Host-soty_app_session=invalid' })).status, 403);
  assert.equal(appRequests.length, before);
  // Change the gateway's expected pin, rather than client headers which Caddy overrides.
  expectedPin = 'e'.repeat(64);
  assert.equal((await get('/?source=changed')).status, 403); assert.equal(appRequests.length, before);
  assert.equal(authRequests.at(-1).pin, 'd'.repeat(64));
});
