import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, basename, dirname, resolve } from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { createReleasePlan, nativeIngress } from './release.mjs';
import { deployment, manifest } from './fixtures.mjs';

for (const profile of ['soty.relay-restricted.v1', 'soty.selected-human-embed.v1', 'soty.selected-human-embed.v2'])
test(`real Caddy preserves Native routing and uses Root for scoped APIs: ${profile}`, { skip: !process.env.SOTY_CADDY_BIN }, async t => {
  const binary = process.env.SOTY_CADDY_BIN, authRequests = [], appRequests = [];
  let expectedPin = 'd'.repeat(64);
  const root = await mkdtemp(join(tmpdir(), 'soty-native-caddy-test-')); let child;
  const gateway = createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    authRequests.push({ path: req.url, pin: req.headers['x-soty-ingress-target'], method: req.method,
      cookie: req.headers.cookie, frame: req.headers['x-soty-frame'], origin: req.headers.origin, body: Buffer.concat(chunks).toString() });
    if (req.url.startsWith('/_soty/boot?')) { res.end('boot'); return; }
    if (req.url.startsWith('/api/embed/')) {
      res.writeHead(req.headers.cookie?.split('; ').includes('__Host-soty_app_session=valid') ? 200 : 403); res.end('scoped gateway'); return;
    }
    if (req.url !== '/_soty/ingress-check' || req.headers['x-soty-ingress-target'] !== expectedPin
      || req.headers.cookie?.includes('__Host-soty_app_session=invalid')) { res.writeHead(403); res.end(); return; }
    res.writeHead(204); res.end();
  });
  const app = createServer((req, res) => {
    appRequests.push({ path: req.url, cookie: req.headers.cookie, frame: req.headers['x-soty-frame'], pin: req.headers['x-soty-ingress-target'] });
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
  const value = deployment(); value.source.port = app.address().port; value.source.profile = profile;
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
  const get = (path, headers = {}, method = 'GET', body) => new Promise((done, fail) => {
    const req = request({ host: '127.0.0.1', port, path, method, headers: { Host: 'project.example', ...headers } }, res => {
      res.resume(); res.on('end', () => done({ status: res.statusCode, headers: res.headers }));
    }); req.on('error', fail); req.setTimeout(3000, () => req.destroy(new Error('probe timeout'))); req.end(body);
  });
  let ready = false;
  for (let retry = 0; retry < 60; retry++) {
    try { await get('/_soty/boot?path=%2F'); ready = true; break; } catch {}
    if (child.exitCode !== null) break;
    await new Promise(done => setTimeout(done, 50));
  }
  assert.equal(ready, true, 'temporary Caddy listener must start');
  const frame='A'.repeat(43),otherFrame='B'.repeat(43);
  const response = await get('/?project=preserved&include=project', { Cookie: '__Host-soty_app_session=valid; __Host-soty_app_session_'+frame+'=main; __Host-own_app=own; __Host-soty_app_session_'+otherFrame+'=widget', 'X-Soty-Frame':frame });
  assert.equal(response.status, 200);
  assert.equal(authRequests.at(-1).path, '/_soty/ingress-check');
  assert.equal(appRequests.at(-1).path, '/?project=preserved&include=project');
  assert.equal(appRequests.at(-1).cookie.includes('__Host-soty_app_session'), false);
  assert.equal(appRequests.at(-1).cookie.includes('__Host-own_app=own'), true);
  assert.equal(appRequests.at(-1).pin, undefined);
  assert.equal(appRequests.at(-1).frame, undefined,'Native fallback must not receive the Root frame selector');
  assert.match(response.headers['content-security-policy'], /default-src 'none'/u);
  assert.match(response.headers['content-security-policy'], /frame-ancestors 'self' https:\/\/shell\.example/u);
  assert.match(response.headers['set-cookie'][0], /__Host-own_app=/u);
  // These requests previously went straight to Native, without Root's Source
  // proof. Exercise the generated Caddy router, not just the template string.
  const scoped = profile.startsWith('soty.selected-human-embed.');
  const nativeBefore = appRequests.length, body = JSON.stringify({ requestId: 'fixture', input: { operation: 'soty.current_context' } });
  const apiHeaders = { Cookie: '__Host-soty_app_session=valid; __Host-soty_app_session_'+frame+'=main; __Host-soty_app_session_'+otherFrame+'=widget', Origin: 'https://project.example', 'Content-Type': 'application/json', 'X-Soty-Frame':frame };
  assert.equal((await get('/api/embed/query', apiHeaders, 'POST', body)).status, 200);
  if (scoped) {
    assert.equal(appRequests.length, nativeBefore);
    assert.deepEqual(authRequests.at(-1), { path: '/api/embed/query', pin: undefined, method: 'POST',
      cookie: apiHeaders.Cookie, frame, origin: apiHeaders.Origin, body });
    assert.equal((await get('/api/embed/query', { ...apiHeaders, Cookie: '__Host-soty_app_session=invalid' }, 'POST', body)).status, 403);
    assert.equal(appRequests.length, nativeBefore, 'denied Source request cannot fall through to Native');
  } else assert.equal(appRequests.at(-1).path, '/api/embed/query');
  assert.equal((await get('/api/account/session')).status, 200);
  assert.equal(appRequests.at(-1).path, '/api/account/session', 'ordinary Native APIs retain their original route');
  const before = appRequests.length;
  assert.equal((await get('/?include=project', { Cookie: '__Host-soty_app_session=invalid' })).status, 403);
  assert.equal(appRequests.length, before);
  // Change the gateway's expected pin, rather than client headers which Caddy overrides.
  expectedPin = 'e'.repeat(64);
  assert.equal((await get('/?source=changed')).status, 403); assert.equal(appRequests.length, before);
  assert.equal(authRequests.at(-1).pin, 'd'.repeat(64));
});
