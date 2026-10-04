import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { namedZoneIngress } from './release.mjs';

test('real Caddy uses leaf on-demand TLS and preserves the registered-zone routing boundary', { skip: !process.env.SOTY_CADDY_BIN }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'soty-zone-caddy-test-')); let child;
  const calls = [];
  const gateway = createServer((req, res) => { calls.push({ host: req.headers.host, path: req.url });
    const known = ['app.primary.example', 'app.retained.example'].includes(req.headers.host.toLowerCase());
    res.writeHead(known ? 200 : 404); res.end(known ? 'application' : 'app_not_found'); });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done));
  const listener = createServer(); await new Promise(done => listener.listen(0, '127.0.0.1', done));
  const port = listener.address().port; await new Promise(done => listener.close(done));
  t.after(async () => {
    if (child && child.exitCode === null) { child.kill('SIGTERM'); await new Promise(done => child.once('exit', done)); }
    await new Promise(done => gateway.close(done));
    assert.equal(dirname(resolve(root)), resolve(tmpdir())); await rm(root, { recursive: true, force: true });
  });
  const text = '{\n on_demand_tls {\n  ask http://127.0.0.1:' + gateway.address().port + '/tls-allow\n }\n}\n'
    + namedZoneIngress({ origin: 'https://primary.example', additionalOrigins: ['https://retained.example'], gatewayPort: gateway.address().port });
  const file = join(root, 'Caddyfile'); await writeFile(file, text);
  const adapted = spawnSync(process.env.SOTY_CADDY_BIN, ['adapt', '--config', file, '--adapter', 'caddyfile'], { encoding: 'utf8' });
  assert.equal(adapted.status, 0, 'named-zone snippet must adapt');
  const config = JSON.parse(adapted.stdout);
  const policies = config.apps.tls.automation.policies;
  assert.ok(policies.some(policy => policy.on_demand === true && !policy.subjects?.length));
  assert.ok(policies.every(policy => !policy.subjects?.some(subject => subject.startsWith('*.'))), 'wildcard TLS subjects require a separately reviewed DNS challenge');
  const routes = Object.values(config.apps.http.servers).flatMap(server => server.routes);
  const probeFile = join(root, 'probe.json'); await writeFile(probeFile, JSON.stringify({ admin: { disabled: true },
    apps: { http: { servers: { probe: { listen: ['127.0.0.1:' + port], automatic_https: { disable: true }, routes } } } } }));
  child = spawn(process.env.SOTY_CADDY_BIN, ['run', '--config', probeFile], { stdio: 'ignore', env: { ...process.env, XDG_CONFIG_HOME: root, XDG_DATA_HOME: root } });
  child.on('error', () => {});
  const get = host => new Promise((done, fail) => {
    const req = request({ host: '127.0.0.1', port, path: '/?preserve=query', headers: { Host: host } }, res => {
      res.resume(); res.on('end', () => done(res.statusCode)); });
    req.on('error', fail); req.setTimeout(3000, () => req.destroy(new Error('probe timeout'))); req.end();
  });
  let ready = false;
  for (let retry = 0; retry < 60; retry++) { try { await get('app.primary.example'); ready = true; break; } catch {}
    if (child.exitCode !== null) break; await new Promise(done => setTimeout(done, 50)); }
  assert.ok(ready);
  assert.equal(await get('app.primary.example'), 200); assert.equal(await get('APP.RETAINED.EXAMPLE'), 200);
  assert.equal(calls.at(-1).path, '/?preserve=query');
  assert.equal(await get('unknown.primary.example'), 404);
  const before = calls.length;
  for (const host of ['nested.app.primary.example', 'primary.example', 'unrelated.example']) assert.equal(await get(host), 421);
  assert.equal(calls.length, before, 'hosts outside the one-label app zones never reach gateway');
});
