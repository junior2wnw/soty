import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { spawn } from 'node:child_process';
import { generateKeyPairSync, sign, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';
import { createHttpApp } from '../../../server/http-app.js';
import { createConnectorStore } from '../../../server/connector-store.js';
import { digestArgs } from '../../connect/server/index.mjs';
import { createSampleApp } from '../examples/sample-app.mjs';

test('full installed-source runtime binds through exact-Origin local claim and signed Connect, then proxies HTTP and WS', { timeout: 70_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'soty-full-runtime-test-'));
  const runtimeDir = join(root, 'runtime'), dataDir = join(root, 'server'), dist = join(root, 'dist');
  await mkdir(runtimeDir); await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Integration fixture</title>');
  await writeFile(join(runtimeDir, 'connector-config.json'), JSON.stringify({ workspaceRoot: runtimeDir, allowedRoots: [runtimeDir] }));
  const localPort = await unusedPort(); let app;
  const server = createServer((req, res) => { if (app) app(req, res); else { res.writeHead(503); res.end(); } });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const serverPort = server.address().port, origin = `http://127.0.0.1:${serverPort}`;
  app = createHttpApp(dist, { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${serverPort}`, localConnectorPort: localPort });
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  const sample = await createSampleApp();
  const child = spawn(process.execPath, [fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)), '--port', String(localPort), '--scope', 'Dev'], {
    windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], cwd: runtimeDir,
    env: {
      PATH: [dirname(process.execPath), process.env.SystemRoot ? join(process.env.SystemRoot, 'System32') : '/usr/bin', '/bin'].join(process.platform === 'win32' ? ';' : ':'),
      ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}),
      TEMP: root, TMP: root, USERPROFILE: root, APPDATA: join(root, 'appdata'), LOCALAPPDATA: join(root, 'localappdata'),
      SOTY_CONNECTOR_DATA_DIR: runtimeDir, SOTY_CONNECTOR_SERVER_URL: origin, SOTY_CONNECTOR_LINK_ID: 'fixture_link_12345678901234567890',
      SOTY_CONNECTOR_DEVICE_ID: 'fixture_host_laptop', SOTY_CONNECTOR_DEVICE_NICK: 'Fixture laptop',
      SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0', SOTY_CONNECTOR_UPDATE_URL: `${origin}/agent/manifest.json`,
    },
  });
  let outputBytes = 0; child.stdout.on('data', value => { outputBytes += value.length; }); child.stderr.on('data', value => { outputBytes += value.length; });
  t.after(async () => {
    if (child.exitCode === null) {
      let timer; const ended = new Promise(resolve => child.once('exit', resolve)); child.kill('SIGTERM');
      await Promise.race([ended, new Promise(resolve => { timer = setTimeout(resolve, 3000); timer.unref(); })]); clearTimeout(timer);
    }
    if (child.exitCode === null) child.kill('SIGKILL');
    await sample.close(); await app.locals.closeServices(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const health = await waitFor(async () => {
    assert.equal(child.exitCode, null, `runtime exited early (output bytes: ${outputBytes})`);
    try { const value = await callHttp(localPort, '/health', { origin }); return value.status === 200 ? value.json : null; } catch { return null; }
  }, 60_000);
  assert.equal(health.ok, true);
  assert.equal((await callHttp(localPort, '/apps/claim', { method: 'POST', body: {} })).status, 403);
  assert.equal((await callHttp(localPort, '/apps/claim', { method: 'POST', origin: `http://localhost:${serverPort}`, body: {} })).status, 403);
  assert.equal((await callHttp(localPort, '/apps/claim', { method: 'GET', origin })).status, 405);
  const claim = await waitFor(async () => {
    const reply = await callHttp(localPort, '/apps/claim', { method: 'POST', origin, body: {} }); return reply.status === 200 ? reply.json : null;
  }, 60_000);
  assert.equal(claim.hostDeviceId, 'fixture_host_laptop'); assert.match(claim.claimCode, /^[A-Za-z0-9_-]{43}$/u);
  const alice = actor(), bob = actor(), outsider = actor();
  const rpc = (principal, op, args) => signedRpc(serverPort, origin, principal, op, args);
  const bootstrap = principal => rpc(principal, 'bootstrap', { label: 'Integration user', encryptionPublicJwk: principal.encryptionPublicJwk });
  const aliceAccount = await bootstrap(alice), bobAccount = await bootstrap(bob); await bootstrap(outsider);
  assert.equal(aliceAccount.ok, true); assert.equal(bobAccount.ok, true);
  const bound = await rpc(alice, 'apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  assert.equal(bound.ok, true, bound.error?.code);
  const devices = await rpc(alice, 'apps.devices', {}); assert.equal(devices.devices[0].hostDeviceId, claim.hostDeviceId);
  const registered = await rpc(alice, 'apps.register', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, name: 'Покупки', port: sample.port, grants: { accountIds: [bobAccount.accountId] } });
  assert.equal(registered.ok, true, registered.error?.code); const id = registered.app.id;
  await waitFor(async () => { const list = await rpc(alice, 'apps.list', {}); return list.apps?.find(item => item.id === id)?.state === 'ready'; }, 5000);
  const denied = await rpc(outsider, 'apps.launch', { appId: id }); assert.equal(denied.ok, false);
  const launched = await rpc(bob, 'apps.launch', { appId: id }); assert.equal(launched.ok, true, launched.error?.code);
  const url = new URL(launched.launchUrl), host = url.host, appOrigin = url.origin;
  const session = await callHttp(serverPort, '/_soty/session', { host, origin: appOrigin, method: 'POST', body: { ticket: url.hash.slice(1) } });
  assert.equal(session.status, 200); const cookie = session.headers['set-cookie'][0].split(';')[0];
  const home = await callHttp(serverPort, '/', { host, cookie, headers: { Accept: 'text/html' } });
  assert.equal(home.status, 200); assert.match(home.text, /Покупки/u); assert.match(home.headers['content-security-policy'], /sandbox allow-scripts allow-forms allow-same-origin/u);
  assert.equal(home.headers['x-frame-options'], undefined, 'main shell DENY must not leak into app gateway');
  const post = await callHttp(serverPort, '/api/items', { host, cookie, origin: appOrigin, method: 'POST', body: { text: 'Full runtime item' } });
  assert.equal(post.status, 200); assert.ok(post.json.some(item => item.text === 'Full runtime item'));
  const ws = new WebSocket(`ws://127.0.0.1:${serverPort}/live`, { headers: { Host: host, Origin: appOrigin, Cookie: cookie } });
  const first = await new Promise((resolve, reject) => { ws.once('message', resolve); ws.once('error', reject); }); assert.match(first.toString(), /Full runtime item/u);
  const closed = new Promise(resolve => ws.once('close', resolve));
  const revoke = await rpc(alice, 'apps.update', { appId: id, grants: {} }); assert.equal(revoke.ok, true); await closed;
  assert.equal((await callHttp(serverPort, '/api/items', { host, cookie })).status, 403);
});

test('typed app proposal persists through the real connector job store without making an inference call', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'soty-app-job-result-')); t.after(async () => { await store.close(); await rm(dir, { recursive: true, force: true }); });
  const store = createConnectorStore(dir), token = randomBytes(32).toString('base64url');
  const auth = { linkId: 'result_link_12345678901234567890', deviceId: 'result_device', connectorId: 'result_connector', token };
  assert.equal((await store.register({ ...auth, token: undefined, scope: 'Dev', protocol: 2, capabilities: ['agent'], agent: { id: 'opencode', provider: 'gonka', available: true } }, token)).ok, true);
  const created = await store.createJob({ linkId: auth.linkId, deviceId: auth.deviceId, kind: 'agent', input: { text: 'Create app', output: 'local-app' } });
  assert.equal(created.ok, true); const job = (await store.poll(auth)).jobs[0]; assert.equal(job.input.output, 'local-app');
  const proposal = { schema: 'soty.local-app.v1', name: 'Test result', port: 3000, entryPath: '/', sourceJobId: job.id };
  assert.equal((await store.finishJob(auth, job.id, { ok: true, exitCode: 0, text: 'Ready', agentId: 'opencode', appProposal: proposal })).ok, true);
  assert.deepEqual((await store.getJob(auth.linkId, job.id)).job.result.appProposal, proposal);
  await store.close();
  const reopened = createConnectorStore(dir);
  try { assert.deepEqual((await reopened.getJob(auth.linkId, job.id)).job.result.appProposal, proposal); }
  finally { await reopened.close(); }
});

function actor() { const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }); return { privateKey: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) }; }
async function signedRpc(port, origin, principal, op, args = {}) {
  const challenge = (await callHttp(port, '/api/connect/rpc', { origin, method: 'POST', body: { protocol: 1, op: 'challenge', args: { operation: op, digest: digestArgs(args) } } })).json;
  assert.equal(challenge.ok, true, challenge.error?.code);
  const signature = sign('sha256', Buffer.from(challenge.message), { key: principal.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url');
  return (await callHttp(port, '/api/connect/rpc', { origin, method: 'POST', body: { protocol: 1, op, args, proof: { challengeId: challenge.challengeId, publicJwk: principal.publicJwk, signature } } })).json;
}
async function waitFor(check, timeout) { const until = Date.now() + timeout; while (Date.now() < until) { const value = await check(); if (value) return value; await new Promise(resolve => setTimeout(resolve, 100)); } throw new Error('integration condition timeout'); }
async function unusedPort() { const server = createServer(); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve)); const port = server.address().port; await new Promise(resolve => server.close(resolve)); return port; }
function callHttp(port, path, { host, cookie, origin, method = 'GET', body, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path, method, headers: { ...(host ? { Host: host } : {}), ...(cookie ? { Cookie: cookie } : {}), ...(origin ? { Origin: origin } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}), ...headers }, timeout: 5000 }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.on('error', reject); res.on('end', () => { const text = Buffer.concat(chunks).toString('utf8'); let json = null; try { json = JSON.parse(text); } catch {} resolve({ status: res.statusCode, headers: res.headers, text, json }); });
    }); req.on('error', reject); req.on('timeout', () => req.destroy(new Error('fixture_http_timeout'))); if (body) req.write(JSON.stringify(body)); req.end();
  });
}
