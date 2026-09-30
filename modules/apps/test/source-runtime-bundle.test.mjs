import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { createHash, generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, delimiter, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket, { WebSocketServer } from 'ws';
import { createHttpApp } from '../../../server/http-app.js';
import { digestArgs } from '../../connect/server/index.mjs';

const enabled = process.env.SOTY_APPS_BUNDLE_TEST === '1';
const pause = ms => new Promise(done => setTimeout(done, ms));
async function until(check, label, timeout = 12_000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { const value = await check(); if (value) return value; await pause(50); }
  throw new Error(`bundle_gate_timeout:${label}`);
}
function principal() {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
async function call(port, path, { origin, host, cookie, method = 'GET', body } = {}) {
  return new Promise((done, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path, method, agent: false,
      headers: { ...(origin ? { origin } : {}), ...(host ? { host } : {}), ...(cookie ? { cookie } : {}), ...(body ? { 'content-type': 'application/json' } : {}) } }, res => {
      const bytes = []; res.on('data', data => bytes.push(data)); res.on('error', reject);
      res.on('end', () => { const text = Buffer.concat(bytes).toString(); let json; try { json = JSON.parse(text); } catch {}
        done({ status: res.statusCode, headers: res.headers, text, json }); });
    });
    req.setTimeout(5000, () => req.destroy(new Error('bundle_http_timeout'))); req.on('error', reject); req.end(body ? JSON.stringify(body) : undefined);
  });
}
async function rpc(port, origin, actor, op, args = {}) {
  const challenge = (await call(port, '/api/connect/rpc', { origin, method: 'POST', body: { protocol: 1, op: 'challenge', args: { operation: op, digest: digestArgs(args) } } })).json;
  assert.equal(challenge?.ok, true, challenge?.error?.code);
  const signature = sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url');
  return (await call(port, '/api/connect/rpc', { origin, method: 'POST', body: { protocol: 1, op, args,
    proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk, signature } } })).json;
}
async function listen(server) { await new Promise(done => server.listen(0, '127.0.0.1', done)); return server.address().port; }
async function vacantPort() { const server = http.createServer(), port = await listen(server); await new Promise(done => server.close(done)); return port; }
async function source(label) {
  const sockets = new Set(), ws = new WebSocketServer({ noServer: true });
  const server = http.createServer((req, res) => { res.setHeader('content-type', 'text/plain'); res.end(`${label}:${req.url}`); });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => ws.handleUpgrade(req, socket, head, client => { client.send(`${label}:hello`); client.on('message', value => client.send(`${label}:${value}`)); }));
  const port = await listen(server);
  return { port, async close() { for (const client of ws.clients) client.terminate(); for (const socket of sockets) socket.destroy(); ws.close(); await new Promise(done => server.close(done)); } };
}

test('immutable installed connector bundle preserves source identity across signed promotion, restart and rollback', { skip: !enabled, timeout: 100_000 }, async t => {
  const repo = fileURLToPath(new URL('../../../', import.meta.url));
  const artifact = join(repo, 'public', 'agent', 'soty-connector.mjs');
  const manifest = JSON.parse(await readFile(join(repo, 'public', 'agent', 'manifest.json'), 'utf8'));
  const bytes = await readFile(artifact), sha256 = createHash('sha256').update(bytes).digest('hex');
  assert.equal(manifest.schema, 'soty.connector.release.v1'); assert.equal(manifest.connectorUrl, '/agent/soty-connector.mjs');
  assert.equal(manifest.sha256, sha256);
  assert.match(manifest.version, /^\d+\.\d+\.\d+$/u);
  const [major, minor] = manifest.version.split('.').map(Number);
  assert.ok(major > 1 || (major === 1 && minor >= 4), 'C2-B requires a newly built connector release >=1.4.0');
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-bundle-independent-'));
  const runtimeDir = join(directory, 'installed'), dataDir = join(directory, 'server'), dist = join(directory, 'dist');
  await mkdir(runtimeDir); await mkdir(dist);
  const installed = join(runtimeDir, 'soty-connector.mjs'); await writeFile(installed, bytes);
  assert.equal(createHash('sha256').update(await readFile(installed)).digest('hex'), manifest.sha256);
  await writeFile(join(dist, 'index.html'), '<!doctype html><title>Bundle acceptance</title>');
  await writeFile(join(runtimeDir, 'connector-config.json'), JSON.stringify({ workspaceRoot: runtimeDir, allowedRoots: [runtimeDir] }));
  let app, child; let outputBytes = 0;
  const sockets = new Set(), webSockets = new Set();
  const gateway = http.createServer((req, res) => { if (app) app(req, res); else res.writeHead(503).end(); });
  gateway.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  const A = await source('A'), B = await source('B');
  async function stopChild() {
    if (!child || child.exitCode !== null || child.signalCode !== null) return;
    const current = child; const ended = new Promise(done => current.once('exit', done)); current.kill('SIGTERM');
    await Promise.race([ended, pause(3500)]);
    if (current.exitCode === null && current.signalCode === null) { current.kill('SIGKILL'); await ended; }
  }
  t.after(async () => {
    await stopChild(); for (const socket of webSockets) socket.terminate(); await app?.locals.closeServices();
    for (const socket of sockets) socket.destroy(); await new Promise(done => gateway.close(done)); await Promise.all([A.close(), B.close()]);
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-source-bundle-independent-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const port = await listen(gateway), localPort = await vacantPort(), origin = `http://127.0.0.1:${port}`;
  app = createHttpApp(dist, { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${port}`, localConnectorPort: localPort });
  gateway.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  const safeEnv = {
    PATH: [dirname(process.execPath), process.env.SystemRoot ? join(process.env.SystemRoot, 'System32') : '/usr/bin', '/bin'].join(delimiter),
    ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}),
    TEMP: directory, TMP: directory, USERPROFILE: directory, APPDATA: join(directory, 'appdata'), LOCALAPPDATA: join(directory, 'localappdata'),
    SOTY_CONNECTOR_DATA_DIR: runtimeDir, SOTY_CONNECTOR_SERVER_URL: origin,
    SOTY_CONNECTOR_LINK_ID: 'bundle_source_link_' + randomBytes(8).toString('hex'), SOTY_CONNECTOR_DEVICE_ID: 'bundle_source_host',
    SOTY_CONNECTOR_DEVICE_NICK: 'Bundle synthetic device', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_AGENT_AUTO_UPDATE: '0',
    SOTY_CONNECTOR_MANAGED: '0', SOTY_CONNECTOR_UPDATE_URL: `${origin}/agent/manifest.json`,
  };
  async function startChild() {
    child = spawn(process.execPath, [installed, '--port', String(localPort), '--scope', 'Dev'], { cwd: runtimeDir, windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'], env: safeEnv });
    child.stdout.on('data', data => { outputBytes += data.length; }); child.stderr.on('data', data => { outputBytes += data.length; });
    child.on('error', () => {});
    await until(async () => {
      assert.equal(child.exitCode, null, `bundle exited (captured output bytes=${outputBytes})`);
      try { return (await call(localPort, '/health', { origin })).json?.ok === true; } catch { return false; }
    }, 'installed connector health', 45_000);
  }
  await startChild();
  assert.equal((await call(localPort, '/apps/claim', { method: 'POST', body: {} })).status, 403);
  const claim = await until(async () => { const result = await call(localPort, '/apps/claim', { method: 'POST', origin, body: {} }); return result.status === 200 ? result.json : null; }, 'actual local claim', 25_000);
  const actor = principal();
  const boot = await rpc(port, origin, actor, 'bootstrap', { label: 'Bundle owner', encryptionPublicJwk: actor.encryptionPublicJwk }); assert.equal(boot.ok, true);
  const signed = (op, args = {}) => rpc(port, origin, actor, op, { expectedAccountId: boot.accountId, ...args });
  const ids = { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId };
  assert.equal((await signed('apps.claim', { ...ids, claimCode: claim.claimCode })).ok, true);
  const created = await signed('apps.register', { ...ids, name: 'Installed source app', port: A.port, entryPath: '/' }); assert.equal(created.ok, true, created.error?.code);
  const appId = created.app.id;
  const policy = async () => { const result = await signed('apps.publication.get', { appId }); assert.equal(result.ok, true, result.error?.code); return result; };
  async function launch() { return until(async () => { const value = await signed('apps.launch', { appId }); return value.ok ? value : null; }, 'fresh exact binding'); }
  async function session() {
    const url = new URL((await launch()).launchUrl);
    const response = await call(port, '/_soty/session', { host: url.host, origin: url.origin, method: 'POST', body: { ticket: url.hash.slice(1) } });
    assert.equal(response.status, 200, response.json?.error); return { host: url.host, origin: url.origin, cookie: response.headers['set-cookie'][0].split(';')[0] };
  }
  async function wsHello(auth, expected) {
    const socket = new WebSocket(`ws://127.0.0.1:${port}/live`, { headers: { Host: auth.host, Origin: auth.origin, Cookie: auth.cookie } });
    webSockets.add(socket); socket.on('error', () => {});
    const value = await new Promise((done, reject) => { const timer = setTimeout(() => reject(new Error('bundle_ws_timeout')), 5000);
      socket.once('message', data => { clearTimeout(timer); done(data.toString()); }); socket.once('error', error => { clearTimeout(timer); reject(error); }); });
    assert.equal(value, `${expected}:hello`); return { socket, closed: new Promise(done => socket.once('close', done)) };
  }
  async function prepare(selection) {
    const current = await policy(); const result = await signed('apps.source.prepare', { appId, expectedPolicyEpoch: current.policyEpoch,
      expectedTargetRevision: current.activeTargetRevision, ...selection }); assert.equal(result.ok, true, result.error?.code); return result;
  }
  const intent = (prepared, requestId) => ({ appId, preparationId: prepared.preparationId, requestId,
    expectedPolicyEpoch: prepared.expectedPolicyEpoch, expectedTargetRevision: prepared.expectedTargetRevision, launchPolicy: 'restricted', listed: false });
  const initial = await session(); assert.equal((await call(port, '/', initial)).text, 'A:/');
  const firstSocket = await wsHello(initial, 'A');
  const candidate = { source: { ...ids, port: B.port, entryPath: '/new-source?value=a%2Bb#view' } };
  const stale = await prepare(candidate);
  await stopChild(); await firstSocket.closed; await startChild(); await launch();
  const denied = await signed('apps.source.promote', intent(stale, 'old-process-proof')); assert.equal(denied.ok, false);
  assert.equal(denied.error.code, 'apps_source_preparation_stale'); assert.equal((await policy()).activeTargetRevision, 1);
  const fresh = await prepare(candidate), pending = intent(fresh, 'actual-bundle-switch');
  const moved = await signed('apps.source.promote', pending); assert.equal(moved.ok, true, moved.error?.code); assert.equal(moved.receipt.targetRevision, 2);
  const after = await session(); assert.equal(after.origin, initial.origin); assert.equal((await call(port, '/', after)).text, 'B:/');
  assert.equal((await call(port, '/', initial)).status, 403);
  const secondSocket = await wsHello(after, 'B');
  const back = await prepare({ targetRevision: 1 }); const returned = await signed('apps.source.promote', intent(back, 'actual-bundle-return'));
  assert.equal(returned.ok, true, returned.error?.code); await secondSocket.closed;
  const final = await session(); assert.equal(final.origin, initial.origin); assert.equal((await call(port, '/', final)).text, 'A:/');
  await wsHello(final, 'A');
  const history = await signed('apps.source.history', { appId }); assert.equal(history.ok, true); assert.equal(history.requiredBindingVersion, 2);
  assert.equal(history.activeTargetRevision, 1); assert.equal(history.targets.length, 2);
  const replay = await signed('apps.source.promote', pending); assert.equal(replay.ok, true); assert.equal(replay.replayed, true);
  assert.equal(replay.receipt.targetRevision, 2); assert.equal(replay.current.activeTargetRevision, 1);
  t.diagnostic(`installed artifact ${manifest.version}; sha256 ${sha256}; actual child process and signed HTTP; no inference or production operation`);
});
