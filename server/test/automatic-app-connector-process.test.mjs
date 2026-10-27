import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { chmod, mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { execFileSync, spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { attachConnectorApi } from '../connector-api.js';

const compiler = 'C:\\Windows\\Microsoft.NET\\Framework64\\v4.0.30319\\csc.exe';
const skip = process.platform === 'win32' && !existsSync(compiler) ? 'Windows C# compiler is unavailable' : false;
const digest = value => createHash('sha256').update(value).digest('hex');
async function waitFor(check, timeout = 15000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { const value = await check(); if (value) return value; await new Promise(done => setTimeout(done, 25)); }
  throw new Error('connector_process_fixture_timeout');
}

test('actual old release cannot lease auto work after a stale new-process heartbeat', { skip, timeout: 45000 }, async t => {
  const parent = resolve(tmpdir()), root = await mkdtemp(join(parent, 'soty-auto-owner-legacy-'));
  const runtime = join(root, 'runtime'), dataDir = join(root, 'data'), marker = join(root, 'legacy-spawned');
  await mkdir(runtime); await writeFile(join(runtime, 'connector-config.json'), JSON.stringify({ workspaceRoot: runtime, allowedRoots: [runtime] }));
  const oldCommit = '2b770d8c2c34ae513e83a1c5bd489f1414c36b3b';
  const oldBundle = execFileSync('git', ['show', `${oldCommit}:public/agent/soty-connector.mjs`], { maxBuffer: 10_000_000 });
  const oldManifest = JSON.parse(execFileSync('git', ['show', `${oldCommit}:public/agent/manifest.json`], { maxBuffer: 10_000_000 }).toString('utf8'));
  assert.equal(oldManifest.version, '1.4.12'); assert.equal(digest(oldBundle), oldManifest.sha256);
  const oldPath = join(root, 'old-connector.mjs'); await writeFile(oldPath, oldBundle);
  const executable = await fakeAgent(root), firstPort = await unusedPort(), oldPort = await unusedPort();
  let paused = false, releaseRegistration, first, old, outputBytes = 0;
  const registrationGate = new Promise(done => { releaseRegistration = done; });
  const app = express();
  app.use((req, _res, next) => { if (paused && req.path === '/api/connectors/register') { void registrationGate.then(next); return; } next(); });
  const { store } = attachConnectorApi(app, { dataDir, gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' },
    storeOptions: { automaticJobActive: () => true } });
  const server = createServer(app); await listen(server); const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    releaseRegistration();
    for (const child of [first, old]) if (child && child.exitCode === null && child.signalCode === null) {
      const exited = new Promise(done => child.once('exit', done)); child.kill(); await exited;
    }
    server.closeAllConnections(); await new Promise(done => server.close(done)); await store.close();
    assert.equal(dirname(resolve(root)), parent); assert.match(basename(root), /^soty-auto-owner-legacy-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  const inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) => ['PATH','PATHEXT','SYSTEMROOT','WINDIR','COMSPEC'].includes(key.toUpperCase())));
  function start(path, port) {
    const child = spawn(process.execPath, [path], { cwd: runtime, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], env: { ...inherited,
      TEMP: root, TMP: root, USERPROFILE: root, APPDATA: join(root, 'appdata'), LOCALAPPDATA: join(root, 'localappdata'),
      SOTY_FIXTURE_MARKER: marker, SOTY_OPENCODE_PATH: executable, SOTY_CONNECTOR_DATA_DIR: runtime,
      SOTY_CONNECTOR_PORT: String(port), SOTY_CONNECTOR_SERVER_URL: origin, SOTY_SPREADEX_BASE_URL: origin,
      SOTY_CONNECTOR_LINK_ID: 'auto_owner_legacy_link_123456789012345', SOTY_CONNECTOR_DEVICE_ID: 'auto_owner_host',
      SOTY_CONNECTOR_SCOPE: 'Dev', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0' } });
    for (const stream of [child.stdout, child.stderr]) stream.on('data', value => { outputBytes += value.length; });
    return child;
  }
  first = start(fileURLToPath(new URL('../../public/agent/soty-connector.mjs', import.meta.url)), firstPort);
  const host = await waitFor(() => {
    assert.equal(first.exitCode, null, `new connector exited (${outputBytes} diagnostic bytes)`);
    return store.state?.connectors[0] && store.automaticPolls.size ? store.state.connectors[0] : null;
  });
  const firstExit = new Promise(done => first.once('exit', done)); first.kill(); await firstExit;
  assert.ok(host.capabilities.includes('soty.auto-owner-check.v1')); assert.equal(store.automaticPolls.size, 1);
  const access = requestId => ({ ownerAccountId: 'fixture-owner', sourceActorDeviceId: 'fixture-device', guard: () => true,
    connectorId: host.connectorId, expectedDeviceId: host.deviceId, requestId, requestIntent: digest(requestId) });
  const create = (requestId, automatic) => store.createJob({ linkId: host.linkId, deviceId: host.deviceId, kind: 'agent',
    input: { kind: 'agent', text: 'Synthetic task', output: 'local-app', ...(automatic ? { autoRegister: true } : {}) } }, access(requestId));
  const queued = await create('legacy-stale-heartbeat', true); assert.ok(queued.job?.id);
  paused = true;
  let oldResult;
  const lease = store.lease.bind(store);
  store.lease = async (auth, options = {}) => { const value = await lease(auth, options); if (!options.automaticOwnerGuard && value.ok) oldResult = value; return value; };
  old = start(oldPath, oldPort);
  await waitFor(() => oldResult);
  assert.equal(oldResult.jobs.length, 0); assert.equal(store.state.jobs.find(job => job.id === queued.job.id).status, 'queued');
  assert.equal(store.automaticPolls.size, 0);
  assert.equal((await create('legacy-new-auto', true)).admission?.reason, 'app_auto_device_update_required');
  paused = false; releaseRegistration();
  await waitFor(() => !store.state.connectors[0].capabilities.includes('soty.auto-owner-check.v1'));
  const manual = await create('legacy-manual', false); assert.ok(manual.job?.id);
  await waitFor(() => existsSync(marker)); assert.equal(await readFile(marker, 'utf8'), 'x');
  assert.equal(store.state.jobs.find(job => job.id === queued.job.id).status, 'queued');
});
async function listen(server) { await new Promise((done, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', done); }); }
async function unusedPort() { const server = createServer(); await listen(server); const port = server.address().port; await new Promise(done => server.close(done)); return port; }

async function fakeAgent(root) {
  if (process.platform === 'win32') {
    const source = join(root, 'fixture.cs'), executable = join(root, 'fixture-agent.exe');
    await writeFile(source, `using System; using System.IO; class Fake { static void Main(string[] a) { if (a.Length>0 && a[0]=="--version") { Console.WriteLine("1.2.3"); return; } File.AppendAllText(Environment.GetEnvironmentVariable("SOTY_FIXTURE_MARKER"), "x"); Console.WriteLine("{\\\"type\\\":\\\"text\\\",\\\"part\\\":{\\\"text\\\":\\\"Done\\\"}}"); } }`);
    execFileSync(compiler, ['/nologo', '/target:exe', `/out:${executable}`, source], { windowsHide: true, stdio: 'ignore' });
    return executable;
  }
  const executable = join(root, 'fixture-agent');
  await writeFile(executable, '#!/bin/sh\nif [ "$1" = "--version" ]; then echo 1.2.3; exit 0; fi\nprintf x >> "$SOTY_FIXTURE_MARKER"\nprintf \'{"type":"text","part":{"text":"Done"}}\\n\'\n');
  await chmod(executable, 0o700); return executable;
}

for (const [kind, connectorPath] of [['source', '../../scripts/soty-connector.mjs'], ['release bundle', '../../public/agent/soty-connector.mjs']]) {
test(`actual ${kind} connector checks current owner immediately before spawning even without local receipts`, { skip, timeout: 45000 }, async t => {
  const parent = resolve(tmpdir()), root = await mkdtemp(join(parent, 'soty-auto-owner-process-'));
  const runtime = join(root, 'runtime'), dataDir = join(root, 'data'), marker = join(root, 'spawned');
  await mkdir(runtime); await writeFile(join(runtime, 'connector-config.json'), JSON.stringify({ workspaceRoot: runtime, allowedRoots: [runtime] }));
  const executable = await fakeAgent(root), localPort = await unusedPort();
  let active = true, watchedJob = '', watchRequest = '', checks = 0, triggerRevoke = false, omitCurrentOwnerProof = false, child, outputBytes = 0;
  const app = express();
  app.use((req, res, next) => {
    const match = /^\/api\/connectors\/jobs\/([^/]+)\/runtime-status$/u.exec(req.path);
    if ((triggerRevoke || omitCurrentOwnerProof) && !watchedJob && match
        && store.state.jobs.some(job => job.id === match[1] && job.requestId === watchRequest)) watchedJob = match[1];
    if (watchedJob && req.path === `/api/connectors/jobs/${watchedJob}/runtime-status`) {
      checks++;
      if (omitCurrentOwnerProof) { res.json({ ok: true, job: { id: watchedJob, status: 'leased', cancelRequested: false } }); return; }
      if (triggerRevoke && checks === 2) active = false;
    }
    next();
  });
  const { store } = attachConnectorApi(app, { dataDir, gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' },
    storeOptions: { automaticJobActive: () => active } });
  const server = createServer(app); await listen(server); const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    if (child && child.exitCode === null && child.signalCode === null) {
      const exited = new Promise(done => child.once('exit', done)); child.kill(); await exited;
    }
    server.closeAllConnections(); await new Promise(done => server.close(done)); await store.close();
    assert.equal(dirname(resolve(root)), parent); assert.match(basename(root), /^soty-auto-owner-process-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  const inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) => ['PATH','PATHEXT','SYSTEMROOT','WINDIR','COMSPEC'].includes(key.toUpperCase())));
  child = spawn(process.execPath, [fileURLToPath(new URL(connectorPath, import.meta.url))], {
    cwd: runtime, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], env: { ...inherited,
      TEMP: root, TMP: root, USERPROFILE: root, APPDATA: join(root, 'appdata'), LOCALAPPDATA: join(root, 'localappdata'),
      SOTY_FIXTURE_MARKER: marker, SOTY_OPENCODE_PATH: executable, SOTY_CONNECTOR_DATA_DIR: runtime,
      SOTY_CONNECTOR_PORT: String(localPort), SOTY_CONNECTOR_SERVER_URL: origin, SOTY_SPREADEX_BASE_URL: origin,
      SOTY_CONNECTOR_LINK_ID: 'auto_owner_process_link_123456789012345', SOTY_CONNECTOR_DEVICE_ID: 'auto_owner_host',
      SOTY_CONNECTOR_SCOPE: 'Dev', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0' },
  });
  for (const stream of [child.stdout, child.stderr]) stream.on('data', value => { outputBytes += value.length; });
  const host = await waitFor(() => {
    assert.equal(child.exitCode, null, `connector exited (${outputBytes} diagnostic bytes)`);
    const connector = store.state?.connectors[0];
    return connector && store.automaticPolls.size > 0 ? connector : null;
  });
  const access = requestId => ({ ownerAccountId: 'fixture-owner', sourceActorDeviceId: 'fixture-device', guard: () => active,
    connectorId: host.connectorId, expectedDeviceId: host.deviceId, requestId, requestIntent: digest(requestId) });
  const create = requestId => store.createJob({ linkId: host.linkId, deviceId: host.deviceId, kind: 'agent',
    input: { kind: 'agent', text: 'Synthetic task', output: 'local-app', autoRegister: true } }, access(requestId));

  // The middleware revokes authority on the second runtime-status request.
  // A native process would write the marker if it were actually spawned.
  triggerRevoke = true; watchRequest = 'fixture-denied';
  const denied = await create('fixture-denied'); assert.ok(denied.job?.id); watchedJob = denied.job.id;
  await waitFor(() => store.state.jobs.find(job => job.id === denied.job.id)?.status === 'cancelled');
  assert.ok(checks >= 2); assert.equal(existsSync(marker), false);

  // Positive control proves the same real connector starts the fixture when
  // the original owner remains current; the fixture intentionally omits an app manifest.
  active = true; watchedJob = ''; checks = 0; triggerRevoke = false;
  await waitFor(() => store.automaticPolls.size > 0);
  const allowed = await create('fixture-allowed'); assert.ok(allowed.job?.id);
  await waitFor(() => existsSync(marker)); assert.equal(await readFile(marker, 'utf8'), 'x');
  await waitFor(() => ['failed', 'succeeded', 'cancelled'].includes(store.state.jobs.find(job => job.id === allowed.job.id)?.status));

  // An older Root can return a valid-looking runtime status without attesting
  // current ownership. The updated process must still refuse to spawn.
  watchedJob = ''; watchRequest = 'fixture-old-root'; checks = 0; omitCurrentOwnerProof = true;
  await waitFor(() => store.automaticPolls.size > 0);
  const unknown = await create('fixture-old-root'); assert.ok(unknown.job?.id);
  await waitFor(() => checks > 0); await new Promise(done => setTimeout(done, 300));
  assert.equal(await readFile(marker, 'utf8'), 'x');
  assert.equal(store.state.jobs.find(job => job.id === unknown.job.id).status, 'leased');
});
}
