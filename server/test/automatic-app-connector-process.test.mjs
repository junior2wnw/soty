import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { chmod, mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createServer, request } from 'node:http';
import { execFileSync, spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { attachConnectorApi } from '../connector-api.js';
import { pinnedHistory } from './support/pinned-history.mjs';

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
  const oldBundle = pinnedHistory(oldCommit, 'public/agent/soty-connector.mjs');
  const oldManifest = JSON.parse(pinnedHistory(oldCommit, 'public/agent/manifest.json').toString('utf8'));
  assert.equal(oldManifest.version, '1.4.12'); assert.equal(digest(oldBundle), oldManifest.sha256);
  const oldPath = join(root, 'old-connector.mjs'); await writeFile(oldPath, oldBundle);
  const executable = await fakeAgent(root), firstPort = await unusedPort(), oldPort = await unusedPort();
  let paused = false, releaseRegistration, first, old, outputBytes = 0;
  const registrationGate = new Promise(done => { releaseRegistration = done; });
  const app = express();
  app.use((req, _res, next) => { if (paused && req.path === '/api/connectors/register') { void registrationGate.then(next); return; } next(); });
  const { store } = attachConnectorApi(app, { dataDir, gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' },
    storeOptions: { automaticJobActive: () => true } });
  let releaseNewWait, newWaitEntered, settleNewPoll, newSignal, oldResult; const newLeases = [];
  const newWait = new Promise(done => { releaseNewWait = done; });
  const waiting = new Promise(done => { newWaitEntered = done; });
  const newPollSettled = new Promise(done => { settleNewPoll = done; });
  const wait = store.waitForChange.bind(store), poll = store.poll.bind(store), lease = store.lease.bind(store);
  store.waitForChange = async (...values) => {
    if (!newSignal && values[3]) { newSignal = values[3]; newWaitEntered(); await newWait; }
    return await wait(...values);
  };
  store.poll = async (...values) => {
    try { const result = await poll(...values); if (newSignal && values[2] === newSignal) settleNewPoll(result); return result; }
    catch (error) { if (newSignal && values[2] === newSignal) settleNewPoll({ ok: false, error: 'unexpected-poll-failure' }); throw error; }
  };
  store.lease = async (auth, options = {}) => {
    const value = await lease(auth, options);
    if (options.automaticOwnerGuard && value.ok) newLeases.push(value.jobs.map(job => job.id));
    if (!options.automaticOwnerGuard && value.ok) oldResult = value;
    return value;
  };
  const server = createServer(app); await listen(server); const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    releaseNewWait(); releaseRegistration();
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
      SOTY_CONNECTOR_PORT: String(port), SOTY_CONNECTOR_SERVER_URL: origin,
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
  await waiting;
  const firstExit = new Promise(done => first.once('exit', done)); first.kill(); await firstExit;
  await waitFor(() => newSignal.aborted);
  assert.ok(host.capabilities.includes('soty.auto-owner-check.v1')); assert.equal(store.automaticPolls.size, 1);
  const access = requestId => ({ ownerAccountId: 'fixture-owner', sourceActorDeviceId: 'fixture-device', guard: () => true,
    connectorId: host.connectorId, expectedDeviceId: host.deviceId, requestId, requestIntent: digest(requestId) });
  const create = (requestId, automatic) => store.createJob({ linkId: host.linkId, deviceId: host.deviceId, kind: 'agent',
    input: { kind: 'agent', text: 'Synthetic task', output: 'local-app', ...(automatic ? { autoRegister: true } : {}) } }, access(requestId));
  const queued = await create('legacy-stale-heartbeat', true); assert.ok(queued.job?.id);
  releaseNewWait(); const closedPoll = await newPollSettled;
  assert.deepEqual(newLeases, [[]], 'the closed new-process poll must not perform another lease');
  assert.equal(closedPoll.error, 'connector-poll-aborted');
  assert.equal(store.state.jobs.find(job => job.id === queued.job.id).status, 'queued');
  paused = true;
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

test('an HTTP poll already closed before its handler cannot lease work', { timeout: 30000 }, async t => {
  const parent = resolve(tmpdir()), root = await mkdtemp(join(parent, 'soty-auto-poll-closed-'));
  let entered, finished; const atMiddleware = new Promise(done => { entered = done; }), handled = new Promise(done => { finished = done; });
  const app = express();
  app.use((req, res, next) => { if (req.path !== '/api/connectors/poll') return next(); res.once('close', next); entered(); });
  const { store } = attachConnectorApi(app, { dataDir: join(root, 'data'), gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' },
    storeOptions: { automaticJobActive: () => true } });
  const server = createServer(app); await listen(server);
  t.after(async () => {
    server.closeAllConnections(); await new Promise(done => server.close(done)); await store.close();
    assert.equal(dirname(resolve(root)), parent); assert.match(basename(root), /^soty-auto-poll-closed-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  const token = 'fixture-token-' + 'x'.repeat(40), host = { linkId: 'closed-poll-link-12345678901234567890', deviceId: 'closed-host', connectorId: 'closed-connector' };
  const auth = { ...host, token };
  assert.equal((await store.register({ ...host, scope: 'CurrentUser', capabilities: ['agent','soty.auto-owner-check.v1'], agent: { id:'opencode', provider:'gonka', available:true } }, token)).ok, true);
  assert.equal((await store.poll(auth, 0, undefined, { automaticOwnerGuard:true })).ok, true);
  const requestId = 'closed-before-handler';
  const created = await store.createJob({ linkId:host.linkId, deviceId:host.deviceId, kind:'agent', input:{ kind:'agent', text:'Synthetic task', output:'local-app', autoRegister:true } },
    { ownerAccountId:'fixture-owner', sourceActorDeviceId:'fixture-device', guard:()=>true, connectorId:host.connectorId, expectedDeviceId:host.deviceId, requestId, requestIntent:digest(requestId) });
  assert.ok(created.job?.id);
  const poll = store.poll.bind(store);
  store.poll = async (...args) => { const alreadyAborted = args[2].aborted; const result = await poll(...args); finished({ alreadyAborted, result }); return result; };
  const params = new URLSearchParams({ ...host, autoOwnerGuard:'1' });
  const client = request(`http://127.0.0.1:${server.address().port}/api/connectors/poll?${params}`, { headers:{ authorization:'Bearer '+token } });
  client.on('error', () => {}); client.end(); await atMiddleware; client.destroy();
  const observed = await handled;
  assert.equal(store.state.jobs.find(job => job.id === created.job.id).status, 'queued');
  assert.equal(observed.alreadyAborted, true); assert.equal(observed.result.error, 'connector-poll-aborted');
  const fresh = await poll(auth, 0, undefined, { automaticOwnerGuard:true }); assert.deepEqual(fresh.jobs.map(job => job.id), [created.job.id]);
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
      SOTY_CONNECTOR_PORT: String(localPort), SOTY_CONNECTOR_SERVER_URL: origin,
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

test(`actual ${kind} connector reports closed stdin once and remains usable`,
  { skip: process.platform === 'win32' ? 'Unix pipe fixture' : false, timeout: 45000 }, async t => {
  const parent = resolve(tmpdir()), root = await mkdtemp(join(parent, 'soty-auto-owner-process-'));
  const runtime = join(root, 'runtime'), dataDir = join(root, 'data'), inputFile = join(root, 'input'), closeMarker = join(root, 'close-input');
  await mkdir(runtime); await writeFile(join(runtime, 'connector-config.json'), JSON.stringify({ workspaceRoot: runtime, allowedRoots: [runtime] }));
  const executable = join(root, 'fixture-agent');
  await writeFile(executable, '#!/bin/sh\nif [ "$1" = "--version" ]; then echo 1.2.3; exit 0; fi\nif [ -f "$SOTY_FIXTURE_CLOSE" ]; then exec 0<&-; printf \'{"type":"text","part":{"text":"Apparent success"}}\\n\'; exec sleep 30; fi\ncat > "$SOTY_FIXTURE_INPUT"\nprintf \'{"type":"text","part":{"text":"Done"}}\\n\'\n');
  await chmod(executable, 0o700);
  const localPort = await unusedPort(), results = new Map(), app = express();
  app.use((req, _res, next) => {
    const match = /^\/api\/connectors\/jobs\/([^/]+)\/result$/u.exec(req.path);
    if (match && req.method === 'POST') results.set(match[1], (results.get(match[1]) || 0) + 1);
    next();
  });
  const { store } = attachConnectorApi(app, { dataDir, gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
  const server = createServer(app); await listen(server); const origin = `http://127.0.0.1:${server.address().port}`;
  let child;
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
    cwd: runtime, stdio: ['ignore', 'pipe', 'pipe'], env: { ...inherited,
      TEMP: root, TMP: root, USERPROFILE: root, APPDATA: join(root, 'appdata'), LOCALAPPDATA: join(root, 'localappdata'),
      SOTY_FIXTURE_INPUT: inputFile, SOTY_FIXTURE_CLOSE: closeMarker, SOTY_OPENCODE_PATH: executable, SOTY_CONNECTOR_DATA_DIR: runtime,
      SOTY_CONNECTOR_PORT: String(localPort), SOTY_CONNECTOR_SERVER_URL: origin,
      SOTY_CONNECTOR_LINK_ID: 'stdin_fixture_link_123456789012345', SOTY_CONNECTOR_DEVICE_ID: 'stdin_fixture_host',
      SOTY_CONNECTOR_SCOPE: 'Dev', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0' },
  });
  child.stdout.resume(); child.stderr.resume();
  const host = await waitFor(() => {
    assert.equal(child.exitCode, null, 'connector exited before registration');
    return store.state?.connectors[0];
  });
  const access = requestId => ({ ownerAccountId: 'fixture-owner', sourceActorDeviceId: 'fixture-device', guard: () => true,
    connectorId: host.connectorId, expectedDeviceId: host.deviceId, requestId, requestIntent: digest(requestId) });
  const create = (requestId, text, context = '') => store.createJob({ linkId: host.linkId, deviceId: host.deviceId, kind: 'agent',
    input: { kind: 'agent', text, context } }, access(requestId));
  const terminal = job => waitFor(() => {
    const current = store.state.jobs.find(row => row.id === job.id);
    return ['failed', 'succeeded', 'cancelled'].includes(current?.status) ? current : null;
  });
  const first = await create('stdin-full', 'Complete synthetic prompt'); assert.ok(first.job?.id);
  assert.equal((await terminal(first.job)).status, 'succeeded');
  assert.equal(await readFile(inputFile, 'utf8'), 'Complete synthetic prompt');
  await writeFile(closeMarker, 'close');
  const broken = await create('stdin-closed', '漢'.repeat(64000), '漢'.repeat(128000)); assert.ok(broken.job?.id);
  const failed = await terminal(broken.job); assert.equal(failed.status, 'failed'); assert.match(failed.result.text, /EPIPE/u);
  assert.equal(child.exitCode, null, 'connector died after the broken pipe');
  await rm(closeMarker);
  const last = await create('stdin-recovered', 'Another complete prompt'); assert.ok(last.job?.id);
  assert.equal((await terminal(last.job)).status, 'succeeded');
  assert.equal(await readFile(inputFile, 'utf8'), 'Another complete prompt');
  for (const job of [first.job, broken.job, last.job]) assert.equal(results.get(job.id), 1);
});
}
