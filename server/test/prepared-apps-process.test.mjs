import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const memory = () => { let value; return { async read() { return structuredClone(value ?? null); },
  async claim(next) { value ??= structuredClone(next); return structuredClone(value); },
  async compareAndSwap(revision, next) { assert.equal(value.localRevision, revision); value = structuredClone(next); return structuredClone(value); } }; };
async function listen(server) { await new Promise((done, fail) => { server.once('error', fail); server.listen(0, '127.0.0.1', done); }); }
async function freePort() { const server = createServer(); await listen(server); const port = server.address().port; await new Promise(done => server.close(done)); return port; }
async function until(check, timeout = 15000) { const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { const result = await check(); if (result) return result; await new Promise(done => setTimeout(done, 50)); }
  throw Error('real_prepared_connector_timeout'); }

for (const [kind, file] of [['source', 'scripts/soty-connector.mjs'], ['release', 'public/agent/soty-connector.mjs']]) {
  test(`real ${kind} connector supports prepared app recovery and owner isolation`, { timeout: 60000 }, async t => {
    const parent = resolve(tmpdir()), root = await mkdtemp(join(parent, 'soty-prepared-process-'));
    const connectorDir = join(root, 'connector'), projects = join(root, 'projects'), project = join(projects, 'sample');
    const dist = join(root, 'dist'), dataDir = join(root, 'data');
    await mkdir(connectorDir); await mkdir(join(project, '.soty'), { recursive: true }); await mkdir(dist);
    await writeFile(join(dist, 'index.html'), '<!doctype html>');
    await writeFile(join(connectorDir, 'connector-config.json'), JSON.stringify({ workspaceRoot: projects, allowedRoots: [projects] }));
    const appServer = createServer((_request, response) => response.end('Real connector fixture'));
    let app, child, outputBytes = 0, loseAck = false; const clients = [], sockets = new Set();
    const shell = createServer((request, response) => app ? app(request, response) : response.writeHead(503).end());
    for (const server of [appServer, shell]) server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
    await Promise.all([listen(appServer), listen(shell)]); const origin = `http://127.0.0.1:${shell.address().port}`;
    await writeFile(join(project, '.soty', 'app.json'), JSON.stringify({ schema: 'soty.local-app.v1', name: 'Real prepared fixture', port: appServer.address().port, entryPath: '/' }));
    const options = { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${shell.address().port}` };
    app = createHttpApp(dist, options);
    shell.on('upgrade', (request, socket, head) => { if (!app.locals.appsService.handleUpgrade(request, socket, head)) socket.destroy(); });
    t.after(async () => {
      if (child && child.exitCode === null && child.signalCode === null) { const exited = new Promise(done => child.once('exit', done)); child.kill(); await exited; }
      clients.forEach(client => client.dispose()); const closing = Promise.all([appServer, shell].map(server => new Promise(done => server.close(done))));
      sockets.forEach(socket => socket.destroy()); await app.locals.closeServices(); await closing;
      assert.equal(dirname(resolve(root)), parent); assert.match(basename(root), /^soty-prepared-process-/u);
      await rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    });
    if (kind === 'release') {
      const manifest = JSON.parse(await readFile(join(repo, 'public/agent/manifest.json'), 'utf8'));
      const source = await readFile(join(repo, 'scripts/soty-connector.mjs'), 'utf8');
      const version = /const connectorVersion = "([^"]+)"/u.exec(source)?.[1];
      assert.ok(version); assert.equal(manifest.version, version);
      assert.equal(digest(await readFile(join(repo, file))), manifest.sha256);
    }
    const port = await freePort(), inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) => ['PATH','PATHEXT','SYSTEMROOT','WINDIR','COMSPEC'].includes(key.toUpperCase())));
    child = spawn(process.execPath, [join(repo, file)], { cwd: connectorDir, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], env: { ...inherited,
      TEMP: root, TMP: root, USERPROFILE: root, APPDATA: join(root, 'appdata'), LOCALAPPDATA: join(root, 'localappdata'),
      SOTY_CONNECTOR_DATA_DIR: connectorDir, SOTY_CONNECTOR_PORT: String(port), SOTY_CONNECTOR_SERVER_URL: origin,
      SOTY_CONNECTOR_UPDATE_URL: `${origin}/agent/manifest.json`,
      SOTY_CONNECTOR_LINK_ID: 'prepared_real_process_link_123456789012345', SOTY_CONNECTOR_DEVICE_ID: 'prepared_real_host',
      SOTY_CONNECTOR_SCOPE: 'Dev', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0', SOTY_OPENCODE_PATH: join(root, 'no-agent') } });
    for (const stream of [child.stdout, child.stderr]) stream.on('data', value => { outputBytes += value.length; });
    const claim = await until(async () => {
      assert.equal(child.exitCode, null, `connector exited (${outputBytes} diagnostic bytes)`);
      try { const response = await fetch(`http://127.0.0.1:${port}/apps/claim`, { method: 'POST', headers: { origin, 'content-type': 'application/json' }, body: '{}' });
        return response.ok ? await response.json() : null; } catch { return null; }
    });
    const makeClient = () => {
      const client = createClientWithStorage({ projectId: 'soty', endpoint: `${origin}/api/connect/rpc`,
        fetch: async (url, options) => { const response = await fetch(url, { ...options, headers: { ...options.headers, origin } });
          if (loseAck && JSON.parse(options.body || '{}').op === 'apps.prepared.create') { loseAck = false; await response.arrayBuffer(); throw Error('synthetic lost response'); }
          return response; } }, memory()); clients.push(client); return client;
    };
    const owner = makeClient(), account = await owner.bootstrap('Owner'), other = makeClient(), stranger = await other.bootstrap('Other');
    await owner.extension('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
    const target = { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, expectedAccountId: account.accountId };
    await assert.rejects(other.extension('apps.prepared.browse', { ...target, expectedAccountId: stranger.accountId }));
    const roots = await owner.extension('apps.prepared.browse', target); assert.equal(roots.roots.length, 1);
    const location = { rootId: roots.roots[0].id, segments: ['sample'] };
    const selected = await owner.extension('apps.prepared.browse', { ...target, location }); assert.equal(selected.project.responding, true);
    const args = { ...target, location, requestId: `real-prepared-${kind}`, manifestDigest: selected.project.digest };
    loseAck = true; await assert.rejects(owner.extension('apps.prepared.create', args));
    const recovered = await owner.extension('apps.prepared.read', args); assert.equal(recovered.state, 'ready');
    assert.equal((await owner.extension('apps.list')).apps.length, 1); assert.equal((await other.extension('apps.list')).apps.length, 0);
    await assert.rejects(other.extension('apps.launch', { appId: recovered.app.id }));
    const phone = makeClient(), enrollment = await phone.startEnrollment('Second device');
    await owner.approveEnrollment(enrollment.requestId); const preview = await phone.previewEnrollment(enrollment.requestId);
    await phone.finishEnrollment(enrollment.requestId, preview.account.accountId);
    const fromPhone = await phone.extension('apps.prepared.read', args); assert.equal(fromPhone.app.id, recovered.app.id);
    assert.equal(fromPhone.feedback, 'unconfirmed');
    await assert.rejects(phone.extension('apps.prepared.create', args), { code: 'job_request_conflict' });
    await phone.revokeDevice(account.deviceId);
    await app.locals.closeServices(); app = createHttpApp(dist, options);
    assert.equal((await phone.extension('apps.prepared.read', args)).app.id, recovered.app.id);
    await phone.extension('apps.revoke', { appId: recovered.app.id });
    assert.equal((await phone.extension('apps.prepared.read', args)).state, 'revoked');
    await assert.rejects(phone.extension('apps.prepared.create', args), { code: 'job_request_conflict' });
    assert.equal((await phone.extension('apps.list')).apps.length, 1);
  });
}
