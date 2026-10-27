import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { lstat, mkdir, mkdtemp, open, opendir, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { execFileSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { pathToFileURL } from 'node:url';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { parseSourceJson } from '../../modules/source-app/shared/strict-json.mjs';
import { createPreparedProjectBrowser } from '../../scripts/agent-modules/prepared-projects.mjs';
import { createLocalAppsRuntime, normalizeLocalAppManifest, probeLocalApp } from '../../scripts/agent-modules/local-apps.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const secret = () => randomBytes(32).toString('base64url');
const until = async check => { const end = Date.now() + 8000; while (Date.now() < end) { if (await check()) return; await new Promise(done => setTimeout(done, 20)); } throw Error('fixture_timeout'); };
const memory = () => { let value; return { async read() { return structuredClone(value ?? null); },
  async claim(next) { value ??= structuredClone(next); return structuredClone(value); },
  async compareAndSwap(revision, next) { assert.equal(value.localRevision, revision); value = structuredClone(next); return structuredClone(value); } }; };
function appRequest(url, pathname, { method = 'GET', headers = {}, body } = {}) {
  return new Promise((resolve, reject) => { const req = request({ hostname: '127.0.0.1', port: url.port, path: pathname, method, headers: { host: url.host, ...headers } }, res => {
    const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString() }));
  }); req.on('error', reject); req.end(body); });
}
const gate = () => { let release, enter; const wait = new Promise(done => { release = done; }), entered = new Promise(done => { enter = done; }); return { release, enter, wait, entered }; };

async function fixture(t) {
  const folder = await mkdtemp(join(tmpdir(), 'soty-prepared-root-')), dataDir = join(folder, 'data'), dist = join(folder, 'dist'), project = join(folder, 'projects', 'sample');
  await mkdir(dist); await mkdir(join(project, '.soty'), { recursive: true }); await writeFile(join(dist, 'index.html'), '<!doctype html>');
  let app, loseAck = false, pause; const clients = [], sockets = new Set();
  const source = createServer((_req, res) => res.end('Prepared local app'));
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  for (const item of [source, server]) item.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await Promise.all([source, server].map(item => new Promise(done => item.listen(0, '127.0.0.1', done))));
  await writeFile(join(project, '.soty', 'app.json'), JSON.stringify({ schema: 'soty.local-app.v1', name: 'Покупки', port: source.address().port, entryPath: '/' }));
  const origin = `http://127.0.0.1:${server.address().port}`, options = { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${server.address().port}` };
  app = createHttpApp(dist, options);
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  const token = secret(), identity = { linkId: 'prepared_project_link_12345678901234567890', hostDeviceId: 'prepared_host', connectorId: 'prepared_connector', name: 'Test device' };
  const auth = { linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, token };
  const registered = await fetch(origin + '/api/connectors/register', { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ ...auth, scope: 'Dev', capabilities: ['apps'], agent: { id: 'opencode', provider: 'gonka', available: false } }) });
  assert.equal((await registered.json()).ok, true);
  const browser = createPreparedProjectBrowser({ allowedRoots: () => [join(folder, 'projects')], lstat, open, opendir, realpath,
    basename, isAbsolute, join, relative, sep, digest: hash, normalizeManifest: normalizeLocalAppManifest, parseJson: parseSourceJson,
    blockedPorts: [], probe: value => probeLocalApp({ httpRequest: request }, value) });
  let runtime;
  function startRuntime(supported = true) {
    runtime = createLocalAppsRuntime({ randomSecret: secret, digest: hash, createWebSocket: url => new WebSocket(url), httpRequest: request,
      encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
    { identity, token, serverUrl: origin, ...(supported ? { preparedBrowse: input => browser.list(input), preparedInspect: async input => {
      if (pause) { pause.enter(); await pause.wait; } return browser.inspect(input);
    } } : {}) });
    runtime.start(); return runtime;
  }
  startRuntime(); await until(() => runtime.status().connected);
  const makeClient = () => { const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
    fetch: async (url, options) => { const response = await fetch(url, { ...options, headers: { ...options.headers, origin } });
      if (loseAck && JSON.parse(options.body || '{}').op === 'apps.prepared.create') { loseAck = false; await response.arrayBuffer(); throw Error('synthetic lost response'); } return response; } }, memory());
    clients.push(client); return client; };
  const owner = makeClient(), account = await owner.bootstrap('Owner'), claim = await runtime.claim();
  await owner.extension('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  const target = { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId };
  const roots = await owner.extension('apps.prepared.browse', { ...target, expectedAccountId: account.accountId });
  const location = { rootId: roots.roots[0].id, segments: ['sample'] };
  const preview = await owner.extension('apps.prepared.browse', { ...target, expectedAccountId: account.accountId, location });
  assert.equal(preview.project.responding, true);
  t.after(async () => { pause?.release(); runtime.stop(); clients.forEach(client => client.dispose());
    const closing = Promise.all([source, server].map(item => new Promise(done => item.close(done)))); sockets.forEach(socket => socket.destroy());
    await app.locals.closeServices(); await closing;
    assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^soty-prepared-root-/u);
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });
  const args = { ...target, expectedAccountId: account.accountId, requestId: 'prepared-request-1', location, manifestDigest: preview.project.digest };
  return { owner, account, makeClient, args, auth, project, dataDir, source, service: () => app,
    loseNext: () => { loseAck = true; }, hold: () => { pause = gate(); return pause; }, release: () => { pause?.release(); pause = null; },
    async oldConnector() { runtime.stop(); startRuntime(false); await until(() => runtime.status().connected); },
    async newConnector() { runtime.stop(); startRuntime(true); await until(() => runtime.status().connected); },
    async restart() { await app.locals.closeServices(); app = createHttpApp(dist, options); await until(() => runtime.status().connected); } };
}

test('signed create connects one private running project, replays and respects its tombstone', { timeout: 30000 }, async t => {
  const f = await fixture(t), bob = f.makeClient(); await bob.bootstrap('Bob');
  await assert.rejects(bob.extension('apps.prepared.create', { ...f.args, expectedAccountId: undefined }));
  const [one, two] = await Promise.all([f.owner.extension('apps.prepared.create', f.args), f.owner.extension('apps.prepared.create', f.args)]);
  assert.equal(one.state, 'ready'); assert.equal(two.app.id, one.app.id); assert.deepEqual(one.app.grants, { accountIds: [], communityIds: [] });
  assert.equal((await f.owner.extension('apps.list')).apps.length, 1); assert.equal((await bob.extension('apps.list')).apps.length, 0);
  assert.equal((await f.service().locals.connectorService.store.state.jobs).length, 0);
  const launch = await f.owner.extension('apps.launch', { appId: one.app.id }), url = new URL(launch.launchUrl);
  const boot = await appRequest(url, '/_soty/session', { method: 'POST', headers: { origin: url.origin, 'content-type': 'application/json' }, body: JSON.stringify({ ticket: url.hash.slice(1) }) });
  assert.equal(boot.status, 200);
  const page = await appRequest(url, '/', { headers: { cookie: boot.headers['set-cookie'][0].split(';')[0] } });
  assert.equal(page.status, 200); assert.equal(page.body, 'Prepared local app');
  await assert.rejects(bob.extension('apps.launch', { appId: one.app.id }));
  await f.owner.extension('apps.revoke', { appId: one.app.id });
  assert.equal((await f.owner.extension('apps.prepared.read', f.args)).state, 'revoked');
  assert.equal((await f.owner.extension('apps.prepared.create', f.args)).state, 'revoked');
  await assert.rejects(f.owner.extension('apps.prepared.create', { ...f.args, manifestDigest: 'f'.repeat(64) }));
});

test('lost reply and Root restart read the exact previously committed card', { timeout: 30000 }, async t => {
  const f = await fixture(t); f.loseNext(); await assert.rejects(f.owner.extension('apps.prepared.create', f.args));
  const first = (await f.owner.extension('apps.list')).apps[0]; assert.ok(first);
  await f.restart(); const recovered = await f.owner.extension('apps.prepared.read', f.args);
  assert.equal(recovered.state, 'ready'); assert.equal(recovered.app.id, first.id);
  assert.equal((await f.owner.extension('apps.prepared.create', f.args)).app.id, first.id);
  assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('old connector cannot inspect; its reserved request can resume with the same intent', { timeout: 30000 }, async t => {
  const f = await fixture(t); await f.oldConnector();
  assert.equal((await f.owner.extension('apps.prepared.read', f.args)).state, 'not-found');
  await assert.rejects(f.owner.extension('apps.prepared.create', f.args));
  assert.equal((await f.owner.extension('apps.prepared.read', f.args)).state, 'pending');
  assert.equal((await f.owner.extension('apps.list')).apps.length, 0);
  await f.newConnector(); const result = await f.owner.extension('apps.prepared.create', f.args);
  assert.equal(result.state, 'ready'); assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('changed manifest fails closed until the same originally selected version is restored', { timeout: 30000 }, async t => {
  const f = await fixture(t), file = join(f.project, '.soty', 'app.json'), original = await readFile(file, 'utf8');
  await writeFile(file, original.replace('Покупки', 'Другая версия'));
  await assert.rejects(f.owner.extension('apps.prepared.create', f.args));
  assert.equal((await f.owner.extension('apps.prepared.read', f.args)).state, 'pending');
  assert.equal((await f.owner.extension('apps.list')).apps.length, 0);
  await writeFile(file, original); assert.equal((await f.owner.extension('apps.prepared.create', f.args)).state, 'ready');
});

test('failure after Apps commit is read back before anything is retried', { timeout: 30000 }, async t => {
  const f = await fixture(t), universal = f.service().locals.universalApps, original = universal.registerPreparedApp;
  universal.registerPreparedApp = (...args) => { original(...args); throw Error('synthetic failure after Apps commit'); };
  const result = await f.owner.extension('apps.prepared.create', f.args);
  assert.equal(result.state, 'ready'); assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
  await f.restart(); assert.equal((await f.owner.extension('apps.prepared.read', f.args)).app.id, result.app.id);
});

test('feedback lock does not block the private app; a later read retries feedback', { timeout: 30000 }, async t => {
  const f = await fixture(t), feedback = new DatabaseSync(join(f.dataDir, 'feedback', 'feedback.sqlite'));
  feedback.exec('BEGIN EXCLUSIVE');
  let appId;
  try {
    const result = await f.owner.extension('apps.prepared.create', f.args);
    assert.equal(result.state, 'ready'); assert.equal(result.feedback, 'waiting'); appId = result.app.id;
    const launch = await f.owner.extension('apps.launch', { appId }), url = new URL(launch.launchUrl);
    const boot = await appRequest(url, '/_soty/session', { method: 'POST', headers: { origin: url.origin, 'content-type': 'application/json' }, body: JSON.stringify({ ticket: url.hash.slice(1) }) });
    const page = await appRequest(url, '/', { headers: { cookie: boot.headers['set-cookie'][0].split(';')[0] } });
    assert.equal(page.status, 200); assert.equal(page.body, 'Prepared local app');
  } finally { feedback.exec('ROLLBACK'); feedback.close(); }
  const recovered = await f.owner.extension('apps.prepared.read', f.args);
  assert.equal(recovered.app.id, appId); assert.equal(recovered.feedback, 'ready');
});

test('revoking the initiating account device while its connector replies prevents registration', { timeout: 30000 }, async t => {
  const f = await fixture(t), phone = f.makeClient(), enrollment = await phone.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await phone.previewEnrollment(enrollment.requestId);
  await phone.finishEnrollment(enrollment.requestId, preview.account.accountId);
  const blocked = f.hold(), pending = f.owner.extension('apps.prepared.create', f.args); await blocked.entered;
  await phone.revokeDevice(f.account.deviceId); f.release(); await assert.rejects(pending);
  assert.equal((await phone.extension('apps.list')).apps.length, 0);
});

test('the real prior SQLite reader retains the immutable receipt through a write and re-upgrade', { timeout: 30000 }, async t => {
  const f = await fixture(t); await f.oldConnector(); await assert.rejects(f.owner.extension('apps.prepared.create', f.args));
  const directory = dirname(f.service().locals.connectorService.store.filePath), oldDir = join(dirname(directory), 'old-prepared-reader'); await mkdir(oldDir);
  for (const name of ['connector-store.js', 'connector-persistence.js', 'connector-model-authorization.js'])
    await writeFile(join(oldDir, name), execFileSync('git', ['show', `e371d14be0aba67892a389f630a6c029f298ac77:server/${name}`]));
  await writeFile(join(oldDir, 'package.json'), '{"type":"module"}');
  const legacy = await import(pathToFileURL(join(oldDir, 'connector-store.js')).href);
  const before = f.service().locals.connectorService.store.state.requests.find(item => item.prepared);
  await f.service().locals.closeServices(); const old = legacy.createConnectorStore(directory);
  try {
    await old.ready; assert.deepEqual(old.state.requests.find(item => item.id === before.id), before);
    const changed = await old.register({ ...f.auth, scope: 'Dev', deviceNick: 'Old reader write', capabilities: ['apps'], agent: { id: 'opencode', provider: 'gonka', available: false } }, f.auth.token);
    assert.equal(changed.ok, true);
  } finally { await old.close(); }
  await f.restart();
  assert.deepEqual(f.service().locals.connectorService.store.state.requests.find(item => item.id === before.id), before);
  assert.equal((await f.owner.extension('apps.prepared.read', f.args)).state, 'pending');
});
