import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { probe } from '../../deploy/connect/maintenance-probe.mjs';
import { normalizeConnectorState } from '../connector-store.js';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createLocalAppsRuntime } from '../../scripts/agent-modules/local-apps.mjs';

const until = async (check, timeout = 8000) => {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { const value = await check(); if (value) return value; await new Promise(resolve => setTimeout(resolve, 20)); }
  throw new Error('automatic_app_fixture_timeout');
};
function memory() {
  let value;
  return { async read() { return structuredClone(value ?? null); }, async claim(candidate) { value ??= structuredClone(candidate); return structuredClone(value); },
    async compareAndSwap(revision, candidate) { assert.equal(value.localRevision, revision); value = structuredClone(candidate); return structuredClone(value); } };
}
function appRequest(url, pathname, { method = 'GET', headers = {}, body } = {}) {
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port: url.port, path: pathname, method, headers: { host: url.host, ...headers } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString() }));
    });
    req.on('error', reject); req.end(body);
  });
}
async function fixture(t) {
  const folder = await mkdtemp(join(tmpdir(), 'soty-auto-app-')), dist = join(folder, 'dist'), dataDir = join(folder, 'data');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html>');
  let app, runtime, loseCreateAck = false, loseArchiveAck = false; const clients = [], sockets = new Set();
  const source = createServer((_req, res) => res.end('Local app'));
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  for (const item of [source, server]) item.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await Promise.all([source, server].map(item => new Promise(resolve => item.listen(0, '127.0.0.1', resolve))));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const options = { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${server.address().port}`,
    localExecutorCurrent: true, gonka: { baseUrl: 'https://fixture.invalid/v1', apiKey: 'synthetic-test-only-key-123456', fetchImpl: async () => { throw Error('no network'); } } };
  app = createHttpApp(dist, options); assert.equal(app.locals.connectorService.modelProxy.ready, true);
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  const token = randomBytes(32).toString('base64url'), identity = { linkId: 'automatic_app_link_12345678901234567890', hostDeviceId: 'automatic_host', connectorId: 'automatic_connector', name: 'Test device' };
  const auth = { linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, token };
  const registered = await fetch(origin + '/api/connectors/register', { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ ...auth, scope: 'Dev', capabilities: ['apps', 'agent', 'soty.auto-owner-check.v1'], agent: { id: 'opencode', provider: 'gonka', available: true } }) });
  assert.equal((await registered.json()).ok, true);
  assert.equal((await app.locals.connectorService.store.poll(auth, 0, undefined, { automaticOwnerGuard: true })).ok, true);
  runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
    createWebSocket: url => new WebSocket(url), httpRequest: request, encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
    { identity, token, serverUrl: origin });
  runtime.start(); await until(() => runtime.status().connected);
  const makeClient = () => {
    const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
      fetch: async (url, options) => {
        const response = await fetch(url, { ...options, headers: { ...options.headers, origin } });
        if (loseCreateAck && JSON.parse(options.body || '{}').op === 'apps.agent.create') {
          loseCreateAck = false; await response.arrayBuffer(); throw new Error('synthetic lost create acknowledgement');
        }
        if (loseArchiveAck && JSON.parse(options.body || '{}').op === 'apps.agent.archive') {
          loseArchiveAck = false; await response.arrayBuffer(); throw new Error('synthetic lost archive acknowledgement');
        }
        return response;
      } }, memory());
    clients.push(client); return client;
  };
  const owner = makeClient(), account = await owner.bootstrap('Owner'), claim = await runtime.claim();
  await owner.extension('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  t.after(async () => {
    runtime.stop(); clients.forEach(client => client.dispose());
    const closing = Promise.all([source, server].map(item => new Promise(resolve => item.close(resolve))));
    sockets.forEach(socket => socket.destroy()); await app.locals.closeServices(); await closing;
    assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^soty-auto-app-/);
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const target = { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId };
  let sequence = 0;
  async function create(autoRegister = true, extra = {}) {
    const result = await owner.extension('apps.agent.create', { ...target, requestId: `auto-request-${++sequence}`, text: 'Создать список', cwd: '', ...extra, ...(autoRegister ? { autoRegister: true } : {}) });
    const leased = await app.locals.connectorService.store.poll(auth, 0, undefined, { automaticOwnerGuard: true }); assert.equal(leased.jobs[0]?.id, result.job.id);
    return result.job.id;
  }
  const proposal = jobId => ({ schema: 'soty.local-app.v1', name: 'Список', port: source.address().port, entryPath: '/', sourceJobId: jobId });
  const result = jobId => ({ ok: true, text: 'Готово', exitCode: 0, appProposal: proposal(jobId) });
  async function submit(jobId) {
    const response = await fetch(`${origin}/api/connectors/jobs/${jobId}/result`, { method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` }, body: JSON.stringify({ ...auth, result: result(jobId) }) });
    return response.json();
  }
  async function poll(compatible = true) {
    const query = new URLSearchParams({ linkId: auth.linkId, deviceId: auth.deviceId, connectorId: auth.connectorId, ...(compatible ? { autoOwnerGuard: '1' } : {}) });
    const response = await fetch(`${origin}/api/connectors/poll?${query}`, { headers: { authorization: `Bearer ${token}` } });
    return response.json();
  }
  async function read(jobId, client = owner) { return client.extension('apps.agent.read', { ...target, jobId }); }
  function watchRegistration() {
    const service = app.locals.appsService, original = service.registerCreationJob; let calls = 0;
    service.registerCreationJob = (...args) => { calls++; return original(...args); }; return () => calls;
  }
  return { owner, account, makeClient, auth, target, create, submit, poll, read, result, proposal, dataDir, source, stopRuntime: () => runtime.stop(),
    watchRegistration,
    capabilities: async () => (await fetch(origin + '/api/apps/capabilities')).json(), loseNextCreateAck: () => { loseCreateAck = true; }, loseNextArchiveAck: () => { loseArchiveAck = true; },
    service: () => app, async restart(overrides = {}, { announce = true } = {}) { await app.locals.closeServices(); app = createHttpApp(dist, { ...options, ...overrides });
      if (announce) assert.equal((await app.locals.connectorService.store.poll(auth, 0, undefined, { automaticOwnerGuard: true })).ok, true); } };
}

test('capability gates a new auto intent while the original lost ACK replays on a temporarily disabled server', { timeout: 25000 }, async t => {
  const f = await fixture(t), capability = (await f.capabilities()).automaticAppRegistration;
  assert.equal(capability.schema, 'soty.automatic-app-registration-capability.v1'); assert.equal(capability.available, true);
  assert.equal(capability.archive, true); assert.equal(capability.archiveCapacity, true); assert.equal(capability.archiveIncomplete, true);
  const args = { ...f.target, requestId: 'automatic-lost-ack', text: 'Создать список', cwd: '', autoRegister: true };
  f.loseNextCreateAck(); await assert.rejects(f.owner.extension('apps.agent.create', args));
  const jobs = f.service().locals.connectorService.store.state.jobs.filter(job => job.requestId === args.requestId);
  assert.equal(jobs.length, 1); assert.equal(jobs[0].input.autoRegister, true); const id = jobs[0].id;
  await f.restart({ universalAppsEnabled: false });
  assert.equal((await f.capabilities()).automaticAppRegistration.available, false);
  assert.equal((await f.owner.extension('apps.agent.create', args)).job.id, id);
  const rejected = await f.owner.extension('apps.agent.create', { ...args, requestId: 'new-disabled-request' });
  assert.equal(rejected.admission.status, 'rejected'); assert.equal(rejected.admission.reason, 'app_autoregistration_unavailable');
  assert.equal(f.service().locals.connectorService.store.state.jobs.filter(job => job.requestId === args.requestId).length, 1);
});

test('a stale modern registration cannot make an old poll lease automatic work; manual jobs remain compatible', { timeout: 25000 }, async t => {
  const f = await fixture(t), store = f.service().locals.connectorService.store;
  const args = { ...f.target, requestId: 'guarded-first', text: 'Create fixture', cwd: '', autoRegister: true };
  const first = await f.owner.extension('apps.agent.create', args); assert.ok(first.job.id);
  const oldPoll = await f.poll(false); assert.equal(oldPoll.jobs.length, 0);
  assert.equal((await f.read(first.job.id)).autoRegistration.state, 'device-update-required');
  const rejectedArgs = { ...args, requestId: 'old-device' };
  const rejected = await f.owner.extension('apps.agent.create', rejectedArgs);
  assert.equal(rejected.admission?.reason, 'app_auto_device_update_required');
  const modernPoll = await f.poll();
  assert.equal(modernPoll.jobs[0]?.id, first.job.id);
  assert.deepEqual((await f.owner.extension('apps.agent.create', rejectedArgs)).admission, rejected.admission);
  const next = await f.owner.extension('apps.agent.create', { ...args, requestId: 'guarded-next' });
  assert.ok(next.job.id);
  const manual = await f.owner.extension('apps.agent.create', { ...args, requestId: 'manual-legacy', autoRegister: false });
  const legacyPoll = await f.poll(false); assert.equal(legacyPoll.jobs[0]?.id, manual.job.id);
  assert.equal(store.state.jobs.find(job => job.id === next.job.id).status, 'queued');
});

test('server restart does not trust a persisted capability until the running process polls', { timeout: 25000 }, async t => {
  const f = await fixture(t);
  await f.restart({ localExecutorCurrent: false, env: {} }, { announce: false });
  const args = { ...f.target, requestId: 'stale-after-restart', text: 'Create fixture', cwd: '', autoRegister: true };
  const rejected = await f.owner.extension('apps.agent.create', args);
  assert.equal(rejected.admission?.reason, 'app_auto_device_update_required');
  assert.equal((await f.poll()).ok, true);
  assert.deepEqual((await f.owner.extension('apps.agent.create', args)).admission, rejected.admission);
  assert.ok((await f.owner.extension('apps.agent.create', { ...args, requestId: 'fresh-after-restart' })).job.id);
});

test('default Root checks the initiating account before lease and at runtime-status for auto jobs', { timeout: 25000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  await f.restart({ localExecutorCurrent: false, env: {} });
  const args = { ...f.target, requestId: 'revoked-before-lease', text: 'Create fixture', cwd: '', autoRegister: true };
  const created = await f.owner.extension('apps.agent.create', args), store = f.service().locals.connectorService.store;
  await second.revokeDevice(f.account.deviceId);
  const result = await store.poll(f.auth, 0, undefined, { automaticOwnerGuard: true });
  assert.equal(result.jobs.length, 0);
  const status = await f.read(created.job.id, second);
  assert.equal(status.job.status, 'cancelled'); assert.equal(status.job.cancelRequested, true);
  assert.equal((await second.extension('apps.list')).apps.length, 0);
});

test('revocation after lease is checked again by runtime-status before a native start', { timeout: 25000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  await f.restart({ localExecutorCurrent: false, env: {} });
  const id = await f.create();
  await second.revokeDevice(f.account.deviceId);
  const status = await f.service().locals.connectorService.store.getAssignedJobState(f.auth, id);
  assert.equal(status.job.currentOwnerChecked, true); assert.equal(status.job.cancelRequested, true);
});

test('one requested job registers one private app and real universal feedback despite duplicate result delivery', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create();
  assert.ok((await Promise.all([f.submit(id), f.submit(id)])).every(value => value.ok));
  const done = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value.autoRegistration.feedback === 'ready' && value; });
  const appId = `app-${id.slice(4)}`; assert.equal(done.autoRegistration.appId, appId);
  const apps = (await f.owner.extension('apps.list')).apps; assert.equal(apps.length, 1); assert.equal(apps[0].id, appId);
  assert.deepEqual(apps[0].grants, { accountIds: [], communityIds: [] });
  const registration = (await f.owner.extension('apps.universal.get', { appId, expectedAccountId: f.account.accountId })).registration;
  assert.equal(registration.state, 'ready'); assert.equal(registration.feedback.state, 'ready');
  assert.ok((await f.owner.extension('apps.feedback.context', { appId })).context.installationId);
});

test('restart recovers a committed job even when the completion callback never ran', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create();
  assert.equal((await f.service().locals.connectorService.store.finishJob(f.auth, id, f.result(id))).ok, true);
  assert.equal((await f.owner.extension('apps.list')).apps.length, 0);
  await f.restart();
  const done = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value.autoRegistration.feedback === 'ready' && value; });
  assert.equal(done.autoRegistration.appId, `app-${id.slice(4)}`); assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('an eight-day-old unfinished result survives pruning, restart and real Apps/feedback readback', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), store = f.service().locals.connectorService.store;
  assert.equal((await store.finishJob(f.auth, id, f.result(id))).ok, true);
  await store.mutate(() => {
    const job = store.state.jobs.find(value => value.id === id), before = Date.now() - 8 * 24 * 60 * 60_000;
    job.createdAt = before - 100; job.updatedAt = before; job.finishedAt = before; store.expire(Date.now());
  });
  assert.ok(store.state.jobs.find(value => value.id === id)); await f.restart();
  const current = f.service().locals.connectorService.store;
  await until(() => current.state.jobs.find(value => value.id === id)?.autoRegistrationFinal === 'ready');
  assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
  assert.equal((await f.read(id)).autoRegistration.feedback, 'ready');
  await current.mutate(() => current.expire(Date.now()));
  assert.equal(current.state.jobs.some(value => value.id === id), false, 'verified completion releases the old job');
  assert.equal((await f.owner.extension('apps.list')).apps.length, 1, 'the application is not deleted by queue cleanup');
});

test('the actual previous reader refuses protected work; after confirmed completion it can reopen safely', { timeout: 30000 }, async t => {
  const f = await fixture(t), id = await f.create(), store = f.service().locals.connectorService.store;
  assert.equal((await store.finishJob(f.auth, id, f.result(id))).ok, true);
  await store.mutate(() => {
    const job = store.state.jobs.find(value => value.id === id), before = Date.now() - 8 * 24 * 60 * 60_000;
    job.createdAt = before - 100; job.updatedAt = before; job.finishedAt = before;
  });
  const directory = dirname(store.filePath), oldDir = join(dirname(directory), 'frozen-old-reader'); await mkdir(oldDir);
  for (const name of ['connector-store.js', 'connector-persistence.js', 'connector-model-authorization.js'])
    await writeFile(join(oldDir, name), execFileSync('git', ['show', `e371d14be0aba67892a389f630a6c029f298ac77:server/${name}`]));
  await writeFile(join(oldDir, 'package.json'), '{"type":"module"}');
  const legacy = await import(pathToFileURL(join(oldDir, 'connector-store.js')).href);
  await f.service().locals.closeServices();
  const old = legacy.createConnectorStore(directory);
  await assert.rejects(old.ready, /Invalid connector database/); await old.close();
  const guarded = new DatabaseSync(join(directory, 'connector-store.sqlite'), { readOnly: true });
  try { assert.equal(guarded.prepare("SELECT value FROM meta WHERE key='schema'").get().value, 'soty.connector-sqlite.v2');
    assert.equal(guarded.prepare('SELECT count(*) AS n FROM jobs WHERE id=?').get(id).n, 1); }
  finally { guarded.close(); }
  await f.restart(); const restored = f.service().locals.connectorService.store;
  await until(() => restored.state.jobs.find(value => value.id === id)?.autoRegistrationFinal === 'ready');
  assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
  await f.service().locals.closeServices(); const olderAfterCompletion = legacy.createConnectorStore(directory);
  try { await olderAfterCompletion.ready; assert.ok(olderAfterCompletion.state.jobs.find(job => job.id === id)); }
  finally { await olderAfterCompletion.close(); }
  await f.restart(); assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('an existing v1 store with a legacy automatic job is fenced before the new server starts serving', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), directory = dirname(f.service().locals.connectorService.store.filePath);
  await f.service().locals.closeServices();
  const db = new DatabaseSync(join(directory, 'connector-store.sqlite'));
  try { db.prepare("UPDATE meta SET value='soty.connector-sqlite.v1' WHERE key='schema'").run(); }
  finally { db.close(); }
  // An old producer stored the v4 job under v1; the new reader must fence it
  // even when universal registration is temporarily disabled.
  await f.restart({ universalAppsEnabled: false }); const current = f.service().locals.connectorService.store;
  await current.ready; assert.ok(current.state.jobs.find(job => job.id === id));
  const checked = new DatabaseSync(join(directory, 'connector-store.sqlite'), { readOnly: true });
  try { assert.equal(checked.prepare("SELECT value FROM meta WHERE key='schema'").get().value, 'soty.connector-sqlite.v2'); }
  finally { checked.close(); }
});

test('a lost Apps readback retains the job; restart reads the same committed app and releases it once', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), apps = f.service().locals.appsService;
  apps.creationJobApp = () => null;
  assert.equal((await f.submit(id)).ok, true);
  await until(async () => (await f.owner.extension('apps.list')).apps.length === 1);
  const original = f.service().locals.connectorService.store;
  assert.equal(original.state.jobs.find(value => value.id === id).autoRegistrationFinal, undefined);
  await f.restart(); const restored = f.service().locals.connectorService.store;
  await until(() => restored.state.jobs.find(value => value.id === id)?.autoRegistrationFinal === 'ready');
  assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
  assert.equal((await f.read(id)).autoRegistration.feedback, 'ready');
});

test('capacity and unexpected failures are explicit and can recover without duplicating an app', { timeout: 25000 }, async t => {
  for (const [code, expected] of [['apps_limit_reached', 'capacity'], ['synthetic_unexpected_failure', 'attention']]) {
    const f = await fixture(t), id = await f.create(), universal = f.service().locals.universalApps;
    const actual = universal.registerCreationJob;
    universal.registerCreationJob = () => { throw Object.assign(Error('synthetic'), { code }); };
    assert.equal((await f.submit(id)).ok, true);
    await until(async () => (await f.read(id)).autoRegistration?.state === expected);
    assert.equal((await f.owner.extension('apps.list')).apps.length, 0);
    assert.equal(f.service().locals.connectorService.store.state.jobs.find(value => value.id === id).autoRegistrationFinal, undefined);
    universal.registerCreationJob = actual; await f.poll();
    await until(async () => (await f.read(id)).autoRegistration?.feedback === 'ready');
    assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
  }
});

test('an owner can stop an unexpected failure with no card; a lost response and restart cannot register it later', { timeout: 30000 }, async t => {
  const f = await fixture(t), stranger = f.makeClient(), other = await stranger.bootstrap('Other owner');
  const id = await f.create(), universal = f.service().locals.universalApps, actual = universal.registerCreationJob;
  universal.registerCreationJob = () => { throw Object.assign(Error('synthetic failure'), { code: 'synthetic_unexpected_failure' }); };
  try {
    assert.equal((await f.submit(id)).ok, true); await until(async () => (await f.read(id)).autoRegistration?.state === 'attention');
    const args = { expectedAccountId: f.account.accountId, jobId: id };
    await assert.rejects(stranger.extension('apps.agent.archive', { expectedAccountId: other.accountId, jobId: id }), { code: 'app_archive_unavailable' });
    f.loseNextArchiveAck(); await assert.rejects(f.owner.extension('apps.agent.archive', args));
    const archived = await f.owner.extension('apps.agent.archiveReceipt', args); assert.equal(archived.state, 'archived');
    assert.deepEqual((await f.owner.extension('apps.agent.archive', args)).receipt, archived.receipt);
    assert.equal((await probe({ dataDir: f.dataDir })).automaticPending, undefined);
  } finally { universal.registerCreationJob = actual; }
  await f.poll(); await f.restart(); await f.poll();
  assert.equal((await f.owner.extension('apps.list')).apps.length, 0);
  assert.equal((await f.read(id)).autoRegistration.state, 'archived');
  assert.equal((await f.owner.extension('apps.agent.archiveReceipt', { expectedAccountId: other.accountId, jobId: id }).catch(() => ({ state: 'denied' }))).state, 'denied');
});

test('an uncertain execution remains protected even when its status needs attention', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), store = f.service().locals.connectorService.store;
  assert.equal((await store.finishJob(f.auth, id, f.result(id))).ok, true);
  await store.mutate(() => { store.state.jobs.find(job => job.id === id).executionUncertain = true; });
  await f.restart(); await until(async () => (await f.read(id)).autoRegistration?.state === 'attention');
  const args = { expectedAccountId: f.account.accountId, jobId: id };
  await assert.rejects(f.owner.extension('apps.agent.archive', args), { code: 'app_archive_unavailable' });
  assert.equal((await f.owner.extension('apps.agent.archiveReceipt', args)).state, 'unconfirmed');
  assert.equal((await probe({ dataDir: f.dataDir })).automaticPending, true);
});

test('conditional feedback stop preserves the card and durable outbox across lost ACK, revocation and restart', { timeout: 30000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId); const secondDevice = (await second.getLocalState()).deviceId;
  const stranger = f.makeClient(), other = await stranger.bootstrap('Other owner'), id = await f.create();
  const feedback = new DatabaseSync(join(f.dataDir, 'feedback', 'feedback.sqlite')); feedback.exec('BEGIN IMMEDIATE');
  let archived, outboxBefore, registrationBefore;
  try {
    assert.equal((await f.submit(id)).ok, true);
    const held = await until(async () => { const value = await f.read(id); return value.autoRegistration?.canArchiveFeedback === true && value; });
    const appId = held.autoRegistration.appId; assert.equal(appId, `app-${id.slice(4)}`); assert.equal(held.autoRegistration.feedback, 'waiting');
    registrationBefore = (await f.owner.extension('apps.universal.get', { appId, expectedAccountId: f.account.accountId })).registration;
    assert.equal(registrationBefore.state, 'pending-feedback');
    const registration = new DatabaseSync(join(f.dataDir, 'app-registration', 'registry.sqlite'), { readOnly: true });
    try { outboxBefore = registration.prepare('SELECT provisioning_key,request_json FROM registration_feedback_outbox').get(); } finally { registration.close(); }
    assert.ok(outboxBefore?.provisioning_key);
    const args = { expectedAccountId: f.account.accountId, jobId: id, requireFeedbackPending: true };
    await assert.rejects(stranger.extension('apps.agent.archive', { ...args, expectedAccountId: other.accountId }), { code: 'app_archive_unavailable' });
    const store = f.service().locals.connectorService.store, actual = store.archiveAutomaticAppJob.bind(store);
    let reached, release; const entered = new Promise(resolve => { reached = resolve; }), hold = new Promise(resolve => { release = resolve; });
    store.archiveAutomaticAppJob = async (...values) => { reached(); await hold; return actual(...values); };
    const denied = second.extension('apps.agent.archive', args);
    try { await entered; await f.owner.revokeDevice(secondDevice); } finally { release(); }
    await assert.rejects(denied); assert.equal(store.state.jobs.find(job => job.id === id).autoRegistrationFinal, undefined);
    store.archiveAutomaticAppJob = actual; f.loseNextArchiveAck(); await assert.rejects(f.owner.extension('apps.agent.archive', args));
    archived = await f.owner.extension('apps.agent.archiveReceipt', { expectedAccountId: f.account.accountId, jobId: id });
    assert.equal(archived.state, 'archived'); assert.equal(archived.receipt.jobId, id);
    assert.deepEqual((await f.owner.extension('apps.agent.archive', args)).receipt, archived.receipt);
    assert.equal((await probe({ dataDir: f.dataDir })).automaticPending, undefined);
    assert.equal((await f.owner.extension('apps.list')).apps[0].id, appId);
  } finally { feedback.exec('ROLLBACK'); feedback.close(); }
  await f.poll(); await f.restart(); await f.poll();
  assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
  assert.deepEqual((await f.owner.extension('apps.agent.archiveReceipt', { expectedAccountId: f.account.accountId, jobId: id })).receipt, archived.receipt);
  const registrationAfter = (await f.owner.extension('apps.universal.get', { appId: `app-${id.slice(4)}`, expectedAccountId: f.account.accountId })).registration;
  assert.equal(registrationAfter.state, 'pending-feedback'); assert.equal(registrationAfter.feedback.provisioningKey, registrationBefore.feedback.provisioningKey);
  const db = new DatabaseSync(join(f.dataDir, 'app-registration', 'registry.sqlite'), { readOnly: true });
  try { assert.deepEqual(db.prepare('SELECT provisioning_key,request_json FROM registration_feedback_outbox').get(), outboxBefore); } finally { db.close(); }
  const directory = dirname(f.service().locals.connectorService.store.filePath), oldDir = join(dirname(directory), 'feedback-old-reader'); await mkdir(oldDir);
  for (const name of ['connector-store.js', 'connector-persistence.js', 'connector-model-authorization.js'])
    await writeFile(join(oldDir, name), execFileSync('git', ['show', `e371d14be0aba67892a389f630a6c029f298ac77:server/${name}`]));
  await writeFile(join(oldDir, 'package.json'), '{"type":"module"}'); const legacy = await import(pathToFileURL(join(oldDir, 'connector-store.js')).href);
  await f.service().locals.closeServices(); const old = legacy.createConnectorStore(directory);
  try {
    await old.ready; assert.equal((await old.automaticAppJobs(id)).length, 0); assert.equal(old.state.jobs.find(job => job.id === id).cancelRequested, true);
    await old.mutate(() => old.expire(Date.now() + 8 * 24 * 60 * 60_000));
    assert.equal(old.state.jobs.some(job => job.id === id), false); assert.equal((await old.poll(f.auth)).error, 'connector-auth-failed');
  } finally { await old.close(); }
  await f.restart({}, { announce: false });
  assert.deepEqual((await f.owner.extension('apps.agent.archiveReceipt', { expectedAccountId: f.account.accountId, jobId: id })).receipt, archived.receipt);
  assert.equal((await f.owner.extension('apps.list')).apps[0].id, `app-${id.slice(4)}`);
  const rollbackRegistration = (await f.owner.extension('apps.universal.get', { appId: `app-${id.slice(4)}`, expectedAccountId: f.account.accountId })).registration;
  assert.equal(rollbackRegistration.state, 'pending-feedback'); assert.equal(rollbackRegistration.feedback.provisioningKey, registrationBefore.feedback.provisioningKey);
  const appId = `app-${id.slice(4)}`, resumed = await f.owner.extension('apps.universal.admit', {
    expectedAccountId: f.account.accountId, appId, requestId: `initial.${appId}`, expectedRevision: 0,
    proposal: { kind: 'author-draft', draft: { title: 'Список' } },
  });
  assert.equal(resumed.replayed, true); assert.equal(resumed.registration.state, 'ready'); assert.equal(resumed.registration.feedback.state, 'ready');
});

test('an existing card without a durable feedback intent cannot be conditionally archived', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), store = f.service().locals.connectorService.store;
  assert.equal((await store.finishJob(f.auth, id, f.result(id))).ok, true);
  const [job] = await store.automaticAppJobs(id), actor = { accountId: job.ownerAccountId, deviceId: job.sourceActorDeviceId };
  const app = f.service().locals.connectService.withCurrentContacts(actor, () => f.service().locals.appsService.registerCreationJob(actor, job)).app;
  const status = (await f.read(id)).autoRegistration; assert.equal(status.state, 'ready'); assert.equal(status.feedback, 'waiting');
  assert.equal(status.canArchiveFeedback, undefined);
  await assert.rejects(f.owner.extension('apps.agent.archive', { expectedAccountId: f.account.accountId, jobId: id, requireFeedbackPending: true }), { code: 'app_archive_unavailable' });
  assert.equal((await probe({ dataDir: f.dataDir })).automaticPending, true); assert.equal((await f.owner.extension('apps.list')).apps[0].id, app.id);
  await f.restart(); await until(async () => (await f.read(id)).autoRegistration?.feedback === 'ready');
});

test('a missing pending-feedback outbox fails closed and leaves the reservation intact', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), feedback = new DatabaseSync(join(f.dataDir, 'feedback', 'feedback.sqlite'));
  feedback.exec('BEGIN IMMEDIATE');
  try {
    assert.equal((await f.submit(id)).ok, true);
    await until(async () => (await f.read(id)).autoRegistration?.canArchiveFeedback === true);
    const db = new DatabaseSync(join(f.dataDir, 'app-registration', 'registry.sqlite'));
    try {
      // Simulate a damaged local fixture, bypassing the normal immutability trigger.
      db.exec('DROP TRIGGER registration_feedback_outbox_no_delete');
      assert.equal(db.prepare('DELETE FROM registration_feedback_outbox').run().changes, 1);
    } finally { db.close(); }
    const status = (await f.read(id)).autoRegistration; assert.equal(status.state, 'ready'); assert.equal(status.feedback, 'waiting');
    assert.equal(status.canArchiveFeedback, undefined);
    await assert.rejects(f.owner.extension('apps.agent.archive', { expectedAccountId: f.account.accountId, jobId: id, requireFeedbackPending: true }), { code: 'app_archive_unconfirmed' });
    assert.equal((await probe({ dataDir: f.dataDir })).automaticPending, true);
    assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
  } finally { feedback.exec('ROLLBACK'); feedback.close(); }
});

test('restart finishes universal and feedback after the app transaction committed alone', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), store = f.service().locals.connectorService.store;
  assert.equal((await store.finishJob(f.auth, id, f.result(id))).ok, true);
  const [job] = await store.automaticAppJobs(id), actor = { accountId: job.ownerAccountId, deviceId: job.sourceActorDeviceId };
  const partial = f.service().locals.connectService.withCurrentContacts(actor, () => f.service().locals.appsService.registerCreationJob(actor, job));
  assert.equal(partial.app.id, `app-${id.slice(4)}`);
  assert.equal((await f.owner.extension('apps.universal.get', { appId: partial.app.id, expectedAccountId: f.account.accountId })).registration, null);
  await f.restart();
  const done = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value.autoRegistration.feedback === 'ready' && value; });
  assert.equal(done.autoRegistration.appId, partial.app.id); assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
  assert.ok((await f.owner.extension('apps.feedback.context', { appId: partial.app.id })).context.installationId);
});

test('a revoked partial app never starts universal or feedback during restart recovery', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), store = f.service().locals.connectorService.store;
  assert.equal((await store.finishJob(f.auth, id, f.result(id))).ok, true);
  const [job] = await store.automaticAppJobs(id), actor = { accountId: job.ownerAccountId, deviceId: job.sourceActorDeviceId };
  const partial = f.service().locals.connectService.withCurrentContacts(actor, () => f.service().locals.appsService.registerCreationJob(actor, job));
  await f.owner.extension('apps.revoke', { appId: partial.app.id }); await f.restart();
  assert.equal((await f.read(id)).autoRegistration.state, 'revoked');
  const feedback = new DatabaseSync(join(f.dataDir, 'feedback', 'feedback.sqlite'));
  try { assert.equal(feedback.prepare('SELECT count(*) AS count FROM feedback_installations').get().count, 0); }
  finally { feedback.close(); }
});

test('the registered app can launch while feedback is locked and poll finishes feedback later', { timeout: 30000 }, async t => {
  const f = await fixture(t), id = await f.create(), feedback = new DatabaseSync(join(f.dataDir, 'feedback', 'feedback.sqlite'));
  feedback.exec('BEGIN IMMEDIATE');
  try {
    await f.submit(id);
    const held = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value.autoRegistration.feedback === 'waiting' && value.autoRegistration.appState === 'ready' && value; });
    assert.equal(held.autoRegistration.appId, `app-${id.slice(4)}`);
    assert.equal(f.service().locals.connectorService.store.state.jobs.find(job => job.id === id).autoRegistrationFinal, undefined);
    const launch = await f.owner.extension('apps.launch', { appId: held.autoRegistration.appId });
    const url = new URL(launch.launchUrl);
    const boot = await appRequest(url, '/_soty/session', { method: 'POST', headers: { origin: url.origin, 'content-type': 'application/json' }, body: JSON.stringify({ ticket: url.hash.slice(1) }) });
    assert.equal(boot.status, 200);
    const page = await appRequest(url, '/', { headers: { cookie: boot.headers['set-cookie'][0].split(';')[0] } });
    assert.equal(page.status, 200); assert.equal(page.body, 'Local app');
  } finally { feedback.exec('ROLLBACK'); feedback.close(); }
  assert.equal((await f.poll()).ok, true);
  const ready = await until(async () => { const value = await f.read(id); return value.autoRegistration?.feedback === 'ready' && value; });
  assert.equal(ready.autoRegistration.appId, `app-${id.slice(4)}`);
  await until(() => f.service().locals.connectorService.store.state.jobs.find(job => job.id === id)?.autoRegistrationFinal === 'ready');
});

test('existing server heartbeat retries pending feedback even after the connector goes offline', { timeout: 30000 }, async t => {
  const f = await fixture(t), id = await f.create(), feedback = new DatabaseSync(join(f.dataDir, 'feedback', 'feedback.sqlite'));
  feedback.exec('BEGIN IMMEDIATE');
  try {
    await f.submit(id);
    await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value.autoRegistration.feedback === 'waiting'; });
  } finally { feedback.exec('ROLLBACK'); feedback.close(); }
  f.stopRuntime();
  const ready = await until(async () => { const value = await f.read(id); return value.autoRegistration?.feedback === 'ready' && value; }, 14000);
  assert.equal(ready.autoRegistration.appId, `app-${id.slice(4)}`);
});

test('revoked application is a tombstone even when the connector repeats its old result', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(); await f.submit(id);
  const ready = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value; });
  const appId = ready.autoRegistration.appId; await f.owner.extension('apps.revoke', { appId }); const registrations = f.watchRegistration();
  assert.equal((await f.submit(id)).ok, true);
  await until(() => registrations() > 0);
  const revoked = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'revoked' && value; });
  assert.equal(revoked.autoRegistration.appId, appId);
  const apps = (await f.owner.extension('apps.list')).apps; assert.equal(apps.length, 1); assert.equal(apps[0].state, 'revoked'); assert.equal(apps[0].id, appId);
  await f.restart(); assert.equal((await f.read(id)).autoRegistration.state, 'revoked');
});

test('replaying a completed job never overwrites subsequent owner edits', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(); await f.submit(id);
  const ready = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value; });
  await f.owner.extension('apps.update', { appId: ready.autoRegistration.appId, name: 'Переименованное приложение' });
  const registrations = f.watchRegistration();
  assert.equal((await f.submit(id)).ok, true);
  await until(() => registrations() > 0);
  const [app] = (await f.owner.extension('apps.list')).apps;
  assert.equal(app.id, ready.autoRegistration.appId); assert.equal(app.name, 'Переименованное приложение');
});

test('old manual jobs stay manual and an occupied unrelated port is not adopted', { timeout: 25000 }, async t => {
  const f = await fixture(t), oldId = await f.create(false); await f.submit(oldId);
  assert.equal((await f.read(oldId)).autoRegistration, undefined); assert.equal((await f.owner.extension('apps.list')).apps.length, 0);
  const existing = (await f.owner.extension('apps.register', { ...f.target, name: 'Список', port: f.source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } })).app;
  const id = await f.create(); await f.submit(id);
  const conflict = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'conflict' && value; });
  assert.equal(conflict.autoRegistration.state, 'conflict'); const apps = (await f.owner.extension('apps.list')).apps;
  assert.equal(apps.length, 1); assert.equal(apps[0].id, existing.id); assert.notEqual(existing.id, `app-${id.slice(4)}`);
});

test('owner explicitly archives a conflict once; receipt survives restart and duplicate completion cannot register it', { timeout: 25000 }, async t => {
  const f = await fixture(t), existing = (await f.owner.extension('apps.register', { ...f.target, name: 'Список', port: f.source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } })).app;
  const id = await f.create(); await f.submit(id);
  await until(async () => (await f.read(id)).autoRegistration?.state === 'conflict');
  const args = { expectedAccountId: f.account.accountId, jobId: id };
  const first = await f.owner.extension('apps.agent.archive', args);
  assert.equal(first.state, 'archived'); assert.equal(first.replayed, false); assert.equal(first.receipt.jobId, id);
  assert.equal(first.receipt.requestId, f.service().locals.connectorService.store.state.jobs.find(job => job.id === id).requestId);
  const replay = await f.owner.extension('apps.agent.archive', args); assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, first.receipt);
  assert.equal((await f.read(id)).autoRegistration.state, 'archived');
  const corrupt = structuredClone(f.service().locals.connectorService.store.state);
  corrupt.requests = corrupt.requests.filter(row => !row.autoArchive); assert.throws(() => normalizeConnectorState(corrupt), /Missing automatic archive receipt/);
  const changed = structuredClone(f.service().locals.connectorService.store.state);
  changed.requests.find(row => row.autoArchive).autoArchive.requestId = 'tampered-request'; assert.throws(() => normalizeConnectorState(changed), /Invalid automatic archive receipt/);
  await f.restart(); assert.equal((await f.owner.extension('apps.agent.archive', args)).replayed, true);
  assert.equal((await f.submit(id)).ok, true); await f.poll();
  const apps = (await f.owner.extension('apps.list')).apps; assert.equal(apps.length, 1); assert.equal(apps[0].id, existing.id);
});

test('another signed device of the same owner archives a result after the original actor is revoked', { timeout: 25000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  const id = await f.create(), original = f.service().locals.connectorService.store;
  assert.equal((await original.finishJob(f.auth, id, f.result(id))).ok, true);
  await second.revokeDevice(f.account.deviceId); await f.restart();
  await until(async () => (await f.read(id, second)).autoRegistration?.state === 'access-lost');
  assert.ok((await second.extension('apps.assistant.history', { expectedAccountId: f.account.accountId })).jobs.some(job => job.id === id));
  const args = { expectedAccountId: f.account.accountId, jobId: id }, archived = await second.extension('apps.agent.archive', args);
  assert.equal(archived.state, 'archived'); assert.equal((await f.read(id, second)).autoRegistration.state, 'archived');
  assert.equal((await second.extension('apps.list')).apps.length, 0); await f.restart();
  assert.deepEqual((await second.extension('apps.agent.archive', args)).receipt, archived.receipt);
  assert.equal((await second.extension('apps.list')).apps.length, 0);
});

test('the revoked-device maintenance blocker clears only after the current owner archives; old reader cannot resume', { timeout: 30000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  const id = await f.create(), before = f.service().locals.connectorService.store;
  assert.equal((await before.finishJob(f.auth, id, f.result(id))).ok, true);
  await second.revokeDevice(f.account.deviceId); await f.restart();
  await until(async () => (await f.read(id, second)).autoRegistration?.state === 'access-lost');
  assert.equal((await probe({ dataDir: f.dataDir })).automaticPending, true);
  const cancellation = await second.extension('apps.agent.cancel', { ...f.target, jobId: id }); assert.equal(cancellation.job.status, 'succeeded');
  const current = f.service().locals.connectorService.store; await current.mutate(() => current.expire(Date.now() + 8 * 24 * 60 * 60_000));
  assert.equal((await current.poll(f.auth)).error, 'connector-auth-failed', 'the simulated offline connector expired');
  assert.ok(current.state.jobs.find(job => job.id === id)); await f.restart({}, { announce: false });
  const args = { expectedAccountId: f.account.accountId, jobId: id }, archived = await second.extension('apps.agent.archive', args);
  assert.equal(archived.state, 'archived'); const status = await probe({ dataDir: f.dataDir }); assert.equal(status.count, 0); assert.equal(status.automaticPending, undefined);
  const directory = dirname(f.service().locals.connectorService.store.filePath), oldDir = join(dirname(directory), 'revoked-old-reader'); await mkdir(oldDir);
  for (const name of ['connector-store.js', 'connector-persistence.js', 'connector-model-authorization.js'])
    await writeFile(join(oldDir, name), execFileSync('git', ['show', `e371d14be0aba67892a389f630a6c029f298ac77:server/${name}`]));
  await writeFile(join(oldDir, 'package.json'), '{"type":"module"}'); const legacy = await import(pathToFileURL(join(oldDir, 'connector-store.js')).href);
  await f.service().locals.closeServices(); const old = legacy.createConnectorStore(directory);
  try { await old.ready; assert.equal((await old.automaticAppJobs(id)).length, 0); } finally { await old.close(); }
  await f.restart({}, { announce: false }); assert.deepEqual((await second.extension('apps.agent.archive', args)).receipt, archived.receipt);
  assert.equal((await second.extension('apps.list')).apps.length, 0);
});

test('archiving a ready card returns the existing app without a new receipt or another registration', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(); await f.submit(id);
  const ready = await until(async () => { const value = await f.read(id); return value.autoRegistration?.feedback === 'ready' && value; });
  const store = f.service().locals.connectorService.store, before = store.state.requests.length;
  const result = await f.owner.extension('apps.agent.archive', { expectedAccountId: f.account.accountId, jobId: id });
  assert.equal(result.state, 'ready'); assert.equal(result.appId, ready.autoRegistration.appId);
  const conditional = await f.owner.extension('apps.agent.archive', { expectedAccountId: f.account.accountId, jobId: id, requireFeedbackPending: true });
  assert.equal(conditional.state, 'ready'); assert.equal(conditional.appId, ready.autoRegistration.appId);
  assert.equal(store.state.requests.length, before); assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('archive waits for an in-flight successful registration and returns the single ready card', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), store = f.service().locals.connectorService.store;
  const actual = store.confirmAutomaticAppJob.bind(store); let reached, release;
  const entered = new Promise(resolve => { reached = resolve; }), hold = new Promise(resolve => { release = resolve; });
  store.confirmAutomaticAppJob = async (...args) => { reached(); await hold; return actual(...args); };
  assert.equal((await f.submit(id)).ok, true); await entered;
  const request = f.owner.extension('apps.agent.archive', { expectedAccountId: f.account.accountId, jobId: id });
  release(); const result = await request; assert.equal(result.state, 'ready'); assert.equal(result.appId, `app-${id.slice(4)}`);
  assert.equal(store.state.requests.some(value => value.autoArchive?.jobId === id), false);
  assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('another account cannot archive a job; a full receipt ledger fails closed without clearing its reservation', { timeout: 25000 }, async t => {
  const f = await fixture(t), other = f.makeClient(), stranger = await other.bootstrap('Other owner');
  await f.owner.extension('apps.register', { ...f.target, name: 'Список', port: f.source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  const id = await f.create(); await f.submit(id); await until(async () => (await f.read(id)).autoRegistration?.state === 'conflict');
  await assert.rejects(other.extension('apps.agent.archive', { expectedAccountId: stranger.accountId, jobId: id }), { code: 'app_archive_unavailable' });
  const store = f.service().locals.connectorService.store, count = store.state.requests.length; store.maxRequestRecords = count;
  await assert.rejects(f.owner.extension('apps.agent.archive', { expectedAccountId: f.account.accountId, jobId: id }), { code: 'app_archive_capacity' });
  assert.equal(store.state.requests.length, count); assert.equal(store.state.jobs.find(job => job.id === id).autoRegistrationFinal, undefined);
  const db = new DatabaseSync(join(dirname(store.filePath), 'connector-store.sqlite'), { readOnly: true });
  try { assert.equal(db.prepare("SELECT value FROM meta WHERE key='schema'").get().value, 'soty.connector-sqlite.v2'); } finally { db.close(); }
});

test('the owner can stop a job at the real app limit and recover one receipt after a lost response and rollback', { timeout: 120000 }, async t => {
  const f = await fixture(t), stranger = f.makeClient(), other = await stranger.bootstrap('Other owner');
  let created = 0;
  for (let port = 25000; created < 100; port++) {
    if (port === f.source.address().port || port === 49424) continue;
    const result = await f.owner.extension('apps.register', { ...f.target, name: `Fixture ${created}`, port, entryPath: '/', grants: {} });
    assert.ok(result.app.id); created++;
  }
  assert.equal((await f.owner.extension('apps.list')).apps.length, 100);
  const id = await f.create(); assert.equal((await f.submit(id)).ok, true);
  await until(async () => (await f.read(id)).autoRegistration?.state === 'capacity');
  assert.equal((await probe({ dataDir: f.dataDir })).automaticPending, true);
  await f.restart(); await until(async () => (await f.read(id)).autoRegistration?.state === 'capacity');
  const args = { expectedAccountId: f.account.accountId, jobId: id }, otherArgs = { expectedAccountId: other.accountId, jobId: id };
  await assert.rejects(stranger.extension('apps.agent.archive', otherArgs), { code: 'app_archive_unavailable' });
  assert.equal((await stranger.extension('apps.agent.archiveReceipt', otherArgs)).state, 'unconfirmed');
  f.loseNextArchiveAck(); await assert.rejects(f.owner.extension('apps.agent.archive', args));
  const archived = await f.owner.extension('apps.agent.archiveReceipt', args);
  assert.equal(archived.state, 'archived'); assert.equal(archived.receipt.jobId, id); assert.equal(archived.receipt.requestId, 'auto-request-1');
  const [first, second] = await Promise.all([f.owner.extension('apps.agent.archive', args), f.owner.extension('apps.agent.archive', args)]);
  assert.equal(first.replayed, true); assert.equal(second.replayed, true);
  assert.deepEqual(first.receipt, archived.receipt); assert.deepEqual(second.receipt, archived.receipt);
  const store = f.service().locals.connectorService.store;
  assert.equal(store.state.requests.filter(row => row.autoArchive?.jobId === id).length, 1);
  assert.equal(store.state.jobs.find(job => job.id === id).cancelRequested, true);
  assert.equal(store.state.jobs.find(job => job.id === id).autoRegistrationFinal, 'archived');
  assert.equal((await probe({ dataDir: f.dataDir })).automaticPending, undefined);
  assert.equal((await f.owner.extension('apps.list')).apps.length, 100);
  assert.equal((await stranger.extension('apps.agent.archiveReceipt', otherArgs)).state, 'unconfirmed');
  const directory = dirname(store.filePath), oldDir = join(dirname(directory), 'capacity-old-reader'); await mkdir(oldDir);
  for (const name of ['connector-store.js', 'connector-persistence.js', 'connector-model-authorization.js'])
    await writeFile(join(oldDir, name), execFileSync('git', ['show', `e371d14be0aba67892a389f630a6c029f298ac77:server/${name}`]));
  await writeFile(join(oldDir, 'package.json'), '{"type":"module"}'); const legacy = await import(pathToFileURL(join(oldDir, 'connector-store.js')).href);
  await f.service().locals.closeServices(); const old = legacy.createConnectorStore(directory);
  try {
    await old.ready; assert.equal((await old.automaticAppJobs(id)).length, 0);
    assert.equal(old.state.jobs.find(job => job.id === id).cancelRequested, true);
    await old.mutate(() => old.expire(Date.now() + 8 * 24 * 60 * 60_000));
    assert.equal(old.state.jobs.some(job => job.id === id), false);
    assert.equal((await old.poll(f.auth)).error, 'connector-auth-failed');
  } finally { await old.close(); }
  await f.restart({}, { announce: false });
  assert.deepEqual((await f.owner.extension('apps.agent.archiveReceipt', args)).receipt, archived.receipt);
  assert.deepEqual((await f.owner.extension('apps.agent.archive', args)).receipt, archived.receipt);
  assert.equal((await f.owner.extension('apps.list')).apps.length, 100);
  assert.equal((await stranger.extension('apps.agent.archiveReceipt', otherArgs)).state, 'unconfirmed');
});

test('the previous reader cannot resume an archived job, and its historical receipt survives rollback', { timeout: 30000 }, async t => {
  const f = await fixture(t); await f.owner.extension('apps.register', { ...f.target, name: 'Список', port: f.source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  const id = await f.create(); await f.submit(id); await until(async () => (await f.read(id)).autoRegistration?.state === 'conflict');
  const args = { expectedAccountId: f.account.accountId, jobId: id }, archived = await f.owner.extension('apps.agent.archive', args);
  const directory = dirname(f.service().locals.connectorService.store.filePath), oldDir = join(dirname(directory), 'archived-old-reader'); await mkdir(oldDir);
  for (const name of ['connector-store.js', 'connector-persistence.js', 'connector-model-authorization.js'])
    await writeFile(join(oldDir, name), execFileSync('git', ['show', `e371d14be0aba67892a389f630a6c029f298ac77:server/${name}`]));
  await writeFile(join(oldDir, 'package.json'), '{"type":"module"}'); const legacy = await import(pathToFileURL(join(oldDir, 'connector-store.js')).href);
  await f.service().locals.closeServices(); const old = legacy.createConnectorStore(directory);
  try { await old.ready; assert.equal((await old.automaticAppJobs(id)).length, 0);
    assert.equal(old.state.jobs.find(job => job.id === id).cancelRequested, true);
    await old.mutate(() => old.expire(Date.now() + 8 * 24 * 60 * 60_000));
    assert.equal((await old.poll(f.auth)).error, 'connector-auth-failed', 'the simulated offline connector expired');
    assert.equal(old.state.jobs.some(job => job.id === id), false, 'the old reader really cleaned up the job'); }
  finally { await old.close(); }
  await f.restart({}, { announce: false }); assert.deepEqual((await f.owner.extension('apps.agent.archiveReceipt', args)).receipt, archived.receipt);
  assert.deepEqual((await f.owner.extension('apps.agent.archive', args)).receipt, archived.receipt);
  assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('a lost archive response and concurrent explicit retries preserve one immutable receipt', { timeout: 25000 }, async t => {
  const f = await fixture(t); await f.owner.extension('apps.register', { ...f.target, name: 'Список', port: f.source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  const id = await f.create(); await f.submit(id); await until(async () => (await f.read(id)).autoRegistration?.state === 'conflict');
  const args = { expectedAccountId: f.account.accountId, jobId: id }; f.loseNextArchiveAck();
  await assert.rejects(f.owner.extension('apps.agent.archive', args));
  const [first, second] = await Promise.all([f.owner.extension('apps.agent.archive', args), f.owner.extension('apps.agent.archive', args)]);
  assert.equal(first.replayed, true); assert.equal(second.replayed, true); assert.deepEqual(first.receipt, second.receipt);
  assert.equal(f.service().locals.connectorService.store.state.requests.filter(row => row.autoArchive?.jobId === id).length, 1);
});

test('lost host access exposes only an owner-bound recovery entry and a durable read-only receipt', { timeout: 30000 }, async t => {
  const f = await fixture(t), stranger = f.makeClient(), other = await stranger.bootstrap('Other owner');
  const id = await f.create(true, { text: 'private prompt SECRET-PROMPT', cwd: 'D:\\private-SECRET-PATH' });
  f.stopRuntime(); const store = f.service().locals.connectorService.store;
  assert.equal((await store.finishJob(f.auth, id, { ...f.result(id), text: 'SECRET-RESULT' })).ok, true);
  // Simulate the existing ownership guard denying this host without changing
  // the owner account or granting any access through the recovery path.
  f.service().locals.appsService.resolveOwnedDevice = () => { throw Object.assign(new Error('apps_device_not_owned'), { code: 'apps_device_not_owned' }); };
  const args = { expectedAccountId: f.account.accountId, jobId: id };
  await assert.rejects(f.read(id));
  const page = await f.owner.extension('apps.assistant.history', { expectedAccountId: f.account.accountId });
  const row = page.jobs.find(job => job.id === id); assert.equal(row.restrictedAuto, 'pending'); assert.equal(row.cwd, '');
  assert.doesNotMatch(JSON.stringify(page), /SECRET-PROMPT|SECRET-PATH|SECRET-RESULT/u);
  assert.deepEqual((await stranger.extension('apps.assistant.history', { expectedAccountId: other.accountId })).jobs, []);
  await assert.rejects(stranger.extension('apps.assistant.history', { expectedAccountId: f.account.accountId }));
  assert.equal((await stranger.extension('apps.agent.archiveReceipt', { expectedAccountId: other.accountId, jobId: id })).state, 'unconfirmed');
  await assert.rejects(stranger.extension('apps.agent.archive', { expectedAccountId: other.accountId, jobId: id }));
  assert.equal((await f.owner.extension('apps.agent.archiveReceipt', args)).state, 'unconfirmed');
  const archived = await f.owner.extension('apps.agent.archive', args); assert.equal(archived.state, 'archived');
  assert.equal((await f.owner.extension('apps.assistant.history', { expectedAccountId: f.account.accountId })).jobs.find(job => job.id === id).restrictedAuto, 'archived');
  assert.equal((await f.owner.extension('apps.agent.archiveReceipt', args)).receipt.requestId, archived.receipt.requestId);
  await store.mutate(() => store.expire(Date.now() + 8 * 24 * 60 * 60_000));
  assert.equal((await store.poll(f.auth)).error, 'connector-auth-failed', 'the simulated offline connector expired');
  assert.equal(store.state.jobs.some(job => job.id === id), false); await f.restart({}, { announce: false });
  const recovered = await f.owner.extension('apps.assistant.history', { expectedAccountId: f.account.accountId });
  const final = recovered.jobs.find(job => job.id === id); assert.equal(final.restrictedAuto, 'archived'); assert.equal(final.cwd, '');
  assert.doesNotMatch(JSON.stringify(recovered), /SECRET-PROMPT|SECRET-PATH|SECRET-RESULT/u);
  assert.deepEqual((await f.owner.extension('apps.agent.archiveReceipt', args)).receipt, archived.receipt);
  assert.deepEqual((await stranger.extension('apps.assistant.history', { expectedAccountId: other.accountId })).jobs, []);
});

test('an archived job may expire but its original request and archive receipt remain readable', { timeout: 25000 }, async t => {
  const f = await fixture(t); await f.owner.extension('apps.register', { ...f.target, name: 'Список', port: f.source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  const id = await f.create(); await f.submit(id); await until(async () => (await f.read(id)).autoRegistration?.state === 'conflict');
  const args = { expectedAccountId: f.account.accountId, jobId: id }, archived = await f.owner.extension('apps.agent.archive', args);
  const store = f.service().locals.connectorService.store;
  await store.mutate(() => { const job = store.state.jobs.find(value => value.id === id), old = Date.now() - 8 * 24 * 60 * 60_000;
    job.createdAt = old - 10; job.updatedAt = old; job.finishedAt = old; store.expire(Date.now()); });
  assert.equal(store.state.jobs.some(job => job.id === id), false); await f.restart();
  const recovered = await f.owner.extension('apps.agent.archive', args);
  assert.equal(recovered.replayed, true); assert.deepEqual(recovered.receipt, archived.receipt);
  assert.ok(recovered.receipt.requestId); assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('a failed archive receipt write rolls back cancellation and keeps the old-reader fence', { timeout: 25000 }, async t => {
  const f = await fixture(t); await f.owner.extension('apps.register', { ...f.target, name: 'Список', port: f.source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  const id = await f.create(); await f.submit(id); await until(async () => (await f.read(id)).autoRegistration?.state === 'conflict');
  const store = f.service().locals.connectorService.store, db = new DatabaseSync(join(dirname(store.filePath), 'connector-store.sqlite'));
  try {
    db.exec("CREATE TRIGGER fixture_fail_archive BEFORE INSERT ON requests WHEN json_extract(NEW.value,'$.autoArchive.schema')='soty.app-auto-archive.v1' BEGIN SELECT RAISE(ABORT,'synthetic archive failure'); END");
    const args = { expectedAccountId: f.account.accountId, jobId: id }; await assert.rejects(f.owner.extension('apps.agent.archive', args));
    const job = store.state.jobs.find(value => value.id === id); assert.equal(job.cancelRequested, false); assert.equal(job.autoRegistrationFinal, undefined);
    assert.equal(store.state.requests.some(value => value.autoArchive?.jobId === id), false);
    assert.equal(db.prepare("SELECT value FROM meta WHERE key='schema'").get().value, 'soty.connector-sqlite.v2');
    db.exec('DROP TRIGGER fixture_fail_archive'); assert.equal((await f.owner.extension('apps.agent.archive', args)).state, 'archived');
  } finally { db.close(); }
});

test('revoking the current actor before archive commit preserves the pending result', { timeout: 30000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId); const secondDevice = (await second.getLocalState()).deviceId;
  await f.owner.extension('apps.register', { ...f.target, name: 'Список', port: f.source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  const id = await f.create(); await f.submit(id); await until(async () => (await f.read(id)).autoRegistration?.state === 'conflict');
  const store = f.service().locals.connectorService.store, actual = store.archiveAutomaticAppJob.bind(store);
  let reached, release; const entered = new Promise(resolve => { reached = resolve; }), hold = new Promise(resolve => { release = resolve; });
  store.archiveAutomaticAppJob = async (...args) => { reached(); await hold; return actual(...args); };
  const request = second.extension('apps.agent.archive', { expectedAccountId: f.account.accountId, jobId: id });
  try { await entered; await f.owner.revokeDevice(secondDevice); } finally { release(); }
  await assert.rejects(request); const job = store.state.jobs.find(value => value.id === id);
  assert.equal(job.autoRegistrationFinal, undefined); assert.equal(job.cancelRequested, false);
  assert.equal(store.state.requests.some(value => value.autoArchive?.jobId === id), false);
  assert.equal((await f.owner.extension('apps.agent.archiveReceipt', { expectedAccountId: f.account.accountId, jobId: id })).state, 'unconfirmed');
});

test('revocation after the last guard yields an unknown reply and the active owner reads the one receipt', { timeout: 30000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId); const secondDevice = (await second.getLocalState()).deviceId;
  await f.owner.extension('apps.register', { ...f.target, name: 'Список', port: f.source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  const id = await f.create(); await f.submit(id); await until(async () => (await f.read(id)).autoRegistration?.state === 'conflict');
  const store = f.service().locals.connectorService.store, original = store.persistence.call.bind(store.persistence);
  let reached, release; const entered = new Promise(resolve => { reached = resolve; }), hold = new Promise(resolve => { release = resolve; });
  store.persistence.call = async (action, delta) => { if (action === 'commit' && delta.requests?.some(row => row.autoArchive?.jobId === id)) { reached(); await hold; } return original(action, delta); };
  const request = second.extension('apps.agent.archive', { expectedAccountId: f.account.accountId, jobId: id });
  try { await entered; await f.owner.revokeDevice(secondDevice); } finally { release(); }
  await assert.rejects(request); const job = store.state.jobs.find(value => value.id === id);
  const records = store.state.requests.filter(value => value.autoArchive?.jobId === id);
  assert.equal(job.autoRegistrationFinal, 'archived'); assert.equal(records.length, 1); assert.equal(records[0].autoArchive.actorDeviceId, secondDevice);
  const read = await f.owner.extension('apps.agent.archiveReceipt', { expectedAccountId: f.account.accountId, jobId: id });
  assert.equal(read.state, 'archived'); assert.equal(read.receipt.requestId, job.requestId); assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('revoking the original signed device before recovery prevents automatic registration', { timeout: 25000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  const id = await f.create(); assert.equal((await f.service().locals.connectorService.store.finishJob(f.auth, id, f.result(id))).ok, true);
  await second.revokeDevice(f.account.deviceId); await f.restart();
  const denied = await until(async () => { const value = await f.read(id, second); return value.autoRegistration?.state === 'access-lost' && value; });
  assert.equal(denied.autoRegistration.state, 'access-lost'); assert.equal((await second.extension('apps.list')).apps.length, 0);
  assert.equal(f.service().locals.connectorService.store.state.jobs.find(job => job.id === id).autoRegistrationFinal, undefined);
});

test('loss of the original actor after the app commit is reported without completing feedback under another device', { timeout: 25000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  const id = await f.create(), store = f.service().locals.connectorService.store;
  assert.equal((await store.finishJob(f.auth, id, f.result(id))).ok, true);
  const [job] = await store.automaticAppJobs(id), actor = { accountId: job.ownerAccountId, deviceId: job.sourceActorDeviceId };
  const partial = f.service().locals.connectService.withCurrentContacts(actor, () => f.service().locals.appsService.registerCreationJob(actor, job));
  await second.revokeDevice(f.account.deviceId); await f.restart();
  const value = await until(async () => { const reply = await f.read(id, second); return reply.autoRegistration?.state === 'access-lost' && reply; });
  assert.equal(value.autoRegistration.appId, partial.app.id);
  const feedback = new DatabaseSync(join(f.dataDir, 'feedback', 'feedback.sqlite'));
  try { assert.equal(feedback.prepare('SELECT count(*) AS count FROM feedback_installations').get().count, 0); }
  finally { feedback.close(); }
});

test('archiving after app commit keeps one launchable card and its persisted data without completing feedback', { timeout: 30000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  const businessPath = join(f.dataDir, 'synthetic-business.sqlite'); let business = new DatabaseSync(businessPath);
  business.exec("CREATE TABLE records (value TEXT NOT NULL); INSERT INTO records VALUES ('durable-personal-record')");
  f.source.removeAllListeners('request'); f.source.on('request', (_req, res) => res.end(business.prepare('SELECT value FROM records').get().value));
  try {
    const id = await f.create(), store = f.service().locals.connectorService.store;
    assert.equal((await store.finishJob(f.auth, id, f.result(id))).ok, true);
    const [job] = await store.automaticAppJobs(id), actor = { accountId: job.ownerAccountId, deviceId: job.sourceActorDeviceId };
    const partial = f.service().locals.connectService.withCurrentContacts(actor, () => f.service().locals.appsService.registerCreationJob(actor, job));
    const appId = partial.app.id;
    assert.equal((await second.extension('apps.universal.get', { appId, expectedAccountId: f.account.accountId })).registration, null);
    await second.revokeDevice(f.account.deviceId); await f.restart();
    await until(async () => (await f.read(id, second)).autoRegistration?.state === 'access-lost');
    const archived = await second.extension('apps.agent.archive', { expectedAccountId: f.account.accountId, jobId: id });
    assert.equal(archived.state, 'archived'); assert.equal(archived.receipt.requestId, job.requestId);
    assert.equal((await second.extension('apps.list')).apps.length, 1); assert.equal((await second.extension('apps.list')).apps[0].id, appId);
    business.close(); business = new DatabaseSync(businessPath);
    assert.equal((await f.submit(id)).ok, true); await f.poll(); await f.restart();
    assert.equal((await f.service().locals.connectorService.store.automaticAppJobs(id)).length, 0);
    assert.equal((await second.extension('apps.list')).apps.length, 1); assert.equal((await second.extension('apps.list')).apps[0].id, appId);
    assert.equal((await second.extension('apps.universal.get', { appId, expectedAccountId: f.account.accountId })).registration, null);
    const launch = await until(async () => { try { return await second.extension('apps.launch', { appId }); } catch { return null; } });
    const url = new URL(launch.launchUrl);
    const boot = await appRequest(url, '/_soty/session', { method: 'POST', headers: { origin: url.origin, 'content-type': 'application/json' }, body: JSON.stringify({ ticket: url.hash.slice(1) }) });
    assert.equal(boot.status, 200);
    const page = await appRequest(url, '/', { headers: { cookie: boot.headers['set-cookie'][0].split(';')[0] } });
    assert.equal(page.status, 200); assert.equal(page.body, 'durable-personal-record');
    assert.deepEqual((await second.extension('apps.agent.archiveReceipt', { expectedAccountId: f.account.accountId, jobId: id })).receipt, archived.receipt);
  } finally { business.close(); }
});

test('a completed registration stays ready for the owner after the initiating device is revoked', { timeout: 25000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  const id = await f.create(); await f.submit(id);
  await until(async () => (await f.read(id)).autoRegistration?.feedback === 'ready');
  await second.revokeDevice(f.account.deviceId); await f.restart();
  const result = await f.read(id, second);
  assert.equal(result.autoRegistration.state, 'ready'); assert.equal(result.autoRegistration.appId, `app-${id.slice(4)}`);
  assert.equal((await second.extension('apps.list')).apps.length, 1);
});
