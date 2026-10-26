import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
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
async function fixture(t) {
  const folder = await mkdtemp(join(tmpdir(), 'soty-auto-app-')), dist = join(folder, 'dist'), dataDir = join(folder, 'data');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html>');
  let app, runtime, loseCreateAck = false; const clients = [], sockets = new Set();
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
    body: JSON.stringify({ ...auth, scope: 'Dev', capabilities: ['apps', 'agent'], agent: { id: 'opencode', provider: 'gonka', available: true } }) });
  assert.equal((await registered.json()).ok, true);
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
  async function create(autoRegister = true) {
    const result = await owner.extension('apps.agent.create', { ...target, requestId: `auto-request-${++sequence}`, text: 'Создать список', cwd: '', ...(autoRegister ? { autoRegister: true } : {}) });
    const leased = await app.locals.connectorService.store.poll(auth, 0); assert.equal(leased.jobs[0]?.id, result.job.id);
    return result.job.id;
  }
  const proposal = jobId => ({ schema: 'soty.local-app.v1', name: 'Список', port: source.address().port, entryPath: '/', sourceJobId: jobId });
  const result = jobId => ({ ok: true, text: 'Готово', exitCode: 0, appProposal: proposal(jobId) });
  async function submit(jobId) {
    const response = await fetch(`${origin}/api/connectors/jobs/${jobId}/result`, { method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` }, body: JSON.stringify({ ...auth, result: result(jobId) }) });
    return response.json();
  }
  async function poll() {
    const query = new URLSearchParams({ linkId: auth.linkId, deviceId: auth.deviceId, connectorId: auth.connectorId });
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
    capabilities: async () => (await fetch(origin + '/api/apps/capabilities')).json(), loseNextCreateAck: () => { loseCreateAck = true; },
    service: () => app, async restart(overrides = {}) { await app.locals.closeServices(); app = createHttpApp(dist, { ...options, ...overrides }); } };
}

test('capability gates a new auto intent while the original lost ACK replays on a temporarily disabled server', { timeout: 25000 }, async t => {
  const f = await fixture(t), capability = (await f.capabilities()).automaticAppRegistration;
  assert.equal(capability.schema, 'soty.automatic-app-registration-capability.v1'); assert.equal(capability.available, true);
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

test('one requested job registers one private app and real universal feedback despite duplicate result delivery', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create();
  assert.ok((await Promise.all([f.submit(id), f.submit(id)])).every(value => value.ok));
  const done = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value; });
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
  const done = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value; });
  assert.equal(done.autoRegistration.appId, `app-${id.slice(4)}`); assert.equal((await f.owner.extension('apps.list')).apps.length, 1);
});

test('restart finishes universal and feedback after the app transaction committed alone', { timeout: 25000 }, async t => {
  const f = await fixture(t), id = await f.create(), store = f.service().locals.connectorService.store;
  assert.equal((await store.finishJob(f.auth, id, f.result(id))).ok, true);
  const [job] = await store.automaticAppJobs(id), actor = { accountId: job.ownerAccountId, deviceId: job.sourceActorDeviceId };
  const partial = f.service().locals.connectService.withCurrentContacts(actor, () => f.service().locals.appsService.registerCreationJob(actor, job));
  assert.equal(partial.app.id, `app-${id.slice(4)}`);
  assert.equal((await f.owner.extension('apps.universal.get', { appId: partial.app.id, expectedAccountId: f.account.accountId })).registration, null);
  await f.restart();
  const done = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value; });
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

test('temporary feedback lock leaves a pending registration and the existing connector poll completes it', { timeout: 30000 }, async t => {
  const f = await fixture(t), id = await f.create(), feedback = new DatabaseSync(join(f.dataDir, 'feedback', 'feedback.sqlite'));
  feedback.exec('BEGIN IMMEDIATE');
  try {
    await f.submit(id);
    const held = await until(async () => { const value = await f.read(id); return value.autoRegistration?.appId && value.autoRegistration.state === 'waiting' && value; });
    assert.equal(held.autoRegistration.appId, `app-${id.slice(4)}`);
  } finally { feedback.exec('ROLLBACK'); feedback.close(); }
  assert.equal((await f.poll()).ok, true);
  const ready = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value; });
  assert.equal(ready.autoRegistration.appId, `app-${id.slice(4)}`);
});

test('existing server heartbeat retries pending feedback even after the connector goes offline', { timeout: 30000 }, async t => {
  const f = await fixture(t), id = await f.create(), feedback = new DatabaseSync(join(f.dataDir, 'feedback', 'feedback.sqlite'));
  feedback.exec('BEGIN IMMEDIATE');
  try {
    await f.submit(id);
    await until(async () => { const value = await f.read(id); return value.autoRegistration?.appId && value.autoRegistration.state === 'waiting'; });
  } finally { feedback.exec('ROLLBACK'); feedback.close(); }
  f.stopRuntime();
  const ready = await until(async () => { const value = await f.read(id); return value.autoRegistration?.state === 'ready' && value; }, 14000);
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

test('revoking the original signed device before recovery prevents automatic registration', { timeout: 25000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  const id = await f.create(); assert.equal((await f.service().locals.connectorService.store.finishJob(f.auth, id, f.result(id))).ok, true);
  await second.revokeDevice(f.account.deviceId); await f.restart();
  const denied = await until(async () => { const value = await f.read(id, second); return value.autoRegistration?.state === 'access-lost' && value; });
  assert.equal(denied.autoRegistration.state, 'access-lost'); assert.equal((await second.extension('apps.list')).apps.length, 0);
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

test('a completed registration stays ready for the owner after the initiating device is revoked', { timeout: 25000 }, async t => {
  const f = await fixture(t), second = f.makeClient(), enrollment = await second.startEnrollment('Second device');
  await f.owner.approveEnrollment(enrollment.requestId); const preview = await second.previewEnrollment(enrollment.requestId);
  await second.finishEnrollment(enrollment.requestId, preview.account.accountId);
  const id = await f.create(); await f.submit(id);
  await until(async () => (await f.read(id)).autoRegistration?.state === 'ready');
  await second.revokeDevice(f.account.deviceId); await f.restart();
  const result = await f.read(id, second);
  assert.equal(result.autoRegistration.state, 'ready'); assert.equal(result.autoRegistration.appId, `app-${id.slice(4)}`);
  assert.equal((await second.extension('apps.list')).apps.length, 1);
});
