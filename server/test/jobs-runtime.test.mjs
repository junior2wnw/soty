import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import express from 'express';
import { attachConnectorApi } from '../connector-api.js';
import { createAppJobsExtension } from '../apps-jobs.js';
import { createConnectHandler } from '../../modules/connect/server/http.mjs';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';

function identity() {
  const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { privateKey: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }),
    encryptionPublicJwk: generateKeyPairSync('ec', { namedCurve: 'prime256v1' }).publicKey.export({ format: 'jwk' }) };
}
async function fixture(t) {
  const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-runtime-http-'));
  const app = express();
  const { store } = attachConnectorApi(app, { dataDir: directory, gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
  const server = createServer(app);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const owner = identity();
  const auth = { linkId: 'runtime-http-link-12345678901234567890', deviceId: 'runtime-http-device',
    connectorId: 'runtime-http-connector', token: randomBytes(32).toString('base64url') };
  let service, ownerAccountId;
  const extension = createAppJobsExtension({ store, actorActive: actor => service.isActorActive(actor),
    resolveOwnedDevice: (actor, ids) => actor.accountId === ownerAccountId && ids.hostDeviceId === auth.deviceId && ids.connectorId === auth.connectorId
      ? { linkId: auth.linkId, hostDeviceId: auth.deviceId, connectorId: auth.connectorId } : null });
  service = createConnectService({ databasePath: ':memory:', projectId: 'runtime-http', allowedOrigins: [origin], extensions: [extension] });
  app.use(createConnectHandler(service));
  app.use((error, _req, res, _next) => res.status(error.status === 413 ? 413 : 500).json({ ok: false, error: 'request-rejected' }));
  t.after(async () => {
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
    await store.close(); service.close();
    assert.equal(dirname(resolve(directory)), base);
    assert.ok(resolve(directory).startsWith(join(base, 'soty-runtime-http-')));
    rmSync(directory, { recursive: true, force: true });
  });
  async function json(path, { body, headers = {} } = {}) {
    const response = await fetch(origin + path, { method: body === undefined ? 'GET' : 'POST', headers: {
      ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers,
    }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
    return { status: response.status, cacheControl: response.headers.get('cache-control'), body: await response.json() };
  }
  const sendRpc = body => json('/api/connect/rpc', { body: { protocol: 1, ...body }, headers: { origin } });
  async function invoke(op, args = {}) {
    const challenge = (await sendRpc({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } })).body;
    assert.equal(challenge.ok, true, challenge.error?.code);
    return (await sendRpc({ op, args, proof: { challengeId: challenge.challengeId, publicJwk: owner.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: owner.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } })).body;
  }
  function runtime(path, { identity = auth, body, headers = {} } = {}) {
    return json(path, { body, headers: {
      authorization: `Bearer ${identity.token}`, 'x-soty-link-id': identity.linkId,
      'x-soty-device-id': identity.deviceId, 'x-soty-connector-id': identity.connectorId, ...headers,
    } });
  }
  async function register(identity = auth) {
    const response = await runtime('/api/connectors/register', { identity, body: { linkId: identity.linkId,
      deviceId: identity.deviceId, connectorId: identity.connectorId, deviceNick: 'Runtime test', scope: 'CurrentUser',
      capabilities: ['agent'], agent: { id: 'opencode', provider: 'gonka', available: true } } });
    assert.equal(response.body.ok, true, response.body.error);
  }
  ownerAccountId = (await invoke('bootstrap', { label: 'Runtime owner', encryptionPublicJwk: owner.encryptionPublicJwk })).accountId;
  assert.ok(ownerAccountId); await register();
  const createArgs = { hostDeviceId: auth.deviceId, connectorId: auth.connectorId, requestId: 'runtime-http-create', text: 'Создай приложение' };
  const target = jobId => ({ hostDeviceId: auth.deviceId, connectorId: auth.connectorId, jobId });
  return { store, auth, json, runtime, register, invoke, createArgs, target };
}

test('an account-owned runtime sees live cancellation after a real HTTP lease without exposing the job to shared links', async t => {
  const f = await fixture(t);
  const created = await f.invoke('apps.agent.create', f.createArgs);
  assert.equal(created.ok, true, created.error?.code);
  const jobId = created.job.id, path = `/api/connectors/jobs/${jobId}/runtime-status`;
  assert.equal((await f.invoke('apps.agent.create', { ...f.createArgs, cwd: '' })).job.id, jobId);
  assert.equal((await f.runtime(path)).status, 401, 'a credential does not authorize a job before assignment');
  const sibling = { ...f.auth, connectorId: 'runtime-http-sibling', token: randomBytes(32).toString('base64url') };
  const otherDevice = { ...f.auth, deviceId: 'runtime-http-other-device', token: randomBytes(32).toString('base64url') };
  await f.register(sibling); await f.register(otherDevice);
  assert.deepEqual((await f.runtime('/api/connectors/poll', { identity: sibling })).body.jobs, []);
  const poll = await f.runtime('/api/connectors/poll');
  assert.equal(poll.body.jobs[0].id, jobId);
  assert.equal(poll.body.jobs[0].input.cwd, '', 'the connector chooses the auto workspace on its own host');
  const expected = { ok: true, job: { id: jobId, status: 'leased', cancelRequested: false } };
  const initialState = await f.runtime(path);
  assert.deepEqual(initialState.body, expected); assert.equal(initialState.cacheControl, 'no-store');
  await new Promise(resolve => setTimeout(resolve, 1_100));
  assert.deepEqual((await f.runtime(path)).body, expected, 'the first one-second cancellation check must keep the job running');
  for (const identity of [sibling, otherDevice, { ...f.auth, token: randomBytes(32).toString('base64url') }]) {
    const response = await f.runtime(path, { identity });
    assert.equal(response.status, 401);
    assert.deepEqual(response.body, { ok: false, error: 'connector-auth-failed' });
  }
  const linkHeaders = { 'x-soty-link-id': f.auth.linkId };
  assert.equal((await f.json(path, { headers: linkHeaders })).status, 401);
  assert.equal((await f.json(`/api/connectors/jobs/${jobId}`, { headers: linkHeaders })).status, 404);
  assert.equal((await f.runtime(`/api/connectors/jobs/${jobId}`)).status, 404, 'runtime credentials do not widen the shared-link route');
  const eventAck = await f.runtime(`/api/connectors/jobs/${jobId}/events`, { body: { event: { type: 'message', text: 'private progress' } } });
  assert.deepEqual(eventAck.body, { ok: true, job: { id: jobId, status: 'running', cancelRequested: false } });
  assert.equal(eventAck.cacheControl, 'no-store');
  assert.deepEqual((await f.runtime(path)).body, { ok: true, job: { id: jobId, status: 'running', cancelRequested: false } });
  assert.equal((await f.invoke('apps.agent.cancel', f.target(jobId))).job.cancelRequested, true);
  assert.deepEqual((await f.runtime(path)).body, { ok: true, job: { id: jobId, status: 'running', cancelRequested: true } });
  assert.ok((await f.runtime('/api/connectors/poll')).body.cancel.includes(jobId));
  assert.equal((await f.runtime(`/api/connectors/jobs/${jobId}/result`, { body: { result: { ok: false, text: 'Отменено', exitCode: 130 } } })).body.ok, true);
  assert.deepEqual((await f.runtime(path)).body, { ok: true, job: { id: jobId, status: 'cancelled', cancelRequested: true } });
});

test('HTTP result uploads accept the bounded escaped-text maximum, cap stored metadata and reject larger bodies before completion', async t => {
  const f = await fixture(t);
  const jobId = (await f.invoke('apps.agent.create', f.createArgs)).job.id;
  assert.equal((await f.runtime('/api/connectors/poll')).body.jobs[0].id, jobId);
  const path = `/api/connectors/jobs/${jobId}/result`;
  const excessive = await f.runtime(path, { body: { result: { ok: true, text: 'x'.repeat(6 * 1024 * 1024), exitCode: 0 } } });
  assert.equal(excessive.status, 413);
  assert.equal((await f.runtime(`/api/connectors/jobs/${jobId}/runtime-status`)).body.job.status, 'leased');
  const result = { ok: true, text: '\u0001'.repeat(1_000_050), exitCode: 0, sessionId: 's'.repeat(1000),
    agentId: 'opencode', ignored: 'this is not a public result field',
    appProposal: { schema: 'soty.local-app.v1', name: 'N'.repeat(300), port: 3100, entryPath: '/', sourceJobId: jobId } };
  const bytes = Buffer.byteLength(JSON.stringify({ result }), 'utf8');
  assert.ok(bytes > 1024 * 1024 && bytes < 6 * 1024 * 1024);
  const finished = await f.runtime(path, { body: { result } });
  assert.equal(finished.status, 200);
  assert.deepEqual(finished.body, { ok: true, job: { id: jobId, status: 'succeeded', cancelRequested: false } });
  assert.ok(Buffer.byteLength(JSON.stringify(finished.body), 'utf8') < 256, 'upload acknowledgement must not echo the megabyte result');
  assert.equal(finished.cacheControl, 'no-store');
  const read = await f.invoke('apps.agent.read', f.target(jobId));
  assert.equal(read.ok, true, read.error?.code);
  assert.equal(read.job.result.textLength, 1_000_000);
  assert.equal(Object.hasOwn(read.job.result, 'sessionId'), false, 'runtime continuation credentials stay server-side');
  assert.equal(f.store.state.jobs.find(job => job.id === jobId).result.sessionId.length, 200, 'the stored continuation metadata remains bounded');
  assert.equal(read.job.result.appProposal.name.length, 64);
  assert.equal(Object.hasOwn(read.job.result, 'ignored'), false);
  const first = await f.invoke('apps.agent.result', { ...f.target(jobId), offset: 0 });
  assert.equal(first.total, 1_000_000); assert.equal(first.text, '\u0001'.repeat(8000));
  const last = await f.invoke('apps.agent.result', { ...f.target(jobId), offset: 999_999 });
  assert.equal(last.text, '\u0001'); assert.equal(last.nextOffset, null);
});
