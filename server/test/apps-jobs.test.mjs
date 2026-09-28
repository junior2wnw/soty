import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createConnectorStore, normalizeConnectorState } from '../connector-store.js';
import { createAppJobsExtension } from '../apps-jobs.js';

const origin = 'http://localhost:5200';
function identity() {
  const keys = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { privateKey: keys.privateKey, publicJwk: keys.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: generateKeyPairSync('ec', { namedCurve: 'prime256v1' }).publicKey.export({ format: 'jwk' }) };
}
async function proof(service, actor, op, args) {
  const challenge = await service.handle({ origin, op: 'challenge', args: { operation: op, digest: digestArgs(args) } });
  assert.equal(challenge.ok, true);
  return { origin, op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
    signature: sign('sha256', Buffer.from(challenge.message), { key: actor.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
}
async function invoke(service, actor, op, args = {}) { return service.handle(await proof(service, actor, op, args)); }
async function setup(t) {
  const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-app-jobs-'));
  const store = createConnectorStore(directory);
  let accountId = '', permitted = true, inferenceReady = true, service;
  const host = { linkId: 'owned-device-link-12345678901234567890', hostDeviceId: 'device-test-host', connectorId: 'connector-test-user' };
  const ownedHosts = [host];
  const extension = createAppJobsExtension({ store, actorActive: actor => service.isActorActive(actor),
    inferenceReady: () => inferenceReady,
    resolveOwnedDevice: (actor, ids) => permitted && actor.accountId === accountId
      ? ownedHosts.find(item => ids.hostDeviceId === item.hostDeviceId && ids.connectorId === item.connectorId) || null : null });
  service = createConnectService({ databasePath: ':memory:', projectId: 'soty', allowedOrigins: [origin], extensions: [extension] });
  const owner = identity(), stranger = identity();
  accountId = (await invoke(service, owner, 'bootstrap', { label: 'Owner', encryptionPublicJwk: owner.encryptionPublicJwk })).accountId;
  await invoke(service, stranger, 'bootstrap', { label: 'Stranger', encryptionPublicJwk: stranger.encryptionPublicJwk });
  const token = randomBytes(32).toString('base64url');
  const auth = { linkId: host.linkId, deviceId: host.hostDeviceId, connectorId: host.connectorId, token };
  const registration = { ...auth, scope: 'CurrentUser', deviceNick: 'Test host', capabilities: ['agent'], agent: { id: 'opencode', provider: 'gonka', available: true } };
  delete registration.token;
  assert.equal((await store.register(registration, token)).ok, true);
  t.after(async () => {
    await store.close(); service.close();
    assert.equal(dirname(resolve(directory)), base);
    assert.ok(resolve(directory).startsWith(join(base, 'soty-app-jobs-')));
    rmSync(directory, { recursive: true, force: true });
  });
  return { store, service, host, owner, stranger, auth, accountId, disable() { permitted = false; },
    ownHost(value) { ownedHosts.push(value); }, setInferenceReady(value) { inferenceReady = value; },
    createArgs: { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, requestId: 'create-a-shopping-app', text: 'Список покупок', cwd: 'C:\\test-workspace' } };
}

test('signed app jobs are owned, replay-safe, durable and inaccessible through a shared room link', async t => {
  const f = await setup(t);
  const created = await invoke(f.service, f.owner, 'apps.agent.create', f.createArgs);
  assert.equal(created.ok, true, created.error?.code);
  assert.equal(created.job.schema, 'soty.connector-job.v4');
  const jobId = created.job.id;
  const malformed = structuredClone(f.store.state);
  delete malformed.jobs.find(job => job.id === jobId).ownerAccountId;
  assert.throws(() => normalizeConnectorState(malformed), /Account job missing account\/request binding/u);
  assert.equal((await invoke(f.service, f.owner, 'apps.agent.create', f.createArgs)).job.id, jobId);
  assert.equal((await invoke(f.service, f.owner, 'apps.agent.create', { ...f.createArgs, text: 'Changed' })).error.code, 'job_request_conflict');
  assert.equal((await invoke(f.service, f.stranger, 'apps.agent.create', f.createArgs)).error.code, 'apps_device_not_owned');
  assert.equal((await f.store.getJob(f.host.linkId, jobId)).ok, false);
  assert.equal((await f.store.cancelJob(f.host.linkId, jobId)).ok, false);
  const accountId = (await invoke(f.service, f.owner, 'status')).accountId;
  assert.equal((await f.store.cancelJob(f.host.linkId, jobId, { ownerAccountId: accountId, expectedDeviceId: 'other-owned-device', connectorId: f.host.connectorId })).ok, false);
  assert.equal((await f.store.cancelJob(f.host.linkId, jobId, { ownerAccountId: accountId, expectedDeviceId: f.host.hostDeviceId, connectorId: 'other-connector' })).ok, false);
  assert.equal((await f.store.getJob(f.host.linkId, jobId, { ownerAccountId: accountId })).job.status, 'queued');
  const leased = await f.store.poll(f.auth, 0);
  assert.equal(leased.jobs[0].id, jobId);
  assert.equal(leased.jobs[0].input.output, 'local-app');
  const proposal = { schema: 'soty.local-app.v1', name: 'Покупки', port: 3000, entryPath: '/', sourceJobId: jobId };
  await f.store.finishJob(f.auth, jobId, { ok: true, text: 'Готово', exitCode: 0, appProposal: proposal });
  const read = await invoke(f.service, f.owner, 'apps.agent.read', { hostDeviceId: f.host.hostDeviceId, connectorId: f.host.connectorId, jobId });
  assert.equal(read.done, true);
  assert.deepEqual(read.job.result.appProposal, proposal);
  await f.store.close();
  const reopened = createConnectorStore(dirname(f.store.filePath));
  try { assert.equal((await reopened.getJob(f.host.linkId, jobId, { ownerAccountId: accountId })).job.id, jobId); }
  finally { await reopened.close(); }
});

test('async Connect extensions release the proof transaction and recheck owner access inside the job queue', async t => {
  const f = await setup(t);
  let release;
  f.store.writeQueue = new Promise(resolveQueue => { release = resolveQueue; });
  const request = await proof(f.service, f.owner, 'apps.agent.create', f.createArgs);
  const creating = f.service.handle(request);
  const status = await invoke(f.service, f.owner, 'status');
  assert.equal(status.ok, true);
  assert.equal((await f.service.handle(request)).error.code, 'challenge_consumed');
  f.disable(); release();
  const denied = await creating;
  assert.equal(denied.ok, false);
  assert.equal(f.store.state.jobs.length, 0);
});

function boundedReply(result, maxBytes = 1024 * 1024) {
  assert.equal(result.ok, true, result.error?.code);
  const wire = JSON.stringify(result);
  assert.ok(Buffer.byteLength(wire, 'utf8') < maxBytes, 'signed reply must fit its transport envelope');
  return JSON.parse(wire);
}
function target(f, jobId, extra = {}) {
  return { hostDeviceId: f.host.hostDeviceId, connectorId: f.host.connectorId, jobId, ...extra };
}

test('large escaped job output remains bounded through signed cancellation, replay, events and complete result pages', async t => {
  const f = await setup(t);
  const created = boundedReply(await invoke(f.service, f.owner, 'apps.agent.create', f.createArgs));
  const jobId = created.job.id;
  assert.equal((await f.store.poll(f.auth, 0)).jobs[0].id, jobId);
  const expectedEvents = [];
  for (let index = 0; index < 6; index += 1) {
    // Control characters expand sixfold in JSON. Event data uses multibyte UTF-8.
    const event = { type: 'progress', text: `step ${index}:` + '\u0001'.repeat(63_970) + '🚀',
      data: { index, output: '界'.repeat(10_000) } };
    assert.equal((await f.store.appendEvent(f.auth, jobId, event)).ok, true);
    expectedEvents.push(event);
  }
  assert.ok(Buffer.byteLength(JSON.stringify(expectedEvents), 'utf8') > 2 * 1024 * 1024);
  const cancelling = boundedReply(await invoke(f.service, f.owner, 'apps.agent.cancel', target(f, jobId)), 256_000);
  assert.equal(cancelling.job.cancelRequested, true);
  assert.equal(Object.hasOwn(cancelling.job, 'events'), false);

  // The surrogate pair crosses some page boundaries; JSON roundtrips and joining
  // all pages must preserve the original text exactly, including those pairs.
  const fullText = ('\u0001'.repeat(7) + '🚀').repeat(111_100) + 'Конец';
  assert.ok(Buffer.byteLength(JSON.stringify({ text: fullText }), 'utf8') > 2 * 1024 * 1024);
  assert.equal((await f.store.finishJob(f.auth, jobId, { ok: false, text: fullText, exitCode: 130 })).ok, true);
  for (const [op, args] of [
    ['apps.agent.cancel', target(f, jobId)],
    ['apps.agent.create', f.createArgs],
  ]) {
    const reply = boundedReply(await invoke(f.service, f.owner, op, args), 256_000);
    assert.equal(reply.job.id, jobId);
    assert.equal(reply.job.status, 'cancelled');
    assert.equal(Object.hasOwn(reply.job, 'events'), false);
    assert.equal(reply.job.result.textTruncated, true);
    assert.equal(reply.job.result.textLength, fullText.length);
    assert.ok(reply.job.result.text.length < fullText.length);
    assert.ok(fullText.startsWith(reply.job.result.text));
  }
  assert.equal(f.store.state.jobs.length, 1, 'retry must not create a duplicate job');

  const receivedEvents = [];
  let cursor = 0, eventPages = 0;
  for (;;) {
    const reply = boundedReply(await invoke(f.service, f.owner, 'apps.agent.read', target(f, jobId, { after: cursor })));
    assert.equal(reply.job.result.textTruncated, true);
    assert.equal(Object.hasOwn(reply.job, 'events'), false);
    for (const event of reply.events) {
      assert.ok(event.seq > cursor, 'page cursor must advance without duplicate events');
      cursor = event.seq;
      if (event.type === 'progress') receivedEvents.push({ type: event.type, text: event.text, data: event.data });
    }
    assert.equal(reply.cursor, cursor);
    eventPages += 1;
    if (reply.done) break;
    assert.ok(reply.events.length > 0 && eventPages < 20, 'finite history must make pagination progress');
  }
  assert.ok(eventPages > 1);
  assert.deepEqual(receivedEvents, expectedEvents);
  const exhausted = boundedReply(await invoke(f.service, f.owner, 'apps.agent.read', target(f, jobId, { after: cursor })));
  assert.deepEqual(exhausted.events, []); assert.equal(exhausted.done, true);

  const pieces = [];
  let offset = 0;
  for (;;) {
    const reply = boundedReply(await invoke(f.service, f.owner, 'apps.agent.result', target(f, jobId, { offset })), 64_000);
    assert.equal(reply.total, fullText.length);
    assert.ok(reply.text.length > 0);
    pieces.push(reply.text);
    if (reply.nextOffset === null) break;
    assert.equal(reply.nextOffset, offset + reply.text.length);
    assert.ok(reply.nextOffset > offset);
    offset = reply.nextOffset;
  }
  assert.ok(pieces.length > 1);
  assert.equal(pieces.join(''), fullText);
  const finalPage = boundedReply(await invoke(f.service, f.owner, 'apps.agent.result', target(f, jobId, { offset: fullText.length })));
  assert.equal(finalPage.text, ''); assert.equal(finalPage.nextOffset, null);
});

test('each signed output page and cancellation checks the selected owner, device, connector and queued revocation', async t => {
  const f = await setup(t);
  const created = await invoke(f.service, f.owner, 'apps.agent.create', f.createArgs);
  const jobId = created.job.id;
  const mismatchedHosts = [
    { ...f.host, hostDeviceId: 'another-owned-device' },
    { ...f.host, connectorId: 'another-owned-connector' },
  ];
  for (const host of mismatchedHosts) {
    f.ownHost(host);
    for (const op of ['apps.agent.read', 'apps.agent.cancel', 'apps.agent.result']) {
      const reply = await invoke(f.service, f.owner, op, { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, jobId });
      assert.equal(reply.error?.code, 'job_not_found');
    }
  }
  assert.equal(f.store.state.jobs[0].cancelRequested, false);
  assert.equal(f.store.state.jobs[0].status, 'queued');
  assert.equal((await f.store.poll(f.auth, 0)).jobs[0].id, jobId);
  assert.equal((await f.store.finishJob(f.auth, jobId, { ok: true, text: 'Only the owner may read this result', exitCode: 0 })).ok, true);
  for (const op of ['apps.agent.read', 'apps.agent.cancel', 'apps.agent.result']) {
    assert.equal((await invoke(f.service, f.stranger, op, target(f, jobId))).error?.code, 'apps_device_not_owned');
  }
  assert.equal((await f.store.getEvents(f.host.linkId, jobId)).ok, false);
  assert.equal((await f.store.getResultPage(f.host.linkId, jobId)).ok, false);

  const queued = await invoke(f.service, f.owner, 'apps.agent.create', { ...f.createArgs, requestId: 'cancel-after-owner-revocation' });
  await f.store.writeQueue;
  let release;
  f.store.writeQueue = new Promise(resolveQueue => { release = resolveQueue; });
  const pending = [];
  for (const [op, args] of [
    ['apps.agent.read', target(f, jobId)],
    ['apps.agent.result', target(f, jobId)],
    ['apps.agent.cancel', target(f, queued.job.id)],
    ['apps.agent.create', f.createArgs],
  ]) pending.push(f.service.handle(await proof(f.service, f.owner, op, args)));
  f.disable(); release();
  for (const response of await Promise.all(pending)) {
    assert.equal(response.error?.code, 'authentication_required');
    assert.equal(Object.hasOwn(response, 'text'), false);
    assert.equal(Object.hasOwn(response, 'job'), false);
    assert.equal(Object.hasOwn(response, 'events'), false);
  }
  assert.equal(f.store.state.jobs.length, 2);
  assert.equal(f.store.state.jobs.find(job => job.id === queued.job.id).cancelRequested, false);
  assert.equal(f.store.state.jobs.find(job => job.id === queued.job.id).status, 'queued');
  assert.equal((await invoke(f.service, f.owner, 'apps.agent.result', target(f, jobId))).error?.code, 'apps_device_not_owned');
});

test('unavailable inference prevents new jobs while existing results and cancellation remain accessible', async t => {
  const f = await setup(t);
  f.setInferenceReady(false);
  assert.equal((await invoke(f.service, f.owner, 'apps.agent.create', f.createArgs)).error?.code, 'app_model_unavailable');
  assert.equal(f.store.state.jobs.length, 0);
  f.setInferenceReady(true);
  const created = await invoke(f.service, f.owner, 'apps.agent.create', f.createArgs);
  const jobId = created.job.id;
  assert.equal((await f.store.poll(f.auth, 0)).jobs[0].id, jobId);
  assert.equal((await f.store.finishJob(f.auth, jobId, { ok: true, text: 'Created before inference went offline', exitCode: 0 })).ok, true);
  const pending = await invoke(f.service, f.owner, 'apps.agent.create', { ...f.createArgs, requestId: 'queued-before-inference-offline' });
  f.setInferenceReady(false);
  const read = boundedReply(await invoke(f.service, f.owner, 'apps.agent.read', target(f, jobId)));
  assert.equal(read.done, true);
  assert.equal((await invoke(f.service, f.owner, 'apps.agent.result', target(f, jobId))).text, 'Created before inference went offline');
  assert.equal((await invoke(f.service, f.owner, 'apps.agent.cancel', target(f, pending.job.id))).job.status, 'cancelled');
  assert.equal((await invoke(f.service, f.owner, 'apps.agent.create', { ...f.createArgs, requestId: 'must-not-queue' })).error?.code, 'app_model_unavailable');
  assert.equal(f.store.state.jobs.length, 2);
});
