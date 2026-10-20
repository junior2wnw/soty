import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = fileURLToPath(new URL('../../', import.meta.url));
const [{ createHttpApp }, { digestArgs }, { createLocalAppsRuntime }] = await Promise.all([
  import(pathToFileURL(join(root, 'server/http-app.js')).href), import(pathToFileURL(join(root, 'modules/connect/server/index.mjs')).href),
  import(pathToFileURL(join(root, 'scripts/agent-modules/local-apps.mjs')).href),
]);
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const identity = () => { const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: pair.publicKey.export({ format: 'jwk' }) }; };
const good = value => { assert.equal(value.ok, true, value.error?.code || value.error); return value; };
const flush = () => new Promise(resolve => setImmediate(resolve));
async function fixture(t, { current = true } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-model-owner-auth-')), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Owned model authorization fixture</title>');
  const connections = new Set(), runtimes = [], hits = [], holds = [];
  const applicationToken = 'synthetic_application_only_'.padEnd(48, 'a'); let held = null, app;
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { connections.add(socket); socket.once('close', () => connections.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!app?.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done)); const origin = `http://127.0.0.1:${server.address().port}`;
  app = createHttpApp(dist, { env: {}, dataDir: join(directory, 'data'), connectOrigins: [origin],
    appOriginTemplate: `http://{appId}.localhost:${server.address().port}`, appHosting: { domainProfile: 'separate-site', namedAppZone: '', discoveryOrigin: '' },
    universalAppsEnabled: false, personalAgent: { enabled: false }, localExecutorCurrent: current,
    gonka: { baseUrl: 'https://synthetic-owned-model.invalid/v1', apiKey: 'synthetic-no-real-upstream-key',
      model: 'zai-org/GLM-5.3-Flash', upstreamModel: 'zai-org/GLM-5.3-Flash', fallbackBaseUrl: '', fallbackApiKey: '', fallbackModels: 'none',
      applicationTokens: JSON.stringify({ synthetic_app: applicationToken }), applicationTokensFile: '', providerStrategy: 'fallback', providerConcurrency: 1,
      internalStream: false, fetchImpl: async (url, init) => {
        assert.equal(new URL(url).origin, 'https://synthetic-owned-model.invalid'); const body = JSON.parse(init.body); hits.push({ body, signal: init.signal });
        if (held) { const gate = held; held = null; gate.entered.resolve(); await gate.release.promise; }
        return Response.json({ id: 'synthetic_completion', model: body.model, choices: [{ index: 0, message: { role: 'assistant', content: 'synthetic_owned_answer' }, finish_reason: 'stop' }], usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 } });
      } } });
  t.after(async () => { holds.forEach(gate => gate.release.resolve()); runtimes.forEach(runtime => runtime.stop());
    connections.forEach(socket => socket.destroy()); await app.locals.closeServices(); await new Promise(done => server.close(done));
    const absolute = await realpath(directory); assert.equal(dirname(absolute), await realpath(tmpdir())); assert.match(absolute, /soty-model-owner-auth-/);
    await rm(absolute, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });
  async function call(actor, op, args = {}) {
    const post = value => fetch(origin + '/api/connect/rpc', { method: 'POST', headers: { origin, 'content-type': 'application/json' }, body: JSON.stringify({ protocol: 1, ...value }) }).then(response => response.json());
    const challenge = good(await post({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return post({ op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const alice = identity(), bob = identity();
  const a = good(await call(alice, 'bootstrap', { label: 'Model owner', encryptionPublicJwk: alice.encryptionPublicJwk }));
  const b = good(await call(bob, 'bootstrap', { label: 'Other model owner', encryptionPublicJwk: bob.encryptionPublicJwk }));
  const store = app.locals.connectorService.store;
  assert.equal(app.locals.connectorService.modelProxy.ready, true, 'synthetic model proxy fixture must be configured before ownership tests');
  let sequence = 0;
  async function device({ claim = true, actor = alice, token = randomBytes(32).toString('base64url') } = {}) {
    const n = ++sequence, binding = { linkId: ('model_owner_link_' + n + '_').padEnd(40, 'x'), hostDeviceId: 'model_owner_host_' + n,
      connectorId: 'model_owner_connector_' + n, name: 'Synthetic owned model device' };
    const headers = { 'content-type': 'application/json', authorization: 'Bearer ' + token };
    good(await fetch(origin + '/api/connectors/register', { method: 'POST', headers,
      body: JSON.stringify({ linkId: binding.linkId, deviceId: binding.hostDeviceId, connectorId: binding.connectorId, scope: 'Dev',
        capabilities: ['agent', 'command', 'script'], agent: { id: 'opencode', provider: 'gonka', available: true } }) }).then(response => response.json()));
    if (claim) {
      const runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
        createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request,
        encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') }, { identity: binding, token, serverUrl: origin });
      runtimes.push(runtime); runtime.start(); const deadline = Date.now() + 5000;
      while (!runtime.status().connected) { assert.ok(Date.now() < deadline); await new Promise(done => setTimeout(done, 10)); }
      const proof = await runtime.claim(); good(await call(actor, 'apps.claim', { hostDeviceId: proof.hostDeviceId, connectorId: proof.connectorId, claimCode: proof.claimCode }));
    }
    return { binding, token, auth: { linkId: binding.linkId, deviceId: binding.hostDeviceId, connectorId: binding.connectorId, token },
      target: { hostDeviceId: binding.hostDeviceId, connectorId: binding.connectorId } };
  }
  async function enqueue(source, actor = alice) {
    const account = actor === alice ? a : b;
    const value = await call(actor, 'apps.assistant.send', { expectedAccountId: account.accountId, ...source.target,
      requestId: 'model_owner_request_' + (++sequence), conversationId: 'model_conversation_' + sequence, text: 'Synthetic local task', cwd: '' });
    return value;
  }
  const model = (token, { application = false, label = 'synthetic_model_question' } = {}) => fetch(origin + (application ? '/api/inference/v1' : '/api/connectors/gonka/v1') + '/chat/completions', {
    method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'zai-org/GLM-5.3-Flash', stream: false, messages: [{ role: 'user', content: label }] }) });
  const cancel = (source, jobId) => call(alice, 'apps.agent.cancel', { expectedAccountId: a.accountId, ...source.target, jobId });
  return { origin, app, store, alice, bob, a, b, hits, call, device, enqueue, model, cancel, applicationToken,
    hold() { const gate = { entered: deferred(), release: deferred() }; held = gate; holds.push(gate); return gate; },
    async lease(source) { const value = good(await store.poll(source.auth, 0)); assert.equal(value.jobs.length, 1); assert.equal(value.jobs[0].schema, 'soty.connector-job.v4'); return value.jobs[0]; },
    async phone() { const actor = identity(), started = good(await call(actor, 'enrollment.start', { label: 'Other signed owner device', encryptionPublicJwk: actor.encryptionPublicJwk }));
      good(await call(alice, 'enrollment.approve', { requestId: started.requestId, wrappedKey: { schema: 'synthetic.encrypted', ciphertext: 'opaque' } }));
      good(await call(actor, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: a.accountId })); return actor; },
  };
}

test('registered protocol2 bearer without an account-owned active task cannot call the model proxy', async t => {
  const f = await fixture(t), source = await f.device({ claim: false });
  const authorized = await f.store.authenticateModelToken(source.token);
  const result = await f.model(source.token), body = await result.text(); assert.equal(result.status, 401, body); assert.equal(authorized, false); assert.equal(f.hits.length, 0);
});
test('claim and queued v4 task alone do not authorize inference before actual lease', async t => {
  const f = await fixture(t), source = await f.device(); good(await f.enqueue(source));
  const authorized = await f.store.authenticateModelToken(source.token);
  const result = await f.model(source.token), body = await result.text(); assert.equal(result.status, 401, body); assert.equal(authorized, false); assert.equal(f.hits.length, 0);
});
test('actual signed current owner plus exact assigned v4 lease permits one completion and terminal task retires it', async t => {
  const f = await fixture(t), source = await f.device(); good(await f.enqueue(source)); const job = await f.lease(source);
  assert.equal(await f.store.authenticateModelToken(source.token), true);
  const result = await f.model(source.token); assert.equal(result.status, 200); assert.match(await result.text(), /synthetic_owned_answer/); assert.equal(f.hits.length, 1);
  good(await f.store.finishJob(source.auth, job.id, { ok: true, text: 'Synthetic completed', exitCode: 0 }));
  assert.equal(await f.store.authenticateModelToken(source.token), false);
});
test('foreign signed account cannot manufacture the target owner admission', async t => {
  const f = await fixture(t), source = await f.device(); const foreign = await f.enqueue(source, f.bob); assert.equal(foreign.ok, false);
  const result = await f.model(source.token); assert.equal(result.status, 401); await result.text(); assert.equal(f.hits.length, 0);
});
test('real Connect revoke after lease denies a new provider POST through the existing current-owner linkage', async t => {
  const f = await fixture(t), source = await f.device(); good(await f.enqueue(source)); await f.lease(source);
  const phone = await f.phone(); good(await f.call(phone, 'device.revoke', { deviceId: f.a.deviceId }));
  const result = await f.model(source.token); assert.equal(result.status, 401); await result.text(); assert.equal(f.hits.length, 0);
});
test('signed cancel invalidates an already active proxy request and original capability never retargets the next job', async t => {
  const f = await fixture(t), source = await f.device(); good(await f.enqueue(source)); const job = await f.lease(source);
  const lease = await f.store.captureModelAuthorization?.(source.token), gate = f.hold(), pending = f.model(source.token); await gate.entered.promise;
  good(await f.cancel(source, job.id)); const abortedBeforeLateAnswer = f.hits[0].signal.aborted; gate.release.resolve();
  const result = await pending; const body = await result.text(); assert.equal(result.status, 403); assert.equal(abortedBeforeLateAnswer, true);
  assert.doesNotMatch(body, /synthetic_owned_answer/);
  good(await f.store.finishJob(source.auth, job.id, { ok: false, text: 'Cancelled', exitCode: 130 }));
  good(await f.enqueue(source)); await f.lease(source); assert.equal(lease.assertCurrent(), false);
  assert.equal(await f.store.authenticateModelToken(source.token), true);
});
test('queued provider admission rechecks real authority even if invalidation notification delivery is lost', async t => {
  const f = await fixture(t), source = await f.device(); good(await f.enqueue(source)); const job = await f.lease(source);
  const original = (f.store.captureModelAuthorization || f.store.authenticateModelToken).bind(f.store);
  // Fault injection drops only notifications. The real captured owner/job
  // assertCurrent remains unchanged, so this tests the queue/fetch boundary.
  f.store.captureModelAuthorization = async token => { const value = await original(token); return value && typeof value === 'object'
    ? { ...value, subscribe: () => () => {} } : value; };
  const gate = f.hold(), first = f.model(source.token); await gate.entered.promise;
  const second = f.model(source.token); const deadline = Date.now() + 3000;
  while (!f.app.locals.connectorService.modelProxy.providers.some(row => row.queued > 0)) {
    assert.ok(Date.now() < deadline); await flush();
  }
  good(await f.cancel(source, job.id)); gate.release.resolve();
  for (const result of await Promise.all([first, second])) { assert.equal(result.status, 403); assert.doesNotMatch(await result.text(), /synthetic_owned_answer/); }
  assert.equal(f.hits.length, 1, 'cancelled queued request must never reach fetchImpl');
});
test('existing Connect revoke notification aborts an active request before a late provider answer can be displayed', async t => {
  const f = await fixture(t), source = await f.device(); good(await f.enqueue(source)); await f.lease(source); const phone = await f.phone();
  const gate = f.hold(), pending = f.model(source.token); await gate.entered.promise;
  good(await f.call(phone, 'device.revoke', { deviceId: f.a.deviceId }));
  const abortedBeforeLateAnswer = f.hits[0].signal.aborted; gate.release.resolve();
  const result = await pending, body = await result.text(); assert.equal(result.status, 403); assert.equal(abortedBeforeLateAnswer, true);
  assert.doesNotMatch(body, /synthetic_owned_answer/);
});
test('ambiguous connector identity sharing one bearer cannot borrow the other installation active task', async t => {
  const f = await fixture(t), source = await f.device(); good(await f.enqueue(source)); await f.lease(source);
  await f.device({ claim: false, token: source.token });
  const result = await f.model(source.token); assert.equal(result.status, 401); await result.text(); assert.equal(f.hits.length, 0);
});
test('legacy host without current executor opt-in preserves its previous connector model authorization', async t => {
  const f = await fixture(t, { current: false }), source = await f.device({ claim: false });
  assert.equal(await f.store.authenticateModelToken(source.token), true);
  const result = await f.model(source.token); assert.equal(result.status, 200); await result.text(); assert.equal(f.hits.length, 1);
});
test('application inference authentication remains separate and cannot be substituted by connector or body owner fields', async t => {
  const f = await fixture(t), source = await f.device({ claim: false });
  const denied = await f.model(source.token, { application: true }); assert.equal(denied.status, 401); await denied.text();
  const allowed = await f.model(f.applicationToken, { application: true }); assert.equal(allowed.status, 200); await allowed.text();
  const wrong = await f.model(f.applicationToken); assert.equal(wrong.status, 401); await wrong.text(); assert.equal(f.hits.length, 1);
});
