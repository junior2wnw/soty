import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, generateKeyPairSync, randomBytes, randomUUID, sign } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { createHttpApp } from '../http-app.js';
import { digestArgs } from '../../modules/connect/server/index.mjs';
import { captureAccountAiConfiguration, ROOT_AI_ISSUER, GATEWAY_AI_ISSUER, GATEWAY_AI_AUDIENCE, ROOT_AI_AUDIENCE } from '../account-ai-configuration.js';
import { createBridgeSigner, createBridgeVerifier } from '../../services/ai-gateway/packages/soty-bridge/index.mjs';
import { createLocalAppsRuntime } from '../../scripts/agent-modules/local-apps.mjs';
import { createSignedConnectHttpClient } from './support/signed-connect-client.mjs';

const MODEL = 'zai-org/GLM-5.3-Flash';
const good = value => { assert.equal(value.ok, true, value.error?.code || value.error); return value; };
const identity = () => { const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }); return { key: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }) }; };
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { resolve, promise }; };
const body = { model: MODEL, messages: [{ role: 'user', content: 'Synthetic route test' }], stream: false };

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-account-ai-http-')), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Account AI transport fixture</title>');
  const rootKeys = generateKeyPairSync('ed25519'), gatewayKeys = generateKeyPairSync('ed25519'), nonce = new Set(), accounts = new Map();
  const verify = createBridgeVerifier({ audience: GATEWAY_AI_AUDIENCE, issuers: [{ issuer: ROOT_AI_ISSUER, kid: 'fixture-root', publicKey: rootKeys.publicKey }], claimNonce(proof) { if (nonce.has(proof.jti)) return false; nonce.add(proof.jti); return true; } });
  const signer = createBridgeSigner({ issuer: GATEWAY_AI_ISSUER, audience: ROOT_AI_AUDIENCE, kid: 'fixture-gateway', privateKey: gatewayKeys.privateKey });
  const state = { dispatches: [], callbacks: [], bypass: 0, hold: null, origins: [], receipts: new Map() }, sockets = new Set(), runtimes = [];
  let origin, app;
  const gateway = createServer(async (req, res) => {
    try {
      const chunks = []; for await (const chunk of req) chunks.push(chunk); const encoded = Buffer.concat(chunks).toString('utf8');
      const proof = await verify.verify({ method: req.method, path: req.url, body: encoded, headers: req.headers });
      res.setHeader('content-type', 'application/json');
      if (req.url.endsWith('/accounts/ensure') || req.url.endsWith('/account')) {
        if (!accounts.has(proof.accountId)) accounts.set(proof.accountId, { schema: 'soty.ai-account.v1', accountId: proof.accountId, state: 'ready',
          defaultKey: { id: randomUUID(), name: 'Internal', status: 'active', createdAt: new Date().toISOString() },
          allowance: { kind: 'included', unit: 'microrubles', grantedMicrorubles: '50000000', availableMicrorubles: '50000000', heldMicrorubles: '0', spentMicrorubles: '0', pendingMicrorubles: '0', limits: { dailyMicrorubles: '5000000' } },
          usage: { requests: 0, tokens: 0, inputTokens: 0, outputTokens: 0 }, models: [{ id: MODEL, status: 'healthy' }], updatedAt: new Date().toISOString() });
        res.end(JSON.stringify(accounts.get(proof.accountId))); return;
      }
      if (req.url.startsWith('/internal/soty/v1/requests/')) {
        const receipt = state.receipts.get(proof.accountId + ':' + req.url.split('/').at(-1));
        res.writeHead(receipt ? 200 : 404).end(JSON.stringify(receipt ? { request: receipt } : {})); return;
      }
      if (req.url.endsWith('/chat/completions')) {
        const input = JSON.parse(encoded), requestId = randomUUID();
        if (state.hold) { const gate = state.hold; state.hold = null; gate.entered.resolve(); await gate.release.promise; }
        const callback = JSON.stringify({ issuer: ROOT_AI_ISSUER, accountId: proof.accountId, deviceId: proof.deviceId, authorizationId: proof.authorizationId,
          requestId: proof.requestId, gatewayRequestId: requestId, bodySha256: createHash('sha256').update(encoded).digest('hex'), providerId: 'synthetic', model: input.model });
        const headers = signer.headers({ method: 'POST', path: '/internal/soty-ai/v1/authorize-dispatch', body: callback, operation: 'dispatch.authorize',
          accountId: proof.accountId, deviceId: proof.deviceId, requestId: proof.requestId, authorizationId: proof.authorizationId });
        const decision = await fetch(origin + '/internal/soty-ai/v1/authorize-dispatch', { method: 'POST', headers: { ...headers, 'content-type': 'application/json' }, body: callback });
        state.callbacks.push({ proof, status: decision.status });
        if (decision.status !== 200) { res.writeHead(403).end('{"error":{"code":"authority_revoked"}}'); return; }
        state.dispatches.push({ proof, body: input });
        res.setHeader('x-soty-request-id', requestId);
        if (Array.isArray(input.tools) && input.tools.some(tool => tool.function?.name === 'finish_answer')) {
          state.receipts.set(proof.accountId + ':' + requestId, { id: requestId, logicalRequestId: proof.requestId, status: 'completed',
            model_requested: MODEL, model_resolved: MODEL, input_tokens: '3', output_tokens: '2', charge_urub: '7' });
          res.end(JSON.stringify({ id: 'synthetic-native-completion', model: MODEL,
            choices: [{ index: 0, message: { role: 'assistant', tool_calls: [{ id: 'native-final', type: 'function', function: {
              name: 'finish_answer', arguments: JSON.stringify({ answer: 'Подтверждённый личный ответ', selectedEvidenceIds: [], claims: [] }) } }] }, finish_reason: 'tool_calls' }],
            usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } })); return;
        }
        if (input.stream) { res.setHeader('content-type', 'text/event-stream'); res.write('data: {"choices":[{"delta":{"content":"сота"}}]}\n\n'); res.end('data: [DONE]\n\n'); }
        else res.end(JSON.stringify({ model: MODEL, choices: [{ message: { role: 'assistant', content: 'Synthetic account response' } }], usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 } }));
        return;
      }
      res.writeHead(404).end('{}');
    } catch { if (!res.destroyed && !res.headersSent) res.writeHead(403).end('{}'); }
  });
  gateway.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done)); const gatewayOrigin = `http://127.0.0.1:${gateway.address().port}`;
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!app?.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done)); origin = `http://127.0.0.1:${server.address().port}`;
  const sponsor = 'svc_' + 'S'.repeat(32), applicationToken = 'synthetic_application_token_'.padEnd(48, 'a');
  const configuration = captureAccountAiConfiguration({ schema: 'soty.account-ai-installed.v1', issuer: ROOT_AI_ISSUER, gatewayOrigin,
    signing: { kid: 'fixture-root', privateKey: rootKeys.privateKey.export({ format: 'pem', type: 'pkcs8' }) },
    gatewaySigner: { issuer: GATEWAY_AI_ISSUER, kid: 'fixture-gateway', publicKey: gatewayKeys.publicKey.export({ format: 'pem', type: 'spki' }) },
    applications: [{ appId: 'synthetic_app', accountId: sponsor, models: [MODEL] }],
    legacyConnectors: [{ connectorId: 'sponsored-connector', accountId: sponsor, models: [MODEL] }] }, { allowLoopbackFixture: true });
  app = createHttpApp(dist, { env: {}, dataDir: join(directory, 'data'), connectOrigins: [origin], accountAi: configuration,
    universalAppsEnabled: false, personalAgent: { enabled: false }, localExecutorCurrent: false,
    appOriginTemplate: `http://{appId}.localhost:${server.address().port}`, appHosting: { domainProfile: 'separate-site', namedAppZone: '', discoveryOrigin: '' },
    gonka: { baseUrl: 'https://must-not-call.invalid/v1', apiKey: 'synthetic-legacy-provider-key', applicationTokens: JSON.stringify({ synthetic_app: applicationToken }),
      applicationTokensFile: '', fetchImpl: async () => { state.bypass++; throw Error('provider bypass forbidden'); } } });
  t.after(async () => { state.hold?.release.resolve(); runtimes.forEach(runtime => runtime.stop()); await app.locals.closeServices(); sockets.forEach(socket => socket.destroy());
    await Promise.all([new Promise(done => server.close(done)), new Promise(done => gateway.close(done))]);
    const target = await realpath(directory); assert.equal(dirname(target), await realpath(tmpdir())); assert.match(target, /soty-account-ai-http-/u); await rm(target, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });
  async function call(actor, op, args = {}) {
    const post = value => fetch(origin + '/api/connect/rpc', { method: 'POST', headers: { origin, 'content-type': 'application/json' }, body: JSON.stringify({ protocol: 1, ...value }) }).then(response => response.json());
    const challenge = good(await post({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return post({ op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk, signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const alice = identity(), a = good(await call(alice, 'bootstrap', { label: 'A', encryptionPublicJwk: alice.publicJwk }));
  const bob = identity(), b = good(await call(bob, 'bootstrap', { label: 'B', encryptionPublicJwk: bob.publicJwk }));
  const store = app.locals.connectorService.store;
  async function device({ connectorId = randomUUID(), claim = false } = {}) {
    const token = randomBytes(32).toString('base64url'), binding = { linkId: randomBytes(32).toString('base64url'), hostDeviceId: 'fixture-host-' + randomUUID(), connectorId };
    good(await fetch(origin + '/api/connectors/register', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' },
      body: JSON.stringify({ linkId: binding.linkId, deviceId: binding.hostDeviceId, connectorId, capabilities: ['agent', 'command', 'script'], agent: { id: 'opencode', provider: 'gonka', available: true } }) }).then(response => response.json()));
    if (claim) {
      const runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'), createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request,
        encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') }, { identity: binding, token, serverUrl: origin });
      runtimes.push(runtime); runtime.start(); const deadline = Date.now() + 5000;
      while (!runtime.status().connected) { assert.ok(Date.now() < deadline); await new Promise(done => setTimeout(done, 10)); }
      const proof = await runtime.claim(); good(await call(alice, 'apps.claim', { hostDeviceId: proof.hostDeviceId, connectorId: proof.connectorId, claimCode: proof.claimCode }));
    }
    return { token, binding, auth: { linkId: binding.linkId, deviceId: binding.hostDeviceId, connectorId, token }, target: { hostDeviceId: binding.hostDeviceId, connectorId } };
  }
  const model = (token, input = body, application = false) => fetch(origin + (application ? '/api/inference/v1' : '/api/connectors/gonka/v1') + '/chat/completions', {
    method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' }, body: JSON.stringify(input) });
  return { origin, app, state, accounts, sponsor, applicationToken, alice, bob, a, b, call, device, model, store,
    hold() { const gate = { entered: deferred(), release: deferred() }; state.hold = gate; return gate; } };
}

test('actual Root composition automatically provisions signed accounts and enables native mode without old allowlist', async t => {
  const f = await fixture(t); let actor; const original = f.app.locals.connectService.withActorAuthorityFence;
  f.app.locals.connectService.withActorAuthorityFence = (value, action) => { actor = value; return original(value, action); };
  const a = good(await f.call(f.alice, 'ai.account.get', { expectedAccountId: f.a.accountId }));
  const b = good(await f.call(f.bob, 'ai.account.get', { expectedAccountId: f.b.accountId }));
  assert.equal(a.state, 'ready'); assert.equal(b.state, 'ready'); assert.notEqual(a.defaultKey.id, b.defaultKey.id);
  assert.equal(f.app.locals.personalAgent.status().configured, true); assert.equal(f.app.locals.personalAgent.modelEligible(actor), true);
  assert.equal(f.state.bypass, 0);
  const unsigned = await fetch(f.origin + '/internal/soty-ai/v1/authorize-dispatch', { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' });
  assert.equal(unsigned.status, 403); assert.deepEqual(await unsigned.json(), { authorized: false });
});

test('real static application route preserves JSON/SSE and bills explicit service sponsor through final signed callback', async t => {
  const f = await fixture(t);
  const json = await f.model(f.applicationToken, body, true); assert.equal(json.status, 200); assert.equal((await json.json()).choices[0].message.content, 'Synthetic account response');
  const stream = await f.model(f.applicationToken, { ...body, stream: true }, true); assert.equal(stream.status, 200); assert.match(stream.headers.get('content-type'), /text\/event-stream/u);
  assert.equal(await stream.text(), 'data: {"choices":[{"delta":{"content":"сота"}}]}\n\ndata: [DONE]\n\n');
  assert.equal(f.state.dispatches.length, 2);
  for (const entry of f.state.dispatches) { assert.equal(entry.proof.accountId, f.sponsor); assert.equal(entry.proof.deviceId, 'service:application:synthetic_app'); }
  assert.equal(f.state.bypass, 0);
});

test('actual native agent.turn uses exact GLM bridge and settled owner receipt before user answer/history; second device shares account', async t => {
  const f = await fixture(t), one = createSignedConnectHttpClient({ origin: f.origin }), second = createSignedConnectHttpClient({ origin: f.origin });
  const account = await one.bootstrap('Native account'), start = await second.enrollmentStart('Second device');
  await one.call('enrollment.approve', { requestId: start.requestId, wrappedKey: { schema: 'fixture.encrypted', ciphertext: 'opaque-fixture' } });
  await second.call('enrollment.finish', { requestId: start.requestId, expectedAccountId: account.accountId });
  const before = await one.call('ai.account.get', { expectedAccountId: account.accountId });
  const answer = await one.call('agent.turn', { requestId: 'native-user-turn', prompt: 'Составь короткую фразу.' });
  assert.equal(answer.status, 'complete'); assert.equal(answer.answer, 'Подтверждённый личный ответ');
  const native = f.state.dispatches.at(-1); assert.equal(native.proof.accountId, account.accountId); assert.equal(native.body.model, MODEL); assert.equal(native.body.autoFallback, false);
  assert.equal(f.state.receipts.size, 1);
  const after = await second.call('ai.account.get', { expectedAccountId: account.accountId }); assert.equal(before.defaultKey.id, after.defaultKey.id);
  const notes = await second.call('notes.list', { expectedAccountId: account.accountId }); assert.equal(notes.notes.length, 1);
  const foreign = good(await f.call(f.bob, 'notes.list', { expectedAccountId: f.b.accountId })); assert.equal(foreign.notes.length, 0);
  assert.equal(f.state.bypass, 0);
});

test('unowned device fails closed while explicit legacy sponsor and owned leased job use distinct validated billing identities', async t => {
  const f = await fixture(t), unknown = await f.device();
  const denied = await f.model(unknown.token, { ...body, accountId: f.a.accountId }); assert.equal(denied.status, 403); await denied.text(); assert.equal(f.state.dispatches.length, 0);
  const sponsored = await f.device({ connectorId: 'sponsored-connector' });
  const sponsor = await f.model(sponsored.token); assert.equal(sponsor.status, 200); await sponsor.text(); assert.equal(f.state.dispatches.at(-1).proof.accountId, f.sponsor);
  const owned = await f.device({ claim: true });
  const queued = good(await f.call(f.alice, 'apps.assistant.send', { expectedAccountId: f.a.accountId, ...owned.target, requestId: 'owned-model-request', conversationId: 'owned-conversation', text: 'Synthetic task', cwd: '' }));
  const beforeLease = await f.model(owned.token); assert.equal(beforeLease.status, 403); await beforeLease.text();
  const lease = good(await f.store.poll(owned.auth, 0)); assert.equal(lease.jobs.length, 1);
  const result = await f.model(owned.token, { ...body, accountId: f.b.accountId }); assert.equal(result.status, 200); await result.text();
  assert.equal(f.state.dispatches.at(-1).proof.accountId, f.a.accountId); assert.equal(f.state.dispatches.at(-1).proof.deviceId, f.a.deviceId);
  assert.equal(f.state.bypass, 0);
});

test('real queued connector cancellation invalidates final callback before any upstream work', async t => {
  const f = await fixture(t), owned = await f.device({ claim: true });
  good(await f.call(f.alice, 'apps.assistant.send', { expectedAccountId: f.a.accountId, ...owned.target, requestId: 'queued-cancel-request', conversationId: 'queued-conversation', text: 'Synthetic task', cwd: '' }));
  const lease = good(await f.store.poll(owned.auth, 0)).jobs[0], gate = f.hold(), pending = f.model(owned.token); await gate.entered.promise;
  good(await f.call(f.alice, 'apps.agent.cancel', { expectedAccountId: f.a.accountId, ...owned.target, jobId: lease.id })); gate.release.resolve();
  const response = await pending; assert.equal(response.status, 403); await response.text(); assert.equal(f.state.dispatches.length, 0);
  assert.equal(f.state.bypass, 0);
});
