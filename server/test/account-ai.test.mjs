import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomUUID, createHash } from 'node:crypto';
import { mkdtemp, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { signedFixture } from '../../modules/connect/test/support/direct-signed-fixture.mjs';
import { captureAccountAiConfiguration, ROOT_AI_ISSUER, GATEWAY_AI_ISSUER, GATEWAY_AI_AUDIENCE, ROOT_AI_AUDIENCE } from '../account-ai-configuration.js';
import { createAccountAiService } from '../account-ai.js';
import { createPersonalAgentTransport } from '../personal-agent-transport.js';
import { GLM_MODEL } from '../../modules/personal-agent/provider/index.mjs';
import { createBridgeSigner, createBridgeVerifier } from '../../services/ai-gateway/packages/soty-bridge/index.mjs';

const operations = new Set(['ai.account.get', 'ai.keys.list', 'ai.keys.create', 'ai.keys.update', 'ai.keys.rotate', 'ai.keys.revoke']);
const hash = text => createHash('sha256').update(text).digest('hex');
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const safeAccount = accountId => ({ schema: 'soty.ai-account.v1', accountId, state: 'ready',
  defaultKey: { id: randomUUID(), name: 'Internal', status: 'active', createdAt: new Date().toISOString(), secret: 'must-never-leak' },
  allowance: { kind: 'included', unit: 'microrubles', grantedMicrorubles: '50000000', availableMicrorubles: '50000000', heldMicrorubles: '0', spentMicrorubles: '0', pendingMicrorubles: '0', limits: { dailyMicrorubles: '5000000', concurrency: 2 } },
  usage: { requests: 0, tokens: 0, inputTokens: 0, outputTokens: 0, private: 'must-never-leak' },
  models: [{ id: GLM_MODEL, name: 'GLM', status: 'healthy' }], updatedAt: new Date().toISOString() });

async function fixture(t, { databasePath = ':memory:', applications = [], legacyConnectors = [], dispatchLeaseMs = 600000, maxAuthorizations = 256,
  reconcileBatchSize = 100, cacheTtlMs = 60000, clock = Date.now } = {}) {
  let ai; const actors = new Map();
  const f = signedFixture(t, [
    { operations, maxResultBytes: 8 * 1024 * 1024, executeAsync: request => ai.executeAsync(request) },
    { operations: new Set(['fixture.actor']), execute({ actor }) { actors.set(actor.deviceId, actor); return { captured: true }; } },
  ]);
  const rootKeys = generateKeyPairSync('ed25519'), gatewayKeys = generateKeyPairSync('ed25519');
  const config = captureAccountAiConfiguration({ schema: 'soty.account-ai-installed.v1', issuer: ROOT_AI_ISSUER, gatewayOrigin: 'https://fixture-ai.invalid',
    signing: { kid: 'test-root', privateKey: rootKeys.privateKey.export({ format: 'pem', type: 'pkcs8' }) },
    gatewaySigner: { issuer: GATEWAY_AI_ISSUER, kid: 'test-gateway', publicKey: gatewayKeys.publicKey.export({ format: 'pem', type: 'spki' }) }, applications, legacyConnectors });
  const nonces = new Set(), accounts = new Map(), receipts = new Map(), keys = new Map(), mutations = new Map();
  const verifier = createBridgeVerifier({ audience: GATEWAY_AI_AUDIENCE, issuers: [{ issuer: ROOT_AI_ISSUER, kid: 'test-root', publicKey: rootKeys.publicKey }],
    claimNonce(proof) { if (nonces.has(proof.jti)) return false; nonces.add(proof.jti); return true; } });
  const gatewaySigner = createBridgeSigner({ issuer: GATEWAY_AI_ISSUER, audience: ROOT_AI_AUDIENCE, kid: 'test-gateway', privateKey: gatewayKeys.privateKey });
  const state = { calls: [], dispatches: 0, outage: false, maintenance: false, loseMutation: false, onChat: null, onEnsure: null, lastCallback: null, lastIngress: null };
  async function callback(proof, encoded, overrides = {}) {
    const value = { issuer: ROOT_AI_ISSUER, accountId: proof.accountId, deviceId: proof.deviceId, authorizationId: proof.authorizationId,
      requestId: proof.requestId, gatewayRequestId: randomUUID(), bodySha256: hash(encoded), providerId: 'fixture-provider', model: JSON.parse(encoded).model, ...overrides };
    const body = JSON.stringify(value), headers = gatewaySigner.headers({ method: 'POST', path: '/internal/soty-ai/v1/authorize-dispatch', body,
      operation: 'dispatch.authorize', accountId: value.accountId, deviceId: value.deviceId, requestId: value.requestId, authorizationId: value.authorizationId });
    state.lastCallback = { method: 'POST', route: '/internal/soty-ai/v1/authorize-dispatch', body, headers };
    return ai.authorizeDispatch(state.lastCallback);
  }
  async function fetchImpl(url, init) {
    const route = new URL(url).pathname, encoded = init.body ?? '';
    const proof = await verifier.verify({ method: init.method, path: route, body: encoded, headers: init.headers });
    state.calls.push({ route, proof, encoded, signal: init.signal });
    if (state.outage) throw Error('fixture gateway disconnected');
    if (route.endsWith('/accounts/ensure')) {
      await state.onEnsure?.(proof);
      if (!accounts.has(proof.accountId)) accounts.set(proof.accountId, safeAccount(proof.accountId));
      return Response.json(accounts.get(proof.accountId));
    }
    if (route.endsWith('/account')) return Response.json(accounts.get(proof.accountId));
    if (route.includes('/requests/')) {
      const request = receipts.get(proof.accountId + ':' + route.split('/').at(-1));
      return Response.json(request ? { request } : { error: { code: 'not_found' } }, { status: request ? 200 : 404 });
    }
    if (route.endsWith('/chat/completions')) {
      state.lastIngress = { proof, encoded };
      if (state.onChat) return state.onChat(proof, encoded, init.signal);
      await callback(proof, encoded); state.dispatches++;
      const requestId = randomUUID(); receipts.set(proof.accountId + ':' + requestId, { id: requestId, logicalRequestId: proof.requestId,
        status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL, input_tokens: '3', output_tokens: '2', charge_urub: '23' });
      return Response.json({ id: 'completion', model: GLM_MODEL, choices: [{ message: { role: 'assistant', content: 'готово' } }], usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } },
        { headers: { 'x-soty-request-id': requestId } });
    }
    if (route.endsWith('/keys')) return Response.json({ keys: [...keys.values()].filter(key => key.owner === proof.accountId) });
    if (route.endsWith('/keys/create')) {
      const identity = proof.accountId + ':' + proof.requestId, previous = mutations.get(identity);
      if (previous) {
        if (previous.encoded !== encoded) return Response.json({ error: { code: 'idempotency_conflict' } }, { status: 409 });
        return Response.json({ key: previous.key, secret: null, replayed: true });
      }
      const key = { id: randomUUID(), owner: proof.accountId, name: JSON.parse(encoded).name, prefix: 'soty…test', status: 'active', policyRevision: 1, limits: {}, secret: 'must-never-leak' };
      keys.set(key.id, key); mutations.set(identity, { encoded, key });
      if (state.loseMutation) { state.loseMutation = false; throw Error('response lost after key committed'); }
      return Response.json({ key, secret: 'soty_developer_synthetic_once_only', replayed: false });
    }
    if (/\/keys\/(?:rotate|revoke|update)$/u.test(route)) {
      const input = JSON.parse(encoded), key = keys.get(input.keyId), operation = route.split('/').at(-1), identity = proof.accountId + ':' + operation + ':' + proof.requestId;
      if (!key || key.owner !== proof.accountId) return Response.json({ error: { code: 'key_not_found' } }, { status: 404 });
      const previous = mutations.get(identity);
      if (previous) return previous.encoded === encoded ? Response.json({ key: previous.key, secret: null, replayed: true }) : Response.json({ error: { code: 'idempotency_conflict' } }, { status: 409 });
      let result = key;
      if (operation === 'revoke') key.status = 'revoked';
      if (operation === 'update') {
        if (input.expectedPolicyRevision !== undefined && input.expectedPolicyRevision !== key.policyRevision) return Response.json({ error: { code: 'policy_revision_conflict' } }, { status: 409 });
        Object.assign(key, Object.fromEntries(Object.entries(input).filter(([name]) => !['keyId', 'expectedPolicyRevision'].includes(name)))); key.policyRevision++;
      }
      if (operation === 'rotate') { key.status = 'revoked'; result = { ...key, id: randomUUID(), status: 'active' }; keys.set(result.id, result); }
      mutations.set(identity, { encoded, key: result });
      return Response.json({ key: result, ...(operation === 'rotate' ? { secret: 'soty_rotated_synthetic_once' } : {}) });
    }
    return Response.json({ error: { code: 'not_found' } }, { status: 404 });
  }
  const create = () => createAccountAiService({ configuration: config, databasePath, getConnect: () => f.service, fetchImpl, admitNewWork: () => !state.maintenance, dispatchLeaseMs, maxAuthorizations, reconcileMs: 20, reconcileBatchSize, cacheTtlMs, clock });
  ai = create();
  t.after(() => ai.close());
  async function issued(person) { await f.call(person, 'fixture.actor'); return actors.get(person.deviceId); }
  return { ...f, ai: () => ai, issued, state, accounts, keys, receipts, callback,
    async restart() { await ai.close(); ai = create(); return ai; },
  };
}

test('new/existing accounts reconcile independently, bootstrap stays usable during outage and second devices reuse account', async t => {
  const f = await fixture(t); f.state.outage = true; f.ai().start();
  const a = await f.person('A'), b = await f.person('B');
  assert.equal(f.service.hasAccount(a.accountId), true); assert.equal(f.service.hasAccount(b.accountId), true);
  const failed = await f.raw(a, 'ai.account.get', { expectedAccountId: a.accountId }); assert.equal(failed.error.code, 'ai_unavailable');
  f.state.outage = false; await f.ai().reconcile();
  const aStatus = await f.call(a, 'ai.account.get', { expectedAccountId: a.accountId });
  const a2 = await f.enroll(a, 'A2'); const a2Status = await f.call(a2, 'ai.account.get', { expectedAccountId: a.accountId });
  assert.notEqual(aStatus.defaultKey.id, (await f.call(b, 'ai.account.get', { expectedAccountId: b.accountId })).defaultKey.id);
  assert.equal(aStatus.defaultKey.id, a2Status.defaultKey.id); assert.equal(f.accounts.size, 2);
  assert.equal(JSON.stringify(aStatus).includes('must-never-leak'), false);
  const before = f.accounts.get(a.accountId).defaultKey.id; await f.restart(); await f.ai().reconcile();
  assert.equal(f.accounts.size, 2); assert.equal(f.accounts.get(a.accountId).defaultKey.id, before);
});

test('concurrent first-use ensure is single-flight and eligibility is synchronous with no side effects', async t => {
  const f = await fixture(t), a = await f.person('A'), actor = await f.issued(a), gate = deferred(); let count = 0;
  f.state.onEnsure = async () => { count++; await gate.promise; };
  const before = f.state.calls.length;
  assert.equal(f.ai().eligible(actor), true); assert.equal(f.state.calls.length, before);
  const attempts = [f.ai().ensure(a.accountId), f.ai().ensure(a.accountId), f.ai().ensure(a.accountId)];
  await new Promise(resolve => setImmediate(resolve)); assert.equal(count, 1); gate.resolve();
  const results = await Promise.all(attempts); assert.equal(new Set(results.map(account => account.defaultKey.id)).size, 1);
});

test('background account scan advances a bounded cursor and skips freshly projected accounts', async t => {
  const f = await fixture(t, { reconcileBatchSize: 2 }); for (let index = 0; index < 5; index++) await f.person('Account ' + index);
  await f.ai().reconcile(); assert.equal(f.accounts.size, 2);
  await f.ai().reconcile(); assert.equal(f.accounts.size, 4);
  await f.ai().reconcile(); assert.equal(f.accounts.size, 5);
  const count = f.state.calls.length; await f.ai().reconcile(); assert.equal(f.state.calls.length, count);
});

test('stale limited projections cannot permanently block automatic first-use after a budget reset', async t => {
  let now = Date.now(); const f = await fixture(t, { cacheTtlMs: 50, clock: () => now }), a = await f.person('A'), actor = await f.issued(a);
  await f.ai().ensure(a.accountId); f.accounts.get(a.accountId).state = 'limited'; await f.call(a, 'ai.account.get', { expectedAccountId: a.accountId });
  assert.equal(f.ai().eligible(actor), false); const before = f.state.calls.length; now += 51;
  assert.equal(f.ai().eligible(actor), true); assert.equal(f.state.calls.length, before);
  f.accounts.get(a.accountId).state = 'ready'; const response = await f.ai().capture({ actor }).complete({ requestId: 'after-reset', body: { model: GLM_MODEL }, signal: new AbortController().signal });
  assert.equal(response.status, 200); await response.json();
});

test('account status accepts the agreed 300-model slim catalog above the old byte/count limits', async t => {
  const f = await fixture(t), a = await f.person('A'); await f.ai().ensure(a.accountId);
  f.accounts.get(a.accountId).models = Array.from({ length: 300 }, (_, index) => ({ id: 'model-' + index + 'x'.repeat(220), name: 'М'.repeat(160), status: 'healthy',
    inputPriceMicrorublesPerMillion: '23000000', outputPriceMicrorublesPerMillion: '23000000', diagnostic: 'must-never-leak' }));
  const account = await f.call(a, 'ai.account.get', { expectedAccountId: a.accountId }); assert.equal(account.models.length, 300);
  assert.equal(account.models[0].inputPriceMicrorublesPerMillion, '23000000'); assert.equal(JSON.stringify(account).includes('diagnostic'), false);
});

test('only issued current actors select billing; stale expected account and caller identity overrides fail before network', async t => {
  const f = await fixture(t), a = await f.person('A'), b = await f.person('B'), actor = await f.issued(a);
  assert.throws(() => f.ai().capture({ actor: { ...actor } }), error => error.code === 'ai_context_changed');
  const before = f.state.calls.length;
  const stale = await f.raw(a, 'ai.keys.create', { expectedAccountId: b.accountId, operationId: 'stable-create', name: 'test' });
  assert.equal(stale.error.code, 'ai_context_changed');
  const override = await f.raw(a, 'ai.account.get', { expectedAccountId: a.accountId, accountId: b.accountId });
  assert.equal(override.error.code, 'ai_invalid_request'); assert.equal(f.state.calls.length, before);
});

test('lost developer-key mutation preserves stable operation identity and never returns a secret on replay', async t => {
  const f = await fixture(t), a = await f.person('A');
  const args = { expectedAccountId: a.accountId, operationId: 'stable-one-key', name: 'First' }; f.state.loseMutation = true;
  const lost = await f.raw(a, 'ai.keys.create', args); assert.equal(lost.error.code, 'ai_operation_unknown');
  const listed = await f.call(a, 'ai.keys.list', { expectedAccountId: a.accountId }); assert.equal(listed.keys.length, 1);
  assert.equal(JSON.stringify(listed).includes('must-never-leak'), false);
  const replay = await f.call(a, 'ai.keys.create', args); assert.equal(replay.replayed, true); assert.equal(replay.secret, null); assert.equal(f.keys.size, 1);
  const conflict = await f.raw(a, 'ai.keys.create', { ...args, name: 'Different intent' }); assert.equal(conflict.error.code, 'ai_request_conflict'); assert.equal(f.keys.size, 1);
  const fresh = await f.call(a, 'ai.keys.create', { ...args, operationId: 'separate-key' }); assert.equal(fresh.secret, 'soty_developer_synthetic_once_only');
  assert.equal(Object.hasOwn(fresh.key, 'secret'), false);
});

test('owner key update/rotate/revoke use exact operation receipts and cannot mutate a foreign account', async t => {
  const f = await fixture(t), a = await f.person('A'), b = await f.person('B');
  const created = await f.call(a, 'ai.keys.create', { expectedAccountId: a.accountId, operationId: 'key-create', name: 'First' });
  const update = await f.call(a, 'ai.keys.update', { expectedAccountId: a.accountId, operationId: 'key-update', keyId: created.key.id,
    name: 'Updated', status: 'paused', limits: { dailyMicrorubles: '100000' }, expiresAt: '2027-01-01T00:00:00.000Z', allowedModels: [GLM_MODEL], expectedPolicyRevision: 1 });
  assert.equal(update.key.status, 'paused'); assert.equal(update.key.limits.dailyMicrorubles, '100000');
  const stale = await f.raw(a, 'ai.keys.update', { expectedAccountId: a.accountId, operationId: 'stale-key-update', keyId: created.key.id, status: 'active', expectedPolicyRevision: 1 });
  assert.equal(stale.error.code, 'ai_key_changed'); assert.equal(f.keys.get(created.key.id).status, 'paused');
  const foreign = await f.raw(b, 'ai.keys.revoke', { expectedAccountId: b.accountId, operationId: 'foreign-key-revoke', keyId: created.key.id });
  assert.equal(foreign.error.code, 'ai_key_unavailable');
  const rotateArgs = { expectedAccountId: a.accountId, operationId: 'key-rotate', keyId: created.key.id };
  const rotated = await f.call(a, 'ai.keys.rotate', rotateArgs); assert.notEqual(rotated.key.id, created.key.id); assert.equal(rotated.secret, 'soty_rotated_synthetic_once');
  const replay = await f.call(a, 'ai.keys.rotate', rotateArgs); assert.equal(replay.key.id, rotated.key.id); assert.equal(replay.secret, null);
  const revoked = await f.call(a, 'ai.keys.revoke', { expectedAccountId: a.accountId, operationId: 'key-revoke', keyId: rotated.key.id }); assert.equal(revoked.key.status, 'revoked');
  assert.equal(f.accounts.size, 2); assert.equal(f.accounts.get(a.accountId).allowance.grantedMicrorubles, '50000000');
});

test('a valid bounded history above 100 keys remains manageable and projects active key metadata', async t => {
  const f = await fixture(t), a = await f.person('A');
  for (let index = 0; index < 101; index++) {
    const key = { id: randomUUID(), owner: a.accountId, name: 'History ' + index, prefix: 'soty…test', status: index ? 'revoked' : 'active', limits: {} };
    f.keys.set(key.id, key);
  }
  const result = await f.call(a, 'ai.keys.list', { expectedAccountId: a.accountId }); assert.equal(result.keys.length, 101); assert.equal(result.keys[0].status, 'active');
  assert.equal(JSON.stringify(result).includes('owner'), false);
});

test('asynchronous or false guards cannot become model authority', async t => {
  const f = await fixture(t), a = await f.person('A'), actor = await f.issued(a);
  assert.throws(() => f.ai().capture({ actor, assertCurrent: async () => true }), error => error.code === 'ai_context_changed');
  const port = f.ai().capture({ actor, assertCurrent: () => Promise.resolve(true) });
  assert.throws(port.guard, error => error.code === 'ai_context_changed');
  const falsePort = f.ai().capture({ actor, assertCurrent: () => false }); assert.throws(falsePort.guard);
  assert.throws(() => f.ai().captureConnector({ owner: { accountId: a.accountId, deviceId: actor.deviceId }, assertCurrent: () => true }));
  assert.equal(f.state.calls.length, 0);
});

test('native branded mode keeps exact GLM/usage/receipt and account-scoped logical idempotency', async t => {
  const f = await fixture(t), a = await f.person('A'), actor = await f.issued(a), transport = createPersonalAgentTransport({ accountBridge: f.ai() });
  t.after(() => transport.close());
  const bound = transport.bind({ actor, scope: { accountId: a.accountId }, turnId: 'turn-1', assertCurrent: () => f.service.withActorAuthorityFence(actor, () => true) });
  const request = { requestId: 'round-1', requestDigest: 'digest-round', body: { model: GLM_MODEL, messages: [{ role: 'user', content: 'Тест' }], stream: false }, signal: new AbortController().signal };
  const response = await bound.complete(request); assert.equal(response.status, 200); await response.json();
  const receipt = await bound.account('round-1', { requestDigest: 'digest-round', usage: { status: 'reported', promptTokens: 3, completionTokens: 2 } });
  assert.equal(receipt.chargeMicrorubles, '23'); assert.equal(f.state.dispatches, 1);
  await assert.rejects(bound.complete(request), error => error.code === 'agent_reconcile_required');
  await assert.rejects(bound.complete({ ...request, requestId: 'round-2', body: { ...request.body, model: 'foreign-model' } }));
  const ingress = f.state.lastIngress; assert.equal(ingress.proof.accountId, a.accountId); assert.equal(JSON.parse(ingress.encoded).autoFallback, false);
});

test('an admitted native result and settled receipt survive a limited status caused by its own hold', async t => {
  const f = await fixture(t), a = await f.person('A'), actor = await f.issued(a), entered = deferred(), release = deferred(), gatewayRequestId = randomUUID();
  f.state.onChat = async (proof, encoded) => {
    await f.callback(proof, encoded); entered.resolve(); await release.promise;
    f.receipts.set(a.accountId + ':' + gatewayRequestId, { id: gatewayRequestId, logicalRequestId: proof.requestId, status: 'completed',
      model_requested: GLM_MODEL, model_resolved: GLM_MODEL, input_tokens: '3', output_tokens: '2', charge_urub: '7' });
    return Response.json({ model: GLM_MODEL, choices: [], usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }, { headers: { 'x-soty-request-id': gatewayRequestId } });
  };
  const transport = createPersonalAgentTransport({ accountBridge: f.ai() }); t.after(() => transport.close());
  const bound = transport.bind({ actor, scope: { accountId: a.accountId }, turnId: 'held-native', assertCurrent: () => f.service.withActorAuthorityFence(actor, () => true) });
  const request = { requestId: 'round-1', requestDigest: 'held-digest', body: { model: GLM_MODEL, stream: false, messages: [{ role: 'user', content: 'Draft' }] }, signal: new AbortController().signal };
  const work = bound.complete(request); await entered.promise;
  f.accounts.get(a.accountId).state = 'limited'; await f.call(a, 'ai.account.get', { expectedAccountId: a.accountId }); assert.equal(f.ai().eligible(actor), false);
  release.resolve(); const response = await work; assert.equal(response.status, 200); await response.json();
  const receipt = await bound.account('round-1', { requestDigest: 'held-digest', usage: { status: 'reported', promptTokens: 3, completionTokens: 2 } }); assert.equal(receipt.chargeMicrorubles, '7');
  assert.equal(f.ai().eligible(actor), false, 'new voice admission is denied by the fresh limited state; accepted accounting remains readable');
});

test('maintenance denies new work without invalidating an admitted response or owner receipt/readback', async t => {
  const f = await fixture(t), a = await f.person('A'), actor = await f.issued(a), port = f.ai().capture({ actor });
  const response = await port.complete({ requestId: 'before-maintenance', body: { model: GLM_MODEL }, signal: new AbortController().signal });
  f.state.maintenance = true; assert.equal(f.ai().admissionStatus().accepting, false); assert.equal(f.ai().eligible(actor), false);
  await assert.rejects(port.complete({ requestId: 'during-maintenance', body: { model: GLM_MODEL }, signal: new AbortController().signal }), error => error.code === 'ai_maintenance');
  const mutation = await f.raw(a, 'ai.keys.create', { expectedAccountId: a.accountId, operationId: 'maintenance-key', name: 'Key' }); assert.equal(mutation.error.code, 'ai_maintenance');
  assert.equal(f.keys.size, 0);
  assert.equal((await f.call(a, 'ai.account.get', { expectedAccountId: a.accountId })).state, 'ready');
  assert.deepEqual((await f.call(a, 'ai.keys.list', { expectedAccountId: a.accountId })).keys, []);
  await response.json(); const receipt = await port.readReceipt({ requestId: response.headers.get('x-soty-request-id'), logicalRequestId: 'before-maintenance' }); assert.equal(receipt.request.status, 'completed');
  assert.equal(f.state.dispatches, 1); assert.equal(f.ai().admissionStatus().activeRequests, 0);
});

test('queued inference rechecks original device on signed callback after revocation and dispatches zero times', async t => {
  const f = await fixture(t), a = await f.person('A'), a2 = await f.enroll(a, 'Recovery device'), actor = await f.issued(a), gate = deferred(), entered = deferred();
  f.ai().start();
  f.state.onChat = async (proof, encoded) => { entered.resolve(); await gate.promise; await f.callback(proof, encoded); f.state.dispatches++; return Response.json({}); };
  const port = f.ai().capture({ actor });
  const work = port.complete({ requestId: 'queued-revoke', body: { model: GLM_MODEL }, signal: new AbortController().signal });
  const rejected = assert.rejects(work, error => error.code === 'ai_context_changed'); await entered.promise;
  await f.call(a2, 'device.revoke', { deviceId: a.deviceId }); gate.resolve(); await rejected; assert.equal(f.state.dispatches, 0);
});

test('observed original-context invalidation is terminal across A to B to A', async t => {
  const f = await fixture(t), a = await f.person('A'), actor = await f.issued(a), gate = deferred(), entered = deferred(); let current = true;
  f.state.onChat = async (proof, encoded) => { entered.resolve(); await gate.promise; await f.callback(proof, encoded); f.state.dispatches++; return Response.json({}); };
  const port = f.ai().capture({ actor, assertCurrent() { if (!current) throw Error('selection changed'); } });
  const work = port.complete({ requestId: 'original-context', body: { model: GLM_MODEL }, signal: new AbortController().signal });
  const rejected = assert.rejects(work); await entered.promise; current = false;
  await assert.rejects(f.callback(f.state.lastIngress.proof, f.state.lastIngress.encoded), error => error.code === 'ai_dispatch_denied');
  current = true; gate.resolve(); await rejected; assert.equal(f.state.dispatches, 0); assert.throws(port.guard, error => error.code === 'ai_context_changed');
});

test('callback binds exact payload, model and request and allows each fallback only inside captured policy', async t => {
  const f = await fixture(t), a = await f.person('A'), actor = await f.issued(a);
  f.state.onChat = async (proof, encoded) => {
    for (const overrides of [{ bodySha256: '0'.repeat(64) }, { accountId: 'acct_' + 'F'.repeat(32) }, { deviceId: 'foreign' }, { requestId: 'foreign' }, { model: 'unapproved-model' }]) {
      await assert.rejects(f.callback(proof, encoded, overrides), error => error.code === 'ai_dispatch_denied');
    }
    assert.deepEqual(await f.callback(proof, encoded), { authorized: true });
    assert.deepEqual(await f.callback(proof, encoded, { model: 'MiniMaxAI/MiniMax-M2.7' }), { authorized: true });
    return Response.json({ ok: true });
  };
  const response = await f.ai().capture({ actor }).complete({ requestId: 'fallback-policy', body: { model: GLM_MODEL, autoFallback: true }, signal: new AbortController().signal });
  await response.json();
  await assert.rejects(f.callback(f.state.lastIngress.proof, f.state.lastIngress.encoded), error => error.code === 'ai_dispatch_denied');
});

test('active cancellation refuses late body and callback; expiry bounds registry capacity', async t => {
  const f = await fixture(t, { maxAuthorizations: 1, dispatchLeaseMs: 50 }), a = await f.person('A'), actor = await f.issued(a), controller = new AbortController();
  let output; f.state.onChat = async (proof, encoded) => { await f.callback(proof, encoded); return new Response(new ReadableStream({ start(value) { output = value; } })); };
  const response = await f.ai().capture({ actor }).complete({ requestId: 'active-abort', body: { model: GLM_MODEL }, signal: controller.signal });
  controller.abort(); output.enqueue(new TextEncoder().encode('late result'));
  await assert.rejects(response.text()); await assert.rejects(f.callback(f.state.lastIngress.proof, f.state.lastIngress.encoded));
  await f.ai().capture({ actor }).complete({ requestId: 'fresh-after-abort', body: { model: GLM_MODEL }, signal: new AbortController().signal }).then(response => response.body.cancel());
});

test('registry enforces bounded active capacity and a deadline frees abandoned authorizations', async t => {
  const f = await fixture(t, { maxAuthorizations: 1, dispatchLeaseMs: 200 }), a = await f.person('A'), actor = await f.issued(a);
  f.state.onChat = async (proof, encoded) => { await f.callback(proof, encoded); return new Response(new ReadableStream()); };
  const port = f.ai().capture({ actor }), first = await port.complete({ requestId: 'abandoned', body: { model: GLM_MODEL }, signal: new AbortController().signal });
  await assert.rejects(port.complete({ requestId: 'over-capacity', body: { model: GLM_MODEL }, signal: new AbortController().signal }), error => error.code === 'ai_capacity');
  await new Promise(resolve => setTimeout(resolve, 250));
  await assert.rejects(f.callback(f.state.lastIngress.proof, f.state.lastIngress.encoded), error => error.code === 'ai_dispatch_denied');
  await first.body.cancel();
  const next = await port.complete({ requestId: 'after-deadline', body: { model: GLM_MODEL }, signal: new AbortController().signal }); await next.body.cancel();
});

test('callback nonce survives Root restart and old authorizations never resurrect', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-ai-nonce-test-'));
  const f = await fixture(t, { databasePath: join(directory, 'bridge.sqlite') });
  t.after(async () => { await f.ai().close(); const target = await realpath(directory); assert.equal(dirname(target), await realpath(tmpdir())); assert.match(target, /soty-ai-nonce-test-/u); await rm(target, { recursive: true, force: true }); });
  const a = await f.person('A'), actor = await f.issued(a);
  const response = await f.ai().capture({ actor }).complete({ requestId: 'before-restart', body: { model: GLM_MODEL }, signal: new AbortController().signal });
  await response.json(); const proof = f.state.lastCallback; await f.restart();
  await assert.rejects(f.ai().authorizeDispatch(proof), error => error.code === 'bridge_replay');
  await assert.rejects(f.callback(f.state.lastIngress.proof, f.state.lastIngress.encoded), error => error.code === 'ai_dispatch_denied');
});

test('explicit service sponsor uses its own namespace, finite model set and cannot be inferred from HTTP claims', async t => {
  const sponsor = 'svc_' + 'S'.repeat(32), f = await fixture(t, { applications: [{ appId: 'server-app', accountId: sponsor, models: [GLM_MODEL] }] });
  await f.ai().reconcile(); assert.equal(f.accounts.has(sponsor), true); assert.equal(f.service.hasAccount(sponsor), false);
  assert.throws(() => f.ai().captureApplication('unknown-app', () => true), error => error.code === 'ai_billing_identity_required');
  await assert.rejects(f.ai().ensure('svc_' + 'F'.repeat(32)), error => error.code === 'ai_context_changed');
  let active = true; const port = f.ai().captureApplication('server-app', () => active);
  await assert.rejects(port.complete({ requestId: 'wrong-model', body: { model: 'foreign' }, signal: new AbortController().signal }), error => error.code === 'ai_model_denied');
  const response = await port.complete({ requestId: 'sponsor-model', body: { model: GLM_MODEL }, signal: new AbortController().signal }); await response.json();
  assert.equal(f.state.lastIngress.proof.accountId, sponsor); assert.equal(f.state.lastIngress.proof.deviceId, 'service:application:server-app');
  active = false; assert.throws(port.guard); active = true; assert.throws(port.guard);
});
