import test from 'node:test';
import assert from 'node:assert/strict';
import { createPersonalAgentModule, turnFailureDiagnostic } from '../personal-agent-module.js';
import { createPersonalAgentTransport } from '../personal-agent-transport.js';
import { GLM_MODEL, ProviderError, providerFailureCode, providerFailureDiagnostic, createGlmProvider } from '../../modules/personal-agent/provider/index.mjs';

const PRIVATE = 'synthetic-private-marker-not-a-real-secret';
const REQUEST_ID = '11111111-1111-4111-8111-111111111111';
const terminal = { capability: '__terminal__', description: 'Complete a synthetic draft',
  inputSchema: { type: 'object', additionalProperties: false, properties: {
    answer: { type: 'string' }, selectedEvidenceIds: { type: 'array' }, claims: { type: 'array' },
  }, required: ['answer', 'selectedEvidenceIds', 'claims'] } };

test('turn diagnostics expose only fixed failure codes and stages', () => {
  assert.deepEqual(turnFailureDiagnostic(new Error('tool-arguments-invalid'), 'runtime'),
    { code: 'tool-arguments-invalid', stage: 'runtime' });
  assert.deepEqual(turnFailureDiagnostic(new Error(PRIVATE), PRIVATE),
    { code: 'agent_unavailable', stage: 'runtime' });
  const thrownGetter = { get message() { throw Error(PRIVATE); } };
  assert.deepEqual(turnFailureDiagnostic(thrownGetter, 'account'),
    { code: 'agent_unavailable', stage: 'account' });
});

function packet({ finishReason = 'tool_calls', name = 'finish_answer', model = GLM_MODEL, outputTokens = 12 } = {}) {
  return JSON.stringify(
    { id: 'synthetic_completion', model, choices: [{ index: 0, message: { role: 'assistant', tool_calls: [{
      id: 'synthetic_call', type: 'function', function: { name,
        arguments: JSON.stringify({ answer: PRIVATE, selectedEvidenceIds: [], claims: [] }) } }] }, finish_reason: finishReason }],
      usage: { prompt_tokens: 10, completion_tokens: outputTokens, total_tokens: 10 + outputTokens } });
}

function fixture(t, options = {}) {
  const observed = { dispatches: 0, receipts: 0, subscriptions: 0 }, diagnostics = [];
  t.mock.method(console, 'error', value => { diagnostics.push(value); if (options.loggerThrows) throw Error(PRIVATE); });
  const keyRef = {}, tenant = 'synthetic-tenant';
  const transport = createPersonalAgentTransport({ gatewayOrigin: 'https://diagnostic-fixture.invalid',
    billingTenant: tenant, keyRef, authorizeActor: () => true,
    resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: PRIVATE }),
    readReceipt: async () => {
      observed.receipts++;
      return { tenantId: tenant, request: { id: REQUEST_ID, status: 'completed',
        model_requested: GLM_MODEL, model_resolved: GLM_MODEL, input_tokens: 10, output_tokens: 12, charge_urub: '7' } };
    },
    fetchImpl: async () => {
      observed.dispatches++;
      if (options.transportThrows) throw Error(PRIVATE);
      return new Response(packet(options), { status: options.httpStatus ?? 200,
        headers: { 'content-type': options.contentType ?? 'application/json',
          'x-soty-request-id': options.missingRequestId ? 'invalid' : REQUEST_ID,
          'x-request-id': PRIVATE } });
    },
  });
  const module = createPersonalAgentModule({ transport,
    getConnect: () => ({ withActorAuthorityFence: (_actor, work) => work(),
      subscribeRevocations: () => { observed.subscriptions++; return () => observed.subscriptions--; } }),
    toolsForActor: async () => ({ tools: {}, assertCurrent: () => true }),
    runtime: { outputProfile: 'host-enforced-terminal.v1', adapterFor: () => ({}),
      createContextStore: () => ({ resolve() {}, snapshot() {}, append() {} }),
      runTurn: async ({ provider }) => {
        const result = await provider([{ role: 'user', content: PRIVATE }], new AbortController().signal,
          { finish_answer: terminal }, { parallelToolCalls: false, maxTokens: 384 });
        return { status: 'complete', answer: result.choices[0].message.tool_calls[0].function.arguments };
      } },
  });
  t.after(() => { module.close(); transport.close(); });
  const turn = () => module.executeAsync({ op: 'agent.turn',
    actor: { accountId: 'synthetic-account', deviceId: 'synthetic-device' },
    args: { requestId: 'synthetic-diagnostic-turn', prompt: PRIVATE } });
  return { observed, diagnostics, turn };
}

test('actual JSON failures have a safe cause code and keep the unknown-turn no-resend fence', async t => {
  const cases = [
    ['truncated terminal output at the 384-token cap', { finishReason: 'length', outputTokens: 384 }, 'provider_output_incomplete'],
    ['invalid finish', { finishReason: 'synthetic-private-finish' }, 'provider_invalid_finish'],
    ['unknown tool', { name: 'synthetic_private_tool' }, 'provider_unknown_tool'],
    ['model mismatch', { model: 'synthetic-private-model' }, 'provider_model_mismatch'],
    ['invalid usage', { outputTokens: -1 }, 'provider_invalid_input'],
    ['wrong content type', { contentType: 'text/event-stream' }, 'provider_invalid_stream'],
    ['missing accounting response identifier', { missingRequestId: true }, 'provider_transport_failed'],
    ... [400, 403, 429, 503].map(httpStatus => ['HTTP ' + httpStatus, { httpStatus }, 'provider_http_failed']),
    ['lost transport ACK', { transportThrows: true }, 'provider_transport_failed'],
  ];
  for (const [label, options, code] of cases) await t.test(label, async t => {
    const f = fixture(t, options);
    await assert.rejects(f.turn(), error => error.code === 'agent_model_unknown');
    assert.equal(f.observed.dispatches, 1); assert.equal(f.observed.receipts, 0); assert.equal(f.observed.subscriptions, 0);
    assert.deepEqual(f.diagnostics.map(value => JSON.parse(value)), [{ schema: 'soty.personal-agent-provider-failure.v1', code,
      httpStatus: options.transportThrows || options.missingRequestId ? null : options.httpStatus ?? 200 }]);
    const serialized = JSON.stringify(f.diagnostics);
    assert.equal(serialized.includes(PRIVATE), false); assert.equal(serialized.includes('synthetic-account'), false);
    assert.equal(serialized.includes(REQUEST_ID), false);
    await assert.rejects(f.turn(), error => error.code === 'agent_reconcile_required');
    assert.equal(f.observed.dispatches, 1); assert.equal(f.diagnostics.length, 1);
  });
});

test('completed and accounted response emits no failure diagnosis', async t => {
  const f = fixture(t);
  const result = await f.turn();
  assert.equal(result.status, 'complete'); assert.equal(result.accounting[0].chargeMicrorubles, '7');
  assert.equal(f.observed.dispatches, 1); assert.equal(f.observed.receipts, 1); assert.equal(f.observed.subscriptions, 0);
  assert.deepEqual(f.diagnostics, []);
});

test('a failed diagnostic sink cannot change unknown status or cause another dispatch', async t => {
  const f = fixture(t, { loggerThrows: true, finishReason: 'length', outputTokens: 384 });
  await assert.rejects(f.turn(), error => error.code === 'agent_model_unknown');
  await assert.rejects(f.turn(), error => error.code === 'agent_reconcile_required');
  assert.equal(f.observed.dispatches, 1); assert.equal(f.observed.receipts, 0); assert.equal(f.observed.subscriptions, 0);
  assert.deepEqual(f.diagnostics.map(value => JSON.parse(value)), [
    { schema: 'soty.personal-agent-provider-failure.v1', code: 'provider_output_incomplete', httpStatus: 200 },
  ]);
});

test('diagnostic projection rejects foreign codes, forged errors, accessors and proxies without reading private fields', () => {
  let reads = 0;
  const known = new ProviderError('provider_invalid_finish', { private: PRIVATE });
  for (const name of ['message', 'cause', 'accounting', 'toJSON']) Object.defineProperty(known, name,
    { get() { reads++; throw Error(PRIVATE); } });
  assert.equal(providerFailureCode(known), 'provider_invalid_finish');
  assert.deepEqual(providerFailureDiagnostic(known), { code: 'provider_invalid_finish', httpStatus: null });
  const getter = new ProviderError('provider_invalid_finish');
  Object.defineProperty(getter, 'code', { get() { reads++; throw Error(PRIVATE); } });
  const forged = Object.create(ProviderError.prototype, { code: { value: 'provider_invalid_finish' } });
  const throwing = new Proxy(known, { get() { reads++; throw Error(PRIVATE); },
    getPrototypeOf() { reads++; throw Error(PRIVATE); }, getOwnPropertyDescriptor() { reads++; throw Error(PRIVATE); } });
  const revoked = Proxy.revocable(known, {}); revoked.revoke();
  for (const error of [null, undefined, PRIVATE, {}, new Error(PRIVATE), new ProviderError(PRIVATE),
    getter, forged, throwing, revoked.proxy]) {
    assert.equal(providerFailureCode(error), 'provider_transport_failed');
    assert.deepEqual(providerFailureDiagnostic(error), { code: 'provider_transport_failed', httpStatus: null });
  }
  assert.equal(reads, 0);
  Object.defineProperty(Object.prototype, 'value', { value: 'provider_model_mismatch', configurable: true });
  try { assert.equal(providerFailureCode(getter), 'provider_transport_failed'); }
  finally { delete Object.prototype.value; }
  assert.equal(reads, 0);
});

// The diagnostic must not traverse even an authentic failure's private accounting.
test('trusted HTTP evidence survives hostile accounting accessors without reading them', async () => {
  let reads = 0;
  const provider = createGlmProvider({ responseMode: 'json', transport: async () => new Response(PRIVATE, { status: 429 }) });
  try {
    await assert.rejects(provider.complete({ requestId: 'synthetic-status', messages: [{ role: 'user', content: PRIVATE }] }), error => {
      for (const key of ['accounting', 'message', 'cause', 'httpStatus']) Object.defineProperty(error, key,
        { get() { reads++; throw Error(PRIVATE); } });
      assert.deepEqual(providerFailureDiagnostic(error), { code: 'provider_http_failed', httpStatus: 429 });
      assert.equal(reads, 0);
      return true;
    });
  } finally { provider.close(); }
});
