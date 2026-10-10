import test from 'node:test';
import assert from 'node:assert/strict';
import { createPersonalAgentModule } from '../personal-agent-module.js';
import { createPersonalAgentTransport, agentError } from '../personal-agent-transport.js';
import { classifyProviderFailure } from '../../modules/personal-agent/runtime/vendor/agent-runtime-core.ts';

test('host grants runtime its supported 90-second bound and releases authority subscription', async t => {
  let captured, subscriptions = 0, dispatches = 0;
  const transport = createPersonalAgentTransport({ gatewayOrigin: 'https://fixture.invalid', billingTenant: 'fixture', keyRef: {},
    resolveCredential: async () => { throw Error('unexpected credential read'); }, readReceipt: async () => { throw Error('unexpected receipt read'); },
    authorizeActor: () => true, fetchImpl: async () => { dispatches++; throw Error('unexpected model dispatch'); } });
  const module = createPersonalAgentModule({ transport,
    getConnect: () => ({ withActorAuthorityFence: (_actor, work) => work(), subscribeRevocations: () => { subscriptions++; return () => subscriptions--; } }),
    toolsForActor: async () => ({ tools: {}, assertCurrent: () => true }),
    runtime: { outputProfile: 'host-enforced-terminal.v1', adapterFor: () => ({}),
      createContextStore: () => ({ resolve() {}, snapshot() {}, append() {} }),
      runTurn: async options => { captured = options; return { status: 'complete', answer: 'fixture' }; } } });
  t.after(() => { module.close(); transport.close(); });
  const result = await module.executeAsync({ op: 'agent.turn', actor: { accountId: 'fixture-account', deviceId: 'fixture-device' },
    args: { requestId: 'fixture-turn-001', prompt: 'fixture' } });
  assert.equal(captured.timeoutMs, 90_000); assert.equal(captured.role, 'human');
  assert.equal(result.status, 'complete'); assert.equal(subscriptions, 0); assert.equal(dispatches, 0);
});

test('host unknown model/accounting errors never enter the core retry branch', () => {
  for (const code of ['agent_model_unknown', 'agent_accounting_unknown', 'agent_reconcile_required']) {
    assert.deepEqual(classifyProviderFailure(agentError(code)), { retryable: false, reasonClass: 'non_transient' });
  }
});
