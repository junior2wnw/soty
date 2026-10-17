import test from 'node:test';
import assert from 'node:assert/strict';
import { createPersonalAgentModule } from '../personal-agent-module.js';
import { createPersonalAgentTransport, agentError } from '../personal-agent-transport.js';

const actor = { accountId: 'status_fixture_account', deviceId: 'status_fixture_device' };
const contextId = 'ctx_abcdefghijklmnop12345678';
function fixture(t, { sourceRecovery, reject = false } = {}) {
  let at = 100, sourceAllowed = true, actorAllowed = true, release, entered;
  const gate = new Promise(done => { release = done; }), ready = new Promise(done => { entered = done; });
  const transport = createPersonalAgentTransport({ gatewayOrigin: 'https://fixture.invalid', billingTenant: 'fixture', keyRef: {},
    authorizeActor: () => true, resolveCredential: async () => { throw Error('unexpected model'); },
    readReceipt: async () => { throw Error('unexpected accounting'); }, fetchImpl: async () => { throw Error('unexpected dispatch'); } });
  const host = createPersonalAgentModule({ transport, clock: () => at,
    getConnect: () => ({ withActorAuthorityFence(_actor, work) { if (!actorAllowed) throw agentError('agent_actor_denied'); return work(); }, subscribeRevocations: () => () => {} }),
    toolsForActor: async () => ({ tools: {}, assertCurrent: () => sourceAllowed, ...(sourceRecovery ? { sourceRecovery } : {}) }),
    runtime: { outputProfile: 'host-enforced-terminal.v1', adapterFor: () => ({}), createContextStore: () => ({ resolve() {}, snapshot() {}, append() {} }),
      async runTurn() { entered(); await gate; if (reject) throw agentError('agent_source_denied');
        return { status: 'complete', answer: 'Own bounded answer', contextId, evidence: [{ id: 'source_one', title: 'Own source',
          observedAt: null, sourceStatus: 'current', data: { private: 'never in status' } }] }; } } });
  t.after(() => { release(); host.close(); transport.close(); });
  const id = 'status_fixture_turn_01';
  return { ready, release, id, host,
    start: () => host.executeAsync({ op: 'agent.turn', actor, args: { requestId: id, prompt: 'Private intent, never in status' } }),
    status: (other = actor) => host.executeAsync({ op: 'agent.status', actor: other, args: { requestId: id } }),
    at: value => { at = value; }, sourceAllowed: value => { sourceAllowed = value; }, actorAllowed: value => { actorAllowed = value; } };
}

test('status retention is bounded, actor-scoped, copied and contains no Source payload', async t => {
  const f = fixture(t), pending = f.start(); await f.ready;
  assert.equal((await f.status()).state, 'running');
  assert.equal((await f.status({ ...actor, accountId: 'different_account' })).state, 'unavailable');
  f.release(); await pending;
  const first = await f.status(); assert.equal(first.state, 'completed'); assert.equal(first.result.answer, 'Own bounded answer');
  assert.equal(JSON.stringify(first).includes('never in status'), false);
  assert.equal(JSON.stringify(first).includes('Private intent'), false);
  first.result.evidence[0].title = 'Mutated copy'; assert.equal((await f.status()).result.evidence[0].title, 'Own source');
  f.at(1800101); const expired = await f.status();
  assert.deepEqual(expired, { schema: 'soty.personal-agent-status.v1', requestId: f.id, state: 'unavailable' });
});

test('a changed current Source fence blocks result recovery without claiming no effect', async t => {
  const f = fixture(t), pending = f.start(); await f.ready; f.release(); await pending; f.sourceAllowed(false);
  const status = await f.status(); assert.equal(status.state, 'unknown'); assert.equal(status.result, undefined);
  f.actorAllowed(false); await assert.rejects(f.status(), { code: 'agent_actor_denied' });
});

test('unknown Source pointers are a closed bounded DTO, never raw input or permission', async t => {
  let calls = 0;
  const f = fixture(t, { reject: true, sourceRecovery: async signal => { assert.ok(signal instanceof AbortSignal); calls++;
    return [{ reference: { capabilityId: 'fixture:write', version: 1 }, invocationId: 'original_invocation_01' }]; } });
  const pending = f.start(); await f.ready; f.release(); await assert.rejects(pending);
  const status = await f.status(); assert.equal(status.state, 'unknown'); assert.equal(status.result, undefined); assert.equal(calls, 1);
  assert.deepEqual(status.sourceRecovery, [{ reference: { capabilityId: 'fixture:write', version: 1 }, invocationId: 'original_invocation_01' }]);
});

test('a malformed Source recovery hook is suppressed rather than leaking private fields', async t => {
  const f = fixture(t, { reject: true, sourceRecovery: async () => [{ reference: { capabilityId: 'fixture:write', version: 1 },
    invocationId: 'original_invocation_01', input: 'private mutation arguments' }] });
  const pending = f.start(); await f.ready; f.release(); await assert.rejects(pending);
  const status = await f.status(); assert.equal(status.state, 'unknown'); assert.equal(status.sourceRecovery, undefined);
  assert.equal(JSON.stringify(status).includes('private mutation'), false);
});
