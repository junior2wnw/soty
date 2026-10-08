import test from 'node:test';
import assert from 'node:assert/strict';
import { createPersonalAgentConversation } from './personal-agent-client.ts';
const defer = () => { let resolve, reject; const promise = new Promise((done, fail) => { resolve = done; reject = fail; }); return { promise, resolve, reject }; };
const contextId = 'ctx_abcdefghijklmnop12345678';
function fixture() {
  const data = new Map(), calls = [], gate = defer(); let accountId = 'account_one', outcome = 'complete', sequence = 0;
  const storage = { getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value) };
  const client = { getLocalState: async () => ({ accountId }), extension: async (op, args, options) => {
    calls.push({ op, args, options });
    if (op === 'agent.cancel') return { schema: 'soty.personal-agent-cancel.v1', requestId: args.requestId, cancelRequested: true, remoteTerminationVerified: false };
    if (outcome === 'held') await gate.promise;
    if (outcome === 'unknown') throw new Error('unknown');
    return { schema: 'soty.personal-agent-turn.v1', status: 'complete', requestId: args.requestId, answer: 'Verified answer', contextId };
  } };
  const make = (options = {}) => createPersonalAgentConversation({ accountId: 'account_one', client, storage, idFactory: () => `browser_turn_${++sequence}`, ...options });
  return { data, calls, storage, client, gate, make, outcome: value => { outcome = value; }, account: value => { accountId = value; } };
}
const settled = async f => { for (let i = 0; i < 20 && !f.calls.length; i++) await new Promise(done => setImmediate(done)); assert.equal(f.calls.length, 1); };

test('UI dispatch uses signed own account, actual ACK/context and no payload authority', async () => {
  const f = fixture(), ui = f.make(); const response = await ui.turn('Question');
  assert.equal(response.phase, 'complete'); assert.equal(response.answer, 'Verified answer'); assert.equal(response.unconfirmed, 0);
  assert.deepEqual(f.calls[0], { op: 'agent.turn', args: { requestId: 'browser_turn_1', prompt: 'Question' }, options: { expectedAccountId: 'account_one' } });
  await ui.turn('Second question'); assert.equal(f.calls[1].args.contextId, contextId);
});
test('unknown send/reload does not repeat; explicit new task preserves original intent', async () => {
  const f = fixture(); f.outcome('unknown'); const ui = f.make(); await assert.rejects(ui.turn('Original exact prompt'));
  const old = [...f.data.values()][0], reopened = f.make(); assert.equal(reopened.view().phase, 'unknown'); assert.equal(f.calls.length, 1);
  await assert.rejects(reopened.turn('Original exact prompt'), { code: 'agent_reconcile_required' }); assert.equal([...f.data.values()][0], old);
  reopened.startNew(); const retained = JSON.parse([...f.data.values()][0]).pending[0];
  assert.deepEqual(retained, { requestId: 'browser_turn_1', prompt: 'Original exact prompt' });
  f.outcome('complete'); await reopened.turn('Explicit new prompt'); assert.equal(f.calls.length, 2); assert.equal(reopened.view().unconfirmed, 1);
});
test('storage rejection happens before any signed model dispatch', async () => {
  const f = fixture(), ui = f.make({ storage: { getItem: () => null, setItem: () => { throw new Error('storage full'); } } });
  await assert.rejects(ui.turn('Retain this draft')); assert.equal(f.calls.length, 0);
});
test('account switch during response denies delivery and retains the old scoped intent', async () => {
  const f = fixture(); f.outcome('held'); const ui = f.make(), pending = ui.turn('Private request'); await settled(f);
  f.account('account_two'); f.gate.resolve(); await assert.rejects(pending, { code: 'authentication_required' });
  assert.equal(ui.view().answer, undefined); assert.equal(ui.view().unconfirmed, 1);
  const other = f.make({ accountId: 'account_two' }); assert.equal(other.view().unconfirmed, 0);
});
test('dispose while response is pending cannot deliver or erase a sent intent', async () => {
  const f = fixture(); f.outcome('held'); const ui = f.make(), pending = ui.turn('Keep unknown'); await settled(f);
  ui.dispose(); f.gate.resolve(); await assert.rejects(pending, { code: 'authentication_required' }); assert.equal(ui.view().answer, undefined); assert.equal(ui.view().unconfirmed, 1);
});
test('signed cancellation acknowledges a local request without claiming remote termination', async () => {
  const f = fixture(); f.outcome('held'); const ui = f.make(), pending = ui.turn('Running task'); await settled(f);
  const cancelled = await ui.cancel(); assert.equal(cancelled.remoteTerminationVerified, false); assert.equal(ui.view().phase, 'cancel_requested');
  assert.deepEqual(f.calls[1], { op: 'agent.cancel', args: { requestId: 'browser_turn_1' }, options: { expectedAccountId: 'account_one' } });
  f.outcome('unknown'); f.gate.resolve(); await assert.rejects(pending); assert.equal(ui.view().phase, 'unknown'); assert.equal(ui.view().unconfirmed, 1);
});
test('wrong ACK cannot release the original pending request', async () => {
  const f = fixture(), ui = f.make({ client: { ...f.client, extension: async () => ({ schema: 'soty.personal-agent-turn.v1',
    requestId: 'different_turn_id', status: 'complete', answer: 'Wrong answer', contextId }) } });
  await assert.rejects(ui.turn('Exact original'), { code: 'agent_output_invalid' }); assert.equal(ui.view().unconfirmed, 1); assert.equal(ui.view().answer, undefined);
});
test('a false Connect envelope never releases a sent intent', async () => {
  const f = fixture(), ui = f.make({ client: { ...f.client, extension: async (_op, args) => ({ ok: false, schema: 'soty.personal-agent-turn.v1',
    requestId: args.requestId, status: 'complete', answer: 'Unconfirmed', contextId }) } });
  await assert.rejects(ui.turn('Exact original'), { code: 'agent_output_invalid' }); assert.equal(ui.view().unconfirmed, 1); assert.equal(ui.view().answer, undefined);
});
test('unknown intent capacity refuses a ninth send without evicting any original intent', async () => {
  const f = fixture(); f.outcome('unknown'); const ui = f.make();
  for (let i = 0; i < 8; i++) { await assert.rejects(ui.turn(`Pending ${i}`)); ui.startNew(); }
  const before = [...f.data.values()][0]; await assert.rejects(ui.turn('Do not dispatch ninth'), { code: 'agent_capacity' });
  assert.equal(f.calls.length, 8); assert.equal([...f.data.values()][0], before);
});
