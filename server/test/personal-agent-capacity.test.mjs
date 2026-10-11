import test from 'node:test';
import assert from 'node:assert/strict';
import { createPersonalAgentModule } from '../personal-agent-module.js';
import { createPersonalAgentComposition } from '../personal-agent-composition.js';
import { createPersonalAgentTransport } from '../personal-agent-transport.js';

function fixture(t, capacity, options = {}) {
  let now = 1000, stores = 0, runs = 0;
  const transport = createPersonalAgentTransport({ gatewayOrigin: 'https://capacity-fixture.invalid', billingTenant: 'fixture', keyRef: {},
    resolveCredential: async () => { throw Error('no credential calls'); }, readReceipt: async () => { throw Error('no accounting calls'); },
    authorizeActor: () => true, fetchImpl: async () => { throw Error('no model calls'); } });
  const module = createPersonalAgentModule({ transport, capacity, clock: () => now,
    getConnect: () => ({ withActorAuthorityFence: (_actor, work) => work(), subscribeRevocations: () => () => {} }),
    toolsForActor: async () => ({ tools: {}, assertCurrent: () => true }),
    runtime: { outputProfile: 'host-enforced-terminal.v1', adapterFor: ({ actor }) => ({ actor }),
      createContextStore() { stores++; let account; return { resolve(value) { account ??= value; assert.equal(value, account, 'fallback context must be account-isolated'); }, snapshot() {}, append() {} }; },
      async runTurn({ adapter, contextStore }) { runs++; contextStore.resolve(adapter.actor.accountId); await options.run?.(); return { status: 'complete' }; } } });
  t.after(() => { module.close(); transport.close(); });
  return { module, setNow: value => { now = value; }, counts: () => ({ stores, runs }),
    turn: (id, accountId = 'account-one', extra = {}) => module.executeAsync({ op: 'agent.turn', actor: { accountId, deviceId: 'fixture-device' },
      args: { requestId: 'capacity-turn-' + id, prompt: 'synthetic local capacity', ...extra } }) };
}

test('default retains128 completed turns; host256 admits256, exact unknown/replay fences and30min expiry remain', async t => {
  for (const [capacity, total] of [[undefined, 128], [{ maxRetainedTurns: 256 }, 256]]) {
    const f = fixture(t, capacity);
    for (let index = 0; index < total; index++) assert.equal((await f.turn(index)).status, 'complete');
    await assert.rejects(f.turn('overflow'), { code: 'agent_capacity' });
    await assert.rejects(f.turn(0), { code: 'agent_reconcile_required' });
    await assert.rejects(f.turn(0, 'account-one', { prompt: 'different intent' }), { code: 'agent_intent_conflict' });
    assert.equal(f.counts().runs, total); assert.equal(f.counts().stores, 1);
    f.setNow(1801000); assert.equal((await f.turn('after-expiry')).status, 'complete');
  }
  const failed = fixture(t, { maxRetainedTurns: 1 }, { run() { throw Error('synthetic unknown'); } });
  await assert.rejects(failed.turn('unknown'));
  await assert.rejects(failed.turn('unknown'), { code: 'agent_reconcile_required' });
  await assert.rejects(failed.turn('other'), { code: 'agent_capacity' });
  assert.equal(failed.counts().runs, 1);
});

test('fallback accounts default128/config256 are independent; running context and turn cannot be evicted by expiry', async t => {
  for (const [capacity, total] of [[{ maxRetainedTurns: 1024 }, 128], [{ maxRetainedTurns: 1024, maxAccountContexts: 256 }, 256]]) {
    const f = fixture(t, capacity);
    for (let index = 0; index < total; index++) await f.turn(index, 'account-' + index);
    await assert.rejects(f.turn('overflow', 'foreign-account'), { code: 'agent_capacity' });
    await f.turn('same-account', 'account-0'); assert.equal(f.counts().stores, total);
  }
  let release, entered;
  const waiting = new Promise(done => { entered = done; }), held = new Promise(done => { release = done; });
  const f = fixture(t, { maxRetainedTurns: 1, maxAccountContexts: 1 }, { async run() { entered(); await held; } });
  const pending = f.turn('held'); await waiting; f.setNow(1801001);
  await assert.rejects(f.turn('too-soon', 'account-two'), { code: 'agent_capacity' });
  assert.equal(f.module.status().activeTurns, 1); release(); await pending;
  await f.turn('after-settlement', 'account-two'); assert.equal(f.counts().stores, 2);
});

test('capacity is constructor-only, strictly bounded; composition forwards it and signed turn bodies reject it', async t => {
  for (const value of [0, -1, 1.5, 65537, Infinity, '256', null]) {
    assert.throws(() => createPersonalAgentModule({ capacity: { maxRetainedTurns: value } }), { code: 'agent_invalid_input' });
    assert.throws(() => createPersonalAgentComposition({ getConnect() {}, getServices() {}, configuration: { capacity: { maxAccountContexts: value } } }));
  }
  for (const capacity of [null, { ttlMs: 1 }, { get maxRetainedTurns() { throw Error('getter must not execute'); } }])
    assert.throws(() => createPersonalAgentModule({ capacity }));
  const composition = createPersonalAgentComposition({ getConnect() {}, getServices() {}, configuration: { capacity: { maxRetainedTurns: 65536, maxAccountContexts: 65536 } } });
  composition.close();
  const f = fixture(t, { maxRetainedTurns: 1 });
  await assert.rejects(f.turn('body-limit', 'account-one', { capacity: { maxRetainedTurns: 256 } }));
  assert.equal(f.counts().runs, 0);
});
