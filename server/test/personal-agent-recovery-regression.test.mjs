import test from 'node:test';
import assert from 'node:assert/strict';
import { createPersonalAgentConversation } from '../../src/platform/personal-agent-client.ts';
import { createPersonalAgentModule } from '../personal-agent-module.js';
import { createPersonalAgentTransport } from '../personal-agent-transport.js';
import { createBrowserAssistPilot } from '../browser-assist-pilot.js';

const contextId = 'ctx_abcdefghijklmnop12345678';
const defer = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };

test('a status captured before cancel cannot erase the acknowledged cancellation', async t => {
  const accountId = 'review_account_01', id = 'review_pending_01', gate = defer(), entered = defer(), calls = [];
  const original = JSON.stringify({ schema: 'soty.personal-agent-ui.v1', draft: 'Original exact question', active: id,
    pending: [{ requestId: id, prompt: 'Original exact question' }] });
  const key = `soty.personal-agent-ui.v1:${accountId}`, records = new Map([[key, original]]);
  const ui = createPersonalAgentConversation({ accountId,
    storage: { getItem: name => records.get(name) ?? null, setItem: (name, value) => records.set(name, value) },
    client: { getLocalState: async () => ({ accountId }), async extension(op, args, options) {
      calls.push(op); assert.deepEqual(options, { expectedAccountId: accountId }); assert.equal(args.requestId, id);
      if (op === 'agent.status') {
        entered.resolve(); await gate.promise;
        return { schema: 'soty.personal-agent-status.v1', requestId: id, state: 'running', stage: 'model',
          modelRounds: 1, completedTools: 0, elapsedMs: 1000, cancelRequested: false };
      }
      assert.equal(op, 'agent.cancel');
      return { schema: 'soty.personal-agent-cancel.v1', requestId: id, cancelRequested: true, remoteTerminationVerified: false };
    } } });
  t.after(() => ui.dispose());
  const checking = ui.checkStatus(); await entered.promise;
  const cancelled = await ui.cancel(); assert.equal(cancelled.remoteTerminationVerified, false);
  assert.equal(ui.view().phase, 'cancel_requested'); gate.resolve(); await checking;
  assert.deepEqual(calls, ['agent.status', 'agent.cancel']); assert.equal(records.get(key), original);
  assert.equal(ui.view().unconfirmed, 1); assert.equal(ui.view().phase, 'cancel_requested');
  assert.equal(ui.view().progress.cancelRequested, true);
});

function browserFixture(t) {
  const actor = { accountId: 'review_browser_account', deviceId: 'review_browser_device' }, id = 'review_browser_turn_01';
  const binding = { appId: 'app-' + 'a'.repeat(32), origin: 'https://app.example', targetRevision: 1,
    targetDigest: 'b'.repeat(64), generation: 1, channel: 'c'.repeat(32) };
  let allowed = true, revision = 1, epoch = 1;
  const connect = { withActorAuthorityFence(who, work) { assert.deepEqual(who, actor); return work(); }, subscribeRevocations: () => () => {} };
  const pilot = createBrowserAssistPilot({ getConnect: () => connect, getApps: () => ({ withAppAuthority({ actor: who, appId }, work) {
    assert.deepEqual(who, actor); assert.equal(appId, binding.appId); if (!allowed) throw Error('revoked fixture app');
    return work({ entry: { origin: binding.origin, domainId: 'review_domain' }, policyEpoch: epoch, appRevision: 1,
      target: { revision, digest: binding.targetDigest } });
  } }) });
  const transport = createPersonalAgentTransport({ gatewayOrigin: 'https://gateway.example', billingTenant: 'review', keyRef: {},
    authorizeActor: () => true, resolveCredential: async () => { throw Error('unexpected credential access'); },
    readReceipt: async () => { throw Error('unexpected accounting'); }, fetchImpl: async () => { throw Error('unexpected model POST'); } });
  const host = createPersonalAgentModule({ getConnect: () => connect, browserPilot: pilot, transport,
    toolsForActor: async () => ({ tools: {}, assertCurrent: () => true }),
    runtime: { outputProfile: 'host-enforced-terminal.v1', createContextStore: () => ({ resolve() {}, snapshot() {}, append() {} }), adapterFor: () => ({}),
      async runTurn() { return { status: 'complete', answer: 'Already completed exact answer', contextId, evidence: [] }; } } });
  t.after(() => { host.close(); transport.close(); });
  return { start: () => host.executeAsync({ op: 'agent.turn', actor, args: { requestId: id, prompt: 'Original exact question', browser: binding } }),
    status: () => host.executeAsync({ op: 'agent.status', actor, args: { requestId: id } }),
    poll: () => host.executeAsync({ op: 'agent.browser.poll', actor, args: { requestId: id, channel: binding.channel } }),
    revoke: () => { allowed = false; }, revise: () => { revision++; }, changePolicy: () => { epoch++; } };
}

test('a closed browser action lease permits read-only answer recovery under current authority', async t => {
  const f = browserFixture(t), original = await f.start(); assert.equal(original.status, 'complete');
  assert.deepEqual(await f.poll(), { pending: false });
  const status = await f.status(); assert.equal(status.state, 'completed'); assert.equal(status.result.answer, original.answer);
  assert.deepEqual(await f.poll(), { pending: false }); // Recovery cannot reopen the execution channel.
});

for (const [changed, change] of [['revoked-app', 'revoke'], ['target-revision', 'revise'], ['policy-epoch', 'changePolicy']]) {
  test(`browser answer recovery preserves the ${changed} fence after execution closes`, async t => {
    const f = browserFixture(t); await f.start(); f[change]();
    const status = await f.status(); assert.equal(status.state, 'unknown'); assert.equal(status.result, undefined);
    assert.deepEqual(await f.poll(), { pending: false });
  });
}
