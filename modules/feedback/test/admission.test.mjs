import test from 'node:test';
import assert from 'node:assert/strict';
import { createFeedbackService } from '../server/index.mjs';
import { DEFAULT_FEEDBACK_PROFILE } from '../server/profile.mjs';

const busy = error => error?.code === 'feedback_busy' && error.status === 503;
function fixture(t, admitNewWork) {
  const actor = { accountId: 'owner', deviceId: 'device' };
  const service = createFeedbackService({ databasePath: ':memory:', actorActive: who => who.accountId === actor.accountId,
    withAppAuthority({ appId }, run) { return run({ appId, accountId: actor.accountId, ownerId: actor.accountId, canManage: true }); },
    ...(admitNewWork ? { admitNewWork } : {}) });
  t.after(() => service.close());
  const call = (operation, args) => service.execute({ op: 'apps.feedback.' + operation, actor, args });
  const { installationId } = call('context', { appId: 'fixture' }).context;
  return { call, scope: { appId: 'fixture', installationId } };
}

test('host admission blocks fresh user effects but keeps exact receipts and existing reads available', t => {
  let accepting = true;
  const f = fixture(t, () => accepting), input = { ...f.scope, requestId: 'one-submit', body: 'Synthetic', attachments: [] };
  const sent = f.call('submit', input), selected = { ...f.scope, ticketId: sent.receipt.ticketId };
  const ready = { ...selected, requestId: 'one-ready', expectedRevision: 1, status: 'ready_to_check' };
  const priorStatus = f.call('status', ready); accepting = false;
  assert.equal(f.call('submit', input).replayed, true); assert.deepEqual(f.call('submit', input).receipt, sent.receipt);
  assert.equal(f.call('status', ready).replayed, true); assert.deepEqual(f.call('status', ready).receipt, priorStatus.receipt);
  const before = f.call('get', selected).ticket;
  for (const [operation, args] of [['submit', { ...input, requestId: 'new-submit' }],
    ['reply', { ...selected, requestId: 'new-reply', expectedRevision: 2, body: 'New reply' }],
    ['status', { ...selected, requestId: 'new-status', expectedRevision: 2, status: 'in_progress' }],
    ['accept', { ...selected, requestId: 'new-accept', expectedRevision: 2 }]]) assert.throws(() => f.call(operation, args), busy);
  assert.deepEqual(f.call('get', selected).ticket, before);
  assert.equal(f.call('list', f.scope).tickets.length, 1);
  assert.equal(f.call('context', { appId: 'another-context' }).context.canSubmit, true);
  accepting = true;
  assert.equal(f.call('submit', { ...input, requestId: 'new-submit' }).replayed, false);
});

test('nonboolean, asynchronous and throwing host decisions fail closed before any user effect', t => {
  const state = { decision: () => true }, f = fixture(t, () => state.decision());
  for (const [index, decision] of [() => false, () => undefined, () => 1, () => 'true', () => ({}),
    () => Promise.resolve(true), () => Promise.reject(Error('synthetic')), () => { throw Error('synthetic'); }].entries()) {
    state.decision = decision;
    assert.throws(() => f.call('submit', { ...f.scope, requestId: 'refusal-' + index, body: 'Synthetic', attachments: [] }), busy);
  }
  assert.equal(f.call('list', f.scope).tickets.length, 0);
});

test('default Source behavior and its frozen profile remain unchanged without a host admission callback', t => {
  const f = fixture(t), input = { ...f.scope, requestId: 'default', body: 'Synthetic', attachments: [] };
  assert.equal(f.call('submit', input).replayed, false); assert.equal(f.call('submit', input).replayed, true);
  assert.equal(DEFAULT_FEEDBACK_PROFILE.digest, '716516e51e0cab9261c737d13a0f494799c516d383a3b494d04ba606fe729a34');
});
