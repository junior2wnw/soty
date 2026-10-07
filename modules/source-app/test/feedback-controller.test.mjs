import test from 'node:test';
import assert from 'node:assert/strict';
import { createSourceFeedbackController } from '../browser/feedback-controller.mjs';

const context = digest => ({ schema: 'soty.source-feedback.context.v1', bindingDigest: digest.repeat(64), ready: true, canSubmit: true, recipientLabel: 'Synthetic Native support' });
test('unknown ACK retries exactly frozen request/body; another Native context never receives it', async () => {
  let scope = 'a', calls = [], lost = true;
  const controller = createSourceFeedbackController({ requestId: () => 'request-fixture-0001', api: {
    context: async () => context(scope), submit: async args => { calls.push(structuredClone(args)); if (lost) { lost = false; throw new Error('wire_unknown'); }
      return { requestId: args.requestId, ticket: { id: 'native-ticket', status: 'received' } }; } } });
  await controller.open(); controller.setDraft({ body: 'Original', attachments: [] }); assert.equal(await controller.send(), false);
  assert.equal(controller.setDraft({ body: 'Changed while unknown', attachments: [] }), false);
  assert.equal(await controller.send(), true); assert.deepEqual(calls[0], calls[1]);
  controller.setDraft({ body: 'Private A draft', attachments: [] }); scope = 'b'; await controller.open();
  assert.equal(controller.snapshot().draft.body, ''); scope = 'a'; await controller.open(); assert.equal(controller.snapshot().draft.body, 'Private A draft');
});
test('Native denial clears pending permission, stale ACK/profile switch cannot confirm or retarget', async () => {
  let current = true, release, entered;
  const gate = new Promise(done => { release = done; }), reached = new Promise(done => { entered = done; });
  const controller = createSourceFeedbackController({ isCurrent: () => current, requestId: () => 'request-fixture-0002', api: {
    context: async () => context('a'), submit: async args => { entered(); await gate; return { requestId: args.requestId, ticket: { id: 'old-context' } }; } } });
  await controller.open(); controller.setDraft({ body: 'Private draft', attachments: [] }); const pending = controller.send(); await reached;
  current = false; controller.invalidate(); release(); assert.equal(await pending, false); assert.equal(controller.snapshot().ticket, null);
  assert.equal(controller.snapshot().state, 'login_required');
});
