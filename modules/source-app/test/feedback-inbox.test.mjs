import test from 'node:test';
import assert from 'node:assert/strict';
import { createSourceFeedbackInbox } from '../browser/feedback-inbox.mjs';

const ticket = (extra = {}) => ({ id: 'ticket-one', revision: 1, body: 'Synthetic private issue', status: 'received',
  canReply: true, canManage: false, canAccept: false, messages: [], attachments: [], ...extra });
const context = bindingDigest => ({ ready: true, bindingDigest, recipientLabel: 'Владелец приложения' });
function fixture(extra = {}) {
  let value = ticket(), calls = [];
  const api = { context: async () => context('a'.repeat(64)), list: async () => ({ tickets: [value], nextCursor: null }),
    get: async () => ({ ticket: value }), reply: async args => { calls.push(args); value = ticket({ revision: 2 }); return { requestId: args.requestId, ticket: value }; }, ...extra };
  const inbox = createSourceFeedbackInbox({ api, requestId: () => 'same-intent-request-0001' });
  return { inbox, api, calls };
}
test('existing SDK reads a private thread and sends the permitted reply without a processor', async () => {
  const { inbox, calls } = fixture(); await inbox.open(); await inbox.select('ticket-one');
  inbox.setReply('Synthetic clarification'); assert.equal(await inbox.write('reply'), true);
  assert.equal(calls.length, 1); assert.equal(calls[0].expectedRevision, 1);
  assert.equal(inbox.snapshot().ticket.revision, 2); assert.equal(inbox.snapshot().replyDraft, ''); inbox.dispose();
});
test('lost reply ACK retains exactly the same immutable intent for explicit reconciliation', async () => {
  const received = []; let attempts = 0;
  const { inbox } = fixture({ reply: async args => { received.push(args); if (++attempts === 1) throw new Error('lost');
    return { requestId: args.requestId, ticket: ticket({ revision: 2 }) }; } });
  await inbox.open(); await inbox.select('ticket-one'); inbox.setReply('Synthetic held reply');
  assert.equal(await inbox.write('reply'), false); assert.equal(inbox.snapshot().state, 'unknown');
  assert.equal(inbox.setReply('A different body'), false); assert.equal(await inbox.select('other-ticket'), false);
  assert.equal(await inbox.write(), true); assert.equal(received.length, 2); assert.equal(received[0], received[1]); inbox.dispose();
});
test('owner status and reporter acceptance require their existing distinct permission flags', async () => {
  let writes = 0;
  const { inbox, api } = fixture({ status: async args => { writes++; return { requestId: args.requestId, ticket: ticket({ status: args.status, revision: 2, canAccept: true }) }; },
    accept: async args => { writes++; return { requestId: args.requestId, ticket: ticket({ status: 'resolved', revision: 3 }) }; } });
  await inbox.open(); await inbox.select('ticket-one');
  assert.equal(await inbox.write('status', 'ready_to_check'), false); assert.equal(await inbox.write('accept'), false); assert.equal(writes, 0);
  api.get = async () => ({ ticket: ticket({ canManage: true }) }); await inbox.select('ticket-one');
  assert.equal(await inbox.write('status', 'resolved'), false); assert.equal(await inbox.write('status', 'ready_to_check'), true);
  assert.equal(await inbox.write('accept'), true); assert.equal(writes, 2); inbox.dispose();
});
test('revocation clears private rendering and a late read cannot reopen the old account', async () => {
  let release, entered; const began = new Promise(resolve => { entered = resolve; });
  const { inbox, api } = fixture(); await inbox.open(); await inbox.select('ticket-one'); inbox.setReply('Private draft');
  api.get = () => new Promise(resolve => { release = resolve; entered(); }); const read = inbox.select('ticket-one');
  await began; inbox.invalidate(); release({ ticket: ticket() }); await read;
  assert.equal(inbox.snapshot().ticket, null); assert.deepEqual(inbox.snapshot().tickets, []); assert.equal(inbox.snapshot().replyDraft, '');
  api.context = async () => { throw Object.assign(new Error('denied'), { status: 403 }); };
  await inbox.open(); assert.equal(inbox.snapshot().state, 'login_required'); inbox.dispose();
});
test('fresh context prevents a retained reply from moving to a different Source binding', async () => {
  const { inbox, api, calls } = fixture(); await inbox.open(); await inbox.select('ticket-one'); inbox.setReply('Former project draft');
  api.context = async () => context('b'.repeat(64)); assert.equal(await inbox.write('reply'), false);
  assert.equal(calls.length, 0); assert.equal(inbox.snapshot().ticket, null); assert.deepEqual(inbox.snapshot().tickets, []); inbox.dispose();
});
test('a binding change during a completed mutation withholds its old private result', async () => {
  const { inbox, api } = fixture(); await inbox.open(); await inbox.select('ticket-one'); inbox.setReply('Synthetic own reply');
  api.reply = async args => { api.context = async () => context('b'.repeat(64)); return { requestId: args.requestId, ticket: ticket({ revision: 2 }) }; };
  assert.equal(await inbox.write('reply'), false); assert.equal(inbox.snapshot().ticket, null); assert.equal(inbox.snapshot().state, 'login_required'); inbox.dispose();
});
test('changed binding drops former thread data; revision conflict keeps the reply draft', async () => {
  const { inbox, api } = fixture({ reply: async () => { throw Object.assign(new Error('conflict'), { status: 409 }); } });
  await inbox.open(); await inbox.select('ticket-one'); inbox.setReply('Retained on conflict');
  assert.equal(await inbox.write('reply'), false); assert.equal(inbox.snapshot().replyDraft, 'Retained on conflict'); assert.equal(inbox.snapshot().pending, false);
  api.context = async () => context('b'.repeat(64)); api.list = async () => ({ tickets: [], nextCursor: null });
  await inbox.open(); assert.equal(inbox.snapshot().ticket, null); assert.deepEqual(inbox.snapshot().tickets, []); inbox.dispose();
});
test('existing cursor continues the queue without duplicating displayed tickets', async () => {
  const { inbox, api } = fixture(); api.list = async () => ({ tickets: [ticket()], nextCursor: 'first-page' }); await inbox.open();
  api.list = async ({ cursor }) => { assert.equal(cursor, 'first-page'); return { tickets: [ticket(), ticket({ id: 'ticket-two' })], nextCursor: null }; };
  assert.equal(await inbox.more(), true); assert.deepEqual(inbox.snapshot().tickets.map(value => value.id), ['ticket-one', 'ticket-two']); inbox.dispose();
});
test('known write ACK refreshes original media bytes; a read failure never queues the write again', async () => {
  let failRead = false, writes = 0;
  const media = { kind: 'audio', name: 'synthetic.ogg', mimeType: 'audio/ogg', byteLength: 1 };
  const { inbox } = fixture({ get: async () => { if (failRead) throw new Error('read unavailable');
    return { ticket: ticket({ attachments: [{ ...media, dataBase64: 'AA==' }] }) }; },
    reply: async args => { writes++; failRead = true; return { requestId: args.requestId, ticket: ticket({ revision: 2, attachments: [media] }) }; } });
  await inbox.open(); await inbox.select('ticket-one'); inbox.setReply('Synthetic reply');
  assert.equal(await inbox.write('reply'), true); assert.equal(writes, 1); assert.equal(inbox.snapshot().pending, false);
  assert.equal(inbox.snapshot().state, 'unavailable'); assert.equal(inbox.snapshot().replyDraft, '');
  inbox.dispose();
});
