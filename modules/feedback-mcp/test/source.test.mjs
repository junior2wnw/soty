import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createFeedbackSource } from '../source.mjs';
import { digest } from '../../source-app/server/wire.mjs';
import { feedbackSourceFixture as fixture } from './support/source-fixture.mjs';

test('Source is disabled without configuration and never reads service', async () => {
  const source = createFeedbackSource({ service: new Proxy({}, { get() { throw Error('must not read'); } }) });
  assert.equal(source.enabled, false); assert.equal(await source.handleRequest({}, {}), false); source.close();
});

test('real Root witness: empty own workspace, create/read/receipt/restart and foreign denial', async t => {
  const f = await fixture(t);
  f.forge(true); assert.ok((await f.request()).status >= 400); f.forge(false);
  assert.ok((await f.request(undefined, undefined, false)).status >= 400);
  let ref = await f.contextRef();
  const query = input => f.request('/api/embed/query', { requestId: 'source-read-00000001', contextRef: ref, input });
  const empty = (await query({ operation: 'projects_list' })).body.data;
  assert.deepEqual(empty.projects, []); assert.equal(empty.nextCursor, '');
  const input = { operation: 'project_create', name: 'Own queue', publicSubmit: false }, requestId = 'source-create-00000001';
  const result = await f.request('/api/embed/invoke', { requestId, contextRef: ref, input });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const listed = await query({ operation: 'projects_list' }); assert.equal(listed.body.data.projects.length, 1);
  const projectId = listed.body.data.projects[0].id;
  const proof = await f.request('/api/embed/receipt', { requestId, contextRef: ref, input: { inputDigest: digest(input) } });
  assert.equal(proof.status, 200, JSON.stringify(proof.body)); assert.equal(proof.body.data.outcome, 'committed');
  const again = await f.request('/api/embed/invoke', { requestId, contextRef: ref, input }); assert.equal(again.status, 200);
  assert.equal((await query({ operation: 'projects_list' })).body.data.projects.length, 1);
  assert.ok((await f.request('/api/embed/invoke', { requestId, contextRef: ref, input: { ...input, name: 'Conflicting queue' } })).status >= 400);
  const ticketInput = { operation: 'ticket_submit', projectId, body: 'Synthetic Source report' }, ticketRequest = 'source-ticket-00000001';
  const ticket = await f.request('/api/embed/invoke', { requestId: ticketRequest, contextRef: ref, input: ticketInput });
  assert.equal(ticket.status, 200, JSON.stringify(ticket.body));
  const ticketProof = await f.request('/api/embed/receipt', { requestId: ticketRequest, contextRef: ref, input: { inputDigest: digest(ticketInput) } });
  assert.equal(ticketProof.status, 200, JSON.stringify(ticketProof.body)); assert.equal(ticketProof.body.data.outcome, 'committed');
  const tickets = await query({ operation: 'tickets_list', projectId }); assert.equal(tickets.status, 200, JSON.stringify(tickets.body));
  assert.equal(tickets.body.data.tickets.length, 1); assert.equal(JSON.stringify(tickets.body).includes('_hostProof'), false);
  assert.ok((await f.request('/api/embed/receipt', { requestId: ticketRequest, contextRef: ref, input: { inputDigest: 'f'.repeat(64) } })).status >= 400);
  f.restart();
  // Existing session survives encrypted storage restart; acquire its new SDK context lease.
  ref = await f.contextRef();
  const durable = await f.request('/api/embed/receipt', { requestId, contextRef: ref, input: { inputDigest: digest(input) } });
  // A new context lease must not falsely rebind an original mutation's proof.
  assert.equal(durable.body.error?.code, 'source_workspace_receipt_denied');
  assert.deepEqual(f.service().sourceReceipt(f.ownActor(), { requestId, inputDigest: digest(input) }), proof.body.data);
  assert.deepEqual(f.service().sourceReceipt(f.ownActor(), { requestId: ticketRequest, inputDigest: digest(ticketInput) }), ticketProof.body.data);
  f.newContext('person-two'); ref = await f.contextRef();
  assert.deepEqual((await query({ operation: 'projects_list' })).body.data.projects, []);
  assert.ok((await query({ operation: 'integration_get', projectId })).status >= 400);
  assert.ok((await f.request('/api/embed/invoke', { requestId: 'source-foreign-000001', contextRef: ref, input: ticketInput })).status >= 400);
  assert.ok((await f.request('/api/embed/query', { requestId: 'source-select-000001', input: { operation: 'soty.context.select', nativeId: projectId, generation: 1 } })).status >= 400);
  f.revoke(); assert.ok((await query({ operation: 'projects_list' })).status >= 400);
});

test('embedded browser client uses maintained frame fetch and reconciles lost ACK without a second invoke', async t => {
  const f = await fixture(t), calls = [];
  const sdkUrl = new URL('../../source-app/browser/frame-fetch.mjs', import.meta.url).href;
  const source = (await readFile(new URL('../public/source-client.js', import.meta.url), 'utf8'))
    .replace("'./source-frame-fetch.js'", JSON.stringify(sdkUrl));
  const { createFeedbackSourceClient } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
  let loseAck = true;
  const client = createFeedbackSourceClient({ href: 'https://feedback.example/embed?sotyFrame=' + 'a'.repeat(43),
    async fetch(path, options) {
      assert.equal(options.credentials, 'same-origin'); assert.equal(options.redirect, 'error');
      assert.equal(new Headers(options.headers).get('x-soty-frame'), 'a'.repeat(43));
      assert.equal(new Headers(options.headers).has('authorization'), false);
      const input = JSON.parse(options.body); calls.push({ path, input });
      const result = await f.request(path, input);
      if (path.endsWith('/invoke') && loseAck) { loseAck = false; throw Error('Synthetic lost ACK after commit'); }
      return new Response(JSON.stringify(result.body), { status: result.status, headers: { 'content-type': 'application/json' } });
    } });
  const empty = await client.call('projects_list'); assert.deepEqual(empty.projects, []); assert.equal(empty.nextCursor, null);
  const args = { name: 'Browser queue', publicSubmit: false, requestId: 'browser-source-create-001' };
  const [made, concurrent] = await Promise.all([client.call('project_create', args), client.call('project_create', args)]);
  assert.equal(made.project.name, args.name); assert.equal(concurrent.project.id, made.project.id);
  assert.equal((await client.call('project_create', args)).project.id, made.project.id);
  assert.equal(calls.filter(row => row.path.endsWith('/invoke')).length, 1);
  assert.ok(calls.some(row => row.path.endsWith('/receipt')));
  await assert.rejects(client.call('project_create', { ...args, name: 'Changed' }), { code: 'request_conflict' });
  await assert.rejects(client.call('arbitrary_owner_operation', {}), { code: 'feedback_source_operation_denied' });
  const ticket = await client.call('ticket_submit', { projectId: made.project.id, body: 'Browser ticket', requestId: 'browser-source-ticket-001' });
  assert.equal(ticket.ticket.body, 'Browser ticket');
  assert.equal((await client.call('tickets_list', { projectId: made.project.id })).tickets.length, 1);
  const image = await readFile(new URL('../../feedback/test/fixtures/chrome.png', import.meta.url));
  const media = f.service().execute(f.ownActor(), 'ticket_submit', { projectId: made.project.id,
    requestId: 'browser-media-seed-001', body: 'Selected image',
    attachments: [{ kind: 'image', name: 'synthetic.png', mimeType: 'image/png', dataBase64: image.toString('base64') }] });
  const detail = await client.call('ticket_get', { projectId: made.project.id, ticketId: media.ticket.id, messageOffset: 0, messageLimit: 1 });
  assert.equal(detail.ticket.attachments.length, 1);
  assert.equal(Object.hasOwn(detail.ticket.attachments[0], 'dataBase64'), false);
  assert.equal(Object.hasOwn(detail, 'content'), false);
  assert.equal(detail.messagesPage.nextOffset, null);
  assert.ok(Buffer.byteLength(JSON.stringify(detail)) < 65536);
  assert.equal(calls.filter(row => row.input.input?.operation === 'attachment_chunk').length, 0);
  const loaded = await client.call('attachment_get', { projectId: made.project.id, ticketId: media.ticket.id, attachmentId: detail.ticket.attachments[0].id });
  assert.deepEqual(Buffer.from(loaded.attachment.dataBase64, 'base64'), image);
  assert.ok(calls.some(row => row.input.input?.operation === 'attachment_chunk'));
  assert.ok(calls.every(row => row.path.startsWith('/api/embed/')));
});

test('expired Root context stays denied; a fresh verified context creates a new bounded actor', async t => {
  const f = await fixture(t); await f.contextRef();
  const oldSignedRequest = await f.request(undefined, undefined, true, true); f.advance(300001);
  assert.ok((await oldSignedRequest()).status >= 400);
  await assert.rejects(f.request(), /./);
  f.newContext(); await f.contextRef();
  const ref = await f.contextRef();
  const result = await f.request('/api/embed/query', { requestId: 'source-refreshed-001', contextRef: ref, input: { operation: 'projects_list' } });
  assert.equal(result.status, 200, JSON.stringify(result.body));
});
