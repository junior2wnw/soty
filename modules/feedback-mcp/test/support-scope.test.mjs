import test from 'node:test';
import assert from 'node:assert/strict';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';
import { httpFixture } from './support/http-fixture.mjs';

const good = result => { assert.notEqual(result.isError, true); return result.structuredContent; };
const errorCode = result => { assert.equal(result.isError, true); return JSON.parse(result.content[0].text).error.code; };
async function mcp(t, origin, token) {
  const client = new Client({ name: 'disposable-support-scope', version: '1' });
  await client.connect(new StreamableHTTPClientTransport(new URL(origin + '/mcp'), {
    requestInit: { headers: { Authorization: 'Bearer ' + token } },
  }));
  t.after(() => client.close());
  return { client, call: (name, args = {}) => client.callTool({ name: 'feedback_' + name, arguments: args }) };
}

// Actual MCP SDK/HTTP/Feedback SQLite. Only the existing fixture's identity
// provider is synthetic; no live account, queue, grant, model or message is used.
test('one management-project credential cannot absorb same-owner or foreign queues; reply persists and revocation closes access', async t => {
  const f = await httpFixture(t);
  const ownerToken = f.service.issueCredential(f.actors.alice, { label: 'Disposable owner' }).token;
  const otherToken = f.service.issueCredential(f.actors.bob, { label: 'Disposable placer' }).token;
  const owner = await mcp(t, f.origin, ownerToken), other = await mcp(t, f.origin, otherToken);
  const create = (peer, requestId, name, publicSubmit = false) => peer.call('project_create', { requestId, name, publicSubmit }).then(good);
  const { project: central } = await create(owner, 'support-central', 'Platform support', true);
  const { project: unrelated } = await create(owner, 'support-unrelated', 'Same owner, separate permission');
  const { project: placed } = await create(other, 'support-placed', 'Foreign placer support');
  assert.equal((await create(owner, 'support-central', 'Platform support', true)).project.id, central.id);

  const token = f.service.issueCredential(f.actors.alice, { label: 'One support project', projectId: central.id }).token;
  const manager = await mcp(t, f.origin, token);
  // Tool listing is availability, not authority over every private queue.
  assert.equal((await manager.client.listTools()).tools.length, 10);
  assert.deepEqual(good(await manager.call('projects_list')).projects.map(row => row.id), [central.id]);
  for (const projectId of [unrelated.id, placed.id]) {
    assert.equal(errorCode(await manager.call('tickets_list', { projectId })), 'project_unavailable');
    assert.equal(errorCode(await manager.call('integration_get', { projectId })), 'project_unavailable');
  }
  assert.equal(errorCode(await other.call('tickets_list', { projectId: central.id })), 'project_unavailable');
  assert.equal(errorCode(await manager.call('tickets_list', { projectId: central.id, owner: true })), 'invalid_arguments');

  const guest = await f.json('/api/public/' + central.id + '/session', { body: {} });
  assert.equal(guest.status, 201);
  const sent = await f.json('/api/public/' + central.id + '/tools/ticket_submit', {
    headers: { Authorization: 'Bearer ' + guest.data.token, Origin: f.origin },
    body: { requestId: 'support-report', body: 'Synthetic issue; this text grants no access to another queue.' },
  });
  assert.equal(sent.status, 200);
  const ticketId = sent.data.ticket.id, page = good(await manager.call('tickets_list', { projectId: central.id }));
  assert.deepEqual(page.tickets.map(row => row.id), [ticketId]);
  const args = { projectId: central.id, ticketId, requestId: 'support-reply', expectedRevision: sent.data.ticket.revision, body: 'Saved support reply' };
  const reply = good(await manager.call('ticket_reply', args));
  assert.equal(reply.replayed, false); assert.equal(good(await manager.call('ticket_reply', args)).replayed, true);
  const reloaded = await mcp(t, f.origin, token);
  const detail = good(await reloaded.call('ticket_get', { projectId: central.id, ticketId }));
  assert.equal(detail.ticket.messages.length, 1); assert.equal(detail.ticket.messages[0].body, args.body);
  assert.equal(detail.ticket.revision, reply.ticket.revision);

  const readonly = f.service.issueCredential(f.actors.alice, { label: 'Read one project', projectId: central.id, readOnly: true }).token;
  const reader = await mcp(t, f.origin, readonly);
  assert.equal(good(await reader.call('ticket_get', { projectId: central.id, ticketId })).ticket.id, ticketId);
  assert.equal(errorCode(await reader.call('ticket_reply', { ...args, requestId: 'readonly-reply', expectedRevision: reply.ticket.revision })), 'read_only');
  f.service.revokeCredential(f.actors.alice, token);
  await assert.rejects(manager.call('ticket_get', { projectId: central.id, ticketId }), error => /"authentication_required"/u.test(error.message));
  assert.equal(good(await reader.call('ticket_get', { projectId: central.id, ticketId })).ticket.messages.length, 1);
});
