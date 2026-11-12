import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { nativeHttpFixture, nativeIdentity, good } from '../../../server/test/support/native-capability-http.mjs';
import { connectorKey } from '../../apps/server/protocol.mjs';
import { appFeedbackResource } from '../../../server/app-feedback-capabilities.js';

test('signed Root accepts private owner video while unchanged v1 MCP grants return an explicit projection refusal', { timeout: 20000 }, async t => {
  const f = await nativeHttpFixture(t, { appOrigins: true });
  const administrator = nativeIdentity('First account and second app owner'), owner = nativeIdentity('Independent app owner'), reporter = nativeIdentity('Reporter');
  const accounts = new Map();
  for (const person of [administrator, owner, reporter]) accounts.set(person, (await f.bootstrap(person)).accountId);
  const apps = [];
  for (const [index, person] of [owner, administrator].entries()) {
    const identity = { linkId: 'video-wire-link-' + index, hostDeviceId: 'video-wire-host-' + index, connectorId: 'video-wire-connector-' + index };
    // Disposable connector metadata only; registration and every user/agent operation use signed Root authority.
    const db = new DatabaseSync(join(f.directory, 'apps', 'registry.sqlite'));
    try { db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(connectorKey(identity), accounts.get(person), JSON.stringify(identity), 'Synthetic video device', Date.now()); }
    finally { db.close(); }
    apps.push(good(await f.call(person, 'apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId,
      name: 'Video private app ' + index, port: 42002, grants: { accountIds: [...accounts.values()].filter(id => id !== accounts.get(person)), communityIds: [] } })).app.id);
  }
  const [appId, foreignId] = apps;
  const context = good(await f.call(reporter, 'apps.feedback.context', { appId })).context;
  assert.equal(context.capabilities.video, true);
  const scope = { appId, installationId: context.installationId };
  const text = good(await f.call(reporter, 'apps.feedback.submit', { ...scope, requestId: 'text-before-video', body: 'Existing text remains readable', attachments: [] }));
  const accountId = accounts.get(owner);
  const principal = good(await f.call(owner, 'access.principals.create', { expectedAccountId: accountId, label: 'V1 video compatibility control' })).principal;
  const grant = good(await f.call(owner, 'access.grants.issue', { expectedAccountId: accountId, principalId: principal.id,
    capabilities: ['list', 'get'].map(operation => ({ capabilityId: 'apps.feedback.' + operation, version: 1 })),
    resources: ['apps.feedback', appFeedbackResource(appId)], effects: [], recipients: ['soty:app-feedback'],
    expiresAt: Date.now() + 600000, allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 10 } })).grant;
  const credential = await f.issue(owner, accountId, { principalId: principal.id, grantId: grant.id, audience: f.origin + '/mcp' });
  const reference = operation => f.app.locals.capabilitiesService.external.contracts.find(entry => entry.capabilityId === 'apps.feedback.' + operation);
  const originalGet = reference('get'), originalList = reference('list');
  let sequence = 0;
  const query = (operation, input) => f.http('/mcp', { method: 'POST', token: credential.token,
    headers: { accept: 'application/json, text/event-stream', 'mcp-protocol-version': '2026-07-28', 'mcp-method': 'tools/call', 'mcp-name': 'apps_query' },
    body: JSON.stringify({ jsonrpc: '2.0', id: ++sequence, method: 'tools/call', params: { name: 'apps_query',
      arguments: { reference: reference(operation), input, idempotencyKey: 'video-query-' + sequence }, _meta: {
        'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientInfo': { name: 'video-compatibility-test', version: '1' },
        'io.modelcontextprotocol/clientCapabilities': {},
      } } }) });
  const oldGet = await query('get', { ...scope, ticketId: text.receipt.ticketId });
  assert.equal(oldGet.status, 200); assert.equal(oldGet.body.result.isError, undefined);
  assert.equal(oldGet.body.result.structuredContent.result.ticket.body, 'Existing text remains readable');
  const bytes = readFileSync(new URL('../../materials/test/fixtures/video.webm', import.meta.url));
  const args = { ...scope, requestId: 'signed-video-once', body: 'Private video for the independent owner',
    attachments: [{ kind: 'video', name: 'selected.webm', mimeType: 'video/webm', dataBase64: bytes.toString('base64') }] };
  const sent = good(await f.call(reporter, 'apps.feedback.submit', args));
  const selected = { ...scope, ticketId: sent.receipt.ticketId };
  assert.equal(good(await f.call(owner, 'apps.feedback.get', selected)).ticket.attachments[0].dataBase64, args.attachments[0].dataBase64);
  assert.equal((await f.call(administrator, 'apps.feedback.get', selected)).error.code, 'feedback_ticket_unavailable');
  assert.deepEqual(good(await f.call(administrator, 'apps.feedback.list', scope)).tickets, []);
  const foreignContext = good(await f.call(administrator, 'apps.feedback.context', { appId: foreignId })).context;
  assert.notEqual(foreignContext.installationId, context.installationId);
  for (const operation of ['list', 'get']) {
    const response = await query(operation, operation === 'list' ? scope : selected);
    assert.equal(response.status, 200); assert.equal(response.body.result.isError, true);
    assert.equal(JSON.parse(response.body.result.content[0].text).error.code, 'unsupported_media_projection');
    assert.equal(JSON.stringify(response.body).includes(args.body), false);
    assert.equal(JSON.stringify(response.body).includes(args.attachments[0].dataBase64), false);
  }
  await f.restart();
  assert.deepEqual(reference('get'), originalGet); assert.deepEqual(reference('list'), originalList);
  assert.equal(good(await f.call(reporter, 'apps.feedback.submit', args)).replayed, true);
  assert.equal(good(await f.call(owner, 'apps.feedback.get', selected)).ticket.attachments[0].dataBase64, args.attachments[0].dataBase64);
  assert.equal((await query('get', { ...scope, ticketId: text.receipt.ticketId })).body.result.isError, undefined);
  const denied = await query('list', { appId: foreignId, installationId: foreignContext.installationId });
  assert.equal(denied.body.result.isError, true); assert.equal(JSON.parse(denied.body.result.content[0].text).error.code, 'access_denied');
});
