import test from 'node:test';
import assert from 'node:assert/strict';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';
import { CLIENT_CAPABILITIES_META_KEY, CLIENT_INFO_META_KEY, PROTOCOL_VERSION_META_KEY } from '@modelcontextprotocol/server';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { oauthNativeFixture } from './support/oauth-native-http.mjs';

const JUNE = '2025-06-18', NOVEMBER = '2025-11-25', MODERN = '2026-07-28';
const supported = [MODERN, NOVEMBER, JUNE];
const input = { title: 'Codex June / create', body: 'Private source text, distinct from title',
  idempotencyKey: 'codex-june-exact-intention' };
const counts = f => ({
  invocations: f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n),
  notes: f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n),
  spent: f.sql(f.capsFile, db => db.prepare('SELECT coalesce(sum(spent_amount),0) AS n FROM cap_budgets').get().n),
});
function success(result) {
  assert.notEqual(result.isError, true, 'successful tool result');
  assert.equal(result.content.length, 1); assert.equal(result.content[0].type, 'text');
  assert.deepEqual(JSON.parse(result.content[0].text), result.structuredContent);
  return result.structuredContent;
}
function privateMaterialAbsent(value, secrets) {
  const serialized = JSON.stringify(value);
  for (const secret of secrets) assert.equal(serialized.includes(secret), false, 'private source or credential absent');
}

test('actual SDK June negotiation uses all four tools, exact replay and current OAuth authority', { timeout: 30000 }, async t => {
  const f = await oauthNativeFixture(t), owner = nativeIdentity('June compatibility owner');
  const account = await f.bootstrap(owner);
  const connected = await f.connect(owner, account.accountId, { resource: `${f.origin}/mcp` });
  const observed = [];
  const client = new Client({ name: 'codex-june-compatibility-fixture', version: '1' }, {
    capabilities: {}, supportedProtocolVersions: [JUNE], versionNegotiation: { mode: 'legacy' },
  });
  client.onerror = () => {}; // Expected post-revoke transport refusals contain no test diagnostics.
  const transport = new StreamableHTTPClientTransport(new URL(`${f.origin}/mcp`), {
    requestInit: { headers: { authorization: `Bearer ${connected.tokens.access_token}` } },
    fetch: async (url, init) => {
      assert.equal(new URL(url).origin, f.origin, 'SDK uses only the owned MCP endpoint');
      const message = typeof init?.body === 'string' ? JSON.parse(init.body) : null;
      const response = await fetch(url, init); // Preserve the genuine SDK request and response bytes.
      observed.push({ method: message?.method ?? null, version: new Headers(init?.headers).get('mcp-protocol-version'),
        offered: message?.method === 'initialize' ? message.params.protocolVersion : null,
        status: response.status, session: response.headers.has('mcp-session-id'),
        type: response.headers.get('content-type')?.split(';')[0] ?? null });
      return response;
    },
  });
  const call = (name, args) => client.callTool({ name, arguments: args }, { timeout: 5000 });
  try {
    await client.connect(transport, { timeout: 5000 });
    assert.equal(transport.protocolVersion, JUNE);
    assert.deepEqual(observed.find(row => row.method === 'initialize'), {
      method: 'initialize', version: null, offered: JUNE, status: 200, session: false, type: 'text/event-stream',
    });
    const notification = observed.find(row => row.method === 'notifications/initialized');
    assert.equal(notification?.version, JUNE); assert.equal(notification?.status, 202);
    const list = await client.listTools(undefined, { timeout: 5000 });
    assert.deepEqual(list.tools.map(tool => tool.name), ['catalog_search', 'catalog_get', 'notes_create_draft', 'invocations_get']);
    const ru = success(await call('catalog_search', { query: 'заметка' }));
    const en = success(await call('catalog_search', { query: 'draft' }));
    const selected = ru.items.find(item => en.items.some(other => other.capabilityId === item.capabilityId && other.version === item.version));
    assert.ok(selected, 'both searches discover the same actual public version');
    const detail = success(await call('catalog_get', { capabilityId: selected.capabilityId, version: selected.version }));
    assert.equal(detail.capability.digest, selected.digest);
    const created = success(await call('notes_create_draft', input));
    assert.equal(created.invocation.status, 'succeeded'); assert.equal(created.reused, false);
    assert.equal(created.result.url, `${f.origin}/#notes/${created.result.noteId}`);
    const note = good(await f.call(owner, 'notes.get', { expectedAccountId: account.accountId, noteId: created.result.noteId })).note;
    assert.equal(note.title, input.title); assert.equal(note.body, input.body);
    const firstRead = success(await call('invocations_get', { invocationId: created.invocation.invocationId }));
    assert.deepEqual(firstRead.invocation, created.invocation);
    good(await f.call(owner, 'notes.put', { expectedAccountId: account.accountId, noteId: note.noteId,
      mutationId: 'codex-june-owner-edit', expectedRevision: 1, title: 'Later human title', body: 'Later private human text',
      items: [], pinned: false, color: 'plain', state: 'active' }));
    const conflict = await call('notes_create_draft', { ...input, body: 'A different intention with an occupied key' });
    assert.equal(conflict.isError, true); assert.equal(Object.hasOwn(conflict, 'structuredContent'), false);
    assert.deepEqual(JSON.parse(conflict.content[0].text), { error: { code: 'invocation_request_conflict' } });
    await f.keyless({ enabled: false });
    const replay = success(await call('notes_create_draft', input));
    assert.equal(replay.reused, true); assert.deepEqual(replay.invocation, created.invocation);
    const read = success(await call('invocations_get', { invocationId: created.invocation.invocationId }));
    assert.deepEqual(read.invocation, created.invocation); assert.deepEqual(read.result, created.result);
    privateMaterialAbsent([firstRead, replay, read], [input.title, input.body, 'Later human title', 'Later private human text',
      connected.tokens.access_token, connected.tokens.refresh_token, account.accountId]);
    assert.deepEqual(counts(f), { invocations: 1, notes: 1, spent: 1 });
    good(await f.call(owner, 'oauth.connections.revoke', { expectedAccountId: account.accountId, connectionId: connected.flow.connectionId }));
    for (const [name, args] of [['notes_create_draft', input], ['invocations_get', { invocationId: created.invocation.invocationId }]]) {
      const before = observed.length; let refused = false;
      try { await call(name, args); } catch { refused = true; }
      assert.equal(refused, true, 'the same SDK client observes a current-authority refusal');
      const replies = observed.slice(before).filter(row => row.method === 'tools/call');
      assert.ok(replies.length > 0 && replies.every(row => [401, 403].includes(row.status)), 'actual MCP wire rejects revoked rights');
    }
    assert.deepEqual(counts(f), { invocations: 1, notes: 1, spent: 1 });
    assert.ok(observed.filter(row => row.method && row.method !== 'initialize').every(row => row.version === JUNE));
    assert.equal(observed.some(row => row.session), false, 'legacy compatibility stays stateless');
  } finally { await client.close(); }
});

async function raw(f, token, body, version = JUNE) {
  const response = await fetch(`${f.origin}/mcp`, { method: 'POST', signal: AbortSignal.timeout(5000),
    headers: { authorization: `Bearer ${token}`, accept: 'application/json, text/event-stream', 'content-type': 'application/json',
      ...(version === null ? {} : { 'mcp-protocol-version': version }) }, body: JSON.stringify(body) });
  const text = await response.text();
  assert.ok(Buffer.byteLength(text) <= 2097152, 'finite response bound');
  const messages = response.headers.get('content-type')?.startsWith('text/event-stream')
    ? text.split(/\r?\n/u).filter(line => line.startsWith('data: ')).map(line => JSON.parse(line.slice(6)))
    : text ? [JSON.parse(text)] : [];
  return { status: response.status, messages, text, session: response.headers.has('mcp-session-id') };
}
const initialize = version => ({ jsonrpc: '2.0', id: 'raw-init', method: 'initialize', params: {
  protocolVersion: version, capabilities: {}, clientInfo: { name: 'raw-june-compatibility-fixture', version: '1' },
} });

test('June raw admission keeps headerless initialization narrow and rejects unknown or mismatched revisions', { timeout: 20000 }, async t => {
  const f = await nativeHttpFixture(t, { capabilitiesVersion: 3 }), owner = nativeIdentity('June raw owner');
  const account = await f.bootstrap(owner), identity = await f.issue(owner, account.accountId, { audience: `${f.origin}/mcp` });
  const initialized = await raw(f, identity.token, initialize(JUNE), null);
  assert.equal(initialized.status, 200); assert.equal(initialized.messages[0].result.protocolVersion, JUNE);
  assert.equal(initialized.session, false);
  const notification = { jsonrpc: '2.0', method: 'notifications/initialized' };
  const accepted = await raw(f, identity.token, notification);
  assert.equal(accepted.status, 202); assert.equal(accepted.text, '');
  for (const body of [notification, { jsonrpc: '2.0', id: 1, method: 'tools/list' },
    { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'notes_create_draft', arguments: input } }]) {
    const refused = await raw(f, identity.token, body, null);
    assert.equal(refused.status, 400); assert.equal(refused.messages[0].error.code, -32020);
  }
  for (const version of ['2025-03-26', '2099-01-01']) {
    for (const [body, header] of [[initialize(version), null], [{ jsonrpc: '2.0', id: 3, method: 'tools/list' }, version]]) {
      const refused = await raw(f, identity.token, body, header);
      assert.equal(refused.status, 400); assert.equal(refused.messages[0].error.code, -32022);
      assert.deepEqual(refused.messages[0].error.data, { supported, requested: version });
    }
  }
  const mismatch = await raw(f, identity.token, { jsonrpc: '2.0', id: 4, method: 'tools/list', params: { _meta: {
    [PROTOCOL_VERSION_META_KEY]: MODERN, [CLIENT_INFO_META_KEY]: { name: 'private-client-canary', version: '1' },
    [CLIENT_CAPABILITIES_META_KEY]: {},
  } } });
  assert.equal(mismatch.status, 400); assert.equal(mismatch.messages[0].error.code, -32020);
  assert.equal(mismatch.messages[0].error.message, 'invalid_input');
  assert.equal(Object.hasOwn(mismatch.messages[0].error, 'data'), false);
  privateMaterialAbsent(mismatch, ['private-client-canary', identity.token, account.accountId]);
  assert.deepEqual(counts(f), { invocations: 0, notes: 0, spent: 0 });
});
