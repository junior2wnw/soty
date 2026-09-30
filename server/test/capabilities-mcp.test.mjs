import test from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:http';
import express from 'express';
import { Server, PROTOCOL_VERSION_META_KEY, SERVER_INFO_META_KEY, CLIENT_INFO_META_KEY, CLIENT_CAPABILITIES_META_KEY } from '@modelcontextprotocol/server';
import { AjvJsonSchemaValidator } from '@modelcontextprotocol/server/validators/ajv';
import { attachCapabilitiesMcp } from '../capabilities-mcp.js';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { oauthNativeFixture } from './support/oauth-native-http.mjs';

const MODERN = '2026-07-28', LEGACY = '2025-11-25';
const input = { title: 'План / MCP', body: 'Закрытый синтетический текст', idempotencyKey: 'mcp-first-intention' };
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const validators = new AjvJsonSchemaValidator();
function envelope(method, params = {}, version = MODERN, id = 1) {
  return { jsonrpc: '2.0', id, method, params: { ...params, ...(version === MODERN ? { _meta: {
    [PROTOCOL_VERSION_META_KEY]: MODERN, [CLIENT_INFO_META_KEY]: { name: 'wire-fixture', version: '1' },
    [CLIENT_CAPABILITIES_META_KEY]: {}, ...params._meta,
  } } : {}) } };
}
function wire(f, token, body, { version = MODERN, method = 'POST', path = '/mcp', headers = {}, onRequest, onHeaders } = {}) {
  const message = typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = request(f.origin, { path, method, headers: {
      ...(token ? { authorization: `Bearer ${token}` } : {}), accept: 'application/json, text/event-stream',
      'content-type': 'application/json', ...(version ? { 'mcp-protocol-version': version } : {}),
      ...(version === MODERN && body && typeof body === 'object' && !Buffer.isBuffer(body) ? {
        'mcp-method': body.method, ...(body.method === 'tools/call' ? { 'mcp-name': body.params?.name } : {}),
      } : {}), ...headers,
    } }, res => {
      onHeaders?.();
      const chunks = []; let bytes = 0;
      res.on('data', chunk => { bytes += chunk.length; if (bytes > 2097152) { req.destroy(new Error('test_response_bound')); return; } chunks.push(chunk); });
      res.once('error', reject);
      res.once('end', () => {
        try {
          const text = Buffer.concat(chunks).toString('utf8');
          const messages = /^text\/event-stream/u.test(res.headers['content-type'] || '')
            ? text.split(/\r?\n/u).filter(line => line.startsWith('data: ')).map(line => JSON.parse(line.slice(6)))
            : text ? [JSON.parse(text)] : [];
          resolve({ status: res.statusCode, headers: res.headers, messages, bytes, text });
        } catch { reject(new Error('test_response_not_protocol')); }
      });
    });
    req.once('error', reject); onRequest?.(req); req.end(message);
  });
}
const call = (f, token, name, args = {}, version = MODERN) => wire(f, token, envelope('tools/call', { name, arguments: args }, version), { version });
function result(response) {
  assert.equal(response.status, 200, 'protocol response status');
  assert.equal(response.messages.length, 1, 'one finite protocol result');
  assert.equal(response.messages[0].error, undefined, 'no protocol error');
  return response.messages[0].result;
}
function success(response, schema) {
  const value = result(response); assert.notEqual(value.isError, true, 'successful tool');
  assert.equal(value.content.length, 1); assert.equal(value.content[0].type, 'text');
  assert.deepEqual(JSON.parse(value.content[0].text), value.structuredContent);
  if (schema) assert.equal(validators.getValidator(schema)(value.structuredContent).valid, true, 'advertised output schema');
  return value.structuredContent;
}
function toolError(response, code) {
  const value = result(response); assert.equal(value.isError, true);
  assert.equal(Object.hasOwn(value, 'structuredContent'), false);
  assert.deepEqual(JSON.parse(value.content[0].text), { error: { code } });
}
async function fixture(t, options) {
  const f = await nativeHttpFixture(t, options), owner = nativeIdentity('MCP owner'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId, { audience: `${f.origin}/mcp` });
  return { ...f, get app() { return f.app; }, owner, account, identity };
}
const countNotes = f => f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n);
function absent(response, values) { for (const value of values) assert.equal(response.text.includes(value), false, 'private material absent'); }

test('actual SDK modern discovery and legacy initialize/list expose four checked tools while execution is disabled', async t => {
  const f = await fixture(t, { enabled: false });
  const modern = result(await wire(f, f.identity.token, envelope('server/discover')));
  assert.ok(modern._meta?.[SERVER_INFO_META_KEY]); assert.ok(modern.capabilities.tools);
  const initialized = result(await wire(f, f.identity.token, envelope('initialize', {
    protocolVersion: LEGACY, capabilities: {}, clientInfo: { name: 'legacy-fixture', version: '1' },
  }, LEGACY), { version: null }));
  assert.equal(initialized.protocolVersion, LEGACY);
  for (const version of [MODERN, LEGACY]) {
    const listed = await wire(f, f.identity.token, envelope('tools/list', {}, version), { version });
    const tools = result(listed).tools;
    assert.deepEqual(tools.map(tool => tool.name), ['catalog_search', 'catalog_get', 'notes_create_draft', 'invocations_get']);
    assert.equal(listed.headers['mcp-session-id'], undefined);
    assert.equal(listed.headers['cache-control'], 'no-store');
    assert.match(listed.headers['content-type'], version === MODERN ? /^application\/json/u : /^text\/event-stream/u);
    const found = success(await call(f, f.identity.token, 'catalog_search', { query: 'заметка' }, version), tools[0].outputSchema);
    assert.equal(found.scope, 'public');
    const detail = success(await call(f, f.identity.token, 'catalog_get', { capabilityId: 'notes.createDraft', version: 1 }, version), tools[1].outputSchema);
    assert.equal(detail.capability.digest, '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204');
    assert.equal(detail.capability.executionEnabled, false);
    toolError(await call(f, f.identity.token, 'notes_create_draft', input, version), 'service_unavailable');
  }
  assert.equal(countNotes(f), 0);
});

test('actual Notes effect and content-free receipt use the same domain through both protocol eras', async t => {
  const f = await fixture(t);
  for (const version of [MODERN, LEGACY]) {
    const args = { ...input, idempotencyKey: `mcp-create-${version}` };
    const schemas = result(await wire(f, f.identity.token, envelope('tools/list', {}, version), { version })).tools;
    const createdResponse = await call(f, f.identity.token, 'notes_create_draft', args, version);
    const created = success(createdResponse, schemas[2].outputSchema);
    assert.equal(created.invocation.status, 'succeeded'); assert.equal(created.reused, false);
    assert.equal(created.result.url, `${f.origin}/#notes/${created.result.noteId}`);
    const note = good(await f.call(f.owner, 'notes.get', { expectedAccountId: f.account.accountId, noteId: created.result.noteId })).note;
    assert.equal(note.title, input.title); assert.equal(note.body, input.body);
    const replay = success(await call(f, f.identity.token, 'notes_create_draft', args, version));
    assert.equal(replay.reused, true); assert.deepEqual(replay.invocation, created.invocation);
    const readResponse = await call(f, f.identity.token, 'invocations_get', { invocationId: created.invocation.invocationId }, version);
    assert.deepEqual(success(readResponse, schemas[3].outputSchema).invocation, created.invocation);
    absent(readResponse, [input.title, input.body, f.identity.token, f.account.accountId]);
    toolError(await call(f, f.identity.token, 'notes_create_draft', { ...args, body: 'Different' }, version), 'invocation_request_conflict');
  }
  assert.equal(countNotes(f), 2);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT spent_amount FROM cap_budgets').get().spent_amount), 2);
});

test('tool errors stay text-only and schemas do not expand the pinned Unicode or document byte profile', async t => {
  const f = await fixture(t);
  for (const version of [MODERN, LEGACY]) {
    toolError(await call(f, f.identity.token, 'catalog_get', { capabilityId: 'unknown.private', version: 1 }, version), 'not_found');
    toolError(await call(f, f.identity.token, 'catalog_search', { cursor: 'forged' }, version), 'cursor_invalid');
    for (const args of [{ ...input, title: '\ud800' }, { ...input, title: '😀'.repeat(81) }, { ...input, accountId: f.account.accountId }])
      toolError(await call(f, f.identity.token, 'notes_create_draft', args, version), 'invalid_input');
    toolError(await call(f, f.identity.token, 'notes_create_draft', { ...input, title: '', body: '中'.repeat(87371) + 'x'.repeat(9) }, version), 'payload_too_large');
    const unknown = await call(f, f.identity.token, 'unknown_tool', {}, version);
    assert.equal(unknown.messages[0].error.code, -32602);
  }
  assert.equal(countNotes(f), 0);
});

test('raw HTTP admission rejects wrong audience, ambiguous headers, malformed framing and unsupported revisions', async t => {
  const f = await fixture(t), ordinary = await f.issue(f.owner, f.account.accountId);
  const body = envelope('tools/list');
  assert.equal((await wire(f, ordinary.token, body)).status, 401);
  const missing = await wire(f, null, body); assert.equal(missing.status, 401);
  assert.equal(missing.headers['www-authenticate'], `Bearer realm="soty", resource_metadata="${f.origin}/.well-known/oauth-protected-resource/mcp"`);
  for (const options of [
    { path: '/mcp?' }, { path: '/MCP' }, { path: '/%6dcp' }, { path: '/mcp/' },
    { headers: { host: 'other.invalid' } }, { headers: { 'mcp-method': ['tools/list', 'tools/list'] } },
  ]) assert.equal((await wire(f, f.identity.token, body, options)).status, 400);
  assert.equal((await wire(f, f.identity.token, body, { headers: { origin: 'null' } })).status, 403);
  assert.equal((await wire(f, f.identity.token, body, { headers: { authorization: [`Bearer ${f.identity.token}`, `Bearer ${f.identity.token}`] } })).status, 401);
  for (const method of ['GET', 'DELETE', 'HEAD', 'OPTIONS']) assert.equal((await wire(f, f.identity.token, undefined, { method })).status, 405);
  for (const version of ['2025-03-26', '2099-01-01']) assert.equal((await wire(f, f.identity.token, body, { version })).messages[0].error.code, -32022);
  assert.equal((await wire(f, f.identity.token, envelope('tools/list', {}, LEGACY), { version: null })).status, 400);
  for (const malformed of [Buffer.from([0xc0, 0xaf]), '\ufeff{}', '[]', '{"jsonrpc":"2.0","id":1,"method":"tools/list","m\u0065thod":"tools/list"}',
    JSON.stringify({ ...body, id: Number.MAX_SAFE_INTEGER + 1 }), '{"jsonrpc":"2.0","method":"tools/list","params":' + '['.repeat(21) + '0' + ']'.repeat(21) + '}']) {
    const response = await wire(f, f.identity.token, malformed); assert.ok([400, 413].includes(response.status));
  }
  const mismatched = await wire(f, f.identity.token, body, { headers: { 'mcp-method': 'tools/call' } });
  assert.equal(mismatched.status, 400);
  assert.equal(countNotes(f), 0);
});

test('MCP history survives human edit/purge and disabled restart without recreating the Note', async t => {
  const f = await fixture(t), created = success(await call(f, f.identity.token, 'notes_create_draft', input, LEGACY));
  const noteId = created.result.noteId;
  good(await f.call(f.owner, 'notes.put', { expectedAccountId: f.account.accountId, noteId, mutationId: 'mcp-human-edit', expectedRevision: 1,
    title: 'Human edit', body: 'Later private text', items: [], pinned: false, color: 'plain', state: 'trashed' }));
  good(await f.call(f.owner, 'notes.purge', { expectedAccountId: f.account.accountId, noteId, mutationId: 'mcp-human-purge', expectedRevision: 2 }));
  await f.restart({ enabled: false });
  assert.deepEqual(success(await call(f, f.identity.token, 'notes_create_draft', input)).invocation, created.invocation);
  assert.deepEqual(success(await call(f, f.identity.token, 'invocations_get', { invocationId: created.invocation.invocationId })).result, created.result);
  toolError(await call(f, f.identity.token, 'notes_create_draft', { ...input, idempotencyKey: 'disabled-different-intention' }), 'service_unavailable');
  assert.equal(countNotes(f), 1);
  assert.equal((await f.call(f.owner, 'notes.get', { expectedAccountId: f.account.accountId, noteId })).error.code, 'notes_note_not_found');
});

test('unsupported HTTP methods use the method-not-found protocol code', async t => {
  const f = await fixture(t);
  for (const method of ['GET', 'DELETE', 'OPTIONS']) {
    const response = await wire(f, f.identity.token, undefined, { method });
    assert.equal(response.status, 405); assert.equal(response.headers.allow, 'POST');
    assert.equal(response.messages[0].error.code, -32601);
  }
});

test('unsupported revision negotiation advertises only fixed versions without reflecting arbitrary metadata', async t => {
  const f = await fixture(t);
  for (const { version, declared, expected } of [
    { version: '2099-01-01', declared: MODERN, expected: '2099-01-01' },
    { version: 'untrusted-header-value', declared: MODERN, expected: 'unknown' },
    { version: MODERN, declared: 'untrusted-meta-value', expected: 'unknown' },
    { version: MODERN, declared: '2028-08-03', expected: '2028-08-03' },
  ]) {
    const message = envelope('tools/list', { _meta: { [PROTOCOL_VERSION_META_KEY]: declared } });
    const response = await wire(f, f.identity.token, message, { version });
    assert.equal(response.status, 400); assert.equal(response.messages[0].error.code, -32022);
    assert.deepEqual(response.messages[0].error.data, { supported: [MODERN, LEGACY], requested: expected });
    absent(response, ['untrusted-header-value', 'untrusted-meta-value']);
  }
});

test('headerless legacy initialization with an unsupported version remains a negotiable version refusal', async t => {
  const f = await fixture(t);
  const response = await wire(f, f.identity.token, envelope('initialize', { protocolVersion: '2025-03-26',
    capabilities: {}, clientInfo: { name: 'old-fixture', version: '1' } }, LEGACY), { version: null });
  assert.equal(response.status, 400); assert.equal(response.messages[0].error.code, -32022);
  assert.deepEqual(response.messages[0].error.data, { supported: [MODERN, LEGACY], requested: '2025-03-26' });
});

test('real OAuth MCP resource refresh and keyless AS-off replay preserve original invocation authority', async t => {
  const f = await oauthNativeFixture(t), owner = nativeIdentity('OAuth MCP owner'), account = await f.bootstrap(owner);
  const connected = await f.connect(owner, account.accountId, { resource: `${f.origin}/mcp` });
  const first = success(await call(f, connected.tokens.access_token, 'notes_create_draft', input));
  assert.equal((await f.http('/api/capabilities/v1/invocations/' + first.invocation.invocationId, { token: connected.tokens.access_token })).status, 401);
  const refreshed = f.tokens(await f.refresh(connected.flow, connected.tokens));
  await f.keyless({ enabled: false });
  const replay = success(await call(f, refreshed.access_token, 'notes_create_draft', input, LEGACY));
  assert.equal(replay.reused, true); assert.deepEqual(replay.invocation, first.invocation);
  toolError(await call(f, refreshed.access_token, 'notes_create_draft', { ...input, idempotencyKey: 'keyless-new-intention' }), 'service_unavailable');
  good(await f.call(owner, 'oauth.connections.revoke', { expectedAccountId: account.accountId, connectionId: connected.flow.connectionId }));
  assert.ok([401, 403].includes((await call(f, refreshed.access_token, 'invocations_get', { invocationId: first.invocation.invocationId })).status));
  assert.equal(countNotes(f), 1);
});

test('lease survives actual socket completion until the real SDK close finishes', async t => {
  const f = await fixture(t), front = express();
  const mounted = attachCapabilitiesMcp(front, { service: f.app.locals.capabilitiesService, origin: f.origin, limits: { readers: 1, readersPerPeer: 1 } });
  front.use(f.app); f.setFront(front);
  const original = Server.prototype.close; let reached, release;
  const atClose = new Promise(resolve => { reached = resolve; }), barrier = new Promise(resolve => { release = resolve; });
  let heldServer, heldClosing;
  Server.prototype.close = function () {
    if (!heldServer) {
      heldServer = this; reached(); heldClosing = barrier.then(() => original.call(this));
    }
    return this === heldServer ? heldClosing : original.call(this);
  };
  try {
    const first = wire(f, f.identity.token, envelope('tools/list'));
    await atClose;
    const second = await wire(f, f.identity.token, envelope('tools/list'));
    assert.equal(second.status, 429, 'SDK cleanup still owns capacity after wire completion');
    release(); await first; Server.prototype.close = original;
    let recovered;
    for (let n = 0; n < 20; n++) { recovered = await wire(f, f.identity.token, envelope('tools/list')); if (recovered.status === 200) break; await delay(5); }
    assert.equal(recovered.status, 200);
  } finally { Server.prototype.close = original; release(); await mounted.close(); }
});

// Pin only the public stream lifecycle: the SDK has its raw SSE Response and
// then its monitored Response. Hold EOF on the latter, after fetch returned.
// Bytes, codec, domain operations and authorization remain real.
function holdLegacyEof() {
  const descriptor = Object.getOwnPropertyDescriptor(Response.prototype, 'body'), streams = new WeakMap();
  let seen = 0, reached, release, cancelled = 0;
  const atEnd = new Promise(resolve => { reached = resolve; }), gate = new Promise(resolve => { release = resolve; });
  Object.defineProperty(Response.prototype, 'body', { ...descriptor, get() {
    const original = descriptor.get.call(this);
    if (!original || !/^text\/event-stream/u.test(this.headers.get('content-type') || '')) return original;
    if (streams.has(this)) return streams.get(this);
    seen++;
    if (seen !== 2) { streams.set(this, original); return original; }
    const reader = original.getReader(); let stopped = false;
    const held = new ReadableStream({
      async pull(controller) {
        try {
          const next = await reader.read();
          if (next.done) { reached(); await gate; if (!stopped) controller.close(); }
          else if (!stopped) controller.enqueue(next.value);
        } catch (error) { if (!stopped) controller.error(error); }
      },
      async cancel() { stopped = true; cancelled++; release(); await reader.cancel().catch(() => {}); },
    });
    streams.set(this, held); return held;
  } });
  return { atEnd, release, get seen() { return seen; }, get cancelled() { return cancelled; },
    restore() { release(); Object.defineProperty(Response.prototype, 'body', descriptor); } };
}

test('legacy finite SSE is fully buffered and reauthorized after real signed revoke before any private wire byte', async t => {
  const f = await fixture(t), hold = holdLegacyEof();
  let headers = false;
  try {
    const pending = wire(f, f.identity.token, envelope('tools/call', { name: 'notes_create_draft', arguments: input }, LEGACY),
      { version: LEGACY, onHeaders() { headers = true; } });
    await hold.atEnd;
    assert.equal(hold.seen, 2, 'public monitored SSE response reached');
    assert.equal(headers, false, 'even response headers wait for the authority gate');
    assert.equal(countNotes(f), 1, 'actual Note COMMIT precedes the held response');
    const invocationId = f.sql(f.capsFile, db => db.prepare('SELECT id FROM cap_invocations').get().id);
    good(await f.call(f.owner, 'access.grants.revoke', { expectedAccountId: f.account.accountId, grantId: f.identity.grantId }));
    hold.release(); const denied = await pending;
    assert.ok([401, 403].includes(denied.status)); absent(denied, [invocationId, input.title, input.body, f.identity.token]);
    assert.equal(countNotes(f), 1, 'transport denial never rolls back or repeats the effect');
  } finally { hold.restore(); }
});

test('disconnect cancels held legacy body and releases the admitted slot only after cleanup', async t => {
  const f = await fixture(t), front = express();
  const mounted = attachCapabilitiesMcp(front, { service: f.app.locals.capabilitiesService, origin: f.origin, limits: { readers: 1, readersPerPeer: 1 } });
  front.use(f.app); f.setFront(front);
  const hold = holdLegacyEof(); let client;
  try {
    const pending = wire(f, f.identity.token, envelope('tools/list', {}, LEGACY), { version: LEGACY, onRequest(req) { client = req; } });
    void pending.catch(() => {});
    await hold.atEnd;
    assert.equal((await wire(f, f.identity.token, envelope('tools/list'))).status, 429);
    client.destroy(); await assert.rejects(pending);
    let recovered;
    for (let n = 0; n < 30; n++) { recovered = await wire(f, f.identity.token, envelope('tools/list')); if (recovered.status === 200) break; await delay(5); }
    assert.equal(hold.cancelled, 1, 'outer monitored body cancelled'); assert.equal(recovered.status, 200);
  } finally { hold.restore(); await mounted.close(); }
});

test('valid subscription requests terminate immediately and cannot allocate a persistent channel', async t => {
  const f = await fixture(t);
  for (const notifications of [{}, { toolsListChanged: true }]) {
    const response = await wire(f, f.identity.token, envelope('subscriptions/listen', { notifications }));
    assert.equal(response.status, 200); assert.equal(response.messages.length, 1);
    assert.equal(response.messages[0].error.code, -32603);
    assert.match(response.headers['content-type'], /^application\/json/u);
    assert.equal(response.headers['mcp-session-id'], undefined);
  }
  assert.equal((await wire(f, f.identity.token, envelope('tools/list'))).status, 200);
  assert.equal(countNotes(f), 0);
});
