import test from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:http';
import { createConnection } from 'node:net';
import express from 'express';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';
import { PROTOCOL_VERSION_META_KEY, CLIENT_INFO_META_KEY, CLIENT_CAPABILITIES_META_KEY } from '@modelcontextprotocol/server';
import { attachCapabilitiesMcp } from '../capabilities-mcp.js';
import { createCatalog } from '../../modules/capabilities/server/catalog.mjs';
import { createPublicDiscovery } from '../../modules/capabilities/server/discovery.mjs';
import { CAPABILITY_VALIDATION_PROFILE } from '../../modules/capabilities/server/documentation.mjs';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';

const MODERN = '2026-07-28', LEGACY = '2025-11-25';
const RESPONSE_BOUND = 2097152;

function envelope(method, params = {}, version = MODERN, id = 1) {
  return { jsonrpc: '2.0', id, method, params: { ...params, ...(version === MODERN ? { _meta: {
    [PROTOCOL_VERSION_META_KEY]: MODERN,
    [CLIENT_INFO_META_KEY]: { name: 'independent-wire-client', version: '1' },
    [CLIENT_CAPABILITIES_META_KEY]: {}, ...params._meta,
  } } : {}) } };
}

// The assertions and diagnostics never print a bearer, request body or raw
// protocol error. Header canaries below are synthetic non-secret strings.
function wire(f, token, body, { version = MODERN, headers = {}, path = '/mcp', allowEarlyClose = false } = {}) {
  return new Promise((resolve, reject) => {
    let receivedBytes = 0, responseStatus = 0, responseHeaders = {}, deadline = false;
    const failed = error => {
      if (allowEarlyClose && !deadline && error.code === 'ECONNRESET') {
        resolve({ status: responseStatus, headers: responseHeaders, bytes: receivedBytes,
          text: '', messages: [], transportClosed: true });
      } else reject(new Error(deadline ? 'independent_wire_deadline' : 'independent_request_failed'));
    };
    const req = request(f.origin, { path, method: 'POST', headers: {
      authorization: `Bearer ${token}`, accept: 'application/json, text/event-stream',
      'content-type': 'application/json', 'mcp-protocol-version': version,
      ...(version === MODERN ? { 'mcp-method': body.method,
        ...(body.method === 'tools/call' ? { 'mcp-name': body.params?.name } : {}) } : {}),
      ...headers,
    } }, res => {
      responseStatus = res.statusCode; responseHeaders = res.headers;
      const chunks = []; let size = 0;
      res.on('data', chunk => {
        size += chunk.length; receivedBytes = size;
        if (size > RESPONSE_BOUND) { req.destroy(new Error('independent_response_bound')); return; }
        chunks.push(chunk);
      });
      res.once('error', failed);
      res.once('end', () => {
        try {
          const text = Buffer.concat(chunks).toString('utf8');
          const messages = /^text\/event-stream/u.test(res.headers['content-type'] || '')
            ? text.split(/\r?\n/u).filter(line => line.startsWith('data: ')).map(line => JSON.parse(line.slice(6)))
            : text ? [JSON.parse(text)] : [];
          resolve({ status: res.statusCode, headers: res.headers, bytes: size, text, messages });
        } catch { reject(new Error('independent_response_not_protocol')); }
      });
    });
    req.setTimeout(8000, () => { deadline = true; req.destroy(new Error('independent_wire_deadline')); });
    req.once('error', failed);
    req.end(JSON.stringify(body));
  });
}

async function fixture(t, options = {}) {
  const f = await nativeHttpFixture(t, options), owner = nativeIdentity('Independent MCP owner');
  const account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId, { audience: `${f.origin}/mcp` });
  return { ...f, get app() { return f.app; }, owner, account, identity };
}

function counts(f) {
  return {
    invocations: f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n),
    notes: f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n),
  };
}

test('independent no-echo wire errors do not disclose malformed MCP headers or client metadata', { timeout: 20000 }, async t => {
  const f = await fixture(t, { enabled: false });
  const headerCanary = 'synthetic-sensitive-header-6e913ca0';
  const metaCanary = 'synthetic-sensitive-client-info-cfe48321';
  const meta = { [CLIENT_INFO_META_KEY]: { name: metaCanary, version: '1' } };
  const cases = [
    { label: 'method-mismatch', body: envelope('tools/list', { _meta: meta }),
      headers: { 'mcp-method': headerCanary }, expectedStatus: 400, expectedCode: -32020 },
    { label: 'name-mismatch', body: envelope('tools/call', { name: 'catalog_search', arguments: {}, _meta: meta }),
      headers: { 'mcp-name': headerCanary }, expectedStatus: 400, expectedCode: -32020 },
    { label: 'name-invalid-sentinel', body: envelope('tools/call', { name: 'catalog_search', arguments: {}, _meta: meta }),
      headers: { 'mcp-name': `=?base64?${headerCanary}?=` }, expectedStatus: 400, expectedCode: -32020 },
    { label: 'valid-client-metadata', body: envelope('tools/list', { _meta: meta }),
      headers: {}, expectedStatus: 200, expectedCode: null },
  ];
  const observations = [];
  for (const item of cases) {
    const response = await wire(f, f.identity.token, item.body, { headers: item.headers });
    const protocolCode = response.messages[0]?.error?.code ?? null;
    observations.push({ case: item.label, status: response.status, protocolCode,
      headerReflected: response.text.includes(headerCanary), metadataReflected: response.text.includes(metaCanary),
      bytes: response.bytes });
    assert.equal(response.status, item.expectedStatus, `status for ${item.label}`);
    assert.equal(protocolCode, item.expectedCode, `protocol code for ${item.label}`);
    assert.equal(response.headers['cache-control'], 'no-store');
  }
  t.diagnostic(JSON.stringify({ observations, storage: counts(f) }));
  assert.deepEqual(counts(f), { invocations: 0, notes: 0 });
  assert.equal(observations.every(item => !item.headerReflected && !item.metadataReflected), true,
    'protocol failures must not reflect submitted headers or client metadata');
});

function checkedSuccess(value) {
  assert.notEqual(value.isError, true, 'tool returned success');
  assert.equal(value.content.length, 1);
  assert.equal(value.content[0].type, 'text');
  assert.deepEqual(JSON.parse(value.content[0].text), value.structuredContent);
  return value.structuredContent;
}
function wireResult(response) {
  assert.equal(response.status, 200);
  assert.equal(response.messages.length, 1);
  assert.equal(response.messages[0].error, undefined, 'one successful protocol response');
  return response.messages[0].result;
}
const call = (f, token, name, args = {}, version = MODERN, options = {}) =>
  wire(f, token, envelope('tools/call', { name, arguments: args }, version), { version, ...options });

for (const revision of [MODERN, LEGACY]) {
  test(`independent actual SDK Client2.2 negotiates ${revision} and uses signed native authority`, { timeout: 20000 }, async t => {
    const f = await fixture(t), observed = [], errors = [];
    const client = new Client({ name: 'independent-sdk-client', version: '1' }, {
      capabilities: {}, supportedProtocolVersions: [revision],
      versionNegotiation: { mode: revision === MODERN ? { pin: MODERN } : 'legacy' },
    });
    client.onerror = error => { errors.push({ code: error?.code ?? null }); };
    const transport = new StreamableHTTPClientTransport(new URL(`${f.origin}/mcp`), {
      requestInit: { headers: { authorization: `Bearer ${f.identity.token}` } },
      fetch: async (url, init) => {
        assert.equal(new URL(url).origin, f.origin, 'SDK remains on the owned endpoint');
        const response = await fetch(url, init);
        let method = null;
        if (typeof init?.body === 'string') {
          const message = JSON.parse(init.body);
          if (['server/discover', 'initialize', 'notifications/initialized', 'tools/list', 'tools/call'].includes(message.method)) method = message.method;
        }
        observed.push({ method, httpMethod: init?.method || 'GET', status: response.status,
          revision: new Headers(init?.headers).get('mcp-protocol-version'),
          type: response.headers.get('content-type')?.split(';')[0] ?? null,
          session: response.headers.has('mcp-session-id') });
        return response;
      },
    });
    try {
      await client.connect(transport, { timeout: 5000 });
      assert.equal(transport.protocolVersion, revision);
      const listed = await client.listTools(undefined, { timeout: 5000 });
      assert.deepEqual(listed.tools.map(item => item.name), ['catalog_search', 'catalog_get', 'notes_create_draft', 'invocations_get']);
      const input = { title: 'SDK independent title', body: 'Independent private source text', idempotencyKey: `independent-sdk-${revision}` };
      const created = checkedSuccess(await client.callTool({ name: 'notes_create_draft', arguments: input }, { timeout: 5000 }));
      assert.equal(created.invocation.status, 'succeeded'); assert.equal(created.reused, false);
      assert.equal(created.result.url, `${f.origin}/#notes/${created.result.noteId}`);
      const ownNote = good(await f.call(f.owner, 'notes.get', { expectedAccountId: f.account.accountId, noteId: created.result.noteId })).note;
      assert.equal(ownNote.body, input.body);
      const read = checkedSuccess(await client.callTool({ name: 'invocations_get', arguments: { invocationId: created.invocation.invocationId } }, { timeout: 5000 }));
      assert.deepEqual(read.result, created.result);
      assert.equal(JSON.stringify(read).includes(input.body), false, 'receipt does not contain current Note text');
      const error = await client.callTool({ name: 'notes_create_draft', arguments: { ...input, body: 'Conflicting content' } }, { timeout: 5000 });
      assert.equal(error.isError, true);
      assert.equal(Object.hasOwn(error, 'structuredContent'), false);
      assert.deepEqual(JSON.parse(error.content[0].text), { error: { code: 'invocation_request_conflict' } });
      assert.deepEqual(counts(f), { invocations: 1, notes: 1 });
      assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT spent_amount FROM cap_budgets').get().spent_amount), 1);
      const first = observed.find(item => item.method !== null);
      assert.equal(first.method, revision === MODERN ? 'server/discover' : 'initialize');
      assert.ok(observed.filter(item => item.method === 'tools/call').every(item => item.revision === revision
        && item.type === (revision === MODERN ? 'application/json' : 'text/event-stream')));
      assert.equal(observed.some(item => item.session), false);
      assert.equal(errors.length, 0, 'SDK client has no protocol/transport errors');
      t.diagnostic(JSON.stringify({ revision, exchanges: observed.length,
        discovery: first.method, toolCalls: observed.filter(item => item.method === 'tools/call').length,
        getAttempts: observed.filter(item => item.httpMethod === 'GET').length, storage: counts(f) }));
    } finally { await client.close(); }
  });
}

const bytes = value => Buffer.byteLength(JSON.stringify(value), 'utf8');
function declaration(capabilityId) {
  return { capabilityId, version: 1, appId: 'independent', title: 'Independent public metadata', description: 'A public declaration.',
    visibility: 'public', executionEnabled: false,
    inputSchema: { type: 'object', properties: { text: { type: 'string', maxLength: 100000 } }, required: ['text'], additionalProperties: false },
    outputSchema: { type: 'object', properties: { accepted: { type: 'boolean' } }, required: ['accepted'], additionalProperties: false },
    resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'],
    executionBinding: { kind: 'native', handler: 'independent.declaration', version: 1 } };
}
function sidecar(entry) {
  const locale = { title: 'Public operation', summary: 'Metadata does not grant execution rights.',
    useWhen: ['Inspect this declared contract.'], notFor: ['Granting access.'], examples: [] };
  return { capabilityId: entry.capabilityId, version: entry.version, contractDigest: entry.digest,
    locales: { ru: structuredClone(locale), en: structuredClone(locale) }, keywords: { ru: ['public'], en: ['public'] } };
}
function catalogFixture() {
  const large = declaration('independent.large');
  large.inputSchema.properties.text.enum = ['"\\'.repeat(30000)];
  large.inputSchema.properties.other = { type: 'string', maxLength: 100000, enum: [''] };
  const remaining = 262144 - bytes(large);
  assert.ok(remaining > 0);
  large.inputSchema.properties.other.enum[0] = '"'.repeat(Math.floor(remaining / 2)) + (remaining % 2 ? 'x' : '');
  assert.equal(bytes(large), 262144, 'literal registry input reaches its actual byte ceiling');
  const declarations = [declaration('independent.a'), large, declaration('independent.z')];
  const registry = createCatalog(declarations), documentation = registry.listPublic().map(sidecar);
  const document = documentation.find(item => item.capabilityId === large.capabilityId);
  document.locales.en.notFor = [''];
  const docRemaining = 16384 - bytes({ ...document, validation: CAPABILITY_VALIDATION_PROFILE, revision: '0'.repeat(64) });
  document.locales.en.notFor[0] = '\\'.repeat(Math.floor(docRemaining / 2)) + (docRemaining % 2 ? 'x' : '');
  assert.equal(bytes({ ...document, validation: CAPABILITY_VALIDATION_PROFILE, revision: '0'.repeat(64) }), 16384);
  const baseline = createPublicDiscovery({ catalog: registry, documentation });
  const hidden = { ...declaration('hidden.operation'), visibility: 'private', title: '\ud800', description: 'private-noninterference-marker' };
  const changedRegistry = createCatalog([hidden, ...declarations].reverse());
  const changed = createPublicDiscovery({ catalog: changedRegistry, documentation: [...documentation,
    { capabilityId: hidden.capabilityId, version: 1, contractDigest: false, locales: '\ud800'.repeat(20000) }] });
  return { largeId: large.capabilityId, baseline, changed, entryBytes: bytes(large), documentationBytes: 16384 };
}

function mountFront(f, { catalog, origin = f.origin, limits, trustProxy = false } = {}) {
  const front = express(); front.set('trust proxy', trustProxy);
  const original = f.app.locals.capabilitiesService;
  // Synthetic public registry projection only. Authentication, actor closures,
  // Connect fences, Notes coordinator and all persisted stores stay genuine.
  const service = catalog ? Object.freeze({ ...original, catalog }) : original;
  const mounted = attachCapabilitiesMcp(front, { service, origin, limits });
  front.use(f.app); f.setFront(front);
  return mounted;
}

test('independent wire catalog preserves private noninterference and maximal escaped registry declarations', { timeout: 20000 }, async t => {
  const f = await fixture(t, { enabled: false }), catalogs = catalogFixture(), transcripts = [];
  let baselineCursor;
  for (const catalog of [catalogs.baseline, catalogs.changed]) {
    const mounted = mountFront(f, { catalog });
    try {
      const transcript = [];
      for (const revision of [MODERN, LEGACY]) {
        const first = checkedSuccess(wireResult(await call(f, f.identity.token, 'catalog_search', { limit: 1 }, revision)));
        assert.equal(first.total, 3); assert.equal(typeof first.cursor, 'string');
        baselineCursor ??= first.cursor;
        const next = checkedSuccess(wireResult(await call(f, f.identity.token, 'catalog_search', { limit: 1, cursor: baselineCursor }, revision)));
        const response = await call(f, f.identity.token, 'catalog_get', { capabilityId: catalogs.largeId, version: 1 }, revision);
        const detail = checkedSuccess(wireResult(response));
        assert.deepEqual(detail, catalogs.baseline.get({ capabilityId: catalogs.largeId, version: 1 }));
        assert.ok(bytes(detail) > 262144 && bytes(detail) <= 384 * 1024, 'fixture exercises a large valid public detail');
        assert.ok(response.bytes > bytes(detail) * 2 && response.bytes <= RESPONSE_BOUND, 'measure actual escaping and the two representations');
        const errors = [];
        for (const capabilityId of ['hidden.operation', 'unknown.operation']) {
          const failure = wireResult(await call(f, f.identity.token, 'catalog_get', { capabilityId, version: 1 }, revision));
          assert.equal(failure.isError, true); assert.equal(Object.hasOwn(failure, 'structuredContent'), false);
          errors.push(JSON.parse(failure.content[0].text));
        }
        assert.deepEqual(errors[0], { error: { code: 'not_found' } }); assert.deepEqual(errors[1], errors[0]);
        const search = checkedSuccess(wireResult(await call(f, f.identity.token, 'catalog_search', { query: 'private-noninterference-marker' }, revision)));
        assert.equal(search.total, 0);
        transcript.push({ revision, first, next, errors, detailDigest: detail.capability.digest, wireBytes: response.bytes });
        t.diagnostic(JSON.stringify({ revision, rawEntryBytes: catalogs.entryBytes, documentationBytes: catalogs.documentationBytes,
          detailBytes: bytes(detail), wireBytes: response.bytes }));
      }
      transcripts.push(transcript);
    } finally { await mounted.close(); f.setFront(null); }
  }
  assert.deepEqual(transcripts[1], transcripts[0], 'private configuration has no public wire effect');
  assert.deepEqual(counts(f), { invocations: 0, notes: 0 });
});

test('independent lower output bound terminates before partial public data and releases capacity', { timeout: 20000 }, async t => {
  const f = await fixture(t, { enabled: false }), catalogs = catalogFixture();
  const mounted = mountFront(f, { catalog: catalogs.baseline, limits: { outputBytes: 65536, readers: 1, readersPerPeer: 1, responseTimeoutMs: 1000 } });
  try {
    assert.equal((await wire(f, f.identity.token, envelope('tools/list'))).status, 200);
    const started = performance.now();
    const response = await call(f, f.identity.token, 'catalog_get', { capabilityId: catalogs.largeId, version: 1 }, LEGACY, { allowEarlyClose: true });
    assert.ok(performance.now() - started < 3000, 'output refusal must finish before the independent network deadline');
    if (response.transportClosed) {
      assert.equal(response.status, 0, 'no success headers before the completed output-size check');
      assert.equal(response.bytes, 0, 'no partial public DTO escaped');
    } else {
      assert.equal(response.status, 413); assert.equal(response.messages[0]?.error?.message, 'payload_too_large');
    }
    let after;
    for (let attempt = 0; attempt < 25; attempt++) {
      after = await wire(f, f.identity.token, envelope('tools/list'));
      if (after.status !== 429) break;
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    assert.equal(after.status, 200, 'capacity is usable after the failed finite exchange');
    assert.deepEqual(counts(f), { invocations: 0, notes: 0 });
    t.diagnostic(JSON.stringify({ bound: 65536, transportClosed: response.transportClosed === true,
      status: response.status, bytes: response.bytes, replacementStatus: after.status }));
  } finally { await mounted.close(); f.setFront(null); }
});

test('independent HTTPS MCP uses the actual Express trust predicate and ignores forged proxy identity', { timeout: 20000 }, async t => {
  const f = await fixture(t, { enabled: false }), secureOrigin = 'https://mcp-independent.invalid';
  const identity = await f.issue(f.owner, f.account.accountId, { audience: `${secureOrigin}/mcp` });
  const headers = { host: 'mcp-independent.invalid', origin: secureOrigin,
    forwarded: 'for=127.0.0.1;host=mcp-independent.invalid;proto=https',
    'x-forwarded-host': 'mcp-independent.invalid', 'x-forwarded-proto': 'https' };
  for (const trustProxy of [false, 'loopback']) {
    const mounted = mountFront(f, { origin: secureOrigin, trustProxy });
    try {
      const response = await wire(f, identity.token, envelope('tools/list'), { headers });
      assert.equal(response.status, trustProxy ? 200 : 403,
        'only the host-configured trusted proxy can establish req.secure');
      assert.equal(response.headers['cache-control'], 'no-store');
      const wrongHost = await wire(f, identity.token, envelope('tools/list'), { headers: { ...headers, host: 'untrusted.invalid' } });
      assert.equal(wrongHost.status, 400, 'X-Forwarded-Host does not replace the canonical Host');
      const wrongAudience = await wire(f, f.identity.token, envelope('tools/list'), { headers });
      assert.equal(wrongAudience.status, trustProxy ? 401 : 403);
      if (trustProxy) assert.equal(wrongAudience.headers['www-authenticate'],
        `Bearer realm="soty", resource_metadata="${secureOrigin}/.well-known/oauth-protected-resource/mcp"`);
    } finally { await mounted.close(); f.setFront(null); }
  }
  assert.deepEqual(counts(f), { invocations: 0, notes: 0 });
});

test('independent malformed MCP route closes a held raw upload before ingress or SDK dispatch', { timeout: 10000 }, async t => {
  const f = await fixture(t, { enabled: false }), front = express(), real = f.app.locals.capabilitiesService;
  let authenticationCalls = 0;
  const mounted = attachCapabilitiesMcp(front, { origin: f.origin, service: Object.freeze({ ...real,
    authenticateCredential(args) { authenticationCalls++; return real.authenticateCredential(args); },
  }) });
  front.use(f.app); f.setFront(front);
  try {
    const address = new URL(f.origin);
    const observation = await new Promise(resolve => {
      const socket = createConnection({ host: '127.0.0.1', port: Number(address.port) });
      let settled = false, response = Buffer.alloc(0), status = null, connection = 'missing';
      function finish(closedBeforeDeadline, timedOut = false) {
        if (settled) return; settled = true; clearTimeout(timer);
        resolve({ status, connection, closedBeforeDeadline, timedOut, responseBytes: response.length });
        socket.destroy();
      }
      const timer = setTimeout(() => finish(false, true), 1200);
      socket.once('connect', () => socket.write(
        `POST /mcp? HTTP/1.1\r\nHost: ${address.host}\r\nContent-Type: application/json\r\nContent-Length: 1048576\r\nConnection: keep-alive\r\n\r\n{`));
      socket.on('data', chunk => {
        if (response.length + chunk.length > 16384) { finish(false); return; }
        response = Buffer.concat([response, chunk]);
        const text = response.toString('latin1'), end = text.indexOf('\r\n\r\n');
        if (end >= 0) {
          status = Number(/^HTTP\/1\.1 (\d{3})/u.exec(text)?.[1]);
          const value = /^Connection:\s*([^\r\n]+)/imu.exec(text.slice(0, end))?.[1]?.toLowerCase();
          connection = value === 'close' || value === 'keep-alive' ? value : value ? 'other' : 'missing';
        }
      });
      socket.once('error', () => { /* close determines the finite wire result; no raw diagnostics */ });
      socket.once('close', () => finish(true));
    });
    const storage = counts(f);
    t.diagnostic(JSON.stringify({ ...observation, authenticationCalls, storage }));
    assert.equal(observation.status, 400, 'literal malformed namespace receives its controlled error');
    assert.equal(authenticationCalls, 0, 'route rejection precedes bearer/SDK work');
    assert.deepEqual(storage, { invocations: 0, notes: 0 });
    assert.equal(observation.connection, 'close', 'early route failure must not keep an unfinished upload reusable');
    assert.equal(observation.closedBeforeDeadline, true, 'server closes the incomplete upload without waiting for its body');
    assert.equal(observation.timedOut, false, 'test timeout is not a successful server close');
  } finally { await mounted.close(); f.setFront(null); }
});
