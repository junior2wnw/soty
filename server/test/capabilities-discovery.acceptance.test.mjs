import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createServer, request } from 'node:http';
import { createConnection } from 'node:net';
import { mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import express from 'express';
import { createHttpApp } from '../http-app.js';
import { attachCapabilitiesDiscovery, validateDiscoveryOrigin } from '../capabilities-discovery.js';
import { createConnectService } from '../../modules/connect/server/index.mjs';
import { createCapabilitiesService } from '../../modules/capabilities/server/index.mjs';
import { BUILTIN_CAPABILITIES, createCatalog } from '../../modules/capabilities/server/catalog.mjs';
import { BUILTIN_DOCUMENTATION } from '../../modules/capabilities/server/documentation.mjs';

// Independent transport fixtures: real service/catalog/SQLite and loopback HTTP.
// No author fixture, renderer mock, actor credential, or production data is used.
const BASE = '/api/capabilities/v1';
const NOTE = `${BASE}/catalog/notes.createDraft/versions/1`;
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const SPA = 'INDEPENDENT_A4_SPA';
const ORIGIN = 'https://discovery.audit.test';

function temporaryDirectory() { return mkdtempSync(join(tmpdir(), 'soty-a4-independent-')); }
function removeTemporaryDirectory(folder) {
  assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
  assert.match(basename(folder), /^soty-a4-independent-/u);
  rmSync(folder, { recursive: true, force: true });
}
function fileSnapshot(folder, prefix = '') {
  return readdirSync(folder, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))
    .flatMap(entry => entry.isDirectory() ? [{ path: `${prefix}${entry.name}/`, type: 'directory' }, ...fileSnapshot(join(folder, entry.name), `${prefix}${entry.name}/`)]
      : [{ path: `${prefix}${entry.name}`, type: 'file', bytes: sha(readFileSync(join(folder, entry.name))) }]);
}

function descriptor(id, visibility = 'public') {
  return { capabilityId: id, version: 1, appId: 'audit', title: `Probe ${id}`, description: 'Synthetic read-only discovery fixture.',
    visibility, executionEnabled: false,
    inputSchema: { type: 'object', properties: {}, required: [], additionalProperties: false },
    outputSchema: { type: 'object', properties: {}, required: [], additionalProperties: false },
    resources: ['audit:example'], effects: ['read'], recipients: ['soty:audit'],
    executionBinding: { kind: 'native', handler: 'audit.example', version: 1 } };
}
function documentation(entries, { long = false } = {}) {
  const registry = createCatalog(entries);
  return entries.filter(entry => entry.visibility === 'public').map(entry => ({
    capabilityId: entry.capabilityId, version: entry.version,
    contractDigest: registry.get(entry.capabilityId, entry.version).digest,
    locales: {
      ru: { title: `Проверка ${entry.capabilityId}`, summary: long ? '界'.repeat(1000) : 'Только описание синтетического контракта.',
        useWhen: ['Проверка HTTP.'], notFor: ['Не выполняет действие.'], examples: [{ input: {}, output: {} }] },
      en: { title: `Audit ${entry.capabilityId}`, summary: 'A synthetic transport-only description.',
        useWhen: ['HTTP acceptance.'], notFor: ['Execution.'], examples: [{ input: {}, output: {} }] },
    }, keywords: { ru: ['проверка'], en: ['audit'] },
  }));
}

async function listening(t, app, close = async () => {}) {
  const server = createServer(app);
  await new Promise((done, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', done); });
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((done, reject) => server.close(error => error ? reject(error) : done()));
    await close();
  });
  const { port } = server.address();
  return { port, call: (path, { method = 'GET', headers = {} } = {}) => new Promise((done, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path, method, headers }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => {
        const bytes = Buffer.concat(chunks);
        done({ status: res.statusCode, headers: res.headers, bytes, text: bytes.toString('utf8'), json: () => JSON.parse(bytes) });
      });
    });
    req.setTimeout(3000, () => req.destroy(new Error('independent HTTP deadline')));
    req.on('error', reject); req.end();
  }) };
}
async function publicServer(t, { entries = BUILTIN_CAPABILITIES, docs = BUILTIN_DOCUMENTATION, origin = ORIGIN } = {}) {
  const service = createCapabilitiesService({ databasePath: ':memory:', projectId: 'discovery_acceptance', actorActive: () => false, catalog: entries, documentation: docs });
  const app = express(); app.disable('x-powered-by');
  attachCapabilitiesDiscovery(app, { catalog: service.catalog, origin });
  app.use((_req, res) => res.status(200).type('text/plain').end(SPA));
  return listening(t, app, () => service.close());
}
async function composedServer(t) {
  const folder = temporaryDirectory(), dist = join(folder, 'dist');
  mkdirSync(dist); writeFileSync(join(dist, 'index.html'), `<!doctype html><title>${SPA}</title>`);
  let app;
  try {
    app = createHttpApp(dist, { dataDir: join(folder, 'data'), connectOrigins: ['http://localhost:4170'], discoveryOrigin: 'http://localhost:4170',
      namedAppZone: 'http://apps.localhost:4170', appOriginTemplate: 'http://{appId}.legacy.localhost:4170',
      gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
  } catch (error) { removeTemporaryDirectory(folder); throw error; }
  return listening(t, app, async () => { await app.locals.closeServices(); removeTemporaryDirectory(folder); });
}
function rawHttp(port, target, hostLines) {
  return new Promise((done, reject) => {
    const chunks = [], socket = createConnection({ host: '127.0.0.1', port });
    socket.setTimeout(3000, () => socket.destroy(new Error('independent raw HTTP deadline')));
    socket.on('connect', () => socket.write(`GET ${target} HTTP/1.1\r\n${hostLines.join('\r\n')}\r\nConnection: close\r\n\r\n`));
    socket.on('data', chunk => chunks.push(chunk)); socket.on('error', reject);
    socket.on('end', () => {
      const text = Buffer.concat(chunks).toString('utf8');
      done({ status: Number(/^HTTP\/1\.1 (\d{3})/u.exec(text)?.[1]), text });
    });
  });
}
function errorResponse(response, code, status) {
  assert.equal(response.status, status); assert.deepEqual(response.json(), { error: { code } });
  assert.equal(response.headers['cache-control'], 'no-store'); assert.equal(response.headers.etag, undefined);
  assert.equal(Number(response.headers['content-length']), response.bytes.length);
  assert.equal(response.headers['set-cookie'], undefined); assert.ok(!response.text.includes(SPA));
}

test('origin admission matches the actual Connect constructor and leaves existing storage byte-identical on rejection', async t => {
  const folder = temporaryDirectory(); t.after(() => removeTemporaryDirectory(folder));
  const dataDir = join(folder, 'data'); mkdirSync(dataDir);
  const prior = createCapabilitiesService({ databasePath: join(dataDir, 'capabilities', 'registry.sqlite'), projectId: 'discovery_acceptance', actorActive: () => false });
  prior.close(); writeFileSync(join(dataDir, 'preserve.txt'), 'synthetic pre-existing storage');
  const before = fileSnapshot(folder);
  const accepted = ['http://localhost:4170', 'http://127.0.0.1:4170', 'http://[::1]:4170', 'https://discovery.audit.test'];
  for (const origin of accepted) {
    const connect = createConnectService({ databasePath: ':memory:', projectId: 'audit', allowedOrigins: [origin] }); connect.close();
    assert.equal(validateDiscoveryOrigin({ discoveryOrigin: origin, shellOrigins: [origin] }), origin);
    const f = await publicServer(t, { origin });
    const sitemap = await f.call('/agents/sitemap.xml');
    assert.equal(sitemap.status, 200); assert.ok(sitemap.text.includes(`${origin}/agents`));
  }
  const denied = ['http://127.0.0.2:4170', 'http://docs.localhost:4170', 'http://localhost.:4170', 'http://[::ffff:127.0.0.1]:4170',
    'http://127.1:4170', 'http://2130706433:4170', 'http://0x7f000001:4170', 'http://LOCALHOST:4170',
    'https://discovery.audit.test:443', 'https://discovery.audit.test/', 'https://name:pass@discovery.audit.test'];
  for (const origin of denied) {
    assert.throws(() => createConnectService({ databasePath: ':memory:', projectId: 'audit', allowedOrigins: [origin] }), { code: 'origin_not_allowed' });
    assert.throws(() => createHttpApp(folder, { dataDir, connectOrigins: [origin], discoveryOrigin: origin,
      appOriginTemplate: '', namedAppZone: '', gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } }), error => /^discovery_origin_/u.test(error.code));
    assert.deepEqual(fileSnapshot(folder), before, `admission must precede all storage: ${origin}`);
  }
});

test('raw duplicate or malformed Host cannot enter discovery and app-host precedence survives forged proxy headers', async t => {
  const f = await composedServer(t), target = `${BASE}/catalog`;
  for (const lines of [
    ['Host: localhost:4170', 'Host: localhost:4170'], ['Host: localhost:4170', 'hOsT: missing.apps.localhost:4170'],
    [], ['Host:'], ['Host: localhost:4170,evil.test'], ['Host: actor@localhost:4170'], ['Host: localhost:invalid'],
  ]) {
    const result = await rawHttp(f.port, target, lines);
    assert.equal(result.status, 400, JSON.stringify(lines));
    assert.ok(!result.text.includes('notes.createDraft') && !result.text.includes(SPA));
  }
  for (const host of ['missing.apps.localhost:4170', 'deeper.missing.apps.localhost:4170', 'missing.legacy.localhost:4170']) {
    const response = await f.call('/agents', { headers: { host, 'x-forwarded-host': 'localhost:4170', forwarded: 'host=localhost:4170;proto=http', origin: 'http://localhost:4170' } });
    assert.equal(response.status, 404); assert.ok(response.text.includes('app_not_found')); assert.ok(!response.text.includes('notes.createDraft'));
  }
  const legit = await f.call('/agents/sitemap.xml', { headers: { host: 'localhost:4170', forwarded: 'host=evil.test;proto=https', origin: 'https://evil.test' } });
  assert.equal(legit.status, 200); assert.ok(legit.text.includes('http://localhost:4170/agents')); assert.ok(!legit.text.includes('evil.test'));
});

test('malformed namespace and decoded duplicate queries return bounded no-store GET/HEAD errors rather than the shell', async t => {
  const f = await publicServer(t);
  const cases = [
    [`${BASE}/catalog?limit=1&%6cimit=1`, 'query_invalid', 400],
    [`${BASE}/catalog?query=x&%71uery=y`, 'query_invalid', 400],
    [`${BASE}/catalog?cursor=x&cursor=y`, 'query_invalid', 400],
    [`${BASE}/catalog?__proto__[x]=audit`, 'query_invalid', 400],
    [`${BASE}/catalog?query=%C0%AF`, 'query_invalid', 400],
    [`${BASE}/catalog?query=%ED%A0%80`, 'query_invalid', 400],
    [`${BASE}/catalog?query=%F4%90%80%80`, 'query_invalid', 400],
    ['/api/capabilities/v9/unknown/%E0%A4', 'invalid_input', 400],
    ['/agents/unknown/%', 'invalid_input', 400],
    [`${BASE}/catalog/notes.createDraft/versions/%31`, 'invalid_input', 400],
    [`${BASE}/catalog/notes.createDraft/versions/1/unknown`, 'not_found', 404],
    ['/agents//capabilities/notes.createDraft/versions/1', 'not_found', 404],
    [`${BASE}/catalog?query=${'x'.repeat(8192)}`, 'uri_too_long', 414],
  ];
  for (const [target, code, status] of cases) {
    const get = await f.call(target, { headers: { 'if-none-match': '*' } }); errorResponse(get, code, status);
    assert.ok(get.bytes.length < 100);
    const head = await f.call(target, { method: 'HEAD', headers: { 'if-none-match': '*' } });
    assert.equal(head.status, status); assert.equal(head.bytes.length, 0);
    for (const key of ['content-type', 'content-length', 'cache-control', 'etag']) assert.equal(head.headers[key], get.headers[key]);
  }
  const post = await f.call(`${BASE}/catalog`, { method: 'POST' });
  errorResponse(post, 'method_not_allowed', 405); assert.equal(post.headers.allow, 'GET, HEAD');
});

test('real UTF-8 catalog paging observes the serialized byte budget without duplicate or missing entries', async t => {
  const entries = Array.from({ length: 25 }, (_, index) => descriptor(`audit.page${String(index).padStart(2, '0')}`));
  const f = await publicServer(t, { entries, docs: documentation(entries, { long: true }) });
  const seen = [], pages = []; let cursor, first;
  do {
    const response = await f.call(`${BASE}/catalog?limit=20${cursor ? `&cursor=${cursor}` : ''}`);
    assert.equal(response.status, 200); assert.ok(response.bytes.length <= 65536);
    assert.equal(Number(response.headers['content-length']), response.bytes.length);
    assert.ok(response.bytes.length > response.text.length, 'the budget must be tested with genuine multibyte output');
    const page = response.json(); first ??= page;
    assert.equal(page.total, entries.length); assert.ok(page.items.length > 0);
    pages.push({ items: page.items.length, bytes: response.bytes.length });
    seen.push(...page.items.map(item => item.capabilityId)); cursor = page.cursor;
  } while (cursor);
  assert.ok(first.items.length < 20 && first.cursor, 'byte limit, not requested row count, must divide this fixture');
  assert.deepEqual(seen, entries.map(entry => entry.capabilityId));
  const repeat = await f.call(`${BASE}/catalog?limit=20`);
  assert.deepEqual(repeat.json(), first);
  t.diagnostic(`Synthetic UTF-8 HTTP pages: ${JSON.stringify(pages)}`);
});

test('HTTP cursors reject changed public snapshots and malformed wire forms while normalized query and private changes preserve them', async t => {
  const entries = [descriptor('audit.first'), descriptor('audit.second')], docs = documentation(entries);
  const privateEntry = descriptor('private.onlyAudit', 'private');
  const original = await publicServer(t, { entries, docs });
  const privateOnly = await publicServer(t, { entries: [...entries, privateEntry], docs: [...docs, { capabilityId: privateEntry.capabilityId, version: 1, malformedPrivate: '\ud800' }] });
  const first = await original.call(`${BASE}/catalog?query=AUDIT&limit=1`), cursor = first.json().cursor;
  assert.ok(cursor); assert.ok(cursor.length <= 512);
  const next = await privateOnly.call(`${BASE}/catalog?query=%20audit%20&limit=1&cursor=${cursor}`);
  assert.equal(next.status, 200); assert.equal(next.json().items[0].capabilityId, 'audit.second');
  assert.equal((await privateOnly.call(`${BASE}/catalog?query=AUDIT&limit=1`)).headers.etag, first.headers.etag);
  const revisedDocs = structuredClone(docs); revisedDocs[0].locales.en.notFor.push('Revision changed.');
  const changedDocs = await publicServer(t, { entries, docs: revisedDocs });
  const changedReadiness = await publicServer(t, { entries: entries.map((entry, index) => index ? entry : { ...entry, executionEnabled: true }), docs });
  for (const changed of [changedDocs, changedReadiness]) errorResponse(await changed.call(`${BASE}/catalog?query=audit&limit=1&cursor=${cursor}`), 'cursor_invalid', 400);
  const parsed = JSON.parse(Buffer.from(cursor, 'base64url'));
  const malformed = [cursor + '=', 'a'.repeat(513), Buffer.from([0xff]).toString('base64url'),
    ...[-1, 0.5, 3, Number.MAX_SAFE_INTEGER + 1].map(offset => Buffer.from(JSON.stringify({ ...parsed, offset })).toString('base64url')),
    Buffer.from(JSON.stringify({ ...parsed, accountId: 'untrusted' })).toString('base64url'),
    Buffer.from(` ${Buffer.from(cursor, 'base64url').toString('utf8')}`).toString('base64url')];
  for (const bad of malformed) errorResponse(await original.call(`${BASE}/catalog?query=audit&limit=1&cursor=${encodeURIComponent(bad)}`), 'cursor_invalid', 400);
  errorResponse(await original.call(`${BASE}/catalog?query=second&cursor=${cursor}`), 'cursor_invalid', 400);
});

test('a public validator or fake credential never yields 304 or a descriptor for known-private routes', async t => {
  const hidden = { ...descriptor('audit.secret', 'private'), title: 'PRIVATE_A4_MARKER', description: 'PRIVATE_A4_MARKER' };
  const f = await publicServer(t, { entries: [...BUILTIN_CAPABILITIES, hidden] });
  const publicDoc = await f.call(NOTE), etag = publicDoc.headers.etag;
  assert.equal(publicDoc.status, 200); assert.ok(etag);
  for (const suffix of ['', '/contract.json', '/schemas/input', '/schemas/output']) {
    const known = `${BASE}/catalog/audit.secret/versions/1${suffix}`, unknown = `${BASE}/catalog/audit.missing/versions/1${suffix}`;
    for (const validator of [etag, '*', `W/${etag}`]) {
      const headers = { 'if-none-match': validator, authorization: 'Bearer SYNTHETIC_A4_NOT_A_CREDENTIAL', cookie: 'synthetic=not-a-session' };
      const denied = await f.call(known, { headers }), absent = await f.call(unknown, { headers });
      errorResponse(denied, 'not_found', 404); assert.equal(absent.status, 404); assert.deepEqual(denied.bytes, absent.bytes);
      assert.equal(denied.headers['access-control-allow-origin'], '*'); assert.equal(denied.headers['access-control-allow-credentials'], undefined);
      const head = await f.call(known, { method: 'HEAD', headers });
      assert.equal(head.status, 404); assert.equal(head.bytes.length, 0); assert.equal(head.headers.etag, undefined);
    }
  }
  const ordinary = await f.call(`${BASE}/catalog`), spoofed = await f.call(`${BASE}/catalog`, { headers: { authorization: 'Bearer SYNTHETIC_A4_NOT_A_CREDENTIAL', cookie: 'synthetic=not-a-session' } });
  assert.deepEqual(ordinary.bytes, spoofed.bytes); assert.equal(ordinary.headers.etag, spoofed.headers.etag);
  assert.ok(!ordinary.text.includes('PRIVATE_A4_MARKER'));
  const invalidConditional = await f.call(NOTE, { headers: { 'if-none-match': `${etag}, malformed-tail` } });
  assert.equal(invalidConditional.status, 200, 'an invalid validator list must not truncate at an earlier match');
});

test('GET and HEAD expose exact canonical bytes and encoded ID links without changing schema or success/error cache policy', async t => {
  const unusual = descriptor('audit/segment@one'), f = await publicServer(t, {
    entries: [...BUILTIN_CAPABILITIES, unusual], docs: [...BUILTIN_DOCUMENTATION, ...documentation([unusual])],
  });
  const note = await f.call(`${NOTE}/contract.json`);
  assert.equal(note.status, 200); assert.equal(sha(note.bytes), '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204');
  const page = (await f.call(`${BASE}/catalog?query=audit`)).json();
  assert.equal(page.items.length, 1); assert.ok(page.items[0].links.detail.includes('audit%2Fsegment%40one'));
  for (const target of [page.items[0].links.detail, page.items[0].links.contract, `${NOTE}/contract.json`, `${NOTE}/schemas/input`, `${NOTE}/schemas/output`, `${BASE}/openapi.json`, `${BASE}/status`]) {
    const get = await f.call(target), head = await f.call(target, { method: 'HEAD' });
    assert.equal(get.status, 200); assert.equal(head.status, 200); assert.equal(head.bytes.length, 0);
    for (const key of ['content-type', 'content-length', 'etag', 'cache-control']) assert.equal(head.headers[key], get.headers[key]);
    assert.equal(Number(get.headers['content-length']), get.bytes.length);
    assert.equal(get.headers['x-content-type-options'], 'nosniff'); assert.equal(get.headers['set-cookie'], undefined);
    if (target.endsWith('/status')) {
      assert.equal(get.headers['cache-control'], 'no-store'); assert.equal(get.headers.etag, undefined);
      assert.deepEqual(get.json(), { notesCreateEnabled: false, audience: null });
      assert.equal((await f.call(target, { headers: { 'if-none-match': '*' } })).status, 200);
    } else {
      assert.equal(get.headers['cache-control'], 'public,no-cache');
      const conditional = await f.call(target, { method: 'HEAD', headers: { 'if-none-match': `W/${get.headers.etag}` } });
      assert.equal(conditional.status, 304); assert.equal(conditional.bytes.length, 0);
    }
  }
  for (const kind of ['input', 'output']) assert.deepEqual((await f.call(`${NOTE}/schemas/${kind}`)).json(), BUILTIN_CAPABILITIES[0][`${kind}Schema`]);
  errorResponse(await f.call(page.items[0].links.detail.replace('%2F', '%252F')), 'invalid_input', 400);
});
