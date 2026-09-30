import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import express from 'express';
import { createHttpApp } from '../http-app.js';
import { attachCapabilitiesDiscovery, validateDiscoveryOrigin } from '../capabilities-discovery.js';
import { BUILTIN_CAPABILITIES, createCatalog } from '../../modules/capabilities/server/catalog.mjs';
import { createCapabilitiesService } from '../../modules/capabilities/server/index.mjs';
import { createPublicDiscovery } from '../../modules/capabilities/server/discovery.mjs';
import { fixtureDocumentation } from '../../modules/capabilities/test/support/documentation.mjs';
import { AccessError } from '../../modules/capabilities/server/validation.mjs';

const BASE = '/api/capabilities/v1';
const NOTE = `${BASE}/catalog/notes.createDraft/versions/1`;
const DIGEST = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';
const PINNED = BUILTIN_CAPABILITIES[0];
const sha = value => createHash('sha256').update(value).digest('hex');

function removeDirectory(folder) {
  assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^soty-discovery-http-/u);
  rmSync(folder, { recursive: true, force: true });
}

function directory(t) {
  const parent = resolve(tmpdir()), folder = mkdtempSync(join(parent, 'soty-discovery-http-'));
  t?.after(() => removeDirectory(folder));
  return folder;
}

async function fixture(t, { entries = BUILTIN_CAPABILITIES, origin = 'https://soty.test', full = false, view } = {}) {
  const folder = directory();
  let app;
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const port = server.address().port, local = `http://127.0.0.1:${port}`;
  t.after(async () => {
    server.closeAllConnections(); await new Promise(done => server.close(done));
    await app?.locals.closeServices?.();
    removeDirectory(folder);
  });
  if (full) {
    const dist = join(folder, 'dist'); mkdirSync(dist);
    writeFileSync(join(dist, 'index.html'), '<!doctype html><title>ROOT_SPA_MARKER</title>');
    copyFileSync(resolve('public/capability-docs-update.js'), join(dist, 'capability-docs-update.js'));
    app = createHttpApp(dist, { dataDir: join(folder, 'data'), connectOrigins: [local], discoveryOrigin: local,
      appOriginTemplate: `http://{appId}.legacy.localhost:${port}`, namedAppZone: `http://named.localhost:${port}`,
      gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
  } else {
    app = express(); app.disable('x-powered-by');
    const catalog = view || createPublicDiscovery({ catalog: createCatalog(entries), documentation: fixtureDocumentation(entries) });
    attachCapabilitiesDiscovery(app, { catalog, origin });
    app.use((_req, res) => res.status(200).send('ROOT_SPA_MARKER'));
  }
  const call = (target, { method = 'GET', headers = {} } = {}) => new Promise((done, reject) => {
    const req = request({ hostname: '127.0.0.1', port, method, path: target, headers: { host: `127.0.0.1:${port}`, ...headers } }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => { const bytes = Buffer.concat(chunks); done({ status: res.statusCode, headers: res.headers,
        bytes, text: bytes.toString('utf8'), json: () => JSON.parse(bytes.toString('utf8')) }); });
    });
    req.on('error', reject); req.end();
  });
  return { call, local, port, folder };
}

test('full HTTP composition exposes exact public contracts without enabling Notes and reserves its namespaces', async t => {
  const f = await fixture(t, { full: true });
  const found = await f.call(`${BASE}/catalog?query=save+note`);
  assert.equal(found.status, 200); assert.equal(found.json().scope, 'public'); assert.equal(found.json().total, 1);
  assert.equal(found.json().items[0].executionEnabled, false);
  assert.equal(found.headers['access-control-allow-origin'], '*');
  assert.equal(found.headers['access-control-allow-credentials'], undefined);
  assert.equal(found.headers['set-cookie'], undefined);
  const detail = await f.call(NOTE); assert.equal(detail.status, 200);
  assert.equal(detail.json().capability.digest, DIGEST);
  assert.equal(detail.json().documentation.locales.en.title, 'Create a note');
  const contract = await f.call(`${NOTE}/contract.json`);
  assert.equal(sha(contract.bytes), DIGEST); assert.ok(!contract.text.endsWith('\n'));
  assert.equal(Object.hasOwn(contract.json(), 'executionEnabled'), false);
  for (const kind of ['input', 'output']) {
    const schema = await f.call(`${NOTE}/schemas/${kind}`);
    assert.deepEqual(schema.json(), PINNED[`${kind}Schema`]);
    assert.equal(schema.headers['content-type'], 'application/json; charset=utf-8');
  }
  const status = await f.call(`${BASE}/status`, { headers: { 'if-none-match': '*' } });
  assert.equal(status.status, 200); assert.equal(status.headers['cache-control'], 'no-store');
  assert.equal(status.headers.etag, undefined); assert.deepEqual(status.json(), { notesCreateEnabled: false, audience: null });
  for (const path of ['/agents/missing', `${BASE}/missing`, '/api/capabilities/v2/catalog']) {
    const result = await f.call(path); assert.equal(result.status, 404, path);
    assert.equal(result.json().error.code, 'not_found'); assert.equal(result.headers['cache-control'], 'no-store');
    assert.ok(!result.text.includes('ROOT_SPA_MARKER'));
  }
  assert.ok((await f.call('/ordinary')).text.includes('ROOT_SPA_MARKER'));
  const asset = await f.call('/capability-docs-update.js');
  assert.equal(asset.status, 200); assert.match(asset.text, /SOTY_PREPARE_UPDATE/u);
  assert.ok(!asset.text.includes('registration'));
});

test('HEAD and conditional GET select the same public representation before sending validators', async t => {
  const f = await fixture(t);
  for (const path of [`${BASE}/catalog`, NOTE, `${NOTE}/contract.json`, `${NOTE}/schemas/input`, '/agents', '/agents/sitemap.xml', `${BASE}/openapi.json`]) {
    const get = await f.call(path), head = await f.call(path, { method: 'HEAD' });
    assert.equal(get.status, 200, path); assert.equal(head.status, 200, path); assert.equal(head.bytes.length, 0);
    for (const header of ['etag', 'content-type', 'content-length', 'cache-control']) assert.equal(head.headers[header], get.headers[header], `${path}: ${header}`);
    assert.equal(Number(get.headers['content-length']), get.bytes.length);
    assert.equal(get.headers['cache-control'], 'public,no-cache');
    const same = await f.call(path, { headers: { 'if-none-match': `"other,opaque", W/${get.headers.etag}` } });
    assert.equal(same.status, 304, path); assert.equal(same.bytes.length, 0); assert.equal(same.headers.etag, get.headers.etag);
    const changed = await f.call(path, { headers: { 'if-none-match': '"other"' } }); assert.equal(changed.status, 200);
  }
  const hidden = await f.call(`${BASE}/catalog/private.secret/versions/1`, { headers: { 'if-none-match': '*' } });
  assert.equal(hidden.status, 404); assert.equal(hidden.headers.etag, undefined);
  const headError = await f.call(`${BASE}/missing`, { method: 'HEAD' });
  assert.equal(headError.status, 404); assert.equal(headError.bytes.length, 0); assert.equal(headError.headers.etag, undefined);
});

test('strict raw query/UTF-8 parsing rejects ambiguity without reflecting supplied values', async t => {
  const f = await fixture(t);
  for (const query of ['query=a&query=b', 'query=a&q%75ery=b', 'accountId=PRIVATE_MARKER', 'actor=PRIVATE_MARKER',
    '__proto__=PRIVATE_MARKER', 'query=%', 'query=%GG', 'query=%C3%28', 'query=%ED%A0%80', 'cursor=',
    'query=notes&&limit=1', 'limit=0', 'limit=21', 'limit=01', 'limit=1e1', 'query=' + 'x'.repeat(201)]) {
    const result = await f.call(`${BASE}/catalog?${query}`);
    assert.equal(result.status, 400, query); assert.match(result.json().error.code, /^(?:invalid_input|query_invalid|cursor_invalid)$/u);
    assert.ok(!result.text.includes('PRIVATE_MARKER')); assert.equal(result.headers['cache-control'], 'no-store');
  }
  assert.equal((await f.call(`${BASE}/catalog?query=save+note`)).json().total, 1);
  assert.equal((await f.call(`${BASE}/catalog?query=save%2Bnote`)).json().total, 0);
  assert.equal((await f.call(`${NOTE}?query=notes`)).status, 400);
  assert.equal((await f.call('/agents/%FF')).status, 400);
  const long = await f.call(`${BASE}/catalog?query=${'x'.repeat(9000)}`);
  assert.equal(long.status, 414); assert.equal(long.json().error.code, 'uri_too_long');
});

test('known read routes reject writes and unknown routes never fall through to SPA for another method', async t => {
  const f = await fixture(t);
  for (const method of ['POST', 'PUT', 'DELETE', 'OPTIONS']) {
    for (const path of [`${BASE}/catalog`, NOTE, `${BASE}/status`, '/agents', '/agents/sitemap.xml']) {
      const result = await f.call(path, { method });
      assert.equal(result.status, 405); assert.equal(result.headers.allow, 'GET, HEAD');
      assert.equal(result.json().error.code, 'method_not_allowed'); assert.equal(result.headers.etag, undefined);
    }
    assert.equal((await f.call(`${BASE}/missing`, { method })).status, 404);
  }
});

test('encoded public IDs containing slash and at-sign round-trip once and versions are canonical decimals', async t => {
  const id = 'sample/action@v1', f = await fixture(t, { entries: [{ ...PINNED, capabilityId: id }] });
  const item = (await f.call(`${BASE}/catalog`)).json().items[0];
  assert.equal(item.capabilityId, id); assert.match(item.links.detail, /sample%2Faction%40v1/u);
  assert.equal((await f.call(item.links.detail)).json().capability.capabilityId, id);
  assert.equal((await f.call(item.links.inputSchema)).status, 200);
  assert.equal((await f.call(item.links.html)).status, 200);
  assert.equal((await f.call(item.links.detail.replace('sample%2Faction%40v1', 'sample%252Faction%2540v1'))).status, 400);
  for (const version of ['0', '01', '%31', '1.0', '-1', '1000001']) {
    assert.equal((await f.call(`${BASE}/catalog/sample%2Faction%40v1/versions/${version}`)).status, 400, version);
  }
  assert.equal((await f.call(`${BASE}/catalog/sample%2Faction%40v1/versions/2`)).status, 404);
  assert.equal((await f.call(`${BASE}/catalog/%C0%AF/versions/1`)).status, 400);
});

test('private-only catalog changes cannot affect any public HTTP transcript, cursor, ETag, or sitemap', async t => {
  const second = { ...PINNED, capabilityId: 'second.note' };
  const hidden = { ...PINNED, capabilityId: 'private.secret', visibility: 'private', title: 'PRIVATE_MARKER' };
  let baseline;
  for (const privateEntries of [[], [hidden], [{ ...hidden, description: 'PRIVATE_CHANGED', executionEnabled: true }]]) {
    const f = await fixture(t, { entries: [...privateEntries, second, PINNED] });
    const transcript = [];
    for (const path of [`${BASE}/catalog?limit=1`, `${BASE}/catalog?query=PRIVATE_MARKER`, NOTE,
      `${NOTE}/contract.json`, `${NOTE}/schemas/input`, '/agents', '/agents/sitemap.xml']) {
      const result = await f.call(path, { headers: { authorization: 'Bearer test-only-ignored', cookie: 'account=test-only-ignored' } });
      transcript.push({ status: result.status, text: result.text, etag: result.headers.etag });
      assert.ok(!result.text.includes('PRIVATE_MARKER') && !result.text.includes('PRIVATE_CHANGED'));
    }
    const first = JSON.parse(transcript[0].text);
    assert.equal(first.total, 2); assert.ok(first.cursor);
    const last = await f.call(`${BASE}/catalog?limit=1&cursor=${first.cursor}`);
    assert.equal(last.json().items.length, 1); assert.equal(last.json().cursor, null);
    assert.notEqual(last.json().items[0].capabilityId, first.items[0].capabilityId);
    transcript.push({ status: last.status, text: last.text, etag: last.headers.etag });
    for (const suffix of ['', '/contract.json', '/schemas/input', '/schemas/output']) {
      const known = await f.call(`${BASE}/catalog/private.secret/versions/1${suffix}`, { headers: { 'if-none-match': '*' } });
      const unknown = await f.call(`${BASE}/catalog/not-present/versions/1${suffix}`);
      assert.equal(known.status, 404); assert.equal(known.text, unknown.text); assert.equal(known.headers.etag, undefined);
    }
    if (baseline) assert.deepEqual(transcript, baseline); else baseline = transcript;
  }
});

test('canonical origins come only from configuration and app hosts retain their earlier classifier', async t => {
  const f = await fixture(t, { full: true });
  const normal = await f.call('/agents');
  const spoof = await f.call('/agents', { headers: { forwarded: 'host=forged.invalid;proto=https',
    'x-forwarded-host': 'forged.invalid', 'x-forwarded-proto': 'https', origin: 'https://forged.invalid' } });
  assert.equal(spoof.status, 200); assert.equal(spoof.text, normal.text); assert.equal(spoof.headers.etag, normal.headers.etag);
  assert.ok(normal.text.includes(`${f.local}/agents`)); assert.ok(!spoof.text.includes('forged.invalid'));
  for (const host of [`unknown.named.localhost:${f.port}`, `deep.unknown.named.localhost:${f.port}`,
    `app-${'0'.repeat(32)}.legacy.localhost:${f.port}`]) {
    for (const path of ['/agents', `${BASE}/catalog`]) {
      const result = await f.call(path, { headers: { host } });
      assert.equal(result.status, 404); assert.match(result.text, /app_not_found/u);
      assert.ok(!result.text.includes('notes.createDraft')); assert.equal(result.headers['x-frame-options'], undefined);
    }
  }
  const unconfigured = await fixture(t, { origin: '' });
  assert.equal((await unconfigured.call('/agents')).status, 200);
  assert.equal((await unconfigured.call('/agents/sitemap.xml')).status, 404);
  assert.ok(!(await unconfigured.call('/agents')).text.includes('rel="canonical"'));
});

test('invalid origins and missing public documentation fail before any application storage is opened', t => {
  const folder = directory(t), dataDir = join(folder, 'untouched');
  const shellOrigins = ['https://soty.test', 'http://127.0.0.1:8080'];
  assert.equal(validateDiscoveryOrigin({ discoveryOrigin: '', shellOrigins }), '');
  assert.equal(validateDiscoveryOrigin({ discoveryOrigin: 'https://soty.test/', shellOrigins }), 'https://soty.test');
  assert.equal(validateDiscoveryOrigin({ discoveryOrigin: 'http://127.0.0.1:8080', shellOrigins }), 'http://127.0.0.1:8080');
  for (const discoveryOrigin of ['http://127.0.0.2:8080', 'http://docs.localhost:8080']) {
    assert.throws(() => createHttpApp(resolve('public'), { dataDir, connectOrigins: [discoveryOrigin], discoveryOrigin,
      appOriginTemplate: '', namedAppZone: '', gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } }), { code: 'discovery_origin_https_required' });
    assert.equal(existsSync(dataDir), false, 'an origin rejected by Connect must fail before storage');
  }
  assert.throws(() => validateDiscoveryOrigin({ discoveryOrigin: 'https://soty.test', shellOrigins: ['https://soty.test', 'https://other.test/'] }), { code: 'discovery_origin_shell_required' });
  for (const discoveryOrigin of ['https://soty.test/agents', 'https://soty.test/.', 'https://soty.test?x=1',
    'https://soty.test#x', 'https://name:pass@soty.test', 'https://unconfigured.test', 'http://soty.test', 'https://soty.test%2F']) {
    assert.throws(() => createHttpApp(resolve('public'), { dataDir, connectOrigins: shellOrigins, discoveryOrigin,
      appOriginTemplate: '', namedAppZone: '', gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } }), error => /^discovery_origin_/u.test(error.code));
    assert.equal(existsSync(dataDir), false);
  }
  const databasePath = join(dataDir, 'capabilities.sqlite');
  assert.throws(() => createCapabilitiesService({ databasePath, projectId: 'discovery_test', actorActive: () => false, documentation: [] }), { code: 'documentation_missing' });
  assert.equal(existsSync(dataDir), false);
});

test('HTTP output bounds and unexpected errors fail closed without leaking internal exception data', async t => {
  const f = await fixture(t, { view: {
    search() { return { privateInternalValue: 'x'.repeat(70 * 1024) }; },
    get() { throw new Error('SENSITIVE_INTERNAL_MARKER'); },
    contract() { throw new AccessError('not_found'); },
  } });
  const large = await f.call(`${BASE}/catalog`); assert.equal(large.status, 500);
  assert.deepEqual(large.json(), { error: { code: 'projection_too_large' } }); assert.equal(large.headers.etag, undefined);
  const error = await f.call(NOTE); assert.equal(error.status, 500);
  assert.deepEqual(error.json(), { error: { code: 'internal_error' } }); assert.ok(!error.text.includes('SENSITIVE_INTERNAL_MARKER'));
});
