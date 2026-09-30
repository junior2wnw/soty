import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { buildCapabilitiesOpenApi } from '../capabilities-openapi.js';
import { buildDiscoveryOpenApi } from '../../modules/capabilities/server/openapi.mjs';
import { nativeHttpFixture, nativeIdentity } from './support/native-capability-http.mjs';

const BASE = '/api/capabilities/v1', CREATE = `${BASE}/notes/drafts`;
function walk(value, fn) { if (value && typeof value === 'object') { fn(value); for (const child of Object.values(value)) walk(child, fn); } }

test('the composed OpenAPI names exactly the attached private operations without changing the standalone discovery contract', () => {
  const before = JSON.stringify(buildDiscoveryOpenApi()), doc = buildCapabilitiesOpenApi();
  assert.equal(JSON.stringify(buildDiscoveryOpenApi()), before);
  assert.equal(doc.openapi, '3.1.2'); assert.deepEqual(doc.servers, [{ url: '/' }]);
  assert.equal(doc.paths[CREATE].post.operationId, 'createNoteDraft');
  assert.deepEqual(Object.keys(doc.paths[CREATE]), ['post']);
  assert.deepEqual(Object.keys(doc.paths[`${BASE}/invocations/{invocationId}`]), ['get']);
  for (const key of [CREATE, `${BASE}/invocations/{invocationId}`]) {
    assert.deepEqual(Object.values(doc.paths[key])[0].security, [{ CapabilityBearer: [] }]);
  }
  for (const key of Object.keys(buildDiscoveryOpenApi().paths)) assert.deepEqual(doc.paths[key].get.security, []);
  const ids = new Set();
  for (const path of Object.values(doc.paths)) for (const operation of Object.values(path)) {
    assert.equal(ids.has(operation.operationId), false); ids.add(operation.operationId);
  }
  walk(doc, value => {
    if (!value.$ref) return;
    assert.match(value.$ref, /^#\/components\/(schemas|parameters|responses)\//u);
    assert.ok(value.$ref.slice(2).split('/').reduce((selected, key) => selected?.[key], doc));
  });
  assert.throws(() => { doc.paths[CREATE].post.security = []; }, TypeError);
  assert.ok(Buffer.byteLength(JSON.stringify(doc)) < 256 * 1024);
  assert.match(doc.info.description, /No OAuth, MCP/u);
});

test('real HTTP responses validate with an independent JSON Schema 2020-12 implementation and expose no owner data in the public document', async t => {
  const probe = spawnSync('python', ['-c', 'import jsonschema'], { encoding: 'utf8', timeout: 10000, windowsHide: true });
  if (probe.status !== 0) { t.skip('Python jsonschema is not installed; no dependency is installed by this test'); return; }
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Автор'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId);
  const documentResponse = await f.http(`${BASE}/openapi.json`), doc = documentResponse.body;
  assert.equal(documentResponse.status, 200); assert.ok(documentResponse.headers.etag);
  assert.equal(documentResponse.headers['cache-control'], 'public,no-cache');
  assert.equal(doc['x-soty-mcp'].endpoint, '/mcp', 'composed host documents its mounted MCP transport independently of native readiness');
  assert.deepEqual(doc['x-soty-mcp'].protocolVersions, ['2026-07-28', '2025-11-25']);
  assert.equal(doc['x-soty-oauth-discovery'], undefined, 'no configured AS is invented for an ordinary service host');
  assert.doesNotMatch(doc.info.description, /No OAuth, MCP/u);
  const input = { title: 'Проверка схемы', body: 'Точный ответ', idempotencyKey: 'openapi-contract-test-01' };
  const created = await f.http(CREATE, { method: 'POST', token: identity.token, body: JSON.stringify(input) });
  assert.equal(created.status, 201);
  const history = await f.http(`${BASE}/invocations/${created.body.invocation.invocationId}`, { token: identity.token });
  const ready = await f.http(`${BASE}/status`);
  await f.restart({ enabled: false });
  const disabled = await f.http(`${BASE}/status`), unchanged = await f.http(`${BASE}/openapi.json`, { headers: { 'if-none-match': documentResponse.headers.etag } });
  assert.equal(unchanged.status, 304, 'operational readiness is not a request-specific OpenAPI cache');
  const samples = [
    ['NativeDraftRequest', input, true], ['NativeCreateResponse', created.body, true], ['NativeReadResponse', history.body, true],
    ['Status', ready.body, true], ['Status', disabled.body, true],
    ['NativeDraftRequest', { ...input, accountId: account.accountId }, false],
    ['NativeCreateResponse', { ...created.body, input }, false],
    ['NativeReadResponse', { ...history.body, reused: true }, false],
    ['NativeInvocation', { ...history.body.invocation, input }, false],
    ['NativeReceipt', { ...history.body.invocation.receipt, body: 'private' }, false],
  ];
  const script = `import json,sys
from jsonschema import Draft202012Validator
p=json.load(sys.stdin)
for value in p['document']['components']['schemas'].values(): Draft202012Validator.check_schema(value)
for name,value,expected in p['samples']:
    schema={'$schema':'https://json-schema.org/draft/2020-12/schema','$ref':'#/components/schemas/'+name,'components':p['document']['components']}
    assert Draft202012Validator(schema).is_valid(value)==expected, name
print('native HTTP JSON Schema PASS')
`;
  const oracle = spawnSync('python', ['-c', script], { input: JSON.stringify({ document: doc, samples }),
    encoding: 'utf8', timeout: 10000, windowsHide: true });
  assert.equal(oracle.status, 0, oracle.stderr); assert.match(oracle.stdout, /native HTTP JSON Schema PASS/u);
  for (const value of [identity.token, account.accountId, created.body.invocation.invocationId]) assert.equal(JSON.stringify(doc).includes(value), false);
});
