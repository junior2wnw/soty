import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { BUILTIN_CAPABILITIES, createCatalog } from '../server/catalog.mjs';
import { BUILTIN_DOCUMENTATION } from '../server/documentation.mjs';
import { createPublicDiscovery } from '../server/discovery.mjs';
import { renderDiscoveryIndex, renderCapabilityPage, DISCOVERY_HTML_LIMITS } from '../server/discovery-pages.mjs';
import { buildDiscoveryOpenApi } from '../server/openapi.mjs';
import { canonicalJson } from '../server/validation.mjs';

const note = BUILTIN_CAPABILITIES[0], args = { capabilityId: 'notes.createDraft', version: 1 };
const clone = value => JSON.parse(JSON.stringify(value));
const fail = code => error => error?.code === code && error.message === code;
function fixture(entries = BUILTIN_CAPABILITIES, edit = () => {}) {
  const catalog = createCatalog(entries), documentation = catalog.listPublic().map(capability => ({
    ...clone(BUILTIN_DOCUMENTATION[0]), capabilityId: capability.capabilityId, version: capability.version, contractDigest: capability.digest,
  }));
  edit(documentation);
  const view = createPublicDiscovery({ catalog, documentation });
  return { catalog, view };
}
function walk(value, visit) {
  if (!value || typeof value !== 'object') return;
  visit(value); for (const child of Object.values(value)) walk(child, visit);
}
const decodeHtml = value => value.replaceAll('&amp;', '&').replaceAll('&quot;', '"').replaceAll('&#39;', "'").replaceAll('&lt;', '<').replaceAll('&gt;', '>');

test('OpenAPI is frozen 3.1.2, contains only actual public GET paths, and all references resolve locally', () => {
  const doc = buildDiscoveryOpenApi(), prefix = '/api/capabilities/v1';
  assert.equal(doc.openapi, '3.1.2'); assert.equal(doc.jsonSchemaDialect, 'https://json-schema.org/draft/2020-12/schema');
  assert.deepEqual(doc.servers, [{ url: '/' }]); assert.deepEqual(doc.security, []);
  assert.deepEqual(Object.keys(doc.paths).sort(), ['/catalog', '/catalog/{id}/versions/{version}',
    '/catalog/{id}/versions/{version}/contract.json', '/catalog/{id}/versions/{version}/schemas/{kind}', '/openapi.json', '/status'].map(path => prefix + path).sort());
  const operations = new Set();
  for (const item of Object.values(doc.paths)) {
    assert.deepEqual(Object.keys(item), ['get']); assert.deepEqual(item.get.security, []);
    assert.equal(Object.hasOwn(item.get, 'requestBody'), false); assert.ok(item.get.responses['200']);
    assert.equal(operations.has(item.get.operationId), false); operations.add(item.get.operationId);
  }
  walk(doc, object => {
    if (!Object.hasOwn(object, '$ref')) return;
    assert.match(object.$ref, /^#\/components\/(schemas|parameters|responses)\//u);
    let selected = doc; for (const key of object.$ref.slice(2).split('/')) selected = selected?.[key];
    assert.ok(selected, `unresolved local reference ${object.$ref}`);
  });
  assert.equal(Object.hasOwn(doc.components, 'securitySchemes'), false);
  assert.deepEqual(doc.components.schemas.Status.properties.notesCreateEnabled, { const: false });
  assert.equal(doc.components.schemas.Documentation.properties.validation.const.stringLength.runtime, 'utf16-code-units');
  assert.throws(() => { doc.servers[0].url = 'https://foreign.invalid'; }, TypeError);
  assert.throws(() => { doc.paths['/write'] = {}; }, TypeError);
  assert.ok(Buffer.byteLength(canonicalJson(doc)) < 128 * 1024);
  assert.equal(buildDiscoveryOpenApi(), doc, 'one immutable document, no request-derived cache');
});

test('installed independent JSON Schema 2020-12 validator accepts real public DTOs and keeps Unicode semantics distinct', t => {
  const probe = spawnSync('python', ['-c', 'import jsonschema'], { encoding: 'utf8', timeout: 10000, windowsHide: true });
  if (probe.status !== 0) { t.skip('Python jsonschema is not installed; no dependency is installed by this test'); return; }
  const { view, catalog } = fixture(), detail = view.get(args), page = view.search();
  const samples = [
    ['CatalogPage', page, true], ['CapabilityDetail', detail, true],
    ['SemanticContract', JSON.parse(view.contract(args).canonicalJson), true],
    ['SchemaDocument', view.schema({ ...args, kind: 'input' }), true],
    ['SchemaDocument', view.schema({ ...args, kind: 'output' }), true],
    ['Documentation', detail.documentation, true], ['Status', { notesCreateEnabled: false, audience: null }, true],
    ['Error', { error: { code: 'cursor_invalid' } }, true], ['OpenApiDocument', buildDiscoveryOpenApi(), true],
    ['CatalogPage', { ...page, privateCount: 3 }, false], ['CatalogPage', { ...page, items: [{ ...page.items[0], match: 'model-opinion' }] }, false],
    ['CapabilityDetail', { ...detail, actor: { accountId: 'hidden' } }, false],
    ['SemanticContract', { ...JSON.parse(view.contract(args).canonicalJson), executionEnabled: false }, false],
    ['Status', { notesCreateEnabled: true, audience: null }, false], ['Error', { error: { code: 'invalid_input', request: 'private' } }, false],
  ];
  const python = `import json,sys,importlib.metadata
from jsonschema import Draft202012Validator
p=json.load(sys.stdin)
for schema in p['document']['components']['schemas'].values(): Draft202012Validator.check_schema(schema)
for name,value,expected in p['samples']:
    schema={'$schema':'https://json-schema.org/draft/2020-12/schema','$ref':'#/components/schemas/'+name,'components':p['document']['components']}
    assert Draft202012Validator(schema).is_valid(value)==expected, name
assert Draft202012Validator(p['inputSchema']).is_valid({'title':'😀'*81,'body':''})
print('jsonschema '+importlib.metadata.version('jsonschema')+': real DTO and Unicode boundary PASS')
`;
  const oracle = spawnSync('python', ['-c', python], { input: JSON.stringify({ document: buildDiscoveryOpenApi(), samples,
    inputSchema: view.schema({ ...args, kind: 'input' }) }), encoding: 'utf8', timeout: 10000, windowsHide: true });
  assert.equal(oracle.status, 0, oracle.stderr); assert.match(oracle.stdout, /real DTO and Unicode boundary PASS/u);
  assert.throws(() => catalog.validateInput(catalog.get(args.capabilityId, 1), { title: '😀'.repeat(81), body: '' }), { code: 'invalid_input' });
});

test('real Notes SSR exposes a native search form and exact ordinary links with no JavaScript dependency', () => {
  const { view } = fixture(), detail = view.get(args);
  const index = renderDiscoveryIndex({ result: view.search(), origin: 'https://soty.example' });
  const page = renderCapabilityPage({ detail, origin: 'https://soty.example' });
  assert.equal([...index.matchAll(/Выполнение пока недоступно/gu)].length, 1,
    'execution status has its own badge and is not repeated in the purpose summary');
  assert.doesNotMatch(detail.documentation.locales.en.summary, /Execution is not available/u);
  for (const output of [index, page]) {
    assert.match(output, /^<!doctype html><html lang="ru">/u); assert.match(output, /<main id="main"/u);
    assert.match(output, /name="viewport" content="width=device-width,initial-scale=1"/u);
    const scripts = [...output.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gu)];
    assert.equal(scripts.length, 1); assert.equal(scripts[0][1], ' defer src="/capability-docs-update.js"'); assert.equal(scripts[0][2], '');
    assert.doesNotMatch(output, /<(?:iframe|canvas)\b|\son(?:click|load|error|submit)=/u);
    assert.ok(output.includes('Выполнение пока недоступно'));
  }
  const withoutScript = index.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gu, '');
  assert.match(withoutScript, /<form[^>]*method="get" action="\/agents" role="search">/u);
  assert.match(withoutScript, /<label for="capability-query">/u); assert.match(withoutScript, /name="query"/u);
  assert.ok(withoutScript.includes(`href="${detail.links.html}"`));
  for (const key of ['detail', 'contract', 'inputSchema', 'outputSchema']) assert.ok(page.includes(`href="${detail.links[key]}"`));
  assert.ok(page.includes(detail.capability.digest)); assert.match(page, /lang="en"/u);
  assert.ok(page.includes('Иллюстрация результата')); assert.ok(page.includes('Illustrative result'));
  assert.doesNotMatch(page, /<button\b/u, 'the capability page offers reading, not a pretend run or connect control');
  assert.ok(Buffer.byteLength(index) <= DISCOVERY_HTML_LIMITS.index); assert.ok(Buffer.byteLength(page) <= DISCOVERY_HTML_LIMITS.detail);
});

test('search query, sidecar titles and examples are escaped as text and never become markup, script or a destination', () => {
  const hostile = '</script><img src=x onerror="STEAL()"> & \' ", \u202e https://outside.invalid/';
  const { view } = fixture(BUILTIN_CAPABILITIES, docs => {
    docs[0].locales.ru.title = 'Записка <b>текст</b>';
    docs[0].locales.ru.summary = hostile; docs[0].locales.en.notFor = [hostile];
    docs[0].locales.ru.examples[0].input.body = '</code><script>STEAL()</script>\n"quoted"';
  });
  const query = '"><img src=x onerror=STEAL()> & \'', index = renderDiscoveryIndex({ result: view.search(), query });
  const detail = renderCapabilityPage({ detail: view.get(args) });
  for (const output of [index, detail]) {
    assert.doesNotMatch(output, /<img\b|<b>текст<\/b>|<script>STEAL/u);
    assert.match(output, /&lt;\/script&gt;&lt;img src=x onerror=&quot;STEAL\(\)&quot;&gt;/u);
    assert.doesNotMatch(output, /href="https:\/\/outside\.invalid/u);
    assert.equal([...output.matchAll(/<script\b/gu)].length, 1);
  }
  assert.match(index, /value="&quot;&gt;&lt;img src=x onerror=STEAL\(\)&gt; &amp; &#39;"/u);
  assert.ok(detail.includes('&lt;/code&gt;&lt;script&gt;STEAL()&lt;/script&gt;'));
  assert.ok(detail.includes('\u202e'), 'well-formed bidi text is preserved as data, not silently normalized');
});

test('only an explicitly canonical trusted origin can create absolute canonical links', () => {
  const { view } = fixture(), result = view.search(), detail = view.get(args);
  for (const origin of ['https://soty.example', 'https://soty.example:8443', 'http://localhost:5300', 'http://127.0.0.1:5300', 'http://[::1]:5300']) {
    assert.ok(renderDiscoveryIndex({ result, origin }).includes(`rel="canonical" href="${origin}/agents"`));
    assert.ok(renderCapabilityPage({ detail, origin }).includes(`rel="canonical" href="${origin}${detail.links.html}"`));
  }
  assert.doesNotMatch(renderDiscoveryIndex({ result }), /rel="canonical"/u);
  for (const origin of ['//soty.example', 'javascript:alert(1)', 'data:text/html,test', 'http://remote.example', 'http://127.0.0.2',
    'http://sub.localhost', 'https://soty.example/', 'https://soty.example/path', 'https://u:p@soty.example',
    'https://soty.example?x=1', 'https://soty.example#fragment', 'https://SOTY.example', 'https://soty.example\\escape', 'https://soty.example\n', null, []])
    assert.throws(() => renderDiscoveryIndex({ result, origin }), fail('discovery_render_invalid'));
});

test('encoded capability IDs stay one segment, and forged metadata links never navigate out of the public route', () => {
  const capabilityId = 'vendor/app@create:note', { view } = fixture([{ ...note, capabilityId }]);
  const detail = view.get({ capabilityId, version: 1 }), output = renderCapabilityPage({ detail });
  assert.ok(output.includes('/api/capabilities/v1/catalog/vendor%2Fapp%40create%3Anote/versions/1/schemas/input'));
  for (const malicious of ['javascript:alert(1)', '//outside.invalid', '/safe" onmouseover="STEAL()', '/api/capabilities/v1/catalog/other/versions/1']) {
    const altered = clone(detail); altered.links.inputSchema = malicious;
    assert.throws(() => renderCapabilityPage({ detail: altered }), fail('discovery_render_invalid'));
  }
  const privateDetail = clone(detail); privateDetail.capability.visibility = 'private';
  assert.throws(() => renderCapabilityPage({ detail: privateDetail }), fail('discovery_render_invalid'));
  const actorPage = { ...view.search(), scope: 'account' };
  assert.throws(() => renderDiscoveryIndex({ result: actorPage }), fail('discovery_render_invalid'));
});

test('native next-page link preserves query and chosen limit, and search/cursor pages are noindex', () => {
  const { view } = fixture(Array.from({ length: 12 }, (_, i) => ({ ...note, capabilityId: `note.item${i}` })));
  const query = 'ＮＯＴＥ', result = view.search({ query, limit: 7 }), output = renderDiscoveryIndex({ result, query, limit: 7 });
  const rawHref = /rel="next" href="([^"]+)"/u.exec(output)?.[1]; assert.ok(rawHref);
  const url = new URL(decodeHtml(rawHref), 'https://test.invalid');
  assert.equal(url.pathname, '/agents'); assert.equal(url.searchParams.get('query'), query);
  assert.equal(url.searchParams.get('limit'), '7'); assert.equal(url.searchParams.get('cursor'), result.cursor);
  assert.deepEqual([...url.searchParams.keys()], ['query', 'limit', 'cursor']);
  assert.match(output, /name="limit" value="7"/u); assert.match(output, /content="noindex,follow"/u);
  assert.match(renderDiscoveryIndex({ result: view.search(), noindex: true }), /content="noindex,follow"/u);
  assert.match(renderDiscoveryIndex({ result: view.search() }), /content="index,follow"/u);
  assert.throws(() => renderDiscoveryIndex({ result, limit: '7' }), fail('discovery_render_invalid'));
  assert.throws(() => renderDiscoveryIndex({ result, accountId: 'spoof' }), fail('discovery_render_invalid'));
});

test('an empty or exhausted public page stays honest and gives a real manual return link', () => {
  const { view } = fixture();
  const empty = renderDiscoveryIndex({ result: view.search({ query: 'weather forecast' }), query: 'weather forecast' });
  assert.ok(empty.includes('Ничего не найдено')); assert.ok(empty.includes('Найдено: 0')); assert.doesNotMatch(empty, /<article class="card">/u);
  assert.match(empty, /href="\/agents">Все возможности/u);
  const base = view.search(), exhausted = renderDiscoveryIndex({ result: { ...base, items: [] }, query: 'note', limit: 1 });
  assert.ok(exhausted.includes('На этой странице нет записей')); assert.ok(exhausted.includes('К началу поиска'));
});

test('actual escaped page bytes are capped; near-limit schemas remain exact linked documents rather than inline bulk', () => {
  const many = Array.from({ length: 20 }, (_, i) => ({ ...note, capabilityId: `big.item${i}` }));
  const { view } = fixture(many, docs => { for (const doc of docs) doc.locales.ru.summary = "'".repeat(3000); });
  const page = view.search({ limit: 20 }); assert.ok(page.items.length > 12);
  assert.throws(() => renderDiscoveryIndex({ result: page, limit: 20 }), fail('projection_too_large'));
  const schema = { type: 'string', maxLength: 100000, enum: Array.from({ length: 60 }, (_, i) => `SCHEMA_ONLY_${i}_${'x'.repeat(4000)}`) };
  const big = fixture([{ ...note, outputSchema: schema }], docs => {
    for (const locale of Object.values(docs[0].locales)) locale.examples = [];
  });
  assert.ok(Buffer.byteLength(canonicalJson(big.view.get(args))) > 240000);
  const output = renderCapabilityPage({ detail: big.view.get(args) });
  assert.ok(Buffer.byteLength(output) < 25000); assert.doesNotMatch(output, /SCHEMA_ONLY_/u);
  assert.deepEqual(big.view.schema({ ...args, kind: 'output' }), schema);
  const oversized = clone(fixture().view.get(args)); oversized.documentation.locales.ru.useWhen = Array(6000).fill('<&>'.repeat(6));
  assert.throws(() => renderCapabilityPage({ detail: oversized }), fail('projection_too_large'));
});

test('SSR affordances include keyboard focus, reduced motion, visible labels and bounded narrow-screen layout rules', () => {
  const { view } = fixture(), index = renderDiscoveryIndex({ result: view.search() }), detail = renderCapabilityPage({ detail: view.get(args) });
  for (const output of [index, detail]) {
    assert.match(output, /:focus-visible\{outline:3px solid var\(--sw-focus\)/u);
    assert.match(output, /prefers-reduced-motion:reduce/u); assert.match(output, /min-height:44px/u);
    assert.match(output, /@media\(max-width:380px\)/u); assert.match(output, /max-height:420px/u);
    assert.match(output, /white-space:pre-wrap;overflow-wrap:anywhere/u);
    assert.match(output, /href="#main">К содержанию/u); assert.match(output, /aria-hidden="true" focusable="false"/u);
  }
  const outline = [...detail.matchAll(/<h([1-6])(?:\s|>)/gu)].map(match => Number(match[1]));
  assert.equal(outline[0], 1);
  outline.forEach((level, index) => assert.ok(index === 0 || level <= outline[index - 1] + 1,
    `heading level jumps from H${outline[index - 1]} to H${level}`));
  // Source-level affordances only. Root's separate real-browser gate owns
  // measured 320/667 geometry, keyboard navigation and JS-disabled operation.
});
