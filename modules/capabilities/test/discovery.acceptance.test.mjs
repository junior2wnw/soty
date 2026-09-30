import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createCatalog } from '../server/catalog.mjs';
import { createPublicDiscovery } from '../server/discovery.mjs';

// Independent declarations: no author fixtures, projection helpers or serializer.
const bytes = value => Buffer.byteLength(JSON.stringify(value), 'utf8');
const hash = value => createHash('sha256').update(value, 'utf8').digest('hex');
const ref = entry => ({ capabilityId: entry.capabilityId, version: entry.version });
const code = expected => error => error?.code === expected && error.message === expected;
const notesRef = { capabilityId: 'notes.createDraft', version: 1 };
const noteDigest = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';

function declaration(capabilityId, version = 1, overrides = {}) {
  return {
    capabilityId, version, appId: 'independent', title: 'Public document operation',
    description: 'Read this declared public contract.', visibility: 'public', executionEnabled: false,
    inputSchema: { type: 'object', properties: { text: { type: 'string', maxLength: 64 } }, required: ['text'], additionalProperties: false },
    outputSchema: { type: 'object', properties: { accepted: { type: 'boolean' } }, required: ['accepted'], additionalProperties: false },
    resources: ['document:new'], effects: ['create'], recipients: ['soty:independent'],
    executionBinding: { kind: 'native', handler: 'independent.create', version: 1 }, ...overrides,
  };
}

function documentation(catalog) {
  return catalog.listPublic().map(entry => ({
    ...ref(entry), contractDigest: entry.digest,
    locales: {
      ru: { title: 'Публичный контракт', summary: 'Описание операции без права исполнения.',
        useWhen: ['Найти контракт операции.'], notFor: ['NOT_FOR_SENTINEL'],
        examples: [{ input: { text: 'пример' }, output: { accepted: true } }] },
      en: { title: 'Public contract', summary: 'A declared operation; this is not execution authority.',
        useWhen: ['Find the contract.'], notFor: ['NEGATIVE_ONLY_SENTINEL'],
        examples: [{ input: { text: 'example' }, output: { accepted: true } }] },
    },
    keywords: { ru: ['публичный', 'контракт'], en: ['public', 'contract', 'café'] },
  }));
}

function fixture(entries, amend = () => {}) {
  const catalog = createCatalog(entries), docs = documentation(catalog);
  amend(docs);
  return { catalog, docs, view: createPublicDiscovery({ catalog, documentation: docs }) };
}

function pages(view, query = '', limit = 2) {
  const result = [];
  let cursor;
  do {
    const page = view.search({ query, limit, ...(cursor ? { cursor } : {}) });
    result.push(page); cursor = page.cursor;
    assert.ok(result.length <= 128, 'the finite public result must terminate');
  } while (cursor);
  return result;
}

function transcript(view, entries) {
  return JSON.stringify({
    queries: ['', 'PUBLIC CONTRACT', 'ＣＡＦＥ\u0301', 'PRIVATE_MARKER', 'NEGATIVE_ONLY_SENTINEL']
      .map(query => ({ query, pages: pages(view, query) })),
    versions: entries.map(entry => ({ detail: view.get(ref(entry)), contract: view.contract(ref(entry)),
      input: view.schema({ ...ref(entry), kind: 'input' }), output: view.schema({ ...ref(entry), kind: 'output' }) })),
  });
}

test('a complete public transcript and its continuation survive private versions, private sidecars and registry reorder', () => {
  const publicEntries = [declaration('vendor/read@document', 1), declaration('vendor/read@document', 3),
    declaration('tools.search', 10), declaration('tools.search', 2), declaration('z.last')];
  const baseline = fixture(publicEntries), expected = transcript(baseline.view, publicEntries);
  const first = baseline.view.search({ limit: 2 });
  for (const secretTitle of ['PRIVATE_MARKER first state', '\ud800', 'PRIVATE_MARKER changed']) {
    const secret = declaration('vendor/read@document', 2, { visibility: 'private', executionEnabled: true,
      title: secretTitle, description: 'PRIVATE_MARKER must not become searchable' });
    const catalog = createCatalog([declaration('private.only', 1, { visibility: 'private' }), ...publicEntries, secret].reverse());
    const docs = documentation(catalog);
    docs.push({ capabilityId: secret.capabilityId, version: 2, contractDigest: 'wrong',
      locales: { ru: { title: '\ud800'.repeat(20000) } }, keywords: { en: ['PRIVATE_MARKER'] } });
    docs.push(...Array.from({ length: 259 }, (_, i) => ({ capabilityId: `missing.${i}`, version: 1,
      arbitrary: null, contractDigest: false, locales: '\udc00' })));
    const view = createPublicDiscovery({ catalog, documentation: docs });
    assert.equal(transcript(view, publicEntries), expected);
    assert.deepEqual(view.search({ limit: 2, cursor: first.cursor }), baseline.view.search({ limit: 2, cursor: first.cursor }));
    for (const hidden of [ref(secret), { capabilityId: 'unknown.same-shape', version: 2 }]) {
      for (const method of ['get', 'contract', 'schema']) assert.throws(() => view[method]({ ...hidden,
        ...(method === 'schema' ? { kind: 'output' } : {}) }), code('not_found'));
    }
  }
});

test('visibility changes need exact-version documentation and do not change an already published snapshot', () => {
  const a = declaration('same/id@action', 1), hidden = declaration(a.capabilityId, 2, { visibility: 'private' });
  const before = fixture([a, hidden]), saved = transcript(before.view, [a]);
  const promoted = createCatalog([a, { ...hidden, visibility: 'public' }]);
  assert.throws(() => createPublicDiscovery({ catalog: promoted, documentation: before.docs }), code('documentation_missing'));
  const after = createPublicDiscovery({ catalog: promoted, documentation: documentation(promoted) });
  assert.equal(after.search().total, 2);
  assert.notEqual(after.search().revision, before.view.search().revision);
  assert.equal(transcript(before.view, [a]), saved);
  assert.throws(() => before.view.get(ref(hidden)), code('not_found'));
});

test('actual UTF-8 page clipping remains complete when callers change page sizes between continuations', () => {
  const entries = Array.from({ length: 73 }, (_, i) => declaration(`batch${String(i).padStart(2, '0')}/${'@'.repeat(135)}`));
  const { view } = fixture(entries, docs => { for (const doc of docs) doc.locales.ru.summary = '界'.repeat(340); });
  const first = view.search({ limit: 20 });
  assert.ok(first.items.length > 0 && first.items.length < 20, 'exercise the byte cut, not ordinary count pagination');
  assert.ok(bytes(first) > 60000, 'this fixture reaches the actual 64KiB page boundary');
  const seen = [], cursors = new Set();
  let page = first, pass = 0;
  while (true) {
    assert.ok(bytes(page) <= 64 * 1024);
    assert.equal(page.total, entries.length);
    for (const item of page.items) {
      assert.ok(bytes(item) <= 4096);
      seen.push(item.capabilityId);
      for (const link of Object.values(item.links)) {
        const url = new URL(link, 'https://independent.invalid');
        assert.equal(url.origin, 'https://independent.invalid');
        assert.equal(url.search, ''); assert.equal(url.hash, '');
        const segments = url.pathname.split('/');
        assert.equal(decodeURIComponent(segments[segments.indexOf('versions') - 1] || ''), item.capabilityId,
          'document links keep a whole encoded ID in one segment');
      }
    }
    if (!page.cursor) break;
    assert.ok(page.cursor.length <= 512); assert.match(page.cursor, /^[A-Za-z0-9_-]+$/u);
    assert.equal(cursors.has(page.cursor), false); cursors.add(page.cursor);
    const limit = [1, 7, 20][pass++ % 3];
    assert.ok(pass < 128);
    page = view.search({ limit, cursor: page.cursor });
  }
  assert.deepEqual(seen, entries.map(entry => entry.capabilityId));
  assert.equal(new Set(seen).size, entries.length);
});

test('cursor binding survives normalized spelling but rejects changed queries, revisions and malformed envelopes', () => {
  const entries = Array.from({ length: 5 }, (_, i) => declaration(`cursor.item${i}`)), { view } = fixture(entries);
  const first = view.search({ query: 'ＣＡＦＥ\u0301', limit: 1 });
  assert.ok(first.cursor);
  assert.deepEqual(view.search({ query: 'café', cursor: first.cursor }),
    view.search({ query: 'ＣＡＦＥ\u0301', cursor: first.cursor }));
  assert.throws(() => view.search({ query: 'contract', cursor: first.cursor }), code('cursor_invalid'));
  const value = JSON.parse(Buffer.from(first.cursor, 'base64url').toString('utf8'));
  const cursorFor = (offset, patch = {}) => Buffer.from(JSON.stringify({ offset, queryHash: value.queryHash,
    revision: value.revision, ...patch })).toString('base64url');
  for (const cursor of [first.cursor + '=', 'A'.repeat(513), '\u00e9', Buffer.from([0xc0, 0xaf]).toString('base64url'),
    Buffer.from('{"offset":1,"offset":2,"queryHash":"x","revision":"y"}').toString('base64url'),
    cursorFor(-1), cursorFor(1.1), cursorFor(6), cursorFor(1, { actor: 'PRIVATE_ACCOUNT' })]) {
    assert.throws(() => view.search({ query: 'café', cursor }), code('cursor_invalid'));
  }
  assert.equal(view.search({ query: 'café', cursor: cursorFor(4) }).items[0].capabilityId, entries[4].capabilityId);
  assert.deepEqual(view.search({ query: 'café', cursor: cursorFor(5) }).items, []);
  for (const changed of [fixture(entries, docs => { docs[0].locales.en.notFor.push('New limitation.'); }),
    fixture(entries.map((entry, i) => ({ ...entry, executionEnabled: i === 0 })))]) {
    assert.throws(() => changed.view.search({ query: 'café', cursor: first.cursor }), code('cursor_invalid'));
    assert.deepEqual(changed.view.contract(ref(entries[0])), view.contract(ref(entries[0])));
  }
});

test('public methods reject injected identity without reflecting credentials or widening private discovery', () => {
  const visible = declaration('safe.public'), secret = declaration('safe.private', 1, { visibility: 'private' });
  const { view } = fixture([visible, secret]);
  for (const extra of ['actor', 'accountId', 'grantId', 'includePrivate', 'authorization', 'cookie']) {
    for (const method of ['search', 'get', 'contract', 'schema']) {
      const args = method === 'search' ? { query: 'safe' } : { ...ref(visible), ...(method === 'schema' ? { kind: 'input' } : {}) };
      assert.throws(() => view[method]({ ...args, [extra]: 'CREDENTIAL_SENTINEL' }), error => {
        assert.equal(error.code, 'invalid_input'); assert.equal(error.message, 'invalid_input');
        assert.equal(JSON.stringify(error).includes('CREDENTIAL_SENTINEL'), false); return true;
      });
    }
  }
  assert.equal(view.search({ query: 'safe.private' }).total, 0);
  assert.equal(view.search({ query: 'safe' }).total, 1);
});

test('construction isolates original descriptors, sidecars and every nested returned schema or example', () => {
  const entries = [declaration('snapshot.first'), declaration('snapshot.second')];
  const f = fixture(entries), before = transcript(f.view, entries), revision = f.view.search().revision;
  entries[0].title = 'MUTATED_ORIGINAL_DESCRIPTOR'; entries[0].outputSchema.properties.accepted.type = 'string';
  f.docs[0].locales.ru.examples[0].input.text = 'MUTATED_ORIGINAL_EXAMPLE';
  f.docs[0].keywords.en.push('MUTATED_ORIGINAL_KEYWORD');
  const detail = f.view.get(ref(entries[0])), result = f.view.search();
  for (const [object, key, replacement] of [[detail.capability.outputSchema.properties.accepted, 'type', 'null'],
    [detail.documentation.locales.en.examples[0].output, 'accepted', false], [detail.links, 'html', '//other.invalid'],
    [result.items[0], 'executionEnabled', true], [f.view.schema({ ...ref(entries[0]), kind: 'input' }).required, '0', 'other']]) {
    assert.equal(Reflect.set(object, key, replacement), false);
  }
  assert.equal(transcript(f.view, entries), before); assert.equal(f.view.search().revision, revision);
  assert.equal(f.view.search({ query: 'MUTATED_ORIGINAL_KEYWORD' }).total, 0);
});

test('semantic changes and documentation/readiness changes are distinguishable with the published contract bytes', () => {
  const base = declaration('revision.example'), f = fixture([base]);
  const docsChanged = fixture([base], docs => { docs[0].locales.en.summary += ' Additional guidance.'; });
  const readinessChanged = fixture([{ ...base, executionEnabled: true }]);
  for (const other of [docsChanged, readinessChanged]) {
    assert.deepEqual(other.view.contract(ref(base)), f.view.contract(ref(base)));
    assert.notEqual(other.view.search().revision, f.view.search().revision);
  }
  const altered = fixture([{ ...base, description: 'Changed semantic description.' }]);
  assert.notEqual(altered.view.contract(ref(base)).digest, f.view.contract(ref(base)).digest);
  assert.throws(() => createPublicDiscovery({ catalog: altered.catalog, documentation: f.docs }), code('documentation_contract_mismatch'));
  const raw = f.view.contract(ref(base));
  assert.equal(hash(raw.canonicalJson), raw.digest);
  assert.equal(raw.canonicalJson.endsWith('\n'), false);
  assert.deepEqual(Object.keys(JSON.parse(raw.canonicalJson)).sort(),
    ['appId', 'capabilityId', 'description', 'effects', 'executionBinding', 'inputSchema', 'outputSchema', 'recipients', 'resources', 'title', 'version', 'visibility']);
});

test('real Notes keeps its historical digest and UTF-16/byte profile while public documentation uses well-formed Unicode', () => {
  const catalog = createCatalog(), entry = catalog.get(notesRef.capabilityId, notesRef.version), view = createPublicDiscovery({ catalog });
  assert.equal(view.contract(notesRef).digest, noteDigest);
  assert.equal(hash(view.contract(notesRef).canonicalJson), noteDigest);
  const inputSchema = { type: 'object', properties: { title: { type: 'string', maxLength: 160 },
    body: { type: 'string', maxLength: 100000 } }, required: ['title', 'body'], additionalProperties: false };
  assert.deepEqual(view.schema({ ...notesRef, kind: 'input' }), inputSchema);
  assert.deepEqual(view.schema({ ...notesRef, kind: 'output' }), { type: 'object', properties: {
    noteId: { type: 'string', maxLength: 96 }, revision: { type: 'integer', minimum: 1 } },
  required: ['noteId', 'revision'], additionalProperties: false });
  for (const title of ['😀'.repeat(80), 'e\u0301'.repeat(80), '\ud800', 'A\tB\nC\rD']) {
    const input = { title, body: ' untouched \u212b / A\u030a ' }, before = JSON.stringify(input);
    catalog.validateInput(entry, input); assert.equal(JSON.stringify(input), before);
  }
  for (const title of ['😀'.repeat(81), 'e\u0301'.repeat(81), '\u0000', '\u007f'])
    assert.throws(() => catalog.validateInput(entry, { title, body: '' }), code('invalid_input'));
  const boundary = { title: '', body: '界'.repeat(87374) };
  assert.equal(bytes(boundary), 262144); catalog.validateInput(entry, boundary);
  assert.throws(() => catalog.validateInput(entry, { ...boundary, body: boundary.body + '界' }), code('payload_too_large'));
  const profile = view.get(notesRef).documentation.validation;
  assert.equal(profile.stringLength.runtime, 'utf16-code-units');
  assert.equal(profile.text.loneSurrogates, 'accepted-by-legacy-validator');
  assert.equal(profile.text.externalWriteAdmission, 'unresolved-before-write-enable');
  assert.equal(entry.executionEnabled, false);
  const custom = declaration('unicode.public'), f = fixture([custom]);
  f.docs[0].locales.en.examples[0].input.text = '\ud800';
  catalog.validateInput(entry, { title: '', body: '\ud800' });
  assert.throws(() => createPublicDiscovery({ catalog: f.catalog, documentation: f.docs }), code('documentation_invalid'));
});

test('output validation rejects a widened result or substituted entry without publishing native values in errors', () => {
  const catalog = createCatalog(), entry = catalog.get(notesRef.capabilityId, 1);
  for (const noteId of ['', '\ud800', '😀'.repeat(48)]) catalog.validateOutput(entry, { noteId, revision: Number.MAX_SAFE_INTEGER });
  for (const output of [{ noteId: 'x', revision: 0 }, { noteId: 'x', revision: 1.5 },
    { noteId: 'x', revision: Number.MAX_SAFE_INTEGER + 1 }, { noteId: '😀'.repeat(49), revision: 1 },
    { noteId: 'x', revision: 1, body: 'PRIVATE_RESULT_SENTINEL' },
    { noteId: 'x', revision: 1, url: 'https://PRIVATE_RESULT_SENTINEL.invalid' }]) {
    assert.throws(() => catalog.validateOutput(entry, output), error => {
      assert.equal(error.code, 'invalid_input'); assert.equal(error.message, 'invalid_input');
      assert.equal(JSON.stringify(error).includes('PRIVATE_RESULT_SENTINEL'), false); return true;
    });
  }
  assert.throws(() => catalog.validateOutput({ ...entry, outputSchema: { type: 'null' } }, null), code('invalid_input'));
  assert.throws(() => catalog.validateOutput(createCatalog().get(notesRef.capabilityId, 1), { noteId: 'x', revision: 1 }), code('invalid_input'));
});

test('maximal public index is complete and hostile query inputs neither echo data nor poison the snapshot', () => {
  const entries = Array.from({ length: 128 }, (_, i) => declaration(`index${String(i).padStart(3, '0')}`));
  const { view } = fixture(entries), expected = JSON.stringify(view.search());
  const all = pages(view, '', 20).flatMap(page => page.items.map(item => item.capabilityId));
  assert.deepEqual(all, entries.map(entry => entry.capabilityId));
  for (const query of ['Q'.repeat(201), '\ud800', 'BAD\u0000SECRET', 'BAD\u007fSECRET',
    Array(13).fill('public').join(' '), '\ufdfa'.repeat(4), ['public'], { query: 'public' }, null]) {
    assert.throws(() => view.search({ query }), error => {
      assert.equal(error.code, 'query_invalid'); assert.equal(error.message, 'query_invalid');
      assert.equal(JSON.stringify(error).includes('SECRET'), false); return true;
    });
  }
  assert.equal(JSON.stringify(view.search()), expected);
});
