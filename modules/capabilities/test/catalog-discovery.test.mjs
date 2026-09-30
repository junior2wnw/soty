import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { BUILTIN_CAPABILITIES, createCatalog } from '../server/catalog.mjs';
import { createPublicDiscovery, DISCOVERY_LIMITS } from '../server/discovery.mjs';
import { BUILTIN_DOCUMENTATION, CAPABILITY_VALIDATION_PROFILE } from '../server/documentation.mjs';
import { canonicalJson } from '../server/validation.mjs';

const notes = BUILTIN_CAPABILITIES[0], NOTE_DIGEST = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';
const hash = text => createHash('sha256').update(text).digest('hex');
const bytes = value => Buffer.byteLength(canonicalJson(value), 'utf8');
const errorCode = code => error => error?.code === code && error.message === code;
const clone = value => JSON.parse(JSON.stringify(value));
function sidecars(catalog) {
  return catalog.listPublic().map(entry => ({ ...clone(BUILTIN_DOCUMENTATION[0]), capabilityId: entry.capabilityId,
    version: entry.version, contractDigest: entry.digest }));
}
function setup(entries = BUILTIN_CAPABILITIES, mutateDocs = () => {}) {
  const catalog = createCatalog(entries), documentation = sidecars(catalog);
  mutateDocs(documentation);
  return { catalog, documentation, view: createPublicDiscovery({ catalog, documentation }) };
}
const args = { capabilityId: notes.capabilityId, version: 1 };

test('real Notes discovery is compact, bilingual, explicitly public and disabled without invented search matches', () => {
  const { view } = setup();
  for (const query of ['создать записку', 'сохранить черновик', 'create note', 'save personal draft', 'ＮＯＴＥＳ.createDraft']) {
    const result = view.search({ query }); assert.equal(result.scope, 'public'); assert.equal(result.total, 1);
    assert.match(result.revision, /^[a-f0-9]{64}$/u); assert.equal(result.cursor, null);
    const item = result.items[0]; assert.equal(item.digest, NOTE_DIGEST); assert.equal(item.executionEnabled, false);
    assert.equal(item.capabilityId, notes.capabilityId); assert.ok(bytes(item) <= DISCOVERY_LIMITS.itemBytes);
    assert.deepEqual(Object.keys(item).sort(), ['appId', 'capabilityId', 'digest', 'executionEnabled', 'links', 'match', 'summary', 'title', 'version']);
    for (const field of ['inputSchema', 'outputSchema', 'resources', 'effects', 'executionBinding']) assert.equal(Object.hasOwn(item, field), false);
  }
  for (const query of ['weather forecast', 'delete existing notes', 'отправить письмо']) assert.equal(view.search({ query }).total, 0);
  assert.equal(view.search().items[0].match, 'browse');
  assert.equal(view.search({ query: 'NOTES.CREATEDRAFT' }).items[0].match, 'exact-id');
  assert.equal(view.get(args).documentation.validation.runtimeProfile, 'soty-capability-v1');
  for (const language of ['ru', 'en']) assert.ok(view.get(args).documentation.locales[language].examples.length > 0,
    'the shipped Notes documentation includes a real validated example in both languages');
});

test('private and unknown sidecars, private content and order cannot influence public bytes, count, revision or cursor', () => {
  const publicEntries = [notes, { ...notes, capabilityId: 'public.second', title: 'Second public item' }];
  const baseline = setup(publicEntries), expected = baseline.view.search({ limit: 1 });
  for (const privateTitle of ['PRIVATE_MARKER', '\ud800', 'Unrelated updated private title']) {
    const secret = { ...notes, capabilityId: 'private.secret', visibility: 'private', title: privateTitle, executionEnabled: true };
    const catalog = createCatalog([secret, ...publicEntries].reverse());
    const documentation = [...sidecars(catalog), { capabilityId: 'private.secret', version: 1,
      contractDigest: 'incorrect', locales: { ru: { title: '\ud800'.repeat(20000) } } },
    ...Array.from({ length: 130 }, (_, i) => ({ capabilityId: `unknown.private${i}`, version: 1,
      contractDigest: null, arbitrary: { data: 'private bad schema' } }))];
    const view = createPublicDiscovery({ catalog, documentation });
    assert.equal(canonicalJson(view.search({ limit: 1 })), canonicalJson(expected));
    assert.equal(canonicalJson(view.get(args)), canonicalJson(baseline.view.get(args)));
    assert.equal(canonicalJson(view.search({ limit: 1, cursor: expected.cursor })), canonicalJson(baseline.view.search({ limit: 1, cursor: expected.cursor })));
    assert.equal(view.search({ query: 'PRIVATE_MARKER' }).total, 0);
    for (const capabilityId of ['private.secret', 'unknown.private']) for (const method of ['get', 'contract', 'schema'])
      assert.throws(() => view[method]({ capabilityId, version: 1, ...(method === 'schema' ? { kind: 'input' } : {}) }), errorCode('not_found'));
  }
  const empty = createPublicDiscovery({ catalog: createCatalog([]), documentation: [{ capabilityId: 'unknown', version: 1, invalid: '\ud800' }] });
  assert.deepEqual(empty.search(), createPublicDiscovery({ catalog: createCatalog([]), documentation: [] }).search());
  assert.equal(empty.search().total, 0);
});

test('get is case-sensitive, schema objects are exact and raw canonical contract bytes match the pinned digest', () => {
  const { view, catalog } = setup(), actual = view.contract(args), entry = catalog.get(args.capabilityId, args.version);
  assert.equal(actual.digest, NOTE_DIGEST); assert.equal(hash(Buffer.from(actual.canonicalJson, 'utf8')), NOTE_DIGEST);
  assert.equal(actual.canonicalJson, canonicalJson(JSON.parse(actual.canonicalJson)));
  const parsed = JSON.parse(actual.canonicalJson);
  for (const field of ['executionEnabled', 'digest', 'charges', 'documentation']) assert.equal(Object.hasOwn(parsed, field), false);
  assert.deepEqual(view.schema({ ...args, kind: 'input' }), entry.inputSchema);
  assert.deepEqual(view.schema({ ...args, kind: 'output' }), entry.outputSchema);
  assert.equal(Object.hasOwn(view.schema({ ...args, kind: 'input' }), '$id'), false);
  assert.throws(() => view.get({ capabilityId: 'Notes.createDraft', version: 1 }), errorCode('not_found'));
  assert.throws(() => view.schema({ ...args, kind: 'other' }), errorCode('invalid_input'));
  assert.throws(() => view.get({ ...args, version: '1' }), errorCode('invalid_input'));
});

test('public schemas/descriptions and relevant sidecars fail closed for ill-formed Unicode, mismatched digest or invalid examples', () => {
  for (const changed of [{ title: '\ud800' }, { description: '\udc00' },
    { outputSchema: { ...notes.outputSchema, description: '\ud800' } }])
    assert.throws(() => setup([{ ...notes, ...changed }]), errorCode('discovery_invalid'));
  const cases = [
    [docs => { docs[0].contractDigest = '0'.repeat(64); }, 'documentation_contract_mismatch'],
    [docs => { docs[0].locales.en.summary = '\ud800'; }, 'documentation_invalid'],
    [docs => { docs[0].locales.ru.examples[0].input.body = '\ud800'; }, 'documentation_invalid'],
    [docs => { docs[0].locales.en.examples[0].input.accountId = 'other-account'; }, 'documentation_invalid'],
    [docs => { docs[0].locales.en.examples[0].output.revision = 0; }, 'documentation_invalid'],
    [docs => { docs[0].locales.ru.examples[0].output.body = 'private'; }, 'documentation_invalid'],
    [docs => { docs[0].schemaUrl = 'https://untrusted.invalid/schema'; }, 'documentation_invalid'],
    [docs => { docs[0].validation = { stringLength: 'bytes' }; }, 'documentation_invalid'],
  ];
  for (const [change, code] of cases) assert.throws(() => setup(BUILTIN_CAPABILITIES, change), errorCode(code));
  const catalog = createCatalog();
  assert.throws(() => createPublicDiscovery({ catalog, documentation: [] }), errorCode('documentation_missing'));
  assert.throws(() => createPublicDiscovery({ catalog, documentation: [BUILTIN_DOCUMENTATION[0], BUILTIN_DOCUMENTATION[0]] }), errorCode('documentation_invalid'));
});

test('sidecar strings and examples remain plain data, and metadata snapshots cannot be changed under an old revision', () => {
  const f = setup(BUILTIN_CAPABILITIES, docs => {
    docs[0].locales.ru.summary = '</script><img src=x onerror=steal()> "quoted" \u202e text';
    docs[0].locales.en.examples[0].input.body = 'https://untrusted.invalid/ is just note text';
  });
  const before = canonicalJson(f.view.get(args)), first = canonicalJson(f.view.search());
  f.documentation[0].locales.ru.summary = 'changed after construction';
  f.documentation[0].keywords.en.push('private-new-index-word');
  const detail = f.view.get(args);
  assert.throws(() => { detail.capability.title = 'mutable'; }, TypeError);
  assert.throws(() => { detail.documentation.locales.ru.summary = 'mutable'; }, TypeError);
  assert.throws(() => { detail.links.html = '//evil.invalid'; }, TypeError);
  assert.throws(() => { f.view.search().items[0].links.html = '/changed'; }, TypeError);
  assert.equal(canonicalJson(f.view.get(args)), before); assert.equal(canonicalJson(f.view.search()), first);
  assert.equal(f.view.search({ query: 'private-new-index-word' }).total, 0);
  assert.match(detail.documentation.locales.ru.summary, /<\/script>/u, 'escaping belongs to the HTML renderer, not mutation of original metadata');
});

test('a public version may document no examples rather than inventing them, with a finite example count', () => {
  const { view } = setup(BUILTIN_CAPABILITIES, docs => {
    for (const locale of Object.values(docs[0].locales)) locale.examples = [];
  });
  assert.deepEqual(view.get(args).documentation.locales.en.examples, []);
  assert.equal(view.search().total, 1);
  const descriptor = { ...notes, inputSchema: { type: 'null' }, outputSchema: { type: 'null' } };
  assert.throws(() => setup([descriptor], docs => {
    for (const locale of Object.values(docs[0].locales)) locale.examples = Array.from({ length: 129 }, () => ({ input: null, output: null }));
  }), errorCode('documentation_invalid'));
});

test('query limits precede normalization and token count follows NFKC, with strict argument types and no actor fallback', () => {
  const { view } = setup();
  view.search({ query: '界'.repeat(200) }); view.search({ query: '😀'.repeat(100) });
  view.search({ query: Array(12).fill('note').join(' ') });
  for (const query of ['x'.repeat(201), '😀'.repeat(101), '\ud800', '\u0000', '\u007f', Array(13).fill('note').join(' '), '\ufdfa'.repeat(4), 4, null, ['note']])
    assert.throws(() => view.search({ query }), errorCode('query_invalid'));
  for (const limit of [0, 21, 1.5, '2', null]) assert.throws(() => view.search({ limit }), errorCode('invalid_input'));
  for (const extra of ['actor', 'accountId', 'grantId', 'includePrivate']) {
    assert.throws(() => view.search({ [extra]: 'spoof' }), errorCode('invalid_input'));
    assert.throws(() => view.get({ ...args, [extra]: 'spoof' }), errorCode('invalid_input'));
  }
});

test('ranking is exact ID then prefix then lexical text, with ordinal ID and numeric version ties', () => {
  const base = { ...notes, capabilityId: 'exact', title: 'Match exact' };
  const entries = [{ ...base, capabilityId: 'text.first' }, { ...base, capabilityId: 'exact.suffix' }, { ...base, version: 10 }, { ...base, version: 2 }];
  const { view } = setup(entries);
  const result = view.search({ query: 'exact' });
  assert.deepEqual(result.items.map(item => [item.capabilityId, item.version, item.match]),
    [['exact', 2, 'exact-id'], ['exact', 10, 'exact-id'], ['exact.suffix', 1, 'id-prefix'], ['text.first', 1, 'text']]);
});

test('cursor is strict canonical base64url/UTF-8/context, while a valid public position never grants private access', () => {
  const entries = Array.from({ length: 4 }, (_, i) => ({ ...notes, capabilityId: `public.item${i}` }));
  const { view } = setup(entries), first = view.search({ limit: 1 });
  assert.ok(first.cursor.length <= 512); const decoded = JSON.parse(Buffer.from(first.cursor, 'base64url').toString('utf8'));
  const forge = patch => Buffer.from(canonicalJson({ ...decoded, ...patch })).toString('base64url');
  const invalid = ['=', `${first.cursor}=`, 'a'.repeat(513), 'ñ', {}, 2, Buffer.from([0xff]).toString('base64url'),
    Buffer.from('{"offset":1,"offset":2}').toString('base64url'), forge({ offset: -1 }), forge({ offset: 1.5 }), forge({ offset: 5 }),
    forge({ queryHash: '0'.repeat(64) }), forge({ revision: '0'.repeat(64) }), forge({ actor: 'privileged' })];
  for (const cursor of invalid) assert.throws(() => view.search({ cursor }), errorCode('cursor_invalid'));
  assert.throws(() => view.search({ query: 'note', cursor: first.cursor }), errorCode('cursor_invalid'));
  assert.equal(view.search({ cursor: forge({ offset: 3 }) }).items[0].capabilityId, 'public.item3');
  assert.deepEqual(view.search({ cursor: forge({ offset: 4 }) }).items, []);
  const changedDocs = setup(entries, docs => { docs[0].keywords.en.push('new-keyword'); });
  assert.throws(() => changedDocs.view.search({ cursor: first.cursor }), errorCode('cursor_invalid'));
  const changedReadiness = setup(entries.map((entry, i) => ({ ...entry, executionEnabled: i === 0 })));
  assert.throws(() => changedReadiness.view.search({ cursor: first.cursor }), errorCode('cursor_invalid'));
  assert.equal(changedReadiness.view.contract({ capabilityId: entries[0].capabilityId, version: 1 }).digest,
    view.contract({ capabilityId: entries[0].capabilityId, version: 1 }).digest);
});

test('byte-limited pages continue after actual returned items, not requested limit, and contain no duplicates', () => {
  const entries = Array.from({ length: 40 }, (_, i) => ({ ...notes, capabilityId: `public${String(i).padStart(2, '0')}${'/'.repeat(142)}` }));
  const { view } = setup(entries, docs => { for (const doc of docs) doc.locales.ru.summary = 'я'.repeat(430); });
  let cursor, count = 0, pages = 0; const ids = new Set();
  do {
    const page = view.search({ limit: 20, ...(cursor ? { cursor } : {}) });
    assert.equal(page.total, 40); assert.ok(bytes(page) <= DISCOVERY_LIMITS.pageBytes);
    if (pages === 0) assert.ok(page.items.length < 20, 'fixture must exercise byte cropping rather than ordinary item-count pagination');
    for (const item of page.items) { assert.ok(bytes(item) <= DISCOVERY_LIMITS.itemBytes); assert.equal(ids.has(item.capabilityId), false); ids.add(item.capabilityId); count++; }
    cursor = page.cursor; pages++; assert.ok(pages < 10);
  } while (cursor);
  assert.equal(count, 40); assert.equal(ids.size, 40);
});

test('128-version public index, schema-sized detail, sidecar and compact item byte bounds are enforced', () => {
  const entries = Array.from({ length: 128 }, (_, i) => ({ ...notes, capabilityId: `public.v${String(i).padStart(3, '0')}` }));
  const { view } = setup(entries); assert.equal(view.search().total, 128); assert.equal(view.search().items.length, 10);
  assert.throws(() => createCatalog([...entries, { ...notes, capabilityId: 'public.overflow' }]), errorCode('catalog_invalid'));
  assert.throws(() => setup(BUILTIN_CAPABILITIES, docs => { docs[0].locales.ru.summary = 'Ж'.repeat(1800); }), errorCode('projection_too_large'));
  assert.throws(() => setup(BUILTIN_CAPABILITIES, docs => { docs[0].locales.en.notFor = ['x'.repeat(16384)]; }), errorCode('documentation_invalid'));
  const descriptor = { ...notes, outputSchema: { type: 'string', maxLength: 100000, enum: Array.from({ length: 60 }, (_, i) => `${i}:${'z'.repeat(4000)}`) } };
  const catalog = createCatalog([descriptor]), documentation = sidecars(catalog);
  for (const locale of Object.values(documentation[0].locales)) locale.examples[0].output = descriptor.outputSchema.enum[0];
  const big = createPublicDiscovery({ catalog, documentation });
  assert.ok(bytes(big.get(args)) > 240000); assert.ok(bytes(big.get(args)) <= DISCOVERY_LIMITS.detailBytes);
  assert.deepEqual(big.schema({ ...args, kind: 'output' }), catalog.get(args.capabilityId, args.version).outputSchema);
});

test('encoded ID links stay relative and versioned, and documentation profile cannot masquerade as the primary schema', () => {
  const entry = { ...notes, capabilityId: 'vendor/app@create:note' }, { view } = setup([entry]);
  const detail = view.get({ capabilityId: entry.capabilityId, version: 1 });
  assert.equal(detail.links.html, '/agents/capabilities/vendor%2Fapp%40create%3Anote/versions/1');
  for (const link of Object.values(detail.links)) { assert.ok(link.startsWith('/')); assert.equal(link.startsWith('//'), false); }
  assert.deepEqual(detail.documentation.validation, CAPABILITY_VALIDATION_PROFILE);
  assert.equal(detail.documentation.validation.stringLength.schema, 'unicode-characters');
  assert.equal(detail.documentation.validation.stringLength.runtime, 'utf16-code-units');
  assert.equal(detail.documentation.contractDigest, detail.capability.digest);
  assert.notEqual(detail.documentation.revision, detail.capability.digest);
});
