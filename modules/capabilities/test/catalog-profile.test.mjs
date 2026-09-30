import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { BUILTIN_CAPABILITIES, createCatalog } from '../server/catalog.mjs';
import { BUILTIN_DOCUMENTATION } from '../server/documentation.mjs';
import { createCapabilitiesService } from '../server/index.mjs';
import { canonicalHash, canonicalJson } from '../server/validation.mjs';

const NOTE_DIGEST = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';
const notes = createCatalog(), entry = notes.get('notes.createDraft', 1);
const invalid = callback => assert.throws(callback, error => ['invalid_input', 'payload_too_large'].includes(error?.code)
  && error.message === error.code);

test('the exact Notes@1 semantic contract, schemas and operational flag remain unchanged', () => {
  assert.equal(entry.digest, NOTE_DIGEST); assert.equal(entry.executionEnabled, false);
  const { executionEnabled, ...contract } = BUILTIN_CAPABILITIES[0];
  assert.equal(canonicalHash(contract), NOTE_DIGEST);
  assert.deepEqual(entry.inputSchema, { type: 'object', properties: {
    title: { type: 'string', maxLength: 160 }, body: { type: 'string', maxLength: 100000 },
  }, required: ['title', 'body'], additionalProperties: false });
  assert.deepEqual(entry.outputSchema, { type: 'object', properties: {
    noteId: { type: 'string', maxLength: 96 }, revision: { type: 'integer', minimum: 1 },
  }, required: ['noteId', 'revision'], additionalProperties: false });
  const enabled = createCatalog([{ ...BUILTIN_CAPABILITIES[0], executionEnabled: true }]).get(entry.capabilityId, 1);
  assert.equal(enabled.digest, NOTE_DIGEST, 'operational readiness is not semantic version identity');
});

test('the legacy input profile retains concrete UTF-16, normalization and lone-surrogate behavior', () => {
  for (const title of ['😀'.repeat(80), 'e\u0301'.repeat(80), '\ud800', '\udc00', '\t\n\r', '']) {
    const input = { title, body: '  original\ntext  ' }, before = canonicalJson(input);
    notes.validateInput(entry, input);
    assert.equal(canonicalJson(input), before, 'validation must not normalize or trim note text');
  }
  for (const title of ['😀'.repeat(81), 'e\u0301'.repeat(81), '\0', '\u0008', '\u000b', '\u000c', '\u001f', '\u007f'])
    invalid(() => notes.validateInput(entry, { title, body: '' }));
  notes.validateInput(entry, { title: '', body: '界'.repeat(80000) });
  invalid(() => notes.validateInput(entry, { title: '', body: '界'.repeat(100000) }));
  invalid(() => notes.validateInput(entry, { title: '', body: 'a'.repeat(100001) }));
});

test('output is checked against the actual immutable registry entry and exact Notes result shape', () => {
  for (const output of [{ noteId: 'note-example', revision: 1 }, { noteId: '', revision: Number.MAX_SAFE_INTEGER },
    { noteId: '😀'.repeat(48), revision: 2 }, { noteId: '\ud800', revision: 1 }]) notes.validateOutput(entry, output);
  for (const output of [undefined, null, [], '', { noteId: 'n' }, { revision: 1 }, { noteId: 'n', revision: 0 },
    { noteId: 'n', revision: 1.5 }, { noteId: 'n', revision: '1' }, { noteId: 'n', revision: Number.MAX_SAFE_INTEGER + 1 },
    { noteId: 'n', revision: Infinity }, { noteId: false, revision: 1 }, { noteId: '😀'.repeat(49), revision: 1 },
    { noteId: '\0', revision: 1 }, { noteId: '\u007f', revision: 1 }]) invalid(() => notes.validateOutput(entry, output));
  for (const extra of ['body', 'title', 'accountId', 'openUrl']) invalid(() => notes.validateOutput(entry, { noteId: 'n', revision: 1, [extra]: 'PRIVATE_RESULT_MARKER' }));
  invalid(() => notes.validateOutput({ ...entry, outputSchema: { type: 'object', properties: {}, additionalProperties: false } }, {}));
  invalid(() => notes.validateOutput(createCatalog().get(entry.capabilityId, 1), { noteId: 'n', revision: 1 }));
});

test('output uses the same bounded recursive type, enum, array and byte profile, not schema coercion', () => {
  const registry = createCatalog([{ ...BUILTIN_CAPABILITIES[0], capabilityId: 'profile.recursive', outputSchema: {
    type: 'object', properties: {
      values: { type: 'array', minItems: 1, maxItems: 2, items: { type: 'integer', minimum: -2, maximum: 2, enum: [-2, 0, 2] } },
      mode: { type: 'string', minLength: 2, maxLength: 2 },
      enabled: { type: 'boolean' }, empty: { type: 'null' }, ratio: { type: 'number', minimum: 0, maximum: 1 },
    }, required: ['values', 'mode', 'enabled', 'empty', 'ratio'], additionalProperties: false,
  } }]);
  const cap = registry.get('profile.recursive', 1), good = { values: [-2, 2], mode: '😀', enabled: true, empty: null, ratio: 0.5 };
  registry.validateOutput(cap, good);
  for (const output of [{ ...good, values: [] }, { ...good, values: [1] }, { ...good, values: [0, 0, 0] },
    { ...good, values: ['2'] }, { ...good, mode: 'x' }, { ...good, mode: 'xxx' }, { ...good, enabled: 1 },
    { ...good, empty: {} }, { ...good, ratio: NaN }, { ...good, ratio: 2 }, { ...good, extra: true }]) invalid(() => registry.validateOutput(cap, output));
  const big = createCatalog([{ ...BUILTIN_CAPABILITIES[0], capabilityId: 'profile.bytes',
    outputSchema: { type: 'array', maxItems: 128, items: { type: 'string', maxLength: 100000 } } }]);
  const largeCap = big.get('profile.bytes', 1);
  big.validateOutput(largeCap, ['界'.repeat(80000)]);
  invalid(() => big.validateOutput(largeCap, ['界'.repeat(100000)]));
  invalid(() => big.validateOutput(largeCap, Object.assign([], { privateOutput: 'PRIVATE_RESULT_MARKER' })));
});

test('real SQLite contract pins refuse semantic changes across reopen and permit operational changes only', t => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-profile-pin-')), databasePath = join(directory, 'capabilities.sqlite');
  let service;
  t.after(() => {
    service?.close(); assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-profile-pin-/u);
    rmSync(directory, { recursive: true, force: true });
  });
  const open = catalog => {
    // Keep this a durable pin test after discovery is wired before SQLite. A
    // candidate's explicit test documentation matches its own semantic digest.
    const documentation = createCatalog(catalog).listPublic().map(candidate => ({
      ...JSON.parse(JSON.stringify(BUILTIN_DOCUMENTATION[0])), capabilityId: candidate.capabilityId,
      version: candidate.version, contractDigest: candidate.digest,
      locales: Object.fromEntries(['ru', 'en'].map(language => [language,
        { ...BUILTIN_DOCUMENTATION[0].locales[language], examples: [] }])),
    }));
    return createCapabilitiesService({ databasePath, actorActive: () => false, catalog, documentation });
  };
  service = open(BUILTIN_CAPABILITIES);
  assert.equal(service.catalog.get({ capabilityId: entry.capabilityId, version: 1 }).capability.digest, NOTE_DIGEST);
  service.close(); service = null;
  for (const changed of [{ description: 'Different semantic description' }, { effects: ['create', 'delete'] },
    { outputSchema: { type: 'string', maxLength: 10 } }])
    assert.throws(() => open([{ ...BUILTIN_CAPABILITIES[0], ...changed }]), { code: 'capability_version_conflict' });
  service = open([{ ...BUILTIN_CAPABILITIES[0], executionEnabled: true }]);
  assert.equal(service.catalog.get({ capabilityId: entry.capabilityId, version: 1 }).capability.digest, NOTE_DIGEST);
  service.close(); service = open(BUILTIN_CAPABILITIES);
  assert.equal(service.catalog.get({ capabilityId: entry.capabilityId, version: 1 }).capability.executionEnabled, false);
});
