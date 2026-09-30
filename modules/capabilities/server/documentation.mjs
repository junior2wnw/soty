import { AccessError, assert, canonicalHash, canonicalJson, exact, freezeDeep, text } from './validation.mjs';

export const DOCUMENTATION_BYTES = 16 * 1024;
export const CAPABILITY_VALIDATION_PROFILE = freezeDeep({
  runtimeProfile: 'soty-capability-v1',
  schemaDialect: 'https://json-schema.org/draft/2020-12/schema',
  schemaSupport: 'bounded-subset',
  stringLength: { schema: 'unicode-characters', runtime: 'utf16-code-units' },
  canonicalJson: { maxBytes: 262144, maxDepth: 20, maxNodes: 10000, algorithm: 'sorted-own-keys-ecmascript-json' },
  integers: 'finite-safe-integer', numbers: 'finite',
  text: { normalization: 'none', forbiddenCodePoints: ['U+0000..U+0008', 'U+000B..U+000C', 'U+000E..U+001F', 'U+007F'],
    loneSurrogates: 'accepted-by-legacy-validator', externalWriteAdmission: 'unresolved-before-write-enable' },
});

export const BUILTIN_DOCUMENTATION = freezeDeep([{
  capabilityId: 'notes.createDraft', version: 1,
  contractDigest: '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204',
  locales: {
    ru: {
      title: 'Создать записку', summary: 'Новая личная записка, которую владелец сможет продолжить в Сотах.',
      useWhen: ['Сохранить новую идею, список или черновик для себя.', 'Передать результат работы в новую личную записку.'],
      notFor: ['Чтение, поиск, исправление или удаление существующих записок.', 'Публикация записки, отправка другим людям или передача данных внешнему получателю.',
        'Описание не выдаёт право вызова. Допуск и проверка результата появятся отдельным этапом.'],
      examples: [{ input: { title: 'Идеи на завтра', body: 'Набросать план проекта.\nПроверить прототип.' }, output: { noteId: 'illustrative-note-id', revision: 1 } }],
    },
    en: {
      title: 'Create a note', summary: 'Create a new private note for its owner to continue in Soty.',
      useWhen: ['Save a new idea, list or personal draft.', 'Keep a work result in a new private note.'],
      notFor: ['Reading, searching, editing or deleting existing notes.', 'Publishing a note, sending it to another person or transferring data to an external recipient.',
        'This description does not grant execution access. Authorization and verified results belong to a later stage.'],
      examples: [{ input: { title: 'Ideas for tomorrow', body: 'Outline the project.\nCheck the prototype.' }, output: { noteId: 'illustrative-note-id', revision: 1 } }],
    },
  },
  keywords: { ru: ['записка', 'записки', 'заметка', 'заметку', 'черновик', 'сохранить', 'идея', 'список'],
    en: ['note', 'notes', 'draft', 'save', 'create', 'idea', 'list', 'private', 'personal'] },
}]);

/** Public metadata is stricter Unicode than the unchanged legacy value validator. */
export function assertPublicStrings(value, code = 'discovery_invalid') {
  function visit(item, depth) {
    assert(depth <= 20, code);
    if (typeof item === 'string') {
      assert(item.isWellFormed(), code);
      text(item, { min: 0, max: 262144, code });
    } else if (Array.isArray(item)) item.forEach(child => visit(child, depth + 1));
    else if (item && typeof item === 'object') for (const [key, child] of Object.entries(item)) {
      visit(key, depth + 1); visit(child, depth + 1);
    }
  }
  visit(value, 0);
}

function strings(value) {
  assert(Array.isArray(value), 'documentation_invalid');
  for (const item of value) text(item, { max: DOCUMENTATION_BYTES, code: 'documentation_invalid' });
}

/** Called only after discovery has selected an exact public capability. */
export function preparePublicDocumentation({ catalog, capability, source }) {
  assert(source !== undefined, 'documentation_missing');
  let sidecar;
  try { sidecar = JSON.parse(canonicalJson(source, { maxBytes: DOCUMENTATION_BYTES })); }
  catch { throw new AccessError('documentation_invalid'); }
  exact(sidecar, ['capabilityId', 'version', 'contractDigest', 'locales', 'keywords'], 'documentation_invalid');
  assert(capability.visibility === 'public' && sidecar.capabilityId === capability.capabilityId && sidecar.version === capability.version,
    'documentation_invalid');
  assert(sidecar.contractDigest === capability.digest, 'documentation_contract_mismatch');
  assertPublicStrings(sidecar, 'documentation_invalid');
  exact(sidecar.locales, ['ru', 'en'], 'documentation_invalid');
  exact(sidecar.keywords, ['ru', 'en'], 'documentation_invalid');
  for (const language of ['ru', 'en']) {
    const locale = sidecar.locales[language];
    exact(locale, ['title', 'summary', 'useWhen', 'notFor', 'examples'], 'documentation_invalid');
    text(locale.title, { max: DOCUMENTATION_BYTES, code: 'documentation_invalid' });
    text(locale.summary, { max: DOCUMENTATION_BYTES, code: 'documentation_invalid' });
    strings(locale.useWhen); strings(locale.notFor); strings(sidecar.keywords[language]);
    assert(Array.isArray(locale.examples) && locale.examples.length <= 128, 'documentation_invalid');
    for (const example of locale.examples) {
      exact(example, ['input', 'output'], 'documentation_invalid');
      try { catalog.validateInput(capability, example.input); catalog.validateOutput(capability, example.output); }
      catch { throw new AccessError('documentation_invalid'); }
    }
  }
  const validation = CAPABILITY_VALIDATION_PROFILE;
  const revision = canonicalHash({ ...sidecar, validation });
  // Include the generated profile/revision and index-only keywords in the same
  // per-version budget. Documentation never replaces the pinned schema.
  try { canonicalJson({ ...sidecar, validation, revision }, { maxBytes: DOCUMENTATION_BYTES }); }
  catch { throw new AccessError('projection_too_large'); }
  return freezeDeep({
    documentation: { revision, contractDigest: capability.digest, locales: sidecar.locales, validation },
    // Negative examples are explanatory data, not positive search keywords.
    searchText: ['ru', 'en'].flatMap(language => { const locale = sidecar.locales[language];
      return [locale.title, locale.summary, ...locale.useWhen, ...sidecar.keywords[language]]; }).join('\n'),
  });
}
