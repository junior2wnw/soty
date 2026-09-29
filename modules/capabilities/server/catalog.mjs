import { assert, canonicalHash, canonicalJson, exact, freezeDeep, identifier, integer, record, stringSet, text } from './validation.mjs';

// P1 accepts an intentionally bounded JSON Schema subset, not arbitrary author schemas.
const KEYS = new Set(['$schema', 'type', 'title', 'description', 'properties', 'required', 'additionalProperties', 'items', 'minItems', 'maxItems', 'minLength', 'maxLength', 'minimum', 'maximum', 'enum']);
function validateSchema(schema, depth = 0) {
  record(schema);
  assert(depth <= 8 && Object.keys(schema).every(key => KEYS.has(key)), 'schema_unsupported');
  assert(['object', 'array', 'string', 'integer', 'number', 'boolean', 'null'].includes(schema.type), 'schema_unsupported');
  if (schema.$schema !== undefined) assert(schema.$schema === 'https://json-schema.org/draft/2020-12/schema', 'schema_unsupported');
  if (schema.title !== undefined) text(schema.title, { max: 160 });
  if (schema.description !== undefined) text(schema.description, { min: 0, max: 2000 });
  if (schema.enum !== undefined) {
    assert(Array.isArray(schema.enum) && schema.enum.length > 0 && schema.enum.length <= 64, 'schema_unsupported');
    schema.enum.forEach(value => canonicalJson(value));
  }
  if (schema.type === 'object') {
    record(schema.properties);
    assert(Object.keys(schema.properties).length <= 32 && schema.additionalProperties === false, 'schema_unsupported');
    for (const [key, child] of Object.entries(schema.properties)) {
      identifier(key);
      validateSchema(child, depth + 1);
    }
    if (schema.required !== undefined) {
      const required = stringSet(schema.required);
      assert(required.every(key => Object.hasOwn(schema.properties, key)), 'schema_unsupported');
    }
  } else {
    assert(schema.properties === undefined && schema.required === undefined && schema.additionalProperties === undefined, 'schema_unsupported');
  }
  if (schema.type === 'array') {
    validateSchema(schema.items, depth + 1);
    integer(schema.maxItems, 0, 128, 'schema_unsupported');
    if (schema.minItems !== undefined) integer(schema.minItems, 0, schema.maxItems, 'schema_unsupported');
  } else assert(schema.items === undefined && schema.minItems === undefined && schema.maxItems === undefined, 'schema_unsupported');
  if (schema.type === 'string') {
    integer(schema.maxLength, 0, 100000, 'schema_unsupported');
    if (schema.minLength !== undefined) integer(schema.minLength, 0, schema.maxLength, 'schema_unsupported');
  } else assert(schema.minLength === undefined && schema.maxLength === undefined, 'schema_unsupported');
  if (schema.minimum !== undefined || schema.maximum !== undefined) {
    assert(['number', 'integer'].includes(schema.type), 'schema_unsupported');
    assert((schema.minimum === undefined || Number.isFinite(schema.minimum)) && (schema.maximum === undefined || Number.isFinite(schema.maximum)), 'schema_unsupported');
    assert(schema.minimum === undefined || schema.maximum === undefined || schema.minimum <= schema.maximum, 'schema_unsupported');
  }
}

function validateValue(schema, value) {
  if (schema.enum) assert(schema.enum.some(item => canonicalHash(item) === canonicalHash(value)));
  switch (schema.type) {
    case 'object':
      record(value);
      assert(Object.keys(value).every(key => Object.hasOwn(schema.properties, key)));
      assert((schema.required || []).every(key => Object.hasOwn(value, key)));
      for (const [key, child] of Object.entries(value)) validateValue(schema.properties[key], child);
      break;
    case 'array':
      assert(Array.isArray(value) && value.length >= (schema.minItems || 0) && value.length <= schema.maxItems);
      value.forEach(item => validateValue(schema.items, item));
      break;
    case 'string': text(value, { min: schema.minLength || 0, max: schema.maxLength }); break;
    case 'integer': assert(Number.isSafeInteger(value)); break;
    case 'number': assert(typeof value === 'number' && Number.isFinite(value)); break;
    case 'boolean': assert(typeof value === 'boolean'); break;
    case 'null': assert(value === null); break;
    default: assert(false, 'schema_unsupported');
  }
  if (schema.minimum !== undefined) assert(value >= schema.minimum);
  if (schema.maximum !== undefined) assert(value <= schema.maximum);
}

export const BUILTIN_CAPABILITIES = freezeDeep([{
  capabilityId: 'notes.createDraft', version: 1, appId: 'notes', title: 'Создать записку',
  description: 'Создать новую личную записку. Не читает и не изменяет существующие записки.',
  visibility: 'public', executionEnabled: false,
  inputSchema: { type: 'object', properties: { title: { type: 'string', maxLength: 160 }, body: { type: 'string', maxLength: 100000 } }, required: ['title', 'body'], additionalProperties: false },
  outputSchema: { type: 'object', properties: { noteId: { type: 'string', maxLength: 96 }, revision: { type: 'integer', minimum: 1 } }, required: ['noteId', 'revision'], additionalProperties: false },
  resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'],
  executionBinding: { kind: 'native', handler: 'notes.createDraft', version: 1 }
}]);

export function createCatalog(entries = BUILTIN_CAPABILITIES) {
  assert(Array.isArray(entries) && entries.length <= 128, 'catalog_invalid');
  const byKey = new Map();
  for (const original of entries) {
    const entry = JSON.parse(canonicalJson(original));
    exact(entry, ['capabilityId', 'version', 'appId', 'title', 'description', 'visibility', 'executionEnabled', 'inputSchema', 'outputSchema', 'resources', 'effects', 'recipients', 'executionBinding']);
    identifier(entry.capabilityId); identifier(entry.appId); integer(entry.version, 1, 1000000);
    text(entry.title, { max: 160 }); text(entry.description, { max: 2000 });
    assert(['public', 'private'].includes(entry.visibility) && typeof entry.executionEnabled === 'boolean', 'catalog_invalid');
    validateSchema(entry.inputSchema); validateSchema(entry.outputSchema);
    entry.resources = stringSet(entry.resources); entry.effects = stringSet(entry.effects); entry.recipients = stringSet(entry.recipients);
    exact(entry.executionBinding, ['kind', 'handler', 'version']);
    assert(entry.executionBinding.kind === 'native', 'executor_unsupported');
    identifier(entry.executionBinding.handler); integer(entry.executionBinding.version, 1, 1, 'executor_unsupported');
    const key = `${entry.capabilityId}@${entry.version}`;
    assert(!byKey.has(key), 'catalog_duplicate');
    const { executionEnabled: _operationalFlag, ...contract } = entry;
    byKey.set(key, freezeDeep({ ...entry, digest: canonicalHash(contract), charges: [{ unit: 'invocations', amount: 1 }] }));
  }
  return Object.freeze({
    get(capabilityId, version) {
      identifier(capabilityId); integer(version, 1, 1000000);
      return byKey.get(`${capabilityId}@${version}`) || null;
    },
    listPublic() { return [...byKey.values()].filter(entry => entry.visibility === 'public'); },
    listAll() { return [...byKey.values()]; },
    validateInput(entry, input) {
      canonicalJson(input);
      validateValue(entry.inputSchema, input);
    }
  });
}
