import test from 'node:test';
import assert from 'node:assert/strict';
import { FIELD_SCHEMA, FIELD_LIMITS, FieldContractError, createFieldDocument, validateFieldDocument, validateFieldEntity, fieldEntityKey } from './contract.mjs';

const make = () => ({ schema: FIELD_SCHEMA, contexts: [{ contextId: 'studio', title: 'Студия', x: 420, y: 360 }],
  shortcuts: [{ shortcutId: 'a', entity: { kind: 'app', id: 'hive' }, contextId: 'studio', slot: [0, 0] }] });
const bad = value => assert.throws(() => validateFieldDocument(value), error => error instanceof FieldContractError && error.code === 'field_document_invalid');

test('empty and ordinary documents detach every nested record, normalize title and negative zero', () => {
  assert.deepEqual(validateFieldDocument(createFieldDocument()), { schema: FIELD_SCHEMA, contexts: [], shortcuts: [] });
  const source = make(); source.contexts[0].title = ' Студия '; source.shortcuts[0].slot[0] = -0;
  const result = validateFieldDocument(source); source.shortcuts[0].slot[1] = 99; source.shortcuts[0].entity.id = 'other';
  assert.equal(result.contexts[0].title, 'Студия'); assert.deepEqual(result.shortcuts[0].slot, [0, 0]); assert.equal(result.shortcuts[0].entity.id, 'hive');
});

test('identity kind and shortcut identity are independent; a source has two personal shortcuts', () => {
  const value = make(); value.contexts.push({ contextId: 'home', title: 'Дома', x: 900, y: 300 });
  value.shortcuts.push({ ...structuredClone(value.shortcuts[0]), shortcutId: 'b', contextId: 'home' });
  assert.equal(validateFieldDocument(value).shortcuts.length, 2);
  assert.notEqual(fieldEntityKey({ kind: 'app', id: 'same' }), fieldEntityKey({ kind: 'device', id: 'same' }));
  for (const kind of ['app', 'person', 'community', 'device', 'builtin']) assert.deepEqual(validateFieldEntity({ kind, id: 'test' }), { kind, id: 'test' });
});

test('unknown metadata keys, polluted prototypes and unsupported kind/schema cannot enter durable layout', () => {
  for (const change of [v => v.accountId = 'other', v => v.shortcuts[0].payload = { title: 'Private' },
    v => v.contexts[0].membership = 'active', v => v.shortcuts[0].entity.hostDeviceId = 'private-host',
    v => v.schema = 'soty.field.v2', v => v.shortcuts[0].entity.kind = 'grant']) {
    const value = make(); change(value); bad(value);
  }
  const value = make(); Object.setPrototypeOf(value.shortcuts[0], { payload: 'hidden' }); bad(value);
});

test('duplicate identity, occupied axial slot and missing context refuse without mutating source', () => {
  for (const change of [v => v.contexts.push(structuredClone(v.contexts[0])),
    v => v.shortcuts.push({ ...structuredClone(v.shortcuts[0]), slot: [1, 0] }),
    v => v.shortcuts.push({ ...structuredClone(v.shortcuts[0]), shortcutId: 'b' }),
    v => v.shortcuts[0].contextId = 'unknown']) {
    const value = make(); change(value); const prior = structuredClone(value); bad(value); assert.deepEqual(value, prior);
  }
});

test('invalid numbers, oversized or fractional axial slots and unprintable text refuse', () => {
  for (const change of [v => v.contexts[0].x = Infinity, v => v.contexts[0].y = NaN,
    v => v.contexts[0].x = FIELD_LIMITS.coordinate + 1, v => v.shortcuts[0].slot = [0, .5],
    v => v.shortcuts[0].slot = [FIELD_LIMITS.slotCoordinate + 1, 0], v => v.shortcuts[0].slot = [0],
    v => v.contexts[0].title = 'x'.repeat(FIELD_LIMITS.titleLength + 1), v => v.contexts[0].title = ' \n ',
    v => v.shortcuts[0].entity.id = 'id\u0000token', v => v.contexts[0].title = '\ud800',
    v => v.shortcuts[0].shortcutId = ' padded ']) { const value = make(); change(value); bad(value); }
});

test('limits accept a complete 256-shortcut layout and reject 257/25 before building an oversized scene', () => {
  const value = make(); value.shortcuts = Array.from({ length: FIELD_LIMITS.shortcuts }, (_, i) => ({ shortcutId: `s-${i}`,
    entity: { kind: i % 2 ? 'person' : 'app', id: `entity-${i}` }, contextId: 'studio', slot: [i, 0] }));
  assert.equal(validateFieldDocument(value).shortcuts.length, 256);
  value.shortcuts.push({ ...value.shortcuts[0], shortcutId: 'excess', slot: [256, 0] });
  assert.throws(() => validateFieldDocument(value), error => error.code === 'field_limit_exceeded');
  const contexts = make(); contexts.contexts = Array.from({ length: 25 }, (_, i) => ({ contextId: `context-${i}`, title: 'Title', x: i * 10, y: 0 }));
  assert.throws(() => validateFieldDocument(contexts), error => error.code === 'field_limit_exceeded');
});

test('wire budget uses UTF-8 bytes, not JS string length', () => {
  const contextId = '界'.repeat(128), value = { schema: FIELD_SCHEMA, contexts: [{ contextId, title: '测试', x: 0, y: 0 }],
    shortcuts: Array.from({ length: 256 }, (_, i) => ({ shortcutId: String(i) + '界'.repeat(124), entity: { kind: 'app', id: '界'.repeat(128) }, contextId, slot: [i, 0] })) };
  assert.ok(JSON.stringify(value).length < FIELD_LIMITS.bytes);
  assert.throws(() => validateFieldDocument(value), error => error.code === 'field_limit_exceeded');
});
