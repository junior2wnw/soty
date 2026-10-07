import test from 'node:test';
import assert from 'node:assert/strict';
import { applyFieldCommand, createFieldHistory, nextFieldSlot, recordFieldHistory, undoFieldHistory } from './unified-field-state.mjs';
import { fieldSlotToPoint, fieldFootprint, fieldPolygonsOverlap } from './unified-field-layout.mjs';

test('automatic add avoids objects in adjacent spaces without moving an existing object', () => {
  const source = fixture(); source.contexts[1].x = 0;
  const before = structuredClone(source), slot = nextFieldSlot(source, 'home', 'app');
  const added = applyFieldCommand(source, { type: 'add-shortcut', shortcutId: 'new', entity: { kind: 'app', id: 'new-project' }, contextId: 'home' });
  const origin = source.contexts[1], point = fieldSlotToPoint(slot);
  const polygon = fieldFootprint('app', { x: origin.x + point.x, y: origin.y + point.y });
  for (const shortcut of source.shortcuts) {
    const context = source.contexts.find(value => value.contextId === shortcut.contextId), other = fieldSlotToPoint(shortcut.slot);
    assert.equal(fieldPolygonsOverlap(polygon, fieldFootprint(shortcut.entity.kind, { x: context.x + other.x, y: context.y + other.y }), 10), false);
  }
  assert.deepEqual(source, before); assert.deepEqual(added.document.shortcuts.slice(0, 2), source.shortcuts);
  assert.deepEqual(added.document.contexts, source.contexts); assert.deepEqual(added.document.shortcuts.at(-1).slot, slot);
});

const fixture = () => ({ schema: 'soty.field.v1', contexts: [{ contextId: 'work', title: 'Работа', x: 0, y: 0 }, { contextId: 'home', title: 'Дома', x: 900, y: 0 }],
  shortcuts: [{ shortcutId: 'a', entity: { kind: 'app', id: 'hive' }, contextId: 'work', slot: [0, 0] },
    { shortcutId: 'b', entity: { kind: 'app', id: 'chess' }, contextId: 'home', slot: [0, 0] }] });

test('cross-context move is only shortcut placement; duplicate source shortcut remains fixed', () => {
  const source = fixture(); source.shortcuts.push({ shortcutId: 'a2', entity: { kind: 'app', id: 'hive' }, contextId: 'home', slot: [1, 0] });
  const moved = applyFieldCommand(source, { type: 'move-shortcut', shortcutId: 'a', contextId: 'home', slot: [-1, 1] });
  assert.deepEqual(source, moved.before); assert.deepEqual(moved.document.shortcuts[2], source.shortcuts[2]);
  assert.deepEqual(moved.document.shortcuts[0].entity, source.shortcuts[0].entity); assert.equal(moved.document.shortcuts[0].contextId, 'home');
  assert.deepEqual(Object.keys(moved.document).sort(), ['contexts', 'schema', 'shortcuts']);
});

test('occupied slot requires explicit swap and changes exactly two placements atomically', () => {
  const source = fixture(); const prior = structuredClone(source);
  assert.throws(() => applyFieldCommand(source, { type: 'move-shortcut', shortcutId: 'a', contextId: 'home', slot: [0, 0] }), e => e.code === 'field_slot_occupied');
  assert.deepEqual(source, prior);
  const swap = applyFieldCommand(source, { type: 'move-shortcut', shortcutId: 'a', contextId: 'home', slot: [0, 0], swap: true });
  assert.equal(swap.document.shortcuts[0].contextId, 'home'); assert.equal(swap.document.shortcuts[1].contextId, 'work'); assert.equal(swap.affected.length, 2);
  const again = applyFieldCommand(swap.document, { type: 'move-shortcut', shortcutId: 'a', contextId: 'work', slot: [0, 0], swap: true });
  assert.deepEqual(again.document, prior);
});

test('whole-context move leaves every child slot and source identity byte-equal', () => {
  const source = fixture(); const result = applyFieldCommand(source, { type: 'move-context', contextId: 'work', x: -400, y: 350 });
  assert.deepEqual(result.document.shortcuts, source.shortcuts); assert.equal(result.document.contexts[0].x, -400);
  assert.deepEqual(result.document.contexts[1], source.contexts[1]);
});

test('undo restores exact placement and refuses after a concurrent edit', () => {
  const source = fixture(), history = createFieldHistory();
  const change = applyFieldCommand(source, { type: 'move-shortcut', shortcutId: 'a', contextId: 'home', slot: [0, 0], swap: true });
  recordFieldHistory(history, change);
  const external = applyFieldCommand(change.document, { type: 'rename-context', contextId: 'home', title: 'Другая версия' });
  assert.throws(() => undoFieldHistory(history, external.document), e => e.code === 'field_undo_conflict'); assert.equal(history.entries.length, 1);
  assert.deepEqual(undoFieldHistory(history, change.document).document, source); assert.equal(history.entries.length, 0);
});

test('new object fills a free typed slot without displacing 120 manual positions', () => {
  const source = fixture(); source.shortcuts = [];
  let current = source;
  for (let i = 0; i < 120; i++) current = applyFieldCommand(current, { type: 'add-shortcut', shortcutId: `s${i}`, entity: { kind: i % 7 ? 'app' : 'person', id: `e${i}` }, contextId: 'work' }).document;
  const prior = structuredClone(current.shortcuts), slot = nextFieldSlot(current, 'work', 'device');
  const result = applyFieldCommand(current, { type: 'add-shortcut', shortcutId: 'new', entity: { kind: 'device', id: 'opaque-own-device' }, contextId: 'work', slot });
  assert.deepEqual(result.document.shortcuts.slice(0, 120), prior); assert.equal(new Set(result.document.shortcuts.map(s => s.slot.join(','))).size, 121);
});

test('bounded history and expected-before make no-op and stale restore explicit', () => {
  let current = fixture(); const history = createFieldHistory(3);
  for (let i = 0; i < 7; i++) { const change = applyFieldCommand(current, { type: 'rename-context', contextId: 'work', title: `Название ${i}` }); recordFieldHistory(history, change); current = change.document; }
  assert.equal(history.entries.length, 3);
  const noChange = applyFieldCommand(current, { type: 'move-shortcut', shortcutId: 'a', contextId: 'work', slot: [0, 0] }); assert.equal(noChange.changed, false);
  assert.throws(() => applyFieldCommand(current, { type: 'restore', expected: fixture(), document: fixture() }), e => e.code === 'field_conflict');
  assert.throws(() => applyFieldCommand(current, { type: 'remove-context', contextId: 'work' }), e => e.code === 'field_context_not_empty');
});

test('explicit context removal with shortcuts is atomic, undoable and does not alter other context', () => {
  const source = fixture(), history = createFieldHistory();
  const removed = applyFieldCommand(source, { type: 'remove-context', contextId: 'work', removeShortcuts: true }); recordFieldHistory(history, removed);
  assert.deepEqual(removed.document.contexts, [source.contexts[1]]); assert.deepEqual(removed.document.shortcuts, [source.shortcuts[1]]);
  assert.deepEqual(removed.affected, ['a']); assert.deepEqual(undoFieldHistory(history, removed.document).document, source);
});
