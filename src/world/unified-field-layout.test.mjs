import test from 'node:test';
import assert from 'node:assert/strict';
import { fieldSlotToPoint, fieldPointToSlot, layoutUnifiedField, createFieldSpatialIndex, fieldMovePreview, nearestFieldNode, fieldPolygonsOverlap } from './unified-field-layout.mjs';
import { applyFieldCommand } from './unified-field-state.mjs';
const document = () => ({ schema: 'soty.field.v1', contexts: [{ contextId: 'studio', title: 'Студия', x: 0, y: 0 }], shortcuts: [
  { shortcutId: 'a', entity: { kind: 'app', id: 'a' }, contextId: 'studio', slot: [0, 0] },
  { shortcutId: 'b', entity: { kind: 'app', id: 'b' }, contextId: 'studio', slot: [-25, 19] },
  { shortcutId: 'c', entity: { kind: 'app', id: 'c' }, contextId: 'studio', slot: [6, 19] }] });
const entities = doc => doc.shortcuts.map(s => ({ entity: s.entity, title: s.entity.id }));

test('point-up axial slots round trip for negative and distant cells', () => {
  for (const q of [-1000, -2, -1, 0, 1, 20, 1000]) for (const r of [-50, -1, 0, 1, 500]) assert.deepEqual(fieldPointToSlot(fieldSlotToPoint([q, r])), [q, r]);
});
test('regular app hexes do not collide even when their rectangular bounds interlock', () => {
  const source = document(), layout = layoutUnifiedField(source, entities(source));
  for (const a of layout.nodes) for (const b of layout.nodes) if (a !== b) assert.equal(fieldPolygonsOverlap(a.footprint, b.footprint, 10), false);
  assert.ok(layout.contexts[0].contour.startsWith('M '));
});
test('rendering only allowed entities never creates a person or copies private payload', () => {
  const source = document(); source.shortcuts.push({ shortcutId: 'hidden', entity: { kind: 'person', id: 'private' }, contextId: 'studio', slot: [2, 0] });
  const layout = layoutUnifiedField(source, entities(source).filter(e => e.entity.id !== 'private'));
  assert.equal(layout.nodes.some(node => node.shortcutId === 'hidden'), false); assert.equal(layout.nodes.length, 3);
  assert.equal(JSON.stringify(layout).includes('private'), false);
});
test('move/swap preview never changes document; cancel is simply discarding preview', () => {
  const source = document(), prior = structuredClone(source);
  const preview = fieldMovePreview(source, entities(source), 'a', { contextId: 'studio', slot: [-25, 19], swap: true });
  assert.equal(preview.valid, true); assert.equal(preview.occupied, 'b'); assert.deepEqual(source, prior);
  assert.deepEqual(preview.layout.nodes.find(n => n.shortcutId === 'a').slot, [-25, 19]);
  assert.equal(fieldMovePreview(source, entities(source), 'a', { contextId: 'studio', slot: [.2, 0] }).valid, false);
});
test('100+ mixed objects preserve established coordinates after append and metadata changes', () => {
  let source = document();
  for (let i = 3; i < 120; i++) source = applyFieldCommand(source, { type: 'add-shortcut', shortcutId: `s${i}`, entity: { kind: i % 8 === 0 ? 'person' : i % 9 === 0 ? 'device' : 'app', id: `e${i}` }, contextId: 'studio' }).document;
  const before = layoutUnifiedField(source, entities(source));
  const afterDoc = applyFieldCommand(source, { type: 'add-shortcut', shortcutId: 'more', entity: { kind: 'app', id: 'more' }, contextId: 'studio' }).document;
  const after = layoutUnifiedField(afterDoc, entities(afterDoc).map(e => ({ ...e, title: 'Длинное обновлённое название' })));
  assert.deepEqual(after.nodes.slice(0, 120).map(n => [n.shortcutId, n.cx, n.cy]), before.nodes.map(n => [n.shortcutId, n.cx, n.cy]));
  const index = createFieldSpatialIndex(after.nodes), visible = index.query({ left: -200, top: -200, right: 300, bottom: 400 }, 0);
  assert.ok(visible.length > 0 && visible.length < 20); assert.equal(new Set(visible.map(n => n.shortcutId)).size, visible.length);
  assert.ok(index.query({ left: -1e6, top: -1e6, right: 1e6, bottom: 1e6 }).length === 121);
});
test('keyboard neighbours are directional and deterministic, including offscreen candidates', () => {
  const source = document(), nodes = layoutUnifiedField(source, entities(source)).nodes;
  assert.equal(nearestFieldNode(nodes, 'a', [0, 1]).shortcutId, 'b');
  assert.equal(nearestFieldNode(nodes, 'b', [1, 0]).shortcutId, 'c');
  assert.equal(nearestFieldNode(nodes, 'c', [-1, 0]).shortcutId, 'b');
});
