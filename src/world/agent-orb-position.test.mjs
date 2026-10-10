import test from 'node:test';
import assert from 'node:assert/strict';
import { clampOrb, dockOrb, readOrbPosition, writeOrbPosition } from './agent-orb-position.ts';

const viewport = (width = 320, height = 740) => ({ width, height, left: 0, top: 0 });
const storage = () => { const values = new Map(); return { values, getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value), removeItem: key => values.delete(key) }; };

test('default 44px orb stays in the header on desktop, 320 and 390 without covering the bottom composer', () => {
  for (const width of [320, 390, 1440]) {
    const point = dockOrb(viewport(width));
    assert.equal(point.y, 6); assert.equal(point.x + 44 + 6, width);
    const moved = clampOrb({ x: -200, y: 9999 }, viewport(width));
    assert.equal(moved.x, 6); assert.equal(moved.y + 44 + 88, 740);
  }
});
test('visible keyboard viewport, pan and invalid drag coordinates cannot strand the control', () => {
  const view = { width: 320, height: 290, top: 30, left: 12 };
  assert.deepEqual(clampOrb({ x: NaN, y: Infinity }, view), { x: 18, y: 36 });
  const bounded = clampOrb({ x: 9999, y: 9999 }, view);
  assert.equal(bounded.x + 44 + 6, 332); assert.equal(bounded.y + 44 + 88, 320);
  assert.deepEqual(dockOrb(viewport(30, 30)), { x: 6, y: 6 });
});
test('layout preference contains only coordinates and remains scoped through A to B to A', () => {
  const store = storage(), view = viewport();
  writeOrbPosition(store, 'A', { x: 96, y: 180 }, view);
  assert.deepEqual(JSON.parse(store.getItem('soty.agent-orb.v1:A')), { x: .3, y: 180 / 740 });
  assert.equal(readOrbPosition(store, 'B', view), null);
  writeOrbPosition(store, 'B', { x: 24, y: 80 }, view);
  assert.deepEqual(readOrbPosition(store, 'A', view), { x: 96, y: 180 });
  writeOrbPosition(store, 'A', null, view);
  assert.equal(readOrbPosition(store, 'A', view), null); assert.ok(readOrbPosition(store, 'B', view));
});
test('corrupt, oversized, extra-field and anonymous preferences fail to the visible default', () => {
  const store = storage();
  for (const value of ['broken', '[]', '{"x":1,"y":2}', '{"x":0,"y":0,"token":"redacted"}', '{"x":1e999,"y":0}']) {
    store.setItem('soty.agent-orb.v1:A', value); assert.equal(readOrbPosition(store, 'A', viewport()), null);
  }
  writeOrbPosition(store, '', { x: 10, y: 20 }, viewport()); assert.equal(readOrbPosition(store, '', viewport()), null);
  const unavailable = { getItem() { throw new Error('disabled'); }, setItem() { throw new Error('quota'); }, removeItem() { throw new Error('disabled'); } };
  assert.equal(readOrbPosition(unavailable, 'A', viewport()), null); assert.doesNotThrow(() => writeOrbPosition(unavailable, 'A', null, viewport()));
});
test('saved desktop preference scales and clamps after rotation to a phone', () => {
  const store = storage(); writeOrbPosition(store, 'A', { x: 1280, y: 600 }, viewport(1440, 900));
  const point = readOrbPosition(store, 'A', viewport(320, 390));
  assert.ok(point.x >= 6 && point.x + 50 <= 320); assert.ok(point.y >= 6 && point.y + 132 <= 390);
});
