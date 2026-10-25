import test from 'node:test';
import assert from 'node:assert/strict';
import { floatingObstacles, placeFloatingControl } from './floating-layout.ts';

const view = (width = 390, height = 844) => ({ left: 0, top: 0, width, height });
const touches = (point, rect) => point.x < rect.left + rect.width && point.x + 44 > rect.left
  && point.y < rect.top + rect.height && point.y + 44 > rect.top;

test('saved assistant position leaves the whole Create button, input and widget controls available', () => {
  for (const [width, height] of [[320, 568], [390, 844], [1440, 900]]) {
    const controls = [
      { left: width - 166, top: height - 172, width: 124, height: 44 },
      { left: 20, top: height - 300, width: width - 40, height: 116 },
      { left: width - 55, top: 58, width: 44, height: 44 },
    ];
    const preferred = { x: width - 85, y: height - 166 };
    const before = structuredClone(preferred), placed = placeFloatingControl(preferred, view(width, height), controls);
    assert.ok(controls.every(control => !touches(placed, control)), 'every point in each action remains outside the assistant');
    assert.deepEqual(preferred, before, 'projection never edits the stored preference');
    assert.ok(placed.x >= 6 && placed.x + 50 <= width && placed.y >= 6 && placed.y + 132 <= height);
    assert.deepEqual(placeFloatingControl(preferred, view(width, height), []), preferred, 'the preference returns after a transient obstacle closes');
  }
});

test('launcher uses its reserved header slot instead of the audited 320px close and 390px contacts areas', () => {
  for (const [width, height, audited] of [[320, 568, { left: 265, top: 58.5, width: 44, height: 44 }],
    [390, 844, { left: 334, top: 74, width: 36, height: 44 }]]) {
    const orb = { left: width - 50, top: 6, width: 44, height: 44 };
    const point = placeFloatingControl({ x: width - 100, y: 6 }, view(width, height), [audited, orb]);
    assert.deepEqual(point, { x: width - 100, y: 6 });
    assert.equal(touches(point, audited), false); assert.equal(touches(point, orb), false);
  }
});

test('offset keyboard viewport and a full-width iframe still leave a reachable free header dock', () => {
  const viewport = { left: 12, top: 150, width: 320, height: 260 };
  const frame = { left: 12, top: 206, width: 320, height: 204 };
  const placed = placeFloatingControl({ x: 280, y: 270 }, viewport, [frame]);
  assert.equal(touches(placed, frame), false); assert.ok(placed.y >= 156); assert.ok(placed.y + 44 <= 200);
  assert.deepEqual(placeFloatingControl({ x: Infinity, y: NaN }, view(30, 30), []), { x: 6, y: 6 });
});

test('projection finds a nearby free region across several competing controls, including disabled CTA geometry', () => {
  const blockers = [{ left: 270, top: 6, width: 44, height: 44 }, { left: 216, top: 6, width: 44, height: 44 },
    { left: 168, top: 6, width: 44, height: 44 }, { left: 6, top: 56, width: 308, height: 420 }];
  const placed = placeFloatingControl({ x: 270, y: 56 }, view(320, 568), blockers);
  assert.ok(blockers.every(control => !touches(placed, control)));
});

test('collector skips hidden/inert/zero geometry and the owned control, while retaining iframe and disabled button rectangles', () => {
  const rectangle = { left: 10, top: 20, width: 44, height: 44 };
  const element = (options = {}) => ({ closest: () => options.hidden ?? null, getClientRects: () => options.zero ? [] : [rectangle],
    getBoundingClientRect: () => rectangle, ...options });
  const own = element(), visible = element({ disabled: true }), frame = element(), hidden = element({ hidden: {} }), zero = element({ zero: true }), invisible = element({ invisible: true });
  const doc = { querySelectorAll: () => [own, visible, frame, hidden, zero, invisible], defaultView: { getComputedStyle: element => ({ visibility: element.invisible ? 'hidden' : 'visible' }) } };
  assert.deepEqual(floatingObstacles(doc, element => element === own), [rectangle, rectangle]);
});

test('compound search controls reserve their whole visible surface instead of only the inner text input', () => {
  const surface = { left: 12, top: 224, width: 296, height: 52 };
  const group = { closest: () => null, getClientRects: () => [surface], getBoundingClientRect: () => surface };
  const doc = { querySelectorAll(selector) { assert.ok(selector.includes('[data-floating-obstacle]')); return [group]; } };
  const obstacles = floatingObstacles(doc, () => false);
  assert.deepEqual(obstacles, [surface]);
  assert.equal(touches(placeFloatingControl({ x: 6, y: 216 }, view(320), obstacles), surface), false);
});

test('scrolled-out children of a reserved result panel do not falsely occupy the rest of the viewport', () => {
  const surface = { left: 12, top: 282, width: 296, height: 264 };
  const group = { closest: () => null, getClientRects: () => [surface], getBoundingClientRect: () => surface };
  const clipped = { ...group, parentElement: { closest: selector => selector === '[data-floating-obstacle]' ? group : null },
    getBoundingClientRect: () => ({ left: 21, top: 760, width: 278, height: 88 }) };
  assert.deepEqual(floatingObstacles({ querySelectorAll: () => [group, clipped] }, () => false), [surface]);
});
