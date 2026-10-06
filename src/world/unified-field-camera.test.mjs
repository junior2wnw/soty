import test from 'node:test';
import assert from 'node:assert/strict';
import { fieldWorldToScreen, fieldScreenToWorld, normalizeFieldCamera, zoomFieldCamera, fitFieldCamera, fieldLevelOfDetail } from './unified-field-camera.mjs';
const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-7, `${a} != ${b}`);

test('world and screen invert exactly at desktop/mobile and extreme user coordinates', () => {
  for (const width of [320, 390, 768, 1024, 1440]) for (const scale of [.0001, .35, .7, 1, 2]) {
    const point = { x: -865.5, y: 1244.8 }, camera = { x: 1300, y: -425, scale }, view = { width, height: 710 };
    const result = fieldScreenToWorld(fieldWorldToScreen(point, camera, view), camera, view); near(result.x, point.x); near(result.y, point.y);
  }
});
test('anchored wheel/pinch zoom keeps the world object under the exact screen anchor', () => {
  const camera = { x: 140, y: -90, scale: .7 }, view = { width: 390, height: 620 }, anchor = { x: 89, y: 410 };
  const original = fieldScreenToWorld(anchor, camera, view);
  for (const scale of [.05, .3, 1, 2]) { const zoomed = zoomFieldCamera(camera, scale, anchor, view); const actual = fieldScreenToWorld(anchor, zoomed, view); near(actual.x, original.x); near(actual.y, original.y); }
});
test('overview fits arbitrary distant valid coordinates while focus can enforce readable item scale', () => {
  const bounds = { left: -1e6, top: -900000, right: 1e6, bottom: 1e6 }, viewport = { width: 320, height: 500 };
  const overview = fitFieldCamera(bounds, viewport, { padding: 12 });
  const a = fieldWorldToScreen({ x: bounds.left, y: bounds.top }, overview, viewport), b = fieldWorldToScreen({ x: bounds.right, y: bounds.bottom }, overview, viewport);
  assert.ok(a.x >= 11.99 && b.x <= 308.01 && a.y >= 11.99 && b.y <= 488.01);
  assert.equal(fitFieldCamera(bounds, viewport, { minScale: .45 }).scale, .45);
});
test('semantic LOD hysteresis removes chatter and invalid camera input stays finite', () => {
  assert.equal(fieldLevelOfDetail(.43, 'overview'), 'overview'); assert.equal(fieldLevelOfDetail(.43, 'context'), 'context');
  assert.equal(fieldLevelOfDetail(.82, 'detail'), 'detail'); assert.equal(fieldLevelOfDetail(.82, 'context'), 'context');
  const camera = normalizeFieldCamera({ x: NaN, y: Infinity, scale: -5 }); assert.ok(Object.values(camera).every(Number.isFinite));
  assert.equal(normalizeFieldCamera({ x: -4e6 }).x, -3e6);
});
