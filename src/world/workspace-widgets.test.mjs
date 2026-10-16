import test from 'node:test';
import assert from 'node:assert/strict';
import { editWidgetWorkspace, readWidgetWorkspace, normalizeWidget, widgetRect, widgetPosition, widgetStorageKey, widgetLimit } from './workspace-widgets.mjs';

const widget = (id = '11111111-1111-4111-8111-111111111111') => ({ id: `widget-${id}`, name: 'Мои задачи',
  route: 'launch/app-11111111111111111111111111111111/dom_11111111111111111111111111111111?path=%2Fwidget',
  visible: true, x: 0.4, y: 0.25, width: 320, height: 260 });
const memory = () => { const rows = new Map(); return { rows, getItem: key => rows.get(key) ?? null, setItem: (key, value) => rows.set(key, value) }; };

test('reload restores each account workspace, visibility, size and front order without storing authority', () => {
  const store = memory(), first = widget(), second = { ...widget('22222222-2222-4222-8222-222222222222'), name: 'Часы', visible: false };
  assert.equal(editWidgetWorkspace(store, 'owner', state => { state.widgets = [second, first]; state.hidden = true; }).saved, true);
  assert.deepEqual(readWidgetWorkspace(store, 'owner'), { hidden: true, widgets: [second, first] });
  assert.deepEqual(readWidgetWorkspace(store, 'reporter'), { hidden: false, widgets: [] });
  const text = store.getItem(widgetStorageKey('owner'));
  assert.ok(!text.includes('launchUrl') && !text.includes('handle') && !text.includes('token'));
});

test('two stale tabs edit different widgets without erasing each other', () => {
  const store = memory(), first = widget(), second = widget('22222222-2222-4222-8222-222222222222');
  editWidgetWorkspace(store, 'owner', state => { state.widgets = [first, second]; });
  const stale = readWidgetWorkspace(store, 'owner');
  editWidgetWorkspace(store, 'owner', state => { state.widgets[1].visible = false; });
  editWidgetWorkspace(store, 'owner', state => { state.widgets.find(item => item.id === stale.widgets[0].id).x = 0.8; });
  const state = readWidgetWorkspace(store, 'owner'); assert.equal(state.widgets[0].x, 0.8); assert.equal(state.widgets[1].visible, false);
});

test('presentation changes preserve the selected native entry and never accept external or administrative routes', () => {
  for (const route of ['https://foreign.invalid/', '//foreign.invalid/', 'notes/note-1', 'app/app-11111111111111111111111111111111?panel=discussion&admin=1',
    'launch/app-11111111111111111111111111111111/dom_11111111111111111111111111111111?path=%2F_soty%2Fboot', 'app/app-11111111111111111111111111111111?handle=secret']) {
    assert.equal(normalizeWidget({ ...widget(), route }), null, route);
  }
  assert.equal(normalizeWidget(widget()).route, widget().route);
  assert.deepEqual(Object.keys(normalizeWidget({ ...widget(), launchUrl: 'secret', handle: 'secret', token: 'secret' })).sort(), ['height', 'id', 'name', 'route', 'visible', 'width', 'x', 'y']);
});

test('malformed, oversized, duplicate and poisoned preferences cannot create arbitrary frames', () => {
  const store = memory();
  for (const raw of ['null', '{bad', JSON.stringify({ schema: 'other', widgets: [widget()] }), 'x'.repeat(220001)]) {
    store.setItem(widgetStorageKey('owner'), raw); assert.deepEqual(readWidgetWorkspace(store, 'owner'), { hidden: false, widgets: [] });
  }
  store.setItem(widgetStorageKey('owner'), JSON.stringify({ schema: 'soty.workspace-widgets.v1', widgets: [widget(), widget(), { ...widget(), id: '../../anything' }] }));
  assert.equal(readWidgetWorkspace(store, 'owner').widgets.length, 1);
  const bounded = normalizeWidget({ ...widget(), x: -2, y: 5, width: 10000, height: -9 });
  assert.deepEqual([bounded.x, bounded.y, bounded.width, bounded.height], [0, 1, 640, 160]);
});

test('size-aware movement is inside all supported viewports and resizing does not change saved desktop size', () => {
  const saved = { ...widget(), width: 640, height: 640, x: 1, y: 1 };
  for (const width of [320, 390, 1280, 1366]) for (const height of [568, 844]) {
    const viewport = { left: 17, top: 23, width, height }, rect = widgetRect(saved, viewport);
    assert.ok(rect.x >= viewport.left && rect.y >= viewport.top);
    assert.ok(rect.x + rect.width <= viewport.left + width);
    assert.ok(rect.y + rect.height <= viewport.top + height);
    assert.deepEqual(widgetPosition(rect, viewport, saved), { x: 1, y: 1 });
  }
  assert.equal(saved.width, 640); assert.equal(saved.height, 640);
});

test('runtime shell insets reserve actual headers and navigation without changing a normalized preference', () => {
  const saved = { ...widget(), width: 640, height: 640, x: .7, y: .3 }, before = { ...saved };
  const desktop = { left: 0, top: 0, width: 1280, height: 900 }, original = widgetRect(saved, desktop);
  for (const topInset of [112, 211.17, 280]) {
    const viewport = { left: 11, top: 23, width: 320, height: 568, topInset, bottomInset: 72 };
    const rect = widgetRect(saved, viewport);
    assert.equal(rect.y, viewport.top + topInset);
    assert.equal(rect.y + rect.height, viewport.top + viewport.height - viewport.bottomInset);
    assert.deepEqual(widgetPosition(rect, viewport, saved), { x: saved.x, y: saved.y }, 'a clamped axis retains its previous normalized position');
  }
  assert.deepEqual(saved, before); assert.deepEqual(widgetRect(saved, desktop), original);
});

test('movement inversion uses the current reserved area on each axis', () => {
  const saved = widget(), viewport = { left: 17, top: 23, width: 1280, height: 900, topInset: 184, bottomInset: 108 };
  const rect = widgetRect(saved, viewport), moved = { ...rect, x: rect.x + 40, y: rect.y + 32 };
  const next = { ...saved, ...widgetPosition(moved, viewport, saved) }, projected = widgetRect(next, viewport);
  assert.ok(Math.abs(projected.x - moved.x) < 1e-9); assert.ok(Math.abs(projected.y - moved.y) < 1e-9);
  assert.equal(next.width, saved.width); assert.equal(next.height, saved.height);
});

test('extreme visual viewports retain a visible header without producing offscreen or negative rectangles', () => {
  const saved = { ...widget(), width: 640, height: 640, x: 1, y: 1 };
  for (const width of [1, 10, 320]) for (const height of [1, 24, 48, 80, 180]) {
    const viewport = { left: 17, top: 23, width, height, topInset: 240, bottomInset: 120 };
    const rect = widgetRect(saved, viewport);
    assert.ok(rect.width >= 1 && rect.height >= Math.min(48, height));
    assert.ok(rect.x >= viewport.left && rect.x + rect.width <= viewport.left + width);
    assert.ok(rect.y >= viewport.top && rect.y + rect.height <= viewport.top + height);
  }
  assert.deepEqual(widgetRect(saved, { left: 0, top: 0, width: 1280, height: 900, topInset: NaN, bottomInset: Infinity }),
    widgetRect(saved, { left: 0, top: 0, width: 1280, height: 900 }));
});

test('quota errors and unavailable local storage are reported as unsaved while preserving the current result', () => {
  const store = memory(); store.setItem = () => { throw new Error('quota'); };
  const result = editWidgetWorkspace(store, 'owner', state => { state.widgets.push(widget()); });
  assert.equal(result.saved, false); assert.equal(result.state.widgets.length, 1);
  assert.equal(editWidgetWorkspace(null, 'owner', state => { state.widgets.push(widget()); }).saved, false);
  assert.equal(editWidgetWorkspace(memory(), '', state => { state.widgets.push(widget()); }).saved, false);
});

test('saved windows are bounded and remove remains separate from hiding', () => {
  const store = memory(); editWidgetWorkspace(store, 'owner', state => { state.widgets = Array.from({ length: widgetLimit + 9 }, (_, i) => widget(`${String(i).padStart(8, '0')}-1111-4111-8111-111111111111`)); });
  assert.equal(readWidgetWorkspace(store, 'owner').widgets.length, widgetLimit);
  editWidgetWorkspace(store, 'owner', state => { state.widgets[0].visible = false; });
  assert.equal(readWidgetWorkspace(store, 'owner').widgets.length, widgetLimit);
  editWidgetWorkspace(store, 'owner', state => { state.widgets = state.widgets.slice(1); });
  assert.equal(readWidgetWorkspace(store, 'owner').widgets.length, widgetLimit - 1);
});
