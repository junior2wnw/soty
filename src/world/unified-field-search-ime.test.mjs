import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

// Invoke directly: node regression.test.mjs EXACT_PUBLIC_SCREEN_SOURCE.ts.
// The real inputChanged + input/composition/keydown wiring execute in RAM.
// Only DOM, routing, filtering and scheduler ports are supplied here.
const sourcePath = process.argv[2] ?? new URL('./unified-field-screen.ts', import.meta.url);
const source = readFileSync(sourcePath, 'utf8');
const startMarker = '  function inputChanged(immediate = false): void {';
const endMarker = "  wide.addEventListener('change', placeSearch, { signal });";
assert.equal(source.split(startMarker).length, 2);
const begin = source.indexOf(startMarker), end = source.indexOf(endMarker, begin);
assert.ok(end > begin);
const block = source.slice(begin, end).replace(startMarker, '  function inputChanged(immediate = false) {');
for (const type of ['input', 'compositionstart', 'compositionend', 'keydown']) {
  assert.equal(block.split(`searchInput.addEventListener('${type}'`).length, 2);
}
const genuine = new vm.Script(block, { filename: 'actual-unified-field-search-wiring.js' });

function harness(mode = 'search', value = 'Москва 東京') {
  const listeners = new Map(), timers = new Map();
  const calls = { closeList: 0, closePreview: 0, closeSearch: 0, filters: 0, routes: [], requests: 0, run: 0, timers: 0, clears: 0 };
  let nextTimer = 1;
  const searchInput = { value, addEventListener(type, listener) { listeners.set(type, listener); } };
  const context = {
    mode, composing: false, searchInput, queries: { mine: value, search: value }, signal: {},
    clear: { hidden: false }, element: { dataset: { query: 'active' } },
    searchTimer: null, searchGeneration: 0, searchCursor: 'cursor', more: { hidden: false }, searchLimit: { hidden: false },
    closeList() { calls.closeList++; }, closePreview() { calls.closePreview++; },
    closeSearch: { click() { calls.closeSearch++; } },
    route() { calls.routes.push({ mode: context.mode, query: context.queries[context.mode] }); },
    updateMineFilter() { calls.filters++; },
    searchDirectory() { calls.requests++; return Promise.resolve(); }, run() { calls.run++; },
    setTimeout(fn, delay) { const id = nextTimer++; timers.set(id, { fn, delay }); calls.timers++; return id; },
    clearTimeout(id) { calls.clears++; timers.delete(id); },
  };
  genuine.runInContext(vm.createContext(context), { timeout: 1000 });
  function emit(type, properties = {}) {
    let stopped = 0, prevented = 0;
    const event = { key: '', isComposing: false, ...properties, stopPropagation() { stopped++; }, preventDefault() { prevented++; } };
    assert.ok(listeners.has(type)); listeners.get(type)(event);
    return { stopped, prevented };
  }
  function snapshot() {
    return JSON.parse(JSON.stringify({ value: searchInput.value, queries: context.queries, composing: context.composing,
      generation: context.searchGeneration, cursor: context.searchCursor, queryState: context.element.dataset.query,
      clearHidden: context.clear.hidden, moreHidden: context.more.hidden, limitHidden: context.searchLimit.hidden,
      pendingTimers: timers.size, calls }));
  }
  return { emit, snapshot, context, searchInput, calls };
}

for (const mode of ['mine', 'search']) {
  for (const isComposing of [true, false]) {
    test(`${mode}: Escape cannot clear an active composition; event.isComposing=${isComposing}`, () => {
      const h = harness(mode);
      h.emit('compositionstart'); h.emit('input');
      const before = h.snapshot();
      const effect = h.emit('keydown', { key: 'Escape', isComposing });
      assert.deepEqual(h.snapshot(), before, 'IME cancellation must not clear query, change route/filter or enqueue a new search');
      assert.deepEqual(effect, { stopped: 0, prevented: 0 }, 'The search handler leaves IME Escape to its owner');
    });
  }
  test(`${mode}: ordinary Escape still clears after composition ends`, () => {
    const h = harness(mode);
    h.emit('compositionstart'); h.emit('input');
    h.searchInput.value = 'Москва 東京'; h.emit('compositionend');
    assert.equal(h.context.composing, false);
    const before = h.snapshot();
    const effect = h.emit('keydown', { key: 'Escape', isComposing: false });
    assert.equal(h.searchInput.value, ''); assert.equal(h.context.queries[mode], '');
    assert.equal(h.calls.routes.at(-1).query, ''); assert.deepEqual(effect, { stopped: 1, prevented: 0 });
    assert.equal(mode === 'mine' ? h.calls.filters - before.calls.filters : h.calls.requests - before.calls.requests, 1);
  });
}

test('event.isComposing alone protects a composing Escape without a delivered compositionstart', () => {
  const h = harness(); const before = h.snapshot();
  const effect = h.emit('keydown', { key: 'Escape', isComposing: true });
  assert.deepEqual(h.snapshot(), before); assert.deepEqual(effect, { stopped: 0, prevented: 0 });
});

test('empty ordinary Escape closes the search without changing its query or dispatching a request', () => {
  const h = harness('search', ''); const before = h.snapshot();
  const effect = h.emit('keydown', { key: 'Escape' });
  before.calls.closeSearch++;
  assert.deepEqual(h.snapshot(), before); assert.deepEqual(effect, { stopped: 1, prevented: 0 });
});

test('another key during composition remains untouched', () => {
  const h = harness(); h.emit('compositionstart'); h.emit('input'); const before = h.snapshot();
  const effect = h.emit('keydown', { key: 'Enter', isComposing: true });
  assert.deepEqual(h.snapshot(), before); assert.deepEqual(effect, { stopped: 0, prevented: 0 });
});
