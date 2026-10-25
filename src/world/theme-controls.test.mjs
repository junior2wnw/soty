import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import { normalizeTheme } from './theme/palette.mjs';

const compiled = ts.transpileModule(await readFile(new URL('./theme/theme.ts', import.meta.url), 'utf8'),
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
class Node extends EventTarget {
  constructor(tag) { super(); Object.assign(this, { tagName: tag, children: [], attributes: new Map(), hidden: false }); }
  append(...nodes) { this.children.push(...nodes); }
  setAttribute(key, value) { this.attributes.set(key, value); }
  all() { return [this, ...this.children.flatMap(node => node.all())]; }
  remove() {}
}

test('theme choices have visible labels and restore the pre-open values through the same controller', () => {
  const document = { createElement: tag => new Node(tag), createElementNS: (_ns, tag) => new Node(tag) }, module = { exports: {} };
  vm.runInNewContext(compiled, { document, module, exports: module.exports,
    require: id => id === './palette.mjs' ? { normalizeTheme } : {} });
  let state = { themeMode: 'system', themeBrightness: 42, resolvedTheme: 'light' }; const listeners = new Set(), writes = [];
  const controller = { get: () => ({ ...state }), set(patch, options = {}) { state = { ...state, ...patch }; if (options.persist !== false) writes.push({ ...patch }); for (const listener of listeners) listener(state); },
    subscribe(listener) { listeners.add(listener); listener(state); return () => listeners.delete(listener); } };
  const controls = module.exports.createThemeControls(controller), nodes = controls.element.all();
  const dark = nodes.find(node => node.attributes.get('aria-label') === 'Тёмная'), undo = nodes.find(node => node.className?.includes('sw-theme-undo'));
  assert.equal(dark.children.at(-1).textContent, 'Тёмная'); assert.equal(undo.hidden, true);
  dark.dispatchEvent(new Event('click')); assert.equal(state.themeMode, 'dark'); assert.equal(undo.hidden, false);
  const range = nodes.find(node => node.type === 'range'); range.value = '67'; range.dispatchEvent(new Event('input'));
  assert.equal(state.themeBrightness, 67); assert.equal(writes.length, 1, 'brightness preview does not persist on each input');
  range.dispatchEvent(new Event('change')); undo.dispatchEvent(new Event('click'));
  assert.deepEqual(normalizeTheme(state), { themeMode: 'system', themeBrightness: 42 }); assert.equal(undo.hidden, true);
  controls.destroy(); assert.equal(listeners.size, 0);
});
