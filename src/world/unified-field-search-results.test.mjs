import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import { resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';

const root = process.env.SOTY_REVIEW_FIELD_ROOT
  ? pathToFileURL(resolve(process.env.SOTY_REVIEW_FIELD_ROOT) + sep) : new URL('../../', import.meta.url);
const ts = createRequire(new URL('package.json', root))('typescript');
const { fieldEntityKey } = await import(new URL('modules/field/contract.mjs', root));
const geometry = await import(new URL('src/world/unified-field-layout.mjs', root));
const source = readFileSync(process.env.SOTY_REVIEW_FIELD_SCREEN_SOURCE || new URL('src/world/unified-field-screen.ts', root), 'utf8');
const parsed = ts.createSourceFile('unified-field-screen.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const screen = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedFieldScreen');
assert.ok(screen);
const statements = [...screen.body.statements];
const named = (nodes, name) => { const found = nodes.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name); assert.ok(found, name); return found; };
const declaration = (nodes, name) => { const found = nodes.find(node => ts.isVariableStatement(node)
  && node.declarationList.declarations.some(item => ts.isIdentifier(item.name) && item.name.text === name)); assert.ok(found, name); return found; };
const construction = statements.slice(statements.indexOf(declaration(statements, 'localSearch')), statements.indexOf(declaration(statements, 'filters')));
assert.ok(construction.length > 1);
const input = named(statements, 'inputChanged');
const wiringEnd = statements.findIndex(node => node.getText(parsed).startsWith("wide.addEventListener('change'"));
assert.ok(wiringEnd > statements.indexOf(input));
const code = ts.transpileModule([
  ...['BUILTINS', 'kindLabel', 'matches'].map(name => print(declaration(parsed.statements, name))),
  ...construction.map(print), print(declaration(statements, 'mineSearchSignature')),
  ...['listEntries', 'activate', 'renderMineSearchResults', 'updateMineFilter', 'updateMineEmpty'].map(name => print(named(statements, name))),
  ...statements.slice(statements.indexOf(input), wiringEnd).map(print),
  'Object.assign(exports, { inputChanged, render: renderMineSearchResults, entries: listEntries, surface: mineSearchResults, status: mineSearchStatus, list: mineSearchList });',
].join('\n'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;

// The maintained production functions execute together. Only DOM, routing,
// source records and the renderer are ports; no new search algorithm is copied.
function harness() {
  const calls = { open: [], preview: [], context: [], list: 0, focus: 0, route: [], writes: 0, camera: 0, close: 0 };
  class NodePort {
    children = []; hidden = false; isConnected = true; attributes = new Map(); handlers = new Map(); parent = null;
    constructor(tag = 'div', className = '', textContent = '') { Object.assign(this, { tag, className, textContent }); }
    append(...nodes) { for (const node of nodes) { node.parent = this; node.isConnected = this.isConnected; } this.children.push(...nodes); }
    replaceChildren(...nodes) { for (const node of this.children) node.disconnect(); this.children = []; this.append(...nodes); }
    disconnect() { this.isConnected = false; for (const child of this.children) child.disconnect(); }
    setAttribute(key, value) { this.attributes.set(key, String(value)); }
    addEventListener(type, listener) { const entries = this.handlers.get(type) || []; entries.push(listener); this.handlers.set(type, entries); }
    focus() { calls.focus++; }
    click() { this.onClick?.(); }
  }
  const node = (tag, className, text) => new NodePort(tag, className, text);
  const notes = { entity: { kind: 'builtin', id: 'notes' }, title: 'Записки', source: 'builtin' };
  const apps = Array.from({ length: 8 }, (_, index) => ({ entity: { kind: 'app', id: 'app-' + String(index + 1).padStart(32, '0') }, title: 'Проект ' + (index + 1), source: 'owner' }));
  const document = { schema: 'soty.field.v1', contexts: [
    { contextId: 'personal', title: 'Личное', x: 0, y: 0 }, { contextId: 'other', title: 'Позже', x: 5000, y: 4000 },
  ], shortcuts: [
    ...apps.map((item, index) => ({ shortcutId: 'app' + index, entity: item.entity, contextId: 'personal', slot: [index * 3, 0] })),
    { shortcutId: 'note', entity: notes.entity, contextId: 'other', slot: [0, 0] },
  ] };
  const metadata = new Map([...apps, notes].map(item => [fieldEntityKey(item.entity), item]));
  const records = new Map(apps.map(item => [fieldEntityKey(item.entity), { id: item.entity.id, access: 'owner' }]));
  const searchInput = Object.assign(node('input'), { value: '' });
  const before = JSON.stringify(document);
  let live = true;
  const environment = {
    exports: {}, mode: 'mine', queries: { mine: '', search: '' }, filter: 'all', composing: false,
    mineGeneration: 1, mineResolvedGeneration: 1, mineConfirmedGeneration: 1, mineKnownRefs: new Set(metadata.keys()), mineFilterAdmitted: true,
    metadata, metadataRevision: 1, emptyDocument: null, emptyScope: '', emptyMetadataRevision: -1,
    element: { dataset: { mineState: 'ready', query: 'empty' } }, empty: node(), searchInput, clear: { hidden: true },
    search: node('label'), listButton: node('button'), sides: Object.assign(node('div'), { classList: { add() {} } }), signal: new AbortController().signal,
    searchTimer: null, searchGeneration: 0, searchCursor: null, more: { hidden: true }, searchLimit: { hidden: true },
    summary: { document, arranging: false }, fieldEntityKey, current: () => live,
    directory: { getRecord: ref => records.get(fieldEntityKey(ref)) || null }, allMetadata: () => [...metadata.values()],
    el: node, icon: () => node('svg'), button: (text, symbol, className, onClick) => Object.assign(node('button', className, text), { onClick }),
    options: { onOpen: (item, record) => calls.open.push({ item, record }) },
    openPreview: (item, shortcutId) => calls.preview.push({ item, shortcutId }), closePreview() {},
    closeList() {}, openList() { calls.list++; }, closeSearch: { click() { calls.close++; } },
    route() { calls.route.push(environment.queries.mine); },
    run() { throw new Error('Mine search must not issue a directory search'); },
    searchDirectory() { throw new Error('No search request'); },
    setTimeout() { throw new Error('Mine search needs no timer'); }, clearTimeout() {},
  };
  const engine = {
    snapshot: () => document, getCamera: () => ({ x: 0, y: 0, scale: 1 }),
    update() {},
    focusContext: id => calls.context.push(id),
    setCamera() { calls.camera++; }, fitOverview() { calls.camera++; },
    commit() { calls.writes++; }, hasUnsavedChanges: () => false,
  };
  environment.engine = engine;
  vm.createContext(environment); new vm.Script(code).runInContext(environment);
  const api = environment.exports;
  const text = n => n.textContent + n.children.map(text).join('');
  const buttons = () => api.list.children.filter(n => n.tag === 'button');
  const emit = (type, props = {}) => {
    const event = { isComposing: false, key: '', stopped: 0, ...props, stopPropagation() { this.stopped++; } };
    for (const listener of searchInput.handlers.get(type) || []) listener(event);
    return event;
  };
  const enter = value => { searchInput.value = value; emit('input'); };
  return { api, environment, calls, notes, apps, document, metadata, records, before, buttons, text, emit, enter, searchInput,
    retire() { live = false; }, assertUnchanged() { assert.equal(JSON.stringify(document), before); assert.equal(calls.writes, 0); assert.equal(calls.camera, 0); } };
}

test('actual input exposes offscreen Notes from another context and opens it directly without camera or layout changes', () => {
  const h = harness(), layout = geometry.layoutUnifiedField(h.document, [...h.metadata.values()]);
  const visible = geometry.createFieldSpatialIndex(layout.nodes).query({ left: -195, right: 195, top: -300, bottom: 300 }, 100);
  assert.equal(visible.some(item => item.shortcutId === 'note'), false, 'The actual spatial index has no Notes node in this viewport');
  h.enter('Записки');
  assert.equal(h.api.surface.hidden, false);
  assert.equal(h.api.surface.attributes.get('role'), 'group');
  const target = h.buttons().find(button => h.text(button).includes('Записки'));
  assert.ok(target, 'Search must expose a real result button independently of canvas culling');
  assert.equal(h.calls.focus, 0); assert.equal(h.calls.open.length, 0);
  target.click(); assert.equal(h.calls.open.length, 1); assert.equal(h.calls.open[0].item.entity.id, 'notes');
  assert.equal(h.calls.open[0].record, null); h.assertUnchanged();
});

test('duplicate shortcuts become one confirmed result, and only a matching context becomes a context result', () => {
  const h = harness();
  h.document.shortcuts.push({ shortcutId: 'note-copy', entity: h.notes.entity, contextId: 'personal', slot: [100, 100] });
  h.enter('записки'); assert.equal(h.buttons().length, 1);
  h.enter('Позже'); assert.ok(h.buttons().some(button => h.text(button).includes('Записки')));
  h.buttons().find(button => h.text(button).startsWith('Позже')).click(); assert.deepEqual(h.calls.context, ['other']);
  assert.equal(h.calls.open.length, 0);
});

test('filter and actual directory record gate every private row and are rechecked on activation', () => {
  const h = harness(); h.enter('Проект 1');
  const target = h.buttons()[0]; assert.ok(target);
  h.records.delete(fieldEntityKey(h.apps[0].entity)); target.click();
  assert.equal(h.calls.open.length, 0); assert.equal(h.buttons().some(button => h.text(button).includes('Проект 1')), false);
  h.environment.filter = 'person'; h.enter('Записки'); assert.equal(h.buttons().length, 0);
});

test('pending generation and source failure never claim an empty completed search', () => {
  for (const state of ['loading', 'unknown', 'error']) {
    const h = harness(); h.environment.mineConfirmedGeneration = 0; h.environment.element.dataset.mineState = state;
    h.enter('Нет такого имени'); assert.equal(h.api.status.textContent, 'Доступ к сотам ещё не подтверждён.');
    assert.equal(h.buttons().length, 0);
  }
  const h = harness(); h.enter('Нет такого имени'); assert.equal(h.api.status.textContent, 'Ничего не найдено.');
});

test('captured result cannot activate after query/filter or screen retirement, including A to B to A query reuse', () => {
  const h = harness(); h.enter('Записки'); const target = h.buttons()[0];
  h.enter('Проект'); h.enter('Записки'); target.click(); assert.equal(h.calls.open.length, 0);
  const current = h.buttons()[0]; h.environment.filter = 'person'; current.click(); assert.equal(h.calls.open.length, 0);
  h.environment.filter = 'all'; h.retire(); current.click(); assert.equal(h.calls.open.length, 0);
});

test('clearing, global-search mode and active composition hide results; existing Escape semantics and focus remain', () => {
  const h = harness(); h.enter('Записки'); h.emit('keydown', { key: 'Escape' });
  assert.equal(h.searchInput.value, ''); assert.equal(h.api.surface.hidden, true); assert.equal(h.calls.open.length, 0);
  h.emit('keydown', { key: 'Escape' }); assert.equal(h.calls.close, 1);
  h.enter('Записки'); h.emit('compositionstart'); h.emit('input');
  assert.equal(h.api.surface.hidden, true); h.emit('keydown', { key: 'Escape', isComposing: true });
  assert.equal(h.searchInput.value, 'Записки'); h.emit('compositionend'); assert.equal(h.api.surface.hidden, false);
  h.environment.mode = 'search'; h.api.render(); assert.equal(h.api.surface.hidden, true);
  assert.equal(h.calls.focus, 0); h.assertUnchanged();
});

test('at most six direct rows plus the existing full-list action stay visible and unchanged data preserves button focus', () => {
  const h = harness(); h.enter('Проект');
  assert.equal(h.buttons().length, 7); assert.equal(h.api.status.textContent, 'Найдено: 8');
  const first = h.buttons()[0]; h.api.render(); assert.equal(h.buttons()[0], first, 'No DOM replacement during unchanged camera summaries');
  assert.equal(h.text(h.buttons().at(-1)), 'Открыть списком');
  h.buttons().at(-1).click(); assert.equal(h.calls.list, 1); assert.equal(h.calls.open.length, 0); h.assertUnchanged();
});

test('people and communities keep the existing preview path instead of acquiring direct app opening', () => {
  for (const kind of ['person', 'community']) {
    const h = harness(), item = { entity: { kind, id: 'synthetic_' + kind }, title: 'Цель', source: 'member' };
    h.metadata.set(fieldEntityKey(item.entity), item); h.records.set(fieldEntityKey(item.entity), { available: true });
    h.environment.mineKnownRefs.add(fieldEntityKey(item.entity));
    h.document.shortcuts.push({ shortcutId: 'target', entity: item.entity, contextId: 'other', slot: [100, 100] });
    h.enter('Цель'); h.buttons()[0].click(); assert.equal(h.calls.preview.length, 1); assert.equal(h.calls.open.length, 0);
  }
});
