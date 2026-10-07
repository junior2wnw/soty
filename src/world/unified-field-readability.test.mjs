import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import vm from 'node:vm';
import ts from 'typescript';
import { createFieldSpatialIndex, layoutUnifiedField, UNIFIED_FIELD_METRICS, fieldBoundsOverlap } from './unified-field-layout.mjs';
import { fitFieldCamera, fieldScreenToWorld, fieldWorldToScreen, fieldLevelOfDetail } from './unified-field-camera.mjs';

// Actual production geometry, rendering and event callbacks, with inert DOM ports.
// These tests do not measure browser typography or claim a real application launch.
const source = readFileSync(new URL('./unified-field.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const production = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedField');
const statement = name => production.body.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name);
const titles = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'compactOverviewTitles');
assert.ok(titles && statement('render') && statement('renderNode'));
const compile = value => ts.transpileModule(value, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const compiled = compile([print(titles), print(statement('renderNode')), print(statement('render')),
  'exports.titles = compactOverviewTitles; exports.render = render;'].join('\n'));
const callback = (owner, event) => {
  let result;
  function visit(node) {
    if (ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression)
      && node.expression.expression.getText(parsed) === owner && node.expression.name.text === 'addEventListener'
      && ts.isStringLiteral(node.arguments[0]) && node.arguments[0].text === event) result = node.arguments[1];
    ts.forEachChild(node, visit);
  }
  visit(production); assert.ok(result, `${owner} ${event} callback exists`);
  return compile('exports.handler = ' + print(result) + ';');
};

class Port {
  children = []; dataset = {}; attributes = new Map(); listeners = new Map(); parent = null;
  style = { setProperty(name, value) { this[name] = value; } };
  constructor(tag = 'div', className = '', text = '') { this.tag = tag; this.className = className; this.textContent = text; }
  get classList() { return { add: (...names) => { this.className += ' ' + names.join(' '); },
    toggle: (name, enabled) => { const names = new Set(this.className.split(/\s+/)); enabled ? names.add(name) : names.delete(name); this.className = [...names].join(' '); },
    remove: name => { this.className = this.className.split(/\s+/).filter(value => value !== name).join(' '); } }; }
  append(...children) { for (const child of children) { child.parent = this; this.children.push(child); } }
  replaceChildren(...children) { this.children = []; this.append(...children); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter(value => value !== this); }
  setAttribute(name, value) { this.attributes.set(name, value); }
  getAttribute(name) { return this.attributes.get(name); }
  addEventListener(name, handler) { this.listeners.set(name, handler); }
  querySelector(selector) {
    for (const child of this.children) {
      if (selector.startsWith('.') ? child.className.split(/\s+/).includes(selector.slice(1)) : child.tag === selector) return child;
      const nested = child.querySelector(selector); if (nested) return nested;
    }
    return null;
  }
  dispatch(type) { const event = { currentTarget: this, prevented: false, preventDefault() { this.prevented = true; } }; this.listeners.get(type)?.(event); return event; }
}
const app = n => ({ kind: 'app', id: 'app-' + String(n).repeat(32) });
function specimen() {
  const document = { schema: 'soty.field.v1', contexts: [
    { contextId: 'studio', title: 'Студия', x: 0, y: 0 },
    { contextId: 'close', title: 'Близкие', x: 640, y: -80 },
    { contextId: 'evening', title: 'После работы', x: 520, y: 340 },
  ], shortcuts: [] };
  const entities = [];
  const add = (id, entity, title, contextId, slot) => {
    document.shortcuts.push({ shortcutId: id, entity, contextId, slot }); entities.push({ entity, title, source: 'owner' });
  };
  add('hive', app(1), 'HIVE', 'studio', [0, 0]);
  add('canvas', app(2), 'Canvas', 'studio', [-25, 19]);
  add('pulse', app(3), 'Pulse', 'studio', [6, 19]);
  add('anna', { kind: 'person', id: 'anna' }, 'Аня', 'studio', [-24, -3]);
  add('tim', { kind: 'person', id: 'tim' }, 'Тим', 'studio', [29, 5]);
  add('notes', { kind: 'builtin', id: 'notes' }, 'Записки', 'close', [0, 0]);
  add('mira', { kind: 'person', id: 'mira' }, 'Мира', 'close', [-16, -1]);
  add('sasha', { kind: 'person', id: 'sasha' }, 'Саша', 'close', [16, 0]);
  add('music', app(4), 'Тавыш', 'evening', [0, 0]);
  add('chess', { kind: 'builtin', id: 'chess' }, 'Шахматы', 'evening', [19, 0]);
  add('kirill', { kind: 'person', id: 'kirill' }, 'Кирилл', 'evening', [35, -1]);
  return { document, entities, layout: layoutUnifiedField(document, entities) };
}
function harness(width, height, { scale, focus = '' } = {}) {
  const data = specimen(), view = { width, height }, camera = fitFieldCamera(data.layout.bounds, view, { padding: 60 });
  if (scale !== undefined) camera.scale = scale;
  const calls = { activate: [], inspect: [], focus: [], remember: 0 }, plane = new Port(), viewport = new Port();
  const environment = { exports: {}, destroyed: false, inPointerFrame: false, activeLayout: () => data.layout,
    viewportSize: () => view, camera: () => camera, cameraFit: { mine: focus ? 'context' : 'overview' },
    contextFocus: { mine: focus }, mode: 'mine', arranging: false, moving: null, contextPreview: null,
    viewport, plane, signal: new AbortController().signal, document: { createElementNS: (_, tag) => new Port(tag) }, window: { innerWidth: width },
    metrics: UNIFIED_FIELD_METRICS, SVG_NS: 'svg', fieldBoundsOverlap, fieldScreenToWorld, fieldLevelOfDetail,
    createFieldSpatialIndex, index: createFieldSpatialIndex(data.layout.nodes),
    nodeElements: new Map(), contextElements: new Map(), inspectElements: new Map(), selectedId: '', persistence: 'saved',
    options: { resolveArt: () => null, onActivate: (...args) => calls.activate.push(args), onInspect: (...args) => calls.inspect.push(args) },
    available: id => data.layout.nodes.find(node => node.shortcutId === id),
    focusContext: id => calls.focus.push(id), remember: () => calls.remember++, schedule() {}, emit() {},
    el: (tag, className = '', text = '') => new Port(tag, className, text),
    icon: (_, className = 'sw-icon') => new Port('svg', className), hexFrame: () => new Port('svg', 'uf-hex-frame'),
    initials: title => title.slice(0, 2), localImage: () => null, safeImageUrl: () => null,
    nounCount: (count, word) => `${count} ${word}`, Date,
    tools: new Port(), moveHint: new Port(), readySettled: true,
  };
  vm.createContext(environment); new vm.Script(compiled).runInContext(environment);
  return { data, view, camera, calls, environment, render: environment.exports.render, titles: environment.exports.titles };
}
const rect = box => ({ left: box.x - box.width / 2, right: box.x + box.width / 2, top: box.y - 22, bottom: box.y + 22 });
const overlaps = (a, b) => a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top;

test('320 and 393 overview headers stay readable, in bounds and outside other headers and node geometry without moving data or camera', () => {
  for (const [width, height] of [[320, 338], [393, 458]]) {
    const h = harness(width, height), before = JSON.stringify([h.data.document, h.camera]);
    const boxes = h.titles(h.data.layout.contexts, h.data.layout.nodes, h.camera, h.view, 28);
    assert.equal(boxes.size, 3);
    for (const box of boxes.values()) {
      const actual = rect(box); assert.ok(box.width >= 112 && box.width <= 160);
      assert.ok(actual.left >= 8 && actual.right <= width - 8 && actual.top >= 8 && actual.bottom <= height - 8);
      for (const other of boxes.values()) if (other !== box) assert.equal(overlaps(actual, rect(other)), false);
      for (const node of h.data.layout.nodes) {
        const a = fieldWorldToScreen({ x: node.bounds.left, y: node.bounds.top }, h.camera, h.view);
        const b = fieldWorldToScreen({ x: node.bounds.right, y: node.bounds.bottom }, h.camera, h.view);
        assert.equal(overlaps(actual, { left: a.x, top: a.y, right: b.x, bottom: b.y }), false);
      }
    }
    assert.equal(JSON.stringify([h.data.document, h.camera]), before);
  }
});

test('derived headers remain finite and bounded for 24 coincident or million-coordinate spaces', () => {
  const h = harness(320, 338);
  for (const far of [false, true]) {
    const document = { schema: 'soty.field.v1', contexts: Array.from({ length: 24 }, (_, n) => ({ contextId: `space-${n}`, title: `Пространство ${n}`, x: far ? n % 2 * 1e6 : 0, y: far ? Math.floor(n / 2) * 80000 : 0 })), shortcuts: [] };
    const layout = layoutUnifiedField(document, []), camera = fitFieldCamera(layout.bounds, h.view, { padding: 60 });
    const before = JSON.stringify(document), boxes = h.titles(layout.contexts, [], camera, h.view, 28);
    assert.equal(boxes.size, 24);
    for (const value of boxes.values()) { assert.ok(Object.values(value).every(Number.isFinite)); const box = rect(value); assert.ok(box.left >= 8 && box.right <= 312 && box.top >= 8 && box.bottom <= 330); }
    assert.equal(JSON.stringify(document), before);
  }
});

test('actual compact overview render keeps each full space name on a 44px header and routes tiny app previews into their existing space', () => {
  const h = harness(320, 338); h.render();
  const { environment: e, calls } = h;
  assert.equal(e.viewport.dataset.compactOverview, 'true');
  for (const [id, wrapper] of e.contextElements) {
    const title = wrapper.querySelector('.uf-context-title');
    assert.equal(title.style.height, '44px'); assert.ok(parseFloat(title.style['--uf-title-screen-width']) >= 112 && parseFloat(title.style['--uf-title-screen-width']) <= 160);
    assert.equal(title.className.includes('is-icon-only'), false);
    assert.equal(title.querySelector('.uf-context-name').textContent, h.data.document.contexts.find(value => value.contextId === id).title);
    assert.equal(title.tabIndex, 0);
  }
  const hive = e.nodeElements.get('hive'); assert.equal(hive.dataset.preview, 'true'); assert.equal(hive.tabIndex, -1);
  assert.equal(e.inspectElements.has('hive'), false); assert.equal(hive.getAttribute('aria-label'), 'Открыть пространство Студия');
  hive.dispatch('click'); assert.deepEqual(calls.focus, ['studio']); assert.equal(calls.activate.length, 0);
  assert.equal(hive.dispatch('contextmenu').prevented, true); assert.deepEqual(calls.focus, ['studio', 'studio']); assert.equal(calls.inspect.length, 0);
});

test('a focused phone space and a readable desktop overview retain ordinary app opening and inspection', () => {
  for (const h of [harness(393, 458, { scale: .45, focus: 'studio' }), harness(1280, 650, { scale: .58 })]) {
    h.render(); const e = h.environment, hive = e.nodeElements.get('hive');
    assert.equal(e.viewport.dataset.compactOverview, 'false'); assert.equal(hive.dataset.preview, 'false');
    assert.equal(hive.getAttribute('aria-label'), 'HIVE'); assert.equal(e.inspectElements.has('hive'), true);
    hive.dispatch('click'); assert.equal(h.calls.activate.length, 1); assert.equal(h.calls.focus.length, 0);
    hive.dispatch('contextmenu'); assert.equal(h.calls.inspect.length, 1);
  }
});

test('keyboard context-menu shortcut cannot inspect a sub44 preview and returns to its space', () => {
  const h = harness(320, 338); h.render(); h.environment.selectedId = 'hive';
  new vm.Script(callback('plane', 'keydown')).runInContext(h.environment);
  let prevented = false;
  h.environment.exports.handler({ target: { closest: () => null }, shiftKey: true, key: 'F10', preventDefault() { prevented = true; } });
  assert.equal(prevented, true); assert.deepEqual(h.calls.focus, ['studio']); assert.equal(h.calls.inspect.length, 0);
});

test('a long press cannot start placement on a micro preview but remains available in a focused space', () => {
  for (const preview of [true, false]) {
    const h = preview ? harness(320, 338) : harness(393, 458, { scale: .45, focus: 'studio' }); h.render();
    const e = h.environment, node = e.nodeElements.get('hive'); let timers = 0;
    Object.assign(e, { pointers: new Map(), pinch: null, gesture: null, suppressClickUntil: 0,
      localPoint: () => ({ x: 100, y: 100 }), setTimeout() { timers++; return 1; } });
    e.options.commit = async () => {};
    new vm.Script(callback('viewport', 'pointerdown')).runInContext(e);
    e.exports.handler({ button: 0, pointerId: 1, target: { closest: selector => selector === '[data-shortcut-id]' ? node : null } });
    assert.equal(timers, preview ? 0 : 1); assert.equal(e.arranging, false);
  }
});

test('actual render restricts title occupancy to retained overview previews rather than all distant nodes', () => {
  const h = harness(320, 338), e = h.environment;
  h.data.document.contexts.push({ contextId: 'remote', title: 'Далёкое пространство', x: 1e6, y: 1e6 });
  for (let n = 0; n < 230; n++) {
    const entity = { kind: 'app', id: 'app-' + (n + 20).toString(16).padStart(32, '0') };
    h.data.document.shortcuts.push({ shortcutId: `remote-${n}`, entity, contextId: 'remote', slot: [n % 16 * 19, Math.floor(n / 16) * 19] });
    h.data.entities.push({ entity, title: `Remote ${n}`, source: 'owner' });
  }
  h.data.layout = layoutUnifiedField(h.data.document, h.data.entities); e.index = createFieldSpatialIndex(h.data.layout.nodes);
  const actual = e.compactOverviewTitles; let occupancy;
  e.compactOverviewTitles = (contexts, nodes, ...args) => { occupancy = nodes; return actual(contexts, nodes, ...args); };
  h.render(); assert.ok(h.data.layout.nodes.length > 230);
  assert.ok(occupancy.length <= 10); assert.equal(occupancy.some(node => node.contextId === 'remote'), false);
});

test('overview titles stay associated with their own space instead of being closer to a neighbouring contour', () => {
  const distance = (point, box) => Math.hypot(Math.max(box.left - point.x, 0, point.x - box.right), Math.max(box.top - point.y, 0, point.y - box.bottom));
  for (const [width, height] of [[320, 338], [393, 458]]) {
    const h = harness(width, height); h.render();
    const bodies = new Map(h.data.layout.contexts.map(context => {
      const a = fieldWorldToScreen({ x: context.body.left, y: context.body.top }, h.camera, h.view);
      const b = fieldWorldToScreen({ x: context.body.right, y: context.body.bottom }, h.camera, h.view);
      return [context.contextId, { left: a.x, top: a.y, right: b.x, bottom: b.y }];
    }));
    for (const [id, wrapper] of h.environment.contextElements) {
      const title = wrapper.querySelector('.uf-context-title');
      const centre = fieldWorldToScreen({ x: parseFloat(wrapper.style.left) + parseFloat(title.style.left), y: parseFloat(wrapper.style.top) + parseFloat(title.style.top) }, h.camera, h.view);
      const own = distance(centre, bodies.get(id));
      for (const [other, box] of bodies) if (other !== id) assert.ok(own <= distance(centre, box) + .01, `${width}px ${id} label must remain closest to its own space, compared with ${other}`);
    }
  }
});

test('visible portrait categories use the same 44px select in Mine and Search, while default Mine and desktop stay unchanged', () => {
  // Parse the actual CSS through Vite's existing PostCSS dependency. These
  // cascade ports cover the filter selectors/size media used by this stylesheet;
  // actual browser layout and native select behavior are a separate gate.
  const require = createRequire(import.meta.url);
  const css = createRequire(require.resolve('vite/package.json'))('postcss').parse(readFileSync(new URL('./unified-field.css', import.meta.url), 'utf8'));
  const rules = [];
  css.walkRules(rule => { if (/uf-(filters|compact-filter)/.test(rule.selector)) rules.push(rule); });
  function cascade(target, state) {
    const result = { display: target === 'row' ? 'block' : 'inline-block' }, ranks = new Map();
    const attributes = { 'data-mode': state.mode, 'data-filter': state.filter, 'data-query': state.query ? 'active' : 'empty' };
    const media = params => {
      const constraints = [...params.matchAll(/\((min|max)-(width|height):\s*(\d+)px\)/g)];
      return constraints.length > 0 && constraints.every(([, bound, dimension, value]) => bound === 'min' ? state[dimension] >= Number(value) : state[dimension] <= Number(value));
    };
    rules.forEach((rule, order) => {
      for (let parent = rule.parent; parent; parent = parent.parent) if (parent.type === 'atrule' && parent.name === 'media' && !media(parent.params)) return;
      for (const selector of rule.selectors) {
        if (selector.includes('::')) continue;
        const terminal = target === 'button' ? /(?:^|[ >])button$/ : target === 'select' ? /\.uf-compact-filter$/ : /\.uf-filters$/;
        if (!terminal.test(selector)) continue;
        const attrs = [...selector.matchAll(/\[([a-z-]+)(?:=([a-z]+))?\]/g)];
        if (attrs.some(([, name, value]) => value === undefined ? attributes[name] === undefined : attributes[name] !== value)) continue;
        // Screen always sets data-query; for its :is(empty,not(query)) rule
        // only the empty branch can match this production state shape.
        const specificity = (selector.match(/\.[\w-]+/g)?.length ?? 0) + attrs.length;
        for (const node of rule.nodes) if (node.type === 'decl' && ['display', 'width', 'min-width', 'min-height'].includes(node.prop)) {
          const rank = [node.important ? 1 : 0, specificity, order], prior = ranks.get(node.prop);
          if (!prior || rank[0] > prior[0] || rank[0] === prior[0] && (rank[1] > prior[1] || rank[1] === prior[1] && rank[2] >= prior[2])) {
            ranks.set(node.prop, rank); result[node.prop] = node.value;
          }
        }
      }
    });
    return result;
  }
  for (const [width, height] of [[320, 740], [393, 852]]) for (const mode of ['mine', 'search']) {
    const state = { width, height, mode, filter: 'all', query: 'HIVE' };
    assert.notEqual(cascade('row', state).display, 'none');
    const select = cascade('select', state);
    assert.equal(select.display, 'block', `${width}px ${mode} category select must be visible`);
    assert.equal(select.width, '100%'); assert.equal(Number.parseFloat(select['min-height']), 44);
    assert.equal(Number.parseFloat(select['min-width']), 0); assert.equal(cascade('button', state).display, 'none');
  }
  const filteredMine = { width: 393, height: 852, mode: 'mine', filter: 'device', query: '' };
  assert.notEqual(cascade('row', filteredMine).display, 'none'); assert.equal(cascade('select', filteredMine).display, 'block');
  assert.equal(cascade('row', { ...filteredMine, filter: 'all' }).display, 'none');
  const desktop = { width: 1280, height: 800, mode: 'mine', filter: 'all', query: 'HIVE' };
  assert.equal(cascade('select', desktop).display, 'none'); assert.notEqual(cascade('button', desktop).display, 'none');
  const landscape = { ...desktop, width: 844, height: 390 };
  assert.equal(cascade('select', landscape).display, 'block'); assert.equal(cascade('button', landscape).display, 'none');
});
