import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import vm from 'node:vm';
import ts from 'typescript';
import { createFieldSpatialIndex, layoutUnifiedField, UNIFIED_FIELD_METRICS, fieldBounds, fieldBoundsOverlap } from './unified-field-layout.mjs';
import { fitFieldCamera, fieldScreenToWorld, fieldWorldToScreen, fieldLevelOfDetail, normalizeFieldCamera } from './unified-field-camera.mjs';
import { resolveAppArt } from './app-art.mjs';
import { fieldEntityKey } from '../../modules/field/contract.mjs';

// Actual production geometry, rendering and event callbacks, with inert DOM ports.
// These tests do not measure browser typography or claim a real application launch.
const source = readFileSync(process.env.SOTY_REVIEW_FIELD_SOURCE || new URL('./unified-field.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const production = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedField');
const statement = name => production.body.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name);
const titles = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'compactOverviewTitles');
assert.ok(titles && statement('render') && statement('renderNode'));
const compile = value => ts.transpileModule(value, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const compiled = compile([[titles, statement('renderNode'), statement('render'), statement('focusPreview'), statement('inspectShortcut')].filter(Boolean).map(print).join('\n'),
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
  focus() { this.focused = true; }
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
  const calls = { activate: [], inspect: [], focus: [], nodeFocus: [], remember: 0 }, plane = new Port(), viewport = new Port();
  const environment = { exports: {}, destroyed: false, inPointerFrame: false, activeLayout: () => data.layout,
    gesture: null, pinch: null, lastInspection: null, suppressClickUntil: 0, performance, clearTimeout,
    viewportSize: () => view, camera: () => camera, cameraFit: { mine: focus ? 'context' : 'overview' },
    contextFocus: { mine: focus }, mode: 'mine', arranging: false, moving: null, contextPreview: null,
    viewport, plane, signal: new AbortController().signal, document: { createElementNS: (_, tag) => new Port(tag) }, window: { innerWidth: width },
    metrics: UNIFIED_FIELD_METRICS, SVG_NS: 'svg', fieldEntityKey, fieldBoundsOverlap, fieldScreenToWorld, fieldLevelOfDetail,
    createFieldSpatialIndex, index: createFieldSpatialIndex(data.layout.nodes),
    nodeElements: new Map(), contextElements: new Map(), inspectElements: new Map(), selectedId: '', persistence: 'saved',
    options: { resolveArt: () => null, onActivate: (...args) => calls.activate.push(args), onInspect: (...args) => calls.inspect.push(args) },
    available: id => data.layout.nodes.find(node => node.shortcutId === id),
    focusContext: id => calls.focus.push(id), focusNode: id => calls.nodeFocus.push(id), remember: () => calls.remember++, schedule() {}, emit() {},
    el: (tag, className = '', text = '') => new Port(tag, className, text),
    icon: (_, className = 'sw-icon') => new Port('svg', className), hexFrame: () => new Port('svg', 'uf-hex-frame'),
    initials: title => title.slice(0, 2), localImage: () => null, safeImageUrl: () => null, resolveAppArt,
    nounCount: (count, word) => `${count} ${word}`, Date,
    tools: new Port(), moveHint: new Port(), readySettled: true,
  };
  vm.createContext(environment); new vm.Script(compiled).runInContext(environment);
  return { data, view, camera, calls, environment, render: environment.exports.render, titles: environment.exports.titles };
}

const cameraFlow = compile([statement('fitOverview'), statement('focusContext')].map(print).join('\n')
  + '\nexports.fitOverview = fitOverview; exports.focusContext = focusContext;');
function twelveApps(mode) {
  const document = { schema: 'soty.field.v1', contexts: [{ contextId: 'personal', title: 'Личное', x: 0, y: 0 }], shortcuts: [] }, entities = [];
  for (let n = 0; n < 12; n++) {
    const q = n % 4 * 40, r = Math.floor(n / 4) * 52 - q / 2;
    const entity = { kind: 'app', id: 'app-' + String(n).padStart(32, '0') };
    document.shortcuts.push({ shortcutId: 'app-' + n, entity, contextId: 'personal', slot: [q, r] });
    entities.push({ entity, title: 'Приложение ' + n });
  }
  const h = harness(1280, 650), data = { document, entities, layout: layoutUnifiedField(document, entities) }, e = h.environment;
  Object.assign(e, { mode, layout: data.layout, activeLayout: () => data.layout, index: createFieldSpatialIndex(data.layout.nodes),
    available: id => data.layout.nodes.find(node => node.shortcutId === id), fitFieldCamera, fieldBounds });
  e.contextFocus[mode] = ''; e.cameraFit[mode] = 'overview';
  // The camera sink is a DOM port; both actual fit functions and camera math run.
  e.setCamera = (next, fit = 'manual') => { Object.assign(h.camera, normalizeFieldCamera(next)); e.cameraFit[e.mode] = fit; };
  new vm.Script(cameraFlow).runInContext(e);
  return { ...h, data, fitOverview: e.exports.fitOverview, focusContext: e.exports.focusContext };
}

test('actual desktop overview then explicit context focus reveals all twelve saved apps at the same 45 percent without moving data', () => {
  for (const mode of ['mine', 'search']) {
    const h = twelveApps(mode), e = h.environment;
    const positions = () => JSON.stringify([h.data.document, h.data.layout.nodes.map(({ shortcutId, slot, cx, cy }) => ({ shortcutId, slot, cx, cy }))]);
    const before = positions();
    h.fitOverview(); h.render();
    assert.ok(h.camera.scale < .4); assert.equal(e.viewport.dataset.lod, 'overview'); assert.equal(e.nodeElements.size, 3);
    h.focusContext('personal'); h.render();
    assert.equal(h.camera.scale, .45); assert.equal(e.contextFocus[mode], 'personal'); assert.equal(e.cameraFit[mode], 'context');
    assert.equal(e.viewport.dataset.lod, 'context'); assert.equal(e.nodeElements.size, 12);
    assert.equal(positions(), before);
    h.fitOverview(); h.render();
    assert.equal(e.contextFocus[mode], ''); assert.equal(e.viewport.dataset.lod, 'overview'); assert.equal(e.nodeElements.size, 3);
    e.setCamera({ ...h.camera, scale: .45 }); h.render();
    assert.equal(e.viewport.dataset.lod, 'overview', 'ordinary unfocused hysteresis is unchanged'); assert.equal(e.nodeElements.size, 3);
    assert.equal(positions(), before);
  }
});

test('focused desktop high zoom retains natural detail while a focused phone retains context', () => {
  for (const scale of [.9, 1, 1.4]) {
    const h = twelveApps('mine'), before = JSON.stringify(h.data.document);
    h.focusContext('personal'); h.environment.setCamera({ ...h.camera, scale }); h.render();
    assert.equal(h.environment.contextFocus.mine, 'personal');
    assert.equal(h.environment.viewport.dataset.lod, 'detail');
    assert.equal(JSON.stringify(h.data.document), before);
  }
  const phone = harness(393, 458, { scale: 1, focus: 'studio' }); phone.render();
  assert.equal(phone.environment.viewport.dataset.lod, 'context');
});

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

test('a saved 21 percent focused phone context keeps its camera but treats sub44 cells as previews without inspect actions', () => {
  const h = harness(320, 180, { scale: .21, focus: 'studio' }), before = JSON.stringify([h.camera, h.data.document]);
  h.render(); const e = h.environment, hive = e.nodeElements.get('hive');
  assert.equal(e.viewport.dataset.lod, 'context', 'the saved focus can still own its context');
  assert.equal(hive.dataset.preview, 'true'); assert.equal(hive.getAttribute('aria-label'), 'Приблизить: HIVE');
  assert.equal(e.inspectElements.has('hive'), false); assert.equal(JSON.stringify([h.camera, h.data.document]), before);
  hive.dispatch('click'); assert.deepEqual(h.calls.nodeFocus, ['hive']); assert.equal(h.calls.activate.length, 0); assert.equal(h.calls.focus.length, 0);
  hive.dispatch('contextmenu'); assert.deepEqual(h.calls.nodeFocus, ['hive', 'hive']); assert.equal(h.calls.inspect.length, 0);
});

test('explicitly enlarging a thumbnail uses actual focusNode math and permits the next tap to open without moving any placement', () => {
  const h = harness(320, 180, { scale: .21, focus: 'studio' }), e = h.environment, before = JSON.stringify(h.data.document);
  const emitted = []; e.options.onStateChange = next => emitted.push(next);
  Object.assign(e, { fieldDocument: h.data.document, exposedSource: h.data.document, exposedDocument: h.data.document,
    localDurable: true, revision: 0, history: { entries: [] }, message: '' });
  e.cameras = { mine: { ...h.camera } }; e.camera = () => e.cameras.mine; e.activeNode = e.available; e.normalizeFieldCamera = normalizeFieldCamera;
  new vm.Script(compile(['focusNode', 'summary', 'emit'].map(name => print(statement(name))).join('\n') + '\nexports.focusNode = focusNode;')).runInContext(e);
  e.focusNode = e.exports.focusNode; h.render();
  const hive = e.nodeElements.get('hive'); hive.dispatch('click');
  const node = e.available('hive'); assert.ok(node.width * e.cameras.mine.scale >= 44); assert.ok(node.height * e.cameras.mine.scale >= 44);
  assert.ok(e.cameras.mine.scale >= .45); assert.equal(hive.focused, true); assert.equal(hive.dataset.preview, 'false');
  assert.equal(emitted.length, 1, 'the actual onStateChange callback must receive the camera change even when the node was already focused');
  assert.equal(emitted[0].camera.scale, e.cameras.mine.scale);
  assert.equal(`${Math.round(emitted[0].camera.scale * 100)}%`, `${Math.round(e.cameras.mine.scale * 100)}%`, 'the summary feeding the zoom badge must describe the rendered scale');
  assert.equal(JSON.stringify(h.data.document), before); assert.equal(h.calls.activate.length, 0); assert.equal(e.contextFocus.mine, 'studio');
  hive.dispatch('click'); assert.equal(h.calls.activate.length, 1);
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


// Additional component checks reuse the established DOM port and execute actual
// AST methods/callbacks. The ports below hold data and dispatch events; they do
// not copy camera, fit, rebuild, matching, keyboard or resize decisions.
const { nearestFieldNode: actualNearestFieldNode } = await import('./unified-field-layout.mjs');
const mobileFlowNames = ['setCamera', 'fitOverview', 'focusContext', 'focusNode', 'available', 'activeNode',
  'matchesMineFilter', 'rebuild', 'remember', 'summary', 'emit'];
for (const name of mobileFlowNames) assert.ok(statement(name), `actual field method ${name}`);
const mobileFlowCode = compile(mobileFlowNames.map(name => print(statement(name))).join('\n')
  + '\n' + mobileFlowNames.map(name => `exports.${name} = ${name};`).join('\n'));
const resizeDeclaration = production.body.statements.filter(ts.isVariableStatement)
  .flatMap(node => [...node.declarationList.declarations]).find(node => node.name.getText(parsed) === 'resize');
assert.ok(resizeDeclaration && ts.isNewExpression(resizeDeclaration.initializer)
  && resizeDeclaration.initializer.expression.getText(parsed) === 'ResizeObserver');
const mobileResizeCode = compile('exports.actualViewportResize = ' + print(resizeDeclaration.initializer.arguments[0]) + ';');
const mobileKeyboardCode = callback('plane', 'keydown');
function mobileFlower() {
  const document = { schema: 'soty.field.v1', contexts: [{ contextId: 'personal', title: 'Личное', x: 0, y: 0 }], shortcuts: [] };
  const entities = [], slots = [[0, 0], [0, -21], [21, -21], [-21, 0], [21, 0], [-21, 21], [0, 21]];
  for (let n = 0; n < slots.length; n++) {
    const entity = n === 1 ? { kind: 'person', id: 'person-one' } : n === 2 ? { kind: 'device', id: 'device-two' }
      : n === 3 ? { kind: 'builtin', id: 'notes' } : app(n + 1);
    document.shortcuts.push({ shortcutId: 'flower-' + n, entity, contextId: 'personal', slot: [...slots[n]] });
    entities.push({ entity, title: n ? 'Сота ' + n : 'Центральная сота', source: 'owner' });
  }
  return { document, entities, layout: layoutUnifiedField(document, entities) };
}
function mobileFlow(width, height, configuration = {}) {
  const h = harness(width, height), e = h.environment, data = configuration.data ?? mobileFlower();
  const savedViews = [], commits = [];
  Object.assign(e, {
    layout: data.layout, index: createFieldSpatialIndex(data.layout.nodes), entities: data.entities, fieldDocument: data.document, exposedSource: data.document, exposedDocument: data.document,
    scene: () => data.document, activeLayout: () => e.layout, layoutUnifiedField,
    cameras: { mine: { ...h.camera } }, camera: () => e.cameras.mine,
    contextFocus: { mine: '' }, cameraFit: { mine: 'overview' }, initialized: { mine: true }, selections: { mine: '' }, viewState: {},
    normalizeFieldCamera, fitFieldCamera, fieldBounds, nearestFieldNode: actualNearestFieldNode,
    localDurable: true, revision: 1, history: { entries: [] }, message: '', filter: '', visibleKinds: undefined,
    HTMLElement: Port, lastWidth: width, lastHeight: height, pointers: new Map(), resetPointers() { throw new Error('no active gesture in this resize test'); },
  });
  e.document.activeElement = null;
  e.viewport.clientWidth = width; e.viewport.clientHeight = height;
  e.options.commit = (document, context) => { commits.push({ document, context }); throw new Error('a view action must not commit a field document'); };
  e.options.onViewStateChange = value => savedViews.push(structuredClone(value));
  if (Object.hasOwn(configuration, 'readableMobileFocus')) e.options.readableMobileFocus = configuration.readableMobileFocus;
  if (Object.hasOwn(configuration, 'dimNonMatches')) e.options.dimNonMatches = configuration.dimNonMatches;
  new vm.Script(mobileFlowCode).runInContext(e);
  new vm.Script(mobileResizeCode).runInContext(e);
  new vm.Script(mobileKeyboardCode).runInContext(e);
  const keydown = e.exports.handler;
  return { ...h, data, savedViews, commits,
    cameraNow: () => ({ ...e.camera() }),
    geometry: () => JSON.stringify([data.document, data.entities.map(item => item.entity),
      e.layout.nodes.map(({ shortcutId, contextId, slot, cx, cy, width, height }) => ({ shortcutId, contextId, slot, cx, cy, width, height }))]),
    key(key) {
      const event = { key, defaultPrevented: false, isComposing: false, altKey: false, ctrlKey: false, metaKey: false, shiftKey: false,
        target: { closest: () => null }, preventDefault() { this.defaultPrevented = true; } };
      keydown(event); return event;
    },
    resizeTo(nextWidth, nextHeight) {
      h.view.width = nextWidth; h.view.height = nextHeight;
      e.viewport.clientWidth = nextWidth; e.viewport.clientHeight = nextHeight;
      e.exports.actualViewportResize();
    },
  };
}
const nearPixel = (actual, expected) => assert.ok(Math.abs(actual - expected) < .001, `${actual}px must be ${expected}px`);

test('readable mobile focus: actual 320/390 context cameras produce 118.4/144px uniform cells without changing placements or entities', () => {
  for (const [width, expected] of [[320, 118.4], [390, 144]]) {
    const h = mobileFlow(width, 844, { readableMobileFocus: true }), e = h.environment, before = h.geometry();
    e.focusContext('personal'); h.render();
    assert.equal(e.cameraFit.mine, 'context'); assert.equal(e.contextFocus.mine, 'personal');
    for (const node of e.layout.nodes) {
      nearPixel(node.width * e.camera().scale, expected);
      const target = e.nodeElements.get(node.shortcutId); assert.ok(target, 'all seven canonical cells have actual render nodes');
      nearPixel(Number.parseFloat(target.style['--uf-node-screen-width']), expected);
    }
    assert.equal(e.nodeElements.get('flower-0').dataset.caption, width === 320 ? 'compact' : 'full');
    assert.equal(h.geometry(), before); assert.deepEqual(h.commits, []);
  }
});

test('readable mobile focus: the actual floor bounds one full cell inside a short viewport height', () => {
  const h = mobileFlow(390, 160, { readableMobileFocus: true }), e = h.environment, before = h.geometry();
  e.focusContext('personal'); h.render();
  nearPixel(e.layout.nodes[0].height * e.camera().scale, 112);
  assert.ok(e.layout.nodes[0].width * e.camera().scale < 144);
  assert.equal(h.geometry(), before); assert.deepEqual(h.commits, []);
});

test('readable mobile focus: omitted/false keeps original fit and the desktop context camera is unchanged', () => {
  const a = mobileFlow(390, 844), b = mobileFlow(390, 844, { readableMobileFocus: false }), c = mobileFlow(390, 844, { readableMobileFocus: true });
  for (const h of [a, b, c]) h.environment.focusContext('personal');
  assert.deepEqual(a.cameraNow(), b.cameraNow()); assert.ok(c.cameraNow().scale > a.cameraNow().scale);
  const desktopOff = mobileFlow(1280, 800, { readableMobileFocus: false }), desktopOn = mobileFlow(1280, 800, { readableMobileFocus: true });
  desktopOff.environment.focusContext('personal'); desktopOn.environment.focusContext('personal');
  assert.deepEqual(desktopOn.cameraNow(), desktopOff.cameraNow());
  for (const h of [a, b, c, desktopOff, desktopOn]) assert.deepEqual(h.commits, []);
});

test('readable mobile focus: explicit overview still fits the complete geometry and clears focus without the floor', () => {
  const on = mobileFlow(390, 844, { readableMobileFocus: true }), off = mobileFlow(390, 844), before = on.geometry();
  on.environment.focusContext('personal'); on.environment.fitOverview(); on.render();
  off.environment.fitOverview(); off.render();
  assert.deepEqual(on.cameraNow(), off.cameraNow()); assert.equal(on.environment.contextFocus.mine, ''); assert.equal(on.environment.cameraFit.mine, 'overview');
  assert.ok(on.environment.layout.nodes[0].width * on.cameraNow().scale < 144);
  for (const node of on.environment.layout.nodes) {
    const a = fieldWorldToScreen({ x: node.bounds.left, y: node.bounds.top }, on.cameraNow(), on.view);
    const b = fieldWorldToScreen({ x: node.bounds.right, y: node.bounds.bottom }, on.cameraNow(), on.view);
    assert.ok(a.x >= 0 && b.x <= on.view.width && a.y >= 0 && b.y <= on.view.height);
  }
  assert.equal(on.geometry(), before); assert.deepEqual(on.commits, []);
});

test('readable mobile focus: explicit manual zoom-out survives actual metadata rebuild and resize', () => {
  const h = mobileFlow(390, 844, { readableMobileFocus: true }), e = h.environment, before = h.geometry();
  e.focusContext('personal'); e.setCamera({ x: 73, y: 41, scale: .18 }); const manual = h.cameraNow();
  e.entities = e.entities.map(item => ({ ...item, description: 'Обновлённые сведения' })); e.rebuild();
  assert.deepEqual(h.cameraNow(), manual); assert.equal(e.cameraFit.mine, 'manual');
  h.resizeTo(450, 940); assert.deepEqual(h.cameraNow(), manual); assert.equal(h.savedViews.at(-1).mineFit, 'manual');
  assert.equal(h.geometry(), before); assert.deepEqual(h.commits, []);
});

test('readable mobile focus: actual off-centre keyboard pan marks manual and is not undone by metadata/resize', () => {
  const h = mobileFlow(390, 844, { readableMobileFocus: true }), e = h.environment, before = h.geometry();
  e.focusContext('personal'); e.selectedId = 'flower-0'; h.render(); const initial = h.cameraNow();
  assert.equal(h.key('ArrowRight').defaultPrevented, true);
  const selected = e.layout.nodes.find(node => node.shortcutId === e.selectedId); assert.ok(selected && selected.cx > 0);
  assert.notDeepEqual(h.cameraNow(), initial); assert.equal(e.cameraFit.mine, 'manual');
  const manual = h.cameraNow(), top = fieldWorldToScreen({ x: selected.bounds.left, y: selected.bounds.top }, manual, h.view), bottom = fieldWorldToScreen({ x: selected.bounds.right, y: selected.bounds.bottom }, manual, h.view);
  assert.ok(top.x >= 40 && bottom.x <= h.view.width - 40 && top.y >= 40 && bottom.y <= h.view.height - 40);
  e.entities = e.entities.map(item => ({ ...item, description: 'Новая информация' })); e.rebuild();
  assert.deepEqual(h.cameraNow(), manual); h.resizeTo(450, 940); assert.deepEqual(h.cameraNow(), manual);
  assert.equal(h.savedViews.at(-1).mineFit, 'manual'); assert.equal(h.geometry(), before);
  assert.equal(h.calls.activate.length, 0); assert.equal(h.calls.inspect.length, 0); assert.deepEqual(h.commits, []);
});

test('readable mobile focus: keyboard selection without camera movement keeps context fitting on the next resize', () => {
  const h = mobileFlow(390, 844, { readableMobileFocus: true }), e = h.environment, before = h.geometry();
  e.focusContext('personal'); const centred = h.cameraNow(); h.render();
  assert.equal(h.key('Home').defaultPrevented, true); assert.equal(e.selectedId, 'flower-0');
  assert.deepEqual(h.cameraNow(), centred); assert.equal(e.cameraFit.mine, 'context');
  e.rebuild(); assert.deepEqual(h.cameraNow(), centred); h.resizeTo(320, 844);
  assert.equal(e.cameraFit.mine, 'context'); nearPixel(e.layout.nodes[0].width * e.camera().scale, 118.4);
  assert.equal(h.geometry(), before); assert.deepEqual(h.commits, []);
});

test('dim matching: actual filter/available/render keep geometry while nonmatching cells cannot activate or inspect', () => {
  const h = mobileFlow(1280, 900, { data: specimen(), dimNonMatches: true }), e = h.environment;
  e.focusContext('studio'); const before = h.geometry(), camera = h.cameraNow();
  e.filter = 'HIVE'; e.rebuild(); h.render();
  assert.equal(h.geometry(), before); assert.deepEqual(h.cameraNow(), camera);
  const hive = e.nodeElements.get('hive'); assert.ok(hive); assert.equal(hive.disabled, false); assert.equal(hive.dataset.muted, 'false');
  assert.ok(e.available('hive')); assert.equal(e.available('canvas'), undefined); assert.ok(e.inspectElements.has('hive'));
  for (const [id, target] of e.nodeElements) if (id !== 'hive') {
    assert.equal(target.disabled, true, id); assert.equal(target.dataset.muted, 'true', id); assert.equal(target.getAttribute('aria-hidden'), 'true', id);
    assert.equal(e.inspectElements.has(id), false, id); target.dispatch('click'); target.dispatch('contextmenu');
  }
  assert.equal(h.calls.activate.length, 0); assert.equal(h.calls.inspect.length, 0); assert.deepEqual(h.commits, []);
});

test('dim matching: clearing the actual query restores the same cells/coordinates/camera and activation', () => {
  const h = mobileFlow(1280, 900, { data: specimen(), dimNonMatches: true }), e = h.environment;
  e.focusContext('studio'); const before = h.geometry(), camera = h.cameraNow();
  e.filter = 'HIVE'; e.rebuild(); h.render();
  const canvas = e.nodeElements.get('canvas'); assert.ok(canvas); assert.equal(canvas.disabled, true); canvas.dispatch('click');
  assert.equal(h.calls.activate.length, 0);
  e.filter = ''; e.rebuild(); h.render();
  assert.equal(h.geometry(), before); assert.deepEqual(h.cameraNow(), camera);
  assert.equal(e.nodeElements.get('canvas'), canvas, 'the existing DOM node is updated, never re-created for a query');
  assert.equal(canvas.disabled, false); assert.equal(canvas.dataset.muted, 'false'); assert.ok(e.available('canvas'));
  canvas.dispatch('click'); assert.equal(h.calls.activate.length, 1); assert.equal(h.calls.activate[0][0].entity.id, app(2).id);
  assert.deepEqual(h.commits, []);
});


function completeVisibleCells(h, padding = 24) {
  return h.environment.layout.nodes.filter(node => {
    const a = fieldWorldToScreen({ x: node.bounds.left, y: node.bounds.top }, h.cameraNow(), h.view);
    const b = fieldWorldToScreen({ x: node.bounds.right, y: node.bounds.bottom }, h.cameraNow(), h.view);
    return a.x >= padding - .001 && b.x <= h.view.width - padding + .001
      && a.y >= padding - .001 && b.y <= h.view.height - padding + .001;
  });
}
function sparsePair() {
  const document = { schema: 'soty.field.v1', contexts: [{ contextId: 'personal', title: 'Личное', x: 0, y: 0 }], shortcuts: [] };
  const entities = [];
  for (const [n, slot] of [[0, [-42, 0]], [1, [42, 0]]]) {
    const entity = app(n + 1);
    document.shortcuts.push({ shortcutId: 'sparse-' + n, entity, contextId: 'personal', slot });
    entities.push({ entity, title: n ? 'Правая сота' : 'Левая сота', source: 'owner' });
  }
  return { document, entities, layout: layoutUnifiedField(document, entities) };
}

test('readable mobile focus: sparse saved cells keep at least one full real cell visible, prefer the selected cell and never move data', () => {
  for (const selected of ['', 'sparse-1']) {
    const h = mobileFlow(390, 844, { readableMobileFocus: true, data: sparsePair() }), e = h.environment, before = h.geometry();
    e.selectedId = selected; e.focusContext('personal'); h.render();
    assert.ok(completeVisibleCells(h).length >= 1, 'a readable focus cannot consist only of blank space between sparse real cells');
    nearPixel(e.layout.nodes[0].width * h.cameraNow().scale, 144);
    if (selected) {
      const node = e.layout.nodes.find(item => item.shortcutId === selected);
      nearPixel(h.cameraNow().x, node.cx); nearPixel(h.cameraNow().y, node.cy);
      assert.ok(completeVisibleCells(h).some(item => item.shortcutId === selected));
    }
    assert.equal(h.geometry(), before); assert.deepEqual(h.commits, []);
  }
  const old = mobileFlow(390, 844, { data: sparsePair() }), off = mobileFlow(390, 844, { data: sparsePair(), readableMobileFocus: false });
  old.environment.focusContext('personal'); off.environment.focusContext('personal');
  assert.deepEqual(old.cameraNow(), off.cameraNow()); assert.equal(completeVisibleCells(off).length, 2);
});

test('dim matching: actual overview retains a fourth-position match, its explicit zoom and matching-only Home/Arrow navigation', () => {
  const h = mobileFlow(320, 338, { data: specimen(), dimNonMatches: true, readableMobileFocus: true }), e = h.environment;
  const before = h.geometry(); e.filter = 'Аня'; e.rebuild(); h.render();
  assert.equal(e.viewport.dataset.lod, 'overview'); assert.equal(e.contextFocus.mine, '');
  const anna = e.nodeElements.get('anna'); assert.ok(anna, 'the fourth studio child must not be omitted by the three-thumbnail overview');
  assert.equal(anna.disabled, false); assert.equal(anna.dataset.muted, 'false'); assert.equal(anna.dataset.preview, 'true');
  assert.ok(e.available('anna')); assert.equal(e.available('hive'), undefined);
  for (const [id, target] of e.nodeElements) if (id !== 'anna') {
    assert.equal(target.disabled, true, id); target.dispatch('click'); target.dispatch('contextmenu');
  }
  assert.equal(h.calls.activate.length, 0); assert.equal(h.calls.inspect.length, 0);
  const overview = h.cameraNow(); anna.dispatch('click'); h.render();
  assert.equal(e.contextFocus.mine, 'studio'); assert.ok(h.cameraNow().scale > overview.scale);
  assert.ok(e.available('anna')); assert.equal(e.nodeElements.get('anna').disabled, false); assert.equal(h.calls.activate.length, 0);
  // A second matching person arrives through the ordinary entity metadata port;
  // Home and ArrowRight must traverse these real matches rather than dim cells.
  e.entities = e.entities.map(item => item.entity.id === 'tim' ? { ...item, title: 'Аня и Тим' } : item); e.rebuild();
  assert.equal(h.key('Home').defaultPrevented, true); assert.equal(e.selectedId, 'anna');
  assert.equal(h.key('ArrowRight').defaultPrevented, true); assert.equal(e.selectedId, 'tim'); assert.ok(e.available(e.selectedId));
  assert.equal(h.geometry(), before); assert.equal(h.calls.activate.length, 0); assert.equal(h.calls.inspect.length, 0); assert.deepEqual(h.commits, []);
});
