import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';
import vm from 'node:vm';
import { createRequire } from 'node:module';

// Execute the shipped listeners/functions. Only DOM, time, rendering and the
// persistence sink are ports; no gesture or inspection decision is duplicated.
// Optional source/root ports allow the same tests to qualify old/new snapshots.
const root = process.env.SOTY_REVIEW_FIELD_ROOT
  ? pathToFileURL(`${resolve(process.env.SOTY_REVIEW_FIELD_ROOT)}${sep}`) : new URL('../../', import.meta.url);
const ts = createRequire(new URL('package.json', root))('typescript');
const source = readFileSync(process.env.SOTY_REVIEW_FIELD_SOURCE || new URL('src/world/unified-field.ts', root), 'utf8');
const { normalizeFieldCamera, zoomFieldCamera, fieldScreenToWorld } = await import(new URL('src/world/unified-field-camera.mjs', root));
const parsed = ts.createSourceFile('unified-field.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const engine = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedField');
assert.ok(engine?.body, 'production field engine');
const functions = names => engine.body.statements.filter(node => ts.isFunctionDeclaration(node) && names.includes(node.name?.text)).map(print).join('\n');
const listener = (owner, name, optional = false) => {
  let found;
  const visit = node => {
    if (!found && ts.isCallExpression(node) && node.expression.getText(parsed) === `${owner}.addEventListener` && node.arguments[0]?.text === name) found = node.arguments[1];
    ts.forEachChild(node, visit);
  };
  visit(engine);
  if (!found && optional) return '() => {}'; // Baseline lacks only the new background shortcut.
  assert.ok(found, `production ${owner} ${name}`); return print(found);
};
const wheelFunction = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'shouldYieldFieldWheel');
assert.ok(wheelFunction, 'production wheel ownership policy');
const compiled = ts.transpileModule(`${print(wheelFunction)}
function fixture(config = {}) {
  const state = { camera: { x: 40, y: -20, scale: .7 }, changes: 0, commits: 0, inspections: [], activations: [],
    previews: 0, moves: 0, rejectCapture: false, missing: false };
  let gesture = null, pinch = null, pendingPointer = null, inPointerFrame = false, moving = null, contextPreview = null,
    suppressClickUntil = 0, destroyed = false, arranging = false, selectedId = 'first', lastInspection = null;
  const pointers = new Map(), captures = new Set(), timers = new Map(); let timer = 0, now = 1000;
  const performance = { now: () => now };
  const setTimeout = (callback, delay) => { timers.set(++timer, { callback, delay }); return timer; }, clearTimeout = id => timers.delete(id);
  const options = { commit: config.writable === false ? null : async () => {},
    onInspect: config.inspect === false ? undefined : (entity, id) => state.inspections.push(id),
    onActivate: (entity, id) => state.activations.push(id) };
  const mode = config.mode || 'mine';
  class Element {
    constructor(kind = 'background', parent = null) { this.kind = kind; this.parentElement = parent; this.dataset = {}; this.scrollWidth = this.clientWidth = 390; this.scrollHeight = this.clientHeight = 620; this.overflowX = this.overflowY = 'visible'; }
    closest(selector) {
      for (let node = this; node; node = node.parentElement) {
        if (node.kind === 'node' && (selector.includes('[data-shortcut-id]') || selector.includes('.uf-node') || selector.split(',').includes('button'))) return node;
        if (node.kind === 'title' && selector.includes('.uf-context-title')) return node;
        if (node.kind === 'control' && (selector.includes('.uf-inspect') || selector.split(',').includes('button'))) return node;
        if (node.kind === 'editable' && selector.includes('[contenteditable]')) return node;
      }
      return null;
    }
  }
  const viewport = new Element(); viewport.ownerDocument = { defaultView: { getComputedStyle: node => node }, scrollingElement: null };
  viewport.getBoundingClientRect = () => ({ left: 0, top: 0 });
  viewport.hasPointerCapture = id => captures.has(id);
  viewport.setPointerCapture = id => { if (state.rejectCapture) throw new Error('inactive pointer'); captures.add(id); };
  viewport.releasePointerCapture = id => captures.delete(id);
  viewport.contains = node => { for (let item = node; item; item = item.parentElement) if (item === viewport) return true; return false; };
  const target = new Element('node', viewport); target.dataset = { shortcutId: 'first', preview: String(!!config.preview) };
  const node = { shortcutId: 'first', contextId: 'space', slot: [0, 0], kind: 'app', entity: { entity: { kind: 'app', id: 'app' }, title: 'Application' } };
  const nodeElements = new Map([['first', target]]);
  const layout = { nodes: [node], contexts: [{ contextId: 'space', x: 0, y: 0, body: { left: -10000, right: 10000, top: -10000, bottom: 10000 } }] };
  const available = id => !state.missing && id === node.shortcutId ? node : undefined;
  const camera = () => state.camera, viewportSize = () => ({ width: 390, height: 620 });
  const setCamera = next => { state.camera = normalizeFieldCamera(next); state.changes++; };
  const render = () => {}, emit = () => {}, schedule = () => {}, announce = () => {}, remember = () => {};
  const focusPreview = () => { state.previews++; }, focusNode = () => {}, nearestFieldNode = () => node;
  const perform = () => {}, commitMove = () => { state.commits++; moving = null; return Promise.resolve(); }, execute = commitMove;
  const beginMove = id => { state.moves++; moving = { shortcutId: id, preview: { valid: true, target: { contextId: 'space', slot: [0, 0] } } }; };
  const fieldPointToSlot = () => [1, 0], metrics = {}, previewMove = value => { moving.preview.target = value; };
  const setArrange = value => { arranging = value; }, fitOverview = () => {}, undo = commitMove;
  ${functions(['localPoint', 'flushPointerSample', 'finishGesture', 'cancel', 'resetPointers', 'capturePointer', 'inspectShortcut'])}
  const handlers = {
    down: ${listener('viewport', 'pointerdown')}, move: ${listener('viewport', 'pointermove')}, up: ${listener('viewport', 'pointerup')},
    cancel: ${listener('viewport', 'pointercancel')}, lost: ${listener('viewport', 'lostpointercapture')}, outsideUp: ${listener('window', 'pointerup')},
    blur: ${listener('window', 'blur')}, key: ${listener('plane', 'keydown')}, click: ${listener('viewport', 'click')},
    context: ${listener('target', 'contextmenu')}, activate: ${listener('target', 'click')}, double: ${listener('viewport', 'dblclick', true)}
  };
  function event(type, overrides = {}) {
    const event = { pointerId: 1, pointerType: 'touch', button: 0, buttons: 1, isPrimary: true, clientX: 100, clientY: 150,
      target, currentTarget: target, detail: 1, defaultPrevented: false, isComposing: false, altKey: false, ctrlKey: false, metaKey: false, shiftKey: false,
      preventDefault() { this.defaultPrevented = true; }, stopPropagation() {}, ...overrides };
    handlers[type](event);
    if (type === 'click' && !event.defaultPrevented && event.target === target) handlers.activate(event);
    return event;
  }
  return { state, target, viewport, event, frame: flushPointerSample,
    setArrange: value => { arranging = value; }, advance: delta => { now += delta; },
    timerCallbacks: () => [...timers.values()].map(value => value.callback),
    timerDelays: () => [...timers.values()].map(value => value.delay),
    fireTimers: () => { const pending = [...timers.values()]; timers.clear(); for (const timer of pending) timer.callback(); },
    snapshot: () => ({ arranging, gesture: gesture?.type ?? null, pointers: [...pointers.keys()], captures: [...captures], timers: timers.size, pending: !!pendingPointer }),
    element: kind => new Element(kind, viewport), dispose: () => { resetPointers(); destroyed = true; } };
}
exports.fixture = fixture;`, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const fixture = config => {
  const context = vm.createContext({ exports: {}, normalizeFieldCamera, zoomFieldCamera, fieldScreenToWorld });
  new vm.Script(compiled).runInContext(context); return context.exports.fixture(config);
};
const clean = f => { const s = f.snapshot(); assert.equal(s.gesture, null); assert.equal(s.pointers.length, 0); assert.equal(s.captures.length, 0); assert.equal(s.timers, 0); assert.equal(s.pending, false); };
const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-7, `${a} != ${b}`);

test('a normal tap activates synchronously and never waits for a double-tap decision', () => {
  const f = fixture(); f.event('down'); f.event('up'); const click = f.event('click');
  assert.equal(click.defaultPrevented, false); assert.equal(f.state.activations.length, 1); assert.equal(f.state.inspections.length, 0); clean(f);
});

test('450 ms hold opens the same inspector once and never enters arrangement, moves, activates or writes', () => {
  const f = fixture(); f.event('down'); assert.deepEqual(Array.from(f.timerDelays()), [450]); f.fireTimers();
  assert.equal(f.state.inspections.length, 1); assert.equal(f.snapshot().arranging, false); assert.equal(f.state.moves, 0);
  assert.equal(f.event('context').defaultPrevented, true); assert.equal(f.state.inspections.length, 1, 'native contextmenu during hold is not a second action');
  const before = { ...f.state.camera }; f.event('move', { clientX: 220 }); f.frame(); for (const key of ['x', 'y', 'scale']) assert.equal(f.state.camera[key], before[key]);
  f.event('up'); assert.equal(f.event('context').defaultPrevented, true); assert.equal(f.state.inspections.length, 1);
  assert.equal(f.event('click').defaultPrevented, true); assert.equal(f.state.activations.length, 0); assert.equal(f.state.commits, 0); clean(f);
  f.event('down'); f.event('up'); assert.equal(f.event('click').defaultPrevented, false); assert.equal(f.state.activations.length, 1);
});

for (const config of [{ writable: false }, { writable: false, mode: 'search' }]) test(`inspection works without a layout write grant in ${config.mode || 'mine'}`, () => {
  const f = fixture(config); f.event('down'); f.fireTimers(); assert.equal(f.state.inspections.length, 1); f.event('up');
  assert.equal(f.snapshot().arranging, false); assert.equal(f.state.commits, 0); clean(f);
});

test('native contextmenu arriving before the hold timer wins once and consumes the release', () => {
  const f = fixture(); f.event('down'); assert.equal(f.event('context').defaultPrevented, true); f.fireTimers();
  assert.equal(f.state.inspections.length, 1); f.event('up'); assert.equal(f.event('click').defaultPrevented, true); assert.equal(f.state.commits, 0); clean(f);
  f.event('down', { button: 2 }); assert.equal(f.event('context').defaultPrevented, true); assert.equal(f.state.inspections.length, 2, 'fresh right click remains usable');
});

test('crossing the existing 7 px movement tolerance cancels hold and pans without moving an object', () => {
  const f = fixture(); f.event('down'); f.event('move', { clientX: 108 }); f.frame(); f.fireTimers();
  assert.equal(f.state.inspections.length, 0); assert.equal(f.snapshot().gesture, 'pan'); assert.equal(f.state.changes, 1); assert.equal(f.state.moves, 0);
  assert.equal(f.event('context').defaultPrevented, true); assert.equal(f.state.inspections.length, 0, 'panning cannot become a native long-press menu');
  f.event('up', { clientX: 108 }); assert.equal(f.state.commits, 0); clean(f);
});

test('a second pointer cancels hold and remains an anchored camera pinch', () => {
  const f = fixture(); f.event('down'); f.event('down', { pointerId: 2, isPrimary: false, clientX: 150 }); f.fireTimers();
  f.event('move', { pointerId: 2, clientX: 200 }); f.frame(); assert.ok(f.state.camera.scale > .7);
  assert.equal(f.state.inspections.length, 0); assert.equal(f.state.moves, 0); f.event('up', { pointerId: 2 }); clean(f);
});

for (const end of ['cancel', 'lost', 'blur', 'outsideUp']) test(`${end} cancels the hold and rejects an already queued stale timer`, () => {
  const f = fixture(); f.event('down'); const pending = f.timerCallbacks();
  f.event(end, end === 'outsideUp' ? { target: {} } : {}); f.fireTimers(); for (const timer of pending) timer();
  assert.equal(f.state.inspections.length, 0); assert.equal(f.state.commits, 0); clean(f);
});

test('dispose and pointer-capture failure do not open a stale inspector or retain timers', () => {
  for (const failCapture of [false, true]) {
    const f = fixture(); f.event('down'); const pending = f.timerCallbacks();
    if (failCapture) f.state.rejectCapture = true; else f.dispose();
    f.fireTimers(); if (!failCapture) for (const callback of pending) callback();
    assert.equal(f.state.inspections.length, 0); assert.equal(f.state.commits, 0); clean(f);
  }
});

test('objects are draggable only after explicit arrangement; a stationary arranged press does not inspect', () => {
  const f = fixture(); f.setArrange(true); f.event('down'); assert.equal(f.snapshot().timers, 0); f.fireTimers();
  f.event('move', { clientX: 120 }); assert.equal(f.snapshot().gesture, 'move'); f.event('up', { clientX: 120 });
  assert.equal(f.state.moves, 1); assert.equal(f.state.commits, 1); assert.equal(f.state.inspections.length, 0); clean(f);
});

for (const key of [{ key: 'F10', shiftKey: true }, { key: 'ContextMenu' }]) test(`${key.key} reaches the same inspector and retains IME/modifier/handled-key boundaries`, () => {
  const f = fixture(); assert.equal(f.event('key', key).defaultPrevented, true); assert.equal(f.state.inspections.length, 1);
  assert.equal(f.event('context').defaultPrevented, true); assert.equal(f.state.inspections.length, 1, 'browser-generated contextmenu cannot duplicate the keyboard action');
  for (const extra of [{ isComposing: true }, { defaultPrevented: true }, { ctrlKey: true }, { metaKey: true }, { altKey: true }]) {
    f.event('key', { ...key, ...extra }); assert.equal(f.state.inspections.length, 1);
  }
});

test('preview-sized nodes retain zoom-to-preview context behavior, and absent inspection support cannot create a move', () => {
  const p = fixture({ preview: true }); p.event('down'); assert.equal(p.snapshot().timers, 0); p.event('up');
  assert.equal(p.event('context').defaultPrevented, true); assert.equal(p.state.previews, 1); assert.equal(p.state.inspections.length, 0);
  const f = fixture({ inspect: false }); f.event('down'); f.fireTimers(); f.event('up');
  assert.equal(f.state.inspections.length, 0); assert.equal(f.snapshot().arranging, false); assert.equal(f.state.moves, 0); assert.equal(f.state.commits, 0); clean(f);
});

test('background double click zooms around its actual pointer without an activation, delayed click or layout write', () => {
  const f = fixture(), anchor = { x: 72, y: 431 }, view = { width: 390, height: 620 };
  const before = fieldScreenToWorld(anchor, f.state.camera, view);
  const event = f.event('double', { target: f.viewport, clientX: anchor.x, clientY: anchor.y, detail: 2 });
  assert.equal(event.defaultPrevented, true); near(f.state.camera.scale, .7 * 1.6);
  const after = fieldScreenToWorld(anchor, f.state.camera, view); near(before.x, after.x); near(before.y, after.y);
  assert.equal(f.state.inspections.length, 0); assert.equal(f.state.activations.length, 0); assert.equal(f.state.commits, 0);
});

test('double click yields on objects, titles, controls, editable content and internally scrolling panels', () => {
  for (const kind of ['node', 'title', 'control', 'editable', 'scroll']) {
    const f = fixture(), target = kind === 'node' ? f.target : f.element(kind);
    if (kind === 'scroll') { target.overflowY = 'auto'; target.scrollHeight = 1500; }
    assert.equal(f.event('double', { target }).defaultPrevented, false, kind); assert.equal(f.state.changes, 0, kind);
  }
});

test('arrangement, owned press/pinch, handled events and a trailing drag double click cannot change the camera', () => {
  for (const scenario of ['arrange', 'press', 'pinch', 'handled', 'drag']) {
    const f = fixture();
    if (scenario === 'arrange') f.setArrange(true);
    if (['press', 'pinch', 'drag'].includes(scenario)) f.event('down');
    if (scenario === 'pinch') f.event('down', { pointerId: 2, clientX: 150 });
    if (scenario === 'drag') { f.event('move', { clientX: 120 }); f.event('up', { clientX: 120 }); }
    const before = f.state.changes;
    assert.equal(f.event('double', { target: f.viewport, defaultPrevented: scenario === 'handled' }).defaultPrevented, scenario === 'handled', scenario);
    assert.equal(f.state.changes, before, scenario);
  }
});
