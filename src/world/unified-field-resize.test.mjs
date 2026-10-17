import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import * as cameraMath from './unified-field-camera.mjs';
import * as fieldLayout from './unified-field-layout.mjs';
import { applyFieldCommand } from './unified-field-state.mjs';

// Actual production pointer, placement, keyboard and ResizeObserver transitions.
// Only DOM/render/timers and the persistence sink are ports; camera math, typed
// geometry, move preview and command application remain the real modules.
const source = readFileSync(process.env.SOTY_REVIEW_FIELD_SOURCE || new URL('./unified-field.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const engine = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedField');
const statements = [...engine.body.statements];
const declaration = name => statements.find(node => ts.isVariableStatement(node)
  && node.declarationList.declarations.some(item => item.name.getText(parsed) === name));
const functions = names => names.map(name => {
  const node = statements.find(item => ts.isFunctionDeclaration(item) && item.name?.text === name);
  assert.ok(node, 'actual production function ' + name); return print(node);
}).join('\n');
const listener = (owner, name) => {
  const statement = statements.find(node => ts.isExpressionStatement(node) && ts.isCallExpression(node.expression)
    && node.expression.expression.getText(parsed) === owner + '.addEventListener' && node.expression.arguments[0]?.text === name);
  assert.ok(statement, 'actual production listener ' + owner + ' ' + name); return print(statement.expression.arguments[1]);
};
const resize = declaration('resize').declarationList.declarations[0].initializer;
assert.ok(ts.isNewExpression(resize) && ts.isArrowFunction(resize.arguments[0]));
const same = parsed.statements.find(node => ts.isVariableStatement(node)
  && node.declarationList.declarations.some(item => item.name.getText(parsed) === 'same'));
assert.ok(same);

const compiled = ts.transpileModule(`${print(same)}
function fixture(notifyInitial = true) {
  const state = { camera: { x: 0, y: 0, scale: 1 }, commands: [], renders: 0, focused: '', fit: 0 };
  let fieldDocument = { schema: 'soty.field.v1', contexts: [{ contextId: 'personal', title: 'Моё', x: 0, y: 0 }],
    shortcuts: [{ shortcutId: 'first', entity: { kind: 'app', id: 'sample-app' }, contextId: 'personal', slot: [0, 0] }] };
  const entities = [{ entity: { kind: 'app', id: 'sample-app' }, title: 'Приложение', source: 'owner' }], metrics = UNIFIED_FIELD_METRICS;
  let layout = layoutUnifiedField(fieldDocument, entities, metrics);
  const mode = 'mine', initialized = { mine: true }, cameraFit = { mine: 'manual' }, contextFocus = { mine: 'personal' };
  let gesture = null, pinch = null, pendingPointer = null, inPointerFrame = false, moving = null, contextPreview = null,
    suppressClickUntil = 0, destroyed = false, arranging = true, selectedId = 'first';
  const pointers = new Map(), captures = new Set(), timers = new Map(), tasks = []; let timer = 0;
  const viewport = { clientWidth: 390, clientHeight: 620, getBoundingClientRect: () => ({ left: 0, top: 0 }),
    hasPointerCapture: id => captures.has(id), setPointerCapture: id => captures.add(id), releasePointerCapture: id => captures.delete(id),
    contains: node => node === viewport };
  ${print(declaration('viewportSize'))}
  const camera = () => state.camera, setCamera = next => { state.camera = normalizeFieldCamera(next); };
  const render = () => { state.renders++; }, emit = () => {}, schedule = () => {}, announce = () => {}, live = {}, tools = {};
  const options = { commit: true }, nodeElements = new Map();
  const setTimeout = callback => { timers.set(++timer, callback); return timer; }, clearTimeout = id => timers.delete(id);
  const available = id => layout.nodes.find(node => node.shortcutId === id), perform = task => tasks.push(task);
  const execute = async command => { state.commands.push(structuredClone(command)); const applied = applyFieldCommand(fieldDocument, command);
    fieldDocument = applied.document; layout = layoutUnifiedField(fieldDocument, entities, metrics); return applied; };
  const focusNode = id => { state.focused = id; }, fitOverview = () => { state.fit++; }, focusContext = () => { state.fit++; }, rebuild = () => {};
  const nearestFieldNode = () => null, setArrange = value => { arranging = value; }, undo = async () => null;
  ${functions(['localPoint', 'flushPointerSample', 'finishGesture', 'cancel', 'resetPointers', 'capturePointer', 'beginMove', 'previewMove', 'commitMove'])}
  ${print(declaration('lastWidth'))}
  const onResize = ${print(resize.arguments[0])};
  const handlers = { down: ${listener('viewport', 'pointerdown')}, move: ${listener('viewport', 'pointermove')},
    up: ${listener('viewport', 'pointerup')}, cancel: ${listener('viewport', 'pointercancel')},
    lost: ${listener('viewport', 'lostpointercapture')}, blur: ${listener('window', 'blur')}, key: ${listener('plane', 'keydown')} };
  if (notifyInitial) onResize();
  return { state, viewport, frame: flushPointerSample, settle: () => Promise.all(tasks), beginMove,
    document: () => structuredClone(fieldDocument),
    snapshot: () => ({ pointers: [...pointers.keys()], captures: [...captures], pinch: !!pinch,
      gesture: gesture?.type ?? null, pending: !!pendingPointer, moving: !!moving, context: !!contextPreview }),
    arrange(value) { arranging = value; },
    resize(width, height) { viewport.clientWidth = width; viewport.clientHeight = height; onResize(); },
    event(type, id = 1, overrides = {}) {
      const target = overrides.targetKind ?? 'background';
      const event = { pointerId: id, pointerType: 'touch', button: 0, buttons: 1, clientX: 195, clientY: 310,
        target: { closest(selector) { return selector === '[data-shortcut-id]' && target === 'node' ? { dataset: { shortcutId: 'first' } }
          : selector === '.uf-context-title' && target === 'context' ? { dataset: { contextId: 'personal' } } : null; } },
        defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, stopPropagation() {}, ...overrides };
      handlers[type](event); return event;
    } };
}
exports.fixture = fixture;`, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;

const fixture = (notifyInitial = true) => {
  // The contract deliberately rejects foreign object prototypes. Transfer the
  // DOM fixture's command across the VM port as structured data, like the real
  // persistence boundary, without weakening that contract or its implementation.
  const apply = (document, command) => applyFieldCommand(structuredClone(document), structuredClone(command));
  const context = vm.createContext({ exports: {}, ...cameraMath, ...fieldLayout, applyFieldCommand: apply, structuredClone, performance });
  new vm.Script(compiled).runInContext(context); return context.exports.fixture(notifyInitial);
};
const snapshot = f => JSON.parse(JSON.stringify(f.snapshot()));
const clean = f => assert.deepEqual(snapshot(f), { pointers: [], captures: [], pinch: false, gesture: null, pending: false, moving: false, context: false });
const drag = (f, targetKind = 'node') => { f.event('down', 1, { targetKind }); f.event('move', 1, { targetKind, clientX: 245 }); };

test('rotating during a pending shortcut drag cancels capture and late pointerup cannot commit another viewport slot', async () => {
  const f = fixture(), before = JSON.stringify(f.document()); drag(f);
  assert.equal(f.snapshot().gesture, 'move'); assert.equal(f.snapshot().pending, true);
  f.resize(780, 390); const resized = snapshot(f); f.frame(); f.event('up', 1, { clientX: 245 }); await f.settle();
  assert.equal(f.state.commands.length, 0, 'late pointerup emitted ' + JSON.stringify(f.state.commands[0]));
  assert.equal(JSON.stringify(f.document()), before); clean(f);
  assert.deepEqual(resized, { pointers: [], captures: [], pinch: false, gesture: null, pending: false, moving: false, context: false });
});

test('rotating after a rendered whole-context preview cannot commit its late pointerup', async () => {
  const f = fixture(), before = JSON.stringify(f.document()); drag(f, 'context'); f.frame();
  assert.equal(f.snapshot().context, true); f.resize(780, 390);
  f.event('up', 1, { clientX: 245 }); await f.settle(); assert.equal(f.state.commands.length, 0); assert.equal(JSON.stringify(f.document()), before); clean(f);
});

test('a resize drops queued pan samples while preserving the already observed camera and allowing a fresh pan', () => {
  const f = fixture(); f.arrange(false); drag(f); f.frame();
  const camera = JSON.stringify(f.state.camera); f.event('move', 1, { clientX: 260 });
  f.resize(780, 390); clean(f); f.frame(); f.event('up', 1, { clientX: 260 });
  assert.equal(JSON.stringify(f.state.camera), camera);
  f.event('down', 2); f.event('move', 2, { clientX: 215 }); f.frame(); f.event('up', 2, { clientX: 215 }); clean(f);
  assert.notEqual(JSON.stringify(f.state.camera), camera); assert.equal(f.state.commands.length, 0);
});

test('rotation during pinch clears both captured fingers and prevents a queued zoom or recycled one-finger move', () => {
  const f = fixture(); f.event('down', 1, { clientX: 100 }); f.event('down', 2, { clientX: 200 });
  f.event('move', 2, { clientX: 250 }); assert.equal(f.snapshot().pinch, true);
  f.resize(780, 390); clean(f); f.frame(); f.event('up', 2, { clientX: 250 }); f.event('move', 1, { clientX: 160 });
  assert.equal(f.state.camera.scale, 1); clean(f);
});

test('identical ResizeObserver notifications keep an intentional drag and its real valid placement', async () => {
  const f = fixture(); drag(f); f.resize(390, 620);
  assert.equal(f.snapshot().gesture, 'move'); f.event('up', 1, { clientX: 245 }); await f.settle();
  assert.equal(f.state.commands.length, 1); assert.equal(f.state.commands[0].type, 'move-shortcut');
  assert.notDeepEqual([...f.document().shortcuts[0].slot], [0, 0]); clean(f);
});

test('identical resize keeps anchored pinch operational', () => {
  const f = fixture(); f.event('down', 1, { clientX: 100 }); f.event('down', 2, { clientX: 200 });
  f.resize(390, 620); f.event('move', 2, { clientX: 300 }); f.frame();
  assert.equal(f.state.camera.scale, 2); f.event('up', 2, { clientX: 300 }); clean(f);
});

test('a real one-pixel viewport shift cancels a pointer sequence even below the camera-refit threshold', () => {
  const f = fixture(); drag(f); f.resize(391, 620); clean(f);
  f.event('up', 1, { clientX: 245 }); assert.equal(f.state.commands.length, 0);
});

test('the first observer notification at unchanged current dimensions does not cancel a gesture', async () => {
  const f = fixture(false); drag(f); f.resize(390, 620);
  assert.equal(f.snapshot().gesture, 'move'); f.event('up', 1, { clientX: 245 }); await f.settle(); assert.equal(f.state.commands.length, 1);
});

test('resize cancels a keyboard placement preview but a fresh explicit keyboard move remains usable', async () => {
  const f = fixture(); f.event('key', 1, { key: ' ' }); assert.equal(f.snapshot().moving, true);
  f.resize(780, 390); clean(f); f.event('key', 1, { key: 'Enter' }); await f.settle(); assert.equal(f.state.commands.length, 0);
  f.event('key', 1, { key: ' ' }); f.event('key', 1, { key: 'ArrowRight' }); f.event('key', 1, { key: 'Enter' }); await f.settle();
  assert.equal(f.state.commands.length, 1); assert.deepEqual([...f.document().shortcuts[0].slot], [1, 0]);
});

test('hiding the viewport during drag cancels before its dimensions return', async () => {
  const f = fixture(); drag(f); f.resize(0, 0); clean(f); f.resize(390, 620); f.event('up', 1, { clientX: 245 }); await f.settle();
  assert.equal(f.state.commands.length, 0);
});
