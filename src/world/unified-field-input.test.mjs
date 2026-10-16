import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import { normalizeFieldCamera, zoomFieldCamera, fieldScreenToWorld, fieldWorldToScreen } from './unified-field-camera.mjs';

// Run the production wheel listener, not a copy of its conditions. Only the DOM
// and the setCamera sink are stubbed; the actual policy and camera math execute.
const source = readFileSync(new URL('./unified-field.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter();
const print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const fn = (nodes, name) => {
  const found = nodes.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name);
  assert.ok(found, `Production function ${name} must exist`); return found;
};
const engine = fn(parsed.statements, 'createUnifiedField');
const wheel = [...engine.body.statements].find(node => ts.isExpressionStatement(node) && ts.isCallExpression(node.expression)
  && node.expression.expression.getText(parsed) === 'viewport.addEventListener' && node.expression.arguments[0]?.text === 'wheel');
assert.ok(wheel && ts.isArrowFunction(wheel.expression.arguments[1]), 'Production wheel listener must exist');
const keydown = [...engine.body.statements].find(node => ts.isExpressionStatement(node) && ts.isCallExpression(node.expression)
  && node.expression.expression.getText(parsed) === 'plane.addEventListener' && node.expression.arguments[0]?.text === 'keydown');
assert.ok(keydown && ts.isArrowFunction(keydown.expression.arguments[1]), 'Production keyboard listener must exist');

const screenSource = readFileSync(new URL('./unified-field-screen.ts', import.meta.url), 'utf8');
const screen = ts.createSourceFile('unified-field-screen.ts', screenSource, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const screenFn = fn(screen.statements, 'createUnifiedFieldScreen');
const panDeclaration = [...screenFn.body.statements].find(node => ts.isVariableStatement(node)
  && node.declarationList.declarations.some(item => ts.isIdentifier(item.name) && item.name.text === 'panField'));
assert.ok(panDeclaration, 'Production tap pan controller must exist');
const compiled = ts.transpileModule([
  print(fn(parsed.statements, 'fieldWheelCamera')),
  print(fn(parsed.statements, 'shouldYieldFieldWheel')),
  'function createInputHarness(viewport, state) {',
  'let gesture = state.gesture ?? null, pinch = state.pinch ?? null, engine = state.engine;',
  'const options = {}, mode = "mine", selectedId = "first", arranging = false, moving = null, contextPreview = null;',
  'const layout = { nodes: [{ shortcutId: "next" }] }, nearestFieldNode = () => layout.nodes[0];',
  'const focusNode = id => { state.focused = id; }, undo = () => { state.undos = (state.undos ?? 0) + 1; }, perform = () => {};',
  'const camera = () => state.camera, viewportSize = () => ({ width: viewport.clientWidth, height: viewport.clientHeight });',
  'const setCamera = next => { state.camera = next; state.changes++; };',
  `const onWheel = ${print(wheel.expression.arguments[1])};`,
  `const onKey = ${print(keydown.expression.arguments[1])};`,
  printer.printNode(ts.EmitHint.Unspecified, panDeclaration, screen),
  'return { onWheel, onKey, panField };',
  '}',
  'exports.createInputHarness = createInputHarness;',
].join('\n'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;

class FakeElement {
  parentElement = null;
  clientWidth = 390; clientHeight = 620; scrollWidth = 390; scrollHeight = 620;
  overflowX = 'visible'; overflowY = 'visible'; native = false;
  ownerDocument = { defaultView: { getComputedStyle: node => node }, scrollingElement: null };
  closest() { for (let node = this; node; node = node.parentElement) if (node.native) return node; return null; }
  getBoundingClientRect() { return { left: 10, top: 20 }; }
}
const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-7, `${a} != ${b}`);
const fixture = (options = {}) => {
  const viewport = new FakeElement(), target = new FakeElement(); target.parentElement = viewport;
  const original = { x: 140, y: -90, scale: .7 };
  const state = { camera: { ...original }, changes: 0, ...options };
  const context = vm.createContext({ exports: {}, normalizeFieldCamera, zoomFieldCamera, Element: FakeElement });
  new vm.Script(compiled).runInContext(context);
  const api = context.exports.createInputHarness(viewport, state);
  function dispatch(overrides = {}) {
    const event = { target, deltaX: 0, deltaY: 100, deltaMode: 0, ctrlKey: false, metaKey: false, shiftKey: false,
      clientX: 99, clientY: 430, defaultPrevented: false, cancelable: true,
      preventDefault() { this.defaultPrevented = true; }, ...overrides };
    api.onWheel(event); return event;
  }
  return { api, state, viewport, target, original, dispatch };
};

test('wheel and trackpad pan in screen pixels without changing scale or moving the anchor by scale-dependent distances', () => {
  for (const scale of [.35, .7, 1, 2]) {
    const f = fixture({ camera: { x: 140, y: -90, scale } });
    const point = { x: 200, y: 150 }, view = { width: 390, height: 620 };
    const before = fieldWorldToScreen(point, f.state.camera, view);
    assert.equal(f.dispatch({ deltaX: 48, deltaY: -84 }).defaultPrevented, true);
    const after = fieldWorldToScreen(point, f.state.camera, view);
    near(after.x - before.x, -48); near(after.y - before.y, 84);
    assert.equal(f.state.camera.scale, scale); assert.equal(f.state.changes, 1);
  }
});

test('line/page wheel modes use their specified units and Shift reroutes only an otherwise vertical gesture', () => {
  for (const [deltaMode, factor] of [[1, 16], [2, 620]]) {
    const f = fixture(); f.dispatch({ deltaMode, deltaX: 2, deltaY: 3 });
    near(f.state.camera.x, 140 + 2 * factor / .7); near(f.state.camera.y, -90 + 3 * factor / .7);
  }
  const shifted = fixture(); shifted.dispatch({ deltaY: 30, shiftKey: true });
  near(shifted.state.camera.x, 140 + 30 / .7); near(shifted.state.camera.y, -90);
  const diagonal = fixture(); diagonal.dispatch({ deltaX: 20, deltaY: 30, shiftKey: true });
  near(diagonal.state.camera.x, 140 + 20 / .7); near(diagonal.state.camera.y, -90 + 30 / .7);
});

test('Ctrl/Meta wheel keep the world point exactly under the pointer through zoom and its limits', () => {
  for (const modifier of ['ctrlKey', 'metaKey']) for (const deltaY of [-100, 100, -100000, 100000]) {
    const f = fixture(), view = { width: 390, height: 620 };
    // At the extreme minimum scale an off-centre anchor can exceed the existing
    // +/-3M world-coordinate safety bound. Test scale limits at the centre and
    // an off-centre anchor for ordinary zoom, rather than ignoring that bound.
    const anchor = Math.abs(deltaY) > 100 ? { x: 195, y: 310 } : { x: 89, y: 410 };
    const before = fieldScreenToWorld(anchor, f.state.camera, view);
    assert.equal(f.dispatch({ [modifier]: true, deltaY, clientX: anchor.x + 10, clientY: anchor.y + 20 }).defaultPrevented, true);
    const after = fieldScreenToWorld(anchor, f.state.camera, view);
    near(after.x, before.x); near(after.y, before.y);
    assert.ok(f.state.camera.scale >= .00001 && f.state.camera.scale <= 2);
  }
});

test('native inputs and move/inspect controls retain wheel handling, including gestures starting on their icon descendants', () => {
  const f = fixture(), control = new FakeElement(); control.native = true; control.parentElement = f.viewport; f.target.parentElement = control;
  for (const ctrlKey of [false, true]) assert.equal(f.dispatch({ ctrlKey }).defaultPrevented, false);
  assert.equal(f.state.changes, 0);
});

test('a nested horizontal table or vertical panel keeps its scrolling even at its boundary', () => {
  for (const axis of ['X', 'Y']) {
    const f = fixture(), panel = new FakeElement(); panel[`overflow${axis}`] = 'auto';
    panel[axis === 'X' ? 'scrollWidth' : 'scrollHeight'] = 1500; panel.parentElement = f.viewport; f.target.parentElement = panel;
    for (const ctrlKey of [false, true]) assert.equal(f.dispatch({ ctrlKey }).defaultPrevented, false);
    assert.equal(f.state.changes, 0);
  }
});

test('a short window gives the enclosing page its vertical scroll, while field zoom and horizontal pan stay available', () => {
  const f = fixture(), parent = new FakeElement(); parent.overflowY = 'auto'; parent.scrollHeight = 1500; f.viewport.parentElement = parent;
  assert.equal(f.dispatch().defaultPrevented, false); assert.equal(f.state.changes, 0);
  assert.equal(f.dispatch({ deltaX: 25, deltaY: 0 }).defaultPrevented, true);
  assert.equal(f.dispatch({ ctrlKey: true }).defaultPrevented, true);
  assert.equal(f.state.changes, 2);
});

test('root document scrolling is respected and hidden overflow is not mistaken for a scrollable page', () => {
  for (const overflowY of ['visible', 'auto', 'hidden', 'clip']) {
    const f = fixture(), root = new FakeElement(); root.overflowY = overflowY; root.scrollHeight = 1500;
    f.viewport.parentElement = root; f.viewport.ownerDocument.scrollingElement = root;
    assert.equal(f.dispatch().defaultPrevented, ['hidden', 'clip'].includes(overflowY));
  }
});

test('a stationary pointer, active drag or pinch cannot have its anchor changed by simultaneous wheel input', () => {
  for (const state of [{ gesture: { type: 'press' } }, { gesture: { type: 'pan' } }, { pinch: { distance: 100 } }]) {
    const f = fixture(state);
    for (const ctrlKey of [false, true]) assert.equal(f.dispatch({ ctrlKey }).defaultPrevented, false);
    assert.equal(f.state.changes, 0);
  }
});

test('handled, non-cancelable, zero and invalid wheel samples leave camera and native defaults untouched', () => {
  for (const input of [{ defaultPrevented: true }, { cancelable: false }, { deltaX: 0, deltaY: 0 }, { deltaY: NaN }, { deltaX: Infinity }, { deltaMode: 3 }]) {
    const f = fixture(), event = f.dispatch(input);
    assert.equal(f.state.changes, 0); assert.equal(event.defaultPrevented, !!input.defaultPrevented);
    assert.deepEqual(f.state.camera, f.original);
  }
});

test('field arrows retain selection while Alt+Left/Right and browser zoom shortcuts are not consumed', () => {
  const f = fixture();
  const key = (value, overrides = {}) => ({ target: f.target, key: value, shiftKey: false, altKey: false, ctrlKey: false, metaKey: false,
    defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, ...overrides });
  for (const value of ['ArrowLeft', 'ArrowRight']) {
    const event = key(value, { altKey: true }); f.api.onKey(event); assert.equal(event.defaultPrevented, false);
  }
  for (const value of ['+', '-', '0']) {
    const event = key(value, { ctrlKey: true }); f.api.onKey(event); assert.equal(event.defaultPrevented, false);
  }
  assert.equal(f.state.focused, undefined);
  const ordinary = key('ArrowRight'); f.api.onKey(ordinary); assert.equal(ordinary.defaultPrevented, true); assert.equal(f.state.focused, 'next');
  const undo = key('z', { ctrlKey: true }); f.api.onKey(undo); assert.equal(undo.defaultPrevented, true); assert.equal(f.state.undos, 1);
});

test('tap pan controller moves the view in all four directions by a useful screen distance and tolerates the pre-ready screen', () => {
  for (const [x, y] of [[-1, 0], [0, -1], [0, 1], [1, 0]]) {
    const f = fixture(), element = { clientWidth: 320, clientHeight: 280 };
    const camera = { x: 100, y: 50, scale: .5 };
    let result = null;
    f.state.engine = null; // The real pan callback also tolerates the pre-ready screen.
    f.api.panField(x, y);
    const withEngine = fixture({ engine: { element, getCamera: () => camera, setCamera: value => { result = value; } } });
    withEngine.api.panField(x, y);
    near(result.x, camera.x + x * 128 / .5); near(result.y, camera.y + y * 112 / .5);
    assert.equal(result.scale, .5);
  }
});
