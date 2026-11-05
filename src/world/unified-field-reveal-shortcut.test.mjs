import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import ts from 'typescript';
import {applyFieldCommand} from './unified-field-state.mjs';
import {fieldBounds, layoutUnifiedField, UNIFIED_FIELD_METRICS} from './unified-field-layout.mjs';
import {fieldWorldToScreen, fitFieldCamera, normalizeFieldCamera} from './unified-field-camera.mjs';

// Execute actual engine functions against actual placement/layout/camera code.
// Only viewport, scheduling and persistence-notification IO are fake ports. This
// is not a DOM/browser/Source-boot or Root membership-authority acceptance claim.
const source = readFileSync(new URL('./unified-field.ts', import.meta.url), 'utf8');
const ast = ts.createSourceFile('unified-field.ts', source, ts.ScriptTarget.Latest, true);
const engine = ast.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedField');
const printer = ts.createPrinter();
const functions = ['activeNode', 'setCamera', 'focusContext', 'revealShortcut'].map(name => {
  const node = engine.body.statements.find(value => ts.isFunctionDeclaration(value) && value.name?.text === name);
  assert.ok(node, `actual engine function ${name} exists`);
  return printer.printNode(ts.EmitHint.Unspecified, node, ast);
}).join('\n');
const compiled = ts.transpileModule(functions + '\nexports.focusContext=focusContext;exports.revealShortcut=revealShortcut;', {
  compilerOptions: {target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS},
}).outputText;

function specimen() {
  let document = {schema: 'soty.field.v1', contexts: [{contextId: 'home', title: 'Личное', x: 0, y: 0}], shortcuts: []};
  const entities = [
    {entity: {kind: 'object', id: 'synthetic_counter'}, title: 'Counter', presentation: {shape: 'rect'}},
    {entity: {kind: 'object', id: 'synthetic_reading_list'}, title: 'Reading List', presentation: {shape: 'rect'}},
    {entity: {kind: 'community', id: 'synthetic_room'}, title: 'Новая комната'},
  ];
  for (const [index, point] of [[0, {x: 0, y: -20.9}], [1, {x: -668, y: -468.9}]]) {
    document = applyFieldCommand(document, {type: 'add-shortcut', contextId: 'home', shortcutId: index ? 'reading-list' : 'counter',
      entity: entities[index].entity, placement: {kind: 'free', ...point, width: 640, height: 420}}).document;
  }
  // The actual automatic slot finder puts the new community around y=276 at
  // its upper edge, beyond the two large existing Source objects.
  document = applyFieldCommand(document, {type: 'add-shortcut', contextId: 'home', shortcutId: 'new-room', entity: entities[2].entity}).document;
  return {document, entities, layout: layoutUnifiedField(document, entities)};
}

function fixture(width = 390, height = 844, scale = 2) {
  const data = specimen(), viewport = {width, height}, calls = {remember: 0, schedule: 0, emit: 0};
  const env = {exports: {}, mode: 'mine', destroyed: false, selectedId: 'reading-list', layout: data.layout,
    options: {readableMobileFocus: true}, metrics: UNIFIED_FIELD_METRICS,
    cameras: {mine: normalizeFieldCamera({x: -800, y: -600, scale})}, cameraFit: {mine: 'manual'}, initialized: {mine: true}, contextFocus: {mine: ''},
    activeLayout: () => data.layout, viewportSize: () => viewport,
    normalizeFieldCamera, fieldBounds, fitFieldCamera,
    remember() {calls.remember++;}, schedule() {calls.schedule++;}, emit() {calls.emit++;},
  };
  env.camera = () => env.cameras.mine;
  vm.runInNewContext(compiled, env);
  const target = data.layout.nodes.find(node => node.shortcutId === 'new-room');
  const rect = () => {
    const first = fieldWorldToScreen({x: target.bounds.left, y: target.bounds.top}, env.cameras.mine, viewport);
    const last = fieldWorldToScreen({x: target.bounds.right, y: target.bounds.bottom}, env.cameras.mine, viewport);
    return {left: first.x, top: first.y, right: last.x, bottom: last.y, width: last.x - first.x, height: last.y - first.y};
  };
  return {data, viewport, calls, env, target, rect, focusContext: env.exports.focusContext, revealShortcut: env.exports.revealShortcut};
}

function assertRevealed(f) {
  const rect = f.rect();
  assert.ok(rect.left >= 0 && rect.top >= 0 && rect.right <= f.viewport.width && rect.bottom <= f.viewport.height,
    `target must be fully within viewport; observed ${JSON.stringify(rect)}`);
  assert.equal(f.env.selectedId, 'new-room', 'the actual added target must be selected');
  assert.equal(f.env.contextFocus.mine, 'home');
  assert.ok(Object.values(f.env.cameras.mine).every(Number.isFinite), 'camera must remain finite');
  assert.ok(f.env.cameras.mine.scale >= .08 && f.env.cameras.mine.scale <= 2, 'scale remains usable and bounded');
  return rect;
}

test('OLD actual focusContext after successful placement leaves the new room offscreen on a 390×844 phone', t => {
  const f = fixture(), documentBefore = JSON.stringify(f.data.document);
  assert.ok(f.target, 'the placement and its allowed metadata really exist before focus');
  f.focusContext('home');
  const rect = f.rect();
  assert.ok(rect.bottom < 0 || rect.top > 844 || rect.right < 0 || rect.left > 390,
    'the actual old call must reproduce the entirely offscreen target, not merely fail the selection assertion');
  assert.throws(() => assertRevealed(f), /target must be fully within viewport/);
  assert.equal(JSON.stringify(f.data.document), documentBefore, 'the original successful placement remains unchanged in the canonical model');
  t.diagnostic(JSON.stringify({sourceSha256: createHash('sha256').update(source).digest('hex'), oldCamera: f.env.cameras.mine, targetRect: rect, target: {x: f.target.x, y: f.target.y}}));
});

for (const [width, height] of [[390, 844], [390, 400], [1280, 800]]) test(`${width}×${height}: reveal selects and centers the new provider without changing neighboring geometry`, t => {
  for (const scale of [.2, .51, 2]) {
    const f = fixture(width, height, scale), beforeDocument = JSON.stringify(f.data.document), beforeLayout = structuredClone(f.data.layout);
    f.revealShortcut('new-room');
    const rect = assertRevealed(f);
    assert.equal((rect.left + rect.right) / 2, width / 2);
    assert.equal((rect.top + rect.bottom) / 2, height / 2);
    assert.ok(f.env.cameras.mine.scale <= scale, 'revealing must not zoom past the existing usable scale');
    assert.equal(JSON.stringify(f.data.document), beforeDocument);
    assert.deepEqual(layoutUnifiedField(f.data.document, f.data.entities), beforeLayout, 'all established object dimensions/coordinates/footprints are unchanged');
    assert.deepEqual(f.calls, {remember: 1, schedule: 1, emit: 1}, 'the real camera update follows the normal existing lifecycle once');
    t.diagnostic(JSON.stringify({viewport: f.viewport, startingScale: scale, camera: f.env.cameras.mine, targetRect: rect}));
  }
});

test('unknown IDs, unavailable metadata and a destroyed engine cannot move camera or change selection', () => {
  for (const mode of ['unknown', 'unavailable', 'destroyed']) {
    const f = fixture(), before = JSON.stringify([f.env.cameras, f.env.contextFocus, f.env.selectedId, f.calls]);
    if (mode === 'destroyed') f.env.destroyed = true;
    if (mode === 'unavailable') f.data.layout.nodes = f.data.layout.nodes.filter(node => node.shortcutId !== 'new-room');
    f.revealShortcut(mode === 'unknown' ? 'missing-shortcut' : 'new-room');
    assert.equal(JSON.stringify([f.env.cameras, f.env.contextFocus, f.env.selectedId, f.calls]), before, mode);
  }
});
