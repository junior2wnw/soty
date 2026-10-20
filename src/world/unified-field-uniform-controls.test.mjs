import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';
import vm from 'node:vm';

const root = process.env.SOTY_REVIEW_FIELD_ROOT
  ? pathToFileURL(`${resolve(process.env.SOTY_REVIEW_FIELD_ROOT)}${sep}`) : new URL('../../', import.meta.url);
const ts = createRequire(new URL('package.json', root))('typescript');
const geometry = await import(new URL('src/world/unified-field-layout.mjs', root));
const camera = await import(new URL('src/world/unified-field-camera.mjs', root));
const { resolveAppArt } = await import(new URL('src/world/app-art.mjs', root));
const { fieldEntityKey } = await import(new URL('modules/field/contract.mjs', root));
const source = readFileSync(process.env.SOTY_REVIEW_FIELD_SOURCE || new URL('src/world/unified-field.ts', root), 'utf8');
const parsed = ts.createSourceFile('unified-field.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const named = (nodes, name) => { const node = nodes.find(item => ts.isFunctionDeclaration(item) && item.name?.text === name); assert.ok(node, name); return node; };
const engine = named(parsed.statements, 'createUnifiedField');
const compile = value => ts.transpileModule(value, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const compiled = compile([named(parsed.statements, 'compactOverviewTitles'), ...['renderNode', 'render', 'focusPreview'].map(name => named(engine.body.statements, name))].map(print).join('\n')
  + '\nexports.titles = compactOverviewTitles; exports.render = render;');

// Reuse the maintained actual-render fixture through AST declarations. Do not
// execute that test file or replace production geometry/branch decisions.
const fixtureSource = readFileSync(process.env.SOTY_REVIEW_RENDER_FIXTURE_SOURCE || new URL('src/world/unified-field-readability.test.mjs', root), 'utf8');
const fixtureParsed = ts.createSourceFile('readability.mjs', fixtureSource, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
const fixturePrint = node => printer.printNode(ts.EmitHint.Unspecified, node, fixtureParsed);
const fixtureNodes = ['Port', 'app', 'specimen', 'harness'].map(name => {
  const node = fixtureParsed.statements.find(item => (ts.isClassDeclaration(item) || ts.isFunctionDeclaration(item)) && item.name?.text === name
    || ts.isVariableStatement(item) && item.declarationList.declarations.some(declaration => ts.isIdentifier(declaration.name) && declaration.name.text === name));
  assert.ok(node, `maintained actual-render fixture ${name}`); return node;
});
const fixtureContext = vm.createContext({ exports: {}, vm, compiled, ...geometry, ...camera, resolveAppArt, fieldEntityKey, AbortController, Date, performance, clearTimeout });
new vm.Script(compile(fixtureNodes.map(fixturePrint).join('\n') + '\nexports.harness = harness;')).runInContext(fixtureContext);
const harness = fixtureContext.exports.harness;
const near = (a, b, message) => assert.ok(Math.abs(a - b) < 1e-7, `${message}: ${a} != ${b}`);
const one = (kind, scale) => {
  const h = harness(1280, 800, { scale, focus: 'only' });
  const document = { schema: 'soty.field.v1', contexts: [{ contextId: 'only', title: 'Only', x: 0, y: 0 }],
    shortcuts: [{ shortcutId: 'single', entity: { kind, id: 'synthetic-entity' }, contextId: 'only', slot: [0, 0] }] };
  h.data.document = document; h.data.entities = [{ entity: document.shortcuts[0].entity, title: 'Same label', source: 'owner' }];
  h.data.layout = geometry.layoutUnifiedField(document, h.data.entities);
  Object.assign(h.camera, { x: 0, y: 0, scale }); h.environment.index = geometry.createFieldSpatialIndex(h.data.layout.nodes);
  const before = JSON.stringify(document); h.render();
  assert.equal(JSON.stringify(document), before, 'render cannot rewrite saved slots');
  assert.equal(h.calls.activate.length, 0); assert.equal(h.calls.inspect.length, 0);
  const element = h.environment.nodeElements.get('single'), action = h.environment.inspectElements.get('single');
  assert.ok(element && action, `${kind} must have actual node and accessible action`);
  return { h, element, action };
};

for (const scale of [.3, .5, 1.18]) test(`actual inspector stays attached to the same hex for every kind at scale ${scale}`, () => {
  const reference = one('app', scale);
  for (const kind of ['person', 'device', 'builtin', 'community']) {
    const actual = one(kind, scale);
    for (const key of ['left', 'top', 'width', 'height']) near(parseFloat(actual.element.style[key]), parseFloat(reference.element.style[key]), `${kind} node ${key}`);
    // Metamorphic expectation: the same visible shape/centre has the same
    // control attachment. No production capsule coordinate formula is copied.
    for (const key of ['left', 'top']) near(parseFloat(actual.action.style[key]), parseFloat(reference.action.style[key]), `${kind} action ${key}`);
    assert.equal(actual.action.getAttribute('aria-label'), 'Действия: Same label');
    actual.action.dispatch('click'); assert.equal(actual.h.calls.inspect.length, 1); assert.equal(actual.h.calls.inspect[0][1], 'single');
    assert.equal(actual.h.calls.activate.length, 0);
  }
});

for (const scale of [.3, .5, 1.18]) test(`actual overview title occupancy is kind-independent for common hex geometry at scale ${scale}`, () => {
  const h = harness(393, 900, { scale }); Object.assign(h.camera, { x: 0, y: 0, scale });
  const placements = (kind, x, y) => {
    const document = { schema: 'soty.field.v1', contexts: [{ contextId: 'first', title: 'First', x: 0, y: 0 }, { contextId: 'other', title: 'Neighbour', x, y }],
      shortcuts: [{ shortcutId: 'anchor', entity: { kind: 'app', id: 'anchor' }, contextId: 'first', slot: [0, 0] },
        { shortcutId: 'obstacle', entity: { kind, id: 'obstacle' }, contextId: 'other', slot: [0, 0] }] };
    const layout = geometry.layoutUnifiedField(document, document.shortcuts.map(item => ({ entity: item.entity, title: 'Same label' })));
    const before = JSON.stringify([document, h.camera]);
    const result = h.titles(layout.contexts, layout.nodes, h.camera, h.view, geometry.UNIFIED_FIELD_METRICS.contourPadding);
    assert.equal(result.size, 2); assert.equal(JSON.stringify([document, h.camera]), before, 'helper is read-only');
    return JSON.stringify(Array.from(result));
  };
  // Include constrained and overlapping legacy layouts: a kind substitution
  // cannot change title collision policy when centres and footprints coincide.
  for (const x of [-240, -120, 0, 120, 240]) for (const y of [-600, -400, -270, -180, 0, 180, 270]) {
    assert.equal(placements('device', x, y), placements('app', x, y), `same footprint, ${scale}, obstacle ${x},${y}`);
  }
});
