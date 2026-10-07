import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import * as contract from '../../modules/field/contract.mjs';
import * as camera from './unified-field-camera.mjs';
import * as layout from './unified-field-layout.mjs';
import { createFieldHistory } from './unified-field-state.mjs';

const source = readFileSync(new URL('./unified-field.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter();
const print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const namedFunction = (nodes, name) => {
  const result = nodes.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name);
  assert.ok(result, `Actual production function ${name} must exist`);
  return result;
};
const production = namedFunction(parsed.statements, 'createUnifiedField');
const statements = [...production.body.statements];
const declaration = name => statements.find(node => ts.isVariableStatement(node)
  && node.declarationList.declarations.some(item => ts.isIdentifier(item.name) && item.name.text === name));
const from = statements.indexOf(declaration('viewState'));
const to = statements.indexOf(namedFunction(statements, 'summary'));
assert.ok(from >= 0 && to > from);
const variables = statements.slice(from, to).filter(ts.isVariableStatement);
const returned = statements.find(ts.isReturnStatement)?.expression;
assert.ok(returned && ts.isObjectLiteralExpression(returned));
const update = returned.properties.find(node => ts.isMethodDeclaration(node) && node.name.getText(parsed) === 'update');
assert.ok(update, 'The public production update method must exist');
const resize = declaration('resize')?.declarationList.declarations[0].initializer;
assert.ok(resize && ts.isNewExpression(resize) && ts.isArrowFunction(resize.arguments?.[0]));

// Execute the production state closure, update method and ResizeObserver callback.
// Only DOM rendering/event dispatch are stubbed; layout, contracts and camera math
// remain real. This avoids copying the conditional which this regression tests.
const closureSource = [
  print(parsed.statements.find(node => ts.isVariableStatement(node)
    && node.declarationList.declarations.some(item => item.name.getText(parsed) === 'same'))),
  print(namedFunction(parsed.statements, 'thinEntities')),
  'function createStateHarness(options) {',
  print(declaration('accountId')),
  ...variables.map(print),
  ...['summary', 'emit', 'announce', 'remember', 'setCamera', 'fitOverview', 'focusContext', 'rebuild', 'cancel', 'finishGesture']
    .map(name => print(namedFunction(statements, name))),
  print(declaration('lastWidth')),
  `const onResize = ${print(resize.arguments[0])};`,
  `const api = { ${print(update)} };`,
  'rebuild();',
  'return { ...api, state: summary, fitOverview, focusContext,',
  'resize(width, height) { viewport.clientWidth = width; viewport.clientHeight = height; onResize(); },',
  'get initialized() { return { ...initialized }; } };',
  '}',
  'exports.createStateHarness = createStateHarness;',
].join('\n');
const compiled = ts.transpileModule(closureSource, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;

function harness(options, width = 1280, height = 800) {
  const viewport = { clientWidth: width, clientHeight: height, hasPointerCapture: () => false };
  const context = vm.createContext({ exports: {}, ...contract, ...camera, ...layout, createFieldHistory,
    structuredClone, performance, clearTimeout,
    viewport, plane: { contains: () => false }, document: { activeElement: null }, HTMLElement: class {},
    live: {}, render() {}, schedule() {}, flushPointerSample() {},
  });
  new vm.Script(compiled).runInContext(context);
  return context.exports.createStateHarness(options);
}

const documentFixture = () => ({ schema: 'soty.field.v1', contexts: [
  { contextId: 'studio', title: 'Студия', x: 0, y: 0 },
  { contextId: 'personal', title: 'Личное', x: 900, y: 0 },
], shortcuts: [
  { shortcutId: 'studio-note', entity: { kind: 'builtin', id: 'notes' }, contextId: 'studio', slot: [0, 0] },
  { shortcutId: 'hive-placement', entity: { kind: 'app', id: 'hive' }, contextId: 'personal', slot: [0, 0] },
] });
const entities = [
  { entity: { kind: 'builtin', id: 'notes' }, title: 'Заметки' },
  { entity: { kind: 'app', id: 'hive' }, title: 'HIVE' },
];
const searchDocument = { contexts: [{ contextId: 'search-apps', title: 'Приложения', x: 0, y: 0 }],
  shortcuts: [{ shortcutId: 'public-hive', entity: { kind: 'app', id: 'hive' }, contextId: 'search-apps', slot: [0, 0] }] };
const options = (viewState, mode = 'search') => ({ accountId: 'synthetic-account', document: documentFixture(), entities, viewState, mode });
const choice = () => ({ mineContext: 'personal', mineFit: 'context', mineSelection: '' });
const assertPersonal = (engine, viewState) => {
  assert.equal(engine.state().mode, 'mine');
  assert.equal(engine.state().focusContextId, 'personal');
  assert.equal(viewState.mineContext, 'personal');
  assert.equal(viewState.mineFit, 'context');
  assert.ok(Number.isFinite(viewState.mine?.x));
};

test('first desktop Mine visit after World placement honors the chosen space without a previous Mine camera', () => {
  const viewState = choice(), engine = harness(options(viewState));
  engine.update({ searchDocument, scope: 'app|HIVE' });
  assert.equal(viewState.mine, undefined);
  assert.equal(engine.initialized.mine, false);
  engine.update({ mode: 'mine', filter: '' });
  assertPersonal(engine, viewState);
  assert.equal(engine.state().document.shortcuts.find(item => item.entity.id === 'hive').contextId, 'personal');
  engine.update({ mode: 'search', searchDocument, scope: 'app|HIVE' });
  engine.update({ mode: 'mine' });
  assertPersonal(engine, viewState);
});

test('direct desktop reload restores a selected space when only the durable preference exists', () => {
  const viewState = choice(), engine = harness(options(viewState, 'mine'));
  assertPersonal(engine, viewState);
  const reloadedState = { mineContext: viewState.mineContext, mineFit: 'context' };
  const reloaded = harness(options(reloadedState, 'mine'));
  assertPersonal(reloaded, reloadedState);
});

test('metadata refresh, remote document and phone-to-desktop resize preserve the selected space', () => {
  const viewState = choice(), engine = harness(options(viewState));
  engine.update({ mode: 'mine' });
  const before = structuredClone(engine.state().document);
  engine.update({ entities: entities.map(item => ({ ...item, description: 'Обновление каталога' })) });
  engine.resize(320, 640);
  assertPersonal(engine, viewState);
  engine.resize(1280, 800);
  assertPersonal(engine, viewState);
  const remote = structuredClone(before);
  remote.contexts[0].title = 'Студия после синхронизации';
  engine.update({ document: remote, revision: 2, acceptRemote: true });
  assertPersonal(engine, viewState);
  assert.deepEqual(engine.state().document, remote);
  assert.deepEqual(engine.state().document.shortcuts, before.shortcuts);
});

test('initial phone focus honors the chosen space instead of the first default context', () => {
  const viewState = choice(), engine = harness(options(viewState), 320, 640);
  engine.update({ searchDocument, scope: 'app|HIVE' });
  engine.update({ mode: 'mine' });
  assertPersonal(engine, viewState);
});

test('no selection and an explicitly requested overview remain overview on desktop', () => {
  for (const viewState of [{}, { mineContext: '', mineFit: 'overview' }]) {
    const engine = harness(options(viewState));
    engine.update({ mode: 'mine' });
    assert.equal(engine.state().focusContextId, '');
    assert.equal(viewState.mineFit, 'overview');
    engine.focusContext('personal');
    engine.fitOverview();
    engine.update({ mode: 'search', searchDocument });
    engine.update({ mode: 'mine' });
    assert.equal(engine.state().focusContextId, '');
    assert.equal(viewState.mineContext, '');
  }
});

test('a removed space falls back safely and cannot leave a phantom selection', () => {
  const viewState = choice();
  const remaining = documentFixture();
  remaining.contexts = remaining.contexts.filter(item => item.contextId !== 'personal');
  remaining.shortcuts = remaining.shortcuts.filter(item => item.contextId !== 'personal');
  const engine = harness({ ...options(viewState), document: remaining });
  engine.update({ mode: 'mine' });
  assert.equal(engine.state().focusContextId, '');
  assert.equal(viewState.mineContext, '');
  assert.equal(viewState.mineFit, 'overview');
  assert.equal(engine.state().document.contexts.length, 1);
});
