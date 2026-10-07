import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import { fieldEntityKey } from '../../modules/field/contract.mjs';
import * as contract from '../../modules/field/contract.mjs';
import * as camera from './unified-field-camera.mjs';
import * as layout from './unified-field-layout.mjs';
import { createFieldHistory } from './unified-field-state.mjs';

// Execute current production empty/load/persistence functions. Deferred ports
// exercise the generation fence; no server request or display authority is real.
const source = readFileSync(new URL('./unified-field-screen.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field-screen.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const production = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedFieldScreen');
const printer = ts.createPrinter();
const functions = ['merge', 'updateMineFilter', 'updateMineEmpty', 'loadMine', 'applyPersistence'].map(name => {
  const found = production.body.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name);
  assert.ok(found, `Actual production ${name} exists`); return printer.printNode(ts.EmitHint.Unspecified, found, parsed);
});
const compiled = ts.transpileModule(functions.join('\n') + '\nObject.assign(exports, { loadMine, updateMineFilter, updateMineEmpty, applyPersistence });', {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
const ref = { kind: 'app', id: 'app-' + 'a'.repeat(32) }, item = { entity: ref, title: 'HIVE', source: 'owner' };
const document = () => ({ schema: 'soty.field.v1', contexts: [{ contextId: 'studio', title: 'Студия', x: 0, y: 0 }],
  shortcuts: [{ shortcutId: 'hive', entity: ref, contextId: 'studio', slot: [0, 0] }] });
const deferred = () => { let resolve, reject; const promise = new Promise((done, fail) => { resolve = done; reject = fail; }); return { promise, resolve, reject }; };
const resolved = (items = [item], unavailable = [], errors = []) => ({ items, unavailable, errors });
const page = (items = [item]) => ({ items, cursor: null, status: 'ready', errors: [] });
function harness({ query = 'HIVE', resolve = async () => resolved(), load = async () => page() } = {}) {
  let liveGeneration = 1, disposed = false, currentDocument = document();
  const state = { updates: [], empties: [], pages: 0, runs: [], mounts: 0 };
  const environment = { exports: {}, mode: 'mine', queries: { mine: query }, filter: 'all',
    mineGeneration: 0, mineResolvedGeneration: -1, mineConfirmedGeneration: -1, mineKnownRefs: new Set(), mineFilterAdmitted: false,
    emptyDocument: null, emptyScope: '', emptyMetadataRevision: -1, metadataRevision: 0,
    metadata: new Map([[fieldEntityKey(ref), { entity: ref, title: 'Проверяем доступ', source: 'owner' }]]),
    current: () => !disposed && liveGeneration === 1,
    element: { dataset: { ready: 'loading' } }, loading: { hidden: false },
    empty: { hidden: true, replaceChildren(...nodes) { state.emptyChildren = nodes; state.empties.push(nodes[0]?.textContent); } },
    selected: null, summary: null, firstResolve: false, wide: { matches: false }, BUILTINS: [], available: [], availableCursor: null,
    fieldEntityKey, matches: (value, text, kind = 'all') => (kind === 'all' || value.entity.kind === kind) && value.title.toLowerCase().includes(text.trim().toLowerCase()),
    el: (_, __, text) => ({ textContent: text }), button: (text, _, __, onClick) => ({ textContent: text, onClick }),
    directory: { resolve, async loadMine() { state.pages++; return load(); } },
    pruneMetadata() {}, allMetadata() { return [...environment.metadata.values()]; },
    updateMineFilter() { environment.exports.updateMineEmpty(currentDocument); },
    updateStatus() {}, closePreview() { state.previewClosed = (state.previewClosed ?? 0) + 1; environment.selected = null; }, openPreview() {},
    run(promise) { state.runs.push(promise); },
    mountEngine() { state.mounts++; environment.engine = engine; environment.exports.updateMineEmpty(currentDocument); },
  };
  const engine = { snapshot: () => currentDocument, hasUnsavedChanges: () => false,
    update(value) { if (value.document) currentDocument = value.document; state.updates.push(value); environment.exports.updateMineEmpty(currentDocument); },
    fitOverview() { throw new Error('Camera must stay intact'); }, focusContext() { throw new Error('Camera must stay intact'); },
  };
  environment.engine = engine; vm.createContext(environment); new vm.Script(compiled).runInContext(environment);
  return { environment, state, api: environment.exports,
    accountAba() { liveGeneration = 2; liveGeneration = 3; }, dispose() { disposed = true; },
    get document() { return currentDocument; } };
}

// Same actual state closure/update/rebuild used by unified-field-focus.test.
// Rendering is inert, while screen filtering and camera/intent state are real.
const engineText = readFileSync(new URL('./unified-field.ts', import.meta.url), 'utf8');
const engineParsed = ts.createSourceFile('unified-field.ts', engineText, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const named = (nodes, name) => { const value = nodes.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name); assert.ok(value, name); return value; };
const engineProduction = named(engineParsed.statements, 'createUnifiedField'), statements = [...engineProduction.body.statements];
const declaration = name => statements.find(node => ts.isVariableStatement(node) && node.declarationList.declarations.some(item => ts.isIdentifier(item.name) && item.name.text === name));
const enginePrint = node => printer.printNode(ts.EmitHint.Unspecified, node, engineParsed);
const variables = statements.slice(statements.indexOf(declaration('viewState')), statements.indexOf(named(statements, 'summary'))).filter(ts.isVariableStatement);
const engineReturn = statements.find(ts.isReturnStatement).expression;
const engineUpdate = engineReturn.properties.find(node => ts.isMethodDeclaration(node) && node.name.getText(engineParsed) === 'update');
const stateClosure = [enginePrint(engineParsed.statements.find(node => ts.isVariableStatement(node) && node.declarationList.declarations.some(item => item.name.getText(engineParsed) === 'same'))),
  enginePrint(named(engineParsed.statements, 'thinEntities')), 'function stateHarness(options) {', enginePrint(declaration('accountId')), ...variables.map(enginePrint),
  ...['summary', 'emit', 'announce', 'remember', 'setCamera', 'fitOverview', 'focusContext', 'rebuild', 'cancel', 'finishGesture'].map(name => enginePrint(named(statements, name))),
  `const api = { ${enginePrint(engineUpdate)} };`, 'rebuild(); return { ...api, state: summary, snapshot: () => structuredClone(fieldDocument), getLayout: () => layout, fitOverview, setCamera, focusContext, hasUnsavedChanges: () => false };',
  '}', 'exports.stateHarness = stateHarness;'].join('\n');
const engineCompiled = ts.transpileModule(stateClosure, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
function withField(h, viewState = { mineContext: 'studio', mineFit: 'context' }) {
  const port = vm.createContext({ exports: {}, ...contract, ...camera, ...layout, createFieldHistory, structuredClone, performance, clearTimeout,
    viewport: { clientWidth: 393, clientHeight: 450, hasPointerCapture: () => false }, plane: { contains: () => false }, document: { activeElement: null }, HTMLElement: class {},
    live: {}, render() {}, schedule() {}, flushPointerSample() {} });
  new vm.Script(engineCompiled).runInContext(port);
  const engine = port.exports.stateHarness({ accountId: 'offline-back-fixture', document: h.document, mode: 'mine', entities: [...h.environment.metadata.values()], viewState });
  h.environment.engine = engine; return { engine, viewState };
}

test('Back with a HIVE query never reports no results before current metadata resolves', async () => {
  const pending = deferred(), h = harness({ resolve: () => pending.promise });
  h.api.updateMineEmpty(h.document); assert.equal(h.environment.empty.hidden, true); assert.equal(h.state.empties.length, 0);
  const load = h.api.loadMine(); h.api.updateMineEmpty(h.document);
  assert.equal(h.environment.element.dataset.mineState, 'loading'); assert.equal(h.environment.empty.hidden, true);
  assert.equal(h.environment.metadata.get(fieldEntityKey(ref)).title, 'Проверяем доступ');
  pending.resolve(resolved()); await load;
  assert.equal(h.environment.loading.hidden, true); assert.equal(h.environment.element.dataset.mineState, 'ready');
  assert.equal(h.environment.empty.hidden, true); assert.equal(h.state.empties.length, 0);
});

test('a genuine no-match result appears after current names settle rather than suppressing empty states', async () => {
  const h = harness({ query: 'Несуществующее' }); await h.api.loadMine();
  assert.equal(h.environment.empty.hidden, false); assert.equal(h.state.empties.at(-1), 'Ничего не найдено');
});

test('an older reentrant resolve cannot replace newer names or settle the newer loading generation', async () => {
  const a = deferred(), b = deferred(); let request = 0;
  const h = harness({ resolve: () => ++request === 1 ? a.promise : b.promise, load: async () => page([]) });
  const older = h.api.loadMine(), newer = h.api.loadMine();
  b.resolve(resolved([{ ...item, title: 'HIVE · новый' }])); await newer;
  const count = h.state.updates.length;
  a.resolve(resolved([{ ...item, title: 'Чужой старый ответ' }])); await older;
  assert.equal(h.state.updates.length, count); assert.equal(h.state.pages, 1);
  assert.equal(h.environment.mineResolvedGeneration, 2); assert.equal(h.environment.metadata.get(fieldEntityKey(ref)).title, 'HIVE · новый');
  assert.equal(h.environment.empty.hidden, true); assert.equal(h.environment.element.dataset.mineState, 'ready');
});

test('account ABA or disposal rejects late private names and performs no display update', async () => {
  for (const stop of ['accountAba', 'dispose']) {
    const pending = deferred(), h = harness({ resolve: () => pending.promise });
    const load = h.api.loadMine(); h[stop](); pending.resolve(resolved()); await load;
    assert.equal(h.state.updates.length, 0); assert.equal(h.state.pages, 0); assert.equal(h.environment.mineResolvedGeneration, -1);
    assert.equal(h.environment.metadata.get(fieldEntityKey(ref)).title, 'Проверяем доступ'); assert.equal(h.state.empties.length, 0);
  }
});

test('uncertain access retains unknown names without fake no-results or a stuck loading overlay', async () => {
  const h = harness({ resolve: async () => resolved([], [ref], [{ source: 'apps', code: 'directory_network_unavailable' }]), load: async () => page([]) });
  await h.api.loadMine();
  assert.equal(h.environment.metadata.get(fieldEntityKey(ref)).title, 'Ждёт подключения');
  assert.equal(h.environment.mineKnownRefs.has(fieldEntityKey(ref)), false); assert.equal(h.environment.element.dataset.mineState, 'unknown');
  assert.equal(h.environment.loading.hidden, true); assert.equal(h.environment.empty.hidden, false); assert.equal(h.state.empties.at(-1), 'Доступ пока не подтверждён');
});

test('a definitive revoked ref replaces its old name and can finish the current no-match result', async () => {
  const h = harness({ resolve: async () => resolved([], [ref]), load: async () => page([]) }); await h.api.loadMine();
  assert.equal(h.environment.metadata.get(fieldEntityKey(ref)).title, 'Недоступно');
  assert.equal(h.environment.mineKnownRefs.has(fieldEntityKey(ref)), true); assert.equal(h.environment.empty.hidden, false);
});

test('a rejected current metadata request releases loading and never turns unknown into an empty result', async () => {
  const h = harness({ resolve: async () => { throw Object.assign(new Error('Synthetic current request rejected'), { code: 'directory_account_changed' }); } });
  await assert.rejects(h.api.loadMine(), { code: 'directory_account_changed' });
  assert.equal(h.environment.loading.hidden, true); assert.equal(h.environment.element.dataset.mineState, 'error');
  h.api.updateMineEmpty(h.document); assert.equal(h.environment.empty.hidden, false); assert.equal(h.state.empties.at(-1), 'Доступ пока не подтверждён');
});

test('cached persistence cannot hide initial metadata loading or flash an empty query result while mounting', () => {
  const h = harness(); h.environment.engine = null;
  h.api.applyPersistence({ state: 'saved', document: h.document, projectedRevision: 1 });
  assert.equal(h.state.mounts, 1); assert.equal(h.environment.loading.hidden, false);
  assert.equal(h.environment.empty.hidden, true); assert.equal(h.state.empties.length, 0);
});

test('a new remote identity invalidates empty certainty before engine update and resolves in a fresh generation', async () => {
  const h = harness({ query: 'Canvas' }); await h.api.loadMine();
  assert.equal(h.environment.empty.hidden, false);
  const pending = deferred(), added = { kind: 'app', id: 'app-' + 'b'.repeat(32) };
  h.environment.directory.resolve = () => pending.promise;
  const next = structuredClone(h.document); next.shortcuts.push({ shortcutId: 'canvas', entity: added, contextId: 'studio', slot: [19, 0] });
  h.api.applyPersistence({ state: 'saved', document: next, projectedRevision: 2 });
  assert.equal(h.environment.empty.hidden, true); assert.equal(h.environment.mineResolvedGeneration, -1);
  pending.resolve(resolved([item, { entity: added, title: 'Canvas', source: 'owner' }])); await Promise.all(h.state.runs);
  assert.equal(h.environment.empty.hidden, true); assert.equal(h.environment.mineKnownRefs.has(fieldEntityKey(added)), true);
});

test('the settled unknown retry uses a fresh generation and replaces the status only after confirmed current metadata', async () => {
  const h = harness({ resolve: async () => resolved([], [ref], [{ source: 'apps', code: 'directory_network_unavailable' }]), load: async () => page([]) });
  await h.api.loadMine(); const retry = h.state.emptyChildren.find(node => node.textContent === 'Повторить');
  assert.ok(retry); assert.equal(h.environment.empty.hidden, false);
  const pending = deferred(); h.environment.directory.resolve = () => pending.promise;
  retry.onClick(); assert.equal(h.environment.empty.hidden, true); assert.equal(h.environment.element.dataset.mineState, 'loading');
  pending.resolve(resolved()); await Promise.all(h.state.runs);
  assert.equal(h.environment.mineGeneration, 2); assert.equal(h.environment.mineResolvedGeneration, 2);
  assert.equal(h.environment.empty.hidden, true); assert.equal(h.environment.metadata.get(fieldEntityKey(ref)).title, 'HIVE');
});

test('a current exceptional resolve drops an unconfirmed selected preview instead of retaining its old private name', async () => {
  const h = harness({ resolve: async () => { throw Object.assign(new Error('Synthetic unresolved selection'), { code: 'directory_network_unavailable' }); } });
  h.environment.selected = { item: { ...item, title: 'Ранее подтверждённое имя' }, shortcutId: 'hive' };
  await assert.rejects(h.api.loadMine(), { code: 'directory_network_unavailable' });
  assert.equal(h.state.previewClosed, 1); assert.equal(h.environment.selected, null);
  assert.equal(h.environment.metadata.get(fieldEntityKey(ref)).title, 'Ждёт подключения');
  assert.equal(h.environment.empty.hidden, false); assert.equal(h.state.empties.at(-1), 'Доступ пока не подтверждён');
});

test('a failed second page closes a confirmed denied preview but keeps confirmed available selection and genuine-empty certainty', async () => {
  for (const available of [true, false]) {
    const h = harness({ resolve: async () => available ? resolved() : resolved([], [ref]),
      load: async () => { throw Object.assign(new Error('Synthetic second page failure'), { code: 'directory_network_unavailable' }); } });
    h.environment.selected = { item: available ? item : { ...item, title: 'Ранее подтверждённое имя' }, shortcutId: 'hive' };
    await assert.rejects(h.api.loadMine(), { code: 'directory_network_unavailable' });
    assert.equal(h.environment.mineKnownRefs.has(fieldEntityKey(ref)), true, 'Known unavailable remains a settled ref, not display authority');
    assert.equal(h.environment.loading.hidden, true);
    assert.equal(h.state.previewClosed ?? 0, available ? 0 : 1);
    assert.equal(h.environment.selected === null, !available);
    assert.equal(h.environment.metadata.get(fieldEntityKey(ref)).title, available ? 'HIVE' : 'Недоступно');
  }
});

test('initial Back composes cachedUnknown HIVE filtering with production rebuild without erasing Studio before or after current names resolve', async () => {
  const pending = deferred(), h = harness({ resolve: () => pending.promise }), f = withField(h);
  h.api.updateMineFilter(); assert.equal(f.engine.state().focusContextId, 'studio'); assert.equal(h.environment.empty.hidden, true);
  const load = h.api.loadMine(); h.api.updateMineFilter(); assert.equal(f.engine.state().focusContextId, 'studio');
  pending.resolve(resolved()); await load;
  assert.equal(f.engine.state().focusContextId, 'studio'); assert.equal(f.viewState.mineFit, 'context');
  assert.equal(h.environment.mineFilterAdmitted, true); assert.equal(h.environment.empty.hidden, true);
  assert.equal(f.engine.getLayout().nodes.length, 1); assert.equal(f.engine.getLayout().nodes[0].entity.title, 'HIVE');
});

test('explicit All and a new manual camera chosen during pending metadata win without restoring a stale Studio snapshot', async () => {
  for (const intent of ['all', 'manual']) {
    const pending = deferred(), h = harness({ resolve: () => pending.promise }), f = withField(h);
    h.api.updateMineFilter(); const load = h.api.loadMine();
    if (intent === 'all') f.engine.fitOverview(); else f.engine.setCamera({ x: 123, y: 77, scale: .61 });
    const selected = JSON.stringify(f.engine.state().camera), context = f.engine.state().focusContextId;
    h.api.updateMineFilter(); pending.resolve(resolved()); await load;
    assert.equal(f.engine.state().focusContextId, context); assert.equal(f.viewState.mineFit, intent === 'all' ? 'overview' : 'manual');
    assert.equal(JSON.stringify(f.engine.state().camera), selected);
  }
});

test('a genuine confirmed cross-space query keeps existing compact navigation to the matching space', async () => {
  const music = { kind: 'app', id: 'app-' + 'c'.repeat(32) }, tune = { entity: music, title: 'Тавыш', source: 'owner' };
  const pending = deferred(), h = harness({ query: 'HIVE', resolve: () => pending.promise, load: async () => page([item, tune]) });
  h.document.contexts.push({ contextId: 'evening', title: 'После работы', x: 520, y: 340 });
  h.document.shortcuts.push({ shortcutId: 'music', entity: music, contextId: 'evening', slot: [0, 0] });
  h.environment.metadata.set(fieldEntityKey(music), { entity: music, title: 'Проверяем доступ', source: 'owner' });
  const f = withField(h); h.api.updateMineFilter(); assert.equal(f.engine.state().focusContextId, 'studio');
  const load = h.api.loadMine(); h.environment.queries.mine = 'Тавыш'; h.api.updateMineFilter();
  assert.equal(f.engine.state().focusContextId, 'studio'); pending.resolve(resolved([item, tune]));
  await load; assert.equal(f.engine.state().focusContextId, 'evening');
  assert.equal(f.engine.getLayout().nodes.length, 1); assert.equal(f.engine.getLayout().nodes[0].entity.title, 'Тавыш');
});

test('settled unknown or an error and retry preserve initial Studio until confirmed HIVE while releasing loading and showing honest status', async () => {
  for (const error of [false, true]) {
    const h = harness({ resolve: async () => { if (error) throw Object.assign(new Error('Synthetic initial network failure'), { code: 'directory_network_unavailable' }); return resolved([], [ref], [{ source: 'apps', code: 'directory_network_unavailable' }]); }, load: async () => page([]) }), f = withField(h);
    if (error) await assert.rejects(h.api.loadMine(), { code: 'directory_network_unavailable' }); else await h.api.loadMine();
    assert.equal(f.engine.state().focusContextId, 'studio'); assert.equal(h.environment.mineFilterAdmitted, false);
    assert.equal(h.environment.loading.hidden, true); assert.equal(h.state.empties.at(-1), 'Доступ пока не подтверждён');
    h.environment.directory.resolve = async () => resolved(); const retry = h.state.emptyChildren.find(node => node.textContent === 'Повторить'); retry.onClick();
    await Promise.all(h.state.runs); assert.equal(f.engine.state().focusContextId, 'studio'); assert.equal(h.environment.mineFilterAdmitted, true);
    assert.equal(h.environment.empty.hidden, true); assert.equal(f.engine.getLayout().nodes[0].entity.title, 'HIVE');
  }
});

test('initial admission honors the newer resolve generation and never admits a late ABA or disposed response', async () => {
  const a = deferred(), b = deferred(); let n = 0; const h = harness({ resolve: () => ++n === 1 ? a.promise : b.promise }), f = withField(h);
  const older = h.api.loadMine(), newer = h.api.loadMine(); a.resolve(resolved()); await older;
  assert.equal(h.environment.mineFilterAdmitted, false); assert.equal(f.engine.state().focusContextId, 'studio');
  b.resolve(resolved()); await newer; assert.equal(h.environment.mineResolvedGeneration, 2); assert.equal(h.environment.mineFilterAdmitted, true);
  for (const stop of ['accountAba', 'dispose']) {
    const pending = deferred(), next = harness({ resolve: () => pending.promise }), field = withField(next);
    const load = next.api.loadMine(); next[stop](); pending.resolve(resolved()); await load;
    assert.equal(next.environment.mineFilterAdmitted, false); assert.equal(field.engine.state().focusContextId, 'studio');
    assert.equal(field.engine.getLayout().nodes[0].entity.title, 'Проверяем доступ');
  }
});

test('a current known HIVE match admits real filtering despite an unrelated unknown device backend', async () => {
  const canvas = { kind: 'app', id: 'app-' + 'd'.repeat(32) }, other = { entity: canvas, title: 'Canvas', source: 'owner' }, device = { kind: 'device', id: 'unknown-device' };
  const h = harness({ resolve: async () => resolved([item, other], [device], [{ source: 'devices', code: 'directory_network_unavailable' }]), load: async () => page([item, other]) });
  h.document.shortcuts.push({ shortcutId: 'canvas', entity: canvas, contextId: 'studio', slot: [19, 0] }, { shortcutId: 'device', entity: device, contextId: 'studio', slot: [0, 19] });
  h.environment.metadata.set(fieldEntityKey(canvas), { entity: canvas, title: 'Проверяем доступ', source: 'owner' });
  h.environment.metadata.set(fieldEntityKey(device), { entity: device, title: 'Проверяем доступ', source: 'owner' });
  const f = withField(h); await h.api.loadMine();
  assert.equal(h.environment.mineKnownRefs.has(fieldEntityKey(device)), false); assert.equal(h.environment.element.dataset.mineState, 'unknown');
  assert.equal(h.environment.mineFilterAdmitted, true); assert.equal(f.engine.state().focusContextId, 'studio');
  assert.equal(f.engine.getLayout().nodes.length, 1); assert.equal(f.engine.getLayout().nodes[0].entity.title, 'HIVE'); assert.equal(h.environment.empty.hidden, true);
});

test('fresh HIVE refs admit a HIVE-only layout during or after an unrelated available-page rejection without using cached names', async () => {
  for (const inputDuringPage of [false, true]) {
    const canvas = { kind: 'app', id: 'app-' + 'e'.repeat(32) }, other = { entity: canvas, title: 'Canvas', source: 'owner' }, pendingPage = deferred();
    const h = harness({ resolve: async () => resolved([item, other]), load: () => pendingPage.promise });
    h.document.shortcuts.push({ shortcutId: 'canvas', entity: canvas, contextId: 'studio', slot: [19, 0] });
    h.environment.metadata.set(fieldEntityKey(canvas), { entity: canvas, title: 'Проверяем доступ', source: 'owner' });
    const f = withField(h); h.api.updateMineFilter(); assert.equal(f.engine.state().focusContextId, 'studio');
    assert.equal(h.environment.mineFilterAdmitted, false, 'Cached titles cannot admit a named filter');
    const load = h.api.loadMine();
    for (let turn = 0; turn < 8 && h.state.pages === 0; turn++) await new Promise(done => setImmediate(done));
    assert.equal(h.state.pages, 1);
    if (inputDuringPage) {
      h.api.updateMineFilter(); assert.equal(f.engine.state().focusContextId, 'studio');
      assert.equal(f.engine.getLayout().nodes.length, 1); assert.equal(f.engine.getLayout().nodes[0].entity.title, 'HIVE');
      assert.equal(h.environment.empty.hidden, true, 'Pending page cannot report definitive no-match');
    }
    pendingPage.reject(Object.assign(new Error('Synthetic available-page failure after confirmed refs'), { code: 'directory_network_unavailable' }));
    await assert.rejects(load, { code: 'directory_network_unavailable' });
    assert.equal(h.environment.mineKnownRefs.has(fieldEntityKey(ref)), true); assert.equal(h.environment.loading.hidden, true);
    assert.equal(h.environment.mineFilterAdmitted, true); assert.equal(f.engine.state().focusContextId, 'studio');
    assert.equal(f.engine.getLayout().nodes.length, 1); assert.equal(f.engine.getLayout().nodes[0].entity.title, 'HIVE');
  }
});

test('a newer load resets ref confirmation and an older page rejection cannot admit cached matches or stamp the current generation', async () => {
  const oldPage = deferred(), newRefs = deferred(); let resolves = 0, pages = 0;
  const h = harness({ resolve: () => ++resolves === 1 ? Promise.resolve(resolved()) : newRefs.promise,
    load: () => ++pages === 1 ? oldPage.promise : Promise.resolve(page()) }), f = withField(h);
  const older = h.api.loadMine();
  for (let turn = 0; turn < 8 && pages === 0; turn++) await new Promise(done => setImmediate(done));
  assert.equal(h.environment.mineConfirmedGeneration, 1);
  const newer = h.api.loadMine(); assert.equal(h.environment.mineConfirmedGeneration, -1);
  oldPage.reject(Object.assign(new Error('Synthetic old available-page rejection'), { code: 'directory_network_unavailable' }));
  await assert.rejects(older, { code: 'directory_network_unavailable' });
  h.api.updateMineFilter(); assert.equal(h.environment.mineFilterAdmitted, false); assert.equal(h.environment.mineConfirmedGeneration, -1);
  assert.equal(f.engine.state().focusContextId, 'studio');
  newRefs.resolve(resolved()); await newer;
  assert.equal(h.environment.mineConfirmedGeneration, 2); assert.equal(h.environment.mineFilterAdmitted, true);
  assert.equal(f.engine.getLayout().nodes.length, 1); assert.equal(f.engine.getLayout().nodes[0].entity.title, 'HIVE');
});
