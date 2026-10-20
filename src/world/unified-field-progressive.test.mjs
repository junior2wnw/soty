import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';

// Exercise the genuine screen orchestration, directory batching and field state.
// Network, rendering and browser focus are ports; this is not visual acceptance.
const sourcePath = process.argv[2] ? resolve(process.argv[2]) : fileURLToPath(new URL('./unified-field-screen.ts', import.meta.url));
const repo = process.argv[3] ? resolve(process.argv[3]) : resolve(dirname(sourcePath), '../..');
const require = createRequire(pathToFileURL(join(repo, 'package.json')));
const ts = require('typescript');
const contract = await import(pathToFileURL(join(repo, 'modules/field/contract.mjs')));
const camera = await import(pathToFileURL(join(repo, 'src/world/unified-field-camera.mjs')));
const layout = await import(pathToFileURL(join(repo, 'src/world/unified-field-layout.mjs')));
const { createFieldHistory } = await import(pathToFileURL(join(repo, 'src/world/unified-field-state.mjs')));
const { createFieldSearchPlacement } = await import(pathToFileURL(join(repo, 'src/world/unified-field-search.mjs')));
const { fieldEntityKey } = contract;
const compile = text => ts.transpileModule(text, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const parse = path => ts.createSourceFile(path, readFileSync(path, 'utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter();
const named = (nodes, name) => { const n = nodes.find(n => ts.isFunctionDeclaration(n) && n.name?.text === name); assert.ok(n, name); return n; };
const screenParsed = parse(sourcePath), screen = named(screenParsed.statements, 'createUnifiedFieldScreen');
const screenCode = compile(['merge', 'pruneMetadata', 'allMetadata', 'updateMineFilter', 'updateMineEmpty', 'loadMine', 'loadMinePass', 'openPreview', 'searchSlot', 'releaseSearchSlot', 'searchDirectory'].filter(name => name !== 'loadMinePass' || screen.body.statements.some(node => ts.isFunctionDeclaration(node) && node.name?.text === name)).map(name => printer.printNode(ts.EmitHint.Unspecified, named(screen.body.statements, name), screenParsed)).join('\n')
  + '\nObject.assign(exports, { loadMine, updateMineFilter, updateMineEmpty, searchDirectory, openPreview });');
const directoryPath = process.argv[4] ? resolve(process.argv[4]) : join(repo, 'src/world/field-directory.ts');
const directoryCode = compile(readFileSync(directoryPath, 'utf8'));
const contractCode = compile(readFileSync(join(repo, 'modules/field/contract.mjs'), 'utf8'));
const engineParsed = parse(join(repo, 'src/world/unified-field.ts')), engineSource = named(engineParsed.statements, 'createUnifiedField'), statements = [...engineSource.body.statements];
const print = node => printer.printNode(ts.EmitHint.Unspecified, node, engineParsed);
const declaration = name => statements.find(node => ts.isVariableStatement(node) && node.declarationList.declarations.some(item => ts.isIdentifier(item.name) && item.name.text === name));
const variables = statements.slice(statements.indexOf(declaration('viewState')), statements.indexOf(named(statements, 'summary'))).filter(ts.isVariableStatement);
const engineReturn = statements.find(ts.isReturnStatement).expression;
const engineUpdate = engineReturn.properties.find(node => ts.isMethodDeclaration(node) && node.name.getText(engineParsed) === 'update');
const engineCode = compile([
  print(engineParsed.statements.find(node => ts.isVariableStatement(node) && node.declarationList.declarations.some(item => item.name.getText(engineParsed) === 'same'))),
  print(named(engineParsed.statements, 'thinEntities')), 'function stateHarness(options) {', print(declaration('accountId')), ...variables.map(print),
  ...['summary', 'emit', 'announce', 'remember', 'setCamera', 'fitOverview', 'focusContext', 'rebuild', 'cancel', 'finishGesture', 'resetPointers'].map(name => print(named(statements, name))),
  `const api = { ${print(engineUpdate)} };`, 'rebuild(); return { ...api, state: summary, snapshot: () => structuredClone(fieldDocument), getLayout: () => layout, hasUnsavedChanges: () => false, setCamera, focusContext, fitOverview, getCamera: () => ({ ...camera() }) };',
  '}', 'exports.stateHarness = stateHarness;'].join('\n'));
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const settle = async () => { for (let n = 0; n < 10; n++) await new Promise(resolve => setImmediate(resolve)); };
const appRef = n => ({ kind: 'app', id: `app-${String(n).padStart(32, '0')}` });
const personRef = { kind: 'person', id: 'slow_person' }, deviceRef = { kind: 'device', id: 'device_unknown' };
const app = (id, title = 'Готовое приложение') => ({ id, name: title, ownerAccountId: 'owner', state: 'ready', createdAt: 1, updatedAt: 2, access: 'owner', canManage: true,
  entry: { appId: id, domainId: 'dom_' + 'a'.repeat(32), origin: 'https://example.invalid', path: '/' } });
const person = ref => ({ profileId: ref.id, displayName: 'Готовый человек', avatarColor: '#abcdef', revision: 1 });
const documentFor = refs => ({ schema: 'soty.field.v1', contexts: [{ contextId: 'studio', title: 'Студия', x: 0, y: 0 }],
  shortcuts: refs.map((entity, i) => ({ shortcutId: `shortcut_${i}`, entity, contextId: 'studio', slot: [i * 3, 0] })) });
function harness({ refs = [appRef(1), personRef, deviceRef], worldGate, deviceGate, pageGate, appGate, unavailable = false, worldFailure = false, mode = 'mine', searchApps = false, communitySearchGate, peopleSearchGate, searchAppGate } = {}) {
  let current = true, sourceGeneration = 1, inFlight = 0, resolveActive = 0;
  const state = { calls: [], updates: [], maxInFlight: 0, maxResolveInFlight: 0, empties: [], pages: 0, previewClosed: 0, listCloses: 0 };
  const api = { async request(op, args) {
    state.calls.push({ op, args: structuredClone(args) }); inFlight++; state.maxInFlight = Math.max(state.maxInFlight, inFlight);
    if (op.endsWith('.directory.resolve')) { resolveActive++; state.maxResolveInFlight = Math.max(state.maxResolveInFlight, resolveActive); }
    try {
      if (op.endsWith('.directory.resolve')) assert.equal(args.expectedAccountId, 'owner');
      if (op === 'apps.directory.resolve') { if (appGate) await appGate(args.appIds); return { items: args.appIds.map(id => ({ ref: { kind: 'app', id }, available: !unavailable, ...(unavailable ? {} : { app: app(id) }) })) }; }
      if (op === 'world.directory.resolve') { if (worldGate) await worldGate.promise; if (worldFailure) throw Object.assign(new Error('Synthetic offline source'), { code: 'NETWORK_ERROR' }); return { items: args.entities.map(ref => ({ ref, available: true, access: 'member', person: person(ref) })) }; }
      if (op === 'apps.devices') { if (deviceGate) await deviceGate.promise; return { devices: [] }; }
      if (op === 'contacts.list') return { contacts: [] };
      if (op === 'world.directory.search') { state.pages++; if (pageGate) await pageGate.promise; if (args.kind === 'communities' && communitySearchGate) await communitySearchGate.promise; if (args.kind === 'people' && peopleSearchGate) await peopleSearchGate.promise; return { people: [], communities: [], nextCursor: null }; }
      if (op === 'apps.directory.search') { if (searchAppGate) await searchAppGate.promise; return { apps: searchApps ? [app(appRef(1).id)] : [], nextCursor: null }; }
      throw new Error('Unexpected synthetic operation');
    } finally { inFlight--; if (op.endsWith('.directory.resolve')) resolveActive--; }
  } };
  const doc = documentFor(refs);
  let environment;
  class NodePort {
    children = []; parent = null; dataset = {}; attributes = new Map(); handlers = new Map(); isConnected = true; hidden = false;
    constructor(tag = 'div', className = '', textContent = '') { this.tagName = tag; this.className = className; this.textContent = textContent; this.classList = { add: name => { this.className += ' ' + name; } }; }
    append(...nodes) { for (const node of nodes) if (node instanceof NodePort) node.parent = this; this.children.push(...nodes); }
    prepend(...nodes) { for (const node of nodes) if (node instanceof NodePort) node.parent = this; this.children.unshift(...nodes); }
    remove() { this.isConnected = false; if (this.parent) this.parent.children = this.parent.children.filter(node => node !== this); this.parent = null; }
    contains(node) { return node === this || this.children.some(child => child instanceof NodePort && child.contains(node)); }
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    addEventListener(name, handler) { this.handlers.set(name, handler); }
    focus() { environment.document.activeElement = this; }
  }
  const node = (tag, className, textContent) => new NodePort(tag, className, textContent);
  // Production Directory, contract and field share one JS realm. These genuine
  // closures share one VM too; only the synthetic initial JSON DTO crosses in.
  environment = { ...camera, ...layout, createFieldHistory, TextEncoder, crypto, performance, clearTimeout,
    viewport: { clientWidth: 393, clientHeight: 700, hasPointerCapture: () => false }, plane: { contains: () => false }, document: { activeElement: null }, HTMLElement: NodePort, live: {}, render() {}, schedule() {}, flushPointerSample() {},
    signal: new AbortController().signal, mode, queries: { mine: '', search: '' }, filter: 'all', mineGeneration: 0, mineResolvedGeneration: -1, mineConfirmedGeneration: -1,
    mineBusy: false, queuedMine: null, mineKnownRefs: new Set(), mineFilterAdmitted: false, emptyDocument: null, emptyScope: '', emptyMetadataRevision: -1, metadataRevision: 0,
    current: () => current && sourceGeneration === 1, element: Object.assign(node('section'), { dataset: { ready: 'loading' } }), loading: { hidden: false },
    empty: { hidden: true, replaceChildren(...nodes) { state.empties.push(nodes[0]?.textContent); } },
    searchGeneration: 0, searchResponseScope: '', searchCursor: null, activeSearches: 0, queuedSearch: null, searchPlacement: createFieldSearchPlacement(),
    more: { disabled: false, hidden: true }, searchLimit: { hidden: true }, options: { onMessage() {} },
    preview: null, previewOpener: null, selected: null, summary: null, firstResolve: false, wide: { matches: false }, BUILTINS: [], available: [], availableCursor: null, searchItems: [], state: null,
    matches: (item, query, filter) => (filter === 'all' || filter === item.entity.kind) && item.title.toLowerCase().includes(query.trim().toLowerCase()),
    el: node, button: (textContent, _, className, onClick) => Object.assign(node('button', className, textContent), { onClick }),
    icon: () => node('svg'), iconButton: (label, _, onClick) => Object.assign(node('button', '', label), { onClick }),
    avatar: () => node('span', 'avatar'),
    kindLabel: kind => kind,
    closeList() { state.listCloses++; }, closePreview() { state.previewClosed++; environment.preview?.remove(); environment.preview = null; delete environment.element.dataset.preview; environment.selected = null; },
  };
  vm.createContext(environment);
  // The field's clone inputs here are strictly JSON documents/DTOs. The VM clone
  // keeps them in its realm; no product validator or incoming RPC body is relaxed.
  environment.structuredClone = vm.runInContext('(value) => JSON.parse(JSON.stringify(value))', environment);
  const module = (code, key) => {
    vm.runInContext(`globalThis.${key} = {};\n(function(exports, require) {\n${code}\n})(globalThis.${key}, globalThis.require);`, environment);
    return environment[key];
  };
  const strictContract = module(contractCode, 'contractExports'); Object.assign(environment, strictContract);
  environment.require = specifier => { assert.equal(specifier, '../../modules/field/contract.mjs'); return strictContract; };
  const directory = module(directoryCode, 'directoryExports').createFieldDirectory({ api, accountId: 'owner', isCurrent: () => current && sourceGeneration === 1 });
  environment.directory = directory;
  environment.metadata = new Map(refs.map(value => { const entity = environment.structuredClone(value); return [fieldEntityKey(entity), { entity, title: 'Загрузка…', symbol: 'lock', source: 'owner' }]; }));
  const field = module(engineCode, 'engineExports').stateHarness(environment.structuredClone({ accountId: 'owner', document: doc, mode, entities: [...environment.metadata.values()], viewState: { mine: { x: 25, y: 33, scale: .7 }, mineContext: 'studio', mineFit: 'manual' } }));
  environment.engine = { ...field, element: environment.viewport, update(value) { state.updates.push(structuredClone(value)); field.update(value); } };
  const screenApi = module(screenCode, 'screenExports');
  assert.equal(field.getLayout().nodes.length, mode === 'mine' ? refs.length : 0, 'The fixture must admit all genuine initial shortcut nodes');
  return { api: screenApi, environment, state, field, directory, doc, strictContract,
    stop(kind) { if (kind === 'ABA') { sourceGeneration = 2; sourceGeneration = 3; } else current = false; } };

}
const hasFreshApp = h => h.field.getLayout().nodes.some(n => n.entity.title === 'Готовое приложение');

test('the shared VM uses the unchanged strict contract without accepting foreign or malicious prototypes', () => {
  const h = harness({ refs: [] }), sameRealm = vm.runInContext('({ kind: "app", id: "app-' + '1'.repeat(32) + '" })', h.environment);
  assert.deepEqual(JSON.parse(JSON.stringify(h.strictContract.validateFieldEntity(sameRealm))), { kind: 'app', id: 'app-' + '1'.repeat(32) });
  assert.throws(() => h.strictContract.validateFieldEntity({ kind: 'app', id: 'app-' + '1'.repeat(32) }), { code: 'field_document_invalid' }, 'Foreign host prototypes are still rejected');
  for (const expression of ['Object.assign(Object.create({ hidden: true }), { kind: "app", id: "a" })', '({ kind: "invalid", id: "a" })', '({ kind: "app", id: "a", extra: true })', '({ kind: "app", id: () => "a" })']) {
    const malformed = vm.runInContext(expression, h.environment);
    assert.throws(() => h.strictContract.validateFieldEntity(malformed), { code: 'field_document_invalid' });
  }
});

test('a ready app renders while unrelated world and device requests remain pending, with saved slots and manual camera intact', async () => {
  const worldGate = deferred(), deviceGate = deferred(), h = harness({ worldGate, deviceGate });
  const before = JSON.stringify(h.field.snapshot()), camera = JSON.stringify(h.field.state().camera), load = h.api.loadMine();
  let early;
  try { await settle(); early = { fresh: hasFreshApp(h), overlay: h.environment.loading.hidden, settled: h.environment.mineResolvedGeneration, empty: h.environment.empty.hidden, pages: h.state.pages, inFlight: h.state.maxInFlight, camera: JSON.stringify(h.field.state().camera) }; }
  finally { worldGate.resolve(); deviceGate.resolve(); await load; }
  assert.equal(early.fresh, true, 'Application metadata must render before the unrelated sources settle');
  assert.equal(early.overlay, true); assert.equal(early.settled, -1); assert.equal(early.empty, true); assert.equal(early.pages, 0); assert.ok(early.inFlight <= 3);
  assert.equal(early.camera, camera); assert.equal(JSON.stringify(h.field.state().camera), camera); assert.equal(JSON.stringify(h.field.snapshot()), before);
  assert.ok(h.state.updates.every(value => !Object.hasOwn(value, 'document')), 'Metadata progress must not replace the durable document');
});

test('the first 60 ready app refs render before a delayed second batch of the same source', async () => {
  const last = deferred(), refs = Array.from({ length: 65 }, (_, i) => appRef(i + 1));
  let batches = 0;
  const h = harness({ refs, appGate: async ids => { if (++batches === 2) await last.promise; } }), load = h.api.loadMine();
  let known, visible, sizes;
  try { await settle(); known = h.environment.mineKnownRefs.size; visible = h.field.getLayout().nodes.filter(n => n.entity.title === 'Готовое приложение').length; sizes = h.state.calls.filter(c => c.op === 'apps.directory.resolve').map(c => c.args.appIds.length); }
  finally { last.resolve(); await load; }
  assert.equal(known, 60); assert.equal(visible, 60); assert.deepEqual(sizes, [60, 5]); assert.equal(h.environment.mineKnownRefs.size, 65); assert.ok(h.state.maxResolveInFlight <= 1, 'Batches of a single source remain sequential even when later directory sources run in parallel');
});

test('confirmed shortcut metadata renders before the slow available-directory page completes', async () => {
  const pageGate = deferred(), h = harness({ refs: [appRef(1)], pageGate }), load = h.api.loadMine();
  let early;
  try { await settle(); early = hasFreshApp(h); assert.equal(h.environment.mineResolvedGeneration, -1); }
  finally { pageGate.resolve(); await load; }
  assert.equal(early, true); assert.equal(h.environment.mineResolvedGeneration, 1);
});

test('an unrelated source failure retains a confirmed app and honest unknown metadata without deleting shortcuts', async () => {
  const h = harness({ refs: [appRef(1), personRef], worldFailure: true }), before = JSON.stringify(h.field.snapshot());
  await h.api.loadMine();
  assert.equal(hasFreshApp(h), true); assert.equal(h.environment.metadata.get(fieldEntityKey(personRef)).title, 'Ждёт подключения');
  assert.equal(h.environment.mineKnownRefs.has(fieldEntityKey(personRef)), false); assert.equal(h.environment.element.dataset.mineState, 'unknown');
  assert.equal(JSON.stringify(h.field.snapshot()), before); assert.equal(h.environment.loading.hidden, true);
});

test('a confirmed unavailable app closes only its selected preview before the other source completes', async () => {
  const worldGate = deferred(), h = harness({ refs: [appRef(1), personRef], worldGate, unavailable: true });
  h.environment.selected = { item: { entity: appRef(1), title: 'Старое имя', source: 'owner' }, shortcutId: 'shortcut_0' };
  const load = h.api.loadMine(); let early;
  try { await settle(); early = { closed: h.state.previewClosed, title: h.environment.metadata.get(fieldEntityKey(appRef(1))).title }; }
  finally { worldGate.resolve(); await load; }
  assert.equal(early.closed, 1); assert.equal(early.title, 'Недоступно'); assert.equal(h.directory.getRecord(appRef(1)), null);
});

test('a ready unrelated app does not clear the selected person while that source is still pending', async () => {
  const worldGate = deferred(), h = harness({ refs: [appRef(1), personRef], worldGate });
  h.environment.selected = { item: { entity: personRef, title: 'Готовый человек', source: 'member' }, shortcutId: 'shortcut_1' };
  const load = h.api.loadMine(); let early;
  try { await settle(); early = { selected: h.environment.selected?.shortcutId, closed: h.state.previewClosed }; }
  finally { worldGate.resolve(); await load; }
  assert.equal(early.selected, 'shortcut_1'); assert.equal(early.closed, 0);
});

test('account ABA and disposal reject late private metadata and stop further display updates', async () => {
  for (const stop of ['ABA', 'dispose']) {
    const worldGate = deferred(), appWait = deferred();
    const h = harness({ refs: [appRef(1), personRef], worldGate, appGate: () => appWait.promise });
    const load = h.api.loadMine(); await settle(); h.stop(stop); const count = h.state.updates.length;
    worldGate.resolve(); appWait.resolve(); await assert.rejects(load, { code: 'directory_account_changed' });
    await settle(); assert.equal(h.state.updates.length, count); assert.equal(h.environment.mineKnownRefs.size, 0); assert.equal(h.state.pages, 0);
  }
});


test('search paints ready apps before held communities/people and preserves its first admitted placement and camera', async () => {
  const communitySearchGate = deferred(), peopleSearchGate = deferred();
  const h = harness({ refs: [], mode: 'search', searchApps: true, communitySearchGate, peopleSearchGate });
  const pending = h.api.searchDirectory(); let early;
  try { await settle(); early = { fresh: hasFreshApp(h), state: h.environment.element.dataset.searchState, pending: h.environment.element.dataset.searchPending,
    empty: h.environment.empty.hidden, camera: JSON.stringify(h.field.state().camera), slots: JSON.stringify(h.field.getLayout().nodes.map(n => [n.shortcutId, n.contextId, n.slot])) }; }
  finally { communitySearchGate.resolve(); peopleSearchGate.resolve(); await pending; }
  assert.equal(early.fresh, true, 'Search must paint a confirmed application before unrelated sources settle');
  assert.equal(early.state, 'partial'); assert.equal(early.pending, 'true'); assert.equal(early.empty, true);
  assert.equal(h.environment.element.dataset.searchState, 'ready'); assert.equal(h.environment.element.dataset.searchPending, 'false');
  assert.equal(JSON.stringify(h.field.state().camera), early.camera); assert.equal(JSON.stringify(h.field.getLayout().nodes.map(n => [n.shortcutId, n.contextId, n.slot])), early.slots);
  assert.ok(h.state.maxInFlight <= 3); assert.ok(h.environment.searchItems.some(item => item.entity.id === appRef(1).id));
});

test('retiring a partial search rejects the late unrelated source results without another field update', async () => {
  const communitySearchGate = deferred(), peopleSearchGate = deferred();
  const h = harness({ refs: [], mode: 'search', searchApps: true, communitySearchGate, peopleSearchGate });
  const pending = h.api.searchDirectory(); await settle(); const updates = h.state.updates.length; h.stop('dispose');
  communitySearchGate.resolve(); peopleSearchGate.resolve(); await pending; await settle(); assert.equal(h.state.updates.length, updates);
});

// Run the actual final ready phase and public refresh bodies with held task ports.
// This verifies dispatch ordering, not a substitute for the full browser boot.
let readyBody;
function locateReady(node) {
  if (ts.isArrowFunction(node) && ts.isBlock(node.body) && node.body.statements.some(statement => statement.getText(screenParsed).includes('const loaded = await persistence.load()'))) readyBody = node.body;
  ts.forEachChild(node, locateReady);
}
locateReady(screen);
assert.ok(readyBody, 'Actual ready closure exists');
const readyPhase = readyBody.statements.slice(-4, -1).map(node => printer.printNode(ts.EmitHint.Unspecified, node, screenParsed)).join('\n');
const refreshMethod = screen.body.statements.find(ts.isReturnStatement).expression.properties.find(node => ts.isMethodDeclaration(node) && node.name.getText(screenParsed) === 'refresh');
assert.ok(refreshMethod, 'Actual public refresh exists');
const phases = compile('async function readyPhase() {\n' + readyPhase + '\n}\nexports.api = { readyPhase, ' + printer.printNode(ts.EmitHint.Unspecified, refreshMethod, screenParsed) + ' };');
function phaseHarness(mode) {
  const mine = deferred(), search = deferred(), calls = [];
  const context = { exports: {}, mode, ready: Promise.resolve(), current: () => true,
    loadMine: () => { calls.push(['mine']); return mine.promise; },
    searchDirectory: (...args) => { calls.push(['search', ...args]); return search.promise; } };
  vm.createContext(context); new vm.Script(phases).runInContext(context);
  return { api: context.exports.api, calls, release() { mine.resolve(); search.resolve(); } };
}

test('initial Search starts its public query without waiting for personal metadata; Mine dispatches no public query', async () => {
  for (const mode of ['search', 'mine']) {
    const h = phaseHarness(mode), pending = h.api.readyPhase(); let early;
    try { await settle(); early = [...h.calls]; } finally { h.release(); await pending; }
    assert.deepEqual(early, mode === 'search' ? [['mine'], ['search']] : [['mine']]);
  }
});

test('public Search refresh starts its current query beside personal metadata and forwards preserveView', async () => {
  const h = phaseHarness('search'), pending = h.api.refresh({ preserveView: true }); let early;
  try { await settle(); early = [...h.calls]; } finally { h.release(); await pending; }
  assert.deepEqual(early, [['mine'], ['search', false, true]]);
});


test('selecting the genuine fast-app preview survives later successful world responses in the same search', async () => {
  const communitySearchGate = deferred(), peopleSearchGate = deferred();
  const h = harness({ refs: [], mode: 'search', searchApps: true, communitySearchGate, peopleSearchGate });
  const pending = h.api.searchDirectory(); let panel, selected, focus;
  try {
    await settle(); const item = h.environment.metadata.get(fieldEntityKey(appRef(1))); assert.equal(item?.title, 'Готовое приложение');
    h.api.openPreview(item, h.field.getLayout().nodes[0].shortcutId); panel = h.environment.preview; selected = h.environment.selected; focus = h.environment.document.activeElement;
    assert.ok(panel); assert.equal(focus, panel);
  } finally { communitySearchGate.resolve(); peopleSearchGate.resolve(); await pending; }
  assert.equal(h.environment.preview, panel); assert.equal(h.environment.selected, selected); assert.equal(h.environment.document.activeElement, focus); assert.equal(panel.isConnected, true);
});

test('firstResolve respects a manual camera or genuine preview chosen after the early app batch', async () => {
  for (const intent of ['camera', 'preview']) {
    const worldGate = deferred(), h = harness({ refs: [appRef(1), personRef], worldGate }); h.environment.firstResolve = true;
    const pending = h.api.loadMine(); let camera, preview;
    try {
      await settle(); assert.equal(hasFreshApp(h), true);
      if (intent === 'camera') h.field.setCamera({ x: 511, y: -277, scale: .83 });
      else h.api.openPreview(h.environment.metadata.get(fieldEntityKey(appRef(1))), 'shortcut_0');
      camera = JSON.stringify(h.field.state().camera); preview = h.environment.preview;
    } finally { worldGate.resolve(); await pending; }
    assert.equal(JSON.stringify(h.field.state().camera), camera, 'Late initial fit must respect the newer view/selection');
    if (intent === 'preview') assert.equal(h.environment.preview, preview); assert.equal(h.environment.firstResolve, false);
  }
});

test('one active Mine pass keeps only the latest queued burst and does not dispatch superseded private reads', async () => {
  const worldGate = deferred(), deviceGate = deferred(), appsGate = deferred();
  const h = harness({ worldGate, deviceGate, appGate: () => appsGate.promise }), first = h.api.loadMine();
  await settle(); const queued = Array.from({ length: 8 }, () => h.api.loadMine()); await settle();
  let calls, peak;
  try { calls = h.state.calls.filter(c => c.op.endsWith('.directory.resolve') || c.op === 'apps.devices').length; peak = h.state.maxInFlight; }
  finally { worldGate.resolve(); deviceGate.resolve(); appsGate.resolve(); await Promise.all([first, ...queued]); }
  assert.equal(calls, 3); assert.ok(peak <= 3); assert.equal(h.environment.mineResolvedGeneration, 9); assert.equal(h.environment.mineBusy, false);
  assert.equal(h.state.calls.filter(c => c.op.endsWith('.directory.resolve')).length, 4, 'Only the first and latest passes resolve app/world refs');
  assert.ok(h.state.maxInFlight <= 3);
});

test('a fatal private resolver retains its slot until held neighboring RPCs settle, then admits the latest recovery', async () => {
  const deviceGate = deferred(), appsGate = deferred();
  const h = harness({ deviceGate, appGate: () => appsGate.promise }); let injected = false;
  h.environment.directory = { ...h.directory, async resolve(refs) { if (!injected && refs.some(ref => ref.kind === 'person')) { injected = true; throw Object.assign(new Error('Synthetic fatal resolver port'), { code: 'synthetic_fatal_resolver' }); } return h.directory.resolve(refs); } };
  let settled = false;
  const first = h.api.loadMine().then(value => { settled = true; return { value }; }, error => { settled = true; return { error }; });
  await settle(); const latest = h.api.loadMine(); await settle(); const early = { settled, calls: h.state.calls.length };
  appsGate.resolve(); deviceGate.resolve(); const outcome = await first; await latest;
  assert.equal(early.settled, false); assert.equal(early.calls, 2); assert.equal(outcome.error?.code, 'synthetic_fatal_resolver');
  assert.equal(h.environment.mineResolvedGeneration, 2); assert.equal(hasFreshApp(h), true); assert.ok(h.state.maxInFlight <= 3);
});

test('disposal with a pending Mine queue starts no new private pass after the active requests complete', async () => {
  const worldGate = deferred(), deviceGate = deferred(), appsGate = deferred();
  const h = harness({ worldGate, deviceGate, appGate: () => appsGate.promise });
  const first = h.api.loadMine().catch(error => error), queued = h.api.loadMine(); await settle(); const before = h.state.calls.length; h.stop('dispose');
  worldGate.resolve(); deviceGate.resolve(); appsGate.resolve(); await Promise.all([first, queued]); assert.equal(h.state.calls.length, before); assert.equal(h.environment.mineBusy, false);
});

test('genuine ready plus personal refs allows the public fast app at a measured field peak of at most six RPCs', async () => {
  const worldGate = deferred(), deviceGate = deferred(), appsGate = deferred(), communitySearchGate = deferred(), peopleSearchGate = deferred(), searchAppGate = deferred();
  const h = harness({ mode: 'search', searchApps: true, worldGate, deviceGate, appGate: () => appsGate.promise, communitySearchGate, peopleSearchGate, searchAppGate });
  const phase = { exports: {}, mode: 'search', current: h.environment.current, loadMine: h.api.loadMine, searchDirectory: h.api.searchDirectory };
  vm.createContext(phase); new vm.Script(phases).runInContext(phase);
  const pending = phase.exports.api.readyPhase(); let early;
  try { await settle(); const peak = h.state.maxInFlight; searchAppGate.resolve(); await settle(); early = { peak, visible: hasFreshApp(h), known: h.environment.mineKnownRefs.size }; }
  finally { worldGate.resolve(); deviceGate.resolve(); appsGate.resolve(); communitySearchGate.resolve(); peopleSearchGate.resolve(); searchAppGate.resolve(); await pending; }
  assert.equal(early.visible, true); assert.equal(early.known, 0); assert.ok(early.peak <= 6); assert.ok(early.peak >= 5, 'The fixture must actually overlap private and public calls'); assert.ok(h.state.maxInFlight <= 6);
});

test('bursts keep one private pass and two public searches bounded at nine concurrent RPCs', async () => {
  const worldGate = deferred(), deviceGate = deferred(), appsGate = deferred(), communitySearchGate = deferred(), peopleSearchGate = deferred(), searchAppGate = deferred();
  const h = harness({ mode: 'search', searchApps: true, worldGate, deviceGate, appGate: () => appsGate.promise, communitySearchGate, peopleSearchGate, searchAppGate });
  const mine = h.api.loadMine(), searches = [h.api.searchDirectory()]; await settle();
  searches.push(h.api.searchDirectory()); await settle(); for (let n = 0; n < 6; n++) searches.push(h.api.searchDirectory());
  const queuedMines = Array.from({ length: 6 }, () => h.api.loadMine()); await settle(); let peak;
  try { peak = h.state.maxInFlight; assert.equal(h.environment.activeSearches, 2); }
  finally { worldGate.resolve(); deviceGate.resolve(); appsGate.resolve(); communitySearchGate.resolve(); peopleSearchGate.resolve(); searchAppGate.resolve(); await Promise.all([mine, ...queuedMines, ...searches]); }
  assert.ok(peak <= 9); assert.ok(peak >= 8, 'The fixture must exercise the overlapping upper bound'); assert.ok(h.state.maxInFlight <= 9); assert.equal(h.environment.activeSearches, 0); assert.equal(h.environment.mineBusy, false);
});
