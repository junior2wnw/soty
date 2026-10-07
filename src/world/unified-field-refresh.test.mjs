import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFile } from 'node:fs/promises';
import ts from 'typescript';
import { createFieldDirectory } from './field-directory.ts';
import { createFieldSearchPlacement } from './unified-field-search.mjs';
import { fieldEntityKey } from '../../modules/field/contract.mjs';
import { layoutUnifiedField, fieldBounds, rectPoints } from './unified-field-layout.mjs';
import { fitFieldCamera } from './unified-field-camera.mjs';

// Run the actual public refresh and search functions. Rendering/viewport ports
// are synthetic; directory federation, placement and camera calculations are real.
const source = await readFile(new URL('./unified-field-screen.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field-screen.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const production = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedFieldScreen');
const statements = [...production.body.statements];
const search = statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'searchDirectory');
const returned = statements.find(ts.isReturnStatement)?.expression;
const refresh = returned.properties.find(node => ts.isMethodDeclaration(node) && node.name.getText(parsed) === 'refresh');
assert.ok(search && refresh, 'actual public methods must exist');
const compiled = ts.transpileModule(print(search) + '\nexports.api = {' + print(refresh) + '};',
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const appId = 'app-' + 'a'.repeat(32), domainId = 'dom_' + 'b'.repeat(32);
const app = { id: appId, name: 'ХочуИпотеку', state: 'ready', access: 'owner', canManage: true,
  ownerAccountId: 'account-A', createdAt: 1, updatedAt: 1, entry: { appId, domainId, origin: 'https://app.fixture.invalid', path: '/' } };

function fixture({ mode = 'search', loadMine = async () => {}, beforeRead = async () => {} } = {}) {
  const calls = [], cameraCalls = [], focusCalls = [], updates = [], metadata = new Map(), exports = {}, state = { current: true };
  const directory = createFieldDirectory({ accountId: 'account-A', isCurrent: () => state.current, api: { async request(op, args) {
    calls.push({ op, args }); await beforeRead(); assert.equal(args.expectedAccountId, 'account-A');
    assert.equal(op, 'apps.directory.search'); return { apps: [app], nextCursor: null };
  } } });
  const environment = { exports, mode, filter: 'app', queries: { search: 'Хочу' }, searchGeneration: 0,
    searchResponseScope: 'Хочу:app', searchItems: [], searchCursor: null, selected: null, summary: null, contextKey: '',
    ready: Promise.resolve(), current: () => state.current, loadMine,
    searchSlot: async () => true, releaseSearchSlot() {}, directory, BUILTINS: [], matches: () => false,
    element: { dataset: {} }, more: { disabled: false, hidden: true }, empty: { hidden: false, replaceChildren() {} },
    merge(items) { for (const item of items) metadata.set(fieldEntityKey(item.entity), item); },
    pruneMetadata() {}, allMetadata: () => [...metadata.values()], searchPlacement: createFieldSearchPlacement(),
    engine: { element: { clientWidth: 1280, clientHeight: 800 },
      update(value) { updates.push(value); }, setCamera(value) { cameraCalls.push(value); }, focusContext(value) { focusCalls.push(value); } },
    wide: { matches: true }, fieldEntityKey, layoutUnifiedField, fieldBounds, rectPoints, fitFieldCamera,
    options: { onMessage() {} }, closePreview() {}, updateSummary() {},
  };
  vm.runInNewContext(compiled, environment);
  return { api: exports.api, environment, state, calls, cameraCalls, focusCalls, updates, directory };
}

test('registration refresh re-queries an existing empty query and shows the new owner app without changing camera or query', async () => {
  const f = fixture(); await f.api.refresh({ preserveView: true });
  assert.equal(f.calls.length, 1); assert.equal(f.calls[0].args.query, 'Хочу'); assert.equal(f.calls[0].args.scope, 'all');
  assert.equal(f.environment.searchItems[0].entity.id, appId); assert.equal(f.environment.empty.hidden, true);
  assert.equal(f.environment.queries.search, 'Хочу'); assert.equal(f.cameraCalls.length, 0); assert.equal(f.focusCalls.length, 0);
  assert.equal(f.updates.length, 1); assert.equal(f.updates[0].searchDocument.shortcuts[0].entity.id, appId);
  f.directory.dispose();
});

test('ordinary search refresh retains its original automatic first-result camera fitting', async () => {
  const f = fixture(); await f.api.refresh(); assert.equal(f.calls.length, 1); assert.equal(f.cameraCalls.length, 1);
  f.directory.dispose();
});

test('Mine refresh loads metadata without public search, camera moves or changing the private focus context', async () => {
  let mineReads = 0; const f = fixture({ mode: 'mine', loadMine: async () => { mineReads++; } });
  await f.api.refresh({ preserveView: true }); assert.equal(mineReads, 1); assert.equal(f.calls.length, 0);
  assert.equal(f.environment.mode, 'mine'); assert.equal(f.cameraCalls.length, 0); assert.equal(f.focusCalls.length, 0);
  f.directory.dispose();
});

test('a changed screen during directory loading cannot dispatch a search or deliver its late private result', async () => {
  const before = fixture(); before.state.current = false; await before.api.refresh({ preserveView: true }); assert.equal(before.calls.length, 0);
  let resolve, entered = false;
  const late = fixture({ beforeRead: async () => { entered = true; await new Promise(done => { resolve = done; }); } });
  const pending = late.api.refresh({ preserveView: true });
  for (let n = 0; n < 3; n++) await new Promise(done => setImmediate(done)); assert.equal(entered, true);
  late.state.current = false; resolve(); await pending;
  assert.equal(late.updates.length, 0); assert.equal(late.environment.searchItems.length, 0); assert.equal(late.cameraCalls.length, 0);
  before.directory.dispose(); late.directory.dispose();
});
