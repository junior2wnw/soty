import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFile } from 'node:fs/promises';
import ts from 'typescript';

// Run the two production methods together. Only transport and rendering are
// controlled ports, so the test exercises their shared ordering and status.
const source = await readFile(new URL('./unified-field-screen.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field-screen.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const factory = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedFieldScreen');
const statements = [...factory.body.statements];
const load = statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'loadOwnedApps');
const returned = statements.find(ts.isReturnStatement)?.expression;
const refresh = returned?.properties.find(node => ts.isMethodDeclaration(node) && node.name.getText(parsed) === 'refreshOwnApps');
assert.ok(load && refresh, 'actual owner-app methods exist');
const compiled = ts.transpileModule(`${print(load)}\nexports.api = { loadOwnedApps, ${print(refresh)} };\nexports.snapshot = () => ({ ownedApps, ownedAppsState, ownAppsReadSequence, ownAppsAppliedRead });`,
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const turn = () => new Promise(resolve => setImmediate(resolve));
function fixture(requests, { focused = 'none' } = {}) {
  const events = [], account = { current: true }, exports = {}, active = {};
  let calls = 0;
  const list = { querySelector(selector) {
    if (selector === '.uf-list-results') return { contains: () => focused === 'list' };
    if (selector === 'input[aria-label="Найти в этом списке"]') return { focus() { events.push('focus-list'); } };
    throw Error('unexpected selector');
  } };
  const environment = {
    exports, mineGeneration: 1, ownAppsReadSequence: 0, ownAppsAppliedRead: 0,
    ownedApps: [{ id: 'previous' }], ownedAppsState: 'ready', mode: 'mine', ready: Promise.resolve(),
    current: () => account.current,
    options: { accountId: 'account-A', api: { request(op, args) {
      assert.equal(op, 'apps.list'); assert.equal(args.expectedAccountId, 'account-A');
      const next = requests[calls++]; assert.ok(next, 'only explicitly expected reads'); return next.promise;
    } } },
    document: { activeElement: active },
    ownAppsOverview: { contains: () => focused === 'overview', querySelector: () => ({ focus() { events.push('focus-retry'); } }) },
    mineSearchResults: { contains: () => focused === 'search' }, listDialog: { element: list },
    searchInput: { focus() { events.push('focus-search'); } }, focusFieldControl() { events.push('focus-field'); },
    renderOwnApps() { events.push(`overview:${environment.ownedAppsState}`); },
    renderMineSearchResults() { events.push('search'); }, renderOpenList() { events.push('list'); },
    engine: { snapshot: () => ({ shortcuts: [] }) }, emptyDocument: {},
    updateMineEmpty() { events.push(`empty:${environment.ownedAppsState}`); },
  };
  vm.runInNewContext(compiled, environment);
  return { api: exports.api, snapshot: exports.snapshot, account, events, get calls() { return calls; } };
}

test('failed newer background read ends loading, rejects older success and allows an explicit recovery', async () => {
  const first = deferred(), newer = deferred(), recovery = deferred(), f = fixture([first, newer, recovery]);
  const oldRead = f.api.loadOwnedApps(1);
  const latestRead = f.api.refreshOwnApps(); await turn();
  assert.equal(f.calls, 2);
  newer.reject(Error('offline')); await latestRead;
  assert.equal(f.snapshot().ownedAppsState, 'offline'); assert.equal(f.snapshot().ownedApps.length, 0);
  assert.ok(f.events.includes('overview:offline')); assert.ok(f.events.includes('empty:offline'));
  first.resolve({ apps: [{ id: 'older' }] }); await oldRead;
  assert.equal(f.snapshot().ownedAppsState, 'offline'); assert.equal(f.snapshot().ownedApps.length, 0);
  const retry = f.api.refreshOwnApps(); await turn(); recovery.resolve({ apps: [{ id: 'new' }] }); await retry;
  assert.equal(f.snapshot().ownedAppsState, 'ready'); assert.equal(f.snapshot().ownedApps[0].id, 'new');
});

test('failed current background read replaces the stale successful view with an unavailable status', async () => {
  const read = deferred(), f = fixture([read]);
  const pending = f.api.refreshOwnApps(); await turn(); read.reject(Error('offline')); await pending;
  assert.equal(f.snapshot().ownedAppsState, 'offline'); assert.equal(f.snapshot().ownedApps.length, 0);
  assert.ok(f.events.includes('overview:offline')); assert.ok(f.events.includes('search')); assert.ok(f.events.includes('list'));
});

test('an older failure cannot overwrite a newer successful read', async () => {
  const first = deferred(), newer = deferred(), f = fixture([first, newer]);
  const oldRead = f.api.loadOwnedApps(1), latestRead = f.api.refreshOwnApps(); await turn();
  newer.resolve({ apps: [{ id: 'new' }] }); await latestRead;
  first.reject(Error('stale failure')); await oldRead;
  assert.equal(f.snapshot().ownedAppsState, 'ready'); assert.equal(f.snapshot().ownedApps[0].id, 'new');
  assert.equal(f.events.includes('overview:offline'), false);
});

test('a failed read after an account or screen change cannot update the retired field', async () => {
  const read = deferred(), f = fixture([read]);
  const pending = f.api.refreshOwnApps(); await turn(); f.account.current = false;
  read.reject(Error('offline')); await pending;
  assert.equal(f.snapshot().ownedAppsState, 'ready'); assert.equal(f.snapshot().ownedApps[0].id, 'previous');
  assert.equal(f.events.length, 0);
});

test('a failed update returns keyboard focus to the relevant search or retry control', async () => {
  for (const [focused, focusEvent] of [['search', 'focus-search'], ['list', 'focus-list'], ['overview', 'focus-retry']]) {
    const read = deferred(), f = fixture([read], { focused });
    const pending = f.api.refreshOwnApps(); await turn(); read.reject(Error('offline')); await pending;
    assert.equal(f.snapshot().ownedAppsState, 'offline');
    assert.ok(f.events.includes(focusEvent), focused);
  }
});

test('the unavailable overview has an explicit retry that uses the current account generation', () => {
  const render = statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'renderOwnApps');
  assert.ok(render);
  const code = ts.transpileModule(`${print(render)}\nexports.render = renderOwnApps;`,
    { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const calls = [], children = [], exports = {}, state = { current: true };
  const overview = { hidden: false, replaceChildren() { children.length = 0; }, append(...items) { children.push(...items); }, querySelector() { return null; } };
  const environment = { exports, ownAppsOverview: overview, ownAppsSignature: '', ownedAppsState: 'offline',
    current: () => state.current, engine: {}, mode: 'mine', element: { dataset: { search: 'closed' } }, summary: null,
    queries: { mine: '' }, filter: 'all', mineGeneration: 7, overviewApps: () => [],
    el: (_tag, _className, text) => ({ text, setAttribute() {} }),
    button: (label, _icon, _className, action) => ({ label, action }),
    loadOwnedApps: generation => { calls.push(generation); return Promise.resolve(); }, run() {},
  };
  vm.runInNewContext(code, environment); exports.render();
  assert.equal(overview.hidden, false);
  assert.match(children[0].text, /Не удалось обновить/u);
  assert.equal(children[1].label, 'Повторить');
  children[1].action(); assert.deepEqual(calls, [7]);
  state.current = false; children[1].action(); assert.deepEqual(calls, [7]);
});
