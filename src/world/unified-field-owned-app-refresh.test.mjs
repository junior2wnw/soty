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
    button: (label, _icon, _className, action) => ({ label, action, getAttribute: () => null }),
    loadOwnedApps: generation => { calls.push(generation); return Promise.resolve(); }, run() {},
  };
  vm.runInNewContext(code, environment); exports.render();
  assert.equal(overview.hidden, false);
  assert.match(children[0].text, /Не удалось обновить/u);
  assert.equal(children[1].label, 'Повторить');
  children[1].action(); assert.deepEqual(calls, [7]);
  state.current = false; children[1].action(); assert.deepEqual(calls, [7]);
});

test('retry keeps focus during loading and moves it only if it is still on that retry', async () => {
  const render = statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'renderOwnApps');
  const code = ts.transpileModule(`${print(render)}\n${print(load)}\nexports.render = renderOwnApps;`,
    { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const doc = { activeElement: null, body: {} }, requests = [], pending = [], exports = {}, state = { current: true };
  class Element {
    constructor(tag, className = '', text = '') { this.tag = tag; this.className = className; this.textContent = text; this.children = []; this.attrs = {}; }
    append(...items) { for (const item of items) { item.parent = this; this.children.push(item); } }
    replaceChildren() { if (this.contains(doc.activeElement)) doc.activeElement = doc.body; this.children = []; }
    contains(item) { return item === this || this.children.some(child => child.contains(item)); }
    querySelector(selector) {
      const names = selector.split(',').map(name => name.trim().slice(1));
      const visit = node => node.children.flatMap(child => [child, ...visit(child)]);
      return visit(this).find(node => names.some(name => node.className.split(' ').includes(name))) ?? null;
    }
    setAttribute(name, value) { this.attrs[name] = value; }
    getAttribute(name) { return this.attrs[name] ?? null; }
    focus() { doc.activeElement = this; }
  }
  const overview = new Element('section'), listView = new Element('button', 'list-view'), elsewhere = new Element('button', 'elsewhere');
  const environment = { exports, document: doc, ownAppsOverview: overview, ownAppsSignature: '',
    ownedAppsState: 'offline', ownedApps: [], ownAppsReadSequence: 0, ownAppsAppliedRead: 0, mineGeneration: 1,
    current: () => state.current, engine: { snapshot: () => ({ shortcuts: [] }) }, mode: 'mine',
    element: { dataset: { search: 'closed' } }, summary: null, queries: { mine: '' }, filter: 'all', listView,
    overviewApps: () => environment.ownedApps, ownAppButton: app => new Element('button', 'uf-own-app', app.name),
    el: (tag, className, text) => new Element(tag, className, text),
    button: (label, _icon, className, action) => Object.assign(new Element('button', className, label), { action }),
    isDialogFocusTarget: node => Boolean(node), focusFieldControl: () => listView.focus(),
    renderMineSearchResults() {}, renderOpenList: null, updateMineEmpty() {}, emptyDocument: null,
    options: { api: { request() { const request = deferred(); requests.push(request); return request.promise; } }, accountId: 'A' },
    run: promise => { pending.push(promise); },
  };
  vm.runInNewContext(code, environment); exports.render();
  let retry = overview.querySelector('.uf-own-apps-retry'); retry.focus(); retry.action();
  assert.equal(doc.activeElement, retry); assert.equal(overview.querySelector('.uf-own-apps-retry'), retry);
  assert.equal(retry.getAttribute('aria-disabled'), 'true');
  assert.equal(overview.querySelector('.uf-own-apps-status').textContent, 'Обновляем ваши приложения…');
  retry.action(); assert.equal(requests.length, 1, 'a second click while loading does not dispatch');
  requests[0].reject(Error('offline')); await pending[0];
  retry = overview.querySelector('.uf-own-apps-retry');
  assert.equal(doc.activeElement, retry, 'another failure returns focus to an enabled retry');
  assert.equal(retry.getAttribute('aria-disabled'), null);
  retry.action(); requests[1].resolve({ apps: [{ id: 'app-a', name: 'A' }] }); await pending[1];
  assert.equal(doc.activeElement, overview.querySelector('.uf-own-app'), 'success focuses the first app');

  environment.ownedAppsState = 'offline'; exports.render(); retry = overview.querySelector('.uf-own-apps-retry'); retry.focus(); retry.action();
  elsewhere.focus(); requests[2].resolve({ apps: [{ id: 'app-a', name: 'A' }] }); await pending[2];
  assert.equal(doc.activeElement, elsewhere, 'a person who moved focus is not pulled back');

  environment.ownedAppsState = 'offline'; exports.render(); retry = overview.querySelector('.uf-own-apps-retry'); retry.focus(); retry.action();
  state.current = false; elsewhere.focus(); requests[3].resolve({ apps: [{ id: 'app-a', name: 'A' }] }); await pending[3];
  assert.equal(doc.activeElement, elsewhere, 'a late reply cannot take focus after an account change');
});
