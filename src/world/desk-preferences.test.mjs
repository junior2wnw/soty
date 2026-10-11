import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import { parseAppLaunchRoute } from './app-launch.mjs';

function harness() {
  const rows = new Map();
  const storage = { getItem: key => rows.get(key) ?? null, setItem: (key, value) => rows.set(key, value) };
  const context = vm.createContext({ exports: {}, require: path => path.includes('app-launch') ? { parseAppLaunchRoute } : {}, localStorage: storage, URLSearchParams });
  const source = readFileSync(new URL('./product.ts', import.meta.url), 'utf8');
  new vm.Script(ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText).runInContext(context);
  return { ...context.exports, rows, storage };
}
const plain = value => JSON.parse(JSON.stringify(value));

test('actual desk preference implementation preserves the selected space across reload and isolates accounts', () => {
  const app = harness();
  const value = { favorites: ['notes'], recent: [], pinnedApps: ['app-11111111111111111111111111111111'], lastSpace: 'after-work' };
  app.saveDeskPreferences('account-a', value, { selectedSpace: true });
  assert.deepEqual(plain(app.loadDeskPreferences('account-a')), value);
  assert.equal(app.loadDeskPreferences('account-b').lastSpace, undefined);
  app.saveDeskPreferences('account-b', { favorites: [], recent: [], lastSpace: 'family' }, { selectedSpace: true });
  assert.equal(app.loadDeskPreferences('account-a').lastSpace, 'after-work');
  assert.equal(app.loadDeskPreferences('account-b').lastSpace, 'family');
});

test('view-only preferences accept a bounded space ID and reject malformed stored data without altering app pins', () => {
  const app = harness();
  for (const lastSpace of ['a'.repeat(129), { contextId: 'family' }, null, '', '\u0000hidden', ' leading', 'trailing ', '\ud800']) {
    app.saveDeskPreferences('account', { favorites: [], recent: [], pinnedApps: ['my-app'], lastSpace }, { selectedSpace: true });
    const result = app.loadDeskPreferences('account');
    assert.equal(result.lastSpace, undefined); assert.deepEqual(plain(result.pinnedApps), ['my-app']);
  }
  app.rows.set('soty.desk.v1:account', 'null');
  assert.equal(app.loadDeskPreferences('account').lastSpace, undefined);
  app.rows.set('soty.desk.v1:account', '{invalid');
  assert.equal(app.loadDeskPreferences('account').lastSpace, undefined);
});

test('an unrelated write in a stale tab preserves the latest explicit space choice of the same account', () => {
  const app = harness();
  app.saveDeskPreferences('account', { favorites: [], recent: [], lastSpace: 'personal' }, { selectedSpace: true });
  const olderTab = app.loadDeskPreferences('account');
  app.saveDeskPreferences('account', { ...olderTab, lastSpace: 'after-work' }, { selectedSpace: true });
  app.saveDeskPreferences('account', { ...olderTab, recent: [{ route: 'notes/fixture-note', title: 'Note', symbol: 'list' }] });
  assert.equal(app.loadDeskPreferences('account').lastSpace, 'after-work');
  app.saveDeskPreferences('account', { ...olderTab, lastSpace: '' }, { selectedSpace: true });
  app.saveDeskPreferences('account', olderTab);
  assert.equal(app.loadDeskPreferences('account').lastSpace, undefined);
});

test('the view accepts opaque printable field IDs without interpreting them as routes', () => {
  const app = harness();
  for (const lastSpace of ['after.work', 'space:after-work', 'работа', '界'.repeat(128), 'x', '🧭', '../outside']) {
    app.saveDeskPreferences('account', { favorites: [], recent: [], lastSpace }, { selectedSpace: true });
    assert.equal(app.loadDeskPreferences('account').lastSpace, lastSpace);
  }
});

test('an unavailable optional preference store does not dispatch or create any core field record', () => {
  const app = harness(); app.storage.setItem = () => { throw new Error('quota'); };
  assert.doesNotThrow(() => app.saveDeskPreferences('account', { favorites: [], recent: [], lastSpace: 'family' }));
  assert.equal(app.rows.size, 0); assert.equal(app.loadDeskPreferences('account').lastSpace, undefined);
});

test('each account restores its last validated screen and stale preference writes preserve another tab navigation', () => {
  const app = harness(), initial = { favorites: ['notes'], recent: [], lastSpace: 'work', lastRoute: 'notes/private-note' };
  app.saveDeskPreferences('a', initial, { selectedSpace: true, selectedRoute: true });
  assert.equal(app.loadDeskPreferences('a').lastRoute, initial.lastRoute);
  assert.equal(app.loadDeskPreferences('b').lastRoute, undefined);
  const stale = app.loadDeskPreferences('a');
  const next = 'launch/app-11111111111111111111111111111111/dom_11111111111111111111111111111111?path=%2Fwidget';
  app.saveDeskPreferences('a', { ...stale, lastRoute: next }, { selectedRoute: true });
  app.saveDeskPreferences('a', { ...stale, favorites: [] });
  assert.equal(app.loadDeskPreferences('a').lastRoute, next);
  assert.equal(app.loadDeskPreferences('a').lastSpace, 'work');
  app.saveDeskPreferences('b', { favorites: [], recent: [], lastRoute: 'messages/group-b' }, { selectedRoute: true });
  assert.equal(app.loadDeskPreferences('a').lastRoute, next);
  assert.equal(app.loadDeskPreferences('b').lastRoute, 'messages/group-b');
});

test('last screen rejects stored external URLs, launch credentials and malformed routes while preserving real search', () => {
  const app = harness();
  for (const route of ['https://foreign.invalid/', '//foreign.invalid', '#mine', 'world?handle=secret', 'world?q=' + 'x'.repeat(101),
    'mine?kind=unknown', 'mine?q=x&q=y', 'community/a/chat', 'assistant/execute', 'notes/../../outside', '\ud800', 'world\u0000',
    'app/app-11111111111111111111111111111111?token=secret']) assert.equal(app.normalizeDeskRoute(route), undefined, route);
  for (const route of ['notes/private-note', 'messages/group-a', 'community/group-a/chat', 'assistant/chat', 'library', 'access', 'mine?kind=device']) {
    assert.equal(app.normalizeDeskRoute(route), route);
  }
  assert.equal(app.normalizeDeskRoute('world?q=%D0%BF%D0%BB%D0%B0%D0%BD%3F&kind=app'), 'world?q=%D0%BF%D0%BB%D0%B0%D0%BD%3F&kind=app');
});
