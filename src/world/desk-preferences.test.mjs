import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';

function harness() {
  const rows = new Map();
  const storage = { getItem: key => rows.get(key) ?? null, setItem: (key, value) => rows.set(key, value) };
  const context = vm.createContext({ exports: {}, require: () => ({}), localStorage: storage });
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
