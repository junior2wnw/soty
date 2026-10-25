import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
const compiled = ts.transpileModule(readFileSync(new URL('./startup-timing.ts', import.meta.url), 'utf8'), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS }
}).outputText;
const exports = {}; vm.runInNewContext(compiled, { exports, URL });
const record = exports.recordStartupTiming;

function fixture() {
  const root = { dataset: {}, ownerDocument: { baseURI: 'https://shell.test/', querySelectorAll: () => [{ src: 'https://extension.test/inpage.js' }, { src: 'https://shell.test/assets/index-test.js' }] } };
  const navigation = { requestStart: 10, responseStart: 110, responseEnd: 115, domContentLoadedEventEnd: 160, loadEventEnd: 0, type: 'reload' };
  let at = 130;
  return { root, navigation, clock: { timeOrigin: 1700000000000, now: () => at, getEntriesByType: () => [navigation],
    getEntriesByName: () => [{ startTime: 116, requestStart: 117, responseEnd: 125, duration: 9, transferSize: 0 }] }, at(value) { at = value; } };
}

test('navigation and entry-resource phases use the browser navigation clock; a later snapshot retains the entry boundary', () => {
  const f = fixture(); record(f.root, f.clock);
  assert.equal(f.root.dataset.startupEntryOffsetMs, '130'); assert.equal(JSON.parse(f.root.dataset.startupNavigation).responseStart, 110);
  assert.equal(JSON.parse(f.root.dataset.startupEntryResource).responseEnd, 125);
  f.navigation.loadEventEnd = 190; f.at(9000); record(f.root, f.clock);
  assert.equal(JSON.parse(f.root.dataset.startupNavigation).loadEventEnd, 190); assert.equal(f.root.dataset.startupEntryOffsetMs, '130');
});

test('only selected numeric timing fields enter the trace, never URLs, identities or arbitrary strings', () => {
  const f = fixture(); Object.assign(f.navigation, { responseStart: NaN, connectStart: -1, loadEventEnd: Infinity,
    name: 'https://shell.test/?code=private', accountId: 'private-account', type: 'private-type' });
  record(f.root, f.clock); const output = JSON.stringify(f.root.dataset);
  assert.equal(/private|extension|https|NaN|Infinity/u.test(output), false);
  const nav = JSON.parse(f.root.dataset.startupNavigation); assert.equal('connectStart' in nav, false); assert.equal('responseStart' in nav, false);
});

test('missing or failing timing APIs explicitly report unsupported rather than inventing zero latency', () => {
  const f = fixture(); record(f.root, { ...f.clock, getEntriesByType: () => [] });
  assert.equal(f.root.dataset.startupTimingStatus, 'unsupported'); assert.equal(f.root.dataset.startupNavigation, undefined);
  record(f.root, { ...f.clock, getEntriesByType() { throw Error('unsupported'); } });
  assert.equal(f.root.dataset.startupTimingStatus, 'unsupported');
});
