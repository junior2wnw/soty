import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';

const source = readFileSync(new URL('../entry.ts', import.meta.url), 'utf8');
const script = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;

async function enter(href) {
  const calls = [], history = [], location = new URL(href);
  const element = tag => ({ tag, dataset: {}, children: [], setAttribute() {},
    append(...items) { this.children.push(...items); }, replaceChildren(...items) { this.children = items; } });
  const root = element('main'), body = element('body');
  const modules = {
    './entry.css': {}, './geometry/dom': {},
    './world/preferences': { loadPreferences: () => ({}) },
    './world/theme/theme': { applyThemePreferences() {} }, './platform/connect-theme.css': {},
    './platform/pwa': { getPwaController() {} }, './platform/startup-timing': { recordStartupTiming() {} },
    './platform/human-login': { startHumanLogin: async () => calls.push('human') },
    './platform/oauth-consent': { startOAuthConsent: async () => calls.push('oauth') },
    './platform/world-adapter': { startWorld: async () => calls.push('world') },
    './main': {},
  };
  const window = { location, history: { state: { marker: 'retained' }, replaceState: (...args) => history.push(args) },
    addEventListener() {}, setTimeout() {} };
  vm.runInNewContext(script, { URL, window, location, document: { body, querySelector: () => root, createElement: element },
    exports: {}, require(name) { assert.ok(Object.hasOwn(modules, name), `unexpected import ${name}`); calls.push(name); return modules[name]; } });
  await new Promise(resolve => setImmediate(resolve));
  return { root, body, calls, history };
}

test('retired link shows a truthful notice without launching pairing and preserves the rest of the URL', async () => {
  const result = await enter('https://соты.online/install/spreadex?pair=fixture&keep=1#context');
  const [notice] = result.root.children;
  assert.equal(result.body.dataset.sotySurface, 'world');
  assert.equal(notice.children[0].tag, 'h1');
  assert.equal(notice.children[0].textContent, 'Этот способ подключения больше недоступен');
  assert.equal(notice.children[2].textContent, 'Открыть Соты');
  assert.equal(notice.children[2].href, '/__soty');
  assert.deepEqual(result.history, [[{ marker: 'retained' }, '', '/install/spreadex?keep=1#context']]);
  assert.equal(result.calls.includes('./main'), false);
  assert.equal(result.calls.includes('./platform/world-adapter'), false);
});

test('retired link also works without a code or with a trailing slash on the primary host', async () => {
  for (const path of ['/install/spreadex', '/install/spreadex/']) {
    const result = await enter(`https://soty.pochinit.online${path}`);
    assert.equal(result.root.children[0].children[2].href, '/');
    assert.deepEqual(result.history, []);
  }
});

test('ordinary entry, other install links and separate sign-in pages keep their routes', async () => {
  for (const [path, expected] of [
    ['/', 'world'], ['/#messages', 'world'], ['/#app/app-11111111111111111111111111111111', 'world'],
    ['/?view=classic', './main'], ['/install/windows', './main'],
    ['/human-identity/interaction/AAAAAAAAAAAAAAAA', 'human'], ['/oauth/interaction/AAAAAAAAAAAAAAAA', 'oauth'],
  ]) {
    const result = await enter(`https://soty.pochinit.online${path}`);
    assert.ok(result.calls.includes(expected), `${path} did not select ${expected}`);
    assert.deepEqual(result.history, []);
  }
});
