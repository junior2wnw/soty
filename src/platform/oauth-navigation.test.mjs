import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';

const source = await readFile(new URL('./oauth-navigation.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const interaction = 'fixture_interaction_123', account = 'fixture_account';

function fixture({ throwSubmit = false } = {}) {
  const forms = new Set(), listeners = new Map(), timers = new Map(); let next = 0, submits = 0;
  const document = {
    body: { append(form) { forms.add(form); form.isConnected = true; } },
    createElement(tag) { return { tag, children: [], isConnected: false,
      append(child) { this.children.push(child); },
      remove() { forms.delete(this); this.isConnected = false; },
      submit() { submits++; if (throwSubmit) throw new Error('synthetic blocked navigation'); },
    }; },
  };
  const window = { addEventListener(name, callback) { listeners.set(name, callback); },
    removeEventListener(name, callback) { if (listeners.get(name) === callback) listeners.delete(name); } };
  const module = { exports: {} };
  vm.runInNewContext(compiled, { module, exports: module.exports, document, window,
    setTimeout(callback, delay) { const id = ++next; timers.set(id, { callback, delay }); return id; },
    clearTimeout(id) { timers.delete(id); } });
  return { forms, listeners, timers, submit: module.exports.submitOAuthCompletion, get submits() { return submits; } };
}

test('completion form stays connected past submit and is cleaned only after navigation', async () => {
  const f = fixture(), pending = f.submit(interaction, account);
  await Promise.resolve();
  assert.equal(f.submits, 1); assert.equal(f.forms.size, 1);
  const form = [...f.forms][0]; assert.equal(form.isConnected, true);
  assert.equal(form.method, 'POST'); assert.equal(form.action, '/oauth/interaction/' + interaction + '/complete');
  assert.equal(form.children.length, 1); assert.equal(form.children[0].name, 'expectedAccountId');
  assert.equal(form.children[0].value, account);
  f.listeners.get('pagehide')(); await pending;
  assert.equal(f.forms.size, 0); assert.equal(f.timers.size, 0); assert.equal(f.listeners.size, 0);
});

test('silent navigation failure has a bounded unknown outcome without any automatic retry', async () => {
  const f = fixture(), pending = f.submit(interaction, account);
  const failure = assert.rejects(pending, error => error.message === 'Completion not confirmed');
  const timer = [...f.timers.values()][0]; assert.equal(timer.delay, 8000);
  timer.callback(); await failure;
  assert.equal(f.submits, 1); assert.equal(f.forms.size, 0); assert.equal(f.listeners.size, 0); assert.equal(f.timers.size, 0);
});

test('synchronous submit refusal also clears the private form and listeners', async () => {
  const f = fixture({ throwSubmit: true });
  await assert.rejects(f.submit(interaction, account), error => error.message === 'Completion not confirmed');
  assert.equal(f.submits, 1); assert.equal(f.forms.size, 0); assert.equal(f.listeners.size, 0); assert.equal(f.timers.size, 0);
});

test('form policy denial is distinct from a timeout and exposes no blocked URL', async () => {
  const f = fixture(), pending = f.submit(interaction, account);
  const failure = assert.rejects(pending, error => error.code === 'navigation_policy_blocked'
    && error.message === 'Completion not confirmed' && error.blockedURI === undefined);
  f.listeners.get('securitypolicyviolation')({ effectiveDirective: 'form-action', blockedURI: 'private-target-not-echoed' });
  await failure;
  assert.equal(f.submits, 1); assert.equal(f.forms.size, 0); assert.equal(f.listeners.size, 0); assert.equal(f.timers.size, 0);
});

test('unbounded or malformed completion context cannot construct a form', async () => {
  const f = fixture();
  for (const [id, actor] of [['../other', account], [interaction, ''], [interaction, 'a'.repeat(161)]]) {
    await assert.rejects(f.submit(id, actor), error => error.message === 'Completion unavailable');
  }
  assert.equal(f.submits, 0); assert.equal(f.forms.size, 0);
});
