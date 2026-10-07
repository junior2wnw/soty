import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFile } from 'node:fs/promises';
import ts from 'typescript';

// Execute the real registration, account fence and placement-cancel methods.
// Rendering, directory readiness and transport are inert ports; no DB/network.
const source = await readFile(new URL('./app.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const appId = 'app-' + 'a'.repeat(32);
const turn = () => new Promise(resolve => setImmediate(resolve));
async function settle() { for (let n = 0; n < 6; n++) await turn(); }
function deferred() { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
class Element {
  constructor(tagName = 'div', className = '', text = '') {
    Object.assign(this, { tagName, className, textContent: text, value: '', children: [], handlers: new Map(), open: true,
      isConnected: true, disabled: false, hidden: false, parent: null, dataset: {}, classList: { add() {} } });
  }
  append(...nodes) { for (const node of nodes) { node.parent = this; this.children.push(node); if (this.tagName === 'select' && (!this.value || node.selected)) this.value = node.value; } }
  replaceChildren(...nodes) { this.children = []; this.append(...nodes); }
  querySelector(selector) { for (const node of this.children) { if (node.tagName === selector || selector.startsWith('.') && node.className.split(' ').includes(selector.slice(1))) return node; const found = node.querySelector(selector); if (found) return found; } return null; }
  querySelectorAll(selector) { return this.children.flatMap(node => [...(node.tagName === selector ? [node] : []), ...node.querySelectorAll(selector)]); }
  addEventListener(type, fn) { if (!this.handlers.has(type)) this.handlers.set(type, []); this.handlers.get(type).push(fn); }
  emit(type) { for (const fn of this.handlers.get(type) || []) fn({ preventDefault() {}, stopImmediatePropagation() {} }); }
  reportValidity() { return true; } setAttribute() {} focus() {}
}
function fixture({ registration, fieldReady = Promise.resolve() } = {}) {
  const ports = {
    './dom': {
      el: (tag, className, text) => new Element(tag, className, text),
      button: (label, _icon, className, callback) => { const value = new Element('button', className, label); if (callback) value.addEventListener('click', callback); return value; },
      textInput: (value = '') => Object.assign(new Element('input'), { value }),
      labeledField: (label, input) => { const value = new Element('label', '', label); value.append(input); return value; },
    },
    './dialogs': { errorText: () => 'Не удалось подключить проект' },
    './app-field-placement.mjs': { createAppFieldPlacementController: () => ({
      async load() { return { contexts: [], pendingCount: 0, persistence: 'saved', localDurable: true }; },
      hasUnsavedChanges: () => false, dispose() {},
    }) },
  };
  const module = { exports: {} };
  vm.runInNewContext(compiled + '\nexports.Controller=WorldApplication;', { module, exports: module.exports, require: name => ports[name] || {} });
  const app = Object.create(module.exports.Controller.prototype), dialogs = [], calls = [], refreshes = [], toasts = [];
  const field = { ready: fieldReady, async refresh(options) { refreshes.push(structuredClone(options)); } };
  Object.assign(app, { destroyed: false, deskAccount: 'account-A', accountGeneration: 1, screenSequence: 1, view: 'world', query: 'Хочу',
    group: null, appStage: null, communities: [], desk: { pinnedApps: [] }, unifiedField: field, main: new Element(),
    homeQuery: 'mine-query', unifiedFieldFilters: { mine: 'app', world: 'all' }, unifiedFieldView: { mineContext: 'personal', mineFit: 'context' },
    toast(message) { toasts.push(message); }, loading: label => new Element('p', '', label),
    navigate() { throw Error('registration cancellation must not navigate'); },
    dialog(title, closed) {
      const value = { title, element: new Element('dialog'), body: new Element(), close() { if (!value.element.open) return; value.element.open = false; closed?.(); } };
      dialogs.push(value); return value;
    },
    api: { async request(op, args) {
      calls.push({ op, args: structuredClone(args) });
      if (op === 'apps.devices') return { devices: [{ claimed: true, hostDeviceId: 'synthetic-host', connectorId: 'synthetic-connector', name: 'Host', online: true }] };
      assert.equal(op, 'apps.register'); return registration ? registration.promise : { app: { id: appId, name: 'ХочуИпотеку' } };
    } },
  });
  async function submit() {
    app.openAddApp(); await settle();
    const dialog = dialogs[0], form = dialog.body.querySelector('form'); assert.ok(form);
    form.querySelector('input').value = 'ХочуИпотеку';
    form.emit('submit'); await settle(); return dialog;
  }
  return { app, field, dialogs, calls, refreshes, toasts, submit };
}

test('successful registration refreshes the existing query without a placement, including chooser cancellation', async () => {
  const ready = deferred(), f = fixture({ fieldReady: ready.promise }); await f.submit();
  const chooser = f.dialogs.find(dialog => dialog.title === 'На моё поле'); assert.ok(chooser);
  chooser.element.emit('cancel'); assert.equal(chooser.element.open, false); assert.equal(f.app.appPlacementDialog, null);
  assert.equal(f.refreshes.length, 0); ready.resolve(); await settle();
  assert.deepEqual(f.refreshes, [{ preserveView: true }]);
  assert.equal(f.calls.filter(call => call.op === 'apps.register').length, 1); assert.equal(f.calls.length, 2);
  assert.deepEqual(f.calls[1].args.grants, { accountIds: [], communityIds: [] });
  assert.equal(f.app.query, 'Хочу'); assert.equal(f.app.view, 'world'); assert.equal(f.app.homeQuery, 'mine-query');
  assert.deepEqual(f.app.desk.pinnedApps, []); assert.equal(f.app.unifiedFieldView.mineContext, 'personal');
  assert.equal(f.app.unifiedFieldFilters.mine, 'app');
});

test('registration failure and malformed app identity never refresh a directory or open placement', async () => {
  for (const value of [null, { app: { id: 'wrong', name: 'Untrusted' } }]) {
    const registration = deferred(), f = fixture({ registration }); await f.submit();
    if (value) registration.resolve(value); else registration.reject(Object.assign(Error('synthetic failure'), { code: 'app_offline' }));
    await settle(); assert.equal(f.refreshes.length, 0); assert.equal(f.dialogs.length, 1); assert.equal(f.toasts.length, 0);
  }
});

test('registration ACK after dialog cancellation, logout or account ABA cannot refresh or populate the new account', async () => {
  for (const change of [f => f.dialogs[0].close(), f => { f.app.deskAccount = ''; f.app.accountGeneration++; },
    f => { f.app.deskAccount = 'account-B'; f.app.accountGeneration++; f.app.deskAccount = 'account-A'; f.app.accountGeneration++; }]) {
    const registration = deferred(), f = fixture({ registration }); await f.submit(); change(f);
    registration.resolve({ app: { id: appId, name: 'Late app' } }); await settle();
    assert.equal(f.refreshes.length, 0); assert.equal(f.dialogs.length, 1); assert.equal(f.toasts.length, 0);
  }
});

test('deferred directory readiness cannot refresh a replaced screen, changed account or new field instance', async () => {
  for (const change of [f => { f.app.screenSequence++; }, f => { f.app.deskAccount = 'account-B'; f.app.accountGeneration++; },
    f => { f.app.unifiedField = { ready: Promise.resolve(), refresh() { throw Error('foreign field'); } }; }]) {
    const ready = deferred(), f = fixture({ fieldReady: ready.promise }); await f.submit(); change(f); ready.resolve(); await settle();
    assert.equal(f.refreshes.length, 0); assert.equal(f.calls.filter(call => call.op === 'apps.register').length, 1);
  }
});
