import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';

const code = ts.transpileModule(await readFile(new URL('./account-center.ts', import.meta.url), 'utf8'),
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;

class ElementPort extends EventTarget {
  constructor(tag, text = '') { super(); Object.assign(this, { tagName: tag, textContent: text, children: [], dataset: {}, attributes: new Map(), isConnected: true, parent: null }); }
  append(...children) { for (const child of children) { child.parent = this; this.children.push(child); } }
  replaceChildren(...children) { this.children = []; this.append(...children); }
  setAttribute(key, value) { this.attributes.set(key, String(value)); }
  getAttribute(key) { return this.attributes.get(key); }
  remove() { this.isConnected = false; if (this.parent) this.parent.children = this.parent.children.filter(child => child !== this); }
  all() { return [this, ...this.children.flatMap(child => child.all())]; }
  click() { this.dispatchEvent(new Event('click')); }
}

function fixture(extra = {}) {
  const make = (tag, _class, text = '') => new ElementPort(tag, text);
  const dom = { el: make, avatar: label => make('span', '', label), button(label, _icon, _class, onClick) {
    const control = make('button', '', label); control.addEventListener('click', onClick); return control;
  } };
  const module = { exports: {} };
  vm.runInNewContext(code, { module, exports: module.exports, require: id => id === './dom' ? dom : {} });
  const host = make('main'), calls = []; let current = true;
  const panel = module.exports.mountAccountCenter({ host, identity: { displayName: '<img src=x onerror=bad()>' },
    isCurrent: () => current, onAction: value => calls.push(value), ...extra });
  return { host, panel, calls, retire: () => { current = false; }, action: id => host.all().find(node => node.dataset.accountAction === id), section: id => host.all().find(node => node.dataset.accountSection === id) };
}

test('one centre exposes scoped categories; appearance, notifications and accepted AI settings reach their canonical editors directly', () => {
  const f = fixture();
  assert.equal(f.host.all().filter(node => node.dataset.accountSection).length, 6);
  f.section('appearance').click(); f.section('notifications').click(); f.section('ai').click();
  assert.deepEqual(f.calls, ['appearance', 'notifications', 'ai-api']);
  assert.ok(f.action('edit-profile')); assert.equal(f.host.all().filter(node => node.tagName === 'img').length, 0);
});

test('security separates identity devices, recovery and grants; data has distinct storage/device/help destinations', () => {
  const f = fixture(); f.section('security').click();
  for (const id of ['auth-devices', 'recovery', 'agent-access']) f.action(id).click();
  assert.equal(f.action('data-export'), undefined); assert.equal(f.action('app-hosts'), undefined);
  f.section('data').click(); f.action('data-export').click(); f.action('device-status').click();
  assert.deepEqual(f.calls, ['auth-devices', 'recovery', 'agent-access', 'data-export', 'device-status']);
});

test('unavailable controls never dispatch; stale account or disposed centre cannot execute an old action', () => {
  const f = fixture({ availableActions: ['edit-profile'] });
  assert.equal(f.action('switch-profile'), undefined);
  f.section('notifications').click(); assert.equal(f.action('notifications'), undefined);
  f.panel.select('profile'); const edit = f.action('edit-profile'); f.retire(); edit.click();
  assert.deepEqual(f.calls, []); f.panel.dispose(); edit.click(); assert.deepEqual(f.calls, []); assert.equal(f.host.children.length, 0);
});
