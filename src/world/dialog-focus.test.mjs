import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import * as audience from './app-audience.mjs';
import * as launch from './app-launch.mjs';
import * as deployment from './app-deployment.mjs';

const compile = async file => ts.transpileModule(await readFile(new URL(file, import.meta.url), 'utf8'),
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const dialogsCode = await compile('./dialogs.ts'), appCode = await compile('./app.ts'), cardCode = await compile('./application-card.ts');

// Deterministic native-close task/focus port. These tests execute the production
// helper and controller; only the browser platform and the settings form are
// ports. Actual layout, native keyboard and the real form have a separate page.
function fixture() {
  const tasks = [], listeners = new Map(), location = { hash: '#mine' };
  let document;
  class Element extends EventTarget {
    children = []; dataset = {}; attributes = new Map(); parentElement = null; open = false; disabled = false;
    classList = { add() {}, remove() {}, toggle() {} };
    constructor(tag = 'div', className = '', text = '') { super(); this.tagName = tag.toUpperCase(); this.className = className; this.textContent = text; }
    get isConnected() { return this === document.body || !!this.parentElement?.isConnected; }
    get tabIndex() { return this.attributes.has('tabindex') ? Number(this.attributes.get('tabindex')) : /^(BUTTON|INPUT|TEXTAREA|SUMMARY)$/.test(this.tagName) ? 0 : -1; }
    set tabIndex(value) { this.attributes.set('tabindex', String(value)); }
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    getAttribute(name) { return this.attributes.get(name) ?? null; }
    hasAttribute(name) { return name === 'open' ? this.open : this.attributes.has(name); }
    removeAttribute(name) { this.attributes.delete(name); }
    append(...nodes) { for (const node of nodes) if (node instanceof Element) { node.parentElement = this; this.children.push(node); } }
    replaceChildren(...nodes) { for (const node of [...this.children]) node.remove(); this.append(...nodes); }
    contains(node) { return node === this || this.children.some(child => child.contains(node)); }
    remove() { if (this.contains(document.activeElement)) document.activeElement = document.body; const parent = this.parentElement; if (parent) parent.children = parent.children.filter(child => child !== this); this.parentElement = null; }
    getClientRects() { return this.isConnected && !this.closest('[hidden], [inert]') ? [{ width: 44, height: 44 }] : []; }
    matches(selector) {
      if (selector.includes(',')) return selector.split(',').some(part => this.matches(part.trim()));
      if (selector === ':disabled') return this.disabled;
      if (selector === 'dialog[open]') return this.tagName === 'DIALOG' && this.open;
      if (selector.startsWith('.')) return this.className.split(' ').includes(selector.slice(1));
      const attr = /^\[([^=\]]+)(?:="([^"]*)")?\]$/.exec(selector);
      if (attr) { const value = attr[1].startsWith('data-') ? this.dataset[attr[1].slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] : this.getAttribute(attr[1]); return value != null && (attr[2] === undefined || value === attr[2]); }
      return this.tagName.toLowerCase() === selector;
    }
    closest(selector) { return this.matches(selector) ? this : this.parentElement?.closest(selector) ?? null; }
    querySelectorAll(selector) { return this.children.flatMap(child => [...(child.matches(selector) ? [child] : []), ...child.querySelectorAll(selector)]); }
    querySelector(selector) {
      if (selector === '.sw-dialog-header button') return this.querySelector('.sw-dialog-header')?.querySelector('button') ?? null;
      return this.querySelectorAll(selector)[0] ?? null;
    }
    focus() { if (!this.isConnected || this.disabled) return; document.activeElement = this; for (const callback of listeners.get('focusin') ?? []) callback({ target: this }); }
    click() { this.dispatchEvent(new Event('click', { cancelable: true })); }
    showModal() { this.previous = document.activeElement; this.open = true; this.querySelector('button')?.focus(); }
    close() {
      if (!this.open) return; this.open = false;
      if (this.contains(document.activeElement)) document.activeElement = document.body;
      this.previous?.focus(); tasks.push(() => this.dispatchEvent(new Event('close')));
    }
    requestClose() { const event = new Event('cancel', { cancelable: true }); if (this.dispatchEvent(event)) this.close(); }
  }
  document = {
    body: new Element('body'), activeElement: null,
    addEventListener(name, callback) { if (!listeners.has(name)) listeners.set(name, new Set()); listeners.get(name).add(callback); },
    removeEventListener(name, callback) { listeners.get(name)?.delete(callback); },
    querySelectorAll: selector => document.body.querySelectorAll(selector),
  };
  document.activeElement = document.body;
  const el = (...args) => new Element(...args);
  const button = (label, _symbol, className, action) => { const value = el('button', className, label); if (action) value.addEventListener('click', action); return value; };
  const dom = { el, button, iconButton: (label, symbol, action) => button(label, symbol, 'sw-icon-button', action) };
  let settings, disposed = 0;
  const ports = { './dom': dom, './icons': { icon: () => el('svg') }, './types': { worldColor: value => value },
    './app-audience.mjs': audience, './app-launch.mjs': launch, './app-deployment.mjs': deployment,
    './app-settings': { mountAppSettings(options) { settings = options; return { dispose() { disposed++; }, requestClose(_trigger, afterClose) { options.onClose(afterClose); } }; } } };
  const globals = { document, location, HTMLElement: Element, getComputedStyle: () => ({ visibility: 'visible' }), crypto: { randomUUID: () => String(Math.random()) },
    URLSearchParams, setTimeout, clearTimeout, clearInterval, Event };
  function evaluate(code) { const module = { exports: {} }; vm.runInNewContext(code, { ...globals, module, exports: module.exports, require: name => ports[name] ?? {} }); return module.exports; }
  const dialogs = evaluate(dialogsCode); ports['./dialogs'] = dialogs;
  const { createApplicationCard } = evaluate(cardCode);
  const { TestController } = evaluate(`${appCode}\nexports.TestController = WorldApplication;`);
  const app = Object.create(TestController.prototype), main = el('main'), root = el('section'); main.tabIndex = -1; root.append(main); document.body.append(root);
  const record = { appId: `app-${'12'.repeat(16)}`, name: 'Fixture app', ownerAccountId: 'account-A', status: 'offline', grants: { accountIds: [], communityIds: [] }, publication: { launchPolicy: 'restricted', activeNamedAddressCount: 0 } };
  let renders = 0;
  Object.assign(app, { root, main, deskAccount: 'account-A', accountGeneration: 1, screenSequence: 1, activeRoute: '#mine', destroyed: false,
    dialogs: new Set(), avatars: { observe() {} }, profile: { profileId: 'account-A' }, apps: [record], communities: [], group: null, view: 'mine', appStage: null,
    appSettingsDialog: null, appSettingsRouteClose: null, api: {},
    renderPersonal() { renders++; render(); }, appStateLabel: () => 'Offline',
    openApplication() { const heading = el('h1', '', 'Preview'); heading.tabIndex = -1; main.replaceChildren(heading); app.screenSequence++; location.hash = '#preview'; heading.focus(); },
  });
  const render = () => { main.replaceChildren(createApplicationCard({ app: app.apps[0], accountId: 'account-A', communities: [], pinned: false,
    inspect: () => app.inspectApplication(app.apps[0]), open() {}, openCommunity() {}, togglePin() { return false; } })); };
  render();
  const opener = () => main.querySelector('[data-app-action="inspect"]');
  const chain = () => { opener().focus(); opener().click(); const inspect = [...app.dialogs][0]; const next = inspect.body.querySelectorAll('button').find(node => node.textContent === 'Название и доступ'); next.focus(); next.click(); return app.appSettingsDialog; };
  const changed = () => settings.onChanged({ app: { name: 'Renamed', state: 'enabled', grants: record.grants }, source: { hostDeviceId: 'host', deviceName: 'Fixture', observation: { state: 'offline' } }, publication: { launchPolicy: 'restricted', activeDomainIds: [] }, addresses: { aliases: [] } });
  return { document, location, el, app, main, dialogs, record, opener, chain, changed, settings: () => settings,
    disposed: () => disposed, renders: () => renders, drain() { while (tasks.length) tasks.shift()(); } };
}

test('managed close runs cleanup/render once before resolving a replacement, never again in the native close task', () => {
  const f = fixture(), first = f.opener(); first.focus(); let count = 0, replacement;
  const modal = f.dialogs.createDialog('Fixture', () => { count++; replacement = f.el('button'); f.main.replaceChildren(replacement); },
    { isCurrent: () => true, resolve: () => replacement });
  modal.close(); assert.equal(count, 1); assert.equal(f.document.activeElement, replacement);
  const other = f.el('button'); f.main.append(other); other.focus(); f.drain(); modal.close();
  assert.equal(count, 1); assert.equal(f.document.activeElement, other);
});

test('callback opening a follow-up modal and nested modal return never lose the newer focus', () => {
  const f = fixture(); f.opener().focus(); let next;
  const first = f.dialogs.createDialog('First', () => { next = f.dialogs.createDialog('Next'); });
  first.close(); const focused = f.document.activeElement; f.drain(); assert.ok(next.element.contains(focused)); assert.equal(f.document.activeElement, focused);
  const nested = f.dialogs.createDialog('Nested'); nested.close(); f.drain(); assert.equal(f.document.activeElement, focused); next.close(); f.drain();
});

test('native cancel can veto Escape/close; queued fallback respects intervening focus and completes once', () => {
  const f = fixture(); f.opener().focus(); let count = 0, interrupted;
  const modal = f.dialogs.createDialog('Guarded', context => { count++; interrupted = context.interrupted; });
  const veto = event => event.preventDefault(); modal.element.addEventListener('cancel', veto);
  modal.element.requestClose(); f.drain(); assert.equal(modal.element.open, true); assert.equal(count, 0);
  modal.element.removeEventListener('cancel', veto); modal.element.requestClose();
  const other = f.el('button'); f.main.append(other); other.focus(); f.drain();
  assert.equal(count, 1); assert.equal(interrupted, true); assert.equal(f.document.activeElement, other); modal.close(); assert.equal(count, 1);
});

test('actual inspect → settings rename returns to the recreated same-app action, unchanged settings to surviving opener', () => {
  for (const rename of [true, false]) {
    const f = fixture(), initial = f.opener(), modal = f.chain(); if (rename) f.changed();
    f.settings().onClose(); assert.equal(f.disposed(), 1); assert.equal(f.renders(), rename ? 1 : 0);
    assert.equal(f.document.activeElement, f.opener()); assert.equal(f.opener() === initial, !rename);
    f.drain(); assert.equal(f.document.activeElement, f.opener()); assert.equal(modal.element.isConnected, false);
  }
});

test('actual native settings close does not rerender over an intervening user focus', () => {
  const f = fixture(), modal = f.chain(); f.changed(); modal.element.close();
  const other = f.el('button'); f.main.append(other); other.focus(); f.drain();
  assert.equal(f.disposed(), 1); assert.equal(f.renders(), 0); assert.equal(f.document.activeElement, other);
});

test('return target rejects same-account ABA, route change and destruction, with no stale metadata repaint', () => {
  for (const boundary of ['account', 'route', 'destroy']) {
    const f = fixture(); f.chain(); f.changed();
    if (boundary === 'account') f.app.accountGeneration += 2;
    if (boundary === 'route') { f.location.hash = '#other'; f.app.screenSequence++; }
    if (boundary === 'destroy') f.app.destroyed = true;
    const next = f.el('button'); f.main.replaceChildren(next);
    f.settings().onClose(); f.drain(); assert.equal(f.renders(), 0, boundary); assert.notEqual(f.document.activeElement, next, boundary);
  }
});

test('handoff runs once after dispose without old repaint; preview owns focus before the native close task', () => {
  for (const preview of [false, true]) {
    const f = fixture(); f.chain(); f.changed(); let resumed = 0;
    if (preview) f.settings().onPreview({ domainId: `dom_${'34'.repeat(16)}`, path: '/board' });
    else f.settings().onClose(() => { assert.equal(f.disposed(), 1); resumed++; f.app.openApplication(); });
    const focused = f.document.activeElement; f.drain();
    assert.equal(f.renders(), 0); assert.equal(f.disposed(), 1); assert.equal(f.document.activeElement, focused);
    assert.equal(focused.textContent, 'Preview'); assert.equal(resumed, preview ? 0 : 1);
  }
});

test('direct runtime settings preserve toolbar and runtime identity while updating metadata', () => {
  const f = fixture(), toolbar = f.el('button'), runtime = f.el('iframe'); f.main.replaceChildren(toolbar, runtime); toolbar.focus(); let updates = 0;
  f.app.openAppSettings(f.record, () => updates++); f.changed(); f.settings().onClose(); f.drain();
  assert.equal(updates, 1); assert.equal(f.renders(), 0); assert.equal(f.main.children[1], runtime); assert.equal(f.document.activeElement, toolbar);
});

test('missing app action uses its same-app launch, then the same-screen main, without picking another app', () => {
  for (const keepLaunch of [true, false]) {
    const f = fixture(); f.chain(); const launchControl = f.main.querySelector('[data-entity-id]');
    f.opener().remove(); if (!keepLaunch) launchControl.remove();
    const unrelated = f.el('button'); unrelated.dataset.appAction = 'inspect'; unrelated.dataset.appId = 'another'; f.main.append(unrelated);
    f.settings().onClose(); f.drain(); assert.equal(f.document.activeElement, keepLaunch ? launchControl : f.main);
  }
});

test('owned same-group repaint gets a new main target without reviving the old screen ticket or waiting for cards', () => {
  for (const changedIdentity of [false, true]) {
    const f = fixture(); f.app.group = { communityId: 'group-fixture' }; f.app.groupTab = 'apps';
    f.app.activeRoute = f.location.hash = '#community/group-fixture/apps';
    const oldTicket = f.app.appDialogReturnTarget(f.record.appId);
    f.app.renderGroup = () => { f.app.screenSequence++; f.main.replaceChildren(f.el('p', '', 'Loading cards')); if (changedIdentity) f.app.accountGeneration += 2; };
    f.chain(); f.changed(); f.settings().onClose();
    assert.equal(oldTicket.isCurrent(), false); assert.equal(f.document.activeElement === f.main, !changedIdentity);
    const laterCard = f.el('button'); laterCard.dataset.appAction = 'inspect'; laterCard.dataset.appId = f.record.appId; f.main.append(laterCard);
    const outside = f.el('button'); f.document.body.append(outside); outside.focus(); f.drain();
    assert.equal(f.document.activeElement, outside);
  }
});
