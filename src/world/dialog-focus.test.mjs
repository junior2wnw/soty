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
const accountCenterCode = await compile('./account-center.ts');
const accessCode = await compile('./access-panel.ts');
const accessAst = ts.createSourceFile('access-panel.js', accessCode, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
const accessMount = accessAst.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'mountAccessPanel');
const accessDialogCode = ['closeDialog', 'openDialog'].map(name => accessMount.body.statements
  .find(node => ts.isFunctionDeclaration(node) && node.name?.text === name).getText(accessAst)).join('\n');
const appAst = ts.createSourceFile('app.js', appCode, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
const appConstructor = appAst.statements.find(node => ts.isClassDeclaration(node) && node.name?.text === 'WorldApplication').members.find(ts.isConstructorDeclaration);
const rootClickHandler = appConstructor.body.statements.find(node => ts.isExpressionStatement(node) && ts.isCallExpression(node.expression)
  && node.expression.expression.getText(appAst) === 'root.addEventListener' && node.expression.arguments[0]?.text === 'click').expression.arguments[1].getText(appAst);

// Deterministic native-close task/focus port. These tests execute the production
// helper and controller; only the browser platform and the settings form are
// ports. Actual layout, native keyboard and the real form have a separate page.
function fixture() {
  const tasks = [], listeners = new Map(), location = { hash: '#mine' }, openedWindows = [];
  let document;
  class Element extends EventTarget {
    children = []; dataset = {}; attributes = new Map(); parentElement = null; open = false; disabled = false;
    style = { setProperty() {} };
    classList = { add: name => { this.className += ' ' + name; }, remove() {}, toggle() {} };
    constructor(tag = 'div', className = '', text = '') { super(); this.tagName = tag.toUpperCase(); this.className = className; this.textContent = text; }
    get isConnected() { return this === document.body || !!this.parentElement?.isConnected; }
    get tabIndex() { return this.attributes.has('tabindex') ? Number(this.attributes.get('tabindex')) : /^(BUTTON|INPUT|TEXTAREA|SUMMARY)$/.test(this.tagName) || this.tagName === 'A' && this.href ? 0 : -1; }
    set tabIndex(value) { this.attributes.set('tabindex', String(value)); }
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    getAttribute(name) { return this.attributes.get(name) ?? null; }
    hasAttribute(name) { return name === 'open' ? this.open : this.attributes.has(name); }
    removeAttribute(name) { this.attributes.delete(name); }
    append(...nodes) { for (const node of nodes) if (node instanceof Element) { if (node.parentElement) node.parentElement.children = node.parentElement.children.filter(child => child !== node); node.parentElement = this; this.children.push(node); } }
    replaceChildren(...nodes) { for (const node of [...this.children]) node.remove(); this.append(...nodes); }
    contains(node) { return node === this || this.children.some(child => child.contains(node)); }
    remove() { if (this.contains(document.activeElement)) document.activeElement = document.body; const parent = this.parentElement; if (parent) parent.children = parent.children.filter(child => child !== this); this.parentElement = null; }
    getClientRects() { return this.isConnected && !this.closest('[hidden], [inert]') ? [{ width: 44, height: 44 }] : []; }
    matches(selector) {
      if (selector.includes(',')) return selector.split(',').some(part => this.matches(part.trim()));
      if (selector === ':disabled') return this.disabled;
      if (selector === 'a[href]') return this.tagName === 'A' && Boolean(this.href);
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
  const dom = { el, button, iconButton: (label, symbol, action) => button(label, symbol, 'sw-icon-button', action),
    textInput: (value = '', placeholder = '') => Object.assign(el('input'), { value, placeholder }),
    labeledField: (label, input) => { const field = el('label'); field.append(el('span', '', label), input); return field; },
    avatar: () => el('span', 'sw-avatar') };
  let settings, disposed = 0;
  const ports = { './dom': dom, './icons': { icon: () => el('svg') }, './types': { worldColor: value => value },
    '../platform/origin-continuity': { createOriginContinuityLinks: () => null },
    './app-audience.mjs': audience, './app-launch.mjs': launch, './app-deployment.mjs': deployment,
    './app-settings': { mountAppSettings(options) { settings = options; return { dispose() { disposed++; }, requestClose(_trigger, afterClose) { options.onClose(afterClose); } }; } } };
  const globals = { document, location, window: { open: (...args) => { openedWindows.push(args); return null; } }, Element, HTMLElement: Element, getComputedStyle: () => ({ visibility: 'visible' }), crypto: { randomUUID: () => String(Math.random()) },
    URLSearchParams, setTimeout, clearTimeout, clearInterval, Event };
  function evaluate(code) { const module = { exports: {} }; vm.runInNewContext(code, { ...globals, module, exports: module.exports, require: name => ports[name] ?? {} }); return module.exports; }
  const dialogs = evaluate(dialogsCode); ports['./dialogs'] = dialogs;
  ports['./account-center'] = evaluate(accountCenterCode);
  const { mountAccessDialogs } = evaluate(`const dialogs_1 = require('./dialogs');
    exports.mountAccessDialogs = function({ workspace, feedbackTarget, addFeedback, refresh, alive }) {
      const content = workspace, view = 'clients'; let dialogState = null;
      ${accessDialogCode}
      return { open: openDialog, isOpen: () => dialogState !== null };
    };`);
  const { createApplicationCard } = evaluate(cardCode);
  const { TestController, bindRootClick } = evaluate(`${appCode}\nexports.TestController = WorldApplication; exports.bindRootClick = function() { return ${rootClickHandler}; };`);
  const app = Object.create(TestController.prototype), main = el('main'), root = el('section'); main.tabIndex = -1; root.append(main); document.body.append(root);
  const record = { appId: `app-${'12'.repeat(16)}`, name: 'Fixture app', ownerAccountId: 'account-A', status: 'offline', grants: { accountIds: [], communityIds: [] }, publication: { launchPolicy: 'restricted', activeNamedAddressCount: 0 } };
  let renders = 0;
  Object.assign(app, { root, main, deskAccount: 'account-A', accountGeneration: 1, screenSequence: 1, activeRoute: '#mine', destroyed: false,
    dialogs: new Set(), avatars: { observe() {} }, profile: { profileId: 'account-A' }, apps: [record], communities: [], group: null, view: 'mine', appStage: null,
    appSettingsDialog: null, appSettingsRouteClose: null, api: {}, options: {},
    renderPersonal() { renders++; render(); }, appStateLabel: () => 'Offline',
    openApplication() { const heading = el('h1', '', 'Preview'); heading.tabIndex = -1; main.replaceChildren(heading); app.screenSequence++; location.hash = '#preview'; heading.focus(); },
  });
  const render = () => { main.replaceChildren(createApplicationCard({ app: app.apps[0], accountId: 'account-A', communities: [], pinned: false,
    inspect: () => app.inspectApplication(app.apps[0]), open() {}, openCommunity() {}, togglePin() { return false; } })); };
  render();
  const opener = () => main.querySelector('[data-app-action="inspect"]');
  const chain = () => { opener().focus(); opener().click(); const inspect = [...app.dialogs][0]; const next = inspect.body.querySelectorAll('button').find(node => node.textContent === 'Название и доступ'); next.focus(); next.click(); return app.appSettingsDialog; };
  const changed = () => settings.onChanged({ actions: { canEdit: true }, app: { name: 'Renamed', state: 'enabled', grants: record.grants }, source: { hostDeviceId: 'host', deviceName: 'Fixture', observation: { state: 'offline' } }, publication: { launchPolicy: 'restricted', activeDomainIds: [] }, addresses: { aliases: [] } });
  return { document, location, el, app, main, dialogs, mountAccessDialogs, record, opener, chain, changed, openedWindows, rootClick: bindRootClick.call(app), settings: () => settings,
    disposed: () => disposed, renders: () => renders, drain() { while (tasks.length) tasks.shift()(); } };
}

function profileData(f) {
  f.app.openProfileMenu(); const menu = [...f.app.dialogs][0];
  assert.equal(menu.body.querySelectorAll('[data-account-section]').length, 5, 'only categories with available actions are shown');
  assert.equal(menu.body.querySelector('[data-account-section="notifications"]'), null, 'the fixture has no notification controller');
  const data = menu.body.querySelector('[data-account-section="data"]'); data.focus(); data.click();
  assert.equal(data.getAttribute('aria-current'), 'page');
  return menu;
}

function bubbleTab(target, shiftKey = false) {
  const event = new Event('keydown', { cancelable: true, bubbles: true });
  Object.defineProperties(event, { target: { value: target }, key: { value: 'Tab' }, shiftKey: { value: shiftKey } });
  let stopped = false; const stop = event.stopPropagation.bind(event); event.stopPropagation = () => { stopped = true; stop(); };
  for (let node = target; node; node = node.parentElement) { node.dispatchEvent(event); if (stopped) break; }
  return event;
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

test('cross-route app consent manages native cancel before its close task and preserves busy/account focus guards', () => {
  const f = fixture(); f.opener().focus();
  const workspace = f.el('section'), connect = f.el('button', '', 'Подключить агента к обращениям'), refresh = f.el('button');
  workspace.append(connect, refresh); f.main.replaceChildren(workspace); f.location.hash = '#access';
  assert.equal(f.document.activeElement, f.document.body, 'the previous route opener has been removed');
  let current = true;
  const panel = f.mountAccessDialogs({ workspace, feedbackTarget: {}, addFeedback: connect, refresh, alive: () => current });
  const state = panel.open('Агент для обращений приложения');
  state.busy = true; state.dialog.element.requestClose();
  assert.equal(state.dialog.element.open, true, 'a busy consent still vetoes native Escape');
  state.busy = false; state.dialog.element.requestClose();
  assert.equal(panel.isOpen(), false, 'managed cleanup must finish before the queued native close task');
  assert.equal(f.document.activeElement, connect, 'return to the current app-scoped access action');
  const next = f.el('button'); f.main.append(next); next.focus(); f.drain();
  assert.equal(f.document.activeElement, next, 'the queued close cannot steal newer focus');
  const stale = panel.open('Старое согласие'); current = false; stale.dialog.element.requestClose(); f.drain();
  assert.notEqual(f.document.activeElement, connect, 'an invalidated account cannot restore its old target');
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

test('Parent World dialog leaves Tab from a nested native dialog to that dialog', () => {
  const f = fixture(), parent = f.dialogs.createDialog('Settings'), outerLast = f.el('button', '', 'Save settings'); parent.body.append(outerLast);
  const nested = f.el('dialog'), first = f.el('button', '', 'Nested first'), last = f.el('button', '', 'Nested last'); nested.append(first, last); parent.body.append(nested); nested.showModal(); last.focus();
  const forward = bubbleTab(last); assert.equal(forward.defaultPrevented, false); assert.equal(f.document.activeElement, last); assert.equal(parent.element.open, true);
  // A parent trap must also ignore a nested target when its own focus fallback
  // would otherwise activate; the nested top-layer dialog owns that sequence.
  f.document.activeElement = f.document.body; const backward = bubbleTab(first, true);
  assert.equal(backward.defaultPrevented, false); assert.equal(f.document.activeElement, f.document.body);
  nested.close(); nested.remove(); parent.close(); f.drain();
});

test('Nested production helper traps both Tab boundaries without activating the settings trap', () => {
  const f = fixture(), settings = f.dialogs.createDialog('Settings'), outerLast = f.el('button', '', 'Save settings'); settings.body.append(outerLast); outerLast.focus();
  const helper = f.dialogs.createDialog('Helper', undefined, undefined, settings.element), input = f.el('textarea'), helperLast = f.el('button', '', 'Send'); helper.body.append(input, helperLast);
  const helperFirst = helper.element.querySelector('button'); helperLast.focus();
  const forward = bubbleTab(helperLast); assert.equal(forward.defaultPrevented, true); assert.equal(f.document.activeElement, helperFirst);
  const backward = bubbleTab(helperFirst, true); assert.equal(backward.defaultPrevented, true); assert.equal(f.document.activeElement, helperLast);
  assert.equal(settings.element.open, true); assert.equal(helper.element.open, true); assert.equal(helper.element.contains(f.document.activeElement), true);
  helper.close(); f.drain(); assert.equal(f.document.activeElement, outerLast); assert.equal(settings.element.open, true);
  const parentFirst = settings.element.querySelector('button'); outerLast.focus(); assert.equal(bubbleTab(outerLast).defaultPrevented, true); assert.equal(f.document.activeElement, parentFirst);
  assert.equal(bubbleTab(parentFirst, true).defaultPrevented, true); assert.equal(f.document.activeElement, outerLast); settings.close(); f.drain();
});

test('Closing a nested production helper keeps the parent form, current focus and close callback intact', () => {
  const f = fixture(); let parentCloses = 0, helperCloses = 0;
  const settings = f.dialogs.createDialog('Settings', () => parentCloses++), input = f.el('input'); input.value = 'Synthetic unsaved setting'; settings.body.append(input); input.focus();
  const helper = f.dialogs.createDialog('Helper', () => helperCloses++, undefined, settings.element); helper.element.requestClose(); f.drain();
  assert.equal(helperCloses, 1); assert.equal(parentCloses, 0); assert.equal(helper.element.isConnected, false); assert.equal(settings.element.open, true);
  assert.equal(input.value, 'Synthetic unsaved setting'); assert.equal(f.document.activeElement, input);
  settings.close(); f.drain(); assert.equal(parentCloses, 1); assert.equal(helperCloses, 1);
});

test('Actual World dialogs select the latest open owned modal and fence account ABA or destruction callbacks', () => {
  for (const boundary of ['account ABA', 'destroy']) {
    const f = fixture(), moves = []; let activeModal = null;
    f.app.agentOrb = { setModal(value) { activeModal = value; moves.push(value); },
      releaseModal(expected) { if (activeModal !== expected) return false; activeModal = null; return true; } };
    const older = f.app.dialog('Older settings'), newer = f.app.dialog('Newer profile');
    assert.equal(moves.at(-1), newer.element); older.element.close(); f.drain(); assert.equal(moves.at(-1), newer.element); assert.equal(f.app.dialogs.size, 1);
    newer.close(); f.drain(); assert.equal(moves.at(-1), null); assert.equal(f.app.dialogs.size, 0);
    const stale = f.app.dialog('Stale settings');
    if (boundary === 'account ABA') f.app.accountGeneration += 2; else f.app.destroyed = true;
    const count = moves.length; stale.element.close(); f.drain(); assert.equal(moves.length, count, boundary); assert.equal(f.app.dialogs.size, 0);
  }
});

test('Actual World close restores its Set only after matching release, preserving a newer raw modal/helper focus', () => {
  const f = fixture(), moves = [], releases = []; let activeModal = null;
  f.app.agentOrb = { setModal(value) { activeModal = value; moves.push(value); },
    releaseModal(expected) { const matched = activeModal === expected; releases.push({ expected, matched }); if (matched) activeModal = null; return matched; } };
  const world = f.app.dialog('Older World settings'); world.element.close();
  const raw = f.el('dialog', 'sf-dialog'), opener = f.el('button', '', 'Synthetic orb'); raw.append(opener); f.document.body.append(raw); raw.showModal(); f.app.agentOrb.setModal(raw); opener.focus();
  const helper = f.dialogs.createDialog('Helper', undefined, undefined, raw), focus = f.document.activeElement, before = moves.length; f.drain();
  assert.equal(activeModal === raw, true); assert.equal(moves.length, before); assert.equal(releases.at(-1).expected === world.element, true); assert.equal(releases.at(-1).matched, false);
  assert.equal(helper.element.open, true); assert.equal(f.document.activeElement === focus, true); assert.equal(f.app.dialogs.size, 0);
  helper.close(); f.drain(); assert.equal(raw.open, true); assert.equal(f.document.activeElement === opener, true); raw.close(); raw.remove(); f.drain();
});

test('profile Help preserves developer docs and restores its original opener after managed or native close', () => {
  for (const native of [false, true]) {
    const f = fixture(), opener = f.opener(); opener.focus();
    const developerMenu = profileData(f), docs = developerMenu.body.querySelector('[data-account-action="developers"]');
    assert.equal(f.dialogs.isDialogFocusTarget(docs), true); docs.focus(); docs.click(); f.drain();
    assert.deepEqual(f.openedWindows, [['/agents', '_blank', 'noopener']]); assert.equal(f.app.dialogs.size, 0);
    opener.focus(); const profile = profileData(f), help = profile.body.querySelector('[data-account-action="help"]');
    assert.equal(f.dialogs.isDialogFocusTarget(help), true); help.focus(); help.click();
    const modal = [...f.app.dialogs][0]; assert.notEqual(modal, profile); assert.equal(profile.element.isConnected, false);
    assert.equal(modal.element.getAttribute('aria-labelledby'), modal.element.querySelector('h2').id);
    assert.equal(modal.body.querySelectorAll('details').length, 4); assert.equal(modal.body.querySelector('details').open, true);
    if (native) modal.element.requestClose(); else {
      f.app.screenHasUnsavedChanges = () => true; f.app.afterNoteSaved = () => assert.fail('Dismissing Help must not drain unrelated work');
      const close = modal.element.querySelector('button'), event = { target: close, defaultPrevented: false,
        preventDefault() { this.defaultPrevented = true; }, stopImmediatePropagation() {} };
      f.rootClick(event); assert.equal(event.defaultPrevented, false); close.click();
    }
    f.drain(); assert.equal(f.app.dialogs.size, 0); assert.equal(f.document.activeElement, opener);
  }
});

test('Help shortcuts close before the real dirty guard, retain blocked work and reject a late account ABA', async () => {
  for (const outcome of ['blocked', 'saved', 'account ABA']) {
    const f = fixture(); let dirty = true, release, navigation = 0, warnings = 0;
    f.app.screenHasUnsavedChanges = () => dirty;
    f.app.flushScreen = () => new Promise(resolve => { release = resolve; });
    f.app.toast = () => warnings++;
    f.app.navigate = view => { assert.equal(view, 'mine'); navigation++; };
    f.app.openHelp(); const modal = [...f.app.dialogs][0];
    modal.body.querySelectorAll('button').find(node => node.textContent === 'Моё поле').click();
    assert.equal(modal.element.isConnected, false); assert.equal(navigation, 0); assert.equal(f.app.noteActionPending, true);
    if (outcome !== 'blocked') dirty = false;
    if (outcome === 'account ABA') f.app.accountGeneration += 2;
    release(); await new Promise(resolve => setImmediate(resolve)); f.drain();
    assert.equal(navigation, outcome === 'saved' ? 1 : 0); assert.equal(warnings, outcome === 'blocked' ? 1 : 0);
    assert.equal(f.app.noteActionPending, false);
  }
});

test('Core support stays keyboard reachable through profile Help and direct Help, and leaves dirty work in its current window', () => {
  for (const entry of ['profile Help', 'Help']) {
    const f = fixture(), draft = f.el('textarea'), frame = f.el('iframe'); draft.value = 'Unsaved private work'; f.main.append(draft, frame);
    const screen = f.app.screenSequence, children = [...f.main.children]; let guarded = 0;
    f.app.screenHasUnsavedChanges = () => true;
    f.app.afterNoteSaved = () => guarded++;
    f.app.navigate = () => assert.fail('Support must not navigate the working window');
    if (entry === 'profile Help') profileData(f).body.querySelector('[data-account-action="help"]').click(); else f.app.openHelp();
    const modal = [...f.app.dialogs][0], support = modal.body.querySelector('.sw-core-support-link');
    assert.ok(support, entry); assert.equal(support.tagName, 'A');
    assert.equal(support.href, 'https://feedback.4-2.xn--p1ai/p/fp_e10dd963e039c476c9139a9a2a7d4749');
    assert.equal(support.target, '_blank'); assert.equal(support.rel, 'noopener noreferrer');
    assert.equal(support.getAttribute('aria-label'), 'Поддержка Сот (в новой вкладке)');
    assert.equal(f.dialogs.isDialogFocusTarget(support), true); support.focus(); assert.equal(f.document.activeElement, support);
    const stops = modal.element.querySelectorAll('button, input, textarea, select, summary, a[href], [tabindex]').filter(f.dialogs.isDialogFocusTarget);
    assert.ok(stops.includes(support)); stops[0].focus(); bubbleTab(stops[0], true); assert.equal(f.document.activeElement, stops.at(-1));
    const event = { target: support, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, stopImmediatePropagation() {} };
    f.rootClick(event); support.click();
    assert.equal(event.defaultPrevented, false); assert.equal(guarded, 0);
    assert.equal(modal.element.open, true); assert.equal(f.app.screenSequence, screen);
    assert.deepEqual(f.main.children, children); assert.equal(draft.value, 'Unsaved private work'); assert.equal(frame.isConnected, true);
    if (entry === 'profile Help') {
      // Help delegates its own actions and is intentionally exempt from the root
      // guard. Exercise ownership/destination checks outside that exemption.
      const guide = modal.body.querySelector('a'); assert.equal(guide.href, '/help/interaction.html'); f.main.append(guide, support);
      event.target = support; f.rootClick(event); assert.equal(event.defaultPrevented, false); assert.equal(guarded, 0);
      event.target = guide;
      f.rootClick(event);
      assert.equal(event.defaultPrevented, true); assert.equal(guarded, 1, 'Other links retain the existing dirty-work guard');
      const foreign = f.el('a', 'sw-core-support-link'); foreign.target = support.target; foreign.rel = support.rel; f.main.append(foreign);
      for (const href of ['https://unrelated.example/support', support.href]) {
        foreign.href = href; event.target = foreign; event.defaultPrevented = false; f.rootClick(event);
        assert.equal(event.defaultPrevented, true, 'A class or matching URL cannot forge an owned support anchor');
      }
      support.href = 'https://unrelated.example/support'; event.target = support; event.defaultPrevented = false; f.rootClick(event);
      assert.equal(event.defaultPrevented, true, 'An owned anchor cannot bypass the guard after its destination changes');
      assert.equal(guarded, 4);
    }
  }
});

test('Actual profile editor support keeps the unsaved About form open without saving or flushing it', () => {
  const f = fixture(); let submits = 0;
  f.app.profile = { profileId: 'account-A', displayName: 'Synthetic profile', bio: '', interests: [], avatarColor: 'honey' };
  f.app.screenHasUnsavedChanges = () => true;
  f.app.afterNoteSaved = () => assert.fail('An external support link must not flush or defer the profile form');
  f.app.flushScreen = () => assert.fail('Opening support must not flush the profile form');
  f.app.updateProfile = () => assert.fail('Opening support must not save the profile');
  f.app.navigate = () => assert.fail('Opening support must not navigate the working window');
  f.app.openProfileEditor();
  const modal = [...f.app.dialogs][0], form = modal.body.querySelector('form'), about = form.querySelector('textarea');
  form.addEventListener('submit', () => submits++);
  about.value = 'Transient unsaved About marker';
  const support = modal.body.querySelector('.sw-core-support-link');
  assert.ok(support, 'The actual native profile editor needs its own support link');
  assert.equal(support.tagName, 'A'); assert.equal(support.target, '_blank'); assert.equal(support.rel, 'noopener noreferrer');
  assert.equal(support.href, 'https://feedback.4-2.xn--p1ai/p/fp_e10dd963e039c476c9139a9a2a7d4749');
  assert.equal(support.getAttribute('aria-label'), 'Поддержка Сот (в новой вкладке)');
  assert.equal(f.dialogs.isDialogFocusTarget(support), true); support.focus(); assert.equal(f.document.activeElement, support);
  const event = { target: support, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, stopImmediatePropagation() {} };
  f.rootClick(event); support.click(); f.drain();
  assert.equal(event.defaultPrevented, false); assert.equal(submits, 0);
  assert.equal(modal.element.open, true); assert.equal(form.isConnected, true);
  assert.equal(form.querySelector('textarea'), about); assert.equal(about.value, 'Transient unsaved About marker');
  assert.equal(f.app.profile.bio, '', 'The saved profile remains unchanged');
});

test('related settings keep the original profile draft and return keyboard focus', async () => {
  for (const label of ['Видимость', 'Оформление', 'Вход и сохранность аккаунта']) {
    const f = fixture(); let child, saves = 0;
    f.app.profile = { profileId: 'account-A', displayName: 'Synthetic profile', bio: '', interests: [], avatarColor: 'honey' };
    f.app.openVisibility = () => { child = f.app.dialog('Видимость'); };
    f.app.openAppearance = () => { child = f.app.dialog('Оформление'); };
    f.app.options.openAccount = async tab => { assert.equal(tab, 'recovery'); child = f.app.dialog('Восстановление'); };
    f.app.updateProfile = () => { saves++; };
    f.app.openProfileEditor();
    const parent = [...f.app.dialogs][0], form = parent.body.querySelector('form'), draft = form.querySelector('textarea');
    draft.value = 'Unsaved draft';
    const action = parent.body.querySelectorAll('button').find(node => node.textContent === label);
    action.focus(); action.click(); await new Promise(resolve => setImmediate(resolve));
    assert.ok(child?.element.open, label);
    assert.equal(parent.element.open, true, label);
    assert.equal(draft.value, 'Unsaved draft'); assert.equal(saves, 0);
    child.close(); f.drain();
    assert.equal(f.document.activeElement, action, label);
    assert.equal(draft.value, 'Unsaved draft'); assert.equal(saves, 0);
    f.app.openProfileEditor();
    assert.equal([...f.app.dialogs].filter(item => item.element.open).length, 1, 'Do not open a second profile editor');
    parent.close(); f.drain();
    f.app.openProfileEditor();
    assert.equal([...f.app.dialogs].filter(item => item.element.open).length, 1, 'A closed editor can be opened again');
  }
});

test('related profile settings cannot open after account changes or a deferred recovery handoff becomes stale', async () => {
  for (const label of ['Видимость', 'Оформление', 'Вход и сохранность аккаунта']) {
    const f = fixture(); let calls = 0;
    f.app.profile = { profileId: 'account-A', displayName: 'Synthetic profile', bio: '', interests: [], avatarColor: 'honey' };
    f.app.openVisibility = f.app.openAppearance = () => { calls++; };
    f.app.options.openAccount = async () => { calls++; };
    f.app.openProfileEditor();
    const parent = [...f.app.dialogs][0], action = parent.body.querySelectorAll('button').find(node => node.textContent === label);
    f.app.accountGeneration += 2; action.click(); await new Promise(resolve => setImmediate(resolve));
    assert.equal(calls, 0, label);
  }
  const f = fixture(); let calls = 0;
  f.app.profile = { profileId: 'account-A', displayName: 'Synthetic profile', bio: '', interests: [], avatarColor: 'honey' };
  f.app.options.openAccount = async () => { calls++; };
  f.app.openProfileEditor();
  const parent = [...f.app.dialogs][0];
  parent.body.querySelectorAll('button').find(node => node.textContent === 'Вход и сохранность аккаунта').click();
  f.app.accountGeneration += 2; await new Promise(resolve => setImmediate(resolve));
  assert.equal(calls, 0);
});

test('removed Help and stale profile entries cannot run actions after close, account ABA or destruction', () => {
  for (const boundary of ['closed', 'account ABA', 'destroy']) {
    const f = fixture(); let calls = 0; f.app.afterNoteSaved = () => calls++;
    f.app.openHelp(); const modal = [...f.app.dialogs][0], action = modal.body.querySelector('button');
    if (boundary === 'closed') modal.close();
    if (boundary === 'account ABA') f.app.accountGeneration += 2;
    if (boundary === 'destroy') f.app.destroyed = true;
    action.click(); f.drain(); assert.equal(calls, 0, boundary);
  }
  for (const boundary of ['closed', 'account ABA', 'destroy']) {
    const f = fixture(), menu = profileData(f), help = menu.body.querySelector('[data-account-action="help"]'), docs = menu.body.querySelector('[data-account-action="developers"]');
    if (boundary === 'closed') menu.close();
    if (boundary === 'account ABA') f.app.accountGeneration += 2;
    if (boundary === 'destroy') f.app.destroyed = true;
    help.click(); docs.click(); f.drain();
    assert.equal(f.app.dialogs.size, boundary === 'closed' ? 0 : 1, boundary); assert.deepEqual(f.openedWindows, [], boundary);
  }
});
