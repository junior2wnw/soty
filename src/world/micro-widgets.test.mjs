import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { setImmediate } from 'node:timers/promises';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';
import * as workspace from './workspace-widgets.mjs';
import * as launch from './app-launch.mjs';
import * as floating from './floating-layout.ts';

// Run the actual controller with inert DOM ports: no browser, network or app runtime.
const pointerGuard = {};
vm.runInNewContext(ts.transpileModule(await readFile(new URL('./iframe-pointer-guard.ts', import.meta.url), 'utf8'), {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
}).outputText, { exports: pointerGuard });
const source = ts.transpileModule(await readFile(new URL('./micro-widgets.ts', import.meta.url), 'utf8'), {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
}).outputText;
const widget = { id: 'widget-11111111-1111-4111-8111-111111111111', name: 'Tasks',
  route: 'app/app-11111111111111111111111111111111', visible: true, x: .4, y: .2, width: 320, height: 260 };
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const tick = async () => { await setImmediate(); };

function fixture({ popover = true, navigation = false, visualViewport = null } = {}) {
  const values = new Map(), storage = { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) };
  const account = 'profile-a', nodes = [], stages = [], notices = [], dialogs = [];
  workspace.editWidgetWorkspace(storage, account, state => { state.widgets.push({ ...widget }); });
  const view = new EventTarget(), doc = new EventTarget(), frames = new Map(); let nextFrame = 0;
  Object.assign(view, { localStorage: storage, innerWidth: 390, innerHeight: 844, location: { hash: '' },
    getComputedStyle: node => ({ visibility: 'visible', position: 'static', top: 'auto', ...node.computedStyle }),
    requestAnimationFrame(callback) { const id = ++nextFrame; frames.set(id, callback); return id; },
    cancelAnimationFrame(id) { frames.delete(id); } });
  Object.assign(doc, { defaultView: view, activeElement: null });
  if (visualViewport) view.visualViewport = Object.assign(new EventTarget(), visualViewport);
  let navigationRect = { top: 0, width: 0, height: 0 };
  const navigationNode = navigation ? { getBoundingClientRect: () => navigationRect } : null, observers = [];
  doc.querySelector = selector => selector === '.sw-mobile-nav' ? navigationNode : null;
  class ResizeObserver {
    constructor(callback) { this.callback = callback; this.targets = new Set(); observers.push(this); }
    observe(target) { this.targets.add(target); }
    unobserve(target) { this.targets.delete(target); }
    disconnect() { this.targets.clear(); }
  }
  const mutations = [];
  class MutationObserver {
    constructor(callback) { this.callback = callback; mutations.push(this); }
    observe() { this.connected = true; }
    disconnect() { this.connected = false; }
  }
  class Port extends EventTarget {
    constructor(tag, className = '', text = '') {
      super(); Object.assign(this, { tagName: tag.toUpperCase(), className, textContent: text, children: [], style: {}, dataset: {}, ownerDocument: doc, hidden: false, open: true });
      this.classList = { add: (...tokens) => { this.className = [...new Set([...this.className.split(' '), ...tokens])].join(' '); },
        contains: token => this.className.split(' ').includes(token) }; nodes.push(this);
    }
    append(...children) { for (const child of children) { this.children.push(child); child.parent = this; } }
    replaceChildren(...children) { for (const child of this.children) child.parent = null; this.children = []; this.append(...children); }
    setAttribute() {}
    focus() { doc.activeElement = this; }
    contains(node) { return this === node || this.children.some(child => child.contains(node)); }
    closest() { for (let node = this; node; node = node.parent) if (node.hidden || node.inert) return node; return null; }
    getClientRects() { return this.closest('[hidden],[inert]') ? [] : [this.getBoundingClientRect()]; }
    querySelectorAll(selector) {
      const selectors = selector.split(',');
      const match = node => selectors.some(token => token.startsWith('.') ? node.classList.contains(token.slice(1)) : node.tagName.toLowerCase() === token);
      return this.children.flatMap(child => [...(match(child) ? [child] : []), ...child.querySelectorAll(selector)]);
    }
    remove() { if (this.parent) this.parent.children = this.parent.children.filter(child => child !== this); this.parent = null; }
    matches() { return !!this.popoverOpen; }
    showPopover() { this.popoverOpen = true; }
    hidePopover() { this.popoverOpen = false; }
    getBoundingClientRect() { if (this.rect) return { ...this.rect, x: this.rect.left, y: this.rect.top }; return { x: parseFloat(this.style.left) || 10, y: parseFloat(this.style.top) || 60,
      width: parseFloat(this.style.width) || 320, height: parseFloat(this.style.height) || 260 }; }
    setPointerCapture(id) { this.pointer = id; }
    hasPointerCapture(id) { return this.pointer === id; }
    releasePointerCapture() { this.pointer = null; }
  }
  if (!popover) {
    Port.prototype.showPopover = undefined;
    Port.prototype.hidePopover = undefined;
    Port.prototype.matches = () => { throw new SyntaxError('Unsupported :popover-open selector'); };
  }
  const el = (...args) => new Port(...args);
  const button = (label, _icon, className, callback) => {
    const node = el('button', className, label); if (callback) node.addEventListener('click', callback); return node;
  };
  const exports = {};
  vm.runInNewContext(source, { exports, AbortController, Event, ResizeObserver, MutationObserver, Element: Port, crypto: globalThis.crypto, queueMicrotask,
    require: name => name === './dom' ? { el, button, iconButton: (label, icon, callback) => button(label, icon, '', callback) }
      : name === './iframe-pointer-guard' ? pointerGuard : name === './workspace-widgets.mjs' ? workspace : name === './app-launch.mjs' ? launch : name === './floating-layout' ? floating : {} });
  doc.head = el('head'); doc.createElement = tag => el(tag);
  const root = el('main'); let layerChanges = 0;
  const control = exports.mountMicroWidgets(root, {
    mount(host, _app, _intent, current, instanceId) {
      const stage = { host, intent: _intent, current, instanceId, disposed: false, dirty: false, flush: async () => {},
        hasUnsavedChanges() { return this.dirty; }, dispose() { this.disposed = true; },
        assistantContext: () => ({ appId: 'tasks' }) };
      stages.push(stage); return stage;
    },
    dialog(_title, onClose) {
      const element = el('dialog'), body = el('div'); element.append(body); root.append(element);
      const dialog = { element, body, close() { element.open = false; element.remove(); onClose(); } };
      dialogs.push(dialog); return dialog;
    }, notify: text => notices.push(text), onLayerChange: () => { layerChanges++; },
  });
  control.setAccount(account);
  const click = label => {
    const node = nodes.filter(node => node.textContent === label && root.contains(node)).at(-1);
    assert.ok(node, `button ${label}`); node.dispatchEvent(new Event('click'));
  };
  const remote = change => {
    workspace.editWidgetWorkspace(storage, account, change);
    const event = new Event('storage'); Object.defineProperty(event, 'key', { value: workspace.widgetStorageKey(account) }); view.dispatchEvent(event);
  };
  const notifyRoute = () => { for (const observer of mutations) if (observer.connected) observer.callback([{ target: root, type: 'childList' }], observer); };
  const chrome = (className, rect, { hidden = false, computedStyle, parent = root } = {}) => {
    const node = el('div', className); Object.assign(node, { rect, hidden, computedStyle }); parent.append(node); return node;
  };
  return { control, stages, notices, root, nodes, view, doc, el, click, remote, storage, account, frames, dialogs,
    frame() { const batch = [...frames.values()]; frames.clear(); for (const callback of batch) callback(); },
    observers, mutations, chrome, notifyRoute, setNavigation(rect) { navigationRect = rect; },
    notifyChrome(node) { for (const observer of observers) if (observer.targets.has(node)) observer.callback([], observer); },
    notifyNavigation() { for (const observer of observers) if (observer.targets.has(navigationNode)) observer.callback([], observer); },
    iframePointersBlocked: () => doc.head.children.some(node => node.tagName === 'STYLE' && /iframe\s*\{\s*pointer-events:\s*none\s*!important\s*;?\s*\}/.test(node.textContent)),
    layerChanges: () => layerChanges, read: () => workspace.readWidgetWorkspace(storage, account) };
}

test('launcher reserves a header slot on the audited 320/390 sizes without moving or remounting the saved workspace', t => {
  const f = fixture(); t.after(() => f.control.dispose());
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account)), stage = f.stages[0];
  const manager = f.nodes.find(node => node.textContent === 'Личные виджеты');
  for (const [width, height, rect] of [[320, 568, { left: 265, top: 58.5, width: 44, height: 44 }], [390, 844, { left: 334, top: 74, width: 36, height: 44 }]]) {
    Object.assign(f.view, { innerWidth: width, innerHeight: height });
    f.doc.querySelectorAll = () => [{ classList: { contains: () => false }, closest: () => null, getClientRects: () => [rect], getBoundingClientRect: () => rect }];
    f.view.dispatchEvent(new Event('resize'));
    assert.equal(manager.style.left, `${width - 100}px`); assert.equal(manager.style.top, '6px');
    assert.equal(f.root.dataset.microWidgets, 'ready'); assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
    assert.equal(f.stages[0], stage); assert.equal(f.stages.length, 1);
  }
});

test('tap controls move and resize an existing live widget, and stale account controls cannot edit another workspace', t => {
  const f = fixture(); t.after(() => f.control.dispose());
  const original = { ...f.read().widgets[0] }, stage = f.stages[0];
  f.click('Личные виджеты'); f.click('Положение и размер');
  const editor = f.dialogs.at(-1), left = f.nodes.find(node => node.textContent === 'Влево' && editor.body.contains(node));
  assert.ok(left); left.dispatchEvent(new Event('click')); assert.ok(f.read().widgets[0].x < original.x);
  f.click('Шире'); assert.equal(f.read().widgets[0].width, original.width + 40);
  assert.equal(f.stages.length, 1); assert.equal(f.stages[0], stage); assert.equal(stage.disposed, false);
  f.click('Исходное положение'); assert.equal(f.read().widgets[0].width, 320); assert.equal(f.read().widgets[0].x, .65);
  f.control.setAccount('profile-b'); left.dispatchEvent(new Event('click'));
  assert.equal(editor.element.open, false); assert.equal(workspace.readWidgetWorkspace(f.storage, 'profile-b').widgets.length, 0);
});

test('mobile navigation retains its full hit area without changing a pinned desktop workspace or remounting its frame', async t => {
  const f = fixture(); t.after(() => f.control.dispose());
  f.remote(state => { Object.assign(state.widgets[0], { width: 640, height: 640, x: 1, y: 1 }); });
  await tick();
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account));
  const node = f.nodes.find(item => item.className === 'sm-widget');
  const stage = f.stages[0];
  for (const [width, height, navigationHeight] of [[320, 568, 64], [390, 844, 84]]) {
    Object.assign(f.view, { innerWidth: width, innerHeight: height });
    const top = height - navigationHeight;
    f.doc.querySelector = selector => selector === '.sw-mobile-nav'
      ? { getBoundingClientRect: () => ({ top, width, height: navigationHeight }) } : null;
    f.view.dispatchEvent(new Event('resize'));
    const rect = node.getBoundingClientRect();
    assert.ok(rect.y + rect.height <= top - 8, 'widget must leave the entire navigation plus an 8px gap');
    assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
    assert.equal(f.stages.length, 1);
    assert.equal(stage.disposed, false);
    assert.equal(stage.current(), true);
  }
  Object.assign(f.view, { innerWidth: 1280, innerHeight: 800 });
  f.doc.querySelector = () => ({ getBoundingClientRect: () => ({ top: 0, width: 0, height: 0 }) });
  f.view.dispatchEvent(new Event('resize'));
  assert.deepEqual(node.getBoundingClientRect(), workspace.widgetRect(f.read().widgets[0], { left: 0, top: 0, width: 1280, height: 800 }));
  assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
});

test('navigation appearing after responsive resize is reserved before the next paint and pending layout is cleaned up', async () => {
  const f = fixture();
  f.remote(state => { Object.assign(state.widgets[0], { width: 640, height: 640, y: 1 }); });
  await tick();
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account));
  const node = f.nodes.find(item => item.className === 'sm-widget');
  let navigation = null;
  f.doc.querySelector = () => navigation;
  Object.assign(f.view, { innerWidth: 320, innerHeight: 568 });
  f.view.dispatchEvent(new Event('resize'));
  f.view.dispatchEvent(new Event('resize'));
  assert.equal(f.frames.size, 1, 'resize work is coalesced');
  navigation = { getBoundingClientRect: () => ({ top: 504, width: 320, height: 64 }) };
  f.frame();
  const rect = node.getBoundingClientRect();
  assert.ok(rect.y + rect.height <= 496);
  assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
  assert.equal(f.stages.length, 1);
  f.view.dispatchEvent(new Event('resize'));
  f.control.dispose();
  assert.equal(f.frames.size, 0);
});

test('navigation geometry changes without a viewport resize preserve the live widget, stored workspace and cleanup', async () => {
  const f = fixture({ navigation: true });
  f.remote(state => { Object.assign(state.widgets[0], { width: 640, height: 640, y: 1 }); });
  await tick();
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account));
  const node = f.nodes.find(item => item.className === 'sm-widget'), stage = f.stages[0];
  const bottom = () => { const rect = node.getBoundingClientRect(); return rect.y + rect.height; };
  assert.equal(bottom(), 808, 'hidden navigation permits the saved bottom position');
  f.setNavigation({ top: 760, width: 390, height: 84 });
  f.notifyNavigation();
  assert.ok(bottom() <= 752, 'Back must reserve the newly visible navigation without a resize event');
  f.frame();
  f.setNavigation({ top: 780, width: 390, height: 64 });
  f.notifyNavigation();
  assert.ok(bottom() <= 772, 'a changed navigation height must also update the clamp');
  f.setNavigation({ top: 0, width: 0, height: 0 });
  f.notifyNavigation(); f.frame();
  assert.equal(bottom(), 808, 'entering a chat restores the saved position without writing it');
  assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
  assert.equal(f.stages.length, 1); assert.equal(stage.disposed, false); assert.equal(stage.current(), true);
  const late = f.observers[0].callback;
  f.notifyNavigation();
  f.control.dispose();
  assert.equal(f.frames.size, 0); assert.equal(f.observers[0].targets.size, 0);
  late([], f.observers[0]);
  assert.equal(f.frames.size, 0, 'a queued observer callback after dispose must not schedule work');
});

for (const [width, height, navHeight, headTop, contactTop] of [[320, 568, 64, 137.17, 141.17], [390, 844, 84, 70, 74]])
test(`a saved 640px widget leaves the audited ${width}px inbox contacts target fully exposed without saving or flushing`, async t => {
  const f = fixture({ navigation: true }); t.after(() => f.control.dispose());
  f.remote(state => { Object.assign(state.widgets[0], { width: 640, height: 640, x: 1, y: 0 }); }); await tick();
  Object.assign(f.view, { innerWidth: width, innerHeight: height });
  f.setNavigation({ top: height - navHeight, width, height: navHeight });
  f.chrome('sw-topbar', { left: 0, top: 0, width, height: 56 });
  const head = f.chrome('sw-inbox-head', { left: 8, top: headTop, width: width - 16, height: 66 });
  const contacts = { left: width - 56, top: contactTop, width: 36, height: 44 };
  const stage = f.stages[0], node = stage.host.parent;
  stage.dirty = true; let flushes = 0; stage.flush = async () => { flushes++; };
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account));
  f.notifyRoute(); f.frame();
  const rect = node.getBoundingClientRect();
  assert.ok(rect.y >= head.rect.top + head.rect.height + 8);
  assert.ok(rect.y + rect.height <= height - navHeight - 8);
  assert.ok(rect.x >= contacts.left + contacts.width || rect.x + rect.width <= contacts.left
    || rect.y >= contacts.top + contacts.height || rect.y + rect.height <= contacts.top, 'the entire target is outside the widget');
  assert.ok(rect.width > 0 && rect.height >= 48);
  assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
  assert.equal(f.stages.length, 1); assert.equal(stage.disposed, false); assert.equal(stage.current(), true);
  assert.equal(flushes, 0); assert.equal(f.control.hasUnsavedChanges(), true);
  assert.equal(node.parent.popoverOpen, true, 'the widget stays in its always-on-top layer');
  const manager = f.nodes.find(item => item.textContent === 'Личные виджеты');
  assert.equal(manager.style.top, '6px'); assert.equal(manager.style.left, `${width - 100}px`, 'the global launcher must not inherit the widget top inset');
});

test('desktop to compact and back restores the same safe projection for the same screen, including fractional positions', async t => {
  const f = fixture({ navigation: true }); t.after(() => f.control.dispose());
  f.remote(state => { Object.assign(state.widgets[0], { width: 640, height: 640, x: .64998, y: .11998 }); }); await tick();
  Object.assign(f.view, { innerWidth: 1280, innerHeight: 900 });
  const toolbar = f.chrome('sa-toolbar', { left: 96, top: 60, width: 1184, height: 64 }); f.notifyRoute(); f.frame();
  const node = f.stages[0].host.parent, original = node.getBoundingClientRect();
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account));
  Object.assign(f.view, { innerWidth: 320, innerHeight: 568 });
  toolbar.rect = { left: 0, top: 56, width: 320, height: 112 }; f.setNavigation({ top: 504, width: 320, height: 64 });
  f.view.dispatchEvent(new Event('resize')); f.frame();
  assert.ok(node.getBoundingClientRect().y >= 176); assert.ok(node.getBoundingClientRect().height < 640);
  Object.assign(f.view, { innerWidth: 1280, innerHeight: 900 });
  toolbar.rect = { left: 96, top: 60, width: 1184, height: 64 }; f.setNavigation({ top: 0, width: 0, height: 0 });
  f.view.dispatchEvent(new Event('resize')); f.frame();
  assert.deepEqual(node.getBoundingClientRect(), original);
  assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved); assert.equal(f.stages.length, 1);
});

test('explicit keyboard and tap movement invert the current shell inset without changing the live application', t => {
  const f = fixture(); t.after(() => f.control.dispose());
  f.chrome('sw-inbox-head', { left: 0, top: 130, width: 390, height: 70 }); f.notifyRoute();
  const stage = f.stages[0], node = stage.host.parent, grip = f.nodes.find(item => item.className === 'sm-widget-grip');
  const before = node.getBoundingClientRect();
  const key = new Event('keydown'); Object.assign(key, { key: 'ArrowDown' }); grip.dispatchEvent(key);
  assert.ok(Math.abs(node.getBoundingClientRect().y - before.y - 16) < 1e-9);
  f.click('Личные виджеты'); f.click('Положение и размер'); f.click('Вверх');
  assert.ok(Math.abs(node.getBoundingClientRect().y - before.y + 16) < 1e-9);
  assert.equal(f.stages.length, 1); assert.equal(f.stages[0], stage); assert.equal(stage.disposed, false);
});

test('route replacement rebinds actual inbox, chat and main app headers then restores the saved desktop geometry', async t => {
  const f = fixture({ navigation: true }); t.after(() => f.control.dispose());
  f.remote(state => { Object.assign(state.widgets[0], { width: 640, height: 640, x: .65, y: .12 }); }); await tick();
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account)), stage = f.stages[0], node = stage.host.parent;
  Object.assign(f.view, { innerWidth: 320, innerHeight: 568 });
  f.setNavigation({ top: 504, width: 320, height: 64 });
  const inbox = f.chrome('sw-inbox-head', { left: 8, top: 136, width: 304, height: 68 }); f.notifyRoute();
  assert.ok(node.getBoundingClientRect().y >= 212); assert.ok(f.observers[0].targets.has(inbox));
  inbox.remove(); f.setNavigation({ top: 0, width: 0, height: 0 });
  const chat = f.chrome('sw-chat-header', { left: 0, top: 0, width: 320, height: 120 }); f.notifyRoute();
  assert.ok(node.getBoundingClientRect().y >= 128); assert.equal(f.observers[0].targets.has(inbox), false);
  chat.rect.height = 144; f.notifyChrome(chat);
  assert.ok(node.getBoundingClientRect().y >= 152, 'font/header reflow is reserved without a route or viewport event');
  chat.remove(); const toolbar = f.chrome('sa-toolbar', { left: 0, top: 56, width: 320, height: 112 }); f.notifyRoute();
  assert.ok(node.getBoundingClientRect().y >= 176, 'wrapped main app actions are reserved');
  assert.equal(f.observers[0].targets.has(chat), false); assert.ok(f.observers[0].targets.has(toolbar));
  toolbar.remove(); Object.assign(f.view, { innerWidth: 1280, innerHeight: 900 }); f.notifyRoute(); f.frame();
  assert.deepEqual(node.getBoundingClientRect(), workspace.widgetRect(f.read().widgets[0], { left: 0, top: 0, width: 1280, height: 900 }));
  assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
  assert.equal(f.stages.length, 1); assert.equal(stage.disposed, false); assert.equal(stage.current(), true);
  assert.equal(f.observers[0].targets.has(toolbar), false);
});

test('field controls reserve only their rendered rows, not a stretched toolbar or the field content', t => {
  const f = fixture(); t.after(() => f.control.dispose());
  Object.assign(f.view, { innerWidth: 1280, innerHeight: 900 });
  const node = f.stages[0].host.parent, saved = f.storage.getItem(workspace.widgetStorageKey(f.account));
  f.chrome('uf-screen-head', { left: 96, top: 60, width: 1184, height: 132 });
  const bar = f.chrome('uf-context-toolbar', { left: 100, top: 200, width: 1170, height: 600 },
    { computedStyle: { position: 'absolute', top: '4px', bottom: '24px' } });
  const zoom = f.chrome('sw-icon-button', { left: 1200, top: 204, width: 44, height: 44 }, { parent: bar }); zoom.tagName = 'BUTTON';
  f.chrome('uf-viewport', { left: 96, top: 200, width: 1184, height: 660 });
  f.chrome('sa-toolbar', { left: 0, top: 200, width: 1280, height: 680 }, { parent: f.stages[0].host });
  f.notifyRoute();
  assert.ok(node.getBoundingClientRect().y >= 256);
  assert.equal(node.getBoundingClientRect().height, widget.height, 'the scene and the stretched toolbar remain available behind the widget');
  bar.computedStyle = { position: 'absolute', top: 'auto', bottom: '24px' };
  zoom.rect.top = 824; f.notifyChrome(bar);
  const rect = node.getBoundingClientRect(); assert.ok(rect.y >= 200); assert.ok(rect.y + rect.height <= 816);
  assert.equal(rect.height, widget.height);
  assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
});

test('hidden, offscreen and widget-owned chrome cannot move the workspace; scrolling a live shell header does', t => {
  const f = fixture(); t.after(() => f.control.dispose());
  const node = f.stages[0].host.parent, before = node.getBoundingClientRect();
  const hidden = f.chrome('sw-inbox-head', { left: 0, top: 60, width: 390, height: 700 }, { hidden: true });
  f.chrome('sw-chat-header', { left: 0, top: 900, width: 390, height: 80 });
  f.chrome('sa-toolbar', { left: 400, top: 60, width: 100, height: 700 });
  f.chrome('sw-pwa-banner', { left: 0, top: 60, width: 390, height: 700 }, { computedStyle: { visibility: 'hidden' } });
  f.chrome('sw-app-toolbar', { left: NaN, top: 60, width: 390, height: 700 });
  f.chrome('sw-chat-header', { left: 0, top: 60, width: 390, height: 700 }, { parent: f.stages[0].host });
  f.notifyRoute(); assert.deepEqual(node.getBoundingClientRect(), before);
  hidden.hidden = false; hidden.rect = { left: 0, top: 100, width: 390, height: 70 }; f.notifyRoute();
  assert.ok(node.getBoundingClientRect().y >= 178);
  hidden.rect.top = -100; f.root.dispatchEvent(new Event('scroll'));
  assert.deepEqual(node.getBoundingClientRect(), before, 'scrolling the header out of view restores the stored preference');
});

test('visual viewport keyboard offsets project a live widget and restore it without modifying its stored size or draft', async t => {
  const f = fixture({ navigation: true, visualViewport: { offsetLeft: 0, offsetTop: 0, width: 390, height: 844 } });
  t.after(() => f.control.dispose());
  f.remote(state => { Object.assign(state.widgets[0], { width: 640, height: 640, x: 1, y: 1 }); }); await tick();
  const stage = f.stages[0], node = stage.host.parent, before = node.getBoundingClientRect();
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account)); stage.dirty = true;
  Object.assign(f.view.visualViewport, { offsetLeft: 11, offsetTop: 180, width: 320, height: 340 });
  f.setNavigation({ top: 456, width: 320, height: 64 });
  const header = f.chrome('sw-chat-header', { left: 11, top: 180, width: 320, height: 104 });
  f.view.visualViewport.dispatchEvent(new Event('resize')); f.frame();
  const rect = node.getBoundingClientRect();
  assert.ok(rect.x >= 17 && rect.x + rect.width <= 325); assert.ok(rect.y >= 292 && rect.y + rect.height <= 448);
  header.remove(); f.setNavigation({ top: 0, width: 0, height: 0 });
  Object.assign(f.view.visualViewport, { offsetLeft: 0, offsetTop: 0, width: 390, height: 844 });
  f.view.visualViewport.dispatchEvent(new Event('scroll')); f.frame();
  assert.deepEqual(node.getBoundingClientRect(), before); assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
  assert.equal(f.stages.length, 1); assert.equal(stage.disposed, false); assert.equal(stage.dirty, true);
});

test('a viewport shorter than its combined chrome keeps the widget header reachable and cancels pending observers on dispose', t => {
  const f = fixture({ navigation: true, visualViewport: { offsetLeft: 0, offsetTop: 20, width: 320, height: 180 } });
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account));
  f.setNavigation({ top: 136, width: 320, height: 64 });
  f.chrome('sw-chat-header', { left: 0, top: 20, width: 320, height: 120 }); f.notifyRoute();
  const rect = f.stages[0].host.parent.getBoundingClientRect();
  assert.ok(rect.height >= 48); assert.ok(rect.y >= 20 && rect.y + rect.height <= 200);
  assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved);
  const lateResize = f.observers[0].callback, lateRoute = f.mutations[0].callback;
  f.control.dispose(); lateResize([], f.observers[0]); lateRoute([{ target: f.root }], f.mutations[0]);
  assert.equal(f.frames.size, 0); assert.equal(f.observers[0].targets.size, 0); assert.equal(f.mutations[0].connected, false);
});

test('an old hide completion cannot close a replacement stage with the same widget id', async t => {
  const f = fixture(); t.after(() => f.control.dispose());
  const pending = deferred(), old = f.stages[0]; let calls = 0;
  old.flush = () => ++calls === 1 ? pending.promise : Promise.resolve();
  f.click('Скрыть виджет: Tasks');
  f.remote(state => { state.widgets[0].visible = false; }); await tick();
  f.remote(state => { state.widgets[0].visible = true; }); await tick();
  assert.equal(f.stages.length, 2); const replacement = f.stages[1]; replacement.dirty = true;
  pending.resolve(); await tick();
  assert.equal(replacement.disposed, false); assert.equal(replacement.current(), true);
  assert.equal(f.read().widgets[0].visible, true);
});

test('hide all cannot close a newly added stage which was never flushed', async t => {
  const f = fixture(); t.after(() => f.control.dispose());
  const pending = deferred(); f.stages[0].flush = () => pending.promise;
  f.click('Личные виджеты'); f.click('Скрыть все виджеты');
  f.control.pin({ appId: 'app-22222222222222222222222222222222', name: 'Other' },
    launch.parseAppLaunchRoute('app/app-22222222222222222222222222222222'));
  let newFlushes = 0; f.stages[1].flush = async () => { newFlushes++; };
  pending.resolve(); await tick();
  assert.equal(newFlushes, 0); assert.equal(f.stages[1].disposed, false); assert.equal(f.read().hidden, false);
});

test('remote removal flushes first and preserves a still dirty stage and its next local edit', async t => {
  const f = fixture(); t.after(() => f.control.dispose());
  let flushes = 0; f.stages[0].dirty = true; f.stages[0].flush = async () => { flushes++; };
  f.remote(state => { state.widgets = []; }); await tick();
  assert.equal(flushes, 1); assert.equal(f.stages[0].disposed, false); assert.equal(f.notices.length, 1);
  f.control.pin({ appId: 'app-22222222222222222222222222222222', name: 'Other' },
    launch.parseAppLaunchRoute('app/app-22222222222222222222222222222222'));
  assert.equal(f.stages[0].disposed, false); assert.equal(f.read().widgets.length, 2);
});

test('remote hide waits for a successful flush before disposing the stage', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const pending = deferred();
  f.stages[0].dirty = true; f.stages[0].flush = async () => { await pending.promise; f.stages[0].dirty = false; };
  f.remote(state => { state.hidden = true; }); await tick(); assert.equal(f.stages[0].disposed, false);
  pending.resolve(); await tick(); assert.equal(f.stages[0].disposed, true); assert.equal(f.notices.length, 0);
});

test('remote flush rejection keeps the live stage and reports the deferred layout', async t => {
  const f = fixture(); t.after(() => f.control.dispose());
  f.stages[0].flush = async () => { throw new Error('unsaved'); };
  f.remote(state => { state.hidden = true; }); await tick();
  assert.equal(f.stages[0].disposed, false); assert.equal(f.notices.length, 1);
});

test('account A to B to A invalidates an old remote flush completion', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const pending = deferred();
  f.stages[0].flush = () => pending.promise;
  f.remote(state => { state.hidden = true; });
  f.control.setAccount('profile-b');
  workspace.editWidgetWorkspace(f.storage, f.account, state => { state.hidden = false; });
  f.control.setAccount(f.account); assert.equal(f.stages.length, 2);
  pending.resolve(); await tick(); assert.equal(f.stages[1].disposed, false); assert.equal(f.stages[1].current(), true);
});

test('a local edit cannot import another tab removal before its flush finishes', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const pending = deferred();
  f.stages[0].flush = () => pending.promise;
  f.remote(state => { state.widgets = []; });
  f.control.pin({ appId: 'app-22222222222222222222222222222222', name: 'Other' },
    launch.parseAppLaunchRoute('app/app-22222222222222222222222222222222'));
  pending.resolve(); await tick();
  assert.equal(f.stages[0].disposed, false); assert.equal(f.read().widgets.length, 2);
});

test('main iframe focus clears widget context while agent button focus retains it', async t => {
  const f = fixture(); t.after(() => f.control.dispose());
  const iframe = f.el('iframe'); f.stages[0].host.append(iframe); f.doc.activeElement = iframe;
  f.view.dispatchEvent(new Event('blur')); await tick(); assert.ok(f.control.assistantContext());
  f.doc.activeElement = f.el('button'); f.view.dispatchEvent(new Event('blur')); await tick(); assert.ok(f.control.assistantContext());
  f.doc.activeElement = f.el('iframe'); f.view.dispatchEvent(new Event('blur')); await tick(); assert.equal(f.control.assistantContext(), null);
});

test('pointer cancellation restores layout without persisting the preview', t => {
  const f = fixture(); t.after(() => f.control.dispose());
  const grip = f.nodes.find(node => node.className === 'sm-widget-grip'), node = f.nodes.find(node => node.className === 'sm-widget');
  const before = { ...node.style }, saved = JSON.stringify(f.read());
  const pointer = (type, x) => { const event = new Event(type); Object.assign(event, { pointerId: 7, button: 0, isPrimary: true, clientX: x, clientY: 100 }); grip.dispatchEvent(event); };
  pointer('pointerdown', 100); pointer('pointermove', 130); assert.notEqual(node.style.left, before.left);
  pointer('pointercancel', 130); pointer('pointerup', 130);
  assert.equal(node.style.left, before.left); assert.equal(JSON.stringify(f.read()), saved);
  const changes = f.layerChanges(); f.control.setAccount(''); assert.ok(f.layerChanges() > changes);
});


test('manager brings the chosen widget forward without moving or replacing its live stage; new pins stagger', t => {
  const f = fixture(); t.after(() => f.control.dispose());
  const original = f.read().widgets[0];
  f.control.pin({ appId: 'app-22222222222222222222222222222222', name: 'Other' },
    launch.parseAppLaunchRoute('app/app-22222222222222222222222222222222'));
  const pinned = f.read().widgets;
  assert.equal(pinned[0].x, original.x); assert.equal(pinned[0].y, original.y);
  assert.notEqual(pinned[1].y, original.y); assert.ok(pinned[1].x <= .95 && pinned[1].y <= .75);
  const first = f.stages[0], hostParent = first.host.parent, layer = hostParent.parent, order = [...layer.children];
  f.click('Личные виджеты');
  const action = f.nodes.find(node => node.textContent === 'На передний план' && f.root.contains(node)
    && node.parent.children[0].textContent === 'Tasks');
  assert.ok(action); action.dispatchEvent(new Event('click'));
  assert.equal(f.read().widgets.at(-1).id, original.id);
  assert.equal(f.stages.length, 2); assert.equal(first.disposed, false); assert.equal(first.current(), true);
  assert.equal(first.host.parent, hostParent); assert.equal(hostParent.parent, layer);
  assert.deepEqual(layer.children, order);
  assert.equal(f.doc.activeElement.className, 'sm-widget-grip'); assert.ok(hostParent.contains(f.doc.activeElement));
  assert.equal(f.nodes.filter(node => node.tagName === 'DIALOG').at(-1).open, false);
});


test('browsers without Popover API never query its unsupported selector and retain CSS fallback', t => {
  const f = fixture({ popover: false }); t.after(() => f.control.dispose());
  assert.equal(f.stages.length, 1); assert.equal(f.stages[0].current(), true);
  f.control.setAccount(''); assert.equal(f.stages[0].disposed, true);
  f.doc.dispatchEvent(new Event('fullscreenchange'));
  f.control.setAccount('profile-b');
  f.doc.dispatchEvent(new Event('fullscreenchange'));
  f.control.setAccount(f.account); assert.equal(f.stages.length, 2);
  f.doc.dispatchEvent(new Event('fullscreenchange'));
  assert.equal(f.stages[1].current(), true); assert.equal(f.stages[1].disposed, false);
  assert.ok(f.root.contains(f.stages[1].host));
});


test('actual orb context never falls back to main A while focused widget B is pending or failed', async t => {
  const app = ts.createSourceFile('app.ts', await readFile(new URL('./app.ts', import.meta.url), 'utf8'), ts.ScriptTarget.Latest, true);
  let context;
  const visit = node => {
    if (ts.isCallExpression(node) && node.expression.getText(app) === 'mountAgentOrb') {
      context = node.arguments[1].properties.find(property => ts.isPropertyAssignment(property) && property.name.getText(app) === 'context').initializer.getText(app);
    }
    ts.forEachChild(node, visit);
  };
  visit(app); assert.ok(context, 'use the actual WorldApplication orb context callback');
  const f = fixture(); t.after(() => f.control.dispose());
  const mainA = { appId: 'main-a' }, widgetB = { appId: 'widget-b' };
  let ready = false; f.stages[0].assistantContext = () => ready ? widgetB : null;
  const owner = { widgets: f.control, appStage: { assistantContext: () => mainA } };
  const readContext = vm.runInNewContext(`(function () { return (${context}); })`).call(owner);
  assert.equal(readContext(), mainA);
  f.stages[0].host.parent.dispatchEvent(new Event('focusin'));
  assert.equal(readContext(), null, 'pending widget B must not borrow main A context');
  f.doc.activeElement = f.el('button'); f.view.dispatchEvent(new Event('blur')); await tick();
  assert.equal(readContext(), null, 'failed widget B stays selected when the orb gets focus');
  f.control.clearActive(); assert.equal(readContext(), mainA);
  f.stages[0].host.parent.dispatchEvent(new Event('focusin')); ready = true;
  assert.equal(readContext(), widgetB);
});


test('keyboard focus raises an overlapped widget while preserving its control, iframe and stage', async t => {
  const f = fixture(); t.after(() => f.control.dispose());
  f.control.pin({ appId: 'app-22222222222222222222222222222222', name: 'Other' },
    launch.parseAppLaunchRoute('app/app-22222222222222222222222222222222'));
  f.remote(state => { for (const item of state.widgets) { item.x = .4; item.y = .2; } }); await tick();
  const back = f.stages[0], front = f.stages[1], node = back.host.parent, frontNode = front.host.parent;
  const layer = node.parent, children = [...layer.children], frame = f.el('iframe'); back.host.append(frame);
  const context = { appId: 'focused-back-widget' }; back.assistantContext = () => context;
  const grip = node.children[0].children[0];
  assert.deepEqual(node.getBoundingClientRect(), frontNode.getBoundingClientRect());
  assert.ok(Number(node.style.zIndex) < Number(frontNode.style.zIndex));
  grip.focus(); node.dispatchEvent(new Event('focusin'));
  assert.ok(Number(node.style.zIndex) > Number(frontNode.style.zIndex), 'keyboard-selected widget must be visible above the overlap');
  assert.equal(f.doc.activeElement, grip); assert.equal(f.control.assistantContext(), context);
  assert.equal(f.read().widgets.at(-1).id, widget.id);
  assert.equal(f.stages.length, 2); assert.equal(back.disposed, false); assert.equal(front.disposed, false);
  assert.equal(back.current(), true); assert.equal(front.current(), true);
  assert.equal(frame.parent, back.host); assert.equal(back.host.parent, node); assert.equal(node.parent, layer);
  assert.deepEqual(layer.children, children);
});

const dragPointer = (handle, type, x = 100, y = 100) => {
  const event = new Event(type); Object.assign(event, { pointerId: 7, button: 0, isPrimary: true, clientX: x, clientY: y }); handle.dispatchEvent(event);
};

for (const end of ['pointerup', 'pointercancel', 'Escape']) test(`widget ${end} after dragging does not open its tap menu, while a fresh tap does`, t => {
  const f = fixture(); t.after(() => f.control.dispose());
  const grip = f.nodes.find(node => node.className === 'sm-widget-grip');
  dragPointer(grip, 'pointerdown'); dragPointer(grip, 'pointermove', 132, 132);
  if (end === 'Escape') { const key = new Event('keydown'); Object.assign(key, { key: 'Escape' }); grip.dispatchEvent(key); }
  else dragPointer(grip, end, 132, 132);
  const click = () => { const event = new Event('click'); Object.assign(event, { detail: 1 }); grip.dispatchEvent(event); };
  click(); assert.equal(f.dialogs.length, 0);
  dragPointer(grip, 'pointerdown'); dragPointer(grip, 'pointerup'); click();
  assert.equal(f.dialogs.length, 1); assert.equal(f.dialogs[0].element.open, true);
});
test('resize crosses a Native iframe without losing capture and saves the final size on the same stage', t => {
  const f = fixture(); t.after(() => f.control.dispose()); const stage = f.stages[0];
  const mainFrame = f.el('iframe'), widgetFrame = f.el('iframe'); f.root.append(mainFrame); stage.host.append(widgetFrame);
  const handle = f.nodes.find(node => node.title === 'Потяните, чтобы изменить размер');
  dragPointer(handle, 'pointerdown'); dragPointer(handle, 'pointermove', 112, 108);
  // Browser boundary port: an unshielded child frame takes the pointer stream.
  if (!f.iframePointersBlocked()) dragPointer(handle, 'lostpointercapture', 112, 108);
  dragPointer(handle, 'pointermove', 140, 140); dragPointer(handle, 'pointerup', 140, 140);
  assert.equal(f.read().widgets[0].width, 360); assert.equal(f.read().widgets[0].height, 300);
  assert.equal(f.iframePointersBlocked(), false); assert.equal(f.stages.length, 1);
  assert.equal(f.stages[0], stage); assert.equal(stage.disposed, false); assert.equal(stage.host.children[0], widgetFrame); assert.equal(mainFrame.parent, f.root);
});

for (const kind of ['move', 'size']) for (const end of ['pointerup', 'pointercancel', 'lostpointercapture', 'Escape', 'hide', 'account switch', 'dispose']) {
  test(`${kind} drag restores iframe interaction after ${end}`, async t => {
    const f = fixture(); t.after(() => f.control.dispose()); const before = f.read().widgets[0];
    const handle = f.nodes.find(node => kind === 'move' ? node.className === 'sm-widget-grip' : node.title === 'Потяните, чтобы изменить размер');
    dragPointer(handle, 'pointerdown'); assert.equal(f.iframePointersBlocked(), true);
    dragPointer(handle, 'pointermove', 128, 128);
    if (end === 'Escape') { const event = new Event('keydown'); Object.assign(event, { key: 'Escape' }); handle.dispatchEvent(event); }
    else if (end === 'hide') { f.click('Скрыть виджет: Tasks'); await tick(); }
    else if (end === 'account switch') f.control.setAccount('profile-b');
    else if (end === 'dispose') f.control.dispose();
    else dragPointer(handle, end, 128, 128);
    assert.equal(f.iframePointersBlocked(), false); assert.equal(handle.hasPointerCapture(7), false);
    if (end !== 'pointerup') {
      dragPointer(handle, 'pointerup', 128, 128);
      const after = f.read().widgets[0];
      for (const key of ['x', 'y', 'width', 'height']) assert.equal(after[key], before[key]);
    }
  });
}
test('rejected widget pointer capture releases the guard without saving a layout', t => {
  const f = fixture(); t.after(() => f.control.dispose()); const before = f.read();
  const handle = f.nodes.find(node => node.className === 'sm-widget-grip');
  handle.setPointerCapture = () => { throw new Error('Synthetic inactive pointer'); };
  dragPointer(handle, 'pointerdown'); assert.equal(f.iframePointersBlocked(), false); assert.deepEqual(f.read(), before);
});

test('saved widget instance locator survives hide/show and remains distinct from main', async () => {
  const f=fixture(); f.control.setAccount(f.account);
  assert.equal(f.stages[0].instanceId,widget.id);
  f.control.setAccount('another');f.control.setAccount(f.account);
  assert.equal(f.stages.at(-1).instanceId,widget.id);assert.notEqual(widget.id,'main');f.control.dispose();
});

const refreshedEntry = (path = '/current') => launch.parseAppLaunchRoute(launch.formatAppLaunchRoute({
  appId: 'app-11111111111111111111111111111111', domainId: 'dom_11111111111111111111111111111111', path,
}));

test('explicit entry replacement creates one new stage for the selected widget and preserves workspace geometry, identity and other frames', async t => {
  const f = fixture(); t.after(() => f.control.dispose());
  f.control.pin({ appId: 'app-22222222222222222222222222222222', name: 'Other' }, launch.parseAppLaunchRoute('app/app-22222222222222222222222222222222'));
  f.remote(state => { Object.assign(state.widgets.find(item => item.id === widget.id), { x: .17, y: .81, width: 410, height: 360 }); }); await tick();
  const before = f.read(), old = f.stages[0], other = f.stages[1], oldTarget = { ...old.intent.target }, intent = refreshedEntry();
  assert.equal(await f.control.replaceEntry(widget.id, intent), true);
  const fresh = f.stages.at(-1); assert.equal(f.stages.length, 3); assert.equal(old.disposed, true); assert.equal(old.current(), false);
  assert.equal(other.disposed, false); assert.equal(other.current(), true); assert.equal(fresh.current(), true); assert.equal(fresh.instanceId, widget.id);
  assert.deepEqual({ ...fresh.intent.target }, { ...intent.target }); assert.deepEqual({ ...old.intent.target }, oldTarget, 'the captured old launcher is not mutated');
  assert.deepEqual(f.read(), { ...before, widgets: before.widgets.map(item => item.id === widget.id ? { ...item, route: intent.route } : item) });
});

test('explicit refresh of the same address still replaces the immutable old stage', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const old = f.stages[0], before = f.read();
  assert.equal(await f.control.replaceEntry(widget.id, launch.parseAppLaunchRoute(widget.route)), true);
  assert.equal(old.disposed, true); assert.equal(f.stages.length, 2); assert.deepEqual(f.read(), before);
});

for (const reason of ['dirty', 'flush rejected']) test(`entry replacement preserves the old stage and preference when ${reason}`, async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const old = f.stages[0], before = f.read();
  if (reason === 'dirty') old.dirty = true; else old.flush = async () => { throw new Error('Synthetic unsaved change'); };
  assert.equal(await f.control.replaceEntry(widget.id, refreshedEntry()), false);
  assert.equal(old.disposed, false); assert.equal(old.current(), true); assert.equal(f.stages.length, 1); assert.deepEqual(f.read(), before);
  assert.match(f.notices.at(-1), /Сохраните незавершённое действие/u);
});

test('entry replacement rejects another app, administrative panel, ephemeral invitation and malformed path before flushing', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); let flushes = 0; f.stages[0].flush = async () => { flushes++; };
  const good = refreshedEntry(), other = launch.parseAppLaunchRoute('app/app-22222222222222222222222222222222');
  for (const intent of [other, { ...good, presentation: { panel: 'discussion', administrative: true } }, { ...good, takeInvitation: () => 'ephemeral' }, { ...good, target: { ...good.target, path: 'https://untrusted.invalid/' } }])
    assert.equal(await f.control.replaceEntry(widget.id, intent), false);
  assert.equal(flushes, 0); assert.equal(f.stages.length, 1); assert.equal(f.stages[0].disposed, false);
});

test('entry replacement pending across account ABA cannot dispose or retarget the replacement account stage', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const gate = deferred(); f.stages[0].flush = () => gate.promise;
  const pending = f.control.replaceEntry(widget.id, refreshedEntry()); f.control.setAccount('profile-b'); f.control.setAccount(f.account);
  const current = f.stages.at(-1), before = f.read(); gate.resolve(); assert.equal(await pending, false);
  assert.equal(current.disposed, false); assert.equal(current.current(), true); assert.deepEqual(f.read(), before);
});

test('late hide of the old stage cannot hide a freshly replaced entry with the same widget id', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const hideGate = deferred(), replaceGate = deferred(); let flushes = 0;
  f.stages[0].flush = () => ++flushes === 1 ? hideGate.promise : replaceGate.promise;
  f.click('Скрыть виджет: Tasks'); const replacing = f.control.replaceEntry(widget.id, refreshedEntry());
  replaceGate.resolve(); assert.equal(await replacing, true); const fresh = f.stages.at(-1);
  hideGate.resolve(); await tick(); assert.equal(fresh.disposed, false); assert.equal(fresh.current(), true);
  assert.equal(f.read().widgets[0].visible, true); assert.equal(f.read().widgets[0].route, refreshedEntry().route);
});

test('an unprocessed newer storage entry fences a stale recovery button without rewriting it', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const old = f.stages[0], gate = deferred(); old.flush = () => gate.promise;
  const replacing = f.control.replaceEntry(widget.id, refreshedEntry('/older-button'));
  workspace.editWidgetWorkspace(f.storage, f.account, state => { state.widgets[0].route = refreshedEntry('/newer-selection').route; });
  const saved = f.storage.getItem(workspace.widgetStorageKey(f.account)); gate.resolve(); assert.equal(await replacing, false);
  assert.equal(f.storage.getItem(workspace.widgetStorageKey(f.account)), saved); assert.equal(old.disposed, false); assert.equal(f.stages.length, 1);
});

test('entry replacement keeps newer geometry written while its old stage is flushing', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const gate = deferred(); f.stages[0].flush = () => gate.promise;
  const replacing = f.control.replaceEntry(widget.id, refreshedEntry());
  workspace.editWidgetWorkspace(f.storage, f.account, state => { Object.assign(state.widgets[0], { x: .91, y: .73, width: 540, height: 420 }); });
  gate.resolve(); assert.equal(await replacing, true);
  assert.deepEqual(f.read().widgets[0], { ...widget, x: .91, y: .73, width: 540, height: 420, route: refreshedEntry().route });
});

test('a newer replacement completion fences an older pending recovery of the same widget', async t => {
  const f = fixture(); t.after(() => f.control.dispose()); const older = deferred(), newer = deferred(); let calls = 0;
  f.stages[0].flush = () => ++calls === 1 ? older.promise : newer.promise;
  const first = f.control.replaceEntry(widget.id, refreshedEntry('/first')), second = f.control.replaceEntry(widget.id, refreshedEntry('/second'));
  newer.resolve(); assert.equal(await second, true); const fresh = f.stages.at(-1);
  older.resolve(); assert.equal(await first, false); assert.equal(fresh.disposed, false);
  assert.equal(f.stages.length, 2); assert.equal(f.read().widgets[0].route, refreshedEntry('/second').route);
});

// Append to src/world/micro-widgets.test.mjs.
for (const kind of ['move', 'size']) test(`widget ${kind} leaves browser/system shortcuts and IME arrows untouched`, t => {
  const f = fixture(); t.after(() => f.control.dispose()); const before = f.read();
  const handle = f.nodes.find(node => kind === 'move' ? node.className === 'sm-widget-grip' : node.title === 'Потяните, чтобы изменить размер');
  for (const modifiers of [{ key: 'ArrowLeft', altKey: true }, { key: 'Home', ctrlKey: true }, { key: 'ArrowRight', metaKey: true }, { key: 'ArrowUp', isComposing: true }]) {
    const key = new Event('keydown', { cancelable: true }); Object.assign(key, modifiers); handle.dispatchEvent(key);
    assert.equal(key.defaultPrevented, false); assert.deepEqual(f.read(), before);
  }
});
