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

function fixture({ popover = true, navigation = false } = {}) {
  const values = new Map(), storage = { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) };
  const account = 'profile-a', nodes = [], stages = [], notices = [], dialogs = [];
  workspace.editWidgetWorkspace(storage, account, state => { state.widgets.push({ ...widget }); });
  const view = new EventTarget(), doc = new EventTarget(), frames = new Map(); let nextFrame = 0;
  Object.assign(view, { localStorage: storage, innerWidth: 390, innerHeight: 844, location: { hash: '' },
    requestAnimationFrame(callback) { const id = ++nextFrame; frames.set(id, callback); return id; },
    cancelAnimationFrame(id) { frames.delete(id); } });
  Object.assign(doc, { defaultView: view, activeElement: null });
  let navigationRect = { top: 0, width: 0, height: 0 };
  const navigationNode = navigation ? { getBoundingClientRect: () => navigationRect } : null, observers = [];
  doc.querySelector = selector => selector === '.sw-mobile-nav' ? navigationNode : null;
  class ResizeObserver {
    constructor(callback) { this.callback = callback; this.targets = new Set(); observers.push(this); }
    observe(target) { this.targets.add(target); }
    disconnect() { this.targets.clear(); }
  }
  class Port extends EventTarget {
    constructor(tag, className = '', text = '') {
      super(); Object.assign(this, { tagName: tag.toUpperCase(), className, textContent: text, children: [], style: {}, dataset: {}, ownerDocument: doc, hidden: false, open: true });
      this.classList = { add() {} }; nodes.push(this);
    }
    append(...children) { for (const child of children) { this.children.push(child); child.parent = this; } }
    replaceChildren(...children) { for (const child of this.children) child.parent = null; this.children = []; this.append(...children); }
    setAttribute() {}
    focus() { doc.activeElement = this; }
    contains(node) { return this === node || this.children.some(child => child.contains(node)); }
    remove() { if (this.parent) this.parent.children = this.parent.children.filter(child => child !== this); this.parent = null; }
    matches() { return !!this.popoverOpen; }
    showPopover() { this.popoverOpen = true; }
    hidePopover() { this.popoverOpen = false; }
    getBoundingClientRect() { return { x: parseFloat(this.style.left) || 10, y: parseFloat(this.style.top) || 60,
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
  vm.runInNewContext(source, { exports, AbortController, Event, ResizeObserver, crypto: globalThis.crypto, queueMicrotask,
    require: name => name === './dom' ? { el, button, iconButton: (label, icon, callback) => button(label, icon, '', callback) }
      : name === './iframe-pointer-guard' ? pointerGuard : name === './workspace-widgets.mjs' ? workspace : name === './app-launch.mjs' ? launch : name === './floating-layout' ? floating : {} });
  doc.head = el('head'); doc.createElement = tag => el(tag);
  const root = el('main'); let layerChanges = 0;
  const control = exports.mountMicroWidgets(root, {
    mount(host, _app, _intent, current, instanceId) {
      const stage = { host, current, instanceId, disposed: false, dirty: false, flush: async () => {},
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
  return { control, stages, notices, root, nodes, view, doc, el, click, remote, storage, account, frames, dialogs,
    frame() { const batch = [...frames.values()]; frames.clear(); for (const callback of batch) callback(); },
    observers, setNavigation(rect) { navigationRect = rect; },
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
