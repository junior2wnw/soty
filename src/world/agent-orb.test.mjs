import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import ts from 'typescript';
import { mountAppFullscreen } from './app-fullscreen.mjs';
import * as feedbackMedia from './feedback-media.mjs';

// Production modules, synthetic DOM/native-close/pointer ports. No browser,
// iframe runtime, SDK, model, Dev, server or network is started by this suite.
const read = file => readFileSync(new URL(file, import.meta.url), 'utf8');
const compile = source => ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const pointerGuardCode = compile(read('./iframe-pointer-guard.ts'));
const orbCode = compile(read('./agent-orb.ts')), positionCode = compile(read('./agent-orb-position.ts')), dialogsCode = compile(read('./dialogs.ts'));
const appCode = compile(`${read('./app.ts')}\nexports.TestController = WorldApplication;`);
const feedbackCode = compile(read('./app-feedback.ts'));
const stageAst = ts.createSourceFile('app-stage.ts', read('./app-stage.ts'), ts.ScriptTarget.Latest, true);
let fullscreenCallback, feedbackModalCallback;
function visit(node) {
  if (ts.isCallExpression(node) && node.expression.getText(stageAst) === 'mountAppFullscreen') {
    fullscreenCallback = node.arguments[0].properties.find(property => property.name?.getText(stageAst) === 'onChange')?.initializer.getText(stageAst);
  }
  if (ts.isCallExpression(node) && node.expression.getText(stageAst) === 'mountAppFeedback') {
    feedbackModalCallback = node.arguments[1].properties.find(property => property.name?.getText(stageAst) === 'onModalChange')?.initializer.getText(stageAst);
  }
  ts.forEachChild(node, visit);
}
visit(stageAst); assert.ok(fullscreenCallback);
const worldAst = ts.createSourceFile('app.ts', read('./app.ts'), ts.ScriptTarget.Latest, true); let worldModalCallback;
function visitWorld(node) {
  if (ts.isCallExpression(node) && node.expression.getText(worldAst) === 'mountAppStage') {
    worldModalCallback = node.arguments[1].properties.find(property => property.name?.getText(worldAst) === 'onModalChange')?.initializer.getText(worldAst);
  }
  ts.forEachChild(node, visitWorld);
}
visitWorld(worldAst); assert.ok(feedbackModalCallback); assert.ok(worldModalCallback);
function bindModalCallback(source, receiver, current, options = {}) {
  const module = { exports: {} };
  vm.runInNewContext(compile(`exports.bind = function(ports) { const { current, options } = ports; return ${source}; };`), { module, exports: module.exports });
  return module.exports.bind.call(receiver, { current, options });
}

function fixture({ width = 390, height = 844, delayed = false } = {}) {
  const tasks = [], records = [], mountWaiters = [], saved = new Map(), controllers = [], mutationObservers = [], location = { hash: '#mine' }; let document, nextId = 0, focusedExisting = false;
  class MutationObserver {
    constructor(callback) { this.callback = callback; mutationObservers.push(this); }
    observe() { this.connected = true; }
    disconnect() { this.connected = false; }
  }
  class TrackedAbortController extends AbortController { constructor() { super(); controllers.push(this); } }
  class Element extends EventTarget {
    children = []; dataset = {}; attributes = new Map(); style = {}; parentElement = null;
    hidden = false; inert = false; open = false; disabled = false; captures = new Set();
    constructor(tag = 'div', className = '', text = '') {
      super(); this.tagName = tag.toUpperCase(); this.className = className; this.textContent = text; this.ownerDocument = document;
      this.classList = { add: (...names) => { this.className = [...new Set([...this.className.split(' ').filter(Boolean), ...names])].join(' '); },
        remove: (...names) => { this.className = this.className.split(' ').filter(name => !names.includes(name)).join(' '); },
        contains: name => this.className.split(' ').includes(name), toggle: (name, enabled) => { if (enabled ?? !this.classList.contains(name)) this.classList.add(name); else this.classList.remove(name); } };
    }
    get isConnected() { return this === document.body || !!this.parentElement?.isConnected; }
    get tabIndex() { return this.attributes.has('tabindex') ? Number(this.attributes.get('tabindex')) : /^(BUTTON|INPUT|TEXTAREA|SUMMARY)$/.test(this.tagName) ? 0 : -1; }
    set tabIndex(value) { this.attributes.set('tabindex', String(value)); }
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    getAttribute(name) { return this.attributes.get(name) ?? null; }
    hasAttribute(name) { return name === 'open' ? this.open : name === 'hidden' ? this.hidden : name === 'inert' ? this.inert : this.attributes.has(name); }
    removeAttribute(name) { this.attributes.delete(name); }
    append(...nodes) {
      for (const node of nodes) {
        if (node.parentElement) node.parentElement.children = node.parentElement.children.filter(child => child !== node);
        node.parentElement = this; this.children.push(node);
      }
    }
    replaceChildren(...nodes) { for (const child of [...this.children]) child.remove(); this.append(...nodes); }
    contains(node) { return node === this || this.children.some(child => child.contains(node)); }
    remove() {
      if (this.contains(document.activeElement)) document.activeElement = document.body;
      if (this.parentElement) this.parentElement.children = this.parentElement.children.filter(child => child !== this);
      this.parentElement = null;
    }
    matches(selector) {
      if (selector.includes(',')) return selector.split(',').some(part => this.matches(part.trim()));
      if (selector === ':disabled') return this.disabled;
      if (selector === 'dialog[open]') return this.tagName === 'DIALOG' && this.open;
      if (selector === '[hidden]') return this.hidden;
      if (selector === '[inert]') return this.inert;
      if (selector.startsWith('.')) return this.classList.contains(selector.slice(1));
      const attribute = /^\[([^=\]]+)(?:="([^"]*)")?\]$/.exec(selector);
      if (attribute) { const value = this.getAttribute(attribute[1]); return value !== null && (attribute[2] === undefined || value === attribute[2]); }
      return this.tagName.toLowerCase() === selector;
    }
    closest(selector) { return this.matches(selector) ? this : this.parentElement?.closest(selector) ?? null; }
    querySelectorAll(selector) { return this.children.flatMap(child => [...(child.matches(selector) ? [child] : []), ...child.querySelectorAll(selector)]); }
    querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null; }
    getClientRects() { return this.isConnected && !this.closest('[hidden], [inert]') ? [this.getBoundingClientRect()] : []; }
    getBoundingClientRect() { const x = parseFloat(this.style.left) || 0, y = parseFloat(this.style.top) || 0; return { x, y, left: x, top: y, width: 44, height: 44, right: x + 44, bottom: y + 44 }; }
    focus() { if (this.getClientRects().length && !this.disabled) document.activeElement = this; }
    showModal() { this.previous = document.activeElement; this.open = true; this.querySelector('button')?.focus(); }
    close() { if (!this.open) return; this.open = false; if (this.contains(document.activeElement)) document.activeElement = document.body; this.previous?.focus(); tasks.push(() => this.dispatchEvent(new Event('close'))); }
    requestClose() { const event = new Event('cancel', { cancelable: true }); if (this.dispatchEvent(event)) this.close(); }
    setPointerCapture(id) { this.captures.add(id); }
    hasPointerCapture(id) { return this.captures.has(id); }
    releasePointerCapture(id) { this.captures.delete(id); this.dispatchEvent(event('lostpointercapture', { pointerId: id })); }
  }
  document = Object.assign(new EventTarget(), { fullscreenEnabled: true, fullscreenElement: null });
  document.body = new Element('body'); document.head = new Element('head'); document.createElement = tag => new Element(tag); document.activeElement = document.body;
  document.querySelectorAll = selector => document.body.querySelectorAll(selector); document.querySelector = selector => document.body.querySelector(selector);
  const view = Object.assign(new EventTarget(), { innerWidth: width, innerHeight: height,
    visualViewport: Object.assign(new EventTarget(), { width, height, offsetLeft: 0, offsetTop: 0 }),
    localStorage: { getItem: key => saved.get(key) ?? null, setItem: (key, value) => saved.set(key, value), removeItem: key => saved.delete(key) } });
  document.defaultView = view;
  const el = (...args) => new Element(...args);
  const button = (label, _symbol, className, action) => { const node = el('button', className ?? '', label); node.append(el('span', '', label)); if (action) node.addEventListener('click', action); return node; };
  const dom = { el, button, iconButton: (label, symbol, action) => button(label, symbol, 'sw-icon-button', action), emptyState: (title, detail) => el('div', 'sw-empty', `${title}: ${detail}`),
    labeledField: (label, control) => { const wrapper = el('label', '', label); wrapper.append(control); return wrapper; } };
  const ports = { './dom': dom, './agent-orb.css': {}, './app-feedback.css': {}, './feedback-media.mjs': feedbackMedia };
  function evaluate(code, controllerOnly = false) {
    const module = { exports: {} };
    vm.runInNewContext(code, { module, exports: module.exports, require: name => {
      if (controllerOnly && !(name in ports)) return {};
      assert.ok(name in ports, `Unexpected Source import ${name}`); return ports[name];
    }, document, location, MutationObserver, HTMLElement: Element, getComputedStyle: () => ({ visibility: 'visible' }), AbortController: TrackedAbortController, Event, DOMException,
    localStorage: view.localStorage, navigator: {},
    URLSearchParams, setTimeout, clearTimeout, clearInterval, crypto: { randomUUID: () => `synthetic-${++nextId}` } });
    return module.exports;
  }
  const positions = evaluate(positionCode); ports['./agent-orb-position'] = positions;
  ports['./floating-layout'] = evaluate(compile(read('./floating-layout.ts')));
  const dialogs = evaluate(dialogsCode); ports['./dialogs'] = dialogs;
  ports['./iframe-pointer-guard'] = evaluate(pointerGuardCode);
  const { mountAgentOrb } = evaluate(orbCode);
  const root = el('section', 'sw-app'), main = el('main'); root.append(main); document.body.append(root);
  const agent = mountAgentOrb(root, {
    context: () => ({ appId: 'synthetic-app', name: 'Synthetic application', path: '/', status: 'online' }), focusExisting: () => focusedExisting,
    mount(host, context) {
      const record = { host, context, disposeCount: 0, flushCount: 0, busy: false, prompt: el('textarea'), handle: null };
      record.prompt.value = 'Synthetic personal prompt';
      record.handle = { dispose() { record.disposeCount++; host.replaceChildren(); }, flush: async () => { record.flushCount++; }, hasUnsavedChanges: () => record.busy };
      records.push(record); for (const waiter of mountWaiters) if (records.length >= waiter.count) waiter.resolve();
      if (!delayed) { host.replaceChildren(record.prompt); return record.handle; }
      return new Promise(resolve => { record.resolve = () => { host.replaceChildren(record.prompt); resolve(record.handle); }; });
    },
  });
  const orb = root.querySelector('.sw-agent-orb'), parking = root.querySelector('.sw-agent-parking');
  const click = (detail = 1) => { orb.focus(); return orb.dispatchEvent(event('click', { detail })); };
  return { agent, orb, parking, root, main, document, view, positions, saved, records, mutationObservers, el, click,
    iframePointersBlocked: () => document.head.children.some(node => node.tagName === 'STYLE' && /iframe\s*\{\s*pointer-events:\s*none\s*!important\s*;?\s*\}/.test(node.textContent)),
    dialog: () => document.querySelectorAll('.sw-agent-dialog').find(node => node.open) ?? null,
    close() { document.querySelectorAll('.sw-agent-dialog').find(node => node.open)?.querySelector('button')?.dispatchEvent(event('click', { detail: 1 })); },
    drain() { while (tasks.length) tasks.shift()(); },
    waitMount: (count = 1) => records.length >= count ? Promise.resolve() : new Promise(resolve => mountWaiters.push({ count, resolve })),
    existing: value => { focusedExisting = value; },
    world() {
      // Execute the actual modal, screen cleanup and account boundary methods.
      // Unused controller imports/constructor and metadata storage are ports;
      // no World API, screen runtime or actor is initialized.
      ports['./product'] = { loadDeskPreferences: () => ({ favorites: [], recent: [] }) };
      ports['./hex-field'] = { createHexFieldState: () => ({}) };
      const { TestController } = evaluate(appCode, true), app = Object.create(TestController.prototype);
      Object.assign(app, { root, main, agentOrb: agent, deskAccount: 'A', accountGeneration: 1, homeRequest: 0,
        screenSequence: 1, activeRoute: '#mine', destroyed: false, dialogs: new Set(), live: el('div'),
        avatars: { observe() {}, setContext() {} }, homeState: { slots: new Map() }, discoveryApps: new Map(), renderNavigation() {} });
      agent.setAccount('A'); return app;
    },
    feedback(parent = main, { isCurrent = () => true, appId = 'synthetic-feedback-app', accountId = 'A' } = {}) {
      const { mountAppFeedback } = evaluate(feedbackCode), start = controllers.length, reads = [], changes = [], feedbackHost = el('div'); parent.append(feedbackHost);
      // The real producer and both exact production callbacks are executed.
      // Only context metadata is returned by an in-memory read-only API port.
      const worldChange = bindModalCallback(worldModalCallback, { agentOrb: agent }, isCurrent);
      const stageChange = bindModalCallback(feedbackModalCallback, null, isCurrent, { onModalChange: worldChange });
      const handle = mountAppFeedback(feedbackHost, { accountId, appId, title: 'Synthetic feedback', isCurrent,
        onModalChange(modal, open) { changes.push({ modal, open, connected: modal.isConnected }); stageChange(modal, open); },
        api: { async request(method) { assert.equal(method, 'apps.feedback.context'); reads.push(method); return { context: {
          appId, installationId: 'synthetic-installation', title: 'Synthetic feedback', recipientLabel: 'Synthetic support', ticketVisibility: 'reporter-and-support', canSubmit: true, canManage: false,
          capabilities: { text: true, voice: false, screenshot: false, asr: false },
        } }; } },
      });
      const modal = feedbackHost.querySelector('dialog');
      return { handle, modal, host: feedbackHost, changes, reads, sessions: () => controllers.slice(start),
        close: () => modal.querySelector('button').dispatchEvent(event('click', { detail: 1 })) };
    },
    fullscreen({ supported = true, reject = false } = {}) {
      const stage = el('section', 'sa-stage'), frame = el('iframe'), prior = el('aside'), navigation = el('nav'); prior.inert = true; main.append(stage); stage.append(frame); root.append(prior, navigation);
      document.fullscreenEnabled = supported;
      stage.requestFullscreen = async () => { if (reject) throw new Error('Synthetic rejection'); document.fullscreenElement = stage; document.dispatchEvent(new Event('fullscreenchange')); };
      document.exitFullscreen = async () => { document.fullscreenElement = null; document.dispatchEvent(new Event('fullscreenchange')); };
      const expand = el('button'), icon = () => el('svg');
      // Evaluate only the exact app-stage callback, with its UI ports injected.
      const callbackModule = { exports: {} };
      vm.runInNewContext(compile(`exports.bind = (ports) => { const { screen, expand, icon, el, options } = ports; return ${fullscreenCallback}; };`), { exports: callbackModule.exports, module: callbackModule });
      const callback = callbackModule.exports.bind({ screen: stage, expand, icon, el, options: { onFullscreenChange: (screen, active) => agent.setStage(screen, active) } });
      const controller = mountAppFullscreen({ screen: stage, isCurrent: () => stage.isConnected, onChange: callback });
      return { stage, frame, prior, navigation, controller };
    },
  };
}
function event(type, properties = {}) { const value = new Event(type, { cancelable: true, bubbles: true }); for (const [name, property] of Object.entries(properties)) Object.defineProperty(value, name, { value: property }); return value; }
const pointer = (node, type, properties = {}) => node.dispatchEvent(event(type, { button: 0, isPrimary: true, pointerType: 'touch', pointerId: 1, clientX: 300, clientY: 20, ...properties }));
function bubbleKey(f, target, key, properties = {}) {
  const value = event('keydown', { key, target, ...properties }); let stopped = false;
  const stop = value.stopPropagation.bind(value); value.stopPropagation = () => { stopped = true; stop(); };
  for (let node = target; node; node = node.parentElement) { node.dispatchEvent(value); if (stopped) break; }
  if (!stopped) f.document.dispatchEvent(value);
  return { value, stopped };
}

test('Close during a running request retains the same connected assistant and reopens without another mount', async () => {
  const f = fixture(); f.agent.setAccount('A'); f.click(); await f.agent.flush(); const session = f.records[0]; session.busy = true;
  f.close(); f.drain(); assert.equal(session.disposeCount, 0); assert.equal(session.host.parentElement, f.parking); assert.equal(session.host.isConnected, true); assert.equal(f.agent.hasUnsavedChanges(), true);
  session.prompt.value = 'Confirmed synthetic answer'; session.busy = false; f.click(); await f.agent.flush();
  assert.equal(f.records.length, 1); assert.equal(f.dialog().contains(session.host), true); assert.equal(session.prompt.value, 'Confirmed synthetic answer'); f.agent.dispose();
});
test('Route, overlay and route facades use one mount; disposing a stale facade cannot park a newer view', async () => {
  const f = fixture(); f.agent.setAccount('A'); const firstPage = f.el('section'); f.main.append(firstPage);
  const first = await f.agent.mountIn(firstPage); const session = f.records[0]; session.busy = true;
  assert.equal(first.hasUnsavedChanges(), false); assert.equal(f.agent.hasUnsavedChanges(), true);
  f.existing(true); f.click(); assert.equal(f.dialog(), null); assert.equal(f.records.length, 1);
  first.dispose(); f.existing(false); f.click(); await f.agent.flush(); first.dispose(); assert.equal(f.dialog().contains(session.host), true);
  f.close(); const secondPage = f.el('section'); f.main.append(secondPage); const second = await f.agent.mountIn(secondPage); first.dispose();
  assert.equal(session.host.parentElement, secondPage); assert.equal(f.records.length, 1); second.dispose(); f.agent.dispose();
});
test('Close/reopen while the first mount is pending still produces one retained session', async () => {
  const f = fixture({ delayed: true }); f.agent.setAccount('A'); f.click(); await f.waitMount(); const originalHost = f.records[0].host;
  f.close(); f.drain(); f.click(); assert.equal(f.records.length, 1); f.records[0].resolve(); await f.agent.flush();
  assert.equal(f.dialog().contains(originalHost), true); assert.equal(f.records[0].disposeCount, 0); f.agent.dispose();
});
test('Account ABA late mount and stale facade clean only their old host', async () => {
  const f = fixture({ delayed: true }); f.agent.setAccount('A'); const firstPage = f.el('section'); f.main.append(firstPage);
  const oldFacade = f.agent.mountIn(firstPage); await f.waitMount(); const old = f.records[0];
  f.agent.setAccount('B'); f.agent.setAccount('A'); const newPage = f.el('section'); f.main.append(newPage); const newFacade = f.agent.mountIn(newPage); await f.waitMount(2); const current = f.records[1];
  assert.notEqual(old.host, current.host); assert.equal(old.context.isCurrent(), false); assert.equal(old.context.context(), null);
  current.resolve(); await newFacade; old.resolve(); const stale = await oldFacade; stale.dispose(); await stale.flush();
  assert.equal(old.disposeCount, 1); assert.equal(current.disposeCount, 0); assert.equal(current.flushCount, 0); assert.equal(current.host.parentElement, newPage); assert.equal(current.host.contains(current.prompt), true); f.agent.dispose();
});
test('Session teardown disposes a late mount once and cannot recreate the control', async () => {
  const f = fixture({ delayed: true }); f.agent.setAccount('A'); f.click(); await f.waitMount(); f.agent.dispose(); f.records[0].resolve(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
  assert.equal(f.records[0].disposeCount, 1); assert.equal(f.orb.isConnected, false); assert.equal(f.parking.isConnected, false);
});
for (const options of [{ supported: true }, { supported: false }, { reject: true }]) test(`Fullscreen ${JSON.stringify(options)} keeps iframe and brings the orb/dialog into its subtree`, async () => {
  const f = fixture(); f.agent.setAccount('A'); const full = f.fullscreen(options); await full.controller.toggle();
  assert.equal(f.orb.parentElement, full.stage); assert.equal(f.orb.inert, false); assert.equal(full.frame.parentElement, full.stage); assert.equal(full.frame.isConnected, true);
  if (!options.supported || options.reject) { assert.equal(full.prior.inert, true); assert.equal(full.navigation.inert, true); assert.equal(f.parking.inert, true); }
  f.click(); await f.agent.flush(); assert.equal(f.dialog().parentElement, full.stage); f.close(); await full.controller.leave();
  assert.equal(f.orb.parentElement, f.root); assert.equal(f.orb.inert, false); assert.equal(full.prior.inert, true); assert.equal(full.navigation.inert, false); assert.equal(f.parking.inert, false); assert.equal(full.frame.parentElement, full.stage); assert.equal(full.frame.isConnected, true); full.controller.dispose(); f.agent.dispose();
});
for (const supported of [true, false]) test(`Escape from the assistant dialog closes only that dialog, fullscreen supported=${supported}`, async () => {
  const f = fixture(); f.agent.setAccount('A'); const full = f.fullscreen({ supported }); await full.controller.toggle(); f.click(); await f.agent.flush();
  const session = f.records[0], modal = f.dialog(); session.prompt.focus();
  const escape = event('keydown', { key: 'Escape' }); Object.defineProperty(escape, 'target', { value: session.prompt });
  let stopped = false; const stop = escape.stopPropagation.bind(escape); escape.stopPropagation = () => { stopped = true; stop(); };
  for (let node = session.prompt; node; node = node.parentElement) { node.dispatchEvent(escape); if (stopped) break; }
  if (!stopped) f.document.dispatchEvent(escape);
  if (!escape.defaultPrevented && modal.open) modal.requestClose(); f.drain(); await Promise.resolve(); await Promise.resolve();
  assert.equal(f.dialog(), null); assert.equal(full.controller.active(), true); assert.equal(f.orb.parentElement, full.stage); assert.equal(session.disposeCount, 0); assert.equal(full.frame.isConnected, true);
  await full.controller.leave(); full.controller.dispose(); f.agent.dispose();
});
test('Primary touch drag suppresses its click; the next tap opens normally', async () => {
  const f = fixture(); f.agent.setAccount('A'); pointer(f.orb, 'pointerdown'); pointer(f.orb, 'pointermove', { clientX: 200, clientY: 100 }); pointer(f.orb, 'pointerup', { clientX: 200, clientY: 100 });
  assert.equal(f.orb.hasPointerCapture(1), false); f.click(1); assert.equal(f.dialog(), null);
  pointer(f.orb, 'pointerdown', { pointerId: 2 }); pointer(f.orb, 'pointerup', { pointerId: 2 }); f.click(1); await f.agent.flush(); assert.ok(f.dialog()); f.agent.dispose();
});
for (const cancel of ['pointercancel', 'lostpointercapture', 'Escape']) test(`${cancel} restores docking and releases capture; keyboard activation is never eaten`, async () => {
  const f = fixture(); f.agent.setAccount('A'); const initial = { ...f.orb.style }; pointer(f.orb, 'pointerdown'); pointer(f.orb, 'pointermove', { clientX: 160, clientY: 120 });
  assert.equal(f.iframePointersBlocked(), true);
  if (cancel === 'Escape') f.orb.dispatchEvent(event('keydown', { key: 'Escape' })); else pointer(f.orb, cancel);
  assert.equal(f.iframePointersBlocked(), false);
  assert.equal(f.orb.hasPointerCapture(1), false); assert.equal(f.orb.dataset.docked, 'true'); assert.deepEqual(f.orb.style, initial); assert.equal(f.saved.size, 0);
  f.click(0); await f.agent.flush(); assert.ok(f.dialog()); f.agent.dispose();
});
test('Non-primary touch does not move or save; account switch fences an old primary drag', () => {
  const f = fixture(); f.agent.setAccount('A'); const initial = { ...f.orb.style };
  pointer(f.orb, 'pointerdown', { isPrimary: false, pointerId: 9 }); pointer(f.orb, 'pointermove', { pointerId: 9, clientX: 0, clientY: 200 }); pointer(f.orb, 'pointerup', { pointerId: 9 });
  assert.deepEqual(f.orb.style, initial); assert.equal(f.saved.size, 0);
  pointer(f.orb, 'pointerdown'); pointer(f.orb, 'pointermove', { clientX: 150 }); f.agent.setAccount('B'); pointer(f.orb, 'pointerup');
  assert.equal(f.saved.has('soty.agent-orb.v1:B'), false); assert.equal(f.orb.dataset.docked, 'true'); f.agent.dispose();
});
test('Keyboard positions are account scoped, Home resets only its account, and 320/390 clamp is bounded', () => {
  const f = fixture({ width: 320, height: 390 }); f.agent.setAccount('A'); f.orb.dispatchEvent(event('keydown', { key: 'ArrowLeft' })); const a = f.orb.style.left;
  assert.ok(f.saved.has('soty.agent-orb.v1:A')); f.agent.setAccount('B'); assert.notEqual(f.orb.style.left, a); f.agent.setAccount('A'); assert.equal(f.orb.style.left, a);
  f.orb.dispatchEvent(event('keydown', { key: 'Home' })); assert.equal(f.saved.has('soty.agent-orb.v1:A'), false);
  for (const width of [320, 390]) {
    const viewport = { width, height: 390, left: 0, top: 0 }, point = f.positions.clampOrb({ x: 9999, y: 9999 }, viewport);
    assert.equal(point.x + 44 <= width - 6, true); assert.equal(point.y + 44 <= viewport.height - 88, true);
  }
  Object.assign(f.view.visualViewport, { width: 320, height: 260, offsetLeft: 12, offsetTop: 150 }); f.view.visualViewport.dispatchEvent(new Event('resize'));
  const rect = f.orb.getBoundingClientRect(); assert.equal(rect.left >= 18, true); assert.equal(rect.top >= 156, true); assert.equal(rect.bottom <= 322, true); f.agent.dispose();
});

test('saved orb preference is projected away from the real Create CTA and returns after the form closes without a storage write', () => {
  const f = fixture();
  const create = f.el('button', '', 'Создать');
  const action = { x: 224, y: 673, left: 224, top: 673, width: 124, height: 44, right: 348, bottom: 717 };
  create.getBoundingClientRect = () => action; f.main.append(create);
  const saved = JSON.stringify({ x: 304 / 390, y: 680 / 844 }); f.saved.set('soty.agent-orb.v1:A', saved);
  f.agent.setAccount('A');
  const overlaps = (a, b) => a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top;
  assert.equal(overlaps(f.orb.getBoundingClientRect(), action), false, 'the full CTA, including its right edge, stays available');
  assert.equal(f.orb.dataset.projected, 'true'); assert.equal(f.saved.get('soty.agent-orb.v1:A'), saved);
  create.remove(); f.root.dispatchEvent(new Event('soty:floating-layout'));
  assert.equal(f.orb.dataset.projected, 'false'); assert.equal(f.orb.style.left, '304px'); assert.equal(f.orb.style.top, '680px');
  assert.equal(f.saved.get('soty.agent-orb.v1:A'), saved); f.agent.dispose();
});

test('a new route action or a scrolled composer changes projection without persisting it, and the observer is disconnected on disposal', () => {
  const f = fixture(), action = f.el('button', '', 'Отправить');
  const saved = JSON.stringify({ x: 304 / 390, y: 680 / 844 }); f.saved.set('soty.agent-orb.v1:A', saved); f.agent.setAccount('A');
  action.getBoundingClientRect = () => ({ left: 224, top: 673, width: 124, height: 44, right: 348, bottom: 717 }); f.main.append(action);
  f.mutationObservers[0].callback([{ target: f.main }]); assert.equal(f.orb.dataset.projected, 'true');
  action.remove(); f.document.dispatchEvent(new Event('scroll')); assert.equal(f.orb.dataset.projected, 'false');
  assert.equal(f.saved.get('soty.agent-orb.v1:A'), saved);
  f.agent.dispose(); assert.equal(f.mutationObservers[0].connected, false);
});

test('tap movement and reset use the existing helper dialog, and an old account dialog cannot move a new account', async () => {
  const f = fixture(); f.agent.setAccount('A'); f.click(); await f.agent.flush();
  const actions = f.dialog().querySelectorAll('.sw-agent-position-actions')[0].querySelectorAll('button');
  const down = actions.find(control => control.textContent === 'Вниз'), reset = actions.find(control => control.textContent === 'Вернуть в угол');
  assert.ok(down); down.dispatchEvent(new Event('click')); assert.ok(f.saved.has('soty.agent-orb.v1:A'));
  assert.equal(f.records.length, 1); reset.dispatchEvent(new Event('click')); assert.equal(f.saved.has('soty.agent-orb.v1:A'), false);
  f.agent.setAccount('B'); down.dispatchEvent(new Event('click'));
  assert.equal(f.saved.has('soty.agent-orb.v1:B'), false); f.agent.dispose();
});

test('Actual World modal stack owns one orb and retains one running helper through close/reopen', async () => {
  const f = fixture(), app = f.world(), settings = app.dialog('Synthetic settings');
  assert.equal(f.orb.parentElement, settings.element); assert.equal(settings.element.classList.contains('sw-agent-modal-host'), true);
  f.click(); await f.agent.flush(); const session = f.records[0]; session.busy = true;
  assert.equal(f.dialog().parentElement, settings.element); assert.equal(f.dialog().contains(session.host), true);
  const profile = app.dialog('Synthetic profile');
  assert.equal(f.dialog(), null); assert.equal(f.orb.parentElement, profile.element); assert.equal(settings.element.classList.contains('sw-agent-modal-host'), false);
  assert.equal(session.host.parentElement, f.parking); assert.equal(session.disposeCount, 0); assert.equal(f.agent.hasUnsavedChanges(), true);
  profile.close(); assert.equal(f.orb.parentElement, settings.element); f.click(); await f.agent.flush();
  assert.equal(f.records.length, 1); assert.equal(f.dialog().parentElement, settings.element); assert.equal(f.dialog().contains(session.host), true);
  settings.close(); f.drain(); assert.equal(f.orb.parentElement, f.root); assert.equal(f.dialog(), null); assert.equal(app.dialogs.size, 0);
  const reopened = app.dialog('Synthetic settings reopened'); f.click(); await f.agent.flush();
  assert.equal(f.records.length, 1); assert.equal(f.dialog().parentElement, reopened.element); assert.equal(session.disposeCount, 0);
  assert.equal(f.document.querySelectorAll('.sw-agent-orb').length, 1); reopened.close(); f.agent.dispose();
});

test('Queued native close from an older World modal cannot close or park a newer helper view', async () => {
  const f = fixture(), app = f.world(), first = app.dialog('Older settings'); first.element.close();
  const second = app.dialog('Newer settings'); f.click(); await f.agent.flush(); const helper = f.dialog(), session = f.records[0];
  f.drain(); first.close(); assert.equal(f.orb.parentElement, second.element); assert.equal(f.dialog(), helper);
  assert.equal(helper.open, true); assert.equal(helper.contains(session.host), true); assert.equal(session.disposeCount, 0);
  assert.equal(app.dialogs.size, 1); second.close(); f.drain(); f.agent.dispose();
});

test('Actual cleanScreen keeps an open owned modal and the retained session while dismissing its helper view', async () => {
  const f = fixture(), app = f.world(), settings = app.dialog('Synthetic settings'); f.click(); await f.agent.flush(); const session = f.records[0]; session.busy = true;
  app.cleanScreen(); assert.equal(settings.element.open, true); assert.equal(settings.element.isConnected, true); assert.equal(f.orb.parentElement, settings.element);
  assert.equal(app.dialogs.has(settings), true); assert.equal(f.dialog(), null); assert.equal(session.host.parentElement, f.parking);
  assert.equal(session.host.isConnected, true); assert.equal(session.disposeCount, 0); assert.equal(f.agent.hasUnsavedChanges(), true);
  f.main.replaceChildren(f.el('h1', '', 'Another screen')); f.click(); await f.agent.flush();
  assert.equal(f.records.length, 1); assert.equal(f.dialog().parentElement, settings.element); assert.equal(f.dialog().contains(session.host), true);
  settings.close(); f.drain(); f.agent.dispose();
});

test('Same-modal refresh leaves a pending helper and primary pointer capture intact', async () => {
  const f = fixture({ delayed: true }), app = f.world(), settings = app.dialog('Synthetic settings'); f.click(); await f.waitMount();
  const helper = f.dialog(), session = f.records[0]; f.agent.setModal(settings.element);
  assert.equal(f.dialog(), helper); assert.equal(helper.contains(session.host), true); assert.equal(f.records.length, 1);
  f.close(); pointer(f.orb, 'pointerdown'); pointer(f.orb, 'pointermove', { clientX: 160 }); f.agent.setModal(settings.element);
  assert.equal(f.orb.hasPointerCapture(1), true); pointer(f.orb, 'pointercancel'); session.resolve(); await f.agent.flush();
  assert.equal(session.disposeCount, 0); settings.close(); f.drain(); f.agent.dispose();
});

test('An owned modal bypasses route focusExisting while reusing the route assistant session', async () => {
  const f = fixture(), app = f.world(), route = f.el('section'); f.main.append(route); const facade = await f.agent.mountIn(route), session = f.records[0];
  f.existing(true); f.click(); assert.equal(f.dialog(), null);
  const settings = app.dialog('Synthetic settings'); f.click(); await f.agent.flush();
  assert.equal(f.dialog().parentElement, settings.element); assert.equal(f.dialog().contains(session.host), true); assert.equal(f.records.length, 1);
  facade.dispose(); assert.equal(f.dialog().contains(session.host), true);
  settings.close(); f.click(); assert.equal(f.dialog(), null); f.drain(); f.agent.dispose();
});

test('Actual account ABA closes old owned modals and disposes a late helper handle only on its old host', async () => {
  const f = fixture({ delayed: true }), app = f.world(), oldModal = app.dialog('Old account settings'); f.click(); await f.waitMount(); const old = f.records[0];
  assert.equal(app.transitionAccount('B'), true); assert.equal(oldModal.element.open, false); assert.equal(oldModal.element.isConnected, false);
  assert.equal(f.orb.parentElement, f.root); assert.equal(app.dialogs.size, 0); assert.equal(old.context.isCurrent(), false); assert.equal(old.context.context(), null);
  assert.equal(app.transitionAccount('A'), true); const currentModal = app.dialog('Current account settings'); f.click(); await f.waitMount(2); const current = f.records[1], helper = f.dialog();
  current.resolve(); await f.agent.flush(); old.resolve(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); f.drain(); oldModal.close();
  assert.equal(old.disposeCount, 1); assert.equal(old.host.isConnected, false); assert.notEqual(old.host, current.host);
  assert.equal(current.disposeCount, 0); assert.equal(f.orb.parentElement, currentModal.element); assert.equal(f.dialog(), helper); assert.equal(helper.contains(current.host), true);
  assert.equal(f.document.querySelectorAll('.sw-agent-orb').length, 1); currentModal.close(); f.agent.dispose();
});

test('Stale account-generation modal close does not call setModal or move a current view', async () => {
  const f = fixture(), app = f.world(), oldModal = app.dialog('Old settings');
  // The old callback remains possible outside transitionAccount's synchronous
  // cleanup. Test its own generation fence independently of the cleanup path.
  app.accountGeneration += 2; f.agent.setAccount('B'); f.agent.setAccount('A'); const currentModal = app.dialog('Current settings'); f.click(); await f.agent.flush();
  const helper = f.dialog(), session = f.records[0], calls = []; const setModal = f.agent.setModal.bind(f.agent);
  f.agent.setModal = value => { calls.push(value); setModal(value); }; oldModal.element.close(); f.drain();
  assert.equal(calls.length, 0); assert.equal(f.orb.parentElement, currentModal.element); assert.equal(f.dialog(), helper); assert.equal(helper.contains(session.host), true);
  currentModal.close(); f.agent.dispose();
});

for (const options of [{ supported: true }, { supported: false }, { reject: true }]) test(`Owned modal close restores fullscreen stage without moving its iframe: ${JSON.stringify(options)}`, async () => {
  const f = fixture(), app = f.world(), full = f.fullscreen(options); await full.controller.toggle();
  const settings = app.dialog('Synthetic settings'); assert.equal(f.orb.parentElement, settings.element); assert.equal(full.frame.parentElement, full.stage);
  f.click(); await f.agent.flush(); const session = f.records[0]; session.busy = true;
  assert.equal(f.dialog().parentElement, settings.element); settings.close(); f.drain();
  assert.equal(f.dialog(), null); assert.equal(f.orb.parentElement, full.stage); assert.equal(f.orb.inert, false); assert.equal(full.controller.active(), true);
  assert.equal(full.frame.parentElement, full.stage); assert.equal(full.frame.isConnected, true); assert.equal(session.disposeCount, 0); assert.equal(f.agent.hasUnsavedChanges(), true);
  await full.controller.leave(); assert.equal(f.orb.parentElement, f.root); assert.equal(full.navigation.inert, false); assert.equal(full.prior.inert, true);
  assert.equal(full.frame.parentElement, full.stage); assert.equal(full.frame.isConnected, true); full.controller.dispose(); f.agent.dispose();
});

for (const boundary of ['modal switch', 'parent close', 'screen reset', 'account switch', 'dispose']) test(`Primary drag ${boundary} releases capture and cannot become an accidental helper click`, async () => {
  const f = fixture(), app = f.world(), settings = app.dialog('Synthetic settings'), initial = { ...f.orb.style }; let releases = 0;
  f.orb.addEventListener('lostpointercapture', () => releases++); pointer(f.orb, 'pointerdown'); pointer(f.orb, 'pointermove', { clientX: 140, clientY: 160 });
  assert.equal(f.iframePointersBlocked(), true);
  if (boundary === 'modal switch') app.dialog('Another settings modal');
  if (boundary === 'parent close') settings.close();
  if (boundary === 'screen reset') app.cleanScreen();
  if (boundary === 'account switch') app.transitionAccount('B');
  if (boundary === 'dispose') f.agent.dispose();
  assert.equal(f.iframePointersBlocked(), false);
  assert.equal(f.orb.hasPointerCapture(1), false); assert.equal(releases, 1); assert.equal(f.saved.size, 0);
  assert.deepEqual(f.orb.style, initial); pointer(f.orb, 'pointerup'); f.click(1); assert.equal(f.dialog(), null); assert.equal(f.records.length, 0);
  if (boundary !== 'dispose') { pointer(f.orb, 'pointerdown', { pointerId: 2 }); pointer(f.orb, 'pointerup', { pointerId: 2 }); f.click(1); await f.agent.flush(); assert.ok(f.dialog()); }
  for (const value of [...app.dialogs]) value.close(); f.drain(); f.agent.dispose();
});

test('Nested helper Escape preserves settings, running request and focus on the same owned orb', async () => {
  const f = fixture(), app = f.world(), settings = app.dialog('Synthetic settings'); f.click(); await f.agent.flush(); const session = f.records[0]; session.busy = true; session.prompt.focus();
  const { value, stopped } = bubbleKey(f, session.prompt, 'Escape'); f.drain();
  assert.equal(value.defaultPrevented, true); assert.equal(stopped, true); assert.equal(f.dialog(), null);
  assert.equal(settings.element.open, true); assert.equal(app.dialogs.has(settings), true); assert.equal(f.orb.parentElement, settings.element); assert.equal(f.document.activeElement, f.orb);
  assert.equal(session.disposeCount, 0); assert.equal(session.host.parentElement, f.parking); assert.equal(f.agent.hasUnsavedChanges(), true);
  f.click(0); await f.agent.flush(); assert.equal(f.records.length, 1); assert.equal(f.dialog().parentElement, settings.element); settings.close(); f.drain(); f.agent.dispose();
});

test('Escape cancelling a modal-owned drag stops before the parent native close path', () => {
  const f = fixture(), app = f.world(), settings = app.dialog('Synthetic settings'); pointer(f.orb, 'pointerdown'); pointer(f.orb, 'pointermove', { clientX: 140 });
  const { value, stopped } = bubbleKey(f, f.orb, 'Escape');
  assert.equal(value.defaultPrevented, true); assert.equal(stopped, true); assert.equal(settings.element.open, true); assert.equal(f.orb.hasPointerCapture(1), false); assert.equal(f.saved.size, 0);
  f.click(1); assert.equal(f.dialog(), null); settings.close(); f.drain(); f.agent.dispose();
});

for (const type of ['pointercancel', 'lostpointercapture']) test(`${type} for another pointer cannot cancel a newer modal-owned primary drag`, () => {
  const f = fixture(), app = f.world(), settings = app.dialog('Synthetic settings'); pointer(f.orb, 'pointerdown'); pointer(f.orb, 'pointermove', { clientX: 140 });
  settings.close(); assert.equal(f.orb.hasPointerCapture(1), false);
  const currentModal = app.dialog('Newer settings'); pointer(f.orb, 'pointerdown', { pointerId: 2 }); pointer(f.orb, 'pointermove', { pointerId: 2, clientX: 200, clientY: 140 });
  const position = { ...f.orb.style }; pointer(f.orb, type, { pointerId: 1, isPrimary: false });
  assert.equal(f.orb.hasPointerCapture(2), true); assert.deepEqual(f.orb.style, position); assert.equal(f.saved.size, 0);
  pointer(f.orb, 'pointerup', { pointerId: 2 }); assert.equal(f.orb.hasPointerCapture(2), false); assert.equal(f.saved.has('soty.agent-orb.v1:A'), true);
  f.click(1); assert.equal(f.dialog(), null); currentModal.close(); f.drain(); f.agent.dispose();
});

test('Closed, detached and foreign-document modal inputs cannot own the orb', () => {
  for (const invalid of ['closed', 'detached', 'foreign']) {
    const f = fixture(), app = f.world(), modal = app.dialog('Synthetic settings');
    if (invalid === 'closed') modal.element.close();
    if (invalid === 'detached') modal.element.remove();
    if (invalid === 'foreign') modal.element.ownerDocument = {};
    f.agent.setModal(modal.element); assert.equal(f.orb.parentElement, f.root, invalid); assert.equal(modal.element.classList.contains('sw-agent-modal-host'), false, invalid);
    modal.close(); f.drain(); f.agent.dispose();
  }
});

for (const options of [{ supported: true }, { supported: false }, { reject: true }]) test(`Actual feedback modal matching close restores fullscreen and retains helper: ${JSON.stringify(options)}`, async () => {
  const f = fixture(), app = f.world(), full = f.fullscreen(options); await full.controller.toggle();
  const feedback = f.feedback(full.stage, { isCurrent: app.accountTask() }); feedback.handle.open();
  assert.equal(f.orb.parentElement, feedback.modal); assert.equal(feedback.modal.open, true); assert.equal(app.dialogs.size, 0);
  f.click(); await f.agent.flush(); const session = f.records[0]; session.busy = true; assert.equal(f.dialog().parentElement, feedback.modal);
  feedback.close(); assert.equal(feedback.modal.open, false); assert.equal(f.dialog(), null); assert.equal(f.orb.parentElement, full.stage);
  assert.equal(full.controller.active(), true); assert.equal(full.frame.parentElement, full.stage); assert.equal(full.frame.isConnected, true);
  assert.equal(session.disposeCount, 0); assert.equal(session.host.parentElement, f.parking); assert.equal(f.agent.hasUnsavedChanges(), true);
  assert.equal(feedback.changes.filter(change => !change.open).length, 1); f.drain(); assert.equal(feedback.changes.filter(change => !change.open).length, 1);
  feedback.handle.open(); f.click(); await f.agent.flush(); assert.equal(f.records.length, 1); assert.equal(f.dialog().contains(session.host), true);
  feedback.handle.dispose(); assert.equal(f.orb.parentElement, full.stage); assert.equal(feedback.modal.isConnected, false);
  assert.equal(feedback.changes.filter(change => !change.open).every(change => change.connected), true);
  await full.controller.leave(); full.controller.dispose(); f.drain(); f.agent.dispose();
});

for (const close of ['managed', 'native']) test(`Actual feedback ${close} close/reopen ignores an old queued close and retains its new helper/session`, async () => {
  const f = fixture(), app = f.world(), feedback = f.feedback(f.main, { isCurrent: app.accountTask() }); feedback.handle.open();
  const firstSession = feedback.sessions().at(-1); if (close === 'managed') feedback.close(); else feedback.modal.close();
  feedback.handle.open(); const currentSession = feedback.sessions().at(-1); f.click(); await f.agent.flush(); const helper = f.dialog(), session = f.records[0];
  assert.equal(currentSession.signal.aborted, false); assert.equal(firstSession === currentSession, close === 'native');
  const released = feedback.changes.filter(change => !change.open).length; f.drain();
  assert.equal(feedback.modal.open, true); assert.equal(f.orb.parentElement, feedback.modal); assert.equal(f.dialog(), helper); assert.equal(helper.contains(session.host), true);
  assert.equal(currentSession.signal.aborted, false); assert.equal(feedback.changes.filter(change => !change.open).length, released); assert.equal(session.disposeCount, 0);
  feedback.handle.dispose(); f.drain(); f.agent.dispose();
});

test('Actual feedback late close or wrong release cannot steal a newer World modal/helper', async () => {
  const f = fixture(), app = f.world(), feedback = f.feedback(f.main, { isCurrent: app.accountTask() }); feedback.handle.open(); feedback.modal.close();
  const settings = app.dialog('Newer World settings'); f.click(); await f.agent.flush(); const helper = f.dialog(), session = f.records[0];
  f.drain(); f.agent.releaseModal(feedback.modal); feedback.handle.dispose();
  assert.equal(f.orb.parentElement, settings.element); assert.equal(f.dialog(), helper); assert.equal(helper.contains(session.host), true);
  assert.equal(session.disposeCount, 0); assert.equal(settings.element.open, true); settings.close(); f.drain(); f.agent.dispose();
});

test('Actual feedback dispose and its queued close cannot release another raw feedback modal', async () => {
  const f = fixture(), app = f.world(), first = f.feedback(f.main, { isCurrent: app.accountTask(), appId: 'synthetic-first' }); first.handle.open(); f.click(); await f.agent.flush(); const session = f.records[0];
  first.modal.close(); const next = f.feedback(f.main, { isCurrent: app.accountTask(), appId: 'synthetic-next' }); next.handle.open(); f.click(); await f.agent.flush(); const helper = f.dialog();
  first.handle.dispose(); f.drain(); f.agent.releaseModal(first.modal);
  assert.equal(first.changes.filter(change => !change.open).length, 1); assert.equal(f.orb.parentElement, next.modal); assert.equal(f.dialog(), helper);
  assert.equal(helper.contains(session.host), true); assert.equal(f.records.length, 1); assert.equal(session.disposeCount, 0); next.handle.dispose(); f.drain(); f.agent.dispose();
});

test('Actual account ABA fences late raw feedback close and old helper mount from the current modal/session', async () => {
  const f = fixture({ delayed: true }), app = f.world(), oldFeedback = f.feedback(f.main, { isCurrent: app.accountTask(), appId: 'synthetic-old' }); oldFeedback.handle.open(); f.click(); await f.waitMount(); const old = f.records[0];
  oldFeedback.modal.close(); app.transitionAccount('B'); app.transitionAccount('A');
  const feedback = f.feedback(f.main, { isCurrent: app.accountTask(), appId: 'synthetic-current' }); feedback.handle.open(); f.click(); await f.waitMount(2); const current = f.records[1], helper = f.dialog();
  current.resolve(); await f.agent.flush(); old.resolve(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
  f.drain(); oldFeedback.handle.open(); oldFeedback.handle.dispose();
  assert.equal(old.disposeCount, 1); assert.equal(old.context.isCurrent(), false); assert.equal(old.host.isConnected, false);
  assert.equal(f.orb.parentElement, feedback.modal); assert.equal(f.dialog(), helper); assert.equal(helper.contains(current.host), true); assert.equal(current.disposeCount, 0);
  assert.equal(oldFeedback.changes.filter(change => change.open).length, 1); feedback.handle.dispose(); f.drain(); f.agent.dispose();
});

test('Orb disposal followed by late raw feedback close and late helper mount cannot recreate UI', async () => {
  const f = fixture({ delayed: true }), app = f.world(), feedback = f.feedback(f.main, { isCurrent: app.accountTask() }); feedback.handle.open(); f.click(); await f.waitMount(); const session = f.records[0];
  feedback.modal.close(); f.agent.dispose(); feedback.handle.dispose(); f.drain(); session.resolve(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
  assert.equal(session.disposeCount, 1); assert.equal(f.orb.isConnected, false); assert.equal(f.parking.isConnected, false); assert.equal(feedback.modal.isConnected, false);
  assert.equal(f.document.querySelectorAll('.sw-agent-orb').length, 0); assert.equal(feedback.changes.filter(change => !change.open).length, 1);
});

test('Actual feedback close veto for an unsaved draft retains modal ownership', () => {
  const f = fixture(), app = f.world(), feedback = f.feedback(f.main, { isCurrent: app.accountTask() }); feedback.handle.open();
  const text = feedback.modal.querySelector('textarea'), storage = feedback.modal.querySelectorAll('input').find(node => node.type === 'checkbox');
  text.value = 'Synthetic unsaved feedback'; text.dispatchEvent(event('input')); storage.checked = false; storage.dispatchEvent(event('change')); feedback.close();
  assert.equal(feedback.handle.hasUnsavedChanges(), true); assert.equal(feedback.modal.open, true); assert.equal(f.orb.parentElement, feedback.modal);
  assert.equal(feedback.changes.filter(change => !change.open).length, 0); feedback.handle.dispose(); f.drain(); f.agent.dispose();
});

test('Escape from a helper nested in actual feedback keeps the raw form, its session and orb focus', async () => {
  const f = fixture(), app = f.world(), feedback = f.feedback(f.main, { isCurrent: app.accountTask() }); feedback.handle.open();
  const text = feedback.modal.querySelector('textarea'); text.value = 'Synthetic feedback draft'; text.dispatchEvent(event('input'));
  f.click(); await f.agent.flush(); const session = f.records[0]; session.busy = true; session.prompt.focus();
  const { value, stopped } = bubbleKey(f, session.prompt, 'Escape'); f.drain();
  assert.equal(value.defaultPrevented, true); assert.equal(stopped, true); assert.equal(f.dialog(), null); assert.equal(feedback.modal.open, true);
  assert.equal(f.orb.parentElement === feedback.modal, true); assert.equal(f.document.activeElement === f.orb, true); assert.equal(text.value, 'Synthetic feedback draft');
  assert.equal(feedback.sessions().at(-1).signal.aborted, false); assert.equal(feedback.changes.filter(change => !change.open).length, 0);
  assert.equal(session.disposeCount, 0); assert.equal(session.host.parentElement, f.parking); assert.equal(f.agent.hasUnsavedChanges(), true); feedback.handle.dispose(); f.drain(); f.agent.dispose();
});

test('Queued native close from a World modal cannot steal a newer custom feedback modal/helper', async () => {
  const f = fixture(), app = f.world(), settings = app.dialog('Older World settings'); settings.element.close();
  const feedback = f.feedback(f.main, { isCurrent: app.accountTask() }); feedback.handle.open(); f.click(); await f.agent.flush(); const helper = f.dialog(), session = f.records[0];
  f.drain(); assert.equal(f.orb.parentElement === feedback.modal, true, 'An older World close must keep the orb inside the current custom modal');
  assert.equal(f.dialog() === helper, true, 'An older World close must retain the current helper view'); assert.equal(helper.contains(session.host), true);
  assert.equal(feedback.modal.open, true); assert.equal(session.disposeCount, 0); feedback.handle.dispose(); f.drain(); f.agent.dispose();
});

test('orb drag crosses a Native iframe and persists without replacing the iframe or opening the assistant', () => {
  const f = fixture(); f.agent.setAccount('A'); const frame = f.el('iframe'); f.main.append(frame);
  const blocked = () => f.document.head.children.some(node => node.tagName === 'STYLE' && /iframe\s*\{\s*pointer-events:\s*none\s*!important\s*;?\s*\}/.test(node.textContent));
  pointer(f.orb, 'pointerdown'); pointer(f.orb, 'pointermove', { clientX: 280, clientY: 40 });
  if (!blocked()) pointer(f.orb, 'lostpointercapture');
  pointer(f.orb, 'pointermove', { clientX: 170, clientY: 170 }); pointer(f.orb, 'pointerup', { clientX: 170, clientY: 170 });
  assert.equal(f.saved.has('soty.agent-orb.v1:A'), true); assert.equal(blocked(), false);
  assert.equal(frame.parentElement, f.main); assert.equal(frame.isConnected, true); assert.equal(f.records.length, 0); f.agent.dispose();
});

test('rejected orb pointer capture releases the guard without saving position or mounting an assistant', () => {
  const f = fixture(); f.agent.setAccount('A');
  f.orb.setPointerCapture = () => { throw new Error('Synthetic inactive pointer'); };
  pointer(f.orb, 'pointerdown'); assert.equal(f.iframePointersBlocked(), false); assert.equal(f.saved.size, 0); assert.equal(f.records.length, 0); f.agent.dispose();
});

test('viewport changes restore normalized orb preferences without overwriting an active or cancelled drag', () => {
  const f = fixture({ width: 1280, height: 720 }), key = 'soty.agent-orb.v1:A', saved = JSON.stringify({ x: .4, y: .6 });
  f.saved.set(key, saved); f.agent.setAccount('A');
  const point = () => ({ x: parseFloat(f.orb.style.left), y: parseFloat(f.orb.style.top) });
  const resize = (width, height, left = 0, top = 0, type = 'resize') => {
    Object.assign(f.view, { innerWidth: width, innerHeight: height });
    Object.assign(f.view.visualViewport, { width, height, offsetLeft: left, offsetTop: top });
    (type === 'resize' ? f.view : f.view.visualViewport).dispatchEvent(new Event(type));
  };
  assert.deepEqual(point(), { x: 512, y: 432 });
  resize(390, 844); assert.deepEqual(point(), { x: 156, y: 506.4 });
  resize(390, 844, 12, 25, 'scroll'); assert.deepEqual(point(), { x: 168, y: 531.4 });
  resize(1280, 720); assert.deepEqual(point(), { x: 512, y: 432 }); assert.equal(f.saved.get(key), saved);

  resize(390, 844);
  pointer(f.orb, 'pointerdown', { clientX: 156, clientY: 506.4 });
  pointer(f.orb, 'pointermove', { clientX: 176, clientY: 526.4 });
  const dragging = point(); resize(420, 900);
  assert.deepEqual(point(), dragging, 'resize cannot replace the unsaved drag with its old preference');
  assert.equal(f.iframePointersBlocked(), true); assert.equal(f.saved.get(key), saved);
  pointer(f.orb, 'pointercancel');
  assert.deepEqual(point(), { x: 168, y: 540 }); assert.equal(f.saved.get(key), saved);
  assert.equal(f.orb.hasPointerCapture(1), false); assert.equal(f.iframePointersBlocked(), false);

  pointer(f.orb, 'pointerdown'); resize(390, 844); pointer(f.orb, 'pointerup');
  assert.deepEqual(point(), { x: 156, y: 506.4 }); assert.equal(f.saved.get(key), saved, 'a click during resize must not save stale pixels');
  assert.equal(f.iframePointersBlocked(), false);
  pointer(f.orb, 'pointerdown', { clientX: 156, clientY: 506.4 });
  pointer(f.orb, 'pointermove', { clientX: 176, clientY: 526.4 }); resize(420, 900); pointer(f.orb, 'pointerup');
  assert.deepEqual(point(), { x: 176, y: 526.4 });
  assert.deepEqual(JSON.parse(f.saved.get(key)), { x: 176 / 420, y: 526.4 / 900 });
  assert.equal(f.iframePointersBlocked(), false); assert.equal(f.orb.hasPointerCapture(1), false);
  Object.defineProperty(f.view, 'localStorage', { get() { throw new Error('Synthetic unavailable storage'); } });
  resize(440, 920); assert.deepEqual(point(), { x: 176, y: 526.4 });
  assert.equal(f.records.length, 0); f.agent.dispose();
});

// Append to src/world/agent-orb.test.mjs.
test('orb leaves browser/system shortcuts and IME arrows untouched while preserving ordinary movement', () => {
  const f = fixture(); f.agent.setAccount('A'); const style = { ...f.orb.style }, saved = new Map(f.saved);
  for (const modifiers of [{ key: 'ArrowLeft', altKey: true }, { key: 'Home', ctrlKey: true }, { key: 'ArrowRight', metaKey: true }, { key: 'ArrowUp', isComposing: true }]) {
    const key = new Event('keydown', { cancelable: true }); Object.assign(key, modifiers); f.orb.dispatchEvent(key);
    assert.equal(key.defaultPrevented, false); assert.deepEqual(f.orb.style, style); assert.deepEqual(f.saved, saved);
  }
  const plain = new Event('keydown', { cancelable: true }); Object.assign(plain, { key: 'ArrowLeft', shiftKey: true }); f.orb.dispatchEvent(plain);
  assert.equal(plain.defaultPrevented, true); assert.equal(parseFloat(f.orb.style.left), parseFloat(style.left) - 40); f.agent.dispose();
});

test('a saved assistant position avoids the whole Stage notice and returns after it is hidden without changing its preference', () => {
  const f = fixture({ width: 320, height: 540 });
  const notice = f.el('aside', 'sa-stage-runtime-notice');
  const bounds = { left: 0, top: 161, width: 320, height: 57, right: 320, bottom: 218 };
  notice.getBoundingClientRect = () => bounds; f.main.append(notice);
  const saved = JSON.stringify({ x: 214 / 320, y: 168 / 540 });
  f.saved.set('soty.agent-orb.v1:A', saved); f.agent.setAccount('A');
  const intersects = () => {
    const orb = f.orb.getBoundingClientRect();
    return orb.left < bounds.right && orb.right > bounds.left && orb.top < bounds.bottom && orb.bottom > bounds.top;
  };
  assert.equal(intersects(), false, 'text outside the details button remains readable');
  assert.equal(f.saved.get('soty.agent-orb.v1:A'), saved, 'automatic avoidance never saves a new preference');
  notice.hidden = true; f.root.dispatchEvent(new Event('soty:floating-layout'));
  assert.equal(f.orb.style.left, '214px'); assert.equal(f.orb.style.top, '168px');
  assert.equal(f.saved.get('soty.agent-orb.v1:A'), saved); f.agent.dispose();
});

test('an owned modal ignores the background Stage notice and closing it restores notice avoidance', () => {
  const f = fixture({ width: 320, height: 540 });
  const notice = f.el('aside', 'sa-stage-runtime-notice');
  notice.getBoundingClientRect = () => ({ left: 0, top: 161, width: 320, height: 57, right: 320, bottom: 218 });
  f.main.append(notice);
  const saved = JSON.stringify({ x: 214 / 320, y: 168 / 540 });
  f.saved.set('soty.agent-orb.v1:A', saved);
  const app = f.world(), owned = app.dialog('Owned recovery');
  assert.equal(f.orb.parentElement, owned.element);
  assert.equal(f.orb.style.left, '214px'); assert.equal(f.orb.style.top, '168px');
  owned.close(); f.drain();
  const rect = f.orb.getBoundingClientRect();
  assert.ok(rect.bottom <= 161 || rect.top >= 218, 'returning to the Stage restores avoidance');
  assert.equal(f.saved.get('soty.agent-orb.v1:A'), saved); f.agent.dispose();
});


test('keyboard handover: Home cancels a drag and late pointer events cannot undock it', async () => {
  const f = fixture({ width: 1280, height: 720 }), key = 'soty.agent-orb.v1:A';
  f.saved.set(key, JSON.stringify({ x: .25, y: .25 })); f.agent.setAccount('A');
  try {
    pointer(f.orb, 'pointerdown', { clientX: 320, clientY: 180 });
    pointer(f.orb, 'pointermove', { clientX: 360, clientY: 220 });
    assert.equal(f.orb.hasPointerCapture(1), true); assert.equal(f.iframePointersBlocked(), true);
    const command = event('keydown', { key: 'Home' }); f.orb.dispatchEvent(command);
    assert.equal(command.defaultPrevented, true);
    assert.equal(f.orb.hasPointerCapture(1), false, 'keyboard releases capture before any late pointer event');
    assert.equal(f.iframePointersBlocked(), false, 'keyboard releases the iframe guard immediately');
    const keyboardPosition = { ...f.orb.style };
    pointer(f.orb, 'pointermove', { clientX: 500, clientY: 400 });
    pointer(f.orb, 'pointerup', { clientX: 500, clientY: 400 });
    assert.deepEqual(f.orb.style, keyboardPosition, 'late pointer events cannot replace the explicit Home command');
    assert.equal(f.orb.dataset.docked, 'true'); assert.equal(f.saved.has(key), false);
    assert.equal(f.orb.hasPointerCapture(1), false); assert.equal(f.iframePointersBlocked(), false);
    assert.equal(f.click(1), false, 'the cancelled drag still suppresses its trailing pointer click');
    assert.equal(f.dialog(), null); assert.equal(f.records.length, 0);
    assert.equal(f.click(0), true, 'keyboard activation is not the cancelled pointer click');
    await f.agent.flush(); assert.ok(f.dialog()); assert.equal(f.records.length, 1);
  } finally { f.agent.dispose(); }
});

test('keyboard handover: ArrowRight cancels a drag before moving the saved position', async () => {
  const f = fixture({ width: 1280, height: 720 }), key = 'soty.agent-orb.v1:A';
  f.saved.set(key, JSON.stringify({ x: .25, y: .25 })); f.agent.setAccount('A');
  try {
    pointer(f.orb, 'pointerdown', { clientX: 320, clientY: 180 });
    pointer(f.orb, 'pointermove', { clientX: 360, clientY: 220 });
    assert.equal(f.orb.hasPointerCapture(1), true); assert.equal(f.iframePointersBlocked(), true);
    const command = event('keydown', { key: 'ArrowRight' }); f.orb.dispatchEvent(command);
    assert.equal(command.defaultPrevented, true);
    assert.equal(parseFloat(f.orb.style.left), 336, 'keyboard starts from the saved position restored by cancellation');
    assert.equal(parseFloat(f.orb.style.top), 180);
    assert.equal(f.orb.hasPointerCapture(1), false, 'keyboard releases capture before any late pointer event');
    assert.equal(f.iframePointersBlocked(), false, 'keyboard releases the iframe guard immediately');
    const keyboardPosition = { ...f.orb.style }, saved = f.saved.get(key);
    assert.deepEqual(JSON.parse(saved), { x: 336 / 1280, y: 180 / 720 });
    pointer(f.orb, 'pointermove', { clientX: 500, clientY: 400 });
    pointer(f.orb, 'pointerup', { clientX: 500, clientY: 400 });
    assert.deepEqual(f.orb.style, keyboardPosition); assert.equal(f.saved.get(key), saved);
    assert.equal(f.orb.hasPointerCapture(1), false); assert.equal(f.iframePointersBlocked(), false);
    assert.equal(f.click(1), false); assert.equal(f.dialog(), null); assert.equal(f.records.length, 0);
    assert.equal(f.click(0), true); await f.agent.flush(); assert.ok(f.dialog()); assert.equal(f.records.length, 1);
  } finally { f.agent.dispose(); }
});
