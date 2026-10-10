import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import ts from 'typescript';
import { mountAppFullscreen } from './app-fullscreen.mjs';

// Production modules, synthetic DOM/native-close/pointer ports. No browser,
// iframe runtime, SDK, model, Dev, server or network is started by this suite.
const read = file => readFileSync(new URL(file, import.meta.url), 'utf8');
const compile = source => ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const orbCode = compile(read('./agent-orb.ts')), positionCode = compile(read('./agent-orb-position.ts')), dialogsCode = compile(read('./dialogs.ts'));
const stageAst = ts.createSourceFile('app-stage.ts', read('./app-stage.ts'), ts.ScriptTarget.Latest, true);
let fullscreenCallback;
function visit(node) {
  if (ts.isCallExpression(node) && node.expression.getText(stageAst) === 'mountAppFullscreen') {
    fullscreenCallback = node.arguments[0].properties.find(property => property.name?.getText(stageAst) === 'onChange')?.initializer.getText(stageAst);
  }
  ts.forEachChild(node, visit);
}
visit(stageAst); assert.ok(fullscreenCallback);

function fixture({ width = 390, height = 844, delayed = false } = {}) {
  const tasks = [], records = [], mountWaiters = [], saved = new Map(); let document, nextId = 0, focusedExisting = false;
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
  document.body = new Element('body'); document.activeElement = document.body;
  document.querySelectorAll = selector => document.body.querySelectorAll(selector); document.querySelector = selector => document.body.querySelector(selector);
  const view = Object.assign(new EventTarget(), { innerWidth: width, innerHeight: height,
    visualViewport: Object.assign(new EventTarget(), { width, height, offsetLeft: 0, offsetTop: 0 }),
    localStorage: { getItem: key => saved.get(key) ?? null, setItem: (key, value) => saved.set(key, value), removeItem: key => saved.delete(key) } });
  document.defaultView = view;
  const el = (...args) => new Element(...args);
  const button = (label, _symbol, className, action) => { const node = el('button', className ?? '', label); if (action) node.addEventListener('click', action); return node; };
  const dom = { el, button, iconButton: (label, symbol, action) => button(label, symbol, 'sw-icon-button', action), emptyState: (title, detail) => el('div', 'sw-empty', `${title}: ${detail}`) };
  const ports = { './dom': dom, './agent-orb.css': {} };
  function evaluate(code) {
    const module = { exports: {} };
    vm.runInNewContext(code, { module, exports: module.exports, require: name => { assert.ok(name in ports, `Unexpected Source import ${name}`); return ports[name]; },
      document, HTMLElement: Element, getComputedStyle: () => ({ visibility: 'visible' }), AbortController, Event, crypto: { randomUUID: () => `synthetic-${++nextId}` } });
    return module.exports;
  }
  const positions = evaluate(positionCode); ports['./agent-orb-position'] = positions;
  const dialogs = evaluate(dialogsCode); ports['./dialogs'] = dialogs;
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
  return { agent, orb, parking, root, main, document, view, positions, saved, records, el, click,
    dialog: () => document.querySelector('dialog[open]'),
    close() { document.querySelector('dialog[open]')?.querySelector('button')?.dispatchEvent(event('click', { detail: 1 })); },
    drain() { while (tasks.length) tasks.shift()(); },
    waitMount: (count = 1) => records.length >= count ? Promise.resolve() : new Promise(resolve => mountWaiters.push({ count, resolve })),
    existing: value => { focusedExisting = value; },
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
  if (cancel === 'Escape') f.orb.dispatchEvent(event('keydown', { key: 'Escape' })); else pointer(f.orb, cancel);
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
