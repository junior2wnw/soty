import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import vm from 'node:vm';
import ts from 'typescript';

// Actual AccessPanel controller, synthetic DOM/dialog and signed-API ports.
// This does not prove native modal focus, layout, Connect signing or an AS flow.
const compiled = ts.transpileModule(await readFile(new URL('../access-panel.ts', import.meta.url), 'utf8'), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
const turn = () => new Promise(resolve => setImmediate(resolve));
const settle = async () => { await turn(); await turn(); };
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const accountId = 'qa_owner', time = 2000000000000;
const principal = (id, managedBy, label = 'Codex CLI') => ({ id, accountId, clientId: `client_${id}`, label,
  state: 'active', createdAt: time, revokedAt: null, ...(managedBy ? { managedBy } : {}) });
const connection = (id, profile = 'soty-codex-cli') => ({ id, clientProfile: profile, resource: 'https://private.example.invalid/api/capabilities/v1',
  createdAt: time, expiresAt: time + 86400000, revokedAt: null, active: true, budget: { limit: 20, spent: 3, reserved: 1, remaining: 16 } });
const error = code => Object.assign(new Error('Synthetic boundary error'), { code });

class Element {
  constructor(document, tag, className = '', text = '') {
    this.ownerDocument = document; this.tagName = tag.toUpperCase(); this.className = className; this.ownText = text;
    this.children = []; this.parentElement = null; this.dataset = {}; this.attributes = new Map(); this.listeners = new Map();
    this.disabled = false; this.hidden = false; this.open = false; this.tabIndex = 0; this.style = {};
    this.classList = {
      contains: name => this.className.split(/\s/u).includes(name),
      add: (...names) => { this.className = [...new Set([...this.className.split(/\s/u).filter(Boolean), ...names])].join(' '); },
      remove: (...names) => { this.className = this.className.split(/\s/u).filter(name => !names.includes(name)).join(' '); },
      toggle: (name, on) => { const enabled = on ?? !this.classList.contains(name); if (enabled) this.classList.add(name); else this.classList.remove(name); return enabled; },
    };
  }
  get isConnected() { return this === this.ownerDocument.body || Boolean(this.parentElement?.isConnected); }
  get textContent() { return this.ownText + this.children.map(node => node.textContent).join(''); }
  set textContent(value) { this.ownText = String(value); this.replaceChildren(); }
  append(...nodes) { for (const node of nodes) { node.remove(); node.parentElement = this; this.children.push(node); } }
  replaceChildren(...nodes) { for (const node of [...this.children]) node.remove(); this.append(...nodes); }
  remove() {
    if (this.contains(this.ownerDocument.activeElement)) this.ownerDocument.activeElement = this.ownerDocument.body;
    if (this.parentElement) this.parentElement.children = this.parentElement.children.filter(node => node !== this);
    this.parentElement = null;
  }
  contains(node) { return node === this || this.children.some(child => child.contains(node)); }
  setAttribute(name, value) { this.attributes.set(name, String(value)); if (name === 'tabindex') this.tabIndex = Number(value); }
  getAttribute(name) {
    if (name.startsWith('data-')) return this.dataset[name.slice(5).replace(/-([a-z])/gu, (_, letter) => letter.toUpperCase())] ?? this.attributes.get(name) ?? null;
    return this.attributes.get(name) ?? null;
  }
  hasAttribute(name) { return name === 'open' ? this.open : this.getAttribute(name) !== null; }
  addEventListener(type, callback, capture = false) { const listeners = this.listeners.get(type) ?? []; listeners.push({ callback, capture: capture === true }); this.listeners.set(type, listeners); }
  removeEventListener(type, callback) { this.listeners.set(type, (this.listeners.get(type) ?? []).filter(item => item.callback !== callback)); }
  dispatch(type, extra = {}) {
    const event = { type, target: this, currentTarget: this, defaultPrevented: false, stopped: false,
      preventDefault() { this.defaultPrevented = true; }, stopPropagation() {}, stopImmediatePropagation() { this.stopped = true; }, ...extra };
    for (const listener of [...(this.listeners.get(type) ?? [])].sort((a, b) => Number(b.capture) - Number(a.capture))) {
      listener.callback(event); if (event.stopped) break;
    }
    return event;
  }
  click() { if (!this.disabled) this.dispatch('click'); }
  focus() { if (this.isConnected && !this.disabled) this.ownerDocument.activeElement = this; }
  matches(selector) {
    if (selector === ':disabled') return this.disabled;
    const tag = /^([a-z]+)/iu.exec(selector)?.[1];
    if (tag && this.tagName !== tag.toUpperCase()) return false;
    for (const name of selector.matchAll(/\.([\w-]+)/gu)) if (!this.classList.contains(name[1])) return false;
    for (const attribute of selector.matchAll(/\[([\w-]+)(?:="([^"]*)")?\]/gu)) {
      if (!this.hasAttribute(attribute[1]) || attribute[2] !== undefined && this.getAttribute(attribute[1]) !== attribute[2]) return false;
    }
    return Boolean(tag || selector.startsWith('.') || selector.startsWith('['));
  }
  querySelectorAll(selector) {
    const parts = selector.split(' '), last = parts.at(-1), result = [];
    const matches = node => {
      if (!node.matches(last)) return false;
      if (parts.length === 1) return true;
      for (let parent = node.parentElement; parent && this.contains(parent); parent = parent.parentElement) if (parent.matches(parts[0])) return true;
      return false;
    };
    const visit = parent => { for (const node of parent.children) { if (matches(node)) result.push(node); visit(node); } }; visit(this); return result;
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null; }
}

function fixture(t, initial = {}) {
  const document = { body: null, activeElement: null }, make = (tag, className = '', text = '') => new Element(document, tag, className, text);
  document.body = make('body'); document.activeElement = document.body;
  const host = make('main'), outside = make('button', '', 'Outside'); document.body.append(host, outside);
  const calls = [], data = { principals: [principal('oauth_one', 'oauth'), principal('key_one')], connections: [connection('connection_one')], ...initial };
  const handlers = new Map();
  const dom = {
    el: make,
    button(label, symbol, className, action) { const node = make('button', `sw-button ${className ?? ''}`); if (symbol) node.append(make('svg')); node.append(make('span', '', label)); if (action) node.addEventListener('click', action); return node; },
    iconButton(label, symbol, action) { const node = this.button(label, symbol, 'sw-icon-button', action); node.setAttribute('aria-label', label); return node; },
    labeledField(label, input) { const node = make('label'); node.append(make('span', '', label), input); return node; },
    textInput(value) { const node = make('input'); node.value = value; return node; },
  };
  // Module functions do not receive dom as `this`.
  dom.iconButton = (label, symbol, action) => { const node = dom.button(label, symbol, 'sw-icon-button', action); node.setAttribute('aria-label', label); return node; };
  const createDialog = (title, onClose, returnTarget) => {
    const opener = document.activeElement, element = make('dialog'), header = make('div', 'sw-dialog-header'), body = make('div', 'sw-dialog-content');
    let closed = false;
    const close = () => {
      if (closed) return; closed = true;
      const owned = document.activeElement === document.body || element.contains(document.activeElement) || document.activeElement === opener;
      element.remove(); onClose?.({ interrupted: !owned });
      if (owned && (!returnTarget || returnTarget.isCurrent())) (returnTarget?.resolve() ?? opener)?.focus();
    };
    header.append(make('h2', '', title), dom.iconButton('Закрыть', 'close', close)); element.append(header, body); document.body.append(element);
    return { element, body, close };
  };
  const module = { exports: {} }, listeners = new Map();
  vm.runInNewContext(compiled, { module, exports: module.exports, require: name => ({ './dom': dom, './dialogs': { createDialog }, './icons': { icon: () => make('svg') } })[name] ?? {},
    document, HTMLElement: Element, crypto: { randomUUID }, URL, Date, Set, Map, console,
    window: { addEventListener: (name, fn) => listeners.set(name, fn), removeEventListener: name => listeners.delete(name) },
  });
  const api = { async request(method, args) {
    assert.equal(args.expectedAccountId, accountId);
    const copied = structuredClone(args); calls.push({ method, args: copied });
    if (handlers.has(method)) return handlers.get(method)(copied);
    if (method === 'access.principals.list') return { principals: structuredClone(data.principals), cursor: null };
    if (method === 'oauth.connections.list') return { connections: structuredClone(data.connections), nextCursor: null };
    if (method === 'access.invocations.list') return { invocations: [], nextCursor: null };
    if (method === 'access.events.list') return { events: [], cursor: null };
    if (method === 'access.grants.list') return { grants: [], cursor: null };
    if (method === 'oauth.connections.revoke') { const item = data.connections.find(row => row.id === args.connectionId); item.revokedAt = time + 1; item.active = false; return { connectionId: item.id, revoked: true }; }
    if (method === 'access.principals.revoke') { const item = data.principals.find(row => row.id === args.principalId); item.state = 'revoked'; item.revokedAt = time + 1; return { principal: structuredClone(item) }; }
    throw new Error(`Unexpected synthetic operation: ${method}`);
  } };
  // Mount after optional gates are installed, without a second transient mount.
  for (const [method, handler] of Object.entries(initial.handlers ?? {})) handlers.set(method, handler);
  const handle = module.exports.mountAccessPanel(host, { api, accountId, accountLabel: 'Вымышленный владелец', availability: async () => ({ notesCreateEnabled: false, audience: null }) });
  t.after(() => handle.dispose());
  const buttons = (label, scope = document.body) => scope.querySelectorAll('button').filter(node => node.textContent === label || node.getAttribute('aria-label') === label);
  const button = (label, scope) => { const found = buttons(label, scope); assert.ok(found.length, `button missing: ${label}`); return found[0]; };
  return { document, host, outside, data, calls, handlers, handle, buttons, button,
    dialog: () => document.body.querySelector('dialog'),
    target: key => host.querySelectorAll('[data-sa-focus]').find(node => node.dataset.saFocus === key),
    writes: () => calls.filter(call => call.method.endsWith('.revoke')),
  };
}

export { fixture, principal, connection, error, deferred, settle, time };
