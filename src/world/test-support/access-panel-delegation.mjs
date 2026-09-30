import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import vm from 'node:vm';
import ts from 'typescript';

// Actual controller; synthetic DOM and signed-API boundary. Native layout,
// checkbox keyboard activation and real Connect signing need the HTML fixture.
const compiled = ts.transpileModule(await readFile(new URL('../access-panel.ts', import.meta.url), 'utf8'), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
export const settle = async () => { await new Promise(setImmediate); await new Promise(setImmediate); };
export const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
export const time = 2000000000000, audience = 'https://delegation.example.invalid';
export const principal = (id, accountId = 'owner_a') => ({ id, accountId, clientId: `client_${id}`, label: 'Одинаковое имя', state: 'active', createdAt: time, revokedAt: null });
export const grant = (id, principalId, overrides = {}) => ({ id, principalId, clientId: `client_${principalId}`, parentGrantId: null, rootGrantId: id,
  capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'],
  expiresAt: time + 86400000, createdAt: time, revokedAt: null, allowDelegation: false, maxDepth: 0, depth: 0,
  budget: { unit: 'invocations', limit: 10, reserved: 1, spent: 2, remaining: 7, uncertain: 1 }, ...overrides });

class Element {
  constructor(document, tag, className = '', text = '') {
    Object.assign(this, { ownerDocument: document, tagName: tag.toUpperCase(), className, ownText: text,
      children: [], parentElement: null, dataset: {}, attributes: new Map(), listeners: new Map(),
      disabled: false, hidden: false, checked: false, open: false, tabIndex: 0, style: {}, _value: '' });
    this.classList = {
      contains: name => this.className.split(/\s/u).includes(name),
      add: (...names) => { this.className = [...new Set([...this.className.split(/\s/u).filter(Boolean), ...names])].join(' '); },
      remove: (...names) => { this.className = this.className.split(/\s/u).filter(name => !names.includes(name)).join(' '); },
      toggle: (name, on) => { const value = on ?? !this.classList.contains(name); this.classList[value ? 'add' : 'remove'](name); return value; },
    };
  }
  get value() { return this.tagName === 'SELECT' ? (this.selectedOptions[0]?.value ?? '') : this._value; }
  set value(value) { this._value = String(value); if (this.tagName === 'SELECT') for (const child of this.children) child.selected = child.value === this._value; }
  get selectedOptions() { return this.children.filter(child => child.selected); }
  get isConnected() { return this === this.ownerDocument.body || Boolean(this.parentElement?.isConnected); }
  get textContent() { return this.ownText + this.children.map(node => node.textContent).join(''); }
  set textContent(value) { this.ownText = String(value); this.replaceChildren(); }
  append(...nodes) { for (const node of nodes) { node.remove(); node.parentElement = this; this.children.push(node); } }
  replaceChildren(...nodes) { for (const node of [...this.children]) node.remove(); this.append(...nodes); }
  contains(node) { return node === this || this.children.some(child => child.contains(node)); }
  remove() { if (this.contains(this.ownerDocument.activeElement)) this.ownerDocument.activeElement = this.ownerDocument.body;
    if (this.parentElement) this.parentElement.children = this.parentElement.children.filter(node => node !== this); this.parentElement = null; }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  getAttribute(name) { return name.startsWith('data-') ? this.dataset[name.slice(5).replace(/-([a-z])/gu, (_, x) => x.toUpperCase())] ?? this.attributes.get(name) ?? null : this.attributes.get(name) ?? null; }
  removeAttribute(name) { this.attributes.delete(name); }
  hasAttribute(name) { return name === 'open' ? this.open : this.getAttribute(name) !== null; }
  addEventListener(type, callback, capture = false) { const list = this.listeners.get(type) ?? []; list.push({ callback, capture: capture === true }); this.listeners.set(type, list); }
  dispatch(type, extra = {}) {
    const event = { type, target: this, currentTarget: this, defaultPrevented: false, stopped: false,
      preventDefault() { this.defaultPrevented = true; }, stopPropagation() {}, stopImmediatePropagation() { this.stopped = true; }, ...extra };
    for (const entry of [...(this.listeners.get(type) ?? [])].sort((a, b) => Number(b.capture) - Number(a.capture))) { entry.callback(event); if (event.stopped) break; }
    return event;
  }
  click() { if (!this.disabled) this.dispatch('click'); }
  focus() { if (this.isConnected && !this.disabled) this.ownerDocument.activeElement = this; }
  select() { this.selectedText = true; }
  reportValidity() { return this.querySelectorAll('input').every(node => node.disabled || !node.required || Boolean(node.value)); }
  matches(selector) {
    if (selector === ':disabled') return this.disabled;
    const tag = /^([a-z]+)/iu.exec(selector)?.[1]; if (tag && this.tagName !== tag.toUpperCase()) return false;
    for (const match of selector.matchAll(/\.([\w-]+)/gu)) if (!this.classList.contains(match[1])) return false;
    for (const match of selector.matchAll(/\[([\w-]+)(?:="([^"]*)")?\]/gu)) if (!this.hasAttribute(match[1]) || match[2] !== undefined && this.getAttribute(match[1]) !== match[2]) return false;
    return Boolean(tag || selector.startsWith('.') || selector.startsWith('['));
  }
  querySelectorAll(selector) {
    const parts = selector.split(' '), result = [];
    const visit = parent => { for (const node of parent.children) {
      if (node.matches(parts.at(-1))) {
        if (parts.length === 1) result.push(node);
        else for (let ancestor = node.parentElement; ancestor && this.contains(ancestor); ancestor = ancestor.parentElement) if (ancestor.matches(parts[0])) { result.push(node); break; }
      }
      visit(node);
    } }; visit(this); return result;
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null; }
}

export function fixture(t, initial = {}) {
  const document = { body: null, activeElement: null }, make = (tag, name = '', text = '') => new Element(document, tag, name, text);
  document.body = make('body'); document.activeElement = document.body;
  const host = make('main'); document.body.append(host);
  const data = { principals: [], grants: [], events: [], ...initial.data }, calls = [], handlers = new Map(), windowEvents = new Map();
  let currentAccount = 'owner_a', handle, created = 0, copied = 0;
  const dom = { el: make, button(label, symbol, name, action) { const node = make('button', `sw-button ${name ?? ''}`); node.type = 'button';
    if (symbol) node.append(make('svg')); node.append(make('span', '', label)); if (action) node.addEventListener('click', action); return node; },
    labeledField(label, input) { const node = make('label', 'sw-field'); node.append(make('span', '', label), input); return node; },
    textInput(value) { const node = make('input'); node.type = 'text'; node.value = value; return node; } };
  dom.iconButton = (label, symbol, action) => { const node = dom.button(label, symbol, 'sw-icon-button', action); node.setAttribute('aria-label', label); return node; };
  const createDialog = (title, onClose, returnTarget) => {
    const opener = document.activeElement, element = make('dialog'), header = make('div', 'sw-dialog-header'), body = make('div', 'sw-dialog-content'); let closed = false;
    const close = () => { if (closed) return; closed = true; const owned = element.contains(document.activeElement) || document.activeElement === document.body;
      element.remove(); onClose?.({ interrupted: !owned }); if (owned && (!returnTarget || returnTarget.isCurrent())) (returnTarget?.resolve() ?? opener)?.focus(); };
    header.append(make('h2', '', title), dom.iconButton('Закрыть', 'close', close)); element.append(header, body); document.body.append(element);
    return { element, body, close };
  };
  const module = { exports: {} };
  vm.runInNewContext(compiled, { module, exports: module.exports, require: name => ({ './dom': dom, './dialogs': { createDialog }, './icons': { icon: () => make('svg') } })[name] ?? {},
    document, HTMLElement: Element, crypto: { randomUUID }, URL, Date, Set, Map,
    navigator: { clipboard: { async writeText(value) { assert.ok(typeof value === 'string' && value.startsWith('soty_cap_'), 'synthetic secret only'); copied++; } } },
    window: { addEventListener: (name, fn) => windowEvents.set(name, fn), removeEventListener: name => windowEvents.delete(name) },
  });
  const api = { async request(method, args) {
    assert.equal(args.expectedAccountId, currentAccount, 'request remains bound to the mounted account');
    const copiedArgs = structuredClone(args); calls.push({ method, args: copiedArgs });
    if (handlers.has(method)) return handlers.get(method)(copiedArgs);
    if (method === 'access.principals.list') return { principals: structuredClone(data.principals.filter(item => item.accountId === currentAccount)), cursor: null };
    if (method === 'access.grants.list') return { grants: structuredClone(data.grants.filter(item => item.principalId === args.principalId)), cursor: null };
    if (method === 'access.events.list') return { events: structuredClone(data.events), cursor: null };
    if (method === 'oauth.connections.list') return { connections: [], nextCursor: null };
    if (method === 'access.invocations.list') return { invocations: [], nextCursor: null };
    if (method === 'access.principals.create') { const item = { ...principal(`new_${++created}`, currentAccount), label: args.label }; data.principals.push(item); return { principal: structuredClone(item) }; }
    if (method === 'access.grants.issue') { const item = grant(`grant_${created}`, args.principalId, { ...copiedArgs, rootGrantId: `grant_${created}` }); delete item.expectedAccountId;
      item.budget = { ...args.budget, spent: 0, reserved: 0, remaining: args.budget.limit, uncertain: 0 }; data.grants.push(item); return { grant: structuredClone(item) }; }
    if (method === 'access.credentials.issue') return { token: `soty_cap_${'q'.repeat(43)}`, credential: { id: 'test_credential', grantId: args.grantId, audience: args.audience, expiresAt: args.expiresAt } };
    if (method === 'access.principals.revoke') { const item = data.principals.find(value => value.id === args.principalId); item.state = 'revoked'; item.revokedAt = time + 1; return { principal: structuredClone(item) }; }
    if (method === 'access.grants.revoke') { const item = data.grants.find(value => value.id === args.grantId); item.revokedAt = time + 1; return { grant: structuredClone(item) }; }
    throw new Error(`Unexpected synthetic operation ${method}`);
  } };
  const controls = { availability: async () => ({ notesCreateEnabled: true, audience }) };
  function mount(accountId = 'owner_a') { handle?.dispose(); currentAccount = accountId;
    handle = module.exports.mountAccessPanel(host, { api, accountId, accountLabel: `Тест ${accountId}`, availability: () => controls.availability() }); return handle; }
  mount(); t.after(() => handle?.dispose());
  const button = (label, scope = document.body) => { const result = scope.querySelectorAll('button').find(node => node.textContent === label || node.getAttribute('aria-label') === label); assert.ok(result, `button missing: ${label}`); return result; };
  return { document, host, data, calls, handlers, controls, mount, button, windowEvents, get handle() { return handle; }, get copied() { return copied; },
    dialog: () => document.body.querySelector('dialog'), target: key => host.querySelectorAll('[data-sa-focus]').find(node => node.dataset.saFocus === key),
    writes: () => calls.filter(call => !call.method.endsWith('.list')),
    async openCreate() { button('Создать доступ по ключу').click(); await settle(); const form = document.body.querySelector('form'); assert.ok(form); return form; },
  };
}
