import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';

// Emitted real controller with synthetic DOM/account/network ports. This proves
// signed-intent selection and async fences, not real browser SSO or geometry.
const compiled = ts.transpileModule(await readFile(new URL('./human-login.ts', import.meta.url), 'utf8'), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
const turn = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
class Element {
  constructor(document, tag = 'div', className = '', text = '') {
    Object.assign(this, { ownerDocument: document, tagName: tag.toUpperCase(), className, ownText: text,
      children: [], parentElement: null, attributes: new Map(), dataset: {}, listeners: new Map(), disabled: false, checked: false });
  }
  get textContent() { return this.ownText + this.children.map(child => child.textContent).join(''); }
  append(...children) { for (const child of children) { child.remove(); child.parentElement = this; this.children.push(child); } }
  replaceChildren(...children) { for (const child of [...this.children]) child.remove(); this.append(...children); }
  remove() { if (this.parentElement) this.parentElement.children = this.parentElement.children.filter(child => child !== this); this.parentElement = null; }
  setAttribute(key, value) { this.attributes.set(key, String(value)); }
  addEventListener(type, listener) { const list = this.listeners.get(type) ?? []; list.push(listener); this.listeners.set(type, list); }
  querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null; }
  querySelectorAll(selector) {
    const control = /^\[data-login-control="([a-z]+)"\]$/u.exec(selector);
    const match = node => control ? node.dataset.loginControl === control[1] : node.tagName.toLowerCase() === selector;
    return this.children.flatMap(child => [...(match(child) ? [child] : []), ...child.querySelectorAll(selector)]);
  }
  focus() { this.ownerDocument.activeElement = this; }
  fire(type) { if (!this.disabled) for (const listener of this.listeners.get(type) ?? []) listener({ currentTarget: this, target: this }); }
  click() { this.fire('click'); }
}
function fixture(t, { accountId = 'account_A', decision = 'pending', deviceConsent = true, continuation = async () => false, presentation = 'login' } = {}) {
  const document = { activeElement: null }, make = (tag, className = '', text = '') => new Element(document, tag, className, text), host = make('main');
  let account = { accountId, label: 'Synthetic profile', revoked: false }, observe = null, serial = 0, monotonic = 100;
  const context = { schema: 'soty.human-login-context.v1', interactionId: 'synthetic_interaction_123', browserNonce: 'b'.repeat(43), csrf: 'c'.repeat(43),
    client: { id: 'app-alpha', label: 'Native application' }, scopes: ['openid', 'profile'], expiresAt: 1000000, remainingMs: 60000, decision,
    renewal: { maximumSessionSeconds: 86400, ...(decision === 'approved' ? { approvedSessionSeconds: 86400 } : {}) },
    ...(deviceConsent ? { deviceConsent: { available: decision === 'pending', ...(decision === 'approved' ? { approvedRememberForDevice: true } : {}) } } : {}) };
  const calls = { continuation: [], decisions: [], complete: 0, create: 0, context: 0 };
  let decidePort = async () => {}, continuePort = continuation, accountPort = async () => ({ ...account }), completePort = async () => {};
  const module = { exports: {} }, timers = new Map(); let timer = 0;
  const dom = { el: make, button(label, _icon, className, onClick) { const node = make('button', className, label); node.addEventListener('click', onClick); return node; } };
  vm.runInNewContext(compiled, { module, exports: module.exports, require: name => name === './dom' ? dom : {},
    crypto: { randomUUID: () => 'synthetic_request_' + (++serial) }, performance: { now: () => monotonic },
    setTimeout(fn) { const id = ++timer; timers.set(id, fn); return id; }, clearTimeout(id) { timers.delete(id); } });
  const handle = module.exports.mountHumanLogin(host, {
    presentation,
    account: () => accountPort(), context: async () => { calls.context++; return structuredClone(context); },
    continue: async args => { calls.continuation.push(structuredClone(args)); return continuePort(args); },
    decide: async args => { calls.decisions.push(structuredClone(args)); return decidePort(args); },
    complete: async () => { calls.complete++; return completePort(); },
    createProfile: async () => { calls.create++; account = { ...account, accountId: 'created_profile' }; return { accountId: account.accountId }; },
    openAccount: async () => {}, observeAccount(listener) { observe = listener; return () => { observe = null; }; },
  });
  t.after(() => handle.dispose());
  return { host, context, calls, handle,
    control(name) { return host.querySelector(`[data-login-control="${name}"]`); },
    select(name, checked) { const node = this.control(name); assert.ok(node && !node.disabled); node.checked = checked; node.fire('change'); },
    setContinuation(fn) { continuePort = fn; }, setDecision(fn) { decidePort = fn; }, setComplete(fn) { completePort = fn; },
    setAccountRead(fn) { accountPort = fn; }, setAccount(id, notify = true) { account = { ...account, accountId: id }; if (notify) observe?.(); },
    expire() { monotonic = 60200; },
  };
}

test('without remembered consent there is one signed attempt, then an explicit unchecked form; no account is created', async t => {
  const f = fixture(t); await turn();
  assert.equal(f.calls.continuation.length, 1); assert.equal(f.calls.create, 0); assert.equal(f.calls.complete, 0);
  assert.equal(f.control('remember').checked, false); assert.equal(f.control('stay').checked, false);
  assert.equal(f.control('approve').textContent, 'Войти'); await turn(); assert.equal(f.calls.continuation.length, 1);
});

test('an existing preference uses fresh signed continuation and completion without another manual approval', async t => {
  const f = fixture(t, { continuation: async () => true }); await turn();
  assert.equal(f.calls.continuation.length, 1); assert.equal(f.calls.continuation[0].expectedAccountId, 'account_A');
  assert.equal(f.calls.decisions.length, 0); assert.equal(f.calls.create, 0); assert.equal(f.calls.complete, 1);
  assert.equal(f.control('remember').checked, true); assert.equal(f.control('stay'), null);
});

test('remember captures one signed finite intent and hides the separate 24h choice without removing normal24h', async t => {
  const f = fixture(t); await turn(); f.select('remember', true);
  assert.equal(f.control('stay'), null); f.control('approve').click(); await turn();
  assert.equal(f.calls.decisions.length, 1); assert.equal(f.calls.decisions[0].rememberForDevice, true);
  assert.equal(f.calls.decisions[0].stayInAppSeconds, 86400); assert.equal(f.calls.complete, 1);
  const ordinary = fixture(t, { deviceConsent: false }); await turn(); ordinary.select('stay', true); ordinary.control('approve').click(); await turn();
  assert.equal(ordinary.calls.continuation.length, 0); assert.equal(ordinary.calls.decisions[0].stayInAppSeconds, 86400);
  assert.equal(Object.hasOwn(ordinary.calls.decisions[0], 'rememberForDevice'), false);
});

test('unknown continuation ACK keeps the identical intent for explicit retry and has no automatic loop', async t => {
  const f = fixture(t, { continuation: async () => { throw new Error('synthetic lost ACK'); } }); await turn();
  assert.equal(f.calls.continuation.length, 1); assert.equal(f.handle.canReload(), false); assert.equal(f.calls.complete, 0);
  await turn(); assert.equal(f.calls.continuation.length, 1); f.setContinuation(async () => true); f.control('approve').click(); await turn();
  assert.deepEqual(f.calls.continuation[0], f.calls.continuation[1]); assert.equal(f.calls.complete, 1);
});

for (const boundary of ['observed-profile', 'unobserved-profile', 'dispose']) test(`late continuation after ${boundary} never completes or creates a profile`, async t => {
  const hold = deferred(), f = fixture(t, { continuation: () => hold.promise }); await turn();
  if (boundary === 'dispose') f.handle.dispose(); else f.setAccount('account_B', boundary === 'observed-profile');
  hold.resolve(true); await turn(); assert.equal(f.calls.complete, 0); assert.equal(f.calls.decisions.length, 0); assert.equal(f.calls.create, 0);
  if (boundary !== 'dispose') { assert.equal(f.control('approve').disabled, true); assert.equal(f.handle.canReload(), true); }
});

test('known revoked preference invalidates the request, while approved reload preserves the old signed choice', async t => {
  const f = fixture(t, { continuation: async () => { throw Object.assign(new Error('revoked'), { code: 'human_identity_device_consent_revoked' }); } }); await turn();
  assert.equal(f.control('approve').disabled, true); assert.equal(f.handle.canReload(), true); assert.equal(f.calls.complete, 0);
  const reload = fixture(t, { decision: 'approved' }); await turn();
  assert.equal(reload.calls.continuation.length, 0); assert.equal(reload.control('remember').checked, true); assert.equal(reload.control('stay'), null);
  reload.control('approve').click(); await turn();
  assert.equal(reload.calls.decisions[0].rememberForDevice, true); assert.equal(reload.calls.decisions[0].stayInAppSeconds, 86400);
});

test('empty profile never auto-creates one and expiry prevents a held preference from signing', async t => {
  const absent = fixture(t, { accountId: null }); await turn(); assert.equal(absent.calls.create, 0); assert.equal(absent.calls.continuation.length, 0);
  const hold = deferred(), f = fixture(t, { continuation: () => hold.promise }); await turn(); f.expire(); hold.resolve(true); await turn();
  assert.equal(f.calls.complete, 0); assert.equal(f.calls.create, 0); assert.equal(f.control('approve').disabled, true);
});

test('forget presentation suppresses continuation and signs only one explicit deny+forget intent', async t => {
  const f = fixture(t, { presentation: 'forget', continuation: async () => true }); await turn();
  assert.equal(f.calls.continuation.length, 0); assert.equal(f.calls.decisions.length, 0); assert.equal(f.calls.complete, 0);
  assert.equal(f.control('remember'), null); assert.equal(f.control('stay'), null);
  assert.equal(f.control('approve').textContent, 'Выйти и забыть на этом устройстве');
  f.control('approve').click(); await turn();
  assert.equal(f.calls.decisions.length, 1); assert.equal(f.calls.decisions[0].decision, 'deny'); assert.equal(f.calls.decisions[0].forgetForDevice, true);
  assert.equal(Object.hasOwn(f.calls.decisions[0], 'rememberForDevice'), false);
  assert.equal(Object.hasOwn(f.calls.decisions[0], 'stayInAppSeconds'), false); assert.equal(f.calls.complete, 1);
  const absent = fixture(t, { presentation: 'forget', accountId: null }); await turn(); absent.control('approve').click(); await turn();
  assert.equal(absent.calls.create, 0); assert.equal(absent.calls.decisions.length, 0);
});

for (const code of ['DEVICE_REVOKED', 'device_revoked']) test(`a current Connect ${code} response is terminal, never an unknown retry loop`, async t => {
  const f = fixture(t, { continuation: async () => { throw Object.assign(new Error('current device unavailable'), { code }); } }); await turn();
  assert.equal(f.calls.continuation.length, 1); assert.equal(f.control('approve').disabled, true); assert.equal(f.handle.canReload(), true);
  assert.equal(f.calls.complete, 0); await turn(); assert.equal(f.calls.continuation.length, 1);
});
