import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';

// Actual consent controller, synthetic UI/account/network/clock ports. This is
// not native browser focus, CSS geometry, Connect signing or an OAuth flow.
const source = await readFile(new URL('./oauth-consent.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
const turn = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const serverTime = 2000000000000;

class NodePort {
  constructor(document, tag = 'div', className = '', text = '') {
    this.ownerDocument = document; this.tagName = tag.toUpperCase(); this.className = className;
    this.ownText = text; this.children = []; this.parentElement = null; this.attributes = new Map(); this.dataset = {};
    this.listeners = new Map(); this.disabled = false; this.open = false; this.hidden = false;
  }
  get isConnected() { return this === this.ownerDocument.body || Boolean(this.parentElement?.isConnected); }
  get textContent() { return this.ownText + this.children.map(child => child.textContent).join(''); }
  set textContent(value) { this.ownText = value; this.replaceChildren(); }
  append(...children) { for (const child of children) { child.remove(); child.parentElement = this; this.children.push(child); } }
  replaceChildren(...children) { for (const child of [...this.children]) child.remove(); this.append(...children); }
  remove() { if (this.parentElement) this.parentElement.children = this.parentElement.children.filter(child => child !== this); this.parentElement = null; }
  contains(node) { return this === node || this.children.some(child => child.contains(node)); }
  setAttribute(key, value) { this.attributes.set(key, String(value)); if (key.startsWith('data-')) this.dataset[key.slice(5).replace(/-([a-z])/gu, (_, letter) => letter.toUpperCase())] = String(value); }
  getAttribute(key) { return this.attributes.get(key) ?? null; }
  removeAttribute(key) { this.attributes.delete(key); }
  addEventListener(type, listener) { const list = this.listeners.get(type) ?? []; list.push(listener); this.listeners.set(type, list); }
  removeEventListener(type, listener) { this.listeners.set(type, (this.listeners.get(type) ?? []).filter(value => value !== listener)); }
  focus() { this.ownerDocument.activeElement = this; }
  querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null; }
  querySelectorAll(selector) {
    const result = [];
    const match = node => {
      if (selector.startsWith('.')) return node.className.split(' ').includes(selector.slice(1));
      if (selector.startsWith('#')) return node.id === selector.slice(1);
      const attribute = /^\[([\w-]+)(?:="([^"]*)")?\]$/u.exec(selector);
      if (attribute) return attribute[2] === undefined ? node.getAttribute(attribute[1]) !== null : node.getAttribute(attribute[1]) === attribute[2];
      return node.tagName.toLowerCase() === selector.toLowerCase();
    };
    for (const child of this.children) { if (match(child)) result.push(child); result.push(...child.querySelectorAll(selector)); }
    return result;
  }
  click() { if (!this.disabled) for (const listener of this.listeners.get('click') ?? []) listener({ currentTarget: this, target: this, preventDefault() {} }); }
}

function fixture(t, { skew = 0, decision = 'pending', decidedAccountId = null, accountId = 'account_A' } = {}) {
  const document = { activeElement: null, body: null, createElement: tag => make(tag) };
  const make = (tag, className = '', text = '') => new NodePort(document, tag, className, text);
  document.body = make('body'); document.activeElement = document.body;
  const host = make('main'), outside = make('button', '', 'Outside fixture control'); document.body.append(host, outside);
  let account = { accountId, label: accountId === 'account_A' ? 'Synthetic A' : 'Synthetic B' }, listener = null;
  let wallTime = serverTime + skew, monotonic = 100;
  const proposal = { interactionId: 'synthetic_interaction_123', contextDigest: 'a'.repeat(64), browserNonce: 'b'.repeat(43),
    clientProfile: 'soty-codex-cli', clientLabel: 'Synthetic external client', resource: 'https://shell.test/mcp', scope: 'notes.createDraft',
    durationMs: 86400000, budgetLimit: 20, checkedAt: serverTime, expiresAt: serverTime + 60000, decision, decidedAccountId };
  const calls = { account: 0, context: 0, decisions: [], complete: 0, completionAccounts: [], openAccount: 0 };
  let decidePort = async () => {}, contextPort = async () => ({ ...proposal }), accountPort = async () => ({ ...account }), completionPort = () => {};
  const timers = new Map(); let timerId = 0;
  const module = { exports: {} }, ports = {
    './dom': { el: make, button(label, _icon, className, onClick) { const node = make('button', `sw-button ${className || ''}`); node.append(make('span', '', label)); if (onClick) node.addEventListener('click', onClick); return node; } },
    './icons': { icon: () => make('svg') },
  };
  class ClockDate extends Date { static now() { return wallTime; } }
  vm.runInNewContext(compiled, { module, exports: module.exports, require: name => ports[name] ?? {}, document,
    HTMLElement: NodePort, HTMLButtonElement: NodePort, Date: ClockDate, performance: { now: () => monotonic },
    setTimeout(callback, delay) { const id = ++timerId; timers.set(id, { callback, delay }); return id; },
    clearTimeout(id) { timers.delete(id); }, CSS: { escape: value => value }, console });
  const handle = module.exports.mountOAuthConsent(host, {
    account: async () => { calls.account++; return accountPort(); },
    observeAccount(value) { listener = value; return () => { listener = null; }; },
    context: async () => { calls.context++; return contextPort(); },
    decide: async (kind, args) => { calls.decisions.push({ kind, args: structuredClone(args) }); await decidePort(kind, args); },
    complete: expectedAccountId => { calls.complete++; calls.completionAccounts.push(expectedAccountId); return completionPort(); },
    openAccount: async () => { calls.openAccount++; },
  });
  t.after(() => handle.dispose());
  return { host, document, outside, handle, proposal, calls, timers,
    button(text) { return host.querySelectorAll('button').find(node => node.textContent.includes(text)); },
    setDecision(port) { decidePort = port; }, setContext(port) { contextPort = port; }, setAccountRead(port) { accountPort = port; },
    setCompletion(port) { completionPort = port; },
    setAccount(id, notify = true) { account = { accountId: id, label: id === 'account_A' ? 'Synthetic A' : 'Synthetic B' }; if (notify) listener?.(); },
    setClocks(wall, mono) { wallTime = wall; monotonic = mono; },
  };
}

test('pending decision/error keeps a connected focus target and the opened disclosure', async t => {
  const f = fixture(t), pending = deferred(); f.setDecision(() => pending.promise); await turn();
  const details = f.host.querySelector('details'); details.open = true;
  const allow = f.button('Разрешить'); allow.focus(); allow.click(); await turn();
  assert.ok(f.host.contains(f.document.activeElement) && f.document.activeElement.isConnected,
    'the focused decision control was detached before the reply');
  assert.equal(f.host.querySelector('details')?.open, true, 'the already opened safety explanation remains open');
  pending.reject(new Error('synthetic network failure')); await turn();
  assert.ok(f.host.contains(f.document.activeElement) && f.document.activeElement.isConnected);
  assert.equal(f.calls.decisions.length, 1); assert.equal(f.calls.complete, 0);
});

test('an account switch followed by reread cannot complete A consent while B is current', async t => {
  const f = fixture(t), pending = deferred(); f.setDecision(() => pending.promise); await turn();
  f.button('Разрешить').click(); await turn();
  assert.equal(f.calls.decisions[0].args.expectedAccountId, 'account_A');
  f.setAccount('account_B');
  f.proposal.decision = 'approved'; f.proposal.decidedAccountId = 'account_A';
  pending.resolve(); await turn(); assert.equal(f.calls.complete, 0);
  f.button('Проверить запрос').click(); await turn();
  f.button('Вернуться в клиент')?.click(); await turn();
  assert.equal(f.calls.complete, 0, 'approved A context must not silently become B completion authority');
});

test('a previously approved A context loaded directly under B cannot complete it', async t => {
  const f = fixture(t, { decision: 'approved', decidedAccountId: 'account_A', accountId: 'account_B' }); await turn();
  f.button('Вернуться в клиент')?.click(); await turn();
  assert.equal(f.calls.complete, 0);
});

test('browser wall clock ahead does not expire a fresh server-checked proposal', async t => {
  const f = fixture(t, { skew: 3600000 }); await turn();
  const allow = f.button('Разрешить');
  assert.ok(allow && !allow.disabled, 'fresh checkedAt/expiresAt must not be compared to skewed browser wall time');
  assert.equal(f.calls.decisions.length, 0);
});

test('late rejection does not move focus from a different element chosen by the user', async t => {
  const f = fixture(t), pending = deferred(); f.setDecision(() => pending.promise); await turn();
  f.button('Разрешить').focus(); f.button('Разрешить').click(); await turn();
  f.outside.focus(); pending.reject(new Error('synthetic network failure')); await turn();
  assert.equal(f.document.activeElement, f.outside); assert.equal(f.calls.complete, 0);
});

test('dispose and observed account change fence late pending responses without another decision', async t => {
  const f = fixture(t), response = deferred(); await turn();
  f.setContext(() => response.promise); f.setAccount('account_B'); f.button('Проверить запрос').click();
  await turn(); f.handle.dispose(); response.resolve({ ...f.proposal }); await turn();
  assert.equal(f.host.children.length, 0); assert.equal(f.calls.decisions.length, 0); assert.equal(f.calls.complete, 0);
});

test('complete rechecks the account while its asynchronous local read is pending', async t => {
  const f = fixture(t, { decision: 'approved', decidedAccountId: 'account_A' }), response = deferred(); await turn();
  f.setAccountRead(() => response.promise); f.button('Вернуться в клиент').click(); await turn();
  // The identity observer has not run yet. The fresh read itself must still
  // prevent native completion with the old proposal's account.
  f.setAccount('account_B', false); response.resolve({ accountId: 'account_B', label: 'Synthetic B' }); await turn();
  assert.equal(f.calls.complete, 0); assert.equal(f.calls.decisions.length, 0);
});

test('observed A to B to A invalidates a held completion even when its old snapshot is A', async t => {
  const f = fixture(t, { decision: 'approved', decidedAccountId: 'account_A' }), response = deferred(); await turn();
  f.setAccountRead(() => response.promise); f.button('Вернуться в клиент').click(); await turn();
  f.setAccount('account_B'); f.setAccount('account_A');
  response.resolve({ accountId: 'account_A', label: 'Synthetic A' }); await turn();
  assert.equal(f.calls.complete, 0); assert.equal(f.calls.decisions.length, 0);
});

test('busy controls remain focusable but repeated decisions and completion are inert', async t => {
  const f = fixture(t), decision = deferred(), completion = deferred(); f.setDecision(() => decision.promise); await turn();
  f.button('Разрешить').focus(); f.button('Разрешить').click(); await turn();
  const approve = f.button('Подтверждаем'), deny = f.button('Отказать');
  assert.equal(approve.disabled, false); assert.equal(approve.getAttribute('aria-disabled'), 'true');
  assert.equal(deny.getAttribute('aria-disabled'), 'true');
  approve.click(); deny.click(); await turn(); assert.equal(f.calls.decisions.length, 1);
  // Account reads after the accepted decision are controlled without using a
  // timer to manufacture a completion ordering.
  f.setAccountRead(() => completion.promise); decision.resolve(); await turn();
  completion.resolve({ accountId: 'account_A', label: 'Synthetic A' }); await turn();
  assert.equal(f.calls.complete, 1); assert.deepEqual(f.calls.completionAccounts, ['account_A']);
  f.button('Вернуться в клиент').click(); await turn();
  assert.equal(f.calls.complete, 1, 'navigation in progress cannot submit a second completion');
});

test('lost decision ACK exposes readback, then approved A can complete without a second decision', async t => {
  const f = fixture(t); f.setDecision(async () => {
    f.proposal.decision = 'approved'; f.proposal.decidedAccountId = 'account_A';
    throw new Error('synthetic reply lost after acceptance');
  }); await turn();
  f.button('Разрешить').click(); await turn();
  assert.equal(f.calls.decisions.length, 1); assert.equal(f.calls.complete, 0);
  assert.equal(f.button('Разрешить'), undefined); assert.equal(f.button('Отказать'), undefined);
  const retry = f.button('Проверить запрос'); assert.ok(retry, 'unknown outcome needs an explicit current-state read');
  retry.click(); await turn(); assert.equal(f.calls.decisions.length, 1);
  f.button('Вернуться в клиент').click(); await turn();
  assert.equal(f.calls.complete, 1); assert.deepEqual(f.calls.completionAccounts, ['account_A']);
});

test('unknown navigation requires readback before a manual completion and never approves or posts automatically', async t => {
  const f = fixture(t, { decision: 'approved', decidedAccountId: 'account_A' }), navigation = deferred();
  f.setCompletion(() => navigation.promise); await turn();
  f.button('Вернуться в клиент').click(); await turn();
  assert.equal(f.calls.complete, 1); assert.equal(f.calls.decisions.length, 0);
  f.button('Вернуться в клиент').click(); await turn(); assert.equal(f.calls.complete, 1);
  navigation.reject(new Error('synthetic navigation timeout')); await turn();
  assert.equal(f.button('Вернуться в клиент'), undefined);
  assert.ok(f.host.textContent.includes('Возврат в клиент не подтверждён'));
  f.button('Проверить запрос').click(); await turn();
  assert.equal(f.calls.context, 2); assert.equal(f.calls.complete, 1); assert.equal(f.calls.decisions.length, 0);
  assert.ok(f.button('Вернуться в клиент'));
});

for (const outcome of ['account-switch', 'dispose']) test(`late navigation rejection after ${outcome} cannot repaint or resubmit`, async t => {
  const f = fixture(t, { decision: 'approved', decidedAccountId: 'account_A' }), navigation = deferred();
  f.setCompletion(() => navigation.promise); await turn();
  f.button('Вернуться в клиент').click(); await turn(); assert.equal(f.calls.complete, 1);
  if (outcome === 'dispose') f.handle.dispose();
  else { f.setAccount('account_B'); f.setAccount('account_A'); }
  f.outside.focus(); const text = f.host.textContent;
  navigation.reject(new Error('late completion timeout')); await turn();
  assert.equal(f.host.textContent, text); assert.equal(f.document.activeElement, f.outside);
  assert.equal(f.calls.complete, 1); assert.equal(f.calls.decisions.length, 0);
});

test('deny completes only the explicit deny decision for its captured account', async t => {
  const f = fixture(t); await turn(); f.button('Отказать').click(); await turn();
  assert.equal(f.calls.decisions.length, 1); assert.equal(f.calls.decisions[0].kind, 'deny');
  assert.equal(f.calls.decisions[0].args.expectedAccountId, 'account_A');
  assert.equal(f.calls.complete, 1); assert.deepEqual(f.calls.completionAccounts, ['account_A']);
});

test('negative wall-clock skew cannot keep a proposal writable past monotonic expiry', async t => {
  const f = fixture(t, { skew: -3600000 }); await turn();
  const allow = f.button('Разрешить'); assert.ok(allow);
  f.setClocks(serverTime - 3600000, 60200); allow.click(); await turn();
  assert.equal(f.calls.decisions.length, 0); assert.equal(f.calls.complete, 0);
  assert.equal(f.button('Разрешить'), undefined); assert.ok(f.button('Проверить запрос'));
});

test('a context response delayed longer than its server TTL is not fresh on arrival', async t => {
  const f = fixture(t), response = deferred(); await turn();
  f.setContext(() => response.promise); f.setAccount('account_B'); f.button('Проверить запрос').click(); await turn();
  f.setClocks(serverTime - 3600000, 70100); response.resolve({ ...f.proposal }); await turn();
  assert.equal(f.button('Разрешить'), undefined); assert.equal(f.calls.decisions.length, 0); assert.equal(f.calls.complete, 0);
});

test('an opened disclosure survives an explicit account-panel readback', async t => {
  const f = fixture(t); await turn(); f.host.querySelector('details').open = true;
  const account = f.button('Synthetic A'); account.focus(); account.click(); await turn();
  assert.equal(f.calls.openAccount, 1); assert.equal(f.calls.context, 2);
  assert.equal(f.host.querySelector('details')?.open, true, 'loading must not silently fold an explanation the person opened');
  assert.ok(f.document.activeElement.isConnected && f.host.contains(f.document.activeElement));
  assert.equal(f.calls.decisions.length, 0);
});
