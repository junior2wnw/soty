import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';

const compile = async file => ts.transpileModule(await readFile(new URL(file, import.meta.url), 'utf8'), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
const controllerCode = await compile('./ai-account.ts'), uiCode = await compile('./ai-settings.ts'), appCode = await compile('./app.ts');
function load(code, ports = {}, globals = {}) {
  const module = { exports: {} };
  vm.runInNewContext(code, { module, exports: module.exports, require: name => ports[name] ?? {}, AbortController, crypto, structuredClone,
    Date, BigInt, Intl, setTimeout, clearTimeout, ...globals });
  return module.exports;
}
const accountModule = load(controllerCode);
const clone = value => JSON.parse(JSON.stringify(value));
const turn = () => new Promise(resolve => setImmediate(resolve));
async function settle() { await turn(); await turn(); }
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const failure = code => Object.assign(new Error(`unsafe upstream error with secret-forbidden ${code}`), { code });
const key = (id = 'key-1', overrides = {}) => ({ id, name: 'Мой проект', prefix: 'sk-soty-live-…1234', status: 'active',
  purpose: 'customer',
  createdAt: '2026-10-25T10:00:00.000Z', lastUsedAt: null, expiresAt: null, limits: { dailyMicrorubles: '1000000', rpm: 10, concurrency: 1 },
  allowedModels: null, spentMicrorubles: '12500', tokens: 27, policyRevision: 1, ...overrides });
const account = (id = 'account-A', overrides = {}) => ({ schema: 'soty.ai-account.v1', accountId: id, state: 'ready',
  defaultKey: { id: 'internal-never-visible', secret: 'default-secret-forbidden' },
  allowance: { kind: 'included', unit: 'microrubles', grantedMicrorubles: '50000000', availableMicrorubles: '48750000',
    spentMicrorubles: '1000000', heldMicrorubles: '100000', pendingMicrorubles: '150000', limits: { dailyMicrorubles: '5000000' } },
  usage: { requests: 2, tokens: 500, inputTokens: 300, outputTokens: 200 }, models: [{ id: 'glm-5', status: 'available' }],
  updatedAt: '2026-10-25T10:02:00.000Z', ...overrides });
function storageFixture() {
  const values = new Map(), queues = new Map(); let deny = false;
  const storage = { getItem: name => values.get(name) ?? null,
    setItem(name, value) { if (deny) throw Error('disk full'); values.set(name, value); }, removeItem: name => values.delete(name) };
  const locks = { request(name, action) { const previous = queues.get(name) ?? Promise.resolve(); const result = previous.catch(() => {}).then(action); queues.set(name, result.catch(() => {})); return result; } };
  return { values, storage, locks, denyWrites() { deny = true; } };
}
function serviceFixture() {
  const calls = [], keys = new Map(), receipts = new Map(); let effects = 0, lose = false, accountError = null;
  const api = { async request(op, args) {
    calls.push({ op, args: clone(args) });
    if (op === 'ai.account.get') { if (accountError) throw failure(accountError); return account(args.expectedAccountId); }
    if (op === 'ai.keys.list') return { keys: [...keys.values()].map(clone) };
    const prior = receipts.get(args.operationId);
    if (prior) { assert.equal(JSON.stringify({ op, args }), prior.body); return { ...clone(prior.value), secret: null, replayed: true }; }
    const target = op === 'ai.keys.create' ? key(`key-${effects}`, { name: args.name }) : clone(keys.get(args.keyId));
    if (op === 'ai.keys.update' && args.expectedPolicyRevision !== undefined && args.expectedPolicyRevision !== target.policyRevision) throw failure('ai_key_changed');
    effects++;
    if (op === 'ai.keys.create') target.id = `key-${effects}`;
    if (op === 'ai.keys.update') { for (const field of ['name', 'expiresAt', 'allowedModels', 'status']) if (args[field] !== undefined) target[field] = clone(args[field]); if (args.limits) target.limits = { ...target.limits, ...clone(args.limits) }; target.policyRevision++; }
    if (op === 'ai.keys.revoke') target.status = 'revoked';
    if (op === 'ai.keys.rotate') target.prefix = 'sk-soty-live-…5678';
    keys.set(target.id, target);
    const value = { key: target, ...(['ai.keys.create', 'ai.keys.rotate'].includes(op) ? { secret: 'one-time-owner-secret' } : {}) };
    receipts.set(args.operationId, { body: JSON.stringify({ op, args }), value: { key: clone(target) } });
    if (lose) { lose = false; throw failure('ai_operation_unknown'); }
    return clone(value);
  } };
  return { api, calls, keys, loseNext() { lose = true; }, effects: () => effects, unavailable() { accountError = 'ai_unavailable'; } };
}
function mounted({ service = serviceFixture(), disk = storageFixture(), accountId = 'account-A', isCurrent = () => true } = {}) {
  let changes = 0, ids = 0;
  const controller = accountModule.createAiAccountController({ accountId, api: service.api, isCurrent, storage: disk.storage, locks: disk.locks,
    randomId: () => `operation-${++ids}`, onChange: () => changes++ });
  return { controller, service, disk, changes: () => changes };
}

test('money input stays exact and included allowance is copied without private/default fields', () => {
  assert.equal(accountModule.aiRublesToMicrorubles('001,000001'), '1000001');
  assert.equal(accountModule.aiRublesToMicrorubles('999999999999.999999'), '999999999999999999');
  assert.equal(accountModule.aiRublesToMicrorubles(''), null);
  for (const invalid of ['NaN', '-1', '1e6', '1.0000001', '1 000', 'Infinity']) assert.throws(() => accountModule.aiRublesToMicrorubles(invalid));
  assert.equal(accountModule.aiRublesInput('1000001'), '1.000001');
  assert.equal(accountModule.aiMoneyLabel('1'), '< 0,01 ₽');
  const safe = accountModule.readAiAccount(account(), 'account-A');
  assert.equal(safe.allowance.pendingMicrorubles, '150000');
  assert.equal(JSON.stringify(safe).includes('secret'), false);
  assert.equal('defaultKey' in safe, false);
  assert.throws(() => accountModule.readAiAccount(account('account-B'), 'account-A'));
  assert.throws(() => accountModule.readAiAccount(account('account-A', { usage: { requests: -1 } }), 'account-A'));
});

test('opening ordinary settings only reads account status and never issues a key', async () => {
  const f = mounted(); await f.controller.start();
  assert.deepEqual(f.service.calls.map(call => call.op), ['ai.account.get']);
  assert.equal(f.controller.read().account.state, 'ready');
  assert.equal(f.controller.read().keys, null);
  assert.equal(f.service.effects(), 0);
  f.controller.dispose();
});

test('lost create response is read back and replayed with the exact operation, without a second effect or secret replay', async () => {
  const f = mounted(); f.service.loseNext();
  await f.controller.mutate('create', { name: 'Рабочий проект', limits: { dailyMicrorubles: '1000000' }, expiresAt: null, allowedModels: null });
  const before = f.service.calls[0];
  assert.equal(f.service.effects(), 1);
  assert.equal(f.controller.read().pending.operationId, before.args.operationId);
  assert.equal(f.controller.read().keys[0].name, 'Рабочий проект');
  assert.deepEqual(f.service.calls.map(call => call.op), ['ai.keys.create', 'ai.keys.list']);
  assert.equal(f.disk.values.size, 1);
  assert.equal([...f.disk.values.values()].join('').includes('secret'), false);
  await f.controller.reconcile(); await settle();
  const creates = f.service.calls.filter(call => call.op === 'ai.keys.create');
  assert.deepEqual(creates[1], creates[0]);
  assert.equal(f.service.effects(), 1);
  assert.equal(f.controller.read().secret, null);
  assert.equal(f.controller.read().pending, null);
  assert.equal(f.disk.values.size, 0);
  assert.match(f.controller.read().notice, /Полное значение ключа уже недоступно/u);
  f.controller.dispose();
});

test('a closed mount aborts its wait and never adopts a late key result; reopening recovers metadata only', async () => {
  const disk = storageFixture(), service = serviceFixture(), reply = deferred(), entered = deferred();
  const underlying = service.api.request;
  service.api.request = async (op, args) => { const value = await underlying(op, args); if (op === 'ai.keys.create' && !value.replayed) { entered.resolve(); await reply.promise; } return value; };
  const first = mounted({ disk, service }); const pending = first.controller.mutate('create', { name: 'Закрытый запрос' });
  await entered.promise; const changes = first.changes(); first.controller.dispose(); await pending;
  assert.equal(first.controller.read().secret, null);
  const second = mounted({ disk, service }); await second.controller.start();
  reply.resolve(); await settle();
  assert.equal(first.changes(), changes);
  assert.equal(second.controller.read().secret, null);
  assert.equal(second.controller.read().keys.length, 1);
  assert.equal(service.effects(), 1);
  assert.equal(disk.values.size, 0);
  second.controller.dispose();
});

test('a historical mutation receipt cannot undo the currently revoked key projection', async () => {
  const f = mounted(); f.service.loseNext(); await f.controller.mutate('create', { name: 'История' });
  f.service.keys.get('key-1').status = 'revoked';
  await f.controller.reconcile(); await settle();
  assert.equal(f.controller.read().keys[0].status, 'revoked');
  assert.equal(f.controller.read().secret, null); assert.equal(f.service.effects(), 1);
  f.controller.dispose();
});

test('A → B → A cannot resurrect an older account reply or clipboard completion', async () => {
  const response = deferred(); let generation = 1;
  const f = mounted({ service: { api: { request: () => response.promise } }, isCurrent: () => generation === 1 });
  const reading = f.controller.loadAccount(); generation = 2; generation = 3;
  response.resolve(account()); await reading;
  assert.equal(f.controller.read().account, null);
  const fresh = mounted(); await fresh.controller.mutate('create', { name: 'Ключ' });
  assert.equal(fresh.controller.read().secret, 'one-time-owner-secret');
  const clipboard = deferred(); let writes = 0;
  const copying = fresh.controller.copySecret(value => { writes++; assert.equal(value, 'one-time-owner-secret'); return clipboard.promise; });
  const changes = fresh.changes(); fresh.controller.dispose(); clipboard.resolve(); await copying;
  await fresh.controller.copySecret(async () => writes++);
  assert.equal(writes, 1); assert.equal(fresh.changes(), changes); assert.equal(fresh.controller.read().secret, null);
  assert.equal([...fresh.disk.values.values()].join('').includes('one-time'), false);
  f.controller.dispose();
});

test('pending operations stay in their own account and a changed request cannot overwrite an unknown effect', async () => {
  const disk = storageFixture(), a = mounted({ disk }); a.service.loseNext();
  await a.controller.mutate('create', { name: 'Исходный' });
  const saved = [...disk.values.values()][0];
  await a.controller.mutate('create', { name: 'Подменённый' });
  assert.equal([...disk.values.values()][0], saved);
  assert.equal(a.service.effects(), 1);
  const b = mounted({ disk, accountId: 'account-B' }); await b.controller.start();
  assert.equal(b.controller.read().pending, null); assert.equal(b.controller.read().account.accountId, 'account-B');
  assert.ok(b.service.calls.every(call => call.args.expectedAccountId === 'account-B'));
  a.controller.dispose(); b.controller.dispose();
});

test('two mounted tabs cannot overwrite a durable mutation; storage failure prevents dispatch but leaves status usable', async () => {
  const disk = storageFixture(), service = serviceFixture(); service.loseNext();
  const a = mounted({ disk, service }), b = mounted({ disk, service });
  await Promise.all([a.controller.mutate('create', { name: 'Первое окно' }), b.controller.mutate('create', { name: 'Второе окно' })]);
  assert.equal(service.effects(), 1); assert.equal(service.calls.filter(call => call.op === 'ai.keys.create').length, 1);
  a.controller.dispose(); b.controller.dispose();
  const denied = mounted(); denied.disk.denyWrites();
  await denied.controller.mutate('create', { name: 'Не отправлять' }); await denied.controller.loadAccount();
  assert.equal(denied.service.effects(), 0); assert.equal(denied.controller.read().account.state, 'ready');
  denied.controller.dispose();
});

test('stored requests have a bounded TTL and secret-bearing or altered payloads never replay', async () => {
  for (const extra of [{ createdAt: Date.now() - 25 * 60 * 60 * 1000 }, { args: { name: 'Подмена', secret: 'forbidden' } }]) {
    const disk = storageFixture(); disk.values.set('soty.ai.pending.v1:account-A', JSON.stringify({ schema: 1, accountId: 'account-A', operationId: 'operation-old', kind: 'create', args: { name: 'Старый' }, createdAt: Date.now(), ...extra }));
    const f = mounted({ disk }); await f.controller.start();
    assert.equal(f.service.effects(), 0); assert.equal(f.service.calls.filter(call => call.op.startsWith('ai.keys.')).length, 0);
    f.controller.dispose();
  }
});

test('unavailable and malformed responses stay honest and never display provider error text', async () => {
  const f = mounted(); f.service.unavailable(); await f.controller.start();
  assert.equal(f.controller.read().account, null); assert.match(f.controller.read().error, /временно недоступен/u);
  assert.equal(f.controller.read().error.includes('secret-forbidden'), false);
  assert.equal(accountModule.aiErrorLabel(failure('arbitrary-provider-code')).includes('secret-forbidden'), false);
  f.controller.dispose();
});

// Small DOM ports exercise the production mount and its event handlers. They do
// not claim browser layout/focus-trap acceptance, which has a separate UI gate.
function uiFixture(service = serviceFixture()) {
  const document = new EventTarget(); document.visibilityState = 'visible'; document.activeElement = null;
  class Node extends EventTarget {
    constructor(tag = 'div', className = '', text = '') { super(); this.tagName = tag; this.className = className; this.children = []; this.parentElement = null; this.isConnected = true;
      this.attributes = new Map(); this.dataset = {}; this.value = ''; this.disabled = false; this.checked = false; this.hidden = false; this.open = false; this._text = text;
      this.classList = { add: name => { this.className += ` ${name}`; } }; }
    get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
    set textContent(value) { this._text = String(value ?? ''); this.children = []; }
    append(...nodes) { for (const node of nodes) { node.parentElement = this; this.children.push(node); } }
    replaceChildren(...nodes) { for (const node of this.children) node.remove(); this._text = ''; this.children = []; this.append(...nodes); }
    remove() { this.isConnected = false; if (this.parentElement) this.parentElement.children = this.parentElement.children.filter(node => node !== this); for (const node of this.children) node.isConnected = false; }
    setAttribute(key, value) { this.attributes.set(key, String(value)); }
    getAttribute(key) { return this.attributes.get(key) ?? null; }
    querySelector(selector) { return this.children.find(child => child.tagName === selector) ?? this.children.map(child => child.querySelector(selector)).find(Boolean) ?? null; }
    focus() { document.activeElement = this; }
    reportValidity() { return true; }
    emit(type) { this.dispatchEvent(new Event(type, { cancelable: true })); }
  }
  const el = (tag, classes, text) => new Node(tag, classes, text);
  const button = (label, _icon, classes, action) => { const node = el('button', classes); node.append(el('span', '', label)); if (action) node.addEventListener('click', action); return node; };
  const ports = { './ai-account': accountModule, './dom': { el, button,
    labeledField: (label, input) => { const node = el('label', 'sw-field', label); node.append(input); return node; },
    textInput: (value = '') => Object.assign(el('input', 'sw-input'), { value }),
  } };
  const disk = storageFixture(), clipboard = [], timers = new Map(); let timerId = 0;
  const module = load(uiCode, ports, { document, localStorage: disk.storage, navigator: { locks: disk.locks, clipboard: { writeText: async text => clipboard.push(text) } },
    setTimeout(fn, delay) { const id = ++timerId; timers.set(id, { fn, delay }); return id; }, clearTimeout: id => timers.delete(id) });
  const host = el('div'), handle = module.mountAiSettings({ host, accountId: 'account-A', api: service.api, isCurrent: () => host.isConnected });
  const all = (node = host) => [node, ...node.children.flatMap(child => all(child))];
  const css = name => all().find(node => node.className?.split(' ').includes(name));
  const action = label => all().find(node => node.tagName === 'button' && node.textContent === label);
  return { host, handle, all, css, action, document, clipboard, disk, service, timers, ports, Node };
}

test('mounted UI is automatic, models/policies update safely, secrets copy only explicitly and disappear on close', async () => {
  const f = uiFixture(); await settle();
  assert.equal(f.css('sw-ai-state').textContent, 'ИИ готов');
  assert.match(f.host.textContent, /Включено 50 ₽.*до 5 ₽ в сутки/u);
  assert.equal(f.host.textContent.includes('default-secret'), false); assert.equal(f.clipboard.length, 0);
  f.css('sw-ai-advanced').open = true; f.css('sw-ai-advanced').emit('toggle'); await settle();
  f.action('Создать API-ключ').emit('click');
  const editor = f.css('sw-ai-editor'), controls = f.all(editor).filter(node => node.tagName === 'input');
  controls[0].value = 'Мобильный проект'; controls[1].value = '0,75';
  editor.emit('submit'); await settle();
  assert.equal(f.service.calls.find(call => call.op === 'ai.keys.create').args.limits.dailyMicrorubles, '750000');
  assert.equal(f.css('sw-ai-secret').hidden, false); assert.equal(f.clipboard.length, 0);
  assert.equal(f.document.activeElement.textContent, 'Сохраните ключ «Мобильный проект»');
  f.action('Скопировать ключ').emit('click'); await settle(); assert.deepEqual(f.clipboard, ['one-time-owner-secret']);
  assert.equal([...f.disk.values.values()].join('').includes('one-time-owner-secret'), false);
  f.handle.dispose();
  assert.equal(f.css('sw-ai-secret-value'), undefined); assert.equal(f.host.children.length, 0); assert.equal(f.timers.size, 0);
});

test('UI keeps focused edit nodes, preserves narrower limits, confirms rotation/revoke and uses safe labels', async () => {
  const service = serviceFixture(); service.keys.set('key-1', key()); const f = uiFixture(service); await settle();
  f.css('sw-ai-advanced').open = true; f.css('sw-ai-advanced').emit('toggle'); await settle();
  f.action('Изменить').emit('click');
  const editor = f.css('sw-ai-editor'), controls = f.all(editor).filter(node => node.tagName === 'input'), input = controls[0];
  input.value = 'Новое имя'; await settle(); assert.equal(f.document.activeElement, input);
  editor.emit('submit'); await settle();
  const update = service.calls.find(call => call.op === 'ai.keys.update');
  assert.equal(update.args.limits, undefined); assert.equal(update.args.status, undefined); assert.equal(update.args.allowedModels, undefined);
  assert.equal(service.keys.get('key-1').limits.rpm, 10); assert.equal(update.args.name, 'Новое имя');
  f.action('Заменить').emit('click'); assert.equal(service.calls.filter(call => call.op === 'ai.keys.rotate').length, 0);
  assert.match(f.css('sw-ai-confirm').textContent, /до 24 часов/u);
  f.action('Заменить ключ').emit('click'); await settle();
  assert.equal(service.calls.filter(call => call.op === 'ai.keys.rotate').length, 1);
  f.action('Готово, скрыть ключ').emit('click');
  f.action('Отозвать').emit('click'); f.action('Отозвать ключ').emit('click'); await settle();
  assert.equal(service.keys.get('key-1').status, 'revoked');
  assert.equal(f.action('Отозвать').hidden, true); f.handle.dispose();
});

test('visible-only status polling has bounded backoff and stops on disposal', async () => {
  const service = serviceFixture(); service.api.request = async (op, args) => { service.calls.push({ op, args }); return account(args.expectedAccountId, { state: 'provisioning' }); };
  const f = uiFixture(service); await settle(); assert.equal(f.timers.size, 1);
  for (const delay of [2000, 5000, 10000, 20000, 30000]) {
    const [id, timer] = [...f.timers][0]; assert.equal(timer.delay, delay); f.timers.delete(id); timer.fn(); await settle();
  }
  assert.equal(f.timers.size, 0);
  f.document.visibilityState = 'hidden'; f.document.dispatchEvent(new Event('visibilitychange')); await settle(); assert.equal(f.timers.size, 0);
  f.document.visibilityState = 'visible'; f.document.dispatchEvent(new Event('visibilitychange')); await settle(); assert.equal(f.timers.size, 1);
  f.handle.dispose(); assert.equal(f.timers.size, 0);
});

test('a stale policy save has no effect, retains the draft and reads the newer paused policy', async () => {
  const service = serviceFixture(); service.keys.set('key-1', key()); const f = uiFixture(service); await settle();
  f.css('sw-ai-advanced').open = true; f.css('sw-ai-advanced').emit('toggle'); await settle();
  f.action('Изменить').emit('click');
  const editor = f.css('sw-ai-editor'), name = f.all(editor).find(node => node.tagName === 'input'); name.value = 'Несохранённый ввод';
  service.keys.get('key-1').status = 'paused'; service.keys.get('key-1').policyRevision++;
  editor.emit('submit'); await settle();
  assert.equal(service.effects(), 0); assert.equal(name.value, 'Несохранённый ввод'); assert.equal(editor.hidden, false);
  assert.equal(service.keys.get('key-1').status, 'paused'); assert.equal(f.disk.values.size, 0);
  assert.match(f.css('sw-ai-error').textContent, /изменились в другом окне/u);
  assert.equal(service.calls.find(call => call.op === 'ai.keys.update').args.expectedPolicyRevision, 1);
  f.handle.dispose();
});

test('the actual lazy dialog rejects closed/ABA mounts and disposes the mounted panel synchronously', async () => {
  const chunk = deferred(), mounts = []; let loadCount = 0, disposed = 0;
  const module = load(`${appCode}\nexports.Controller=WorldApplication;`, { './dom': { el: (tag, className, text) => ({ tagName: tag, className, textContent: text }) }, './ai-settings': chunk.promise });
  // Capture dynamic require to count code loads without mounting a browser.
  const app = Object.create(module.Controller.prototype); app.deskAccount = 'account-A'; app.accountGeneration = 1; app.destroyed = false; app.api = {};
  const dialogs = [];
  app.dialog = (_title, close) => {
    const element = { open: true, classList: { add() {} } }, body = { append() {}, replaceChildren() {} };
    const dialog = { element, body, close() { if (!element.open) return; element.open = false; close(); } }; dialogs.push(dialog); return dialog;
  };
  const first = app.openAiSettings(); dialogs[0].close(); app.accountGeneration++;
  const second = app.openAiSettings(); app.accountGeneration++;
  chunk.resolve({ mountAiSettings(options) { loadCount++; mounts.push(options); return { dispose() { disposed++; } }; } });
  await Promise.all([first, second]); assert.equal(loadCount, 0);
  await app.openAiSettings(); assert.equal(mounts.length, 1);
  dialogs.at(-1).close(); assert.equal(disposed, 1); assert.equal(mounts[0].isCurrent(), false);
});
