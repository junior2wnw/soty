import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import * as media from './feedback-media.mjs';
import { coreFeedbackHttpFixture } from '../../server/test/support/core-feedback-http.mjs';

// Actual panel + World guard methods + signed SDK + disposable HTTP/SQLite.
// Only DOM, browser storage and Web Locks are controlled ports; no visual or
// microphone acceptance is claimed by these deterministic controller tests.
const compile = source => ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const [panelSource, worldSource] = await Promise.all(['./app-feedback.ts', './app.ts'].map(file => readFile(new URL(file, import.meta.url), 'utf8')));
const panelCode = compile(panelSource), worldCode = compile(worldSource);
const ast = ts.createSourceFile('actual-world.ts', worldSource, ts.ScriptTarget.Latest, true); let updateCallback;
const findUpdateGuard = node => { if (ts.isCallExpression(node) && node.expression.getText(ast) === 'registerUpdateGuard') updateCallback = node.arguments[0]; ts.forEachChild(node, findUpdateGuard); };
findUpdateGuard(ast); assert.ok(updateCallback);
const updateCode = compile(`exports.bind = function() { return ${updateCallback.getText(ast)}; };`);
const pwaSource = await readFile(new URL('../platform/pwa.ts', import.meta.url), 'utf8');
const pwaAst = ts.createSourceFile('actual-pwa.ts', pwaSource, ts.ScriptTarget.Latest, true);
const watcher = pwaAst.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'watchFormEdits');
const watcherCode = compile(`exports.watch = ${watcher.getText(pwaAst).replace(/^export\s+/u, '')};`);
const turn = () => new Promise(resolve => setImmediate(resolve));
async function until(check) { const end = Date.now() + 5000; while (!check()) { assert.ok(Date.now() < end, 'controlled operation settles'); await turn(); } }
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };

function ui(api, accountId, storage = new Map()) {
  let writable = true;
  const document = { activeElement: null };
  class Node {
    children = []; attributes = new Map(); dataset = {}; handlers = new Map(); parent = null; open = false; disabled = false; hidden = false; value = ''; ownText = '';
    classList = { add() {}, remove() {}, toggle() {} };
    constructor(tag, className = '', text = '') { this.tagName = tag; this.className = className; this.ownText = text; }
    set textContent(value) { this.ownText = value; this.children = []; }
    get textContent() { return this.ownText + this.children.map(child => child.textContent).join(''); }
    get childElementCount() { return this.children.length; }
    get isConnected() { return this === document.body || Boolean(this.parent?.isConnected); }
    setAttribute(name, value) { this.attributes.set(name, value); }
    removeAttribute(name) { this.attributes.delete(name); }
    addEventListener(type, fn) { const handlers = this.handlers.get(type) ?? []; handlers.push(fn); this.handlers.set(type, handlers); }
    emit(type, target = this) { for (const fn of this.handlers.get(type) ?? []) fn({ target, preventDefault() {}, isTrusted: false }); if (['focusin', 'beforeinput', 'input'].includes(type)) this.parent?.emit(type, target); }
    append(...children) { for (const child of children) { child.parent = this; this.children.push(child); } }
    replaceChildren(...children) { this.children = []; this.append(...children); }
    remove() { if (this.parent) this.parent.children = this.parent.children.filter(child => child !== this); this.parent = null; }
    matches(selector) {
      if (selector.startsWith('.')) return this.className.split(' ').includes(selector.slice(1));
      const data = /^\[data-(.+)\]$/.exec(selector); if (data) return Object.hasOwn(this.dataset, data[1].replace(/-([a-z])/gu, (_, letter) => letter.toUpperCase()));
      return selector === this.tagName;
    }
    closest(selector) { return this.matches(selector) ? this : this.parent?.closest(selector) ?? null; }
    querySelectorAll(selector) { return this.children.flatMap(child => [...(child.matches(selector) ? [child] : []), ...child.querySelectorAll(selector)]); }
    querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null; }
    focus() { document.activeElement = this; this.emit('focusin'); }
    showModal() { this.open = true; }
    close() { if (!this.open) return; this.open = false; this.emit('close'); }
    click() { if (!this.disabled) this.emit('click'); }
  }
  class Input extends Node {} class Textarea extends Node {}
  const el = (tag, ...args) => tag === 'input' ? new Input(tag, ...args) : tag === 'textarea' ? new Textarea(tag, ...args) : new Node(tag, ...args);
  const dom = { el, labeledField(label, control) { const node = el('label', '', label); node.append(control); return node; },
    button(label, _icon, style = '', run) { const node = el('button', style); node.append(el('span', '', label)); if (run) node.addEventListener('click', run); return node; } };
  const localStorage = { getItem: key => storage.get(key) ?? null, setItem(key, value) { if (!writable) throw Error('fixture_storage_denied'); storage.set(key, value); }, removeItem(key) { if (!writable) throw Error('fixture_storage_denied'); storage.delete(key); } };
  let locked = Promise.resolve();
  const globals = { URL, AbortController, crypto, DOMException, document, localStorage, HTMLInputElement: Input, HTMLTextAreaElement: Textarea,
    navigator: { locks: { request(_key, run) { const result = locked.then(run); locked = result.catch(() => {}); return result; } } } };
  function evaluate(code, ports) { const module = { exports: {} }; vm.runInNewContext(code, { ...globals, module, exports: module.exports, require: name => ports[name] ?? {} }); return module.exports; }
  const panel = evaluate(panelCode, { './dom': dom, './feedback-media.mjs': media });
  const { World } = evaluate(worldCode + '\nexports.World = WorldApplication;', { './dom': dom, './app-feedback': panel });
  const app = Object.create(World.prototype), root = el('main');
  document.body = root;
  document.querySelector = selector => { assert.equal(selector, 'dialog[open] form'); return root.querySelector('.sf-dialog')?.open ? root.querySelector('form') : null; };
  Object.assign(app, { root, main: root, live: el('div'), api, deskAccount: accountId, accountGeneration: 1, screenSequence: 1, destroyed: false,
    noteActionPending: false, mountFieldCovers() {}, renderNavigation() {}, dialogs: new Set(), toast(value) { this.warning = value; },
    options: {}, avatars: { setContext() {} }, homeStatus: {}, routeLoaded: true, view: 'mine',
    chatDrafts: { hasVolatile: () => false }, formEdits: evaluate(watcherCode, {}).watch(root) });
  return { app, root, storage, setWritable(value) { writable = value; }, panel, updateGuard: evaluate(updateCode, {}).bind.call(app),
    open() { app.openCoreFeedback(); },
    text(value) { const node = root.querySelector('textarea'); node.focus(); node.emit('beforeinput'); node.value = value; node.emit('input'); },
    submit() { root.querySelector('form').emit('submit'); },
    button(label) { return root.querySelectorAll('button').find(node => node.textContent === label); },
    get draft() { return JSON.parse(storage.get(`soty.core-feedback.v1:${encodeURIComponent(accountId)}`) ?? 'null'); } };
}

test('actual core panel sends only core RPC, restores its private draft and reads the signed saved ticket', async t => {
  const f = await coreFeedbackHttpFixture(t); await f.restart(); const calls = [];
  const api = { request(method, args = {}) { calls.push({ method, args: JSON.parse(JSON.stringify(args)) }); return f.reporter.client.extension(method, JSON.parse(JSON.stringify(args)), { expectedAccountId: f.reporter.account.accountId }); } };
  const u = ui(api, f.reporter.account.accountId); u.open();
  await until(() => u.root.querySelector('.sf-audience').textContent.includes('Поддержка Сот'));
  u.text('Saved private core draft'); await u.app.flushScreen();
  assert.equal(u.app.screenHasUnsavedChanges(), false); assert.equal(u.draft.body, 'Saved private core draft');
  assert.equal([...u.storage.keys()].some(key => key.startsWith('soty.feedback.v1:')), false);
  u.app.cleanScreen(); u.open(); await until(() => !u.button('Отправить').disabled);
  assert.equal(u.root.querySelector('textarea').value, 'Saved private core draft');
  u.submit(); await until(() => u.root.querySelector('.sf-notice').textContent.includes('Принято сервером'));
  assert.equal(u.draft.body, ''); assert.equal(u.draft.pending, null);
  u.button('История').click(); await until(() => u.root.querySelectorAll('[data-feedback-ticket-id]').length === 1);
  u.root.querySelector('[data-feedback-ticket-id]').click(); await until(() => u.root.querySelector('.sf-ticket-body'));
  assert.equal(u.root.querySelector('.sf-ticket-body').textContent, 'Saved private core draft');
  assert.ok(calls.every(call => call.method.startsWith('core.feedback.') && !Object.hasOwn(call.args, 'appId')));
  u.app.cleanScreen();
});

test('volatile core draft participates in actual navigation/flush guard and cannot close until saved', async t => {
  const f = await coreFeedbackHttpFixture(t); await f.restart();
  const u = ui({ request: (method, args = {}) => f.reporter.client.extension(method, JSON.parse(JSON.stringify(args))) }, f.reporter.account.accountId);
  u.open(); await until(() => !u.button('Отправить').disabled);
  u.setWritable(false); u.text('Keep this volatile text'); let navigations = 0;
  assert.equal(u.app.screenHasUnsavedChanges(), true);
  assert.equal(await u.updateGuard(), false);
  const openPanel = u.root.querySelector('.sf-dialog'); await u.app.refresh();
  assert.equal(u.root.querySelector('.sf-dialog'), openPanel, 'same-account metadata refresh preserves the owned support dialog');
  assert.equal(u.root.querySelector('textarea').value, 'Keep this volatile text');
  u.app.afterNoteSaved(() => navigations++); await until(() => !u.app.noteActionPending);
  assert.equal(navigations, 0); assert.equal(u.root.querySelector('textarea').value, 'Keep this volatile text');
  u.button('Закрыть').click(); assert.equal(u.app.coreFeedback.isOpen(), true);
  u.setWritable(true); u.app.afterNoteSaved(() => navigations++); await until(() => navigations === 1);
  assert.equal(u.draft.body, 'Keep this volatile text'); assert.equal(u.app.screenHasUnsavedChanges(), false);
  assert.equal(await u.updateGuard(), false, 'an open form still vetoes an update after its draft becomes durable');
  u.button('Закрыть').click(); assert.equal(u.app.coreFeedback.isOpen(), false);
  assert.equal(await u.updateGuard(), true);
  u.app.cleanScreen();
});

test('configured support account uses the same actual panel to reply and request acceptance from the original reporter', async t => {
  const f = await coreFeedbackHttpFixture(t); await f.restart();
  const { context } = await f.reporter.client.extension('core.feedback.context');
  const original = await f.reporter.client.extension('core.feedback.submit', { installationId: context.installationId, requestId: 'owner-ui-fixture', body: 'Reporter needs help', attachments: [] });
  const api = who => ({ request: (method, args = {}) => who.client.extension(method, JSON.parse(JSON.stringify(args)), { expectedAccountId: who.account.accountId }) });
  const owner = ui(api(f.owner), f.owner.account.accountId); owner.open(); await until(() => !owner.button('Отправить').disabled);
  owner.button('История').click(); await until(() => owner.root.querySelector('[data-feedback-ticket-id]'));
  assert.ok(owner.root.textContent.includes('Обращения в поддержку Сот'));
  owner.root.querySelector('[data-feedback-ticket-id]').click(); await until(() => owner.button('Ответить'));
  const answer = owner.root.querySelector('.sf-ticket').querySelector('textarea'); answer.value = 'Actual signed UI support answer'; answer.emit('input'); owner.button('Ответить').click();
  await until(() => owner.root.querySelector('.sf-notice').textContent.includes('Ответ принят сервером'));
  owner.button('Передать на проверку').click(); await until(() => owner.root.querySelector('.sf-ticket-status').textContent === 'Готово для проверки');
  const reporter = ui(api(f.reporter), f.reporter.account.accountId); reporter.open(); await until(() => !reporter.button('Отправить').disabled);
  reporter.button('История').click(); await until(() => reporter.root.querySelector('[data-feedback-ticket-id]'));
  reporter.root.querySelector('[data-feedback-ticket-id]').click(); await until(() => reporter.button('Проблема решена'));
  assert.ok(reporter.root.textContent.includes('Actual signed UI support answer'));
  reporter.button('Проблема решена').click(); await until(() => reporter.root.querySelector('.sf-ticket-status').textContent === 'Решено');
  assert.equal((await f.reporter.client.extension('core.feedback.get', { installationId: context.installationId, ticketId: original.receipt.ticketId })).ticket.status, 'resolved');
  owner.app.cleanScreen(); reporter.app.cleanScreen();
});

test('late signed submit ACK cannot clear the retained draft after actual dispose and account ABA; reopen reconciles the same intent', async t => {
  const f = await coreFeedbackHttpFixture(t); await f.restart(); const held = deferred(); let hold = true, committed = false; const intents = [];
  const api = { async request(method, args = {}) {
    const captured = JSON.parse(JSON.stringify(args));
    const result = await f.reporter.client.extension(method, captured, { expectedAccountId: f.reporter.account.accountId });
    if (method === 'core.feedback.submit') { intents.push(captured); committed = true; if (hold) { hold = false; await held.promise; } }
    return result;
  } };
  const u = ui(api, f.reporter.account.accountId); u.open(); await until(() => !u.button('Отправить').disabled);
  u.text('Reconcile only my original intent'); u.submit(); await until(() => committed);
  const pending = u.draft.pending; assert.ok(pending);
  u.app.deskAccount = f.foreign.account.accountId; u.app.accountGeneration++; u.app.cleanScreen();
  u.app.deskAccount = f.reporter.account.accountId; u.app.accountGeneration++;
  held.resolve(); await turn(); await turn();
  assert.deepEqual(u.draft.pending, pending); assert.equal(u.draft.body, 'Reconcile only my original intent');
  assert.equal(u.root.querySelector('.sf-dialog'), null);
  u.open(); await until(() => !u.button('Проверить отправку').disabled); u.submit();
  await until(() => u.root.querySelector('.sf-notice').textContent.includes('Принято сервером'));
  assert.deepEqual(intents[1], intents[0]); assert.equal(u.draft.pending, null);
  const { context } = await f.reporter.client.extension('core.feedback.context');
  assert.equal((await f.reporter.client.extension('core.feedback.list', { installationId: context.installationId })).tickets.length, 1);
  u.app.cleanScreen();
});
