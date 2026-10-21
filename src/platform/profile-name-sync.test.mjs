import * as connectionIntents from './connection-intent.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { ConnectError } from '../../modules/connect/browser/crypto.mjs';
import { validateState } from '../../modules/connect/browser/storage.mjs';
import { createConnectService } from '../../modules/connect/server/index.mjs';
import { createWorldService } from '../../modules/world/server/index.mjs';

// Actual form -> controller -> account-bound adapter -> browser signing -> both
// SQLite services. Only native DOM, IndexedDB and HTTP are controlled ports.
const compile = async path => ts.transpileModule(await readFile(new URL(path, import.meta.url), 'utf8'), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
const [appCode, adapterCode, dialogsCode, typesCode] = await Promise.all([
  compile('../world/app.ts'), compile('./world-adapter.ts'), compile('../world/dialogs.ts'), compile('../world/types.ts'),
]);
const projectId = 'profile-name-sync-test', origin = 'https://profile.test', endpoint = `${origin}/rpc`;
const scope = { projectId, endpoint };
const deferred = () => { let resolve; const promise = new Promise(yes => { resolve = yes; }); return { promise, resolve }; };
const turn = () => new Promise(resolve => setImmediate(resolve));
async function until(predicate) {
  const deadline = Date.now() + 5000;
  while (!predicate()) { assert.ok(Date.now() < deadline, 'controlled form operation completed'); await new Promise(resolve => setTimeout(resolve, 2)); }
}

function memoryStorage() {
  let value = null, afterSnapshot = null;
  const copy = () => value === null ? null : structuredClone(value);
  return {
    async read() { const result = copy(), hook = afterSnapshot; afterSnapshot = null; if (hook) await hook(); return result; },
    async claim(candidate) { if (value === null) value = structuredClone(candidate); return copy(); },
    async compareAndSwap(expected, candidate) {
      if (value?.localRevision !== expected) throw new ConnectError('LOCAL_CONFLICT', 'Concurrent synthetic change');
      validateState(candidate, scope); value = structuredClone(candidate); return copy();
    },
    inspect: copy,
    nextRead(hook) { afterSnapshot = hook; },
  };
}

class ElementPort {
  children = []; handlers = new Map(); attributes = new Map(); dataset = {}; style = { setProperty() {} };
  open = true; hidden = false; disabled = false; className = ''; tagName = ''; value = ''; textContent = '';
  classList = { add() {}, remove() {}, toggle() {} };
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  addEventListener(type, callback) { this.handlers.set(type, callback); }
  reportValidity() { return true; }
}
const element = (tagName, className = '', textContent = '') => Object.assign(new ElementPort(), { tagName, className, textContent });
const descendants = node => [node, ...node.children.filter(child => child instanceof ElementPort).flatMap(descendants)];
const dom = {
  el: element,
  button(label, _symbol, className, callback) { const node = element('button', className, label); if (callback) node.addEventListener('click', callback); return node; },
  textInput(value, placeholder) { return Object.assign(element('input'), { value, placeholder }); },
  labeledField(label, control) { const node = element('label', '', label); node.append(control); return node; },
  avatar() { return element('div'); },
};
const dialogsModule = { exports: {} }, typesModule = { exports: {} }, appModule = { exports: {} };
vm.runInNewContext(dialogsCode, { module: dialogsModule, exports: dialogsModule.exports, require: name => name === './dom' ? dom : {}, crypto: globalThis.crypto });
vm.runInNewContext(typesCode, { module: typesModule, exports: typesModule.exports, require: () => ({}) });
vm.runInNewContext(`${appCode}\nexports.TestController = WorldApplication;`, {
  module: appModule, exports: appModule.exports,
  require: name => name === './dom' ? dom : name === './dialogs' ? dialogsModule.exports : name === './types' ? typesModule.exports : {},
});

async function fixture(t) {
  const world = createWorldService({ databasePath: ':memory:', projectId });
  const connect = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin], extensions: [world] });
  const store = memoryStorage(), secondStore = memoryStorage(), clients = [], observers = new Set(), traffic = [], mounts = [];
  let afterResponse = null, renameFailure = false, loseRenameResponse = false;
  const publish = state => { for (const observer of [...observers]) observer(state); };
  const fetcher = async (_url, options) => {
    const body = JSON.parse(options.body); traffic.push(body.op);
    if (body.op === 'profile.rename' && renameFailure) return new Response(JSON.stringify({ ok: false, error: { code: 'SERVER_BUSY', message: 'Synthetic projection unavailable.' } }), { status: 503 });
    const response = await connect.handle({ ...body, origin });
    if (afterResponse) await afterResponse(body, response);
    if (body.op === 'profile.rename' && loseRenameResponse) { loseRenameResponse = false; throw new TypeError('Synthetic lost acknowledgement'); }
    return new Response(JSON.stringify(response), { status: response.ok ? 200 : 400 });
  };
  const client = storage => { const result = createClientWithStorage({ projectId, endpoint, fetch: fetcher, onState: publish }, storage); clients.push(result); return result; };
  const primary = client(store), bob = client(secondStore);
  t.after(() => { clients.forEach(value => value.dispose()); connect.close(); world.close(); });
  const a = (await primary.bootstrap('Alice')).accountId, b = (await bob.bootstrap('Bob')).accountId;
  const profiles = new Map([
    [a, (await primary.extension('world.profile.get', {}, { expectedAccountId: a })).profile],
    [b, (await bob.extension('world.profile.get', {}, { expectedAccountId: b })).profile],
  ]);
  const merged = store.inspect(); merged.installations.push(...secondStore.inspect().installations); merged.localRevision++;
  await store.compareAndSwap(merged.localRevision - 1, merged);
  const otherTab = client(store);
  const activeAccount = () => { const state = store.inspect(); return state.installations.find(item => item.deviceId === state.activeDeviceId)?.accountId; };
  function mount(options) {
    const app = Object.create(appModule.exports.TestController.prototype), dialogs = [], notices = [], paints = [], toasts = [];
    Object.assign(app, {
      options, api: options.api, deskAccount: activeAccount(), accountGeneration: 1, destroyed: false,
      profile: structuredClone(profiles.get(activeAccount())), group: null, view: 'mine', avatars: { setContext() {} },
      renderNavigation() { paints.push(this.profile.displayName); }, renderPersonal() {}, announce(text) { notices.push(text); }, toast(text) { toasts.push(text); },
      colorPicker() { return element('div'); },
      dialog() { const value = { element: element('dialog'), body: element('div'), close() { this.element.open = false; } }; dialogs.push(value); return value; },
    });
    const mounted = {
      app, options, dialogs, notices, paints, toasts,
      destroy() { app.destroyed = true; dialogs.forEach(dialog => dialog.close()); }, async refresh() {}, editProfile() { app.openProfileEditor(); },
      submit(name) {
        this.editProfile(); const dialog = dialogs.at(-1), nodes = descendants(dialog.body), form = nodes.find(node => node.tagName === 'form');
        const input = nodes.find(node => node.tagName === 'input' && node.type !== 'file'); input.value = name;
        const save = descendants(dialog.element).find(node => node.type === 'submit');
        const error = nodes.find(node => node.className === 'sw-error');
        const submit = () => form.handlers.get('submit')({ preventDefault() {} }); submit();
        return { dialog, form, input, save, error, retry: submit, settled: () => until(() => !save.disabled) };
      },
    };
    mounts.push(mounted); return mounted;
  }
  const module = { exports: {} };
  // VM literals have another Object prototype. Normalize only that test realm
  // boundary; production modules all share one browser realm.
  const accountPort = { ...primary, extension: (operation, args = {}, context) => primary.extension(operation, JSON.parse(JSON.stringify(args)),
    context === undefined ? undefined : JSON.parse(JSON.stringify(context))) };
  const ports = {
    './connection-intent.mjs': connectionIntents,
    './connection-hub': { async ensureConnectionAccount(_host, options) { const state = await options.client.getLocalState(); return { state, intent: null }; } },
    '../core/connect-client': { accountClient: accountPort, observeAccount(callback) { observers.add(callback); return () => observers.delete(callback); } },
    '../world/app': { mountWorldApp(_root, options) { return mount(options); } },
    './local-apps': { createAppActions() { return { connectDevice() {}, agentCreate() {}, resetAccount() {}, destroy() {} }; } },
    './assistant': { mountAssistant() {} }, './call-identity': { bindCallIdentity() { return () => {}; } },
  };
  vm.runInNewContext(adapterCode, { module, exports: module.exports, require: name => ports[name] ?? {}, URL, AbortSignal, AbortController,
    fetch: async () => ({ ok: false, status: 503 }), window: { location: { href: `${origin}/#mine`, origin }, addEventListener() {}, removeEventListener() {} },
  });
  await module.exports.startWorld({ isConnected: true }, { callLifecycle: {} }); traffic.length = 0;
  return {
    a, b, primary, bob, otherTab, store, traffic, mounts, current: () => mounts.at(-1),
    afterResponse(callback) { afterResponse = callback; }, failRename(value) { renameFailure = value; }, loseRenameReply() { loseRenameResponse = true; },
  };
}

test('one actual profile form save updates World, QR/card, request labels and saved local name', { timeout: 8000 }, async t => {
  const f = await fixture(t), original = f.current(), card = await f.primary.card();
  await f.primary.requestContact((await f.bob.card()).cardId);
  const edit = original.submit('  Алина новая  '); await edit.settled();
  assert.equal(edit.dialog.element.open, false, edit.error.textContent + JSON.stringify(f.traffic)); assert.deepEqual(original.toasts, ['Профиль сохранён']);
  assert.equal((await f.primary.extension('world.profile.get', {}, { expectedAccountId: f.a })).profile.displayName, 'Алина новая');
  assert.equal((await f.primary.card()).label, 'Алина новая');
  assert.equal((await f.bob.resolveCard(card.cardId)).label, 'Алина новая');
  const incoming = (await f.bob.contacts()).requests.incoming[0];
  assert.equal(incoming.label, 'Алина новая');
  await f.bob.acceptContact(incoming.requestId);
  assert.equal((await f.bob.contacts()).contacts[0].label, 'Алина новая');
  assert.equal((await f.primary.getLocalState()).label, 'Алина новая');
  assert.equal(original.app.profile.displayName, 'Алина новая');
  assert.equal(f.traffic.filter(op => op === 'profile.rename').length, 1);
});

test('contact-name failure retains World revision, inputs and editor; explicit retry repairs the projection', { timeout: 8000 }, async t => {
  const f = await fixture(t), original = f.current(); f.failRename(true);
  const edit = original.submit('Новое имя'); await edit.settled();
  assert.equal(edit.dialog.element.open, true); assert.equal(edit.input.value, 'Новое имя');
  assert.equal(original.app.profile.revision, 2); assert.equal(original.app.profile.displayName, 'Новое имя');
  assert.equal((await f.primary.card()).label, 'Alice');
  assert.match(edit.error.textContent, /Имя и описание сохранены.*контактах.*Сохранить/);
  assert.deepEqual(original.notices, []); assert.deepEqual(original.toasts, []);
  f.failRename(false); edit.retry(); await edit.settled();
  assert.equal(original.app.profile.revision, 3); assert.equal((await f.primary.card()).label, 'Новое имя');
  assert.equal(edit.dialog.element.open, false); assert.deepEqual(original.toasts, ['Профиль сохранён']);
});

test('lost Connect acknowledgement is presented as unconfirmed and the same form can safely retry', { timeout: 8000 }, async t => {
  const f = await fixture(t), original = f.current(); f.loseRenameReply();
  const edit = original.submit('Повторяемое имя'); await edit.settled();
  assert.equal((await f.primary.card()).label, 'Повторяемое имя');
  assert.equal((await f.primary.getLocalState()).label, 'Alice');
  assert.equal(edit.dialog.element.open, true); assert.match(edit.error.textContent, /не подтверждено/); assert.deepEqual(original.toasts, []);
  edit.retry(); await edit.settled();
  assert.equal((await f.primary.getLocalState()).label, 'Повторяемое имя');
  assert.equal(edit.dialog.element.open, false); assert.deepEqual(original.toasts, ['Профиль сохранён']);
});

test('World response from an obsolete A editor cannot continue into any rename after observed A → B → A', { timeout: 8000 }, async t => {
  const f = await fixture(t), original = f.current(), entered = deferred(), release = deferred();
  f.afterResponse(async body => { if (body.op === 'world.profile.update') { entered.resolve(); await release.promise; } });
  const edit = original.submit('Old A edit'); await entered.promise;
  await f.otherTab.switchProfile(f.b); await f.otherTab.switchProfile(f.a); release.resolve();
  await edit.settled();
  assert.equal(original.app.destroyed, true); assert.equal(edit.dialog.element.open, false);
  assert.equal(f.traffic.includes('profile.rename'), false); assert.deepEqual(original.toasts, []);
  assert.equal((await f.primary.card()).label, 'Alice'); assert.equal((await f.bob.card()).label, 'Bob');
  await assert.rejects(original.options.syncContactName('Obsolete callback'), { code: 'ACTIVE_PROFILE_CHANGED' });
});

test('coalesced selection change between World and rename never admits B even without an observer notice', { timeout: 8000 }, async t => {
  const f = await fixture(t), original = f.current();
  // Direct synthetic storage CAS models another tab whose broadcast is delayed.
  f.afterResponse(body => {
    if (body.op !== 'world.profile.update') return;
    f.store.nextRead(async () => {
      const next = f.store.inspect(), bob = next.installations.find(item => item.accountId === f.b);
      next.activeDeviceId = bob.deviceId; next.localRevision++;
      await f.store.compareAndSwap(next.localRevision - 1, next);
    });
  });
  const edit = original.submit('Must not become Bob'); await edit.settled();
  assert.equal((await f.bob.card()).label, 'Bob'); assert.equal(f.traffic.includes('profile.rename'), false);
  assert.deepEqual(original.toasts, []); assert.match(edit.error.textContent, /Аккаунт изменился/);
  f.afterResponse(null); await f.otherTab.switchProfile(f.a);
  assert.equal((await f.primary.card()).label, 'Alice');
});

test('account changes while the contact rename is pending cannot confirm or rename the new account', { timeout: 8000 }, async t => {
  const f = await fixture(t), original = f.current(), entered = deferred(), release = deferred();
  f.afterResponse(async body => { if (body.op === 'profile.rename') { entered.resolve(); await release.promise; } });
  const edit = original.submit('A only'); await entered.promise;
  await f.otherTab.switchProfile(f.b); await f.otherTab.switchProfile(f.a); release.resolve();
  await edit.settled(); await turn();
  assert.equal((await f.primary.card()).label, 'A only'); assert.equal((await f.bob.card()).label, 'Bob');
  assert.equal(original.app.destroyed, true); assert.deepEqual(original.toasts, []); assert.deepEqual(original.notices, []);
});

test('duplicate form submit dispatches one update and unchanged-name retry still repairs an old card', { timeout: 8000 }, async t => {
  const f = await fixture(t), original = f.current();
  await f.primary.rename('Old independent contact label', f.a); f.traffic.length = 0;
  const edit = original.submit('Alice'); edit.retry(); await edit.settled();
  assert.equal(f.traffic.filter(op => op === 'world.profile.update').length, 1);
  assert.equal(f.traffic.filter(op => op === 'profile.rename').length, 1);
  assert.equal((await f.primary.card()).label, 'Alice');
});
