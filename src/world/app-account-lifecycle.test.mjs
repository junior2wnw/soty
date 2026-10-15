import * as appInvitationCarrier from './app-invitation-carrier.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import * as appLaunch from './app-launch.mjs';
import * as appBootRecovery from './app-boot-recovery.mjs';
import * as browserAssistPilot from './browser-assist-pilot.mjs';
import * as appAudience from './app-audience.mjs';
import * as appActions from './app-actions.mjs';
import * as hiveDeviceBridge from './hive-device-bridge.mjs';
import * as appFullscreen from './app-fullscreen.mjs';
import * as appDeployment from './app-deployment.mjs';
import * as projectCapture from './project-feedback-capture.mjs';
import * as projectPicker from './project-feedback-picker.mjs';
import * as appProjectCapture from './app-project-capture.mjs';
import * as appScopedRenewal from './app-scoped-renewal.mjs';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createConnectService } from '../../modules/connect/server/index.mjs';
import { validateState } from '../../modules/connect/browser/storage.mjs';
import { DatabaseSync } from 'node:sqlite';
import { migrateAppsSchema, ensureCanonicalDomain, ensureInitialPublication, runtimeTargetDigest } from '../../modules/apps/server/schema.mjs';
import { createPublicationRegistry } from '../../modules/apps/server/publications.mjs';
import { createSourceRegistry } from '../../modules/apps/server/sources.mjs';
import { createEngagementEntryResolver } from '../../modules/apps/server/engagement-access.mjs';
import { createScopedAdmissionRegistry } from '../../modules/apps/scoped-embed/admissions.mjs';
import { HIVE_SELECTED_SOURCE } from '../../modules/apps/scoped-embed/resource-route-adapters.mjs';

// Exercise the actual controller methods. UI/network ports are inert fixtures;
// no source regex assertions, copied transition implementation or DOM package.
const source = await readFile(new URL('./app.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const stageSource = await readFile(new URL('./app-stage.ts', import.meta.url), 'utf8');
const stageCompiled = ts.transpileModule(stageSource, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const dialogsSource = await readFile(new URL('./dialogs.ts', import.meta.url), 'utf8');
const dialogsCompiled = ts.transpileModule(dialogsSource, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const productSource = await readFile(new URL('./product.ts', import.meta.url), 'utf8');
const productCompiled = ts.transpileModule(productSource, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const turn = () => new Promise(resolve => setImmediate(resolve));
const failure = code => Object.assign(new Error(code), { code });
class ElementPort {
  parent = null;
  handlers = new Map();
  children = []; open = true; isConnected = true; textContent = ''; changes = 0; dataset = {}; tagName = ''; className = '';
  attributes = new Map();
  classList = { add() {}, remove() {}, toggle() {} };
  append(...nodes) { for (const node of nodes) if (node instanceof ElementPort) node.parent = this; this.children.push(...nodes); this.changes++; }
  prepend(...nodes) { this.children.unshift(...nodes); this.changes++; }
  insertBefore(node, target) { const index = this.children.indexOf(target); if (index < 0) this.append(node); else this.children.splice(index, 0, node); }
  replaceChildren(...nodes) { for (const node of this.children) if (node instanceof ElementPort) node.parent = null; this.children = []; this.append(...nodes); }
  after(...nodes) { if (!this.parent) return; const index = this.parent.children.indexOf(this); for (const node of nodes) if (node instanceof ElementPort) node.parent = this.parent; this.parent.children.splice(index + 1, 0, ...nodes); }
  querySelector(selector) {
    for (const child of this.children) {
      if (!(child instanceof ElementPort)) continue;
      if (selector.startsWith('.') ? child.className.split(' ').includes(selector.slice(1)) : child.tagName === selector) return child;
      const found = child.querySelector(selector); if (found) return found;
    }
    return null;
  }
  listeners = new Map();
  reportValidity(){return true;}
  addEventListener(type, fn) { if (!this.handlers.has(type)) this.handlers.set(type, new Set()); this.handlers.get(type).add(fn);this.listeners.set(type,fn); }
  removeEventListener(type, fn) { this.handlers.get(type)?.delete(fn); }
  emit(type) { for (const fn of [...this.handlers.get(type) || []]) fn(); }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  removeAttribute(name) { this.attributes.delete(name); }
  getAttribute(name) { return this.attributes.get(name) ?? null; }
  contains(node) { return node === this || this.children.some(child => child instanceof ElementPort && child.contains(node)); }
  focus() {}
  remove() { this.isConnected = false; if (this.parent) this.parent.children = this.parent.children.filter(node => node !== this); this.parent = null; }
}
function fixture({ initial = 'account-A', hash = '#mine' } = {}) {
  const module = { exports: {} }, location = { hash, href: `https://shell.example/${hash}` };
  const document = { activeElement: null, visibilityState: 'visible', addEventListener() {}, removeEventListener() {} };
  const timers = new Map(), intervals = new Map(); let timerId = 0;
  const view = Object.assign(new EventTarget(), { location, document, Node: ElementPort, crypto, btoa,
    setTimeout(fn, delay) { const id = ++timerId; timers.set(id, { fn, delay }); return id; }, clearTimeout(id) { timers.delete(id); },
    setInterval(fn, delay) { const id = ++timerId; intervals.set(id, { fn, delay }); return id; }, clearInterval(id) { intervals.delete(id); },
    navigator: { userActivation: { isActive: false } }, matchMedia: () => ({ matches: false, addEventListener() {} }) });
  document.defaultView = view;
  const discussionCalls = { refresh: 0 };
  const makeElement = (tagName, className, label) => Object.assign(new ElementPort(), { ownerDocument: document, textContent: label ?? '', tagName, className: className ?? '',
    ...(tagName === 'iframe' ? { contentWindow: { sent: [], postMessage(data, origin) { this.sent.push({ data, origin }); } } } : {}) });
  const makeButton = (label, _icon, className, callback) => { const value = makeElement('button', className, label); if (callback) value.addEventListener('click', callback); return value; };
  const ports = {
    './dom': { el: makeElement, button:makeButton,iconButton:(label,icon,callback)=>makeButton(label,icon,'',callback), emptyState: label => makeElement('div', '', label),
      textInput: (value = '', placeholder = '') => Object.assign(makeElement('input'), { value, placeholder }),
      labeledField: (label, control) => { const element = makeElement('label', '', label); element.append(control); return element; } },
    './dialogs': { errorText: error => error.code ?? 'failure' },
    './product': { loadDeskPreferences: accountId => ({ favorites: [`favorite-${accountId}`], recent: [], pinnedApps: [`pin-${accountId}`] }) },
    './hex-field': { createHexFieldState: () => ({ fresh: true }) },
    './app-launch.mjs': appLaunch,
    './app-boot-recovery.mjs': appBootRecovery,
    './browser-assist-pilot.mjs': browserAssistPilot,
    './app-audience.mjs': appAudience,
    './app-actions.mjs': appActions,
    './hive-device-bridge.mjs': hiveDeviceBridge,
    './app-invitation-carrier.mjs': appInvitationCarrier,
    './app-fullscreen.mjs': appFullscreen,
    './app-deployment.mjs': appDeployment,
    './project-feedback-capture.mjs': projectCapture,
    './project-feedback-picker.mjs': projectPicker,
    './app-project-capture.mjs': appProjectCapture,
    './app-scoped-renewal.mjs': appScopedRenewal,
    './icons': { icon: () => makeElement('svg') },
    './application-card': { appTone: () => 'neutral' },
    './app-saved': { mountAppSaved: () => ({ dispose() {}, async refresh() {} }) },
    './app-discussion': { mountAppDiscussion: () => ({ dispose() {}, async refresh() { discussionCalls.refresh++; }, async flush() {}, hasUnsavedChanges: () => false, setVisible() {}, async updateEntry() {}, async updateSelection() {}, focus() {} }) },
  };
  const dialogsModule = { exports: {} };
  vm.runInNewContext(dialogsCompiled, { module: dialogsModule, exports: dialogsModule.exports, require: name => ports[name] ?? {}, crypto: globalThis.crypto });
  ports['./dialogs'].pinDialogSubmit = dialogsModule.exports.pinDialogSubmit;
  const productModule = { exports: {} }, preferences = new Map();
  vm.runInNewContext(productCompiled, { module: productModule, exports: productModule.exports, require: name => ports[name] ?? {},
    localStorage: { getItem: key => preferences.get(key) ?? null, setItem: (key, value) => preferences.set(key, value) }, URLSearchParams });
  ports['./product'].saveDeskPreferences = productModule.exports.saveDeskPreferences;
  const stageModule = { exports: {} };
  vm.runInNewContext(stageCompiled, { module: stageModule, exports: stageModule.exports, require: name => ports[name] ?? {}, AbortController, crypto: globalThis.crypto });
  ports['./app-stage'] = stageModule.exports;
  vm.runInNewContext(`${compiled}\nexports.TestController = WorldApplication;`, {
    module, exports: module.exports, require: name => ports[name] ?? {}, location, window: view,
    document, HTMLElement: ElementPort, URLSearchParams, setTimeout, clearTimeout, clearInterval,
    history: { replaceState(_state, _title, url) { location.hash = url; }, pushState(_state, _title, url) { location.hash = url; } }, console,
  });
  const Controller = module.exports.TestController, app = Object.create(Controller.prototype);
  let local = initial, online = true, apiError = null;
  const requests = [], personalFrames = [], dialogs = [];
  Object.assign(app, {
    deskAccount: initial, accountGeneration: 1, requestSequence: 0, screenSequence: 1, homeRequest: 0, destroyed: false,
    root: makeElement('div'), main: makeElement('main'), live: makeElement('div'), dialogs: new Set(), avatars: { setContext() {} },
    appSettingsDialog: null, appSettingsRouteClose: null, field: null, homeHandle: null, notesHandle: null, assistantHandle: null, accessHandle: null, appStage: null,
    chatCleanup: null, chatTimer: null, searchTimer: null, toastTimer: null, noteActionPending: false, visibilityOpen: false,
    profile: { profileId: initial, displayName: `private-${initial}` }, communities: [{ communityId: `group-${initial}` }],
    apps: [{ appId: `app-${initial}`, name: `private-${initial}` }], devices: [{ deviceId: `device-${initial}` }], homeNotes: [{ noteId: `note-${initial}` }],
    group: null, selected: null, selectedChat: `chat-${initial}`, groupReturn: 'world', groupTab: 'apps',
    homeState: { slots: new Map([['private-slot', initial]]), scroll: 20, fieldX: 3, fieldY: 9, communityId: 'private-group', focusId: 'private-focus', focusControl: null, lens: 'all', pinned: new Set(['old-pin']) },
    homeStatus: { devices: 'ready', apps: 'ready', communities: 'ready', notes: 'ready' },
    desk: { favorites: [`favorite-${initial}`], recent: [] }, fieldState: {}, homeFieldState: {}, discoveryApps: new Map([['private-group', [{ appId: `private-${initial}` }]]]), homeQuery: 'private apps', homeFilter: 'together', discoveryScope: 'private-scope', discoveryPages: ['private-cursor'],
    results: { people: [{ name: `private-${initial}` }], communities: [], totals: {} }, discoveryStatus: 'ready', query: 'private search', kind: 'people', routeLoaded: true,
    view: 'mine', activeRoute: hash,
    options: { localAccount: async () => ({ accountId: local, label: 'Local identity' }), listDevices: async () => [{ deviceId: `device-${local}` }] },
    renderNavigation() { this.lastNavigation = { accountId: this.deskAccount, profileId: this.profile?.profileId ?? null }; },
    renderCurrent() { this.cleanScreen(); },
    renderPersonal() { personalFrames.push({ accountId: this.deskAccount, names: this.apps.map(value => value.name) }); },
    openRoute: async () => false,
    openNotes() { this.notesHandle = { accountId: this.deskAccount, dispose() {} }; },
    toast(message) { this.lastToast = message; },
    syntheticEmblem() { return new ElementPort(); },
    remember() {},
    dialog() {
      const element = new ElementPort(), body = new ElementPort();
      const dialog = { element, body, close() { element.open = false; } };
      dialogs.push(dialog); this.dialogs.add(dialog); return dialog;
    },
  });
  app.api = { request: async (op, args = {}) => {
    requests.push({ op, args: structuredClone(args), accountId: local });
    if (!online) throw failure('NETWORK_ERROR'); if (apiError) throw failure(apiError);
    if (op === 'world.profile.get') return { profile: { profileId: local, displayName: `profile-${local}` } };
    if (op === 'world.community.list') return { communities: [{ communityId: `group-${local}` }] };
    if (op === 'apps.list') return { apps: [{ id: `app-${local}`, name: `project-${local}`, hostDeviceId: `device-${local}`, state: 'enabled', ownerAccountId: local }] };
    if (op === 'apps.catalog') return { apps: [] };
    if (op === 'notes.list') return { notes: [{ noteId: `note-${local}` }] };
    throw failure('unexpected_fixture_operation');
  } };
  return { app, Controller, view, timers, intervals, location, requests, personalFrames, dialogs, discussionCalls, mountStage: stageModule.exports.mountAppStage,
    workspacePreferences: productModule.exports, useSavedWorkspace() { ports['./product'].loadDeskPreferences = productModule.exports.loadDeskPreferences; },
    setLocal(value) { local = value; }, setOnline(value) { online = value; }, setApiError(value) { apiError = value; } };
}

test('actual shell stores its exact screen and restores the correct account workspace on a bare entry or account switch', () => {
  const f = fixture(); f.useSavedWorkspace();
  const route = 'launch/app-11111111111111111111111111111111/dom_11111111111111111111111111111111?path=%2Fwidget';
  f.app.writeRoute(route); assert.equal(f.workspacePreferences.loadDeskPreferences('account-A').lastRoute, route);
  f.workspacePreferences.saveDeskPreferences('account-B', { favorites: [], recent: [], lastRoute: 'notes/note-b' }, { selectedRoute: true });
  f.app.transitionAccount('account-B'); assert.equal(f.location.hash, '#notes/note-b');
  f.app.transitionAccount('account-A'); assert.equal(f.location.hash, '#' + route);
  const fresh = fixture({ initial: '', hash: '#mine' }); fresh.useSavedWorkspace(); fresh.app.restoreLastRoute = true;
  fresh.workspacePreferences.saveDeskPreferences('account-A', { favorites: [], recent: [], lastRoute: route }, { selectedRoute: true });
  fresh.app.transitionAccount('account-A'); assert.equal(fresh.location.hash, '#' + route);
  const linked = fixture({ initial: '', hash: '#notes/shared-note' }); linked.useSavedWorkspace(); linked.app.restoreLastRoute = false;
  linked.workspacePreferences.saveDeskPreferences('account-A', { favorites: [], recent: [], lastRoute: route }, { selectedRoute: true });
  linked.app.transitionAccount('account-A'); assert.equal(linked.location.hash, '#notes/shared-note');
});

test('actual app onboarding cannot paint a delayed device list after A switches to B', async () => {
  const f = fixture(), pending = deferred(); let received;
  f.app.api = { request(op, args) { assert.equal(op, 'apps.devices'); received = args; return pending.promise; } };
  f.app.openAddApp(); const dialog = f.dialogs.at(-1), before = dialog.body.changes;
  assert.equal(received.expectedAccountId, 'account-A'); f.app.transitionAccount('account-B');
  pending.resolve({ devices: [{ claimed: true, online: true, hostDeviceId: 'old-A-host', connectorId: 'old-A-connector', name: 'Private A device' }] }); await turn();
  assert.equal(dialog.body.changes, before); assert.equal(dialog.element.open, false);
});

test('actual app onboarding pins A at dispatch and ignores a late success after A to B to A', async () => {
  const f = fixture(), pending = deferred(), calls = [];
  f.app.api = { request(op, args) { calls.push({ op, args: structuredClone(args) });
    if (op === 'apps.devices') return Promise.resolve({ devices: [{ claimed: true, online: true, hostDeviceId: 'host-A', connectorId: 'connector-A', name: 'A device' }] });
    assert.equal(op, 'apps.register'); return pending.promise; } };
  f.app.openAddApp(); await turn(); const dialog = f.dialogs.at(-1), form = dialog.body.children[0];
  const collect = (element, tag) => [element, ...element.children.flatMap(child => child instanceof ElementPort ? collect(child, tag) : [])].filter(child => child.tagName === tag);
  const inputs = collect(form, 'input'), selectors = collect(form, 'select');
  inputs[0].value = 'Synthetic A project'; inputs[1].value = '3000'; selectors[0].value = 'host-A'; selectors[1].value = '';
  const event = { preventDefault() {} }; form.listeners.get('submit')(event); form.listeners.get('submit')(event);
  const dispatched = calls.filter(call => call.op === 'apps.register'); assert.equal(dispatched.length, 1); assert.equal(dispatched[0].args.expectedAccountId, 'account-A');
  f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A');
  pending.resolve({ app: { id: 'synthetic-accepted-A' } }); await turn();
  assert.equal(f.app.lastToast, undefined); assert.equal(f.personalFrames.length, 0); assert.equal(dialog.element.open, false);
});

test('first local identity keeps a visible status while the real profile request is pending', async () => {
  const f = fixture({ initial: '' }), pending = deferred(), original = f.app.api.request;
  f.setLocal('account-A');
  f.app.renderCurrent = () => f.app.main.replaceChildren(new ElementPort());
  f.app.api.request = (op, args) => op === 'world.profile.get' ? pending.promise : original(op, args);
  const refreshing = f.app.refresh(); await turn();
  assert.equal(f.app.deskAccount, 'account-A'); assert.equal(f.app.profile, null);
  const status = f.app.main.children[0];
  assert.equal(status.getAttribute('role'), 'status'); assert.equal(status.getAttribute('aria-live'), 'polite');
  assert.equal(status.children[0].textContent, 'Проверяем аккаунт');
  assert.equal(f.app.apps.length, 0); assert.equal(f.app.results.people.length, 0);
  pending.resolve({ profile: { profileId: 'account-A', displayName: 'verified-A' } }); await refreshing;
  assert.equal(f.app.profile.profileId, 'account-A'); assert.equal(f.app.main.contains(status), false);
});

test('pending account switch clears private content and stale reply cannot restore it', async () => {
  const f = fixture(), pending = deferred(), original = f.app.api.request;
  f.setLocal('account-B');
  f.app.api.request = (op, args) => op === 'world.profile.get' ? pending.promise : original(op, args);
  const refreshing = f.app.refresh(true); await turn();
  assert.equal(f.app.profile, null); assert.equal(f.app.apps.length, 0); assert.equal(f.app.communities.length, 0);
  assert.equal(f.app.main.children[0].children[0].textContent, 'Проверяем аккаунт');
  f.setLocal('account-C');
  pending.resolve({ profile: { profileId: 'account-B', displayName: 'private-B' } }); await refreshing;
  assert.equal(f.app.profile, null); assert.equal(f.app.apps.length, 0);
  assert.equal(f.app.main.children[0].textContent, 'Соты ждут вас');
});

test('same-account pending refresh retains the live application without inserting a boot status', async () => {
  const f = fixture(), pending = deferred(), original = f.app.api.request, screen = new ElementPort();
  const dialog = f.app.dialog(); let disposed = 0;
  f.app.appSettingsDialog = dialog; f.app.appStage = { dispose() { disposed++; } }; f.app.main.append(screen);
  f.app.api.request = (op, args) => op === 'world.profile.get' ? pending.promise : original(op, args);
  const refreshing = f.app.refresh(); await turn();
  assert.equal(f.app.main.children[0], screen); assert.equal(disposed, 0);
  pending.resolve({ profile: { profileId: 'account-A', displayName: 'verified-A' } }); await refreshing;
  assert.equal(f.app.main.children[0], screen); assert.equal(disposed, 0); assert.equal(dialog.element.open, true);
});

test('actual refresh A → offline B → online B clears every private cache before B metadata is available', async () => {
  const f = fixture(), oldDialog = f.app.dialog();
  let disposed = 0; f.app.notesHandle = { dispose() { disposed++; } };
  f.setLocal('account-B'); f.setOnline(false); await f.app.refresh(true);
  assert.equal(f.app.deskAccount, 'account-B'); assert.equal(f.app.profile, null);
  for (const key of ['apps', 'communities', 'devices']) assert.equal(f.app[key].length, 0, key);
  assert.equal(f.app.homeNotes, null); assert.equal(f.app.results.people.length, 0);
  assert.equal(f.app.homeState.slots.size, 0); assert.equal(f.app.query, '');
  assert.equal(f.app.discoveryApps.size, 0); assert.equal(f.app.homeQuery, ''); assert.equal(f.app.homeFilter, 'all');
  assert.equal(oldDialog.element.open, false); assert.equal(disposed, 1);
  assert.equal(f.app.desk.favorites[0], 'favorite-account-B');
  f.setOnline(true); await f.app.refresh(true);
  assert.equal(f.app.profile.profileId, 'account-B');
  assert.equal(f.app.apps.length, 1); assert.equal(f.app.apps[0].name, 'project-account-B');
  assert.equal(f.personalFrames.some(value => value.accountId === 'account-B' && value.names.some(name => name.includes('account-A'))), false);
  assert.equal(f.requests.filter(value => value.op === 'apps.list').every(value => value.args.expectedAccountId === 'account-B'), true);
});

test('actual network failure preserves notes and settings only with a matching local identity', async () => {
  for (const kind of ['notes', 'settings']) {
    const f = fixture(); let disposed = 0;
    const notes = { dispose() { disposed++; } }, window = f.app.dialog();
    if (kind === 'notes') f.app.notesHandle = notes;
    else f.app.appSettingsDialog = window;
    const sequence = f.app.screenSequence; f.setOnline(false); await f.app.refresh(true);
    assert.equal(f.app.screenSequence, sequence, kind); assert.equal(disposed, 0);
    if (kind === 'notes') assert.equal(f.app.notesHandle, notes); else assert.equal(window.element.open, true);
    f.setOnline(true); await f.app.refresh(true);
    assert.equal(f.app.screenSequence, sequence, kind); assert.equal(disposed, 0);
  }
});

test('missing or unreadable local identity never takes the preserveNote catch shortcut', async () => {
  for (const mode of ['missing', 'unreadable']) {
    const f = fixture(); let disposed = 0; f.app.notesHandle = { dispose() { disposed++; } };
    if (mode === 'missing') f.setLocal(null);
    else f.app.options.localAccount = async () => { throw failure('NETWORK_ERROR'); };
    f.setOnline(false); await f.app.refresh(true);
    assert.equal(disposed, 1); assert.equal(f.app.notesHandle, null);
    assert.equal(f.app.deskAccount, ''); assert.equal(f.app.profile, null); assert.equal(f.app.apps.length, 0);
    assert.equal(f.app.main.children[0].textContent, 'Соты ждут вас');
  }
});

test('a server-revoked identity cannot preserve private notes merely because the local account ID still matches', async () => {
  const f = fixture(); let disposed = 0; f.app.notesHandle = { dispose() { disposed++; } };
  f.setApiError('DEVICE_REVOKED'); await f.app.refresh(true);
  assert.equal(disposed, 1); assert.equal(f.app.deskAccount, ''); assert.equal(f.app.profile, null); assert.equal(f.app.apps.length, 0);
});

test('actual deferred resource loader cannot refill old account caches or its still-connected closed dialog', async () => {
  const f = fixture(), pending = deferred(); f.app.options.listApps = () => pending.promise;
  f.app.openResources('apps'); const oldDialog = f.dialogs.at(-1), before = oldDialog.body.changes;
  f.app.transitionAccount('account-B');
  // Native close removes the DOM in a later task. Even a retained stale node
  // cannot admit an old result through the account/screen fence.
  oldDialog.element.isConnected = true;
  pending.resolve([{ appId: 'old-app', name: 'private-account-A' }]); await turn();
  assert.equal(f.app.apps.length, 0); assert.equal(oldDialog.body.changes, before);
});

test('returning to A cannot make an earlier A loader current again after A → B → A', async () => {
  const f = fixture(), pending = deferred(); f.app.options.listApps = () => pending.promise;
  const loading = f.app.loadApps(); const rejected = assert.rejects(loading, { code: 'ACTIVE_PROFILE_CHANGED' });
  f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A');
  pending.resolve([{ appId: 'old-app', name: 'old-generation-A' }]); await rejected;
  assert.equal(f.app.apps.length, 0);
});

test('late resource errors are ignored after a transition instead of appearing in another account window', async () => {
  const f = fixture(), pending = deferred(); f.app.options.listApps = () => pending.promise;
  f.app.openResources('apps'); const old = f.dialogs.at(-1), before = old.body.changes;
  f.app.transitionAccount('account-B'); pending.reject(failure('NETWORK_ERROR')); await turn();
  assert.equal(old.body.changes, before); assert.equal(f.app.lastToast, undefined);
});

test('profile response for A cannot repopulate the shell if the local identity changed to B while it was loading', async () => {
  const f = fixture(), pending = deferred(), original = f.app.api.request;
  f.app.api.request = (op, args) => op === 'world.profile.get' ? pending.promise : original(op, args);
  const refreshing = f.app.refresh(true); await turn(); f.setLocal('account-B');
  pending.resolve({ profile: { profileId: 'account-A', displayName: 'private-A' } }); await refreshing;
  assert.equal(f.app.profile, null); assert.equal(f.app.apps.length, 0); assert.equal(f.app.deskAccount, '');
});

test('same-account settings refresh retains the running app and existing window', async () => {
  const f = fixture(), dialog = f.app.dialog(); let disposed = 0;
  f.app.appSettingsDialog = dialog; f.app.appStage = { dispose() { disposed++; } };
  const sequence = f.app.screenSequence; await f.app.refresh();
  assert.equal(disposed, 0); assert.equal(dialog.element.open, true); assert.equal(f.app.screenSequence, sequence);
});

const stageAppId = `app-${'c'.repeat(32)}`, stageDomainId = `dom_${'d'.repeat(32)}`;
const stageResponse = path => ({ url: `https://runtime.example/_soty/boot?${new URLSearchParams({ path })}#${'e'.repeat(43)}`,
  entry: { appId: stageAppId, domainId: stageDomainId, origin: 'https://runtime.example', path } });
const stageBinding = { schema: 'soty.app-launch-binding.v1', policyEpoch: 1, targetRevision: 1,
  targetDigest: 'b'.repeat(64), profile: 'soty.relay-restricted.v1', bindingFloor: 1 };

// Real Apps8 publication, Source promotion, admission and entry projection.
// The controller's transport remains an inert UI port, not Native/SSO proof.
function promotedEntryFixture(t) {
  const db = new DatabaseSync(':memory:'); db.exec('PRAGMA foreign_keys=ON');
  migrateAppsSchema(db, { allowScopedEmbedMigration: true, allowSelectedResourceMigration: true });
  const actor = Object.freeze({ accountId: 'account-A', deviceId: 'synthetic-browser' });
  const identity = { linkId: 'synthetic-link', hostDeviceId: 'synthetic-host', connectorId: 'synthetic-connector' };
  const connectorKey = [identity.linkId, identity.hostDeviceId, identity.connectorId].join('|');
  db.exec('BEGIN IMMEDIATE');
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey, actor.accountId, JSON.stringify(identity), 'Synthetic device', 1);
  db.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(stageAppId, actor.accountId, connectorKey, 'Отзывы', 63345, '/',
    JSON.stringify({ accountIds: [], communityIds: [] }), 'enabled', 1, 1, 1);
  const row = db.prepare('SELECT * FROM local_apps WHERE id=?').get(stageAppId);
  ensureCanonicalDomain(db, row, 'https://{appId}.runtime.example'); ensureInitialPublication(db, row); db.exec('COMMIT');
  const target = { appId: stageAppId, revision: 2, ownerAccountId: actor.accountId, connectorKey, port: 63345,
    entryPath: '/embed', profile: 'soty.selected-human-embed.v2' };
  target.digest = runtimeTargetDigest(target);
  const assertActor = value => assert.deepEqual(value, actor);
  const admissions = createScopedAdmissionRegistry({ db, profiles: [{ schema: target.profile, appId: stageAppId, connector: identity,
    target: { revision: target.revision, digest: target.digest }, sourceProfile: { ...HIVE_SELECTED_SOURCE },
    resource: { registryId: 'soty', tenantId: actor.accountId, appId: stageAppId, environmentId: 'production', resourceId: 'selected',
      selection: { kind: 'hive.project.v1', nativeId: 'synthetic-native', incarnationId: 'one' } },
    issuer: 'https://shell.example/human-identity', clientId: 'synthetic-selected', embedOrigin: `https://${stageAppId}.runtime.example`,
    nativeOrigin: 'https://native.example', parentOrigin: 'https://shell.example' }] });
  const publications = createPublicationRegistry({ db, assertActor, canUse: value => value.accountId === actor.accountId && value.deviceId === actor.deviceId });
  const channel = Object.freeze({ owned: true });
  const sources = createSourceRegistry({ db, assertActor, publications, scopedAdmission: admissions,
    prepareTarget: async frame => Object.freeze({ target: frame.target, channel }),
    verifyPreparedTarget: frame => frame.evidence.channel === channel && frame.evidence.target === frame.target });
  const resolve = createEngagementEntryResolver({ db, assertActor, publications, inspectSource: () => ({ state: 'responding' }) });
  t.after(() => { sources.close(); db.close(); });
  function entry(args = {}) {
    db.exec('BEGIN');
    try { const { appId, domainId, origin, path } = resolve({ actor, appId: stageAppId,
      ...(args.domainId === undefined ? {} : { domainId: args.domainId }), ...(args.path === undefined ? {} : { path: args.path }) });
      db.exec('COMMIT'); return { appId, domainId, origin, path };
    } catch (error) { db.exec('ROLLBACK'); throw error; }
  }
  return { entry, db, async promote() {
    const before = publications.execute({ op: 'apps.publication.get', actor, args: { appId: stageAppId } });
    const prepared = await sources.execute({ op: 'apps.source.prepare', actor, args: { appId: stageAppId,
      expectedPolicyEpoch: before.policyEpoch, expectedTargetRevision: before.activeTargetRevision,
      source: { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, port: target.port,
        entryPath: target.entryPath, profile: target.profile } } });
    const ack = sources.execute({ op: 'apps.source.promote', actor, args: { appId: stageAppId, requestId: 'synthetic-promotion',
      preparationId: prepared.preparationId, expectedPolicyEpoch: prepared.expectedPolicyEpoch,
      expectedTargetRevision: prepared.expectedTargetRevision, launchPolicy: 'restricted', listed: false } });
    assert.equal(ack.current.activeTargetRevision, 2); assert.equal(admissions.require(target).schema, target.profile);
  } };
}
function entryTransport(f, model, calls, denyLaunch = () => false) {
  f.app.api = { async request(op, args) {
    assert.equal(args.expectedAccountId, 'account-A'); calls.push({ op, args: structuredClone(args) });
    assert.ok(op === 'apps.launch' || op === 'apps.entry.get');
    if (op === 'apps.launch' && denyLaunch(args)) throw failure('app_scoped_admission_required');
    const entry = model.entry(args);
    return op === 'apps.entry.get' ? { entry } : { entry,
      launchUrl: `${entry.origin}/_soty/boot?${new URLSearchParams({ path: entry.path })}#${'e'.repeat(43)}` };
  } };
}

test('ordinary application click resolves the active entry after actual Apps8 Source promotion', async t => {
  const model = promotedEntryFixture(t), f = fixture(), calls = [];
  const cached = { appId: stageAppId, name: 'Отзывы', entry: model.entry() };
  entryTransport(f, model, calls);
  await f.app.openApplication(cached);
  assert.equal(f.app.appStage.entry().path, '/'); assert.ok(f.app.main.querySelector('iframe'));
  await model.promote(); assert.equal(model.entry().path, '/embed');
  assert.equal(model.db.prepare('SELECT entry_path FROM local_apps WHERE id=?').get(stageAppId).entry_path, '/');
  calls.length = 0; await f.app.openApplication(cached);
  assert.equal(cached.entry.path, '/', 'the existing field/catalogue snapshot remains stale');
  assert.equal(calls.length, 1); assert.equal(calls[0].op, 'apps.launch');
  assert.equal(Object.hasOwn(calls[0].args, 'path'), false, 'ordinary click asks the server for its current default');
  assert.equal(calls[0].args.domainId, cached.entry.domainId);
  assert.equal(f.app.appStage.entry().path, '/embed');
  assert.equal(new URL(f.app.main.querySelector('iframe').src).searchParams.get('path'), '/embed');
  assert.equal(appLaunch.parseAppLaunchRoute(f.location.hash).target.path, '/embed');
  f.app.cleanScreen();
});

test('explicit legacy deep links stay exact on refusal; a new ordinary click recovers the active Source entry', async t => {
  const model = promotedEntryFixture(t), f = fixture(), calls = [];
  const cached = { appId: stageAppId, name: 'Отзывы', entry: model.entry() };
  const deep = appLaunch.parseAppLaunchRoute(appLaunch.formatAppLaunchRoute({ appId: stageAppId,
    domainId: cached.entry.domainId, path: '/old-report?tab=private' }));
  entryTransport(f, model, calls); await f.app.openApplication(cached, deep);
  assert.equal(calls[0].args.path, deep.target.path); assert.equal(f.app.appStage.entry().path, deep.target.path);
  await model.promote(); calls.length = 0;
  // Exercise the actual UI's handling of the selected server's existing denial.
  entryTransport(f, model, calls, args => args.path !== undefined && args.path !== '/embed');
  const old = appLaunch.parseAppLaunchRoute(appLaunch.formatAppLaunchRoute({ appId: stageAppId,
    domainId: cached.entry.domainId, path: '/' }, undefined, { panel: 'discussion' }));
  await f.app.openApplication(cached, old);
  assert.equal(f.app.main.querySelector('iframe'), null);
  assert.deepEqual(calls.map(({ op, args }) => [op, args.path]), [['apps.launch', '/'], ['apps.entry.get', '/']]);
  assert.equal(f.app.appStage.entry().path, '/'); assert.equal(appLaunch.parseAppLaunchRoute(f.location.hash).target.path, '/');
  const failures = calls.length; await turn(); assert.equal(calls.length, failures, 'no automatic default retry of a refused deep link');
  calls.length = 0; await f.app.openApplication(cached);
  assert.equal(calls.length, 1); assert.equal(Object.hasOwn(calls[0].args, 'path'), false);
  assert.equal(f.app.appStage.entry().path, '/embed'); assert.ok(f.app.main.querySelector('iframe'));
  f.app.cleanScreen();
});
function bootHint(f, frame) {
  frame.emit('load'); const sent = frame.contentWindow.sent.at(-1); assert.ok(sent);
  assert.equal(sent.origin, 'https://runtime.example');
  const data = { schema: 'soty.app-boot-failure.v1', appId: stageAppId, nonce: sent.data.nonce, error: 'app_session_check_failed' };
  return () => f.view.dispatchEvent(Object.assign(new Event('message'), { data, source: frame.contentWindow, origin: 'https://runtime.example', ports: [] }));
}
async function recoveryStage(request) {
  const route = appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/' });
  const f = fixture({ hash: '#' + route }); f.app.options.openApp = request;
  await f.app.openApplication({ appId: stageAppId, name: 'Editor' }, appLaunch.parseAppLaunchRoute(route));
  return f;
}

async function widgetStage(request) {
  const f = fixture({ hash: '#world' }), account = f.app.deskAccount, generation = f.app.accountGeneration;
  const intent = appLaunch.parseAppLaunchRoute(appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/' }));
  f.app.appStage = f.mountStage(f.app.main, { api: f.app.api, accountId: account, app: { appId: stageAppId, name: 'Widget' }, intent,
    presentation: 'widget', instanceId: 'widget-11111111-1111-4111-8111-111111111111', request,
    isCurrent: () => !f.app.destroyed && f.app.deskAccount === account && f.app.accountGeneration === generation,
    onNavigate() {}, onBack() {}, async onAccount() {}, onSettings() {},
  });
  await f.app.appStage.ready;
  return f;
}

for (const profile of ['legacy', 'scoped']) test(`${profile} widget no-load has a bounded UNKNOWN notice and manual retry without another launch or API request`, async t => {
  let calls = 0;
  const f = await widgetStage(async () => { calls++; return { ...stageResponse('/'), ...(profile === 'scoped' ? {
    runtimeProfile: 'soty.selected-human-embed.v2', scopedCloseHandle: 'h'.repeat(43),
    scopedSource: { id: 'soty.standard-workspace', version: 2, digest: 'a'.repeat(64) },
  } : {}) }; }); t.after(() => f.app.cleanScreen());
  const runtime = f.app.main.querySelector('.sa-runtime'), message = f.app.main.querySelector('.sa-message');
  const retry = f.app.main.querySelector('.sa-recovery-retry'), deadline = [...f.timers.values()].find(value => value.delay === 12000);
  assert.ok(deadline); assert.match(message.textContent, /Открываем окно/u); assert.equal(runtime.getAttribute('aria-busy'), 'true');
  assert.equal(retry.hidden, false); assert.equal(retry.disabled, false);
  deadline.fn(); await turn();
  assert.match(message.textContent, /не подтверждена/u); assert.equal(runtime.getAttribute('aria-busy'), 'false');
  assert.equal(retry.hidden, false); assert.equal(calls, 1); assert.deepEqual(f.requests, []); assert.equal(f.timers.size, 0);
});

for (const profile of ['legacy', 'scoped']) test(`${profile} widget load settles transport only and keeps manual recovery after load-to-blank`, async t => {
  let calls = 0;
  const f = await widgetStage(async () => { calls++; return { ...stageResponse('/'), ...(profile === 'scoped' ? {
    runtimeProfile: 'soty.selected-human-embed.v2', scopedCloseHandle: 'h'.repeat(43),
    scopedSource: { id: 'soty.standard-workspace', version: 2, digest: 'a'.repeat(64) },
  } : {}) }; }); t.after(() => f.app.cleanScreen());
  const frame = f.app.main.querySelector('iframe'), message = f.app.main.querySelector('.sa-message');
  f.view.dispatchEvent(Object.assign(new Event('message'), { source: frame.contentWindow, origin: 'https://runtime.example',
    data: { schema: 'soty.app-slot-ready.v1', requestId: 'guessed', ready: true }, ports: [] }));
  assert.match(message.textContent, /Открываем окно/u, 'a renewal hint cannot attest this visible frame');
  frame.emit('load'); await turn();
  assert.equal(message.hidden, true); assert.equal(message.textContent, '');
  assert.equal(f.app.main.querySelector('.sa-runtime').getAttribute('aria-busy'), 'false');
  assert.equal(f.app.main.querySelector('.sa-recovery-retry').hidden, false);
  assert.equal(calls, 1); assert.deepEqual(f.requests, []); assert.equal(f.timers.size, 0);
});

test('widget transport error permits one explicit retry and ignores queued events from the replaced frame', async t => {
  let calls = 0; const pending = deferred();
  const f = await widgetStage(async () => ++calls === 1 ? stageResponse('/') : pending.promise); t.after(() => f.app.cleanScreen());
  const first = f.app.main.querySelector('iframe'), oldEvents = [...first.handlers.get('load'), ...first.handlers.get('error')];
  first.emit('error');
  const retry = f.app.main.querySelector('.sa-recovery-retry');
  assert.match(f.app.main.querySelector('.sa-message').textContent, /Не удалось загрузить окно/u);
  assert.equal(retry.hidden, false); assert.equal(f.timers.size, 0);
  retry.emit('click'); retry.emit('click'); await turn();
  assert.equal(calls, 2); assert.equal(retry.disabled, true);
  pending.resolve(stageResponse('/')); await turn();
  const second = f.app.main.querySelector('iframe'), notice = f.app.main.querySelector('.sa-message').textContent;
  assert.notEqual(second, first); assert.equal(retry.disabled, false);
  for (const event of oldEvents) event();
  assert.equal(f.app.main.querySelector('iframe'), second); assert.equal(f.app.main.querySelector('.sa-message').textContent, notice);
  assert.equal(calls, 2); assert.equal([...f.timers.values()].filter(value => value.delay === 12000).length, 1);
});

for (const transition of ['Back', 'account ABA']) test(`widget ${transition} clears the display timer and denies late frame events`, async () => {
  let calls = 0;
  const f = await widgetStage(async () => { calls++; return stageResponse('/'); });
  const frame = f.app.main.querySelector('iframe'), events = [...frame.handlers.get('load'), ...frame.handlers.get('error')];
  const deadline = [...f.timers.values()].find(value => value.delay === 12000);
  if (transition === 'Back') { f.location.hash = '#mine'; f.app.cleanScreen(); }
  else { f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A'); }
  for (const event of events) event(); deadline.fn(); await turn();
  assert.equal(f.app.appStage, null); assert.equal(f.app.main.children.length, 0); assert.equal(f.timers.size, 0); assert.equal(calls, 1);
});

test('actual stage uses one fresh launch per original open and offers a manual action after a repeated untrusted hint', async () => {
  const calls = [], f = await recoveryStage(async (_app, args) => { calls.push(args); return { ...stageResponse('/'), launchBinding: stageBinding }; });
  const first = f.app.main.querySelector('iframe'), emit = bootHint(f, first); emit(); emit(); await turn();
  const second = f.app.main.querySelector('iframe'); assert.notEqual(first, second); assert.equal(calls.length, 2);
  assert.deepEqual(calls[1], { appId: stageAppId, domainId: stageDomainId, path: '/', expectedAccountId: 'account-A', instanceId: 'main' });
  bootHint(f, second)(); await turn(); assert.equal(calls.length, 2);
  assert.equal(f.app.main.querySelector('.sa-recovery-retry').hidden, false);
  assert.match(f.app.main.querySelector('.sa-message').textContent, /Откройте приложение заново/u);
  f.app.cleanScreen(); assert.equal(f.timers.size, 0);
});

test('actual controller account ABA during recovery cannot mount or display a late former-account response', async () => {
  const pending = deferred(); let calls = 0;
  const f = await recoveryStage(async () => ++calls === 1 ? { ...stageResponse('/'), launchBinding: stageBinding } : pending.promise);
  const first = f.app.main.querySelector('iframe'); bootHint(f, first)(); await turn(); assert.equal(calls, 2);
  f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A');
  pending.resolve({ ...stageResponse('/'), launchBinding: stageBinding }); await turn();
  assert.equal(f.app.appStage, null); assert.equal(f.app.main.querySelector('iframe'), null); assert.equal(f.app.main.children.length, 0);
  assert.equal(f.timers.size, 0);
});

test('actual route Back during recovery disposes the watcher and never replaces the new screen', async () => {
  const pending = deferred(); let calls = 0;
  const f = await recoveryStage(async () => ++calls === 1 ? { ...stageResponse('/'), launchBinding: stageBinding } : pending.promise);
  bootHint(f, f.app.main.querySelector('iframe'))(); await turn();
  f.location.hash = '#mine'; f.app.cleanScreen(); pending.resolve({ ...stageResponse('/'), launchBinding: stageBinding }); await turn();
  assert.equal(f.app.main.querySelector('iframe'), null); assert.equal(f.app.main.children.length, 0); assert.equal(calls, 2);
});

test('observed grant removal and same-ID reactivation cancel an in-flight recovery without reviving its nonce', async () => {
  const pending = deferred(); let calls = 0;
  const f = await recoveryStage(async () => ++calls === 1 ? { ...stageResponse('/'), launchBinding: stageBinding } : pending.promise);
  const stage = f.app.appStage, original = f.app.main.querySelector('iframe');
  const granted = { appId: stageAppId, name: 'Editor', grants: { accountIds: ['account-A'], communityIds: [] } };
  stage.updateApp(granted); const emit = bootHint(f, original); emit(); await turn();
  stage.updateApp({ ...granted, grants: { accountIds: [], communityIds: [] } }); stage.updateApp(granted);
  pending.resolve({ ...stageResponse('/'), launchBinding: stageBinding }); await turn(); emit(); await turn();
  assert.equal(f.app.main.querySelector('iframe'), original); assert.equal(calls, 2); f.app.cleanScreen();
});

test('actual stage rejects source drift and lost ACK with a clear manual recovery action', async () => {
  for (const mode of ['source-drift', 'lost-ack']) {
    let calls = 0;
    const f = await recoveryStage(async () => {
      if (++calls === 1) return { ...stageResponse('/'), launchBinding: stageBinding };
      if (mode === 'lost-ack') throw failure('NETWORK_TIMEOUT');
      return { ...stageResponse('/'), launchBinding: { ...stageBinding, policyEpoch: 2 } };
    });
    const original = f.app.main.querySelector('iframe'); bootHint(f, original)(); await turn();
    assert.equal(f.app.main.querySelector('iframe'), original); assert.equal(calls, 2);
    assert.equal(f.app.main.querySelector('.sa-recovery-retry').hidden, false);
    assert.equal(f.app.main.querySelector('.sa-message').hidden, false); f.app.cleanScreen();
  }
});

test('legacy launch response mounts its runtime without requiring new metadata or a recovery message channel', async () => {
  let calls = 0; const f = await recoveryStage(async () => { calls++; return stageResponse('/'); });
  const original = f.app.main.querySelector('iframe'); original.emit('load'); await turn();
  assert.equal(original.contentWindow.sent.length, 0); assert.equal(calls, 1); assert.equal(f.timers.size, 0); f.app.cleanScreen();
});

test('actual stage preserves a discussion through initial metadata and rename, but refreshes changed access', async () => {
  const route = appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/' }, undefined, { panel: 'discussion' });
  const f = fixture({ hash: '#' + route });
  f.app.options.openApp = async () => stageResponse('/');
  await f.app.openApplication({ appId: stageAppId, name: 'Editor' }, appLaunch.parseAppLaunchRoute(route));
  const stage = f.app.appStage, frame = f.app.main.querySelector('iframe');
  const observed = { appId: stageAppId, name: 'Observed editor', status: 'ready', ownerAccountId: 'account-A', grants: { accountIds: ['person-b', 'person-a'], communityIds: [] } };
  stage.updateApp(observed); assert.equal(f.discussionCalls.refresh, 0);
  stage.updateApp({ ...observed, name: 'Renamed', deviceLabel: 'Another label', grants: { accountIds: ['person-a', 'person-b'], communityIds: [] } });
  assert.equal(f.discussionCalls.refresh, 0); assert.equal(f.app.main.querySelector('iframe'), frame);
  const changed = { ...observed, grants: { accountIds: ['person-a'], communityIds: [] } };
  stage.updateApp(changed); assert.equal(f.discussionCalls.refresh, 1);
  stage.updateApp({ ...changed, publication: { launchPolicy: 'anyone', activeNamedAddressCount: 1 } });
  assert.equal(f.discussionCalls.refresh, 2); assert.equal(f.app.main.querySelector('iframe'), frame);
});
test('actual controller and stage preserve the same runtime through discussion, archive and route Back', async () => {
  const route = appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/editor?q=one#row' });
  const f = fixture({ hash: '#' + route }), calls = [];
  f.app.options.openApp = async (_app, parameters) => { calls.push(parameters); return stageResponse(parameters.path); };
  await f.app.openApplication({ appId: stageAppId, name: 'Editor' }, appLaunch.parseAppLaunchRoute(route));
  const frame = f.app.main.querySelector('iframe'), stage = f.app.appStage, sequence = f.app.screenSequence;
  assert.ok(frame);
  for (const presentation of [{ panel: 'discussion' }, { panel: 'discussion', conversationId: `conv_${'f'.repeat(32)}` }, { panel: 'discussion' }, undefined]) {
    f.location.hash = '#' + appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/editor?q=one#row' }, undefined, presentation);
    await f.Controller.prototype.openRoute.call(f.app);
    assert.equal(f.app.appStage, stage); assert.equal(f.app.main.querySelector('iframe'), frame);
    assert.equal(f.app.screenSequence, sequence); assert.equal(calls.length, 1);
  }
  f.location.hash = '#' + appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/other' });
  await f.Controller.prototype.openRoute.call(f.app);
  assert.notEqual(f.app.main.querySelector('iframe'), frame); assert.equal(calls.length, 2);
  assert.equal(calls[1].path, '/other'); f.app.cleanScreen();
});

test('actual stage starts no runtime in the owner archive and opens it only on ordinary navigation', async () => {
  const admin = appLaunch.parseAppLaunchRoute(appLaunch.formatAppLaunchRoute({ appId: stageAppId }, undefined, { panel: 'discussion', administrative: true }));
  const f = fixture({ hash: '#' + admin.route }); let calls = 0;
  f.app.options.openApp = async () => { calls++; return stageResponse('/'); };
  await f.app.openApplication({ appId: stageAppId, name: 'Editor' }, admin);
  assert.equal(calls, 0); assert.equal(f.app.main.querySelector('iframe'), null);
  f.location.hash = '#' + appLaunch.formatAppLaunchRoute({ appId: stageAppId }, undefined, { panel: 'discussion' });
  await f.Controller.prototype.openRoute.call(f.app); await turn();
  assert.equal(calls, 1); assert.ok(f.app.main.querySelector('iframe')); f.app.cleanScreen();
});

test('actual application stage cannot revive a late launch after account A to B to A', async () => {
  const route = appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/' });
  const f = fixture({ hash: '#' + route }), pending = deferred();
  f.app.options.openApp = () => pending.promise;
  const opening = f.app.openApplication({ appId: stageAppId, name: 'Editor' }, appLaunch.parseAppLaunchRoute(route));
  const firstGeneration = f.app.accountGeneration;
  f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A');
  pending.resolve(stageResponse('/')); await opening;
  assert.equal(f.app.accountGeneration, firstGeneration + 2); assert.equal(f.app.appStage, null);
  assert.equal(f.app.main.querySelector('iframe'), null); assert.equal(f.app.main.children.length, 0);
});

test('same-account network loss during shell refresh preserves the running app stage', async () => {
  const route = appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/' });
  const f = fixture({ hash: '#' + route }); f.app.options.openApp = async () => stageResponse('/');
  await f.app.openApplication({ appId: stageAppId, name: 'Editor' }, appLaunch.parseAppLaunchRoute(route));
  const stage = f.app.appStage, frame = f.app.main.querySelector('iframe');
  f.setOnline(false); await f.app.refresh();
  assert.equal(f.app.appStage, stage); assert.equal(f.app.main.querySelector('iframe'), frame);
  assert.equal(f.app.lastToast, 'NETWORK_ERROR'); f.app.cleanScreen();
});

test('incoming call polling stays single-flight, pauses on pagehide, and never joins before Accept', async () => {
  const f = fixture(), pending = deferred(); let checks = 0, snapshots = 0, cancels = 0, joins = 0;
  f.app.options.humanCalls = {
    async checkIncoming() { checks++; return pending.promise; },
    cancelIncomingChecks() { cancels++; },
    snapshot() { snapshots++; return { state: 'idle', microphone: 'off', connection: 'idle', peers: 0,
      playbackBlocked: false, roomId: null, mode: null, code: null, turnConfigured: false, incomingCall: null }; },
    join() { joins++; return Promise.resolve({ ok: true }); },
  };
  f.app.destroyed = false; f.app.incomingPollTimer = null; f.app.incomingPollGeneration = 0;
  f.app.incomingPollInFlight = false; f.app.incomingPollSuspended = false;
  assert.equal(typeof f.app.options.humanCalls.checkIncoming, 'function');
  f.app.startIncomingCallPolling(); await turn();
  assert.equal(checks, 1, 'starting the poll checks the inbox immediately');
  assert.equal(f.intervals.size, 1, 'starting the poll schedules one interval');
  assert.equal([...f.intervals.values()][0].delay, 5000);
  [...f.intervals.values()][0].fn(); await turn(); assert.equal(checks, 1, 'an interval tick cannot overlap the first inbox read');
  f.app.stopIncomingCallPolling(); assert.equal(f.intervals.size, 0); assert.equal(cancels, 1);
  pending.resolve({ callId: 'call-late', epoch: 2, roomId: 'caller', label: 'Contact' }); await turn();
  assert.equal(snapshots, 0, 'pagehide invalidates the late inbox result'); assert.equal(joins, 0, 'polling never auto-joins');
});

test('pagehide and pageshow share one unresolved inbox poll and retry after its stale result settles', async () => {
  const f = fixture(), requests = []; let unsettled = 0, maximum = 0, snapshots = 0, painted = 0;
  f.app.destroyed = false; f.app.incomingPollTimer = null; f.app.incomingPollGeneration = 0;
  f.app.incomingPollInFlight = false; f.app.incomingPollSuspended = false;
  f.app.options.humanCalls = {
    checkIncoming() {
      const request = deferred(); requests.push(request); unsettled++; maximum = Math.max(maximum, unsettled);
      return request.promise.finally(() => { unsettled--; });
    },
    cancelIncomingChecks() {},
    snapshot() { snapshots++; return { state: 'idle', microphone: 'off', connection: 'idle', peers: 0,
      playbackBlocked: false, roomId: null, mode: null, code: null, turnConfigured: false, incomingCall: null }; },
  };
  f.app.receiveHumanCallState = () => { painted++; };
  f.app.startIncomingCallPolling(); await turn(); assert.equal(requests.length, 1);
  f.app.stopIncomingCallPolling(); f.app.startIncomingCallPolling(); await turn();
  assert.equal(requests.length, 1, 'resume cannot overlap the unresolved pagehide request');
  requests[0].resolve(null); await turn();
  assert.equal(requests.length, 2, 'the active resumed generation retries after the old request settles');
  assert.equal(maximum, 1, 'there is never more than one unresolved request');
  assert.equal(snapshots, 0, 'the stale hidden generation cannot paint its result'); assert.equal(painted, 0);
  requests[1].resolve(null); await turn();
  assert.equal(snapshots, 1); assert.equal(painted, 1, 'the current generation can recover after the old request settles');
  f.app.stopIncomingCallPolling(); assert.equal(f.intervals.size, 0); assert.equal(unsettled, 0);
});

test('incoming call card names explicit actions and disables Accept while another call is active', () => {
  const f = fixture(), app = f.app, element = () => new ElementPort();
  app.humanCallIncoming = element(); app.humanCallIncomingLabel = element(); app.humanCallIncomingStatus = element();
  app.humanCallIncomingAccept = element(); app.humanCallIncomingDecline = element(); app.humanCallIncomingPending = false;
  app.renderHumanIncomingCall({ callId: 'call-1', epoch: 1, roomId: 'peer', label: 'Собеседник' }, true);
  assert.equal(app.humanCallIncoming.hidden, false); assert.equal(app.humanCallIncomingLabel.textContent, 'Входящий звонок: Собеседник');
  assert.equal(app.humanCallIncomingAccept.textContent, 'Принять'); assert.equal(app.humanCallIncomingAccept.disabled, true);
  assert.equal(app.humanCallIncomingDecline.textContent, 'Отклонить'); assert.equal(app.humanCallIncomingDecline.disabled, false);
  assert.match(app.humanCallIncomingStatus.textContent, /текущий звонок/u);
});

test('actual app controller enqueues launch before slow optional metadata on the real serialized Connect client', { timeout: 10_000 }, async t => {
  for (const held of ['apps.list', 'apps.catalog', 'world.community.get']) {
    const projectId = 'actual-app-queue', origin = 'https://shell.example', endpoint = `${origin}/rpc`, scope = { projectId, endpoint };
    const appId = `app-${'a'.repeat(32)}`, domainId = `dom_${'b'.repeat(32)}`, operations = [], entered = deferred(), release = deferred();
    const launchUrl = `https://app.runtime.example/_soty/boot?path=%2F#${'t'.repeat(43)}`;
    let value = null;
    const copy = () => value === null ? null : structuredClone(value);
    const storage = {
      async read() { return copy(); },
      async claim(candidate) { if (value === null) { validateState(candidate, scope); value = structuredClone(candidate); } return copy(); },
      async compareAndSwap(revision, candidate) {
        assert.equal(value.localRevision, revision); validateState(candidate, scope); value = structuredClone(candidate); return copy();
      },
    };
    const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin], extensions: [{
      operations: new Set(['apps.launch', 'apps.list', 'apps.catalog', 'world.community.get']), execute({ op, actor }) {
        operations.push(op);
        if (op === 'apps.launch') return { launchUrl, entry: { appId, domainId, origin: 'https://app.runtime.example', path: '/' } };
        if (op === 'world.community.get') return { community: { communityId: 'community-test', membership: { state: 'none' } } };
        if (op === 'apps.list' && held === 'apps.catalog') return { apps: [] };
        if (op === 'apps.catalog') return { apps: [{ id: appId, name: 'Actual app name', state: 'enabled', ownerAccountId: 'another-owner',
          publication: { named: { access: 'anyone', listed: true } }, entry: { domainId: `dom_${'c'.repeat(32)}`, origin: 'https://metadata.runtime.example', path: '/changed' } }] };
        return { apps: [{ id: appId, name: 'Actual app name', state: 'enabled', ownerAccountId: actor.accountId, hostDeviceId: 'fixture-device' }] };
      },
    }] });
    const client = createClientWithStorage({ projectId, endpoint, fetch: async (_url, options) => {
      const body = JSON.parse(options.body), response = await service.handle({ ...body, origin });
      if (body.op === held) { entered.resolve(); await release.promise; }
      return new Response(JSON.stringify(response), { status: response.ok ? 200 : 400 });
    } }, storage);
    t.after(() => { client.dispose(); service.close(); });
    const identity = await client.bootstrap('Owner');
    const route = appLaunch.formatAppLaunchRoute(held !== 'world.community.get' ? { appId, domainId } : { appId }, held !== 'world.community.get' ? undefined : 'community-test');
    const f = fixture({ initial: identity.accountId, hash: `#${route}` }); f.app.communities = [];
    f.app.api = { request: (op, args) => client.extension(op, JSON.parse(JSON.stringify(args ?? {})), { expectedAccountId: identity.accountId }) };
    const metadata = deferred(), loadApps = f.app.loadApps.bind(f.app);
    f.app.loadApps = async (...args) => { try { return await loadApps(...args); } finally { metadata.resolve(); } };
    const opening = f.app.openApplication({ appId, name: 'Приложение' }, appLaunch.parseAppLaunchRoute(route), true);
    try {
      await entered.promise; await turn();
      assert.equal(operations[0], 'apps.launch', held);
      assert.equal(f.app.main.querySelector('iframe')?.src, launchUrl, 'runtime frame is mounted while optional response is still held');
      await opening;
    } finally { release.resolve(); await opening; }
    await metadata.promise; await turn();
    assert.equal(f.app.main.querySelector('iframe')?.title, 'Actual app name');
    assert.equal(f.app.main.querySelector('iframe')?.src, launchUrl, 'public metadata cannot replace the admitted runtime');
    assert.deepEqual(f.app.appStage.entry(), { appId, domainId, origin: 'https://app.runtime.example', path: '/' });
    f.app.cleanScreen();
  }
});


test('invitation route never reaches Apps request or saved account route', async () => {
  const token='a'.repeat(64), route='#app/'+stageAppId+'?invitation='+token;
  const f=fixture({hash:route}),calls=[];
  f.app.options.openApp=async(_app,parameters)=>{calls.push(parameters);return stageResponse('/embed');};
  await f.app.openApplication({appId:stageAppId,name:'EcoLab'},appLaunch.parseAppLaunchRoute(route));
  assert.equal(calls.length,1);assert.ok(!JSON.stringify(calls).includes(token));
  assert.ok(!f.location.hash.includes(token));assert.ok(!JSON.stringify(f.app.desk).includes(token));
  assert.equal(calls[0].path,undefined);f.app.cleanScreen();
});
